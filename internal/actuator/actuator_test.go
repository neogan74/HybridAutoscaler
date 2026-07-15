package actuator

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	autoscalingv1alpha1 "github.com/neogan74/hybridautoscaler/api/v1alpha1"
	"github.com/neogan74/hybridautoscaler/internal/coordinator"
)

// ---- Test fixtures ----

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = appsv1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	_ = autoscalingv1alpha1.AddToScheme(s)
	return s
}

func int32Ptr(v int32) *int32 { return &v }

func newHorizontalDecision(target int32) coordinator.HorizontalAction {
	dir := coordinator.ScaleUp
	if target < 3 {
		dir = coordinator.ScaleDown
	}
	return coordinator.HorizontalAction{
		ShouldScale:    true,
		TargetReplicas: target,
		Direction:      dir,
		Reason:         "test",
	}
}

func newVerticalDecision(cpu, memory string) coordinator.VerticalAction {
	return coordinator.VerticalAction{
		ShouldScale: true,
		Direction:   coordinator.ScaleUp,
		ContainerResources: map[string]*coordinator.ContainerResourceAction{
			"app": {
				ContainerName: "app",
				TargetRequest: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(cpu),
					corev1.ResourceMemory: resource.MustParse(memory),
				},
				TargetLimit: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("2"),
					corev1.ResourceMemory: resource.MustParse("2Gi"),
				},
				CurrentRequest: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("128Mi"),
				},
			},
		},
		Reason: "test",
	}
}

func makeDeployment(name string, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "app",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("500m"),
									corev1.ResourceMemory: resource.MustParse("512Mi"),
								},
							},
						},
					},
				},
			},
		},
	}
}

func makePod(name string, ready bool, age time.Duration) corev1.Pod {
	conditions := []corev1.PodCondition{}
	if ready {
		conditions = append(conditions, corev1.PodCondition{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		})
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-age)),
			Labels:            map[string]string{"app": "dep"},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: conditions,
		},
	}
}

func newHA(kind, name string, vertical *autoscalingv1alpha1.VerticalSpec) *autoscalingv1alpha1.HybridAutoscaler {
	return &autoscalingv1alpha1.HybridAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ha", Namespace: "default"},
		Spec: autoscalingv1alpha1.HybridAutoscalerSpec{
			TargetRef: autoscalingv1alpha1.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       kind,
				Name:       name,
			},
			Horizontal: autoscalingv1alpha1.HorizontalSpec{
				MinReplicas: int32Ptr(int32(2)),
				MaxReplicas: 10,
			},
			Vertical: vertical,
		},
	}
}

// ---- Pure function tests ----

func TestDefaultActuatorConfig(t *testing.T) {
	cfg := DefaultActuatorConfig()
	if cfg.DryRun {
		t.Error("DryRun default should be false")
	}
	if cfg.MaxEvictionsPerCycle != 1 {
		t.Errorf("MaxEvictionsPerCycle = %d, want 1", cfg.MaxEvictionsPerCycle)
	}
	if cfg.EvictionTimeout != 60*time.Second {
		t.Errorf("EvictionTimeout = %v, want 60s", cfg.EvictionTimeout)
	}
	if cfg.PodReadyTimeout != 300*time.Second {
		t.Errorf("PodReadyTimeout = %v, want 300s", cfg.PodReadyTimeout)
	}
}

func TestNewActuator(t *testing.T) {
	a := NewActuator(nil, nil, DefaultActuatorConfig())
	if a == nil {
		t.Fatal("NewActuator returned nil")
	}
	if a.config.MaxEvictionsPerCycle != 1 {
		t.Errorf("config not set: MaxEvictionsPerCycle = %d", a.config.MaxEvictionsPerCycle)
	}
}

func TestIsPodReady(t *testing.T) {
	tests := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{
			name: "ready true",
			pod: &corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			}}},
			want: true,
		},
		{
			name: "ready false",
			pod: &corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			}}},
			want: false,
		},
		{
			name: "no conditions",
			pod:  &corev1.Pod{Status: corev1.PodStatus{}},
			want: false,
		},
		{
			name: "other condition only",
			pod: &corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			}}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPodReady(tt.pod); got != tt.want {
				t.Errorf("isPodReady() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSelectPodsToEvict(t *testing.T) {
	pods := []corev1.Pod{
		makePod("pod-new", true, 1*time.Minute),
		makePod("pod-old", true, 10*time.Minute),
		makePod("pod-middle", true, 5*time.Minute),
	}

	tests := []struct {
		name  string
		pods  []corev1.Pod
		count int
		want  []string
	}{
		{
			name:  "empty input",
			pods:  nil,
			count: 2,
			want:  nil,
		},
		{
			name:  "zero count",
			pods:  pods,
			count: 0,
			want:  nil,
		},
		{
			name:  "negative count",
			pods:  pods,
			count: -1,
			want:  nil,
		},
		{
			name:  "select oldest one",
			pods:  pods,
			count: 1,
			want:  []string{"pod-old"},
		},
		{
			name:  "select oldest two",
			pods:  pods,
			count: 2,
			want:  []string{"pod-old", "pod-middle"},
		},
		{
			name:  "count exceeds pods - returns all sorted",
			pods:  pods,
			count: 10,
			want:  []string{"pod-old", "pod-middle", "pod-new"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selectPodsToEvict(tt.pods, tt.count)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d pods, want %d", len(got), len(tt.want))
			}
			for i, wantName := range tt.want {
				if got[i].Name != wantName {
					t.Errorf("pod[%d] = %q, want %q", i, got[i].Name, wantName)
				}
			}
		})
	}
}

func TestSelectPodsToEvictDoesNotMutateInput(t *testing.T) {
	pods := []corev1.Pod{
		makePod("pod-a", true, 1*time.Minute),
		makePod("pod-b", true, 10*time.Minute),
	}
	original := []string{pods[0].Name, pods[1].Name}

	_ = selectPodsToEvict(pods, 1)

	for i, want := range original {
		if pods[i].Name != want {
			t.Errorf("input mutated: pods[%d].Name = %q, want %q", i, pods[i].Name, want)
		}
	}
}

// ---- Apply / priority tests ----

func TestApplyDryRun(t *testing.T) {
	a := NewActuator(nil, nil, ActuatorConfig{DryRun: true, MaxEvictionsPerCycle: 1})
	ha := newHA("Deployment", "dep", nil)

	decision := &coordinator.Decision{
		Priority:         coordinator.PriorityHorizontal,
		HorizontalAction: newHorizontalDecision(5),
	}

	result, err := a.Apply(context.Background(), ha, decision)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
	if result.HorizontalApplied {
		t.Error("DryRun should not apply horizontal scaling")
	}
	if result.VerticalApplied {
		t.Error("DryRun should not apply vertical scaling")
	}
	if len(result.ScalingEvents) != 0 {
		t.Errorf("DryRun should not produce events, got %d", len(result.ScalingEvents))
	}
}

func TestApplyNilDecisionNoop(t *testing.T) {
	a := NewActuator(nil, nil, DefaultActuatorConfig())
	ha := newHA("Deployment", "dep", nil)

	result, err := a.Apply(context.Background(), ha, nil)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
	if result.HorizontalApplied || result.VerticalApplied {
		t.Fatal("nil decision should not apply scaling")
	}
	if len(result.ScalingEvents) != 0 {
		t.Fatalf("ScalingEvents length = %d, want 0", len(result.ScalingEvents))
	}
}

func TestApplyPriorityNone(t *testing.T) {
	a := NewActuator(nil, nil, DefaultActuatorConfig())
	ha := newHA("Deployment", "dep", nil)

	decision := &coordinator.Decision{Priority: coordinator.PriorityNone}

	result, err := a.Apply(context.Background(), ha, decision)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
	if result.HorizontalApplied || result.VerticalApplied {
		t.Error("PriorityNone should not apply anything")
	}
}

func TestApplyUnsupportedTargetKind(t *testing.T) {
	a := NewActuator(crfake.NewClientBuilder().WithScheme(newTestScheme()).Build(), nil, DefaultActuatorConfig())
	ha := newHA("DaemonSet", "dep", nil)

	decision := &coordinator.Decision{
		Priority:         coordinator.PriorityHorizontal,
		HorizontalAction: newHorizontalDecision(5),
	}

	result, err := a.Apply(context.Background(), ha, decision)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
	if len(result.Errors) == 0 {
		t.Error("expected an error in result for unsupported kind")
	}
	if result.HorizontalApplied {
		t.Error("should not have applied horizontal scaling for unsupported kind")
	}
}

func TestApplyHorizontalScalingDeployment(t *testing.T) {
	dep := makeDeployment("dep", 3)
	kubeClient := kubefake.NewSimpleClientset(dep)

	a := NewActuator(nil, kubeClient, DefaultActuatorConfig())
	ha := newHA("Deployment", "dep", nil)

	decision := &coordinator.Decision{
		Priority:         coordinator.PriorityHorizontal,
		HorizontalAction: newHorizontalDecision(6),
	}

	result, err := a.Apply(context.Background(), ha, decision)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
	if len(result.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
	if !result.HorizontalApplied {
		t.Error("expected HorizontalApplied to be true")
	}
	if result.NewReplicas != 6 {
		t.Errorf("NewReplicas = %d, want 6", result.NewReplicas)
	}
	if len(result.ScalingEvents) != 1 {
		t.Fatalf("expected 1 scaling event, got %d", len(result.ScalingEvents))
	}
	ev := result.ScalingEvents[0]
	if ev.Type != autoscalingv1alpha1.HorizontalScaling {
		t.Errorf("event type = %q, want Horizontal", ev.Type)
	}
	if !ev.Success {
		t.Error("event should be successful")
	}
	if ev.FromReplicas == nil || *ev.FromReplicas != 3 {
		t.Errorf("FromReplicas = %v, want 3", ev.FromReplicas)
	}
	if ev.ToReplicas == nil || *ev.ToReplicas != 6 {
		t.Errorf("ToReplicas = %v, want 6", ev.ToReplicas)
	}

	updated, err := kubeClient.AppsV1().Deployments("default").Get(context.Background(), "dep", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get deployment: %v", err)
	}
	if updated.Spec.Replicas == nil || *updated.Spec.Replicas != 6 {
		t.Errorf("deployment replicas = %v, want 6", updated.Spec.Replicas)
	}
}

func TestApplyHorizontalScalingStatefulSet(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "sts", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: int32Ptr(2)},
	}
	kubeClient := kubefake.NewSimpleClientset(sts)

	a := NewActuator(nil, kubeClient, DefaultActuatorConfig())
	ha := newHA("StatefulSet", "sts", nil)

	decision := &coordinator.Decision{
		Priority:         coordinator.PriorityHorizontal,
		HorizontalAction: newHorizontalDecision(4),
	}

	result, err := a.Apply(context.Background(), ha, decision)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
	if !result.HorizontalApplied || result.NewReplicas != 4 {
		t.Fatalf("expected horizontal applied with 4 replicas, got applied=%v replicas=%d",
			result.HorizontalApplied, result.NewReplicas)
	}
}

func TestApplyHorizontalScalingReplicaSet(t *testing.T) {
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "rs", Namespace: "default"},
		Spec:       appsv1.ReplicaSetSpec{Replicas: int32Ptr(2)},
	}
	kubeClient := kubefake.NewSimpleClientset(rs)

	a := NewActuator(nil, kubeClient, DefaultActuatorConfig())
	ha := newHA("ReplicaSet", "rs", nil)

	decision := &coordinator.Decision{
		Priority:         coordinator.PriorityHorizontal,
		HorizontalAction: newHorizontalDecision(3),
	}

	result, err := a.Apply(context.Background(), ha, decision)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
	if !result.HorizontalApplied || result.NewReplicas != 3 {
		t.Fatalf("expected horizontal applied with 3 replicas, got applied=%v replicas=%d",
			result.HorizontalApplied, result.NewReplicas)
	}
}

func TestApplyHorizontalScalingShouldScaleFalse(t *testing.T) {
	a := NewActuator(nil, kubefake.NewSimpleClientset(), DefaultActuatorConfig())
	ha := newHA("Deployment", "dep", nil)

	decision := &coordinator.Decision{
		Priority: coordinator.PriorityHorizontal,
		HorizontalAction: coordinator.HorizontalAction{
			ShouldScale: false,
		},
	}

	result, err := a.Apply(context.Background(), ha, decision)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
	if result.HorizontalApplied {
		t.Error("should not apply when ShouldScale is false")
	}
}

// ---- Vertical scaling tests ----

func TestApplyVerticalScalingUpdateModeOff(t *testing.T) {
	dep := makeDeployment("dep", 3)
	cl := crfake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(dep).Build()

	a := NewActuator(cl, kubefake.NewSimpleClientset(), DefaultActuatorConfig())
	ha := newHA("Deployment", "dep", &autoscalingv1alpha1.VerticalSpec{
		UpdatePolicy: &autoscalingv1alpha1.UpdatePolicy{
			UpdateMode: autoscalingv1alpha1.UpdateModeOff,
		},
	})

	decision := &coordinator.Decision{
		Priority:       coordinator.PriorityVertical,
		VerticalAction: newVerticalDecision("200m", "256Mi"),
	}

	result, err := a.Apply(context.Background(), ha, decision)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
	if result.VerticalApplied {
		t.Error("should not apply vertical scaling when UpdateMode=Off")
	}
}

func TestApplyVerticalScalingDeploymentTemplate(t *testing.T) {
	dep := makeDeployment("dep", 3)
	cl := crfake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(dep).Build()

	a := NewActuator(cl, kubefake.NewSimpleClientset(), DefaultActuatorConfig())
	ha := newHA("Deployment", "dep", &autoscalingv1alpha1.VerticalSpec{
		UpdatePolicy: &autoscalingv1alpha1.UpdatePolicy{
			UpdateMode:  autoscalingv1alpha1.UpdateModeInitial,
			MinReplicas: int32Ptr(int32(2)),
		},
	})

	decision := &coordinator.Decision{
		Priority:       coordinator.PriorityVertical,
		VerticalAction: newVerticalDecision("250m", "384Mi"),
	}

	result, err := a.Apply(context.Background(), ha, decision)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
	if len(result.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
	if !result.VerticalApplied {
		t.Fatal("expected VerticalApplied to be true")
	}
	if len(result.ScalingEvents) != 1 {
		t.Fatalf("expected 1 scaling event, got %d", len(result.ScalingEvents))
	}
	if result.ScalingEvents[0].Type != autoscalingv1alpha1.VerticalScaling {
		t.Errorf("event type = %q, want Vertical", result.ScalingEvents[0].Type)
	}
	if len(result.EvictedPods) != 0 {
		t.Errorf("UpdateMode=Initial should not evict pods, got %v", result.EvictedPods)
	}

	updated := &appsv1.Deployment{}
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "dep"}, updated); err != nil {
		t.Fatalf("failed to get deployment: %v", err)
	}
	c := updated.Spec.Template.Spec.Containers[0]
	if got := c.Resources.Requests.Cpu().MilliValue(); got != 250 {
		t.Errorf("CPU request = %dm, want 250m", got)
	}
	expectedMem := resource.MustParse("384Mi")
	if got := c.Resources.Requests.Memory().Value(); got != expectedMem.Value() {
		t.Errorf("Memory request = %d, want 384Mi (%d)", got, expectedMem.Value())
	}
	if got := c.Resources.Limits.Cpu().MilliValue(); got != 2000 {
		t.Errorf("CPU limit = %dm, want 2000m (2)", got)
	}
}

// ---- Eviction tests ----

func TestEvictPodsForVerticalScalingRespectsMinReplicas(t *testing.T) {
	dep := makeDeployment("dep", 3)
	cl := crfake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(dep).
		WithLists(&corev1.PodList{Items: []corev1.Pod{makePod("pod-1", true, 10*time.Minute), makePod("pod-2", true, 5*time.Minute), makePod("pod-3", true, 1*time.Minute)}}).
		Build()

	kubeClient := kubefake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "default"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Namespace: "default"}},
	)

	a := NewActuator(cl, kubeClient, DefaultActuatorConfig())

	// 3 ready pods, minReplicas=3 -> canEvict = 0
	ha := newHA("Deployment", "dep", &autoscalingv1alpha1.VerticalSpec{
		UpdatePolicy: &autoscalingv1alpha1.UpdatePolicy{
			UpdateMode:  autoscalingv1alpha1.UpdateModeAuto,
			MinReplicas: int32Ptr(int32(3)),
		},
	})

	evicted, err := a.evictPodsForVerticalScaling(context.Background(), ha, newVerticalDecision("200m", "256Mi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(evicted) != 0 {
		t.Errorf("expected 0 evictions at minReplicas, got %v", evicted)
	}
}

func TestEvictPodsForVerticalScalingLimitsMaxEvictions(t *testing.T) {
	dep := makeDeployment("dep", 1)
	cl := crfake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(dep).
		WithLists(&corev1.PodList{Items: []corev1.Pod{
			makePod("pod-1", true, 10*time.Minute),
			makePod("pod-2", true, 8*time.Minute),
			makePod("pod-3", true, 5*time.Minute),
			makePod("pod-4", true, 1*time.Minute),
		}}).
		Build()

	kubeClient := kubefake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "default"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Namespace: "default"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-4", Namespace: "default"}},
	)

	a := NewActuator(cl, kubeClient, ActuatorConfig{
		MaxEvictionsPerCycle: 2,
		EvictionTimeout:      60 * time.Second,
		PodReadyTimeout:      300 * time.Second,
	})

	ha := newHA("Deployment", "dep", &autoscalingv1alpha1.VerticalSpec{
		UpdatePolicy: &autoscalingv1alpha1.UpdatePolicy{
			UpdateMode:  autoscalingv1alpha1.UpdateModeAuto,
			MinReplicas: int32Ptr(int32(1)),
		},
	})

	evicted, err := a.evictPodsForVerticalScaling(context.Background(), ha, newVerticalDecision("200m", "256Mi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(evicted) != 2 {
		t.Errorf("expected 2 evictions (MaxEvictionsPerCycle), got %d: %v", len(evicted), evicted)
	}
}

func TestEvictPodsForVerticalScalingNoPods(t *testing.T) {
	dep := makeDeployment("dep", 1)
	cl := crfake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(dep).Build()
	kubeClient := kubefake.NewSimpleClientset()

	a := NewActuator(cl, kubeClient, DefaultActuatorConfig())
	ha := newHA("Deployment", "dep", &autoscalingv1alpha1.VerticalSpec{
		UpdatePolicy: &autoscalingv1alpha1.UpdatePolicy{
			UpdateMode:  autoscalingv1alpha1.UpdateModeAuto,
			MinReplicas: int32Ptr(int32(1)),
		},
	})

	evicted, err := a.evictPodsForVerticalScaling(context.Background(), ha, newVerticalDecision("200m", "256Mi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(evicted) != 0 {
		t.Errorf("expected 0 evictions with no pods, got %v", evicted)
	}
}
