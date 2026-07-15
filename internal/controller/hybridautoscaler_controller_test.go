package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	autoscalingv1alpha1 "github.com/neogan74/hybridautoscaler/api/v1alpha1"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add apps scheme: %v", err)
	}
	if err := autoscalingv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add autoscaling scheme: %v", err)
	}
	return scheme
}

func testHA(namespace, name, targetKind, targetName string) *autoscalingv1alpha1.HybridAutoscaler {
	return &autoscalingv1alpha1.HybridAutoscaler{
		ObjectMeta: metav1ObjectMeta(namespace, name),
		Spec: autoscalingv1alpha1.HybridAutoscalerSpec{
			TargetRef: autoscalingv1alpha1.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       targetKind,
				Name:       targetName,
			},
			Horizontal: autoscalingv1alpha1.HorizontalSpec{
				MaxReplicas: 10,
			},
		},
	}
}

func metav1ObjectMeta(namespace, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Namespace: namespace,
		Name:      name,
	}
}

func TestTargetRefIndexValue(t *testing.T) {
	if got, want := targetRefIndexValue("Deployment", "api"), "Deployment/api"; got != want {
		t.Fatalf("targetRefIndexValue() = %q, want %q", got, want)
	}
}

func TestTargetKindForObject(t *testing.T) {
	tests := []struct {
		name string
		obj  client.Object
		want string
		ok   bool
	}{
		{name: "deployment", obj: &appsv1.Deployment{}, want: "Deployment", ok: true},
		{name: "statefulset", obj: &appsv1.StatefulSet{}, want: "StatefulSet", ok: true},
		{name: "replicaset", obj: &appsv1.ReplicaSet{}, want: "ReplicaSet", ok: true},
		{name: "unsupported", obj: &autoscalingv1alpha1.HybridAutoscaler{}, want: "", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := targetKindForObject(tt.obj)
			if got != tt.want || ok != tt.ok {
				t.Fatalf("targetKindForObject() = (%q, %v), want (%q, %v)", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestRequestsForTargetUsesTargetRefIndex(t *testing.T) {
	ctx := context.Background()
	matching := testHA("default", "api-ha", "Deployment", "api")
	wrongTarget := testHA("default", "worker-ha", "Deployment", "worker")
	wrongKind := testHA("default", "api-sts-ha", "StatefulSet", "api")
	wrongNamespace := testHA("other", "other-api-ha", "Deployment", "api")

	cl := crfake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(matching, wrongTarget, wrongKind, wrongNamespace).
		WithIndex(&autoscalingv1alpha1.HybridAutoscaler{}, targetRefIndexField, func(rawObj client.Object) []string {
			ha := rawObj.(*autoscalingv1alpha1.HybridAutoscaler)
			return []string{targetRefIndexValue(ha.Spec.TargetRef.Kind, ha.Spec.TargetRef.Name)}
		}).
		Build()

	r := &HybridAutoscalerReconciler{Client: cl}
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1ObjectMeta("default", "api"),
	}

	requests := r.requestsForTarget(ctx, deployment)
	if len(requests) != 1 {
		t.Fatalf("requests length = %d, want 1: %#v", len(requests), requests)
	}

	want := types.NamespacedName{Namespace: "default", Name: "api-ha"}
	if requests[0].NamespacedName != want {
		t.Fatalf("request = %s, want %s", requests[0].NamespacedName, want)
	}
}
