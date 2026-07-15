package coordinator

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	autoscalingv1alpha1 "github.com/neogan74/hybridautoscaler/api/v1alpha1"
	"github.com/neogan74/hybridautoscaler/internal/metrics"
	"github.com/neogan74/hybridautoscaler/internal/recommender"
)

// Test helper functions

func ptr[T any](v T) *T {
	return &v
}

func newTestCoordinator() *Coordinator {
	return NewCoordinator(DefaultCoordinatorConfig())
}

func newTestHybridAutoscaler(strategy autoscalingv1alpha1.CoordinationStrategy) *autoscalingv1alpha1.HybridAutoscaler {
	return &autoscalingv1alpha1.HybridAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-ha",
			Namespace: "default",
		},
		Spec: autoscalingv1alpha1.HybridAutoscalerSpec{
			TargetRef: autoscalingv1alpha1.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "test-deployment",
			},
			Horizontal: autoscalingv1alpha1.HorizontalSpec{
				MinReplicas: ptr(int32(2)),
				MaxReplicas: 10,
			},
			Vertical: &autoscalingv1alpha1.VerticalSpec{},
			Coordination: &autoscalingv1alpha1.CoordinationSpec{
				Strategy: strategy,
			},
		},
		Status: autoscalingv1alpha1.HybridAutoscalerStatus{
			CurrentReplicas: 3,
		},
	}
}

func newTestMetrics(cpuUtil, memUtil float64) *metrics.CollectedMetrics {
	if cpuUtil <= 1 {
		cpuUtil *= 100
	}
	if memUtil <= 1 {
		memUtil *= 100
	}

	return &metrics.CollectedMetrics{
		Timestamp: time.Now(),
		Aggregated: &metrics.AggregatedMetrics{
			AverageCPUUtilization:    cpuUtil,
			AverageMemoryUtilization: memUtil,
		},
		Pods: []*metrics.PodMetricsData{
			{
				Name:      "test-pod-1",
				Namespace: "default",
				Containers: map[string]*metrics.ContainerMetrics{
					"app": {
						Name: "app",
						CPU: metrics.CPUMetrics{
							UsageMillicores:    100,
							RequestMillicores:  100,
							LimitMillicores:    200,
							UtilizationPercent: cpuUtil,
						},
						Memory: metrics.MemoryMetrics{
							UsageBytes:         128 * 1024 * 1024,
							WorkingSetBytes:    128 * 1024 * 1024,
							RequestBytes:       128 * 1024 * 1024,
							LimitBytes:         256 * 1024 * 1024,
							UtilizationPercent: memUtil,
						},
					},
				},
				Timestamp: time.Now(),
			},
		},
	}
}

func newHorizontalRec(replicas int32, confidence float64) *recommender.HorizontalRecommendation {
	return &recommender.HorizontalRecommendation{
		DesiredReplicas: replicas,
		CurrentReplicas: 3,
		MinReplicas:     2,
		MaxReplicas:     10,
		Reason:          "test-reason",
		Confidence:      confidence,
		Timestamp:       time.Now(),
	}
}

func newVerticalRec(containers map[string]corev1.ResourceList, confidence float64) *recommender.VerticalRecommendation {
	recs := make(map[string]*recommender.ContainerRecommendation)
	for name, target := range containers {
		recs[name] = &recommender.ContainerRecommendation{
			ContainerName: name,
			Target:        target,
			CurrentRequest: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
		}
	}
	return &recommender.VerticalRecommendation{
		ContainerRecommendations: recs,
		Reason:                   "test-reason",
		Confidence:               confidence,
		Timestamp:                time.Now(),
	}
}

func newCurrentState(replicas int32, resources map[string]corev1.ResourceRequirements) *CurrentState {
	return &CurrentState{
		Replicas:           replicas,
		ReadyReplicas:      replicas,
		ContainerResources: resources,
	}
}

func newRecommendations(hRec *recommender.HorizontalRecommendation, vRec *recommender.VerticalRecommendation) *recommender.Recommendations {
	return &recommender.Recommendations{
		Horizontal: hRec,
		Vertical:   vRec,
	}
}

// TestNewCoordinator tests coordinator initialization
func TestNewCoordinator(t *testing.T) {
	c := NewCoordinator(DefaultCoordinatorConfig())
	if c == nil {
		t.Fatal("NewCoordinator returned nil")
	}
	if c.cooldownTracker == nil {
		t.Error("cooldownTracker not initialized")
	}
}

// TestDefaultCoordinatorConfig tests default configuration values
func TestDefaultCoordinatorConfig(t *testing.T) {
	cfg := DefaultCoordinatorConfig()

	tests := []struct {
		name     string
		got      interface{}
		expected interface{}
	}{
		{"StabilizationWindow", cfg.DefaultStabilizationWindow, 300 * time.Second},
		{"ScaleUpCooldown", cfg.DefaultScaleUpCooldown, 60 * time.Second},
		{"ScaleDownCooldown", cfg.DefaultScaleDownCooldown, 300 * time.Second},
		{"VerticalScaleUpCooldown", cfg.DefaultVerticalScaleUpCooldown, 120 * time.Second},
		{"VerticalScaleDownCooldown", cfg.DefaultVerticalScaleDownCooldown, 600 * time.Second},
		{"VerticalScaleUpThreshold", cfg.VerticalScaleUpThreshold, 0.9},
		{"VerticalScaleDownThreshold", cfg.VerticalScaleDownThreshold, 0.5},
		{"MinChangeFraction", cfg.MinChangeFraction, 0.1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("%s = %v, want %v", tt.name, tt.got, tt.expected)
			}
		})
	}
}

// TestHorizontalFirstStrategy tests the HorizontalFirst coordination strategy
func TestHorizontalFirstStrategy(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name                  string
		currentReplicas       int32
		horizontalReplicas    int32
		verticalChange        bool
		expectHorizontalScale bool
		expectVerticalScale   bool
	}{
		{
			name:                  "scale up horizontally",
			currentReplicas:       3,
			horizontalReplicas:    5,
			verticalChange:        false,
			expectHorizontalScale: true,
			expectVerticalScale:   false,
		},
		{
			name:                  "scale down horizontally",
			currentReplicas:       5,
			horizontalReplicas:    3,
			verticalChange:        false,
			expectHorizontalScale: true,
			expectVerticalScale:   false,
		},
		{
			name:                  "no changes needed",
			currentReplicas:       3,
			horizontalReplicas:    3,
			verticalChange:        false,
			expectHorizontalScale: false,
			expectVerticalScale:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coord := newTestCoordinator()
			ha := newTestHybridAutoscaler(autoscalingv1alpha1.HorizontalFirst)
			ha.Status.CurrentReplicas = tt.currentReplicas

			hRec := newHorizontalRec(tt.horizontalReplicas, 0.9)

			var vRec *recommender.VerticalRecommendation
			if tt.verticalChange {
				vRec = newVerticalRec(map[string]corev1.ResourceList{
					"app": {
						corev1.ResourceCPU:    resource.MustParse("200m"),
						corev1.ResourceMemory: resource.MustParse("256Mi"),
					},
				}, 0.9)
			} else {
				vRec = newVerticalRec(map[string]corev1.ResourceList{}, 0.9)
			}

			currentResources := map[string]corev1.ResourceRequirements{
				"app": {
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("100m"),
						corev1.ResourceMemory: resource.MustParse("128Mi"),
					},
				},
			}

			currentState := newCurrentState(tt.currentReplicas, currentResources)
			recommendations := newRecommendations(hRec, vRec)

			decision, err := coord.Coordinate(ctx, ha, currentState, recommendations, newTestMetrics(0.7, 0.7))
			if err != nil {
				t.Fatalf("Coordinate failed: %v", err)
			}

			if decision.HorizontalAction.ShouldScale != tt.expectHorizontalScale {
				t.Errorf("HorizontalAction.ShouldScale = %v, want %v", decision.HorizontalAction.ShouldScale, tt.expectHorizontalScale)
			}
			if decision.VerticalAction.ShouldScale != tt.expectVerticalScale {
				t.Errorf("VerticalAction.ShouldScale = %v, want %v", decision.VerticalAction.ShouldScale, tt.expectVerticalScale)
			}
		})
	}
}

// TestBalancedStrategy tests the Balanced coordination strategy
func TestBalancedStrategy(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name                  string
		horizontalReplicas    int32
		currentReplicas       int32
		verticalChange        bool
		verticalUp            bool
		expectHorizontalScale bool
		expectVerticalScale   bool
	}{
		{
			name:                  "both scale up - prefer horizontal",
			horizontalReplicas:    5,
			currentReplicas:       3,
			verticalChange:        true,
			verticalUp:            true,
			expectHorizontalScale: true,
			expectVerticalScale:   false,
		},
		{
			name:                  "both scale down - prefer horizontal first",
			horizontalReplicas:    2,
			currentReplicas:       5,
			verticalChange:        true,
			verticalUp:            false,
			expectHorizontalScale: true,
			expectVerticalScale:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coord := newTestCoordinator()
			ha := newTestHybridAutoscaler(autoscalingv1alpha1.Balanced)
			ha.Status.CurrentReplicas = tt.currentReplicas

			hRec := newHorizontalRec(tt.horizontalReplicas, 0.9)

			currentResources := map[string]corev1.ResourceRequirements{
				"app": {
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("100m"),
						corev1.ResourceMemory: resource.MustParse("128Mi"),
					},
				},
			}

			var vRec *recommender.VerticalRecommendation
			if tt.verticalChange {
				var cpu, mem string
				if tt.verticalUp {
					cpu = "200m"
					mem = "256Mi"
				} else {
					cpu = "50m"
					mem = "64Mi"
				}
				vRec = newVerticalRec(map[string]corev1.ResourceList{
					"app": {
						corev1.ResourceCPU:    resource.MustParse(cpu),
						corev1.ResourceMemory: resource.MustParse(mem),
					},
				}, 0.9)
			} else {
				vRec = newVerticalRec(map[string]corev1.ResourceList{}, 0.9)
			}

			currentState := newCurrentState(tt.currentReplicas, currentResources)
			recommendations := newRecommendations(hRec, vRec)

			decision, err := coord.Coordinate(ctx, ha, currentState, recommendations, newTestMetrics(0.7, 0.7))
			if err != nil {
				t.Fatalf("Coordinate failed: %v", err)
			}

			if decision.HorizontalAction.ShouldScale != tt.expectHorizontalScale {
				t.Errorf("HorizontalAction.ShouldScale = %v, want %v", decision.HorizontalAction.ShouldScale, tt.expectHorizontalScale)
			}
			if decision.VerticalAction.ShouldScale != tt.expectVerticalScale {
				t.Errorf("VerticalAction.ShouldScale = %v, want %v", decision.VerticalAction.ShouldScale, tt.expectVerticalScale)
			}
		})
	}
}

// TestVerticalFirstStrategy tests the VerticalFirst coordination strategy
func TestVerticalFirstStrategy(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name                  string
		cpuUtilization        float64
		memUtilization        float64
		verticalChange        bool
		horizontalReplicas    int32
		currentReplicas       int32
		expectHorizontalScale bool
		expectVerticalScale   bool
	}{
		{
			name:                  "high utilization - prefer vertical",
			cpuUtilization:        0.95,
			memUtilization:        0.92,
			verticalChange:        true,
			horizontalReplicas:    5,
			currentReplicas:       3,
			expectHorizontalScale: false,
			expectVerticalScale:   true,
		},
		{
			name:                  "low utilization - allow horizontal",
			cpuUtilization:        0.5,
			memUtilization:        0.4,
			verticalChange:        false,
			horizontalReplicas:    5,
			currentReplicas:       3,
			expectHorizontalScale: true,
			expectVerticalScale:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coord := newTestCoordinator()
			ha := newTestHybridAutoscaler(autoscalingv1alpha1.VerticalFirst)
			ha.Status.CurrentReplicas = tt.currentReplicas

			hRec := newHorizontalRec(tt.horizontalReplicas, 0.9)

			var vRec *recommender.VerticalRecommendation
			if tt.verticalChange {
				vRec = newVerticalRec(map[string]corev1.ResourceList{
					"app": {
						corev1.ResourceCPU:    resource.MustParse("200m"),
						corev1.ResourceMemory: resource.MustParse("256Mi"),
					},
				}, 0.9)
			} else {
				vRec = newVerticalRec(map[string]corev1.ResourceList{}, 0.9)
			}

			currentResources := map[string]corev1.ResourceRequirements{
				"app": {
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("100m"),
						corev1.ResourceMemory: resource.MustParse("128Mi"),
					},
				},
			}

			currentState := newCurrentState(tt.currentReplicas, currentResources)
			recommendations := newRecommendations(hRec, vRec)

			decision, err := coord.Coordinate(ctx, ha, currentState, recommendations, newTestMetrics(tt.cpuUtilization, tt.memUtilization))
			if err != nil {
				t.Fatalf("Coordinate failed: %v", err)
			}

			if decision.HorizontalAction.ShouldScale != tt.expectHorizontalScale {
				t.Errorf("HorizontalAction.ShouldScale = %v, want %v (utilization: %.2f/%.2f)",
					decision.HorizontalAction.ShouldScale, tt.expectHorizontalScale, tt.cpuUtilization, tt.memUtilization)
			}
			if decision.VerticalAction.ShouldScale != tt.expectVerticalScale {
				t.Errorf("VerticalAction.ShouldScale = %v, want %v (utilization: %.2f/%.2f)",
					decision.VerticalAction.ShouldScale, tt.expectVerticalScale, tt.cpuUtilization, tt.memUtilization)
			}
		})
	}
}

// TestPredictiveStrategy tests the Predictive coordination strategy (currently falls back to Balanced)
func TestPredictiveStrategy(t *testing.T) {
	ctx := context.Background()
	coord := newTestCoordinator()

	ha := newTestHybridAutoscaler(autoscalingv1alpha1.Predictive)
	ha.Status.CurrentReplicas = 3

	hRec := newHorizontalRec(5, 0.9)
	vRec := newVerticalRec(map[string]corev1.ResourceList{
		"app": {
			corev1.ResourceCPU:    resource.MustParse("200m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}, 0.9)

	currentResources := map[string]corev1.ResourceRequirements{
		"app": {
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
		},
	}

	currentState := newCurrentState(3, currentResources)
	recommendations := newRecommendations(hRec, vRec)

	decision, err := coord.Coordinate(ctx, ha, currentState, recommendations, newTestMetrics(0.7, 0.7))
	if err != nil {
		t.Fatalf("Coordinate failed: %v", err)
	}

	// Predictive currently falls back to Balanced, which should prefer horizontal when both scale up
	if !decision.HorizontalAction.ShouldScale {
		t.Error("Expected horizontal scaling for Predictive strategy (fallback to Balanced)")
	}
}

func TestCoordinateNilRecommendationsNoop(t *testing.T) {
	ctx := context.Background()
	coord := newTestCoordinator()
	ha := newTestHybridAutoscaler(autoscalingv1alpha1.Balanced)
	currentState := newCurrentState(3, map[string]corev1.ResourceRequirements{})

	decision, err := coord.Coordinate(ctx, ha, currentState, nil, nil)
	if err != nil {
		t.Fatalf("Coordinate failed: %v", err)
	}
	if decision.Priority != PriorityNone {
		t.Fatalf("Priority = %q, want %q", decision.Priority, PriorityNone)
	}
	if decision.HorizontalAction.ShouldScale || decision.VerticalAction.ShouldScale {
		t.Fatal("nil recommendations should not produce scaling actions")
	}
}

func TestVerticalFirstNilMetricsDoesNotPanic(t *testing.T) {
	ctx := context.Background()
	coord := newTestCoordinator()
	ha := newTestHybridAutoscaler(autoscalingv1alpha1.VerticalFirst)
	currentState := newCurrentState(3, map[string]corev1.ResourceRequirements{
		"app": {
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
		},
	})
	recommendations := newRecommendations(newHorizontalRec(5, 0.9), newVerticalRec(map[string]corev1.ResourceList{
		"app": {
			corev1.ResourceCPU:    resource.MustParse("200m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}, 0.9))

	decision, err := coord.Coordinate(ctx, ha, currentState, recommendations, nil)
	if err != nil {
		t.Fatalf("Coordinate failed: %v", err)
	}
	if !decision.HorizontalAction.ShouldScale {
		t.Fatal("expected horizontal fallback when metrics are unavailable")
	}
}

// TestCooldownEnforcement tests that cooldown periods prevent rapid scaling
func TestCooldownEnforcement(t *testing.T) {
	ctx := context.Background()
	coord := newTestCoordinator()

	ha := newTestHybridAutoscaler(autoscalingv1alpha1.Balanced)
	ha.Status.CurrentReplicas = 3

	hRec := newHorizontalRec(5, 0.9)
	vRec := newVerticalRec(map[string]corev1.ResourceList{}, 0.9)
	currentResources := map[string]corev1.ResourceRequirements{
		"app": {
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
		},
	}

	currentState := newCurrentState(3, currentResources)
	recommendations := newRecommendations(hRec, vRec)

	// First scaling - should work
	decision1, err := coord.Coordinate(ctx, ha, currentState, recommendations, newTestMetrics(0.7, 0.7))
	if err != nil {
		t.Fatalf("First Coordinate failed: %v", err)
	}
	if !decision1.HorizontalAction.ShouldScale {
		t.Error("First decision should allow scale up")
	}

	// Record the scaling action
	key := ha.Namespace + "/" + ha.Name
	coord.RecordHorizontalScale(key, decision1.HorizontalAction.Direction)
	coord.RecordVerticalScale(key, decision1.VerticalAction.Direction)

	// Immediate second attempt - should be blocked by cooldown
	decision2, err := coord.Coordinate(ctx, ha, currentState, recommendations, newTestMetrics(0.7, 0.7))
	if err != nil {
		t.Fatalf("Second Coordinate failed: %v", err)
	}
	if decision2.HorizontalAction.ShouldScale {
		t.Error("Second decision should be blocked by cooldown")
	}
}

// TestRecordScaling tests that scaling events are recorded correctly
func TestRecordScaling(t *testing.T) {
	coord := newTestCoordinator()
	ha := newTestHybridAutoscaler(autoscalingv1alpha1.Balanced)

	decision := &Decision{
		HorizontalAction: HorizontalAction{
			ShouldScale: true,
			Direction:   ScaleUp,
		},
		VerticalAction: VerticalAction{
			ShouldScale: true,
			Direction:   ScaleUp,
		},
		Priority: PriorityHorizontal,
	}

	key := ha.Namespace + "/" + ha.Name
	coord.RecordHorizontalScale(key, decision.HorizontalAction.Direction)
	coord.RecordVerticalScale(key, decision.VerticalAction.Direction)

	// Check that times were recorded
	coord.cooldownTracker.mu.RLock()
	_, hasHScaleUp := coord.cooldownTracker.lastHorizontalScaleUp[key]
	_, hasVScaleUp := coord.cooldownTracker.lastVerticalScaleUp[key]
	coord.cooldownTracker.mu.RUnlock()

	if !hasHScaleUp {
		t.Error("Horizontal scale up time should be recorded")
	}
	if !hasVScaleUp {
		t.Error("Vertical scale up time should be recorded")
	}
}
