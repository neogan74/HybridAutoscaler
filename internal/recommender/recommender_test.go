package recommender

import (
	"context"
	"testing"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	autoscalingv1alpha1 "github.com/neogan74/hybridautoscaler/api/v1alpha1"
	"github.com/neogan74/hybridautoscaler/internal/metrics"
)

func int32Ref(v int32) *int32 {
	return &v
}

func scalingModeRef(v autoscalingv1alpha1.ContainerScalingMode) *autoscalingv1alpha1.ContainerScalingMode {
	return &v
}

func testHybridAutoscaler() *autoscalingv1alpha1.HybridAutoscaler {
	return &autoscalingv1alpha1.HybridAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-ha",
			Namespace: "default",
		},
		Spec: autoscalingv1alpha1.HybridAutoscalerSpec{
			TargetRef: autoscalingv1alpha1.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "app",
			},
			Horizontal: autoscalingv1alpha1.HorizontalSpec{
				MinReplicas: int32Ref(2),
				MaxReplicas: 10,
			},
			Vertical: &autoscalingv1alpha1.VerticalSpec{},
		},
	}
}

func testCollectedMetrics(cpuUtil, memoryUtil float64) *metrics.CollectedMetrics {
	return &metrics.CollectedMetrics{
		Timestamp:       time.Now(),
		TargetNamespace: "default",
		TargetName:      "app",
		TargetKind:      "Deployment",
		Pods: []*metrics.PodMetricsData{
			{
				Name:      "app-1",
				Namespace: "default",
				Containers: map[string]*metrics.ContainerMetrics{
					"app": {
						Name: "app",
						CPU: metrics.CPUMetrics{
							UsageMillicores:    500,
							RequestMillicores:  400,
							LimitMillicores:    1000,
							UtilizationPercent: cpuUtil,
						},
						Memory: metrics.MemoryMetrics{
							UsageBytes:         768 * 1024 * 1024,
							WorkingSetBytes:    700 * 1024 * 1024,
							RequestBytes:       512 * 1024 * 1024,
							LimitBytes:         1024 * 1024 * 1024,
							UtilizationPercent: memoryUtil,
						},
					},
				},
				Timestamp: time.Now(),
			},
		},
		Aggregated: &metrics.AggregatedMetrics{
			TotalPods:                 3,
			ReadyPods:                 3,
			AverageCPUUtilization:     cpuUtil,
			AverageMemoryUtilization:  memoryUtil,
			TotalCPUMillicores:        1500,
			TotalMemoryBytes:          2304 * 1024 * 1024,
			TotalCPURequestMillicores: 1200,
			TotalMemoryRequestBytes:   1536 * 1024 * 1024,
			ContainerMetrics: map[string]*metrics.ContainerAggregatedMetrics{
				"app": {
					ContainerName:            "app",
					AverageCPUUtilization:    cpuUtil,
					AverageMemoryUtilization: memoryUtil,
					CPUP95Millicores:         900,
					CPUP99Millicores:         1000,
					MemoryP95Bytes:           900 * 1024 * 1024,
					MemoryP99Bytes:           1000 * 1024 * 1024,
					CurrentRequestCPU:        resource.MustParse("500m"),
					CurrentRequestMemory:     resource.MustParse("512Mi"),
					CurrentLimitCPU:          resource.MustParse("1000m"),
					CurrentLimitMemory:       resource.MustParse("1Gi"),
				},
			},
		},
		CustomMetrics: make(map[string]float64),
	}
}

func TestGenerateRecommendationsDefaultCPUHorizontal(t *testing.T) {
	r := NewRecommender(DefaultRecommenderConfig())
	ha := testHybridAutoscaler()
	collected := testCollectedMetrics(140, 70)

	rec, err := r.GenerateRecommendations(context.Background(), ha, collected, 3)
	if err != nil {
		t.Fatalf("GenerateRecommendations returned error: %v", err)
	}
	if rec.Horizontal == nil {
		t.Fatal("Horizontal recommendation is nil")
	}
	if rec.Horizontal.DesiredReplicas != 6 {
		t.Fatalf("DesiredReplicas = %d, want 6", rec.Horizontal.DesiredReplicas)
	}
	if rec.Horizontal.Metric != "cpu" {
		t.Fatalf("Metric = %q, want cpu", rec.Horizontal.Metric)
	}
	if rec.Horizontal.Confidence != 1 {
		t.Fatalf("Confidence = %v, want 1", rec.Horizontal.Confidence)
	}
}

func TestGenerateRecommendationsNilMetricsReturnsError(t *testing.T) {
	r := NewRecommender(DefaultRecommenderConfig())
	rec, err := r.GenerateRecommendations(context.Background(), testHybridAutoscaler(), nil, 3)
	if err == nil {
		t.Fatal("expected error for nil metrics")
	}
	if rec != nil {
		t.Fatalf("recommendations = %#v, want nil", rec)
	}
}

func TestGenerateHorizontalRecommendationChoosesMetricWithMostReplicas(t *testing.T) {
	r := NewRecommender(DefaultRecommenderConfig())
	ha := testHybridAutoscaler()
	ha.Spec.Horizontal.Metrics = []autoscalingv2.MetricSpec{
		{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name: corev1.ResourceCPU,
				Target: autoscalingv2.MetricTarget{
					Type:               autoscalingv2.UtilizationMetricType,
					AverageUtilization: int32Ref(80),
				},
			},
		},
		{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name: corev1.ResourceMemory,
				Target: autoscalingv2.MetricTarget{
					Type:               autoscalingv2.UtilizationMetricType,
					AverageUtilization: int32Ref(60),
				},
			},
		},
	}
	collected := testCollectedMetrics(120, 180)

	rec, err := r.generateHorizontalRecommendation(context.Background(), ha, collected, 3)
	if err != nil {
		t.Fatalf("generateHorizontalRecommendation returned error: %v", err)
	}
	if rec.DesiredReplicas != 9 {
		t.Fatalf("DesiredReplicas = %d, want 9", rec.DesiredReplicas)
	}
	if rec.Metric != "memory" {
		t.Fatalf("Metric = %q, want memory", rec.Metric)
	}
}

func TestGenerateHorizontalRecommendationBoundsReplicas(t *testing.T) {
	r := NewRecommender(DefaultRecommenderConfig())
	ha := testHybridAutoscaler()
	collected := testCollectedMetrics(1000, 70)

	rec, err := r.generateHorizontalRecommendation(context.Background(), ha, collected, 3)
	if err != nil {
		t.Fatalf("generateHorizontalRecommendation returned error: %v", err)
	}
	if rec.DesiredReplicas != ha.Spec.Horizontal.MaxReplicas {
		t.Fatalf("DesiredReplicas = %d, want max %d", rec.DesiredReplicas, ha.Spec.Horizontal.MaxReplicas)
	}

	collected.Aggregated.AverageCPUUtilization = 1
	rec, err = r.generateHorizontalRecommendation(context.Background(), ha, collected, 3)
	if err != nil {
		t.Fatalf("generateHorizontalRecommendation returned error: %v", err)
	}
	if rec.DesiredReplicas != *ha.Spec.Horizontal.MinReplicas {
		t.Fatalf("DesiredReplicas = %d, want min %d", rec.DesiredReplicas, *ha.Spec.Horizontal.MinReplicas)
	}
}

func TestGenerateVerticalRecommendationAppliesContainerPolicyBounds(t *testing.T) {
	r := NewRecommender(DefaultRecommenderConfig())
	ha := testHybridAutoscaler()
	ha.Spec.Vertical.ResourcePolicy = &autoscalingv1alpha1.ResourcePolicy{
		ContainerPolicies: []autoscalingv1alpha1.ContainerResourcePolicy{
			{
				ContainerName: "app",
				MinAllowed: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1200m"),
					corev1.ResourceMemory: resource.MustParse("1400Mi"),
				},
				MaxAllowed: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1500m"),
					corev1.ResourceMemory: resource.MustParse("2Gi"),
				},
			},
		},
	}

	rec, err := r.GenerateRecommendations(context.Background(), ha, testCollectedMetrics(100, 100), 3)
	if err != nil {
		t.Fatalf("GenerateRecommendations returned error: %v", err)
	}
	containerRec := rec.Vertical.ContainerRecommendations["app"]
	if containerRec == nil {
		t.Fatal("app container recommendation is nil")
	}
	if got := containerRec.Target.Cpu().MilliValue(); got != 1200 {
		t.Fatalf("CPU target = %dm, want 1200m", got)
	}
	expectedMemoryLower := resource.MustParse("1400Mi")
	if got := containerRec.LowerBound.Memory().Value(); got != expectedMemoryLower.Value() {
		t.Fatalf("memory lower bound = %d, want %d", got, expectedMemoryLower.Value())
	}
	if got := containerRec.UpperBound.Cpu().MilliValue(); got != 1500 {
		t.Fatalf("CPU upper bound = %dm, want 1500m", got)
	}
}

func TestGenerateVerticalRecommendationSkipsDisabledContainer(t *testing.T) {
	r := NewRecommender(DefaultRecommenderConfig())
	ha := testHybridAutoscaler()
	ha.Spec.Vertical.ResourcePolicy = &autoscalingv1alpha1.ResourcePolicy{
		ContainerPolicies: []autoscalingv1alpha1.ContainerResourcePolicy{
			{
				ContainerName: "app",
				Mode:          scalingModeRef(autoscalingv1alpha1.ContainerScalingModeOff),
			},
		},
	}

	rec, err := r.GenerateRecommendations(context.Background(), ha, testCollectedMetrics(100, 100), 3)
	if err != nil {
		t.Fatalf("GenerateRecommendations returned error: %v", err)
	}
	if len(rec.Vertical.ContainerRecommendations) != 0 {
		t.Fatalf("ContainerRecommendations length = %d, want 0", len(rec.Vertical.ContainerRecommendations))
	}
}

func TestAddMetricsPrunesOldSamples(t *testing.T) {
	config := DefaultRecommenderConfig()
	config.HistoryLength = time.Hour
	r := NewRecommender(config)
	r.history.CPUSamples["app"] = &TimeSeries{
		DecayHalfLife: config.CPUHistogramDecayHalfLife,
		Samples: []Sample{
			{Value: 100, Timestamp: time.Now().Add(-2 * time.Hour), Weight: 1},
			{Value: 200, Timestamp: time.Now().Add(-30 * time.Minute), Weight: 1},
		},
	}
	r.history.MemorySamples["app"] = &TimeSeries{
		DecayHalfLife: config.MemoryHistogramDecayHalfLife,
		Samples: []Sample{
			{Value: 100, Timestamp: time.Now().Add(-2 * time.Hour), Weight: 1},
			{Value: 200, Timestamp: time.Now().Add(-30 * time.Minute), Weight: 1},
		},
	}

	r.AddMetrics(testCollectedMetrics(70, 70))

	if got := len(r.history.CPUSamples["app"].Samples); got != 2 {
		t.Fatalf("CPU sample count = %d, want 2", got)
	}
	if got := len(r.history.MemorySamples["app"].Samples); got != 2 {
		t.Fatalf("memory sample count = %d, want 2", got)
	}
}
