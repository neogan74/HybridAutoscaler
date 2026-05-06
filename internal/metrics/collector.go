package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	autoscalingv1alpha1 "github.com/neogan74/hybridautoscaler/api/v1alpha1"
)

// Collector collects metrics from various sources
type Collector struct {
	client        client.Client
	kubeClient    kubernetes.Interface
	metricsClient metricsclient.Interface

	// Cache for metrics to reduce API calls
	cache     *MetricsCache
	cacheTTL  time.Duration
	cacheLock sync.RWMutex
}

// MetricsCache caches collected metrics
type MetricsCache struct {
	PodMetrics map[types.NamespacedName]*PodMetricsData
	LastUpdate time.Time
}

// PodMetricsData contains metrics for a single pod
type PodMetricsData struct {
	Name       string
	Namespace  string
	Containers map[string]*ContainerMetrics
	Timestamp  time.Time
}

// ContainerMetrics contains metrics for a single container
type ContainerMetrics struct {
	Name   string
	CPU    CPUMetrics
	Memory MemoryMetrics
}

// CPUMetrics contains CPU-related metrics
type CPUMetrics struct {
	// Current usage in millicores
	UsageMillicores int64

	// Request in millicores
	RequestMillicores int64

	// Limit in millicores
	LimitMillicores int64

	// Utilization as a percentage of request
	UtilizationPercent float64

	// Throttling information
	ThrottledPeriods   int64
	ThrottledTimeNanos int64
}

// MemoryMetrics contains memory-related metrics
type MemoryMetrics struct {
	// Current usage in bytes
	UsageBytes int64

	// Working set in bytes
	WorkingSetBytes int64

	// Request in bytes
	RequestBytes int64

	// Limit in bytes
	LimitBytes int64

	// Utilization as a percentage of request
	UtilizationPercent float64

	// OOM kill count
	OOMKillCount int64
}

// CollectedMetrics contains all collected metrics for a target
type CollectedMetrics struct {
	// Timestamp when metrics were collected
	Timestamp time.Time

	// Target information
	TargetNamespace string
	TargetName      string
	TargetKind      string

	// Pod metrics
	Pods []*PodMetricsData

	// Aggregated metrics
	Aggregated *AggregatedMetrics

	// Custom metrics (from Prometheus or custom metrics API)
	CustomMetrics map[string]float64
}

// AggregatedMetrics contains aggregated metrics across all pods
type AggregatedMetrics struct {
	// Total pods
	TotalPods int32

	// Ready pods
	ReadyPods int32

	// Average CPU utilization across all pods (percentage of request)
	AverageCPUUtilization float64

	// Average memory utilization across all pods (percentage of request)
	AverageMemoryUtilization float64

	// Total CPU usage in millicores
	TotalCPUMillicores int64

	// Total memory usage in bytes
	TotalMemoryBytes int64

	// Total CPU request in millicores
	TotalCPURequestMillicores int64

	// Total memory request in bytes
	TotalMemoryRequestBytes int64

	// Per-container aggregated metrics
	ContainerMetrics map[string]*ContainerAggregatedMetrics
}

// ContainerAggregatedMetrics contains aggregated metrics for a specific container across pods
type ContainerAggregatedMetrics struct {
	ContainerName string

	// Average CPU utilization (percentage of request)
	AverageCPUUtilization float64

	// Average memory utilization (percentage of request)
	AverageMemoryUtilization float64

	// P50, P95, P99 CPU usage
	CPUP50Millicores int64
	CPUP95Millicores int64
	CPUP99Millicores int64

	// P50, P95, P99 memory usage
	MemoryP50Bytes int64
	MemoryP95Bytes int64
	MemoryP99Bytes int64

	// Current resource specs (from first pod)
	CurrentRequestCPU    resource.Quantity
	CurrentRequestMemory resource.Quantity
	CurrentLimitCPU      resource.Quantity
	CurrentLimitMemory   resource.Quantity
}

// NewCollector creates a new metrics collector
func NewCollector(
	client client.Client,
	kubeClient kubernetes.Interface,
	metricsClient metricsclient.Interface,
) *Collector {
	return &Collector{
		client:        client,
		kubeClient:    kubeClient,
		metricsClient: metricsClient,
		cache: &MetricsCache{
			PodMetrics: make(map[types.NamespacedName]*PodMetricsData),
		},
		cacheTTL: 15 * time.Second,
	}
}

// Collect gathers metrics for the target workload
func (c *Collector) Collect(ctx context.Context, ha *autoscalingv1alpha1.HybridAutoscaler) (*CollectedMetrics, error) {
	logger := log.FromContext(ctx)

	// Get target pods
	pods, err := c.getTargetPods(ctx, ha)
	if err != nil {
		return nil, fmt.Errorf("failed to get target pods: %w", err)
	}

	if len(pods) == 0 {
		logger.Info("No pods found for target")
		return &CollectedMetrics{
			Timestamp:       time.Now(),
			TargetNamespace: ha.Namespace,
			TargetName:      ha.Spec.TargetRef.Name,
			TargetKind:      ha.Spec.TargetRef.Kind,
			Pods:            []*PodMetricsData{},
			Aggregated:      &AggregatedMetrics{},
			CustomMetrics:   make(map[string]float64),
		}, nil
	}

	// Collect pod metrics from metrics server
	podMetrics, err := c.collectPodMetrics(ctx, ha.Namespace, pods)
	if err != nil {
		logger.Error(err, "Failed to collect pod metrics, continuing with partial data")
		// Don't fail completely, we might have cached data
	}

	// Enrich with resource specs from pod specs
	c.enrichWithResourceSpecs(pods, podMetrics)

	// Aggregate metrics
	aggregated := c.aggregateMetrics(pods, podMetrics)

	// Collect custom metrics if configured
	customMetrics := make(map[string]float64)
	for _, metric := range ha.Spec.Horizontal.Metrics {
		if metric.Type == "External" || metric.Type == "Pods" || metric.Type == "Object" {
			value, err := c.collectCustomMetric(ctx, ha, metric)
			if err != nil {
				logger.Error(err, "Failed to collect custom metric", "metric", metric)
				continue
			}
			customMetrics[getMetricKey(metric)] = value
		}
	}

	return &CollectedMetrics{
		Timestamp:       time.Now(),
		TargetNamespace: ha.Namespace,
		TargetName:      ha.Spec.TargetRef.Name,
		TargetKind:      ha.Spec.TargetRef.Kind,
		Pods:            podMetrics,
		Aggregated:      aggregated,
		CustomMetrics:   customMetrics,
	}, nil
}

// getTargetPods returns pods managed by the target workload
func (c *Collector) getTargetPods(ctx context.Context, ha *autoscalingv1alpha1.HybridAutoscaler) ([]corev1.Pod, error) {
	// Get the target workload to find its selector
	selector, err := c.getWorkloadSelector(ctx, ha)
	if err != nil {
		return nil, fmt.Errorf("failed to get workload selector: %w", err)
	}

	// List pods matching the selector
	podList := &corev1.PodList{}
	listOpts := &client.ListOptions{
		Namespace:     ha.Namespace,
		LabelSelector: selector,
	}

	if err := c.client.List(ctx, podList, listOpts); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	// Filter to running pods only
	var runningPods []corev1.Pod
	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil {
			runningPods = append(runningPods, pod)
		}
	}

	return runningPods, nil
}

// getWorkloadSelector returns the label selector for the target workload
func (c *Collector) getWorkloadSelector(ctx context.Context, ha *autoscalingv1alpha1.HybridAutoscaler) (labels.Selector, error) {
	switch ha.Spec.TargetRef.Kind {
	case "Deployment":
		return c.getDeploymentSelector(ctx, ha.Namespace, ha.Spec.TargetRef.Name)
	case "StatefulSet":
		return c.getStatefulSetSelector(ctx, ha.Namespace, ha.Spec.TargetRef.Name)
	case "ReplicaSet":
		return c.getReplicaSetSelector(ctx, ha.Namespace, ha.Spec.TargetRef.Name)
	default:
		return nil, fmt.Errorf("unsupported target kind: %s", ha.Spec.TargetRef.Kind)
	}
}

func (c *Collector) getDeploymentSelector(ctx context.Context, namespace, name string) (labels.Selector, error) {
	deployment, err := c.kubeClient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
}

func (c *Collector) getStatefulSetSelector(ctx context.Context, namespace, name string) (labels.Selector, error) {
	sts, err := c.kubeClient.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return metav1.LabelSelectorAsSelector(sts.Spec.Selector)
}

func (c *Collector) getReplicaSetSelector(ctx context.Context, namespace, name string) (labels.Selector, error) {
	rs, err := c.kubeClient.AppsV1().ReplicaSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return metav1.LabelSelectorAsSelector(rs.Spec.Selector)
}

// collectPodMetrics collects metrics for pods from the metrics server
func (c *Collector) collectPodMetrics(ctx context.Context, namespace string, pods []corev1.Pod) ([]*PodMetricsData, error) {
	// Get metrics from metrics server
	podMetricsList, err := c.metricsClient.MetricsV1beta1().PodMetricses(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list pod metrics: %w", err)
	}

	// Create a map for quick lookup
	metricsMap := make(map[string]*metricsv1beta1.PodMetrics)
	for i := range podMetricsList.Items {
		pm := &podMetricsList.Items[i]
		metricsMap[pm.Name] = pm
	}

	// Build metrics data for each pod
	result := make([]*PodMetricsData, 0, len(pods))
	for _, pod := range pods {
		podMetrics, exists := metricsMap[pod.Name]
		if !exists {
			continue
		}

		podData := &PodMetricsData{
			Name:       pod.Name,
			Namespace:  pod.Namespace,
			Containers: make(map[string]*ContainerMetrics),
			Timestamp:  podMetrics.Timestamp.Time,
		}

		for _, container := range podMetrics.Containers {
			cpuUsage := container.Usage.Cpu().MilliValue()
			memUsage := container.Usage.Memory().Value()

			podData.Containers[container.Name] = &ContainerMetrics{
				Name: container.Name,
				CPU: CPUMetrics{
					UsageMillicores: cpuUsage,
				},
				Memory: MemoryMetrics{
					UsageBytes:      memUsage,
					WorkingSetBytes: memUsage, // Approximation
				},
			}
		}

		result = append(result, podData)
	}

	return result, nil
}

// enrichWithResourceSpecs adds resource requests/limits from pod specs
func (c *Collector) enrichWithResourceSpecs(pods []corev1.Pod, podMetrics []*PodMetricsData) {
	podSpecMap := make(map[string]*corev1.Pod)
	for i := range pods {
		podSpecMap[pods[i].Name] = &pods[i]
	}

	for _, pm := range podMetrics {
		pod, exists := podSpecMap[pm.Name]
		if !exists {
			continue
		}

		for _, container := range pod.Spec.Containers {
			cm, exists := pm.Containers[container.Name]
			if !exists {
				continue
			}

			// CPU
			if req, ok := container.Resources.Requests[corev1.ResourceCPU]; ok {
				cm.CPU.RequestMillicores = req.MilliValue()
				if cm.CPU.RequestMillicores > 0 {
					cm.CPU.UtilizationPercent = float64(cm.CPU.UsageMillicores) / float64(cm.CPU.RequestMillicores) * 100
				}
			}
			if lim, ok := container.Resources.Limits[corev1.ResourceCPU]; ok {
				cm.CPU.LimitMillicores = lim.MilliValue()
			}

			// Memory
			if req, ok := container.Resources.Requests[corev1.ResourceMemory]; ok {
				cm.Memory.RequestBytes = req.Value()
				if cm.Memory.RequestBytes > 0 {
					cm.Memory.UtilizationPercent = float64(cm.Memory.UsageBytes) / float64(cm.Memory.RequestBytes) * 100
				}
			}
			if lim, ok := container.Resources.Limits[corev1.ResourceMemory]; ok {
				cm.Memory.LimitBytes = lim.Value()
			}
		}
	}
}

// aggregateMetrics calculates aggregated metrics across all pods
func (c *Collector) aggregateMetrics(pods []corev1.Pod, podMetrics []*PodMetricsData) *AggregatedMetrics {
	agg := &AggregatedMetrics{
		TotalPods:        int32(len(pods)),
		ContainerMetrics: make(map[string]*ContainerAggregatedMetrics),
	}

	// Count ready pods
	for _, pod := range pods {
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				agg.ReadyPods++
				break
			}
		}
	}

	if len(podMetrics) == 0 {
		return agg
	}

	// Aggregate per-container metrics
	containerCPUUsages := make(map[string][]int64)
	containerMemUsages := make(map[string][]int64)
	containerCPUUtils := make(map[string][]float64)
	containerMemUtils := make(map[string][]float64)

	for _, pm := range podMetrics {
		for containerName, cm := range pm.Containers {
			containerCPUUsages[containerName] = append(containerCPUUsages[containerName], cm.CPU.UsageMillicores)
			containerMemUsages[containerName] = append(containerMemUsages[containerName], cm.Memory.UsageBytes)

			if cm.CPU.RequestMillicores > 0 {
				containerCPUUtils[containerName] = append(containerCPUUtils[containerName], cm.CPU.UtilizationPercent)
			}
			if cm.Memory.RequestBytes > 0 {
				containerMemUtils[containerName] = append(containerMemUtils[containerName], cm.Memory.UtilizationPercent)
			}

			agg.TotalCPUMillicores += cm.CPU.UsageMillicores
			agg.TotalMemoryBytes += cm.Memory.UsageBytes
			agg.TotalCPURequestMillicores += cm.CPU.RequestMillicores
			agg.TotalMemoryRequestBytes += cm.Memory.RequestBytes
		}
	}

	// Calculate aggregated container metrics
	for containerName := range containerCPUUsages {
		cam := &ContainerAggregatedMetrics{
			ContainerName: containerName,
		}

		// CPU percentiles
		cpuUsages := containerCPUUsages[containerName]
		if len(cpuUsages) > 0 {
			cam.CPUP50Millicores = percentile(cpuUsages, 50)
			cam.CPUP95Millicores = percentile(cpuUsages, 95)
			cam.CPUP99Millicores = percentile(cpuUsages, 99)
		}

		// Memory percentiles
		memUsages := containerMemUsages[containerName]
		if len(memUsages) > 0 {
			cam.MemoryP50Bytes = percentile(memUsages, 50)
			cam.MemoryP95Bytes = percentile(memUsages, 95)
			cam.MemoryP99Bytes = percentile(memUsages, 99)
		}

		// Average utilization
		if utils, ok := containerCPUUtils[containerName]; ok && len(utils) > 0 {
			cam.AverageCPUUtilization = average(utils)
		}
		if utils, ok := containerMemUtils[containerName]; ok && len(utils) > 0 {
			cam.AverageMemoryUtilization = average(utils)
		}

		// Get current resource specs from first pod
		if len(podMetrics) > 0 {
			if cm, ok := podMetrics[0].Containers[containerName]; ok {
				cam.CurrentRequestCPU = *resource.NewMilliQuantity(cm.CPU.RequestMillicores, resource.DecimalSI)
				cam.CurrentRequestMemory = *resource.NewQuantity(cm.Memory.RequestBytes, resource.BinarySI)
				cam.CurrentLimitCPU = *resource.NewMilliQuantity(cm.CPU.LimitMillicores, resource.DecimalSI)
				cam.CurrentLimitMemory = *resource.NewQuantity(cm.Memory.LimitBytes, resource.BinarySI)
			}
		}

		agg.ContainerMetrics[containerName] = cam
	}

	// Overall average utilization
	if agg.TotalCPURequestMillicores > 0 {
		agg.AverageCPUUtilization = float64(agg.TotalCPUMillicores) / float64(agg.TotalCPURequestMillicores) * 100
	}
	if agg.TotalMemoryRequestBytes > 0 {
		agg.AverageMemoryUtilization = float64(agg.TotalMemoryBytes) / float64(agg.TotalMemoryRequestBytes) * 100
	}

	return agg
}

// collectCustomMetric collects a custom metric value
func (c *Collector) collectCustomMetric(ctx context.Context, ha *autoscalingv1alpha1.HybridAutoscaler, metric interface{}) (float64, error) {
	// TODO: Implement custom metrics collection via:
	// - Custom Metrics API (metrics.k8s.io)
	// - External Metrics API (external.metrics.k8s.io)
	// - Direct Prometheus queries

	return 0, fmt.Errorf("custom metrics collection not yet implemented")
}

// getMetricKey returns a unique key for a metric
func getMetricKey(metric interface{}) string {
	// TODO: Implement proper metric key generation
	return fmt.Sprintf("%v", metric)
}

// Helper functions

func percentile(data []int64, p int) int64 {
	if len(data) == 0 {
		return 0
	}

	// Simple implementation - for production use a proper statistical library
	sorted := make([]int64, len(data))
	copy(sorted, data)
	sortInt64s(sorted)

	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func sortInt64s(data []int64) {
	// Simple bubble sort - use sort.Slice in production
	for i := 0; i < len(data)-1; i++ {
		for j := 0; j < len(data)-i-1; j++ {
			if data[j] > data[j+1] {
				data[j], data[j+1] = data[j+1], data[j]
			}
		}
	}
}

func average(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range data {
		sum += v
	}
	return sum / float64(len(data))
}
