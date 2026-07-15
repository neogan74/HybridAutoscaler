package recommender

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/log"

	autoscalingv1alpha1 "github.com/neogan74/hybridautoscaler/api/v1alpha1"
	"github.com/neogan74/hybridautoscaler/internal/metrics"
)

// Recommender generates scaling recommendations based on metrics
type Recommender struct {
	// Historical data for recommendations
	history     *MetricsHistory
	historyLock sync.RWMutex

	// Configuration
	config RecommenderConfig
}

// RecommenderConfig holds configuration for the recommender
type RecommenderConfig struct {
	// HistoryLength is how much historical data to keep
	HistoryLength time.Duration

	// MinSampleCount is minimum samples needed for a valid recommendation
	MinSampleCount int

	// TargetCPUUtilization is the default target CPU utilization (percentage)
	TargetCPUUtilization float64

	// TargetMemoryUtilization is the default target memory utilization (percentage)
	TargetMemoryUtilization float64

	// SafetyMarginFraction is extra headroom for recommendations (0.0-1.0)
	SafetyMarginFraction float64

	// CPUHistogramDecayHalfLife controls how quickly historical data decays
	CPUHistogramDecayHalfLife time.Duration

	// MemoryHistogramDecayHalfLife controls how quickly historical data decays
	MemoryHistogramDecayHalfLife time.Duration

	// RecommendationMarginFraction is additional margin for recommendations
	RecommendationMarginFraction float64
}

// DefaultRecommenderConfig returns sensible defaults
func DefaultRecommenderConfig() RecommenderConfig {
	return RecommenderConfig{
		HistoryLength:                24 * time.Hour,
		MinSampleCount:               8,
		TargetCPUUtilization:         70.0,
		TargetMemoryUtilization:      80.0,
		SafetyMarginFraction:         0.15,
		CPUHistogramDecayHalfLife:    24 * time.Hour,
		MemoryHistogramDecayHalfLife: 24 * time.Hour,
		RecommendationMarginFraction: 0.15,
	}
}

// MetricsHistory stores historical metrics for analysis
type MetricsHistory struct {
	// Per-container CPU usage samples (millicores)
	CPUSamples map[string]*TimeSeries

	// Per-container memory usage samples (bytes)
	MemorySamples map[string]*TimeSeries

	// OOM events
	OOMEvents []OOMEvent

	// Throttling events
	ThrottlingEvents []ThrottlingEvent
}

// TimeSeries stores time-indexed samples with decay
type TimeSeries struct {
	Samples       []Sample
	DecayHalfLife time.Duration
}

// Sample is a single data point
type Sample struct {
	Value     float64
	Timestamp time.Time
	Weight    float64 // For weighted calculations
}

// OOMEvent records an OOM kill
type OOMEvent struct {
	ContainerName string
	PodName       string
	Timestamp     time.Time
	MemoryAtOOM   int64
}

// ThrottlingEvent records CPU throttling
type ThrottlingEvent struct {
	ContainerName string
	PodName       string
	Timestamp     time.Time
	ThrottledTime time.Duration
}

// Recommendations contains both horizontal and vertical recommendations
type Recommendations struct {
	Horizontal *HorizontalRecommendation
	Vertical   *VerticalRecommendation
}

// HorizontalRecommendation contains horizontal scaling recommendation
type HorizontalRecommendation struct {
	DesiredReplicas int32
	CurrentReplicas int32
	MinReplicas     int32
	MaxReplicas     int32
	Metric          string
	CurrentValue    float64
	TargetValue     float64
	Reason          string
	Confidence      float64
	Timestamp       time.Time
}

// VerticalRecommendation contains vertical scaling recommendation
type VerticalRecommendation struct {
	ContainerRecommendations map[string]*ContainerRecommendation
	Reason                   string
	Confidence               float64
	Timestamp                time.Time
}

// ContainerRecommendation contains recommendations for a single container
type ContainerRecommendation struct {
	ContainerName string

	// Target is the recommended resource request
	Target corev1.ResourceList

	// LowerBound is the minimum recommended request
	LowerBound corev1.ResourceList

	// UpperBound is the maximum recommended request
	UpperBound corev1.ResourceList

	// UncappedTarget is what we'd recommend without policy limits
	UncappedTarget corev1.ResourceList

	// Current values for comparison
	CurrentRequest corev1.ResourceList
	CurrentLimit   corev1.ResourceList
}

// NewRecommender creates a new recommender with the given config
func NewRecommender(config RecommenderConfig) *Recommender {
	return &Recommender{
		config: config,
		history: &MetricsHistory{
			CPUSamples:       make(map[string]*TimeSeries),
			MemorySamples:    make(map[string]*TimeSeries),
			OOMEvents:        []OOMEvent{},
			ThrottlingEvents: []ThrottlingEvent{},
		},
	}
}

// AddMetrics adds collected metrics to history
func (r *Recommender) AddMetrics(collected *metrics.CollectedMetrics) {
	if collected == nil {
		return
	}

	r.historyLock.Lock()
	defer r.historyLock.Unlock()

	for _, pod := range collected.Pods {
		for containerName, cm := range pod.Containers {
			// CPU samples
			if _, exists := r.history.CPUSamples[containerName]; !exists {
				r.history.CPUSamples[containerName] = &TimeSeries{
					DecayHalfLife: r.config.CPUHistogramDecayHalfLife,
				}
			}
			r.history.CPUSamples[containerName].Samples = append(
				r.history.CPUSamples[containerName].Samples,
				Sample{
					Value:     float64(cm.CPU.UsageMillicores),
					Timestamp: collected.Timestamp,
					Weight:    1.0,
				},
			)

			// Memory samples
			if _, exists := r.history.MemorySamples[containerName]; !exists {
				r.history.MemorySamples[containerName] = &TimeSeries{
					DecayHalfLife: r.config.MemoryHistogramDecayHalfLife,
				}
			}
			r.history.MemorySamples[containerName].Samples = append(
				r.history.MemorySamples[containerName].Samples,
				Sample{
					Value:     float64(cm.Memory.WorkingSetBytes),
					Timestamp: collected.Timestamp,
					Weight:    1.0,
				},
			)
		}
	}

	// Prune old samples
	r.pruneHistory()
}

// pruneHistory removes samples older than HistoryLength
func (r *Recommender) pruneHistory() {
	cutoff := time.Now().Add(-r.config.HistoryLength)

	for _, ts := range r.history.CPUSamples {
		ts.Samples = filterSamplesAfter(ts.Samples, cutoff)
	}
	for _, ts := range r.history.MemorySamples {
		ts.Samples = filterSamplesAfter(ts.Samples, cutoff)
	}
}

func filterSamplesAfter(samples []Sample, after time.Time) []Sample {
	result := make([]Sample, 0, len(samples))
	for _, s := range samples {
		if s.Timestamp.After(after) {
			result = append(result, s)
		}
	}
	return result
}

// GenerateRecommendations produces both horizontal and vertical recommendations
func (r *Recommender) GenerateRecommendations(
	ctx context.Context,
	ha *autoscalingv1alpha1.HybridAutoscaler,
	collected *metrics.CollectedMetrics,
	currentReplicas int32,
) (*Recommendations, error) {
	logger := log.FromContext(ctx)

	if collected == nil {
		return nil, fmt.Errorf("collected metrics are nil")
	}
	if collected.Aggregated == nil {
		return nil, fmt.Errorf("aggregated metrics are nil")
	}

	// Add current metrics to history
	r.AddMetrics(collected)

	// Generate horizontal recommendation
	hRec, err := r.generateHorizontalRecommendation(ctx, ha, collected, currentReplicas)
	if err != nil {
		logger.Error(err, "Failed to generate horizontal recommendation")
	}

	// Generate vertical recommendation
	vRec, err := r.generateVerticalRecommendation(ctx, ha, collected)
	if err != nil {
		logger.Error(err, "Failed to generate vertical recommendation")
	}

	return &Recommendations{
		Horizontal: hRec,
		Vertical:   vRec,
	}, nil
}

// generateHorizontalRecommendation generates a horizontal scaling recommendation
func (r *Recommender) generateHorizontalRecommendation(
	ctx context.Context,
	ha *autoscalingv1alpha1.HybridAutoscaler,
	collected *metrics.CollectedMetrics,
	currentReplicas int32,
) (*HorizontalRecommendation, error) {
	logger := log.FromContext(ctx)

	minReplicas := int32(1)
	if ha.Spec.Horizontal.MinReplicas != nil {
		minReplicas = *ha.Spec.Horizontal.MinReplicas
	}
	maxReplicas := ha.Spec.Horizontal.MaxReplicas

	rec := &HorizontalRecommendation{
		CurrentReplicas: currentReplicas,
		MinReplicas:     minReplicas,
		MaxReplicas:     maxReplicas,
		Timestamp:       time.Now(),
		DesiredReplicas: currentReplicas, // Default to no change
	}

	if len(ha.Spec.Horizontal.Metrics) == 0 {
		// Default to CPU-based scaling
		return r.generateCPUBasedHorizontalRec(collected, currentReplicas, minReplicas, maxReplicas, r.config.TargetCPUUtilization)
	}

	// Process each metric and find the one requiring the most replicas
	var maxDesiredReplicas int32 = currentReplicas
	var primaryMetric string
	var primaryCurrentValue, primaryTargetValue float64

	for _, metricSpec := range ha.Spec.Horizontal.Metrics {
		var desiredReplicas int32
		var currentValue, targetValue float64
		var metricName string

		switch metricSpec.Type {
		case autoscalingv2.ResourceMetricSourceType:
			if metricSpec.Resource == nil {
				continue
			}
			metricName = string(metricSpec.Resource.Name)

			switch metricSpec.Resource.Name {
			case corev1.ResourceCPU:
				targetUtil := r.config.TargetCPUUtilization
				if metricSpec.Resource.Target.AverageUtilization != nil {
					targetUtil = float64(*metricSpec.Resource.Target.AverageUtilization)
				}
				currentValue = collected.Aggregated.AverageCPUUtilization
				targetValue = targetUtil
				desiredReplicas = calculateDesiredReplicas(currentReplicas, currentValue, targetValue)

			case corev1.ResourceMemory:
				targetUtil := r.config.TargetMemoryUtilization
				if metricSpec.Resource.Target.AverageUtilization != nil {
					targetUtil = float64(*metricSpec.Resource.Target.AverageUtilization)
				}
				currentValue = collected.Aggregated.AverageMemoryUtilization
				targetValue = targetUtil
				desiredReplicas = calculateDesiredReplicas(currentReplicas, currentValue, targetValue)
			}

		case autoscalingv2.ExternalMetricSourceType:
			if metricSpec.External == nil {
				continue
			}
			metricName = metricSpec.External.Metric.Name
			if val, ok := collected.CustomMetrics[metricName]; ok {
				currentValue = val
				if metricSpec.External.Target.AverageValue != nil {
					targetValue = float64(metricSpec.External.Target.AverageValue.MilliValue()) / 1000
					desiredReplicas = calculateDesiredReplicas(currentReplicas, currentValue, targetValue)
				}
			}

		default:
			logger.Info("Unsupported metric type", "type", metricSpec.Type)
			continue
		}

		if desiredReplicas > maxDesiredReplicas {
			maxDesiredReplicas = desiredReplicas
			primaryMetric = metricName
			primaryCurrentValue = currentValue
			primaryTargetValue = targetValue
		}
	}

	// Apply min/max constraints
	rec.DesiredReplicas = boundReplicas(maxDesiredReplicas, minReplicas, maxReplicas)
	rec.Metric = primaryMetric
	rec.CurrentValue = primaryCurrentValue
	rec.TargetValue = primaryTargetValue
	rec.Confidence = r.calculateHorizontalConfidence(collected)
	rec.Reason = fmt.Sprintf("%s utilization %.1f%% vs target %.1f%%",
		primaryMetric, primaryCurrentValue, primaryTargetValue)

	return rec, nil
}

// generateCPUBasedHorizontalRec generates a CPU-based horizontal recommendation
func (r *Recommender) generateCPUBasedHorizontalRec(
	collected *metrics.CollectedMetrics,
	currentReplicas, minReplicas, maxReplicas int32,
	targetUtilization float64,
) (*HorizontalRecommendation, error) {
	currentUtil := collected.Aggregated.AverageCPUUtilization
	desiredReplicas := calculateDesiredReplicas(currentReplicas, currentUtil, targetUtilization)

	return &HorizontalRecommendation{
		DesiredReplicas: boundReplicas(desiredReplicas, minReplicas, maxReplicas),
		CurrentReplicas: currentReplicas,
		MinReplicas:     minReplicas,
		MaxReplicas:     maxReplicas,
		Metric:          "cpu",
		CurrentValue:    currentUtil,
		TargetValue:     targetUtilization,
		Reason:          fmt.Sprintf("CPU utilization %.1f%% vs target %.1f%%", currentUtil, targetUtilization),
		Confidence:      r.calculateHorizontalConfidence(collected),
		Timestamp:       time.Now(),
	}, nil
}

// generateVerticalRecommendation generates vertical scaling recommendations
func (r *Recommender) generateVerticalRecommendation(
	ctx context.Context,
	ha *autoscalingv1alpha1.HybridAutoscaler,
	collected *metrics.CollectedMetrics,
) (*VerticalRecommendation, error) {
	r.historyLock.RLock()
	defer r.historyLock.RUnlock()

	rec := &VerticalRecommendation{
		ContainerRecommendations: make(map[string]*ContainerRecommendation),
		Timestamp:                time.Now(),
	}

	// Get resource policies
	var policies []autoscalingv1alpha1.ContainerResourcePolicy
	if ha.Spec.Vertical != nil && ha.Spec.Vertical.ResourcePolicy != nil {
		policies = ha.Spec.Vertical.ResourcePolicy.ContainerPolicies
	}

	// Generate recommendations for each container
	for containerName, cam := range collected.Aggregated.ContainerMetrics {
		policy := findContainerPolicy(containerName, policies)

		// Skip if mode is Off
		if policy != nil && policy.Mode != nil && *policy.Mode == autoscalingv1alpha1.ContainerScalingModeOff {
			continue
		}

		containerRec := r.generateContainerRecommendation(containerName, cam, policy)
		if containerRec != nil {
			rec.ContainerRecommendations[containerName] = containerRec
		}
	}

	rec.Confidence = r.calculateVerticalConfidence()
	rec.Reason = "Based on historical resource usage patterns"

	return rec, nil
}

// generateContainerRecommendation generates a recommendation for a single container
func (r *Recommender) generateContainerRecommendation(
	containerName string,
	cam *metrics.ContainerAggregatedMetrics,
	policy *autoscalingv1alpha1.ContainerResourcePolicy,
) *ContainerRecommendation {
	rec := &ContainerRecommendation{
		ContainerName:  containerName,
		Target:         make(corev1.ResourceList),
		LowerBound:     make(corev1.ResourceList),
		UpperBound:     make(corev1.ResourceList),
		UncappedTarget: make(corev1.ResourceList),
		CurrentRequest: make(corev1.ResourceList),
		CurrentLimit:   make(corev1.ResourceList),
	}

	// Store current values
	rec.CurrentRequest[corev1.ResourceCPU] = cam.CurrentRequestCPU
	rec.CurrentRequest[corev1.ResourceMemory] = cam.CurrentRequestMemory
	rec.CurrentLimit[corev1.ResourceCPU] = cam.CurrentLimitCPU
	rec.CurrentLimit[corev1.ResourceMemory] = cam.CurrentLimitMemory

	// Calculate CPU recommendation
	cpuTarget := r.calculateCPURecommendation(containerName, cam)
	rec.UncappedTarget[corev1.ResourceCPU] = *resource.NewMilliQuantity(cpuTarget, resource.DecimalSI)

	// Calculate memory recommendation
	memTarget := r.calculateMemoryRecommendation(containerName, cam)
	rec.UncappedTarget[corev1.ResourceMemory] = *resource.NewQuantity(memTarget, resource.BinarySI)

	// Apply policy constraints
	cpuTarget, cpuLower, cpuUpper := applyPolicyConstraints(cpuTarget, corev1.ResourceCPU, policy)
	memTarget, memLower, memUpper := applyPolicyConstraints(memTarget, corev1.ResourceMemory, policy)

	rec.Target[corev1.ResourceCPU] = *resource.NewMilliQuantity(cpuTarget, resource.DecimalSI)
	rec.Target[corev1.ResourceMemory] = *resource.NewQuantity(memTarget, resource.BinarySI)
	rec.LowerBound[corev1.ResourceCPU] = *resource.NewMilliQuantity(cpuLower, resource.DecimalSI)
	rec.LowerBound[corev1.ResourceMemory] = *resource.NewQuantity(memLower, resource.BinarySI)
	rec.UpperBound[corev1.ResourceCPU] = *resource.NewMilliQuantity(cpuUpper, resource.DecimalSI)
	rec.UpperBound[corev1.ResourceMemory] = *resource.NewQuantity(memUpper, resource.BinarySI)

	return rec
}

// calculateCPURecommendation calculates CPU recommendation based on history
func (r *Recommender) calculateCPURecommendation(containerName string, cam *metrics.ContainerAggregatedMetrics) int64 {
	// Use P95 as the target, with safety margin
	p95 := cam.CPUP95Millicores
	if p95 == 0 {
		// Fall back to current request if no historical data
		return cam.CurrentRequestCPU.MilliValue()
	}

	// Add safety margin
	target := float64(p95) * (1 + r.config.SafetyMarginFraction)

	// Use historical data if available for better estimation
	if ts, ok := r.history.CPUSamples[containerName]; ok && len(ts.Samples) >= r.config.MinSampleCount {
		// Calculate weighted percentile from history
		histP95 := calculateWeightedPercentile(ts.Samples, 95, ts.DecayHalfLife)
		if histP95 > 0 {
			// Average current and historical
			target = (target + histP95*(1+r.config.SafetyMarginFraction)) / 2
		}
	}

	// Minimum 10m CPU
	if target < 10 {
		target = 10
	}

	return int64(math.Ceil(target))
}

// calculateMemoryRecommendation calculates memory recommendation based on history
func (r *Recommender) calculateMemoryRecommendation(containerName string, cam *metrics.ContainerAggregatedMetrics) int64 {
	// Memory should target P99 or higher to avoid OOMs
	p99 := cam.MemoryP99Bytes
	if p99 == 0 {
		return cam.CurrentRequestMemory.Value()
	}

	// Add larger safety margin for memory (OOM is worse than throttling)
	target := float64(p99) * (1 + r.config.SafetyMarginFraction*1.5)

	// Use historical data for better estimation
	if ts, ok := r.history.MemorySamples[containerName]; ok && len(ts.Samples) >= r.config.MinSampleCount {
		histP99 := calculateWeightedPercentile(ts.Samples, 99, ts.DecayHalfLife)
		if histP99 > 0 {
			// For memory, take the max to be safe
			histTarget := histP99 * (1 + r.config.SafetyMarginFraction*1.5)
			if histTarget > target {
				target = histTarget
			}
		}
	}

	// Account for OOM events - if we've seen OOMs, bump up significantly
	oomCount := r.countRecentOOMs(containerName, time.Hour*24)
	if oomCount > 0 {
		target *= 1.0 + (0.1 * float64(oomCount)) // 10% increase per OOM
	}

	// Minimum 64Mi
	minMem := int64(64 * 1024 * 1024)
	if int64(target) < minMem {
		target = float64(minMem)
	}

	return int64(math.Ceil(target))
}

// countRecentOOMs counts OOM events for a container within the given duration
func (r *Recommender) countRecentOOMs(containerName string, within time.Duration) int {
	cutoff := time.Now().Add(-within)
	count := 0
	for _, event := range r.history.OOMEvents {
		if event.ContainerName == containerName && event.Timestamp.After(cutoff) {
			count++
		}
	}
	return count
}

// applyPolicyConstraints applies min/max constraints from policy
func applyPolicyConstraints(value int64, resourceName corev1.ResourceName, policy *autoscalingv1alpha1.ContainerResourcePolicy) (target, lower, upper int64) {
	target = value
	lower = value / 2 // Default lower bound is 50% of target
	upper = value * 2 // Default upper bound is 200% of target

	if policy == nil {
		return
	}

	// Apply min constraint
	if minAllowed, ok := policy.MinAllowed[resourceName]; ok {
		minVal := minAllowed.Value()
		if resourceName == corev1.ResourceCPU {
			minVal = minAllowed.MilliValue()
		}
		if target < minVal {
			target = minVal
		}
		lower = minVal
	}

	// Apply max constraint
	if maxAllowed, ok := policy.MaxAllowed[resourceName]; ok {
		maxVal := maxAllowed.Value()
		if resourceName == corev1.ResourceCPU {
			maxVal = maxAllowed.MilliValue()
		}
		if target > maxVal {
			target = maxVal
		}
		upper = maxVal
	}

	return
}

// findContainerPolicy finds the policy for a container
func findContainerPolicy(containerName string, policies []autoscalingv1alpha1.ContainerResourcePolicy) *autoscalingv1alpha1.ContainerResourcePolicy {
	var wildcardPolicy *autoscalingv1alpha1.ContainerResourcePolicy

	for i := range policies {
		p := &policies[i]
		if p.ContainerName == containerName {
			return p
		}
		if p.ContainerName == "*" {
			wildcardPolicy = p
		}
	}

	return wildcardPolicy
}

// calculateDesiredReplicas calculates desired replicas based on current metrics
func calculateDesiredReplicas(currentReplicas int32, currentValue, targetValue float64) int32 {
	if targetValue <= 0 {
		return currentReplicas
	}

	ratio := currentValue / targetValue
	desired := float64(currentReplicas) * ratio

	// Round up to be safe
	return int32(math.Ceil(desired))
}

// boundReplicas constrains replicas to min/max
func boundReplicas(desired, min, max int32) int32 {
	if desired < min {
		return min
	}
	if desired > max {
		return max
	}
	return desired
}

// calculateWeightedPercentile calculates a percentile with time-decay weighting
func calculateWeightedPercentile(samples []Sample, percentile int, halfLife time.Duration) float64 {
	if len(samples) == 0 {
		return 0
	}

	now := time.Now()
	weightedSamples := make([]weightedValue, 0, len(samples))

	for _, s := range samples {
		// Calculate decay weight
		age := now.Sub(s.Timestamp)
		weight := math.Pow(0.5, float64(age)/float64(halfLife))

		weightedSamples = append(weightedSamples, weightedValue{
			value:  s.Value,
			weight: weight,
		})
	}

	// Sort by value
	sortWeightedValues(weightedSamples)

	// Find weighted percentile
	totalWeight := 0.0
	for _, ws := range weightedSamples {
		totalWeight += ws.weight
	}

	targetWeight := totalWeight * float64(percentile) / 100
	cumulativeWeight := 0.0

	for _, ws := range weightedSamples {
		cumulativeWeight += ws.weight
		if cumulativeWeight >= targetWeight {
			return ws.value
		}
	}

	return weightedSamples[len(weightedSamples)-1].value
}

type weightedValue struct {
	value  float64
	weight float64
}

func sortWeightedValues(values []weightedValue) {
	// Simple bubble sort
	for i := 0; i < len(values)-1; i++ {
		for j := 0; j < len(values)-i-1; j++ {
			if values[j].value > values[j+1].value {
				values[j], values[j+1] = values[j+1], values[j]
			}
		}
	}
}

// calculateHorizontalConfidence calculates confidence in horizontal recommendation
func (r *Recommender) calculateHorizontalConfidence(collected *metrics.CollectedMetrics) float64 {
	if collected == nil || collected.Aggregated == nil {
		return 0
	}

	// Confidence based on sample count and variance
	if collected.Aggregated.ReadyPods == 0 {
		return 0
	}

	// Base confidence from ready pod ratio
	readyRatio := float64(collected.Aggregated.ReadyPods) / float64(collected.Aggregated.TotalPods)
	if readyRatio < 0.5 {
		return 0.3 // Low confidence if many pods aren't ready
	}

	return math.Min(readyRatio, 1.0)
}

// calculateVerticalConfidence calculates confidence in vertical recommendation
func (r *Recommender) calculateVerticalConfidence() float64 {
	// Check if we have enough historical data
	minSamples := r.config.MinSampleCount
	totalSamples := 0

	for _, ts := range r.history.CPUSamples {
		totalSamples += len(ts.Samples)
	}

	if totalSamples < minSamples {
		return 0.3 + (0.7 * float64(totalSamples) / float64(minSamples))
	}

	return 0.9
}
