package coordinator

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/log"

	autoscalingv1alpha1 "github.com/neogan74/hybridautoscaler/api/v1alpha1"
	"github.com/neogan74/hybridautoscaler/internal/metrics"
	"github.com/neogan74/hybridautoscaler/internal/recommender"
)

// Coordinator decides how to combine horizontal and vertical scaling decisions
type Coordinator struct {
	cooldownTracker *CooldownTracker
	config          CoordinatorConfig
}

// CoordinatorConfig holds configuration for coordination
type CoordinatorConfig struct {
	// DefaultStabilizationWindow is the default stabilization period
	DefaultStabilizationWindow time.Duration

	// DefaultScaleUpCooldown is cooldown after scale up
	DefaultScaleUpCooldown time.Duration

	// DefaultScaleDownCooldown is cooldown after scale down
	DefaultScaleDownCooldown time.Duration

	// DefaultVerticalScaleUpCooldown is cooldown after vertical scale up
	DefaultVerticalScaleUpCooldown time.Duration

	// DefaultVerticalScaleDownCooldown is cooldown after vertical scale down
	DefaultVerticalScaleDownCooldown time.Duration

	// VerticalScaleUpThreshold is the utilization threshold to trigger vertical scale up
	VerticalScaleUpThreshold float64

	// VerticalScaleDownThreshold is the utilization threshold to trigger vertical scale down
	VerticalScaleDownThreshold float64

	// MinChangeFraction is the minimum change needed to trigger scaling (as fraction of current)
	MinChangeFraction float64
}

// DefaultCoordinatorConfig returns sensible defaults
func DefaultCoordinatorConfig() CoordinatorConfig {
	return CoordinatorConfig{
		DefaultStabilizationWindow:       300 * time.Second,
		DefaultScaleUpCooldown:           60 * time.Second,
		DefaultScaleDownCooldown:         300 * time.Second,
		DefaultVerticalScaleUpCooldown:   120 * time.Second,
		DefaultVerticalScaleDownCooldown: 600 * time.Second,
		VerticalScaleUpThreshold:         0.9,
		VerticalScaleDownThreshold:       0.5,
		MinChangeFraction:                0.1,
	}
}

// CooldownTracker tracks cooldown periods for scaling actions
type CooldownTracker struct {
	mu sync.RWMutex

	// Last scale times per HybridAutoscaler
	lastHorizontalScaleUp   map[string]time.Time
	lastHorizontalScaleDown map[string]time.Time
	lastVerticalScaleUp     map[string]time.Time
	lastVerticalScaleDown   map[string]time.Time

	// Stabilization windows - recent recommendations
	recentRecommendations map[string][]timestampedRec
}

type timestampedRec struct {
	timestamp time.Time
	replicas  int32
	resources map[string]corev1.ResourceList
}

// Decision represents the coordinated scaling decision
type Decision struct {
	// HorizontalAction is the horizontal scaling action to take
	HorizontalAction HorizontalAction

	// VerticalAction is the vertical scaling action to take
	VerticalAction VerticalAction

	// Reason explains the decision
	Reason string

	// Priority indicates which scaling type should happen first
	Priority ScalingPriority
}

// HorizontalAction represents a horizontal scaling action
type HorizontalAction struct {
	// ShouldScale indicates if horizontal scaling should occur
	ShouldScale bool

	// TargetReplicas is the desired replica count
	TargetReplicas int32

	// Direction is scale up or down
	Direction ScaleDirection

	// Reason explains the action
	Reason string
}

// VerticalAction represents a vertical scaling action
type VerticalAction struct {
	// ShouldScale indicates if vertical scaling should occur
	ShouldScale bool

	// ContainerResources contains the target resources per container
	ContainerResources map[string]*ContainerResourceAction

	// Direction is scale up or down
	Direction ScaleDirection

	// Reason explains the action
	Reason string
}

// ContainerResourceAction contains resource changes for a container
type ContainerResourceAction struct {
	ContainerName  string
	TargetRequest  corev1.ResourceList
	TargetLimit    corev1.ResourceList
	CurrentRequest corev1.ResourceList
	CurrentLimit   corev1.ResourceList
}

// ScaleDirection indicates scaling direction
type ScaleDirection string

const (
	ScaleUp   ScaleDirection = "up"
	ScaleDown ScaleDirection = "down"
	NoScale   ScaleDirection = "none"
)

// ScalingPriority indicates which scaling to perform first
type ScalingPriority string

const (
	PriorityHorizontal ScalingPriority = "horizontal"
	PriorityVertical   ScalingPriority = "vertical"
	PriorityBoth       ScalingPriority = "both"
	PriorityNone       ScalingPriority = "none"
)

// NewCoordinator creates a new coordinator
func NewCoordinator(config CoordinatorConfig) *Coordinator {
	return &Coordinator{
		config: config,
		cooldownTracker: &CooldownTracker{
			lastHorizontalScaleUp:   make(map[string]time.Time),
			lastHorizontalScaleDown: make(map[string]time.Time),
			lastVerticalScaleUp:     make(map[string]time.Time),
			lastVerticalScaleDown:   make(map[string]time.Time),
			recentRecommendations:   make(map[string][]timestampedRec),
		},
	}
}

// Coordinate produces a coordinated scaling decision
func (c *Coordinator) Coordinate(
	ctx context.Context,
	ha *autoscalingv1alpha1.HybridAutoscaler,
	currentState *CurrentState,
	recommendations *recommender.Recommendations,
	collectedMetrics *metrics.CollectedMetrics,
) (*Decision, error) {
	logger := log.FromContext(ctx)
	key := fmt.Sprintf("%s/%s", ha.Namespace, ha.Name)

	strategy := c.getStrategy(ha)

	// Record recommendation for stabilization
	c.recordRecommendation(key, recommendations)

	// Get stabilized recommendations
	stabilizedHRec := c.getStabilizedHorizontalRec(key, ha, recommendations.Horizontal)
	stabilizedVRec := recommendations.Vertical // Vertical doesn't need stabilization in the same way

	// Check cooldowns
	canScaleHorizontal := c.canScaleHorizontally(key, ha, stabilizedHRec, currentState)
	canScaleVertical := c.canScaleVertically(key, ha, stabilizedVRec, currentState)

	logger.V(1).Info("Coordination inputs",
		"strategy", strategy,
		"currentReplicas", currentState.Replicas,
		"desiredReplicas", stabilizedHRec.DesiredReplicas,
		"canScaleHorizontal", canScaleHorizontal,
		"canScaleVertical", canScaleVertical,
	)

	// Generate decision based on strategy
	var decision *Decision
	switch strategy {
	case autoscalingv1alpha1.HorizontalFirst:
		decision = c.horizontalFirstDecision(currentState, stabilizedHRec, stabilizedVRec, canScaleHorizontal, canScaleVertical, collectedMetrics)
	case autoscalingv1alpha1.VerticalFirst:
		decision = c.verticalFirstDecision(currentState, stabilizedHRec, stabilizedVRec, canScaleHorizontal, canScaleVertical, collectedMetrics)
	case autoscalingv1alpha1.Balanced:
		decision = c.balancedDecision(currentState, stabilizedHRec, stabilizedVRec, canScaleHorizontal, canScaleVertical, collectedMetrics)
	case autoscalingv1alpha1.Predictive:
		decision = c.predictiveDecision(currentState, stabilizedHRec, stabilizedVRec, canScaleHorizontal, canScaleVertical, collectedMetrics)
	default:
		decision = c.balancedDecision(currentState, stabilizedHRec, stabilizedVRec, canScaleHorizontal, canScaleVertical, collectedMetrics)
	}

	return decision, nil
}

// CurrentState represents the current state of the target workload
type CurrentState struct {
	Replicas           int32
	ReadyReplicas      int32
	ContainerResources map[string]corev1.ResourceRequirements
}

// getStrategy returns the coordination strategy to use
func (c *Coordinator) getStrategy(ha *autoscalingv1alpha1.HybridAutoscaler) autoscalingv1alpha1.CoordinationStrategy {
	if ha.Spec.Coordination != nil && ha.Spec.Coordination.Strategy != "" {
		return ha.Spec.Coordination.Strategy
	}
	return autoscalingv1alpha1.Balanced
}

// recordRecommendation records a recommendation for stabilization
func (c *Coordinator) recordRecommendation(key string, rec *recommender.Recommendations) {
	c.cooldownTracker.mu.Lock()
	defer c.cooldownTracker.mu.Unlock()

	tr := timestampedRec{
		timestamp: time.Now(),
		resources: make(map[string]corev1.ResourceList),
	}

	if rec.Horizontal != nil {
		tr.replicas = rec.Horizontal.DesiredReplicas
	}

	if rec.Vertical != nil {
		for name, cr := range rec.Vertical.ContainerRecommendations {
			tr.resources[name] = cr.Target
		}
	}

	c.cooldownTracker.recentRecommendations[key] = append(
		c.cooldownTracker.recentRecommendations[key],
		tr,
	)

	// Keep only recent recommendations (last 10 minutes)
	cutoff := time.Now().Add(-10 * time.Minute)
	filtered := make([]timestampedRec, 0)
	for _, r := range c.cooldownTracker.recentRecommendations[key] {
		if r.timestamp.After(cutoff) {
			filtered = append(filtered, r)
		}
	}
	c.cooldownTracker.recentRecommendations[key] = filtered
}

// getStabilizedHorizontalRec returns a stabilized horizontal recommendation
func (c *Coordinator) getStabilizedHorizontalRec(
	key string,
	ha *autoscalingv1alpha1.HybridAutoscaler,
	rec *recommender.HorizontalRecommendation,
) *recommender.HorizontalRecommendation {
	c.cooldownTracker.mu.RLock()
	defer c.cooldownTracker.mu.RUnlock()

	if rec == nil {
		return nil
	}

	stabilizationWindow := c.config.DefaultStabilizationWindow
	if ha.Spec.Coordination != nil && ha.Spec.Coordination.StabilizationWindowSeconds != nil {
		stabilizationWindow = time.Duration(*ha.Spec.Coordination.StabilizationWindowSeconds) * time.Second
	}

	cutoff := time.Now().Add(-stabilizationWindow)
	recentRecs := c.cooldownTracker.recentRecommendations[key]

	if len(recentRecs) == 0 {
		return rec
	}

	// For scale down, use the highest recent recommendation (conservative)
	// For scale up, use the current recommendation (responsive)
	if rec.DesiredReplicas < rec.CurrentReplicas {
		maxReplicas := rec.DesiredReplicas
		for _, r := range recentRecs {
			if r.timestamp.After(cutoff) && r.replicas > maxReplicas {
				maxReplicas = r.replicas
			}
		}
		result := *rec
		result.DesiredReplicas = maxReplicas
		return &result
	}

	return rec
}

// canScaleHorizontally checks if horizontal scaling is allowed
func (c *Coordinator) canScaleHorizontally(
	key string,
	ha *autoscalingv1alpha1.HybridAutoscaler,
	rec *recommender.HorizontalRecommendation,
	current *CurrentState,
) bool {
	c.cooldownTracker.mu.RLock()
	defer c.cooldownTracker.mu.RUnlock()

	if rec == nil {
		return false
	}

	// No change needed
	if rec.DesiredReplicas == current.Replicas {
		return false
	}

	now := time.Now()
	isScaleUp := rec.DesiredReplicas > current.Replicas

	// Get cooldown periods
	scaleUpCooldown := c.config.DefaultScaleUpCooldown
	scaleDownCooldown := c.config.DefaultScaleDownCooldown

	if ha.Spec.Coordination != nil && ha.Spec.Coordination.CooldownPeriods != nil {
		if ha.Spec.Coordination.CooldownPeriods.ScaleUpSeconds != nil {
			scaleUpCooldown = time.Duration(*ha.Spec.Coordination.CooldownPeriods.ScaleUpSeconds) * time.Second
		}
		if ha.Spec.Coordination.CooldownPeriods.ScaleDownSeconds != nil {
			scaleDownCooldown = time.Duration(*ha.Spec.Coordination.CooldownPeriods.ScaleDownSeconds) * time.Second
		}
	}

	if isScaleUp {
		if lastUp, ok := c.cooldownTracker.lastHorizontalScaleUp[key]; ok {
			if now.Sub(lastUp) < scaleUpCooldown {
				return false
			}
		}
	} else {
		if lastDown, ok := c.cooldownTracker.lastHorizontalScaleDown[key]; ok {
			if now.Sub(lastDown) < scaleDownCooldown {
				return false
			}
		}
	}

	return true
}

// canScaleVertically checks if vertical scaling is allowed
func (c *Coordinator) canScaleVertically(
	key string,
	ha *autoscalingv1alpha1.HybridAutoscaler,
	rec *recommender.VerticalRecommendation,
	current *CurrentState,
) bool {
	c.cooldownTracker.mu.RLock()
	defer c.cooldownTracker.mu.RUnlock()

	if rec == nil || len(rec.ContainerRecommendations) == 0 {
		return false
	}

	// Check if vertical scaling is enabled
	if ha.Spec.Vertical == nil {
		return false
	}
	if ha.Spec.Vertical.UpdatePolicy != nil && ha.Spec.Vertical.UpdatePolicy.UpdateMode == autoscalingv1alpha1.UpdateModeOff {
		return false
	}

	// Check if any container needs scaling
	needsScaling := false
	isScaleUp := false

	for containerName, cr := range rec.ContainerRecommendations {
		currentRes, exists := current.ContainerResources[containerName]
		if !exists {
			continue
		}

		// Check CPU change
		currentCPU := currentRes.Requests.Cpu().MilliValue()
		targetCPU := cr.Target.Cpu().MilliValue()
		if currentCPU > 0 {
			changeFraction := math.Abs(float64(targetCPU-currentCPU)) / float64(currentCPU)
			if changeFraction >= c.config.MinChangeFraction {
				needsScaling = true
				if targetCPU > currentCPU {
					isScaleUp = true
				}
			}
		}

		// Check memory change
		currentMem := currentRes.Requests.Memory().Value()
		targetMem := cr.Target.Memory().Value()
		if currentMem > 0 {
			changeFraction := math.Abs(float64(targetMem-currentMem)) / float64(currentMem)
			if changeFraction >= c.config.MinChangeFraction {
				needsScaling = true
				if targetMem > currentMem {
					isScaleUp = true
				}
			}
		}
	}

	if !needsScaling {
		return false
	}

	// Check cooldowns
	now := time.Now()
	scaleUpCooldown := c.config.DefaultVerticalScaleUpCooldown
	scaleDownCooldown := c.config.DefaultVerticalScaleDownCooldown

	if ha.Spec.Coordination != nil && ha.Spec.Coordination.CooldownPeriods != nil {
		if ha.Spec.Coordination.CooldownPeriods.VerticalScaleUpSeconds != nil {
			scaleUpCooldown = time.Duration(*ha.Spec.Coordination.CooldownPeriods.VerticalScaleUpSeconds) * time.Second
		}
		if ha.Spec.Coordination.CooldownPeriods.VerticalScaleDownSeconds != nil {
			scaleDownCooldown = time.Duration(*ha.Spec.Coordination.CooldownPeriods.VerticalScaleDownSeconds) * time.Second
		}
	}

	if isScaleUp {
		if lastUp, ok := c.cooldownTracker.lastVerticalScaleUp[key]; ok {
			if now.Sub(lastUp) < scaleUpCooldown {
				return false
			}
		}
	} else {
		if lastDown, ok := c.cooldownTracker.lastVerticalScaleDown[key]; ok {
			if now.Sub(lastDown) < scaleDownCooldown {
				return false
			}
		}
	}

	return true
}

// RecordHorizontalScale records a horizontal scaling action
func (c *Coordinator) RecordHorizontalScale(key string, direction ScaleDirection) {
	c.cooldownTracker.mu.Lock()
	defer c.cooldownTracker.mu.Unlock()

	now := time.Now()
	if direction == ScaleUp {
		c.cooldownTracker.lastHorizontalScaleUp[key] = now
	} else if direction == ScaleDown {
		c.cooldownTracker.lastHorizontalScaleDown[key] = now
	}
}

// RecordVerticalScale records a vertical scaling action
func (c *Coordinator) RecordVerticalScale(key string, direction ScaleDirection) {
	c.cooldownTracker.mu.Lock()
	defer c.cooldownTracker.mu.Unlock()

	now := time.Now()
	if direction == ScaleUp {
		c.cooldownTracker.lastVerticalScaleUp[key] = now
	} else if direction == ScaleDown {
		c.cooldownTracker.lastVerticalScaleDown[key] = now
	}
}

// horizontalFirstDecision implements the HorizontalFirst strategy
func (c *Coordinator) horizontalFirstDecision(
	current *CurrentState,
	hRec *recommender.HorizontalRecommendation,
	vRec *recommender.VerticalRecommendation,
	canScaleH, canScaleV bool,
	collected *metrics.CollectedMetrics,
) *Decision {
	decision := &Decision{
		Priority: PriorityNone,
	}

	// Always prefer horizontal scaling
	if canScaleH && hRec != nil && hRec.DesiredReplicas != current.Replicas {
		decision.HorizontalAction = HorizontalAction{
			ShouldScale:    true,
			TargetReplicas: hRec.DesiredReplicas,
			Direction:      c.getHorizontalDirection(current.Replicas, hRec.DesiredReplicas),
			Reason:         hRec.Reason,
		}
		decision.Priority = PriorityHorizontal
		decision.Reason = fmt.Sprintf("HorizontalFirst: scaling replicas from %d to %d", current.Replicas, hRec.DesiredReplicas)
	}

	// Only do vertical scaling if horizontal is at limits or stable
	atMinReplicas := hRec != nil && current.Replicas <= hRec.MinReplicas
	atMaxReplicas := hRec != nil && current.Replicas >= hRec.MaxReplicas
	horizontalStable := !decision.HorizontalAction.ShouldScale

	if canScaleV && (atMinReplicas || atMaxReplicas || horizontalStable) {
		vAction := c.buildVerticalAction(current, vRec)
		if vAction.ShouldScale {
			decision.VerticalAction = vAction
			if decision.Priority == PriorityNone {
				decision.Priority = PriorityVertical
				decision.Reason = fmt.Sprintf("HorizontalFirst: horizontal stable, applying vertical scaling")
			}
		}
	}

	return decision
}

// verticalFirstDecision implements the VerticalFirst strategy
func (c *Coordinator) verticalFirstDecision(
	current *CurrentState,
	hRec *recommender.HorizontalRecommendation,
	vRec *recommender.VerticalRecommendation,
	canScaleH, canScaleV bool,
	collected *metrics.CollectedMetrics,
) *Decision {
	decision := &Decision{
		Priority: PriorityNone,
	}

	// Check if pods are resource-constrained (high utilization)
	resourceConstrained := false
	if collected.Aggregated != nil {
		resourceConstrained = collected.Aggregated.AverageCPUUtilization > c.config.VerticalScaleUpThreshold*100 ||
			collected.Aggregated.AverageMemoryUtilization > c.config.VerticalScaleUpThreshold*100
	}

	// Prefer vertical scaling when resource-constrained
	if canScaleV && resourceConstrained {
		vAction := c.buildVerticalAction(current, vRec)
		if vAction.ShouldScale {
			decision.VerticalAction = vAction
			decision.Priority = PriorityVertical
			decision.Reason = "VerticalFirst: pods resource-constrained, scaling vertically"
		}
	}

	// Fall back to horizontal if vertical can't help or is at limits
	if decision.Priority == PriorityNone && canScaleH && hRec != nil && hRec.DesiredReplicas != current.Replicas {
		decision.HorizontalAction = HorizontalAction{
			ShouldScale:    true,
			TargetReplicas: hRec.DesiredReplicas,
			Direction:      c.getHorizontalDirection(current.Replicas, hRec.DesiredReplicas),
			Reason:         hRec.Reason,
		}
		decision.Priority = PriorityHorizontal
		decision.Reason = fmt.Sprintf("VerticalFirst: scaling horizontally from %d to %d", current.Replicas, hRec.DesiredReplicas)
	}

	return decision
}

// balancedDecision implements the Balanced strategy
func (c *Coordinator) balancedDecision(
	current *CurrentState,
	hRec *recommender.HorizontalRecommendation,
	vRec *recommender.VerticalRecommendation,
	canScaleH, canScaleV bool,
	collected *metrics.CollectedMetrics,
) *Decision {
	decision := &Decision{
		Priority: PriorityNone,
	}

	// Evaluate both options
	var hAction HorizontalAction
	var vAction VerticalAction

	if canScaleH && hRec != nil && hRec.DesiredReplicas != current.Replicas {
		hAction = HorizontalAction{
			ShouldScale:    true,
			TargetReplicas: hRec.DesiredReplicas,
			Direction:      c.getHorizontalDirection(current.Replicas, hRec.DesiredReplicas),
			Reason:         hRec.Reason,
		}
	}

	if canScaleV {
		vAction = c.buildVerticalAction(current, vRec)
	}

	// Decision logic for balanced approach
	if hAction.ShouldScale && vAction.ShouldScale {
		// Both can scale - decide based on situation
		if hAction.Direction == ScaleUp && vAction.Direction == ScaleUp {
			// Both scaling up - prefer horizontal for faster response
			decision.HorizontalAction = hAction
			decision.Priority = PriorityHorizontal
			decision.Reason = "Balanced: scaling up horizontally for faster response"
		} else if hAction.Direction == ScaleDown && vAction.Direction == ScaleDown {
			// Both scaling down - scale down horizontal first, then vertical
			decision.HorizontalAction = hAction
			decision.Priority = PriorityHorizontal
			decision.Reason = "Balanced: scaling down horizontally first"
		} else if hAction.Direction == ScaleUp {
			// Horizontal up, vertical down - do horizontal
			decision.HorizontalAction = hAction
			decision.Priority = PriorityHorizontal
			decision.Reason = "Balanced: prioritizing horizontal scale up"
		} else {
			// Horizontal down, vertical up - do vertical to optimize resources
			decision.VerticalAction = vAction
			decision.Priority = PriorityVertical
			decision.Reason = "Balanced: optimizing resources vertically before scaling down"
		}
	} else if hAction.ShouldScale {
		decision.HorizontalAction = hAction
		decision.Priority = PriorityHorizontal
		decision.Reason = "Balanced: horizontal scaling needed"
	} else if vAction.ShouldScale {
		decision.VerticalAction = vAction
		decision.Priority = PriorityVertical
		decision.Reason = "Balanced: vertical scaling needed"
	}

	return decision
}

// predictiveDecision implements the Predictive strategy (placeholder for ML-based)
func (c *Coordinator) predictiveDecision(
	current *CurrentState,
	hRec *recommender.HorizontalRecommendation,
	vRec *recommender.VerticalRecommendation,
	canScaleH, canScaleV bool,
	collected *metrics.CollectedMetrics,
) *Decision {
	// TODO: Implement predictive scaling using historical patterns
	// For now, fall back to balanced
	decision := c.balancedDecision(current, hRec, vRec, canScaleH, canScaleV, collected)
	decision.Reason = "Predictive: " + decision.Reason
	return decision
}

// buildVerticalAction creates a VerticalAction from recommendations
func (c *Coordinator) buildVerticalAction(
	current *CurrentState,
	vRec *recommender.VerticalRecommendation,
) VerticalAction {
	action := VerticalAction{
		ContainerResources: make(map[string]*ContainerResourceAction),
	}

	if vRec == nil {
		return action
	}

	overallDirection := NoScale
	reasons := make([]string, 0)

	for containerName, cr := range vRec.ContainerRecommendations {
		currentRes, exists := current.ContainerResources[containerName]
		if !exists {
			continue
		}

		cra := &ContainerResourceAction{
			ContainerName:  containerName,
			TargetRequest:  cr.Target,
			CurrentRequest: currentRes.Requests,
			CurrentLimit:   currentRes.Limits,
		}

		// Calculate limits based on requests (maintain ratio or set equal)
		cra.TargetLimit = c.calculateTargetLimits(cr.Target, currentRes)

		// Determine direction
		currentCPU := currentRes.Requests.Cpu().MilliValue()
		targetCPU := cr.Target.Cpu().MilliValue()
		currentMem := currentRes.Requests.Memory().Value()
		targetMem := cr.Target.Memory().Value()

		cpuChange := float64(targetCPU-currentCPU) / float64(max(currentCPU, 1))
		memChange := float64(targetMem-currentMem) / float64(max(currentMem, 1))

		if math.Abs(cpuChange) >= c.config.MinChangeFraction || math.Abs(memChange) >= c.config.MinChangeFraction {
			action.ShouldScale = true
			action.ContainerResources[containerName] = cra

			if cpuChange > 0 || memChange > 0 {
				overallDirection = ScaleUp
				reasons = append(reasons, fmt.Sprintf("%s: CPU %.0f%%, Mem %.0f%%", containerName, cpuChange*100, memChange*100))
			} else {
				if overallDirection != ScaleUp {
					overallDirection = ScaleDown
				}
				reasons = append(reasons, fmt.Sprintf("%s: CPU %.0f%%, Mem %.0f%%", containerName, cpuChange*100, memChange*100))
			}
		}
	}

	action.Direction = overallDirection
	if len(reasons) > 0 {
		action.Reason = fmt.Sprintf("Resource changes: %v", reasons)
	}

	return action
}

// calculateTargetLimits calculates new limits based on target requests
func (c *Coordinator) calculateTargetLimits(targetRequest corev1.ResourceList, current corev1.ResourceRequirements) corev1.ResourceList {
	limits := make(corev1.ResourceList)

	// CPU limit
	if targetCPU, ok := targetRequest[corev1.ResourceCPU]; ok {
		if currentLimit, hasLimit := current.Limits[corev1.ResourceCPU]; hasLimit {
			if currentReq, hasReq := current.Requests[corev1.ResourceCPU]; hasReq && currentReq.MilliValue() > 0 {
				// Maintain ratio
				ratio := float64(currentLimit.MilliValue()) / float64(currentReq.MilliValue())
				newLimit := int64(float64(targetCPU.MilliValue()) * ratio)
				limits[corev1.ResourceCPU] = *resource.NewMilliQuantity(newLimit, resource.DecimalSI)
			} else {
				limits[corev1.ResourceCPU] = targetCPU
			}
		}
	}

	// Memory limit
	if targetMem, ok := targetRequest[corev1.ResourceMemory]; ok {
		if currentLimit, hasLimit := current.Limits[corev1.ResourceMemory]; hasLimit {
			if currentReq, hasReq := current.Requests[corev1.ResourceMemory]; hasReq && currentReq.Value() > 0 {
				// Maintain ratio
				ratio := float64(currentLimit.Value()) / float64(currentReq.Value())
				newLimit := int64(float64(targetMem.Value()) * ratio)
				limits[corev1.ResourceMemory] = *resource.NewQuantity(newLimit, resource.BinarySI)
			} else {
				limits[corev1.ResourceMemory] = targetMem
			}
		}
	}

	return limits
}

// getHorizontalDirection determines the scaling direction
func (c *Coordinator) getHorizontalDirection(current, desired int32) ScaleDirection {
	if desired > current {
		return ScaleUp
	} else if desired < current {
		return ScaleDown
	}
	return NoScale
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
