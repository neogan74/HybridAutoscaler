package actuator

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	autoscalingv1alpha1 "github.com/neogan74/hybridautoscaler/api/v1alpha1"
	"github.com/neogan74/hybridautoscaler/internal/coordinator"
)

// Actuator applies scaling decisions to the cluster
type Actuator struct {
	client     client.Client
	kubeClient kubernetes.Interface
	config     ActuatorConfig
}

// ActuatorConfig holds configuration for the actuator
type ActuatorConfig struct {
	// DryRun if true, don't actually apply changes
	DryRun bool

	// MaxEvictionsPerCycle limits how many pods can be evicted per reconciliation
	MaxEvictionsPerCycle int

	// EvictionTimeout is how long to wait for eviction to complete
	EvictionTimeout time.Duration

	// PodReadyTimeout is how long to wait for pods to become ready after scaling
	PodReadyTimeout time.Duration
}

// DefaultActuatorConfig returns sensible defaults
func DefaultActuatorConfig() ActuatorConfig {
	return ActuatorConfig{
		DryRun:               false,
		MaxEvictionsPerCycle: 1,
		EvictionTimeout:      60 * time.Second,
		PodReadyTimeout:      300 * time.Second,
	}
}

// ApplyResult contains the result of applying a decision
type ApplyResult struct {
	// HorizontalApplied indicates if horizontal scaling was applied
	HorizontalApplied bool

	// VerticalApplied indicates if vertical scaling was applied
	VerticalApplied bool

	// NewReplicas is the new replica count (if horizontal applied)
	NewReplicas int32

	// EvictedPods are pods that were evicted for vertical scaling
	EvictedPods []string

	// Errors contains any errors that occurred
	Errors []error

	// ScalingEvents contains events to record
	ScalingEvents []autoscalingv1alpha1.ScalingEvent
}

// NewActuator creates a new actuator
func NewActuator(client client.Client, kubeClient kubernetes.Interface, config ActuatorConfig) *Actuator {
	return &Actuator{
		client:     client,
		kubeClient: kubeClient,
		config:     config,
	}
}

// Apply executes the scaling decision
func (a *Actuator) Apply(
	ctx context.Context,
	ha *autoscalingv1alpha1.HybridAutoscaler,
	decision *coordinator.Decision,
) (*ApplyResult, error) {
	logger := log.FromContext(ctx)
	result := &ApplyResult{}

	if a.config.DryRun {
		logger.Info("DryRun mode: would apply decision", "decision", decision)
		return result, nil
	}

	// Apply based on priority
	switch decision.Priority {
	case coordinator.PriorityHorizontal:
		if err := a.applyHorizontalScaling(ctx, ha, decision.HorizontalAction, result); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("horizontal scaling failed: %w", err))
		}

	case coordinator.PriorityVertical:
		if err := a.applyVerticalScaling(ctx, ha, decision.VerticalAction, result); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("vertical scaling failed: %w", err))
		}

	case coordinator.PriorityBoth:
		// Apply horizontal first (faster), then vertical
		if err := a.applyHorizontalScaling(ctx, ha, decision.HorizontalAction, result); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("horizontal scaling failed: %w", err))
		}
		if err := a.applyVerticalScaling(ctx, ha, decision.VerticalAction, result); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("vertical scaling failed: %w", err))
		}

	case coordinator.PriorityNone:
		logger.V(1).Info("No scaling action needed")
	}

	return result, nil
}

// applyHorizontalScaling scales the target horizontally
func (a *Actuator) applyHorizontalScaling(
	ctx context.Context,
	ha *autoscalingv1alpha1.HybridAutoscaler,
	action coordinator.HorizontalAction,
	result *ApplyResult,
) error {
	logger := log.FromContext(ctx)

	if !action.ShouldScale {
		return nil
	}

	logger.Info("Applying horizontal scaling",
		"target", ha.Spec.TargetRef.Name,
		"targetReplicas", action.TargetReplicas,
		"direction", action.Direction,
	)

	var currentReplicas int32
	var err error

	switch ha.Spec.TargetRef.Kind {
	case "Deployment":
		currentReplicas, err = a.scaleDeployment(ctx, ha.Namespace, ha.Spec.TargetRef.Name, action.TargetReplicas)
	case "StatefulSet":
		currentReplicas, err = a.scaleStatefulSet(ctx, ha.Namespace, ha.Spec.TargetRef.Name, action.TargetReplicas)
	case "ReplicaSet":
		currentReplicas, err = a.scaleReplicaSet(ctx, ha.Namespace, ha.Spec.TargetRef.Name, action.TargetReplicas)
	default:
		return fmt.Errorf("unsupported target kind: %s", ha.Spec.TargetRef.Kind)
	}

	if err != nil {
		return err
	}

	result.HorizontalApplied = true
	result.NewReplicas = action.TargetReplicas
	result.ScalingEvents = append(result.ScalingEvents, autoscalingv1alpha1.ScalingEvent{
		Timestamp:    metav1.Now(),
		Type:         autoscalingv1alpha1.HorizontalScaling,
		FromReplicas: &currentReplicas,
		ToReplicas:   &action.TargetReplicas,
		Reason:       action.Reason,
		Success:      true,
	})

	return nil
}

// scaleDeployment scales a Deployment
func (a *Actuator) scaleDeployment(ctx context.Context, namespace, name string, replicas int32) (int32, error) {
	var currentReplicas int32

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment, err := a.kubeClient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if deployment.Spec.Replicas != nil {
			currentReplicas = *deployment.Spec.Replicas
		}

		deployment.Spec.Replicas = &replicas
		_, err = a.kubeClient.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		return err
	})

	return currentReplicas, err
}

// scaleStatefulSet scales a StatefulSet
func (a *Actuator) scaleStatefulSet(ctx context.Context, namespace, name string, replicas int32) (int32, error) {
	var currentReplicas int32

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sts, err := a.kubeClient.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if sts.Spec.Replicas != nil {
			currentReplicas = *sts.Spec.Replicas
		}

		sts.Spec.Replicas = &replicas
		_, err = a.kubeClient.AppsV1().StatefulSets(namespace).Update(ctx, sts, metav1.UpdateOptions{})
		return err
	})

	return currentReplicas, err
}

// scaleReplicaSet scales a ReplicaSet
func (a *Actuator) scaleReplicaSet(ctx context.Context, namespace, name string, replicas int32) (int32, error) {
	var currentReplicas int32

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rs, err := a.kubeClient.AppsV1().ReplicaSets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if rs.Spec.Replicas != nil {
			currentReplicas = *rs.Spec.Replicas
		}

		rs.Spec.Replicas = &replicas
		_, err = a.kubeClient.AppsV1().ReplicaSets(namespace).Update(ctx, rs, metav1.UpdateOptions{})
		return err
	})

	return currentReplicas, err
}

// applyVerticalScaling applies vertical scaling by updating pod templates and evicting pods
func (a *Actuator) applyVerticalScaling(
	ctx context.Context,
	ha *autoscalingv1alpha1.HybridAutoscaler,
	action coordinator.VerticalAction,
	result *ApplyResult,
) error {
	logger := log.FromContext(ctx)

	if !action.ShouldScale {
		return nil
	}

	// Check update mode
	updateMode := autoscalingv1alpha1.UpdateModeAuto
	if ha.Spec.Vertical != nil && ha.Spec.Vertical.UpdatePolicy != nil {
		updateMode = ha.Spec.Vertical.UpdatePolicy.UpdateMode
	}

	if updateMode == autoscalingv1alpha1.UpdateModeOff {
		logger.Info("Vertical scaling disabled (UpdateMode=Off)")
		return nil
	}

	logger.Info("Applying vertical scaling",
		"target", ha.Spec.TargetRef.Name,
		"direction", action.Direction,
		"updateMode", updateMode,
	)

	// Update pod template with new resource requests/limits
	fromResources, toResources, err := a.updatePodTemplate(ctx, ha, action)
	if err != nil {
		return fmt.Errorf("failed to update pod template: %w", err)
	}

	// For Auto mode, evict pods to pick up new resources
	if updateMode == autoscalingv1alpha1.UpdateModeAuto {
		evictedPods, err := a.evictPodsForVerticalScaling(ctx, ha, action)
		if err != nil {
			logger.Error(err, "Failed to evict some pods for vertical scaling")
			// Continue - partial eviction is acceptable
		}
		result.EvictedPods = evictedPods
	}

	result.VerticalApplied = true
	result.ScalingEvents = append(result.ScalingEvents, autoscalingv1alpha1.ScalingEvent{
		Timestamp:     metav1.Now(),
		Type:          autoscalingv1alpha1.VerticalScaling,
		FromResources: fromResources,
		ToResources:   toResources,
		Reason:        action.Reason,
		Success:       true,
	})

	return nil
}

// updatePodTemplate updates the pod template with new resource values
func (a *Actuator) updatePodTemplate(
	ctx context.Context,
	ha *autoscalingv1alpha1.HybridAutoscaler,
	action coordinator.VerticalAction,
) (map[string]corev1.ResourceList, map[string]corev1.ResourceList, error) {
	fromResources := make(map[string]corev1.ResourceList)
	toResources := make(map[string]corev1.ResourceList)

	switch ha.Spec.TargetRef.Kind {
	case "Deployment":
		return a.updateDeploymentTemplate(ctx, ha.Namespace, ha.Spec.TargetRef.Name, action)
	case "StatefulSet":
		return a.updateStatefulSetTemplate(ctx, ha.Namespace, ha.Spec.TargetRef.Name, action)
	case "ReplicaSet":
		return a.updateReplicaSetTemplate(ctx, ha.Namespace, ha.Spec.TargetRef.Name, action)
	default:
		return fromResources, toResources, fmt.Errorf("unsupported target kind: %s", ha.Spec.TargetRef.Kind)
	}
}

// updateDeploymentTemplate updates a Deployment's pod template
func (a *Actuator) updateDeploymentTemplate(
	ctx context.Context,
	namespace, name string,
	action coordinator.VerticalAction,
) (map[string]corev1.ResourceList, map[string]corev1.ResourceList, error) {
	fromResources := make(map[string]corev1.ResourceList)
	toResources := make(map[string]corev1.ResourceList)

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment := &appsv1.Deployment{}
		if err := a.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, deployment); err != nil {
			return err
		}

		// Update each container's resources
		for i, container := range deployment.Spec.Template.Spec.Containers {
			if cra, ok := action.ContainerResources[container.Name]; ok {
				fromResources[container.Name] = container.Resources.Requests.DeepCopy()
				toResources[container.Name] = cra.TargetRequest.DeepCopy()

				deployment.Spec.Template.Spec.Containers[i].Resources.Requests = cra.TargetRequest.DeepCopy()
				if len(cra.TargetLimit) > 0 {
					deployment.Spec.Template.Spec.Containers[i].Resources.Limits = cra.TargetLimit.DeepCopy()
				}
			}
		}

		return a.client.Update(ctx, deployment)
	})

	return fromResources, toResources, err
}

// updateStatefulSetTemplate updates a StatefulSet's pod template
func (a *Actuator) updateStatefulSetTemplate(
	ctx context.Context,
	namespace, name string,
	action coordinator.VerticalAction,
) (map[string]corev1.ResourceList, map[string]corev1.ResourceList, error) {
	fromResources := make(map[string]corev1.ResourceList)
	toResources := make(map[string]corev1.ResourceList)

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sts := &appsv1.StatefulSet{}
		if err := a.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, sts); err != nil {
			return err
		}

		for i, container := range sts.Spec.Template.Spec.Containers {
			if cra, ok := action.ContainerResources[container.Name]; ok {
				fromResources[container.Name] = container.Resources.Requests.DeepCopy()
				toResources[container.Name] = cra.TargetRequest.DeepCopy()

				sts.Spec.Template.Spec.Containers[i].Resources.Requests = cra.TargetRequest.DeepCopy()
				if len(cra.TargetLimit) > 0 {
					sts.Spec.Template.Spec.Containers[i].Resources.Limits = cra.TargetLimit.DeepCopy()
				}
			}
		}

		return a.client.Update(ctx, sts)
	})

	return fromResources, toResources, err
}

// updateReplicaSetTemplate updates a ReplicaSet's pod template
func (a *Actuator) updateReplicaSetTemplate(
	ctx context.Context,
	namespace, name string,
	action coordinator.VerticalAction,
) (map[string]corev1.ResourceList, map[string]corev1.ResourceList, error) {
	fromResources := make(map[string]corev1.ResourceList)
	toResources := make(map[string]corev1.ResourceList)

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rs := &appsv1.ReplicaSet{}
		if err := a.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, rs); err != nil {
			return err
		}

		for i, container := range rs.Spec.Template.Spec.Containers {
			if cra, ok := action.ContainerResources[container.Name]; ok {
				fromResources[container.Name] = container.Resources.Requests.DeepCopy()
				toResources[container.Name] = cra.TargetRequest.DeepCopy()

				rs.Spec.Template.Spec.Containers[i].Resources.Requests = cra.TargetRequest.DeepCopy()
				if len(cra.TargetLimit) > 0 {
					rs.Spec.Template.Spec.Containers[i].Resources.Limits = cra.TargetLimit.DeepCopy()
				}
			}
		}

		return a.client.Update(ctx, rs)
	})

	return fromResources, toResources, err
}

// evictPodsForVerticalScaling evicts pods to pick up new resource values
func (a *Actuator) evictPodsForVerticalScaling(
	ctx context.Context,
	ha *autoscalingv1alpha1.HybridAutoscaler,
	action coordinator.VerticalAction,
) ([]string, error) {
	logger := log.FromContext(ctx)
	evictedPods := make([]string, 0)

	// Get pods for the target
	pods, err := a.getTargetPods(ctx, ha)
	if err != nil {
		return evictedPods, fmt.Errorf("failed to get target pods: %w", err)
	}

	if len(pods) == 0 {
		return evictedPods, nil
	}

	// Check minimum replicas for eviction
	minReplicas := int32(1)
	if ha.Spec.Vertical != nil && ha.Spec.Vertical.UpdatePolicy != nil && ha.Spec.Vertical.UpdatePolicy.MinReplicas != nil {
		minReplicas = *ha.Spec.Vertical.UpdatePolicy.MinReplicas
	}

	// Count ready pods
	readyPods := 0
	for _, pod := range pods {
		if isPodReady(&pod) {
			readyPods++
		}
	}

	// Calculate how many we can evict
	canEvict := readyPods - int(minReplicas)
	if canEvict <= 0 {
		logger.Info("Cannot evict any pods - at minimum replicas", "ready", readyPods, "min", minReplicas)
		return evictedPods, nil
	}

	// Limit evictions per cycle
	if canEvict > a.config.MaxEvictionsPerCycle {
		canEvict = a.config.MaxEvictionsPerCycle
	}

	// Select pods to evict (prefer oldest pods)
	podsToEvict := selectPodsToEvict(pods, canEvict)

	for _, pod := range podsToEvict {
		logger.Info("Evicting pod for vertical scaling", "pod", pod.Name)

		err := a.evictPod(ctx, &pod)
		if err != nil {
			if errors.IsNotFound(err) {
				// Pod already gone
				continue
			}
			logger.Error(err, "Failed to evict pod", "pod", pod.Name)
			continue
		}

		evictedPods = append(evictedPods, pod.Name)
	}

	return evictedPods, nil
}

// getTargetPods returns pods managed by the target workload
func (a *Actuator) getTargetPods(ctx context.Context, ha *autoscalingv1alpha1.HybridAutoscaler) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}

	// Get label selector from workload
	var labelSelector map[string]string

	switch ha.Spec.TargetRef.Kind {
	case "Deployment":
		deployment := &appsv1.Deployment{}
		if err := a.client.Get(ctx, types.NamespacedName{Namespace: ha.Namespace, Name: ha.Spec.TargetRef.Name}, deployment); err != nil {
			return nil, err
		}
		labelSelector = deployment.Spec.Selector.MatchLabels

	case "StatefulSet":
		sts := &appsv1.StatefulSet{}
		if err := a.client.Get(ctx, types.NamespacedName{Namespace: ha.Namespace, Name: ha.Spec.TargetRef.Name}, sts); err != nil {
			return nil, err
		}
		labelSelector = sts.Spec.Selector.MatchLabels

	case "ReplicaSet":
		rs := &appsv1.ReplicaSet{}
		if err := a.client.Get(ctx, types.NamespacedName{Namespace: ha.Namespace, Name: ha.Spec.TargetRef.Name}, rs); err != nil {
			return nil, err
		}
		labelSelector = rs.Spec.Selector.MatchLabels

	default:
		return nil, fmt.Errorf("unsupported target kind: %s", ha.Spec.TargetRef.Kind)
	}

	listOpts := &client.ListOptions{
		Namespace: ha.Namespace,
	}

	if err := a.client.List(ctx, podList, listOpts, client.MatchingLabels(labelSelector)); err != nil {
		return nil, err
	}

	// Filter to running pods
	runningPods := make([]corev1.Pod, 0)
	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil {
			runningPods = append(runningPods, pod)
		}
	}

	return runningPods, nil
}

// evictPod evicts a single pod
func (a *Actuator) evictPod(ctx context.Context, pod *corev1.Pod) error {
	return a.kubeClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
}

// selectPodsToEvict selects which pods to evict, preferring oldest
func selectPodsToEvict(pods []corev1.Pod, count int) []corev1.Pod {
	if len(pods) == 0 || count <= 0 {
		return nil
	}

	// Sort by creation timestamp (oldest first)
	sorted := make([]corev1.Pod, len(pods))
	copy(sorted, pods)

	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j].CreationTimestamp.Before(&sorted[i].CreationTimestamp) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	if count > len(sorted) {
		count = len(sorted)
	}

	return sorted[:count]
}

// isPodReady checks if a pod is ready
func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
