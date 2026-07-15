package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	autoscalingv1alpha1 "github.com/neogan74/hybridautoscaler/api/v1alpha1"
	"github.com/neogan74/hybridautoscaler/internal/actuator"
	"github.com/neogan74/hybridautoscaler/internal/coordinator"
	"github.com/neogan74/hybridautoscaler/internal/metrics"
	"github.com/neogan74/hybridautoscaler/internal/recommender"
)

const (
	// DefaultReconcileInterval is the default interval between reconciliations
	DefaultReconcileInterval = 15 * time.Second

	// MaxScalingHistorySize is the maximum number of scaling events to keep in status
	MaxScalingHistorySize = 10

	// targetRefIndexField indexes HybridAutoscaler resources by target kind/name.
	targetRefIndexField = ".spec.targetRef"
)

// HybridAutoscalerReconciler reconciles a HybridAutoscaler object
type HybridAutoscalerReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	KubeClient    kubernetes.Interface
	MetricsClient metricsclient.Interface
	Recorder      record.EventRecorder

	// Components
	metricsCollector *metrics.Collector
	recommender      *recommender.Recommender
	coordinator      *coordinator.Coordinator
	actuator         *actuator.Actuator

	// Config
	ReconcileInterval time.Duration
}

// NewHybridAutoscalerReconciler creates a new reconciler with all components initialized
func NewHybridAutoscalerReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	kubeClient kubernetes.Interface,
	metricsClient metricsclient.Interface,
	recorder record.EventRecorder,
) *HybridAutoscalerReconciler {
	return &HybridAutoscalerReconciler{
		Client:            client,
		Scheme:            scheme,
		KubeClient:        kubeClient,
		MetricsClient:     metricsClient,
		Recorder:          recorder,
		metricsCollector:  metrics.NewCollector(client, kubeClient, metricsClient),
		recommender:       recommender.NewRecommender(recommender.DefaultRecommenderConfig()),
		coordinator:       coordinator.NewCoordinator(coordinator.DefaultCoordinatorConfig()),
		actuator:          actuator.NewActuator(client, kubeClient, actuator.DefaultActuatorConfig()),
		ReconcileInterval: DefaultReconcileInterval,
	}
}

// +kubebuilder:rbac:groups=autoscaling.hybridautoscaler.io,resources=hybridautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling.hybridautoscaler.io,resources=hybridautoscalers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=autoscaling.hybridautoscaler.io,resources=hybridautoscalers/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets;replicasets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments/scale;statefulsets/scale;replicasets/scale,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=metrics.k8s.io,resources=pods,verbs=get;list

// Reconcile is part of the main kubernetes reconciliation loop
func (r *HybridAutoscalerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the HybridAutoscaler instance
	ha := &autoscalingv1alpha1.HybridAutoscaler{}
	if err := r.Get(ctx, req.NamespacedName, ha); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("HybridAutoscaler not found, ignoring")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get HybridAutoscaler")
		return ctrl.Result{}, err
	}

	// Skip if being deleted
	if ha.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	logger.Info("Reconciling HybridAutoscaler", "target", ha.Spec.TargetRef.Name)

	// Get current state of target workload
	currentState, err := r.getCurrentState(ctx, ha)
	if err != nil {
		logger.Error(err, "Failed to get current state")
		if err := r.updateCondition(ctx, ha, metav1.Condition{
			Type:    "Ready",
			Status:  metav1.ConditionFalse,
			Reason:  "TargetNotFound",
			Message: fmt.Sprintf("Failed to get target: %v", err),
		}); err != nil {
			logger.Error(err, "Failed to update status")
		}
		return ctrl.Result{RequeueAfter: r.ReconcileInterval}, nil
	}

	// Collect metrics
	collectedMetrics, err := r.metricsCollector.Collect(ctx, ha)
	if err != nil {
		logger.Error(err, "Failed to collect metrics")
		r.Recorder.Event(ha, corev1.EventTypeWarning, "MetricsCollectionFailed", err.Error())
		// Continue with potentially stale data
	}

	// Generate recommendations
	recommendations, err := r.recommender.GenerateRecommendations(ctx, ha, collectedMetrics, currentState.Replicas)
	if err != nil {
		logger.Error(err, "Failed to generate recommendations")
		r.Recorder.Event(ha, corev1.EventTypeWarning, "RecommendationFailed", err.Error())
	}

	// Coordinate scaling decision
	decision, err := r.coordinator.Coordinate(ctx, ha, currentState, recommendations, collectedMetrics)
	if err != nil {
		logger.Error(err, "Failed to coordinate scaling")
		r.Recorder.Event(ha, corev1.EventTypeWarning, "CoordinationFailed", err.Error())
	}

	// Apply decision
	key := fmt.Sprintf("%s/%s", ha.Namespace, ha.Name)
	result, err := r.actuator.Apply(ctx, ha, decision)
	if err != nil {
		logger.Error(err, "Failed to apply scaling decision")
		r.Recorder.Event(ha, corev1.EventTypeWarning, "ScalingFailed", err.Error())
	}

	// Record scaling actions for cooldown tracking
	if result != nil {
		if result.HorizontalApplied {
			r.coordinator.RecordHorizontalScale(key, decision.HorizontalAction.Direction)
			r.Recorder.Eventf(ha, corev1.EventTypeNormal, "HorizontalScaling",
				"Scaled %s from %d to %d replicas", ha.Spec.TargetRef.Name,
				currentState.Replicas, result.NewReplicas)
		}
		if result.VerticalApplied {
			r.coordinator.RecordVerticalScale(key, decision.VerticalAction.Direction)
			r.Recorder.Eventf(ha, corev1.EventTypeNormal, "VerticalScaling",
				"Updated resource requests for %s", ha.Spec.TargetRef.Name)
		}
	}

	// Update status
	if err := r.updateStatus(ctx, ha, currentState, collectedMetrics, recommendations, result); err != nil {
		logger.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: r.ReconcileInterval}, nil
}

// getCurrentState retrieves the current state of the target workload
func (r *HybridAutoscalerReconciler) getCurrentState(ctx context.Context, ha *autoscalingv1alpha1.HybridAutoscaler) (*coordinator.CurrentState, error) {
	state := &coordinator.CurrentState{
		ContainerResources: make(map[string]corev1.ResourceRequirements),
	}

	switch ha.Spec.TargetRef.Kind {
	case "Deployment":
		deployment := &appsv1.Deployment{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: ha.Namespace, Name: ha.Spec.TargetRef.Name}, deployment); err != nil {
			return nil, err
		}

		if deployment.Spec.Replicas != nil {
			state.Replicas = *deployment.Spec.Replicas
		}
		state.ReadyReplicas = deployment.Status.ReadyReplicas

		for _, container := range deployment.Spec.Template.Spec.Containers {
			state.ContainerResources[container.Name] = container.Resources
		}

	case "StatefulSet":
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: ha.Namespace, Name: ha.Spec.TargetRef.Name}, sts); err != nil {
			return nil, err
		}

		if sts.Spec.Replicas != nil {
			state.Replicas = *sts.Spec.Replicas
		}
		state.ReadyReplicas = sts.Status.ReadyReplicas

		for _, container := range sts.Spec.Template.Spec.Containers {
			state.ContainerResources[container.Name] = container.Resources
		}

	case "ReplicaSet":
		rs := &appsv1.ReplicaSet{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: ha.Namespace, Name: ha.Spec.TargetRef.Name}, rs); err != nil {
			return nil, err
		}

		if rs.Spec.Replicas != nil {
			state.Replicas = *rs.Spec.Replicas
		}
		state.ReadyReplicas = rs.Status.ReadyReplicas

		for _, container := range rs.Spec.Template.Spec.Containers {
			state.ContainerResources[container.Name] = container.Resources
		}

	default:
		return nil, fmt.Errorf("unsupported target kind: %s", ha.Spec.TargetRef.Kind)
	}

	return state, nil
}

// updateStatus updates the HybridAutoscaler status
func (r *HybridAutoscalerReconciler) updateStatus(
	ctx context.Context,
	ha *autoscalingv1alpha1.HybridAutoscaler,
	currentState *coordinator.CurrentState,
	collectedMetrics *metrics.CollectedMetrics,
	recommendations *recommender.Recommendations,
	result *actuator.ApplyResult,
) error {
	var updatedStatus autoscalingv1alpha1.HybridAutoscalerStatus
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &autoscalingv1alpha1.HybridAutoscaler{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(ha), latest); err != nil {
			return err
		}

		r.populateStatus(latest, currentState, collectedMetrics, recommendations, result)
		updatedStatus = latest.Status
		return r.Status().Update(ctx, latest)
	})
	if err != nil {
		return err
	}

	ha.Status = updatedStatus
	return nil
}

func (r *HybridAutoscalerReconciler) populateStatus(
	ha *autoscalingv1alpha1.HybridAutoscaler,
	currentState *coordinator.CurrentState,
	collectedMetrics *metrics.CollectedMetrics,
	recommendations *recommender.Recommendations,
	result *actuator.ApplyResult,
) {
	// Update replicas
	ha.Status.CurrentReplicas = currentState.Replicas
	ha.Status.DesiredReplicas = currentState.Replicas
	if recommendations != nil && recommendations.Horizontal != nil {
		ha.Status.DesiredReplicas = recommendations.Horizontal.DesiredReplicas
	}

	// Update current resources
	ha.Status.CurrentResources = make(map[string]corev1.ResourceList)
	for name, res := range currentState.ContainerResources {
		ha.Status.CurrentResources[name] = res.Requests.DeepCopy()
	}

	// Update recommended resources
	if recommendations != nil && recommendations.Vertical != nil {
		ha.Status.RecommendedResources = make(map[string]autoscalingv1alpha1.ResourceRecommendation)
		for name, cr := range recommendations.Vertical.ContainerRecommendations {
			ha.Status.RecommendedResources[name] = autoscalingv1alpha1.ResourceRecommendation{
				Target:     cr.Target.DeepCopy(),
				LowerBound: cr.LowerBound.DeepCopy(),
				UpperBound: cr.UpperBound.DeepCopy(),
			}
		}
	}

	// Update recommendations in status
	ha.Status.Recommendations = &autoscalingv1alpha1.Recommendations{}
	if recommendations != nil {
		if recommendations.Horizontal != nil {
			now := metav1.Now()
			ha.Status.Recommendations.Horizontal = &autoscalingv1alpha1.HorizontalRecommendation{
				DesiredReplicas: recommendations.Horizontal.DesiredReplicas,
				Metric:          recommendations.Horizontal.Metric,
				Reason:          recommendations.Horizontal.Reason,
				Timestamp:       &now,
			}
		}
		if recommendations.Vertical != nil {
			now := metav1.Now()
			containerRecs := make(map[string]autoscalingv1alpha1.ResourceRecommendation)
			for name, cr := range recommendations.Vertical.ContainerRecommendations {
				containerRecs[name] = autoscalingv1alpha1.ResourceRecommendation{
					Target:     cr.Target.DeepCopy(),
					LowerBound: cr.LowerBound.DeepCopy(),
					UpperBound: cr.UpperBound.DeepCopy(),
				}
			}
			ha.Status.Recommendations.Vertical = &autoscalingv1alpha1.VerticalRecommendation{
				ContainerRecommendations: containerRecs,
				Reason:                   recommendations.Vertical.Reason,
				Timestamp:                &now,
			}
		}
	}

	// Update metrics in status
	if collectedMetrics != nil && collectedMetrics.Aggregated != nil {
		ha.Status.Metrics = []autoscalingv1alpha1.MetricStatus{
			{
				Type: "Resource",
				Resource: &autoscalingv1alpha1.ResourceMetricStatus{
					Name: corev1.ResourceCPU,
					Current: autoscalingv1alpha1.MetricValueStatus{
						AverageUtilization: int32Ptr(int32(collectedMetrics.Aggregated.AverageCPUUtilization)),
					},
				},
			},
			{
				Type: "Resource",
				Resource: &autoscalingv1alpha1.ResourceMetricStatus{
					Name: corev1.ResourceMemory,
					Current: autoscalingv1alpha1.MetricValueStatus{
						AverageUtilization: int32Ptr(int32(collectedMetrics.Aggregated.AverageMemoryUtilization)),
					},
				},
			},
		}
	}

	// Update scaling history
	if result != nil {
		for _, event := range result.ScalingEvents {
			ha.Status.ScalingHistory = append(ha.Status.ScalingHistory, event)
		}

		// Trim history to max size
		if len(ha.Status.ScalingHistory) > MaxScalingHistorySize {
			ha.Status.ScalingHistory = ha.Status.ScalingHistory[len(ha.Status.ScalingHistory)-MaxScalingHistorySize:]
		}

		// Update last scale time
		if result.HorizontalApplied || result.VerticalApplied {
			now := metav1.Now()
			ha.Status.LastScaleTime = &now
			if result.VerticalApplied {
				ha.Status.LastVerticalScaleTime = &now
			}
		}
	}

	// Update observed generation
	ha.Status.ObservedGeneration = ha.Generation

	// Update conditions
	r.setCondition(ha, metav1.Condition{
		Type:    "Ready",
		Status:  metav1.ConditionTrue,
		Reason:  "Reconciled",
		Message: "HybridAutoscaler is operating normally",
	})

	// Check scaling limited conditions
	if recommendations != nil && recommendations.Horizontal != nil {
		if recommendations.Horizontal.DesiredReplicas > ha.Spec.Horizontal.MaxReplicas {
			r.setCondition(ha, metav1.Condition{
				Type:    "ScalingLimited",
				Status:  metav1.ConditionTrue,
				Reason:  "AtMaxReplicas",
				Message: fmt.Sprintf("Desired replicas (%d) exceeds max (%d)", recommendations.Horizontal.DesiredReplicas, ha.Spec.Horizontal.MaxReplicas),
			})
		} else {
			minReplicas := int32(1)
			if ha.Spec.Horizontal.MinReplicas != nil {
				minReplicas = *ha.Spec.Horizontal.MinReplicas
			}
			if recommendations.Horizontal.DesiredReplicas < minReplicas {
				r.setCondition(ha, metav1.Condition{
					Type:    "ScalingLimited",
					Status:  metav1.ConditionTrue,
					Reason:  "AtMinReplicas",
					Message: fmt.Sprintf("Desired replicas (%d) below min (%d)", recommendations.Horizontal.DesiredReplicas, minReplicas),
				})
			} else {
				r.setCondition(ha, metav1.Condition{
					Type:    "ScalingLimited",
					Status:  metav1.ConditionFalse,
					Reason:  "WithinLimits",
					Message: "Scaling is not limited",
				})
			}
		}
	}
}

// setCondition sets or updates a condition in the status
func (r *HybridAutoscalerReconciler) setCondition(ha *autoscalingv1alpha1.HybridAutoscaler, condition metav1.Condition) {
	condition.LastTransitionTime = metav1.Now()

	for i, existing := range ha.Status.Conditions {
		if existing.Type == condition.Type {
			if existing.Status != condition.Status {
				ha.Status.Conditions[i] = condition
			} else {
				// Keep existing transition time if status hasn't changed
				condition.LastTransitionTime = existing.LastTransitionTime
				ha.Status.Conditions[i] = condition
			}
			return
		}
	}

	ha.Status.Conditions = append(ha.Status.Conditions, condition)
}

func (r *HybridAutoscalerReconciler) updateCondition(ctx context.Context, ha *autoscalingv1alpha1.HybridAutoscaler, condition metav1.Condition) error {
	var updatedStatus autoscalingv1alpha1.HybridAutoscalerStatus
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &autoscalingv1alpha1.HybridAutoscaler{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(ha), latest); err != nil {
			return err
		}

		r.setCondition(latest, condition)
		updatedStatus = latest.Status
		return r.Status().Update(ctx, latest)
	})
	if err != nil {
		return err
	}

	ha.Status = updatedStatus
	return nil
}

// SetupWithManager sets up the controller with the Manager
func (r *HybridAutoscalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &autoscalingv1alpha1.HybridAutoscaler{}, targetRefIndexField, func(rawObj client.Object) []string {
		ha := rawObj.(*autoscalingv1alpha1.HybridAutoscaler)
		return []string{targetRefIndexValue(ha.Spec.TargetRef.Kind, ha.Spec.TargetRef.Name)}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&autoscalingv1alpha1.HybridAutoscaler{}).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(r.requestsForTarget)).
		Watches(&appsv1.StatefulSet{}, handler.EnqueueRequestsFromMapFunc(r.requestsForTarget)).
		Watches(&appsv1.ReplicaSet{}, handler.EnqueueRequestsFromMapFunc(r.requestsForTarget)).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 10,
		}).
		Complete(r)
}

func (r *HybridAutoscalerReconciler) requestsForTarget(ctx context.Context, obj client.Object) []reconcile.Request {
	kind, ok := targetKindForObject(obj)
	if !ok {
		return nil
	}

	haList := &autoscalingv1alpha1.HybridAutoscalerList{}
	if err := r.List(ctx, haList,
		client.InNamespace(obj.GetNamespace()),
		client.MatchingFields{targetRefIndexField: targetRefIndexValue(kind, obj.GetName())},
	); err != nil {
		log.FromContext(ctx).Error(err, "Failed to list HybridAutoscalers for target",
			"kind", kind,
			"namespace", obj.GetNamespace(),
			"name", obj.GetName(),
		)
		return nil
	}

	requests := make([]reconcile.Request, 0, len(haList.Items))
	for _, ha := range haList.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: ha.Namespace,
				Name:      ha.Name,
			},
		})
	}
	return requests
}

func targetKindForObject(obj client.Object) (string, bool) {
	switch obj.(type) {
	case *appsv1.Deployment:
		return "Deployment", true
	case *appsv1.StatefulSet:
		return "StatefulSet", true
	case *appsv1.ReplicaSet:
		return "ReplicaSet", true
	default:
		return "", false
	}
}

func targetRefIndexValue(kind, name string) string {
	return fmt.Sprintf("%s/%s", kind, name)
}

func int32Ptr(i int32) *int32 {
	return &i
}
