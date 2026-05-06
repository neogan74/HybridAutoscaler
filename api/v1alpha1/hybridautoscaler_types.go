package v1alpha1

import (
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ha
// +kubebuilder:printcolumn:name="Target",type="string",JSONPath=".spec.targetRef.name"
// +kubebuilder:printcolumn:name="Min",type="integer",JSONPath=".spec.horizontal.minReplicas"
// +kubebuilder:printcolumn:name="Max",type="integer",JSONPath=".spec.horizontal.maxReplicas"
// +kubebuilder:printcolumn:name="Replicas",type="integer",JSONPath=".status.currentReplicas"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// HybridAutoscaler is the Schema for the hybridautoscalers API
// It provides coordinated horizontal and vertical pod autoscaling
type HybridAutoscaler struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HybridAutoscalerSpec   `json:"spec,omitempty"`
	Status HybridAutoscalerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// HybridAutoscalerList contains a list of HybridAutoscaler
type HybridAutoscalerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HybridAutoscaler `json:"items"`
}

// HybridAutoscalerSpec defines the desired state of HybridAutoscaler
type HybridAutoscalerSpec struct {
	// TargetRef points to the controller managing the set of pods
	// +kubebuilder:validation:Required
	TargetRef CrossVersionObjectReference `json:"targetRef"`

	// Horizontal defines horizontal scaling configuration
	// +kubebuilder:validation:Required
	Horizontal HorizontalSpec `json:"horizontal"`

	// Vertical defines vertical scaling configuration
	// +optional
	Vertical *VerticalSpec `json:"vertical,omitempty"`

	// Coordination defines how horizontal and vertical scaling interact
	// +optional
	Coordination *CoordinationSpec `json:"coordination,omitempty"`

	// Behavior configures scaling behavior
	// +optional
	Behavior *ScalingBehavior `json:"behavior,omitempty"`
}

// CrossVersionObjectReference contains enough information to identify the referred resource
type CrossVersionObjectReference struct {
	// APIVersion of the referent
	// +optional
	APIVersion string `json:"apiVersion,omitempty"`

	// Kind of the referent
	// +kubebuilder:validation:Required
	Kind string `json:"kind"`

	// Name of the referent
	// +kubebuilder:validation:Required
	Name string `json:"name"`
}

// HorizontalSpec defines horizontal scaling parameters
type HorizontalSpec struct {
	// MinReplicas is the lower limit for the number of replicas
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	// +optional
	MinReplicas *int32 `json:"minReplicas,omitempty"`

	// MaxReplicas is the upper limit for the number of replicas
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Required
	MaxReplicas int32 `json:"maxReplicas"`

	// Metrics contains the specifications for which to use to calculate the desired replica count
	// +optional
	Metrics []autoscalingv2.MetricSpec `json:"metrics,omitempty"`

	// ScaleTargetRef is an optional direct reference to scale (if different from targetRef)
	// +optional
	ScaleTargetRef *CrossVersionObjectReference `json:"scaleTargetRef,omitempty"`
}

// VerticalSpec defines vertical scaling parameters
type VerticalSpec struct {
	// UpdatePolicy controls how the autoscaler applies changes to the pod resources
	// +optional
	UpdatePolicy *UpdatePolicy `json:"updatePolicy,omitempty"`

	// ResourcePolicy controls how individual containers are vertically scaled
	// +optional
	ResourcePolicy *ResourcePolicy `json:"resourcePolicy,omitempty"`

	// RecommenderName specifies which recommender to use (default: built-in)
	// +optional
	RecommenderName string `json:"recommenderName,omitempty"`
}

// UpdatePolicy describes how VPA updates are applied
type UpdatePolicy struct {
	// UpdateMode controls how updates are applied
	// +kubebuilder:validation:Enum=Off;Initial;Auto
	// +kubebuilder:default=Auto
	UpdateMode UpdateMode `json:"updateMode,omitempty"`

	// MinReplicas is the minimum number of replicas that need to be running
	// for the updater to attempt a pod eviction
	// +kubebuilder:validation:Minimum=0
	// +optional
	MinReplicas *int32 `json:"minReplicas,omitempty"`

	// EvictionRequirements specifies conditions that need to be met for eviction
	// +optional
	EvictionRequirements []EvictionRequirement `json:"evictionRequirements,omitempty"`
}

// UpdateMode controls how VPA updates pods
// +kubebuilder:validation:Enum=Off;Initial;Auto
type UpdateMode string

const (
	// UpdateModeOff disables automatic updates
	UpdateModeOff UpdateMode = "Off"

	// UpdateModeInitial only applies recommendations at pod creation
	UpdateModeInitial UpdateMode = "Initial"

	// UpdateModeAuto applies recommendations by evicting and recreating pods
	UpdateModeAuto UpdateMode = "Auto"
)

// EvictionRequirement defines a condition that must be met for eviction
type EvictionRequirement struct {
	// Resources is the list of resources that must meet the change threshold
	Resources []corev1.ResourceName `json:"resources,omitempty"`

	// ChangeRequirement specifies the required change
	ChangeRequirement ChangeRequirement `json:"changeRequirement"`
}

// ChangeRequirement specifies the type of change required
// +kubebuilder:validation:Enum=TargetHigherThanRequests;TargetLowerThanRequests
type ChangeRequirement string

const (
	TargetHigherThanRequests ChangeRequirement = "TargetHigherThanRequests"
	TargetLowerThanRequests  ChangeRequirement = "TargetLowerThanRequests"
)

// ResourcePolicy controls how individual containers are vertically scaled
type ResourcePolicy struct {
	// ContainerPolicies are per-container policies
	// +optional
	ContainerPolicies []ContainerResourcePolicy `json:"containerPolicies,omitempty"`
}

// ContainerResourcePolicy controls vertical scaling for a specific container
type ContainerResourcePolicy struct {
	// ContainerName is the name of the container (or "*" for all)
	// +kubebuilder:validation:Required
	ContainerName string `json:"containerName"`

	// Mode controls whether autoscaling is enabled for this container
	// +kubebuilder:validation:Enum=Auto;Off
	// +kubebuilder:default=Auto
	// +optional
	Mode *ContainerScalingMode `json:"mode,omitempty"`

	// MinAllowed specifies the minimum resource requests
	// +optional
	MinAllowed corev1.ResourceList `json:"minAllowed,omitempty"`

	// MaxAllowed specifies the maximum resource requests
	// +optional
	MaxAllowed corev1.ResourceList `json:"maxAllowed,omitempty"`

	// ControlledResources specifies which resources are managed
	// +optional
	ControlledResources []corev1.ResourceName `json:"controlledResources,omitempty"`

	// ControlledValues specifies which resource values are controlled
	// +kubebuilder:validation:Enum=RequestsAndLimits;RequestsOnly
	// +optional
	ControlledValues *ContainerControlledValues `json:"controlledValues,omitempty"`
}

// ContainerScalingMode controls whether autoscaling is enabled for a container
// +kubebuilder:validation:Enum=Auto;Off
type ContainerScalingMode string

const (
	ContainerScalingModeAuto ContainerScalingMode = "Auto"
	ContainerScalingModeOff  ContainerScalingMode = "Off"
)

// ContainerControlledValues specifies which resource values are controlled
// +kubebuilder:validation:Enum=RequestsAndLimits;RequestsOnly
type ContainerControlledValues string

const (
	ContainerControlledValuesRequestsAndLimits ContainerControlledValues = "RequestsAndLimits"
	ContainerControlledValuesRequestsOnly      ContainerControlledValues = "RequestsOnly"
)

// CoordinationSpec defines how horizontal and vertical scaling interact
type CoordinationSpec struct {
	// Strategy defines the coordination strategy
	// +kubebuilder:validation:Enum=HorizontalFirst;VerticalFirst;Balanced;Predictive
	// +kubebuilder:default=Balanced
	Strategy CoordinationStrategy `json:"strategy,omitempty"`

	// StabilizationWindowSeconds defines the time window for stabilization
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=300
	// +optional
	StabilizationWindowSeconds *int32 `json:"stabilizationWindowSeconds,omitempty"`

	// VerticalScaleUpThreshold is the utilization threshold for vertical scale up (0.0-1.0)
	// +kubebuilder:validation:Pattern=`^0(\.[0-9]+)?$|^1(\.0+)?$`
	// +optional
	VerticalScaleUpThreshold *string `json:"verticalScaleUpThreshold,omitempty"`

	// VerticalScaleDownThreshold is the utilization threshold for vertical scale down (0.0-1.0)
	// +kubebuilder:validation:Pattern=`^0(\.[0-9]+)?$|^1(\.0+)?$`
	// +optional
	VerticalScaleDownThreshold *string `json:"verticalScaleDownThreshold,omitempty"`

	// HorizontalPriority gives precedence to horizontal scaling when true
	// +optional
	HorizontalPriority bool `json:"horizontalPriority,omitempty"`

	// ResourceBuffer is extra headroom to add to recommendations (percentage)
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=15
	// +optional
	ResourceBuffer *int32 `json:"resourceBuffer,omitempty"`

	// CooldownPeriods defines cooldown periods for scaling actions
	// +optional
	CooldownPeriods *CooldownPeriods `json:"cooldownPeriods,omitempty"`
}

// CoordinationStrategy defines how horizontal and vertical scaling interact
// +kubebuilder:validation:Enum=HorizontalFirst;VerticalFirst;Balanced;Predictive
type CoordinationStrategy string

const (
	// HorizontalFirst prioritizes horizontal scaling
	HorizontalFirst CoordinationStrategy = "HorizontalFirst"

	// VerticalFirst prioritizes vertical scaling
	VerticalFirst CoordinationStrategy = "VerticalFirst"

	// Balanced tries to find optimal balance between both
	Balanced CoordinationStrategy = "Balanced"

	// Predictive uses historical data to predict scaling needs
	Predictive CoordinationStrategy = "Predictive"
)

// CooldownPeriods defines cooldown durations
type CooldownPeriods struct {
	// ScaleUpSeconds is cooldown after scale up
	// +kubebuilder:default=60
	// +optional
	ScaleUpSeconds *int32 `json:"scaleUpSeconds,omitempty"`

	// ScaleDownSeconds is cooldown after scale down
	// +kubebuilder:default=300
	// +optional
	ScaleDownSeconds *int32 `json:"scaleDownSeconds,omitempty"`

	// VerticalScaleUpSeconds is cooldown after vertical scale up
	// +kubebuilder:default=120
	// +optional
	VerticalScaleUpSeconds *int32 `json:"verticalScaleUpSeconds,omitempty"`

	// VerticalScaleDownSeconds is cooldown after vertical scale down
	// +kubebuilder:default=600
	// +optional
	VerticalScaleDownSeconds *int32 `json:"verticalScaleDownSeconds,omitempty"`
}

// ScalingBehavior configures the scaling behavior of the target
type ScalingBehavior struct {
	// ScaleUp is scaling policy for scaling Up
	// +optional
	ScaleUp *ScalingRules `json:"scaleUp,omitempty"`

	// ScaleDown is scaling policy for scaling Down
	// +optional
	ScaleDown *ScalingRules `json:"scaleDown,omitempty"`
}

// ScalingRules configures the scaling behavior for one direction
type ScalingRules struct {
	// StabilizationWindowSeconds is the number of seconds for which past recommendations
	// should be considered while scaling up or scaling down
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=3600
	// +optional
	StabilizationWindowSeconds *int32 `json:"stabilizationWindowSeconds,omitempty"`

	// SelectPolicy is used to specify which policy should be used
	// +kubebuilder:validation:Enum=Max;Min;Disabled
	// +optional
	SelectPolicy *ScalingPolicySelect `json:"selectPolicy,omitempty"`

	// Policies is a list of potential scaling polices
	// +optional
	Policies []ScalingPolicy `json:"policies,omitempty"`
}

// ScalingPolicySelect specifies which policy to use
// +kubebuilder:validation:Enum=Max;Min;Disabled
type ScalingPolicySelect string

const (
	MaxChangePolicySelect ScalingPolicySelect = "Max"
	MinChangePolicySelect ScalingPolicySelect = "Min"
	DisabledPolicySelect  ScalingPolicySelect = "Disabled"
)

// ScalingPolicy is a single policy which must hold true for a specified past interval
type ScalingPolicy struct {
	// Type is used to specify the scaling policy
	// +kubebuilder:validation:Enum=Pods;Percent
	Type ScalingPolicyType `json:"type"`

	// Value contains the amount of change which is permitted by the policy
	// +kubebuilder:validation:Minimum=0
	Value int32 `json:"value"`

	// PeriodSeconds specifies the window of time for which the policy should hold true
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1800
	PeriodSeconds int32 `json:"periodSeconds"`
}

// ScalingPolicyType specifies the type of scaling policy
// +kubebuilder:validation:Enum=Pods;Percent
type ScalingPolicyType string

const (
	PodsScalingPolicy    ScalingPolicyType = "Pods"
	PercentScalingPolicy ScalingPolicyType = "Percent"
)

// HybridAutoscalerStatus defines the observed state of HybridAutoscaler
type HybridAutoscalerStatus struct {
	// CurrentReplicas is the current number of replicas
	CurrentReplicas int32 `json:"currentReplicas,omitempty"`

	// DesiredReplicas is the desired number of replicas
	DesiredReplicas int32 `json:"desiredReplicas,omitempty"`

	// CurrentResources contains current resource allocation per container
	// +optional
	CurrentResources map[string]corev1.ResourceList `json:"currentResources,omitempty"`

	// RecommendedResources contains recommended resource allocation per container
	// +optional
	RecommendedResources map[string]ResourceRecommendation `json:"recommendedResources,omitempty"`

	// LastScaleTime is the last time the HybridAutoscaler scaled
	// +optional
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`

	// LastVerticalScaleTime is the last time vertical scaling was applied
	// +optional
	LastVerticalScaleTime *metav1.Time `json:"lastVerticalScaleTime,omitempty"`

	// ObservedGeneration is the most recent generation observed
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ScalingHistory contains recent scaling events
	// +optional
	ScalingHistory []ScalingEvent `json:"scalingHistory,omitempty"`

	// Recommendations contains the latest recommendations
	// +optional
	Recommendations *Recommendations `json:"recommendations,omitempty"`

	// Metrics contains current metric values
	// +optional
	Metrics []MetricStatus `json:"metrics,omitempty"`
}

// ResourceRecommendation contains recommendations for a container
type ResourceRecommendation struct {
	// Target is the recommended resource request
	Target corev1.ResourceList `json:"target,omitempty"`

	// LowerBound is the minimum recommended resource request
	LowerBound corev1.ResourceList `json:"lowerBound,omitempty"`

	// UpperBound is the maximum recommended resource request
	UpperBound corev1.ResourceList `json:"upperBound,omitempty"`

	// UncappedTarget is the recommendation without policy constraints
	UncappedTarget corev1.ResourceList `json:"uncappedTarget,omitempty"`
}

// Recommendations contains both horizontal and vertical recommendations
type Recommendations struct {
	// Horizontal contains horizontal scaling recommendations
	// +optional
	Horizontal *HorizontalRecommendation `json:"horizontal,omitempty"`

	// Vertical contains vertical scaling recommendations
	// +optional
	Vertical *VerticalRecommendation `json:"vertical,omitempty"`
}

// HorizontalRecommendation contains horizontal scaling recommendation details
type HorizontalRecommendation struct {
	// DesiredReplicas is the recommended replica count
	DesiredReplicas int32 `json:"desiredReplicas"`

	// Metric is the metric that drove the recommendation
	// +optional
	Metric string `json:"metric,omitempty"`

	// CurrentValue is the current value of the metric
	// +optional
	CurrentValue *resource.Quantity `json:"currentValue,omitempty"`

	// TargetValue is the target value of the metric
	// +optional
	TargetValue *resource.Quantity `json:"targetValue,omitempty"`

	// Reason explains the recommendation
	// +optional
	Reason string `json:"reason,omitempty"`

	// Timestamp is when this recommendation was generated
	// +optional
	Timestamp *metav1.Time `json:"timestamp,omitempty"`
}

// VerticalRecommendation contains vertical scaling recommendation details
type VerticalRecommendation struct {
	// ContainerRecommendations contains per-container recommendations
	ContainerRecommendations map[string]ResourceRecommendation `json:"containerRecommendations,omitempty"`

	// Reason explains the recommendation
	// +optional
	Reason string `json:"reason,omitempty"`

	// Timestamp is when this recommendation was generated
	// +optional
	Timestamp *metav1.Time `json:"timestamp,omitempty"`
}

// ScalingEvent records a scaling action
type ScalingEvent struct {
	// Timestamp is when the event occurred
	Timestamp metav1.Time `json:"timestamp"`

	// Type is the type of scaling (Horizontal or Vertical)
	// +kubebuilder:validation:Enum=Horizontal;Vertical
	Type ScalingType `json:"type"`

	// FromReplicas is the previous replica count (for horizontal)
	// +optional
	FromReplicas *int32 `json:"fromReplicas,omitempty"`

	// ToReplicas is the new replica count (for horizontal)
	// +optional
	ToReplicas *int32 `json:"toReplicas,omitempty"`

	// FromResources is the previous resource allocation (for vertical)
	// +optional
	FromResources map[string]corev1.ResourceList `json:"fromResources,omitempty"`

	// ToResources is the new resource allocation (for vertical)
	// +optional
	ToResources map[string]corev1.ResourceList `json:"toResources,omitempty"`

	// Reason explains why the scaling occurred
	Reason string `json:"reason"`

	// Success indicates if the scaling was successful
	Success bool `json:"success"`

	// Message contains additional details
	// +optional
	Message string `json:"message,omitempty"`
}

// ScalingType represents the type of scaling action
// +kubebuilder:validation:Enum=Horizontal;Vertical
type ScalingType string

const (
	HorizontalScaling ScalingType = "Horizontal"
	VerticalScaling   ScalingType = "Vertical"
)

// MetricStatus contains the current value of a metric
type MetricStatus struct {
	// Type is the type of metric source
	Type autoscalingv2.MetricSourceType `json:"type"`

	// Resource contains current value of a resource metric
	// +optional
	Resource *ResourceMetricStatus `json:"resource,omitempty"`

	// External contains current value of an external metric
	// +optional
	External *ExternalMetricStatus `json:"external,omitempty"`

	// Pods contains current value of a pods metric
	// +optional
	Pods *PodsMetricStatus `json:"pods,omitempty"`

	// Object contains current value of an object metric
	// +optional
	Object *ObjectMetricStatus `json:"object,omitempty"`
}

// ResourceMetricStatus contains the current value of a resource metric
type ResourceMetricStatus struct {
	// Name is the name of the resource
	Name corev1.ResourceName `json:"name"`

	// Current contains the current value for the given metric
	Current MetricValueStatus `json:"current"`
}

// ExternalMetricStatus contains the current value of an external metric
type ExternalMetricStatus struct {
	// Metric identifies the external metric
	Metric autoscalingv2.MetricIdentifier `json:"metric"`

	// Current contains the current value for the given metric
	Current MetricValueStatus `json:"current"`
}

// PodsMetricStatus contains the current value of a pods metric
type PodsMetricStatus struct {
	// Metric identifies the pods metric
	Metric autoscalingv2.MetricIdentifier `json:"metric"`

	// Current contains the current value for the given metric
	Current MetricValueStatus `json:"current"`
}

// ObjectMetricStatus contains the current value of an object metric
type ObjectMetricStatus struct {
	// Metric identifies the object metric
	Metric autoscalingv2.MetricIdentifier `json:"metric"`

	// DescribedObject specifies the descriptions of a object
	DescribedObject CrossVersionObjectReference `json:"describedObject"`

	// Current contains the current value for the given metric
	Current MetricValueStatus `json:"current"`
}

// MetricValueStatus holds the current value of a metric
type MetricValueStatus struct {
	// Value is the current value of the metric (as a quantity)
	// +optional
	Value *resource.Quantity `json:"value,omitempty"`

	// AverageValue is the average value of the metric
	// +optional
	AverageValue *resource.Quantity `json:"averageValue,omitempty"`

	// AverageUtilization is the average utilization as a percentage
	// +optional
	AverageUtilization *int32 `json:"averageUtilization,omitempty"`
}

func init() {
	SchemeBuilder.Register(&HybridAutoscaler{}, &HybridAutoscalerList{})
}
