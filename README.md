# HybridAutoscaler

## The Problem

VPA and HPA don't play well together out of the box:

- HPA scales horizontally based on CPU/memory utilization
- VPA adjusts resource requests/limits vertically
- When both target CPU/memory, they can fight each other and cause oscillations

## Core Concept

A single CRD that orchestrates both scaling dimensions with intelligent coordination:
```yaml
apiVersion: autoscaling.example.io/v1alpha1
kind: HybridAutoscaler
metadata:
  name: my-app-autoscaler
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: my-app
  
  horizontal:
    minReplicas: 2
    maxReplicas: 10
    metrics:
      - type: Resource
        resource:
          name: cpu
          target:
            type: Utilization
            averageUtilization: 70
      - type: External
        external:
          metric:
            name: requests_per_second
          target:
            type: AverageValue
            averageValue: "1000"
  
  vertical:
    updatePolicy:
      updateMode: Auto  # Off, Initial, Auto
    resourcePolicy:
      containerPolicies:
        - containerName: "*"
          minAllowed:
            cpu: 100m
            memory: 128Mi
          maxAllowed:
            cpu: 4
            memory: 8Gi
    
  coordination:
    strategy: VerticalFirst  # HorizontalFirst, VerticalFirst, Balanced
    stabilizationWindowSeconds: 300
    verticalScaleUpThreshold: 0.9   # Scale up vertically when at 90% of current request
    horizontalPreferred: true        # Prefer horizontal when possible
    cooldownPeriods:
      scaleUp: 60s
      scaleDown: 300s
```

### Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                   HybridAutoscaler Controller               │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌────────────────┐  │
│  │   Metrics   │    │ Recommender │    │  Coordinator   │  │
│  │  Collector  │───▶│   Engine    │───▶│    Engine      │  │
│  └─────────────┘    └─────────────┘    └────────────────┘  │
│         │                  │                   │            │
│         ▼                  ▼                   ▼            │
│  ┌─────────────┐    ┌─────────────┐    ┌────────────────┐  │
│  │  Metrics    │    │  VPA        │    │   Decision     │  │
│  │  Server     │    │  Recommender│    │   Arbiter      │  │
│  │  Prometheus │    │  (embedded) │    │                │  │
│  └─────────────┘    └─────────────┘    └────────────────┘  │
│                                                │            │
│                                                ▼            │
│                                        ┌────────────────┐  │
│                                        │    Actuator    │  │
│                                        │  (HPA + VPA)   │  │
│                                        └────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

1. Metrics Collector
```go
type MetricsCollector struct {
    metricsClient   metrics.Interface
    promClient      promapi.Client
    customMetrics   map[string]MetricSource
}

type PodMetrics struct {
    CPU           resource.Quantity
    Memory        resource.Quantity
    CustomMetrics map[string]float64
    Timestamp     time.Time
}
```

2. Recommender Engine
```go
type Recommendation struct {
    Horizontal HorizontalRecommendation
    Vertical   VerticalRecommendation
    Confidence float64
    Reason     string
}

type HorizontalRecommendation struct {
    DesiredReplicas int32
    Metric          string
    CurrentValue    float64
    TargetValue     float64
}

type VerticalRecommendation struct {
    ContainerRecommendations map[string]ContainerResources
    // Target, LowerBound, UpperBound like VPA
}
```