# HybridAutoscaler

A Kubernetes operator that provides coordinated horizontal and vertical pod autoscaling, solving the conflict between HPA and VPA.

## Overview

The standard Kubernetes HPA (Horizontal Pod Autoscaler) and VPA (Vertical Pod Autoscaler) don't work well together when targeting the same resource metrics. HybridAutoscaler coordinates both scaling dimensions with intelligent decision-making:

- **Prevents oscillation** between horizontal and vertical scaling
- **Configurable coordination strategies** (HorizontalFirst, VerticalFirst, Balanced, Predictive)
- **Smart cooldown periods** to prevent thrashing
- **Stabilization windows** for safer scale-down decisions
- **Per-container resource policies** with min/max bounds
- **Historical metrics analysis** for better recommendations

## Architecture

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
│  │  Metrics    │    │  VPA-style  │    │   Decision     │  │
│  │  Server     │    │  Recommender│    │   Arbiter      │  │
│  └─────────────┘    └─────────────┘    └────────────────┘  │
│                                                │            │
│                                                ▼            │
│                                        ┌────────────────┐  │
│                                        │    Actuator    │  │
│                                        │  (Scale/Evict) │  │
│                                        └────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Installation

### Prerequisites

- Kubernetes 1.26+
- Metrics Server installed
- kubectl configured

### Deploy

```bash
# Install CRDs
kubectl apply -f config/crd/

# Install RBAC
kubectl apply -f config/rbac/

# Deploy the controller
kubectl apply -f config/manager/
```

### Build from source

```bash
# Build
make build

# Build Docker image
make docker-build IMG=your-registry/hybrid-autoscaler:tag

# Push to registry
make docker-push IMG=your-registry/hybrid-autoscaler:tag
```

## Usage

### Basic Example

```yaml
apiVersion: autoscaling.hybridautoscaler.io/v1alpha1
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
  
  vertical:
    updatePolicy:
      updateMode: Auto
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
    strategy: Balanced
    stabilizationWindowSeconds: 300
```

### Coordination Strategies

#### HorizontalFirst
Prioritizes horizontal scaling. Vertical scaling only happens when horizontal limits are reached or horizontal scaling is stable.

Best for: Web services, stateless applications

#### VerticalFirst
Prioritizes vertical scaling when pods are resource-constrained. Falls back to horizontal scaling when vertical limits are reached.

Best for: Memory-intensive workloads, ML inference

#### Balanced (default)
Intelligently chooses between horizontal and vertical based on:
- Current utilization patterns
- Cost efficiency
- Response time requirements

Best for: General-purpose workloads

#### Predictive
Uses historical metrics to anticipate scaling needs (advanced).

Best for: Workloads with predictable patterns

### Configuration Reference

#### Spec Fields

| Field | Description | Default |
|-------|-------------|---------|
| `targetRef` | Reference to the scaling target (Deployment/StatefulSet/ReplicaSet) | Required |
| `horizontal` | Horizontal scaling configuration | Required |
| `vertical` | Vertical scaling configuration | Optional |
| `coordination` | How H/V scaling interact | Optional |
| `behavior` | Fine-grained scaling policies | Optional |

#### Horizontal Configuration

| Field | Description | Default |
|-------|-------------|---------|
| `minReplicas` | Minimum replica count | 1 |
| `maxReplicas` | Maximum replica count | Required |
| `metrics` | Metrics to drive scaling (same as HPA) | CPU@70% |

#### Vertical Configuration

| Field | Description | Default |
|-------|-------------|---------|
| `updatePolicy.updateMode` | Off, Initial, or Auto | Auto |
| `updatePolicy.minReplicas` | Min replicas for eviction | 1 |
| `resourcePolicy` | Per-container min/max bounds | None |

#### Coordination Configuration

| Field | Description | Default |
|-------|-------------|---------|
| `strategy` | HorizontalFirst, VerticalFirst, Balanced, Predictive | Balanced |
| `stabilizationWindowSeconds` | Window for scale-down decisions | 300 |
| `resourceBuffer` | Extra headroom percentage | 15% |
| `cooldownPeriods.scaleUpSeconds` | Cooldown after scale up | 60 |
| `cooldownPeriods.scaleDownSeconds` | Cooldown after scale down | 300 |

### Status Fields

The controller populates these status fields:

```yaml
status:
  currentReplicas: 4
  desiredReplicas: 5
  currentResources:
    my-container:
      cpu: 500m
      memory: 512Mi
  recommendedResources:
    my-container:
      target:
        cpu: 750m
        memory: 640Mi
      lowerBound:
        cpu: 400m
        memory: 400Mi
      upperBound:
        cpu: 1000m
        memory: 1Gi
  conditions:
    - type: Ready
      status: "True"
    - type: ScalingLimited
      status: "False"
  lastScaleTime: "2025-05-20T10:30:00Z"
  scalingHistory:
    - timestamp: "2025-05-20T10:30:00Z"
      type: Horizontal
      fromReplicas: 3
      toReplicas: 4
      reason: "CPU utilization 85% vs target 70%"
      success: true
```

## How It Works

### Reconciliation Loop

1. **Collect Metrics**: Gather CPU/memory usage from Metrics Server
2. **Generate Recommendations**: Analyze metrics to recommend scaling actions
3. **Coordinate**: Apply coordination strategy to resolve H/V conflicts
4. **Apply**: Execute scaling decisions (update replicas, evict pods)
5. **Record**: Update status and emit events

### Conflict Resolution

When both HPA and VPA want to scale:

| HPA Direction | VPA Direction | Balanced Strategy |
|--------------|---------------|-------------------|
| Scale Up | Scale Up | Horizontal (faster response) |
| Scale Down | Scale Down | Horizontal first, then Vertical |
| Scale Up | Scale Down | Horizontal (address load first) |
| Scale Down | Scale Up | Vertical (optimize before shrinking) |

### Safety Features

- **Stabilization Windows**: Prevents rapid oscillation
- **Cooldown Periods**: Enforces minimum time between scaling actions
- **Minimum Replicas**: Ensures enough pods remain for eviction
- **Eviction Limits**: Controls how many pods evicted per cycle

## Monitoring

### Metrics Exposed

The controller exposes Prometheus metrics at `:8080/metrics`:

- `hybrid_autoscaler_current_replicas`
- `hybrid_autoscaler_desired_replicas`
- `hybrid_autoscaler_scaling_events_total`
- `hybrid_autoscaler_recommendation_cpu_millicores`
- `hybrid_autoscaler_recommendation_memory_bytes`

### Events

Watch events for scaling actions:

```bash
kubectl get events --field-selector reason=HorizontalScaling
kubectl get events --field-selector reason=VerticalScaling
```

## Troubleshooting

### Common Issues

**Scaling not happening:**
1. Check if metrics server is running: `kubectl top pods`
2. Verify RBAC permissions
3. Check controller logs: `kubectl logs -n hybrid-autoscaler-system deployment/hybrid-autoscaler-controller-manager`

**Oscillating replicas:**
1. Increase `stabilizationWindowSeconds`
2. Increase cooldown periods
3. Use `Balanced` or `HorizontalFirst` strategy

**Pods not picking up new resources:**
1. Ensure `updateMode: Auto` is set
2. Check `minReplicas` allows eviction
3. Verify no PDB blocking eviction

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `make test`
5. Submit a pull request

## License

Apache License 2.0
