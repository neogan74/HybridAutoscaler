# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

HybridAutoscaler is a Kubernetes operator that provides coordinated horizontal and vertical pod autoscaling. It solves the fundamental problem that standard Kubernetes HPA (Horizontal Pod Autoscaler) and VPA (Vertical Pod Autoscaler) conflict when targeting the same resource metrics, causing oscillation and thrashing.

The operator uses a pipeline architecture with four main components:
1. **Metrics Collector** - Gathers CPU/memory metrics from Kubernetes Metrics Server
2. **Recommender Engine** - Generates HPA-style horizontal and VPA-style vertical recommendations using historical analysis
3. **Coordinator Engine** - Resolves conflicts between H/V recommendations using configurable strategies
4. **Actuator** - Applies scaling decisions (scale replicas, update resources, evict pods)

## Essential Commands

### Code Generation (Critical)
```bash
# ALWAYS run after modifying API types in api/v1alpha1/
make generate        # Generate DeepCopy methods
make manifests       # Generate CRDs and RBAC

# Common workflow after API changes:
make generate && make manifests && make fmt && make vet
```

### Build and Development
```bash
make build           # Build manager binary
make run            # Run controller locally against current kubeconfig
make fmt            # Format all Go code
make vet            # Run static analysis
make test           # Run unit tests with coverage
make lint           # Run golangci-lint
```

### Deployment
```bash
# Install CRDs to cluster
make install

# Deploy controller (after building image)
make docker-build IMG=your-registry/hybrid-autoscaler:tag
make docker-push IMG=your-registry/hybrid-autoscaler:tag
make deploy

# Cleanup
make uninstall      # Remove CRDs
make undeploy       # Remove controller
```

## Architecture Deep Dive

### Component Interaction Flow

The reconciliation loop follows this sequence:

1. **Controller** (`internal/controller/hybridautoscaler_controller.go`) receives HybridAutoscaler resource
2. Calls **MetricsCollector** to gather current pod metrics from Metrics Server
3. Calls **Recommender** to generate both horizontal and vertical recommendations
4. Calls **Coordinator** to resolve conflicts and produce unified decision
5. Calls **Actuator** to apply the decision (scale, update templates, evict pods)
6. Updates HybridAutoscaler status with current state, recommendations, and history

### Coordinator Strategies

The coordinator (`internal/coordinator/coordinator.go`) implements four strategies to resolve H/V conflicts:

**HorizontalFirst**: Always prefers horizontal scaling. Vertical only when at replica limits.
- Best for: Stateless web services

**VerticalFirst**: Prefers vertical when pods are resource-constrained (>90% utilization).
- Best for: Memory-intensive workloads, ML inference

**Balanced** (default): Intelligent decision matrix:
- Both scale up → Horizontal (faster response)
- Both scale down → Horizontal first, then vertical
- H up, V down → Horizontal
- H down, V up → Vertical (optimize before shrinking)

**Predictive**: Uses historical patterns (placeholder for future ML-based implementation).

### Key Design Patterns

**Cooldown Tracking**: Coordinator maintains last scale times per HybridAutoscaler instance to enforce cooldown periods and prevent thrashing.

**Stabilization Windows**: For scale-down decisions, the coordinator uses the highest recent recommendation within the stabilization window (conservative approach).

**Eviction Safety**: Actuator respects `minReplicas` from UpdatePolicy and limits evictions per cycle to prevent disruption.

**Historical Analysis**: Recommender maintains time-series data with decay half-life, using weighted percentiles (P95 for CPU, P99 for memory) to generate recommendations.

**Resource Limit Calculation**: When updating requests, actuator maintains the ratio between current requests and limits to preserve QoS class.

## API Structure

### Core Types (`api/v1alpha1/hybridautoscaler_types.go`)

**HybridAutoscalerSpec**:
- `targetRef` - Points to Deployment/StatefulSet/ReplicaSet
- `horizontal` - HPA-like config (minReplicas, maxReplicas, metrics)
- `vertical` - VPA-like config (updatePolicy, resourcePolicy per container)
- `coordination` - Strategy, stabilization windows, cooldown periods, thresholds
- `behavior` - Fine-grained scaling policies (scaleUp/scaleDown rules)

**Important Field Details**:
- `VerticalScaleUpThreshold` and `VerticalScaleDownThreshold` are **strings** (not floats) due to CRD limitations. Use regex pattern validation: `^0(\.[0-9]+)?$|^1(\.0+)?$`
- `UpdateMode` can be Off/Initial/Auto (controls if pods are evicted for VPA)
- `ContainerScalingMode` can be Auto/Off (per-container VPA enable/disable)

### Status Fields

The status provides rich observability:
- `currentReplicas` / `desiredReplicas`
- `currentResources` / `recommendedResources` (per container)
- `recommendations.horizontal` / `recommendations.vertical` (latest with timestamps)
- `metrics[]` - Current CPU/memory utilization
- `scalingHistory[]` - Last 10 scaling events with from/to values
- `conditions[]` - Ready, ScalingLimited

## Component Details

### Recommender (`internal/recommender/recommender.go`)

**Horizontal Recommendations**:
- Processes multiple metrics (Resource, External types)
- Uses standard HPA formula: `desiredReplicas = ceil(currentReplicas * (currentMetric / targetMetric))`
- Returns the metric requiring the **most replicas** as primary

**Vertical Recommendations**:
- Targets P95 for CPU (throttling acceptable), P99 for memory (OOM is worse)
- Applies safety margins (15% default, 22.5% for memory)
- Respects per-container policies (minAllowed, maxAllowed, mode)
- Uses weighted percentiles with time decay (24h half-life)
- Increases memory recommendation if recent OOM events detected

**Historical Data**:
- Stores per-container time series for CPU/memory
- Prunes samples older than `HistoryLength` (24h default)
- OOM and throttling events tracked separately

### Actuator (`internal/actuator/actuator.go`)

**Horizontal Scaling**:
- Updates `.spec.replicas` on Deployment/StatefulSet/ReplicaSet
- Uses `retry.RetryOnConflict` for safe concurrent updates

**Vertical Scaling**:
- Updates pod template with new resource requests/limits
- For `UpdateMode: Auto`, evicts oldest pods (respects minReplicas)
- Limits evictions to `MaxEvictionsPerCycle` (default: 1 per reconciliation)
- Maintains request/limit ratios to preserve QoS class

### Metrics Collector (`internal/metrics/collector.go`)

Interfaces with Kubernetes Metrics Server to collect:
- Per-pod, per-container CPU/memory usage
- Aggregates to calculate average utilization across pods
- Calculates P95/P99 percentiles for recommender
- Supports custom metrics (External metric type)

## Code Generation Requirements

This is a Kubebuilder v4 project. After modifying types in `api/v1alpha1/`:

1. **Run `make generate`** to regenerate:
   - `zz_generated.deepcopy.go` - DeepCopy methods required by controller-runtime

2. **Run `make manifests`** to regenerate:
   - `config/crd/autoscaling.hybridautoscaler.io_hybridautoscalers.yaml` - CRD with OpenAPI schema
   - `config/rbac/role.yaml` - RBAC from `+kubebuilder:rbac` markers

**Common markers used**:
- `+kubebuilder:validation:Enum` - Enum validation
- `+kubebuilder:validation:Minimum/Maximum` - Numeric bounds
- `+kubebuilder:validation:Pattern` - Regex validation
- `+kubebuilder:validation:Required` - Required field
- `+kubebuilder:default` - Default value
- `+optional` - Optional field documentation

**Important**: Float fields in CRDs must use string type with pattern validation due to controller-gen limitations.

## RBAC Permissions

The controller requires:
- HybridAutoscaler resources: full CRUD
- Deployments/StatefulSets/ReplicaSets: get, list, watch, update, patch
- Scale subresources: get, update, patch
- Pods: get, list, watch, delete (for eviction)
- Pods (metrics.k8s.io): get, list (Metrics Server)
- Events: create, patch

Markers are in `internal/controller/hybridautoscaler_controller.go`.

## Testing Strategy

**Unit Tests**: Test individual components (recommender logic, coordinator strategies)
**Integration Tests**: Use envtest to test controller with fake K8s API
**E2E Tests**: Require real cluster with Metrics Server

The controller reconciles every 15 seconds by default (`DefaultReconcileInterval`).

## Common Patterns

**Pointer Fields**: Many spec fields are pointers for optional/default behavior (e.g., `MinReplicas *int32`). Always check nil before dereferencing.

**DeepCopy Usage**: Always `.DeepCopy()` when storing or returning K8s resource types to prevent mutation bugs.

**Context Propagation**: All functions accept `context.Context` for cancellation and logging. Use `log.FromContext(ctx)` to get logger.

**Retry Logic**: Use `retry.RetryOnConflict` when updating resources to handle concurrent modifications.

**Max History**: Status limits `scalingHistory` to 10 most recent events to prevent unbounded growth.

## Configuration Defaults

- Reconcile interval: 15s
- Stabilization window: 300s (5 min)
- Scale up cooldown: 60s (horizontal), 120s (vertical)
- Scale down cooldown: 300s (horizontal), 600s (vertical)
- Target CPU utilization: 70%
- Target memory utilization: 80%
- Safety margin: 15%
- History length: 24 hours
- Max evictions per cycle: 1 pod

## Project Structure

Standard Kubebuilder v4 layout:
- `/api/v1alpha1/` - CRD types and scheme
- `/cmd/main.go` - Controller entrypoint
- `/internal/controller/` - Reconciler
- `/internal/coordinator/` - Coordination logic
- `/internal/recommender/` - Recommendation engine
- `/internal/actuator/` - Scaling execution
- `/internal/metrics/` - Metrics collection
- `/config/` - Kustomize manifests (CRD, RBAC, manager, samples)
- `/hack/boilerplate.go.txt` - License header for generated code

## Go Version

Requires Go 1.25.4 (specified in `go.mod`).
Uses controller-runtime v0.22.4 and Kubernetes v0.34.2 client libraries.
