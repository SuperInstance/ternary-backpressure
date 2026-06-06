# ternary-backpressure

**Backpressure management for GPU pipeline stages using ternary pressure signals: Ready (+1), Balanced (0), Overloaded (-1). Adaptive flow control with congestion detection.**

## Background

Backpressure is a fundamental flow control mechanism in distributed systems. Reactive Streams (Java), Akka Streams, and RxJS all implement backpressure to prevent fast producers from overwhelming slow consumers. In GPU pipelines, this is especially critical: a fast data-loading stage can overwhelm a slower inference stage, causing GPU memory exhaustion.

`ternary-backpressure` models pipeline pressure as a **ternary signal**:

| Value | Signal | Meaning |
|-------|--------|---------|
| +1 | Ready | Stage has capacity — send more work |
| 0 | Balanced | Stage is at healthy utilization |
| -1 | Overloaded | Stage is near capacity — throttle upstream |

### Why Ternary Instead of Binary?

Binary backpressure (stop/go) causes **oscillation**: producers alternate between flooding and starving downstream stages. The ternary signal provides a middle state (`Balanced`) that prevents this oscillation. Producers only ramp up when downstream is `Ready` and only throttle when downstream is `Overloaded`. During `Balanced`, they maintain their current rate.

## How It Works

### PipelineStage

Each stage tracks:
- `buffer_capacity`: Maximum work items.
- `buffer_occupancy`: Current items in buffer.
- `processing_rate`: Items processed per tick.
- `downstream`: Connected downstream stages.
- `weight`: Fairness weight for multi-producer scenarios.

### Pressure Signal

```rust
fn pressure_signal(&self) -> TernaryPressureSignal {
    let utilization = occupancy / capacity;
    if utilization > 0.8 → Overloaded (-1)
    if utilization > 0.4 → Balanced (0)
    else → Ready (+1)
}
```

### Signal Combination

When a stage has multiple downstream signals, they combine conservatively:

```
any Overloaded → Overloaded
any Balanced (no Overloaded) → Balanced
all Ready → Ready
```

### FlowController

An adaptive controller adjusts throttle factors per stage:

```rust
fn update(stage_id, signal) → throttle_factor:
    current_factor += match signal:
        Overloaded → -adaptation_rate
        Balanced → 0
        Ready → +adaptation_rate
    clamp(0.0, 1.0)
```

The `throttle_factor` (0.0–1.0) scales the effective send rate. A factor of 0.0 means fully throttled; 1.0 means no throttling.

### Congestion Detection

The pipeline detects **congestion spread** — when multiple connected stages are simultaneously overloaded. This indicates a systemic bottleneck, not a localized issue.

## Experimental Results

The test suite validates:

- **Pressure signal computation**: Stages correctly emit Overloaded/Balanced/Ready based on utilization thresholds.
- **Signal combination**: Multiple downstream signals are combined conservatively (Overloaded wins).
- **Adaptive throttling**: Throttle factors increase/decrease based on pressure signals.
- **Congestion detection**: Overloaded stages are identified.
- **Congestion spread**: Multi-stage congestion propagation is detected.
- **Effective rate computation**: Throttle factors correctly scale base send rates.
- **Pipeline tick simulation**: Stages process items at their configured rate.

## Impact for GPU Cluster Computing

Ternary backpressure is uniquely suited to GPU pipelines:

- **Prevents GPU memory exhaustion**: By throttling upstream stages when GPU buffers fill up, backpressure prevents the OOM errors that crash GPU workloads.
- **Smoother than binary**: The Balanced state prevents the oscillation that binary backpressure causes, resulting in more stable throughput.
- **Multi-stage pipelines**: Real GPU workloads have multiple stages (load → preprocess → inference → postprocess). Ternary backpressure coordinates them automatically.

## Use Cases

1. **ML Inference Pipeline**: Data loading (Stage 1) → Preprocessing (Stage 2) → GPU Inference (Stage 3) → Postprocessing (Stage 4). If inference is slow, backpressure throttles data loading to prevent GPU memory overflow.
2. **Video Processing Pipeline**: Frame decode → GPU filter → GPU encode. If the encoder is the bottleneck, backpressure throttles the decoder.
3. **Multi-GPU Data Parallel**: A parameter server aggregates gradients from multiple GPU workers. Backpressure throttles workers when the parameter server is overloaded.
4. **Stream Processing**: A GPU-accelerated stream processor reads from Kafka, processes on GPU, writes to storage. Backpressure throttles the Kafka consumer when GPU is busy.

## Open Questions

1. **Optimal thresholds**: Are the 0.4/0.8 utilization thresholds optimal for all GPU workloads, or should they be workload-adaptive?
2. **Cross-node backpressure**: Can backpressure signals propagate across network boundaries (GPU-to-GPU across nodes)?
3. **Priority-aware backpressure**: Should high-priority work bypass backpressure throttling?

## Connection to Oxide Stack

`ternary-backpressure` is the **flow control layer**:

| Layer | Crate | Role |
|-------|-------|------|
| 1 — Production | `ternary-priority-queue` | Priority queue feeds the pipeline |
| 2 — Flow Control | **`ternary-backpressure`** | Regulates pipeline flow |
| 3 — Concurrency | `ternary-semaphore` | Semaphore limits per-stage concurrency |
| 4 — Rate Limiting | `ternary-rate-limiter` | Rate limiter caps throughput |
| 5 — Routing | `ternary-routing` | Router adapts to backpressure signals |

Backpressure is the cluster's circulatory system — it ensures data flows at the right rate through every stage.
