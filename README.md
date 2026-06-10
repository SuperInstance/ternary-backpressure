# ternary-backpressure

Backpressure management for GPU pipeline stages using ternary pressure signals.

## Why This Exists

A GPU inference pipeline has stages: load data → preprocess → infer → postprocess. If the inference stage gets backed up, you need to tell upstream stages to slow down. Binary backpressure (go/stop) causes oscillation. Floating-point backpressure (throttle to 73.4%) is unnecessarily precise. Ternary backpressure uses three signals: `Ready (+1)` — send more, `Balanced (0)` — maintain pace, `Overloaded (-1)` — slow down. These propagate upstream through the pipeline, with each stage combining downstream signals into its own state.

## Architecture

### Core Types

- **`TernaryPressureSignal`** — The three-state signal with `value()` and `combine()` (majority vote).
- **`PipelineStage`** — A processing stage with buffer capacity, processing rate, and weight. Emits its own pressure signal based on buffer fill level.
- **`FlowController`** — Per-stage throttle factors that adapt based on pressure signals.
- **`Pipeline`** — Multi-stage pipeline with congestion detection, pressure propagation, and weighted fair share scheduling.

### Pressure Propagation

Each stage computes its signal from buffer utilization:
- `>70%` full → `Overloaded`
- `30-70%` → `Balanced`
- `<30%` → `Ready`

The pipeline propagates signals upstream and adjusts flow rates via the `FlowController`.

## Usage

```rust
use ternary_backpressure::{Pipeline, PipelineStage};

let mut pipeline = Pipeline::new(0.1); // adaptation rate

pipeline.add_stage(PipelineStage::new(0, 100, 50.0, 1.0)); // load
pipeline.add_stage(PipelineStage::new(1, 50, 30.0, 1.0));  // preprocess
pipeline.add_stage(PipelineStage::new(2, 20, 10.0, 1.0));  // infer (bottleneck)

// Run pipeline ticks
let congested = pipeline.tick();
// Stage 2 (infer) fills up → reports overloaded → stages 0 and 1 throttle

// Check congestion
let spreading = pipeline.is_congestion_spreading();
let congested_stages = pipeline.detect_congestion();

// Get effective rates
let rates = pipeline.weighted_fair_share(100.0);
```

## API Reference

| Method | Returns | Description |
|--------|---------|-------------|
| `PipelineStage::new(id, capacity, rate, weight)` | `PipelineStage` | Create a stage |
| `stage.pressure_signal()` | `TernaryPressureSignal` | Current pressure |
| `stage.enqueue(count)` | `usize` | Buffer items, return accepted count |
| `Pipeline::new(adaptation_rate)` | `Pipeline` | Create pipeline |
| `pipeline.add_stage(stage)` | `()` | Add a processing stage |
| `pipeline.tick()` | `Vec<usize>` | Advance one tick, return congested stages |
| `pipeline.detect_congestion()` | `Vec<usize>` | Currently overloaded stages |
| `pipeline.is_congestion_spreading()` | `bool` | Is congestion cascading |
| `pipeline.propagate_pressure()` | `HashMap<usize, Signal>` | Get all stage signals |
| `pipeline.weighted_fair_share(base)` | `HashMap<usize, f64>` | Fair throughput allocation |

## The Deeper Idea

Ternary backpressure is **TCP congestion control in miniature**. TCP uses binary signals (ACK = good, loss = bad) with exponential backoff. Ternary backpressure adds the middle state: "I'm fine, keep going as-is." This eliminates the oscillation problem where binary systems alternate between full speed and stopped. The `Balanced` state is a stable attractor — the pipeline naturally converges to a state where most stages report balanced most of the time.

## Related Crates

- **ternary-rate-limiter** — rate limiting with ternary feedback
- **ternary-semaphore** — resource permits with ternary capacity
- **ternary-priority-queue** — priority scheduling with ternary scoring
