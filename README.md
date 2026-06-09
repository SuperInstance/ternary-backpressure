# ternary-backpressure

Backpressure management for GPU pipelines with ternary pressure signals. Adaptive flow control, congestion detection, weighted fairness.

## Why This Matters

# ternary-backpressure
Backpressure management for GPU pipeline stages using ternary pressure signals.
Each stage emits: `+1` (ready for more), `0` (balanced), `-1` (overloaded).
Upstream stages throttle adaptively when downstream signals overload.

## The Five-Layer Stack

This crate is part of the **Oxide Stack** — a distributed GPU runtime built on five layers:

```
┌─────────────────┐
│  cudaclaw        │  Persistent GPU kernels, warp consensus, SmartCRDT
├─────────────────┤
│  cuda-oxide      │  Flux → MIR → Pliron → NVVM → PTX compiler
├─────────────────┤
│  flux-core       │  Bytecode VM + A2A agent protocol
├─────────────────┤
│  pincher         │  "Vector DB as runtime, LLM as compiler"
├─────────────────┤
│  open-parallel   │  Async runtime (tokio fork)
└─────────────────┘
```

The key insight: **ternary values {-1, 0, +1} map directly to GPU compute**. They pack 16× denser than FP32, enable XNOR+popcount matmul, and conservation laws become compile-time checks.

## Design

Every value in this crate follows **ternary algebra** (Z₃):

| Value | Meaning | GPU Analog |
|-------|---------|------------|
| +1 | Positive / Active / Healthy | Warp vote yes |
| 0 | Neutral / Pending / Balanced | Warp vote abstain |
| -1 | Negative / Failed / Overloaded | Warp vote no |

This isn't arbitrary — ternary is the natural encoding for:
1. **BitNet b1.58** (Microsoft) — ternary LLMs at 60% less power
2. **GPU warp voting** — hardware ballot returns ternary consensus
3. **Conservation laws** — {-1, 0, +1} preserves quantity

## Key Types

```rust
pub enum TernaryPressureSignal
pub fn value
pub fn combine
pub struct PipelineStage
pub fn new
pub fn connect_downstream
pub fn pressure_signal
pub fn tick
pub fn enqueue
pub fn utilization
pub struct FlowController
pub fn new
```

## Usage

```toml
[dependencies]
ternary-backpressure = "0.1.0"
```

```rust
use ternary_backpressure::*;
// See src/lib.rs tests for complete working examples
```

## Testing

```bash
git clone https://github.com/SuperInstance/ternary-backpressure.git
cd ternary-backpressure
cargo test    # 10 tests
```

## Stats

| Metric | Value |
|--------|-------|
| Tests | 10 |
| Lines of Rust | 440 |
| Public API | 23 items |

## License

Apache-2.0
