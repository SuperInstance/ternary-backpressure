# ternary-backpressure

**Backpressure management for GPU pipelines with ternary pressure signals.**

`ternary-backpressure` provides adaptive flow control for multi-stage processing pipelines. Each stage emits a ternary pressure signal — **Ready (+1)**, **Balanced (0)**, or **Overloaded (−1)** — that propagates upstream to throttle or accelerate producers. Includes congestion detection, weighted fairness allocation, and discrete-event simulation.

## Why It Matters

In GPU pipelines and streaming data systems, a fast upstream producer can overwhelm a slow downstream consumer, causing buffer overflow, latency spikes, and resource starvation. **Backpressure** is the standard solution: downstream stages signal upstream to slow down.

Traditional binary backpressure (go/stop) is coarse — it oscillates between full throughput and zero, creating bursty traffic patterns. **Ternary backpressure** adds a **Balanced** state that enables smooth adaptation: the pipeline operates near equilibrium rather than lurching between extremes.

This crate implements the full stack: per-stage pressure computation, adaptive throttle control, multi-producer weighted fairness, congestion spreading detection, and end-to-end pipeline simulation.

## How It Works

### Ternary Pressure Signals

Each pipeline stage computes a pressure signal from its buffer utilization $u = \text{occupancy} / \text{capacity}$:

$$\text{signal}(u) = \begin{cases} \text{Overloaded} & \text{if } u > 0.8 \\ \text{Balanced} & \text{if } 0.4 < u \leq 0.8 \\ \text{Ready} & \text{if } u \leq 0.4 \end{cases}$$

The thresholds 0.4 and 0.8 create three zones: a green zone (room for more work), a yellow zone (sustainable load), and a red zone (approaching overflow). These can be tuned per stage.

**Signal combination** uses pessimistic aggregation — the worst signal dominates:

$$\text{combine}(\mathbf{s}) = \begin{cases} \text{Overloaded} & \text{if any } s_i = \text{Overloaded} \\ \text{Balanced} & \text{if any } s_i = \text{Balanced} \text{ (and none overloaded)} \\ \text{Ready} & \text{otherwise} \end{cases}$$

This ensures that a single bottleneck propagates upstream conservatively.

**Complexity:** $O(k)$ for $k$ signals being combined.

### Adaptive Flow Control

The `FlowController` adjusts a throttle factor $\tau_i \in [0, 1]$ for each stage $i$ based on downstream pressure:

$$\tau_i^{(t+1)} = \text{clamp}\!\left(\tau_i^{(t)} + \Delta(s),\; 0,\; 1\right)$$

where the update $\Delta$ depends on the signal:

$$\Delta(s) = \begin{cases} -\alpha & \text{if } s = \text{Overloaded} \\ 0 & \text{if } s = \text{Balanced} \\ +\alpha & \text{if } s = \text{Ready} \end{cases}$$

The **adaptation rate** $\alpha \in (0, 1)$ controls responsiveness. Higher $\alpha$ reacts faster but may oscillate; lower $\alpha$ is smoother but slower to converge.

The **effective send rate** for a stage with base rate $r$ is:

$$r_{\text{eff}} = r \cdot \tau_i$$

**Complexity:** $O(1)$ per stage per tick for throttle updates.

### Congestion Detection and Spreading

**Congestion** is detected by scanning all stages for the Overloaded signal. **Congestion spreading** — a more serious condition — occurs when connected stages are simultaneously overloaded:

$$\text{spreading} = \exists\; (i, j) : i \to j \;\wedge\; \text{signal}(i) = \text{signal}(j) = \text{Overloaded}$$

This detects cascading failures where a bottleneck at one stage backs up into its upstream neighbors.

**Complexity:** $O(n)$ for congestion detection; $O(n \cdot \bar{d})$ for spreading detection, where $\bar{d}$ is the average downstream degree.

### Weighted Fairness Allocation

When multiple producers feed a single stage, capacity is allocated proportionally to weights:

$$\text{share}_i = \frac{w_i}{\sum_j w_j} \cdot C_{\text{effective}}$$

where $C_{\text{effective}} = (\text{capacity} - \text{occupancy}) \cdot \tau$ is the throttle-adjusted available capacity. This is a **max-min fair allocation** generalized to weighted shares, analogous to **Weighted Fair Queuing (WFQ)** in network scheduling.

**Complexity:** $O(p)$ for $p$ producers — a single pass to compute total weight and allocate shares.

### Pipeline Simulation

The `tick()` method advances the pipeline by one time unit:

1. Each stage processes $\lfloor \text{rate} \rfloor$ items from its buffer.
2. Pressure signals propagate upstream.
3. Flow controller adjusts throttle factors.
4. Congested stages are reported.

This enables discrete-event simulation of pipeline behavior under various load patterns.

## Quick Start

```toml
[dependencies]
ternary-backpressure = "0.1"
```

```rust
use ternary_backpressure::{Pipeline, PipelineStage, TernaryPressureSignal};

let mut pipeline = Pipeline::new(0.3); // adaptation rate α = 0.3

// Create stages
let mut s0 = PipelineStage::new(0, 100, 5.0, 1.0);
s0.connect_downstream(1);
let mut s1 = PipelineStage::new(1, 100, 3.0, 1.0);
s1.connect_downstream(2);
let s2 = PipelineStage::new(2, 50, 8.0, 1.0);

pipeline.add_stage(s0);
pipeline.add_stage(s1);
pipeline.add_stage(s2);

// Load stage 0 heavily
pipeline.stages.get_mut(&0).unwrap().enqueue(90);

// Simulate ticks
for t in 0..30 {
    let congested = pipeline.tick();
    if !congested.is_empty() {
        println!("tick {}: congested stages = {:?}", t, congested);
    }
}

// Weighted fair share allocation
let producers = vec![(10, 3.0), (20, 1.0), (30, 1.0)];
let shares = pipeline.weighted_fair_share(0, &producers);
println!("Fair shares: {:?}", shares);
```

## API

| Type | Purpose | Key Methods |
|------|---------|-------------|
| `TernaryPressureSignal` | Ready / Balanced / Overloaded enum | `value()`, `combine()` |
| `PipelineStage` | Buffer, rate, weight, downstream links | `pressure_signal()`, `enqueue()`, `tick()`, `utilization()` |
| `FlowController` | Adaptive throttle management | `update()`, `throttle_factor()`, `effective_rate()` |
| `Pipeline` | Multi-stage pipeline with congestion detection | `add_stage()`, `detect_congestion()`, `is_congestion_spreading()`, `propagate_pressure()`, `weighted_fair_share()`, `tick()` |

## Architecture Notes

Backpressure is the flow-control manifestation of the SuperInstance conservation law **γ + η = C**. Data flowing through the pipeline represents **γ** (growth/throughput), while buffer occupancy represents **η** (entropy/accumulated backlog). The conservation law demands:

$$\text{throughput} + \text{backlog} \leq C$$

When backlog grows ($\eta \uparrow$), the Overloaded signal propagates upstream, reducing throughput ($\gamma \downarrow$) to maintain $\gamma + \eta = C$. The throttle factor $\tau$ is the mechanism enforcing this: $\tau$ approaches 0 when $\eta$ approaches $C$, effectively halting new growth.

The ternary signal structure (three zones) provides smoother control than binary backpressure. The Balanced zone is the **equilibrium band** where $\gamma \approx \eta \approx C/2$ — the pipeline operates at half-capacity with maximal stability margin.

## References

- Demers, A. et al. *Efficient Fair Queuing Using Deficit Round-Robin.* IEEE/ACM TON 4(3), 1996. — Weighted fair queueing.
- Parekh, A. & Gallager, R. *A Generalized Processor Sharing Approach to Flow Control.* IEEE/ACM TON 1(3), 1993. — WFQ foundations.
- McKenney, P.E. *Stochastic Fairness Queuing.* INFOCOM 1990. — Fair bandwidth allocation.
- Hopps, C. *Analysis of an Equal-Cost Multi-Path Algorithm.* RFC 2992, 2000. — Flow distribution analysis.

## License

MIT
