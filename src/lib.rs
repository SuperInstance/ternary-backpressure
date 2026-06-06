//! # ternary-backpressure
//!
//! Backpressure management for GPU pipeline stages using ternary pressure signals.
//!
//! Each stage emits: `+1` (ready for more), `0` (balanced), `-1` (overloaded).
//! Upstream stages throttle adaptively when downstream signals overload.

use std::collections::HashMap;

/// Ternary pressure signal emitted by pipeline stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TernaryPressureSignal {
    /// Ready for more work.
    Ready = 1,
    /// Balanced — current load is sustainable.
    Balanced = 0,
    /// Overloaded — upstream should throttle.
    Overloaded = -1,
}

impl TernaryPressureSignal {
    /// Numeric value of the signal.
    pub fn value(&self) -> i8 {
        match self {
            TernaryPressureSignal::Ready => 1,
            TernaryPressureSignal::Balanced => 0,
            TernaryPressureSignal::Overloaded => -1,
        }
    }

    /// Combine multiple signals: if any is Overloaded, result is Overloaded.
    /// If any is Balanced (and none overloaded), result is Balanced.
    /// Otherwise Ready.
    pub fn combine(signals: &[TernaryPressureSignal]) -> TernaryPressureSignal {
        if signals.is_empty() {
            return TernaryPressureSignal::Balanced;
        }
        if signals.iter().any(|s| *s == TernaryPressureSignal::Overloaded) {
            return TernaryPressureSignal::Overloaded;
        }
        if signals.iter().any(|s| *s == TernaryPressureSignal::Balanced) {
            return TernaryPressureSignal::Balanced;
        }
        TernaryPressureSignal::Ready
    }
}

/// A single stage in a GPU processing pipeline.
#[derive(Debug, Clone)]
pub struct PipelineStage {
    /// Unique stage identifier.
    pub id: usize,
    /// Maximum buffer capacity (number of work items).
    pub buffer_capacity: usize,
    /// Current buffer occupancy.
    pub buffer_occupancy: usize,
    /// Processing rate (items per tick).
    pub processing_rate: f64,
    /// Downstream stage IDs.
    pub downstream: Vec<usize>,
    /// Weight for multi-producer fairness (higher = more share).
    pub weight: f64,
}

impl PipelineStage {
    /// Create a new pipeline stage.
    pub fn new(id: usize, buffer_capacity: usize, processing_rate: f64, weight: f64) -> Self {
        Self {
            id,
            buffer_capacity,
            buffer_occupancy: 0,
            processing_rate,
            downstream: Vec::new(),
            weight,
        }
    }

    /// Connect this stage to a downstream stage.
    pub fn connect_downstream(&mut self, downstream_id: usize) {
        if !self.downstream.contains(&downstream_id) {
            self.downstream.push(downstream_id);
        }
    }

    /// Compute the current pressure signal based on buffer occupancy and processing rate.
    pub fn pressure_signal(&self) -> TernaryPressureSignal {
        let utilization = self.buffer_occupancy as f64 / self.buffer_capacity as f64;
        if utilization > 0.8 {
            TernaryPressureSignal::Overloaded
        } else if utilization > 0.4 {
            TernaryPressureSignal::Balanced
        } else {
            TernaryPressureSignal::Ready
        }
    }

    /// Simulate one tick: process items at the processing rate.
    pub fn tick(&mut self) {
        let processed = (self.processing_rate) as usize;
        self.buffer_occupancy = self.buffer_occupancy.saturating_sub(processed);
    }

    /// Enqueue items into the buffer. Returns the number actually accepted.
    pub fn enqueue(&mut self, count: usize) -> usize {
        let available = self.buffer_capacity.saturating_sub(self.buffer_occupancy);
        let accepted = available.min(count);
        self.buffer_occupancy += accepted;
        accepted
    }

    /// Current utilization ratio (0.0–1.0).
    pub fn utilization(&self) -> f64 {
        self.buffer_occupancy as f64 / self.buffer_capacity as f64
    }
}

/// Adaptive flow controller that throttles upstream based on downstream signals.
#[derive(Debug, Clone)]
pub struct FlowController {
    /// Current throttle factor per stage (0.0 = fully throttled, 1.0 = no throttle).
    throttle_factors: HashMap<usize, f64>,
    /// Rate of adaptation (how fast throttle responds).
    adaptation_rate: f64,
}

impl FlowController {
    /// Create a new flow controller with the given adaptation rate.
    pub fn new(adaptation_rate: f64) -> Self {
        Self {
            throttle_factors: HashMap::new(),
            adaptation_rate,
        }
    }

    /// Update the throttle factor for a stage based on downstream pressure.
    pub fn update(&mut self, stage_id: usize, signal: TernaryPressureSignal) -> f64 {
        let current = self.throttle_factors.get(&stage_id).copied().unwrap_or(1.0);
        let delta = match signal {
            TernaryPressureSignal::Overloaded => -self.adaptation_rate,
            TernaryPressureSignal::Balanced => 0.0,
            TernaryPressureSignal::Ready => self.adaptation_rate,
        };
        let new_factor = (current + delta).clamp(0.0, 1.0);
        self.throttle_factors.insert(stage_id, new_factor);
        new_factor
    }

    /// Get the current throttle factor for a stage.
    pub fn throttle_factor(&self, stage_id: usize) -> f64 {
        self.throttle_factors.get(&stage_id).copied().unwrap_or(1.0)
    }

    /// Compute the effective send rate for a stage given its base rate.
    pub fn effective_rate(&self, stage_id: usize, base_rate: f64) -> f64 {
        base_rate * self.throttle_factor(stage_id)
    }
}

/// A multi-stage pipeline with congestion detection and weighted fairness.
#[derive(Debug, Clone)]
pub struct Pipeline {
    /// All stages indexed by ID.
    pub stages: HashMap<usize, PipelineStage>,
    /// Flow controller for adaptive throttling.
    pub flow_controller: FlowController,
}

impl Pipeline {
    /// Create a new pipeline with the given adaptation rate.
    pub fn new(adaptation_rate: f64) -> Self {
        Self {
            stages: HashMap::new(),
            flow_controller: FlowController::new(adaptation_rate),
        }
    }

    /// Add a stage to the pipeline.
    pub fn add_stage(&mut self, stage: PipelineStage) {
        self.stages.insert(stage.id, stage);
    }

    /// Detect congestion: returns stage IDs that are currently overloaded.
    pub fn detect_congestion(&self) -> Vec<usize> {
        self.stages
            .values()
            .filter(|s| s.pressure_signal() == TernaryPressureSignal::Overloaded)
            .map(|s| s.id)
            .collect()
    }

    /// Detect congestion spread: returns true if congestion propagates across
    /// multiple connected stages.
    pub fn is_congestion_spreading(&self) -> bool {
        let congested: Vec<usize> = self.detect_congestion();
        if congested.len() < 2 {
            return false;
        }
        // Check if any congested stage has a downstream that is also congested.
        for stage_id in &congested {
            if let Some(stage) = self.stages.get(stage_id) {
                for ds in &stage.downstream {
                    if congested.contains(ds) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Propagate pressure signals upstream and update flow control.
    /// For each stage, look at downstream signals and apply throttle accordingly.
    pub fn propagate_pressure(&mut self) -> HashMap<usize, TernaryPressureSignal> {
        // Collect downstream signals for each stage.
        let mut stage_signals: HashMap<usize, Vec<TernaryPressureSignal>> = HashMap::new();

        for stage in self.stages.values() {
            let downstream_signals: Vec<TernaryPressureSignal> = stage
                .downstream
                .iter()
                .filter_map(|ds_id| self.stages.get(ds_id).map(|ds| ds.pressure_signal()))
                .collect();
            stage_signals.insert(stage.id, downstream_signals);
        }

        // Now update flow control for each stage based on combined downstream signal.
        let mut results = HashMap::new();
        for (stage_id, signals) in &stage_signals {
            let combined = TernaryPressureSignal::combine(signals);
            self.flow_controller.update(*stage_id, combined);
            results.insert(*stage_id, combined);
        }
        results
    }

    /// Compute weighted fairness allocation for multi-producer scenarios.
    /// Given producers with weights, compute the fair share of available capacity
    /// for the target stage.
    pub fn weighted_fair_share(
        &self,
        target_stage_id: usize,
        producers: &[(usize, f64)], // (producer_id, weight)
    ) -> HashMap<usize, f64> {
        let total_weight: f64 = producers.iter().map(|(_, w)| w).sum();
        if total_weight == 0.0 {
            return HashMap::new();
        }

        let stage = match self.stages.get(&target_stage_id) {
            Some(s) => s,
            None => return HashMap::new(),
        };

        let available = (stage.buffer_capacity - stage.buffer_occupancy) as f64;
        let throttle = self.flow_controller.throttle_factor(target_stage_id);
        let effective_capacity = available * throttle;

        producers
            .iter()
            .map(|(id, weight)| {
                let share = (weight / total_weight) * effective_capacity;
                (*id, share)
            })
            .collect()
    }

    /// Run one simulation tick: process, propagate pressure, return congestion state.
    pub fn tick(&mut self) -> Vec<usize> {
        // Each stage processes items.
        for stage in self.stages.values_mut() {
            stage.tick();
        }
        // Propagate pressure signals.
        self.propagate_pressure();
        // Return congested stages.
        self.detect_congestion()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pressure_signal_values() {
        assert_eq!(TernaryPressureSignal::Ready.value(), 1);
        assert_eq!(TernaryPressureSignal::Balanced.value(), 0);
        assert_eq!(TernaryPressureSignal::Overloaded.value(), -1);
    }

    #[test]
    fn test_pressure_signal_combine() {
        assert_eq!(
            TernaryPressureSignal::combine(&[
                TernaryPressureSignal::Ready,
                TernaryPressureSignal::Ready,
            ]),
            TernaryPressureSignal::Ready
        );
        assert_eq!(
            TernaryPressureSignal::combine(&[
                TernaryPressureSignal::Ready,
                TernaryPressureSignal::Overloaded,
            ]),
            TernaryPressureSignal::Overloaded
        );
        assert_eq!(
            TernaryPressureSignal::combine(&[
                TernaryPressureSignal::Ready,
                TernaryPressureSignal::Balanced,
            ]),
            TernaryPressureSignal::Balanced
        );
        assert_eq!(
            TernaryPressureSignal::combine(&[]),
            TernaryPressureSignal::Balanced
        );
    }

    #[test]
    fn test_stage_pressure_transitions() {
        let mut stage = PipelineStage::new(0, 100, 10.0, 1.0);
        stage.buffer_occupancy = 20;
        assert_eq!(stage.pressure_signal(), TernaryPressureSignal::Ready);

        stage.buffer_occupancy = 50;
        assert_eq!(stage.pressure_signal(), TernaryPressureSignal::Balanced);

        stage.buffer_occupancy = 90;
        assert_eq!(stage.pressure_signal(), TernaryPressureSignal::Overloaded);
    }

    #[test]
    fn test_stage_enqueue_and_tick() {
        let mut stage = PipelineStage::new(0, 100, 10.0, 1.0);
        let accepted = stage.enqueue(50);
        assert_eq!(accepted, 50);
        assert_eq!(stage.buffer_occupancy, 50);

        stage.tick();
        assert_eq!(stage.buffer_occupancy, 40);

        // Overflow enqueue
        let accepted = stage.enqueue(70);
        assert_eq!(accepted, 60);
        assert_eq!(stage.buffer_occupancy, 100);
    }

    #[test]
    fn test_flow_controller_throttle() {
        let mut fc = FlowController::new(0.2);

        // Repeated overload should reduce throttle
        for _ in 0..5 {
            fc.update(0, TernaryPressureSignal::Overloaded);
        }
        assert!(fc.throttle_factor(0) < 0.2);

        // Repeated ready should increase throttle
        for _ in 0..10 {
            fc.update(0, TernaryPressureSignal::Ready);
        }
        assert_eq!(fc.throttle_factor(0), 1.0);
    }

    #[test]
    fn test_flow_controller_effective_rate() {
        let mut fc = FlowController::new(0.5);
        fc.update(0, TernaryPressureSignal::Overloaded); // 1.0 - 0.5 = 0.5
        assert_eq!(fc.effective_rate(0, 100.0), 50.0);
    }

    #[test]
    fn test_pipeline_congestion_detection() {
        let mut pipeline = Pipeline::new(0.2);
        let mut s0 = PipelineStage::new(0, 100, 10.0, 1.0);
        s0.buffer_occupancy = 90; // overloaded
        let mut s1 = PipelineStage::new(1, 100, 10.0, 1.0);
        s1.buffer_occupancy = 30; // ready
        pipeline.add_stage(s0);
        pipeline.add_stage(s1);

        let congested = pipeline.detect_congestion();
        assert_eq!(congested, vec![0]);
    }

    #[test]
    fn test_pipeline_congestion_spreading() {
        let mut pipeline = Pipeline::new(0.2);
        let mut s0 = PipelineStage::new(0, 100, 10.0, 1.0);
        s0.buffer_occupancy = 90;
        s0.connect_downstream(1);
        let mut s1 = PipelineStage::new(1, 100, 10.0, 1.0);
        s1.buffer_occupancy = 95;
        pipeline.add_stage(s0);
        pipeline.add_stage(s1);

        assert!(pipeline.is_congestion_spreading());
    }

    #[test]
    fn test_weighted_fair_share() {
        let mut pipeline = Pipeline::new(0.2);
        let stage = PipelineStage::new(0, 100, 10.0, 1.0);
        pipeline.add_stage(stage);

        let producers = vec![(10, 3.0), (20, 1.0), (30, 1.0)];
        let shares = pipeline.weighted_fair_share(0, &producers);

        // Total weight = 5.0, available = 100. producers get 60, 20, 20
        assert!((shares[&10] - 60.0).abs() < 0.001);
        assert!((shares[&20] - 20.0).abs() < 0.001);
        assert!((shares[&30] - 20.0).abs() < 0.001);
    }

    #[test]
    fn test_full_pipeline_tick_simulation() {
        let mut pipeline = Pipeline::new(0.3);
        let mut s0 = PipelineStage::new(0, 100, 5.0, 1.0);
        s0.connect_downstream(1);
        let mut s1 = PipelineStage::new(1, 100, 5.0, 1.0);
        pipeline.add_stage(s0);
        pipeline.add_stage(s1);

        // Load up stage 0
        pipeline.stages.get_mut(&0).unwrap().enqueue(90);

        // Tick should process and detect congestion
        let congested = pipeline.tick();
        assert!(congested.contains(&0));

        // After many ticks, should drain
        for _ in 0..20 {
            pipeline.tick();
        }
        let congested = pipeline.detect_congestion();
        assert!(!congested.contains(&0));
    }
}
