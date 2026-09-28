//! Bounded runtime memory and CPU sample history.
//!
//! Port of the `RuntimeSampleRing` portion of
//! `packages/core/src/services/memoryPressureMonitor.ts`. A CPU sampler is
//! supplied by the caller because the standard library has no portable,
//! safe process CPU counter. A missing/failed CPU sample is treated as the
//! zero baseline, matching the source's `safeCpuUsage()` fallback.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum number of recent runtime samples retained.
pub const RING_BUFFER_SIZE: usize = 60;

/// A single runtime sample capturing memory and normalized CPU usage.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RuntimeSample {
    /// Unix timestamp in milliseconds.
    pub ts: i64,
    pub rss: u64,
    pub heap_used: u64,
    pub heap_total: u64,
    pub external: u64,
    /// CPU usage as a percentage of total system capacity (0–100).
    pub cpu_percent: f64,
}

/// Memory values supplied by a caller that already fetched its snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoryUsage {
    pub rss: u64,
    pub heap_used: u64,
    pub heap_total: u64,
    pub external: u64,
}

/// Cumulative process CPU time in microseconds.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CpuUsage {
    pub user_us: i64,
    pub system_us: i64,
}

type ClockProvider = Arc<dyn Fn() -> i64 + Send + Sync>;
type CpuUsageProvider = Arc<dyn Fn() -> Option<CpuUsage> + Send + Sync>;

/// Holds up to 60 recent memory/CPU samples.
///
/// Use [`RuntimeSampleRing::with_providers`] to inject a platform CPU sampler
/// and deterministic clock. Returning `None` from the CPU provider is safe
/// and is treated as `{ user_us: 0, system_us: 0 }`.
pub struct RuntimeSampleRing {
    samples: VecDeque<RuntimeSample>,
    clock: ClockProvider,
    cpu_usage_provider: CpuUsageProvider,
    cpu_core_count: usize,
    prev_cpu_usage: CpuUsage,
    prev_sample_time: i64,
}

impl RuntimeSampleRing {
    /// Create a ring using the system wall clock and a safe zero CPU sampler.
    ///
    /// Rust's standard library does not expose a portable, safe process CPU
    /// counter, so callers that need nonzero CPU metrics should provide one
    /// with [`RuntimeSampleRing::with_providers`].
    pub fn new() -> Self {
        Self::with_providers(system_time_millis, || None, system_cpu_core_count())
    }

    /// Create a ring with injectable clock, CPU sampling, and core count.
    ///
    /// The clock returns Unix milliseconds. The CPU provider returns
    /// cumulative user/system microseconds, or `None` when sampling is
    /// unavailable. A zero core count is normalized to one.
    pub fn with_providers<C, P>(clock: C, cpu_usage_provider: P, cpu_core_count: usize) -> Self
    where
        C: Fn() -> i64 + Send + Sync + 'static,
        P: Fn() -> Option<CpuUsage> + Send + Sync + 'static,
    {
        let clock: ClockProvider = Arc::new(clock);
        let cpu_usage_provider: CpuUsageProvider = Arc::new(cpu_usage_provider);
        let prev_cpu_usage = safe_cpu_usage(&cpu_usage_provider);
        let prev_sample_time = clock();

        Self {
            samples: VecDeque::with_capacity(RING_BUFFER_SIZE),
            clock,
            cpu_usage_provider,
            cpu_core_count: cpu_core_count.max(1),
            prev_cpu_usage,
            prev_sample_time,
        }
    }

    /// Record a sample using an already-fetched memory snapshot.
    pub fn record(&mut self, memory: MemoryUsage) -> RuntimeSample {
        let now = (self.clock)();
        let absolute_cpu = safe_cpu_usage(&self.cpu_usage_provider);
        let elapsed_ms = now.saturating_sub(self.prev_sample_time);

        // Same-millisecond (or backwards-clock) samples still capture the
        // caller's fresh memory snapshot. Keep the previous CPU baseline and
        // timestamp so the accumulated CPU delta is measured next time.
        if elapsed_ms <= 0 {
            let cpu_percent = self
                .samples
                .back()
                .map(|sample| sample.cpu_percent)
                .unwrap_or(0.0);
            return self.push(make_sample(now, memory, cpu_percent));
        }

        let delta_user = absolute_cpu.user_us as f64 - self.prev_cpu_usage.user_us as f64;
        let delta_system = absolute_cpu.system_us as f64 - self.prev_cpu_usage.system_us as f64;
        let cpu_total_us = delta_user + delta_system;
        let raw_cpu_percent =
            ((cpu_total_us / (elapsed_ms as f64 * 1_000.0)) * 100.0) / self.cpu_core_count as f64;
        let cpu_percent = (raw_cpu_percent.clamp(0.0, 100.0) * 100.0).round() / 100.0;

        self.prev_cpu_usage = absolute_cpu;
        self.prev_sample_time = now;
        self.push(make_sample(now, memory, cpu_percent))
    }

    /// Return a snapshot of all retained samples in chronological order.
    pub fn get_all(&self) -> Vec<RuntimeSample> {
        self.samples.iter().copied().collect()
    }

    /// Clear samples and establish fresh time and CPU baselines.
    pub fn reset(&mut self) {
        self.samples.clear();
        self.prev_cpu_usage = safe_cpu_usage(&self.cpu_usage_provider);
        self.prev_sample_time = (self.clock)();
    }

    fn push(&mut self, sample: RuntimeSample) -> RuntimeSample {
        if self.samples.len() == RING_BUFFER_SIZE {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
        sample
    }
}

impl Default for RuntimeSampleRing {
    fn default() -> Self {
        Self::new()
    }
}

fn make_sample(ts: i64, memory: MemoryUsage, cpu_percent: f64) -> RuntimeSample {
    RuntimeSample {
        ts,
        rss: memory.rss,
        heap_used: memory.heap_used,
        heap_total: memory.heap_total,
        external: memory.external,
        cpu_percent,
    }
}

fn safe_cpu_usage(provider: &CpuUsageProvider) -> CpuUsage {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| provider()))
        .ok()
        .flatten()
        .unwrap_or_default()
}

fn system_time_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn system_cpu_core_count() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::{CpuUsage, MemoryUsage, RING_BUFFER_SIZE, RuntimeSampleRing};
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::{Arc, Mutex};

    fn memory() -> MemoryUsage {
        MemoryUsage {
            rss: 100,
            heap_used: 50,
            heap_total: 80,
            external: 10,
        }
    }

    fn deterministic_ring(
        time: Arc<AtomicI64>,
        cpu: Arc<Mutex<CpuUsage>>,
        cores: usize,
    ) -> RuntimeSampleRing {
        RuntimeSampleRing::with_providers(
            {
                let time = Arc::clone(&time);
                move || time.load(Ordering::SeqCst)
            },
            {
                let cpu = Arc::clone(&cpu);
                move || Some(*cpu.lock().expect("CPU sample mutex poisoned"))
            },
            cores,
        )
    }

    #[test]
    fn records_the_memory_snapshot_and_timestamp() {
        let time = Arc::new(AtomicI64::new(1_700_000_000_000));
        let cpu = Arc::new(Mutex::new(CpuUsage::default()));
        let mut ring = deterministic_ring(time, cpu, 4);
        let input = MemoryUsage {
            rss: 500_000_000,
            heap_used: 300_000_000,
            heap_total: 400_000_000,
            external: 10_000_000,
        };

        let sample = ring.record(input);

        assert_eq!(sample.rss, 500_000_000);
        assert_eq!(sample.heap_used, 300_000_000);
        assert_eq!(sample.heap_total, 400_000_000);
        assert_eq!(sample.external, 10_000_000);
        assert_eq!(sample.ts, 1_700_000_000_000);
        assert!(sample.cpu_percent.is_finite());
    }

    #[test]
    fn computes_core_normalized_cpu_percentage() {
        let time = Arc::new(AtomicI64::new(1_000));
        let cpu = Arc::new(Mutex::new(CpuUsage::default()));
        let mut ring = deterministic_ring(Arc::clone(&time), Arc::clone(&cpu), 4);

        time.store(1_100, Ordering::SeqCst);
        *cpu.lock().unwrap() = CpuUsage {
            user_us: 4_000,
            system_us: 4_000,
        };
        let sample = ring.record(memory());

        assert_eq!(sample.cpu_percent, 2.0);
    }

    #[test]
    fn same_tick_captures_fresh_memory_and_reuses_cpu_percentage() {
        let time = Arc::new(AtomicI64::new(1_000));
        let cpu = Arc::new(Mutex::new(CpuUsage::default()));
        let mut ring = deterministic_ring(Arc::clone(&time), Arc::clone(&cpu), 1);

        time.store(1_100, Ordering::SeqCst);
        *cpu.lock().unwrap() = CpuUsage {
            user_us: 4_000,
            system_us: 4_000,
        };
        let first = ring.record(memory());
        let second = ring.record(MemoryUsage {
            rss: 999,
            heap_used: 777,
            ..memory()
        });

        assert_eq!(second.cpu_percent, first.cpu_percent);
        assert_eq!(second.rss, 999);
        assert_eq!(second.heap_used, 777);
        assert_ne!(second, first);
    }

    #[test]
    fn first_same_tick_record_is_stored_with_zero_cpu() {
        let time = Arc::new(AtomicI64::new(1_000));
        let cpu = Arc::new(Mutex::new(CpuUsage::default()));
        let mut ring = deterministic_ring(time, cpu, 1);

        let sample = ring.record(MemoryUsage {
            rss: 123,
            ..memory()
        });

        assert_eq!(sample.cpu_percent, 0.0);
        assert_eq!(sample.rss, 123);
        assert_eq!(ring.get_all().len(), 1);
    }

    #[test]
    fn carries_cpu_delta_across_a_same_tick_sample() {
        let time = Arc::new(AtomicI64::new(1_000));
        let cpu = Arc::new(Mutex::new(CpuUsage::default()));
        let mut ring = deterministic_ring(Arc::clone(&time), Arc::clone(&cpu), 2);

        time.store(1_100, Ordering::SeqCst);
        *cpu.lock().unwrap() = CpuUsage {
            user_us: 4_000,
            system_us: 4_000,
        };
        ring.record(memory());

        *cpu.lock().unwrap() = CpuUsage {
            user_us: 8_000,
            system_us: 8_000,
        };
        let same_tick = ring.record(memory());
        assert_eq!(same_tick.cpu_percent, 4.0);

        time.store(1_200, Ordering::SeqCst);
        *cpu.lock().unwrap() = CpuUsage {
            user_us: 12_000,
            system_us: 12_000,
        };
        let after_same_tick = ring.record(memory());

        // 16ms CPU over 100ms, normalized by two cores = 8%.
        assert_eq!(after_same_tick.cpu_percent, 8.0);
    }

    #[test]
    fn retains_only_the_most_recent_sixty_samples() {
        let time = Arc::new(AtomicI64::new(1_000));
        let cpu = Arc::new(Mutex::new(CpuUsage::default()));
        let mut ring = deterministic_ring(Arc::clone(&time), cpu, 1);

        for _ in 0..65 {
            time.fetch_add(1_000, Ordering::SeqCst);
            ring.record(memory());
        }

        let samples = ring.get_all();
        assert_eq!(samples.len(), RING_BUFFER_SIZE);
        assert_eq!(samples.first().unwrap().ts, 7_000);
    }

    #[test]
    fn get_all_returns_an_independent_snapshot() {
        let time = Arc::new(AtomicI64::new(1_000));
        let cpu = Arc::new(Mutex::new(CpuUsage::default()));
        let mut ring = deterministic_ring(Arc::clone(&time), cpu, 1);
        time.fetch_add(1_000, Ordering::SeqCst);
        ring.record(memory());
        time.fetch_add(1_000, Ordering::SeqCst);
        ring.record(memory());

        let mut snapshot = ring.get_all();
        assert_eq!(snapshot.len(), 2);
        snapshot.clear();
        assert_eq!(ring.get_all().len(), 2);
    }

    #[test]
    fn reset_clears_samples_and_sets_new_baselines() {
        let time = Arc::new(AtomicI64::new(1_000));
        let cpu = Arc::new(Mutex::new(CpuUsage::default()));
        let mut ring = deterministic_ring(Arc::clone(&time), Arc::clone(&cpu), 1);
        time.store(1_100, Ordering::SeqCst);
        ring.record(memory());
        assert_eq!(ring.get_all().len(), 1);

        time.store(2_000, Ordering::SeqCst);
        *cpu.lock().unwrap() = CpuUsage {
            user_us: 10_000,
            system_us: 5_000,
        };
        ring.reset();
        assert!(ring.get_all().is_empty());

        time.store(2_100, Ordering::SeqCst);
        *cpu.lock().unwrap() = CpuUsage {
            user_us: 14_000,
            system_us: 7_000,
        };
        assert_eq!(ring.record(memory()).cpu_percent, 6.0);
    }

    #[test]
    fn unavailable_cpu_samples_safely_fall_back_to_zero() {
        let time = Arc::new(AtomicI64::new(1_000));
        let clock = Arc::clone(&time);
        let mut ring =
            RuntimeSampleRing::with_providers(move || clock.load(Ordering::SeqCst), || None, 1);
        time.store(1_100, Ordering::SeqCst);

        let sample = ring.record(memory());
        assert_eq!(sample.cpu_percent, 0.0);

        time.store(1_200, Ordering::SeqCst);
        ring.reset();
        assert!(ring.get_all().is_empty());
    }

    #[test]
    fn cpu_provider_panics_are_contained_at_construction_record_and_reset() {
        let time = Arc::new(AtomicI64::new(1_000));
        let clock = Arc::clone(&time);
        let mut ring = RuntimeSampleRing::with_providers(
            move || clock.load(Ordering::SeqCst),
            || -> Option<CpuUsage> { panic!("CPU sampling unavailable") },
            1,
        );
        time.store(1_100, Ordering::SeqCst);

        assert_eq!(ring.record(memory()).cpu_percent, 0.0);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| ring.reset())).is_ok());
    }

    #[test]
    fn clamps_cpu_percentage_at_zero_and_one_hundred() {
        let time = Arc::new(AtomicI64::new(1_000));
        let cpu = Arc::new(Mutex::new(CpuUsage::default()));
        let mut ring = deterministic_ring(Arc::clone(&time), Arc::clone(&cpu), 2);

        time.store(1_100, Ordering::SeqCst);
        *cpu.lock().unwrap() = CpuUsage {
            user_us: 100_000,
            system_us: 100_000,
        };
        assert_eq!(ring.record(memory()).cpu_percent, 100.0);

        time.store(1_200, Ordering::SeqCst);
        *cpu.lock().unwrap() = CpuUsage {
            user_us: 50_000,
            system_us: 50_000,
        };
        assert_eq!(ring.record(memory()).cpu_percent, 0.0);
    }

    #[test]
    fn clamps_zero_core_count_to_one() {
        let time = Arc::new(AtomicI64::new(1_000));
        let cpu = Arc::new(Mutex::new(CpuUsage::default()));
        let mut ring = deterministic_ring(Arc::clone(&time), Arc::clone(&cpu), 0);
        time.store(1_100, Ordering::SeqCst);
        *cpu.lock().unwrap() = CpuUsage {
            user_us: 5_000,
            system_us: 5_000,
        };

        assert_eq!(ring.record(memory()).cpu_percent, 10.0);
    }
}
