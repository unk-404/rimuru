//! Internal runtime topology. Not a public runtime API: endpoints share one.

use crate::{Error, Result};

/// Runtime budget floor below which binding is refused (DOCS 13.1).
const MIN_BUDGET: usize = 8 << 20;
/// Thread-count bounds: at least one I/O thread and two total threads.
const MIN_IO_THREADS: usize = 1;
const MIN_TOTAL_THREADS: usize = 2;
const MAX_TOTAL_THREADS: usize = 17;
const MAX_IO_THREADS: usize = 64;
const MAX_CPU_THREADS: usize = 64;
/// Two shared I/O workers are used once the budget allows them.
const TWO_IO_THRESHOLD: usize = 4;
const TWO_IO_THREADS: usize = 2;
const ONE_IO_THREADS: usize = 1;
const DEFAULT_MEMORY_BUDGET: usize = 256 << 20;

#[derive(Clone, Debug)]
pub(crate) struct RuntimeConfig {
    pub io_threads: usize,
    /// Zero processes checksums serially; otherwise one shared Rayon pool.
    pub cpu_threads: usize,
    pub memory_budget: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        let cores = std::thread::available_parallelism().map_or(1, usize::from);
        let total = (cores + 1).clamp(MIN_TOTAL_THREADS, MAX_TOTAL_THREADS);
        let io = if total >= TWO_IO_THRESHOLD {
            TWO_IO_THREADS
        } else {
            ONE_IO_THREADS
        };
        Self {
            io_threads: io,
            cpu_threads: total - 1 - io,
            memory_budget: DEFAULT_MEMORY_BUDGET,
        }
    }
}

impl RuntimeConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if !(MIN_IO_THREADS..=MAX_IO_THREADS).contains(&self.io_threads)
            || self.cpu_threads > MAX_CPU_THREADS
        {
            return Err(Error::InvalidConfig(
                "I/O threads 1..=64; CPU threads 0..=64",
            ));
        }
        if self.memory_budget < MIN_BUDGET {
            return Err(Error::InvalidConfig("runtime budget below 8 MiB"));
        }
        Ok(())
    }
}
