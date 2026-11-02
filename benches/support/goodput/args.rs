//! CLI for the paired raw-UDP / L1 saturation diagnostic.

use crate::support::matrix;
use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Integrity {
    Crc32c,
    UdpChecksum,
}

#[derive(Parser, Debug)]
#[command(about = "Paired single-socket raw UDP / L1 Whole goodput on this host")]
pub struct Args {
    #[arg(long)]
    pub run: bool,
    #[arg(long, value_delimiter = ',', default_value = "1200,4096,16384,65507")]
    pub datagrams: Vec<usize>,
    /// Whole-frame sizes; the wire-level name is `--messages` for all diagnostics.
    #[arg(
        long,
        alias = "frames",
        value_delimiter = ',',
        default_value = "65536,1048576"
    )]
    pub messages: Vec<usize>,
    /// One logical dispatcher plus `T-1` shared Rayon checksum workers.
    #[arg(
        long,
        alias = "processing-threads",
        value_delimiter = ',',
        default_value = "1,2,4,8"
    )]
    pub threads: Vec<usize>,
    #[arg(long, default_value_t = 5)]
    pub rounds: usize,
    #[arg(long, default_value_t = 500)]
    pub duration_ms: u64,
    #[arg(long, default_value_t = 100)]
    pub warmup_ms: u64,
    #[arg(long, default_value_t = 50)]
    pub drain_ms: u64,
    #[arg(long, default_value_t = 32)]
    pub batch: usize,
    #[arg(long, default_value_t = 4_194_304)]
    pub socket_buffer: usize,
    #[arg(long, value_enum, default_value_t = Integrity::Crc32c)]
    pub integrity: Integrity,
    /// Zero is unpaced saturation; a positive value is an explicitly rate-limited diagnostic.
    #[arg(long, default_value_t = 0.0)]
    pub offered_mbps: f64,
    #[arg(long, default_value_t = 16)]
    pub latency_samples: usize,
    #[arg(long)]
    pub output: Option<PathBuf>,
    /// Return nonzero if ANY cell misses 90% or experiences frame loss.
    #[arg(long)]
    pub strict: bool,
}

impl Args {
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        let bounded = self.rounds != 0
            && self.rounds <= 100
            && (10..=60_000).contains(&self.duration_ms)
            && self.drain_ms <= 10_000
            && self.warmup_ms <= 10_000
            && self.latency_samples <= 10_000
            && (1..=matrix::MAX_BATCH).contains(&self.batch)
            && self.offered_mbps.is_finite()
            && self.offered_mbps >= 0.0
            && !self.datagrams.is_empty()
            && !self.messages.is_empty()
            && !self.threads.is_empty()
            && self.datagrams.iter().all(|d| (128..=65507).contains(d))
            && self.messages.iter().all(|&m| m != 0 && m <= 16 << 20)
            && self.threads.iter().all(|&t| (1..=32).contains(&t));
        if !bounded {
            return Err("invalid benchmark dimensions/limits".into());
        }
        Ok(())
    }
}
