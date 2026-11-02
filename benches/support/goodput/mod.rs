//! Saturation diagnostics. Statistical benchmark suite: `l1` (Criterion).
//!
//! Each `*_goodput` target is a separate harness=false binary that shares the same
//! CLI/report code but selects one matched-layer comparison.

pub mod args;
pub mod report;
mod runner;

use clap::Parser;

pub fn main() -> ! {
    if std::env::args().any(|arg| arg == "--test") {
        std::process::exit(0);
    }
    let args = args::Args::parse_from(std::env::args_os().filter(|arg| arg != "--bench"));
    if !args.run {
        println!("Run with: cargo bench --bench udp_goodput -- --run (see --help)");
        std::process::exit(0);
    }
    match runner::run(args) {
        Ok(()) => std::process::exit(0),
        Err(error) => {
            eprintln!("benchmark failed: {error}");
            std::process::exit(1);
        }
    }
}
