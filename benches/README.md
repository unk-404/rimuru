# Rimuru benchmark suite

This directory is the complete benchmark home. The library (`src/`) contains no
benchmark implementation; only `rimuru::benchmarking::Environment` remains as the
minimal topology fixture because the public API deliberately does not expose a
runtime constructor.

## Layout

| Path | Purpose |
|---|---|
| `support/matrix.rs` | Single source of truth: D/M/T/channel matrix, Criterion policy, group naming |
| `support/fixture.rs` | Matched L1/L2/L3 whole-message window fixture |
| `support/raw.rs` | Raw-UDP control fixture |
| `support/relay.rs` | Bidirectional fault relay (also used by network integration tests) |
| `support/report.rs` | Versioned JSON for matched L2/L3 diagnostics |
| `support/l2l3.rs` | Diagnostic driver shared by `l2_goodput` and `l3_goodput` |
| `support/goodput/` | Raw/UDP vs L1 saturation diagnostic, CLI and JSON schema 2 |
| `support/quinn.rs` | Quinn ordered-stream reference used by `comparison` |
| `l1.rs`, `l1_costs.rs`, `l2.rs`, `l3.rs`, `comparison.rs` | Criterion statistical benchmarks |
| `udp_goodput.rs`, `l2_goodput.rs`, `l3_goodput.rs` | Saturation diagnostics (`harness = false`) |
| `scripts/check_goal.py` | Validates a Windows L2/L3 goal report |

## Statistical benchmarks (Criterion)

```text
cargo bench --bench l1
cargo bench --bench l1 -- --test          # smoke, no thresholds
cargo bench --bench l1_costs
cargo bench --bench l2
cargo bench --bench l3
cargo bench --bench comparison
```

Filtering and baselines are standard Criterion arguments, for example:

```text
cargo bench --bench l1 -- 'frame_loopback/t1/d1200/.*/65536$' --save-baseline windows
```

Group names are `level/t<threads>/d<datagram>[/c<channels>][/w<window>]`.
`T` is one RX dispatcher plus `T-1` Rayon workers. Every group uses 20 samples,
500 ms warm-up and 2 s measurement; `l1_costs` uses 30 samples, 300 ms and 1 s.

## Diagnostics (paired, saturated)

```text
cargo bench --bench udp_goodput -- --run --output benchmark-results/windows-goodput.json
cargo bench --bench udp_goodput -- --run --datagrams 1200 --messages 65536 \
    --threads 1 --rounds 5 --duration-ms 500 --output benchmark-results/windows-small.json
cargo bench --bench l2_goodput -- --run --loss-ppm 100000 --rtt-ms 10
cargo bench --bench l3_goodput -- --run --channels 4 --timeline
cargo bench --bench l3_goodput -- --run --verify-payload --loss-ppm 100000 --rtt-ms 10
```

`--messages` (alias `--frames`), `--threads` (alias `--processing-threads`) and
all other dimensions are validated against `support/matrix.rs` limits. `--test`
exits immediately so CI can compile every diagnostic without running it.

Diagnostics report paired runs on the same two UDP ports, with alternating order,
warm-up, median and relative MAD. Raw UDP and Rimuru runs are never concurrent.
Fault injection goes through the external relay, so each endpoint still owns
exactly one socket. `--strict` fails only the lossless 90% target cells.

Results are written to `benchmark-results/` (git-ignored). Both report schemas are
versioned; changing a field's meaning requires a schema bump. No throughput number
here is a capacity proof for the public Internet, and a single passing cell does
not satisfy the full DOCS matrix.

The L2/L3 diagnostics accept `--verify-payload` to compare each received message
byte-for-byte with its own deterministic nonuniform input. The window's distinct
inputs are created once before timing, shared by both fixtures, and limited to
64 MiB in total. The comparison runs inside
the timed window and the report records whether it was enabled; its throughput
is therefore not directly comparable to runs without payload verification.
Pass `--require-payload-verification` to `scripts/check_goal.py` when checking a
goal report to reject reports without that verification.
Pass `--require-distinct-payloads` to require the four distinct inputs as well.
With `--require-timeline`, each complete subwindow must last between 1 and 1.1
seconds: measurements close subwindows only after a whole four-message window,
so a longer interval is insufficient evidence of per-second stability. A short
final partial subwindow is recorded but is not itself a one-second sample.

For the Windows 1 MiB/D=65507 goal with byte-for-byte verification and
one-second subwindows, run the following without competing benchmark workloads:

```text
cargo bench --bench l3_goodput -- --run --verify-payload --timeline --datagrams 65507 --messages 1048576 --threads 1 --window 4 --batch 32 --channels 1 --rounds 1 --duration-ms 60000 --warmup-ms 1000 --output benchmark-results/goal-verified.json
python benches/scripts/check_goal.py benchmark-results/goal-verified.json --rounds 1 --duration-ms 60000 --require-timeline --require-distinct-payloads
```

For optional process-level CPU, working-set and thread sampling, install the
Python `psutil` package in the local environment and launch the compiled
diagnostic through `python benches/scripts/monitor_process.py --output
benchmark-results/process-samples.json -- <diagnostic> <arguments>`. Samples
cover the whole child process, including fixture setup and warm-up; the last
sample can precede process exit by one sampling interval. Derive average CPU
cores from the difference in sampled CPU time divided by the difference in
sample timestamps. A sampled peak is a lower bound on the actual peak, and
working set is distinct from the protocol's internal controlled memory budget.
Sampling itself can influence the measured performance.
