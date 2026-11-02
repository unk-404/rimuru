//! Matched L1/L2 window diagnostic with optional fault relay (harness = false).

mod support;

fn main() {
    support::l2l3::main(false);
}
