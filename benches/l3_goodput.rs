//! Matched L2/L3 window diagnostic with optional fault relay (harness = false).

mod support;

fn main() {
    support::l2l3::main(true);
}
