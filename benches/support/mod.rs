//! Shared benchmark harness for every explicit `[[bench]]` target.
//!
//! Each target compiles this module and uses a subset of it, so unused items are
//! expected here rather than duplicated per target. The library crate contains no
//! benchmark implementation; only `rimuru::benchmarking` remains as the documented
//! topology fixture required to build the same executor the public API uses.

#![allow(dead_code)]

pub mod fixture;
pub mod goodput;
pub mod l2l3;
pub mod matrix;
pub mod quinn;
pub mod raw;
pub mod relay;
pub mod report;
