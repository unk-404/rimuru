//! Bounded frame reassembly: L1 Whole delivery and the shared L2 fragment engine.

pub(crate) mod assembly;
mod contexts;
pub(crate) mod mask;
mod receiver;

pub(crate) use receiver::Receiver;

#[cfg(test)]
mod test;
