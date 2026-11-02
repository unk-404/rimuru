//! Explicit fixtures for performance tools. Normal application code binds an
//! Endpoint directly and never creates or maintains an executor.
use crate::{Endpoint, EndpointConfig, Result, Runtime, RuntimeConfig, udp::DatagramSocket};
use std::{future::Future, net::SocketAddr, sync::Arc};

/// Production checksum adapters for isolated CPU measurements.
pub fn checksum(seed: u32, bytes: &[u8]) -> u32 {
    crate::checksum::append(seed, bytes)
}
pub fn combine_checksums(first: u32, second: u32, len: usize) -> u32 {
    crate::checksum::combine(first, second, len)
}
pub struct Environment {
    runtime: Runtime,
}
impl Environment {
    pub fn new(processing_threads: usize) -> Result<Self> {
        if !(1..=32).contains(&processing_threads) {
            return Err(crate::Error::InvalidConfig("benchmark T must be 1..=32"));
        }
        Ok(Self {
            runtime: Runtime::new(RuntimeConfig {
                io_threads: 2,
                cpu_threads: processing_threads - 1,
                memory_budget: 512 << 20,
            })?,
        })
    }
    pub fn bind_endpoint(&self, address: SocketAddr, config: EndpointConfig) -> Result<Endpoint> {
        Endpoint::bind_on(&self.runtime, address, config)
    }
    pub fn bind_socket(&self, address: SocketAddr, buffer: usize) -> Result<Arc<DatagramSocket>> {
        DatagramSocket::bind_on(&self.runtime, address, buffer)
    }
    pub fn block_on<F: Future>(&self, future: F) -> Result<F::Output> {
        self.runtime.block_on(future)
    }
    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime.spawn(future)
    }
}
