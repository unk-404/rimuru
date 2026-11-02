use crate::{Error, Result, RuntimeConfig, buffer::Budget};
use rayon::ThreadPool;
use std::{
    future::Future,
    sync::{Arc, Mutex, Weak, mpsc},
    thread,
    time::Duration,
};
#[derive(Clone)]
pub(crate) struct Runtime(pub(crate) Arc<Inner>);
pub(crate) struct Inner {
    pub handle: tokio::runtime::Handle,
    pub compute: Option<Arc<ThreadPool>>,
    pub budget: Arc<Budget>,
    pub cookie_key: [u8; 32],
    shutdown: mpsc::Sender<()>,
}
impl Runtime {
    pub fn shared() -> Result<Self> {
        static SHARED: Mutex<Weak<Inner>> = Mutex::new(Weak::new());
        let mut shared = SHARED.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(inner) = shared.upgrade() {
            return Ok(Self(inner));
        }
        let runtime = Self::new(RuntimeConfig::default())?;
        *shared = Arc::downgrade(&runtime.0);
        Ok(runtime)
    }
    pub fn new(config: RuntimeConfig) -> Result<Self> {
        config.validate()?;
        let mut cookie_key = [0; 32];
        getrandom::fill(&mut cookie_key)
            .map_err(|_| Error::Protocol("OS randomness unavailable"))?;
        let compute = if config.cpu_threads == 0 {
            None
        } else {
            Some(Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(config.cpu_threads)
                    .thread_name(|i| format!("rimuru-cpu-{i}"))
                    .build()
                    .map_err(|_| Error::NoBuffer)?,
            ))
        };
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.io_threads)
            .thread_name("rimuru-io")
            .enable_io()
            .enable_time()
            .build()?;
        let handle = rt.handle().clone();
        let (shutdown, receiver) = mpsc::channel();
        // Destruction must not join a runtime from inside its own worker.
        thread::Builder::new()
            .name("rimuru-lifecycle".into())
            .spawn(move || {
                let _ = receiver.recv();
                rt.shutdown_timeout(Duration::from_secs(5));
            })?;
        Ok(Self(Arc::new(Inner {
            handle,
            compute,
            budget: Budget::new(config.memory_budget, None),
            cookie_key,
            shutdown,
        })))
    }
    pub fn block_on<F: Future>(&self, future: F) -> Result<F::Output> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::WouldDeadlock);
        }
        Ok(self.0.handle.block_on(future))
    }

    /// Block a caller thread without requiring an outside-runtime context.
    ///
    /// Used only by the C ABI. When the caller already carries a Tokio context, the
    /// future runs on a scoped OS thread against the library driver, so no runtime
    /// is nested and the caller's executor is never re-entered.
    pub(crate) fn block_on_ffi<F>(&self, future: F) -> Result<F::Output>
    where
        F: Future + Send,
        F::Output: Send,
    {
        if tokio::runtime::Handle::try_current().is_ok() {
            return std::thread::scope(|scope| {
                let handle = self.0.handle.clone();
                let thread = scope.spawn(move || handle.block_on(future));
                thread.join().map_err(|_| Error::Closed)
            });
        }
        Ok(self.0.handle.block_on(future))
    }
    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.0.handle.spawn(future)
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
    }
}

/// Cross-core dispatch is a measured cost, not a free optimization. Small frames
/// or frames with few large datagrams stay local; sufficiently large fragment
/// sets can overlap CRC work with the single RX dispatcher.
pub(crate) fn checksum_pool(
    pool: &Option<Arc<ThreadPool>>,
    bytes: usize,
    segments: u32,
) -> Option<&Arc<ThreadPool>> {
    pool.as_ref().filter(|pool| {
        bytes >= crate::checksum::PARALLEL_MIN && segments >= 256 && pool.current_num_threads() > 1
    })
}

/// RX checks can overlap the dispatcher after a large frame is assembled.
pub(crate) fn receive_checksum_pool(
    pool: &Option<Arc<ThreadPool>>,
    bytes: usize,
) -> Option<&Arc<ThreadPool>> {
    pool.as_ref()
        .filter(|pool| bytes >= crate::checksum::PARALLEL_MIN && pool.current_num_threads() > 1)
}
