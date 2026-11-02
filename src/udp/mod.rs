//! The shared portable UDP backend, also used by the raw-UDP benchmark.
use crate::{MetricsSnapshot, Result, Runtime, metrics::Metrics};
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    io,
    mem::ManuallyDrop,
    net::SocketAddr,
    sync::{Arc, atomic::Ordering::Relaxed},
};
use tokio::{io::Interest, net::UdpSocket};
#[cfg(windows)]
pub(crate) mod windows;

pub struct DatagramSocket {
    socket: UdpSocket,
    pub(crate) metrics: Arc<Metrics>,
    recv_buffer: usize,
    send_buffer: usize,
    runtime_guard: Option<Runtime>,
    #[cfg(windows)]
    segmentation_size: std::sync::atomic::AtomicUsize,
    #[cfg(all(windows, test))]
    fail_segment_once: std::sync::atomic::AtomicBool,
}
impl DatagramSocket {
    pub fn bind(address: SocketAddr, buffer_bytes: usize) -> Result<Arc<Self>> {
        let runtime = Runtime::shared()?;
        let mut socket = Self::bind_on(&runtime, address, buffer_bytes)?;
        Arc::get_mut(&mut socket).unwrap().runtime_guard = Some(runtime);
        Ok(socket)
    }
    pub(crate) fn bind_on(
        runtime: &Runtime,
        address: SocketAddr,
        buffer_bytes: usize,
    ) -> Result<Arc<Self>> {
        let socket = Socket::new(
            Domain::for_address(address),
            Type::DGRAM,
            Some(Protocol::UDP),
        )?;
        if address.is_ipv6() {
            socket.set_only_v6(true)?;
        }
        socket.set_recv_buffer_size(buffer_bytes)?;
        socket.set_send_buffer_size(buffer_bytes)?;
        socket.set_nonblocking(true)?;
        socket.bind(&address.into())?;
        let recv_buffer = socket.recv_buffer_size()?;
        let send_buffer = socket.send_buffer_size()?;
        let _enter = runtime.0.handle.enter();
        Ok(Arc::new(Self {
            socket: UdpSocket::from_std(socket.into())?,
            metrics: Arc::new(Metrics::default()),
            recv_buffer,
            send_buffer,
            runtime_guard: None,
            #[cfg(windows)]
            segmentation_size: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(all(windows, test))]
            fail_segment_once: std::sync::atomic::AtomicBool::new(false),
        }))
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
    // Borrow the SAME handle, not a second socket or a duplicated descriptor.
    // The temporary wrapper never escapes this call and never closes the handle.
    fn with_std<T>(&self, f: impl FnOnce(&std::net::UdpSocket) -> T) -> T {
        #[cfg(windows)]
        let socket = {
            use std::os::windows::io::{AsRawSocket, FromRawSocket};
            // SAFETY: self keeps the handle live; ManuallyDrop prevents ownership transfer.
            ManuallyDrop::new(unsafe {
                std::net::UdpSocket::from_raw_socket(self.socket.as_raw_socket())
            })
        };
        #[cfg(unix)]
        let socket = {
            use std::os::fd::{AsRawFd, FromRawFd};
            // SAFETY: same borrowing and lifetime argument as on Windows.
            ManuallyDrop::new(unsafe { std::net::UdpSocket::from_raw_fd(self.socket.as_raw_fd()) })
        };
        f(&socket)
    }
    pub(crate) async fn writable(&self) -> io::Result<()> {
        self.socket.writable().await
    }
    #[cfg(windows)]
    pub(crate) fn enable_segmentation(&self, size: usize) {
        if windows::enable_segmentation(&self.socket, size) {
            self.segmentation_size.store(size, Relaxed);
        }
    }
    #[cfg(windows)]
    pub(crate) fn segmentation_size(&self) -> usize {
        self.segmentation_size.load(Relaxed)
    }
    #[cfg(windows)]
    pub(crate) fn disable_segmentation(&self) {
        self.segmentation_size.store(0, Relaxed);
    }
    #[cfg(all(windows, test))]
    pub(crate) fn fail_next_segment_for_test(&self) {
        self.fail_segment_once.store(true, Relaxed);
    }
    #[cfg(windows)]
    pub(crate) fn try_send_segments(
        &self,
        buffers: &[io::IoSlice<'_>],
        peer: SocketAddr,
        datagrams: usize,
        expected: usize,
    ) -> io::Result<usize> {
        #[cfg(test)]
        if self.fail_segment_once.swap(false, Relaxed) {
            return Err(io::Error::from_raw_os_error(
                windows_sys::Win32::Networking::WinSock::WSAEMSGSIZE,
            ));
        }
        let address = socket2::SockAddr::from(peer);
        let result = self.socket.try_io(Interest::WRITABLE, || {
            self.with_std(|s| {
                self.metrics.send_syscalls.fetch_add(1, Relaxed);
                socket2::SockRef::from(s).send_to_vectored(buffers, &address)
            })
        });
        if let Ok(n) = result {
            if n != expected {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "partial segmented UDP send",
                ));
            }
            self.metrics
                .sent_datagrams
                .fetch_add(datagrams as u64, Relaxed);
            self.metrics.sent_wire_bytes.fetch_add(n as u64, Relaxed);
        }
        result
    }
    pub fn try_send_to(&self, bytes: &[u8], peer: SocketAddr) -> io::Result<usize> {
        let result = self.socket.try_io(Interest::WRITABLE, || {
            self.with_std(|s| {
                self.metrics.send_syscalls.fetch_add(1, Relaxed);
                s.send_to(bytes, peer)
            })
        });
        if let Ok(n) = result {
            self.metrics.sent_datagrams.fetch_add(1, Relaxed);
            self.metrics.sent_wire_bytes.fetch_add(n as u64, Relaxed);
        }
        result
    }
    pub async fn send_to(&self, bytes: &[u8], peer: SocketAddr) -> io::Result<usize> {
        loop {
            match self.try_send_to(bytes, peer) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => self.socket.writable().await?,
                result => return result,
            }
        }
    }
    /// Scatter/gather one UDP datagram without an intermediate application copy.
    /// Kernel UDP copying still applies; this is not kernel zero-copy.
    pub async fn send_vectored_to(
        &self,
        header: &[u8],
        payload: &[u8],
        peer: SocketAddr,
    ) -> io::Result<usize> {
        loop {
            match self.try_send_vectored_to(header, payload, peer) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => self.socket.writable().await?,
                result => return result,
            }
        }
    }
    pub(crate) fn try_send_vectored_to(
        &self,
        header: &[u8],
        payload: &[u8],
        peer: SocketAddr,
    ) -> io::Result<usize> {
        let buffers = [io::IoSlice::new(header), io::IoSlice::new(payload)];
        let address = socket2::SockAddr::from(peer);
        let result = self.socket.try_io(Interest::WRITABLE, || {
            self.with_std(|s| {
                self.metrics.send_syscalls.fetch_add(1, Relaxed);
                socket2::SockRef::from(s).send_to_vectored(&buffers, &address)
            })
        });
        if let Ok(n) = result {
            if n != header.len() + payload.len() {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "partial UDP datagram",
                ));
            }
            self.metrics.sent_datagrams.fetch_add(1, Relaxed);
            self.metrics.sent_wire_bytes.fetch_add(n as u64, Relaxed);
        }
        result
    }
    pub fn try_recv_from(&self, bytes: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let result = self.socket.try_io(Interest::READABLE, || {
            self.with_std(|s| {
                self.metrics.receive_syscalls.fetch_add(1, Relaxed);
                s.recv_from(bytes)
            })
        });
        if let Ok((n, _)) = result {
            self.metrics.received_datagrams.fetch_add(1, Relaxed);
            self.metrics
                .received_wire_bytes
                .fetch_add(n as u64, Relaxed);
        }
        result
    }
    pub async fn recv_from(&self, bytes: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        loop {
            match self.try_recv_from(bytes) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => self.socket.readable().await?,
                result => return result,
            }
        }
    }
    pub async fn readable(&self) -> io::Result<()> {
        self.socket.readable().await
    }
    pub fn metrics(&self) -> MetricsSnapshot {
        let mut out = self.metrics.snapshot();
        out.socket_recv_buffer = self.recv_buffer;
        out.socket_send_buffer = self.send_buffer;
        out.bound_udp_sockets = 1;
        out
    }
}
