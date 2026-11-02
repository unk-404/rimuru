//! OS wait-source bindings. Every function exists on every target; unsupported
//! operations return `RM_UNSUPPORTED` rather than removing an exported symbol.

pub(crate) enum Wait {
    Signaled,
    TimedOut,
    Failed,
}

#[cfg(windows)]
mod platform {
    use super::Wait;
    use crate::ffi::abi::{RM_IO, RM_UNSUPPORTED, RmStatus, RmWaitSourceKind};
    use windows_sys::Win32::{
        Foundation::{CloseHandle, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT},
        System::IO::PostQueuedCompletionStatus,
        System::Threading::{CreateEventW, INFINITE, ResetEvent, SetEvent, WaitForSingleObject},
    };

    pub(crate) const KIND: RmWaitSourceKind = crate::ffi::abi::RM_WAIT_WIN_EVENT;
    pub(crate) const HAS_IOCP: bool = true;
    pub(crate) const HAS_KQUEUE: bool = false;

    pub(crate) struct Source(isize);

    // SAFETY: a Winsock/Win32 HANDLE is an opaque kernel handle; all operations on
    // it are thread-safe system calls, and the handle is closed exactly once.
    unsafe impl Send for Source {}
    unsafe impl Sync for Source {}

    impl Source {
        pub(crate) fn create() -> Result<Self, RmStatus> {
            let handle = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
            if handle.is_null() {
                return Err(RM_IO);
            }
            Ok(Self(handle as isize))
        }

        fn handle(&self) -> HANDLE {
            self.0 as HANDLE
        }

        pub(crate) fn clear(&self) {
            unsafe {
                ResetEvent(self.handle());
            }
        }

        pub(crate) fn signal(&self) -> Result<(), RmStatus> {
            if unsafe { SetEvent(self.handle()) } == 0 {
                Err(RM_IO)
            } else {
                Ok(())
            }
        }

        pub(crate) fn wait(&self, timeout_ns: u64) -> Wait {
            let millis = if timeout_ns == u64::MAX {
                INFINITE
            } else {
                timeout_ns.div_ceil(1_000_000).min(u32::MAX as u64) as u32
            };
            // SAFETY: live manual-reset event handle owned by this source.
            match unsafe { WaitForSingleObject(self.handle(), millis) } {
                WAIT_OBJECT_0 => Wait::Signaled,
                WAIT_TIMEOUT => Wait::TimedOut,
                _ => Wait::Failed,
            }
        }

        pub(crate) fn raw(&self) -> u64 {
            self.0 as u64
        }
    }

    impl Drop for Source {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.handle());
            }
        }
    }

    pub(crate) fn post_iocp(port: usize, key: usize) -> Result<(), RmStatus> {
        // SAFETY: caller-owned IOCP handle; lpOverlapped is NULL by contract.
        let result =
            unsafe { PostQueuedCompletionStatus(port as HANDLE, 0, key, std::ptr::null()) };
        if result == 0 { Err(RM_IO) } else { Ok(()) }
    }

    pub(crate) fn register_kqueue(_: i32, _: usize) -> Result<(), RmStatus> {
        Err(RM_UNSUPPORTED)
    }

    pub(crate) fn trigger_kqueue(_: i32, _: usize) -> Result<(), RmStatus> {
        Err(RM_UNSUPPORTED)
    }

    pub(crate) fn delete_kqueue(_: i32, _: usize) -> Result<(), RmStatus> {
        Err(RM_UNSUPPORTED)
    }
}

#[cfg(unix)]
mod platform {
    use super::Wait;
    use crate::ffi::abi::{RM_IO, RM_UNSUPPORTED, RmStatus, RmWaitSourceKind};
    use std::ffi::c_int;

    pub(crate) const KIND: RmWaitSourceKind = crate::ffi::abi::RM_WAIT_POSIX_FD;
    pub(crate) const HAS_IOCP: bool = false;
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub(crate) const HAS_KQUEUE: bool = true;
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    pub(crate) const HAS_KQUEUE: bool = false;

    pub(crate) struct Source {
        read_fd: c_int,
        write_fd: c_int,
    }

    // SAFETY: a file descriptor is an integer handle; read/write/poll are
    // thread-safe system calls and close happens exactly once in `Drop`.
    unsafe impl Send for Source {}
    unsafe impl Sync for Source {}

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn create_fd() -> Result<(c_int, c_int), RmStatus> {
        // SAFETY: eventfd with fixed flags; no pointers.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 { Err(RM_IO) } else { Ok((fd, fd)) }
    }

    #[cfg(any(test, not(any(target_os = "linux", target_os = "android"))))]
    fn create_pipe() -> Result<(c_int, c_int), RmStatus> {
        let mut fds = [0 as c_int; 2];
        // SAFETY: two-element output array.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            return Err(RM_IO);
        }
        for fd in fds {
            // SAFETY: just-created descriptors.
            unsafe {
                let flags = libc::fcntl(fd, libc::F_GETFL);
                if flags < 0
                    || libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0
                    || libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) < 0
                {
                    libc::close(fds[0]);
                    libc::close(fds[1]);
                    return Err(RM_IO);
                }
            }
        }
        Ok((fds[0], fds[1]))
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn create_fd() -> Result<(c_int, c_int), RmStatus> {
        create_pipe()
    }

    #[cfg(test)]
    pub(super) fn test_pipe_source() -> Result<Source, RmStatus> {
        create_pipe().map(|(read_fd, write_fd)| Source { read_fd, write_fd })
    }

    impl Source {
        pub(crate) fn create() -> Result<Self, RmStatus> {
            create_fd().map(|(read_fd, write_fd)| Self { read_fd, write_fd })
        }

        pub(crate) fn clear(&self) {
            let mut buffer = [0u8; 64];
            loop {
                // SAFETY: valid buffer and owned descriptor.
                let read =
                    unsafe { libc::read(self.read_fd, buffer.as_mut_ptr().cast(), buffer.len()) };
                if read <= 0 {
                    break;
                }
            }
        }

        pub(crate) fn signal(&self) -> Result<(), RmStatus> {
            // eventfd requires an 8-byte counter; pipes accept one byte.
            #[cfg(any(target_os = "linux", target_os = "android"))]
            let value = 1u64.to_ne_bytes();
            #[cfg(not(any(target_os = "linux", target_os = "android")))]
            let value = [1u8];
            // SAFETY: valid buffer and owned descriptor; EAGAIN is a coalesced signal.
            let written = unsafe { libc::write(self.write_fd, value.as_ptr().cast(), value.len()) };
            if written < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::WouldBlock {
                    return Ok(());
                }
                return Err(RM_IO);
            }
            Ok(())
        }

        pub(crate) fn wait(&self, timeout_ns: u64) -> Wait {
            let millis = if timeout_ns == u64::MAX {
                -1
            } else {
                timeout_ns.div_ceil(1_000_000).min(c_int::MAX as u64) as c_int
            };
            let mut descriptor = libc::pollfd {
                fd: self.read_fd,
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: single valid pollfd.
            let result = unsafe { libc::poll(&mut descriptor, 1, millis) };
            if result == 0 {
                Wait::TimedOut
            } else if result < 0 {
                if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                    Wait::TimedOut
                } else {
                    Wait::Failed
                }
            } else if descriptor.revents & libc::POLLIN != 0 {
                Wait::Signaled
            } else {
                Wait::Failed
            }
        }

        pub(crate) fn raw(&self) -> u64 {
            self.read_fd as u64
        }
    }

    impl Drop for Source {
        fn drop(&mut self) {
            // SAFETY: owned descriptor closed exactly once here.
            unsafe {
                libc::close(self.read_fd);
                if self.write_fd != self.read_fd {
                    libc::close(self.write_fd);
                }
            }
        }
    }

    pub(crate) fn post_iocp(_: usize, _: usize) -> Result<(), RmStatus> {
        Err(RM_UNSUPPORTED)
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub(crate) fn register_kqueue(kq: i32, ident: usize) -> Result<(), RmStatus> {
        kernel_event(kq, ident, libc::EV_ADD | libc::EV_CLEAR, 0)
    }

    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    pub(crate) fn register_kqueue(_: i32, _: usize) -> Result<(), RmStatus> {
        Err(RM_UNSUPPORTED)
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub(crate) fn trigger_kqueue(kq: i32, ident: usize) -> Result<(), RmStatus> {
        kernel_event(kq, ident, 0, libc::NOTE_TRIGGER)
    }

    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    pub(crate) fn trigger_kqueue(_: i32, _: usize) -> Result<(), RmStatus> {
        Err(RM_UNSUPPORTED)
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub(crate) fn delete_kqueue(kq: i32, ident: usize) -> Result<(), RmStatus> {
        kernel_event(kq, ident, libc::EV_DELETE, 0)
    }

    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    pub(crate) fn delete_kqueue(_: i32, _: usize) -> Result<(), RmStatus> {
        Err(RM_UNSUPPORTED)
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    fn kernel_event(kq: i32, ident: usize, flags: u16, fflags: u32) -> Result<(), RmStatus> {
        let mut event: libc::kevent = {
            // SAFETY: kevent is a plain C struct; zero is a valid empty event.
            unsafe { std::mem::zeroed() }
        };
        event.ident = ident;
        event.filter = libc::EVFILT_USER;
        event.flags = flags;
        event.fflags = fflags;
        // SAFETY: host-owned kqueue descriptor and one initialized event.
        let result =
            unsafe { libc::kevent(kq, &event, 1, std::ptr::null_mut(), 0, std::ptr::null()) };
        if result < 0 { Err(RM_IO) } else { Ok(()) }
    }
}

pub(crate) use platform::{HAS_IOCP, HAS_KQUEUE, KIND, Source};

#[cfg(windows)]
pub(crate) use platform::post_iocp;
#[cfg(windows)]
pub(crate) use platform::{delete_kqueue, register_kqueue, trigger_kqueue};

#[cfg(unix)]
pub(crate) use platform::post_iocp;
#[cfg(unix)]
pub(crate) use platform::{delete_kqueue, register_kqueue, trigger_kqueue};

#[cfg(test)]
mod tests {
    use super::{Source, Wait};

    #[test]
    fn native_source_signals_and_clears() {
        let source = Source::create().unwrap();
        assert!(matches!(source.wait(0), Wait::TimedOut));
        source.signal().unwrap();
        assert!(matches!(source.wait(1_000_000_000), Wait::Signaled));
        source.clear();
        assert!(matches!(source.wait(0), Wait::TimedOut));
        source.signal().unwrap();
        assert!(matches!(source.wait(1_000_000_000), Wait::Signaled));
    }

    #[cfg(unix)]
    #[test]
    fn pipe_source_signals_and_clears() {
        let source = super::platform::test_pipe_source().unwrap();
        assert!(matches!(source.wait(0), Wait::TimedOut));
        source.signal().unwrap();
        assert!(matches!(source.wait(1_000_000_000), Wait::Signaled));
        source.clear();
        assert!(matches!(source.wait(0), Wait::TimedOut));
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    #[test]
    fn kqueue_user_event_registers_triggers_and_deletes() {
        // SAFETY: the returned descriptor is owned and closed below.
        let kq = unsafe { libc::kqueue() };
        assert!(kq >= 0);
        let ident = 42;
        super::register_kqueue(kq, ident).unwrap();
        super::trigger_kqueue(kq, ident).unwrap();
        let mut event: libc::kevent = unsafe { std::mem::zeroed() };
        let timeout = libc::timespec {
            tv_sec: 1,
            tv_nsec: 0,
        };
        // SAFETY: live kqueue and initialized one-event output buffer.
        let count = unsafe { libc::kevent(kq, std::ptr::null(), 0, &mut event, 1, &timeout) };
        assert_eq!(count, 1);
        assert_eq!(event.ident, ident);
        assert_eq!(event.filter, libc::EVFILT_USER);
        super::delete_kqueue(kq, ident).unwrap();
        unsafe { libc::close(kq) };
    }
}
