//! Optional Windows UDP segmentation on the existing endpoint socket.
use std::os::windows::io::AsRawSocket;
use tokio::net::UdpSocket;
use windows_sys::Win32::Networking::WinSock::{
    IPPROTO_UDP, UDP_SEND_MSG_SIZE, WSAEINVAL, WSAEMSGSIZE, WSAENOPROTOOPT, WSAEOPNOTSUPP,
    setsockopt,
};

pub(super) fn enable_segmentation(socket: &UdpSocket, size: usize) -> bool {
    let Ok(value) = u32::try_from(size) else {
        return false;
    };
    // SAFETY: socket owns a live Winsock handle, and value points to a DWORD
    // for the duration of the call. The option is installed before any IO.
    unsafe {
        setsockopt(
            socket.as_raw_socket() as usize,
            IPPROTO_UDP,
            UDP_SEND_MSG_SIZE,
            (&raw const value).cast(),
            size_of::<u32>() as i32,
        ) == 0
    }
}

pub(crate) fn unsupported(error: &std::io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(WSAEINVAL | WSAEMSGSIZE | WSAENOPROTOOPT | WSAEOPNOTSUPP)
    )
}
