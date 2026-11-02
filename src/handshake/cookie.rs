//! Random identities and the keyed source-address cookie.

use crate::{Error, Result};
use std::{
    net::SocketAddr,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{SystemTime, UNIX_EPOCH},
};

/// Cookie time bucket; current and previous bucket are accepted.
pub(super) const BUCKET_SECONDS: u64 = 30;
/// Rotation: the key is derived for each two-bucket epoch.
const KEY_ROTATION_BUCKETS: u64 = 2;

pub(super) fn nonce() -> Result<[u8; 16]> {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).map_err(|_| Error::Protocol("random source"))?;
    Ok(nonce)
}

/// Process-wide monotonic direction identity, OS-random seeded and never reused.
pub(super) fn identity() -> Result<u64> {
    static NEXT: OnceLock<AtomicU64> = OnceLock::new();
    if NEXT.get().is_none() {
        let mut seed = [0; 8];
        getrandom::fill(&mut seed).map_err(|_| Error::Protocol("random source"))?;
        let _ = NEXT.set(AtomicU64::new(u64::from_le_bytes(seed).max(1)));
    }
    NEXT.get()
        .expect("identity source initialized above")
        .try_update(Relaxed, Relaxed, |value| value.checked_add(1))
        .map_err(|_| Error::IdExhausted)
}

pub(super) fn time_bucket() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / BUCKET_SECONDS
}

/// RETRY cookie: keyed hash over normalized source address, incarnation, nonce and
/// time bucket. It proves source reachability and limits amplification only.
pub(super) fn cookie(
    key: [u8; 32],
    peer: SocketAddr,
    remote: u64,
    client: &[u8; 16],
    bucket: u64,
) -> [u8; 32] {
    let derived = blake3::keyed_hash(&key, &(bucket / KEY_ROTATION_BUCKETS).to_le_bytes());
    let mut hasher = blake3::Hasher::new_keyed(derived.as_bytes());
    match peer {
        SocketAddr::V4(address) => {
            hasher.update(&[4]);
            hasher.update(&address.ip().octets());
        }
        SocketAddr::V6(address) => {
            hasher.update(&[6]);
            hasher.update(&address.ip().octets());
            hasher.update(&address.scope_id().to_le_bytes());
        }
    }
    hasher.update(&peer.port().to_le_bytes());
    hasher.update(&remote.to_le_bytes());
    hasher.update(client);
    hasher.update(&bucket.to_le_bytes());
    *hasher.finalize().as_bytes()
}
