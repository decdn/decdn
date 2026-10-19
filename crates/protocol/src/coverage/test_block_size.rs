//! The overridable discovery-block size behind the `test-support` twins of
//! [`super::discovery_block_bytes`] and [`super::num_blocks`].

use std::sync::atomic::{AtomicU64, Ordering};

use super::DISCOVERY_BLOCK_BYTES;

/// The active block size. Starts at [`DISCOVERY_BLOCK_BYTES`], so an
/// un-overridden `test-support` build behaves exactly like production.
static BLOCK_BYTES: AtomicU64 = AtomicU64::new(DISCOVERY_BLOCK_BYTES);

pub(super) fn current() -> u64 {
    BLOCK_BYTES.load(Ordering::Relaxed)
}

pub(super) fn set(bytes: u64) {
    BLOCK_BYTES.store(bytes, Ordering::Relaxed);
}

pub(super) fn reset() {
    BLOCK_BYTES.store(DISCOVERY_BLOCK_BYTES, Ordering::Relaxed);
}
