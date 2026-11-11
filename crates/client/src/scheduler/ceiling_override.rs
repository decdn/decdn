//! The test-only override of the cap-free sizing ceiling.
//!
//! The override is process-global, so it assumes the one-process-per-test
//! isolation `cargo nextest run` gives; two tests that override it
//! concurrently in one process (plain `cargo test`) would race.

use std::sync::atomic::{AtomicU64, Ordering};

use super::UNCAPPED_SIZING_CEILING;

/// The ceiling a test set, or `0` for the production value.
static OVERRIDE: AtomicU64 = AtomicU64::new(0);

/// The active ceiling.
pub(super) fn current() -> u64 {
    match OVERRIDE.load(Ordering::Relaxed) {
        0 => UNCAPPED_SIZING_CEILING,
        bytes => bytes,
    }
}

/// Set the cap-free sizing ceiling to `bytes` until the returned guard drops.
#[must_use = "the override is reverted when the returned guard is dropped"]
pub(crate) fn override_uncapped_sizing_ceiling(bytes: u64) -> CeilingGuard {
    OVERRIDE.store(bytes, Ordering::Relaxed);
    CeilingGuard { _private: () }
}

/// Restores the production ceiling when dropped.
#[derive(Debug)]
pub(crate) struct CeilingGuard {
    _private: (),
}

impl Drop for CeilingGuard {
    fn drop(&mut self) {
        OVERRIDE.store(0, Ordering::Relaxed);
    }
}
