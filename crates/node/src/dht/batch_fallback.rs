//! Per-receiver `BatchStore` support cache (ADR 022 §STORE Flow Batched
//! STORE, #648 / AC 20).
//!
//! `BatchStore` is a pure optimization with a per-hash `Store` fallback.
//! A publisher detects that a receiver does not support batching when the
//! receiver closes the `BatchStore` stream with `APP_ERR_UNSUPPORTED_MESSAGE`
//! (`0x01`) — the signal a pre-#648 deCDN node emits for the unimplemented
//! variant. On that close the publisher MUST fall back to per-hash `Store`
//! for that receiver, and SHOULD cache the negative result for a bounded
//! duration so it doesn't pay the drop-and-fallback round-trip on every
//! subsequent publish (ADR 022 §STORE Flow, §Schema Evolution).
//!
//! A receiver that instead *silently* drops the unknown variant — closing
//! with no decodable application code — is indistinguishable at the
//! transport layer from a connect/handshake/timeout failure (both surface
//! as no app code). `client::classify_batch_error`
//! deliberately treats that ambiguous case as a transient hard failure
//! (no cache, no per-hash retry) rather than risk an `n`-deep per-hash
//! timeout storm against a merely-unreachable peer; it is retried as a
//! batch on the next cycle. Only the explicit `0x01` close populates this
//! cache.
//!
//! This cache is the "SHOULD cache" half. It is intentionally tiny: a
//! `NodeId → Instant` map of "treat this receiver as batch-unsupported
//! until this time". Expired entries are pruned lazily on read. The
//! window is bounded so a receiver that gains support (e.g. after an
//! upgrade) is retried once the entry lapses, and a transient close that
//! was misclassified as unsupported self-heals.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::dht::routing::NodeId;

/// Default bound on how long a receiver is treated as batch-unsupported
/// after an `UNSUPPORTED` close. ADR 022 leaves this as a bounded "some
/// duration"; 15 minutes is half the *minimum* steady-state republish
/// interval ([`crate::dht::publish::STEADY_STATE_MIN`] = 30 min; window
/// 30–50 min), so a misclassified receiver's negative entry always lapses
/// before its next republish cycle, while a genuinely-old receiver isn't
/// re-probed on every publish.
pub const DEFAULT_BATCH_UNSUPPORTED_TTL: Duration = Duration::from_mins(15);

/// Per-receiver "is `BatchStore` supported" cache. Cheap to clone the
/// `Arc` of and share across the publisher's fan-out tasks; all methods
/// take `&self` and lock internally for the microsecond-scale map op.
#[allow(missing_debug_implementations)]
pub struct BatchStoreFallback {
    ttl: Duration,
    /// `peer → instant after which we retry batching`. Presence means
    /// "currently treated as unsupported". Pruned lazily in
    /// [`Self::supports_batch_at`].
    unsupported_until: Mutex<HashMap<NodeId, Instant>>,
}

impl BatchStoreFallback {
    /// Build a cache whose negative entries expire after `ttl`.
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            unsupported_until: Mutex::new(HashMap::new()),
        }
    }

    /// Whether `peer` should be attempted with `BatchStore`. `true` for a
    /// peer never marked unsupported, or whose unsupported window has
    /// lapsed (the stale entry is pruned in passing).
    #[must_use]
    pub fn supports_batch(&self, peer: &NodeId) -> bool {
        self.supports_batch_at(peer, Instant::now())
    }

    /// Mark `peer` batch-unsupported for `ttl` from now (call after a
    /// stream-close-without-ack on a `BatchStore` attempt).
    pub fn mark_unsupported(&self, peer: NodeId) {
        self.mark_unsupported_at(peer, Instant::now());
    }

    /// Number of currently-tracked unsupported peers (no pruning). Used
    /// by tests and as a cheap size signal.
    #[must_use]
    pub fn tracked(&self) -> usize {
        self.unsupported_until.lock().map_or(0, |m| m.len())
    }

    /// [`Self::supports_batch`] with an injected clock. A lapsed entry is
    /// removed so the map doesn't accumulate dead receivers.
    fn supports_batch_at(&self, peer: &NodeId, now: Instant) -> bool {
        let Ok(mut map) = self.unsupported_until.lock() else {
            // Poisoned lock: fail open (attempt batch). Worst case is one
            // drop-and-fallback round-trip, which is the un-cached
            // behaviour — strictly correct, just not optimized. A poisoned
            // mutex means another thread panicked while holding it; log at
            // error so the degraded (cache-disabled) state is visible,
            // matching the `handle_store` record-store-poison precedent.
            tracing::error!(
                "dht batch-fallback cache mutex poisoned; treating peer as batch-capable"
            );
            return true;
        };
        match map.get(peer) {
            Some(&until) if until > now => false,
            Some(_) => {
                // Window lapsed — prune and retry batching.
                map.remove(peer);
                true
            }
            None => true,
        }
    }

    /// [`Self::mark_unsupported`] with an injected clock.
    fn mark_unsupported_at(&self, peer: NodeId, now: Instant) {
        let Ok(mut map) = self.unsupported_until.lock() else {
            // Poisoned lock: the mark is dropped, so this peer keeps being
            // re-attempted as a batch (re-paying the drop-and-fallback
            // round-trip) until the lock recovers. Correct but un-cached;
            // log at error so the silent de-optimization is visible.
            tracing::error!("dht batch-fallback cache mutex poisoned; unsupported mark dropped");
            return;
        };
        // `checked_add` only overflows at the far end of the monotonic
        // clock; on the impossible overflow we fall back to `now`, making
        // the entry expire immediately (fail open to a batch attempt)
        // rather than panicking.
        let until = now.checked_add(self.ttl).unwrap_or(now);
        map.insert(peer, until);
    }
}

impl Default for BatchStoreFallback {
    fn default() -> Self {
        Self::new(DEFAULT_BATCH_UNSUPPORTED_TTL)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn peer(b: u8) -> NodeId {
        [b; 32]
    }

    #[test]
    fn fresh_peer_supports_batch() {
        let fb = BatchStoreFallback::new(Duration::from_mins(10));
        assert!(fb.supports_batch(&peer(1)));
        assert_eq!(fb.tracked(), 0);
    }

    #[test]
    fn marked_peer_is_unsupported_within_window() {
        let fb = BatchStoreFallback::new(Duration::from_mins(10));
        let now = Instant::now();
        fb.mark_unsupported_at(peer(1), now);
        // Anywhere inside the window: unsupported.
        assert!(!fb.supports_batch_at(&peer(1), now));
        assert!(!fb.supports_batch_at(&peer(1), now + Duration::from_secs(599)));
        assert_eq!(fb.tracked(), 1);
    }

    #[test]
    fn support_returns_after_window_lapses_and_prunes() {
        let fb = BatchStoreFallback::new(Duration::from_mins(10));
        let now = Instant::now();
        fb.mark_unsupported_at(peer(1), now);
        // Past the TTL: supported again, and the stale entry is removed.
        assert!(fb.supports_batch_at(&peer(1), now + Duration::from_secs(601)));
        assert_eq!(fb.tracked(), 0, "lapsed entry must be pruned on read");
    }

    #[test]
    fn marking_one_peer_does_not_affect_another() {
        let fb = BatchStoreFallback::new(Duration::from_mins(10));
        let now = Instant::now();
        fb.mark_unsupported_at(peer(1), now);
        assert!(!fb.supports_batch_at(&peer(1), now));
        assert!(fb.supports_batch_at(&peer(2), now));
    }

    #[test]
    fn re_marking_extends_the_window() {
        let fb = BatchStoreFallback::new(Duration::from_mins(10));
        let t0 = Instant::now();
        fb.mark_unsupported_at(peer(1), t0);
        // A later mark pushes the expiry forward.
        let t1 = t0 + Duration::from_mins(5);
        fb.mark_unsupported_at(peer(1), t1);
        // At t0 + 601s the *first* window would have lapsed, but the
        // re-mark at t1 extends it to t1 + 600 = t0 + 900.
        assert!(!fb.supports_batch_at(&peer(1), t0 + Duration::from_secs(601)));
        assert!(fb.supports_batch_at(&peer(1), t0 + Duration::from_secs(901)));
    }
}
