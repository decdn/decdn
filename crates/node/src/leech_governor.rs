//! Seed-leech caps for window-paced cache-miss pull-through (#856, ADR 037
//! §Seed-leech caps).
//!
//! The per-request `pull_ahead_bytes` window (enforced by the serve loop) bounds
//! the loss on a *single* abandoned request. Two node-wide caps, implemented
//! here, bound abuse spread across requests:
//!
//! - **Global unrecouped-leech budget.** A rolling node-wide
//!   `Σ(bytes pulled for cache misses) − Σ(bytes served)`. When it reaches
//!   `max_unrecouped_leech_bytes` the node refuses to *begin or continue* a
//!   speculative pull (one for a range it does not already hold) and resumes as
//!   it serves bytes and recoups. Absorbs distributed abuse — many sources each
//!   requesting one unpopular hash — in aggregate, independent of distribution.
//! - **Per-peer share ratio.** The node will not pull more than
//!   `share_ratio × bytes_served_to_that_peer`, plus a small initial allowance
//!   (`pull_ahead_bytes`) so a peer with no service history can still be served
//!   the opening window. Bounds concentrated single-peer manufactured demand,
//!   mirroring a `BitTorrent` share ratio.
//!
//! Neither cap ever refuses to serve a range the node already holds — only
//! speculative pulls are gated. State is in-memory, cumulative since process
//! start, and the per-peer table is bounded (`MAX_TRACKED_PEERS`) so a churn
//! of distinct peers cannot grow it without bound.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use crate::metrics::Metrics;

/// Hard cap on the per-peer leech table. At ~56 bytes per entry this is ~3.5 MB
/// resident — generous for any real peer set. When full, the least-recently
/// touched peer is evicted (its history forgotten); an actively-pulling leecher
/// is touched every chunk, so it is never the eviction victim.
const MAX_TRACKED_PEERS: usize = 65_536;

/// Fixed-point denominator for `share_ratio_percent` (so `100` == 1.0×).
const SHARE_RATIO_SCALE: u64 = 100;

/// Operator-tunable seed-leech cap parameters (ADR 037 §Parameters), resolved
/// from `[cache]` config. All three default finite/bounded per the ADR's
/// "load-bearing commitments".
#[derive(Debug, Clone, Copy)]
pub struct LeechCaps {
    /// Node-wide circuit breaker on aggregate speculative spend, in bytes.
    /// `0` disables the global cap (unbounded — the per-request window and the
    /// share ratio remain in force).
    pub max_unrecouped_leech_bytes: u64,
    /// Per-peer initial pull allowance for a peer with no service history — the
    /// opening window. Set equal to `pull_ahead_bytes` so a fresh peer can be
    /// served exactly the first window before its share ratio must catch up.
    pub initial_allowance_bytes: u64,
    /// Per-peer pull ceiling as a percentage of bytes served to that peer
    /// (`100` == 1.0×). `0` pins the peer to only its `initial_allowance_bytes`
    /// (no growth with service); large values effectively disable the per-peer
    /// cap, leaving only the global budget.
    pub share_ratio_percent: u64,
}

/// One peer's cumulative speculative-pull accounting.
#[derive(Debug, Default, Clone, Copy)]
struct PeerLeech {
    /// Bytes speculatively pulled to satisfy this peer's cache-miss requests.
    pulled: u64,
    /// Bytes served to this peer (raises its pull allowance).
    served: u64,
    /// Last-touch tick for LRU eviction.
    last_tick: u64,
}

#[derive(Debug, Default)]
struct LeechState {
    /// Node-wide bytes speculatively pulled (saturating).
    global_pulled: u64,
    /// Node-wide bytes served (saturating); recoups the global budget.
    global_served: u64,
    peers: HashMap<[u8; 32], PeerLeech>,
    /// LRU index ordered by each peer's `last_tick`, so the least-recently
    /// touched peer (the eviction victim) is the first entry — `O(log n)` to
    /// find and evict, rather than an `O(n)` scan of `peers`. Ticks are unique
    /// and monotonic, so the map holds exactly one entry per tracked peer.
    lru: BTreeMap<u64, [u8; 32]>,
    /// Monotonic LRU clock.
    tick: u64,
}

/// Node-wide seed-leech governor (#856). Cheap to share via [`Arc`]; the serve
/// handler holds one and consults it before each speculative pull and records
/// served bytes from the voucher path.
pub struct LeechGovernor {
    caps: LeechCaps,
    state: Mutex<LeechState>,
    metrics: Arc<Metrics>,
}

impl std::fmt::Debug for LeechGovernor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeechGovernor")
            .field("caps", &self.caps)
            .finish_non_exhaustive()
    }
}

impl LeechGovernor {
    /// Build a governor with the given caps, wiring the pause counters.
    #[must_use]
    pub fn new(caps: LeechCaps, metrics: Arc<Metrics>) -> Self {
        Self {
            caps,
            state: Mutex::new(LeechState::default()),
            metrics,
        }
    }

    /// Whether a speculative pull for `peer` may proceed right now: the global
    /// unrecouped budget has headroom AND the peer is under its share allowance.
    /// Used both to *admit* a fresh pull and to gate *continuing* one window at a
    /// time. On denial it bumps the corresponding pause metric (global budget vs.
    /// per-peer ratio) and returns `false`, and the caller pauses/refuses.
    ///
    /// Never gates serving a range already held — the caller only consults this
    /// for the speculative-pull branch.
    pub fn may_pull(&self, peer: &[u8; 32]) -> bool {
        let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);

        // Global budget first: a node-wide breach pauses every peer.
        if self.caps.max_unrecouped_leech_bytes > 0 {
            let unrecouped = guard.global_pulled.saturating_sub(guard.global_served);
            if unrecouped >= self.caps.max_unrecouped_leech_bytes {
                drop(guard);
                self.metrics.node_pull_through_leech_budget_paused();
                return false;
            }
        }

        // Per-peer share ratio: allowance = initial + served × share_ratio.
        let tick = guard.next_tick();
        let peer_entry = guard.touch(peer, tick);
        let allowance = self.caps.initial_allowance_bytes.saturating_add(
            peer_entry
                .served
                .saturating_mul(self.caps.share_ratio_percent)
                / SHARE_RATIO_SCALE,
        );
        if peer_entry.pulled >= allowance {
            drop(guard);
            self.metrics.node_pull_through_share_ratio_paused();
            return false;
        }
        true
    }

    /// Account `bytes` speculatively pulled for `peer` (raises the node-wide
    /// unrecouped frontier and the peer's pulled total). Call as chunks are
    /// pulled in the window-paced loop.
    pub fn record_pulled(&self, peer: &[u8; 32], bytes: u64) {
        let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        guard.global_pulled = guard.global_pulled.saturating_add(bytes);
        let tick = guard.next_tick();
        guard.touch(peer, tick);
        if let Some(entry) = guard.peers.get_mut(peer) {
            entry.pulled = entry.pulled.saturating_add(bytes);
        }
    }

    /// Account `bytes` served to `peer` (recoups the global budget and raises the
    /// peer's share allowance). Called from the voucher path for ALL accepted
    /// downstream deliveries — cache-hit and pull-through alike — so an honest
    /// peer's service history credits its ratio.
    pub fn record_served(&self, peer: &[u8; 32], bytes: u64) {
        let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        guard.global_served = guard.global_served.saturating_add(bytes);
        let tick = guard.next_tick();
        guard.touch(peer, tick);
        if let Some(entry) = guard.peers.get_mut(peer) {
            entry.served = entry.served.saturating_add(bytes);
        }
    }
}

impl LeechState {
    /// Advance and return the LRU clock.
    const fn next_tick(&mut self) -> u64 {
        self.tick = self.tick.saturating_add(1);
        self.tick
    }

    /// Ensure a per-peer entry exists, stamp its last-touch tick, and return a
    /// copy. Evicts the least-recently-touched peer first if the table is full
    /// and `peer` is new — an active leecher is touched every chunk, so its tick
    /// is always recent and it is never the eviction victim. The `lru` index
    /// keeps both the victim lookup and the re-stamp `O(log n)`.
    fn touch(&mut self, peer: &[u8; 32], tick: u64) -> PeerLeech {
        if let Some(entry) = self.peers.get_mut(peer) {
            // Existing peer: move its LRU position from the old tick to `tick`.
            let old_tick = entry.last_tick;
            entry.last_tick = tick;
            let copy = *entry;
            self.lru.remove(&old_tick);
            self.lru.insert(tick, *peer);
            return copy;
        }
        // New peer: evict the least-recently-touched first if the table is full.
        if self.peers.len() >= MAX_TRACKED_PEERS
            && let Some((&victim_tick, &victim)) = self.lru.iter().next()
        {
            self.lru.remove(&victim_tick);
            self.peers.remove(&victim);
        }
        let entry = PeerLeech {
            last_tick: tick,
            ..Default::default()
        };
        self.peers.insert(*peer, entry);
        self.lru.insert(tick, *peer);
        entry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn governor(max: u64, initial: u64, ratio_percent: u64) -> LeechGovernor {
        LeechGovernor::new(
            LeechCaps {
                max_unrecouped_leech_bytes: max,
                initial_allowance_bytes: initial,
                share_ratio_percent: ratio_percent,
            },
            Arc::new(Metrics::new()),
        )
    }

    const PEER_A: [u8; 32] = [1u8; 32];
    const PEER_B: [u8; 32] = [2u8; 32];

    #[test]
    fn fresh_peer_gets_the_opening_window() {
        let gov = governor(0, 1_000, 0);
        assert!(
            gov.may_pull(&PEER_A),
            "a no-history peer must get the opening window"
        );
        gov.record_pulled(&PEER_A, 1_000);
        // With share_ratio 0 and no service, the allowance is exactly the opening
        // window — once consumed, further pulls are refused.
        assert!(
            !gov.may_pull(&PEER_A),
            "peer exhausted its initial allowance with no service"
        );
    }

    #[test]
    fn share_ratio_grows_allowance_with_service() {
        // initial 0, ratio 200% (2.0×): a peer may pull up to 2× what it served.
        let gov = governor(0, 0, 200);
        assert!(
            !gov.may_pull(&PEER_A),
            "no initial allowance and no service → no pull"
        );
        gov.record_served(&PEER_A, 500);
        assert!(
            gov.may_pull(&PEER_A),
            "after serving 500, the peer may pull up to 1000"
        );
        gov.record_pulled(&PEER_A, 1_000);
        assert!(
            !gov.may_pull(&PEER_A),
            "pulled 1000 == 2× served; ceiling reached"
        );
        gov.record_served(&PEER_A, 250); // allowance now 1500
        assert!(
            gov.may_pull(&PEER_A),
            "further service raises the allowance again"
        );
    }

    #[test]
    fn global_budget_pauses_all_peers_and_recoups_on_serve() {
        // Generous per-peer allowance so only the global cap binds.
        let gov = governor(1_000, 1_000_000, 100);
        gov.record_pulled(&PEER_A, 1_000); // unrecouped == max
        assert!(
            !gov.may_pull(&PEER_B),
            "global breach pauses an unrelated peer too"
        );
        gov.record_served(&PEER_A, 600); // unrecouped 400 < 1000
        assert!(
            gov.may_pull(&PEER_B),
            "serving bytes recoups the global budget"
        );
    }

    #[test]
    fn zero_global_cap_disables_only_the_global_budget() {
        let gov = governor(0, 0, 100);
        gov.record_served(&PEER_A, 10);
        gov.record_pulled(&PEER_A, 1_000_000_000); // far beyond any budget
        // Global cap is off, but the per-peer share ratio still binds.
        assert!(
            !gov.may_pull(&PEER_A),
            "share ratio still caps the peer with the global cap off"
        );
    }

    #[test]
    fn full_table_evicts_lru_and_keeps_the_active_peer() {
        // initial 1_000, ratio 100% (1.0×): a peer's allowance grows with
        // service, so an evicted-and-reset peer is observably different from one
        // that kept its history.
        let gov = governor(0, 1_000, 100);
        gov.record_served(&PEER_A, 10_000); // A's allowance is now ~11_000
        // Fill the table past capacity with distinct one-touch peers, keeping A
        // the most-recently-used on every step so it is never the victim.
        for i in 0..(MAX_TRACKED_PEERS as u64 + 10) {
            let mut id = [0xFFu8; 32];
            id[..8].copy_from_slice(&i.to_le_bytes());
            gov.record_served(&id, 1);
            gov.may_pull(&PEER_A); // touch A so its tick stays the most recent
        }
        // A survived with its history intact: it may still pull well past a fresh
        // peer's initial 1_000-byte allowance. An evicted-and-reset A refuses here.
        gov.record_pulled(&PEER_A, 5_000);
        assert!(
            gov.may_pull(&PEER_A),
            "the continuously-active peer must survive eviction with its history intact"
        );
    }

    #[test]
    fn counters_saturate_without_panicking() {
        // A peer that has served an enormous amount has an enormous allowance, so
        // a large (but smaller) pull is still admitted — and nothing overflows.
        let gov = governor(0, 0, 100);
        gov.record_served(&PEER_A, u64::MAX);
        gov.record_served(&PEER_A, u64::MAX); // global_served saturates at MAX
        gov.record_pulled(&PEER_A, 1_000_000);
        assert!(
            gov.may_pull(&PEER_A),
            "a heavy server may still pull; saturating math must not wrap"
        );
    }
}
