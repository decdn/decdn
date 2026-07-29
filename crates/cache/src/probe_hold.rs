//! Probe-triggered eviction-hold constants (ADR 005 §Probe-triggered
//! eviction hold, §Hold budget; appendix-blob-cache-eviction.md §4).
//!
//! When a node signs `has_blob: true` in a `ProbeResponse` it keeps the blob
//! eviction-exempt for [`PROBE_HOLD_DURATION`] so it is still resident when the
//! probing client's follow-up pull arrives — an availability guarantee, kept
//! because the node is about to earn on that pull. All three timing parameters
//! derive from a single base value [`PROBE_SLASH_WINDOW`], which remains the
//! on-chain rate-manipulation probe↔stream window (ADR 014).

use std::time::Duration;

/// The on-chain slashing window for probe responses (ADR 005). The single
/// base value the other timing parameters derive from.
pub const PROBE_SLASH_WINDOW: Duration = Duration::from_secs(30);

/// Extra margin over [`PROBE_SLASH_WINDOW`] absorbing network latency between
/// signing and the requester opening a stream.
pub const PROBE_HOLD_MARGIN: Duration = Duration::from_secs(5);

/// How long a blob stays eviction-exempt after the node signs
/// `has_blob: true` for it: the slashing window plus a safety margin (35s).
pub const PROBE_HOLD_DURATION: Duration = PROBE_SLASH_WINDOW.saturating_add(PROBE_HOLD_MARGIN);

/// Default maximum number of concurrently held (eviction-exempt) blobs (ADR
/// 005 §Hold budget). Holds are per-blob: many peers probing one hash share a
/// single slot. The canonical definition now lives in the
/// `decdn-config-types` leaf crate (#578) so the config default
/// (`decdn_common::config`) and the cache engine default cannot drift apart;
/// re-exported here for the existing `crate::probe_hold::DEFAULT_MAX_PROBE_HOLDS`
/// path.
pub use decdn_config_types::DEFAULT_MAX_PROBE_HOLDS;

/// Outcome of [`crate::CacheEngine::try_probe_hold`]. The caller maps this to
/// the `has_blob` it signs (ADR 005 §Probe-triggered eviction hold) — only
/// [`ProbeHoldOutcome::Held`] permits `has_blob: true`. The variants
/// distinguish the three `has_blob: false` causes so the probe handler can
/// emit the right metric without a second cache lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeHoldOutcome {
    /// Blob present (and not operator-evicted) **and** a hold is guaranteed
    /// for the full slashing window — safe to sign `has_blob: true`.
    Held,
    /// Blob absent or operator-evicted — sign `has_blob: false`. Not a
    /// degradation: the node simply does not have the content.
    ///
    /// Deliberately the one outcome with **no** metric. The
    /// `probe_hold_unavailable` counter records hold *refusals* for content the
    /// node has; a true negative is not a refusal, and counting it would swamp
    /// the signal every alert on that metric depends on. Despite the shared
    /// word, this is not `ProbeHoldUnavailableReason` — that enum's values all
    /// mean "we have it but would not hold it".
    Unavailable,
    /// Blob present but **all hold slots are in use** (`max_probe_holds > 0`
    /// and the live-hold count has reached it) — sign `has_blob: false` and
    /// count it as a `probe_hold_unavailable{reason="exhausted"}` availability
    /// degradation (ADR 005 §Hold budget). Genuine budget pressure: the
    /// operator-actionable
    /// remedy is to raise `max_probe_holds`. Never a safety fault — the node
    /// loses revenue but is never falsely slashed.
    BudgetExhausted,
    /// Blob present but the eviction-hold path is **disabled by config**
    /// (`max_probe_holds == 0`) — sign `has_blob: false`. Operationally
    /// distinct from [`Self::BudgetExhausted`] (#739): this is an intentional
    /// operator decision, not load. Counted as
    /// `probe_hold_unavailable{reason="disabled"}`, never as
    /// `{reason="exhausted"}` (whose alert remedy is "increase
    /// `max_probe_holds`", nonsensical when holds are deliberately off).
    HoldsDisabled,
}
