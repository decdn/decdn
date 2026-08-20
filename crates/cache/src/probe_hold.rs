//! Probe-triggered eviction-hold constants (ADR 005 §Probe-triggered
//! eviction hold, §Hold budget; ADR 040 §Pinning, durable operator-evict, and
//! the probe-hold stay engine-enforced).
//!
//! When a node signs `has_blob: true` in a `ProbeResponse` it tries to keep the
//! blob eviction-exempt for [`PROBE_HOLD_DURATION`] so it is still resident when
//! the probing client's follow-up pull arrives. The hold is **best-effort**, not
//! a guarantee: presence governs the advertisement, and a hold is placed only if
//! a slot is free (see [`ProbeHoldOutcome::BudgetExhausted`]). All three timing
//! parameters derive from a single base value [`PROBE_SLASH_WINDOW`], which
//! remains the on-chain rate-manipulation probe↔stream window (ADR 014).

use std::time::Duration;

/// The on-chain probe↔stream window `SlashJudge` enforces for rate
/// manipulation (ADR 014; surfaced in ADR 005 §Derived constants). The single
/// base value the other timing parameters derive from.
pub const PROBE_SLASH_WINDOW: Duration = Duration::from_secs(30);

/// Extra margin over [`PROBE_SLASH_WINDOW`] absorbing network latency between
/// signing and the requester opening a stream.
pub const PROBE_HOLD_MARGIN: Duration = Duration::from_secs(5);

/// How long a blob stays eviction-exempt after the node signs
/// `has_blob: true` for it: [`PROBE_SLASH_WINDOW`] plus [`PROBE_HOLD_MARGIN`]
/// (35s) — long enough to cover the probe→pull round trip. Derived from the
/// slashing window because the two share a timescale, not because a hold has
/// any slashing consequence (ADR 005 §Derived constants).
pub const PROBE_HOLD_DURATION: Duration = PROBE_SLASH_WINDOW.saturating_add(PROBE_HOLD_MARGIN);

/// Default maximum number of concurrently held (eviction-exempt) blobs (ADR
/// 005 §Hold budget). Holds are per-blob: many peers probing one hash share a
/// single slot. The canonical definition lives in the
/// `decdn-config-types` leaf crate (#578) so the config default
/// (`decdn_common::config`) and the cache engine default cannot drift apart;
/// re-exported here as `crate::probe_hold::DEFAULT_MAX_PROBE_HOLDS`.
pub use decdn_config_types::DEFAULT_MAX_PROBE_HOLDS;

/// Outcome of [`crate::CacheEngine::try_probe_hold`], carrying two independent
/// facts (ADR 005 §Probe-triggered eviction hold):
///
/// - **whether to advertise** the blob — [`ProbeHoldOutcome::advertises`];
/// - **whether a hold was actually placed** — [`ProbeHoldOutcome::hold_placed`].
///
/// Holds are best-effort, so these are two independent bits:
/// [`Self::BudgetExhausted`] advertises *without* holding. Use the two
/// predicates rather than matching on variants, so the mapping stays in one
/// place; the variants themselves exist so the probe handler can emit the right
/// metric without a second cache lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeHoldOutcome {
    /// Blob present (and not operator-evicted) **and** an eviction hold was
    /// placed, so the blob stays resident for the follow-up pull — sign
    /// `has_blob: true`.
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
    /// and the live-hold count has reached it). The caller still advertises
    /// `has_blob: true` but places no hold, so the blob may be LRU-evicted
    /// before the pull — costing one wasted round trip, never a slash. Counted
    /// as `probe_hold_unavailable{reason="exhausted"}` (ADR 005 §Hold budget);
    /// the operator-actionable remedy for the lost holds is to raise
    /// `max_probe_holds`.
    BudgetExhausted,
    /// Blob present but the eviction-hold path is **disabled by config**
    /// (`max_probe_holds == 0`) — the operator opt-out from advertising
    /// store-backed content, so the caller signs `has_blob: false`.
    /// Operationally distinct from [`Self::BudgetExhausted`] (#739): a
    /// deliberate choice, not load. Counted as
    /// `probe_hold_unavailable{reason="disabled"}`, never as
    /// `{reason="exhausted"}` (whose alert remedy is "increase
    /// `max_probe_holds`", nonsensical when holds are deliberately off).
    ///
    /// Scope note: this silences *store-backed* advertisements only. Content
    /// servable from a configured origin takes no hold and is advertised
    /// regardless — see the origin-held fallback in the probe handler.
    HoldsDisabled,
}

impl ProbeHoldOutcome {
    /// Whether this outcome authorises signing `has_blob: true` for the
    /// **store-backed** answer (the origin-held fallback may still promote a
    /// `false` afterwards).
    ///
    /// Presence governs the answer, not the hold: [`Self::BudgetExhausted`]
    /// advertises despite placing no hold, because refusing there would let a
    /// probe flood that fills the hold cache suppress truthful answers.
    #[must_use]
    pub const fn advertises(self) -> bool {
        matches!(self, Self::Held | Self::BudgetExhausted)
    }

    /// Whether an eviction hold was actually placed, making the blob
    /// LRU-exempt for [`PROBE_HOLD_DURATION`].
    ///
    /// Distinct from [`Self::advertises`]: holds are best-effort, and only
    /// [`Self::Held`] consumes a slot.
    #[must_use]
    pub const fn hold_placed(self) -> bool {
        matches!(self, Self::Held)
    }
}
