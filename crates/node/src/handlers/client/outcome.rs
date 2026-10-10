//! How one serve stream ends, as a value.
//!
//! Every exit of `serve_stream` returns a [`ServeEnd`], so the compiler proves
//! each path names its outcome. The dispatch loop records it once on the
//! stream's `serve_stream` span — once, because the OpenTelemetry layer keeps
//! every recorded value of a field rather than the last.
//!
//! The same place counts it. Every failed inbound stream counts on exactly one
//! reason counter in [`INBOUND_FAILURE_REASONS`]: an `Ok` end through
//! [`ServeEnd::meter`], an `Err` end through [`ErrEnd::meter`]. Both are
//! exhaustive matches, so a new variant does not compile until it names its
//! counter.
//!
//! [`INBOUND_FAILURE_REASONS`]: crate::metrics::INBOUND_FAILURE_REASONS

use super::ServeRejectReason;
use super::wire::{ClientPaymentFault, PaidProgress, is_peer_attributable};
use crate::metrics::Metrics;
use tracing::Span;

/// How one serve stream ended.
#[derive(Debug, Clone, Copy)]
pub(super) enum ServeEnd {
    /// The whole request delivered, every interval paid, `StreamEnd` written.
    Completed {
        /// Payload bytes written to the client.
        bytes: u64,
    },
    /// Refused before delivery with a signed `StreamResponse`.
    Refused(ServeRejectReason),
    /// Stopped mid-delivery.
    Stopped {
        /// Why the node stopped.
        stop: ServeStop,
        /// Payload bytes written to the client before the stop.
        bytes: u64,
    },
    /// Reset with no signed response.
    Reset(ResetCause),
}

/// Why a serve stream was reset with no signed response. The causes call for
/// different responses: a full stream cap is this node's capacity, a bad binding
/// or an unreadable request is the client's fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResetCause {
    /// The per-connection stream cap was full (`RATE_LIMITED`).
    StreamCapFull,
    /// The client binding failed to verify (`MALFORMED_MESSAGE`).
    BadBinding,
    /// The first request could not be read: a timeout, a peer reset or lost
    /// connection, or a frame that failed to decode.
    RequestUnreadable,
}

impl ResetCause {
    /// The `reason` value a span records for this reset.
    const fn as_str(self) -> &'static str {
        match self {
            Self::StreamCapFull => "stream_cap_full",
            Self::BadBinding => "bad_binding",
            Self::RequestUnreadable => "request_unreadable",
        }
    }
}

/// The `reason` value a span records for a refusal: snake case, like every
/// other `reason` and `outcome` value, so one attribute reads one way. A
/// serve-miss refusal's log line records the same value.
pub(super) const fn refusal_reason(reason: ServeRejectReason) -> &'static str {
    match reason {
        ServeRejectReason::EvictedSinceProbe => "evicted_since_probe",
        ServeRejectReason::CacheMiss => "cache_miss",
        ServeRejectReason::InternalError => "internal_error",
        ServeRejectReason::UnknownLane => "unknown_lane",
        ServeRejectReason::OwnerMismatch => "owner_mismatch",
        ServeRejectReason::InsufficientDeposit => "insufficient_deposit",
        ServeRejectReason::PoolUnconfirmed => "pool_unconfirmed",
        ServeRejectReason::PoolClosing { .. } => "pool_closing",
        ServeRejectReason::SignerCapExhausted => "signer_cap_exhausted",
        ServeRejectReason::SignerFloorAtCap => "signer_floor_at_cap",
        ServeRejectReason::PullLoopGuard => "pull_loop_guard",
        ServeRejectReason::LoadShedHit => "load_shed_hit",
        ServeRejectReason::LoadShedMiss => "load_shed_miss",
        ServeRejectReason::RangeNotSatisfiable => "range_not_satisfiable",
        ServeRejectReason::BlobTooLarge => "blob_too_large",
        ServeRejectReason::HashDenied => "hash_denied",
        ServeRejectReason::ChainHashDenied => "chain_hash_denied",
        ServeRejectReason::OriginDenied => "origin_denied",
        ServeRejectReason::ForeignNamespaceDeclined => "foreign_declined",
        ServeRejectReason::ChainStale => "chain_stale",
    }
}

/// Why a serve stopped after delivery began.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ServeStop {
    /// The client sent a voucher the node rejected.
    VoucherRejected,
    /// The pool can no longer fund the floor credit (`PoolExhausted`).
    PoolExhausted,
    /// The signer's shared cap drained at other nodes (`SignerCapExhausted`).
    SignerCapExhausted,
    /// A blacklist takedown landed for the hash.
    Takedown,
    /// The payer spent its per-chunk proof budget without settling the chunk.
    ProofBudgetExhausted,
}

impl ServeStop {
    /// The `reason` value a span records for this stop.
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::VoucherRejected => "voucher_rejected",
            Self::PoolExhausted => "pool_exhausted",
            Self::SignerCapExhausted => "signer_cap_exhausted",
            Self::Takedown => "takedown",
            Self::ProofBudgetExhausted => "proof_budget_exhausted",
        }
    }
}

impl ServeEnd {
    /// The `outcome` value a span records for this end.
    const fn outcome(self) -> &'static str {
        match self {
            Self::Completed { .. } => "completed",
            Self::Refused(_) => "refused",
            Self::Stopped { .. } => "stopped",
            Self::Reset(_) => "reset",
        }
    }

    /// Count this end on its one reason counter. Returns whether the stream
    /// completed, which the caller feeds to the completed / failed family after
    /// the reason, so a scrape between the two never shows an unclaimed failure.
    pub(super) fn meter(self, metrics: &Metrics) -> bool {
        match self {
            Self::Completed { .. } => return true,
            Self::Refused(reason) => meter_refusal(metrics, reason),
            Self::Stopped { stop, .. } => match stop {
                ServeStop::VoucherRejected => metrics.serve_stream_voucher_rejected(),
                ServeStop::PoolExhausted => metrics.serve_stream_midstream_pool_exhausted(),
                ServeStop::SignerCapExhausted => {
                    metrics.serve_stream_midstream_signer_cap_exhausted();
                }
                ServeStop::Takedown => metrics.serve_stream_terminated_takedown(),
                ServeStop::ProofBudgetExhausted => metrics.serve_stream_proof_budget_exhausted(),
            },
            Self::Reset(cause) => match cause {
                ResetCause::StreamCapFull => metrics.serve_stream_rejected_stream_cap_full(),
                ResetCause::BadBinding => metrics.serve_stream_rejected_bad_binding(),
                ResetCause::RequestUnreadable => metrics.serve_stream_request_unreadable(),
            },
        }
        false
    }

    /// Record `outcome`, `reason` and `bytes` on `span`. Call once per span.
    pub(super) fn record(self, span: &Span) {
        span.record("outcome", self.outcome());
        match self {
            Self::Completed { bytes } => {
                span.record("bytes", bytes);
            }
            Self::Refused(reason) => {
                span.record("reason", refusal_reason(reason));
            }
            Self::Stopped { stop, bytes } => {
                span.record("reason", stop.as_str());
                span.record("bytes", bytes);
            }
            Self::Reset(cause) => {
                span.record("reason", cause.as_str());
            }
        }
    }
}

/// Bump the refusal counter for `reason`: one `serve_stream_rejected_*` sibling
/// per [`ServeRejectReason`].
fn meter_refusal(metrics: &Metrics, reason: ServeRejectReason) {
    match reason {
        ServeRejectReason::EvictedSinceProbe => {
            metrics.serve_stream_rejected_evicted_since_probe();
        }
        ServeRejectReason::CacheMiss => metrics.serve_stream_rejected_cache_miss(),
        ServeRejectReason::InternalError => metrics.serve_stream_rejected_internal_error(),
        ServeRejectReason::UnknownLane => {
            metrics.serve_stream_rejected_unknown_lane();
        }
        ServeRejectReason::OwnerMismatch => metrics.serve_stream_rejected_owner_mismatch(),
        ServeRejectReason::InsufficientDeposit => {
            metrics.serve_stream_rejected_insufficient_deposit();
        }
        ServeRejectReason::PoolUnconfirmed => {
            metrics.serve_stream_rejected_pool_unconfirmed();
        }
        ServeRejectReason::PoolClosing { .. } => metrics.serve_stream_rejected_pool_closing(),
        ServeRejectReason::SignerCapExhausted => {
            metrics.serve_stream_rejected_signer_cap_exhausted();
        }
        ServeRejectReason::SignerFloorAtCap => {
            metrics.serve_stream_rejected_signer_floor_at_cap();
        }
        ServeRejectReason::PullLoopGuard => metrics.serve_stream_rejected_pull_loop_guard(),
        ServeRejectReason::LoadShedHit => metrics.serve_stream_rejected_load_shed_hit(),
        ServeRejectReason::LoadShedMiss => metrics.serve_stream_rejected_load_shed_miss(),
        ServeRejectReason::RangeNotSatisfiable => {
            metrics.serve_stream_rejected_range_not_satisfiable();
        }
        ServeRejectReason::BlobTooLarge => metrics.serve_stream_rejected_blob_too_large(),
        ServeRejectReason::HashDenied => metrics.serve_stream_rejected_hash_denied(),
        ServeRejectReason::ChainHashDenied => {
            metrics.serve_stream_rejected_chain_hash_denied();
        }
        ServeRejectReason::OriginDenied => metrics.serve_stream_rejected_origin_denied(),
        ServeRejectReason::ForeignNamespaceDeclined => {
            metrics.serve_stream_rejected_foreign_declined();
        }
        ServeRejectReason::ChainStale => {
            metrics.serve_stream_rejected_chain_stale();
        }
    }
}

/// How a `serve_stream` error ends the stream: the reason an `Err` end counts
/// on, read from the markers the error carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ErrEnd {
    /// No peer marker: a fault this node caused.
    NodeFault,
    /// A [`ClientPaymentFault`]: a voucher that fails the rate check.
    VoucherRejected,
    /// A peer that left or broke the protocol after a voucher credited bytes
    /// ([`PaidProgress`]).
    ClientAbandoned,
    /// A peer that left or broke the protocol before any voucher credited a
    /// byte.
    ClientDeclined,
}

impl ErrEnd {
    /// Classify `e`. The node-fault test runs first, so a node fault that a serve
    /// loop tagged [`PaidProgress`] stays a node fault; a rate-check bail comes
    /// before the payment split, so it stays a rejected voucher after payment.
    ///
    /// Both proof-wait faults, the stall and the chunk's ceiling, carry
    /// [`PeerFault`](super::wire::PeerFault) and take the declined or abandoned
    /// split like any other peer that stops paying. The ceiling is not a node bug, so it does not
    /// belong on the node-fault counter, and every end must count on exactly one
    /// reason. Only the pull-through abandon counter, which is not a reason
    /// counter, leaves the ceiling out (`serve_leg`).
    pub(super) fn of(e: &anyhow::Error) -> Self {
        if !is_peer_attributable(e) {
            Self::NodeFault
        } else if e.is::<ClientPaymentFault>() {
            Self::VoucherRejected
        } else if e.is::<PaidProgress>() {
            Self::ClientAbandoned
        } else {
            Self::ClientDeclined
        }
    }

    /// Count this end on its one reason counter.
    pub(super) fn meter(self, metrics: &Metrics) {
        match self {
            Self::NodeFault => metrics.serve_stream_node_fault(),
            Self::VoucherRejected => metrics.serve_stream_voucher_rejected(),
            Self::ClientAbandoned => metrics.serve_stream_client_abandoned(),
            Self::ClientDeclined => metrics.serve_stream_client_declined(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests;
