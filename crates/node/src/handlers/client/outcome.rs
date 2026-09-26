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
/// other `reason` and `outcome` value, so one attribute reads one way.
const fn refusal_reason(reason: ServeRejectReason) -> &'static str {
    match reason {
        ServeRejectReason::EvictedSinceProbe => "evicted_since_probe",
        ServeRejectReason::CacheMiss => "cache_miss",
        ServeRejectReason::InternalError => "internal_error",
        ServeRejectReason::UnknownChannel => "unknown_lane",
        ServeRejectReason::OwnerMismatch => "owner_mismatch",
        ServeRejectReason::InsufficientDeposit => "insufficient_deposit",
        ServeRejectReason::PoolUnconfirmed => "pool_unconfirmed",
        ServeRejectReason::SignerCapExhausted => "signer_cap_exhausted",
        ServeRejectReason::SignerFloorAtCap => "signer_floor_at_cap",
        ServeRejectReason::LoadShedHit => "load_shed_hit",
        ServeRejectReason::LoadShedMiss => "load_shed_miss",
        ServeRejectReason::RangeNotSatisfiable => "range_not_satisfiable",
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
        ServeRejectReason::UnknownChannel => {
            metrics.serve_stream_rejected_unknown_lane();
        }
        ServeRejectReason::OwnerMismatch => metrics.serve_stream_rejected_owner_mismatch(),
        ServeRejectReason::InsufficientDeposit => {
            metrics.serve_stream_rejected_insufficient_deposit();
        }
        ServeRejectReason::PoolUnconfirmed => {
            metrics.serve_stream_rejected_pool_unconfirmed();
        }
        ServeRejectReason::SignerCapExhausted => {
            metrics.serve_stream_rejected_signer_cap_exhausted();
        }
        ServeRejectReason::SignerFloorAtCap => {
            metrics.serve_stream_rejected_signer_floor_at_cap();
        }
        ServeRejectReason::LoadShedHit => metrics.serve_stream_rejected_load_shed_hit(),
        ServeRejectReason::LoadShedMiss => metrics.serve_stream_rejected_load_shed_miss(),
        ServeRejectReason::RangeNotSatisfiable => {
            metrics.serve_stream_rejected_range_not_satisfiable();
        }
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
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
    use tracing_subscriber::prelude::*;

    use super::super::dispatch::{record_request, serve_stream_span};
    use super::super::wire::{ClientPaymentFault, PeerFault, tag_paid_progress};
    use super::{ErrEnd, ResetCause, ServeEnd, ServeStop};
    use crate::handlers::client::{ServeRejectReason, StreamRequest};
    use crate::metrics::{INBOUND_FAILURE_REASONS, Metrics};

    /// One exported span: its name and its attributes as strings.
    type Exported = (String, HashMap<String, String>);

    /// Exporter that keeps each exported span's name and attributes.
    #[derive(Debug, Clone, Default)]
    struct AttrExporter(Arc<Mutex<Vec<Exported>>>);

    impl SpanExporter for AttrExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            let mut spans = self.0.lock().unwrap();
            for span in batch {
                let attrs = span
                    .attributes
                    .iter()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect();
                spans.push((span.name.into_owned(), attrs));
            }
            Ok(())
        }
    }

    fn export_one(end: ServeEnd) -> HashMap<String, String> {
        let exporter = AttrExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let peer = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let local = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let req = StreamRequest {
            hash: [0xAB; 32],
            namespace_id: [0; 32],
            pool_id: [0xCD; 32],
            byte_offset: 4096,
            byte_len: 8192,
            timestamp_us: 0,
        };
        tracing::subscriber::with_default(subscriber, || {
            let span = serve_stream_span(peer, local);
            record_request(&span, &req);
            end.record(&span);
        });
        provider.force_flush().unwrap();
        let spans = exporter.0.lock().unwrap().clone();
        assert_eq!(spans.len(), 1, "exported: {spans:?}");
        let (name, attrs) = spans.into_iter().next().unwrap();
        assert_eq!(name, "serve_stream");
        assert_eq!(attrs["peer"], peer.to_string());
        assert_eq!(attrs["local_node_id"], local.to_string());
        attrs
    }

    /// The join keys a requester's `open_progressive_pull` span matches on
    /// reach the exporter in the same renderings: lowercase-hex `hash` (as
    /// `iroh_blobs::Hash` and `ContentHash` print), `0x` `pool_id` (as `B256`
    /// prints), and the numeric range.
    #[test]
    fn serve_stream_span_exports_the_join_keys_and_outcome() {
        let attrs = export_one(ServeEnd::Completed { bytes: 8192 });
        assert_eq!(attrs["hash"], "ab".repeat(32));
        assert_eq!(attrs["pool_id"], format!("0x{}", "cd".repeat(32)));
        assert_eq!(attrs["byte_offset"], "4096");
        assert_eq!(attrs["byte_len"], "8192");
        assert_eq!(attrs["direction"], "inbound");
        assert_eq!(attrs["outcome"], "completed");
        assert_eq!(attrs["bytes"], "8192");
    }

    #[test]
    fn stopped_and_refused_streams_record_their_reason() {
        let stopped = export_one(ServeEnd::Stopped {
            stop: ServeStop::PoolExhausted,
            bytes: 100,
        });
        assert_eq!(stopped["outcome"], "stopped");
        assert_eq!(stopped["reason"], "pool_exhausted");
        assert_eq!(stopped["bytes"], "100");

        let refused = export_one(ServeEnd::Refused(super::ServeRejectReason::CacheMiss));
        assert_eq!(refused["outcome"], "refused");
        assert_eq!(refused["reason"], "cache_miss");
        assert!(!refused.contains_key("bytes"));

        let reset = export_one(ServeEnd::Reset(ResetCause::BadBinding));
        assert_eq!(reset["outcome"], "reset");
        assert_eq!(reset["reason"], "bad_binding");

        let unreadable = export_one(ServeEnd::Reset(ResetCause::RequestUnreadable));
        assert_eq!(unreadable["outcome"], "reset");
        assert_eq!(unreadable["reason"], "request_unreadable");

        let budget = export_one(ServeEnd::Stopped {
            stop: ServeStop::ProofBudgetExhausted,
            bytes: 64,
        });
        assert_eq!(budget["outcome"], "stopped");
        assert_eq!(budget["reason"], "proof_budget_exhausted");
        assert_eq!(budget["bytes"], "64");
    }

    /// The one listed reason counter `meter` moves, asserting it moves exactly
    /// one and leaves every other listed counter at zero.
    fn only_reason(meter: impl FnOnce(&Metrics)) -> &'static str {
        let metrics = Metrics::new();
        meter(&metrics);
        let text = metrics.encode().unwrap();
        let value = |name: &str| -> u64 {
            text.lines()
                .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
                .unwrap_or(0)
        };
        let moved: Vec<&'static str> = INBOUND_FAILURE_REASONS
            .iter()
            .copied()
            .filter(|n| value(n) != 0)
            .collect();
        assert_eq!(moved.len(), 1, "expected one reason to move, got {moved:?}");
        assert_eq!(value(moved[0]), 1, "{} moved by more than one", moved[0]);
        moved[0]
    }

    /// Names every refusal, so a new `ServeRejectReason` fails to compile here
    /// until the test below covers it.
    const fn refusal_is_listed(reason: ServeRejectReason) {
        match reason {
            ServeRejectReason::EvictedSinceProbe
            | ServeRejectReason::CacheMiss
            | ServeRejectReason::InternalError
            | ServeRejectReason::UnknownChannel
            | ServeRejectReason::OwnerMismatch
            | ServeRejectReason::InsufficientDeposit
            | ServeRejectReason::PoolUnconfirmed
            | ServeRejectReason::SignerCapExhausted
            | ServeRejectReason::SignerFloorAtCap
            | ServeRejectReason::LoadShedHit
            | ServeRejectReason::LoadShedMiss
            | ServeRejectReason::RangeNotSatisfiable
            | ServeRejectReason::HashDenied
            | ServeRejectReason::ChainHashDenied
            | ServeRejectReason::OriginDenied
            | ServeRejectReason::ForeignNamespaceDeclined
            | ServeRejectReason::ChainStale => {}
        }
    }

    /// Every failed end — each refusal, stop and reset, and each error shape —
    /// counts on exactly one listed reason, distinct ends never share one, and
    /// together they reach every name in `INBOUND_FAILURE_REASONS`. A completed
    /// end counts on none.
    #[test]
    fn every_failed_end_counts_on_exactly_one_listed_reason() {
        let refusals = [
            ServeRejectReason::EvictedSinceProbe,
            ServeRejectReason::CacheMiss,
            ServeRejectReason::InternalError,
            ServeRejectReason::UnknownChannel,
            ServeRejectReason::OwnerMismatch,
            ServeRejectReason::InsufficientDeposit,
            ServeRejectReason::PoolUnconfirmed,
            ServeRejectReason::SignerCapExhausted,
            ServeRejectReason::SignerFloorAtCap,
            ServeRejectReason::LoadShedHit,
            ServeRejectReason::LoadShedMiss,
            ServeRejectReason::RangeNotSatisfiable,
            ServeRejectReason::HashDenied,
            ServeRejectReason::ChainHashDenied,
            ServeRejectReason::OriginDenied,
            ServeRejectReason::ForeignNamespaceDeclined,
            ServeRejectReason::ChainStale,
        ];
        let stops = [
            ServeStop::VoucherRejected,
            ServeStop::PoolExhausted,
            ServeStop::SignerCapExhausted,
            ServeStop::Takedown,
            ServeStop::ProofBudgetExhausted,
        ];
        let resets = [
            ResetCause::StreamCapFull,
            ResetCause::BadBinding,
            ResetCause::RequestUnreadable,
        ];
        let ends = refusals
            .into_iter()
            .inspect(|r| refusal_is_listed(*r))
            .map(ServeEnd::Refused)
            .chain(
                stops
                    .into_iter()
                    .map(|stop| ServeEnd::Stopped { stop, bytes: 1 }),
            )
            .chain(resets.into_iter().map(ServeEnd::Reset));

        let mut reached = std::collections::BTreeMap::new();
        for end in ends {
            let name = only_reason(|m| assert!(!end.meter(m), "{end:?} is not completed"));
            let other = reached.insert(name, format!("{end:?}"));
            assert!(other.is_none(), "{end:?} and {other:?} share {name}");
        }

        let peer_gone = || anyhow::Error::new(PeerFault).context("peer gone");
        let errors = [
            (anyhow::anyhow!("store fault"), ErrEnd::NodeFault),
            (
                tag_paid_progress(anyhow::anyhow!("store fault"), 1),
                ErrEnd::NodeFault,
            ),
            (
                tag_paid_progress(anyhow::Error::new(ClientPaymentFault).context("rate"), 1),
                ErrEnd::VoucherRejected,
            ),
            (tag_paid_progress(peer_gone(), 1), ErrEnd::ClientAbandoned),
            (tag_paid_progress(peer_gone(), 0), ErrEnd::ClientDeclined),
        ];
        for (err, want) in errors {
            assert_eq!(ErrEnd::of(&err), want, "classify {err:#}");
            // A rate-check bail is a rejected voucher, so it shares the
            // `Stopped { VoucherRejected }` counter; the other error ends each
            // have a counter no `ServeEnd` reaches.
            let expected = match want {
                ErrEnd::NodeFault => "decdn_serve_stream_node_fault_total",
                ErrEnd::VoucherRejected => "decdn_serve_stream_voucher_rejected_total",
                ErrEnd::ClientAbandoned => "decdn_serve_stream_client_abandoned_total",
                ErrEnd::ClientDeclined => "decdn_serve_stream_client_declined_total",
            };
            assert_eq!(only_reason(|m| want.meter(m)), expected, "{want:?}");
            let shared = reached.get(expected);
            assert!(
                shared.is_none_or(|end| *end == format!("{want:?}")
                    || (want == ErrEnd::VoucherRejected && end.contains("VoucherRejected"))),
                "{want:?} shares {expected} with {shared:?}"
            );
            reached
                .entry(expected)
                .or_insert_with(|| format!("{want:?}"));
        }
        let listed: std::collections::BTreeSet<&str> =
            INBOUND_FAILURE_REASONS.iter().copied().collect();
        let reached: std::collections::BTreeSet<&str> = reached.into_keys().collect();
        assert_eq!(reached, listed);

        let metrics = Metrics::new();
        assert!(ServeEnd::Completed { bytes: 1 }.meter(&metrics));
    }
}
