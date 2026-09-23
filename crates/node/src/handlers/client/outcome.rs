//! How one serve stream ends, as a value.
//!
//! Every exit of `serve_stream` returns a [`ServeEnd`], so the compiler proves
//! each path names its outcome. The dispatch loop records it once on the
//! stream's `serve_stream` span — once, because the OpenTelemetry layer keeps
//! every recorded value of a field rather than the last.

use super::ServeRejectReason;
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

/// Why a serve stream was reset with no signed response. The two causes call
/// for opposite responses: a full stream cap is this node's capacity, a bad
/// binding is the client's fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResetCause {
    /// The per-connection stream cap was full (`RATE_LIMITED`).
    StreamCapFull,
    /// The client binding failed to verify (`MALFORMED_MESSAGE`).
    BadBinding,
}

impl ResetCause {
    /// The `reason` value a span records for this reset.
    const fn as_str(self) -> &'static str {
        match self {
            Self::StreamCapFull => "stream_cap_full",
            Self::BadBinding => "bad_binding",
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
}

impl ServeStop {
    /// The `reason` value a span records for this stop.
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::VoucherRejected => "voucher_rejected",
            Self::PoolExhausted => "pool_exhausted",
            Self::SignerCapExhausted => "signer_cap_exhausted",
            Self::Takedown => "takedown",
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
    use tracing_subscriber::prelude::*;

    use super::super::dispatch::{record_request, serve_stream_span};
    use super::{ResetCause, ServeEnd, ServeStop};
    use crate::handlers::client::StreamRequest;

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
    }
}
