//! Reputation-report transport: validation, rate limiting, and the trait seams
//! that keep this crate independent of `decdn-reputation` (ADR 008 §Gossip
//! Protocol, §Rate Limiting).
//!
//! The subscriber path validates an inbound [`decdn_protocol::ReputationReport`]
//! (signature, recency, staked-reporter membership), receiver-enforces the
//! per-(reporter, node) and per-reporter rate limits, then hands a
//! reputation-free [`ValidatedReport`] to a [`ReputationSink`] implemented by
//! the consumer. The publisher path drains pending outbound reports from a
//! [`ReportDrain`]. Neither trait references `decdn-reputation`, so the gossip
//! crate stays a leaf with respect to the scoring engine.

use std::collections::{HashMap, VecDeque};

use decdn_protocol::{GOSSIP_VERSION, GossipEnvelope, GossipPayload, ReportMetrics, SIGNATURE_LEN};
use thiserror::Error;

use crate::validation::MAX_TRAILING_BYTES;

/// Maximum age (seconds) of an accepted report (ADR 008 §Gossip Protocol).
pub const MAX_REPORT_AGE_SECS: u64 = 3600;
/// Maximum future skew (seconds) tolerated on a report timestamp (ADR 008).
pub const ALLOWED_CLOCK_SKEW_SECS: u64 = 300;
/// Maximum accepted reports per reporter per hour (ADR 008 §Rate Limiting).
pub const MAX_REPORTS_PER_REPORTER_PER_HR: usize = 10;
/// Rate-limit window in seconds (one hour).
const RATE_WINDOW_SECS: u64 = 3600;

/// A reputation report that passed signature, recency, and staked-reporter
/// validation. Carries only plain wire data so consumers can map it into the
/// reputation engine without this crate depending on `decdn-reputation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedReport {
    /// The rated node's `NodeId` bytes.
    pub provider: [u8; 32],
    /// The reporting node's `NodeId` bytes (verified signer).
    pub reporter: [u8; 32],
    /// Reported delivery rate in bytes/sec, if measured.
    pub delivery_speed: Option<u32>,
    /// Reported reachability, if observed.
    pub uptime_observed: Option<bool>,
    /// Reported data correctness, if observed.
    pub data_correct: Option<bool>,
    /// Report generation time (seconds since Unix epoch).
    pub timestamp_secs: u64,
}

/// Consumer-side sink for validated, rate-limited reports (ADR 008 §Network
/// Score Aggregation). Implemented in `decdn-node` over the reputation
/// aggregator. Called on the subscriber hot path — implementations must be
/// cheap and non-blocking.
pub trait ReputationSink: Send + Sync + 'static {
    /// Accept one validated report. Rate-limit drops happen before this call.
    fn accept(&self, report: ValidatedReport);
}

/// Membership test for the live staked-node set. Implemented in `decdn-node`
/// over the chain staker set (kept fresh by the `NodeRegistered` /
/// `NodeDeregistered` / `NodeAutoEjected` event tail). Gates two subscriber
/// paths: `NodeAnnounce` admission (ADR 001 rule 2 — see
/// [`crate::validation::validate_envelope`]) and reputation-report admission
/// (ADR 008 §Gossip Protocol). Called on the subscriber hot path —
/// implementations must be cheap and non-blocking.
pub trait StakedNodeSet: Send + Sync + 'static {
    /// Whether `node_id` (a `NodeId`'s 32 bytes) is a currently staked node.
    fn contains(&self, node_id: &[u8; 32]) -> bool;
}

// The ADR 008 staked-reporter admission gate used to live here as its own
// `ReportGate` enum, mirroring `AnnounceGate`'s shape. It was folded into
// `ReputationWiring::Enabled` in #1342: its `Enforce` payload is that variant's
// `staked` field and its `Disabled` is `ReputationWiring::Disabled`, so the
// "admission set without a sink" state stopped being representable rather than
// being warned about at runtime. Note the polarity inversion that made two
// same-shaped gates hazardous in the first place (#1338) is gone with it —
// `AnnounceGate::Disabled` fails OPEN, and there is no longer a fail-CLOSED
// twin of the same shape to confuse it with.

/// Source of pending outbound reports for the publisher (ADR 008 §Gossip
/// Protocol). Implemented in `decdn-node` over the observation buffer. Returns
/// the latest metrics observed per rated peer since the previous drain.
pub trait ReportDrain: Send + Sync + 'static {
    /// Take and clear all pending outbound observations.
    fn drain(&self) -> Vec<([u8; 32], ReportMetrics)>;
}

/// Every reason an inbound reputation envelope can be rejected. Labels are
/// stable Prometheus values, all prefixed `reputation_` so they never collide
/// with [`crate::AnnounceReject`] labels on the shared `inc_rejected` counter.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReputationReject {
    #[error("envelope postcard decode failed")]
    DecodeFailed,
    #[error("unknown envelope version")]
    UnknownVersion,
    #[error("not a reputation-report payload variant")]
    UnknownVariant,
    #[error("trailing bytes after envelope exceed {MAX_TRAILING_BYTES} byte allowance")]
    OversizeTrailingBytes,
    #[error("signature length != {SIGNATURE_LEN}")]
    BadSignatureLen,
    #[error("invalid public key")]
    InvalidPublicKey,
    #[error("body postcard encode failed")]
    BodyEncodeFailed,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("report older than {MAX_REPORT_AGE_SECS}s")]
    StaleReport,
    #[error("report timestamp more than {ALLOWED_CLOCK_SKEW_SECS}s in the future")]
    FutureReport,
    #[error("reporter not in staked set")]
    NotStakedReporter,
    #[error("reporter and provider are the same node (self-report)")]
    SelfReport,
    #[error("rate limit: 1 report per (reporter, node) per hour exceeded")]
    RateLimitedPair,
    #[error("rate limit: {MAX_REPORTS_PER_REPORTER_PER_HR} reports per reporter per hour exceeded")]
    RateLimitedReporter,
}

impl ReputationReject {
    /// Stable metric label for this rejection reason.
    pub const fn label(&self) -> &'static str {
        match self {
            Self::DecodeFailed => "reputation_decode_failed",
            Self::UnknownVersion => "reputation_unknown_version",
            Self::UnknownVariant => "reputation_unknown_variant",
            Self::OversizeTrailingBytes => "reputation_oversize_trailing_bytes",
            Self::BadSignatureLen => "reputation_bad_signature_len",
            Self::InvalidPublicKey => "reputation_invalid_public_key",
            Self::BodyEncodeFailed => "reputation_body_encode_failed",
            Self::InvalidSignature => "reputation_invalid_signature",
            Self::StaleReport => "reputation_stale_report",
            Self::FutureReport => "reputation_future_report",
            Self::NotStakedReporter => "reputation_not_staked_reporter",
            Self::SelfReport => "reputation_self_report",
            Self::RateLimitedPair => "reputation_rate_limited_pair",
            Self::RateLimitedReporter => "reputation_rate_limited_reporter",
        }
    }
}

/// Validate a postcard-encoded [`GossipEnvelope`] carrying a
/// [`decdn_protocol::ReputationReport`]. On success returns a
/// [`ValidatedReport`]. Stateful rate limiting is intentionally NOT done here
/// (see [`ReputationRateLimiter`]); this function is pure and unit-testable.
///
/// `now_secs` is the receiver's wall clock in seconds. `staked` gates reports
/// to staked reporters per ADR 008 §Gossip Protocol.
pub fn validate_reputation_envelope(
    bytes: &[u8],
    now_secs: u64,
    staked: &dyn StakedNodeSet,
) -> Result<ValidatedReport, ReputationReject> {
    // Version byte first (same discipline as `validate_envelope`): an unknown
    // version is a silent drop per ADR 013, not a decode error.
    let version = bytes.first().ok_or(ReputationReject::DecodeFailed)?;
    if *version != GOSSIP_VERSION {
        return Err(ReputationReject::UnknownVersion);
    }

    let (env, rest): (GossipEnvelope, &[u8]) =
        postcard::take_from_bytes(bytes).map_err(|_| ReputationReject::DecodeFailed)?;
    if rest.len() > MAX_TRAILING_BYTES {
        return Err(ReputationReject::OversizeTrailingBytes);
    }

    let GossipPayload::ReputationReport(report) = env.payload else {
        return Err(ReputationReject::UnknownVariant);
    };
    let body = report.body;

    // Reject self-reports (#861): a node signing a report about itself would
    // fold a self-vote into its own network score AND fill one of the three
    // distinct-reporter slots its score needs, dropping ADR 008's eclipse/Sybil
    // external-identity cost from 3 to 2. A cheap 32-byte comparison, so it
    // runs before the recency and signature checks.
    if body.reporter == body.provider {
        return Err(ReputationReject::SelfReport);
    }

    // Recency window (ADR 008): reject stale or far-future reports before
    // spending verification cycles.
    if now_secs.saturating_sub(body.timestamp_secs) > MAX_REPORT_AGE_SECS {
        return Err(ReputationReject::StaleReport);
    }
    if body.timestamp_secs.saturating_sub(now_secs) > ALLOWED_CLOCK_SKEW_SECS {
        return Err(ReputationReject::FutureReport);
    }

    // Signature verification against the claimed reporter key.
    let sig_bytes: &[u8; SIGNATURE_LEN] = report
        .signature
        .as_slice()
        .try_into()
        .map_err(|_| ReputationReject::BadSignatureLen)?;
    let pk = iroh::PublicKey::from_bytes(&body.reporter)
        .map_err(|_| ReputationReject::InvalidPublicKey)?;
    let signing_bytes = body
        .signing_bytes()
        .map_err(|_| ReputationReject::BodyEncodeFailed)?;
    let sig = iroh::Signature::from_bytes(sig_bytes);
    pk.verify(&signing_bytes, &sig)
        .map_err(|_| ReputationReject::InvalidSignature)?;

    // Staked-reporter gate (ADR 008): only staked nodes may submit reports.
    if !staked.contains(&body.reporter) {
        return Err(ReputationReject::NotStakedReporter);
    }

    Ok(ValidatedReport {
        provider: body.provider,
        reporter: body.reporter,
        delivery_speed: body.metrics.delivery_speed,
        uptime_observed: body.metrics.uptime_observed,
        data_correct: body.metrics.data_correct,
        timestamp_secs: body.timestamp_secs,
    })
}

/// Receiver-side sliding-window rate limiter (ADR 008 §Rate Limiting): at most
/// one report per (reporter, node) pair per hour, and at most
/// [`MAX_REPORTS_PER_REPORTER_PER_HR`] reports per reporter per hour. Held by
/// the subscriber task (single-threaded access — no internal locking).
#[derive(Debug, Default)]
pub struct ReputationRateLimiter {
    /// Accepted-report timestamps (secs) per reporter, within the window.
    per_reporter: HashMap<[u8; 32], VecDeque<u64>>,
    /// Last accepted-report time (secs) per (reporter, provider) pair.
    per_pair: HashMap<([u8; 32], [u8; 32]), u64>,
    /// Last `now_secs` at which expired `per_pair` / empty `per_reporter`
    /// entries were swept, so the O(n) sweep runs at most once per window
    /// rather than per report.
    last_swept_secs: u64,
}

impl ReputationRateLimiter {
    /// Create an empty limiter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check both limits for `(reporter, provider)` at `now_secs` and, if both
    /// pass, record the acceptance. Returns the matching reject otherwise.
    pub fn check_and_record(
        &mut self,
        reporter: [u8; 32],
        provider: [u8; 32],
        now_secs: u64,
    ) -> Result<(), ReputationReject> {
        self.sweep_expired(now_secs);
        if let Some(&last) = self.per_pair.get(&(reporter, provider))
            && now_secs.saturating_sub(last) < RATE_WINDOW_SECS
        {
            return Err(ReputationReject::RateLimitedPair);
        }
        let times = self.per_reporter.entry(reporter).or_default();
        prune_older_than(times, now_secs, RATE_WINDOW_SECS);
        if times.len() >= MAX_REPORTS_PER_REPORTER_PER_HR {
            return Err(ReputationReject::RateLimitedReporter);
        }
        times.push_back(now_secs);
        self.per_pair.insert((reporter, provider), now_secs);
        Ok(())
    }

    /// Drop `per_pair` entries older than the window and `per_reporter` entries
    /// whose deque has fully expired, so neither map grows unbounded over the
    /// process lifetime. Runs at most once per `RATE_WINDOW_SECS` (the maps only
    /// need an hour of history), so the O(n) scan cost is amortized to ~zero.
    fn sweep_expired(&mut self, now_secs: u64) {
        if now_secs.saturating_sub(self.last_swept_secs) < RATE_WINDOW_SECS {
            return;
        }
        self.last_swept_secs = now_secs;
        self.per_pair
            .retain(|_, &mut last| now_secs.saturating_sub(last) < RATE_WINDOW_SECS);
        self.per_reporter.retain(|_, times| {
            prune_older_than(times, now_secs, RATE_WINDOW_SECS);
            !times.is_empty()
        });
    }
}

/// Drop timestamps that fall outside `[now - window, now]` from the front.
fn prune_older_than(times: &mut VecDeque<u64>, now_secs: u64, window_secs: u64) {
    while let Some(&front) = times.front() {
        if now_secs.saturating_sub(front) >= window_secs {
            times.pop_front();
        } else {
            break;
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use decdn_protocol::{ReputationReport, ReputationReportBody};
    use iroh::SecretKey;

    struct AllStaked;
    impl StakedNodeSet for AllStaked {
        fn contains(&self, _node_id: &[u8; 32]) -> bool {
            true
        }
    }
    struct NoneStaked;
    impl StakedNodeSet for NoneStaked {
        fn contains(&self, _node_id: &[u8; 32]) -> bool {
            false
        }
    }

    const NOW: u64 = 1_700_000_000;

    fn metrics() -> ReportMetrics {
        ReportMetrics {
            delivery_speed: Some(1_048_576),
            uptime_observed: Some(true),
            data_correct: Some(true),
        }
    }

    fn body(reporter: &SecretKey, provider: [u8; 32], ts: u64) -> ReputationReportBody {
        ReputationReportBody {
            provider,
            reporter: *reporter.public().as_bytes(),
            metrics: metrics(),
            timestamp_secs: ts,
        }
    }

    fn encode_signed(sk: &SecretKey, body: ReputationReportBody) -> Vec<u8> {
        let signature = sk.sign(&body.signing_bytes().unwrap()).to_bytes().to_vec();
        postcard::to_allocvec(&GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::ReputationReport(ReputationReport { body, signature }),
        })
        .unwrap()
    }

    #[test]
    fn reputation_reject_labels_are_stable() {
        const fn expected(r: &ReputationReject) -> &'static str {
            match r {
                ReputationReject::DecodeFailed => "reputation_decode_failed",
                ReputationReject::UnknownVersion => "reputation_unknown_version",
                ReputationReject::UnknownVariant => "reputation_unknown_variant",
                ReputationReject::OversizeTrailingBytes => "reputation_oversize_trailing_bytes",
                ReputationReject::BadSignatureLen => "reputation_bad_signature_len",
                ReputationReject::InvalidPublicKey => "reputation_invalid_public_key",
                ReputationReject::BodyEncodeFailed => "reputation_body_encode_failed",
                ReputationReject::InvalidSignature => "reputation_invalid_signature",
                ReputationReject::StaleReport => "reputation_stale_report",
                ReputationReject::FutureReport => "reputation_future_report",
                ReputationReject::NotStakedReporter => "reputation_not_staked_reporter",
                ReputationReject::SelfReport => "reputation_self_report",
                ReputationReject::RateLimitedPair => "reputation_rate_limited_pair",
                ReputationReject::RateLimitedReporter => "reputation_rate_limited_reporter",
            }
        }
        for r in [
            ReputationReject::DecodeFailed,
            ReputationReject::UnknownVersion,
            ReputationReject::UnknownVariant,
            ReputationReject::OversizeTrailingBytes,
            ReputationReject::BadSignatureLen,
            ReputationReject::InvalidPublicKey,
            ReputationReject::BodyEncodeFailed,
            ReputationReject::InvalidSignature,
            ReputationReject::StaleReport,
            ReputationReject::FutureReport,
            ReputationReject::NotStakedReporter,
            ReputationReject::SelfReport,
            ReputationReject::RateLimitedPair,
            ReputationReject::RateLimitedReporter,
        ] {
            assert_eq!(r.label(), expected(&r), "label drift for {r:?}");
        }
    }

    #[test]
    fn happy_path() {
        let sk = SecretKey::generate();
        let bytes = encode_signed(&sk, body(&sk, [9u8; 32], NOW));
        let v = validate_reputation_envelope(&bytes, NOW, &AllStaked).expect("valid");
        assert_eq!(v.reporter, *sk.public().as_bytes());
        assert_eq!(v.provider, [9u8; 32]);
        assert_eq!(v.delivery_speed, Some(1_048_576));
    }

    #[test]
    fn self_report_rejected() {
        // #861: a node reporting on itself (reporter == provider) is rejected
        // even with a valid signature from a staked reporter — it must not
        // self-boost its score or fill a distinct-reporter slot.
        let sk = SecretKey::generate();
        let self_id = *sk.public().as_bytes();
        let bytes = encode_signed(&sk, body(&sk, self_id, NOW));
        assert_eq!(
            validate_reputation_envelope(&bytes, NOW, &AllStaked),
            Err(ReputationReject::SelfReport)
        );
    }

    #[test]
    fn unknown_version_rejected() {
        let sk = SecretKey::generate();
        let mut bytes = encode_signed(&sk, body(&sk, [9u8; 32], NOW));
        bytes[0] = 99;
        assert_eq!(
            validate_reputation_envelope(&bytes, NOW, &AllStaked),
            Err(ReputationReject::UnknownVersion)
        );
    }

    #[test]
    fn wrong_variant_rejected() {
        // A NodeAnnounce envelope must be rejected by the reputation validator.
        use decdn_protocol::{NodeAnnounce, NodeAnnounceBody};
        let sk = SecretKey::generate();
        let ann_body = NodeAnnounceBody {
            node_id: *sk.public().as_bytes(),
            region: "US".to_string(),
            timestamp_us: NOW * 1_000_000,
        };
        let signature = sk
            .sign(&ann_body.signing_bytes().unwrap())
            .to_bytes()
            .to_vec();
        let bytes = postcard::to_allocvec(&GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::NodeAnnounce(NodeAnnounce {
                body: ann_body,
                signature,
            }),
        })
        .unwrap();
        assert_eq!(
            validate_reputation_envelope(&bytes, NOW, &AllStaked),
            Err(ReputationReject::UnknownVariant)
        );
    }

    #[test]
    fn stale_and_future_rejected() {
        let sk = SecretKey::generate();
        let stale = encode_signed(&sk, body(&sk, [9u8; 32], NOW - MAX_REPORT_AGE_SECS - 1));
        assert_eq!(
            validate_reputation_envelope(&stale, NOW, &AllStaked),
            Err(ReputationReject::StaleReport)
        );
        let future = encode_signed(&sk, body(&sk, [9u8; 32], NOW + ALLOWED_CLOCK_SKEW_SECS + 1));
        assert_eq!(
            validate_reputation_envelope(&future, NOW, &AllStaked),
            Err(ReputationReject::FutureReport)
        );
    }

    #[test]
    fn recency_boundaries_accepted() {
        let sk = SecretKey::generate();
        let at_age = encode_signed(&sk, body(&sk, [9u8; 32], NOW - MAX_REPORT_AGE_SECS));
        assert!(validate_reputation_envelope(&at_age, NOW, &AllStaked).is_ok());
        let at_skew = encode_signed(&sk, body(&sk, [9u8; 32], NOW + ALLOWED_CLOCK_SKEW_SECS));
        assert!(validate_reputation_envelope(&at_skew, NOW, &AllStaked).is_ok());
    }

    #[test]
    fn wrong_key_signature_rejected() {
        let sk = SecretKey::generate();
        let other = SecretKey::generate();
        // Body claims `sk` as reporter, but is signed by `other`.
        let mut b = body(&sk, [9u8; 32], NOW);
        let signature = other.sign(&b.signing_bytes().unwrap()).to_bytes().to_vec();
        let bytes = postcard::to_allocvec(&GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::ReputationReport(ReputationReport {
                body: std::mem::replace(&mut b, body(&sk, [0u8; 32], NOW)),
                signature,
            }),
        })
        .unwrap();
        assert_eq!(
            validate_reputation_envelope(&bytes, NOW, &AllStaked),
            Err(ReputationReject::InvalidSignature)
        );
    }

    #[test]
    fn body_mutation_breaks_signature() {
        let sk = SecretKey::generate();
        let original = body(&sk, [9u8; 32], NOW);
        let signature = sk
            .sign(&original.signing_bytes().unwrap())
            .to_bytes()
            .to_vec();
        let mut mutated = original;
        mutated.provider = [1u8; 32];
        let bytes = postcard::to_allocvec(&GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::ReputationReport(ReputationReport {
                body: mutated,
                signature,
            }),
        })
        .unwrap();
        assert_eq!(
            validate_reputation_envelope(&bytes, NOW, &AllStaked),
            Err(ReputationReject::InvalidSignature)
        );
    }

    #[test]
    fn bad_signature_length_rejected() {
        let sk = SecretKey::generate();
        let b = body(&sk, [9u8; 32], NOW);
        let bytes = postcard::to_allocvec(&GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::ReputationReport(ReputationReport {
                body: b,
                signature: vec![0u8; 63],
            }),
        })
        .unwrap();
        assert_eq!(
            validate_reputation_envelope(&bytes, NOW, &AllStaked),
            Err(ReputationReject::BadSignatureLen)
        );
    }

    #[test]
    fn unstaked_reporter_rejected() {
        let sk = SecretKey::generate();
        let bytes = encode_signed(&sk, body(&sk, [9u8; 32], NOW));
        assert_eq!(
            validate_reputation_envelope(&bytes, NOW, &NoneStaked),
            Err(ReputationReject::NotStakedReporter)
        );
    }

    #[test]
    fn oversize_trailing_rejected() {
        let sk = SecretKey::generate();
        let mut bytes = encode_signed(&sk, body(&sk, [9u8; 32], NOW));
        bytes.extend(std::iter::repeat_n(0xAAu8, MAX_TRAILING_BYTES + 1));
        assert_eq!(
            validate_reputation_envelope(&bytes, NOW, &AllStaked),
            Err(ReputationReject::OversizeTrailingBytes)
        );
    }

    #[test]
    fn rate_limiter_pair_limit() {
        let mut rl = ReputationRateLimiter::new();
        let (r, p) = ([1u8; 32], [2u8; 32]);
        assert!(rl.check_and_record(r, p, NOW).is_ok());
        // Second report for the same pair within the hour is rejected.
        assert_eq!(
            rl.check_and_record(r, p, NOW + 10),
            Err(ReputationReject::RateLimitedPair)
        );
        // After the window the pair is admitted again.
        assert!(rl.check_and_record(r, p, NOW + RATE_WINDOW_SECS).is_ok());
    }

    #[test]
    fn rate_limiter_sweeps_expired_entries() {
        let mut rl = ReputationRateLimiter::new();
        // 50 distinct reporters, one report each (avoids the per-reporter cap),
        // all at NOW → 50 per_pair + 50 per_reporter entries.
        for i in 0..50u8 {
            let mut r = [0u8; 32];
            r[0] = i;
            rl.check_and_record(r, [9u8; 32], NOW).unwrap();
        }
        assert_eq!(rl.per_pair.len(), 50);
        // One more report a full window later triggers the sweep, dropping all
        // 50 now-expired pairs and their empty reporter deques.
        let later = NOW + RATE_WINDOW_SECS + 1;
        rl.check_and_record([200u8; 32], [9u8; 32], later).unwrap();
        assert_eq!(rl.per_pair.len(), 1, "expired pairs must be swept");
        assert_eq!(
            rl.per_reporter.len(),
            1,
            "emptied reporter deques must be swept"
        );
    }

    #[test]
    fn rate_limiter_reporter_limit() {
        let mut rl = ReputationRateLimiter::new();
        let r = [1u8; 32];
        // 10 distinct providers within the hour are accepted; the 11th is not.
        for i in 0..MAX_REPORTS_PER_REPORTER_PER_HR {
            let mut p = [0u8; 32];
            p[0] = u8::try_from(i).unwrap();
            assert!(rl.check_and_record(r, p, NOW + i as u64).is_ok());
        }
        assert_eq!(
            rl.check_and_record(r, [200u8; 32], NOW + 11),
            Err(ReputationReject::RateLimitedReporter)
        );
        // After the window slides past the earliest entries, admit again.
        assert!(
            rl.check_and_record(r, [201u8; 32], NOW + RATE_WINDOW_SECS + 1)
                .is_ok()
        );
    }
}
