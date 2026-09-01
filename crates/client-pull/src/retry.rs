//! Failover classification shared by both paid-fetch paths (#1174, ADR 037 §
//! Fallback): given a fetch failure, decide whether trying a DIFFERENT provider
//! (single-source failover) or reassigning the range to a DIFFERENT lane
//! (multi-source scheduler) can succeed, or whether the failure is terminal for
//! the whole fetch.
//!
//! One classifier serves both callers so a "try elsewhere" verdict is identical
//! whether the CLI's single-source loop or the multi-source scheduler asks: a
//! payment/pool fault is global to the one shared pool (ADR 003), so no other
//! provider or lane can fix it, while a delivery fault is a property of one
//! provider's leg and is worth retrying against another.

use crate::{BlobTooLarge, UpstreamRefused, UpstreamVoucherRejected};
use decdn_protocol::client::StreamError;

/// Whether a failed delivery attempt should fall over to the next candidate
/// provider (#1174, ADR 037 § Fallback) — or, in the multi-source scheduler,
/// have its range reassigned to another lane — or end the fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDisposition {
    /// The failure is a property of THIS provider or its delivery — not of the
    /// content or the caller's pool — so the next candidate (or lane) is worth
    /// trying.
    RetryElsewhere,
    /// Another provider or lane cannot fix this: the shared pool or funder is
    /// refused everywhere, or the blob is unservable to this client whoever holds
    /// it.
    Terminal,
}

/// Classify a fetch failure for failover (#1174, ADR 037 § Fallback): decide
/// whether continuing to the next candidate provider — or, in the multi-source
/// scheduler, reassigning the range to another lane — can succeed.
///
/// The classification follows the retry disposition each [`StreamError`] variant
/// already documents, plus the pool model's global facts:
///
/// - A payment-layer rejection ([`UpstreamVoucherRejected`], or a mid-stream
///   [`StreamError::VoucherRejected`]) is **terminal**. One pool fans out to
///   every provider (ADR 003), so its remaining deposit, its capability cap, and
///   the on-chain delivery floor are the same against any provider, and the
///   driver has already exhausted any wallet-less watermark self-heal.
/// - [`StreamError::OriginBlacklisted`] is **terminal** — the pool's funder is
///   refused under this address everywhere.
/// - A [`crate::BlobTooLarge`] is **terminal** — the blob is BLAKE3-addressed, so
///   its size is identical whoever serves it, and its received bytes stay over the
///   client's cap.
/// - Every other refusal ([`StreamError::NotFound`], `Overloaded`,
///   `BlobTooLarge`, `InternalError`, `EvictedSinceProbe`, `HashBlacklisted`)
///   and every non-refusal error — a stall (the progress deadline tripped), a
///   transport fault, or a bao/hash verification failure on the bytes this node
///   served — is a property of this provider's delivery, so the fetch **fails
///   over**. When every candidate is exhausted the caller returns the last such
///   error, so a genuinely absent or wrong hash still surfaces its refusal.
///
/// A shared-pool exhaustion ([`crate::PoolExhausted`], the pacer's
/// [`PaceDecision::Refuse`](crate::PaceDecision)) is deliberately **NOT** terminal
/// here — it is `RetryElsewhere`. Single-source failover (#1174) intends to try
/// another provider on a budget refusal: provider A's rate-priced next voucher may
/// not fit the remaining deposit while a cheaper provider B's does. Only the
/// multi-source scheduler treats pool exhaustion as terminal (all its lanes draw
/// the ONE pool, so no lane can fix it), and it applies that rule itself rather
/// than folding it into this shared classifier.
#[must_use]
pub fn retry_disposition(err: &anyhow::Error) -> RetryDisposition {
    use RetryDisposition::{RetryElsewhere, Terminal};

    if err.downcast_ref::<UpstreamVoucherRejected>().is_some()
        || err.downcast_ref::<BlobTooLarge>().is_some()
    {
        return Terminal;
    }
    if let Some(refused) = err.downcast_ref::<UpstreamRefused>() {
        return match refused.error() {
            StreamError::OriginBlacklisted | StreamError::VoucherRejected { .. } => Terminal,
            _ => RetryElsewhere,
        };
    }
    RetryElsewhere
}

#[cfg(test)]
mod tests {
    use super::{RetryDisposition, retry_disposition};
    use crate::driver::PoolExhausted;
    use crate::{BlobTooLarge, UpstreamRefused, UpstreamVoucherRejected};
    use decdn_protocol::client::StreamError;

    /// The refusal these tests classify, built the way the fetch path builds it:
    /// the typed `UpstreamRefused` sentinel (#1144). Never hand-roll one with
    /// `anyhow!("delivery refused: …")` — the classifier downcasts, so a
    /// look-alike string would exercise nothing.
    fn refusal(error: StreamError) -> anyhow::Error {
        anyhow::Error::new(UpstreamRefused::mid_stream(error))
    }

    /// A payment-layer voucher rejection is terminal for failover: the shared
    /// pool's cap/deposit/floor are global, so the next provider fails the same.
    #[test]
    fn voucher_rejection_is_terminal() {
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: decdn_protocol::client::VoucherRejectReason::SpendingCapExhausted,
            bundle: None,
        });
        assert_eq!(retry_disposition(&err), RetryDisposition::Terminal);
    }

    /// A refused `OriginBlacklisted` is terminal — the pool's funder is refused
    /// under this address everywhere, so no other provider can serve it.
    #[test]
    fn origin_blacklisted_refusal_is_terminal() {
        let err = refusal(StreamError::OriginBlacklisted);
        assert_eq!(retry_disposition(&err), RetryDisposition::Terminal);
    }

    /// The client's own size cap, tripped by the bytes that actually arrived, is
    /// terminal — the blob is content-addressed, so its size is the same anywhere.
    #[test]
    fn blob_too_large_is_terminal() {
        let err = anyhow::Error::new(BlobTooLarge {
            received: 1 << 40,
            ceiling: 1 << 20,
        });
        assert_eq!(retry_disposition(&err), RetryDisposition::Terminal);
    }

    /// A shared-pool exhaustion is `RetryElsewhere`, NOT terminal, in the shared
    /// classifier: single-source failover (#1174) must try another provider on a
    /// budget refusal — provider A's rate-priced voucher may not fit the remaining
    /// deposit while a cheaper provider B's does. The multi-source scheduler owns
    /// the "exhaustion is terminal for a shared pool" rule separately. This test
    /// guards against re-introducing the single-source regression.
    #[test]
    fn pool_exhausted_falls_over_in_the_shared_classifier() {
        let err = anyhow::Error::new(PoolExhausted {
            gap_start: 0,
            gap_len: 1 << 20,
        });
        assert_eq!(retry_disposition(&err), RetryDisposition::RetryElsewhere);
    }

    /// Every "try another node" refusal fails over to the next candidate.
    #[test]
    fn node_specific_refusals_fail_over() {
        for error in [
            StreamError::NotFound,
            StreamError::Overloaded,
            StreamError::BlobTooLarge,
            StreamError::InternalError,
            StreamError::EvictedSinceProbe,
            StreamError::HashBlacklisted,
        ] {
            assert_eq!(
                retry_disposition(&refusal(error.clone())),
                RetryDisposition::RetryElsewhere,
                "{error:?} must fail over to the next candidate"
            );
        }
    }

    /// A stall, transport fault, or bao/hash verification failure carries no
    /// typed sentinel; it is specific to this provider's delivery, so fail over.
    #[test]
    fn untyped_delivery_failures_fail_over() {
        let err = anyhow::anyhow!("connect failed: timed out");
        assert_eq!(retry_disposition(&err), RetryDisposition::RetryElsewhere);
    }
}
