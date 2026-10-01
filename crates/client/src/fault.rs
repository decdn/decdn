//! What a failed lane means for the acquire loop (ADR 039 § Failure handling:
//! reassign-only tail).
//!
//! A fault belongs to the command (only the human can fix it), to one item, to
//! one source's delivery, to one source's price against the pool, or to the
//! chain side of building a lane. The acquire loop acts on the class; it never
//! inspects the error further.

use decdn_protocol::client::StreamError;

use crate::driver::PoolExhausted;
use crate::{BlobTooLarge, LocalPullFault, UpstreamRefused, UpstreamVoucherRejected};

/// What a failed lane, lane build, or discovery means for the acquire loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Only the human can fix it. The scope says what stops.
    Fatal(FatalScope),
    /// A property of this source's delivery. The source cools and its ranges
    /// move to other sources.
    Source,
    /// This source's next voucher does not fit the pool's current deposit. The
    /// source waits for the deposit to rise.
    Unaffordable,
    /// A chain or RPC fault outside any source's delivery. The loop retries
    /// with backoff and the source keeps its health.
    Transient,
}

/// What a [`Fault::Fatal`] stops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FatalScope {
    /// Every item of the command.
    Command,
    /// This item only. Other bundle entries continue.
    Item,
}

/// An error raised while building a source's lane (pool open or reuse, client
/// binding signature). It is chain-side, so it never blames the source.
#[derive(Debug)]
pub struct LaneBuildFault(pub anyhow::Error);

impl std::fmt::Display for LaneBuildFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "building the payment lane failed: {:#}", self.0)
    }
}

impl std::error::Error for LaneBuildFault {}

/// A voucher rejection whose watermark healed the lane ledger after the lane
/// spent its resume budget ([`crate::MAX_RESUME_ATTEMPTS`]). The ledger and
/// this source agree again, but the source kept rejecting after each heal, so
/// the source cools and its range moves to another source.
///
/// It is a marker on the rejection, so it composes:
/// `.context(HealExhausted)` keeps the [`UpstreamVoucherRejected`] in the
/// chain. A bare rejection, which no heal took, ends the command.
#[derive(Debug)]
pub struct HealExhausted;

impl std::fmt::Display for HealExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the lane ledger healed, but this source spent its resume budget")
    }
}

impl std::error::Error for HealExhausted {}

/// Classify `err` for the acquire loop.
#[must_use]
pub fn classify(err: &anyhow::Error) -> Fault {
    if err
        .downcast_ref::<crate::source_set::NoAffordableSource>()
        .is_some()
    {
        return Fault::Fatal(FatalScope::Command);
    }
    if err
        .downcast_ref::<crate::source_set::NoSourceHasBlob>()
        .is_some()
    {
        return Fault::Fatal(FatalScope::Item);
    }
    if err.downcast_ref::<HealExhausted>().is_some() {
        return Fault::Source;
    }
    if err.downcast_ref::<UpstreamVoucherRejected>().is_some() {
        return Fault::Fatal(FatalScope::Command);
    }
    if err.downcast_ref::<LocalPullFault>().is_some() || is_local_disk_fault(err) {
        return Fault::Fatal(FatalScope::Command);
    }
    if err.downcast_ref::<BlobTooLarge>().is_some() {
        return Fault::Fatal(FatalScope::Item);
    }
    if err.downcast_ref::<PoolExhausted>().is_some() {
        return Fault::Unaffordable;
    }
    if err.downcast_ref::<LaneBuildFault>().is_some() {
        return Fault::Transient;
    }
    if let Some(refused) = err.downcast_ref::<UpstreamRefused>() {
        return match refused.error() {
            StreamError::OriginBlacklisted | StreamError::VoucherRejected { .. } => {
                Fault::Fatal(FatalScope::Command)
            }
            StreamError::InsufficientDeposit => Fault::Unaffordable,
            _ => Fault::Source,
        };
    }
    Fault::Source
}

/// Whether `err` is a source saying it does not hold the blob. On the wire the
/// same `NotFound` also means load shed or a pool the node cannot confirm yet,
/// so a [`crate::SourceSet`] counts it against a pull-through target only
/// ([`crate::Holder::probed_holder`]).
#[must_use]
pub fn says_absent(err: &anyhow::Error) -> bool {
    err.downcast_ref::<UpstreamRefused>()
        .is_some_and(|refused| {
            matches!(
                refused.error(),
                StreamError::NotFound | StreamError::EvictedSinceProbe
            )
        })
}

/// Whether `err`'s chain holds an I/O error only this machine can fix.
fn is_local_disk_fault(err: &anyhow::Error) -> bool {
    use std::io::ErrorKind;
    err.chain()
        .filter_map(|cause| cause.downcast_ref::<std::io::Error>())
        .any(|io| {
            matches!(
                io.kind(),
                ErrorKind::PermissionDenied
                    | ErrorKind::StorageFull
                    | ErrorKind::ReadOnlyFilesystem
                    | ErrorKind::NotADirectory
                    | ErrorKind::IsADirectory
                    | ErrorKind::FileTooLarge
            )
        })
}

#[cfg(test)]
mod tests {
    use super::{FatalScope, Fault, HealExhausted, LaneBuildFault, classify};
    use crate::driver::PoolExhausted;
    use crate::{BlobTooLarge, LocalPullFault, UpstreamRefused, UpstreamVoucherRejected};
    use decdn_protocol::client::{StreamError, VoucherRejectReason};

    fn refusal(error: StreamError) -> anyhow::Error {
        anyhow::Error::new(UpstreamRefused::mid_stream(error))
    }

    fn rejected(reason: VoucherRejectReason) -> anyhow::Error {
        anyhow::Error::new(UpstreamVoucherRejected {
            reason,
            bundle: None,
            proof_generation: None,
        })
    }

    #[test]
    fn payment_and_blacklist_faults_end_the_command() {
        for reason in [
            VoucherRejectReason::SpendingCapExhausted,
            VoucherRejectReason::CapabilityExpired,
            VoucherRejectReason::BadSignature,
            VoucherRejectReason::WrongSigner,
        ] {
            assert_eq!(
                classify(&rejected(reason)),
                Fault::Fatal(FatalScope::Command),
                "{reason:?}"
            );
        }
        assert_eq!(
            classify(&refusal(StreamError::OriginBlacklisted)),
            Fault::Fatal(FatalScope::Command)
        );
        let local = anyhow::anyhow!("store write").context(LocalPullFault);
        assert_eq!(classify(&local), Fault::Fatal(FatalScope::Command));
    }

    /// A watermark rejection that healed the lane ledger after the lane spent
    /// its resume budget (#2257) is this source's, so the range moves on and
    /// the rest of the bundle continues.
    #[test]
    fn a_rejection_healed_past_the_resume_budget_is_the_sources() {
        for reason in [
            VoucherRejectReason::UnderFold,
            VoucherRejectReason::AmountRegression,
            VoucherRejectReason::Underpaid,
        ] {
            let err = rejected(reason).context(HealExhausted);
            assert_eq!(classify(&err), Fault::Source, "{reason:?}");
            assert!(
                err.downcast_ref::<UpstreamVoucherRejected>().is_some(),
                "the marker keeps the rejection in the chain"
            );
        }
    }

    /// A watermark rejection that no heal took ends the command, as ADR 005
    /// says: a `BytesRegression` is a single-signer fault, and an `Underpaid`
    /// or a trailing proof that no bundle heals has nothing to retry from.
    #[test]
    fn a_rejection_no_heal_took_ends_the_command() {
        for reason in [
            VoucherRejectReason::BytesRegression,
            VoucherRejectReason::Underpaid,
            VoucherRejectReason::UnderFold,
            VoucherRejectReason::AmountRegression,
        ] {
            assert_eq!(
                classify(&rejected(reason)),
                Fault::Fatal(FatalScope::Command),
                "{reason:?}"
            );
        }
    }

    #[test]
    fn a_full_disk_ends_the_command() {
        let io = std::io::Error::from(std::io::ErrorKind::StorageFull);
        let err = anyhow::Error::new(io).context("write .partial");
        assert_eq!(classify(&err), Fault::Fatal(FatalScope::Command));
    }

    #[test]
    fn an_over_cap_blob_ends_only_its_item() {
        let err = anyhow::Error::new(BlobTooLarge {
            received: 1 << 40,
            ceiling: 1 << 20,
        });
        assert_eq!(classify(&err), Fault::Fatal(FatalScope::Item));
    }

    #[test]
    fn a_dry_pool_marks_the_source_unaffordable() {
        let dry = anyhow::Error::new(PoolExhausted {
            gap_start: 0,
            gap_len: 1 << 20,
        });
        assert_eq!(classify(&dry), Fault::Unaffordable);
        assert_eq!(
            classify(&refusal(StreamError::InsufficientDeposit)),
            Fault::Unaffordable
        );
    }

    #[test]
    fn a_lane_build_error_is_transient() {
        let err = anyhow::Error::new(LaneBuildFault(anyhow::anyhow!("rpc timed out")));
        assert_eq!(classify(&err), Fault::Transient);
    }

    #[test]
    fn delivery_faults_are_the_sources() {
        for error in [
            StreamError::NotFound,
            StreamError::Overloaded,
            StreamError::BlobTooLarge,
            StreamError::InternalError,
            StreamError::EvictedSinceProbe,
            StreamError::HashBlacklisted,
        ] {
            assert_eq!(
                classify(&refusal(error.clone())),
                Fault::Source,
                "{error:?}"
            );
        }
        assert_eq!(
            classify(&anyhow::anyhow!("connection reset")),
            Fault::Source
        );
    }

    #[test]
    fn not_found_is_a_source_fault_that_says_absent() {
        let err = refusal(StreamError::NotFound);
        assert_eq!(classify(&err), Fault::Source);
        assert!(super::says_absent(&err));
        assert!(!super::says_absent(&refusal(StreamError::Overloaded)));
    }

    #[test]
    fn unanimous_stops_are_fatal_with_their_scope() {
        use crate::source_set::NoAffordableSource;
        let dry = anyhow::Error::new(NoAffordableSource {
            deposit: alloy::primitives::U256::ZERO,
        });
        assert_eq!(classify(&dry), Fault::Fatal(FatalScope::Command));
        let absent = anyhow::Error::new(crate::source_set::NoSourceHasBlob);
        assert_eq!(classify(&absent), Fault::Fatal(FatalScope::Item));
    }
}
