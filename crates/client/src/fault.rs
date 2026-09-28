//! What a failed lane means for the acquire loop (spec § Fault classes).
//!
//! A fault belongs to the command (only the human can fix it), to one item, to
//! one source's delivery, to one source's price against the pool, to one
//! source's view of the blob size, or to the chain side of building a lane.
//! The acquire loop acts on the class; it never inspects the error further.

use decdn_protocol::client::StreamError;

use crate::driver::PoolExhausted;
use crate::{
    BlobTooLarge, LocalPullFault, SignedSizeMismatch, UpstreamRefused, UpstreamVoucherRejected,
};

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
    /// This source signs a size other than the one the store is keyed by. The
    /// source is excluded for this item.
    WrongSize,
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

/// Classify `err` for the acquire loop.
#[must_use]
pub fn classify(err: &anyhow::Error) -> Fault {
    if err.downcast_ref::<UpstreamVoucherRejected>().is_some()
        || err.downcast_ref::<LocalPullFault>().is_some()
        || is_local_disk_fault(err)
    {
        return Fault::Fatal(FatalScope::Command);
    }
    if err.downcast_ref::<BlobTooLarge>().is_some() {
        return Fault::Fatal(FatalScope::Item);
    }
    if err.downcast_ref::<PoolExhausted>().is_some() {
        return Fault::Unaffordable;
    }
    if err.downcast_ref::<SignedSizeMismatch>().is_some() {
        return Fault::WrongSize;
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
    use super::{FatalScope, Fault, LaneBuildFault, classify};
    use crate::driver::PoolExhausted;
    use crate::{
        BlobTooLarge, LocalPullFault, SignedSizeMismatch, UpstreamRefused, UpstreamVoucherRejected,
    };
    use decdn_protocol::client::{StreamError, VoucherRejectReason};

    fn refusal(error: StreamError) -> anyhow::Error {
        anyhow::Error::new(UpstreamRefused::mid_stream(error))
    }

    #[test]
    fn payment_and_blacklist_faults_end_the_command() {
        let rejected = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::AmountRegression,
            bundle: None,
            proof_generation: None,
        });
        assert_eq!(classify(&rejected), Fault::Fatal(FatalScope::Command));
        assert_eq!(
            classify(&refusal(StreamError::OriginBlacklisted)),
            Fault::Fatal(FatalScope::Command)
        );
        let local = anyhow::anyhow!("store write").context(LocalPullFault);
        assert_eq!(classify(&local), Fault::Fatal(FatalScope::Command));
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
    fn a_signed_size_disagreement_is_wrong_size() {
        let err = anyhow::Error::new(SignedSizeMismatch {
            signed: 10,
            expected: 11,
        });
        assert_eq!(classify(&err), Fault::WrongSize);
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
}
