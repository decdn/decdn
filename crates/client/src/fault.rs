//! What a failed lane means for the acquire loop (ADR 039 § Failure handling:
//! reassign-only tail).
//!
//! A fault belongs to the command (only the human can fix it), to one item, to
//! one source's delivery, to one source's price against the pool, or to the
//! chain side of building a lane. The acquire loop acts on the class; it never
//! inspects the error further.

use decdn_protocol::client::{StreamError, VoucherRejectReason};

use crate::buyer_pool::{EscrowUntracked, TopUpUnconfirmed, WalletShortfall};
use crate::driver::{PoolExhausted, TopUpFailed};
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
/// chain. A bare rejection, which no heal took, acts as `Unfunded` or
/// `Declined` from its source ([`classify`]).
#[derive(Debug)]
pub struct HealExhausted;

impl std::fmt::Display for HealExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the lane ledger healed, but this source spent its resume budget")
    }
}

impl std::error::Error for HealExhausted {}

/// Whether `err` reports a `topUp` that may have escrowed USDC no local record
/// credits ([`EscrowUntracked`], [`TopUpUnconfirmed`]). A retry escrows again,
/// so it is fatal to the command.
fn escrow_untracked(err: &anyhow::Error) -> bool {
    err.downcast_ref::<EscrowUntracked>().is_some()
        || err.downcast_ref::<TopUpUnconfirmed>().is_some()
}

/// Whether a mid-stream voucher rejection for `reason` acts as `Unfunded`
/// from its source: the pool's deposit or the signer's capability no longer
/// covers the stream (ADR 005 §`VoucherRejected` semantics).
const fn is_funding_reason(reason: VoucherRejectReason) -> bool {
    matches!(
        reason,
        VoucherRejectReason::PoolExhausted
            | VoucherRejectReason::SpendingCapExhausted
            | VoucherRejectReason::SignerCapExhausted
            | VoucherRejectReason::CapabilityExpired
    )
}

/// The reason of the mid-stream voucher rejection `err` carries, when no
/// watermark heal took it ([`HealExhausted`] marks one that did).
fn unhealed_rejection(err: &anyhow::Error) -> Option<VoucherRejectReason> {
    if err.downcast_ref::<HealExhausted>().is_some() {
        return None;
    }
    if let Some(rejected) = err.downcast_ref::<UpstreamVoucherRejected>() {
        return Some(rejected.reason);
    }
    match err.downcast_ref::<UpstreamRefused>()?.error() {
        StreamError::VoucherRejected { reason, .. } => Some(*reason),
        StreamError::NotFound | StreamError::Declined | StreamError::Unfunded => None,
    }
}

/// The reason of an unhealed mid-stream voucher rejection in `err` that acts
/// as `Declined` from its source: one no bundle heals and that names no
/// funding reason. A dishonest node can send any reason, so it declines only
/// that node for this fetch (ADR 005 §`VoucherRejected` semantics).
#[must_use]
pub(crate) fn declining_rejection(err: &anyhow::Error) -> Option<VoucherRejectReason> {
    unhealed_rejection(err).filter(|reason| !is_funding_reason(*reason))
}

/// Classify `err` for the acquire loop. A node's refusal never ends the
/// fetch on its own (ADR 039 §Failure handling): only a local fault, or a
/// stop the candidate set reached, is fatal.
#[must_use]
pub fn classify(err: &anyhow::Error) -> Fault {
    if escrow_untracked(err) {
        return Fault::Fatal(FatalScope::Command);
    }
    if err
        .downcast_ref::<crate::source_set::NoAffordableSource>()
        .is_some()
    {
        return Fault::Fatal(FatalScope::Command);
    }
    if err
        .downcast_ref::<crate::source_set::NoSourceHasBlob>()
        .is_some()
        || err
            .downcast_ref::<crate::source_set::NoNodeWillServe>()
            .is_some()
    {
        return Fault::Fatal(FatalScope::Item);
    }
    if err.downcast_ref::<HealExhausted>().is_some()
        || err
            .downcast_ref::<crate::driver::StaleDepositView>()
            .is_some()
    {
        return Fault::Source;
    }
    if err.downcast_ref::<LocalPullFault>().is_some() || is_local_disk_fault(err) {
        return Fault::Fatal(FatalScope::Command);
    }
    if err.downcast_ref::<BlobTooLarge>().is_some() {
        return Fault::Fatal(FatalScope::Item);
    }
    // An unhealed voucher rejection acts as `Unfunded` from its source when it
    // names a funding reason, and as `Declined` otherwise.
    if let Some(reason) = unhealed_rejection(err) {
        return if is_funding_reason(reason) {
            Fault::Unaffordable
        } else {
            Fault::Source
        };
    }
    if err.downcast_ref::<PoolExhausted>().is_some()
        || err.downcast_ref::<WalletShortfall>().is_some()
    {
        return Fault::Unaffordable;
    }
    if err.downcast_ref::<TopUpFailed>().is_some() {
        return Fault::Transient;
    }
    // A lane build is chain-side: it never blames the source, and it retries
    // unless what failed it is fatal on its own (an escrow no record credits, a
    // pool that cannot pay and cannot be funded, a full disk).
    if let Some(LaneBuildFault(inner)) = err.downcast_ref::<LaneBuildFault>() {
        return match classify(inner) {
            Fault::Fatal(scope) => Fault::Fatal(scope),
            Fault::Source | Fault::Unaffordable | Fault::Transient => Fault::Transient,
        };
    }
    if let Some(refused) = err.downcast_ref::<UpstreamRefused>() {
        return match refused.error() {
            StreamError::Unfunded => Fault::Unaffordable,
            StreamError::NotFound | StreamError::Declined | StreamError::VoucherRejected { .. } => {
                Fault::Source
            }
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
        .is_some_and(|refused| matches!(refused.error(), StreamError::NotFound))
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
mod tests;
