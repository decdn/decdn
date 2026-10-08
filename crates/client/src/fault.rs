//! What a failed lane means for the acquire loop (ADR 039 § Failure handling:
//! reassign-only tail).
//!
//! A fault belongs to the command (only the human can fix it), to one item, to
//! one source's delivery, to one source's price against the pool, or to the
//! chain side of building a lane. The acquire loop acts on the class; it never
//! inspects the error further.

use decdn_protocol::client::StreamError;

use crate::buyer_pool::{EscrowUntracked, TopUpUnconfirmed, WalletShortfall};
use crate::driver::{PoolExhausted, TopUpFailed};
use crate::{
    BlobTooLarge, LocalPullFault, SignerCapDrained, UpstreamRefused, UpstreamVoucherRejected,
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
    /// A chain or RPC fault outside any source's delivery. The loop retries
    /// with backoff and the source keeps its health.
    Transient,
}

impl Fault {
    /// Whether the fault is one an operator watches for, so its log line is at
    /// warn: a source failing its delivery, or a fault only the human can fix.
    /// A deposit wait ([`Fault::Unaffordable`]) and a chain-side retry
    /// ([`Fault::Transient`]) leave the source's health untouched and log at
    /// info.
    #[must_use]
    pub(crate) const fn warns(self) -> bool {
        matches!(self, Self::Source | Self::Fatal(_))
    }
}

/// Emit one `tracing` event at warn when `$warn` holds and at info otherwise.
/// A `tracing` level must be a constant, so the choice is two calls that
/// share one field list.
macro_rules! warn_or_info {
    ($warn:expr, $($event:tt)+) => {
        if $warn {
            tracing::warn!($($event)+);
        } else {
            tracing::info!($($event)+);
        }
    };
}
pub(crate) use warn_or_info;

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

/// Whether `err` reports a `topUp` that may have escrowed USDC no local record
/// credits ([`EscrowUntracked`], [`TopUpUnconfirmed`]). A retry escrows again,
/// so it is fatal to the command.
fn escrow_untracked(err: &anyhow::Error) -> bool {
    err.downcast_ref::<EscrowUntracked>().is_some()
        || err.downcast_ref::<TopUpUnconfirmed>().is_some()
}

/// Classify `err` for the acquire loop.
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
            .downcast_ref::<crate::source_set::NoSourceServesSigner>()
            .is_some()
    {
        return Fault::Fatal(FatalScope::Item);
    }
    // A drained capability signer: every provider refuses it once its
    // registration expired or nothing is left of its cap. Otherwise a
    // cheaper provider can still serve it, so only this source is barred.
    if let Some(drained) = err.downcast_ref::<SignerCapDrained>() {
        return if drained.at_every_rate() {
            Fault::Fatal(FatalScope::Command)
        } else {
            Fault::Source
        };
    }
    if err.downcast_ref::<HealExhausted>().is_some()
        || err
            .downcast_ref::<crate::driver::StaleDepositView>()
            .is_some()
    {
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
mod tests;
