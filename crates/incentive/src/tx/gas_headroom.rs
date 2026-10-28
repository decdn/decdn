//! A gas filler that sends every estimated transaction with a fixed gas
//! headroom above the estimate.
//!
//! `eth_estimateGas` prices a call at the pending block's timestamp. The
//! transaction runs at the timestamp of the block it mines in, and a call
//! whose storage writes depend on `block.timestamp` can take a costlier path
//! there. `CapacityBond.declareMbps` and `deregisterNode` append a
//! `Checkpoints.Trace208` entry keyed by the timestamp. In the second of the
//! operator's last checkpoint the entry is overwritten in place. In any later
//! second the call pushes a fresh slot, which costs about 22k gas more. So an
//! estimate taken in that second prices the overwrite, and the transaction
//! runs out of gas if it mines in the next second (#2730). A redeem has the
//! same shape at an epoch roll: `FeeRouter` adds the delivered bytes to
//! counters keyed by `block.timestamp / epochLength`, so a redeem mined in a
//! later epoch than its estimate writes two fresh slots.
//!
//! alloy's `GasFiller` sends at the raw estimate. [`GasHeadroomFiller`] wraps
//! it and adds [`GAS_HEADROOM`] to the limit it estimated. A gas limit the
//! caller set goes out unchanged. Unused gas is refunded, so the headroom
//! costs the sender nothing beyond a larger `maxFeePerGas × gasLimit` balance
//! check.
//!
//! Every provider that sends installs it in place of alloy's gas filler, with
//! the nonce and chain-id fillers after it:
//! `ProviderBuilder::new().disable_recommended_fillers().filler(GasHeadroomFiller::default())`.
//! A provider that keeps alloy's recommended fillers carries `GasFiller` as
//! well: it takes a second estimate, and whichever filler runs last sets the
//! gas limit. The builders in `decdn-client`, the node and `sponsord-core` each
//! carry it, and a test per builder guards that.

use alloy::network::{Network, TransactionBuilder};
use alloy::providers::fillers::{FillerControlFlow, GasFiller, TxFiller};
use alloy::providers::{Provider, SendableTx};
use alloy::transports::TransportResult;

/// Gas added on top of every estimated gas limit.
///
/// It covers two fresh storage slots that the estimate's path did not write,
/// at 22,100 each for a cold zero-to-nonzero `SSTORE`, plus the 1/64 of the
/// remaining gas a call frame withholds from the call it makes. A checkpoint
/// push writes one fresh slot, and a `FeeRouter` settle in a new epoch writes
/// two.
pub const GAS_HEADROOM: u64 = 50_000;

/// alloy's `GasFiller` with [`GAS_HEADROOM`] added to every gas limit it
/// estimates. The fee fields are filled exactly as `GasFiller` fills them.
/// The headroom is [`GAS_HEADROOM`]'s, for the reasons its docs give.
#[derive(Clone, Debug, Default)]
pub struct GasHeadroomFiller {
    inner: GasFiller,
}

/// The gas limit `estimated` gets once the headroom is added. Saturates, so
/// an estimate near `u64::MAX` cannot wrap to a small limit.
#[must_use]
pub const fn with_headroom(estimated: u64) -> u64 {
    estimated.saturating_add(GAS_HEADROOM)
}

impl<N: Network> TxFiller<N> for GasHeadroomFiller {
    /// `GasFiller`'s fillable, and whether its gas limit is an estimate
    /// (`true`) rather than the caller's own (`false`).
    type Fillable = (<GasFiller as TxFiller<N>>::Fillable, bool);

    fn status(&self, tx: &N::TransactionRequest) -> FillerControlFlow {
        TxFiller::<N>::status(&self.inner, tx)
    }

    fn fill_sync(&self, _tx: &mut SendableTx<N>) {}

    async fn prepare<P: Provider<N>>(
        &self,
        provider: &P,
        tx: &N::TransactionRequest,
    ) -> TransportResult<Self::Fillable> {
        let estimated = tx.gas_limit().is_none();
        let fillable = self.inner.prepare(provider, tx).await?;
        Ok((fillable, estimated))
    }

    async fn fill(
        &self,
        (fillable, estimated): Self::Fillable,
        tx: SendableTx<N>,
    ) -> TransportResult<SendableTx<N>> {
        let mut tx = self.inner.fill(fillable, tx).await?;
        if estimated
            && let Some(builder) = tx.as_mut_builder()
            && let Some(limit) = builder.gas_limit()
        {
            builder.set_gas_limit(with_headroom(limit));
        }
        Ok(tx)
    }
}

#[cfg(test)]
mod tests;
