//! A provider layer that marks a failure of the send request itself, so a
//! caller can tell it from a failure of the chain reads before it.
//!
//! alloy's `FillProvider` fills a transaction (`eth_getTransactionCount`,
//! `eth_estimateGas`, the fee reads, `eth_chainId`), signs it, and hands the
//! envelope to the provider below it. [`SendLegLayer`] sits there, between
//! the fillers and the root provider, so the one request it sees is the send
//! (`eth_sendRawTransaction`; `eth_sendTransaction` on a wallet-free
//! provider). It wraps a failure of that request in [`SendLegFailure`]. A
//! failure that reaches the caller unwrapped happened before the send: a chain
//! read, the signing, a local fault. Nothing was broadcast.
//!
//! A JSON-RPC error *response* to the send passes through unwrapped. The node
//! answered, and the callers read that shape: `already known`, a nonce
//! rejection, a revert, an oversized batch.
//!
//! [`super::send_broadcast_unknown`] is the reader. Every provider that sends
//! installs the layer: `ProviderBuilder::new().layer(SendLegLayer)`. The
//! fillers always sit above it, whatever the order of the builder calls; a
//! layer added after it sits below it, so such a layer must make no chain
//! read in its send hook. A provider without the layer reports every
//! transport failure of a send as "nothing sent", which lets a caller re-send
//! a transaction the node may hold; the builders in `decdn-client`, the node
//! and `sponsord-core` each carry it, and a test per builder guards that.
//!
//! The layer marks every send entry point alloy's `FillProvider` forwards:
//! `send_transaction` (the `CallBuilder::send()` path), `send_transaction_sync`,
//! `send_raw_transaction`, `send_raw_transaction_sync` and
//! `send_raw_transaction_conditional`.

use std::error::Error as StdError;
use std::fmt;
use std::marker::PhantomData;

use alloy::contract::Error as ContractError;
use alloy::network::{Ethereum, Network};
use alloy::providers::{
    PendingTransactionBuilder, Provider, ProviderLayer, RootProvider, SendableTx,
};
use alloy::rpc::types::erc4337::TransactionConditional;
use alloy::transports::{RpcError, TransportError, TransportErrorKind, TransportResult};
use async_trait::async_trait;

/// The layer: `ProviderBuilder::new().layer(SendLegLayer)`. It wraps the
/// provider below the fillers in a [`SendLegProvider`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SendLegLayer;

impl<P, N> ProviderLayer<P, N> for SendLegLayer
where
    P: Provider<N>,
    N: Network,
{
    type Provider = SendLegProvider<P, N>;

    fn layer(&self, inner: P) -> Self::Provider {
        SendLegProvider {
            inner,
            _network: PhantomData,
        }
    }
}

/// The provider [`SendLegLayer`] builds. `root()` and the send hooks go to
/// the provider it wraps, and a failed send comes back as a
/// [`SendLegFailure`]; every other call uses the root, as alloy's own layers
/// do.
#[derive(Clone, Debug)]
pub struct SendLegProvider<P, N = Ethereum> {
    inner: P,
    _network: PhantomData<N>,
}

#[async_trait]
impl<P, N> Provider<N> for SendLegProvider<P, N>
where
    P: Provider<N>,
    N: Network,
{
    #[inline]
    fn root(&self) -> &RootProvider<N> {
        self.inner.root()
    }

    // The hook alloy gives a layer: `FillProvider` calls it with the filled
    // and signed transaction, and nothing else reaches it.
    async fn send_transaction_internal(
        &self,
        tx: SendableTx<N>,
    ) -> TransportResult<PendingTransactionBuilder<N>> {
        self.inner
            .send_transaction_internal(tx)
            .await
            .map_err(SendLegFailure::mark)
    }

    async fn send_transaction_sync_internal(
        &self,
        tx: SendableTx<N>,
    ) -> TransportResult<N::ReceiptResponse> {
        self.inner
            .send_transaction_sync_internal(tx)
            .await
            .map_err(SendLegFailure::mark)
    }

    async fn send_raw_transaction(
        &self,
        encoded_tx: &[u8],
    ) -> TransportResult<PendingTransactionBuilder<N>> {
        self.inner
            .send_raw_transaction(encoded_tx)
            .await
            .map_err(SendLegFailure::mark)
    }

    async fn send_raw_transaction_sync(
        &self,
        encoded_tx: &[u8],
    ) -> TransportResult<N::ReceiptResponse> {
        self.inner
            .send_raw_transaction_sync(encoded_tx)
            .await
            .map_err(SendLegFailure::mark)
    }

    async fn send_raw_transaction_conditional(
        &self,
        encoded_tx: &[u8],
        conditional: TransactionConditional,
    ) -> TransportResult<PendingTransactionBuilder<N>> {
        self.inner
            .send_raw_transaction_conditional(encoded_tx, conditional)
            .await
            .map_err(SendLegFailure::mark)
    }
}

/// A failure of the send request itself: the transaction was filled (and,
/// with a wallet, signed), and `eth_sendRawTransaction` (or
/// `eth_sendTransaction` on a wallet-free provider) failed without a JSON-RPC
/// answer. The node may have received it.
///
/// It prints as the failure it wraps, and its `source()` is that failure's
/// source, so the operator-facing render
/// ([`decdn_common::redact::sanitize_err_chain`], which folds a layer that
/// repeats its source) reads the same as for the unwrapped failure. A plain
/// `{err:#}` repeats the text once more, as it does for alloy's own custom
/// transport errors. A classifier that matches on `TransportErrorKind` sees
/// the marked error as `Custom` and must read [`Self::inner`].
#[derive(Debug)]
pub struct SendLegFailure(TransportError);

impl SendLegFailure {
    /// Wrap `err` unless it is a JSON-RPC error response, which passes
    /// through unchanged: the node answered, and the message says what.
    pub(crate) fn mark(err: TransportError) -> TransportError {
        if matches!(err, RpcError::ErrorResp(_)) {
            return err;
        }
        TransportErrorKind::custom(Self(err))
    }

    /// The failure of the send request.
    #[must_use]
    pub const fn inner(&self) -> &TransportError {
        &self.0
    }

    /// The marked send failure inside `err`, if that is what `err` is.
    #[must_use]
    pub fn from_error(err: &ContractError) -> Option<&Self> {
        let ContractError::TransportError(err) = err else {
            return None;
        };
        Self::from_transport_error(err)
    }

    /// The marked send failure inside `err`, for a send made on the provider
    /// directly rather than through a contract call.
    #[must_use]
    pub fn from_transport_error(err: &TransportError) -> Option<&Self> {
        let RpcError::Transport(TransportErrorKind::Custom(boxed)) = err else {
            return None;
        };
        boxed.downcast_ref::<Self>()
    }
}

impl fmt::Display for SendLegFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl StdError for SendLegFailure {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
