//! Shared chain-event log streaming for the on-chain watchers.
//!
//! Each watcher previously installed one server-side `eth_getLogs` filter **per
//! event** (via the generated `<Event>_filter().watch()`), so a node held ~20
//! long-lived filters, each issuing its own `eth_getFilterChanges` per poll tick
//! and flooding a local anvil (#1011). `watch_contract_events` collapses those
//! into a **single multi-topic filter per contract address** (a `topic0` OR-set),
//! leaving the demux-by-event to the caller via [`alloy::rpc::types::Log::topic0`].
//!
//! The error contract matches the per-event `.watch()` streams this replaces:
//! the underlying alloy poller swallows transient transport errors and retries
//! internally, the stream ends (`None`) only when the provider is dropped, and
//! log decoding is the caller's responsibility — so a malformed log surfaces at
//! the caller's `decode_log_data`, exactly as a decode error did on the old
//! typed streams.

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use futures_util::{Stream, StreamExt};

/// Open one poller over `address` matching any `topic0` in `signatures` and
/// yield individual logs. `signatures` is the set of `SolEvent::SIGNATURE_HASH`
/// values for the events the watcher cares about; demux each yielded log by its
/// [`Log::topic0`] and decode with the matching event's `decode_log_data`.
pub(crate) async fn watch_contract_events<P: Provider>(
    provider: &P,
    address: Address,
    signatures: impl IntoIterator<Item = B256>,
) -> anyhow::Result<impl Stream<Item = Log> + Unpin> {
    let filter = Filter::new()
        .address(address)
        .event_signature(signatures.into_iter().collect::<Vec<_>>());
    Ok(provider
        .watch_logs(&filter)
        .await?
        .into_stream()
        .flat_map(futures_util::stream::iter))
}
