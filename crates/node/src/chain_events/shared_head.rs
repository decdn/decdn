//! One shared, TTL-cached `eth_blockNumber` read for every chain watcher.
//!
//! Without a shared read, every tick would cost one `eth_blockNumber` *per
//! watcher* — ~7 head reads per `event_poll_interval` against a single endpoint,
//! on top of each watcher's `eth_getLogs`. [`SharedHead`] collapses them: the
//! first caller within a TTL window issues the RPC, everyone else reads the
//! cache.
//!
//! Two properties matter:
//!
//! - **Monotonicity — why a stale head is safe.** [`SharedHead`] only ever
//!   returns a head at-or-before the true head at the call instant; it can never
//!   report a head *ahead* of the chain. Every consumer of the head in
//!   `resumable_watcher` clamps *against* it (`resolve_persisted_start`,
//!   `resolve_head_window_start`, and the `from > to` idle branch), so a head
//!   that lags only ever *widens* an already-idempotent rescan or defers work to
//!   the next tick — it can never open a gap. Note this is a property of the
//!   cache direction, not a coincidence: a cache that could run *ahead* would
//!   skip blocks, which is exactly the #751/#762 hazard (anchoring a durable
//!   checkpoint floor too high). The cost is bounded observation latency (≤ TTL),
//!   not correctness.
//! - **Single-flight, including failures.** The mutex is held across the RPC, so
//!   concurrent callers queue and then read the fresh entry rather than each
//!   issuing their own read. Failures are cached for the same TTL, which is what
//!   keeps a wedged provider from serializing timeouts: without it, N watchers
//!   queued behind a stalled endpoint would each wait the full
//!   `chain_events::DEFAULT_RPC_CALL_TIMEOUT` in turn, so the last one's tick — and
//!   therefore its `*_backoff_started` gauge — would be delayed by N × 10s. With
//!   it, one caller pays the timeout and the rest fail instantly, so every
//!   watcher enters backoff at the same moment.

use std::sync::Arc;
use std::time::Duration;

use alloy::eips::BlockId;
use alloy::primitives::Address;
use alloy::providers::Provider;
use anyhow::Result;
use async_trait::async_trait;
use tokio::time::Instant;
use tracing::info;

use super::timed;

/// A source of the current chain head block number.
///
/// Implementations MAY serve from a short TTL cache. The returned value is always
/// at-or-before the true head at the call instant and never ahead of it — callers
/// may rely on that direction (see the module doc), but MUST NOT assume the value
/// is exactly current. A caller needing the head for a *deadline* decision ("has
/// block X been reached yet?") rather than a scan upper bound would be delayed by
/// up to the TTL; no watcher does that today.
///
/// `Err` keeps the provider's typed cause in its chain. A watcher tick treats
/// every `Err` as retryable and fails into backoff; a boot read classifies it
/// (`boot_retry::is_permanent_boot_error`), so a deterministic failure such as a
/// rejected API key fails boot at once.
#[async_trait]
pub trait HeadSource: Send + Sync {
    /// The current chain head, at or before the true head and never ahead
    /// of it. `Err` keeps its typed cause in its chain.
    async fn head(&self) -> Result<u64>;
}

/// How far below the reported head an enumeration snapshot pins its reads.
///
/// A load-balanced RPC reports the head of its freshest upstream, then routes
/// each call to any upstream. An `eth_call` pinned to that exact block fails on
/// every upstream that has not reached it yet, while an older block resolves on
/// all of them. So the reported head is not a block every upstream can serve.
/// 256 blocks is about 64s on Arbitrum, above the 40–200 block upstream lag seen
/// behind a hosted balancer (#2164).
///
/// The margin is a block count, sized for Arbitrum's ~250ms blocks and a
/// provider that serves state that far back. A full node that prunes state
/// within 256 blocks (geth keeps 128) answers every pinned read with a
/// transient error, so a boot against one spends its retry budget.
///
/// The margin costs no correctness. A boot enumeration seeds its tail cursor at
/// the snapshot block and every sink is idempotent, so the first tail tick
/// replays `[snapshot, head]` as a harmless overlap. A periodic re-enumeration
/// keeps every entry the tail changed after the snapshot block (see each
/// watcher's fold).
pub const SNAPSHOT_LAG_MARGIN_BLOCKS: u64 = 256;

/// The block an enumeration of `contract` pins its reads to:
/// [`SNAPSHOT_LAG_MARGIN_BLOCKS`] below the head.
///
/// A contract with no code at that block was deployed inside the margin — a
/// fresh local chain, or a boot right after a deploy. Reads pinned there would
/// decode an empty return, so the snapshot takes the head instead. That head pin
/// is exposed to upstream lag again; a failed read retries, and the lagged pin
/// returns once the chain is `SNAPSHOT_LAG_MARGIN_BLOCKS` past the deploy.
pub(crate) async fn snapshot_block<P: Provider>(
    provider: &P,
    head: &dyn HeadSource,
    contract: Address,
) -> Result<u64> {
    let head = head.head().await?;
    let lagged = head.saturating_sub(SNAPSHOT_LAG_MARGIN_BLOCKS);
    let code = timed(
        None,
        "eth_getCode",
        provider
            .get_code_at(contract)
            .block_id(BlockId::number(lagged)),
    )
    .await?;
    if code.is_empty() {
        info!(
            %contract,
            lagged,
            head,
            "no contract code at the lagged snapshot block; pinning the snapshot at head"
        );
        return Ok(head);
    }
    Ok(lagged)
}

/// A cached head read plus the instant it was taken.
struct CachedHead {
    at: Instant,
    /// `Arc` because `anyhow::Error` is not `Clone` and one failed read is
    /// replayed to every caller that arrives within the TTL.
    result: std::result::Result<u64, Arc<anyhow::Error>>,
}

/// A failed head read, shared by every caller inside the TTL.
///
/// The failure is kept whole behind an `Arc` (`anyhow::Error` is not `Clone`)
/// and exposed as this error's [`source`](std::error::Error::source), so a
/// caller's `err.chain()` still reaches the typed cause — the provider's
/// `TransportError` — and can classify it.
#[derive(Debug)]
struct HeadReadFailed(Arc<anyhow::Error>);

impl std::fmt::Display for HeadReadFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("chain head read failed")
    }
}

impl std::error::Error for HeadReadFailed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&**self.0)
    }
}

/// TTL-cached, single-flight [`HeadSource`] over one provider.
pub struct SharedHead<P> {
    provider: P,
    ttl: Duration,
    rpc_call_timeout: Option<Duration>,
    cached: tokio::sync::Mutex<Option<CachedHead>>,
}

/// Hand-written rather than derived: `P` is a provider and carries no `Debug`
/// bound, and the cached entry sits behind an async mutex this must not block on.
impl<P> std::fmt::Debug for SharedHead<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedHead")
            .field("ttl", &self.ttl)
            .field("rpc_call_timeout", &self.rpc_call_timeout)
            .finish_non_exhaustive()
    }
}

impl<P: Provider> SharedHead<P> {
    /// Cache head reads for half the watcher poll interval.
    ///
    /// Half, rather than a full interval, because the watchers are not
    /// phase-aligned: they bootstrap sequentially and their ticks drift across
    /// the interval, so it is the TTL — not the single-flight — that collapses
    /// the reads. A full-interval TTL would let a watcher act on a head an entire
    /// tick old, silently stretching the effective cadence of a documented knob
    /// toward 2×; half an interval bounds the added staleness below one tick for
    /// one extra RPC per interval. It also self-scales: an operator who drops
    /// `event_poll_interval_ms` to 250 ms for a local anvil gets a 125 ms TTL and
    /// a near-live head, with no second knob to tune.
    pub fn new(provider: P, poll_interval: Duration) -> Self {
        Self::with_ttl(provider, poll_interval / 2, None)
    }

    /// [`Self::new`] with an explicit TTL and per-call timeout. `Duration::ZERO`
    /// disables caching (every call re-reads).
    pub fn with_ttl(provider: P, ttl: Duration, rpc_call_timeout: Option<Duration>) -> Self {
        Self {
            provider,
            ttl,
            rpc_call_timeout,
            cached: tokio::sync::Mutex::new(None),
        }
    }
}

#[async_trait]
impl<P: Provider> HeadSource for SharedHead<P> {
    async fn head(&self) -> Result<u64> {
        // Held across the RPC: that IS the single-flight. A caller that queues
        // here finds the entry already refreshed and returns it, rather than
        // issuing a duplicate read.
        let mut guard = self.cached.lock().await;
        if let Some(entry) = guard.as_ref()
            && entry.at.elapsed() < self.ttl
        {
            return match &entry.result {
                Ok(head) => Ok(*head),
                // Every caller inside the TTL gets the same failure, typed
                // cause included, and renders the same text the first did.
                Err(err) => Err(HeadReadFailed(Arc::clone(err)).into()),
            };
        }
        let result = timed(
            self.rpc_call_timeout,
            "get_block_number",
            self.provider.get_block_number(),
        )
        .await;
        match result {
            Ok(head) => {
                *guard = Some(CachedHead {
                    at: Instant::now(),
                    result: Ok(head),
                });
                Ok(head)
            }
            Err(err) => {
                let shared = Arc::new(err);
                *guard = Some(CachedHead {
                    at: Instant::now(),
                    result: Err(Arc::clone(&shared)),
                });
                Err(HeadReadFailed(shared).into())
            }
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
mod tests;
