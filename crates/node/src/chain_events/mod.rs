//! Shared chain-event log streaming for the on-chain watchers.
//!
//! Every watcher drives its log source through `resumable_watcher::run`, a
//! single `eth_getLogs` polling loop that unifies historical backfill and the
//! live tail into one resumable, cursor-persisting loop (#1092). It replaced the
//! per-watcher `watch_logs` (`eth_newFilter` + `eth_getFilterChanges`) streams,
//! which the default public Arbitrum Sepolia RPC and most keyless endpoints
//! reject with `-32601` (#1106). No `watch_logs`/`eth_newFilter` call remains on
//! the node's hot path.

pub(crate) mod backfill;
pub(crate) mod resumable_watcher;

pub(crate) use backfill::{
    MAX_BACKFILL_BLOCK_SPAN, REORG_MARGIN_BLOCKS, backfill_windows, check_backfill_range,
};

use std::time::Duration;

/// Default first backoff after a failing poll tick, doubled (bounded by
/// [`WATCHER_MAX_BACKOFF`]) on each successive failure and reset on a clean
/// tick. Shared by every watcher so the recovery cadence can't drift between
/// them (#1092); a watcher that genuinely needs a different cap overrides
/// `max_backoff` explicitly at its `WatcherConfig` site (e.g. the slash watcher).
pub(crate) const WATCHER_INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Default ceiling for the per-tick exponential backoff. See
/// [`WATCHER_INITIAL_BACKOFF`].
pub(crate) const WATCHER_MAX_BACKOFF: Duration = Duration::from_mins(1);
