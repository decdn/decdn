//! Shared chain-event log streaming for the on-chain watchers.
//!
//! Every watcher drives its log source through `resumable_watcher::run`, a
//! single `eth_getLogs` polling loop that unifies historical backfill and the
//! live tail into one resumable, cursor-persisting loop (#1092). It replaced the
//! per-watcher `watch_logs` (`eth_newFilter` + `eth_getFilterChanges`) streams,
//! which the default public Arbitrum Sepolia RPC and most keyless endpoints
//! reject with `-32601` (#1106). No `watch_logs`/`eth_newFilter` call remains on
//! the node's hot path.

pub(crate) mod resumable_watcher;
