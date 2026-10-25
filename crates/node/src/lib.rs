//! The library behind the `decdn-node` daemon: a deCDN node that caches
//! content-addressed blobs and serves them over iroh QUIC for per-megabyte USDC
//! payment.
//!
//! The `decdn-node` binary is a thin shell. It parses `--config` and a single
//! `run` subcommand, and calls [`commands::run`]. This library exists so the
//! daemon's integration tests can drive its parts. It is not an SDK, and its
//! API carries no stability promise. To fetch from deCDN, use
//! [`decdn-client`]. The user-facing `decdn` CLI is the [`decdn-cli`] crate.
//!
//! # Modules
//!
//! | Area | Modules |
//! |---|---|
//! | Bring-up | [`commands`] (the entry point), [`runtime`] (the iroh endpoint, the protocol router, and the background tasks) |
//! | Serving | [`handlers`] (one handler per ALPN), [`dispatch`] and [`rate_limit`] (per-source limits), [`load_shed`] (overload shedding), [`serve_economics`] and [`warming_allowance`] (when a cache-miss relay pays) |
//! | Cache misses | [`node_origin`] (the paid node-to-node pull-through origin), [`selection`] (provider choice), [`dht`] (the content DHT) |
//! | Payment | [`payment_settlement`] (seller-side redemption), [`buyer_pool`] and [`buyer_ledgers`] (buyer-side pools and lanes), [`pool_store`] and [`pool_view`] (lane state), [`onchain_tx`] (transaction submission) |
//! | Chain watchers | [`chain_events`], [`blacklist_watcher`], [`slash_watcher`], [`fee_shares_watcher`], [`chain_freshness`], [`binding_check`] |
//! | Compliance | [`content_deny`] (the origin deny-set) |
//! | Operations | [`admin`] (the loopback admin JSON-RPC server, [Local Admin HTTP]), [`metrics`], [`receipt_log`], [`systemd_watchdog`] |
//!
//! Wire types live in [`decdn_protocol`]. The config schema and admin RPC types
//! that the daemon shares with the CLI live in [`decdn_common`].
//!
//! [`decdn-client`]: https://docs.rs/decdn-client
//! [`decdn-cli`]: https://docs.rs/decdn-cli
//! [Local Admin HTTP]: https://github.com/decdn/decdn/blob/main/adr/appendix-local-admin-http.md

pub mod admin;
pub mod binding_check;
pub mod blacklist_watcher;
pub mod buyer_ledgers;
pub mod buyer_pool;
pub mod chain_events;
pub mod chain_freshness;
pub mod coarse_clock;
pub mod commands;
pub mod content_deny;
pub mod dht;
pub mod dispatch;
pub mod fee_shares;
pub mod fee_shares_watcher;
pub mod handlers;
pub mod load_shed;
pub mod metrics;
pub(crate) mod net;
pub mod node_origin;
pub mod onchain_tx;
pub mod payment_settlement;
pub mod pool_store;
pub mod pool_view;
pub(crate) mod prune_guard;
pub mod rate_limit;
pub mod receipt_log;
pub(crate) mod rpc_metrics;
pub mod runtime;
pub mod selection;
pub mod serve_economics;
pub mod settlement_ledger;
pub mod slash_watcher;
pub mod stop_handle;
pub mod systemd_watchdog;
pub mod warming_allowance;
pub(crate) mod warn_throttle;
