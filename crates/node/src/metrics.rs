//! OpenMetrics/Prometheus metrics and a minimal `/metrics` HTTP server.
//!
//! Metrics live in an [`iroh_metrics::Registry`] so we can surface both our
//! `decdn_*` counters and iroh's own transport metrics through a single
//! endpoint. Output is `OpenMetrics` text, which Prometheus scrapers accept.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use bytes::Bytes;
use decdn_cache::CacheMetrics;
use decdn_incentive::ChannelOpenFailureReason;
use http_body_util::Full;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use iroh::Endpoint;
use iroh_metrics::{Counter, Gauge, MetricsGroup, MetricsSource, Registry};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, oneshot};

/// Cap concurrent `/metrics` connections. Prevents a trivial `DoS` where a
/// peer opens many sockets to the operational-data endpoint and exhausts
/// tasks.
const MAX_METRICS_CONNECTIONS: usize = 32;

/// deCDN-specific counters and gauges surfaced at `/metrics`.
///
/// The group name becomes the metric-name prefix, so fields appear as
/// e.g. `decdn_probe_requests_total`.
#[derive(Debug, Default, Serialize, Deserialize, MetricsGroup)]
#[metrics(name = "decdn")]
pub struct DecdnMetrics {
    /// Total probe requests served.
    pub probe_requests: Counter,
    /// Currently open QUIC connections.
    pub active_connections: Gauge,
    /// Seconds since node start.
    pub uptime_seconds: Gauge,
    /// `NodeAnnounce` messages published to any gossip topic.
    pub gossip_announces_published_total: Counter,
    /// `NodeAnnounce`-bearing gossip envelopes received on any topic.
    pub gossip_announces_received_total: Counter,
    /// Incoming gossip envelopes rejected by validation (any reason).
    pub gossip_announces_rejected_total: Counter,
    /// Current peer-table size.
    pub gossip_peer_table_size: Gauge,
    /// Successful subscriber reconnections after a stream drop.
    pub gossip_subscriber_reconnections_total: Counter,
    /// JSON-RPC endpoint reachability per the watchdog task. `1` =
    /// reachable, `0` = unreachable. ADR 020 names operational gauges in
    /// the `decdn_*` family; this is the per-tick mirror of the startup
    /// `check_rpc_reachability` probe so dashboards/alerts can fire on
    /// a sustained outage rather than relying on a one-shot startup line.
    pub rpc_healthy: Gauge,
    /// Connections rejected because the global concurrency semaphore was
    /// exhausted. Field has no `_total` suffix because the `OpenMetrics`
    /// encoder appends it automatically; the operator-visible name is
    /// `decdn_dispatch_rejected_global_total`.
    pub dispatch_rejected_global: Counter,
    /// Connections rejected by the per-source rate limiter. Operator-
    /// visible name: `decdn_dispatch_rejected_per_source_total`.
    pub dispatch_rejected_per_source: Counter,
    /// Currently in-flight QUIC handler tasks holding a dispatch permit.
    pub dispatch_in_flight: Gauge,
    /// Connections accepted on a relay-only path (no resolvable peer
    /// IP) while the per-source layer was enabled. The per-source rate
    /// limit cannot be enforced for these — operators chasing
    /// `dispatch_rejected_per_source` anomalies need this counter to
    /// distinguish "the layer didn't fire" from "the layer wasn't
    /// applicable." Operator-visible name:
    /// `decdn_dispatch_per_source_skipped_no_addr_total`.
    pub dispatch_per_source_skipped_no_addr: Counter,
    /// QUIC 0-RTT connection attempts on `cdn/probe/v1` — a cached
    /// session ticket existed and early data was sent (ADR 015
    /// §Observability). Operator-visible name:
    /// `decdn_quic_0rtt_attempts_total`.
    pub quic_0rtt_attempts: Counter,
    /// 0-RTT attempts the server accepted (early data processed without a
    /// full handshake). Operator-visible name:
    /// `decdn_quic_0rtt_accepted_total`.
    pub quic_0rtt_accepted: Counter,
    /// 0-RTT attempts the server rejected; the client fell back to a
    /// 1-RTT handshake and re-sent the request. Operator-visible name:
    /// `decdn_quic_0rtt_rejected_total`.
    pub quic_0rtt_rejected: Counter,
    /// Approximate 0-RTT working-set size (ADR 015 §Observability).
    /// rustls owns the real session stores and exposes no size API, so
    /// this is a *proxy*: the number of distinct remote endpoints that
    /// completed a probe handshake on the 0-RTT-enabled server path —
    /// cold clients included, since the server still issues a
    /// `NewSessionTicket` to each. It is **not** a mirror of any specific
    /// rustls cache: server-side resumption state lives in rustls's
    /// internal, default-sized server store (iroh's `max_tls_tickets`
    /// knob sizes only the *client* `ClientSessionMemoryCache`). The
    /// value saturates at `SESSION_TICKET_CACHE_CEILING` because the
    /// backing set is bounded there for memory safety, not because it
    /// tracks a cache of that size — close enough at deployment scales
    /// where the cap is rarely hit organically.
    pub quic_session_ticket_cache_size: Gauge,
    /// Distinct *new* peers dropped from the tracking set because it hit
    /// `SESSION_TICKET_CACHE_CEILING`. Zero under organic load at
    /// expected deployment scales; a rising value means the
    /// unauthenticated probe handler is being fed many distinct node ids
    /// — i.e. it distinguishes a Sybil-style saturation from the gauge
    /// legitimately reaching the ceiling. Operator-visible name:
    /// `decdn_quic_session_ticket_peers_dropped_total`.
    pub quic_session_ticket_peers_dropped: Counter,
    /// `decdn_probe_hold_violations_total` per the canonical metric registry
    /// (`adr/appendix-observability.md` — the authoritative naming source,
    /// superseding informal ADR-005 references). The registry's alert
    /// remediation for this counter is "reduce load or increase
    /// `max_probe_holds`", i.e. it is the budget-pressure signal: this code
    /// increments it when the blob is present but the
    /// [`crate::handlers::probe`] hold could not be guaranteed (budget
    /// exhausted), so the node answers `has_blob: false`. That is an
    /// availability degradation, never a safety fault — the node loses
    /// revenue but never signs a phantom announcement (the hold mechanism
    /// makes the registry's literal "evicted after signing `has_blob:true`"
    /// case unreachable by construction, so this counter surfaces the
    /// budget-pressure cause the operator can actually act on).
    pub probe_hold_violations: Counter,
    /// `decdn_probe_holds_disabled_total` (#739): probes answered
    /// `has_blob: false` for a *present* blob because the eviction-hold path
    /// is **disabled by config** (`max_probe_holds == 0`), as opposed to
    /// genuine slot exhaustion. Split out from `probe_hold_violations` so an
    /// intentional operator disable does not trip that counter's "increase
    /// `max_probe_holds`" alert — a nonsensical remedy when holds are
    /// deliberately off. Field has no `_total` suffix because the
    /// `OpenMetrics` encoder appends it.
    pub probe_holds_disabled: Counter,
    /// `decdn_probe_stake_lane_reserved_total` (#757): an end-client probe
    /// (a requester that is *not* a registered operator) answered
    /// `has_blob: false` because the hold budget had reached the end-client
    /// ceiling (`max_probe_holds - cache.stake_lane_reserved_holds`),
    /// reserving the remaining slots for stake-lane (node-to-node
    /// cache-miss) probes per ADR 003 §Admission and Priority. Unlike the
    /// two counters above, this fires *before* `try_probe_hold`, so the
    /// cache is **not consulted** — the blob may or may not be present; the
    /// reservation is a content-independent admission decision. Distinct
    /// from `probe_hold_violations` (genuine exhaustion of the *whole*
    /// budget, checked after a confirmed-present blob) and
    /// `probe_holds_disabled` (`max_probe_holds == 0`): this is a deliberate
    /// priority decision, not budget pressure or a disable, so it must not
    /// trip either of those counters' alerts. Zero whenever the reservation
    /// is unconfigured (`stake_lane_reserved_holds == 0`). Field has no
    /// `_total` suffix because the `OpenMetrics` encoder appends it.
    pub probe_stake_lane_reserved: Counter,
    /// `decdn_probe_hold_slots_used` (registry): current active
    /// probe-triggered eviction holds (distinct held blobs), ADR 005
    /// §Probe-triggered eviction hold. Sampled from the cache engine on
    /// each probe; pair with `probe_hold_slots_max` for a saturation ratio.
    pub probe_hold_slots_used: Gauge,
    /// `decdn_probe_hold_slots_max` (registry, mandatory): the configured
    /// `max_probe_holds` budget. Set once at startup. Pairs with
    /// `probe_hold_slots_used` so dashboards can alert on a saturation
    /// ratio rather than an absolute count.
    pub probe_hold_slots_max: Gauge,
    /// Times the node clamped `rate_per_mb` to the configured delivery
    /// bounds before signing a `ProbeResponse` (ADR 005 §Rate bounds
    /// validation). Operator-visible name:
    /// `decdn_rate_bounds_clamp_events_total`.
    pub rate_bounds_clamp_events: Counter,
    /// Vouchers refused because their cumulative `amount / bytes_delivered`
    /// watermark fell below the configured per-byte price floor (`delivery_floor`)
    /// at zero tolerance, mirroring the on-chain `PaymentChannel`
    /// `RateFloorViolation` settlement check (which likewise floors the cumulative
    /// claim, #846). Only increments when `delivery_floor > 0` (i.e. the node has
    /// synced on-chain bounds); at the default floor of `0` the check is inert and
    /// this stays zero regardless of voucher quality. A non-zero count means a
    /// counterparty signed a voucher this node could not redeem on-chain.
    /// Operator-visible name: `decdn_voucher_rate_floor_rejections_total`.
    pub voucher_rate_floor_rejections: Counter,
    /// `cdn/dht/v1` requests rejected by the per-peer (`NodeId`) token
    /// bucket (ADR 022 §DHT Rate Limiting). One Counter per layer to match
    /// the existing `dispatch_rejected_*` convention since the metrics
    /// backend doesn't support per-field labels. Operator-visible name:
    /// `decdn_dht_rate_limit_rejected_per_peer_total`.
    pub dht_rate_limit_rejected_per_peer: Counter,
    /// `cdn/dht/v1` requests rejected by the per-IP token bucket. Sibling
    /// to `dht_rate_limit_rejected_per_peer` — see its docs. Operator-
    /// visible name: `decdn_dht_rate_limit_rejected_per_ip_total`.
    pub dht_rate_limit_rejected_per_ip: Counter,
    /// `cdn/dht/v1` requests rejected by the global token bucket. Sibling
    /// to `dht_rate_limit_rejected_per_peer` — see its docs. Operator-
    /// visible name: `decdn_dht_rate_limit_rejected_global_total`.
    pub dht_rate_limit_rejected_global: Counter,
    /// `retain_recent` sweeps of the per-IP DHT keyed-limiter map that
    /// actually ran (single-flight CAS won, layer enabled). Bump from
    /// both the lazy-prune path in `DhtRateLimiter::check` and the
    /// periodic `gc_per_ip` GC task. Persistent growth without a matching
    /// drop in `decdn_dht_rate_limit_tracked_per_ip` means the limiter
    /// is observing churn faster than `retain_recent` can release entries
    /// — investigate cap saturation. Operator-visible name:
    /// `decdn_dht_rate_limit_prune_sweeps_per_ip_total` (#645).
    pub dht_rate_limit_prune_sweeps_per_ip: Counter,
    /// Sibling of `dht_rate_limit_prune_sweeps_per_ip` for the per-peer
    /// (`NodeId`) keyed-limiter map. Operator-visible name:
    /// `decdn_dht_rate_limit_prune_sweeps_per_peer_total` (#645).
    pub dht_rate_limit_prune_sweeps_per_peer: Counter,
    /// Current size of the per-IP DHT keyed-limiter map after the most
    /// recent prune (lazy or periodic). Pair with
    /// `decdn_dht_rate_limit_prune_sweeps_per_ip_total` to detect cap
    /// saturation. Updated post-prune so the value is at most one
    /// `DHT_RATE_LIMIT_GC_INTERVAL` stale on quiet nodes; reads `0`
    /// until the first successful prune (lazy or periodic) after
    /// startup. Operator-visible name:
    /// `decdn_dht_rate_limit_tracked_per_ip` (#645).
    pub dht_rate_limit_tracked_per_ip: Gauge,
    /// Sibling of `dht_rate_limit_tracked_per_ip` for the per-peer map.
    /// Operator-visible name: `decdn_dht_rate_limit_tracked_per_peer` (#645).
    pub dht_rate_limit_tracked_per_peer: Gauge,
    /// `cdn/probe/v1` requests rejected by the per-peer (`NodeId`) token
    /// bucket (ADR 005 §Probe rate limiting). One Counter per layer to match
    /// the existing `dht_rate_limit_rejected_*` / `dispatch_rejected_*`
    /// convention since the metrics backend doesn't support per-field labels;
    /// operators recover the rolled-up rate with
    /// `sum(rate(decdn_probe_rate_limit_rejected_{per_peer,per_ip,global}_total[1m]))`.
    /// Operator-visible name: `decdn_probe_rate_limit_rejected_per_peer_total`.
    pub probe_rate_limit_rejected_per_peer: Counter,
    /// `cdn/probe/v1` requests rejected by the per-IP token bucket. Sibling
    /// to `probe_rate_limit_rejected_per_peer` — see its docs. Operator-
    /// visible name: `decdn_probe_rate_limit_rejected_per_ip_total`.
    pub probe_rate_limit_rejected_per_ip: Counter,
    /// `cdn/probe/v1` requests rejected by the global token bucket. Sibling
    /// to `probe_rate_limit_rejected_per_peer` — see its docs. Operator-
    /// visible name: `decdn_probe_rate_limit_rejected_global_total`.
    pub probe_rate_limit_rejected_global: Counter,
    /// `retain_recent` sweeps of the per-IP probe keyed-limiter map that
    /// actually ran (single-flight CAS won, layer enabled). Bumped from both
    /// the lazy-prune path in the limiter's `check` and the periodic
    /// `gc_per_ip` GC task. Operator-visible name:
    /// `decdn_probe_rate_limit_prune_sweeps_per_ip_total` (#645).
    pub probe_rate_limit_prune_sweeps_per_ip: Counter,
    /// Sibling of `probe_rate_limit_prune_sweeps_per_ip` for the per-peer
    /// (`NodeId`) keyed-limiter map. Operator-visible name:
    /// `decdn_probe_rate_limit_prune_sweeps_per_peer_total` (#645).
    pub probe_rate_limit_prune_sweeps_per_peer: Counter,
    /// Current size of the per-IP probe keyed-limiter map after the most
    /// recent prune (lazy or periodic). Pair with
    /// `decdn_probe_rate_limit_prune_sweeps_per_ip_total` to detect cap
    /// saturation. Operator-visible name:
    /// `decdn_probe_rate_limit_tracked_per_ip` (#645).
    pub probe_rate_limit_tracked_per_ip: Gauge,
    /// Sibling of `probe_rate_limit_tracked_per_ip` for the per-peer map.
    /// Operator-visible name: `decdn_probe_rate_limit_tracked_per_peer` (#645).
    pub probe_rate_limit_tracked_per_peer: Gauge,
    /// `cdn/dht/v1` request handling failed after the request was admitted
    /// by the rate limiter — frame decode error, response write error,
    /// read timeout, etc. Tracked separately from the rate-limit
    /// rejections so an operator running with default `RUST_LOG=info` can
    /// see the rate of "I accepted this request and then it broke"
    /// failures without scraping debug-level logs. Operator-visible name:
    /// `decdn_dht_requests_failed_total`.
    pub dht_requests_failed: Counter,
    /// `Store` rejected because `holder != authenticated NodeId` (ADR
    /// 022 §STORE Flow line 140). This is the lying-`holder` attack
    /// signal — a non-zero value means at least one peer is trying to
    /// publish records on behalf of someone else's `NodeId`. Operator-
    /// visible name: `decdn_dht_store_rejected_holder_mismatch_total`.
    pub dht_store_rejected_holder_mismatch: Counter,
    /// `Store` rejected because the holder is not in the active-staker
    /// set (ADR 022 §STORE Flow line 140). Sustained growth from many
    /// distinct holders without matching `decdn_dht_store_accepted`
    /// growth is a Sybil-attempt indicator. Operator-visible name:
    /// `decdn_dht_store_rejected_non_staked_total`.
    pub dht_store_rejected_non_staked: Counter,
    /// `Store` rejected because the publisher is at the per-publisher
    /// quota (ADR 022 §Content Records and TTL — 200-record hard cap).
    /// Normal load shouldn't trip this. Operator-visible name:
    /// `decdn_dht_store_rejected_quota_total`.
    pub dht_store_rejected_quota: Counter,
    /// `Store` admitted to the record store (newly inserted OR
    /// refreshed). Pair with the three `dht_store_rejected_*` counters
    /// to compute admission rate. Operator-visible name:
    /// `decdn_dht_store_accepted_total`.
    pub dht_store_accepted: Counter,
    /// `BatchStore` requests that passed stage-1 rate limiting and the
    /// batch-level `holder == authenticated NodeId` check, i.e. reached
    /// per-hash admission (ADR 022 §STORE Flow Batched STORE, #648). The
    /// per-hash outcomes still land in the shared `dht_store_*` counters
    /// above, so this counts batches, not hashes — pair the two to see
    /// average admitted batch size and batch adoption. Operator-visible
    /// name: `decdn_dht_batch_store_received_total`.
    pub dht_batch_store_received: Counter,
    /// Hashes in a `BatchStore` acked `false` purely because the
    /// two-stage rate-limit budget was exhausted before reaching them
    /// (the `n - k` tail, ADR 022 §Batch token accounting / AC 18) — NOT
    /// counted as a per-hash `Store` rejection because they were never
    /// processed. Sustained growth means publishers are sending batches
    /// larger than the per-peer burst allows in one window; the publisher
    /// retries the tail. Operator-visible name:
    /// `decdn_dht_batch_store_hashes_deferred_rate_limit_total`.
    pub dht_batch_store_hashes_deferred_rate_limit: Counter,
    /// Accepted vouchers whose nonce skipped one or more values past the
    /// previously-accepted nonce (`voucher.nonce > last_nonce + 1`), counted
    /// once per gapped voucher (#747). The voucher is still accepted —
    /// vouchers are cumulative, so on-chain settlement is unaffected — but a
    /// non-zero rate flags dropped vouchers (per-voucher deliveries the node
    /// never billed) or a client resetting/forking its counter (a replay-probe
    /// signal). The precise skipped-count rides the paired `tracing::warn!` in
    /// `ChannelState::apply_voucher`. Operator-visible name:
    /// `decdn_voucher_nonce_gaps_total`.
    pub voucher_nonce_gaps: Counter,
    /// Redeem hints the voucher-accept path could not enqueue because the
    /// bounded advisory channel was full (`try_send` → `Full`), counted once
    /// per dropped hint (#751). A hint is advisory — the next voucher re-hints
    /// and the redeemer self-tick / shutdown close still redeem — so a few drops
    /// are benign, but a sustained non-zero rate means the redeemer is not
    /// keeping up with channel fan-out and threshold redemption is leaning on
    /// the slow self-tick. Operator-visible name:
    /// `decdn_redeem_hints_dropped_total`.
    pub redeem_hints_dropped: Counter,
    /// Download-receipt audit writes dropped because the bounded writer queue
    /// was full (`try_send` → `Full`, #803), counted once per dropped receipt.
    /// The receipt log is audit-only and the payment already committed to the
    /// fsynced channel store, so a drop never affects settlement — but a
    /// sustained non-zero rate means the receipt writer cannot keep up with disk
    /// I/O (a slow or full `data_dir`, the end-state of #802) and audit/dispute
    /// records are being lost. Operator-visible name:
    /// `decdn_receipt_writes_dropped_total`.
    pub receipt_writes_dropped: Counter,
    /// Redemption attempts (`try_redeem`) that returned an error — a failed
    /// `getChannel`/`withdraw` RPC or receipt wait (#751). Each is otherwise
    /// only a single `warn!`; a sustained rate means accrued earnings are not
    /// being withdrawn and warrants investigating the RPC / wallet. Operator-
    /// visible name: `decdn_redemption_failures_total`.
    pub redemption_failures: Counter,
    /// Buyer-side reclaim-sweep attempts (`try_reclaim`) that failed — a failed
    /// `getChannel`/`reclaimExpired` RPC, a receipt wait, an on-chain revert, or
    /// a failed store write when clearing the local record after a reclaim/drop
    /// (#906). Each is otherwise only a single `warn!` per hourly sweep; a
    /// sustained rate means an expired channel's refundable deposit is not being
    /// recovered (check the gas wallet / RPC). Pairs with the `error!`
    /// escalation once the same channel fails `RECLAIM_ESCALATION_THRESHOLD`
    /// consecutive sweeps. Operator-visible name:
    /// `decdn_buyer_reclaim_failures_total`.
    pub buyer_reclaim_failures: Counter,
    /// Idle buyer channels cooperatively closed by the reconcile sweep (#972),
    /// reclaiming their deposit early instead of waiting for on-chain expiry. A
    /// healthy capital-efficiency signal — each increment is one deposit freed
    /// ahead of expiry. Operator-visible name:
    /// `decdn_buyer_reconcile_settled_total`.
    pub buyer_reconcile_settled: Counter,
    /// Idle buyer channels the reconcile sweep `closeChannel`d **unilaterally**
    /// because the provider was unreachable for a cooperative close (#988) —
    /// either it deregistered (`node_id_for` → `None`) or it failed
    /// `RECONCILE_CLOSE_ESCALATION_THRESHOLD` consecutive dial/waiver attempts
    /// with a timeout-shaped error. This is the *timeout-shaped unreachability*
    /// bucket #989 asks for: it is the count of escalations to the unilateral
    /// close path, distinct from `buyer_unilateral_close_rpc_failure` (an
    /// on-chain submission fault). The unilateral close opens the dispute window;
    /// the buyer settle sweep finalizes it. Operator-visible name:
    /// `decdn_buyer_unilateral_close_unreachable_total`.
    pub buyer_unilateral_close_unreachable: Counter,
    /// Unilateral `closeChannel` submissions (#988) that did NOT secure the
    /// claim because the RPC send / receipt errored or the tx reverted on-chain
    /// — an infrastructure or on-chain fault, NOT evidence the provider is
    /// unreachable (#989). Split from `buyer_unilateral_close_unreachable` so an
    /// operator can tell "provider genuinely gone" from "our gas wallet / RPC is
    /// the problem"; a sustained rate here means the early-reclaim optimization
    /// is failing for a reason the operator can fix. The expiry-reclaim sweep
    /// remains the safety net. Operator-visible name:
    /// `decdn_buyer_unilateral_close_rpc_failure_total`.
    pub buyer_unilateral_close_rpc_failure: Counter,
    /// Unilateral `closeChannel` submissions (#988) that landed, opening the
    /// dispute window so the buyer settle sweep can reclaim the deposit ~2 days
    /// out instead of at the 90-day expiry. The healthy signal of the early
    /// unilateral path. Operator-visible name:
    /// `decdn_buyer_unilateral_close_ok_total`.
    pub buyer_unilateral_close_ok: Counter,
    /// Buyer `settleChannel` finalization passes (#988) that landed — the
    /// unilaterally-closed channel cleared its dispute window and the deposit
    /// refund settled — or that re-read the channel as already-`Closed` (a
    /// co-settler finalized first). The healthy terminal outcome of the buyer
    /// close→settle lifecycle. Operator-visible name:
    /// `decdn_buyer_settle_ok_total`.
    pub buyer_settle_ok: Counter,
    /// Buyer `settleChannel` finalization passes (#988) that did not finalize
    /// this sweep and were left for the next one: a transient RPC send/receipt
    /// fault, a dispute-extended re-stamp, an unresolved revert, or a pending
    /// store-write failure. Folds the seller path's finer `transient_*` /
    /// `restamped` / `confirm_failed` / `persist_failure` split into one
    /// retry-pending signal — the buyer's settle is a self-refund with the
    /// expiry-reclaim safety net, so the fine breakdown the revenue-critical
    /// seller path needs is not warranted here. A sustained rate means buyer
    /// deposits are not reclaiming early (check gas wallet / RPC). Operator-
    /// visible name: `decdn_buyer_settle_deferred_total`.
    pub buyer_settle_deferred: Counter,
    /// Channel-lifecycle reconciliation the settlement watcher could not apply
    /// from a poll tick: a failed `register_open_channel` /
    /// `update_channel_deposit` / `forget_channel` store write (#751), or a
    /// failed closing-reconciliation on a `ChannelCloseInitiated` (#839) — which
    /// is a `getChannel` read *or* a `record_pending` write, so this counter is
    /// not store-writes-only. Recovery differs by arm: the `ChannelOpened` /
    /// `ChannelToppedUp` / `ChannelCloseInitiated` arms return `Err`, failing the
    /// tick so the window re-scans on the next one; the `forget_channel` arm is
    /// swallowed (the cursor advances past it) and is re-driven only by the settle
    /// sweep. Either way a non-zero rate flags a struggling channel store or RPC.
    /// Operator-visible name: `decdn_watcher_persist_failures_total`.
    pub watcher_persist_failures: Counter,
    /// Slashes detected against this node's operator by the slash watcher
    /// (`SlashJudge.Slashed`), counting each distinct `slashId` once across the
    /// bring-up backfill and the live stream (#1032). A non-zero value means the
    /// operator was slashed and should consider `decdn appeal slash` within the
    /// 30-day window. Operator-visible name: `decdn_slashes_detected_total`.
    pub slashes_detected: Counter,
    /// `decdn_slash_watcher_restarts_total` (#1032): distinct drift windows the
    /// slash-detection watcher has entered, bumped once on the edge into the
    /// error/backoff state. Pairs with `slash_watcher_down_seconds` to tell one
    /// long outage from repeated flapping. Mirrors `staker_set_watcher_restarts`.
    pub slash_watcher_restarts: Counter,
    /// `decdn_slash_watcher_down_seconds` (#1032): seconds the slash-detection
    /// watcher has been stuck in its per-tick backoff loop (`0` on a healthy
    /// cycle), recomputed at scrape from `slash_watcher_down_since`. Mirrors the
    /// other chain watchers — since `admin_v1_slashes` is always wired, this is
    /// the only signal that a wedged watcher could be silently missing slashes.
    pub slash_watcher_down_seconds: Gauge,
    /// Channels the seller path proactively `closeChannel`d because an
    /// operator-configured auto-settlement trigger fired — the un-redeemed
    /// value or voucher count crossed its threshold (#742). Each close starts
    /// the dispute window so a large unsubmitted balance is secured before the
    /// client can go dark; the settle sweep finalizes the remainder. A rising
    /// count is normal under heavy delivery; a flat-zero count on a node that
    /// configured a trigger means nothing has crossed it yet. Operator-visible
    /// name: `decdn_settlement_auto_triggered_total`.
    pub settlement_auto_triggered: Counter,
    /// Auto-settlement close attempts that FAILED after a trigger fired (#742):
    /// the `closeChannel` submit returned no pending tx, or the awaited receipt
    /// reverted / errored. Distinct from `settlement_auto_triggered` (which
    /// counts only secured closes) so an operator can compute a fire-vs-secured
    /// ratio — a sustained non-zero rate means a configured trigger keeps firing
    /// but the close never lands (RPC / wallet / on-chain revert), so the
    /// at-risk balance is NOT being secured and the revenue-protection feature
    /// is silently defeated. The fall-through leaves the redeem path to run, so
    /// `redemption_failures` does NOT move on these — this is the only signal.
    /// Operator-visible name: `decdn_settlement_auto_failures_total`.
    pub settlement_auto_failures: Counter,
    /// `settleChannel` finalization-sweep passes that landed: the closed
    /// channel cleared its dispute window and the provider's un-withdrawn
    /// remainder was routed through the `FeeRouter` (a no-op when the channel
    /// was already fully drawn via `withdraw`), and the pending-settle entry
    /// was dropped (#810). The healthy terminal outcome of the seller
    /// settlement lifecycle; a steady rate tracking closes means settlement is
    /// landing. Distinct from `settlement_auto_triggered` (which counts only
    /// the *`closeChannel`* that opens the window, a different lifecycle
    /// stage). Operator-visible name: `decdn_settlement_finalize_ok_total`.
    pub settlement_finalize_ok: Counter,
    /// `settleChannel` finalization passes where `settleChannel().send()`
    /// itself errored — an RPC timeout / network blip, not an on-chain
    /// decision (#810). The pending entry is left in place and retried next
    /// sweep, so a brief blip self-heals; a sustained rate means the RPC
    /// endpoint is unhealthy and the provider's settled remainder is not being
    /// routed. Operator-visible name:
    /// `decdn_settlement_finalize_transient_send_total`.
    pub settlement_finalize_transient_send: Counter,
    /// `settleChannel` finalization passes where the submit succeeded but
    /// `get_receipt()` errored before a receipt was observed (#810). Like
    /// `..._transient_send` this is a transient RPC condition (the tx may well
    /// have landed) and is retried next sweep; split from the send arm so an
    /// operator can tell a submit-side from a receipt-side RPC fault. Operator-
    /// visible name: `decdn_settlement_finalize_transient_receipt_total`.
    pub settlement_finalize_transient_receipt: Counter,
    /// `settleChannel` finalization passes whose receipt came back with
    /// `status() == false` — an on-chain revert (#810). This is the RAW revert
    /// count; `drop_pending_if_finalized` then re-reads the channel and the
    /// outcome is broken out across the three `settlement_finalize_confirmed_*`
    /// / `_restamped` / `_confirm_failed` counters below, so this total on its
    /// own is NOT a "stuck settlement" signal — the benign co-settler race and
    /// the dispute-extension re-stamp both land here. Distinct from the two
    /// transient arms in that the revert is an on-chain decision, not an RPC
    /// fault. Operator-visible name: `decdn_settlement_finalize_reverted_total`.
    pub settlement_finalize_reverted: Counter,
    /// Reverted `settleChannel` passes resolved as already-`Closed` on re-read:
    /// a co-settler (typically the client claiming its refund) finalized the
    /// channel first, so the obligation is genuinely retired and the pending
    /// entry dropped (#810). Benign — together with `..._restamped` it accounts
    /// for the reverts that need no action; only `..._confirm_failed` is the
    /// stuck-settlement signal. Operator-visible name:
    /// `decdn_settlement_finalize_confirmed_closed_total`.
    pub settlement_finalize_confirmed_closed: Counter,
    /// Reverted `settleChannel` passes resolved as still-`Closing` on re-read:
    /// a `disputeChannel` extended `disputeDeadline` past the value stored at
    /// close, so the sweep re-stamps the gate and retries after the new window
    /// (#810). Also benign and self-healing; a steady rate just means disputes
    /// are landing. Operator-visible name:
    /// `decdn_settlement_finalize_restamped_total`.
    pub settlement_finalize_restamped: Counter,
    /// Reverted `settleChannel` passes that could NOT be resolved to a benign
    /// terminal state, so the pending entry is kept for the next sweep (#810):
    /// either the confirming `getChannel` re-read itself errored, or it
    /// returned an unexpected non-terminal status (`Open` is unreachable for a
    /// channel we closed, so seeing it means a contract/`channel_id`/reorg
    /// anomaly). This is the genuinely-degraded signal the raw `..._reverted`
    /// total cannot give on its own: a revert we cannot resolve. A sustained
    /// rate (especially with a pending set that never drains) means settlement
    /// is stuck and warrants investigation. Operator-visible name:
    /// `decdn_settlement_finalize_confirm_failed_total`.
    pub settlement_finalize_confirm_failed: Counter,
    /// Pending-settle store writes the finalization path swallowed: a
    /// `forget_pending` (after a landed or already-`Closed` settle) or a
    /// `record_pending` re-stamp (after a dispute-extended revert) that
    /// returned a `StoreError` (#810). The settlement itself is unaffected
    /// on-chain — the cost is a redundant `settleChannel` next sweep (which
    /// reverts and re-drops via the `Closed` path) or a stale gate — but a
    /// non-zero rate flags a struggling `PendingSettleStore`, the same way
    /// `watcher_persist_failures` does for the watcher. Operator-visible name:
    /// `decdn_settlement_pending_persist_failures_total`.
    pub settlement_pending_persist_failures: Counter,
    /// `decdn_staker_set_watcher_restarts_total` (#783): distinct drift
    /// windows the [`crate::dht::chain_staker_set`] watcher has entered —
    /// bumped once on the *transition* from a healthy cycle into the
    /// error/backoff state, NOT on every backoff iteration of one continuous
    /// outage. Each increment therefore brackets exactly one window in which
    /// the cached active-staker set can drift from chain state — the module's
    /// documented mid-run degradation. Because the stake-lane probe
    /// reservation (#757) and the DHT `Store` admission path both read that
    /// cached set, sustained restarts are revenue-impacting, not just a
    /// discovery-health blip. The loop-level `tracing::warn!` in
    /// `resumable_watcher::run` (`"watcher RPC error; restarting after backoff"`)
    /// is emitted in the same failed-tick `Err` arm that fires the `on_backoff`
    /// hook this counter hangs off, and carries the underlying error; this
    /// counter is the alertable rate. An idle poll tick (head has not advanced /
    /// no new logs) is NOT an error and does not bump this. Field has no
    /// `_total` suffix because the `OpenMetrics` encoder appends it.
    pub staker_set_watcher_restarts: Counter,
    /// `decdn_staker_set_watcher_resolve_failures_total` (#788): times an
    /// operator-indexed event (`Reinstated` / `UnbondingRequested`) was
    /// dropped because the follow-up `nodeIdOf(operator)` RPC failed. A dropped
    /// resolution leaves the cached active set out of sync with chain state for
    /// that operator until a later event or the (future) resync path corrects
    /// it — exactly the silent drift these #783/#788 metrics exist to surface.
    /// Unlike a stream-level error, this does NOT trip a backoff/restart, so it
    /// would otherwise move no metric at all. Pairs with the per-failure
    /// `warn!` in `apply_operator_change`. Field has no `_total` suffix because
    /// the `OpenMetrics` encoder appends it.
    pub staker_set_watcher_resolve_failures: Counter,
    /// `decdn_staker_set_watcher_down_seconds` (#783, semantics corrected
    /// #788): true downtime — seconds the staker-set watcher has been in the
    /// error/backoff state with no established event filters. Reads `0` for the
    /// entire life of any established cycle, however long or quiet (a healthy
    /// filter stream persists indefinitely, so this must NOT measure cycle
    /// age), and climbs only while the loop is between a failed cycle and the
    /// next successful re-establishment. Reads `0` until the first cycle is
    /// established after bootstrap. Recomputed at scrape time from a monotonic
    /// `down_since` timestamp. A poisoned lock reports `i64::MAX` (not `0`) so
    /// it trips the alert rather than masking an in-progress outage — the
    /// conservative direction for a downtime gauge. This is the drift-window
    /// depth gauge that `decdn_rpc_healthy` (a different watchdog) does not
    /// cover.
    pub staker_set_watcher_down_seconds: Gauge,
    /// `decdn_staker_set_active_count` (#783): current cached active-staker
    /// set size, sampled on every membership change the watcher applies. Pairs
    /// with `decdn_staker_set_watcher_down_seconds` to spot a collapse —
    /// e.g. the count holding flat while down-seconds climbs means the cache
    /// is frozen, not that the network genuinely lost operators.
    pub staker_set_active_count: Gauge,
    /// `decdn_node_address_directory_size` (#831): current count of cached
    /// `NodeId → operator address` bindings the node-to-node pull path resolves
    /// against ([`crate::dht::node_address::ChainNodeAddressDirectory`]).
    /// Sampled on every `NodeRegistered` / `NodeDeregistered` the watcher
    /// applies. A binding the watcher misses (drift window) means the
    /// orchestrator cannot pay that provider and skips it, so a collapsed or
    /// frozen count here directly caps reachable upstream providers.
    pub node_address_directory_size: Gauge,
    /// `decdn_node_pull_attempts_total` (#831): node-to-node cache-miss pull
    /// orchestrations that found ≥1 candidate provider to try. Denominator for
    /// the success/corruption/unreachable rates below.
    pub node_pull_attempts: Counter,
    /// `decdn_node_pull_success_total` (#831): pulls that delivered verified
    /// bytes from an upstream node.
    pub node_pull_success: Counter,
    /// `decdn_node_pull_no_providers_total` (#831): misses where discovery
    /// (DHT plus the origin-directory fallback) surfaced no provider — the blob
    /// is unavailable on the network, not a pull failure.
    pub node_pull_no_providers: Counter,
    /// `decdn_node_pull_corruption_total` (#831): an upstream served
    /// hash-mismatched bytes for a paid pull (Byzantine / buggy provider). A
    /// sustained nonzero rate means a peer is taking payment for wrong content;
    /// the cache engine rejects the bytes, but the USDC was still spent.
    pub node_pull_corruption: Counter,
    /// `decdn_node_pull_unreachable_total` (#831): a probe or pull to a
    /// candidate failed at the transport (scored [`Outcome::Unreachable`]).
    ///
    /// [`Outcome::Unreachable`]: decdn_reputation::Outcome::Unreachable
    pub node_pull_unreachable: Counter,
    /// `decdn_node_pull_channel_open_failures_total` (#831): a buyer
    /// `open_or_reuse_channel` failed before a pull could start. This is the
    /// node's own payment-side fault (gas, RPC, expired channel), NOT the
    /// provider's — a sustained rate means node→node buying is wedged. This is
    /// the *unlabeled total* across all causes; the
    /// `channel_open_failures_*_total` family below (#966) breaks the
    /// `openChannel`-tx failures out by cause so an operator can tell a
    /// misconfiguration (`insufficient_deposit`) from infrastructure
    /// (`rpc_error`). It also covers store/expired-reclaim causes the by-reason
    /// family does not, so the two are not expected to sum equal.
    pub node_pull_channel_open_failures: Counter,
    /// `decdn_channel_open_failures_insufficient_deposit_total` (#966): a buyer
    /// `openChannel` tx reverted because the node's USDC balance/allowance could
    /// not cover the deposit, or the deposit was below the on-chain `minDeposit`
    /// floor. A *misconfiguration* signal — the fix is operator-side (fund the
    /// wallet, raise the configured deposit), not infrastructure. `iroh_metrics`
    /// has no label support, so the issue's `{reason=…}` split is realized as
    /// three sibling counters (mirroring `dht_rate_limit_rejected_*`); the
    /// `reason` value is the field-name token. The `OpenMetrics` encoder appends
    /// the `_total` suffix.
    pub channel_open_failures_insufficient_deposit: Counter,
    /// `decdn_channel_open_failures_contract_revert_total` (#966): a buyer
    /// `openChannel` tx reverted on-chain for a reason other than insufficient
    /// deposit (provider not active, a paused contract, a mined revert whose
    /// reason is not recoverable from the receipt). The deposit was not
    /// escrowed; the cause is on-chain state, not this node's wallet or RPC.
    pub channel_open_failures_contract_revert: Counter,
    /// `decdn_channel_open_failures_rpc_error_total` (#966): a buyer
    /// `openChannel` submit or receipt wait failed at the transport layer (no
    /// revert data) — connectivity, a timed-out receipt, a nonce blip. A
    /// *transient infrastructure* signal; retrying typically clears it. Pair
    /// with the two reverting counters above to tell "operator under-funded the
    /// wallet" from "the RPC endpoint is flaky".
    pub channel_open_failures_rpc_error: Counter,
    /// `decdn_node_pull_too_large_total` (#840): a selected upstream claimed a
    /// `total_bytes` above this node's `max_blob_size` ceiling, so the buyer
    /// rejected it before buffering. Like a channel-open failure this is a
    /// buyer-side policy decision, NOT necessarily provider misbehavior (the
    /// provider may legitimately serve larger blobs to nodes with a higher
    /// ceiling), so it does not tar the provider's reputation. A sustained rate
    /// means this node's ceiling is below the content it is trying to warm.
    pub node_pull_too_large: Counter,
    /// `decdn_node_pull_timeout_total` (#857): a buyer→upstream pull hit this
    /// node's own per-candidate `pull_timeout` deadline. Like a channel-open
    /// failure this is a buyer-side condition (a possibly mis-sized local
    /// timeout), NOT evidence the provider is unreachable, so it does NOT tar the
    /// provider's reputation locally or over gossip. Distinct from
    /// `node_pull_through_timeouts` (the delivery handler's own serving deadline).
    /// A sustained rate means this node's `pull_timeout` is too tight for the
    /// upstreams it selects.
    pub node_pull_timeout: Counter,
    /// `decdn_node_pull_voucher_rejected_total` (#857): an upstream rejected a
    /// voucher this node presented mid-pull (a stale nonce per #852, deposit
    /// exhaustion, or a channel mismatch). This is the node's own payment-side
    /// fault, NOT the provider's, so it does NOT tar the provider's reputation. A
    /// sustained rate means this node's buyer channels are drifting out of sync
    /// with what upstreams accept.
    pub node_pull_voucher_rejected: Counter,
    /// `decdn_node_pull_progress_persist_failures_total` (#852): a pull paid ≥1
    /// voucher but persisting the buyer channel's resume watermark
    /// (`record_progress`) failed. The bytes were delivered, but the channel's
    /// stored `nonce`/`bytes`/`amount` now lags what the upstream accepted — the
    /// next reuse of this channel will re-sign a stale voucher and be rejected. A
    /// non-zero count means a provider is at risk of becoming unusable until
    /// channel rotation.
    pub node_pull_progress_persist_failures: Counter,
    /// `decdn_node_pull_through_timeouts_total` (#831): cache-miss pull-through
    /// attempts the delivery handler abandoned at its deadline. Distinguishes a
    /// slow/wedged upstream from a genuine miss (both otherwise return
    /// `NotFound`). Covers BOTH reactive fill tiers — node→node (#831) and the
    /// local-origin populate (#1116) — so it is not solely a node→node-health
    /// signal; a cache-only operator's own wedged origin bumps it too.
    pub node_pull_through_timeouts: Counter,
    /// `decdn_node_pull_through_errors_total` (#831): cache-engine errors (not
    /// clean misses) hit while filling a miss on the delivery path — a real
    /// store/pull fault, surfaced as `NotFound` to the client but logged + bumped
    /// here so it isn't silent. Covers BOTH reactive fill tiers: node→node (#831)
    /// and the local-origin populate (#1116).
    pub node_pull_through_errors: Counter,
    /// `decdn_node_pull_through_background_spawned_total` (#859): detached
    /// background cache-fill tasks spawned after the foreground delivery
    /// deadline fired, to keep warming the cache from a slow-but-available
    /// upstream for future requests.
    pub node_pull_through_background_spawned: Counter,
    /// `decdn_node_pull_through_background_succeeded_total` (#859): background
    /// cache-fills that populated the blob into the store.
    pub node_pull_through_background_succeeded: Counter,
    /// `decdn_node_pull_through_background_failed_total` (#859): background
    /// cache-fills that gave up (engine error, clean miss, or their own
    /// deadline) without populating the blob. Cancellation on shutdown is not
    /// counted as a failure.
    pub node_pull_through_background_failed: Counter,
    /// `decdn_node_pull_through_window_paused_total` (#856): times the
    /// window-paced serve loop paused the upstream pull because the per-request
    /// unrecouped frontier (`bytes pulled − bytes paid`) reached the effective
    /// window — `pull_ahead_bytes`, floored at one voucher interval — and it waited
    /// for the downstream voucher to clear. A high rate is benign (the window is
    /// doing its job pacing speculation); a flat zero under real pull-through
    /// traffic means the window never binds.
    pub node_pull_through_window_paused: Counter,
    /// `decdn_node_pull_through_leech_budget_paused_total` (#856): speculative
    /// pull-throughs refused or paused because the node-wide unrecouped-leech
    /// budget (`max_unrecouped_leech_bytes`) was exhausted. A sustained rate means
    /// aggregate speculative spend is hitting the operator's circuit breaker.
    pub node_pull_through_leech_budget_paused: Counter,
    /// `decdn_node_pull_through_share_ratio_paused_total` (#856): speculative
    /// pull-throughs refused because a single requesting peer exceeded its
    /// `share_ratio` ceiling (pulled-vs-served). Isolates concentrated
    /// single-peer manufactured-demand abuse.
    pub node_pull_through_share_ratio_paused: Counter,
    /// `decdn_node_pull_through_client_abandoned_total` (#856): window-paced
    /// serves the requesting client dropped or underpaid mid-pull, so the node
    /// aborted the upstream pull and abandoned the partial fill. The per-request
    /// loss is bounded to `pull_ahead_bytes`; a sustained rate flags a leech.
    pub node_pull_through_client_abandoned: Counter,
    /// `decdn_node_pull_through_tee_finalize_failed_total` (#856): a window-paced
    /// serve delivered (and was paid for) the full blob, but promoting the teed
    /// bytes into the local cache failed for a LOCAL, non-integrity reason (store
    /// fault, size-cap breach, or import-task join failure — a tee bao-verify
    /// failure routes to `upstream_verify_failed` instead, ADR 038). The client
    /// got correct bytes; the node forfeits the warm-cache benefit and does NOT
    /// become a holder. A sustained rate means the node is paying upstream egress
    /// on every pull-through and caching none of it — investigate the store /
    /// `data_dir`. Field has no `_total` suffix because the `OpenMetrics` encoder
    /// appends it.
    pub node_pull_through_tee_finalize_failed: Counter,
    /// `decdn_node_pull_through_upstream_verify_failed_total` (#856/#915): a
    /// window-paced serve forwarded an upstream stream that was short of the
    /// promised wire bytes, or whose teed bao stream failed verification against
    /// the content root (ADR 038) — at finalization or mid-stream (the
    /// bait-and-switch case). The teed blob is dropped (never cached), the
    /// upstream is scored `Corruption` on the verify-failure arms, and the
    /// client's own bao decoder rejects the forwarded bytes. Distinct from
    /// `node_pull_corruption` (the buffered orchestration's own check) — this is
    /// the fused serve path. A sustained rate means clients are being served
    /// corrupt-upstream bytes through this node. Field has no `_total` suffix
    /// because the `OpenMetrics` encoder appends it.
    pub node_pull_through_upstream_verify_failed: Counter,
    /// `decdn_node_pull_through_local_tee_failed_total` (#856): a window-paced
    /// serve aborted mid-pull because writing an already-paid upstream chunk into
    /// the local cache tee failed for a genuinely LOCAL reason (a
    /// store/`data_dir` fault) — a mid-stream bao-verify rejection routes to
    /// `upstream_verify_failed` instead (#915). Distinct from
    /// `node_pull_through_client_abandoned`, which counts the downstream client
    /// dropping or underpaying. Splitting the three lets an operator tell a
    /// failing local store from a lying upstream from flaky/abusive downstreams:
    /// a sustained rate HERE points at disk, not peers. Field has no `_total`
    /// suffix because the `OpenMetrics` encoder appends it.
    pub node_pull_through_local_tee_failed: Counter,
    /// `decdn_node_pull_delta_overflow_total` (#820): per-pull prefetch-ledger
    /// spend/byte deltas that exceeded `u64` when narrowed from `U256` and were
    /// clamped to `0` (the conservative under-count direction). A real per-pull
    /// delta never approaches `u64::MAX`, so any nonzero value signals upstream
    /// voucher-accounting corruption — otherwise visible only by log-grep of the
    /// `narrow_pull_delta` warning. Field has no `_total` suffix because the
    /// `OpenMetrics` encoder appends it.
    pub node_pull_delta_overflow: Counter,
    /// `decdn_node_address_watcher_restarts_total` (#831): distinct drift windows
    /// of the `NodeId → address` resolver's event watcher (mirrors the staker-set
    /// watcher, #788). Edge-triggered once per outage, not per backoff iteration.
    /// While down, fresh `NodeRegistered` bindings are missed → those providers
    /// become unpayable and are silently skipped, so this is the signal that the
    /// pull path's reachable-provider set may be capped by stale bindings.
    pub node_address_watcher_restarts: Counter,
    /// `decdn_node_address_watcher_down_seconds` (#831): seconds the
    /// `NodeId → address` resolver watcher has been in its current error/backoff
    /// window, recomputed at scrape from `node_address_watcher_down_since`. Reads
    /// `0` across any established cycle; a poisoned lock reports `i64::MAX` (the
    /// conservative alerting direction).
    pub node_address_watcher_down_seconds: Gauge,
    /// `decdn_reputation_indexer_rpc_failures_total` (#326): failing poll ticks —
    /// the settlement indexer's `get_logs`/head RPC errored, or a `nodeIdOf`
    /// party-resolution RPC (in the sink's `apply`) failed. Both now propagate to
    /// the same per-tick backoff path (`on_backoff`): a resolution failure no
    /// longer skips a party in place, and the un-credited settlement is retried,
    /// so a persistently flaky RPC can bump this once per retry cycle. A
    /// sustained nonzero rate means the indexer is not ingesting settlements, so
    /// reporter weights silently stay 0 and network scores never leave neutral —
    /// exactly the dead-indexer condition that is otherwise log-only. Field has
    /// no `_total` suffix because the `OpenMetrics` encoder appends it.
    pub reputation_indexer_rpc_failures: Counter,
    /// `decdn_reputation_indexer_settlements_credited_total` (#326): party
    /// creditings applied to the settlement source (two per fully-resolved
    /// settlement). A flat-zero counter alongside live `ChannelSettled` traffic
    /// indicates a binding-resolution or correlation problem.
    pub reputation_indexer_settlements_credited: Counter,
    /// `decdn_reputation_indexer_amount_overflows_total` (#326): `ChannelSettled`
    /// events whose `routedAmount` exceeded `u128` and were skipped rather than
    /// saturated (a saturated value would poison the node-local credibility
    /// denominator). Expected to stay 0; a nonzero value flags malformed/hostile
    /// on-chain data.
    pub reputation_indexer_amount_overflows: Counter,
    /// `decdn_origin_directory_watcher_restarts_total` (#651): distinct drift
    /// windows the [`crate::dht::chain_origin_directory`] watcher has entered.
    /// Same semantics as `staker_set_watcher_restarts` — bumped once on the
    /// transition into the error/backoff state, not per backoff iteration.
    /// During such a window the cached `namespace → operator` directory can
    /// drift from chain state, and the prefetch authorized-origin gate reads
    /// that cache, so sustained restarts gate real prefetch demand. The
    /// `OpenMetrics` encoder appends the `_total` suffix.
    pub origin_directory_watcher_restarts: Counter,
    /// `decdn_origin_directory_watcher_resolve_failures_total` (#651): times a
    /// `getOrigins` for a newly-claimed namespace OR a `nodeIdOf(operator)`
    /// binding lookup failed, leaving an operator unmapped (and so unresolvable
    /// as an origin) until a later event re-surfaces it. Does not trip a
    /// backoff, so without this counter it would move no metric. Pairs with the
    /// per-failure `warn!` in `chain_origin_directory`.
    pub origin_directory_watcher_resolve_failures: Counter,
    /// `decdn_origin_directory_watcher_down_seconds` (#651): true downtime —
    /// seconds the origin-directory watcher has been in the error/backoff state
    /// with no established filters. Reads `0` for the life of any established
    /// cycle; recomputed at scrape time from a monotonic `down_since`. A
    /// poisoned lock reports `i64::MAX` (alerting direction).
    pub origin_directory_watcher_down_seconds: Gauge,
    /// `decdn_origin_directory_operator_count` (#651): distinct operator
    /// addresses currently authorised as origins across all namespaces plus the
    /// default-open allow-list. Recomputed and sampled after every event that
    /// mutates an authorised set, so it rises on activate/add and falls on
    /// revoke/prune/remove/replace (unlike the monotonic binding cache).
    pub origin_directory_operator_count: Gauge,
    /// `1` if `prefetch.enabled`, else `0` (ADR 022 §Prefetch Decision;
    /// appendix-observability §Prefetch Metrics). Stable schema across nodes:
    /// every node reports the prefetch family regardless of whether the
    /// feature is on. Visible name: `decdn_prefetch_enabled`.
    pub prefetch_enabled: Gauge,
    /// Prefetch acquisitions that passed the authorized-origin gate. Visible
    /// name: `decdn_prefetch_acquisitions_authorized_total`. (`iroh_metrics`
    /// has no labels, so the appendix's `{gate_result=…}` split is realized as
    /// three sibling counters, mirroring `dht_rate_limit_rejected_*`.)
    pub prefetch_acquisitions_authorized: Counter,
    /// Prefetch attempts rejected by the authorized-origin gate. Visible name:
    /// `decdn_prefetch_acquisitions_unauthorized_total`.
    pub prefetch_acquisitions_unauthorized: Counter,
    /// Prefetch acquisitions where the origin gate was disabled (bypassed).
    /// Visible name: `decdn_prefetch_acquisitions_bypassed_total`.
    pub prefetch_acquisitions_bypassed: Counter,
    /// Cumulative micro-USDC paid for prefetch acquisitions (#820). Incremented
    /// on each prefetch-initiated paid pull that acked vouchers — a success OR a
    /// paid-but-failed delivery (whose voucher watermark advanced) — so it tracks
    /// all speculative spend, not just successes. Visible name:
    /// `decdn_prefetch_spend_usdc_total`.
    pub prefetch_spend_usdc: Counter,
    /// Times the rolling-1h prefetch budget was hit, blocking acquisitions
    /// until the window advanced. Visible name:
    /// `decdn_prefetch_budget_exhaustion_events_total`.
    pub prefetch_budget_exhaustion_events: Counter,
    /// Prefetch attempts skipped by the origin gate (identical by construction
    /// to `prefetch_acquisitions_unauthorized`; surfaced standalone for
    /// alerting). Visible name: `decdn_prefetch_origin_gate_rejections_total`.
    pub prefetch_origin_gate_rejections: Counter,
    /// Current rolling-window `served / acquired` ratio scaled ×1000 (an
    /// integer gauge — `iroh_metrics::Gauge` is integer-valued). Visible name:
    /// `decdn_prefetch_demand_quality_ratio_milli`.
    pub prefetch_demand_quality_ratio_milli: Gauge,
    /// `1` while the demand-quality auto-throttle suppresses prefetch, else
    /// `0`. Visible name: `decdn_prefetch_throttle_active`.
    pub prefetch_throttle_active: Gauge,
    /// Speculative acquisitions whose pull-through completed and cached the blob
    /// (#820). Visible name: `decdn_prefetch_acquire_succeeded_total`.
    pub prefetch_acquire_succeeded: Counter,
    /// Speculative acquisitions whose pull-through found no source or errored
    /// (#820). Visible name: `decdn_prefetch_acquire_failed_total`.
    pub prefetch_acquire_failed: Counter,
    /// Speculative acquisitions that hit their per-acquisition deadline (#820).
    /// Visible name: `decdn_prefetch_acquire_timeout_total`.
    pub prefetch_acquire_timeout: Counter,
    /// Speculative acquisitions dropped because the concurrency cap was full
    /// (#820). Visible name: `decdn_prefetch_acquire_dropped_saturated_total`.
    pub prefetch_acquire_dropped_saturated: Counter,
    /// Paid-delivery (`serve_stream`) requests refused because the blob was
    /// deliberately evicted between probe and stream (#279). One `Counter` per
    /// reason — like the `dispatch_rejected_*` convention — because the metrics
    /// backend has no per-field labels, and the wire `StreamError` deliberately
    /// conflates the three `NotFound` reasons (`cache_miss`, `unknown_channel`,
    /// `owner_mismatch`) (#876). Visible name:
    /// `decdn_serve_stream_rejected_evicted_since_probe_total`.
    pub serve_stream_rejected_evicted_since_probe: Counter,
    /// `serve_stream` requests refused because the blob is absent and
    /// pull-through was unavailable or failed to fill it. Visible name:
    /// `decdn_serve_stream_rejected_cache_miss_total`.
    pub serve_stream_rejected_cache_miss: Counter,
    /// `serve_stream` requests refused by a local store fault (`has`/`inspect`
    /// error, or a present blob reporting no size) — surfaced to the client as
    /// `InternalError`, not a signed absence. Visible name:
    /// `decdn_serve_stream_rejected_internal_error_total`.
    pub serve_stream_rejected_internal_error: Counter,
    /// `serve_stream` requests refused because the blob exceeds the configured
    /// `max_blob_size_bytes`. Visible name:
    /// `decdn_serve_stream_rejected_blob_too_large_total`.
    pub serve_stream_rejected_blob_too_large: Counter,
    /// `serve_stream` requests refused on an unknown / never-opened channel
    /// (#848). Wire-indistinguishable from `cache_miss`/`owner_mismatch` (all
    /// signed as `NotFound` to avoid leaking channel existence), so this
    /// server-side counter is the only place the distinction lives — a rising
    /// value isolates an unknown-channel abuse campaign. Visible name:
    /// `decdn_serve_stream_rejected_unknown_channel_total`.
    pub serve_stream_rejected_unknown_channel: Counter,
    /// `serve_stream` requests refused because a verified client binding does
    /// not authorize the named channel (#327). Visible name:
    /// `decdn_serve_stream_rejected_owner_mismatch_total`.
    pub serve_stream_rejected_owner_mismatch: Counter,
    /// `serve_stream` cache-miss requests refused before any upstream pull
    /// because the requesting channel's remaining deposit could not cover the
    /// worst-case blob cost at the node's rate (#856 pre-flight deposit guard).
    /// Wire-indistinguishable from `cache_miss` (signed as `NotFound`), so this
    /// server-side counter is the only place the distinction lives — a rising
    /// value isolates near-empty-deposit pull-through abuse. Visible name:
    /// `decdn_serve_stream_rejected_insufficient_deposit_total`.
    pub serve_stream_rejected_insufficient_deposit: Counter,
    /// `serve_stream` cache-miss requests refused before any upstream pull
    /// because the operator's `pull_through_require_authorized_origin` gate is on
    /// and the hash's namespace has no currently-authorized origin (#821, ADR 037
    /// §Seed-leech caps). Wire-indistinguishable from `cache_miss` (signed as
    /// `NotFound`), so this server-side counter is the only place the distinction
    /// lives — a rising value shows how much unclaimed-content warming the gate is
    /// shedding. Visible name:
    /// `decdn_serve_stream_rejected_unauthorized_origin_total`.
    pub serve_stream_rejected_unauthorized_origin: Counter,
    /// New delivery refused because the channel has a signed cooperative-close
    /// waiver (ADR 003 §Cooperative close) — the node committed to settling at
    /// the watermark and serves no further bytes. Wire-indistinguishable from
    /// `unknown_channel` (signed as `NotFound`), so this counter is the only
    /// place the distinction lives. Visible name:
    /// `decdn_serve_stream_rejected_cooperative_close_signed_total`.
    pub serve_stream_rejected_cooperative_close_signed: Counter,
    /// Delivery refused because the requested bounded range
    /// `[byte_offset, byte_offset + byte_len)` is out of bounds for the blob
    /// (ADR 005 §Bounded byte ranges: the node MUST reject an overflowing or
    /// past-EOF range). Wire-indistinguishable from `cache_miss` (signed as
    /// `NotFound`), so this server-side counter is the only place the
    /// distinction lives — a rising value flags clients issuing malformed
    /// ranges. Visible name:
    /// `decdn_serve_stream_rejected_range_not_satisfiable_total`.
    pub serve_stream_rejected_range_not_satisfiable: Counter,
}

/// Self-imposed cap on the distinct-peer tracking set (and hence the
/// `quic_session_ticket_cache_size` gauge). It is **not** a rustls cache
/// size — the server-side ticket store is rustls-internal and untouched
/// by iroh's `max_tls_tickets`. We reuse
/// [`decdn_protocol::SESSION_TICKET_CACHE_SIZE`] (the value that *does*
/// size the client-side `ClientSessionMemoryCache`) purely so the node's
/// 0-RTT memory budget is described by one number across client and
/// server roles.
const SESSION_TICKET_CACHE_CEILING: usize = decdn_protocol::SESSION_TICKET_CACHE_SIZE;

/// Aggregated deCDN node metrics.
#[derive(Debug)]
pub struct Metrics {
    registry: Arc<RwLock<Registry>>,
    decdn: Arc<DecdnMetrics>,
    cache: Arc<CacheMetrics>,
    started_at: Instant,
    /// Distinct remote endpoint ids with a completed 0-RTT-eligible
    /// handshake. Backs the approximate `quic_session_ticket_cache_size`
    /// gauge (rustls exposes no session-store size API). Bounded at
    /// `SESSION_TICKET_CACHE_CEILING` entries by `note_session_ticket_peer`
    /// — the insert path is fed by the unauthenticated probe handler, so
    /// the cap is what stops an unbounded-distinct-peer memory leak.
    session_ticket_peers: Mutex<HashSet<[u8; 32]>>,
    /// Monotonic instant at which the staker-set watcher entered its current
    /// error/backoff window (#783, downtime semantics #788). `None` whenever a
    /// cycle is established (healthy) — including from bootstrap until the
    /// first cycle and after every successful re-establishment. `Some(t)` only
    /// while the loop is between a failed cycle and the next success. Backs the
    /// `staker_set_watcher_down_seconds` gauge, recomputed from this at scrape
    /// time so the value reads exactly `0` across an arbitrarily long healthy
    /// cycle and climbs only through an actual outage. A poisoned lock reports
    /// `i64::MAX` down-seconds (the conservative, alerting direction for a
    /// downtime gauge — reporting `0` would mask an in-progress outage).
    staker_set_watcher_down_since: Mutex<Option<Instant>>,
    /// Monotonic instant at which the `NodeId → address` resolver watcher
    /// entered its current error/backoff window (#831). Same semantics as
    /// `staker_set_watcher_down_since`: `None` while a cycle is healthy, `Some`
    /// only during an outage; backs `node_address_watcher_down_seconds`.
    node_address_watcher_down_since: Mutex<Option<Instant>>,
    /// Monotonic instant at which the origin-directory watcher entered its
    /// current error/backoff window (#651). `None` whenever a cycle is
    /// established. Backs the `origin_directory_watcher_down_seconds` gauge,
    /// recomputed at scrape time. Mirrors `staker_set_watcher_down_since`.
    origin_directory_watcher_down_since: Mutex<Option<Instant>>,
    /// `Instant` the slash-detection watcher entered its current error/backoff
    /// window (#1032). `None` whenever a cycle is established. Backs the
    /// `slash_watcher_down_seconds` gauge, recomputed at scrape time. Mirrors
    /// `staker_set_watcher_down_since`.
    slash_watcher_down_since: Mutex<Option<Instant>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create the registry and register deCDN's metric group plus the
    /// cache crate's `decdn_cache_*` group. The cache handle is shared
    /// with the engine via [`Self::cache_metrics`] so engine-side bumps
    /// land in the same encoder output.
    pub fn new() -> Self {
        let decdn = Arc::new(DecdnMetrics::default());
        let cache = Arc::new(CacheMetrics::default());
        let mut registry = Registry::default();
        registry.register(decdn.clone() as Arc<dyn MetricsGroup>);
        // Cache metrics live under the `decdn_cache` prefix so they
        // share the `decdn_*` family the rest of the metrics use.
        registry
            .sub_registry_with_prefix("decdn")
            .register(cache.clone() as Arc<dyn MetricsGroup>);
        Self {
            registry: Arc::new(RwLock::new(registry)),
            decdn,
            cache,
            started_at: Instant::now(),
            session_ticket_peers: Mutex::new(HashSet::new()),
            staker_set_watcher_down_since: Mutex::new(None),
            node_address_watcher_down_since: Mutex::new(None),
            origin_directory_watcher_down_since: Mutex::new(None),
            slash_watcher_down_since: Mutex::new(None),
        }
    }

    /// Shared `Arc<CacheMetrics>` for wiring into [`decdn_cache::CacheEngine`].
    pub fn cache_metrics(&self) -> Arc<CacheMetrics> {
        Arc::clone(&self.cache)
    }

    /// Register iroh's transport metrics under the `decdn_iroh_` prefix so
    /// `magicsock_*`, `net_report_*`, etc. come out as
    /// `decdn_iroh_magicsock_*`, matching ADR 020's naming convention.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned.
    pub fn register_iroh_endpoint(&self, ep: &Endpoint) -> anyhow::Result<()> {
        let mut reg = self
            .registry
            .write()
            .map_err(|_| anyhow::anyhow!("metrics registry lock poisoned"))?;
        reg.sub_registry_with_prefix("decdn_iroh")
            .register_all(ep.metrics());
        Ok(())
    }

    pub fn started(&self) {
        self.decdn.uptime_seconds.set(0);
    }

    pub fn probe_request(&self) {
        self.decdn.probe_requests.inc();
    }

    /// A probe answered `has_blob: false` despite the bytes being present,
    /// because the eviction hold could not be guaranteed (ADR 005 §Hold
    /// budget).
    pub fn probe_hold_violation(&self) {
        self.decdn.probe_hold_violations.inc();
    }

    /// A probe answered `has_blob: false` for a present blob because the
    /// eviction-hold path is disabled by config (`max_probe_holds == 0`) —
    /// an intentional operator decision, not budget pressure (#739, ADR 005
    /// §Hold budget).
    pub fn probe_holds_disabled(&self) {
        self.decdn.probe_holds_disabled.inc();
    }

    /// An end-client probe answered `has_blob: false` because the hold
    /// budget reached the stake-lane-reserved end-client ceiling, before any
    /// cache lookup (#757, ADR 003 §Admission and Priority). A deliberate,
    /// content-independent priority decision — not budget pressure or a
    /// config disable.
    pub fn probe_stake_lane_reserved(&self) {
        self.decdn.probe_stake_lane_reserved.inc();
    }

    /// Publish the current count of active probe holds (ADR 005).
    pub fn probe_hold_slots(&self, used: usize) {
        self.decdn
            .probe_hold_slots_used
            .set(i64::try_from(used).unwrap_or(i64::MAX));
    }

    /// Publish the configured `max_probe_holds` budget (registry-mandatory
    /// `decdn_probe_hold_slots_max`). Called once at runtime bring-up.
    pub fn probe_hold_slots_max(&self, max: usize) {
        self.decdn
            .probe_hold_slots_max
            .set(i64::try_from(max).unwrap_or(i64::MAX));
    }

    /// The node clamped `rate_per_mb` to the configured delivery bounds
    /// before signing (ADR 005 §Rate bounds validation).
    pub fn rate_bounds_clamped(&self) {
        self.decdn.rate_bounds_clamp_events.inc();
    }

    /// A voucher was refused for paying below the protocol per-byte price
    /// floor (`delivery_floor`) — the off-chain mirror of the on-chain
    /// `RateFloorViolation` settlement guard (#846).
    pub fn voucher_rate_floor_rejected(&self) {
        self.decdn.voucher_rate_floor_rejections.inc();
    }

    /// Set the `decdn_prefetch_enabled` gauge once at startup (ADR 022
    /// §Prefetch; appendix-observability §Prefetch Metrics).
    pub fn set_prefetch_enabled(&self, enabled: bool) {
        self.decdn.prefetch_enabled.set(i64::from(enabled));
    }

    /// Record the outcome of a prefetch decision against the skip counters.
    /// The would-acquire split (authorized vs bypassed) is recorded separately
    /// by [`Self::record_prefetch_acquire`].
    pub fn record_prefetch_decision(&self, outcome: crate::prefetch::PrefetchOutcome) {
        use crate::prefetch::PrefetchOutcome;
        use crate::prefetch::decision::{PrefetchDecision, SkipReason};
        let PrefetchOutcome::Decided(PrefetchDecision::Skip(reason)) = outcome else {
            return;
        };
        match reason {
            SkipReason::Unauthorized => {
                self.decdn.prefetch_acquisitions_unauthorized.inc();
                self.decdn.prefetch_origin_gate_rejections.inc();
            }
            SkipReason::BudgetExhausted => {
                self.decdn.prefetch_budget_exhaustion_events.inc();
            }
            SkipReason::Disabled | SkipReason::Throttled => {}
        }
    }

    /// Record a would-acquire decision, split by whether the origin gate was
    /// applied (`authorized`) or disabled (`bypassed`).
    pub fn record_prefetch_acquire(&self, gate_applied: bool) {
        if gate_applied {
            self.decdn.prefetch_acquisitions_authorized.inc();
        } else {
            self.decdn.prefetch_acquisitions_bypassed.inc();
        }
    }

    /// Add `micro_usdc` to the cumulative prefetch spend (#820). Called from the
    /// acquisition observer on each prefetch-initiated paid pull that acked
    /// vouchers, whether it ultimately succeeded or failed after paying.
    pub fn add_prefetch_spend(&self, micro_usdc: u64) {
        self.decdn.prefetch_spend_usdc.inc_by(micro_usdc);
    }

    /// A speculative acquisition cached the blob (#820).
    pub fn prefetch_acquire_succeeded(&self) {
        self.decdn.prefetch_acquire_succeeded.inc();
    }

    /// A speculative acquisition found no source or errored (#820).
    pub fn prefetch_acquire_failed(&self) {
        self.decdn.prefetch_acquire_failed.inc();
    }

    /// A speculative acquisition hit its per-acquisition deadline (#820).
    pub fn prefetch_acquire_timeout(&self) {
        self.decdn.prefetch_acquire_timeout.inc();
    }

    /// A speculative acquisition was dropped at the concurrency cap (#820).
    pub fn prefetch_acquire_dropped_saturated(&self) {
        self.decdn.prefetch_acquire_dropped_saturated.inc();
    }

    /// Refresh the demand-quality gauges from the policy state. `ratio` is
    /// scaled ×1000 into the integer gauge; non-finite/negative ratios report
    /// `0`.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )] // ratio is a small non-negative f64; the clamp keeps `as i64` in range.
    pub fn set_prefetch_quality(&self, ratio: f64, throttled: bool) {
        let milli = (ratio * 1000.0).round();
        let milli = if !milli.is_finite() || milli <= 0.0 {
            0
        } else if milli >= i64::MAX as f64 {
            i64::MAX
        } else {
            milli as i64
        };
        self.decdn.prefetch_demand_quality_ratio_milli.set(milli);
        self.decdn
            .prefetch_throttle_active
            .set(i64::from(throttled));
    }

    /// An accepted voucher skipped one or more nonce values past
    /// `last_nonce + 1` (#747). Counted once per gapped voucher; the precise
    /// skip count rides the paired `tracing::warn!` in `apply_voucher`.
    pub fn voucher_nonce_gap(&self) {
        self.decdn.voucher_nonce_gaps.inc();
    }

    /// A redeem hint was dropped because the bounded advisory channel was full
    /// (`try_send` → `Full`, #751). Advisory, so a few drops are benign; a
    /// sustained rate means the redeemer is not keeping up with fan-out.
    pub fn redeem_hint_dropped(&self) {
        self.decdn.redeem_hints_dropped.inc();
    }

    /// A download-receipt audit write was dropped because the bounded writer
    /// queue was full (`try_send` → `Full`, #803). Audit-only, so a drop never
    /// affects settlement; a sustained rate means the writer is not keeping up
    /// with disk I/O and audit records are being lost.
    pub fn receipt_write_dropped(&self) {
        self.decdn.receipt_writes_dropped.inc();
    }

    /// A redemption attempt (`try_redeem`) failed with an RPC/receipt error
    /// (#751). Pairs with the `warn!` in `redeemer_loop`.
    pub fn redemption_failure(&self) {
        self.decdn.redemption_failures.inc();
    }

    /// A buyer-side reclaim-sweep attempt (`try_reclaim`) failed — an RPC/receipt
    /// error, an on-chain revert, or a failed store write when clearing the local
    /// record (#906). Pairs with the per-attempt `warn!` in `try_reclaim` and the
    /// threshold `error!` in `reclaim_once`.
    pub fn buyer_reclaim_failure(&self) {
        self.decdn.buyer_reclaim_failures.inc();
    }

    /// The idle-reconcile sweep cooperatively closed one idle buyer channel,
    /// reclaiming its deposit early (#972).
    pub fn buyer_reconcile_settled(&self) {
        self.decdn.buyer_reconcile_settled.inc();
    }

    /// The reconcile sweep escalated an idle channel to a **unilateral**
    /// `closeChannel` because the provider was unreachable for a cooperative
    /// close — deregistered, or timing out repeatedly (#988/#989). The
    /// timeout-shaped-unreachability bucket, distinct from
    /// `buyer_unilateral_close_rpc_failure`.
    pub fn buyer_unilateral_close_unreachable(&self) {
        self.decdn.buyer_unilateral_close_unreachable.inc();
    }

    /// A unilateral `closeChannel` did not secure the claim — the RPC
    /// send/receipt errored or the tx reverted on-chain (#988/#989). An
    /// infrastructure/on-chain fault, not provider unreachability.
    pub fn buyer_unilateral_close_rpc_failure(&self) {
        self.decdn.buyer_unilateral_close_rpc_failure.inc();
    }

    /// A unilateral `closeChannel` landed, opening the dispute window for the
    /// buyer settle sweep to reclaim the deposit early (#988).
    pub fn buyer_unilateral_close_ok(&self) {
        self.decdn.buyer_unilateral_close_ok.inc();
    }

    /// A buyer `settleChannel` finalization landed, or re-read the channel as
    /// already-`Closed` (a co-settler finalized first) — either way the buyer's
    /// deposit refund is recovered (#988).
    pub fn buyer_settle_ok(&self) {
        self.decdn.buyer_settle_ok.inc();
    }

    /// A buyer `settleChannel` finalization did not finalize this sweep and was
    /// left for the next — a transient RPC fault, a dispute-extended re-stamp,
    /// an unresolved revert, or a pending store-write failure (#988).
    pub fn buyer_settle_deferred(&self) {
        self.decdn.buyer_settle_deferred.inc();
    }

    /// An auto-settlement trigger fired and the seller path `closeChannel`d a
    /// channel to secure its un-redeemed balance on-chain (#742). Pairs with
    /// the `info!` in `try_redeem`.
    pub fn settlement_auto_triggered(&self) {
        self.decdn.settlement_auto_triggered.inc();
    }

    /// An auto-settlement trigger fired but the `closeChannel` did NOT secure
    /// the balance — the submit failed or the receipt reverted/errored (#742).
    /// Pairs with the `warn!` in `try_auto_settle_close`. Kept distinct from
    /// `settlement_auto_triggered` so a fire-vs-secured ratio is computable.
    pub fn settlement_auto_failure(&self) {
        self.decdn.settlement_auto_failures.inc();
    }

    /// A `settleChannel` finalization sweep landed: the dispute window cleared,
    /// the provider remainder routed through the `FeeRouter`, and the pending
    /// entry was dropped (#810). Pairs with the success `info!` in `try_settle`.
    pub fn settlement_finalize_ok(&self) {
        self.decdn.settlement_finalize_ok.inc();
    }

    /// A `settleChannel` finalization sweep could not submit — `send()` errored
    /// on a transient RPC/network fault and the entry is retried next sweep
    /// (#810). Pairs with the send-arm `warn!` in `try_settle`.
    pub fn settlement_finalize_transient_send(&self) {
        self.decdn.settlement_finalize_transient_send.inc();
    }

    /// A `settleChannel` finalization sweep submitted but `get_receipt()`
    /// errored before a receipt was seen — a transient RPC fault, retried next
    /// sweep (#810). Pairs with the receipt-arm `warn!` in `try_settle`.
    pub fn settlement_finalize_transient_receipt(&self) {
        self.decdn.settlement_finalize_transient_receipt.inc();
    }

    /// A `settleChannel` finalization sweep reverted on-chain
    /// (`receipt.status() == false`, #810). Raw revert count; the cause is
    /// classified afterward by `drop_pending_if_finalized` into the
    /// `settlement_finalize_confirmed_closed` / `_restamped` / `_confirm_failed`
    /// counters. Pairs with the revert-arm `warn!` in `try_settle`.
    pub fn settlement_finalize_reverted(&self) {
        self.decdn.settlement_finalize_reverted.inc();
    }

    /// A reverted `settleChannel` re-read as already-`Closed`: a co-settler
    /// finalized first, the obligation is retired (#810). The benign
    /// co-settler-race case. Pairs with the `Closed`-arm `info!` in
    /// `drop_pending_if_finalized`.
    pub fn settlement_finalize_confirmed_closed(&self) {
        self.decdn.settlement_finalize_confirmed_closed.inc();
    }

    /// A reverted `settleChannel` re-read as still-`Closing`: a dispute
    /// extended the window, the gate is re-stamped and retried (#810). Benign.
    /// Pairs with the `Closing`-arm re-stamp in `drop_pending_if_finalized`.
    pub fn settlement_finalize_restamped(&self) {
        self.decdn.settlement_finalize_restamped.inc();
    }

    /// A reverted `settleChannel` left unresolved: the confirming `getChannel`
    /// re-read errored, or returned an unexpected non-terminal status, so the
    /// entry is kept (#810) — the genuinely-degraded signal. Pairs with the
    /// read-error and unexpected-status `warn!`s in `drop_pending_if_finalized`.
    pub fn settlement_finalize_confirm_failed(&self) {
        self.decdn.settlement_finalize_confirm_failed.inc();
    }

    /// A finalization-path pending-settle store write (`forget_pending` or a
    /// re-stamp `record_pending`) returned a `StoreError` and was swallowed
    /// (#810). Pairs with the persist-failure `warn!` sites in
    /// `forget_pending_logged` / `restamp_pending_logged`.
    pub fn settlement_pending_persist_failure(&self) {
        self.decdn.settlement_pending_persist_failures.inc();
    }

    /// The settlement watcher hit a channel-lifecycle persist failure
    /// (`register_open_channel` / `update_channel_deposit` / `forget_channel`,
    /// #751), in `SettlementSink::apply` (`payment_settlement.rs`). Only the
    /// `forget_channel` arm is swallowed, and it carries the sole per-site
    /// `warn!` ("failed to forget settled channel"); the other arms return `Err`,
    /// so their context surfaces in `resumable_watcher::run`'s loop-level
    /// `warn!` ("watcher RPC error; restarting after backoff") instead.
    pub fn watcher_persist_failure(&self) {
        self.decdn.watcher_persist_failures.inc();
    }

    /// A distinct slash against this node's operator was detected by the slash
    /// watcher (#1032). Counts each `slashId` once (backfill + live dedup).
    pub fn slash_detected(&self) {
        self.decdn.slashes_detected.inc();
    }

    /// The slash-detection watcher's cycle errored and the loop is about to back
    /// off (#1032). Stamps `slash_watcher_down_since` (once per drift window) so
    /// `slash_watcher_down_seconds` climbs until the next healthy cycle. Mirrors
    /// [`Self::staker_set_watcher_backoff_started`]; a poisoned lock skips the
    /// update (the gauge keeps climbing — the safe alerting direction).
    pub fn slash_watcher_backoff_started(&self) {
        if let Ok(mut down_since) = self.slash_watcher_down_since.lock()
            && down_since.is_none()
        {
            *down_since = Some(Instant::now());
            self.decdn.slash_watcher_restarts.inc();
        }
    }

    /// Mark the slash-detection watcher cycle established (#1032): clear
    /// `slash_watcher_down_since` so `slash_watcher_down_seconds` reads `0` for
    /// the life of the cycle. Mirrors [`Self::staker_set_watcher_cycle_established`].
    pub fn slash_watcher_cycle_established(&self) {
        if let Ok(mut down_since) = self.slash_watcher_down_since.lock() {
            *down_since = None;
        }
    }

    /// A staker-set watcher poll tick failed with an error and the loop is
    /// about to back off (#783, [`crate::dht::chain_staker_set`]).
    /// Stamps `down_since` so `staker_set_watcher_down_seconds` begins to climb,
    /// and — on the *transition* from a healthy cycle into the error state
    /// (`down_since` was `None`) — bumps `staker_set_watcher_restarts_total`
    /// exactly once per drift window, rather than once per backoff iteration of
    /// one continuous outage. A poisoned lock is treated as "skip the update"
    /// rather than panicking (anti-panic policy); the gauge's poison fallback
    /// (`i64::MAX`) still keeps the alert tripped. This method is the
    /// `on_backoff` hook wired in [`crate::dht::chain_staker_set`]; it pairs with
    /// the loop-level `warn!` in `resumable_watcher::run` (`"watcher RPC error;
    /// restarting after backoff"`).
    pub fn staker_set_watcher_backoff_started(&self) {
        if let Ok(mut down_since) = self.staker_set_watcher_down_since.lock()
            && down_since.is_none()
        {
            // Edge into the error state: open a fresh drift window and count it.
            *down_since = Some(Instant::now());
            self.decdn.staker_set_watcher_restarts.inc();
        }
    }

    /// A `nodeIdOf(operator)` resolution for an operator-indexed event failed,
    /// dropping the membership change (#788, [`crate::dht::chain_staker_set`]).
    /// Bumps `staker_set_watcher_resolve_failures_total`. Pairs with the
    /// per-failure `warn!` in `apply_operator_change`.
    pub fn staker_set_watcher_resolve_failure(&self) {
        self.decdn.staker_set_watcher_resolve_failures.inc();
    }

    /// Mark the staker-set watcher's poll cycle as established (#783,
    /// downtime semantics #788): a poll tick succeeded and logs are flowing
    /// again (the `on_established` hook). Clears `down_since` to `None` so
    /// `staker_set_watcher_down_seconds`
    /// reads `0` for the entire life of this cycle, however long. A poisoned
    /// lock is treated as "skip the update" rather than panicking (anti-panic
    /// policy); the gauge then keeps climbing, which is the safe (alerting)
    /// direction.
    pub fn staker_set_watcher_cycle_established(&self) {
        if let Ok(mut down_since) = self.staker_set_watcher_down_since.lock() {
            *down_since = None;
        }
    }

    /// Publish the current cached active-staker set size (#783). Sampled on
    /// every membership change the watcher applies, so the gauge tracks the
    /// cached view — which under a watcher outage is exactly the (possibly
    /// stale) set that admission decisions read.
    pub fn staker_set_active_count(&self, count: usize) {
        self.decdn
            .staker_set_active_count
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }

    /// Publish the current cached `NodeId → operator address` binding count
    /// (#831). Sampled on every binding change the node-address watcher applies,
    /// so it tracks the cached view the pull path resolves against.
    pub fn node_address_directory_size(&self, count: usize) {
        self.decdn
            .node_address_directory_size
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }

    /// A node-to-node pull orchestration found ≥1 candidate and is attempting a
    /// fill (#831).
    pub fn node_pull_attempt(&self) {
        self.decdn.node_pull_attempts.inc();
    }

    /// A node-to-node pull delivered verified bytes (#831).
    pub fn node_pull_success(&self) {
        self.decdn.node_pull_success.inc();
    }

    /// A cache miss surfaced no provider from discovery (#831).
    pub fn node_pull_no_providers(&self) {
        self.decdn.node_pull_no_providers.inc();
    }

    /// An upstream served hash-mismatched bytes for a paid pull (#831).
    pub fn node_pull_corruption(&self) {
        self.decdn.node_pull_corruption.inc();
    }

    /// A probe or pull to a candidate failed at the transport (#831).
    pub fn node_pull_unreachable(&self) {
        self.decdn.node_pull_unreachable.inc();
    }

    /// A buyer channel open/reuse failed before a pull could start (#831).
    pub fn node_pull_channel_open_failure(&self) {
        self.decdn.node_pull_channel_open_failures.inc();
    }

    /// Record a buyer `openChannel`-tx failure broken out by cause (#966): bumps
    /// the `decdn_channel_open_failures_{reason}_total` sibling counter for
    /// `reason`. Pairs with the structured `reason` field on the `warn!`/`debug!`
    /// in [`crate::node_origin`]. Distinct from
    /// [`Self::node_pull_channel_open_failure`], the unlabeled total (which also
    /// counts store/expired-reclaim causes that never reach the `openChannel`
    /// tx).
    pub fn channel_open_failure_by_reason(&self, reason: ChannelOpenFailureReason) {
        match reason {
            ChannelOpenFailureReason::InsufficientDeposit => {
                self.decdn.channel_open_failures_insufficient_deposit.inc();
            }
            ChannelOpenFailureReason::ContractRevert => {
                self.decdn.channel_open_failures_contract_revert.inc();
            }
            ChannelOpenFailureReason::RpcError => {
                self.decdn.channel_open_failures_rpc_error.inc();
            }
        }
    }

    /// A selected upstream claimed a `total_bytes` above this node's
    /// `max_blob_size` ceiling and the buyer rejected it before buffering
    /// (#840). A buyer-side policy decision, so it does not score the provider.
    pub fn node_pull_too_large(&self) {
        self.decdn.node_pull_too_large.inc();
    }

    /// Record a paid-delivery (`serve_stream`) request refused because the
    /// blob was evicted between probe and stream (#876).
    pub fn serve_stream_rejected_evicted_since_probe(&self) {
        self.decdn.serve_stream_rejected_evicted_since_probe.inc();
    }

    /// Record a `serve_stream` request refused on a cache miss that
    /// pull-through could not fill (#876).
    pub fn serve_stream_rejected_cache_miss(&self) {
        self.decdn.serve_stream_rejected_cache_miss.inc();
    }

    /// Record a `serve_stream` request refused by a local store fault,
    /// surfaced as `InternalError` (#876).
    pub fn serve_stream_rejected_internal_error(&self) {
        self.decdn.serve_stream_rejected_internal_error.inc();
    }

    /// Record a `serve_stream` request refused because the blob exceeds
    /// `max_blob_size_bytes` (#876).
    pub fn serve_stream_rejected_blob_too_large(&self) {
        self.decdn.serve_stream_rejected_blob_too_large.inc();
    }

    /// Record a `serve_stream` request refused on an unknown channel (#876).
    pub fn serve_stream_rejected_unknown_channel(&self) {
        self.decdn.serve_stream_rejected_unknown_channel.inc();
    }

    /// Record a `serve_stream` request refused because the client binding did
    /// not authorize the named channel (#876).
    pub fn serve_stream_rejected_owner_mismatch(&self) {
        self.decdn.serve_stream_rejected_owner_mismatch.inc();
    }

    /// Record a `serve_stream` cache-miss refused by the pre-flight deposit guard
    /// (#856): the requesting channel could not cover the worst-case blob cost,
    /// so no upstream pull was started.
    pub fn serve_stream_rejected_insufficient_deposit(&self) {
        self.decdn.serve_stream_rejected_insufficient_deposit.inc();
    }

    /// Record a `serve_stream` cache-miss refused by the authorized-origin gate
    /// (#821): `pull_through_require_authorized_origin` is on and the hash's
    /// namespace has no authorized origin, so no upstream pull was started.
    pub fn serve_stream_rejected_unauthorized_origin(&self) {
        self.decdn.serve_stream_rejected_unauthorized_origin.inc();
    }

    /// Record a `serve_stream` delivery refused because the channel has a signed
    /// cooperative-close waiver (ADR 003 §Cooperative close).
    pub fn serve_stream_rejected_cooperative_close_signed(&self) {
        self.decdn
            .serve_stream_rejected_cooperative_close_signed
            .inc();
    }

    /// Record a `serve_stream` delivery refused because the requested bounded
    /// range is out of bounds for the blob (ADR 005 §Bounded byte ranges).
    pub fn serve_stream_rejected_range_not_satisfiable(&self) {
        self.decdn.serve_stream_rejected_range_not_satisfiable.inc();
    }

    /// The window-paced serve loop paused the upstream pull at `pull_ahead_bytes`
    /// to wait for the downstream voucher to clear (#856).
    pub fn node_pull_through_window_paused(&self) {
        self.decdn.node_pull_through_window_paused.inc();
    }

    /// A speculative pull-through was refused/paused by the node-wide
    /// unrecouped-leech budget (#856).
    pub fn node_pull_through_leech_budget_paused(&self) {
        self.decdn.node_pull_through_leech_budget_paused.inc();
    }

    /// A speculative pull-through was refused because a peer exceeded its
    /// `share_ratio` ceiling (#856).
    pub fn node_pull_through_share_ratio_paused(&self) {
        self.decdn.node_pull_through_share_ratio_paused.inc();
    }

    /// A window-paced serve was abandoned because the requesting client dropped
    /// or underpaid mid-pull (#856).
    pub fn node_pull_through_client_abandoned(&self) {
        self.decdn.node_pull_through_client_abandoned.inc();
    }

    /// A window-paced serve delivered the full blob but failed to promote it into
    /// the local cache (#856).
    pub fn node_pull_through_tee_finalize_failed(&self) {
        self.decdn.node_pull_through_tee_finalize_failed.inc();
    }

    /// A window-paced serve forwarded an upstream stream that failed its whole-blob
    /// hash check at finalization (#856).
    pub fn node_pull_through_upstream_verify_failed(&self) {
        self.decdn.node_pull_through_upstream_verify_failed.inc();
    }

    /// A window-paced serve aborted because a local cache-tee write of an
    /// already-paid upstream chunk failed (#856) — a store fault, not a downstream
    /// client drop.
    pub fn node_pull_through_local_tee_failed(&self) {
        self.decdn.node_pull_through_local_tee_failed.inc();
    }

    /// A per-pull prefetch-ledger delta overflowed `u64` and was clamped to `0`
    /// (#820) — a signal of upstream voucher-accounting corruption.
    pub fn node_pull_delta_overflow(&self) {
        self.decdn.node_pull_delta_overflow.inc();
    }

    /// A buyer→upstream pull hit this node's own `pull_timeout` deadline (#857).
    /// A buyer-side condition, so it does not score the provider's reputation.
    pub fn node_pull_timeout(&self) {
        self.decdn.node_pull_timeout.inc();
    }

    /// An upstream rejected a voucher this node presented mid-pull (#857) — a
    /// buyer payment-side fault, so it does not score the provider's reputation.
    pub fn node_pull_voucher_rejected(&self) {
        self.decdn.node_pull_voucher_rejected.inc();
    }

    /// A pull paid ≥1 voucher but persisting the buyer channel resume watermark
    /// failed (#852); the channel's stored progress now lags the upstream.
    pub fn node_pull_progress_persist_failure(&self) {
        self.decdn.node_pull_progress_persist_failures.inc();
    }

    /// The delivery handler abandoned a pull-through at its deadline (#831).
    pub fn node_pull_through_timeout(&self) {
        self.decdn.node_pull_through_timeouts.inc();
    }

    /// A cache-engine error (not a clean miss) was hit filling a miss (#831).
    pub fn node_pull_through_error(&self) {
        self.decdn.node_pull_through_errors.inc();
    }

    /// A detached background cache-fill was spawned after the foreground
    /// delivery deadline fired (#859).
    pub fn node_pull_through_background_spawned(&self) {
        self.decdn.node_pull_through_background_spawned.inc();
    }

    /// A background cache-fill populated the blob into the store (#859).
    pub fn node_pull_through_background_succeeded(&self) {
        self.decdn.node_pull_through_background_succeeded.inc();
    }

    /// A background cache-fill gave up without populating the blob (#859).
    pub fn node_pull_through_background_failed(&self) {
        self.decdn.node_pull_through_background_failed.inc();
    }

    /// Open a drift window for the `NodeId → address` resolver watcher (#831):
    /// stamp `node_address_watcher_down_since` and, on the edge into the error
    /// state, bump `node_address_watcher_restarts_total` exactly once. Mirrors
    /// [`Self::staker_set_watcher_backoff_started`]; a poisoned lock skips the
    /// update (the gauge then keeps climbing — the safe alerting direction).
    pub fn node_address_watcher_backoff_started(&self) {
        if let Ok(mut down_since) = self.node_address_watcher_down_since.lock()
            && down_since.is_none()
        {
            *down_since = Some(Instant::now());
            self.decdn.node_address_watcher_restarts.inc();
        }
    }

    /// Mark the `NodeId → address` resolver watcher cycle established (#831):
    /// clear `node_address_watcher_down_since` so `..._down_seconds` reads `0`
    /// for the life of the cycle. Mirrors
    /// [`Self::staker_set_watcher_cycle_established`].
    pub fn node_address_watcher_cycle_established(&self) {
        if let Ok(mut down_since) = self.node_address_watcher_down_since.lock() {
            *down_since = None;
        }
    }

    /// Bump `reputation_indexer_rpc_failures_total` (#326): an indexer event
    /// stream errored or a `nodeIdOf` resolution RPC failed. Pairs with the
    /// per-failure `warn!` in `crate::reputation_indexer`.
    pub fn reputation_indexer_rpc_failure(&self) {
        self.decdn.reputation_indexer_rpc_failures.inc();
    }

    /// Bump `reputation_indexer_settlements_credited_total` (#326) by `n` party
    /// creditings applied for one settlement (0, 1, or 2).
    pub fn reputation_indexer_settlements_credited(&self, n: u64) {
        self.decdn.reputation_indexer_settlements_credited.inc_by(n);
    }

    /// Bump `reputation_indexer_amount_overflows_total` (#326): a settlement
    /// amount exceeded `u128` and was skipped (not saturated).
    pub fn reputation_indexer_amount_overflow(&self) {
        self.decdn.reputation_indexer_amount_overflows.inc();
    }

    /// An origin-directory watcher poll tick errored and the loop is
    /// about to back off (#651). Mirrors `staker_set_watcher_backoff_started`:
    /// stamps `down_since` and counts exactly one restart per drift window.
    pub fn origin_directory_watcher_backoff_started(&self) {
        if let Ok(mut down_since) = self.origin_directory_watcher_down_since.lock()
            && down_since.is_none()
        {
            *down_since = Some(Instant::now());
            self.decdn.origin_directory_watcher_restarts.inc();
        }
    }

    /// A `getOrigins` / `nodeIdOf` resolution failed, leaving an operator
    /// unmapped in the origin directory (#651). Bumps
    /// `origin_directory_watcher_resolve_failures_total`.
    pub fn origin_directory_watcher_resolve_failure(&self) {
        self.decdn.origin_directory_watcher_resolve_failures.inc();
    }

    /// Mark the origin-directory watcher's poll cycle as established
    /// (#651): clears `down_since` so `origin_directory_watcher_down_seconds`
    /// reads `0` for the life of this cycle.
    pub fn origin_directory_watcher_cycle_established(&self) {
        if let Ok(mut down_since) = self.origin_directory_watcher_down_since.lock() {
            *down_since = None;
        }
    }

    /// Publish the count of distinct operator addresses currently authorised as
    /// origins — the union of every namespace's operator set and the
    /// default-open allow-list (#651). Falls on revoke/prune/remove/replace,
    /// unlike the monotonic `operator → NodeId` binding cache. The caller
    /// recomputes this (`authorized_operator_count`) after each set mutation.
    pub fn origin_directory_operator_count(&self, count: usize) {
        self.decdn
            .origin_directory_operator_count
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }

    pub fn connection_opened(&self) {
        self.decdn.active_connections.inc();
    }

    pub fn connection_closed(&self) {
        self.decdn.active_connections.dec();
    }

    pub fn gossip_published(&self, _topic: &str) {
        self.decdn.gossip_announces_published_total.inc();
    }

    pub fn gossip_received(&self, _topic: &str) {
        self.decdn.gossip_announces_received_total.inc();
    }

    pub fn gossip_rejected(&self, _reason: &'static str) {
        self.decdn.gossip_announces_rejected_total.inc();
    }

    pub fn gossip_peer_table_size(&self, n: i64) {
        self.decdn.gossip_peer_table_size.set(n);
    }

    pub fn gossip_reconnected(&self, _topic: &str) {
        self.decdn.gossip_subscriber_reconnections_total.inc();
    }

    /// Set the RPC health gauge. `true` -> 1 (reachable), `false` -> 0
    /// (unreachable). Driven by the watchdog task spawned in
    /// `runtime::run`.
    pub fn rpc_healthy(&self, ok: bool) {
        self.decdn.rpc_healthy.set(i64::from(ok));
    }

    /// Record a connection rejected by the global concurrency semaphore.
    pub fn dispatch_rejected_global(&self) {
        self.decdn.dispatch_rejected_global.inc();
    }

    /// Record a connection rejected by the per-source rate limiter.
    pub fn dispatch_rejected_per_source(&self) {
        self.decdn.dispatch_rejected_per_source.inc();
    }

    /// Increment the in-flight dispatch permit gauge.
    pub fn dispatch_permit_acquired(&self) {
        self.decdn.dispatch_in_flight.inc();
    }

    /// Decrement the in-flight dispatch permit gauge.
    pub fn dispatch_permit_released(&self) {
        self.decdn.dispatch_in_flight.dec();
    }

    /// Current value of the `dispatch_in_flight` gauge as a `u64`. Read
    /// by `admin_v1_health.in_flight_streams` (issue #604) so the
    /// `decdn node drain --wait` client can observe in-flight client
    /// streams reach 0 during a graceful drain.
    ///
    /// A negative gauge reading is forbidden by the `Permit`
    /// acquire/release RAII pair: `dispatch_permit_acquired` runs once
    /// before each `Permit` exists and `dispatch_permit_released` runs
    /// once on `Drop`. If the conversion fails the pair has been
    /// broken — we both `debug_assert!` (loud failure in tests/dev),
    /// log `tracing::error!` (visible in production), and return 0
    /// rather than wrapping to a huge `u64`. **Returning 0 here would
    /// be read by `decdn node drain --wait` as "drain complete" while
    /// real streams are still in flight**, so the loud log is the
    /// operator-actionable signal that something broke upstream.
    #[must_use]
    pub fn dispatch_in_flight_value(&self) -> u64 {
        let raw = self.decdn.dispatch_in_flight.get();
        if let Ok(v) = u64::try_from(raw) {
            return v;
        }
        debug_assert!(false, "dispatch_in_flight gauge went negative: {raw}");
        tracing::error!(
            raw,
            "dispatch_in_flight gauge is negative; the Permit \
             acquire/release pair is unbalanced. Reporting 0 to \
             avoid wraparound — `decdn node drain --wait` clients \
             may see a premature 'complete' signal until the \
             underlying accounting bug is fixed."
        );
        0
    }

    /// Record a relay-only connection accepted while the per-source
    /// layer was enabled but no peer IP could be resolved at accept
    /// time.
    pub fn dispatch_per_source_skipped_no_addr(&self) {
        self.decdn.dispatch_per_source_skipped_no_addr.inc();
    }

    /// Record a `cdn/dht/v1` request rejected at the per-peer layer.
    pub fn dht_rate_limit_rejected_per_peer(&self) {
        self.decdn.dht_rate_limit_rejected_per_peer.inc();
    }

    /// Record a `cdn/dht/v1` request rejected at the per-IP layer.
    pub fn dht_rate_limit_rejected_per_ip(&self) {
        self.decdn.dht_rate_limit_rejected_per_ip.inc();
    }

    /// Record a `cdn/dht/v1` request rejected at the global layer.
    pub fn dht_rate_limit_rejected_global(&self) {
        self.decdn.dht_rate_limit_rejected_global.inc();
    }

    /// Record a `retain_recent` sweep of the per-IP DHT keyed-limiter
    /// map (#645).
    pub fn dht_rate_limit_prune_sweep_per_ip(&self) {
        self.decdn.dht_rate_limit_prune_sweeps_per_ip.inc();
    }

    /// Record a `retain_recent` sweep of the per-peer DHT keyed-limiter
    /// map (#645).
    pub fn dht_rate_limit_prune_sweep_per_peer(&self) {
        self.decdn.dht_rate_limit_prune_sweeps_per_peer.inc();
    }

    /// Set the per-IP DHT keyed-limiter tracked-size gauge (#645). The
    /// `try_from(...).unwrap_or(i64::MAX)` clamp matches the existing
    /// gauge-set pattern elsewhere in this module and stays within the
    /// workspace's anti-panic policy.
    pub fn dht_rate_limit_tracked_per_ip_set(&self, n: usize) {
        self.decdn
            .dht_rate_limit_tracked_per_ip
            .set(i64::try_from(n).unwrap_or(i64::MAX));
    }

    /// Set the per-peer DHT keyed-limiter tracked-size gauge (#645).
    pub fn dht_rate_limit_tracked_per_peer_set(&self, n: usize) {
        self.decdn
            .dht_rate_limit_tracked_per_peer
            .set(i64::try_from(n).unwrap_or(i64::MAX));
    }

    /// Record a `cdn/probe/v1` request rejected at the per-peer layer.
    pub fn probe_rate_limit_rejected_per_peer(&self) {
        self.decdn.probe_rate_limit_rejected_per_peer.inc();
    }

    /// Record a `cdn/probe/v1` request rejected at the per-IP layer.
    pub fn probe_rate_limit_rejected_per_ip(&self) {
        self.decdn.probe_rate_limit_rejected_per_ip.inc();
    }

    /// Record a `cdn/probe/v1` request rejected at the global layer.
    pub fn probe_rate_limit_rejected_global(&self) {
        self.decdn.probe_rate_limit_rejected_global.inc();
    }

    /// Record a `retain_recent` sweep of the per-IP probe keyed-limiter
    /// map (#645).
    pub fn probe_rate_limit_prune_sweep_per_ip(&self) {
        self.decdn.probe_rate_limit_prune_sweeps_per_ip.inc();
    }

    /// Record a `retain_recent` sweep of the per-peer probe keyed-limiter
    /// map (#645).
    pub fn probe_rate_limit_prune_sweep_per_peer(&self) {
        self.decdn.probe_rate_limit_prune_sweeps_per_peer.inc();
    }

    /// Set the per-IP probe keyed-limiter tracked-size gauge (#645).
    pub fn probe_rate_limit_tracked_per_ip_set(&self, n: usize) {
        self.decdn
            .probe_rate_limit_tracked_per_ip
            .set(i64::try_from(n).unwrap_or(i64::MAX));
    }

    /// Set the per-peer probe keyed-limiter tracked-size gauge (#645).
    pub fn probe_rate_limit_tracked_per_peer_set(&self, n: usize) {
        self.decdn
            .probe_rate_limit_tracked_per_peer
            .set(i64::try_from(n).unwrap_or(i64::MAX));
    }

    /// Record a `cdn/dht/v1` request that was admitted by the rate
    /// limiter but failed after that (frame decode, write, encode,
    /// timeout, etc).
    pub fn dht_request_failed(&self) {
        self.decdn.dht_requests_failed.inc();
    }

    /// Record a `Store` rejected by the `holder != authenticated NodeId`
    /// check (ADR 022 §STORE Flow line 140 — lying-holder attack).
    pub fn dht_store_rejected_holder_mismatch(&self) {
        self.decdn.dht_store_rejected_holder_mismatch.inc();
    }

    /// Record a `Store` rejected by the active-staker filter (ADR 022
    /// §STORE Flow line 140 — non-staked publisher).
    pub fn dht_store_rejected_non_staked(&self) {
        self.decdn.dht_store_rejected_non_staked.inc();
    }

    /// Record a `Store` rejected by the per-publisher quota (ADR 022
    /// §Content Records and TTL — 200-record hard cap).
    pub fn dht_store_rejected_quota(&self) {
        self.decdn.dht_store_rejected_quota.inc();
    }

    /// Record a `Store` admitted to the record store (newly inserted or
    /// refreshed).
    pub fn dht_store_accepted(&self) {
        self.decdn.dht_store_accepted.inc();
    }

    /// Record a `BatchStore` that reached per-hash admission (passed
    /// stage-1 rate limiting + the batch-level holder check, #648).
    pub fn dht_batch_store_received(&self) {
        self.decdn.dht_batch_store_received.inc();
    }

    /// Record `count` `BatchStore` hashes deferred (acked `false`)
    /// because the two-stage rate-limit budget ran out before reaching
    /// them (ADR 022 §Batch token accounting, #648).
    pub fn dht_batch_store_hashes_deferred_rate_limit(&self, count: u64) {
        self.decdn
            .dht_batch_store_hashes_deferred_rate_limit
            .inc_by(count);
    }

    /// Record a 0-RTT connection attempt (ADR 015): a cached session
    /// ticket existed and early data was sent.
    pub fn record_0rtt_attempt(&self) {
        self.decdn.quic_0rtt_attempts.inc();
    }

    /// Record that the server accepted a 0-RTT attempt.
    pub fn record_0rtt_accepted(&self) {
        self.decdn.quic_0rtt_accepted.inc();
    }

    /// Record that the server rejected a 0-RTT attempt and the client
    /// fell back to a 1-RTT handshake.
    pub fn record_0rtt_rejected(&self) {
        self.decdn.quic_0rtt_rejected.inc();
    }

    /// Note a remote endpoint with which a 0-RTT-eligible handshake
    /// completed, refreshing the approximate
    /// `quic_session_ticket_cache_size` gauge. Idempotent per peer; a
    /// poisoned lock is treated as "skip the update" rather than
    /// panicking (anti-panic policy).
    ///
    /// The tracking set is itself bounded at `SESSION_TICKET_CACHE_CEILING`,
    /// not just the gauge value: this is called from the *unauthenticated*
    /// probe handler, so a peer presenting many distinct node ids (cheap
    /// to generate) would otherwise grow the set without limit — a slow
    /// memory-exhaustion vector on untrusted input. Once the set is full
    /// new peers are no longer tracked (re-noting an already-tracked peer
    /// stays a no-op) and `quic_session_ticket_peers_dropped` is bumped so
    /// the saturation is distinguishable from organic growth; the gauge
    /// then sits at the ceiling. The ceiling is the node's own memory
    /// bound, not a rustls cache size (see `SESSION_TICKET_CACHE_CEILING`).
    pub fn note_session_ticket_peer(&self, remote_id: [u8; 32]) {
        let Ok(mut peers) = self.session_ticket_peers.lock() else {
            return;
        };
        if peers.len() < SESSION_TICKET_CACHE_CEILING {
            peers.insert(remote_id);
        } else if !peers.contains(&remote_id) {
            // Set is full AND this is a genuinely new peer: the memory
            // bound is engaging on (untrusted) input. Surface it so a
            // Sybil-style flood is distinguishable from organic
            // saturation. Re-noting an already-tracked peer is a
            // legitimate no-op and must NOT count as a drop, or the
            // counter becomes noise.
            self.decdn.quic_session_ticket_peers_dropped.inc();
        }
        let size = peers.len();
        self.decdn
            .quic_session_ticket_cache_size
            .set(i64::try_from(size).unwrap_or(i64::MAX));
    }

    /// Read the current value of the `rpc_healthy` gauge. Test-only —
    /// non-test callers should rely on the `OpenMetrics` endpoint rather
    /// than reaching into individual gauges.
    #[cfg(test)]
    pub(crate) fn rpc_healthy_value(&self) -> i64 {
        self.decdn.rpc_healthy.get()
    }

    /// RAII guard that increments `active_connections` on construction and
    /// decrements it on drop, so the gauge stays correct even if the handler
    /// future is cancelled between open and close.
    pub fn connection_guard(&self) -> ConnectionGuard<'_> {
        ConnectionGuard::new(self)
    }

    /// Render the registry as `OpenMetrics` text — exactly the body the
    /// public `/metrics` HTTP endpoint serves. `pub` (not `pub(crate)`)
    /// so integration tests in sibling crates can assert on the exported
    /// series without scraping over TCP; it exposes no data the
    /// unauthenticated `/metrics` endpoint doesn't already.
    pub fn encode(&self) -> anyhow::Result<String> {
        let uptime = i64::try_from(self.started_at.elapsed().as_secs()).unwrap_or(i64::MAX);
        self.decdn.uptime_seconds.set(uptime);

        // Recompute the staker-set watcher down-seconds gauge from `down_since`
        // (true downtime), mirroring `uptime_seconds`. `None` (healthy cycle,
        // including pre-bootstrap) reads `0` regardless of how long the cycle
        // has been live. A poisoned lock reports `i64::MAX` — the conservative,
        // alerting direction for a downtime gauge: reporting `0` would MASK an
        // in-progress outage. Once an error window opens the value climbs until
        // the next `staker_set_watcher_cycle_established` clears `down_since`.
        let down_seconds = match self.staker_set_watcher_down_since.lock() {
            Ok(down_since) => down_since
                .map(|t| t.elapsed().as_secs())
                .map_or(0, |s| i64::try_from(s).unwrap_or(i64::MAX)),
            Err(_) => i64::MAX,
        };
        self.decdn.staker_set_watcher_down_seconds.set(down_seconds);

        // Same recompute for the node-address resolver watcher (#831).
        let node_addr_down_seconds = match self.node_address_watcher_down_since.lock() {
            Ok(down_since) => down_since
                .map(|t| t.elapsed().as_secs())
                .map_or(0, |s| i64::try_from(s).unwrap_or(i64::MAX)),
            Err(_) => i64::MAX,
        };
        self.decdn
            .node_address_watcher_down_seconds
            .set(node_addr_down_seconds);

        // Same recompute for the origin-directory watcher (#651).
        let origin_dir_down_seconds = match self.origin_directory_watcher_down_since.lock() {
            Ok(down_since) => down_since
                .map(|t| t.elapsed().as_secs())
                .map_or(0, |s| i64::try_from(s).unwrap_or(i64::MAX)),
            Err(_) => i64::MAX,
        };
        self.decdn
            .origin_directory_watcher_down_seconds
            .set(origin_dir_down_seconds);

        // Same recompute for the slash-detection watcher (#1032).
        let slash_watcher_down_seconds = match self.slash_watcher_down_since.lock() {
            Ok(down_since) => down_since
                .map(|t| t.elapsed().as_secs())
                .map_or(0, |s| i64::try_from(s).unwrap_or(i64::MAX)),
            Err(_) => i64::MAX,
        };
        self.decdn
            .slash_watcher_down_seconds
            .set(slash_watcher_down_seconds);

        let reg = self
            .registry
            .read()
            .map_err(|_| anyhow::anyhow!("metrics registry lock poisoned"))?;
        reg.encode_openmetrics_to_string()
            .map_err(|e| anyhow::anyhow!("openmetrics encode failed: {e}"))
    }
}

/// Which side of a `PaymentChannel` the shared settle-finalization helper
/// ([`crate::payment_settlement::settle_pass`]) is running for, so the same
/// `settleChannel` → revert-resolution state machine routes its outcome to the
/// correct metric family. The seller path keeps its full
/// `settlement_finalize_*` breakdown (revenue-critical); the buyer path
/// (#988, a self-refund with the expiry-reclaim safety net) folds that into the
/// compact `buyer_settle_ok` / `buyer_settle_deferred` pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SettleParty {
    /// This node is the provider finalizing a client's drawn-down channel.
    Seller,
    /// This node is the buyer reclaiming its own deposit after a unilateral
    /// close of an unreachable provider's channel.
    Buyer,
}

impl SettleParty {
    /// `settleChannel` landed (or the channel was already `Closed` on re-read).
    pub(crate) fn finalize_ok(self, m: &Metrics) {
        match self {
            Self::Seller => m.settlement_finalize_ok(),
            Self::Buyer => m.buyer_settle_ok(),
        }
    }

    /// `settleChannel` reverted on-chain (raw count). For the buyer this is a
    /// no-op: the revert is always reclassified by one of the resolution arms
    /// below, so counting it here too would double-count against the compact
    /// `buyer_settle_*` pair.
    pub(crate) fn finalize_reverted(self, m: &Metrics) {
        if let Self::Seller = self {
            m.settlement_finalize_reverted();
        }
    }

    /// `settleChannel().send()` errored on a transient RPC fault.
    pub(crate) fn finalize_transient_send(self, m: &Metrics) {
        match self {
            Self::Seller => m.settlement_finalize_transient_send(),
            Self::Buyer => m.buyer_settle_deferred(),
        }
    }

    /// `get_receipt()` errored after a successful submit (transient RPC fault).
    pub(crate) fn finalize_transient_receipt(self, m: &Metrics) {
        match self {
            Self::Seller => m.settlement_finalize_transient_receipt(),
            Self::Buyer => m.buyer_settle_deferred(),
        }
    }

    /// A reverted settle re-read as already-`Closed` (a co-settler finalized
    /// first). For the buyer the deposit is recovered either way, so this is a
    /// success.
    pub(crate) fn finalize_confirmed_closed(self, m: &Metrics) {
        match self {
            Self::Seller => m.settlement_finalize_confirmed_closed(),
            Self::Buyer => m.buyer_settle_ok(),
        }
    }

    /// A reverted settle re-read as still-`Closing` (a dispute extended the
    /// window); the gate is re-stamped and retried.
    pub(crate) fn finalize_restamped(self, m: &Metrics) {
        match self {
            Self::Seller => m.settlement_finalize_restamped(),
            Self::Buyer => m.buyer_settle_deferred(),
        }
    }

    /// A reverted settle left unresolved (the confirming read errored or
    /// returned an unexpected status); the entry is kept for the next sweep.
    pub(crate) fn finalize_confirm_failed(self, m: &Metrics) {
        match self {
            Self::Seller => m.settlement_finalize_confirm_failed(),
            Self::Buyer => m.buyer_settle_deferred(),
        }
    }

    /// A pending-settle store write (`forget_pending` / re-stamp
    /// `record_pending`) returned a `StoreError` and was swallowed.
    pub(crate) fn pending_persist_failure(self, m: &Metrics) {
        match self {
            Self::Seller => m.settlement_pending_persist_failure(),
            Self::Buyer => m.buyer_settle_deferred(),
        }
    }
}

/// Bind the `/metrics` HTTP listener synchronously so startup can fail fast
/// if the port is unavailable. The returned listener is consumed by [`serve`].
///
/// Emits a `WARN` if `addr` is non-loopback (#579). The `OpenMetrics`
/// surface exposes peer-table size, gossip rejection reasons,
/// pull-through byte volumes, GC/connection stats — useful
/// reconnaissance for anyone who can reach it. The default config
/// binds loopback (`appendix-local-admin-http` calls metrics
/// "loopback-only"), but `observability.metrics_bind` is operator-
/// settable to `0.0.0.0` for containerised deployments
/// (`crates/common/src/config/types.rs` — `ObservabilityConfig::metrics_bind`).
/// We warn but do not reject so that documented container workflows
/// keep working.
///
/// # Errors
///
/// Returns an error if the `TcpListener::bind` call fails (port in use,
/// permissions, etc.).
pub async fn bind(addr: SocketAddr) -> anyhow::Result<TcpListener> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("metrics bind {addr} failed: {e}"))?;
    // `to_canonical()` unwraps IPv4-mapped IPv6 (e.g.
    // `::ffff:127.0.0.1`) so an operator binding the dual-stack
    // form of loopback doesn't get a false "non-loopback" warning.
    // `Ipv6Addr::is_loopback()` only matches `::1`.
    let ip = addr.ip().to_canonical();
    if !ip.is_loopback() {
        // `is_unspecified()` (`0.0.0.0` / `::`) is the common
        // containerised case; we call it out by name so an operator
        // grepping startup logs sees the intent. A public IP falls
        // through to the generic non-loopback message.
        if ip.is_unspecified() {
            tracing::warn!(
                %addr,
                "metrics server is binding all interfaces (non-loopback); the OpenMetrics \
                 endpoint exposes peer-table size, gossip rejection reasons, pull-through \
                 byte volumes, and GC/connection stats — gate it behind a private network \
                 or reverse proxy if reachable from outside the host"
            );
        } else {
            tracing::warn!(
                %addr,
                "metrics server is binding a non-loopback address; the OpenMetrics \
                 endpoint exposes peer-table size, gossip rejection reasons, pull-through \
                 byte volumes, and GC/connection stats — restrict reachability to trusted \
                 scrapers"
            );
        }
    }
    tracing::info!(%addr, "metrics server listening");
    Ok(listener)
}

/// Serve `/metrics` over HTTP on the pre-bound `listener` until `shutdown`
/// fires. The shutdown receiver is consumed; send `()` to stop the accept
/// loop.
///
/// Per-connection tasks are spawned with `tokio::spawn` and **detached** —
/// they are not tracked or awaited during shutdown. In practice scrapes
/// complete in milliseconds, and dropping an in-flight `/metrics` response
/// is harmless (the scraper will retry on its next interval). This is a
/// deliberate choice: tracking an unbounded `JoinSet` alongside the accept
/// loop would add complexity without a consumer that cares about the
/// guarantee.
#[allow(clippy::cognitive_complexity)] // Accept+permit+spawn reads linearly.
pub async fn serve(
    listener: TcpListener,
    metrics: Arc<Metrics>,
    mut shutdown: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let limiter = Arc::new(Semaphore::new(MAX_METRICS_CONNECTIONS));

    loop {
        let (stream, peer) = tokio::select! {
            biased;
            _ = &mut shutdown => {
                tracing::debug!("metrics server shutdown signal received");
                return Ok(());
            }
            res = listener.accept() => match res {
                Ok(pair) => pair,
                Err(err) => {
                    tracing::warn!(%err, "metrics accept failed");
                    continue;
                }
            },
        };

        let Ok(permit) = Arc::clone(&limiter).try_acquire_owned() else {
            tracing::warn!(
                %peer,
                limit = MAX_METRICS_CONNECTIONS,
                "metrics connection rejected: at capacity",
            );
            drop(stream);
            continue;
        };

        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let _permit = permit; // released when task finishes
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                let metrics = Arc::clone(&metrics);
                async move { handle(req, metrics) }
            });
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await
            {
                tracing::debug!(%err, "metrics connection ended");
            }
        });
    }
}

#[allow(clippy::unnecessary_wraps, clippy::needless_pass_by_value)] // hyper service_fn signature requires Result and owned req.
fn handle(
    req: Request<hyper::body::Incoming>,
    metrics: Arc<Metrics>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    if req.uri().path() != "/metrics" {
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from_static(b"not found\n")))
            .unwrap_or_else(|_| Response::new(Full::new(Bytes::new()))));
    }

    match metrics.encode() {
        Ok(body) => Ok(Response::builder()
            .status(StatusCode::OK)
            .header(
                "content-type",
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )
            .body(Full::new(Bytes::from(body)))
            .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))),
        Err(err) => {
            tracing::warn!(%err, "metrics encode error");
            Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::new(Bytes::from_static(b"encode error\n")))
                .unwrap_or_else(|_| Response::new(Full::new(Bytes::new()))))
        }
    }
}

/// RAII guard for the `active_connections` gauge.
///
/// Increments the gauge on construction, decrements on drop — so the count
/// stays correct even if the handler future is cancelled (e.g. during
/// shutdown) between open and close.
#[derive(Debug)]
pub struct ConnectionGuard<'a> {
    metrics: &'a Metrics,
}

impl<'a> ConnectionGuard<'a> {
    fn new(metrics: &'a Metrics) -> Self {
        metrics.connection_opened();
        Self { metrics }
    }
}

impl Drop for ConnectionGuard<'_> {
    fn drop(&mut self) {
        self.metrics.connection_closed();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// Match an exact `<name> <value>` metric line, anchored against
    /// surrounding lines so `decdn_cache_hits_total 1` doesn't
    /// accidentally substring-match into a future
    /// `decdn_cache_hits_total_foo` series or the `OpenMetrics`
    /// `_created` companion line.
    fn has_metric_line(text: &str, name: &str, value: u64) -> bool {
        let needle = format!("{name} {value}");
        text.lines().any(|l| l == needle)
    }

    #[test]
    fn cache_metrics_counters_start_at_zero() {
        // Pinning down the OpenMetrics shape — a fresh registry must
        // expose the cache counters at zero so dashboards built before
        // any fetch has fired don't render `(no data)`.
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        for name in [
            "decdn_cache_origin_fetches_total",
            "decdn_cache_origin_retry_exhausted_total",
            "decdn_cache_origin_fallback_total",
            "decdn_cache_hits_total",
            "decdn_cache_misses_total",
            "decdn_cache_bytes_returned_total",
            "decdn_cache_pull_through_bytes_total",
            // GC counters (#518). The Rust struct fields are `gc_runs`
            // / `gc_bytes_reclaimed`; the OpenMetrics encoder appends
            // `_total`. Asserting the suffixed forms locks in the
            // exported names — a regression that re-renamed the
            // struct fields to include `_total` would emit
            // `..._total_total`, breaking dashboards/alerts that
            // reference the names below.
            "decdn_cache_gc_runs_total",
            "decdn_cache_gc_bytes_reclaimed_total",
            // Circuit-breaker counters (#963). Auto-exposed via the
            // `MetricsGroup` derive; pin the exported names so dashboards
            // tracking origin-outage load-shed don't silently lose them.
            "decdn_cache_circuit_breaker_trips_total",
            "decdn_cache_circuit_breaker_recoveries_total",
            "decdn_cache_circuit_breaker_short_circuits_total",
        ] {
            assert!(
                has_metric_line(&text, name, 0),
                "counter {name} should be exposed at zero on a fresh registry:\n{text}"
            );
        }
    }

    #[test]
    fn quic_0rtt_metrics_start_at_zero_and_increment() {
        let metrics = Metrics::new();

        // Fresh registry: ADR 015 §Observability metrics exposed at zero
        // so dashboards don't render `(no data)` before the first probe.
        let text = metrics.encode().unwrap();
        for name in [
            "decdn_quic_0rtt_attempts_total",
            "decdn_quic_0rtt_accepted_total",
            "decdn_quic_0rtt_rejected_total",
            "decdn_quic_session_ticket_peers_dropped_total",
        ] {
            assert!(
                has_metric_line(&text, name, 0),
                "0-RTT counter {name} should start at zero:\n{text}"
            );
        }
        assert!(
            has_metric_line(&text, "decdn_quic_session_ticket_cache_size", 0),
            "session-ticket gauge should start at zero:\n{text}"
        );

        metrics.record_0rtt_attempt();
        metrics.record_0rtt_attempt();
        metrics.record_0rtt_accepted();
        metrics.record_0rtt_rejected();

        let text = metrics.encode().unwrap();
        assert!(has_metric_line(&text, "decdn_quic_0rtt_attempts_total", 2));
        assert!(has_metric_line(&text, "decdn_quic_0rtt_accepted_total", 1));
        assert!(has_metric_line(&text, "decdn_quic_0rtt_rejected_total", 1));
    }

    #[test]
    fn voucher_nonce_gap_metric_starts_at_zero_and_increments() {
        // #747. The struct field is `voucher_nonce_gaps`; the OpenMetrics
        // encoder appends `_total`, so the exported name is
        // `decdn_voucher_nonce_gaps_total` — the operator-visible name the
        // `apply_voucher` docs and any alert reference. Asserting the suffixed
        // form locks it in: re-naming the field to include `_total` would emit
        // `..._total_total` (the same footgun the cache GC counters guard
        // against above). The counter is event-scoped — `voucher_nonce_gap()`
        // bumps it once per gapped voucher regardless of gap size — so two
        // calls must read exactly 2.
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_voucher_nonce_gaps_total", 0),
            "voucher nonce-gap counter should be exposed at zero on a fresh registry:\n{text}"
        );

        metrics.voucher_nonce_gap();
        metrics.voucher_nonce_gap();

        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_voucher_nonce_gaps_total", 2),
            "expected 2 gap events (one bump each, not gap-size weighted):\n{text}"
        );
    }

    #[test]
    fn serve_stream_rejected_counters_start_at_zero_and_increment_per_reason() {
        // #876. Each `serve_stream` reject branch maps to a distinct counter
        // because the metrics backend has no per-field labels and the wire
        // `StreamError` deliberately conflates the three `NotFound` reasons.
        // The exported names carry the encoder-appended `_total` suffix. All
        // must be exposed at zero on a fresh registry (so dashboards don't read
        // `(no data)`) and each method must bump exactly its own counter.
        let metrics = Metrics::new();
        let reasons = [
            "decdn_serve_stream_rejected_evicted_since_probe_total",
            "decdn_serve_stream_rejected_cache_miss_total",
            "decdn_serve_stream_rejected_internal_error_total",
            "decdn_serve_stream_rejected_blob_too_large_total",
            "decdn_serve_stream_rejected_unknown_channel_total",
            "decdn_serve_stream_rejected_owner_mismatch_total",
            "decdn_serve_stream_rejected_insufficient_deposit_total",
            "decdn_serve_stream_rejected_unauthorized_origin_total",
            "decdn_serve_stream_rejected_cooperative_close_signed_total",
        ];
        let text = metrics.encode().unwrap();
        for name in reasons {
            assert!(
                has_metric_line(&text, name, 0),
                "reject counter {name} should be exposed at zero on a fresh registry:\n{text}"
            );
        }

        metrics.serve_stream_rejected_evicted_since_probe();
        metrics.serve_stream_rejected_cache_miss();
        metrics.serve_stream_rejected_internal_error();
        metrics.serve_stream_rejected_blob_too_large();
        metrics.serve_stream_rejected_unknown_channel();
        metrics.serve_stream_rejected_owner_mismatch();
        metrics.serve_stream_rejected_insufficient_deposit();
        metrics.serve_stream_rejected_unauthorized_origin();
        metrics.serve_stream_rejected_cooperative_close_signed();

        let text = metrics.encode().unwrap();
        for name in reasons {
            assert!(
                has_metric_line(&text, name, 1),
                "reject counter {name} should read exactly 1 after one bump:\n{text}"
            );
        }
    }

    #[test]
    fn receipt_writes_dropped_metric_starts_at_zero_and_increments() {
        // #803. The struct field is `receipt_writes_dropped`; the OpenMetrics
        // encoder appends `_total`, so the exported name is
        // `decdn_receipt_writes_dropped_total` — the operator-visible name an
        // alert on lost audit records references. Must be exposed at zero on a
        // fresh registry and bump once per dropped receipt.
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_receipt_writes_dropped_total", 0),
            "receipt-write-dropped counter should be exposed at zero on a fresh registry:\n{text}"
        );

        metrics.receipt_write_dropped();

        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_receipt_writes_dropped_total", 1),
            "expected one dropped-receipt event:\n{text}"
        );
    }

    #[test]
    fn settlement_auto_counters_start_at_zero_and_increment() {
        // #742. Both auto-settlement counters must be exposed at zero on a
        // fresh registry (dashboards built before any close don't render
        // `(no data)`) and increment independently — the success counter
        // (`settlement_auto_triggered`) and the failure counter
        // (`settlement_auto_failures`) are deliberately distinct so an operator
        // can compute a fire-vs-secured ratio. The OpenMetrics encoder appends
        // `_total`, so the exported names are the suffixed forms asserted here.
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        for name in [
            "decdn_settlement_auto_triggered_total",
            "decdn_settlement_auto_failures_total",
        ] {
            assert!(
                has_metric_line(&text, name, 0),
                "settlement-auto counter {name} should start at zero:\n{text}"
            );
        }

        metrics.settlement_auto_triggered();
        metrics.settlement_auto_failure();
        metrics.settlement_auto_failure();

        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_settlement_auto_triggered_total", 1),
            "expected 1 secured close:\n{text}"
        );
        assert!(
            has_metric_line(&text, "decdn_settlement_auto_failures_total", 2),
            "expected 2 failed closes:\n{text}"
        );
    }

    #[test]
    fn settlement_finalize_counters_start_at_zero_and_increment() {
        // #810. The `settleChannel` finalization-sweep counters: four
        // `try_settle` outcome arms (ok / two transient / raw reverted), three
        // post-revert resolution counters bumped by `drop_pending_if_finalized`
        // (confirmed_closed / restamped / confirm_failed), and one shared
        // pending-store persist-failure counter. All must be exposed at zero on
        // a fresh registry (dashboards built before any sweep don't render
        // `(no data)`) and increment independently so an operator can tell a
        // flaky-RPC condition (`transient_*`, self-healing) from a raw revert
        // and its benign-vs-degraded resolution. The OpenMetrics encoder
        // appends `_total`, so the exported names are the suffixed forms
        // asserted here; re-naming a field to include `_total` would emit
        // `..._total_total`.
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        for name in [
            "decdn_settlement_finalize_ok_total",
            "decdn_settlement_finalize_transient_send_total",
            "decdn_settlement_finalize_transient_receipt_total",
            "decdn_settlement_finalize_reverted_total",
            "decdn_settlement_finalize_confirmed_closed_total",
            "decdn_settlement_finalize_restamped_total",
            "decdn_settlement_finalize_confirm_failed_total",
            "decdn_settlement_pending_persist_failures_total",
        ] {
            assert!(
                has_metric_line(&text, name, 0),
                "settlement-finalize counter {name} should start at zero:\n{text}"
            );
        }

        // Distinct counts per counter so independence is observable (not a
        // single shared bump): 1 / 2 / 3 / 4 / 5 / 6 / 7 / 8.
        metrics.settlement_finalize_ok();
        for _ in 0..2 {
            metrics.settlement_finalize_transient_send();
        }
        for _ in 0..3 {
            metrics.settlement_finalize_transient_receipt();
        }
        for _ in 0..4 {
            metrics.settlement_finalize_reverted();
        }
        for _ in 0..5 {
            metrics.settlement_finalize_confirmed_closed();
        }
        for _ in 0..6 {
            metrics.settlement_finalize_restamped();
        }
        for _ in 0..7 {
            metrics.settlement_finalize_confirm_failed();
        }
        for _ in 0..8 {
            metrics.settlement_pending_persist_failure();
        }

        let text = metrics.encode().unwrap();
        for (name, want) in [
            ("decdn_settlement_finalize_ok_total", 1),
            ("decdn_settlement_finalize_transient_send_total", 2),
            ("decdn_settlement_finalize_transient_receipt_total", 3),
            ("decdn_settlement_finalize_reverted_total", 4),
            ("decdn_settlement_finalize_confirmed_closed_total", 5),
            ("decdn_settlement_finalize_restamped_total", 6),
            ("decdn_settlement_finalize_confirm_failed_total", 7),
            ("decdn_settlement_pending_persist_failures_total", 8),
        ] {
            assert!(
                has_metric_line(&text, name, want),
                "expected {name} == {want}:\n{text}"
            );
        }
    }

    #[test]
    fn buyer_unilateral_close_and_settle_metrics_start_at_zero_and_increment() {
        // #988/#989. Lock the operator-visible (suffixed) names so an alert on
        // the timeout-vs-RPC-failure distinction stays stable, same posture as
        // the seller settlement-finalize test above.
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        for name in [
            "decdn_buyer_unilateral_close_unreachable_total",
            "decdn_buyer_unilateral_close_rpc_failure_total",
            "decdn_buyer_unilateral_close_ok_total",
            "decdn_buyer_settle_ok_total",
            "decdn_buyer_settle_deferred_total",
        ] {
            assert!(
                has_metric_line(&text, name, 0),
                "buyer close/settle counter {name} should start at zero:\n{text}"
            );
        }

        // Distinct counts so independence is observable: 1 / 2 / 3 / 4 / 5.
        metrics.buyer_unilateral_close_unreachable();
        for _ in 0..2 {
            metrics.buyer_unilateral_close_rpc_failure();
        }
        for _ in 0..3 {
            metrics.buyer_unilateral_close_ok();
        }
        for _ in 0..4 {
            metrics.buyer_settle_ok();
        }
        for _ in 0..5 {
            metrics.buyer_settle_deferred();
        }

        let text = metrics.encode().unwrap();
        for (name, want) in [
            ("decdn_buyer_unilateral_close_unreachable_total", 1),
            ("decdn_buyer_unilateral_close_rpc_failure_total", 2),
            ("decdn_buyer_unilateral_close_ok_total", 3),
            ("decdn_buyer_settle_ok_total", 4),
            ("decdn_buyer_settle_deferred_total", 5),
        ] {
            assert!(
                has_metric_line(&text, name, want),
                "expected {name} == {want}:\n{text}"
            );
        }
    }

    #[test]
    fn settle_party_routes_finalize_signals_to_the_right_family() {
        // #988/#989. The shared settle-finalize state machine is party-agnostic;
        // `SettleParty` is what keeps the buyer self-refund metrics from polluting
        // the revenue-critical seller settlement metrics. Drive every finalize
        // signal for BOTH parties on one registry and assert the split.
        let m = Arc::new(Metrics::new());

        // Seller: full breakdown, each signal to its own counter.
        SettleParty::Seller.finalize_ok(&m);
        SettleParty::Seller.finalize_reverted(&m);
        SettleParty::Seller.finalize_transient_send(&m);
        SettleParty::Seller.finalize_transient_receipt(&m);
        SettleParty::Seller.finalize_confirmed_closed(&m);
        SettleParty::Seller.finalize_restamped(&m);
        SettleParty::Seller.finalize_confirm_failed(&m);
        SettleParty::Seller.pending_persist_failure(&m);

        // Buyer: compact pair. ok + confirmed_closed → buyer_settle_ok (deposit
        // recovered either way); reverted is a no-op (reclassified by a
        // resolution arm); everything else → buyer_settle_deferred.
        SettleParty::Buyer.finalize_ok(&m); // → buyer_settle_ok
        SettleParty::Buyer.finalize_confirmed_closed(&m); // → buyer_settle_ok
        SettleParty::Buyer.finalize_reverted(&m); // → no-op (avoids double count)
        SettleParty::Buyer.finalize_transient_send(&m); // → buyer_settle_deferred
        SettleParty::Buyer.finalize_transient_receipt(&m); // → buyer_settle_deferred
        SettleParty::Buyer.finalize_restamped(&m); // → buyer_settle_deferred
        SettleParty::Buyer.finalize_confirm_failed(&m); // → buyer_settle_deferred
        SettleParty::Buyer.pending_persist_failure(&m); // → buyer_settle_deferred

        let text = m.encode().unwrap();
        for (name, want) in [
            // Seller family: one each.
            ("decdn_settlement_finalize_ok_total", 1),
            ("decdn_settlement_finalize_reverted_total", 1),
            ("decdn_settlement_finalize_transient_send_total", 1),
            ("decdn_settlement_finalize_transient_receipt_total", 1),
            ("decdn_settlement_finalize_confirmed_closed_total", 1),
            ("decdn_settlement_finalize_restamped_total", 1),
            ("decdn_settlement_finalize_confirm_failed_total", 1),
            ("decdn_settlement_pending_persist_failures_total", 1),
            // Buyer family: ok = 2 (ok + confirmed_closed), deferred = 5
            // (transient_send + transient_receipt + restamped + confirm_failed +
            // persist_failure), reverted not counted.
            ("decdn_buyer_settle_ok_total", 2),
            ("decdn_buyer_settle_deferred_total", 5),
        ] {
            assert!(
                has_metric_line(&text, name, want),
                "expected {name} == {want}:\n{text}"
            );
        }
    }

    #[test]
    fn staker_set_watcher_metrics_start_at_zero_and_increment() {
        // #783. The struct field is `staker_set_watcher_restarts`; the
        // OpenMetrics encoder appends `_total`, so the exported name is
        // `decdn_staker_set_watcher_restarts_total` — the operator-visible
        // name any alert references. Asserting the suffixed form locks it in:
        // re-naming the field to include `_total` would emit `..._total_total`
        // (the same footgun the cache GC counters guard against).
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_restarts_total", 0),
            "watcher restart counter should be exposed at zero on a fresh registry:\n{text}"
        );
        // Both gauges are exposed at zero so dashboards don't render `(no
        // data)` before the watcher's first event. `down_seconds` reads 0
        // before any cycle is established (a quiet "not yet up").
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_down_seconds", 0),
            "watcher down-seconds gauge should start at zero:\n{text}"
        );
        assert!(
            has_metric_line(&text, "decdn_staker_set_active_count", 0),
            "active-count gauge should start at zero:\n{text}"
        );

        // The resolve-failure counter (#788) is also exposed at zero.
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_resolve_failures_total", 0),
            "resolve-failure counter should start at zero:\n{text}"
        );

        // Two distinct drift windows (each `backoff_started` is an edge into
        // the error state; `cycle_established` between them closes the first).
        metrics.staker_set_watcher_backoff_started();
        metrics.staker_set_watcher_cycle_established();
        metrics.staker_set_watcher_backoff_started();
        metrics.staker_set_watcher_resolve_failure();
        metrics.staker_set_active_count(7);

        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_restarts_total", 2),
            "expected 2 restart events:\n{text}"
        );
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_resolve_failures_total", 1),
            "expected 1 resolve failure:\n{text}"
        );
        assert!(
            has_metric_line(&text, "decdn_staker_set_active_count", 7),
            "expected active-count gauge to report 7:\n{text}"
        );
    }

    #[test]
    fn staker_set_watcher_down_seconds_reads_zero_across_a_long_healthy_cycle() {
        // CRITICAL-fix regression guard (#788): `down_seconds` measures true
        // downtime, NOT cycle age. A healthy filter stream persists
        // indefinitely, so the gauge must read 0 for the entire life of an
        // established cycle — even when the node has been up (and the cycle
        // live) for a while. We simulate "a while" by backdating the metrics'
        // `started_at` well into the past: under the OLD age-based model the
        // gauge climbed with cycle age and would read ~3600 here; under the
        // downtime model `down_since` is `None`, so it stays 0.
        let mut metrics = Metrics::new();
        metrics.started_at = Instant::now()
            .checked_sub(Duration::from_hours(1))
            .unwrap_or_else(Instant::now);
        metrics.staker_set_watcher_cycle_established();

        let text = metrics.encode().unwrap();
        // Sanity: the node really is "old" (uptime reflects the backdate), so a
        // gauge that tracked age would be non-zero.
        assert!(
            has_metric_line(&text, "decdn_uptime_seconds", 3600),
            "uptime should reflect the backdated start:\n{text}"
        );
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_down_seconds", 0),
            "down-seconds must read 0 across a long healthy cycle (true downtime, not age):\n{text}"
        );

        // An error opens the drift window: down-seconds is now driven by
        // `down_since` (not cycle age). An immediate scrape reads ~0s of
        // *downtime*; backdating `down_since` proves it then climbs.
        metrics.staker_set_watcher_backoff_started();
        if let Ok(mut down_since) = metrics.staker_set_watcher_down_since.lock() {
            *down_since = Instant::now().checked_sub(Duration::from_secs(150));
        }
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_down_seconds", 150),
            "down-seconds should climb to the downtime depth once in backoff:\n{text}"
        );

        // Re-establishing the cycle clears the window back to 0.
        metrics.staker_set_watcher_cycle_established();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_down_seconds", 0),
            "down-seconds should reset to 0 once filters re-establish:\n{text}"
        );
    }

    #[test]
    fn slash_watcher_down_seconds_tracks_true_downtime() {
        // Mirrors the staker-set guard for the slash watcher (#1032): the gauge
        // measures downtime, not cycle age, so a long healthy cycle reads 0, a
        // backoff window climbs, and re-establishing clears it.
        let mut metrics = Metrics::new();
        metrics.started_at = Instant::now()
            .checked_sub(Duration::from_hours(1))
            .unwrap_or_else(Instant::now);
        metrics.slash_watcher_cycle_established();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_slash_watcher_down_seconds", 0),
            "down-seconds must read 0 across a long healthy cycle:\n{text}"
        );

        metrics.slash_watcher_backoff_started();
        if let Ok(mut down_since) = metrics.slash_watcher_down_since.lock() {
            *down_since = Instant::now().checked_sub(Duration::from_secs(150));
        }
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_slash_watcher_down_seconds", 150),
            "down-seconds should climb to the downtime depth once in backoff:\n{text}"
        );
        // The restart counter bumps exactly once per drift window (edge-triggered).
        metrics.slash_watcher_backoff_started();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_slash_watcher_restarts_total", 1),
            "restarts must bump once per drift window, not per call:\n{text}"
        );

        metrics.slash_watcher_cycle_established();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_slash_watcher_down_seconds", 0),
            "down-seconds should reset to 0 once the cycle re-establishes:\n{text}"
        );
    }

    #[test]
    fn session_ticket_gauge_counts_distinct_peers_and_is_idempotent() {
        let metrics = Metrics::new();

        metrics.note_session_ticket_peer([1u8; 32]);
        metrics.note_session_ticket_peer([2u8; 32]);
        // Re-noting the same peer must not double-count (the real rustls
        // cache holds one ticket entry per peer).
        metrics.note_session_ticket_peer([1u8; 32]);

        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_quic_session_ticket_cache_size", 2),
            "expected 2 distinct peers, got:\n{text}"
        );
    }

    #[test]
    fn session_ticket_set_is_bounded_against_unbounded_distinct_peers() {
        // Regression: the insert path is fed by the unauthenticated probe
        // handler, so the tracking set MUST stay bounded under a flood of
        // distinct node ids — not just the gauge value.
        let metrics = Metrics::new();
        for i in 0..(SESSION_TICKET_CACHE_CEILING + 50) {
            let mut id = [0u8; 32];
            let tag = u64::try_from(i).unwrap().to_le_bytes();
            id.iter_mut().zip(tag).for_each(|(dst, src)| *dst = src);
            metrics.note_session_ticket_peer(id);
        }

        let ceiling = u64::try_from(SESSION_TICKET_CACHE_CEILING).unwrap();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_quic_session_ticket_cache_size", ceiling),
            "gauge must saturate at the ceiling, got:\n{text}"
        );
        // The set itself stopped growing at the ceiling (the leak fix),
        // not merely the reported gauge.
        let len = metrics.session_ticket_peers.lock().unwrap().len();
        assert_eq!(len, SESSION_TICKET_CACHE_CEILING);
        // The 50 distinct peers beyond the ceiling were each counted as a
        // drop, so the saturation is observable (not silent).
        assert!(
            has_metric_line(&text, "decdn_quic_session_ticket_peers_dropped_total", 50),
            "expected 50 dropped peers, got:\n{text}"
        );
    }

    #[test]
    fn prefetch_quality_gauges_clamp_and_track_throttle() {
        let metrics = Metrics::new();

        // Non-finite / negative ratios (a poisoned-lock fallback, or arithmetic
        // upstream) must clamp the milli gauge to 0, never emit garbage — this
        // exercises the `!is_finite()` / `<= 0.0` branch of set_prefetch_quality.
        for bad in [f64::NAN, f64::NEG_INFINITY, -1.0] {
            metrics.set_prefetch_quality(bad, false);
            let text = metrics.encode().unwrap();
            assert!(
                has_metric_line(&text, "decdn_prefetch_demand_quality_ratio_milli", 0),
                "ratio {bad} must clamp to 0, got:\n{text}"
            );
        }

        // Healthy ratio scales ×1000, and the throttle gauge transitions
        // 0 -> 1 -> 0 as `throttled` flips — no latch (level-triggered, ADR 022).
        metrics.set_prefetch_quality(0.25, true);
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_prefetch_demand_quality_ratio_milli", 250),
            "0.25 should scale to 250 milli, got:\n{text}"
        );
        assert!(
            has_metric_line(&text, "decdn_prefetch_throttle_active", 1),
            "throttle active should read 1, got:\n{text}"
        );
        metrics.set_prefetch_quality(1.0, false);
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_prefetch_throttle_active", 0),
            "throttle must clear to 0 once recovered (no latch), got:\n{text}"
        );
    }

    #[tokio::test]
    async fn engine_bumps_surface_in_openmetrics_output() {
        use std::sync::Arc;

        use bytes::Bytes;
        use decdn_cache::{
            CacheEngine, Origin, OriginFetch, OriginKind, OriginPullError, PinnedHashes,
            RetryPolicy,
        };
        use iroh_blobs::Hash;

        // Minimal in-memory origin: returns the prearranged payload for
        // its hash, NotFound otherwise. Mirrors the StubOrigin used in
        // crates/cache tests but is local to this integration test so
        // we don't need to expose the cache crate's test fixtures.
        #[derive(Debug)]
        struct StubOrigin {
            data: Bytes,
            hash: Hash,
        }
        impl Origin for StubOrigin {
            fn kind(&self) -> OriginKind {
                OriginKind::Http
            }
            fn fetch(
                &self,
                hash: Hash,
                _max_bytes: u64,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = Result<OriginFetch, OriginPullError>>
                        + Send
                        + '_,
                >,
            > {
                let result = if hash == self.hash {
                    Ok(OriginFetch::found_one_shot(self.data.clone()))
                } else {
                    Ok(OriginFetch::NotFound)
                };
                Box::pin(async move { result })
            }
        }

        let payload = b"hello /metrics integration".to_vec();
        let hash = Hash::new(&payload);
        let stub = StubOrigin {
            data: Bytes::from(payload.clone()),
            hash,
        };

        let metrics = Arc::new(Metrics::new());
        let cache_handle = metrics.cache_metrics();
        let tmp = tempfile::tempdir().unwrap();
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![Arc::new(stub) as Arc<dyn decdn_cache::Origin>],
            10,
            PinnedHashes::empty(),
            RetryPolicy::default(),
            decdn_cache::CircuitBreakerPolicy::default(),
            Some(Arc::clone(&cache_handle)),
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();

        // 1 miss (pull-through) + 1 hit.
        let _ = engine.get(hash).await.unwrap();
        let _ = engine.get(hash).await.unwrap();

        let text = metrics.encode().unwrap();
        let payload_len = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        for (name, expected) in [
            ("decdn_cache_hits_total", 1u64),
            ("decdn_cache_misses_total", 1),
            ("decdn_cache_pull_through_bytes_total", payload_len),
            ("decdn_cache_bytes_returned_total", payload_len * 2),
        ] {
            assert!(
                has_metric_line(&text, name, expected),
                "counter {name} should report {expected} after 1 miss + 1 hit:\n{text}"
            );
        }
    }

    /// `bind` accepts both loopback and non-loopback addresses (the
    /// non-loopback path emits a `WARN` per #579 but does not reject).
    /// We can't easily intercept the tracing emission without a
    /// dedicated capture subscriber, so this is a smoke test of both
    /// branches plus IPv6 loopback — a future refactor that narrowed
    /// the predicate to e.g. `addr.ip() == Ipv4Addr::LOCALHOST` would
    /// regress on `::1` and break here visibly.
    #[tokio::test]
    async fn bind_accepts_loopback_and_warns_on_non_loopback() {
        use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

        // IPv4 loopback: warn-free.
        let v4_loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = bind(v4_loopback).await.unwrap();
        let bound = listener.local_addr().unwrap();
        assert!(
            bound.ip().is_loopback(),
            "IPv4 loopback bind should resolve to a loopback addr: got {bound}"
        );
        drop(listener);

        // IPv6 loopback `::1`: also warn-free. Some hosts disable
        // IPv6; skip rather than fail if the bind itself errors.
        let v6_loopback = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0);
        if let Ok(listener) = bind(v6_loopback).await {
            let bound = listener.local_addr().unwrap();
            assert!(
                bound.ip().is_loopback(),
                "IPv6 loopback bind should resolve to a loopback addr: got {bound}"
            );
        }

        // Unspecified (`0.0.0.0`): allowed, but the bind path WARNs.
        // Bind succeeds (a regression that rejected unspecified
        // would surface as a `bind` error here). `local_addr()`
        // echoes the requested IP so `is_unspecified()` is the
        // direct post-bind assertion.
        let unspecified = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
        let listener = bind(unspecified).await.unwrap();
        let bound = listener.local_addr().unwrap();
        assert!(
            bound.ip().is_unspecified(),
            "0.0.0.0 bind should resolve to the unspecified addr: got {bound}"
        );
        drop(listener);
    }

    #[test]
    fn cache_metrics_handle_shares_atomic_with_registered_group() {
        // Sanity: the Arc<CacheMetrics> handed to the engine must be
        // the same one the registry reads from at scrape time. A bug
        // that built two Arcs would surface as cache bumps never
        // appearing in the scrape output.
        let metrics = Metrics::new();
        let handle = metrics.cache_metrics();
        handle.origin_fetches.inc();
        handle.origin_retry_exhausted.inc();
        handle.origin_fallback.inc();
        handle.hits.inc();
        handle.misses.inc();
        handle.bytes_returned.inc_by(1024);
        handle.pull_through_bytes.inc_by(2048);
        // GC counters (#518). The struct fields are `gc_runs` /
        // `gc_bytes_reclaimed`; bumping them here and asserting the
        // `..._total`-suffixed exported names round-trip locks in the
        // encoder behavior that motivated the field-name shape.
        handle.gc_runs.inc();
        handle.gc_bytes_reclaimed.inc_by(4096);
        let text = metrics.encode().unwrap();
        for (name, expected) in [
            ("decdn_cache_origin_fetches_total", 1u64),
            ("decdn_cache_origin_retry_exhausted_total", 1),
            ("decdn_cache_hits_total", 1),
            ("decdn_cache_misses_total", 1),
            ("decdn_cache_bytes_returned_total", 1024),
            ("decdn_cache_pull_through_bytes_total", 2048),
            ("decdn_cache_gc_runs_total", 1),
            ("decdn_cache_gc_bytes_reclaimed_total", 4096),
        ] {
            assert!(
                has_metric_line(&text, name, expected),
                "counter {name} should report {expected}:\n{text}"
            );
        }
    }

    #[test]
    fn channel_open_failures_by_reason_label_distinct_counters() {
        // The three buyer `openChannel` failure classes (#966) must each land
        // in their own `decdn_channel_open_failures_{reason}_total` sibling
        // counter — that label split is the whole point of the issue, so a
        // bump on one reason must NOT leak into another.
        let metrics = Metrics::new();

        // Fresh registry: every reason exposed at zero so dashboards don't
        // render `(no data)` before the first failure.
        let text = metrics.encode().unwrap();
        for name in [
            "decdn_channel_open_failures_insufficient_deposit_total",
            "decdn_channel_open_failures_contract_revert_total",
            "decdn_channel_open_failures_rpc_error_total",
        ] {
            assert!(
                has_metric_line(&text, name, 0),
                "reason counter {name} should start at zero:\n{text}"
            );
        }

        // Bump each reason a distinct number of times so a cross-wired counter
        // is caught by the mismatched count, not just a nonzero value.
        metrics.channel_open_failure_by_reason(ChannelOpenFailureReason::InsufficientDeposit);
        metrics.channel_open_failure_by_reason(ChannelOpenFailureReason::InsufficientDeposit);
        metrics.channel_open_failure_by_reason(ChannelOpenFailureReason::ContractRevert);
        metrics.channel_open_failure_by_reason(ChannelOpenFailureReason::RpcError);
        metrics.channel_open_failure_by_reason(ChannelOpenFailureReason::RpcError);
        metrics.channel_open_failure_by_reason(ChannelOpenFailureReason::RpcError);

        let text = metrics.encode().unwrap();
        for (name, expected) in [
            (
                "decdn_channel_open_failures_insufficient_deposit_total",
                2u64,
            ),
            ("decdn_channel_open_failures_contract_revert_total", 1),
            ("decdn_channel_open_failures_rpc_error_total", 3),
        ] {
            assert!(
                has_metric_line(&text, name, expected),
                "reason counter {name} should report {expected}:\n{text}"
            );
        }

        // The by-reason family is independent of the unlabeled total — bumping
        // a reason does NOT touch `node_pull_channel_open_failures` (that total
        // is bumped separately, and also covers non-tx causes).
        assert!(
            has_metric_line(&text, "decdn_node_pull_channel_open_failures_total", 0),
            "unlabeled total must not move when only the by-reason helper is called:\n{text}"
        );
    }
}
