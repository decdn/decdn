# Appendix: Observability and Metrics

> **This is an appendix, not a core protocol ADR.** Metric implementation is consumer-side — operators choose their own monitoring stack, dashboards, and alerting. This appendix specifies a recommended naming convention, the canonical metric registry, and slash-risk alert thresholds, so that monitoring tooling and operator runbooks can converge on a common vocabulary.

## Context

Metrics are referenced throughout the protocol ADRs and listed informally in `architecture.md § Observability`, but no single document defines: the canonical naming convention; the complete registry of names, types, and labels; which metrics are **mandatory** vs. **recommended**; alert thresholds for slash-risk metrics; and the HTTP export format and endpoint contract. Without this, operators cannot build dashboards, detect slashable conditions before they occur, or compare metrics across nodes — instrumentation becomes ad-hoc.

### Note on existing ADR names

Several ADRs reference informal metric names (e.g., `gossip_messages_rejected_clock_skew`, `probe_hold_violations`, `blacklist_sync_lag_seconds` — from ADRs 001, 005, 011). This ADR is the authoritative canonical registry; the names below supersede those informal references. The changes are purely naming — the semantic intent is unchanged.

## Decision

### 1. Naming Convention

All metrics use the `decdn_` prefix, snake_case, and Prometheus-standard unit suffixes:

| Pattern | Example | Rule |
|---------|---------|------|
| `decdn_{subsystem}_{noun}_{unit}` | `decdn_cache_bytes` | Gauge: descriptive noun + unit |
| `decdn_{subsystem}_{noun}_total` | `decdn_streams_completed_total` | Counter: always `_total` suffix |
| `decdn_{subsystem}_{noun}_seconds` | `decdn_probe_collection_latency_seconds` | Histogram/Summary: `_seconds` for duration |

Label names: snake_case, no abbreviations. Label values: lowercase where possible.

All metrics are exported in **Prometheus text format 0.0.4** on a configurable HTTP port (default `9090`) at `/metrics`. The same port exposes `/health` (see [Health Endpoint](#3-health-endpoint)). The port MUST be operator-configurable and MUST NOT be publicly accessible without authentication in production (firewall or auth proxy).

### 2. Metric Registry

Metrics are grouped into **mandatory** (M) and **recommended** (R) tiers.

**Mandatory (M):** The node MUST expose these or refuse to start. They cover slash-risk conditions and delivery accountability.

**Recommended (R):** The node SHOULD expose these. Absence is not a startup blocker, but operators lose subsystem visibility.

#### 2.1 Slash-Safety Metrics (all Mandatory)

Early warning for the five slashable offenses in [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn). A sustained non-zero value for any of these requires immediate operator attention.

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_probe_hold_violations_total` | Counter | M | Blob evicted within `probe_hold_duration` after signing `has_blob: true`. Each increment is a signed phantom-announcement slash risk ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)). |
| `decdn_probe_hold_slots_used` | Gauge | M | Eviction-hold slots in use out of `max_probe_holds`. Saturation forces `has_blob: false` at probe time. |
| `decdn_probe_hold_slots_max` | Gauge | M | Configured `max_probe_holds`. Paired with `decdn_probe_hold_slots_used` for a saturation ratio. |
| `decdn_blacklist_sync_lag_seconds` | Gauge | M | Seconds since the last successful `getBlacklistVersion()` poll. Exceeding the compliance window makes serving any recently-blacklisted hash slashable ([ADR 011](011-content-takedown.md)). |
| `decdn_blacklist_version_behind` | Gauge | M | `on_chain_version − local_version`. Positive means new blacklist entries not yet fetched. |
| `decdn_rate_bounds_clamp_events_total` | Counter | M | Times `rate_per_mb` was clamped to governance bounds before signing a `ProbeResponse` — configured rate is outside the current governance window ([ADR 003](003-payments.md), [ADR 005](005-protocol.md)). |
| `decdn_slash_evidence_exposure_total` | Counter | M | Self-detected `has_blob: true` probe followed by a stream response within the 30-second slashing window — valid phantom slash evidence ([ADR 005](005-protocol.md)). Non-zero is a critical bug signal. |

**Recommended alert thresholds:**

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| `decdn_probe_hold_violations_total` (rate) | > 0 | > 0 sustained | Reduce load or increase `max_probe_holds`; check for OOM. |
| `decdn_blacklist_sync_lag_seconds` | > 600s (1 poll interval) | > 1800s | Check RPC provider; manual sync if needed. |
| `decdn_blacklist_version_behind` | > 0 | > 1 | Investigate RPC / poll failure. |
| `decdn_rate_bounds_clamp_events_total` (rate) | > 0 | — | Update `rate_per_mb` config to within governance bounds. |
| `decdn_slash_evidence_exposure_total` (rate) | — | > 0 | File a bug; stop node immediately if rate is sustained. |

#### 2.2 Delivery Metrics (`cdn/client/v1`)

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_streams_active` | Gauge | M | `direction={inbound,outbound}` | Currently open delivery streams. |
| `decdn_streams_completed_total` | Counter | M | `direction={inbound,outbound}` | Successfully completed streams. |
| `decdn_streams_failed_total` | Counter | M | `direction, reason` | Failed streams. `reason` values: `hash_mismatch`, `channel_insufficient`, `rate_mismatch`, `blob_too_large`, `evicted`, `timeout`, `protocol_error`, `other`. |
| `decdn_bytes_served_total` | Counter | M | — | Bytes delivered to clients and downstream nodes (inbound streams from the requester's perspective). |
| `decdn_bytes_received_total` | Counter | M | — | Bytes received as a client in node-to-node cache-miss pulls. |

#### 2.3 Cache Metrics

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_cache_bytes` | Gauge | M | Total cache size in bytes (all blobs). Paired with `decdn_cache_size_limit_bytes` for a saturation ratio. |
| `decdn_cache_size_limit_bytes` | Gauge | R | Configured cache capacity in bytes (`cache.cache_size_mb × 1 048 576`). See [appendix-blob-cache-eviction.md](appendix-blob-cache-eviction.md). |
| `decdn_cache_hits_total` | Counter | M | Probe or stream requests satisfied from local cache. |
| `decdn_cache_misses_total` | Counter | M | Probe or stream requests requiring origin pull or peer pull. |
| `decdn_cache_bytes_returned_total` | Counter | R | Bytes returned from `CacheEngine::get` to the caller on success. Counts both cache-hit and pull-through-success paths. |
| `decdn_cache_pull_through_bytes_total` | Counter | R | Bytes received from origin during a cache miss, counted regardless of BLAKE3 verification or store landing — origin egress is paid either way. Independent of `decdn_cache_bytes_returned_total`: equal values mean pure pass-through; `bytes_returned_total >> pull_through_bytes_total` indicates effective caching. |
| `decdn_cache_evictions_total` | Counter | R | Blobs evicted by LRU pressure (eviction-driver loop). See [appendix-blob-cache-eviction.md](appendix-blob-cache-eviction.md). |
| `decdn_cache_evicted_operator_total` | Counter | R | Hashes removed via `decdn node evict` (durable, persisted to `<cache_dir>/evicted.log`). Distinct from `decdn_cache_evictions_total`. See [appendix-blob-cache-eviction.md §3](appendix-blob-cache-eviction.md#3-operator-evict-is-orthogonal-to-lru). |
| `decdn_cache_pinned_count` | Gauge | R | Size of the operator-pinned set (LRU-exempt). See [appendix-blob-cache-eviction.md §2](appendix-blob-cache-eviction.md#2-operator-pinning-overrides-lru). |
| `decdn_probe_post_eviction_failures_total` | Counter | R | `EvictedSinceProbe` responses from remote nodes during cache-hit stream requests. A sustained rate above ~1% of cache-hit attempts suggests remote hold mechanism failures ([ADR 001](001-network.md), [ADR 005](005-protocol.md)). |

#### 2.4 Probe Metrics (`cdn/probe/v1`)

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_probe_collection_latency_seconds` | Histogram | M | `outcome={0rtt_warm,1rtt_cold}` | Duration of a complete probe collection window, send to collection end. The `outcome` label measures 0-RTT impact per [ADR 015](015-zero-rtt.md). Buckets: `[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]`. |
| `decdn_probe_responses_total` | Counter | R | `result={has_blob,no_blob,timeout}` | Probe responses received, by result. |

#### 2.5 Payment Channel Metrics

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_channels_open` | Gauge | M | Currently open payment channels (as node — inbound from clients). |
| `decdn_channels_settled_total` | Counter | M | Channels settled on-chain. |
| `decdn_channel_deposit_usdc` | Gauge | M | Total USDC deposited across all currently open inbound channels. Represents maximum on-chain recoverable value. |
| `decdn_vouchers_signed_total` | Counter | M | Vouchers signed by this node as the payee. |
| `decdn_vouchers_received_total` | Counter | R | Vouchers received by this node as the payer (node-to-node pulls). |
| `decdn_channel_disputes_total` | Counter | R | Channels that entered the dispute window. |

#### 2.6 Gossip Metrics

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_gossip_messages_rejected_total` | Counter | M | `reason={clock_skew,invalid_signature,not_registered,stale_timestamp,invalid_region,duplicate_hashes,table_full}` | Gossip messages rejected during validation ([ADR 001](001-network.md#gossip-validation)) or peer-table admission ([appendix-peer-table-eviction.md](appendix-peer-table-eviction.md)). `reason=clock_skew` is the canonical replacement for ADR 001's `gossip_messages_rejected_clock_skew`. `table_full` fires only when the optional `gossip.max_peer_entries` ceiling is set and exceeded. |
| `decdn_peer_table_size` | Gauge | M | — | Number of distinct peers in the local peer table. |
| `decdn_peer_table_evicted_ttl_total` | Counter | R | — | Peer-table entries removed by the TTL sweeper ([appendix-peer-table-eviction.md §1](appendix-peer-table-eviction.md#1-lifecycle-and-ttl)). |
| `decdn_peer_table_evicted_registry_total` | Counter | R | `reason={deregistered,ejected}` | Peer-table entries removed in response to a `NodeDeregistered` or `NodeAutoEjected` registry event ([appendix-peer-table-eviction.md §3](appendix-peer-table-eviction.md#3-registry-cache-interaction-active-eviction)). |
| `decdn_gossip_announces_sent_total` | Counter | R | — | `NodeAnnounce` messages published. |
| `decdn_gossip_announces_received_total` | Counter | R | — | `NodeAnnounce` messages accepted (passed validation). |
| `decdn_gossip_subscriber_reconnections_total` | Counter | R | — | Successful subscriber reconnections after a gossip stream drop. |

#### 2.7 Reputation Metrics

The metrics below apply once the reputation gossip layer in [ADR 008](008-reputation.md) is implemented; local-only-scoring nodes expose only `decdn_reputation_score`.

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_reputation_reports_sent_total` | Counter | R | `ReputationReport` messages published to `cdn/reputation/v1`. |
| `decdn_reputation_reports_received_total` | Counter | R | `ReputationReport` messages accepted from peers. |
| `decdn_reputation_score` | Gauge | R | This node's current `final_score` (0.0–1.0) as computed locally — local observations (70%) + gossip (30%) per [ADR 008](008-reputation.md). |

#### 2.8 QUIC / 0-RTT Metrics

Per [ADR 015](015-zero-rtt.md). All labeled by `alpn`.

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_quic_0rtt_attempts_total` | Counter | R | 0-RTT connection attempts. |
| `decdn_quic_0rtt_accepted_total` | Counter | R | 0-RTT connections accepted by server. |
| `decdn_quic_0rtt_rejected_total` | Counter | R | 0-RTT rejected, fell back to 1-RTT. |

These replace the identical names from ADR 015 — no semantic change, now under the canonical naming regime.

#### 2.9 Node / Process Metrics

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_node_uptime_seconds` | Gauge | R | Seconds since the node process started. Used by the `/health` endpoint and operator dashboards to correlate events with restarts. |

#### 2.10 Tokenomics Metrics

Per [ADR 026](026-gauge-boost-tokenomics.md). These metrics expose the `FeeRouter`, `VotingEscrow`, and `SafetyReserve` contract surfaces to operator dashboards, keeper monitoring, gauge-claim debugging, governance dashboards, and the public reporting required by the `SafetyReserve` transparency rules ([ADR 026 §5](026-gauge-boost-tokenomics.md), [ADR 009](009-governance.md)).

A subset is sourced from on-chain contract state (`FeeRouter`, `VotingEscrow`, `SafetyReserve`, `BuybackBurner` / `DelegatorBuyer`) via the same RPC client used for blacklist polling and channel-state queries ([ADR 011](011-content-takedown.md), [ADR 003](003-payments.md)). They use the **same Prometheus text-format `/metrics` endpoint, scrape interval, and retention defaults** defined in Section 1 — no separate export pipeline. Contract-sourced gauges sample at the existing RPC-poll cadence; counters tracking on-chain events advance only when the node observes the corresponding event log.

##### 2.10.1 FeeRouter Metrics

| Metric | Type | Tier | Labels | Source | Consumer | Description |
|--------|------|------|--------|--------|----------|-------------|
| `decdn_fee_router_inflow_usdc_total` | Counter | R | `bucket={node_base,gauge,delegator,burn,treasury,safety}` | `FeeRouter` settlement events (RPC) | Operator + governance dashboards | Cumulative per-bucket USDC inflow at `FeeRouter.routeSettlement`. Rate of change gives the per-bucket inflow rate ([ADR 026 §2](026-gauge-boost-tokenomics.md)). |
| `decdn_fee_router_inflow_usdc_rate` | Gauge | R | `bucket={node_base,gauge,delegator,burn,treasury,safety}` | Derived (rolling 7-epoch avg over `..._inflow_usdc_total`) | Governance + capacity-planning dashboards | Rolling-average per-bucket USDC inflow per epoch. Computed locally from the counter. |
| `decdn_gauge_pool_distribution_usdc_total` | Counter | R | `phase={inflow,claimed,swept_to_treasury}` | `FeeRouter` events: bucket inflow, `claimBoost`, sweep-on-26-epoch-timeout | Gauge-claim debugging + treasury reporting | Per-epoch gauge-pool USDC lifecycle: inflow, disbursed via `claimBoost`, swept to treasury on the 26-epoch unclaimed timeout ([ADR 026 §2](026-gauge-boost-tokenomics.md)). |
| `decdn_pool_swept_to_treasury_usdc_total` | Counter | R | `pool={gauge,delegator}` | `FeeRouter` sweep events | Treasury + governance dashboards | Cumulative unclaimed-after-26-epochs sweep volume per pool. Differs from `..._gauge_pool_distribution_..._{phase=swept_to_treasury}` only in covering both pools; the gauge-pool label remains the canonical gauge-specific view. |

##### 2.10.2 Operator Gauge-Boost Metrics

| Metric | Type | Tier | Labels | Source | Consumer | Description |
|--------|------|------|--------|--------|----------|-------------|
| `decdn_operator_working_bytes` | Gauge | R | `epoch` | `FeeRouter.workingBytes(operator, epoch)` (RPC) | Operator dashboard, gauge-claim debugging | This node's `working_bytes_i` for the labeled epoch, per the gauge formula ([ADR 026 §3](026-gauge-boost-tokenomics.md)). Explains why a claim payout is what it is. |
| `decdn_operator_bytes_delivered` | Gauge | R | `epoch` | `FeeRouter.bytesDelivered(operator, epoch)` (RPC) | Operator dashboard | Raw `bytes_i` for the labeled epoch — paired with `decdn_operator_working_bytes` to derive the boost factor. |
| `decdn_operator_boost_factor` | Gauge | R | `epoch` | Derived (`working_bytes / bytes_delivered`) | Operator dashboard, UI "your current boost" surface | Effective boost factor for the labeled epoch, in `[boostFloor, 1.0]` (default `[0.4, 1.0]`). At `boostFloor` for a zero-ve operator; `1.0` for a fair-share-or-higher ve operator. |

##### 2.10.3 Delegator-Pool Execution Metrics

| Metric | Type | Tier | Labels | Source | Consumer | Description |
|--------|------|------|--------|--------|----------|-------------|
| `decdn_delegator_swap_slippage_bps` | Histogram | R | — | `DelegatorBuyer` (or `BuybackBurner` multi-output mode) swap events | Keeper monitoring, MEV-defense review | Realized TWAP slippage on the per-epoch USDC→TOKEN swap, in basis points below the swap's `minOut` reference. Buckets: `[1, 5, 10, 25, 50, 100, 250, 500]`. Sustained tail = MEV / liquidity-cap pressure ([ADR 026 §6](026-gauge-boost-tokenomics.md), [ADR 018](018-liquidity-strategy.md)). |
| `decdn_delegator_liquidity_cap_utilization` | Gauge | R | — | Derived (`swap_size_usdc / per_epoch_cap_usdc`) | Keeper monitoring, governance dashboard | Per-epoch liquidity-cap utilization in `[0, 1]`. Sustained `≥ 1.0` means the cap is binding and excess delegator-pool USDC rolls forward — feeds the cap-resize discussion. |
| `decdn_delegator_usdc_to_token_ratio` | Gauge | R | `epoch` | Derived (`token_acquired / usdc_spent`) | Governance + delegator-yield dashboards | Per-epoch ratio of TOKEN distributed to delegators against USDC acquired into the bucket. Tracks market-price drift and aggregate swap quality across the epoch. |

##### 2.10.4 SafetyReserve Metrics

| Metric | Type | Tier | Labels | Source | Consumer | Description |
|--------|------|------|--------|--------|----------|-------------|
| `decdn_safety_reserve_balance_usdc` | Gauge | R | — | `SafetyReserve.balance()` (RPC) | Public dashboard, governance, enterprise-tier credibility | Current USDC balance of the `SafetyReserve` contract. Public-transparency requirement per [ADR 026 §5](026-gauge-boost-tokenomics.md). |
| `decdn_safety_reserve_incidents` | Gauge | R | `state={pending,approved,disputed}` | `SafetyReserve` incident-registry (RPC) | Public incident registry, governance dashboard | Incident count per registry state. `pending` = bundle filed, awaiting governance/multisig action; `approved` = approved for payout (within or after 48h appeal window); `disputed` = under on-chain challenge. |
| `decdn_safety_reserve_payouts_total` | Counter | R | — | `SafetyReserve.payout` events | Public registry, governance reporting | Cumulative executed payouts across the reserve's lifetime. |
| `decdn_safety_reserve_outflow_usdc_total` | Counter | R | — | `SafetyReserve.payout` events | Public registry, governance reporting | Cumulative USDC paid out across all approved incidents. |

##### 2.10.5 VotingEscrow Metrics

| Metric | Type | Tier | Labels | Source | Consumer | Description |
|--------|------|------|--------|--------|----------|-------------|
| `decdn_ve_total_supply` | Gauge | R | — | `VotingEscrow.totalSupply()` (RPC) | Governance dashboard, gauge-share denominator sanity-check | Current total ve-supply (sum of ve-balances across all live locks). |
| `decdn_ve_lock_rate` | Gauge | R | — | Derived (`TOKEN.balanceOf(address(VotingEscrow)) / TOKEN.totalSupply()`) | Governance dashboard, adaptive-feedback heuristic input | Fraction of TOKEN supply locked in `VotingEscrow`, in `[0, 1]`. The canonical numerator is the underlying TOKEN balance held by the escrow — `TOKEN.balanceOf(address(VotingEscrow))` — **not** the time-weighted ve-supply from `VotingEscrow.totalSupply()` / `totalSupplyAt(...)`. Every consumer (governance dashboards, automated controllers) MUST key off this same underlying-locked definition; ve-supply has different units. |
| `decdn_ve_lock_duration_median_seconds` | Gauge | R | — | `VotingEscrow` per-lock checkpoint scan (RPC) | Governance dashboard, ve-economy health view | Median remaining lock duration across all live locks, in seconds. Distribution-shape signal complementing aggregate `decdn_ve_total_supply` and `decdn_ve_lock_rate`. |

#### 2.11 DHT / Content-Discovery Metrics

Per [ADR 022](022-content-discovery.md) (`cdn/dht/v1`). DHT STORE and FIND_VALUE carry no protocol-level fee; these metrics expose discovery health only.

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_dht_store_published_total` | Counter | R | — | DHT STORE records this node published to the K-closest peers ([ADR 022](022-content-discovery.md)). |
| `decdn_dht_findvalue_queries_total` | Counter | R | — | DHT FIND_VALUE lookups this node issued to discover providers. |
| `decdn_dht_routing_table_size` | Gauge | R | — | Distinct entries in the local Kademlia routing table. |

### 3. Health Endpoint

`GET /health` (same HTTP port as `/metrics`) returns a JSON object:

```json
{
  "status": "ready" | "degraded" | "not_ready",
  "node_id": "<hex iroh NodeId>",
  "registry_active": true,
  "blacklist_version": 42,
  "blacklist_synced": true,
  "rate_bounds_loaded": true,
  "peer_table_size": 12,
  "channels_open": 3,
  "channel_deposit_usdc": "15.23",
  "node_uptime_seconds": 3601
}
```

**JSON key → Prometheus metric mapping:**

| JSON key | Prometheus metric | Notes |
|----------|------------------|-------|
| `peer_table_size` | `decdn_peer_table_size` | Direct gauge value |
| `channels_open` | `decdn_channels_open` | Direct gauge value |
| `channel_deposit_usdc` | `decdn_channel_deposit_usdc` | Formatted as decimal string for readability; metric stores raw value |
| `node_uptime_seconds` | `decdn_node_uptime_seconds` | Direct gauge value |
| `blacklist_version` | `decdn_blacklist_version_behind` (derived) | Absolute version number from RPC, not the lag gauge |

**Status semantics:**

| `status` | Meaning |
|----------|---------|
| `ready` | All Phase 5 acceptance criteria satisfied ([ADR 019](019-node-onboarding.md#phase-5-accepting-paid-delivery)); serving traffic. |
| `degraded` | Running but one or more non-critical conditions impaired (e.g., gossip mesh thin, 0-RTT cache cold). Traffic still accepted. |
| `not_ready` | A mandatory startup check failed or is incomplete (blacklist un-synced, rate bounds not loaded, not registered). Not accepting traffic. |

HTTP status codes: `200` for `ready` and `degraded`; `503` for `not_ready`. Monitoring systems SHOULD alert on `503` responses.

### 4. Structured Logging

Metrics cover aggregates; structured logs cover per-event detail. Complementary — logs are not a substitute for metrics.

- **Library:** `tracing` crate (standard in the iroh ecosystem).
- **Format:** JSON (`tracing-subscriber` `json` formatter) for production machine consumption. Human-readable (`pretty`) available via config flag for local development.
- **Log levels:**
  - `ERROR` — unrecoverable, needs operator intervention (startup failures, slash-evidence exposure, RPC unreachable after all retries).
  - `WARN` — recoverable degraded conditions (blacklist poll lag > 1 interval, startup clock skew > 10 s, probe hold slot saturation > 90%).
  - `INFO` — significant lifecycle events (node ready, channel opened/settled, peer joined/left, `NodeAnnounce` published).
  - `DEBUG` — per-stream and per-probe events. Not for high-volume production.

**Mandatory log fields** on every event:

- `node_id` — iroh NodeId (hex)
- `ts` — RFC 3339 timestamp
- `level` — log level
- `target` — Rust module path

### 5. Canonical Metric Name Cross-Reference

Earlier ADRs used informal metric names; this table maps them to canonical replacements. **No wire protocol or on-chain change** — instrumentation names only.

| Informal name (prior ADR) | Canonical name (this ADR) | Source ADR |
|---------------------------|---------------------------|------------|
| `gossip_messages_rejected_clock_skew` | `decdn_gossip_messages_rejected_total{reason="clock_skew"}` | ADR 001 |
| `probe_hold_violations` | `decdn_probe_hold_violations_total` | ADR 005, architecture.md |
| `probe_hold_slots_used` | `decdn_probe_hold_slots_used` | ADR 005, architecture.md |
| `rate_bounds_clamp_events` | `decdn_rate_bounds_clamp_events_total` | architecture.md |
| `blacklist_sync_lag_seconds` | `decdn_blacklist_sync_lag_seconds` | ADR 011 |
| `blacklist_version_behind` | `decdn_blacklist_version_behind` | ADR 011 |
| `slash_evidence_exposure` | `decdn_slash_evidence_exposure_total` | architecture.md |
| `quic_0rtt_attempts_total` | `decdn_quic_0rtt_attempts_total` | ADR 015 |
| `quic_0rtt_accepted_total` | `decdn_quic_0rtt_accepted_total` | ADR 015 |
| `quic_0rtt_rejected_total` | `decdn_quic_0rtt_rejected_total` | ADR 015 |
| `probe_collection_latency_seconds` | `decdn_probe_collection_latency_seconds` | ADR 015 |
| `streams_active` | `decdn_streams_active` | architecture.md |
| `streams_completed` | `decdn_streams_completed_total` | architecture.md |
| `streams_failed` | `decdn_streams_failed_total` | architecture.md |
| `vouchers_signed` | `decdn_vouchers_signed_total` | architecture.md |
| `vouchers_received` | `decdn_vouchers_received_total` | architecture.md |
| `reputation_reports_sent` | `decdn_reputation_reports_sent_total` | architecture.md |
| `reputation_reports_received` | `decdn_reputation_reports_received_total` | architecture.md |
| `channels_open` | `decdn_channels_open` | architecture.md |
| `channels_settled` | `decdn_channels_settled_total` | architecture.md |
| `cache_hits` | `decdn_cache_hits_total` | architecture.md |
| `cache_misses` | `decdn_cache_misses_total` | architecture.md |
| `cache_bytes` | `decdn_cache_bytes` | architecture.md |

### 6. Reference dashboards and alerts

A reference Grafana dashboard and starter Prometheus alerting rules ship in the top-level [`monitoring/`](../monitoring/) directory. They consume only canonical §2 metric names — no new metrics — and are an onboarding starting point, not a normative deliverable.

| File | Purpose |
|------|---------|
| [`monitoring/prometheus-alerts.yml`](../monitoring/prometheus-alerts.yml) | Three rule groups: `decdn-slash-safety` (thresholds copied verbatim from §2.1), `decdn-liveness`, `decdn-delivery`. |
| [`monitoring/grafana-dashboard.json`](../monitoring/grafana-dashboard.json) | Single overview dashboard (`uid: decdn-poc-overview`); rows for health, slash safety, delivery, cache, probes, payments. Datasource parameterised via `${DS_PROMETHEUS}`; node selection via the `instance` template variable. |

#### Importing the dashboard

In Grafana, *Dashboards → New → Import*; upload or paste the JSON. Select your Prometheus datasource at the prompt; the `instance` variable auto-populates from `decdn_node_uptime_seconds`.

#### Using the alerts

Add the file via Prometheus `rule_files:` and reload. Validate with `promtool check rules monitoring/prometheus-alerts.yml`. Tune `for:` durations and thresholds for your fleet size before paging.

#### Scope

Covers M-tier slash-safety metrics and the most common R-tier panels for a first dashboard. Deliberately not exhaustive: §2.10 tokenomics, reputation, and 0-RTT panels are left to deployment-specific dashboards.

## Consequences

### Positive

- Single reference for dashboard configuration — no hunting across 8 ADRs for metric names.
- Mandatory M-tier slash-risk metrics enforced at startup, so operators cannot accidentally run without slash-risk visibility.
- Canonical `decdn_` prefix and `_total` suffix allow automated registry validation (e.g., a CI check that exported names match the registry).
- Alert thresholds provide actionable defaults for new operators.
- The `/health` endpoint integrates with standard load balancers and container readiness probes without parsing Prometheus text.

### Negative

- Existing ADRs reference informal names differing from the canonical ones here. The cross-reference table (Section 6) documents all renames; no ADR is retroactively edited (avoids draft-document churn), but implementations must use this ADR's canonical names.
- Mandatory metrics add startup complexity — all M-tier collectors must initialize before accepting connections. Small overhead for guaranteed observability.

## Deferred & Open

- **OpenMetrics migration.** Prometheus text format 0.0.4 is the current default; the OpenMetrics exposition format (used by `prometheus_client` crate's `MetricsEncoder`) adds exemplars and native histograms — evaluate once tooling support is broader.
- **Tokenomics + reputation dashboard panels.** The reference dashboard in [`monitoring/grafana-dashboard.json`](../monitoring/grafana-dashboard.json) is deliberately scoped to M-tier core operations. §2.7 reputation and §2.10 tokenomics metrics warrant their own dedicated dashboards (governance, delegator-yield, gauge-claim debugging) — these are deployment-specific and belong outside the reference set.
