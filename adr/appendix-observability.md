# Appendix: Observability and Metrics

> **This is an appendix, not a core protocol ADR.** Metric implementation is consumer-side. Operators choose their own monitoring stack, dashboards, and alerting. This appendix specifies a recommended naming convention, the canonical metric registry, and slash-risk alert thresholds. Monitoring tooling and operator runbooks then share a common vocabulary.

## Context

Protocol ADRs reference metrics, and `architecture.md § Observability` lists them informally. No single document defines these:

- the canonical naming convention;
- the complete registry of names, types, and labels;
- which metrics are **mandatory** vs. **recommended**;
- alert thresholds for slash-risk metrics;
- the HTTP export format and endpoint contract.

Without this document, operators cannot build dashboards, detect slashable conditions before they occur, or compare metrics across nodes. Instrumentation becomes ad-hoc.

### Note on existing ADR names

Several ADRs reference informal metric names (e.g., `gossip_messages_rejected_clock_skew`, `probe_hold_violations`, `blacklist_sync_lag_seconds` — from ADRs 001, 005, 011). This appendix is the authoritative canonical registry. The names below are the canonical forms of those informal references, with identical semantic intent.

## Decision

### Naming Convention

All metrics use the `decdn_` prefix, snake_case, and Prometheus-standard unit suffixes:

| Pattern | Example | Rule |
|---------|---------|------|
| `decdn_{subsystem}_{noun}_{unit}` | `decdn_cache_bytes` | Gauge: descriptive noun + unit |
| `decdn_{subsystem}_{noun}_total` | `decdn_streams_completed_total` | Counter: always `_total` suffix |
| `decdn_{subsystem}_{noun}_seconds` | `decdn_probe_collection_latency_seconds` | Histogram/Summary: `_seconds` for duration |

Label names: snake_case, no abbreviations. Label values: lowercase where possible.

All metrics are exported in **Prometheus text format 0.0.4** on a configurable HTTP port (default `9090`) at `/metrics`. The same port exposes `/health` (see [Health Endpoint](#health-endpoint)). The port MUST be operator-configurable and MUST NOT be publicly accessible without authentication in production (firewall or auth proxy).

### Metric Registry

Metrics are grouped into **mandatory** (M) and **recommended** (R) tiers.

**Mandatory (M):** The node MUST expose these or refuse to start. They cover slash-risk conditions and delivery accountability.

**Recommended (R):** The node SHOULD expose these. Absence is not a startup blocker, but operators lose subsystem visibility.

#### Slash-Safety Metrics (all Mandatory)

These give early warning for the two slashable offenses in [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn), grouped here with the closely-related probe-hold capacity metrics. A sustained non-zero value for the blacklist-lag **counters** requires immediate operator attention. The **gauges** (`decdn_probe_hold_slots_used`/`_max`, `decdn_blacklist_version_behind`) are normally non-zero — alert on the thresholds/rates in the table below, not on presence. `decdn_probe_hold_unavailable_total` is an availability signal, not a slash risk — see its row, and alert only on `reason="exhausted"`, the budget-pressure value.

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_probe_hold_unavailable_total{reason}` | Counter | M | A probe that could not be answered from a guaranteed eviction hold, so the node signed `has_blob: false` ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)). Always an availability degradation, never a safety fault — the literal "evicted after signing `has_blob: true`" case is unreachable by construction (held hashes are invisible to the LRU driver). The `reason` label carries the cause, because each has a different operator remedy: **`exhausted`** — blob present but **all** hold slots were live (`max_probe_holds` reached); genuine budget pressure and the only value the "raise `max_probe_holds`" alert fires on. **`disabled`** — blob present but the hold path is **off by config** (`max_probe_holds == 0`); an intentional operator choice, so alerting on it would be nonsensical (#739). **`stake_lane_reserved`** — an end-client probe hit the stake-lane-reserved end-client ceiling (`max_probe_holds − cache.stake_lane_reserved_holds`), keeping headroom for registered node-to-node cache-miss probes (#757, [ADR 003 § Admission and Priority](003-payments.md#admission-and-priority)); fires *before* the hold attempt, so the cache is not consulted — a content-independent admission decision — and stays zero unless `cache.stake_lane_reserved_holds > 0`. All three children are exported at zero from startup, so a missing series means a broken exporter, not an idle node. |
| `decdn_probe_hold_slots_used` | Gauge | M | Eviction-hold slots in use out of `max_probe_holds`. Saturation forces `has_blob: false` at probe time. |
| `decdn_probe_hold_slots_max` | Gauge | M | Configured `max_probe_holds`. Paired with `decdn_probe_hold_slots_used` for a saturation ratio. |
| `decdn_blacklist_sync_lag_seconds` | Gauge | M | Seconds since the last successful `getBlacklistVersion()` poll. Exceeding the compliance window makes serving any recently-blacklisted hash slashable ([ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)). |
| `decdn_blacklist_version_behind` | Gauge | M | `on_chain_version − local_version`. Positive means new blacklist entries not yet fetched. |
| `decdn_rate_bounds_clamp_events_total` | Counter | M | Times `rate_per_mb` was raised to the governance `deliveryFloor` before signing a `ProbeResponse` / `StreamResponse` — the configured rate sits below the current floor ([ADR 003](003-payments.md#adr-003-payment-model), [ADR 005](005-protocol.md#adr-005-wire-protocol)). |

**Recommended alert thresholds:**

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| `decdn_probe_hold_unavailable_total{reason="exhausted"}` (rate) | > 0 | > 0 sustained | Reduce load or increase `max_probe_holds`; check for OOM. Filter on `reason="exhausted"` — the `disabled` and `stake_lane_reserved` values are deliberate operator decisions and must not trip this alert. |
| `decdn_blacklist_sync_lag_seconds` | > 600s (1 poll interval) | > 1800s | Check RPC provider; manual sync if needed. |
| `decdn_blacklist_version_behind` | > 0 | > 1 | Investigate RPC / poll failure. |
| `decdn_rate_bounds_clamp_events_total` (rate) | > 0 | — | Raise the `rate_per_mb` config to at least the governance `deliveryFloor`. |

#### Delivery Metrics (`cdn/client/v1`)

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_streams_active` | Gauge | M | `direction={inbound,outbound}` | Currently open delivery streams. |
| `decdn_streams_completed_total` | Counter | M | `direction={inbound,outbound}` | Successfully completed streams. |
| `decdn_streams_failed_total` | Counter | M | `direction, reason` | Failed streams. `reason` values: `hash_mismatch`, `channel_insufficient`, `rate_mismatch`, `blob_too_large`, `evicted`, `timeout`, `protocol_error`, `other`. |
| `decdn_bytes_served_total` | Counter | M | — | Bytes delivered to clients and downstream nodes (inbound streams from the requester's perspective). |
| `decdn_bytes_received_total` | Counter | M | — | Bytes received as a client in node-to-node cache-miss pulls. |

#### Cache Metrics

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_cache_bytes` | Gauge | M | Total cache size in bytes (all blobs). Paired with `decdn_cache_size_limit_bytes` for a saturation ratio. |
| `decdn_cache_size_limit_bytes` | Gauge | R | Configured cache capacity in bytes (`cache.cache_size_mb × 1 048 576`). See [appendix-blob-cache-eviction.md](appendix-blob-cache-eviction.md#appendix-blob-cache-eviction-policy). |
| `decdn_cache_hits_total` | Counter | M | Probe or stream requests satisfied from local cache. |
| `decdn_cache_misses_total` | Counter | M | Probe or stream requests requiring origin pull or peer pull. |
| `decdn_cache_bytes_returned_total` | Counter | R | Bytes returned from `CacheEngine::get` to the caller on success. Counts both cache-hit and pull-through-success paths. |
| `decdn_cache_pull_through_bytes_total` | Counter | R | Bytes received from origin during a cache miss, counted regardless of BLAKE3 verification or store landing — origin egress is paid either way. Independent of `decdn_cache_bytes_returned_total`: equal values mean pure pass-through; `bytes_returned_total >> pull_through_bytes_total` indicates effective caching. |
| `decdn_cache_evictions_total` | Counter | R | Blobs evicted by LRU pressure (eviction-driver loop). See [appendix-blob-cache-eviction.md](appendix-blob-cache-eviction.md#appendix-blob-cache-eviction-policy). |
| `decdn_cache_evicted_operator_total` | Counter | R | Hashes removed via `decdn node evict` (durable, persisted to `<cache_dir>/evicted.log`). Distinct from `decdn_cache_evictions_total`. See [appendix-blob-cache-eviction.md § Operator-evict is orthogonal to LRU](appendix-blob-cache-eviction.md#operator-evict-is-orthogonal-to-lru). |
| `decdn_cache_pinned_count` | Gauge | R | Size of the operator-pinned set (LRU-exempt). See [appendix-blob-cache-eviction.md § Operator pinning overrides LRU](appendix-blob-cache-eviction.md#operator-pinning-overrides-lru). |
| `decdn_probe_post_eviction_failures_total` | Counter | R | `EvictedSinceProbe` responses from remote nodes during cache-hit stream requests. A sustained rate above ~1% of cache-hit attempts suggests remote hold mechanism failures ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh), [ADR 005](005-protocol.md#adr-005-wire-protocol)). |

#### Probe Metrics (`cdn/probe/v1`)

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_probe_collection_latency_seconds` | Histogram | M | — | Duration of a complete probe collection window, send to collection end, bounding the window in [ADR 001 § Probe response collection](001-network.md#probe-response-collection). Buckets: `[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]`. |
| `decdn_probe_requests_total` | Counter | R | — | Probe requests served by this node, incremented once the response frame is written. The serve-side counterpart to `decdn_probe_responses_total` (which counts probes this node *sent* and got answers to). Flat while a node is reachable but unprobed; the first signal that a restarted or re-keyed node is being found again. |
| `decdn_probe_responses_total` | Counter | R | `result={has_blob,no_blob,timeout}` | Probe responses received, by result. |
| `decdn_probe_cache_hits_total` | Counter | R | — | Cache-miss pulls whose candidate walk started from a live [ADR 001 § Probe cache](001-network.md#adr-001-network-topology-and-peer-mesh) entry. DHT lookup and probe fanout are skipped, unless every cached provider fails — the fetch then either falls through to a fresh lookup + probe (if attempt budget remains) or returns a clean miss (if the cached providers exhausted the budget first, the common case since an entry holds up to 10 providers but the budget is 3), and stays counted here either way (the hit measures "the cache had something worth trying", not delivery). With `decdn_probe_cache_misses_total` this is the hit ratio the TTL exists to buy (`probe_cache_ttl = PROBE_SLASH_WINDOW / 2`, [ADR 005 § Derived constants](005-protocol.md#adr-005-wire-protocol)); a ratio near zero means the TTL is shorter than the inter-arrival time for hot blobs and the cache is pure overhead. |
| `decdn_probe_cache_misses_total` | Counter | R | — | Cache-miss pulls that ran a fresh DHT lookup + probe. Counts an entry that was absent, expired, **or fully suppressed** (every cached provider negative-cached, wedged, no longer an active staker, or otherwise unselectable) — all three cost the same network work, which is what this measures. |

> The probe-hold capacity counter `decdn_probe_hold_unavailable_total{reason}`
> and the `decdn_probe_hold_slots_used` / `_max` gauges live in
> [§ Slash-Safety Metrics](#slash-safety-metrics-all-mandatory), not here. They
> are grouped there deliberately, alongside the slash-evidence counters they sit
> next to on an operator's dashboard. Documenting them twice is how the two
> copies drifted apart.

#### Payment Channel Metrics

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_channels_open` | Gauge | M | Currently open payment channels (as node — inbound from clients). |
| `decdn_channels_settled_total` | Counter | M | Channels settled on-chain. |
| `decdn_channel_deposit_usdc` | Gauge | M | Total USDC deposited across all currently open inbound channels. Represents maximum on-chain recoverable value. |
| `decdn_buyer_channel_store_skipped_undecodable_records_total` | Counter | M | Buyer-channel rows omitted from successful store hydration because their persisted values cannot be decoded. One bad row does not stop healthy channels from loading or being reclaimed; each load attempt counts every omitted row, so any increase means a buyer deposit is escrowed but untracked and requires record repair. |
| `decdn_vouchers_signed_total` | Counter | M | Vouchers signed by this node as the payee. |
| `decdn_vouchers_received_total` | Counter | R | Vouchers received by this node as the payer (node-to-node pulls). |
| `decdn_channel_disputes_total` | Counter | R | Channels that entered the dispute window. |

#### Gossip Metrics

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_gossip_messages_rejected_total` | Counter | M | `reason={clock_skew,invalid_signature,not_registered,stale_timestamp,invalid_region,duplicate_hashes,table_full}` | Gossip messages rejected during validation ([ADR 001](001-network.md#gossip-validation)) or peer-table admission ([appendix-peer-table-eviction.md](appendix-peer-table-eviction.md#appendix-peer-table-eviction-policy)). `reason=clock_skew` is the canonical replacement for [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)'s `gossip_messages_rejected_clock_skew`. `table_full` fires only when the optional `gossip.max_peer_entries` ceiling is set and exceeded. |
| `decdn_peer_table_size` | Gauge | M | — | Number of distinct peers in the local peer table. |
| `decdn_peer_table_evicted_ttl_total` | Counter | R | — | Peer-table entries removed by the TTL sweeper ([appendix-peer-table-eviction.md § Lifecycle and TTL](appendix-peer-table-eviction.md#lifecycle-and-ttl)). |
| `decdn_peer_table_evicted_registry_total` | Counter | R | `reason={deregistered,ejected}` | Peer-table entries removed in response to a `NodeDeregistered` or `NodeAutoEjected` registry event ([appendix-peer-table-eviction.md § Registry-cache interaction (active eviction)](appendix-peer-table-eviction.md#registry-cache-interaction-active-eviction)). |
| `decdn_gossip_announces_sent_total` | Counter | R | — | `NodeAnnounce` messages published. |
| `decdn_gossip_announces_received_total` | Counter | R | — | `NodeAnnounce` messages accepted (passed validation). |
| `decdn_gossip_subscriber_reconnections_total` | Counter | R | — | Successful subscriber reconnections after a gossip stream drop. |

#### Reputation Metrics

Reputation is local-only per [ADR 008](008-reputation.md#adr-008-reputation-system) — no gossip, so the only reputation metric is the local score gauge.

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_reputation_score` | Gauge | R | This node's current local reputation score (0.0–1.0) for a peer, computed from its own delivery observations per [ADR 008](008-reputation.md#adr-008-reputation-system). |

#### Node / Process Metrics

| Metric | Type | Tier | Description |
|--------|------|------|-------------|
| `decdn_node_uptime_seconds` | Gauge | R | Seconds since the node process started. Used by the `/health` endpoint and operator dashboards to correlate events with restarts. |

#### DHT / Content-Discovery Metrics

Per [ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale) (`cdn/dht/v1`). DHT STORE and FIND_VALUE carry no protocol-level fee; these metrics expose discovery health only.

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_dht_store_published_total` | Counter | R | — | DHT STORE records this node published to the K-closest peers ([ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale)). |
| `decdn_dht_findvalue_queries_total` | Counter | R | — | DHT FIND_VALUE lookups this node issued to discover providers. |
| `decdn_dht_routing_table_size` | Gauge | R | — | Distinct entries in the local Kademlia routing table. |

##### Active-Staker Set Watcher Metrics

The shared `capacity-bond` watcher follows `CapacityBond` membership events to keep a cached active-staker set in sync with chain state. Since #1226 that one loop also feeds the `NodeId → operator address` bindings projection, so these metrics are its health for both — there is no separate node-address watcher family (#1231) ([ADR 022 § STORE Flow](022-content-discovery.md#adr-022--content-discovery-at-scale), [ADR 019 § Step 3.3](019-node-onboarding.md#step-33--build-initial-peer-table-from-on-chain-registry)). On a mid-run watcher RPC outage the cache can **drift** from chain state (there is no `getActiveNodes` resync after extended outage). That drift is revenue-impacting: the cached set decides which probes the stake-lane reservation sheds ([ADR 003 § Admission and Priority](003-payments.md#admission-and-priority), #757) and gates DHT `Store` admission. These metrics make the drift window alertable rather than log-grep-only — `decdn_rpc_healthy` tracks the reachability watchdog, **not** this watcher (#783).

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_staker_set_watcher_restarts_total` | Counter | M | — | Distinct drift windows the watcher has entered (#783, edge-triggered #788): bumped **once** on the transition from a healthy cycle into the error/backoff state, so each increment brackets exactly one drift window (it does **not** count individual backoff iterations of one continuous outage). Pairs with the per-error `warn!` in `resumable_watcher::run` ("watcher RPC error; restarting after backoff"). |
| `decdn_staker_set_watcher_resolve_failures_total` | Counter | M | — | Operator-indexed events (`Reinstated` / `UnbondingRequested`) dropped because the follow-up `nodeIdOf(operator)` RPC failed (#788). The membership change is lost, leaving the cached set out of sync for that operator until a later event corrects it — silent drift that trips no restart/down-seconds metric, hence its own counter. Pairs with the per-failure `warn!` in `capacity_bond_registry`'s `RegistrySink::on_operator_change`. |
| `decdn_staker_set_watcher_down_seconds` | Gauge | M | — | True downtime (#783, semantics corrected #788): seconds the watcher has been in the error/backoff state, i.e. failing its `eth_getLogs` poll tick. Reads `0` for the **entire life of any established cycle**, however long or quiet (a healthy poll loop persists indefinitely — this is *not* cycle age), and climbs only while between a failed cycle and the next re-establishment, so an alert fires on a *sustained* outage rather than a transient restart. Reads `0` until the first cycle is established after bootstrap; a poisoned internal lock reports `i64::MAX` (conservative — never masks an in-progress outage). |
| `decdn_staker_set_active_count` | Gauge | R | — | Current cached active-staker set size (#783), sampled on bootstrap and on every membership change. Pair with `decdn_staker_set_watcher_down_seconds`: the count holding flat while down-seconds climbs means the cache is frozen, not that the network genuinely lost operators. |

**Recommended alerts:**

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| `decdn_staker_set_watcher_down_seconds` | > 120s | > 600s sustained | Check the blockchain RPC provider; the cached active-staker set may be drifting from chain state, mis-shedding stake-lane probes and DHT `Store`s. |
| `decdn_staker_set_watcher_restarts_total` (rate) | > 0 | > 0 sustained | Investigate a flapping RPC endpoint; correlate with `decdn_staker_set_watcher_down_seconds` depth. |
| `decdn_staker_set_watcher_resolve_failures_total` (rate) | > 0 | > 0 sustained | A `nodeIdOf` RPC is failing and silently dropping membership changes — the cached active-staker set is drifting from chain state. Check the RPC provider; correlate with `decdn_staker_set_active_count`. |

##### Watcher Liveness, Panic, and Enforcement Metrics

All five chain-event watchers run through one shared `resumable_watcher::run` loop. The `*_down_seconds` / `*_restarts_total` down-family above is **error-triggered**: it moves only when a poll tick returns `Err`. A task that panics while healthy, wedges in an await the per-call timeout does not cover, or exits cleanly on shutdown leaves the down-since state unset, so `*_down_seconds` reads a healthy `0` — a dead watcher is byte-identical to a live one (#1316, #1320). Two additive families close that gap uniformly across all five, and #1283/#1316 also brought `blacklist` and `settlement` to full down-family parity: they now expose `restarts_total` / `down_seconds` matching the staker-set rows above.

In the metric names below, `<watcher>` expands to one of **`slash_watcher`**, **`staker_set_watcher`**, **`origin_directory_watcher`**, **`blacklist_watcher`**, or **`settlement_watcher`**.

| Metric | Type | Tier | Labels | Description |
|--------|------|------|--------|-------------|
| `decdn_<watcher>_last_tick_timestamp_seconds` | Gauge | M | — | **Positive liveness signal** (#1316): Unix wall-clock time of the last *successful* poll tick, stamped every tick (including idle ticks — a successful head read is proof of life). Unlike the error-triggered down-family, a panicked, wedged, or cleanly-exited task stops advancing this, so staleness is detectable. One series per watcher (`slash`, `staker_set`, `origin_directory`, `blacklist`, `settlement`). Reads `0` until the first successful tick. |
| `decdn_<watcher>_task_panicked_total` | Counter | M | — | The watcher task unwound on a panic (#1316). Bumped from a `Drop` guard in `resumable_watcher::run` — the only thing that runs on the unwind, since the detached task is never awaited. **Any non-zero value is a bug in this node.** One series per watcher. |
| `decdn_blacklist_enforcement_failures_total` | Counter | M | — | Distinct hashes a batched re-scope could not re-verify or evict this pass (`Recheck::Failed` — a disk error or a scope `eth_call` failure) (#1319). Non-zero means the deny-set is **not fully enforced** and a blacklisted blob may still be servable and slashable (`SlashJudge.submitBlacklistChallenge`), even while `decdn_blacklist_watcher_down_seconds` reads `0` — the two answer different questions (deny-set enforced vs chain readable). Pairs with the aggregate `warn!` (`"blacklist re-scope could not enforce every entry"`). |

**Recommended alerts:**

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| `(decdn_<watcher>_last_tick_timestamp_seconds > 0) and (time() - decdn_<watcher>_last_tick_timestamp_seconds > 3 × poll_interval)` | ✓ | sustained | The watcher has not completed a tick — panicked, wedged, or exited. For `blacklist` this means the node may be serving content blacklisted after the failure (slashable); for the others, the corresponding cache/projection is drifting. Restart the daemon and check the RPC provider. **The `> 0` guard is required:** the gauge reads `0` until the first successful tick, so an unguarded `time() - gauge` fires forever on a fresh boot and permanently on a node that never enables a conditionally-spawned watcher (`origin_directory`). A watcher that never establishes at all is caught by its down-family / the blacklist readiness gate, not here. |
| `decdn_<watcher>_task_panicked_total` (rate) | > 0 | > 0 | A watcher task panicked. Never expected — capture the `error!` log line and file a bug. |
| `decdn_blacklist_enforcement_failures_total` (rate) | > 0 | > 0 sustained | The blacklist deny-set is not fully enforced — a blacklisted blob may be servable and slashable. Check disk health and the `ContentBlacklist` RPC; correlate with the `unenforced` `warn!`. |

> **Alert on the gauge, not the restart-counter rate (#1322).** The `*_watcher_restarts_total` counters are edge-triggered through a `Mutex<Option<Instant>>` whose update is skipped on a poisoned lock (anti-panic policy) — once poisoned, the counter freezes silently, so `rate(*_watcher_restarts_total[5m])` can go permanently quiet with no signal that it did. Alert on `*_down_seconds` (a poisoned lock there reports `i64::MAX`, the safe direction) for sustained chain-read outages, and on `time() - *_last_tick_timestamp_seconds` for liveness. The restart counter is for correlation/depth, not as a primary alert.

### Health Endpoint

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
| `ready` | All Phase 5 acceptance criteria satisfied ([ADR 019](019-node-onboarding.md#phase-5--accepting-paid-delivery)); serving traffic. |
| `degraded` | Running but one or more non-critical conditions impaired (e.g., gossip mesh thin, peer table sparse). Traffic still accepted. |
| `not_ready` | A mandatory startup check failed or is incomplete (blacklist un-synced, rate floor not loaded, not registered). Not accepting traffic. |

HTTP status codes: `200` for `ready` and `degraded`; `503` for `not_ready`. Monitoring systems SHOULD alert on `503` responses.

### Structured Logging

Metrics cover aggregates; structured logs cover per-event detail. Logs complement metrics; they do not replace them.

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

### Canonical Metric Name Cross-Reference

Earlier ADRs used informal metric names; this table maps them to canonical replacements. **No wire protocol or on-chain change** — instrumentation names only.

| Informal name (prior ADR) | Canonical name (this appendix) | Source ADR |
|---------------------------|---------------------------|------------|
| `gossip_messages_rejected_clock_skew` | `decdn_gossip_messages_rejected_total{reason="clock_skew"}` | [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh) |
| `probe_hold_violations` | `decdn_probe_hold_unavailable_total{reason="exhausted"}` | [ADR 005](005-protocol.md#adr-005-wire-protocol), architecture.md |
| `probe_holds_disabled` | `decdn_probe_hold_unavailable_total{reason="disabled"}` | [ADR 005](005-protocol.md#adr-005-wire-protocol), #739 |
| `probe_stake_lane_reserved` | `decdn_probe_hold_unavailable_total{reason="stake_lane_reserved"}` | [ADR 003 § Admission and Priority](003-payments.md#admission-and-priority), #757 |
| `probe_hold_slots_used` | `decdn_probe_hold_slots_used` | [ADR 005](005-protocol.md#adr-005-wire-protocol), architecture.md |
| `rate_bounds_clamp_events` | `decdn_rate_bounds_clamp_events_total` | architecture.md |
| `blacklist_sync_lag_seconds` | `decdn_blacklist_sync_lag_seconds` | [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting) |
| `blacklist_version_behind` | `decdn_blacklist_version_behind` | [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting) |
| `probe_collection_latency_seconds` | `decdn_probe_collection_latency_seconds` | [ADR 001 § Probe response collection](001-network.md#probe-response-collection) |
| `streams_active` | `decdn_streams_active` | architecture.md |
| `streams_completed` | `decdn_streams_completed_total` | architecture.md |
| `streams_failed` | `decdn_streams_failed_total` | architecture.md |
| `vouchers_signed` | `decdn_vouchers_signed_total` | architecture.md |
| `vouchers_received` | `decdn_vouchers_received_total` | architecture.md |
| `channels_open` | `decdn_channels_open` | architecture.md |
| `channels_settled` | `decdn_channels_settled_total` | architecture.md |
| `cache_hits` | `decdn_cache_hits_total` | architecture.md |
| `cache_misses` | `decdn_cache_misses_total` | architecture.md |
| `cache_bytes` | `decdn_cache_bytes` | architecture.md |

### Reference dashboards and alerts

A reference Grafana dashboard and starter Prometheus alerting rules ship in the top-level [`monitoring/`](../monitoring/) directory. They consume only canonical [§ Metric Registry](#metric-registry) metric names — no new metrics — and are an onboarding starting point, not a normative deliverable.

| File | Purpose |
|------|---------|
| [`monitoring/prometheus-alerts.yml`](../monitoring/prometheus-alerts.yml) | Three rule groups: `decdn-slash-safety` (thresholds copied verbatim from [§ Slash-Safety Metrics (all Mandatory)](#slash-safety-metrics-all-mandatory)), `decdn-liveness`, `decdn-delivery`. |
| [`monitoring/grafana-dashboard.json`](../monitoring/grafana-dashboard.json) | Single overview dashboard (`uid: decdn-poc-overview`); rows for health, slash safety, delivery, cache, probes, payments. Datasource parameterised via `${DS_PROMETHEUS}`; node selection via the `instance` template variable. |

#### Importing the dashboard

In Grafana, *Dashboards → New → Import*; upload or paste the JSON. Select your Prometheus datasource at the prompt; the `instance` variable auto-populates from `decdn_node_uptime_seconds`.

#### Using the alerts

Add the file via Prometheus `rule_files:` and reload. Validate with `promtool check rules monitoring/prometheus-alerts.yml`. Tune `for:` durations and thresholds for your fleet size before paging.

#### Scope

Covers M-tier slash-safety metrics and the most common R-tier panels for a first dashboard. Deliberately not exhaustive.

## Consequences

### Positive

- Single reference for dashboard configuration — no hunting across 8 ADRs for metric names.
- Mandatory M-tier slash-risk metrics enforced at startup, so operators cannot accidentally run without slash-risk visibility.
- Canonical `decdn_` prefix and `_total` suffix allow automated registry validation (e.g., a CI check that exported names match the registry).
- Alert thresholds provide actionable defaults for new operators.
- The `/health` endpoint integrates with standard load balancers and container readiness probes without parsing Prometheus text.

### Negative

- Existing ADRs reference informal names differing from the canonical ones here. The cross-reference table (Section 6) documents all renames. No ADR is retroactively edited, but implementations must use this appendix's canonical names.
- Mandatory metrics add startup complexity — all M-tier collectors must initialize before accepting connections. Small overhead for guaranteed observability.

## Deferred & Open

- **OpenMetrics migration.** Prometheus text format 0.0.4 is the current default; the OpenMetrics exposition format (used by `prometheus_client` crate's `MetricsEncoder`) adds exemplars and native histograms — evaluate once tooling support is broader.
