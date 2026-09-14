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

Several ADRs reference informal metric names (e.g., `probe_hold_violations` — from [ADR 005](005-protocol.md#adr-005-wire-protocol)). This appendix is the authoritative canonical registry. The names below are the canonical forms of those informal references, with identical semantic intent.

## Decision

### Naming Convention

All metrics use the `decdn_` prefix, snake_case, and Prometheus-standard unit suffixes:

| Pattern | Example | Rule |
|---------|---------|------|
| `decdn_{subsystem}_{noun}_{unit}` | `decdn_cache_bytes` | Gauge: descriptive noun + unit |
| `decdn_{subsystem}_{noun}_total` | `decdn_streams_completed_total` | Counter: always `_total` suffix |
| `decdn_{subsystem}_{noun}_seconds` | `decdn_probe_collection_latency_seconds` | Histogram/Summary: `_seconds` for duration |

Label names: snake_case, no abbreviations. Label values: lowercase where possible.

#### Reason splits: sibling counters, not labels

Several subsystems classify a failure into a small closed set of reasons. The convention is **one unlabeled sibling counter per reason**, not one counter with a `reason` label:

| Family | Series |
|--------|--------|
| Connection dispatch rejection (`RejectReason`) | `decdn_dispatch_rejected_global_total`, `decdn_dispatch_rejected_per_source_total` |
| Rate-limit rejection (`RejectLayer`, shared by the probe and DHT paths) | `decdn_probe_rate_limit_rejected_{per_peer,per_ip,global}_total` and `decdn_dht_rate_limit_rejected_{per_peer,per_ip,global}_total` |
| Buyer pool-open failure (`PoolOpenFailureReason`) | `decdn_pool_open_failures_insufficient_deposit_total`, `decdn_pool_open_failures_contract_revert_total`, `decdn_pool_open_failures_rpc_error_total` (plus the aggregate `decdn_node_pull_pool_open_failures_total`) |

Siblings are the default because each reason in these families has an **unrelated operator remedy**, so no single alert spans the family and a shared label buys nothing; a sibling also needs no typed `EncodeLabelSet` and no pre-materialization to keep a series exporting at zero.

**`decdn_probe_hold_unavailable_total{reason}` is the one deliberate exception among reason splits.** Its three values share one *aggregate* and one budget axis (all three derive from `max_probe_holds`), so operators query the aggregate first and drill in on the label second — the shape a label serves well. They pointedly do **not** share an alert: the alert filters to `reason="exhausted"`, because `disabled` is an intentional operator choice and `stake_lane_reserved` has its own knob; being able to express that filter is part of what the label buys. Note this is the one labelled *reason split*, not the only labelled metric — `decdn_streams_active{direction}` is labelled on another axis. A new split should follow the sibling convention unless it meets that same bar. Either way the invariant is absolute: **no metric may silently stop exporting at zero**, and an alert whose remedy applies to only one reason must carry the corresponding filter.

All metrics are exported in **Prometheus text format 0.0.4** on a configurable HTTP port (default `9090`) at `/metrics`. The same port exposes `/health` (see [Health Endpoint](#health-endpoint)). The port MUST be operator-configurable and MUST NOT be publicly accessible without authentication in production (firewall or auth proxy).

### Metric Registry

Every registry row carries two independent axes. **Tier** says how strongly the protocol requires the metric; **Status** says whether the reference implementation emits it today. They are orthogonal on purpose: an M-tier row can be `planned`, and that combination is exactly the gap worth tracking.

Metrics are grouped into **mandatory** (M) and **recommended** (R) tiers.

**Mandatory (M):** The node MUST expose these or refuse to start. They cover slash-risk conditions and delivery accountability.

**Recommended (R):** The node SHOULD expose these. Absence is not a startup blocker, but operators lose subsystem visibility.

Status is `live` or `planned`:

**`live`:** `decdn-node` exports this series today. Safe to put on a dashboard or in an alert.

**`planned`:** specified here, not yet implemented. **Nothing emits it**, so a panel or alert built on it will render `(no data)` and a threshold rule will never fire. Do not add it to `monitoring/`.

The `live` rows are enforced: `crates/node/src/metrics.rs`'s `adr_registry_names_are_exported` test asserts every one of them appears in the encoder's output, so a `live` row naming a series the node does not emit fails CI. Two limits worth knowing. The gate skips rows whose metric cell carries a label or a `<placeholder>` (`decdn_probe_hold_unavailable_total{reason}` and the two `decdn_<watcher>_*` templates), so those three are documented but unenforced. And it runs in one direction only — it proves no documented row is fiction, **not** that every exported series is documented. The registry is a curated subset of roughly 180 exported series, so absence from this table is not evidence that a metric does not exist; grep `crates/node/src/metrics.rs` and `crates/cache/src/metrics.rs` before concluding that. `planned` is the allowlist that gate skips — which is why marking a row `planned` is a deliberate act: it keeps an unbuilt series out of the enforced set while still documenting it.

Adding a metric therefore means editing this table in the same change, not afterwards.

#### Slash-Safety Metrics (all Mandatory)

These give early warning for the two slashable offenses in [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn), grouped here with the closely-related probe-hold capacity metrics. Blacklist-watcher liveness is monitored through `decdn_blacklist_watcher_last_tick_timestamp_seconds` (the `DecdnBlacklistWatcherStalled` rule in `monitoring/` reads it); the watcher rebuilds the deny-set by full enumeration and keeps no version cursor, so there is no sync-lag gauge to alert on. The probe-hold gauges (`decdn_probe_hold_slots_used`/`_max`) are live and normally non-zero — alert on the thresholds/rates in the table below, not on presence. `decdn_probe_hold_unavailable_total` is an availability signal, not a slash risk — see its row, and alert only on `reason="exhausted"`, the budget-pressure value.

| Metric | Type | Tier | Status | Description |
|--------|------|------|--------|-------------|
| `decdn_probe_hold_unavailable_total{reason}` | Counter | M | live | A probe for a present blob that got **no** eviction hold ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)). Holds are best-effort, so this is not the same as answering `has_blob: false` — see the per-reason split. Never a safety fault: no offense pairs a probe with a later miss ([ADR 014](014-on-chain-verification.md#rate-manipulation)), and an unheld advertisement that loses the eviction race costs one wasted round trip. The `reason` label carries the cause, because each has a different operator remedy: **`exhausted`** — blob present but **all** hold slots were live (`max_probe_holds` reached); the node still advertises `has_blob: true` and forgoes only the hold, so the blob may be LRU-evicted before the pull. Genuine budget pressure and the only value the "raise `max_probe_holds`" alert fires on. **`disabled`** — blob present but the hold path is **off by config** (`max_probe_holds == 0`); the one reason that also suppresses the advertisement (`has_blob: false` for store-backed content; origin-held content takes no hold and is unaffected). An intentional operator choice, so alerting on it would be nonsensical. **`stake_lane_reserved`** — an end-client probe hit the stake-lane-reserved end-client ceiling (`max_probe_holds − cache.stake_lane_reserved_holds`), keeping hold headroom for registered node-to-node cache-miss probes ([ADR 003 § Admission and Priority](003-payments.md#admission-and-priority)). The reservation is a content-independent admission decision taken before any hold attempt, but the handler still consults the cache to answer honestly and advertises a present blob. Stays zero unless `cache.stake_lane_reserved_holds > 0`. All three children are exported at zero from startup, so a missing series means a broken exporter, not an idle node. |
| `decdn_probe_hold_slots_used` | Gauge | M | live | Eviction-hold slots in use out of `max_probe_holds`. Saturation means new probes are advertised without a hold, not answered `has_blob: false`. |
| `decdn_probe_hold_slots_max` | Gauge | M | live | Configured `max_probe_holds`. Paired with `decdn_probe_hold_slots_used` for a saturation ratio. |
| `decdn_rate_bounds_clamp_events_total` | Counter | M | live | Times `rate_per_mb` was raised to the governance `deliveryFloor` before signing a `ProbeResponse` / `StreamResponse` — the configured rate sits below the current floor ([ADR 003](003-payments.md#adr-003-payment-model), [ADR 005](005-protocol.md#adr-005-wire-protocol)). |

**Recommended alert thresholds:**

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| `decdn_probe_hold_unavailable_total{reason="exhausted"}` (rate) | > 0 | > 0 sustained | Reduce load or increase `max_probe_holds`; check for OOM. Filter on `reason="exhausted"` — the `disabled` and `stake_lane_reserved` values are deliberate operator decisions and must not trip this alert. |
| `decdn_rate_bounds_clamp_events_total` (rate) | > 0 | — | Raise the `rate_per_mb` config to at least the governance `deliveryFloor`. |

#### Delivery Metrics (`cdn/client/v1`)

| Metric | Type | Tier | Status | Labels | Description |
|--------|------|------|--------|--------|-------------|
| `decdn_streams_active` | Gauge | M | live | `direction={inbound,outbound}` | Currently open delivery streams. |
| `decdn_streams_completed_total` | Counter | M | planned | `direction={inbound,outbound}` | Successfully completed streams. |
| `decdn_streams_failed_total` | Counter | M | planned | `direction, reason` | Failed streams. `reason` values: `hash_mismatch`, `channel_insufficient`, `rate_mismatch`, `blob_too_large`, `evicted`, `timeout`, `protocol_error`, `other`. |
| `decdn_bytes_served_total` | Counter | M | planned | — | Bytes delivered to clients and downstream nodes (inbound streams from the requester's perspective). |
| `decdn_bytes_received_total` | Counter | M | planned | — | Bytes received as a client in node-to-node cache-miss pulls. |
| `decdn_serve_frame_accounting_fault_total` | Counter | R | live | — | Times a serve leg refused to cut or frame a `ChunkData` because its own byte accounting did not add up: a zero frame target, a `queued`/`queue` desync, a payload whose chunks disagree with the length the header would declare, or a header the encoder refused. Each refusal is correct — the delivery aborts without `StreamEnd`, so the client gets no mislabelled frame and pays no closing voucher — which is exactly why it is otherwise invisible: the stream simply ends, and that reads as a client that hung up. This is a **latent-bug report, not a degradation**. Alert on `> 0` and file it rather than tuning anything. |
| `decdn_serve_stream_node_fault_total` | Counter | R | live | — | Times a `cdn/client/v1` delivery ended on a fault this node caused: an encode fault, an alignment error, a store fault, or a framing fault. A peer hang-up and a client payment fault are excluded — they are routine. The client sees only a short stream, so this counter and the `error!` line beside it are the operator's sole signal that a delivery was abandoned. This is a **latent-bug report, not a degradation**. Alert on `> 0` and file it. It is a superset of `decdn_serve_frame_accounting_fault_total`, which meters one of the four classes on its own. |

#### Cache Metrics

| Metric | Type | Tier | Status | Description |
|--------|------|------|--------|-------------|
| `decdn_cache_bytes` | Gauge | M | live | Total cache size in bytes (all blobs). Paired with `decdn_cache_size_limit_bytes` for a saturation ratio. |
| `decdn_cache_size_limit_bytes` | Gauge | R | live | Configured cache capacity in bytes (`cache.cache_size_mb × 1 048 576`). See [ADR 040](040-cache-policy.md#adr-040-pluggable-cache-admission-and-eviction-policies). |
| `decdn_cache_hits_total` | Counter | M | live | Probe or stream requests satisfied from local cache. |
| `decdn_cache_misses_total` | Counter | M | live | Probe or stream requests requiring origin pull or peer pull. |
| `decdn_cache_bytes_returned_total` | Counter | R | live | Bytes returned from `CacheEngine::get` to the caller on success. Counts both cache-hit and pull-through-success paths. |
| `decdn_cache_pull_through_bytes_total` | Counter | R | live | Bytes received from origin during a cache miss, counted regardless of BLAKE3 verification or store landing — origin egress is paid either way. Independent of `decdn_cache_bytes_returned_total`: equal values mean pure pass-through; `bytes_returned_total >> pull_through_bytes_total` indicates effective caching. |
| `decdn_cache_evictions_total` | Counter | R | live | Blobs evicted by LRU pressure (eviction-driver loop). See [ADR 040](040-cache-policy.md#adr-040-pluggable-cache-admission-and-eviction-policies). |
| `decdn_cache_evicted_operator_total` | Counter | R | live | Hashes removed via `decdn node evict` (durable, persisted to `<cache_dir>/evicted.log`). Distinct from `decdn_cache_evictions_total`. See [ADR 040 § Pinning, durable operator-evict, and the probe-hold stay engine-enforced](040-cache-policy.md#pinning-durable-operator-evict-and-the-probe-hold-stay-engine-enforced). |
| `decdn_cache_held_corruption_quarantined_total` | Counter | R | live | Held hashes quarantined because a serve export failed bao validation against the content root. The stored bytes or outboard changed after admission. The node stops serving and announcing the hash and releases the entry to GC. The quarantine is not durable, and the hash is re-acquirable after the sweep. Distinct from `decdn_cache_evicted_operator_total`. A nonzero rate means disk rot or tampering under the cache directory. See [ADR 040 § Pinning, durable operator-evict, and the probe-hold stay engine-enforced](040-cache-policy.md#pinning-durable-operator-evict-and-the-probe-hold-stay-engine-enforced). |
| `decdn_cache_inflight_mutex_poisoned_total` | Counter | R | live | Times a task panicked while holding the in-flight fill-coalescing map, counted once per poisoning (the engine clears the poison, so this does not climb with request volume). The engine recovers the guard and coalescing is preserved, so this is a **latent-bug report, not a degradation** — any nonzero value is worth filing rather than tuning, and needs no restart. Alert on `> 0`. See [docs/runbook.md § Cache coalescing mutex poisoned](../docs/runbook.md). |
| `decdn_cache_origin_probe_failures_total` | Counter | R | live | Origin size probes that faulted during an origin rescan — one or more configured origins returned a transport error and none confirmed the object, or the probe walk overran its ceiling. A rescan decides what this node advertises. A retry-eligible fault keeps whatever entry an earlier pass indexed, so a sustained nonzero rate means the node may still advertise content the origin has stopped holding, and every request for it is a refusal; a permanent fault (a revoked ACL, a symlink escape) is not carried, and a candidate first seen inside the fault window has nothing to carry. Correlate with the origin backend's own error rate. An HTTP origin reports a *server-side* 5xx as a plain negative, so that one case is not counted; its transport faults, S3/R2 and the filesystem are. |
| `decdn_cache_origin_enumerate_failures_total` | Counter | R | live | Origins whose `enumerate` failed during a rescan, counted once per origin per rescan. The sibling of `decdn_cache_origin_probe_failures_total` for the other leg of the same rescan, and the more severe of the two: a failed listing produces no candidates, so every hash discoverable only through that origin leaves the announce set with no per-hash fault and nothing to carry forward — only operator pins naming those hashes survive. |
| `decdn_cache_pinned_count` | Gauge | R | live | Size of the operator-pinned set (LRU-exempt). See [ADR 040 § Pinning, durable operator-evict, and the probe-hold stay engine-enforced](040-cache-policy.md#pinning-durable-operator-evict-and-the-probe-hold-stay-engine-enforced). |
| `decdn_probe_post_eviction_failures_total` | Counter | R | live | `EvictedSinceProbe` responses from remote nodes during cache-hit stream requests. A sustained rate above ~1% of cache-hit attempts suggests remote hold mechanism failures ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh), [ADR 005](005-protocol.md#adr-005-wire-protocol)). |
| `decdn_local_outboard_serves_total` | Counter | R | live | Times a whole-blob cache miss was filled by streaming straight from the node's own configured origin into the paying client while teeing the bytes into the local cache store — the stream-while-store path ([ADR 037 § Origin-tier whole-blob miss](037-regional-proxy-warming.md#origin-tier-whole-blob-miss-stream-while-store)), entered only when the origin publishes a `{H}.obao4` outboard. Incremented on entry, before any admission guard, so it marks which serve tier fired rather than whether the fill succeeded. A miss that instead falls back to the buffered whole-blob import path never bumps this counter, which is what lets a dashboard tell the two fill strategies apart. |
| `decdn_warming_credits_applied_total` | Counter | R | live | Speculative-warming serve credits the background aggregator applied to a source's allowance ledger ([ADR 041 § Negative](041-refuse-to-serve.md#negative)). Read it beside `decdn_warming_credits_dropped_total`: a node whose serve path is not wired to a credit sink enqueues nothing and therefore drops nothing, so zero drops is only good news next to a climbing apply count. A node serving source-tagged blobs must show this rising. |
| `decdn_warming_credits_dropped_total` | Counter | R | live | Serve credits that never reached the allowance ledger ([ADR 041 § Negative](041-refuse-to-serve.md#negative)) — the bounded aggregator queue was full, or the aggregator task was gone. Every drop is conservative: it leaves the source's ledger more negative than its true profit and loss, so it can only throttle warming from that source, never over-fund it. A sustained rate means warming is throttled by lost bookkeeping rather than by real losses; a rate that tracks the serve rate means the aggregator died and every source will drift to blocked. |

#### Probe Metrics (`cdn/probe/v1`)

| Metric | Type | Tier | Status | Labels | Description |
|--------|------|------|--------|--------|-------------|
| `decdn_probe_collection_latency_seconds` | Histogram | M | planned | — | Duration of a complete probe collection window, send to collection end, bounding the window in [ADR 001 § Probe response collection](001-network.md#probe-response-collection). Buckets: `[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]`. |
| `decdn_probe_requests_total` | Counter | R | live | — | Probe requests served by this node, incremented once the response frame is written. The serve-side counterpart to `decdn_probe_responses_total` (which counts probes this node *sent* and got answers to). Flat while a node is reachable but unprobed; the first signal that a restarted or re-keyed node is being found again. |
| `decdn_probe_responses_total` | Counter | R | planned | `result={has_blob,no_blob,timeout}` | Probe responses received, by result. |
| `decdn_probe_cache_hits_total` | Counter | R | live | — | Cache-miss pulls whose candidate walk started from a live [ADR 001 § Probe cache](001-network.md#adr-001-network-topology-and-peer-mesh) entry. DHT lookup and probe fanout are skipped, unless every cached provider fails — the fetch then either falls through to a fresh lookup + probe (if attempt budget remains) or returns a clean miss (if the cached providers exhausted the budget first, the common case since an entry holds up to 10 providers but the budget is 3), and stays counted here either way (the hit measures "the cache had something worth trying", not delivery). With `decdn_probe_cache_misses_total` this is the hit ratio the TTL exists to buy (`probe_cache_ttl = PROBE_SLASH_WINDOW / 2`, [ADR 005 § Derived constants](005-protocol.md#adr-005-wire-protocol)); a ratio near zero means the TTL is shorter than the inter-arrival time for hot blobs and the cache is pure overhead. |
| `decdn_probe_rate_limit_rejected_per_peer_total` | Counter | R | live | — | Probe requests shed by the per-peer token bucket. |
| `decdn_probe_rate_limit_rejected_per_ip_total` | Counter | R | live | — | Probe requests shed by the per-IP token bucket — the layer that catches one host cycling node IDs. |
| `decdn_probe_rate_limit_rejected_global_total` | Counter | R | live | — | Probe requests shed by the node-wide ceiling. Rising while the per-peer and per-IP siblings stay flat means aggregate load, not one abusive source. |
| `decdn_probe_rate_limit_prune_sweeps_per_ip_total` | Counter | R | live | — | Sweeps that expired idle per-IP buckets. |
| `decdn_probe_rate_limit_prune_sweeps_per_peer_total` | Counter | R | live | — | Sweeps that expired idle per-peer buckets. |
| `decdn_probe_rate_limit_tracked_per_ip` | Gauge | R | live | — | Live per-IP buckets. Unbounded growth against a flat sweep rate is a memory-exhaustion signal. |
| `decdn_probe_rate_limit_tracked_per_peer` | Gauge | R | live | — | Live per-peer buckets. Same reading as the per-IP gauge. |
| `decdn_probe_cache_misses_total` | Counter | R | live | — | Cache-miss pulls that ran a fresh DHT lookup + probe. Counts an entry that was absent, expired, **or fully suppressed** (every cached provider negative-cached, wedged, no longer an active staker, or otherwise unselectable) — all three cost the same network work, which is what this measures. |

> The probe-hold capacity counter `decdn_probe_hold_unavailable_total{reason}`
> and the `decdn_probe_hold_slots_used` / `_max` gauges live in
> [§ Slash-Safety Metrics](#slash-safety-metrics-all-mandatory), not here. They
> are grouped there deliberately, alongside the blacklist-compliance metrics
> they sit next to on an operator's dashboard. Documenting them twice is how the
> two copies drifted apart.

#### Payment Pool Metrics

| Metric | Type | Tier | Status | Description |
|--------|------|------|--------|-------------|
| `decdn_lanes_open` | Gauge | M | live | Currently open inbound serve lanes — distinct `(pool, signer, provider)` keys with unredeemed vouchers. |
| `decdn_pool_redemptions_total` | Counter | M | planned | On-chain redemptions by this node. |
| `decdn_pool_deposit_usdc` | Gauge | M | live | Total USDC deposited in pools currently paying this node. Represents maximum on-chain recoverable value. |
| `decdn_buyer_pool_store_skipped_undecodable_records_total` | Counter | M | live | Buyer-pool rows omitted from successful store hydration because their persisted values cannot be decoded. One bad row does not stop healthy pools from loading or being reclaimed; each load attempt counts every omitted row, so any increase means a buyer deposit is escrowed but untracked and requires record repair. |
| `decdn_vouchers_signed_total` | Counter | M | planned | Vouchers signed by this node as the payee. |
| `decdn_vouchers_received_total` | Counter | R | planned | Vouchers received by this node as the payer (node-to-node pulls). |
| `decdn_pool_grace_closes_total` | Counter | R | planned | Pools observed entering the redemption grace window (owner close) while this node holds unredeemed vouchers. |

#### Reputation Metrics

Reputation is local-only per [ADR 008](008-reputation.md#adr-008-reputation-system) — no cross-node propagation, so the only reputation metric is the local score gauge.

| Metric | Type | Tier | Status | Description |
|--------|------|------|--------|-------------|
| `decdn_reputation_score` | Gauge | R | planned | This node's current local reputation score (0.0–1.0) for a peer, computed from its own delivery observations per [ADR 008](008-reputation.md#adr-008-reputation-system). |

#### Node / Process Metrics

| Metric | Type | Tier | Status | Description |
|--------|------|------|--------|-------------|
| `decdn_node_uptime_seconds` | Gauge | R | live | Seconds since the node process started. Used by the `/health` endpoint and operator dashboards to correlate events with restarts. |

#### DHT / Content-Discovery Metrics

Per [ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale) (`cdn/dht/v1`). DHT STORE and FIND_VALUE carry no protocol-level fee; these metrics expose discovery health only.

| Metric | Type | Tier | Status | Labels | Description |
|--------|------|------|--------|--------|-------------|
| `decdn_dht_store_published_total` | Counter | R | planned | — | DHT STORE records this node published to the K-closest peers ([ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale)). |
| `decdn_dht_findvalue_queries_total` | Counter | R | planned | — | DHT FIND_VALUE lookups this node issued to discover providers. |
| `decdn_dht_routing_table_size` | Gauge | R | planned | — | Distinct entries in the local Kademlia routing table. |
| `decdn_dht_rate_limit_rejected_per_peer_total` | Counter | R | live | — | DHT requests shed by the per-peer token bucket. |
| `decdn_dht_rate_limit_rejected_per_ip_total` | Counter | R | live | — | DHT requests shed by the per-IP token bucket. |
| `decdn_dht_rate_limit_rejected_global_total` | Counter | R | live | — | DHT requests shed by the node-wide ceiling. |
| `decdn_dht_rate_limit_prune_sweeps_per_ip_total` | Counter | R | live | — | Sweeps that expired idle per-IP buckets. |
| `decdn_dht_rate_limit_prune_sweeps_per_peer_total` | Counter | R | live | — | Sweeps that expired idle per-peer buckets. |
| `decdn_dht_rate_limit_tracked_per_ip` | Gauge | R | live | — | Live per-IP buckets. |
| `decdn_dht_rate_limit_tracked_per_peer` | Gauge | R | live | — | Live per-peer buckets. |
| `decdn_dht_republish_lag_sweeps_total` | Counter | R | live | — | Times the republisher lost its cache-commit event window (`Lagged` on the insert broadcast) ([ADR 022 § Bootstrap](022-content-discovery.md#bootstrap)). Counts lag events, not distinct store walks: a lag arriving mid-sweep folds into that sweep's next pass. |
| `decdn_dht_republish_lag_sweeps_coalesced_total` | Counter | R | live | — | Lag events folded into a sweep that was already running or already queued, splitting `lag_sweeps_total` into the lags that claimed an idle slot and the lags that did not. Neither this nor the difference counts store walks: the slot holds one queued position, so any number of lags arriving behind a running worker all count here and collapse into a single further pass. Read the ratio — coalesced climbing at the lag rate with the difference flat means the slot is never released. |
| `decdn_dht_republish_sweep_reseeded_total` | Counter | R | live | — | Hashes a lag sweep newly scheduled for republish. Seeding is idempotent, so this counts the repair, not the walk. |
| `decdn_dht_republish_seed_store_walk_failures_total` | Counter | R | live | — | Bulk republish seeds (boot cold start or lag sweep) that could not walk the local blob store and covered only the origin-held half. |
| `decdn_dht_republish_seed_origin_probe_failures_total` | Counter | R | live | — | Stored hashes a bulk republish seed could not put to its origin — the ownership test an origin-only node applies to every blob in its store, answered neither way. The origin-side twin of the store-walk counter above: an unconfirmable hash is left out of the announce set rather than advertised, so an unreachable remote origin shrinks the announce set of a node that holds the content. Distinct from `decdn_cache_origin_probe_failures_total`, which counts the rescan's probes rather than the seed's. |

Both rate-limit trios come from the same `RejectLayer` enum and the same shared limiter (`crates/node/src/rate_limit.rs`); the probe copies are in [§ Probe Metrics](#probe-metrics-cdnprobev1). They are **sibling counters, not a `layer` label**, per [§ Reason splits](#reason-splits-sibling-counters-not-labels) — recover the rolled-up rate with `sum(rate({__name__=~"decdn_dht_rate_limit_rejected_(per_peer|per_ip|global)_total"}[1m]))` (a `__name__` regex, not shell-style brace expansion — PromQL has no such syntax; the braces elsewhere in this appendix are naming shorthand for a family, never a query). [ADR 005 § Probe rate limiting](005-protocol.md#probe-rate-limiting) and [ADR 022 § DHT Rate Limiting](022-content-discovery.md#dht-rate-limiting) specify these sibling trios; no single labelled `*_rejections_total` counter exists.

##### Active-Staker Set Watcher Metrics

The shared `capacity-bond` watcher follows `CapacityBond` membership events to keep a cached active-staker set in sync with chain state. That one loop also feeds the `NodeId → operator address` bindings projection, so these metrics are its health for both — there is no separate node-address watcher family ([ADR 022 § STORE Flow](022-content-discovery.md#adr-022--content-discovery-at-scale), [ADR 019](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow)). On a mid-run watcher RPC outage the cache can **drift** from chain state, so the watcher re-enumerates `getRegisteredNodes` to repair any membership event lost to a reorg or a backoff gap. Two triggers drive that re-enumeration: a fixed interval (a correctness backstop, not a tuning knob), and the tick the watcher recovers from an errored one. The recovery trigger is what covers the case the cadence handles worst — the re-enumeration is skipped while the poll is erroring, and a failed re-read stamps the cadence without repairing the set, so on the cadence alone a sustained RPC outage defers the repair by further intervals. It bounds the drift from a *poll* outage, not every drift: a dropped `nodeIdOf` is returned as `Ok` and never errors the route, so a change lost that way still waits for the cadence; and a re-enumeration forced by recovery can itself fail, deferring a further interval. The window closes on the first successful re-enumeration after recovery. That drift is revenue-impacting while it lasts: the cached set decides which probes the stake-lane reservation sheds ([ADR 003 § Admission and Priority](003-payments.md#admission-and-priority)) and gates DHT `Store` admission. These metrics make the drift window alertable rather than log-grep-only — `decdn_rpc_healthy` tracks the reachability watchdog, **not** this watcher.

| Metric | Type | Tier | Status | Labels | Description |
|--------|------|------|--------|--------|-------------|
| `decdn_staker_set_watcher_restarts_total` | Counter | M | live | — | Distinct drift windows the watcher has entered (edge-triggered): bumped **once** on the transition from a healthy cycle into the error/backoff state, so each increment brackets exactly one drift window (it does **not** count individual backoff iterations of one continuous outage). Pairs with the per-error `warn!` in `resumable_watcher::run` ("watcher RPC error; restarting after backoff"). |
| `decdn_staker_set_watcher_resolve_failures_total` | Counter | M | live | — | Operator-indexed events (`Reinstated` / `UnbondingRequested`) dropped because the follow-up `nodeIdOf(operator)` RPC failed. The membership change is lost, leaving the cached set out of sync for that operator until the next `getRegisteredNodes` re-enumeration corrects it — the one forced on the tick the watcher recovers, when the drop travelled with a poll outage, otherwise the cadence — silent drift that trips no restart/down-seconds metric, hence its own counter. Pairs with the per-failure `warn!` in `capacity_bond_registry`'s `RegistrySink::on_operator_change`. |
| `decdn_staker_set_watcher_down_seconds` | Gauge | M | live | — | True downtime: seconds the watcher has been in the error/backoff state, i.e. failing its `eth_getLogs` poll tick. Reads `0` for the **entire life of any established cycle**, however long or quiet (a healthy poll loop persists indefinitely — this is *not* cycle age), and climbs only while between a failed cycle and the next re-establishment, so an alert fires on a *sustained* outage rather than a transient restart. Reads `0` until the first cycle is established after bootstrap; a poisoned internal lock reports `i64::MAX` (conservative — never masks an in-progress outage). |
| `decdn_staker_set_active_count` | Gauge | R | live | — | Current cached active-staker set size, sampled on bootstrap and on every membership change. Pair with `decdn_staker_set_watcher_down_seconds`: the count holding flat while down-seconds climbs means the cache is frozen, not that the network genuinely lost operators. |
| `decdn_capacity_bond_registry_resync_failures_total` | Counter | M | live | — | Re-enumerations of `getRegisteredNodes` that could not read chain state and kept the projections they already had. The re-enumeration deliberately reports success upward — an error would mark the watcher route errored and stall event pickup — so a persistently failing repair moves nothing else. Pairs with the per-failure `warn!` in `capacity_bond_registry`'s `RegistrySink::on_tick_complete`. |
| `decdn_capacity_bond_registry_last_resync_timestamp_seconds` | Gauge | M | live | — | Unix time of the last successful re-enumeration. The only signal that catches a repair being *skipped*, which emits nothing at all — the reconcile does not run while the route is errored. Stamped by the bootstrap enumeration as well, which is the same read against the same contract — without that, a node whose every later resync fails would hold the gauge at `0` and never trip a `> 0`-guarded alert. The guard still covers the window before bootstrap completes; the threshold must exceed the resync interval. |

**Recommended alerts:**

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| `decdn_staker_set_watcher_down_seconds` | > 120s | > 600s sustained | Check the blockchain RPC provider; the cached active-staker set may be drifting from chain state, mis-shedding stake-lane probes and DHT `Store`s. |
| `decdn_staker_set_watcher_restarts_total` (rate) | > 0 | > 0 sustained | Investigate a flapping RPC endpoint; correlate with `decdn_staker_set_watcher_down_seconds` depth. |
| `decdn_staker_set_watcher_resolve_failures_total` (rate) | > 0 | > 0 sustained | A `nodeIdOf` RPC is failing and silently dropping membership changes — the cached active-staker set is drifting from chain state. Check the RPC provider; correlate with `decdn_staker_set_active_count`. |
| `increase(decdn_capacity_bond_registry_resync_failures_total[1h])` | > 0 | > 0 sustained | The repair for a drifted active-staker set is itself failing, so a dropped membership change stays dropped. Check the RPC provider's `eth_call` path; correlate with `decdn_staker_set_watcher_resolve_failures_total`. The window has to exceed the attempt spacing — attempts are one resync interval apart, so a window of that same length sees a zero-rate gap on any delay and never sustains. |
| `(decdn_capacity_bond_registry_last_resync_timestamp_seconds > 0) and (time() - decdn_capacity_bond_registry_last_resync_timestamp_seconds > 2 × resync interval)` | ✓ | sustained | No re-enumeration has succeeded in two intervals. Unlike the counter above this also fires when the repair is being *skipped* rather than failing, which emits nothing. The **`> 0` guard is required**: the gauge reads `0` until the first success. |

##### Watcher Liveness, Panic, and Enforcement Metrics

All four chain-event watchers run through one shared `resumable_watcher::run` loop. The `*_down_seconds` / `*_restarts_total` down-family above is **error-triggered**: it moves only when a poll tick returns `Err`. A task that panics while healthy, wedges in an await the per-call timeout does not cover, or exits cleanly on shutdown leaves the down-since state unset, so `*_down_seconds` reads a healthy `0` — a dead watcher is byte-identical to a live one. Two additive families close that gap uniformly across all four, and `blacklist` and `settlement` carry full down-family parity: they expose `restarts_total` / `down_seconds` matching the staker-set rows above.

In the metric names below, `<watcher>` expands to one of **`slash_watcher`**, **`staker_set_watcher`**, **`blacklist_watcher`**, or **`settlement_watcher`**.

| Metric | Type | Tier | Status | Labels | Description |
|--------|------|------|--------|--------|-------------|
| `decdn_<watcher>_last_tick_timestamp_seconds` | Gauge | M | live | — | **Positive liveness signal**: Unix wall-clock time of the last *successful* poll tick, stamped every tick (including idle ticks — a successful head read is proof of life). Unlike the error-triggered down-family, a panicked, wedged, or cleanly-exited task stops advancing this, so staleness is detectable. One series per watcher (`slash`, `staker_set`, `blacklist`, `settlement`). Reads `0` until the first successful tick. |
| `decdn_<watcher>_task_panicked_total` | Counter | M | live | — | The watcher task unwound on a panic. Bumped from a `Drop` guard in `resumable_watcher::run` — the only thing that runs on the unwind, since the detached task is never awaited. **Any non-zero value is a bug in this node.** One series per watcher. |
| `decdn_blacklist_enforcement_failures_total` | Counter | M | live | — | Distinct hashes a batched re-scope could not re-verify or evict this pass (`Recheck::Failed` — a disk error or a scope `eth_call` failure). Non-zero means the deny-set is **not fully enforced** and a blacklisted blob may still be servable and slashable (`SlashJudge.submitBlacklistChallenge`), even while `decdn_blacklist_watcher_down_seconds` reads `0` — the two answer different questions (deny-set enforced vs chain readable). Pairs with the aggregate `warn!` (`"blacklist re-scope could not enforce every entry"`). |

The origin directory is not a watcher — it is a lazy, on-demand TTL cache with no poll loop, so it carries none of the tick/panic/down metrics above. It exposes its own pair instead: `decdn_origin_directory_get_origins_failures_total` (Counter) counts `getOrigins` lookups that failed on a cold-namespace cache miss, and `decdn_origin_directory_cache_size` (Gauge) reports the current namespace count held in the cache (positive and negative entries, bounded by the configured capacity).

**Recommended alerts:**

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| `(decdn_<watcher>_last_tick_timestamp_seconds > 0) and (time() - decdn_<watcher>_last_tick_timestamp_seconds > 3 × poll_interval)` | ✓ | sustained | The watcher has not completed a tick — panicked, wedged, or exited. For `blacklist` this means the node may be serving content blacklisted after the failure (slashable); for the others, the corresponding cache/projection is drifting. Restart the daemon and check the RPC provider. **The `> 0` guard is required:** the gauge reads `0` until the first successful tick, so an unguarded `time() - gauge` fires forever on a fresh boot. A watcher that never establishes at all is caught by its down-family / the blacklist readiness gate, not here. |
| `decdn_<watcher>_task_panicked_total` (rate) | > 0 | > 0 | A watcher task panicked. Never expected — capture the `error!` log line and file a bug. |
| `decdn_blacklist_enforcement_failures_total` (rate) | > 0 | > 0 sustained | The blacklist deny-set is not fully enforced — a blacklisted blob may be servable and slashable. Check disk health and the `ContentBlacklist` RPC; correlate with the `unenforced` `warn!`. |
| `decdn_origin_directory_get_origins_failures_total` (rate) | > 0 | sustained | `getOrigins` reads are failing on cache misses — requests are silently losing their origin fallback. Check the RPC provider. |

> **Alert on the gauge, not the restart-counter rate.** The `*_watcher_restarts_total` counters are edge-triggered through a `Mutex<Option<Instant>>` whose update is skipped on a poisoned lock (anti-panic policy) — once poisoned, the counter freezes silently, so `rate(*_watcher_restarts_total[5m])` can go permanently quiet with no signal that it did. Alert on `*_down_seconds` (a poisoned lock there reports `i64::MAX`, the safe direction) for sustained chain-read outages, and on `time() - *_last_tick_timestamp_seconds` for liveness. The restart counter is for correlation/depth, not as a primary alert.

### Health Endpoint

`GET /health` (same HTTP port as `/metrics`) returns a JSON object:

```json
{
  "status": "ready" | "degraded" | "not_ready",
  "node_id": "<hex iroh NodeId>",
  "registry_active": true,
  "blacklist_synced": true,
  "rate_bounds_loaded": true,
  "staker_set_active_count": 27,
  "lanes_open": 3,
  "pool_deposit_usdc": "15.23",
  "node_uptime_seconds": 3601
}
```

**JSON key → Prometheus metric mapping:**

| JSON key | Prometheus metric | Notes |
|----------|------------------|-------|
| `staker_set_active_count` | `decdn_staker_set_active_count` | Cached active-staker set size (registry health) |
| `lanes_open` | `decdn_lanes_open` | Direct gauge value |
| `pool_deposit_usdc` | `decdn_pool_deposit_usdc` | Formatted as decimal string for readability; metric stores raw value |
| `node_uptime_seconds` | `decdn_node_uptime_seconds` | Direct gauge value |

**Status semantics:**

| `status` | Meaning |
|----------|---------|
| `ready` | All Phase 4 acceptance criteria satisfied ([ADR 019](019-node-onboarding.md#phase-4--accepting-paid-delivery)); serving traffic. |
| `degraded` | Running but one or more non-critical conditions impaired (e.g., DHT routing table sparse, a chain-event watcher in backoff). Traffic still accepted. |
| `not_ready` | A mandatory startup check failed or is incomplete (blacklist un-synced, rate floor not loaded, not registered). Not accepting traffic. |

HTTP status codes: `200` for `ready` and `degraded`; `503` for `not_ready`. Monitoring systems SHOULD alert on `503` responses.

### Structured Logging

Metrics cover aggregates; structured logs cover per-event detail. Logs complement metrics; they do not replace them.

- **Library:** `tracing` crate (standard in the iroh ecosystem).
- **Format:** JSON (`tracing-subscriber` `json` formatter) for production machine consumption. Human-readable (`pretty`) available via config flag for local development.
- **Log levels:**
  - `ERROR` — unrecoverable, needs operator intervention (startup failures, RPC unreachable after all retries).
  - `WARN` — recoverable degraded conditions (blacklist poll lag > 1 interval, startup clock skew > 10 s, probe hold slot saturation > 90%).
  - `INFO` — significant lifecycle events (node ready, pool opened/redeemed, registry active-set change, config reloaded).
  - `DEBUG` — per-stream and per-probe events. Not for high-volume production.

**Mandatory log fields** on every event:

- `node_id` — iroh NodeId (hex)
- `ts` — RFC 3339 timestamp
- `level` — log level
- `target` — Rust module path

### Canonical Metric Name Cross-Reference

Each metric series has one canonical `decdn_`-prefixed name; informal short names map to it below. **Instrumentation names only** — no wire protocol or on-chain surface.

| Informal name | Canonical name | Source ADR |
|---------------|----------------|------------|
| `probe_hold_violations` | `decdn_probe_hold_unavailable_total{reason="exhausted"}` | [ADR 005](005-protocol.md#adr-005-wire-protocol), architecture.md |
| `probe_holds_disabled` | `decdn_probe_hold_unavailable_total{reason="disabled"}` | [ADR 005](005-protocol.md#adr-005-wire-protocol) |
| `probe_stake_lane_reserved` | `decdn_probe_hold_unavailable_total{reason="stake_lane_reserved"}` | [ADR 003 § Admission and Priority](003-payments.md#admission-and-priority) |
| `probe_hold_slots_used` | `decdn_probe_hold_slots_used` | [ADR 005](005-protocol.md#adr-005-wire-protocol), architecture.md |
| `rate_bounds_clamp_events` | `decdn_rate_bounds_clamp_events_total` | architecture.md |
| `probe_collection_latency_seconds` | `decdn_probe_collection_latency_seconds` | [ADR 001 § Probe response collection](001-network.md#probe-response-collection) |
| `streams_active` | `decdn_streams_active` | architecture.md |
| `streams_completed` | `decdn_streams_completed_total` | architecture.md |
| `streams_failed` | `decdn_streams_failed_total` | architecture.md |
| `vouchers_signed` | `decdn_vouchers_signed_total` | architecture.md |
| `vouchers_received` | `decdn_vouchers_received_total` | architecture.md |
| `channels_open` | `decdn_lanes_open` | architecture.md |
| `channels_settled` | `decdn_pool_redemptions_total` | architecture.md |
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
