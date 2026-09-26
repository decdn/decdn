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

**`decdn_probe_hold_unavailable_total{reason}` is the one deliberate exception among reason splits.** Its three values share one *aggregate* and one budget axis (all three derive from `max_probe_holds`), so operators query the aggregate first and drill in on the label second — the shape a label serves well. They pointedly do **not** share an alert: the alert filters to `reason="exhausted"`, because `disabled` is an intentional operator choice and `stake_lane_reserved` has its own knob; being able to express that filter is part of what the label buys. Note this is the one labelled *reason split*, not the only labelled metric — `decdn_streams_active{direction}` and `decdn_staker_set_active_by_region{node_region}` are labelled on other axes. A new split should follow the sibling convention unless it meets that same bar. Either way the invariant is absolute: **no metric may silently stop exporting at zero**, and an alert whose remedy applies to only one reason must carry the corresponding filter. `decdn_staker_set_active_by_region{node_region}` is the one exception. A region with no active node has no series, so the world map shows no empty country. The name is absent when no active node has a valid region. `decdn_staker_set_active_unknown_region` and `decdn_staker_set_active_count` always export, and they show the zero case.

All metrics are exported in **Prometheus text format 0.0.4** on a configurable HTTP port (default `9090`) at `/metrics`. Health is separate: it is the `admin_v1_health` JSON-RPC method on the admin listener, not a route on this port (see [Health](#health)). The port MUST be operator-configurable and MUST NOT be publicly accessible without authentication in production (firewall or auth proxy).

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

These give early warning for the two slashable offenses in [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn), grouped here with the closely-related probe-hold capacity metrics. Blacklist-watcher liveness is monitored through `decdn_blacklist_watcher_last_tick_timestamp_seconds` and `decdn_blacklist_watcher_down_seconds` (the `DecdnBlacklistWatcherStalled` rule in `monitoring/` reads both); the watcher rebuilds the deny-set by full enumeration and keeps no version cursor, so there is no sync-lag gauge to alert on. The probe-hold gauges (`decdn_probe_hold_slots_used`/`_max`) are live and normally non-zero — alert on the thresholds/rates in the table below, not on presence. `decdn_probe_hold_unavailable_total` is an availability signal, not a slash risk — see its row, and alert only on `reason="exhausted"`, the budget-pressure value.

| Metric | Type | Tier | Status | Description |
|--------|------|------|--------|-------------|
| `decdn_probe_hold_unavailable_total{reason}` | Counter | M | live | A probe for a present blob that got **no** eviction hold ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)). Holds are best-effort, so this is not the same as answering `has_blob: false` — see the per-reason split. Never a safety fault: no offense pairs a probe with a later miss ([ADR 014](014-on-chain-verification.md#rate-manipulation)), and an unheld advertisement that loses the eviction race costs one wasted round trip. The `reason` label carries the cause, because each has a different operator remedy: **`exhausted`** — blob present but **all** hold slots were live (`max_probe_holds` reached); the node still advertises `has_blob: true` and forgoes only the hold, so the blob may be LRU-evicted before the pull. Genuine budget pressure and the only value the "raise `max_probe_holds`" alert fires on. **`disabled`** — blob present but the hold path is **off by config** (`max_probe_holds == 0`); the one reason that also suppresses the advertisement (`has_blob: false` for store-backed content; origin-held content takes no hold and is unaffected). An intentional operator choice, so alerting on it would be nonsensical. **`stake_lane_reserved`** — an end-client probe hit the stake-lane-reserved end-client ceiling (`max_probe_holds − cache.stake_lane_reserved_holds`), keeping hold headroom for registered node-to-node cache-miss probes ([ADR 003 § Admission and Priority](003-payments.md#admission-and-priority)). The reservation is a content-independent admission decision taken before any hold attempt, but the handler still consults the cache to answer honestly and advertises a present blob. Stays zero unless `cache.stake_lane_reserved_holds > 0`. All three children are exported at zero from startup, so a missing series means a broken exporter, not an idle node. |
| `decdn_probe_hold_slots_used` | Gauge | M | live | Eviction-hold slots in use out of `max_probe_holds`. Saturation means new probes are advertised without a hold, not answered `has_blob: false`. |
| `decdn_probe_hold_slots_max` | Gauge | M | live | Configured `max_probe_holds`. Paired with `decdn_probe_hold_slots_used` for a saturation ratio. |

**Recommended alert thresholds:**

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| `decdn_probe_hold_unavailable_total{reason="exhausted"}` (rate) | > 0 | > 0 sustained | Reduce load or increase `max_probe_holds`; check for OOM. Filter on `reason="exhausted"` — the `disabled` and `stake_lane_reserved` values are deliberate operator decisions and must not trip this alert. |

#### Delivery Metrics (`cdn/client/v1`)

| Metric | Type | Tier | Status | Labels | Description |
|--------|------|------|--------|--------|-------------|
| `decdn_streams_active` | Gauge | M | live | `direction={inbound,outbound}` | Currently open delivery streams. |
| `decdn_streams_completed_total` | Counter | M | live | `direction={inbound,outbound}` | Streams that delivered the whole request. `inbound` counts streams this node served. `outbound` counts node-to-node pulls this node made: one per upstream candidate it opened a stream to on the buffered miss path, and one per assembled range on the streaming miss path. |
| `decdn_streams_failed_total` | Counter | M | live | `direction={inbound,outbound}` | Streams that ended without the whole request: refused, stopped mid-stream, reset, panicked, or ended on an error. Every ended stream counts once in this counter or in `decdn_streams_completed_total`. A stream that shutdown cancels counts in neither. Routine outcomes count here too: a cache-miss refusal, a requester that declines the quote, and a client that leaves mid-stream. So do not read `completed / (completed + failed)` as health. Read the sibling counters `decdn_serve_stream_*` and `decdn_node_pull_*`, per [§ Reason splits](#reason-splits-sibling-counters-not-labels). Each `inbound` failure increments exactly one `decdn_serve_stream_*` reason counter. The node crate lists them in `INBOUND_FAILURE_REASONS`. So `inbound` minus their sum is zero. A positive residual is a failure that no reason claims. It is a bug. |
| `decdn_bytes_served_total` | Counter | M | live | — | Payload bytes this node wrote to clients and downstream nodes, counted per frame as it is written. |
| `decdn_bytes_received_total` | Counter | M | live | — | Payload bytes this node admitted from upstream nodes in paid node-to-node pulls, counted per verified range. The range is chunk-group aligned, so the count can exceed the requested bytes. |
| `decdn_serve_first_byte_hit_seconds` | Histogram | R | live | — | Time to first byte of a paid serve that the blob-availability gate classes as a hit, complete or partial. The window starts when the node decodes the request. The window stops when the node writes the first `ChunkData` frame. The first frame uses the opening credit window, so the window does not include a client payment round trip. The window includes one `getPool` chain read when the pool view has no entry for the pool. A stream that ends before its first frame records nothing: a refusal, a reset, or an empty blob. The value does not depend on blob size, so a latency SLO can use it. Buckets: `[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]`. The top bucket reaches past the outer pull deadline at the default timeouts, so a slow miss does not fall into `+Inf` alone. |
| `decdn_serve_first_byte_miss_seconds` | Histogram | R | live | — | Time to first byte of a paid serve that the gate classes as a miss. The window is the same as in `decdn_serve_first_byte_hit_seconds`. It also includes the discovery and the fill that produce the first frame. The gate sets the class. The serve loop does not set it. A buffered fill serves a miss through the cache-hit delivery loop, and that serve records in this histogram. A window-paced fill sends its first frame before the fill completes. A buffered fill completes the whole blob before the first frame, so on that tier the value increases with blob size. This histogram is a sibling of the hit histogram, per [§ Reason splits](#reason-splits-sibling-counters-not-labels). A slow hit and a slow miss have different remedies. Buckets: `[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]`. The top bucket reaches past the outer pull deadline at the default timeouts, so a slow miss does not fall into `+Inf` alone. |
| `decdn_node_pull_first_byte_seconds` | Histogram | R | live | — | Time to first byte of one paid node-to-node pull leg. The window starts before the node opens the pull. The window includes a dial when the node has no warm connection to the peer. The window stops when the node reads the first bao bytes of the leg. Each leg is one upstream request and records once. When a drive adopts a header-handshake pull as its first leg, the window starts before the handshake open. A leg that reads no bytes records nothing. Buckets: `[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]`. The top bucket reaches past the outer pull deadline at the default timeouts, so a slow miss does not fall into `+Inf` alone. |
| `decdn_node_pull_through_wait_seconds` | Histogram | R | live | — | Time one window-paced pause holds the upstream pull. A pause starts when the pull reaches its credit window or waits for the minimum draw. It ends when a downstream payment or a parked serve leg releases the pull, or when the leg cancels the pause. Each pause records once. Most pauses end within a voucher round trip. A tail near the top bucket means a stalled payer or a pacing regression that parks the pull with no serve leg to wake it. A pause past 30 s also logs a warning. Buckets: same as `decdn_node_pull_first_byte_seconds`. |
| `decdn_serve_stream_midstream_pool_exhausted_total` | Counter | R | live | — | Paying streams this node stopped mid-delivery with `PoolExhausted`: the pool can no longer fund the floor credit committed across its signers. The pool-level sibling of `decdn_serve_stream_midstream_signer_cap_exhausted_total`. The owner must top up. |
| `decdn_serve_frame_accounting_fault_total` | Counter | R | live | — | Times a serve leg refused to cut or frame a `ChunkData` because its own byte accounting did not add up: a zero frame target, a `queued`/`queue` desync, a payload whose chunks disagree with the length the header would declare, or a header the encoder refused. Each refusal is correct — the delivery aborts without `StreamEnd`, so the client gets no mislabelled frame and pays no closing voucher — which is exactly why it is otherwise invisible: the stream simply ends, and that reads as a client that hung up. This is a **latent-bug report, not a degradation**. Alert on `> 0` and file it rather than tuning anything. |
| `decdn_serve_stream_node_fault_total` | Counter | R | live | — | Times a `cdn/client/v1` delivery ended on a fault this node caused: an encode fault, an alignment error, a store fault, or a framing fault. A peer hang-up and a client payment fault are excluded. They are routine, and other reason counters count them. The client sees only a short stream, so this counter and the `error!` line beside it are the operator's sole signal that a delivery was abandoned. This is a **latent-bug report, not a degradation**. Alert on `> 0` and file it. It is a superset of `decdn_serve_frame_accounting_fault_total`, which meters one of the four classes on its own. |
| `decdn_serve_stream_proof_budget_exhausted_total` | Counter | R | live | — | Times a `cdn/client/v1` delivery ended because the payer sent the per-chunk proof limit (`MAX_PROOFS_PER_CHUNK`, 8) for one chunk and no proof settled that chunk. The node counts it for the cache-hit serve loop and for the miss serve loop. On the miss loop, the same stop also increments `decdn_node_pull_through_client_abandoned_total`. This is a fault of the payer, not a node fault. The serve loop logs it at `debug!` and stops the stream. At the default log level, this counter is the operator's only signal. A payer that pays one chunk in many small parts, or with vouchers that credit nothing, reaches this limit. The node then ends the stream. Each increment ends exactly one inbound stream as failed. |
| `decdn_serve_stream_rejected_bad_binding_total` | Counter | R | live | — | Streams reset because the client binding failed to verify: the signature is invalid, or it recovers a different address than the one claimed. No signed response is sent. Any peer can cause these, so the matching `warn!` line is throttled; this counter records every event. |
| `decdn_serve_stream_rejected_stream_cap_full_total` | Counter | R | live | — | Streams reset with `RATE_LIMITED` because the connection already had `max_concurrent_streams` streams in flight. No signed response is sent. A client that opens more concurrent streams than the cap causes these. |
| `decdn_serve_stream_request_unreadable_total` | Counter | R | live | — | Streams reset because the node could not read the first request. The read timed out, the peer reset the stream or closed the connection, or the frame did not decode. The cause is the peer. |
| `decdn_serve_stream_voucher_rejected_total` | Counter | R | live | — | Paying streams this node stopped because it rejected a voucher or a chunk preimage. Examples are a wrong pool or signer, a bad signature, an underpayment, a regression, an expired capability, and a voucher that fails the rate check. The node counts it for the cache-hit serve loop and for the miss serve loop. On the miss loop, the same stop also increments `decdn_node_pull_through_client_abandoned_total`. The node counts the rejection here also when the peer leaves before it reads the reject frame. |
| `decdn_serve_stream_client_declined_total` | Counter | R | live | — | Streams that the requester left before a voucher credited a byte. The usual cause is a requester that reads the signed `StreamResponse` and does not accept the quote. The header handshake that a downstream miss pull opens and does not adopt is one example. A peer that sends a malformed proof before it pays also counts here. The reference requester closes every stream with code `0`, and the node does not read the code. So the node cannot see why the peer left. It sees only that no voucher credited a byte. |
| `decdn_serve_stream_client_abandoned_total` | Counter | R | live | — | Paying streams that the requester left, or broke the protocol on, after a voucher credited bytes. The requester stopped reading, reset the stream, closed the connection, stopped sending proofs, or sent a malformed proof. A fully paid stream whose `StreamEnd` write fails also counts here. A sustained rise against `decdn_streams_completed_total{direction="inbound"}` shows clients that give up during delivery. |
| `decdn_serve_stream_rejected_load_shed_hit_total` | Counter | R | live | — | Cache-hit serves shed under overload. The cache-class split of a load-shed refusal; the reason split is the `decdn_load_shed_refused_*` siblings below. |
| `decdn_serve_stream_rejected_load_shed_miss_total` | Counter | R | live | — | Cache-miss serves shed under overload. Sibling of `decdn_serve_stream_rejected_load_shed_hit_total` on the cache-class axis. |
| `decdn_load_shed_egress_bps` | Gauge | R | live | — | Current measured egress EWMA in bytes/sec, the live value the `ResourcePressure` policy checks against `egress_budget_mbps`. |
| `decdn_load_shed_pressure_active` | Gauge | R | live | — | 1 while the load-shed policy considers the node pressured, else 0. Says whether the concurrency mark is crossed; `decdn_load_shed_streams_in_flight` gives the headroom. |
| `decdn_load_shed_streams_in_flight` | Gauge | R | live | — | Node-wide paid serves in flight, sampled from the shed controller. Read against `max_concurrent_serves_high` for how close the node is to shedding on concurrency. |
| `decdn_load_shed_refused_node_at_capacity_total` | Counter | R | live | — | New serves shed because node-wide concurrency was above the high-water mark. A reason sibling, not a label, per [§ Reason splits](#reason-splits-sibling-counters-not-labels): the remedy is add capacity or raise `max_concurrent_serves_high`. Orthogonal to the hit/miss cache-class split above. |
| `decdn_load_shed_refused_egress_saturated_total` | Counter | R | live | — | New serves shed because measured egress reached the configured budget. Remedy: raise `egress_budget_mbps` or add bandwidth. |
| `decdn_load_shed_refused_client_at_capacity_total` | Counter | R | live | — | New serves shed because one client already held its fair share while the node was pressured. Fairness working as designed, not node distress — dashboard- and log-only, deliberately not alerted (like `decdn_serve_stream_rejected_signer_floor_at_cap_total`). |
| `decdn_serve_economics_refused_total` | Counter | R | live | — | Cache-miss buys declined because every candidate quoted above this node's serve-economics buy ceiling ([ADR 041 § The buy ceiling](041-refuse-to-serve.md#the-buy-ceiling)). A buyer-side policy decision, so it does not tar the provider. The node signs the client `NotFound`, so the pricing floor never reaches the wire and a sustained rate is visible only here. The aggregate over the two regime siblings below. |
| `decdn_serve_economics_refused_warming_total` | Counter | R | live | — | The subset of `decdn_serve_economics_refused_total` where the source still had warming allowance, so the ceiling was the market price `max(sell, amortized)` and the market is above it ([ADR 041 § The buy ceiling](041-refuse-to-serve.md#the-buy-ceiling)). A reason sibling, not a label, per [§ Reason splits](#reason-splits-sibling-counters-not-labels): no local config fixes it, so it is dashboard-only. |
| `decdn_serve_economics_refused_amortized_total` | Counter | R | live | — | The subset of `decdn_serve_economics_refused_total` where the source's warming allowance was spent (or warming is off), so the ceiling was the amortized floor ([ADR 041 § The buy ceiling](041-refuse-to-serve.md#the-buy-ceiling)). Remedy: this node's own sell rate or margin config is too tight — the operator-actionable half, and the one the `DecdnServeEconomicsTooTight` alert fires on. |

#### Cache Metrics

| Metric | Type | Tier | Status | Description |
|--------|------|------|--------|-------------|
| `decdn_cache_bytes` | Gauge | M | live | Total cache size in bytes (all blobs). Paired with `decdn_cache_size_limit_bytes` for a saturation ratio. |
| `decdn_cache_size_limit_bytes` | Gauge | R | live | Configured cache capacity in bytes (`cache.cache_size_mb × 1 048 576`). See [ADR 040](040-cache-policy.md#adr-040-pluggable-cache-admission-and-eviction-policies). |
| `decdn_cache_hits_total` | Counter | M | live | `CacheEngine::get` calls satisfied from the local store. Scoped to that whole-blob buffered read, which the paid serve path never calls — read `decdn_serve_cache_hit_total` for the serve-path hit rate. |
| `decdn_cache_misses_total` | Counter | M | live | Local-store lookups that did not find the blob. Wider scope than `decdn_cache_hits_total`: bumped by `CacheEngine::get`, and also by `populate`/`populate_local` and `pull_through_range`, which the serve-path fill tiers do call. Pairing it with the `get`-only `hits` therefore yields a populated, permanently-0% ratio on a serving node rather than an empty one. |
| `decdn_cache_bytes_returned_total` | Counter | R | live | Bytes returned from `CacheEngine::get` to the caller on success. Counts both cache-hit and pull-through-success paths. `get`-scoped like `decdn_cache_hits_total`, so a node that only serves paying clients holds this at zero — measure delivered bytes with `decdn_bytes_served_total`, noting that it counts the bao wire form (content plus interleaved proof nodes, over chunk-group-aligned ranges) written to clients and to downstream nodes, so it runs a few percent above raw content delivered. |
| `decdn_serve_cache_hit_total` | Counter | M | live | Paid serves the blob-availability gate classified as servable from a complete locally-held blob, before any fill tier runs. Counted ahead of the load-shed gate, so the hit rate stays a property of the store rather than of current pressure. Sibling of `decdn_serve_cache_partial_hit_total` and `decdn_serve_cache_miss_total`; at most one of the three is bumped per request. Two requests that reach the gate bump none: a withdrawn hash (operator evict, corruption quarantine) is refused `EvictedSinceProbe`, and a `serve_audit` store fault is refused `InternalError`. Neither is an availability class, so the ratio's denominator excludes both. |
| `decdn_serve_cache_partial_hit_total` | Counter | R | live | Paid serves admitted from a blob that is not `Complete` but whose held chunk groups already cover the requested span ([ADR 022 § Range-keyed partial-holder discovery](022-content-discovery.md#range-keyed-partial-holder-discovery)). A hit for hit-rate purposes, counted apart so the payoff of partial-holder advertisement stays legible. |
| `decdn_serve_cache_miss_total` | Counter | M | live | Paid serves the gate could not satisfy from held bytes. Counts the admission decision and nothing downstream of it: the bump precedes the load-shed gate, the pre-spend floor reservation and the `pull_authorized` check every fill tier is gated on, so a shed refusal, a floor refusal and an unbound request all count here having asked no origin and no peer. A miss a tier does satisfy still counts here; the `decdn_serve_stream_rejected_*` siblings say whether it ended in a refusal. Also absorbs a store fault during the partial-coverage lookup, which reads as absence. |
| `decdn_cache_pull_through_bytes_total` | Counter | R | live | Bytes received from origin during a cache miss, counted regardless of BLAKE3 verification or store landing — origin egress is paid either way. On the range-pull path it includes each `{H}.obao4` outboard the origin serves; a cached outboard is not counted again. Independent of `decdn_cache_bytes_returned_total`: equal values mean pure pass-through; `bytes_returned_total >> pull_through_bytes_total` indicates effective caching. |
| `decdn_cache_fill_not_coalesced_total` | Counter | R | live | Serve-miss fill claims not coalesced onto a live same-hash fill whose covered range overlaps the request, because the request starts ahead of that fill's paid frontier (decdn#2062). Each claim counts once. The refused attach opens its own pull. The part of the overlap that is not yet in the store when that pull starts is fetched from origin twice. An overlap already in the store is not fetched again. A sustained rate means clients habitually resume ahead of what payers have cleared. |
| `decdn_cache_evictions_total` | Counter | R | live | Blobs evicted by LRU pressure (eviction-driver loop). See [ADR 040](040-cache-policy.md#adr-040-pluggable-cache-admission-and-eviction-policies). |
| `decdn_cache_evicted_operator_total` | Counter | R | live | Hashes removed via `decdn node evict` (durable, persisted to `<cache_dir>/evicted.log`). Distinct from `decdn_cache_evictions_total`. See [ADR 040 § Pinning, durable operator-evict, and the probe-hold stay engine-enforced](040-cache-policy.md#pinning-durable-operator-evict-and-the-probe-hold-stay-engine-enforced). |
| `decdn_cache_held_corruption_quarantined_total` | Counter | R | live | Held hashes quarantined because a serve export failed bao validation against the content root. The stored bytes or outboard changed after admission. The node stops serving and announcing the hash and releases the entry to GC. The quarantine is not durable, and the hash is re-acquirable after the sweep. Distinct from `decdn_cache_evicted_operator_total`. A nonzero rate means disk rot or tampering under the cache directory. See [ADR 040 § Pinning, durable operator-evict, and the probe-hold stay engine-enforced](040-cache-policy.md#pinning-durable-operator-evict-and-the-probe-hold-stay-engine-enforced). |
| `decdn_cache_inflight_mutex_poisoned_total` | Counter | R | live | Times a task panicked while holding the in-flight fill-coalescing map, counted once per poisoning (the engine clears the poison, so this does not climb with request volume). The engine recovers the guard and coalescing is preserved, so this is a **latent-bug report, not a degradation** — any nonzero value is worth filing rather than tuning, and needs no restart. Alert on `> 0`. See [docs/runbook.md § Cache coalescing mutex poisoned](../docs/runbook.md). |
| `decdn_cache_origin_probe_failures_total` | Counter | R | live | Origin size probes that faulted during an origin rescan — one or more configured origins returned a transport error and none confirmed the object, or the probe walk overran its ceiling. A rescan decides what this node advertises. A retry-eligible fault keeps whatever entry an earlier pass indexed, so a sustained nonzero rate means the node may still advertise content the origin has stopped holding, and every request for it is a refusal; a permanent fault (a revoked ACL, a symlink escape) is not carried, and a candidate first seen inside the fault window has nothing to carry. Correlate with the origin backend's own error rate. An HTTP origin reports a *server-side* 5xx as a plain negative, so that one case is not counted; its transport faults, S3/R2 and the filesystem are. |
| `decdn_cache_origin_enumerate_failures_total` | Counter | R | live | Origins whose `enumerate` failed during a rescan, counted once per origin per rescan. The sibling of `decdn_cache_origin_probe_failures_total` for the other leg of the same rescan, and the more severe of the two: a failed listing produces no candidates, so every hash discoverable only through that origin leaves the announce set with no per-hash fault and nothing to carry forward — only operator pins naming those hashes survive. |
| `decdn_cache_origin_range_timeouts_total` | Counter | R | live | Origin reads on the range-pull path (an `{H}.obao4` fetch or one data window) that ran past their time budget: `cache.node_pull_stall_window_sec` plus the read size at `cache.node_pull_min_throughput_bps` ([ADR 037](037-regional-proxy-warming.md#origin-tier-pull-through-ranged-fetch--external-outboard)). Each timeout is also an origin fault, so it advances the origin chain or fails the fill. A nonzero rate means an origin that stalls or runs below the throughput floor. |
| `decdn_cache_range_pull_permit_waits_total` | Counter | R | live | Own-origin range draws that found the engine-wide pool of range-pull permits full and waited for a permit. Every draw of every fill shares the pool. A slow or hung origin holds a permit for a whole draw. A wait past 10 s also logs a warning. A sustained rate means more concurrent own-origin fills than the pool serves, or an origin slow enough to hold permits. Check `decdn_cache_origin_range_timeouts_total` and the origin latency. |
| `decdn_cache_pinned_count` | Gauge | R | live | Size of the operator-pinned set (LRU-exempt). See [ADR 040 § Pinning, durable operator-evict, and the probe-hold stay engine-enforced](040-cache-policy.md#pinning-durable-operator-evict-and-the-probe-hold-stay-engine-enforced). |
| `decdn_probe_post_eviction_failures_total` | Counter | R | live | `EvictedSinceProbe` responses from remote nodes during cache-hit stream requests. A sustained rate above ~1% of cache-hit attempts suggests remote hold mechanism failures ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh), [ADR 005](005-protocol.md#adr-005-wire-protocol)). |
| `decdn_local_outboard_serves_total` | Counter | R | live | Times a cache miss of any request shape (whole blob, bounded range, resumed tail) was filled by streaming straight from the node's own configured origin into the paying client while admitting the bytes into the local cache store — the stream-while-store path ([ADR 037 § Origin-tier miss](037-regional-proxy-warming.md#origin-tier-miss-stream-while-store)), entered only when the origin publishes a `{H}.obao4` outboard. Incremented on entry, before any admission guard, so it marks which serve tier fired rather than whether the fill succeeded. A miss that instead falls back to the buffered whole-blob import path never bumps this counter, which is what lets a dashboard tell the two fill strategies apart. |
| `decdn_warming_credits_applied_total` | Counter | R | live | Speculative-warming serve credits the background aggregator applied to a source's allowance ledger ([ADR 041 § Negative](041-refuse-to-serve.md#negative)). Read it beside `decdn_warming_credits_dropped_total`: a node whose serve path is not wired to a credit sink enqueues nothing and therefore drops nothing, so zero drops is only good news next to a climbing apply count. A node serving source-tagged blobs must show this rising. |
| `decdn_warming_credits_dropped_total` | Counter | R | live | Serve credits that never reached the allowance ledger ([ADR 041 § Negative](041-refuse-to-serve.md#negative)) — the bounded aggregator queue was full, or the aggregator task was gone. Every drop is conservative: it leaves the source's allowance lower than its true profit and loss, so it can only throttle warming from that source, never over-fund it. A sustained rate means warming is throttled by lost bookkeeping rather than by real losses; a rate that tracks the serve rate means the aggregator died and every source will drift to blocked. |
| `decdn_warming_speculative_blocked_total` | Counter | R | live | Times an above-floor (speculative) warming buy was downgraded to the amortized floor because the upstream source's per-source allowance was spent ([ADR 041 § Negative](041-refuse-to-serve.md#negative)). Counted only when warming is enabled (`warming_budget > 0`), so a node with warming off does not report every source blocked. A sustained rate means sources are being griefed down; pair with `decdn_warming_sources_blocked`. |
| `decdn_warming_sources_blocked` | Gauge | R | live | How many upstream sources currently hold no warming allowance — a spent ledger at or below zero with its time refill projected forward ([ADR 041 § Negative](041-refuse-to-serve.md#negative)), sampled beside the egress EWMA. A rising gauge alongside a rising `decdn_warming_speculative_blocked_total` is warming being throttled by real per-source losses; either at zero means warming is unconstrained. |

#### Probe Metrics (`cdn/probe/v1`)

| Metric | Type | Tier | Status | Labels | Description |
|--------|------|------|--------|--------|-------------|
| `decdn_probe_collection_latency_seconds` | Histogram | M | live | — | Duration of one probe collection window ([ADR 001 § Probe response collection](001-network.md#probe-response-collection)). The window starts when the concurrent probes start, and it includes their dials. Collection ends at the early stop, or when every probe resolves. The 500 ms probe timeout bounds each probe. A round that sends no probe records nothing. A probe-cache hit sends no probe. Buckets: `[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]`. |
| `decdn_probe_requests_total` | Counter | R | live | — | Probe requests served by this node, incremented once the response frame is written. The serve-side counterpart to `decdn_probe_responses_total` (which counts probes this node *sent* and got answers to). Flat while a node is reachable but unprobed; the first signal that a restarted or re-keyed node is being found again. |
| `decdn_probe_read_faults_total` | Counter | R | live | — | Probe requests this node could not read: the read timed out, the frame or message failed to decode, or the peer sent a response on the server stream. Each one closes the connection with its code from [ADR 013 § Application Error Codes](013-schema-evolution.md#application-error-codes). Any peer can cause these, so the matching `warn!` line is throttled; this counter records every event. |
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
| `decdn_pool_redemptions_total` | Counter | M | live | Lane claims this node redeemed on-chain in a landed `redeemMany`. Each lane redeems one cumulative voucher, however many vouchers the lane accepted. |
| `decdn_onchain_tx_landed_total` | Counter | R | live | Settlement transactions (`redeemMany`) that mined and succeeded. The outcome family is `landed`, `reverted`, `send_failed`, `receipt_failed` and `timeout`. It counts each transaction the node sends through its settlement path one time. `decdn_onchain_tx_receipt_recovered_total` is not part of this family. Buyer-side pool transactions go through `decdn-client` and do not count. |
| `decdn_onchain_tx_reverted_total` | Counter | R | live | Node transactions that mined and reverted. |
| `decdn_onchain_tx_send_failed_total` | Counter | R | live | Node transactions the RPC refused at `send`. No transaction was issued. An oversize `redeemMany` that the redeemer then halves and retries counts here once. |
| `decdn_onchain_tx_receipt_failed_total` | Counter | R | live | Issued node transactions whose receipt wait failed. The node then fetches the receipt by hash three times, and none of the fetches found it. The transaction is unconfirmed and can still mine. Its hash is in the `warn!` line. |
| `decdn_onchain_tx_timeout_total` | Counter | R | live | Issued node transactions whose receipt did not arrive inside the caller's bound. The node then fetches the receipt by hash three times, and none of the fetches found it. The transaction is unconfirmed and can still mine. Its hash is in the `warn!` line. |
| `decdn_onchain_tx_receipt_recovered_total` | Counter | R | live | Node transactions whose receipt wait failed or timed out, but whose receipt a fetch by hash then found. Each one also counts once as landed or reverted, so this counter is not part of the outcome family. A steady rate shows an RPC provider that lags or balances load across backends that lag. |
| `decdn_redemption_failures_total` | Counter | M | live | Redemption steps that failed. A step fails when a `redeemMany` chunk reverts, when the RPC refuses it at `send`, or when the redeemer cannot plan or load a lane. An unconfirmed receipt does not count here. The claims stay and retry on the next sweep. |
| `decdn_pool_deposit_usdc` | Gauge | M | live | USDC still recoverable from the pools currently paying this node: the sum of `deposit - totalRedeemed` over the distinct pools the redeemer plans lanes against. Already-redeemed funds have left the pool, so this is the ceiling those pools can still pay, not their lifetime deposits. Refreshed once per redeemer self-tick, beside `decdn_unredeemed_usdc`. |
| `decdn_buyer_wallet_usdc` | Gauge | M | live | USDC in this node's own buyer wallet — what it can still escrow when it opens a payment pool on its cache-miss leg. An unfunded wallet reverts every `openPool` on the ERC-20 transfer, which reads as a node that serves perfectly and buys nothing. Read once per reclaim sweep, so it lags a spend by up to one interval. Zero is meaningful only where `cache.node_to_node_pull_through_enabled` is on; a cache-only node never opens a pool. |
| `decdn_buyer_lane_seed_failures_total` | Counter | M | live | Pulls refused because this node could not establish a lane's already-paid watermark. Resuming such a lane from zero is permanent, not per-pull: the pull persists its own progress on every exit path, which gives the lane a local row and stops the reseed ever running for that provider again. The node refuses instead. A sustained rate means the chain lane or the buyer store is unhealthy and this node is buying nothing from the affected providers. |
| `decdn_buyer_pool_adoption_failures_total` | Counter | M | live | Bootstraps that could not determine whether this node already owns a payment pool on chain, so the first cache miss opens one. Every increment is a chance the node escrows a second deposit beside one it already holds. Adoption runs once per process, so this does not self-correct before the next restart. |
| `decdn_unredeemed_usdc` | Gauge | M | live | Raw USDC in accepted vouchers this node has not redeemed on-chain. The value sums `owed − paid` over the lanes the redeemer plans to collect. The redeemer refreshes it once per self-tick, so it lags live accrual by up to one `redeem_interval_secs`. |
| `decdn_buyer_pool_store_skipped_undecodable_records_total` | Counter | M | live | Buyer-pool rows omitted from successful store hydration because their persisted values cannot be decoded. One bad row does not stop healthy pools from loading or being reclaimed; each load attempt counts every omitted row, so any increase means a buyer deposit is escrowed but untracked and requires record repair. |
| `decdn_vouchers_signed_total` | Counter | M | planned | Vouchers this node signed as the payer (node-to-node pulls). |
| `decdn_vouchers_received_total` | Counter | R | live | Signed vouchers this node accepted as the payee on an inbound stream. `PayWord` preimage reveals are not vouchers and do not count. A `PayWord` stream counts its anchor voucher. |
| `decdn_pool_grace_closes_total` | Counter | R | live | Pools that entered the owner-close grace window while this node held unredeemed vouchers on them. The node reads the flushed lane store when the close event arrives. Each one is revenue that must redeem before the grace window ends. A pool that is already closing at startup does not count. |

#### Reputation Metrics

Reputation is local-only per [ADR 008](008-reputation.md#adr-008-reputation-system) — no cross-node propagation, so the only reputation metric is the local score gauge.

| Metric | Type | Tier | Status | Description |
|--------|------|------|--------|-------------|
| `decdn_reputation_score` | Gauge | R | planned | This node's current local reputation score (0.0–1.0) for a peer, computed from its own delivery observations per [ADR 008](008-reputation.md#adr-008-reputation-system). |

#### Node / Process Metrics

| Metric | Type | Tier | Status | Description |
|--------|------|------|--------|-------------|
| `decdn_node_uptime_seconds` | Gauge | R | live | Seconds since the node process started. Mirrors `admin_v1_health`'s `uptime_s`; operator dashboards use it to correlate events with restarts. |
| `decdn_config_reload_failures_total` | Counter | R | live | SIGHUP or admin config reloads that failed. Sections committed before the failure stay applied. The other sections keep their previous values. |
| `decdn_receipt_write_failures_total` | Counter | R | live | Download-receipt audit records the writer failed to persist: an I/O error or a panicked write task. Audit only; settlement is unaffected. |
| `decdn_otlp_export_failures_total` | Counter | R | live | OTLP span-export batches whose export call failed: connect error, non-OK gRPC status, or timeout. A sustained rate means traces are lost. The counter does not count spans that the batch queue drops when full, or spans that a collector rejects inside an OK partial-success reply, so 0 does not prove that no traces were lost. The SDK logs queue drops as `BatchSpanProcessor.SpanDroppingStarted` and `BatchSpanProcessor.SpansDropped` under the `opentelemetry_sdk` target. Stays 0 when `observability.otlp_endpoint` is unset or the node emits no spans. |

#### DHT / Content-Discovery Metrics

Per [ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale) (`cdn/dht/v1`). DHT STORE and FIND_VALUE carry no protocol-level fee; these metrics expose discovery health only.

| Metric | Type | Tier | Status | Labels | Description |
|--------|------|------|--------|--------|-------------|
| `decdn_dht_store_published_total` | Counter | R | live | — | DHT STORE records a peer accepted from this node ([ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale)). A batched STORE adds one per accepted hash. |
| `decdn_dht_findvalue_queries_total` | Counter | R | live | — | DHT FIND_VALUE lookups this node ran to discover providers. |
| `decdn_dht_lookup_round_timeouts_total` | Counter | R | live | — | Lookup rounds that hit the round timeout and aborted their in-flight RPCs. |
| `decdn_dht_routing_table_size` | Gauge | R | live | — | Distinct entries in the local Kademlia routing table, set after bootstrap and on every bucket-refresh tick. |
| `decdn_dht_bucket_refresh_failures_total` | Counter | R | live | — | Bucket refreshes that failed: a `FIND_NODE` RPC error, a routing-table peer that is not a valid public key, or a panicked refresh task. |
| `decdn_dht_bootstrap_find_node_failures_total` | Counter | R | live | — | Bootstrap `FIND_NODE` RPCs against a seed that failed. |
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
| `decdn_staker_set_active_by_region{node_region}` | Gauge | R | live | `node_region` | Active-staker set size per declared region ([ADR 030](030-node-region-self-attestation.md#adr-030-node-region-self-attestation)). Each node's registry holds the whole network, so one scrape covers every active node. Aggregate across nodes with `max`, not `sum`. The label value passes the region allowlist. A region with no active node has no series (see the exception in [§ Naming Convention](#naming-convention)). The node updates this family at the end of each watcher tick, so a `RegionUpdated` event shows on the next tick. The label is `node_region` because `region` is a scrape-side target label on every reference dashboard. The chain dashboard's world map reads this family. |
| `decdn_staker_set_active_unknown_region` | Gauge | R | live | — | Active stakers whose `regionHint` is empty or is not an accepted region code. These nodes do not appear in `decdn_staker_set_active_by_region`. After each complete watcher tick, the two metrics together sum to `decdn_staker_set_active_count`. The count updates on each event. The region metrics update at the end of the tick. Between ticks, and during a watcher outage, the two can differ. |
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

All five chain-event watchers run through one shared `multiplexed_poller::run` loop. Each watcher exposes the down-family: `*_restarts_total` and `*_down_seconds`. The down-family is **error-triggered**: it moves only when a poll tick fails. A task that panics while healthy, wedges in an await the per-call timeout does not cover, or exits cleanly on shutdown leaves the down-since state unset, so `*_down_seconds` reads a healthy `0` — a dead watcher is byte-identical to a live one. Two additive families close that gap for all five watchers.

In the metric names below, `<watcher>` expands to one of **`slash_watcher`**, **`staker_set_watcher`**, **`blacklist_watcher`**, **`settlement_watcher`**, or **`fee_shares_watcher`**.

| Metric | Type | Tier | Status | Labels | Description |
|--------|------|------|--------|--------|-------------|
| `decdn_<watcher>_last_tick_timestamp_seconds` | Gauge | M | live | — | **Positive liveness signal**: Unix wall-clock time of the last *successful* poll tick, stamped every tick (including idle ticks — a successful head read is proof of life). Unlike the error-triggered down-family, a panicked, wedged, or cleanly-exited task stops advancing this, so staleness is detectable. One series per watcher (`slash`, `staker_set`, `blacklist`, `settlement`, `fee_shares`). Reads `0` until the first successful tick. |
| `decdn_<watcher>_task_panicked_total` | Counter | M | live | — | The watcher task unwound on a panic. Bumped from a `Drop` guard in `multiplexed_poller::run` — the only thing that runs on the unwind, since the detached task is never awaited. **Any non-zero value is a bug in this node.** One series per watcher. |
| `decdn_fee_shares_watcher_poll_failures_total` | Counter | R | live | — | Authoritative `getShares()` re-reads that failed. The fee-shares watcher keeps the current share and its tick still succeeds, so this counter is the only signal that the safety-net re-read is not landing. |
| `decdn_fee_shares_watcher_unregistered` | Gauge | R | live | — | `1` when the startup `PaymentPool.feeRouter()` read failed. The fee-shares watcher then does not run, and the operator share stays at the floor until the node restarts. `0` otherwise. The watcher's down gauges read healthy in that state, because the watcher never started. |
| `decdn_fee_shares_watcher_restarts_total` | Counter | R | live | — | Distinct drift windows the fee-shares watcher entered. The same down-family as the `blacklist` and `settlement` rows. |
| `decdn_fee_shares_watcher_down_seconds` | Gauge | R | live | — | Seconds the fee-shares watcher has been failing its chain read. `0` on a healthy cycle. |
| `decdn_chain_get_logs_span` | Gauge | R | live | — | Block span of the shared chain-event poller's next `eth_getLogs` window. It starts at `blockchain.get_logs_max_block_span`. It drops to half a window that the RPC provider rejects for its range or result count. It doubles after 32 accepted windows, up to the ceiling. A caught-up node climbs back to the ceiling after a transient rejection, so a low value alone does not prove a cap. |
| `decdn_chain_get_logs_range_rejections_total` | Counter | R | live | — | `eth_getLogs` windows the RPC provider rejected for their block range or result count. Each rejection costs one request. Rejections that continue show that the provider caps `eth_getLogs`. The lowest span during those rejections is the cap. |
| `decdn_chain_get_logs_retries_total` | Counter | R | live | — | In-tick retries of `eth_getLogs` windows by the shared chain-event poller after a transient provider error. Each window gets two retries on the same block range, so one window adds up to two. A retry that succeeds does not increase `*_watcher_down_seconds` or `*_watcher_restarts_total`. A window that fails on every retry fails the tick. The poller does not retry a rate limit or a permanent error. A steady rate shows an unreliable RPC provider. The `info` log line of each retry gives the cause. |
| `decdn_chain_boot_read_retries_total` | Counter | R | live | — | Retries of a boot-time chain read after a transient error. One read can retry many times. The reads are the registry, slash, `usdc()` self-check and blacklist bootstraps, and the best-effort fee-share reads. The metrics listener binds after these reads, so the value shows only after boot completes. A rise after a restart points at the RPC provider. |
| `decdn_blacklist_enforcement_failures_total` | Counter | M | live | — | Distinct hashes a batched re-scope could not re-verify or evict in one pass (`Recheck::Failed` — a disk error or a scope `eth_call` failure). An increase means that a pass did not fully enforce the deny-set. A blacklisted blob can then be servable and slashable (`SlashJudge.submitBlacklistChallenge`). This can occur while `decdn_blacklist_watcher_down_seconds` reads `0`: the two metrics answer different questions (deny-set enforced vs chain readable). The counter pairs with the aggregate `warn!` (`"blacklist re-scope could not enforce every entry"`). A boot that retries its enforcement pass also adds to it. The node serves only after a clean pass, so that increase does not mean a served blob. |
| `decdn_blacklist_reenumeration_failures_total` | Counter | M | live | — | Periodic re-enumerations of the `ContentBlacklist` deny-set that could not read chain state and kept the deny-set the node already had. The re-enumeration is the backstop for a lost tail event and for a region/ripening transition, which emits no event. It reports success upward so the event tail keeps running, so a failing backstop moves nothing else. Pairs with the `warn!` in `blacklist_watcher`'s `BlacklistSink::on_tick_complete`. |
| `decdn_slash_resync_failures_total` | Counter | M | live | — | Periodic re-enumerations of this operator's slashes that could not read chain state and kept the detected-slash set the node already had. The resync reports success upward so the event tail keeps running, so a failing repair moves nothing else. Pairs with the `warn!` in `slash_watcher`'s `SlashSink::on_tick_complete`. |

The origin directory is not a watcher — it is a lazy, on-demand TTL cache with no poll loop, so it carries none of the tick/panic/down metrics above. It exposes its own pair instead: `decdn_origin_directory_get_origins_failures_total` (Counter) counts `getOrigins` lookups that failed on a cold-namespace cache miss, and `decdn_origin_directory_cache_size` (Gauge) reports the current namespace count held in the cache (positive and negative entries, bounded by the configured capacity).

**Recommended alerts:**

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| `((decdn_<watcher>_last_tick_timestamp_seconds > 0) and (time() - decdn_<watcher>_last_tick_timestamp_seconds > 3 × poll_interval)) or (decdn_<watcher>_down_seconds > 3 × poll_interval)` | ✓ | sustained | The watcher does not complete a tick. It panicked, wedged, or exited, or every poll tick fails. For `blacklist` this means the node may be serving content blacklisted after the failure (slashable); for the others, the corresponding cache/projection is drifting. For failing ticks, check the RPC provider first. Then restart the daemon. **The `> 0` guard is required on the tick arm:** the gauge reads `0` until the first successful tick, so an unguarded `time() - gauge` fires forever on a fresh boot. **The down-seconds arm is required too:** the guard hides a watcher that never ticks. Down-seconds counts from the first failed poll tick, so it catches a watcher whose every `eth_getLogs` fails from boot. An unregistered watcher never fails a tick, so its down-seconds stays `0`. |
| `max without (watcher)` of `increase(decdn_<watcher>_restarts_total[30m])` | ≥ 4, sustained 45 m | — | The watcher fails and recovers again and again. Each success clears the down-seconds and the tick age, so the stalled rule above does not fire. The watcher lags head and gets events late. A single burst lifts the 30 m increase for 30 m only, so the 45 m hold ignores it. `increase()` handles a daemon restart. Check the RPC provider. A rising `decdn_chain_get_logs_range_rejections_total` shows an `eth_getLogs` cap. A rising `decdn_chain_get_logs_retries_total` shows an unreliable provider that the in-tick retries do not fully absorb. |
| `decdn_<watcher>_task_panicked_total` (rate) | > 0 | > 0 | A watcher task panicked. Never expected — capture the `error!` log line and file a bug. |
| `decdn_blacklist_enforcement_failures_total` (rate) | > 0 | > 0 sustained | The blacklist deny-set is not fully enforced — a blacklisted blob may be servable and slashable. Check disk health and the `ContentBlacklist` RPC; correlate with the `unenforced` `warn!`. |
| `increase(decdn_blacklist_reenumeration_failures_total[1h])` | > 0 | > 0 sustained | The deny-set backstop is failing, so a lost takedown event or an unannounced region/ripening transition stays unenforced. Check the RPC provider's `eth_call` path. The window has to exceed the attempt spacing (the `blockchain.content_blacklist_poll_interval_sec` cadence). |
| `increase(decdn_slash_resync_failures_total[1h])` | > 0 | > 0 sustained | The detected-slash repair is failing, so a slash whose tail event was lost stays off the admin surface and eats into its appeal window. Check the RPC provider's `eth_call` path. The window has to exceed the 15-minute attempt spacing. |
| `decdn_origin_directory_get_origins_failures_total` (rate) | > 0 | sustained | `getOrigins` reads are failing on cache misses — requests are silently losing their origin fallback. Check the RPC provider. |

> **Alert on the gauge for outages; use the restart counter only for flapping.** The `*_watcher_restarts_total` counters are edge-triggered through a `Mutex<Option<Instant>>` whose update is skipped on a poisoned lock (anti-panic policy) — once poisoned, the counter freezes silently, so `rate(*_watcher_restarts_total[5m])` can go permanently quiet with no signal that it did. Alert on `*_down_seconds` (a poisoned lock there reports `i64::MAX`, the safe direction) for sustained chain-read outages, and on `time() - *_last_tick_timestamp_seconds` for liveness. Use the restart counter only for the flapping rule, where it is the one signal that counts on-and-off failure. A frozen counter makes that rule miss flapping. The poisoned lock also makes `*_down_seconds` report `i64::MAX`, so the stalled rule fires.

### Health

Health is an **admin JSON-RPC method**, `admin_v1_health`, served on the admin
listener (`admin.listen`) — not an HTTP route beside `/metrics`. `decdn node
health` and `decdn node drain --wait` are its callers. It returns:

```json
{
  "node_id": "<hex iroh NodeId>",
  "uptime_s": 3601,
  "in_flight_streams": 3,
  "binding": "bound",
  "bound_node_id": "<hex iroh NodeId>",
  "registry_active": true
}
```

The body is `decdn_common::admin::HealthResponse`; that struct is the shape, and
this block tracks it. The endpoint is a readiness and identity probe, not a
second metrics surface — every quantity an operator graphs comes from
`/metrics`, so nothing is mirrored here.

| JSON key | Meaning |
|----------|---------|
| `node_id` | This node's iroh public key, lowercase hex |
| `uptime_s` | Whole seconds since process start, from a monotonic `Instant` |
| `in_flight_streams` | QUIC handler tasks holding a dispatch permit; `decdn node drain --wait` polls it |
| `binding` | Whether `node_id` is the key bound to this operator on-chain: `bound`, `mismatch`, `unbound` or `unknown`. Sampled once at bring-up |
| `bound_node_id` | The key the operator IS bound to, when it could be read. Names the key to restore on a `mismatch` |
| `registry_active` | Whether this node is in the on-chain active-staker set right now. Read live on every poll. The answer to "my node is up, why is it earning nothing" |

There is no aggregate `status` verdict and no HTTP status code to alert on. A
successful call means the daemon is up and answering; what it is up *for* is
`registry_active` and `binding`, which an operator reads directly. Liveness and
degradation are `/metrics` questions — see the watcher `*_down_seconds` and
`*_last_tick_timestamp_seconds` gauges above.

### Structured Logging

Metrics cover aggregates; structured logs cover per-event detail. Logs complement metrics; they do not replace them.

- **Library:** `tracing` crate (standard in the iroh ecosystem).
- **Format:** JSON (`tracing-subscriber` `json` formatter) for production machine consumption. Human-readable (`pretty`) available via config flag for local development.
- **Log levels:**
  - `ERROR` — unrecoverable, needs operator intervention (startup failures, RPC unreachable after all retries).
  - `WARN` — recoverable degraded conditions (blacklist poll lag > 1 interval, startup clock skew > 10 s, probe hold slot saturation > 90%).
  - `INFO` — significant lifecycle events (node ready, pool opened/redeemed, registry active-set change, config reloaded).
  - `DEBUG` — per-stream and per-probe events. Not for high-volume production.

**Mandatory log fields.** Every JSON event is one object with these top-level keys:

- `node_id` — this node's iroh NodeId, as lowercase hex. It is on every event, from the first event of the process.
- `timestamp` — RFC 3339 timestamp.
- `level` — log level.
- `target` — Rust module path.
- `fields` — the event fields. `fields.message` is the log message, when the event has one.

An event inside a span also has `span` and `spans`. `span` is the current span. `spans` is the list of spans from the root. Use the `pretty` format in a terminal. It has no `node_id` key. The startup banner (`event = "startup_banner"`) has a `node_id` field in both formats.

**Field names.** One concept has one field name on every event:

- `error` — the error value. A pre-commit hook rejects `err` and the `%err` shorthand.
- `peer` — a remote iroh NodeId, as lowercase hex. The DHT `NodeId` and `iroh::PublicKey` print the same string.
- `hash` — a content hash, as lowercase hex.
- `tx` — a transaction hash.

**Peer-triggered warnings.** A remote peer can fire some `WARN` lines at any rate: a bad client binding, a request with no binding, a request on an unknown lane, a bad probe request. Each of these lines passes a per-cause throttle. The throttle admits one line per window and counts the lines it drops. The admitted line carries `peer` and `suppressed`.

### Trace Spans

The node exports spans over OTLP when `observability.otlp_endpoint` is set. The export filter is separate from the log filter, so a change to `log_level` does not change the traces. The filter admits `INFO` spans and events from the deCDN crates. From other crates it admits `WARN` and `ERROR` events only, so a dependency failure lands on the deCDN span it happened in. The filter admits no span from another crate at any level. iroh opens its periodic network-report spans at `WARN`. These spans are more numerous than all deCDN spans, and they are not deCDN work. The OTLP transport crates are always off. A span covers one stream, pull, lookup, or transaction. No span covers a single frame.

The node sets `service.name = "decdn"` and `service.version` on the OTLP resource. The OpenTelemetry SDK also adds the `telemetry.sdk.*` attributes and each pair in `OTEL_RESOURCE_ATTRIBUTES`. The node does not set host identity by default. The collector of the reference Grafana stack adds `service.instance.id`, `region`, and `deployment.environment`. It also sets `service.name` to `decdn-node`. The dashboards query that name. A deployment without this collector must add the three host attributes itself, in its collector or in `OTEL_RESOURCE_ATTRIBUTES`. Only a collector can change `service.name`, because the value of the node replaces the value from the environment. The iroh NodeId of the node is the `local_node_id` field on the spans that need it.

| Span | Covers | Fields |
|------|--------|--------|
| `serve_stream` | One inbound `cdn/client/v1` stream | `peer`, `local_node_id`, `hash`, `pool_id`, `byte_offset`, `byte_len`, `direction`, `outcome`, `reason`, `bytes`, `error` |
| `pull_through` | One buffered cache-fill tier for a serve miss | `tier`, `hash`, `outcome` |
| `serve_miss_pull` | The streaming pull thread for a serve miss | `tier`, `hash` |
| `origin_pull` | One walk of the origin chain | `hash`, `local_only`, `outcome`, `error` |
| `origin_range_pull` | One ranged pull from the origin chain | `hash`, `byte_offset`, `byte_len`, `outcome`, `error` |
| `node_pull` | One node-to-node pull, over every candidate | `hash`, `outcome` |
| `upstream_stream` | One paid pull from one candidate: the whole blob on a buffered miss, one run of the range on a streaming miss | `peer`, `local_node_id`, `hash`, `pool_id`, `direction`, `outcome` |
| `open_progressive_pull` | The dial and handshake of one pulled range | `peer`, `local_node_id`, `hash`, `pool_id`, `byte_offset`, `byte_len`, `error` |
| `dht_lookup` | One `FIND_VALUE` lookup | `hash`, `rounds`, `providers` |
| `redeem_cycle` | One submission pass of the redeemer | `chunks`, `strict_flush` |
| `onchain_tx` | One `redeemMany` from send to receipt | `op`, `depth`, `voucher_count`, `tx`, `outcome` |

`serve_stream` records one `outcome`:

- `completed`: the whole request is delivered and paid.
- `refused`: a signed refusal. `reason` names it, for example `cache_miss`.
- `stopped`: delivery stops mid-stream. `reason` names the stop, for example `pool_exhausted`. For `proof_budget_exhausted`, `error` holds the unsettled chunk.
- `reset`: a reset with no signed response. `reason` is `stream_cap_full`, `bad_binding`, or `request_unreadable`. For `request_unreadable`, `error` holds the read error.
- `failed`: the stream ends on an error. `error` holds the error text.
- `panicked` or `cancelled`: the task panics, or it stops before the stream ends.

A `refused` or `stopped` stream keeps its `reason` when the peer leaves before it reads the frame. Then `error` holds the write error.

A failed request read, a full stream cap, and a bad binding end before the node reads the request fields, so those spans have no `hash`.

`upstream_stream` records one `outcome`. On a buffered miss, the values are `filled`, `clean_miss`, `local_fault`, and `below_margin`. On a streaming miss, the values are `filled`, `reassigned`, `terminal`, and `cancelled`. `reassigned` means that the run stops and the node drops this candidate. The node then plans the missing bytes again over the other candidates. A limit applies to the number of dropped candidates.

A buffered miss nests as `serve_stream` → `pull_through` → `origin_pull` → `node_pull` → `upstream_stream` → `open_progressive_pull`. A streaming miss from a peer nests as `serve_stream` → `serve_miss_pull` → `upstream_stream` → `open_progressive_pull`. Before the pull starts, the header handshake opens an `open_progressive_pull` under `serve_stream` for each candidate that it asks. When the probe reports the blob size, the pull leg adopts this pull as the first leg of its first run. That leg has no `upstream_stream` parent.

**Correlation across nodes.** No trace context crosses the wire. A peer controls what it sends, so a remote trace parent would let it set this node's sampling decision. Instead, both ends of a transfer record the same fields in the same format. The requester's `open_progressive_pull` and the server's `serve_stream` share `hash`, `pool_id` and `byte_offset`, and each side's `local_node_id` is the other side's `peer`. One TraceQL query finds both spans.

**Latency.** Latency SLOs use the histograms in the [§ Metric Registry](#metric-registry): `decdn_probe_collection_latency_seconds` and the three `*_first_byte_*_seconds` histograms. Tempo gives the drill-down from span durations. Group by span name and `outcome`. Do not group by `hash` or `peer`, because each value makes a new series. The duration of `serve_stream` increases with blob size, so do not use it for an SLO. On a buffered fill, the miss histogram also increases with blob size.

### Canonical Metric Name Cross-Reference

Each metric series has one canonical `decdn_`-prefixed name; informal short names map to it below. **Instrumentation names only** — no wire protocol or on-chain surface.

| Informal name | Canonical name | Source ADR |
|---------------|----------------|------------|
| `probe_hold_violations` | `decdn_probe_hold_unavailable_total{reason="exhausted"}` | [ADR 005](005-protocol.md#adr-005-wire-protocol), architecture.md |
| `probe_holds_disabled` | `decdn_probe_hold_unavailable_total{reason="disabled"}` | [ADR 005](005-protocol.md#adr-005-wire-protocol) |
| `probe_stake_lane_reserved` | `decdn_probe_hold_unavailable_total{reason="stake_lane_reserved"}` | [ADR 003 § Admission and Priority](003-payments.md#admission-and-priority) |
| `probe_hold_slots_used` | `decdn_probe_hold_slots_used` | [ADR 005](005-protocol.md#adr-005-wire-protocol), architecture.md |
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

Four Grafana dashboards and starter Prometheus alerting rules ship in the top-level [`monitoring/`](../monitoring/) directory. They are an operator starting point, not a normative deliverable.

The panels draw on the whole exported surface, not only the [§ Metric Registry](#metric-registry) subset. The registry is a curated view of roughly 180 exported series, so a panel may name a series this appendix does not list. What it may never name is a series the exporter does not emit, and `crates/node/src/metrics.rs` enforces that: `monitoring_selectors_are_exported` sweeps every `.yml` and `.json` in `monitoring/` and fails on any `decdn_*` token absent from a live scrape, including the `decdn_iroh_*` transport sub-registry. Adding a file to the directory therefore gates it; no list needs updating.

| File | Purpose |
|------|---------|
| [`monitoring/prometheus-alerts.yml`](../monitoring/prometheus-alerts.yml) | Three rule groups: `decdn-slash-safety` (thresholds copied verbatim from [§ Slash-Safety Metrics (all Mandatory)](#slash-safety-metrics-all-mandatory)), `decdn-liveness`, `decdn-delivery`. Every rule carries a `component` label for routing, and a `runbook_url` annotation where [`docs/runbook.md`](../docs/runbook.md) has a matching section. |
| [`monitoring/grafana-dashboard.json`](../monitoring/grafana-dashboard.json) | Fleet overview (`uid: decdn-poc-overview`): status, delivery funnel, slash safety, and the logs and traces that explain them. |
| [`monitoring/dashboard-delivery.json`](../monitoring/dashboard-delivery.json) | Delivery and cache (`uid: decdn-delivery`): the serve leg, the paying pull leg, cache, origin and warming. Every serve-refusal and pull-failure reason gets its own series. |
| [`monitoring/dashboard-chain.json`](../monitoring/dashboard-chain.json) | Chain, payments and slash safety (`uid: decdn-chain`): watcher liveness for all five watchers, the capacity-bond and active-staker registries with a world map of active nodes by declared region, and both sides of the payment flow. |
| [`monitoring/dashboard-node.json`](../monitoring/dashboard-node.json) | Single-node drilldown (`uid: decdn-node`): host resources, process state, iroh transport, DHT and probe, plus that node's logs and traces. |

All four share a datasource variable per signal — `${DS_PROMETHEUS}`, `${DS_LOKI}`, `${DS_TEMPO}`. The `$env`, `$region` and `$instance` variables read the Prometheus labels `deployment_environment`, `region` and `instance`. The Loki selectors use `unit="decdn-node.service"` and `instance`. They do not use `job`, because the log streams keep the host job for the Linux Server integration. The Loki `instance` label must equal the Prometheus `instance` label. The dashboards cross-link through a dashboard link on the `decdn` tag.

#### Two query shapes worth knowing

`rate()` and `increase()` drop `__name__`. A family panel written as `sum by (__name__) (rate({__name__=~"decdn_x_.+_total"}[5m]))` does not evaluate at all — the per-reason series collapse to identical label sets and Prometheus refuses the vector. So:

- Rate panels name each series explicitly, one target per metric. This also puts every name under the gate; a `__name__` regex scans as the wildcard token `decdn_x_` and is skipped.
- Instant panels over a family call `label_replace` on the raw selector, before any operator strips the name. The watcher-liveness table and the `DecdnWatcherTaskPanicked` rule are both built this way.

#### Importing the dashboards

In Grafana, *Dashboards → New → Import*; upload or paste each JSON. The datasource variables carry a saved value but re-resolve on load, so an import into another Grafana falls back to the picker. The `instance` variable auto-populates from `decdn_node_uptime_seconds`.

#### Using the alerts

Add the file via Prometheus `rule_files:` and reload. Validate with `promtool check rules monitoring/prometheus-alerts.yml`. Tune `for:` durations and thresholds for your fleet size before paging.

On Grafana Cloud the hosted ruler does not accept writes through the stack's service-account token, so the rules are translated into Grafana-managed rules instead. The translation is not a literal one: each rule's PromQL already contains its own comparison, so the result set is empty when the rule should not fire but the surviving values are not usable as a threshold — `up == 0` fires at value 0. The condition therefore counts datapoints rather than testing them, with `noDataState: OK` carrying the not-firing case.

#### Scope

Covers M-tier slash-safety metrics, the full delivery and pull surface, and the chain and payment paths. Deliberately not exhaustive.

## Consequences

### Positive

- Single reference for dashboard configuration — no hunting across 8 ADRs for metric names.
- Mandatory M-tier slash-risk metrics enforced at startup, so operators cannot accidentally run without slash-risk visibility.
- Canonical `decdn_` prefix and `_total` suffix allow automated registry validation (e.g., a CI check that exported names match the registry).
- Alert thresholds provide actionable defaults for new operators.
- `admin_v1_health` answers readiness and identity without parsing Prometheus text, for operators and for `decdn node drain --wait`.

### Negative

- Existing ADRs reference informal names differing from the canonical ones here. The cross-reference table (Section 6) documents all renames. No ADR is retroactively edited, but implementations must use this appendix's canonical names.
- Mandatory metrics add startup complexity — all M-tier collectors must initialize before accepting connections. Small overhead for guaranteed observability.

## Deferred & Open

- **OpenMetrics migration.** Prometheus text format 0.0.4 is the current default; the OpenMetrics exposition format (used by `prometheus_client` crate's `MetricsEncoder`) adds exemplars and native histograms — evaluate once tooling support is broader.
