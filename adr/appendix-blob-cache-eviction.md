# Appendix: Blob Cache Eviction Policy

> **This is an appendix, not a core protocol ADR.** Blob cache eviction is a local implementation choice. Two nodes with different eviction strategies (LRU, LFU, hybrid) still interoperate. They must honour the probe-triggered hold in [ADR 005 § Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold). This appendix codifies the recommended LRU approach (refreshed on every successful `CacheEngine::get`), the operator-pinning override, the durable operator-evict orthogonality, the probe-hold composition, and the observability metrics. Alternative implementations are acceptable.

## Context

The local blob cache holds pulled content. Certain entries must be exempt from eviction: the probe-triggered hold from [ADR 005 § Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold). The protocol does **not** specify the eviction order. The canonical wording is *"LRU or frequency-weighted eviction (LFU)"*, which leaves the choice open. This appendix resolves five questions:

1. **Eviction key** — evict by recency (LRU), frequency (LFU), size (largest-first), or a hybrid? This appendix closes the non-committal wording in `architecture.md`.
2. **Pinning interaction** — how does an operator-pinned hash compose with eviction?
3. **Operator-evict interaction** — how does the durable DMCA-style evict compose with cache-pressure eviction?
4. **Probe-hold interaction** — how does the [ADR 005](005-protocol.md#probe-triggered-eviction-hold) hold layer compose with cache-pressure eviction?
5. **Cache size enforcement** — what triggers eviction, and what is the unit?

The implementation in `crates/cache/src/engine.rs` already commits to LRU. [§ Eviction key](#eviction-key) catalogues its symbols and config keys. [§ Eviction driver loop](#eviction-driver-loop) specifies the driver loop that consumes `CacheEngine::eviction_candidates()`; its implementation is pending. [ADR 005 § Hold Budget](005-protocol.md#hold-budget) specifies the hold-queue / cache-size interaction (`max_probe_holds = 256`, recommended ≤ 25 % of cache capacity); this appendix cross-references it.

## Decision

The blob cache uses **least-recently-used (LRU) eviction**. The key is the `Instant` of the last successful `CacheEngine::get`, refreshed on both the cache-hit path and the post-pull-through path. Pinned hashes are exempt from LRU. Operator-evicted hashes are durably hidden orthogonally to LRU. Probe-hold-marked hashes defer to [ADR 005](005-protocol.md#adr-005-wire-protocol). Reputation does not factor into eviction.

### Eviction key

Each in-cache hash carries a `last_accessed: Instant` (`access_times: Mutex<HashMap<Hash, Instant>>`, `crates/cache/src/engine.rs:36`). Every successful `CacheEngine::get` updates it: both the cache-hit path and the post-pull-through path call `touch`. `has`, `probe`, and `is_pinned` lookups do NOT refresh it. When the local cache footprint exceeds `cache_size_mb`, the eviction driver picks the smallest-`last_accessed` candidate from `CacheEngine::eviction_candidates()` (`crates/cache/src/engine.rs:839`, an LRU snapshot with pinned hashes filtered out). It removes the candidate via the cache engine's removal path. This matches the `crates/cache/src/engine.rs` implementation.

| Parameter | Value | Source |
|---|---|---|
| Cache size limit | 10 GB default, operator-configurable | `DEFAULT_CACHE_SIZE_MB` in `crates/common/src/config/mod.rs` |
| Configuration key | `cache.cache_size_mb` | resolved in `resolve_cache` (`crates/common/src/config/mod.rs`); CLI override `--cache-size-mb` |
| Eviction key | `last_accessed` `Instant` (refreshed on every successful `get`) | `CacheEngine::touch` and `CacheEngine::eviction_candidates` in `crates/cache/src/engine.rs` |

Once the eviction-driver loop is wired, it MUST honour the candidate-snapshot semantics (pinned-excluded, LRU-ordered).

**Why `last_accessed` and not insertion time.** Insertion-time eviction (FIFO) discards hot blobs when they age past their freshness threshold. This is wrong for a CDN cache. Refreshing on every hit makes "recently useful" the survival signal: standard LRU that any operator understands.

### Operator pinning overrides LRU

Hashes in the operator-pinned set (`pinned: ArcSwap<HashSet<Hash>>` at `crates/cache/src/engine.rs`) are filtered out of `CacheEngine::eviction_candidates()`. They never appear as LRU victims. The pin set reloads atomically on SIGHUP. Pinning does NOT refresh `last_accessed`. If a pin is later removed, the hash re-enters the LRU pool with the timestamp of its last `get`. This is the right behaviour: recently-served pins survive briefly, long-stale pins go to the front of the eviction queue. The interaction is **one-way**: pinning protects against LRU but NOT against operator `evict()` — see [§ Operator-evict is orthogonal to LRU](#operator-evict-is-orthogonal-to-lru).

### Operator-evict is orthogonal to LRU

`CacheEngine::evict(hash)` is the DMCA / corruption-recovery path. It writes the hash into the in-memory `evicted: Mutex<HashSet<Hash>>`. It appends the hash to `<cache_dir>/evicted.log` with `fsync`, so the eviction survives a process restart. `CacheEngine::has` and `CacheEngine::get` short-circuit to "not present" for any evicted hash, so serving stops immediately.

Disk reclaim is best-effort. It follows on the next GC sweep **when periodic GC is enabled (`cache.gc_interval_sec > 0`) and the tag deletion succeeds**. `Blobs::delete` is `pub(crate)` in iroh-blobs (GC-only), but **tag deletion is public**. So `evict` deletes the blob's protecting named tag(s). Without that, GC can never reclaim a tagged blob and the evicted bytes leak forever (#860). A tag-delete failure does not fail the takedown, because serving is already blocked. It leaves the bytes GC-protected, surfaced via `decdn_cache_tag_drop_failures_total`. Reclaim is therefore not unconditional for DMCA/compliance expectations.

Wrong-hash bytes from a failed pull-through are handled separately and are **not** logically evicted. Under content-addressing, those bytes are valid content for their own hash. So the node makes them GC-eligible (drop the temp tag on the streaming path / delete the named tag on the drain path). It does not blacklist them in `evicted.log`, which would durably censor a legitimate hash (#853, #837).

The two layers compose cleanly. LRU eviction is *ephemeral cache pressure*: a victim selected by the driver loop. Operator eviction is a *durable operator directive*: a hash hidden permanently. LRU eviction does not append to `evicted.log`. Operator eviction does not consult `last_accessed`. Pinning protects against LRU but loses to operator evict — DMCA always wins.

### Probe-hold integration defers to [ADR 005](005-protocol.md#adr-005-wire-protocol)

A node signs `has_blob: true` for a hash. That hash is eviction-exempt for the last `probe_hold_duration` (35 s), per [ADR 005 § Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold). The hold layer composes above LRU: a held hash is invisible to the LRU driver until the hold expires. `max_probe_holds` (default 256) bounds concurrent holds, per [ADR 005 § Hold Budget](005-protocol.md#hold-budget). When the budget is exhausted, the node still responds `has_blob: true` and forgoes only the hold — presence governs the answer, so a probe flood that fills the hold cache cannot suppress truthful availability. Such a blob stays visible to the LRU driver and may be evicted before the pull arrives. Operators sizing small caches SHOULD keep `max_probe_holds ≤ 25 %` of cache capacity (the § Hold Budget recommendation).

This appendix adds nothing to the hold mechanism itself — a separate layer with its own ADR and metrics. DHT-record retraction ([ADR 022 § Content Records and TTL](022-content-discovery.md#content-records-and-ttl)) is similarly downstream. On eviction a node stops re-publishing. Stale records self-expire within TTL with no explicit retraction.

### Reputation does not factor into eviction

Reputation governs *selection*, not local-cache retention. The unified score lives in [ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm) and [ADR 008](008-reputation.md#adr-008-reputation-system). A blob's reputation-derived "value" is irrelevant to the cache; only access recency matters. This mirrors [appendix-peer-table-eviction.md § Reputation does not factor into eviction](appendix-peer-table-eviction.md#reputation-does-not-factor-into-eviction) for the same reasons. Coupling reputation to eviction would conflate two concerns whose designs live in separate ADRs.

### Observability

Naming follows [appendix-observability.md § Cache Metrics](appendix-observability.md#cache-metrics). The `decdn_cache_evictions_total` counter is retained. Its description is tightened to "LRU pressure only" (was "LRU/LFU pressure"). Operator-evict and pinning counts are surfaced separately:

| Metric | Type | Description |
|---|---|---|
| `decdn_cache_bytes` | gauge | **Existing**, see appendix; current on-disk cache footprint. Pairs with `decdn_cache_size_limit_bytes` for a saturation ratio. |
| `decdn_cache_evictions_total` | counter, unlabeled | **Existing**, see appendix; entries removed by LRU pressure (driver loop). Description tightened per the intro above. |
| `decdn_cache_size_limit_bytes` | gauge | New: configured `cache.cache_size_mb × 1 048 576`. Paired with `decdn_cache_bytes` for a saturation ratio. |
| `decdn_cache_evicted_operator_total` | counter, unlabeled | New: hashes removed via `decdn node evict`. Distinct from `decdn_cache_evictions_total`. |
| `decdn_cache_pinned_count` | gauge | New: size of the operator-pinned set. |
| `decdn_cache_tag_drop_failures_total` | counter, unlabeled | New: best-effort named-tag deletions that failed, on the `evict()` (#860) and drain-path hash-mismatch (#837) paths. Serving is unaffected; a sustained nonzero rate means disk reclaim is stuck (an `evict()` failure is a DMCA/compliance concern, the drain path a hostile-origin disk leak). Not auto-retried. |

[ADR 005](005-protocol.md#adr-005-wire-protocol) owns the `decdn_probe_hold_*` metrics ([appendix § Slash-Safety Metrics (all Mandatory)](appendix-observability.md#slash-safety-metrics-all-mandatory)); this appendix does not redefine them. A sustained non-zero `decdn_probe_hold_unavailable_total{reason="exhausted"}` rate, paired with `decdn_cache_bytes ≈ decdn_cache_size_limit_bytes`, indicates the eviction driver is racing the hold layer. The operator response is to raise `cache.cache_size_mb` or lower `max_probe_holds`, not to disable the hold.

[§ Eviction driver loop](#eviction-driver-loop) below lists the driver-loop-specific counters alongside the driver mechanism they instrument.

### Eviction driver loop

The driver loop consumes `CacheEngine::eviction_candidates()` and removes hashes until the cache footprint is below target. It runs as a single async task owned by the `node` crate's wiring layer (per [appendix-poc-production-seams.md](appendix-poc-production-seams.md#appendix-pocproduction-seam-architecture-rust-implementation)), independent of the cache write path.

#### Trigger and target

| Parameter | Value | Hard bounds | Rationale |
|---|---:|---|---|
| `eviction_high_water_pct` | 90 | `[60, 95]` | Above this fraction of `cache.cache_size_mb` the driver actively evicts. Set above the 25% probe-hold recommendation so a full hold budget plus typical in-flight writes do not trip it; below 95% to leave write headroom between sweeps. |
| `eviction_target_pct` | 80 | `[40, 90]` | The driver evicts down to this fraction before returning to idle. The 10-point gap below `eviction_high_water_pct` is the hysteresis band preventing thrash on writes hovering near the trigger. Lower bound 40 prevents governance error starving the cache; upper bound 90 enforces a minimum 5-point gap below high-water. |

The driver MUST refuse to start (or reject a SIGHUP reload) if `eviction_target_pct > eviction_high_water_pct - 5` — the hysteresis gap is structural, not a tunable nicety.

#### Per-sweep budget

`eviction_per_sweep_budget = 16` (governable bounds `[1, 256]`). At each tick the driver removes at most this many candidates before yielding the cache lock. This bounds worst-case driver-induced latency on the cache hot path: at typical filesystem-unlink cost ~1 ms per entry, 16 evictions produce ~16 ms of locked work before yielding. The driver does NOT hold the `eviction_candidates()` snapshot lock across the sweep. It acquires per-hash removal locks, so concurrent reads on unrelated hashes are not blocked.

The driver continues across consecutive ticks until either (a) `decdn_cache_bytes ≤ eviction_target_pct × cache_size_mb_bytes`, or (b) `eviction_candidates()` returns empty (everything pinned, evicted-durably, or held — see [§ Operator pinning overrides LRU](#operator-pinning-overrides-lru), [§ Operator-evict is orthogonal to LRU](#operator-evict-is-orthogonal-to-lru), and [§ Probe-hold integration defers to ADR 005](#probe-hold-integration-defers-to-adr-005)). Case (b) emits `decdn_cache_evictions_starved_total`, and the driver returns to idle until the next tick. For sustained starvation, the operator response is to raise `cache.cache_size_mb`, lower `max_probe_holds` ([ADR 005 § Hold Budget](005-protocol.md#hold-budget)), or trim the pinned set — never to disable any of the three layers.

#### Tick cadence

`eviction_tick_secs = 1` (governable bounds `[1, 60]`). The driver wakes once per second, checks the high-water condition, and sweeps if needed. Below high-water the tick is near-zero-cost (one comparison plus one yield). The 1-second default is the floor the OS scheduler resolves cleanly; sub-second polling adds CPU cost without recovery benefit.

A future optimization MAY add an event-driven path: cache-write completions notify the driver on crossing the high-water threshold. This eliminates the up-to-1-second detection lag under bursty load. It is not required for the v1 driver — under sustained pressure the timer-based path converges to high-water-bound within one tick.

#### Backstop behaviour

The driver enforces the `cache.cache_size_mb` ceiling, not the cache write path. Writes remain agnostic: they write to disk via iroh-blobs and bump `decdn_cache_bytes`. If sustained pressure exceeds eviction throughput (adversarial fill, runaway pin set, undersized cache), disk-full errors from iroh-blobs propagate to callers as the hard backstop. Operators should treat sustained `decdn_cache_evictions_starved_total > 0` with `decdn_cache_bytes` approaching `disk_capacity` as an operational alarm. It is distinct from the in-bounds `decdn_cache_bytes ≈ decdn_cache_size_limit_bytes` operating regime.

#### Metrics

| Metric | Type | Description |
|---|---|---|
| `decdn_cache_evictions_sweeps_total` | counter, label `outcome={evicted, starved, idle}` | New: one increment per driver tick. `evicted` if ≥1 candidate was removed; `starved` if pressure persisted but `eviction_candidates()` returned empty; `idle` if the high-water condition was not met. |
| `decdn_cache_evictions_starved_total` | counter, unlabeled | New: convenience counter equivalent to `decdn_cache_evictions_sweeps_total{outcome="starved"}` for alerting (avoids label-filtering at scrape time). Emitted alongside the labeled metric. |
| `decdn_cache_evictions_bytes_total` | counter, unlabeled | New: cumulative bytes freed by the driver via LRU eviction. Pairs with `decdn_cache_evictions_total` (count-based) so dashboards show both "how many" and "how much" without computing byte/entry products from cache-size estimates. |

## Consequences

### Positive

- Codifies what the implementation already does. Shipping the policy contract requires no code change. The new [§ Observability](#observability) metrics (`decdn_cache_size_limit_bytes`, `decdn_cache_evicted_operator_total`, `decdn_cache_pinned_count`) land alongside the eviction-driver loop when it is wired.
- Three layers (pinning, operator-evict, probe-hold) compose without entanglement. Each has a single owner ([§ Operator pinning overrides LRU](#operator-pinning-overrides-lru) / [§ Operator-evict is orthogonal to LRU](#operator-evict-is-orthogonal-to-lru) / [ADR 005](005-protocol.md#adr-005-wire-protocol)) and a single rule.
- DMCA compliance is preserved exactly: operator-evict beats pinning, beats LRU, and is durable across restart. No policy gap lets a pinned-and-evicted hash resurface.
- LRU's bookkeeping is one timestamp per cached hash. At PoC scale (10 GB / typical blob ~ 10 MB → ~1 000 entries), the `HashMap<Hash, Instant>` overhead is < 100 KB.

### Negative

- LRU does not reflect blob *value* — a 10 GB cold blob and a 1 MB cold blob age out at the same rate. A popularity-weighted policy (LFU or hybrid) would serve hit rate marginally better, at the cost of bookkeeping and a counter-griefing surface; rejected in *Alternatives*.
- Until the [§ Eviction driver loop](#eviction-driver-loop) eviction-driver loop is implemented in `crates/cache`, `cache.cache_size_mb` is an aspirational ceiling and the cache grows monotonically. The driver MUST land before the network is exposed to adversarial fill.
- Coupling `last_accessed` to `get`-only refresh means a blob pulled by a peer (cache-miss pull, paid) but never read locally ages by the same rule as a stale local hit. This is correct: the local node's cache is sized for the local workload, not through-traffic. Through-traffic blobs are re-pullable from peers via DHT.
