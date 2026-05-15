# Appendix: Blob Cache Eviction Policy

> **This is an appendix, not a core protocol ADR.** Blob cache eviction is a local implementation choice — two nodes running different eviction strategies (LRU, LFU, hybrid) still interoperate so long as they honour the probe-triggered hold in [ADR 005 § Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold). This appendix codifies the recommended LRU approach (refreshed on every successful `CacheEngine::get`), the operator-pinning override, the durable operator-evict orthogonality, the probe-hold composition, and the observability metrics. Alternative implementations are acceptable.

**Touches:** [ADR 005](005-protocol.md), [ADR 011](011-content-takedown.md), [ADR 022](022-content-discovery.md), [architecture.md](architecture.md), [appendix-observability.md](appendix-observability.md)

## Context

The local blob cache holds pulled content; certain entries must be exempt from eviction — the probe-triggered hold introduced by [ADR 005 § Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold). The protocol does **not** specify the eviction order itself — the canonical wording is *"LRU or frequency-weighted eviction (LFU)"*, which leaves the choice open. Issue [#220](https://github.com/decdn/decdn/issues/220) tracks the gap. This appendix resolves five questions:

1. **Eviction key** — evict by recency (LRU), frequency (LFU), size (largest-first), or a hybrid? The non-committal wording in `architecture.md` is the gap this appendix closes.
2. **Pinning interaction** — how does an operator-pinned hash ([#276](https://github.com/decdn/decdn/issues/276)) compose with eviction?
3. **Operator-evict interaction** — how does the durable DMCA-style evict ([#279](https://github.com/decdn/decdn/issues/279)) compose with cache-pressure eviction?
4. **Probe-hold interaction** — how does the [ADR 005](005-protocol.md#probe-triggered-eviction-hold) hold layer compose with cache-pressure eviction?
5. **Cache size enforcement** — what triggers eviction, and what is the unit?

The existing implementation in `crates/cache/src/engine.rs` already commits to LRU (the concrete symbols and config keys are catalogued in §1); the driver loop that consumes `CacheEngine::eviction_candidates()` is specified in §7, its implementation pending. The hold-queue / cache-size interaction is already specified in [ADR 005 § Hold Budget](005-protocol.md#hold-budget) (`max_probe_holds = 256`, recommended ≤ 25 % of cache capacity) — this appendix cross-references it.

## Decision

The blob cache uses **least-recently-used (LRU) eviction** keyed on the `Instant` of the last successful `CacheEngine::get` (refreshed on both the cache-hit path and the post-pull-through path). Pinned hashes are exempt from LRU; operator-evicted hashes are durably hidden orthogonally to LRU; probe-hold-marked hashes defer to ADR 005. Reputation does not factor into eviction.

### 1. Eviction key

Each in-cache hash carries a `last_accessed: Instant` (`access_times: Mutex<HashMap<Hash, Instant>>`, `crates/cache/src/engine.rs:36`) updated on every successful `CacheEngine::get` — both the cache-hit path and the post-pull-through path call `touch`. `has`, `probe`, and `is_pinned` lookups do NOT refresh. When the local cache footprint exceeds `cache_size_mb`, the eviction driver picks the smallest-`last_accessed` candidate from `CacheEngine::eviction_candidates()` (`crates/cache/src/engine.rs:839`, an LRU snapshot with pinned hashes filtered out) and removes it via the cache engine's removal path. This matches the existing `crates/cache/src/engine.rs` implementation.

| Parameter | Value | Source |
|---|---|---|
| Cache size limit | 10 GB default, operator-configurable | `DEFAULT_CACHE_SIZE_MB` in `crates/common/src/config/mod.rs` |
| Configuration key | `cache.cache_size_mb` | resolved in `resolve_cache` (`crates/common/src/config/mod.rs`); CLI override `--cache-size-mb` |
| Eviction key | `last_accessed` `Instant` (refreshed on every successful `get`) | `CacheEngine::touch` and `CacheEngine::eviction_candidates` in `crates/cache/src/engine.rs` |

Once the eviction-driver loop is wired, it MUST honour the candidate-snapshot semantics (pinned-excluded, LRU-ordered).

**Why `last_accessed` and not insertion time.** Insertion-time eviction (FIFO) discards hot blobs the moment they age past their freshness threshold — exactly wrong for a CDN cache. Refreshing on every hit makes "recently useful" the survival signal: standard LRU any operator already understands.

### 2. Operator pinning overrides LRU (#276)

Hashes in the operator-pinned set (`pinned: ArcSwap<HashSet<Hash>>` at `crates/cache/src/engine.rs`) are filtered out of `CacheEngine::eviction_candidates()` and therefore never appear as LRU victims. The pin set is reloaded atomically on SIGHUP. Pinning does NOT refresh `last_accessed`; if a pin is later removed the hash re-enters the LRU pool with whatever timestamp it last saw on a `get` — the right behaviour: recently-served pins survive briefly, long-stale pins go to the front of the eviction queue. The interaction is **one-way**: pinning protects against LRU but NOT against operator `evict()` (#279) — see §3.

### 3. Operator-evict is orthogonal to LRU (#279)

`CacheEngine::evict(hash)` (`crates/cache/src/engine.rs:568`) is the DMCA / corruption-recovery path. It writes the hash into the in-memory `evicted: Mutex<HashSet<Hash>>` and appends to `<cache_dir>/evicted.log` with `fsync`, so the eviction survives a process restart. `CacheEngine::has` and `CacheEngine::get` short-circuit to "not present" for any evicted hash, regardless of whether the bytes still live in the iroh-blobs store (iroh-blobs 0.99 exposes no public delete; disk-byte reclaim happens on the next GC sweep once it ships one).

The two layers compose cleanly: LRU eviction is *ephemeral cache pressure* (a victim selected by the driver loop); operator eviction is a *durable operator directive* (a hash hidden permanently). LRU eviction does not append to `evicted.log`; operator eviction does not consult `last_accessed`. Pinning protects against LRU but loses to operator evict — DMCA always wins.

### 4. Probe-hold integration defers to ADR 005

Hashes for which the node has signed `has_blob: true` within the last `probe_hold_duration` (35 s) are eviction-exempt for that window, per [ADR 005 § Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold). The hold layer composes above LRU: a held hash is invisible to the LRU driver until the hold expires. Concurrent holds are bounded by `max_probe_holds` (default 256) per [ADR 005 § Hold Budget](005-protocol.md#hold-budget); when the budget is exhausted the node responds `has_blob: false` rather than evict-and-slash. Operators sizing small caches SHOULD keep `max_probe_holds ≤ 25 %` of cache capacity (the §Hold Budget recommendation).

This appendix adds nothing to the hold mechanism itself — a separate layer with its own ADR and metrics. DHT-record retraction ([ADR 022 § 1.4 Content Records and TTL](022-content-discovery.md#14-content-records-and-ttl)) is similarly downstream: a node stops re-publishing on eviction; stale records self-expire within TTL with no explicit retraction.

### 5. Reputation does not factor into eviction

Reputation governs *selection* (the unified score in [ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm) and [ADR 008](008-reputation.md)), not local-cache retention. A blob's reputation-derived "value" is irrelevant to the cache; only access recency is. This mirrors [appendix-peer-table-eviction.md §4](appendix-peer-table-eviction.md#4-reputation-does-not-factor-into-eviction) for the same reasons: coupling reputation to eviction would create a collusive-reporting vector and conflate two concerns whose designs live in separate ADRs.

### 6. Observability

Naming follows [appendix-observability.md § 2.3 Cache Metrics](appendix-observability.md#23-cache-metrics). The existing `decdn_cache_evictions_total` counter is retained but its description is tightened to "LRU pressure only" (was "LRU/LFU pressure"); operator-evict and pinning counts are surfaced separately:

| Metric | Type | Description |
|---|---|---|
| `decdn_cache_bytes` | gauge | **Existing**, see appendix; current on-disk cache footprint. Pairs with `decdn_cache_size_limit_bytes` for a saturation ratio. |
| `decdn_cache_evictions_total` | counter, unlabeled | **Existing**, see appendix; entries removed by LRU pressure (driver loop). Description tightened per the intro above. |
| `decdn_cache_size_limit_bytes` | gauge | New: configured `cache.cache_size_mb × 1 048 576`. Paired with `decdn_cache_bytes` for a saturation ratio. |
| `decdn_cache_evicted_operator_total` | counter, unlabeled | New: hashes removed via `decdn node evict` (#279). Distinct from `decdn_cache_evictions_total`. |
| `decdn_cache_pinned_count` | gauge | New: size of the operator-pinned set (#276). |

`decdn_probe_hold_*` metrics ([appendix § 2.1](appendix-observability.md#21-slash-safety-metrics-all-mandatory)) are owned by ADR 005 and not redefined here. A sustained non-zero `decdn_probe_hold_violations_total` rate, paired with `decdn_cache_bytes ≈ decdn_cache_size_limit_bytes`, indicates the eviction driver is racing the hold layer — the operator response is to raise `cache.cache_size_mb` or lower `max_probe_holds`, not to disable the hold.

Driver-loop-specific counters are listed in §7 below alongside the driver mechanism they instrument.

### 7. Eviction driver loop

The driver loop is the runtime that consumes `CacheEngine::eviction_candidates()` and removes hashes until the cache footprint is below target. It runs as a single async task owned by the `node` crate's wiring layer (per [appendix-poc-production-seams.md](appendix-poc-production-seams.md)), independent of the cache write path.

#### Trigger and target

| Parameter | Value | Hard bounds | Rationale |
|---|---:|---|---|
| `eviction_high_water_pct` | 90 | `[60, 95]` | Above this fraction of `cache.cache_size_mb` the driver actively evicts. Set above the 25% probe-hold recommendation so a full hold budget plus typical in-flight writes do not trip it; below 95% to leave write headroom between sweeps. |
| `eviction_target_pct` | 80 | `[40, 90]` | The driver evicts down to this fraction before returning to idle. The 10-point gap below `eviction_high_water_pct` is the hysteresis band preventing thrash on writes hovering near the trigger. Lower bound 40 prevents governance error starving the cache; upper bound 90 enforces a minimum 5-point gap below high-water. |

The driver MUST refuse to start (or reject a SIGHUP reload) if `eviction_target_pct > eviction_high_water_pct - 5` — the hysteresis gap is structural, not a tunable nicety.

#### Per-sweep budget

`eviction_per_sweep_budget = 16` (governable bounds `[1, 256]`). At each tick the driver removes at most this many candidates before yielding the cache lock, bounding worst-case driver-induced latency on the cache hot path: at typical filesystem-unlink cost ~1 ms per entry, 16 evictions produce ~16 ms of locked work before yielding. The driver does NOT hold the `eviction_candidates()` snapshot lock across the sweep — it acquires per-hash removal locks, so concurrent reads on unrelated hashes are not blocked.

The driver continues across consecutive ticks until either (a) `decdn_cache_bytes ≤ eviction_target_pct × cache_size_mb_bytes`, or (b) `eviction_candidates()` returns empty (everything pinned, evicted-durably, or held — see §§2–4). Case (b) emits `decdn_cache_evictions_starved_total` and the driver returns to idle until the next tick. The operator response to sustained starvation is to raise `cache.cache_size_mb`, lower `max_probe_holds` ([ADR 005 § Hold Budget](005-protocol.md#hold-budget)), or trim the pinned set ([#276](https://github.com/decdn/decdn/issues/276)) — never to disable any of the three layers.

#### Tick cadence

`eviction_tick_secs = 1` (governable bounds `[1, 60]`). The driver wakes once per second, checks the high-water condition, and sweeps if needed. Below high-water the tick is near-zero-cost (one comparison plus one yield), so the 1-second default is the floor the OS scheduler resolves cleanly; sub-second polling adds CPU cost without recovery benefit.

A future optimization MAY add an event-driven path where cache-write completions notify the driver on crossing the high-water threshold, eliminating the up-to-1-second detection lag under bursty load. Not required for the v1 driver — under sustained pressure the timer-based path converges to high-water-bound within one tick.

#### Backstop behaviour

The `cache.cache_size_mb` ceiling is enforced by the driver, not the cache write path. Writes remain agnostic: they write to disk via iroh-blobs and bump `decdn_cache_bytes`. If sustained pressure exceeds eviction throughput (adversarial fill, runaway pin set, undersized cache), disk-full errors from iroh-blobs propagate to callers as the hard backstop. Operators should treat sustained `decdn_cache_evictions_starved_total > 0` with `decdn_cache_bytes` approaching `disk_capacity` as an operational alarm distinct from the in-bounds `decdn_cache_bytes ≈ decdn_cache_size_limit_bytes` operating regime.

#### Metrics

| Metric | Type | Description |
|---|---|---|
| `decdn_cache_evictions_sweeps_total` | counter, label `outcome={evicted, starved, idle}` | New: one increment per driver tick. `evicted` if ≥1 candidate was removed; `starved` if pressure persisted but `eviction_candidates()` returned empty; `idle` if the high-water condition was not met. |
| `decdn_cache_evictions_starved_total` | counter, unlabeled | New: convenience counter equivalent to `decdn_cache_evictions_sweeps_total{outcome="starved"}` for alerting (avoids label-filtering at scrape time). Emitted alongside the labeled metric. |
| `decdn_cache_evictions_bytes_total` | counter, unlabeled | New: cumulative bytes freed by the driver via LRU eviction. Pairs with `decdn_cache_evictions_total` (count-based) so dashboards show both "how many" and "how much" without computing byte/entry products from cache-size estimates. |

## Consequences

**Positive.**

- Codifies what the implementation already does. No code change is required to ship the policy contract; the new §6 metrics (`decdn_cache_size_limit_bytes`, `decdn_cache_evicted_operator_total`, `decdn_cache_pinned_count`) land alongside the eviction-driver loop when it is wired.
- Three layers (pinning, operator-evict, probe-hold) compose without entanglement. Each has a single owner (#276 / #279 / ADR 005) and a single rule.
- DMCA compliance is preserved exactly: operator-evict beats pinning, beats LRU, and is durable across restart. No policy gap lets a pinned-and-evicted hash resurface.
- LRU's bookkeeping is one timestamp per cached hash. At PoC scale (10 GB / typical blob ~ 10 MB → ~1 000 entries), the `HashMap<Hash, Instant>` overhead is < 100 KB.

**Negative.**

- LRU does not reflect blob *value* — a 10 GB cold blob and a 1 MB cold blob age out at the same rate. A popularity-weighted policy (LFU or hybrid) would serve hit rate marginally better at the cost of bookkeeping and a counter-griefing surface; rejected in *Alternatives*.
- Until the §7 eviction-driver loop is implemented in `crates/cache`, `cache.cache_size_mb` is an aspirational ceiling and the cache grows monotonically. The driver MUST land before the network is exposed to adversarial fill.
- Coupling `last_accessed` to `get`-only refresh means a blob pulled by a peer (cache-miss pull, paid) but never read locally ages by the same rule as a stale local hit. This is correct: the local node's cache is sized for the local workload, not through-traffic, and through-traffic blobs are re-pullable from peers via DHT.

## Alternatives Considered

The rejected eviction-policy alternatives (LFU, size-weighted, hybrid LRU+LFU, no-eviction, reputation-priority, refresh-on-every-probe) are recorded in [`_history/alternatives-pre-launch.md` § Blob Cache Eviction Policy (appendix)](_history/alternatives-pre-launch.md#blob-cache-eviction-policy-appendix).
