# Appendix: Blob Cache Eviction Policy

> **This is an appendix, not a core protocol ADR.** Blob cache eviction is a local implementation choice — two nodes running different eviction strategies (LRU, LFU, hybrid) still interoperate so long as they honour the probe-triggered hold in [ADR 005 § Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold). This appendix codifies the recommended LRU-based approach (refreshed on every successful `CacheEngine::get`), the operator-pinning override, the durable operator-evict orthogonality, the probe-hold composition, and the observability metrics. Alternative implementations are acceptable.

**Touches:** [ADR 005](005-protocol.md), [ADR 011](011-content-takedown.md), [ADR 022](022-content-discovery.md), [architecture.md](architecture.md), [appendix-observability.md](appendix-observability.md)

## Context

[architecture.md § Cache Behavior](architecture.md#cache-behavior) describes the local blob cache and the conditions under which entries must be exempt from eviction (the probe-triggered hold introduced by [ADR 005 § Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold)). It does **not** specify the eviction order itself — the canonical wording is *"LRU or frequency-weighted eviction (LFU)"*, which leaves the choice open. Issue [#220](https://github.com/decdn/decdn/issues/220) tracks the gap. This appendix resolves five questions:

1. **Eviction key** — should the cache evict by recency (LRU), frequency (LFU), size (largest-first), or a hybrid? The non-committal wording in `architecture.md` is the gap this appendix closes.
2. **Pinning interaction** — how does an operator-pinned hash ([#276](https://github.com/decdn/decdn/issues/276)) compose with eviction?
3. **Operator-evict interaction** — how does the durable DMCA-style evict ([#279](https://github.com/decdn/decdn/issues/279)) compose with cache-pressure eviction?
4. **Probe-hold interaction** — how does the [ADR 005](005-protocol.md#probe-triggered-eviction-hold) hold layer compose with cache-pressure eviction?
5. **Cache size enforcement** — what triggers eviction, and what is the unit?

The existing implementation in `crates/cache/src/engine.rs` already commits to LRU: `access_times: Mutex<HashMap<Hash, Instant>>` (`crates/cache/src/engine.rs:36`) is refreshed by `CacheEngine::touch` on every successful `get`, and `CacheEngine::eviction_candidates` (`crates/cache/src/engine.rs:839`) returns an LRU snapshot with pinned hashes filtered out. The cache size limit is operator-set via `cache_size_mb: Option<u64>` (default `DEFAULT_CACHE_SIZE_MB = 10_240` at `crates/common/src/config/mod.rs`). The driver loop that consumes `eviction_candidates()` is not yet implemented. The hold-queue / cache-size interaction is already specified in [ADR 005 § Hold Budget](005-protocol.md#hold-budget) (`max_probe_holds = 256`, recommended ≤ 25 % of cache capacity) — this appendix cross-references it.

## Decision

The blob cache uses **least-recently-used (LRU) eviction** keyed on the `Instant` of the last successful `CacheEngine::get` (refreshed on both the cache-hit path and the post-pull-through path). Pinned hashes are exempt from LRU; operator-evicted hashes are durably hidden orthogonally to LRU; probe-hold-marked hashes defer to ADR 005. Reputation does not factor into eviction.

### 1. Eviction key

Each in-cache hash carries a `last_accessed: Instant` updated on every successful `CacheEngine::get` — both the cache-hit path and the post-pull-through path (after a miss is satisfied) call `touch`. `has`, `probe`, and `is_pinned` lookups do NOT refresh. When the local cache footprint exceeds `cache_size_mb`, the eviction driver picks the smallest-`last_accessed` candidate from `CacheEngine::eviction_candidates()` and removes it via the cache engine's removal path. This matches the existing implementation in `crates/cache/src/engine.rs`.

| Parameter | Value | Source |
|---|---|---|
| Cache size limit | 10 GB default, operator-configurable | `DEFAULT_CACHE_SIZE_MB` in `crates/common/src/config/mod.rs` |
| Configuration key | `cache.cache_size_mb` | resolved in `resolve_cache` (`crates/common/src/config/mod.rs`); CLI override `--cache-size-mb` |
| Eviction key | `last_accessed` `Instant` (refreshed on every successful `get`) | `CacheEngine::touch` and `CacheEngine::eviction_candidates` in `crates/cache/src/engine.rs` |

Once the eviction-driver loop is wired, it MUST honour the candidate-snapshot semantics (pinned-excluded, LRU-ordered).

**Why `last_accessed` and not insertion time.** Insertion-time eviction (FIFO) discards hot blobs the moment they age past their freshness threshold, which is exactly the wrong behaviour for a CDN cache. Refreshing on every hit makes "recently useful" the survival signal — the standard LRU semantics that any operator already understands.

### 2. Operator pinning overrides LRU (#276)

Hashes in the operator-pinned set (`pinned: ArcSwap<HashSet<Hash>>` at `crates/cache/src/engine.rs`) are filtered out of `CacheEngine::eviction_candidates()` and therefore never appear as LRU victims. The pin set is reloaded atomically on SIGHUP. Pinning does NOT refresh `last_accessed`; if a pin is later removed, the hash re-enters the LRU pool with whatever timestamp it last saw on a `get`, which is the right behaviour — recently-served pins survive briefly, long-stale pins go to the front of the eviction queue. The interaction is **one-way**: pinning protects against LRU but does NOT protect against operator `evict()` (#279) — see §3.

### 3. Operator-evict is orthogonal to LRU (#279)

`CacheEngine::evict(hash)` (`crates/cache/src/engine.rs:568`) is the DMCA / corruption-recovery path. It writes the hash into the in-memory `evicted: Mutex<HashSet<Hash>>` and appends to `<cache_dir>/evicted.log` with `fsync`, so the eviction survives a process restart. `CacheEngine::has` and `CacheEngine::get` short-circuit to "not present" for any evicted hash, regardless of whether the bytes still live in the underlying iroh-blobs store (iroh-blobs 0.99 does not yet expose a public delete; reclaim of disk bytes will happen on the next GC sweep when iroh-blobs ships one).

The two layers compose cleanly: LRU eviction is *ephemeral cache pressure* (a victim selected by the driver loop); operator eviction is a *durable operator directive* (a hash hidden permanently). LRU eviction does not append to `evicted.log`; operator eviction does not consult `last_accessed`. Pinning protects against LRU but loses to operator evict — DMCA always wins.

### 4. Probe-hold integration defers to ADR 005

Hashes for which the node has signed `has_blob: true` within the last `probe_hold_duration` (35 s) are eviction-exempt for that window, per [ADR 005 § Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold). The hold layer composes above LRU: a held hash is invisible to the LRU driver until the hold expires. The total number of concurrent holds is bounded by `max_probe_holds` (default 256) per [ADR 005 § Hold Budget](005-protocol.md#hold-budget); when the budget is exhausted the node responds `has_blob: false` rather than evict-and-slash. Operators sizing small caches SHOULD keep `max_probe_holds ≤ 25 %` of cache capacity (the §Hold Budget recommendation).

This appendix adds nothing to the hold mechanism itself. It is a separate layer with its own ADR and metrics. The DHT-record retraction behaviour ([ADR 022 § 1.4 Content Records and TTL](022-content-discovery.md#14-content-records-and-ttl)) is similarly downstream: a node stops re-publishing on eviction; stale records self-expire within TTL with no explicit retraction.

### 5. Reputation does not factor into eviction

Reputation governs *selection* (the unified score in [ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm) and [ADR 008](008-reputation.md)), not local-cache retention. A blob's reputation-derived "value" is irrelevant to the cache; only access recency is. This mirrors [appendix-peer-table-eviction.md §4](appendix-peer-table-eviction.md#4-reputation-does-not-factor-into-eviction) for the same reasons: coupling reputation to eviction would create a collusive-reporting vector and conflate two concerns whose design lives in separate ADRs.

### 6. Observability

Naming follows [appendix-observability.md § 2.3 Cache Metrics](appendix-observability.md#23-cache-metrics). The existing `decdn_cache_evictions_total` counter is retained but its description is tightened to "LRU pressure only" (was "LRU/LFU pressure"); operator-evict and pinning counts are surfaced separately:

| Metric | Type | Description |
|---|---|---|
| `decdn_cache_bytes` | gauge | **Existing**, see appendix; current on-disk cache footprint. Description tightened in §6 to pair with `decdn_cache_size_limit_bytes` for a saturation ratio. |
| `decdn_cache_evictions_total` | counter, unlabeled | **Existing**, see appendix; entries removed by LRU pressure (driver loop). Description tightened from "LRU/LFU" to "LRU only" in lockstep with this appendix. |
| `decdn_cache_size_limit_bytes` | gauge | New: configured `cache.cache_size_mb × 1 048 576`. Paired with `decdn_cache_bytes` for a saturation ratio. |
| `decdn_cache_evicted_operator_total` | counter, unlabeled | New: hashes removed via `decdn node evict` (#279). Distinct from `decdn_cache_evictions_total`. |
| `decdn_cache_pinned_count` | gauge | New: size of the operator-pinned set (#276). |

`decdn_probe_hold_*` metrics ([appendix § 2.1](appendix-observability.md#21-slash-safety-metrics-all-mandatory)) are owned by ADR 005 and are not redefined here. A sustained non-zero `decdn_probe_hold_violations_total` rate, paired with `decdn_cache_bytes ≈ decdn_cache_size_limit_bytes`, indicates the eviction driver is racing the hold layer — the operator response is to raise `cache.cache_size_mb` or lower `max_probe_holds`, not to disable the hold.

## Consequences

**Positive.**

- Codifies what the implementation already does. No code change is required to ship the policy contract; the new `decdn_cache_size_limit_bytes`, `decdn_cache_evicted_operator_total`, and `decdn_cache_pinned_count` metrics in §6 land alongside the eviction-driver loop when it is wired.
- Three layers (pinning, operator-evict, probe-hold) compose without entanglement. Each has a single owner (#276 / #279 / ADR 005) and a single rule.
- DMCA compliance is preserved exactly: operator-evict beats pinning, beats LRU, and is durable across restart. There is no policy gap that lets a pinned-and-evicted hash resurface.
- LRU's bookkeeping is one timestamp per cached hash. At PoC scale (10 GB / typical blob ~ 10 MB → ~1 000 entries), the `HashMap<Hash, Instant>` overhead is < 100 KB.

**Negative.**

- LRU does not reflect blob *value* — a 10 GB cold blob and a 1 MB cold blob age out at the same rate. A popularity-weighted policy (LFU or hybrid) would serve hit rate marginally better at the cost of bookkeeping and a counter-griefing surface; rejected in *Alternatives*.
- Until the eviction-driver loop is implemented, `cache.cache_size_mb` is an aspirational ceiling and the cache grows monotonically. The driver loop MUST land before the network is exposed to adversarial fill.
- Coupling `last_accessed` to `get`-only refresh means a blob that is pulled by a peer (cache-miss pull, paid) but never read locally ages by the same rule as a stale local hit. This is correct: the local node's cache is sized for the local workload, not for through-traffic, and through-traffic blobs are re-pullable from peers via DHT.

## Alternatives Considered

- **LFU.** Rejected. Per-hash hit-counter bookkeeping grows without decay heuristics; counters are gameable by an attacker who repeatedly probes a low-value blob to keep it resident, wasting cache capacity on adversarial-popular content. LRU's "recently useful" proxy is robust enough for PoC scale and resists the same attack (the attacker has to keep accessing the blob, paying per access — the cost defends the policy).
- **Size-weighted (largest-first).** Rejected. Penalizes the legitimate large-blob use case (video, datasets) the network is designed for. A 1 GB blob would always evict before a 1 MB blob even when both are equally hot, defeating the purpose of running a CDN cache for large content.
- **Hybrid LRU + LFU (e.g. SLRU, ARC, W-TinyLFU).** Rejected for PoC. The bookkeeping overhead and parameter-tuning burden ("how do we set the segment ratio?") buy a marginal hit-rate gain at scales orders of magnitude larger than the PoC. Revisit at production hardening if cache-hit telemetry shows a clear miss-rate floor that LRU is responsible for.
- **No eviction (rely on `cache_size_mb` as a soft hint).** Rejected. The cache is bounded storage; unbounded growth either wedges the disk or relies on the operator manually evicting via `decdn node evict`, neither acceptable. The driver loop is deferred (see *Negative consequences*) but the policy is mandatory.
- **Reputation-priority eviction.** Rejected, mirrors [appendix-peer-table-eviction.md §4](appendix-peer-table-eviction.md#4-reputation-does-not-factor-into-eviction). Conflates retention with selection; creates a collusive-reporting vector against ADR 008's hard floor; the cache layer should not consult reputation at all.
- **Refresh `last_accessed` on every probe / `has` check.** Rejected. A coordinated probe flood from many peers would refresh every cached hash to "recent" and turn the LRU policy into approximate FIFO. Refresh on `get` only — the paid-delivery path — ties recency to the operator's revenue signal, which is the right alignment.
