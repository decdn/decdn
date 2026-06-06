# DHT Prefetch — Popularity Signal + Decision Engine (core slice of #650)

**Date:** 2026-06-05
**Issue:** [#650](https://github.com/decdn/decdn/issues/650) — DHT: prefetch and popularity signals (ACs 7–8 of #320, ADR 022 §Popularity Signals)
**Status:** Implemented (core slice) in PR #819; the deferred live-acquisition work (AC 7) is tracked in #820.

## Context and source of truth

ADR 022 §Popularity Signals and Market Dynamics is the canonical spec. Note that the
issue text predates an ADR change and is **partly stale**:

- ADR 022 §255 now states speculative prefetch is driven by a **single** non-suppressible
  signal — **DHT FIND_VALUE query frequency**.
- The issue's "Signal 2" (local cache-miss frequency as a prefetch trigger) was
  **superseded by ADR 037** (chunk-paced reactive pull-through). Per-hash cache-miss
  frequency remains an observability metric only and does **not** drive acquisition. We
  implement Signal 1 only and will note this on the issue.

## Goal

Land the ADR-022-canonical popularity oracle and prefetch-decision logic as a
self-contained, fully-unit-tested unit, plus its config surface and metrics, behind
`prefetch.enabled = false`. The actual network acquisition and on-chain event
subscription are deferred to a tracked follow-up.

## Integration boundary — "decide and meter, don't yet act"

The DHT FIND_VALUE handler feeds the popularity tracker on every inbound request and, on
a threshold-cross, runs the decision engine and emits metrics/logs. It does **not** fire
the real `FindValue → probe → cdn/client/v1 pull-through` acquisition. With
`prefetch.enabled = false` (the default) the entire path is inert.

This satisfies **AC 8** (demand signals derive from FIND_VALUE traffic — the tracker is
live and metered). **AC 7** (a node initiates a prefetch subject to the gates) is
completed by the follow-up that replaces the "would-prefetch" log with the real
acquisition and feeds served/acquired bytes back into the ledgers.

## Architecture

New module `crates/node/src/prefetch/` (top-level under `node`, not buried in `dht/`,
because prefetch spans DHT + origin-directory + budget economics):

- `mod.rs` — module wiring, `PrefetchEngine` façade tying tracker + policy + metrics.
- `popularity.rs` — `PopularityTracker`.
- `decision.rs` — `PrefetchPolicy` (decision engine) + ledgers.
- `metrics.rs` — `PrefetchMetrics`.

The decision engine consumes the existing `crate::dht::origin::OriginDirectory` trait for
the authorized-origin gate (`lookup_origins(hash) -> Vec<NodeId>`, already filtered to
active stakers), so no new origin abstraction is introduced.

### Components

**`[prefetch]` config block.** Top-level TOML table; keys match ADR 022's recommended-
configuration table exactly:

| Key | Type | Default | Validation |
|---|---|---|---|
| `enabled` | bool | `false` | — |
| `require_authorized_origin` | bool | `true` | — |
| `budget_usdc_per_hour` | u64 (micro-USDC) | `0` | — (0 ⇒ no budget ⇒ never acquires; safe default) |
| `find_value_threshold` | u32 | `5` | `> 0` |
| `threshold_window_secs` | u64 | `300` | `> 0` |
| `demand_quality_min_ratio` | f64 | `0.1` | `0.0 ..= 1.0` |
| `demand_quality_window_secs` | u64 | `3600` | `> 0` |

`FileConfig.prefetch: Option<PrefetchConfig>` (all fields `Option`, `deny_unknown_fields`)
resolves to `ResolvedPrefetch` (all fields concrete). Surfacing follows the established
convention (a config knob touches ~5 sites): the file struct, the resolved struct +
resolver defaults, validation, the `config validate` summary output, and the
`DEFAULT_CONFIG` template.

**`PopularityTracker`** (`popularity.rs`). Responder-side sliding-window per-hash
FIND_VALUE counter.

- `observe(hash, now) -> bool` — records a timestamp for `hash`, prunes timestamps older
  than `threshold_window_secs`, returns `true` iff the in-window count is
  `>= find_value_threshold` (the threshold-cross). Idempotent w.r.t. monotonic `now`.
- Bounded memory: a cap on tracked hashes (e.g. 10,000) with LRU eviction of the
  least-recently-observed hash; per-hash timestamps held in a `VecDeque<u64>`.
- `now` is injected (`u64` seconds) so tests are deterministic. No wall-clock reads
  inside the type.
- `count(hash, now) -> u32` accessor for metrics/inspection.

**`PrefetchPolicy` (decision engine)** (`decision.rs`).

- `decide(&self, hash, dir: &dyn OriginDirectory, now) -> PrefetchDecision` where
  `PrefetchDecision ∈ { Acquire, Skip(SkipReason) }` and
  `SkipReason ∈ { Disabled, Throttled, Unauthorized, BudgetExhausted }`.
- Gates evaluated in order:
  1. `Disabled` if `!enabled`.
  2. `Throttled` if the demand-quality auto-throttle is active (see ledger below).
  3. `Unauthorized` if `require_authorized_origin` and `dir.lookup_origins(hash)` is empty.
     (When the gate is disabled this step is bypassed → counts as `bypassed` in metrics.)
  4. `BudgetExhausted` if rolling-1h spend `>= budget_usdc_per_hour`.
  5. otherwise `Acquire`.
- Ledgers (interior `Mutex`, all windows `VecDeque<(ts, value)>`, pruned by `now`):
  - **Budget ledger** — `spent_usdc(now)` sums acquisitions within the last
    `3600` s (the rolling-1h window is fixed by the ADR, independent of the demand-quality
    window). `record_acquisition(micro_usdc, now)` appends.
  - **Demand-quality ledger** — tracks `served_bytes` and `acquired_bytes` within
    `demand_quality_window_secs`. `ratio(now) = served / acquired` (defined only once
    `acquired > 0`; while `acquired == 0` the throttle is inactive). The throttle latches
    active when `ratio < demand_quality_min_ratio` and clears when it recovers.
    `record_served(bytes, now)` / `record_acquired(bytes, now)` append.
- Fully injected (clock, directory, ledgers) → every gate and ledger-rollover path is
  unit-testable without I/O.

In this PR the live handler path calls `decide()` and records the metric/log outcome, but
`record_acquisition` / `record_served` / `record_acquired` are exercised only by unit
tests (no live acquisition feeds them yet). The methods ship complete and tested so the
follow-up only has to call them from the real acquisition path.

**`PrefetchMetrics`** (`metrics.rs`). The seven metrics from
[appendix-observability §Prefetch Metrics], exposed regardless of `enabled`:

- `decdn_prefetch_enabled` (gauge), set once from config.
- `decdn_prefetch_acquisitions_total{gate_result=authorized|unauthorized|bypassed}` (counter).
- `decdn_prefetch_spend_usdc_total` (counter).
- `decdn_prefetch_budget_exhaustion_events_total` (counter).
- `decdn_prefetch_origin_gate_rejections_total` (counter; == `acquisitions_total{unauthorized}`).
- `decdn_prefetch_demand_quality_ratio` (gauge).
- `decdn_prefetch_throttle_active` (gauge).

Wired into the existing node metrics registry alongside the DHT metrics.

**Handler wiring** (`crates/node/src/handlers/dht.rs`). `handle_find_value` gains an
optional `Arc<PrefetchEngine>`. When present and `enabled`, it calls
`engine.on_find_value(hash, now)`, which `observe()`s and — on a threshold-cross — runs
`decide()` and updates metrics (`acquisitions_total` by gate result for an `Acquire` /
`Unauthorized`, `budget_exhaustion_events_total` on `BudgetExhausted`, etc.) plus a
`tracing` line. `observe()` is the per-request cost and must stay cheap; `decide()` only
runs on the rare threshold-cross. No network I/O is initiated.

## Error handling

- Mutex poisoning in the tracker/ledgers is handled the same way as the existing record
  store (`tracing::error!` + safe fallback: treat as "no trigger" / "do not acquire" —
  fail closed, never panic; the crate denies `unwrap`/`expect`/`panic`).
- Config validation errors surface through the existing `ConfigError` path with a clear
  message naming the offending key and bound.

## Testing

TDD, unit tests per component (no new integration test — nothing fires on the network):

- **Tracker:** below threshold → no trigger; crossing threshold within window → trigger;
  timestamps aging out of the window drop the count; LRU eviction at the hash cap;
  monotonic-`now` determinism.
- **Decision engine:** each gate independently (`Disabled`, `Throttled`, `Unauthorized`,
  `BudgetExhausted`, `Acquire`); gate ordering (e.g. disabled short-circuits before origin
  lookup); budget window rollover frees capacity; demand-quality latch activates below
  floor and clears on recovery; origin-gate-disabled ⇒ `bypassed`.
- **Config:** defaults match the ADR table; round-trip parse; each validation bound
  rejects out-of-range values; `config validate` summary renders the block.

## Out of scope (follow-up issue)

- Real `FindValue → probe → cdn/client/v1 pull-through` acquisition (completes AC 7).
- `OriginAssignment` event subscription (`AssignmentActivated` / `AssignmentRevoked` /
  `DefaultOpenAllowlistUpdated`) for a live, fail-closed gate cache. This PR's gate reads
  whatever `OriginDirectory` the runtime supplies (e.g. the existing
  `ConfigOriginDirectory` for testnet bring-up).
- Feeding `record_served` / `record_acquired` / `record_acquisition` from live
  acquisitions and serving.
