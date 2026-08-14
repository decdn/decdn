# Credit slow-start — ramped credit window (#1669)

## Problem

The delivery credit window (unbilled egress a node fronts on a stream before the next
voucher clears) must be at least the bandwidth-delay product (`throughput × RTT`) for a
stream to run link-bound. But that same standing window is the free-ride an attacker draws
once per stream and abandons. A single fixed window cannot be both: small enough to deny a
cheap free-ride and large enough to fill a fast, high-latency link.

Today the node ships a flat window: `credit_window_bytes`, default 8 MiB
(`DEFAULT_CREDIT_WINDOW_BYTES`). Every stream receives the whole window on first contact,
before it has paid for anything.

The upstream cache-miss leg carries the same tension under a different name — the ADR 037
seed-leech caps (`pull_ahead_bytes`, `pull_share_ratio_percent`,
`max_unrecouped_leech_bytes`, driven by `LeechGovernor`). On the fused serve-miss path each
pulled chunk is forwarded to the client immediately, so the upstream pull and the downstream
serve are one stream governed by one window.

## Solution

Make the credit window a pure function of the stream's own cumulative bytes paid. A stream
starts at a small floor and grows its window in proportion to what it has already paid, up to
a ceiling. A non-paying stream stays pinned at the floor.

```
window(paid) =
    if credit_ramp_speed == 0 { max(floor, credit_max) }         // instant
    else { (paid / credit_ramp_speed).clamp(floor, max(floor, credit_max)) }
```

- `floor` = one voucher interval. This keeps the ADR 003 invariant that a stream can always
  deliver a full interval and then recoup it, so the first voucher can clear and drive the
  ramp. Without a floor of at least one interval the ramp's own feedback loop never turns
  over.
- `credit_max` = window ceiling, node config, default `64 * 1024 * 1024` (64 MiB).
- `credit_ramp_speed` = the paid-bytes-per-window divisor, node config, default `2`.
  Lower is faster; `0` is instant full `credit_max`.

At `credit_ramp_speed = 2` the window is always at most half of what the stream has already
paid. A lane that has paid 128 MiB gets the full 64 MiB window; a lane that has paid nothing
sits at the floor.

### Why linear in paid, not exponential in vouchers

The risk the window represents (unbilled egress the node may never collect) is linear in
bytes. Payments are linear in bytes. Tying the window linearly to cumulative paid bytes makes
the node's credit exposure a fixed fraction of confirmed revenue:

```
exposure = delivered − paid ≤ window ≤ paid / credit_ramp_speed
```

So the node has always collected at least `credit_ramp_speed ×` its current exposure. With
`credit_ramp_speed = 2` an attacker must pay for two bytes to extract one byte of free-ride;
the node nets positive on every stream, including a pure pay-a-little-then-abandon attack.
The window is a pure function of this stream's `paid` counter, which resets on every new
stream — no stored client history, no graduation table.

### Node-wide exposure bound

Each stream's exposure is at most `paid / credit_ramp_speed`, and `paid` can never exceed the
requesting pool's deposit (a voucher cannot claim more than the pool holds). So the node's
aggregate speculative exposure is bounded by `Σ pool deposits / credit_ramp_speed` — a
real-capital bound, tighter than a byte counter and un-gameable by a flood of unfunded pools.
This bound plus the pre-flight deposit guard replace the ADR 037 `max_unrecouped_leech_bytes`
node-wide circuit breaker and the per-peer `share_ratio` outright.

## Config surface

`[payment]` section (`crates/common/src/config/types.rs`, `mod.rs`, `resolved.rs`):

Remove:

- `credit_window_bytes` (file field, resolution, `ResolvedPayment.credit_window_bytes`)
- `DEFAULT_CREDIT_WINDOW_BYTES`

Add:

- `credit_max: Option<Bytes>` → `ResolvedPayment.credit_max: u64`, default
  `DEFAULT_CREDIT_MAX = 64 * 1024 * 1024`.
- `credit_ramp_speed: Option<u64>` → `ResolvedPayment.credit_ramp_speed: u64`, default
  `DEFAULT_CREDIT_RAMP_SPEED = 2`. `0` means instant.

`[cache]` section — delete outright:

- `pull_ahead_bytes` + `DEFAULT_PULL_AHEAD_BYTES`
- `max_unrecouped_leech_bytes` + `DEFAULT_MAX_UNRECOUPED_LEECH_BYTES`
- `pull_share_ratio_percent` + `DEFAULT_PULL_SHARE_RATIO_PERCENT`
- the `pull_ahead_bytes ≤ max_unrecouped_leech_bytes` cross-field validation
- the three fields on `ResolvedCache`

## Node changes

### Ramp accessor (`crates/node/src/handlers/client/mod.rs`)

Replace the flat `credit_window(interval_bytes) -> u64` accessor (currently
`credit_window_bytes.max(interval_bytes)`) with a ramp-aware pair:

- `credit_floor(interval_bytes) -> u64` = `interval_bytes` (one voucher interval).
- `credit_window(interval_bytes, paid) -> u64` = the `window(paid)` formula above, using the
  handler's `credit_max` and `credit_ramp_speed` fields (from deps → runtime config).

Pre-flight callers that have no `paid` yet pass `paid = 0`, which yields the floor.

### Serve loop (`crates/node/src/handlers/client/delivery.rs`)

Today `window` is computed once (`let window = self.credit_window(interval_bytes)`) before the
loop. Change it to recompute from the live `paid` counter each loop iteration, so the pre-send
pause `delivered − paid < window` uses the current ramped window. The group-commit `batch_cap`
(intervals per window) is sized against `credit_max` (the ceiling) so the fsync-batching cap is
stable across the ramp.

### Reservation / deposit guards

Drop the `pull_ahead_bytes` term everywhere it appears and price the pre-flight deposit guard
against the floor (one interval), i.e. `credit_window(interval_bytes, 0)`:

- `crates/node/src/handlers/client/window.rs` — the pull-through peer leg (~104-137) and the
  own-origin leg (~492-509). The `.max(pull_ahead_bytes)` term is removed; the reservation is
  the floor.
- `crates/node/src/handlers/client/dispatch.rs` — the direct-serve floor gate (~824-844) and
  the pull-through solvency gate (~435-464). Both price the floor, still capped by the
  request's aligned span where the request bounds itself.

The floor-vs-ceiling residuals documented in ADR 037 §Implementation status shrink: with the
ramp, the pre-flight reservation is one interval, and in-stream exposure is bounded by the
ramped window, which never exceeds `paid / credit_ramp_speed`.

### Upstream pull-leg pacer (`crates/node/src/node_origin/pull_leg.rs`)

`LeechPacer::decide` currently gates the upstream draw on `pull_ahead_bytes` plus
`governor.poll_admission` / `record_pulled`. Rework it to pace the upstream frontier on the
**ramped downstream window**: on the fused serve-miss path the pull may run ahead of cleared
downstream payment only as far as the current `window(paid)` allows, matching the downstream
pause. Remove the `LeechGovernor` parameter and all `record_pulled` / `poll_admission` calls;
remove `record_served` on the voucher path.

### Delete `LeechGovernor`

- Delete `crates/node/src/leech_governor.rs` and its `pub mod leech_governor;` in
  `crates/node/src/lib.rs`.
- Remove construction and wiring in `crates/node/src/runtime/mod.rs` (~1134-1176, 1241) and
  the `leech_governor` field on `ClientHandlerDeps` / `ClientHandler`
  (`handlers/client/mod.rs` ~400, 593, 699) and `handlers/client/fill.rs` `leech_admit`
  (~250-256) and its `window.rs` caller (~142).
- Remove the leech-pause metrics (`node_pull_through_leech_budget_paused`,
  `node_pull_through_share_ratio_paused`) in `crates/node/src/metrics.rs` (~1091-1100, 2127,
  2131).

## Client change (`crates/cli/src/commands/fetch.rs`)

The shortfall-estimate diagnostic (~538-561) uses `DEFAULT_CREDIT_WINDOW_BYTES` as a
lower-bound estimate of what the node reserves before serving. The node now reserves only the
floor (one interval) pre-flight, so the estimate switches to one voucher interval. The
"raise your working deposit" diagnostic becomes correspondingly less aggressive.

## Config template / dump (`crates/cli/src/commands/config.rs`)

- Remove the `credit_window_bytes` template comment and dump lines (~685, 1004, 1011).
- Remove the three `[cache]` seed-leech knob template comments and dump lines (~272-275,
  677-679, 938-996).
- Add `credit_max` and `credit_ramp_speed` template comments and dump lines under `[payment]`.

## ADR changes

Both ADRs read as present-tense canon of current behavior — no changelog / history voice, no
issue or PR references, each line stands alone read cold.

### ADR 003 §Credit Window (`adr/003-payments.md` ~90-106, 246-248)

Rewrite from a fixed window to a ramped window:

- The window is `min(credit_max, max(floor, paid / credit_ramp_speed))`; a stream starts at
  one voucher interval and grows in proportion to its own confirmed payment, up to
  `credit_max`.
- State the exposure invariant: the node's unbilled egress on a stream is at most
  `paid / credit_ramp_speed`, so a stream never fronts more than a fixed fraction of the
  revenue it has already confirmed.
- Keep the floor-at-one-interval, node-local-policy, durability, and takedown-latency
  paragraphs, updated so the "window" they refer to is the ramped window (takedown latency is
  bounded by `credit_max`, floored at one interval).
- Update the §Concurrent Streams "per-lane" stop text so the window each lane pauses on is the
  ramped window.

### ADR 037 §Seed-leech caps (`adr/037-regional-proxy-warming.md` ~86-93, 105-133, 154-180)

- Delete the §Seed-leech caps section (global unrecouped-leech budget, per-peer share ratio).
- Delete the three rows from the parameters table.
- Rewrite §Implementation status: the fused serve-miss path is paced by the ramped credit
  window (ADR 003); the pre-flight deposit guard refuses a cache-miss pull whose pool cannot
  cover the floor, and node-wide speculative exposure is bounded by `Σ pool deposits /
  credit_ramp_speed`. Remove `LeechGovernor`, `pull_ahead_bytes`, `share_ratio`,
  `max_unrecouped_leech_bytes` references.
- Update the threat-model and acceptance-criteria items that named the caps to name the ramp
  and the deposit bound instead.

## Tests

- `crates/common/src/config/mod.rs` — replace `credit_window_bytes` resolution tests with
  `credit_max` / `credit_ramp_speed` defaults + explicit-override round-trips; drop the
  seed-leech-cap resolution and cross-field-validation tests; fix struct-literal fixtures.
- `crates/node/tests/client_loopback.rs` — replace the flat-window behavioral tests with ramp
  tests:
  - a non-paying stream is served exactly the floor (one interval) ahead of zero payment and
    then pauses (pinned at floor),
  - paying advances `paid` and grows the window to `paid / credit_ramp_speed`,
  - `credit_ramp_speed = 0` serves the full `credit_max` immediately,
  - the deposit gate covers the floor, not the ceiling.
- Delete `LeechGovernor` unit tests (`crates/node/src/leech_governor.rs`) and the pull-leg
  tests that assert seed-leech-cap pausing (`crates/node/tests/node_origin_pull.rs`,
  `crates/node/tests/origin_range_pull.rs`) — replace the latter with ramped-window pacing
  assertions.
- Fix `credit_window_bytes` / `DEFAULT_CREDIT_WINDOW_BYTES` and seed-leech-cap fixtures in
  `crates/cli/tests/config_validate.rs`, `crates/node/tests/sighup_signal.rs`,
  `crates/node/tests/anvil_bringup_shutdown_e2e.rs`, `crates/node/src/runtime/mod.rs`,
  `crates/node/src/runtime/reload.rs`, `crates/e2e/tests/cli_fetch_topup.rs`.

## Out of scope

- Voucher cadence deletion (`voucher_interval_mb`) is #1676, sequenced after this issue. This
  PR keeps the voucher interval as-is; the floor is one voucher interval. When #1676 lands it
  collapses the interval to chunk granularity and the floor becomes chunk-sized for free.
- PayWord hash-chain vouchers (#1670).
- Sub-MB / byte-granularity voucher confirmation.

## Verification

- `cargo build && cargo clippy` (anti-panic policy: no `unwrap`/`expect`/`panic`/indexing).
- `cargo nextest run` across `decdn-common`, `decdn-node`, `decdn-cli`.
- `cargo fmt -- --check`.
- Grep clean: no remaining references to `credit_window_bytes`, `DEFAULT_CREDIT_WINDOW_BYTES`,
  `leech_governor`, `LeechGovernor`, `pull_ahead_bytes`, `pull_share_ratio_percent`,
  `max_unrecouped_leech_bytes` outside `adr/_history/`.
