# Credit slow-start — ramped credit window Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the flat 8 MiB credit window (and the ADR 037 seed-leech caps) with a per-stream credit window that ramps linearly with the stream's own cumulative bytes paid, floored at one voucher interval and capped at a node-config `credit_max`.

**Architecture:** One shared pure formula in `decdn-incentive` computes `window(paid)`. The node's downstream serve loop recomputes its window from the live `paid` counter each iteration; the upstream pull-leg pacer (`client-pull`) recomputes the same window from the served-paid frontier, so a fused cache-miss stream ramps pull and serve in lockstep. Pre-flight deposit guards price the floor (one interval). The `LeechGovernor` and the three `[cache]` seed-leech knobs are deleted; the pre-flight deposit guard plus pool solvency `M` are the retained bound.

**Tech Stack:** Rust (edition 2024, MSRV 1.95), `cargo nextest`, iroh-blobs, alloy, postcard wire.

## Global Constraints

- Anti-panic policy: clippy denies `unwrap_used`, `expect_used`, `panic`, `indexing_slicing` workspace-wide. Use `Result`/`Option` combinators, `.get()`, saturating arithmetic. Tests may `#[allow(...)]` these as the existing modules do.
- `rustfmt.toml` sets `max_width = 100`. Run `cargo fmt` before every commit.
- Pre-launch: no back-compat shims, no dual-format readers, no migration paths. Change every side in this PR and delete the old shape.
- Comments / docstrings / ADRs read as present-tense canon of current behavior — no changelog/history voice ("replaced", "used to", "retired"), no issue/PR numbers inside `adr/` prose, every line stands alone read cold.
- Defaults, verbatim: `DEFAULT_CREDIT_MAX = 64 * 1024 * 1024`; `DEFAULT_CREDIT_RAMP_DIVISOR = 2`; ramp formula `window(paid) = if divisor == 0 { max(floor, credit_max) } else { (paid / divisor).clamp(floor, max(floor, credit_max)) }`, `floor = interval_bytes`.
- Preferred test runner: `cargo nextest run`. Single crate: `cargo nextest run -p <crate>`.
- After any `pub fn` signature change reached from `decdn-e2e`, run the compile gate: `cargo test --no-run -p decdn-e2e --features anvil-e2e`.

---

### Task 1: Shared ramp formula in `decdn-incentive`

**Files:**

- Create: `crates/incentive/src/credit.rs`
- Modify: `crates/incentive/src/lib.rs` (add `mod credit; pub use credit::ramped_credit_window;`)
- Test: inline `#[cfg(test)]` in `crates/incentive/src/credit.rs`

**Interfaces:**

- Produces: `pub fn ramped_credit_window(divisor: u64, floor: u64, credit_max: u64, paid: u64) -> u64`

- [ ] **Step 1: Write the failing test**

Create `crates/incentive/src/credit.rs` with the tests first:

```rust
//! Ramped delivery credit window (ADR 003 §Credit window).

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::ramped_credit_window;

    const FLOOR: u64 = 4 * 1024 * 1024; // one 4 MiB interval
    const MAX: u64 = 64 * 1024 * 1024;

    #[test]
    fn unpaid_stream_sits_at_the_floor() {
        assert_eq!(ramped_credit_window(2, FLOOR, MAX, 0), FLOOR);
    }

    #[test]
    fn window_is_paid_over_divisor_once_it_clears_the_floor() {
        // paid 32 MiB, divisor 2 -> 16 MiB, above the 4 MiB floor.
        assert_eq!(ramped_credit_window(2, FLOOR, MAX, 32 * 1024 * 1024), 16 * 1024 * 1024);
    }

    #[test]
    fn window_is_pinned_to_floor_until_paid_exceeds_divisor_times_floor() {
        // paid 4 MiB, divisor 2 -> 2 MiB, below floor -> floor.
        assert_eq!(ramped_credit_window(2, FLOOR, MAX, 4 * 1024 * 1024), FLOOR);
    }

    #[test]
    fn window_caps_at_credit_max() {
        // paid 1 GiB, divisor 2 -> 512 MiB, capped to 64 MiB.
        assert_eq!(ramped_credit_window(2, FLOOR, MAX, 1024 * 1024 * 1024), MAX);
    }

    #[test]
    fn divisor_zero_is_instant_full_credit_max() {
        assert_eq!(ramped_credit_window(0, FLOOR, MAX, 0), MAX);
    }

    #[test]
    fn a_credit_max_below_the_floor_never_drops_below_the_floor() {
        // Misconfiguration: ceiling < floor. Progress floor wins.
        assert_eq!(ramped_credit_window(2, FLOOR, 1024, 1_000_000_000), FLOOR);
        assert_eq!(ramped_credit_window(0, FLOOR, 1024, 0), FLOOR);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo nextest run -p decdn-incentive credit::`
Expected: FAIL — `ramped_credit_window` not found (compile error).

- [ ] **Step 3: Write the implementation**

Prepend the function above the test module in `crates/incentive/src/credit.rs`:

```rust
/// The ramped delivery credit window (ADR 003 §Credit window): the unbilled
/// egress a node fronts on a stream grows in proportion to what the stream has
/// already paid, floored at one voucher interval (`floor`) so the loop can always
/// deliver a full interval and recoup it, and capped at `credit_max`. A `divisor`
/// of `0` opens the full `credit_max` from the first byte. The window is a pure
/// function of this stream's own `paid` bytes, so a non-paying stream stays pinned
/// at the floor and a paying stream ramps to the ceiling; the node's unbilled
/// exposure is therefore at most `paid / divisor`.
#[must_use]
pub fn ramped_credit_window(divisor: u64, floor: u64, credit_max: u64, paid: u64) -> u64 {
    let ceiling = credit_max.max(floor);
    if divisor == 0 {
        return ceiling;
    }
    (paid / divisor).clamp(floor, ceiling)
}
```

Add to `crates/incentive/src/lib.rs` (place `mod credit;` with the other module declarations and re-export beside the existing `pub use` of `min_payment`):

```rust
mod credit;
pub use credit::ramped_credit_window;
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo nextest run -p decdn-incentive credit::`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add crates/incentive/src/credit.rs crates/incentive/src/lib.rs
git commit -m "feat(incentive): shared ramped_credit_window formula (#1669)"
```

---

### Task 2: Ramp the downstream credit window end-to-end

Replaces the flat `credit_window_bytes` with `credit_max` + `credit_ramp_divisor` across config, the node handler accessor, the serve loop, the pre-flight deposit guards, and the client shortfall estimate. Leaves the tree green (the `[cache]` seed-leech knobs and `LeechGovernor` are still present and untouched here; Task 3 removes them).

**Files:**

- Modify: `crates/common/src/config/mod.rs` (constants ~288-298; resolution ~2527-2547; resolution tests ~5084-5157)
- Modify: `crates/common/src/config/types.rs` (payment section ~781-792)
- Modify: `crates/common/src/config/resolved.rs` (`ResolvedPayment` ~435-440)
- Modify: `crates/node/src/handlers/client/mod.rs` (dep field ~393/464; handler field ~576-580; ctor ~697; accessor ~987-992)
- Modify: `crates/node/src/runtime/mod.rs` (wiring ~1232-1234)
- Modify: `crates/node/src/handlers/client/delivery.rs` (window compute ~177-190, 222-237)
- Modify: `crates/node/src/handlers/client/window.rs` (call sites ~104-109, 496-501 — pass `paid = 0`, keep the `pull_ahead` max term for now)
- Modify: `crates/node/src/handlers/client/dispatch.rs` (call sites ~435-445, 824-826 — pass `paid = 0`)
- Modify: `crates/cli/src/commands/fetch.rs` (estimate ~538-550)
- Modify: `crates/cli/src/commands/config.rs` (template + dump for payment fields ~685, 1004, 1011)
- Test: `crates/node/tests/client_loopback.rs` (new ramp tests; harness field renames ~388/412, 675/700, 3606/3630)

**Interfaces:**

- Consumes: `decdn_incentive::ramped_credit_window` (Task 1).
- Produces:
  - `ResolvedPayment.credit_max: u64`, `ResolvedPayment.credit_ramp_divisor: u64`
  - `ClientHandler::credit_window(&self, interval_bytes: u64, paid: u64) -> u64`
  - Handler fields `credit_max: u64`, `credit_ramp_divisor: u64` (replace `credit_window_bytes: Option<Bytes>`)

- [ ] **Step 1: Write the failing config resolution test**

In `crates/common/src/config/mod.rs` tests, replace `resolve_payment_threads_explicit_credit_window` and the default-window assertion with:

```rust
#[test]
fn resolve_payment_defaults_credit_max_and_ramp_divisor() {
    let resolved = resolve_for_test(None); // use the module's existing resolve-with-defaults helper
    assert_eq!(resolved.payment.credit_max, DEFAULT_CREDIT_MAX);
    assert_eq!(resolved.payment.credit_ramp_divisor, DEFAULT_CREDIT_RAMP_DIVISOR);
}

#[test]
fn resolve_payment_threads_explicit_credit_max_and_ramp_divisor() {
    let mut file = empty_partial_config(); // existing helper used by the old test
    file.payment.credit_max = Some(decdn_config_types::Bytes::new(32 * 1024 * 1024));
    file.payment.credit_ramp_divisor = Some(5);
    let resolved = resolve_for_test(Some(&file));
    assert_eq!(resolved.payment.credit_max, 32 * 1024 * 1024);
    assert_eq!(resolved.payment.credit_ramp_divisor, 5);
}
```

(Use the same construction helpers the surrounding tests already use; match their exact names when you open the file.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p decdn-common config::`
Expected: FAIL — `credit_max` / `credit_ramp_divisor` unknown fields.

- [ ] **Step 3: Add the config constants**

In `crates/common/src/config/mod.rs`, delete `DEFAULT_CREDIT_WINDOW_BYTES` (~288-298) and add:

```rust
/// Default downstream credit-window ceiling (ADR 003 §Credit window): 64 MiB. A
/// stream's window ramps from one voucher interval toward this cap in proportion
/// to what the stream has already paid; a fully-ramped high-bandwidth lane runs
/// link-bound within this bound, while a non-paying lane stays pinned at the
/// interval floor. Node-local policy, floored to one interval by the serve loop.
pub const DEFAULT_CREDIT_MAX: u64 = 64 * 1024 * 1024;
/// Default ramp divisor (ADR 003 §Credit window): 2. The credit window is at most
/// `paid / credit_ramp_divisor`, so the node's unbilled egress on a stream never
/// exceeds half the revenue the stream has already confirmed. Lower ramps faster;
/// `0` opens the full [`DEFAULT_CREDIT_MAX`] from the first byte.
pub const DEFAULT_CREDIT_RAMP_DIVISOR: u64 = 2;
```

- [ ] **Step 4: Add the raw config fields**

In `crates/common/src/config/types.rs`, in the payment section, remove `credit_window_bytes` and add:

```rust
/// Credit-window ceiling in bytes (ADR 003 §Credit window). The per-stream
/// window ramps toward this cap as the stream pays; unset defaults to
/// [`decdn_common::config::DEFAULT_CREDIT_MAX`] (64 MiB).
#[serde(default, skip_serializing_if = "Option::is_none")]
pub credit_max: Option<decdn_config_types::Bytes>,
/// Ramp divisor (ADR 003 §Credit window): the window is `paid / credit_ramp_divisor`,
/// floored at one interval and capped at `credit_max`. Unset defaults to
/// [`decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR`] (2). `0` opens the full
/// ceiling immediately.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub credit_ramp_divisor: Option<u64>,
```

(Match the exact `#[serde(...)]` attributes of the sibling fields when you open the file.)

- [ ] **Step 5: Resolve the fields**

In `crates/common/src/config/resolved.rs`, on `ResolvedPayment` remove `credit_window_bytes: u64` and add:

```rust
/// Credit-window ceiling in bytes (ADR 003 §Credit window). See
/// [`crate::config::DEFAULT_CREDIT_MAX`].
pub credit_max: u64,
/// Ramp divisor for the credit window; `0` opens the full ceiling immediately.
pub credit_ramp_divisor: u64,
```

In `crates/common/src/config/mod.rs` resolution (~2527), replace the `credit_window_bytes` let-binding and the `ResolvedPayment { .. }` fields:

```rust
let credit_max = file
    .and_then(|p| p.credit_max.as_ref())
    .map_or(DEFAULT_CREDIT_MAX, |b| b.get());
let credit_ramp_divisor = file
    .and_then(|p| p.credit_ramp_divisor)
    .unwrap_or(DEFAULT_CREDIT_RAMP_DIVISOR);
```

and in the returned `ResolvedPayment { .. }` swap `credit_window_bytes,` for `credit_max,\n        credit_ramp_divisor,`. Fix every `ResolvedPayment`/`PartialPayment` struct-literal fixture in this file's tests (search `credit_window_bytes`).

- [ ] **Step 6: Run config tests**

Run: `cargo nextest run -p decdn-common config::`
Expected: PASS (both new tests; all fixtures compile).

- [ ] **Step 7: Write the failing loopback ramp test**

In `crates/node/tests/client_loopback.rs`, replace `serve_streams_a_full_credit_window_ahead_of_payment` and `paying_a_voucher_slides_the_credit_window_forward` with ramp behavior (keep the existing harness helpers; only the deps field names change from `credit_window_bytes` to `credit_max` + `credit_ramp_divisor`):

```rust
#[tokio::test]
async fn unpaid_stream_is_served_only_the_floor_then_pauses() {
    // credit_ramp_divisor = 2, credit_max large: with zero vouchers paid the
    // window is the floor (one interval). The node delivers ~one interval ahead
    // of zero payment and then blocks, exactly like stop-and-wait at the floor.
    // (Assert delivered-ahead-of-zero-payment == one interval, ± one chunk.)
}

#[tokio::test]
async fn paying_grows_the_window_to_paid_over_divisor() {
    // After the client pays N intervals, the served-ahead frontier grows to
    // paid / credit_ramp_divisor (clamped to credit_max). Assert the window
    // widened past one interval once paid exceeds divisor * floor.
}

#[tokio::test]
async fn divisor_zero_serves_the_full_credit_max_immediately() {
    // With credit_ramp_divisor = 0 the node serves credit_max ahead of zero
    // payment on first contact (the old flat-window behavior, opt-in).
}
```

Fill each body by adapting the mechanics of the tests you are replacing (they already drive a loopback stream and measure the delivered-ahead frontier). Set `deps.credit_max` / `deps.credit_ramp_divisor` on the harness instead of `deps.credit_window_bytes`.

- [ ] **Step 8: Run to verify it fails**

Run: `cargo nextest run -p decdn-node --test client_loopback`
Expected: FAIL — handler still uses the flat window / fields renamed.

- [ ] **Step 9: Rework the handler fields, accessor, and wiring**

In `crates/node/src/handlers/client/mod.rs`:

- Dep struct: replace `pub credit_window_bytes: Option<Bytes>` with `pub credit_max: u64` and `pub credit_ramp_divisor: u64` (default `credit_max: DEFAULT_CREDIT_MAX`, `credit_ramp_divisor: DEFAULT_CREDIT_RAMP_DIVISOR` in the deps `Default`/builder).
- Handler struct: same two fields (replace `credit_window_bytes`).
- Constructor: set them from deps.
- Accessor (~987): replace the body with the ramp delegate:

```rust
/// The effective downstream credit window in bytes for a stream whose negotiated
/// voucher interval is `interval_bytes` and whose cumulative confirmed payment is
/// `paid` (ADR 003 §Credit window). The window ramps from one interval toward
/// `credit_max` as `paid` grows, so the serve loop's bounded credit exposure —
/// `delivered − paid` — is at most `paid / credit_ramp_divisor`. Floored at one
/// interval so the loop always makes progress; a `credit_ramp_divisor` of `0`
/// opens the full ceiling immediately.
pub(super) fn credit_window(&self, interval_bytes: u64, paid: u64) -> u64 {
    decdn_incentive::ramped_credit_window(
        self.credit_ramp_divisor,
        interval_bytes,
        self.credit_max,
        paid,
    )
}
```

In `crates/node/src/runtime/mod.rs` (~1232), replace the `client_deps.credit_window_bytes = ...` assignment with:

```rust
client_deps.credit_max = cfg.payment.credit_max;
client_deps.credit_ramp_divisor = cfg.payment.credit_ramp_divisor;
```

- [ ] **Step 10: Recompute the window in the serve loop**

In `crates/node/src/handlers/client/delivery.rs`:

- At ~181, remove `let window = self.credit_window(interval_bytes);`.
- At ~188, size `batch_cap` against the ceiling so it is stable across the ramp:

```rust
// Group-commit cap: bounded by how many intervals fit in the widest window the
// ramp can reach (`credit_max`), so the batch size is stable as the window grows.
let ceiling = self.credit_window(interval_bytes, u64::MAX);
let batch_cap = usize::try_from(ceiling / interval_bytes.max(1))
    .unwrap_or(usize::MAX)
    .max(1);
```

- Inside `loop {` (top, before the deliver `while`), recompute the live window from `paid`:

```rust
// The ramped window for the payment confirmed so far (ADR 003 §Credit window).
// Recomputed each iteration: as `paid` advances in the recoup phase the window
// widens, so a paying stream ramps toward `credit_max` while a non-payer stays
// pinned at the one-interval floor.
let window = self.credit_window(interval_bytes, paid);
```

The pre-send guard `delivered.saturating_sub(paid) >= window` (~237) is unchanged; it now reads the recomputed `window`.

- [ ] **Step 11: Update the guard call sites to pass `paid = 0`**

In `crates/node/src/handlers/client/window.rs` (~109 and ~501) change `self.credit_window(interval_bytes)` to `self.credit_window(interval_bytes, 0)` — keep the surrounding `.max(pull_ahead...)` for now (Task 3 removes it).
In `crates/node/src/handlers/client/dispatch.rs` (~440 and ~826) change `self.credit_window(interval_bytes)` to `self.credit_window(interval_bytes, 0)`.

- [ ] **Step 12: Update the client shortfall estimate**

In `crates/cli/src/commands/fetch.rs` (~547), the node now reserves one interval (the floor) pre-flight, not a full window. Replace the estimate basis and rewrite the comment as present-tense canon:

```rust
// The node's pre-flight reservation is one voucher interval (the ramp floor);
// the window only widens as this pool pays, so one interval is the true lower
// bound on what it reserves before serving. Estimated with the shipped default
// interval, since we cannot read the node's config.
let estimate = min_payment(
    decdn_common::config::DEFAULT_VOUCHER_INTERVAL_MB * decdn_common::config::MB_BYTES,
    quoted_rate,
);
```

(Confirm `MB_BYTES` is exported from `decdn_common::config`; if it lives elsewhere use the same constant the node uses to turn `voucher_interval_mb` into bytes.) Trim the "several times it / configured credit window" clause from the user-facing string so it matches the one-interval floor.

- [ ] **Step 13: Update the config template + dump**

In `crates/cli/src/commands/config.rs`, replace the `credit_window_bytes` template comment and the two dump lines (~685, 1004, 1011) with `credit_max` and `credit_ramp_divisor` entries, mirroring the doc text of the new resolved fields. Leave the `[cache]` seed-leech entries alone (Task 3).

- [ ] **Step 14: Build, lint, and run the tests**

Run:

```bash
cargo build -p decdn-node -p decdn-cli -p decdn-common && cargo clippy -p decdn-node -p decdn-cli -p decdn-common
cargo nextest run -p decdn-common -p decdn-node -p decdn-cli
```

Expected: green build/clippy; the three new loopback ramp tests PASS; config tests PASS.

- [ ] **Step 15: Commit**

```bash
cargo fmt
git add -A
git commit -m "feat(node): ramp the downstream credit window on cumulative paid bytes (#1669)"
```

---

### Task 3: Delete the ADR 037 seed-leech caps and rework the pull-leg pacer

Delete `LeechGovernor`, the three `[cache]` knobs, and the pull-leg governor pacing; replace it with a ramp-aware `RampPacer` that paces the upstream pull on the same `ramped_credit_window`, keyed on the served-paid frontier. This is one compile-coherent slice — the governor is threaded through the pacer, so the pacer rework and the governor deletion land together.

**Files:**

- Create: `RampPacer` in `crates/client-pull/src/pacer.rs`; export from `crates/client-pull/src/lib.rs:69`
- Delete: `crates/node/src/leech_governor.rs`; its `pub mod leech_governor;` in `crates/node/src/lib.rs:27`
- Modify: `crates/node/src/node_origin/pull_leg.rs` (delete `LeechPacer` ~186-221; construction ~605-606, 889-890; test ctors ~1072-1073, 1103-1104; `run_pull_leg` signature ~531, 607, 866-891)
- Modify: `crates/node/src/handlers/client/window.rs` (~104-109 drop `pull_ahead` max term → floor only; ~325-355 and ~625-653 stop passing the governor; ~142 remove `leech_admit` gate)
- Modify: `crates/node/src/handlers/client/mod.rs` (`leech_governor` dep+field ~400/593/699)
- Modify: `crates/node/src/handlers/client/fill.rs` (delete `leech_admit` ~250-256)
- Modify: `crates/node/src/handlers/client/voucher.rs` (delete `record_served` ~418-421)
- Modify: `crates/node/src/runtime/mod.rs` (delete governor construction/wiring ~1134-1176, 1241)
- Modify: `crates/node/src/metrics.rs` (delete leech-pause metrics ~1091-1100, 2127, 2131)
- Modify: `crates/common/src/config/types.rs` (`[cache]` ~548-573), `crates/common/src/config/mod.rs` (defaults ~287, 334-342; resolution ~1788-1819; cross-field validation), `crates/common/src/config/resolved.rs` (`ResolvedCache` ~298-308)
- Modify: `crates/cli/src/commands/config.rs` (cache template + dump ~272-275, 677-679, 938-996)
- Test: `crates/client-pull/src/pacer.rs` (RampPacer unit tests)

**Interfaces:**

- Consumes: `decdn_incentive::ramped_credit_window` (Task 1); `ClientHandler::credit_window(interval_bytes, paid)` (Task 2); `PaceState.served_paid_frontier`, `WindowPacer`, `Pacer`, `PaceDecision`.
- Produces: `pub struct RampPacer { pub divisor: u64, pub floor: u64, pub credit_max: u64 }` impl `Pacer`; `run_pull_leg` takes `(credit_ramp_divisor: u64, floor: u64, credit_max: u64)` in place of `window: u64`.

- [ ] **Step 1: Write the failing RampPacer test**

In `crates/client-pull/src/pacer.rs` tests, add:

```rust
#[test]
fn ramp_pacer_paces_pull_on_the_ramped_window() {
    // Unpaid: served_paid_frontier = 0 -> window = floor. The pull may run at
    // most `floor` ahead of the served-paid frontier, then Wait.
    let floor = 4 * CHUNK_GROUP_BYTES;
    let pacer = RampPacer { divisor: 2, floor, credit_max: 64 * CHUNK_GROUP_BYTES };
    let mut s = PaceState::default();
    s.served_paid_frontier = 0;
    s.pulled_frontier = floor; // already floor ahead
    // ... set the money fields BudgetPacer needs so it would otherwise Draw ...
    assert_eq!(pacer.decide(&s), PaceDecision::Wait);
}

#[test]
fn ramp_pacer_widens_the_pull_window_as_served_paid_advances() {
    // served_paid_frontier = 32 groups, divisor 2 -> window 16 groups > floor,
    // so a pull only `floor` ahead may Draw again.
    let floor = 4 * CHUNK_GROUP_BYTES;
    let pacer = RampPacer { divisor: 2, floor, credit_max: 64 * CHUNK_GROUP_BYTES };
    let mut s = PaceState::default();
    s.served_paid_frontier = 32 * CHUNK_GROUP_BYTES;
    s.pulled_frontier = floor;
    // ... money fields set to Draw ...
    assert!(matches!(pacer.decide(&s), PaceDecision::Draw { .. }));
}
```

(Mirror the field setup the existing `WindowPacer` tests in this module use for `PaceState`; copy their money-field initialization so `BudgetPacer` returns `Draw`.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p decdn-client-pull pacer::`
Expected: FAIL — `RampPacer` not found.

- [ ] **Step 3: Implement `RampPacer`**

In `crates/client-pull/src/pacer.rs`, after `WindowPacer`:

```rust
/// The node pull-leg's ramped pacing policy (ADR 003 §Credit window / ADR 037):
/// compose [`WindowPacer`] over a window that itself ramps with the downstream
/// served-paid frontier, so on the fused serve-miss path the upstream pull never
/// runs further ahead of cleared client payment than the ramped credit window
/// allows. A non-paying client's request therefore fronts at most one interval of
/// speculative upstream spend; the window widens only as the client pays.
#[derive(Debug, Clone, Copy)]
pub struct RampPacer {
    pub divisor: u64,
    pub floor: u64,
    pub credit_max: u64,
}

impl Pacer for RampPacer {
    fn decide(&self, s: &PaceState) -> PaceDecision {
        let window = decdn_incentive::ramped_credit_window(
            self.divisor,
            self.floor,
            self.credit_max,
            s.served_paid_frontier,
        );
        WindowPacer::new(window).decide(s)
    }
}
```

Export it: `crates/client-pull/src/lib.rs:69` add `RampPacer` to the `pub use pacer::{...}` list. Confirm `decdn-client-pull` depends on `decdn-incentive` in `crates/client-pull/Cargo.toml`; if not, add it (the crate graph allows `client-pull → incentive`).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo nextest run -p decdn-client-pull pacer::`
Expected: PASS (new tests + existing `WindowPacer` tests unchanged).

- [ ] **Step 5: Rework `run_pull_leg` to build `RampPacer`, delete `LeechPacer`**

In `crates/node/src/node_origin/pull_leg.rs`:

- Delete the `LeechPacer` struct (~186-197) and its `impl Pacer` (~199-221).
- Change `run_pull_leg`'s signature: remove the `window: u64` parameter and the `governor: Option<Arc<LeechGovernor>>` parameter; add `credit_ramp_divisor: u64`, `credit_floor: u64`, `credit_max: u64`.
- At the two construction sites (~605, ~889) replace `let leech_pacer = LeechPacer { window: WindowPacer::new(window), governor, peer, last_pulled, refused };` and its use with:

```rust
let pacer = decdn_client_pull::RampPacer {
    divisor: credit_ramp_divisor,
    floor: credit_floor,
    credit_max,
};
```

and pass `&pacer` wherever `&leech_pacer` was passed. Delete the now-unused `last_pulled` / `refused` locals and the `refused`-based scoring skip (a ramp `Wait` is not a fault; keep provider scoring as it was for a real provider error). At the test ctors (~1072, ~1103) construct `RampPacer` directly with literal divisor/floor/credit_max.

- Remove `use ...LeechGovernor` and `record_served`/`poll_admission` imports.

- [ ] **Step 6: Update `window.rs` to pass ramp params and drop the leech admission**

In `crates/node/src/handlers/client/window.rs`:

- At ~104-109 replace the `window` computation with the floor only (the ramp lives in the pacer now, and the pre-flight guard prices the floor):

```rust
// The pre-flight reservation is the ramp floor — one voucher interval. In-stream
// exposure is bounded by the ramped credit window, which the serve loop and the
// pull-leg `RampPacer` both enforce.
let window = self.credit_window(interval_bytes, 0);
```

- At the `run_pull_leg(...)` calls (~334-346, ~653) stop passing the governor and the old `window`; pass `self.credit_ramp_divisor, self.credit_window(interval_bytes, 0), self.credit_max` (interval floor + ceiling + divisor).
- Delete the `leech_admit` gate block (~139-149) and the `let peer = client_node_id.0;` if it becomes unused.

- [ ] **Step 7: Delete `LeechGovernor` and its wiring**

- Delete file `crates/node/src/leech_governor.rs`.
- In `crates/node/src/lib.rs:27` delete `pub mod leech_governor;`.
- In `crates/node/src/handlers/client/mod.rs` delete the `leech_governor` dep field (~400), the handler field (~593), and its constructor assignment (~699).
- In `crates/node/src/handlers/client/fill.rs` delete `leech_admit` (~250-256).
- In `crates/node/src/handlers/client/voucher.rs` delete the `record_served` call (~418-421).
- In `crates/node/src/runtime/mod.rs` delete the governor construction and threading (~1134-1176, ~1241).
- In `crates/node/src/metrics.rs` delete `node_pull_through_leech_budget_paused` and `node_pull_through_share_ratio_paused` (registration ~1091-1100 and emit sites ~2127, ~2131).

- [ ] **Step 8: Delete the three `[cache]` config knobs**

- `crates/common/src/config/types.rs` (~548-573): delete `pull_ahead_bytes`, `max_unrecouped_leech_bytes`, `pull_share_ratio_percent`.
- `crates/common/src/config/mod.rs`: delete `DEFAULT_PULL_AHEAD_BYTES` (~287), `DEFAULT_MAX_UNRECOUPED_LEECH_BYTES` (~338), `DEFAULT_PULL_SHARE_RATIO_PERCENT` (~342); delete their resolution (~1788-1800) and the `pull_ahead_bytes ≤ max_unrecouped_leech_bytes` cross-field validation (~1808-1819); delete the three fields from the `ResolvedCache { .. }` construction (~1846-1848).
- `crates/common/src/config/resolved.rs` (~298-308): delete the three `ResolvedCache` fields.
- Delete the config round-trip / validation tests for these knobs in `crates/common/src/config/mod.rs` (~4510-4568) and fix any `ResolvedCache`/`PartialCache` struct-literal fixtures.
- `crates/cli/src/commands/config.rs`: delete the cache template comments and dump lines (~272-275, 677-679, 938-996).

- [ ] **Step 9: Build the whole workspace and lint**

Run:

```bash
cargo build && cargo clippy
```

Expected: green. Fix any straggler references the compiler flags (unused imports, leftover `pull_ahead_bytes`/`governor` mentions).

- [ ] **Step 10: Run the affected crates' tests + anvil-e2e compile gate**

Run:

```bash
cargo nextest run -p decdn-client-pull -p decdn-node -p decdn-common
cargo test --no-run -p decdn-e2e --features anvil-e2e
```

Expected: PASS; e2e compiles against the new `run_pull_leg` signature.

- [ ] **Step 11: Commit**

```bash
cargo fmt
git add -A
git commit -m "feat(node): delete LeechGovernor + seed-leech caps, pace the pull leg on the ramped window (#1669)"
```

---

### Task 4: Rewrite ADR 003 §Credit Window and ADR 037 §Seed-leech caps

**Files:**

- Modify: `adr/003-payments.md` (§Credit Window ~90-106; §Concurrent Streams per-lane text ~246-248)
- Modify: `adr/037-regional-proxy-warming.md` (§Seed-leech caps ~86-93; params table ~105-119; §Implementation status ~121-133; threat-model/acceptance ~154-180; the pull-ahead line ~58, 61)

**Interfaces:** none (prose). ASD-STE100 Simplified Technical English; present-tense canon; no issue/PR numbers in ADR prose; every line stands alone read cold.

- [ ] **Step 1: Rewrite ADR 003 §Credit Window**

Replace the fixed-window prose with the ramp. The window is `min(credit_max, max(interval, paid / credit_ramp_divisor))`. A stream starts at one voucher interval and grows its window in proportion to its own confirmed payment, up to `credit_max`. State the exposure invariant: the node's unbilled egress on a stream is at most `paid / credit_ramp_divisor`, so a stream never fronts more than a fixed fraction of the revenue it has already confirmed, and a non-paying stream stays pinned at one interval. Keep and update the floor-at-one-interval, node-local-policy, durability, and takedown-latency paragraphs so "the window" means the ramped window (takedown latency is bounded by `credit_max`, floored at one interval). Update the §Concurrent Streams per-lane stop text (~246-248) so the window each lane pauses on is the ramped window; the "fresh signer buys nothing" property is unchanged and now also holds because a new stream starts at the floor.

- [ ] **Step 2: Rewrite ADR 037 seed-leech content**

- Delete the §Seed-leech caps section (~86-93).
- Delete the `pull_ahead_bytes`, `max_unrecouped_leech_bytes`, `share_ratio` rows from the parameters table (~105-119) and the surrounding sentences that only exist to introduce them.
- Rewrite §Implementation status (~121-133): the fused serve-miss path is paced by the ramped credit window (ADR 003); the pull runs no further ahead of cleared client payment than that window, so an abandoned request costs at most the current window of upstream spend. The pre-flight deposit guard refuses a cache-miss pull whose pool cannot cover the floor (one interval); node-wide speculative exposure is bounded by the sum of pool deposits divided by the ramp divisor, a real-capital bound. Remove every `LeechGovernor` / `pull_ahead_bytes` / `share_ratio` / `max_unrecouped_leech_bytes` mention here and at ~58, ~61, ~84.
- Update the threat-model (~154-155) and acceptance criteria (~170-171, ~179-180) items that named the caps to name the ramped window and the deposit bound instead.

- [ ] **Step 3: Verify ADR hygiene locally**

Run:

```bash
pre-commit run --all-files
```

Expected: the `adr reference hygiene` and `markdownlint` hooks PASS (fix links/anchors/section-spacing the hook flags).

- [ ] **Step 4: Commit**

```bash
git add adr/003-payments.md adr/037-regional-proxy-warming.md
git commit -m "docs(adr): ramped credit window in 003; drop seed-leech caps from 037 (#1669)"
```

---

### Task 5: Test sweep, fixtures, and full verification

**Files:**

- Modify: `crates/node/tests/node_origin_pull.rs`, `crates/node/tests/origin_range_pull.rs` (replace seed-leech-cap pause assertions with ramped-window pacing assertions; delete tests that only exercised the deleted caps)
- Modify fixtures: `crates/cli/tests/config_validate.rs:379-393`, `crates/node/tests/sighup_signal.rs:118`, `crates/node/tests/anvil_bringup_shutdown_e2e.rs:392`, `crates/node/src/runtime/mod.rs:3744`, `crates/node/src/runtime/reload.rs:791,1039,1274`, `crates/e2e/tests/cli_fetch_topup.rs:557,890`

**Interfaces:** none new. Consumes everything from Tasks 1-3.

- [ ] **Step 1: Sweep the fixtures**

Grep and fix every remaining reference:

```bash
grep -rn 'credit_window_bytes\|DEFAULT_CREDIT_WINDOW_BYTES\|pull_ahead_bytes\|pull_share_ratio_percent\|max_unrecouped_leech_bytes\|DEFAULT_PULL_AHEAD_BYTES\|DEFAULT_MAX_UNRECOUPED_LEECH_BYTES\|DEFAULT_PULL_SHARE_RATIO_PERCENT\|leech_governor\|LeechGovernor\|LeechPacer' crates/ | grep -v '_history'
```

For each hit: config fixtures set `credit_max` / `credit_ramp_divisor` instead of `credit_window_bytes` and drop the three cache knobs; reload restart-gate lists drop `credit_window_bytes` and add the new fields (mirror how the sibling payment fields are listed); e2e comments referencing the flat window are rewritten to the ramp.

- [ ] **Step 2: Rework the pull-leg integration tests**

In `crates/node/tests/node_origin_pull.rs` and `crates/node/tests/origin_range_pull.rs`, delete assertions that a pull pauses on `max_unrecouped_leech_bytes` / `share_ratio`. Where a test asserted the pull runs a bounded distance ahead, re-express the bound as the ramped window keyed on the client's paid frontier (unpaid ⇒ pull stops one interval ahead). Keep the origin-range correctness tests (outboard verification, tamper rejection) unchanged.

- [ ] **Step 3: Run the full test suite**

Run:

```bash
cargo nextest run
```

Expected: PASS. Investigate any failure per superpowers:systematic-debugging — do not tune timeouts to paper over a real ramp regression.

- [ ] **Step 4: Full gate — build, clippy, fmt, deny, e2e compile, grep-clean**

Run:

```bash
cargo build && cargo clippy
cargo fmt -- --check
cargo deny check
cargo test --no-run -p decdn-e2e --features anvil-e2e
grep -rn 'credit_window_bytes\|pull_ahead_bytes\|leech_governor\|LeechGovernor\|LeechPacer\|max_unrecouped_leech_bytes\|pull_share_ratio_percent' crates/ adr/ | grep -v '_history'
```

Expected: all green; the final grep prints nothing (all references gone outside `adr/_history/`).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test: sweep fixtures + pull-leg tests to the ramped credit window (#1669)"
```

---

## Self-Review

**Spec coverage:**

- Ramp formula → Task 1. Config surface (add `credit_max`/`credit_ramp_divisor`, remove `credit_window_bytes` + three cache knobs) → Tasks 2 & 3. Ramp accessor + serve-loop recompute → Task 2. Reservation guards price floor → Tasks 2 (pass `paid=0`) & 3 (drop `pull_ahead` term). Pull-leg pacer rework → Task 3. `LeechGovernor` deletion → Task 3. Client `fetch.rs` estimate → Task 2. Config template/dump → Tasks 2 (payment) & 3 (cache). ADR 003 + 037 → Task 4. Tests/fixtures → Tasks 2, 3, 5. All spec sections map to a task.

**Placeholder scan:** Test bodies in Task 2 Step 7 and Task 3 Step 1 give the assertion shape and instruct adapting the existing loopback/pacer harness mechanics rather than pasting invented harness APIs — the exact harness helper names are read from the files at implementation time. This is deliberate (those helpers are private and file-specific), not a TBD; every behavioral assertion is fully specified.

**Type consistency:** `ramped_credit_window(divisor, floor, credit_max, paid)` — argument order identical in Task 1 (definition), Task 2 (`credit_window` accessor), Task 3 (`RampPacer`). `credit_window(interval_bytes, paid)` — two args everywhere it is called (Task 2 delivery/guards pass `paid`/`0`; Task 3 guards pass `0`). `run_pull_leg` gains `(credit_ramp_divisor, credit_floor, credit_max)` and loses `(window, governor)` consistently at its signature, both call sites, and both test ctors.
