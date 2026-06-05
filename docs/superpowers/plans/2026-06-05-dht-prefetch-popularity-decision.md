# DHT Prefetch — Popularity Signal + Decision Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the ADR-022-canonical FIND_VALUE popularity oracle and prefetch-decision engine in `decdn-node`, plus its `[prefetch]` config block and metrics, behind `prefetch.enabled=false`, with the DHT handler deciding-and-metering but not yet firing the real acquisition.

**Architecture:** A new pure `crates/node/src/prefetch/` module holds a `PopularityTracker` (sliding-window per-hash FIND_VALUE counter) and a `PrefetchPolicy` decision engine (enabled → demand-quality throttle → authorized-origin → budget gates). A `PrefetchEngine` façade ties them to config, metrics, and the existing `OriginDirectory` trait. The FIND_VALUE handler feeds the tracker and, on a threshold-cross, runs the decision and emits metrics. With `enabled=false` the whole path is inert.

**Tech Stack:** Rust (edition 2024, MSRV 1.95), `iroh_metrics` (Counter/Gauge), `cargo nextest`. Workspace clippy denies `unwrap_used`/`expect_used`/`panic`/`indexing_slicing`; `rustfmt.toml` `max_width = 100`.

**Spec:** `docs/superpowers/specs/2026-06-05-dht-prefetch-popularity-decision-design.md`

**Conventions used below:**

- Lock poisoning is handled with `if let Ok(g) = m.lock() { … } else { tracing::error!(…); <safe fallback> }` — never `unwrap`/`expect`. Tracker fallback = "no trigger"; decision fallback = "skip" (fail closed).
- Run all tests with `cargo nextest run` (preferred over `cargo test`).
- Commit after each task with a conventional-commit message.

---

## Task 1: `[prefetch]` config block (file + resolved + resolver)

**Files:**

- Modify: `crates/common/src/config/types.rs` (add `PrefetchConfig`, `FileConfig.prefetch`)
- Modify: `crates/common/src/config/resolved.rs` (add `ResolvedPrefetch` + `Default`, `ResolvedConfig.prefetch`)
- Modify: `crates/common/src/config/mod.rs` (constants, `resolve_prefetch_into`, `#[cfg(test)] resolve_prefetch` shim, wire into `resolve_config`, re-export)

- [ ] **Step 1: Add the file-config struct.** In `crates/common/src/config/types.rs`, add a field to `FileConfig` (after `pub receipts: Option<ReceiptsConfig>,`):

```rust
    /// Speculative-prefetch operator policy (ADR 022 §Prefetch Decision).
    /// Absent => prefetch disabled with the ADR's recommended defaults.
    pub prefetch: Option<PrefetchConfig>,
```

Then add the struct (place it after `ReceiptsConfig`):

```rust
/// `[prefetch]` — speculative-prefetch operator policy (ADR 022 §Prefetch
/// Decision "Recommended configuration"). Every field is optional; absent
/// keys take the ADR's recommended defaults. The whole feature is gated off
/// by `enabled = false` by default — operators must affirmatively opt in.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrefetchConfig {
    /// Master switch. Absent => `false` (opt-in).
    pub enabled: Option<bool>,
    /// Require an authorized origin in the FIND_VALUE candidate set before
    /// prefetching. Absent => `true`. Closes the demand-supply Sybil attack.
    pub require_authorized_origin: Option<bool>,
    /// Hard cap on aggregate prefetch spend over a rolling 1-hour window, in
    /// micro-USDC. Absent => `0` (no budget => never prefetches; a finite cap
    /// is the load-bearing recommendation).
    pub budget_usdc_per_hour: Option<u64>,
    /// FIND_VALUE queries for a hash within `threshold_window_secs` that trip
    /// the prefetch trigger. Absent => `5`. Must be `> 0`.
    pub find_value_threshold: Option<u32>,
    /// Rolling-window length (seconds) for the FIND_VALUE trigger. Absent =>
    /// `300`. Must be `> 0`.
    pub threshold_window_secs: Option<u64>,
    /// Auto-throttle floor on `served_bytes / acquired_bytes` over the
    /// demand-quality window. Absent => `0.1`. Must be finite in `[0.0, 1.0]`.
    pub demand_quality_min_ratio: Option<f64>,
    /// Rolling-window length (seconds) for the demand-quality predicate.
    /// Absent => `3600`. Must be `> 0`.
    pub demand_quality_window_secs: Option<u64>,
}
```

- [ ] **Step 2: Add the resolved struct.** In `crates/common/src/config/resolved.rs`, add after `ResolvedReceipts`'s `Default` impl:

```rust
/// Resolved speculative-prefetch policy (ADR 022 §Prefetch Decision).
///
/// All fields validated by the resolver: `find_value_threshold > 0`,
/// `threshold_window_secs > 0`, `demand_quality_window_secs > 0`, and
/// `demand_quality_min_ratio` finite in `[0.0, 1.0]`. `Default` reuses the
/// `DEFAULT_PREFETCH_*` resolver constants so hand-built `ResolvedConfig`s in
/// tests cannot drift from production defaults.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedPrefetch {
    pub enabled: bool,
    pub require_authorized_origin: bool,
    pub budget_usdc_per_hour: u64,
    pub find_value_threshold: u32,
    pub threshold_window_secs: u64,
    pub demand_quality_min_ratio: f64,
    pub demand_quality_window_secs: u64,
}

impl Default for ResolvedPrefetch {
    fn default() -> Self {
        Self {
            enabled: super::DEFAULT_PREFETCH_ENABLED,
            require_authorized_origin: super::DEFAULT_PREFETCH_REQUIRE_AUTHORIZED_ORIGIN,
            budget_usdc_per_hour: super::DEFAULT_PREFETCH_BUDGET_USDC_PER_HOUR,
            find_value_threshold: super::DEFAULT_PREFETCH_FIND_VALUE_THRESHOLD,
            threshold_window_secs: super::DEFAULT_PREFETCH_THRESHOLD_WINDOW_SECS,
            demand_quality_min_ratio: super::DEFAULT_PREFETCH_DEMAND_QUALITY_MIN_RATIO,
            demand_quality_window_secs: super::DEFAULT_PREFETCH_DEMAND_QUALITY_WINDOW_SECS,
        }
    }
}
```

Then add `pub prefetch: ResolvedPrefetch,` to `ResolvedConfig` (after `pub receipts: ResolvedReceipts,`).

- [ ] **Step 3: Add resolver constants.** In `crates/common/src/config/mod.rs`, after the `MAX_RECEIPT_RETAINED_FILES` const (~line 160):

```rust
/// Default `prefetch.enabled` (ADR 022 §Prefetch Decision): opt-in.
pub const DEFAULT_PREFETCH_ENABLED: bool = false;
/// Default `prefetch.require_authorized_origin`: closes the demand-supply Sybil.
pub const DEFAULT_PREFETCH_REQUIRE_AUTHORIZED_ORIGIN: bool = true;
/// Default `prefetch.budget_usdc_per_hour`: `0` => never prefetches.
pub const DEFAULT_PREFETCH_BUDGET_USDC_PER_HOUR: u64 = 0;
/// Default `prefetch.find_value_threshold` (ADR 022 §Prefetch Decision table).
pub const DEFAULT_PREFETCH_FIND_VALUE_THRESHOLD: u32 = 5;
/// Default `prefetch.threshold_window_secs` (ADR 022 §Prefetch Decision table).
pub const DEFAULT_PREFETCH_THRESHOLD_WINDOW_SECS: u64 = 300;
/// Default `prefetch.demand_quality_min_ratio` (ADR 022 §Prefetch Decision table).
pub const DEFAULT_PREFETCH_DEMAND_QUALITY_MIN_RATIO: f64 = 0.1;
/// Default `prefetch.demand_quality_window_secs` (ADR 022 §Prefetch Decision table).
pub const DEFAULT_PREFETCH_DEMAND_QUALITY_WINDOW_SECS: u64 = 3600;
```

- [ ] **Step 4: Write the failing resolver test.** Append to the `#[cfg(test)] mod tests` block in `crates/common/src/config/mod.rs` (find it with `grep -n "mod tests" crates/common/src/config/mod.rs`; add the tests inside it):

```rust
    #[test]
    fn prefetch_defaults_match_adr() {
        let r = resolve_prefetch(None).expect("defaults must resolve");
        assert!(!r.enabled);
        assert!(r.require_authorized_origin);
        assert_eq!(r.budget_usdc_per_hour, 0);
        assert_eq!(r.find_value_threshold, 5);
        assert_eq!(r.threshold_window_secs, 300);
        assert!((r.demand_quality_min_ratio - 0.1).abs() < f64::EPSILON);
        assert_eq!(r.demand_quality_window_secs, 3600);
    }

    #[test]
    fn prefetch_rejects_zero_threshold() {
        let file = types::PrefetchConfig {
            find_value_threshold: Some(0),
            ..Default::default()
        };
        assert!(resolve_prefetch(Some(&file)).is_err());
    }

    #[test]
    fn prefetch_rejects_ratio_above_one() {
        let file = types::PrefetchConfig {
            demand_quality_min_ratio: Some(1.5),
            ..Default::default()
        };
        assert!(resolve_prefetch(Some(&file)).is_err());
    }

    #[test]
    fn prefetch_rejects_zero_windows() {
        let win = types::PrefetchConfig {
            threshold_window_secs: Some(0),
            ..Default::default()
        };
        assert!(resolve_prefetch(Some(&win)).is_err());
        let dq = types::PrefetchConfig {
            demand_quality_window_secs: Some(0),
            ..Default::default()
        };
        assert!(resolve_prefetch(Some(&dq)).is_err());
    }
```

- [ ] **Step 5: Run the test to verify it fails.**

Run: `cargo nextest run -p decdn-common prefetch_`
Expected: FAIL — `resolve_prefetch` / `types::PrefetchConfig` / `ResolvedPrefetch` not found.

- [ ] **Step 6: Implement the resolver.** In `crates/common/src/config/mod.rs`, add next to `resolve_receipts` / `resolve_receipts_into` (~line 1669):

```rust
/// Single-section shim for direct unit tests; `resolve_config` uses the
/// `_into` worker with the shared bag.
#[cfg(test)]
fn resolve_prefetch(file: Option<&types::PrefetchConfig>) -> anyhow::Result<ResolvedPrefetch> {
    one_section(|bag| resolve_prefetch_into(file, bag))
}

/// Bag-threading worker for the `[prefetch]` section (ADR 022 §Prefetch
/// Decision). Shares a bag with the other sections during startup so an
/// operator sees every config problem in one pass; see `resolve_payment_into`.
fn resolve_prefetch_into(
    file: Option<&types::PrefetchConfig>,
    bag: &mut ConfigErrorBag,
) -> ResolvedPrefetch {
    let enabled = file
        .and_then(|p| p.enabled)
        .unwrap_or(DEFAULT_PREFETCH_ENABLED);
    let require_authorized_origin = file
        .and_then(|p| p.require_authorized_origin)
        .unwrap_or(DEFAULT_PREFETCH_REQUIRE_AUTHORIZED_ORIGIN);
    let budget_usdc_per_hour = file
        .and_then(|p| p.budget_usdc_per_hour)
        .unwrap_or(DEFAULT_PREFETCH_BUDGET_USDC_PER_HOUR);

    let find_value_threshold = file
        .and_then(|p| p.find_value_threshold)
        .unwrap_or(DEFAULT_PREFETCH_FIND_VALUE_THRESHOLD);
    bag.check(
        find_value_threshold > 0,
        "prefetch.find_value_threshold",
        "prefetch.find_value_threshold must be > 0",
    );

    let threshold_window_secs = file
        .and_then(|p| p.threshold_window_secs)
        .unwrap_or(DEFAULT_PREFETCH_THRESHOLD_WINDOW_SECS);
    bag.check(
        threshold_window_secs > 0,
        "prefetch.threshold_window_secs",
        "prefetch.threshold_window_secs must be > 0",
    );

    let demand_quality_min_ratio = file
        .and_then(|p| p.demand_quality_min_ratio)
        .unwrap_or(DEFAULT_PREFETCH_DEMAND_QUALITY_MIN_RATIO);
    bag.check(
        demand_quality_min_ratio.is_finite()
            && (0.0..=1.0).contains(&demand_quality_min_ratio),
        "prefetch.demand_quality_min_ratio",
        "prefetch.demand_quality_min_ratio must be a finite number in [0.0, 1.0]",
    );

    let demand_quality_window_secs = file
        .and_then(|p| p.demand_quality_window_secs)
        .unwrap_or(DEFAULT_PREFETCH_DEMAND_QUALITY_WINDOW_SECS);
    bag.check(
        demand_quality_window_secs > 0,
        "prefetch.demand_quality_window_secs",
        "prefetch.demand_quality_window_secs must be > 0",
    );

    ResolvedPrefetch {
        enabled,
        require_authorized_origin,
        budget_usdc_per_hour,
        find_value_threshold,
        threshold_window_secs,
        demand_quality_min_ratio,
        demand_quality_window_secs,
    }
}
```

- [ ] **Step 7: Wire into `resolve_config` and re-export.** In `resolve_config` (~line 214), after `let receipts = resolve_receipts_into(...)`:

```rust
    let prefetch = resolve_prefetch_into(file.prefetch.as_ref(), &mut bag);
```

and add `prefetch,` to the `Ok(ResolvedConfig { … })` literal (after `receipts,`).

Then mirror every place `ResolvedReceipts` / `ReceiptsConfig` is re-exported. Run:

```bash
grep -rn "ResolvedReceipts" crates/common/src
grep -rn "ReceiptsConfig" crates/common/src/config/mod.rs crates/common/src/lib.rs
```

For each `pub use ... ResolvedReceipts ...` line (e.g. the `pub use resolved::{...}` group near `crates/common/src/config/mod.rs:24` and any `crates/common/src/lib.rs` re-export), add `ResolvedPrefetch` to the same list, alphabetically adjacent. If `ReceiptsConfig` (the file type) is re-exported anywhere, add `PrefetchConfig` alongside it.

- [ ] **Step 8: Fix hand-built `ResolvedConfig` literals.** Adding a non-`Option` field breaks every struct-literal `ResolvedConfig { … }`. Find them:

```bash
grep -rn "ResolvedConfig {" crates/
```

In each (production and tests), add `prefetch: ResolvedPrefetch::default(),` (import `decdn_common::config::ResolvedPrefetch` where needed, or use the crate-local path). Builders that use `..Default::default()` need no change.

- [ ] **Step 9: Run resolver tests + full common build.**

Run: `cargo nextest run -p decdn-common && cargo build -p decdn-common`
Expected: PASS (the four new tests pass; no broken literals).

- [ ] **Step 10: Commit.**

```bash
git add crates/common/src/config
git commit -m "feat(config): add [prefetch] block resolving ADR 022 prefetch policy (#650)"
```

---

## Task 2: DEFAULT_CONFIG template + `config validate` summary line

**Files:**

- Modify: `crates/cli/src/commands/config.rs` (`write_validate_summary`, `DEFAULT_CONFIG`)

- [ ] **Step 1: Write the failing summary test.** Find the test module in `crates/cli/src/commands/config.rs` (`grep -n "mod tests" crates/cli/src/commands/config.rs`). If there is an existing summary test that builds a `ResolvedConfig`, copy its setup; otherwise add:

```rust
    #[test]
    fn summary_includes_prefetch_enabled() {
        let mut resolved = sample_resolved_config(); // existing test helper; if absent, build via resolve_config in a tempdir like the other summary tests
        resolved.prefetch.enabled = true;
        let mut buf = Vec::new();
        write_validate_summary(&mut buf, None, &resolved).expect("write summary");
        let text = String::from_utf8(buf).expect("utf8");
        assert!(text.contains("prefetch_enabled:"));
        assert!(text.contains("prefetch_enabled:            true") || text.contains("prefetch_enabled: true"));
    }
```

If there is no `sample_resolved_config()` helper, mirror whatever existing `write_validate_summary` test constructs a `ResolvedConfig` (search `write_validate_summary` usages in tests) and set `resolved.prefetch.enabled = true` on it.

- [ ] **Step 2: Run it to verify failure.**

Run: `cargo nextest run -p decdn summary_includes_prefetch_enabled` (the CLI binary crate is `decdn`; confirm with `grep name crates/cli/Cargo.toml`)
Expected: FAIL — no `prefetch_enabled:` line.

- [ ] **Step 3: Add the summary line.** In `write_validate_summary`, before the final `Ok(())` (after the `otlp_endpoint` block, ~line 157):

```rust
    writeln!(
        w,
        "  prefetch_enabled:         {}",
        resolved.prefetch.enabled
    )?;
```

- [ ] **Step 4: Add the template block.** In the `DEFAULT_CONFIG` string, before the closing `"#;` (after the `[observability]` block, ~line 228):

```toml

[prefetch]
# ADR 022 speculative-prefetch operator policy. Disabled by default.
# enabled = false
# require_authorized_origin = true          # require an authorized origin in the FIND_VALUE candidate set
# budget_usdc_per_hour = 0                  # micro-USDC rolling-1h spend cap; 0 = never prefetch
# find_value_threshold = 5                  # FIND_VALUE queries within the window that trip the trigger
# threshold_window_secs = 300               # rolling-window length for the trigger
# demand_quality_min_ratio = 0.1            # served/acquired auto-throttle floor
# demand_quality_window_secs = 3600         # rolling-window length for the demand-quality predicate
```

- [ ] **Step 5: Run tests to verify pass.**

Run: `cargo nextest run -p decdn config`
Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add crates/cli/src/commands/config.rs
git commit -m "feat(cli): surface [prefetch] in default config + validate summary (#650)"
```

---

## Task 3: `PopularityTracker` (FIND_VALUE popularity oracle)

**Files:**

- Create: `crates/node/src/prefetch/popularity.rs`
- Modify: `crates/node/src/prefetch/mod.rs` (created in this task as a stub `mod` file)
- Modify: `crates/node/src/lib.rs` (add `pub mod prefetch;`)

- [ ] **Step 1: Create the module skeleton.** Create `crates/node/src/prefetch/mod.rs`:

```rust
//! Speculative-prefetch operator policy (ADR 022 §Popularity Signals and
//! Market Dynamics). Off-by-default; the FIND_VALUE handler feeds the
//! [`popularity::PopularityTracker`] and, on a threshold-cross, consults the
//! [`decision::PrefetchPolicy`]. This crate slice decides and meters but does
//! not yet fire the real acquisition (see #650 follow-up).

pub mod popularity;
```

Add `pub mod prefetch;` to `crates/node/src/lib.rs` next to the existing `pub mod dht;` declaration (find with `grep -n "pub mod dht;" crates/node/src/lib.rs`).

- [ ] **Step 2: Write the failing tracker tests.** Create `crates/node/src/prefetch/popularity.rs` with ONLY the test module first (so it fails to compile against missing items), then implement in Step 4. Write:

```rust
#[cfg(test)]
mod tests {
    use super::{PopularityTracker, MAX_TRACKED_HASHES};

    fn h(b: u8) -> [u8; 32] {
        [b; 32]
    }

    #[test]
    fn below_threshold_no_trigger() {
        // threshold 3, window 300s.
        let mut t = PopularityTracker::new(300, 3, MAX_TRACKED_HASHES);
        assert!(!t.observe(&h(1), 0));
        assert!(!t.observe(&h(1), 1));
        assert_eq!(t.count(&h(1), 1), 2);
    }

    #[test]
    fn crossing_threshold_triggers() {
        let mut t = PopularityTracker::new(300, 3, MAX_TRACKED_HASHES);
        assert!(!t.observe(&h(1), 0));
        assert!(!t.observe(&h(1), 10));
        assert!(t.observe(&h(1), 20)); // third within window
    }

    #[test]
    fn stale_timestamps_age_out() {
        let mut t = PopularityTracker::new(100, 3, MAX_TRACKED_HASHES);
        assert!(!t.observe(&h(1), 0));
        assert!(!t.observe(&h(1), 50));
        // First two are now > 100s old; the third does not reach threshold 3.
        assert!(!t.observe(&h(1), 201));
        assert_eq!(t.count(&h(1), 201), 1);
    }

    #[test]
    fn separate_hashes_are_independent() {
        let mut t = PopularityTracker::new(300, 2, MAX_TRACKED_HASHES);
        assert!(!t.observe(&h(1), 0));
        assert!(!t.observe(&h(2), 0));
        assert!(t.observe(&h(1), 1));
    }

    #[test]
    fn evicts_least_recent_at_cap() {
        let mut t = PopularityTracker::new(1000, 2, 2);
        t.observe(&h(1), 0); // h1 newest = 0
        t.observe(&h(2), 5); // h2 newest = 5
        t.observe(&h(3), 10); // at cap (2) + new hash => evict h1 (oldest newest=0)
        assert_eq!(t.count(&h(1), 10), 0); // evicted
        assert_eq!(t.count(&h(2), 10), 1);
        assert_eq!(t.count(&h(3), 10), 1);
    }
}
```

- [ ] **Step 3: Run to verify failure.**

Run: `cargo nextest run -p decdn-node popularity::`
Expected: FAIL — `PopularityTracker` undefined / does not compile.

- [ ] **Step 4: Implement the tracker.** Prepend to `crates/node/src/prefetch/popularity.rs` (above the test module):

```rust
//! Responder-side FIND_VALUE popularity oracle (ADR 022 §"Prefetch Demand
//! Signal: DHT FIND_VALUE Query Frequency"). A node positioned close to hash
//! H in the Kademlia keyspace receives FIND_VALUE queries for H from the whole
//! network regardless of whether it holds H; the count within a rolling window
//! is the non-suppressible demand signal that MAY trip a speculative prefetch.
//!
//! Pure: reads no clock, performs no I/O. The caller supplies a monotonic
//! `now` (seconds) so tests are deterministic.

use std::collections::{HashMap, VecDeque};

/// 32-byte BLAKE3 content hash used as the tracker key.
pub type HashKey = [u8; 32];

/// Hard cap on the number of distinct hashes tracked at once (ADR 022 names
/// 10,000 for the per-hash demand map). Bounds memory; the least-recently-seen
/// hash is evicted when a brand-new hash arrives at the cap.
pub const MAX_TRACKED_HASHES: usize = 10_000;

/// Sliding-window per-hash FIND_VALUE query counter with a bounded number of
/// tracked hashes.
#[derive(Debug)]
pub struct PopularityTracker {
    /// FIND_VALUE arrival timestamps (seconds) per hash, oldest-first.
    windows: HashMap<HashKey, VecDeque<u64>>,
    /// Rolling-window length in seconds (`prefetch.threshold_window_secs`).
    window_secs: u64,
    /// Trigger threshold (`prefetch.find_value_threshold`).
    threshold: u32,
    /// Hard cap on the number of tracked hashes (`>= 1`).
    max_hashes: usize,
}

impl PopularityTracker {
    /// Construct a tracker. `max_hashes` is clamped to at least 1.
    #[must_use]
    pub fn new(window_secs: u64, threshold: u32, max_hashes: usize) -> Self {
        Self {
            windows: HashMap::new(),
            window_secs,
            threshold,
            max_hashes: max_hashes.max(1),
        }
    }

    /// Record a FIND_VALUE arrival for `hash` at `now` (seconds). Returns
    /// `true` iff the in-window count reached the trigger threshold.
    pub fn observe(&mut self, hash: &HashKey, now: u64) -> bool {
        if !self.windows.contains_key(hash) && self.windows.len() >= self.max_hashes {
            self.evict_least_recent();
        }
        let window = self.windows.entry(*hash).or_default();
        Self::prune(window, self.window_secs, now);
        window.push_back(now);
        u32::try_from(window.len()).unwrap_or(u32::MAX) >= self.threshold
    }

    /// Current in-window query count for `hash` at `now` (seconds), pruning
    /// stale timestamps as a side effect. `0` for an untracked hash.
    pub fn count(&mut self, hash: &HashKey, now: u64) -> u32 {
        let window_secs = self.window_secs;
        match self.windows.get_mut(hash) {
            Some(window) => {
                Self::prune(window, window_secs, now);
                u32::try_from(window.len()).unwrap_or(u32::MAX)
            }
            None => 0,
        }
    }

    /// Drop timestamps strictly older than the window (`age >= window_secs`).
    fn prune(window: &mut VecDeque<u64>, window_secs: u64, now: u64) {
        while let Some(front) = window.front() {
            if now.saturating_sub(*front) >= window_secs {
                window.pop_front();
            } else {
                break;
            }
        }
    }

    /// Evict the hash whose most-recent observation is the oldest. O(n) but
    /// only runs when a brand-new hash arrives at the cap.
    fn evict_least_recent(&mut self) {
        let victim = self
            .windows
            .iter()
            .map(|(k, v)| (*k, v.back().copied().unwrap_or(0)))
            .min_by_key(|(_, newest)| *newest)
            .map(|(k, _)| k);
        if let Some(k) = victim {
            self.windows.remove(&k);
        }
    }
}
```

Note: the `unwrap_or` calls are infallible-fallback (no panic) and satisfy the anti-panic lint. `window.back().copied().unwrap_or(0)` handles the never-empty deque defensively.

- [ ] **Step 5: Run to verify pass.**

Run: `cargo nextest run -p decdn-node popularity:: && cargo clippy -p decdn-node`
Expected: PASS, no clippy warnings.

- [ ] **Step 6: Commit.**

```bash
git add crates/node/src/prefetch/ crates/node/src/lib.rs
git commit -m "feat(node): FIND_VALUE popularity tracker for prefetch (#650)"
```

---

## Task 4: `PrefetchPolicy` decision engine + ledgers

**Files:**

- Create: `crates/node/src/prefetch/decision.rs`
- Modify: `crates/node/src/prefetch/mod.rs` (add `pub mod decision;`)

- [ ] **Step 1: Declare the submodule.** Add to `crates/node/src/prefetch/mod.rs`:

```rust
pub mod decision;
```

- [ ] **Step 2: Write the failing decision tests.** Create `crates/node/src/prefetch/decision.rs` with the test module first:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use decdn_common::config::ResolvedPrefetch;

    use super::{PrefetchDecision, PrefetchPolicy, SkipReason};
    use crate::dht::origin::{ConfigOriginDirectory, Hash, OriginDirectory};
    use crate::dht::routing::NodeId;

    fn hash() -> Hash {
        Hash::from_bytes([7u8; 32])
    }

    /// Directory that authorizes `hash()` -> one origin.
    fn authorized_dir() -> Arc<dyn OriginDirectory> {
        let mut m = std::collections::HashMap::new();
        m.insert(hash(), vec![NodeId::from_bytes([1u8; 32])]);
        Arc::new(ConfigOriginDirectory::new(m))
    }

    /// Directory that authorizes nothing.
    fn empty_dir() -> Arc<dyn OriginDirectory> {
        Arc::new(ConfigOriginDirectory::new(std::collections::HashMap::new()))
    }

    fn cfg(enabled: bool) -> ResolvedPrefetch {
        ResolvedPrefetch {
            enabled,
            require_authorized_origin: true,
            budget_usdc_per_hour: 1_000_000,
            find_value_threshold: 5,
            threshold_window_secs: 300,
            demand_quality_min_ratio: 0.1,
            demand_quality_window_secs: 3600,
        }
    }

    #[test]
    fn disabled_skips_before_origin_lookup() {
        let p = PrefetchPolicy::new(cfg(false));
        assert_eq!(p.decide(&hash(), &*empty_dir(), 0), PrefetchDecision::Skip(SkipReason::Disabled));
    }

    #[test]
    fn unauthorized_when_no_origin() {
        let p = PrefetchPolicy::new(cfg(true));
        assert_eq!(
            p.decide(&hash(), &*empty_dir(), 0),
            PrefetchDecision::Skip(SkipReason::Unauthorized)
        );
    }

    #[test]
    fn acquires_when_authorized_and_in_budget() {
        let p = PrefetchPolicy::new(cfg(true));
        assert_eq!(p.decide(&hash(), &*authorized_dir(), 0), PrefetchDecision::Acquire);
    }

    #[test]
    fn origin_gate_bypassed_when_disabled() {
        let mut c = cfg(true);
        c.require_authorized_origin = false;
        let p = PrefetchPolicy::new(c);
        // No authorized origin, but the gate is off => proceeds.
        assert_eq!(p.decide(&hash(), &*empty_dir(), 0), PrefetchDecision::Acquire);
    }

    #[test]
    fn budget_exhaustion_blocks_until_window_advances() {
        let mut c = cfg(true);
        c.budget_usdc_per_hour = 100;
        let p = PrefetchPolicy::new(c);
        p.record_acquisition(100, 1_000, 0); // spend hits the cap at t=0
        assert_eq!(
            p.decide(&hash(), &*authorized_dir(), 10),
            PrefetchDecision::Skip(SkipReason::BudgetExhausted)
        );
        // 1h (3600s) later the spend has aged out of the rolling window.
        assert_eq!(p.decide(&hash(), &*authorized_dir(), 3700), PrefetchDecision::Acquire);
    }

    #[test]
    fn zero_budget_always_exhausted() {
        let mut c = cfg(true);
        c.budget_usdc_per_hour = 0;
        let p = PrefetchPolicy::new(c);
        assert_eq!(
            p.decide(&hash(), &*authorized_dir(), 0),
            PrefetchDecision::Skip(SkipReason::BudgetExhausted)
        );
    }

    #[test]
    fn throttle_latches_below_ratio_and_clears_on_recovery() {
        let p = PrefetchPolicy::new(cfg(true));
        // Acquire 1000 bytes, serve only 50 => ratio 0.05 < 0.1 => throttle.
        p.record_acquisition(10, 1_000, 0);
        p.record_served(50, 0);
        assert_eq!(
            p.decide(&hash(), &*authorized_dir(), 1),
            PrefetchDecision::Skip(SkipReason::Throttled)
        );
        // Serve 200 more => 250/1000 = 0.25 >= 0.1 => recovers.
        p.record_served(200, 2);
        assert_eq!(p.decide(&hash(), &*authorized_dir(), 3), PrefetchDecision::Acquire);
    }

    #[test]
    fn no_acquisitions_means_no_throttle() {
        let p = PrefetchPolicy::new(cfg(true));
        // acquired == 0 => ratio undefined => not throttled.
        assert_eq!(p.decide(&hash(), &*authorized_dir(), 0), PrefetchDecision::Acquire);
        assert!(!p.throttle_active(0));
    }
}
```

- [ ] **Step 3: Run to verify failure.**

Run: `cargo nextest run -p decdn-node decision::`
Expected: FAIL — `PrefetchPolicy` undefined.

- [ ] **Step 4: Implement the decision engine.** Prepend to `crates/node/src/prefetch/decision.rs`:

```rust
//! Speculative-prefetch decision engine (ADR 022 §Prefetch Decision). Given a
//! hash whose FIND_VALUE demand crossed the trigger threshold, decide whether
//! to acquire it, applying the operator-policy gates in order:
//! enabled → demand-quality throttle → authorized-origin → budget.
//!
//! Pure: the caller injects the clock (`now`, seconds) and the
//! [`OriginDirectory`]. Ledger state lives behind a `std::sync::Mutex`; lock
//! poisoning fails closed (the decision becomes a skip).

use std::collections::VecDeque;
use std::sync::Mutex;

use decdn_common::config::ResolvedPrefetch;

use crate::dht::origin::{Hash, OriginDirectory};

/// Fixed rolling-window length for the spend budget: 1 hour, in seconds. ADR
/// 022 names the cap as `budget_usdc_per_hour`, so the window is not operator-
/// tunable (only the cap value is).
const BUDGET_WINDOW_SECS: u64 = 3600;

/// Outcome of a prefetch decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchDecision {
    /// Proceed to acquire the hash (the live acquisition is a #650 follow-up).
    Acquire,
    /// Do not acquire; carries the first gate that rejected.
    Skip(SkipReason),
}

/// Why a prefetch was skipped (gate-ordered).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// `prefetch.enabled == false`.
    Disabled,
    /// Demand-quality auto-throttle is latched active.
    Throttled,
    /// `require_authorized_origin` and no authorized origin for the hash.
    Unauthorized,
    /// Rolling-1h spend has reached `budget_usdc_per_hour`.
    BudgetExhausted,
}

/// Rolling-window ledgers + throttle latch.
#[derive(Debug, Default)]
struct Ledgers {
    /// (timestamp_secs, micro_usdc) prefetch spends, oldest-first.
    spend: VecDeque<(u64, u64)>,
    /// (timestamp_secs, bytes) acquired via prefetch, oldest-first.
    acquired: VecDeque<(u64, u64)>,
    /// (timestamp_secs, bytes) served from prefetched content, oldest-first.
    served: VecDeque<(u64, u64)>,
    /// Whether the demand-quality auto-throttle is currently latched active.
    throttled: bool,
}

/// Operator-policy prefetch decision engine.
#[derive(Debug)]
pub struct PrefetchPolicy {
    cfg: ResolvedPrefetch,
    ledgers: Mutex<Ledgers>,
}

impl PrefetchPolicy {
    /// Construct from resolved config.
    #[must_use]
    pub fn new(cfg: ResolvedPrefetch) -> Self {
        Self {
            cfg,
            ledgers: Mutex::new(Ledgers::default()),
        }
    }

    /// Decide whether to prefetch `hash` at `now` (seconds), consulting `dir`
    /// for the authorized-origin gate. Gates apply in order; the first to
    /// reject wins. Fails closed (`Skip(Throttled)` as a conservative stand-in)
    /// if the ledger lock is poisoned.
    #[must_use]
    pub fn decide(&self, hash: &Hash, dir: &dyn OriginDirectory, now: u64) -> PrefetchDecision {
        // Gate 1: master switch.
        if !self.cfg.enabled {
            return PrefetchDecision::Skip(SkipReason::Disabled);
        }

        // Gate 2: demand-quality throttle (also prunes ledgers for gates 4).
        let Ok(mut led) = self.ledgers.lock() else {
            tracing::error!("prefetch decide: ledger mutex poisoned; skipping");
            return PrefetchDecision::Skip(SkipReason::Throttled);
        };
        Self::prune(&mut led.spend, BUDGET_WINDOW_SECS, now);
        Self::prune(&mut led.acquired, self.cfg.demand_quality_window_secs, now);
        Self::prune(&mut led.served, self.cfg.demand_quality_window_secs, now);
        led.throttled = Self::compute_throttle(&led, self.cfg.demand_quality_min_ratio);
        if led.throttled {
            return PrefetchDecision::Skip(SkipReason::Throttled);
        }

        // Gate 3: authorized origin.
        if self.cfg.require_authorized_origin && dir.lookup_origins(hash).is_empty() {
            return PrefetchDecision::Skip(SkipReason::Unauthorized);
        }

        // Gate 4: rolling-1h budget.
        let spent: u64 = led.spend.iter().map(|(_, v)| *v).sum();
        if spent >= self.cfg.budget_usdc_per_hour {
            return PrefetchDecision::Skip(SkipReason::BudgetExhausted);
        }

        PrefetchDecision::Acquire
    }

    /// Record a completed prefetch acquisition (drives budget + demand-quality
    /// denominator). Exercised by unit tests this slice; wired to the live
    /// acquisition path in the #650 follow-up.
    pub fn record_acquisition(&self, micro_usdc: u64, bytes: u64, now: u64) {
        if let Ok(mut led) = self.ledgers.lock() {
            led.spend.push_back((now, micro_usdc));
            led.acquired.push_back((now, bytes));
        } else {
            tracing::error!("prefetch record_acquisition: ledger mutex poisoned");
        }
    }

    /// Record bytes served from prefetched content (demand-quality numerator).
    pub fn record_served(&self, bytes: u64, now: u64) {
        if let Ok(mut led) = self.ledgers.lock() {
            led.served.push_back((now, bytes));
        } else {
            tracing::error!("prefetch record_served: ledger mutex poisoned");
        }
    }

    /// Current rolling-window `served / acquired` ratio at `now`. Returns
    /// `1.0` (healthy) when nothing has been acquired yet.
    #[must_use]
    pub fn demand_quality_ratio(&self, now: u64) -> f64 {
        let Ok(mut led) = self.ledgers.lock() else {
            return 1.0;
        };
        Self::prune(&mut led.acquired, self.cfg.demand_quality_window_secs, now);
        Self::prune(&mut led.served, self.cfg.demand_quality_window_secs, now);
        Self::ratio(&led)
    }

    /// Whether the demand-quality throttle is latched active at `now`.
    #[must_use]
    pub fn throttle_active(&self, now: u64) -> bool {
        let Ok(mut led) = self.ledgers.lock() else {
            return true; // fail closed
        };
        Self::prune(&mut led.acquired, self.cfg.demand_quality_window_secs, now);
        Self::prune(&mut led.served, self.cfg.demand_quality_window_secs, now);
        Self::compute_throttle(&led, self.cfg.demand_quality_min_ratio)
    }

    /// `served / acquired` over the current ledger; `1.0` when `acquired == 0`.
    fn ratio(led: &Ledgers) -> f64 {
        let acquired: u64 = led.acquired.iter().map(|(_, v)| *v).sum();
        if acquired == 0 {
            return 1.0;
        }
        let served: u64 = led.served.iter().map(|(_, v)| *v).sum();
        // u64 -> f64 is lossy only past 2^53 bytes (~9 PB); irrelevant here.
        served as f64 / acquired as f64
    }

    /// Throttle active iff at least one acquisition exists and the ratio is
    /// below the floor.
    fn compute_throttle(led: &Ledgers, min_ratio: f64) -> bool {
        let acquired: u64 = led.acquired.iter().map(|(_, v)| *v).sum();
        acquired > 0 && Self::ratio(led) < min_ratio
    }

    /// Drop `(ts, _)` entries strictly older than `window_secs` (`age >= window`).
    fn prune(q: &mut VecDeque<(u64, u64)>, window_secs: u64, now: u64) {
        while let Some((ts, _)) = q.front() {
            if now.saturating_sub(*ts) >= window_secs {
                q.pop_front();
            } else {
                break;
            }
        }
    }
}
```

- [ ] **Step 5: Run to verify pass.**

Run: `cargo nextest run -p decdn-node decision:: && cargo clippy -p decdn-node`
Expected: PASS, no clippy warnings. (If clippy flags `cast_precision_loss` on `as f64`, the inline comment plus the fact it is already a workspace-allowed pattern should suffice; if it errors, add `#[allow(clippy::cast_precision_loss)]` with the same justifying comment.)

- [ ] **Step 6: Commit.**

```bash
git add crates/node/src/prefetch/decision.rs crates/node/src/prefetch/mod.rs
git commit -m "feat(node): prefetch decision engine with origin/budget/quality gates (#650)"
```

---

## Task 5: `PrefetchEngine` façade

**Files:**

- Modify: `crates/node/src/prefetch/mod.rs` (add the `PrefetchEngine` struct)

- [ ] **Step 1: Write the failing façade test.** Add to `crates/node/src/prefetch/mod.rs` a test module:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use decdn_common::config::ResolvedPrefetch;

    use super::{PrefetchEngine, PrefetchOutcome};
    use crate::dht::origin::{ConfigOriginDirectory, Hash, NodeId, OriginDirectory};

    fn dir_with(hash: Hash) -> Arc<dyn OriginDirectory> {
        let mut m = std::collections::HashMap::new();
        m.insert(hash, vec![NodeId::from_bytes([1u8; 32])]);
        Arc::new(ConfigOriginDirectory::new(m))
    }

    #[test]
    fn disabled_engine_never_triggers() {
        let cfg = ResolvedPrefetch::default(); // enabled = false
        let key = [9u8; 32];
        let dir: Arc<dyn OriginDirectory> = Arc::new(ConfigOriginDirectory::new(Default::default()));
        let engine = PrefetchEngine::new(cfg, dir);
        for t in 0..10 {
            assert_eq!(engine.on_find_value(&key, t), PrefetchOutcome::Inert);
        }
    }

    #[test]
    fn enabled_engine_triggers_and_decides() {
        let mut cfg = ResolvedPrefetch::default();
        cfg.enabled = true;
        cfg.find_value_threshold = 3;
        cfg.budget_usdc_per_hour = 1_000_000;
        let key = [9u8; 32];
        let dir = dir_with(Hash::from_bytes(key));
        let engine = PrefetchEngine::new(cfg, dir);
        assert_eq!(engine.on_find_value(&key, 0), PrefetchOutcome::BelowThreshold);
        assert_eq!(engine.on_find_value(&key, 1), PrefetchOutcome::BelowThreshold);
        assert_eq!(engine.on_find_value(&key, 2), PrefetchOutcome::Decided(super::decision::PrefetchDecision::Acquire));
    }
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `cargo nextest run -p decdn-node prefetch::tests`
Expected: FAIL — `PrefetchEngine` undefined.

- [ ] **Step 3: Implement the façade.** Add to `crates/node/src/prefetch/mod.rs` (below the `pub mod` lines, above the test module). Re-export `NodeId` through `origin` if the test path needs it — confirm `crate::dht::origin` re-exports `NodeId`; if not, the test should import `crate::dht::routing::NodeId` instead (adjust the test import accordingly):

```rust
use std::sync::Mutex;
use std::sync::Arc;

use decdn_common::config::ResolvedPrefetch;

use crate::dht::origin::{Hash, OriginDirectory};
use self::decision::{PrefetchDecision, PrefetchPolicy};
use self::popularity::{PopularityTracker, MAX_TRACKED_HASHES};

/// Result of feeding one FIND_VALUE arrival to the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchOutcome {
    /// Prefetch is disabled; nothing happened.
    Inert,
    /// Observed, but the demand threshold was not reached.
    BelowThreshold,
    /// Threshold reached; the decision engine produced this outcome.
    Decided(PrefetchDecision),
}

/// Ties the popularity tracker + decision engine + origin directory together.
/// The DHT FIND_VALUE handler calls [`PrefetchEngine::on_find_value`] for every
/// inbound request; with `enabled == false` it is inert.
#[derive(Debug)]
pub struct PrefetchEngine {
    cfg: ResolvedPrefetch,
    tracker: Mutex<PopularityTracker>,
    policy: PrefetchPolicy,
    directory: Arc<dyn OriginDirectory>,
}

impl PrefetchEngine {
    /// Construct from resolved config and the origin directory used for the
    /// authorized-origin gate.
    #[must_use]
    pub fn new(cfg: ResolvedPrefetch, directory: Arc<dyn OriginDirectory>) -> Self {
        let tracker = PopularityTracker::new(
            cfg.threshold_window_secs,
            cfg.find_value_threshold,
            MAX_TRACKED_HASHES,
        );
        Self {
            cfg,
            tracker: Mutex::new(tracker),
            policy: PrefetchPolicy::new(cfg),
            directory,
        }
    }

    /// Whether prefetch is enabled (drives the `decdn_prefetch_enabled` gauge).
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.cfg.enabled
    }

    /// Borrow the decision engine (metrics readers + the follow-up acquisition
    /// path feed it via `record_*`).
    #[must_use]
    pub const fn policy(&self) -> &PrefetchPolicy {
        &self.policy
    }

    /// Feed one FIND_VALUE arrival for `hash_bytes` at `now` (seconds). Records
    /// the demand signal and, on a threshold-cross, runs the decision engine.
    /// Returns the outcome for metrics/logging. Does NOT perform any network
    /// acquisition (that is the #650 follow-up).
    pub fn on_find_value(&self, hash_bytes: &[u8; 32], now: u64) -> PrefetchOutcome {
        if !self.cfg.enabled {
            return PrefetchOutcome::Inert;
        }
        let triggered = match self.tracker.lock() {
            Ok(mut t) => t.observe(hash_bytes, now),
            Err(_) => {
                tracing::error!("prefetch on_find_value: tracker mutex poisoned");
                return PrefetchOutcome::Inert;
            }
        };
        if !triggered {
            return PrefetchOutcome::BelowThreshold;
        }
        let hash = Hash::from_bytes(*hash_bytes);
        let decision = self.policy.decide(&hash, &*self.directory, now);
        PrefetchOutcome::Decided(decision)
    }
}
```

If `crate::dht::origin` does not re-export `NodeId`, change the Step-1 test import to `use crate::dht::routing::NodeId;`.

- [ ] **Step 4: Run to verify pass.**

Run: `cargo nextest run -p decdn-node prefetch:: && cargo clippy -p decdn-node`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add crates/node/src/prefetch/mod.rs
git commit -m "feat(node): PrefetchEngine façade tying tracker, policy, origin gate (#650)"
```

---

## Task 6: Prefetch metrics

**Files:**

- Modify: `crates/node/src/metrics.rs` (add fields to `DecdnMetrics` + helper methods on `Metrics`)

Note on naming: `iroh_metrics` has no label support, so the appendix's
`decdn_prefetch_acquisitions_total{gate_result=…}` is realized as three
counters — the same per-counter precedent as `dht_rate_limit_rejected_{per_peer,
per_ip,global}`. Task 9 reconciles the appendix table.

- [ ] **Step 1: Add metric fields.** In `DecdnMetrics` (after the DHT-related fields, e.g. after `dht_store_accepted`/`staker_set_active_count`), add:

```rust
    /// `1` if `prefetch.enabled`, else `0` (ADR 022 §Prefetch; appendix-
    /// observability §Prefetch Metrics). Stable schema across nodes.
    pub prefetch_enabled: Gauge,
    /// Prefetch acquisitions that passed the authorized-origin gate.
    /// Operator-visible: `decdn_prefetch_acquisitions_authorized_total`.
    pub prefetch_acquisitions_authorized: Counter,
    /// Prefetch attempts rejected by the authorized-origin gate.
    /// `decdn_prefetch_acquisitions_unauthorized_total`.
    pub prefetch_acquisitions_unauthorized: Counter,
    /// Prefetch acquisitions where the origin gate was disabled (bypassed).
    /// `decdn_prefetch_acquisitions_bypassed_total`.
    pub prefetch_acquisitions_bypassed: Counter,
    /// Cumulative micro-USDC paid for prefetch acquisitions (0 until the
    /// #650 follow-up wires real acquisition). `decdn_prefetch_spend_usdc_total`.
    pub prefetch_spend_usdc: Counter,
    /// Times the rolling-1h prefetch budget was hit.
    /// `decdn_prefetch_budget_exhaustion_events_total`.
    pub prefetch_budget_exhaustion_events: Counter,
    /// Prefetch attempts skipped by the origin gate (== acquisitions_unauthorized).
    /// `decdn_prefetch_origin_gate_rejections_total`.
    pub prefetch_origin_gate_rejections: Counter,
    /// Current rolling-window served/acquired ratio (×1000, integer gauge).
    /// `decdn_prefetch_demand_quality_ratio_milli`.
    pub prefetch_demand_quality_ratio_milli: Gauge,
    /// `1` while the demand-quality auto-throttle suppresses prefetch.
    /// `decdn_prefetch_throttle_active`.
    pub prefetch_throttle_active: Gauge,
}
```

(`Gauge` is integer-valued in `iroh_metrics`, so the ratio gauge is scaled ×1000 to `_milli`; note this in Task 9's appendix update.)

- [ ] **Step 2: Add helper methods.** Find the `impl Metrics` block (~line 395) and add methods mirroring the existing style (e.g. `probe_request`). The `DecdnMetrics` group is reached via `self.decdn`:

```rust
    /// Set the `decdn_prefetch_enabled` gauge once at startup.
    pub fn set_prefetch_enabled(&self, enabled: bool) {
        self.decdn.prefetch_enabled.set(i64::from(enabled));
    }

    /// Record the outcome of a prefetch decision against the gate counters.
    pub fn record_prefetch_decision(&self, outcome: crate::prefetch::PrefetchOutcome) {
        use crate::prefetch::PrefetchOutcome;
        use crate::prefetch::decision::{PrefetchDecision, SkipReason};
        if let PrefetchOutcome::Decided(decision) = outcome {
            match decision {
                PrefetchDecision::Acquire => {
                    // Distinguished authorized vs bypassed by the caller via
                    // `prefetch_acquire_authorized` / `_bypassed` below.
                }
                PrefetchDecision::Skip(SkipReason::Unauthorized) => {
                    self.decdn.prefetch_acquisitions_unauthorized.inc();
                    self.decdn.prefetch_origin_gate_rejections.inc();
                }
                PrefetchDecision::Skip(SkipReason::BudgetExhausted) => {
                    self.decdn.prefetch_budget_exhaustion_events.inc();
                }
                PrefetchDecision::Skip(SkipReason::Disabled | SkipReason::Throttled) => {}
            }
        }
    }

    /// Record a would-acquire decision, split by whether the origin gate was
    /// applied (`authorized`) or disabled (`bypassed`).
    pub fn record_prefetch_acquire(&self, gate_applied: bool) {
        if gate_applied {
            self.decdn.prefetch_acquisitions_authorized.inc();
        } else {
            self.decdn.prefetch_acquisitions_bypassed.inc();
        }
    }

    /// Refresh the demand-quality gauges from the policy state.
    pub fn set_prefetch_quality(&self, ratio: f64, throttled: bool) {
        let milli = (ratio * 1000.0).round();
        let milli = if milli.is_finite() && milli >= 0.0 {
            milli.min(i64::MAX as f64) as i64
        } else {
            0
        };
        self.decdn.prefetch_demand_quality_ratio_milli.set(milli);
        self.decdn.prefetch_throttle_active.set(i64::from(throttled));
    }
```

(If `Gauge::set` takes `i64`/`u64` differently in this `iroh_metrics` version, match the signature already used by `uptime_seconds.set(0)` and `dht_rate_limit_tracked_per_ip`. Adjust `i64::from`/`as i64` to the expected type.)

- [ ] **Step 3: Build to verify it compiles.**

Run: `cargo build -p decdn-node && cargo clippy -p decdn-node`
Expected: PASS. (`record_prefetch_decision`/`record_prefetch_acquire`/`set_*` may be flagged dead-code until Task 7 calls them — that is expected; Task 7 follows immediately. If clippy's `dead_code` blocks the build, proceed to Task 7 before re-running clippy, or add a temporary `#[allow(dead_code)]` removed in Task 7.)

- [ ] **Step 4: Commit.**

```bash
git add crates/node/src/metrics.rs
git commit -m "feat(node): prefetch metric counters/gauges per appendix-observability (#650)"
```

---

## Task 7: Wire the engine into the FIND_VALUE handler

**Files:**

- Modify: `crates/node/src/handlers/dht.rs` (`DhtHandler` field, `with_prefetch` builder, `handle_find_value`)

- [ ] **Step 1: Add the optional engine field + builder.** In `DhtHandler` (struct at line 70), add a field after `metrics: Arc<Metrics>,`:

```rust
    /// Operator-policy prefetch engine (ADR 022 §Prefetch). `None` => prefetch
    /// wiring absent (tests, and the default until the runtime attaches one).
    prefetch: Option<Arc<crate::prefetch::PrefetchEngine>>,
```

Initialize `prefetch: None` in BOTH constructors (`new` at line 116 and `with_routing` at line 141 — add `prefetch: None,` to each struct literal). Then add a builder method in `impl DhtHandler`:

```rust
    /// Attach the prefetch engine. Called by the runtime; left `None` in tests
    /// and bootstrap paths that do not exercise prefetch.
    #[must_use]
    pub fn with_prefetch(mut self, engine: Arc<crate::prefetch::PrefetchEngine>) -> Self {
        self.prefetch = Some(engine);
        self
    }
```

- [ ] **Step 2: Feed the engine from `handle_find_value`.** Replace the body of `handle_find_value` (lines 582-598) so it observes demand before building the response. Keep the existing response construction unchanged:

```rust
    fn handle_find_value(&self, req: wire::FindValueRequest) -> wire::FindValueResponse {
        let now_us = now_us();
        if let Some(engine) = &self.prefetch {
            self.run_prefetch(engine, req.hash.as_bytes(), now_us / 1_000_000);
        }
        let providers = if let Ok(mut store) = self.records.lock() {
            store.providers_at(&req.hash, now_us)
        } else {
            tracing::error!("dht FindValue: record-store mutex poisoned; returning empty");
            Vec::new()
        };
        wire::FindValueResponse {
            hash: req.hash,
            providers,
            closer_nodes: self.closest_to(req.hash.as_bytes()),
        }
    }

    /// Feed the FIND_VALUE demand signal to the prefetch engine and translate
    /// the decision into metrics. Decides and meters only — the live
    /// acquisition is the #650 follow-up.
    fn run_prefetch(
        &self,
        engine: &Arc<crate::prefetch::PrefetchEngine>,
        hash_bytes: &[u8; 32],
        now_secs: u64,
    ) {
        use crate::prefetch::PrefetchOutcome;
        use crate::prefetch::decision::PrefetchDecision;
        let outcome = engine.on_find_value(hash_bytes, now_secs);
        self.metrics.record_prefetch_decision(outcome);
        if let PrefetchOutcome::Decided(PrefetchDecision::Acquire) = outcome {
            let gate_applied = engine.policy_requires_origin();
            self.metrics.record_prefetch_acquire(gate_applied);
            tracing::debug!(
                hash = %hex_short(hash_bytes),
                "prefetch: demand threshold crossed; acquisition deferred to #650 follow-up"
            );
        }
        self.metrics.set_prefetch_quality(
            engine.policy().demand_quality_ratio(now_secs),
            engine.policy().throttle_active(now_secs),
        );
    }
```

This references two helpers: `engine.policy_requires_origin()` and `hex_short`. Add `policy_requires_origin` to `PrefetchEngine` in `crates/node/src/prefetch/mod.rs`:

```rust
    /// Whether the authorized-origin gate is active (drives the
    /// authorized-vs-bypassed metric split).
    #[must_use]
    pub const fn policy_requires_origin(&self) -> bool {
        self.cfg.require_authorized_origin
    }
```

For `hex_short`, reuse the handler module's existing short-hash formatting if one exists (`grep -n "fn hex_short\|fn short_hash\|hex::encode" crates/node/src/handlers/dht.rs`). If none exists, inline a minimal non-panicking formatter at module scope in `handlers/dht.rs`:

```rust
/// First 4 bytes of a 32-byte hash as hex, for log lines.
fn hex_short(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(8);
    for b in bytes.iter().take(4) {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}
```

- [ ] **Step 3: Add a handler-level test.** Add to the `#[cfg(test)] mod tests` in `crates/node/src/handlers/dht.rs` (mirror an existing test that constructs a `DhtHandler`; reuse its helpers for `Metrics`, `DhtRateLimiter`, `ConnectionLimiter`, `StakerSet`, `RecordStore`). The test asserts that an enabled engine flips `decdn_prefetch_enabled` and increments an acquisition counter after the threshold is crossed:

```rust
    #[test]
    fn find_value_feeds_prefetch_engine() {
        use std::sync::Arc;
        use decdn_common::config::ResolvedPrefetch;
        use crate::dht::origin::{ConfigOriginDirectory, Hash, OriginDirectory};
        use crate::dht::routing::NodeId;
        use crate::prefetch::PrefetchEngine;

        // Build a handler via the existing test helper (adapt name as needed).
        let handler = test_handler(); // existing helper that returns a DhtHandler

        let target = [0xABu8; 32];
        let mut origins = std::collections::HashMap::new();
        origins.insert(Hash::from_bytes(target), vec![NodeId::from_bytes([1u8; 32])]);
        let dir: Arc<dyn OriginDirectory> = Arc::new(ConfigOriginDirectory::new(origins));

        let mut cfg = ResolvedPrefetch::default();
        cfg.enabled = true;
        cfg.find_value_threshold = 2;
        cfg.budget_usdc_per_hour = 1_000_000;
        let engine = Arc::new(PrefetchEngine::new(cfg, dir));
        let handler = handler.with_prefetch(Arc::clone(&engine));

        let req = wire::FindValueRequest { hash: wire::ContentHash::from_bytes(target) };
        let _ = handler.handle_find_value(req.clone());
        let _ = handler.handle_find_value(req); // second crosses threshold 2 => Acquire
        // authorized acquisition counter incremented exactly once.
        // (Assert via the metrics text scrape used by the rate-limit tests, or
        // expose a typed accessor; mirror whatever the existing dht metrics
        // tests use — e.g. `render_metrics(&handler.metrics)`.)
    }
```

Adapt the construction to the file's existing test helpers (the rate-limit tests at the bottom of `dht.rs` already build a `DhtHandler` and scrape `/metrics` text — copy that exact pattern, asserting the rendered text contains `decdn_prefetch_acquisitions_authorized_total 1`). If `wire::ContentHash` lacks `from_bytes`/`Clone`, mirror the `ch(0x11)` helper in `crates/protocol/src/dht.rs` tests for constructing one.

- [ ] **Step 4: Run handler tests + clippy.**

Run: `cargo nextest run -p decdn-node handlers::dht && cargo clippy -p decdn-node`
Expected: PASS. (The Task 6 dead-code concern is now resolved — the metrics helpers are called.)

- [ ] **Step 5: Commit.**

```bash
git add crates/node/src/handlers/dht.rs crates/node/src/prefetch/mod.rs
git commit -m "feat(node): feed FIND_VALUE demand to prefetch engine + meter decisions (#650)"
```

---

## Task 8: Construct + attach the engine in the runtime

**Files:**

- Modify: `crates/node/src/runtime/mod.rs` (build `PrefetchEngine`, set the enabled gauge, attach via `with_prefetch`)

- [ ] **Step 1: Locate the DHT handler construction.** Run:

```bash
grep -n "DhtHandler::new\|DhtHandler::with_routing\|ConfigOriginDirectory\|OriginDirectory\|resolved\.dht\|\.prefetch" crates/node/src/runtime/mod.rs
```

Identify where `DhtHandler` is built and where the origin directory for the iterative lookup is constructed (there should already be an `Arc<dyn OriginDirectory>` — likely a `ConfigOriginDirectory` from `resolved` static origins). If the runtime does not yet build a directory, construct an empty one: `Arc::new(ConfigOriginDirectory::new(Default::default()))` (the engine still meters; the gate simply rejects everything until the follow-up wires a populated directory).

- [ ] **Step 2: Build + attach the engine.** Immediately after the `DhtHandler` is constructed, wrap it:

```rust
    let prefetch_engine = std::sync::Arc::new(crate::prefetch::PrefetchEngine::new(
        resolved.prefetch,
        origin_directory.clone(), // the Arc<dyn OriginDirectory> already in scope
    ));
    metrics.set_prefetch_enabled(prefetch_engine.enabled());
    let dht_handler = dht_handler.with_prefetch(prefetch_engine);
```

Adjust binding names to the runtime's locals (`metrics` is the `Arc<Metrics>` already threaded into `DhtHandler::new`; `origin_directory` is whatever the lookup path uses — reuse that exact `Arc`).

- [ ] **Step 3: Verify the node still boots in tests.**

Run: `cargo nextest run -p decdn-node && cargo build -p decdn-node`
Expected: PASS — runtime construction compiles and existing runtime/integration tests stay green.

- [ ] **Step 4: Commit.**

```bash
git add crates/node/src/runtime/mod.rs
git commit -m "feat(node): construct + attach prefetch engine in runtime bring-up (#650)"
```

---

## Task 9: Docs reconciliation + full verification + follow-up

**Files:**

- Modify: `adr/appendix-observability.md` (§Prefetch Metrics — per-counter realization note)

- [ ] **Step 1: Reconcile the metrics appendix.** In `adr/appendix-observability.md` §Prefetch Metrics, add a sentence under the table noting the per-counter realization (no labels in `iroh_metrics`), and adjust the metric names to the implemented ones:
  - `decdn_prefetch_acquisitions_total{gate_result=authorized|unauthorized|bypassed}` → realized as `decdn_prefetch_acquisitions_{authorized,unauthorized,bypassed}_total` (same precedent as `decdn_dht_rate_limit_rejected_{per_peer,per_ip,global}_total`).
  - `decdn_prefetch_demand_quality_ratio` (Gauge) → `decdn_prefetch_demand_quality_ratio_milli` (integer gauge ×1000, since `iroh_metrics::Gauge` is integer-valued).

  Keep the edit minimal — a parenthetical + the renamed rows. Run the ADR hygiene hook afterward (`pre-commit run --all-files` covers `adr reference hygiene`).

- [ ] **Step 2: Full local CI parity.**

Run:

```bash
cargo fmt -- --check
cargo clippy --workspace --all-targets
cargo nextest run --workspace
cargo deny check
```

Expected: all PASS. Fix any `unwrap_used`/`expect_used`/`indexing_slicing`/`max_width` findings in the new code.

- [ ] **Step 3: Run the pre-commit hooks.**

Run: `pre-commit run --all-files`
Expected: PASS (markdownlint on the new docs; cargo-fmt/clippy/doc). Re-stage any auto-fixes and re-run.

- [ ] **Step 4: Note the stale "Signal 2" on the issue + file the follow-up.** This is done by the orchestrator outside the plan (it needs `gh`): comment on #650 that local-cache-miss "Signal 2" is superseded by ADR 037 (implemented Signal 1 only), and open the follow-up issue for AC 7 (live `FindValue→probe→pull-through` acquisition, `OriginAssignment` event subscription, byte-feedback into the ledgers).

- [ ] **Step 5: Final commit (if any doc/lint fixups remain uncommitted).**

```bash
git add -A
git commit -m "docs(observability): reconcile prefetch metric names to per-counter realization (#650)"
```

---

## Self-review notes (spec coverage)

- **FIND_VALUE popularity signal (ADR 022, AC 8):** Task 3 (`PopularityTracker`), wired live in Task 7.
- **Prefetch decision gates — enabled/origin/budget/quality (ADR 022 table):** Task 4 (`PrefetchPolicy`).
- **`[prefetch]` config block + defaults + validation:** Task 1; surfaced in Task 2.
- **7 prefetch metrics, exposed regardless of `enabled`:** Task 6; gauge set in Task 8.
- **"Decide and meter, don't act" boundary:** Tasks 7–8 (no acquisition fired).
- **Out of scope (follow-up):** real acquisition, `OriginAssignment` subscription, live byte-feedback — Task 9 Step 4.
- **Doc/SoT consistency:** Task 9 Step 1 reconciles the appendix to the implemented metric names.
