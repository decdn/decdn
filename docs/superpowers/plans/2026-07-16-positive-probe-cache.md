# Lean Positive Probe Cache Implementation Plan (#1165)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the ADR 001 §Probe cache positive cache — a 15s-TTL LRU `hash → Vec<(NodeId, rate_per_mb, rtt)>` checked before the DHT lookup — so a repeat cache miss for a popular hash skips both the DHT FIND_VALUE and the probe fanout.

**Architecture:** A new `PositiveProbeCache` in `crates/node/src/dht/probe_cache.rs`, modelled line-for-line on its existing negative twin (`dht/negative_cache.rs`): hand-rolled LRU over `indexmap::IndexMap`, `std::sync::Mutex<Inner>`, `Instant` expiry anchored at insert. It is written at the tail of `probe_and_rank` (the one chokepoint both pull paths share) and read before `discover` in both `Origin::fetch` and `open_progressive_pull`. It stores **only** the ADR triple — never the signed `ProbeResponse`, never `reputation`.

**Tech Stack:** Rust edition 2024, `indexmap` (already a node dep), `iroh_metrics`, `cargo nextest`.

**Branch:** `feat/1165-positive-probe-cache` — branched off `main`, in this worktree. (This worktree currently sits on `claude/issue-1165-fix-plan-c09852`; step 0 is `git checkout -b feat/1165-positive-probe-cache`.)

**Plan file:** commit a copy to `docs/superpowers/plans/2026-07-16-positive-probe-cache.md` as the branch's first commit.

## Context

**Why:** Only the *negative* probe cache exists today. Every repeated miss for a popular hash re-runs a full DHT FIND_VALUE plus probe fanout (`crates/node/src/node_origin.rs:799` and `:399`). ADR 001 §Probe cache has specified the positive cache since the ADR was accepted; it was simply never built.

**The spec already exists and needs no edit.** `adr/001-network.md:93` specifies this feature verbatim, including every number in the issue (15s TTL, 1024 entries, ≤10 per hash). The issue's "see ADR-001 edit issue" refers to commit `1c8a846` (#1200, *"trim probe machinery"*), which is **already merged**: it removed the slashing-evidence-retention clause while leaving the positive-cache spec intact. Nothing blocks this work, and no ADR prose changes — only the observability appendix gains rows.

**"No evidence retention" is a security property, not a memory optimisation.** Before #1200 the ADR had the cache holding `Vec<(NodeId, rate_per_mb, rtt, ProbeResponse)>`. A `ProbeResponse` carries `slash_sig` — a peer's EIP-712-signed `has_blob: true`, which is on-chain phantom-announcement slash evidence for `PROBE_SLASH_WINDOW` (`adr/005-protocol.md:56`). A structure that survives one request to speed up the next has no business holding another node's slashable statements. Store the triple; drop the signature.

**Three decisions the user has settled** (do not re-open):
1. **Shared attempt budget.** ADR 001 says that when every cached provider fails, the node runs a fresh DHT lookup + probe. Given each phase its own `MAX_PROVIDER_ATTEMPTS`, one fetch could cost six sequential pulls against an `outer_pull_deadline` (`selection.rs:127`) sized for three — 172s at defaults. That deadline is enforced *outside* this path, so nothing would fail loudly; the fetch would just be killed mid-pull by a timeout sized for a world it no longer lived in. That is the #859 starvation the arithmetic exists to prevent. So: **one budget for the whole fetch**, cached candidates first, cold path continues on the remainder. Worst case is unchanged, and `outer_pull_deadline` needs no edit.
2. **Metrics in scope.** Hit/miss counters, plus wiring the ADR-mandated `decdn_probe_post_eviction_failures_total`, which `adr/appendix-observability.md:86` specifies but nothing emits — `monitoring/grafana-dashboard.json:411` has been scraping a dead panel.
3. **`PROBE_TIMEOUT` is out of scope.** `selection.rs:25` is `5s`; ADR 001 §Probe response collection and #1200 both mandate `500ms`. Commit `deaab0f` (#1145) appears to have reverted it while relocating the constant. File a separate issue (Task 0) — fixing it shifts `PULL_THROUGH_OUTER_SLACK` and every derived deadline, which deserves its own review.

## Global Constraints

- **Anti-panic policy:** clippy denies `unwrap_used`, `expect_used`, `panic`, `indexing_slicing` workspace-wide. Test modules carry the `#[allow(...)]` block copied verbatim from `negative_cache.rs:200-206`.
- **CI clippy is `cargo clippy --workspace --all-targets -- -D warnings`** — it lints test code. Run the exact command before pushing.
- `rustfmt.toml` sets `max_width = 100`.
- **Add no new dependency.** The node crate has no `lru`/`moka`/`dashmap`; `negative_cache` hand-rolls LRU over `indexmap` and documents why (`negative_cache.rs:43-48`). Follow it.
- **Constants, not config** — matching the negative cache. The TTL must be *derived* from `PROBE_SLASH_WINDOW`, never a literal `15s` (`adr/005-protocol.md:62`).
- **On macOS run `cargo nextest run --no-fail-fast`** (a case-insensitive-APFS `bundle_create` failure fail-fasts the workspace run — issue #697).

## File Structure

| File | Responsibility |
|---|---|
| `crates/node/src/dht/probe_cache.rs` (new) | The `PositiveProbeCache` type: LRU, TTL, ≤10-per-hash truncation. No knowledge of candidates or selection. |
| `crates/node/src/dht/mod.rs` | `pub mod probe_cache;` + `pub use probe_cache::{PositiveProbeCache, ProbedProvider};` |
| `crates/node/src/node_origin.rs` | Deps field, the read hook in both pull paths, the write hook, the attempts budget, the `EvictedSinceProbe` metric fire. |
| `crates/node/src/metrics.rs` | Three `Counter` fields + helpers. |
| `crates/node/src/runtime/mod.rs` | One line of bring-up wiring (`:1710`, beside `negative_cache`). |
| `crates/node/tests/node_origin_pull.rs` | 4 builders + a new dual-cache test seam + integration tests. |
| `adr/appendix-observability.md` | Two Metric Registry rows. |

---

## Task 0: Branch, land the plan, file the `PROBE_TIMEOUT` follow-up

- [ ] **Step 1: Branch off main and commit the plan**

```bash
git checkout -b feat/1165-positive-probe-cache
mkdir -p docs/superpowers/plans
# copy this plan to docs/superpowers/plans/2026-07-16-positive-probe-cache.md
git add docs/superpowers/plans/2026-07-16-positive-probe-cache.md
git commit -m "docs(plans): positive probe cache implementation plan (#1165)"
```

Verify the base is real `main`, not a stale worktree HEAD: `git rev-parse HEAD` should match `git rev-parse origin/main` at branch time.

- [ ] **Step 2: File the follow-up issue** (inline `--body`, no heredoc)

```bash
gh issue create --repo decdn/decdn --title "PROBE_TIMEOUT regressed 500ms -> 5s, contradicting ADR 001 §Probe response collection" --label technical,rust --body "\`crates/node/src/selection.rs:25\` declares \`pub const PROBE_TIMEOUT: Duration = Duration::from_secs(5);\`.

ADR 001 §Probe response collection (\`adr/001-network.md:103\`) mandates a **500ms** probe-collection ceiling, and commit \`1c8a846\` (#1200) set it accordingly (\"The node-origin probe timeout drops from 5s to 500ms to match\").

Commit \`deaab0f\` (#1145) landed after #1200 and, while relocating the constant from \`node_origin.rs\` to \`selection.rs\` \"beside the deadline arithmetic\", reinstated the pre-trim 5s value and its pre-trim doc comment.

This is a 10x violation of the cold-miss latency ceiling. It also propagates: \`selection.rs:36\` derives \`PULL_THROUGH_OUTER_SLACK = PROBE_TIMEOUT + DEFAULT_ROUND_TIMEOUT * MAX_LOOKUP_ROUNDS\`, which feeds \`outer_pull_deadline\`. Restoring 500ms therefore shifts the derived pull deadlines and the \`outer_pull_deadline_at_defaults_is_172s\` test — which is why it is split out of #1165 (the positive probe cache) rather than bundled into it.

Found during #1165 planning."
```

---

## Task 1: The `PositiveProbeCache` type

**Files:**
- Create: `crates/node/src/dht/probe_cache.rs`
- Modify: `crates/node/src/dht/mod.rs`
- Test: inline `mod tests` (matching `negative_cache.rs`)

**Interfaces produced** (later tasks depend on these exact signatures):
```rust
pub struct ProbedProvider { pub node_id: NodeId, pub rate_per_mb: u64, pub rtt_ms: u32 }
impl PositiveProbeCache {
    pub fn new() -> Self;
    pub fn with_capacity_and_ttl(cap: usize, ttl: Duration) -> Self;
    pub fn get(&self, hash: &Hash) -> Option<Vec<ProbedProvider>>;
    pub fn insert(&self, hash: Hash, providers: Vec<ProbedProvider>);
    pub fn invalidate(&self, hash: &Hash);
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}
```

- [ ] **Step 1: Read the template first**

Read `crates/node/src/dht/negative_cache.rs` in full. This task is that file with a different key and a `Vec` value. Mirror its module-doc shape, its `Inner`/`lock()`/poison-recovery structure, its ADR-citing const comments, and its test style. Divergences from it are called out explicitly below and each needs its own justification in a comment.

- [ ] **Step 2: Write the failing tests**

Create `crates/node/src/dht/probe_cache.rs` containing only the test module (it will not compile — that is the red):

```rust
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use std::thread;

    fn nid(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 32])
    }
    fn h(byte: u8) -> Hash {
        Hash::from_bytes([byte; 32])
    }
    fn p(byte: u8) -> ProbedProvider {
        ProbedProvider {
            node_id: nid(byte),
            rate_per_mb: u64::from(byte),
            rtt_ms: u32::from(byte),
        }
    }

    /// ADR 005 §Derived constants: `probe_cache_ttl = PROBE_SLASH_WINDOW / 2`.
    /// Pins the DERIVATION, not the number: a literal `15s` passes an
    /// `== Duration::from_secs(15)` assertion and then silently fails to move
    /// when governance changes the slashing window — the one thing ADR 005
    /// explicitly asks implementations to get right.
    #[test]
    fn ttl_is_derived_from_the_probe_slash_window() {
        assert_eq!(DEFAULT_TTL * 2, PROBE_SLASH_WINDOW);
        assert_eq!(DEFAULT_TTL, Duration::from_secs(15));
    }

    #[test]
    fn absent_hash_returns_none() {
        let c = PositiveProbeCache::new();
        assert!(c.get(&h(1)).is_none());
        assert!(c.is_empty());
    }

    #[test]
    fn insert_then_get_returns_providers_in_order() {
        let c = PositiveProbeCache::new();
        c.insert(h(1), vec![p(1), p(2)]);
        assert_eq!(c.get(&h(1)), Some(vec![p(1), p(2)]));
        assert!(c.get(&h(2)).is_none());
        assert_eq!(c.len(), 1);
    }

    /// ADR 001 §Probe cache: "Each hash entry retains at most 10 responses (top
    /// 10 by selection score)." This truncation is what bounds the cache at the
    /// ADR's stated ~1 MB; without it a hash with a large probe fanout is
    /// unbounded.
    #[test]
    fn insert_keeps_only_the_top_ten_providers() {
        let c = PositiveProbeCache::new();
        let many: Vec<_> = (1..=25u8).map(p).collect();
        c.insert(h(1), many);
        let got = c.get(&h(1)).unwrap();
        assert_eq!(got.len(), MAX_PROVIDERS_PER_HASH);
        // The FIRST ten — the caller ranked best-first, so truncation must drop
        // the tail. Taking the last ten would keep the ten WORST providers.
        assert_eq!(got, (1..=10u8).map(p).collect::<Vec<_>>());
    }

    /// An empty entry would occupy an LRU slot, hit on every read, and yield
    /// nothing — strictly worse than no entry.
    #[test]
    fn inserting_no_providers_is_a_no_op() {
        let c = PositiveProbeCache::new();
        c.insert(h(1), vec![]);
        assert!(c.is_empty());
        assert!(c.get(&h(1)).is_none());
    }

    #[test]
    fn expired_entry_returns_none_and_is_evicted() {
        // Margins kept generous (TTL 500ms, sleep 750ms) so loaded CI runners
        // with cargo-nextest parallelism don't flake on wall-clock checks.
        let c = PositiveProbeCache::with_capacity_and_ttl(8, Duration::from_millis(500));
        c.insert(h(1), vec![p(1)]);
        assert!(c.get(&h(1)).is_some());
        thread::sleep(Duration::from_millis(750));
        assert!(c.get(&h(1)).is_none());
        assert!(c.is_empty(), "expired entry should be evicted on read");
    }

    /// ADR 001 §Probe cache anchors the TTL at insertion. A read that re-stamped
    /// expiry during the LRU bump would keep a hot hash's probe results alive
    /// indefinitely — exactly the staleness the 15s window exists to bound, and
    /// it would ship green without this test.
    #[test]
    fn read_hit_does_not_refresh_ttl() {
        let c = PositiveProbeCache::with_capacity_and_ttl(8, Duration::from_millis(500));
        c.insert(h(1), vec![p(1)]);
        thread::sleep(Duration::from_millis(250));
        assert!(c.get(&h(1)).is_some());
        thread::sleep(Duration::from_millis(500));
        assert!(c.get(&h(1)).is_none(), "read-hit illegally extended the TTL");
    }

    #[test]
    fn lru_eviction_at_cap_drops_oldest() {
        let c = PositiveProbeCache::with_capacity(2);
        c.insert(h(1), vec![p(1)]);
        c.insert(h(2), vec![p(2)]);
        c.insert(h(3), vec![p(3)]);
        assert!(c.get(&h(1)).is_none());
        assert!(c.get(&h(2)).is_some());
        assert!(c.get(&h(3)).is_some());
        assert_eq!(c.len(), 2);
    }

    /// Pins that `get`'s remove-then-`shift_insert(0, ..)` really is an MRU bump
    /// and not an accidental no-op. The non-`Copy` value forces a different
    /// dance than `negative_cache`'s single `shift_insert`, so its equivalent
    /// test does not cover this one.
    #[test]
    fn read_hit_bumps_lru_so_oldest_eviction_changes() {
        let c = PositiveProbeCache::with_capacity(2);
        c.insert(h(1), vec![p(1)]);
        c.insert(h(2), vec![p(2)]);
        assert!(c.get(&h(1)).is_some()); // bump h(1) → h(2) becomes LRU
        c.insert(h(3), vec![p(3)]);
        assert!(c.get(&h(1)).is_some());
        assert!(c.get(&h(2)).is_none());
        assert!(c.get(&h(3)).is_some());
    }

    #[test]
    fn reinsert_replaces_providers_and_does_not_grow_len() {
        let c = PositiveProbeCache::with_capacity(4);
        c.insert(h(1), vec![p(1), p(2)]);
        c.insert(h(1), vec![p(3)]);
        assert_eq!(c.get(&h(1)), Some(vec![p(3)]));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn invalidate_removes_the_entry() {
        let c = PositiveProbeCache::new();
        c.insert(h(1), vec![p(1)]);
        c.invalidate(&h(1));
        assert!(c.get(&h(1)).is_none());
        assert!(c.is_empty());
    }

    /// The documented "disabled" configuration, so no `disabled()` constructor
    /// needs to exist for production code to eventually call.
    #[test]
    fn a_zero_ttl_cache_never_hits() {
        let c = PositiveProbeCache::with_capacity_and_ttl(8, Duration::ZERO);
        c.insert(h(1), vec![p(1)]);
        assert!(c.get(&h(1)).is_none());
    }
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `cargo nextest run -p decdn-node dht::probe_cache`
Expected: compile error — `PositiveProbeCache` not found. (`dht/mod.rs` must declare `pub mod probe_cache;` for the file to be compiled at all — add that first, then the failure is the type, not the module.)

- [ ] **Step 4: Write the implementation**

Prepend to `crates/node/src/dht/probe_cache.rs`:

```rust
//! Requester-side POSITIVE probe cache (ADR 001 §Probe cache).
//!
//! The mirror of [`super::negative_cache`]: where that one remembers which
//! `(NodeId, hash)` pairs answered `has_blob: false`, this one remembers which
//! nodes answered `has_blob: true` — so a second miss for the same hash inside
//! the TTL skips the DHT lookup AND the probe fanout entirely and goes straight
//! to selection. Per ADR 001 §Probe cache the cache:
//!
//! - is keyed by `hash`;
//! - holds at most 1024 hashes with LRU eviction;
//! - retains at most 10 providers per hash (top 10 by selection score);
//! - retains entries for `PROBE_SLASH_WINDOW / 2` = 15s (ADR 005 §Derived
//!   constants), anchored at insertion;
//! - stores ONLY the ADR triple `(NodeId, rate_per_mb, rtt)` — never the
//!   signed `ProbeResponse`.
//!
//! That last point is #1165's "no evidence retention" requirement, and it is
//! not a memory optimisation. A `ProbeResponse` carries `slash_sig`: a peer's
//! signed `has_blob: true`, which is on-chain phantom-announcement slash
//! evidence for `PROBE_SLASH_WINDOW` (ADR 005). A structure that survives one
//! request in order to speed up the next has no business holding another node's
//! slashable statements — retaining them turns an availability cache into an
//! evidence locker.
//!
//! `reputation` is likewise NOT stored, for a different reason: it is a local,
//! live value that moves on every pull outcome. Freezing it for 15s would let a
//! node that just failed three pulls keep the rank it held before them.
//! `node_origin::cached_candidates` recomputes reputation and region on read and
//! re-runs `rank_candidates` — ADR 001's "goes straight to selection" means
//! skipping discovery and probing, not skipping the selection algorithm.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use decdn_cache::PROBE_SLASH_WINDOW;
use indexmap::IndexMap;
use tracing::warn;

use crate::dht::routing::NodeId;

pub use crate::dht::records::Hash;

/// ADR 005 §Derived constants: `probe_cache_ttl = PROBE_SLASH_WINDOW / 2` = 15s.
///
/// Derived rather than written as a literal `15s` because ADR 005 says so
/// ("Implementations SHOULD define `PROBE_SLASH_WINDOW` as a named constant and
/// compute the others from it"): a governance change to the slashing window must
/// move this with it, and a literal would silently not move. The 15s ceiling is
/// what keeps a stream opened from a cached entry inside the window during which
/// a misbehaving provider is still slashable.
///
/// Expressed through `as_secs` because `Duration: Div<u32>` is not `const` — the
/// same reason [`decdn_cache::PROBE_HOLD_DURATION`] reaches for
/// `saturating_add`. Integer division truncates sub-second remainders; harmless
/// at the current even 30s, and the halving is a policy ratio, not an exact
/// arithmetic requirement.
const DEFAULT_TTL: Duration = Duration::from_secs(PROBE_SLASH_WINDOW.as_secs() / 2);

/// ADR 001 §Probe cache: max 1024 entries.
const DEFAULT_CAPACITY: usize = 1024;

/// ADR 001 §Probe cache: "Each hash entry retains at most 10 responses (top 10
/// by selection score)." With [`DEFAULT_CAPACITY`] this is what bounds the
/// cache's memory at the ADR's stated ~1 MB (1024 × 10 × ~100 bytes).
///
/// Enforced by [`PositiveProbeCache::insert`] rather than by its caller, so the
/// bound is an invariant of the TYPE: a cap a caller can forget is not a cap.
const MAX_PROVIDERS_PER_HASH: usize = 10;

/// One probed provider — exactly ADR 001 §Probe cache's entry triple
/// (`hash → Vec<(NodeId, rate_per_mb, rtt)>`), nothing more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProbedProvider {
    /// The provider that answered `has_blob: true`.
    pub node_id: NodeId,
    /// Its quoted `ProbeResponse::rate_per_mb`.
    pub rate_per_mb: u64,
    /// The round-trip latency observed on that probe.
    pub rtt_ms: u32,
}

#[derive(Debug)]
struct Entry {
    /// Providers in write-time ranked order, best-first, truncated to
    /// [`MAX_PROVIDERS_PER_HASH`].
    providers: Vec<ProbedProvider>,
    /// Absolute expiry, anchored at insert and never refreshed on read.
    expiry: Instant,
}

#[derive(Debug)]
struct Inner {
    /// Hash → entry. `IndexMap` collapses what would otherwise be a `HashMap`
    /// + side `VecDeque` (kept in lockstep to track LRU order) into a single
    /// store: insertion order is the LRU ordering, index 0 = most-recently-used
    /// and `len()-1` = least-recently-used. Identical to
    /// [`super::negative_cache`] — deliberately, so the two caches' eviction and
    /// expiry semantics cannot drift.
    entries: IndexMap<Hash, Entry>,
    /// Hard cap on live hashes. Clamped to ≥ 1 in the constructor.
    cap: usize,
    /// TTL applied on insert. Anchored at insertion, NOT refreshed on read.
    ttl: Duration,
}

/// Bounded LRU cache of `hash → Vec<(NodeId, rate_per_mb, rtt)>`.
#[derive(Debug)]
pub struct PositiveProbeCache {
    inner: Mutex<Inner>,
}

impl Default for PositiveProbeCache {
    fn default() -> Self {
        Self::new()
    }
}

impl PositiveProbeCache {
    /// Build a cache with the ADR 001 §Probe cache defaults (1024 hashes, 15s
    /// TTL, ≤10 providers per hash).
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity_and_ttl(DEFAULT_CAPACITY, DEFAULT_TTL)
    }

    #[cfg(test)]
    #[must_use]
    fn with_capacity(cap: usize) -> Self {
        Self::with_capacity_and_ttl(cap, DEFAULT_TTL)
    }

    /// Build a cache with an explicit capacity and TTL.
    ///
    /// Production code uses [`Self::new`]; this is the test / tuning seam, for
    /// the same reason
    /// [`super::negative_cache::NegativeProbeCache::with_capacity_and_ttl`] is:
    /// the TTL is anchored on [`Instant`], so `tokio::time` pause / advance has
    /// no effect on it, and 15s of wall clock per assertion is not a test suite.
    /// `cap` is clamped to ≥ 1.
    ///
    /// A `ttl` of [`Duration::ZERO`] disables the cache behaviourally: every
    /// entry is already expired the instant it is read, so [`Self::get`] always
    /// misses. That is the supported way to opt out of positive caching — there
    /// is deliberately no `disabled()` constructor, because a constructor that
    /// turns off an ADR-mandated behaviour is a thing production code will
    /// eventually call.
    #[must_use]
    pub fn with_capacity_and_ttl(cap: usize, ttl: Duration) -> Self {
        let cap = cap.max(1);
        Self {
            inner: Mutex::new(Inner {
                entries: IndexMap::with_capacity(cap),
                cap,
                ttl,
            }),
        }
    }

    /// The cached providers for `hash`, best-first, iff an entry exists and its
    /// TTL hasn't elapsed.
    ///
    /// A live hit bumps the entry to the front of the LRU ordering (index 0)
    /// **without refreshing its expiry** — TTL is anchored at insertion per
    /// ADR 001 §Probe cache. An expired entry is evicted before returning
    /// `None`.
    ///
    /// Returns a clone rather than a guard-scoped borrow on purpose: the caller
    /// rebuilds `Candidate`s from this, which means an `await` on `region_of(..)`
    /// per provider, and holding a `std::sync::Mutex` guard across an await is
    /// exactly the hazard `clippy::await_holding_lock` exists for. At ≤10 small
    /// `Copy` structs the copy is not worth arguing about.
    #[must_use]
    pub fn get(&self, hash: &Hash) -> Option<Vec<ProbedProvider>> {
        let now = Instant::now();
        let mut guard = self.lock();
        let expiry = guard.entries.get(hash)?.expiry;
        if expiry <= now {
            guard.entries.shift_remove(hash);
            return None;
        }
        // Bump to front of LRU, preserving the original expiry. Unlike
        // `negative_cache`, whose `Instant` value is `Copy` and so can ride a
        // single `shift_insert(0, key, expiry)`, the `Entry` here must be moved
        // out and back.
        let entry = guard.entries.shift_remove(hash)?;
        let providers = entry.providers.clone();
        guard.entries.shift_insert(0, *hash, entry);
        Some(providers)
    }

    /// Insert / replace the entry for `hash` with `now + TTL` expiry, moving it
    /// to the front of the LRU ordering. Evicts the least-recently-used hash on
    /// cap overflow.
    ///
    /// `providers` MUST already be in selection order, best-first: this keeps the
    /// first [`MAX_PROVIDERS_PER_HASH`], which is ADR 001's "top 10 by selection
    /// score" only if the caller ranked first. The cache cannot rank them itself
    /// — the selection score needs `reputation`, which is precisely the field
    /// this cache refuses to store.
    ///
    /// An empty `providers` is a no-op. An empty entry is strictly worse than no
    /// entry: it would occupy an LRU slot, hit on every read, and yield nothing
    /// — a cache of "we found nobody", which is the negative cache's job and not
    /// on the negative cache's terms.
    pub fn insert(&self, hash: Hash, mut providers: Vec<ProbedProvider>) {
        if providers.is_empty() {
            return;
        }
        providers.truncate(MAX_PROVIDERS_PER_HASH);
        let mut guard = self.lock();
        let expiry = Instant::now() + guard.ttl;
        let entry = Entry { providers, expiry };
        // `shift_insert` moves an existing key to the new index and replaces the
        // value — the MRU bump we want on the refresh path.
        guard.entries.shift_insert(0, hash, entry);
        if guard.entries.len() > guard.cap {
            // `pop` removes the last entry — the LRU back.
            guard.entries.pop();
        }
    }

    /// Drop the entry for `hash`, if any.
    ///
    /// ADR 001 §Probe cache: "if all fail, run a fresh DHT lookup + probe." A hit
    /// whose every provider failed to deliver has been disproved by the only
    /// evidence that outranks a probe — an actual pull — so it is removed rather
    /// than left to keep hitting for the rest of its 15s.
    pub fn invalidate(&self, hash: &Hash) {
        self.lock().entries.shift_remove(hash);
    }

    /// Current hash count. Includes expired entries that haven't been swept yet
    /// — call [`Self::get`] first if a precise live count is needed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// Whether the cache holds zero entries (including stale).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().entries.is_empty()
    }

    /// Poison-tolerant lock acquisition — see
    /// [`super::negative_cache::NegativeProbeCache`] for why we recover rather
    /// than propagate.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                warn!("PositiveProbeCache mutex poisoned; recovering inner state");
                poisoned.into_inner()
            }
        }
    }
}
```

In `crates/node/src/dht/mod.rs`: add `pub mod probe_cache;` (alphabetical — after `origin`, before `publish`) and `pub use probe_cache::{PositiveProbeCache, ProbedProvider};` beside the existing `pub use negative_cache::NegativeProbeCache;` at `:31`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo nextest run -p decdn-node dht::probe_cache`
Expected: PASS, 13 tests.

- [ ] **Step 6: Lint and commit**

```bash
cargo clippy -p decdn-node --all-targets -- -D warnings && cargo fmt -- --check
git add crates/node/src/dht/probe_cache.rs crates/node/src/dht/mod.rs
git commit -m "feat(node): lean positive probe cache (ADR 001, 15s TTL, no evidence retention)"
```

---

## Task 2: Emit `decdn_probe_post_eviction_failures_total`

ADR 001 §Probe cache attaches one observability requirement to this feature: track the `EvictedSinceProbe` response rate. `adr/appendix-observability.md:86` already assigns it the canonical name — and `monitoring/grafana-dashboard.json:411` already scrapes it. The panel has been reading a metric nothing emits.

**Files:**
- Modify: `crates/node/src/metrics.rs`, `crates/node/src/node_origin.rs`
- Test: `crates/node/src/node_origin.rs` inline tests (beside `:2341`)

**The blocker:** `classify_refusal` (`node_origin.rs:1396`) maps **both** `EvictedSinceProbe` and `BlobTooLarge` onto `RefusalVerdict::DurableMiss`. Firing the metric from that arm as-is over-counts. `classify_refusal` is a pure `const fn` with no `deps`, so it cannot fire a metric itself — give the verdict its cause and keep the function pure.

- [ ] **Step 1: Write the failing test**

Add beside the existing `classify_refusal` table test at `node_origin.rs:2339`:

```rust
    /// ADR 001 §Probe cache mandates tracking the `EvictedSinceProbe` rate, and
    /// ADR 005 explains why it is not just "a candidate failed": a peer emitting
    /// it inside `PROBE_SLASH_WINDOW` has handed us slash evidence. Both causes
    /// are `DurableMiss` — they say the same thing about the peer — but only one
    /// is that signal, and a shared unpayloaded variant cannot tell them apart.
    #[test]
    fn only_an_eviction_carries_the_post_eviction_cause() {
        assert_eq!(
            classify_refusal(&StreamError::EvictedSinceProbe),
            RefusalVerdict::DurableMiss(DurableMissCause::EvictedSinceProbe)
        );
        assert_eq!(
            classify_refusal(&StreamError::BlobTooLarge),
            RefusalVerdict::DurableMiss(DurableMissCause::BlobTooLarge)
        );
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p decdn-node only_an_eviction_carries`
Expected: compile error — `DurableMissCause` not found.

- [ ] **Step 3: Split the verdict**

In `crates/node/src/node_origin.rs`, beside `RefusalVerdict` (`:1358`):

```rust
/// Why a [`RefusalVerdict::DurableMiss`] is durable.
///
/// The verdict itself answers "what does this refusal say about the peer?", and
/// both causes give the same answer: asking this peer for this hash again inside
/// the TTL gets the same reply, so suppress it and don't spend a candidate slot
/// finding out. They are NOT the same thing to an operator, though —
/// `EvictedSinceProbe` is a peer contradicting its own signed `has_blob: true`
/// and is the ADR 001-mandated `decdn_probe_post_eviction_failures_total`
/// signal, while `BlobTooLarge` is a static fact about the blob that says
/// nothing about anyone's hold mechanism. A payload rather than a fourth
/// `RefusalVerdict` variant, because a variant would claim the two mean
/// different things about the peer, and they do not (#1165).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurableMissCause {
    /// The peer held the blob at probe time and lost it to cache pressure
    /// before we opened the stream — a hold-mechanism failure (ADR 005).
    EvictedSinceProbe,
    /// The blob is over the peer's ceiling — deterministic for this blob.
    BlobTooLarge,
}
```

Change `RefusalVerdict::DurableMiss` → `DurableMiss(DurableMissCause)`. `node_origin.rs:1396` becomes:

```rust
        StreamError::EvictedSinceProbe => {
            RefusalVerdict::DurableMiss(DurableMissCause::EvictedSinceProbe)
        }
        StreamError::BlobTooLarge => RefusalVerdict::DurableMiss(DurableMissCause::BlobTooLarge),
```

Keep the existing "Honest and durable" comment above them. Fix the two other callers the compiler will flag: `:2235` and `:2341` (both unit tests, mechanical).

- [ ] **Step 4: Add the counters**

In `crates/node/src/metrics.rs`, three `Counter` fields beside `node_pull_*` (~`:614`):

```rust
    /// `decdn_probe_cache_hits_total` (#1165): cache-miss pulls that found a
    /// live ADR 001 §Probe cache entry with at least one still-selectable
    /// provider, and so skipped the DHT lookup and the probe fanout entirely.
    /// Field has no `_total` suffix because the `OpenMetrics` encoder appends it.
    /// With `probe_cache_misses` this is the hit ratio the 15s TTL exists to buy;
    /// a ratio near zero means the TTL is shorter than the request inter-arrival
    /// time for hot blobs and the cache is pure overhead.
    pub probe_cache_hits: Counter,
    /// `decdn_probe_cache_misses_total` (#1165): cache-miss pulls that had to run
    /// a fresh DHT lookup + probe. Counts an entry that was absent, expired, OR
    /// fully suppressed (every cached provider negative-cached or wedged) — all
    /// three cost the same network work, which is what this measures.
    pub probe_cache_misses: Counter,
    /// `decdn_probe_post_eviction_failures_total` (ADR 001 §Probe cache,
    /// ADR 005 §`EvictedSinceProbe` semantics; #1165): an upstream answered
    /// `StreamError::EvictedSinceProbe` — it held the blob when it signed
    /// `has_blob: true` and lost it to cache pressure before we opened the
    /// stream.
    ///
    /// ADR 001 mandates tracking this rate; ADR 005 says why it matters more than
    /// "a candidate failed": a node using probe-triggered eviction holds
    /// correctly should *rarely* emit this, because a held blob is invisible to
    /// the LRU driver. A sustained rate above ~1% therefore indicates a remote
    /// hold-mechanism FAILURE — an implementation bug or resource exhaustion —
    /// not a budget-configuration issue, which would surface as `has_blob: false`
    /// at probe time and never reach a stream request.
    ///
    /// Narrower than the `RefusalVerdict::DurableMiss` arm that fires it, which
    /// also covers `BlobTooLarge` — hence `DurableMissCause`.
    pub probe_post_eviction_failures: Counter,
```

Helpers beside `node_pull_attempt` (~`:1699`):

```rust
    /// A cache-miss pull was served from the ADR 001 probe cache: no DHT lookup,
    /// no probe fanout (#1165).
    pub fn probe_cache_hit(&self) {
        self.decdn.probe_cache_hits.inc();
    }

    /// A cache-miss pull found no usable probe-cache entry and ran a fresh
    /// lookup + probe (#1165).
    pub fn probe_cache_miss(&self) {
        self.decdn.probe_cache_misses.inc();
    }

    /// An upstream refused a stream with `EvictedSinceProbe` after answering
    /// `has_blob: true` at probe (ADR 001 §Probe cache; #1165).
    pub fn probe_post_eviction_failure(&self) {
        self.decdn.probe_post_eviction_failures.inc();
    }
```

- [ ] **Step 5: Fire it**

`node_origin.rs:1832` becomes:

```rust
                RefusalVerdict::DurableMiss(cause) => {
                    if cause == DurableMissCause::EvictedSinceProbe {
                        // ADR 001 §Probe cache mandates tracking this rate; ADR 005
                        // says a correct hold mechanism should make it rare, so a
                        // sustained rate is a remote implementation bug, not a
                        // tuning knob. `monitoring/grafana-dashboard.json` already
                        // scrapes this panel — it has been reading a metric nobody
                        // emitted (#1165).
                        deps.metrics.probe_post_eviction_failure();
                    }
                    suppress(None);
                    debug!(%provider_addr, ?cause, %err, "node-origin: upstream does not have this blob; negative-caching this (peer, hash) for the full TTL without tarring reputation");
                }
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo nextest run -p decdn-node node_origin metrics`
Expected: PASS, including `only_an_eviction_carries_the_post_eviction_cause`.

- [ ] **Step 7: Commit**

```bash
cargo clippy -p decdn-node --all-targets -- -D warnings
git add crates/node/src/metrics.rs crates/node/src/node_origin.rs
git commit -m "feat(node): emit decdn_probe_post_eviction_failures_total (ADR 001 §Probe cache)

The metric has been specified at appendix-observability.md:86 and scraped by
monitoring/grafana-dashboard.json:411 without ever being emitted. Splitting
RefusalVerdict::DurableMiss over a DurableMissCause keeps BlobTooLarge — which
shares the verdict but says nothing about any hold mechanism — out of the count."
```

---

## Task 3: Thread a fetch-wide attempt budget (behaviour-neutral refactor)

Sequenced **before** any behaviour change so the existing 100%-green suite is its oracle. Do not merge this with Task 5.

**Files:** Modify `crates/node/src/node_origin.rs`

**Interfaces produced:**
```rust
struct PullOutcome { bytes: Option<Bytes>, attempts: usize }
async fn try_pull(deps: &NodeOriginDeps, ranked: &[Candidate], hash_bytes: [u8; 32], budget: usize) -> PullOutcome;
// method on NodeOrigin:
async fn open_from_candidates(&self, deps: &NodeOriginDeps, ranked: &[Candidate], hash_bytes: [u8; 32], budget: usize)
    -> (Option<(UpstreamPullHeader, NodeProgressivePull)>, usize);
```

- [ ] **Step 1: Add `PullOutcome`**

```rust
/// What one walk of a ranked candidate list consumed and produced.
///
/// The `attempts` count is the load-bearing half (#1165). [`MAX_PROVIDER_ATTEMPTS`]
/// is a budget for the whole FETCH, not for each list, and a fetch that hits the
/// probe cache walks two lists: the cached providers, then — if they all fail — a
/// freshly discovered one. Handing each list its own [`MAX_PROVIDER_ATTEMPTS`]
/// would double the worst case to six sequential pulls, silently blowing through
/// [`crate::selection::outer_pull_deadline`], which budgets for exactly three
/// (`(open + pull + stall) × MAX_PROVIDER_ATTEMPTS + slack`, 172s at defaults).
/// The deadline is enforced OUTSIDE this path, so nothing would fail loudly: the
/// fetch would just be killed mid-pull by a timeout sized for a world it no
/// longer lived in — the #859 starvation that formula exists to prevent.
///
/// So the caller carries a remaining-attempts budget across both phases and this
/// reports what was spent.
struct PullOutcome {
    /// The blob, iff some candidate delivered.
    bytes: Option<Bytes>,
    /// Candidates actually TRIED — i.e. `pull_from_candidate` calls, whether or
    /// not they delivered. Never exceeds the `budget` passed in.
    attempts: usize,
}
```

- [ ] **Step 2: Rewrite `try_pull`** (replaces `node_origin.rs:1001-1015`)

Keep the existing doc comment, appending: "`budget` is the fetch-wide [`MAX_PROVIDER_ATTEMPTS`] remainder rather than the constant itself — see [`PullOutcome`]."

```rust
async fn try_pull(
    deps: &NodeOriginDeps,
    ranked: &[Candidate],
    hash_bytes: [u8; 32],
    budget: usize,
) -> PullOutcome {
    let mut attempts = 0;
    for candidate in ranked.iter().take(budget) {
        attempts += 1;
        if let Some(bytes) = pull_from_candidate(deps, candidate, hash_bytes).await {
            return PullOutcome { bytes: Some(bytes), attempts };
        }
    }
    PullOutcome { bytes: None, attempts }
}
```

- [ ] **Step 3: Add `open_from_candidates`** — the window twin, lifting the loop out of `open_progressive_pull:407`

```rust
    /// The window twin of [`try_pull`]: walk the ranked candidates, opening from
    /// each until one succeeds, bounded by `budget` remaining attempts. Returns
    /// the opened pull (if any) and how many candidates were tried.
    ///
    /// Not a [`PullOutcome`] because the payload type differs, and a generic over
    /// the terminal async op costs more `Pin<Box<dyn Future>>` ceremony than the
    /// four duplicated lines are worth.
    async fn open_from_candidates(
        &self,
        deps: &NodeOriginDeps,
        ranked: &[Candidate],
        hash_bytes: [u8; 32],
        budget: usize,
    ) -> (Option<(UpstreamPullHeader, NodeProgressivePull)>, usize) {
        let mut attempts = 0;
        for candidate in ranked.iter().take(budget) {
            attempts += 1;
            if let Some(opened) = self.open_from_candidate(deps, candidate, hash_bytes).await {
                return (Some(opened), attempts);
            }
        }
        (None, attempts)
    }
```

- [ ] **Step 4: Update both call sites to pass `MAX_PROVIDER_ATTEMPTS`**

`fetch:807` → `match try_pull(deps, &ranked, hash_bytes, MAX_PROVIDER_ATTEMPTS).await.bytes {`
`open_progressive_pull:407-411` → `self.open_from_candidates(deps, &ranked, hash_bytes, MAX_PROVIDER_ATTEMPTS).await.0`

- [ ] **Step 5: Run the full suite — it is the oracle**

Run: `cargo nextest run -p decdn-node --no-fail-fast`
Expected: PASS, exactly as before. Any diff is a bug in this refactor.

- [ ] **Step 6: Commit**

```bash
cargo clippy -p decdn-node --all-targets -- -D warnings
git add crates/node/src/node_origin.rs
git commit -m "refactor(node): thread a fetch-wide attempt budget through the candidate walk"
```

---

## Task 4: Wire the cache in + the write hook (write-only, behaviour-neutral)

Nothing reads the cache yet, so the suite must stay green unchanged — that is the test.

**Files:** Modify `crates/node/src/node_origin.rs`, `crates/node/src/runtime/mod.rs`, `crates/node/tests/node_origin_pull.rs`

- [ ] **Step 1: Add the deps field**

In `NodeOriginDeps`, immediately after `negative_cache` (`node_origin.rs:288`) so the two read as a pair:

```rust
    /// Requester-side positive probe cache (ADR 001 §Probe cache): lets a repeat
    /// miss for a hash inside the TTL skip the DHT lookup and probe fanout
    /// (#1165). Beside the negative cache, and by value for the same reason:
    /// `NodeOrigin` holds its deps behind `Arc<OnceLock<NodeOriginDeps>>`, so
    /// every concurrent pull already shares one instance through that `Arc`, and
    /// the cache's own `Mutex` gives it interior mutability behind `&self`. A
    /// second `Arc` here would buy nothing but a pointer chase.
    pub probe_cache: PositiveProbeCache,
```

Import `PositiveProbeCache` and `ProbedProvider` from `crate::dht`. Add `probe_cache: crate::dht::PositiveProbeCache::new(),` at `runtime/mod.rs:1711`, beside the `negative_cache` line.

- [ ] **Step 2: Extract `rank` and add the write hook** (replaces `probe_and_rank`'s tail, `node_origin.rs:895-899`)

```rust
    let ranked = rank(candidates);
    // ADR 001 §Probe cache: retain the top 10 by selection score, so a repeat
    // miss for this hash inside the TTL skips the lookup and the probe fanout.
    // `insert` does the truncation; `ranked` is already in selection order,
    // which is the ordering that claim depends on.
    //
    // Only the triple is stored — never the signed `ProbeResponse` (its
    // `slash_sig` is another node's slashable statement, and #1165's "no
    // evidence retention" is that this cache must not become an evidence
    // locker), and never `reputation`, which `cached_candidates` recomputes.
    deps.probe_cache.insert(
        target,
        ranked
            .iter()
            .map(|c| ProbedProvider {
                node_id: DhtNodeId::from_bytes(c.node_id),
                rate_per_mb: c.rate_per_mb,
                rtt_ms: c.rtt_ms,
            })
            .collect(),
    );
    ranked
```

(`target` is already bound at `:864`.) Add the shared helper:

```rust
/// `rank_candidates` reduced to the best-first `Candidate` list both pull paths
/// consume. Shared by the cold path and the probe-cache-hit path so a change to
/// what "ranked" means cannot apply to one and not the other.
fn rank(candidates: Vec<Candidate>) -> Vec<Candidate> {
    rank_candidates(candidates)
        .into_iter()
        .map(|r| r.candidate)
        .collect()
}
```

- [ ] **Step 3: Add the dual-cache test seam**

In `crates/node/tests/node_origin_pull.rs`, beside `build_origin_with_negative_cache` (`:693`):

```rust
/// [`build_origin_with_timeout`] with BOTH probe caches injected, so a test can pick each
/// TTL independently.
///
/// The positive cache anchors expiry on `Instant` like its negative twin, so `tokio::time`
/// cannot fast-forward it and 15s per assertion is not a test suite. Injecting the two
/// separately is also what makes their INTERACTION observable: a positive entry that
/// outlives a negative one is how a peer becomes selectable again without a re-probe.
#[allow(clippy::too_many_arguments, clippy::expect_used)]
fn build_origin_with_probe_caches(
    /* …same params as build_origin_with_negative_cache… */
    negative_cache: NegativeProbeCache,
    probe_cache: PositiveProbeCache,
) -> NodeOrigin { /* …body of build_origin_with_negative_cache, passing probe_cache… */ }
```

Refactor `build_origin_with_negative_cache` to delegate to it with `PositiveProbeCache::new()`. Give the four default builders (`:792`, `:995`, `:1440`, `:9130`) `probe_cache: PositiveProbeCache::new()`.

**Do not disable the cache in the default builders.** Nothing breaks (Task 5 Step 1 explains why), so a disabled default would only stop the existing repeat-fetch tests from exercising the feature — turning free integration coverage into dead code, and making every future test in this file a test of a configuration production never runs. `Duration::ZERO` via `with_capacity_and_ttl` is the documented escape hatch if a future test genuinely needs it off.

- [ ] **Step 4: Run the suite — must be green, unchanged**

Run: `cargo nextest run -p decdn-node --test node_origin_pull --no-fail-fast`
Expected: PASS. Nothing reads the cache yet, so any failure means the write hook has a side effect it should not have.

- [ ] **Step 5: Commit**

```bash
cargo clippy -p decdn-node --all-targets -- -D warnings
git add crates/node/src/node_origin.rs crates/node/src/runtime/mod.rs crates/node/tests/node_origin_pull.rs
git commit -m "feat(node): populate the positive probe cache from probe_and_rank"
```

---

## Task 5: `cached_candidates` + cached-first `Origin::fetch`

**Files:** Modify `crates/node/src/node_origin.rs`, `crates/node/tests/node_origin_pull.rs`

**Existing-test impact — investigated, no breakage.** `Origin::fetch` has 34 call sites in `node_origin_pull.rs`; exactly four issue two calls, and three reuse a hash. All three pass, and one becomes free regression coverage:
- `node_origin_not_found_refusal_does_not_tar_upstream` (`:6164`/`:6207`) — fetch 1 caches `[N, A]`; N refuses `NotFound` → negative-cached 30s; A delivers. Fetch 2 hits the cache, `cached_candidates` filters N via the negative cache → `[A]` → delivers. **This test only stays green because of the negative-cache filter in `cached_candidates`** — drop it and N is resurrected and refused again. Do not touch this test.
- `refusal_suppression_after` (`:6326`/`:6335`, 3 callers) — injects a 100ms negative TTL, deliberately inverted against the positive cache's 15s. All three assertions hold; the third now skips a probe it used to send, but asserts on `node_pull_refused_total`, which fires in `classify_pull_failure` regardless. **Amend its doc in this commit**: "observed through `probe_and_rank`'s filter" becomes "through `cached_candidates`' filter on a probe-cache hit, and `probe_and_rank`'s on the cold path — the two chokepoints every candidate passes."
- `node_origin_reused_channel_resumes_voucher_progress` (`:6711`/`:6724`) — one provider; fetch 2 hits the cache and pulls it directly. Asserts on the voucher log, which the shortcut does not touch.
- The pairs at `:5487`/`:5500` and `:9150`/`:9151` use *different* hashes — different cache keys, unaffected.

`node_pull_attempts_total == 1` is asserted at `:1797`, `:3576`, `:4761`, `:6605` — all single-fetch, so none exercises the double-count guard. That is precisely why the tests below must add their own.

- [ ] **Step 1: Write the failing integration tests**

Add `spawn_a_probe_counting_server` (a variant of the existing probe-server helpers that increments an `Arc<AtomicUsize>` in the probe handler). A cache hit is *defined* as "no probe was sent" — assert at the wire, not on the counter.

```rust
/// ADR 001 §Probe cache: "On a cache miss the requester checks the probe cache first; if a
/// valid entry exists, it skips DHT lookup and goes straight to selection."
///
/// Observed where it is defined — at the WIRE. Asserting on
/// `decdn_probe_cache_hits_total` alone would pass an implementation that increments the
/// counter and probes anyway; the counter is the report, the silent probe endpoint is the
/// property.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_fetch_inside_the_ttl_skips_the_probe_entirely() -> Result<()> {
    // … A holds the blob and counts probes …
    let _ = Origin::fetch(&origin, hash, u64::MAX).await?.collect_to_bytes().await?;
    anyhow::ensure!(probes.load(Ordering::SeqCst) == 1, "first fetch must probe");
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;

    let second = Origin::fetch(&origin, hash, u64::MAX).await?.collect_to_bytes().await?;
    anyhow::ensure!(second.is_some_and(|b| b.as_ref() == payload.as_slice()));
    anyhow::ensure!(
        probes.load(Ordering::SeqCst) == 1,
        "the second fetch re-probed — the probe cache saved nothing, which is the entire \
         point of ADR 001 §Probe cache"
    );
    assert_counter(&b_metrics, "probe_cache_hits_total", 1)?;
    // ONE orchestration per fetch, however many candidate lists it walks.
    assert_counter(&b_metrics, "node_pull_attempts_total", 2)?;
    Ok(())
}

/// The TTL is not decorative. Past it the entry is gone and the fetch pays for a fresh
/// lookup + probe — which is what bounds how stale a served candidate can be. Uses an
/// injected 300ms TTL via `build_origin_with_probe_caches`: the real 15s is anchored on
/// `Instant`, so `tokio::time` cannot skip it.
#[tokio::test(flavor = "multi_thread")]
async fn a_fetch_past_the_ttl_probes_again() -> Result<()> { /* … probes == 2 … */ }

/// ONE `MAX_PROVIDER_ATTEMPTS` budget for the whole fetch (#1165), not one per phase.
///
/// Three cached providers that all refuse must consume the budget and END the fetch — not
/// hand a fresh lookup three more pulls. Six sequential pulls against an
/// `outer_pull_deadline` sized for three (172s at defaults) is a fetch killed mid-pull by a
/// timeout that no longer describes it, and nothing in `selection.rs` would catch it: the
/// deadline is enforced elsewhere, on the assumption this constant is honoured here.
#[tokio::test(flavor = "multi_thread")]
async fn cached_candidates_and_the_cold_path_share_one_attempt_budget() -> Result<()> {
    // 3 refusing providers, pre-warmed into the cache by fetch #1 (which spends the budget
    // refusing). Fetch #2 hits the cache, spends 3 attempts, invalidates, and returns
    // NotFound WITHOUT a fresh lookup — assert zero further probes and that
    // node_pull_refused_total advanced by exactly 3, not 6.
}

/// A cached entry can go stale INSIDE its own 15s. A pull-time refusal recorded a negative
/// for this (peer, hash) seconds ago, and the hit path must honour it — otherwise the
/// positive cache resurrects exactly the peers the last fetch just learned not to ask,
/// which is a positive cache that undoes the negative one.
#[tokio::test(flavor = "multi_thread")]
async fn a_probe_cache_hit_still_honours_the_negative_cache() -> Result<()> {
    // N refuses, A delivers. Assert at the WIRE: across both fetches N's probe counter
    // stays at 1 AND N's stream counter stays at 1.
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo nextest run -p decdn-node --test node_origin_pull probe_cache --no-fail-fast`
Expected: FAIL — the second fetch re-probes (`probes == 2`), and `probe_cache_hits_total` does not exist.

- [ ] **Step 3: Implement `cached_candidates`**

```rust
/// Rebuild ranked [`Candidate`]s from a probe-cache hit (ADR 001 §Probe cache).
///
/// `None` means "no usable cached candidate", covering three cases the caller has
/// no reason to distinguish: no entry, an expired entry, and an entry whose every
/// provider is currently suppressed. All three cost a fresh lookup + probe and all
/// three count as `probe_cache_miss` — a hit that saves no network work is not a
/// hit in any sense a dashboard cares about.
///
/// The cache stores only the ADR triple. `reputation` and `region` are rebuilt
/// here, FRESH, and the result re-ranked. That is what ADR 001's "goes straight to
/// selection" means: skip discovery and probing — not skip the selection
/// algorithm. Caching a `Candidate` whole would have been less code and would have
/// frozen `reputation` for 15 seconds, letting a node that failed three pulls in
/// the meantime keep the rank it earned before them; the fields we decline to
/// cache are the ones that MOVE.
async fn cached_candidates(deps: &NodeOriginDeps, target: DhtHash) -> Option<Vec<Candidate>> {
    let providers = deps.probe_cache.get(&target)?;
    let now_secs = crate::payment_settlement::unix_now();
    let mut candidates = Vec::with_capacity(providers.len());
    for provider in providers {
        // A 15-second-old entry can be stale INSIDE its own TTL, so the same two
        // filters `probe_and_rank` applies are applied here — this is the other
        // chokepoint every candidate passes through. A pull-time refusal recorded
        // a negative for this exact (peer, hash) seconds ago
        // (`classify_pull_failure`), and a wedged channel cannot serve ANY hash.
        if deps.negative_cache.contains_active(&provider.node_id, &target)
            || deps.provider_is_wedged(&provider.node_id, now_secs)
        {
            continue;
        }
        let Ok(pk) = PublicKey::from_bytes(provider.node_id.as_bytes()) else {
            // Matches `probe_candidate`: a key that does not decode implies
            // upstream state corruption — skip rather than panic.
            continue;
        };
        let region = deps
            .region_accountant
            .region_of(provider.node_id.as_bytes())
            .await
            .unwrap_or_default();
        // Deliberately NOT re-running the ADR 030 region-latency penalty here,
        // unlike `probe_candidate`. That penalty reads a probe's `rtt` as EVIDENCE
        // against a self-attested region claim, and we already scored this rtt
        // once — at probe time, when it was evidence. A cache hit performs no
        // probe and produces no new evidence, so re-recording would charge one
        // probe's latency to the peer again on every hit inside the TTL: an EWMA
        // beaten down by a single 15-second-old sample replayed as many times as
        // the blob happens to be requested. Popularity is not guilt.
        candidates.push(Candidate {
            node_id: *provider.node_id.as_bytes(),
            rate_per_mb: provider.rate_per_mb,
            rtt_ms: provider.rtt_ms,
            reputation: combined_reputation(deps, pk, now_secs),
            region,
            stake: None,
        });
    }
    if candidates.is_empty() {
        return None;
    }
    Some(rank(candidates))
}
```

- [ ] **Step 4: Restructure `Origin::fetch`** (replaces `node_origin.rs:798-824`)

```rust
            let hash_bytes = *hash.as_bytes();
            let target = DhtHash::from_bytes(hash_bytes);
            // ONE budget for the whole fetch, spent across both phases — see
            // `PullOutcome`.
            let mut budget = MAX_PROVIDER_ATTEMPTS;
            // `node_pull_attempts` counts pull ORCHESTRATIONS ("found ≥1 candidate
            // to try"), and is the denominator for the success / corruption /
            // unreachable rates. One `fetch` is one orchestration however many
            // candidate lists it walks, so a probe-cache hit that exhausts its
            // providers and falls through to the cold path must still meter
            // exactly once — otherwise every such fetch inflates the denominator
            // and quietly deflates every rate built on it.
            let mut attempt_metered = false;

            // ADR 001 §Probe cache: "On a cache miss the requester checks the probe
            // cache first; if a valid entry exists, it skips DHT lookup and goes
            // straight to selection."
            if let Some(cached) = cached_candidates(deps, target).await {
                deps.metrics.probe_cache_hit();
                deps.metrics.node_pull_attempt();
                attempt_metered = true;
                let outcome = try_pull(deps, &cached, hash_bytes, budget).await;
                if let Some(bytes) = outcome.bytes {
                    return Ok(OriginFetch::found_one_shot(bytes));
                }
                budget = budget.saturating_sub(outcome.attempts);
                // Every cached provider failed to deliver. The entry has been
                // disproved by the only evidence that outranks a probe — an actual
                // pull — so drop it rather than let it keep hitting for the rest of
                // its 15s.
                deps.probe_cache.invalidate(&target);
                if budget == 0 {
                    // The cached candidates ate the whole fetch-wide budget.
                    // Running a fresh lookup + probe now would either exceed the
                    // worst case `outer_pull_deadline` is sized for, or discover
                    // providers it has no attempts left to try. This fetch is a
                    // clean miss; the entry is gone, so the next one goes cold.
                    debug!(%hash, "node-origin: probe-cache candidates exhausted the attempt budget");
                    return Ok(OriginFetch::NotFound);
                }
            } else {
                deps.metrics.probe_cache_miss();
            }

            // ADR 001 §Probe cache: "if all fail, run a fresh DHT lookup + probe."
            let providers = discover(deps, hash_bytes).await;
            if providers.is_empty() {
                deps.metrics.node_pull_no_providers();
                debug!(%hash, "node-origin: no providers discovered for cache-miss pull");
                return Ok(OriginFetch::NotFound);
            }
            if !attempt_metered {
                deps.metrics.node_pull_attempt();
            }
            // Writes the probe cache at its tail.
            let ranked = probe_and_rank(deps, providers, hash_bytes).await;
            match try_pull(deps, &ranked, hash_bytes, budget).await.bytes {
                Some(bytes) => Ok(OriginFetch::found_one_shot(bytes)),
                // KNOWN LIMITATION (#1145 review, #1129): … [keep the existing comment block verbatim]
                None => Ok(OriginFetch::NotFound),
            }
```

`take(budget)` bounds `attempts <= budget`, so `saturating_sub` can never actually saturate. Use it anyway: a subtraction that is only correct given an invariant two functions away is not worth the character it saves.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo nextest run -p decdn-node --test node_origin_pull --no-fail-fast`
Expected: PASS — the four new tests, and all 34 existing `fetch` tests unchanged.

- [ ] **Step 6: Amend `refusal_suppression_after`'s doc and commit**

```bash
cargo clippy -p decdn-node --all-targets -- -D warnings
git add crates/node/src/node_origin.rs crates/node/tests/node_origin_pull.rs
git commit -m "feat(node): serve cache-miss pulls from the probe cache before the DHT (ADR 001)"
```

---

## Task 6: Cached-first `open_progressive_pull`

**Files:** Modify `crates/node/src/node_origin.rs`, `crates/node/tests/node_origin_pull.rs`

- [ ] **Step 1: Write the failing test**

```rust
/// The window-paced path shares the probe cache with the buffered one — it must, because it
/// shares the chokepoint that fills it (`probe_and_rank`). A path that populates the cache
/// and never reads it pays the write cost for someone else's benefit.
#[tokio::test(flavor = "multi_thread")]
async fn a_progressive_pull_reuses_a_probe_cache_entry_written_by_a_buffered_fetch()
-> Result<()> {
    // Origin::fetch, then open_progressive_pull for the same hash; assert the probe
    // counter stays at 1 and the progressive pull still delivers the payload.
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p decdn-node --test node_origin_pull a_progressive_pull_reuses`
Expected: FAIL — probe counter is 2.

- [ ] **Step 3: Restructure `open_progressive_pull`** (replaces `node_origin.rs:398-412`)

```rust
        let hash_bytes = *hash.as_bytes();
        let target = DhtHash::from_bytes(hash_bytes);
        let mut budget = MAX_PROVIDER_ATTEMPTS;
        let mut attempt_metered = false;

        if let Some(cached) = cached_candidates(deps, target).await {
            deps.metrics.probe_cache_hit();
            deps.metrics.node_pull_attempt();
            attempt_metered = true;
            let (opened, attempts) = self
                .open_from_candidates(deps, &cached, hash_bytes, budget)
                .await;
            if let Some(opened) = opened {
                return Some(opened);
            }
            budget = budget.saturating_sub(attempts);
            deps.probe_cache.invalidate(&target);
            if budget == 0 {
                debug!(%hash, "node-origin: probe-cache candidates exhausted the attempt budget");
                return None;
            }
        } else {
            deps.metrics.probe_cache_miss();
        }

        let providers = discover(deps, hash_bytes).await;
        if providers.is_empty() {
            deps.metrics.node_pull_no_providers();
            debug!(%hash, "node-origin: no providers discovered for window-paced pull");
            return None;
        }
        if !attempt_metered {
            deps.metrics.node_pull_attempt();
        }
        let ranked = probe_and_rank(deps, providers, hash_bytes).await;
        self.open_from_candidates(deps, &ranked, hash_bytes, budget)
            .await
            .0
```

**Deliberate metering delta:** this path currently meters `node_pull_attempt()` *before* probing (on "discovery found providers"). Both paths now meter after the same check, so the cached and cold branches agree. No test asserts on `node_pull_attempts_total` from the progressive path.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo nextest run -p decdn-node --test node_origin_pull --no-fail-fast`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo clippy -p decdn-node --all-targets -- -D warnings
git add crates/node/src/node_origin.rs crates/node/tests/node_origin_pull.rs
git commit -m "feat(node): serve window-paced pulls from the probe cache too"
```

---

## Task 7: Register the new metrics in the observability appendix

**Files:** Modify `adr/appendix-observability.md`

`adr/001-network.md` and `adr/005-protocol.md` need **no edit** — both already specify this feature verbatim. Per the repo ADR convention, edits are in place with no historical/tombstone text.

- [ ] **Step 1: Add two rows** to `#### Probe Metrics (cdn/probe/v1)` (after `:115`; the table has 5 columns — `Metric | Type | Tier | Labels | Description`)

```markdown
| `decdn_probe_cache_hits_total` | Counter | R | — | Cache-miss pulls served from a live [ADR 001 § Probe cache](001-network.md#adr-001-network-topology-and-peer-mesh) entry — DHT lookup and probe fanout both skipped. With `decdn_probe_cache_misses_total` this is the hit ratio the 15 s TTL exists to buy (`probe_cache_ttl = PROBE_SLASH_WINDOW / 2`, [ADR 005 § Derived constants](005-protocol.md#adr-005-wire-protocol)); a ratio near zero means the TTL is shorter than the inter-arrival time for hot blobs and the cache is pure overhead. |
| `decdn_probe_cache_misses_total` | Counter | R | — | Cache-miss pulls that ran a fresh DHT lookup + probe. Counts an entry that was absent, expired, **or fully suppressed** (every cached provider negative-cached or wedged) — all three cost the same network work, which is what this measures. |
```

**No row** is needed in the § Canonical Metric Name Cross-Reference table (~`:302`): that table maps informal names used in prior ADRs to canonical ones, and these two are canonical from birth. `decdn_probe_post_eviction_failures_total` already has its registry row at `:86` — it was specified and never implemented, which Task 2 fixes; that is not a naming drift.

- [ ] **Step 2: Verify and commit**

```bash
pre-commit run --all-files
git add adr/appendix-observability.md
git commit -m "docs(adr): register decdn_probe_cache_{hits,misses}_total (#1165)"
```

---

## Verification

**Full gate (run before pushing — CI's clippy is `--all-targets` and lints test code):**

```bash
cargo fmt -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace --no-fail-fast   # --no-fail-fast is required on macOS (#697)
```

**End-to-end behavioural proof** — the property is "no probe on the second fetch", observed at the wire by `spawn_a_probe_counting_server`, not at the counter:

```bash
cargo nextest run -p decdn-node --test node_origin_pull probe_cache --no-fail-fast
```

**Metric proof** — `decdn_probe_post_eviction_failures_total` must actually appear on the scrape endpoint. `monitoring/grafana-dashboard.json:411` has been querying it against nothing; after Task 2 the panel reports for the first time. Confirm the counter is registered by running an anvil-e2e node and curling its metrics endpoint (rebuild both binaries first — anvil-e2e tests exec pre-built binaries, #1213 gotcha):

```bash
cargo build -p decdn-node -p decdn-cli
curl -s localhost:<metrics_port>/metrics | grep decdn_probe_
# expect: decdn_probe_cache_hits_total, decdn_probe_cache_misses_total,
#         decdn_probe_post_eviction_failures_total
```

## Risks

- **`get`'s clone-on-hit** is deliberate: `cached_candidates` awaits `region_of(..)` per provider, and holding a `std::sync::MutexGuard` across an await is `clippy::await_holding_lock`. ≤10 small `Copy` structs.
- **Task 3's blast radius.** The budget refactor touches both pull paths with no new test of its own. It is sequenced before any behaviour change specifically so the existing green suite is its oracle — do not merge it with Task 5.
- **The `DurableMiss` payload** ripples to `node_origin.rs:2235` and `:2341`. Mechanical, and the match is exhaustive by design ("a new `StreamError` must break this build"), so the compiler finds them all.
