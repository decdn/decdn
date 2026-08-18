//! Per-journey overall-timeout ceilings, anchored to measurement (issue #1620).
//!
//! # The rule (copy this reasoning, not a nearby number)
//!
//! A journey's overall ceiling is NOT derived from other timeouts. Pick the tier
//! whose budget both:
//!
//! 1. clears this journey's observed p100 runtime with a CI-contention margin, and
//! 2. contains the fixture's largest *reachable in-test retry ladder*, so an
//!    exhausted ladder surfaces its own specific message (e.g. "deploy failed
//!    after N attempts") before this opaque ceiling fires (#785).
//!
//! The binding ladder is the deploy retry ladder in [`crate::chain`]: CI worst
//! case `DEPLOY_ATTEMPTS * ci_scaled(DEPLOY_TIMEOUT)` = `2 * 120s = 240s`, so
//! every tier here clears 240s. The deploy runs once per test run, not per
//! journey (see `ensure_shared_deployment` in [`crate::chain`]), but a journey that races
//! that one deploy still pays the ladder inside its own ceiling — either as the
//! lock winner running it or as a sibling blocked on the lock for its duration —
//! so the containment requirement is unchanged.
//!
//! `forge build` is NOT part of that binding ladder *under CI*. Every journey's
//! [`crate::chain::ChainFixture::launch`] still calls `forge_build`, but the
//! GitHub Actions jobs hoist a one-time `forge build` step
//! (`.github/workflows/ci.yml`) that warms `contracts/out` first, so the in-test
//! build is a sub-second incremental no-op there. The same holds for a local run
//! with already-warm artifacts. Only a *cold local* run pays the full build
//! ladder inside a per-test ceiling — which is fine, because a local run has no
//! job-level kill racing the per-test message; the containment guarantee this
//! rule protects is a CI property. Do not read the tiers as containing a cold
//! `forge build` on a fresh checkout.
//!
//! # Measurement (source: issue #1620, runs of 2026-08-05)
//!
//! `anvil e2e (journeys)` on `main`, four consecutive runs: 5m23s / 5m40s /
//! 6m00s / 8m37s. Full suite locally at `--test-threads 1`: 377s for 36 tests.
//! Only two journeys exceed 25s — `origin_stream_while_store` corrupt-outboard
//! (94s, a real pull-deadline wait) and `cli_publish` (72s, watcher-convergence
//! bound); every other journey is <= 23s.
//!
//! # The two tiers
//!
//! - [`STANDARD`] (300s) — the default. ~3x the 94s p100, and it contains the
//!   240s deploy ladder plus one slow poll under 4-vCPU / `--test-threads 2`
//!   contention.
//! - [`HEAVY`] (600s) — journeys whose *internal* poll ladder is long enough
//!   that a slow poll under contention could crowd the deploy ladder inside
//!   300s: a sequential poll budget above ~150s, i.e. more than half the
//!   standard tier. Today that is `g_gov_02` (a 120+60+120+60s repricing walk),
//!   `origin_blacklist_compliance` (repeated 180s catch-up polls), `slash_appeal`
//!   (120+30+30s), and the `g_node_07` Ethereum leg (five sequential CLI
//!   invocations across the unbonding window).
//!
//! When you add a journey, pick a tier by the rule above. Do not copy `STANDARD`
//! because a neighbouring test uses it — confirm its poll ladder stays under
//! ~150s first.

use std::time::Duration;

/// Default per-journey overall ceiling. See the [module rule](self).
pub const STANDARD: Duration = Duration::from_secs(300);

/// Heavier per-journey ceiling for long internal poll ladders. See the
/// [module rule](self).
pub const HEAVY: Duration = Duration::from_secs(600);
