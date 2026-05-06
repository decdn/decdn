# Appendix: deCDN Binaries — `decdn-node` + `decdn` split

**Date:** 2026-05-06
**Status:** Accepted (issue #421)

## Context

The pre-#421 single `decdn` binary fused two unrelated jobs:

- a long-lived cache-node daemon (runs in containers, lives under
  systemd, reachable on QUIC `:4433` and metrics `:9090`), and
- a one-shot human-facing CLI that an operator or publisher types in a
  terminal (`probe`, `node peers`, `key-gen`, `config validate`, and
  the future `pull`, `bundle …`, `fetch`, `publish`, `channel`,
  `wallet` commands).

Threat surface, dependency footprint, and update cadence are different
for each. The fused shape forced every operator deployment to ship
publisher tooling, every publisher install to drag in the daemon's
runtime, and contaminated upcoming work like `decdn bundle …` (#391)
and `decdn pull` (ADR 012) with a "which binary owns this?" question
that doesn't exist in `dockerd` + `docker`, `kubelet` + `kubectl`, or
`containerd` + `nerdctl`.

## Decision

Adopt the dockerd shape. Two binaries, one shared support crate:

- **`decdn-node`** (`crates/node`, package `decdn-node`) — daemon only.
  The single subcommand is `decdn-node run [--config <path>]`. Lean,
  easy to containerise, minimal supply-chain surface.
- **`decdn`** (`crates/cli`, package `decdn-cli`) — single
  human-facing CLI. Carries every command anyone types: `probe`,
  `node {peers,health,announce,drain,evict,reload}`, `key-gen`,
  `config {init,validate}`, and the deferred `pull`, `bundle …`,
  `fetch`, `publish`, `channel`, `wallet`.
- **`decdn-common`** (`crates/common`) — shared types both binaries
  need: the TOML config schema and resolver, identity loading, the
  `AdminRpc` trait + DTOs, and clap argument structs. No runtime,
  no peer table, no cache engine.

The pre-#421 `decdn run …` binary form does not survive. The cut-over
is hard: `decdn run` errors with a redirect to `decdn-node run`; there
is no compatibility shim. Release notes call it out under the
`BREAKING CHANGE:` footer.

### Why the `node` admin namespace lives on `decdn`, not `decdn-node`

Operator-local admin (`peers`, `health`, `announce`, `drain`, `evict`,
`reload`) is loopback-HTTP-only per the [ADR 025 admin appendix](appendix-local-admin-http.md):
the binary that runs the commands does not have to be the daemon, just
running on the daemon's host. That makes the `node` namespace a
natural fit for the user CLI: operators don't need to remember which
binary owns it, and the daemon binary stays focused on starting up.
It also reads symmetrically: `decdn node peers`, `decdn node drain`,
`decdn node evict <hash>` — `node` reads as the noun the command
operates on.

### Why a single shared `decdn-common`, not per-binary common crates

The atomic-PR refactor cost dominates. A two-crate split
(`decdn-config` + `decdn-admin-types`) would buy a tighter dep graph,
but everything in `decdn-common` is consumed by both binaries already,
so the fragmentation has no immediate payoff. The `decdn-cache`
transitive dep — pulled in for the cache-typed config fields
(`DecompressMode`, `RetryPolicy`, `OriginUrl`, `PinnedHashes`,
`Hash`) — is the largest single contributor to CLI binary size; if
`cargo bloat --release --bin decdn -n 20` shows the CLI exceeding
~50 MB stripped, the right next move is splitting the cache-typed
fields out of `decdn-common`'s `config` module rather than splitting
the crate.

### Why the metric prefix and OTLP `service.name` stay `decdn`

The Prometheus metric prefix in `crates/node/src/metrics.rs` and the
OTLP `service.name` set in `crates/node/src/commands/mod.rs` both
remain `decdn` (not `decdn-node`) for dashboard and alert continuity.
A future reviewer looking at `decdn_*` series and asking "shouldn't
this be `decdn_node_*`?" should consult this appendix and `monitoring/
prometheus-alerts.yml` / `monitoring/grafana-dashboard.json` — those
dashboards and alert rules already match `decdn_*` and would silently
miss data on a prefix rename.

## Distribution

- **Container image** ships `decdn-node` only. Daemon-only deployment
  is the dominant container use case; a publisher who wants the CLI
  in a container can rebuild from the release archive.
- **Release archives** ship both binaries as separate tarballs per
  target (Linux x86_64 + aarch64, macOS x86_64 + aarch64, Windows
  x86_64): `decdn-node-${VERSION}-${TARGET}.tar.gz` (operators) and
  `decdn-${VERSION}-${TARGET}.tar.gz` (publishers).
- **`cargo install --path crates/cli`** and
  **`cargo install --path crates/node`** both work standalone.

## Implications & follow-ups

- `decdn bundle …` (#391) lands directly on `decdn`; no decision
  about which binary owns it remains.
- `decdn pull` (ADR 012) lands on `decdn` post-split; it's no longer
  blocked on a binary-placement question.
- Shell completions and man pages are deferred — both binaries should
  generate them via `clap_complete`, but adding that to this PR would
  expand scope. Tracked as follow-up issues.
- CLI binary-size budget is tracked, not enforced. Run
  `cargo bloat --release --bin decdn -n 20` and
  `ls -lh target/release/decdn target/release/decdn-node` after each
  significant CLI change. Soft target ≤ 50 MB stripped (the
  dockerd CLI is ≈ 50 MB; `iroh` + `iroh-blobs` via `decdn-cache`
  pull in most of the weight). If exceeded, split the cache-typed
  config fields out of `decdn-common`.
