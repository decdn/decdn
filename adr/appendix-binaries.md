# Appendix: deCDN Binaries — `decdn-node` + `decdn` split

**Date:** 2026-05-06
**Status:** Accepted

## Context

A deCDN deployment has two unrelated jobs:

- run a long-lived cache-node daemon (lives in containers, runs under
  systemd, reachable on QUIC `:4433` and metrics `:9090`), and
- run one-shot human commands an operator or publisher types in a
  terminal — `probe`, `fetch`, `bundle …`, `publish`, `pool`,
  `node status`, `key-gen`, `config validate`.

Threat surface, dependency footprint, and update cadence differ
between the two. A single fused binary would force every operator
deployment to ship publisher tooling, every publisher install to drag
in the daemon's runtime, and contaminate features like
`decdn bundle …` and `decdn fetch` ([ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model)) with a "which
binary owns this?" question absent from `dockerd` + `docker`,
`kubelet` + `kubectl`, or `containerd` + `nerdctl`.

## Decision

Adopt the dockerd shape. Two binaries, one shared support crate:

- **`decdn-node`** (`crates/node`, package `decdn-node`) — daemon only.
  The single subcommand is `decdn-node run [--config <path>]`. Lean,
  easy to containerise, minimal supply-chain surface.
- **`decdn`** (`crates/cli`, package `decdn-cli`) — single
  human-facing CLI. Carries every command anyone types: `probe`,
  `fetch`, `bundle {create,pull}`, `publish`, `pool`, `appeal`,
  `setup`, `key-gen`, `config {init,validate}`, and the `node`
  admin/on-chain subcommands (health, status, lanes, evict, reload,
  drain, top, lookup, register, deregister, bond, unbond,
  rotate-key).
- **`decdn-common`** (`crates/common`) — shared types both binaries
  need: the TOML config schema and resolver, identity loading, the
  `AdminRpc` trait + DTOs, and clap argument structs. No runtime, no
  engine handles. The config-vocabulary value types it
  is built from (`DecompressMode`, `RetryPolicy`, `OriginUrl`,
  `OriginKind`, `PinnedHashes`, `Hash`) and the `Hash` returned by
  `parse_hash_arg` live in the `decdn-config-types` leaf crate
  (serde + `url` only — no iroh-blobs, no AWS SDK). `decdn-common`
  therefore does **not** depend on `decdn-cache`, so `decdn` links
  none of the blob-store/AWS weight. The daemon's `evict`
  handler converts the leaf `Hash` to the blob-store hash via
  `decdn_cache::to_store_hash` at its boundary.

There is no `decdn run` subcommand. `decdn run` errors as an
unrecognized subcommand (clap's default), with `decdn --help` listing
only the user-facing commands. There is no compatibility shim and no
friendly redirect — operators starting the daemon use `decdn-node run`.

### Why the `node` admin namespace lives on `decdn`, not `decdn-node`

Operator-local admin (`health`, `status`, `evict`, `reload`,
`drain`) is loopback-HTTP-only per the [admin appendix](appendix-local-admin-http.md#appendix-local-admin-http-surface):
the binary running the commands need not be the daemon, just on the
daemon's host. So the `node` namespace fits the user CLI naturally —
operators don't track which binary owns it, the daemon stays focused
on starting up, and it reads symmetrically (`decdn node health`,
`decdn node drain`, `decdn node evict <hash>`), with `node` as the
noun the command operates on.

### Why a single shared `decdn-common`, not per-binary common crates

`decdn-common` stays a single crate: everything in it is consumed by
both binaries, so fragmenting it has no payoff. The cache-typed
`config` value types were instead split into the `decdn-config-types`
leaf crate — extract the shared value types, not the crate — so the
CLI links no blob store.

### Why the metric prefix and OTLP `service.name` stay `decdn`

The Prometheus metric prefix in `crates/node/src/metrics.rs` and the
OTLP `service.name` in `crates/node/src/commands/mod.rs` both read
`decdn` (not `decdn-node`) for dashboard and alert continuity. A
reviewer asking "shouldn't `decdn_*` be `decdn_node_*`?" should
consult this appendix and `monitoring/prometheus-alerts.yml` /
`monitoring/grafana-dashboard.json` — those rules match `decdn_*`
and would silently miss data on a prefix rename.

## Distribution

- **Container image** ships `decdn-node` only — daemon-only is the
  dominant container use case; a publisher wanting the CLI in a
  container rebuilds from the release archive. The image copies the
  `decdn-node` binary out of the Linux release archives. It does not
  compile from source. The binary in the image is therefore identical
  to the archived one, and one signed `SHA256SUMS` covers both.
- **Release archives** ship both binaries as separate tarballs per
  target (Linux x86_64 + aarch64, macOS x86_64 + aarch64, Windows
  x86_64): `decdn-node-${VERSION}-${TARGET}.tar.gz` (operators) and
  `decdn-${VERSION}-${TARGET}.tar.gz` (publishers).
- **Platform floor.** The Linux binaries link glibc dynamically. Both
  Linux targets build with `cross`, which sets the floor at glibc 2.31
  (Debian 11, Ubuntu 20.04, RHEL 8). See
  [ADR 000](000-language.md#consequences).
- **`cargo install --path crates/cli`** and
  **`cargo install --path crates/node`** both work standalone.

## Cross-ADR Impact

- `decdn bundle …` and `decdn fetch` ([ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model)) land directly
  on `decdn`; no binary-placement question remains for either.
- Shell completions and man pages are deferred — both binaries should
  generate them via `clap_complete`. Tracked as follow-up issues.
- CLI binary-size budget is tracked, not enforced. Run
  `cargo bloat --release --bin decdn -n 20` and
  `ls -lh target/release/decdn target/release/decdn-node` after each
  significant CLI change. Soft target ≤ 50 MB stripped (the dockerd
  CLI is ≈ 50 MB). The single largest contributor — `iroh-blobs` and
  the AWS SDK reaching `decdn` via `cli → common → cache` — was
  removed by extracting `decdn-config-types`; `decdn` no
  longer links the blob store or AWS SDK at all (`reqwest`/`iroh`
  remain, via `alloy` and the direct `iroh` dep — not the cache).
  Regression guard: `cargo tree -p decdn-cli -e normal -i iroh-blobs`
  and `-i aws-sdk-s3` must be empty (dev-deps still pull them for the
  integration tests; that does not affect the release binary).
