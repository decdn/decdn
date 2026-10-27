# Appendix: deCDN Binaries — `decdn-node` + `decdn` split

**Date:** 2026-05-06
**Status:** Accepted

## Context

A deCDN deployment has two unrelated jobs:

- run a long-lived cache-node daemon (lives in containers, runs under
  systemd, reachable on QUIC `:4433` and metrics `:9090`), and
- run one-shot human commands an operator or publisher types in a
  terminal — `probe`, `get`, `publish`, `pool`,
  `node status`, `key-gen`, `config validate`.

Threat surface, dependency footprint, and update cadence differ
between the two. A single fused binary would force every operator
deployment to ship publisher tooling, every publisher install to drag
in the daemon's runtime, and contaminate features like
`decdn get` ([ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model)) with a "which
binary owns this?" question absent from `dockerd` + `docker`,
`kubelet` + `kubectl`, or `containerd` + `nerdctl`.

## Decision

Adopt the dockerd shape. Two binaries, one shared support crate:

- **`decdn-node`** (`crates/node`, package `decdn-node`) — daemon only.
  The single subcommand is `decdn-node run [--config <path>]`. Lean,
  easy to containerise, minimal supply-chain surface.
- **`decdn`** (`crates/cli`, package `decdn-cli`) — single
  human-facing CLI. Carries every command anyone types: `probe`,
  `get`, `gateway`, `dashboard`, `origin import`, `publish`, `pool`, `appeal`,
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
OTLP `service.name` in `crates/node/src/commands/otlp.rs` both read
`decdn` (not `decdn-node`) for dashboard and alert continuity. A
reviewer asking "shouldn't `decdn_*` be `decdn_node_*`?" should
consult this appendix and the reference alert rules and dashboards in
[`decdn/devops`](https://github.com/decdn/devops/tree/main/charts/decdn-node/files/monitoring) — those rules match `decdn_*`
and would silently miss data on a prefix rename.

## Dashboard

`decdn dashboard` serves one web page on a loopback address. The default
address is `127.0.0.1:8485`. The page shows what this host does with deCDN.
It has three sections:

| Section | Source | Shown |
|---------|--------|-------|
| Pulls | The record each `decdn get` run writes in `~/.decdn/activity/pulls/`. | Always. |
| Gateway | `GET /_status` of the gateway at `--gateway` ([Gateway § State](appendix-gateway.md#state)). The default is `http://127.0.0.1:8484`. | When the gateway answers. |
| Node | The admin RPC and `/metrics` of the node, found as `decdn node status` and `decdn node top` find them. | When the node answers. |

The page reads nothing from another origin. The dashboard process reads
every source. The page reads only the dashboard's own `GET /_status`. Thus
the gateway grants no cross-origin read, and a browser never reaches the
node's admin port, which has no authentication. A loopback gateway's
`/_status` needs the gateway token. The dashboard reads it from
`--gateway-token-file`, by default `~/.decdn/gateway/token`. A gateway on
another host is read through a tunnel to its `--metrics-listen` address.
The tunnel keeps the same port on both ends, because the listener refuses a
`Host` that is not its own.

The dashboard reads each source at a fixed rate, whatever the number of
pages open. It reads the pull records every second and the gateway every 2
seconds. It reads the node's health, status and `/metrics` every 2
seconds. It reads the node's lanes, earnings, buyer pools and slashes every
15 seconds, because `lanes` can read the chain and `earnings` reads the
whole settlement ledger. Each node read has its own result. A read that the
node does not support (no slash detection, no buyer pool, no settlement
ledger, no DHT) shows as not configured, not as an error.

Each `decdn get` run writes one JSON file for itself, named
`<start in Unix ms>-<pid>.json`. The record holds the URI, the destination,
the state, the bytes verified, what the run paid to each node and, at the
end, the result. The run writes the whole file again every second, through a
temporary file and a rename. Thus a reader never sees a partial record. The
directory is the same for every `--data-dir`, so one page shows every run on
the host. A run that cannot write its record continues the pull. A run that
exits before its last write leaves a `running` record. The dashboard shows a
`running` record that is older than 10 seconds as a run that stopped without
a summary. Each run deletes the oldest records when it starts, and keeps 200.
A `--dry-run` writes no record.

The dashboard's `/_status` needs the dashboard's own token in
`x-decdn-token`. The token is in `~/.decdn/dashboard/token`. The page shows
earnings and pool balances, so another local user must not read it. The
token reaches the page in the fragment of the link that `decdn dashboard`
prints: `http://<listen>/#token=<token>`. A browser does not send a fragment
to the server. The page moves the token to session storage and removes the
fragment from the address. The dashboard answers only its own loopback
names in `Host`, and refuses every other `Host` with 403. It answers only
`GET`. It sends no CORS headers. The dashboard only reads: no route changes
a source.

The page loads its script and its style from the dashboard only. It holds
no inline script and no inline style. Its content security policy is
`default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self';
img-src 'self' data:; base-uri 'none'; form-action 'none';
frame-ancestors 'none'`.

The page is a Vite app in `crates/cli/dashboard/`. Its build is three files:
`index.html`, `app.js` and `app.css`. The build script of `decdn-cli` embeds
them in the binary. A build with no Node toolchain embeds a short page that
tells how to build the dashboard. Thus `cargo build` works without Node. A
release never ships that page: the release workflow builds the dashboard
first and refuses a `decdn` binary that holds it.

## Distribution

- **Container images.** Four stages of one Dockerfile build the
  container images:
  - `decdn-node` runs the daemon. It also contains the `decdn` CLI.
    An operator uses `docker exec <container> decdn node …` to reach
    the loopback admin port.
  - `decdn` runs the CLI only. Publishers and clients use it.
  - `sponsord` and `sponsord-onramp` run the sponsorship daemon and
    its public onramp (`crates/sponsord`, `crates/sponsord-onramp`).
    A publisher who pays for their users' downloads runs them. The
    daemon is built on `decdn-client` and `decdn-incentive` through
    `sponsord-core`; the onramp on the `sponsord-api` wire types
    alone. Neither depends on the node daemon's crates.

  Each image copies its binaries out of the Linux release archives.
  It does not compile from source. Each binary in an image is
  identical to the archived binary. The signed `SHA256SUMS` covers
  the archive. It therefore also covers the binary in the image.
- **Release archives** ship the node binaries as separate tarballs per
  target (Linux x86_64 + aarch64, macOS x86_64 + aarch64, Windows
  x86_64; `decdn` also on Windows aarch64):
  `decdn-node-${VERSION}-${TARGET}.tar.gz` (operators) and
  `decdn-${VERSION}-${TARGET}.tar.gz` (publishers). The two sponsord
  servers ship for the two Linux targets only, as
  `sponsord-${VERSION}-${TARGET}.tar.gz` and
  `sponsord-onramp-${VERSION}-${TARGET}.tar.gz`.
- **Platform floor.** The Linux binaries link glibc dynamically. Both
  Linux targets build with `cross`, which sets the floor at glibc 2.31
  (Debian 11, Ubuntu 20.04, RHEL 8). See
  [ADR 000](000-language.md#consequences).
- **`cargo install --path crates/cli`** and
  **`cargo install --path crates/node`** both work standalone.
- **Self-update channel.** `decdn self-update` replaces the running
  `decdn` binary with the latest release. The release carries
  `SHA256SUMS.minisig`, a minisign signature over the same manifest
  the GPG signature covers. The binary embeds the maintainers' public
  keys, committed as `crates/cli/minisign-<maintainer>.pub`, and
  refuses a release that no embedded key verifies. It then checks the
  archive against the manifest, unpacks it, and swaps the new binary
  in for the old one.
  It replaces only a binary the user can write and that
  `cargo install` did not place. It never applies a downgrade or a
  pre-release. Once a day, after a command succeeds on a terminal,
  `decdn` prints one line when a newer release exists;
  `[self_update] check` and `DECDN_SELF_UPDATE_CHECK` turn that off.
  `decdn-node` has no self-update: operators deploy it through
  their own tooling.

## Cross-ADR Impact

- `decdn get` ([ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model)) lands directly
  on `decdn`; no binary-placement question remains for it.
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
  Regression guard: `.github/scripts/check_crate_edges.py` fails CI when
  the normal or build closure of `decdn-cli` or of the `decdn-client` SDK
  contains `iroh-blobs` or an AWS SDK crate. Dev-dependencies still pull
  them for the integration tests. That does not affect the release binary.
