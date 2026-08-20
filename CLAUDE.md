# CLAUDE.md

## Project Overview

Decentralized CDN (deCDN) — nodes cache and serve content-addressed blobs over iroh QUIC, clients pay per-MB via off-chain USDC shared payment pools. Rust implementation; the initial network deployment targets tens of nodes on an Arbitrum Sepolia testnet. "PoC" in code and ADR comments refers to that network-scale milestone, not contract-surface scope — the on-chain surface ships at full production shape with governance-tunable economics from day one (see [ADR 016 § Contract Inventory](adr/016-contract-interactions.md) and [§ Tunable Economics](adr/016-contract-interactions.md#tunable-economics)).

**Status: Early implementation.** Cargo workspace with 11 crates and two binaries (#421): the `node` crate builds the `decdn-node` daemon (runtime bring-up, admin RPC server, dispatch limiter, probe handler); the `cli` crate builds the user-facing `decdn` binary (`probe`, `node {health,region-stats,drain,evict,reload}`, `key-gen`, `config {…}`, `bundle {create}`). See the [Crate Structure](#crate-structure) section for what each crate owns. No crate is a stub.

**Pre-launch: wire-breaking changes are fine.** Nothing is deployed and there are no live peers. Do not add backward-compatibility shims, version negotiation, dual-format readers, or migration paths for wire, postcard, ABI, config, or storage changes. Change the format, update every side in the same PR, and delete the old shape. Compatibility work only becomes real after the first public deployment.

See [CONTRIBUTING.md](CONTRIBUTING.md) for build commands, ADR conventions, pre-commit hooks, and development environment setup.

**Present-tense canon, never changelog voice.** Docstrings, comments, and ADRs describe current behavior as it is. No "replaced the old X", "used to…", "re-homed from", dates, or "supersedes". Every line must stand alone read cold; history belongs in git, not the source.

**Working specs stay out of the repo.** Write specs to a scratch location while you work; never commit them. Repos store no historical specs.

**ADRs.** ADRs live in `adr/`. They are the primary design artifacts. `adr/architecture.md` is the living overview. See [CONTRIBUTING.md](CONTRIBUTING.md) for ADR conventions.

- The next ADR number is 040. Name each file `NNN-topic.md`. Use a 3-digit prefix.
- List `adr/` and find the highest number before you make a new ADR. Do not trust this note for the current number.
- Do not use these numbers again: 004, 006, 010, 015, 027, 029, 031, 032, 033, 034, 035. Each one is retired or reclassified.
- Retired ADRs move to `adr/_history/`. Read the file there if you need the history. Do not add the history to this file.
- Write ADRs in ASD-STE100 Simplified Technical English: short sentences, active voice, present tense, one idea per sentence. Every ADR follows this. Keep new ADRs and edits the same.

## Common Commands

```bash
cargo build && cargo clippy          # build + lint (clippy is the usual CI failure)
cargo nextest run                    # test (preferred over cargo test)
cargo nextest run -p decdn-protocol  # single crate
cargo fmt -- --check                 # check formatting
cargo deny check                     # license + advisory audit
pre-commit run --all-files           # run all hooks
# Contracts — mirror what CI runs.
(cd contracts && forge fmt --check && FOUNDRY_PROFILE=ci forge build --sizes --deny warnings && forge test)
(cd contracts && aderyn -o /tmp/aderyn.md --no-snippets --skip-update-check)  # fail-on: high
(cd contracts && slither . --config-file slither.config.json)                 # fail-on: medium
```

Full Solidity workflow, CI gotchas, static analysis, coverage, and gas snapshots live in [CONTRIBUTING.md § Solidity development](CONTRIBUTING.md#solidity-development). CI fails on warnings that local `forge test` ignores, so reproduce with the `FOUNDRY_PROFILE=ci` commands before pushing.

## Architecture

**Language:** Rust (edition 2024, MSRV 1.95). **Networking:** iroh (QUIC transport, NAT traversal, content-addressed blobs).

**Code style:** `rustfmt.toml` sets `max_width = 100`.

**Anti-panic policy:** Clippy denies `unwrap_used`, `expect_used`, `panic`, and `indexing_slicing` workspace-wide. Use `Result`/`Option` combinators or `.get()` for indexing. This is the most common CI failure for new code.

### Crate Structure

```
crates/
  node/         — daemon binary `decdn-node`: runtime bring-up, handlers, admin RPC server, dispatch limiter
  cli/          — user CLI binary `decdn`: probe, node admin, key-gen, config, bundle
  common/       — shared types: config schema + resolver, identity loading, AdminRpc trait + DTOs
  protocol/     — shared types, wire format, ALPN message definitions (leaf crate, minimal deps)
  config-types/ — config-vocabulary value types (RetryPolicy, DecompressMode, OriginUrl, OriginKind, Hash, PinnedHashes) shared by cache + common (leaf crate: serde + url + anyhow, no iroh-blobs / no AWS — #578)
  bao-range/    — iroh-blobs-free bao verified-range helpers (ADR 038): chunk-group alignment, range encode/verify against an untrusted `{H}.obao4` pre-order outboard. Builds on `bao-tree` rather than `iroh-blobs`, which is what keeps the CLI pull path iroh-blobs-free (#823, #915, #578)
  cache/        — cache engine wrapping iroh-blobs + origin pull-through
  client-pull/  — reusable `cdn/client/v1` paid-pull requester (`stream_fetch`) + buyer-side channel open: signs the request, verifies the signed `StreamResponse`, pays cumulative vouchers at each interval, assembles the blob. Shared by `node` (node-to-node miss pulls, #317) and `cli` (client fetch / bundle pull)
  incentive/    — shared payment pools, staking, vouchers (alloy for Ethereum)
  reputation/   — reputation scoring (ADR 008): local per-peer EWMA only; no cross-node aggregation
  e2e/          — test-only (`publish = false`) cross-layer Rust↔contract fixtures (#1028): `ChainFixture` (anvil + the production `DeployProtocol` script), `NodeFixture` (daemon subprocess + admin RPC), `ClientFixture` (real paid client path). Test targets are gated behind the `anvil-e2e` feature
contracts/      — Solidity contracts + Foundry (repo root, excluded from workspace; ships Token, CapacityBond, FeeRouter, PaymentPool, SlashAppeal, SlashJudge, OriginAssignment, BuybackBurner, ContentBlacklist, PublisherRegistry, DecdnGovernor with test suites, plus Ed25519Verifier + BondMath helpers)
```

**Dependency flow** (normal deps; `→` reads "depends on"): `node → cache, client-pull, incentive, reputation, protocol, common`; `cli → client-pull, incentive, common, protocol`; `client-pull → incentive, common, protocol, bao-range`; `incentive → common, protocol`; `cache → config-types, protocol, bao-range`; `common → config-types, protocol` (no longer `→ cache`, #578); `reputation → protocol`. Three true leaves — `protocol`, `config-types`, `bao-range` — so the publisher CLI links no blob store / AWS SDK. `e2e` depends on most of the graph and nothing depends on it; likewise nothing depends on `cli`. Both are sinks.

`client-pull` is the shared paid-fetch requester, and its edge into `node` is the one worth internalizing: **the daemon is itself a paying client on its upstream cache-miss leg**, so `node` takes `decdn-client-pull` as a normal dependency and re-exports it as `client_requester` (`crates/node/src/lib.rs:19`) to preserve pre-split call-site paths.

The two binaries share `common` for config schema, identity, and admin wire types — see [`adr/appendix-binaries.md`](adr/appendix-binaries.md) for the dockerd-style split rationale. Cache and incentive are independent — `cache` works without payment logic (useful for testing/local dev); the paid path lives in `client-pull` instead. The only cycle-shaped edges are dev-only: `cli` dev-depends on `node`, `cache`, and `incentive`, while no library depends on `cli`.

### Wire Protocols (Core CDN)

| ALPN | Purpose |
|------|---------|
| `cdn/probe/v1` | Latency + availability probing |
| `cdn/client/v1` | All paid delivery (client→node and node→node) |
| `cdn/dht/v1` | Content discovery via Kademlia DHT (see ADR 022) |

### Key Design Decisions

- Content is BLAKE3-addressed; clients verify hashes on received bytes
- No external origin URLs are ever exposed — origin backends (S3/R2/B2) are opaque per-node config
- All byte transfers are paid, including node-to-node cache-miss pulls
- TOKEN for staking/governance, USDC for payments (dual-currency model)
- Payment is content-agnostic by design — no on-chain content gates. Compliance is enforced via blacklist + bounty-slash, not by gating delivery on content.
- "Unnecessary at tens of nodes but earns its keep at scale" is not a valid reason to drop a feature. Drop-cases must hold at all scales.
- Domain crates (`cache`, `reputation`, etc.) are "leaf" — no mode branching or `#[cfg(feature = "poc")]`. The `node` crate's wiring layer selects backends/implementations. See [adr/appendix-poc-production-seams.md](adr/appendix-poc-production-seams.md) for the full Rust implementation pattern.
