# CLAUDE.md

## Project Overview

Decentralized CDN (deCDN) — nodes cache and serve content-addressed blobs over iroh QUIC, clients pay per-MB via off-chain USDC payment channels. Rust implementation; the initial network deployment targets tens of nodes on an Arbitrum Sepolia testnet. "PoC" in code and ADR comments refers to that network-scale milestone, not contract-surface scope — the on-chain surface ships at full production shape with governance-tunable economics from day one (see [ADR 016 § Contract Inventory](adr/016-contract-interactions.md) and [§ Tunable Economics](adr/016-contract-interactions.md#tunable-economics)).

**Status: Early implementation.** Cargo workspace with 12 crates. Two binaries (#421): `node` produces the `decdn-node` daemon with the runtime bring-up, admin RPC server, dispatch limiter, and probe handler; `cli` produces the user-facing `decdn` binary carrying `probe`, `node {peers,…}`, `key-gen`, `config {…}`, `bundle {create}`. `common` holds the shared config schema, identity loading, and AdminRpc trait + DTOs both binaries import. `protocol` has varint framing, `ProbeMessage` (ADR 013), and `NodeAnnounce` gossip types; `cache` has the pull-through engine + HTTP/filesystem origin adapters; `gossip` has the `NodeAnnounce` pub/sub service with peer table. `incentive` has implemented payment-channel, staking, and voucher logic (alloy); `reputation` has ADR-008-conformant local per-peer EWMA scoring (in-memory; local-only by design — reputation is not gossiped or aggregated across nodes, per ADR 008). Neither crate is a stub. ADRs in `adr/` remain the primary design artifacts.

See [CONTRIBUTING.md](CONTRIBUTING.md) for build commands, ADR conventions, pre-commit hooks, and development environment setup.

**ADR note:**

- **Next ADR number: 040.** File naming: `NNN-topic.md` (zero-padded 3-digit prefix). Always verify by listing `adr/` for the highest number before creating a new ADR.
- **Do not reuse numbers:** 004, 010, 027, 029, 032, 033, 034, 035 (retired or reclassified — see history below).
- **Canonical ADRs (recent):** 028, 030, 031, 036, 037, 038, 039.
- **History:**
  - 026 (tokenomics) — **Genesis Bond Credit removal:** the on-chain Genesis Bond Credit mechanism (the `CapacityBond` grant/vest/claim/forfeit surface, `GENESIS_GRANTOR_ROLE`, `PendingCredit`, the credit leg of slashing/escrow, and `claimSlashGateEpochs`) is gone. Its 5pp earmark folds back into the operational DAO Treasury (group 2, now a flat 15% — 30% at TGE, then 48-month linear); any retroactive testnet-operator recognition is a discretionary off-chain Treasury TGE-unlock with no contract surface. Slashing and granted-appeal refunds are now bond-only. Touched ADRs 026, 016, 028, 036, 008, architecture, glossary, and observability/key-rotation appendices.
  - 036 (`036-served-bytes-voting-weight.md`): supersedes voting-weight clauses of ADR 009 §Production and ADR 026 §Governance; promotes `FeeRouter.bytesPerEpoch` from analytics-only to governance-canonical, adds `windowEpochs` governable parameter, adds `slashedAtEpoch` zero-out on `CapacityBond`.
  - 035 (`035-delegator-pool.md`): retired under the work-token rewrite; archived in `adr/_history/035-delegator-pool.md`.
  - 034 (`034-gauge-boost-voting-escrow.md`): retired under the work-token rewrite; archived in `adr/_history/034-gauge-boost-voting-escrow.md`.
  - 033 (`033-safety-insurance-reserve.md`): retired under the **SafetyReserve removal** — the `SafetyReserve` contract, its 5% FeeRouter bucket, and the 30% slash-redirect are gone; slash restitution is now **escrow-on-slash** in ADR 026 §Slashing and burn (the FeeRouter split drops to 60/30/10 and the slash distribution at finality to 50 challenger / 50 burn). Archived in `adr/_history/033-safety-insurance-reserve.md`.
  - 032 (`032-safety-reserve-appeals-contract.md`): retired under the SafetyReserve removal — the slash-appeal state machine moved to the standalone `SlashAppeal` contract; the canonical surface is now ADR 028 §Contract surface. Archived in `adr/_history/032-safety-reserve-appeals-contract.md`.
  - 031 (`031-content-blacklist-appeals-contract.md`); 030 (`030-node-region-self-attestation.md`, #400).
  - 029: reclassified as `appendix-peer-table-eviction.md`.
  - 028 (`028-slashing-appeals.md`): status unlocked from "Locked-for-implementation" to "Draft" pending CapacityBond rebase.
  - 027 (Distinct-Client Diversity Gating / Delivery Receipts): collapsed into ADR 026 §3 per-operator gauge-share cap (itself now retired).
  - 010 (Multi-Token Payment Support): dropped for a single immutable USDC token set at deployment; rationale archived in `adr/_history/alternatives-pre-launch.md`.
  - 006/020/021/023/025: demoted to appendices in a pre-launch cleanup (`appendix-encrypted-content-publishing.md`, `appendix-observability.md`, `appendix-l2-deployment.md`, `appendix-poc-production-seams.md`, `appendix-local-admin-http.md`).
  - 004 (tokenomics): superseded by ADR 026, which was rewritten to the work-token model and again to the no-emission variant (App Incentives in place of OperatorEmissions; the interim Genesis Bond Credits mechanism was later removed — see the ADR 026 note above).

## Common Commands

```bash
cargo build && cargo clippy          # build + lint (clippy is the usual CI failure)
cargo nextest run                    # test (preferred over cargo test)
cargo nextest run -p decdn-protocol  # single crate
cargo fmt -- --check                 # check formatting
cargo deny check                     # license + advisory audit
pre-commit run --all-files           # run all hooks
# Contracts — mirror what CI runs (see § Solidity CI gotchas below).
(cd contracts && forge fmt --check && FOUNDRY_PROFILE=ci forge build --sizes --deny warnings && forge test)
(cd contracts && aderyn -o /tmp/aderyn.md --no-snippets --skip-update-check)  # fail-on: high
(cd contracts && slither . --config-file slither.config.json)                 # fail-on: medium
```

Full Solidity workflow (static analysis, coverage, gas snapshots) lives in [CONTRIBUTING.md § Solidity development](CONTRIBUTING.md#solidity-development).

### Solidity CI gotchas

CI (`.github/workflows/ci.yml`) runs Solidity jobs that fail on subtler warnings than local `forge test`. Reproduce locally by running the exact commands above before pushing.

- **`--deny warnings` is fatal under `FOUNDRY_PROFILE=ci`.** Includes both solc warnings AND forge-lint warnings. The most common solc trap is **W5740 (unreachable code) in OZ `ReentrancyGuard._nonReentrantAfter`** when a `nonReentrant` function body always reverts. Either drop `nonReentrant` on always-reverting stubs, or make the call go through a TRULY-abstract function (no body) so solc can't propagate the revert. Virtual hooks with concrete bodies are NOT enough — solc inlines them at compile time.
- **Forge-lint warnings** (`unsafe-typecast`, `erc20-unchecked-transfer`) are suppressed globally in `contracts/foundry.toml` `[lint] exclude_lints = [...]` because per-line waivers would be ~60 sites. The `bytes32("literal")` event-key casts and test-only ERC20 transfers are safe by construction.
- **Slither directive placement matters.** `// slither-disable-next-line <detector>` must be the **immediate predecessor** of the target line — comments in between break the targeting. For `unused-return` on tuple destructuring, slither attributes the finding to the **enclosing function**, so the directive goes above the `function` declaration, not the call site.
- **Aderyn directives** (`// aderyn-ignore-next-line(<detector>)`) likewise need to be the immediate predecessor. Detector names live in `aderyn registry`.
- **`forge fmt` vs `solhint` 120-char rule** can disagree by ±1 char on named-args revert calls. Positional args (`revert ParamOutOfBounds(value, floor, ceiling)`) are the safe tie-breaker.
- **`emit` before `revert`** in always-reverting functions lets solc classify them as state-mutating (avoids the "function state mutability can be restricted to view" warning, which is also fatal under `--deny warnings`).
- **Pre-commit hooks** (`pre-commit run --all-files`) re-run forge-fmt and solhint. If a hook auto-fixes, the commit aborts and you re-stage; CI's `forge fmt --check` then passes.

The fail thresholds for the static-analysis jobs are set in their respective configs: aderyn uses `fail-on: high` via the GitHub Action input; slither uses `fail_on: medium` in `contracts/slither.config.json`.

## Architecture

**Language:** Rust (edition 2024, MSRV 1.95). **Networking:** iroh (QUIC transport, NAT traversal, content-addressed blobs, gossip).

**Code style:** `rustfmt.toml` sets `max_width = 100`.

**Anti-panic policy:** Clippy denies `unwrap_used`, `expect_used`, `panic`, and `indexing_slicing` workspace-wide. Use `Result`/`Option` combinators or `.get()` for indexing. This is the most common CI failure for new code.

### Crate Structure

```
crates/
  node/         — daemon binary `decdn-node`: runtime bring-up, handlers, admin RPC server, dispatch limiter
  cli/          — user CLI binary `decdn`: probe, node admin, key-gen, config, bundle
  common/       — shared types: config schema + resolver, identity loading, AdminRpc trait + DTOs
  protocol/     — shared types, wire format, ALPN message definitions (leaf crate, minimal deps)
  config-types/ — config-vocabulary value types (RetryPolicy, DecompressMode, OriginUrl, OriginKind, Hash, PinnedHashes) shared by cache + common (leaf crate: serde + url only, no iroh-blobs / no AWS — #578)
  bao-range/    — iroh-blobs-free bao verified-range helpers (ADR 038): chunk-group alignment, range encode/verify against an untrusted `{H}.obao4` pre-order outboard. Builds on `bao-tree` rather than `iroh-blobs`, which is what keeps the CLI pull path iroh-blobs-free (#823, #915, #578)
  cache/        — cache engine wrapping iroh-blobs + origin pull-through
  client-pull/  — reusable `cdn/client/v1` paid-pull requester (`stream_fetch`) + buyer-side channel open: signs the request, verifies the signed `StreamResponse`, pays cumulative vouchers at each interval, assembles the blob. Shared by `node` (node-to-node miss pulls, #317) and `cli` (client fetch / bundle pull)
  gossip/       — NodeAnnounce pub/sub over iroh-gossip, peer table, envelope validation
  incentive/    — payment channels, staking, vouchers (alloy for Ethereum)
  reputation/   — reputation scoring (ADR 008): local per-peer EWMA only; no gossip aggregation
  e2e/          — test-only (`publish = false`) cross-layer Rust↔contract fixtures (#1028): `ChainFixture` (anvil + the production `DeployProtocol` script), `NodeFixture` (daemon subprocess + admin RPC), `ClientFixture` (real paid client path). Test targets are gated behind the `anvil-e2e` feature
contracts/      — Solidity contracts + Foundry (repo root, excluded from workspace; ships Token, CapacityBond, FeeRouter, PaymentChannel, SlashAppeal, SlashJudge, OriginAssignment, BuybackBurner, ContentBlacklist, PublisherRegistry, DecdnGovernor with test suites, plus Ed25519Verifier + BondMath helpers)
```

**Dependency flow** (normal deps; `→` reads "depends on"): `node → cache, client-pull, gossip, incentive, reputation, protocol, common`; `cli → client-pull, incentive, common, protocol`; `client-pull → incentive, common, protocol, bao-range`; `incentive → common, protocol`; `cache → config-types, protocol, bao-range`; `common → config-types, protocol` (no longer `→ cache`, #578); `gossip → protocol`; `reputation → protocol`. Three true leaves — `protocol`, `config-types`, `bao-range` — so the publisher CLI links no blob store / AWS SDK. `e2e` depends on most of the graph and nothing depends on it; likewise nothing depends on `cli`. Both are sinks.

`client-pull` is the shared paid-fetch requester, and its edge into `node` is the one worth internalizing: **the daemon is itself a paying client on its upstream cache-miss leg**, so `node` takes `decdn-client-pull` as a normal dependency and re-exports it as `client_requester` (`crates/node/src/lib.rs:19`) to preserve pre-split call-site paths.

The two binaries share `common` for config schema, identity, and admin wire types — see [`adr/appendix-binaries.md`](adr/appendix-binaries.md) for the dockerd-style split rationale. Cache and incentive are independent — `cache` works without payment logic (useful for testing/local dev); the paid path lives in `client-pull` instead. The only cycle-shaped edges are dev-only: `cli` dev-depends on `node`, `cache`, `gossip`, and `incentive`, while no library depends on `cli`.

### Wire Protocols (Core CDN)

| ALPN | Purpose |
|------|---------|
| `cdn/probe/v1` | Latency + availability probing |
| `cdn/client/v1` | All paid delivery (client→node and node→node) |
| `cdn/dht/v1` | Content discovery via Kademlia DHT (see ADR 022) |
| iroh-gossip (built-in) | Node metadata broadcast (`NodeAnnounce`), node discovery |

### Key Design Decisions

- Content is BLAKE3-addressed; clients verify hashes on received bytes
- No external origin URLs are ever exposed — origin backends (S3/R2/B2) are opaque per-node config
- All byte transfers are paid, including node-to-node cache-miss pulls
- TOKEN for staking/governance, USDC for payments (dual-currency model)
- Domain crates (`cache`, `gossip`, etc.) are "leaf" — no mode branching or `#[cfg(feature = "poc")]`. The `node` crate's wiring layer selects backends/implementations. See [adr/appendix-poc-production-seams.md](adr/appendix-poc-production-seams.md) for the full Rust implementation pattern.
- ADRs in `adr/` document all major decisions; `adr/architecture.md` is the living overview
