# Contributing to deCDN

## Development Environment

deCDN builds with a native toolchain and works with any editor. Install the prerequisites below, then clone and build.

### Prerequisites

| Tool | Why | Install |
|---|---|---|
| [rustup](https://rustup.rs) | `rust-toolchain.toml` pins Rust `1.95.0` with `rustfmt` and `clippy`; a rustup-managed `cargo` installs and selects that pin on first invocation | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` |
| `cargo-nextest` | the test runner this repo and CI use instead of `cargo test` | `cargo install --locked cargo-nextest` |
| `cargo-deny` | license + advisory audit; both a pre-commit hook and a CI check; findings warn and never block, but a run that checked nothing fails | `cargo install --locked cargo-deny` |
| [pre-commit](https://pre-commit.com/) | runs the commit- and push-stage hooks | `pipx install pre-commit` |
| [Foundry](https://book.getfoundry.sh) (`forge`, `anvil`) | contract build/test/fmt hooks and the Anvil e2e journeys; CI pins `v1.7.1` | `curl -L https://foundry.paradigm.xyz \| bash` then `foundryup -i v1.7.1` |
| A C compiler (`cc`) + `make` | `ring` and `aws-lc-sys` compile C sources during `cargo build`, and rustc needs `cc` to link; `make` also drives the `adr-book-list` and `adr-ref-hygiene` hooks via `make -C adr` | system package manager (`build-essential` on Debian/Ubuntu); `cmake` is optional but lets `aws-lc-sys` skip its slower cc-only fallback |
| [`gh`](https://cli.github.com/) | issue and PR workflow | system package manager |

Pre-commit covers Solidity too: `forge fmt` and `solhint` run on every commit that touches `contracts/` (solhint needs no local install — pre-commit builds its own node sandbox), `forge build` and `forge test` run on push. The `slither` and `aderyn` static-analysis hooks are manual-stage and need their own installs — see [Solidity development](#solidity-development).

Two more tools are optional: `jq`, for the [Anvil deployment](#local-deployment-anvil) commands, and Node.js with `npm`, only if you run solhint outside pre-commit (`npm ci` in `contracts/`).

### Clone and Build

```bash
git clone --recurse-submodules git@github.com:decdn/decdn.git
cd decdn
pre-commit install --hook-type pre-commit --hook-type pre-push
cargo build
```

`--recurse-submodules` matters: `contracts/lib/` is four git submodules (forge-std, openzeppelin-contracts, solady, crypto-lib), and every `forge` command fails without them. An existing clone catches up with `git submodule update --init --recursive`.

Both hook types matter too: `.pre-commit-config.yaml` sets no `default_install_hook_types`, so a bare `pre-commit install` wires the commit stage only and the `forge build` / `forge test` push gates never fire.

The first build downloads the pinned toolchain and compiles the full dependency graph, so expect a few minutes. Later builds are incremental.

### Verify

```bash
rustc --version            # 1.95.0
cargo nextest --version
cargo deny --version
pre-commit --version
forge --version            # 1.7.1
anvil --version            # the e2e journeys need it
```

## Pre-commit Hooks

The repo uses [pre-commit](https://pre-commit.com/) to enforce formatting and linting, and to report supply chain findings, before each commit.

```bash
pre-commit install --hook-type pre-commit --hook-type pre-push   # one-time setup
pre-commit run --all-files        # run all hooks manually
```

Hooks: trailing-whitespace, end-of-file-fixer, check-yaml, check-merge-conflict, check-added-large-files, markdownlint, `cargo fmt`, `cargo clippy`, `cargo doc`, `cargo deny`, adr-book-list, adr-ref-hygiene, package-embeds, deployment-manifest-mirror, toolchain-pin, workspace-manifests, crate-edges, forge-fmt, solhint, forge-build, forge-test, slither, aderyn.

The repo-shape hooks (package-embeds, toolchain-pin, workspace-manifests, crate-edges) are stdlib-Python guards under `.github/scripts/`, each with a thin `.sh` wrapper and a pytest suite under `.github/scripts/tests/`; deployment-manifest-mirror is plain bash. They mirror the `packaging` CI job, which is authoritative. The pytest suites run in the `CI tooling tests` job, which is not part of the `CI success` gate, so a failing guard test does not block a merge — the guards themselves do. A new guard follows the same shape: a `check(...)` function the tests can drive on a fixture, a module docstring saying what breaks silently without it, a `repo: local` hook with a scoped `files:` regex, and a named step in `packaging`.

## Build and Test

```bash
cargo build && cargo clippy          # build + lint
cargo nextest run                    # test (preferred over cargo test)
cargo nextest run -p decdn-protocol   # single crate
cargo fmt -- --check                 # check formatting
cargo deny check                     # license + advisory audit (deny.toml)
```

The pre-commit hook lints only the default-feature workspace. CI's `clippy` job additionally runs
the configurations that unification hides, and they fail independently of the hook:

```bash
cargo clippy --workspace --all-targets --all-features -- -D warnings   # feature-gated targets
cargo clippy -p decdn-incentive --no-default-features -- -D warnings   # redb not linked
cargo clippy -p decdn-incentive --no-default-features \
  --features buyer-store-core -- -D warnings                           # the node's config
```

The `--all-features` run is the only one that reaches anything behind an off-by-default feature —
the `anvil-e2e` targets (journeys and the two `decdn-node` anvil tests), `public-api-test`. Run it
before pushing a change that touches gated code, or the first thing that tells you is a red CI.

### Public API snapshot

`decdn-protocol` and `decdn-client` each carry a [`public_api`](https://crates.io/crates/public_api) +
[`insta`](https://crates.io/crates/insta) snapshot test that guards the exported surface
against unintended changes: the wire types and ALPN definitions ([issue #304]), and the
client SDK ([issue #1150]). Each is
feature-gated and **not** part of `cargo nextest run --workspace`. The dedicated `public-api`
CI job runs it and **blocks the PR** on any diff.

```bash
# Run (and update) the snapshot. Required when you intentionally change the public API:
INSTA_UPDATE=always cargo nextest run -p decdn-protocol --features public-api-test
INSTA_UPDATE=always cargo nextest run -p decdn-client --features public-api-test
# or, to review interactively:
cargo insta review
```

- The `.snap` diff **must land in the same PR** as the API change — that's the whole point.
- **No nightly needed.** `public-api` parses rustdoc JSON (nominally nightly-only); the test
  emits it with the pinned **stable** toolchain via `RUSTC_BOOTSTRAP=1`, so it's deterministic
  and runs anywhere the workspace already builds. The one coupling: a deliberate toolchain
  bump can change rustdoc's JSON `format_version`, which may require bumping `public-api` and
  regenerating the `.snap` in the same PR (see the header comment in
  `crates/protocol/tests/public_api.rs`).

[issue #304]: https://github.com/decdn/decdn/issues/304
[issue #1150]: https://github.com/decdn/decdn/issues/1150

## Solidity development

Contracts live in `contracts/` and use [Foundry](https://book.getfoundry.sh). CI pins Foundry to `v1.7.1`; install a matching local toolchain via [`foundryup`](https://book.getfoundry.sh/getting-started/installation).

**Common commands (run from `contracts/`):**

```bash
forge fmt --check                       # formatting check (CI gate)
forge build --sizes --deny warnings     # build + report sizes; fail on warnings (CI gate)
forge test                              # default profile: 256 fuzz, 32-depth invariants
FOUNDRY_PROFILE=ci forge test           # 1024 fuzz, 256/50-depth (matches CI)
FOUNDRY_PROFILE=fuzz forge test         # 10k fuzz, 1024/100-depth (nightly/manual)
FOUNDRY_PROFILE=coverage forge coverage --report lcov --no-match-coverage 'test/'
FOUNDRY_PROFILE=ci forge snapshot --diff .gas-snapshot   # current gas vs committed baseline
```

**Local static analysis:**

```bash
# Solhint (lint) — uses contracts/package.json + package-lock.json
cd contracts && npm ci && npm run lint

# Slither (SAST) — requires `pip install slither-analyzer`
cd contracts && slither . --config-file slither.config.json

# Aderyn (SAST) — install pinned to the version CI uses (aderyn-v0.6.8).
# Inspect the installer before piping to bash if you don't trust the
# Cyfrin signing chain; the install URL is reproducible across runs.
#   curl --proto '=https' --tlsv1.2 -LsSf \
#     https://github.com/Cyfrin/aderyn/releases/download/aderyn-v0.6.8/aderyn-installer.sh \
#     | bash
cd contracts && aderyn .
```

The pre-commit hooks run `forge-fmt` and `solhint` on every commit; `forge-build` and `forge-test` on `git push`; `slither` and `aderyn` are manual-stage (`pre-commit run --hook-stage manual <id>`).

**Updating the gas snapshot baseline:**

The `.gas-snapshot` baseline is an **enforced gate**, not advisory. The `solidity build+test` CI
job (path-filtered to `contracts/**`) regenerates the snapshot under `FOUNDRY_PROFILE=ci` and
**fails the build if the deterministic entries differ** from the committed file. So when a
contract change moves gas, regenerate the committed snapshot in the **same PR**:

```bash
cd contracts && FOUNDRY_PROFILE=ci forge snapshot --snap .gas-snapshot
git add .gas-snapshot
```

The gate compares only the deterministic (non-fuzz) entries — lines containing `runs:`
(fuzz mean/median, invariant call counts) are stripped before the diff. Foundry's fuzz μ/~ gas
is not reproducible across machines, so those lines drift freely (and a fresh `forge snapshot`
will rewrite them); only the ~620 non-fuzz entries are byte-stable at the pinned forge + solc
version and are what the gate enforces. A sticky PR comment shows the deterministic diff on both
pass (empty) and fail (the drifted entries).

**Interpreting CI findings:**

- **Slither** fails CI on medium-and-above findings (`fail_on: medium` in `slither.config.json`, also passed as `fail-on: medium` to `crytic/slither-action`); detailed output lives in the job log. SARIF upload to code-scanning is **commented out** in `ci.yml` because the repo does not have GitHub Advanced Security enabled; re-enable the step (and the matching `security-events: write` + `actions: read` permissions) when GHAS is turned on or the repo flips public. For true positives, fix the contract. For confirmed false positives, suppress *inline* (`// slither-disable-next-line <detector>` with a comment justifying the suppression) — never expand `detectors_to_exclude` in `slither.config.json`. Detectors currently excluded globally: `naming-convention` (overlaps with solhint's name-mixedcase rules), `solc-version` and `pragma` (satisfied by the explicit `solc_version` pin in `foundry.toml`).
- **Aderyn** runs via `Cyfrin/aderyn-ci@v0.0.10` with `fail-on: high`; output lives in the job log under the action's summary.
- **Solhint** failures point at code; fix the code rather than disabling the rule. Rule changes require a separate PR with rationale. Solhint lints `contracts/src/`, `contracts/testnet/`, and `contracts/script/` (per the `lint` script in `contracts/package.json`); test files (Foundry's `test_xxx_yyy` convention) are out of scope by design.
- **Coverage** goes to GitHub Code Quality, which posts the PR coverage comment and per-file deltas against `main`. The `solidity-coverage` job converts forge's LCOV with `.github/scripts/lcov_to_cobertura.py`, because Code Quality reads Cobertura XML only; the `test` job uploads the Rust report that `cargo llvm-cov --cobertura` writes. Both uploads are best-effort and need Code Quality enabled in the repository settings.

**CI gotchas (subtler than local `forge test`):**

CI (`.github/workflows/ci.yml`) runs Solidity jobs that fail on warnings local `forge test` ignores. Reproduce them locally by running the `FOUNDRY_PROFILE=ci` commands above before pushing.

- **`--deny warnings` is fatal under `FOUNDRY_PROFILE=ci`.** It covers both solc warnings AND forge-lint warnings. The most common solc trap is **W5740 (unreachable code) in OZ `ReentrancyGuard._nonReentrantAfter`** when a `nonReentrant` function body always reverts. Either drop `nonReentrant` on always-reverting stubs, or route the call through a TRULY-abstract function (no body) so solc can't propagate the revert. Virtual hooks with concrete bodies are NOT enough — solc inlines them at compile time.
- **Forge-lint warnings** (`unsafe-typecast`, `erc20-unchecked-transfer`) are suppressed globally in `contracts/foundry.toml` `[lint] exclude_lints = [...]` because per-line waivers would be ~60 sites. The `bytes32("literal")` event-key casts and test-only ERC20 transfers are safe by construction.
- **Slither directive placement matters.** `// slither-disable-next-line <detector>` must be the **immediate predecessor** of the target line — comments in between break the targeting. For `unused-return` on tuple destructuring, slither attributes the finding to the **enclosing function**, so the directive goes above the `function` declaration, not the call site.
- **Aderyn directives** (`// aderyn-ignore-next-line(<detector>)`) likewise need to be the immediate predecessor. Detector names live in `aderyn registry`.
- **`forge fmt` vs `solhint` 120-char rule** can disagree by ±1 char on named-args revert calls. Positional args (`revert ParamOutOfBounds(value, floor, ceiling)`) are the safe tie-breaker.
- **`emit` before `revert`** in always-reverting functions lets solc classify them as state-mutating (avoids the "function state mutability can be restricted to view" warning, which is also fatal under `--deny warnings`).
- **Pre-commit hooks** re-run forge-fmt and solhint. If a hook auto-fixes, the commit aborts and you re-stage; CI's `forge fmt --check` then passes.

### Local deployment (Anvil)

`script/DeployProtocol.s.sol` deploys the full contract suite, hands all governance roles to the `TimelockController`, and writes a `deployments/<chainId>.json` manifest. The script does **not** deploy USDC — it wraps an existing token — so a local deploy first stands up the `MintableUSDC` mock from `test/mocks/`.

**One-command setup:** from `contracts/`, run [`./dev-deploy.sh`](contracts/dev-deploy.sh). It boots Anvil, deploys the USDC mock + full protocol + a funded TOKEN faucet, prints the deployed addresses, and stays in the foreground (Ctrl-C tears it all down). The manual steps below show what it does end to end.

Run everything from `contracts/`. The addresses/keys below are Anvil's deterministic defaults (accounts #0–#2); never use them anywhere but a local chain.

```bash
# 1. Start a local chain (chain id 31337). Leave running in another terminal.
anvil

# 2. Deploy the USDC mock and capture its address.
USDC=$(forge create test/mocks/MintableUSDC.sol:MintableUSDC \
  --rpc-url http://127.0.0.1:8545 \
  --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
  --broadcast --json | jq -r .deployedTo)

# 3. Deploy the protocol. The addresses are required; for local testing any EOA
#    works. EMERGENCY_MULTISIG is account #1; INITIAL_TOKEN_HOLDER is the
#    deployer (account #0, the `--sender`) so it holds the genesis TOKEN and can
#    later fund the faucet. INITIAL_VETTER is the genesis VETTER_ROLE holder on
#    ManualVettingPolicy — required, because a deploy with no vetter can vet no
#    publisher and is a governance deadlock; the deployer plays it locally. The
#    deployer must NOT be forge's default sender, so always pass `--sender`
#    explicitly. CURRENT_TERMS_HASH is any non-zero bytes32 for local testing —
#    CapacityBond rejects the zero sentinel, so an accidental terms-disabled
#    deployment is forbidden.
USDC_ADDRESS=$USDC \
EMERGENCY_MULTISIG=0x70997970C51812dc3A010C7d01b50e0d17dc79C8 \
INITIAL_TOKEN_HOLDER=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
INITIAL_VETTER=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
CURRENT_TERMS_HASH=0x0000000000000000000000000000000000000000000000000000000000000001 \
forge script script/DeployProtocol.s.sol:DeployProtocol \
  --rpc-url http://127.0.0.1:8545 \
  --sender 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
  --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
  --broadcast

# 4. Read back the deployed addresses.
jq . deployments/31337.json
```

Notes and common snags:

- **Drop `--broadcast` for a dry run** — the script simulates against the fork and prints addresses without sending transactions.
- **Required env vars:** `USDC_ADDRESS`, `EMERGENCY_MULTISIG`, `INITIAL_TOKEN_HOLDER`, `INITIAL_VETTER`, `CURRENT_TERMS_HASH`. Every economic parameter (`MIN_BOND`, `TIMELOCK_DELAY`, `FEE_ROUTER_WINDOW_EPOCHS`, the slash/blacklist bonds, …) has a production-shaped default and is overridable via env var — see `_readConfig` in `DeployProtocol.s.sol` for the full list and defaults.
- **`--sender` becomes the deployer.** The script reverts (`DeployerIsForgeDefaultSender`) if you let forge use its default sender, so always pass `--sender`. The deployer's roles are granted to the Timelock and then revoked as the final step; the script reverts if any privileged role is left on the deployer.
- **Re-running on the same chain reverts** with `ManifestAlreadyExists` (the guard fires before any gas is spent). Either `rm deployments/31337.json`, restart Anvil for a clean slate, or set `FORCE_OVERWRITE_MANIFEST=true`.
- **`BuybackBurner` is recorded as the zero address** — it ships unwired at launch (operators take 90%, treasury 10%); its 30% buyback share activates by governance once the concrete Uniswap V3 burner is deployed.
- **A real-network redeploy must be mirrored into the CLI.** `decdn config init --chain <name>` bakes contract addresses read from the manifest, embedded at compile time from `crates/cli/deployments/<chainId>.json` — a copy, because `include_str!` cannot reach outside the crate and the published `decdn-cli` would otherwise not build. `contracts/deployments/` stays canonical; after a redeploy of a chain the CLI knows about, `cp` the manifest across. The `packaging` CI job and the `deployment-manifest-mirror` pre-commit hook fail on drift, so this cannot be forgotten silently. Local chain ids (31337) are not mirrored.
- **Testnet TOKEN faucet (optional):** `script/TestnetFaucet.s.sol:DeployTestnetFaucet` funds and deploys a cooldown-gated dispenser. It reads `TOKEN_ADDRESS` (from the manifest), `TREASURY_ADDRESS` (must equal `--sender`), `FUNDING_AMOUNT`, and the `ADMIN_ADDRESS`/`GOVERNANCE_ADDRESS`/`PAUSER_ADDRESS` role holders; `CLAIM_AMOUNT` and `COOLDOWN_SECONDS` are optional.

## Rust Toolchain

`rust-toolchain.toml` pins an exact stable release (currently `1.95.0`); CI uses the same pin via `dtolnay/rust-toolchain@1.95.0` so pre-commit's `cargo clippy` runs the identical lint *rules* as CI. Identical rules, not identical coverage: the hook runs one invocation over the default-feature workspace, while the `clippy` job runs four (see [Build and Test](#build-and-test)). Under a rustup-managed `cargo`, the pinned toolchain auto-installs and is selected on first `cargo` invocation; other setups need to install `1.95.0` manually.

Dependabot auto-bumps the GitHub Actions refs only — it does **not** touch `rust-toolchain.toml` or `Cargo.toml`'s `rust-version`. A toolchain bump therefore touches three kinds of site: `.github/scripts/check-toolchain-pin.sh` (the `toolchain-pin` hook and the `packaging` CI job) refuses them to differ, so a Dependabot bump stays red until `rust-toolchain.toml` and `Cargo.toml` move in the same PR. All carry the same `X.Y.Z`; a two-part `rust-version` is refused for that reason, and a `with: toolchain:` input on a `dtolnay/rust-toolchain` step counts as that step's version.

MSRV (`rust-version.workspace = true` → 1.95.0) is the lower bound the workspace must compile under. It equals the stable lint pin, so there is no separate MSRV job: every Rust job compiles on that toolchain, and the pin check keeps the two from drifting apart. If MSRV is ever lowered below the pinned toolchain, add a dedicated `cargo check` job on the older floor and relax the pin check to a floor comparison.

The workspace is on `resolver = "3"`, which reads that `rust-version`: it defaults `resolver.incompatible-rust-versions` to `fallback`, so `cargo update` prefers dependency versions whose own declared MSRV is at or below ours instead of silently pulling one that raised it past the pinned toolchain. Resolver 3's feature-unification rules are identical to resolver 2's. If a dependency you need resolves to an older version than expected, that is this fallback at work — raise the workspace MSRV and the toolchain pin together rather than reaching for `--ignore-rust-version`.

## Code Style

**Language:** Rust (edition 2024, MSRV 1.95).

`rustfmt.toml` sets `max_width = 100`.

**Anti-panic policy:** Clippy denies `unwrap_used`, `expect_used`, `panic`, `indexing_slicing`, `todo`, and `unimplemented` workspace-wide. Use `Result`/`Option` combinators or `.get()` for indexing. This is the most common CI failure for new code.

**Numeric safety:** `cast_possible_truncation`, `cast_sign_loss`, and `cast_precision_loss` are `deny`, not `warn`. A narrowing or sign-changing cast needs a per-site `#[allow]`/`#[expect]` with a comment saying why it cannot lose data.

**No stray output:** `print_stdout`, `print_stderr`, and `dbg_macro` are `deny`. The `decdn` CLI is a terminal UI and allows both print lints at its library crate root; everywhere else, use `tracing`. The few production sites that run before the subscriber exists carry a per-site `#[expect]` with a reason — except where the `eprintln!` is itself `cfg`-gated, which takes an `#[allow]` scoped to that block, because an `#[expect]` would go unfulfilled in the build that turns the `cfg` on.

**Say what you mean:** `elided_lifetimes_in_paths` and `unreachable_pub` are `warn`. Write `Foo<'_>` when the type borrows, and give an item inside a private module the visibility it actually has (`pub(crate)` / `pub(super)`) rather than a bare `pub`. Both are machine-fixable — `cargo clippy --fix --workspace --all-targets` applies them.

**`missing_docs` is `warn`:** every public item — including struct fields and enum variants — carries a doc comment. The `alloy::sol!` bindings are the exception: each generated block opts out where it is declared, so a new binding needs the same `#[allow(missing_docs)]` on its wrapper. New docs are link-checked too: the `doc` gate runs `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --document-private-items`, so a broken `[`Type`]` link fails the build.

## Working with ADRs

ADRs in `adr/` are the primary deliverables right now. `adr/architecture.md` is the living overview and index of all decisions; numbered files cover individual decisions.

Operator runbook entries go in [docs/runbook.md](docs/runbook.md); cross-reference the alert or metric that surfaces the failure.

**Conventions:**

- File naming: `NNN-topic.md` (zero-padded 3-digit prefix). Check `adr/` for the current highest number to determine the next sequence.
- Write ADR prose in [ASD-STE100 Simplified Technical English](https://en.wikipedia.org/wiki/Simplified_Technical_English): short sentences, active voice, present tense, one idea per sentence, simple approved vocabulary. Every ADR already follows this — keep new ADRs and edits consistent with it.
- When changing any ADR, check for cross-ADR consistency — terms, parameters, and protocol names must match across all ADRs and `architecture.md`. This is the most common source of bugs in this repo.
- `architecture.md` must be updated whenever an ADR changes a user-visible summary point

**Consistency checks when editing ADRs:**

- Grep for renamed terms/parameters across all `adr/*.md` files
- Verify ALPN strings, message type names, and protocol version identifiers match `005-protocol.md`
- Verify token names (TOKEN/USDC), contract references, and fee parameters match `003-payments.md` and `026-tokenomics.md`
- Verify contract interaction flows and function signatures match `016-contract-interactions.md`
- Verify privacy claims and data-flow assertions match `017-privacy.md`
- Confirm `architecture.md` summary still reflects any changed ADR

```bash
# Quick consistency checks
grep -rn 'cdn/[a-z]*/v[0-9]' adr/      # find all ALPN references
grep -rn 'TOKEN\|USDC' adr/             # find all token references
grep -rn 'function\|contract\|modifier' adr/  # find Solidity interface references
```

## Adding a New ALPN Protocol Handler

Each ALPN listed in [ADR 005](adr/005-protocol.md) (`cdn/probe/v1`, `cdn/client/v1`, `cdn/dht/v1`) is served by a struct that implements `iroh::protocol::ProtocolHandler` and is registered on the iroh `Router` at runtime startup. [`crates/node/src/handlers/probe.rs`](crates/node/src/handlers/probe.rs) is the canonical reference — copy its shape when adding a new handler.

The pieces live in two crates, in this order:

1. **`crates/protocol/`** (leaf crate) — ALPN constant, wire message enum, request/response structs.
2. **`crates/node/src/handlers/`** — handler struct + `ProtocolHandler` impl. The `node` crate is the wiring layer; per [Appendix: PoC/Production Seams](adr/appendix-poc-production-seams.md), domain crates stay free of mode branching and the runtime in `crates/node/src/runtime/mod.rs` selects the concrete handler.

### Step-by-step recipe

#### 1. Declare the ALPN in `crates/protocol/src/lib.rs`

```rust
/// ALPN protocol identifier for <one-line purpose>. See ADR 005.
pub const ALPN_FOO: &[u8] = b"cdn/foo/v1";
```

The `cdn/<name>/v<n>` shape and the version suffix are mandatory — version bumps are how wire-breaking changes are signalled per [ADR 013](adr/013-schema-evolution.md).

#### 2. Add wire types to `crates/protocol/src/message.rs`

Define a top-level enum (e.g. `FooMessage`) wrapping per-direction structs. Variant order is **frozen** — postcard encodes each variant by its declaration index, so reordering is a wire-breaking change. Add a discriminant-locking test alongside the existing `probe_message_request_discriminant_is_zero` pattern:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FooMessage {
    Request(FooRequest),   // discriminant 0 — locked by test
    Response(FooResponse), // discriminant 1 — locked by test
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foo_message_request_discriminant_is_zero() -> Result<(), postcard::Error> {
        let msg = FooMessage::Request(FooRequest { /* ... */ });
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(0u8));
        Ok(())
    }

    #[test]
    fn foo_message_response_discriminant_is_one() -> Result<(), postcard::Error> {
        let msg = FooMessage::Response(FooResponse { /* ... */ });
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(1u8));
        Ok(())
    }
}
```

Re-export the new types from `crates/protocol/src/lib.rs` so node-side code can `use decdn_protocol::FooMessage;`.

#### 3. Implement the handler in `crates/node/src/handlers/<name>.rs`

The handler MUST:

- Take an `Arc<ConnectionLimiter>` and call `limiter.acquire(&conn)` first thing inside `serve`. On `Err(reason)`, close the connection with `APP_ERR_RATE_LIMITED` (`0x10`) and return `Ok(())` — rate-limited rejections are normal load-shedding, not protocol faults, and returning `Err` here makes iroh log every rejection (the exact amplification a flooder is trying to cause).
- Hold a `_guard = self.metrics.connection_guard()` for the lifetime of an accepted connection so `decdn_active_connections` is correct.
- Wrap each ordered protocol step in a `tokio::time::timeout`. Probe uses `ACCEPT_BI_TIMEOUT = 5s`, `*_READ_TIMEOUT = 5s`, `*_CLOSE_TIMEOUT = 3s`, `REJECTION_CLOSE_TIMEOUT = 250ms`. Without timeouts a single peer can pin a handler task indefinitely by stalling at any step.
- Map frame/decode errors to ADR 013 [Application Error Codes](adr/013-schema-evolution.md#application-error-codes) — `0x01 UNSUPPORTED_MESSAGE`, `0x02 MESSAGE_TOO_LARGE`, `0x03 MALFORMED_MESSAGE` — and propagate them via `RecvStream::stop` / `SendStream::reset`. For 1:1 connection-stream protocols also call `conn.close(VarInt::from_u32(code), b"...")` so the code reaches the peer deterministically.
- Match every `FrameError` variant explicitly when mapping to app codes — the explicit match makes a future `#[non_exhaustive]` addition fail the build instead of silently collapsing into `MALFORMED_MESSAGE`.
- Increment a metric on success (e.g. `self.metrics.foo_request()`) and emit `tracing::warn!(app_code, error = %e, ...)` on each rejected request.

The shape of the handler:

```rust
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::endpoint::Connection;

pub struct FooHandler { /* fields: node_id, metrics, limiter, ... */ }

impl FooHandler {
    pub const ALPN: &'static [u8] = decdn_protocol::ALPN_FOO;
    pub fn new(/* deps */) -> Self { /* ... */ }

    async fn serve(&self, conn: Connection) -> anyhow::Result<()> {
        // limiter.acquire → metrics guard → accept_bi → read_frame
        // → decode_message → handle → encode_message → write_frame → finish
    }
}

impl ProtocolHandler for FooHandler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        self.serve(conn)
            .await
            .map_err(|e| AcceptError::from_err(std::io::Error::other(e.to_string())))
    }
}
```

#### 4. Re-export the module in `crates/node/src/handlers/mod.rs`

```rust
pub mod foo;
```

#### 5. Wire it onto the `Router` in `crates/node/src/runtime/mod.rs`

Construct the handler after `ConnectionLimiter` and `Metrics` exist, then chain it onto the `Router::builder` next to `ProbeHandler`:

```rust
let foo_handler = Arc::new(FooHandler::new(/* deps */));

let router = Router::builder(ep.clone())
    .accept(ProbeHandler::ALPN, probe_handler)
    .accept(FooHandler::ALPN, foo_handler)
    .accept(GOSSIP_ALPN, gossip.clone())
    .spawn();
```

If your handler holds reloadable state (rates, pinned hashes, security limits), follow the existing pattern in `RuntimeReloadState`: store the value behind `Arc<AtomicU64>` / `Arc<RwLock<…>>` and call `reload_state.attach_*` immediately after construction so a SIGHUP delivered during the rest of startup still finds a target.

Bump `SHUTDOWN_DEADLINE` only if your handler's worst-case `accept_bi + read + close` budget exceeds the existing 15-second ceiling.

#### 6. Add a loopback integration test

Mirror [`crates/node/tests/probe_loopback.rs`](crates/node/tests/probe_loopback.rs): build two `iroh::Endpoint`s on `127.0.0.1` with `RelayMode::Disabled`, run your handler on the server endpoint, exercise the full request → response path on the client, and assert metrics increment. Use `permissive_limiter` to bypass rate limits in tests that aren't exercising rate-limit behaviour, and a tightly-configured `ConnectionLimiter` for tests that are.

### Cross-cutting requirements

- **Anti-panic policy** (see [Code Style](#code-style)): no `unwrap`, `expect`, `panic!`, or `arr[i]` indexing. The handler runs on every accepted connection — a panic crashes one task per connection and risks leaking handler state through the `JoinSet`.
- **ADR cross-references in module docs**: every handler module's doc-comment header should name the ALPN it serves and the ADR section that defines its message format and error codes (see the `probe.rs` header for the pattern).
- **Variant-order discipline**: when adding fields, follow [ADR 013](adr/013-schema-evolution.md) — append-only for Tier 1, frozen-body split for signed payloads. Don't reorder existing variants.
