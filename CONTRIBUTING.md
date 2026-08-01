# Contributing to deCDN

## Development Environment

This repo uses a VS Code devcontainer with a firewall-isolated environment. The container runs as the `node` user.

### Prerequisites

| Requirement | Windows | macOS | Linux |
|---|---|---|---|
| Docker | [Docker Desktop](https://docs.docker.com/desktop/install/windows-install/) (WSL2 backend) | [Docker Desktop](https://docs.docker.com/desktop/install/mac-install/) | [Docker Engine](https://docs.docker.com/engine/install/) |
| Editor | [VS Code](https://code.visualstudio.com/) | VS Code | VS Code |
| Extension | [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) | Dev Containers | Dev Containers |
| API Key | `ANTHROPIC_API_KEY` set on host | `ANTHROPIC_API_KEY` set on host | `ANTHROPIC_API_KEY` set on host |

#### Platform Notes

- **Windows**: Docker Desktop must use the WSL2 backend. Enable it in Docker Desktop Settings > General > "Use the WSL 2 based engine".
- **macOS (Apple Silicon)**: The container runs natively on arm64. Only enable Rosetta in Docker Desktop if you encounter compatibility issues with specific packages.
- **Linux**: Your user must be in the `docker` group (`sudo usermod -aG docker $USER`) or use rootless Docker.

## Quick Start

### 1. Set Your API Key

**Linux / macOS** (add to `~/.bashrc`, `~/.zshrc`, or `~/.profile`):

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

**Windows PowerShell** (persistent, user-level):

```powershell
[Environment]::SetEnvironmentVariable("ANTHROPIC_API_KEY", "sk-ant-...", "User")
```

**Windows cmd** (persistent, user-level):

```cmd
setx ANTHROPIC_API_KEY "sk-ant-..."
```

Restart your terminal after setting the variable.

### 2. Clone and Open

```bash
git clone git@github.com:thiras/decdn.git
cd decdn
code .
```

### 3. Reopen in Container

When VS Code detects the `.devcontainer/` folder, it will prompt:

> **Folder contains a Dev Container configuration file. Reopen folder to develop in a container.**

Click **Reopen in Container**. Or open the Command Palette (`Ctrl+Shift+P` / `Cmd+Shift+P`) and run:

```text
Dev Containers: Reopen in Container
```

The first build takes a few minutes (Rust toolchain + dependencies). Subsequent starts reuse the cached image.

### 4. Verify

Inside the container terminal:

```bash
claude --version       # Claude Code CLI
rustc --version        # Rust compiler
cargo --version        # Cargo package manager
[ -n "$ANTHROPIC_API_KEY" ] && echo "Key is set" || echo "Key is missing"
```

## What's Included

| Tool | Purpose |
|---|---|
| Rust (stable) | Compiler, cargo, rust-analyzer, cargo-watch, cargo-nextest |
| Claude Code | Anthropic's AI coding assistant CLI |
| git + git-delta | Version control with enhanced diffs |
| zsh + Powerlevel10k | Shell with prompt theme, fzf, git integration |
| gh | GitHub CLI |
| iptables + ipset | Container firewall (auto-configured on start) |
| Node.js 20 | Runtime for Claude Code |

### VS Code Extensions (Auto-Installed)

- `anthropic.claude-code` — Claude Code
- `rust-lang.rust-analyzer` — Rust language server (clippy on save)
- `eamodio.gitlens` — Git history and blame

## Authentication

The `ANTHROPIC_API_KEY` environment variable is forwarded from your host machine into the container via `devcontainer.json`:

```json
"containerEnv": {
  "ANTHROPIC_API_KEY": "${localEnv:ANTHROPIC_API_KEY}"
}
```

The key is never baked into the image or committed to the repo. Each team member sets their own key on their host.

## Running Claude Code Unattended

The container's firewall isolation enables safe use of `--dangerously-skip-permissions` for long-running, unattended sessions:

```bash
claude --dangerously-skip-permissions
```

This bypasses all permission prompts, allowing Claude to read/write files, run commands, and operate autonomously within the container.

**Safety considerations:**

- The firewall restricts network access to only whitelisted domains (see below)
- The container is isolated from your host filesystem (except the mounted workspace)
- Only use this with trusted repositories — a malicious project could exfiltrate data accessible within the container
- Monitor Claude's activities, especially during initial use

## Pre-commit Hooks

The repo uses [pre-commit](https://pre-commit.com/) to enforce formatting, linting, and supply chain checks before each commit. The devcontainer runs `pre-commit install` automatically on creation.

```bash
pre-commit install                # one-time setup (done automatically in devcontainer)
pre-commit run --all-files        # run all hooks manually
```

Hooks: trailing-whitespace, end-of-file-fixer, check-yaml, check-merge-conflict, check-added-large-files, markdownlint, `cargo fmt`, `cargo clippy`, `cargo doc`, `cargo deny`.

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
the `anvil-e2e` journeys, `otlp`, `public-api-test`. Run it before pushing a change that touches
gated code, or the first thing that tells you is a red CI.

### Public API snapshot

`decdn-protocol` carries a [`public_api`](https://crates.io/crates/public_api) +
[`insta`](https://crates.io/crates/insta) snapshot test that guards its exported surface
(wire types, ALPN definitions) against unintended changes — see [issue #304]. It is
feature-gated and **not** part of `cargo nextest run --workspace`. The dedicated `public-api`
CI job runs it and **blocks the PR** on any diff.

```bash
# Run (and update) the snapshot. Required when you intentionally change the public API:
INSTA_UPDATE=always cargo nextest run -p decdn-protocol --features public-api-test
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

## Solidity development

Contracts live in `contracts/` and use [Foundry](https://book.getfoundry.sh). CI pins Foundry to `v1.7.1`; install a matching local toolchain via [`foundryup`](https://book.getfoundry.sh/getting-started/installation).

**Common commands (run from `contracts/`):**

```bash
forge fmt --check                       # formatting check (CI gate)
forge build --sizes --deny warnings     # build + report sizes; fail on warnings (CI gate)
forge test                              # default profile: 256 fuzz, 32-depth invariants
FOUNDRY_PROFILE=ci forge test           # 1024 fuzz, 256/50-depth (matches CI)
FOUNDRY_PROFILE=fuzz forge test         # 10k fuzz, 1024/100-depth (nightly/manual)
FOUNDRY_PROFILE=coverage forge coverage --report lcov
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
- **Coverage** posts a sticky PR comment with total line coverage + delta vs `main` (the `solidity-coverage` job uploads an LCOV baseline on push-to-main and downloads it on PRs). The comment script is `.github/scripts/contracts-coverage-comment.sh`; the Rust side uses the analogous `coverage-diff.py`.

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

# 3. Deploy the protocol. The three addresses are required; for local testing any
#    EOA works. EMERGENCY_MULTISIG is account #1; INITIAL_TOKEN_HOLDER is the
#    deployer (account #0, the `--sender`) so it holds the genesis TOKEN and can
#    later fund the faucet. The deployer must NOT be forge's default sender, so
#    always pass `--sender` explicitly. CURRENT_TERMS_HASH is any non-zero
#    bytes32 for local testing — CapacityBond rejects the zero sentinel, so an
#    accidental terms-disabled deployment is forbidden.
USDC_ADDRESS=$USDC \
EMERGENCY_MULTISIG=0x70997970C51812dc3A010C7d01b50e0d17dc79C8 \
INITIAL_TOKEN_HOLDER=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
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
- **Required env vars:** `USDC_ADDRESS`, `EMERGENCY_MULTISIG`, `INITIAL_TOKEN_HOLDER`, `CURRENT_TERMS_HASH`. Every economic parameter (`MIN_BOND`, `TIMELOCK_DELAY`, `FEE_ROUTER_WINDOW_EPOCHS`, the slash/blacklist bonds, …) has a production-shaped default and is overridable via env var — see `_readConfig` in `DeployProtocol.s.sol` for the full list and defaults.
- **`--sender` becomes the deployer.** The script reverts (`DeployerIsForgeDefaultSender`) if you let forge use its default sender, so always pass `--sender`. The deployer's roles are granted to the Timelock and then revoked as the final step; the script reverts if any privileged role is left on the deployer.
- **Re-running on the same chain reverts** with `ManifestAlreadyExists` (the guard fires before any gas is spent). Either `rm deployments/31337.json`, restart Anvil for a clean slate, or set `FORCE_OVERWRITE_MANIFEST=true`.
- **`BuybackBurner` is recorded as the zero address** — it ships unwired at launch (operators take 90%, treasury 10%); its 30% buyback share activates by governance once a concrete Balancer V3 subclass is deployed.
- **Testnet TOKEN faucet (optional):** `script/TestnetFaucet.s.sol:DeployTestnetFaucet` funds and deploys a cooldown-gated dispenser. It reads `TOKEN_ADDRESS` (from the manifest), `TREASURY_ADDRESS` (must equal `--sender`), `FUNDING_AMOUNT`, and the `ADMIN_ADDRESS`/`GOVERNANCE_ADDRESS`/`PAUSER_ADDRESS` role holders; `CLAIM_AMOUNT` and `COOLDOWN_SECONDS` are optional.

## Rust Toolchain

`rust-toolchain.toml` pins an exact stable release (currently `1.95.0`); CI uses the same pin via `dtolnay/rust-toolchain@1.95.0` so pre-commit's `cargo clippy` runs the identical lint *rules* as CI. Identical rules, not identical coverage: the hook runs one invocation over the default-feature workspace, while the `clippy` job runs four (see [Build and Test](#build-and-test)). Under a rustup-managed `cargo` (what the devcontainer ships), the pinned toolchain auto-installs and is selected on first `cargo` invocation; other setups need to install `1.95.0` manually.

Dependabot auto-bumps the GitHub Actions refs only — it does **not** touch `rust-toolchain.toml` or `Cargo.toml`'s `rust-version`. When accepting a Dependabot toolchain bump, update those two files in the same PR (and `Cargo.toml`'s `rust-version` if MSRV is moving in lockstep) so developer machines and CI stay aligned.

MSRV (`rust-version.workspace = true` → 1.95) is the lower bound the workspace must compile under; it's checked separately by the `msrv` job in `.github/workflows/ci.yml` and is a distinct knob from the stable lint pin (currently set to the same value).

## Code Style

**Language:** Rust (edition 2024, MSRV 1.95).

`rustfmt.toml` sets `max_width = 100`.

**Anti-panic policy:** Clippy denies `unwrap_used`, `expect_used`, `panic`, and `indexing_slicing` workspace-wide. Use `Result`/`Option` combinators or `.get()` for indexing. This is the most common CI failure for new code.

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

## Firewall and Security

On container start, `init-firewall.sh` configures a default-deny iptables firewall.

**Whitelisted domains** (HTTPS/TCP):

| Category | Domains |
|---|---|
| **Anthropic** | `api.anthropic.com`, `sentry.io`, `statsig.anthropic.com`, `statsig.com` |
| **GitHub** | Dynamic IP ranges from `api.github.com/meta` (web, API, git) |
| **npm** | `registry.npmjs.org` |
| **VS Code** | `marketplace.visualstudio.com`, `vscode.blob.core.windows.net`, `update.code.visualstudio.com` |
| **Rust** | `crates.io`, `static.crates.io`, `index.crates.io`, `static.rust-lang.org` |
| **Ethereum** | `sepolia-rollup.arbitrum.io`, `arb-sepolia.g.alchemy.com` |

**Infrastructure rules** (always allowed):

| Rule | Scope | Purpose |
|---|---|---|
| DNS (UDP/TCP 53) | Docker resolver (`127.0.0.11`) only | Name resolution — restricted to prevent direct external DNS access |
| SSH (TCP 22) | Whitelisted IPs only | Git over SSH to GitHub — not open to arbitrary hosts |
| Localhost | `lo` interface | Inter-process communication |
| Host gateway | Single gateway IP | Docker host ↔ container communication |

All other outbound traffic is rejected.

### Adding a New Domain

Edit `.devcontainer/init-firewall.sh` and add the domain to either the `CRITICAL_DOMAINS` array (must resolve or container fails to start) or the `OPTIONAL_DOMAINS` array (best-effort):

```bash
CRITICAL_DOMAINS=(
    ...
    "your-critical-domain.example.com"
)

OPTIONAL_DOMAINS=(
    ...
    "your-optional-domain.example.com"
)
```

Rebuild the container image for the change to take effect (the script is copied during build, then executed on every container start).

## Persistent Volumes

These volumes survive container rebuilds:

| Volume | Path in container | Contents |
|---|---|---|
| `decdn-bashhistory-*` | `/commandhistory` | Shell history |
| `decdn-claude-config-*` | `/home/node/.claude` | Claude Code config and session data |
| `decdn-cargo-registry-*` | `/home/node/.cargo/registry` | Downloaded crate sources and indices |

Your workspace files are bind-mounted from the host, so they always persist.

## Customization

### Add a VS Code Extension

Edit `.devcontainer/devcontainer.json`:

```json
"extensions": [
  "anthropic.claude-code",
  "rust-lang.rust-analyzer",
  "eamodio.gitlens",
  "your-publisher.your-extension"
]
```

### Change the Timezone

The container inherits your host's `TZ` environment variable. To override, set it before opening:

```bash
export TZ="Europe/Istanbul"
```

Or change the default in `devcontainer.json` build args.

### Add System Packages

Edit the `apt-get install` block in `.devcontainer/Dockerfile` and rebuild.

## Troubleshooting

### "Reopen in Container" Doesn't Appear

Ensure the Dev Containers extension is installed. Open Command Palette and search for "Dev Containers: Reopen in Container".

### Container Build Fails

```bash
# Build manually to see full output
docker build -f .devcontainer/Dockerfile .devcontainer/
```

### `ANTHROPIC_API_KEY` is Empty Inside the Container

The key must be set in your host shell **before** opening VS Code. Verify on your host:

```bash
echo $ANTHROPIC_API_KEY   # Linux/macOS
$env:ANTHROPIC_API_KEY    # PowerShell
echo %ANTHROPIC_API_KEY%  # cmd
```

If set but not visible, restart VS Code — it reads environment variables at launch.

### Firewall Blocks a Domain You Need

Check which domain is blocked:

```bash
curl -v https://the-domain.com 2>&1 | head -20
```

Add it to `init-firewall.sh` and rebuild the container image (the script is copied during build).

### Slow First Build on Apple Silicon

Compiling Rust dev tools (`cargo-watch`, `cargo-nextest`) from source takes longer on arm64. The first build may take 5-10 minutes. Subsequent starts reuse cached layers.

### Docker Not Running (Windows)

Ensure Docker Desktop is running and the WSL2 backend is active. If using WSL2, run `wsl --update` to ensure it's current.
