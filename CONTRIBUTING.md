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

## Rust Toolchain

`rust-toolchain.toml` pins an exact stable release (currently `1.95.0`); CI uses the same pin so pre-commit's `cargo clippy` runs the identical lint set as CI. `rustup` auto-installs and selects the pinned toolchain on first `cargo` invocation in this repo — no manual `rustup update` needed. Dependabot opens PRs to bump the pin as new stable releases land; bumps go through normal review.

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
- When changing any ADR, check for cross-ADR consistency — terms, parameters, and protocol names must match across all ADRs and `architecture.md`. This is the most common source of bugs in this repo.
- `architecture.md` must be updated whenever an ADR changes a user-visible summary point

**Consistency checks when editing ADRs:**

- Grep for renamed terms/parameters across all `adr/*.md` files
- Verify ALPN strings, message type names, and protocol version identifiers match `005-protocol.md`
- Verify token names (TOKEN/USDC), contract references, and fee parameters match `003-payments.md` and `026-gauge-boost-tokenomics.md`
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
