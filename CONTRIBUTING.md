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

Hooks: trailing-whitespace, end-of-file-fixer, check-yaml, check-merge-conflict, check-added-large-files, markdownlint, `cargo fmt`, `cargo clippy`, `cargo deny`.

## Build and Test

```bash
cargo build && cargo clippy          # build + lint
cargo nextest run                    # test (preferred over cargo test)
cargo nextest run -p decdn-protocol   # single crate
cargo fmt -- --check                 # check formatting
cargo deny check                     # license + advisory audit (deny.toml)
```

## Code Style

**Language:** Rust (edition 2024, MSRV 1.85).

`rustfmt.toml` sets `max_width = 100`.

**Anti-panic policy:** Clippy denies `unwrap_used`, `expect_used`, `panic`, and `indexing_slicing` workspace-wide. Use `Result`/`Option` combinators or `.get()` for indexing. This is the most common CI failure for new code.

## Working with ADRs

ADRs in `adr/` are the primary deliverables right now. `adr/architecture.md` is the living overview and index of all decisions; numbered files cover individual decisions.

**Conventions:**

- File naming: `NNN-topic.md` (zero-padded 3-digit prefix). Check `adr/` for the current highest number to determine the next sequence.
- When changing any ADR, check for cross-ADR consistency — terms, parameters, and protocol names must match across all ADRs and `architecture.md`. This is the most common source of bugs in this repo.
- `architecture.md` must be updated whenever an ADR changes a user-visible summary point

**Consistency checks when editing ADRs:**

- Grep for renamed terms/parameters across all `adr/*.md` files
- Verify ALPN strings, message type names, and protocol version identifiers match `005-protocol.md`
- Verify token names (TOKEN/USDC), contract references, and fee parameters match `003-payments.md` and `004-tokenomics.md`
- Verify contract interaction flows and function signatures match `016-contract-interactions.md`
- Verify privacy claims and data-flow assertions match `017-privacy.md`
- Confirm `architecture.md` summary still reflects any changed ADR

```bash
# Quick consistency checks
grep -rn 'cdn/[a-z]*/v[0-9]' adr/      # find all ALPN references
grep -rn 'TOKEN\|USDC' adr/             # find all token references
grep -rn 'function\|contract\|modifier' adr/  # find Solidity interface references
```

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
