# deCDN

Decentralized CDN where nodes cache and serve content-addressed blobs over [iroh](https://iroh.computer/) QUIC, and clients pay per-MB via off-chain USDC payment channels. Rust implementation targeting a PoC of tens of nodes on Arbitrum Sepolia testnet.

## How It Works

- **Nodes** stake TOKEN, cache content, and serve BLAKE3-addressed blobs over QUIC
- **Clients** probe candidate nodes, pick the best by `rate_per_mb × rtt_ms`, stream content, and pay via off-chain USDC vouchers
- On a **cache miss**, nodes pull from peers (paid), cache locally, and stream to the client simultaneously
- All byte transfers are paid — client-to-node and node-to-node

```
  ┌────────────┐ ┌────────────┐ ┌────────────┐
  │    Node    │◄┤    Node    │◄┤    Node    │
  │  (cached)  ├►│  (origin)  ├►│  (cached)  │
  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
        │              │              │
     pays per-MB    pays per-MB    pays per-MB
       (USDC)         (USDC)         (USDC)
        │              │              │
  ┌─────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐
  │   Client   │ │   Client   │ │   Client   │
  └────────────┘ └────────────┘ └────────────┘
```

### Wire Protocols (Core CDN)

| ALPN | Purpose |
|------|---------|
| `cdn/probe/v1` | Latency + availability probing |
| `cdn/client/v1` | All paid delivery (client→node and node→node) |
| `cdn/watchtower/v1` | Channel-dispute monitoring (voucher registration) |
| iroh-gossip (built-in) | Node metadata broadcast (`NodeAnnounce`), node discovery |

### Companion Protocol (App Server)

| ALPN | Purpose |
|------|---------|
| `cdn/keys/v1` | Epoch key delivery, play requests, offline leases (app server) |

> The app server is not a CDN protocol participant — see [ADR 006](adr/006-e2e-encryption.md).

### Planned Crate Structure

```
crates/
  node/         — binary entry point, CLI, config, wiring
  protocol/     — shared types, wire format, ALPN message definitions
  cache/        — cache engine wrapping iroh-blobs + origin pull-through
  incentive/    — payment channels, staking, vouchers (alloy for Ethereum)
  reputation/   — gossip-based reputation scoring
  contracts/    — Solidity contracts + Foundry
```

### Key Design Decisions

Architecture decision records live in [`adr/`](adr/), with [`adr/architecture.md`](adr/architecture.md) as the living overview. Highlights:

- **Content addressing:** BLAKE3 hashes; clients verify on receipt
- **Dual currency:** USDC for payments, TOKEN for staking/governance
- **No exposed origins:** Origin backends (S3/R2/B2) are opaque per-node config
- **E2E encryption:** Envelope encryption with epoch-rotated keys; CDN nodes only see ciphertext
- **Watchtowers:** Non-custodial dispute monitors for payment channel safety
- **Discovery:** Gossip-only content routing for PoC; DHT/Kademlia deferred to post-PoC
- **Reputation:** Interaction-weighted scoring propagated via gossip
- **Governance:** Admin key for PoC; token-weighted governance with safety bounds for production
- **Multi-token payments (post-PoC):** PoC uses USDC only; production supports governance-approved ERC-20 allowlist
- **Content takedown:** Governance-controlled hash blacklisting with regional compliance bodies
- **Client architecture:** Lightweight QUIC endpoints; gossip subscribe (no publish); registry bootstrap with fallback; per-connection ephemeral identity binding
- **Schema evolution:** Varint-length framing, protocol enums, three-tier evolution model (minor/medium/major)

## Development

This repository includes a devcontainer for a consistent, firewall-isolated development environment.

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

### Quick Start

#### 1. Set Your API Key

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

#### 2. Clone and Open

```bash
git clone git@github.com:thiras/decdn.git
cd decdn
code .
```

#### 3. Reopen in Container

When VS Code detects the `.devcontainer/` folder, it will prompt:

> **Folder contains a Dev Container configuration file. Reopen folder to develop in a container.**

Click **Reopen in Container**. Or open the Command Palette (`Ctrl+Shift+P` / `Cmd+Shift+P`) and run:

```
Dev Containers: Reopen in Container
```

The first build takes a few minutes (Rust toolchain + dependencies). Subsequent starts reuse the cached image.

#### 4. Verify

Inside the container terminal:

```bash
claude --version       # Claude Code CLI
rustc --version        # Rust compiler
cargo --version        # Cargo package manager
[ -n "$ANTHROPIC_API_KEY" ] && echo "Key is set" || echo "Key is missing"
```

### What's Included

| Tool | Purpose |
|---|---|
| Rust (stable) | Compiler, cargo, rust-analyzer, cargo-watch, cargo-nextest |
| Claude Code | Anthropic's AI coding assistant CLI |
| git + git-delta | Version control with enhanced diffs |
| zsh + Powerlevel10k | Shell with prompt theme, fzf, git integration |
| gh | GitHub CLI |
| iptables + ipset | Container firewall (auto-configured on start) |
| Node.js 20 | Runtime for Claude Code |

#### VS Code Extensions (Auto-Installed)

- `anthropic.claude-code` — Claude Code
- `rust-lang.rust-analyzer` — Rust language server (clippy on save)
- `eamodio.gitlens` — Git history and blame

### Authentication

The `ANTHROPIC_API_KEY` environment variable is forwarded from your host machine into the container via `devcontainer.json`:

```json
"containerEnv": {
  "ANTHROPIC_API_KEY": "${localEnv:ANTHROPIC_API_KEY}"
}
```

The key is never baked into the image or committed to the repo. Each team member sets their own key on their host.

### Running Claude Code Unattended

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

### Firewall and Security

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

#### Adding a New Domain

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

Rebuild the container image for the change to take effect (the script is baked in at build time, then executed on every container start).

### Persistent Volumes

These volumes survive container rebuilds:

| Volume | Path in container | Contents |
|---|---|---|
| `decdn-bashhistory-*` | `/commandhistory` | Shell history |
| `decdn-claude-config-*` | `/home/node/.claude` | Claude Code config and session data |
| `decdn-cargo-registry-*` | `/home/node/.cargo/registry` | Downloaded crate sources and indices |

Your workspace files are bind-mounted from the host, so they always persist.

### Customization

#### Add a VS Code Extension

Edit `.devcontainer/devcontainer.json`:

```json
"extensions": [
  "anthropic.claude-code",
  "rust-lang.rust-analyzer",
  "eamodio.gitlens",
  "your-publisher.your-extension"
]
```

#### Change the Timezone

The container inherits your host's `TZ` environment variable. To override, set it before opening:

```bash
export TZ="Europe/Istanbul"
```

Or change the default in `devcontainer.json` build args.

#### Add System Packages

Edit the `apt-get install` block in `.devcontainer/Dockerfile` and rebuild.

### Troubleshooting

#### "Reopen in Container" Doesn't Appear

Ensure the Dev Containers extension is installed. Open Command Palette and search for "Dev Containers: Reopen in Container".

#### Container Build Fails

```bash
# Build manually to see full output
docker build -f .devcontainer/Dockerfile .devcontainer/
```

#### `ANTHROPIC_API_KEY` is Empty Inside the Container

The key must be set in your host shell **before** opening VS Code. Verify on your host:

```bash
echo $ANTHROPIC_API_KEY   # Linux/macOS
$env:ANTHROPIC_API_KEY    # PowerShell
echo %ANTHROPIC_API_KEY%  # cmd
```

If set but not visible, restart VS Code — it reads environment variables at launch.

#### Firewall Blocks a Domain You Need

Check which domain is blocked:

```bash
curl -v https://the-domain.com 2>&1 | head -20
```

Add it to `init-firewall.sh` and rebuild the container image (the script is copied during build).

#### Slow First Build on Apple Silicon

Compiling Rust dev tools (`cargo-watch`, `cargo-nextest`) from source takes longer on arm64. The first build may take 5-10 minutes. Subsequent starts reuse cached layers.

#### Docker Not Running (Windows)

Ensure Docker Desktop is running and the WSL2 backend is active. If using WSL2, run `wsl --update` to ensure it's current.
