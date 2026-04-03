# CLAUDE.md

## Project Overview

Decentralized CDN (deCDN) — nodes cache and serve content-addressed blobs over iroh QUIC, clients pay per-MB via off-chain USDC payment channels. Rust implementation targeting a PoC of tens of nodes on an Arbitrum Sepolia testnet.

**Status: Pre-implementation (design/ADR phase).** No Rust source code or `Cargo.toml` exists yet. The repo currently contains architecture decision records (`adr/`), a devcontainer setup, and this documentation.

## Development Environment

This repo uses a VS Code devcontainer with a firewall-isolated environment. The container runs as the `node` user.

```bash
# Open in devcontainer (VS Code will prompt automatically)
# Or: Ctrl+Shift+P → "Dev Containers: Reopen in Container"
```

### Working with ADRs

ADRs in `adr/` are the primary deliverables right now. `adr/architecture.md` is the living overview and index of all decisions; numbered files cover individual decisions.

**Conventions:**
- File naming: `NNN-topic.md` (zero-padded 3-digit prefix, next number is 015)
- When changing any ADR, check for cross-ADR consistency — terms, parameters, and protocol names must match across all ADRs and `architecture.md`. This is the most common source of bugs in this repo.
- `architecture.md` must be updated whenever an ADR changes a user-visible summary point

**Consistency checks when editing ADRs:**
- Grep for renamed terms/parameters across all `adr/*.md` files
- Verify ALPN strings, message type names, and protocol version identifiers match `005-protocol.md`
- Verify token names (TOKEN/USDC), contract references, and fee parameters match `003-payments.md` and `004-tokenomics.md`
- Confirm `architecture.md` summary still reflects any changed ADR

```bash
# Quick consistency checks
grep -rn 'cdn/[a-z]*/v[0-9]' adr/      # find all ALPN references
grep -rn 'TOKEN\|USDC' adr/             # find all token references
```

### Build and Test (once implementation begins)

```bash
cargo build && cargo clippy          # build + lint
cargo nextest run                    # test (preferred over cargo test)
cargo nextest run -p protocol        # single crate
cargo fmt -- --check                 # check formatting
```

## Architecture

**Language:** Rust. **Networking:** iroh (QUIC transport, NAT traversal, content-addressed blobs, gossip).

### Crate Structure (planned)

```
crates/
  node/         — binary entry point, CLI, config, wiring
  protocol/     — shared types, wire format, ALPN message definitions (leaf crate, minimal deps)
  cache/        — cache engine wrapping iroh-blobs + origin pull-through
  incentive/    — payment channels, staking, vouchers (alloy for Ethereum)
  reputation/   — gossip-based reputation scoring
  contracts/    — Solidity contracts + Foundry
```

**Dependency flow:** `node → cache, incentive, reputation, protocol`. Cache and incentive are independent — cache works without payment logic (useful for testing/local dev).

### Wire Protocols (Core CDN)

| ALPN | Purpose |
|------|---------|
| `cdn/probe/v1` | Latency + availability probing |
| `cdn/client/v1` | All paid delivery (client→node and node→node) |
| `cdn/watchtower/v1` | Channel-dispute monitoring, voucher registration |
| iroh-gossip (built-in) | Node metadata broadcast (`NodeAnnounce`), node discovery |

### Companion Protocol (App Server)

| ALPN | Purpose |
|------|---------|
| `cdn/keys/v1` | Epoch key delivery, play requests, offline leases (app server) |

> The app server is not a CDN protocol participant — see [ADR 006](adr/006-e2e-encryption.md).

### Key Design Decisions

- Content is BLAKE3-addressed; clients verify hashes on received bytes
- No external origin URLs are ever exposed — origin backends (S3/R2/B2) are opaque per-node config
- All byte transfers are paid, including node-to-node cache-miss pulls
- TOKEN for staking/governance, USDC for payments (dual-currency model)
- ADRs in `adr/` document all major decisions; `adr/architecture.md` is the living overview

### Firewall (Devcontainer)

The container runs a default-deny iptables firewall (`init-firewall.sh`). To whitelist a new domain, add it to `CRITICAL_DOMAINS` or `OPTIONAL_DOMAINS` in `.devcontainer/init-firewall.sh` and rebuild the container.
