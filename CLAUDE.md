# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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

ADRs in `adr/` are the primary deliverables right now. `adr/architecture.md` is the living overview; numbered files (`000-language.md`, `001-network.md`, etc.) cover individual decisions.

### Build and Test (once implementation begins)

```bash
cargo build                          # build all crates
cargo clippy                         # lint (rust-analyzer runs this on save)
cargo test                           # run all tests
cargo nextest run                    # run tests with cargo-nextest (preferred)
cargo nextest run -p protocol        # run tests for a single crate
cargo nextest run test_name          # run a single test by name
cargo watch -x test                  # re-run tests on file change
```

### Formatting

```bash
cargo fmt                            # format all code
cargo fmt -- --check                 # check formatting without modifying
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

### Wire Protocols (ALPN-identified)

| ALPN | Purpose |
|------|---------|
| `cdn/probe/v1` | Latency + availability probing |
| `cdn/client/v1` | All paid delivery (client→node and node→node) |
| `cdn/keys/v1` | Epoch key delivery, sealed envelope requests (app server ↔ client) |
| `cdn/watchtower/v1` | Channel-dispute monitoring, voucher registration |
| iroh-gossip | Content availability broadcast, node discovery |

### Key Design Decisions

- Content is BLAKE3-addressed; clients verify hashes on received bytes
- No external origin URLs are ever exposed — origin backends (S3/R2/B2) are opaque per-node config
- All byte transfers are paid, including node-to-node cache-miss pulls
- TOKEN for staking/governance, USDC for payments (dual-currency model)
- ADRs in `adr/` document all major decisions; `adr/architecture.md` is the living overview

### Firewall (Devcontainer)

The container runs a default-deny iptables firewall (`init-firewall.sh`). To whitelist a new domain, add it to `CRITICAL_DOMAINS` or `OPTIONAL_DOMAINS` in `.devcontainer/init-firewall.sh` and rebuild the container.
