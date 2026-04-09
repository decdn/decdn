# CLAUDE.md

## Project Overview

Decentralized CDN (deCDN) — nodes cache and serve content-addressed blobs over iroh QUIC, clients pay per-MB via off-chain USDC payment channels. Rust implementation targeting a PoC of tens of nodes on an Arbitrum Sepolia testnet.

**Status: Early implementation.** Cargo workspace with 5 crates is scaffolded (stub `lib.rs` files, `node` has initial CLI/config). ADRs in `adr/` remain the primary design artifacts.

See [CONTRIBUTING.md](CONTRIBUTING.md) for build commands, ADR conventions, pre-commit hooks, and development environment setup.

**ADR note:** Next ADR number is 023. File naming: `NNN-topic.md` (zero-padded 3-digit prefix).

## Architecture

**Language:** Rust (edition 2024, MSRV 1.85). **Networking:** iroh (QUIC transport, NAT traversal, content-addressed blobs, gossip).

**Code style:** `rustfmt.toml` sets `max_width = 100`.

**Anti-panic policy:** Clippy denies `unwrap_used`, `expect_used`, `panic`, and `indexing_slicing` workspace-wide. Use `Result`/`Option` combinators or `.get()` for indexing. This is the most common CI failure for new code.

### Crate Structure (planned)

```
crates/
  node/         — binary entry point, CLI, config, wiring
  protocol/     — shared types, wire format, ALPN message definitions (leaf crate, minimal deps)
  cache/        — cache engine wrapping iroh-blobs + origin pull-through
  incentive/    — payment channels, staking, vouchers (alloy for Ethereum)
  reputation/   — gossip-based reputation scoring
  contracts/    — Solidity contracts + Foundry (excluded from workspace, not yet populated)
```

**Dependency flow:** `node → cache, incentive, reputation, protocol`. Cache and incentive are independent — cache works without payment logic (useful for testing/local dev).

### Wire Protocols (Core CDN)

| ALPN | Purpose |
|------|---------|
| `cdn/probe/v1` | Latency + availability probing |
| `cdn/client/v1` | All paid delivery (client→node and node→node) |
| `cdn/watchtower/v1` | Channel-dispute monitoring, voucher registration |
| `cdn/dht/v1` | Content discovery via Kademlia DHT (see ADR 022) |
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
