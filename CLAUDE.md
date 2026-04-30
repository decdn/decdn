# CLAUDE.md

## Project Overview

Decentralized CDN (deCDN) — nodes cache and serve content-addressed blobs over iroh QUIC, clients pay per-MB via off-chain USDC payment channels. Rust implementation targeting a PoC of tens of nodes on an Arbitrum Sepolia testnet.

**Status: Early implementation.** Cargo workspace with 6 crates. `node` has CLI, config, runtime bring-up, and a probe handler; `protocol` has varint framing, `ProbeMessage` (ADR 013), and `NodeAnnounce` gossip types; `cache` has the pull-through engine + HTTP/filesystem origin adapters; `gossip` has the `NodeAnnounce` pub/sub service with peer table. `incentive` and `reputation` are still stubs. ADRs in `adr/` remain the primary design artifacts.

See [CONTRIBUTING.md](CONTRIBUTING.md) for build commands, ADR conventions, pre-commit hooks, and development environment setup.

**ADR note:** Next ADR number is 033. File naming: `NNN-topic.md` (zero-padded 3-digit prefix). Always verify by checking `adr/` for the highest number before creating a new ADR.

## Common Commands

```bash
cargo build && cargo clippy          # build + lint (clippy is the usual CI failure)
cargo nextest run                    # test (preferred over cargo test)
cargo nextest run -p decdn-protocol  # single crate
cargo fmt -- --check                 # check formatting
cargo deny check                     # license + advisory audit
pre-commit run --all-files           # run all hooks
```

## Architecture

**Language:** Rust (edition 2024, MSRV 1.85). **Networking:** iroh (QUIC transport, NAT traversal, content-addressed blobs, gossip).

**Code style:** `rustfmt.toml` sets `max_width = 100`.

**Anti-panic policy:** Clippy denies `unwrap_used`, `expect_used`, `panic`, and `indexing_slicing` workspace-wide. Use `Result`/`Option` combinators or `.get()` for indexing. This is the most common CI failure for new code.

### Crate Structure

```
crates/
  node/         — binary entry point, CLI, config, wiring
  protocol/     — shared types, wire format, ALPN message definitions (leaf crate, minimal deps)
  cache/        — cache engine wrapping iroh-blobs + origin pull-through
  gossip/       — NodeAnnounce pub/sub over iroh-gossip, peer table, envelope validation
  incentive/    — payment channels, staking, vouchers (alloy for Ethereum)
  reputation/   — gossip-based reputation scoring
  contracts/    — Solidity contracts + Foundry (excluded from workspace, not yet populated)
```

**Dependency flow:** `node → cache, gossip, incentive, reputation, protocol`. Cache and incentive are independent — cache works without payment logic (useful for testing/local dev).

### Wire Protocols (Core CDN)

| ALPN | Purpose |
|------|---------|
| `cdn/probe/v1` | Latency + availability probing |
| `cdn/client/v1` | All paid delivery (client→node and node→node) |
| `cdn/watchtower/v1` | Channel-dispute monitoring, voucher registration |
| `cdn/dht/v1` | Content discovery via Kademlia DHT (see ADR 022) |
| iroh-gossip (built-in) | Node metadata broadcast (`NodeAnnounce`), node discovery |
| `cdn/reputation/v1` (gossip topic) | Reputation reports over iroh-gossip |

### Key Design Decisions

- Content is BLAKE3-addressed; clients verify hashes on received bytes
- No external origin URLs are ever exposed — origin backends (S3/R2/B2) are opaque per-node config
- All byte transfers are paid, including node-to-node cache-miss pulls
- TOKEN for staking/governance, USDC for payments (dual-currency model)
- Domain crates (`cache`, `gossip`, etc.) are "leaf" — no mode branching or `#[cfg(feature = "poc")]`. The `node` crate's wiring layer selects backends/implementations. See [adr/appendix-poc-production-seams.md](adr/appendix-poc-production-seams.md) for the full Rust implementation pattern.
- ADRs in `adr/` document all major decisions; `adr/architecture.md` is the living overview
