# CLAUDE.md

## Project Overview

Decentralized CDN (deCDN) — nodes cache and serve content-addressed blobs over iroh QUIC, clients pay per-MB via off-chain USDC payment channels. Rust implementation; the initial network deployment targets tens of nodes on an Arbitrum Sepolia testnet. "PoC" in code and ADR comments refers to that network-scale milestone, not contract-surface scope — the on-chain surface ships at full production shape with governance-tunable economics from day one (see [ADR 016 § Contract Inventory](adr/016-contract-interactions.md) and [§ Tunable Economics](adr/016-contract-interactions.md#tunable-economics)).

**Status: Early implementation.** Cargo workspace with 8 crates. Two binaries (#421): `node` produces the `decdn-node` daemon with the runtime bring-up, admin RPC server, dispatch limiter, and probe handler; `cli` produces the user-facing `decdn` binary carrying `probe`, `node {peers,…}`, `key-gen`, `config {…}`. `common` holds the shared config schema, identity loading, and AdminRpc trait + DTOs both binaries import. `protocol` has varint framing, `ProbeMessage` (ADR 013), and `NodeAnnounce` gossip types; `cache` has the pull-through engine + HTTP/filesystem origin adapters; `gossip` has the `NodeAnnounce` pub/sub service with peer table. `incentive` and `reputation` are still stubs. ADRs in `adr/` remain the primary design artifacts.

See [CONTRIBUTING.md](CONTRIBUTING.md) for build commands, ADR conventions, pre-commit hooks, and development environment setup.

**ADR note:** Next ADR number is 032. File naming: `NNN-topic.md` (zero-padded 3-digit prefix). Always verify by checking `adr/` for the highest number before creating a new ADR. (030 is canonical as `030-node-region-self-attestation.md` (closes #400). 031 is canonical as `031-content-blacklist-appeals-contract.md`. 032 was reclaimed when its proposal moved to the internal ideas repo — it was never canonical; free to claim. 029 was canonical but reclassified as `appendix-peer-table-eviction.md`; do not reuse 029. 027 was canonical (Distinct-Client Diversity Gating / Delivery Receipts) but deleted when its content collapsed into ADR 026 §3 per-operator gauge-share cap; do not reuse 027.)

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

**Language:** Rust (edition 2024, MSRV 1.95). **Networking:** iroh (QUIC transport, NAT traversal, content-addressed blobs, gossip).

**Code style:** `rustfmt.toml` sets `max_width = 100`.

**Anti-panic policy:** Clippy denies `unwrap_used`, `expect_used`, `panic`, and `indexing_slicing` workspace-wide. Use `Result`/`Option` combinators or `.get()` for indexing. This is the most common CI failure for new code.

### Crate Structure

```
crates/
  node/         — daemon binary `decdn-node`: runtime bring-up, handlers, admin RPC server, dispatch limiter
  cli/          — user CLI binary `decdn`: probe, node admin, key-gen, config
  common/       — shared types: config schema + resolver, identity loading, AdminRpc trait + DTOs
  protocol/     — shared types, wire format, ALPN message definitions (leaf crate, minimal deps)
  cache/        — cache engine wrapping iroh-blobs + origin pull-through
  gossip/       — NodeAnnounce pub/sub over iroh-gossip, peer table, envelope validation
  incentive/    — payment channels, staking, vouchers (alloy for Ethereum)
  reputation/   — gossip-based reputation scoring
contracts/      — Solidity contracts + Foundry (repo root, excluded from workspace, not yet populated)
```

**Dependency flow:** `node → cache, gossip, incentive, reputation, protocol, common`; `cli → common, protocol`. The two binaries share `common` for config schema, identity, and admin wire types — see [`adr/appendix-binaries.md`](adr/appendix-binaries.md) for the dockerd-style split rationale. Cache and incentive are independent — cache works without payment logic (useful for testing/local dev).

### Wire Protocols (Core CDN)

| ALPN | Purpose |
|------|---------|
| `cdn/probe/v1` | Latency + availability probing |
| `cdn/client/v1` | All paid delivery (client→node and node→node) |
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
