# AGENTS.md

## Project Overview

Decentralized CDN (deCDN) — nodes cache and serve content-addressed blobs over iroh QUIC, clients pay per-MB via off-chain USDC payment channels. Rust implementation; the initial network deployment targets tens of nodes on an Arbitrum Sepolia testnet. "PoC" in code and ADR comments refers to that network-scale milestone, not contract-surface scope — the on-chain surface ships at full production shape with governance-tunable economics from day one.

**Status: Early implementation.** Cargo workspace with 8 crates. ADRs in `adr/` are the primary design artifacts.

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

**Dependency flow:** `node → cache, gossip, incentive, reputation, protocol, common`; `cli → common, protocol`. Domain crates (`cache`, `gossip`, etc.) are leaf crates — no mode branching. The `node` crate's wiring layer selects backends/implementations.

### Wire Protocols

| ALPN | Purpose |
|------|---------|
| `cdn/probe/v1` | Latency + availability probing |
| `cdn/client/v1` | All paid delivery (client→node and node→node) |
| `cdn/dht/v1` | Content discovery via Kademlia DHT |
| iroh-gossip (built-in) | Node metadata broadcast (`NodeAnnounce`), node discovery |
| `cdn/reputation/v1` (gossip topic) | Reputation reports over iroh-gossip |

### Key Design Decisions

- Content is BLAKE3-addressed; clients verify hashes on received bytes
- No external origin URLs are ever exposed — origin backends (S3/R2/B2) are opaque per-node config
- All byte transfers are paid, including node-to-node cache-miss pulls
- TOKEN for staking/governance, USDC for payments (dual-currency model)
- ADRs in `adr/` document all major decisions; `adr/architecture.md` is the living overview

## ADRs

ADRs in `adr/` are the primary deliverables. `adr/architecture.md` is the living overview and index.

**Conventions:**

- File naming: `NNN-topic.md` (zero-padded 3-digit prefix). Always check `adr/` for the current highest number before creating a new ADR.
- **Next ADR number is 030.** (030/031/032 were reclaimed and never canonical; 029 reclassified as `appendix-peer-table-eviction.md` — do not reuse; 027 deleted — do not reuse.)
- When changing any ADR, check for cross-ADR consistency — terms, parameters, and protocol names must match across all ADRs and `architecture.md`.
- `architecture.md` must be updated whenever an ADR changes a user-visible summary point.

**Consistency checks when editing ADRs:**

```bash
grep -rn 'cdn/[a-z]*/v[0-9]' adr/      # find all ALPN references
grep -rn 'TOKEN\|USDC' adr/             # find all token references
grep -rn 'function\|contract\|modifier' adr/  # find Solidity interface references
```

## Adding a New ALPN Protocol Handler

See [CONTRIBUTING.md](CONTRIBUTING.md#adding-a-new-alpn-protocol-handler) for the full step-by-step recipe. Summary:

1. Declare the ALPN constant in `crates/protocol/src/lib.rs`
2. Add wire types (enum + structs) in `crates/protocol/src/message.rs` with discriminant-locking tests
3. Implement `ProtocolHandler` in `crates/node/src/handlers/<name>.rs` — see `probe.rs` as the canonical reference
4. Re-export from `crates/node/src/handlers/mod.rs`
5. Wire onto the `Router` in `crates/node/src/runtime/mod.rs`
6. Add a loopback integration test mirroring `crates/node/tests/probe_loopback.rs`

**Every handler must:** acquire the `ConnectionLimiter`, hold a connection metrics guard, wrap each step in a `tokio::time::timeout`, and map frame/decode errors to [ADR 013 app error codes](adr/013-schema-evolution.md#application-error-codes).

## Testing

- Prefer `cargo nextest run` over `cargo test`
- Integration tests live in `crates/<name>/tests/`
- Use `permissive_limiter` to bypass rate limits in tests not exercising rate-limit behaviour
- Unit tests live in an in-file `#[cfg(test)] mod tests` with `#[allow(clippy::unwrap_used)]` etc. where necessary
