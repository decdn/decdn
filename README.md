# deCDN

Decentralized CDN where nodes cache and serve content-addressed blobs over [iroh](https://iroh.computer/) QUIC, and clients pay per-MB via off-chain USDC payment channels. Rust implementation targeting a PoC of tens of nodes on Arbitrum Sepolia testnet.

## How It Works

- **Nodes** bond TOKEN proportional to declared bandwidth capacity, cache content, and serve BLAKE3-addressed blobs over QUIC
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
| `cdn/dht/v1` | Content discovery via Kademlia DHT (see [ADR 022](adr/022-content-discovery.md)) |
| iroh-gossip (built-in) | Node metadata broadcast (`NodeAnnounce`), node discovery |

### Binaries

| Binary | Role |
|--------|------|
| `decdn-node` | Daemon — runs the cache node service. Single subcommand: `decdn-node run [--config <path>]`. |
| `decdn` | User CLI — `probe`, `node {peers,health,announce,drain,evict,reload}`, `key-gen`, `config {init,validate}`, `bundle {create}`. |

The container image ships `decdn-node` only. Publishers grab the
`decdn-${VERSION}-${TARGET}.tar.gz` release archive; operators grab
both. See [`adr/appendix-binaries.md`](adr/appendix-binaries.md) for
the dockerd-style split rationale.

### Crate Structure

```
crates/
  node/         — daemon binary `decdn-node`: runtime, handlers, admin server, dispatch limits
  cli/          — user CLI binary `decdn`: probe, node admin, key-gen, config, bundle
  common/       — shared types: config schema, identity, AdminRpc trait + DTOs
  protocol/     — shared types, wire format, ALPN message definitions (leaf crate)
  config-types/ — config-vocabulary value types shared by cache + common (leaf crate, #578)
  cache/        — cache engine wrapping iroh-blobs + origin pull-through
  gossip/       — NodeAnnounce pub/sub over iroh-gossip
  incentive/    — payment channels, staking, vouchers (alloy for Ethereum)
  reputation/   — reputation scoring (ADR 008): local EWMA now, gossip aggregation deferred
contracts/      — Solidity contracts + Foundry (repo root, excluded from workspace)
```

### Key Design Decisions

Architecture decision records live in [`adr/`](adr/), with [`adr/architecture.md`](adr/architecture.md) as the living overview. Highlights:

- **Content addressing:** BLAKE3 hashes; clients verify on receipt
- **Dual currency:** USDC for payments, TOKEN for staking/governance
- **No exposed origins:** Origin backends (S3/R2/B2) are opaque per-node config
- **Encryption-agnostic protocol:** the CDN shuttles bytes; ciphertext vs plaintext is the publisher's choice. An optional [encrypted-content publishing](adr/appendix-encrypted-content-publishing.md) appendix documents one deployment pattern (companion app server, epoch-rotated keys).
- **Stale-close defense:** in-process dispute monitor + permissionless `disputeChannel` submission; optional [fraud-detection layer](adr/appendix-fraud-detection.md) anyone can run for `SlashJudge`-bonded challenges
- **Discovery:** `cdn/dht/v1` Kademlia DHT for content discovery from PoC onward; broadcast probe fan-out as bootstrap fallback
- **Reputation:** Interaction-weighted scoring propagated via gossip
- **Governance:** Admin key for PoC; token-weighted governance with safety bounds for production
- **Multi-token payments (post-PoC):** PoC uses USDC only; production supports governance-approved ERC-20 allowlist
- **Content takedown:** Governance-controlled hash blacklisting with regional compliance bodies
- **Client architecture:** Lightweight QUIC endpoints; gossip subscribe (no publish); registry bootstrap with fallback; per-connection ephemeral identity binding
- **Schema evolution:** Varint-length framing, protocol enums, three-tier evolution model (minor/medium/major)
- **On-chain verification:** Dual-key slash signatures (ed25519 wire + secp256k1 on-chain) with optimistic challenge-response
- **0-RTT probing:** QUIC 0-RTT for `cdn/probe/v1` repeat connections, eliminating TLS handshake round trip
- **Liquidity:** Protocol-owned liquidity via Balancer V3 80/20 TOKEN/USDC weighted pool
- **Production L2:** Arbitrum One for all on-chain contracts (PoC on Arbitrum Sepolia)

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for development environment setup, build commands, coding standards, and contribution guidelines.

## Operations

- [Operator runbook](docs/runbook.md)
