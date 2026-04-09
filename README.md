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
| `cdn/dht/v1` | Content discovery via Kademlia DHT (see [ADR 022](adr/022-content-discovery.md)) |
| iroh-gossip (built-in) | Node metadata broadcast (`NodeAnnounce`), node discovery |

### Companion Protocol (App Server)

| ALPN | Purpose |
|------|---------|
| `cdn/keys/v1` | Epoch key delivery, play requests, offline leases (app server) |

> The app server is not a CDN protocol participant — see [ADR 006](adr/006-e2e-encryption.md).

### Crate Structure

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
