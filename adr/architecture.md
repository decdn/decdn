# Architecture Overview

**Date:** 2026-03-28
**Status:** Living document — updated as ADRs are added or revised

---

## What This Is

A decentralized CDN with two participant roles:

- **Nodes** (providers) cache and serve content. They stake TOKEN to participate in the peer mesh and compete on price and latency. Some nodes are configured with an origin backend (S3, NFS, local disk) making them the canonical source for specific content — this is a deployment choice, not a protocol distinction. No external origin URL is ever exposed.
- **Clients** consume content. They pay nodes per MB via off-chain USDC payment channels.

The PoC scope is tens of nodes on a testnet, proving the core delivery and payment protocol.

---

## System Diagram

```
  ┌────────────┐ ┌────────────┐ ┌────────────┐
  │    Node    │◄┤    Node    │◄┤    Node    │
  │  (cached)  ├►│  (origin)  ├►│  (cached)  │
  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
        │  all transfers paid via cdn/client/v1 (USDC)
     pays           pays           pays
   per-MB          per-MB         per-MB
   (USDC)          (USDC)         (USDC)
        │               │               │
  ┌─────▼──────┐ ┌──────▼─────┐ ┌──────▼─────┐
  │   Client   │ │   Client   │ │   Client   │
  └────────────┘ └────────────┘ └────────────┘
```

Clients probe candidate nodes, pick the best by `rate_per_mb × rtt_ms`, stream over `cdn/client/v1`, and pay via off-chain USDC vouchers. On a cache miss, a node pulls from another node that has the blob (paid via `cdn/client/v1`) and caches locally. Every byte delivered — whether client→node or node→node — is paid.

---

## Architectural Decisions

### [ADR 000 — Language and Core Networking Stack](000-language.md)

**Rust + iroh (0.35+).**

The implementation language is Rust. The networking stack is iroh, which provides QUIC transport, NAT traversal, content-addressed blob transfer, and gossip as a cohesive unit. A single statically linked binary runs as a node or client depending on configuration.

---

### [ADR 001 — Network Topology and Peer Mesh](001-network.md)

**Flat peer mesh. Gossip for content availability. DHT for lookup.**

All staked nodes form a flat mesh. Cache state is broadcast over iroh-gossip on regional topics. On a cache miss, nodes pull from another node that has the blob (paid via `cdn/client/v1`). No external URL is ever accessed — the network is fully self-contained.

---

### [ADR 002 — Content Addressing](002-content-addressing.md)

**BLAKE3 content-addressed blobs. Node backends are opaque to the network.**

Every blob is identified by its BLAKE3 hash. Clients verify received bytes against the known hash. The hash→backend mapping is internal to each origin-backed node and never shared — no participant in the network can learn or bypass the node's backing storage.

---

### [ADR 003 — Payment Model](003-payments.md)

**Off-chain USDC payment channels. Market-driven rates.**

Clients pay nodes per MB. On a cache miss, nodes pay origin-backed nodes per MB for initial content pulls, then amortise that cost across many client deliveries. Origin-backed nodes set the effective price ceiling (reflecting their backend egress costs). Rates are fully market-driven within governance-set bounds.

---

### [ADR 004 — Dual-Currency Token Model](004-tokenomics.md)

**USDC for payments. TOKEN for staking, governance, and fee discounts.**

TOKEN is not used for payments. All nodes must stake TOKEN to participate. Staking cost creates accountability and Sybil resistance. 20% of protocol fees buy back and burn TOKEN. Fixed supply of 1B at genesis.

---

### [ADR 005 — Wire Protocol](005-protocol.md)

**Three ALPN-identified protocols. `cdn/client/v1` covers all paid delivery.**

| ALPN | Purpose |
| --- | --- |
| `cdn/probe/v1` | Parallel latency + availability check before node selection |
| `cdn/client/v1` | Paid delivery: client→node, node→node (cache miss) |
| iroh-gossip built-in | Content availability and node discovery |

`redirect` in `StreamResponse` always points to a NodeId, never an external URL. The origin backend is never revealed.

---

### [ADR 006 — End-to-End Encryption and Key Distribution](006-e2e-encryption.md)

**Envelope encryption with epoch-rotated key distribution.**

Each blob is encrypted once at ingest with a random symmetric key (XChaCha20-Poly1305). The ciphertext is content-addressed and cached normally — one hash, one copy for all clients. An app server gates access: on each play request it wraps the blob key with a rotating epoch key and seals it to the client's public key. Epoch keys are pushed over an authenticated persistent connection; closing the connection revokes access within one epoch (5 minutes). CDN nodes only ever see ciphertext.

---

## Key Invariants

- No external origin URL exists — content enters the network through origin-backed nodes whose backends are hidden
- A node cannot deliver paid content without being reachable via iroh NodeId; the backend is always hidden
- A node cannot earn without delivering verifiable bytes — BLAKE3 hash mismatch voids payment
- A node cannot join the peer mesh without staking — prevents free-riders and provides a slashable bond
- Payment channels amortize on-chain costs across an entire session; per-MB payments are off-chain
- Safety bounds on all governable parameters are hardcoded — governance cannot set fees to 100% or stake to zero

---

## What Is Not Decided Yet

- Production L2 choice (Arbitrum One, Base, or other) — gated on PoC validation
- Watchtower design for offline node protection during channel disputes
- Parallel streaming from multiple nodes for a single blob (protocol supports it, not prioritised)
