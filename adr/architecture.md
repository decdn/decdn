# Architecture Overview

**Date:** 2026-03-28
**Status:** Living document — updated as ADRs are added or revised

---

## What This Is

A decentralized CDN with three node roles:

- **Vault nodes** hold canonical content behind a hidden backend (S3, NFS, local disk — irrelevant to the network). They stake TOKEN to publish content and are the only entry point for content into the network. No external origin URL is ever exposed.
- **Edge nodes** cache content and serve it close to clients. They stake TOKEN to participate in the peer mesh and compete on price and latency.
- **Clients** consume content. They pay edge nodes (or vault nodes directly) per MB via off-chain USDC payment channels.

The PoC scope is tens of nodes on a testnet, proving the core delivery and payment protocol.

---

## System Diagram

```
       ┌──────────────────────────────────────────┐
       │  Vault Nodes  (hidden backend)           │
       │  Staked content publishers               │
       │  Sets price ceiling per blob             │
       └───────────────┬──────────────────────────┘
                       │ paid pull (cdn/client/v1)
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
  ┌────────────┐ ┌────────────┐ ┌────────────┐
  │ Edge Node  │◄┤ Edge Node  │◄┤ Edge Node  │
  │ (cached)   ├►│ (cached)   ├►│ (cached)   │
  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
        │  peer mesh (cdn/peer/v1, free between edges)
     pays           pays           pays
   per-MB          per-MB         per-MB
   (USDC)          (USDC)         (USDC)
        │               │               │
  ┌─────▼──────┐ ┌──────▼─────┐ ┌──────▼─────┐
  │   Client   │ │   Client   │ │   Client   │
  └────────────┘ └────────────┘ └────────────┘
```

Clients probe candidates (edge nodes or vault nodes directly), pick the best by `rate_per_mb × rtt_ms`, stream over `cdn/client/v1`, and pay via off-chain USDC vouchers. Edge nodes pull from peers for free; on a cache miss they pay a vault node wholesale and serve subsequent clients at a markup.

---

## Architectural Decisions

### [ADR 000 — Language and Core Networking Stack](000-language.md)

**Rust + iroh (0.35+).**

The implementation language is Rust. The networking stack is iroh, which provides QUIC transport, NAT traversal, content-addressed blob transfer, and gossip as a cohesive unit. A single statically linked binary runs as vault node, edge node, or client depending on configuration.

---

### [ADR 001 — Network Topology and Peer Mesh](001-network.md)

**Flat peer mesh. Gossip for content availability. DHT for lookup. Vault nodes replace external origin.**

All staked nodes (vault and edge) form a flat mesh. Cache state is broadcast over iroh-gossip on regional topics. On a cache miss, edge nodes pull from peers for free, then from a vault node (paid). No external URL is ever accessed — the network is fully self-contained.

---

### [ADR 002 — Content Addressing](002-content-addressing.md)

**BLAKE3 content-addressed blobs. Vault node backends are opaque to the network.**

Every blob is identified by its BLAKE3 hash. Clients verify received bytes against the known hash. The hash→backend mapping is internal to each vault node and never shared — no node in the network can learn or bypass the vault's backing storage.

---

### [ADR 003 — Payment Model](003-payments.md)

**Off-chain USDC payment channels at two tiers. Market-driven rates.**

Clients pay edge nodes per MB. Edge nodes pay vault nodes per MB for initial content pulls, then amortise that cost across many client deliveries. Vault nodes set the effective price ceiling. Rates are fully market-driven within governance-set bounds.

---

### [ADR 004 — Dual-Currency Token Model](004-tokenomics.md)

**USDC for payments. TOKEN for staking, governance, and fee discounts.**

TOKEN is not used for payments. Both vault nodes (to publish content) and edge nodes (to serve content) must stake TOKEN. Staking cost creates accountability and Sybil resistance. 20% of protocol fees buy back and burn TOKEN. Fixed supply of 1B at genesis.

---

### [ADR 005 — Wire Protocol](005-protocol.md)

**Four ALPN-identified protocols. `cdn/client/v1` covers all paid delivery tiers.**

| ALPN | Purpose |
| --- | --- |
| `cdn/probe/v1` | Parallel latency + availability check before node selection |
| `cdn/client/v1` | Paid delivery: client→edge, edge→vault, client→vault |
| `cdn/peer/v1` | Unpaid pull between staked edge nodes only |
| iroh-gossip built-in | Content availability and node discovery |

`redirect` in `StreamResponse` always points to a NodeId, never an external URL. The vault's backend is never revealed.

---

## Key Invariants

- No external origin URL exists — all content enters the network through staked vault nodes
- A node cannot deliver paid content without being reachable via iroh NodeId; the backend is always hidden
- An edge node cannot earn without delivering verifiable bytes — BLAKE3 hash mismatch voids payment
- A node cannot join the peer mesh without staking — prevents free-riders and provides a slashable bond
- Payment channels amortize on-chain costs across an entire session; per-MB payments are off-chain
- Safety bounds on all governable parameters are hardcoded — governance cannot set fees to 100% or stake to zero

---

## What Is Not Decided Yet

- Production L2 choice (Arbitrum One, Base, or other) — gated on PoC validation
- Vault node minimum stake vs edge node minimum stake — same amount currently, may diverge
- Watchtower design for offline node protection during channel disputes
- Parallel streaming from multiple nodes for a single blob (protocol supports it, not prioritised)
- Whether vault nodes can charge edge nodes differently from clients (wholesale vs retail rates)
