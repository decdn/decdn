# Architecture Overview

**Date:** 2026-03-28
**Status:** Living document — updated as ADRs are added or revised

---

## What This Is

A decentralized CDN where **storage stays centralized** (S3/R2/object store) and **delivery is decentralized**. Edge nodes operated by third parties cache content close to end users, serve it over QUIC, and are paid per MB delivered. The operator of the origin pays edge nodes; edge nodes compete on latency and price.

The PoC scope is tens of nodes on a testnet, proving the core delivery and payment protocol. Audio streaming is the motivating use case but the network is general-purpose blob delivery.

---

## System Diagram

```
                  ┌─────────────────────────┐
                  │   Origin (S3/R2/etc.)   │
                  │   Canonical blob store  │
                  └────────────┬────────────┘
                               │ pull (last resort)
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
       ┌────────────┐  ┌────────────┐  ┌────────────┐
       │  Edge Node │◄─┤  Edge Node │◄─┤  Edge Node │
       │  (cached)  ├─►│  (cached)  ├─►│  (cached)  │
       └──────┬─────┘  └──────┬─────┘  └──────┬─────┘
              │  peer mesh (cdn/peer/v1, free)  │
           pays             pays            pays
        per-MB            per-MB          per-MB
        (USDC)            (USDC)          (USDC)
              │               │               │
       ┌──────▼─────┐  ┌──────▼─────┐  ┌──────▼─────┐
       │   Client   │  │   Client   │  │   Client   │
       └────────────┘  └────────────┘  └────────────┘
```

Clients probe candidates, pick the best node, stream over `cdn/client/v1`, and pay via off-chain USDC vouchers. Edge nodes form a peer mesh and pull from each other before touching origin. Origin is accessed as infrequently as possible.

---

## Architectural Decisions

### [ADR 000 — Language and Core Networking Stack](000-language.md)

**Rust + iroh (0.35+).**

The implementation language is Rust. The networking stack is iroh, which provides QUIC transport, NAT traversal, content-addressed blob transfer, and gossip as a cohesive unit. A single statically linked binary runs as edge node or client depending on configuration.

---

### [ADR 001 — Network Topology and Peer Mesh](001-network.md)

**Flat peer mesh. Gossip for cache announcements. DHT for content lookup.**

Edge nodes form a flat mesh with no routing hierarchy. Cache state is broadcast over iroh-gossip on regional topics. DHT lookup serves as fallback when the local routing table has no match. On a cache miss, nodes resolve via peer pull before falling back to origin. Edge-to-edge transfers are unpaid — the incentive is lower origin egress cost for everyone.

---

### [ADR 002 — Content Addressing](002-content-addressing.md)

**BLAKE3 content-addressed blobs. Manifest as canonical track identifier.**

Every blob is identified by its BLAKE3 hash. Clients verify received bytes against the known hash — an edge node cannot serve corrupted data without immediate detection. This makes the hash both the content identifier and the delivery proof, eliminating the need for a separate proof-of-delivery oracle.

---

### [ADR 003 — Payment Model](003-payments.md)

**Off-chain USDC payment channels. Voucher per MB delivered.**

Clients open a USDC payment channel with an edge node via the `StablePaymentChannel` contract. As bytes are delivered, the client signs cumulative off-chain vouchers. The edge node submits the final voucher on-chain to close the channel. USDC denomination gives edge node operators predictable unit economics independent of native token price.

---

### [ADR 004 — Dual-Currency Token Model](004-tokenomics.md)

**USDC for payments. AUDIO for staking, governance, and fee discounts.**

The native token (AUDIO) is not used for delivery payments. It is used for: staking (required to operate an edge node), governance (parameter votes), fee discounts (≥10× minimum stake → 1.5% fee instead of 3%), and a buyback-and-burn sink funded by 20% of protocol fees. Fixed supply of 1B AUDIO at genesis.

---

### [ADR 005 — Wire Protocol](005-protocol.md)

**Four ALPN-identified protocols over iroh QUIC connections.**

| ALPN | Purpose |
|------|---------|
| `cdn/probe/v1` | Parallel latency + availability check before node selection |
| `cdn/client/v1` | Blob delivery with USDC payment vouchers |
| `cdn/peer/v1` | Unpaid peer blob pull between staked edge nodes |
| iroh-gossip built-in | Cache announcements and node discovery |

Clients probe candidates in parallel (200ms window), score by `rate_per_mb × rtt_ms`, then stream from the winner. All messages serialized with postcard.

---

## Key Invariants

- An edge node cannot earn without delivering verifiable bytes — BLAKE3 hash mismatch voids payment
- A node cannot join the peer mesh without staking — prevents free-riders and provides a slashable bond
- Origin is the source of truth; edge caches are opportunistic and evictable
- Payment channels amortize on-chain costs across an entire session; per-MB payments are off-chain
- Safety bounds on all governable parameters are hardcoded — governance cannot set fees to 100% or stake to zero

---

## What Is Not Decided Yet

- Production L2 choice (Arbitrum One, Base, or other) — gated on PoC validation
- Content catalog decentralization — currently a centralized operator API; DHT is a future option
- Watchtower design for offline client protection during channel disputes
- Parallel streaming from multiple edge nodes for a single blob (protocol supports it, not prioritized)
