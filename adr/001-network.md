# ADR 001: Network Topology and Peer Mesh

**Date:** 2026-03-28
**Status:** Draft

## Context

The CDN requires edge nodes to discover each other, announce what they have cached, and pull blobs from peers before hitting origin. We need a topology model that keeps coordination simple, avoids central routers, and scales without requiring nodes to maintain full global state.

Two questions are in scope:

1. How do nodes discover peers and learn what content each peer has?
2. How does an edge node resolve a cache miss — pull from peer or fall back to origin?

## Decision

Edge nodes form a flat peer mesh with no fixed routing hierarchy. Discovery uses two complementary mechanisms:

- **iroh-gossip** for ongoing cache state broadcast. Nodes publish `CacheAnnounce` messages on regional topics (`cdn/region/{cc}/v1`) and a global topic (`cdn/global/v1`). Each announcement lists cached blob hashes (capped at 500 entries) or a Bloom filter for large caches. Both clients and edge nodes maintain a local routing table (`hash → Vec<NodeId>`) built from received announcements.

- **DHT-based lookup** as fallback when the local routing table has no match for a requested hash. Standard Kademlia approach — each edge node publishes `(hash → nodeIds)` records when content enters its cache and removes them on eviction. TTL is 1 hour.

On a cache miss, an edge node resolves in priority order: peer pull (free, staked nodes only) → origin pull → redirect to origin gateway.

Edge-to-edge transfers are unpaid. The serving edge is compensated indirectly — a denser peer mesh reduces every node's origin egress costs.

Node identity is the iroh `NodeId` (ed25519 public key). Staked nodes are registered in an on-chain registry mapping `NodeId → QUIC multiaddrs + Ethereum address`. This registry is the bootstrap mechanism; clients and new nodes query it on first startup to find initial peers.

## Consequences

**Positive:**

- No central coordinator or DHT bootstrap server is a single point of failure — the on-chain registry is the only coordination point, and it is read-only for most operations
- Regional gossip topics bound message volume: nodes in one region don't receive cache announcements from nodes in irrelevant regions
- Peer-first cache miss resolution means origin egress costs drop as the network grows; the first request for content in a region pays origin cost, all subsequent ones pay peer cost
- The flat mesh is simple to reason about and easy to test at small scale (PoC is tens of nodes)

**Negative:**

- Gossip consistency is eventual — a node that evicts content may still appear in routing tables for up to one DHT TTL (1 hour); clients must handle stale entries gracefully by falling back to the next candidate
- Bloom filter announcements (for large caches) introduce false positives: clients make a direct round-trip to a node that turns out not to have the blob, wasting a connection attempt
- Edge-to-edge transfers being unpaid creates a free-rider edge case: a staked node can set `accepts_peer_pull: false` and benefit from the mesh without contributing; mitigated by staking requirements (unstaked nodes are rejected) but not fully eliminated
- Self-reported region hints (ISO 3166-1 alpha-2) are unverified; a node could lie about its region to appear in more gossip topics
