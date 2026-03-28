# ADR 001: Network Topology and Peer Mesh

**Date:** 2026-03-28
**Status:** Draft

## Context

The CDN has three node roles. **Vault nodes** hold canonical content backed by a hidden storage system (S3, NFS, local disk — the network never learns what). **Edge nodes** cache and serve content close to clients. **Clients** consume content. No external origin URL exists; the network is fully self-contained.

Two questions are in scope:

1. How do nodes discover each other and learn what content each holds?
2. How does an edge node resolve a cache miss — pull from a peer edge or pull from a vault node?

## Decision

All staked nodes (vault and edge) form a flat peer mesh with no fixed routing hierarchy. Discovery uses two complementary mechanisms:

- **iroh-gossip** for ongoing content state broadcast. Nodes publish `CacheAnnounce` messages on regional topics (`cdn/region/{cc}/v1`) and a global topic (`cdn/global/v1`). Vault nodes announce all content they hold; edge nodes announce their current cache. Each announcement lists blob hashes (capped at 500 entries) or a Bloom filter for large sets. Both clients and edge nodes maintain a local routing table (`hash → Vec<NodeId>`) built from received announcements. The routing table does not distinguish between vault and edge nodes — the probe step determines which is cheaper and faster.

- **DHT-based lookup** as fallback when the local routing table has no match. Standard Kademlia approach — each node publishes `(hash → nodeIds)` records as content enters its store and removes them when it leaves. TTL is 1 hour.

On a cache miss, an edge node resolves in priority order:

1. **Peer pull** — free, from another staked edge node via `cdn/peer/v1`
2. **Vault pull** — paid, from a vault node via `cdn/client/v1`, same as a client-to-edge payment

Edge-to-edge transfers are unpaid. Edge-to-vault transfers are paid because vault nodes bear real backend costs (storage, egress from their hidden backing store) and have no indirect incentive to serve for free.

Node identity is the iroh `NodeId` (ed25519 public key). All staked nodes register in an on-chain registry mapping `NodeId → QUIC multiaddrs + Ethereum address + role`. Role is either `Vault` or `Edge`. Clients query this registry on first startup to find initial peers.

## Consequences

**Positive:**

- No external infrastructure is reachable from the network — vault nodes completely hide their backends, so no client or edge node can bypass the payment layer by going directly to an origin URL
- Vault nodes and edge nodes participate in the same discovery and transport protocols; the only difference is payment behaviour on pulls
- Regional gossip topics bound message volume: nodes in one region don't receive announcements from irrelevant regions
- Peer-first cache miss resolution means vault nodes are hit only for the first request for content in a region; all subsequent edge nodes in that region pull from peers for free
- The flat mesh is simple to reason about and easy to test at small scale (PoC is tens of nodes)

**Negative:**

- Gossip consistency is eventual — a node that evicts or loses content may still appear in routing tables for up to one DHT TTL (1 hour); clients and edge nodes must handle stale entries by falling back to the next candidate
- Bloom filter announcements (for large caches) introduce false positives: a probe to a node that turns out not to have the blob wastes a round-trip
- Edge-to-edge transfers being unpaid creates a free-rider edge case: a staked edge node can set `accepts_peer_pull: false` and benefit from the mesh without contributing; mitigated by staking requirements but not fully eliminated
- Self-reported region hints (ISO 3166-1 alpha-2) are unverified; a node could misreport its region to appear in more gossip topics
- Vault nodes become the last line of defence for content availability — if all vault nodes for a given blob go offline or are deregistered, the content becomes permanently unavailable. Content owners are responsible for vault node uptime.
