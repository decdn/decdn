# ADR 022 — Content Discovery at Scale

**Status:** Proposed
**Deciders:** Core team
**Date:** 2026-04-08

---

## Context

Content discovery answers the question: "which nodes currently hold blob H?" The answer drives both client→node delivery (client picks a node to stream from) and node→node pull-through (a node with a cache miss finds a provider to pull from).

### The scaling problem with probe fan-out

The current design in [ADR 001](001-network.md) uses **broadcast probe fan-out**: on a cache miss, a node sends a `cdn/probe/v1` message to every known peer simultaneously. This works at PoC scale (tens of nodes) but breaks at production scale:

- **O(N) probes per cache miss.** At 1,000 nodes each cache miss generates ~1,000 outbound probe messages. Under the existing 10 fan-outs/second rate limit that is 10,000 probe messages/second/node — a self-DoS risk and a meaningful burden on the peers being probed.
- **O(N) probe overhead for the prober.** Even with rate limiting, the fan-out latency grows with N because the node must wait for the probe collection window on each of those N connections.

These problems exist regardless of network scale. While probe fan-out is fine as a **bootstrap fallback** (when a node first joins and has no routing table), it is the wrong primary mechanism even from day one.

### Why gossip content announcements don't work

Gossiping a `ContentAnnounce` message every time a node caches or evicts a blob would generate unbounded traffic: content churn is proportional to demand × network size, not to a configurable interval like `NodeAnnounce`. High-demand blobs with frequent cache rotation produce interleaved announce/retract storms. Rejected.

### Why hash-prefix range hints don't work

A node cannot advertise "I hold hashes in prefix range 0x00–0x3F" because nodes are economically incentivised to cache **popular** content regardless of hash prefix. Range hints would be uniformly meaningless in an incentive-driven network. Rejected.

### Why a content DHT works

A **Kademlia-based content DHT** has the right properties for an incentive-driven CDN:

- Nodes publish `(hash → NodeId)` records **only when they hold a blob** — a voluntary, self-interested advertisement to attract paying clients. No economic incentive exists to publish records for blobs you don't hold (false records attract probes that reveal the lie, degrading reputation and earnings).
- **O(log N) lookup** — a querying node contacts ~5 peers to find providers at 1,000 nodes.
- **O(log N) publish cost** — a STORE record is pushed to only the K nodes closest to the hash in keyspace. No global broadcast.
- **At PoC scale (30 nodes), DHT is trivially cheap** — `log2(30) ≈ 5`, routing tables hold all 30 peers, FIND_VALUE resolves in 1–2 hops. The overhead is negligible. Implementing DHT from day one avoids a later rewrite and validates the mechanism under controlled test conditions.
- **The probe step is preserved** — DHT lookup narrows the candidate set; `cdn/probe/v1` still confirms live availability and measures latency before any delivery commitment.

iroh's built-in `DhtDiscovery` (mainline BitTorrent DHT via pkarr) is unrelated — it resolves `NodeId → address` on the public internet. A separate content DHT scoped to the registered node set is required.

---

## Decision

`cdn/dht/v1` is the **primary content discovery mechanism from day one**, including PoC. Broadcast probe fan-out is retained as a **bootstrap fallback only** — used when a node's routing table is not yet populated, or when a DHT lookup returns no providers. There is no phased rollout; the DHT is always on.

---

## §1 — `cdn/dht/v1` Protocol

### §1.1 ALPN and Transport

All DHT messages use the ALPN `cdn/dht/v1` over iroh QUIC. Connections are short-lived and request/response oriented — no persistent streams. The same iroh endpoint used for `cdn/probe/v1` and `cdn/client/v1` handles DHT connections.

### §1.2 Message Types

```rust
/// Top-level DHT protocol enum (one variant per request/response pair)
enum DhtMessage {
    FindValue(FindValueRequest),
    FindValueResponse(FindValueResponse),
    Store(StoreRequest),
    StoreAck(StoreAck),
    FindNode(FindNodeRequest),
    FindNodeResponse(FindNodeResponse),
}

/// Query for providers of a specific content hash
struct FindValueRequest {
    hash: Hash,           // BLAKE3 content hash being sought
    requester: NodeId,    // caller's NodeId (for routing table update)
}

/// Response: known providers and/or closer nodes to continue the lookup
struct FindValueResponse {
    hash: Hash,
    providers: Vec<NodeId>,    // nodes known to hold this hash (may be empty)
    closer_nodes: Vec<NodeId>, // K closest nodes to hash in responder's table
}

/// Publish a content record: "I hold this hash"
struct StoreRequest {
    hash: Hash,
    holder: NodeId,          // the node claiming to hold this hash
    published_at_us: u64,    // microseconds since epoch (for TTL)
    signature: Signature,    // holder's ed25519 key signs (hash || published_at_us)
}

struct StoreAck {
    hash: Hash,
    accepted: bool,
}

/// Standard Kademlia node lookup — used during routing table bootstrap
struct FindNodeRequest {
    target: NodeId,
    requester: NodeId,
}

struct FindNodeResponse {
    target: NodeId,
    closer_nodes: Vec<NodeId>,
}
```

### §1.3 Routing Table

Each node maintains a Kademlia routing table: **k-buckets** partitioned by XOR distance from the node's own `NodeId` in 256-bit keyspace. NodeIds are already 32-byte ed25519 public keys — no separate DHT key needed.

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| k (bucket size) | 20 | Standard Kademlia; tolerates churn |
| α (concurrency) | 3 | Parallel lookup RPCs |
| Bucket refresh interval | 1 hour | Keeps routing table fresh |
| Routing table storage | In-memory | Rebuilt via bootstrap on restart |

### §1.4 Content Records and TTL

Content records are stored in-memory at the K nodes closest to the hash in keyspace.

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Record TTL | 1 hour | Bounds stale record lifetime after eviction |
| Re-publish interval | 45 minutes | Re-published while blob is held, before TTL expiry |
| Max providers per hash | 50 | Well above useful redundancy; bounds record size |
| Max records per node | 100,000 | ~50 MB memory at max record size |

A node **stops re-publishing** when it evicts the blob. Stale records self-expire within TTL — no explicit retraction messages needed.

### §1.5 STORE Flow (Cache Event → DHT Publish)

When a node caches blob H:

1. Identify the K closest nodes to H from the local routing table.
2. Send a signed `StoreRequest { hash: H, holder: self.node_id, published_at_us, signature }` to each.
3. Schedule re-publish at T+45 minutes while blob remains cached.

The `StoreRequest` signature (ed25519 over `hash || published_at_us`) lets receiving nodes verify the record was created by the claimed holder. Receiving nodes do **not** verify that the holder actually has the blob — that is the probe step's job. A false publisher fails at probe time, degrading its reputation.

### §1.6 FIND_VALUE Flow (Cache Miss → DHT Lookup)

When a node gets a cache miss for hash H and the probe cache is empty:

1. Check local routing table for the α (=3) closest nodes to H.
2. Send parallel `FindValueRequest { hash: H }` to all α nodes.
3. Iterate: each responder returns known providers or closer nodes (standard iterative Kademlia).
4. Continue until providers are found or lookup converges (no closer nodes returned).
5. Probe the returned `NodeId` set via `cdn/probe/v1` to confirm live availability and measure latency.
6. Select provider by unified node selection score ([ADR 001](001-network.md#node-selection-algorithm)); deliver via `cdn/client/v1`.

**Fallback:** if DHT returns no providers, fall back to broadcast probe fan-out across all known peers (the existing mechanism). If that also returns nothing, the blob is not available in the network.

### §1.7 Bootstrap

On node startup:

1. Build initial routing table from the on-chain registry peer list (same source as the peer table bootstrap in [ADR 019](019-node-onboarding.md)).
2. Issue `FindNode(self.node_id)` to initial peers — standard Kademlia self-lookup that populates k-buckets.
3. **Until routing table has ≥k entries**, use broadcast probe fan-out as fallback for content discovery.

At PoC scale (30 nodes) the routing table is fully populated after a single self-lookup round; the fallback window is seconds.

---

## §2 — Popularity Signals and Market Dynamics

Content discovery in an incentive-driven network requires nodes to learn what content is in demand *before* being asked to serve it. Two complementary signals provide this.

### §2.1 Signal 1: `popular_hashes` Gossip (Advisory)

`NodeAnnounce` carries `popular_hashes` (up to 20 hashes, per [ADR 001](001-network.md)) — a self-reported list of the most-requested hashes a node is actively serving. A node observing hash H in multiple peers' `popular_hashes` lists (default: 3+ peers within 10 minutes) treats H as network-popular and prefetches proactively.

**Integrity:** `popular_hashes` is self-reported and unenforceable. A node could suppress entries to maintain pricing advantage by preventing competitors from prefetching the same content.

**Suppression is economically self-limiting.** A node hiding popular hash H concentrates all demand on itself. Concentrated demand raises its `LoadHint`. Higher `LoadHint` depresses its unified selection score, causing clients to route around it. Under sustained load, suppressing `popular_hashes` reduces earnings — the market corrects without protocol enforcement. This is accepted: `popular_hashes` affects selection quality, not safety.

### §2.2 Signal 2: DHT FIND_VALUE Query Frequency (Non-Suppressible)

In Kademlia, `FindValueRequest` messages for hash H are routed to nodes closest to H in keyspace **regardless of whether those nodes hold H**. A node close to H in keyspace receives all FIND_VALUE queries for H from the entire network without holding H and without receiving any gossip.

This creates a **natural popularity oracle that cannot be suppressed**:

- Many FIND_VALUE queries for H → H is in high demand.
- The node is already well-positioned to be a STORE target for H.
- Prefetching H and publishing a STORE record turns incoming routing queries into paid delivery opportunities.

The signal is honest by construction: FIND_VALUE traffic reflects real client demand, not voluntary self-reporting. No node can suppress it — the routing traffic arrives regardless of what anyone gossips.

### §2.3 Prefetch Decision

A node prefetches hash H when **any** signal crosses its threshold:

| Signal | Default threshold | Action |
|--------|-------------------|--------|
| `popular_hashes` appearances | ≥3 distinct peers within 10 min | DHT lookup → pull → STORE publish |
| FIND_VALUE query rate for H | ≥5 queries within 5 min | DHT lookup → pull → STORE publish |
| Local miss rate for H | ≥3 misses within 5 min | DHT lookup → pull → STORE publish |

All three thresholds are configurable. All three trigger the same action: DHT FIND_VALUE lookup to find a provider, pull via `cdn/client/v1` (paid), cache locally, publish STORE record.

### §2.4 No Discovery Fees

DHT STORE and FIND_VALUE operations carry no protocol-level fee. The incentive to publish STORE records is indirect: advertising that you hold a blob attracts probe traffic, which converts to paid delivery. Charging for DHT operations would create a new attack surface (collect fee, fail to hold) requiring a new slash condition. All fees remain on delivery.

---

## §3 — Interaction with Existing Protocols

| Mechanism | Interaction with DHT |
|-----------|---------------------|
| `cdn/probe/v1` | Unchanged. DHT provides candidates; `cdn/probe/v1` confirms live availability and measures latency. Probe cache (15s TTL, [ADR 001](001-network.md)) still prevents redundant probes for recently confirmed providers. |
| `cdn/client/v1` | Unchanged. All delivery is paid; DHT affects only how providers are discovered. |
| `NodeAnnounce` gossip | Unchanged. `popular_hashes` field reused as Signal 1. No new gossip message types. |
| Reputation system ([ADR 008](008-reputation.md)) | A node publishing a false STORE record fails at probe time → reputation penalty → fewer clients selected. No new slash condition needed. |
| Eviction hold ([ADR 005](005-protocol.md)) | Nodes stop re-publishing DHT records when a blob is evicted. TTL ensures stale records expire within 1 hour. |
| Client discovery ([ADR 012](012-client.md)) | Clients use DHT FIND_VALUE for content discovery the same way nodes do. Probe fan-out bootstrap fallback applies equally. |

---

## §4 — Schema Evolution

`cdn/dht/v1` follows the standard evolution model from [ADR 013](013-schema-evolution.md):

- **Minor** (new optional fields on existing message types): no ALPN bump.
- **Medium** (new mandatory fields): `cdn/dht/v2` ALPN.
- **Major** (incompatible routing changes): new ALPN + migration period.

`DhtMessage` uses a top-level enum consistent with the per-ALPN protocol enum pattern in [ADR 013 — Protocol Enums](013-schema-evolution.md#protocol-enums). Unknown variants are silently dropped.

---

## §5 — Acceptance Criteria

1. A node in a 30-node PoC network can discover providers for a cached blob in ≤3 FIND_VALUE hops.
2. A node in a 500-node network can discover providers in ≤5 FIND_VALUE hops.
3. A cache event (blob added) generates ≤k (=20) outgoing STORE messages, not O(N).
4. A stale STORE record (node evicted the blob) expires within TTL (1 hour) with no explicit retraction.
5. A false STORE record (node claims to hold a blob it doesn't) fails at the probe step; the publishing node incurs a reputation penalty within one gossip cycle.
6. During bootstrap (routing table < k entries), broadcast probe fan-out is used as fallback; the fallback window completes within 2 self-lookup rounds.
7. A node observing ≥5 FIND_VALUE queries for hash H within 5 minutes initiates a prefetch for H.
8. A node suppressing `popular_hashes` entries for a popular blob experiences measurable `LoadHint` increase under sustained demand, verifiable in [ADR 020](020-observability.md) metrics.

---

## §6 — Alternatives Considered

### Broadcast probe fan-out as primary mechanism

The existing approach in [ADR 001](001-network.md). Generates O(N) probe messages per cache miss. Retained as a bootstrap fallback and emergency fallback when DHT returns no providers. Not suitable as the primary mechanism even at PoC scale, because the O(N) cost is a design ceiling rather than an operational limit — the network should not be architected around it. Probe fan-out is a sound fallback because it is maximally complete: a miss definitively means no node holds the blob.

### Gossip content announcements

Each cache/evict event generates a gossip message. Rejected: unbounded traffic proportional to cache churn, retraction storms under high eviction rates. See Context section.

### Hash-prefix range hints in `NodeAnnounce`

Rejected: economically irrational in an incentive-driven network where nodes cache popular content regardless of hash prefix. See Context section.

### iroh mainline DHT (`DhtDiscovery` / pkarr)

Rejected for this use case. iroh's built-in DHT resolves `NodeId → address` on the public mainline BitTorrent DHT. It does not support content-hash records, is not scoped to the deCDN registered-node set, and exposes lookup patterns to the public internet.

### Indexer nodes (`cdn/search/v1`)

Not rejected — deferred. Dedicated indexer nodes aggregating DHT records into a searchable catalog are a natural complement once the network grows large enough to justify a separate indexing tier. Out of scope for this ADR.

### Full libp2p Kademlia

Deferred. `libp2p-kad` is battle-tested but built on libp2p's transport stack. Bridging to iroh QUIC adds a large dependency and upstream governance coupling. The lightweight subset specified here covers the deCDN use case with ~300–500 lines of Rust.

---

## §7 — Cross-ADR Consistency

- **ADR 001** Future Work section ("Scaling Content Discovery") is superseded by this ADR. The three strategies listed there are resolved: selective fan-out is subsumed by DHT, content DHT is formalised here, gossip content hints are rejected.
- **ADR 005** probe protocol is unchanged. DHT provides candidates only.
- **ADR 008** reputation penalties for delivery failure cover false STORE record publishers.
- **ADR 012** client discovery uses DHT FIND_VALUE; probe fan-out bootstrap fallback applies to clients equally.
- **ADR 013** schema evolution rules apply to `cdn/dht/v1`.
- **ADR 020** SHOULD add DHT subsystem metrics: `decdn_dht_store_published_total`, `decdn_dht_findvalue_queries_total`, `decdn_dht_routing_table_size`.
