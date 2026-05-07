# ADR 022 — Content Discovery at Scale

**Status:** Proposed
**Deciders:** Core team
**Date:** 2026-04-08

## Context

Content discovery answers the question: "which nodes currently hold blob H?" The answer drives both client→node delivery (client picks a node to stream from) and node→node pull-through (a node with a cache miss finds a provider to pull from).

### The scaling problem with probe fan-out

An earlier design in [ADR 001](001-network.md) used **broadcast probe fan-out**: on a cache miss, a node sends a `cdn/probe/v1` message to every known peer simultaneously. This works at PoC scale (tens of nodes) but breaks at production scale:

- **O(N) probes per cache miss.** At 1,000 nodes each cache miss generates ~1,000 outbound probe messages. Under a 10 fan-outs/second rate limit that would have been 10,000 probe messages/second/node — a self-DoS risk and a meaningful burden on the peers being probed.
- **O(N) probe overhead for the prober.** Even with rate limiting, the fan-out latency would have grown with N because the node would need to wait for the probe collection window on each of those N connections.

These problems would have existed regardless of network scale. Probe fan-out is the wrong primary mechanism even from day one — the DHT is.

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

## Decision

`cdn/dht/v1` is the **primary content discovery mechanism from day one**, including PoC. The DHT bootstraps from `StakingRegistry.getActiveNodes()` — a freshly-started node's first peers come from the on-chain registry and immediately participate in DHT lookups, so there is no separate bootstrap window during which DHT cannot resolve. When a DHT lookup returns no providers, the on-chain origin directory (§ Origin discovery below) is the deterministic last-resort fallback. Broadcast probe fan-out is not part of the protocol. There is no phased rollout; the DHT is always on.

### 1. `cdn/dht/v1` Protocol

#### 1.1 ALPN and Transport

All DHT messages use the ALPN `cdn/dht/v1` over iroh QUIC. Connections are short-lived and request/response oriented — no persistent streams. The same iroh endpoint used for `cdn/probe/v1` and `cdn/client/v1` handles DHT connections.

#### 1.2 Message Types

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

#### 1.3 Routing Table

Each node maintains a Kademlia routing table: **k-buckets** partitioned by XOR distance from the node's own `NodeId` in 256-bit keyspace. NodeIds are already 32-byte ed25519 public keys — no separate DHT key needed.

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| k (bucket size) | 20 | Standard Kademlia; tolerates churn |
| α (concurrency) | 3 | Parallel lookup RPCs |
| Bucket refresh interval | 1 hour | Keeps routing table fresh |
| Routing table storage | In-memory | Rebuilt via bootstrap on restart |

#### 1.4 Content Records and TTL

Content records are stored in-memory at the K nodes closest to the hash in keyspace.

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Record TTL | 1 hour | Bounds stale record lifetime after eviction |
| Re-publish interval | 45 minutes | Re-published while blob is held, before TTL expiry |
| Max providers per hash | 50 | Well above useful redundancy; bounds record size |
| Max records per node | 100,000 | ~50 MB memory at max record size |

A node **stops re-publishing** when it evicts the blob. Stale records self-expire within TTL — no explicit retraction messages needed.

#### 1.5 STORE Flow (Cache Event → DHT Publish)

When a node caches blob H:

1. Identify the K closest nodes to H from the local routing table.
2. Send a signed `StoreRequest { hash: H, holder: self.node_id, published_at_us, signature }` to each.
3. Schedule re-publish at T+45 minutes while blob remains cached.

The receiving node MUST verify the `StoreRequest` signature (ed25519 over `hash || published_at_us`) against `holder`'s key — NodeIds are 32-byte ed25519 public keys (§1.3), so the signature alone binds the record to its claimed origin. As a fast precheck the receiver MUST also reject any record whose `holder` does not equal the authenticated NodeId of the inbound QUIC connection; the signature check would catch impersonation downstream, but the equality check lets mismatched records fail before the signature work. The receiver MUST additionally verify that `holder` is in the cached active-staker set (populated from `StakingRegistry.getActiveNodes()` per [ADR 019 § Bootstrap](019-node-onboarding.md)) before accepting the record; non-staked publishers are rejected with `StoreAck { accepted: false }`. Receiving nodes do **not** verify that the holder actually has the blob — that is the probe step's job. A false STORE publisher (a node claiming to hold a blob it does not) fails at probe time, degrading its reputation. **Note:** the term "publisher" in this ADR refers to a node publishing a DHT STORE record (an act of advertising). It is distinct from the on-chain *content publisher* identity defined in [ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces), which is an Ethereum address registered in `PublisherRegistry`. Where confusion is possible this ADR uses "STORE publisher" or "holder" for the DHT-record sender.

#### 1.6 FIND_VALUE Flow (Cache Miss → DHT Lookup)

When a node gets a cache miss for hash H and the probe cache is empty:

1. Check local routing table for the α (=3) closest nodes to H.
2. Send parallel `FindValueRequest { hash: H }` to all α nodes.
3. Iterate: each responder returns known providers or closer nodes (standard iterative Kademlia).
4. Continue until providers are found or lookup converges (no closer nodes returned).
5. Probe the returned `NodeId` set via `cdn/probe/v1` to confirm live availability and measure latency.
6. Select provider by unified node selection score ([ADR 001](001-network.md#node-selection-algorithm)); deliver via `cdn/client/v1`.

**Provider ordering.** When a responder returns multiple providers in `FindValueResponse.providers`, the order SHOULD be randomized so probe traffic spreads across the set rather than concentrating on whichever provider was inserted first. The wire protocol does not enforce per-responder ordering — operators can run modified implementations — randomization is the recommended default.

**Fallback:** if DHT returns no providers, fall back to the on-chain origin directory (§ Origin discovery below). If that also returns nothing, the blob is not available in the network.

**Origin discovery.** A requester that prefers an authorized origin for a hash (e.g., a cache-miss pull where freshness from a publisher-committed source is desirable) discovers candidates through the standard DHT path. The DHT does not discriminate origin vs cache providers — `StoreRequest` is the same wire format regardless of role — so any holder may publish a record. The wire protocol does not surface origin-vs-cache status at probe time either; instead, the requester resolves origin status off-chain by reading `PublisherRegistry.namespaceOf(hash)` and `OriginAssignment.getOrigins(namespaceId)` and intersecting against the probed peer set.

The on-chain origin set is also the directory of last resort if the DHT returns no providers: resolve namespaces via `PublisherRegistry.namespaceOf(hash)` and union the operator-address sets via `OriginAssignment.getOrigins(namespaceId)` for each ([ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)). Operator addresses map to NodeIds via `StakingRegistry.nodeIdOf(operator)` (a single read per operator, returning `(nodeId, active)` — see [ADR 003 § NodeId-to-Ethereum Binding](003-payments.md#nodeid-to-ethereum-binding) and [ADR 016 § Off-Chain Read API](016-contract-interactions.md#off-chain-read-api-client--node-bootstrap)); the requester then probes those NodeIds directly. The chain of reads (namespace lookup → operator union → NodeId binding → blacklist filter) has no inter-call dependencies *within* a tier and is straightforward to batch via the standard `Multicall3` aggregator deployed on Arbitrum, collapsing the fallback to a small number of RPC round-trips; the implementation pattern is left to client libraries since it does not affect protocol semantics. This fallback is uncommon — under normal operation the DHT contains entries for every actively-serving authorized origin — but provides a deterministic recovery path during DHT churn or bootstrap. For default-open content (`namespaceId == 0`) the on-chain directory is `OriginAssignment.getOrigins(0)` — the DAO-maintained default-open allow-list — resolved to NodeIds the same way as for registered namespaces. The on-chain fallback is always queryable; it returns an empty set during the bootstrap window before the allow-list is first activated (`defaultOpenAllowlistActive == false`), in which case clients fall through to the DHT path. Once activated, the allow-list populates the fallback the same way registered namespaces do.

#### 1.7 Bootstrap

On node startup:

1. Build initial routing table from the on-chain registry peer list (same source as the peer table bootstrap in [ADR 019](019-node-onboarding.md)).
2. Issue `FindNode(self.node_id)` to initial peers — standard Kademlia self-lookup that populates k-buckets.

The registry-seeded peer list participates in DHT lookups immediately, so there is no separate bootstrap window during which content discovery is unavailable. If a `FindValue` lookup returns no providers during the first few seconds — before k-buckets are fully populated — the on-chain origin directory (§ Origin discovery above) provides the deterministic fallback. At PoC scale (30 nodes) the routing table is fully populated after a single self-lookup round.

### 2. Popularity Signals and Market Dynamics

Content discovery in an incentive-driven network requires nodes to learn what content is in demand *before* being asked to serve it. Two complementary signals provide this.

#### 2.1 Signal 1: DHT FIND_VALUE Query Frequency (Non-Suppressible)

In Kademlia, `FindValueRequest` messages for hash H are routed to nodes closest to H in keyspace **regardless of whether those nodes hold H**. A node close to H in keyspace receives all FIND_VALUE queries for H from the entire network without holding H and without receiving any gossip.

This creates a **natural popularity oracle that cannot be suppressed**:

- Many FIND_VALUE queries for H → H is in high demand.
- The node is already well-positioned to be a STORE target for H.
- Prefetching H and publishing a STORE record turns incoming routing queries into paid delivery opportunities.

The signal is honest by construction: FIND_VALUE traffic reflects real client demand, not voluntary self-reporting. No node can suppress it — the routing traffic arrives regardless of what anyone gossips.

#### 2.2 Signal 2: Local Cache-Miss Frequency

Each node tracks cache miss timestamps per hash in a bounded map ([ADR 001 § Prefetching from Local Demand](001-network.md)). A hash crossing the local-miss threshold (default: 3 misses in 5 minutes) is prefetched proactively.

#### 2.3 Prefetch Decision

A node prefetches hash H when **either** signal crosses its threshold:

| Signal | Default threshold | Action |
|--------|-------------------|--------|
| FIND_VALUE query rate for H | ≥5 queries within 5 min | DHT lookup → pull → STORE publish |
| Local miss rate for H | ≥3 misses within 5 min | DHT lookup → pull → STORE publish |

Both thresholds are configurable. Both trigger the same action: DHT FIND_VALUE lookup to find a provider, pull via `cdn/client/v1` (paid), cache locally, publish STORE record.

#### 2.4 No Discovery Fees

DHT STORE and FIND_VALUE operations carry no protocol-level fee. The incentive to publish STORE records is indirect: advertising that you hold a blob attracts probe traffic, which converts to paid delivery. Charging for DHT operations would create a new attack surface (collect fee, fail to hold) requiring a new slash condition. All fees remain on delivery.

### 3. Interaction with Existing Protocols

| Mechanism | Interaction with DHT |
|-----------|---------------------|
| `cdn/probe/v1` | Unchanged. DHT provides candidates; `cdn/probe/v1` confirms live availability and measures latency. Probe cache (15s TTL, [ADR 001](001-network.md)) still prevents redundant probes for recently confirmed providers. |
| `cdn/client/v1` | Unchanged. All delivery is paid; DHT affects only how providers are discovered. |
| `NodeAnnounce` gossip | Unchanged. Carries node-level metadata only (region, load); demand signals are derived from DHT FIND_VALUE traffic and local cache misses. No new gossip message types. |
| Reputation system ([ADR 008](008-reputation.md)) | A node publishing a false STORE record fails at probe time → reputation penalty → fewer clients selected. No new slash condition needed. |
| Eviction hold ([ADR 005](005-protocol.md)) | Nodes stop re-publishing DHT records when a blob is evicted. TTL ensures stale records expire within 1 hour. |
| Client discovery ([ADR 012](012-client.md)) | Clients use DHT FIND_VALUE for content discovery the same way nodes do. The on-chain origin-directory fallback applies equally. |

### 4. Schema Evolution

`cdn/dht/v1` follows the standard evolution model from [ADR 013](013-schema-evolution.md):

- **Minor** (new optional fields on existing message types): no ALPN bump.
- **Medium** (new mandatory fields): `cdn/dht/v2` ALPN.
- **Major** (incompatible routing changes): new ALPN + migration period.

`DhtMessage` uses a top-level enum consistent with the per-ALPN protocol enum pattern in [ADR 013 — Protocol Enums](013-schema-evolution.md#protocol-enums). Unknown variants are silently dropped.

### 5. Acceptance Criteria

1. A node in a 30-node PoC network can discover providers for a cached blob in ≤3 FIND_VALUE hops.
2. A node in a 500-node network can discover providers in ≤5 FIND_VALUE hops.
3. A cache event (blob added) generates ≤k (=20) outgoing STORE messages, not O(N).
4. A stale STORE record (node evicted the blob) expires within TTL (1 hour) with no explicit retraction.
5. A false STORE record (node claims to hold a blob it doesn't) fails at the probe step; the publishing node incurs a reputation penalty within one gossip cycle.
6. During bootstrap (routing table < k entries), the on-chain origin directory provides the fallback; routing table fully populated within 2 self-lookup rounds at PoC scale.
7. A node observing ≥5 FIND_VALUE queries for hash H within 5 minutes initiates a prefetch for H.
8. Demand signals derive from DHT FIND_VALUE traffic and local cache-miss timestamps; both are emitted as observability metrics in [Appendix: Observability](appendix-observability.md).

## Alternatives Considered

The six discovery alternatives evaluated against `cdn/dht/v1` (broadcast probe fan-out as primary, gossip content announcements, hash-prefix range hints, iroh mainline DHT, indexer nodes, full libp2p Kademlia) are recorded in [`_history/alternatives-pre-launch.md` § ADR 022 — Content Discovery at Scale](_history/alternatives-pre-launch.md#adr-022--content-discovery-at-scale).

## Cross-ADR Consistency

- **ADR 001** Future Work section ("Scaling Content Discovery") is superseded by this ADR. The three strategies listed there are resolved: selective fan-out is removed entirely (the on-chain origin directory is the deterministic last-resort fallback when the DHT returns no providers), content DHT is formalised here, gossip content hints are rejected.
- **ADR 005** probe protocol is unchanged. DHT provides candidates only.
- **ADR 008** reputation penalties for delivery failure cover false STORE records (a node publishing a DHT record claiming to hold a blob it does not have).
- **ADR 011** origin assignment authority is *not* consulted at probe time — origin status is not signaled on the wire. The DHT remains permissionless; per-namespace origin authorization is queried off-chain via `OriginAssignment.getOrigins(namespaceId)` for routing/discovery preferences.
- **ADR 012** client discovery uses DHT FIND_VALUE; the on-chain origin-directory fallback applies to clients equally.
- **ADR 013** schema evolution rules apply to `cdn/dht/v1`.
- **the observability appendix** SHOULD add DHT subsystem metrics: `decdn_dht_store_published_total`, `decdn_dht_findvalue_queries_total`, `decdn_dht_routing_table_size`.
