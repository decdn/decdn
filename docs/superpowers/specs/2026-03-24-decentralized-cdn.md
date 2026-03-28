# Decentralized CDN: Incentivized Delivery Network

**Date:** 2026-03-24
**Status:** Draft
**Scope:** Architecture for a decentralized CDN where storage is centralized (S3/R2/Backblaze) but delivery is handled by incentivized nodes that cache and share content with each other. Some nodes are configured with an origin backend (S3/R2) and can serve any blob; others are pure caches.
**Relationship to storage design:** This replaces the storage-decentralization model. The storage layer becomes a conventional object store. The decentralized network handles only delivery.

---

## 1. The Shift

The decentralized storage design solves a hard problem (durable, replicated, incentivized storage) at significant complexity cost. But the actual bottleneck for content delivery isn't storage — it's delivery latency and bandwidth cost.

S3-class object storage is:
- Cheap ($0.023/GB/month)
- Reliable (11 nines durability)
- Already solved

What S3 is not:
- Low latency globally (origin egress is slow from a single region)
- Free to egress (S3 egress costs $0.09/GB, CloudFront is $0.008/GB)
- Decentralized

A decentralized CDN inverts the model: **keep storage centralized and cheap; decentralize delivery**. Nodes cache and serve content close to clients. They earn per MB delivered. Storage costs stay predictable. The network gains geographic distribution without requiring nodes to commit to durable storage.

---

## 2. System Overview

```
       ┌────────────┐  ┌────────────┐  ┌────────────────────┐
       │    Node     │◄─┤    Node    │◄─┤  Node              │
       │  (cached)   ├─►│  (cached)  ├─►│  (origin-backed)   │
       └──────┬─────┘  └──────┬─────┘  │  S3/R2 configured  │
              │   peer share  │         └──────┬─────────────┘
           pays             pays            pays
        per-MB            per-MB          per-MB
              │               │               │
       ┌──────▼─────┐  ┌──────▼─────┐  ┌──────▼─────┐
       │   Client   │  │   Client   │  │   Client   │
       └────────────┘  └────────────┘  └────────────┘
```

**Nodes** form a peer mesh. Some are configured with an origin backend (S3/R2/B2) — they can serve any blob in their origin store and never experience a true cache miss. Others are pure caches that rely on peer pulls or redirects when they don't have the requested content. The protocol does not distinguish between them; staking, slashing, and peer pulls all work the same regardless of whether a node has an origin backend.

On a cache miss, a node first tries to pull from another node before touching any origin store. Nodes cache everything they pull — from origin or from peers — and serve it to clients. They are paid per MB delivered to clients via stablecoin payment channels. Node-to-node transfers carry no payment; the economic incentive is saving origin egress cost.

**Origin backends** are conventional object stores (S3, R2, Backblaze B2, or self-hosted MinIO). They are the source of truth for all blobs but are accessed as infrequently as possible — only when no peer node has the content. Clients can also fall back directly to an origin-backed node if no pure-cache node is available.

---

## 3. Node Roles

### 3.1 Node (Provider)

A node is the core participant in the delivery network. It caches and serves content. Some nodes are configured with an origin backend (S3/R2/B2) — they can serve any blob in their origin store (never cache miss). Others are pure caches. The protocol doesn't distinguish between them.

**Responsibilities:**
- Maintain a local cache of blobs (size is operator-configured)
- Announce cached content to all peers (clients and other nodes) via gossip
- Serve cached blobs to clients over QUIC/iroh (paid per MB)
- Share cached blobs with other nodes over QUIC/iroh (unpaid, see Section 7.4)
- On cache miss: check peer nodes first, fall back to origin pull (if origin-backed) or redirect to an origin-backed node
- Sign up with staking contract to be eligible for client payments and peer sharing

**Origin backend configuration (optional):** A node configured with an origin backend holds credentials for an object store and can serve any blob, always (no cache miss possible). It typically charges a higher delivery rate to reflect origin egress cost. The network operator runs at least one origin-backed node as the fallback provider of last resort. Its delivery rate is publicly known and sets an effective price ceiling — no node can charge more than origin and expect traffic.

**Economics:**
- **Revenue:** per-MB delivery payments from clients
- **Costs:** bandwidth (egress to clients and peers + ingress from origin or peers), storage hardware, staking opportunity cost
- **Cache strategy:** purely operator's choice — the protocol doesn't dictate eviction policy. Nodes cache what they expect will be requested again. Popular content = more delivery earnings = caching them is profitable. Peer pulls are cheaper than origin pulls, so nodes benefit from a well-seeded peer mesh.

**Staking requirement:** Nodes must stake a minimum amount of TOKEN (the native network token) to be listed in the DHT-backed node registry. Stake is slashable for provably bad behavior (serving corrupted data — detected by content hash mismatch, phantom blob announcements, or rate manipulation). Stake is **not** slashable for cache misses or going offline.

### 3.2 Client

A lightweight QUIC endpoint that streams content. Clients:
- Discover nodes via gossip or a bootstrap registry
- Open stablecoin payment channels with one or more nodes
- Send micro-payment vouchers per MB received
- Fall back to an origin-backed node if no suitable node responds
- Verify blob integrity via content hashes (BLAKE3) on every received chunk

---

## 4. Content Addressing

All content is stored as BLAKE3-hashed blobs. The protocol is content-agnostic — it stores and delivers arbitrary bytes with no assumptions about format, structure, or metadata.

**Blob ID:** BLAKE3 hash of the raw bytes. Computed once at upload time.

**Size:** Discoverable at delivery time via the `total_bytes` field in `StreamResponse`.

**Application-level metadata:** The protocol does not define a manifest or metadata format. Applications may layer their own metadata, manifest, or catalog structures on top — for example, linking multiple blobs, adding descriptive fields, or organizing content into collections. These are opaque to the delivery network, which sees only hashes and bytes.

**Integrity guarantee:** Because blobs are content-addressed, a client can verify every chunk it receives against the known hash. A node cannot serve corrupted data without being detected. This removes the need for complex proof-of-delivery schemes — the hash is the proof.

---

## 5. Node Discovery

Clients need to find nodes that have the content they want. Two complementary mechanisms:

### 5.1 Gossip-Based Announcement

Nodes broadcast cache state over a gossip topic per region or category (e.g., by geography or a flat global topic for small networks). A `CacheAnnounce` message:

```rust
struct CacheAnnounce {
    node_id: NodeId,             // iroh NodeId (ed25519 public key)
    eth_address: Address,        // for payment channel opening (client→node only)
    cached_hashes: Vec<Hash>,    // blobs currently in cache (max 500 per message)
    delivery_rate_per_mb: u64,   // USDC base units per MB charged to clients
    accepts_peer_pull: bool,     // whether this node serves other nodes
    has_origin_backend: bool,    // whether this node is origin-backed
    region_hint: Option<String>, // ISO 3166-1 alpha-2 country code, self-reported
    signed_at: u64,              // unix timestamp
    signature: Bytes,            // signs all fields above
}
```

`accepts_peer_pull: true` means this node will respond to blob requests from other staked nodes without requiring a payment channel. Nodes opt in; it defaults to true for any node that has accepted staking terms. Origin-backed nodes may set this to false to avoid absorbing egress costs for free.

Both **clients** and **nodes** maintain a local routing table: `hash → Vec<NodeId>` built from received announcements. Clients use it to find a serving node; nodes use it to find a peer to pull from on a cache miss. The same gossip layer serves both use cases.

**Message size bound:** `cached_hashes` is capped at 500 entries per message. Nodes with large caches split announcements across multiple messages or use a bloom filter summary instead of an exact list.

**Bloom filter alternative (for large caches):** A node with 100,000 cached blobs cannot enumerate them in gossip. It instead sends a Bloom filter (targeting ~1% false positive rate) in its announcement. Clients query the node directly for a hash if the filter says "maybe cached." This trades gossip bandwidth for a direct round-trip on false positives.

### 5.2 DHT-Based Lookup

For content not found in the local routing table, clients fall back to a DHT lookup: "which nodes have hash X?" This is the standard Kademlia approach used by BitTorrent and iroh's existing content routing.

Each node publishes `(hash → [nodeId, nodeId, ...])` records as content enters its cache and removes them on eviction.

**Consistency trade-off:** DHT records have TTL (default 1 hour). A node that evicts content and goes offline may still appear in DHT results for up to an hour. Clients handle this gracefully: if a node doesn't have the content (returns a 404-equivalent), the client tries the next candidate or falls back to an origin-backed node.

---

## 6. Payment Model

Delivery payments use the same stablecoin payment channel architecture as the stablecoin payments spec, but scoped to delivery only.

### 6.1 Channel Structure

A payment channel is opened between one client and one node. It holds USDC. The client signs cumulative vouchers as MB are delivered.

```
Client opens channel with 10 USDC deposit
  → Streams 500 MB, signs vouchers at $0.00001/MB
  → Final voucher: 0.005 USDC cumulative
  → Node closes channel, receives 0.00485 USDC (after 3% fee)
  → Client reclaims 9.995 USDC
```

**Channel economics for a typical small blob (3.75 MB):**

| Item | Cost |
|------|------|
| Delivery payment per blob | 0.0000375 USDC |
| Protocol fee (3%) | ~0.000001 USDC |
| Blobs of this size per $1 deposit | ~26,000 |

Clients rarely need to top up. A $1 deposit funds thousands of streams.

### 6.2 Rate Negotiation

Before streaming begins, the node advertises its rate in the `StreamResponse` message. The client either accepts (sends first voucher) or disconnects and tries another node. No surprise pricing.

**Rate bounds (governance-set):**

| Parameter | Floor | Ceiling |
|-----------|-------|---------|
| Delivery rate (USDC / MB) | 0.000001 ($0.000001) | 0.10 ($0.10) |

The ceiling is intentionally high. Origin-backed nodes set the practical ceiling — any node charging more than origin loses all traffic.

### 6.3 Voucher Cadence

Vouchers are sent every **1 MB delivered** (the cadence depends on transfer speed). This is coarser than the storage design's chunk-level vouchers because CDN delivery is sequential and the hash-based integrity check provides sufficient protection against non-delivery without per-chunk payments.

**Rationale:** More frequent vouchers = more cryptographic overhead. 1 MB cadence means a node risks losing at most one voucher's worth of revenue if a client disconnects without signing. At $0.00001/MB, the maximum risk per disconnect is $0.00001 — negligible.

### 6.4 Delivery Verification

The content hash is the delivery receipt. A client that receives a complete blob and verifies its BLAKE3 hash:
- Knows it got exactly the bytes it asked for
- Can prove it to anyone (hash is deterministic)
- Has no grounds for a refund dispute

A node that sends corrupted bytes:
- Will have its payment rejected (client doesn't sign the voucher)
- Gets detected immediately (hash mismatch on first bad chunk)
- Gets flagged in the client's local reputation table

**No proof-of-delivery oracle needed.** The BLAKE3 hash is sufficient.

---

## 7. Cache Behavior

The protocol does not dictate cache policy. Nodes are economically motivated to make good caching decisions. This section describes the rational strategy; operators may implement variations.

### 7.1 Cache Miss Handling

When a client requests a blob the node doesn't have, the node resolves it in this priority order:

**Step 1 — Peer pull (preferred):**

1. Node checks its gossip routing table for a peer node with `accepts_peer_pull: true` and the requested hash
2. If found: pulls the blob from the peer over QUIC (no payment — see Section 7.4), caches it locally
3. Streams to the client at the node's normal delivery rate while the peer pull is in progress

**Step 2 — Paid pull from an origin-backed node (fallback):**

If no peer has the blob via free peer pull (or all peers time out):

1. Node connects to an origin-backed node via `cdn/client/v1` (the same protocol clients use)
2. Pays the origin-backed node's delivery rate, caches locally, streams to client
3. The requesting node absorbs the origin-backed node's delivery cost from its margin

**Step 3 — Redirect (last resort):**

If the node chooses not to do a pull-through (config option `pull_through: false`):

1. Returns a redirect to an origin-backed node's address
2. Client opens a channel with the origin-backed node directly

The `StreamResponse` message includes an optional `redirect` field for Step 3. Pull-through (Steps 1-2) is the default and preferred path — it earns the node delivery revenue and warms the cache for future requests.

**Peer-first rationale:** Peer pulls are typically faster (nearby datacenter, no S3 round-trip overhead), free of egress charges, and keep origin load minimal. After the first client in a region triggers an origin pull, all subsequent nodes in that region get the blob from peers.

### 7.2 Prefetching and Warming

Nodes can proactively cache popular content before it's requested:

- **Popularity signals from gossip:** If multiple peers announce a blob, it's popular. Worth caching.
- **Related content prefetch:** Applications can hint at related blobs (e.g., via a prefetch list in application-level metadata). Nodes can speculatively cache related blobs when one is requested.

These are local heuristics. No coordination protocol is needed.

### 7.3 Eviction

LRU or frequency-weighted eviction (LFU) are both reasonable. Operators should tune cache size to maximize hit rate within their storage budget. A node with a 100% hit rate on a popular catalog earns maximum revenue per unit of bandwidth.

**Minimum viable cache:** A set of popular blobs (e.g., 400 MB of frequently requested content) cached by 10 nodes globally is a functional CDN for that content.

### 7.4 Node-to-Node Transfer Protocol

Nodes pull blobs from peers using the same QUIC/iroh transport used for client delivery, with a different ALPN identifier to distinguish the connection type.

**ALPN:** `cdn/peer/v1` (client delivery uses `cdn/client/v1`)

**Authentication:** The requesting node identifies itself by its iroh `NodeId`. The serving node verifies the requester is a staked node by checking the on-chain registry (cached locally, refreshed every 10 minutes). Unstaked nodes are rejected — this prevents free-riders who never serve clients from draining peer bandwidth.

**No payment channel:** Peer pulls carry no vouchers and no payment. The serving node donates bandwidth; the economic return is indirect — a better-seeded network means more clients find content locally, raising delivery volume for everyone.

**Peer pull availability:** Peer pulls are available between any staked nodes that opt in (via `accepts_peer_pull` in their `CacheAnnounce` message). Origin-backed nodes may set this to false to avoid absorbing egress costs for free — in that case, other nodes can still fetch from them via the paid `cdn/client/v1` path.

**Transfer flow:**

```
Requesting node                     Serving node
      │                                   │
      │── PeerPullRequest(hash, nodeId) ──►│
      │                                   │ verify nodeId is staked
      │◄── PeerPullResponse(ok | reject) ──│
      │                                   │
      │◄══════ blob chunks (BLAKE3) ══════│
      │                                   │
      │ verify hash, cache locally        │
```

**`PeerPullRequest` message:**

```rust
struct PeerPullRequest {
    hash: Hash,
    requester_node_id: NodeId,
    requester_eth_address: Address,  // for registry lookup
    timestamp: u64,
    signature: Bytes,                // signs hash + timestamp, proves key ownership
}
```

**`PeerPullResponse` variants:**

```rust
enum PeerPullResponse {
    Ok { size_bytes: u64 },
    NotCached,                    // don't have it; try someone else
    Reject { reason: RejectReason }, // not staked, rate-limited, etc.
}
```

**Rate limiting:** Serving nodes apply a per-peer bandwidth cap (default: 100 MB/hour per requesting node, operator-configurable). This prevents a single node from monopolizing a peer's outbound bandwidth. Requests exceeding the cap receive `Reject { reason: RateLimited }`.

**Partial failure:** If a peer pull stalls mid-transfer (peer goes offline, connection drops), the requesting node falls back to the next candidate in its routing table or to an origin-backed node. Partial data received so far is discarded — BLAKE3 verification only passes on a complete blob.

**Why free rather than a wholesale rate?**

A wholesale micro-payment for peer pulls would require payment channels between every pair of nodes — O(n^2) channels. The economic benefit to individual nodes is small (peer pull costs are a fraction of total operating costs), while the protocol complexity is large. Free peer sharing keeps the design simple and creates a network externality: every node that shares freely benefits from others doing the same.

---

## 8. Origin Integration

This section describes the origin backend configuration for nodes, not a separate system. The `OriginStore` trait is part of the node binary when configured with an origin backend.

### 8.1 Supported Origins

The node software ships with adapters for common object stores:

| Origin | Auth Method | Notes |
|--------|-------------|-------|
| AWS S3 | IAM credentials or pre-signed URLs | Most common |
| Cloudflare R2 | S3-compatible API | No egress fees between R2 and Cloudflare Workers |
| Backblaze B2 | S3-compatible API | Cheapest egress ($0.01/GB) |
| MinIO (self-hosted) | S3-compatible API | For operators who want full control |

**Abstraction:** All origin pull goes through a single `OriginStore` trait:

```rust
trait OriginStore: Send + Sync {
    async fn fetch(&self, hash: &Hash) -> Result<Bytes>;
    async fn head(&self, hash: &Hash) -> Result<ObjectMeta>;
}
```

### 8.2 Origin Pull Authentication

Origin objects can be public (presigned URL not needed) or private (presigned URLs with short TTLs). For private origins, the origin-backed node holds credentials and generates presigned URLs for pull-through. Nodes that want to pull directly from origin can request a presigned URL from the origin-backed node — it validates the requesting node's stake before issuing the URL.

**Simpler alternative:** Require origins to be public S3 buckets. Any node can pull directly without auth delegation. This is acceptable for content that isn't access-controlled and is appropriate for the PoC.

### 8.3 Hash-to-Object-Key Mapping

S3 objects are addressed by key (a path string). Blobs are addressed by BLAKE3 hash. The mapping is stored in a content catalog:

```
catalog: hash → {s3_bucket, s3_key, size_bytes, content_type}
```

The catalog is a small database (PostgreSQL or SQLite) maintained by the operator. Nodes query it on cache miss to find the origin pull URL. The catalog is not on-chain — it's an operational concern.

**Catalog API:**
```
GET /catalog/{hash} → {origin_url, size_bytes, content_type}
```

---

## 9. Gossip and Routing Protocol

### 9.1 Topic Structure

Unlike the storage design's per-content gossip topic, CDN cache announcements use **region-based topics** to limit gossip fan-out:

| Topic | Participants | Purpose |
|-------|-------------|---------|
| `cdn/global/v1` | All nodes | New content announcements, node joins/leaves |
| `cdn/region/{region}/v1` | Nodes in that region | Regional cache state |

Region is self-reported (ISO 3166-1 alpha-2 country code). Clients subscribe to their local region topic and the global topic.

**Why region topics?** A client in Japan doesn't benefit from knowing that a node in Brazil has a blob cached. Region-scoped gossip reduces irrelevant routing table entries and keeps message volume proportional to useful information.

### 9.2 Node Registry

A lightweight on-chain registry (or a governance-curated off-chain list) maps staked node addresses to their iroh NodeIds and QUIC addresses. This is the bootstrap mechanism:

```solidity
struct NodeInfo {
    address ethAddress;
    bytes32 irhNodeId;    // 32-byte ed25519 public key
    string  multiaddrs;   // comma-separated QUIC multiaddrs
    uint256 stakedAmount;
    uint256 registeredAt;
}
```

All staked nodes are registered in the registry with the same role. Whether a node has an origin backend is a deployment detail, not a protocol-level distinction.

Clients query this registry on first startup to find their initial peers, then rely on gossip for ongoing discovery.

---

## 10. Node Economics

### 10.1 Revenue Model

A node earns only when it delivers bytes. No delivery, no revenue.

**Unit economics example** (operator at $35/month VPS with 10 TB/month bandwidth):

| Metric | Without peer sharing | With peer sharing |
| ------ | ------------------- | ----------------- |
| Bandwidth allowance | 10,000 GB/month | 10,000 GB/month |
| Cache miss rate | 30% (3,000 GB miss) | 10% (1,000 GB miss) |
| Origin pull cost (B2, $0.01/GB) | $30/month | $10/month |
| Peer pull bandwidth cost | — | ~$2/month (inbound) |
| Infrastructure cost | $65/month | $47/month |
| Revenue at $0.00001/MB | $100/month | $100/month |
| Gross profit | ~$35/month | ~$53/month |

Peer sharing reduces origin pull costs significantly once the network reaches a critical mass of nodes. A new node in the same region as an established node can warm most of its cache from peers rather than origin.

Revenue depends entirely on traffic to clients. A node serving no clients earns $0. This is the correct incentive: nodes compete on cache quality, latency, and price to attract clients.

### 10.2 Profitability Drivers

| Factor | Effect | Provider control |
| ------ | ------ | ---------------- |
| Cache hit rate | Higher hit rate = lower origin pull costs | Cache popular content |
| Peer mesh density | More peers with overlapping catalogs = fewer origin pulls | Operate in a well-seeded region |
| Geographic placement | Closer to clients = lower latency = preferred in selection | Choose datacenter region |
| Delivery rate | Lower rate attracts more clients, lower margin | Set rate strategically |
| Bandwidth cost | Lower cost provider has more margin headroom | Choose cheap bandwidth provider |
| Cache size | Larger cache = higher hit rate and more shareable content | Provision more disk |

### 10.3 Cold Start Problem

A new node has an empty cache. It earns nothing until it caches content. Two strategies:

**Passive warm-up:** Serve origin pull-through requests (at a slight loss or break-even) to populate the cache. Once cache is warm, flip to serving from cache and earning margin.

**Active warm-up:** Prefetch the top N most popular blobs before accepting client connections. The node can pull these from peers (free) or from origin. A public popularity API (or on-chain analytics from delivery payment volume) provides the top-N list.

**Peer-assisted warm-up:** A new node announces itself on gossip without any cached content. Existing nodes in the region detect the new peer and can proactively push their most popular blobs to it (unsolicited `PeerPush` — see Section 7.4). This is optional and altruistic, but established nodes benefit from having a local peer that reduces origin load for both.

The protocol supports all strategies; no special handling is required.

---

## 11. Staking and Slashing

Staking serves two purposes in the CDN context:

1. **Sybil resistance:** Stake has a cost, preventing trivial fake node creation
2. **Bad-data penalty:** A node serving data that doesn't match the advertised hash can be slashed

All staked nodes share a single staking role. Whether a node has an origin backend is a deployment choice, not a protocol distinction.

### 11.1 Stake Parameters

| Parameter | Value | Notes |
|-----------|-------|-------|
| Minimum stake | 100 USDC equivalent | Low enough to be accessible; high enough to have skin in the game |
| Slash amount | Full stake | Only for provably bad behavior (hash mismatch, phantom blob announcements, rate manipulation) |
| Unbonding period | 7 days | Standard delay after unstake request |

**Slashable offenses:**

- **Hash mismatch:** Node serves data that doesn't match the advertised BLAKE3 hash. Either malicious (sending garbage) or catastrophic failure. Either warrants slashing.
- **Phantom blob announcements:** Node claims `has_blob: true` in gossip or DHT but cannot deliver the blob when requested. This pollutes the routing table and wastes client time.
- **Rate manipulation:** Node advertises one delivery rate in its probe/announcement but charges a different (higher) rate in the actual `StreamResponse`. This is bait-and-switch behavior.

**Not slashable:** Going offline, having a cache miss, or being slow are not malicious acts — they're just poor service. Clients handle them by switching to another node. Slashing for reliability would punish operators for infrastructure failures and add governance complexity.

### 11.2 Slash Evidence

A client that receives data failing the BLAKE3 hash check can submit a slash claim:

```solidity
function submitSlashClaim(
    bytes32 nodeId,
    bytes32 expectedHash,
    bytes   receivedData,   // the bad bytes
    bytes   signature       // client's sig on the delivery session
) external;
```

The contract verifies: `keccak256(receivedData) != expectedHash` (using a bridge from BLAKE3 to EVM — see below). If the check fails (meaning the data really is wrong), the node is slashed and the client receives a portion as the bounty.

**BLAKE3 on EVM:** BLAKE3 is not a native EVM precompile. For the PoC, use a simplified verification: the delivery session includes a Merkle root of chunks. The client submits the specific failing chunk and its Merkle proof. The contract verifies the proof against the session root using standard keccak256. Full BLAKE3 verification can be done off-chain by a challenge verifier role in a later version.

---

## 12. Client Behavior

### 12.1 Node Selection

When a client wants to fetch a blob, it follows this selection algorithm:

```
1. Check local routing table for nodes advertising the blob's content hash
2. Filter: only staked nodes (from registry), within acceptable latency
3. Sort by: delivery_rate ASC (cheapest first), then latency ASC
4. Try top candidate: open payment channel (or reuse existing), request stream
5. If no candidates in routing table: query DHT for hash → nodes
6. If DHT yields nothing: fall back to an origin-backed node
```

**Channel reuse:** Clients maintain a pool of open payment channels with frequently used nodes. Opening a new channel requires an on-chain transaction (~$0.05 on Arbitrum). Reusing an existing channel is free. Clients should open channels with 2-3 preferred nodes and top them up as needed.

### 12.2 Parallel Streaming (Future)

For a single blob, a client could open channels with multiple nodes and request different byte ranges in parallel (like BitTorrent's rarest-first piece selection). This is out of scope for the initial version but the content-addressed blob model supports it natively — any node serving the correct BLAKE3-verified bytes is interchangeable.

### 12.3 Fallback Chain

```
preferred node
    → retry with another node from routing table
        → query DHT for additional candidates
            → origin-backed node (higher rate, always available)
                → direct origin URL (no payment, but client needs access)
```

Clients implement this as a priority queue with exponential backoff on failures.

---

## 13. Differences from Decentralized Storage Design

This CDN model deliberately simplifies the problem:

| Concern | Storage Design | CDN Design |
| ------- | -------------- | ---------- |
| Where data lives permanently | Decentralized providers (iroh-blobs) | Centralized origin (S3/R2) |
| What providers are paid for | Storing + delivering | Delivering to clients only |
| Inter-node data transfer | Not applicable (each node owns its data) | Free peer pulls between staked nodes |
| Storage durability guarantee | Replication factor N, pinning deals | S3's 11-nines (operator responsibility) |
| Provider data commitment | Must store for agreed duration | Can evict anytime |
| Staking slash conditions | Missing data, not replicating | Serving bad data (hash mismatch) |
| Node operator burden | Must guarantee data persistence | Cache, share, and serve |
| Content upload flow | Client→providers via iroh-blobs | Client→S3, node pulls on demand |
| Economics complexity | Storage payments + delivery payments | Delivery payments only |
| Node roles | Distinct provider types | Single node role (origin backend is config) |

**The CDN model is strictly simpler.** It trades decentralized storage guarantees (which require complex pinning deals, replication verification, and challenge games) for the simplicity of trusting S3 for durability while decentralizing only the delivery layer — which is where latency and cost actually matter at scale.

---

## 14. Open Questions

1. **Catalog service decentralization.** The content catalog (hash → S3 key) is currently centralized. Should it be an on-chain registry, a DHT, or just an operator-run API? On-chain is trustless but expensive to update. DHT is decentralized but complex. A simple HTTPS API is fine for PoC. **Recommendation:** Start with HTTPS API; evaluate DHT if the operator becomes a bottleneck.

2. **Per-region origin mirrors.** S3 in us-east-1 serving clients in Asia incurs high latency on origin pulls. Should operators be required to replicate origins to multiple regions? **Recommendation:** Optional. Operators who serve global audiences will self-motivate to deploy regional origins (e.g., R2's global replication). The CDN layer masks most of this latency after the first cache warm.

3. **Proof of bandwidth.** Unlike storage proofs, there's no cryptographic proof that a node actually served X bytes to Y clients. Payment channels are the economic signal (nodes only get paid if clients send vouchers), but there's no trustless audit of delivery volume for analytics purposes. **Recommendation:** Acceptable for now. Payment channel data on-chain provides a lower bound on delivery volume. Full delivery analytics require a more complex attestation scheme (out of scope).

4. **Content moderation.** A node caching content from S3 doesn't inherently know what it's caching. Should nodes be able to blocklist content by hash? **Recommendation:** Yes — provide a simple `blocklist: Vec<Hash>` in the node config. Nodes will not cache or serve blocklisted hashes. This is an operator-level control, not a protocol-level mechanism.

5. **Competing on latency vs. price.** The client selection algorithm currently prioritizes price then latency. In practice, a $0.000001/MB cheaper node that's 200ms farther away is probably worse for UX. Should selection weight latency more heavily? **Recommendation:** Make the selection function configurable with sensible defaults. Latency x price scoring (e.g., `score = rate * latency_ms`) is more realistic than lexicographic ordering.

6. **PeerPush unsolicited transfers.** Section 7.4 mentions that established nodes can proactively push blobs to new peers. This is useful for warm-up but could be abused (flooding a new node with unwanted data). **Recommendation:** New nodes must opt in to unsolicited pushes via a flag in their `CacheAnnounce` message (`accepts_peer_push: bool`). Default false. Nodes enable it during their warm-up phase and disable it once the cache is populated.
