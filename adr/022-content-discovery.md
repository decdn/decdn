# ADR 022 — Content Discovery at Scale

**Status:** Proposed
**Deciders:** Core team
**Date:** 2026-04-08

## Context

Content discovery answers: "which nodes currently hold blob H?" The answer drives both client→node delivery (client picks a node to stream from) and node→node pull-through (a node with a cache miss finds a provider to pull from).

A **Kademlia-based content DHT** has the right properties for an incentive-driven CDN:

- Nodes publish `(hash → NodeId)` records **only when they hold a blob** — a voluntary, self-interested advertisement to attract paying clients. No economic incentive exists to publish records for blobs you don't hold (false records attract probes that reveal the lie, degrading reputation and earnings).
- **O(log N) lookup** — a querying node contacts ~5 peers to find providers at 1,000 nodes.
- **O(log N) publish cost** — a STORE record is pushed to only the K nodes closest to the hash in keyspace. No global broadcast.
- **The probe step is preserved** — DHT lookup narrows the candidate set; `cdn/probe/v1` still confirms live availability and measures latency before any delivery commitment.

The DHT defined here is a separate content DHT scoped to the registered node set, operating over the `cdn/dht/v1` ALPN on iroh QUIC. It is distinct from iroh's built-in `DhtDiscovery` (mainline BitTorrent DHT via pkarr), which resolves `NodeId → address` on the public internet.

## Decision

`cdn/dht/v1` is the **primary content discovery mechanism**. The DHT bootstraps from `CapacityBond.getActiveNodes()` — a freshly-started node's first peers come from the on-chain registry and immediately participate in DHT lookups, so there is no separate bootstrap window during which DHT cannot resolve. When a DHT lookup returns no providers, the on-chain origin directory is the deterministic last-resort fallback.

### `cdn/dht/v1` Protocol

#### ALPN and Transport

All DHT messages use the ALPN `cdn/dht/v1` over iroh QUIC. Connections are short-lived and request/response oriented — no persistent streams. The same iroh endpoint serving `cdn/probe/v1` and `cdn/client/v1` handles DHT connections.

#### Message Types

```rust
/// Top-level DHT protocol enum (one variant per request/response pair)
enum DhtMessage {
    FindValue(FindValueRequest),
    FindValueResponse(FindValueResponse),
    Store(StoreRequest),
    StoreAck(StoreAck),
    BatchStore(BatchStoreRequest),
    BatchStoreAck(BatchStoreAck),
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
}

struct StoreAck {
    hash: Hash,
    accepted: bool,
}

/// Batched publication: many hashes from one holder to one receiver in a single RPC.
/// Optimization for bootstrap and dense re-publish; see § DHT Bandwidth Analysis.
struct BatchStoreRequest {
    hashes: Vec<Hash>,       // ≤256 entries; one holder, many hashes
    holder: NodeId,          // single holder; checked once vs authenticated QUIC NodeId
}

struct BatchStoreAck {
    results: Vec<bool>,          // per-hash outcome (accepted/rejected) in request order
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

#### Routing Table

Each node maintains a Kademlia routing table: **k-buckets** partitioned by XOR distance from the node's own `NodeId` in 256-bit keyspace. NodeIds are already 32-byte ed25519 public keys — no separate DHT key needed.

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| k (bucket size) | 20 | Standard Kademlia; tolerates churn |
| α (concurrency) | 3 | Parallel lookup RPCs |
| Bucket refresh interval | 1 hour | Keeps routing table fresh |
| Routing table storage | In-memory | Rebuilt via bootstrap on restart |

#### Content Records and TTL

Content records are stored in-memory at the K nodes closest to the hash in keyspace.

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Record TTL | 1 hour | Bounds stale record lifetime after eviction |
| Re-publish interval | jittered, mean 40 min, range [30 min, 50 min] | Re-published while blob is held, before TTL expiry; jitter spreads the targeted-DoS cost on the receiver set across a 20-minute window per record |
| Max providers per hash | 50 | Well above useful redundancy; bounds record size |
| Max records per node | 100,000 | ~50 MB memory at max record size |
| Max records per publisher (per receiver) | 200 | Bounds a single publisher's storage footprint at any receiver; sized so the per-node cap admits ≥500 distinct active publishers concurrently |

**Per-publisher quota and eviction.** Two limits apply: a per-publisher cap of 200 records (hard) and a per-node global cap of 100,000 records. The two limits compose as follows:

- When publisher P is at its per-publisher cap, the receiver MUST reject new `StoreRequest`s from P with `StoreAck { accepted: false }`. P's existing records are not evicted by P's own further STOREs, and no other publisher's records are evicted on P's behalf.
- When the global cap is reached but the inserting publisher P is below P's per-publisher cap, the receiver MUST evict the globally-oldest record (by receive time) to make room for the new STORE.

The per-publisher cap is what prevents the exhaustion attack where a single publisher fills all 100,000 slots and forces eviction of legitimate records from other publishers. With that cap in place, global LRU for below-quota inserts is safe and ensures that newly-active staked publishers can always participate in the DHT — without global LRU, a node reaching capacity with 500 publishers each at 200 records would lock out the 501st publisher entirely. The per-publisher cap also bounds the surface area of a false-STORE publisher (a node advertising hashes it does not hold) at any one receiver, so the requester's negative probe cache ([ADR 001 § Probe cache](001-network.md#probe-cache)) sees a bounded set of `(NodeId, hash)` pairs to absorb.

A node **stops re-publishing** when it evicts the blob. Stale records self-expire within TTL — no explicit retraction messages needed. **TTL is anchored on the receiver's wall-clock at acceptance time:** the receiver computes `expiry_us = receive_us + record_ttl_us`, where `receive_us` is the receiver's wall-clock microsecond timestamp at acceptance and `record_ttl_us` is the Record TTL parameter above in microseconds (1 hour = 3,600,000,000 μs). `StoreRequest` carries no sender-asserted timestamp, so record lifetime is independent of any clock the holder controls. Each re-publish (scheduled within the jittered re-publish window) refreshes the receiver's record by replacing stored `expiry_us` with one derived from the new `receive_us`, extending effective lifetime ahead of the previous expiry while the holder still has the blob. Rationale matches the receiver-anchored TTL pattern in [Appendix: Peer Table Eviction](appendix-peer-table-eviction.md#appendix-peer-table-eviction-policy).

#### STORE Flow (Cache Event → DHT Publish)

When a node caches blob H:

1. Identify the K+3 closest nodes to H from the local routing table. The three positions beyond K are overflow targets — publishing to a wider set than the minimum required by the routing geometry means an attacker forcing record expiry by suppressing receivers must take down K+3 hosts rather than K.
2. Send a `StoreRequest { hash: H, holder: self.node_id }` to each.
3. Schedule the next re-publish at `T + uniform(30 min, 50 min)`, where `T` is the local wall-clock at which step 2 was last performed for this blob. The jitter is drawn independently per record so the next re-publish window for a given hash is not predictable from outside the publisher.

**Re-publish.** Re-publish reuses the same step 1–2 sequence with a fresh jitter draw per step 3; missed slots (e.g., local downtime) re-fire at the next scheduler tick rather than back-filling.

**Batched STORE.** When a publisher has multiple hashes to send to the same receiver — typically at cold start ([§ Bootstrap](#bootstrap)) or when re-publish windows across cached blobs concentrate on overlapping receiver sets — it MAY send a single `BatchStoreRequest { hashes, holder }` covering all hashes destined for that receiver instead of separate `StoreRequest` RPCs. The receiver checks the `holder == authenticated QUIC NodeId` equality once for the batch (the field is a single `NodeId`, not per-hash) and applies the remaining admission rules per-hash (per-publisher quota with two-tier eviction, active-staker check, rate-limit accounting per [§ DHT Rate Limiting](#dht-rate-limiting)). The reply is `BatchStoreAck { results }` with per-hash outcomes in request order. Receiver-anchored TTL is computed per-hash from a single `receive_us` for the batch.

Batch size is bounded at 256 hashes (≈8 KB request payload at 32B/hash). Publishers split larger publish sets into multiple batches. A `BatchStoreRequest` with `hashes.len() > 256` is rejected by closing the stream with `MALFORMED_MESSAGE` (`0x03`) per [ADR 013 § Application Error Codes](013-schema-evolution.md#application-error-codes); the publisher is expected to fix the request shape and retry, not back off.

Backward compatibility: receivers that do not implement the `BatchStore` / `BatchStoreAck` variants silently drop the message ([§ Schema Evolution](#schema-evolution)). Publishers detect non-support by stream-close-without-ack and MUST fall back to per-hash `StoreRequest` for that receiver. Once fallback is observed for a receiver, the publisher SHOULD cache the result and skip batched attempts for some bounded duration to avoid repeated drop-and-fallback cycles. The motivation for batching and the modeled bandwidth savings are in [§ DHT Bandwidth Analysis](#dht-bandwidth-analysis).

The receiving node MUST reject any record whose `holder` does not equal the authenticated NodeId of the inbound QUIC connection. NodeIds are 32-byte ed25519 public keys ([§ Routing Table](#routing-table)) and iroh's QUIC handshake authenticates the connection against that key, so the equality check binds the record to its claimed origin without a per-record signature. This depends on the publisher-only re-publish model ([§ Content Records and TTL](#content-records-and-ttl) and [§ STORE Flow (Cache Event → DHT Publish)](#store-flow-cache-event--dht-publish) step 2): records are pushed directly by the holder to the K-closest nodes and never propagated peer-to-peer. If a future scheme introduces peer relay of records (e.g., Kademlia replication-on-churn), receivers no longer have a direct authenticated connection to `holder` and a per-record signature MUST be reintroduced. In that case an in-scope sender timestamp MUST also be reintroduced inside the signed body — otherwise the captured signed bytes are constant and trivially replayable, defeating the signature ([ADR 015 § Replay Safety Analysis](015-zero-rtt.md#replay-safety-analysis) makes the same point at the transport layer). The receiver MUST additionally verify that `holder` is in the cached active-staker set (populated from `CapacityBond.getActiveNodes()` per [ADR 019 § Step 3.3](019-node-onboarding.md#step-33--build-initial-peer-table-from-on-chain-registry)) before accepting the record; non-staked publishers are rejected with `StoreAck { accepted: false }`. Receiving nodes do **not** verify that the holder actually has the blob — that is the probe step's job. A false STORE publisher (a node claiming to hold a blob it does not) fails at probe time, degrading its reputation. **Note:** "publisher" in this ADR refers to a node publishing a DHT STORE record (an act of advertising). It is distinct from the on-chain *content publisher* identity defined in [ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces), which is an Ethereum address registered in `PublisherRegistry`. Where confusion is possible this ADR uses "STORE publisher" or "holder" for the DHT-record sender.

#### DHT Rate Limiting

`cdn/dht/v1` inbound requests are subject to three layered token-bucket rate limits applied **before** any routing-table lookup, per-publisher quota check, or response serialization. A request must pass all three layers to be admitted; failing any layer closes the stream with QUIC application error code `0x10` (`RATE_LIMITED`) per [ADR 013 § Application Error Codes](013-schema-evolution.md#application-error-codes).

| Layer | Sustained rate | Burst | Source |
|-------|----------------|-------|--------|
| Per-peer (NodeId) | 20 requests/sec | 40 | The source iroh `NodeId` on the QUIC connection |
| Per-IP | 100 requests/sec | 200 | Source IP address on the QUIC connection |
| Global | 1000 requests/sec | 2000 | All inbound DHT traffic across all peers |

Checks fire cheapest-first (global → per-IP → per-peer) so a request rejected by the global cap never costs a per-IP-bucket lookup. Each admitted `FindValueRequest`, `FindNodeRequest`, or `StoreRequest` consumes exactly one token from each bucket — admission decisions for these three variants do not inspect message-type-specific contents. `BatchStoreRequest` uses a two-stage variant of the same rule (see § Batch token accounting below) that keeps cheapest-first ordering intact while charging per-hash cost.

**Batch token accounting.** A `BatchStoreRequest` is admission-tested in two stages so the rate limit fires before any deserialization of the request body:

1. **Frame admission.** Consume 1 token from each bucket to admit the inbound frame, identical to per-hash STORE. If any layer is exhausted, reject with `RATE_LIMITED` without deserializing the body — an attacker mounting oversized-batch floods is rate-limited at this stage before paying deserialization cost, preserving the cheapest-first ordering principle.
2. **Per-hash admission.** Deserialize the request header to obtain `n = hashes.len()`. Verify the batch-level `holder == authenticated QUIC NodeId`; reject the entire batch with `MALFORMED_MESSAGE` if not. Let `b = min(remaining_tokens_per_peer, remaining_tokens_per_ip, remaining_tokens_global)`. Consume `min(b, n - 1)` more tokens from each bucket. The first `k = 1 + min(b, n - 1)` hashes pass through per-hash processing (per-publisher quota with two-tier eviction, active-staker filter); the remaining `n - k` hashes are marked `accepted: false` in the ack without further processing.

Total tokens consumed: `k`, matching what the per-hash equivalent would have charged. The work done per second is bounded the same whether the publisher sends `n` separate `StoreRequest`s or one `BatchStoreRequest` of size `n`. The wire-framing, `holder`-de-duplication, and ack-shape savings are real; the throughput ceiling is unchanged. Partial admission avoids forcing a publisher to retransmit a full batch when only the tail is over budget — the ack identifies rejected hashes by their request-order position, and the publisher retries only those.

**Why three layers, not one.** Per-peer alone is bypassable: any QUIC client can initiate a `cdn/dht/v1` connection and rotate `NodeId` at zero cost (the STORE staker-set check applies only after admission, and FIND_VALUE / FIND_NODE require no staker membership at all). The per-IP layer raises the cost of single-source flooding — IP rotation requires money (proxies, IPv6 prefix delegation, cloud bills). The global cap is defence in depth against distributed attacks across many IPs that would otherwise exhaust the node's routing-table-lookup and response-serialization capacity. The asymmetry between cheap request (`FindValueRequest` is ~40 bytes on the wire) and expensive response (full k-bucket walk plus serialization of K closer-node entries) is what makes DHT flooding economical to mount without these limits.

**Interaction with the per-publisher quota.** STORE admission additionally consumes one slot from the publisher's record quota ([§ Content Records and TTL](#content-records-and-ttl)); the rate limit fires first, so a rate-limited STORE never consumes a quota slot. FIND_VALUE responses that route the requester onward (via `closer_nodes`) do not multiply rate-limit consumption on the responder — one inbound request, one bucket token, regardless of response size.

**Trusted-IP exemption.** Operators MAY configure a list of trusted source IPs that bypass the per-IP layer only — typical use is peer operators with predictable cross-peer DHT traffic, or in-cluster monitoring. The trusted-IP list does NOT bypass the per-peer or global layers. Configuration key: `dht.rate_limit.trusted_ips`. The mechanism mirrors the probe-side equivalent in [ADR 005 § Probe rate limiting](005-protocol.md#probe-rate-limiting).

**Observability.** `decdn_dht_rate_limit_rejections_total{layer={per_peer, per_ip, global}}` counter, same shape as the probe-side metric.

#### FIND_VALUE Flow (Cache Miss → DHT Lookup)

When a node gets a cache miss for hash H and the probe cache is empty:

1. Check local routing table for the α (=3) closest nodes to H.
2. Send parallel `FindValueRequest { hash: H }` to all α nodes.
3. Iterate: each responder returns known providers or closer nodes (standard iterative Kademlia). Apply the lookup-integrity filters below to each response before incorporating it into the candidate set.
4. Continue until providers are found or lookup converges (no closer nodes returned).
5. Randomize the surviving provider set, then probe it via `cdn/probe/v1` to confirm live availability and measure latency.
6. Select provider by unified node selection score ([ADR 001](001-network.md#node-selection-algorithm)); deliver via `cdn/client/v1`.

**Lookup integrity.** Three requester-side invariants govern lookup result acceptance, applied in order to each `FindValueResponse`:

1. **XOR distance check on `closer_nodes`.** The requester MUST drop any `closer_nodes` entry whose XOR distance to the target hash is not strictly less than the responder's own. A responder that returns "closer" nodes that are not in fact closer is attempting to redirect the lookup; honest Kademlia responders never produce such entries. Offending entries are dropped without affecting honest entries from the same response.
2. **Active-staker filter on `closer_nodes` and `providers`.** The requester MUST filter both fields against its cached active-staker set ([ADR 019](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow) registry cache). Non-staked NodeIds are silently dropped — the DHT routing pool is restricted to staked nodes, and STORE receivers reject non-staked publishers at admission ([§ STORE Flow (Cache Event → DHT Publish)](#store-flow-cache-event--dht-publish)), so a non-staked entry in either field is either responder misbehavior or stale state on the responder's side. Filtering at the requester ensures lookup iteration only routes through nodes that can legitimately participate and probe traffic only reaches nodes that can legitimately hold records.
3. **Negative probe cache consultation on `providers`.** The requester MUST consult the negative probe cache ([ADR 001 § Probe cache](001-network.md#probe-cache)) and drop any `(NodeId, H)` pair present. A NodeId that previously returned `has_blob: false` for H within the cache TTL is not re-probed for H during that window; this bounds the cost of false-STORE publishers at the K-closest receivers to one failed probe per requester per cache window.

After these three filters, the requester MUST randomize the order of the surviving provider set before issuing probe RPCs. Responder-side ordering is not authoritative: the wire protocol cannot enforce honest ordering on the response side, and a modified responder can deterministically promote attacker-controlled NodeIds to bias selection. Randomization on the requester is the load-bearing defense against ordering manipulation.

**Fallback:** if DHT returns no providers (or all returned providers are filtered out by the lookup-integrity checks), fall back to the on-chain origin directory. If that also returns nothing, the blob is not available in the network.

**Origin discovery.** A requester that prefers an authorized origin for a hash (e.g., a cache-miss pull where freshness from a publisher-committed source is desirable) discovers candidates through the standard DHT path. The DHT does not discriminate origin vs cache providers — `StoreRequest` is the same wire format regardless of role — so any holder may publish a record. The wire protocol does not surface origin-vs-cache status at probe time either; the requester resolves origin status off-chain by reading `PublisherRegistry.namespaceOf(hash)` and `OriginAssignment.getOrigins(namespaceId)` and intersecting against the probed peer set.

The on-chain origin set is also the directory of last resort if the DHT returns no providers: resolve namespaces via `PublisherRegistry.namespaceOf(hash)` and union the operator-address sets via `OriginAssignment.getOrigins(namespaceId)` for each ([ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)). Operator addresses map to NodeIds via `CapacityBond.nodeIdOf(operator)` (a single read per operator, returning `(nodeId, active)` — see [ADR 003 § NodeId-to-Ethereum Binding](003-payments.md#nodeid-to-ethereum-binding) and [ADR 016 § Off-Chain Read API](016-contract-interactions.md#off-chain-read-api-client--node-bootstrap)); the requester filters by `active == true` and probes those NodeIds directly. The read chain (namespace lookup → operator union → NodeId binding → blacklist filter) has no inter-call dependencies *within* a tier and batches via the standard `Multicall3` aggregator deployed on Arbitrum, collapsing the fallback to a few RPC round-trips; the batching pattern is left to client libraries since it does not affect protocol semantics. This fallback is uncommon — under normal operation the DHT contains entries for every actively-serving authorized origin — but provides a deterministic recovery path during DHT churn or bootstrap. For default-open content (`namespaceId == 0`) the on-chain directory is `OriginAssignment.getOrigins(0)` — the DAO-maintained default-open allow-list — resolved to NodeIds the same way as registered namespaces.

#### Bootstrap

On node startup:

1. Build initial routing table from the on-chain registry peer list (same source as the peer table bootstrap in [ADR 019](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow)).
2. Issue `FindNode(self.node_id)` to initial peers — standard Kademlia self-lookup that populates k-buckets.

The registry-seeded peer list participates in DHT lookups immediately, so there is no separate bootstrap window during which content discovery is unavailable. If a `FindValue` lookup returns no providers during the first few seconds — before k-buckets are populated — the on-chain origin directory provides the deterministic fallback.

**Cold-start re-publish scheduling.** A node holding C cached blobs at startup must establish DHT records for all of them. The publisher MUST draw a per-record jitter from `uniform(0, 40 min)` for each blob's first re-publish after startup, independently per record. The window matches the steady-state mean re-publish cycle (40 min — the mean of `uniform(30, 50)`), so bootstrap rate matches steady-state rate by construction. Subsequent re-publishes use the standard `uniform(30 min, 50 min)` interval per [§ STORE Flow (Cache Event → DHT Publish)](#store-flow-cache-event--dht-publish). Naive "re-publish everything on the next scheduler tick" implementations are non-conforming: at moderate cache sizes a single-tick re-publish issues `C × (K+3)` STOREs in one window, saturating the per-peer rate limit at every receiver and turning startup into a multi-minute rate-limited drip. See [§ DHT Bandwidth Analysis](#dht-bandwidth-analysis) for the throughput model.

### DHT Bandwidth Analysis

Per-RPC wire costs (32B hashes and NodeIds, varint framing, QUIC stream overhead):

| RPC | Request | Response | Round-trip typical |
|-----|---------|----------|--------------------|
| FindValue | ~100B | ~500B typical, ~2.3 KB max (50 providers + 20 closer_nodes) | ~600B |
| Store | ~100B | ~70B `StoreAck` | ~170B |
| FindNode | ~100B | ~700B (20 closer_nodes) | ~800B |

#### Steady state

STORE re-publish dominates and scales with cached blob count C, **not** network size N. Each STORE targets K+3 receivers regardless of N; a receiver's inbound rate from any one publisher decreases as 1/N while the publisher count increases as N, leaving total per-receiver inbound constant in N. With K+3=23 targets and 40-minute average re-publish cycle:

| C (cached blobs/node) | STORE bandwidth (per node, each direction) | Notes |
|-----------------------|---------------------------------------------|-------|
| 100 | ~1.3 Kbps | Light client |
| 1,000 | ~13 Kbps | Typical staked node |
| 10,000 | ~130 Kbps | Comparable order to gossip at N=1000 ([ADR 001 § Gossip Bandwidth Analysis](001-network.md#gossip-bandwidth-analysis)) |
| 100,000 | ~1.3 Mbps | Heavy-caching node |

FIND_VALUE traffic scales with the cache-miss rate M and weakly with N (hop count grows as log N, mitigated by α=3 parallelism). At N=500 and M=1 miss/sec: ~15 RPCs per miss × ~600B/RPC ≈ 75 Kbps outbound. FIND_NODE bucket-refresh traffic (every 1 hour per bucket × ~3 RPCs/refresh) is ~1.4 Kbps — negligible.

At a representative operating point — C=10,000, M=1 miss/sec, N=500 — total per-node DHT bandwidth is ~200–300 Kbps in each direction, the same order as gossip at scale.

#### Bootstrap surge

A restarting publisher with C cached blobs needs `C × (K+3)` STOREs to fully populate DHT records. At C=10,000: 230,000 STOREs. The natural spread mechanism is per-record jitter on cold start ([§ Bootstrap](#bootstrap)): with `uniform(0, 40 min)` draws, bootstrap rate is `230,000 / (40 × 60) ≈ 96 STOREs/sec` averaged across all receivers, matching steady-state rate by construction (the window equals the steady-state mean cycle of 40 min). This fits comfortably within the rate-limit budget. Without per-record jitter at cold start, the same 230,000 STOREs deliver in a single scheduler interval, saturating the per-peer rate limit at every receiver. The cold-start jitter requirement is what makes bootstrap the publisher's problem to schedule rather than the receiver's problem to absorb.

#### Headroom against rate limits

At C=10,000 and N=500 steady state, the per-peer inbound STORE rate is `(C × (K+3)) / (N × cycle_sec)` ≈ 0.19/sec average, ~2/sec burst under jitter clustering:

| Bucket | Modeled steady-state load | Limit ([§ DHT Rate Limiting](#dht-rate-limiting)) | Headroom |
|--------|---------------------------|----------------------------------------------------|----------|
| Per-peer (NodeId) | ~0.19/sec avg, ~2/sec burst | 20/sec | ~10× burst, ~100× sustained |
| Per-IP | same in single-peer-per-IP case | 100/sec | ~50× |
| Global inbound | ~95/sec STORE + ~10–50/sec FIND_VALUE serving ≈ 100–150/sec | 1000/sec | ~7–10× |

The limits are conservative ceilings on adversarial load, not steady-state operating targets. Operators may tighten or loosen them; the table above is the baseline for what's economically justifiable at the analyzed parameters.

#### Bottleneck regimes

Three asymptotic regimes inform protocol-level optimizations:

- **Low C (≤1,000).** Bandwidth is gossip-dominated; DHT is a rounding error. No DHT-specific optimization warranted.
- **Moderate C (10,000–50,000).** DHT and gossip are comparable. The bottleneck is QUIC stream-setup overhead — 230,000 STOREs at C=10,000 bootstrap cost ~39 MB total, of which ~12 MB is per-stream framing, ~7 MB is repeated `holder` field across every request, and ~7 MB is repeated `hash` field across every per-RPC ack. Batched STORE collapses concentration into one RPC per publisher-receiver pair per scheduler tick (256 hashes per batch ≈ 898 batches total), with a `Vec<bool>` ack carrying per-hash outcomes in request order (no hashes repeated). Total bootstrap bytes-on-wire drop to ~8 MB — **a ~80% reduction**, of which ~30 percentage points come from framing elimination, ~20 from `holder` de-duplication, and ~20 from ack-shape compaction (bitmap-style ack instead of per-result hash). Specified in [§ STORE Flow (Cache Event → DHT Publish)](#store-flow-cache-event--dht-publish) and [§ DHT Rate Limiting](#dht-rate-limiting).
- **High C (≥100,000).** DHT dominates and per-publisher STORE bandwidth exceeds 1 Mbps. The protocol does not impose a ceiling on C; operator-policy caps on cached-blob count become the relevant capacity-planning lever.

### Popularity Signals and Market Dynamics

Content discovery in an incentive-driven network requires nodes to learn what content is in demand *before* being asked to serve it. Two complementary signals provide this.

#### Signal 1: DHT FIND_VALUE Query Frequency (Non-Suppressible)

In Kademlia, `FindValueRequest` messages for hash H are routed to nodes closest to H in keyspace **regardless of whether those nodes hold H**. A node close to H receives all FIND_VALUE queries for H from the entire network without holding H and without receiving any gossip.

This creates a **natural popularity oracle that cannot be suppressed**:

- Many FIND_VALUE queries for H → H is in high demand.
- The node is already well-positioned to be a STORE target for H.
- Prefetching H and publishing a STORE record turns incoming routing queries into paid delivery opportunities.

The signal is honest by construction: FIND_VALUE traffic reflects real client demand, not voluntary self-reporting, and arrives regardless of what anyone gossips.

#### Signal 2: Local Cache-Miss Frequency

Each node tracks cache miss timestamps per hash in a bounded map (`HashMap<Hash, VecDeque<u64>>`, max 10,000 entries, LRU eviction). Each miss appends a timestamp (refreshing the entry's LRU position); entries older than 5 minutes are pruned on access. When a hash crosses a configurable threshold (default: 3 misses in 5 minutes), the node proactively pulls the blob via the DHT FIND_VALUE → probe → `cdn/client/v1` (paid) path.

#### Prefetch Decision

A node MAY prefetch hash H when either signal crosses its threshold, subject to operator-local policy. Prefetch is operator policy, not protocol behavior: the wire protocol carries no prefetch state, and the receiving end of any pull cannot distinguish a prefetch-driven pull from a regular cache-miss pull-through.

##### Threat model

Both demand signals are cheap to manufacture, since neither the FIND_VALUE wire path nor the `cdn/probe/v1` wire path requires payment or any signature beyond a QUIC NodeId. Two compositional Sybil attacks follow:

1. **Demand-only Sybil.** Attacker fans out FIND_VALUE queries or `cdn/probe/v1` requests from rotating NodeIds and IPs to drive a victim's prefetch toward content the attacker chooses. Bounded above by the DHT and probe rate limits ([ADR 005 § Probe rate limiting](005-protocol.md#probe-rate-limiting)), but those bounds throttle the rate, not the existence, of the attack.
2. **Demand-supply Sybil.** Attacker also stakes a node, publishes a synthetic blob to the DHT pointing at their own node, and Sybil-triggers the victim's prefetch for that blob. The victim's DHT FIND_VALUE for the hash returns only the attacker; the attacker is paid USDC for delivering bytes no real customer demanded. Stake is recoverable on deregister, so attacker cost is the unbond opportunity cost; revenue is `delivery_rate × blob_size` per extracted blob, bounded above by `deliveryCeiling`.

Acceptance Criterion 5's reputation penalty only fires on *false* STORE records — the attacker's STORE is honest at the wire level (they really do hold the bytes they generated). The demand they manufactured is what's synthetic, and the wire protocol has no way to detect that from the publisher side.

##### Recommended configuration

A node MAY prefetch from popularity signals subject to a configuration block whose recommended defaults close both attacks:

| Key | Recommended default | Purpose |
|---|---|---|
| `prefetch.enabled` | `false` | Opt-in. Operators must affirmatively choose to take on the prefetch surface — disables both signals when false. |
| `prefetch.require_authorized_origin` | `true` | Prefetch fires for a hash only if the DHT FIND_VALUE candidate set contains at least one operator currently authorized as origin for the hash's namespace via `OriginAssignment.getOrigins(namespaceId)`, with `namespaceId == 0` resolving to the default-open allow-list (see [ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)). Closes the demand-supply Sybil attack: an attacker has to obtain DAO-ratified origin status to bait a prefetch, which is governance-gated by timelock. The cache-tier serving role is unaffected — once any authorized origin holds the hash, cache-tier candidates compete on the unified selection score as usual. |
| `prefetch.budget_usdc_per_hour` | operator-set, finite | Hard circuit breaker on aggregate prefetch spend over a rolling 1-hour window. Independent of the origin gate; defends the loss function even if the gate is disabled or partially defeated. The default value is operator-policy, but *some* finite cap is the load-bearing recommendation. |
| `prefetch.find_value_threshold` | `5` queries | Signal 1 trigger: FIND_VALUE queries for hash H received within `prefetch.threshold_window_secs`. |
| `prefetch.miss_threshold` | `3` misses | Signal 2 trigger: local cache misses for hash H within `prefetch.threshold_window_secs`. |
| `prefetch.threshold_window_secs` | `300` | Shared rolling-window length for both trigger signals (matches the existing 5-minute LRU pruning window for the miss tracker). |
| `prefetch.demand_quality_min_ratio` | `0.1` | Auto-throttle predicate: `served_bytes / acquired_bytes` over the rolling demand-quality window. When the ratio falls below the floor, prefetch pauses until it recovers. Detects sustained signal poisoning past the origin gate. |
| `prefetch.demand_quality_window_secs` | `3600` | Rolling-window length for the demand-quality predicate. |

`OriginAssignment` state for the gate is kept current via subscription to `AssignmentActivated`, `AssignmentRevoked`, and `DefaultOpenAllowlistUpdated` events against the local registry cache (same pattern used elsewhere for blacklist polling and channel-state queries); on RPC unavailability the gate fails closed (no prefetch) until the cache recovers.

##### Scope and limits

The gate governs the protocol-driven *speculative acquisition decision* only. The cache role remains permissionless: any staked operator may serve any hash they have, and the cache-miss pull-through path used to fulfill an in-flight `cdn/client/v1` `StreamRequest` from a paying customer is unaffected by these knobs (it has its own selection logic and is driven by an active paid request, not by speculative popularity inference).

A publisher whose `OriginAssignment` has not yet been ratified (timelock per [ADR 009](009-governance.md#adr-009-governance-model)) does not get cache-tier propagation via prefetch during the wait — content is served from their own configured origin until ratification. Truly-unclaimed hashes (no namespace, not default-open) get no prefetch ever; operators wanting to cache them MUST explicitly pin (see [appendix-blob-cache-eviction.md § Operator pinning overrides LRU](appendix-blob-cache-eviction.md#operator-pinning-overrides-lru)).

Observability for the prefetch surface is in [appendix-observability.md § Prefetch Metrics](appendix-observability.md#prefetch-metrics).

#### No Discovery Fees

DHT STORE and FIND_VALUE operations carry no protocol-level fee. The incentive to publish STORE records is indirect: advertising that you hold a blob attracts probe traffic, which converts to paid delivery. Charging for DHT operations would create a new attack surface (collect fee, fail to hold) requiring a new slash condition. All fees remain on delivery.

### Interaction with Existing Protocols

| Mechanism | Interaction with DHT |
|-----------|---------------------|
| `cdn/probe/v1` | Unchanged. DHT provides candidates; `cdn/probe/v1` confirms live availability and measures latency. Probe cache (15s TTL, [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)) still prevents redundant probes for recently confirmed providers. |
| `cdn/client/v1` | Unchanged. All delivery is paid; DHT affects only how providers are discovered. |
| `NodeAnnounce` gossip | Unchanged. Carries node-level metadata only (region, load); demand signals are derived from DHT FIND_VALUE traffic and local cache misses. No new gossip message types. |
| Reputation system ([ADR 008](008-reputation.md#adr-008-reputation-system)) | A node publishing a false STORE record fails at probe time → reputation penalty → fewer clients selected. No new slash condition needed. |
| Eviction hold ([ADR 005](005-protocol.md#adr-005-wire-protocol)) | Nodes stop re-publishing DHT records when a blob is evicted. TTL ensures stale records expire within 1 hour. |
| Client discovery ([ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model)) | Clients use DHT FIND_VALUE for content discovery the same way nodes do. The on-chain origin-directory fallback applies equally. |

### Schema Evolution

`cdn/dht/v1` follows the standard evolution model from [ADR 013](013-schema-evolution.md#adr-013-schema-evolution):

- **Minor** (new optional fields on existing message types): no ALPN bump.
- **Medium** (new mandatory fields): `cdn/dht/v2` ALPN.
- **Major** (incompatible routing changes): new ALPN + migration period.

`DhtMessage` uses a top-level enum consistent with the per-ALPN protocol enum pattern in [ADR 013 — Protocol Enums](013-schema-evolution.md#protocol-enums). Unknown variants are silently dropped.

New optional variants (e.g., `BatchStore` / `BatchStoreAck` added for publishing optimization in [§ STORE Flow (Cache Event → DHT Publish)](#store-flow-cache-event--dht-publish)) are minor changes — no ALPN bump. Publishers detect receiver support by issuing the new variant and falling back to the per-hash equivalent on stream-close-without-ack, the natural signal under "unknown variants are silently dropped." This negotiation pattern relies on the variant being a pure optimization with an existing per-hash equivalent; variants without a fallback path would require an ALPN bump.

### Acceptance Criteria

1. A node in a 30-node network can discover providers for a cached blob in ≤3 FIND_VALUE hops.
2. A node in a 500-node network can discover providers in ≤5 FIND_VALUE hops.
3. A cache event (blob added) generates ≤(K+3) (=23) outgoing STORE messages, not O(N).
4. A stale STORE record (node evicted the blob) expires within TTL (1 hour) with no explicit retraction.
5. A false STORE record (node claims to hold a blob it doesn't) fails at the probe step; the publishing node incurs a reputation penalty within one gossip cycle.
6. During bootstrap (routing table < k entries), the on-chain origin directory provides the fallback.
7. A node with `prefetch.enabled = true` observing a prefetch trigger for hash H (either ≥`prefetch.find_value_threshold` FIND_VALUE queries or ≥`prefetch.miss_threshold` local cache misses within `prefetch.threshold_window_secs`) initiates a prefetch for H, subject to the `prefetch.require_authorized_origin` gate (default `true`), the `prefetch.budget_usdc_per_hour` ceiling, and the demand-quality auto-throttle (see [§ Prefetch Decision](#prefetch-decision)).
8. Demand signals derive from DHT FIND_VALUE traffic and local cache-miss timestamps; both are emitted as observability metrics in [Appendix: Observability](appendix-observability.md#appendix-observability-and-metrics).
9. An accepted `StoreRequest`'s record TTL is anchored on the receiver's wall-clock at acceptance time (`expiry_us = receive_us + record_ttl_us`), independent of any holder-supplied timestamp.
10. A receiver enforces both a per-publisher record cap (hard reject with `StoreAck { accepted: false }` when at cap) and a per-node global cap (global LRU eviction when at cap and the inserting publisher is below its per-publisher cap). No publisher can force eviction of another publisher's records by exceeding its own cap.
11. `cdn/dht/v1` inbound traffic is bounded by global, per-IP, and per-peer token buckets; rejected requests close the stream with `RATE_LIMITED` and are counted in `decdn_dht_rate_limit_rejections_total` labeled by layer.
12. A `FindValueResponse` containing `closer_nodes` entries whose XOR distance is not strictly less than the responder's own has those entries dropped by the requester; honest entries from the same response are retained.
13. The requester randomizes the surviving provider set before issuing `cdn/probe/v1` requests; probe order is statistically independent of `FindValueResponse.providers` order.
14. NodeIds returning `has_blob: false` for hash H are not re-probed for H within the negative probe cache TTL ([ADR 001 § Probe cache](001-network.md#probe-cache)).
15. Re-publish time per record is drawn from `uniform(30 min, 50 min)` independently per draw; STORE targets are the K+3 closest nodes in the publisher's routing table.
16. On cold start, each cached blob's first re-publish time is drawn from `uniform(0, 40 min)` independently per record. A naive single-tick bulk re-publish across all cached blobs at startup is non-conforming.
17. A `BatchStoreRequest` with `n ≤ 256` hashes from publisher P produces a `BatchStoreAck` whose `results` carries one `bool` per request hash, in request order. The batch-level `holder` is checked once against the authenticated QUIC NodeId; the remaining admission decisions (rate limit, per-publisher quota with two-tier eviction, active-staker check) are applied per-hash and match those of `n` separate `StoreRequest` RPCs from P.
18. A `BatchStoreRequest` consumes between 1 and `n` tokens from each rate-limit bucket: stage 1 charges 1 token before deserialization; stage 2 charges up to `n - 1` more after reading `n = hashes.len()`, bounded by remaining budget. The first `k` admitted hashes pass per-hash processing; the remaining `n - k` are marked `false` in the ack without further processing.
19. A `BatchStoreRequest` with `hashes.len() > 256` is rejected by closing the stream with `MALFORMED_MESSAGE` (`0x03`); the publisher MUST split larger publish sets across multiple batches.
20. A publisher whose `BatchStoreRequest` to a given receiver results in stream-close-without-ack MUST fall back to per-hash `StoreRequest` for subsequent publications to that receiver; the publisher SHOULD cache the negative result for some bounded duration to avoid repeated drop-and-fallback cycles.
