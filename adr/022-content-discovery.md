# ADR 022 — Content Discovery at Scale

**Status:** Proposed
**Date:** 2026-04-08

## Context

Content discovery answers: "which nodes currently hold blob H, and which byte ranges of it?" The answer drives both client→node delivery (client picks a node to stream from) and node→node pull-through (a node with a cache miss finds a provider to pull from). A holder that has only some ranges is serving supply too — disjoint ranges compose a blob from several partial holders — so discovery carries a coarse coverage bitmap, not a possession bool alone.

A **Kademlia-based content DHT** has the right properties for an incentive-driven CDN:

- Nodes publish `(hash → NodeId, coverage)` records **once they hold at least one verified 64 MiB block of a blob** — a voluntary, self-interested advertisement to attract paying clients. No economic incentive exists to publish records for blobs you don't hold (false records attract probes that reveal the lie, degrading reputation and earnings).
- **O(log N) lookup** — a querying node contacts ~5 peers to find providers at 1,000 nodes.
- **O(log N) publish cost** — a STORE record is pushed to only the K nodes closest to the hash in keyspace. No global broadcast.
- **The probe step is preserved** — DHT lookup narrows the candidate set; `cdn/probe/v1` still confirms live availability and measures latency before any delivery commitment.

The DHT defined here is a separate content DHT scoped to the registered node set, operating over the `cdn/dht/v1` ALPN on iroh QUIC. It is distinct from iroh's built-in `DhtDiscovery` (mainline BitTorrent DHT via pkarr), which resolves `NodeId → address` on the public internet.

## Decision

`cdn/dht/v1` is the **primary content discovery mechanism**. The DHT bootstraps from `CapacityBond.getRegisteredNodes()` — a freshly-started node's first peers come from the on-chain registry and immediately participate in DHT lookups, so there is no separate bootstrap window during which DHT cannot resolve. When a DHT lookup returns no providers, the on-chain origin directory is the deterministic last-resort fallback.

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

/// A coarse map of which DISCOVERY_BLOCK_BYTES (64 MiB) blocks of a blob the
/// sender will serve. Bit `i` set ⇒ the sender serves block `i` = bytes
/// `[i·64MiB, min((i+1)·64MiB, total))`: either a cached, root-verified block,
/// or — when the sender is an origin-serve-capable source for `H` — any block,
/// which it warms and verifies against `H` on delivery. An origin knows the
/// total size (it read it to answer at all), so it sets every bit. The
/// granularity is decoupled from the 16 KiB bao verification granularity:
/// coarse enough to keep records and probe frames small, fine enough to expose
/// genuine disjointness between holders. An empty bitmap means the sender
/// serves no full block (a sub-block, non-origin fragment reports empty and is
/// not advertised); an all-ones bitmap means a full holder or an origin.
struct Coverage {
    blocks: Vec<u8>,         // bit-packed, little-endian block index
}

/// A provider record: a holder and the ranges it can serve from cache.
struct Provider {
    node: NodeId,
    coverage: Coverage,      // may be partial; empty is never published
}

/// Response: known providers and/or closer nodes to continue the lookup
struct FindValueResponse {
    hash: Hash,
    providers: Vec<Provider>,  // holders and their coverage (may be empty)
    closer_nodes: Vec<NodeId>, // K closest nodes to hash in responder's table
}

/// Publish a content record: "I hold these ranges of this hash"
struct StoreRequest {
    hash: Hash,
    holder: NodeId,          // the node claiming to hold this hash
    coverage: Coverage,      // the blocks it has verified; non-empty
}

struct StoreAck {
    hash: Hash,
    accepted: bool,
}

/// Batched publication: many hashes from one holder to one receiver in a single RPC.
/// Part of cdn/dht/v1; carries bootstrap and dense re-publish. See § DHT Bandwidth Analysis.
struct BatchStoreRequest {
    entries: Vec<(Hash, Coverage)>, // ≤256 entries; one holder, many hashes with per-hash coverage
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
| Max records per node | 1,000,000 | ~450 MB memory at max record size (200 B coverage bitmap + ~250 B index overhead per record); typical records carry far smaller bitmaps |
| Max records per publisher (per receiver) | 100,000 | Bounds a single publisher's storage footprint at any receiver; set well above the chunk count of a large bundle so an honest heavy-caching publisher is never rejected, while the per-node cap still admits multiple such publishers concurrently |

**Per-publisher quota and eviction.** Two limits apply: a per-publisher cap of 100,000 records (hard) and a per-node global cap of 1,000,000 records. The two limits compose as follows:

- When publisher P is at its per-publisher cap, the receiver MUST reject new `StoreRequest`s from P with `StoreAck { accepted: false }`. P's existing records are not evicted by P's own further STOREs, and no other publisher's records are evicted on P's behalf.
- When the global cap is reached but the inserting publisher P is below P's per-publisher cap, the receiver MUST evict the globally-oldest record (by receive time) to make room for the new STORE.

The active-bonded-operator filter is the security boundary against record spam: a publisher must be a bonded operator to insert at all, so filling slots carries a stake cost. The per-publisher cap is a fairness bound layered on top — it keeps one publisher's footprint from dominating a receiver's store, so the global set stays a broad sample of the network rather than one node's catalogue. With the cap in place, global LRU for below-quota inserts is safe and ensures that newly-active bonded publishers can always participate in the DHT — without global LRU, a node reaching capacity with many publishers each at their cap would lock out the next publisher entirely. The per-publisher cap also bounds the surface area of a false-STORE publisher (a node advertising hashes it does not hold) at any one receiver, so the requester's negative probe cache ([ADR 001 § Probe cache](001-network.md#probe-cache)) sees a bounded set of `(NodeId, hash)` pairs to absorb.

A node **stops re-publishing** when it evicts the blob. Stale records self-expire within TTL — no explicit retraction messages needed. **TTL is anchored on the receiver's wall-clock at acceptance time:** the receiver computes `expiry_us = receive_us + record_ttl_us`, where `receive_us` is the receiver's wall-clock microsecond timestamp at acceptance and `record_ttl_us` is the Record TTL parameter above in microseconds (1 hour = 3,600,000,000 μs). `StoreRequest` carries no sender-asserted timestamp, so record lifetime is independent of any clock the holder controls. Each re-publish (scheduled within the jittered re-publish window) refreshes the receiver's record by replacing stored `expiry_us` with one derived from the new `receive_us`, extending effective lifetime ahead of the previous expiry while the holder still has the blob. The re-publish also carries the holder's current `coverage`, so a partial holder that has filled more blocks since its last STORE advertises the wider set on its next cycle.

**Coverage is a stale routing hint, never a serve commitment.** The record is a push record refreshed at most once per re-publish window, so its `coverage` lags the holder's live bitfield — a holder that filled or evicted blocks since its last STORE. A requester uses `coverage` to prune which holders it probes for a given range; the fresh, authoritative coverage is the one a `cdn/probe/v1` response carries at query time ([§ Range-keyed partial-holder discovery](#range-keyed-partial-holder-discovery)). For a holder still filling, coverage grows monotonically until the blob is complete or evicted, so a stale "has block `k`" is more often still-true than false; a false positive costs one wasted probe, a false negative one missed candidate, both recovered at probe time.

#### STORE Flow (Cache Event → DHT Publish)

When a node verifies its first 64 MiB block of blob H — on any verified partial, not only on `Complete`:

1. Identify the K+3 closest nodes to H from the local routing table. The three positions beyond K are overflow targets — publishing to a wider set than the minimum required by the routing geometry means an attacker forcing record expiry by suppressing receivers must take down K+3 hosts rather than K.
2. Send a `StoreRequest { hash: H, holder: self.node_id, coverage }` to each, where `coverage` is the holder's current block bitmap for H.
3. Schedule the next re-publish at `T + uniform(30 min, 50 min)`, where `T` is the local wall-clock at which step 2 was last performed for this blob. The jitter is drawn independently per record so the next re-publish window for a given hash is not predictable from outside the publisher.

**Re-publish.** Re-publish reuses the same step 1–2 sequence with a fresh jitter draw per step 3; missed slots (e.g., local downtime) re-fire at the next scheduler tick rather than back-filling.

**Batched STORE.** When a publisher has multiple hashes to send to the same receiver — typically at cold start ([§ Bootstrap](#bootstrap)) or when re-publish windows across cached blobs concentrate on overlapping receiver sets — it sends a single `BatchStoreRequest { entries, holder }` covering all `(hash, coverage)` pairs destined for that receiver instead of separate `StoreRequest` RPCs. The re-publish scheduler groups the hashes due in a drain cycle by receiver (each hash's K+3 closest nodes) and sends each receiver its due set as one or more batches. The receiver checks the `holder == authenticated QUIC NodeId` equality once for the batch (the field is a single `NodeId`, not per-hash) and applies the remaining admission rules per-hash (per-publisher quota with two-tier eviction, active-bonded-operator check, rate-limit accounting per [§ DHT Rate Limiting](#dht-rate-limiting)). The reply is `BatchStoreAck { results }` with per-hash outcomes in request order. Receiver-anchored TTL is computed per-hash from a single `receive_us` for the batch. The eager publish on a fresh cache event carries a single hash and uses per-hash `StoreRequest` — a size-1 batch buys nothing.

Batch size is bounded at 256 entries. Each entry is a hash plus a coverage bitmap; the bitmap is `ceil(blocks / 8)` bytes, one bit per 64 MiB block, so even a 64 GiB blob's coverage is 128 bytes and the batch payload stays small. Publishers split larger publish sets into multiple batches. A `BatchStoreRequest` with `entries.len() > 256` is rejected by closing the stream with `MALFORMED_MESSAGE` (`0x03`) per [ADR 013 § Application Error Codes](013-schema-evolution.md#application-error-codes); the publisher is expected to fix the request shape and retry, not back off.

Each coverage bitmap is separately bounded at decode: the largest blob partial-holder discovery represents is 100 GiB, which spans 1,600 discovery blocks and bit-packs to a 200-byte bitmap, so a `Coverage` longer than that names more blocks than any advertisable blob spans and is rejected as `MALFORMED_MESSAGE`. A blob larger than this is published as a chunked bundle of smaller content-addressed chunk-blobs, each independently discoverable, rather than one monolithic blob. This is a wire representability bound, not a serving cap — a node may hold and serve a larger blob (subject to its own `max_blob_size_mb`); it simply cannot advertise partial coverage for one over the DHT. This bound is independent of the per-publisher record quota and composes with it: stored memory per publisher is at most `record cap × 2,048 bytes`, so a bonded publisher cannot pin arbitrary receiver memory by publishing records whose bitmaps are larger than any real blob's block count. The bitmap carries no `num_blocks` field, so the byte length is the only wire-checkable bound; a receiver still clamps bit interpretation to the blob's own block count at query time (spurious high bits in the trailing byte are ignored, never trusted as coverage).

`BatchStore` / `BatchStoreAck` are part of `cdn/dht/v1` from the first deployment: every DHT node implements them, so there is no per-hash fallback path and no support negotiation. The motivation for batching and the modeled bandwidth savings are in [§ DHT Bandwidth Analysis](#dht-bandwidth-analysis).

The receiving node MUST reject any record whose `holder` does not equal the authenticated NodeId of the inbound QUIC connection. NodeIds are 32-byte ed25519 public keys ([§ Routing Table](#routing-table)) and iroh's QUIC handshake authenticates the connection against that key, so the equality check binds the record to its claimed origin without a per-record signature. This depends on the publisher-only re-publish model ([§ Content Records and TTL](#content-records-and-ttl) and [§ STORE Flow (Cache Event → DHT Publish)](#store-flow-cache-event--dht-publish) step 2): records are pushed directly by the holder to the K-closest nodes and never propagated peer-to-peer. If a future scheme introduces peer relay of records (e.g., Kademlia replication-on-churn), receivers no longer have a direct authenticated connection to `holder` and a per-record signature MUST be reintroduced. In that case an in-scope sender timestamp MUST also be reintroduced inside the signed body — otherwise the captured signed bytes are constant and trivially replayable, defeating the signature. The receiver MUST additionally verify that `holder` is in the cached active-bonded-operator set (populated from `CapacityBond.getRegisteredNodes()` per [ADR 019](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow)) before accepting the record; unbonded publishers are rejected with `StoreAck { accepted: false }`. Receiving nodes do **not** verify that the holder actually has the blob, nor that its advertised `coverage` is accurate — that is the probe step's job. A false STORE publisher (a node claiming to hold a blob it does not) fails at probe time, degrading its reputation; an inflated `coverage` wastes one prober's probe against a holder that does not serve the claimed range, a reputation matter ([ADR 008](008-reputation.md#adr-008-reputation-system)), not a slashable one — there is deliberately no "advertised coverage but did not serve" offense. **Note:** "publisher" in this ADR refers to a node publishing a DHT STORE record (an act of advertising). It is distinct from the on-chain *content publisher* identity defined in [ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces), which is an Ethereum address registered in `PublisherRegistry`. Where confusion is possible this ADR uses "STORE publisher" or "holder" for the DHT-record sender.

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
2. **Per-hash admission.** Deserialize the request header to obtain `n = entries.len()`. Verify the batch-level `holder == authenticated QUIC NodeId`; reject the entire batch with `MALFORMED_MESSAGE` if not. Let `b = min(remaining_tokens_per_peer, remaining_tokens_per_ip, remaining_tokens_global)`. Consume `min(b, n - 1)` more tokens from each bucket. The first `k = 1 + min(b, n - 1)` entries pass through per-entry processing (per-publisher quota with two-tier eviction, active-bonded-operator filter); the remaining `n - k` entries are marked `accepted: false` in the ack without further processing.

Total tokens consumed: `k`, matching what the per-hash equivalent would have charged. The work done per second is bounded the same whether the publisher sends `n` separate `StoreRequest`s or one `BatchStoreRequest` of size `n`. The wire-framing, `holder`-de-duplication, and ack-shape savings are real; the throughput ceiling is unchanged. Partial admission avoids forcing a publisher to retransmit a full batch when only the tail is over budget — the ack identifies rejected hashes by their request-order position, and the publisher retries only those.

**Why three layers, not one.** Per-peer alone is bypassable: any QUIC client can initiate a `cdn/dht/v1` connection and rotate `NodeId` at zero cost (the STORE bonded-operator-set check applies only after admission, and FIND_VALUE / FIND_NODE require no bonded-operator membership at all). The per-IP layer raises the cost of single-source flooding — IP rotation requires money (proxies, IPv6 prefix delegation, cloud bills). The global cap is defence in depth against distributed attacks across many IPs that would otherwise exhaust the node's routing-table-lookup and response-serialization capacity. The asymmetry between cheap request (`FindValueRequest` is ~40 bytes on the wire) and expensive response (full k-bucket walk plus serialization of K closer-node entries) is what makes DHT flooding economical to mount without these limits.

**Interaction with the per-publisher quota.** STORE admission additionally consumes one slot from the publisher's record quota ([§ Content Records and TTL](#content-records-and-ttl)); the rate limit fires first, so a rate-limited STORE never consumes a quota slot. FIND_VALUE responses that route the requester onward (via `closer_nodes`) do not multiply rate-limit consumption on the responder — one inbound request, one bucket token, regardless of response size.

**Observability.** `decdn_dht_rate_limit_rejected_{per_peer,per_ip,global}_total` — three unlabeled sibling counters, the same shape as the probe-side metric. Siblings rather than one `layer`-labelled counter, per the sibling-counter convention ([appendix-observability.md § Reason splits](appendix-observability.md#reason-splits-sibling-counters-not-labels)); no single labelled `decdn_dht_rate_limit_rejections_total` counter exists; the three sibling counters above are the only export.

#### FIND_VALUE Flow (Cache Miss → DHT Lookup)

When a node gets a cache miss for hash H and the probe cache is empty:

1. Check local routing table for the α (=3) closest nodes to H.
2. Send parallel `FindValueRequest { hash: H }` to all α nodes.
3. Iterate: each responder returns known providers (each a `Provider { node, coverage }`) or closer nodes (standard iterative Kademlia). Apply the lookup-integrity filters below to each response before incorporating it into the candidate set.
4. Continue until providers are found or lookup converges (no closer nodes returned). The `FindValueRequest` carries no range — the query is hash-level, Kademlia semantics are unchanged, and coverage is response-side only. A requester seeking a specific range MAY drop providers whose (stale) `coverage` shows no overlap with it before probing, pruning the probe set; a requester composing a whole blob keeps them all.
5. Randomize the surviving provider set, then probe it via `cdn/probe/v1` to confirm live availability, measure latency, and read the holder's **fresh** coverage ([§ Range-keyed partial-holder discovery](#range-keyed-partial-holder-discovery)).
6. Select and assign per the delivery path: a single provider by unified node selection score for whole-blob single-source delivery ([ADR 001](001-network.md#node-selection-algorithm)), or disjoint ranges across covering providers for range-keyed composition ([ADR 039](039-multi-source-parallel-fetch.md#adr-039-multi-source-parallel-fetch-scheduling-on-cdnclientv1)); deliver via `cdn/client/v1`.

**Lookup integrity.** Three requester-side invariants govern lookup result acceptance, applied in order to each `FindValueResponse`:

1. **XOR distance check on `closer_nodes`.** The requester MUST drop any `closer_nodes` entry whose XOR distance to the target hash is not strictly less than the responder's own. A responder that returns "closer" nodes that are not in fact closer is attempting to redirect the lookup; honest Kademlia responders never produce such entries. Offending entries are dropped without affecting honest entries from the same response.
2. **Active-bonded-operator filter on `closer_nodes` and `providers`.** The requester MUST filter both fields against its cached active-bonded-operator set ([ADR 019](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow) registry cache). Unbonded NodeIds are silently dropped — the DHT routing pool is restricted to bonded operators, and STORE receivers reject unbonded publishers at admission ([§ STORE Flow (Cache Event → DHT Publish)](#store-flow-cache-event--dht-publish)), so an unbonded entry in either field is either responder misbehavior or stale state on the responder's side. Filtering at the requester ensures lookup iteration only routes through nodes that can legitimately participate and probe traffic only reaches nodes that can legitimately hold records.
3. **Negative probe cache consultation on `providers`.** The requester MUST consult the negative probe cache ([ADR 001 § Probe cache](001-network.md#probe-cache)) and drop any `(NodeId, H)` pair present. A NodeId that previously returned `has_blob: false` for H within the cache TTL is not re-probed for H during that window; this bounds the cost of false-STORE publishers at the K-closest receivers to one failed probe per requester per cache window.

After these three filters, the requester MUST randomize the order of the surviving provider set before issuing probe RPCs. Responder-side ordering is not authoritative: the wire protocol cannot enforce honest ordering on the response side, and a modified responder can deterministically promote attacker-controlled NodeIds to bias selection. Randomization on the requester is the load-bearing defense against ordering manipulation.

**Fallback:** if DHT returns no providers (or all returned providers are filtered out by the lookup-integrity checks), fall back to the on-chain origin directory. If that also returns nothing, the blob is not available in the network.

**Origin discovery.** A requester that prefers an authorized origin for a hash (e.g., a cache-miss pull where freshness from a publisher-committed source is desirable) discovers candidates through the standard DHT path. The DHT does not discriminate origin vs cache providers — `StoreRequest` is the same wire format regardless of role — so any holder may publish a record. The wire protocol does not surface origin-vs-cache status at probe time either; the requester resolves origin status off-chain by reading `OriginAssignment.getOrigins(namespaceId)` for the request's `namespaceId` and intersecting against the probed peer set. There is no on-chain hash→namespace lookup — the requester already knows the namespace (see [ADR 002 § Retrieval by namespace](002-content-addressing.md#retrieval-by-namespace)). The origin set is thus a property of the **namespace**, not of a specific operator holding a specific hash: any active operator in `getOrigins(namespaceId)` is a candidate origin for the whole namespace. This is a **routing** signal only — a requester uses it to prefer a publisher-committed source; a node never uses it to authorize or refuse a serve (the `namespaceId` is a routing hint, not a trust anchor, [ADR 002 § Hash-to-namespace association](002-content-addressing.md#hash-to-namespace-association)).

For a `namespaceId != 0` request, the on-chain origin set is also the directory of last resort if the DHT returns no providers: read `OriginAssignment.getOrigins(namespaceId)` ([ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)). Operator addresses map to NodeIds via `CapacityBond.nodeIdOf(operator)` (a single read per operator, returning `(nodeId, active)` — see [ADR 003 § NodeId-to-Ethereum Binding](003-payments.md#nodeid-to-ethereum-binding) and [ADR 016 § Off-Chain Read API](016-contract-interactions.md#off-chain-read-api-client--node-bootstrap)); the requester filters by `active == true` and probes those NodeIds directly. The read chain (operator union → NodeId binding → blacklist filter) has no inter-call dependencies *within* a tier and batches via the standard `Multicall3` aggregator deployed on Arbitrum, collapsing the fallback to a few RPC round-trips; the batching pattern is left to client libraries since it does not affect protocol semantics. This fallback is uncommon — under normal operation the DHT contains entries for every actively-serving authorized origin — but provides a deterministic recovery path during DHT churn or bootstrap. For a `namespaceId == 0` request there is no on-chain directory: it has no authorized origins, so discovery is DHT/cache only and a miss simply fails. The origin-directory read itself fails closed: when the chain RPC is unavailable — or the `OriginAssignment` / `PublisherRegistry` addresses are unconfigured, so the directory is empty — resolution yields no origin candidate rather than a spurious one, so the fallback simply finds no origin to route to rather than routing to a wrong one.

This fail-closed guarantee is the **cold** case: an empty or never-resolved directory. It bounds what an *unknown* namespace resolves to, not how fast a *known* one reacts during an outage. A node's resolved directory is a live in-memory projection maintained from the event tail and re-reads; the authorization check does no live RPC read (it consults only the cached origin set + the cached active-staker set) and the caches are not invalidated on a failed refresh. So a **warm** namespace — one already resolved before the RPC went away — keeps serving its cached authorization through a transient outage: it favors availability and serves stale, healing when the watcher's next successful read applies any change. The staleness window is bounded by the outage duration, and the conservative direction there is a de-authorization that lags (a revoked origin briefly still authorized), not an un-authorized one admitted.

### Range-keyed partial-holder discovery

A holder that has only some 64 MiB blocks of `H` is serving supply: disjoint blocks compose the blob from several partial holders, so serve load and revenue distribute across the population instead of funnelling onto full holders. Two discovery surfaces carry coverage, split by volatility.

**DHT record — stale routing hint.** The provider record carries the holder's `coverage` ([§ Content Records and TTL](#content-records-and-ttl)). It is node-facing (a cache-missing node's `FIND_VALUE`) and refreshed at most once per re-publish window, so it seeds admission: a requester prunes which holders it probes for a wanted range and never treats it as a serve guarantee.

**Probe response — fresh authority.** `cdn/probe/v1` carries the same 64 MiB coverage bitmap as an **unsigned** field in the probe extension ([ADR 013 § Tier 1](013-schema-evolution.md#adr-013-schema-evolution)), derived per-probe from the holder's live bitfield, so it is always current. A requester confirms coverage here before it commits a payment lane for a range.

**The signed set stays frozen — no contract change.** The signed probe fields `{hash, has_blob, rate_per_mb, timestamp_us}` are unchanged ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)). `has_blob` is redefined as "will serve ≥1 full 64 MiB block of `H`" — a cached, root-verified block, or any block when the node is an origin-serve-capable source — which is equivalently "the coverage bitmap is non-empty" and which the contract's existing `hasBlob == true` possession check covers unchanged. The block granularity aligns the possession bool with what discovery can route on: a node that advertises has at least one full block it will serve. A non-origin holder of only a sub-block fragment signs `has_blob: false` and is not discoverable, but serving any range of a blacklisted blob still signs `ok: true` over `H` in the `StreamResponse`, dispositive on its own, so every actual serve stays fully slashable regardless of the possession bool. No slash offense reads coverage detail, so the bitmap is unsigned; that also keeps the 64 MiB granularity a freely tunable knob rather than a typehash-frozen field.

An **origin-serve-capable** node — one that answers `has_blob` from its origin backend (an enumerable origin-held entry, or a live size probe) rather than from cache — sets every bit: it read the total size to answer at all, so it knows the block count, and it will serve any block by warming and verifying it against `H` on delivery. So `coverage` states what the sender will serve, not only what it has cached: a cache holder advertises its cached blocks, an origin advertises all of them, and a node that is both advertises all. This keeps the biconditional exact and never asks a non-origin to serve a block it lacks.

**Consistency rule.** `has_blob` and the coverage bitmap are two views of one fact, so they agree by definition: **`has_blob: true` ⟺ the bitmap is non-empty.** Either mismatch is malformed — `has_blob: false` with a non-empty bitmap, or `has_blob: true` with an empty one. Every requester MUST reject such a response. A node requester additionally scores it a protocol violation ([ADR 008](008-reputation.md#adr-008-reputation-system)), the same posture as the mandatory-`slash_sig` rule ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)); a client only rejects. The signed bool stays authoritative: a node cannot advertise via the unsigned bitmap while keeping its signed possession claim false, because compliant clients ignore such probes and the node earns nothing without signing stream evidence.

**Partial holders serve their ranges.** A holder serves any verified range it holds from cache, over the ordinary `cdn/client/v1` bao verified-range path ([ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1)) — no longer gated on `Complete`. Serving is range-granular (16 KiB bao chunk groups); only discovery is quantized to 64 MiB blocks, so a holder can serve finer than it advertises. A request for a range it does not hold falls to the existing warm-on-miss origin chain ([ADR 037](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality)), unchanged.

**Node assembly.** On a cache miss, a node discovers partial holders via `FIND_VALUE`, confirms coverage by probe, and composes the blob by assigning disjoint blocks across covering holders — the [ADR 039](039-multi-source-parallel-fetch.md#adr-039-multi-source-parallel-fetch-scheduling-on-cdnclientv1) scheduler driven on the node's own pull leg. The assembly is **demand-windowed**: the node fills only within the serve frontier's credit window, so it becomes a full holder exactly when the client consumes the whole blob and otherwise holds the demanded prefix. A block no holder covers is warmed once from origin, and the warmer's new partial coverage becomes discoverable supply for the next requester.

#### Bootstrap

On node startup:

1. Build initial routing table from the on-chain registry active set (the same source nodes build their peer view from in [ADR 019](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow)).
2. Issue `FindNode(self.node_id)` to initial peers — standard Kademlia self-lookup that populates k-buckets.

The registry-seeded peer list participates in DHT lookups immediately, so there is no separate bootstrap window during which content discovery is unavailable. If a `FindValue` lookup returns no providers during the first few seconds — before k-buckets are populated — the on-chain origin directory provides the deterministic fallback.

**Cold-start re-publish scheduling.** A node holding C cached blobs at startup must establish DHT records for all of them. The publisher MUST draw a per-record jitter from `uniform(0, 40 min)` for each blob's first re-publish after startup, independently per record. The window matches the steady-state mean re-publish cycle (40 min — the mean of `uniform(30, 50)`), so bootstrap rate matches steady-state rate by construction. Subsequent re-publishes use the standard `uniform(30 min, 50 min)` interval per [§ STORE Flow (Cache Event → DHT Publish)](#store-flow-cache-event--dht-publish). Naive "re-publish everything on the next scheduler tick" implementations are non-conforming: at moderate cache sizes a single-tick re-publish issues `C × (K+3)` STOREs in one window, saturating the per-peer rate limit at every receiver and turning startup into a multi-minute rate-limited drip. See [§ DHT Bandwidth Analysis](#dht-bandwidth-analysis) for the throughput model.

**Re-seed after a lost commit window.** A publisher schedules the first publish of a newly-cached blob from an internal cache-commit event. That event channel is bounded and best-effort: under sustained commit pressure it drops events, and the cache does not retain the dropped hashes. The publisher MUST then re-derive the full set of hashes it advertises and seed each one that is not already scheduled. Each re-seeded record draws its own offset from `uniform(0, 40 min)`, the same window as cold start. An immediate bulk re-publish is non-conforming here for the reason it is non-conforming at boot: it issues `C × (K+3)` STOREs in one window and saturates the per-peer rate limit at every receiver. Each re-derivation reads the advertised set once, at its start. A drop that happens after that read MUST cause another re-derivation, so a blob is never left waiting on a snapshot taken before its own commit. A blob missed by a dropped event thus becomes discoverable within one cold-start window of the re-derivation that observes it, not at the next process restart.

### DHT Bandwidth Analysis

Per-RPC wire costs (32B hashes and NodeIds, varint framing, QUIC stream overhead). Each `FindValue` provider carries a range-keyed `Coverage` bitmap alongside its `NodeId` (§ Content Records and TTL), which puts a single-block holder's per-provider cost at ~34B rather than a bare NodeId's 32B:

| RPC | Request | Response | Round-trip typical |
|-----|---------|----------|--------------------|
| FindValue | ~100B | ~500B typical, ~2.45 KB max (50 providers × 34B + 20 closer_nodes × 32B) | ~600B |
| Store | ~100B | ~70B `StoreAck` | ~170B |
| FindNode | ~100B | ~700B (20 closer_nodes) | ~800B |

#### Steady state

STORE re-publish dominates and scales with cached blob count C, **not** network size N. Each STORE targets K+3 receivers regardless of N; a receiver's inbound rate from any one publisher decreases as 1/N while the publisher count increases as N, leaving total per-receiver inbound constant in N. With K+3=23 targets and 40-minute average re-publish cycle:

| C (cached blobs/node) | STORE bandwidth (per node, each direction) | Notes |
|-----------------------|---------------------------------------------|-------|
| 100 | ~1.3 Kbps | Light client |
| 1,000 | ~13 Kbps | Typical bonded operator |
| 10,000 | ~130 Kbps | Heavy-caching operator |
| 100,000 | ~1.3 Mbps | Heavy-caching node |

FIND_VALUE traffic scales with the cache-miss rate M and weakly with N (hop count grows as log N, mitigated by α=3 parallelism). At N=500 and M=1 miss/sec: ~15 RPCs per miss × ~600B/RPC ≈ 75 Kbps outbound. FIND_NODE bucket-refresh traffic (every 1 hour per bucket × ~3 RPCs/refresh) is ~1.4 Kbps — negligible.

At a representative operating point — C=10,000, M=1 miss/sec, N=500 — total per-node DHT bandwidth is ~200–300 Kbps in each direction.

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

- **Low C (≤1,000).** DHT bandwidth is a rounding error against ordinary delivery traffic. No DHT-specific optimization warranted.
- **Moderate C (10,000–50,000).** DHT bandwidth becomes noticeable. The bottleneck is QUIC stream-setup overhead — 230,000 STOREs at C=10,000 bootstrap cost ~39 MB total, of which ~12 MB is per-stream framing, ~7 MB is repeated `holder` field across every request, and ~7 MB is repeated `hash` field across every per-RPC ack. Batched STORE collapses concentration into one RPC per publisher-receiver pair per scheduler tick (256 hashes per batch ≈ 898 batches total), with a `Vec<bool>` ack carrying per-hash outcomes in request order (no hashes repeated). Total bootstrap bytes-on-wire drop to ~8 MB — **a ~80% reduction**, of which ~30 percentage points come from framing elimination, ~20 from `holder` de-duplication, and ~20 from ack-shape compaction (bitmap-style ack instead of per-result hash). Specified in [§ STORE Flow (Cache Event → DHT Publish)](#store-flow-cache-event--dht-publish) and [§ DHT Rate Limiting](#dht-rate-limiting).
- **High C (≥100,000).** DHT dominates and per-publisher STORE bandwidth exceeds 1 Mbps. The protocol does not impose a ceiling on C; operator-policy caps on cached-blob count become the relevant capacity-planning lever.

### Popularity Signals and Market Dynamics

Content discovery in an incentive-driven network lets nodes learn what content is in demand. Realized demand is served reactively: a node that receives a cache miss for hash H serves it by window-paced pull-through, warming its cache as a paid, loss-bounded side effect of delivery ([ADR 037](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality)). Nodes do not acquire content ahead of a paying request — content propagates on the real, paid demand path plus explicit operator pinning.

#### Local cache-miss demand is reactive

A node that gets a cache miss for hash H serves it reactively by window-paced pull-through, warming its cache incrementally as a paid side effect of delivery ([ADR 037 § Node serving: window-paced pull-through](037-regional-proxy-warming.md#node-serving-window-paced-pull-through)). This covers realized local demand on real paying requests with bounded loss. Cache-miss frequency is not collected as a per-hash demand signal and does not drive acquisition — propagation follows the paid request path.

#### Cache warming is permissionless by default

The cache role is permissionless: any bonded operator may serve any hash it holds, and the cache-miss pull-through path used to fulfill an in-flight `cdn/client/v1` `StreamRequest` from a paying customer pulls and warm-caches unconditionally, driven by an active paid request rather than any speculative inference. The reactive path is not narrowed by the request's namespace — that is a routing hint, not a serve authorization ([ADR 002 § Hash-to-namespace association](002-content-addressing.md#hash-to-namespace-association)); unwanted content is bounded by the ramped credit window ([ADR 037 § Node serving](037-regional-proxy-warming.md#node-serving-window-paced-pull-through), [ADR 003 § Credit window](003-payments.md#credit-window)) and removed, once held, through [`ContentBlacklist`](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting).

A publisher not yet vetted in `OriginAssignment` (timelock per [ADR 009](009-governance.md#adr-009-governance-model)) does not get gated cache-tier propagation during the wait — content is served from their own configured origin until governance vets them and they seat an origin. For a truly-unclaimed hash (`namespaceId == 0`, which has no authorized origins) an operator nonetheless wants resident, the operator MUST explicitly pin (see [ADR 040 § Pinning, durable operator-evict, and the probe-hold stay engine-enforced](040-cache-policy.md#pinning-durable-operator-evict-and-the-probe-hold-stay-engine-enforced)).

#### No Discovery Fees

DHT STORE and FIND_VALUE operations carry no protocol-level fee. The incentive to publish STORE records is indirect: advertising that you hold a blob attracts probe traffic, which converts to paid delivery. Charging for DHT operations would create a new attack surface (collect fee, fail to hold) requiring a new slash condition. All fees remain on delivery.

### Interaction with Existing Protocols

| Mechanism | Interaction with DHT |
|-----------|---------------------|
| `cdn/probe/v1` | Unchanged. DHT provides candidates; `cdn/probe/v1` confirms live availability and measures latency. Probe cache (15s TTL, [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)) still prevents redundant probes for recently confirmed providers. |
| `cdn/client/v1` | Unchanged. All delivery is paid; DHT affects only how providers are discovered. |
| Proxy warming ([ADR 037](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality)) | Reactive window-paced pull-through warms a regional copy on real paid demand. STORE-on-commit makes a completed warmed copy discoverable through the normal FIND_VALUE path. |
| Reputation system ([ADR 008](008-reputation.md#adr-008-reputation-system)) | A node publishing a false STORE record fails at probe time → reputation penalty → fewer pulling nodes select it. No new slash condition needed. |
| Eviction hold ([ADR 005](005-protocol.md#adr-005-wire-protocol)) | Nodes stop re-publishing DHT records when a blob is evicted. TTL ensures stale records expire within 1 hour. |
| Client discovery ([ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model)) | Clients do not query the DHT. A client picks a node from its peer store or the on-chain registry by measured RTT ([ADR 039 § Source set and selection](039-multi-source-parallel-fetch.md#source-set-and-selection)); the node it picks resolves holders through FIND_VALUE and the origin-directory fallback on its own pull leg. |

### Schema Evolution

`cdn/dht/v1` follows the standard evolution model from [ADR 013](013-schema-evolution.md#adr-013-schema-evolution):

- **Tier 1 — Minor** (new optional trailing fields on existing message types): same ALPN.
- **Tier 2 — Medium** (new optional `DhtMessage` enum variants): same ALPN.
- **Tier 3 — Major** (new mandatory fields or other incompatible changes): new ALPN. The focused ADR for the concrete breaking change defines its migration guarantees.

`DhtMessage` uses a top-level enum consistent with the per-ALPN protocol enum pattern in [ADR 013 — Protocol Enums](013-schema-evolution.md#protocol-enums). `cdn/dht/v1` is a QUIC stream protocol, so a receiver that does not know a variant closes that individual stream with `UNSUPPORTED_MESSAGE` (`0x01`) per [ADR 013 § Unknown variant handling](013-schema-evolution.md#unknown-variant-handling).

A future optional variant is a Tier 2 change — no ALPN bump — when it is a pure optimization with an existing equivalent the publisher can fall back to: the publisher detects receiver support by issuing the new variant and falling back on the `UNSUPPORTED_MESSAGE` stream close. A variant without a fallback path requires an ALPN bump. `BatchStore` / `BatchStoreAck` are not such a case — they ship in `cdn/dht/v1` from the first deployment (every node implements them; see [§ STORE Flow (Cache Event → DHT Publish)](#store-flow-cache-event--dht-publish)), so no publisher ever negotiates their support.

The `coverage` field on `Provider` / `StoreRequest` / `BatchStoreRequest` likewise ships in `cdn/dht/v1` from the first deployment; every node reads and writes it, so it is not negotiated. On the probe side the coverage bitmap is an unsigned trailing field on the probe extension, a Tier 1 addition ([ADR 013 § Tier 1](013-schema-evolution.md#adr-013-schema-evolution)): the 64 MiB discovery-block granularity is a tunable knob, freely changed without an ALPN bump because no signed field or slashing typehash reads it.

### Acceptance Criteria

1. A node in a 30-node network can discover providers for a cached blob in ≤3 FIND_VALUE hops.
2. A node in a 500-node network can discover providers in ≤5 FIND_VALUE hops.
3. A cache event (blob added) generates ≤(K+3) (=23) outgoing STORE messages, not O(N).
4. A stale STORE record (node evicted the blob) expires within TTL (1 hour) with no explicit retraction.
5. A false STORE record (node claims to hold a blob it doesn't) fails at the probe step; the publishing node incurs a reputation penalty. A record's `coverage` is likewise unverified at STORE time — an inflated bitmap wastes one prober's probe against an unserved range and is a reputation matter only, never a slashable offense.
6. During bootstrap (routing table < k entries), the on-chain origin directory provides the fallback.
7. Content propagation is driven entirely by realized paid demand — reactive cache-miss pull-through per [ADR 037](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality) plus explicit operator pinning. No FIND_VALUE-query-frequency or per-hash cache-miss-frequency signal is collected or emitted to drive speculative acquisition.
8. An accepted `StoreRequest`'s record TTL is anchored on the receiver's wall-clock at acceptance time (`expiry_us = receive_us + record_ttl_us`), independent of any holder-supplied timestamp.
9. A receiver enforces both a per-publisher record cap (hard reject with `StoreAck { accepted: false }` when at cap) and a per-node global cap (global LRU eviction when at cap and the inserting publisher is below its per-publisher cap). No publisher can force eviction of another publisher's records by exceeding its own cap.
10. `cdn/dht/v1` inbound traffic is bounded by global, per-IP, and per-peer token buckets; rejected requests close the stream with `RATE_LIMITED` and are counted in the `decdn_dht_rate_limit_rejected_{per_peer,per_ip,global}_total` sibling counters.
11. A `FindValueResponse` containing `closer_nodes` entries whose XOR distance is not strictly less than the responder's own has those entries dropped by the requester; honest entries from the same response are retained.
12. The requester randomizes the surviving provider set before issuing `cdn/probe/v1` requests; probe order is statistically independent of `FindValueResponse.providers` order.
13. NodeIds returning `has_blob: false` for hash H are not re-probed for H within the negative probe cache TTL ([ADR 001 § Probe cache](001-network.md#probe-cache)).
14. Re-publish time per record is drawn from `uniform(30 min, 50 min)` independently per draw; STORE targets are the K+3 closest nodes in the publisher's routing table.
15. On cold start, each cached blob's first re-publish time is drawn from `uniform(0, 40 min)` independently per record. A naive single-tick bulk re-publish across all cached blobs at startup is non-conforming.
16. A `BatchStoreRequest` with `n ≤ 256` hashes from publisher P produces a `BatchStoreAck` whose `results` carries one `bool` per request hash, in request order. The batch-level `holder` is checked once against the authenticated QUIC NodeId; the remaining admission decisions (rate limit, per-publisher quota with two-tier eviction, active-bonded-operator check) are applied per-hash and match those of `n` separate `StoreRequest` RPCs from P.
17. A `BatchStoreRequest` consumes between 1 and `n` tokens from each rate-limit bucket: stage 1 charges 1 token before deserialization; stage 2 charges up to `n - 1` more after reading `n = entries.len()`, bounded by remaining budget. The first `k` admitted entries pass per-entry processing; the remaining `n - k` are marked `false` in the ack without further processing.
18. A `BatchStoreRequest` with `entries.len() > 256` is rejected by closing the stream with `MALFORMED_MESSAGE` (`0x03`); the publisher MUST split larger publish sets across multiple batches.
19. The re-publish scheduler groups the hashes due in a drain cycle by receiver (each hash's K+3 closest nodes) and sends each receiver its due set as one or more `BatchStoreRequest`s, split at the 256-hash cap. `BatchStore` / `BatchStoreAck` are part of `cdn/dht/v1`; every node implements them, so there is no per-hash fallback path.
20. A publisher that drops cache-commit events re-derives the set of hashes it advertises and seeds each unscheduled hash with an independent `uniform(0, 40 min)` draw. A hash already scheduled keeps its existing due time and gets no second entry. A drop observed after a re-derivation started causes a further re-derivation. An immediate bulk re-publish on this path is non-conforming.
21. A node publishes a STORE for `H` once it holds ≥1 verified 64 MiB block, not only on `Complete`. The `StoreRequest` / `Provider` / `BatchStoreRequest` carries a `coverage` bitmap at 64 MiB block granularity, and `FindValueResponse` returns `Provider { node, coverage }` per holder. The `FindValueRequest` carries no range.
22. A `cdn/probe/v1` response carries the holder's live coverage bitmap as an unsigned field; the signed set `{hash, has_blob, rate_per_mb, timestamp_us}` is unchanged, and `has_blob` is true iff the node will serve ≥1 full 64 MiB block of `H` — a cached verified block, or any block when the node is origin-serve-capable (an origin sets every bit from the size it already read) — equivalently, its coverage bitmap is non-empty. No contract change is required.
23. A probe response whose `has_blob` and coverage bitmap disagree — `has_blob: false` with a non-empty bitmap, or `has_blob: true` with an empty one — is malformed: the requester rejects it and scores a protocol violation. A holder cannot earn by advertising coverage while signing `has_blob: false`.
24. A partial holder serves any 64 MiB block it has verified over `cdn/client/v1`, and a request for a block it lacks falls to the warm-on-miss origin chain — the serve path is no longer gated on `Complete`.
25. A cache-missing node composes `H` by assigning disjoint blocks across covering partial holders (the [ADR 039](039-multi-source-parallel-fetch.md#adr-039-multi-source-parallel-fetch-scheduling-on-cdnclientv1) scheduler on its pull leg), demand-windowed to the serve frontier, warming any uncovered block once from origin; it becomes a full holder only when demand consumes the whole blob.
