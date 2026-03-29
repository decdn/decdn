# Separated Node Discovery and Content Discovery

**Date:** 2026-03-30
**Status:** Draft
**Issue:** [#25](https://github.com/thiras/decdn/issues/25)

---

## Problem

ADR 001 conflates node discovery ("what nodes exist?") and content discovery ("who has blob X?") into a single gossip mechanism: `CacheAnnounce` messages that broadcast blob hash lists or Bloom filters. This creates several problems identified in issue #25:

- Bloom filter parameters are unspecified (expected items, false positive rate, max size).
- A Bloom filter for 100,000 items at 1% FP ≈ 120 KB, exceeding iroh-gossip's ~64 KB message limit.
- Hash lists (500 × 32 bytes = 16 KB) are already large, and the switchover threshold to Bloom filters is undefined.
- Routing tables built from gossip are eventually consistent — stale entries cause wasted probes.

## Solution

Separate the two concerns completely:

| Concern | Mechanism | What it carries |
|---------|-----------|----------------|
| Node discovery | iroh-gossip on regional + global topics | Node presence, region, load, popularity hints |
| Content discovery | `cdn/probe/v1` fan-out | "Do you have hash X?" — on-demand, per-blob |
| Reputation | iroh-gossip on `reputation/v1` | Signed interaction reports (unchanged from ADR 008) |

`CacheAnnounce` is removed entirely. No hash lists, no Bloom filters, no local routing tables mapping `hash → Vec<NodeId>`. Issue #25 goes away completely.

---

## Design

### 1. Gossip: NodeAnnounce

Replaces `CacheAnnounce`. Carries node-level metadata only — no content inventory.

```rust
struct NodeAnnounce {
    node_id: NodeId,
    region: String,              // ISO 3166-1 alpha-2 (self-reported)
    load: LoadHint,              // approximate current utilization
    popular_hashes: Vec<Hash>,   // top-N most-requested hashes (max 20)
    timestamp: u64,              // microseconds since epoch
    signature: Signature,        // node's iroh key signs the message
}

struct LoadHint {
    active_streams: u32,         // current concurrent delivery streams
    bandwidth_utilization: u8,   // 0-100 percentage of self-reported capacity
}
```

- **`popular_hashes`** (capped at 20): popularity signal for prefetching, not an inventory. "These are hot right now."
- **Message size:** ~700 bytes worst case (20 hashes × 32 bytes + metadata). No Bloom filter sizing issues.
- **`LoadHint`:** makes the "approximate load in gossip announcements" from ADR 008 section 9 concrete.
- **Announce interval:** per-node config parameter (PoC default TBD during implementation).

**Gossip topics** (unchanged structure, repurposed content):
- `cdn/region/{cc}/v1` — regional `NodeAnnounce` messages
- `cdn/global/v1` — global `NodeAnnounce` messages
- `reputation/v1` — unchanged (ADR 008)

### 2. Content Discovery via Probe Fan-Out

When a node has a cache miss, it discovers providers by probing all known nodes using the existing `cdn/probe/v1` protocol. No protocol changes needed.

**Flow:**

1. **Probe cache check.** Look up `hash` in a short-lived LRU cache (`hash → Vec<(NodeId, rate_per_mb, rtt)>`, TTL 30 seconds). If a valid entry exists, skip to step 4.
2. **Fan-out.** Send `ProbeRequest {hash, timestamp_us}` in parallel to all known nodes (regional + global). The existing `cdn/probe/v1` protocol is unchanged — `ProbeResponse {has_blob, rate_per_mb, timestamp_us, signature}`.
3. **Collect.** Wait up to 200ms. Store all `has_blob: true` responses in the probe cache.
4. **Select.** Pick the best provider by `rate_per_mb × rtt_ms` (lowest wins). Open `cdn/client/v1` stream and pull.

```
Cache miss for hash X
    │
    ▼
Probe cache hit? ──yes──► Select best provider ──► Pull via cdn/client/v1
    │
    no
    │
    ▼
Fan-out ProbeRequest to all known nodes
    │
    ▼
Wait up to 200ms, collect has_blob:true responses
    │
    ▼
Store in probe cache (30s TTL)
    │
    ▼
Select best provider ──► Pull via cdn/client/v1
```

**Probe cache:**
- LRU map, bounded at 1024 entries.
- TTL 30 seconds — short enough that evictions and rate changes don't cause persistent staleness.
- On probe cache hit, if the selected provider no longer has the blob (evicted since the cached probe), the node falls back to a fresh fan-out.

**No change to `cdn/probe/v1` itself.** The protocol already carries `has_blob` and `rate_per_mb`. The only change is the usage pattern: instead of probing a few candidates from a routing table, nodes probe all known peers.

### 3. Prefetching with Dual Signals

Two complementary signals drive proactive caching:

**Local demand signal:**
- Each node tracks cache miss frequency per hash in a bounded counter map (`HashMap<Hash, (count, last_seen)>`, max 10,000 entries, LRU eviction).
- When a hash crosses a configurable threshold (default: 3 misses in 5 minutes), the node proactively pulls the blob via the same probe fan-out → `cdn/client/v1` path.
- Local decision — no coordination needed.

**Network popularity signal:**
- Nodes observe which hashes appear in `popular_hashes` across multiple `NodeAnnounce` messages from different peers.
- A hash appearing in N peers' top-20 lists suggests cross-region demand. The node can prefetch before any local client requests it.
- Threshold is configurable (default: seen in 3+ peers' popular lists within 10 minutes).

Both signals feed the same action: probe fan-out → select provider → pull via `cdn/client/v1` (paid). No special prefetch protocol.

**PoC simplification:** Both signals are implemented. Network popularity thresholds are conservative (high) — operators tune down as they observe real traffic patterns.

---

## Impact on Existing ADRs

### ADR 001 (Network Topology)

- Remove `CacheAnnounce` message and local routing table (`hash → Vec<NodeId>`).
- Remove all Bloom filter discussion.
- Replace with `NodeAnnounce` message definition.
- Gossip topics keep regional structure but carry node metadata only.
- Cache miss resolution changes from "query routing table → probe candidates" to "check probe cache → fan-out probe → select."
- "Future Work: Content-Addressed DHT" reframed: DHT becomes relevant for reducing probe fan-out at scale (targeted lookups instead of broadcast probes), not for replacing gossip content discovery.

### ADR 005 (Wire Protocol)

- Gossip section updated: `CacheAnnounce` replaced with `NodeAnnounce`.
- `cdn/probe/v1` section unchanged (protocol is the same, usage pattern changes).
- No new ALPNs needed.

### ADR 008 (Reputation)

- Unchanged. Reputation still uses its own gossip topic and the same scoring model.
- `LoadHint` in `NodeAnnounce` makes "approximate load in gossip announcements" concrete (ADR 008 section 9 references this for tie-breaking).

### Architecture overview (`architecture.md`)

- System diagram: gossip arrows labeled `NodeAnnounce` instead of `CacheAnnounce`.
- Cache miss flowchart updated to show probe fan-out path.
- Prefetching section updated with dual-signal model.

---

## Trade-offs

### Positive

- Eliminates issue #25 entirely — no Bloom filters, no hash list size limits, no switchover threshold.
- No routing tables to maintain — no stale entries, no eventual consistency concerns for content state.
- Reuses existing `cdn/probe/v1` protocol — minimal new code.
- Gossip messages drop from up to 64 KB to ~700 bytes.
- Node discovery and content discovery evolve independently.
- Probe cache prevents redundant fan-outs for popular content.

### Negative

- More probe traffic per cache miss: O(N) probes instead of a routing table lookup. At PoC scale (tens of nodes) this is negligible; at production scale, probe fan-out must be bounded (DHT or selective fan-out).
- Adds ~200ms latency on a cold cache miss (probe timeout) compared to an instant routing table lookup. Mitigated by probe cache for repeated lookups.
- `popular_hashes` in `NodeAnnounce` reveals demand patterns. Same privacy profile as the current `CacheAnnounce` (content availability is already public), but more explicit about popularity.
- Probe cache introduces a brief staleness window (up to 30s) where a node may attempt to pull from a provider that has evicted the blob.

---

## Future Scaling

At production scale (hundreds+ nodes), broadcast probe fan-out becomes expensive. The migration path:

1. **Selective fan-out:** Probe only regional peers + a random subset of global peers. Reduces probe count while maintaining discovery probability.
2. **Content-addressed DHT:** A Kademlia overlay publishing `(hash → Vec<NodeId>)` records. Targeted O(log N) lookups replace O(N) fan-out. The probe protocol remains unchanged — DHT narrows the candidate set, probes confirm and measure.
3. **Gossip-based content hints:** Nodes that frequently serve certain content can advertise content "categories" or prefix ranges in `NodeAnnounce`, enabling smarter probe targeting without a full DHT.

These are additive — the probe-based content discovery mechanism doesn't change, only the strategy for selecting who to probe.
