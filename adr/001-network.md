# ADR 001: Network Topology and Peer Mesh

**Date:** 2026-03-28
**Status:** Draft

## Context

The CDN has two participant roles. **Nodes** (providers) cache and serve content close to clients — some have an origin backend (S3, NFS, local disk) making them the canonical source for specific content, others are pure caches. **Clients** consume content. No external origin URL exists; the network is fully self-contained.

Two questions are in scope:

1. How do nodes discover each other and learn what content each holds?
2. How does a node resolve a cache miss?

## Decision

All staked nodes form a flat peer mesh with no fixed routing hierarchy. Node discovery is gossip-based; content discovery uses `cdn/dht/v1` (a lightweight Kademlia subset — see [ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale)), with the on-chain origin directory as the deterministic fallback when the DHT returns no providers:

```mermaid
graph TD
    subgraph Topics["iroh-gossip Topics"]
        GLOBAL["cdn/global/v1"]
        REG_US["cdn/region/US/v1"]
        REG_DE["cdn/region/DE/v1"]
        REG_ETC["cdn/region/.../v1"]
    end

    NA["NodeAnnounce<br/>{region, load}"]

    NA -->|all staked nodes publish| GLOBAL
    NA -->|regional nodes publish| REG_US
    NA -->|regional nodes publish| REG_DE
    NA -->|regional nodes publish| REG_ETC

    GLOBAL --> PT["Peer Table<br/>NodeId -> NodeAnnounce"]
    REG_US --> PT
    REG_DE --> PT
    REG_ETC --> PT

    PT -->|cache miss| DHT["cdn/dht/v1<br/>FIND_VALUE for hash"]
    DHT -->|providers found| PROBE["cdn/probe/v1<br/>targeted probe"]
    DHT -->|no providers| DIR["On-chain origin directory<br/>(ADR 022 last-resort fallback)"]
    PROBE -->|has_blob: true| SELECT["Select best by unified selection score"]
    PROBE -->|no provider found| DIR
    DIR -->|provider found| PROBE
    DIR -->|no provider found| MISS["No Known Provider<br/>(serve from local origin if configured,<br/>otherwise reject)"]
```

### Node Discovery (Gossip)

Nodes broadcast lightweight metadata over iroh-gossip on regional topics (`cdn/region/{cc}/v1`) and a global topic (`cdn/global/v1`). Each node publishes `NodeAnnounce` messages:

```rust
struct NodeAnnounce {
    node_id: NodeId,
    region: String,              // ISO 3166-1 alpha-2 (self-reported)
    load: LoadHint,              // approximate current utilization
    timestamp_us: u64,           // microseconds since epoch
    signature: Signature,        // node's iroh key signs all fields above
}

struct LoadHint {
    active_streams: u32,         // current concurrent delivery streams
    bandwidth_utilization: u8,   // 0-100 percentage of self-reported capacity
}
```

#### Schema evolution note

The struct above is a flat definition for readability. [ADR 013](013-schema-evolution.md#adr-013-schema-evolution) specifies that `NodeAnnounce` uses a `NodeAnnounceBody` (signed portion) + `signature` + optional extensions pattern with two-phase deserialization, enabling unsigned fields to be appended via minor evolution without an ALPN bump. See [ADR 013 — Signed Field Freezing](013-schema-evolution.md#signed-field-freezing) for the canonical struct layout.

`LoadHint` is advisory and untrusted. The reputation system ([ADR 008](008-reputation.md#adr-008-reputation-system)) penalizes nodes whose observed delivery performance contradicts their advertised load.

- **`NodeAnnounce` carries node-level metadata only** — no content inventory and no demand signals. Content discovery and demand are derived from DHT FIND_VALUE traffic and local cache-miss timestamps (see Prefetch Triggers below). Message size is ~150 bytes.
- **`LoadHint`** makes the "approximate load in gossip announcements" from [ADR 008, Tie-Breaking](008-reputation.md#tie-breaking) concrete, feeding tie-breaking logic.
- **Announce interval** is a per-node configuration parameter (default 60 seconds). This interval directly governs gossip bandwidth — see [Gossip Bandwidth Analysis](#gossip-bandwidth-analysis) below.

Both clients and nodes maintain a **peer table** (`NodeId → NodeAnnounce`) built from received gossip messages. This table tracks which nodes exist and their metadata — it does not track content. Peer-table lifecycle (TTL, registry-driven eviction, reputation independence) is documented in [appendix-peer-table-eviction.md](appendix-peer-table-eviction.md#appendix-peer-table-eviction-policy).

#### Registry cache

Nodes maintain a local cache of the on-chain registry, kept fresh by subscribing to `NodeRegistered`, `NodeDeregistered`, and `NodeAutoEjected` events; sub-second L2 block times keep the staleness window small. The cache is checked during gossip validation and before initiating paid pulls (see Content Discovery step 5). On `NodeDeregistered` and `NodeAutoEjected`, the cache also removes the corresponding peer-table entry — see [appendix-peer-table-eviction.md](appendix-peer-table-eviction.md#appendix-peer-table-eviction-policy).

#### Gossip validation

Gossip messages arrive wrapped in a `GossipEnvelope` ([ADR 013](013-schema-evolution.md#adr-013-schema-evolution)). The receiver deserializes the envelope first; messages with unknown envelope versions or unknown payload variants are silently dropped. The rules below apply to the inner payload after unwrapping. Before accepting a `NodeAnnounce` and updating the peer table, a node verifies: (1) the `signature` is valid for the `node_id`'s public key over the signed body fields (serialized via postcard, consistent with [ADR 005](005-protocol.md#adr-005-wire-protocol) and [ADR 013](013-schema-evolution.md#adr-013-schema-evolution)); (2) the `node_id` corresponds to an active staked node in the on-chain registry (checked against a local registry cache); (3) `timestamp_us` is within ±60 seconds of the receiver's local clock to prevent replay of old messages; (4) `timestamp_us` is strictly greater than the `timestamp_us` of the existing peer table entry for the same `node_id` (monotonic — prevents replay of older messages within the freshness window). Messages failing any check are silently dropped. Additionally: (5) `region` is exactly 2 ASCII uppercase letters matching a known ISO 3166-1 alpha-2 code set. Messages with invalid region values are dropped. This prevents unregistered, unstaked, or replayed nodes from appearing in or corrupting peer tables.

**Gossip deduplication:** iroh-gossip uses PlumTree (epidemic broadcast trees), which performs message-level deduplication internally — each message is assigned a unique identifier and nodes track a bounded in-memory set of seen message IDs, so the same message arriving via multiple paths is delivered to the application at most once while its ID remains in that seen-set. This is not a global or persistent exactly-once guarantee: duplicates may be re-delivered after seen-set eviction or process restart. This transport-layer dedup is the primary mechanism preventing redundant processing of `NodeAnnounce` in a multi-path topology. As defense-in-depth, gossip validation rule (monotonic `timestamp_us` per `node_id`) independently rejects any duplicate or older `NodeAnnounce` — even if transport-level dedup were bypassed (e.g., after a restart), a replayed message fails the strictly-greater timestamp check against the peer table. The peer table itself (`NodeId → NodeAnnounce`), keyed by `node_id` with only the latest timestamp retained, is inherently convergent regardless of delivery order or multiplicity. No application-level seen-message set or content-hash table is required at the gossip layer.

#### Clock synchronization

The ±60-second freshness check in gossip validation is evaluated against the receiver's local clock. A process whose wall-clock offset exceeds 60 seconds relative to well-synchronized peers will both (a) have its own `NodeAnnounce` messages silently rejected by those peers and (b) silently reject otherwise-valid `NodeAnnounce` from correctly synchronized peers — in either case making peers invisible in the local mesh view, with no error feedback. All processes that perform gossip validation and maintain a peer table (staked nodes and any validating clients) MUST run NTP (or an equivalent time-synchronization service) to maintain wall-clock accuracy well within this 60-second window. At startup, such a process SHOULD query an NTP server and log a warning if the measured offset exceeds 10 seconds.

**Observability:** Nodes SHOULD expose a `gossip_messages_rejected_clock_skew` counter (Prometheus metric). Additionally, a node SHOULD periodically compare its own `NodeAnnounce` timestamp against timestamps in received `NodeAnnounce` messages from peers to detect relative drift. If median peer timestamps diverge from the local clock by more than 30 seconds, the node logs a warning.

### Content Discovery (DHT + Probe)

Content discovery uses `cdn/dht/v1` as the primary mechanism. The full iterative `FindValueRequest` flow, origin-directory fallback, and bootstrap behavior are specified in [ADR 022 § FIND_VALUE Flow (Cache Miss → DHT Lookup)](022-content-discovery.md#find_value-flow-cache-miss--dht-lookup). `cdn/probe/v1` runs **after** the DHT lookup to confirm live availability and measure latency before any paid pull. This ADR owns three pieces layered on top of that flow: the probe cache, the probe-response collection window, and probe-rate limiting.

#### Probe cache

A short-lived LRU cache holds `hash → Vec<(NodeId, rate_per_mb, rtt, ProbeResponse)>` entries, TTL 15 seconds, max 1024 entries. Each hash entry retains at most 10 responses (top 10 by selection score); each entry retains the full signed `ProbeResponse` for slashing evidence. Approximate memory: 1,024 × 10 × ~200 bytes ≈ 2 MB. On a cache miss the requester checks the probe cache first; if a valid entry exists, it skips DHT lookup and goes straight to selection. On probe cache hit, if the selected provider no longer has the blob (evicted — rare with eviction holds), try the next-best cached provider; if all fail, run a fresh DHT lookup + probe. **Observability:** track `EvictedSinceProbe` response rate; sustained >1% may indicate eviction hold failures ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)).

**Probe cache TTL is 15 seconds** — half the 30-second slashing evidence window from [ADR 005](005-protocol.md#adr-005-wire-protocol). Probe cache TTL (15s) < `probe_hold_duration` (35s), so any cached probe response used for a stream is within both the slashing window and the eviction hold period.

#### Probe response collection

After issuing the DHT FIND_VALUE + parallel `ProbeRequest` fan-out (per [ADR 022](022-content-discovery.md#find_value-flow-cache-miss--dht-lookup)), wait for probe responses in two phases:

- **Phase 1 — Minimum wait** (`probe_min_wait`, default 50ms): Always wait this long to let multiple candidates respond.
- **Phase 2 — Extended wait with optional early exit** (`probe_max_wait`, default 500ms): Exit early when **both** conditions are met: (a) at least `min_probe_responses` (default: 3) `has_blob: true` responses, and (b) best selection score is below `early_exit_score_threshold` (default: `1.5 × rolling_median_score`). Rolling median from last 100 successful pull scores, seeded with `0` (early exit disabled until history exists). If not met, wait up to 500ms. The 500ms ceiling accommodates inter-continental RTTs (e.g., London↔Sydney ~250–300ms).

Store all `has_blob: true` responses in the probe cache, then select the best provider using the unified [node selection algorithm](#node-selection-algorithm). Before opening `cdn/client/v1`, verify the selected `node_id` is still active in the local registry cache; if not, skip to the next-best provider.

**Early-exit rationale:** For popular content on nearby nodes, early exit reduces P50 cache-miss latency from ~500ms to ~50–100ms; distant but cheaper or more reputable nodes still win when nearby responses score poorly. All four parameters operator-configurable; setting `probe_min_wait = probe_max_wait` disables early exit.

#### Probe rate limits

- **Outbound:** 3 probe batches per second per node (each batch targets the DHT-returned candidate set, typically 3–5 NodeIds). Excess cache misses queue.
- **Inbound:** 5 probe requests per peer per second (token bucket). Excess probes silently dropped.

### Gossip Bandwidth Analysis

All gossip bandwidth scales O(N²) across the network (each of N nodes publishes to N−1 receivers); per-node cost scales O(N), linear in network size. The `NodeAnnounce` interval is the dominant variable.

**Assumptions:** PlumTree delivers each unique message to each subscriber once. `NodeAnnounce` worst case is 800 bytes. `ReputationReport` is ~200 bytes (2×NodeId + ReportMetrics + timestamp + signature + framing). Egress ≈ ingress (PlumTree tree-forwarding).

#### NodeAnnounce (`cdn/global/v1`, 60-second interval)

Per-node ingress: `(N−1) × 800 bytes × (3600 / interval_s)` per hour.

| Nodes | Messages/node/hr | Ingress/node/hr | Sustained rate |
| --- | --- | --- | --- |
| 30 | 1,740 | ~1.4 MB | ~3 Kbps |
| 100 | 5,940 | ~4.8 MB | ~11 Kbps |
| 500 | 29,940 | ~24.0 MB | ~53 Kbps |
| 1,000 | 59,940 | ~48.0 MB | ~107 Kbps |

Regional topics (`cdn/region/{cc}/v1`) add per-region bandwidth but do not reduce global topic traffic — all staked nodes publish to and subscribe to `cdn/global/v1`.

#### ReputationReport (`cdn/reputation/v1`, production only)

Per [ADR 008](008-reputation.md#adr-008-reputation-system) rate limits: max 10 reports per reporter per hour, max 1 per (reporter, target) pair per hour. Worst case: all N nodes send 10 reports/hr, each delivered to N−1 subscribers.

| Nodes | Reports received/node/hr | Ingress/node/hr |
| --- | --- | --- |
| 100 | ~1,000 | ~0.2 MB |
| 500 | ~5,000 | ~1.0 MB |
| 1,000 | ~10,000 | ~2.0 MB |

The strict rate limits (Section 11 of [ADR 008](008-reputation.md#adr-008-reputation-system)) keep reputation gossip modest vs. `NodeAnnounce`.

#### Combined per-node budget (60-second announce interval)

| Nodes | NodeAnnounce | ReputationReport | Combined/node/hr | Sustained rate | ed25519 verify/s |
| --- | --- | --- | --- | --- | --- |
| 30 | ~1.4 MB | ~0.0 MB | ~1.4 MB | ~3 Kbps | <1 |
| 100 | ~4.8 MB | ~0.2 MB | ~5.0 MB | ~11 Kbps | ~2 |
| 500 | ~24.0 MB | ~1.0 MB | ~25.0 MB | ~56 Kbps | ~10 |
| 1,000 | ~48.0 MB | ~2.0 MB | ~50.0 MB | ~112 Kbps | ~19 |

**CPU cost:** Modern hardware handles ~50,000–100,000 ed25519 verifications/sec/core. At 1,000 nodes, ~19 verify/sec is negligible — CPU is not the gossip bottleneck.

#### Scale thresholds

| Scale | Gossip overhead | Recommended action |
| --- | --- | --- |
| ≤200 nodes | <10 MB/node/hr (~22 Kbps) | No action needed |
| 200–500 nodes | ~25 MB/node/hr (~56 Kbps) | Monitor bandwidth metrics; consider increasing interval to 120s if constrained |
| 500–1,000 nodes | ~50 MB/node/hr (~111 Kbps) | Evaluate selective gossip (regional-only subscription for non-global nodes) |
| >1,000 nodes | Scales linearly (~50 KB/node/hr per additional node) | Structured overlay (DHT) or gossip partitioning required — see [ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale) |

### Node Selection Algorithm

The unified selection score combines price, latency, and reputation into a single comparable value:

```
selection_score = rate_per_mb × rtt_ms × (1 / max(reputation, 0.1)²)
```

Lower is better. Reputation is clamped to a minimum of 0.1 to prevent division by zero ([ADR 008](008-reputation.md#adr-008-reputation-system) allows a floor of 0.0, but a node at 0.0 reputation is effectively unusable). The `reputation²` term amplifies reputation: a node at 0.5 (neutral) is 4× more expensive in score terms than a node at 1.0 (perfect):

| Reputation | Score multiplier (vs. rep=1.0) |
| --- | --- |
| 1.0 | 1.0× |
| 0.8 | 1.56× |
| 0.5 | 4.0× |
| 0.3 | 11.1× |
| 0.1 | 100× |

For new nodes with the initial reputation of 0.5 ([ADR 008](008-reputation.md#adr-008-reputation-system)), the 4× multiplier means they must be ~4× cheaper or faster to compete with established nodes — a bootstrap barrier softened by the cold-start bonus in [ADR 008](008-reputation.md#adr-008-reputation-system).

**Inputs:** `rate_per_mb` and `rtt_ms` come from `ProbeResponse` (see [ADR 005](005-protocol.md#adr-005-wire-protocol)). `reputation` is the node's `final_score` from [ADR 008 § Combined Score](008-reputation.md#combined-score).

#### Minimum-reputation rejection floor

The `max(reputation, 0.1)` clamp above only bounds the *score denominator* — it caps the worst-case multiplier at 100×, but a sufficiently cheap and close node can still produce the lowest score and win selection despite a poor reputation. Independently of that clamp, a client MAY configure a hard **minimum-reputation floor**: candidates whose `reputation` is below the floor are removed from the candidate pool *before* scoring, so price and RTT can never override a sub-floor reputation. The boundary is inclusive (a node exactly at the floor is retained); a `NaN` reputation is rejected whenever the floor is active (any comparison with `NaN` is false).

The floor defaults to `0.0`, which disables filtering and preserves the pre-floor ranking behavior exactly (including the defensive negative-reputation clamp). Per-client configuration wiring is deferred until the client fetch path that consumes the selection algorithm exists; the floor is currently exposed as a selection-API parameter.

#### Tie-breaking

(scores within 1% of each other): see [ADR 008, Tie-Breaking](008-reputation.md#tie-breaking).

This score is used in Content Discovery step 4 above and in all other node selection contexts. The simpler `rate_per_mb × rtt_ms` product is the price×latency component; the full selection algorithm adds reputation weighting as shown above.

### On-chain Registration

Node identity is the iroh `NodeId` (ed25519 public key). All staked nodes register in `StakingRegistry` — the on-chain contract holding `NodeId → (Ethereum address, multiaddrs, region, active flag)`. The contract surface (`NodeInfo` struct, `registerNode` with atomic NodeId↔Ethereum binding and ed25519 ownership proof, `updateMultiaddrs`, `deregisterNode`, `reclaimNodeId`, events, gas costs) is specified in [ADR 003 § Node Registry](003-payments.md#node-registry). Nodes and clients bootstrap their peer table from this registry on startup (see [ADR 019 § Step 3.3](019-node-onboarding.md#step-33--build-initial-peer-table-from-on-chain-registry) and [ADR 012 § Bootstrap Procedure](012-client.md#bootstrap-procedure)).

## Consequences

### Positive

- No external infrastructure is reachable — origin-backed nodes completely hide their backends, so no client or node can bypass the payment layer via a direct storage URL
- All nodes share the same discovery and transport protocols. The cache-only role remains permissionless — any staked operator may pull cached blobs from authorized origins and re-serve them. The origin role is DAO-governed: registered namespaces have publisher-proposed `OriginAssignment` operator sets, and a global allow-list covers unregistered content. See [ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority) and [ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces)
- Gossip messages are lightweight (~800 bytes) — no content inventories, Bloom filters, or hash lists. At the PoC default 60-second interval, per-node bandwidth is ~3 Kbps at 30 nodes, scaling linearly to ~111 Kbps at 1,000 nodes (see [Gossip Bandwidth Analysis](#gossip-bandwidth-analysis)). Regional topics speed regional delivery but do not reduce global topic bandwidth
- Content discovery via `cdn/dht/v1` provides targeted O(log N) provider lookup; probe confirms live availability. No stale content inventory to maintain — stale DHT records self-expire within TTL (1 hour)
- Probe cache prevents redundant probe batches for popular content within a 15-second window
- Once a node in a region caches a blob, other regional nodes pull from it at competitive rates rather than origin-backed prices — popular content gets cheaper as it spreads
- The flat mesh is simple to reason about and easy to test at small scale (PoC is tens of nodes)
- NodeId squatting is prevented by on-chain ed25519 ownership proof — an attacker cannot register a NodeId they do not control, and a legitimate owner can reclaim a squatted NodeId

### Negative

- Cold cache miss adds up to 500ms latency (probe maximum wait) vs. a pre-built content index lookup; mitigated by probe cache for repeated lookups within 15 seconds and by adaptive early exit (see Collect step above), which reduces P50 latency to ~50-100ms once the node has sufficient score history
- Probe cache introduces a brief staleness window (up to 15s) where a node may pull from a provider that has evicted the blob; mitigated by the probe-triggered eviction hold ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)), with fallback to the next cached provider, then a fresh DHT lookup + probe
- Self-reported region hints (ISO 3166-1 alpha-2) are unverified; a node could misreport its region to appear in more gossip topics. Mitigation: clients apply a reputation penalty when observed latency contradicts the claimed region (e.g., RTT > 150ms to a node in the same claimed region). Cryptographic hardening via an IP-geolocation oracle or third-party attestation was considered and rejected — see [ADR 030](030-node-region-self-attestation.md#adr-030-node-region-self-attestation); the latency-based signal is the canonical mitigation.
- Every transfer is paid, so nodes pulling on cache miss incur a cost recouped through subsequent client deliveries — a natural economic barrier to speculative caching
- Origin-backed nodes are the last line of defense for availability — if all authorized origins for a blob go offline or are deregistered, the content becomes permanently unavailable (unless cached elsewhere). The protocol does not guarantee a redundancy floor: publishers choose how many operators to commit per namespace via `OriginAssignment` ([ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)), and governance does the same for default-open content via the allow-list ([ADR 011 § Default-open allow-list](011-content-takedown.md#default-open-allow-list)).
- `registerNode` gas cost increases ~4–7× due to on-chain ed25519 signature verification (~650k–1.15M gas vs. ~150k without); acceptable as a one-time cost per node lifetime
