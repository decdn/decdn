# ADR 017: Privacy Analysis

**Date:** 2026-04-04
**Status:** Draft

## Context

The protocol makes several deliberate privacy tradeoffs favoring decentralization and accountability over confidentiality. These decisions are scattered across [ADR 001](001-network.md), [ADR 002](002-content-addressing.md), [ADR 003](003-payments.md), [ADR 005](005-protocol.md), [ADR 006](006-e2e-encryption.md), [ADR 007](007-watchtower.md), [ADR 008](008-reputation.md), [ADR 012](012-client.md), and [architecture.md](architecture.md). No single document maps the full privacy surface, making it difficult to reason about the cumulative exposure or prioritize mitigations.

This ADR consolidates that analysis into a single reference. It does not introduce new functionality — it systematizes privacy properties that other ADRs already specify, assigns an explicit disposition to each, and prioritizes mitigations for PoC versus production.

**Scope boundary:** This ADR covers protocol-level privacy — data observable through participation in the CDN protocol, on-chain interactions, and gossip. Application-level privacy (what a content provider's app server does with subscriber data) is out of scope; it is the content provider's responsibility, as noted in [ADR 006](006-e2e-encryption.md).

## Decision

### 1. Adversary Model

The analysis is organized by adversary capability. Each tier subsumes the capabilities of the tier below it.

| Tier | Adversary | Capabilities | Real-World Examples |
|------|-----------|-------------|---------------------|
| T1 | Passive network observer | Observes QUIC connection metadata (IP pairs, timing, volume), public gossip messages, on-chain transactions and events | ISP, nation-state passive surveillance, blockchain analytics firm |
| T2 | Active protocol participant | All of T1 plus: operates staked nodes, sends probes, opens payment channels, subscribes to gossip topics, observes probe responses | Competing CDN, curious node operator, researcher |
| T3 | Infrastructure operator | All of T2 plus: operates an RPC endpoint, iroh relay, or watchtower | RPC provider (Alchemy, Infura), relay operator, watchtower service |
| T4 | Compromised endpoint | Has memory or disk access to a specific client or node | Device theft, malware, law enforcement with warrant |

### 2. Privacy Surface Inventory

Each row identifies a discrete data exposure. The **ID** column is used for back-reference in the analysis and disposition sections.

| ID | Surface | Data Exposed | Min. Tier | Source ADR |
|----|---------|-------------|-----------|------------|
| P-01 | `popular_hashes` in `NodeAnnounce` | Top-20 most-requested content hashes per node, broadcast to all peers | T1 | [001](001-network.md) §NodeAnnounce |
| P-02 | On-chain payment channels | Channel IDs, client/provider Ethereum addresses, deposit amounts, settlement events | T1 | [003](003-payments.md) |
| P-03 | On-chain staking registry | `nodeId`, `ethAddress`, `multiaddrs`, `regionHint`, registration timestamps | T1 | [001](001-network.md), [architecture.md](architecture.md) |
| P-04 | ALPN protocol identification | QUIC TLS ClientHello reveals which ALPN is negotiated (`cdn/probe/v1`, `cdn/client/v1`, `cdn/watchtower/v1`, `cdn/keys/v1`) | T1 | [005](005-protocol.md) |
| P-05 | `ReputationReport` gossip | Provider, reporter, metrics (delivery speed, correctness, uptime), timestamps — signed and broadcast on `cdn/reputation/v1` | T1 | [008](008-reputation.md) §6 |
| P-06 | `RateChange` gossip | Node pricing updates broadcast on `cdn/global/v1` | T1 | [005](005-protocol.md), [architecture.md](architecture.md) |
| P-07 | Node earnings inference | Channel closures and settlement amounts are on-chain; node revenue is computable | T1 | [003](003-payments.md) |
| P-08 | BLAKE3 hash as global identifier | Same content always produces the same hash; repeated requests for a hash are correlatable | T1 | [002](002-content-addressing.md) |
| P-09 | Probe fan-out content leakage | All probed nodes learn which content hash the requester wants | T2 | [005](005-protocol.md) §Probe |
| P-10 | Cache miss detection | Probe fan-outs triggered by cache misses are visible to all peers, revealing regionally uncommon content | T2 | [001](001-network.md) §Content Discovery |
| P-11 | Probe cache timing correlation | 15-second probe cache ([ADR 001](001-network.md)) means the interval between probe and subsequent `StreamRequest` is trivially observable | T2 | [001](001-network.md), [005](005-protocol.md) |
| P-12 | Gossip topic enumeration | An attacker joining regional gossip topics (`cdn/region/{cc}/v1`) can enumerate all nodes and their region announcements | T2 | [001](001-network.md), [005](005-protocol.md) |
| P-13 | GeoIP inference | Self-reported `regionHint` combined with IP addresses from `multiaddrs` enables geolocation | T2 | [001](001-network.md) |
| P-14 | Reporter credibility leakage | Reporter weight is based on `effective_settled_value` (ADR 008 §4), which depends on on-chain settlement history — reveals a reporter's payment activity | T1 | [008](008-reputation.md) §4 |
| P-15 | RPC provider query visibility | Registry queries, blacklist polling, and rate-bounds lookups are visible to the RPC provider | T3 | [architecture.md](architecture.md) §Trust Assumptions |
| P-16 | Watchtower voucher patterns | Voucher updates expose cumulative bytes delivered, update frequency, and session duration | T3 | [007](007-watchtower.md) |
| P-17 | Relay connection metadata | iroh relays see source/destination IP pairs and connection timing for relayed connections | T3 | [architecture.md](architecture.md) §Trust Assumptions |
| P-18 | Unencrypted iroh key (PoC) | Client's Ed25519 secret key stored at `~/.decdn/iroh_key` with `0600` permissions, no encryption | T4 | [012](012-client.md) §iroh Identity Key |
| P-19 | Offline lease blast radius | Up to 500 `K_blob` values extractable from a compromised device's sealed lease | T4 | [006](006-e2e-encryption.md) §Offline Leases |
| P-20 | No forward secrecy for epoch keys | Compromising `server_secret` retroactively exposes all past and future epoch keys until rotation | T4 | [006](006-e2e-encryption.md) §Consequences |
| P-21 | Permanent client NodeId | Ed25519 identity is persistent across sessions; all content requests are correlatable under one identity | T2 | [012](012-client.md) §iroh Identity Key |

### 3. Analysis by Adversary Tier

#### T1: Passive Network Observer

A passive observer sees gossip messages, on-chain state, and QUIC connection metadata. The key concern is whether aggregating these signals reveals more than any single signal.

**Content demand patterns (P-01, P-06, P-08).** `popular_hashes` in `NodeAnnounce` is the most explicit content-interest signal: it broadcasts the top-20 most-requested hashes per node at the default interval of 60 seconds to all peers. Combined with `RateChange` events (which signal pricing adjustments that may correlate with demand shifts) and the deterministic nature of BLAKE3 hashes, a passive observer can build a per-node demand profile over time. This is a deliberate design choice — `popular_hashes` feeds the prefetching system ([ADR 001](001-network.md) §Prefetch Triggers) and cannot be removed without losing that capability.

**Payment and identity linkability (P-02, P-03, P-07, P-14).** On-chain data permanently links client Ethereum addresses to provider Ethereum addresses via payment channels. Settlement amounts make node revenue computable. The `StakingRegistry` publishes node identity and network location. Reporter credibility in the reputation system leaks a node's settlement history. These are inherent to the accountability model: staking, slashing, and dispute resolution require on-chain identities and state. For the PoC (testnet with no real economic value), this is acceptable.

**Protocol fingerprinting (P-04).** ALPN negotiation in the QUIC TLS ClientHello reveals whether a connection is a probe, a paid stream, a watchtower interaction, or a key delivery session. A network-level observer can classify connections by type. This is standard for any QUIC-based multi-protocol system and is not considered a significant privacy concern — the protocols are not secret.

**Reputation gossip (P-05).** Signed `ReputationReport` messages on `cdn/reputation/v1` broadcast which nodes interact with which other nodes, with performance metrics and timestamps. This enables an observer to map the interaction graph. The 70/30 local/network weight split ([ADR 008](008-reputation.md) §2) limits the value of manipulating gossip, but does not reduce the observability of the data surface itself.

#### T2: Active Protocol Participant

An active participant can probe nodes, join gossip topics, and observe responses to their own protocol interactions. The primary additional concern is content access pattern leakage.

**Probe content leakage (P-09, P-10, P-11).** When a client probes peers for a content hash, all probed nodes learn what is being requested. Probes triggered by cache misses are visible to the entire fan-out set, revealing regionally uncommon or newly requested content. The 15-second probe cache creates a tight timing correlation between probe and subsequent `StreamRequest`. However, probes are explicitly public information ([ADR 005](005-protocol.md)): node identities are in a public registry, content availability is discoverable via probing, and pricing is revealed by design. The protocol fundamentally requires the delivering node to know the requested hash. Mitigating leakage to non-delivering nodes (e.g., via dummy probes) adds bandwidth cost without changing the fundamental property.

**Network enumeration (P-12, P-13).** Regional gossip topics are enumerable, and joining them reveals all participating nodes' identities and self-reported regions. Combined with `multiaddrs` from the on-chain registry, this enables geolocation. This is inherent to any system where nodes must be discoverable to serve content.

**Cross-session client tracking (P-21).** A persistent client NodeId allows any node that has served the client to correlate all past and future requests. Payment channels are keyed by Ethereum address ([ADR 012](012-client.md)), not NodeId, so NodeId rotation would not break the payment model. This is the most actionable privacy improvement with the lowest implementation cost.

#### T3: Infrastructure Operator

Infrastructure operators have a privileged view of specific interaction channels.

**RPC provider (P-15).** The RPC provider observes all on-chain queries: registry lookups, blacklist polling, rate-bounds checks. This reveals which nodes a client or node is interested in. The [architecture.md](architecture.md) trust assumptions already document this and plan multi-source bootstrap for production.

**Watchtower (P-16).** Voucher updates expose channel activity patterns (amounts, frequency, session duration). [ADR 007](007-watchtower.md) acknowledges this: "the privacy impact is low — vouchers are not secret (the counterparty already has them) — but it is a new data surface." The watchtower sees no more than the channel counterparty already knows.

> **Aggregation risk:** Watchtower operators aggregate voucher update patterns across all monitored channels, providing a qualitatively broader payment activity view than any single bilateral counterparty. Production watchtower selection guidance SHOULD recommend using watchtowers operated by different entities than the node's primary business partners to limit cross-channel correlation.

**Relay (P-17).** iroh relays see source/destination IP pairs and connection timing for relayed connections (~10% of conditions). Relays cannot inspect content (all traffic is end-to-end encrypted). This is standard for any relay-based NAT traversal system.

#### T4: Compromised Endpoint

Endpoint compromise yields secrets specific to that endpoint.

**Client key material (P-18).** The PoC stores the iroh secret key unencrypted at `~/.decdn/iroh_key` ([ADR 012](012-client.md)). An attacker with file access gains the client's network identity. Production uses the platform keychain.

**Offline lease extraction (P-19).** A compromised device yields up to 500 `K_blob` values from the sealed offline lease ([ADR 006](006-e2e-encryption.md)). Mitigations are operational: device attestation, per-account device limits (3-5), audio watermarking, and behavioral detection. This is the same tradeoff every major streaming service makes.

**Epoch key derivation (P-20).** Compromising `server_secret` exposes all past and future epoch keys until rotation ([ADR 006](006-e2e-encryption.md)). Production mitigations (HSM-backed derivation, periodic rotation, audit log) are already specified in ADR 006. This is the most significant cryptographic limitation but is a server-side concern, not a protocol privacy issue per se.

### 4. Disposition Summary

| ID | Surface | Disposition | Rationale | Milestone |
|----|---------|-------------|-----------|-----------|
| P-01 | `popular_hashes` gossip | Mitigate | Reduces cardinality of explicit demand signal | Pre-mainnet |
| P-02 | On-chain payment channels | Accept | Required for dispute resolution and slashing | — |
| P-03 | On-chain staking registry | Accept | Required for node accountability and discovery | — |
| P-04 | ALPN protocol identification | Accept | Standard QUIC behavior; protocols are not secret | — |
| P-05 | Reputation gossip | Accept | Accountability requires observable reports; 70/30 local/network split limits exploitation | — |
| P-06 | `RateChange` gossip | Accept | Pricing transparency is a design goal | — |
| P-07 | Node earnings inference | Accept | Inherent to on-chain settlement; no mitigation without breaking dispute model | — |
| P-08 | BLAKE3 global identifier | Accept | Fundamental to content-addressed delivery; no alternative without breaking the architecture | — |
| P-09 | Probe content leakage | Accept | Probes are public by design ([ADR 005](005-protocol.md)); delivering node must know the hash | — |
| P-10 | Cache miss detection | Accept | Inherent to probe fan-out for cache-miss pulls | — |
| P-11 | Probe cache timing | Accept | 15-second window is an optimization tradeoff; attacker already sees the probe | — |
| P-12 | Gossip topic enumeration | Accept | Inherent to any system with discoverable nodes | — |
| P-13 | GeoIP inference | Accept | Self-reported region is intentionally public for client selection | — |
| P-14 | Reporter credibility leakage | Accept | Settlement history is already on-chain (P-02, P-07) | — |
| P-15 | RPC provider visibility | Mitigate | Operational guidance reduces single-provider trust | Pre-mainnet |
| P-16 | Watchtower voucher patterns | Accept | Counterparty already has vouchers; low incremental exposure | — |
| P-17 | Relay connection metadata | Accept | Standard relay behavior; traffic is E2E encrypted | — |
| P-18 | Unencrypted iroh key | Mitigate | Already planned: platform keychain in production | Pre-mainnet |
| P-19 | Offline lease blast radius | Accept | Industry-standard tradeoff; operational mitigations in [ADR 006](006-e2e-encryption.md) | — |
| P-20 | No epoch key forward secrecy | Mitigate | HSM-backed derivation and rotation already specified in [ADR 006](006-e2e-encryption.md) | Pre-mainnet |
| P-21 | Permanent client NodeId | Mitigate | Breaks cross-session linkability at low cost | Pre-mainnet |

### 5. Candidate Mitigations

#### 5.1 Client NodeId Rotation (P-21)

**Current state:** [ADR 012](012-client.md) says rotation is "not supported" for PoC; generating a new key requires deleting the key file and restarting. Production rotation is described but not prioritized.

**Proposal:** Periodic rotation (configurable interval, e.g., every N connections or every T minutes). The client generates a new Ed25519 key, reconnects, and discards the old key. Payment channels are keyed by Ethereum address ([ADR 012](012-client.md)), so rotation does not affect open channels. The Ethereum key (and associated on-chain identity) remains stable — NodeId rotation breaks correlation at the transport layer only.

**Limitation:** A T1 adversary correlating the Ethereum address across channels can still link sessions. NodeId rotation mitigates T2 adversaries (node operators) who see the NodeId in QUIC connections but may not know the client's Ethereum address.

**Effort:** Low. Key generation is cheap, no protocol message changes needed, no on-chain interaction.

#### 5.2 `popular_hashes` Cardinality Reduction (P-01)

**Current state:** `NodeAnnounce` broadcasts the top-20 most-requested hashes every 60 seconds ([ADR 001](001-network.md)). This field is actively consumed by the prefetching system: nodes observe which hashes appear in multiple peers' `popular_hashes` to detect cross-region demand ([ADR 001](001-network.md) §Prefetch Triggers).

**Options:**
- **(a) Reduce cardinality** from 20 to 5. Reduces the signal while preserving the prefetch mechanism. The network popularity threshold (default: 3+ peers within 10 minutes) still works with smaller lists.
- **(b) Add Laplacian noise** (differential privacy). Insert random hashes alongside real ones. Preserves cardinality but degrades prefetch accuracy.
- **(c) Remove entirely.** Not viable — breaks the network popularity signal for prefetching.

**Recommendation:** Option (a) — reduce to 5. The default prefetch threshold of 3+ peers is still achievable with top-5 lists across a network of tens or hundreds of nodes, and the demand signal is 75% smaller.

**Effort:** Low. Change the cap constant and update gossip validation.

#### 5.3 Operational RPC Guidance (P-15)

**Proposal:** Document a production recommendation to use multiple independent RPC providers or a self-hosted node for on-chain queries. This is already implied by [architecture.md](architecture.md) §Trust Assumptions (multi-source bootstrap) but should be made explicit as a privacy recommendation, not just a reliability one.

**Effort:** Minimal. Documentation change in [ADR 012](012-client.md) and [architecture.md](architecture.md).

#### 5.4 Client iroh Key Encryption (P-18)

**Current state:** [ADR 012](012-client.md) already specifies platform keychain for production. No additional design needed — this mitigation is already planned.

#### 5.5 Epoch Key Forward Secrecy (P-20)

**Current state:** [ADR 006](006-e2e-encryption.md) already specifies three mitigations: HSM-backed derivation, periodic `server_secret` rotation, and an append-only key rotation log. No additional design needed — these mitigations are already specified and should be completed pre-mainnet as part of mainnet readiness.

#### 5.6 Dummy Probes (Not Recommended)

**Purpose:** Obscure which content is actually being requested by mixing real probes with decoy probes for random hashes.

**Assessment:** Probes are explicitly public information ([ADR 005](005-protocol.md)). The delivering node must know the requested hash to serve it — dummy probes only hide requests from non-delivering nodes. The benefit is marginal relative to the cost: a 3-5x increase in probe traffic, additional latency (must wait for dummy responses or accept a timing side-channel), and increased node CPU load. Probes are free (no USDC cost), but the bandwidth and compute costs are non-trivial at scale.

**Disposition:** Defer post-mainnet. Revisit only if content access pattern privacy becomes a product requirement.

#### 5.7 Payment Channel Mixing (Not Recommended)

**Purpose:** Break the on-chain link between client and provider Ethereum addresses.

**Assessment:** Options include hub-and-spoke mixing via an intermediary, Tornado Cash-style pooling (significant regulatory risk), or disposable addresses funded from a mixer. On-chain channel data already reveals less than it appears: channels are long-lived and amortized across many sessions ([ADR 003](003-payments.md)). The PoC is on testnet where on-chain privacy is not meaningful. Production mitigation requires regulatory analysis that is out of scope for the protocol design.

**Disposition:** Defer post-mainnet. Requires legal review before any design work.

### 6. Prioritized Mitigation Roadmap

| Priority | Mitigation | Phase | Effort | Impact |
|----------|-----------|-------|--------|--------|
| 1 | Client NodeId rotation (§5.1) | Pre-mainnet | Low | High — breaks cross-session correlation at transport layer |
| 2 | `popular_hashes` cap reduction to 5 (§5.2) | Pre-mainnet | Low | Medium — reduces explicit demand signal by 75% |
| 3 | Client iroh key encryption (§5.4) | Pre-mainnet | Low | Medium — protects identity from T4 on client devices |
| 4 | Operational RPC guidance (§5.3) | Pre-mainnet | Minimal | Medium — documents trust boundary as privacy concern |
| 5 | Epoch key forward secrecy (§5.5) | Pre-mainnet | Medium | High — but already specified in ADR 006; implementation priority |
| 6 | Dummy probes (§5.6) | Post-mainnet | Medium | Low — probes are public by design |
| 7 | Payment channel mixing (§5.7) | Post-mainnet | High | Medium — requires regulatory analysis first |

## Consequences

**Positive:**

- Single reference for the protocol's privacy posture, enabling informed tradeoff decisions before implementation begins
- Explicit disposition for every identified privacy surface prevents implicit acceptance of unanalyzed risks
- Prioritized mitigation roadmap focuses engineering effort on highest-impact, lowest-cost items first
- Adversary-tier framing makes it clear which threats apply to which real-world actors, avoiding over- or under-engineering
- Documents which properties are fundamental protocol limitations (delivering node must know the hash) versus implementation choices (NodeId rotation, `popular_hashes` cardinality)

**Negative:**

- Must be kept in sync as other ADRs evolve — any new protocol feature or gossip message should be evaluated against the inventory in §2
- Some "accept" dispositions may need revisiting as the threat landscape changes, regulatory requirements emerge, or the network scales beyond PoC
- Does not cover application-layer privacy (content provider's app server data handling, subscriber analytics) — this is an explicit scope boundary, not an oversight
- The adversary model assumes rational actors; state-level adversaries with traffic analysis capabilities may extract more from T1-level data than this analysis suggests

## References

- [ADR 001 — Network Topology and Peer Mesh](001-network.md): `NodeAnnounce`, `popular_hashes`, probe fan-out, gossip topics, prefetch triggers
- [ADR 002 — Content Addressing](002-content-addressing.md): BLAKE3 as global content identifier
- [ADR 003 — Payment Model](003-payments.md): payment channel on-chain visibility, probe fishing rate limits
- [ADR 005 — Wire Protocol](005-protocol.md): probe publicity statement, ALPN definitions, `RateChange` gossip
- [ADR 006 — End-to-End Encryption and Key Distribution](006-e2e-encryption.md): epoch keys, forward secrecy, offline lease blast radius, app server privacy boundary
- [ADR 007 — Watchtower Design for Channel Disputes](007-watchtower.md): voucher sharing privacy impact
- [ADR 008 — Reputation System](008-reputation.md): `ReputationReport` gossip, reporter credibility weighting
- [ADR 012 — Client Architecture, Bootstrap, and Trust Model](012-client.md): client NodeId, key storage, rotation
- [ADR 014 — On-Chain Verification for Slashing Evidence](014-on-chain-verification.md): on-chain verification data surface
- [Architecture Overview](architecture.md): trust assumptions (RPC provider, relay, app server), system diagram
