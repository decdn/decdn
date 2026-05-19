# ADR 017: Privacy Analysis

**Date:** 2026-04-04
**Status:** Draft

## Context

The protocol makes deliberate privacy tradeoffs favoring decentralization and accountability over confidentiality. These decisions are scattered across [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh), [ADR 002](002-content-addressing.md#adr-002-content-addressing), [ADR 003](003-payments.md#adr-003-payment-model), [ADR 005](005-protocol.md#adr-005-wire-protocol), [Appendix: Encrypted Content Publishing](appendix-encrypted-content-publishing.md#appendix-encrypted-content-publishing-on-decdn), [ADR 008](008-reputation.md#adr-008-reputation-system), [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model), and [architecture.md](architecture.md#architecture-overview). No single document maps the full privacy surface.

This ADR consolidates that analysis. It introduces no new functionality — it systematizes privacy properties other ADRs already specify, assigns an explicit disposition to each, and prioritizes mitigations for PoC versus production.

**Scope boundary:** Covers protocol-level privacy — data observable through CDN protocol participation, on-chain interactions, and gossip. Application-level privacy (a content provider's app server handling of subscriber data) is out of scope; it is the content provider's responsibility, as noted in [Appendix: Encrypted Content Publishing](appendix-encrypted-content-publishing.md#appendix-encrypted-content-publishing-on-decdn).

## Decision

### Adversary Model

Organized by adversary capability. Each tier subsumes the tier below it.

| Tier | Adversary | Capabilities | Real-World Examples |
|------|-----------|-------------|---------------------|
| T1 | Passive network observer | Observes QUIC connection metadata (IP pairs, timing, volume), public gossip messages, on-chain transactions and events | ISP, nation-state passive surveillance, blockchain analytics firm |
| T2 | Active protocol participant | All of T1 plus: operates staked nodes, sends probes, opens payment channels, subscribes to gossip topics, observes probe responses | Competing CDN, curious node operator, researcher |
| T3 | Infrastructure operator | All of T2 plus: operates an RPC endpoint or iroh relay | RPC provider (Alchemy, Infura), relay operator |
| T4 | Compromised endpoint | Has memory or disk access to a specific client or node | Device theft, malware, law enforcement with warrant |

### Privacy Surface Inventory

Each row is a discrete data exposure. **ID** back-references the analysis and disposition sections.

| ID | Surface | Data Exposed | Min. Tier | Source ADR |
|----|---------|-------------|-----------|------------|
| P-02 | On-chain payment channels | Channel IDs, client/provider Ethereum addresses, deposit amounts, settlement events | T1 | [003](003-payments.md#adr-003-payment-model) |
| P-03 | On-chain staking registry | `nodeId`, `ethAddress`, `multiaddrs`, `regionHint`, registration timestamps | T1 | [001](001-network.md#adr-001-network-topology-and-peer-mesh), [architecture.md](architecture.md#architecture-overview) |
| P-04 | ALPN protocol identification | QUIC TLS ClientHello reveals which ALPN is negotiated (`cdn/probe/v1`, `cdn/client/v1`) | T1 | [005](005-protocol.md#adr-005-wire-protocol) |
| P-05 | `ReputationReport` gossip | Provider, reporter, metrics (delivery speed, correctness, uptime), timestamps — signed and broadcast on `cdn/reputation/v1` | T1 | [008](008-reputation.md#adr-008-reputation-system) [§ Gossip Protocol](008-reputation.md#gossip-protocol) |
| P-07 | Node earnings inference | Channel closures and settlement amounts are on-chain; node revenue is computable | T1 | [003](003-payments.md#adr-003-payment-model) |
| P-08 | BLAKE3 hash as global identifier | Same content always produces the same hash; repeated requests for a hash are correlatable | T1 | [002](002-content-addressing.md#adr-002-content-addressing) |
| P-09 | Probe content leakage | All probed nodes (the DHT-returned candidate set) learn which content hash the requester wants | T2 | [005](005-protocol.md#adr-005-wire-protocol) § Probe |
| P-10 | Cache miss detection | Probes triggered by cache misses are visible to the targeted DHT candidate set, plus DHT FIND_VALUE traffic is visible to nodes close to the hash in keyspace — both reveal regionally uncommon content | T2 | [001](001-network.md#adr-001-network-topology-and-peer-mesh) § Content Discovery |
| P-11 | Probe cache timing correlation | 15-second probe cache ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)) means the interval between probe and subsequent `StreamRequest` is trivially observable | T2 | [001](001-network.md#adr-001-network-topology-and-peer-mesh), [005](005-protocol.md#adr-005-wire-protocol) |
| P-12 | Gossip topic enumeration | An attacker joining regional gossip topics (`cdn/region/{cc}/v1`) can enumerate all nodes and their region announcements | T2 | [001](001-network.md#adr-001-network-topology-and-peer-mesh), [005](005-protocol.md#adr-005-wire-protocol) |
| P-13 | GeoIP inference | Self-reported `regionHint` combined with IP addresses from `multiaddrs` enables geolocation | T2 | [001](001-network.md#adr-001-network-topology-and-peer-mesh) |
| P-14 | Reporter credibility leakage | Reporter weight is based on `effective_settled_value` ([ADR 008 § Network Score Aggregation](008-reputation.md#network-score-aggregation)), which depends on on-chain settlement history — reveals a reporter's payment activity | T1 | [008](008-reputation.md#adr-008-reputation-system) [§ Network Score Aggregation](008-reputation.md#network-score-aggregation) |
| P-15 | RPC provider query visibility | Registry queries, blacklist polling, and rate-bounds lookups are visible to the RPC provider | T3 | [architecture.md](architecture.md#architecture-overview) § Trust Assumptions |
| P-17 | Relay connection metadata | iroh relays see source/destination IP pairs and connection timing for relayed connections | T3 | [architecture.md](architecture.md#architecture-overview) § Trust Assumptions |
| P-18 | Unencrypted iroh key (PoC) | Client's Ed25519 secret key stored at `~/.decdn/iroh_key` with `0600` permissions, no encryption | T4 | [012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) § iroh Identity Key |
| P-19 | Offline lease blast radius | Up to 500 `K_blob` values extractable from a compromised device's sealed lease | T4 | [Appendix: Encrypted Content Publishing](appendix-encrypted-content-publishing.md#appendix-encrypted-content-publishing-on-decdn) § Offline Leases |
| P-20 | No forward secrecy for epoch keys | Compromising `server_secret` retroactively exposes all past and future epoch keys until rotation | T4 | [Appendix: Encrypted Content Publishing](appendix-encrypted-content-publishing.md#appendix-encrypted-content-publishing-on-decdn) § Consequences |
| P-21 | Permanent client NodeId | Ed25519 identity is persistent across sessions; all content requests are correlatable under one identity | T2 | [012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) § iroh Identity Key |
| P-22 | On-chain settlement volume leakage | Voucher nonce and cumulative amount at `settleChannel` reveal per-channel delivery volume; nonce spacing reveals session granularity | T1 | [003](003-payments.md#adr-003-payment-model) § settleChannel |
| P-23 | `slash_sig` as content inventory proof | A node's `slash_sig` on `ProbeResponse` with `has_blob: true` constitutes non-repudiable cryptographic proof that the node held specific content at a specific time; accumulated signatures build a verifiable content inventory | T2 | [014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence) § slash_sig |

### Analysis by Adversary Tier

#### T1: Passive Network Observer

Sees gossip, on-chain state, and QUIC connection metadata. Key concern: whether aggregating signals reveals more than any single one.

##### Content demand patterns (P-08)

BLAKE3 hashes are deterministic global identifiers; repeated requests are correlatable across observers. No `popular_hashes` gossip signal exists; demand is observable only through DHT FIND_VALUE traffic to the K closest nodes for a hash and cache-miss timing inferences. Intrinsic to a content-addressed network; not eliminable without protocol-level mixing.

##### Payment and identity linkability (P-02, P-03, P-07, P-14)

On-chain data permanently links client to provider Ethereum addresses via payment channels; settlement amounts make node revenue computable; `StakingRegistry` publishes node identity and network location; reporter credibility leaks a node's settlement history. Inherent to the accountability model — staking, slashing, and dispute resolution require on-chain identities and state. Acceptable for the PoC (testnet, no real economic value).

##### Settlement volume leakage (P-22)

At on-chain settlement the final voucher nonce and cumulative amount are public. Vouchers issued per MB (default cadence), so the nonce reveals the count of MB-sized increments. Combined with P-02 (client/provider address linkage) and public rate information, an observer computes the exact volume between a specific client-provider pair. Inherent to the on-chain dispute model — settlement amount must be public for dispute resolution.

##### Protocol fingerprinting (P-04)

ALPN negotiation in the QUIC TLS ClientHello reveals whether a connection is a probe, paid stream, or key delivery session, letting a network observer classify connections by type. Standard for any QUIC multi-protocol system; not a significant concern — the protocols are not secret.

##### Reputation gossip (P-05)

Signed `ReputationReport` messages on `cdn/reputation/v1` broadcast which nodes interact with which, with metrics and timestamps, enabling interaction-graph mapping. The 70/30 local/network weight split ([ADR 008](008-reputation.md#adr-008-reputation-system) [§ Score Model](008-reputation.md#score-model)) limits the value of manipulating gossip but does not reduce observability of the data surface.

#### T2: Active Protocol Participant

Can probe nodes, join gossip topics, and observe responses to its own interactions. Primary added concern: content access pattern leakage.

##### Probe content leakage (P-09, P-10, P-11)

Probing peers for a hash tells all probed nodes what is requested. Cache-miss probes are visible to the targeted DHT-candidate set, and DHT FIND_VALUE queries to keyspace-close nodes — both reveal regionally uncommon or newly requested content. The 15-second probe cache creates a tight timing correlation between probe and subsequent `StreamRequest`. Probes are explicitly public ([ADR 005](005-protocol.md#adr-005-wire-protocol)): node identities are in a public registry, content availability is discoverable via probing, pricing is revealed by design. The delivering node fundamentally must know the requested hash; mitigating leakage to non-delivering nodes (e.g., dummy probes) adds bandwidth cost without changing the fundamental property.

##### Network enumeration (P-12, P-13)

Regional gossip topics are enumerable; joining reveals all participating nodes' identities and self-reported regions. Combined with `multiaddrs` from the on-chain registry, this enables geolocation. Inherent to any system where nodes must be discoverable to serve content.

##### `slash_sig` as content inventory proof (P-23)

Every `ProbeResponse` carries a `slash_sig` — an EIP-712 secp256k1 signature binding the node's Ethereum address to specific content hashes and timestamps ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)). Any T2 prober collects non-repudiable, EVM-verifiable, individually attributable proof that the node committed to having (or not having) specific content at a specific time. Systematic probing builds a cryptographic per-node content inventory keyed by Ethereum address. Inherent to the on-chain slashing design — `slash_sig` exists to make node commitments provable; removing it eliminates on-chain slashability.

##### Cross-session client tracking (P-21)

A persistent client NodeId lets any node that served the client correlate all past and future requests. Payment channels are keyed by Ethereum address ([ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model)), not NodeId, so rotation would not break the payment model. Most actionable privacy improvement at lowest implementation cost.

#### T3: Infrastructure Operator

Privileged view of specific interaction channels.

##### RPC provider (P-15)

Observes all on-chain queries: registry lookups, blacklist polling, rate-bounds checks — revealing which nodes a client or node is interested in. [architecture.md](architecture.md#architecture-overview) trust assumptions already document this and plan multi-source bootstrap for production.

##### Relay (P-17)

iroh relays see source/destination IP pairs and connection timing for relayed connections (~10% of conditions). Relays cannot inspect content (all traffic is E2E encrypted). Standard for any relay-based NAT traversal system.

#### T4: Compromised Endpoint

Yields secrets specific to that endpoint.

##### Client key material (P-18)

PoC stores the iroh secret key unencrypted at `~/.decdn/iroh_key` ([ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model)); file access yields the client's network identity. Production uses the platform keychain.

##### Offline lease extraction (P-19)

A compromised device yields up to 500 `K_blob` values from the sealed offline lease ([Appendix: Encrypted Content Publishing](appendix-encrypted-content-publishing.md#appendix-encrypted-content-publishing-on-decdn)). Mitigations are operational: device attestation, per-account device limits (3-5), audio watermarking, behavioral detection. Same tradeoff every major streaming service makes.

##### Epoch key derivation (P-20)

Compromising `server_secret` exposes all past and future epoch keys until rotation ([Appendix: Encrypted Content Publishing](appendix-encrypted-content-publishing.md#appendix-encrypted-content-publishing-on-decdn)). Production mitigations (HSM-backed derivation, periodic rotation, audit log) are already specified in that appendix. Most significant cryptographic limitation, but a server-side concern, not a protocol privacy issue per se.

### Disposition Summary

| ID | Surface | Disposition | Rationale | Milestone |
|----|---------|-------------|-----------|-----------|
| P-02 | On-chain payment channels | Accept | Required for dispute resolution and slashing | — |
| P-03 | On-chain staking registry | Accept | Required for node accountability and discovery | — |
| P-04 | ALPN protocol identification | Accept | Standard QUIC behavior; protocols are not secret | — |
| P-05 | Reputation gossip | Accept | Accountability requires observable reports; 70/30 local/network split limits exploitation | — |
| P-07 | Node earnings inference | Accept | Inherent to on-chain settlement; no mitigation without breaking dispute model | — |
| P-08 | BLAKE3 global identifier | Accept | Fundamental to content-addressed delivery; no alternative without breaking the architecture | — |
| P-09 | Probe content leakage | Accept | Probes are public by design ([ADR 005](005-protocol.md#adr-005-wire-protocol)); delivering node must know the hash | — |
| P-10 | Cache miss detection | Accept | Inherent to probing and DHT lookups for cache-miss pulls | — |
| P-11 | Probe cache timing | Accept | 15-second window is an optimization tradeoff; attacker already sees the probe | — |
| P-12 | Gossip topic enumeration | Accept | Inherent to any system with discoverable nodes | — |
| P-13 | GeoIP inference | Accept | Self-reported region is intentionally public for client selection | — |
| P-14 | Reporter credibility leakage | Accept | Settlement history is already on-chain (P-02, P-07) | — |
| P-15 | RPC provider visibility | Mitigate | Operational guidance reduces single-provider trust | Pre-mainnet |
| P-17 | Relay connection metadata | Accept | Standard relay behavior; traffic is E2E encrypted | — |
| P-18 | Unencrypted iroh key | Mitigate | Already planned: platform keychain in production | Pre-mainnet |
| P-19 | Offline lease blast radius | Accept | Industry-standard tradeoff; operational mitigations in [Appendix: Encrypted Content Publishing](appendix-encrypted-content-publishing.md#appendix-encrypted-content-publishing-on-decdn) | — |
| P-20 | No epoch key forward secrecy | Mitigate | HSM-backed derivation and rotation already specified in [Appendix: Encrypted Content Publishing](appendix-encrypted-content-publishing.md#appendix-encrypted-content-publishing-on-decdn) | Pre-mainnet |
| P-21 | Permanent client NodeId | Mitigate | Breaks cross-session linkability at low cost | Pre-mainnet |
| P-22 | Settlement volume leakage | Accept | Inherent to on-chain settlement; settlement amount must be public for dispute resolution | — |
| P-23 | `slash_sig` content inventory | Accept | Required for on-chain accountability; removing `slash_sig` eliminates slashability | — |

### Candidate Mitigations

#### Client NodeId Rotation (P-21)

**Current state:** [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) says rotation is "not supported" for PoC; a new key requires deleting the key file and restarting. Production rotation is described but not prioritized.

**Proposal:** Periodic rotation (configurable interval, e.g., every N connections or every T minutes): client generates a new Ed25519 key, reconnects, discards the old key. Payment channels are keyed by Ethereum address ([ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model)), so rotation does not affect open channels. The Ethereum key and on-chain identity stay stable — rotation breaks correlation at the transport layer only.

**Limitation:** A T1 adversary correlating the Ethereum address across channels can still link sessions. A T2 node operator that serves the client learns the Ethereum address via the `ethereum_address` field in `StreamRequest` ([ADR 005](005-protocol.md#adr-005-wire-protocol)), so rotation does NOT prevent a serving node from linking sessions. It primarily mitigates correlation by non-serving T2 participants (nodes that probe but are not selected for delivery) and T1 passive observers who see QUIC metadata but not TLS-encrypted `StreamRequest` contents.

**Effort:** Low. Key generation is cheap; no protocol message changes; no on-chain interaction.

#### Operational RPC Guidance (P-15)

**Proposal:** Document a production recommendation to use multiple independent RPC providers or a self-hosted node for on-chain queries. Already implied by [architecture.md](architecture.md#architecture-overview) § Trust Assumptions (multi-source bootstrap) but should be explicit as a privacy recommendation, not just reliability.

**Effort:** Minimal. Documentation change in [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) and [architecture.md](architecture.md#architecture-overview).

#### Client iroh Key Encryption (P-18)

**Current state:** [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) already specifies platform keychain for production. No additional design needed — already planned.

#### Epoch Key Forward Secrecy (P-20)

**Current state:** [Appendix: Encrypted Content Publishing](appendix-encrypted-content-publishing.md#appendix-encrypted-content-publishing-on-decdn) already specifies three mitigations: HSM-backed derivation, periodic `server_secret` rotation, and an append-only key rotation log. No additional design needed — complete pre-mainnet as part of mainnet readiness.

#### Dummy Probes (Not Recommended)

**Purpose:** Obscure requested content by mixing real probes with decoy probes for random hashes.

**Assessment:** Probes are explicitly public ([ADR 005](005-protocol.md#adr-005-wire-protocol)). The delivering node must know the requested hash — dummy probes only hide requests from non-delivering nodes. Benefit is marginal relative to cost: 3-5x more probe traffic, added latency (wait for dummy responses or accept a timing side-channel), more node CPU. Probes are free (no USDC cost), but bandwidth and compute costs are non-trivial at scale.

**Disposition:** Defer post-mainnet. Revisit only if content access pattern privacy becomes a product requirement.

#### Payment Channel Mixing (Not Recommended)

**Purpose:** Break the on-chain link between client and provider Ethereum addresses.

**Assessment:** Options: hub-and-spoke mixing via an intermediary, Tornado Cash-style pooling (significant regulatory risk), or disposable addresses funded from a mixer. On-chain channel data reveals less than it appears: channels are long-lived and amortized across many sessions ([ADR 003](003-payments.md#adr-003-payment-model)). The PoC is on testnet where on-chain privacy is not meaningful. Production mitigation requires regulatory analysis, out of scope for protocol design.

**Disposition:** Defer post-mainnet. Requires legal review before any design work.

### Prioritized Mitigation Roadmap

| Priority | Mitigation | Phase | Effort | Impact |
|----------|-----------|-------|--------|--------|
| 1 | Client NodeId rotation ([§ Client NodeId Rotation (P-21)](#client-nodeid-rotation-p-21)) | Pre-mainnet | Low | High — breaks cross-session correlation at transport layer |
| 2 | Client iroh key encryption ([§ Client iroh Key Encryption (P-18)](#client-iroh-key-encryption-p-18)) | Pre-mainnet | Low | Medium — protects identity from T4 on client devices |
| 3 | Operational RPC guidance ([§ Operational RPC Guidance (P-15)](#operational-rpc-guidance-p-15)) | Pre-mainnet | Minimal | Medium — documents trust boundary as privacy concern |
| 4 | Epoch key forward secrecy ([§ Epoch Key Forward Secrecy (P-20)](#epoch-key-forward-secrecy-p-20)) | Pre-mainnet | Medium | High — but already specified in the encrypted-content-publishing appendix; implementation priority |
| 5 | Dummy probes ([§ Dummy Probes (Not Recommended)](#dummy-probes-not-recommended)) | Post-mainnet | Medium | Low — probes are public by design |
| 6 | Payment channel mixing ([§ Payment Channel Mixing (Not Recommended)](#payment-channel-mixing-not-recommended)) | Post-mainnet | High | Medium — requires regulatory analysis first |

## Consequences

### Positive

- Single reference for the protocol's privacy posture, enabling informed tradeoffs before implementation
- Explicit disposition for every privacy surface prevents implicit acceptance of unanalyzed risks
- Prioritized roadmap focuses engineering effort on highest-impact, lowest-cost items first
- Adversary-tier framing maps threats to real-world actors, avoiding over- or under-engineering
- Documents fundamental protocol limitations (delivering node must know the hash) versus implementation choices (NodeId rotation cadence, RPC provider trust)

### Negative

- Must be kept in sync as other ADRs evolve — any new protocol feature or gossip message must be evaluated against [§ Privacy Surface Inventory](#privacy-surface-inventory)
- Some "accept" dispositions may need revisiting as the threat landscape or regulatory requirements change, or the network scales beyond PoC
- Does not cover application-layer privacy (content provider's app server data, subscriber analytics) — an explicit scope boundary, not an oversight
- The adversary model assumes rational actors; state-level adversaries with traffic analysis may extract more from T1-level data than this analysis suggests

## References

- [ADR 001 — Network Topology and Peer Mesh](001-network.md#adr-001-network-topology-and-peer-mesh): `NodeAnnounce`, DHT-candidate probing, gossip topics, prefetch triggers
- [ADR 002 — Content Addressing](002-content-addressing.md#adr-002-content-addressing): BLAKE3 as global content identifier
- [ADR 003 — Payment Model](003-payments.md#adr-003-payment-model): payment channel on-chain visibility, probe fishing rate limits
- [ADR 005 — Wire Protocol](005-protocol.md#adr-005-wire-protocol): probe publicity statement, ALPN definitions
- [Appendix: Encrypted Content Publishing](appendix-encrypted-content-publishing.md#appendix-encrypted-content-publishing-on-decdn): epoch keys, forward secrecy, offline lease blast radius, app server privacy boundary
- [ADR 008 — Reputation System](008-reputation.md#adr-008-reputation-system): `ReputationReport` gossip, reporter credibility weighting
- [ADR 012 — Client Architecture, Bootstrap, and Trust Model](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model): client NodeId, key storage, rotation
- [ADR 014 — On-Chain Verification for Slashing Evidence](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence): on-chain verification data surface
- [Architecture Overview](architecture.md#architecture-overview): trust assumptions (NTP, RPC provider, relay), system diagram
