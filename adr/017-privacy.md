# ADR 017: Privacy Analysis

**Date:** 2026-04-04
**Status:** Draft

## Context

The protocol makes deliberate privacy tradeoffs favoring decentralization and accountability over confidentiality. These decisions are scattered across [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh), [ADR 002](002-content-addressing.md#adr-002-content-addressing), [ADR 003](003-payments.md#adr-003-payment-model), [ADR 005](005-protocol.md#adr-005-wire-protocol), [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model), and [architecture.md](architecture.md#architecture-overview). No single document maps the full privacy surface.

This ADR consolidates that analysis. It introduces no new functionality — it systematizes privacy properties other ADRs already specify, assigns an explicit disposition to each, and prioritizes mitigations for PoC versus production.

**Scope boundary:** Covers protocol-level privacy — data observable through CDN protocol participation, on-chain interactions, and gossip. Application-level privacy is out of scope and remains the application's responsibility.

## Decision

### Adversary Model

Organized by adversary capability. Each tier subsumes the tier below it.

| Tier | Adversary | Capabilities | Real-World Examples |
|------|-----------|-------------|---------------------|
| T1 | Passive network observer | Observes QUIC connection metadata (IP pairs, timing, volume), public gossip messages, on-chain transactions and events | ISP, nation-state passive surveillance, blockchain analytics firm |
| T2 | Active protocol participant | All of T1 plus: operates staked nodes, sends probes, opens payment pools, subscribes to gossip topics, observes probe responses | Competing CDN, curious node operator, researcher |
| T3 | Infrastructure operator | All of T2 plus: operates an RPC endpoint or iroh relay | RPC provider (Alchemy, Infura), relay operator |
| T4 | Compromised endpoint | Has memory or disk access to a specific client or node | Device theft, malware, law enforcement with warrant |

### Privacy Surface Inventory

Each row is a discrete data exposure. **ID** back-references the analysis and disposition sections.

| ID | Surface | Data Exposed | Min. Tier | Source ADR |
|----|---------|-------------|-----------|------------|
| P-02 | On-chain payment pools | Pool IDs, owner Ethereum address, signer and provider addresses (revealed at redemption), deposit amounts, redemption events | T1 | [003](003-payments.md#adr-003-payment-model) |
| P-03 | On-chain staking registry | `nodeId`, `ethAddress`, `multiaddrs`, `regionHint`, registration timestamps | T1 | [001](001-network.md#adr-001-network-topology-and-peer-mesh), [architecture.md](architecture.md#architecture-overview) |
| P-04 | ALPN protocol identification | QUIC TLS ClientHello reveals which ALPN is negotiated (`cdn/probe/v1`, `cdn/client/v1`) | T1 | [005](005-protocol.md#adr-005-wire-protocol) |
| P-07 | Node earnings inference | Redemption and pool-close amounts are on-chain; node revenue is computable | T1 | [003](003-payments.md#adr-003-payment-model) |
| P-08 | BLAKE3 hash as global identifier | Same content always produces the same hash; repeated requests for a hash are correlatable | T1 | [002](002-content-addressing.md#adr-002-content-addressing) |
| P-09 | Probe content leakage | All probed nodes (the DHT-returned candidate set) learn which content hash the requester wants | T2 | [005](005-protocol.md#adr-005-wire-protocol) § Probe |
| P-10 | Cache miss detection | Probes triggered by cache misses are visible to the targeted DHT candidate set, plus DHT FIND_VALUE traffic is visible to nodes close to the hash in keyspace — both reveal regionally uncommon content | T2 | [001](001-network.md#adr-001-network-topology-and-peer-mesh) § Content Discovery |
| P-11 | Probe cache timing correlation | 15-second probe cache ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)) means the interval between probe and subsequent `StreamRequest` is trivially observable | T2 | [001](001-network.md#adr-001-network-topology-and-peer-mesh), [005](005-protocol.md#adr-005-wire-protocol) |
| P-12 | Gossip topic enumeration | An attacker joining regional gossip topics (`cdn/region/{cc}/v1`) can enumerate all nodes and their region announcements | T2 | [001](001-network.md#adr-001-network-topology-and-peer-mesh), [005](005-protocol.md#adr-005-wire-protocol) |
| P-13 | GeoIP inference | Self-reported `regionHint` combined with IP addresses from `multiaddrs` enables geolocation | T2 | [001](001-network.md#adr-001-network-topology-and-peer-mesh) |
| P-15 | RPC provider query visibility | Registry queries, blacklist polling, and rate-bounds lookups are visible to the RPC provider | T3 | [architecture.md](architecture.md#architecture-overview) § Trust Assumptions |
| P-17 | Relay connection metadata | iroh relays see source/destination IP pairs and connection timing for relayed connections | T3 | [architecture.md](architecture.md#architecture-overview) § Trust Assumptions |
| P-18 | Unencrypted iroh key (PoC) | Client's Ed25519 secret key stored at `~/.decdn/iroh_key` with `0600` permissions, no encryption | T4 | [012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) § iroh Identity Key |
| P-21 | Permanent client NodeId | Ed25519 identity is persistent across sessions; all content requests are correlatable under one identity | T2 | [012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) § iroh Identity Key |
| P-22 | On-chain settlement volume leakage | Cumulative amount and byte count at `redeem` reveal per-lane delivery volume; per-lane nonce spacing reveals session granularity | T1 | [003](003-payments.md#adr-003-payment-model) § Redemption and Close |
| P-23 | `slash_sig` as content inventory proof | A node's `slash_sig` on `ProbeResponse` with `has_blob: true` constitutes non-repudiable cryptographic proof that the node held specific content at a specific time; accumulated signatures build a verifiable content inventory | T2 | [014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence) § slash_sig |

### Analysis by Adversary Tier

#### T1: Passive Network Observer

Sees gossip, on-chain state, and QUIC connection metadata. Key concern: whether aggregating signals reveals more than any single one.

##### Content demand patterns (P-08)

BLAKE3 hashes are deterministic global identifiers; repeated requests are correlatable across observers. No `popular_hashes` gossip signal exists; demand is observable only through DHT FIND_VALUE traffic to the K closest nodes for a hash and cache-miss timing inferences. Intrinsic to a content-addressed network; not eliminable without protocol-level mixing.

##### Payment and identity linkability (P-02, P-03, P-07)

On-chain data links a pool owner to the provider addresses it pays, revealed at redemption; settlement amounts make node revenue computable; `CapacityBond` publishes node identity and network location. Inherent to the accountability model — staking, slashing, and settlement require on-chain identities and state. Acceptable for the PoC (testnet, no real economic value).

##### Settlement volume leakage (P-22)

At each on-chain redemption the lane's cumulative amount and byte count are public. Vouchers issued per MB (default cadence), so the per-lane nonce reveals the count of MB-sized increments. Combined with P-02 (signer/provider address linkage) and public rate information, an observer computes the exact volume between a specific signer-provider pair. Inherent to on-chain settlement — redemption amounts must be public.

##### Protocol fingerprinting (P-04)

ALPN negotiation in the QUIC TLS ClientHello reveals whether a connection is a probe, paid stream, or key delivery session, letting a network observer classify connections by type. Standard for any QUIC multi-protocol system; not a significant concern — the protocols are not secret.

#### T2: Active Protocol Participant

Can probe nodes, join gossip topics, and observe responses to its own interactions. Primary added concern: content access pattern leakage.

##### Probe content leakage (P-09, P-10, P-11)

Probing peers for a hash tells all probed nodes what is requested. Cache-miss probes are visible to the targeted DHT-candidate set, and DHT FIND_VALUE queries to keyspace-close nodes — both reveal regionally uncommon or newly requested content. The 15-second probe cache creates a tight timing correlation between probe and subsequent `StreamRequest`. Probes are explicitly public ([ADR 005](005-protocol.md#adr-005-wire-protocol)): node identities are in a public registry, content availability is discoverable via probing, pricing is revealed by design. The delivering node fundamentally must know the requested hash; mitigating leakage to non-delivering nodes (e.g., dummy probes) adds bandwidth cost without changing the fundamental property.

##### Network enumeration (P-12, P-13)

Regional gossip topics are enumerable; joining reveals all participating nodes' identities and self-reported regions. Combined with `multiaddrs` from the on-chain registry, this enables geolocation. Inherent to any system where nodes must be discoverable to serve content.

##### `slash_sig` as content inventory proof (P-23)

Every `ProbeResponse` carries a `slash_sig` — an EIP-712 secp256k1 signature binding the node's Ethereum address to specific content hashes and timestamps ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)). Any T2 prober collects non-repudiable, EVM-verifiable, individually attributable proof that the node committed to having (or not having) specific content at a specific time. Systematic probing builds a cryptographic per-node content inventory keyed by Ethereum address. Inherent to the on-chain slashing design — `slash_sig` exists to make node commitments provable; removing it eliminates on-chain slashability.

##### Cross-session client tracking (P-21)

A persistent client NodeId exposes a stable transport identifier to every node the client connects to. Rotating it does not provide cross-session unlinkability: the serving node must resolve the signer's Ethereum address to attribute vouchers and redeem them on-chain, and a T1 observer correlates that same address across public payment-pool activity (P-02). Rotation only obscures the client from non-serving T2 probers — participants that learn a NodeId by completing a handshake but are never selected for delivery — and that residual is already leaked as content demand through the accepted P-08 and P-10 surfaces. Details in [§ Client NodeId Rotation (P-21)](#client-nodeid-rotation-p-21).

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

### Disposition Summary

| ID | Surface | Disposition | Rationale | Milestone |
|----|---------|-------------|-----------|-----------|
| P-02 | On-chain payment pools | Accept | Required for settlement and slashing | — |
| P-03 | On-chain staking registry | Accept | Required for node accountability and discovery | — |
| P-04 | ALPN protocol identification | Accept | Standard QUIC behavior; protocols are not secret | — |
| P-07 | Node earnings inference | Accept | Inherent to on-chain settlement; no mitigation without breaking dispute model | — |
| P-08 | BLAKE3 global identifier | Accept | Fundamental to content-addressed delivery; no alternative without breaking the architecture | — |
| P-09 | Probe content leakage | Accept | Probes are public by design ([ADR 005](005-protocol.md#adr-005-wire-protocol)); delivering node must know the hash | — |
| P-10 | Cache miss detection | Accept | Inherent to probing and DHT lookups for cache-miss pulls | — |
| P-11 | Probe cache timing | Accept | 15-second window is an optimization tradeoff; attacker already sees the probe | — |
| P-12 | Gossip topic enumeration | Accept | Inherent to any system with discoverable nodes | — |
| P-13 | GeoIP inference | Accept | Self-reported region is intentionally public for client selection | — |
| P-15 | RPC provider visibility | Mitigate | Operational guidance reduces single-provider trust | Pre-mainnet |
| P-17 | Relay connection metadata | Accept | Standard relay behavior; traffic is E2E encrypted | — |
| P-18 | Unencrypted iroh key | Mitigate | Already planned: platform keychain in production | Pre-mainnet |
| P-21 | Permanent client NodeId | Accept | Serving nodes and T1 observers link sessions by the stable Ethereum address the payment model requires (P-02); the residual non-serving-prober benefit is already leaked through accepted P-08 and P-10 | — |
| P-22 | Settlement volume leakage | Accept | Inherent to on-chain settlement; settlement amount must be public for dispute resolution | — |
| P-23 | `slash_sig` content inventory | Accept | Required for on-chain accountability; removing `slash_sig` eliminates slashability | — |

### Candidate Mitigations

#### Client NodeId Rotation (P-21)

**Available capability:** [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) documents production rotation: generate a new iroh key, reconnect, and sign a fresh `BindNodeId` with the same Ethereum key. Open payment pools remain valid because they are keyed by `(owner_ethereum_address, poolNonce)`, not by NodeId. Rotation is not supported for PoC — deleting the key file and restarting generates a new identity, but that is a side effect, not a rotation procedure.

**Privacy effect:** Rotation replaces the stable transport identifier, obscuring cross-session correlation by non-serving T2 probers — participants that learn a NodeId by completing a handshake but are never selected for delivery, and so never see a `StreamRequest`. It does not help against a T1 passive observer: P-21 is a T2 surface because the iroh identity is exchanged under TLS handshake encryption, leaving only the ALPN visible in the clear (P-04), so a passive observer correlates by IP regardless of rotation.

**Limitation:** Rotation does not provide cross-session unlinkability. Every request carries `pool_id = keccak256(owner, poolNonce)` ([ADR 003](003-payments.md#adr-003-payment-model)) in the base `StreamRequest`, and the serving node must resolve the signer's Ethereum address to attribute vouchers and redeem them on-chain — from the on-chain binding for a registered client, or from the `ethereum_address` field in `StreamRequestExt` for an off-chain one ([ADR 005](005-protocol.md#adr-005-wire-protocol)). A T1 observer then correlates that address across public payment-pool activity. This limitation is scale-independent because every serving relationship exposes the stable payment identity regardless of node count.

**Disposition:** Accept. P-02 is what defeats rotation — the payment model requires a stable client address visible to both the serving node and any on-chain observer — and the remaining non-serving-prober benefit is already leaked as content demand through the accepted P-08 and P-10 surfaces. Production rotation remains an optional identity-lifecycle capability, not a privacy roadmap commitment.

#### Operational RPC Guidance (P-15)

**Proposal:** Document a production recommendation to use multiple independent RPC providers or a self-hosted node for on-chain queries. Already implied by [architecture.md](architecture.md#architecture-overview) § Trust Assumptions (multi-source bootstrap) but should be explicit as a privacy recommendation, not just reliability.

**Effort:** Minimal. Documentation change in [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) and [architecture.md](architecture.md#architecture-overview).

#### Client iroh Key Encryption (P-18)

**Current state:** [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) already specifies platform keychain for production. No additional design needed — already planned.

#### Dummy Probes (Not Recommended)

**Purpose:** Obscure requested content by mixing real probes with decoy probes for random hashes.

**Assessment:** Probes are explicitly public ([ADR 005](005-protocol.md#adr-005-wire-protocol)). The delivering node must know the requested hash — dummy probes only hide requests from non-delivering nodes. Benefit is marginal relative to cost: 3-5x more probe traffic, added latency (wait for dummy responses or accept a timing side-channel), more node CPU. Probes are free (no USDC cost), but bandwidth and compute costs are non-trivial at scale.

**Disposition:** Defer post-mainnet. Revisit only if content access pattern privacy becomes a product requirement.

#### Payment Pool Mixing (Not Recommended)

**Purpose:** Break the on-chain link between client and provider Ethereum addresses.

**Assessment:** Options: hub-and-spoke mixing via an intermediary, Tornado Cash-style pooling (significant regulatory risk), or disposable addresses funded from a mixer. On-chain pool data reveals less than it appears: pools are long-lived and amortized across many sessions ([ADR 003](003-payments.md#adr-003-payment-model)). The PoC is on testnet where on-chain privacy is not meaningful. Production mitigation requires regulatory analysis, out of scope for protocol design.

**Disposition:** Defer post-mainnet. Requires legal review before any design work.

### Prioritized Mitigation Roadmap

| Priority | Mitigation | Phase | Effort | Impact |
|----------|-----------|-------|--------|--------|
| 1 | Client iroh key encryption ([§ Client iroh Key Encryption (P-18)](#client-iroh-key-encryption-p-18)) | Pre-mainnet | Low | Medium — protects identity from T4 on client devices |
| 2 | Operational RPC guidance ([§ Operational RPC Guidance (P-15)](#operational-rpc-guidance-p-15)) | Pre-mainnet | Minimal | Medium — documents trust boundary as privacy concern |
| 3 | Dummy probes ([§ Dummy Probes (Not Recommended)](#dummy-probes-not-recommended)) | Post-mainnet | Medium | Low — probes are public by design |
| 4 | Payment pool mixing ([§ Payment Pool Mixing (Not Recommended)](#payment-pool-mixing-not-recommended)) | Post-mainnet | High | Medium — requires regulatory analysis first |

## Consequences

### Positive

- Single reference for the protocol's privacy posture, enabling informed tradeoffs before implementation
- Explicit disposition for every privacy surface prevents implicit acceptance of unanalyzed risks
- Prioritized roadmap focuses engineering effort on highest-impact, lowest-cost items first
- Adversary-tier framing maps threats to real-world actors, avoiding over- or under-engineering
- Distinguishes fundamental protocol limitations (delivering nodes learn requested hashes and stable payment identities), optional defense-in-depth capabilities (production NodeId rotation), and prioritized operational mitigations (RPC provider trust)

### Negative

- Must be kept in sync as other ADRs evolve — any new protocol feature or gossip message must be evaluated against [§ Privacy Surface Inventory](#privacy-surface-inventory)
- Some "accept" dispositions may need revisiting as the threat landscape or regulatory requirements change, or the network scales beyond PoC
- Does not cover application-layer privacy (content provider's app server data, subscriber analytics) — an explicit scope boundary, not an oversight
- The adversary model assumes rational actors; state-level adversaries with traffic analysis may extract more from T1-level data than this analysis suggests

## References

- [ADR 001 — Network Topology and Peer Mesh](001-network.md#adr-001-network-topology-and-peer-mesh): `NodeAnnounce`, DHT-candidate probing, gossip topics
- [ADR 002 — Content Addressing](002-content-addressing.md#adr-002-content-addressing): BLAKE3 as global content identifier
- [ADR 003 — Payment Model](003-payments.md#adr-003-payment-model): payment pool on-chain visibility, probe fishing rate limits
- [ADR 005 — Wire Protocol](005-protocol.md#adr-005-wire-protocol): probe publicity statement, ALPN definitions
- [ADR 012 — Client Architecture, Bootstrap, and Trust Model](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model): client NodeId, key storage, rotation
- [ADR 014 — On-Chain Verification for Slashing Evidence](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence): on-chain verification data surface
- [Architecture Overview](architecture.md#architecture-overview): trust assumptions (NTP, RPC provider, relay), system diagram
