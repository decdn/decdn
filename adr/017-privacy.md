# ADR 017: Privacy Analysis

**Date:** 2026-04-04
**Status:** Draft

## Context

The protocol makes deliberate privacy tradeoffs favoring decentralization and accountability over confidentiality. These decisions are scattered across [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh), [ADR 002](002-content-addressing.md#adr-002-content-addressing), [ADR 003](003-payments.md#adr-003-payment-model), [ADR 005](005-protocol.md#adr-005-wire-protocol), [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model), and [architecture.md](architecture.md#architecture-overview). No single document maps the full privacy surface.

This ADR consolidates that analysis. It introduces no new functionality — it systematizes privacy properties other ADRs already specify, assigns an explicit disposition to each, and prioritizes mitigations for PoC versus production.

**Scope boundary:** Covers protocol-level privacy — data observable through CDN protocol participation and on-chain interactions. Application-level privacy is out of scope and remains the application's responsibility.

## Decision

### Adversary Model

Organized by adversary capability. Each tier subsumes the tier below it.

| Tier | Adversary | Capabilities | Real-World Examples |
|------|-----------|-------------|---------------------|
| T1 | Passive network observer | Observes QUIC connection metadata (IP pairs, timing, volume), on-chain transactions and events | ISP, nation-state passive surveillance, blockchain analytics firm |
| T2 | Active protocol participant | All of T1 plus: operates staked nodes, sends discovery opens, opens payment pools, observes discovery answers | Competing CDN, curious node operator, researcher |
| T3 | Infrastructure operator | All of T2 plus: operates an RPC endpoint or iroh relay | RPC provider (Alchemy, Infura), relay operator |
| T4 | Compromised endpoint | Has memory or disk access to a specific client or node | Device theft, malware, law enforcement with warrant |

### Privacy Surface Inventory

Each row is a discrete data exposure. **ID** back-references the analysis and disposition sections.

| ID | Surface | Data Exposed | Min. Tier | Source ADR |
|----|---------|-------------|-----------|------------|
| P-02 | On-chain payment pools | Pool IDs, owner Ethereum address, signer and provider addresses (revealed at redemption), deposit amounts, redemption events | T1 | [003](003-payments.md#adr-003-payment-model) |
| P-03 | On-chain staking registry | `nodeId`, `ethAddress`, `multiaddrs`, `regionHint`, registration timestamps | T1 | [001](001-network.md#adr-001-network-topology-and-peer-mesh), [architecture.md](architecture.md#architecture-overview) |
| P-04 | ALPN protocol identification | QUIC TLS ClientHello reveals which ALPN is negotiated (`cdn/client/v1`, `cdn/dht/v1`) | T1 | [005](005-protocol.md#adr-005-wire-protocol) |
| P-07 | Node earnings inference | Redemption and pool-close amounts are on-chain; node revenue is computable | T1 | [003](003-payments.md#adr-003-payment-model) |
| P-08 | BLAKE3 hash as global identifier | Same content always produces the same hash; repeated requests for a hash are correlatable | T1 | [002](002-content-addressing.md#adr-002-content-addressing) |
| P-09 | Discovery-open content leakage | Every node that gets a discovery open (the candidate set) learns which content hash the requester wants, and the pool and payer address that the open names | T2 | [005](005-protocol.md#discovery-opens) § Discovery opens |
| P-10 | Cache miss detection | Discovery opens triggered by cache misses are visible to the targeted DHT candidate set, plus DHT FIND_VALUE traffic is visible to nodes close to the hash in keyspace — both reveal regionally uncommon content | T2 | [001](001-network.md#adr-001-network-topology-and-peer-mesh) § Content Discovery |
| P-11 | Discovery-to-delivery correlation | The winner's discovery open becomes its paid stream, and a pull from the 30-second candidate cache ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)) follows a recent discovery round, so the link between discovery and delivery is trivially observable | T2 | [001](001-network.md#adr-001-network-topology-and-peer-mesh), [005](005-protocol.md#adr-005-wire-protocol) |
| P-12 | Registry node enumeration | Reading the on-chain `CapacityBond` active set (`getRegisteredNodes`) enumerates every active node with its self-reported `regionHint` | T1 | [001](001-network.md#adr-001-network-topology-and-peer-mesh), [003](003-payments.md#adr-003-payment-model) |
| P-13 | GeoIP inference | Self-reported `regionHint` combined with IP addresses from `multiaddrs` enables geolocation | T2 | [001](001-network.md#adr-001-network-topology-and-peer-mesh) |
| P-15 | RPC provider query visibility | Registry queries, blacklist polling, and rate-bounds lookups are visible to the RPC provider | T3 | [architecture.md](architecture.md#architecture-overview) § Trust Assumptions |
| P-17 | Relay connection metadata | iroh relays see source/destination IP pairs and connection timing for relayed connections | T3 | [architecture.md](architecture.md#architecture-overview) § Trust Assumptions |
| P-18 | Unencrypted iroh key (PoC) | Client's Ed25519 secret key stored at `~/.decdn/iroh_key` with `0600` permissions, no encryption | T4 | [012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) § iroh Identity Key |
| P-21 | Permanent client NodeId | Ed25519 identity is persistent across sessions; all content requests are correlatable under one identity | T2 | [012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) § iroh Identity Key |
| P-22 | On-chain settlement volume leakage | Cumulative amount and byte count at `redeem` reveal per-lane delivery volume; their spacing across redemptions reveals session granularity | T1 | [003](003-payments.md#adr-003-payment-model) § Redemption and Close |
| P-23 | `slash_sig` as content inventory proof | A node's `slash_sig` on a `StreamResponse` with `ok: true`, the answer to a discovery open included, constitutes non-repudiable cryptographic proof that the node served specific content at a specific time; accumulated signatures build a verifiable content inventory | T2 | [005](005-protocol.md#signer-binding) § Signer binding |
| P-24 | Origin attestation in DHT records | Every DHT record carries the signed attestation of a seated origin for the hash, so anyone who runs FIND_VALUE for a known hash learns which origin operator attests it, one namespace that it belongs to, and the time and TTL of that attestation | T2 | [022](022-content-discovery.md#origin-attestation) § Origin Attestation |

### Analysis by Adversary Tier

#### T1: Passive Network Observer

Sees on-chain state and QUIC connection metadata. Key concern: whether aggregating signals reveals more than any single one.

##### Content demand patterns (P-08)

BLAKE3 hashes are deterministic global identifiers; repeated requests are correlatable across observers. The protocol publishes no `popular_hashes` demand signal; demand is observable only through DHT FIND_VALUE traffic to the K closest nodes for a hash and cache-miss timing inferences. Intrinsic to a content-addressed network; not eliminable without protocol-level mixing.

##### Payment and identity linkability (P-02, P-03, P-07)

On-chain data links a pool owner to the provider addresses it pays, revealed at redemption; settlement amounts make node revenue computable; `CapacityBond` publishes node identity and network location. Inherent to the accountability model — staking and settlement require on-chain identities and state. Acceptable for the PoC (testnet, no real economic value).

##### Settlement volume leakage (P-22)

At each on-chain redemption the lane's cumulative amount and byte count are public. Delivery is metered in 1 MiB chunks, and a redeemed `chainIndex` is itself public, so the cumulative amount reveals the count of 1 MiB increments. Combined with P-02 (signer/provider address linkage) and public rate information, an observer computes the exact volume between a specific signer-provider pair. Inherent to on-chain settlement — redemption amounts must be public.

##### Protocol fingerprinting (P-04)

ALPN negotiation in the QUIC TLS ClientHello reveals whether a connection carries `cdn/client/v1` streams or `cdn/dht/v1` queries, letting a network observer classify connections by type. A discovery open and a paid stream share `cdn/client/v1`, so the ALPN does not tell them apart. Standard for any QUIC multi-protocol system; not a significant concern — the protocols are not secret.

##### Registry enumeration (P-12)

The on-chain registry active set is public, so a T1 observer reads it without connecting to any node. See [§ Network enumeration (P-12, P-13)](#network-enumeration-p-12-p-13) for the full analysis.

#### T2: Active Protocol Participant

Can send discovery opens to nodes, read the on-chain registry, and observe responses to its own interactions. Primary added concern: content access pattern leakage.

##### Discovery-open content leakage (P-09, P-10, P-11)

A discovery open for a hash tells every candidate what is requested. It also names the pool and carries the client binding, so every candidate learns the payer address, as the delivering node does. Cache-miss discovery opens are visible to the targeted DHT-candidate set, and DHT FIND_VALUE queries to keyspace-close nodes — both reveal regionally uncommon or newly requested content. The winner's discovery open becomes its paid stream, so discovery and delivery are trivially linked. Discovery answers are explicitly public ([ADR 005 § Discovery opens](005-protocol.md#discovery-opens)): node identities are in a public registry, content availability is discoverable via discovery opens, pricing is revealed by design. The delivering node fundamentally must know the requested hash; mitigating leakage to non-delivering nodes (e.g., dummy discovery opens) adds bandwidth cost without changing the fundamental property.

##### Network enumeration (P-12, P-13)

The on-chain registry active set is enumerable; reading it reveals all active nodes' identities and self-reported regions. Combined with `multiaddrs` from the same registry, this enables geolocation. Inherent to any system where nodes must be discoverable to serve content.

##### `slash_sig` as content inventory proof (P-23)

Every `StreamResponse` carries a `slash_sig` — an EIP-712 secp256k1 signature binding the node's Ethereum address to specific content hashes and timestamps ([ADR 005 § Signer binding](005-protocol.md#signer-binding)). The answer to a discovery open is a `StreamResponse` too. Any T2 participant that sends discovery opens collects non-repudiable, EVM-verifiable, individually attributable proof that the node committed to serving (or refused) specific content at a specific time. Systematic discovery opens build a cryptographic per-node content inventory keyed by Ethereum address. Inherent to signed answers — `slash_sig` exists to make node commitments attributable. A requester recovers it before it trusts a quote, and a pull receipt keeps it as portable evidence of a serve ([Appendix: Pull Receipts](appendix-pull-receipts.md#what-a-receipt-proves)). Removing it leaves a quote that binds the node to nothing.

##### Cross-session client tracking (P-21)

A persistent client NodeId exposes a stable transport identifier to every node the client connects to. Rotating it does not provide cross-session unlinkability: the serving node must resolve the signer's Ethereum address to attribute vouchers and redeem them on-chain, and a T1 observer correlates that same address across public payment-pool activity (P-02). Rotation only obscures the client from T2 participants that learn a NodeId by completing a handshake and get no request from it. A candidate that gets a discovery open sees the pool and the payer address, as the serving node does. The residual is already leaked as content demand through the accepted P-08 and P-10 surfaces. Details in [§ Client NodeId Rotation (P-21)](#client-nodeid-rotation-p-21).

#### T3: Infrastructure Operator

Privileged view of specific interaction channels.

##### RPC provider (P-15)

Observes all on-chain queries: registry lookups, blacklist polling, rate-bounds checks — revealing which nodes a client or node is interested in. A client and a node use a governance-chain RPC and a payment-chain RPC, and each provider sees only the queries for its own chain. [architecture.md](architecture.md#architecture-overview) trust assumptions already document this and plan multi-source bootstrap for production.

##### Relay (P-17)

iroh relays see source/destination IP pairs and connection timing for relayed connections (~10% of conditions). Relays cannot inspect content (all traffic is E2E encrypted). Standard for any relay-based NAT traversal system.

#### T4: Compromised Endpoint

Yields secrets specific to that endpoint.

##### Client key material (P-18)

PoC stores the iroh secret key unencrypted at `~/.decdn/iroh_key` ([ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model)); file access yields the client's network identity. Production uses the platform keychain.

### Disposition Summary

| ID | Surface | Disposition | Rationale | Milestone |
|----|---------|-------------|-----------|-----------|
| P-02 | On-chain payment pools | Accept | Required for settlement | — |
| P-03 | On-chain staking registry | Accept | Required for node accountability and discovery | — |
| P-04 | ALPN protocol identification | Accept | Standard QUIC behavior; protocols are not secret | — |
| P-07 | Node earnings inference | Accept | Inherent to on-chain settlement; no mitigation without breaking dispute model | — |
| P-08 | BLAKE3 global identifier | Accept | Fundamental to content-addressed delivery; no alternative without breaking the architecture | — |
| P-09 | Discovery-open content leakage | Accept | Discovery answers are public by design ([ADR 005 § Discovery opens](005-protocol.md#discovery-opens)); delivering node must know the hash | — |
| P-10 | Cache miss detection | Accept | Inherent to discovery opens and DHT lookups for cache-miss pulls | — |
| P-11 | Discovery-to-delivery correlation | Accept | The paid stream reuses the discovery stream to save a round trip; the attacker already sees the discovery open | — |
| P-12 | Registry node enumeration | Accept | Inherent to any system with discoverable nodes | — |
| P-13 | GeoIP inference | Accept | Self-reported region is intentionally public for client selection | — |
| P-15 | RPC provider visibility | Mitigate | Operational guidance reduces single-provider trust | Pre-mainnet |
| P-17 | Relay connection metadata | Accept | Standard relay behavior; traffic is E2E encrypted | — |
| P-18 | Unencrypted iroh key | Mitigate | Already planned: platform keychain in production | Pre-mainnet |
| P-21 | Permanent client NodeId | Accept | Serving nodes and T1 observers link sessions by the stable Ethereum address the payment model requires (P-02); the residual benefit against participants that get no request is already leaked through accepted P-08 and P-10 | — |
| P-22 | Settlement volume leakage | Accept | Inherent to on-chain settlement; settlement amount must be public for dispute resolution | — |
| P-23 | `slash_sig` content inventory | Accept | Required for attributable quotes and pull receipts; removing `slash_sig` leaves the quote unattributed | — |
| P-24 | Origin attestation in DHT records | Accept | Required so that every cached copy traces to a vetted origin. The attestation names no pool, payer or client, and an origin's seats are public already. The link from a hash to one of its namespaces is new public information. It is accepted: the namespace is the publisher's own, and the publisher chooses to publish the hash through it | — |

### Candidate Mitigations

#### Client NodeId Rotation (P-21)

**Available capability:** [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) documents production rotation: generate a new iroh key, reconnect, and sign a fresh `BindNodeId` with the same Ethereum key. Open payment pools remain valid because they are keyed by `(owner_ethereum_address, poolNonce)`, not by NodeId. Rotation is not supported for PoC — deleting the key file and restarting generates a new identity, but that is a side effect, not a rotation procedure.

**Privacy effect:** Rotation replaces the stable transport identifier, obscuring cross-session correlation by T2 participants that learn a NodeId by completing a handshake and never see a `StreamRequest`. A discovery open is a `StreamRequest`, so every candidate that gets one sees the pool and the payer address. It does not help against a T1 passive observer: P-21 is a T2 surface because the iroh identity is exchanged under TLS handshake encryption, leaving only the ALPN visible in the clear (P-04), so a passive observer correlates by IP regardless of rotation.

**Limitation:** Rotation does not provide cross-session unlinkability. Every request carries `pool_id = keccak256(owner, poolNonce)` ([ADR 003](003-payments.md#adr-003-payment-model)) in the base `StreamRequest`, and the serving node must resolve the signer's Ethereum address to attribute vouchers and redeem them on-chain — from the on-chain binding for a registered client, or from the `ethereum_address` field in `StreamRequestExt` for an off-chain one ([ADR 005](005-protocol.md#adr-005-wire-protocol)). A T1 observer then correlates that address across public payment-pool activity. This limitation is scale-independent because every serving relationship exposes the stable payment identity regardless of node count.

**Disposition:** Accept. P-02 is what defeats rotation — the payment model requires a stable client address visible to both the serving node and any on-chain observer — and the remaining benefit against participants that get no request is already leaked as content demand through the accepted P-08 and P-10 surfaces. Production rotation remains an optional identity-lifecycle capability, not a privacy roadmap commitment.

#### Operational RPC Guidance (P-15)

**Proposal:** Document a production recommendation to use multiple independent RPC providers or a self-hosted node for on-chain queries. Already implied by [architecture.md](architecture.md#architecture-overview) § Trust Assumptions (multi-source bootstrap) but should be explicit as a privacy recommendation, not just reliability.

**Effort:** Minimal. Documentation change in [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) and [architecture.md](architecture.md#architecture-overview).

#### Client iroh Key Encryption (P-18)

**Current state:** [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) already specifies platform keychain for production. No additional design needed — already planned.

#### Dummy Discovery Opens (Not Recommended)

**Purpose:** Obscure requested content by mixing real discovery opens with decoy opens for random hashes.

**Assessment:** Discovery answers are explicitly public ([ADR 005 § Discovery opens](005-protocol.md#discovery-opens)). The delivering node must know the requested hash — dummy opens only hide requests from non-delivering nodes, and each open still names the pool and the payer address. Benefit is marginal relative to cost: 3-5x more open traffic, added latency (wait for dummy answers or accept a timing side-channel), more node CPU. Opens are free (no USDC cost), but bandwidth and compute costs are non-trivial at scale.

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
| 3 | Dummy discovery opens ([§ Dummy Discovery Opens (Not Recommended)](#dummy-discovery-opens-not-recommended)) | Post-mainnet | Medium | Low — discovery answers are public by design |
| 4 | Payment pool mixing ([§ Payment Pool Mixing (Not Recommended)](#payment-pool-mixing-not-recommended)) | Post-mainnet | High | Medium — requires regulatory analysis first |

## Consequences

### Positive

- Single reference for the protocol's privacy posture, enabling informed tradeoffs before implementation
- Explicit disposition for every privacy surface prevents implicit acceptance of unanalyzed risks
- Prioritized roadmap focuses engineering effort on highest-impact, lowest-cost items first
- Adversary-tier framing maps threats to real-world actors, avoiding over- or under-engineering
- Distinguishes fundamental protocol limitations (delivering nodes learn requested hashes and stable payment identities), optional defense-in-depth capabilities (production NodeId rotation), and prioritized operational mitigations (RPC provider trust)

### Negative

- Must be kept in sync as other ADRs evolve — any new protocol feature or message must be evaluated against [§ Privacy Surface Inventory](#privacy-surface-inventory)
- Some "accept" dispositions may need revisiting as the threat landscape or regulatory requirements change, or the network scales beyond PoC
- Does not cover application-layer privacy (content provider's app server data, subscriber analytics) — an explicit scope boundary, not an oversight
- The adversary model assumes rational actors; state-level adversaries with traffic analysis may extract more from T1-level data than this analysis suggests

## References

- [ADR 001 — Network Topology and Peer Mesh](001-network.md#adr-001-network-topology-and-peer-mesh): registry enumeration, discovery opens to DHT candidates
- [ADR 002 — Content Addressing](002-content-addressing.md#adr-002-content-addressing): BLAKE3 as global content identifier
- [ADR 003 — Payment Model](003-payments.md#adr-003-payment-model): payment pool on-chain visibility, discovery-open load limits
- [ADR 005 — Wire Protocol](005-protocol.md#adr-005-wire-protocol): discovery-open publicity statement, ALPN definitions, `StreamResponse` signature
- [ADR 012 — Client Architecture, Bootstrap, and Trust Model](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model): client NodeId, key storage, rotation
- [Architecture Overview](architecture.md#architecture-overview): trust assumptions (NTP, RPC provider, relay), system diagram
