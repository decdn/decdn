# Architecture Overview

**Date:** 2026-03-28
**Status:** Living document — updated as ADRs are added or revised

## What This Is

A decentralized CDN with two participant roles:

- **Nodes** (providers) cache and serve content. They bond TOKEN proportional to declared capacity (`bond = k × Mbps^α` per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)) to participate in the peer mesh and compete on price and latency. Some nodes are configured with an origin backend (S3, NFS, local disk) making them the canonical source for specific content. The **cache role** is permissionless — any bonded operator may pull cached blobs from authorized origins and re-serve them. The **origin role** is DAO-governed: governance vets a publisher wallet once and the vetted publisher then seats its own operators per namespace, while namespace 0 (content published without a namespace) has no authorized origins. No external origin URL is ever exposed.

- **Clients** consume content. They pay nodes per MB via off-chain vouchers backed by a shared payment pool on a payment chain. The protocol has two tiers: the governance chain (Ethereum L1) and any number of payment chains ([ADR 016 § Deployment Tiers](016-contract-interactions.md#deployment-tiers)). In multi-source pulls, they pay only for verified bytes, not frames received beyond a stolen range split.

## System Diagram

```mermaid
graph TD
    subgraph Nodes
        N1["Node (cached)"]
        N2["Node (origin-backed)"]
        N3["Node (cached)"]
    end

    subgraph Clients
        C1[Client]
        C2[Client]
        C3[Client]
    end

    subgraph "Provider Infrastructure (external)"
        S3[("Hidden Origin Backend<br/>S3 / R2 / B2")]
    end

    N1 <-->|"cdn/client/v1<br/>paid per-MB"| N2
    N2 <-->|"cdn/client/v1<br/>paid per-MB"| N3
    N1 <-->|"cdn/client/v1<br/>paid per-MB"| N3

    C1 -->|"cdn/client/v1<br/>paid per-MB"| N1
    C2 -->|"cdn/client/v1<br/>paid per-MB"| N2
    C3 -->|"cdn/client/v1<br/>paid per-MB"| N3

    N2 -.->|opaque fetch| S3
```

Clients send discovery opens — `cdn/client/v1` streams with no voucher — to candidate nodes from the on-chain registry (or skip them with a fresh persisted peer store), pick the nearest by path RTT, then quote and coverage ([ADR 037](037-regional-proxy-warming.md#client-selection-policy-latency-driven-proxy-preference)), stream from the winner over that same `cdn/client/v1` stream, and pay via off-chain payment vouchers (denominated in the payment token). On a cache miss, a node performs a DHT FIND_VALUE lookup (`cdn/dht/v1`), sends the returned candidates discovery opens, selects the best, and pulls via `cdn/client/v1` (paid). When DHT returns no providers, or the holders that answer do not cover every block of the blob, the on-chain origin directory ([ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale)) is the deterministic last-resort fallback. Every byte delivered — whether client→node or node→node — is paid.

## Reading Order

For readers approaching the protocol top-to-bottom, follow this thematic order rather than the numeric one. Each chapter assumes the previous chapters are read. (See also [`README.md` § Design principles](README.md#design-principles) for the protocol's framing before diving in.)

> **Maintenance note:** the reading-order book PDF in [`README.md` § Reading-order build (book layout)](README.md#reading-order-build-book-layout) lists every ADR explicitly in the same order. Edits to the chapters below — adding, removing, or reordering ADRs — must update that pandoc command in lockstep, or the rendered book will drift from this index.

### Chapter 1 — Foundations

The implementation language, the peer mesh's shape, the content-addressing primitive every other chapter inherits, and the wire-protocol surface that carries paid delivery.

1. [ADR 000 — Language and Core Networking Stack](000-language.md#adr-000-language-and-core-networking-stack)
2. [ADR 001 — Network Topology and Peer Mesh](001-network.md#adr-001-network-topology-and-peer-mesh)
3. [ADR 002 — Content Addressing](002-content-addressing.md#adr-002-content-addressing)
4. [ADR 005 — Wire Protocol](005-protocol.md#adr-005-wire-protocol)
5. [ADR 038 — Bao Verified-Range Streaming on cdn/client/v1](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1)

### Chapter 2 — Discovery

How a node finds the right peer for a given hash. The DHT is the primary mechanism; a client picks its node from the registry by measured RTT ([ADR 037](037-regional-proxy-warming.md#client-selection-policy-latency-driven-proxy-preference)); proxy warming bootstraps the first regional copy when discovery returns only distant holders.

1. [ADR 022 — Content Discovery at Scale (DHT)](022-content-discovery.md#adr-022--content-discovery-at-scale)
2. [ADR 037 — Latency-Driven Proxy Warming for Regional Locality](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality)
3. [ADR 039 — Multi-Source Parallel Fetch Scheduling on cdn/client/v1](039-multi-source-parallel-fetch.md#adr-039-multi-source-parallel-fetch-scheduling-on-cdnclientv1)

### Chapter 3 — Payments

Off-chain vouchers for per-MB delivery, metered between signatures by a hash chain, backed by a shared payment pool on a payment chain that a client opens once and redeems against per node. Client-side architecture and smart-wallet support are included here because client trust boundaries and key management hang off the payment path.

1. [ADR 003 — Payment Model](003-payments.md#adr-003-payment-model)
2. [ADR 012 — Client Architecture, Bootstrap, and Trust Model](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model)
3. [ADR 024 — Account Abstraction and Safe Smart Wallet Support](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support)

### Chapter 4 — Tokenomics & incentives

The economic model that ties the protocol together. [ADR 026](026-tokenomics.md#adr-026-tokenomics) is the canonical umbrella (1B fixed supply, the governance-chain `FeeRouter` split of each sweep into a rebate and a treasury remainder, the `CapacityBond` lock-to-capacity curve `bond = k × Mbps^α`, no ongoing service emission, the [§ App Incentives](026-tokenomics.md#app-incentives) demand-side bucket, operator-only governance with `age_ramp` (served-bytes-weighted under [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)), bootstrap, and governable-parameter bounds). The launch contract surface is forward-compatible (additive integration via standard `AccessControl` role grants per [ADR 016 § Access Control Matrix](016-contract-interactions.md#access-control-matrix)) so future economic-layer products can land as additive top-level contracts without changing existing contracts.

1. [ADR 026 — Tokenomics](026-tokenomics.md#adr-026-tokenomics) (canonical umbrella)

### Chapter 5 — Verification & enforcement

How the network responds to misbehavior. Each client verifies bytes and signatures itself ([ADR 005 § Signer binding](005-protocol.md#signer-binding)). Each node scores its peers locally. Governance blacklists content and can eject a non-compliant operator.

1. [ADR 008 — Reputation System](008-reputation.md#adr-008-reputation-system)
2. [ADR 011 — Content Takedown and Hash Blacklisting](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)
3. [ADR 030 — Node Region Self-Attestation](030-node-region-self-attestation.md#adr-030-node-region-self-attestation)

### Chapter 6 — Governance & contracts

The governance model that sets the parameters earlier chapters consume, and the cross-contract interaction map that consolidates the on-chain surface.

1. [ADR 009 — Governance Model](009-governance.md#adr-009-governance-model)
2. [ADR 036 — Served-Bytes Voting Weight](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) — defines the canonical DAO voting-weight formula (served bytes from `FeeRouter.bytesPerEpoch` × `age_ramp`) that [ADR 009 § Production](009-governance.md#production-operator-weighted-dao-governance) and [ADR 026 § Governance](026-tokenomics.md#governance) reference; adds the governable `windowEpochs`.
3. [ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md#adr-016-smart-contract-interaction-model)

### Chapter 7 — Operations

The operator-facing onboarding flow that takes a bare server through staking, registration, and accepting paid delivery, the node-local cache policy that governs what an onboarded node keeps on disk, and the node-local economics gate that decides when relaying a cache miss is worth it.

1. [ADR 019 — Node Onboarding and Bootstrapping Flow](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow)
2. [ADR 040 — Pluggable Cache Admission and Eviction Policies](040-cache-policy.md#adr-040-pluggable-cache-admission-and-eviction-policies)
3. [ADR 041 — Refuse-to-Serve Economics](041-refuse-to-serve.md#adr-041-refuse-to-serve-economics)

### Chapter 8 — Supporting infrastructure

Wire framing, compatible in-version evolution, signed-field freezing, and the privacy-surface inventory. Both apply across the chapters above.

1. [ADR 013 — Schema Evolution](013-schema-evolution.md#adr-013-schema-evolution)
2. [ADR 017 — Privacy Analysis](017-privacy.md#adr-017-privacy-analysis)

The numeric per-ADR index below stays as the canonical reference.

### Appendices — Reference Patterns

Appendices document patterns, reference implementations, and operational guidance built **on top of** the protocol. They are not part of the core spec — alternative implementations are acceptable. See [`README.md` § Decision-record context](README.md#decision-record-context) for the ADR-vs-appendix distinction.

1. [Directory Bundles](appendix-bundles.md#appendix-directory-bundles) — publisher-side convenience for grouping content-addressed blobs into a single JSON manifest, and the `decdn:` URI that names a blob, a bundle, or one bundle entry; nodes deliver individual hashes and do not require bundle support
2. [Observability and Metrics](appendix-observability.md#appendix-observability-and-metrics) — recommended metric naming, registry, and alert thresholds
3. [Governance chain and payment chain admission](appendix-l2-deployment.md#appendix-governance-chain-and-payment-chain-admission) — Ethereum L1 as the governance chain, the four-rule admission test for a payment chain, and the sweep cost gates
4. [PoC/Production Seam Architecture (Rust)](appendix-poc-production-seams.md#appendix-pocproduction-seam-architecture-rust-implementation) — leaf-crate principle, wiring-layer mode selection, contract surface is not a PoC/production seam
5. [deCDN Binaries — `decdn-node` + `decdn` Split](appendix-binaries.md#appendix-decdn-binaries--decdn-node--decdn-split) — rationale for the dockerd-style split into the long-lived cache-node daemon (`decdn-node`) and the one-shot operator/publisher CLI (`decdn`)
6. [Local Admin HTTP Surface](appendix-local-admin-http.md#appendix-local-admin-http-surface) — loopback-bound admin API for operator runbook automation
7. [Operator Key Rotation Runbook](appendix-operator-key-rotation.md#appendix-operator-key-rotation-runbook) — sequenced procedure for rotating the operator's two keys — the iroh node-key and the Ethereum signing key — via `bindNodeId`, deregister-and-re-stake, or a Safe owner swap
8. [Operator Protocol-Upgrade Runbook](appendix-operator-upgrade-path.md#appendix-operator-protocol-upgrade-runbook) — tier-independent safe-restart drain procedure plus Tier 1/2 operator checklists for compatible in-version releases; Tier 3 migrations are defined with the concrete breaking change
9. [Public HTTP Gateway](appendix-gateway.md#appendix-public-http-gateway) — a paying client reached over HTTP: the hosts it serves, the capabilities that pay for it with no wallet on its machine (each scoped to any content or to a pool owner's signed namespaces), the namespace pointer that gives a namespace a stable host, ranges, and its metrics
10. [Pull Receipts](appendix-pull-receipts.md#appendix-pull-receipts) — the file `decdn get --receipt` writes from the node responses and vouchers a pull already exchanged: what `decdn receipt verify` checks offline, and what a receipt proves and does not prove

## Architectural Decisions

Numeric per-ADR index.

- **[ADR 000 — Language and Core Networking Stack](000-language.md#adr-000-language-and-core-networking-stack)** — Rust + iroh (1.0).
- **[ADR 001 — Network Topology and Peer Mesh](001-network.md#adr-001-network-topology-and-peer-mesh)** — Flat peer mesh; the on-chain `CapacityBond` registry active set for node discovery; `cdn/dht/v1` (Kademlia subset) for content discovery, with the on-chain origin directory ([ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale)) as the deterministic last-resort fallback when DHT returns no providers or the answering holders do not cover every block.
- **[ADR 002 — Content Addressing](002-content-addressing.md#adr-002-content-addressing)** — BLAKE3 content-addressed blobs. Node backends are opaque to the network.
- **[ADR 003 — Payment Model](003-payments.md#adr-003-payment-model)** — Off-chain vouchers backed by a shared payment pool on a payment chain (native USDC, fixed per pool deployment): one deposit, many capped signers, node-addressed vouchers, per-lane redemption. A voucher's cumulative `amount` is the settlement anchor, and an optional PayWord hash chain advances it one 1 MiB chunk per released preimage with no signature. Market-driven rates within governance-set bounds.
- **[ADR 005 — Wire Protocol](005-protocol.md#adr-005-wire-protocol)** — Two core protocols (ALPN-negotiated): `cdn/client/v1`, `cdn/dht/v1`. `cdn/client/v1` covers all discovery and all paid delivery: a discovery open is a stream that carries no voucher, and the requester pays on the winner's stream and cancels the rest. A node pins the blob of an open stream and runs funding admission at the first voucher. A node refuses a stream with one of three classes (`NotFound`, `Declined`, `Unfunded`). Each refusal scopes to that node for the current fetch, so no single node's refusal ends a fetch; on a refusal, a client adds funds only when no node serves at its current funding, and again only after it verifies new bytes.
- **[ADR 008 — Reputation System](008-reputation.md#adr-008-reputation-system)** — Local per-peer interaction-weighted scoring; no cross-node propagation.
- **[ADR 009 — Governance Model](009-governance.md#adr-009-governance-model)** — Admin key for PoC; bootstrap multisig phase post-launch; transition to served-bytes-weighted Governor (`FeeRouter.bytesInWindow × age_ramp` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)) + Timelock via a one-shot transition the multisig executes when the operator set is broad enough.
- **[ADR 036 — Served-Bytes Voting Weight](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)** — Promotes `FeeRouter.bytesPerEpoch` from analytics-only to governance-canonical; vote weight = served-bytes trailing-window sum × `age_ramp`, capped per-operator at 10% of bytes-weighted total (`voteCapBps`).
- **[ADR 011 — Content Takedown and Hash Blacklisting](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)** — Governance-controlled on-chain hash blacklist with regional bodies and emergency fast-path; a wrongful entry comes off via the ordinary removal path ([§ Removing a Wrongful Entry](011-content-takedown.md#removing-a-wrongful-entry)).
- **[ADR 012 — Client Architecture, Bootstrap, and Trust Model](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model)** — Client bootstrap, key management, identity lifecycle, and trust boundary.
- **[ADR 013 — Schema Evolution](013-schema-evolution.md#adr-013-schema-evolution)** — Varint-length framing and bounds, protocol-enum discipline, two-phase deserialization, three evolution tiers, and signed-field freezing. A concrete Tier 3 break defines its own migration.
- **[ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md#adr-016-smart-contract-interaction-model)** — Cross-contract call graph, fund custody, access control matrix, and reentrancy analysis.
- **[ADR 017 — Privacy Analysis](017-privacy.md#adr-017-privacy-analysis)** — Unified privacy surface inventory, adversary model, and mitigation roadmap.
- **[ADR 019 — Node Onboarding and Bootstrapping Flow](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow)** — End-to-end procedure from bare server to actively accepting paid delivery; five sequential onboarding phases.
- **[ADR 022 — Content Discovery at Scale](022-content-discovery.md#adr-022--content-discovery-at-scale)** — `cdn/dht/v1` Kademlia subset for content discovery; realized demand is served reactively via cache-miss pull-through per [ADR 037](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality). No discovery fees. Every record carries an origin attestation: a seated origin's signed statement that it serves the hash in a namespace, with a TTL of 1 h to 24 h that the origin picks. A node serves and advertises a hash only while it holds a live attestation for it, and refreshes the attestation from itself, the DHT or a seated origin before it expires.
- **[ADR 037 — Latency-Driven Proxy Warming for Regional Locality](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality)** — Bootstraps the first regional cache copy at selection time: a client finding only distant holders routes its paid request through a measured-nearby non-holder, which fills it by credit-paced pull-through (the pull draws only what the requesting stream has paid for) and caches the blob. Proxy selection ranks by measured RTT only (region-spoofing irrelevant); speculative spend bounded by the same stream credit that the downstream serve loop debits. No wire change.
- **[ADR 024 — Account Abstraction and Safe Smart Wallet Support](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support)** — Universal `SignatureChecker` across all contracts, so any ERC-1271 smart account works on-chain without a retrofit; the EOA keystore is the documented default wallet and Safe is supported rather than recommended; off-chain, every signature is EOA-recovered, so a smart-account client is served through a capability-delegated EOA signer with no residual, and a node serving from a smart account (operator custody while serving) is unimplemented — the obvious ERC-1271 route is unsound for signatures that must stay verifiable after payment.
- **[ADR 026 — Tokenomics](026-tokenomics.md#adr-026-tokenomics)** — Canonical economic-model umbrella: 1B fixed supply, the governance-chain `FeeRouter` split of each sweep (rebate, treasury), the `CapacityBond` lock-to-capacity curve (no ongoing service emission), App Incentives demand-side bucket, operator-only served-bytes-weighted governance per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight), bootstrap, and governable-parameter bounds.
- **[ADR 030 — Node Region Self-Attestation](030-node-region-self-attestation.md#adr-030-node-region-self-attestation)** — Region claims (`regionHint` on `CapacityBond`) are accepted at face value; the IP-geolocation oracle / third-party attestation path is explicitly rejected. Reactive blacklist-scope flipping is closed by a 7-day `regionLastChanged` stability window on `CapacityBond` (governable `[3d, 30d]`; [ADR 011 § Regional Scope](011-content-takedown.md#regional-scope)). Latency-vs.-claim reputation penalty from [ADR 001 § Consequences](001-network.md#consequences) is the canonical soft mitigation for residual pre-positioned misdeclaration.

## Key Invariants

- No external origin URL exists — content enters the network through origin-backed nodes whose backends are hidden
- A node cannot deliver paid content without being reachable via iroh NodeId
- A node cannot earn without delivering verifiable bytes — BLAKE3 hash mismatch voids payment
- A node cannot join the peer mesh without bonding — prevents free-riders and ties each slot to a bond proportional to declared capacity
- A node cannot register without bonding — `CapacityBond` enforces `bond >= bond_required(declared_capacity_Mbps)` via the `bond = k × Mbps^α` curve before accepting a `register` call
- A node cannot register without binding — `registerNode` atomically writes the NodeId-to-address mapping via EIP-712 signature, so every signed response of an active node is attributable to its operator address
- A node cannot register a NodeId it does not control — `registerNode` verifies an ed25519 signature proving ownership of the NodeId's private key, preventing squatting ([ADR 003 § NodeId Ownership Verification](003-payments.md#nodeid-ownership-verification))
- A payment pool amortizes on-chain costs across every node and signer it backs; per-MB payments are off-chain vouchers, redeemed per node
- Safety bounds on all governable parameters are hardcoded — governance cannot set fees to 100% or stake to zero (see [ADR 009](009-governance.md#adr-009-governance-model))
- A node stops serving a blacklisted hash within the compliance window — governance can blacklist and eject an operator that does not comply (see [ADR 011 § Hash Evasion and Origin Blacklisting](011-content-takedown.md#hash-evasion-and-origin-blacklisting))

## Threat Model

This is the coverage map for adversarial behavior. Each ADR reasons about the attacks against its own subsystem; this table names every adversary class and points to the one section that owns the defense. Add the attack here when you write it, and link to that home instead of re-deriving the argument.

Two things shape how a defense reads in its home ADR. First: most defenses are **node-local policy**, not protocol rules. A node bounds its own exposure because doing so is its rational optimum — the ADR's job is to prove that local self-interest is sufficient, not to pin the knob. Protocol-enforced defenses (signatures, on-chain checks) are marked below; everything else is node policy and is tunable per operator. Second: the durable content is the **invariant** a defense preserves, not the mechanism that preserves it. The mechanism (credit window, admission cap, rate-bucket size) is local and tunable and lives in code and config; the ADR fixes the property.

**Free-riding or resource-exhausting client** — wants bytes without paying, or wants to burn node resources.

| Attack | Bounded by | Home |
|---|---|---|
| Voucher withholding | Self-enforcing per-stream credit: the node sends a frame only against payment already on the stream, so its unpaid exposure is zero; a payer's pay-ahead is capped by its own window | [ADR 003 § Voucher withholding](003-payments.md#voucher-withholding) |
| Pool oversubscription | Contract pays `min(desired, capRoom, remaining)`; refundable floor `M` (protocol) | [ADR 003 § Pool oversubscription](003-payments.md#pool-oversubscription-one-deposit-backs-many-nodes) |
| Owner reclaims before a node redeems | Grace window + periodic redeem sweep capped well inside it (protocol + node policy) | [ADR 003 § Owner reclaims before a node redeems](003-payments.md#owner-reclaims-before-a-node-redeems) |
| Discovery-open load / resource exhaustion | Layered per-peer + global token bucket; no upstream work without `pull_through`; a per-connection cap on unadmitted streams (node policy) | [ADR 003 § Discovery-open load](003-payments.md#discovery-open-load) |
| Unpaid request flood (signatures, origin `{H}.sizeproof` reads, chain reads) | Layered per-peer + global token bucket before the request is read; a node-wide cap on admission chain reads (node policy) | [ADR 005 § Request rate limiting](005-protocol.md#request-rate-limiting) |

**Malicious serving node** — wants payment without honest service, or to cheat pricing, region, or takedown.

| Attack | Bounded by | Home |
|---|---|---|
| Data withholding | Self-enforcing; client never pays past acknowledged bytes, resumes elsewhere | [ADR 003 § Data withholding](003-payments.md#data-withholding) |
| Corrupted delivery | Progressive BLAKE3 verification voids payment for a corrupt window (protocol) | [ADR 003 § Corrupted delivery](003-payments.md#corrupted-delivery) |
| Rate bait-and-switch | Self-enforced: the client sees the signed quote before it pays and signs the price into each voucher (protocol) | [ADR 003 § Rate bait-and-switch](003-payments.md#rate-bait-and-switch) |
| Content withholding (advertise, refuse to serve) | Reputation penalty only; publishers seat redundant operators | [ADR 003 § Content withholding](003-payments.md#content-withholding) |
| Serving a blacklisted hash | Honest nodes refuse it after the compliance window; governance can blacklist and eject the operator (protocol) | [ADR 011 § Hash Evasion and Origin Blacklisting](011-content-takedown.md#hash-evasion-and-origin-blacklisting) |
| Region mis-attestation | Self-attestation is canonical; a latency-vs-claim reputation penalty checks it | [ADR 030 § Self-attestation is canonical](030-node-region-self-attestation.md#self-attestation-is-canonical) |
| Third-party forced close / redeeming another's lane | Access control: `closePool` is owner-only, redemption binds `msg.sender` (protocol) | [ADR 003 § Third-party forced close (DoS)](003-payments.md#third-party-forced-close-dos) |

**Network / peer-layer adversary** — wants to poison discovery, isolate a client, or flood the mesh.

| Attack | Bounded by | Home |
|---|---|---|
| Eclipse | Multi-source bootstrap (on-chain registry + DNS seeds) | [ADR 012 § Bootstrap Procedure](012-client.md#bootstrap-procedure) |
| DHT poisoning / flooding | Holder bound to its QUIC NodeId + origin attestation on every record + DHT rate limiting + a routing table of bonded peers only (protocol + node policy) | [ADR 022 § DHT Rate Limiting](022-content-discovery.md#dht-rate-limiting), [§ Routing Table](022-content-discovery.md#routing-table), [§ Origin Attestation](022-content-discovery.md#origin-attestation) |
| Voucher replay | Cumulative watermark pays `0` on a re-submitted voucher (protocol) | [ADR 003 § Replay attack on vouchers](003-payments.md#replay-attack-on-vouchers) |

**Sybil / economic adversary** — wants cheap identities to dominate selection, pricing, or voting weight.

| Attack | Bounded by | Home |
|---|---|---|
| Sybil nodes | Capacity bond per node + selection score + reputation lag | [ADR 003 § Sybil nodes](003-payments.md#sybil-nodes) |
| Rate-fixing cartel | Origin-backed nodes cap price; permissionless cache-only entry disciplines it | [ADR 003 § Rate-fixing cartel](003-payments.md#rate-fixing-cartel) |
| Wash-trading for vote-buying | Fee cut on every self-dealt cycle taxes fabricated volume (protocol) | [ADR 036 § Threat Model](036-served-bytes-voting-weight.md#threat-model) |

**Governance adversary** — wants to capture parameters.

| Attack | Bounded by | Home |
|---|---|---|
| Governance capture | Hardcoded safety bounds on every governable parameter (protocol) | [ADR 009 § Governable Parameters with Safety Bounds](009-governance.md#governable-parameters-with-safety-bounds) |

**Privacy adversary** — wants to deanonymize clients or nodes, or link their activity.

| Attack | Bounded by | Home |
|---|---|---|
| Traffic analysis, endpoint linkage (tiered T1–T4) | Adversary-tier analysis + prioritized mitigations | [ADR 017 § Analysis by Adversary Tier](017-privacy.md#analysis-by-adversary-tier) |

Attacks the protocol does **not** defend against — where it relies on an outside assumption instead — are listed under Trust Assumptions below.

## Trust Assumptions

The system relies on several infrastructure-level assumptions beyond the cryptographic guarantees verified on-chain or in-protocol. [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) documents the client-specific trust boundary (verified / trusted / not trusted); this section covers system-wide assumptions that span multiple components.

- **RPC provider honesty, per chain.** Nodes and clients trust their governance-chain RPC provider for registry queries and blacklist polling, and their payment-chain RPC provider for pool reads. A malicious payment-chain RPC provider could censor a node's redemption transactions so it misses the grace window and forfeits outstanding vouchers (the same force-inclusion path covers this — see [ADR 003 § L2 sequencer censorship](003-payments.md#l2-sequencer-censorship)), or return a fabricated node list to eclipse a client. Mitigation: multi-source bootstrap ([ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) Option B) and multiple independent RPC providers.

- **Encrypted transport integrity for voucher confidentiality.** Vouchers are bearer instruments — a leaked voucher is valid regardless of how it was obtained. The system assumes vouchers only traverse encrypted authenticated channels between the relevant parties: client↔node and node↔node cache-miss pulls. Mitigation: QUIC/TLS provides in-transit encryption on all these links; vouchers are never logged or persisted in plaintext. Endpoint compromise or debug output leaking vouchers remains an operational risk.

- **Censorship window, per payment chain.** The redemption grace window assumes that a node's `redeem` reaches the chain within that chain's censorship window ([ADR 003 § L2 sequencer censorship](003-payments.md#l2-sequencer-censorship)). If a sequencer or a group of validators censors it beyond this window, the owner could reclaim before the node is paid. Mitigation: each pool's `disputeWindow` is larger than its chain's censorship window, so a payment chain whose window exceeds the grace window is not admitted ([appendix](appendix-l2-deployment.md#appendix-governance-chain-and-payment-chain-admission)).

- **Payment-token behavior stability.** The payment token is native USDC, fixed per pool deployment as an immutable constructor argument. USDC is a standard ERC-20 (no fee-on-transfer, rebase, or transfer hooks). A Circle-side change to USDC semantics or an address/contract freeze is outside protocol control and is the accepted counterparty risk noted in [ADR 003](003-payments.md#adr-003-payment-model).

- **iroh relay availability.** iroh relays are stateless servers that broker NAT traversal and relay encrypted traffic as a fallback when direct peer-to-peer connections fail (~10% of networking conditions). Relays are not CDN protocol participants — they cannot inspect, cache, or modify content (all traffic is end-to-end encrypted). The deCDN does not incentivize relay operators: paying relays per-byte would create a perverse incentive to prevent direct connections from forming. Production deployments should self-host dedicated relays as operational infrastructure, funded from protocol treasury or node staking fees — not as an incentivized network role. If direct-connection success rates drop below ~85%, investigate NAT traversal improvements before considering relay incentivization.

## Origin Backends

Origin-backed nodes hold the canonical bytes and are pulled only on cache miss; whether an operator is *recognized* as origin is governed on-chain via `OriginAssignment`, where governance vets the publisher wallet and the vetted publisher seats its own operators (see [ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority) and [ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces)). Configuring an origin backend locally without DAO authorization simply means the operator's bytes are served as cache. Supported backends — any S3-compatible object store (AWS S3, Cloudflare R2, Backblaze B2, self-hosted MinIO), an NFS mount, or local disk — and how a node maps a hash to its stored object are purely operational: the protocol only requires that a node deliver the correct bytes for a given hash.

## Observability

The daemon serves an OpenMetrics `/metrics` endpoint (loopback, default `:9090`) and a loopback JSON-RPC admin surface (default `:9191`, see [Local Admin HTTP Surface](appendix-local-admin-http.md#appendix-local-admin-http-surface)). ADRs and this overview refer to metrics by informal short names — the stream counters (`streams_active`, `streams_completed`, `streams_failed`), and the payment counters (`vouchers_signed`, `vouchers_received`, `channels_open`). These names are vocabulary, not exported series: the canonical `decdn_`-prefixed registry in [Observability and Metrics](appendix-observability.md#appendix-observability-and-metrics) maps each to its exported form (e.g. `channels_open` → `decdn_lanes_open`), marks which series are shipped vs planned, and pins the naming convention, the mandatory-vs-recommended split, and the alert thresholds.

## Non-Goals

- **DRM / content protection** — blobs are served public-by-default; confidentiality is an app-layer concern.
- **Transcoding / adaptive formats** — content-addressed bytes are delivered verbatim; transforming them would break the hash.
- **Search, discovery, recommendation** — an external/additive layer, not the core protocol.
- **Mobile or web clients** — the reference client is a native binary; other surfaces are downstream.
- **Erasure coding** — blobs are fully replicated across nodes, not erasure-coded.
