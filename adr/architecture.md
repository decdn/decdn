# Architecture Overview

**Date:** 2026-03-28
**Status:** Living document — updated as ADRs are added or revised

## What This Is

A decentralized CDN with two participant roles:

- **Nodes** (providers) cache and serve content. They stake TOKEN to participate in the peer mesh and compete on price and latency. Some nodes are configured with an origin backend (S3, NFS, local disk) making them the canonical source for specific content. The **cache role** is permissionless — any staked operator may pull cached blobs from authorized origins and re-serve them. The **origin role** is DAO-governed for all content — per-namespace `OriginAssignment` set for registered namespaces, and the DAO-maintained default-open allow-list (`OriginAssignment` keyed by `namespaceId == 0`) for unregistered content — with a permissive bootstrap window for default-open serving until the allow-list is first activated (see [ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority), [ADR 011 § Default-open allow-list](011-content-takedown.md#default-open-allow-list), and [ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces)). No external origin URL is ever exposed.
- **Clients** consume content. They pay nodes per MB via off-chain payment channels.

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

    N1 <-.->|"iroh-gossip<br/>NodeAnnounce"| N2
    N2 <-.->|"iroh-gossip<br/>NodeAnnounce"| N3
```

Clients probe candidate nodes, pick the best by the unified selection score (see [ADR 001](001-network.md#node-selection-algorithm) for the full formula), stream over `cdn/client/v1`, and pay via off-chain payment vouchers (denominated in the payment token). On a cache miss, a node performs a DHT FIND_VALUE lookup (`cdn/dht/v1`), probes the returned candidates via `cdn/probe/v1`, selects the best, and pulls via `cdn/client/v1` (paid). When DHT returns no providers, the on-chain origin directory ([ADR 022](022-content-discovery.md)) is the deterministic last-resort fallback. Every byte delivered — whether client→node or node→node — is paid.

## Reading Order

For readers approaching the protocol top-to-bottom, follow this thematic order rather than the numeric one. Each chapter assumes the previous chapters are read. (See also [`README.md` § Design principles](README.md#design-principles) for the protocol's framing before diving in.)

> **Maintenance note:** the reading-order book PDF in [`README.md` § Reading-order build (book layout)](README.md#reading-order-build-book-layout) lists every ADR explicitly in the same order. Edits to the chapters below — adding, removing, or reordering ADRs — must update that pandoc command in lockstep, or the rendered book will drift from this index.

### Chapter 1 — Foundations

The implementation language, the peer mesh's shape, the content-addressing primitive every other chapter inherits, and the wire-protocol surface that carries paid delivery.

1. [ADR 000 — Language and Core Networking Stack](000-language.md)
2. [ADR 001 — Network Topology and Peer Mesh](001-network.md)
3. [ADR 002 — Content Addressing](002-content-addressing.md)
4. [ADR 005 — Wire Protocol](005-protocol.md)

### Chapter 2 — Discovery

How a client or node finds the right peer for a given hash. The DHT is the primary mechanism; 0-RTT is the latency optimization for repeat probes.

1. [ADR 022 — Content Discovery at Scale (DHT)](022-content-discovery.md)
2. [ADR 015 — QUIC 0-RTT Connection Establishment](015-zero-rtt.md)

### Chapter 3 — Payments

Off-chain payment channels for per-MB delivery, with on-chain settlement. Client-side architecture and smart-wallet support are included here because client trust boundaries and key management hang off the payment path.

1. [ADR 003 — Payment Model](003-payments.md)
2. [ADR 012 — Client Architecture, Bootstrap, and Trust Model](012-client.md)
3. [ADR 024 — Account Abstraction and Safe Smart Wallet Support](024-account-abstraction.md)

### Chapter 4 — Tokenomics & incentives

The economic model that ties the protocol together. Two ADRs: [ADR 026](026-gauge-boost-tokenomics.md) (canonical, including the per-operator gauge-share cap that defends against wash-trading) and [ADR 018](018-liquidity-strategy.md) (Balancer V3 POL). The launch contract surface is forward-compatible (additive integration via standard `AccessControl` role grants per [ADR 016 §5](016-contract-interactions.md#5-access-control-matrix)) so future economic-layer products can land as additive top-level contracts without changing existing contracts.

1. [ADR 026 — Gauge-Boost Tokenomics](026-gauge-boost-tokenomics.md) (canonical)
2. [ADR 018 — Liquidity Strategy (Balancer 80/20 POL)](018-liquidity-strategy.md)

### Chapter 5 — Verification & enforcement

How protocol violations are detected, adjudicated, and punished. The slashing schedule lives in [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn); this chapter is the evidence and adjudication path.

1. [ADR 014 — On-Chain Verification for Slashing Evidence](014-on-chain-verification.md)
2. [ADR 008 — Reputation System](008-reputation.md)
3. [ADR 011 — Content Takedown and Hash Blacklisting](011-content-takedown.md)
4. [ADR 028 — Slashing Appeals and Dispute Escalation](028-slashing-appeals.md)
5. [ADR 032 — SafetyReserve Appeal-Surface Contract Surface](032-safety-reserve-appeals-contract.md) — contract-implementation pin for ADR 028's appeal flow
6. [ADR 031 — ContentBlacklist Appeal-Contract Surface](031-content-blacklist-appeals-contract.md) — contract-implementation pin for ADR 011's blacklist-entry appeals
7. [ADR 030 — Node Region Self-Attestation](030-node-region-self-attestation.md)

### Chapter 6 — Governance & contracts

The governance model that sets the parameters earlier chapters consume, and the cross-contract interaction map that consolidates the on-chain surface.

1. [ADR 009 — Governance Model](009-governance.md)
2. [ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md)

### Chapter 7 — Operations

The operator-facing onboarding flow that takes a bare server through staking, registration, gossip warmup, and accepting paid delivery.

1. [ADR 019 — Node Onboarding and Bootstrapping Flow](019-node-onboarding.md)

### Chapter 8 — Supporting infrastructure

Wire-format evolution rules and the privacy-surface inventory. Both apply across the chapters above.

1. [ADR 013 — Schema Evolution](013-schema-evolution.md)
2. [ADR 017 — Privacy Analysis](017-privacy.md)

The numeric per-ADR index below stays as the canonical reference.

### Appendices — Reference Patterns

Appendices document patterns, reference implementations, and operational guidance built **on top of** the protocol. They are not part of the core spec — alternative implementations are acceptable. See [`README.md` § Decision-record context](README.md#decision-record-context) for the ADR-vs-appendix distinction.

1. [Encrypted Content Publishing](appendix-encrypted-content-publishing.md) — pattern for building an encrypted-content publishing system on top of deCDN; companion app server and `cdn/keys/v1` ALPN
2. [Directory Bundles (`decdn bundle`)](appendix-bundles.md) — publisher-side convenience for grouping content-addressed blobs into a single JSON manifest; nodes deliver individual hashes and do not require bundle support
3. [Observability and Metrics](appendix-observability.md) — recommended metric naming, registry, and slash-risk alert thresholds
4. [Peer-Table Eviction Policy](appendix-peer-table-eviction.md) — TTL-based eviction (default 600 s) keyed on `last_seen_us`; active eviction on registry deregistration / origin blacklisting; no hard size cap (staking registry bounds growth); reputation does not factor into eviction
5. [Blob Cache Eviction Policy](appendix-blob-cache-eviction.md) — LRU keyed on last successful `CacheEngine::get` timestamp; operator pinning overrides LRU; operator evict is durable and orthogonal; probe-hold ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)) composes above LRU; reputation does not factor into eviction
6. [Production L2 Deployment Target](appendix-l2-deployment.md) — Arbitrum One selection (deployment decision; protocol depends on Arbitrum-class properties calibrated in core ADRs)
7. [PoC/Production Seam Architecture (Rust)](appendix-poc-production-seams.md) — leaf-crate principle, wiring-layer mode selection, contract surface is not a PoC/production seam
8. [deCDN Binaries — `decdn-node` + `decdn` Split](appendix-binaries.md) — rationale for the dockerd-style split into the long-lived cache-node daemon (`decdn-node`) and the one-shot operator/publisher CLI (`decdn`)
9. [Local Admin HTTP Surface](appendix-local-admin-http.md) — loopback-bound admin API for operator runbook automation
10. [Operator Key Rotation Runbook](appendix-operator-key-rotation.md) — sequenced procedure for rotating the operator's iroh node-key, Ethereum signing key, and (production) session keys via `bindNodeId`, deregister-and-re-stake, or `erc7579/smartsessions`
11. [Operator Protocol-Upgrade Runbook](appendix-operator-upgrade-path.md) — sequenced operator actions for each ADR 013 tier (Tier 1/2 checklists; Tier 3 rolling-upgrade procedure; client and governance coordination)
12. [Permissionless Fraud-Detection Layer](appendix-fraud-detection.md) — optional, anyone-can-run on-chain monitoring of stale closes and fraudulent epoch summaries via the existing `SlashJudge` bond mechanism

## Architectural Decisions

Numeric per-ADR index. The thematic chapter ordering for top-to-bottom reading lives above in [Reading Order](#reading-order); this section is the canonical per-ADR reference. Each entry is a one-line summary of the ADR's decision; the full Context / Decision / Consequences sections live in the linked file.

- **[ADR 000 — Language and Core Networking Stack](000-language.md)** — Rust + iroh (0.98).
- **[ADR 001 — Network Topology and Peer Mesh](001-network.md)** — Flat peer mesh; gossip for node discovery; `cdn/dht/v1` (Kademlia subset) for content discovery from PoC onward, with the on-chain origin directory ([ADR 022](022-content-discovery.md)) as the deterministic last-resort fallback when DHT returns no providers.
- **[ADR 002 — Content Addressing](002-content-addressing.md)** — BLAKE3 content-addressed blobs. Node backends are opaque to the network.
- **[ADR 003 — Payment Model](003-payments.md)** — Off-chain payment-token channels (USDC, fixed at deployment). Market-driven rates within governance-set bounds.
- **[ADR 005 — Wire Protocol](005-protocol.md)** — Two core protocols (ALPN-negotiated) plus iroh-gossip. `cdn/client/v1` covers all paid delivery.
- **[ADR 008 — Reputation System](008-reputation.md)** — Interaction-weighted scoring with gossip propagation; complements the [ADR 026 §3 per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) as off-chain wash-trading signal.
- **[ADR 009 — Governance Model](009-governance.md)** — Admin key for PoC; ve-weighted Governor + Timelock with safety bounds for production.
- **[ADR 011 — Content Takedown and Hash Blacklisting](011-content-takedown.md)** — Governance-controlled on-chain hash blacklist with regional bodies and emergency fast-path; per-entry appeals for regional entries via emergency-multisig fast-track + ve-Governor ratification ([§ Blacklist Entry Appeals](011-content-takedown.md#blacklist-entry-appeals)).
- **[ADR 012 — Client Architecture, Bootstrap, and Trust Model](012-client.md)** — Client bootstrap, key management, identity lifecycle, trust boundary, multi-node parallel download, crash recovery, and file manifests.
- **[ADR 013 — Schema Evolution](013-schema-evolution.md)** — Varint-length framing, protocol enums, three-tier evolution model.
- **[ADR 014 — On-Chain Verification for Slashing Evidence](014-on-chain-verification.md)** — secp256k1 EIP-712 `slash_sig` on `ProbeResponse`/`StreamResponse`; unified `SlashJudge` contract for the three signature-dependent offenses (phantom, rate, blacklist). Content corruption is absorbed at the wire (no on-chain path) — see [ADR 003 §Corrupted delivery](003-payments.md#corrupted-delivery). Wash-trading defense lives in [ADR 026 §3](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) (per-operator gauge-share cap), not in `SlashJudge`.
- **[ADR 015 — QUIC 0-RTT Connection Establishment](015-zero-rtt.md)** — 0-RTT early data for `cdn/probe/v1` only; `cdn/client/v1` (paid delivery) and `cdn/dht/v1` (multiplexes state-changing `StoreRequest`) stay 1-RTT.
- **[ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md)** — Cross-contract call graph, fund custody, access control matrix, and reentrancy analysis.
- **[ADR 017 — Privacy Analysis](017-privacy.md)** — Unified privacy surface inventory, adversary model, and mitigation roadmap.
- **[ADR 018 — Liquidity Strategy (Balancer 80/20 POL)](018-liquidity-strategy.md)** — Protocol-Owned Liquidity in a Balancer V3 80/20 TOKEN/USDC weighted pool, seeded from the genesis liquidity allocation.
- **[ADR 019 — Node Onboarding and Bootstrapping Flow](019-node-onboarding.md)** — End-to-end procedure from bare server to actively accepting paid delivery; five sequential onboarding phases.
- **[ADR 022 — Content Discovery at Scale](022-content-discovery.md)** — `cdn/dht/v1` Kademlia subset for content discovery (primary from PoC onward); demand surfaced via DHT FIND_VALUE query frequency and local cache-miss timestamps. No discovery fees.
- **[ADR 024 — Account Abstraction and Safe Smart Wallet Support](024-account-abstraction.md)** — Universal `SignatureChecker` across all contracts; Safe as the recommended wallet for nodes and clients; session keys via ERC-7579 `smartsessions` deferred to production.
- **[ADR 026 — Gauge-Boost Tokenomics](026-gauge-boost-tokenomics.md)** — 1B fixed supply; `FeeRouter` six-bucket split with Curve-style gauge boost, delegator pool, and SafetyReserve. Per-operator gauge-share cap (5% default) is the wash-trading defense; gauge bucket paused via `gaugeLaunched == false` until cap is enforced (`enableGauge()` cutover).
- **[ADR 028 — Slashing Appeals and Dispute Escalation](028-slashing-appeals.md)** — 30-day post-slash appeal window via `SafetyReserve` restitution; emergency multisig fast-track + 14-day ve-Governor ratification; one accepted appeal per operator per 365 days.
- **[ADR 030 — Node Region Self-Attestation](030-node-region-self-attestation.md)** — Region claims in `NodeAnnounce` and `StakingRegistry` are accepted at face value; the IP-geolocation oracle / third-party attestation path is explicitly rejected. Appeals-standing flipping and reactive blacklist-scope flipping are closed by a 7-day `regionLastChanged` stability window on `StakingRegistry` (governable `[3d, 30d]`; [ADR 011 § Standing](011-content-takedown.md#standing) path 2 and [§ Regional Scope](011-content-takedown.md#regional-scope)). Latency-vs.-claim reputation penalty from [ADR 001 § Consequences](001-network.md#consequences) is the canonical soft mitigation for residual pre-positioned misdeclaration.
- **[ADR 031 — ContentBlacklist Appeal-Contract Surface](031-content-blacklist-appeals-contract.md)** — Pins the contract surface for ADR 011 § Blacklist Entry Appeals: per-appeal storage layout, event-topic ordering, state machine, and integration with `ContentBlacklist` core (suspension flag, `_removeHashRegional`, permissionless cleanup).
- **[ADR 032 — SafetyReserve Appeal-Surface Contract Surface](032-safety-reserve-appeals-contract.md)** — Pins the contract surface for ADR 028: per-appeal storage layout, escrow accounting, all six Solidity event signatures (`SlashAppealOpened` / `FastTracked` / `Rejected` / `Ratified` / `Reversed` / `Lapsed`), and a permissionless `cleanupExpiredAppeal` entry point.

## Key Invariants

- No external origin URL exists — content enters the network through origin-backed nodes whose backends are hidden
- A node cannot deliver paid content without being reachable via iroh NodeId; the backend is always hidden
- A node cannot earn without delivering verifiable bytes — BLAKE3 hash mismatch voids payment
- A node cannot join the peer mesh without staking — prevents free-riders and provides a slashable bond
- A node cannot register without staking — `StakingRegistry` enforces `stake >= minStake` before accepting a `registerNode` call
- A node cannot register without binding — `registerNode` atomically writes the NodeId-to-address mapping via EIP-712 signature, ensuring every active node is immediately slashable
- A node cannot register a NodeId it does not control — `registerNode` verifies an ed25519 signature proving ownership of the NodeId's private key, preventing squatting ([ADR 001](001-network.md#nodeid-ownership-verification))
- Payment channels amortize on-chain costs across an entire session; per-MB payments are off-chain
- Safety bounds on all governable parameters are hardcoded — governance cannot set fees to 100% or stake to zero (see [ADR 009](009-governance.md))
- A node cannot serve a blacklisted hash after the compliance window — doing so is a slashable offense (see [ADR 011](011-content-takedown.md))
- The cache and incentive layers are independent crates — the cache layer works without payment logic (leaf-crate principle, [appendix-poc-production-seams.md](appendix-poc-production-seams.md)); the binary split and crate dependency flow are specified in [appendix-binaries.md](appendix-binaries.md). `decdn-config-types` is a second leaf crate (alongside `decdn-protocol`): it owns the config-vocabulary value types (`RetryPolicy`, `DecompressMode`, `OriginUrl`, `OriginKind`, `Hash`, `PinnedHashes`) shared by `decdn-cache` and `decdn-common`, so `decdn-common` (and the publisher CLI) link no blob store / AWS SDK — the cache↔common decoupling is genuinely clean

## Trust Assumptions

The system relies on several infrastructure-level assumptions beyond the cryptographic guarantees verified on-chain or in-protocol. [ADR 012](012-client.md) documents the client-specific trust boundary (verified / trusted / not trusted); this section covers system-wide assumptions that span multiple components.

- **NTP availability and correctness.** Gossip validation depends on loose clock agreement: ±60 s for `NodeAnnounce` freshness ([ADR 001](001-network.md)), and for `ReputationReport`, a maximum age of 1 h with up to +5 min allowed future skew ([ADR 008](008-reputation.md)). A compromised or unavailable NTP source could cause mesh partitions or cause nodes to reject valid gossip. Mitigation: nodes detect relative drift via peer timestamp comparison; the tolerance windows are generous enough to absorb typical NTP jitter.

- **L2 RPC provider honesty.** Nodes and clients trust their RPC provider to return correct event logs for registry queries, blacklist polling, and rate-bounds lookups. A malicious RPC provider could hide `ChannelCloseInitiated` events from the in-process dispute monitor or third-party fraud detectors, defeating dispute protection, or return a fabricated node list to eclipse a client. Mitigation: PoC accepts single-RPC trust; production plans multi-source bootstrap ([ADR 012](012-client.md) Option B) and multiple independent RPC providers.

- **Encrypted transport integrity for voucher confidentiality.** Vouchers are bearer instruments — a leaked voucher is valid regardless of how it was obtained. The system assumes vouchers only traverse encrypted authenticated channels between the relevant parties: client↔node and node↔node cache-miss pulls. Mitigation: QUIC/TLS provides in-transit encryption on all these links; vouchers are never logged or persisted in plaintext. Endpoint compromise or debug output leaking vouchers remains an operational risk.

- **Arbitrum sequencer liveness.** The dispute mechanism assumes forced-inclusion transactions complete within ~24 h ([ADR 003 § L2 sequencer censorship](003-payments.md#l2-sequencer-censorship)). If the sequencer censors dispute transactions beyond this window, a fraudulent close could settle before the honest party responds. Mitigation: PoC dispute window is 48 h (governable 12h–72h), providing at least 24 h of effective response time after worst-case sequencer censorship.

- **Payment-token behavior stability.** The payment token is USDC, fixed at deployment as an immutable constructor argument. USDC is a standard ERC-20 (no fee-on-transfer, rebase, or transfer hooks). A Circle-side change to USDC semantics or an address/contract freeze is outside protocol control and is the accepted counterparty risk noted in [ADR 003](003-payments.md).

- **iroh relay availability.** iroh relays are stateless servers that broker NAT traversal and relay encrypted traffic as a fallback when direct peer-to-peer connections fail (~10% of networking conditions). Relays are not CDN protocol participants — they cannot inspect, cache, or modify content (all traffic is end-to-end encrypted). The deCDN does not incentivize relay operators: paying relays per-byte would create a perverse incentive to prevent direct connections from forming. PoC uses n0.computer's public relays (rate-limited, no SLA). Production deployments should self-host dedicated relays as operational infrastructure, funded from protocol treasury or node staking fees — not as an incentivized network role. If direct-connection success rates drop below ~85%, investigate NAT traversal improvements before considering relay incentivization.

## Origin Backends

Origin-backed nodes hold the canonical bytes and are pulled only on cache miss; whether an operator is *recognized* as origin is governed on-chain via `OriginAssignment` (see [ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority) and [ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces)). Configuring an origin backend locally without DAO authorization simply means the operator's bytes are served as cache. Supported backends — any S3-compatible object store (AWS S3, Cloudflare R2, Backblaze B2, self-hosted MinIO), an NFS mount, or local disk — and how a node maps a hash to its stored object are purely operational: the protocol only requires that a node deliver the correct bytes for a given hash.

## Non-Goals

Permanent scope boundaries — not deferred work.

- **DRM / content protection** — blobs are served public-by-default; confidentiality is an app-layer concern.
- **Transcoding / adaptive formats** — content-addressed bytes are delivered verbatim; transforming them would break the hash.
- **Search, discovery, recommendation** — an external/additive layer, not the core protocol.
- **Mobile or web clients** — the reference client is a native binary; other surfaces are downstream.
- **Multi-chain support** — settlement runs on a single L2; cross-chain is out of scope.
- **Erasure coding** — blobs are fully replicated across nodes, not erasure-coded.
