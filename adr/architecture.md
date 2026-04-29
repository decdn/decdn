# Architecture Overview

**Date:** 2026-03-28
**Status:** Living document — updated as ADRs are added or revised

---

## What This Is

A decentralized CDN with two participant roles:

- **Nodes** (providers) cache and serve content. They stake TOKEN to participate in the peer mesh and compete on price and latency. Some nodes are configured with an origin backend (S3, NFS, local disk) making them the canonical source for specific content — this is a deployment choice, not a protocol distinction. No external origin URL is ever exposed.
- **Clients** consume content. They pay nodes per MB via off-chain payment channels (USDC in PoC; multiple governance-approved ERC-20 tokens in production — see [ADR 010](010-multi-token.md)).

The PoC scope is tens of nodes on a testnet, proving the delivery pipeline (content discovery, probing, paid streaming) and payment channel lifecycle (open, voucher, close, dispute). Reputation, encryption, watchtowers, and governance use simplified stand-ins.

---

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
        A["App Server<br/>(ADR 006)"]
    end

    N1 <-->|"cdn/client/v1<br/>paid per-MB"| N2
    N2 <-->|"cdn/client/v1<br/>paid per-MB"| N3
    N1 <-->|"cdn/client/v1<br/>paid per-MB"| N3

    C1 -->|"cdn/client/v1<br/>paid per-MB"| N1
    C2 -->|"cdn/client/v1<br/>paid per-MB"| N2
    C3 -->|"cdn/client/v1<br/>paid per-MB"| N3

    N2 -.->|opaque fetch| S3

    N1 <-.->|"iroh-gossip<br/>NodeAnnounce, RateChange"| N2
    N2 <-.->|"iroh-gossip<br/>NodeAnnounce, RateChange"| N3

    S3 -.->|"K_blob at ingest"| A
    A -.->|"cdn/keys/v1<br/>epoch keys + envelopes"| C1
    A -.->|"cdn/keys/v1<br/>epoch keys + envelopes"| C2
    A -.->|"cdn/keys/v1<br/>epoch keys + envelopes"| C3
```

**Note:** PoC payments use USDC only; production supports multiple governance-approved ERC-20 tokens (see [ADR 010](010-multi-token.md)).

Clients probe candidate nodes, pick the best by the unified selection score (see [ADR 001](001-network.md#node-selection-algorithm) for the full formula), stream over `cdn/client/v1`, and pay via off-chain payment vouchers (USDC in PoC). On a cache miss, a node performs a DHT FIND_VALUE lookup (`cdn/dht/v1`), probes the returned candidates via `cdn/probe/v1`, selects the best, and pulls via `cdn/client/v1` (paid). During bootstrap, broadcast probe fan-out is used as a fallback. Every byte delivered — whether client→node or node→node — is paid.

---

## Architectural Decisions

### [ADR 000 — Language and Core Networking Stack](000-language.md)

**Rust + iroh (0.97).**

The implementation language is Rust. The networking stack is iroh, which provides QUIC transport, NAT traversal, content-addressed blob transfer, and gossip as a cohesive unit. A single statically linked binary runs as a node or client depending on configuration.

---

### [ADR 001 — Network Topology and Peer Mesh](001-network.md)

**Flat peer mesh. Gossip for node discovery; `cdn/dht/v1` (Kademlia subset) for content discovery from PoC onward, with broadcast probe fan-out as a bootstrap fallback (see [ADR 022](022-content-discovery.md)).**

All staked nodes form a flat mesh. Node metadata is broadcast over iroh-gossip on regional topics (`cdn/region/{cc}/v1`) and a global topic (`cdn/global/v1`) via lightweight `NodeAnnounce` messages (~800 bytes, 60-second PoC default interval). Per-node gossip bandwidth scales linearly: ~3 Kbps at 30 nodes (PoC), ~111 Kbps at 1,000 nodes; structured overlay or gossip partitioning is needed beyond ~1,000 nodes. Content discovery is on-demand: on a cache miss, nodes probe all known peers via `cdn/probe/v1` in parallel and select the best provider by the unified selection score (`rate_per_mb × rtt_ms × (1 / max(reputation, 0.1)²)` — lower is better). No content inventories are broadcast — no Bloom filters, no hash lists. The on-chain node registry is part of the `StakingRegistry` contract; the `NodeInfo` struct maps `NodeId` (ed25519 public key) to QUIC multiaddrs and Ethereum address. Registration requires both an EIP-712 binding signature (for slashability) and an ed25519 ownership proof (preventing NodeId squatting).

---

### [ADR 002 — Content Addressing](002-content-addressing.md)

**BLAKE3 content-addressed blobs. Node backends are opaque to the network.**

Every blob is identified by its BLAKE3 hash. Clients verify received bytes against the known hash. The hash→backend mapping is internal to each origin-backed node and never shared — no participant in the network can learn or bypass the node's backing storage.

---

### [ADR 003 — Payment Model](003-payments.md)

**Off-chain USDC payment channels. Market-driven rates.**

Clients pay nodes per MB. On a cache miss, nodes pay origin-backed nodes per MB for initial content pulls, then amortise that cost across many client deliveries. Origin-backed nodes set the effective price ceiling (reflecting their backend egress costs). Rates are fully market-driven within governance-set bounds. Voucher cadence (default 1 MB) is negotiable per-stream for large blob transfers, reducing overhead without materially increasing risk. Nodes join the mesh via `StakingRegistry.registerNode()` ([ADR 001](001-network.md)), which atomically establishes the cryptographic NodeId-to-Ethereum address binding (EIP-712 signature, see [ADR 003](003-payments.md)) and verifies ed25519 ownership of the NodeId (preventing squatting) for slash evidence and payment attribution. `bindNodeId()` remains available for post-registration key rotation. Client bindings are ephemeral (per-session).

---

### [ADR 004 — Dual-Currency Token Model](004-tokenomics.md)

**USDC for payments. TOKEN for staking and governance.**

> **Superseded by [ADR 026 — Gauge-Boost Tokenomics](026-gauge-boost-tokenomics.md).** ADR 026 is the canonical tokenomics source going forward. The summary below is preserved for historical context only.

TOKEN is not used for payments. All nodes must stake TOKEN to participate. Staking cost creates accountability and Sybil resistance. 20% of protocol fees buy back and burn TOKEN (accumulate-only in PoC; buyback execution deferred to production). Fixed supply of 1B at genesis. Challenge bonds (100 TOKEN in PoC, 50 TOKEN in production) are required for slash claims, preventing zero-cost griefing. The buyback venue, pool type, and liquidity-seeding strategy are specified in [ADR 018](018-liquidity-strategy.md). Governance is covered separately in [ADR 009](009-governance.md).

---

### [ADR 005 — Wire Protocol](005-protocol.md)

**Three core protocols (ALPN-negotiated) plus iroh-gossip. `cdn/client/v1` covers all paid delivery.**

| Protocol | Purpose |
| --- | --- |
| `cdn/probe/v1` | Parallel latency + availability check before node selection |
| `cdn/client/v1` | Paid delivery: client→node, node→node (cache miss) |
| `cdn/watchtower/v1` | Channel-dispute monitoring ([ADR 007](007-watchtower.md)) |
| iroh-gossip (built-in) | Node metadata broadcast (`NodeAnnounce`), rate change announcements (`RateChange`), watchtower discovery (`WatchtowerAnnounce`, production), node discovery |

**Companion protocol (app server — not a CDN protocol participant):**

| Protocol | Purpose |
| --- | --- |
| `cdn/keys/v1` | Epoch key delivery, play requests, offline leases ([ADR 006](006-e2e-encryption.md)) |

The app server shares the iroh QUIC transport but does not participate in gossip, probing, or staking. See [External Components](#external-components).

Gossip topics: `cdn/global/v1` (all nodes — `NodeAnnounce`, `RateChange`, `WatchtowerAnnounce` (production)), `cdn/region/{cc}/v1` (regional — `NodeAnnounce`), `cdn/reputation/v1` (reputation reports — [ADR 008](008-reputation.md)).

`redirect` in `StreamResponse` always points to a NodeId, never an external URL. The origin backend is never revealed.

---

### [ADR 006 — End-to-End Encryption and Key Distribution](006-e2e-encryption.md)

**Envelope encryption with epoch-rotated key distribution.**

Each blob is encrypted once at ingest with a random symmetric key (XChaCha20-Poly1305). The ciphertext is content-addressed and cached normally — one hash, one copy for all clients. An **app server** — an external component operated by the content provider — gates access: on each play request it wraps the blob key with a rotating epoch key and sends it over an authenticated `cdn/keys/v1` QUIC stream. Closing the epoch key stream revokes access within one epoch (5 minutes). CDN nodes only ever see ciphertext. See [External Components](#external-components) for the app server's role and deployment model.

---

### [ADR 007 — Watchtower Design for Channel Disputes](007-watchtower.md)

**Non-custodial watchtowers for dispute-window liveness.**

A watchtower holds the latest voucher for a registered channel and submits a `disputeChannel` transaction if a stale close is detected on-chain. Watchtowers cannot steal funds or worsen settlement — the voucher's EIP-712 signature is the only authorisation the contract checks. Nodes register with 2–3 independent watchtowers via `cdn/watchtower/v1` over iroh QUIC. A local in-process dispute monitor provides defense-in-depth for the node-is-online case. Production deployments use an on-chain `WatchtowerEscrow` contract for fee accountability: watchtowers submit periodic heartbeats with mandatory voucher state attestation (BLAKE3 commitment), and watched parties can reclaim escrowed fees on heartbeat failure.

---

### [ADR 008 — Reputation System](008-reputation.md)

**Interaction-weighted scoring with gossip propagation.**

Nodes are ranked by a reputation score (0.0–1.0) derived from local observations (70%) and gossip-propagated reports (30%). Reports are weighted by the reporter's effective settled value — total settled value adjusted for counterparty diversity (minimum 5 distinct counterparties for full credit) and time decay (half-life ≈ 7 weeks), both on-chain verifiable. USDC-only in PoC, multi-token normalized in production (see [ADR 010](010-multi-token.md)). Reporter weight is capped at 3× to limit incumbency advantage while keeping manipulation expensive. Only staked nodes may submit gossip reports; clients contribute via local scores only. Scores decay toward neutral without fresh data, clamping limits per-report impact, and a cold-start bootstrap gives new nodes initial traffic (one-time per operator address to prevent re-staking abuse).

---

### [ADR 009 — Governance Model](009-governance.md)

**Admin key for PoC. ve-weighted governance with safety bounds for production.**

During the PoC, a single deployer address controls all contract parameters. Production governance uses OpenZeppelin Governor with ve-weighted voting (per [ADR 026](026-gauge-boost-tokenomics.md) §4 / `VotingEscrow.balanceOfAt`), a 4% ve-supply quorum, a 7-day voting period, and a 48-hour timelock. All governable parameters have hardcoded safety bounds that even governance cannot override (e.g., slash 5%–50%, dispute window 12h–72h; PoC deployments default the dispute window to 48h within this range). A 3-of-5 emergency multisig can only pause contracts and add emergency blacklist entries, with a 12-month sunset enforced via an immutable constructor deadline. A renewable sunset mechanism is recommended for production — governance can vote to extend the deadline by capped increments, preserving the anti-centralization default while maintaining emergency capability.

---

### [ADR 010 — Multi-Token Payment Support](010-multi-token.md)

**Token-agnostic payments with governance-managed ERC-20 allowlist. USDC-only for PoC.**

Extends ADR 003 to support multiple ERC-20 tokens. The production `PaymentChannel` contract maintains a governance-managed allowlist of approved tokens; `openChannel` reverts if the token is not on the allowlist. Per-token rate bounds are set by governance. A `payment_token` field is added to `StreamRequest` and `token_rates` is added alongside the single `rate_per_mb` in gossip advertisements (nodes accepting only USDC may retain the single field for simplicity). The EIP-712 voucher already carries a `token` field from ADR 003 — no signature scheme migration is needed. Production deploys a new `PaymentChannel` contract (not an upgrade of the PoC `StablePaymentChannel`). Channels in governance-removed tokens can be force-closed by any address via `forceCloseChannel`, entering the standard dispute/settle flow.

---

### [ADR 011 — Content Takedown and Hash Blacklisting](011-content-takedown.md)

**Governance-controlled on-chain hash blacklist with regional bodies and emergency fast-path.**

A `ContentBlacklist` contract supports global (network-wide) and regional (jurisdiction-scoped) takedown via designated regional governance bodies. Standard governance entries have a 24-hour compliance window; the emergency multisig path takes effect immediately with a 2-hour slash window. Emergency entries auto-expire after 14 days unless ratified by governance; entries categorized as CSAM or terrorist content use a 90-day auto-expiry to prevent re-exposure due to governance latency. Origin blacklisting by operator address counters hash evasion via trivial re-encoding — each re-upload requires fresh stake and a new identity. Each node also maintains a local denylist for direct legal notices. Serving a blacklisted hash after the compliance window is a slashable offense, subject to the escalating schedule in [ADR 004](004-tokenomics.md).

### [ADR 012 — Client Architecture, Bootstrap, and Trust Model](012-client.md)

**Client bootstrap, key management, identity lifecycle, trust boundary, multi-node parallel download, crash recovery, and file manifests.**

Clients are lightweight QUIC endpoints that subscribe to gossip (but do not publish), maintain local peer tables and reputation scores, and pay for content via off-chain vouchers. The bootstrap procedure covers iroh key generation, Ethereum key import, registry query with exponential-backoff retry and `peers.json` fallback, gossip subscription, and periodic registry refresh. Key management distinguishes PoC (file-based) from production (platform keychain, hardware wallet with derived hot key for voucher signing). Ephemeral NodeId-to-Ethereum bindings are per-connection with `nonce=0` sentinel. Eclipse attack mitigation is resolved: registry-only for PoC; multi-source bootstrap (Option B — on-chain registry + DNS seed list) for production, with minimum peer diversity (Option C) as supplementary client-side policy. An explicit three-tier trust boundary classifies what the client verifies, trusts, and does not trust.

Also specifies three client-side features: **multi-node parallel download** (`--max-channels N`; one channel per node; economical above ~10 GiB at N=4; incompatible with streaming output; PoC capped at 1); **crash recovery and resume** (atomic `state.json` under `~/.decdn/downloads/<hash>/`; resume from last BLAKE3-verified byte; voucher nonce persisted on every send; 64 MiB flush cadence); and **file manifests** (256 MiB chunks; postcard-encoded manifest blob whose BLAKE3 hash is the canonical file ID; client fetches manifest first then chunks; per-chunk BLAKE3 verification; blob retention for re-serving; per-chunk encryption via ADR 006 epoch keys).

---

### [ADR 013 — Schema Evolution](013-schema-evolution.md)

**Varint-length framing, protocol enums, three-tier evolution model.**

All QUIC stream messages use varint-length-prefixed frames containing a top-level protocol enum (one enum per ALPN). Gossip messages are wrapped in a versioned `GossipEnvelope`. Schema evolution follows three tiers: minor (append optional trailing fields), medium (add enum variants), major (ALPN version bump with QUIC TLS negotiation). Fields covered by cryptographic signatures are frozen per protocol version — unsigned fields can still be appended via minor evolution using a signed-body / unsigned-outer-fields pattern. Resolves the schema evolution limitation identified in [ADR 005](005-protocol.md).

---

### [ADR 014 — On-Chain Verification for Slashing Evidence](014-on-chain-verification.md)

**Dual-key slash signatures, optimistic challenge-response, unified SlashJudge contract.**

All four slashable offenses (corrupted delivery, phantom announcements, rate manipulation, blacklist violations) now have concrete on-chain evidence paths. Ed25519 signatures from iroh NodeIds are not EVM-verifiable, so protocol messages (`ProbeResponse`, `StreamResponse`, `RateChange`) carry a secp256k1 `slash_sig` — an EIP-712 signature over the same security-relevant fields — enabling `ecrecover`-based verification at 3,000 gas. The `slash_sig` is optional at the wire/gossip level (`Option<Bytes>`), but for `RateChange` a valid `slash_sig` is required to serve as on-chain counter-evidence in rate manipulation challenges (vs ~500k–1M for a Solidity Ed25519 library). Phantom and blacklist offenses are immediate (no counter window). Corruption and rate manipulation are deferred with a 24-hour counter-evidence window: corruption counters use a signed `DeliveryReceipt` (the contract verifies `receipt.requester == challenge.challenger` to prevent cross-requester receipt reuse); rate manipulation counters use a signed `RateChange` gossip message proving a legitimate rate change between the disputed timestamps. Evidence staleness is enforced via `MAX_EVIDENCE_AGE_US` (PoC: 5 days), which must be strictly less than the unbonding period to guarantee slashability. The production path upgrades corruption to an interactive keccak256 Merkle proof over 1024-byte chunks. A unified `SlashJudge` contract adjudicates all offense types and calls `StakingRegistry.slash()` on resolution.

---

### [ADR 015 — QUIC 0-RTT Connection Establishment](015-zero-rtt.md)

**0-RTT early data for latency-sensitive protocols.**

QUIC 0-RTT eliminates the TLS handshake round trip on repeat connections. `cdn/probe/v1` is the sole beneficiary — after the first probe cycle, subsequent cache-miss fan-outs send `ProbeRequest` alongside the ClientHello with zero handshake delay. All other protocols reject 0-RTT: payment-bearing (`cdn/client/v1`) and state-changing (`cdn/watchtower/v1`) to prevent replay-based accounting confusion, and `cdn/keys/v1` because authentication sequencing ([ADR 006](006-e2e-encryption.md)) requires `EpochKeyAuth` before any request stream. Session tickets are cached per `(remote_node_id, ALPN)` in an in-memory LRU (max 1,000 entries).

---

### [ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md)

**Cross-contract call graph, fund custody, access control matrix, and reentrancy analysis.**

Consolidates the interaction model across all on-chain contracts (StakingRegistry, StablePaymentChannel/PaymentChannel, BuybackBurner, ContentBlacklist, SlashJudge, WatchtowerEscrow). Documents the deployment order and initialization dependencies (with an atomicity recommendation for post-deployment role grants), every cross-contract call path with required authorization, which contracts hold which token types, the full role-based access control matrix (PoC admin key vs production Governor + timelock), and a per-function reentrancy analysis covering all contracts including WatchtowerEscrow. The SlashJudge uses per-offense challenge functions (`submitPhantomChallenge`, `submitRateChallenge`, `submitBlacklistChallenge`, `submitCorruptionChallenge`) with `resolveChallenge()` for post-window resolution; `StakingRegistry.slash()` returns 50% of the slash to `msg.sender` (SlashJudge), which forwards it to the challenger. All contracts inherit from [OpenZeppelin Contracts](https://docs.openzeppelin.com/contracts/) — `AccessControl`, `ReentrancyGuard`, `Pausable`, `SafeERC20`, `EIP712`, and `Governor` — to minimize custom security-critical code. Cross-contract state mutations are limited to two paths: `ContentBlacklist → StakingRegistry.ejectNode()` (via `BLACKLIST_ROLE`) and `SlashJudge → StakingRegistry.slash()` (via `SLASH_ROLE`).

---

### [ADR 017 — Privacy Analysis](017-privacy.md)

**Unified privacy surface inventory, adversary model, and mitigation roadmap.**

Consolidates privacy properties scattered across ADRs 001, 003, 005, 006, 007, 008, 012, and 014 into a single reference. Defines a four-tier adversary model (passive observer, active participant, infrastructure operator, compromised endpoint) and catalogs 23 privacy surfaces with explicit dispositions (accept or mitigate), including on-chain settlement volume leakage (P-22) and `slash_sig` as non-repudiable content inventory proof (P-23). Most surfaces are accepted as inherent to the accountability-first design (probes are public, on-chain channels enable disputes, gossip enables discovery). Five pre-mainnet mitigations are prioritized: client NodeId rotation, `popular_hashes` cardinality reduction from 20 to 5, client key encryption via platform keychain, operational RPC provider guidance, and epoch key forward secrecy (already specified in ADR 006). Dummy probes and payment channel mixing are deferred post-mainnet.

---

### [ADR 018 — Liquidity Strategy (Balancer 80/20 POL)](018-liquidity-strategy.md)

**Protocol-Owned Liquidity in a Balancer V3 80/20 TOKEN/USDC weighted pool, seeded from the genesis liquidity allocation.**

Reverses the implicit Uniswap V3 venue choice in prior ADRs. Balancer 80/20 weighted pools let the treasury seed the pool with roughly 1/4 the USDC of a 50/50 position for comparable near-spot depth *for small trades* (critical for a TOKEN-rich, USDC-poor treasury), eliminate concentrated-liquidity range-management overhead (no `LiquidityManager`, no keeper for rebalancing), and reduce impermanent loss by ~1.75× for a 2× TOKEN price move (~3.3% vs ~5.7%), aligning the DAO's IL profile with the TOKEN-upside thesis. The DAO treasury holds BPT directly; no LP rewards or liquidity mining. `BuybackBurner.executeBuyback()` swaps USDC → TOKEN via `BalancerV3Router.swapSingleTokenExactIn()`, with TWAP + `minTokenOut` as the primary MEV defense and CoW Swap batch-auction routing as a conditional add-on pending verification of V3-pool solver coverage. Balancer V3 is chosen over V2 in response to the 2025-11-03 V2 Composable Stable Pool exploit (~$125M); V3's new Vault architecture mitigates the bug class per Certora/Trail of Bits post-mortems. The `IBuybackBurner` interface adds one setter (`setPool(address)`) to stay venue-symmetric, preserving the option to supplement with a Uniswap V3 position in a future ADR once treasury USDC reserves and keeper infrastructure justify it.

### [ADR 019 — Node Onboarding and Bootstrapping Flow](019-node-onboarding.md)

**End-to-end procedure from bare server to actively accepting paid delivery, covering the five sequential onboarding phases.**

Formalizes the complete ordered flow that existing ADRs left implicit: Phase 1 (pre-flight: clock sync, iroh key generation, Ethereum key funding, region selection); Phase 2 (on-chain setup: TOKEN approval, staking, atomic `registerNode` with ed25519 + EIP-712 signatures); Phase 3 (node startup: rate bounds fetch, blacklist sync, peer table bootstrap from registry); Phase 4 (gossip subscription: join `cdn/global/v1` and regional topic, publish first `NodeAnnounce`); Phase 5 (accepting paid delivery: seven acceptance criteria for operational readiness). Also covers NAT/multiaddr handling (iroh hole-punching, when to call `updateMultiaddrs`), re-onboarding after deregistration or auto-ejection (nonce increment, preserved `firstRegisteredAt`), and PoC vs. production differences. Resolves the bootstrapping gap identified in Issue #190.

### [ADR 020 — Observability and Metrics Standard](020-observability.md)

**Canonical Prometheus metric registry, naming convention, alert thresholds, and `/health` endpoint contract.**

Consolidates metrics scattered across ADRs 001, 005, 011, 015 and `architecture.md § Observability` into a single reference. Defines a `decdn_` prefix + `_total`/unit-suffix naming convention; splits 35 metrics across eight subsystems into **mandatory** (M) and **recommended** (R) tiers; provides recommended alert thresholds for the seven slash-safety metrics; specifies the `/health` JSON endpoint with `ready`/`degraded`/`not_ready` semantics; and supplies a cross-reference table mapping all informal prior-ADR metric names to their canonical replacements. Resolves the observability gap identified in Issue #190.

### [ADR 021 — Production L2 Chain Selection](021-l2-chain-selection.md)

**Arbitrum One (chain ID 42161) is the canonical production chain for all deCDN contracts.**

Resolves the explicit deferral in ADR 004 and formalises the Arbitrum assumptions already embedded in ADRs 004, 007, and 018. Arbitrum One is selected over Base and OP Mainnet on the basis of: PoC continuity (Arbitrum Sepolia → Arbitrum One is a same-family migration), highest DeFi TVL and aggregator routing density for Balancer V3 buybacks, prior ADR consistency (gas estimates, forced-inclusion delay, Balancer V3 Router address all calibrated for Arbitrum One), and battle-tested OpenZeppelin Governor + TimelockController deployments. Native USDC (Circle CCTP, `0xaf88d065e77c8cC2239327C5EDb3A432268e5831`) is used — not bridged USDC.e. Cross-chain payment channels are excluded from v1. Re-evaluation triggers are defined for gas cost spikes, fraud-proof vulnerabilities, and sequencer censorship events.

### [ADR 022 — Content Discovery at Scale](022-content-discovery.md)

**`cdn/dht/v1` Kademlia subset for content discovery — primary mechanism from PoC onward. `cdn/probe/v1` broadcast fan-out retained as bootstrap/emergency fallback. Two popularity signals: `popular_hashes` gossip (advisory) and DHT FIND_VALUE query frequency (non-suppressible oracle). No discovery fees.**

Probe fan-out is O(N) per cache miss and does not scale beyond ~100 nodes. Gossip content announcements were rejected (unbounded traffic proportional to cache churn). Hash-prefix range hints were rejected (economically irrational — nodes cache popular content regardless of hash prefix). The production path is a lightweight Kademlia subset (`cdn/dht/v1` ALPN): nodes self-publish `(hash → NodeId)` STORE records when caching a blob, attracting paying clients; FIND_VALUE lookups are O(log N). No discovery fees — all revenue stays on delivery. Popularity is surfaced by two complementary signals: `popular_hashes` gossip (advisory, self-reported; suppression is self-limiting via `LoadHint`/selection score) and DHT FIND_VALUE query frequency (non-suppressible — routing traffic reaches nearby-keyspace nodes regardless of gossip). The probe step (`cdn/probe/v1`) is preserved as the final availability confirmation before any delivery commitment.

### [ADR 023 — PoC/Production Seam Architecture](023-poc-production-seams.md)

**Trait-based seams for PoC/production differences; single `poc` Cargo feature on the `node` crate only; `NetworkConstants` as the single source of truth for all numeric differences.**

All PoC/production behavioral differences are expressed as Rust traits with separate concrete implementations (`file.rs` / `keychain.rs`, `simple.rs` / `weighted.rs`, etc.). The `node` crate wires the correct implementations at compile time via a single `poc` Cargo feature declared only on that crate. No `#[cfg(feature = "poc")]` appears in leaf crates. Seven seams are defined: `KeyStore`, `ReputationEngine`, `PaymentChannelClient`, `GovernanceClient`, `WatchtowerClient`, `CorruptionChallenger`, and `NetworkConstants`. Production is the default compile target — the `poc` feature must be explicitly opted in. Solidity contract differences (admin key vs Governor, `adminReclaimNodeId` presence) are managed via separate Foundry deploy scripts rather than Rust feature flags.

### [ADR 024 — Account Abstraction and Safe Smart Wallet Support](024-account-abstraction.md)

**Universal `SignatureChecker` across all contracts; Safe as the recommended wallet for nodes and clients; session keys via ERC-7579 `smartsessions` deferred to production.**

Every signature verification site (voucher close/dispute, node registration, slash challenges) uses OpenZeppelin's `SignatureChecker.isValidSignatureNow` rather than `ECDSA.recover` — transparently supporting both EOAs (`ecrecover`, ~5.6K gas) and smart accounts (ERC-1271 `isValidSignature`, ~12–15K gas for Safe). EIP-712 domains, typed data hashes, and voucher formats are unchanged. PoC configuration: node operators and high-value clients run a 1-of-1 Safe with a software-held owner key on the signing host — same trust posture as today's `eth_keystore`, but routed through Safe for ERC-1271 compatibility on day one. `SlashJudge`'s verification pattern shifts from "recover-then-lookup" to "verify-against-provided-address" because ERC-1271 has no recovery. Production migrates the hot signing path (per-MB vouchers, per-probe/stream `slash_sig`) to Safe-7579 + [`erc7579/smartsessions`](https://github.com/erc7579/smartsessions) — a standardized session-key module with ERC-1271 validation, time windows, selector/domain-scoped action policies, per-session spending caps, and first-class revocation. EOAs remain fully functional for clients who prefer them; `SignatureChecker` makes wallet type transparent at the protocol level.

### [ADR 025 — Local Admin HTTP Surface](025-local-admin-http.md)

**Loopback-only JSON admin HTTP on `observability.admin_port` (default `9191`); versioned `/v1/...` routes; operator-local surface, distinct from `/metrics` and from any node-to-node ALPN.**

Running nodes expose a loopback HTTP admin surface so operator CLIs (`decdn node peers`, and later `decdn node drain` etc.) can read live state and trigger local control actions without going through either the Prometheus `/metrics` endpoint (read-only, text-only, aggregate) or an iroh ALPN (node-to-node, not local-operator). Transport mirrors the existing metrics server (hyper `service_fn`, loopback `TcpListener`, oneshot shutdown, semaphore-bounded concurrency). JSON responses on `/v1/...` paths leave room for schema evolution within the major version. No auth is required in the PoC — the loopback binding is the trust boundary; a later ADR can layer a shared-secret header if multi-tenant hosts ever enter scope.

---

### [ADR 026 — Gauge-Boost Tokenomics](026-gauge-boost-tokenomics.md)

**1B fixed supply. `FeeRouter` 40/40/7/5/5/3 split with Curve-style gauge boost, delegator pool, and SafetyReserve. Supersedes [ADR 004](004-tokenomics.md) in full.**

Replaces the original tokenomics in full: a six-bucket genesis allocation (30% treasury / 24% seed / 19% team / 15% community / 10% POL / 2% public) with no auto-ve-lock on vest, and a new `FeeRouter` contract that atomically splits operator USDC settlement into six destinations — 40% direct node base, 40% gauge-boost pool (weekly epoch, ve-weighted `working_bytes`), 7% delegator pool (TWAP USDC→TOKEN, distributed pro-rata to ve-lockers), 5% buyback-and-burn ([ADR 018](018-liquidity-strategy.md) mechanics unchanged), 5% protocol treasury, and 3% `SafetyReserve` for governance-gated incident payouts. Gross client rate is fixed at $0.01/GB at parity with budget commodity CDNs.

The 40% gauge pool uses a Curve veCRV-style boost: `working_bytes_i = min(bytes_i, 0.4·bytes_i + 0.6·(ve_i/total_ve)·total_bytes)`, directly linking operator compensation to long-term ve-commitment. ADR 004's 200M-TOKEN node-bootstrap fund is removed entirely and replaced by externally-raised pre-seed USDC capital ([ADR 030](030-preseed-usdc-deployment.md)). Slashing rate schedule (5%/15%/50%) and lifetime offense counter from ADR 004 carry over unchanged. Six follow-up ADRs (027–032) close out the v3-era design surface.

---

### [ADR 027 — Distinct-Client Delivery Receipts](027-distinct-client-receipts.md)

**Client-signed `DeliveryReceipt` Merkle-batched per epoch; gauge-pool eligibility gated on a distinct-client diversity threshold. Required for v1 production launch — without it, the gauge pool is gameable via wash-trading.**

The [ADR 026](026-gauge-boost-tokenomics.md) gauge formula is bounded by `bytes_i`, but `bytes_i` itself is currently the operator-supplied `claimedBytes` from settlement vouchers. An operator can wash-trade by running both sides of a payment channel — the 60% non-base router skim is denominated in their own USDC and is not a deterrent at the intended TOKEN price levels. This ADR introduces a `DeliveryReceipt` EIP-712 typed message signed by the *requester's* secp256k1 key (the address that funded the channel), paired one-to-one with each voucher and bound to a `contentRoot` keccak256 Merkle root over 1024-byte chunks (matching the [ADR 014 §2](014-on-chain-verification.md) production path).

Receipts are batched into a Merkle tree per operator per epoch; only the root and a small summary land on chain at settlement, with individual receipts surfacing only on challenge (watchtower-monitored, [ADR 008](008-reputation.md)-reputation gated). Gauge-pool eligibility is gated on a distinct-client diversity threshold computed across recovered `clientPubKey` addresses. Vouchers without matching receipts are still fully redeemable for USDC — only gauge eligibility for the underlying bytes depends on the receipt. Forward-referenced from [ADR 026 §Risks](026-gauge-boost-tokenomics.md) as priority-1 and **not optional for production launch**.

---

### [ADR 028 — Native sveTOKEN Liquid-ve Wrapper](028-sve-token-wrapper.md)

**Frax `sfrxETH`-style native ERC-20 wrapper around a pooled `VotingEscrow` lock. Pre-empts Convex-style third-party governance capture. Deferred — ship within 6 months of v1 mainnet.**

[ADR 026](026-gauge-boost-tokenomics.md) §4 defines `VotingEscrow` as a strict no-early-exit commitment device. Curve veCRV's history shows that illiquid commitment devices attract third-party liquid wrappers (Convex `cvxCRV`, Aura for veBAL) which capture 30–50% of underlying ve-supply, leak wrapper-economy revenue (deposit fees, bribe markets) outside the issuing DAO, and concentrate gauge-vote weight in a third party. This ADR ships a *native* `SveToken` ERC-20 wrapper, owned and operated by the deCDN DAO, before a third party ships one outside it.

`SveToken` holds exactly one pooled `VotingEscrow` lock, kept at max duration via continuous re-extension. `deposit(amount)` mints `sveTOKEN` at an ERC-4626-style appreciation rate; there is no protocol-level redemption — holders exit only via secondary-market sale or hold-to-natural-decay. Exchange rate appreciates from auto-compounded delegator-pool TOKEN yield ([ADR 026 §6](026-gauge-boost-tokenomics.md)). The deferment window exists because Convex-style capture takes time to develop (third-party wrappers depend on observable veTOKEN supply); shipping within 6 months stays ahead of that window without forcing the contract into a v1 audit slot.

---

### [ADR 029 — Adaptive FeeRouter Parameters](029-adaptive-fee-router.md)

**Two automated feedback hooks (lock-rate and price-floor) within [ADR 026 §11](026-gauge-boost-tokenomics.md) safety bounds. Deferred — adopt after v1 governance dynamics observable.**

[ADR 026 §11](026-gauge-boost-tokenomics.md) makes the six `FeeRouter` shares governable, but standard governance (7-day vote + 48h timelock per [ADR 009](009-governance.md)) is too slow for two state-driven feedback regimes the v3 design surfaces but does not solve: ve-lock-rate windows (the gauge boost under-fires below ~15% lock rate and over-rewards a saturated population above ~50%) and TOKEN-price-floor scenarios (the 5% buyback flow is invariant to drawdown when buyback would be most useful).

This ADR adds an `AdaptiveFeeRouterController` evaluated at epoch rollover with two hooks: a lock-rate hook that shifts ±2pp between `treasury` and `delegator` shares based on `ve_locked / circulating_supply`, and a price-floor hook keyed off the [ADR 018](018-liquidity-strategy.md) Balancer V3 80/20 30-day TWAP. Both hooks are clamped to [ADR 026 §11](026-gauge-boost-tokenomics.md) safety bounds, observable via events, and disable-able by governance. Deferred status reflects a "merge the spec, do not deploy" stance — if v1 governance rebalancing turns out to be fast enough in practice, this ADR may close as Rejected.

---

### [ADR 030 — Pre-Seed USDC Deployment Program](030-preseed-usdc-deployment.md)

**$1M floor / $3M target externally-raised USDC pre-seed capital, structured into five funded programs. Replaces ADR 004's 200M-TOKEN bootstrap fund and is a prerequisite for v3 mainnet launch.**

Charters the externally-raised USDC pool that [ADR 026 §10](026-gauge-boost-tokenomics.md) substitutes for ADR 004's reflexive 200M-TOKEN node-bootstrap fund. Capital is held in a Timelock-custodied multisig sub-account with hard caps for a multisig fast-track path ($50K per incident / $250K per 30-day rolling) and standard governance proposals above those bounds. Five non-overlapping programs: 30% Protocol-Owned Operators (DAO-operated nodes in priority underserved regions), 25% hardware-leasing subsidies, 15% staking loans, 20% regional-deploy grants, and 10% Enterprise SLA guarantee fund (paired backing for [ADR 026](026-gauge-boost-tokenomics.md) `SafetyReserve`).

The program runs in parallel with the canonical [ADR 019](019-node-onboarding.md) self-funded onboarding flow — it does not replace any phase, it supplies stake/hardware/regional capital/SLA backing to operators who would otherwise be filtered out at Phase 1 or Phase 2. Sized for the S0→S1 (~178 → ~1,778 nodes) transition where operator-side reflexivity bites hardest and treasury inflow has not yet caught up. Raising at least the $1M floor is a prerequisite for v3 mainnet launch; contingency is governance re-allocation from the 30% Protocol Treasury bucket at the cost of development runway.

---

### [ADR 031 — Burn-and-Mint Client TOKEN Prepay](031-bme-client-prepay.md)

**Optional client-side TOKEN-prepay path implementing Helium's Burn-and-Mint Equilibrium pattern as a demand-side TOKEN sink. Deferred to v2.**

[ADR 026](026-gauge-boost-tokenomics.md)'s three TOKEN demand sources (operator stake, gauge ve-locking, delegator-pool TWAP buys) are all on the operator side; clients pay only USDC. If operator-side ve-lock adoption falters, the demand-side flywheel collapses with no usage-driven floor underneath it. This ADR pins a v2 design intent: a `BmePrepay` contract that lets clients optionally prepay bandwidth in TOKEN at a 5–8% discount to the equivalent USDC rate, with the prepaid TOKEN *burned* on consumption (no router skim, no treasury cut, no operator routing of the TOKEN itself).

The BME path coexists with [ADR 003](003-payments.md) USDC channels — opt-in for clients and opt-in for operators (operators may refuse BME-routed traffic if cost-of-conversion exceeds the discount). Two operator-payment options are documented but not yet pinned: protocol-mints-USDC-equivalent against a managed reserve, vs. operator receives TOKEN and swaps separately. Deferred to v2 because the design has two unresolved dependencies — a hardened TOKEN→USDC pricing oracle (the [ADR 018](018-liquidity-strategy.md) TWAP needs v1-stability data and multi-source check / circuit breakers before clients can fund prepay positions against it) and a v1 contract scope that does not justify the additional audit surface.

---

### [ADR 032 — Bandwidth Futures and Enterprise SLA Tier](032-bandwidth-futures-enterprise.md)

**TOKEN-denominated bandwidth futures + explicit Enterprise SLA contracts backed in priority order by `SafetyReserve` ([ADR 026 §5](026-gauge-boost-tokenomics.md)) and the pre-seed Enterprise SLA guarantee fund ([ADR 030 §2e](030-preseed-usdc-deployment.md)). Deferred to v2 mainnet.**

v1 deCDN serves best-effort delivery at $0.01/GB — sufficient for freemium / Pro tier clients but not for two adjacent client segments that the v3 economic model and market-dynamics analysis explicitly target: streaming startups and content aggregators that need cost certainty for capacity planning, and Enterprise clients that require contractually-binding availability/latency/throughput SLAs with credible recourse paths before they will migrate from Cloudflare/Akamai/Fastly. This ADR is the product-tier and contract-shape decision record for both segments.

**Bandwidth Futures** are TOKEN-denominated period-bounded contracts that pre-purchase a specified GB volume in a specified region at a strike rate, settling against verified delivery (`PhysicalDelivery` or `CashSettle`) at expiry — providing cost certainty for the client, revenue certainty for participating operators, and a meaningful demand-side TOKEN sink. **Enterprise SLA contracts** carry explicit penalty clauses backed by `SafetyReserve` (3% of routed USDC, organically-replenished) with the [ADR 030 §2e](030-preseed-usdc-deployment.md) pre-seed fund as paired backing for early-period worst-case payouts. The freemium / Pro / Enterprise tier ladder is the canonical client-segmentation model from v2 forward. Deferred to v2 because v1 must first establish the prerequisite infrastructure: `SafetyReserve`, [ADR 027](027-distinct-client-receipts.md) distinct-client receipts (required for SLA-attestation integrity), and the pre-seed Enterprise fund. This ADR is **not** a futures-DEX design — secondary-market mechanics are out of scope.

---

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

---

## Trust Assumptions

The system relies on several infrastructure-level assumptions beyond the cryptographic guarantees verified on-chain or in-protocol. [ADR 012](012-client.md) documents the client-specific trust boundary (verified / trusted / not trusted); this section covers system-wide assumptions that span multiple components.

- **NTP availability and correctness.** Gossip validation depends on loose clock agreement: ±60 s for `NodeAnnounce` freshness ([ADR 001](001-network.md)), and for `ReputationReport`, a maximum age of 1 h with up to +5 min allowed future skew ([ADR 008](008-reputation.md)). A compromised or unavailable NTP source could cause mesh partitions or cause nodes to reject valid gossip. Mitigation: nodes detect relative drift via peer timestamp comparison; the tolerance windows are generous enough to absorb typical NTP jitter.

- **L2 RPC provider honesty.** Nodes and clients trust their RPC provider to return correct event logs for registry queries, blacklist polling, and rate-bounds lookups. A malicious RPC provider could hide `ChannelCloseInitiated` events from watchtowers, defeating dispute protection, or return a fabricated node list to eclipse a client. Mitigation: PoC accepts single-RPC trust; production plans multi-source bootstrap ([ADR 012](012-client.md) Option B) and multiple independent RPC providers.

- **App server as trusted infrastructure.** The app server holds all blob encryption keys (`K_blob`) for encrypted content ([ADR 006](006-e2e-encryption.md)). Compromise of the app server key store exposes all content. Mitigation: epoch key rotation limits the blast radius for epoch-level access control, but does not provide forward secrecy for stored blob keys — this is an accepted trade-off documented in ADR 006.

- **Encrypted transport integrity for voucher confidentiality.** Vouchers are bearer instruments — a leaked voucher is valid regardless of how it was obtained. The system assumes vouchers only traverse encrypted authenticated channels between the relevant parties: client↔node, node↔watchtower ([ADR 007](007-watchtower.md)), and node↔node cache-miss pulls. Mitigation: QUIC/TLS provides in-transit encryption on all these links; vouchers are never logged or persisted in plaintext. Endpoint compromise or debug output leaking vouchers remains an operational risk.

- **Arbitrum sequencer liveness.** The dispute mechanism assumes forced-inclusion transactions complete within ~24 h ([ADR 007](007-watchtower.md)). If the sequencer censors dispute transactions beyond this window, a fraudulent close could settle before the honest party responds. Mitigation: PoC dispute window is 48 h (governable 12h–72h), providing at least 24 h of effective response time after worst-case sequencer censorship.

- **ERC-20 token behavior stability.** Governance-approved tokens are assumed not to change behavior post-approval (e.g., a proxy-upgradeable token adding fee-on-transfer). Changed token semantics could break settlement arithmetic or trap funds. Mitigation: PoC is USDC-only, reducing token-surface complexity; production token allowlisting and vetting criteria remain an open question in [ADR 010](010-multi-token.md).

- **iroh relay availability.** iroh relays are stateless servers that broker NAT traversal and relay encrypted traffic as a fallback when direct peer-to-peer connections fail (~10% of networking conditions). Relays are not CDN protocol participants — they cannot inspect, cache, or modify content (all traffic is end-to-end encrypted). The deCDN does not incentivize relay operators: paying relays per-byte would create a perverse incentive to prevent direct connections from forming. PoC uses n0.computer's public relays (rate-limited, no SLA). Production deployments should self-host dedicated relays as operational infrastructure, funded from protocol treasury or node staking fees — not as an incentivized network role. If direct-connection success rates drop below ~85%, investigate NAT traversal improvements before considering relay incentivization.

---

## Non-Goals (PoC)

- DRM or content protection
- Content transcoding or adaptive format conversion
- Search, discovery, or recommendation (see Future Work below)
- Mobile or web clients
- Multi-chain support (single L2 only)
- Multi-token payment support (USDC only for PoC; see [ADR 010](010-multi-token.md))
- Erasure coding (full replication only)

---

## Glossary

| Term | Definition |
| --- | --- |
| **Blob** | A content-addressed byte sequence identified by its BLAKE3 hash |
| **Chunk** | The BLAKE3 hash tree leaf size (1024 bytes). iroh-blobs uses this for verified streaming. On-chain Merkle proofs for slash evidence reference this leaf size — see [ADR 002](002-content-addressing.md) |
| **Hash sequence** | An ordered collection of blob hashes (iroh's equivalent of a directory/manifest) |
| **Voucher** | A signed off-chain payment message: `{channelId, amount, nonce, token, signature}` |
| **ALPN** | Application-Layer Protocol Negotiation — identifies which protocol a QUIC connection uses |
| **Node** | A staked participant that caches and serves blobs. Some are configured with an origin backend; others are pure caches. |
| **Client** | A lightweight QUIC endpoint that streams content and pays per MB |
| **Origin-backed node** | A node configured with an S3-compatible object store (e.g., S3/R2/B2/MinIO), NFS mount, or local disk — can serve any blob in that store, never experiences a true cache miss |
| **Slash signature** | An EIP-712 secp256k1 signature (`slash_sig`) on protocol messages, used for on-chain slash evidence via `ecrecover`. Distinct from the Ed25519 wire signature — see [ADR 014](014-on-chain-verification.md) |
| **SlashJudge** | The on-chain contract that adjudicates all slashable offenses, verifies slash signatures, manages challenge bonds, and calls `StakingRegistry.slash()` — see [ADR 014](014-on-chain-verification.md) |
| **WatchtowerEscrow** | The on-chain contract that manages prepaid watchtower monitoring fees and enforces heartbeat-based liveness accountability. Standalone contract that reads channel state but does not modify the payment channel contract — see [ADR 007](007-watchtower.md) |

---

## Origin Integration

Some nodes are configured with an origin backend (S3, R2, Backblaze B2, self-hosted MinIO, NFS, or local disk). They are the source of truth for all blobs but are accessed as infrequently as possible — only when no peer node has the content.

### Supported Origins

| Origin | Auth Method | Notes |
| --- | --- | --- |
| AWS S3 | IAM credentials or pre-signed URLs | Most common |
| Cloudflare R2 | S3-compatible API | No egress fees between R2 and Workers |
| Backblaze B2 | S3-compatible API | Cheapest egress ($0.01/GB) |
| MinIO (self-hosted) | S3-compatible API | Full operator control |
| NFS (local mount) | Filesystem access | No egress fees; requires local/network mount |
| Local disk | Filesystem access | Simplest setup; single-machine only |

All origin access goes through a single trait:

```rust
trait OriginStore: Send + Sync {
    async fn fetch(&self, hash: &Hash) -> Result<Bytes>;
    async fn head(&self, hash: &Hash) -> Result<ObjectMeta>;
}
```

### Hash-to-Object-Key Mapping

S3 objects are addressed by key (a path string). Blobs are addressed by BLAKE3 hash. The mapping is stored in a content catalog — a small database (PostgreSQL or SQLite) maintained by the operator:

```
catalog: hash → {s3_bucket, s3_key, size_bytes, content_type}
```

Nodes query it on cache miss to find the origin pull URL. The catalog is not on-chain — it is an operational concern.

---

## Cache Behavior

The protocol does not dictate cache policy. Nodes are economically motivated to make good caching decisions.

**Cache miss resolution** follows a priority order:

1. **Paid pull-through (preferred):** Node checks its probe cache or performs a DHT FIND_VALUE lookup + probe (`cdn/dht/v1` → `cdn/probe/v1`), selects the best provider by the unified selection score ([ADR 001](001-network.md#node-selection-algorithm)), pulls via `cdn/client/v1` (paid), caches locally, and streams to the client while the pull is in progress.
2. **Redirect (last resort):** If pull-through is disabled (`pull_through: false` in config), the node returns a redirect to an origin-backed node's NodeId. The client opens a channel with that node directly.

**Prefetching:** Nodes can proactively cache popular content using two signals: (1) local demand — tracking cache miss frequency per hash and prefetching when a threshold is crossed (default: 3 misses in 5 minutes); (2) network popularity — observing which hashes appear in multiple peers' `popular_hashes` fields in `NodeAnnounce` gossip messages (default threshold: 3+ peers within 10 minutes). All prefetch pulls use the same DHT FIND_VALUE → probe → `cdn/client/v1` path (paid).

**Eviction:** LRU or frequency-weighted eviction (LFU). Operators tune cache size to maximize hit rate within their storage budget. Blobs for which the node has signed `has_blob: true` in a `cdn/probe/v1` response are temporarily exempt from eviction via the **probe-triggered eviction hold** (`probe_hold_duration`, see [ADR 005](005-protocol.md#probe-triggered-eviction-hold)), which prevents false phantom-announcement slashing when cache pressure would otherwise evict a blob between probe and subsequent stream request.

**Maximum blob size:** Nodes may configure a `max_blob_size` (PoC recommended default: 10 GB). Requests for blobs exceeding this limit are rejected with `StreamError::BlobTooLarge` ([ADR 005](005-protocol.md#error-handling-and-retry-semantics)). This prevents a single large blob from exhausting cache capacity or tying up connections for extended periods. The limit is per-node — nodes with larger storage budgets can raise it; cache-only nodes on constrained hardware can lower it.

```mermaid
flowchart TD
    A[Client requests blob via StreamRequest] --> B{Node has blob in cache?}
    B -->|Hit| C[Stream from local cache]
    C --> D[Client pays per MB via vouchers]

    B -->|Miss| E{pull_through enabled?}
    E -->|Yes| F["Probe fan-out (cdn/probe/v1 to all known peers)"]
    F --> G["Collect has_blob:true responses (50ms min, 500ms max; early exit on good score)"]
    G --> H["Select best: unified selection score"]
    H --> I["Pull via cdn/client/v1 (node pays peer)"]
    I --> J[Cache locally + stream to client simultaneously]
    J --> D

    E -->|No| K[Return redirect with origin NodeId]
    K --> L[Client connects to origin-backed node directly]
```

---

## Crate Structure

```
decdn/
├── Cargo.toml                    # workspace root
├── crates/
│   ├── node/                     # Binary — CLI entry, config, wiring
│   ├── protocol/                 # Shared types, wire format, messages
│   ├── cache/                    # Cache engine wrapping iroh-blobs + origin pull
│   ├── incentive/                # Payment channels, staking, vouchers
│   ├── reputation/               # Gossip-based reputation system
│   └── contracts/                # Solidity contracts + Foundry
├── tests/                        # Integration tests
└── adr/                          # Architecture decision records
# The app server (ADR 006) is an external component, not part of this workspace.
# Content providers build it using their own stack. A reference implementation
# may be provided as a separate repository.
```

### Dependency Chain

```mermaid
graph TD
    node[node]
    cache[cache]
    incentive[incentive]
    reputation[reputation]
    protocol[protocol]

    iroh([iroh])
    iroh_blobs([iroh-blobs])
    iroh_gossip([iroh-gossip])
    alloy([alloy])
    serde([serde])
    postcard([postcard])

    node --> cache
    node --> incentive
    node --> reputation
    node --> protocol

    cache --> protocol
    cache --> iroh
    cache --> iroh_blobs

    incentive --> protocol
    incentive --> alloy

    reputation --> protocol
    reputation --> iroh_gossip

    protocol --> serde
    protocol --> postcard
    protocol --> iroh

    style node fill:#4a9eff,color:#fff
    style cache fill:#34d399,color:#fff
    style incentive fill:#f59e0b,color:#fff
    style reputation fill:#a78bfa,color:#fff
    style protocol fill:#f87171,color:#fff
```

`protocol` is the leaf crate with minimal dependencies. Everything depends on it; it depends on almost nothing. The cache and incentive layers are separate crates — the cache layer works without incentives (useful for testing, local dev, private deployments). The incentive layer wraps cache operations with payment logic. The `node` crate wires them together.

---

## External Components

Components referenced by ADRs that are operated by content providers, not part of the CDN protocol or workspace.

### App Server ([ADR 006](006-e2e-encryption.md))

The app server is operated by the content provider (e.g., a streaming platform's backend). It shares the iroh QUIC transport layer with the CDN but is **not** a CDN protocol participant — it does not participate in gossip, probing, or paid delivery.

**Responsibilities:**

- Stores blob encryption keys (`K_blob`) received from the origin at ingest time
- Authenticates client sessions and validates subscription status
- Delivers epoch keys over an authenticated `cdn/keys/v1` QUIC stream; signals `server_secret` rotation via `epoch_key_revoked` events on the same stream
- Issues envelopes containing epoch-key-wrapped `K_blob` on play requests
- Builds and returns offline playback leases (client seals locally to device keystore)

**Why iroh QUIC:** Key delivery is tightly coupled to the client's iroh identity — the QUIC handshake provides mutual authentication (client NodeId ↔ app server NodeId) and TLS 1.3 confidentiality in a single step, eliminating the need for `crypto_box_seal` and X25519 key management. This gives clients a single transport stack for both CDN delivery and key delivery, and makes iroh key rotation seamless (reconnect with new NodeId, re-authenticate with session token). The app server still handles subscription billing, OAuth/session auth, and key management — content providers integrate an `iroh::Endpoint` accepting `cdn/keys/v1` connections alongside their existing auth/billing infrastructure.

**Scaling model:** One iroh QUIC connection per active subscriber. Standard QUIC server scaling applies (connection migration, load balancer affinity). The app server scales with subscriber count, not CDN node count.

**PoC scope:** A minimal reference implementation may be provided in a separate repository. The CDN crates do not depend on it.

---

## Observability

The canonical metric registry, naming convention (`decdn_` prefix, `_total` suffix for
counters), mandatory vs. recommended tiers, alert thresholds, and `/health` endpoint
contract are defined in [ADR 020](020-observability.md). The summary below is for
orientation only — ADR 020 is authoritative.

- **Structured logging** via `tracing` crate (JSON in production).
- **Metrics** via `prometheus` crate, exposed at `:{port}/metrics` (default port 9090).
  Key metric groups: delivery (`decdn_streams_*`, `decdn_bytes_*`), cache
  (`decdn_cache_*`), payment channels (`decdn_channels_*`, `decdn_vouchers_*`), gossip
  (`decdn_gossip_*`, `decdn_peer_table_size`), and slash-safety (see below).
- **Health endpoint** at `:{port}/health` — JSON with `ready`/`degraded`/`not_ready`
  status, peer count, channel balances, and blacklist sync state.
- **Slash-risk metrics** (all mandatory — nodes must expose these at startup):
  - `decdn_probe_hold_violations_total` — phantom announcement risk ([ADR 005](005-protocol.md))
  - `decdn_probe_hold_slots_used` / `decdn_probe_hold_slots_max` — eviction-hold saturation
  - `decdn_rate_bounds_clamp_events_total` — rate outside governance bounds ([ADR 003](003-payments.md))
  - `decdn_blacklist_sync_lag_seconds` / `decdn_blacklist_version_behind` — compliance lag ([ADR 011](011-content-takedown.md))
  - `decdn_slash_evidence_exposure_total` — self-detected slashing contradiction ([ADR 005](005-protocol.md))

---

## Considered Alternatives

A fully decentralized storage model was evaluated: nodes would commit to durable storage with replication factor N, pinning deals, and replication maintenance protocols. This was rejected in favor of centralized storage (S3/R2) + decentralized delivery because:

- S3-class storage is cheap ($0.023/GB/month), reliable (11 nines), and already solved
- The actual bottleneck is delivery latency and bandwidth cost, not storage
- Decentralized storage requires complex pinning deals, replication verification, and challenge games
- The CDN model is strictly simpler: trust S3 for durability, decentralize only delivery

---

## Future Work: Search & Discovery

Not in PoC scope. The planned approach for the next phase:

Dedicated **indexer nodes** subscribe to gossip topics and respond to `cdn/probe/v1` queries to build a searchable index of content metadata (via `tantivy` or equivalent), exposing a query API on a custom ALPN (`cdn/search/v1`). Multiple independent indexers can coexist. Clients pay per query via the same payment channel mechanism. Indexers register in the `StakingRegistry` and are slashable for fabricated results.

Content discovery uses `cdn/dht/v1` from PoC onward — at 30 nodes, FIND_VALUE resolves in 1–2 hops and is negligible overhead. Indexers complement DHT by providing metadata search. Broadcast probe fan-out remains the bootstrap/emergency fallback.

---

## Future Work: KV-CRDT Content Catalogs

Not in PoC scope. iroh's KV-CRDT protocol (`iroh-docs`) provides a replicated key-value store with eventual consistency via range-based set reconciliation. Entries are `(namespace, author, key) → (BLAKE3 hash, size, timestamp)` — metadata only; actual content travels via iroh-blobs separately. This maps naturally to deCDN's content-addressing model.

**Primary use case — content catalog replication.** A KV-CRDT namespace per content provider could replicate a catalog of `hash → content metadata` entries across nodes. Nodes would learn what content exists before needing it, enabling smarter prefetching. This complements (not replaces) `cdn/dht/v1` — CRDT replication propagates metadata; DHT locates holders.

**Secondary use cases to evaluate:**

- **Node metadata.** A shared document keyed by `NodeId` could provide persistent, eventually-consistent node state (rates, capacity, regions) that survives reconnections — supplementing or replacing ephemeral gossip `NodeAnnounce` messages.
- **Watchtower voucher state.** A KV-CRDT keyed by `(channel_id, nonce)` between a watchtower and its client could keep voucher state consistent, simplifying the bespoke sync and heartbeat commitment described in [ADR 007](007-watchtower.md).
- **Indexer replication layer.** Indexer nodes (see [Search & Discovery](#future-work-search--discovery) above) could subscribe to content catalog namespaces and build their search index from replicated entries, rather than relying solely on gossip and probe participation.

**Why not in PoC:** DHT already handles content discovery at PoC scale. CRDT replication adds value at larger scale for smarter prefetching; deferred until the network grows beyond where DHT alone suffices.

**Reference:** [iroh-docs protocol](https://docs.iroh.computer/protocols/kv-crdts)

---

## What Is Not Decided Yet

- ~~Production L2 choice~~: decided — [ADR 021](021-l2-chain-selection.md) selects Arbitrum One (chain ID 42161). Sequencer censorship mitigation uses Arbitrum's 24h forced-inclusion path; see [ADR 007](007-watchtower.md#l2-sequencer-censorship)
- ~~Content discovery scaling strategy (DHT vs gossip hints)~~: decided — [ADR 022](022-content-discovery.md) specifies `cdn/dht/v1` as the primary discovery mechanism from day one, with broadcast probe fan-out as a bootstrap/emergency fallback; gossip content hints rejected
- ~~PoC→production feature-flag / toggle architecture~~: decided — [ADR 023](023-poc-production-seams.md) defines trait-based seams with a single `poc` Cargo feature on the `node` crate; `NetworkConstants` as the single source of truth for all numeric differences
- Parallel streaming from multiple nodes for a single blob (protocol supports it, not prioritised)
- ~~Maximum blob size~~: decided — nodes may configure a `max_blob_size` limit (PoC recommended default: 10 GB). Requests exceeding a node's limit are rejected with `StreamError::BlobTooLarge` ([ADR 005](005-protocol.md#error-handling-and-retry-semantics)). This is a per-node operational policy, not an on-chain governance parameter, because different nodes have different storage and bandwidth budgets
- ~~Schema evolution strategy for postcard wire messages~~: decided — [ADR 013](013-schema-evolution.md) defines varint-length framing, protocol enums, a three-tier evolution model, and a gossip envelope
- ~~On-chain verification for slash evidence (Ed25519 signatures, BLAKE3 mismatch)~~: decided — [ADR 014](014-on-chain-verification.md) specifies dual-key slash signatures (`SignatureChecker` for EOA + ERC-1271 smart-wallet verification), optimistic challenge-response for corruption, and a unified `SlashJudge` contract
- ~~NodeId ownership proof for registration~~: decided — [ADR 001](001-network.md#nodeid-ownership-verification) specifies on-chain ed25519 signature verification at registration time, with `reclaimNodeId` for production and `adminReclaimNodeId` as a PoC safety valve
- ~~Account abstraction / smart wallet support~~: decided — [ADR 024](024-account-abstraction.md) specifies ERC-1271 (`SignatureChecker`) in all contracts from the PoC, Safe as recommended wallet for nodes and clients, session keys for high-frequency signing (vouchers, slash_sig). Supersedes the delegated voucher signer approach (PR 196)
