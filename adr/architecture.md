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

Clients probe candidate nodes, pick the best by the unified selection score (see [ADR 001](001-network.md#node-selection-algorithm) for the full formula), stream over `cdn/client/v1`, and pay via off-chain payment vouchers (USDC in PoC). On a cache miss, a node discovers providers via probe fan-out (`cdn/probe/v1` to all known peers), selects the best, and pulls via `cdn/client/v1` (paid). Every byte delivered — whether client→node or node→node — is paid.

---

## Architectural Decisions

### [ADR 000 — Language and Core Networking Stack](000-language.md)

**Rust + iroh (0.97).**

The implementation language is Rust. The networking stack is iroh, which provides QUIC transport, NAT traversal, content-addressed blob transfer, and gossip as a cohesive unit. A single statically linked binary runs as a node or client depending on configuration.

---

### [ADR 001 — Network Topology and Peer Mesh](001-network.md)

**Flat peer mesh. Gossip for node discovery, probe fan-out for content discovery (DHT deferred to post-PoC).**

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

**USDC for payments. TOKEN for staking, governance, and fee discounts.**

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

**Admin key for PoC. Token-weighted governance with safety bounds for production.**

During the PoC, a single deployer address controls all contract parameters. Production governance uses OpenZeppelin Governor with TOKEN voting, 4% quorum, and a 2-day timelock. All governable parameters have hardcoded safety bounds that even governance cannot override (e.g., slash 5%–50%, dispute window 12h–72h; PoC deployments default the dispute window to 48h within this range). A 3-of-5 emergency multisig can only pause contracts and add emergency blacklist entries, with a 12-month sunset enforced via an immutable constructor deadline. A renewable sunset mechanism is recommended for production — governance can vote to extend the deadline by capped increments, preserving the anti-centralization default while maintaining emergency capability.

---

### [ADR 010 — Multi-Token Payment Support](010-multi-token.md)

**Token-agnostic payments with governance-managed ERC-20 allowlist. USDC-only for PoC.**

Extends ADR 003 to support multiple ERC-20 tokens. The production `PaymentChannel` contract maintains a governance-managed allowlist of approved tokens; `openChannel` reverts if the token is not on the allowlist. Per-token rate bounds are set by governance. A `payment_token` field is added to `StreamRequest` and `token_rates` is added alongside the single `rate_per_mb` in gossip advertisements (nodes accepting only USDC may retain the single field for simplicity). The EIP-712 voucher already carries a `token` field from ADR 003 — no signature scheme migration is needed. Production deploys a new `PaymentChannel` contract (not an upgrade of the PoC `StablePaymentChannel`). Channels in governance-removed tokens can be force-closed by any address via `forceCloseChannel`, entering the standard dispute/settle flow.

---

### [ADR 011 — Content Takedown and Hash Blacklisting](011-content-takedown.md)

**Governance-controlled on-chain hash blacklist with regional bodies and emergency fast-path.**

A `ContentBlacklist` contract supports global (network-wide) and regional (jurisdiction-scoped) takedown via designated regional governance bodies. Standard governance entries have a 24-hour compliance window; the emergency multisig path takes effect immediately with a 2-hour slash window. Emergency entries auto-expire after 14 days unless ratified by governance; entries categorized as CSAM or terrorist content use a 90-day auto-expiry to prevent re-exposure due to governance latency. Origin blacklisting by operator address counters hash evasion via trivial re-encoding — each re-upload requires fresh stake and a new identity. Each node also maintains a local denylist for direct legal notices. Serving a blacklisted hash after the compliance window is a slashable offense, subject to the escalating schedule in [ADR 004](004-tokenomics.md).

### [ADR 012 — Client Architecture, Bootstrap, and Trust Model](012-client.md)

**Client bootstrap, key management, identity lifecycle, and trust boundary.**

Clients are lightweight QUIC endpoints that subscribe to gossip (but do not publish), maintain local peer tables and reputation scores, and pay for content via off-chain vouchers. The bootstrap procedure covers iroh key generation, Ethereum key import, registry query with exponential-backoff retry and `peers.json` fallback, gossip subscription, and periodic registry refresh. Key management distinguishes PoC (file-based) from production (platform keychain, hardware wallet with derived hot key for voucher signing). Ephemeral NodeId-to-Ethereum bindings are per-connection with `nonce=0` sentinel. Eclipse attack mitigation is resolved: registry-only for PoC; multi-source bootstrap (Option B — on-chain registry + DNS seed list) for production, with minimum peer diversity (Option C) as supplementary client-side policy. An explicit three-tier trust boundary classifies what the client verifies, trusts, and does not trust.

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

**Protocol-Owned Liquidity in a Balancer V2 80/20 TOKEN/USDC weighted pool, seeded from the genesis liquidity allocation.**

Reverses the implicit Uniswap V3 venue choice in prior ADRs. Balancer 80/20 weighted pools require roughly 1/4 the USDC of a 50/50 position for comparable near-spot depth (critical for a TOKEN-rich, USDC-poor treasury), eliminate concentrated-liquidity range-management overhead (no `LiquidityManager`, no keeper for rebalancing), and reduce impermanent loss by ~1.75× for a 2× TOKEN price move (~3.3% vs ~5.7%), aligning the DAO's IL profile with the TOKEN-upside thesis. The DAO treasury holds BPT directly; no LP rewards or liquidity mining. `BuybackBurner.executeBuyback()` swaps USDC → TOKEN via `BalancerV2Vault.swap()` with native MEV protection available through CoW Swap batch-auction routing. The `IBuybackBurner` interface adds one setter (`setPoolId(bytes32)`) to stay venue-symmetric, preserving the option to supplement with a Uniswap V3 position in a future ADR once treasury USDC reserves and keeper infrastructure justify it.

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

1. **Paid pull-through (preferred):** Node checks its probe cache or performs a probe fan-out (`cdn/probe/v1` to all known peers), selects the best provider by the unified selection score ([ADR 001](001-network.md#node-selection-algorithm)), pulls via `cdn/client/v1` (paid), caches locally, and streams to the client while the pull is in progress.
2. **Redirect (last resort):** If pull-through is disabled (`pull_through: false` in config), the node returns a redirect to an origin-backed node's NodeId. The client opens a channel with that node directly.

**Prefetching:** Nodes can proactively cache popular content using two signals: (1) local demand — tracking cache miss frequency per hash and prefetching when a threshold is crossed (default: 3 misses in 5 minutes); (2) network popularity — observing which hashes appear in multiple peers' `popular_hashes` fields in `NodeAnnounce` gossip messages (default threshold: 3+ peers within 10 minutes). All prefetch pulls use the same probe fan-out → `cdn/client/v1` path (paid).

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

- **Structured logging** via `tracing` crate (standard in iroh ecosystem). JSON output for machine consumption.
- **Metrics** via `prometheus` crate, exposed on a configurable HTTP port:
  - `streams_active`, `streams_completed`, `streams_failed` — delivery activity
  - `vouchers_signed`, `vouchers_received` — payment activity
  - `reputation_reports_sent`, `reputation_reports_received` — gossip health
  - `channels_open`, `channels_settled` — payment channel lifecycle
  - `cache_hits`, `cache_misses`, `cache_bytes` — cache performance
- **Health endpoint** at `/health` on the metrics HTTP port — returns node status, peer count, and channel balances
- **Slash-risk metrics** — early warning for conditions that can lead to slashing (see [ADR 004](004-tokenomics.md)):
  - `probe_hold_violations` — times a blob was evicted within `probe_hold_duration` after signing `has_blob: true` (phantom announcement risk — [ADR 005](005-protocol.md))
  - `probe_hold_slots_used` — current occupied hold slots out of `max_probe_holds` (saturation signal — [ADR 005](005-protocol.md))
  - `rate_bounds_clamp_events` — times `rate_per_mb` was clamped to governance bounds before signing ([ADR 005](005-protocol.md))
  - `blacklist_sync_lag_seconds` — seconds since last successful `getBlacklistVersion()` poll ([ADR 011](011-content-takedown.md))
  - `blacklist_version_behind` — gap between local and on-chain blacklist version ([ADR 011](011-content-takedown.md))
  - `slash_evidence_exposure` — times the node detected it produced a signed probe + stream pair meeting slashing contradiction conditions within the 30-second window ([ADR 005](005-protocol.md))

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

Dedicated **indexer nodes** subscribe to gossip topics and participate in probe fan-out (responding to `cdn/probe/v1` queries) to build a searchable index of content metadata (via `tantivy` or equivalent), exposing a query API on a custom ALPN (`cdn/search/v1`). Multiple independent indexers can coexist. Clients pay per query via the same payment channel mechanism. Indexers register in the `StakingRegistry` and are slashable for fabricated results.

During PoC (before indexers exist), content discovery uses probe fan-out — every cache miss probes all known peers via `cdn/probe/v1`. At PoC scale (tens of nodes), this provides complete coverage. The migration to indexers or DHT-based discovery is additive — probe fan-out remains the fallback.

---

## Future Work: KV-CRDT Content Catalogs

Not in PoC scope. iroh's KV-CRDT protocol (`iroh-docs`) provides a replicated key-value store with eventual consistency via range-based set reconciliation. Entries are `(namespace, author, key) → (BLAKE3 hash, size, timestamp)` — metadata only; actual content travels via iroh-blobs separately. This maps naturally to deCDN's content-addressing model.

**Primary use case — content catalog replication.** A KV-CRDT namespace per content provider could replicate a catalog of `hash → content metadata` entries across nodes. Nodes would learn what content exists before needing it, enabling smarter prefetching and reducing probe fan-out pressure as the network scales beyond PoC. This is the most natural replacement for brute-force probe fan-out at scale.

**Secondary use cases to evaluate:**

- **Node metadata.** A shared document keyed by `NodeId` could provide persistent, eventually-consistent node state (rates, capacity, regions) that survives reconnections — supplementing or replacing ephemeral gossip `NodeAnnounce` messages.
- **Watchtower voucher state.** A KV-CRDT keyed by `(channel_id, nonce)` between a watchtower and its client could keep voucher state consistent, simplifying the bespoke sync and heartbeat commitment described in [ADR 007](007-watchtower.md).
- **Indexer replication layer.** Indexer nodes (see [Search & Discovery](#future-work-search--discovery) above) could subscribe to content catalog namespaces and build their search index from replicated entries, rather than relying solely on gossip and probe participation.

**Why not in PoC:** At tens of nodes, probe fan-out provides complete coverage and is simpler. Adding a CRDT replication layer is worthwhile only when the network grows large enough that probing all peers becomes expensive. The migration is additive — probe fan-out remains the fallback.

**Reference:** [iroh-docs protocol](https://docs.iroh.computer/protocols/kv-crdts)

---

## What Is Not Decided Yet

- Production L2 choice (Arbitrum One, Base, or other) — gated on PoC validation. Sequencer censorship mitigation for the dispute window is addressed in [ADR 007](007-watchtower.md#l2-sequencer-censorship) (PoC: 48h default; production: forced-inclusion deadline extension); the extension's detection logic depends on the L2 chosen
- Parallel streaming from multiple nodes for a single blob (protocol supports it, not prioritised)
- ~~Maximum blob size~~: decided — nodes may configure a `max_blob_size` limit (PoC recommended default: 10 GB). Requests exceeding a node's limit are rejected with `StreamError::BlobTooLarge` ([ADR 005](005-protocol.md#error-handling-and-retry-semantics)). This is a per-node operational policy, not an on-chain governance parameter, because different nodes have different storage and bandwidth budgets
- ~~Schema evolution strategy for postcard wire messages~~: decided — [ADR 013](013-schema-evolution.md) defines varint-length framing, protocol enums, a three-tier evolution model, and a gossip envelope
- ~~On-chain verification for slash evidence (Ed25519 signatures, BLAKE3 mismatch)~~: decided — [ADR 014](014-on-chain-verification.md) specifies dual-key slash signatures (`ecrecover` at 3,000 gas), optimistic challenge-response for corruption, and a unified `SlashJudge` contract
- ~~NodeId ownership proof for registration~~: decided — [ADR 001](001-network.md#nodeid-ownership-verification) specifies on-chain ed25519 signature verification at registration time, with `reclaimNodeId` for production and `adminReclaimNodeId` as a PoC safety valve
