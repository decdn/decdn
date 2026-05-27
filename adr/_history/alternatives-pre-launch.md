# Pre-Launch Alternatives Considered

> **Audit trail.** This file collects the `## Alternatives Considered` sections that previously lived inline across the ADR set. It is **not** part of the canonical protocol specification — it is a record of design alternatives evaluated and rejected before launch, retained so future contributors can see what was on the table without inferring it from the current design. Tracked in [#346](https://github.com/decdn/decdn/issues/346); see [`adr/README.md` § Decision-record context](../README.md#decision-record-context) for the project-level framing.
>
> Neither the numeric `adrs.pdf` nor the reading-order `adrs-book.pdf` build includes this file, and the ADR bodies intentionally do **not** link to it — there is no `## Alternatives Considered` stub or breadcrumb in any ADR. Readers approaching the protocol get the canonical spec; this archive is browsed directly, by the per-ADR sections below.
>
> **Related: retired full ADRs.** Full ADRs that were accepted, then retired when a later spec superseded the underlying mechanism, live as standalone files alongside this one — a different artifact category than the rejected-pre-launch-alternative entries below. Current retirees: [`034-gauge-boost-voting-escrow.md`](034-gauge-boost-voting-escrow.md) and [`035-delegator-pool.md`](035-delegator-pool.md), both retired by the v2.1 work-token rewrite (replaces the ve-gauge model with the `CapacityBond` lock-to-capacity curve). Each retired file has a top-of-file retirement banner with the canonical pointer; original bodies are preserved verbatim.

Sections below are anchored by source ADR. Cross-references back to each source use relative paths (`../NNN-name.md`).

---

## ADR 010 — Multi-Token Payment Support (dropped)

Source: ADR 010 was removed entirely. The payment-token decision now lives in [ADR 003 — Payment Model](../003-payments.md): the payment token is **USDC, with its address fixed at contract deployment** (immutable constructor argument). There is no governance token allowlist, no `addToken`/`removeToken`, no per-token rate bounds, no token-keyed channel IDs, and no on-protocol price oracle or swap. Decision recorded in [#591](https://github.com/decdn/decdn/issues/591); supersedes the multi-stablecoin variant [#583](https://github.com/decdn/decdn/issues/583) / PR [#585](https://github.com/decdn/decdn/pull/585).

ADR 010 had proposed a token-agnostic `PaymentChannel` with a governance-managed ERC-20 allowlist (motivated by network heterogeneity and Circle-freeze censorship resistance). A later revision narrowed it to a multi-*stablecoin* allowlist to avoid a price oracle. Both were rejected pre-launch.

| Approach | Why Not |
| --- | --- |
| Arbitrary-ERC-20 allowlist (`PaymentChannel`, original ADR 010) | Largest attack surface: fee-on-transfer / rebase / pausable / hook-bearing tokens each break channel accounting; cross-token value comparison for reputation weighting needs a price oracle; per-token rate bounds, `forceCloseChannel`-on-removal, and a Token Vetting Checklist add standing governance burden and audit scope. |
| Multi-stablecoin allowlist (revised ADR 010 / [#583](https://github.com/decdn/decdn/issues/583)) | Removes the oracle but keeps the allowlist machinery (`addToken`/`removeToken`, per-token rate bounds, vetting checklist, token-removal force-close, per-token decimals) and the multi-token contract/reentrancy surface — still materially more complex than launch requires. |
| Per-node arbitrary token (no governance gate) | Lets adversarial ERC-20s touch channel funds with no vetting; rejected even within ADR 010's own framing. |
| **Single immutable USDC address, set at deployment (chosen)** | One token, no allowlist, no oracle, no swap, no per-token machinery. Smallest contract and audit surface. Holders of other assets swap into USDC off-protocol before opening a channel. Circle-freeze counterparty risk is accepted (stated in ADR 003 Consequences). |

---

## ADR 014 — Slash Signature Scheme

Source: [ADR 014 § 1 — Slash Signatures (secp256k1 EIP-712)](../014-on-chain-verification.md#1-slash-signatures--secp256k1-eip-712).

The alternatives below are scoped to the choice of signature scheme used for on-chain slash evidence. The chosen design — secp256k1 EIP-712 `slash_sig` verified via `ecrecover` — is documented in ADR 014 § 1. (The original draft also retained an Ed25519 wire signature alongside `slash_sig`; this redundancy was removed pre-launch — see [#408](https://github.com/decdn/decdn/issues/408).)

| Approach | Gas Cost | PoC Suitability | Why Not |
| --- | --- | --- | --- |
| RIP-7212 Ed25519 precompile | ~3,000 | Not available | Not yet deployed on the production L2 as of 2026-04 (see [Appendix: L2 Deployment](../appendix-l2-deployment.md)) |
| Solidity Ed25519 library (e.g., `ed25519-sol`) | ~500k–1M | Too expensive | A single slash verification would cost $0.25–$0.50; two-signature offenses double that |
| ZK proof of Ed25519 signature | ~300k verify | Too complex | Requires a proving circuit, prover infrastructure, and proof generation latency |
| Optimistic (no signature verification) | ~50k | Insufficient security | A node could deny authorship of any message; counter-evidence alone is not enough |
| **secp256k1 EIP-712 `slash_sig` (chosen)** | **~3,000** | **Recommended** | Uses proven `ecrecover`; one signature per message; reuses existing NodeId↔Eth binding from `StakingRegistry` |

---

## ADR 018 — Liquidity Strategy

Source: [ADR 018 — Liquidity Strategy (Balancer 80/20 POL)](../018-liquidity-strategy.md).

### Uniswap V3 (and when to revisit)

Uniswap V3 concentrated liquidity remains a reasonable choice *if and when*:

- The treasury has sufficient USDC reserves to seed a 50/50 concentrated range without depleting operational runway, and
- The team has bandwidth to operate a range-management keeper, and
- Fee revenue from active management materially exceeds the operational cost.

None of these hold at PoC or early production. A future ADR may introduce a supplemental V3 position alongside the Balancer pool once the treasury accumulates USDC from organic fee flow. Because `IBuybackBurner` is venue-agnostic, adding a second venue does not require changes to ADR 018.

**Quantitative revisit triggers (placeholders).** Governance SHOULD consider revisiting the venue decision when any of the following hold. The specific thresholds are placeholders to be tuned in the revisiting ADR once production data is available:

- Treasury USDC reserves exceed 12 months of projected operational runway, freeing capital for a 50/50 V3 range without risking bootstrap subsidies.
- Monthly Balancer LP fee revenue on the POL position falls below a threshold of protocol fee revenue (placeholder: 5%) for a sustained period (placeholder: 3 months), indicating the pool is under-trafficked.
- The `BuybackBurner` has been unable to execute a buyback within `slippageBps` for a sustained period due to pool depth, even after POL top-ups.
- Aggregator routing or CoW Swap coverage of the Balancer pool regresses materially, reducing effective MEV protection or price discovery.

These are governance-policy guidelines, not on-chain enforcement. The revisiting ADR is responsible for setting concrete thresholds informed by production data.

---

## ADR 022 — Content Discovery at Scale

Source: [ADR 022 — Content Discovery at Scale (DHT)](../022-content-discovery.md). The canonical decision — `cdn/dht/v1` (Kademlia subset) as the primary content-discovery mechanism, with the on-chain origin directory as the deterministic last-resort fallback — is documented inline in ADR 022. The rejected alternatives:

### Broadcast probe fan-out as primary mechanism

An earlier design in [ADR 001](../001-network.md#adr-001-network-topology-and-peer-mesh) used **broadcast probe fan-out**: on a cache miss, a node sends a `cdn/probe/v1` message to every known peer simultaneously. Tolerable at network sizes of tens of nodes, but fails as N grows:

- **O(N) probes per cache miss.** At 1,000 nodes each cache miss generates ~1,000 outbound probe messages. Under a 10 fan-outs/second rate limit that is 10,000 probe messages/second/node — a self-DoS risk and a meaningful burden on the probed peers.
- **O(N) probe overhead for the prober.** Even rate-limited, fan-out latency grows with N: the node waits for the probe collection window on each of those N connections.

The O(N) cost is a design ceiling rather than an operational limit, and the protocol should not be architected around it at any N. The registry-seeded DHT bootstrap path ([ADR 022 § Bootstrap](../022-content-discovery.md#bootstrap)) removes any need for a fallback discovery mechanism.

### Gossip content announcements

Each cache/evict event generates a gossip `ContentAnnounce` message. Rejected: content churn is proportional to demand × network size, not to a configurable interval like `NodeAnnounce`, so traffic is unbounded by design. High-demand blobs with frequent cache rotation produce interleaved announce/retract storms.

### Hash-prefix range hints in `NodeAnnounce`

A node advertises "I hold hashes in prefix range 0x00–0x3F" via gossip, and queriers route lookups by prefix overlap. Rejected: nodes are economically incentivised to cache **popular** content regardless of hash prefix, so range hints would be uniformly meaningless in an incentive-driven network.

### iroh mainline DHT (`DhtDiscovery` / pkarr)

Rejected for this use case. iroh's built-in DHT resolves `NodeId → address` on the public mainline BitTorrent DHT. It does not support content-hash records, is not scoped to the deCDN registered-node set, and exposes lookup patterns to the public internet. A separate content DHT scoped to the registered node set is required, which is what `cdn/dht/v1` provides.

### Indexer nodes (`cdn/search/v1`)

Not rejected — deferred. Dedicated indexer nodes aggregating DHT records into a searchable catalog are a natural complement once the network grows large enough to justify a separate indexing tier. Out of scope for ADR 022.

### Full libp2p Kademlia

Deferred. `libp2p-kad` is battle-tested but built on libp2p's transport stack. Bridging to iroh QUIC adds a large dependency and upstream governance coupling. The lightweight subset specified in ADR 022 covers the deCDN use case with ~300–500 lines of Rust.

---

## ADR 026 — Gauge-Boost Tokenomics

Source: [ADR 026 — Gauge-Boost Tokenomics](../026-tokenomics.md).

- **Original tokenomics shape** — 3% flat protocol fee, regressive stake-multiple fee discount, 200M-TOKEN bootstrap fund, 50/50 challenger/burn slashing. **Rejected:** burn was structurally noise (~0.014%/yr supply vs. ~24%/yr vesting per §1); no yield path for passive holders or long-term lockers; the stake-multiple discount weakened the deflationary sink as operators qualified; TOKEN-denominated bootstrap was reflexive against price drops.
- **Auto-ve-lock-on-vest** — vesting contracts auto-lock released TOKEN into `VotingEscrow` for a fixed term before delivery. **Rejected:** the §3 gauge boost is a stronger voluntary incentive than a forced lock; auto-lock complicates seed/team term sheets and adds audit surface; the thinner-initial-veTOKEN cost is absorbed by §9's optional ve-lock-on-claim airdrop.
- **USDC distribution to a passive ve-pool** — distribute §6's 7% delegator bucket as USDC directly to ve-lockers (skip USDC→TOKEN swap). **Rejected:** decouples ve-locker yield from TOKEN appreciation, removing the "real yield in TOKEN" lever and the demand-side TWAP buy pressure that compounds with operator-side ve-locking.
- **Pure-deflationary slashing (50/50 challenger/burn)** — §8 with no SafetyReserve share. **Rejected:** user-harm incidents need a funded recourse path; the 3% router share alone can't seed it at early scale; the 20% burn share preserves a meaningful deflationary lever within §11's `[0%, 25%]` bound.
- **TOKEN-denominated node-bootstrap fund** — protocol-issued multi-hundred-million-TOKEN fund disbursed to early operators. **Rejected:** subsidy purchasing power tracks TOKEN price (least valuable when most needed); concentrates pre-launch dilution against bootstrap duration rather than network outcomes. Replaced by §10's externally-raised USDC pre-seed.

---

## Architecture Overview — Decentralized Storage vs Decentralized Delivery

Source: [Architecture Overview](../architecture.md).

A fully decentralized storage model was evaluated: nodes would commit to durable storage with replication factor N, pinning deals, and replication maintenance protocols. This was rejected in favor of centralized storage (S3/R2) + decentralized delivery because:

- S3-class storage is cheap ($0.023/GB/month), reliable (11 nines), and already solved
- The actual bottleneck is delivery latency and bandwidth cost, not storage
- Decentralized storage requires complex pinning deals, replication verification, and challenge games
- The CDN model is strictly simpler: trust S3 for durability, decentralize only delivery

---

## Encrypted Content Publishing (appendix)

Source: [Appendix: Encrypted Content Publishing](../appendix-encrypted-content-publishing.md).

### Client-enforced expiry (timestamp in envelope, no epoch keys)

The app server wraps `{K_blob, expires_at}` and sends it to the client. The client checks the timestamp before decrypting.

Rejected because a hacked client can ignore the timestamp. Expiry becomes advisory, not enforced. Acceptable for a PoC but not for production subscription gating.

### Decryption proxy (server-side decryption)

A proxy fetches ciphertext from the CDN, decrypts with K_blob, and streams plaintext to the client over TLS. The client never sees any key.

Rejected because it exposes plaintext to a non-authorized intermediary, defeating the "only authorized clients can decrypt" property. It also introduces a centralized bottleneck that undermines the decentralized CDN architecture.

### Proxy re-encryption (PRE)

The origin encrypts under its own key. A re-encryption proxy transforms ciphertext for each authorized client without learning the plaintext.

Rejected for the PoC due to complexity (BLS12-381 pairing-based crypto), performance overhead, and a significant new dependency (`recrypt`). May be revisited post-PoC if delegated access without origin involvement becomes a requirement.

### Per-client encrypted blobs (ECIES per recipient)

Each blob is re-encrypted per client, producing different ciphertexts and different BLAKE3 hashes.

Rejected because it destroys global content-addressing. The same track would have a different hash per client, breaking CDN caching, deduplication, and gossip announcements.

---

## Local Admin HTTP Surface (appendix)

Source: [Appendix: Local Admin HTTP Surface](../appendix-local-admin-http.md).

- **Hand-rolled hyper with REST-style routes.** The original draft of this appendix (and the initial #247 implementation) went this way. It works, but the ergonomic cost grows per-method: paired server handler and client parser, no shared schema between them, verb/URI choices argued case by case. Migrated to jsonrpsee before the first follow-up method (`drain`, #244) would have doubled that maintenance surface.
- **jsonrpsee + OpenRPC spec generation (`typed-openrpc`, `yerpc`).** OpenRPC is the JSON-RPC analog of OpenAPI; both ecosystem crates that generate it from Rust are thin/early (handful of stars, one-person maintenance). For a loopback surface with a small method count, hand-maintained docs are cheaper than a generator dependency. Revisit if the surface outgrows ~10 methods.
- **Unix domain socket.** Better multi-user isolation; worse portability and higher client-side friction. Revisit if multi-tenant hosts enter scope.
- **New iroh ALPN (e.g. `cdn/admin/v1`).** NodeId-based auth is attractive for remote admin, but this surface is specifically not remote — running it over iroh would pull in relay traffic, QUIC handshakes, and the iroh connection lifecycle for what needs to be a zero-dependency, always-on-localhost debug channel. Keeping admin off the iroh ALPNs also means a buggy admin route cannot affect the CDN wire protocol.
- **Extending `/metrics` with non-Prometheus routes.** Mixes a scraped time-series surface with mutating operations; operators would have to lock down the metrics endpoint more aggressively than they do today.

---

## PoC/Production Seam Architecture (appendix)

Source: [Appendix: PoC/Production Seam Architecture (Rust)](../appendix-poc-production-seams.md).

### `cfg!()` macro with `if`/`else` in a single function

Considered (and initially implemented) as:

```rust
fn network_constants() -> NetworkConstants {
    if cfg!(feature = "poc") {
        NetworkConstants::poc()
    } else {
        NetworkConstants::production()
    }
}
```

Rejected. `cfg!()` is a macro that evaluates to `true`/`false` at compile time, but **both branches are still compiled**. The compiler may optimize away the dead branch, but this is not guaranteed — PoC types (`FileKeyStore`, `NoopWatchtowerClient`) may be present in the production binary. The `#[cfg()]` attribute form on separate function definitions provides a hard guarantee: excluded code is never compiled, never linked, and never present in the binary.

### Two explicit features: `poc` and `prod`

Considered as an alternative to single `poc` with `not(poc)`. Rejected because it introduces a third invalid state (neither feature set, or both set simultaneously) that requires a `compile_error!` guard to catch. It also makes PoC removal harder: after migration, every `#[cfg(feature = "prod")]` attribute must be stripped from what are now the only implementations. With single `poc` + `not(poc)`, production functions need no attribute changes at all — the `not(poc)` attribute simply disappears with the feature declaration.

### Runtime `NetworkMode` enum throughout

Rejected. Leads to `if mode == PoC` branches scattered across all crates. Makes it impossible to statically verify that no PoC code runs in a production binary.

### Single implementation with `Option`-typed production fields

Rejected. `Option<WatchtowerClient>` forces every call site to unwrap and handle the None case, which is just a verbose runtime mode-check with worse ergonomics.

### Two separate repositories

Rejected. Shared protocol types, cache logic, and contract interaction code is large enough that duplication would create divergence. The trait abstraction achieves the same clean separation within a monorepo.

### Compile-time `#[cfg(feature = "poc")]` throughout all crates

Rejected. Scatters the PoC/production boundary into every crate, making it hard to track all the differences and audit the production surface. Centralizing in `wiring.rs` gives a single readable inventory.

---

## ADR 028 — Slashing Appeals

Source: [ADR 028 — Slashing Appeals and Dispute Escalation](../028-slashing-appeals.md).

- **ve-Governor-only path (no multisig fast-track).** Rejected: ~9-day minimum governance latency (7d voting + 48h timelock per [ADR 009](../009-governance.md#production-ve-weighted-governance)) is too slow for an operator who needs working-capital relief during an active business. The multisig fast-track + ratification structure is borrowed exactly from [ADR 011 § Regional Governance Bodies](../011-content-takedown.md#regional-governance-bodies) for the same reason.
- **Dedicated arbitration committee.** Rejected: introduces a new on-chain governance body, a new election mechanism, and a new attack surface, none of which is justified by the appeal volume the protocol expects (single-digit appeals per quarter at PoC scale, low-tens at production scale).
- **On-chain slash reversal.** Rejected: clawback on already-distributed challenger rewards (50% of slashed amount per [ADR 026 §8](../026-tokenomics.md#8-slashing-and-burn)) is intractable — the challenger may have already moved the funds. `SafetyReserve` restitution is equivalent in capital terms and avoids the clawback complexity entirely. Reputation/offense-count preservation is a feature, not a bug (§7).
- **Hybrid stake reversal + reputation reset.** Rejected for the same clawback reason, plus the reputation-preservation rationale in §7.
- **Narrowing scope to a subset of offenses.** Rejected: phantom, rate, and blacklist all execute immediately with no in-protocol due process; restricting appeals to a subset would leave a corresponding portion of operator-trust gap unaddressed.

---

## ADR 030 — Node Region Self-Attestation

Source: [ADR 030 — Node Region Self-Attestation](../030-node-region-self-attestation.md).

- **IP-geolocation oracle (Chainlink Functions, governance-approved oracle set, or similar).** Rejected. (a) Introduces a centralized trust root — the oracle operator(s) become a choke point that can blackhole or misclassify a node's region; (b) systematically misclassifies legitimate deployments behind VPN, anycast, IXP relays, or mobile/cellular allocations; (c) creates a net-new external dependency for the codebase (no oracle infrastructure exists in `crates/incentive/` or the contracts directory today); (d) does not produce a strictly better signal than the latency/reputation loop already in place — IP-geolocation databases are themselves imperfect heuristics over BGP allocations.
- **Required attestation at announce time (every `NodeAnnounce` carries a fresh oracle-signed attestation; gossip drops announces without one).** Rejected for the same reasons plus a harder operational floor — a brief oracle outage now causes every node's announces to age out, partitioning the gossip mesh until the oracle recovers.
- **Peer-witnessed latency challenge with on-chain dispute (mirror [ADR 028 § 3](../028-slashing-appeals.md#3-eligibility-and-evidence-standard) evidence-bundle pattern for region claims).** Rejected as overengineered. The same latency signal is already used at lower cost by the [ADR 001 § Consequences](../001-network.md#consequences) reputation penalty; promoting it to an on-chain adjudication path adds bond economics, multisig load, and a new ratification window without changing the operational outcome (a misdeclaring node already loses payouts under the soft path).
- **Scoping-only ADR (document the requirements/interface, defer mechanism).** Rejected — this is the posture issue #400 already objects to, and re-issuing it under a new ADR number does not close the gap.

---

## ADR 031 — ContentBlacklist Appeal-Contract Surface

Source: [ADR 031 — ContentBlacklist appeal-contract surface](../031-content-blacklist-appeals-contract.md).

- **Separate `BlacklistAppealRegistry` contract.** Rejected for the reason stated under § Decision: cross-contract hops on every transition, no audit-surface savings, and the cleanup admissibility tests depend on `ContentBlacklist` state anyway.
- **Per-appeal escrow contract** (one contract per active appeal, holding its own bond). Rejected as massive deployment overhead for no benefit; `ContentBlacklist` itself custodies bonds and burns / refunds them inline.

---

## ADR 032 — SafetyReserve Appeal-Surface

Source: [ADR 032 — SafetyReserve appeal-surface contract surface](../032-safety-reserve-appeals-contract.md).

- **Dedicated `SlashAppealRegistry` contract.** Rejected for the reason stated under § Decision: cross-contract hops on every transition, no audit-surface savings, and the `payout()` integration would need to be re-exposed. Mirrors ADR 031's rejection of `BlacklistAppealRegistry`.
- **Per-appeal `counterBundleFiler` storage field.** Considered for §2's bond-routing dispatch (counter-bundle filer recorded at gate-3 acceptance, read at `reverseAppeal`). Rejected: the gate-3 state already exists on `SafetyReserve` proper; duplicating it into the `Appeal` struct would cost another slot per appeal and require two writes (gate-3 + appeal) on every counter-bundle acceptance. The current design reads gate-3 state directly and surfaces the recipient via the `bondSplitRecipient` event field.
- **Five-condition `LapseReason` enum (ADR 031 style).** Rejected: slash appeals have only two lapse triggers (multisig timeout, ratification timeout), with no standing-path or global-override analogue. A two-enum mapping would over-engineer the case set; the `(escrowReturned == 0)` test suffices for off-chain disambiguation.

---

## Blob Cache Eviction Policy (appendix)

Source: [Appendix: Blob Cache Eviction Policy](../appendix-blob-cache-eviction.md).

- **LFU.** Rejected. Per-hash hit-counter bookkeeping grows without decay heuristics; counters are gameable by an attacker who repeatedly probes a low-value blob to keep it resident, wasting cache capacity on adversarial-popular content. LRU's "recently useful" proxy is robust enough for PoC scale and resists the same attack (the attacker must keep accessing the blob, paying per access — the cost defends the policy).
- **Size-weighted (largest-first).** Rejected. Penalizes the legitimate large-blob use case (video, datasets) the network is designed for. A 1 GB blob would always evict before a 1 MB blob even when both are equally hot, defeating the purpose of a CDN cache for large content.
- **Hybrid LRU + LFU (e.g. SLRU, ARC, W-TinyLFU).** Rejected for PoC. The bookkeeping overhead and parameter-tuning burden ("how do we set the segment ratio?") buy a marginal hit-rate gain at scales orders of magnitude larger than the PoC. Revisit at production hardening if cache-hit telemetry shows a clear miss-rate floor LRU is responsible for.
- **No eviction (rely on `cache_size_mb` as a soft hint).** Rejected. The cache is bounded storage; unbounded growth either wedges the disk or relies on the operator manually evicting via `decdn node evict` — neither acceptable. The driver loop is deferred (see *Negative consequences*) but the policy is mandatory.
- **Reputation-priority eviction.** Rejected, mirrors [appendix-peer-table-eviction.md §4](../appendix-peer-table-eviction.md#4-reputation-does-not-factor-into-eviction). Conflates retention with selection; creates a collusive-reporting vector against ADR 008's hard floor; the cache layer should not consult reputation at all.
- **Refresh `last_accessed` on every probe / `has` check.** Rejected. A coordinated probe flood from many peers would refresh every cached hash to "recent" and turn the LRU policy into approximate FIFO. Refresh on `get` only — the paid-delivery path — ties recency to the operator's revenue signal, which is the right alignment.

---

## Peer Table Eviction Policy (appendix)

Source: [Appendix: Peer Table Eviction Policy](../appendix-peer-table-eviction.md).

- **Reputation-priority eviction.** Rejected. Conflates discovery with selection (§4); creates a collusive-reporting vector against ADR 008's hard reputation floor; punishes transient noise. Reputation already governs selection via the score formula, the right place for it.
- **Lazy deregistration (TTL-only, no active evict).** Rejected. The registry-cache subscriber already runs on every event; marginal cost is one `HashMap::remove`. Lazy handling would leave a deregistered node visible to operators and analytics for up to TTL with no benefit.
- **LRU under a hard size cap.** Rejected. Adds eviction-priority bookkeeping for a problem the staking registry already bounds. If observed `peer_table_size` exceeds `registered_node_count × 1.5` in production, revisit — but the right next step is a registry-validation audit, not an LRU layer.
- **Persisting the peer table across restarts.** Rejected. Restart cost is < one announce interval (~60 s) of cold gossip; durability machinery is not justified.
- **Eviction by `announce.timestamp_us` rather than `last_seen_us`.** Rejected. Couples eviction to peer wall-clock instead of receiver wall-clock; creates surprises when peer clocks drift within the ±60 s skew window. The existing implementation correctly uses `last_seen_us`.
- **Shorter TTL aligned to a single announce interval (60 s).** Rejected. Below 2× announce interval a single dropped announce evicts a healthy peer; PlumTree gossip is best-effort, so single drops occur.

---

## ADR 003 — Probe-Fishing Rate-Limit Alternatives

Source: [ADR 003 § Attack Vectors → Probe fishing](../003-payments.md#probe-fishing). The chosen mitigation — the layered per-peer / per-IP / global token-bucket rate limit applied before any signature or hold-slot allocation ([ADR 005 § Probe rate limiting](../005-protocol.md#probe-rate-limiting)) — is documented inline in ADR 003. The rejected alternatives:

- **Option B — Require an open channel to probe.** Rejected. Creates a bootstrap catch-22: clients need probe results (rate, latency) to choose a node before opening a channel, but this requires a channel before probing. Since probes happen before channel opens (see [ADR 005](../005-protocol.md) probe flow), requiring a channel is architecturally incompatible with the protocol sequence. Probes are unauthenticated and free — ADR 005 states "`ProbeRequest` requires no authentication."
- **Option C — Proof-of-work on probe requests.** Rejected for two reasons: (a) probe latency is part of the unified node-selection score ([ADR 001 § Node Selection Algorithm](../001-network.md#node-selection-algorithm)), so mandatory hashing on every probe degrades the selection signal the probe was meant to provide; (b) PoW is bypassable by an attacker with cheaper compute than the honest client (cloud GPU vs mobile CPU), inverting the intended cost asymmetry.
- **Option D — Accept the risk and monitor only.** Rejected. A probe response is a 200-byte signed message; per-probe cost is dominated by the EIP-712 signature (~1 ms CPU on a typical node). At scale a Sybil attacker can saturate the signing path and exhaust the hold budget. Monitoring without enforcement is insufficient — the locked mechanism is enforced rate limiting per [ADR 005 § Probe rate limiting](../005-protocol.md#probe-rate-limiting).

---

## ADR 018 — Balancer V2 vs V3 (pre-merge migration)

Source: [ADR 018 — Liquidity Strategy](../018-liquidity-strategy.md). The canonical decision (Balancer **V3** 80/20 weighted POL, with the affirmative V3 security justification in ADR 018 § Consequences) is documented inline. The rejected alternative:

- **Balancer V2.** An earlier draft targeted Balancer V2. Rejected before merge after the 2025-11-03 V2 Composable Stable Pool exploit (~$125M, per Certora / Trail of Bits / OpenZeppelin post-mortems) demonstrated a latent V2 codebase risk not present in V3's new Vault architecture. The core 80/20 weighted-POL decision (USDC efficiency, IL alignment, zero-keeper posture) is a property of weighted pools in general and is not version-specific.

---

## ADR 036 — Served-Bytes Voting Weight

Source: [ADR 036 — Served-Bytes Voting Weight](../036-served-bytes-voting-weight.md). The canonical decision — DAO vote weight as the trailing-window sum of an operator's served bytes × `age_ramp`, per-operator-capped, with a slashing zero-out — is documented inline in ADR 036. The rejected alternatives:

1. **Cumulative-lifetime served bytes (no decay).** Mirrors the FeeRouter operator-leg's denominator exactly. Rejected: oldest operators dominate forever; fresh entrants cannot catch up; vote weight does not reflect *current* contribution.
2. **EWMA-decayed served bytes (single accumulator, exponential decay).** Smoother than fixed-window; no hard edge as epochs fall off; requires one accumulator updated on each settlement and supports a single `_getVotes` SLOAD instead of O(N). Rejected: requires per-settlement on-chain writes to maintain the accumulator (small but non-zero gas), couples vote weight to settlement timing in ways harder to audit, and the fixed-window approach gives operationally-clearer reasoning during governance disputes ("here are the 13 epoch totals").
3. **No slashing zero-out — let the rolling window do it naturally.** Slashed operators retain accumulated bytes-weight and vote for up to N weeks until the window decays past the slash. Rejected: leaves a meaningful immediate-response gap; the slashing zero-out costs one storage slot per operator and one SLOAD per vote-cast.
4. **Probe-verified delivery cap multiplier.** Use `min(voucher_bytes, probe_capacity × epoch_length)` as the per-epoch bytes input. Rejected: this would require re-introducing probe-vs-declared-capacity enforcement on-chain, the same machinery the design deliberately omits alongside capacity-shortfall slashing (see [ADR 026](../026-tokenomics.md#adr-026-tokenomics)). The fixed-window served-bytes accounting plus the per-operator cap covers the threat surface; revisit only if wash-trading economics shift materially.
