# Pre-Launch Alternatives Considered

> **Audit trail.** This file collects the `## Alternatives Considered` sections that previously lived inline across the ADR set. It is **not** part of the canonical protocol specification — it is a record of design alternatives evaluated and rejected before launch, retained so future contributors can see what was on the table without inferring it from the current design. Tracked in [#346](https://github.com/decdn/decdn/issues/346); see [`adr/README.md` § Decision-record context](../README.md#decision-record-context) for the project-level framing.
>
> Neither the numeric `adrs.pdf` nor the reading-order `adrs-book.pdf` build includes this file. Readers approaching the protocol top-to-bottom get the canonical spec; readers researching a specific decision can follow the breadcrumb at the bottom of each source ADR's stub `## Alternatives Considered` section back here.

Sections below are anchored by source ADR. Cross-references back to each source use relative paths (`../NNN-name.md`).

---

## ADR 014 — Slash Signature Scheme

Source: [ADR 014 § 1 — Ed25519 Signature Verification — Dual-Key Slash Signatures](../014-on-chain-verification.md#1-ed25519-signature-verification--dual-key-slash-signatures).

The alternatives below are scoped to the choice of signature scheme used for on-chain slash evidence (`slash_sig` field on signed wire messages). The chosen design — dual-key slash signatures with secp256k1 `ecrecover` — is documented in ADR 014 § 1.

| Approach | Gas Cost | PoC Suitability | Why Not |
| --- | --- | --- | --- |
| RIP-7212 Ed25519 precompile | ~3,000 | Not available | Not yet deployed on the production L2 as of 2026-04 (see [Appendix: L2 Deployment](../appendix-l2-deployment.md)) |
| Solidity Ed25519 library (e.g., `ed25519-sol`) | ~500k–1M | Too expensive | A single slash verification would cost $0.25–$0.50; two-signature offenses double that |
| ZK proof of Ed25519 signature | ~300k verify | Too complex | Requires a proving circuit, prover infrastructure, and proof generation latency |
| Optimistic (no signature verification) | ~50k | Insufficient security | A node could deny authorship of any message; counter-evidence alone is not enough |
| **Dual-key slash signatures (chosen)** | **~3,000** | **Recommended** | Uses proven `ecrecover`; adds one `Option` field per message; no new infrastructure |

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

Source: [ADR 022 — Content Discovery at Scale (DHT)](../022-content-discovery.md).

### Broadcast probe fan-out as primary mechanism

The existing approach in [ADR 001](../001-network.md). Generates O(N) probe messages per cache miss. Retained as a bootstrap fallback and emergency fallback when DHT returns no providers. Not suitable as the primary mechanism even at PoC scale, because the O(N) cost is a design ceiling rather than an operational limit — the network should not be architected around it. Probe fan-out is a sound fallback because it is maximally complete: a miss definitively means no node holds the blob.

### Gossip content announcements

Each cache/evict event generates a gossip message. Rejected: unbounded traffic proportional to cache churn, retraction storms under high eviction rates. See ADR 022 Context section.

### Hash-prefix range hints in `NodeAnnounce`

Rejected: economically irrational in an incentive-driven network where nodes cache popular content regardless of hash prefix. See ADR 022 Context section.

### iroh mainline DHT (`DhtDiscovery` / pkarr)

Rejected for this use case. iroh's built-in DHT resolves `NodeId → address` on the public mainline BitTorrent DHT. It does not support content-hash records, is not scoped to the deCDN registered-node set, and exposes lookup patterns to the public internet.

### Indexer nodes (`cdn/search/v1`)

Not rejected — deferred. Dedicated indexer nodes aggregating DHT records into a searchable catalog are a natural complement once the network grows large enough to justify a separate indexing tier. Out of scope for ADR 022.

### Full libp2p Kademlia

Deferred. `libp2p-kad` is battle-tested but built on libp2p's transport stack. Bridging to iroh QUIC adds a large dependency and upstream governance coupling. The lightweight subset specified in ADR 022 covers the deCDN use case with ~300–500 lines of Rust.

---

## ADR 026 — Gauge-Boost Tokenomics

Source: [ADR 026 — Gauge-Boost Tokenomics](../026-gauge-boost-tokenomics.md).

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
