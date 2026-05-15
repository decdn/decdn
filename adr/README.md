# deCDN — Protocol Specification

A decentralized CDN. Nodes cache and serve content-addressed blobs over iroh QUIC; clients pay per-MB via off-chain USDC payment channels with on-chain settlement; staked operators compete on price and latency, with reputation, slashing, and gauge-weighted incentives keeping the mesh honest.

This directory is the protocol's canonical specification. Each numbered file is an Architecture Decision Record (ADR) covering one component or invariant of the design. [`architecture.md`](architecture.md) is the living overview — system diagram, ADR-by-ADR summaries, key invariants, trust assumptions, and the canonical reading order.

## Design principles

- **Content-addressed, location-independent.** Every blob is identified by its BLAKE3 hash. Delivery verification is inherent: the receiver hashes received bytes and rejects mismatches. Nodes are interchangeable as long as the hash matches.
- **Paid byte delivery, end to end.** Every byte transferred — client→node *and* node→node — is paid. There is no free-rider tier and no unpaid relay layer. Off-chain payment channels (USDC, with multi-token allowlist post-PoC) settle on-chain.
- **Stake to participate, slash on misbehavior.** Nodes must stake TOKEN before joining the mesh. Misbehavior (phantom announcements, rate manipulation, corruption, blacklist violation) is detectable on-chain and slashable. Challenge bonds prevent zero-cost griefing.
- **Origin storage is opaque.** Origin-backed nodes hold canonical content in S3/R2/B2/NFS/local-disk backends, but no external origin URL is ever exposed. Bypassing the payment layer requires bypassing the network entirely.
- **Operator return is differentiated by long-term commitment, not raw stake.** Curve-style gauge-boost via opt-in `VotingEscrow` rewards operators who lock TOKEN for longer periods, instead of a regressive stake-multiple fee discount.
- **Pre-launch the protocol has one design.** ADRs read as the canonical specification, not as an iteration log. Rejected alternatives appear in each ADR's `## Alternatives Considered` section (see *Decision-record context* below).

## Reading order

For first-time readers, follow this thematic order rather than the numeric one. Each chapter assumes the previous chapters are read. The full chapter-by-chapter ADR list lives in [`architecture.md` § Reading Order](architecture.md#reading-order).

1. **Foundations** — language stack, network topology, content addressing, wire protocol.
2. **Discovery** — DHT-based content lookup, QUIC 0-RTT.
3. **Payments** — channels, vouchers, multi-token allowlist, client architecture, smart-wallet support.
4. **Tokenomics & incentives** — gauge-boost, delivery receipts, liquidity strategy, deferred follow-ups.
5. **Verification & enforcement** — on-chain slashing evidence, reputation, content takedown.
6. **Governance & contracts** — Governor + Timelock model, contract interaction map.
7. **Operations** — node onboarding.
8. **Supporting infrastructure** — schema evolution, privacy analysis.

Plus a set of **appendices** documenting reference patterns, operator runbooks, and operational layers built on top of the protocol (encrypted content publishing, observability, L2 deployment selection, PoC/production seam architecture, local admin HTTP surface, operator key rotation, operator protocol-upgrade runbook, permissionless fraud detection).

The numeric index in [`architecture.md` § Architectural Decisions](architecture.md#architectural-decisions) stays as the canonical per-ADR reference.

## Glossary

Terms used across multiple ADRs without inline definition.

### Wire protocol & content

| Term | Definition |
| --- | --- |
| **Blob** | A content-addressed byte sequence identified by its BLAKE3 hash. |
| **Chunk** | The BLAKE3 hash-tree leaf size (1024 bytes). iroh-blobs uses this for verified streaming; on-chain Merkle proofs for slash evidence reference this leaf size — see [ADR 002](002-content-addressing.md) for the addressing scheme and [ADR 014](014-on-chain-verification.md) for the slash-evidence verification flow. |
| **Hash sequence** | An ordered collection of blob hashes (iroh's equivalent of a directory/manifest). |
| **NodeId** | An iroh public-key identifier; the on-wire identity of a node. Bound to an Ethereum address on-chain via EIP-712 signature in `StakingRegistry.registerNode` ([ADR 001](001-network.md)). |
| **Node** | A staked participant that caches and serves blobs. Some nodes are configured with an origin backend; others are pure caches. |
| **Client** | A lightweight QUIC endpoint that streams content and pays per MB. |
| **Origin-backed node** | A node configured with an S3-compatible object store (S3/R2/B2/MinIO), NFS mount, or local disk. Can serve any blob in that store, never experiences a true cache miss. |
| **ALPN** | Application-Layer Protocol Negotiation — identifies which protocol a QUIC connection uses (e.g., `cdn/probe/v1`, `cdn/client/v1`). |

### Payments

| Term | Definition |
| --- | --- |
| **Channel** | An off-chain payment channel between a client and a node. Funded with USDC (or a governance-approved ERC-20 in production), settled on-chain after the dispute window — see [ADR 003](003-payments.md). |
| **Voucher** | A signed off-chain payment message: `{channelId, amount, nonce, token, signature}`. The bearer instrument for per-MB payments. |
| **TOKEN** | The protocol's native fixed-supply (1B) ERC-20. Used for staking, governance, gauge-boost, and slashing — see [ADR 026](026-gauge-boost-tokenomics.md). |
| **USDC** | The payment-and-settlement currency. All channel deposits, fee distribution, and the externally-raised pre-seed pool are USDC-denominated — see [ADR 003](003-payments.md), [ADR 026](026-gauge-boost-tokenomics.md). |

### Tokenomics & incentives

| Term | Definition |
| --- | --- |
| **Stake** | TOKEN deposited in `StakingRegistry` as a prerequisite for node registration. Minimum: 50,000 TOKEN per node — see [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake). |
| **Slashing** | Punitive reduction of staked TOKEN on detected protocol violations. Escalating tiers 5%/15%/50% by lifetime offense count; distribution 50% challenger / 30% safety reserve / 20% burn — see [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn). |
| **Epoch** | The 1-week settlement and ve-snapshot window. Gauge buckets, delegator buckets, and `bytes_delivered` counters reset at epoch rollover — see [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553). |
| **Gauge / gauge-boost** | Curve-style mechanism that scales an operator's share of the 40% gauge pool by ve-weighted commitment, not raw bytes. The boost-floor parameter caps the worst-case ratio between an unboosted and fully-boosted operator. |
| **`working_bytes`** | The gauge-formula input. Per-operator: `min(bytes_i, 0.4 * bytes_i + 0.6 * (ve_i / total_ve) * total_bytes)`. Replaces Curve's LP-deposit primitive with verified-bytes-delivered. |
| **ve / VotingEscrow** | Vote-escrowed TOKEN: a non-transferable, time-decaying lock of underlying TOKEN that grants gauge-boost and governance weight. Opt-in (no auto-ve-lock-on-vest) — see [ADR 026 §4](026-gauge-boost-tokenomics.md#4-voting-escrow-votingescrow). |
| **FeeRouter** | The settlement-time six-bucket USDC distributor. Split: 40 node base / 40 gauge / 7 delegator / 5 burn / 5 treasury / 3 safety. Atomic same-tx for the 40+5+5+3 legs; epoch-bucketed for the 40 gauge / 7 delegator legs. |
| **SafetyReserve** | A governance-gated USDC incident reserve (3% bucket). Payouts cover incorrect-slashing reversals, payment-channel downtime, and bad-data incidents — see [ADR 026 §5](026-gauge-boost-tokenomics.md#5-safety-and-insurance-reserve-3-bucket). |

### On-chain enforcement

| Term | Definition |
| --- | --- |
| **`StakingRegistry`** | The on-chain stake + node registration contract. Holds TOKEN stake, enforces `stake ≥ minStake`, performs slashing, manages the NodeId↔Ethereum-address binding. |
| **`SlashJudge`** | The on-chain contract that adjudicates all slashable offenses: verifies slash signatures, manages challenge bonds, runs counter-evidence windows, calls `StakingRegistry.slash()` — see [ADR 014](014-on-chain-verification.md). |
| **`BuybackBurner`** | The contract that swaps the 5% burn-bucket USDC into TOKEN via Balancer V3 80/20 weighted pool and burns the proceeds — see [ADR 003 § BuybackBurner](003-payments.md#buybackburner) and [ADR 018](018-liquidity-strategy.md). |
| **Slash signature** | An EIP-712 secp256k1 signature (`slash_sig`) on `ProbeResponse` / `StreamResponse`, used for on-chain slash evidence via `ecrecover` and as the message-body attribution signature for the paid-delivery path — see [ADR 014](014-on-chain-verification.md). |
| **Challenge bond** | The 100 TOKEN amount a challenger must post when submitting slash evidence. Returned on successful slash, forfeited on successful node counter (50% burned, 50% to node) — see [ADR 014 § Bond Handling](014-on-chain-verification.md#bond-handling). |

## ADRs vs appendices

This directory contains two kinds of documents:

- **Core protocol ADRs** (`NNN-name.md`) — invariants every conforming node, client, or contract must implement the same way for the network to function. These are the canonical specification.
- **Appendices** (`appendix-name.md`) — patterns, reference implementations, operational guidance, and optional layers built **on top of** the protocol. Alternative implementations are acceptable. Examples: encrypted content publishing (companion app server, `cdn/keys/v1`), the recommended observability metric registry, the Arbitrum One deployment selection, the Rust implementation pattern for PoC/production seams, the local admin HTTP surface, the operator key-rotation runbook, the operator protocol-upgrade runbook, and the permissionless fraud-detection layer.

Appendices are listed in [`architecture.md` § Appendices — Reference Patterns](architecture.md#appendices-reference-patterns). They are deliberately **not** numbered as ADRs because they document optional patterns rather than core protocol decisions.

## Decision-record context

ADRs are **decision records**: they capture both the canonical design and the alternatives that were considered and rejected, so future contributors can see what was on the table without inferring it from the current design. Each ADR with rejected alternatives carries a `## Alternatives Considered` section covering the alternatives, the rationale for rejection, and the design that replaced them.

> **Note:** moving the rejected-alternatives content out of ADR bodies into a dedicated `adr/_history/` audit trail is tracked in #346 (Tier 3 of the book-readiness program). Until that lands, alternatives live inline.

## Contributing

See [CONTRIBUTING.md § Working with ADRs](../CONTRIBUTING.md#working-with-adrs) for ADR conventions, file naming, cross-reference checks, and the next-ADR-number protocol.

## Building a single PDF

Bundle every ADR (overview + numbered ADRs + appendices) into one printable PDF with a table of contents and rendered Mermaid diagrams.

Dependencies:

- [`pandoc`](https://pandoc.org/) — `brew install pandoc`
- [`typst`](https://typst.app/) — `brew install typst` (PDF engine)
- [`mermaid-filter`](https://github.com/raghur/mermaid-filter) — `npm install -g mermaid-filter` (renders ` ```mermaid ` blocks via headless Chromium pulled in by puppeteer; first install is ~150 MB)

The `PATH` prefix below makes the build work even when your shell hasn't picked up npm's global bin directory:

```bash
cd adr
PATH="$(npm config get prefix)/bin:$PATH" \
pandoc --from=markdown+gfm_auto_identifiers \
  --toc --toc-depth=2 \
  --pdf-engine=typst \
  -F mermaid-filter \
  -V title="deCDN — Architecture Decision Records" \
  -V date="$(date +%Y-%m-%d)" \
  architecture.md $(ls [0-9]*.md | sort) $(ls appendix-*.md | sort) \
  -o adrs.pdf
```

Build takes ~1 minute (most of it spent rendering Mermaid diagrams). Without `-F mermaid-filter` the diagrams ship as raw source text.

### Reading-order build (book layout)

The numeric build above is the canonical per-ADR reference. For a top-to-bottom read, build the same set in the thematic chapter order from [`architecture.md` § Reading Order](architecture.md#reading-order) — Foundations → Discovery → Payments → Tokenomics → Verification → Governance → Operations → Supporting → Appendices. Output goes to `adrs-book.pdf` so both PDFs can coexist.

This build also strips *Alternatives Considered*, *Considered Alternatives*, *Open Questions*, *Future Work*, and *"Why not …"* sections at render time via [`_build/strip-meta-sections.lua`](_build/strip-meta-sections.lua), so the document reads as a single canonical design rather than a debate transcript. The source `.md` files keep those sections untouched, and the numeric `adrs.pdf` build above includes them for readers who want the full decision-record context. A short notice on the first page ([`_build/preface.md`](_build/preface.md)) tells readers what was omitted and where to find it.

```bash
cd adr
PATH="$(npm config get prefix)/bin:$PATH" \
pandoc --from=markdown+gfm_auto_identifiers \
  --toc --toc-depth=2 \
  --pdf-engine=typst \
  -F mermaid-filter \
  --lua-filter=_build/strip-meta-sections.lua \
  -V title="deCDN — Architecture Decision Records (Reading Order)" \
  -V date="$(date +%Y-%m-%d)" \
  _build/preface.md \
  architecture.md \
  000-language.md 001-network.md 002-content-addressing.md 005-protocol.md \
  022-content-discovery.md 015-zero-rtt.md \
  003-payments.md 010-multi-token.md 012-client.md 024-account-abstraction.md \
  026-gauge-boost-tokenomics.md 018-liquidity-strategy.md \
  014-on-chain-verification.md 008-reputation.md 011-content-takedown.md 028-slashing-appeals.md \
  030-safety-reserve-appeals-contract.md 031-content-blacklist-appeals-contract.md \
  009-governance.md 016-contract-interactions.md \
  019-node-onboarding.md \
  013-schema-evolution.md 017-privacy.md \
  appendix-encrypted-content-publishing.md \
  appendix-observability.md \
  appendix-peer-table-eviction.md \
  appendix-blob-cache-eviction.md \
  appendix-l2-deployment.md \
  appendix-poc-production-seams.md \
  appendix-local-admin-http.md \
  appendix-operator-key-rotation.md \
  appendix-operator-upgrade-path.md \
  appendix-fraud-detection.md \
  -o adrs-book.pdf
```

If `architecture.md`'s Reading Order changes, this file list needs to be updated by hand — there's no auto-generation. The build itself takes the same ~1 minute.
