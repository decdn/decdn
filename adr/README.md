# deCDN — Protocol Specification

A decentralized CDN. Nodes cache and serve content-addressed blobs over iroh QUIC; clients pay per-MB via off-chain USDC payment channels with on-chain settlement; staked operators compete on price and latency, with reputation, slashing, and gauge-weighted incentives keeping the mesh honest.

This directory is the protocol's canonical specification. Each numbered file is an Architecture Decision Record (ADR) covering one component or invariant of the design. [`architecture.md`](architecture.md) is the living overview — system diagram, ADR-by-ADR summaries, key invariants, trust assumptions, and the canonical reading order.

## Design principles

- **Content-addressed, location-independent.** Every blob is identified by its BLAKE3 hash. Delivery verification is inherent: the receiver hashes received bytes and rejects mismatches. Nodes are interchangeable as long as the hash matches.
- **Paid byte delivery, end to end.** Every byte transferred — client→node *and* node→node — is paid. There is no free-rider tier and no unpaid relay layer. Off-chain payment channels (USDC, with multi-token allowlist post-PoC) settle on-chain.
- **Stake to participate, slash on misbehavior.** Nodes must stake TOKEN before joining the mesh. Misbehavior (phantom announcements, rate manipulation, corruption, blacklist violation) is detectable on-chain and slashable. Challenge bonds prevent zero-cost griefing.
- **Origin storage is opaque.** Origin-backed nodes hold canonical content in S3/R2/B2/NFS/local-disk backends, but no external origin URL is ever exposed. Bypassing the payment layer requires bypassing the network entirely.
- **Operator return is differentiated by long-term commitment, not raw stake.** Curve-style gauge-boost via opt-in `VotingEscrow` rewards operators who lock TOKEN for longer periods, instead of a regressive stake-multiple fee discount.
- **Pre-launch the protocol has one design.** ADRs read as the canonical specification, not as an iteration log. Rejected pre-launch alternatives are kept out of the ADR bodies entirely — archived in [`adr/_history/`](_history/alternatives-pre-launch.md), which is part of neither built PDF (see *Decision-record context* below).

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

ADRs are **decision records** — but the *rendered* spec carries only the canonical design. The pre-launch alternatives that were weighed and rejected (and the rationale) are collected in [`adr/_history/alternatives-pre-launch.md`](_history/alternatives-pre-launch.md), kept out of every ADR body and out of both built PDFs, so future contributors can see what was on the table without the spec reading as a debate transcript.

> **Note:** the rejected-alternatives relocation (#346, Tier 3 of the book-readiness program) is complete — ADR bodies carry no `## Alternatives Considered` section and no breadcrumb link; the `_history/` archive is reached directly.

## Contributing

See [CONTRIBUTING.md § Working with ADRs](../CONTRIBUTING.md#working-with-adrs) for ADR conventions, file naming, cross-reference checks, and the next-ADR-number protocol.

## Building a single PDF

Bundle every ADR (overview + numbered ADRs + appendices) into one printable PDF with a table of contents and rendered Mermaid diagrams.

Dependencies:

- [`pandoc`](https://pandoc.org/) — `brew install pandoc`
- [`typst`](https://typst.app/) — `brew install typst` (PDF engine)
- [`mermaid-filter`](https://github.com/raghur/mermaid-filter) — `npm install -g mermaid-filter` (renders ` ```mermaid ` blocks via headless Chromium pulled in by puppeteer; first install is ~150 MB)

Both PDFs build from [`adr/Makefile`](Makefile) — the single source of truth for the pandoc invocation. Run from the `adr/` directory:

```bash
make            # both PDFs: adrs.pdf (numeric) + adrs-book.pdf (reading order)
make book       # adrs-book.pdf only
make numeric    # adrs.pdf only
make MERMAID=0  # build without mermaid-filter (no mmdc/puppeteer needed)
make clean      # remove generated PDFs
make help       # list targets
```

The canonical build (`make`) renders Mermaid diagrams and takes ~1 minute (most of it rendering diagrams); it adds the npm global-bin `PATH` prefix automatically so it works even when your shell hasn't picked it up. `make MERMAID=0` omits `-F mermaid-filter` — diagrams ship as raw source text — for environments without `mmdc`/puppeteer and for the no-mermaid content-soundness check.

The Makefile applies a purely-presentational page-density config — [`_build/book-margins.yaml`](_build/book-margins.yaml) (1.6 cm/1.8 cm margins) plus [`_build/book-density.typ`](_build/book-density.typ) (10 pt, `linestretch 1.0`, tighter leading/code/tables/headings) — that replaces the very loose pandoc/typst defaults (≈2.5 cm margins, 11 pt, slack leading). It changes **no content and no decision**, only whitespace: it takes the reading-order book from ≈430 to ≈260 pages (no mermaid) with the section-level TOC and every cross-reference intact. Delete the `DENSITY` flags from the Makefile to render byte-identical content at the loose default density.

### Reading-order build (book layout)

The numeric build above is the canonical per-ADR reference. For a top-to-bottom read, build the same set in the thematic chapter order from [`architecture.md` § Reading Order](architecture.md#reading-order) — Foundations → Discovery → Payments → Tokenomics → Verification → Governance → Operations → Supporting → Appendices. Output goes to `adrs-book.pdf` so both PDFs can coexist.

This build also strips *Deferred & Open* (and its legacy aliases *Open Questions* / *Future Work*) and *Alternatives Considered* / *Considered Alternatives* / *"Why not …"* sections at render time via [`_build/strip-meta-sections.lua`](_build/strip-meta-sections.lua), so the document reads as a single canonical design rather than a debate transcript. *Cross-ADR Impact* is **not** stripped — it carries substantive cross-cutting design content, not scaffolding. *Deferred & Open* is retained verbatim in the source `.md` files and the numeric `adrs.pdf` build — only the reading-order book strips it. Rejected alternatives are in no ADR; they live solely in [`adr/_history/`](_history/alternatives-pre-launch.md), which neither PDF includes. A short notice on the first page ([`_build/preface.md`](_build/preface.md)) states what is omitted.

`make book` produces this. The thematic file order (with the `_build/part-*.md` dividers) and the `--lua-filter` wiring live in the `BOOK_SRCS` variable and the `adrs-book.pdf` recipe of [`adr/Makefile`](Makefile).

If `architecture.md`'s Reading Order changes, the `BOOK_SRCS` list in the Makefile must be updated by hand to match — there's no auto-generation. `make check-book-list` guards against the common drift (a numbered ADR or appendix on disk that was never added to `BOOK_SRCS`, which would silently omit it from the book) and is a cheap pre-commit / CI check. The build itself takes the same ~1 minute.

### Canonical section taxonomy

Recurring non-`Context`/`Decision`/`Consequences` sections use one canonical name so the book filter keys off a stable set instead of an ever-growing list of synonyms. When authoring an ADR, use these names — do not coin new ones:

| Canonical `## H2` | Purpose | In the book? | Replaces (do not reuse) |
| --- | --- | --- | --- |
| **Cross-ADR Impact** | How this ADR amends / relates to / couples with other ADRs | Yes — substantive | *ADRs Affected*, *Amendments to Existing ADRs*, *Cross-ADR Consistency*, *Cross-references*, *Forward references*, *Implications & follow-ups* |
| **Deferred & Open** | Deferred work, open questions, forward-looking items | No — stripped | *Open Questions*, *Future Work*, *Future work and non-goals* (split: non-goals → Non-Goals) |
| **Non-Goals** | Substantive scope exclusions | Yes — substantive | (was sometimes merged into *Future work and non-goals*) |
| **References** | Bibliography / external links | Yes | (already consistent) |
| **Alternatives Considered** | Rejected-alternative record | No — not in ADRs | Removed from ADR bodies; archived only in `_history/` (in neither PDF) |

The numeric `adrs.pdf` build keeps every section regardless of name; only the reading-order `adrs-book.pdf` applies the strip.
