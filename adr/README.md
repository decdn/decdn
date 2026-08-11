# deCDN — Protocol Specification

A decentralized CDN. Nodes cache and serve content-addressed blobs over iroh QUIC; clients pay per-MB via off-chain vouchers backed by a shared on-chain payment pool; bonded operators compete on price and latency, with reputation, slashing, and capacity-bonded incentives keeping the mesh honest.

This directory is the protocol's canonical specification. Each numbered file is an Architecture Decision Record (ADR) covering one component or invariant of the design. [`architecture.md`](architecture.md) is the living overview — system diagram, ADR-by-ADR summaries, key invariants, trust assumptions, and the canonical reading order.

## Design principles

- **Content-addressed, location-independent.** Every blob is identified by its BLAKE3 hash. Delivery verification is inherent: the receiver hashes received bytes and rejects mismatches. Nodes are interchangeable as long as the hash matches.
- **Paid byte delivery, end to end.** Every byte transferred — client→node *and* node→node — is paid. There is no free-rider tier and no unpaid relay layer. Off-chain payment-token vouchers, backed by a shared on-chain pool, settle on-chain.
- **Stake to participate, slash on misbehavior.** Nodes must stake TOKEN before joining the mesh. Misbehavior (rate manipulation, corruption, blacklist violation) is detectable on-chain and slashable. Challenge bonds prevent zero-cost griefing.
- **Origin storage is opaque.** Origin-backed nodes hold canonical content in S3/R2/B2/NFS/local-disk backends, but no external origin URL is ever exposed. Bypassing the payment layer requires bypassing the network entirely.
- **Operator return is differentiated by capacity commitment, not raw stake.** The `CapacityBond` lock-to-capacity curve `bond = k × Mbps^α` requires operators to bond TOKEN proportional to declared bandwidth, with super-linear pressure against concentration. No fee discounts, no passive yield to non-operators.
- **Pre-launch the protocol has one design.** ADRs read as the canonical specification, not as an iteration log. Rejected pre-launch alternatives are kept out of the ADR bodies entirely — archived in [`adr/_history/`](_history/alternatives-pre-launch.md), which is part of neither built PDF (see *Decision-record context* below).

## Reading order

For first-time readers, follow this thematic order rather than the numeric one. Each chapter assumes the previous chapters are read. The full chapter-by-chapter ADR list lives in [`architecture.md` § Reading Order](architecture.md#reading-order).

1. **Foundations** — language stack, network topology, content addressing, wire protocol.
2. **Discovery** — DHT-based content lookup, regional proxy warming, multi-source fetch.
3. **Payments** — the shared payment pool, vouchers, client architecture, smart-wallet support.
4. **Tokenomics & incentives** — work-token tokenomics, liquidity strategy, deferred follow-ups.
5. **Verification & enforcement** — on-chain slashing evidence, reputation, content takedown.
6. **Governance & contracts** — Governor + Timelock model, contract interaction map.
7. **Operations** — node onboarding.
8. **Supporting infrastructure** — schema evolution, privacy analysis.

Plus a set of **appendices** documenting reference patterns, operator runbooks, and operational layers built on top of the protocol (observability, L2 deployment selection, PoC/production seam architecture, local admin HTTP surface, operator key rotation, operator protocol-upgrade runbook, permissionless fraud detection).

The numeric index in [`architecture.md` § Architectural Decisions](architecture.md#architectural-decisions) stays as the canonical per-ADR reference.

## Glossary

Terms used across multiple ADRs are defined in [`glossary.md`](glossary.md), grouped into wire protocol & content, payments, tokenomics & incentives, and on-chain enforcement. `glossary.md` is the canonical source and is compiled as its own chapter into both built PDFs.

## ADRs vs appendices

This directory contains two kinds of documents:

- **Core protocol ADRs** (`NNN-name.md`) — invariants every conforming node, client, or contract must implement the same way for the network to function. These are the canonical specification.
- **Appendices** (`appendix-name.md`) — patterns, reference implementations, operational guidance, and optional layers built **on top of** the protocol. Alternative implementations are acceptable. Examples: the recommended observability metric registry, the Arbitrum One deployment selection, the Rust implementation pattern for PoC/production seams, the local admin HTTP surface, the operator key-rotation runbook, the operator protocol-upgrade runbook, and the permissionless fraud-detection layer.

Appendices are listed in [`architecture.md` § Appendices — Reference Patterns](architecture.md#appendices--reference-patterns). They are deliberately **not** numbered as ADRs because they document optional patterns rather than core protocol decisions.

## Decision-record context

ADRs are **decision records** — but the *rendered* spec carries only the canonical design. The pre-launch alternatives that were weighed and rejected (and the rationale) are collected in [`adr/_history/alternatives-pre-launch.md`](_history/alternatives-pre-launch.md), kept out of every ADR body and out of both built PDFs, so future contributors can see what was on the table without the spec reading as a debate transcript.

> **Note:** the rejected-alternatives relocation is complete — ADR bodies carry no `## Alternatives Considered` section and no breadcrumb link; the `_history/` archive is reached directly.

## Contributing

See [CONTRIBUTING.md § Working with ADRs](../CONTRIBUTING.md#working-with-adrs) for ADR conventions, file naming, cross-reference checks, and the next-ADR-number protocol.

## Building a single PDF

Bundle every ADR (overview + numbered ADRs + appendices + glossary) into one printable PDF with a table of contents and rendered Mermaid diagrams.

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

A second purely-presentational filter, [`_build/table-autofit.lua`](_build/table-autofit.lua), also changes **no content and no decision**: it hands table column sizing to the typst engine (content-aware `auto` columns) instead of the uniform equal widths pandoc derives from the GFM `| --- | --- |` delimiters, which otherwise squish a prose-heavy column (e.g. ADR 016 § Contract Inventory) into a tall ribbon beside near-empty short columns. Delete the `TABLEFIT` flag from the Makefile to restore pandoc's default equal-width tables.

### Reading-order build (book layout)

The numeric build above is the canonical per-ADR reference. For a top-to-bottom read, build the same set in the thematic chapter order from [`architecture.md` § Reading Order](architecture.md#reading-order) — Foundations → Discovery → Payments → Tokenomics → Verification → Governance → Operations → Supporting → Appendices. Output goes to `adrs-book.pdf` so both PDFs can coexist.

This build also strips *Deferred & Open* (and its legacy aliases *Open Questions* / *Future Work*) and *Alternatives Considered* / *Considered Alternatives* / *"Why not …"* sections at render time via [`_build/strip-meta-sections.lua`](_build/strip-meta-sections.lua), so the document reads as a single canonical design rather than a debate transcript. The architecture overview's *Reading Order* section is dropped from the book too — the book's part dividers and generated table of contents *are* the reading order, so the prose chapter list would be redundant there; it stays in the source and the numeric `adrs.pdf`. *Cross-ADR Impact* is **not** stripped — it carries substantive cross-cutting design content, not scaffolding. *Deferred & Open* is retained verbatim in the source `.md` files and the numeric `adrs.pdf` build — only the reading-order book strips it. Rejected alternatives are in no ADR; they live solely in [`adr/_history/`](_history/alternatives-pre-launch.md), which neither PDF includes. A short notice on the first page ([`_build/preface.md`](_build/preface.md)) states what is omitted.

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
