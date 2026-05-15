# deCDN ADR Book Reduction — Implementation Plan (Phase 1)

> **For agentic workers:** Execute Phase 2 stage-by-stage. Each stage = separate reviewable commit(s). PAUSE for user review after every stage. Steps use checkbox (`- [ ]`) syntax. Stop and ask the user if any single edit would change a decision's meaning.

**Goal:** Reduce the `adrs-book.pdf` (reading-order, lua-filtered) from ~400 to ~250 pages without deleting any architectural decision, invariant, contract behavior, security property, non-goal, or cross-reference.

**Approach:** Six levers (A–F). Convert the spec from iteration-log to canonical (Lever A), structural dedup (B/C/D), in-place condensation (E/F). Every appendix stays in the book; the two load-bearing anchors are preserved exactly.

**Tech:** pandoc 3.9 + typst 0.14.2, `_build/strip-meta-sections.lua`. Build per `adr/README.md § Building a single PDF`.

---

## 0. Headline finding — read this first

**The prescribed levers A–F, executed within the hard constraints, realistically yield ~65–76 pages, not ~150.** The task's per-lever estimates are not supported by the measured corpus. The single most important Phase-1 result is this reconciliation:

| Lever | Task estimate | Measured-realistic | Why the gap |
|---|---|---|---|
| A — fold Cross-ADR Impact | ~25–40 pp | **~4–5 pp** | The 11 Cross-ADR Impact sections total **1,627 words**, not tens of pages. Most amendments are *already applied* to their target ADRs (verified) — Lever A is mostly verify-and-**delete** redundant residue, not fold. Real value = canonical-spec correctness, not pages. |
| B — architecture.md | ~9 pp | **~6 pp** | Confirmed close. 1,874 removable words (1,208 duplicated + 666 Future-Work→stripped). |
| C — ADR 016 §1 pointer table | ~15 pp | **~4–6 pp** | **Premise broken.** §1 embeds exactly one interface, `IFeeRouter` (L116–267, ~150 lines), and it is the corpus's **only** definition of the FeeRouter surface — not a restatement. Replacing it with a pointer would delete a decision (forbidden). Realistic C = prose-condense 016's verbose NatSpec/Notes. |
| D — 028/031/032 shared appendix | ~12 pp | **~4–5 pp** | The new appendix is *in the book*, so it re-adds most extracted words. Net dedup ≈ 1,400–1,800 words. Value = audit-surface clarity. |
| E — condense 12 appendices | ~30 pp | **~20–26 pp** | Biggest real lever. ~8,600 words at ~33% cut. |
| F — prose-condense 003/011/026 + general | 20–30% | **~27–28 pp** | Big-3 prose-tighten ≈ 2,450 w (~7–8 pp). "Relocate to appendix" = **0 net pages** (appendices are in the book). General ~10% tighten of the other ~20 numbered ADRs (67,382 w) ≈ ~20 pp. |
| **Total** | "~150 pp" | **~65–76 pp** | |

**Residual gap ≈ 75–85 pages (~25,000 words).** It cannot be closed by A–F within the hard constraints. Closing it needs a user decision (see § 8 Gap Decision). This plan executes A–F honestly, reports actuals after each stage, and recommends constraint-respecting supplementary levers.

### Calibration (how pages were measured)

- Total corpus (all `adr/*.md`): **140,679 words**. Filtered book (post lua-strip, the actual `adrs-book.pdf` content): **131,258 words**.
- `mdls -name kMDItemNumberOfPages` returns `(null)` for typst-generated PDFs (no XMP page metadata), even from a Spotlight-indexed dir. **Reliable substitute, validated:** parse the PDF Pages-tree `/Count` (Python, no deps). Use this in Phase 2.
- Baseline builds (this environment has no `mmdc`, so built **without** `-F mermaid-filter` — content-sound, exit 0, **zero typst dangling-label errors** on both): no-mermaid book = **448 pp**, no-mermaid numeric = **439 pp**.
- Task states the real (mermaid-rendered) book ≈ **400 pp**. Calibration: 131,258 w / 400 pp ≈ **328 words per real book page**. Whole-section / code-block removals also delete headings + whitespace, so effective yield for structural cuts ≈ ~300 w/p. Mermaid rendering compresses ~48 pp vs raw source; none of A–F adds/removes diagrams except B (one mermaid + one flowchart) and D (state diagrams) — flagged where relevant.
- **Target restated:** 400 → 250 real pages = remove ~150 pp ≈ **~49,000 words (~37% of the 131,258-word filtered book).**

---

## 1. Hard constraints (apply to every task)

- Never delete a decision/invariant/contract behavior/security property/non-goal/cross-reference. Only condense its expression or relocate it.
- Every `appendix-*.md` stays in the book. Two anchors preserved **exactly** (heading text + slug):
  - `appendix-poc-production-seams.md:43` → `` ### 1. `KeyStore` — `crates/incentive` `` (slug `#1-keystore--cratesincentive`; linked from `003-payments.md:327`). Do not renumber the 9 seam subsections.
  - `appendix-observability.md:112` → `#### 2.6 Gossip Metrics` (slug `#26-gossip-metrics`; linked from `019-node-onboarding.md:35` and `appendix-peer-table-eviction.md:52,96`). Do not renumber 2.x.
- Classify before deleting. Confirmed traps: ADR 010 `## What This Adds` / `## Scope of Changes` / `## Migration from ADR 003` are **substantive** (production `PaymentChannel` Solidity, allowlist, force-close, per-token rate bounds, token-vetting checklist, migration steps) — **do not strip**.
- No new abstractions/features. Any new shared doc (Lever D) is an `appendix-*.md`, never a numbered ADR. Match rustfmt-100 / ADR house style. Preserve the canonical section taxonomy from #571 (do not reintroduce synonym headings).
- The plan file lives in `docs/superpowers/plans/` — **never** in `adr/` (any `adr/*.md` is swept into the PDF builds).

## 2. Verification protocol (run after EVERY stage, before its commit)

```bash
cd adr
# (1) Both builds succeed, exit 0, ZERO typst dangling-label errors.
#     This env lacks mmdc → build WITHOUT mermaid-filter (content-soundness).
#     Report which mode was used. Do NOT regenerate/commit binary adrs*.pdf
#     unless mermaid-filter works.
PATH="$(npm config get prefix)/bin:$PATH" pandoc --from=markdown+gfm_auto_identifiers \
  --toc --toc-depth=2 --pdf-engine=typst \
  architecture.md $(ls [0-9]*.md|sort) $(ls appendix-*.md|sort) \
  -o _verify-numeric.pdf 2>&1 | tee /tmp/vnum.log ; echo "numeric exit ${PIPESTATUS[0]}"
PATH="$(npm config get prefix)/bin:$PATH" pandoc --from=markdown+gfm_auto_identifiers \
  --toc --toc-depth=2 --pdf-engine=typst --lua-filter=_build/strip-meta-sections.lua \
  _build/preface.md architecture.md _build/part-1-foundations.md 000-language.md 001-network.md \
  002-content-addressing.md 005-protocol.md _build/part-2-discovery.md 022-content-discovery.md \
  015-zero-rtt.md _build/part-3-payments.md 003-payments.md 010-multi-token.md 012-client.md \
  024-account-abstraction.md _build/part-4-tokenomics.md 026-gauge-boost-tokenomics.md \
  018-liquidity-strategy.md _build/part-5-verification.md 014-on-chain-verification.md \
  008-reputation.md 011-content-takedown.md 028-slashing-appeals.md \
  032-safety-reserve-appeals-contract.md 031-content-blacklist-appeals-contract.md \
  030-node-region-self-attestation.md _build/part-6-governance.md 009-governance.md \
  016-contract-interactions.md _build/part-7-operations.md 019-node-onboarding.md \
  _build/part-8-supporting.md 013-schema-evolution.md 017-privacy.md _build/part-9-appendices.md \
  appendix-encrypted-content-publishing.md appendix-bundles.md appendix-observability.md \
  appendix-peer-table-eviction.md appendix-blob-cache-eviction.md appendix-l2-deployment.md \
  appendix-poc-production-seams.md appendix-binaries.md appendix-local-admin-http.md \
  appendix-operator-key-rotation.md appendix-operator-upgrade-path.md appendix-fraud-detection.md \
  -o _verify-book.pdf 2>&1 | tee /tmp/vbook.log ; echo "book exit ${PIPESTATUS[0]}"
grep -i 'dangling\|error' /tmp/vnum.log /tmp/vbook.log   # MUST be empty (ignore mermaid-filter env noise)

# (2) Dangling-anchor scan — BOTH forms (a same-file miss broke the build in #571):
#     cross-file file.md#anchor AND same-file (#anchor). After any heading
#     rename/move/removal, grep every link whose anchor targets a changed
#     heading and repoint or rewrite it.
grep -rnoE '\]\(([0-9a-z-]+\.md)?#[a-z0-9-]+\)' *.md   # enumerate; diff vs heading slugs

# (3) mermaid-filter may fail (Node/puppeteer) — environmental, not content.
#     Confirm soundness WITHOUT it (above). Report mode used.

# (4) Page count (mdls is null for typst PDFs — use Pages-tree /Count):
/usr/bin/python3 - <<'PY'
import re
for f in ["_verify-book.pdf","_verify-numeric.pdf"]:
    d=open(f,'rb').read()
    c=[int(x) for x in re.findall(rb'/Type\s*/Pages.{0,200}?/Count\s+(\d+)',d,re.S)]
    c+=[int(x) for x in re.findall(rb'/Count\s+(\d+)\s*/Type\s*/Pages',d)]
    print(f, max(c) if c else "PARSE-FAIL")
PY
rm -f _verify-book.pdf _verify-numeric.pdf   # do not commit raw-text PDFs
```

After each stage report: **pages-saved-vs-estimate** and **remaining gap to 250** (use no-mermaid page count; the real book is ~0.89× — note both).

---

## 3. Per-file baseline (current word counts)

| File | Words | Touched by |
|---|---:|---|
| 003-payments.md | 12,826 | F |
| 011-content-takedown.md | 10,825 | A, F |
| 016-contract-interactions.md | 9,528 | A(target), C |
| 026-gauge-boost-tokenomics.md | 7,462 | A, F |
| architecture.md | 4,885 | A(target), B |
| 028-slashing-appeals.md | 4,317 | A, D |
| 032-safety-reserve-appeals-contract.md | 4,233 | A, D |
| 031-content-blacklist-appeals-contract.md | 3,151 | A, D |
| other 17 numbered ADRs | ~58,000 | A(targets), F-general |
| 12 appendix-*.md (total) | 25,673 | E |
| **Filtered book total** | **131,258** | — |

Cross-ADR Impact section sizes (Lever A scope, measured): 011=381, 028=222, 032=177, 022=165, 014=159, 024=143, 030=121, 031=100, 012=79, 013=56, 026=24 → **1,627 words total**.

---

## 4. Lever A — Cross-ADR Impact (Stage 1) — one commit per source ADR

**Reclassification:** This is **mostly verify-and-delete**, not fold. Verified against current targets:

| Source bullet | Target already amended? (grep proof) | Action |
|---|---|---|
| 011 → 001 permissionless-origin → DAO-gated | 001 has no "permissionless origin" claim (already gone) | **Delete bullet** |
| 012 → 003 eclipse A/B/C resolved | 003:273–277 already states "resolved in ADR 012; PoC registry-only" | **Delete bullet**; 003 body already canonical |
| 014 → 005 Ed25519 body-sig removed | 005:57 already "slash_sig … sole … no opt-out" | **Delete bullet** |
| 024 → 016 add SignatureChecker to OZ table | 016:819 already lists `SignatureChecker … (ADR 024)` | **Delete bullet** |
| 024 → 003 SignatureChecker replaces ECDSA | 003:54/599/704 already SignatureChecker-native | **Delete bullet** |
| 032 → 028 five→six entry points | 028:178 already "six new entry points" | **Delete bullet** |
| 031 → 009 enumerate fastTrackAppeal in emergency-multisig | 009 does **not** enumerate it | **Genuine fold:** edit 009 capability list, then delete bullet |
| 028 SafetyReserve future-split migration list | no other home | **Keep** (forward-coupling) — condense, retitle if section otherwise empty |
| 026 "(none currently outstanding…)" | content already in §3 | **Delete section** (≈ nothing) |

**Per-bullet rule:** for each bullet, grep the target ADR for the post-amendment text. If present → delete the bullet (residue). If absent and it is a real amendment → apply the minimal native edit to the target, then delete the bullet. If it is forward-looking coupling with no natural home → keep, condensed. Once a section holds only kept-coupling or nothing → remove the heading.

**Estimate:** net removal ≈ 1,300–1,500 of 1,627 words (little re-added — targets already say it) → **~4–5 pp**. **Risk: low** (verify-then-delete of *confirmed* redundancy; the one genuine fold, 031→009, is a small enumerated list). DESTRUCTIVE classification per task, but evidence shows it is the *safest* destructive lever.

- [ ] **A.1** ADR 024 (`## Cross-ADR Impact` L194–202). Verify all 5 targets already amended (003,012,014,016,019 — grep each). Delete confirmed-redundant bullets; for any not-yet-applied, make the minimal native edit in the target first. Remove the section. Run § 2. Commit: `docs(adr): fold ADR 024 cross-ADR residue (verified already-applied; section removed)`.
- [ ] **A.2** ADR 014 (L272–278) — same procedure. Commit `docs(adr): fold ADR 014 cross-ADR residue`.
- [ ] **A.3** ADR 012 (L391–396). Commit `docs(adr): fold ADR 012 cross-ADR residue`.
- [ ] **A.4** ADR 013 (L440–444) — note `architecture.md` bullet is bookkeeping (verify entry exists → delete). Commit `docs(adr): fold ADR 013 cross-ADR residue`.
- [ ] **A.5** ADR 022 (L235–243) — fold the observability-metrics bullet into `appendix-observability.md` (add the 3 `decdn_dht_*` metrics there) then delete; keep genuine coupling (008/011/013) condensed. Commit `docs(adr): fold ADR 022 cross-ADR residue into observability appendix`.
- [ ] **A.6** ADR 011 (L647–657) — 9 bullets; mostly verify-delete (001/002/003/005/026/009/016/022) + keep 028-coupling condensed. Commit `docs(adr): fold ADR 011 cross-ADR residue`.
- [ ] **A.7** ADR 031 (L309–315) — **genuine fold:** edit `009-governance.md` Emergency-Multisig capability (1) to enumerate `fastTrackAppeal`/`unFastTrackAppeal`/`rejectAppeal`/`rejectAppealAsPerjury`; verify 016/011 already reference; delete bullets; keep 032 pointer. Commit `docs(adr): fold ADR 031 cross-ADR residue (enumerate appeal sub-modes in ADR 009)`.
- [ ] **A.8** ADR 032 (L223–230) — verify 026 stub / 028 six-entry-points already applied; delete bookkeeping bullets (architecture.md, CLAUDE.md). Commit `docs(adr): fold ADR 032 cross-ADR residue`.
- [ ] **A.9** ADR 028 (L200–211) — **keep** the SafetyReserve future-split migration list (forward-coupling, no home); condense; delete the 032/011 restated bullets. Commit `docs(adr): condense ADR 028 cross-ADR section to forward-coupling only`.
- [ ] **A.10** ADR 026 (L649–651) & ADR 030 (L103–107) — 026: delete the empty section. 030: keep as tracked-divergence note (deliberate), condense. Commit `docs(adr): tidy ADR 026/030 cross-ADR sections`.

---

## 5. Levers B + C + D — structural dedup (Stage 2) — separate commits

### B — architecture.md (4,885 w) → remove ~1,874 w (**~6 pp**) — risk **structural-dedup (low)**

- Remove (duplicated elsewhere): `## Origin Integration` (195 w), `## Cache Behavior` (399 w), `## Crate Structure` (149 w), `### Binaries` (108 w), `### Dependency Chain` (205 w; one mermaid), `## Observability` (152 w) — each defers to an ADR/appendix that is authoritative. = 1,208 w.
- **Keep (do not remove):** System Diagram, Reading Order, Architectural Decisions index, Key Invariants, Trust Assumptions, and the *unique* `### Supported Origins` (126 w) + `### Hash-to-Object-Key Mapping` (69 w) — these are not duplicated; relocate them under a kept section rather than delete.
- Normalize `## Future Work: Search & Discovery` (119 w), `## Future Work: KV-CRDT Content Catalogs` (206 w), `## What Is Not Decided Yet` (341 w) → merge into one `## Deferred & Open` (canonical name → lua strips it from the book) = 666 book-words removed. architecture.md was excluded from #571; this brings it into taxonomy.
- [ ] **B.1** Remove the 6 duplicated sections; relocate the 2 unique subsections. Repoint any links targeting removed headings (dangling scan, both forms). Run § 2. Commit `docs(adr): drop architecture.md sections duplicated by ADRs/appendices`.
- [ ] **B.2** Merge 3 deferred-style headings into `## Deferred & Open`; dangling scan. Run § 2. Commit `docs(adr): normalize architecture.md deferred sections to canonical taxonomy`.

### C — ADR 016 (9,528 w) → realistic ~1,200–1,800 w (**~4–6 pp**) — risk **destructive-prose (medium)**

**The specified mechanism does not apply.** §1 embeds exactly one interface, `IFeeRouter` (L116–267), which is the corpus's only FeeRouter definition (ADR 026 defines IVotingEscrow/ISafetyReserve/IDelegatorBuyer, never IFeeRouter). Pointer-izing it would delete a decision — forbidden. The §1 inventory **table** (L20–35), classDiagram, `#### Tunable Economics` (anchor `#tunable-economics`, linked from CLAUDE.md — preserve), `##### No proxy deployment patterns` (internal anchor — preserve), `#### Shared swap helper` are synthesized — **keep**. §2–§8 (deployment order, call graph, fund flow, access-control matrix, reentrancy) are synthesized — **keep**, prose-tighten only.

- [ ] **C.1** Condense the verbose multi-line NatSpec **comments** inside `IFeeRouter` (keep every signature, every invariant/security line, all event signatures; cut restatement of ADR 026 mechanics that the comments duplicate). Tighten the `**Notes:**` block and §2–§8 synthesized prose (no structural removal). Preserve anchors `#tunable-economics`, `#no-proxy-deployment-patterns`, `#1-contract-inventory`. Dangling scan. Run § 2. Commit `docs(adr): condense ADR 016 interface NatSpec and synthesized prose (no signatures/decisions removed)`. **Stop and ask** if tightening any comment would drop an invariant.

### D — 028/031/032 shared appeal-surface → new `appendix-appeal-surface.md` (Stage 2); net ~1,400–1,800 w (**~4–5 pp**) — risk **structural-dedup with high divergence**

Extract only the genuinely-shared, semantically-identical pattern (verified verbatim-ish across 031↔032, with 028 §4 the canonical bond authority): `appeals[0]` None-sentinel + `appealCounter`; event-topic indexer convention; `_pad0/_pad1` + `forge inspect storageLayout` test; "extend existing contract vs new registry" rationale; the two boilerplate Consequence/Alternative paragraphs. Each reduced ADR keeps a ~1-line back-reference.

**Must NOT generalize (per 9 verified divergences):** lapse-bond outcome (031 synthetic-standing clawback **burns**; 032 **refunds**) — keep per-ADR; the state machine (031 = 8 states incl. non-terminal `UnFastTracked`; 032 = 6) — **no unified diagram**; multisig capability *number* differs (028/032 → cap (4); 031 → cap (1)) — appendix states the *principle* only; 032 solvency invariant `totalEscrowLien + Σ pendingClaim ≤ generalBalance` stays in 032; 028 §4 remains the bond-economics authority (031:16/032:12 self-reference it — keep direction). Preserve 031→ADR 014 evidence-staleness cross-ref.

- [ ] **D.1** Create `appendix-appeal-surface.md` (appendix, not an ADR) with only the shared pattern; add to `architecture.md § Appendices` index and the README book file-list. Run § 2. Commit `docs(adr): add appendix-appeal-surface for shared appeal-contract pattern`.
- [ ] **D.2** Reduce 028 → slashing-specific delta + back-ref. Run § 2. Commit. **D.3** 031 → blacklist delta + back-ref. **D.4** 032 → safety-reserve delta + back-ref. One commit each; dangling scan each (031/032 are anchored-cross-referenced). **Stop and ask** before collapsing any state-machine or bond-outcome text.

---

## 6. Lever E — condense 12 appendices (Stage 3) — one commit per appendix

~33% aggregate cut, ~8,589 w → **~20–26 pp**. Risk: **destructive-prose**, file-specific. Preserve the two load-bearing anchors verbatim and the anchored-cluster's 9 numbered headings (observability ↔ blob-cache-eviction ↔ peer-table-eviction ↔ poc-seams). Correction to task beliefs: `appendix-l2-deployment.md` has **10** inbound refs (not zero) — medium risk; `appendix-bundles.md` is the true orphan (1 TOC link) — condense hardest.

| File | Words | Refs | % cut | After | Risk |
|---|---:|---:|---:|---:|---|
| appendix-bundles.md | 1,151 | 1 | 40% | 691 | low (orphan) |
| appendix-operator-key-rotation.md | 2,588 | 2 | 35% | 1,682 | low |
| appendix-operator-upgrade-path.md | 2,045 | 2 | 35% | 1,329 | low |
| appendix-binaries.md | 772 | 3 | 20% | 618 | low |
| appendix-l2-deployment.md | 2,180 | 10 | 40% | 1,308 | medium (table-restating prose) |
| appendix-local-admin-http.md | 1,424 | 6 | 35% | 926 | medium |
| appendix-fraud-detection.md | 774 | 10 | 30% | 542 | medium (file-level refs) |
| appendix-encrypted-content-publishing.md | 3,569 | 14 | 30% | 2,498 | medium (keep §-name headings ADR 017 cites) |
| appendix-blob-cache-eviction.md | 2,464 | 7 | 30% | 1,725 | high (anchored §2/§3) |
| appendix-peer-table-eviction.md | 2,108 | 10 | 30% | 1,476 | high (anchored §1/§3/§4) |
| appendix-poc-production-seams.md | 2,908 | 4 | 35% | 1,890 | high (anchor §1; 9-section numbering) |
| appendix-observability.md | 3,690 | 13 | 35% | 2,399 | high (anchor 2.6; keep all metric rows) |

- [ ] **E.1–E.12** One commit per file, lowest-risk first (bundles → key-rotation → upgrade-path → binaries → l2-deployment → local-admin-http → fraud-detection → encrypted-content-publishing → blob-cache-eviction → peer-table-eviction → poc-seams → observability). Per file: tighten prose, drop redundant examples, collapse oversized tables; **never** delete metric names/rows, the two load-bearing headings, anchored numbered headings, or any base-ADR-referenced substance. Dangling scan (both forms) + § 2 each. Commit `docs(adr): condense <appendix> (~N%)`.

---

## 7. Lever F — prose-condense (Stage 4) — one commit per ADR

Big-3 prose-tighten ≈ 2,450 w (**~7–8 pp**). **"Relocate deep detail into the relevant appendix" yields ZERO net book pages** — appendices are in the book; relocation only moves pages. Treat F as prose-tightening only. Then F-general: ~10% tighten across the other ~20 numbered ADRs (67,382 w base) ≈ ~20 pp (15% ≈ ~30 pp, higher decision-loss risk). Risk: **destructive-prose**.

- [ ] **F.1** 003-payments.md — collapse the zero-voucher-close mechanic (specified 3× at L114/L451/L188) to one canonical statement + internal refs; cut ceremonial closers in Attack Vectors; dedupe Deposit-Economics 10-USDC restatement. Do-not-touch: settlement invariants L118–121, Channel struct, function tables, decimal-handling. ≈ 1,000–1,200 w. § 2. Commit `docs(adr): condense ADR 003 prose (dedupe zero-voucher restatement)`.
- [ ] **F.2** 011-content-takedown.md — tighten decoupling-rationale triplication + Consequences bullet-restatement; ceremonial framing only in dense Standing/clawback section. ≈ 700 w. § 2. Commit.
- [ ] **F.3** 026-gauge-boost-tokenomics.md — tighten §5 payout-ordering prose, §2 pre-launch behavior bullets, §11 post-table justification, Risks/§3 wash-trading restatement. ≈ 750 w. § 2. Commit.
- [ ] **F.4…** General ~10% tighten, one commit per ADR, lowest-risk first; **Stop and ask** if any single edit changes a decision's meaning. § 2 each.

---

## 8. Gap decision (recommend to user after Stage 1 report)

A–F realistically land the book at **~325–335 pp** (no-mermaid; ~290–300 pp mermaid-rendered), not 250. To reach ~250, pick one — **[your call]**:

1. **Accept ~270–300 pp** (recommended): execute A–F faithfully, no decision risk. Honest, constraint-clean.
2. **Add supplementary levers** (constraint-respecting, ~ +30–50 pp): corpus-wide NatSpec/comment condensation in all Solidity blocks (2,441 code lines; keep signatures+invariants) ~15–24 pp; collapse duplicated governable-parameter tables recurring across 003/009/011/016/026 to one canonical table + pointers ~8–12 pp; cut mermaid diagrams that duplicate adjacent prose ~5–10 pp.
3. **Aggressive 15–20% general prose cut** (~ +15–25 pp) — raises decision-loss risk; needs the per-edit "stop if meaning changes" gate on every commit.
4. **Relax a hard constraint** (e.g., move `## Alternatives Considered` bulk to `_history/` per tracked #346 so it leaves even the numeric build) — out of current scope; flag to user.

Recommended: **1 + 2.** Reaching exactly 250 within constraints is unlikely; ~270 is the honest realistic floor.

---

## 9. Self-review (against task spec)

- Per-file word counts ✔ (§3). Proposed change per lever ✔. Estimated pages saved ✔ (§0 table, per-lever). Risk classified structural-dedup vs destructive-prose ✔ (each lever).
- Leads A–F all addressed; each reconciled against the task's estimate with evidence ✔.
- Hard constraints + 2 anchors + ADR 010 trap + taxonomy preservation ✔ (§1).
- Verification protocol covers both builds, both dangling-anchor forms, mermaid isolation, page-count via Pages-tree `/Count` (mdls is null for typst PDFs) ✔ (§2).
- Phase-2 staging matches task order (1: A per source ADR; 2: B+C+D; 3: E per appendix; 4: F per ADR), separate reviewable commits, pause after each ✔.
- Surfaced, with evidence: Lever A ≈ 4–5 pp (mostly verify-delete; amendments already applied); Lever C premise broken (IFeeRouter canonical-only); D/F relocation = 0 net pages (appendices in book); ~150 pp is ~2× realistic A–F yield; explicit gap decision for the user ✔.
