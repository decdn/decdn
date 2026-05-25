# Work-Token Tokenomics — Alternatives Investigation

**Status:** Brainstorming output (not yet a design decision; precedes any spec).
**Relationship to existing specs:** Companion to `2026-05-24-work-token-tokenomics-redesign-v2.1.md` (v2.1). This document does not supersede or replace v2.1; it sweeps the architectural space *around* v2.1 so the chosen path can be confirmed (or revised) on the merits of the alternatives.
**Scope:** Architectural sweep — five archetypically-different tokenomics frames evaluated against the four v2.1 priorities plus capital efficiency and fundraising posture.

## Summary

v2.1 is a tight local optimum *within the work-token frame*: it strips contract surface (deletes `VotingEscrow` + `DelegatorBuyer`, simplifies `FeeRouter` 6→4 buckets), maximizes regulatory cleanliness (no LM, no passive-yield path, operator-only governance), and concentrates value accrual into two prongs — bond demand from capacity growth + 25% burn. It buys this with two real costs: (a) non-operator holders are politically disenfranchised from the DAO, and (b) no external LP attraction in year 1 (POL-only liquidity).

This document explores whether either of those costs is material enough to motivate switching frames — and if so, to which alternative.

## Evaluation axes

All alternatives are scored against the four priorities from the v2.1 spec, plus two additional axes that v2.1 doesn't surface but matter for stakeholder conversations:

1. **Contract surface** (smaller is better) — solidity LOC, audit cost, operational complexity
2. **Value accrual** — strength of TOKEN demand-creation mechanism
3. **Operator decentralization** — structural pressure favoring a diverse operator set
4. **Regulatory defensibility** — Howey-test exposure, particularly prong 4 ("solely from the efforts of others")
5. **Capital efficiency** (added) — how much TOKEN must be tied up per unit of bandwidth served
6. **Fundraising narrative** (added) — how readily the model communicates to investors and operators

## Five archetypically-different alternatives

### A — v2.1 baseline: Pure work-token (for reference)

**Concept:** Operator-only bond, capacity-curved, no passive yield, operator-only governance.

**Mechanism:** `CapacityBond.bond = k × Mbps^α` (α=1.2). 60/25/10/5 fee split. Service emissions auto-deposit into bond. 19% POL, no LM.

**Wins on:** (iv) cleanest regulatory posture of any model. (i) smallest contract surface — 1 net contract added vs ADR 026 minus 2.

**Loses on:** External LP attraction in year 1. Non-operator holders structurally inert (politically and economically). Operator capital barrier non-trivial at 100G+ tiers.

**Right when:** Regulatory defensibility is non-negotiable and the holder class accepts being passive.

---

### B — Dual-token: separate WORK and GOV instruments

**Concept:** Two distinct tokens with disjoint roles. WORK is operator-only (bonded, service-emitted, non-transferable or heavily restricted). GOV is the fundraising and governance instrument held by investors, team, community, treasury.

**Mechanism:**

- **WORK**: minted by `OperatorEmissions` for verified delivery; bonded into `CapacityBond`; non-transferable until burned-to-mint GOV at a fixed ratio (or burned for an exit USDC claim against treasury) on operator unbond.
- **GOV**: 1B fixed supply, ERC20Burnable, freely transferable. Holds *all* DAO governance weight. Receives fee-share or buyback-and-burn flow. Cannot be bonded for operator role.
- Fee split (illustrative): 60% operator USDC / 20% GOV-buyback-burn / 15% treasury / 5% safety. WORK is paid out separately via emission bucket.
- Operators earn USDC (operating cashflow) + WORK (capacity progression). Holders earn GOV value-accrual (burn) + governance.

**Wins on:**

- Splits the regulatory burden — WORK looks like an operator-bond instrument (unambiguously utility); GOV looks like a governance-and-claim instrument with cleaner Howey analysis per token.
- Restores non-operator holder enfranchisement without re-introducing v2.1's "passive yield from work" tension.
- Stakeholder politics get easier — investors hold something with cashflow and votes.

**Loses on:**

- (i) Roughly 2× contract surface (two ERC20s, two POL pools or a forced single-sided design, conversion bridge between WORK and GOV).
- (ii) Value accrual diluted across two tokens; market cap split confuses the fundraising narrative; dual-listing cost.
- Most past dual-token experiments collapsed back to single-token within 2–3 years. MKR/DAI is the survivor, but DAI is a stable, not a gov token.
- Operator-side incentive alignment with GOV price is indirect.

**Right when:** Investors should retain economic + governance participation and the team is willing to absorb the dual-token complexity and ambiguity penalty.

---

### C — Restaking-delegated bond (EigenLayer / Cosmos pattern)

**Concept:** Single TOKEN. Operators bond capacity. Non-operator holders can *delegate* TOKEN to a specific operator, share the operator's USDC fee revenue minus a commission, and inherit slashing risk.

**Mechanism:**

- `CapacityBond.bond(operator)` accepts own-deposits and delegate-deposits identically; capacity-curve unchanged.
- `CapacityBond.delegateTo(operator, amount)` lets any holder add to an operator's bond pool. Delegated TOKEN counts toward the operator's capacity tier the same as self-bond.
- Operator sets a commission rate (e.g., 5–20% of USDC fee share to operator, remainder pro-rata to delegators).
- Delegators inherit the slashing tail — they lose their share if the operator is slashed.
- Governance: operator-only, vote weight unchanged. Delegators *do not* vote.

**Wins on:**

- Restores capital efficiency and passive-holder participation.
- Operator decentralization improves: under-capitalized but technically competent operators can scale via delegated bond rather than being capped by their own treasury.
- Solves v2.1's S4 "thin float" concern — delegators absorb supply demand without needing α-tuning.

**Loses on:**

- **(iv) Worst regulatory posture of any candidate.** Delegators are the textbook "passive class earning yield from the efforts of others." Coinbase Staking faced an SEC suit on essentially this construction.
- Even with the "you're not the legal holder, your operator is, you accepted slashing risk" framing, this is the construction the SEC has most directly attacked. Counsel will not bless this without a Helium-2024-style settlement risk premium baked in.
- (i) Adds delegate accounting and commission distribution in `CapacityBond` and `FeeRouter`.

**Right when:** Capital efficiency dominates and there is appetite (or jurisdictional cover — e.g., a Swiss-Verein primary launch with US-blocked addresses) for an EigenLayer-shaped regulatory profile.

---

### D — Filecoin-style emission-heavy work-token

**Concept:** Pure work-token, but with light bond requirements and *liquid* TOKEN emission to operators (vs v2.1's emission-locked-into-bond). Inflationary during issuance, fixed-cap on bond.

**Mechanism:**

- Bond curve scaled down 5–10× (1G tier = ~5K TOKEN bond, not 50K). Bond is a security deposit, not a tier-gate.
- Allocation: e.g., 40% Operator Emissions (vs v2.1's 20%); emission curve runs 8–10 years rather than 4–6.
- Emitted TOKEN is **liquid** — operators may sell it. This is the central Filecoin precedent.
- Burn share unchanged (25%) or scaled up (35%) to absorb emission inflation.
- Governance: operator-only, capacity-weighted (v2.1-style).

**Wins on:**

- (iii) much lower operator capital barrier — anyone with infrastructure can start; bond is nominal.
- Liquid emission means operators can pay infra costs out of TOKEN revenue without forced sales of locked balances.
- Strong direct precedent (Filecoin, Bitcoin pre-halving).
- (iv) Filecoin's regulatory framing has held through ~8 years and an L1-scale audit surface.

**Loses on:**

- (ii) **Significant inflation during issuance** depresses TOKEN price; investors and treasury allocations dilute against emission.
- Operators have weaker price-alignment incentive (they receive supply, not a scarcity claim).
- Treasury / seed term-sheet conversations get harder — the scarcity narrative is weaker.
- Without a strong burn lever, this can equilibrate at a permanently low TOKEN price (Filecoin trades at ~5% of its 2021 peak; this is partly the issuance schedule).

**Right when:** Operator decentralization is the dominant priority and the team is willing to absorb the price-stability tax.

---

### E — No-TOKEN / pure-stablecoin settlement

**Concept:** Drop the native TOKEN entirely. All settlement, bonding, and incentives in USDC. Governance shifts to an off-chain instrument (Verein membership or Labs equity).

**Mechanism:**

- Operators bond USDC (or a stablecoin basket) sized to capacity, same curve shape.
- `FeeRouter` distributes USDC: e.g., 70% operator / 20% treasury / 10% safety. No buyback-burn (nothing to buy or burn).
- No emission, no TGE, no airdrop, no public sale.
- DAO becomes an off-chain DAO — Verein-level voting by identified Verein members; or token-equivalent via Labs equity.

**Wins on:**

- (iv) **Maximum regulatory cleanliness** — no token = no Howey analysis on the protocol asset.
- (i) Smallest possible contract surface: no `BuybackBurner`, no `OperatorEmissions`, no `TOKEN` contract, smaller `FeeRouter`. Audit cost roughly halves.

**Loses on:**

- (ii) **Zero protocol-level value accrual.** No fundraising lever — equity is the only investment instrument; the existing pre-seed/seed/private allocations as conceived don't exist.
- No long-term operator-alignment instrument beyond USDC operating margin.
- Loses the "deCDN equity exposure via TOKEN" pitch that's load-bearing for current fundraising.
- Closes off the "decentralized network with skin in the game" messaging.

**Right when:** A future-state regulatory event (e.g., SEC declares all utility tokens are securities by default; or the team chooses a non-US jurisdiction with very strict rules) forces a tokenless re-architecture. *Not a realistic candidate given existing pre-seed commitments and term-sheet exposure.*

---

## Comparison matrix

| | (i) Contract surface | (ii) Value accrual | (iii) Decentralization | (iv) Regulatory | Capital efficiency | Fundraise narrative |
|---|---|---|---|---|---|---|
| **A — v2.1 baseline** | ★★★★★ | ★★★ | ★★★★ | ★★★★★ | ★★ | ★★★ |
| **B — Dual-token** | ★★ | ★★★ | ★★★★ | ★★★★ | ★★★ | ★★ (confusing) |
| **C — Restaking-delegated bond** | ★★★★ | ★★★★ | ★★★★★ | ★★ | ★★★★★ | ★★★★ |
| **D — Filecoin-emission** | ★★★★ | ★★ | ★★★★★ | ★★★★ | ★★★★ | ★★ (inflation story) |
| **E — No-TOKEN** | ★★★★★ | ★ | ★★★★ | ★★★★★ | ★★★ | ★ (no token) |

Star scale is relative within this matrix, not absolute. v2.1 is treated as the reference point; alternatives are scored against it.

## Recommendation

If v2.1's known costs (year-1 external LP attraction, structurally inert non-operator holders) feel material enough to revisit, **B (dual-token)** is the most-architecturally-honest alternative — it gives investors a real instrument while keeping operator economics regulatorily clean. The 2× contract cost is real but bounded.

If those costs *don't* feel material — i.e., v2.1's tradeoffs are deliberate and acceptable — then v2.1 is already near-optimal and switching frames cuts against a strong design. In that case the productive move is to *enrich v2.1* with a single delegation lever borrowed from C (operator-controlled delegated bond, but no delegator yield-share — just capital efficiency) rather than swap frames.

C as a full frame is the highest-capital-efficiency option but flag it as the one most likely to need a Helium-2024-style settlement risk reserve.

D and E are honest design points but probably not for deCDN's current posture — D undercuts the scarcity narrative the existing seed/private term sheets imply, and E reopens fundraising decisions already made.

## Mix-and-match candidates worth flagging

The five frames above are not mutually exclusive. Three combinations are worth naming explicitly because they capture most of the design surface:

1. **v2.1 + C-lite delegation primitive.** Take v2.1 verbatim, add operator-controlled bond delegation *with no delegator yield-share* — delegators contribute capital, operator pays them back in TOKEN (not USDC) on unbond + a small commission denominated as a one-time fixed fee. Pure capital-efficiency lever; no recurring passive-yield path.
2. **B + v2.1's POL-heavy distribution.** Dual-token, but apply v2.1's Section 5 distribution mechanics (POL-heavy, no LM) to the GOV token specifically. WORK token has its own simpler allocation (100% Operator Emissions).
3. **D + v2.1's bond curve.** Filecoin-style liquid emission, but keep v2.1's super-linear bond curve as a decentralization lever. Lower capital barrier than v2.1, more inflation than v2.1, similar decentralization shape.

## Open questions / things to settle next

1. **Are v2.1's two known costs (year-1 LP attraction, holder inertness) actually material?** This is the load-bearing question. If yes → develop B or a B-hybrid. If no → v2.1 is near-optimal; flag the C-lite delegation primitive as a possible future-state ADR but don't change frames.
2. **Howey-prong-4 risk tolerance.** v2.1 sets this dial to zero. B sets it to "moderate, isolated to one of two tokens." C sets it to "deliberately accept Helium-2024-shaped risk." The team's actual tolerance (informed by counsel review, jurisdiction strategy, and target investor base) determines which alternatives are live.
3. **Fundraising narrative compatibility.** Pre-seed commitments and any draft term sheets assume a single-token design. B would require renegotiating those expectations. C would not. D would require rebriefing investors on the inflation story.
4. **Operator-class composition at launch.** v2.1 + C-lite would shift the operator class toward "well-connected operators with delegate networks" rather than "well-capitalized operators." Is that desirable?
5. **Cross-jurisdictional posture.** Some alternatives (C) benefit substantially from a non-US primary jurisdiction; others (E) are forced by certain regulatory events. The entity design (Verein + Labs + GmbH) already accommodates this, but the tokenomics frame influences which jurisdictional levers matter.

## What this document is NOT

- Not a design decision. It is input to one.
- Not a contract specification. Per-alternative Solidity sketches are not included.
- Not a legal opinion. Howey framings are design intent; counsel review is required.
- Not a transition plan. Adopting any of B–E from v2.1 would require its own implementation spec downstream.

## Next step

Confirm which alternative (or mix-and-match candidate) to develop into a full design spec following the v2.1 template. Until then, v2.1 remains the proposed canonical design.
