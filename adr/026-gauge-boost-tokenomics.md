# ADR 026: Gauge-Boost Tokenomics

**Date:** 2026-04-25
**Status:** Draft
**Supersedes:** [ADR 004](004-tokenomics.md) in full
**Source design spec:** [`docs/superpowers/specs/2026-04-18-tokenomics-v2-gauge-boost-design.md`](../docs/superpowers/specs/2026-04-18-tokenomics-v2-gauge-boost-design.md)
**Economic source of truth:** [`docs/superpowers/specs/2026-04-25-decdn-economic-model-40-40-gauge-pool.md`](../docs/superpowers/specs/2026-04-25-decdn-economic-model-40-40-gauge-pool.md)

---

## Context

[ADR 004](004-tokenomics.md) defined the original 1B-token model with a 3% protocol fee, 80/20 dev/audit/eco/burn allocation, a 200M-TOKEN node-bootstrap fund, a regressive fee-discount mechanic for large stakers, and 50/50 burn/challenger slashing distribution. Eighteen months of design iteration (captured in the source design spec §1 and the v3-era specs in `docs/superpowers/specs/`) surfaced four structural weaknesses that the cosmetic-fix surface area in ADR 004 cannot reach:

1. **Burn flow is structurally noise.** At a 1,000-node mature network, ADR 004's burn rate is ~0.014%/yr of supply (20% of 3% of revenue at $0.05 TOKEN), well below the ~22%/yr circulating-supply growth from vesting unlocks. Burn is the only deflationary lever in ADR 004; it is not enough.
2. **No yield path to token holders.** ADR 004's only utilities for TOKEN are stake-to-operate, fee discount, and governance. Passive holders earn nothing from network usage; long-term lockers have no compensation lever distinct from short-term holders.
3. **Regressive fee discount.** "Stake 10× minimum for 1.5% fee" reduces the buyback flow as more operators qualify — large stakers weaken the deflationary sink. The mechanic is non-progressive and non-aligned with long-term commitment.
4. **TOKEN-denominated bootstrap is reflexive.** The 200M-TOKEN bootstrap fund is most valuable when TOKEN is healthy and least valuable when subsidies are most needed. Single-asset reflexivity is the dominant tail risk for early operator recruitment.

The v3 redesign is structural, not cosmetic. Burn becomes a secondary deflationary lever; real yield to delegators / ve-lockers, supply discipline via fixed-supply-plus-vesting (no auto-ve-lock-on-vest), USDC-denominated bootstrap, and a direct operator-compensation link to ve-commitment via a Curve-style gauge boost become primary.

**Inputs assumed by this ADR.** Pre-launch design with no holder-compensation or contract-migration concerns. ~$1M+ pre-seed USDC capital secured (planning target $3M) — program structure defined in companion [ADR 030](030-preseed-usdc-deployment.md). 2026 unmetered-bandwidth provider economics per the design spec's input matrix (1 Gbps VPS, 10 Gbps dedicated, 100 Gbps edge tiers); dedicated-bandwidth nodes are realistic at every scale band the protocol is sized for.

**ADR-numbering coordination.** Per the rollout plan §3:

- The repo's local `adr/` already contains [ADR 025](025-local-admin-http.md) (`local-admin-http`, Accepted 2026-04-17).
- Remote branch `origin/adr/027-content-discovery-namespace-registry` introduces a competing `adr/025-content-discovery-namespace-registry.md`.
- Remote branch `origin/adr/024-026-client-improvements` claims numbers 024–026 by branch name without yet committing ADR files.

This ADR claims the number **026** per the rollout plan. The local 025 stands; the namespace-registry branch's 025 must be renumbered, and the client-improvements branch must specify and renumber its actual ADR file(s) before merging. Coordinate with both branch authors before this ADR lands on `main`.

---

## Decision

The protocol adopts the v3 economic model in full. Token distribution, fee allocation, fee-discount mechanic, bootstrap fund, and slashing distribution from ADR 004 are replaced. The slashing rate schedule (5% / 15% / 50%) and the lifetime offense counter from ADR 004 carry over unchanged.

The full design reasoning, MEV-defense analysis, equilibrium-stability argument, and reference-implementation pointers live in the source design spec; this ADR is the decision layer. Where a table fully duplicates one in the spec, this ADR shows the canonical defaults and points to the spec for the surrounding analysis.

### 1. Supply and distribution

**Supply.** 1,000,000,000 TOKEN, fixed at genesis. No post-genesis minting function exists on the production token contract.

**Allocation (1B total).** Six buckets summing to 100%. Vesting profile per the design spec §2.1; effective release rate is ~24%/yr during the active vesting window (Y1–Y3), 16%/yr in Y4, then zero.

| Allocation | Share | TOKEN | Vesting |
| --- | ---: | ---: | --- |
| Protocol treasury | 30% | 300,000,000 | 4-year linear |
| Seed backers | 24% | 240,000,000 | 3-year linear, 6-month cliff |
| Team & core contributors | 19% | 190,000,000 | 4-year linear, 12-month cliff |
| Community & ecosystem | 15% | 150,000,000 | 4-year linear |
| Genesis liquidity (POL) | 10% | 100,000,000 | Fully unlocked at genesis (Balancer V3 80/20 per [ADR 018](018-liquidity-strategy.md)) |
| Public sale / airdrop | 2% | 20,000,000 | Fully unlocked at genesis |
| **Total** | **100%** | **1,000,000,000** | |

**Genesis liquid supply.** 120,000,000 TOKEN (POL + public sale / airdrop). All other buckets release on vesting schedules.

**No auto-ve-lock on vest.** Vesting contracts release TOKEN unlocked into the recipient's wallet. Locking into `VotingEscrow` is opt-in. Rationale: the gauge-boost mechanism (§2) supplies a stronger and voluntary economic incentive to ve-lock than auto-lock did, the v3 model removes the "force long-term alignment via vesting contract" pattern in favor of "compensate long-term alignment via gauge boost," and seed/team term sheets are simpler under this design. The cost is a thinner initial veTOKEN base than the v2 interim designs; governance bootstrap may require treasury-funded ve-lock-on-claim airdrops in the first 6–12 months (see §9).

**No protocol-issued node-bootstrap fund.** ADR 004's 200M-TOKEN bootstrap fund is removed entirely. Bootstrap supply-side incentive is funded externally via $1M+ pre-seed USDC capital. Eliminates the TOKEN-price reflexivity in subsidy purchasing power. Program structure deferred to [ADR 030](#forward-references-follow-up-adrs).

### 2. FeeRouter split (40/40/7/5/5/3)

A new contract `FeeRouter` receives the full operator USDC balance from `PaymentChannel.settleChannel` and atomically splits it into six buckets. Replaces ADR 003's settlement-time fee-skim pattern in full. Full mechanic per design spec §2.2.

| Destination | Share | Unit | Distribution mechanic |
| --- | ---: | --- | --- |
| Node base | 40% | USDC | Direct same-tx, per-byte proportional to verified delivery |
| Gauge boost pool | 40% | USDC | Weekly epoch pool; pro-rata by ve-weighted `working_bytes` (§3); pull-based claim |
| Delegator pool | 7% | USDC → TOKEN | TWAP USDC→TOKEN swap; distributed pro-rata by ve-balance to delegators / ve-lockers (§6) |
| Buyback-and-burn | 5% | USDC → TOKEN | Direct same-tx to `BuybackBurner`; mechanics unchanged from [ADR 018](018-liquidity-strategy.md); TOKEN burned |
| Protocol treasury | 5% | USDC | Direct same-tx to Timelock-custodied treasury wallet |
| Safety & insurance reserve | 3% | USDC | Direct same-tx to `SafetyReserve`; governance-gated incident payouts (§5) |
| **Total** | **100%** | | |

**Aggregate operator-aligned compensation = 80%** (40% direct + 40% gauge pool). Identical headline operator share to ADR 004 and the v2 interim designs; the v3 split changes *how* the bucket is distributed, not its size.

**Same-transaction guarantees.** The 40% base, 5% burn, 5% treasury, and 3% safety legs all transfer in the settlement transaction. The 40% gauge and 7% delegator buckets accumulate in per-epoch buckets and are claim-based.

**Node-to-node cache-miss paid pulls bypass the router.** Direct peer USDC payment, no skim. Internal cost-recovery flow, not net protocol revenue.

**Gross client rate.** $0.01/GB — at parity with Bunny.net's budget tier and 7–20× cheaper than major traditional CDNs. No deCDN-specific premium. The router's 60% aggregate non-base skim is absorbed by operator net revenue, recovered through gauge-boost yield (§7), TOKEN-economy exposure, and the pre-seed USDC subsidy programs ([ADR 030](#forward-references-follow-up-adrs)) — never passed to clients.

**Epoch mechanics.** Epoch length is 1 week (7 × 86400 s, block-timestamp-aligned). At epoch rollover the gauge and delegator buckets freeze, new buckets open, and per-operator `bytes_delivered` counters reset. ve-balance snapshots are taken at the epoch-boundary timestamp via `VotingEscrow.balanceOfAt(user, ts)`. Claim window is 26 epochs (~6 months); unclaimed allocations sweep to the treasury.

### 3. Gauge-boost formula

Adapted from Curve Finance's veCRV gauge boost (in production since 2020). Replaces the LP-deposit primitive with verified-bytes-delivered.

For each operator `i` in epoch `e`:

```
bytes_i        = verified bytes delivered by operator i in epoch e
total_bytes    = sum of bytes_i over all operators
ve_i           = operator i's ve-balance at the epoch-boundary timestamp
total_ve       = total ve-balance at the epoch-boundary timestamp

working_bytes_i = min(
                    bytes_i,
                    0.4 × bytes_i + 0.6 × (ve_i / total_ve) × total_bytes
                  )

boost_share_i   = working_bytes_i / sum(working_bytes)
boost_payout_i  = boost_share_i × gauge_boost_pool_usdc[epoch_e]
```

**Properties** (illustrated graphically in design spec §9.4):

- **No ve-lock:** `working = 0.4 × bytes` — the commodity floor. Receives 40% of what a fair-share-ve operator with the same byte count would.
- **Fair-share ve** (`ve_i / total_ve ≥ bytes_i / total_bytes`): `working = bytes_i` — the cap binds. Full proportional share of the pool.
- **Over-ve:** `working` capped at `bytes_i` — no over-boost in the gauge pool. Excess ve still earns from the 7% delegator pool linearly.
- **Maximum boost ratio = 1 / 0.4 = 2.5×** between a max-ve-locker and a zero-ve-locker delivering the same byte count.

The Curve formula is bounded by `bytes` in both directions (a non-locker still earns 40% of fair-share, a whale-locker cannot exceed fair-share), which prevents both the "starve commodity operators" and "ve-whale captures the pool" failure modes of simpler `boost = 1 + k × ve` mechanics. The fair-share normalization gives the system a stable equilibrium where operators who match their ve-share to their byte-share collectively neither over- nor under-claim — matching Curve's gauge-equilibrium pattern.

The boost-floor parameter (default `boostFloor = 0.4`) is governable within `[0.2, 0.8]`. A lower floor sharpens the penalty for non-lockers and raises the max boost ratio; a higher floor softens differentiation. See §11 for the safety-bound table.

### 4. Voting escrow (`VotingEscrow`)

Vote-escrowed TOKEN. Modeled on veCRV with deliberate deviations.

| Parameter | Value |
| --- | --- |
| Lockable token | TOKEN (ERC-20) |
| Min lock duration | 1 week |
| Max lock duration | 4 years |
| ve-balance formula | `amount × remaining_lock_time / 4y` (linear decay to zero at expiry) |
| Lock extension | Allowed (up to 4y from current time) |
| Lock shortening | Not allowed |
| Early exit | **None** — no penalty-exit option (stricter than Convex; matches veCRV) |
| Transferability | **Non-transferable** — no `transfer` / `approve` for ve-positions |
| Slashing on ve-position | **No** — ve-locked TOKEN is never slashable, even if the locker is also a node operator |
| `create_lock_for` privileged path | **None** — auto-ve-lock-on-vest is removed in v3 |

**Historical checkpointing.** `VotingEscrow.balanceOfAt(user, ts)` and `totalSupplyAt(ts)` are load-bearing for the epoch-snapshot pattern in §2 and the governance pattern in §8. Per-lock checkpoints; reads O(log n) on the checkpoint array; writes O(1) amortized.

**Operator stake and ve-positions are separate.** A node's operator stake is held in `StakingRegistry` and is slashable (rates per §8). A ve-position is held in `VotingEscrow` and is not. Neither satisfies the other's requirements; an operator may hold any combination. This separation is a hard invariant — no contract path lets ve-locked TOKEN be slashed.

### 5. Safety and insurance reserve (3% bucket)

The 3% safety bucket is held in `SafetyReserve`, a governance-gated incident reserve. Eligible payout categories per design spec §2.2.5:

- Enterprise SLA compensation (per [ADR 030](#forward-references-follow-up-adrs) / [ADR 032](#forward-references-follow-up-adrs)).
- Incorrect slashing / appeal reversals.
- Relay, sequencer, or payment-channel downtime.
- Bad-data incidents where user recourse is more valuable than pure burn.

**Spending controls.** Disbursements require all of:

1. An attested incident bundle (cryptographic evidence of the failure, identity of the harmed party, proposed payout amount).
2. A governance proposal, or fast-track multisig approval (within hard caps per [ADR 009](009-governance.md)).
3. A 48-hour appeal window during which the bundle is challengeable on-chain.
4. Post-incident reporting published to a public registry maintained by `SafetyReserve`.

No path exists for unattested payouts; the `payout(bundle, recipient, amount)` entry point checks all four gates. Sizing analysis (number of $100K and $1M incidents covered per year per scenario) lives in the economic-model spec §7; this ADR does not duplicate the table.

### 6. Delegator pool — USDC → TOKEN conversion

The 7% delegator bucket flows through a USDC→TOKEN buy-and-distribute pipeline rather than direct USDC distribution.

1. `FeeRouter` accumulates 7% of routed USDC into the delegator-pool epoch bucket per epoch.
2. At epoch rollover (or via keeper trigger within the epoch), the bucket's USDC is swapped for TOKEN against the Balancer V3 80/20 pool ([ADR 018](018-liquidity-strategy.md)) under the same TWAP + minOut + per-epoch liquidity-cap protections as `BuybackBurner`. Implementation may share the swap helper (a `BuybackBurner` multi-output mode) or a parallel `DelegatorBuyer`; this ADR does not pin the choice.
3. The acquired TOKEN is held in the delegator-pool epoch bucket as TOKEN.
4. Delegators / ve-lockers call `FeeRouter.claimDelegator(epochs[])`. Payout per locker = `ve_i / total_ve_at_epoch_boundary × token_in_delegator_bucket[epoch]`.

**Distinction from buyback-and-burn.** Both are buy-side market pressure on USDC→TOKEN. Burn removes TOKEN from circulation; the delegator pool routes TOKEN to long-term ve-locked holders. Both are required.

**Why TOKEN-denominated, not USDC?** Routes acquired TOKEN to the participants with the longest commitment horizon and couples ve-locker yield to TOKEN value rather than to network revenue alone — when network revenue grows, TOKEN buy pressure grows, ve-locker positions appreciate. This is the v3 model's primary "real yield in TOKEN" lever, replacing the v2 interim "real yield in USDC via passive ve-pool" pattern.

**MEV / slippage.** TWAP windows + per-epoch liquidity caps + private-RPC routing (Flashbots-style bundles) for the swap. Same defenses as the [ADR 018](018-liquidity-strategy.md) buyback flow; per-epoch liquidity caps are a hard requirement on this path, not optional.

### 7. Operator economics and minimum stake

**Minimum stake.** **50,000 TOKEN.** Slashable (rates per §8), 7-day unbonding, slashable during unbonding. Up from ADR 004's 1,000-TOKEN minimum, sized for the v3 expectation that operators will additionally hold ve-positions for gauge boost (a low operator-stake floor with a separate ve-lock incentive splits the two roles cleanly).

**Discount-stake threshold removed.** ADR 004's "stake 10× minimum for 1.5% fee" mechanic is removed in full. Replaced structurally by the gauge boost: operators who want more return from capital ve-lock and earn a larger gauge-pool share, rather than paying lower fees. Simpler, non-regressive, and the core incentive loop the v3 model is designed around.

**Revenue streams** (per design spec §2.4):

1. **40% of every channel settlement** — direct USDC, same-tx, per-byte.
2. **Share of the 40% gauge-boost pool** — USDC, weekly distribution, weighted by `working_bytes`. Non-ve-lockers receive ~40% of fair-share; max-ve-lockers receive 100% of fair-share (2.5× more per byte than non-lockers).
3. **Optional delegator-pool yield** (TOKEN-denominated) on any TOKEN they ve-lock. Disjoint from the gauge pool; uncapped relative to byte share.

**Sample 1 Gbps node P&L** (30K GB/mo at $0.01/GB; 1,000-operator network reference; full assumptions and arithmetic in design spec §2.4). Three operator profiles:

| Profile | Capital ve-locked | Net P&L (USDC-equivalent) |
| --- | --- | ---: |
| **Case A — No ve-lock** (commodity operator) | 0 | **$53/mo** |
| **Case B — Fair-share ve** (~100K TOKEN @ 4y, ~$5K capital @ $0.05) | $5K | **$177/mo** |
| **Case C — Over-ve** (~400K TOKEN @ 4y, ~$20K capital @ $0.05) | $20K | **$240/mo** |

**Observations** (full discussion in design spec §2.4):

- Case A → Case B: +$124/mo on $5K capital → ~30% APR on the ve-locked capital before TOKEN appreciation. Payback ~3.4 years on a 4-year lock — meaningful and practical.
- Case C: gauge pool is capped at fair-share so the marginal gain over Case B comes entirely from the delegator pool — flat in gauge, linear in delegator-pool TOKEN yield, and a directional bet on TOKEN price.
- Case A is positive but thin. Commodity operators below ~40K GB/mo without a ve-position will struggle; the v3 pre-seed staking-loan and hardware-lease programs ([ADR 030](#forward-references-follow-up-adrs)) reduce this filter's harshness for new operators.

The economic model spec §3 contains the full multi-scenario / multi-node-type unmetered-infra cost matrix (S0–S3 × node types A–E); this ADR does not duplicate.

### 8. Slashing and burn

**Slashing rates unchanged from ADR 004:** 5% / 15% / 50% escalation tiers, lifetime offense counter (`uint32`, monotonically increasing), increasing reset periods, auto-ejection at 50% of minimum stake, challenge-bond mechanics. Carried over verbatim.

**Slashing distribution updated:**

| Destination | ADR 004 | ADR 026 (this ADR) |
| --- | ---: | ---: |
| Challenger reward | 50% | 50% |
| Safety & insurance reserve | 0% | 30% |
| Burn | 50% | 20% |
| **Total** | **100%** | **100%** |

Half of the prior burn share is redirected to `SafetyReserve` so user-harm incidents have a recourse path beyond pure deflation. The challenger share is unchanged — the reduction comes entirely from the burn share. Security review should confirm that the 20% remaining burn share preserves the deterrence argument materially; if not, the safety bound on the burn share (§11) leaves room for governance to recalibrate.

**Buyback-and-burn inflow rate.** 5% of fee inflow flows to `BuybackBurner` (vs ADR 004's 20% × 3% = 0.6% effective — an ~8× increase in USDC flow per unit of network revenue). [ADR 018](018-liquidity-strategy.md) mechanics, MEV protection, and POL custody are unchanged; only the inflow source (now `FeeRouter`, not manual treasury transfer) and rate change.

**Mature-scale burn estimate.** At 1,000 nodes × 30K GB/mo × $0.01/GB = $300K/mo gross revenue: 5% × $300K = $15K/mo USDC = $180K/yr → at $0.05 TOKEN = 3.6M TOKEN burned/yr = **0.36%/yr of 1B supply**. Full burn-vs-vesting-pressure and burn-sensitivity tables (S0–S3 × $0.001–$1.00 TOKEN price) live in the economic-model spec §§4–5.

**Operational constraint.** Burn must be TWAP-limited and liquidity-aware. Mature burn budgets can exceed available market depth, especially at low TOKEN prices (per economic-model spec §4, S2/S3 are the regimes where liquidity caps bind).

### 9. Governance

**Voting weight = ve-balance** (not raw TOKEN holdings). Sourced from `VotingEscrow.balanceOfAt(user, ts)` rather than `TOKEN.getPastVotes()`. Quorum and threshold are calibrated against `VotingEscrow.totalSupplyAt(ts)`.

| Parameter | Value |
| --- | --- |
| Voting source | `VotingEscrow.balanceOfAt` (was `TOKEN.getPastVotes`) |
| Proposal threshold | 0.1% of total ve-supply |
| Quorum | 4% of total ve-supply |
| Voting period | 7 days (matches [ADR 009](009-governance.md)) |
| Timelock | 48 hours (matches [ADR 009](009-governance.md)) |
| Delegation | ve-balance delegatable, Governor Bravo pattern |

Traders with no ve-position cannot vote. With v3 dropping auto-ve-lock-on-vest, the early veTOKEN base is concentrated in self-locked seed/team/treasury positions and POL/airdrop recipients who choose to lock — a smaller initial veTOKEN base than the v2 interim designs anticipated. **Governance bootstrapping may require a treasury-funded ve-lock-on-claim airdrop in the first 6–12 months** (sourced from the community / ecosystem allocation or pre-seed); sizing is open and tracked in the design spec's open-question list. Rest of [ADR 009](009-governance.md) (emergency multisig, hard-cap pause powers, etc.) unchanged.

### 10. Bootstrap mechanism — pre-seed USDC

ADR 004's 200M-TOKEN node-bootstrap fund is replaced by **$1M+ pre-seed USDC capital** (planning target: $3M). Removes the v2 / ADR 004 reflexive dependency on TOKEN price for bootstrap purchasing power. Program structure (Protocol-Owned Operators, hardware-leasing subsidies, staking loans, regional-deploy grants, Enterprise SLA guarantee fund) is forward-referenced to [ADR 030](#forward-references-follow-up-adrs); this ADR commits only to the funding mechanism (USDC, externally raised) and the size floor ($1M).

[ADR 019](019-node-onboarding.md) is the canonical onboarding flow and is updated under §"ADRs to update on acceptance" below.

### 11. Governable parameters with safety bounds

Router shares and the boost-floor parameter are governable, gated by 48-hour timelock per [ADR 009](009-governance.md), and bounded as below. Sum-to-100% across the six router shares is enforced on every governance update; updates that violate the sum or exceed any individual bound revert.

| Parameter | Default | Min | Max |
| --- | ---: | ---: | ---: |
| Node base share | 40% | 20% | 80% |
| Gauge boost share | 40% | 0% | 60% |
| Delegator share | 7% | 0% | 30% |
| Burn share | 5% | 0% | 25% |
| Treasury share | 5% | 0% | 20% |
| Safety share | 3% | 0% | 15% |
| `boostFloor` | 0.4 | 0.2 | 0.8 |

The 20% floor on the node-base share guarantees operators always receive enough liquid USDC to cover at least a meaningful fraction of infrastructure costs even under extreme governance proposals — preserves the cashflow invariant. The `boostFloor` bounds prevent governance from collapsing the gauge pool to a winner-take-all distribution (lower-bound) or flattening it into uselessness (upper-bound).

---

## Consequences

### Positive

- **Operator-driven ve-lock adoption.** Case B materially out-earns Case A — operators have a direct and persistent economic reason to ve-lock that ADR 004's flat-share design lacked. Likely converges to a steady-state ve-lock rate of 30–50% of total supply, matching Curve's 40–60% veCRV lock rate.
- **Stronger TOKEN demand loop, three-pronged.** Operators ve-lock to capture gauge boost (operator-side TOKEN demand). 7% delegator pool performs continuous TWAP USDC→TOKEN buys (delegator-side, proportional to revenue). 5% buyback-and-burn provides permanent supply reduction (deflationary).
- **Proven mechanism.** Curve's gauge + veCRV system has operated for 4+ years with billions in TVL. Reference implementations are open-source and auditable.
- **No cashflow crisis at the operator layer.** 40% liquid USDC per settlement is sufficient to cover infrastructure costs at the reference 1 Gbps / 30K GB/mo node (Case A is positive). Operators are never starved of USDC by the design.
- **USDC pre-seed eliminates TOKEN-price reflexivity in bootstrap.** Subsidy purchasing power does not collapse with TOKEN price — the largest tail risk of ADR 004's bootstrap design is removed.
- **Self-funding treasury at S1+.** Per economic-model spec §2, treasury net of $33K/mo team burn is positive from S1 (Early) onward.
- **Safety reserve creates enterprise-tier credibility.** Funded SLA-failure compensation makes the Enterprise tier sellable rather than purely best-effort decentralized.
- **Aggregate operator share = 80% of revenue, identical to ADR 004.** Existing operator-economics modeling carries over; only the ve-vs-non-ve distribution within the bucket changes.
- **Slashing redirected to user recourse.** 30% of slashed stake now funds incident payouts via `SafetyReserve` rather than disappearing to burn. User-harm incidents have a structural recourse path.

### Negative

- **Higher contract surface than ADR 004.** `FeeRouter` (with two pool types and the delegator-swap path), `VotingEscrow`, `SafetyReserve`, and the optional `DelegatorBuyer` add audit burden vs ADR 004's simpler `BuybackBurner` + `StakingRegistry` surface.
- **Per-epoch byte accounting adds gas.** Every settlement increments an operator's byte counter — 5K–15K gas on top of router forwarding. Minor but non-zero; needs validation on the chosen L2 (see [ADR 021](021-l2-chain-selection.md)).
- **Case A operators may exit.** Commodity operators who refuse to ve-lock see lower margins than Case B. This is the designed incentive pressure, but the failure mode is under-supply of operators if the filter is too sharp. Pre-seed staking-loan and hardware-lease programs ([ADR 030](#forward-references-follow-up-adrs)) partially offset.
- **Governance bootstrap thinner without auto-ve-lock-on-vest.** Initial veTOKEN supply depends on voluntary locking; first 6–12 months may need treasury-funded lock incentives.
- **Delegator-pool swap adds keeper dependency.** USDC→TOKEN conversion needs a keeper trigger (or fold into `BuybackBurner`'s existing keeper). Not a new failure mode — [ADR 018](018-liquidity-strategy.md) already has keeper dependency — but it expands the keeper's responsibilities.
- **Load-bearing math is harder to explain.** The Curve formula and the delegator-conversion mechanic are not intuitive to casual readers. UI, documentation, and operator dashboards need to expose "your boost factor," "your delegator-pool TOKEN earnings," and "delegator-pool slippage" clearly.
- **Effective supply growth ~24%/yr during vesting window.** Without auto-ve-lock-on-vest to slow it, the model relies on burn flow plus scenario-driven revenue growth to outweigh release pressure. Per economic-model spec §4, burn dominates monthly vesting only at S2+ at $0.05/TOKEN.
- **Replaces a familiar discount mechanic.** Operators who modeled ADR 004's fee discount must re-model under the gauge boost. Net P&L improves at fair-share ve, but the framing is unfamiliar.

### Risks

- **Equilibrium fragility.** The Curve-style model converges to a stable equilibrium *if* the boost is valuable enough to lock for but not so valuable that a winner-take-all dynamic emerges. The 40% gauge-pool default is sized in the middle by reasoned default; production tuning may be needed.
- **Reflexive bootstrap intensified at the operator-margin layer.** TOKEN price drop → ve-lock value drops → Case B margins shrink → operators unwind commitment. Pre-seed USDC insulates the *funding* side; the *operator-recruitment* side still depends on TOKEN price for ve-incentive strength. Mitigated, not eliminated.
- **Delegator-conversion MEV risk.** TWAP + private-RPC routing mitigates front-running, but the swap is observable on-chain post-fact. Flashbots-style bundles and per-epoch liquidity caps are required on this path, not optional. Keeper-cost economics under L2 gas conditions ([ADR 021](021-l2-chain-selection.md)) need validation.
- **Wash-trading / self-routed traffic.** An operator could induce noise settlements to inflate gauge-pool share. Mitigations are per-event settlement gas cost (~$0.08), watchtower observation of self-settlement patterns ([ADR 007](007-watchtower.md)), and most importantly **client-signed delivery receipts from distinct identities** tied to funded payment channels — the latter is the strongest invariant in the gauge-pool security model and is forward-referenced as [ADR 027](#forward-references-follow-up-adrs). **Strongly recommended for production launch; not optional.**
- **Governance-weight concentration.** Operators who lock heavily for boost also accumulate disproportionate governance weight. [ADR 009](009-governance.md) safety bounds prevent extreme abuse; team / seed / treasury vesting acts as a counterweight during the first ~3 years.
- **Convex-capture risk.** Third-party liquid-ve wrappers (Convex / Votium / Aura analogs) can concentrate governance power outside the DAO. Native `SveToken` (Frax sfrxETH model) is recommended; forward-referenced as [ADR 028](#forward-references-follow-up-adrs). Treat as priority-1 follow-up after launch.
- **Reduced burn-share deterrence.** Slashing distribution shift from 50% → 20% burn weakens pure-deflationary deterrence. The design relies on the 50% challenger share (unchanged) plus the new safety-reserve recourse path to keep deterrence net-positive. Security review should validate.

---

## Forward references (follow-up ADRs)

- **[ADR 027 — Distinct-client delivery receipts](027-distinct-client-receipts.md)** *(priority-1 follow-up; required for gauge-pool security; not optional for production launch).* Cryptographic protocol for client-signed delivery receipts tied to verifiable distinct client identities; gauge-pool eligibility gated on receipt validity. Closes the wash-trading attack surface flagged in §Risks.
- **[ADR 028 — Native sveTOKEN liquid-ve wrapper](028-sve-token-wrapper.md)** *(ship within 6 months of mainnet).* Frax sfrxETH-style native wrapper around `VotingEscrow` ve-positions; captures wrapper economics inside the DAO and pre-empts third-party Convex-capture.
- **[ADR 029 — Adaptive FeeRouter parameters](029-adaptive-fee-router.md)** *(deferred).* Two automated feedback hooks (lock-rate feedback shifting treasury → delegator pool when lock rate is low; price-floor feedback shifting treasury → BuybackBurner when 30-day TOKEN TWAP is below a governance-set floor). Both bounded within the §11 safety limits; no per-event governance vote.
- **[ADR 030 — Pre-seed USDC deployment program](030-preseed-usdc-deployment.md)** *(charter for the $1M+ pre-seed capital).* Protocol-Owned Operators, hardware-leasing subsidies, staking loans, regional-deploy grants, Enterprise SLA guarantee fund. Fills out the bootstrap mechanism this ADR commits to in principle.
- **[ADR 031 — Burn-and-Mint client TOKEN prepay path](031-bme-client-prepay.md)** *(deferred to v2).* Optional client-side TOKEN-prepay path (Helium BME pattern) for demand-side TOKEN sink; complements the operator- and delegator-side TOKEN demand in this ADR.
- **[ADR 032 — Bandwidth Futures / Enterprise SLA tier](032-bandwidth-futures-enterprise.md)** *(deferred to v2).* TOKEN-denominated bandwidth pre-purchase contracts; Enterprise SLA tier subsidized by the SafetyReserve plus pre-seed capital; SLA-failure compensation via SafetyReserve.

---

## ADRs to update on acceptance

This ADR is the decision record. The cross-cutting changes below are tracked separately and land in Phase 2 of the rollout plan; this list is the authoritative summary of what changes where.

| ADR | What changes |
| --- | --- |
| [ADR 003 — Payments](003-payments.md) | `PaymentChannel.settleChannel` routes the full operator USDC balance to `FeeRouter.routeSettlement(operator, bytesDelivered, amount)` in a single transaction (no settlement-time fee skim). Voucher payload carries per-settlement byte counts. New `FeeRouter` interface section. Operator's 40% base is paid same-tx; cache-miss node-to-node paid pulls bypass the router (documented). |
| [ADR 004 — Tokenomics](004-tokenomics.md) | **Marked Superseded.** Forwarding pointer to ADR 026; file kept as historical record. Token distribution, fee allocation, fee-discount mechanic, bootstrap fund, and slashing distribution all replaced; slashing rate schedule (5/15/50) carries over. |
| [ADR 009 — Governance](009-governance.md) | Voting source = `VotingEscrow.balanceOfAt`; quorum / threshold against `VotingEscrow.totalSupplyAt`. Safety-bound table replaced by §11 of this ADR. `boostFloor` bound `[0.2, 0.8]`. SafetyReserve payout rules added (evidence bundle + 48h appeal + post-incident registry; emergency multisig may execute under hard caps). Governor Bravo delegation for veTOKEN documented. Note on thinner governance bootstrap without auto-ve-lock-on-vest. |
| [ADR 016 — Contract Interactions](016-contract-interactions.md) | New contracts: `FeeRouter`, `VotingEscrow`, `SafetyReserve`, plus optional `DelegatorBuyer` (or `BuybackBurner` extension). Modified: `PaymentChannel.settleChannel` → `FeeRouter.routeSettlement`; `StakingRegistry` discount-threshold logic removed and min stake = 50K TOKEN; `Governor` voting source = `VotingEscrow.balanceOfAt`; `BuybackBurner` inflow source = `FeeRouter`. |
| [ADR 018 — Liquidity Strategy](018-liquidity-strategy.md) | Mechanics unchanged (Balancer V3 80/20, MEV via TWAP + minOut, POL custody). Inflow source = `FeeRouter` (not manual treasury transfer). Inflow rate ~8× higher per unit revenue (5% of 100% vs ADR 004's 20% × 3%). Parallel delegator-pool USDC→TOKEN swap path through the same pool. MEV mitigation hardened: Flashbots-style private RPC required, per-epoch liquidity caps required (not optional). |
| [ADR 019 — Node Onboarding](019-node-onboarding.md) | Bootstrap mechanism = $1M+ pre-seed USDC (forward-referenced to ADR 030). 100M / 200M TOKEN bootstrap fund removed. Min stake = 50K TOKEN; discount-threshold logic removed entirely. New onboarding paths (hardware leasing, staking loans, Protocol-Owned Operators, regional-deploy grants) per ADR 030. No auto-ve-lock-on-bootstrap. |
| [ADR 023 — PoC/Production Seams](023-poc-production-seams.md) | New seams in the wiring layer of the `node` crate: `FeeRouter` selector (PoC stub vs production contract per network); `VotingEscrow` lookup (local fixture vs on-chain `balanceOfAt`); swap-helper backend (local mock pool vs Balancer V3); `SafetyReserve` payout flow (local approval mock vs Governor-gated production). Domain crates stay free of v3-tokenomics conditional logic per ADR 023's leaf-crate principle. |

Additional ancillary updates (see rollout plan §1 for the full list): [ADR 007](007-watchtower.md) (wash-trading detection, receipt validation), [ADR 008](008-reputation.md) (gauge-eligibility gating on attested receipts), [ADR 020](020-observability.md) (new metrics for gauge / delegator / safety / ve-lock surfaces), [ADR 021](021-l2-chain-selection.md) (validation of per-settlement and per-epoch keeper gas economics on the chosen L2).
