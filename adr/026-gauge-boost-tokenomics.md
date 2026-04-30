# ADR 026: Gauge-Boost Tokenomics

**Date:** 2026-04-25
**Status:** Draft
**Source design spec:** internal `tokenomics-v2-gauge-boost-design` (2026-04-18)
**Economic source of truth:** internal `decdn-economic-model-40-40-gauge-pool` (2026-04-25)

---

## Context

The economic model — sitting on top of paid byte delivery ([ADR 003](003-payments.md)) and the slashing primitive ([ADR 014](014-on-chain-verification.md)) — has to hold up under four pressures:

1. **A deflationary lever that scales with network usage.** A nominally "deflationary" token model whose burn rate sits well below circulating-supply growth from vesting unlocks is structurally inflationary in practice. Burn must be sized to compete with vesting flows at mature scale.
2. **A real-yield path to token holders.** Passive holders need compensation tied to network usage; long-term lockers need a compensation lever distinct from short-term holders. Without one, governance weight, liquidity provision, and long-term capital formation all weaken.
3. **A progressive operator incentive.** Per-operator return must scale with long-term commitment, not with stake size alone. A flat-rate or regressive mechanic (e.g., a fee discount that grows with raw stake) attracts capital without aligning it.
4. **A TOKEN-price-insulated bootstrap.** Subsidies denominated in the token they're meant to bootstrap collapse in purchasing power exactly when most needed. Bootstrap capital must be denominated in a unit independent of the protocol's own TOKEN price.

This ADR is the canonical economic model addressing all four. Burn is one of several deflationary levers; real yield in TOKEN flows to delegators and ve-lockers; operator compensation differentiates by long-term ve-commitment via a Curve-style gauge boost rather than by a discounted skim percentage; bootstrap is USDC-denominated. Full design reasoning, MEV-defense analysis, equilibrium-stability argument, and reference-implementation pointers live in the source design spec; this ADR is the decision layer.

**Inputs assumed by this ADR.** Pre-launch design with no holder-compensation or contract-migration concerns. ~$1M+ pre-seed USDC capital secured (planning target $3M) — program structure defined in companion [ADR 030](030-preseed-usdc-deployment.md). 2026 unmetered-bandwidth provider economics per the design spec's input matrix (1 Gbps VPS, 10 Gbps dedicated, 100 Gbps edge tiers); dedicated-bandwidth nodes are realistic at every scale band the protocol is sized for.

Earlier internal drafts explored alternative shapes — a flat protocol-fee skim, a 200M-TOKEN bootstrap fund, a regressive fee-discount mechanic, auto-ve-lock-on-vest. Those are documented in [Alternatives Considered](#alternatives-considered) below.

---

## Decision

The protocol's economic model is defined by the following sections. Where a table fully duplicates one in the source design spec, this ADR shows the canonical defaults and points to the spec for the surrounding analysis.

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

**No auto-ve-lock on vest.** Vesting contracts release TOKEN unlocked into the recipient's wallet. Locking into `VotingEscrow` is opt-in. Rationale: the gauge-boost mechanism (§2) supplies a strong voluntary economic incentive to ve-lock without forcing long-term alignment via the vesting contract — seed/team term sheets are simpler, and lockers self-select. The cost is a thinner initial veTOKEN base; governance bootstrap may require treasury-funded ve-lock-on-claim airdrops in the first 6–12 months (see §9).

**No protocol-issued node-bootstrap fund.** Bootstrap supply-side incentive is funded externally via $1M+ pre-seed USDC capital, eliminating TOKEN-price reflexivity in subsidy purchasing power. Program structure in [ADR 030](030-preseed-usdc-deployment.md).

### 2. FeeRouter split (40/40/7/5/5/3)

`FeeRouter` receives the full operator USDC balance from `PaymentChannel.settleChannel` and atomically splits it into six buckets (full mechanic per design spec §2.2). `PaymentChannel` does not skim a protocol fee inline; all bucket distribution happens in `FeeRouter`.

| Destination | Share | Unit | Distribution mechanic |
| --- | ---: | --- | --- |
| Node base | 40% | USDC | Direct same-tx, per-byte proportional to verified delivery |
| Gauge boost pool | 40% | USDC | Weekly epoch pool; pro-rata by ve-weighted `working_bytes` (§3); pull-based claim |
| Delegator pool | 7% | USDC → TOKEN | TWAP USDC→TOKEN swap; distributed pro-rata by ve-balance to delegators / ve-lockers (§6) |
| Buyback-and-burn | 5% | USDC → TOKEN | Direct same-tx to `BuybackBurner`; mechanics unchanged from [ADR 018](018-liquidity-strategy.md); TOKEN burned |
| Protocol treasury | 5% | USDC | Direct same-tx to Timelock-custodied treasury wallet |
| Safety & insurance reserve | 3% | USDC | Direct same-tx to `SafetyReserve`; governance-gated incident payouts (§5) |
| **Total** | **100%** | | |

**Aggregate operator-aligned compensation = 80%** (40% direct + 40% gauge pool). The 60% non-base buckets capture deflationary, governance, real-yield-to-lockers, and incident-recourse flows; the 40% gauge pool routes operator yield by long-term ve-commitment rather than by raw byte count.

**Same-transaction guarantees.** The 40% base, 5% burn, 5% treasury, and 3% safety legs all transfer in the settlement transaction. The 40% gauge and 7% delegator buckets accumulate in per-epoch buckets and are claim-based.

**Node-to-node cache-miss paid pulls bypass the router.** Direct peer USDC payment, no skim. Internal cost-recovery flow, not net protocol revenue.

**Gross client rate.** $0.01/GB — at parity with Bunny.net's budget tier and 7–20× cheaper than major traditional CDNs. No deCDN-specific premium. The router's 60% aggregate non-base skim is absorbed by operator net revenue, recovered through gauge-boost yield (§7), TOKEN-economy exposure, and the pre-seed USDC subsidy programs ([ADR 030](030-preseed-usdc-deployment.md)) — never passed to clients.

**Epoch mechanics.** Epoch length is 1 week (7 × 86400 s, block-timestamp-aligned). At epoch rollover the gauge and delegator buckets freeze, new buckets open, and per-operator `bytes_delivered` counters reset. ve-balance snapshots are taken at the epoch-boundary timestamp via `VotingEscrow.balanceOfAt(user, ts)`. Claim window is 26 epochs (~6 months); unclaimed allocations sweep to the treasury.

### 3. Gauge-boost formula

Adapted from Curve Finance's veCRV gauge boost (in production since 2020). Replaces the LP-deposit primitive with verified-bytes-delivered.

Per-operator pool share = `working_bytes_i / sum(working_bytes)`, where `working_bytes_i = min(bytes_i, 0.4·bytes_i + 0.6·(ve_i/total_ve)·total_bytes)` over the epoch's verified bytes (full derivation in design spec §9.4).

**Properties:**

- **No ve-lock:** `working = 0.4 × bytes` — the commodity floor. Receives 40% of what a fair-share-ve operator with the same byte count would.
- **Fair-share ve** (`ve_i / total_ve ≥ bytes_i / total_bytes`): `working = bytes_i` — the cap binds. Full proportional share of the pool.
- **Over-ve:** `working` capped at `bytes_i` — no over-boost in the gauge pool. Excess ve still earns from the 7% delegator pool linearly.
- **Maximum boost ratio = 1 / 0.4 = 2.5×** between a max-ve-locker and a zero-ve-locker delivering the same byte count.

**Degenerate-input fallbacks** (required to prevent division-by-zero at launch and on quiet epochs):

- `total_ve == 0` (no ve-locks exist anywhere — bootstrap window): the `ve_i / total_ve` term is undefined. The contract MUST treat `working_bytes_i = boostFloor × bytes_i = 0.4 × bytes_i` for every operator — every operator receives the commodity floor, share is purely byte-proportional. This is the natural limit of the Curve formula as ve-supply approaches zero.
- `sum(working_bytes) == 0` (no operator delivered any verified bytes in the epoch): the per-operator share is undefined. The epoch's gauge bucket is **not** distributed; it remains in `FeeRouter`'s gauge accumulator and is included in the next epoch's bucket. This is preferred over sweeping to treasury immediately because the empty-epoch case is most likely an outage, not a permanent state — the next active epoch should benefit from the rolled-over USDC. The 26-epoch claim window (§2 Epoch mechanics) caps the total rollover; unclaimed-after-26-epochs USDC sweeps to treasury per the existing rule.
- `bytes_i == 0` (operator delivered nothing this epoch): trivially `working_bytes_i = 0` and that operator's share is `0`. No special-case required — the formula handles this directly.

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
| `create_lock_for` privileged path | **None** — no auto-ve-lock path |

**Historical checkpointing.** `VotingEscrow.balanceOfAt(user, ts)` and `totalSupplyAt(ts)` are load-bearing for the epoch-snapshot pattern in §2 and the governance pattern in §8. Per-lock checkpoints; reads O(log n) on the checkpoint array; writes O(1) amortized.

**Operator stake and ve-positions are separate.** A node's operator stake is held in `StakingRegistry` and is slashable (rates per §8). A ve-position is held in `VotingEscrow` and is not. Neither satisfies the other's requirements; an operator may hold any combination. This separation is a hard invariant — no contract path lets ve-locked TOKEN be slashed.

### 5. Safety and insurance reserve (3% bucket)

The 3% safety bucket is held in `SafetyReserve`, a governance-gated incident reserve. Eligible payout categories per design spec §2.2.5:

- Enterprise SLA compensation (per [ADR 030](030-preseed-usdc-deployment.md) / [ADR 032](032-bandwidth-futures-enterprise.md)).
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

**Why TOKEN-denominated, not USDC?** Routes acquired TOKEN to the participants with the longest commitment horizon and couples ve-locker yield to TOKEN value rather than to network revenue alone — when network revenue grows, TOKEN buy pressure grows, ve-locker positions appreciate. This is the model's primary "real yield in TOKEN" lever; an alternative pattern (USDC distribution to a passive ve-pool) is documented in [Alternatives Considered](#alternatives-considered).

**MEV / slippage.** TWAP windows + per-epoch liquidity caps + private-RPC routing (Flashbots-style bundles) for the swap. Same defenses as the [ADR 018](018-liquidity-strategy.md) buyback flow; per-epoch liquidity caps are a hard requirement on this path, not optional.

### 7. Operator economics and minimum stake

**Minimum stake.** **50,000 TOKEN.** Slashable (rates per §8), 7-day unbonding, slashable during unbonding. Sized so operator stake is a meaningful skin-in-the-game floor while keeping the gauge-boost ve-position the differentiating capital channel — the two roles are split cleanly.

**No fee-discount mechanic.** Operator yield differentiates by long-term ve-commitment via the gauge boost (§3), not by stake-multiple-keyed fee discounts. A discount-on-stake pattern is documented in [Alternatives Considered](#alternatives-considered).

**Revenue streams** (per design spec §2.4):

1. **40% of every channel settlement** — direct USDC, same-tx, per-byte.
2. **Share of the 40% gauge-boost pool** — USDC, weekly distribution, weighted by `working_bytes`. Non-ve-lockers receive ~40% of fair-share; max-ve-lockers receive 100% of fair-share (2.5× more per byte than non-lockers).
3. **Optional delegator-pool yield** (TOKEN-denominated) on any TOKEN they ve-lock. Disjoint from the gauge pool; uncapped relative to byte share.

**Sample 1 Gbps node P&L** (full multi-scenario model, including absolute figures and the S0–S3 × node-type-A–E unmetered-infra cost matrix, lives in design spec §2.4 / §3). Qualitative shape: fair-share ve materially out-earns no-ve at the reference 30K GB/mo node (the commodity operator is positive but thin and is the design's intended filter); over-ve is gauge-flat and earns its marginal yield via the delegator pool. Pre-seed staking-loan and hardware-lease programs ([ADR 030](030-preseed-usdc-deployment.md)) soften the filter for new operators.

### 8. Slashing and burn

**Slashing rates.** 5% / 15% / 50% escalation tiers, lifetime offense counter (`uint32`, monotonically increasing), increasing reset periods, auto-ejection at 50% of minimum stake, challenge-bond mechanics.

**Slashing distribution.** **50% challenger / 30% SafetyReserve / 20% burn.** The challenger share is the deterrent that pays for active enforcement; the SafetyReserve share funds user-harm incident recourse beyond pure deflation; the burn share preserves the deflationary deterrent at a level governance can recalibrate within §11 bounds. Pure-burn variants are documented in [Alternatives Considered](#alternatives-considered).

**Buyback-and-burn inflow.** 5% of routed USDC flows to `BuybackBurner` from `FeeRouter`. [ADR 018](018-liquidity-strategy.md) mechanics (Balancer V3 80/20 swap, TWAP, `minTokenOut`, POL custody) are inherited.

**Mature-scale burn estimate.** ~0.3–0.4%/yr of 1B supply at S2 reference scale (full burn-vs-vesting and burn-sensitivity tables across S0–S3 × $0.001–$1.00 TOKEN price live in economic-model spec §§4–5).

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
| Total governance latency | ≈9 days (7-day vote + 48-hour timelock) |
| Delegation | ve-balance delegatable, Governor Bravo pattern |

Traders with no ve-position cannot vote. The early veTOKEN base is concentrated in self-locked seed/team/treasury positions and POL/airdrop recipients who choose to lock; **governance bootstrapping may require a treasury-funded ve-lock-on-claim airdrop in the first 6–12 months** (sourced from the community / ecosystem allocation or pre-seed). Sizing is open and tracked in the design spec's open-question list. Rest of [ADR 009](009-governance.md) (emergency multisig, hard-cap pause powers, etc.) unchanged.

### 10. Bootstrap mechanism — pre-seed USDC

Bootstrap supply-side incentive is **$1M+ pre-seed USDC capital** (planning target: $3M), externally raised. USDC denomination insulates subsidy purchasing power from TOKEN price. Program structure (Protocol-Owned Operators, hardware-leasing subsidies, staking loans, regional-deploy grants, Enterprise SLA guarantee fund) lives in [ADR 030](030-preseed-usdc-deployment.md); this ADR commits the funding mechanism (USDC, externally raised) and the size floor ($1M).

[ADR 019](019-node-onboarding.md) is the canonical onboarding flow.

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

- **Operator-driven ve-lock adoption.** The gauge boost gives operators a direct and persistent economic reason to ve-lock; the system is expected to converge to a steady-state ve-lock rate of 30–50% of total supply, matching Curve's 40–60% veCRV lock rate.
- **Three-pronged TOKEN demand loop.** Operators ve-lock to capture gauge boost (operator side); the 7% delegator pool performs continuous TWAP USDC→TOKEN buys (delegator side, proportional to revenue); 5% buyback-and-burn provides permanent supply reduction.
- **Proven mechanism.** Curve's gauge + veCRV system has operated for 4+ years with billions in TVL. Reference implementations are open-source and auditable.
- **No cashflow crisis at the operator layer.** 40% liquid USDC per settlement covers infrastructure costs at the reference 1 Gbps / 30K GB/mo node — operators are never starved of USDC by the design.
- **USDC pre-seed eliminates TOKEN-price reflexivity in bootstrap.** Subsidy purchasing power does not collapse with TOKEN price.
- **Self-funding treasury at S1+.** Per economic-model spec §2, treasury net of $33K/mo team burn is positive from S1 (Early) onward.
- **Safety reserve creates enterprise-tier credibility.** Funded SLA-failure compensation makes the Enterprise tier sellable rather than purely best-effort decentralized.
- **Slashing funds user recourse.** 30% of slashed stake funds incident payouts via `SafetyReserve` — user-harm incidents have a structural recourse path.

### Negative

- **Significant contract surface.** `FeeRouter` (with two pool types and the delegator-swap path), `VotingEscrow`, `SafetyReserve`, and the optional `DelegatorBuyer` add meaningful audit burden.
- **Per-epoch byte accounting adds gas.** Every settlement increments an operator's byte counter — 5K–15K gas on top of router forwarding. Minor but non-zero; needs validation on the chosen L2 (see [Appendix: L2 Deployment](appendix-l2-deployment.md)).
- **Commodity operators face thin margins.** Operators who refuse to ve-lock see lower margins than fair-share-ve operators. This is the designed incentive pressure, but the failure mode is under-supply of operators if the filter is too sharp. Pre-seed staking-loan and hardware-lease programs ([ADR 030](030-preseed-usdc-deployment.md)) partially offset.
- **Governance bootstrap depends on voluntary locking.** Initial veTOKEN supply tracks self-locking decisions; first 6–12 months may need treasury-funded lock incentives.
- **Delegator-pool swap adds keeper dependency.** USDC→TOKEN conversion needs a keeper trigger (or fold into `BuybackBurner`'s existing keeper). Not a new failure mode — [ADR 018](018-liquidity-strategy.md) already has keeper dependency — but it expands the keeper's responsibilities.
- **Load-bearing math is harder to explain.** The Curve formula and the delegator-conversion mechanic are not intuitive to casual readers. UI, documentation, and operator dashboards need to expose "your boost factor," "your delegator-pool TOKEN earnings," and "delegator-pool slippage" clearly.
- **Effective supply growth ~24%/yr during vesting window.** With ve-locking opt-in, the model relies on burn flow plus scenario-driven revenue growth to outweigh release pressure. Per economic-model spec §4, burn dominates monthly vesting only at S2+ at $0.05/TOKEN.

### Risks

- **Equilibrium fragility.** The Curve-style model converges to a stable equilibrium *if* the boost is valuable enough to lock for but not so valuable that a winner-take-all dynamic emerges. The 40% gauge-pool default is sized in the middle by reasoned default; production tuning may be needed.
- **Reflexive operator-margin layer.** TOKEN price drop → ve-lock value drops → fair-share-ve margins shrink → operators unwind commitment. Pre-seed USDC insulates the *funding* side; the *operator-recruitment* side still depends on TOKEN price for ve-incentive strength. Mitigated, not eliminated.
- **Delegator-conversion MEV risk.** TWAP + private-RPC routing mitigates front-running, but the swap is observable on-chain post-fact. Flashbots-style bundles and per-epoch liquidity caps are required on this path, not optional. Keeper-cost economics under L2 gas conditions ([Appendix: L2 Deployment](appendix-l2-deployment.md)) need validation.
- **Wash-trading / self-routed traffic.** An operator could induce noise settlements to inflate gauge-pool share. Mitigations are per-event settlement gas cost (~$0.08), watchtower observation of self-settlement patterns ([ADR 007](007-watchtower.md)), and most importantly **client-signed delivery receipts from distinct identities** tied to funded payment channels — the latter is the strongest invariant in the gauge-pool security model and is forward-referenced as [ADR 027](027-distinct-client-receipts.md). **The protocol can launch with the gauge pool paused, but enabling and paying the gauge pool requires distinct-client receipts to be live** (see [ADR 027 §9 — Implementation sequencing and launch prerequisite](027-distinct-client-receipts.md#9-implementation-sequencing-and-launch-prerequisite)).
- **Governance-weight concentration.** Operators who lock heavily for boost also accumulate disproportionate governance weight. [ADR 009](009-governance.md) safety bounds prevent extreme abuse; team / seed / treasury vesting acts as a counterweight during the first ~3 years.
- **Convex-capture risk.** Third-party liquid-ve wrappers (Convex / Votium / Aura analogs) can concentrate governance power outside the DAO. Native `SveToken` (Frax sfrxETH model) is recommended; forward-referenced as [ADR 028](028-sve-token-wrapper.md). Treat as priority-1 follow-up after launch.
- **20% burn share deterrence.** A higher burn share would weight slashing more toward pure deflation; the chosen 50/30/20 distribution prefers user-harm recourse via `SafetyReserve`. The §11 safety bound on the burn share leaves room for governance recalibration; security review should confirm 20% preserves slashing's deterrent value.

---

## Alternatives Considered

Earlier internal drafts of the tokenomics model explored alternative shapes. The following were considered and rejected for the reasons noted; this section is the canonical record so future readers can see what was on the table without inferring it from the current design.

### Original tokenomics shape

A 1B-token model with a 3% protocol fee, 80/20/0/20 dev/audit/eco/burn allocation of the fee bucket, a regressive fee-discount mechanic ("stake 10× minimum to pay 1.5% fee instead of 3%"), a 200M-TOKEN node-bootstrap fund, and 50/50 burn/challenger slashing distribution.

Rejected because:

- **Burn flow was structurally noise.** ~0.014%/yr of supply at a 1,000-node mature network ($0.05 TOKEN), well below the ~22%/yr circulating-supply growth from vesting. Burn alone could not bound supply.
- **No yield path to passive holders or long-term lockers.** Stake-to-operate, fee discount, and governance were the only TOKEN utilities; long-term lockers had no compensation lever distinct from short-term holders.
- **Regressive fee discount.** The discount-on-stake-multiple pattern reduced buyback flow as more operators qualified — large stakers weakened the deflationary sink. Non-progressive and non-aligned with long-term commitment.
- **TOKEN-denominated bootstrap was reflexive.** The 200M-TOKEN bootstrap fund's purchasing power collapsed exactly when subsidies were most needed.

### Auto-ve-lock-on-vest

Variant: vesting contracts auto-lock released TOKEN into `VotingEscrow` for a fixed term (commonly 1 year) before delivering to the recipient. Intent: forced long-term alignment and a thicker veTOKEN base from genesis.

Rejected because:

- The gauge boost (§3) is a stronger *voluntary* incentive than a vesting-imposed lock — recipients who would benefit from ve-locking choose to, others don't. Forced locking is not load-bearing for the equilibrium.
- Term sheets get more complex (seed / team locking semantics interact with the existing 4-year vest), and the auto-lock contract is a new audit surface.
- The cost is a thinner initial veTOKEN base; the design absorbs that cost via §9's optional treasury-funded ve-lock-on-claim airdrop in the first 6–12 months.

### USDC distribution to a passive ve-pool

Variant for §6 delegator pool: distribute the 7% bucket as USDC directly to ve-lockers, rather than performing the USDC→TOKEN swap.

Rejected because:

- Decouples ve-locker yield from TOKEN appreciation — the "real yield in TOKEN" lever is the larger long-term value driver and creates the demand-side TWAP buy pressure that compounds with operator-side ve-locking.
- USDC distribution to ve-lockers is structurally a stablecoin-yield product, which fits poorly with the ve-position's role as a long-duration commitment device.

### Pure-deflationary slashing (50/50 challenger/burn)

Variant for §8: keep the original 50% challenger / 50% burn distribution, omit the SafetyReserve share.

Rejected because:

- User-harm incidents have no structural recourse path under pure-deflationary slashing. Enterprise SLA credibility, incorrect-slash appeals, and bad-data incident compensation all need a funded reserve, and the 3% router share alone is insufficient to seed it at early scale.
- The 20% burn share (vs. the original 50%) preserves a meaningful pure-deflationary lever and the §11 safety bound `[0%, 25%]` lets governance recalibrate post-launch if deterrence proves insufficient.

### TOKEN-denominated node-bootstrap fund

Variant: a protocol-issued multi-hundred-million-TOKEN bootstrap fund that disburses TOKEN to early operators.

Rejected because:

- Subsidy purchasing power tracks TOKEN price; subsidies become least valuable exactly when most needed.
- Concentrates pre-launch dilution in a single allocation tied to bootstrap duration rather than network outcomes.
- Externally-raised USDC pre-seed ([ADR 030](030-preseed-usdc-deployment.md)) is the chosen alternative — TOKEN-price-independent, externally-priced, and structured into the five outcome-targeted programs in ADR 030 §2.

---

## Forward references (follow-up ADRs)

- **[ADR 027 — Distinct-client delivery receipts](027-distinct-client-receipts.md)** — priority-1; required for gauge-pool security at mainnet launch.
- **[ADR 028 — Native sveTOKEN liquid-ve wrapper](028-sve-token-wrapper.md)** — within 6 months of mainnet; pre-empts third-party Convex-capture.
- **[ADR 029 — Adaptive FeeRouter parameters](029-adaptive-fee-router.md)** — post-launch; bounded automated feedback hooks within §11 limits.
- **[ADR 030 — Pre-seed USDC deployment program](030-preseed-usdc-deployment.md)** — charter for the $1M+ pre-seed capital.
- **[ADR 031 — Burn-and-Mint client TOKEN prepay path](031-bme-client-prepay.md)** — post-launch follow-up; demand-side TOKEN sink.
- **[ADR 032 — Bandwidth Futures / Enterprise SLA tier](032-bandwidth-futures-enterprise.md)** — post-launch follow-up; TOKEN-denominated pre-purchase + Enterprise SLA tier.
