# ADR 026: Tokenomics

**Date:** 2026-04-25
**Status:** Draft

## Context

The economic model — sitting on top of paid byte delivery ([ADR 003](003-payments.md#adr-003-payment-model)) and the slashing primitive ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)) — has to hold up under four pressures:

1. **A deflationary lever that scales with network usage.** A nominally "deflationary" token model whose burn rate sits well below circulating-supply growth from vesting unlocks is structurally inflationary in practice. Burn must be sized to compete with vesting flows at mature scale.
2. **A real-yield path to token holders.** Passive holders need compensation tied to network usage; long-term lockers need a compensation lever distinct from short-term holders. Without one, governance weight, liquidity provision, and long-term capital formation all weaken.
3. **A progressive operator incentive.** Per-operator return must scale with long-term commitment, not with stake size alone. A flat-rate or regressive mechanic (e.g., a fee discount that grows with raw stake) attracts capital without aligning it.
4. **A TOKEN-price-insulated bootstrap.** Subsidies denominated in the token they're meant to bootstrap collapse in purchasing power exactly when most needed. Bootstrap capital must be denominated in a unit independent of the protocol's own TOKEN price.

This ADR is the canonical economic model addressing all four. Burn is one of several deflationary levers; real yield in TOKEN flows to delegators and ve-lockers; operator compensation differentiates by long-term ve-commitment via a Curve-style gauge boost rather than by a discounted skim percentage; bootstrap is USDC-denominated. Full design reasoning, MEV-defense analysis, and equilibrium-stability argument are out of scope here; this ADR is the decision layer.

### Inputs assumed by this ADR

Pre-launch design with no holder-compensation or contract-migration concerns. ~$1M+ pre-seed USDC capital secured (planning target $3M); program structure is operational and tracked separately. 2026 unmetered-bandwidth provider economics (1 Gbps VPS, 10 Gbps dedicated, 100 Gbps edge tiers); dedicated-bandwidth nodes are realistic at every scale band the protocol is sized for.

## Decision

The protocol's economic model is defined by the following sections.

### Supply and distribution

**Supply.** 1,000,000,000 TOKEN, fixed at genesis. No post-genesis minting function exists on the production token contract.

#### Burnability

TOKEN is `ERC20Burnable`; any contract may burn TOKEN it holds via `burn` / `burnFrom`. Burns reduce `totalSupply` and emit `Transfer(from, address(0), amount)`. The [§ Slashing and burn](#slashing-and-burn) slashing-burn path uses this; future contract surfaces that need a TOKEN sink integrate via the same standard interface without contract changes.

#### Allocation (1B total)

Six buckets summing to 100%. Vesting profile: effective release rate is ~24%/yr during the active vesting window (Y1–Y3), 16%/yr in Y4, then zero.

| Allocation | Share | TOKEN | Vesting |
| --- | ---: | ---: | --- |
| Protocol treasury | 30% | 300,000,000 | 4-year linear |
| Seed backers | 24% | 240,000,000 | 3-year linear, 6-month cliff |
| Team & core contributors | 19% | 190,000,000 | 4-year linear, 12-month cliff |
| Community & ecosystem | 15% | 150,000,000 | 4-year linear |
| Genesis liquidity (POL) | 10% | 100,000,000 | Fully unlocked at genesis (Balancer V3 80/20 per [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol)) |
| Public sale / airdrop | 2% | 20,000,000 | Fully unlocked at genesis |
| **Total** | **100%** | **1,000,000,000** | |

**Genesis liquid supply.** 120,000,000 TOKEN (POL + public sale / airdrop). All other buckets release on vesting schedules.

#### No auto-ve-lock on vest

Vesting contracts release TOKEN unlocked into the recipient's wallet. Locking into `VotingEscrow` is opt-in. Rationale: the gauge-boost mechanism ([§ FeeRouter split (40/40/7/5/5/3)](#feerouter-split-40407553)) supplies a strong voluntary economic incentive to ve-lock without forcing long-term alignment via the vesting contract — seed/team term sheets are simpler, and lockers self-select. The cost is a thinner initial veTOKEN base; governance bootstrap may require treasury-funded ve-lock-on-claim airdrops in the first 6–12 months (see [§ Governance](#governance)).

#### No protocol-issued node-bootstrap fund

Bootstrap supply-side incentive is funded externally via $1M+ pre-seed USDC capital, eliminating TOKEN-price reflexivity in subsidy purchasing power. Program structure is operational and tracked separately.

### FeeRouter split (40/40/7/5/5/3)

`FeeRouter` receives the full operator USDC balance from `PaymentChannel.settleChannel` and atomically splits it into six buckets. `PaymentChannel` does not skim a protocol fee inline; all bucket distribution happens in `FeeRouter`. The bucket structure (six buckets, the named categories below, sum-to-100% invariant) is fixed at the contract level; **the share percentages themselves are governance-tunable** via `FeeRouter.setShares(...)` per [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics) so the network can launch with a simplified split (e.g. `80/0/0/10/10/0`) and dial up gauge / delegator / safety as their dependency contracts are wired in.

| Destination | Steady-state share | Unit | Distribution mechanic |
| --- | ---: | --- | --- |
| Node base | 40% | USDC | Direct same-tx, per-byte proportional to verified delivery |
| Gauge boost pool | 40% | USDC | Weekly epoch pool; pro-rata by ve-weighted `working_bytes` ([§ Gauge-boost formula](#gauge-boost-formula)); pull-based claim |
| Delegator pool | 7% | USDC → TOKEN | TWAP USDC→TOKEN swap; distributed pro-rata by ve-balance to delegators / ve-lockers ([§ Delegator pool — USDC → TOKEN conversion](#delegator-pool--usdc--token-conversion)) |
| Buyback-and-burn | 5% | USDC → TOKEN | Direct same-tx to `BuybackBurner`; mechanics unchanged from [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol); TOKEN burned |
| Protocol treasury | 5% | USDC | Direct same-tx to Timelock-custodied treasury wallet |
| Safety & insurance reserve | 3% | USDC | Direct same-tx to `SafetyReserve`; governance-gated incident payouts ([§ Safety and insurance reserve (3% bucket)](#safety-and-insurance-reserve-3-bucket)) |
| **Total** | **100%** | | |

The 40/40/7/5/5/3 row above is the **steady-state target**, reached once `VotingEscrow`, `SafetyReserve`, and `DelegatorBuyer` are deployed and governance has executed the corresponding `setShares` proposal under the standard 48h timelock. Inactive buckets (share = 0) accumulate zero with no reverts; same-tx legs short-circuit on the share check, epoch-bucket legs (gauge / delegator) skip the storage write. The launch share configuration is documented in [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics).

#### Aggregate operator-aligned compensation = 80%

(40% direct + 40% gauge pool). The 60% non-base buckets capture deflationary, governance, real-yield-to-lockers, and incident-recourse flows; the 40% gauge pool routes operator yield by long-term ve-commitment rather than by raw byte count.

#### Same-transaction guarantees

The 40% base, 5% burn, 5% treasury, and 3% safety legs all transfer in the settlement transaction. The 40% gauge and 7% delegator buckets accumulate in per-epoch buckets and are claim-based.

#### Node-to-node cache-miss paid pulls bypass the router

Direct peer USDC payment, no skim. Internal cost-recovery flow, not net protocol revenue.

**Gross client rate.** $0.01/GB — at parity with Bunny.net's budget tier and 7–20× cheaper than major traditional CDNs. No deCDN-specific premium. The router's 60% aggregate non-base skim is absorbed by operator net revenue, recovered through gauge-boost yield ([§ Operator economics and minimum stake](#operator-economics-and-minimum-stake)), TOKEN-economy exposure, and externally-funded pre-seed USDC subsidies — never passed to clients.

#### Epoch mechanics

Epoch length is 1 week (7 × 86400 s, block-timestamp-aligned). At epoch rollover the gauge and delegator buckets freeze, new buckets open, and per-operator `bytes_delivered` counters reset. ve-balance snapshots are taken at the epoch-boundary timestamp via `VotingEscrow.balanceOfAt(user, ts)`. Claim window is 26 epochs (~6 months); unclaimed allocations sweep to the treasury.

#### Claim rounding

Per-claimant payouts from the gauge and delegator epoch buckets use **truncating unsigned integer division** (the EVM's native `/`; all operands — `working_bytes`, `ve`, bucket amounts — are `uint`, so floor and round-toward-zero coincide). A gauge claim pays `floor(working_bytes_i × gaugeBucket[epoch] / sum(working_bytes))` with the [§ Gauge-boost formula](#gauge-boost-formula) per-operator cap applied first; a delegator claim pays `floor(ve_i × tokenBucket[epoch] / total_ve_at_epoch_boundary)`. No claim path rounds up: rounding up would let the final claimant of an epoch revert on an under-funded transfer. This direction is contract-level and **not** governance-tunable.

The truncation remainder — at most one base unit (USDC or TOKEN wei) per claimant per epoch — stays in that epoch's bucket and is **not** redistributed per-claim. It leaves via the existing [§ Epoch mechanics](#epoch-mechanics) 26-epoch claim-window sweep to the treasury, the same sink as genuinely unclaimed allocations. This deliberately does **not** use the [§ Gauge-boost formula](#gauge-boost-formula) next-epoch rollover path: that path is reserved for the whole-bucket degenerate cases (`sum(working_bytes) == 0`, post-cap residual). Per-claim truncation dust is bounded and already covered by the claim-window sweep; routing it through [§ Gauge-boost formula](#gauge-boost-formula) rollover would reopen the destination distinction (next-epoch bucket vs treasury sweep) that [§ FeeRouter split (40/40/7/5/5/3)](#feerouter-split-40407553) [Pre-launch gauge accumulation](#pre-launch-gauge-accumulation) is careful to keep separate.

#### Pre-launch gauge accumulation

The 40% gauge bucket MUST NOT pay out until the [ADR 034 § Per-operator gauge-share cap](034-gauge-boost-voting-escrow.md#per-operator-gauge-share-cap) is enforced (that section gives the wash-trading rationale and trade-offs). This sub-section pins the contract-level pause-and-cutover mechanism.

```solidity
// FeeRouter pre-launch gauge state.
bool    public gaugeLaunched;                                       // false until receipts ship and the cutover fires
mapping(uint64 epochId => uint256) public preLaunchGaugeAccumulator; // epoch-keyed escrow for the 40% gauge share
uint64  public gaugeLaunchEpoch;                                    // set on enableGauge(); zero pre-launch

/// One-shot governance setter (cannot be re-disabled — pre-launch is a launch-only state).
/// Sets gaugeLaunched = true and records gaugeLaunchEpoch = currentEpoch.
function enableGauge() external onlyGovernor;

event GaugeLaunched(uint64 indexed epoch);
```

**Behavior.**

- While `gaugeLaunched == false`: `routeSettlement` deposits the 40% gauge share into `preLaunchGaugeAccumulator[currentEpoch]` instead of the live gauge bucket. The other five buckets (40% direct, 7% delegator, 5% burn, 5% treasury, 3% safety) flow normally — the [§ FeeRouter split (40/40/7/5/5/3)](#feerouter-split-40407553) Same-transaction guarantees invariant holds end-to-end.
- `claimBoost(epochs[])` reverts on every requested epoch while `gaugeLaunched == false`. Once `true`, epochs in `[0, gaugeLaunchEpoch)` pay from `preLaunchGaugeAccumulator[epoch]` and epochs `≥ gaugeLaunchEpoch` pay from the live gauge bucket — both weighted by the ve-snapshot taken at each epoch boundary (§ Epoch mechanics captures these regardless of `gaugeLaunched` state; pre-launch epochs reuse them).
- **Partial cutover epoch.** Because `enableGauge()` is `onlyGovernor` and inherits the [ADR 009](009-governance.md#adr-009-governance-model) ~9-day latency (7-day vote + 48-hour timelock), the cutover lands at an arbitrary block within an epoch. Settlements before the cutover block credit `preLaunchGaugeAccumulator[gaugeLaunchEpoch]`; settlements after route via the live gauge bucket — both halves credit the same `gaugeLaunchEpoch` under the same epoch-boundary ve-snapshot, so a claimant receives `(preLaunchGaugeAccumulator[gaugeLaunchEpoch] + liveGaugeBucket[gaugeLaunchEpoch]) × ve_share`; the split is invisible at claim time.
- **Claim window for pre-launch epochs.** For any epoch `< gaugeLaunchEpoch` the 26-epoch window starts at `gaugeLaunchEpoch`, not the original epoch; unclaimed USDC sweeps to treasury after `gaugeLaunchEpoch + 26` per the § Epoch mechanics sweep rule.
- **Empty-snapshot at a pre-launch epoch.** Operators with zero ve at the historical snapshot get zero retroactive claim — intentional (gauge rewards ve-commitment, not retroactive attestation). The [§ Gauge-boost formula](#gauge-boost-formula) `sum(working_bytes) == 0` next-epoch rollover does **not** apply (the gauge bucket was never live during pre-launch epochs); un-distributable pre-launch USDC sweeps to treasury via claim-window expiry, not [§ Gauge-boost formula](#gauge-boost-formula) rollover. This differs from [§ Gauge-boost formula](#gauge-boost-formula)'s rollover only in destination (treasury sweep vs next-epoch bucket); do not conflate the two.

### Gauge-boost formula

The 40% gauge bucket is distributed by a Curve-style ve-weighted gauge-boost formula with degenerate-input fallbacks and a per-operator gauge-share cap (the canonical wash-trading defense). Full specification is in [ADR 034](034-gauge-boost-voting-escrow.md#adr-034-gauge-boost-and-voting-escrow).

### Voting escrow (`VotingEscrow`)

Operators opt into gauge boost by time-locking TOKEN in the non-transferable `VotingEscrow` contract (historical checkpointing, lock ownership, the contract interface). Full specification is in [ADR 034](034-gauge-boost-voting-escrow.md#adr-034-gauge-boost-and-voting-escrow).

### Safety and insurance reserve (3% bucket)

The 3% safety bucket is held in `SafetyReserve`, a governance-gated incident reserve covering incorrect-slashing / appeal reversals, relay / sequencer / payment-channel downtime, and bad-data incidents. Disbursements require an attested incident bundle, governance (or capped fast-track multisig) authorization, a 48-hour on-chain appeal window, and immutable post-incident reporting; queued claims disburse permissionlessly in epoch-FIFO order. Full specification — payout categories, spending controls, cross-category payout ordering, interface stability, and the `ISafetyReserve` contract surface — is in [ADR 033](033-safety-insurance-reserve.md#adr-033-safety-and-insurance-reserve).

### Delegator pool — USDC → TOKEN conversion

The 7% delegator bucket flows through a USDC→TOKEN buy-and-distribute pipeline (the `DelegatorBuyer` contract) rather than direct USDC distribution. Full specification is in [ADR 035](035-delegator-pool.md#adr-035-delegator-pool).

### Operator economics and minimum stake

**Minimum stake.** **50,000 TOKEN.** Slashable (rates per [§ Slashing and burn](#slashing-and-burn)), 7-day unbonding, slashable during unbonding. Sized so operator stake is a meaningful skin-in-the-game floor while keeping the gauge-boost ve-position the differentiating capital channel — the two roles are split cleanly.

#### No fee-discount mechanic

Operator yield differentiates by long-term ve-commitment via the gauge boost ([§ Gauge-boost formula](#gauge-boost-formula)), not by stake-multiple-keyed fee discounts. A discount-on-stake pattern was considered and rejected.

#### Revenue streams

1. **40% of every channel settlement** — direct USDC, same-tx, per-byte.
2. **Share of the 40% gauge-boost pool** — USDC, weekly distribution, weighted by `working_bytes`. Non-ve-lockers receive ~40% of fair-share; max-ve-lockers receive 100% of fair-share (2.5× more per byte than non-lockers).
3. **Optional delegator-pool yield** (TOKEN-denominated) on any TOKEN they ve-lock. Disjoint from the gauge pool; uncapped relative to byte share.

#### Sample 1 Gbps node P&L

Qualitative shape: fair-share ve materially out-earns no-ve at the reference 30K GB/mo node (the commodity operator is positive but thin and is the design's intended filter); over-ve is gauge-flat and earns its marginal yield via the delegator pool. Externally-funded operator-onboarding programs soften the filter for new operators.

### Slashing and burn

**Slashing rates.** 5% / 15% / 50% escalation tiers, lifetime offense counter (`uint32`, monotonically increasing), increasing reset periods, auto-ejection at 50% of minimum stake, challenge-bond mechanics.

**Slashing distribution.** **50% challenger / 30% SafetyReserve / 20% burn.** The challenger share is the deterrent that pays for active enforcement; the SafetyReserve share funds user-harm incident recourse beyond pure deflation; the burn share preserves the deflationary deterrent at a level governance can recalibrate within [§ Governable parameters with safety bounds](#governable-parameters-with-safety-bounds) bounds. Pure-burn variants were considered and rejected.

**Buyback-and-burn inflow.** 5% of routed USDC flows to `BuybackBurner` from `FeeRouter`. [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol) mechanics (Balancer V3 80/20 swap, TWAP, `minTokenOut`, POL custody) are inherited.

**Mature-scale burn estimate.** ~0.3–0.4%/yr of 1B supply at S2 reference scale.

#### Operational constraint

Burn must be TWAP-limited and liquidity-aware. Mature burn budgets can exceed available market depth, especially at low TOKEN prices (S2/S3 are the regimes where liquidity caps bind).

### Governance

#### Voting weight = ve-balance

(not raw TOKEN holdings). Sourced from `VotingEscrow.balanceOfAt(user, ts)` rather than `TOKEN.getPastVotes()`. Quorum and threshold are calibrated against `VotingEscrow.totalSupplyAt(ts)`.

| Parameter | Value |
| --- | --- |
| Voting source | `VotingEscrow.balanceOfAt` (was `TOKEN.getPastVotes`) |
| Proposal threshold | 0.1% of total ve-supply |
| Quorum | 4% of total ve-supply |
| Voting period | 7 days (matches [ADR 009](009-governance.md#adr-009-governance-model)) |
| Timelock | 48 hours (matches [ADR 009](009-governance.md#adr-009-governance-model)) |
| Total governance latency | ≈9 days (7-day vote + 48-hour timelock) |
| Delegation | ve-balance delegatable, Governor Bravo pattern |

Traders with no ve-position cannot vote. The early veTOKEN base is concentrated in self-locked seed/team/treasury positions and POL/airdrop recipients who choose to lock; **governance bootstrapping may require a treasury-funded ve-lock-on-claim airdrop in the first 6–12 months** (sourced from the community / ecosystem allocation or pre-seed). Rest of [ADR 009](009-governance.md#adr-009-governance-model) (emergency multisig, hard-cap pause powers, etc.) unchanged.

### Bootstrap mechanism — pre-seed USDC

Bootstrap supply-side incentive is **$1M+ pre-seed USDC capital** (planning target: $3M), externally raised. USDC denomination insulates subsidy purchasing power from TOKEN price. The protocol commits to the funding mechanism (USDC, externally raised) and the size floor ($1M); the operational program structure (allocation across operator-recruitment programs, eligibility, success metrics, governance flow) is tracked separately as a foundation/team operational concern, not as a protocol decision.

[ADR 019](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow) is the canonical onboarding flow.

### Governable parameters with safety bounds

Router shares, the boost-floor parameter, the per-operator gauge-share cap, and the claim window are governable, gated by 48-hour timelock per [ADR 009](009-governance.md#adr-009-governance-model), and bounded as below. Sum-to-100% across the six router shares is enforced on every governance update; updates that violate the sum or exceed any individual bound revert.

| Parameter | Default | Min | Max |
| --- | ---: | ---: | ---: |
| Node base share | 40% | 20% | 80% |
| Gauge boost share | 40% | 0% | 60% |
| Delegator share | 7% | 0% | 30% |
| Burn share | 5% | 0% | 25% |
| Treasury share | 5% | 0% | 20% |
| Safety share | 3% | 0% | 15% |
| `boostFloor` | 0.4 | 0.2 | 0.8 |
| `MAX_GAUGE_SHARE_PER_OPERATOR` | 5% | 1% | 25% |
| `epochLiquidityCapFraction` | 10% | 1% | 30% |
| `claimWindow` | 26 epochs | 13 epochs | 52 epochs (`uint16` count of epochs; the contract internally multiplies by the immutable `epochLength` to derive a seconds-domain deadline) |

The 20% floor on the node-base share guarantees operators always receive enough liquid USDC to cover at least a meaningful fraction of infrastructure costs even under extreme governance proposals — preserves the cashflow invariant. The `boostFloor` and `MAX_GAUGE_SHARE_PER_OPERATOR` bounds keep governance from breaking the gauge mechanics they parameterize (winner-take-all vs flat distribution; disabled vs over-tight wash-trading defense) — rationale in [ADR 034 § Gauge-boost formula](034-gauge-boost-voting-escrow.md#gauge-boost-formula) and [ADR 034 § Per-operator gauge-share cap](034-gauge-boost-voting-escrow.md#per-operator-gauge-share-cap). `epochLiquidityCapFraction` is the combined per-epoch ceiling on USDC notional swapped through the Balancer V3 80/20 pool across `BuybackBurner` and the delegator-pool swap path. The 1% floor prevents governance from starving the swap paths; the 30% ceiling prevents a single epoch from draining pool depth; the 10% default sizes one epoch's combined pressure conservatively against worst-case sustained execution. The cap is a single pool-wide budget per [ADR 018 § Liquidity-cap interaction](018-liquidity-strategy.md#liquidity-cap-interaction).

**Non-numeric one-shot setters.**

| Setter | Effect | Reversibility |
| --- | --- | --- |
| `enableGauge()` | Flips `gaugeLaunched = false → true`, records `gaugeLaunchEpoch`, emits `GaugeLaunched`. Activates the live gauge bucket from `gaugeLaunchEpoch` onward; pre-launch escrow becomes claimable from `gaugeLaunchEpoch` against the historical ve-snapshots already taken at each pre-launch epoch boundary (per [§ FeeRouter split (40/40/7/5/5/3)](#feerouter-split-40407553) Pre-launch gauge accumulation). | One-shot, irreversible. The pre-launch state is launch-only — there is no `disableGauge()`. |

`enableGauge()` is governable per [ADR 009](009-governance.md#adr-009-governance-model), inherits the `AccessControl` role-gating from [§ Governable parameters with safety bounds](#governable-parameters-with-safety-bounds) Setter contract-level bound enforcement, and has no numeric bound (binary state).

#### Setter contract-level bound enforcement

Parameter setters on `FeeRouter` and `VotingEscrow` are role-gated via `AccessControl` and bound-checked at the contract level — bounds are enforced regardless of caller. A future automated controller granted the parameter-setter role operates within the same bounds; out-of-range writes revert. This makes the bounds above effective for any caller (governance proposals or additive controllers), without trusting the caller to self-clamp.

## Consequences

### Positive

- **Operator-driven ve-lock adoption.** The gauge boost gives operators a direct and persistent economic reason to ve-lock; the system is expected to converge to a steady-state ve-lock rate of 30–50% of total supply, matching Curve's 40–60% veCRV lock rate.
- **Three-pronged TOKEN demand loop.** Operators ve-lock to capture gauge boost (operator side); the 7% delegator pool performs continuous TWAP USDC→TOKEN buys (delegator side, proportional to revenue); 5% buyback-and-burn provides permanent supply reduction.
- **Proven mechanism.** Curve's gauge + veCRV system has operated for 4+ years with billions in TVL. Reference implementations are open-source and auditable.
- **No cashflow crisis at the operator layer.** 40% liquid USDC per settlement covers infrastructure costs at the reference 1 Gbps / 30K GB/mo node — operators are never starved of USDC by the design.
- **USDC pre-seed eliminates TOKEN-price reflexivity in bootstrap.** Subsidy purchasing power does not collapse with TOKEN price.
- **Self-funding treasury at S1+.** Treasury net of $33K/mo team burn is positive from S1 (Early) onward.
- **Safety reserve creates enterprise-tier credibility.** Funded SLA-failure compensation makes the Enterprise tier sellable rather than purely best-effort decentralized.
- **Slashing funds user recourse.** 30% of slashed stake funds incident payouts via `SafetyReserve` — user-harm incidents have a structural recourse path.

### Negative

- **Significant contract surface.** `FeeRouter` (with two pool types and the delegator-swap path), `VotingEscrow`, `SafetyReserve`, and the optional `DelegatorBuyer` add meaningful audit burden. The [§ FeeRouter split (40/40/7/5/5/3)](#feerouter-split-40407553) Pre-launch gauge accumulation adds three storage slots (`gaugeLaunched`, `gaugeLaunchEpoch`, the `preLaunchGaugeAccumulator` mapping), one one-shot governance setter (`enableGauge()`), and one event (`GaugeLaunched`) on top of the existing `FeeRouter` surface — a small but non-zero increment that audit must include.
- **Per-epoch byte accounting adds gas.** Every settlement increments an operator's byte counter — 5K–15K gas on top of router forwarding. Minor but non-zero; needs validation on the chosen L2 (see [Appendix: L2 Deployment](appendix-l2-deployment.md#appendix-production-l2-deployment-target)).
- **Commodity operators face thin margins.** Operators who refuse to ve-lock see lower margins than fair-share-ve operators. This is the designed incentive pressure, but the failure mode is under-supply of operators if the filter is too sharp. Externally-funded operator-onboarding programs partially offset.
- **Governance bootstrap depends on voluntary locking.** Initial veTOKEN supply tracks self-locking decisions; first 6–12 months may need treasury-funded lock incentives.
- **Delegator-pool swap adds keeper dependency.** USDC→TOKEN conversion needs a keeper trigger (or fold into `BuybackBurner`'s existing keeper). Not a new failure mode — [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol) already has keeper dependency — but it expands the keeper's responsibilities.
- **Load-bearing math is harder to explain.** The Curve formula and the delegator-conversion mechanic are not intuitive to casual readers. UI, documentation, and operator dashboards need to expose "your boost factor," "your delegator-pool TOKEN earnings," and "delegator-pool slippage" clearly.
- **Effective supply growth ~24%/yr during vesting window.** With ve-locking opt-in, the model relies on burn flow plus scenario-driven revenue growth to outweigh release pressure. Burn dominates monthly vesting only at S2+ at $0.05/TOKEN.

### Risks

- **Equilibrium fragility.** The Curve-style model converges to a stable equilibrium *if* the boost is valuable enough to lock for but not so valuable that a winner-take-all dynamic emerges. The 40% gauge-pool default is sized in the middle by reasoned default; production tuning may be needed.
- **Reflexive operator-margin layer.** TOKEN price drop → ve-lock value drops → fair-share-ve margins shrink → operators unwind commitment. Pre-seed USDC insulates the *funding* side; the *operator-recruitment* side still depends on TOKEN price for ve-incentive strength. Mitigated, not eliminated.
- **Delegator-conversion MEV risk.** TWAP + private-RPC routing mitigates front-running, but the swap is observable on-chain post-fact. Flashbots-style bundles and per-epoch liquidity caps are required on this path, not optional. Keeper-cost economics under L2 gas conditions ([Appendix: L2 Deployment](appendix-l2-deployment.md#appendix-production-l2-deployment-target)) need validation.
- **Wash-trading / self-routed traffic.** An operator could induce noise settlements to inflate gauge-pool share. The structural defense and economic argument are in [ADR 034 § Per-operator gauge-share cap](034-gauge-boost-voting-escrow.md#per-operator-gauge-share-cap). The launch prerequisite is contract-pinned in [Pre-launch gauge accumulation](#pre-launch-gauge-accumulation): `gaugeLaunched == false` escrows the 40% gauge bucket per epoch, and the one-shot `enableGauge()` setter is the only path to live gauge payouts.
- **Governance-weight concentration.** Operators who lock heavily for boost also accumulate disproportionate governance weight. [ADR 009](009-governance.md#adr-009-governance-model) safety bounds prevent extreme abuse; team / seed / treasury vesting acts as a counterweight during the first ~3 years.
- **Convex-capture risk.** Third-party liquid-ve wrappers (Convex / Votium / Aura analogs) can concentrate governance power outside the DAO. Mitigation is operational — the DAO may ship a native liquid-ve wrapper as an additive top-level contract (integrating with `VotingEscrow` via the standard lock-creation / increase-amount / snapshot interfaces per [§ Voting escrow (`VotingEscrow`)](#voting-escrow-votingescrow)) without changing the launch contract surface.
- **20% burn share deterrence.** A higher burn share would weight slashing more toward pure deflation; the chosen 50/30/20 distribution prefers user-harm recourse via `SafetyReserve`. The [§ Governable parameters with safety bounds](#governable-parameters-with-safety-bounds) safety bound on the burn share leaves room for governance recalibration; security review should confirm 20% preserves slashing's deterrent value.
