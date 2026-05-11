# Appendix: Wash-Trading Economics

> **This is an appendix, not a core protocol ADR.** It records the quantitative analysis backing the launch prerequisite in [ADR 026 §2 Pre-launch gauge accumulation](026-gauge-boost-tokenomics.md#pre-launch-gauge-accumulation) and the per-operator gauge-share cap rationale in [ADR 026 §3 Per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap). Where the canonical ADR text cites this appendix, the underlying argument is here.

## Context

[ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula) distributes 40% of `FeeRouter` revenue via a Curve-style gauge formula whose input is per-operator `bytes_delivered`. Without a structural bound, an operator can inflate `bytes_delivered` by routing self-funded settlements through self-controlled clients — paying USDC from one wallet they control into a channel funded by another wallet they control, settling the channel through `PaymentChannel.settleChannel`, and capturing the resulting share of the 40% gauge bucket. This appendix derives the economics of that attack and shows why the `MAX_GAUGE_SHARE_PER_OPERATOR` cap (default 5%, governable `[1%, 25%]` per [ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds)) is the binding on-chain defense.

The reputation-layer distinct-counterparty discount ([ADR 008 §4.1](008-reputation.md#41-distinct-counterparty-discount)) and the reputation-as-off-chain-signal mechanism ([ADR 008 §12](008-reputation.md#12-gauge-pool-wash-trading-reputation-as-off-chain-signal)) are the soft layer; this appendix is concerned with the on-chain cap because it is the only mechanism enforced without external attestation.

## Assumptions

The derivation uses parameters canonical in the ADR set:

| Quantity | Value | Source |
| --- | --- | --- |
| `FeeRouter` node-base share | 40% | [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) |
| `FeeRouter` gauge-boost share | 40% | [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) |
| `FeeRouter` delegator share | 7% | [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) |
| `FeeRouter` burn share | 5% | [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) |
| `FeeRouter` treasury share | 5% | [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) |
| `FeeRouter` safety-reserve share | 3% | [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) |
| `MAX_GAUGE_SHARE_PER_OPERATOR` default | 5% | [ADR 026 §3](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) |
| `MAX_GAUGE_SHARE_PER_OPERATOR` governance bounds | `[1%, 25%]` | [ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds) |
| Minimum operator stake | 50,000 TOKEN | [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake) |

**Wash-trader posture.** A worst-case wash-trader is a registered operator (50,000 TOKEN staked per pseudo-operator) and a ve-locker (some TOKEN locked in `VotingEscrow` to capture both gauge boost and a share of the delegator pool). This posture maximizes the wash-trader's recoverable fraction of `FeeRouter` outputs, which is the conservative direction for evaluating the defense.

## Attack model

The wash-trader operates a node A and one or more client wallets they control. They route a synthetic settlement of `$X` USDC through `PaymentChannel.settleChannel`; `FeeRouter` splits `$X` across the six buckets per the table above. The wash-trader pays `$X` from their own client wallet to themselves (as the node) — the principal `$X` does not leave their balance sheet; only the portion that escapes their control via `FeeRouter` is a real cost.

For the wash-trader who is also a registered staker and a ve-locker:

- **Node-base share (40%).** Returns to the wash-trader as the receiving node. Net cost: zero.
- **Gauge-boost share (40%).** Distributed pro-rata by ve-weighted `working_bytes`, subject to `MAX_GAUGE_SHARE_PER_OPERATOR`. The wash-trader's recovery is `min(their_share_fraction, MAX_GAUGE_SHARE_PER_OPERATOR) × 40c per $1`.
- **Delegator share (7%).** Distributed pro-rata by ve-balance. The wash-trader recovers their ve-share — assumed small but non-zero. Treated as recoverable below; conservative direction is to exclude it, but doing so does not change the qualitative conclusion.
- **Burn share (5%).** TOKEN burned via USDC→TOKEN swap, reducing `totalSupply`. The wash-trader holds TOKEN (at minimum the 50,000 stake; usually more if ve-locked); supply reduction accrues pro-rata to all TOKEN holders. Treated as recoverable below.
- **Treasury share (5%).** Direct same-tx to the treasury wallet. The wash-trader does not control treasury disbursements. Net cost: 5c per `$1`.
- **Safety-reserve share (3%).** Direct same-tx to `SafetyReserve`. The wash-trader does not control reserve disbursements except as a potential payout recipient under [ADR 026 §5](026-gauge-boost-tokenomics.md#5-safety-and-insurance-reserve-3-bucket) (a far-future contingent claim). Net cost: 3c per `$1`.

**Net leakage per `$1` wash-traded: 8c (treasury + safety).** This matches the figure used in the canonical ADR text. The burn and delegator components are treated as recoverable for the wash-trader who holds TOKEN; treating either as leakage strengthens the defense (a lower-bound margin on the breakeven below).

## Breakeven derivation

Per `$1` wash-traded:

- **Cost:** 8c (treasury + safety leakage).
- **Gain:** `min(s, c) × 40c`, where `s` is the wash-trader's effective `working_bytes` share fraction (numerator: their inflated bytes; denominator: total network bytes including the inflation) and `c = MAX_GAUGE_SHARE_PER_OPERATOR`.

Setting gain = cost:

```
s* × 40c = 8c     ⟹     s* = 0.20
```

**The breakeven `working_bytes`-share fraction is 20%.** A wash-trader whose effective share exceeds 20% extracts more from the gauge pool than they leak per cycle; below 20%, wash-trading is net-negative per cycle (ignoring capital costs, which only worsen the wash-trader's position).

The cap `c` bounds the gain:

| Cap value | Maximum recoverable gain per `$1` | Net per `$1` (gain − cost) | Wash-trading is… |
| ---: | ---: | ---: | --- |
| No cap (`s` can approach 100%) | 40c | +32c | net-positive |
| 25% (governance upper bound) | 10c | +2c | thin net-positive |
| 20% (breakeven) | 8c | 0 | breakeven |
| 5% (default) | 2c | −6c | net-negative |
| 1% (governance lower bound) | 0.4c | −7.6c | net-negative |

**The default cap of 5% leaves a margin of 15 percentage points below the breakeven share fraction.** The governance lower bound of 1% preserves the defense even under aggressive cap tightening. The governance upper bound of 25% admits a configuration that is thinly net-positive in pure per-cycle USDC terms; this is the rationale for the 25% upper bound rather than a higher value — the bound is set just above the breakeven so governance can tune the cap to admit legitimate large operators without crossing into a wash-trade-profitable regime, but cannot configure the parameter so loosely that the structural defense disappears.

## Sybil expansion

A wash-trader can attempt to evade the per-operator cap by splitting their wash-trade across `N` pseudo-operators, each below the cap individually. Two constraints make this expensive:

1. **Per-pseudo-operator staking cost.** Each pseudo-operator requires its own [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake) minimum stake of 50,000 TOKEN locked in `StakingRegistry`. For `N` pseudo-operators, total capital lockup is `N × 50,000` TOKEN, with the lockup duration bounded below by `StakingRegistry.unbondingPeriod` (3–30 days per [ADR 009](009-governance.md#governable-parameters-with-safety-bounds), default 7 days). The capital cost scales linearly with operator count.

2. **Per-counterparty diversity discount on reputation.** [ADR 008 §4.1](008-reputation.md#41-distinct-counterparty-discount) imposes a `diversity_factor` of `min(distinct_counterparties / 5, 1)` on reported settled value. A wash-trader cycling funds among `N` pseudo-operators each settles with the same small counterparty set; below 5 distinct counterparties per reporter, the `diversity_factor` shrinks the reputation signal by up to 80%. This does not directly affect on-chain gauge-share — the cap is the binding defense there — but it does limit the wash-trader's ability to convert the synthetic settlement history into reputation-weighted protocol standing (node selection, gauge-eligibility tier signaling) that compounds with the gauge-share extraction.

Combined: a wash-trader who wants to extract `g × 40c per $1` (where `g > c`, the cap) by splitting across `N` operators needs `g/c` operators, each locking 50,000 TOKEN. **Sybil expansion converts wash-trading from a heuristic-bypass attack into a stake-proportional capital-lockup attack** — the attacker's marginal cost scales linearly with their target extraction rate, while the per-cycle USDC margin (at the cap) remains net-negative without the sybil expansion.

**TOKEN-price interaction.** Capital lockup is denominated in TOKEN; gauge-share gain is denominated in USDC. At elevated TOKEN prices (genesis target is `~$0.20`; 3–5× scenarios contemplated in [ADR 026 §2](026-gauge-boost-tokenomics.md#pre-launch-gauge-accumulation)), the capital lockup per pseudo-operator rises proportionally while the per-cycle USDC margin is unchanged. The sybil-expansion cost therefore *increases* at higher TOKEN prices, not decreases. The relevant price-sensitivity hazard is the converse: at *genesis-low* TOKEN prices, the 50,000-TOKEN lockup is denominated in cheap TOKEN, and sybil expansion is at its most affordable relative to extractable USDC. This is the precise window the [ADR 026 §2 Pre-launch gauge accumulation](026-gauge-boost-tokenomics.md#pre-launch-gauge-accumulation) launch prerequisite addresses by escrowing the 40% gauge bucket until the cap is enforced — pre-launch, the cap-bound defense above is not yet active, and the per-cycle margin is +32c (no cap), turning the cheap-TOKEN sybil expansion into a net-positive attack.

## Why the launch prerequisite is contract-pinned

[ADR 026 §2 Pre-launch gauge accumulation](026-gauge-boost-tokenomics.md#pre-launch-gauge-accumulation) pins `gaugeLaunched == false` as a contract-level state: while false, the 40% gauge bucket is escrowed per epoch and the one-shot `enableGauge()` setter is the only path to live gauge payouts. The off-chain prerequisite for calling `enableGauge()` is the analysis in this appendix: an auditor verifying the launch checklist can confirm

- the cap (`MAX_GAUGE_SHARE_PER_OPERATOR`) is set within the `[1%, 25%]` bound,
- the cap is actively enforced in the gauge formula at the contract layer,
- the FeeRouter shares match the assumption table above, so the 8c leakage figure holds,

and conclude that the per-cycle wash-trade margin is net-negative at any cap setting that admits production operations. Without the cap, the per-cycle margin is +32c (the "without that cap" condition in [ADR 026 §2](026-gauge-boost-tokenomics.md#pre-launch-gauge-accumulation)); pre-launch escrow holds the bucket out of reach until the cap is wired in.

## Residual risk

The cap does not defend against:

- **Real-traffic operator concentration.** A legitimate operator with a dominant byte share is also subject to the cap. This is the intended posture — the gauge pool exists to incentivize a diverse operator set, not to reward concentration. The cap binds in either direction; the trade-off is recorded in [ADR 026 §3 Per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap).
- **Cross-operator collusion at scale.** Two or more operators colluding to wash-trade across their respective identities can stay under the per-operator cap each while jointly exceeding the breakeven share. The on-chain defense is the same per-operator cap (which still binds each colluder individually). The supplementary defense is reputation-layer cluster detection ([ADR 008 §12](008-reputation.md#12-gauge-pool-wash-trading-reputation-as-off-chain-signal)) and governance input for cap-tuning ([ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds)) — if persistent cluster patterns surface, governance can tighten the cap toward the 1% lower bound to reduce the per-collusion-ring extractable gauge share.
- **`FeeRouter` share drift.** The 8c leakage figure depends on the treasury (5%) + safety (3%) shares summing to 8%. Governance can update these within `[0%, 20%]` and `[0%, 15%]` respectively per [ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds), subject to the sum-to-100% invariant. If governance flattens both to 0%, the wash-trade leakage drops to whatever the remaining non-base, non-gauge buckets contribute (burn + delegator, with the wash-trader's recoverability arguments above). The sum-to-100% invariant prevents leakage from going negative, but the *minimum-leakage* configuration is a governance attack surface separate from the per-operator cap.

The first item is by design. The second and third are bounded by the supplementary mechanisms named and remain governance-tunable; this appendix's analysis is conditional on the current `FeeRouter` shares and the cap being enforced.

## References

- [ADR 008 § Reputation as off-chain wash-trading signal](008-reputation.md#12-gauge-pool-wash-trading-reputation-as-off-chain-signal) — soft layer
- [ADR 008 § Distinct-counterparty discount](008-reputation.md#41-distinct-counterparty-discount) — sybil-extension reputation discount
- [ADR 026 § Pre-launch gauge accumulation](026-gauge-boost-tokenomics.md#pre-launch-gauge-accumulation) — launch prerequisite
- [ADR 026 § Per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) — cap rationale
- [ADR 026 § FeeRouter split](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) — bucket shares used in the leakage derivation
- [ADR 026 § Operator economics and minimum stake](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake) — sybil-expansion capital cost
- [ADR 026 § Governable parameters with safety bounds](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds) — cap and share bounds
