# Appendix: Wash-Trading Economics

> **This is an appendix, not a core protocol ADR.** It records the public-derivable analysis backing the launch prerequisite in [ADR 026 §2 Pre-launch gauge accumulation](026-gauge-boost-tokenomics.md#pre-launch-gauge-accumulation) and the per-operator gauge-share cap rationale in [ADR 026 §3 Per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap). The full equilibrium derivation — covering the ve-boost-amplified single-operator breakeven and the multi-epoch reputation interaction — lives in the source design spec referenced at the head of ADR 026; this appendix derives the auditable bounds and identifies which structural mechanism does the load-bearing work.

## Context

[ADR 026 §3](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) distributes 40% of `FeeRouter` revenue via a Curve-style gauge formula whose input is per-operator `bytes_delivered`. Without a structural bound, a ve-locked operator can inflate `bytes_delivered` by routing self-funded settlements through self-controlled clients — paying USDC from one wallet they control into a channel funded by another wallet they control, settling the channel through `PaymentChannel.settleChannel`, and capturing the resulting share of the 40% gauge bucket. This appendix derives the structural defenses against that attack and shows why the `MAX_GAUGE_SHARE_PER_OPERATOR` cap (default 5%, governable `[1%, 25%]` per [ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds)) and the per-pseudo-operator capital lockup from [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake) are the binding on-chain mechanisms.

The reputation-layer distinct-counterparty discount ([ADR 008 §4.1](008-reputation.md#41-distinct-counterparty-discount)) and the reputation-as-off-chain-signal mechanism ([ADR 008 §12](008-reputation.md#12-gauge-pool-wash-trading-reputation-as-off-chain-signal)) are the soft layer; this appendix is concerned with the on-chain defenses because they are the mechanisms enforced without external attestation.

## Assumptions and simplifications

The derivation uses parameters canonical in the ADR set. **The numerical conclusions in this appendix are conditional on the steady-state `FeeRouter` shares from [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553); launch-time shares may differ per the §2 staged-launch story, and any future `setShares` proposal that materially changes the table below requires the analysis to be re-derived for the new shares.**

| Quantity | Steady-state value | Source |
| --- | --- | --- |
| `FeeRouter` node-base share | 40% | [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) |
| `FeeRouter` gauge-boost share | 40% | [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) |
| `FeeRouter` delegator share | 7% | [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) |
| `FeeRouter` burn share | 5% | [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) |
| `FeeRouter` treasury share | 5% | [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) |
| `FeeRouter` safety-reserve share | 3% | [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) |
| `MAX_GAUGE_SHARE_PER_OPERATOR` default | 5% | [ADR 026 §3](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) |
| `MAX_GAUGE_SHARE_PER_OPERATOR` governance bounds | `[1%, 25%]` | [ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds) |
| Minimum operator stake (default) | 50,000 TOKEN | [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake) |
| `StakingRegistry.unbondingPeriod` (default) | 7 days | [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake), bounded `[3 days, 30 days]` per [ADR 009](009-governance.md#governable-parameters-with-safety-bounds) |
| TOKEN supply | 1,000,000,000 | [ADR 026 §1 Supply and distribution](026-gauge-boost-tokenomics.md#1-supply-and-distribution) |

**Wash-trader posture.** A worst-case wash-trader is a registered operator (50,000 TOKEN staked per pseudo-operator) and a ve-locker (some TOKEN locked in `VotingEscrow` to capture the working_bytes ve-boost). Both sides of the channel — the paying client wallet and the receiving node wallet — are controlled by the same party, so the `$X` principal of any wash-trade is internal to the wash-trader's balance sheet; only the portion that escapes via `FeeRouter` to non-controlled addresses is a real cost.

**Simplifying assumptions.** This appendix uses three simplifications to make the structural bounds auditable from public inputs: (1) uniform per-byte fees (so `byte share = revenue share`); (2) `total_ve > 0` so the ve-boost formula is well-defined; (3) the wash-trader is a single party who may operate one or more pseudo-operators but does not collude with independent operators (collusion is handled separately under Residual Risks). The closed-pool dilution argument below is exact under these assumptions; the ve-boost-amplified per-cycle breakeven depends on the full distribution of ve-balances across all operators and is left to the source design spec.

## Per-cycle leakage: the closed-pool argument

Consider a wash-trader with byte share `s = X_wash / X_total` of the network's working_bytes in some epoch, where `X_total = X_honest + X_wash`. Their per-`$1`-wash-traded cash flow through `FeeRouter` decomposes as:

- **Node-base (40%).** The wash-trader is the receiving node, so the `$0.40` base share returns to them in the same transaction. Net: contributes `$0.40` to recovery, exactly offsetting the principal they paid on the node-base leg.
- **Gauge (40%).** The wash-trader's slice of the gauge bucket is `min(s, cap) × $0.40 × X_total` in absolute terms per epoch, which per `$1` of wash-trade volume equals `min(s, cap) × $0.40 × (X_total / X_wash) = (min(s, cap) / s) × $0.40`. Critically, the wash-trader contributed `$0.40` of every wash-traded `$1` to the bucket they're claiming from:
  - **If `s ≤ cap` (cap doesn't bind):** slice per `$1` = `(s / s) × $0.40 = $0.40` — the wash-trader recovers exactly what they contributed. *Net gauge revenue = 0.*
  - **If `s > cap` (cap binds):** slice per `$1` = `(cap / s) × $0.40 < $0.40`. *Net gauge revenue is negative* (the wash-trader contributes more than they recover from gauge).
- **Treasury (5%).** Direct same-tx to the treasury wallet. The wash-trader does not control treasury disbursements. Non-recoverable. Cost: `$0.05 per $1`.
- **Safety-reserve (3%).** Direct same-tx to `SafetyReserve`. The wash-trader does not control reserve disbursements except as a potential payout recipient under [ADR 026 §5](026-gauge-boost-tokenomics.md#5-safety-and-insurance-reserve-3-bucket) (a far-future contingent claim). Non-recoverable in expectation. Cost: `$0.03 per $1`.
- **Burn (5%).** USDC is market-bought into TOKEN via Balancer V3 ([ADR 018](018-liquidity-strategy.md)) and burned. The wash-trader benefits in two coupled ways — a market-buy push on TOKEN price and a supply-reduction effect on the TOKEN they already hold — but the *direct* recovery is bounded above by the wash-trader's pro-rata share of total TOKEN supply. For a wash-trader at the 50,000-TOKEN minimum stake out of 1,000,000,000 TOKEN supply, that share is `0.005%` per pseudo-operator (scaling roughly linearly with sybil count, still negligible at any realistic attack scale). The price-impact dimension is held outside the appendix's per-cycle accounting because it depends on Balancer pool depth and is not load-bearing for the structural bounds derived below. Effective cost: approximately `$0.05 per $1`, treated as leakage.
- **Delegator (7%).** USDC is swapped to TOKEN via TWAP and distributed pro-rata by ve-balance. Recoverable only to the extent the wash-trader holds ve: recovery rate is `ve_attacker / total_ve`. For a wash-trader who is not a major ve-locker, recovery is small; conservative direction is to treat as leakage. Effective cost: up to `$0.07 per $1`.

**Per-cycle minimum non-recoverable leakage: `$0.08 per $1` (treasury + safety).** This is the floor — the amount that is unambiguously a real cost no matter how the wash-trader is positioned in TOKEN supply and ve-balance.

**Per-cycle conservative leakage (treating burn and delegator as ≈ leakage): up to `$0.20 per $1`.**

**Combining the gauge and leakage legs** under the simplifying assumptions above, the wash-trader's expected per-`$1`-wash-traded payoff is:

- **`s ≤ cap`:** net = `0` (gauge) − leakage = between `−$0.08` (minimum) and `−$0.20` (conservative). **Wash-trading is structurally net-negative when the cap does not bind, regardless of the cap value.**
- **`s > cap`:** net gauge revenue is *negative* (the wash-trader is over-contributing to the bucket relative to their slice), and leakage stacks on top. **Strictly worse than the `s ≤ cap` case.**

**The conclusion under uniform ve-share assumptions: single-operator wash-trading is net-negative at any cap value the protocol could plausibly configure.** The cap is not the binding mechanism in this regime; the closed-pool dilution is.

## What the cap actually defends against: ve-boost amplification

The closed-pool argument above assumes the wash-trader's gauge slice is proportional to their byte share. The actual formula from [ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula) is:

```
working_bytes_i = min(bytes_i, 0.4·bytes_i + 0.6·(ve_i / total_ve) · total_bytes)
```

with the per-operator pool share = `min(working_bytes_i / sum(working_bytes), MAX_GAUGE_SHARE_PER_OPERATOR)`. The `min(...)` on the inside binds against `bytes_i` for any operator whose ve-share `ve_i / total_ve` is large enough that the second term meets or exceeds `bytes_i`; for an honest operator without ve-locks, the inside-min instead binds at `0.4·bytes_i`. This creates a structural asymmetry:

- A ve-locked wash-trader contributes `bytes_i` to the working_bytes denominator (the inside-min binds on `bytes_i`).
- A non-ve honest operator with the same raw byte count contributes `0.4 · bytes_i`.

If the honest set is uniformly non-ve, the wash-trader's *effective* share `s_eff = working_bytes_wash / sum(working_bytes)` can exceed their *raw* byte share `s_raw = bytes_wash / sum(bytes)` by a factor up to `1 / 0.4 = 2.5×`. Concretely, a wash-trader with 10% raw byte share against a fully-non-ve honest set has `s_eff ≈ 10% / (10% + 0.4·90%) ≈ 21.7%`.

This asymmetry breaks the closed-pool result: the ve-locked wash-trader's slice can exceed their contribution. **Without the cap, the per-cycle gauge revenue net of contribution can be positive, and the per-cycle payoff can flip net-positive against the 8¢ leakage floor.** The full breakeven depends on the actual ve-distribution across operators (which the appendix can't pin without the source design spec's ve-equilibrium model), but the qualitative claim is robust: without the cap, ve-boost amplification is a viable wash-trading route.

**With the cap at `cap = 5%`, the wash-trader's slice is bounded at `cap × $0.40 × X_total` regardless of `s_eff`.** Working through the per-`$1`-wash arithmetic with the cap binding:

- Slice per `$1` = `(cap / s_raw) × $0.40` (the wash-trader's actual contribution to the bucket is `0.40 · X_wash`, so per-`$1`-wash slice equals `cap × $0.40 × (X_total / X_wash) = cap × $0.40 / s_raw`).
- For the cap to be the binding constraint rather than `s_eff`, the wash-trader's `s_eff > cap` — i.e., without the cap they would have captured more than `cap` of the bucket.
- Per-`$1` net (vs `$0.08` minimum leakage): `(cap / s_raw) × $0.40 − $0.40 − $0.08` (slice − own gauge contribution − leakage). For a single-operator wash-trader with `s_raw ≥ cap`, this is bounded above by `−$0.08` and degrades as `s_raw` grows.

The cap therefore caps the per-operator gauge slice at `cap × $0.40` per `$1` of *gauge bucket*, equivalently `(cap / s_raw) × $0.40` per `$1` of *wash trade*. The table below shows the maximum per-operator gauge capture at each cap setting — the upper bound on what a single concentrated wash-trader can extract from the bucket regardless of their ve-position:

| Cap value | Max per-operator gauge capture | Above 8¢ floor? |
| ---: | ---: | --- |
| No cap | up to `$0.40` per `$1` of gauge bucket | yes — leakage floor does not bind defense |
| 25% (governance upper bound) | `$0.10` per `$1` of gauge bucket | yes — thinly above floor; see Residual Risks |
| 5% (default) | `$0.02` per `$1` of gauge bucket | no — capped slice is below leakage floor |
| 1% (governance lower bound) | `$0.004` per `$1` of gauge bucket | no — capped slice well below floor |

The "above 8¢ floor?" column reads as: can the cap-bound slice exceed the minimum non-recoverable leakage *if the wash-trader could costlessly amplify their share via ve-boost*? At the 5% default the answer is no — the cap closes off the ve-boost amplification route below the leakage floor. At the 25% upper bound the answer is yes, with a thin `$0.02` margin remaining negative *only* because of the leakage floor; this admits a configuration where, if leakage shifts (e.g., governance lowers treasury+safety shares within their `[0%, 20%]` / `[0%, 15%]` bounds — see Residual Risks), wash-trading could become thinly profitable. The governance lower bound of 1% is included so the cap remains non-zero (a `cap = 0` setting would disable gauge claims for honest operators entirely — a denial-of-service, not a defense).

## Sybil expansion: the binding defense at multi-operator scale

The cap is *per-operator-identity*. A wash-trader can attempt to evade it by splitting their wash-trade across `N` pseudo-operators, each below the cap individually. **In this regime the per-operator cap stops binding** — every pseudo-operator has `s_i < cap`, so each falls back to the closed-pool result (`net gauge revenue = 0` under uniform ve-share, modestly positive only if each individual pseudo-operator is ve-amplified against the honest set). The structural defense at multi-operator scale is not the cap itself but the per-pseudo-operator capital lockup:

1. **Per-pseudo-operator staking cost.** Each pseudo-operator requires its own [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake) minimum stake of 50,000 TOKEN locked in `StakingRegistry`. The unbonding period (3–30 days governance bound, 7-day default per ADR 026 §7) bounds the minimum lockup duration. For `N` pseudo-operators, total capital lockup is `N × 50,000` TOKEN — `5M TOKEN` at 100-operator scale, `50M TOKEN` at 1,000-operator scale.

2. **Per-counterparty diversity discount on reputation.** [ADR 008 §4.1](008-reputation.md#41-distinct-counterparty-discount) imposes a `diversity_factor` of `min(distinct_counterparties / 5, 1)` on reported settled value. A wash-trader cycling funds among `N` pseudo-operators each settles with the same small counterparty set; below 5 distinct counterparties per reporter the `diversity_factor` shrinks the reputation signal by up to 80%. This does not directly affect on-chain gauge-share (the cap binds independently of reputation), but it limits the wash-trader's ability to convert the synthetic settlement history into reputation-weighted protocol standing (node selection, gauge-eligibility tier signaling) that compounds with the gauge-share extraction.

Combined: a wash-trader operating at scale converts the attack from a heuristic-bypass into a **stake-proportional capital-lockup attack**. Marginal cost scales linearly with operator count and is denominated in TOKEN at the time of staking; the per-cycle USDC margin per pseudo-operator stays bounded by the closed-pool result (≤ 0 under uniform-ve-share, modestly positive only via ve-boost asymmetry against a non-ve honest set).

**TOKEN-price interaction.** Capital lockup is denominated in TOKEN; per-pseudo-operator USDC margin is denominated in USDC and bounded structurally by `cap × $0.40` per `$1` of bucket. The dollar cost of running `N` pseudo-operators is `N × 50,000 × p_TOKEN`. At genesis TOKEN price `$0.20`, that is `$10,000` per pseudo-operator; at `$0.05` (a low-TOKEN-price scenario), `$2,500`. **Sybil expansion is cheapest at genesis-low TOKEN prices** — the same window the [ADR 026 §2 Pre-launch gauge accumulation](026-gauge-boost-tokenomics.md#pre-launch-gauge-accumulation) launch prerequisite addresses by escrowing the 40% gauge bucket until the cap is enforced. At elevated TOKEN prices (3–5× genesis), the same `N` pseudo-operators cost 3–5× more in dollar terms, raising the breakeven volume the wash-trader must push through each pseudo-operator before the capital lockup is amortized.

## Why the launch prerequisite is contract-pinned

[ADR 026 §2 Pre-launch gauge accumulation](026-gauge-boost-tokenomics.md#pre-launch-gauge-accumulation) pins `gaugeLaunched == false` as a contract-level state: while false, the 40% gauge bucket is escrowed per epoch and the one-shot `enableGauge()` setter is the only path to live gauge payouts. The off-chain prerequisite for calling `enableGauge()` is the analysis in this appendix: an auditor verifying the launch checklist can confirm

- the `FeeRouter` shares match the steady-state assumption table above (or, if shares differ at launch, that the launch-share derivation has been re-done for the active configuration),
- `MAX_GAUGE_SHARE_PER_OPERATOR` is set within the `[1%, 25%]` bound,
- the cap is actively enforced in the gauge formula at the contract layer (i.e., the `min(s_i, cap)` clamp is wired in, not just declared),
- the `StakingRegistry.minStake` is set at or above 50,000 TOKEN so per-pseudo-operator capital lockup binds,

and conclude that single-operator wash-trading is net-negative at any plausible ve-boost amplification and multi-operator wash-trading is bounded by capital lockup. Without the cap, the ve-boost-amplified per-cycle margin can flip positive against the leakage floor; pre-launch escrow holds the bucket out of reach until the cap is wired in.

## Residual risks

The cap + capital lockup combination does not defend against:

- **Real-traffic operator concentration.** A legitimate operator with a dominant byte share is also subject to the cap. This is the intended posture — the gauge pool exists to incentivize a diverse operator set, not to reward concentration. The cap binds in either direction; trade-off recorded in [ADR 026 §3 Per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap).
- **Cross-operator collusion at scale.** A coalition of `⌈cap_breakeven / cap⌉` or more independent operators colluding can stay under the per-operator cap each while jointly exceeding any per-operator-bounded share. At the default 5% cap, **4 or more colluding operators** can jointly exceed 20% of the bucket; at the 1% lower bound, **20 or more colluders** are required. The on-chain defense is the per-operator cap (which still binds each colluder individually); the supplementary defense is reputation-layer cluster detection ([ADR 008 §12](008-reputation.md#12-gauge-pool-wash-trading-reputation-as-off-chain-signal)) and governance input for cap-tuning. If persistent cluster patterns surface, governance can tighten the cap toward the 1% lower bound to reduce the per-collusion-ring extractable share.
- **Cap-near-upper-bound governance setting.** The 25% governance upper bound admits a configuration where the cap-bound per-operator gauge capture (`$0.10` per `$1` of bucket) exceeds the minimum 8¢ leakage floor. If governance also moves treasury or safety shares toward their lower bounds within `[0%, 20%]` / `[0%, 15%]` (see next bullet), wash-trading can become thinly net-positive at the cap upper bound. Mitigated by the 48-hour timelock per [ADR 009](009-governance.md#governable-parameters-with-safety-bounds), the requirement that any cap update is observed before taking effect, and reputation-layer surfacing — but not eliminated by the cap structure alone.
- **`FeeRouter` share drift.** The 8¢ minimum-leakage figure depends on `treasury_share + safety_share = 8%`. Governance can update these within `[0%, 20%]` and `[0%, 15%]` respectively per [ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds), subject to the sum-to-100% invariant. A configuration where both are reduced toward 0% drops the leakage floor toward the burn + delegator residue (recoverable in part for TOKEN-holding ve-locked wash-traders) and weakens the structural defense. Any future `setShares` proposal moving these shares toward 0% requires re-derivation of this appendix's bounds against the new shares.
- **Gauge-share drift.** The 40% gauge bucket size used throughout this derivation is itself governable within `[0%, 60%]` per [ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds). At the 60% upper bound, cap-bound capture grows (cap `× $0.60` per `$1` of bucket); at 0%, the wash-trading attack surface disappears entirely. Same re-derivation requirement as the share-drift case above.
- **Bootstrap `total_ve == 0` window.** During the early-bootstrap window when `total_ve == 0` (no ve-locks yet, per [ADR 026 §1 No auto-ve-lock on vest](026-gauge-boost-tokenomics.md#no-auto-ve-lock-on-vest)), the ve-boost term `0.6·(ve_i / total_ve) · total_bytes` is undefined; the protocol falls back to `working_bytes_i = 0.4·bytes_i` for all operators. In this window the ve-boost amplification route is closed off and the closed-pool result (single-operator wash-trading net-negative) is exact. The launch prerequisite still binds because the cap-enforcement check must hold for the post-bootstrap window when `total_ve > 0` and the amplification route reopens.

The first item is by design. The other items are bounded by the supplementary mechanisms named or remain governance-tunable within explicit bounds; this appendix's quantitative conclusions are conditional on the current `FeeRouter` shares and the cap being enforced.

## References

- [ADR 008 § Reputation as off-chain wash-trading signal](008-reputation.md#12-gauge-pool-wash-trading-reputation-as-off-chain-signal) — soft layer
- [ADR 008 § Distinct-counterparty discount](008-reputation.md#41-distinct-counterparty-discount) — sybil-expansion reputation discount
- [ADR 026 § Pre-launch gauge accumulation](026-gauge-boost-tokenomics.md#pre-launch-gauge-accumulation) — launch prerequisite
- [ADR 026 § Per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) — cap rationale
- [ADR 026 § Operator economics and minimum stake](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake) — sybil-expansion capital cost
- [ADR 026 § Governable parameters with safety bounds](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds) — cap and share bounds
- ADR 026 source design spec (`tokenomics-v2-gauge-boost-design`, 2026-04-18; see ADR 026 frontmatter) — full ve-boost-amplified single-operator breakeven derivation and multi-epoch reputation interaction
