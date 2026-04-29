# ADR 029: Adaptive FeeRouter Parameters

**Date:** 2026-04-25
**Status:** Draft (deferred — adopt after governance dynamics observable post-v1)
**Driver:** [ADR 026](026-gauge-boost-tokenomics.md) §Decision §11 (governable parameters)
**Touches:** [ADR 026](026-gauge-boost-tokenomics.md), [ADR 009](009-governance.md), [ADR 018](018-liquidity-strategy.md)

---

## Context

[ADR 026](026-gauge-boost-tokenomics.md) §11 makes the six `FeeRouter` shares and the
`boostFloor` governable within hard-coded safety bounds. Every change is a
governance proposal: 7-day vote + 48-hour timelock minimum, per
[ADR 009](009-governance.md). That cadence is appropriate for structural
re-balancing (changing what the protocol *is*) but too slow for two
state-driven feedback regimes that the v3 design surfaces but does not solve:

1. **Lock-rate scenarios.** The gauge boost in [ADR 026](026-gauge-boost-tokenomics.md) §3
   only does its job at a healthy ve-lock rate. If `ve_locked / total_supply`
   sits **below ~15%**, gauge-boost has too few committed lockers to
   differentiate from the commodity floor, the system is dilutive to delegators,
   and the operator-side TOKEN demand loop ([ADR 026](026-gauge-boost-tokenomics.md)
   §Consequences) under-fires. If it sits **above ~50%**, the system is
   over-locked: each marginal locker captures a smaller boost share, the
   delegator pool is over-rewarding a saturated population, and treasury under-
   accrues relative to its useful work.

2. **Price-floor scenarios.** [ADR 018](018-liquidity-strategy.md) buyback
   pressure is fixed at 5% of routed USDC ([ADR 026](026-gauge-boost-tokenomics.md) §2,
   §8). Under sustained TOKEN drawdown that flow is invariant to the drawdown —
   the protocol does not lean into the buyback when buyback is most useful. A
   30-day TWAP price floor is a cheap signal for "the deflationary lever should
   be heavier right now."

In both cases, manual governance is too slow. Both are bounded, well-defined
state queries on contracts the protocol already operates: `VotingEscrow` for
lock rate, the Balancer V3 80/20 pool for TWAP price (the same source
[ADR 018](018-liquidity-strategy.md) already trusts for buyback MEV defense).

This ADR specifies two automated feedback hooks within the
[ADR 026](026-gauge-boost-tokenomics.md) §11 safety bounds. The hooks never violate
those bounds; if a shift would, the shift is clamped. Governance retains an
absolute veto and can disable either hook at any time.

**Deferment.** [ADR 026](026-gauge-boost-tokenomics.md) §Forward references lists this
ADR as deferred. The recommendation here is to **author and merge the spec but
not deploy** — adoption waits until v1 governance dynamics are observable. If
governance rebalancing turns out to be fast enough in practice, the hooks may
not be needed at all and this ADR closes as Rejected. See §Deferment rationale
below.

---

## Decision

Two automated feedback hooks are added to a new `AdaptiveFeeRouterController`
(or, equivalently, methods on `FeeRouter` itself — implementation choice
deferred). Both are evaluated at epoch rollover (1 week, per
[ADR 026](026-gauge-boost-tokenomics.md) §2 epoch mechanics). Both are bounded by
[ADR 026](026-gauge-boost-tokenomics.md) §11. Neither replaces governance — both are
clamped, observable, and disable-able.

### 1. Lock-rate feedback hook

**Read source.** The underlying TOKEN balance locked in `VotingEscrow` —
canonically `TOKEN.balanceOf(address(VotingEscrow))` — and the live TOKEN supply
counter. No oracle. The lock rate is computed as
`token_locked_underlying / token_circulating_supply`, where the numerator is
underlying TOKEN held by the escrow contract, **not** the time-weighted
ve-supply from `VotingEscrow.totalSupplyAt(...)`. The two have different units;
using ve-supply would mis-fire the 15% / 50% thresholds because ve-balance
decays linearly to lock expiry while the threshold is intended to track "% of
circulating TOKEN that is currently committed". The same underlying-locked
definition is exposed as the `decdn_ve_lock_rate` metric per [ADR 020](020-observability.md#210-tokenomics-v3-metrics).
(Implementation note: the denominator is the live TOKEN total supply minus
burned and minus locked contract reserves — fully on-chain, no off-chain feed.)

**Action.** At epoch rollover, the controller reads the lock rate and applies
the table below to the next epoch's router shares:

| Lock rate window | Next-epoch shift | Direction |
| --- | --- | --- |
| `< 15%` | +2 pp | from `treasury` → `delegator` |
| `15% ≤ rate ≤ 50%` | none | (steady state) |
| `> 50%` | +2 pp | from `delegator` → `treasury` |

**Rationale.** Below 15%, the system needs a stronger ve-locker incentive — the
delegator pool's TOKEN-denominated yield is the cleanest "lock more TOKEN"
signal. Above 50%, the system is over-locked and the marginal pp is more useful
in treasury (working capital, ecosystem grants, runway) than in further
incentivising lockers who are already saturated. The thresholds (15% / 50%)
were the recommendation in the source design spec §2.7 and the survival-
additions §6; production tuning is governance-adjustable per §3 below.

**Hysteresis.** To prevent flipping when the lock rate sits exactly at a
threshold:

- The 15% activation requires `rate < 15%`; the deactivation (revert the +2 pp
  to delegator) requires `rate ≥ 17%`. Two-percentage-point band.
- The 50% activation requires `rate > 50%`; the deactivation requires
  `rate ≤ 48%`. Two-percentage-point band.

**Clamping.** Per [ADR 026](026-gauge-boost-tokenomics.md) §11, `treasury` is bounded
`[0%, 20%]` and `delegator` is bounded `[0%, 30%]`. If a +2 pp shift would push
either share outside its bound, the shift is reduced to whatever fits inside
the bound (down to and including 0 pp). The controller emits an event when a
shift is clamped so observers can detect saturation against the safety bounds.

### 2. Price-floor feedback hook

**Read source.** 30-day TWAP of TOKEN against USDC on the Balancer V3 80/20
pool already used for buyback ([ADR 018](018-liquidity-strategy.md)). No new
oracle infrastructure; the TWAP window is identical to the burn-side guard
window. Cross-reference [ADR 018](018-liquidity-strategy.md) §"Buyback execution
via Balancer V3" for the per-epoch liquidity cap and `subSwapMinBlockGap`
machinery this hook reuses.

**Action.** At epoch rollover, the controller reads TWAP. If TWAP is below the
governance-set `priceFloor` (default `$0.01`, governable; see §3), the
controller shifts +2 pp from `treasury` → `burn` for the next epoch.

| TWAP state | Next-epoch shift | Direction |
| --- | --- | --- |
| `TWAP < priceFloor` | +2 pp | from `treasury` → `burn` |
| `TWAP ≥ priceFloor + hysteresis` | revert (shift back) | from `burn` → `treasury` |
| in-between | no change | (state held from prior epoch) |

**Hysteresis.** Default hysteresis = `priceFloor × 10%` (i.e., at default floor
$0.01, the revert threshold is $0.011). Governance-adjustable per §3. The
explicit hysteresis prevents single-epoch flicker around the floor and makes
the hook trivially auditable from on-chain state.

**Clamping.** Per [ADR 026](026-gauge-boost-tokenomics.md) §11, `treasury` is bounded
`[0%, 20%]` and `burn` is bounded `[0%, 25%]`. The +2 pp shift is clamped to
whatever fits inside both bounds simultaneously; if either bound binds, the
shift reduces accordingly (down to 0 pp). Clamping events are emitted.

**TWAP-manipulation defenses.** A 30-day TWAP is intrinsically expensive to
manipulate but not free. Two additional defenses, both reusing
[ADR 018](018-liquidity-strategy.md) infrastructure:

- **Pool-depth cap.** If the Balancer V3 pool's USDC depth at the read moment
  is below a governance-set `minPoolDepth` (default $250K), the price-floor
  hook is **skipped for that epoch**. Thin pools are precisely where TWAP
  manipulation is cheapest; skipping the adaptive shift in that regime is safer
  than firing it.
- **Read-window staleness guard.** The TWAP read MUST cover at least 24 of the
  trailing 30 days of pool data; if the pool was paused or had insufficient
  trade volume to populate the TWAP, the hook is skipped for that epoch (same
  failure mode as a thin pool).

The price-floor hook does not introduce a new oracle; it is a thin consumer of
the Balancer V3 TWAP that [ADR 018](018-liquidity-strategy.md) already relies
on for the buyback path's `minTokenOut` MEV guard. This ADR does not specify
oracle implementation beyond that.

### 3. Hook parameters

The hook parameters themselves are governable, gated by 48-hour timelock per
[ADR 009](009-governance.md), with hard-coded outer safety bounds:

| Parameter | Default | Min | Max | Governable |
| --- | ---: | ---: | ---: | --- |
| Lock-rate low threshold | 15% | 5% | 25% | Yes |
| Lock-rate high threshold | 50% | 35% | 75% | Yes |
| Lock-rate hysteresis band | 2 pp | 1 pp | 5 pp | Yes |
| Lock-rate shift size | 2 pp | 1 pp | 5 pp | Yes |
| `priceFloor` | $0.01 | $0.001 | $1.00 | Yes |
| Price hysteresis (fraction of floor) | 10% | 1% | 25% | Yes |
| Price-shift size | 2 pp | 1 pp | 5 pp | Yes |
| `minPoolDepth` (pool-depth cap) | $250K | $50K | $5M | Yes |
| Lock-rate hook enabled | true | — | — | Yes (boolean) |
| Price-floor hook enabled | true | — | — | Yes (boolean) |

The shift-size cap of 5 pp prevents the hooks from making outsized changes
between epochs (multiple successive +5 pp shifts can still re-shape the split,
but each individual hook fire is bounded). The 75% upper limit on the high
threshold ensures the hook cannot be silently disabled by setting the upper
band beyond achievable lock rates.

### 4. Trigger cadence

Both hooks evaluate exactly once per epoch, at epoch rollover (1 week,
[ADR 026](026-gauge-boost-tokenomics.md) §2.4). The controller is keeper-triggered (the
same keeper class that already drives `BuybackBurner` and the delegator-pool
swap path per [ADR 018](018-liquidity-strategy.md) is sufficient — no new
keeper role). If the keeper fails to fire, the hook is simply skipped for that
epoch; the prior epoch's shares carry forward. There is no catch-up: missed
fires do not retroactively apply.

### 5. Governance interaction (vote always wins)

The adaptive hooks operate **strictly within** the bounds set by
[ADR 026](026-gauge-boost-tokenomics.md) §11. Governance retains all of:

| Override | Mechanism | Effect |
| --- | --- | --- |
| Disable a single hook | Governance vote on `setHookEnabled(hook, false)` | Hook's epoch evaluation becomes a no-op until re-enabled |
| Adjust hook parameters | Governance vote (table in §3) within outer bounds | Next-epoch evaluation uses new parameters |
| Override an adaptive shift | Governance vote on the underlying `FeeRouter` shares | Vote always wins; subsequent hook fires evaluate against the new baseline |
| Pause both hooks (emergency) | Emergency-multisig pause per [ADR 009](009-governance.md) | Hooks halted under the standard pause-deadline sunset |
| Disable both hooks permanently | Governance vote on `setControllerEnabled(false)` | Controller becomes a no-op; underlying `FeeRouter` reverts to manual governance only |

Two consequences of this design worth surfacing:

- **The adaptive hook never violates the §11 safety bounds.** If a shift
  would, it is clamped to the bound (potentially down to a 0-pp no-op). The
  outer governance safety envelope is unchanged by this ADR.
- **The vote always wins.** A direct governance update to the underlying
  `FeeRouter` shares takes effect in the next-epoch settlement. The next hook
  evaluation reads the new state and decides whether to apply a shift on top.
  There is no path where the adaptive hook can "undo" a governance decision
  within a single epoch — they compose by evaluation order, not by override.

### 6. Implementation

A new `AdaptiveFeeRouterController` contract reads `VotingEscrow` and the
Balancer V3 TWAP, computes the per-epoch shifts, and applies them via a
privileged path on `FeeRouter` (`applyAdaptiveShift(deltaTreasuryToDelegator,
deltaTreasuryToBurn)`). Equivalent: methods on `FeeRouter` itself — choice
deferred to implementation review. Either way:

- The privileged path checks both proposed shifts against §11 bounds and
  reverts if the resulting shares would violate a bound (the controller is
  expected to clamp first, but the contract enforces the invariant).
- Every shift (including a clamped 0-pp no-op caused by hysteresis or bound
  saturation) emits an event: `AdaptiveShift(epoch, hook, requestedDelta,
  appliedDelta, reason)`. Observers MUST be able to reconstruct the shift
  history from on-chain logs alone.
- The sum-to-100% invariant on the six router shares is preserved: every
  applied shift moves N pp from one bucket to another, leaving the sum
  unchanged.
- The price-floor read uses the same Balancer V3 oracle helper the
  [ADR 018](018-liquidity-strategy.md) buyback path uses for `minTokenOut`;
  no new oracle code, no new audit surface for the price feed itself.

### 7. Deferment rationale

This ADR is **Draft, deferred** for three reasons:

1. **Governance dynamics aren't observable yet.** The cadence question — "is
   manual governance fast enough?" — has no pre-launch answer. Curve and
   similar protocols ship adaptive hooks because their governance is provably
   slow at scale; deCDN does not yet have data on its own timelock-vs-state
   gap.
2. **Adaptive logic introduces a new code path that can mis-fire.** TWAP
   manipulation under thin liquidity, edge cases at threshold boundaries, and
   keeper-failure regressions all require post-mainnet observation. Adding
   them at v1 means auditing a code path the protocol may never need.
3. **The §11 safety bounds are sufficient as a hard floor.** Even without
   adaptive feedback, governance can rebalance within bounds via the standard
   timelock path. The cost of "governance is slow" is recoverable; the cost of
   "adaptive logic mis-fires under stress" is operationally noisy and harder
   to roll back inside a single epoch.

If, six months post-mainnet, ve-lock rate or TWAP excursions measurably
out-pace governance response, this ADR moves from Draft (deferred) to Accepted
and the controller is deployed. If not, it closes as Rejected with the
[ADR 026](026-gauge-boost-tokenomics.md) §11 bounds + manual governance loop providing
sufficient response surface.

---

## Consequences

### Positive

- **Faster response inside the §11 envelope.** Lock-rate excursions and
  drawdown regimes get a 1-epoch (1-week) automatic response instead of a
  multi-week timelocked vote, without bypassing any safety bound.
- **Reuses existing infrastructure.** No new oracle, no new keeper class, no
  new external trust — the lock-rate read is on-chain native; the TWAP read
  uses the same Balancer V3 pool [ADR 018](018-liquidity-strategy.md) already
  trusts.
- **Preserves governance veto.** Per §5, every adaptive shift can be disabled,
  re-parameterised, or overridden by direct vote. The hooks compose with
  manual governance; they do not displace it.
- **Tight bound discipline.** Every shift is clamped to the §11 bounds at the
  contract layer; the adaptive hook cannot enlarge governance's reachable
  state space, only respond inside it.
- **Auditable.** Every shift (including no-op clamps and skips for thin-pool /
  staleness guards) emits an event. Hook behaviour is reconstructable from
  logs alone.

### Negative

- **Adds contract surface.** `AdaptiveFeeRouterController` (or equivalent
  methods on `FeeRouter`) is new code. Audit burden on top of
  [ADR 026](026-gauge-boost-tokenomics.md)'s already-expanded surface.
- **Adds keeper responsibility.** The same keeper that drives buyback +
  delegator swap also fires the controller. A new failure mode (controller
  fires but cannot read TWAP; controller fires under thin pool depth) joins
  the existing keeper-failure surface.
- **Threshold parameters are reasoned defaults.** The 15% / 50% lock-rate
  bands and the $0.01 price floor are guesses informed by the source design
  spec, not data. Production tuning will likely be needed within the §3 outer
  bounds.
- **Hysteresis adds state.** The controller maintains a small state machine
  (which side of which threshold each hook last triggered on) so the revert
  semantics work. Minor but non-zero added storage and complexity.
- **The "vote always wins" semantics need clear UI.** Operators and lockers
  need a dashboard surface showing both the manual share state and the
  active adaptive shift; otherwise the difference between "governance set
  the share" and "the hook shifted it for this epoch" is invisible to users.

### Risks

- **TWAP manipulation under thin pool depth.** A 30-day TWAP is expensive but
  not free to manipulate, and a price-floor hook is a clear attack target if
  pool depth is thin. The pool-depth cap (§2) skips the hook in exactly that
  regime, but the calibration of `minPoolDepth` (default $250K) is a
  reasoned guess. Mitigation: per-epoch liquidity cap from
  [ADR 018](018-liquidity-strategy.md) bounds the maximum shift's market impact,
  and the staleness guard skips the hook on suspicious read windows.
  Residual risk: a sophisticated adversary trading at the floor over weeks to
  cheaply move the TWAP. Acceptable post-launch monitoring target; explicit
  reason for the deferment.
- **Governance-bypass perception.** Even though §5 preserves the veto, the
  hooks shift parameters without per-event vote. Sophisticated holders may
  perceive this as governance dilution. Mitigation: explicit `setHookEnabled`
  toggle, explicit clamping events, and a standing recommendation that the
  hooks be **disabled by default at deployment** and turned on by an explicit
  governance vote once dynamics are observable.
- **Mis-fire under stress.** The most failure-prone window is exactly when
  the hooks should be most useful: lock-rate at 14.9%, TWAP at $0.0099. Hook
  stutter at thresholds, keeper drift, oracle staleness during an active
  drawdown. The hysteresis band, the staleness guard, and the pool-depth cap
  are designed to fail-safe (no-op) in these cases — the hook does nothing
  rather than shift in the wrong direction. Confirm under chaos-test before
  enabling in production.
- **Composition surprise.** If governance moves shares manually in the same
  epoch the controller fires, the adaptive shift composes on top (per §5).
  The contract enforces sum-to-100% and per-share bounds, but the interaction
  may surprise observers. Mitigation: `AdaptiveShift` events carry the
  pre-shift baseline so observers can always reconstruct what the shift saw.
- **Deferred adoption may not happen.** If post-launch governance is fast
  enough, this ADR closes as Rejected. The drafting cost is small; the
  alternative (shipping adaptive hooks at v1 without observability) is worse.

---

## Forward references

This ADR depends on no follow-up ADRs. Its dependencies — `FeeRouter`,
`VotingEscrow`, the Balancer V3 80/20 pool, the §11 safety bounds — are all
defined in [ADR 026](026-gauge-boost-tokenomics.md), [ADR 018](018-liquidity-strategy.md),
and [ADR 009](009-governance.md). The keeper class is the existing buyback
keeper from [ADR 018](018-liquidity-strategy.md).

If adopted, this ADR adds a new contract (`AdaptiveFeeRouterController`) or a
set of methods on `FeeRouter`; either way [ADR 016](016-contract-interactions.md)
is updated to register the new surface, and [ADR 020](020-observability.md) is
updated to expose the new metrics (lock rate, TWAP read, last-shift event,
clamp counters). Both updates are deferred until this ADR moves out of
deferred status.
