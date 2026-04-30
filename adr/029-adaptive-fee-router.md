# ADR 029: Adaptive FeeRouter Parameters

**Date:** 2026-04-25
**Status:** Draft (deferred — adopt after post-launch governance dynamics observable)
**Driver:** [ADR 026](026-gauge-boost-tokenomics.md) §Decision §11 (governable parameters)
**Touches:** [ADR 026](026-gauge-boost-tokenomics.md), [ADR 009](009-governance.md), [ADR 018](018-liquidity-strategy.md)

---

## Context

[ADR 026](026-gauge-boost-tokenomics.md) §11 makes the six `FeeRouter` shares and the
`boostFloor` governable within hard-coded safety bounds. Every change is a
governance proposal: 7-day vote + 48-hour timelock minimum, per
[ADR 009](009-governance.md). That cadence is appropriate for structural
re-balancing (changing what the protocol *is*) but too slow for two
state-driven feedback regimes the design surfaces but does not solve directly:

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
not deploy** — adoption waits until post-launch governance dynamics are
observable. If governance rebalancing turns out to be fast enough in practice,
the hooks may not be needed at all and this ADR closes as Rejected. See
§Deferment rationale below.

---

## Decision

Two automated feedback hooks are added to a new `AdaptiveFeeRouterController`
(or, equivalently, methods on `FeeRouter` itself — implementation choice
deferred). Both are evaluated at epoch rollover (1 week, per
[ADR 026](026-gauge-boost-tokenomics.md) §2 epoch mechanics). Both are bounded by
[ADR 026](026-gauge-boost-tokenomics.md) §11. Neither replaces governance — both are
clamped, observable, and disable-able.

### 1. Lock-rate feedback hook

Read lock rate as `TOKEN.balanceOf(address(VotingEscrow)) / TOKEN.totalSupply()` (no oracle; matches the canonical `decdn_ve_lock_rate` metric in [ADR 020](020-observability.md) — underlying TOKEN locked, **not** ve-supply from `VotingEscrow.totalSupply()` / `totalSupplyAt(...)`). At epoch rollover:

| Lock rate window | Next-epoch shift | Direction |
| --- | --- | --- |
| `< 15%` | +2 pp | `treasury` → `delegator` |
| `15% ≤ rate ≤ 50%` | none | (steady state) |
| `> 50%` | +2 pp | `delegator` → `treasury` |

Thresholds per design spec §2.7. **Hysteresis:** 2 pp band on each threshold (revert at `≥ 17%` / `≤ 48%`). **Clamping:** shifts that would push `treasury` outside `[0, 20]` or `delegator` outside `[0, 30]` ([ADR 026](026-gauge-boost-tokenomics.md) §11) are reduced to fit (down to 0 pp); clamping events are emitted.

### 2. Price-floor feedback hook

Read 30-day TWAP from the Balancer V3 80/20 pool already trusted by [ADR 018](018-liquidity-strategy.md) (no new oracle).

| TWAP state | Next-epoch shift | Direction |
| --- | --- | --- |
| `TWAP < priceFloor` | +2 pp | `treasury` → `burn` |
| `TWAP ≥ priceFloor + hysteresis` | revert | `burn` → `treasury` |
| in-between | no change | (state held) |

Default `priceFloor = $0.01`, hysteresis = 10% of floor (governable per §3). **Clamping:** as §1, against `treasury [0, 20]` and `burn [0, 25]`. **TWAP-manipulation defenses:** skip the hook for an epoch if pool USDC depth is below `minPoolDepth` (default $250K) or if the TWAP read fails to cover at least 24 of the trailing 30 days. Both reuse [ADR 018](018-liquidity-strategy.md) infrastructure.

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
| Lock-rate hook enabled | false | — | — | Yes (boolean) |
| Price-floor hook enabled | false | — | — | Yes (boolean) |

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

Each hook is individually disable-able (`setHookEnabled`), parameter-tunable, and pause-able (emergency multisig per [ADR 009](009-governance.md)); the controller is wholesale disable-able (`setControllerEnabled(false)`). Direct governance shares-updates take effect in the next-epoch settlement and the next hook evaluation reads the new baseline — vote always wins by evaluation order, not by override. The adaptive shifts never violate [ADR 026](026-gauge-boost-tokenomics.md) §11 (clamped to bounds, down to 0-pp if necessary).

### 6. Implementation

A new `AdaptiveFeeRouterController` (or methods on `FeeRouter` itself — choice deferred) reads `VotingEscrow` and the Balancer V3 TWAP, computes per-epoch shifts, and applies them via a privileged path on `FeeRouter`. The privileged path enforces the §11 bounds (the controller clamps first, the contract reverts on violation) and the sum-to-100% invariant on the six router shares. Every shift — including 0-pp no-ops from hysteresis or bound saturation — emits `AdaptiveShift(epoch, hook, requestedDelta, appliedDelta, reason)` so observers can reconstruct history from logs alone.

### 7. Deferment rationale

Drafted but **not deployed at launch**. Governance dynamics aren't observable yet; adaptive logic introduces a new code path that can mis-fire under stress (TWAP manipulation, threshold-edge stutter, keeper drift); the §11 safety bounds + manual governance are a sufficient hard floor at launch. If, six months post-mainnet, lock-rate or TWAP excursions measurably out-pace governance response, this ADR moves to Accepted and the controller deploys; otherwise it closes as Rejected.

---

## Consequences

### Positive

- **1-epoch response inside the §11 envelope.** Lock-rate and drawdown excursions get an automatic response instead of multi-week timelocked votes, without enlarging governance's reachable state space.
- **No new oracle or keeper class.** Lock-rate is on-chain native; TWAP reuses the [ADR 018](018-liquidity-strategy.md) pool; the controller fires from the existing buyback keeper.
- **Auditable.** Every shift (incl. no-op clamps and thin-pool/staleness skips) emits an event; behaviour is reconstructable from logs alone.

### Negative

- **New contract surface and keeper failure mode.** `AdaptiveFeeRouterController` plus a new "controller fires but read fails" failure class on top of [ADR 026](026-gauge-boost-tokenomics.md)'s expanded surface.
- **Threshold defaults are reasoned guesses.** The 15% / 50% lock-rate bands and $0.01 price floor are pre-launch estimates; production tuning expected within §3 outer bounds.
- **UI overhead.** Dashboards must distinguish "governance set this share" from "the hook shifted it this epoch", or users will conflate the two.

### Risks

- **TWAP manipulation under thin pools.** Pool-depth cap and staleness guard skip the hook in exactly that regime, but `minPoolDepth = $250K` is a reasoned guess. Residual: sustained-floor trading to cheaply move the 30-day TWAP — explicit reason for the deferment.
- **Governance-bypass perception.** Recommended deployment posture: hooks **disabled by default**, turned on by explicit vote once dynamics are observable.
- **Mis-fire at the threshold edge** (lock-rate at 14.9%, TWAP at $0.0099). The hysteresis band + staleness guard + pool-depth cap are designed to fail-safe (no-op) rather than shift in the wrong direction. Chaos-test before enabling.
- **Deferred adoption may not happen.** If post-launch governance is fast enough, this ADR closes as Rejected — drafting cost is small.

---

## Forward references

No follow-up ADRs. Dependencies — `FeeRouter`, `VotingEscrow`, the Balancer V3 80/20 pool, §11 safety bounds — already exist in [ADR 026](026-gauge-boost-tokenomics.md), [ADR 018](018-liquidity-strategy.md), [ADR 009](009-governance.md). On adoption, [ADR 016](016-contract-interactions.md) and [ADR 020](020-observability.md) update to register the new surface and metrics; both updates wait for this ADR to move out of deferred status.
