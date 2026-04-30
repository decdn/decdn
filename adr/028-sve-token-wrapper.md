# ADR 028: Native sveTOKEN Liquid-ve Wrapper

**Date:** 2026-04-25
**Status:** Draft (deferred — ship within 6 months of mainnet)
**Driver:** [ADR 026](026-gauge-boost-tokenomics.md) §Risks (Convex-capture risk)
**Touches:** [ADR 026](026-gauge-boost-tokenomics.md), [ADR 009](009-governance.md), [ADR 018](018-liquidity-strategy.md)

---

## Context

[ADR 026](026-gauge-boost-tokenomics.md) §4 defines `VotingEscrow` as a non-transferable, no-early-exit ve-position contract modeled on veCRV. The design is deliberate: a strict commitment device couples governance weight and gauge-boost yield to a multi-year capital lockup. The cost is illiquidity — once TOKEN enters a ve-lock, the only release path is the lock's natural decay to expiry.

Third-party liquid wrappers historically capture 30–50% of ve-supply on illiquid commitment devices (Convex/veCRV reached ~50% within 18 months; Votium and Aura play similar roles for cvxCRV and veBAL). Wrapper holders trade governance influence to a third party for liquidity; the wrapper economy — issuance fees, bribe revenue, secondary spreads — accrues outside the issuing DAO. For deCDN this is a structural risk to ADR 026: governance capture (third-party wrapper concentrates §9 voting weight outside the DAO), wrapper-economy leakage (revenue accrues to the wrapper protocol), first-mover lock-in (Curve hasn't displaced Convex in four years).

[ADR 026](026-gauge-boost-tokenomics.md) §Risks flags this as "Convex-capture risk" and forward-references this ADR as priority-1. The defensive move (per design-spec §2.3 and survival-additions §3): ship a *native* liquid wrapper under DAO control before a third party ships one outside it.

The reference implementation is Frax's `sfrxETH`: an ERC-20 wrapper around an internally-pooled illiquid yield-bearing position, with appreciation-based exchange rate evolution and no protocol-level redemption (secondary-market exit only). The Frax model captures the wrapper-economy economics inside the issuing DAO, defends against third-party capture, and gives users a liquid asset without breaking the underlying commitment device.

This ADR specifies the deCDN-native equivalent: `SveToken`.

**Status note.** Draft, with a deferred ship-by date of **6 months after mainnet**. Not blocking for launch. The window exists because Convex-style capture takes time to develop — third-party wrappers depend on observable veTOKEN supply and gauge-pool size to be worth building. Shipping within 6 months stays ahead of that window without forcing the contract into the launch audit slot.

---

## Decision

The protocol will ship a native liquid-ve wrapper, `SveToken`, owned and operated by the deCDN DAO, modeled on Frax `sfrxETH` and adapted for the ADR 026 ve-system.

### 1. Wrapper contract: `SveToken`

A new ERC-20 contract, transferable, with the following properties:

| Property | Value |
| --- | --- |
| Symbol | `sveTOKEN` |
| Standard | ERC-20 (full `transfer` / `approve` / `permit`) |
| Underlying asset | TOKEN locked in `VotingEscrow` via the `SveToken` contract's own ve-position |
| Ownership | DAO (Governor + Timelock per [ADR 009](009-governance.md)) |
| Upgrade path | Proxied; upgrades governed by Timelock |
| Total supply | Mints on deposit, burns only on natural lock-decay redemption (§4) |
| Position model | Single pooled `VotingEscrow` lock owned by `SveToken`; each sveTOKEN = a fractional claim on that pooled position |

The contract holds exactly one `VotingEscrow` lock at any time. All deposits extend or top up that lock; all sveTOKEN claims are pro-rata over its current TOKEN balance. There is no per-user lock under the wrapper — that is the entire point of the abstraction.

### 2. Deposit mechanics

`deposit(amount) → sveAmount`:

1. User calls `TOKEN.approve(SveToken, amount)`.
2. User calls `SveToken.deposit(amount)`.
3. `SveToken` pulls `amount` TOKEN from the user.
4. `SveToken` accumulates the deposit and lazily folds it into the pooled `VotingEscrow` lock:
   - Create the pooled lock at max duration (4 years) on the first-ever deposit (with `increaseAmount` by `amount`), or
   - On subsequent deposits, the deposited TOKEN sits in `SveToken`'s direct balance until the next batched fold (per §5 / §6.1 cadence). At fold time, a single transaction calls `extendLock` back to max duration and `increaseAmount` by the accumulated balance. This batching is gas-essential: per-deposit `increaseAmount` calls are gas-prohibitive for small deposits, and exchange-rate accuracy is preserved in the interim because `underlyingTokenBalance` already includes `SveToken`'s direct balance (see formula below).
5. `SveToken` mints `sveAmount = amount * totalSupply / underlyingTokenBalance` to the depositor (the standard ERC-4626-style appreciation formula). On the first-ever deposit, `sveAmount = amount` (initial 1:1 exchange rate, matching Frax sfrxETH).

`underlyingTokenBalance` is the TOKEN amount currently held by the pooled `VotingEscrow` lock plus any TOKEN sitting in `SveToken`'s direct balance from accrued yield not yet recompounded.

There is no `withdraw` or `redeem` entry point in the protocol — see §4.

### 3. Exchange rate evolution

sveTOKEN appreciates against TOKEN over time. Three sources of appreciation:

1. **Delegator-pool TOKEN yield** ([ADR 026](026-gauge-boost-tokenomics.md) §6). The pooled `VotingEscrow` lock accrues a share of the 7% delegator-pool TOKEN inflows, distributed pro-rata by ve-balance. `SveToken` claims on behalf of the pool via `FeeRouter.claimDelegator(epochs[])`, receives TOKEN, and either:
   - Auto-compounds: calls `VotingEscrow.increaseAmount` to deposit the claimed TOKEN into the pooled lock. This grows `underlyingTokenBalance` without growing `totalSupply`, raising the exchange rate.
   - Or holds the claimed TOKEN in the `SveToken` contract's direct balance until a recompound batch (see §6); the direct balance is included in `underlyingTokenBalance`, so the exchange rate updates immediately even before the recompound transaction.

2. **Gauge-pool USDC yield** ([ADR 026](026-gauge-boost-tokenomics.md) §3). The wrapper is not an operator (this is forward-flagged for governance), so this leg is zero — `SveToken` is a passive ve-holder.

3. **Auto-compound mechanism for delegator yield.** A keeper or any caller invokes `SveToken.harvestAndCompound()`. This:
   1. Calls `FeeRouter.claimDelegator(epochs)` for any unclaimed epoch buckets.
   2. Calls `VotingEscrow.increaseAmount(claimedTokenAmount)` against the pooled lock.
   3. Emits `Compound(amount, newUnderlyingBalance, newExchangeRate)`.
   The function is permissionless; gas is reimbursed to the caller from a small fixed `compoundTip` paid out of the harvested TOKEN (default 0.1%, governable within `[0%, 1%]`).

The exchange rate is read via `convertToAssets(sveAmount) → tokenAmount` and `convertToShares(tokenAmount) → sveAmount`, ERC-4626-style. The wrapper does **not** guarantee the secondary-market price tracks `convertToAssets` — that is the depeg surface (§10).

### 4. Liquidity — secondary market only

**The protocol does not offer redemption.** There is no `redeem(sveAmount)` or `withdraw(amount)` entry point that converts sveTOKEN back to TOKEN under the wrapper. Holders exit only via two paths:

1. **Secondary-market sale.** Sell sveTOKEN for TOKEN (or USDC) on a Balancer V3 / Curve / DEX pool. Market price is whatever the AMM pays — usually below `convertToAssets` (the implied "fair" rate), because the underlying TOKEN is genuinely locked for up to 4 years.
2. **Hold to natural decay and redeem.** Once the pooled `VotingEscrow` lock expires (only happens if `SveToken` stops extending it — see §6 termination clause), `SveToken` enters a wind-down state: depositors can no longer mint, and any sveTOKEN holder can call `redeemAfterExpiry(sveAmount)` to claim their pro-rata share of the underlying TOKEN at the post-expiry exchange rate. This is the only direct-from-protocol exit, and it is unavailable while the wrapper is in normal operation (which it always is, by §6 design).

**Why no protocol-level redemption?** Any redemption path either breaks the [ADR 026](026-gauge-boost-tokenomics.md) §4 "no early exit" invariant, holds a non-yielding TOKEN reserve that defeats the wrapper, or becomes an internal secondary market worse than a Balancer pool. Frax `sfrxETH` and `cvxCRV` both reach the same conclusion.

### 5. Lock-duration policy

The pooled `VotingEscrow` lock is maintained at **maximum duration (4 years), rolling**. Default cadence: per-epoch keeper extension with an opportunistic refresh inside `deposit` when the lock has more than 1 week of decay since last extension. Caps ve-balance decay at ~0.05% (1 week / 4 years) between extensions — below the weekly delegator-pool yield variance.

**Termination clause.** Governance can vote to stop extensions (a wind-down vote per [ADR 009](009-governance.md)). The pooled lock then decays to expiry over up to 4 years; deposits are disabled at the vote-execution timestamp; sveTOKEN remains transferable; `redeemAfterExpiry` activates after lock-decay completion.

### 6. Haircut policy

A small deposit fee captures wrapper-economy value for the DAO treasury rather than letting it leak to wrapper-protocol arbitrageurs.

| Parameter | Default | Min | Max |
| --- | ---: | ---: | ---: |
| `depositHaircut` | 0.10% | 0% | 0.50% |
| `compoundTip` | 0.10% | 0% | 1.00% |

`depositHaircut` is taken on the way in: of every TOKEN deposited, `(1 − depositHaircut)` is added to the pooled lock and `depositHaircut` flows to the protocol treasury (Timelock-custodied per [ADR 026](026-gauge-boost-tokenomics.md) §2). The haircut is governable inside the bounds shown.

**Why a haircut at all?** It is the wrapper's only deCDN-DAO revenue stream. Without it, the entire wrapper economy (deposit fees, secondary-market spreads, bribe revenue) accrues to wrapper-protocol arbitrageurs and to whatever bribe market eventually forms around sveTOKEN. The default of 0.10% deposit haircut sits between Frax sfrxETH (no deposit fee; protocol revenue comes from a small fee on accrued yield) and cvxCRV (0% nominal but ~5% effective via the staking-vs-not-staking spread), and is intentionally low to preserve user incentive to use the native wrapper rather than build a third-party fork around it. Governance can raise it later if usage proves sticky.

`compoundTip` rewards the keeper / caller who runs `harvestAndCompound`. It is not strictly a wrapper-economy fee — it is a permissionless-keeper subsidy. The default 0.10% covers L2 gas at every realistic gauge-boost-pool TVL.

### 6.1 Consolidated wrapper parameter table

| Parameter | Default | Min | Max | Governable | Notes |
| --- | ---: | ---: | ---: | --- | --- |
| Initial exchange rate | 1.0 sveTOKEN / TOKEN | — | — | No | Frax sfrxETH parity; first-deposit invariant |
| Pooled lock target duration | 4 years (max) | 1 week | 4 years | No | Always extended back to max per §5 |
| Lock extension cadence | Per epoch + opportunistic-on-deposit | per-deposit | per-block | Yes | Default keeps decay ≤ 0.05% between extensions |
| `depositHaircut` | 0.10% | 0% | 0.50% | Yes | Wrapper-economy revenue to treasury |
| `compoundTip` | 0.10% of harvested | 0% | 1.00% | Yes | Permissionless-keeper subsidy; paid only on non-zero harvest |
| Claim window inheritance | 26 epochs | — | — | No | Inherited from [ADR 026](026-gauge-boost-tokenomics.md) §2; wrapper claims internally |
| Compound RPC routing | Private (Flashbots-style) | — | — | No | MEV defense; matches [ADR 018](018-liquidity-strategy.md) pattern |
| sveTOKEN/TOKEN POL seed (target) | $200K–$500K notional | TBD | TBD | Yes | Seeded at launch; size landed in [ADR 018](018-liquidity-strategy.md) |
| Wind-down vote outcome | Stop-extensions only | — | — | Yes | Per [ADR 009](009-governance.md) governance flow |
| Vote-mirroring policy | Deferred to a follow-up ADR | — | — | Yes | Follow-up ADR governs the multisig's casting rules |

**Entry-point shape (informative).** ERC-4626-style: `deposit`, `harvestAndCompound`, and the `convertToAssets` / `convertToShares` / `exchangeRate` views are permissionless. `redeemAfterExpiry` is gated on wind-down + post-decay; wind-down is Governor + Timelock only ([ADR 009](009-governance.md)). No other entry point mints, burns, or moves the underlying TOKEN. Final ABI lands with the implementation, not this ADR.

### 7. Governance pass-through

The largest open design question. Three options, all viable:

| Option | What sveTOKEN holders get | What the DAO gets | Verdict |
| --- | --- | --- | --- |
| **A. Direct pass-through** | Each sveTOKEN holder votes their pro-rata share of the underlying ve-balance | Voting power scales with sveTOKEN distribution; resembles direct veTOKEN voting | High UX cost (every sveTOKEN holder needs to vote on every proposal); negates one of the wrapper's main benefits (passive holding) |
| **B. Wrapper delegates to a DAO multisig with vote-mirroring policy** | Holders enjoy passive yield; do not vote | DAO retains effective control via the multisig; multisig is bound by published mirroring policy | **Chosen** — preserves wrapper's passive-holding utility, keeps governance weight inside the DAO, defers the harder vote-mirroring details to a follow-up ADR |
| **C. Hybrid: snapshot-style off-chain vote of sveTOKEN holders, executed on-chain by a wrapper-controlled relay** | Holders vote off-chain by holding sveTOKEN at snapshot block | DAO sees aggregated sveTOKEN preferences as an input to the on-chain vote | Highest infrastructure burden; best holder representation; deferred follow-up |

**Decision: Option B.** The wrapper contract holds the ve-position, the ve-position's voting weight is delegated to a DAO-controlled multisig (per [ADR 009](009-governance.md) emergency multisig topology), and the multisig is bound by a published vote-mirroring policy that defines how it casts the sveTOKEN-attributable votes (e.g., mirror the unwrapped-veTOKEN vote distribution, or default to "abstain" on contentious proposals, etc.).

**Vote-mirroring policy is deferred to a follow-up ADR.** The policy gets contentious — it is a gating choice on what kinds of governance pressure sveTOKEN holders can exert, and on how the DAO can be challenged about the exercise of the wrapper-attributable weight. Pinning it down here would either undersell the question (vague enough to allow capture) or oversell it (specific enough to require a re-vote when reality contradicts it). The follow-up ADR is authored at the next available slot when the conversation opens.

### 8. MEV / liquidity considerations

sveTOKEN's utility is entirely a function of secondary-market depth. A liquid secondary market lets holders rotate out at close to fair value; a thin one means sveTOKEN trades at a steep discount to `convertToAssets` and the wrapper provides little usable liquidity over a direct ve-lock.

**DAO seeds initial liquidity.** When `SveToken` launches, the DAO seeds a Balancer V3 80/20 sveTOKEN/TOKEN pool from the protocol treasury (allocation TBD; estimate $200K–$500K notional at then-current prices, governable). The pool is Protocol-Owned-Liquidity per the [ADR 018](018-liquidity-strategy.md) pattern: treasury holds the BPT, no liquidity-mining rewards, MEV defenses (TWAP, private-RPC routing, per-epoch caps) inherited from [ADR 018](018-liquidity-strategy.md).

The exact pool seeding parameters (size, weights, fee tier) are **out of scope for this ADR** — they are a [ADR 018](018-liquidity-strategy.md) decision and should land there if and when sveTOKEN ships. The commitment this ADR makes is that DAO liquidity seeding **must happen** on or before sveTOKEN launch; without it the wrapper is underwater on day one.

**MEV around `harvestAndCompound`.** The compound transaction is observable. A searcher could front-run it to buy sveTOKEN cheaply pre-compound and sell post-compound. Mitigations:

- Compound calls go through the same private-RPC routing pattern as [ADR 018](018-liquidity-strategy.md) buybacks.
- Compounds are scheduled on randomized intra-epoch timestamps, not on a deterministic block.
- The exchange rate already includes uncompounded yield via the direct-balance term (§3), so the *visible* exchange-rate jump on compound is small (only the gas-tip is moving).

### 9. Phasing / launch timing

| Phase | Trigger | Action |
| --- | --- | --- |
| Pre-launch | Mainnet launch | sveTOKEN **does not exist**. veTOKEN is the only ve-position type. |
| Launch window | Within 6 months of mainnet | Audit `SveToken`; seed Balancer V3 sveTOKEN/TOKEN POL; deploy under Timelock; initial deposits open |
| Steady state | 6+ months after launch | Auto-compound keepers active; haircut accruing to treasury; secondary market provides exit |
| Wind-down (only if invoked) | Governance vote to stop extensions | Lock decays to expiry; `redeemAfterExpiry` opens after decay completes |

**Why deferred — not at launch?** Shipping a native wrapper requires (a) observable veTOKEN supply and gauge-pool revenue to make the wrapper economically meaningful, (b) Balancer V3 POL depth to seed against, and (c) audit slots not consumed by the launch contract surface (`FeeRouter`, `VotingEscrow`, `SafetyReserve`). A launch-concurrent wrapper has all three constraints binding simultaneously; a 6-month-deferred wrapper has none. The Convex-capture risk window is ~12–18 months, so 6 months is a comfortable margin.

**Not blocking for launch.** The launch deployment ships with [ADR 026](026-gauge-boost-tokenomics.md)'s `VotingEscrow` only. Third-party wrapper risk in the first 6 months is bounded by the same constraints (a) (b) above; it is unattractive to attack a wrapper economy with no liquid float.

### 10. Risks specific to wrapper design

| Risk | Mitigation |
| --- | --- |
| **Persistent secondary-market discount** (the wrapper's signature failure mode; ~0.5–2% on Frax sfrxETH, historically 5–25% on cvxCRV as of 2026-Q1) | DAO POL seed; per-epoch liquidity caps inheriting [ADR 018](018-liquidity-strategy.md) defenses; documented expected-discount range |
| **Cascade depeg** (large dump → discount widens → no protocol redemption to arbitrage) | Per-epoch caps; POL absorbs fair-value side; wind-down is the structural escape valve |
| **Contagion to TOKEN price** (depeg arbitrage routes through TOKEN) | Same POL/cap defenses as [ADR 018](018-liquidity-strategy.md); [ADR 029](029-adaptive-fee-router.md) price-floor hook provides automatic counter-pressure |
| **Governance capture via Option B multisig** (whale accumulates sveTOKEN to pressure mirror policy) | Vote-mirroring policy specifies whether mirroring is share-weighted or capped; reserve the right to disregard a hostile mirror |

---

## Consequences

### Positive

- **Convex-capture defense.** Native wrapper exists before a third party ships one; wrapper-economy revenue accrues to the DAO; governance weight stays inside via Option B vote-mirroring.
- **Liquidity without breaking the commitment device.** Underlying lock is untouched; [ADR 026](026-gauge-boost-tokenomics.md) §4 "no early exit" invariant preserved.
- **Auto-compounded delegator-pool yield** (the "real yield in TOKEN" lever) packaged for passive holders.
- **Mature precedent.** Frax sfrxETH has 2+ years at $400M+ TVL with a tight ~1% discount band; audit playbook is known.

### Negative

- **New contract surface.** `SveToken` proxy + implementation + vote-mirroring policy expands [ADR 026](026-gauge-boost-tokenomics.md) audit scope ~30–40%.
- **Operational burden.** Compound keepers, discount tracking, mirror-policy execution.
- **Discount UX.** Users may conflate `1 sveTOKEN` with `1 TOKEN`; dashboards must surface `convertToAssets` and the appreciation rate.
- **Two competing ve-products** (direct ve-lockers vs. sveTOKEN holders). Differentiation is gauge-boost yield: only direct ve-lockers capture it. Documentation must not imply sveTOKEN replaces direct ve-locking for operators.

### Risks

- **Wrapper depeg.** Sustained secondary-market discount is the wrapper's signature failure mode (Frax: 0.5–2%, cvxCRV: 5–25% structurally as of 2026-Q1). Defenses (auto-compound + POL + per-epoch caps) lean Frax; if the discount widens past ~5%, governance can invoke wind-down.
- **sveTOKEN→TOKEN contagion.** Depeg arbitrage routes through TOKEN; per-epoch caps bound per-epoch volume but multi-epoch drawdowns are possible. Compounds with [ADR 026](026-gauge-boost-tokenomics.md) §Risks "Reflexive bootstrap intensified".
- **Vote-mirroring capture.** The mirror policy is the only defense against Option B becoming a governance single-point-of-failure. The follow-up ADR is a hard prerequisite for sveTOKEN shipping.

---

## Forward references

- **sveTOKEN vote-mirroring policy** *(deferred follow-up ADR; authored when vote-mirroring details become contentious or the launch slot opens, whichever first).* Specifies how the wrapper-controlled multisig (Option B above) casts sveTOKEN-attributable votes. Topics: snapshot vs continuous mirror, weighting (raw sveTOKEN balance vs caps), abstain rules, contentious-proposal handling, override conditions.
- **[ADR 018](018-liquidity-strategy.md) update** *(at sveTOKEN launch).* Add the sveTOKEN/TOKEN Balancer V3 80/20 POL pool to the [ADR 018](018-liquidity-strategy.md) liquidity strategy, including seed size, MEV defenses (inheriting the existing TWAP / private-RPC / per-epoch-cap pattern), and BPT custody.
- **[ADR 020](020-observability.md) update** *(at sveTOKEN launch).* Surface metrics: sveTOKEN total supply, exchange rate, secondary-market discount, compound frequency, deposit haircut accrual, POL pool depth.
