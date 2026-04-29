# ADR 028: Native sveTOKEN Liquid-ve Wrapper

**Date:** 2026-04-25
**Status:** Draft (deferred — ship within 6 months of v1 mainnet)
**Driver:** [ADR 026](026-gauge-boost-tokenomics.md) §Risks (Convex-capture risk)
**Touches:** [ADR 026](026-gauge-boost-tokenomics.md), [ADR 009](009-governance.md), [ADR 018](018-liquidity-strategy.md)

---

## Context

[ADR 026](026-gauge-boost-tokenomics.md) §4 defines `VotingEscrow` as a non-transferable, no-early-exit ve-position contract modeled on veCRV. The design is deliberate: a strict commitment device couples governance weight and gauge-boost yield to a multi-year capital lockup. The cost is illiquidity — once TOKEN enters a ve-lock, the only release path is the lock's natural decay to expiry.

Curve Finance's veCRV experience is the canonical case study for what happens when an illiquid commitment device meets a market that wants liquidity:

- **Convex Finance** launched a liquid wrapper (`cvxCRV`) on top of veCRV positions, accepting CRV deposits, locking them at maximum duration on a pooled basis, and issuing transferable cvxCRV at a 1:1 ratio.
- **Votium** built a vote-bribe market on top of cvxCRV's pooled voting power.
- **Aura** plays the same role for Balancer's veBAL.

Within ~18 months of veCRV launching, Convex held roughly 50% of veCRV supply. Wrapper holders trade governance influence to a third party in exchange for liquidity; the third party then directs gauge votes (and accepts bribes for them). The wrapper economy — wrapper-issuance fees, bribe revenue, secondary-market spreads — accrues entirely outside the issuing protocol's DAO.

For deCDN this is a structural risk to ADR 026:

1. **Governance capture.** A third-party wrapper holding 30–50% of veTOKEN concentrates ADR 026 §9 voting weight outside the deCDN DAO. The §11 safety bounds bound parameter changes but do not bound which gauges, payouts, or program directives the captured weight can push through.
2. **Wrapper-economy leakage.** Wrapper deposit fees, bribe markets, and liquidity-mining rewards become revenue for the wrapper protocol, not the deCDN DAO that built the gauge system.
3. **First-mover lock-in.** Once a third-party wrapper has liquidity depth and brand, displacing it is hard — Curve has not displaced Convex despite four years of trying.

[ADR 026](026-gauge-boost-tokenomics.md) §Risks flags this as "Convex-capture risk" and forward-references this ADR as a priority-1 follow-up. The [gauge-boost design spec §2.3](../docs/superpowers/specs/2026-04-18-tokenomics-v2-gauge-boost-design.md) ("Liquid-ve wrapper consideration") and the [survival-additions spec §3](../docs/superpowers/specs/2026-04-19-tokenomics-v2-survival-additions.md) ("Native liquid-ve wrapper — don't let Convex eat your governance") both recommend the same defensive move: ship a *native* liquid wrapper under DAO control before a third party ships one outside it.

The reference implementation is Frax's `sfrxETH`: an ERC-20 wrapper around an internally-pooled illiquid yield-bearing position, with appreciation-based exchange rate evolution and no protocol-level redemption (secondary-market exit only). The Frax model captures the wrapper-economy economics inside the issuing DAO, defends against third-party capture, and gives users a liquid asset without breaking the underlying commitment device.

This ADR specifies the deCDN-native equivalent: `SveToken`.

**Status note.** Draft, with a deferred ship-by date of **6 months after v1 mainnet**. Not blocking for v1 launch. The window exists because Convex-style capture takes time to develop — third-party wrappers depend on observable veTOKEN supply and gauge-pool size to be worth building. Shipping within 6 months stays ahead of that window without forcing the contract into a v1 audit slot.

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
4. `SveToken` calls into `VotingEscrow` to either:
   - Create the pooled lock at max duration (4 years) on the first-ever deposit, or
   - `extendLock` the existing pooled lock back to max duration **and** `increaseAmount` by `amount` on every subsequent deposit (see §6 for cadence).
5. `SveToken` mints `sveAmount = amount * totalSupply / underlyingTokenBalance` to the depositor (the standard ERC-4626-style appreciation formula). On the first-ever deposit, `sveAmount = amount` (initial 1:1 exchange rate, matching Frax v1 sfrxETH).

`underlyingTokenBalance` is the TOKEN amount currently held by the pooled `VotingEscrow` lock plus any TOKEN sitting in `SveToken`'s direct balance from accrued yield not yet recompounded.

There is no `withdraw` or `redeem` entry point in the protocol — see §4.

### 3. Exchange rate evolution

sveTOKEN appreciates against TOKEN over time. Three sources of appreciation:

1. **Delegator-pool TOKEN yield** ([ADR 026](026-gauge-boost-tokenomics.md) §6). The pooled `VotingEscrow` lock accrues a share of the 7% delegator-pool TOKEN inflows, distributed pro-rata by ve-balance. `SveToken` claims on behalf of the pool via `FeeRouter.claimDelegator(epochs[])`, receives TOKEN, and either:
   - Auto-compounds: calls `VotingEscrow.increaseAmount` to deposit the claimed TOKEN into the pooled lock. This grows `underlyingTokenBalance` without growing `totalSupply`, raising the exchange rate.
   - Or holds the claimed TOKEN in the `SveToken` contract's direct balance until a recompound batch (see §6); the direct balance is included in `underlyingTokenBalance`, so the exchange rate updates immediately even before the recompound transaction.

2. **Gauge-pool USDC yield** ([ADR 026](026-gauge-boost-tokenomics.md) §3). If the pooled `VotingEscrow` lock holder is also a registered operator (the wrapper is **not** an operator in v1; this is forward-flagged for governance), gauge-pool USDC accrues. Otherwise this leg is zero. In v1 we assume zero — `SveToken` is a passive ve-holder, not an operator.

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

**Why no protocol-level redemption?** A redemption path either:

- Forces an early-exit penalty path inside `VotingEscrow` (which [ADR 026](026-gauge-boost-tokenomics.md) §4 explicitly forbids — "Early exit: None"), or
- Backs redemptions with a TOKEN reserve (which means the wrapper holds liquid TOKEN that is not earning ve-yield, defeating the wrapper's purpose), or
- Uses a queue-and-wait model where redeemers are paid out from new deposits (which is just an internal secondary market with worse UX than a Balancer pool).

Frax `sfrxETH` and `cvxCRV` both reach the same conclusion: no protocol redemption, secondary-market exit only. We follow.

### 5. Lock-duration policy

The pooled `VotingEscrow` lock is maintained at **maximum duration (4 years), rolling**. Two extension cadences are viable:

| Cadence | When extension fires | Tradeoff |
| --- | --- | --- |
| **Per epoch** (default) | Once per 7-day epoch, called by a keeper or any caller | Lock duration drops by at most 1 week between extensions; ve-balance decay is bounded; cheap and predictable gas |
| **Per deposit** | On every `deposit(amount)` call | Lock duration is always exactly 4 years immediately after a deposit; deposit gas higher; quiet periods see longer ve-balance decay |
| **Hybrid** | Per deposit + per-epoch keeper as a backstop | Best ve-balance preservation; highest contract complexity |

**Default: per-epoch keeper extension, with an opportunistic refresh inside `deposit` if the current lock has more than 1 week of decay since last extension.** This caps ve-balance decay at ~0.05% (1 week / 4 years) between extensions, which is below the noise floor of weekly delegator-pool yield variation.

**Termination clause.** Governance can vote to stop extensions (a wind-down vote per [ADR 009](009-governance.md)). The pooled lock then decays to expiry over up to 4 years; deposits are disabled at the vote-execution timestamp; sveTOKEN remains transferable; `redeemAfterExpiry` activates after lock-decay completion.

### 6. Haircut policy

A small deposit fee captures wrapper-economy value for the DAO treasury rather than letting it leak to wrapper-protocol arbitrageurs.

| Parameter | Default | Min | Max |
| --- | ---: | ---: | ---: |
| `depositHaircut` | 0.10% | 0% | 0.50% |
| `compoundTip` | 0.10% | 0% | 1.00% |

`depositHaircut` is taken on the way in: of every TOKEN deposited, `(1 − depositHaircut)` is added to the pooled lock and `depositHaircut` flows to the protocol treasury (Timelock-custodied per [ADR 026](026-gauge-boost-tokenomics.md) §2). The haircut is governable inside the bounds shown.

**Why a haircut at all?** It is the wrapper's only deCDN-DAO revenue stream. Without it, the entire wrapper economy (deposit fees, secondary-market spreads, bribe revenue) accrues to wrapper-protocol arbitrageurs and to whatever bribe market eventually forms around sveTOKEN. The default of 0.10% is comparable to Frax sfrxETH's ~0% and to cvxCRV's 0% nominal but ~5% effective (via the staking-vs-not-staking spread), and is intentionally low to preserve user incentive to use the native wrapper rather than build a third-party fork around it. Governance can raise it later if usage proves sticky.

`compoundTip` rewards the keeper / caller who runs `harvestAndCompound`. It is not strictly a wrapper-economy fee — it is a permissionless-keeper subsidy. The default 0.10% covers L2 gas at every realistic gauge-boost-pool TVL.

### 6.1 Consolidated wrapper parameter table

| Parameter | Default | Min | Max | Governable | Notes |
| --- | ---: | ---: | ---: | --- | --- |
| Initial exchange rate | 1.0 sveTOKEN / TOKEN | — | — | No | Frax v1 sfrxETH parity; first-deposit invariant |
| Pooled lock target duration | 4 years (max) | 1 week | 4 years | No | Always extended back to max per §5 |
| Lock extension cadence | Per epoch + opportunistic-on-deposit | per-deposit | per-block | Yes | Default keeps decay ≤ 0.05% between extensions |
| `depositHaircut` | 0.10% | 0% | 0.50% | Yes | Wrapper-economy revenue to treasury |
| `compoundTip` | 0.10% of harvested | 0% | 1.00% | Yes | Permissionless-keeper subsidy; paid only on non-zero harvest |
| Claim window inheritance | 26 epochs | — | — | No | Inherited from [ADR 026](026-gauge-boost-tokenomics.md) §2; wrapper claims internally |
| Compound RPC routing | Private (Flashbots-style) | — | — | No | MEV defense; matches [ADR 018](018-liquidity-strategy.md) pattern |
| sveTOKEN/TOKEN POL seed (target) | $200K–$500K notional | TBD | TBD | Yes | Seeded at launch; size landed in [ADR 018](018-liquidity-strategy.md) |
| Wind-down vote outcome | Stop-extensions only | — | — | Yes | Per [ADR 009](009-governance.md) governance flow |
| Vote-mirroring policy | Deferred to ADR 028.1 | — | — | Yes | Sub-ADR governs the multisig's casting rules |

### 6.2 External interfaces (informative)

The contract surface that downstream code will see (signatures shown for reference; final ABI lands with the implementation, not this ADR):

```
function deposit(uint256 tokenAmount) external returns (uint256 sveAmount);
function harvestAndCompound(uint256[] calldata epochs) external returns (uint256 harvested);
function convertToAssets(uint256 sveAmount) external view returns (uint256 tokenAmount);
function convertToShares(uint256 tokenAmount) external view returns (uint256 sveAmount);
function exchangeRate() external view returns (uint256 wadRate); // 1e18-scaled
function redeemAfterExpiry(uint256 sveAmount) external returns (uint256 tokenAmount); // post-decay only
event Deposit(address indexed from, uint256 tokenAmount, uint256 sveAmount, uint256 haircutAmount);
event Compound(uint256 harvested, uint256 newUnderlyingBalance, uint256 newExchangeRate);
event WindDownInitiated(uint256 expiryTimestamp);
event RedeemAfterExpiry(address indexed to, uint256 sveAmount, uint256 tokenAmount);
```

`deposit` and `harvestAndCompound` are permissionless. `redeemAfterExpiry` is gated on the wrapper being in wind-down state (§5 termination clause) **and** the pooled lock having decayed past expiry. Wind-down is initiated only by Governor via Timelock per [ADR 009](009-governance.md). No other entry points mint, burn, or move the underlying TOKEN.

### 7. Governance pass-through

The largest open design question. Three options, all viable:

| Option | What sveTOKEN holders get | What the DAO gets | Verdict for v1 |
| --- | --- | --- | --- |
| **A. Direct pass-through** | Each sveTOKEN holder votes their pro-rata share of the underlying ve-balance | Voting power scales with sveTOKEN distribution; resembles direct veTOKEN voting | High UX cost (every sveTOKEN holder needs to vote on every proposal); negates one of the wrapper's main benefits (passive holding) |
| **B. Wrapper delegates to a DAO multisig with vote-mirroring policy** | Holders enjoy passive yield; do not vote | DAO retains effective control via the multisig; multisig is bound by published mirroring policy | **Recommended for v1** — preserves wrapper's passive-holding utility, keeps governance weight inside the DAO, defers the harder vote-mirroring details to a sub-ADR |
| **C. Hybrid: snapshot-style off-chain vote of sveTOKEN holders, executed on-chain by a wrapper-controlled relay** | Holders vote off-chain by holding sveTOKEN at snapshot block | DAO sees aggregated sveTOKEN preferences as an input to the on-chain vote | Highest infrastructure burden; best holder representation; defer to v2 |

**Recommendation for v1: Option B.** The wrapper contract holds the ve-position, the ve-position's voting weight is delegated to a DAO-controlled multisig (per [ADR 009](009-governance.md) emergency multisig topology), and the multisig is bound by a published vote-mirroring policy that defines how it casts the sveTOKEN-attributable votes (e.g., mirror the unwrapped-veTOKEN vote distribution, or default to "abstain" on contentious proposals, etc.).

**Vote-mirroring policy is deferred to a sub-ADR (provisionally ADR 028.1).** The policy gets contentious — it is a gating choice on what kinds of governance pressure sveTOKEN holders can exert, and on how the DAO can be challenged about the exercise of the wrapper-attributable weight. Pinning down the policy in this ADR would either undersell the question (vague enough to allow capture) or oversell it (specific enough to require a re-vote when reality contradicts it). The sub-ADR slot is reserved for that conversation.

### 8. MEV / liquidity considerations

sveTOKEN's utility is entirely a function of secondary-market depth. A liquid secondary market lets holders rotate out at close to fair value; a thin one means sveTOKEN trades at a steep discount to `convertToAssets` and the wrapper provides little usable liquidity over a direct ve-lock.

**DAO seeds initial liquidity.** When `SveToken` launches, the DAO seeds a Balancer V3 80/20 sveTOKEN/TOKEN pool from the protocol treasury (allocation TBD; estimate $200K–$500K notional at v1 prices, governable). The pool is Protocol-Owned-Liquidity per the [ADR 018](018-liquidity-strategy.md) pattern: treasury holds the BPT, no liquidity-mining rewards, MEV defenses (TWAP, private-RPC routing, per-epoch caps) inherited from [ADR 018](018-liquidity-strategy.md).

The exact pool seeding parameters (size, weights, fee tier) are **out of scope for this ADR** — they are a [ADR 018](018-liquidity-strategy.md) decision and should land there if and when sveTOKEN ships. The commitment this ADR makes is that DAO liquidity seeding **must happen** on or before sveTOKEN launch; without it the wrapper is underwater on day one.

**MEV around `harvestAndCompound`.** The compound transaction is observable. A searcher could front-run it to buy sveTOKEN cheaply pre-compound and sell post-compound. Mitigations:

- Compound calls go through the same private-RPC routing pattern as [ADR 018](018-liquidity-strategy.md) buybacks.
- Compounds are scheduled on randomized intra-epoch timestamps, not on a deterministic block.
- The exchange rate already includes uncompounded yield via the direct-balance term (§3), so the *visible* exchange-rate jump on compound is small (only the gas-tip is moving).

### 9. Phasing / launch timing

| Phase | Trigger | Action |
| --- | --- | --- |
| Pre-launch (v1) | Mainnet launch | sveTOKEN **does not exist**. veTOKEN is the only ve-position type. |
| Launch window | Within 6 months of v1 mainnet | Audit `SveToken`; seed Balancer V3 sveTOKEN/TOKEN POL; deploy under Timelock; initial deposits open |
| Steady state | 6+ months after launch | Auto-compound keepers active; haircut accruing to treasury; secondary market provides exit |
| Wind-down (only if invoked) | Governance vote to stop extensions | Lock decays to expiry; `redeemAfterExpiry` opens after decay completes |

**Why deferred to within 6 months of v1 — not at v1?** Shipping a native wrapper requires (a) observable veTOKEN supply and gauge-pool revenue to make the wrapper economically meaningful, (b) Balancer V3 POL depth to seed against, and (c) audit slots not consumed by the v1 contract surface (`FeeRouter`, `VotingEscrow`, `SafetyReserve`). A v1-concurrent wrapper has all three constraints binding simultaneously; a 6-month-deferred wrapper has none. The Convex-capture risk window is ~12–18 months, so 6 months is a comfortable margin while still preserving v1 launch focus.

**Not blocking for v1.** v1 ships with [ADR 026](026-gauge-boost-tokenomics.md)'s `VotingEscrow` only. Third-party wrapper risk in the first 6 months is bounded by the same constraints (a) (b) above; it is unattractive to attack a wrapper economy with no liquid float.

### 10. Risks specific to wrapper design

| Risk | Mechanism | Severity | Mitigation |
| --- | --- | --- | --- |
| **De-peg risk (downside)** | sveTOKEN trades below `convertToAssets` on the secondary market — the standard liquid-staked-token discount | High likelihood, low-medium impact | Expected behavior, not pathological. Frax `sfrxETH` typically trades 0.5–2% below `convertToAssets`; cvxCRV has historically traded 5–25% below CRV. DAO seeds POL to keep the discount tight |
| **De-peg risk (cascade)** | A large sveTOKEN holder dumps; the secondary pool moves; other holders panic-sell; discount widens; arbitrage doesn't close because there is no protocol redemption | Low-medium likelihood, high impact | Per-epoch liquidity caps on the sveTOKEN/TOKEN pool (per [ADR 018](018-liquidity-strategy.md) MEV-cap pattern); DAO POL absorbs at the pool's fair-value side; communications protocol around expected discount range |
| **Contagion to TOKEN price** | sveTOKEN discount widens → secondary-market arbitrage involves selling TOKEN → TOKEN price drops → ve-lock value drops → compound case-B from [ADR 026](026-gauge-boost-tokenomics.md) §Risks | Medium likelihood, medium impact | Same set of POL / liquidity-cap defenses as the [ADR 018](018-liquidity-strategy.md) buyback flow; the [ADR 029](029-adaptive-fee-router.md) price-floor feedback hook provides an automatic counter-pressure |
| **Governance capture by large sveTOKEN holders** | A whale accumulates sveTOKEN, then pressures the DAO multisig (Option B in §7) into mirroring their preferred votes | Medium likelihood, high impact | The vote-mirroring policy is the primary defense; sub-ADR specifies whether mirroring is "share-weighted" (whale wins) or "snapshot-weighted with caps" (whale capped); reserve right to publicly disregard a hostile mirror |
| **Auto-compound griefing** | Adversary spams `harvestAndCompound` with zero pending yield, draining the `compoundTip` over time | Low likelihood, low impact | `compoundTip` is paid only on actual harvested amounts (`tip = compoundTip × harvested`, not flat); zero-harvest calls cost the caller gas without paying out |
| **Oracle / mispricing scenarios** | An on-chain oracle misreads sveTOKEN value, a downstream protocol uses sveTOKEN as collateral at the wrong price | Low likelihood (no v1 collateral integrations), high impact if realized | Out of scope for v1 (wrapper is not collateral-eligible anywhere by default); document the risk for downstream protocols |
| **Wind-down failure** | Governance votes wind-down but a contract bug prevents `redeemAfterExpiry` | Very low likelihood, very high impact | `redeemAfterExpiry` is in the v1 contract from day one and is exercised in tests via shortened-lock fixtures; emergency-multisig pause does not affect post-decay redemption (it is a one-shot withdraw) |
| **Unbounded recompounding gas** | Pool of yield grows large; per-epoch `harvestAndCompound` runs into block gas limits | Low likelihood, low impact | `harvestAndCompound` accepts an `epochs[]` array and processes in chunks; permissionless callers can split work across multiple transactions |

---

## Consequences

### Positive

- **Convex-capture defense.** A native wrapper exists before a third party ships one. Wrapper-economy revenue (deposit haircut, future bribe-mediation fees) accrues to the deCDN DAO. Governance weight stays inside the DAO via Option B vote-mirroring.
- **User liquidity without breaking the commitment device.** Holders gain a tradeable token; the underlying lock is untouched; [ADR 026](026-gauge-boost-tokenomics.md)'s "no early exit" invariant is preserved.
- **Treasury revenue stream.** The 0.10% deposit haircut compounds with deposit volume. Modest in absolute terms but a non-zero recurring USD-denominated TOKEN flow to the treasury, separate from the [ADR 026](026-gauge-boost-tokenomics.md) §2 router buckets.
- **Auto-compounding raises effective ve-locker yield.** sveTOKEN holders effectively receive the auto-compound benefit that direct ve-lockers must DIY. This is the v3 model's "real yield in TOKEN" lever (per [ADR 026](026-gauge-boost-tokenomics.md) §6) packaged for passive holders.
- **Frax sfrxETH precedent.** The reference implementation has run for 2+ years at $400M+ TVL with no exploits and a tight ~1% discount band. Audit playbook is well-developed.

### Negative

- **Significant new contract surface.** `SveToken` proxy + implementation, vote-mirroring policy contract or multisig integration, possibly a `DelegatorClaimAdapter` if `FeeRouter.claimDelegator` is not directly callable by `SveToken`. Each is small individually; together they expand audit scope by ~30–40% over [ADR 026](026-gauge-boost-tokenomics.md)'s already-larger-than-ADR-004 contract surface.
- **Operational burden.** Compound keepers, deposit-flow monitoring, secondary-market discount tracking, vote-mirroring policy execution. Not contract-level burden but DAO-process burden.
- **Discount UX.** Users will be confused that `1 sveTOKEN ≠ 1 TOKEN` on the open market even though `convertToAssets(1 sveTOKEN) ≥ 1 TOKEN`. Documentation, dashboards, and front-end displays must explain the discount and the appreciation rate clearly.
- **Concentration of governance weight via Option B multisig.** Even with vote-mirroring policy, the multisig is a focal point for capture attempts. Mitigated by the multisig being a [ADR 009](009-governance.md) emergency-multisig topology with limited unilateral authority.
- **Two competing ve-products.** Direct ve-lockers (governance + delegator-pool yield + gauge-boost yield, illiquid) and sveTOKEN holders (governance via mirror + delegator-pool yield via auto-compound, liquid). Differentiation is gauge-boost-yield: only direct ve-lockers can capture the §3 boost (because only they are operators). Documentation must not imply sveTOKEN replaces direct ve-locking for operators.

### Risks

- **Liquid wrapper depeg risk.** Persistent secondary-market discount is the wrapper's signature failure mode. Frax handles it with auto-compounding and tight POL; cvxCRV historically didn't, and trades at a structural 5–25% discount. Our defenses (auto-compound + POL + per-epoch caps) lean toward the Frax pattern. If the discount widens past ~5% sustainedly, governance has the option to invoke wind-down (§5 termination clause).
- **sveTOKEN→TOKEN price contagion.** A wide sveTOKEN discount creates arbitrage flows that move the underlying TOKEN price. The sveTOKEN/TOKEN pool's per-epoch caps bound the per-epoch contagion volume, but a multi-epoch drawdown is possible. Compounds with [ADR 026](026-gauge-boost-tokenomics.md) §Risks "Reflexive bootstrap intensified at the operator-margin layer" — a sveTOKEN depeg could intensify operator-margin pressure during a TOKEN drawdown.
- **Protocol-complexity tax.** Each additional yield-routing contract is one more thing to audit, monitor, upgrade, and explain. The [ADR 026](026-gauge-boost-tokenomics.md) §Negative "Higher contract surface than ADR 004" remark applies again, compounded. The deferred ship date (within 6 months of mainnet) is partly a tax-amortization choice.
- **Vote-mirroring-policy capture.** The mirror policy is the entire defense against Option B becoming a single-point-of-failure for governance. Sub-ADR (provisionally 028.1) is required before sveTOKEN ships.
- **Wrapper-on-wrapper risk.** Once sveTOKEN exists, third parties may build wrappers on sveTOKEN itself (a "Convex on the deCDN-Convex"). The defense is the same: native sveTOKEN should be liquid and useful enough that a third-party wrapper-of-wrapper doesn't add value. If it does, that's a v3 problem.

---

## Forward references

- **ADR 028.1 — sveTOKEN vote-mirroring policy** *(sub-ADR; deferred until vote-mirroring details become contentious or the launch slot opens, whichever first).* Specifies how the wrapper-controlled multisig (Option B above) casts sveTOKEN-attributable votes. Topics: snapshot vs continuous mirror, weighting (raw sveTOKEN balance vs caps), abstain rules, contentious-proposal handling, override conditions.
- **[ADR 018](018-liquidity-strategy.md) update** *(at sveTOKEN launch).* Add the sveTOKEN/TOKEN Balancer V3 80/20 POL pool to the [ADR 018](018-liquidity-strategy.md) liquidity strategy, including seed size, MEV defenses (inheriting the existing TWAP / private-RPC / per-epoch-cap pattern), and BPT custody.
- **[ADR 020](020-observability.md) update** *(at sveTOKEN launch).* Surface metrics: sveTOKEN total supply, exchange rate, secondary-market discount, compound frequency, deposit haircut accrual, POL pool depth.
