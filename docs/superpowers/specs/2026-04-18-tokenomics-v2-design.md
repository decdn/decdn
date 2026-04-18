# Tokenomics v2 — ve-escrow + fee router

**Date:** 2026-04-18
**Status:** Design spec (pre-ADR)
**Supersedes (on acceptance):** ADR 004 §Token Distribution, §Fee Allocation, §BuybackBurner, §Node Unit Economics (fee-discount subsection); portions of ADR 003 relating to settlement-time fee skim.
**Also updates:** ADR 009 (governance weighting), ADR 018 (buyback inflow rate only; POL mechanics unchanged), finance `params.py`.

---

## 1. Problem

ADR 004's tokenomics produces structurally weak value accrual to TOKEN:

- **Burn flow is ~0.014% of supply/yr** at a 1,000-node mature network (20% of 3% of revenue, burned at $0.05 TOKEN). Noise against ~22%/yr circulating-supply growth from vesting unlocks.
- **No yield to token holders.** Only utilities are stake-to-operate, fee discount (small $), and governance voting. Passive holders earn nothing from network usage.
- **Regressive discount.** The "stake 10× min for fee discount" mechanic reduces the buyback flow as more operators qualify — larger stakers weaken the deflationary sink.
- **Treasury hoards USDC.** 80% of protocol fees (dev fund 40% + audits 20% + ecosystem 20%) are held as stablecoin; no return pathway to TOKEN holders.

The rewrite is structural, not cosmetic. Burn becomes secondary. Real yield to ve-lockers and supply discipline via auto-ve-lock become primary.

---

## 2. Design

### 2.1 Supply & distribution

**500M TOKEN fixed supply, no minting post-genesis.** Halved from ADR 004's 1B.

| Bucket | % | Tokens | Vesting | Auto-ve-lock on vest |
|---|---|---|---|---|
| Protocol treasury | 25% | 125M | 6y linear, 6mo cliff | 2y |
| Node bootstrap fund | 20% | 100M | On-demand via governance | 1y |
| Community & ecosystem | 20% | 100M | 6y linear, no cliff | 1y |
| Team & contributors | 15% | 75M | 6y linear, 12mo cliff | 2y |
| Liquidity (POL) | 5% | 25M | Unlocked at genesis, Timelock-custodied (ADR 018) | None |
| Seed / early supporters | 10% | 50M | 6y linear, 12mo cliff | 2y |

**Auto-ve-lock semantics.** When a vested tranche is released from the vesting contract into a recipient's wallet, it is atomically deposited into `VotingEscrow` with the bucket's stated lock period. The recipient owns the ve-position (accrues voting weight and ve-locker yield immediately) but the underlying TOKEN is non-transferable until the lock decays to zero. Recipients cannot bypass the auto-lock — the vesting contract calls `VotingEscrow.create_lock_for(recipient, amount, duration)` directly; no path routes TOKEN to the recipient unlocked.

**Bootstrap fund subsidies.** Bootstrap TOKEN disbursed to node operators as subsidies also auto-ve-lock (1y) on delivery. Operators receive the ve-position and accrue yield while waiting for the lock to decay. Rationale: subsidies should bind operators to the network over time, not fund immediate sell pressure.

**Effective circulating-supply growth.** 500M ÷ (6y vest + 2y mean lock) ≈ **~62.5M/yr ≈ 12.5%/yr** during the active vesting window. Halves ADR 004's effective unlock rate (~25%/yr).

**Liquidity allocation halved (10% → 5%).** POL pool depth in absolute TOKEN terms is unchanged (25M TOKEN on a 500M supply represents the same pool-depth fraction as 50M on 1B); ADR 018's Balancer V3 80/20 weighted-pool mechanics continue unchanged.

### 2.2 Fee router

New contract `FeeRouter` replaces the ADR 003 settlement-fee-skim pattern entirely. Channel settlements route **100% of operator payment** through the router; the router performs the economic split atomically.

**Split (T2):**

| Destination | % | Payment flow |
|---|---|---|
| Operator | 75% | Transferred USDC to operator's address in the same `settleChannel` transaction |
| ve-locker pool | 12% | Accumulates in router per epoch; distributed pro-rata by ve-balance snapshot at epoch boundary |
| BuybackBurner | 8% | Transferred USDC to `BuybackBurner` (ADR 018 mechanics unchanged) |
| Treasury | 5% | Transferred USDC to Timelock-custodied treasury wallet |

**Applies only to client→node channel settlements.** Node-to-node cache-miss paid pulls bypass the router — direct peer USDC payment, no skim. Rationale: internal cost recovery, not net revenue.

**Rate mechanics.** Per-MB rates are operator-set via probe responses (ADR 003 unchanged). Operators pricing to preserve $0.01/GB **net** revenue quote ~$0.0133/GB gross; clients pay the quoted rate; router's 25% skim is structural.

**Governable split parameters (within hard-coded safety bounds):**

| Parameter | Default | Min | Max |
|---|---|---|---|
| Operator share | 75% | 50% | 95% |
| ve-locker share | 12% | 0% | 30% |
| Burn share | 8% | 0% | 25% |
| Treasury share | 5% | 0% | 20% |

Shares must sum to 100% on any update. Governance changes gated by 48h timelock (ADR 009).

**ve-locker pool epoch mechanics.**

- Epoch length: 1 week (7 × 86400 seconds, block-timestamp-aligned).
- Router accumulates USDC in a per-epoch bucket. At epoch rollover, the finalized bucket is frozen; the new bucket begins accumulating.
- ve-balance snapshot taken at the exact epoch-boundary timestamp using `VotingEscrow.balanceOfAt(user, ts)` (historical-checkpoint pattern).
- Distribution: pull-based. ve-lockers call `FeeRouter.claim(epochs[])` with the list of epochs they want to claim from. Each call transfers their pro-rata share of those epochs' USDC and marks them claimed.
- Claim window: 26 epochs (~6 months). Unclaimed allocations after 26 epochs are swept to treasury to prevent indefinite dust accumulation.

**Rationale for pull-over-push:** push-distribution requires iterating over all ve-lockers weekly — unbounded gas cost and keeper dependency. Pull is O(1) per locker, locker pays their own gas.

### 2.3 Voting escrow (`VotingEscrow`)

Vote-escrow TOKEN (veTOKEN). Modeled on veCRV with deliberate deviations noted.

| Parameter | Value | Note |
|---|---|---|
| Lockable token | TOKEN | ERC-20 |
| Min lock duration | 1 week | |
| Max lock duration | 4 years | |
| ve-balance formula | `amount × remaining_lock_time / 4y` | Decays linearly to zero at lock expiry |
| Extension | Allowed | Lock can be extended up to 4y from current time |
| Shortening | Not allowed | |
| Early exit | **None** | No penalty-exit option (stricter than Convex; matches veCRV) |
| Transferability | Non-transferable | No `transfer` / `approve` for ve-positions |
| Slashing on ve-position | **No** | Ve-locked TOKEN is never slashable, even if locker is also a node operator |

**Historical checkpointing.** `VotingEscrow` implements `balanceOfAt(user, ts)` via per-lock checkpoints. Reads are O(log n) on checkpoint array; writes are O(1) amortized. This is a load-bearing requirement for the epoch-snapshot pattern in 2.2.

**Operator ve-positions.** Operators holding ve-locks appear in the same contract as passive holders; their ve-balance earns from the 12% pool identically. There is no separate "operator lock-boost" mechanic — operators earn the 75% router share on their delivery revenue (work), and may optionally earn from the 12% pool on their locked TOKEN (capital). The two are orthogonal.

**ve-positions and operator stake are separate.** A node's operator stake is held in `StakingRegistry` (ADR 003) and is slashable; a ve-position is held in `VotingEscrow` and is not. An operator may hold both; they are not interchangeable and neither satisfies the other's requirements.

### 2.4 Operator economics

**Stake (from branch `claude/review-adrs-unmetered-nodes-gmFkQ`):** Min 10K TOKEN, slashable, 7-day unbonding. The branch's `discount_stake_threshold` (100K TOKEN) is vestigial under this spec — see "Fee discount mechanic — removed" below.

**Revenue streams:**

1. **75% of every channel settlement.** Per-byte proportional via router; no pool accounting required on the operator side.
2. **Optional ve-locker pool yield** on any TOKEN the operator ve-locks. Same terms as passive holders.

**Fee discount mechanic — removed.** ADR 004's "stake 100K for 1% fee" is dropped. Replaced conceptually by the ve-locker pool: operators wanting return on non-stake capital ve-lock it and earn from the 12% pool instead of receiving a discount. Simpler, non-regressive, aligns operators with the supply sink rather than against it.

**Sample P&L — 1 Gbps node at 30K GB/mo, gross rate $0.0133/GB:**

```
Gross revenue:                 $400/mo  (30,000 GB × $0.0133/GB)
Router → operator (75%):       $300/mo
Cache-miss paid pulls (15%):   −$45/mo
Infrastructure (mid):          −$90/mo
─────────────────────────────────────
Operator gross profit:         $165/mo

Optional ve-lock of 50K TOKEN for 4y:
  ve-balance:                  50K × 1.0 = 50K veTOKEN
  At mature-scale pool yield (~5.8% USDC APR on locked TOKEN at $0.05):
  Ve-yield:                    ~$12/mo
─────────────────────────────────────
Operator w/ ve-lock:           ~$177/mo
```

Matches the branch's 1 Gbps 30K GB/mo line at unchanged **net** economics. The router's skim is entirely absorbed by the rate increase from $0.01/GB → $0.0133/GB — still 3× cheaper than Bunny.net, 6–15× cheaper than CloudFront/Akamai.

### 2.5 Governance

**Voting power = ve-balance** (not raw TOKEN holdings).

| Parameter | Value | Note |
|---|---|---|
| Proposal threshold | 0.1% of total ve-supply | Prevents spam proposals |
| Quorum | 4% of total ve-supply | |
| Voting period | 7 days | Matches ADR 009 |
| Timelock | 48 hours | Matches ADR 009 |
| Delegation | ve-balance delegatable | Governor Bravo pattern |

Governance weight aligns with lock commitment; traders with no ve-position cannot vote. Auto-ve-lock on vesting means team/seed/treasury vote with their position over the full vesting timeline — their governance influence decays as their tokens unlock, so late-stage governance is weighted toward long-term holders rather than original allocation recipients.

Rest of ADR 009 (safety bounds on governable parameters, emergency multisig with limited pause powers, etc.) unchanged.

### 2.6 Slashing & burn

**Slashing schedule unchanged** from ADR 004 (branch version): 5%/15%/50% escalation tiers, lifetime offense counter, 50/50 burn/challenger distribution, auto-ejection at 50% of min stake, challenge-bond mechanics. No changes — the slashing system works and should not be modified in this spec.

**Buyback-and-burn inflow rate changes only.** 8% of fee inflow → `BuybackBurner` (vs. ADR 004's 20% × 3% = 0.6% effective). All ADR 018 mechanics unchanged: Balancer V3 80/20 TOKEN/USDC pool, MEV protection via TWAP + `minTokenOut`, activation criteria, POL custody.

**Mature-scale burn flow estimate.**

- Baseline: 1,000 nodes × 30K GB/mo × $0.0133/GB = $400K/mo gross revenue
- Burn inflow: 8% × $400K = **$32K/mo USDC = $384K/yr**
- At $0.05 TOKEN: 7.68M TOKEN burned/yr = **1.54%/yr of 500M supply**

**Scaling behavior.** Burn USDC flow scales linearly with network revenue (nodes × GB/node × $/GB). TOKEN-denominated burn scales with revenue and inversely with TOKEN price. If network grows 10× while TOKEN price is flat, per-year supply burn reaches ~15%/yr — genuinely deflationary. If network grows 10× and TOKEN price grows 10× proportionally, burn stays around 1.5%/yr of supply but TOKEN market-cap-destroyed grows 10×. Either trajectory is a working flywheel; which one dominates depends on the ratio of price growth to network growth.

The burn is meaningful at mature scale but still secondary to the supply sink from ve-locks and auto-lock vesting. That's intentional — burn narratives without real yield capture are thin.

---

## 3. Key invariants

1. **Router split must sum to 100%.** Enforced on-chain at every governance update.
2. **ve-locked TOKEN is never slashable.** Ve-position slashing is out of scope, even for operator offenses. Separation of capital risk from operator risk.
3. **No early exit from ve-locks.** No penalty-exit function. Lock-period expiration is the only release path.
4. **Auto-ve-lock is atomic with vesting.** Vested tokens never exist in recipient wallets in unlocked form.
5. **Fixed supply, no minting.** Total supply is 500M at genesis; no function exists on the production token contract to create more.
6. **Node-to-node cache-miss pulls bypass the router.** No skim on internal cost-recovery flow.
7. **Operator's 75% share is paid in the same transaction as settlement.** No claim step, no latency, no separate withdrawal flow for the operator share.

---

## 4. Supersession plan

This spec will be promoted to **ADR 025** after user review. The ADR itself is the durable artifact; this spec is the design-reasoning record.

ADRs affected on acceptance:

| Artifact | Impact |
|---|---|
| ADR 004 — Tokenomics | **Superseded** for: token distribution (§Token Distribution), fee allocation (§Fee Allocation + §BuybackBurner), fee discount mechanic (§Fee Discount), node unit-economics fee-discount row. Retained: staking/slashing schedule, staking-role equivalence, dual-currency rationale. |
| ADR 003 — Payments | Updated: `settleChannel` routes full operator balance to `FeeRouter`, not split at settlement contract. Interface section for `FeeRouter` added. Operator receives their 75% via router transfer, same tx. |
| ADR 009 — Governance | Updated: voting power = ve-balance; quorum/threshold recalibrated against ve-supply rather than total supply. Governor Bravo delegation pattern documented. Safety bounds updated to include fee-router share bounds (Section 2.2). |
| ADR 018 — Liquidity | **Unchanged** for POL mechanics, pool choice, MEV protection. Buyback inflow-rate note added referencing ADR 025's 8% share. |
| ADR 019 — Node onboarding | Updated: bootstrap-fund subsidies auto-ve-lock for 1y on delivery. |
| `finance/notebooks/_shared/params.py` | Updated: `total_supply: 500_000_000`; new `AllocationSplit` dataclass for router shares; `alloc_*` fields repurposed to describe router split (replacing fee-allocation fields); new `VeEscrowParams` dataclass (min/max lock, epoch length, claim window). |
| `finance/notebooks/` | New notebook `09_ve_model.ipynb` modeling ve-lock participation, ve-locker APR vs. lock rate, supply sink projections. `04_token_flows.ipynb` updated to use new split; `02_bootstrap_runway.ipynb` updated for 500M supply and bootstrap-subsidy auto-ve-lock. |

**Production is not yet launched.** This is a pre-launch redesign, not a migration. No holder compensation, no contract migration path, no breaking-change warning needed.

---

## 5. New contracts (implementation scope)

Listed for planning context; detailed interfaces and invariants belong to the implementation plan, not this spec.

- **`FeeRouter`** — receives full channel-settlement USDC; splits 75/12/8/5 atomically; holds ve-locker epoch buckets; exposes `claim(epochs[])` for ve-lockers. Governable share parameters with bounds per Section 2.2.
- **`VotingEscrow`** — ERC-20-lockable ve-position contract with `balanceOfAt(user, ts)`, `create_lock`, `extend_lock`, `withdraw` (only after expiry), and a privileged `create_lock_for(recipient, amount, duration, creator)` callable by authorized vesting and bootstrap-subsidy contracts.
- **`VestingWithAutoLock`** — replaces the current vesting contract family. On vest, calls `VotingEscrow.create_lock_for(recipient, vestedAmount, bucketLockPeriod)` atomically.
- **`BuybackBurner`** — **unchanged from ADR 018** except for inflow source (now `FeeRouter`, not treasury manual transfer).

Modified contracts:

- **`PaymentChannel`** (ADR 003) — `settleChannel` transfers the full operator balance to `FeeRouter.routeSettlement(operator, amount)` instead of splitting at settlement time. `FeeRouter` then transfers operator's 75% and retains the rest.
- **`StakingRegistry`** — `getStakeMultiple` discount-threshold logic is removed from the production contract (not queried by any fee path under this spec). PoC artifacts carrying the method can be left alone since they aren't on the production deployment path.
- **`Governor`** — voting weight source changes from `TOKEN.getPastVotes()` to `VotingEscrow.balanceOfAt()`. Proposal and quorum thresholds recalibrated against `VotingEscrow.totalSupplyAt()`.

---

## 6. Consequences

### Positive

- **Burn flow 13× stronger per unit network revenue** (8% of 100% vs. 20% of 3%); material burn rate at mature scale.
- **Three independent demand sources for TOKEN:** operators (stake-to-operate), yield-seekers (ve-lock for USDC fee share), governance participants (ve-lock for voting). Each source is independent of TOKEN price — they scale with network usage.
- **Auto-ve-lock eliminates vesting-cliff dumps.** Team, seed, treasury, ecosystem all locked into multi-year timelines after vest; no "cliff + dump" opportunity.
- **Operator P&L unchanged at same served volume.** The 25% router skim passes through to client rates via operator pricing; net $/GB to operators is identical to the branch baseline.
- **Single contract owns the economic split.** `FeeRouter` is the governance lever; operator contracts, payment channels, and settlement logic stay stable.
- **ve-governance aligns voting with commitment.** Short-term holders can't govern; long-term holders have proportionally amplified voice.

### Negative

- **Significant new contract surface.** `FeeRouter`, `VotingEscrow`, `VestingWithAutoLock` are all new and non-trivial. Audit burden is substantial.
- **Client-rate perception.** Marketing the network as "3% protocol fee" vs. "25% protocol cut" is optically harder even though the operator keeps the same net $. Requires careful messaging about net operator economics.
- **1 Gbps operators remain margin-thin.** At 20K GB/mo (the branch's "comfortably profitable" threshold), operator gross profit is ~$30/mo under the new model (vs. ~$80/mo pre-rate-adjustment); scale-up to 30K+ GB/mo is now essential for healthy operation. Rate-adjustment is the primary mitigation.
- **Seed-investor negotiations may contest auto-ve-lock.** Traditional term sheets assume liquid positions post-cliff; a 2y auto-ve-lock is non-standard. Should be a negotiated parameter per seed round, not a hard rule. Spec fixes the default; term sheets may deviate.
- **ve-position illiquidity creates Convex-capture risk.** If third-party protocols launch liquid-ve wrappers (Convex/Votium model), they can concentrate governance power. Mitigation: governance should monitor and consider direct treasury incentive programs to keep ve-lockers in the native contract. Out of scope for this spec.
- **ve-locker pool epoch claims add UX overhead.** Lockers must claim each epoch (or batch up to 26 weeks). Non-claim → sweep to treasury. Acceptable UX; could be improved with a claim-aggregator in a later ADR.
- **Effective supply growth still +10.5–11.5%/yr during vesting window** even after S4 + T2 burn. This design compresses the inflation story but does not eliminate it. Long-term deflation requires mature network scale ($1M+/mo fee flow) or further supply-side changes.

### Risks

- **Reflexive bootstrap dependency intensified.** The 200M TOKEN bootstrap fund is now 100M TOKEN (halved with total supply). At $0.01 TOKEN, the fund is worth $1M — tight against the early-production shortfall ($900K for 100 nodes over 12 months per branch). If TOKEN price drops below $0.01 during bootstrap, subsidies don't reach. Mitigation: ADR 004's circuit-breaker trigger (reduce per-node subsidies at 2× runway threshold) remains in force.
- **ve-locker pool accumulates USDC with no floor guarantee.** At early production (few ve-lockers), pool yield per ve-balance is very high (USDC concentrated among few lockers); as participation grows, per-locker yield diminishes even as total flow grows. This is correct economic behavior but may be optically confusing during rollout.
- **Governance capture by auto-lock recipients.** Team + seed + treasury + ecosystem together hold ~70% of supply, all auto-ve-locked on vest. For the first ~2 years they hold dominant governance power. Mitigation: safety bounds on governable parameters (ADR 009) prevent extreme abuse. This is the dominant governance-risk trade-off of the design.

---

## 7. Open questions (deferred to implementation or later ADRs)

1. **Treasury-bucket auto-ve-lock duration.** Current default: 2y, matching team/seed. Alternative: shorter (0–1y) because treasury is protocol-owned and long locks hurt responsiveness. Recommend resolving at implementation-plan time.
2. **ve-locker claim aggregator.** A helper contract that batches claims across multiple epochs for a user with a single transaction. Not required for v1; usability optimization.
3. **Liquid-ve wrapper strategy.** Convex-style wrappers are a known pattern; protocol should decide whether to pre-empt with its own (like Frax's vlCVX) or accept third-party capture. Defer to a governance ADR once the protocol ships.
4. **Rate-advertising UX.** How do probe responses expose the pre-router-skim rate to clients? Should clients see "rate $0.0133/GB" or "rate $0.01/GB + 25% protocol cut"? Client-UX question for the decdn-website or client ADRs, not tokenomics.
5. **Bootstrap-subsidy auto-ve-lock duration.** Current default: 1y. Operators may prefer shorter to maintain cashflow. Could be made per-tranche governable.
6. **Effect on existing ADR 003 `settleChannel` semantics.** The channel close/dispute flow currently assumes operator receives payout in the settlement transaction. Routing via `FeeRouter` keeps this property (the router forwards 75% in the same tx) but the ADR 003 text needs rework to reflect the new call graph.

---

## 8. Acceptance criteria for implementation

Work is done when:

1. `FeeRouter`, `VotingEscrow`, `VestingWithAutoLock` are deployed, unit-tested, and integration-tested against a local Arbitrum fork with representative channel-settlement load.
2. `PaymentChannel.settleChannel` routes to `FeeRouter.routeSettlement` in a single transaction; operator receives 75% in the same tx; remaining splits land in the correct contracts.
3. Governance change to router splits requires 48h timelock and enforces the min/max bounds in Section 2.2.
4. ve-locker `claim(epochs[])` correctly computes pro-rata share from historical ve-balance snapshots; unclaimed epochs past 26-week window sweep to treasury.
5. Vesting contracts call `VotingEscrow.create_lock_for` atomically; no code path releases vested TOKEN unlocked.
6. Governor uses `VotingEscrow.balanceOfAt` for voting weight; proposal/quorum thresholds sourced from ve-supply.
7. ADRs 003, 004, 009, 018, 019 updated per Section 4.
8. `finance/notebooks/_shared/params.py` updated; `04_token_flows.ipynb` and `02_bootstrap_runway.ipynb` re-run and produce sensible output; new `09_ve_model.ipynb` included.
9. No on-chain path bypasses the router for client→node settlement.
10. Invariants in Section 3 are enforced either by contract code or explicit runtime assertions in tests.

---

**End of spec.**
