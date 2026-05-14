# ADR 026: Gauge-Boost Tokenomics

**Date:** 2026-04-25
**Status:** Draft
**Source design spec:** internal `tokenomics-v2-gauge-boost-design` (2026-04-18)
**Economic source of truth:** internal `decdn-economic-model-40-40-gauge-pool` (2026-04-25)

## Context

The economic model — sitting on top of paid byte delivery ([ADR 003](003-payments.md)) and the slashing primitive ([ADR 014](014-on-chain-verification.md)) — has to hold up under four pressures:

1. **A deflationary lever that scales with network usage.** A nominally "deflationary" token model whose burn rate sits well below circulating-supply growth from vesting unlocks is structurally inflationary in practice. Burn must be sized to compete with vesting flows at mature scale.
2. **A real-yield path to token holders.** Passive holders need compensation tied to network usage; long-term lockers need a compensation lever distinct from short-term holders. Without one, governance weight, liquidity provision, and long-term capital formation all weaken.
3. **A progressive operator incentive.** Per-operator return must scale with long-term commitment, not with stake size alone. A flat-rate or regressive mechanic (e.g., a fee discount that grows with raw stake) attracts capital without aligning it.
4. **A TOKEN-price-insulated bootstrap.** Subsidies denominated in the token they're meant to bootstrap collapse in purchasing power exactly when most needed. Bootstrap capital must be denominated in a unit independent of the protocol's own TOKEN price.

This ADR is the canonical economic model addressing all four. Burn is one of several deflationary levers; real yield in TOKEN flows to delegators and ve-lockers; operator compensation differentiates by long-term ve-commitment via a Curve-style gauge boost rather than by a discounted skim percentage; bootstrap is USDC-denominated. Full design reasoning, MEV-defense analysis, equilibrium-stability argument, and reference-implementation pointers live in the source design spec; this ADR is the decision layer.

### Inputs assumed by this ADR

Pre-launch design with no holder-compensation or contract-migration concerns. ~$1M+ pre-seed USDC capital secured (planning target $3M); program structure is operational and tracked separately. 2026 unmetered-bandwidth provider economics per the design spec's input matrix (1 Gbps VPS, 10 Gbps dedicated, 100 Gbps edge tiers); dedicated-bandwidth nodes are realistic at every scale band the protocol is sized for.

Earlier internal drafts explored alternative shapes — a flat protocol-fee skim, a 200M-TOKEN bootstrap fund, a regressive fee-discount mechanic, auto-ve-lock-on-vest. Those are documented in [Alternatives Considered](#alternatives-considered) below.

## Decision

The protocol's economic model is defined by the following sections. Where a table fully duplicates one in the source design spec, this ADR shows the canonical defaults and points to the spec for the surrounding analysis.

### 1. Supply and distribution

**Supply.** 1,000,000,000 TOKEN, fixed at genesis. No post-genesis minting function exists on the production token contract.

#### Burnability

TOKEN is `ERC20Burnable`; any contract may burn TOKEN it holds via `burn` / `burnFrom`. Burns reduce `totalSupply` and emit `Transfer(from, address(0), amount)`. The §8 slashing-burn path uses this; future contract surfaces that need a TOKEN sink integrate via the same standard interface without contract changes.

#### Allocation (1B total)

Six buckets summing to 100%. Vesting profile per the design spec §2.1; effective release rate is ~24%/yr during the active vesting window (Y1–Y3), 16%/yr in Y4, then zero.

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

#### No auto-ve-lock on vest

Vesting contracts release TOKEN unlocked into the recipient's wallet. Locking into `VotingEscrow` is opt-in. Rationale: the gauge-boost mechanism (§2) supplies a strong voluntary economic incentive to ve-lock without forcing long-term alignment via the vesting contract — seed/team term sheets are simpler, and lockers self-select. The cost is a thinner initial veTOKEN base; governance bootstrap may require treasury-funded ve-lock-on-claim airdrops in the first 6–12 months (see §9).

#### No protocol-issued node-bootstrap fund

Bootstrap supply-side incentive is funded externally via $1M+ pre-seed USDC capital, eliminating TOKEN-price reflexivity in subsidy purchasing power. Program structure is operational and tracked separately.

### 2. FeeRouter split (40/40/7/5/5/3)

`FeeRouter` receives the full operator USDC balance from `PaymentChannel.settleChannel` and atomically splits it into six buckets (full mechanic per design spec §2.2). `PaymentChannel` does not skim a protocol fee inline; all bucket distribution happens in `FeeRouter`. The bucket structure (six buckets, the named categories below, sum-to-100% invariant) is fixed at the contract level; **the share percentages themselves are governance-tunable** via `FeeRouter.setShares(...)` per [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics) so the network can launch with a simplified split (e.g. `80/0/0/10/10/0`) and dial up gauge / delegator / safety as their dependency contracts are wired in.

| Destination | Steady-state share | Unit | Distribution mechanic |
| --- | ---: | --- | --- |
| Node base | 40% | USDC | Direct same-tx, per-byte proportional to verified delivery |
| Gauge boost pool | 40% | USDC | Weekly epoch pool; pro-rata by ve-weighted `working_bytes` (§3); pull-based claim |
| Delegator pool | 7% | USDC → TOKEN | TWAP USDC→TOKEN swap; distributed pro-rata by ve-balance to delegators / ve-lockers (§6) |
| Buyback-and-burn | 5% | USDC → TOKEN | Direct same-tx to `BuybackBurner`; mechanics unchanged from [ADR 018](018-liquidity-strategy.md); TOKEN burned |
| Protocol treasury | 5% | USDC | Direct same-tx to Timelock-custodied treasury wallet |
| Safety & insurance reserve | 3% | USDC | Direct same-tx to `SafetyReserve`; governance-gated incident payouts (§5) |
| **Total** | **100%** | | |

The 40/40/7/5/5/3 row above is the **steady-state target**, reached once `VotingEscrow`, `SafetyReserve`, and `DelegatorBuyer` are deployed and governance has executed the corresponding `setShares` proposal under the standard 48h timelock. Inactive buckets (share = 0) accumulate zero with no reverts; same-tx legs short-circuit on the share check, epoch-bucket legs (gauge / delegator) skip the storage write. The launch share configuration is documented in [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics).

#### Aggregate operator-aligned compensation = 80%

(40% direct + 40% gauge pool). The 60% non-base buckets capture deflationary, governance, real-yield-to-lockers, and incident-recourse flows; the 40% gauge pool routes operator yield by long-term ve-commitment rather than by raw byte count.

#### Same-transaction guarantees

The 40% base, 5% burn, 5% treasury, and 3% safety legs all transfer in the settlement transaction. The 40% gauge and 7% delegator buckets accumulate in per-epoch buckets and are claim-based.

#### Node-to-node cache-miss paid pulls bypass the router

Direct peer USDC payment, no skim. Internal cost-recovery flow, not net protocol revenue.

**Gross client rate.** $0.01/GB — at parity with Bunny.net's budget tier and 7–20× cheaper than major traditional CDNs. No deCDN-specific premium. The router's 60% aggregate non-base skim is absorbed by operator net revenue, recovered through gauge-boost yield (§7), TOKEN-economy exposure, and externally-funded pre-seed USDC subsidies — never passed to clients.

#### Epoch mechanics

Epoch length is 1 week (7 × 86400 s, block-timestamp-aligned). At epoch rollover the gauge and delegator buckets freeze, new buckets open, and per-operator `bytes_delivered` counters reset. ve-balance snapshots are taken at the epoch-boundary timestamp via `VotingEscrow.balanceOfAt(user, ts)`. Claim window is 26 epochs (~6 months); unclaimed allocations sweep to the treasury.

#### Pre-launch gauge accumulation

The 40% gauge bucket MUST NOT pay out until the per-operator gauge-share cap from §3 above is enforced — without the cap, the ve-boost factor in §3's `working_bytes` formula admits an effective gauge slice exceeding fee-contribution share, which opens a wash-trading route on self-routed settlements (see §3 [Per-operator gauge-share cap](#per-operator-gauge-share-cap) for the structural defense and trade-offs). This sub-section pins the contract-level mechanism for that pause and the cutover.

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

- While `gaugeLaunched == false`: `routeSettlement` deposits the 40% gauge share into `preLaunchGaugeAccumulator[currentEpoch]` instead of the live gauge bucket. The other five buckets (40% direct, 7% delegator, 5% burn, 5% treasury, 3% safety) flow normally per §2 — the §2 Same-transaction guarantees invariant is preserved end-to-end.
- `claimBoost(epochs[])` reverts on every requested epoch while `gaugeLaunched == false` (no live distribution has occurred). Once `gaugeLaunched == true`, requested epochs in `[0, gaugeLaunchEpoch)` are paid from `preLaunchGaugeAccumulator[epoch]`, and epochs `≥ gaugeLaunchEpoch` are paid from the live gauge bucket — both weighted by the ve-snapshot already taken at each epoch boundary (§Epoch mechanics captures these snapshots regardless of `gaugeLaunched` state; pre-launch epochs reuse them).
- **Cutover** via `enableGauge()`: sets `gaugeLaunched = true`, records `gaugeLaunchEpoch = currentEpoch`, emits `GaugeLaunched(currentEpoch)`. After `enableGauge()` returns, `claimBoost(epochs[])` no longer reverts for any epoch ≥ 0 — pre-launch epochs become claimable from `preLaunchGaugeAccumulator[epoch]`, and the cutover epoch onward routes through the live gauge bucket via the normal §2 path.
- **Partial cutover epoch.** Because `enableGauge()` is `onlyGovernor` and inherits the [ADR 009](009-governance.md) ~9-day governance latency (7-day vote + 48-hour timelock), the cutover transaction lands at an arbitrary block within an epoch. Settlements before the cutover block in that epoch deposit into `preLaunchGaugeAccumulator[gaugeLaunchEpoch]`; settlements after the cutover block route via the live gauge bucket for the same epoch. Both halves credit the same `gaugeLaunchEpoch` and use the same ve-snapshot (taken at the epoch boundary before either half executed), so a claimant for `gaugeLaunchEpoch` receives `(preLaunchGaugeAccumulator[gaugeLaunchEpoch] + liveGaugeBucket[gaugeLaunchEpoch]) × ve_share` — the partial-epoch split is invisible at claim time.
- **Claim window for pre-launch epochs.** The 26-epoch claim window for any epoch `< gaugeLaunchEpoch` starts at `gaugeLaunchEpoch`, not at the original epoch. Unclaimed pre-launch USDC sweeps to treasury after `gaugeLaunchEpoch + 26` per the §Epoch mechanics sweep rule.
- **Empty-snapshot at a pre-launch epoch.** Operators with zero ve at the historical snapshot get zero retroactive claim — this is the intentional shape (gauge rewards long-term ve-commitment, not retroactive attestation). The §3 `sum(working_bytes) == 0` rollover-to-next-epoch rule does **not** apply to pre-launch epochs because the gauge bucket itself was never live during them; un-distributable pre-launch USDC sweeps to treasury via the standard claim-window expiry path, not via §3 rollover.

This pattern is shape-analogous to §3's empty-epoch rollover (gauge bucket parked in `FeeRouter` and claimable later), but the rollover destination differs (treasury sweep on expiry vs next-epoch bucket); readers should not conflate the two.

### 3. Gauge-boost formula

Adapted from Curve Finance's veCRV gauge boost (in production since 2020). Replaces the LP-deposit primitive with verified-bytes-delivered.

Per-operator pool share = `min(working_bytes_i / sum(working_bytes), MAX_GAUGE_SHARE_PER_OPERATOR)`, where `working_bytes_i = min(bytes_i, 0.4·bytes_i + 0.6·(ve_i/total_ve)·total_bytes)` over the epoch's verified bytes (full derivation in design spec §9.4). `bytes_i` is sourced from `FeeRouter.bytesPerEpoch[operator][epoch]` (canonical in [ADR 016 §FeeRouter](016-contract-interactions.md#feerouter)); the per-operator share cap is the binding wash-trading defense.

**Properties:**

- **No ve-lock:** `working = 0.4 × bytes` — the commodity floor. Receives 40% of what a fair-share-ve operator with the same byte count would.
- **Fair-share ve** (`ve_i / total_ve ≥ bytes_i / total_bytes`): `working = bytes_i` — the cap binds. Full proportional share of the pool.
- **Over-ve:** `working` capped at `bytes_i` — no over-boost in the gauge pool. Excess ve still earns from the 7% delegator pool linearly.
- **Maximum boost ratio = 1 / 0.4 = 2.5×** between a max-ve-locker and a zero-ve-locker delivering the same byte count.

#### Degenerate-input fallbacks

(required to prevent division-by-zero at launch and on quiet epochs):

- `total_ve == 0` (no ve-locks exist anywhere — bootstrap window): the `ve_i / total_ve` term is undefined. The contract MUST treat `working_bytes_i = boostFloor × bytes_i = 0.4 × bytes_i` for every operator — every operator receives the commodity floor, share is purely byte-proportional. This is the natural limit of the Curve formula as ve-supply approaches zero.
- `sum(working_bytes) == 0` (no operator delivered any verified bytes in the epoch): the per-operator share is undefined. The epoch's gauge bucket is **not** distributed; it remains in `FeeRouter`'s gauge accumulator and is included in the next epoch's bucket. This is preferred over sweeping to treasury immediately because the empty-epoch case is most likely an outage, not a permanent state — the next active epoch should benefit from the rolled-over USDC. The 26-epoch claim window (§2 Epoch mechanics) caps the total rollover; unclaimed-after-26-epochs USDC sweeps to treasury per the existing rule.
- `bytes_i == 0` (operator delivered nothing this epoch): trivially `working_bytes_i = 0` and that operator's share is `0`. No special-case required — the formula handles this directly.

The Curve formula is bounded by `bytes` in both directions (a non-locker still earns 40% of fair-share, a whale-locker cannot exceed fair-share), which prevents both the "starve commodity operators" and "ve-whale captures the pool" failure modes of simpler `boost = 1 + k × ve` mechanics. The fair-share normalization gives the system a stable equilibrium where operators who match their ve-share to their byte-share collectively neither over- nor under-claim — matching Curve's gauge-equilibrium pattern.

The boost-floor parameter (default `boostFloor = 0.4`) is governable within `[0.2, 0.8]`. A lower floor sharpens the penalty for non-lockers and raises the max boost ratio; a higher floor softens differentiation. See §11 for the safety-bound table.

#### Per-operator gauge-share cap

The per-epoch gauge share for any single operator is capped at `MAX_GAUGE_SHARE_PER_OPERATOR` (default **5%**, governable within `[1%, 25%]` per [ADR 009](009-governance.md) safety bounds). Concretely:

```
share_i = min(working_bytes_i / sum(working_bytes), MAX_GAUGE_SHARE_PER_OPERATOR)
```

Any residual gauge bucket left after capping (which occurs when one or more operators would have received more than the cap) rolls over to the next epoch's gauge accumulator under the same rule as the `sum(working_bytes) == 0` degenerate case in §Degenerate-input fallbacks above. The 26-epoch claim window in §2 Epoch mechanics caps the total rollover.

**Rationale.** Bounds wash-trading payoff at 5% of the gauge bucket per operator-identity. Combined with the boost formula's `0.4·bytes_i` floor for low-ve operators and the closed-pool gauge structure (every settlement contributes to the same global bucket the operator is then claiming from), this makes wash-trading economically marginal at any reasonable TOKEN price — the attacker pays into the pool they're trying to drain, with 8% leakage to treasury+safety per self-deal, and the cap suppresses any non-proportional share they could extract via ve-boost. Sybil expansion of attack-operator count requires fresh `StakingRegistry` registrations each with the §7 minimum stake, converting wash-trading from a heuristic-bypass attack into a stake-proportional capital-lockup attack. A single honest operator with a dominant byte share is also subject to the cap, which is the intended posture — the gauge pool exists to incentivize a diverse operator set, not to reward concentration.

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

#### Historical checkpointing

`VotingEscrow.balanceOfAt(user, ts)` and `totalSupplyAt(ts)` are load-bearing for the epoch-snapshot pattern in §2 and the governance pattern in §8. Per-lock checkpoints; reads O(log n) on the checkpoint array; writes O(1) amortized.

#### Lock ownership

Locks may be held by any address — EOA or contract. Lock creation (`createLock`), amount increase (`increaseAmount`), and time extension (`increaseUnlockTime`) are stable for cross-contract integration. A future contract that holds a pooled lock on behalf of multiple beneficiaries (e.g., a liquid-ve wrapper) integrates as an additive top-level contract via these interfaces without changing `VotingEscrow`.

**Operator stake and ve-positions are separate.** A node's operator stake is held in `StakingRegistry` and is slashable (rates per §8). A ve-position is held in `VotingEscrow` and is not. Neither satisfies the other's requirements; an operator may hold any combination. This separation is a hard invariant — no contract path lets ve-locked TOKEN be slashed.

#### Contract: VotingEscrow

```solidity
interface IVotingEscrow {
    // ─── Lock lifecycle ────────────────────────────────────────────────
    // Lock `amount` of TOKEN until `unlockTime` (absolute, seconds).
    // `unlockTime` is rounded down to the nearest week boundary internally
    // (week-aligned slopes — Curve veCRV pattern). Reverts if the caller
    // already holds a lock, if `unlockTime - block.timestamp` is outside
    // [minLockDuration, maxLockDuration], or if `amount == 0`.
    function createLock(uint256 amount, uint256 unlockTime) external;

    // Add `amount` of TOKEN to the caller's existing lock without changing
    // the unlock time. Reverts if the caller has no active lock or the
    // lock has already expired.
    function increaseAmount(uint256 amount) external;

    // Extend the caller's lock to a later `unlockTime` (absolute, seconds,
    // week-aligned internally). Reverts if `unlockTime` is at or before
    // the current end, if the new remaining duration would exceed
    // `maxLockDuration`, or if the lock has already expired.
    function increaseUnlockTime(uint256 unlockTime) external;

    // Withdraw the full locked TOKEN balance after the lock's unlock time
    // has passed. Lump-sum only — no partial withdrawals. Reverts if the
    // lock has not yet expired (no early-exit penalty path; ve-locked
    // TOKEN never exits early).
    function withdraw() external;

    // ─── Lock view ─────────────────────────────────────────────────────
    // Returns the specified account's lock state: locked amount and
    // unlock time. Returns (0, 0) for addresses that have never locked
    // or have already withdrawn. Single read covers the common "what
    // does this address hold and when does it unlock" query.
    function locked(address account)
        external view returns (uint256 amount, uint256 end);

    // ─── ve-balance ────────────────────────────────────────────────────
    // Current voting weight: amount × remaining_lock_time / maxLockDuration.
    // Decays linearly to zero at the lock's unlock time.
    function balanceOf(address account) external view returns (uint256);

    // Historical voting weight at unix timestamp `ts`. Per-lock checkpoints
    // make this an O(log n) read on the checkpoint array. Load-bearing for
    // the epoch-snapshot pattern in §2 (FeeRouter gauge accounting) and
    // governance vote-weight reads in §9. `ts` may be in the past or
    // present; future timestamps are rejected.
    function balanceOfAt(address account, uint256 timestamp)
        external view returns (uint256);

    // Current total ve-supply (sum of all balanceOf at block.timestamp).
    function totalSupply() external view returns (uint256);

    // Historical total ve-supply at unix timestamp `ts`. Same checkpoint
    // pattern as `balanceOfAt`; used by Governor for quorum calculations
    // calibrated against `VotingEscrow.totalSupplyAt(ts)` per §9.
    function totalSupplyAt(uint256 timestamp) external view returns (uint256);

    // ─── Vote delegation (Governor Bravo pattern) ──────────────────────
    // Delegate the caller's ve-balance voting weight to `delegatee`. The
    // underlying ve-position remains non-transferable; only voting weight
    // is reassigned. Pass `address(0)` to clear delegation (weight reverts
    // to self-delegation by default). See ADR 009 §44.
    function delegate(address delegatee) external;

    // EIP-712 signed delegation, for gasless delegation flows.
    function delegateBySig(
        address delegatee,
        uint256 nonce,
        uint256 expiry,
        uint8 v,
        bytes32 r,
        bytes32 s
    ) external;

    // Returns the address `account` has delegated to, or `account` itself
    // if no delegation has been set (self-delegation is the default).
    function delegates(address account) external view returns (address);

    // ─── Events ────────────────────────────────────────────────────────
    event LockCreated(address indexed account, uint256 amount, uint256 unlockTime);
    event LockIncreased(address indexed account, uint256 addedAmount, uint256 newAmount);
    event LockExtended(address indexed account, uint256 oldUnlockTime, uint256 newUnlockTime);
    event Withdrawn(address indexed account, uint256 amount);
    event DelegateChanged(address indexed delegator, address indexed fromDelegate, address indexed toDelegate);
    event DelegateVotesChanged(address indexed delegate, uint256 previousBalance, uint256 newBalance);
}
```

**Notes:**

- `createLock` is one-lock-per-address.
- `getVotes` and `getPastVotes` are omitted; voting weight is read via `balanceOfAt(user, ts)` and `totalSupplyAt(ts)` because ve-weight is a function of timestamp (linear decay), not block number.
- Delegation reassigns voting weight but not the underlying ve-position; locks remain non-transferable per the §4 invariant. Delegation events follow OZ Governor Bravo.

### 5. Safety and insurance reserve (3% bucket)

The 3% safety bucket is held in `SafetyReserve`, a governance-gated incident reserve. Eligible payout categories per design spec §2.2.5:

- Incorrect slashing / appeal reversals.
- Relay, sequencer, or payment-channel downtime.
- Bad-data incidents where user recourse is more valuable than pure burn.
- Future incident-response contracts that integrate via the stable `payout(bundleHash, recipient, amount)` interface.

#### Spending controls

Disbursements require all of:

1. An attested incident bundle (cryptographic evidence of the failure, identity of the harmed party, proposed payout amount).
2. A governance proposal, or fast-track multisig approval (within hard caps per [ADR 009](009-governance.md)).
3. A 48-hour appeal window during which the bundle is challengeable on-chain.
4. **Post-incident reporting.** On payout settlement, `SafetyReserve` writes an immutable record to its public on-chain registry (see [ADR 009 § SafetyReserve Payout Authorization](009-governance.md#safetyreserve-payout-authorization) for the record fields and reporting obligations).

No path exists for unattested payouts; the `payout(bundleHash, recipient, amount)` entry point checks all four gates. Sizing analysis (number of $100K and $1M incidents covered per year per scenario) lives in the economic-model spec §7; this ADR does not duplicate the table.

#### Cross-category payout ordering

When `SafetyReserve` solvency is insufficient to immediately fund every authorized disbursement — most plausibly during a correlated-outage window combining slash-restitution appeals (per [ADR 028 §5](028-slashing-appeals.md#5-hard-caps-and-frequency-limits)) with concurrent SLA-breach payouts — the unfunded portion of each authorization is recorded as a *pending claim* and disbursed once solvency permits. The queue is keyed on `(accrualEpoch asc, claimId asc)`:

- **`accrualEpoch`** is the FeeRouter 1-week epoch ([§2 Epoch mechanics](#epoch-mechanics)) in which the original `payout()` authorization first hit insolvency. SafetyReserve does not maintain a separate epoch clock; using the FeeRouter epoch keeps `accrualEpoch` derivable from any block timestamp without an additional canonical clock.
- **`claimId`** is a monotonic `uint256` counter assigned by `SafetyReserve` at authorization time, incremented atomically as each pending claim is recorded. It is the within-epoch tiebreaker — not a payout-category priority signal, just a deterministic disambiguator for the rare case of multiple claims accruing in the same epoch.

The queue ordering is therefore **epoch-FIFO across all payout categories with a per-claim monotonic tiebreaker within an epoch**. Three properties follow:

- **No payout category has cross-category priority.** Slash-appellants, SLA-breach claimants, and future incident-response integrations all enter the same queue keyed by accrual epoch and `claimId`; no constituency is privileged. The `claimId` tiebreaker is protocol-monotonic, not category-coded — it does not assert that any payout category is preferred over another.
- **No multisig-as-orderer hazard.** Authorization order (the sequence in which `SafetyReserve.payout()` is called or the multisig fast-tracks an appeal) determines `claimId` only in the rare same-epoch tie, and even then only deterministically; once authorized, queue position is fixed.
- **Forward-compatible with new payout categories.** Future incident-response contracts integrating via the stable `payout(bundleHash, recipient, amount)` interface inherit the same queue semantics without amending this ADR.

**Disbursement of queued claims is permissionless.** Once the original `payout()` authorization completes — gates 1–3 of the four [Spending controls](#spending-controls) (attested bundle, authorization, 48-hour appeal window) were checked at authorization; gate 4 (post-incident reporting) writes atomically on each disbursement — the claim is in the queue and any caller may invoke a `disbursePending()` head-of-queue path when reserve solvency permits. No second-stage authorization is required, which is what makes the queue-ordering guarantee meaningful: the multisig cannot selectively re-authorize favored queued claims because no re-authorization step exists. This mirrors the permissionless-detection pattern in [Appendix: Fraud Detection](appendix-fraud-detection.md). The exact storage shape and the `disbursePending` entry-point signature are pinned in a future SafetyReserve contract-implementation ADR (tracked at [#524](https://github.com/decdn/decdn/issues/524)); this section pins only the ordering semantics and the permissionless-disbursement property.

#### Interface stability

The `payout(bundleHash, recipient, amount)` signature is contract-stable: future incident-response tooling, insurance products, and SLA-style contracts integrate via this entry point without contract changes. Evidence formats live off-chain and are referenced by hash on-chain; the contract enforces the four payout gates uniformly regardless of caller identity (subject to `AccessControl` role grants per [ADR 016 §5](016-contract-interactions.md#5-access-control-matrix)). `payout()` is the AccessControl-gated authorization path; `disbursePending()` is permissionless by design (see [Cross-category payout ordering](#cross-category-payout-ordering)) and inherits its evidence-and-gates guarantees from the original `payout()` authorization that placed the claim on the queue.

#### Contract: SafetyReserve

```solidity
interface ISafetyReserve {
    // ─── Payouts ──────────────────────────────────────────────────────
    // Single entry point for incident disbursements. Payouts are USDC-only
    // by design — TOKEN inflow from the 30% slashing redirect is swapped
    // to USDC via `swapAccumulatedTokens` before becoming available here.
    // The `bundle` hash references an off-chain attested incident bundle;
    // the contract enforces the four payout gates uniformly:
    //   1. Attested bundle (cryptographic evidence)
    //   2. Authorization (Governor or emergency-multisig within hard caps)
    //   3. 48h appeal window since the bundle was first surfaced
    //   4. Post-incident registry write (atomic with disbursement)
    // Reverts if any gate fails. Returns the assigned incident id.
    function payout(
        bytes32 bundle,
        address recipient,
        uint256 usdcAmount
    ) external returns (uint256 id);

    // ─── Incident registry ────────────────────────────────────────────
    enum IncidentReason {
        OutageRestitution,
        SlashAppealRatification,
        ProtocolHack,
        MisattributionFix,
        Other
    }

    struct Incident {
        bytes32 bundle;          // attested evidence hash
        address recipient;       // payout target
        uint256 usdcAmount;      // USDC base units (6 decimals)
        uint64 paidAt;           // block timestamp
        address paidBy;          // Governor or emergency-multisig that authorized
        IncidentReason reason;   // categorical tag for indexers and audit
    }

    function incidents(uint256 id) external view returns (Incident memory);
    function incidentCount() external view returns (uint256);

    // ─── Slashing-redirect inflow (callback from StakingRegistry) ─────
    // Records the 30% slashed-TOKEN redirect against an indexable
    // operator+amount tuple. `SLASH_INFLOW_REPORTER_ROLE`-gated; granted
    // to `StakingRegistry` post-deploy per [ADR 016 § Post-Deployment
    // Initialization](016-contract-interactions.md#post-deployment-initialization).
    // The TOKEN itself is transferred separately via `safeTransfer`;
    // this call is the indexable accounting event.
    function recordSlashInflow(address operator, uint256 amount) external;

    // ─── TOKEN → USDC swap (keeper) ───────────────────────────────────
    // Swaps `amountIn` of accumulated TOKEN to USDC against the
    // [ADR 018](018-liquidity-strategy.md) Balancer V3 80/20 pool — same
    // Vault-scoped self-approval, TWAP, `minOut`, and per-epoch
    // liquidity-cap defenses as `BuybackBurner`. Per-call batch shape
    // (rather than full-balance) lets keepers MEV-sequence across
    // multiple sub-swaps. `KEEPER_ROLE`-gated. `amountIn` is bounded by
    // contract-level `min/maxBatchAmount` parameters.
    function swapAccumulatedTokens(uint256 amountIn, uint256 minOut) external;

    // ─── Slash-appeal extensions (per ADR 028) ────────────────────────
    // Signature stubs only; full appeal state machine, window timing,
    // storage layout, per-appeal escrow accounting, and event-parameter
    // semantics are specified in
    // [ADR 030](030-safety-reserve-appeals-contract.md) and
    // [ADR 028 §6](028-slashing-appeals.md#6-contract-surface);
    // parameter values lock down in #451.
    function openSlashAppeal(uint256 slashId, bytes32 evidenceBundleHash)
        external returns (uint256 appealId);
    function fastTrackAppeal(uint256 appealId) external;
    function rejectAppeal(uint256 appealId) external;
    function ratifyAppeal(uint256 appealId) external;
    function reverseAppeal(uint256 appealId) external;
    function cleanupExpiredAppeal(uint256 appealId) external;

    // ─── Governance setters ───────────────────────────────────────────
    function setGovernor(address newGovernor) external;
    function setEmergencyMultisig(address newMultisig) external;
    function setAppealWindow(uint64 seconds_) external;
    function setMinBatchAmount(uint256 amount) external;
    function setMaxBatchAmount(uint256 amount) external;
    function setPool(address newPool) external;
    function setSlippageToleranceBps(uint256 bps) external;

    // ─── Pause control ────────────────────────────────────────────────
    // `pause()` blocks `payout` and `swapAccumulatedTokens`;
    // `recordSlashInflow` continues to work so slashing accounting is
    // never lost during a pause window.
    function pause() external;
    function unpause() external;

    // ─── Events ───────────────────────────────────────────────────────
    event Paid(
        uint256 indexed id,
        address indexed recipient,
        uint256 usdcAmount,
        bytes32 bundle,
        address paidBy,
        IncidentReason reason
    );
    event SlashInflowRecorded(address indexed operator, uint256 amount);
    event SwapExecuted(uint256 amountIn, uint256 amountOut);
    // Parameter lists pinned in
    // [ADR 030 §3](030-safety-reserve-appeals-contract.md#3-solidity-event-signatures-all-six-pinned).
    event SlashAppealOpened(uint256 indexed appealId, uint256 indexed slashId, address indexed appellant, bytes32 evidenceBundleHash, uint256 bond);
    event SlashAppealFastTracked(uint256 indexed appealId, uint256 escrowAmount);
    event SlashAppealRejected(uint256 indexed appealId, uint256 bondSlashed);
    event SlashAppealRatified(uint256 indexed appealId, address indexed recipient, uint256 restitutionAmount);
    event SlashAppealReversed(uint256 indexed appealId, uint256 escrowReturned, address bondSplitRecipient, uint256 bondSplitAmount);
    event SlashAppealLapsed(uint256 indexed appealId, uint256 escrowReturned, uint256 bondRefunded);
    event GovernorUpdated(address indexed oldAddr, address indexed newAddr);
    event EmergencyMultisigUpdated(address indexed oldAddr, address indexed newAddr);
    event AppealWindowUpdated(uint64 oldValue, uint64 newValue);
    event PoolUpdated(address indexed oldPool, address indexed newPool);
    event SlippageToleranceUpdated(uint256 oldBps, uint256 newBps);
    event MinBatchAmountUpdated(uint256 oldValue, uint256 newValue);
    event MaxBatchAmountUpdated(uint256 oldValue, uint256 newValue);
}
```

**Notes:**

- **USDC-only payouts.** `usdcAmount` is named explicitly so the constraint is visible in the storage layout and on every `Paid` event. If a future ADR ever motivates multi-currency payouts, the additive shape is a `tokenOut` field plus an allowlist setter — no breaking change to existing `Incident` storage.
- **Governor and emergency-multisig addresses are governance-mutable.** The `setGovernor` / `setEmergencyMultisig` setters allow the eventual handover from the deployer EOA to `TimelockController` (per [ADR 016 § Post-Deployment Initialization](016-contract-interactions.md#post-deployment-initialization)) and any future re-pointing without contract redeployment. The 48h timelock constraint applies via `GOVERNANCE_ROLE`.
- **Appeal extensions are signature stubs.** This interface pins the function names and parameter types; the full state machine (`Open` → `FastTracked` / `Rejected` → `Ratified` / `Reversed` / `Lapsed`), window timing (filing, multisig review, ratification), and bond/restitution caps live in [ADR 028 §6](028-slashing-appeals.md#6-contract-surface) and are surface-locked under #451.

### 6. Delegator pool — USDC → TOKEN conversion

The 7% delegator bucket flows through a USDC→TOKEN buy-and-distribute pipeline rather than direct USDC distribution.

1. `FeeRouter` accumulates 7% of routed USDC into the delegator-pool epoch bucket per epoch.
2. At epoch rollover (or via keeper trigger within the epoch), the bucket's USDC is swapped for TOKEN against the Balancer V3 80/20 pool ([ADR 018](018-liquidity-strategy.md)) under the same TWAP + minOut + per-epoch liquidity-cap protections as `BuybackBurner`. Implementation is a parallel `DelegatorBuyer` contract per [ADR 016 § Shared swap helper](016-contract-interactions.md#shared-swap-helper-buybackburner--delegatorbuyer); the two contracts share the swap execution path through an internal `BalancerV3SwapHelper` abstract contract while preserving distinct downstream destinations and governance setters.
3. The acquired TOKEN is held in the delegator-pool epoch bucket as TOKEN.
4. Delegators / ve-lockers call `FeeRouter.claimDelegator(epochs[])`. Payout per locker = `ve_i / total_ve_at_epoch_boundary × token_in_delegator_bucket[epoch]`.

#### Distinction from buyback-and-burn

Both are buy-side market pressure on USDC→TOKEN. Burn removes TOKEN from circulation; the delegator pool routes TOKEN to long-term ve-locked holders. Both are required.

#### Why TOKEN-denominated, not USDC?

Routes acquired TOKEN to the participants with the longest commitment horizon and couples ve-locker yield to TOKEN value rather than to network revenue alone — when network revenue grows, TOKEN buy pressure grows, ve-locker positions appreciate. This is the model's primary "real yield in TOKEN" lever; an alternative pattern (USDC distribution to a passive ve-pool) is documented in [Alternatives Considered](#alternatives-considered).

#### MEV / slippage

TWAP windows + per-epoch liquidity caps + private-RPC routing (Flashbots-style bundles) for the swap. Same defenses as the [ADR 018](018-liquidity-strategy.md) buyback flow; per-epoch liquidity caps are a hard requirement on this path, not optional.

#### Contract: DelegatorBuyer

```solidity
interface IDelegatorBuyer {
    // ─── Per-epoch USDC → TOKEN swap (FeeRouter-only) ─────────────────
    // Called by `FeeRouter.executeDelegatorSwap`. `msg.sender == feeRouter`
    // is the only authorization check — DelegatorBuyer is a single-purpose
    // helper trusting FeeRouter exclusively, so no role grants are needed
    // post-deploy. Swaps `amountIn` USDC for at least `minOut` TOKEN
    // against the configured Balancer V3 pool, then deposits the
    // resulting TOKEN back to FeeRouter via
    // `IFeeRouter.depositDelegatorTokens(epoch, amount)` — see
    // [ADR 016 § Contract: FeeRouter](016-contract-interactions.md#contract-feerouter).
    // Same Vault-scoped self-approval pattern as `BuybackBurner`.
    function swapDelegatorBucket(
        uint64 epochId,
        uint256 amountIn,
        uint256 minOut
    ) external;

    // ─── Read views ───────────────────────────────────────────────────
    function feeRouter() external view returns (address);
    function pool() external view returns (address);
    function slippageToleranceBps() external view returns (uint256);
    function minSwapAmount() external view returns (uint256);
    function maxSwapAmount() external view returns (uint256);

    // ─── Governance setters ───────────────────────────────────────────
    function setFeeRouter(address newFeeRouter) external;
    function setPool(address newPool) external;
    function setSlippageToleranceBps(uint256 bps) external;
    function setMinSwapAmount(uint256 amount) external;
    function setMaxSwapAmount(uint256 amount) external;

    // ─── Pause control ────────────────────────────────────────────────
    function pause() external;
    function unpause() external;

    // ─── Events ───────────────────────────────────────────────────────
    event DelegatorSwapped(uint64 indexed epochId, uint256 amountIn, uint256 amountOut);
    event FeeRouterUpdated(address indexed oldRouter, address indexed newRouter);
    event PoolUpdated(address indexed oldPool, address indexed newPool);
    event SlippageToleranceUpdated(uint256 oldBps, uint256 newBps);
    event MinSwapAmountUpdated(uint256 oldValue, uint256 newValue);
    event MaxSwapAmountUpdated(uint256 oldValue, uint256 newValue);
}
```

**Notes:**

- **Parallel contract to `BuybackBurner`** per [ADR 016 § Shared swap helper](016-contract-interactions.md#shared-swap-helper-buybackburner--delegatorbuyer). The two contracts share the Balancer V3 swap execution path through an internal `BalancerV3SwapHelper` abstract contract while keeping separate addresses, separate governance setters on `FeeRouter`, and divergent downstream value flows (burn vs deposit-back).
- **`msg.sender == feeRouter` as the sole auth check.** No `KEEPER_ROLE` on `DelegatorBuyer` because there are no other legitimate callers — keepers trigger swaps via `FeeRouter.executeDelegatorSwap(epoch, minOut)` (which holds `KEEPER_ROLE` on `FeeRouter`), and `FeeRouter` then calls `swapDelegatorBucket` here. Single trust boundary; one role grant fewer post-deploy.
- **`setFeeRouter` carve-out** matches the [ADR 016 § No proxy deployment patterns](016-contract-interactions.md#no-proxy-deployment-patterns) carve-out for non-signing helper addresses: `DelegatorBuyer` has no domain-separator-bound state, so re-pointing the configured `FeeRouter` is safe under the standard 48h timelock.

### 7. Operator economics and minimum stake

**Minimum stake.** **50,000 TOKEN.** Slashable (rates per §8), 7-day unbonding, slashable during unbonding. Sized so operator stake is a meaningful skin-in-the-game floor while keeping the gauge-boost ve-position the differentiating capital channel — the two roles are split cleanly.

#### No fee-discount mechanic

Operator yield differentiates by long-term ve-commitment via the gauge boost (§3), not by stake-multiple-keyed fee discounts. A discount-on-stake pattern is documented in [Alternatives Considered](#alternatives-considered).

#### Revenue streams

(per design spec §2.4):

1. **40% of every channel settlement** — direct USDC, same-tx, per-byte.
2. **Share of the 40% gauge-boost pool** — USDC, weekly distribution, weighted by `working_bytes`. Non-ve-lockers receive ~40% of fair-share; max-ve-lockers receive 100% of fair-share (2.5× more per byte than non-lockers).
3. **Optional delegator-pool yield** (TOKEN-denominated) on any TOKEN they ve-lock. Disjoint from the gauge pool; uncapped relative to byte share.

#### Sample 1 Gbps node P&L

(full multi-scenario model, including absolute figures and the S0–S3 × node-type-A–E unmetered-infra cost matrix, lives in design spec §2.4 / §3). Qualitative shape: fair-share ve materially out-earns no-ve at the reference 30K GB/mo node (the commodity operator is positive but thin and is the design's intended filter); over-ve is gauge-flat and earns its marginal yield via the delegator pool. Externally-funded operator-onboarding programs soften the filter for new operators.

### 8. Slashing and burn

**Slashing rates.** 5% / 15% / 50% escalation tiers, lifetime offense counter (`uint32`, monotonically increasing), increasing reset periods, auto-ejection at 50% of minimum stake, challenge-bond mechanics.

**Slashing distribution.** **50% challenger / 30% SafetyReserve / 20% burn.** The challenger share is the deterrent that pays for active enforcement; the SafetyReserve share funds user-harm incident recourse beyond pure deflation; the burn share preserves the deflationary deterrent at a level governance can recalibrate within §11 bounds. Pure-burn variants are documented in [Alternatives Considered](#alternatives-considered).

**Buyback-and-burn inflow.** 5% of routed USDC flows to `BuybackBurner` from `FeeRouter`. [ADR 018](018-liquidity-strategy.md) mechanics (Balancer V3 80/20 swap, TWAP, `minTokenOut`, POL custody) are inherited.

**Mature-scale burn estimate.** ~0.3–0.4%/yr of 1B supply at S2 reference scale (full burn-vs-vesting and burn-sensitivity tables across S0–S3 × $0.001–$1.00 TOKEN price live in economic-model spec §§4–5).

#### Operational constraint

Burn must be TWAP-limited and liquidity-aware. Mature burn budgets can exceed available market depth, especially at low TOKEN prices (per economic-model spec §4, S2/S3 are the regimes where liquidity caps bind).

### 9. Governance

#### Voting weight = ve-balance

(not raw TOKEN holdings). Sourced from `VotingEscrow.balanceOfAt(user, ts)` rather than `TOKEN.getPastVotes()`. Quorum and threshold are calibrated against `VotingEscrow.totalSupplyAt(ts)`.

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

Bootstrap supply-side incentive is **$1M+ pre-seed USDC capital** (planning target: $3M), externally raised. USDC denomination insulates subsidy purchasing power from TOKEN price. The protocol commits to the funding mechanism (USDC, externally raised) and the size floor ($1M); the operational program structure (allocation across operator-recruitment programs, eligibility, success metrics, governance flow) is tracked separately as a foundation/team operational concern, not as a protocol decision.

[ADR 019](019-node-onboarding.md) is the canonical onboarding flow.

### 11. Governable parameters with safety bounds

Router shares, the boost-floor parameter, the per-operator gauge-share cap, and the claim window are governable, gated by 48-hour timelock per [ADR 009](009-governance.md), and bounded as below. Sum-to-100% across the six router shares is enforced on every governance update; updates that violate the sum or exceed any individual bound revert.

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

The 20% floor on the node-base share guarantees operators always receive enough liquid USDC to cover at least a meaningful fraction of infrastructure costs even under extreme governance proposals — preserves the cashflow invariant. The `boostFloor` bounds prevent governance from collapsing the gauge pool to a winner-take-all distribution (lower-bound) or flattening it into uselessness (upper-bound). The `MAX_GAUGE_SHARE_PER_OPERATOR` bounds prevent governance from disabling the wash-trading defense (lower bound implicitly enforced by the cap being non-zero) or so over-tightening that legitimate large operators are starved (upper bound). `epochLiquidityCapFraction` is the combined per-epoch ceiling on USDC notional swapped through the Balancer V3 80/20 pool across `BuybackBurner` and the delegator-pool swap path. The 1% floor prevents governance from starving the swap paths; the 30% ceiling prevents a single epoch from draining pool depth; the 10% default sizes one epoch's combined pressure conservatively against worst-case sustained execution. The cap is a single pool-wide budget per [ADR 018 § Liquidity-cap interaction](018-liquidity-strategy.md#liquidity-cap-interaction).

**Non-numeric one-shot setters.**

| Setter | Effect | Reversibility |
| --- | --- | --- |
| `enableGauge()` | Flips `gaugeLaunched = false → true`, records `gaugeLaunchEpoch`, emits `GaugeLaunched`. Activates the live gauge bucket from `gaugeLaunchEpoch` onward; pre-launch escrow becomes claimable from `gaugeLaunchEpoch` against the historical ve-snapshots already taken at each pre-launch epoch boundary (per §2 Pre-launch gauge accumulation). | One-shot, irreversible. The pre-launch state is launch-only — there is no `disableGauge()`. |

`enableGauge()` is governable per [ADR 009](009-governance.md), inherits the `AccessControl` role-gating from §11 Setter contract-level bound enforcement, and has no numeric bound (binary state).

#### Setter contract-level bound enforcement

Parameter setters on `FeeRouter` and `VotingEscrow` are role-gated via `AccessControl` and bound-checked at the contract level — bounds are enforced regardless of caller. A future automated controller granted the parameter-setter role operates within the same bounds; out-of-range writes revert. This makes the bounds above effective for any caller (governance proposals or additive controllers), without trusting the caller to self-clamp.

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

- **Significant contract surface.** `FeeRouter` (with two pool types and the delegator-swap path), `VotingEscrow`, `SafetyReserve`, and the optional `DelegatorBuyer` add meaningful audit burden. The §2 Pre-launch gauge accumulation adds three storage slots (`gaugeLaunched`, `gaugeLaunchEpoch`, the `preLaunchGaugeAccumulator` mapping), one one-shot governance setter (`enableGauge()`), and one event (`GaugeLaunched`) on top of the existing `FeeRouter` surface — a small but non-zero increment that audit must include.
- **Per-epoch byte accounting adds gas.** Every settlement increments an operator's byte counter — 5K–15K gas on top of router forwarding. Minor but non-zero; needs validation on the chosen L2 (see [Appendix: L2 Deployment](appendix-l2-deployment.md)).
- **Commodity operators face thin margins.** Operators who refuse to ve-lock see lower margins than fair-share-ve operators. This is the designed incentive pressure, but the failure mode is under-supply of operators if the filter is too sharp. Externally-funded operator-onboarding programs partially offset.
- **Governance bootstrap depends on voluntary locking.** Initial veTOKEN supply tracks self-locking decisions; first 6–12 months may need treasury-funded lock incentives.
- **Delegator-pool swap adds keeper dependency.** USDC→TOKEN conversion needs a keeper trigger (or fold into `BuybackBurner`'s existing keeper). Not a new failure mode — [ADR 018](018-liquidity-strategy.md) already has keeper dependency — but it expands the keeper's responsibilities.
- **Load-bearing math is harder to explain.** The Curve formula and the delegator-conversion mechanic are not intuitive to casual readers. UI, documentation, and operator dashboards need to expose "your boost factor," "your delegator-pool TOKEN earnings," and "delegator-pool slippage" clearly.
- **Effective supply growth ~24%/yr during vesting window.** With ve-locking opt-in, the model relies on burn flow plus scenario-driven revenue growth to outweigh release pressure. Per economic-model spec §4, burn dominates monthly vesting only at S2+ at $0.05/TOKEN.

### Risks

- **Equilibrium fragility.** The Curve-style model converges to a stable equilibrium *if* the boost is valuable enough to lock for but not so valuable that a winner-take-all dynamic emerges. The 40% gauge-pool default is sized in the middle by reasoned default; production tuning may be needed.
- **Reflexive operator-margin layer.** TOKEN price drop → ve-lock value drops → fair-share-ve margins shrink → operators unwind commitment. Pre-seed USDC insulates the *funding* side; the *operator-recruitment* side still depends on TOKEN price for ve-incentive strength. Mitigated, not eliminated.
- **Delegator-conversion MEV risk.** TWAP + private-RPC routing mitigates front-running, but the swap is observable on-chain post-fact. Flashbots-style bundles and per-epoch liquidity caps are required on this path, not optional. Keeper-cost economics under L2 gas conditions ([Appendix: L2 Deployment](appendix-l2-deployment.md)) need validation.
- **Wash-trading / self-routed traffic.** An operator could induce noise settlements to inflate gauge-pool share. The defense is the **per-operator gauge-share cap** from §3 above (`MAX_GAUGE_SHARE_PER_OPERATOR`, default 5%, governable `[1%, 25%]`), bounding the wash-trading payoff per operator-identity. Combined with the closed-pool gauge bucket (every settlement contributes to the same pool the operator is then claiming from) and the 8% treasury+safety leakage per self-deal, this makes wash-trading economically marginal at any reasonable TOKEN price. The launch prerequisite is contract-pinned in §2 Pre-launch gauge accumulation: `gaugeLaunched == false` escrows the 40% gauge bucket per epoch, and the one-shot `enableGauge()` setter is the only path to live gauge payouts.
- **Governance-weight concentration.** Operators who lock heavily for boost also accumulate disproportionate governance weight. [ADR 009](009-governance.md) safety bounds prevent extreme abuse; team / seed / treasury vesting acts as a counterweight during the first ~3 years.
- **Convex-capture risk.** Third-party liquid-ve wrappers (Convex / Votium / Aura analogs) can concentrate governance power outside the DAO. Mitigation is operational — the DAO may ship a native liquid-ve wrapper as an additive top-level contract (integrating with `VotingEscrow` via the standard lock-creation / increase-amount / snapshot interfaces per §4) without changing the launch contract surface.
- **20% burn share deterrence.** A higher burn share would weight slashing more toward pure deflation; the chosen 50/30/20 distribution prefers user-harm recourse via `SafetyReserve`. The §11 safety bound on the burn share leaves room for governance recalibration; security review should confirm 20% preserves slashing's deterrent value.

## Alternatives Considered

The five tokenomics shapes evaluated against this design (original 3%-flat / stake-multiple-discount / 200M-TOKEN-bootstrap / 50-50-burn shape, auto-ve-lock-on-vest, USDC distribution to a passive ve-pool, pure-deflationary slashing, TOKEN-denominated bootstrap fund) are recorded in [`_history/alternatives-pre-launch.md` § ADR 026 — Gauge-Boost Tokenomics](_history/alternatives-pre-launch.md#adr-026--gauge-boost-tokenomics).

## Forward references (follow-up ADRs)

(none currently outstanding — wash-trading defense is the per-operator gauge-share cap from §3, with the cap-enforcement launch prerequisite contract-pinned in §2.)
