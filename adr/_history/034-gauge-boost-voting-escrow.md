# ADR 034: Gauge Boost and Voting Escrow

> **Status:** Retired 2026-05-25 by [spec v2.1 (work-token rewrite)](../../docs/superpowers/specs/2026-05-24-work-token-tokenomics-redesign-v2.1.md). The gauge-boost formula, `VotingEscrow` contract, and per-operator gauge-share cap are replaced by the CapacityBond lock-to-capacity curve (`bond = k × Mbps^α`) in [ADR 026](../026-tokenomics.md#adr-026-tokenomics). Operator-yield differentiation now flows from capital-cost-to-operate rather than yield-haircut-to-not-lock. Original ADR body preserved verbatim below for historical reference; do not link to from canonical ADRs.

**Date:** 2026-04-25
**Status (pre-retirement):** Draft

## Context

The gauge-boost mechanism and the `VotingEscrow` contract are the operator-incentive core of the 40% gauge bucket in the `FeeRouter` six-bucket split ([ADR 026 § FeeRouter split (40/40/7/5/5/3)](026-tokenomics.md#feerouter-split-40407553)). This ADR specifies the gauge-boost formula (including degenerate-input fallbacks and the per-operator gauge-share cap that is the canonical wash-trading defense) and the vote-escrow contract (historical checkpointing, lock ownership, the `VotingEscrow` interface). The economic-model umbrella that sizes and ties the buckets together is [ADR 026](026-tokenomics.md#adr-026-tokenomics).

## Decision

### Gauge-boost formula

Adapted from Curve Finance's veCRV gauge boost (in production since 2020). Replaces the LP-deposit primitive with verified-bytes-delivered.

Per-operator pool share = `min(working_bytes_i / sum(working_bytes), MAX_GAUGE_SHARE_PER_OPERATOR)`, where `working_bytes_i = min(bytes_i, 0.4·bytes_i + 0.6·(ve_i/total_ve)·total_bytes)` over the epoch's verified bytes. `bytes_i` is sourced from `FeeRouter.bytesPerEpoch[operator][epoch]` (canonical in [ADR 016 § FeeRouter](016-contract-interactions.md#feerouter)); the per-operator share cap is the binding wash-trading defense.

**Properties:**

- **No ve-lock:** `working = 0.4 × bytes` — the commodity floor. Receives 40% of what a fair-share-ve operator with the same byte count would.
- **Fair-share ve** (`ve_i / total_ve ≥ bytes_i / total_bytes`): `working = bytes_i` — the cap binds. Full proportional share of the pool.
- **Over-ve:** `working` capped at `bytes_i` — no over-boost in the gauge pool. Excess ve still earns from the 7% delegator pool linearly.
- **Maximum boost ratio = 1 / 0.4 = 2.5×** between a max-ve-locker and a zero-ve-locker delivering the same byte count.

#### Degenerate-input fallbacks

(required to prevent division-by-zero at launch and on quiet epochs):

- `total_ve == 0` (no ve-locks exist anywhere — bootstrap window): the `ve_i / total_ve` term is undefined. The contract MUST treat `working_bytes_i = boostFloor × bytes_i = 0.4 × bytes_i` for every operator — every operator receives the commodity floor, share is purely byte-proportional. This is the natural limit of the Curve formula as ve-supply approaches zero.
- `sum(working_bytes) == 0` (no operator delivered any verified bytes in the epoch): the per-operator share is undefined. The epoch's gauge bucket is **not** distributed; it remains in `FeeRouter`'s gauge accumulator and is included in the next epoch's bucket. This is preferred over sweeping to treasury immediately because the empty-epoch case is most likely an outage, not a permanent state — the next active epoch should benefit from the rolled-over USDC. The 26-epoch claim window ([ADR 026 § Epoch mechanics](026-tokenomics.md#epoch-mechanics)) caps the total rollover; unclaimed-after-26-epochs USDC sweeps to treasury per the existing rule.
- `bytes_i == 0` (operator delivered nothing this epoch): trivially `working_bytes_i = 0` and that operator's share is `0`. No special-case required — the formula handles this directly.

The Curve formula is bounded by `bytes` in both directions (a non-locker still earns 40% of fair-share, a whale-locker cannot exceed fair-share), which prevents both the "starve commodity operators" and "ve-whale captures the pool" failure modes of simpler `boost = 1 + k × ve` mechanics. The fair-share normalization gives the system a stable equilibrium where operators who match their ve-share to their byte-share collectively neither over- nor under-claim — matching Curve's gauge-equilibrium pattern.

The boost-floor parameter (default `boostFloor = 0.4`) is governable within `[0.2, 0.8]`. A lower floor sharpens the penalty for non-lockers and raises the max boost ratio; a higher floor softens differentiation. See [ADR 026 § Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds) for the safety-bound table.

#### Per-operator gauge-share cap

The per-epoch gauge share for any single operator is capped at `MAX_GAUGE_SHARE_PER_OPERATOR` (default **5%**, governable within `[1%, 25%]` per [ADR 009](009-governance.md#adr-009-governance-model) safety bounds). Concretely:

```
share_i = min(working_bytes_i / sum(working_bytes), MAX_GAUGE_SHARE_PER_OPERATOR)
```

Any residual gauge bucket left after capping (which occurs when one or more operators would have received more than the cap) rolls over to the next epoch's gauge accumulator under the same rule as the `sum(working_bytes) == 0` degenerate case in [Degenerate-input fallbacks](#degenerate-input-fallbacks) above. The 26-epoch claim window in [ADR 026 § Epoch mechanics](026-tokenomics.md#epoch-mechanics) caps the total rollover.

**Rationale.** Bounds wash-trading payoff at 5% of the gauge bucket per operator-identity. Combined with the boost formula's `0.4·bytes_i` floor for low-ve operators and the closed-pool gauge structure (every settlement contributes to the same global bucket the operator is then claiming from), this makes wash-trading economically marginal at any reasonable TOKEN price — the attacker pays into the pool they're trying to drain, with 8% leakage to treasury+safety per self-deal, and the cap suppresses any non-proportional share they could extract via ve-boost. Sybil expansion of attack-operator count requires fresh `StakingRegistry` registrations each with the [ADR 026 § Operator economics and minimum stake](026-tokenomics.md#operator-economics-and-minimum-stake) minimum stake, converting wash-trading from a heuristic-bypass attack into a stake-proportional capital-lockup attack. A single honest operator with a dominant byte share is also subject to the cap, which is the intended posture — the gauge pool exists to incentivize a diverse operator set, not to reward concentration.

### Voting escrow (`VotingEscrow`)

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

`VotingEscrow.balanceOfAt(user, ts)` and `totalSupplyAt(ts)` are load-bearing for the epoch-snapshot pattern in [ADR 026 § FeeRouter split (40/40/7/5/5/3)](026-tokenomics.md#feerouter-split-40407553) and the governance pattern in [ADR 026 § Governance](026-tokenomics.md#governance). Per-lock checkpoints; reads O(log n) on the checkpoint array; writes O(1) amortized.

#### Lock ownership

Locks may be held by any address — EOA or contract. Lock creation (`createLock`), amount increase (`increaseAmount`), and time extension (`increaseUnlockTime`) are stable for cross-contract integration. A future contract that holds a pooled lock on behalf of multiple beneficiaries (e.g., a liquid-ve wrapper) integrates as an additive top-level contract via these interfaces without changing `VotingEscrow`.

**Operator stake and ve-positions are separate.** A node's operator stake is held in `StakingRegistry` and is slashable (rates per [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn)). A ve-position is held in `VotingEscrow` and is not. Neither satisfies the other's requirements; an operator may hold any combination. This separation is a hard invariant — no contract path lets ve-locked TOKEN be slashed.

#### Contract: VotingEscrow

```solidity
interface IVotingEscrow {
    // ─── Lock lifecycle ────────────────────────────────────────────────
    // Lock `amount` TOKEN until `unlockTime` (absolute seconds), rounded
    // down to the nearest week boundary (week-aligned slopes — veCRV
    // pattern). Reverts if the caller already holds a lock, if
    // `unlockTime - block.timestamp` is outside
    // [minLockDuration, maxLockDuration], or if `amount == 0`.
    function createLock(uint256 amount, uint256 unlockTime) external;

    // Add `amount` to the caller's existing lock; unlock time unchanged.
    // Reverts if the caller has no active lock or it has expired.
    function increaseAmount(uint256 amount) external;

    // Extend the caller's lock to a later `unlockTime` (week-aligned).
    // Reverts if `unlockTime` is at or before the current end, if the new
    // remaining duration exceeds `maxLockDuration`, or if expired.
    function increaseUnlockTime(uint256 unlockTime) external;

    // Withdraw the full locked balance after unlock time. Lump-sum only.
    // Reverts if not yet expired (no early-exit path; ve-locked TOKEN
    // never exits early).
    function withdraw() external;

    // ─── Lock view ─────────────────────────────────────────────────────
    // Account's locked amount and unlock time. Returns (0, 0) for
    // never-locked or already-withdrawn addresses.
    function locked(address account)
        external view returns (uint256 amount, uint256 end);

    // ─── ve-balance ────────────────────────────────────────────────────
    // Current voting weight: amount × remaining_lock_time / maxLockDuration.
    // Decays linearly to zero at unlock time.
    function balanceOf(address account) external view returns (uint256);

    // Historical voting weight at unix timestamp `ts`. Per-lock
    // checkpoints make this O(log n). Load-bearing for the ADR 026 § FeeRouter split
    // epoch-snapshot pattern (FeeRouter gauge accounting) and ADR 026 § Governance
    // governance vote-weight reads. Future timestamps are rejected.
    function balanceOfAt(address account, uint256 timestamp)
        external view returns (uint256);

    // Current total ve-supply (sum of all balanceOf at block.timestamp).
    function totalSupply() external view returns (uint256);

    // Historical total ve-supply at `ts`. Same checkpoint pattern as
    // `balanceOfAt`; used for Governor quorum per ADR 026 § Governance.
    function totalSupplyAt(uint256 timestamp) external view returns (uint256);

    // ─── Vote delegation (Governor Bravo pattern) ──────────────────────
    // Delegate the caller's ve voting weight to `delegatee`. The
    // ve-position stays non-transferable; only voting weight moves.
    // `address(0)` clears delegation (defaults to self). See ADR 009 § Production: ve-Weighted Governance.
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

    // Address `account` delegates to, or `account` itself if unset
    // (self-delegation is the default).
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
- Delegation reassigns voting weight but not the underlying ve-position; locks remain non-transferable per the [Voting escrow (`VotingEscrow`)](#voting-escrow-votingescrow) invariant. Delegation events follow OZ Governor Bravo.
