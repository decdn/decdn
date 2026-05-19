# ADR 016: Smart Contract Interaction Model

**Date:** 2026-04-04
**Status:** Draft

## Context

The deCDN deploys multiple interacting smart contracts with cross-contract calls, role-based access control, and funds custody. Individual contracts are specified across [ADR 003](003-payments.md), [ADR 009](009-governance.md), [ADR 011](011-content-takedown.md), [ADR 014](014-on-chain-verification.md), and [ADR 026](026-gauge-boost-tokenomics.md). However, no single document maps the full interaction surface: who calls whom, which contracts hold funds, who is authorized to do what, and where reentrancy risks exist.

This ADR consolidates that analysis into a single reference for security audits and implementation. It does not introduce new functionality — it systematizes what other ADRs already specify.

> **[ADR 026](026-gauge-boost-tokenomics.md) driver.** The contract surface in this ADR is materially expanded by [ADR 026](026-gauge-boost-tokenomics.md), which adds `FeeRouter`, `VotingEscrow`, `SafetyReserve`, and `DelegatorBuyer`, and rewires `PaymentChannel`, `StakingRegistry`, and `BuybackBurner`. Read [ADR 026](026-gauge-boost-tokenomics.md) first for the economic model; this ADR is the integration view.

## Decision

### 1. Contract Inventory

All on-chain contracts inherit from [OpenZeppelin Contracts](https://docs.openzeppelin.com/contracts/) to minimize custom security-critical code. The full surface ships in a single audit pass; per-bucket economics are governance-tunable from day one (see [§ Tunable Economics](#tunable-economics) below) so the network can launch with a simplified split (e.g. 80/0/0/10/10/0) and dial up gauge boost / delegator pool / safety reserve as the dependent infrastructure stabilizes.

| Contract | ADR | Holds Funds | Token Types | OZ Base Contracts |
| --- | --- | --- | --- | --- |
| TOKEN (ERC-20) | [026](026-gauge-boost-tokenomics.md) | No (fungible token) | — | `ERC20`, `ERC20Permit`, `ERC20Votes` (fixed-supply per [ADR 026](026-gauge-boost-tokenomics.md) §1; no post-genesis mint function) |
| StakingRegistry | [003](003-payments.md), [026](026-gauge-boost-tokenomics.md) | Yes | TOKEN | `AccessControl`, `ReentrancyGuard`, `Pausable`, `EIP712` (includes `isActive(operator)` per [ADR 003](003-payments.md) `IStakingRegistry`) |
| PaymentChannel | [003](003-payments.md) | Yes | USDC | `Ownable`, `ReentrancyGuard`, `Pausable`, `EIP712` (USDC-only; the USDC address is fixed at deployment; `settleChannel` forwards full balance to `FeeRouter.routeSettlement` rather than skimming inline) |
| FeeRouter | [026](026-gauge-boost-tokenomics.md) | Yes | USDC (transient + epoch buckets), TOKEN (delegator-pool epoch buckets) | `AccessControl`, `ReentrancyGuard`, `Pausable` |
| VotingEscrow | [026](026-gauge-boost-tokenomics.md) | Yes | TOKEN (locked, non-transferable) | `ReentrancyGuard`, `Pausable` |
| SafetyReserve | [026](026-gauge-boost-tokenomics.md), [028](028-slashing-appeals.md) | Yes | USDC (3% bucket + slashing redirect), TOKEN (transient until keeper swap) | `AccessControl`, `ReentrancyGuard`, `Pausable` (includes [ADR 028](028-slashing-appeals.md) appeal extensions: `openSlashAppeal` / `fastTrackAppeal` / `rejectAppeal` / `ratifyAppeal` / `reverseAppeal`) |
| BuybackBurner | [018](018-liquidity-strategy.md), [026](026-gauge-boost-tokenomics.md) | Yes | USDC, TOKEN (transient) | `AccessControl`, `ReentrancyGuard`, `Pausable` (Balancer V3 swap-and-burn path) |
| DelegatorBuyer | [026](026-gauge-boost-tokenomics.md) §6 | Yes | USDC, TOKEN (transient) | `AccessControl`, `ReentrancyGuard`, `Pausable` (USDC→TOKEN swap for the delegator pool; parallel contract to `BuybackBurner` — the two share the Balancer V3 swap execution path via an internal swap-helper library but settle to different downstream destinations and have independent governance setters on `FeeRouter` — see [§ Shared swap helper](#shared-swap-helper-buybackburner--delegatorbuyer) below) |
| ContentBlacklist | [011](011-content-takedown.md) | No | — | `AccessControl`, `ReentrancyGuard` (full surface: hash-level — global + regional — operator-level — `addOrigin` / `removeOrigin` / `isOriginBlacklisted` — and the [ADR 011 § Blacklist Entry Appeals](011-content-takedown.md#blacklist-entry-appeals) API) |
| PublisherRegistry | [002](002-content-addressing.md) | No | — | `AccessControl`, `ReentrancyGuard` |
| OriginAssignment | [011](011-content-takedown.md) | No | — | `AccessControl`, `ReentrancyGuard` |
| SlashJudge | [014](014-on-chain-verification.md) | Yes | TOKEN (challenge bonds) | `AccessControl`, `ReentrancyGuard`, `Pausable`, `EIP712` |
| DecdnGovernor | [009](009-governance.md) | No | — | OZ `Governor` + `GovernorSettings` + `GovernorVotes` + `GovernorVotesQuorumFraction` + `GovernorTimelockControl` (thin wrapper supplying deCDN defaults: 7-day vote, 0.1% proposal threshold, 4% quorum, vote source = `VotingEscrow`) |
| TimelockController | [009](009-governance.md) | Yes (treasury custodian) | USDC | OZ `TimelockController` (no custom code; 48h delay; holds the 5% protocol-treasury bucket and is the `DEFAULT_ADMIN_ROLE` of every contract above) |

#### Contract Architecture (classDiagram)

The diagram below shows the full contract surface and its primary call relationships, with `StakingRegistry`, `Governor`, etc. included for orientation. `StakingRegistry` is unconnected on the fee-router path because it is independent of settlement — it governs slashable stake and is read by gossip / peer-validation logic ([ADR 001](001-network.md), [ADR 003](003-payments.md)) rather than by `FeeRouter`.

```mermaid
classDiagram
    class PaymentChannel {
        +settleChannel(op, bytes, amount)
    }
    class FeeRouter {
        +routeSettlement(op, bytes, amount, epochId)
        +bytesPerEpoch(op, epoch)
        +claimBoost(epochs)
        +claimDelegator(epochs)
        +workingBytes(op, epoch)
        +executeDelegatorSwap(epoch, minOut)
        +depositDelegatorTokens(epoch, amount)
    }
    class VotingEscrow {
        +createLock(amount, duration)
        +extendLock(duration)
        +withdraw()
        +balanceOfAt(user, ts)
        +totalSupplyAt(ts)
    }
    class BuybackBurner {
        +executeBuyback(amount, minOut)
    }
    class SafetyReserve {
        +payout(bundle, recipient, amount)
        +incidents(id)
    }
    class DelegatorBuyer {
        +swapDelegatorBucket(epoch, amountIn, minOut)
    }
    class StakingRegistry {
        +stake()
        +unstake()
        +slash()
    }
    class Governor {
        +propose()
        +vote()
        +execute()
    }
    class Treasury
    class BalancerV3Pool

    PaymentChannel ..> FeeRouter : routeSettlement
    FeeRouter ..> VotingEscrow : balanceOfAt
    FeeRouter ..> BuybackBurner : 5% USDC
    FeeRouter ..> Treasury : 5% USDC
    FeeRouter ..> SafetyReserve : 3% USDC
    FeeRouter ..> DelegatorBuyer : 7% USDC (delegator pool)
    DelegatorBuyer ..> BalancerV3Pool : USDC→TOKEN swap
    DelegatorBuyer ..> FeeRouter : depositDelegatorTokens
    BuybackBurner ..> BalancerV3Pool : swap USDC→TOKEN
    Governor ..> VotingEscrow : voting weight
    Governor ..> SafetyReserve : payout authorization
```

The full FeeRouter six-bucket split (40/40/7/5/5/3) is the steady-state target specified in [ADR 026](026-gauge-boost-tokenomics.md) §2; epoch-bucket mechanics and the gauge-boost formula live in [ADR 026](026-gauge-boost-tokenomics.md) §2–§3. This ADR does not duplicate the bucket table; the launch-default share configuration and the tunability mechanism are in [§ Tunable Economics](#tunable-economics) below.

#### Tunable Economics

The full six-bucket structure ships from day one, but every bucket share and every dependency address is governance-mutable. This lets the network launch with a simplified split — a typical default is `80% operator / 0% gauge / 0% delegator / 10% buyback / 10% treasury / 0% safety` — and dial up the gauge / delegator / safety legs as `VotingEscrow`, `SafetyReserve`, and `DelegatorBuyer` are deployed and as the dependent ADRs ([026](026-gauge-boost-tokenomics.md), [028](028-slashing-appeals.md)) settle into operational defaults.

The pattern has three knobs, all under `GOVERNANCE_ROLE` (i.e. the `TimelockController`):

1. **Bucket shares.** `FeeRouter.setShares(operatorBaseBps, gaugeBoostBps, delegatorBps, buybackBps, treasuryBps, safetyBps)` updates the six-bucket split in basis points. Sum-to-10000 invariant enforced; cross-validated against dependency addresses (see knob 2). Steady-state target per [ADR 026](026-gauge-boost-tokenomics.md) §2 is `4000 / 4000 / 700 / 500 / 500 / 300`.
2. **Dependency addresses.** `FeeRouter.setVotingEscrow(addr)`, `setSafetyReserve(addr)`, `setDelegatorBuyer(addr)`, `setBuybackBurner(addr)`, `setTreasury(addr)` may be called any time. **Cross-validation:** `setShares(...)` reverts if any non-zero share has its destination set to `address(0)` — so a bucket can only become live once its sink contract is wired in. Same applies in reverse: `set*(address(0))` reverts if the corresponding share is non-zero.
3. **Helper-contract addresses on signing contracts.** `PaymentChannel.setFeeRouter(addr)` (per [ADR 003](003-payments.md)) lets governance re-point the router target without redeploying the payment channel. The EIP-712 domain separator is unaffected because it does not include the FeeRouter address; see [§ No proxy deployment patterns](#no-proxy-deployment-patterns) below for the full carve-out.

**Inactive buckets accumulate zero with no reverts.** Same-tx legs (operator / buyback / treasury / safety) execute inline against their `safeTransfer` paths. Epoch-bucket legs (gauge / delegator) accumulate to per-epoch storage; at zero share, no storage writes happen and `claimBoost(epochs)` / `claimDelegator(epochs)` return zero for those epochs. No code path reverts when a bucket is off — the contract is uniformly dormant on the disabled legs.

**Activation sequence is governance-driven.** When `VotingEscrow` / `SafetyReserve` / `DelegatorBuyer` are deployed and audited, governance calls the relevant `set*(addr)` then `setShares(...)` to allocate the bucket. Because share updates pass through the standard 48h timelock, bucket activations are externally observable in advance.

**Launch deployment.** `FeeRouter` deploys with zero-address dependencies for `VotingEscrow` / `SafetyReserve` / `DelegatorBuyer` if those aren't co-deployed (see [§ Deployment Order](#2-deployment-order-and-initialization-dependencies) below). The launch share configuration honors the cross-validation invariant — only buckets whose destinations are wired may be set non-zero.

#### Contract: FeeRouter

```solidity
interface IFeeRouter {
    // Called by `PaymentChannel.settleChannel`. Forwards the operator's
    // full USDC balance through the configured six-bucket split (per ADR 026
    // §2); same-tx legs execute inline, epoch-bucket legs accumulate to
    // per-epoch storage. Updates `lastSettlementAt[operator]` on
    // `StakingRegistry` via `SETTLEMENT_REPORTER_ROLE`. Reverts if paused.
    function routeSettlement(
        address operator,
        uint256 bytesDelivered,
        uint256 amount,
        uint64  epochId             // voucher.epochId per ADR 003
    ) external;

    // Populated inline by routeSettlement; read by gauge claim per ADR 026
    // §3 to feed bytes_i. The per-operator gauge-share cap (ADR 026 §3) is
    // the binding wash-trading defense.
    function bytesPerEpoch(address operator, uint64 epoch) external view returns (uint256);

    // Pull-based claims for the gauge-boost and delegator epoch buckets.
    // INVARIANT: `epochs` MUST all be in the past 26-epoch claim window;
    // older epochs are swept to treasury via `sweepUnclaimed` and revert
    // here. Returns the total amount transferred, for tooling.
    function claimBoost(uint64[] calldata epochs) external returns (uint256 amount);
    function claimDelegator(uint64[] calldata epochs) external returns (uint256 amount);

    // Per-epoch USDC→TOKEN delegator-bucket swap: forwards accumulated USDC
    // to `DelegatorBuyer.swapDelegatorBucket`. `DelegatorBuyer` is the
    // single Balancer V3 caller (liquidity-cap defenses in one place).
    // `KEEPER_ROLE`-gated.
    function executeDelegatorSwap(uint64 epochId, uint256 minOut) external;

    // Callback from `DelegatorBuyer.swapDelegatorBucket` after the swap;
    // deposits TOKEN into the delegator bucket for `epoch`. INVARIANT:
    // `msg.sender == delegatorBuyer` is the only authorization — single
    // trust boundary, no post-deploy role grants.
    function depositDelegatorTokens(uint64 epochId, uint256 amount) external;

    // Permissionless: sweeps the unclaimed remainder of any epoch past the
    // 26-epoch claim window to the treasury, freeing the storage slot
    // (same pattern as `OriginAssignment.pruneBlacklistedAssignment`,
    // [ADR 011](011-content-takedown.md#interaction-with-contentblacklist)).
    function sweepUnclaimed(uint64[] calldata epochs) external;

    // Per-(operator, epoch) ve-weighted byte count fed into the gauge
    // formula ([ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula)).
    function workingBytes(address operator, uint64 epochId) external view returns (uint256);
    function epochTotalWorkingBytes(uint64 epochId) external view returns (uint256);

    // Per-epoch USDC accumulators. `delegatorBucket` is USDC pre-swap and
    // TOKEN post-swap (swap zeros the USDC slot, writes the TOKEN slot);
    // the value reported is the active bucket currency.
    function gaugeBucket(uint64 epochId) external view returns (uint256);
    function delegatorBucket(uint64 epochId) external view returns (uint256);

    // Configured shares (bps) and dependency addresses. INVARIANT: these
    // are governance-set state (via `setShares` / `setSharesAndDestinations`),
    // NOT operator-asserted and NOT derived from settlement state. Array
    // order matches `setShares`: [operatorBase, gaugeBoost, delegator,
    // buyback, treasury, safety].
    function getShares() external view returns (uint256[6] memory);
    function votingEscrow() external view returns (address);
    function safetyReserve() external view returns (address);
    function delegatorBuyer() external view returns (address);
    function buybackBurner() external view returns (address);
    function treasury() external view returns (address);
    function boostFloor() external view returns (uint256); // 4-decimal fixed-point: 4000 = 0.4

    // Atomic shares + dependency-address update in one timelock proposal,
    // so the cross-validation invariant ([§ Tunable Economics](#tunable-economics)
    // — non-zero share requires non-zero destination) holds at every
    // observable state. Use for activation flips; per-knob setters below
    // are for routine post-wiring governance.
    struct ShareDestinations {
        address votingEscrow;
        address safetyReserve;
        address delegatorBuyer;
        address buybackBurner;
        address treasury;
    }
    function setSharesAndDestinations(
        uint256[6] calldata sharesBps,
        ShareDestinations calldata dests
    ) external;

    // Per-knob setters. Each must satisfy the cross-validation invariant:
    // `setShares` reverts if any non-zero share targets `address(0)`;
    // each `set*(address(0))` reverts if the corresponding share is
    // non-zero. Order of operations: zero out the share first, then
    // re-point the destination.
    function setShares(uint256[6] calldata sharesBps) external;
    function setVotingEscrow(address newVotingEscrow) external;
    function setSafetyReserve(address newSafetyReserve) external;
    function setDelegatorBuyer(address newDelegatorBuyer) external;
    function setBuybackBurner(address newBuybackBurner) external;
    function setTreasury(address newTreasury) external;
    function setBoostFloor(uint256 newFloor) external; // bounded [2000, 8000] per [ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds)

    // `pause()` blocks `routeSettlement`, `claimBoost`, `claimDelegator`,
    // `executeDelegatorSwap`, `sweepUnclaimed`. `PaymentChannel`
    // close/dispute are independent and remain available — settlement
    // queues until `unpause`.
    function pause() external;
    function unpause() external;

    // ─── Events ───────────────────────────────────────────────────────
    event Settled(
        address indexed operator,
        uint256 bytesDelivered,
        uint256 amount,
        uint64 indexed epochId
    );
    event BoostClaimed(address indexed operator, uint64 indexed epochId, uint256 amount);
    event DelegatorClaimed(address indexed account, uint64 indexed epochId, uint256 amount);
    event DelegatorSwapped(uint64 indexed epochId, uint256 amountIn, uint256 amountOut);
    event DelegatorTokensDeposited(uint64 indexed epochId, uint256 amount);
    event UnclaimedSwept(uint64 indexed epochId, uint256 gaugeAmount, uint256 delegatorAmount);
    event SharesUpdated(uint256[6] newShares);
    event VotingEscrowUpdated(address indexed oldAddr, address indexed newAddr);
    event SafetyReserveUpdated(address indexed oldAddr, address indexed newAddr);
    event DelegatorBuyerUpdated(address indexed oldAddr, address indexed newAddr);
    event BuybackBurnerUpdated(address indexed oldAddr, address indexed newAddr);
    event TreasuryUpdated(address indexed oldAddr, address indexed newAddr);
    event BoostFloorUpdated(uint256 oldFloor, uint256 newFloor);
}
```

**Notes:**

- **Epoch length / claim window are immutable** (1 week / 26 epochs, constructor-set, [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553)). Changing them post-deploy shifts every stored `epoch` index; a change ships as a fresh `FeeRouter` with state migration ([§ No proxy deployment patterns](#no-proxy-deployment-patterns)).
- **`workingBytes` is per-operator only.** Off-chain calculators bundle it with `epochTotalWorkingBytes` and `gaugeBucket` via Multicall3.
- **Gauge-bytes tracking is inline in `routeSettlement`.** INVARIANT: there is no operator-asserted summary, no on-chain identity gate, and no fraud-challenge mechanism — the [ADR 026 §3](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) per-operator gauge-share cap is the binding wash-trading defense.
- **No `initialize(...)` helper** — proxies are forbidden ([§ No proxy deployment patterns](#no-proxy-deployment-patterns)); constructor + post-deploy `setSharesAndDestinations` from `TimelockController` suffices.
- **Storage shape is implementation-defined** — the `gaugeBucket`/`delegatorBucket` views document semantic per-epoch state; separate mappings are recommended (buckets accrue independently; packing forces an `SSTORE` of the unchanged half each accumulation).

##### No proxy deployment patterns

No deCDN contract uses proxy (upgradeable) deployment patterns. Production contract upgrades deploy new contracts at new addresses with state migration as described in Section 6. This constraint ensures that EIP-712 domain separators computed in constructors (as `immutable`) remain valid for the contract's lifetime — a proxy migration to a different address or chain would invalidate all existing voucher signatures.

**Carve-out for non-signing helper addresses.** The immutability constraint applies only to fields included in voucher / challenge domain separators on the signing contracts (`PaymentChannel`, `SlashJudge`, `StakingRegistry.bindNode`). Helper-contract addresses referenced by signing contracts — `feeRouter` on `PaymentChannel`, `stakingRegistry` on `SlashJudge`, `contentBlacklist` on `OriginAssignment`, and the `setVotingEscrow` / `setSafetyReserve` / `setDelegatorBuyer` / `setBuybackBurner` / `setTreasury` setters on `FeeRouter` — may be re-pointed via `GOVERNANCE_ROLE`-gated setters under the standard 48h timelock. Helper addresses are not domain-separator inputs, so re-pointing them does not invalidate any existing signatures.

**Build toolchain:** [Foundry](https://book.getfoundry.sh/) (forge, cast, anvil) for compilation, testing, and deployment.

#### Shared swap helper (BuybackBurner ↔ DelegatorBuyer)

`BuybackBurner` and `DelegatorBuyer` are separate contracts with separate addresses and separate audit boundaries. Both consume USDC and produce TOKEN via the same Balancer V3 80/20 pool but settle to different downstream destinations — `BuybackBurner` burns its TOKEN output ([ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn)); `DelegatorBuyer` deposits its TOKEN output back into the `FeeRouter` per-epoch delegator bucket via `IFeeRouter.depositDelegatorTokens(epochId, amount)` ([ADR 026 §6](026-gauge-boost-tokenomics.md#6-delegator-pool-usdc-token-conversion)). The two are wired through distinct `FeeRouter` setters (`setBuybackBurner` / `setDelegatorBuyer`) and expose divergent governance and event surfaces ([`IDelegatorBuyer`](026-gauge-boost-tokenomics.md#contract-delegatorbuyer) is single-purpose with `msg.sender == feeRouter` as its sole auth check; `BuybackBurner` is `KEEPER_ROLE`-gated and burn-typed).

They share the Balancer V3 swap execution path — TWAP splitting, `minTokenOut` aggregate guard, `subSwapMinBlockGap` spacing, mandatory private-RPC routing, the [`epochLiquidityCapFraction` per-epoch combined cap](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds), and Vault-scoped self-approval — via an internal abstract contract (e.g., `BalancerV3SwapHelper`) inherited by both. The combined-path per-epoch liquidity-cap accounting per [ADR 018 § Liquidity-cap interaction](018-liquidity-strategy.md#liquidity-cap-interaction) is implemented as shared state in that base contract so both children see the same epoch-to-date notional.

A unified `BuybackBurner` multi-output mode (the rejected alternative) was considered for its smaller surface count. It is rejected because (a) the audit savings are captured by the shared base contract without entangling the two children's downstream value flows, (b) collapsing the surfaces would force the two paths to share governance setters and events despite serving semantically distinct purposes (deflation vs ve-locker redistribution), and (c) the [`IDelegatorBuyer` interface](026-gauge-boost-tokenomics.md#contract-delegatorbuyer) already specifies a distinct, single-purpose contract.

### 2. Deployment Order and Initialization Dependencies

Contracts must be deployed in dependency order — each contract's constructor requires the addresses of contracts deployed before it. `FeeRouter` accepts `address(0)` for `VotingEscrow` / `SafetyReserve` / `DelegatorBuyer` at construction; the cross-validation invariant in [§ Tunable Economics](#tunable-economics) ensures any non-zero share has a non-zero destination, so the launch share configuration determines which dependencies must already be wired. `DelegatorBuyer` is deployed after `FeeRouter` so its constructor can pin the router address as immutable; governance then calls `FeeRouter.setDelegatorBuyer(addr)` to complete the bidirectional wiring.

```mermaid
graph TD
    TOKEN["1. TOKEN (ERC-20, fixed-supply)"]
    USDC["2. USDC (existing or testnet)"]
    TL["3. TimelockController"]
    SR["4. StakingRegistry"]
    VE["5. VotingEscrow"]
    SAFE["6. SafetyReserve"]
    BB["7. BuybackBurner"]
    FR["8. FeeRouter"]
    DB["9. DelegatorBuyer"]
    SPC["10. PaymentChannel"]
    PR["11. PublisherRegistry"]
    OA["12. OriginAssignment"]
    CB["13. ContentBlacklist"]
    SJ["14. SlashJudge"]
    GOV["15. DecdnGovernor"]

    SR --> TOKEN
    VE --> TOKEN
    SAFE --> USDC
    BB --> TOKEN
    BB --> USDC
    FR --> USDC
    FR --> TL
    FR -.->|"optional at deploy"| BB
    FR -.->|"optional at deploy"| SAFE
    FR -.->|"optional at deploy"| VE
    DB --> TOKEN
    DB --> USDC
    DB --> FR
    SPC --> USDC
    SPC --> SR
    SPC --> FR
    OA --> SR
    OA --> PR
    OA --> CB
    CB --> SR
    SJ --> SR
    SJ --> TOKEN
    GOV --> VE
    GOV --> TL
```

#### Constructor Dependencies

| Step | Contract | Constructor Requires |
| --- | --- | --- |
| 1 | TOKEN | Initial holder, initial supply (1B fixed per [ADR 026](026-gauge-boost-tokenomics.md) §1), owner. No `mint()` function; testnet seeding happens via the constructor `_mint(initialHolder, 1_000_000_000e18)`. |
| 2 | USDC | External (testnet faucet or mainnet address) |
| 3 | TimelockController | OZ `TimelockController(minDelay, proposers, executors, admin)` — `minDelay` is 48h ([ADR 009](009-governance.md)). Deployed early so its address is available to `FeeRouter` as the treasury bucket destination and to every `AccessControl`-bearing contract as the eventual `DEFAULT_ADMIN_ROLE` holder. `proposers` is initialized empty and `PROPOSER_ROLE` is granted to `DecdnGovernor` post-deploy (step 15); `executors` is `[address(0)]` (anyone may execute after the delay). |
| 4 | StakingRegistry | TOKEN address, `minStake` (**50,000 TOKEN** per [ADR 026](026-gauge-boost-tokenomics.md) §7), `unbondingPeriod` (7 days). No discount-threshold parameters; operator return is differentiated through the gauge-boost flow in `FeeRouter`. |
| 5 | VotingEscrow | TOKEN address, `minLockDuration` (1 week), `maxLockDuration` (4 years). No `create_lock_for` privileged path; auto-ve-lock not exposed. Implements `balanceOfAt(user, ts)` and `totalSupplyAt(ts)` historical checkpointing ([ADR 026](026-gauge-boost-tokenomics.md) §4). |
| 6 | SafetyReserve | USDC address, Governor address (payout authorizer; may be `address(0)` at deploy and set via `setGovernor` once `DecdnGovernor` is deployed), emergency-multisig address (fast-track approver under hard caps), `appealWindow` (48h) ([ADR 026](026-gauge-boost-tokenomics.md) §5). [ADR 028](028-slashing-appeals.md) appeal extensions are part of the same contract — no separate deployment. |
| 7 | BuybackBurner | TOKEN address, USDC address, Balancer V3 Router address, initial pool contract `address` (may be zero-address at deploy and set later via `setPool(address)` — see [ADR 003](003-payments.md#buybackburner) for the interface and [ADR 018](018-liquidity-strategy.md) for the venue rationale). The pool address remains governance-mutable post-deploy via `setPool(address)`; the constructor value is an initial convenience, not a hard requirement. **Inflow source:** `FeeRouter` ([ADR 026](026-gauge-boost-tokenomics.md) §8). **Router address and naming:** see [ADR 018 §"Buyback execution via Balancer V3"](018-liquidity-strategy.md#buyback-execution-via-balancer-v3) for the canonical Balancer V3 Router address and the `Router v2` label disambiguation. **Approvals note:** `BuybackBurner` MUST self-approve the Balancer V3 **Vault** address (distinct from the Router) during initialization — the Vault pulls input tokens from `msg.sender`, which is `BuybackBurner`. The V3 footgun reference and Vault address live in [ADR 018](018-liquidity-strategy.md#buyback-execution-via-balancer-v3). |
| 8 | FeeRouter | USDC address, TOKEN address, **`TimelockController` address** (treasury bucket destination), Balancer V3 Router address, `epochLength` (1 week), `claimWindow` (26 epochs), launch split shares (cross-validated against dependency addresses), and `boostFloor` (0.4) per [ADR 026](026-gauge-boost-tokenomics.md) §3. **Dependency addresses** (`votingEscrow`, `safetyReserve`, `delegatorBuyer`, `buybackBurner`) may all be `address(0)` at deploy and set later via the governance-mutable setters in [§ Tunable Economics](#tunable-economics); the cross-validation invariant ensures any non-zero share has a non-zero destination at construction time. Steady-state target shares per [ADR 026](026-gauge-boost-tokenomics.md) §2 are 40/40/7/5/5/3 in basis points. |
| 9 | DelegatorBuyer | TOKEN address, USDC address, Balancer V3 Router address, initial pool address (same handling as `BuybackBurner`), and the **`FeeRouter` address** (deposit target — `depositDelegatorTokens(epoch, amount)`). Same Vault-scoped self-approval pattern. Parallel contract to `BuybackBurner` per [§ Shared swap helper](#shared-swap-helper-buybackburner--delegatorbuyer). After deployment, governance calls `FeeRouter.setDelegatorBuyer(address(delegatorBuyer))` to complete the bidirectional wiring. |
| 10 | PaymentChannel | USDC address, StakingRegistry address, FeeRouter address, `disputeWindow` (48h), `maxChannelDuration` (90 days), rate bounds ([ADR 003](003-payments.md)). `settleChannel` does not skim a protocol fee inline — it transfers the full operator USDC balance to `FeeRouter.routeSettlement(operator, bytesDelivered, amount, epochId)` in the same transaction. `setFeeRouter(address)` is governance-mutable per [§ No proxy deployment patterns](#no-proxy-deployment-patterns) carve-out. |
| 11 | PublisherRegistry | None. Permissionless registration; namespace cap and transfer-timelock parameters are read from the governance-controlled parameter store at call time. See [ADR 002 § Contract: PublisherRegistry](002-content-addressing.md#contract-publisherregistry). |
| 12 | OriginAssignment | StakingRegistry, PublisherRegistry, ContentBlacklist (latter may be zero at deploy; bound via `setContentBlacklist`). Min-redundancy, timelock, and default-open parameters are governance-controlled. See [ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority) and [§ OriginAssignment construction notes](#originassignment-construction-notes) below. |
| 13 | ContentBlacklist | `ContentBlacklist(address stakingRegistry)`. StakingRegistry address is required for `ejectNode()`. `ContentBlacklist` does not cross-call `OriginAssignment`; security relies on runtime checks (see [ADR 011 § Interaction with ContentBlacklist](011-content-takedown.md#interaction-with-contentblacklist)). After deployment, `OriginAssignment.setContentBlacklist(address)` is called once via the deployer / admin to wire the read direction (`OriginAssignment.pruneBlacklistedAssignment` queries `ContentBlacklist.isOriginBlacklisted`). |
| 14 | SlashJudge | StakingRegistry address, TOKEN address, `challengeBond` (100 TOKEN), `counterEvidenceWindow` (24h) |
| 15 | DecdnGovernor | OZ Governor wrapper composing `Governor` + `GovernorSettings` + `GovernorVotes` + `GovernorVotesQuorumFraction` + `GovernorTimelockControl`. Constructor wires `VotingEscrow` (vote source) + `TimelockController` (execution target) + the [ADR 009](009-governance.md) defaults (7-day vote, 0.1% proposal threshold, 4% quorum). After deployment, `TimelockController.grantRole(PROPOSER_ROLE, address(decdnGovernor))`. |

#### OriginAssignment construction notes

- **Default-open allow-list bootstrap.** Entries keyed by `namespaceId == 0` start empty with `defaultOpenAllowlistActive == false` (permissive bootstrap window — any active staker may serve as origin for default-open content). The first non-empty governance activation flips `defaultOpenAllowlistActive` to `true` permanently; thereafter only allow-listed operators appear in `getOrigins(0)`.
- **Default-open governance entry points.** `setDefaultOpenAllowlist`, `addDefaultOpenOperator`, `removeDefaultOpenOperator`, `setDefaultOpenMinRedundancy`, `setDefaultOpenMaxOrigins` all carry `GOVERNANCE_ROLE` and run under the Governor's standard 48h timelock.
- **ContentBlacklist binding.** Until `setContentBlacklist(address)` is called post-deploy (see [§ Post-Deployment Initialization](#post-deployment-initialization) below), `pruneBlacklistedAssignment` reverts — it cannot read `isOriginBlacklisted` against the zero address. This does not block usage: off-chain consumers of `getOrigins(...)` cross-reference `ContentBlacklist.isOriginBlacklisted` directly via RPC.

#### Post-Deployment Initialization

After all contracts are deployed, the deployer must execute these transactions before the system accepts user traffic:

1. **Grant `BLACKLIST_ROLE`** on StakingRegistry to ContentBlacklist:

   ```solidity
   stakingRegistry.grantRole(BLACKLIST_ROLE, address(contentBlacklist));
   ```

2. **Bind ContentBlacklist into OriginAssignment** (one-shot read wiring):

   ```solidity
   originAssignment.setContentBlacklist(address(contentBlacklist));
   ```

   This authorizes `OriginAssignment.pruneBlacklistedAssignment` to query `ContentBlacklist.isOriginBlacklisted` for permissionless storage cleanup. Until this call is made, prune calls revert; runtime authorization checks (probe, peer table) consult both contracts directly via the off-chain RPC path and are unaffected.

3. **Grant `SLASH_ROLE`** on StakingRegistry to SlashJudge:

   ```solidity
   stakingRegistry.grantRole(SLASH_ROLE, address(slashJudge));
   ```

   Pair the slash-redirect inflow grant in the same multicall:

   ```solidity
   safetyReserve.grantRole(SLASH_INFLOW_REPORTER_ROLE, address(stakingRegistry));
   ```

   This authorizes `StakingRegistry.slash` to call `SafetyReserve.recordSlashInflow(operator, amount)` for the 30% slashed-TOKEN redirect per [ADR 026](026-gauge-boost-tokenomics.md) §8 (challenger 50% / SafetyReserve 30% / burn 20%). **Slash currency:** stake is denominated in TOKEN, so the 30% share lands in `SafetyReserve` as TOKEN. `SafetyReserve` exposes a keeper-triggered swap (`swapAccumulatedTokens`) into the [ADR 018](018-liquidity-strategy.md) Balancer V3 80/20 pool (same Vault-scoped self-approval, TWAP, `minOut`, private-RPC, and per-epoch liquidity-cap defenses as `BuybackBurner` and the delegator-pool swap path). USDC is the only currency available for `payout`; until swapped, slashed TOKEN is held as part of `SafetyReserve`'s assets-under-management.

4. **Grant `ROUTER_CALLER_ROLE` on FeeRouter to PaymentChannel:**

   ```solidity
   feeRouter.grantRole(ROUTER_CALLER_ROLE, address(paymentChannel));
   ```

   This authorizes `PaymentChannel.settleChannel` to invoke `FeeRouter.routeSettlement(operator, bytesDelivered, amount, epochId)`. Without this grant the settlement path reverts.

5. **Grant `SETTLEMENT_REPORTER_ROLE` on StakingRegistry to FeeRouter:**

   ```solidity
   stakingRegistry.grantRole(SETTLEMENT_REPORTER_ROLE, address(feeRouter));
   ```

   See §3 below; `lastSettlementAt[operator]` is updated on each `routeSettlement` call.

6. **Register regional governance bodies** (when jurisdictional bodies are constituted):

   ```solidity
   contentBlacklist.registerRegionalBody(regionCode, bodyAddress);
   ```

7. **Transfer admin roles** to `TimelockController`:

   ```solidity
   // For each contract with AccessControl:
   contract.grantRole(DEFAULT_ADMIN_ROLE, address(timelockController));
   contract.revokeRole(DEFAULT_ADMIN_ROLE, deployer);
   ```

> **Admin handover hardening:** Deployments SHOULD execute `grantRole(DEFAULT_ADMIN_ROLE, timelockController)` and `revokeRole(DEFAULT_ADMIN_ROLE, deployer)` in a single multicall transaction to minimize the dual-admin window between the two operations.

> **Deployment atomicity.** The post-deployment initialization steps (1–7) SHOULD be executed atomically via a multicall contract or a deployment script that reverts on any failure. A partially initialized system (e.g., `SLASH_ROLE` granted but `BLACKLIST_ROLE` not yet, or `ROUTER_CALLER_ROLE` not yet granted to `PaymentChannel`) could create a window where some security mechanisms work but settlements revert or land in the wrong contract. Between deployment and initialization completion, `StakingRegistry` SHOULD reject `registerNode` calls (e.g., via a `paused` initial state or a deployment flag) to prevent nodes from registering before the security infrastructure is fully wired. A Foundry deployment script with sequential `vm.broadcast()` calls provides sufficient atomicity at launch scale.

### 3. Cross-Contract Call Graph

```mermaid
graph LR
    SPC["PaymentChannel"]
    SR["StakingRegistry"]
    CB["ContentBlacklist"]
    PR["PublisherRegistry"]
    OA["OriginAssignment"]
    SJ["SlashJudge"]
    BB["BuybackBurner"]
    DB["DelegatorBuyer"]
    FR["FeeRouter"]
    VE["VotingEscrow"]
    SAFE["SafetyReserve"]
    GOV["DecdnGovernor +<br/>TimelockController"]
    ERC["ERC-20 Tokens<br/>(USDC, TOKEN)"]
    BAL["Balancer V3 Router"]

    SPC -->|"getStakeMultiple(provider)"| SR
    SPC -->|"safeTransferFrom / safeTransfer"| ERC
    SPC -->|"routeSettlement(op, bytes, amount)"| FR
    FR -->|"balanceOfAt / totalSupplyAt"| VE
    FR -->|"5% USDC same-tx"| BB
    FR -->|"3% USDC same-tx"| SAFE
    FR -->|"7% USDC (delegator pool)"| DB
    FR -->|"safeTransfer (operator base, treasury, claims)"| ERC
    DB -->|"Router.swapSingleTokenExactIn() (delegator pool)"| BAL
    DB -->|"depositDelegatorTokens(epoch, amount)"| FR
    GOV -->|"balanceOfAt / totalSupplyAt"| VE
    GOV -->|"payout(bundle, recipient, amount)"| SAFE
    GOV -->|"setShares / setVotingEscrow / setSafetyReserve / setDelegatorBuyer / setBuybackBurner / setTreasury"| FR
    CB -->|"ejectNode(operatorAddress)"| SR
    OA -->|"isActive(operator)"| SR
    OA -->|"ownerOf(namespaceId)"| PR
    OA -->|"isOriginBlacklisted(operator)"| CB
    GOV -->|"activateAssignment(...)"| OA
    SJ -->|"slash(node, offenseType)"| SR
    SJ -->|"safeTransferFrom / safeTransfer"| ERC
    SR -->|"safeTransferFrom / safeTransfer"| ERC
    SR -->|"30% slashed TOKEN"| SAFE
    BB -->|"Router.swapSingleTokenExactIn()"| BAL
    BB -->|"safeTransferFrom / safeTransfer"| ERC
```

#### Complete Call Table

| Caller | Callee | Function | Authorization | Mutates Callee State |
| --- | --- | --- | --- | --- |
| PaymentChannel | StakingRegistry | `getStakeMultiple(provider)` | Public (read-only) | No |
| PaymentChannel | IERC20 (USDC) | `safeTransferFrom()` | Caller must have allowance | Yes |
| PaymentChannel | IERC20 (USDC) | `safeTransfer()` | Caller holds balance | Yes |
| PaymentChannel | FeeRouter | `routeSettlement(operator, bytesDelivered, amount, epochId)` | `ROUTER_CALLER_ROLE` on FeeRouter ([ADR 026](026-gauge-boost-tokenomics.md) §2) | Yes |
| FeeRouter | VotingEscrow | `balanceOfAt(user, ts)`, `totalSupplyAt(ts)` | Public (read-only) | No |
| FeeRouter | StakingRegistry | `recordSettlement(operator)` | `SETTLEMENT_REPORTER_ROLE` (granted to FeeRouter post-deploy; settlement counter moves with the routing call) | Yes |
| FeeRouter | BuybackBurner | `safeTransfer()` (5% USDC same-tx) | Caller holds balance | Yes |
| FeeRouter | SafetyReserve | `safeTransfer()` (3% USDC same-tx) | Caller holds balance | Yes |
| FeeRouter | Treasury wallet | `safeTransfer()` (5% USDC same-tx) | Caller holds balance | Yes |
| FeeRouter | IERC20 (USDC, TOKEN) | `safeTransfer()` (operator 40% base, claim payouts) | Caller holds balance | Yes |
| FeeRouter | DelegatorBuyer | `safeTransfer()` (7% USDC same-tx into the delegator-pool epoch bucket) and `swapDelegatorBucket(epoch, amountIn, minOut)` keeper trigger | Caller holds balance; `KEEPER_ROLE` for `swapDelegatorBucket` | Yes |
| DelegatorBuyer | Balancer V3 Router | `swapSingleTokenExactIn(...)` (USDC→TOKEN for the delegator pool; same Vault-scoped self-approval pattern as `BuybackBurner`) | Vault-scoped self-approval | Yes |
| DelegatorBuyer | FeeRouter | `depositDelegatorTokens(epoch, amount)` (deposits swapped TOKEN into `FeeRouter`'s delegator bucket so `claimDelegator` can pay against it) | Caller is the configured `DelegatorBuyer` address | Yes |
| Governor | FeeRouter | `setShares(operatorBaseBps, gaugeBoostBps, delegatorBps, buybackBps, treasuryBps, safetyBps)`, `setVotingEscrow(addr)`, `setSafetyReserve(addr)`, `setDelegatorBuyer(addr)`, `setBuybackBurner(addr)`, `setTreasury(addr)` | `GOVERNANCE_ROLE` on FeeRouter; sum-to-10000 invariant; cross-validated against dependency addresses (see [§ Tunable Economics](#tunable-economics)) | Yes |
| Governor | VotingEscrow | `balanceOfAt(user, ts)`, `totalSupplyAt(ts)` | Public (read-only) | No |
| Governor | SafetyReserve | `payout(bundle, recipient, amount)` | `PAYOUT_AUTHORIZER_ROLE` (Governor + emergency-multisig within hard caps; [ADR 026](026-gauge-boost-tokenomics.md) §5) | Yes |
| ContentBlacklist | StakingRegistry | `ejectNode(operatorAddress)` | `BLACKLIST_ROLE` | Yes |
| OriginAssignment | StakingRegistry | `isActive(operator)` | Public (read-only) | No |
| OriginAssignment | PublisherRegistry | `ownerOf(namespaceId)` | Public (read-only) | No |
| OriginAssignment | ContentBlacklist | `isOriginBlacklisted(operator)` | Public (read-only) | No |
| Governor | OriginAssignment | `activateAssignment(namespaceId)`, `revokeAssignment(namespaceId, operator)`, `setMinRedundancy(floor)`, `setMaxOriginsPerNamespace(cap)`, `setAssignmentTimelock(seconds)`, `setDefaultOpenAllowlist(operators[])`, `addDefaultOpenOperator(operator)`, `removeDefaultOpenOperator(operator)`, `setDefaultOpenMinRedundancy(floor)`, `setDefaultOpenMaxOrigins(cap)`, `setContentBlacklist(address)` | `GOVERNANCE_ROLE` on OriginAssignment | Yes |
| SlashJudge | StakingRegistry | `slash(node, offenseType)` | `SLASH_ROLE` | Yes |
| SlashJudge | IERC20 (TOKEN) | `safeTransferFrom()` / `safeTransfer()` | Caller must have allowance/balance | Yes |
| StakingRegistry | IERC20 (TOKEN) | `safeTransferFrom()` / `safeTransfer()` | Caller must have allowance/balance | Yes |
| StakingRegistry | SafetyReserve | `safeTransfer()` of TOKEN (30% of slashed stake; the remaining 50% goes to the challenger and 20% burns per [ADR 026](026-gauge-boost-tokenomics.md) §8). `SafetyReserve` swaps the accumulated TOKEN balance to USDC via a keeper-triggered call into the [ADR 018](018-liquidity-strategy.md) Balancer V3 80/20 pool. | Caller holds balance | Yes |
| BuybackBurner | Balancer V3 Router | `swapSingleTokenExactIn(pool, tokenIn, tokenOut, exactAmountIn, minAmountOut, deadline, wethIsEth, userData)` | `BuybackBurner` self-approves the **Balancer V3 Vault** address (NOT the Router) during its initialization — the Vault pulls input tokens from the `msg.sender` of the Router call. This is the V3 footgun; see [ADR 018](018-liquidity-strategy.md#buyback-execution-via-balancer-v3) | Yes |
| BuybackBurner | IERC20 (USDC, TOKEN) | `safeTransferFrom()` / `safeTransfer()` | Caller must have allowance/balance | Yes |

**Note:** No contract calls governance functions on another deCDN contract. Cross-contract state mutations are limited to `ejectNode()`, `slash()`, `routeSettlement()`, `recordSettlement()`, and `payout()` — each protected by a dedicated role.

#### Off-Chain Read API (Client / Node Bootstrap)

The cross-contract call table above covers contract-to-contract interactions only. Off-chain components — clients and nodes — also need a stable set of view functions for cold-start peer discovery and live state inspection. These are specified in detail in the referenced ADRs but were not surfaced here, leaving room for them to be missed during contract scaffolding.

| Caller | Callee | Function | Used by | Reference |
| --- | --- | --- | --- | --- |
| Off-chain client/node | StakingRegistry | `getActiveNodeCount() returns (uint256)` | Bootstrap pagination loop | [ADR 001](001-network.md), [ADR 012](012-client.md), [ADR 019](019-node-onboarding.md) |
| Off-chain client/node | StakingRegistry | `getActiveNodes(uint256 offset, uint256 limit) returns (NodeInfo[])` | Cold-start peer discovery | [ADR 001](001-network.md), [ADR 012](012-client.md), [ADR 019](019-node-onboarding.md) |
| Off-chain client/node | StakingRegistry | `getFirstRegisteredAt(address ethAddress) returns (uint256)` | Reputation cold-start bonus window (`ethAddress` is the operator address that registered the node) | [ADR 001](001-network.md), [ADR 008](008-reputation.md), [ADR 019](019-node-onboarding.md) |
| Off-chain client/node | StakingRegistry | `nodeIdOf(address operator) returns (bytes32 nodeId, bool active)` | Bundled per-operator binding + activity lookup; the canonical operator→NodeId step in the on-chain origin-discovery fallback (intersected with `OriginAssignment.getOrigins(...)` and filtered against `ContentBlacklist.isOriginBlacklisted`). Bundles the binding read and active flag to avoid a second RPC. Storage per [ADR 003 § NodeId-to-Ethereum Binding](003-payments.md#nodeid-to-ethereum-binding) | [ADR 003](003-payments.md#nodeid-to-ethereum-binding), [ADR 022](022-content-discovery.md) |
| Off-chain client/node | StakingRegistry | `isActive(address operator) returns (bool)` | Single-purpose per-operator activity check; consumed by `OriginAssignment.proposeAssignment` / `activateAssignment` / default-open allow-list setters per [ADR 011](011-content-takedown.md) where callers work with operator addresses and don't need the NodeId binding. Equivalent to the `active` field of `nodeIdOf(operator)` | [ADR 003](003-payments.md#nodeid-to-ethereum-binding), [ADR 011](011-content-takedown.md) |
| Off-chain client/node | PublisherRegistry | `namespaceOf(bytes32 blake3Hash) returns (uint256[])` | Probe-time and request-time check: set of non-zero namespaces claiming this hash (empty array → default-open semantics); per [ADR 002 § Multi-claim semantics](002-content-addressing.md#multi-claim-semantics) | [ADR 002](002-content-addressing.md), [ADR 005](005-protocol.md) |
| Off-chain client/node | OriginAssignment | `isAuthorizedOrigin(uint256 namespaceId, address operator) returns (bool)` | Probe-time check: is this operator authorized to act as origin for this namespace | [ADR 005](005-protocol.md), [ADR 011](011-content-takedown.md) |
| Off-chain client/node | OriginAssignment | `getOrigins(uint256 namespaceId) returns (address[])` | Discovery: list of authorized origin operators for a namespace; `getOrigins(0)` returns the default-open allow-list | [ADR 011](011-content-takedown.md), [ADR 022](022-content-discovery.md) |
| Off-chain client/node | OriginAssignment | `defaultOpenAllowlistActive() returns (bool)`, `defaultOpenActivatedAt() returns (uint64)` | Detect whether the default-open bootstrap window is still open; pivot probe-time and slashing logic accordingly | [ADR 005](005-protocol.md), [ADR 011](011-content-takedown.md) |

##### Bootstrap pattern

(per [ADR 012 § Bootstrap](012-client.md#bootstrap-procedure)): paginated `getActiveNodes(offset, 100)` calls until a page returns fewer than `limit` results. For PoC scale (tens of nodes) a single call suffices; the pagination pattern is preserved so the same code works at production scale.

**Liveness caveat:** the registry is a cold-start *seed list*, not a liveness oracle. The chain has no liveness signal, so returned operators include staked-but-offline nodes. Clients filter to live peers via gossip (`NodeAnnounce` TTL) and probe RTT after bootstrap.

##### Settlement-Weighted Bootstrap Ranking

For a paid CDN, the registry exposes an on-chain signal stronger than registration order: **settlement activity**. Every `closeChannel` / `settleChannel` is on-chain proof that the operator served bytes to a paying client — backward-looking, expensive to fake (real counterparty paying real USDC), and already going on-chain via `PaymentChannel`. Clients use it to bias bootstrap toward proven deliverers; staked-but-dead nodes sink to the bottom but remain reachable.

Contract surface:

| Element | Purpose |
| --- | --- |
| `StakingRegistry.lastSettlementAt[operator]` (`uint64`) | Timestamp of last settlement; updated by `FeeRouter` on each `routeSettlement` |
| `SETTLEMENT_REPORTER_ROLE` on `StakingRegistry` | Granted to `FeeRouter` |
| `StakingRegistry.recordSettlement(operator)` | Single-purpose, role-gated; one SSTORE (~5K gas) |
| `getActiveNodes(...)` returns `(operator, nodeId, lastSettlementAt)` tuples | Raw signals, not policy — clients sort off-chain. Stake-tier callers can fetch `getStakeMultiple(operator)` per-node on demand. |

Design principles for forward compatibility:

1. **Return raw signals, not policy.** Surface timestamps + flags as views; let off-chain decide ranking. New ranking logic ships as client updates, not contract migrations.
2. **Region stays off-chain for now.** Region already lives in signed `NodeAnnounce` (gossip). Registry remains globally-flat; clients filter regionally via gossip after bootstrap. If on-chain regional sharding ever becomes necessary, it's an additive `bytes2 region => EnumerableSet` map — non-breaking.

Cold-start operators (`lastSettlementAt == 0`) sink to the bottom by recency but are not excluded — they get probed once early settlers are exhausted, settle their first channel, and rise. A short on-boarding grace window can be added in a follow-up if needed.

### 4. Fund Flow Diagrams

#### USDC Flow (Payments)

The unified payments flow — settlement always passes through `FeeRouter`; bucket-share defaults at launch versus steady state are governance-tunable per [§ Tunable Economics](#tunable-economics):

```mermaid
flowchart TD
    Client["Client (USDC holder)"]
    PC["PaymentChannel<br/>(escrow)"]
    FR["FeeRouter<br/>(splits per setShares;<br/>steady-state 40/40/7/5/5/3)"]
    Provider["Provider (node operator)"]
    GAUGE["Gauge boost pool<br/>(epoch bucket, USDC)"]
    DELE["Delegator pool<br/>(epoch bucket, USDC → TOKEN)"]
    Treasury["Treasury (Timelock-custodied)"]
    SAFE["SafetyReserve"]
    BB["BuybackBurner"]
    BAL["Balancer V3 Router<br/>(→ 80/20 TOKEN/USDC Weighted Pool)"]
    BURN["Burn Address<br/>(0x...dEaD)"]
    OpClaim["Operator (claimBoost)"]
    LockerClaim["ve-locker (claimDelegator)"]

    Client -->|"openChannel() / topUp()<br/>deposit USDC"| PC
    PC -->|"settleChannel(): full operator balance"| FR
    PC -->|"settleChannel(): unused balance"| Client
    FR -->|"40% same-tx (per-byte)"| Provider
    FR -->|"40% (epoch bucket)"| GAUGE
    FR -->|"7% (epoch bucket)"| DELE
    FR -->|"5% same-tx"| BB
    FR -->|"5% same-tx"| Treasury
    FR -->|"3% same-tx"| SAFE
    DELE -->|"executeDelegatorSwap()<br/>(Router.swapSingleTokenExactIn)"| BAL
    BAL -->|"TOKEN"| DELE
    BB -->|"executeBuyback()"| BAL
    BAL -->|"TOKEN"| BB
    BB -->|"burn()"| BURN
    GAUGE -->|"claimBoost(epochs[])"| OpClaim
    DELE -->|"claimDelegator(epochs[])"| LockerClaim
```

The canonical 40/40/7/5/5/3 split is in [ADR 026](026-gauge-boost-tokenomics.md) §2; this ADR does not duplicate the bucket table. Treasury disbursement requires a governance proposal ([ADR 009](009-governance.md)).

#### TOKEN Flow (Staking & Slashing)

```mermaid
flowchart TD
    Operator["Node Operator"]
    SR["StakingRegistry<br/>(staked TOKEN)"]
    SJ["SlashJudge<br/>(challenge bonds)"]
    Challenger["Challenger"]
    SAFE["SafetyReserve"]
    BURN["Burn Address<br/>(0x...dEaD)"]

    Operator -->|"stake(amount)"| SR
    SR -->|"unstake() after unbonding"| Operator
    Challenger -->|"submitPhantomChallenge() /<br/>submitRateChallenge() /<br/>submitBlacklistChallenge()<br/>bond deposit"| SJ
    SJ -->|"slash(node, offenseType)<br/>(amount computed internally)"| SR
    SR -->|"50% of slash to msg.sender"| SJ
    SR -->|"30% of slash"| SAFE
    SR -->|"20% of slash"| BURN
    SJ -->|"slash reward + bond return"| Challenger
    SJ -->|"bond forfeit: 50% burn, 50% to node"| BURN
```

**Slashing distribution** ([ADR 026](026-gauge-boost-tokenomics.md) §8):

| Destination | Share |
| --- | ---: |
| Challenger reward | 50% |
| SafetyReserve | 30% |
| Burn | 20% |

#### Contracts Holding Funds Summary

| Contract | Token | Source | Release Condition |
| --- | --- | --- | --- |
| PaymentChannel | USDC | Client deposits | `settleChannel()`, `reclaimExpired()` |
| FeeRouter | USDC (gauge + delegator epoch buckets, transient base/treasury/burn/safety legs); TOKEN (delegator-pool epoch buckets after USDC→TOKEN swap) | `PaymentChannel.settleChannel` | `claimBoost(epochs[])` (operators); `claimDelegator(epochs[])` (ve-lockers); same-tx forwards to BuybackBurner / Treasury / SafetyReserve / operator base; 26-epoch claim window then sweep to treasury |
| VotingEscrow | TOKEN (locked, non-transferable) | User `createLock` deposits | `withdraw()` after lock expiry only; no early exit, no `create_lock_for` privileged path ([ADR 026](026-gauge-boost-tokenomics.md) §4) |
| SafetyReserve | USDC (3% router bucket; primary holding) + TOKEN (30% slashing redirect; swapped to USDC via keeper) | `FeeRouter`, `StakingRegistry` slashing path | `payout(bundle, recipient, amount)` USDC-only after evidence bundle, Governor (or emergency-multisig within hard caps), and 48h appeal window ([ADR 026](026-gauge-boost-tokenomics.md) §5) |
| StakingRegistry | TOKEN | Node operator stakes | `unstake()` after unbonding |
| SlashJudge | TOKEN | Challenger bond deposits | Synchronous resolution inside each `submit*Challenge` (slash reward + bond return to challenger on success; revert on failed verification) |
| BuybackBurner | USDC (accumulated), TOKEN (transient) | 5% USDC same-tx from `FeeRouter` ([ADR 026](026-gauge-boost-tokenomics.md) §8) | `executeBuyback()` |
| DelegatorBuyer | USDC (per-epoch delegator-pool bucket, transient), TOKEN (transient before deposit back to FeeRouter) | 7% USDC same-tx from `FeeRouter` ([ADR 026](026-gauge-boost-tokenomics.md) §6) | `swapDelegatorBucket(epoch, amountIn, minOut)` (KEEPER_ROLE); deposits resulting TOKEN into `FeeRouter`'s delegator bucket for `claimDelegator` |
| TimelockController | USDC (5% protocol-treasury bucket) | 5% USDC same-tx from `FeeRouter` | Treasury disbursement requires a `DecdnGovernor` proposal under the standard 48h timelock ([ADR 009](009-governance.md)) |

(`PublisherRegistry` and `OriginAssignment` hold no funds — they are pure registry contracts.)

### 5. Access Control Matrix

All role-based access uses OpenZeppelin `AccessControl`. The `DEFAULT_ADMIN_ROLE` holder can grant and revoke all other roles. Named roles below (`KEEPER_ROLE`, `GOVERNANCE_ROLE`, `EMERGENCY_ROLE`) formalize the implicit access patterns described across source ADRs into concrete `AccessControl` role identifiers for implementation.

#### Additive contract surface

New top-level contracts integrate with the launch-time set via standard `AccessControl` role grants — governance can grant new roles or revoke existing ones via the standard 7-day vote + 48-hour timelock path, without contract changes, state migration, or redeploy of the existing contracts. The launch-time interface surface (function signatures and events on `PaymentChannel`, `FeeRouter`, `SafetyReserve`, `StakingRegistry`, `BuybackBurner`, `VotingEscrow`, `SlashJudge`) is treated as stable for cross-contract integration. Concretely: `openChannel` is permissionless, `SafetyReserve.payout(bundleHash, recipient, amount)` accepts arbitrary evidence-bundle hashes (per [ADR 026 §5](026-gauge-boost-tokenomics.md#5-safety-and-insurance-reserve-3-bucket)), TOKEN is `ERC20Burnable` (per [ADR 026 §1](026-gauge-boost-tokenomics.md#1-supply-and-distribution)), and no contract is locked to a specific set of integrators. Future contract surfaces deploy as additive top-level contracts, not as upgrades or migrations of the launch set.

#### Role Assignments

| Role | Contract | Authorized Functions | At-launch holder | Steady-state holder |
| --- | --- | --- | --- | --- |
| `DEFAULT_ADMIN_ROLE` | All contracts | Grant/revoke roles, set parameters | Deployer EOA | `TimelockController` (2-day delay) |
| `BLACKLIST_ROLE` | StakingRegistry | `ejectNode()` | ContentBlacklist contract | ContentBlacklist contract |
| `GOVERNANCE_ROLE` | OriginAssignment | `activateAssignment()`, `revokeAssignment()`, `setMinRedundancy()`, `setMaxOriginsPerNamespace()`, `setAssignmentTimelock()`, `setDefaultOpenAllowlist()`, `addDefaultOpenOperator()`, `removeDefaultOpenOperator()`, `setDefaultOpenMinRedundancy()`, `setDefaultOpenMaxOrigins()` | Admin | Governor via timelock |
| `SLASH_ROLE` | StakingRegistry | `slash()` | SlashJudge contract | SlashJudge contract |
| `SETTLEMENT_REPORTER_ROLE` | StakingRegistry | `recordSettlement(operator)` | FeeRouter | FeeRouter; see [§3](#3-cross-contract-call-graph) |
| `SLASH_INFLOW_REPORTER_ROLE` | SafetyReserve | `recordSlashInflow(operator, amount)` | StakingRegistry | StakingRegistry; granted post-deploy. Mirrors `SETTLEMENT_REPORTER_ROLE` — gives auditors a clean event to track slash-redirect provenance |
| `KEEPER_ROLE` | BuybackBurner, FeeRouter, SafetyReserve | `executeBuyback()` (BuybackBurner), `executeDelegatorSwap(epochId, minOut)` (FeeRouter), `swapAccumulatedTokens(amountIn, minOut)` (SafetyReserve) | Admin / disabled | Keeper bot or governance |
| `ROUTER_CALLER_ROLE` | FeeRouter | `routeSettlement(op, bytes, amount)` | PaymentChannel | PaymentChannel (and any future settlement-emitting contract) |
| `PAYOUT_AUTHORIZER_ROLE` | SafetyReserve | `payout(bundle, recipient, amount)` | n/a | Governor via timelock; emergency multisig within hard caps ([ADR 026](026-gauge-boost-tokenomics.md) §5) |
| `GOVERNANCE_ROLE` | ContentBlacklist, FeeRouter (share parameters / `boostFloor`) | `addHash()`, `removeHash()`, `addOrigin()`, `removeOrigin()`, `registerRegionalBody()` (ContentBlacklist); `setShares(...)`, `setBoostFloor(...)` (FeeRouter) | Admin | Governor via timelock |
| `EMERGENCY_ROLE` | ContentBlacklist (emergency functions), fund-holding contracts (`pause()`), SafetyReserve (fast-track payout under hard caps) | `emergencyAdd()`, `emergencyAddOrigin()`, `suspendRegionalBody()` (ContentBlacklist); `pause()` (Pausable contracts only); `payout(...)` under hard caps (SafetyReserve) | Admin | 3-of-5 multisig (12-month sunset) |
| Regional body | ContentBlacklist | `addHashRegional(region)` | Not registered at launch | Per-jurisdiction multisig |

#### Governance-Controlled Parameters

Full parameter table with safety bounds is in [ADR 009](009-governance.md#governable-parameters-with-safety-bounds). Key bounds:

| Parameter | Min | Max | Contract |
| --- | --- | --- | --- |
| Slash % per offense | 5% | 50% | StakingRegistry |
| Dispute window | 12h | 72h | PaymentChannel |
| **Minimum stake** | **50,000 TOKEN (default)** | **per [ADR 026](026-gauge-boost-tokenomics.md) §7** | **StakingRegistry — discount-threshold logic removed** |
| Challenge bond | 1 TOKEN | 1,000 TOKEN | SlashJudge (note: [ADR 009](009-governance.md) lists this under StakingRegistry; SlashJudge is correct per [ADR 014](014-on-chain-verification.md)) |
| Unbonding period | 3 days | 30 days | StakingRegistry |
| **VotingEscrow lock duration** | **1 week (min)** | **4 years (max)** | **VotingEscrow** (`immutable`) |

The six FeeRouter shares (with their bounds and defaults) and `boostFloor` are governed in `FeeRouter` per [ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds); sum-to-100% across the six shares is enforced on every governance update. Other safety bounds are `immutable` — hardcoded in constructors, not overridable by governance or admin.

#### Emergency Multisig (Production)

- 3-of-5 threshold multisig
- Can pause fund-holding contracts (`Pausable.pause()`)
- Can add emergency blacklist entries (hashes and origins)
- Can suspend regional governance bodies
- **Cannot** withdraw treasury funds, modify fee parameters, or grant roles
- **12-month sunset:** All emergency functions revert after `block.timestamp > deployTimestamp + 365 days` ([ADR 009](009-governance.md#emergency-multisig))
- Emergency blacklist entries expire after 14 days unless ratified by governance

### 6. Reentrancy Analysis

Every state-mutating function that makes an external call is listed below with its guards and call pattern.

#### PaymentChannel

| Function | External Calls | Guards |
| --- | --- | --- |
| `openChannel()` | `IERC20.safeTransferFrom()`, `StakingRegistry.getStakeMultiple()` (read) | `nonReentrant`, checks-effects-interactions |
| `topUp()` | `IERC20.safeTransferFrom()` | `nonReentrant`, checks-effects-interactions |
| `settleChannel()` | `IERC20.safeTransfer()` (unused balance to client), `FeeRouter.routeSettlement(operator, bytesDelivered, amount, epochId)` (full operator balance forwarded; FeeRouter performs the six-way split internally) | `nonReentrant`, checks-effects-interactions; FeeRouter is `nonReentrant`-guarded on `routeSettlement` to defend against re-entry through the operator-base `safeTransfer` |
| `reclaimExpired()` | `IERC20.safeTransfer()` | `nonReentrant`, checks-effects-interactions |

#### StakingRegistry

| Function | External Calls | Guards |
| --- | --- | --- |
| `stake()` | `IERC20.safeTransferFrom()` (TOKEN) | `nonReentrant`, checks-effects-interactions |
| `unstake()` | `IERC20.safeTransfer()` (TOKEN) | `nonReentrant`, checks-effects-interactions |
| `slash()` | `IERC20.safeTransfer()` (TOKEN: 50% challenger / 30% SafetyReserve / 20% burn per [ADR 026](026-gauge-boost-tokenomics.md) §8) | `nonReentrant`, checks-effects-interactions, `SLASH_ROLE` |
| `recordSettlement(operator)` | None (single SSTORE) | `SETTLEMENT_REPORTER_ROLE` |
| `ejectNode()` | None (state change only) | `BLACKLIST_ROLE` |
| `getStakeMultiple()` | None (read-only) | N/A |

#### SlashJudge

| Function | External Calls | Guards |
| --- | --- | --- |
| `submitPhantomChallenge()` | `IERC20.safeTransferFrom()` (TOKEN bond deposit), `StakingRegistry.slash()`, `IERC20.safeTransfer()` (slash reward + bond return on success) | `nonReentrant`, checks-effects-interactions |
| `submitRateChallenge()` | `IERC20.safeTransferFrom()` (TOKEN bond deposit), `StakingRegistry.slash()`, `IERC20.safeTransfer()` (slash reward + bond return on success) | `nonReentrant`, checks-effects-interactions |
| `submitBlacklistChallenge()` | `IERC20.safeTransferFrom()` (TOKEN bond deposit), `ContentBlacklist.getEntry()` (read), `StakingRegistry.slash()`, `IERC20.safeTransfer()` (slash reward + bond return on success) | `nonReentrant`, checks-effects-interactions |

#### BuybackBurner

| Function | External Calls | Guards |
| --- | --- | --- |
| `executeBuyback()` | `BalancerV3Router.swapSingleTokenExactIn()` (swaps contract-held USDC; Router forwards to Vault which pulls input tokens via Vault-scoped allowance), `IERC20.safeTransfer()` (TOKEN to burn) | `nonReentrant`, checks-effects-interactions, `KEEPER_ROLE` |

> **MEV protection (production).** See [ADR 018 — Buyback execution via Balancer V3](018-liquidity-strategy.md#buyback-execution-via-balancer-v3) for the authoritative policy. In summary: Balancer's weighted-pool curve reduces (but does not eliminate) price-impact concerns compared to concentrated liquidity, and `executeBuyback` MAY split large buybacks into `subSwapCount` sub-swaps spaced by `subSwapMinBlockGap` blocks. **Direct Router execution with TWAP + `minTokenOut` guards is the primary production path and the required fallback.** Routing through CoW Swap is a conditional add-on that requires operator verification of CoW solver routing against the deployed Balancer V3 pool (per [ADR 018](018-liquidity-strategy.md)'s activation criteria); if CoW routing is unavailable or regresses, direct Router + TWAP remains correct. The `maxBuybackAmount` parameter MUST be enforced to limit per-transaction MEV exposure regardless of venue.

> **Inflow source.** Under [ADR 026](026-gauge-boost-tokenomics.md) §8, `BuybackBurner` receives the buyback share (5% of every settlement at steady state) same-tx from `FeeRouter`; share is governance-tunable per [§ Tunable Economics](#tunable-economics). The `executeBuyback` mechanics, `KEEPER_ROLE`-gating, and Vault-scoped self-approval pattern are independent of the share value.

#### FeeRouter

| Function | External Calls | Guards |
| --- | --- | --- |
| `routeSettlement(operator, bytesDelivered, amount, epochId)` | `IERC20.safeTransfer()` × 4 (operator base 40%, BuybackBurner 5%, Treasury 5%, SafetyReserve 3%; gauge 40% and delegator 7% retained in epoch USDC buckets, no transfer), `StakingRegistry.recordSettlement(operator)`. State updates: increments per-epoch USDC accumulators for the gauge and delegator buckets; increments `bytesPerEpoch[operator][epochId]` for the gauge formula; emits `Settled`. Off-chain reputation indexers correlate this `Settled` event with `PaymentChannel.ChannelSettled(channelId, ...)` from the same transaction to recover the channel context (channel.client lookup via the historical `ChannelOpened` event). Gauge eligibility (`bytes_i` in the [ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula) formula) is sourced from this view at gauge-claim time, with the per-operator share cap binding the output. | `nonReentrant`, checks-effects-interactions, `ROUTER_CALLER_ROLE` |
| `claimBoost(epochs[])` | `IERC20.safeTransfer()` (USDC to claiming operator), `VotingEscrow.balanceOfAt(...)` × N epochs (read), `VotingEscrow.totalSupplyAt(...)` × N epochs (read) | `nonReentrant`, checks-effects-interactions; epoch must be finalized |
| `claimDelegator(epochs[])` | `IERC20.safeTransfer()` (TOKEN to claiming ve-locker), `VotingEscrow.balanceOfAt(...)` × N (read), `VotingEscrow.totalSupplyAt(...)` × N (read) | `nonReentrant`, checks-effects-interactions; epoch's delegator-pool USDC→TOKEN swap must be settled |
| `executeDelegatorSwap(epoch, minOut)` | `BalancerV3Router.swapSingleTokenExactIn()` (Vault-scoped self-approval, same V3 footgun pattern as `BuybackBurner`) | `nonReentrant`, checks-effects-interactions, `KEEPER_ROLE`; per-epoch liquidity caps and TWAP-window guards REQUIRED ([ADR 026](026-gauge-boost-tokenomics.md) §6) |
| `setShares(...)`, `setBoostFloor(...)` | None (state change only) | `GOVERNANCE_ROLE` (Governor via timelock); sum-to-100% across the six router shares enforced; per-share bounds enforced ([ADR 026](026-gauge-boost-tokenomics.md) §11) |
| `sweepUnclaimed(epoch)` | `IERC20.safeTransfer()` (USDC/TOKEN to treasury) | `nonReentrant`, checks-effects-interactions; only callable after the 26-epoch claim window expires |

> **Cashflow invariant.** The 20% lower bound on the node-base share is `immutable` and guarantees operators always receive enough liquid USDC to cover at least a meaningful fraction of infrastructure costs even under extreme governance proposals. See [ADR 026](026-gauge-boost-tokenomics.md) §11.

#### VotingEscrow

| Function | External Calls | Guards |
| --- | --- | --- |
| `createLock(amount, duration)` | `IERC20.safeTransferFrom()` (TOKEN) | `nonReentrant`, checks-effects-interactions; `duration ∈ [1 week, 4 years]` (`immutable` bounds) |
| `extendLock(duration)` | None (state change only) | `nonReentrant`; new expiry capped at `now + 4 years`; lock shortening NOT allowed |
| `withdraw()` | `IERC20.safeTransfer()` (TOKEN) | `nonReentrant`, checks-effects-interactions; only callable after lock expiry; **no early-exit penalty path** |
| `balanceOfAt(user, ts)`, `totalSupplyAt(ts)` | None (read-only; per-lock checkpoint binary search) | N/A |

> **No `create_lock_for` privileged path.** Auto-ve-lock-on-vest is removed ([ADR 026](026-gauge-boost-tokenomics.md) §1, §4). All ve-positions are voluntarily created by the locker. **No slashing path on ve-locked TOKEN** — the slashing-immunity invariant is enforced by the absence of any `slash` / `burn` / `sweep` entry point on `VotingEscrow`. Operator stake (slashable) lives in `StakingRegistry`; ve-positions (non-slashable) live here. An operator may hold any combination but the contracts never share state.

#### SafetyReserve

| Function | External Calls | Guards |
| --- | --- | --- |
| `payout(bundle, recipient, amount)` | `IERC20.safeTransfer()` (USDC to recipient) | `nonReentrant`, checks-effects-interactions, `PAYOUT_AUTHORIZER_ROLE` (Governor via timelock; emergency multisig under hard caps); attested incident bundle REQUIRED; 48h appeal window REQUIRED before disbursement; post-incident registry write atomic with disbursement ([ADR 026](026-gauge-boost-tokenomics.md) §5) |
| `recordIncident(bundle)` | None (event + storage write) | Public; bundle signature verified against attestor allowlist |
| `challengeIncident(id, evidence)` | None (storage write) | Public during 48h appeal window; valid challenge pauses disbursement pending Governor resolution |

> **Spending control invariant.** No path on `SafetyReserve` exists for unattested or non-Governor-authorized payouts. The four-gate check (evidence bundle + Governor or emergency-multisig within hard caps + 48h appeal + post-incident registry write) is enforced atomically inside `payout`; partial paths revert.

#### PublisherRegistry

| Function | External Calls | Guards |
| --- | --- | --- |
| `registerPublisher()` | None (state change only) | Permissionless |
| `createNamespace()` | None (state change only) | Caller must hold `publisherId`; namespace cap enforced |
| `initiateNamespaceTransfer()` | None (state change only) | Caller must own the namespace |
| `finalizeNamespaceTransfer()` | None (state change only) | Pending transfer must exist; current time ≥ `readyAt` |
| `cancelNamespaceTransfer()` | None (state change only) | Caller must be the current owner |
| `claimContent()` | None (state change only) | Caller must own the namespace; multi-claim per [ADR 002 § Multi-claim semantics](002-content-addressing.md#multi-claim-semantics) — reverts only if THIS namespace has already claimed THIS hash (idempotency); other namespaces' prior claims do not block |

No external calls; no funds held. `nonReentrant` is not required but is included on state-mutating functions for defense-in-depth.

#### OriginAssignment

| Function | External Calls | Guards |
| --- | --- | --- |
| `proposeAssignment(namespaceId, operators[])` | `PublisherRegistry.ownerOf(namespaceId)` (read), `StakingRegistry.isActive(operator)` per operator (read) | Caller must own the namespace; `operators.length` within `[minRedundancy, maxOriginsPerNamespace]`; `operators` array MUST contain unique addresses (duplicates revert) |
| `activateAssignment(...)` | `StakingRegistry.isActive(operator)` per pending operator (read), `ContentBlacklist.isOriginBlacklisted(operator)` per pending operator (read) | `GOVERNANCE_ROLE`; pending proposal must exist; every pending operator must still be active and not blacklisted at activation time; min-redundancy invariant enforced post-activation |
| `revokeAssignment(namespaceId, operator)` | None (state change only) | Either `GOVERNANCE_ROLE` or namespace owner; revocation that would drop the active set below `minRedundancy` is allowed (publishers may shrink their assignment set; the constraint is on activation, not on revocation) |
| `pruneBlacklistedAssignment(namespaceId, operator)` | `ContentBlacklist.isOriginBlacklisted(operator)` (read) | Permissionless; reverts if operator is not currently blacklisted in `ContentBlacklist`; works for `namespaceId == 0` as well |
| `setMinRedundancy(uint256)`, `setMaxOriginsPerNamespace(uint256)`, `setAssignmentTimelock(uint256)` | None (state change only) | `GOVERNANCE_ROLE`; safety bounds enforced ([ADR 009](009-governance.md)); cross-parameter invariant `1 ≤ minRedundancy ≤ maxOriginsPerNamespace` enforced at the contract layer on every `setMinRedundancy` / `setMaxOriginsPerNamespace` call (revert on violation); all three apply to non-zero namespaces only |
| `setDefaultOpenAllowlist(operators[])`, `addDefaultOpenOperator(operator)`, `removeDefaultOpenOperator(operator)` | `StakingRegistry.isActive(operator)` per operator (read) | `GOVERNANCE_ROLE`; first non-empty activation flips `defaultOpenAllowlistActive` permanently and emits `DefaultOpenAllowlistActivated`; resulting set size must be within `[defaultOpenMinRedundancy, defaultOpenMaxOrigins]`; duplicates revert; runs under the Governor's standard 48h timelock |
| `setDefaultOpenMinRedundancy(uint256)`, `setDefaultOpenMaxOrigins(uint256)` | None (state change only) | `GOVERNANCE_ROLE`; safety bounds enforced ([ADR 009](009-governance.md)); cross-parameter invariants `5 ≤ defaultOpenMinRedundancy ≤ defaultOpenMaxOrigins ≤ 500` and `defaultOpenMinRedundancy ≥ minRedundancy` enforced at the contract layer |
| `isAuthorizedOrigin()`, `getOrigins()`, `getPendingAssignment()`, `defaultOpenAllowlistActive()`, `defaultOpenActivatedAt()` | None (read-only) | N/A |

The contract holds no funds. It maintains an `EnumerableSet` of currently-authorized operators per namespace, plus the bootstrap state for the default-open allow-list (`bool defaultOpenAllowlistActive`, `uint64 defaultOpenActivatedAt`). The first non-empty default-open activation flips `defaultOpenAllowlistActive` to `true` permanently, sets `defaultOpenActivatedAt` to that block's timestamp, and emits `DefaultOpenAllowlistActivated`. Until that moment, `isAuthorizedOrigin(0, op)` returns `true` for any active staker (permissive bootstrap). Off-chain consumers of `getOrigins(namespaceId)` cross-reference each returned operator against `ContentBlacklist.isOriginBlacklisted` and treat blacklisted entries as unauthorized regardless of stale `OriginAssignment` state, so storage cleanup via `pruneBlacklistedAssignment` is a lazy optimisation rather than a security primitive.

#### Payment-Channel Reentrancy

`PaymentChannel` moves USDC on `openChannel`, `topUp`, `settleChannel`, and `reclaimExpired`. All such functions use `nonReentrant` guards and follow checks-effects-interactions, and all ERC-20 interactions use OpenZeppelin `SafeERC20` ([ADR 003](003-payments.md)). The payment token is USDC, fixed at deployment — a standard ERC-20 with no fee-on-transfer, rebase, default-pausable, or transfer-hook behavior.

### 7. OpenZeppelin Framework Usage

Every deCDN contract should inherit from audited OpenZeppelin base contracts rather than implementing security primitives from scratch.

| OZ Contract | Used By | Purpose |
| --- | --- | --- |
| `Ownable` | PaymentChannel | Admin-key escape hatch for the USDC-only payment-channel contract (handed to `TimelockController` after deployment) |
| `AccessControl` | StakingRegistry, ContentBlacklist, PublisherRegistry, OriginAssignment, SlashJudge, BuybackBurner, DelegatorBuyer, FeeRouter, SafetyReserve | Role-based function authorization |
| `ReentrancyGuard` | All fund-holding contracts | `nonReentrant` modifier on state-mutating functions with external calls |
| `Pausable` | All fund-holding contracts | Emergency pause capability |
| `SafeERC20` | All contracts interacting with ERC-20 tokens | Safe wrappers for `transfer`, `transferFrom`, `approve` |
| `EIP712` | PaymentChannel, SlashJudge, StakingRegistry (`bindNode`), SafetyReserve (attested incident bundles) | Domain separator for voucher/slash/incident-bundle signature verification |
| `SignatureChecker` | PaymentChannel, StakingRegistry, SlashJudge, SafetyReserve | Unified EOA + ERC-1271 smart account signature verification ([ADR 024](024-account-abstraction.md)) |
| `ERC20` + `ERC20Permit` + `ERC20Votes` | TOKEN | Fixed-supply fungible token with gasless approvals and historical voting weight |
| `Governor` + `GovernorSettings` + `GovernorVotes` + `GovernorVotesQuorumFraction` + `GovernorTimelockControl` | DecdnGovernor | Token-weighted voting; voting weight sourced from `VotingEscrow.balanceOfAt` per [ADR 026](026-gauge-boost-tokenomics.md) §9 |
| `TimelockController` | TimelockController | Queued execution of governance proposals (48h delay); custodian of the protocol-treasury 5% bucket |

**Rationale:** OpenZeppelin Contracts are the most widely audited Solidity library, used by the majority of production DeFi protocols. Using audited primitives for access control, reentrancy protection, token handling, and governance eliminates entire classes of implementation bugs and reduces the surface area that a security audit must cover to deCDN-specific business logic.

### 8. Launch vs Steady-State Configuration

The contract surface is identical at launch and at steady state — every contract in [§1 Contract Inventory](#1-contract-inventory) ships in a single audit pass. Behavioral differences across the network's lifecycle are governance-tunable parameters, not contract redeployments.

| Aspect | At launch (typical) | At steady state |
| --- | --- | --- |
| `FeeRouter.setShares` | `8000 / 0 / 0 / 1000 / 1000 / 0` (operator / gauge / delegator / buyback / treasury / safety) — only same-tx legs active until `VotingEscrow` / `SafetyReserve` / `DelegatorBuyer` are wired | `4000 / 4000 / 700 / 500 / 500 / 300` per [ADR 026](026-gauge-boost-tokenomics.md) §2 |
| `FeeRouter` dependency addresses | `votingEscrow / safetyReserve / delegatorBuyer = address(0)` permitted at deploy; `setVotingEscrow(addr)` etc. activate them | All wired |
| `DEFAULT_ADMIN_ROLE` holder | Deployer EOA (handed off to `TimelockController` immediately post-deploy per [§ Post-Deployment Initialization](#post-deployment-initialization) step 7) | `TimelockController` |
| `DecdnGovernor` activity | Deployed but no proposals pending | Active proposal stream |
| Emergency multisig | Active (12-month sunset; fast-track `SafetyReserve.payout` under hard caps) | Active until sunset, then disabled |
| Default-open allow-list | `defaultOpenAllowlistActive == false`; `isAuthorizedOrigin(0, op)` permissive (any active staker) | Activated; strict membership |
| Regional governance bodies | Not registered | Per-jurisdiction multisigs registered as needed |
| `BuybackBurner.executeBuyback` | Callable from day one; share = 0 means no USDC to swap until `setShares` raises the buyback bucket | Routinely keeper-triggered |
| Slashing distribution | 50% challenger / 30% SafetyReserve / 20% burn ([ADR 026](026-gauge-boost-tokenomics.md) §8) — applies as soon as `SafetyReserve` is wired into `StakingRegistry`'s slash-redirect path | Same |
| `TOKEN` supply | 1B fixed at genesis; no mint function | Same |
| Minimum stake | 50,000 TOKEN per [ADR 026](026-gauge-boost-tokenomics.md) §7 | Same |

**No contract migration is planned.** Tunable parameters and governance-mutable dependency addresses (per [§ Tunable Economics](#tunable-economics)) carry the system from launch to steady state without redeployment.

## Consequences

### Positive

- Single reference document for all contract interactions, reducing audit scope ambiguity
- Explicit deployment order prevents initialization-order bugs
- Access control matrix makes privilege escalation paths visible and auditable
- OZ base contract prescriptions eliminate classes of implementation bugs before code is written

### Negative

- Must be kept in sync as other ADRs evolve — any change to contract interfaces in ADRs 003, 009, 011, 014, or 026 requires updating this document
- Does not cover off-chain interaction patterns (voucher exchange, gossip, probing) — those remain in their respective ADRs
- The contract surface includes three fund-holding contracts (`FeeRouter`, `VotingEscrow`, `SafetyReserve`) plus optional `DelegatorBuyer`, materially expanding audit scope

## References

- [ADR 003 — Payment Model](003-payments.md): PaymentChannel specification, `PaymentChannel.settleChannel` → `FeeRouter` routing
- [ADR 009 — Governance Model](009-governance.md): Safety bounds, Governor, emergency multisig
- [ADR 002 — Content Addressing](002-content-addressing.md): PublisherRegistry, namespaces, content claims
- [ADR 011 — Content Takedown](011-content-takedown.md): ContentBlacklist, origin ejection, OriginAssignment, DAO origin authority
- [ADR 014 — On-Chain Verification](014-on-chain-verification.md): SlashJudge, challenge bonds
- [ADR 018 — Liquidity Strategy](018-liquidity-strategy.md): Balancer V3 80/20 pool, MEV protection, POL custody, BuybackBurner execution
- [ADR 026 — Gauge-Boost Tokenomics](026-gauge-boost-tokenomics.md): FeeRouter six-bucket split, VotingEscrow, SafetyReserve, gauge-boost formula, the slashing distribution
- [OpenZeppelin Contracts](https://docs.openzeppelin.com/contracts/): Base contract framework
