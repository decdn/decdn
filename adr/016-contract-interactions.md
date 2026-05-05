# ADR 016: Smart Contract Interaction Model

**Date:** 2026-04-04
**Status:** Draft

## Context

The deCDN deploys multiple interacting smart contracts with cross-contract calls, role-based access control, and funds custody. Individual contracts are specified across [ADR 003](003-payments.md), [ADR 007](007-watchtower.md), [ADR 009](009-governance.md), [ADR 010](010-multi-token.md), [ADR 011](011-content-takedown.md), [ADR 014](014-on-chain-verification.md), and [ADR 026](026-gauge-boost-tokenomics.md). However, no single document maps the full interaction surface: who calls whom, which contracts hold funds, who is authorized to do what, and where reentrancy risks exist.

This ADR consolidates that analysis into a single reference for security audits and implementation. It does not introduce new functionality — it systematizes what other ADRs already specify.

> **ADR 026 driver.** The contract surface in this ADR is materially expanded by [ADR 026](026-gauge-boost-tokenomics.md), which adds `FeeRouter`, `VotingEscrow`, and `SafetyReserve`, and rewires `PaymentChannel`, `StakingRegistry`, `BuybackBurner`, and `Governor`. Read ADR 026 first for the economic model; this ADR is the integration view.

## Decision

### 1. Contract Inventory

All on-chain contracts inherit from [OpenZeppelin Contracts](https://docs.openzeppelin.com/contracts/) to minimize custom security-critical code.

| Contract | ADR | Holds Funds | Token Types | OZ Base Contracts | Phase |
| --- | --- | --- | --- | --- | --- |
| TOKEN (ERC-20) | [026](026-gauge-boost-tokenomics.md) | No (fungible token) | — | `ERC20`, `ERC20Permit` (recommended; enables gasless approvals) | PoC + Production |
| StakingRegistry | [003](003-payments.md), [026](026-gauge-boost-tokenomics.md) | Yes | TOKEN | `AccessControl`, `ReentrancyGuard`, `Pausable` | PoC + Production |
| StablePaymentChannel | [003](003-payments.md) | Yes | USDC | `Ownable`, `ReentrancyGuard`, `Pausable`, `EIP712` | PoC only |
| PaymentChannel | [010](010-multi-token.md), [026](026-gauge-boost-tokenomics.md) | Yes | Governance-approved ERC-20s | `AccessControl`, `ReentrancyGuard`, `Pausable`, `EIP712` | Production only |
| FeeRouter | [026](026-gauge-boost-tokenomics.md) | Yes | USDC (transient + epoch buckets), TOKEN (delegator-pool epoch buckets) | `AccessControl`, `ReentrancyGuard`, `Pausable` | Production only |
| VotingEscrow | [026](026-gauge-boost-tokenomics.md) | Yes | TOKEN (locked, non-transferable) | `ReentrancyGuard`, `Pausable` | Production only |
| SafetyReserve | [026](026-gauge-boost-tokenomics.md) | Yes | USDC (3% bucket + slashing redirect) | `AccessControl`, `ReentrancyGuard`, `Pausable` | Production only |
| BuybackBurner | [018](018-liquidity-strategy.md), [026](026-gauge-boost-tokenomics.md) | Yes | USDC, TOKEN (transient) | `AccessControl`, `ReentrancyGuard`, `Pausable` | PoC (accumulate-only) + Production |
| ContentBlacklist | [011](011-content-takedown.md) | No | — | `AccessControl`, `ReentrancyGuard` | PoC + Production |
| PublisherRegistry | [002](002-content-addressing.md) | No | — | `AccessControl`, `ReentrancyGuard` | PoC + Production |
| OriginAssignment | [011](011-content-takedown.md) | No | — | `AccessControl`, `ReentrancyGuard` | PoC + Production |
| SlashJudge | [014](014-on-chain-verification.md) | Yes | TOKEN (challenge bonds) | `AccessControl`, `ReentrancyGuard`, `Pausable`, `EIP712` | PoC + Production |
| WatchtowerEscrow | [007](007-watchtower.md) | Yes | USDC | `ReentrancyGuard`, `Pausable`, `EIP712` | Production only |

**Deferred / optional contracts (forward-referenced):**

| Contract | ADR | Status |
| --- | --- | --- |
| DelegatorBuyer (or `BuybackBurner` multi-output extension) | [026](026-gauge-boost-tokenomics.md) §6 | Implementation choice deferred — either a parallel contract or a `BuybackBurner` mode performs the delegator-pool 7% USDC→TOKEN swap. Selected during implementation. |

#### Contract Architecture (classDiagram)

The diagram below shows the production contract surface and its primary call relationships. Reproduced from [ADR 026](026-gauge-boost-tokenomics.md)'s source design spec §9.5 with the existing-ADR-016 contracts (`StakingRegistry`, `Governor`, etc.) included for orientation. `StakingRegistry` is unconnected on the fee-router path because it is independent of settlement — it governs slashable stake and is read by gossip / peer-validation logic ([ADR 001](001-network.md), [ADR 003](003-payments.md)) rather than by `FeeRouter`.

```mermaid
classDiagram
    class PaymentChannel {
        +settleChannel(op, bytes, amount)
    }
    class FeeRouter {
        +routeSettlement(op, bytes, amount, root)
        +claimBoost(epochs)
        +claimDelegator(epochs)
        +workingBytes(op, epoch)
        +executeDelegatorSwap(epoch, minOut)
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
    FeeRouter ..> BalancerV3Pool : delegator USDC→TOKEN swap
    BuybackBurner ..> BalancerV3Pool : swap USDC→TOKEN
    Governor ..> VotingEscrow : voting weight
    Governor ..> SafetyReserve : payout authorization
```

The full FeeRouter six-bucket split (40/40/7/5/5/3), epoch-bucket mechanics, and gauge-boost formula are specified in [ADR 026](026-gauge-boost-tokenomics.md) §2–§3; this ADR does not duplicate the bucket table.

##### No proxy deployment patterns

No deCDN contract uses proxy (upgradeable) deployment patterns. Production contract upgrades deploy new contracts at new addresses with state migration as described in Section 6. This constraint ensures that EIP-712 domain separators computed in constructors (as `immutable`) remain valid for the contract's lifetime — a proxy migration to a different address or chain would invalidate all existing voucher signatures.

**Build toolchain:** [Foundry](https://book.getfoundry.sh/) (forge, cast, anvil) for compilation, testing, and deployment.

### 2. Deployment Order and Initialization Dependencies

Contracts must be deployed in dependency order — each contract's constructor requires the addresses of contracts deployed before it.

```mermaid
graph TD
    TOKEN["1. TOKEN (ERC-20)"]
    USDC["2. USDC (existing or testnet)"]
    SR["3. StakingRegistry"]
    VE["4. VotingEscrow"]
    SAFE["5. SafetyReserve"]
    BB["6. BuybackBurner"]
    FR["7. FeeRouter"]
    SPC["8. StablePaymentChannel (PoC)<br/>PaymentChannel (production)"]
    PR["9. PublisherRegistry"]
    OA["10. OriginAssignment"]
    CB["11. ContentBlacklist"]
    SJ["12. SlashJudge"]
    WE["13. WatchtowerEscrow (production)"]

    SR --> TOKEN
    VE --> TOKEN
    SAFE --> USDC
    BB --> TOKEN
    BB --> USDC
    FR --> USDC
    FR --> BB
    FR --> SAFE
    FR --> VE
    SPC --> USDC
    SPC --> SR
    SPC --> FR
    OA --> SR
    OA --> PR
    CB --> SR
    SJ --> SR
    SJ --> TOKEN
    WE --> SPC
```

#### Constructor Dependencies

| Step | Contract | Constructor Requires |
| --- | --- | --- |
| 1 | TOKEN | None. **PoC:** freely mintable testnet token with `onlyOwner` mint. **Production:** fixed 1B supply, no mint function ([ADR 026](026-gauge-boost-tokenomics.md) §1). |
| 2 | USDC | External (testnet faucet or mainnet address) |
| 3 | StakingRegistry | TOKEN address, `minStake` (**50,000 TOKEN** per [ADR 026](026-gauge-boost-tokenomics.md) §7), `unbondingPeriod` (7 days). No discount-threshold parameters; operator return is differentiated through the gauge-boost flow in `FeeRouter`. |
| 4 | VotingEscrow (production) | TOKEN address, `minLockDuration` (1 week), `maxLockDuration` (4 years). No `create_lock_for` privileged path; auto-ve-lock not exposed. Implements `balanceOfAt(user, ts)` and `totalSupplyAt(ts)` historical checkpointing ([ADR 026](026-gauge-boost-tokenomics.md) §4). |
| 5 | SafetyReserve (production) | USDC address, Governor address (payout authorizer), emergency-multisig address (fast-track approver under hard caps), `appealWindow` (48h) ([ADR 026](026-gauge-boost-tokenomics.md) §5). |
| 6 | BuybackBurner | TOKEN address, USDC address, Balancer V3 Router address, initial pool contract `address` (may be zero-address at deploy and set later via `setPool(address)` — see [ADR 003](003-payments.md#buybackburner) for the interface and [ADR 018](018-liquidity-strategy.md) for the venue rationale). The pool address remains governance-mutable post-deploy via `setPool(address)`; the constructor value is an initial convenience, not a hard requirement. **Production inflow source:** `FeeRouter` rather than manual treasury transfer ([ADR 026](026-gauge-boost-tokenomics.md) §8); the contract surface is otherwise unchanged. **Router address and naming:** see [ADR 018 §"Buyback execution via Balancer V3"](018-liquidity-strategy.md#buyback-execution-via-balancer-v3) for the canonical Balancer V3 Router address and the `Router v2` label disambiguation. **Approvals note:** `BuybackBurner` MUST self-approve the Balancer V3 **Vault** address (distinct from the Router) during initialization — the Vault pulls input tokens from `msg.sender`, which is `BuybackBurner`. The V3 footgun reference and Vault address live in [ADR 018](018-liquidity-strategy.md#buyback-execution-via-balancer-v3). |
| 7 | FeeRouter (production) | USDC address, TOKEN address, VotingEscrow address, BuybackBurner address, SafetyReserve address, treasury wallet address, Balancer V3 Router + pool addresses (for the delegator-pool USDC→TOKEN swap; may share `BuybackBurner`'s configuration), `epochLength` (1 week), `claimWindow` (26 epochs), default split shares (40/40/7/5/5/3 per [ADR 026](026-gauge-boost-tokenomics.md) §2), and `boostFloor` (0.4) per [ADR 026](026-gauge-boost-tokenomics.md) §3. Sum-to-100% across the six router shares is enforced on every governance update. |
| 8a | StablePaymentChannel (PoC) | Constructor args: USDC address, `treasuryAddress`, `disputeWindow` (48h). Initialized in constructor body: StakingRegistry address, `feePercentage` (300 bps), `discountedFeePercentage` (150 bps), `maxChannelDuration` (90 days), rate bounds ([ADR 003](003-payments.md)) |
| 8b | PaymentChannel (production) | StakingRegistry address, Governor address, **FeeRouter address** ([ADR 026](026-gauge-boost-tokenomics.md) §2). `settleChannel` no longer skims a protocol fee; it transfers the full operator USDC balance to `FeeRouter.routeSettlement(operator, bytesDelivered, amount, receiptBatchRoot)` in the same transaction. The `feePercentage` / `discountedFeePercentage` constructor arguments from the PoC contract are removed. |
| 9 | PublisherRegistry | None. Permissionless registration; namespace cap and transfer-timelock parameters are read from the governance-controlled parameter store at call time. See [ADR 002 § Contract: PublisherRegistry](002-content-addressing.md#contract-publisherregistry). |
| 10 | OriginAssignment | StakingRegistry, PublisherRegistry, ContentBlacklist (latter may be zero at deploy; bound via `setContentBlacklist`). Min-redundancy, timelock, and default-open parameters are governance-controlled. See [ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority) and [§ OriginAssignment construction notes](#originassignment-construction-notes) below. |
| 11 | ContentBlacklist | `ContentBlacklist(address stakingRegistry)`. StakingRegistry address is required for `ejectNode()`. ContentBlacklist no longer cross-calls `OriginAssignment` (security relies on runtime checks; see [ADR 011 § Interaction with ContentBlacklist](011-content-takedown.md#interaction-with-contentblacklist)). After deployment, `OriginAssignment.setContentBlacklist(address)` is called once via the deployer / admin to wire the read direction (`OriginAssignment.pruneBlacklistedAssignment` queries `ContentBlacklist.isOriginBlacklisted`). |
| 12 | SlashJudge | StakingRegistry address, TOKEN address, `challengeBond` (100 TOKEN), `counterEvidenceWindow` (24h) |
| 13 | WatchtowerEscrow | StablePaymentChannel/PaymentChannel address, `heartbeatInterval`, `missThreshold`, `feeRateBps`, `minFee`, `monitoringPeriod` |

#### OriginAssignment construction notes

- **Default-open allow-list bootstrap.** Entries keyed by `namespaceId == 0` start empty with `defaultOpenAllowlistActive == false` (permissive bootstrap window — any active staker may serve as origin for default-open content). The first non-empty governance activation flips `defaultOpenAllowlistActive` to `true` permanently; thereafter only allow-listed operators appear in `getOrigins(0)`.
- **Default-open governance entry points.** `setDefaultOpenAllowlist`, `addDefaultOpenOperator`, `removeDefaultOpenOperator`, `setDefaultOpenMinRedundancy`, `setDefaultOpenMaxOrigins` all carry `GOVERNANCE_ROLE` and run under the Governor's standard 48h timelock.
- **ContentBlacklist binding.** Until `setContentBlacklist(address)` is called post-deploy (see § Post-Deployment Initialization below), `pruneBlacklistedAssignment` reverts — it cannot read `isOriginBlacklisted` against the zero address. This does not block usage: off-chain consumers of `getOrigins(...)` cross-reference `ContentBlacklist.isOriginBlacklisted` directly via RPC.

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

   **Production:** also grant a slashing-redirect role so SlashJudge can route 30% of slashed stake to `SafetyReserve` per [ADR 026](026-gauge-boost-tokenomics.md) §8 (challenger 50% / SafetyReserve 30% / burn 20%). **Slash currency:** stake is denominated in TOKEN, so the 30% share lands in `SafetyReserve` as TOKEN. `SafetyReserve` exposes a keeper-triggered swap into the [ADR 018](018-liquidity-strategy.md) Balancer V3 80/20 pool (same Vault-scoped self-approval, TWAP, `minOut`, private-RPC, and per-epoch liquidity-cap defenses as `BuybackBurner` and the delegator-pool swap path). USDC is the only currency available for `payout`; until swapped, slashed TOKEN is held as part of `SafetyReserve`'s assets-under-management.

4. **Grant `ROUTER_CALLER_ROLE` on FeeRouter to PaymentChannel:**

   ```solidity
   feeRouter.grantRole(ROUTER_CALLER_ROLE, address(paymentChannel));
   ```

   This authorizes `PaymentChannel.settleChannel` to invoke `FeeRouter.routeSettlement(operator, bytesDelivered, amount, receiptBatchRoot)`. Without this grant the production settlement path reverts.

5. **Grant `SETTLEMENT_REPORTER_ROLE` on StakingRegistry to FeeRouter** (and to `PaymentChannel` if the bootstrap-ranking signal is sourced from settlement events):

   ```solidity
   stakingRegistry.grantRole(SETTLEMENT_REPORTER_ROLE, address(feeRouter));
   ```

   See §3 below; `lastSettlementAt[operator]` is updated on each `routeSettlement` call.

6. **Add initial token to PaymentChannel** (production only):

   ```solidity
   paymentChannel.addToken(USDC_ADDRESS, rateFloor, rateCeiling);
   ```

7. **Register regional governance bodies** (production, if applicable):

   ```solidity
   contentBlacklist.registerRegionalBody(regionCode, bodyAddress);
   ```

8. **Transfer admin roles** to Governor + timelock (production):

   ```solidity
   // For each contract with AccessControl:
   contract.grantRole(DEFAULT_ADMIN_ROLE, address(timelockController));
   contract.revokeRole(DEFAULT_ADMIN_ROLE, deployer);
   ```

> **Production hardening:** Production deployments SHOULD execute `grantRole(DEFAULT_ADMIN_ROLE, timelockController)` and `revokeRole(DEFAULT_ADMIN_ROLE, deployer)` in a single multicall transaction to minimize the dual-admin window between the two operations.

> **Deployment atomicity.** The post-deployment initialization steps (1–8) SHOULD be executed atomically via a multicall contract or a deployment script that reverts on any failure. A partially initialized system (e.g., `SLASH_ROLE` granted but `BLACKLIST_ROLE` not yet, or `ROUTER_CALLER_ROLE` not yet granted to `PaymentChannel`) could create a window where some security mechanisms work but settlements revert or land in the wrong contract. Between deployment and initialization completion, `StakingRegistry` SHOULD reject `registerNode` calls (e.g., via a `paused` initial state or a deployment flag) to prevent nodes from registering before the security infrastructure is fully wired. For the PoC, a Foundry deployment script with sequential `vm.broadcast()` calls provides sufficient atomicity.

### 3. Cross-Contract Call Graph

```mermaid
graph LR
    SPC["StablePaymentChannel /<br/>PaymentChannel"]
    SR["StakingRegistry"]
    CB["ContentBlacklist"]
    PR["PublisherRegistry"]
    OA["OriginAssignment"]
    SJ["SlashJudge"]
    BB["BuybackBurner"]
    FR["FeeRouter"]
    VE["VotingEscrow"]
    SAFE["SafetyReserve"]
    GOV["Governor"]
    WE["WatchtowerEscrow"]
    ERC["ERC-20 Tokens<br/>(USDC, TOKEN)"]
    BAL["Balancer V3 Router"]

    SPC -->|"getStakeMultiple(provider)"| SR
    SPC -->|"safeTransferFrom / safeTransfer"| ERC
    SPC -->|"routeSettlement(op, bytes, amount, root)"| FR
    FR -->|"balanceOfAt / totalSupplyAt"| VE
    FR -->|"5% USDC same-tx"| BB
    FR -->|"3% USDC same-tx"| SAFE
    FR -->|"Router.swapSingleTokenExactIn() (delegator pool)"| BAL
    FR -->|"safeTransfer (operator base, treasury, claims)"| ERC
    GOV -->|"balanceOfAt / totalSupplyAt"| VE
    GOV -->|"payout(bundle, recipient, amount)"| SAFE
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
    WE -->|"read channel state"| SPC
```

#### Complete Call Table

| Caller | Callee | Function | Authorization | Mutates Callee State |
| --- | --- | --- | --- | --- |
| StablePaymentChannel | StakingRegistry | `getStakeMultiple(provider)` | Public (read-only) | No |
| StablePaymentChannel | IERC20 (USDC) | `safeTransferFrom()` | Caller must have allowance | Yes |
| StablePaymentChannel | IERC20 (USDC) | `safeTransfer()` | Caller holds balance | Yes |
| PaymentChannel | StakingRegistry | `getStakeMultiple(provider)` | Public (read-only) | No |
| PaymentChannel | IERC20 (per-token) | `safeTransferFrom()` / `safeTransfer()` | Caller must have allowance/balance | Yes |
| PaymentChannel | FeeRouter | `routeSettlement(operator, bytesDelivered, amount, receiptBatchRoot)` | `ROUTER_CALLER_ROLE` on FeeRouter ([ADR 026](026-gauge-boost-tokenomics.md) §2) | Yes |
| FeeRouter | VotingEscrow | `balanceOfAt(user, ts)`, `totalSupplyAt(ts)` | Public (read-only) | No |
| FeeRouter | StakingRegistry | `recordSettlement(operator)` | `SETTLEMENT_REPORTER_ROLE` (granted to FeeRouter post-deploy; settlement counter moves with the routing call) | Yes |
| FeeRouter | BuybackBurner | `safeTransfer()` (5% USDC same-tx) | Caller holds balance | Yes |
| FeeRouter | SafetyReserve | `safeTransfer()` (3% USDC same-tx) | Caller holds balance | Yes |
| FeeRouter | Treasury wallet | `safeTransfer()` (5% USDC same-tx) | Caller holds balance | Yes |
| FeeRouter | IERC20 (USDC, TOKEN) | `safeTransfer()` (operator 40% base, claim payouts) | Caller holds balance | Yes |
| FeeRouter | Balancer V3 Router | `swapSingleTokenExactIn(...)` (delegator-pool USDC→TOKEN; may be delegated to a `DelegatorBuyer` or `BuybackBurner` extension) | Vault-scoped self-approval, `KEEPER_ROLE` for `executeDelegatorSwap` | Yes |
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
| WatchtowerEscrow | StablePaymentChannel / PaymentChannel | Channel state reads | Public (read-only) | No |

**Note:** No contract calls governance functions on another deCDN contract. Cross-contract state mutations are limited to `ejectNode()`, `slash()`, `routeSettlement()`, `recordSettlement()`, and `payout()` — each protected by a dedicated role.

#### Off-Chain Read API (Client / Node Bootstrap)

The cross-contract call table above covers contract-to-contract interactions only. Off-chain components — clients and nodes — also need a stable set of view functions for cold-start peer discovery and live state inspection. These are specified in detail in the referenced ADRs but were not surfaced here, leaving room for them to be missed during contract scaffolding.

| Caller | Callee | Function | Used by | Reference |
| --- | --- | --- | --- | --- |
| Off-chain client/node | StakingRegistry | `getActiveNodeCount() returns (uint256)` | Bootstrap pagination loop | [ADR 001](001-network.md), [ADR 012](012-client.md), [ADR 019](019-node-onboarding.md) |
| Off-chain client/node | StakingRegistry | `getActiveNodes(uint256 offset, uint256 limit) returns (NodeInfo[])` | Cold-start peer discovery | [ADR 001](001-network.md), [ADR 012](012-client.md), [ADR 019](019-node-onboarding.md) |
| Off-chain client/node | StakingRegistry | `getFirstRegisteredAt(address ethAddress) returns (uint256)` | Reputation cold-start bonus window (`ethAddress` is the operator address that registered the node) | [ADR 001](001-network.md), [ADR 008](008-reputation.md), [ADR 019](019-node-onboarding.md) |
| Off-chain client/node | PublisherRegistry | `namespaceOf(bytes32 blake3Hash) returns (uint256[])` | Probe-time and request-time check: set of non-zero namespaces claiming this hash (empty array → default-open semantics); per [ADR 002 § Multi-claim semantics](002-content-addressing.md#multi-claim-semantics) | [ADR 002](002-content-addressing.md), [ADR 005](005-protocol.md) |
| Off-chain client/node | OriginAssignment | `isAuthorizedOrigin(uint256 namespaceId, address operator) returns (bool)` | Probe-time check: is this operator authorized to act as origin for this namespace | [ADR 005](005-protocol.md), [ADR 011](011-content-takedown.md) |
| Off-chain client/node | OriginAssignment | `getOrigins(uint256 namespaceId) returns (address[])` | Discovery: list of authorized origin operators for a namespace; `getOrigins(0)` returns the default-open allow-list | [ADR 011](011-content-takedown.md), [ADR 022](022-content-discovery.md) |
| Off-chain client/node | OriginAssignment | `defaultOpenAllowlistActive() returns (bool)`, `defaultOpenActivatedAt() returns (uint64)` | Detect whether the default-open bootstrap window is still open; pivot probe-time and slashing logic accordingly | [ADR 005](005-protocol.md), [ADR 011](011-content-takedown.md) |

##### Bootstrap pattern

(per [ADR 012 §Bootstrap](012-client.md#bootstrap-procedure)): paginated `getActiveNodes(offset, 100)` calls until a page returns fewer than `limit` results. For PoC scale (tens of nodes) a single call suffices; the pagination pattern is preserved so the same code works at production scale.

**Liveness caveat:** the registry is a cold-start *seed list*, not a liveness oracle. The chain has no liveness signal, so returned operators include staked-but-offline nodes. Clients filter to live peers via gossip (`NodeAnnounce` TTL) and probe RTT after bootstrap.

##### Settlement-Weighted Bootstrap Ranking

For a paid CDN, the registry exposes an on-chain signal stronger than registration order: **settlement activity**. Every `closeChannel` / `settleChannel` is on-chain proof that the operator served bytes to a paying client — backward-looking, expensive to fake (real counterparty paying real USDC), and already going on-chain via `StablePaymentChannel`. Clients use it to bias bootstrap toward proven deliverers; staked-but-dead nodes sink to the bottom but remain reachable.

Contract surface:

| Element | Purpose |
| --- | --- |
| `StakingRegistry.lastSettlementAt[operator]` (`uint64`) | Timestamp of last settlement; updated by `StablePaymentChannel` (PoC) or by `FeeRouter` on each `routeSettlement` (production) |
| `SETTLEMENT_REPORTER_ROLE` on `StakingRegistry` | Granted to `StablePaymentChannel` (PoC) and to `FeeRouter` (production) |
| `StakingRegistry.recordSettlement(operator)` | Single-purpose, role-gated; one SSTORE (~5K gas) |
| `getActiveNodes(...)` returns `(operator, nodeId, lastSettlementAt)` tuples | Raw signals, not policy — clients sort off-chain. Stake-tier callers can fetch `getStakeMultiple(operator)` per-node on demand. |

Design principles for forward compatibility:

1. **Return raw signals, not policy.** Surface timestamps + flags as views; let off-chain decide ranking. New ranking logic ships as client updates, not contract migrations.
2. **Region stays off-chain for now.** Region already lives in signed `NodeAnnounce` (gossip). Registry remains globally-flat; clients filter regionally via gossip after bootstrap. If on-chain regional sharding ever becomes necessary, it's an additive `bytes2 region => EnumerableSet` map — non-breaking.

Cold-start operators (`lastSettlementAt == 0`) sink to the bottom by recency but are not excluded — they get probed once early settlers are exhausted, settle their first channel, and rise. A short on-boarding grace window can be added in a follow-up if needed.

### 4. Fund Flow Diagrams

#### USDC Flow (Payments)

The PoC flow (`StablePaymentChannel` + manual treasury → buyback) is documented in [ADR 003](003-payments.md). The production flow:

```mermaid
flowchart TD
    Client["Client (USDC holder)"]
    PC["PaymentChannel<br/>(escrow)"]
    FR["FeeRouter<br/>(splits 40/40/7/5/5/3)"]
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
    ClientStaker["Client (optional staker)"]
    SR["StakingRegistry<br/>(staked TOKEN)"]
    SJ["SlashJudge<br/>(challenge bonds)"]
    Challenger["Challenger /<br/>Watchtower"]
    SAFE["SafetyReserve"]
    BURN["Burn Address<br/>(0x...dEaD)"]

    Operator -->|"stake(amount)"| SR
    SR -->|"unstake() after unbonding"| Operator
    ClientStaker -->|"clientStake(amount)"| SR
    SR -->|"clientUnstake() (no unbonding)"| ClientStaker
    Challenger -->|"submitPhantomChallenge() /<br/>submitRateChallenge() /<br/>submitBlacklistChallenge() /<br/>submitCorruptionChallenge()<br/>bond deposit"| SJ
    SJ -->|"resolveChallenge()<br/>→ slash(node, offenseType)<br/>(amount computed internally)"| SR
    SR -->|"50% of slash to msg.sender"| SJ
    SR -->|"30% of slash (production only)"| SAFE
    SR -->|"50% of slash (PoC) / 20%"| BURN
    SJ -->|"slash reward + bond return"| Challenger
    SJ -->|"bond forfeit: 50% burn, 50% to node"| BURN
```

**Slashing distribution by phase:**

| Destination | PoC | Production ([ADR 026](026-gauge-boost-tokenomics.md) §8) |
| --- | ---: | ---: |
| Challenger reward | 50% | 50% |
| SafetyReserve | 0% | 30% |
| Burn | 50% | 20% |

#### Contracts Holding Funds Summary

| Contract | Token | Source | Release Condition |
| --- | --- | --- | --- |
| StablePaymentChannel / PaymentChannel | USDC (PoC) / approved ERC-20s (production) | Client deposits | `settleChannel()`, `reclaimExpired()`, `forceCloseChannel()` |
| FeeRouter | USDC (gauge + delegator epoch buckets, transient base/treasury/burn/safety legs); TOKEN (delegator-pool epoch buckets after USDC→TOKEN swap) | `PaymentChannel.settleChannel` | `claimBoost(epochs[])` (operators); `claimDelegator(epochs[])` (ve-lockers); same-tx forwards to BuybackBurner / Treasury / SafetyReserve / operator base (40%); 26-epoch claim window then sweep to treasury |
| VotingEscrow | TOKEN (locked, non-transferable) | User `createLock` deposits | `withdraw()` after lock expiry only; no early exit, no `create_lock_for` privileged path ([ADR 026](026-gauge-boost-tokenomics.md) §4) |
| SafetyReserve | USDC (3% router bucket; primary holding) + TOKEN (30% slashing redirect; swapped to USDC via keeper) | `FeeRouter`, `StakingRegistry` slashing path | `payout(bundle, recipient, amount)` USDC-only after evidence bundle, Governor (or emergency-multisig within hard caps), and 48h appeal window ([ADR 026](026-gauge-boost-tokenomics.md) §5) |
| StakingRegistry | TOKEN | Node operator stakes, client priority stakes | `unstake()` after unbonding (operators), `clientUnstake()` anytime (clients) |
| SlashJudge | TOKEN | Challenger bond deposits | `resolveChallenge()` (slash reward + bond return to challenger) or bond forfeiture |
| BuybackBurner | USDC (accumulated), TOKEN (transient) | PoC: treasury transfers. Production: 5% USDC same-tx from `FeeRouter` ([ADR 026](026-gauge-boost-tokenomics.md) §8) | `executeBuyback()` (production; accumulate-only in PoC) |
| WatchtowerEscrow | USDC | Prepaid watchtower fees | Heartbeat-based payouts, reclaim on liveness failure |

(`PublisherRegistry` and `OriginAssignment` hold no funds — they are pure registry contracts.)

### 5. Access Control Matrix

All role-based access uses OpenZeppelin `AccessControl`. The `DEFAULT_ADMIN_ROLE` holder can grant and revoke all other roles. Named roles below (`KEEPER_ROLE`, `GOVERNANCE_ROLE`, `EMERGENCY_ROLE`) formalize the implicit access patterns described across source ADRs into concrete `AccessControl` role identifiers for implementation.

#### Additive contract surface

New top-level contracts integrate with the launch-time set via standard `AccessControl` role grants — governance can grant new roles or revoke existing ones via the standard 7-day vote + 48-hour timelock path, without contract changes, state migration, or redeploy of the existing contracts. The launch-time interface surface (function signatures and events on `StablePaymentChannel` / `PaymentChannel`, `FeeRouter`, `SafetyReserve`, `StakingRegistry`, `BuybackBurner`, `VotingEscrow`, `SlashJudge`, `WatchtowerEscrow`) is treated as stable for cross-contract integration. Concretely: `openChannel` is permissionless, `SafetyReserve.payout(bundleHash, recipient, amount)` accepts arbitrary evidence-bundle hashes (per [ADR 026 §5](026-gauge-boost-tokenomics.md#5-safety-and-insurance-reserve-3-bucket)), TOKEN is `ERC20Burnable` (per [ADR 026 §1](026-gauge-boost-tokenomics.md#1-supply-and-distribution)), and no contract is locked to a specific set of integrators. Future contract surfaces deploy as additive top-level contracts, not as upgrades or migrations of the launch set.

#### Role Assignments

| Role | Contract | Authorized Functions | PoC Holder | Production Holder |
| --- | --- | --- | --- | --- |
| `DEFAULT_ADMIN_ROLE` | All contracts | Grant/revoke roles, set parameters | Deployer EOA | `TimelockController` (2-day delay) |
| `BLACKLIST_ROLE` | StakingRegistry | `ejectNode()` | ContentBlacklist contract | ContentBlacklist contract |
| `GOVERNANCE_ROLE` | OriginAssignment | `activateAssignment()`, `revokeAssignment()`, `setMinRedundancy()`, `setMaxOriginsPerNamespace()`, `setAssignmentTimelock()`, `setDefaultOpenAllowlist()`, `addDefaultOpenOperator()`, `removeDefaultOpenOperator()`, `setDefaultOpenMinRedundancy()`, `setDefaultOpenMaxOrigins()` | Admin | Governor via timelock |
| `SLASH_ROLE` | StakingRegistry | `slash()` | SlashJudge contract | SlashJudge contract |
| `SETTLEMENT_REPORTER_ROLE` | StakingRegistry | `recordSettlement(operator)` | StablePaymentChannel | FeeRouter; see §3 |
| `KEEPER_ROLE` | BuybackBurner, FeeRouter | `executeBuyback()` (BB), `executeDelegatorSwap(epoch, minOut)` (FeeRouter) | Admin / disabled | Keeper bot or governance |
| `ROUTER_CALLER_ROLE` | FeeRouter | `routeSettlement(op, bytes, amount, root)` | n/a | PaymentChannel (and any future settlement-emitting contract) |
| `PAYOUT_AUTHORIZER_ROLE` | SafetyReserve | `payout(bundle, recipient, amount)` | n/a | Governor via timelock; emergency multisig within hard caps ([ADR 026](026-gauge-boost-tokenomics.md) §5) |
| `GOVERNANCE_ROLE` | ContentBlacklist, FeeRouter (share parameters / `boostFloor`) | `addHash()`, `removeHash()`, `addOrigin()`, `removeOrigin()`, `registerRegionalBody()` (ContentBlacklist); `setShares(...)`, `setBoostFloor(...)` (FeeRouter) | Admin | Governor via timelock |
| `EMERGENCY_ROLE` | ContentBlacklist (emergency functions), fund-holding contracts (`pause()`), SafetyReserve (fast-track payout under hard caps) | `emergencyAdd()`, `emergencyAddOrigin()`, `suspendRegionalBody()` (ContentBlacklist); `pause()` (Pausable contracts only); `payout(...)` under hard caps (SafetyReserve) | Admin | 3-of-5 multisig (12-month sunset) |
| Regional body | ContentBlacklist | `addHashRegional(region)` | Not used in PoC | Per-jurisdiction multisig |

#### Governance-Controlled Parameters

Full parameter table with safety bounds is in [ADR 009](009-governance.md#governable-parameters-with-safety-bounds). Key bounds:

| Parameter | Min | Max | Contract |
| --- | --- | --- | --- |
| Protocol fee (PoC) | 0 bps | 2000 bps (20%) | StablePaymentChannel |
| Slash % per offense | 5% | 50% | StakingRegistry |
| Dispute window | 12h | 72h | StablePaymentChannel / PaymentChannel |
| Minimum stake (PoC) | 100 TOKEN | 100,000 TOKEN | StakingRegistry |
| **Minimum stake (production)** | **50,000 TOKEN (default)** | **per [ADR 026](026-gauge-boost-tokenomics.md) §7** | **StakingRegistry — discount-threshold logic removed** |
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

#### StablePaymentChannel / PaymentChannel

| Function | External Calls | Guards |
| --- | --- | --- |
| `openChannel()` | `IERC20.safeTransferFrom()`, `StakingRegistry.getStakeMultiple()` (read) | `nonReentrant`, checks-effects-interactions |
| `topUp()` | `IERC20.safeTransferFrom()` | `nonReentrant`, checks-effects-interactions |
| `settleChannel()` (PoC) | `IERC20.safeTransfer()` × 3 (provider, treasury, client) | `nonReentrant`, checks-effects-interactions |
| `settleChannel()` (production) | `IERC20.safeTransfer()` (unused balance to client), `FeeRouter.routeSettlement(operator, bytesDelivered, amount, receiptBatchRoot)` (full operator balance forwarded; FeeRouter performs the six-way split internally) | `nonReentrant`, checks-effects-interactions; FeeRouter is `nonReentrant`-guarded on `routeSettlement` to defend against re-entry through the operator-base `safeTransfer` |
| `reclaimExpired()` | `IERC20.safeTransfer()` | `nonReentrant`, checks-effects-interactions |
| `forceCloseChannel()` | None (state change only) | N/A |

#### StakingRegistry

| Function | External Calls | Guards |
| --- | --- | --- |
| `stake()` | `IERC20.safeTransferFrom()` (TOKEN) | `nonReentrant`, checks-effects-interactions |
| `unstake()` | `IERC20.safeTransfer()` (TOKEN) | `nonReentrant`, checks-effects-interactions |
| `clientStake()` | `IERC20.safeTransferFrom()` (TOKEN) | `nonReentrant`, checks-effects-interactions |
| `clientUnstake()` | `IERC20.safeTransfer()` (TOKEN) | `nonReentrant`, checks-effects-interactions |
| `slash()` (PoC) | `IERC20.safeTransfer()` (TOKEN, 50% to `msg.sender` i.e. SlashJudge), burn (50%) | `nonReentrant`, checks-effects-interactions, `SLASH_ROLE` |
| `slash()` | `IERC20.safeTransfer()` (TOKEN: 50% challenger / 30% SafetyReserve / 20% burn per [ADR 026](026-gauge-boost-tokenomics.md) §8) | `nonReentrant`, checks-effects-interactions, `SLASH_ROLE` |
| `recordSettlement(operator)` | None (single SSTORE) | `SETTLEMENT_REPORTER_ROLE` |
| `ejectNode()` | None (state change only) | `BLACKLIST_ROLE` |
| `getStakeMultiple()` | None (read-only) | N/A |

#### SlashJudge

| Function | External Calls | Guards |
| --- | --- | --- |
| `submitPhantomChallenge()` | `IERC20.safeTransferFrom()` (TOKEN bond deposit) | `nonReentrant`, checks-effects-interactions |
| `submitRateChallenge()` | `IERC20.safeTransferFrom()` (TOKEN bond deposit) | `nonReentrant`, checks-effects-interactions |
| `submitBlacklistChallenge()` | `IERC20.safeTransferFrom()` (TOKEN bond deposit), `ContentBlacklist.getEntry()` (read) | `nonReentrant`, checks-effects-interactions |
| `submitCorruptionChallenge()` | `IERC20.safeTransferFrom()` (TOKEN bond deposit) | `nonReentrant`, checks-effects-interactions |
| `counterChallenge()` | `IERC20.safeTransfer()` (bond forfeit: 50% burn, 50% to node) | `nonReentrant`, checks-effects-interactions |
| `resolveChallenge()` | `StakingRegistry.slash()`, `IERC20.safeTransfer()` (slash reward + bond return to challenger) | `nonReentrant`, checks-effects-interactions |

#### BuybackBurner

| Function | External Calls | Guards |
| --- | --- | --- |
| `executeBuyback()` | `BalancerV3Router.swapSingleTokenExactIn()` (swaps contract-held USDC; Router forwards to Vault which pulls input tokens via Vault-scoped allowance), `IERC20.safeTransfer()` (TOKEN to burn) | `nonReentrant`, checks-effects-interactions, `KEEPER_ROLE` |

> **MEV protection (production).** See [ADR 018 — Buyback execution via Balancer V3](018-liquidity-strategy.md#buyback-execution-via-balancer-v3) for the authoritative policy. In summary: Balancer's weighted-pool curve reduces (but does not eliminate) price-impact concerns compared to concentrated liquidity, and `executeBuyback` MAY split large buybacks into `subSwapCount` sub-swaps spaced by `subSwapMinBlockGap` blocks. **Direct Router execution with TWAP + `minTokenOut` guards is the primary production path and the required fallback.** Routing through CoW Swap is a conditional add-on that requires operator verification of CoW solver routing against the deployed Balancer V3 pool (per ADR 018's activation criteria); if CoW routing is unavailable or regresses, direct Router + TWAP remains correct. The `maxBuybackAmount` parameter MUST be enforced to limit per-transaction MEV exposure regardless of venue.

> **Production inflow source.** Under [ADR 026](026-gauge-boost-tokenomics.md) §8, `BuybackBurner` receives 5% of every settlement same-tx from `FeeRouter`. The `executeBuyback` mechanics, `KEEPER_ROLE`-gating, and Vault-scoped self-approval pattern are unchanged.

#### FeeRouter (production)

| Function | External Calls | Guards |
| --- | --- | --- |
| `routeSettlement(operator, bytesDelivered, amount, receiptBatchRoot)` | `IERC20.safeTransfer()` × 4 (operator base 40%, BuybackBurner 5%, Treasury 5%, SafetyReserve 3%; gauge 40% and delegator 7% retained in epoch buckets, no transfer), `StakingRegistry.recordSettlement(operator)`. State updates: appends one leaf to the per-(operator, epoch) MMR receipt accumulator (per ADR 027 §4), and increments `totalBytesPerEpoch[epoch]` and `totalWorkingBytesPerEpoch[epoch]` global counters that `claimBoost` later reads as the gauge-share denominator (avoids gas-prohibitive iteration at claim time). `receiptBatchRoot` is the gauge-eligibility commitment per [ADR 027 §4](027-distinct-client-receipts.md#4-on-chain-anchoring-merkle-batched); zero root signals operator opt-out of gauge credit for this settlement. | `nonReentrant`, checks-effects-interactions, `ROUTER_CALLER_ROLE` |
| `claimBoost(epochs[])` | `IERC20.safeTransfer()` (USDC to claiming operator), `VotingEscrow.balanceOfAt(...)` × N epochs (read), `VotingEscrow.totalSupplyAt(...)` × N epochs (read) | `nonReentrant`, checks-effects-interactions; epoch must be finalized |
| `claimDelegator(epochs[])` | `IERC20.safeTransfer()` (TOKEN to claiming ve-locker), `VotingEscrow.balanceOfAt(...)` × N (read), `VotingEscrow.totalSupplyAt(...)` × N (read) | `nonReentrant`, checks-effects-interactions; epoch's delegator-pool USDC→TOKEN swap must be settled |
| `executeDelegatorSwap(epoch, minOut)` | `BalancerV3Router.swapSingleTokenExactIn()` (Vault-scoped self-approval, same V3 footgun pattern as `BuybackBurner`) | `nonReentrant`, checks-effects-interactions, `KEEPER_ROLE`; per-epoch liquidity caps and TWAP-window guards REQUIRED ([ADR 026](026-gauge-boost-tokenomics.md) §6) |
| `setShares(...)`, `setBoostFloor(...)` | None (state change only) | `GOVERNANCE_ROLE` (Governor via timelock); sum-to-100% across the six router shares enforced; per-share bounds enforced ([ADR 026](026-gauge-boost-tokenomics.md) §11) |
| `sweepUnclaimed(epoch)` | `IERC20.safeTransfer()` (USDC/TOKEN to treasury) | `nonReentrant`, checks-effects-interactions; only callable after the 26-epoch claim window expires |

> **Cashflow invariant.** The 20% lower bound on the node-base share is `immutable` and guarantees operators always receive enough liquid USDC to cover at least a meaningful fraction of infrastructure costs even under extreme governance proposals. See [ADR 026](026-gauge-boost-tokenomics.md) §11.

#### VotingEscrow (production)

| Function | External Calls | Guards |
| --- | --- | --- |
| `createLock(amount, duration)` | `IERC20.safeTransferFrom()` (TOKEN) | `nonReentrant`, checks-effects-interactions; `duration ∈ [1 week, 4 years]` (`immutable` bounds) |
| `extendLock(duration)` | None (state change only) | `nonReentrant`; new expiry capped at `now + 4 years`; lock shortening NOT allowed |
| `withdraw()` | `IERC20.safeTransfer()` (TOKEN) | `nonReentrant`, checks-effects-interactions; only callable after lock expiry; **no early-exit penalty path** |
| `balanceOfAt(user, ts)`, `totalSupplyAt(ts)` | None (read-only; per-lock checkpoint binary search) | N/A |

> **No `create_lock_for` privileged path.** Auto-ve-lock-on-vest is removed ([ADR 026](026-gauge-boost-tokenomics.md) §1, §4). All ve-positions are voluntarily created by the locker. **No slashing path on ve-locked TOKEN** — the slashing-immunity invariant is enforced by the absence of any `slash` / `burn` / `sweep` entry point on `VotingEscrow`. Operator stake (slashable) lives in `StakingRegistry`; ve-positions (non-slashable) live here. An operator may hold any combination but the contracts never share state.

#### SafetyReserve (production)

| Function | External Calls | Guards |
| --- | --- | --- |
| `payout(bundle, recipient, amount)` | `IERC20.safeTransfer()` (USDC to recipient) | `nonReentrant`, checks-effects-interactions, `PAYOUT_AUTHORIZER_ROLE` (Governor via timelock; emergency multisig under hard caps); attested incident bundle REQUIRED; 48h appeal window REQUIRED before disbursement; post-incident registry write atomic with disbursement ([ADR 026](026-gauge-boost-tokenomics.md) §5) |
| `recordIncident(bundle)` | None (event + storage write) | Public; bundle signature verified against attestor allowlist |
| `challengeIncident(id, evidence)` | None (storage write) | Public during 48h appeal window; valid challenge pauses disbursement pending Governor resolution |

> **Spending control invariant.** No path on `SafetyReserve` exists for unattested or non-Governor-authorized payouts. The four-gate check (evidence bundle + Governor or emergency-multisig within hard caps + 48h appeal + post-incident registry write) is enforced atomically inside `payout`; partial paths revert.

#### WatchtowerEscrow

| Function | External Calls | Guards |
| --- | --- | --- |
| `depositFee()` | `IERC20.safeTransferFrom()` (USDC fee deposit) | `nonReentrant`, checks-effects-interactions |
| `submitHeartbeat()` | None (state change only) | N/A |
| `claimFees()` | `IERC20.safeTransfer()` (USDC to watchtower) | `nonReentrant`, checks-effects-interactions |
| `reclaimOnLivenessFailure()` | `IERC20.safeTransfer()` (USDC to watched party) | `nonReentrant`, checks-effects-interactions |
| Channel state reads | `StablePaymentChannel`/`PaymentChannel.getChannel()` (read-only) | N/A |

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
| `activateAssignment(...)` | None (state change only) | `GOVERNANCE_ROLE`; pending proposal must exist; min-redundancy invariant enforced post-activation |
| `revokeAssignment(namespaceId, operator)` | None (state change only) | Either `GOVERNANCE_ROLE` or namespace owner; revocation that would drop the active set below `minRedundancy` is allowed (publishers may shrink their assignment set; the constraint is on activation, not on revocation) |
| `pruneBlacklistedAssignment(namespaceId, operator)` | `ContentBlacklist.isOriginBlacklisted(operator)` (read) | Permissionless; reverts if operator is not currently blacklisted in `ContentBlacklist`; works for `namespaceId == 0` as well |
| `setMinRedundancy(uint256)`, `setMaxOriginsPerNamespace(uint256)`, `setAssignmentTimelock(uint256)` | None (state change only) | `GOVERNANCE_ROLE`; safety bounds enforced ([ADR 009](009-governance.md)); cross-parameter invariant `1 ≤ minRedundancy ≤ maxOriginsPerNamespace` enforced at the contract layer on every `setMinRedundancy` / `setMaxOriginsPerNamespace` call (revert on violation); all three apply to non-zero namespaces only |
| `setDefaultOpenAllowlist(operators[])`, `addDefaultOpenOperator(operator)`, `removeDefaultOpenOperator(operator)` | `StakingRegistry.isActive(operator)` per operator (read) | `GOVERNANCE_ROLE`; first non-empty activation flips `defaultOpenAllowlistActive` permanently and emits `DefaultOpenAllowlistActivated`; resulting set size must be within `[defaultOpenMinRedundancy, defaultOpenMaxOrigins]`; duplicates revert; runs under the Governor's standard 48h timelock |
| `setDefaultOpenMinRedundancy(uint256)`, `setDefaultOpenMaxOrigins(uint256)` | None (state change only) | `GOVERNANCE_ROLE`; safety bounds enforced ([ADR 009](009-governance.md)); cross-parameter invariants `5 ≤ defaultOpenMinRedundancy ≤ defaultOpenMaxOrigins ≤ 500` and `defaultOpenMinRedundancy ≥ minRedundancy` enforced at the contract layer |
| `isAuthorizedOrigin()`, `getOrigins()`, `getPendingAssignment()`, `defaultOpenAllowlistActive()`, `defaultOpenActivatedAt()` | None (read-only) | N/A |

The contract holds no funds. It maintains an `EnumerableSet` of currently-authorized operators per namespace, plus the bootstrap state for the default-open allow-list (`bool defaultOpenAllowlistActive`, `uint64 defaultOpenActivatedAt`). The first non-empty default-open activation flips `defaultOpenAllowlistActive` to `true` permanently, sets `defaultOpenActivatedAt` to that block's timestamp, and emits `DefaultOpenAllowlistActivated`. Until that moment, `isAuthorizedOrigin(0, op)` returns `true` for any active staker (permissive bootstrap). Off-chain consumers of `getOrigins(namespaceId)` cross-reference each returned operator against `ContentBlacklist.isOriginBlacklisted` and treat blacklisted entries as unauthorized regardless of stale `OriginAssignment` state, so storage cleanup via `pruneBlacklistedAssignment` is a lazy optimisation rather than a security primitive.

#### Multi-Token Reentrancy Considerations

Production `PaymentChannel` accepts arbitrary governance-approved ERC-20s ([ADR 010](010-multi-token.md)). Even with `SafeERC20` and `nonReentrant`, governance must vet tokens before allowlisting:

- **Reject:** Fee-on-transfer tokens, rebase tokens, pausable tokens, tokens with transfer hooks (ERC-777) that could re-enter
- **Accept:** Standard IERC20 tokens with no callback mechanisms
- **Mitigations in contract code:** `nonReentrant` blocks all re-entry regardless of token behavior; `SafeERC20` handles non-reverting transfers and missing return values

All ERC-20 interactions use OpenZeppelin `SafeERC20` to handle non-standard token implementations ([ADR 003](003-payments.md), [ADR 010](010-multi-token.md)).

### 7. OpenZeppelin Framework Usage

Every deCDN contract should inherit from audited OpenZeppelin base contracts rather than implementing security primitives from scratch.

| OZ Contract | Used By | Purpose |
| --- | --- | --- |
| `Ownable` | StablePaymentChannel (PoC) | Admin-key governance for PoC-only contract |
| `AccessControl` | StakingRegistry, PaymentChannel, ContentBlacklist, PublisherRegistry, OriginAssignment, SlashJudge, BuybackBurner, FeeRouter, SafetyReserve | Role-based function authorization |
| `ReentrancyGuard` | All fund-holding contracts | `nonReentrant` modifier on state-mutating functions with external calls |
| `Pausable` | All fund-holding contracts | Emergency pause capability |
| `SafeERC20` | All contracts interacting with ERC-20 tokens | Safe wrappers for `transfer`, `transferFrom`, `approve` |
| `EIP712` | StablePaymentChannel, PaymentChannel, SlashJudge, WatchtowerEscrow, SafetyReserve (production, for attested incident bundles) | Domain separator for voucher/slash/heartbeat/incident-bundle signature verification |
| `SignatureChecker` | StablePaymentChannel, PaymentChannel, StakingRegistry, SlashJudge, SafetyReserve | Unified EOA + ERC-1271 smart account signature verification ([ADR 024](024-account-abstraction.md)) |
| `ERC20` + `ERC20Permit` | TOKEN | Standard fungible token with gasless approvals |
| `Governor` | Production governance | Token-weighted voting (production: voting weight sourced from `VotingEscrow.balanceOfAt` rather than `TOKEN.getPastVotes`) |
| `GovernorVotes` | Production governance | **PoC:** TOKEN as voting token. **Production:** `VotingEscrow`-backed vote source per [ADR 026](026-gauge-boost-tokenomics.md) §9 |
| `GovernorTimelockControl` | Production governance | 2-day timelock on parameter changes |
| `TimelockController` | Production governance | Queued execution of governance proposals; custodian of the protocol-treasury 5% bucket |

**Rationale:** OpenZeppelin Contracts are the most widely audited Solidity library, used by the majority of production DeFi protocols. Using audited primitives for access control, reentrancy protection, token handling, and governance eliminates entire classes of implementation bugs and reduces the surface area that a security audit must cover to deCDN-specific business logic.

### 8. PoC vs Production Contract Topology

| Aspect | PoC | Production |
| --- | --- | --- |
| Payment contract | `StablePaymentChannel` (USDC only) | `PaymentChannel` (multi-token allowlist), routes full operator balance to `FeeRouter` |
| Settlement-fee mechanic | Skim at settlement contract (3% / 1.5% discounted) | `FeeRouter` six-bucket split (40/40/7/5/5/3) per [ADR 026](026-gauge-boost-tokenomics.md) §2 |
| FeeRouter | Not deployed | Deployed; epoch buckets, claim-based gauge / delegator pools |
| VotingEscrow | Not deployed | Deployed; voluntary ve-locking, no `create_lock_for` |
| SafetyReserve | Not deployed | Deployed; receives 3% router bucket (USDC) + 30% slashing redirect (TOKEN, swapped via keeper to USDC); payouts gated per [ADR 026](026-gauge-boost-tokenomics.md) §5 |
| Governance | Admin key (single EOA) | OpenZeppelin Governor + 2-day timelock; voting weight = `VotingEscrow.balanceOfAt` |
| Emergency multisig | Admin key | 3-of-5 multisig with 12-month sunset (also fast-track SafetyReserve payouts under hard caps) |
| BuybackBurner | Accumulate-only (execution disabled) | Active (keeper or governance triggered); inflow from `FeeRouter` (5% same-tx) rather than manual treasury transfer |
| WatchtowerEscrow | Not deployed | Deployed |
| TOKEN minting | `onlyOwner` mint for testnet flexibility | No mint function; fixed 1B supply |
| Minimum stake | 1,000 TOKEN | 50,000 TOKEN ([ADR 026](026-gauge-boost-tokenomics.md) §7); discount-threshold logic removed |
| Slashing distribution | 50% challenger / 50% burn | 50% challenger / 30% SafetyReserve / 20% burn ([ADR 026](026-gauge-boost-tokenomics.md) §8) |
| Regional bodies | Not used | Jurisdiction-scoped multisigs |
| PublisherRegistry | Deployed; admin-key escape hatch active | Deployed; admin-key escape hatch removed; namespace cap and transfer timelock under governance |
| OriginAssignment | Deployed; admin key activates assignments directly; default-open allow-list inactive (`defaultOpenAllowlistActive == false`, permissive bootstrap window) | Deployed; activations gated by Governor + 24h–14d timelock for registered namespaces; default-open allow-list activated by governance under the standard 48h timelock, flipping the gate to strict |
| Contract migration | N/A | New `PaymentChannel` + production contracts deployed; PoC `StablePaymentChannel` decommissioned |

**Migration path:** Production deploys a new `PaymentChannel` contract (not an upgrade of `StablePaymentChannel`). Per [ADR 010](010-multi-token.md), no phased migration is required because the PoC `StablePaymentChannel` has no real users or funds in production; the PoC contract is decommissioned rather than operated in a close-only mode alongside `PaymentChannel`.

## Consequences

**Positive:**

- Single reference document for all contract interactions, reducing audit scope ambiguity
- Explicit deployment order prevents initialization-order bugs
- Access control matrix makes privilege escalation paths visible and auditable
- OZ base contract prescriptions eliminate classes of implementation bugs before code is written

**Negative:**

- Must be kept in sync as other ADRs evolve — any change to contract interfaces in ADRs 003, 007, 009, 010, 011, 014, or 026 requires updating this document
- Does not cover off-chain interaction patterns (voucher exchange, gossip, probing) — those remain in their respective ADRs
- The contract surface includes three fund-holding contracts (`FeeRouter`, `VotingEscrow`, `SafetyReserve`) plus optional `DelegatorBuyer`, materially expanding audit scope

## References

- [ADR 003 — Payment Model](003-payments.md): StablePaymentChannel specification, production `PaymentChannel.settleChannel` → `FeeRouter` routing
- [ADR 007 — Watchtower Design](007-watchtower.md): WatchtowerEscrow
- [ADR 009 — Governance Model](009-governance.md): Safety bounds, Governor, emergency multisig
- [ADR 010 — Multi-Token Payment Support](010-multi-token.md): PaymentChannel, token allowlist
- [ADR 002 — Content Addressing](002-content-addressing.md): PublisherRegistry, namespaces, content claims
- [ADR 011 — Content Takedown](011-content-takedown.md): ContentBlacklist, origin ejection, OriginAssignment, DAO origin authority
- [ADR 014 — On-Chain Verification](014-on-chain-verification.md): SlashJudge, challenge bonds
- [ADR 018 — Liquidity Strategy](018-liquidity-strategy.md): Balancer V3 80/20 pool, MEV protection, POL custody, BuybackBurner execution
- [ADR 026 — Gauge-Boost Tokenomics](026-gauge-boost-tokenomics.md): FeeRouter six-bucket split, VotingEscrow, SafetyReserve, gauge-boost formula, the slashing distribution
- [OpenZeppelin Contracts](https://docs.openzeppelin.com/contracts/): Base contract framework
