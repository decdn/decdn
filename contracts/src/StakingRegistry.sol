// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

import { ISafetyReserve } from "./interfaces/ISafetyReserve.sol";

/// @title StakingRegistry — staking, slashing, and settlement-recording core
/// @notice Custodies operator TOKEN stake, executes slashing with the
///         50% challenger / 30% SafetyReserve / 20% burn split from
///         ADR 026 § Slashing and burn, and is the canonical inflow point
///         for `FeeRouter` settlement reporting.
///
///         This PR ships the stake-and-slash core. Node-registry surface
///         (`registerNode`, `bindNodeId`, ed25519 verification, multiaddrs,
///         pagination views) lands in a follow-up — see ADR 003 § Node
///         Registry for the surface that will be added there. The two
///         halves are split because the node-identity work has its own
///         audit boundary (ed25519 verifier, EIP-712 binding signatures)
///         that's orthogonal to stake mechanics.
///
/// @dev    Composition:
///           - `AccessControl`: 4 role grants (GOVERNANCE / SLASH /
///             BLACKLIST / SETTLEMENT_REPORTER) plus `DEFAULT_ADMIN_ROLE`
///             held by the Timelock per ADR 016 § Deployment Order.
///           - `ReentrancyGuard`: covers every state-mutating function
///             that calls into TOKEN or `SafetyReserve`.
///           - `Pausable`: emergency stop on stake / unstake / slash.
///
///         Slash math (ADR 026 § Slashing and burn):
///           - Lifetime offense counter (`uint32`, monotonically
///             increasing) determines the percentage tier:
///             1st → 5%, 2nd → 15%, 3rd+ → 50%.
///           - Slash applies to the operator's full at-risk stake
///             (active + unbonding). Active stake is reduced first;
///             the unbonding bucket absorbs any remainder.
///           - Distribution: 50% to challenger, 30% to SafetyReserve
///             (via `safeTransfer` + `recordSlashInflow`), 20% burned
///             via `ERC20Burnable.burn` on the TOKEN contract.
///           - Auto-ejection: if post-slash active stake falls below
///             `minStake / 2`, the operator's `ejected` flag is set;
///             they must re-stake to at least `minStake` to clear it
///             (ADR 003 § Node Registry — "must re-stake at full
///             minimum to rejoin").
///
///         ADR drift note: ADR 014 line 237 specifies `slash(address node,
///         uint8 offenseType)` — 2 args — but the 50% challenger share
///         requires StakingRegistry to know the challenger address. This
///         implementation adds the challenger as a third arg
///         (`slash(address operator, address challenger, uint8 offenseType)`).
///         A follow-up ADR amendment will reconcile the spec.
contract StakingRegistry is AccessControl, ReentrancyGuard, Pausable {
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    /// @notice Setter authority for `minStake`, `unbondingPeriod`, and
    ///         `safetyReserve`. Held by `TimelockController` post-deploy.
    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");

    /// @notice Authority to call `slash`. Granted to `SlashJudge` post-deploy
    ///         (ADR 016 § Post-Deployment Initialization, step 3).
    bytes32 public constant SLASH_ROLE = keccak256("SLASH_ROLE");

    /// @notice Authority to call `ejectNode`. Granted to `ContentBlacklist`
    ///         post-deploy (ADR 016 § Post-Deployment Initialization, step 1).
    bytes32 public constant BLACKLIST_ROLE = keccak256("BLACKLIST_ROLE");

    /// @notice Authority to call `recordSettlement`. Granted to `FeeRouter`
    ///         post-deploy (ADR 016 § Post-Deployment Initialization, step 5).
    bytes32 public constant SETTLEMENT_REPORTER_ROLE = keccak256("SETTLEMENT_REPORTER_ROLE");

    /// @notice Authority to pause / unpause. Held by the emergency multisig.
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    // -----------------------------------------------------------------
    // Slash schedule constants (ADR 026 § Slashing and burn)
    // -----------------------------------------------------------------

    uint256 internal constant BPS_DENOMINATOR = 10_000;

    /// @notice Percentage (in basis points) slashed on each lifetime offense.
    ///         5% / 15% / 50% for offenses 1 / 2 / 3+ per ADR 026.
    uint256 internal constant SLASH_BPS_TIER_1 = 500;
    uint256 internal constant SLASH_BPS_TIER_2 = 1500;
    uint256 internal constant SLASH_BPS_TIER_3 = 5000;

    /// @notice Split applied to every slash amount (sum to BPS_DENOMINATOR).
    uint256 internal constant CHALLENGER_BPS = 5000;
    uint256 internal constant SAFETY_BPS = 3000;
    // The 20% burn share is the implicit remainder; computed as
    // `slashAmount - challengerShare - safetyShare` so the three legs
    // sum to exactly slashAmount with no rounding leak.

    // -----------------------------------------------------------------
    // Governable-parameter safety bounds (ADR 009 § Governable parameters)
    // -----------------------------------------------------------------

    uint256 internal constant MIN_STAKE_FLOOR = 10_000e18;
    uint256 internal constant MIN_STAKE_CEILING = 1_000_000e18;

    uint256 internal constant UNBONDING_PERIOD_FLOOR = 3 days;
    uint256 internal constant UNBONDING_PERIOD_CEILING = 30 days;

    // -----------------------------------------------------------------
    // Immutable wiring
    // -----------------------------------------------------------------

    /// @notice TOKEN contract. Typed as OZ's `ERC20Burnable` directly — Token
    ///         inherits from it, so callers pass `Token` without a cast and
    ///         the 20%-burn leg of `slash` calls `token.burn(amount)` against
    ///         the canonical OZ surface (no custom interface).
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ERC20Burnable public immutable token;

    // -----------------------------------------------------------------
    // Storage
    // -----------------------------------------------------------------

    /// @notice Per-operator unbonding request. At most one in-flight.
    /// @dev    Both fields are `uint256` for consistency with the contract's
    ///         other timestamp + balance storage (`lastSettlementAt`,
    ///         `activeStake`). `unlockAt`'s `uint256` typing is the binding
    ///         constraint — the struct cannot pack into one slot regardless
    ///         of `amount`'s type — so no narrowing-cast trade-off is left
    ///         on the table.
    struct UnbondingRequest {
        uint256 amount;
        uint256 unlockAt;
    }

    /// @notice Active stake per operator. Excludes amounts in unbonding —
    ///         those live in `unbondingOf` but remain slashable (ADR 003
    ///         "Stake remains slashable during unbonding").
    mapping(address operator => uint256 amount) public activeStake;

    /// @notice In-flight unbonding per operator. At most one outstanding;
    ///         `requestUnstake` reverts if a request already exists.
    mapping(address operator => UnbondingRequest) public unbondingOf;

    /// @notice Monotonically-increasing lifetime offense count per operator.
    ///         Determines slash tier per ADR 026 § Slashing and burn.
    mapping(address operator => uint32) public lifetimeOffenseCount;

    /// @notice `block.timestamp` of the last `recordSettlement` call for
    ///         this operator. Read by gauge / claim machinery in
    ///         `FeeRouter` (ADR 016 line 463).
    mapping(address operator => uint256) public lastSettlementAt;

    /// @notice Auto-ejection flag. Set when post-slash active stake falls
    ///         below `minStake / 2`, or when `ejectNode` is called by
    ///         `ContentBlacklist`. Cleared when the operator re-stakes to
    ///         `>= minStake` (ADR 003 § Node Registry).
    mapping(address operator => bool) public ejected;

    /// @notice Minimum stake to be considered active (ADR 026 § Operator
    ///         economics and minimum stake — 50_000 TOKEN at launch).
    uint256 public minStake;

    /// @notice Unbonding period (ADR 003 — default 7 days, bounded by ADR 009).
    uint256 public unbondingPeriod;

    /// @notice SafetyReserve address for the 30% slash redirect. May be
    ///         `address(0)` until SafetyReserve is deployed and wired
    ///         (ADR 016 § Deployment Order — SafetyReserve is step 6,
    ///         StakingRegistry is step 4). `slash` reverts while the
    ///         address is zero, so SafetyReserve MUST be wired before any
    ///         slash path can execute.
    ISafetyReserve public safetyReserve;

    // -----------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------

    event Staked(address indexed operator, uint256 amount, uint256 newActiveStake);
    event UnbondingRequested(address indexed operator, uint256 amount, uint256 unlockAt, uint256 newActiveStake);
    event Unstaked(address indexed operator, uint256 amount);
    event Slashed(
        address indexed operator,
        address indexed challenger,
        uint8 offenseType,
        uint32 lifetimeOffenseCount,
        uint256 slashAmount,
        uint256 challengerShare,
        uint256 safetyShare,
        uint256 burnShare
    );
    event AutoEjected(address indexed operator, uint256 remainingStake);
    event EjectedByBlacklist(address indexed operator);
    event Reinstated(address indexed operator);
    /// @dev `block.timestamp` is implicit on every log via the block header;
    ///      a redundant explicit timestamp would only inflate the log-data
    ///      gas paid by the caller for a value indexers can already read
    ///      from the block.
    event SettlementRecorded(address indexed operator);
    event MinStakeUpdated(uint256 oldValue, uint256 newValue);
    event UnbondingPeriodUpdated(uint256 oldValue, uint256 newValue);
    event SafetyReserveUpdated(address indexed oldAddr, address indexed newAddr);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error ZeroAmount();
    error InsufficientStake(uint256 requested, uint256 available);
    error UnbondingInProgress();
    error UnbondingNotComplete(uint256 unlockAt);
    error NoUnbondingRequest();
    error SafetyReserveNotWired();
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param token_           TOKEN contract address (must implement `burn`).
    /// @param admin            Initial holder of `DEFAULT_ADMIN_ROLE`; per
    ///                         ADR 016 § Deployment Order this is the
    ///                         `TimelockController` (or a deployer multisig
    ///                         on testnet that hands off to Timelock).
    /// @param minStake_        Initial minimum active stake (ADR 026 default
    ///                         50_000 TOKEN; bounded `[10k, 1M]`).
    /// @param unbondingPeriod_ Initial unbonding period (ADR 003 default
    ///                         7 days; bounded `[3 days, 30 days]` per
    ///                         ADR 009).
    constructor(ERC20Burnable token_, address admin, uint256 minStake_, uint256 unbondingPeriod_) {
        if (address(token_) == address(0) || admin == address(0)) revert ZeroAddress();
        _enforceMinStakeBounds(minStake_);
        _enforceUnbondingPeriodBounds(unbondingPeriod_);

        token = token_;
        minStake = minStake_;
        unbondingPeriod = unbondingPeriod_;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
    }

    // -----------------------------------------------------------------
    // Staking
    // -----------------------------------------------------------------

    /// @notice Lock `amount` TOKEN as active stake for `msg.sender`.
    /// @dev    The caller must `approve` this contract for at least `amount`
    ///         beforehand (or use `Token.permit` to set the allowance in the
    ///         same user op — see ADR 024).
    ///         If the operator was auto-ejected and the post-stake balance
    ///         meets `minStake`, the ejection flag is cleared (ADR 003 —
    ///         "must re-stake at full minimum to rejoin").
    function stake(uint256 amount) external nonReentrant whenNotPaused {
        if (amount == 0) revert ZeroAmount();
        IERC20(address(token)).safeTransferFrom(msg.sender, address(this), amount);
        uint256 newBalance = activeStake[msg.sender] + amount;
        activeStake[msg.sender] = newBalance;

        if (ejected[msg.sender] && newBalance >= minStake) {
            ejected[msg.sender] = false;
            emit Reinstated(msg.sender);
        }

        emit Staked(msg.sender, amount, newBalance);
    }

    /// @notice Move `amount` of `msg.sender`'s active stake into unbonding.
    ///         The amount remains slashable for `unbondingPeriod` seconds,
    ///         after which `unstake()` withdraws it.
    /// @dev    Only one outstanding request per operator at a time; calling
    ///         again before `unstake()` reverts with `UnbondingInProgress`.
    function requestUnstake(uint256 amount) external nonReentrant whenNotPaused {
        if (amount == 0) revert ZeroAmount();
        if (amount > activeStake[msg.sender]) {
            revert InsufficientStake({ requested: amount, available: activeStake[msg.sender] });
        }
        if (unbondingOf[msg.sender].amount != 0) revert UnbondingInProgress();

        activeStake[msg.sender] -= amount;
        uint256 unlockAt = block.timestamp + unbondingPeriod;
        unbondingOf[msg.sender] = UnbondingRequest({ amount: amount, unlockAt: unlockAt });

        emit UnbondingRequested(msg.sender, amount, unlockAt, activeStake[msg.sender]);
    }

    /// @notice Withdraw the operator's matured unbonding request to their wallet.
    /// @dev    Reverts if no request is outstanding or the unbonding period
    ///         hasn't elapsed.
    function unstake() external nonReentrant whenNotPaused {
        UnbondingRequest memory req = unbondingOf[msg.sender];
        if (req.amount == 0) revert NoUnbondingRequest();
        if (block.timestamp < req.unlockAt) revert UnbondingNotComplete(req.unlockAt);

        delete unbondingOf[msg.sender];
        IERC20(address(token)).safeTransfer(msg.sender, req.amount);
        emit Unstaked(msg.sender, req.amount);
    }

    // -----------------------------------------------------------------
    // Slashing (SLASH_ROLE — held by SlashJudge)
    // -----------------------------------------------------------------

    /// @notice Slash `operator`, distributing the 50% / 30% / 20% split to
    ///         `challenger` / `SafetyReserve` / burn.
    /// @dev    Active stake is reduced first; the unbonding bucket absorbs
    ///         any remainder so stake-during-unbonding remains slashable
    ///         (ADR 003 — prevents slash-then-run).
    ///         Reverts with `SafetyReserveNotWired` if `safetyReserve` is
    ///         the zero address — SafetyReserve must be wired before any
    ///         slash path can execute (ADR 016 § Deployment Order pairs
    ///         the wiring with the SLASH_ROLE grant in step 3).
    ///         Returns the total slashed amount so `SlashJudge` can emit
    ///         the canonical `Slashed(slashId, ..., amount, ...)` event
    ///         (ADR 014 line 201) in the same transaction.
    /// @param operator    The operator being slashed.
    /// @param challenger  EOA / contract that submitted the winning challenge;
    ///                    receives the 50% reward leg.
    /// @param offenseType The `OffenseType` enum value from `SlashJudge`
    ///                    (ADR 014 § SlashJudge Contract). Stored in the
    ///                    event for off-chain indexers but does not affect
    ///                    the slash percentage — that is determined solely
    ///                    by the lifetime offense counter per ADR 026.
    /// @return slashAmount Total amount slashed (sum of the three legs).
    function slash(address operator, address challenger, uint8 offenseType)
        external
        nonReentrant
        whenNotPaused
        onlyRole(SLASH_ROLE)
        returns (uint256 slashAmount)
    {
        if (operator == address(0) || challenger == address(0)) revert ZeroAddress();
        if (address(safetyReserve) == address(0)) revert SafetyReserveNotWired();

        // Compute tier from lifetime counter (1 → 5%, 2 → 15%, 3+ → 50%).
        uint32 newCount = lifetimeOffenseCount[operator] + 1;
        lifetimeOffenseCount[operator] = newCount;
        uint256 tierBps = newCount == 1 ? SLASH_BPS_TIER_1 : newCount == 2 ? SLASH_BPS_TIER_2 : SLASH_BPS_TIER_3;

        // Slash applies to total at-risk stake (active + unbonding).
        UnbondingRequest memory req = unbondingOf[operator];
        uint256 totalAtRisk = activeStake[operator] + uint256(req.amount);
        slashAmount = (totalAtRisk * tierBps) / BPS_DENOMINATOR;

        // Reduce active stake first, then unbonding bucket.
        if (slashAmount <= activeStake[operator]) {
            activeStake[operator] -= slashAmount;
        } else {
            uint256 remainder = slashAmount - activeStake[operator];
            activeStake[operator] = 0;
            unbondingOf[operator].amount = req.amount - remainder;
        }

        // Split: 50% challenger / 30% SafetyReserve / 20% burn.
        // Burn is the remainder so the three legs sum exactly to slashAmount.
        // The divide-before-multiply pattern (slashAmount was computed by
        // a prior division) is intentional: rounding dust from the bps
        // splits is captured in burnShare via subtraction, so no value
        // is lost. The three-leg sum invariant is exercised by
        // testFuzz_slash_threeLegsSumToSlashAmount.
        // slither-disable-next-line divide-before-multiply
        uint256 challengerShare = (slashAmount * CHALLENGER_BPS) / BPS_DENOMINATOR;
        // slither-disable-next-line divide-before-multiply
        uint256 safetyShare = (slashAmount * SAFETY_BPS) / BPS_DENOMINATOR;
        uint256 burnShare = slashAmount - challengerShare - safetyShare;

        if (challengerShare != 0) {
            IERC20(address(token)).safeTransfer(challenger, challengerShare);
        }
        if (safetyShare != 0) {
            IERC20(address(token)).safeTransfer(address(safetyReserve), safetyShare);
            safetyReserve.recordSlashInflow(operator, safetyShare);
        }
        if (burnShare != 0) {
            token.burn(burnShare);
        }

        // Auto-ejection check (ADR 026 — "auto-ejection at 50% of
        // minimum stake"). Triggered on the *active* leg only; an
        // operator with stake in unbonding is already exiting.
        if (activeStake[operator] < (minStake / 2) && !ejected[operator]) {
            ejected[operator] = true;
            emit AutoEjected(operator, activeStake[operator]);
        }

        emit Slashed(operator, challenger, offenseType, newCount, slashAmount, challengerShare, safetyShare, burnShare);
    }

    // -----------------------------------------------------------------
    // Ejection by ContentBlacklist
    // -----------------------------------------------------------------

    /// @notice Force-eject an operator irrespective of stake level. Used by
    ///         `ContentBlacklist` when a blacklisted hash is served.
    /// @dev    Does not move TOKEN — the operator's stake stays put and
    ///         remains subject to the standard unbonding flow if they want
    ///         to exit. The flag prevents the operator from being treated
    ///         as active by downstream consumers; clearing it requires the
    ///         standard re-stake-to-minimum path.
    function ejectNode(address operator) external onlyRole(BLACKLIST_ROLE) {
        if (operator == address(0)) revert ZeroAddress();
        if (!ejected[operator]) {
            ejected[operator] = true;
            emit EjectedByBlacklist(operator);
        }
    }

    // -----------------------------------------------------------------
    // Settlement reporter callback (FeeRouter)
    // -----------------------------------------------------------------

    /// @notice Record that the operator just settled a payment-channel
    ///         payout through `FeeRouter`. Updates `lastSettlementAt`.
    /// @dev    Carries `SETTLEMENT_REPORTER_ROLE`, granted to `FeeRouter`
    ///         post-deploy (ADR 016 § Post-Deployment Initialization,
    ///         step 5). Read by claim / gauge machinery.
    function recordSettlement(address operator) external onlyRole(SETTLEMENT_REPORTER_ROLE) {
        if (operator == address(0)) revert ZeroAddress();
        lastSettlementAt[operator] = block.timestamp;
        emit SettlementRecorded(operator);
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function setMinStake(uint256 newMinStake) external onlyRole(GOVERNANCE_ROLE) {
        _enforceMinStakeBounds(newMinStake);
        uint256 oldMinStake = minStake;
        minStake = newMinStake;
        emit MinStakeUpdated(oldMinStake, newMinStake);
    }

    function setUnbondingPeriod(uint256 newPeriod) external onlyRole(GOVERNANCE_ROLE) {
        _enforceUnbondingPeriodBounds(newPeriod);
        uint256 oldPeriod = unbondingPeriod;
        unbondingPeriod = newPeriod;
        emit UnbondingPeriodUpdated(oldPeriod, newPeriod);
    }

    function setSafetyReserve(ISafetyReserve newSafetyReserve) external onlyRole(GOVERNANCE_ROLE) {
        address oldAddr = address(safetyReserve);
        safetyReserve = newSafetyReserve;
        emit SafetyReserveUpdated(oldAddr, address(newSafetyReserve));
    }

    // -----------------------------------------------------------------
    // Pause control
    // -----------------------------------------------------------------

    function pause() external onlyRole(PAUSER_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }

    // -----------------------------------------------------------------
    // Views
    // -----------------------------------------------------------------

    /// @notice Current active stake for `operator`. Excludes unbonding.
    /// @dev    Convenience view aliasing `activeStake[operator]` so the
    ///         public-surface name matches ADR 003's `stakeOf(address)`
    ///         consumer reference.
    function stakeOf(address operator) external view returns (uint256) {
        return activeStake[operator];
    }

    /// @notice `activeStake[operator] / minStake`, used by off-chain
    ///         node-selection / admission policy per ADR 003 line 736.
    ///         Returns 0 if the operator is below the minimum.
    function getStakeMultiple(address operator) external view returns (uint256) {
        return activeStake[operator] / minStake;
    }

    /// @notice Provisional active predicate based on stake alone.
    ///         Returns `true` iff the operator has `activeStake >= minStake`
    ///         and is not flagged ejected. Full ADR 003 semantics
    ///         (registration + binding) layer on top of this in the
    ///         follow-up node-registry PR.
    function isActive(address operator) external view returns (bool) {
        return !ejected[operator] && activeStake[operator] >= minStake;
    }

    // -----------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------

    function _enforceMinStakeBounds(uint256 value) internal pure {
        if (value < MIN_STAKE_FLOOR || value > MIN_STAKE_CEILING) {
            revert ParamOutOfBounds({ value: value, floor: MIN_STAKE_FLOOR, ceiling: MIN_STAKE_CEILING });
        }
    }

    function _enforceUnbondingPeriodBounds(uint256 value) internal pure {
        if (value < UNBONDING_PERIOD_FLOOR || value > UNBONDING_PERIOD_CEILING) {
            revert ParamOutOfBounds({ value: value, floor: UNBONDING_PERIOD_FLOOR, ceiling: UNBONDING_PERIOD_CEILING });
        }
    }
}
