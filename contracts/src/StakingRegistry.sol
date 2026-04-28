// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import {
    ReentrancyGuardTransient
} from "@openzeppelin/contracts/utils/ReentrancyGuardTransient.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { EIP712 } from "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import { SignatureChecker } from "@openzeppelin/contracts/utils/cryptography/SignatureChecker.sol";
import { SafeCast } from "@openzeppelin/contracts/utils/math/SafeCast.sol";
import { EnumerableSet } from "@openzeppelin/contracts/utils/structs/EnumerableSet.sol";

import { IStakingRegistry } from "./interfaces/IStakingRegistry.sol";
import { IBurnable } from "./interfaces/IBurnable.sol";
import { Errors } from "./libraries/Errors.sol";
import { Roles } from "./libraries/Roles.sol";

/// @title StakingRegistry
/// @notice Custody of operator and client TOKEN stakes for the deCDN PoC.
/// @dev See ADR 003 §Staking, ADR 004 §Slashing, ADR 016 §3 (call graph), ADR
///      024 (SignatureChecker). PoC uses a flat 10% slash schedule per ADR 004.
///      Each operator holds a single active stake slot; `stake()` tops up the
///      existing slot rather than creating a new one.
///
///      Slashable pool = active + unbonding. Slashing pulls from active first
///      and spills into the oldest unbonding entries if necessary — this
///      preserves the "stake remains slashable during unbonding" invariant
///      (ADR 004) while keeping the common path cheap.
contract StakingRegistry is
    IStakingRegistry,
    AccessControl,
    ReentrancyGuardTransient,
    Pausable,
    EIP712
{
    using SafeERC20 for IERC20;
    using SafeCast for uint256;
    using EnumerableSet for EnumerableSet.AddressSet;

    // ---------------------------------------------------------------------
    //  Constants
    // ---------------------------------------------------------------------

    /// @dev Immutable safety bounds for governable parameters (ADR 009).
    uint256 public constant MIN_STAKE_FLOOR = 100e18; // 100 TOKEN
    uint256 public constant MIN_STAKE_CEILING = 100_000e18; // 100,000 TOKEN
    uint64 public constant UNBONDING_FLOOR = 3 days;
    uint64 public constant UNBONDING_CEILING = 30 days;

    /// @dev Caps the per-operator unbonding queue depth. Slashing and
    /// withdrawal compact the queue in-place (O(N)); without a cap an
    /// operator could grief themselves or — worse — inflate `slash()` gas
    /// past the block limit. Honest operators should rarely exceed 2 or 3.
    uint256 public constant MAX_UNBONDING_ENTRIES = 32;

    /// @dev Flat PoC slash percentage, in basis points. ADR 004.
    uint256 public constant SLASH_BPS_POC = 1000; // 10%
    uint256 public constant BPS_DENOMINATOR = 10_000;

    /// @dev EIP-712 typehash for binding an iroh NodeId to an EVM address.
    ///      ADR 024 mandates EIP-712 + SignatureChecker for all client-signed
    ///      payloads so smart accounts (ERC-4337 / ERC-1271) work out of the box.
    ///      Frozen — bump name on any schema change; existing signed
    ///      bindings would otherwise become unverifiable.
    bytes32 public constant BIND_NODE_TYPEHASH =
        keccak256("BindNodeId(bytes32 nodeId,uint64 nonce)");

    // ---------------------------------------------------------------------
    //  Types
    // ---------------------------------------------------------------------

    /// @notice Operator lifecycle. Encoded as a single enum so the
    ///         nonsensical pair (registered && ejected) is unrepresentable.
    /// @dev `Unregistered` is the zero default: freshly-allocated storage
    ///      reads as Unregistered, not as a valid state.
    enum OperatorState {
        Unregistered,
        Registered,
        Ejected
    }

    struct StakeInfo {
        uint256 active; // currently-bonded balance
        uint256 unbonding; // sum of pending unbonding requests (still slashable)
        bytes32 nodeId; // bound iroh pubkey, zero when unregistered
        OperatorState state;
        uint64 bindingNonce; // monotonic nonce for binding signatures
    }

    struct UnbondRequest {
        uint128 amount;
        uint64 unlockTime;
    }

    // ---------------------------------------------------------------------
    //  Storage
    // ---------------------------------------------------------------------

    IERC20 public immutable TOKEN_CONTRACT;

    /// @notice Minimum stake required for an operator to register a node.
    uint256 public minStake;

    /// @notice Seconds an unbonded amount must wait before withdrawal.
    uint64 public unbondingPeriod;

    mapping(address operator => StakeInfo) internal _stakes;
    mapping(address operator => UnbondRequest[]) internal _unbondQueue;
    mapping(address client => uint256) internal _clientStakes;
    mapping(bytes32 nodeId => address) public nodeIdToOperator;

    /// @notice Set of currently-registered (non-ejected) operators. Mutated
    ///         in `registerNode` and `ejectNode`; read by `getActiveNodes`
    ///         for cold-start bootstrap (ADR 016 §3 Off-Chain Read API).
    EnumerableSet.AddressSet internal _activeOperators;

    /// @notice Unix timestamp (seconds) of the operator's first-ever
    ///         `registerNode` call. Stable across re-registrations — used by
    ///         the reputation cold-start bonus window (ADR 008).
    mapping(address operator => uint64) internal _firstRegisteredAt;

    /// @notice Unix timestamp (seconds) of the operator's most recent
    ///         settlement. Stamped by PaymentChannel contracts holding
    ///         `SETTLEMENT_REPORTER_ROLE`. Returned in `NodeInfo` so clients
    ///         can rank cold-start candidates by recent delivery activity
    ///         (ADR 016 §3 Off-Chain Read API).
    mapping(address operator => uint64) internal _lastSettlementAt;

    // ---------------------------------------------------------------------
    //  Events
    // ---------------------------------------------------------------------

    event Staked(address indexed operator, uint256 amount, uint256 newActive);
    event UnstakeInitiated(
        address indexed operator, uint256 amount, uint64 unlockTime, uint256 queueIndex
    );
    event UnstakeWithdrawn(address indexed operator, uint256 amount);
    event Slashed(
        address indexed node,
        IStakingRegistry.OffenseType indexed offense,
        uint256 slashed,
        uint256 reward,
        address indexed challenger
    );
    event NodeEjected(address indexed operator, uint256 movedToUnbonding, uint64 unlockTime);
    event NodeRegistered(address indexed operator, bytes32 indexed nodeId, uint64 nonce);
    event SettlementRecorded(address indexed operator, uint64 timestamp);
    event ClientStaked(address indexed client, uint256 amount, uint256 newTotal);
    event ClientUnstaked(address indexed client, uint256 amount, uint256 newTotal);
    event MinStakeUpdated(uint256 oldValue, uint256 newValue);
    event UnbondingPeriodUpdated(uint64 oldValue, uint64 newValue);

    // ---------------------------------------------------------------------
    //  Errors
    // ---------------------------------------------------------------------

    error StakeBelowMinimum();
    error InsufficientActiveStake();
    error InsufficientClientStake();
    error AlreadyRegistered();
    error NotRegistered();
    error Ejected();
    error NodeIdTaken();
    error NodeNotStaked();
    error NothingToWithdraw();
    error UnbondingQueueFull();
    error StakeTooSmallToSlash();
    error UnspecifiedOffense();

    // ---------------------------------------------------------------------
    //  Constructor
    // ---------------------------------------------------------------------

    constructor(
        IERC20 token,
        uint256 initialMinStake,
        uint64 initialUnbondingPeriod,
        address admin
    ) EIP712("StakingRegistry", "1") {
        if (address(token) == address(0) || admin == address(0)) revert Errors.ZeroAddress();
        if (initialMinStake < MIN_STAKE_FLOOR || initialMinStake > MIN_STAKE_CEILING) {
            revert Errors.OutOfBounds();
        }
        if (initialUnbondingPeriod < UNBONDING_FLOOR || initialUnbondingPeriod > UNBONDING_CEILING)
        {
            revert Errors.OutOfBounds();
        }

        TOKEN_CONTRACT = token;
        minStake = initialMinStake;
        unbondingPeriod = initialUnbondingPeriod;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _pause(); // deploy script unpauses after all role grants land (ADR 016 §2)
    }

    // ---------------------------------------------------------------------
    //  Operator staking
    // ---------------------------------------------------------------------

    /// @notice Deposit TOKEN into the operator stake slot.
    function stake(
        uint256 amount
    ) external nonReentrant whenNotPaused {
        if (amount == 0) revert Errors.ZeroAmount();
        StakeInfo storage s = _stakes[msg.sender];
        if (s.state == OperatorState.Ejected) revert Ejected();

        s.active += amount;
        emit Staked(msg.sender, amount, s.active);

        TOKEN_CONTRACT.safeTransferFrom(msg.sender, address(this), amount);
    }

    /// @notice Queue `amount` for unbonding. Unbonded funds remain slashable
    /// until withdrawn.
    function unstake(
        uint256 amount
    ) external nonReentrant whenNotPaused {
        if (amount == 0) revert Errors.ZeroAmount();
        StakeInfo storage s = _stakes[msg.sender];
        if (s.active < amount) revert InsufficientActiveStake();
        if (_unbondQueue[msg.sender].length >= MAX_UNBONDING_ENTRIES) {
            revert UnbondingQueueFull();
        }

        // PoC policy: dropping below `minStake` while registered is
        // permitted; `getStakeMultiple` returns 0 and the channel layer
        // denies the discount tier (ADR 003). Registration remains but
        // provides no on-chain benefit until topped back up.

        s.active -= amount;
        s.unbonding += amount;
        uint64 unlock = block.timestamp.toUint64() + unbondingPeriod;
        _unbondQueue[msg.sender].push(
            UnbondRequest({ amount: amount.toUint128(), unlockTime: unlock })
        );
        emit UnstakeInitiated(msg.sender, amount, unlock, _unbondQueue[msg.sender].length - 1);
    }

    /// @notice Sweep all matured unbonding entries and pay out.
    function withdrawUnbonded() external nonReentrant whenNotPaused {
        UnbondRequest[] storage queue = _unbondQueue[msg.sender];
        StakeInfo storage s = _stakes[msg.sender];
        uint256 payout = 0;
        uint256 i = 0;
        uint256 len = queue.length;
        // Two-pointer compaction: pop matured entries from the front, shift
        // survivors. Expected queue depth is O(unstakes since last withdraw)
        // which for honest operators is small (1-2); MAX_UNBONDING_ENTRIES
        // bounds the worst case.
        while (i < len && queue[i].unlockTime <= block.timestamp) {
            payout += queue[i].amount;
            unchecked {
                ++i;
            }
        }
        if (payout == 0) revert NothingToWithdraw();

        // Shift tail down by `i` positions.
        for (uint256 j = i; j < len; ++j) {
            queue[j - i] = queue[j];
        }
        for (uint256 k = 0; k < i; ++k) {
            queue.pop();
        }

        s.unbonding -= payout;
        emit UnstakeWithdrawn(msg.sender, payout);
        TOKEN_CONTRACT.safeTransfer(msg.sender, payout);
    }

    // ---------------------------------------------------------------------
    //  Client staking (ADR 003) — no unbonding, no slashing
    // ---------------------------------------------------------------------

    function clientStake(
        uint256 amount
    ) external nonReentrant whenNotPaused {
        if (amount == 0) revert Errors.ZeroAmount();
        _clientStakes[msg.sender] += amount;
        emit ClientStaked(msg.sender, amount, _clientStakes[msg.sender]);
        TOKEN_CONTRACT.safeTransferFrom(msg.sender, address(this), amount);
    }

    function clientUnstake(
        uint256 amount
    ) external nonReentrant whenNotPaused {
        if (amount == 0) revert Errors.ZeroAmount();
        uint256 bal = _clientStakes[msg.sender];
        if (bal < amount) revert InsufficientClientStake();
        _clientStakes[msg.sender] = bal - amount;
        emit ClientUnstaked(msg.sender, amount, bal - amount);
        TOKEN_CONTRACT.safeTransfer(msg.sender, amount);
    }

    function clientStakeOf(
        address client
    ) external view returns (uint256) {
        return _clientStakes[client];
    }

    // ---------------------------------------------------------------------
    //  Registration
    // ---------------------------------------------------------------------

    /// @notice Bind an iroh NodeId to the caller's address and mark the
    /// operator as registered. Signature is an EIP-712 signature over
    /// `BindNodeId(bytes32 nodeId, uint64 nonce)` by `msg.sender`.
    function registerNode(
        bytes32 nodeId,
        bytes calldata bindingSignature
    ) external nonReentrant whenNotPaused {
        if (nodeId == bytes32(0)) revert Errors.ZeroAddress();
        StakeInfo storage s = _stakes[msg.sender];
        if (s.state == OperatorState.Ejected) revert Ejected();
        if (s.state == OperatorState.Registered) revert AlreadyRegistered();
        if (s.active < minStake) revert StakeBelowMinimum();
        if (nodeIdToOperator[nodeId] != address(0)) revert NodeIdTaken();

        bytes32 structHash = keccak256(abi.encode(BIND_NODE_TYPEHASH, nodeId, s.bindingNonce));
        bytes32 digest = _hashTypedDataV4(structHash);
        if (!SignatureChecker.isValidSignatureNow(msg.sender, digest, bindingSignature)) {
            revert Errors.InvalidSignature();
        }

        uint64 nonce = s.bindingNonce;
        s.nodeId = nodeId;
        s.state = OperatorState.Registered;
        unchecked {
            s.bindingNonce = nonce + 1;
        }
        nodeIdToOperator[nodeId] = msg.sender;
        _activeOperators.add(msg.sender);
        // Stable across re-registrations — only set on the very first call.
        if (_firstRegisteredAt[msg.sender] == 0) {
            _firstRegisteredAt[msg.sender] = block.timestamp.toUint64();
        }

        emit NodeRegistered(msg.sender, nodeId, nonce);
    }

    // ---------------------------------------------------------------------
    //  Role-gated mutations
    // ---------------------------------------------------------------------

    /// @inheritdoc IStakingRegistry
    function slash(
        address node,
        IStakingRegistry.OffenseType offense
    )
        external
        override
        nonReentrant
        whenNotPaused
        onlyRole(Roles.SLASH_ROLE)
        returns (uint256 slashed)
    {
        if (offense == IStakingRegistry.OffenseType.Unspecified) {
            revert UnspecifiedOffense();
        }
        StakeInfo storage s = _stakes[node];
        uint256 slashable = s.active + s.unbonding;
        if (slashable == 0) revert NodeNotStaked();

        slashed = (slashable * SLASH_BPS_POC) / BPS_DENOMINATOR;
        // Reject dust stakes outright rather than silently zero the challenger
        // reward. Under the 10% PoC schedule this fires when slashable < 10
        // wei — within noise of the min-stake bound, so this is a no-op for
        // honest operators.
        if (slashed == 0) revert StakeTooSmallToSlash();

        // Pull from active first, then spill into oldest unbonding entries.
        if (slashed <= s.active) {
            s.active -= slashed;
        } else {
            uint256 remaining = slashed - s.active;
            s.active = 0;
            _consumeUnbonding(node, remaining);
        }

        uint256 reward = slashed / 2;
        uint256 burn = slashed - reward;

        emit Slashed(node, offense, slashed, reward, msg.sender);

        if (reward > 0) TOKEN_CONTRACT.safeTransfer(msg.sender, reward);
        // Real burn: decrements `totalSupply` via ERC20Burnable.burn(). Keeps
        // ADR 004's deflationary semantics intact (a dead-address transfer
        // would not).
        if (burn > 0) IBurnable(address(TOKEN_CONTRACT)).burn(burn);
    }

    /// @inheritdoc IStakingRegistry
    function recordSettlement(
        address operator
    ) external override whenNotPaused onlyRole(Roles.SETTLEMENT_REPORTER_ROLE) {
        // No reentrancy guard: single SSTORE, no external call, no fund
        // movement. Caller is a trusted PaymentChannel that already holds
        // its own nonReentrant guard. Off-chain ranking only — accuracy
        // does not need to be enforced for non-existent operators (a stamp
        // on an unregistered address is harmless and won't appear in
        // `getActiveNodes`).
        uint64 ts = block.timestamp.toUint64();
        _lastSettlementAt[operator] = ts;
        emit SettlementRecorded(operator, ts);
    }

    /// @inheritdoc IStakingRegistry
    function ejectNode(
        address operator
    ) external override nonReentrant whenNotPaused onlyRole(Roles.BLACKLIST_ROLE) {
        StakeInfo storage s = _stakes[operator];
        if (s.state == OperatorState.Ejected) return; // idempotent
        s.state = OperatorState.Ejected;
        _activeOperators.remove(operator);

        // Intentionally do NOT clear `nodeIdToOperator[s.nodeId]`. Leaving
        // the mapping in place permanently locks that NodeId to the ejected
        // address — any fresh attempt to register the same iroh identity
        // from a different EVM address will hit `NodeIdTaken`, and the
        // ejected address itself is now rejected by `Ejected`. This makes
        // ejection a network-identity ban, not just a per-address one.
        // `s.nodeId` is also retained so `nodeIdOf(operator)` continues to
        // report what this operator ran, for audit purposes.

        uint256 moved;
        if (s.active > 0) {
            moved = s.active;
            s.active = 0;
            s.unbonding += moved;
            uint64 unlock = block.timestamp.toUint64() + unbondingPeriod;
            UnbondRequest[] storage queue = _unbondQueue[operator];
            if (queue.length >= MAX_UNBONDING_ENTRIES) {
                // Queue is full — fold the ejected balance into the newest
                // entry rather than pushing a new one. This preserves the
                // MAX_UNBONDING_ENTRIES gas bound relied on by `slash()`.
                // The resulting unlock time is the later of the two.
                UnbondRequest storage tail = queue[queue.length - 1];
                tail.amount = (uint256(tail.amount) + moved).toUint128();
                if (unlock > tail.unlockTime) tail.unlockTime = unlock;
            } else {
                queue.push(UnbondRequest({ amount: moved.toUint128(), unlockTime: unlock }));
            }
            emit NodeEjected(operator, moved, unlock);
        } else {
            emit NodeEjected(operator, 0, 0);
        }
    }

    // ---------------------------------------------------------------------
    //  Governance-controlled parameters
    // ---------------------------------------------------------------------

    function setMinStake(
        uint256 newMinStake
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        if (newMinStake < MIN_STAKE_FLOOR || newMinStake > MIN_STAKE_CEILING) {
            revert Errors.OutOfBounds();
        }
        emit MinStakeUpdated(minStake, newMinStake);
        minStake = newMinStake;
    }

    function setUnbondingPeriod(
        uint64 newPeriod
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        if (newPeriod < UNBONDING_FLOOR || newPeriod > UNBONDING_CEILING) {
            revert Errors.OutOfBounds();
        }
        emit UnbondingPeriodUpdated(unbondingPeriod, newPeriod);
        unbondingPeriod = newPeriod;
    }

    function pause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _unpause();
    }

    // ---------------------------------------------------------------------
    //  Views
    // ---------------------------------------------------------------------

    /// @inheritdoc IStakingRegistry
    function getStakeMultiple(
        address provider
    ) external view override returns (uint256) {
        return _stakes[provider].active / minStake;
    }

    /// @inheritdoc IStakingRegistry
    function nodeIdOf(
        address operator
    ) external view override returns (bytes32) {
        return _stakes[operator].nodeId;
    }

    /// @inheritdoc IStakingRegistry
    function getActiveNodeCount() external view override returns (uint256) {
        return _activeOperators.length();
    }

    /// @inheritdoc IStakingRegistry
    function getActiveNodes(
        uint256 offset,
        uint256 limit
    ) external view override returns (NodeInfo[] memory) {
        uint256 total = _activeOperators.length();
        if (offset >= total) {
            return new NodeInfo[](0);
        }
        uint256 end = offset + limit;
        if (end > total) {
            end = total;
        }
        uint256 count = end - offset;
        NodeInfo[] memory out = new NodeInfo[](count);
        for (uint256 i = 0; i < count; ++i) {
            address op = _activeOperators.at(offset + i);
            out[i] = NodeInfo({
                operator: op, nodeId: _stakes[op].nodeId, lastSettlementAt: _lastSettlementAt[op]
            });
        }
        return out;
    }

    /// @inheritdoc IStakingRegistry
    function getFirstRegisteredAt(
        address ethAddress
    ) external view override returns (uint256) {
        return _firstRegisteredAt[ethAddress];
    }

    /// @notice Direct getter for the settlement timestamp. Mirrors the
    ///         field exposed in `NodeInfo` so callers that already hold an
    ///         operator address can fetch it without paginating the active
    ///         set. Returns 0 if the operator has never had a settlement.
    function lastSettlementAt(
        address operator
    ) external view returns (uint64) {
        return _lastSettlementAt[operator];
    }

    function getStakeInfo(
        address operator
    ) external view returns (StakeInfo memory) {
        return _stakes[operator];
    }

    function unbondingQueueLength(
        address operator
    ) external view returns (uint256) {
        return _unbondQueue[operator].length;
    }

    function unbondingEntry(
        address operator,
        uint256 idx
    ) external view returns (UnbondRequest memory) {
        return _unbondQueue[operator][idx];
    }

    function domainSeparator() external view returns (bytes32) {
        return _domainSeparatorV4();
    }

    // ---------------------------------------------------------------------
    //  Internals
    // ---------------------------------------------------------------------

    /// @dev Consume `amount` from the front of the unbonding queue (oldest
    /// entries first). Called only by `slash()` after active is exhausted.
    function _consumeUnbonding(
        address operator,
        uint256 amount
    ) internal {
        UnbondRequest[] storage queue = _unbondQueue[operator];
        StakeInfo storage s = _stakes[operator];
        uint256 i = 0;
        uint256 len = queue.length;
        while (amount > 0 && i < len) {
            uint256 entry = queue[i].amount;
            if (entry <= amount) {
                amount -= entry;
                s.unbonding -= entry;
                queue[i].amount = 0;
                unchecked {
                    ++i;
                }
            } else {
                // entry is already uint128; (entry - amount) cannot exceed entry.
                queue[i].amount = uint128(entry - amount); // forge-lint:
                // disable-line(unsafe-typecast)
                s.unbonding -= amount;
                amount = 0;
            }
        }
        // Compact: drop fully-consumed leading entries.
        if (i > 0) {
            for (uint256 j = i; j < len; ++j) {
                queue[j - i] = queue[j];
            }
            for (uint256 k = 0; k < i; ++k) {
                queue.pop();
            }
        }
        // Invariant: s.unbonding == sum(queue[i].amount) at entry, so the
        // loop above consumes the full requested amount. Any residual here
        // would mean storage corruption or a broken refactor — surface it.
        if (amount != 0) revert Errors.InvariantViolated();
    }
}
