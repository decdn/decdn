// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { EIP712 } from "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import { SignatureChecker } from "@openzeppelin/contracts/utils/cryptography/SignatureChecker.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

import { IEd25519Verifier } from "./interfaces/IEd25519Verifier.sol";
import { ISafetyReserve } from "./interfaces/ISafetyReserve.sol";

/// @title StakingRegistry — stake, slashing, settlement reporting, and node registry
/// @notice Custodies operator TOKEN stake, executes the 50% / 30% / 20% slash
///         split from ADR 026 § Slashing and burn, is the canonical inflow
///         point for `FeeRouter` settlement reporting, and is the registry
///         for iroh-NodeId ↔ Ethereum-address bindings.
///
/// @dev    OZ v5 composition:
///           - `AccessControl`: 5 role grants (GOVERNANCE / SLASH /
///             BLACKLIST / SETTLEMENT_REPORTER / PAUSER) plus
///             `DEFAULT_ADMIN_ROLE` held by the Timelock per
///             ADR 016 § Deployment Order
///           - `ReentrancyGuard`: every fund-mutating function
///           - `Pausable`: emergency stop on stake / unstake / slash /
///             registerNode / deregisterNode
///           - `EIP712`: domain separator for the `BindNodeId` typed-data
///             signature that `registerNode` consumes
///           - `SafeERC20`: every TOKEN call goes through `safeTransfer*`
///           - `SignatureChecker`: EOA + ERC-1271 + ERC-4337 binding-signature
///             verification (ADR 024)
///
///         Slash math (ADR 026 § Slashing and burn): `uint32` lifetime
///         offense counter; 1st / 2nd / 3rd+ → 5% / 15% / 50%. Applies to
///         active + unbonding stake (active reduced first; unbonding
///         absorbs remainder to prevent slash-then-run per ADR 003).
///         Distribution: 50% to `challenger`, 30% to `SafetyReserve`
///         (via `safeTransfer` + `recordSlashInflow`), 20% burned via
///         `ERC20Burnable.burn`. Auto-ejection: if post-slash active stake
///         falls below `minStake / 2`, the operator's `ejected` flag is
///         set; clearing requires re-staking to `>= minStake` (ADR 003
///         § Node Registry — "must re-stake at full minimum to rejoin").
///
///         Node registry (ADR 003 § Node Registry): `registerNode` binds
///         an iroh NodeId (ed25519 public key) to `msg.sender` after
///         verifying two signatures — an EIP-712 `BindNodeId` from the
///         Ethereum key (`SignatureChecker.isValidSignatureNow`) and an
///         ed25519 signature from the NodeId key (via the injected
///         `IEd25519Verifier` — see ADR 003 § NodeId Ownership Verification).
///         `deregisterNode` clears the `active` flag, removes the operator
///         from the active set, and increments `registrationNonce[nodeId]`
///         to invalidate any pre-deregistration ed25519 signatures. The
///         stake remains untouched — operators manage exit via the
///         separate `requestUnstake` / `unstake` flow.
///
///         Out of this PR: `bindNodeId` (rebinding / key rotation) and
///         `reclaimNodeId` (squat recovery). Both are advanced flows
///         from ADR 003 that don't block the happy path; they layer on
///         top of the binding state this PR establishes.
///
///         ADR drift: ADR 014 line 237 originally specified
///         `slash(node, offenseType)` (2 args). The 50% challenger share
///         requires StakingRegistry to know the challenger address, so
///         the canonical signature is
///         `slash(operator, challenger, offenseType) returns (uint256)`.
///         ADR 014 has been updated to match.
contract StakingRegistry is AccessControl, ReentrancyGuard, Pausable, EIP712 {
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    /// @notice Setter authority for `minStake`, `unbondingPeriod`,
    ///         `safetyReserve`, `multiaddrUpdateCooldown`, and
    ///         `maxMultiaddrSize`. Held by `TimelockController` post-deploy.
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
    uint256 internal constant SLASH_BPS_TIER_1 = 500;
    uint256 internal constant SLASH_BPS_TIER_2 = 1500;
    uint256 internal constant SLASH_BPS_TIER_3 = 5000;
    uint256 internal constant CHALLENGER_BPS = 5000;
    uint256 internal constant SAFETY_BPS = 3000;
    // 20% burn share is the implicit remainder so legs sum exactly.

    // -----------------------------------------------------------------
    // Governable-parameter safety bounds (ADR 009 § Governable parameters)
    // -----------------------------------------------------------------

    uint256 internal constant MIN_STAKE_FLOOR = 10_000e18;
    uint256 internal constant MIN_STAKE_CEILING = 1_000_000e18;

    uint256 internal constant UNBONDING_PERIOD_FLOOR = 3 days;
    uint256 internal constant UNBONDING_PERIOD_CEILING = 30 days;

    uint256 internal constant MULTIADDR_COOLDOWN_CEILING = 1 days;

    uint256 internal constant MAX_MULTIADDR_SIZE_FLOOR = 64;
    uint256 internal constant MAX_MULTIADDR_SIZE_CEILING = 1024;

    // -----------------------------------------------------------------
    // EIP-712 typehashes
    // -----------------------------------------------------------------

    /// @notice EIP-712 typehash for the `BindNodeId(bytes32 nodeId,uint64 nonce)`
    ///         struct that `registerNode` requires the caller to sign with
    ///         their Ethereum key. Per ADR 003 § Binding Message Format.
    bytes32 public constant BIND_NODE_TYPEHASH = keccak256("BindNodeId(bytes32 nodeId,uint64 nonce)");

    // -----------------------------------------------------------------
    // Immutable wiring
    // -----------------------------------------------------------------

    /// @notice TOKEN contract. `ERC20Burnable` typing lets `slash` call
    ///         `token.burn(amount)` against the canonical OZ surface.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ERC20Burnable public immutable token;

    /// @notice Ed25519 signature verifier. Pluggable per ADR 003
    ///         § NodeId Ownership Verification — wraps a vetted Solidity
    ///         library today, swappable if RIP-7212 ever ships on Arbitrum.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IEd25519Verifier public immutable ed25519Verifier;

    // -----------------------------------------------------------------
    // Storage — staking
    // -----------------------------------------------------------------

    struct UnbondingRequest {
        uint256 amount;
        uint256 unlockAt;
    }

    /// @notice Active stake per operator. Excludes unbonding amounts — those
    ///         live in `unbondingOf` but remain slashable (ADR 003 — "stake
    ///         remains slashable during unbonding").
    mapping(address operator => uint256 amount) public activeStake;

    /// @notice In-flight unbonding per operator. At most one outstanding;
    ///         `requestUnstake` reverts if a request already exists.
    mapping(address operator => UnbondingRequest) public unbondingOf;

    /// @notice Monotonically-increasing lifetime offense count.
    mapping(address operator => uint32) public lifetimeOffenseCount;

    /// @notice `block.timestamp` of the most recent `recordSettlement`.
    mapping(address operator => uint256) public lastSettlementAt;

    /// @notice Auto-eject / blacklist-eject flag. Cleared on re-stake to
    ///         `>= minStake` (ADR 003 § Node Registry).
    mapping(address operator => bool) public ejected;

    uint256 public minStake;
    uint256 public unbondingPeriod;

    /// @notice SafetyReserve sink for the 30% slash leg. Zero at construction;
    ///         must be wired post-deploy before any `slash` can fire.
    ISafetyReserve public safetyReserve;

    // -----------------------------------------------------------------
    // Storage — node registry (ADR 003 § Node Registry)
    // -----------------------------------------------------------------

    struct NodeInfo {
        bytes32 nodeId;
        address ethAddress;
        bool active;
        uint256 registeredAt;
        uint256 firstRegisteredAt;
        uint256 lastMultiaddrUpdate;
        bytes multiaddrs;
        string regionHint;
    }

    /// @notice Per-operator registration record. Persists across deregister /
    ///         re-register cycles; `firstRegisteredAt` is write-once.
    mapping(address operator => NodeInfo) internal _nodes;

    /// @notice NodeId → Ethereum address. Reverse of `addressToNodeId`.
    mapping(bytes32 nodeId => address operator) public nodeIdToAddress;

    /// @notice Ethereum address → NodeId. Cleared via `bindNodeId` rebinding
    ///         (which lands with the rebinding PR).
    mapping(address operator => bytes32 nodeId) public addressToNodeId;

    /// @notice Per-Ethereum-address replay counter for the EIP-712 binding
    ///         signature. Incremented after each successful `registerNode`.
    mapping(address operator => uint64) public bindingNonce;

    /// @notice Per-NodeId replay counter for the ed25519 ownership signature.
    ///         Incremented on `deregisterNode` so a previously-signed
    ///         registration can't be replayed after the binding is released.
    mapping(bytes32 nodeId => uint64) public registrationNonce;

    /// @notice Operators currently registered (`NodeInfo.active == true`).
    ///         Backing array for `getActiveNodes` pagination. NOT filtered
    ///         by stake / ejected state — clients combine with `isActive`
    ///         for the fully-active predicate.
    address[] internal _registeredAddrs;

    /// @notice 1-indexed position of operator in `_registeredAddrs`. 0 means
    ///         not currently in the array. Enables O(1) swap-pop removal.
    mapping(address operator => uint256 onePlusIndex) internal _registeredIndex;

    uint256 public multiaddrUpdateCooldown;
    uint256 public maxMultiaddrSize;

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
    /// @dev `block.timestamp` is implicit on every log via the block header.
    event SettlementRecorded(address indexed operator);
    event MinStakeUpdated(uint256 oldValue, uint256 newValue);
    event UnbondingPeriodUpdated(uint256 oldValue, uint256 newValue);
    event SafetyReserveUpdated(address indexed oldAddr, address indexed newAddr);
    event MultiaddrUpdateCooldownUpdated(uint256 oldValue, uint256 newValue);
    event MaxMultiaddrSizeUpdated(uint256 oldValue, uint256 newValue);

    // Node-registry events (ADR 003 § Node Registry).
    event NodeRegistered(
        bytes32 indexed nodeId,
        address indexed ethAddress,
        bytes multiaddrs,
        string regionHint,
        uint64 bindingNonce,
        uint64 registrationNonce
    );
    event NodeIdBound(address indexed ethAddress, bytes32 indexed nodeId, uint64 bindingNonce);
    event NodeMultiaddrUpdated(bytes32 indexed nodeId, bytes multiaddrs);
    event NodeDeregistered(bytes32 indexed nodeId);
    event NodeAutoEjected(bytes32 indexed nodeId, uint256 remainingStake);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error ZeroAmount();
    error ZeroNodeId();
    error InsufficientStake(uint256 requested, uint256 available);
    error UnbondingInProgress();
    error UnbondingNotComplete(uint256 unlockAt);
    error NoUnbondingRequest();
    error SafetyReserveNotWired();
    error ParamOutOfBounds(uint256 value, uint256 floor, uint256 ceiling);
    error NodeAlreadyRegistered();
    error NodeIdAlreadyBound(address currentOwner);
    error AddressAlreadyBound(bytes32 currentNodeId);
    error InvalidBindingSignature();
    error InvalidEd25519Signature();
    error MultiaddrsTooLarge(uint256 size, uint256 ceiling);
    error MultiaddrCooldownActive(uint256 readyAt);
    error NodeNotActive();
    error StakeBelowMinimum(uint256 stake, uint256 required);

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param token_                    TOKEN contract (must implement `burn`).
    /// @param ed25519Verifier_          Verifier abstraction for `registerNode`'s
    ///                                  NodeId-ownership proof.
    /// @param admin                     Initial holder of `DEFAULT_ADMIN_ROLE`
    ///                                  (Timelock at mainnet).
    /// @param minStake_                 Initial minimum active stake (default 50k
    ///                                  per ADR 026; bounded `[10k, 1M]`).
    /// @param unbondingPeriod_          Initial unbonding period (default 7 days;
    ///                                  bounded `[3, 30]` days per ADR 009).
    /// @param multiaddrUpdateCooldown_  Initial cooldown between `updateMultiaddrs`
    ///                                  calls (default 0 per ADR 003 line 684;
    ///                                  bounded `[0, 1 day]`).
    /// @param maxMultiaddrSize_         Initial cap on multiaddrs byte length
    ///                                  (default 1024 per ADR 003; bounded `[64, 1024]`).
    constructor(
        ERC20Burnable token_,
        IEd25519Verifier ed25519Verifier_,
        address admin,
        uint256 minStake_,
        uint256 unbondingPeriod_,
        uint256 multiaddrUpdateCooldown_,
        uint256 maxMultiaddrSize_
    ) EIP712("StakingRegistry", "1") {
        if (address(token_) == address(0) || address(ed25519Verifier_) == address(0) || admin == address(0)) {
            revert ZeroAddress();
        }
        _enforceMinStakeBounds(minStake_);
        _enforceUnbondingPeriodBounds(unbondingPeriod_);
        _enforceMultiaddrCooldownBounds(multiaddrUpdateCooldown_);
        _enforceMaxMultiaddrSizeBounds(maxMultiaddrSize_);

        token = token_;
        ed25519Verifier = ed25519Verifier_;
        minStake = minStake_;
        unbondingPeriod = unbondingPeriod_;
        multiaddrUpdateCooldown = multiaddrUpdateCooldown_;
        maxMultiaddrSize = maxMultiaddrSize_;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, admin);
    }

    // -----------------------------------------------------------------
    // Staking
    // -----------------------------------------------------------------

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

    function unstake() external nonReentrant whenNotPaused {
        UnbondingRequest memory req = unbondingOf[msg.sender];
        if (req.amount == 0) revert NoUnbondingRequest();
        // Validator timestamp manipulation is bounded by consensus drift
        // (seconds) and dwarfed by the unbonding period (days per ADR 003).
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp < req.unlockAt) revert UnbondingNotComplete(req.unlockAt);

        delete unbondingOf[msg.sender];
        IERC20(address(token)).safeTransfer(msg.sender, req.amount);
        emit Unstaked(msg.sender, req.amount);
    }

    // -----------------------------------------------------------------
    // Node registry (ADR 003 § Node Registry)
    // -----------------------------------------------------------------

    /// @notice Register or re-register a node, binding `nodeId` to `msg.sender`.
    /// @dev    Two-signature verification per ADR 003 § NodeId-to-Ethereum
    ///         Binding: the EIP-712 `BindNodeId` (Ethereum key, via
    ///         `SignatureChecker` for EOA + ERC-1271 + ERC-4337) proves
    ///         consent; the ed25519 signature (via `IEd25519Verifier`)
    ///         proves NodeId ownership. Requires `activeStake[msg.sender]
    ///         >= minStake` per ADR 003 line 626 "Registration (requires
    ///         active stake >= minStake)".
    ///         For re-registration after deregister, the same NodeId may
    ///         be reused — `nodeIdToAddress[nodeId] == msg.sender` is
    ///         allowed. Switching to a NEW NodeId after deregister
    ///         requires the rebinding PR's `bindNodeId` flow.
    /// @param nodeId             32-byte iroh NodeId (ed25519 public key).
    /// @param multiaddrs         Packed QUIC multiaddrs; bounded by `maxMultiaddrSize`.
    /// @param regionHint         ISO 3166-1 alpha-2 code, unverified.
    /// @param bindingSignature   EIP-712 signature over
    ///                           `BindNodeId(nodeId, bindingNonce[msg.sender])`
    ///                           by `msg.sender`'s Ethereum key.
    /// @param ed25519Signature   ed25519 signature over
    ///                           `keccak256(abi.encodePacked(nodeId, msg.sender,
    ///                           block.chainid, registrationNonce[nodeId]))`
    ///                           by the NodeId's ed25519 key.
    function registerNode(
        bytes32 nodeId,
        bytes calldata multiaddrs,
        string calldata regionHint,
        bytes calldata bindingSignature,
        bytes calldata ed25519Signature
    ) external nonReentrant whenNotPaused {
        _checkRegistrationPreconditions(nodeId, multiaddrs.length);
        _checkBindingOneToOne(nodeId);
        uint64 usedBindingNonce = _verifyBindingSignature(nodeId, bindingSignature);
        uint64 usedRegistrationNonce = _verifyEd25519OwnershipSignature(nodeId, ed25519Signature);

        // Commit binding + node info. `firstRegisteredAt` is write-once
        // (ADR 003 line 680 — retained across re-register).
        nodeIdToAddress[nodeId] = msg.sender;
        addressToNodeId[msg.sender] = nodeId;
        bindingNonce[msg.sender] = usedBindingNonce + 1;
        _writeNodeInfo(nodeId, multiaddrs, regionHint);
        _addToRegisteredSet(msg.sender);

        emit NodeRegistered(nodeId, msg.sender, multiaddrs, regionHint, usedBindingNonce, usedRegistrationNonce);
        emit NodeIdBound(msg.sender, nodeId, usedBindingNonce);
    }

    function _checkRegistrationPreconditions(bytes32 nodeId, uint256 multiaddrsLength) internal view {
        if (nodeId == bytes32(0)) revert ZeroNodeId();
        if (multiaddrsLength > maxMultiaddrSize) {
            revert MultiaddrsTooLarge({ size: multiaddrsLength, ceiling: maxMultiaddrSize });
        }
        if (activeStake[msg.sender] < minStake) {
            revert StakeBelowMinimum({ stake: activeStake[msg.sender], required: minStake });
        }
        if (_nodes[msg.sender].active) revert NodeAlreadyRegistered();
    }

    /// @dev One-to-one binding constraint (ADR 003 line 676). Re-registration
    ///      with the *same* NodeId is allowed; switching to a different
    ///      NodeId is rebinding territory.
    function _checkBindingOneToOne(bytes32 nodeId) internal view {
        address nodeIdOwner = nodeIdToAddress[nodeId];
        if (nodeIdOwner != address(0) && nodeIdOwner != msg.sender) {
            revert NodeIdAlreadyBound(nodeIdOwner);
        }
        bytes32 currentBinding = addressToNodeId[msg.sender];
        if (currentBinding != bytes32(0) && currentBinding != nodeId) {
            revert AddressAlreadyBound(currentBinding);
        }
    }

    function _verifyBindingSignature(bytes32 nodeId, bytes calldata sig) internal view returns (uint64 nonce) {
        nonce = bindingNonce[msg.sender];
        bytes32 digest = _hashTypedDataV4(keccak256(abi.encode(BIND_NODE_TYPEHASH, nodeId, nonce)));
        if (!SignatureChecker.isValidSignatureNow(msg.sender, digest, sig)) {
            revert InvalidBindingSignature();
        }
    }

    function _verifyEd25519OwnershipSignature(bytes32 nodeId, bytes calldata sig) internal view returns (uint64 nonce) {
        nonce = registrationNonce[nodeId];
        bytes32 messageHash = keccak256(abi.encodePacked(nodeId, msg.sender, block.chainid, nonce));
        if (!ed25519Verifier.verify(nodeId, messageHash, sig)) {
            revert InvalidEd25519Signature();
        }
    }

    function _writeNodeInfo(bytes32 nodeId, bytes calldata multiaddrs, string calldata regionHint) internal {
        NodeInfo storage info = _nodes[msg.sender];
        info.nodeId = nodeId;
        info.ethAddress = msg.sender;
        info.active = true;
        info.registeredAt = block.timestamp;
        // `firstRegisteredAt == 0` is a write-once sentinel (zero means
        // "never registered"), not a value-bearing comparison — the strict
        // equality is exactly the intended semantics per ADR 003 line 680.
        // slither-disable-next-line incorrect-equality
        if (info.firstRegisteredAt == 0) {
            info.firstRegisteredAt = block.timestamp;
        }
        info.lastMultiaddrUpdate = block.timestamp;
        info.multiaddrs = multiaddrs;
        info.regionHint = regionHint;
    }

    /// @notice Mark `msg.sender`'s registered node as inactive and remove
    ///         from the active set. Does NOT move stake — operators
    ///         exit stake separately via `requestUnstake` / `unstake`.
    /// @dev    Increments `registrationNonce[nodeId]` so any pre-deregistration
    ///         ed25519 signature cannot be replayed after a re-registration.
    ///         Stake remains slashable while it sits in the unbonding flow;
    ///         the `MAX_EVIDENCE_AGE_US < unbondingPeriod` invariant
    ///         (ADR 014 § Unbonding interaction) prevents slash-then-run.
    function deregisterNode() external nonReentrant whenNotPaused {
        NodeInfo storage info = _nodes[msg.sender];
        if (!info.active) revert NodeNotActive();

        bytes32 nodeId = info.nodeId;
        info.active = false;
        registrationNonce[nodeId] += 1;
        _removeFromRegisteredSet(msg.sender);

        emit NodeDeregistered(nodeId);
    }

    /// @notice Replace the registered multiaddrs for `msg.sender`'s node.
    /// @dev    Governable cooldown rate-limits this path against compromised
    ///         keys flipping multiaddrs to redirect traffic. Default cooldown
    ///         is 0 (disabled) per ADR 003 line 684.
    function updateMultiaddrs(bytes calldata multiaddrs) external whenNotPaused {
        NodeInfo storage info = _nodes[msg.sender];
        if (!info.active) revert NodeNotActive();
        if (multiaddrs.length > maxMultiaddrSize) {
            revert MultiaddrsTooLarge({ size: multiaddrs.length, ceiling: maxMultiaddrSize });
        }
        uint256 readyAt = info.lastMultiaddrUpdate + multiaddrUpdateCooldown;
        // Validator timestamp manipulation is bounded by consensus drift
        // (seconds) and the cooldown is a coarse anti-abuse throttle (hours),
        // so second-level skew is immaterial here.
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp < readyAt) revert MultiaddrCooldownActive(readyAt);

        info.multiaddrs = multiaddrs;
        info.lastMultiaddrUpdate = block.timestamp;

        emit NodeMultiaddrUpdated(info.nodeId, multiaddrs);
    }

    // -----------------------------------------------------------------
    // Slashing (SLASH_ROLE — held by SlashJudge)
    // -----------------------------------------------------------------

    /// @notice Slash `operator`, splitting `slashAmount` as 50% to
    ///         `challenger` / 30% to SafetyReserve / 20% burn.
    /// @dev    Returns the total slashed amount so `SlashJudge` can emit
    ///         the canonical `Slashed` event with the figure inline.
    function slash(address operator, address challenger, uint8 offenseType)
        external
        nonReentrant
        whenNotPaused
        onlyRole(SLASH_ROLE)
        returns (uint256 slashAmount)
    {
        if (operator == address(0) || challenger == address(0)) revert ZeroAddress();
        if (address(safetyReserve) == address(0)) revert SafetyReserveNotWired();

        uint32 newCount = lifetimeOffenseCount[operator] + 1;
        lifetimeOffenseCount[operator] = newCount;
        uint256 tierBps = newCount == 1 ? SLASH_BPS_TIER_1 : newCount == 2 ? SLASH_BPS_TIER_2 : SLASH_BPS_TIER_3;

        // Block scopes `req`, `totalAtRisk`, and `remainder` so they release
        // their stack slots before the share locals + eject bookkeeping are
        // declared below — otherwise the function overflows Solidity's
        // 16-slot stack limit (compile fails without via_ir).
        {
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

        // Checks-effects-interactions: decide the eject status from
        // post-slash state and commit ALL eject effects (the `ejected` flag
        // plus the node-registry teardown via `_ejectNodeEffects`) BEFORE the
        // external calls below. ADR 026 — "auto-ejection at 50% of minimum
        // stake"; triggered on the *active* leg only since an operator with
        // stake in unbonding is already exiting. The AutoEjected /
        // NodeAutoEjected emits are deferred to the end so indexer log order
        // still reads "transfers, then eject".
        bool autoEject = activeStake[operator] < (minStake / 2) && !ejected[operator];
        bytes32 ejectedNodeId = bytes32(0);
        if (autoEject) {
            ejected[operator] = true;
            ejectedNodeId = _ejectNodeEffects(operator);
        }

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

        if (autoEject) {
            emit AutoEjected(operator, activeStake[operator]);
            if (ejectedNodeId != bytes32(0)) {
                emit NodeAutoEjected(ejectedNodeId, activeStake[operator]);
            }
        }

        emit Slashed(operator, challenger, offenseType, newCount, slashAmount, challengerShare, safetyShare, burnShare);
    }

    // -----------------------------------------------------------------
    // Blacklist ejection
    // -----------------------------------------------------------------

    function ejectNode(address operator) external onlyRole(BLACKLIST_ROLE) {
        if (operator == address(0)) revert ZeroAddress();
        if (!ejected[operator]) {
            ejected[operator] = true;
            emit EjectedByBlacklist(operator);
            bytes32 nodeId = _ejectNodeEffects(operator);
            if (nodeId != bytes32(0)) {
                emit NodeAutoEjected(nodeId, activeStake[operator]);
            }
        }
    }

    // -----------------------------------------------------------------
    // Settlement reporter callback (FeeRouter)
    // -----------------------------------------------------------------

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

    function setMultiaddrUpdateCooldown(uint256 newCooldown) external onlyRole(GOVERNANCE_ROLE) {
        _enforceMultiaddrCooldownBounds(newCooldown);
        uint256 oldCooldown = multiaddrUpdateCooldown;
        multiaddrUpdateCooldown = newCooldown;
        emit MultiaddrUpdateCooldownUpdated(oldCooldown, newCooldown);
    }

    function setMaxMultiaddrSize(uint256 newSize) external onlyRole(GOVERNANCE_ROLE) {
        _enforceMaxMultiaddrSizeBounds(newSize);
        uint256 oldSize = maxMultiaddrSize;
        maxMultiaddrSize = newSize;
        emit MaxMultiaddrSizeUpdated(oldSize, newSize);
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
    // Views — stake
    // -----------------------------------------------------------------

    function stakeOf(address operator) external view returns (uint256) {
        return activeStake[operator];
    }

    function getStakeMultiple(address operator) external view returns (uint256) {
        return activeStake[operator] / minStake;
    }

    /// @notice Full ADR 003 active predicate: registered + active flag + stake
    ///         at or above `minStake` + not ejected.
    function isActive(address operator) public view returns (bool) {
        return _nodes[operator].active && activeStake[operator] >= minStake && !ejected[operator];
    }

    // -----------------------------------------------------------------
    // Views — node registry (ADR 003 § Node Registry)
    // -----------------------------------------------------------------

    /// @notice Bundled per-operator binding + activity read (ADR 003 line 780).
    ///         `active` is the full predicate (registered + stake + not ejected);
    ///         `nodeId` is `bytes32(0)` if the operator has no binding.
    function nodeIdOf(address operator) external view returns (bytes32 nodeId, bool active) {
        nodeId = addressToNodeId[operator];
        active = isActive(operator);
    }

    function getNode(bytes32 nodeId) external view returns (NodeInfo memory) {
        address ethAddress = nodeIdToAddress[nodeId];
        return _nodes[ethAddress];
    }

    function getNodeByAddress(address ethAddress) external view returns (NodeInfo memory) {
        return _nodes[ethAddress];
    }

    function isActiveNode(bytes32 nodeId) external view returns (bool) {
        address ethAddress = nodeIdToAddress[nodeId];
        if (ethAddress == address(0)) return false;
        return isActive(ethAddress);
    }

    function getFirstRegisteredAt(address operator) external view returns (uint256) {
        return _nodes[operator].firstRegisteredAt;
    }

    /// @notice Count of currently-registered operators (`NodeInfo.active == true`).
    function getActiveNodeCount() external view returns (uint256) {
        return _registeredAddrs.length;
    }

    /// @notice Paginated registered-operator NodeInfo. Returns up to `limit`
    ///         entries starting at `offset`; if `offset >= len`, returns empty.
    /// @dev    The underlying list tracks `NodeInfo.active == true` operators
    ///         only; it does NOT filter by stake or ejection. Consumers that
    ///         want the fully-active set combine each returned entry with a
    ///         per-operator `isActive(ethAddress)` check.
    function getActiveNodes(uint256 offset, uint256 limit) external view returns (NodeInfo[] memory page) {
        uint256 len = _registeredAddrs.length;
        if (offset >= len || limit == 0) {
            return new NodeInfo[](0);
        }
        uint256 end = offset + limit;
        if (end > len) end = len;
        uint256 size = end - offset;
        page = new NodeInfo[](size);
        for (uint256 i = 0; i < size; i++) {
            page[i] = _nodes[_registeredAddrs[offset + i]];
        }
    }

    // -----------------------------------------------------------------
    // Internal helpers — active set
    // -----------------------------------------------------------------

    function _addToRegisteredSet(address operator) internal {
        if (_registeredIndex[operator] != 0) return; // already present
        _registeredAddrs.push(operator);
        _registeredIndex[operator] = _registeredAddrs.length; // 1-indexed
    }

    function _removeFromRegisteredSet(address operator) internal {
        uint256 onePlusIdx = _registeredIndex[operator];
        if (onePlusIdx == 0) return; // not present
        uint256 idx = onePlusIdx - 1;
        uint256 lastIdx = _registeredAddrs.length - 1;
        if (idx != lastIdx) {
            address swapped = _registeredAddrs[lastIdx];
            _registeredAddrs[idx] = swapped;
            _registeredIndex[swapped] = idx + 1;
        }
        _registeredAddrs.pop();
        delete _registeredIndex[operator];
    }

    /// @notice Effects-only node teardown shared by slash auto-eject and
    ///         blacklist eject. If the operator currently has an active
    ///         registered node, flips the flag, bumps `registrationNonce[nodeId]`,
    ///         and drops them from the active set. Returns the affected
    ///         `nodeId`, or `bytes32(0)` if the operator had no active node.
    /// @dev    The `NodeAutoEjected` emit is left to the caller so `slash`
    ///         can defer it past its external calls (checks-effects-interactions
    ///         log ordering). The `registrationNonce` bump mirrors
    ///         `deregisterNode`: every exit from the active set invalidates
    ///         any pre-exit ed25519 registration signature, so re-registration
    ///         always requires a fresh proof of NodeId ownership (ADR 003
    ///         § NodeId Ownership Verification).
    function _ejectNodeEffects(address operator) internal returns (bytes32 nodeId) {
        NodeInfo storage info = _nodes[operator];
        if (!info.active) return bytes32(0);
        nodeId = info.nodeId;
        info.active = false;
        registrationNonce[nodeId] += 1;
        _removeFromRegisteredSet(operator);
    }

    // -----------------------------------------------------------------
    // Internal helpers — parameter bounds
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

    function _enforceMultiaddrCooldownBounds(uint256 value) internal pure {
        if (value > MULTIADDR_COOLDOWN_CEILING) {
            revert ParamOutOfBounds({ value: value, floor: 0, ceiling: MULTIADDR_COOLDOWN_CEILING });
        }
    }

    function _enforceMaxMultiaddrSizeBounds(uint256 value) internal pure {
        if (value < MAX_MULTIADDR_SIZE_FLOOR || value > MAX_MULTIADDR_SIZE_CEILING) {
            revert ParamOutOfBounds({
                value: value, floor: MAX_MULTIADDR_SIZE_FLOOR, ceiling: MAX_MULTIADDR_SIZE_CEILING
            });
        }
    }
}
