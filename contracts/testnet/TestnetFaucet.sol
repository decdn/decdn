// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

/// @title TestnetFaucet — per-address TOKEN dispenser for testnet onboarding
/// @notice ⚠️ TESTNET ONLY. Intentionally **out of the audited contract
///         surface** tracked in decdn/decdn#452. Three layers prevent it
///         from reaching Ethereum mainnet:
///           1. On-chain: the constructor reverts `MainnetForbidden` when
///              `block.chainid == 1`.
///           2. CI: a grep gate in `.github/workflows/ci.yml` (`solidity
///              build+test` job) fails any PR referencing `TestnetFaucet`
///              from `contracts/src/` or from any `contracts/script/*.s.sol`
///              other than `TestnetFaucet.s.sol`.
///           3. Convention: the file lives at `contracts/testnet/`, off the
///              audited source tree, and its deploy script is separately
///              named so future production deploy scripts cannot reference
///              it without tripping the CI gate.
///
///         The deployer pre-funds the faucet in the constructor by approving
///         `initialFunding` TOKEN to this address and then deploying. Each
///         caller may invoke `claim()` to receive `claimAmount` TOKEN once
///         per `cooldown` seconds. Governance can re-tune `claimAmount` and
///         `cooldown` at any time, pause claims via `Pausable`, and reclaim
///         any leftover balance via `withdraw`. USDC distribution is **not**
///         in scope: Circle operates the canonical Sepolia USDC faucet at
///         developers.circle.com/stablecoins/docs/usdc-on-testnet.
///
/// @dev    Composition mirrors `CapacityBond`:
///           - `AccessControl`: three roles
///               * `DEFAULT_ADMIN_ROLE` — held by the deployer multisig;
///                 grants and revokes the other two roles.
///               * `GOVERNANCE_ROLE` — `setClaimAmount`, `setCooldown`,
///                 `withdraw`.
///               * `PAUSER_ROLE` — `pause` / `unpause`.
///           - `ReentrancyGuard` — covers `claim` and `withdraw` (both move
///             TOKEN out of the contract).
///           - `Pausable` — emergency stop on the `claim` path. `withdraw`
///             is intentionally pause-independent so governance can drain a
///             paused faucet without unpausing it.
///
///         No upper bounds on `claimAmount` or `cooldown` — testnet-only,
///         blast radius is contained by `Pausable` and by the governance
///         multisig holding both `GOVERNANCE_ROLE` and `PAUSER_ROLE`.
contract TestnetFaucet is AccessControl, ReentrancyGuard, Pausable {
    using SafeERC20 for IERC20;

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    /// @notice Setter authority for `claimAmount`, `cooldown`, and `withdraw`.
    bytes32 public constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");

    /// @notice Authority to pause / unpause the `claim` path.
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    // -----------------------------------------------------------------
    // Immutable wiring
    // -----------------------------------------------------------------

    /// @notice TOKEN dispensed by this faucet. Typed as the minimal `IERC20`
    ///         surface — the faucet only needs `balanceOf` + `transfer` +
    ///         `transferFrom`, never `burn` or any extension.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    IERC20 public immutable token;

    // -----------------------------------------------------------------
    // Storage
    // -----------------------------------------------------------------

    /// @notice TOKEN dispensed per `claim()`. Settable by `GOVERNANCE_ROLE`.
    uint256 public claimAmount;

    /// @notice Minimum seconds between consecutive claims per address.
    ///         Settable by `GOVERNANCE_ROLE`. May be 0 (no cooldown). Bounded
    ///         above by `MAX_COOLDOWN` so `last + cooldown` cannot overflow
    ///         `uint256` for any realistic `last`, keeping `claim()` and
    ///         `timeUntilNext()` from reverting with a generic `Panic(0x11)`.
    uint256 public cooldown;

    /// @notice Upper bound on `cooldown`. 30 days is far longer than any
    ///         realistic testnet faucet cadence and many orders of magnitude
    ///         below `type(uint256).max`, so `last + cooldown` never
    ///         overflows. The bound also prevents a misconfigured (or
    ///         compromised) `GOVERNANCE_ROLE` from bricking the faucet's
    ///         revert messages by pushing every claimer into an unreachable
    ///         future.
    uint256 public constant MAX_COOLDOWN = 30 days;

    /// @notice `block.timestamp` of each caller's most recent successful
    ///         claim. Sentinel value `0` means "never claimed", which is
    ///         always allowed regardless of `cooldown`.
    mapping(address claimer => uint256 timestamp) public lastClaimedAt;

    // -----------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------

    event Claimed(address indexed claimer, uint256 amount);
    event ClaimAmountSet(uint256 oldValue, uint256 newValue);
    event CooldownSet(uint256 oldValue, uint256 newValue);
    event Withdrawn(address indexed to, uint256 amount);

    // -----------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------

    error ZeroAddress();
    error ZeroAmount();
    error CooldownNotElapsed(uint256 remaining);
    error InsufficientBalance(uint256 balance, uint256 requested);
    error CooldownTooLarge(uint256 provided, uint256 max);
    error MainnetForbidden();

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    /// @param token_              TOKEN contract to dispense.
    /// @param treasury            Source of the initial funding. Must have
    ///                            approved this contract for at least
    ///                            `initialFunding` TOKEN before deploy.
    /// @param initialFunding      Pulled from `treasury` into this contract
    ///                            atomically in the constructor.
    /// @param initialClaimAmount  Per-claim payout at launch (e.g. `1_000e18`).
    /// @param initialCooldown     Seconds between claims per address at
    ///                            launch (e.g. `1 days`).
    /// @param admin               Initial `DEFAULT_ADMIN_ROLE` holder
    ///                            (deployer multisig).
    /// @param governance          Initial `GOVERNANCE_ROLE` holder.
    /// @param pauser              Initial `PAUSER_ROLE` holder.
    constructor(
        IERC20 token_,
        address treasury,
        uint256 initialFunding,
        uint256 initialClaimAmount,
        uint256 initialCooldown,
        address admin,
        address governance,
        address pauser
    ) {
        // Defense-in-depth against accidental mainnet deployment. Combined
        // with the CI grep gate and the `contracts/testnet/` path convention,
        // this is the only layer that catches an operator pointing
        // `forge create` directly at this file with a mainnet RPC.
        if (block.chainid == 1) revert MainnetForbidden();

        if (
            address(token_) == address(0) || treasury == address(0) || admin == address(0) || governance == address(0)
                || pauser == address(0)
        ) {
            revert ZeroAddress();
        }
        if (initialFunding == 0 || initialClaimAmount == 0) revert ZeroAmount();
        // `initialCooldown == 0` is allowed: governance may legitimately want
        // a no-cooldown faucet for a short test burst. Pausing is the safety
        // net if this is abused.
        if (initialCooldown > MAX_COOLDOWN) {
            revert CooldownTooLarge({ provided: initialCooldown, max: MAX_COOLDOWN });
        }

        token = token_;
        claimAmount = initialClaimAmount;
        cooldown = initialCooldown;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(GOVERNANCE_ROLE, governance);
        _grantRole(PAUSER_ROLE, pauser);

        token_.safeTransferFrom(treasury, address(this), initialFunding);
    }

    // -----------------------------------------------------------------
    // Claim
    // -----------------------------------------------------------------

    /// @notice Dispense `claimAmount` TOKEN to `msg.sender`. First-time
    ///         callers may claim immediately; subsequent calls must wait
    ///         at least `cooldown` seconds since the caller's last claim.
    /// @dev    CEI: `lastClaimedAt` is stamped before the external transfer.
    ///         Validator timestamp drift is bounded by consensus (seconds)
    ///         and is dwarfed by typical cooldown values (hours-to-days);
    ///         since this is testnet onboarding, treat `block.timestamp` as
    ///         authoritative.
    ///
    ///         Modifier order: `whenNotPaused` runs before `nonReentrant`.
    ///         Either ordering produces equivalent state (the revert rolls
    ///         back any SSTORE the guard performs); the win is gas — the
    ///         pause-revert path avoids executing the reentrancy-guard's
    ///         SSTORE before the revert.
    function claim() external whenNotPaused nonReentrant {
        uint256 amount = claimAmount;
        uint256 last = lastClaimedAt[msg.sender];

        if (last != 0) {
            uint256 nextAvailable = last + cooldown;
            // forge-lint: disable-next-line(block-timestamp)
            if (block.timestamp < nextAvailable) {
                // forge-lint: disable-next-line(block-timestamp)
                revert CooldownNotElapsed({ remaining: nextAvailable - block.timestamp });
            }
        }

        uint256 balance = token.balanceOf(address(this));
        if (balance < amount) revert InsufficientBalance({ balance: balance, requested: amount });

        // forge-lint: disable-next-line(block-timestamp)
        lastClaimedAt[msg.sender] = block.timestamp;
        emit Claimed(msg.sender, amount);

        token.safeTransfer(msg.sender, amount);
    }

    /// @notice Seconds until `claimer` may claim again, or `0` if they may
    ///         claim now. UX helper for clients deciding whether to surface
    ///         a "claim" button as enabled.
    function timeUntilNext(address claimer) external view returns (uint256) {
        uint256 last = lastClaimedAt[claimer];
        if (last == 0) return 0;
        uint256 nextAvailable = last + cooldown;
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp >= nextAvailable) return 0;
        // forge-lint: disable-next-line(block-timestamp)
        return nextAvailable - block.timestamp;
    }

    // -----------------------------------------------------------------
    // Governance
    // -----------------------------------------------------------------

    function setClaimAmount(uint256 newAmount) external onlyRole(GOVERNANCE_ROLE) {
        if (newAmount == 0) revert ZeroAmount();
        uint256 old = claimAmount;
        claimAmount = newAmount;
        emit ClaimAmountSet(old, newAmount);
    }

    function setCooldown(uint256 newCooldown) external onlyRole(GOVERNANCE_ROLE) {
        if (newCooldown > MAX_COOLDOWN) revert CooldownTooLarge({ provided: newCooldown, max: MAX_COOLDOWN });
        uint256 old = cooldown;
        cooldown = newCooldown;
        emit CooldownSet(old, newCooldown);
    }

    /// @notice Sweep `amount` TOKEN from this contract to `to`. Pause-independent
    ///         so governance can drain a paused faucet without unpausing it.
    /// @dev    Modifier order: `onlyRole` runs before `nonReentrant` —
    ///         unauthorized callers revert on the cheap role-check without
    ///         executing the reentrancy-guard's SSTORE (gas saving on the
    ///         revert path; equivalent end state since the SSTORE would
    ///         roll back either way).
    function withdraw(address to, uint256 amount) external onlyRole(GOVERNANCE_ROLE) nonReentrant {
        if (to == address(0)) revert ZeroAddress();
        if (amount == 0) revert ZeroAmount();
        uint256 balance = token.balanceOf(address(this));
        if (balance < amount) revert InsufficientBalance({ balance: balance, requested: amount });
        emit Withdrawn(to, amount);
        token.safeTransfer(to, amount);
    }

    // -----------------------------------------------------------------
    // Pause
    // -----------------------------------------------------------------

    function pause() external onlyRole(PAUSER_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }
}
