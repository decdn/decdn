// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { ERC20Permit } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Permit.sol";

/// @title deCDN governance / staking TOKEN
/// @notice Fixed-supply (1B) ERC20 with permit and burn. The entire supply is
///         minted to `initialHolder` in the constructor and is the only TOKEN
///         that will ever exist — there is no mint path post-genesis, no
///         `Ownable` authority, and no upgrade hook.
/// @dev    Composition per ADR 016 § Contract Inventory:
///           - `ERC20` + `ERC20Burnable`: base + `burn` / `burnFrom`,
///             consumed by the slashing path's 20% burn leg
///             (ADR 026 § Slashing and burn).
///           - `ERC20Permit`: EIP-2612 gasless approvals. Used by
///             `StakingRegistry.stake` and `VotingEscrow.createLock` so
///             users can sign approval + state-changing call as a single
///             user op (ADR 024 § ERC-4337 path).
///
///         `ERC20Votes` is intentionally omitted. Governance vote weight is
///         sourced from `VotingEscrow.balanceOfAt` per ADR 026 § Voting weight
///         = ve-balance — not from raw-TOKEN checkpoints. Inheriting
///         `ERC20Votes` would add ~2× transfer gas and ~10 public functions
///         to the audit perimeter for a checkpoint stream the Governor never
///         reads.
///
///         Audit checklist (this contract is intentionally tiny):
///           1. Total supply is exactly `1_000_000_000e18`, set once,
///              never changes (no `_mint` reachable post-construction).
///           2. `initialHolder` receives the full supply atomically.
contract Token is ERC20, ERC20Burnable, ERC20Permit {
    /// @notice Fixed total supply: 1,000,000,000 TOKEN at 18 decimals.
    /// @dev    Sized per ADR 026 § Supply and distribution. Hard-coded as a
    ///         constant — not a constructor argument — so the audit guarantee
    ///         "supply is exactly 1B" is enforced at compile time.
    uint256 public constant TOTAL_SUPPLY = 1_000_000_000e18;

    /// @notice Thrown when the constructor is called with `initialHolder == address(0)`.
    /// @dev    A zero recipient would lock the entire supply at the burn address
    ///         and brick the network before launch.
    error ZeroInitialHolder();

    /// @param initialHolder Recipient of the entire 1B fixed supply. Typically
    ///                      the deployer-controlled multisig at testnet, the
    ///                      treasury Timelock at mainnet — see ADR 016 §
    ///                      Deployment Order.
    constructor(address initialHolder) ERC20("deCDN", "DCDN") ERC20Permit("deCDN") {
        if (initialHolder == address(0)) revert ZeroInitialHolder();
        _mint(initialHolder, TOTAL_SUPPLY);
    }
}
