// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { ERC20Permit } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Permit.sol";

/// @title deCDN governance / bonding TOKEN
/// @notice Fixed-supply (1B) ERC20 with permit. The constructor mints the
///         entire supply to `initialHolder`, and the total supply stays at
///         that value for the life of the contract.
/// @dev    Composition per ADR 016 § Contract Inventory:
///           - `ERC20`: base.
///           - `ERC20Permit`: EIP-2612 gasless approvals
///             (ADR 016 § Contract Inventory). Available to holders; no
///             protocol contract consumes it today — `CapacityBond.bond`
///             takes a plain `safeTransferFrom` against a prior `approve`.
///
///         `ERC20Votes` is intentionally omitted. Governance vote weight is
///         sourced from `FeeRouter.bytesInWindow` per ADR 036 § Formula —
///         not from raw-TOKEN checkpoints. Inheriting `ERC20Votes` would
///         add ~2× transfer gas and ~10 public functions to the audit
///         perimeter for a checkpoint stream the Governor never reads.
///
///         Audit checklist (this contract is intentionally tiny):
///           1. Total supply is exactly `1_000_000_000e18`, set once,
///              never changes (no `_mint` reachable post-construction).
///           2. `initialHolder` receives the full supply atomically.
contract Token is ERC20, ERC20Permit {
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
