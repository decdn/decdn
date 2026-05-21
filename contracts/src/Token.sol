// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { ERC20Permit } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Permit.sol";
import { ERC20Votes } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Votes.sol";
import { Nonces } from "@openzeppelin/contracts/utils/Nonces.sol";

/// @title deCDN governance / staking TOKEN
/// @notice Fixed-supply (1B) ERC20 with permit, vote checkpoints, and burn.
///         All custom Solidity is the constructor and two multi-inheritance
///         override stubs — there is no mint path post-genesis, no `Ownable`
///         authority, no upgrade hook. The entire supply is minted to
///         `initialHolder` in the constructor and is the only TOKEN that
///         will ever exist.
/// @dev    Composition per ADR 016 § Contract Inventory:
///           - `ERC20` + `ERC20Burnable`: base + `burn` / `burnFrom`,
///             consumed by the slashing path's 20% burn leg
///             (ADR 026 § Slashing and burn).
///           - `ERC20Permit`: EIP-2612 gasless approvals for ERC-4337 / AA
///             flows (ADR 024).
///           - `ERC20Votes`: per-address vote checkpoints. Governor reads
///             vote weight from `VotingEscrow.balanceOfAt`, not this
///             contract; `ERC20Votes` is retained because ADR 016's
///             inventory lists it and the storage cost is bounded.
///
///         Audit checklist (this contract is intentionally tiny):
///           1. Total supply is exactly `1_000_000_000e18`, set once,
///              never changes (no `_mint` reachable post-construction).
///           2. `initialHolder` receives the full supply atomically.
///           3. The two `_update` / `nonces` overrides forward to `super`
///              and add no logic.
contract Token is ERC20, ERC20Burnable, ERC20Permit, ERC20Votes {
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

    // ---------------------------------------------------------------
    // Multi-inheritance override stubs required by Solidity. Both
    // simply forward to `super`; they add no logic and exist solely to
    // resolve the C3 linearization across ERC20 / ERC20Votes / Nonces.
    // ---------------------------------------------------------------

    /// @dev Forwards to `super` — ERC20Votes hooks vote checkpointing on transfer.
    function _update(address from, address to, uint256 value) internal override(ERC20, ERC20Votes) {
        super._update(from, to, value);
    }

    /// @dev Forwards to `super` — `ERC20Permit` and `Nonces` both expose `nonces`.
    function nonces(address owner) public view override(ERC20Permit, Nonces) returns (uint256) {
        return super.nonces(owner);
    }
}
