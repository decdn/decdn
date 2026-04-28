// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { ERC20Permit } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Permit.sol";
import { Ownable } from "@openzeppelin/contracts/access/Ownable.sol";

import { Errors } from "./libraries/Errors.sol";

/// @title TOKEN
/// @notice deCDN governance and staking token (PoC variant).
/// @dev ADR 004 specifies a 1B fixed supply for production. The PoC variant
///      exposes `mint()` behind `onlyOwner` so testnet operators can provision
///      test balances freely. ADR 023's PoC/production inventory table lists
///      TOKEN minting as PoC-only behavior. When migrating to production,
///      this contract is replaced wholesale with a fixed-supply variant — do
///      not add migration hooks here.
contract TOKEN is ERC20, ERC20Burnable, ERC20Permit, Ownable {
    /// @param initialHolder Address that receives the initial supply.
    /// @param initialSupply Initial supply minted at construction (18 decimals).
    /// @param owner_ Owner authorized to call `mint()` during PoC.
    constructor(
        address initialHolder,
        uint256 initialSupply,
        address owner_
    ) ERC20("deCDN", "DCDN") ERC20Permit("deCDN") Ownable(owner_) {
        if (initialHolder == address(0) || owner_ == address(0)) {
            revert Errors.ZeroAddress();
        }
        if (initialSupply > 0) {
            _mint(initialHolder, initialSupply);
        }
    }

    /// @notice PoC-only: mint additional TOKEN to `to`.
    /// @dev REMOVE IN PRODUCTION. ADR 004 mandates fixed supply for production.
    ///      Zero-address rejection is handled by OZ `_mint` via
    ///      `ERC20InvalidReceiver` — no need to duplicate here.
    function mint(
        address to,
        uint256 amount
    ) external onlyOwner {
        if (amount == 0) revert Errors.ZeroAmount();
        _mint(to, amount);
    }
}
