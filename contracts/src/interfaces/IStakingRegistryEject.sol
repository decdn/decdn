// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IStakingRegistryEject
/// @notice Minimal surface of `StakingRegistry` consumed by `ContentBlacklist`
///         when an operator address is origin-blacklisted (ADR 011 § Hash
///         Evasion and Origin Blacklisting).
/// @dev    `ContentBlacklist` is granted `BLACKLIST_ROLE` on `StakingRegistry`
///         (ADR 016 § Post-Deployment Initialization) so it may call
///         `ejectNode`. The call is idempotent on the registry side (a
///         second eject of an already-ejected operator is a no-op).
interface IStakingRegistryEject {
    /// @notice Force-eject `operator`: deactivates its node and forces its
    ///         remaining stake into unbonding (still slashable). No-op if the
    ///         operator is already ejected.
    function ejectNode(address operator) external;
}
