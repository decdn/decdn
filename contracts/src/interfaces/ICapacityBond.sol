// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ICapacityBond
/// @notice Read surface of `CapacityBond` consumed by `DecdnGovernor` for
///         vote weight per ADR 036.
/// @dev    The full `CapacityBond` surface (bond / unbond / node registry /
///         region attestation) lives on the concrete contract. This interface
///         declares only the cross-contract read entrypoints other contracts in
///         the system need to know about.
interface ICapacityBond {
    /// @notice Start of the operator's current tenure: the timestamp of the
    ///         entry (a `bond()` that leaves `activeBond` at or above the live
    ///         `minBond` while no tenure runs, or `registerNode`) that began its
    ///         current run of bonding at or above its floor
    ///         (`bondFloorAtActivation`). Later bonds and re-registrations keep
    ///         it. An `unbond()` that leaves the bond below the floor resets it
    ///         to 0, and the next entry stamps a fresh value.
    ///         Source of the `age_ramp` numerator on `DecdnGovernor` per ADR 036
    ///         § Formula. Returns 0 while the operator holds no tenure.
    function firstBondedAt(address operator) external view returns (uint64);

    /// @notice `declaredMbps` in effect at the END of `epoch`
    ///         (`(epoch + 1) × EPOCH_LENGTH − 1`). `DecdnGovernor` caps each
    ///         epoch's vote-weight bytes at this capacity per ADR 036 § Formula
    ///         — declared capacity caps delivery-based weight, it never grants
    ///         weight. Returns 0 when the operator has no declaration on or
    ///         before that instant.
    function declaredMbpsAtEpoch(address operator, uint64 epoch) external view returns (uint256);
}
