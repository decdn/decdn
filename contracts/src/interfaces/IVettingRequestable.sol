// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IVettingRequestable
/// @notice Optional self-service verb, used only by the `decdn` CLI and never by
///         `OriginAssignment`. Every shipped vetting policy implements it so the
///         `publish request-vetting` command is policy-agnostic: a self-service
///         policy starts a request, and a policy with no self-service path
///         reverts `VettingRequestUnsupported` with a caller-facing reason. The
///         error is shared here so the CLI decodes it without knowing which
///         concrete policy is installed.
interface IVettingRequestable {
    /// @notice The current policy grants vetting some other way; `reason`
    ///         explains how (for display to the caller).
    error VettingRequestUnsupported(string reason);

    /// @notice Ask the current policy to vet the caller. Succeeds, no-ops, or
    ///         reverts `VettingRequestUnsupported`, per the policy.
    function requestVetting() external;
}
