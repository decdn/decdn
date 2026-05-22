// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IEd25519Verifier } from "../../src/interfaces/IEd25519Verifier.sol";

/// @notice Test-only stand-in for an ed25519 verifier. Toggle `accept` to
///         control whether `verify` returns true or false. `verify` is `view`
///         (matching the real interface) so tests assert the arguments
///         StakingRegistry passes via `vm.expectCall`, not via stored state.
///         NEVER deployed outside `forge test`.
contract MockEd25519Verifier is IEd25519Verifier {
    bool public accept = true;

    function setAccept(bool ok) external {
        accept = ok;
    }

    function verify(bytes32, bytes32, bytes calldata) external view override returns (bool) {
        return accept;
    }
}
