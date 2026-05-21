// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Script } from "forge-std/Script.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { TestnetFaucet } from "../testnet/TestnetFaucet.sol";

/// @title DeployTestnetFaucet — testnet-only forge script
/// @notice ⚠️ TESTNET ONLY. Never run against mainnet. This script lives at
///         the path `contracts/script/TestnetFaucet.s.sol`; any future
///         production deploy script MUST be named differently (e.g.
///         `DeployMainnet.s.sol`) and MUST NOT reference `TestnetFaucet`.
///         The CI grep gate in `.github/workflows/ci.yml` enforces this.
///
/// @dev    Required env vars:
///           - `TOKEN_ADDRESS`     — deployed TOKEN contract
///           - `TREASURY_ADDRESS`  — pre-funding source; must equal the
///             forge `--sender` so the same account approves and deploys.
///           - `FUNDING_AMOUNT`    — initial faucet balance in wei
///           - `ADMIN_ADDRESS`     — `DEFAULT_ADMIN_ROLE` holder
///           - `GOVERNANCE_ADDRESS`— `GOVERNANCE_ROLE` holder
///           - `PAUSER_ADDRESS`    — `PAUSER_ROLE` holder
///         Optional env vars (with documented defaults):
///           - `CLAIM_AMOUNT`      — per-claim payout in wei (default `1_000e18`)
///           - `COOLDOWN_SECONDS`  — cooldown per address (default `1 days`)
contract DeployTestnetFaucet is Script {
    uint256 internal constant DEFAULT_CLAIM_AMOUNT = 1000e18;
    uint256 internal constant DEFAULT_COOLDOWN_SECONDS = 1 days;

    error AddressPredictionMismatch(address predicted, address actual);
    error FundingMismatch(uint256 expected, uint256 actual);

    function run() external returns (TestnetFaucet faucet) {
        IERC20 token = IERC20(vm.envAddress("TOKEN_ADDRESS"));
        address treasury = vm.envAddress("TREASURY_ADDRESS");
        uint256 funding = vm.envUint("FUNDING_AMOUNT");
        address admin = vm.envAddress("ADMIN_ADDRESS");
        address governance = vm.envAddress("GOVERNANCE_ADDRESS");
        address pauser = vm.envAddress("PAUSER_ADDRESS");

        uint256 claimAmount = vm.envOr("CLAIM_AMOUNT", DEFAULT_CLAIM_AMOUNT);
        uint256 cooldown = vm.envOr("COOLDOWN_SECONDS", DEFAULT_COOLDOWN_SECONDS);

        // Predict the deployed faucet address so we can approve before the
        // constructor calls `safeTransferFrom`. forge script broadcasts under
        // a single sender (the treasury), so `vm.getNonce(treasury)` is the
        // nonce that will be consumed by `new TestnetFaucet`.
        address predicted = vm.computeCreateAddress(treasury, vm.getNonce(treasury));

        vm.startBroadcast(treasury);
        token.approve(predicted, funding);
        faucet = new TestnetFaucet(token, treasury, funding, claimAmount, cooldown, admin, governance, pauser);
        vm.stopBroadcast();

        if (address(faucet) != predicted) revert AddressPredictionMismatch(predicted, address(faucet));
        uint256 actualBalance = token.balanceOf(address(faucet));
        if (actualBalance != funding) revert FundingMismatch(funding, actualBalance);
    }
}
