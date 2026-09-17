// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Script, console2 } from "forge-std/Script.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { GuardedBuybackBurner } from "../src/GuardedBuybackBurner.sol";
import { FeeRouter } from "../src/FeeRouter.sol";
import { BuybackVenueLib } from "./lib/BuybackVenueLib.sol";

// This is an operator runbook script whose entire purpose is to print the
// deployed address and the governance calldata to schedule through the
// Timelock — console output is intentional here.
// solhint-disable no-console

/// @title ActivateBuyback — deploy the Uniswap V3 BuybackBurner and print the
///        governance calldata that activates the buyback bucket.
/// @notice `DeployProtocol` intentionally leaves the buyback bucket dormant
///         (`FeeRouter.buybackBurner == address(0)`, shares `[9000, 0, 1000]`)
///         because activation is a deliberate, audited, post-deploy event gated
///         on a seeded live pool + private-RPC keeper + a calibrated per-epoch
///         cap (ADR 018 § Activation Criteria 1–7). This script performs the one
///         broadcast step that is safe to run ahead of time — deploying the
///         concrete `GuardedBuybackBurner` with `GOVERNANCE_ROLE`/`DEFAULT_ADMIN_ROLE`
///         held by the Timelock — then PRINTS (does not execute) the three
///         calldata blobs governance schedules through the 48h Timelock:
///           1. `FeeRouter.setSharesAndDestinations([6000, 3000, 1000], …)`
///              (steady-state split, ADR 016 § Deployment Order).
///           2. `GuardedBuybackBurner.setKeeper(keeper)`.
///           3. `GuardedBuybackBurner.renounceRole(DEFAULT_ADMIN_ROLE, timelock)`
///              (#2028 — freeze the burner's role table to match every other
///              contract; without it the Timelock keeps a master key on the burner
///              that a captured governance could use to mint a fresh GOVERNANCE_ROLE).
///              `setKeeper` is GOVERNANCE_ROLE-gated and PAUSER_ROLE is
///              GOVERNANCE_ROLE-administered, so both stay reachable after this
///              renounce; schedule it last. Genesis activation via `DeployProtocol`
///              renounces the same admin automatically in its handoff loop.
///         The script holds no privileged role and never touches FeeRouter, so
///         it cannot itself activate the bucket — the Timelock proposal does.
///
///         The burner sits behind the abstract `GuardedBuybackBurner`; this script
///         assumes the pool already exists and is seeded — for the deploy-time
///         genesis convenience that also creates + seeds the pool, see the
///         `ACTIVATE_BUYBACK` flag on `DeployProtocol` (ADR 018 § Deploy-time genesis
///         activation).
///
///         Env vars:
///           - `TOKEN_ADDRESS`        — protocol TOKEN (ERC20Burnable)
///           - `USDC_ADDRESS`         — settlement token
///           - `GOVERNANCE_TIMELOCK`  — Timelock that holds the new burner's roles
///           - `FEE_ROUTER`           — deployed FeeRouter
///           - `BUYBACK_KEEPER`       — keeper EOA/bot to grant KEEPER_ROLE
///           - `UNISWAP_SWAP_ROUTER`  — Uniswap V3 SwapRouter02 (swap call target)
///           - `UNISWAP_POOL`         — seeded TOKEN/USDC V3 pool
///
///         Optional env vars (defaults from ADR 018 § Parameter Table):
///           - `MAX_BUYBACK_AMOUNT`        (default 10_000e6 USDC; must be non-zero)
///           - `MIN_BUYBACK_AMOUNT`        (default 100e6 USDC)
///           - `SLIPPAGE_BPS`              (default 200; ceiling 1000 = 10%)
///           - `EPOCH_CAP_FRACTION_BPS`    (default 1000 = 10%)
///           - `TWAP_MIN_WINDOW_SECS`      (default 1800)
contract ActivateBuyback is Script {
    function run() external returns (GuardedBuybackBurner burner) {
        address tokenAddr = vm.envAddress("TOKEN_ADDRESS");
        address usdcAddr = vm.envAddress("USDC_ADDRESS");
        address timelock = vm.envAddress("GOVERNANCE_TIMELOCK");
        address feeRouter = vm.envAddress("FEE_ROUTER");
        address keeper = vm.envAddress("BUYBACK_KEEPER");

        vm.startBroadcast();
        // Construction goes through `BuybackVenueLib`, the same seam
        // `DeployProtocol`'s genesis path uses, so the two entry points cannot wire
        // different burners from the same inputs.
        burner = BuybackVenueLib.deployUniswapBurner(
            IERC20(usdcAddr),
            ERC20Burnable(tokenAddr),
            timelock,
            timelock,
            vm.envAddress("UNISWAP_SWAP_ROUTER"),
            vm.envAddress("UNISWAP_POOL"),
            _readGuardParams()
        );
        vm.stopBroadcast();

        console2.log("BuybackBurner deployed at:", address(burner));
        console2.log("Roles (DEFAULT_ADMIN, GOVERNANCE) held by Timelock:", timelock);
        console2.log("DEFAULT_ADMIN is renounced by step 3 below (#2028).");
        console2.log("");
        console2.log("== Schedule the following through the 48h Timelock ==");

        bytes memory activateCalldata = abi.encodeCall(
            FeeRouter.setSharesAndDestinations,
            (
                BuybackVenueLib.steadyShares(),
                FeeRouter.ShareDestinations({ buybackBurner: address(burner), treasury: timelock })
            )
        );
        console2.log("1) target:", feeRouter);
        console2.log("   FeeRouter.setSharesAndDestinations([6000,3000,1000], {burner, timelock})");
        console2.logBytes(activateCalldata);

        bytes memory keeperCalldata = abi.encodeCall(GuardedBuybackBurner.setKeeper, (keeper));
        console2.log("2) target:", address(burner));
        console2.log("   GuardedBuybackBurner.setKeeper(keeper)");
        console2.logBytes(keeperCalldata);

        // #2028 — the Timelock renounces its own DEFAULT_ADMIN_ROLE on the burner,
        // freezing the role table so no master key survives on this contract either.
        // OZ `renounceRole(bytes32 role, address callerConfirmation)` requires its
        // second argument to equal `msg.sender`. The Timelock executes this batch,
        // so the argument is the Timelock's own address — it renounces its own role.
        bytes memory renounceCalldata =
            abi.encodeCall(IAccessControl.renounceRole, (burner.DEFAULT_ADMIN_ROLE(), timelock));
        console2.log("3) target:", address(burner));
        console2.log("   GuardedBuybackBurner.renounceRole(DEFAULT_ADMIN_ROLE, timelock)  [#2028, schedule last]");
        console2.logBytes(renounceCalldata);
    }

    /// @dev The MEV-defense guard band. Defaults track ADR 018 § Parameter Table
    ///      and match `DeployProtocol._readBuybackActivation`'s, so the runbook and
    ///      the genesis path configure the same burner.
    function _readGuardParams() internal view returns (GuardedBuybackBurner.GuardParams memory) {
        return GuardedBuybackBurner.GuardParams({
            twapMinWindow_: vm.envOr("TWAP_MIN_WINDOW_SECS", uint256(1800)),
            maxBuybackAmount_: vm.envOr("MAX_BUYBACK_AMOUNT", uint256(10_000e6)),
            minBuybackAmount_: vm.envOr("MIN_BUYBACK_AMOUNT", uint256(100e6)),
            slippageBps_: vm.envOr("SLIPPAGE_BPS", uint256(200)),
            epochLiquidityCapFraction_: vm.envOr("EPOCH_CAP_FRACTION_BPS", uint256(1000))
        });
    }
}
