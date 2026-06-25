// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Script, console2 } from "forge-std/Script.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

import { BuybackBurnerBalancerV3 } from "../src/BuybackBurnerBalancerV3.sol";
import { FeeRouter } from "../src/FeeRouter.sol";
import { IBalancerV3Router } from "../src/interfaces/IBalancerV3Router.sol";

// This is an operator runbook script whose entire purpose is to print the
// deployed address and the governance calldata to schedule through the
// Timelock — console output is intentional here.
// solhint-disable no-console

/// @title ActivateBuyback — deploy the concrete Balancer V3 BuybackBurner and
///        print the governance calldata that activates the buyback bucket.
/// @notice `DeployProtocol` intentionally leaves the buyback bucket dormant
///         (`FeeRouter.buybackBurner == address(0)`, shares `[9000, 0, 1000]`)
///         because activation is a deliberate, audited, post-deploy event gated
///         on a seeded live pool + private-RPC keeper + a calibrated per-epoch
///         cap (ADR 018 § Activation Criteria 1–7). This script performs the one
///         broadcast step that is safe to run ahead of time — deploying the
///         `BuybackBurnerBalancerV3` with `GOVERNANCE_ROLE`/`DEFAULT_ADMIN_ROLE`
///         held by the Timelock — then PRINTS (does not execute) the two
///         calldata blobs governance schedules through the 48h Timelock:
///           1. `FeeRouter.setSharesAndDestinations([6000, 3000, 1000], …)`
///              (steady-state split, ADR 016 § Deployment Order).
///           2. `BuybackBurnerBalancerV3.setKeeper(keeper)`.
///         The script holds no privileged role and never touches FeeRouter, so
///         it cannot itself activate the bucket — the Timelock proposal does.
///
///         Required env vars:
///           - `TOKEN_ADDRESS`        — protocol TOKEN (ERC20Burnable)
///           - `USDC_ADDRESS`         — settlement token
///           - `BALANCER_ROUTER`      — Balancer V3 Router (swap call target)
///           - `BALANCER_VAULT`       — Balancer V3 Vault (pool reads + registration)
///           - `BALANCER_POOL`        — 80/20 TOKEN/USDC weighted pool
///           - `GOVERNANCE_TIMELOCK`  — Timelock that holds the new burner's roles
///           - `FEE_ROUTER`           — deployed FeeRouter
///           - `BUYBACK_KEEPER`       — keeper EOA/bot to grant KEEPER_ROLE
///
///         Optional env vars (defaults from ADR 018 § Parameter Table):
///           - `PERMIT2_ADDRESS`          (default canonical Permit2, same on every chain)
///           - `MAX_BUYBACK_AMOUNT`        (default 10_000e6 USDC)
///           - `MIN_BUYBACK_AMOUNT`        (default 100e6 USDC)
///           - `SLIPPAGE_BPS`              (default 200)
///           - `EPOCH_CAP_FRACTION_BPS`    (default 1000 = 10%)
///           - `TWAP_MIN_WINDOW_SECS`      (default 1800)
///           - `SUB_SWAP_COUNT`            (default 4)
///           - `SUB_SWAP_MIN_BLOCK_GAP`    (default 10)
contract ActivateBuyback is Script {
    // Steady-state FeeRouter split once buyback is live (ADR 016 § Deployment
    // Order): 60% operator / 30% buyback / 10% treasury.
    uint256 internal constant STEADY_OPERATOR_SHARE = 6000;
    uint256 internal constant STEADY_BUYBACK_SHARE = 3000;
    uint256 internal constant STEADY_TREASURY_SHARE = 1000;

    function run() external returns (BuybackBurnerBalancerV3 burner) {
        address tokenAddr = vm.envAddress("TOKEN_ADDRESS");
        address usdcAddr = vm.envAddress("USDC_ADDRESS");
        address timelock = vm.envAddress("GOVERNANCE_TIMELOCK");
        address feeRouter = vm.envAddress("FEE_ROUTER");
        address keeper = vm.envAddress("BUYBACK_KEEPER");

        BuybackBurnerBalancerV3.Config memory cfg = BuybackBurnerBalancerV3.Config({
            swapRouter_: IBalancerV3Router(vm.envAddress("BALANCER_ROUTER")),
            pool_: vm.envAddress("BALANCER_POOL"),
            vault_: vm.envAddress("BALANCER_VAULT"),
            // Canonical Uniswap Permit2 (same CREATE2 address on every chain); the
            // V3 Router pulls the swap's input USDC through it.
            permit2_: vm.envOr("PERMIT2_ADDRESS", address(0x000000000022D473030F116dDEE9F6B43aC78BA3)),
            subSwapCount_: vm.envOr("SUB_SWAP_COUNT", uint256(4)),
            subSwapMinBlockGap_: vm.envOr("SUB_SWAP_MIN_BLOCK_GAP", uint256(10)),
            twapMinWindow_: vm.envOr("TWAP_MIN_WINDOW_SECS", uint256(1800)),
            maxBuybackAmount_: vm.envOr("MAX_BUYBACK_AMOUNT", uint256(10_000e6)),
            minBuybackAmount_: vm.envOr("MIN_BUYBACK_AMOUNT", uint256(100e6)),
            slippageBps_: vm.envOr("SLIPPAGE_BPS", uint256(200)),
            epochLiquidityCapFraction_: vm.envOr("EPOCH_CAP_FRACTION_BPS", uint256(1000))
        });

        vm.startBroadcast();
        burner = new BuybackBurnerBalancerV3(IERC20(usdcAddr), ERC20Burnable(tokenAddr), timelock, cfg);
        vm.stopBroadcast();

        console2.log("BuybackBurnerBalancerV3 deployed at:", address(burner));
        console2.log("Roles (DEFAULT_ADMIN, GOVERNANCE) held by Timelock:", timelock);
        console2.log("");
        console2.log("== Schedule the following through the 48h Timelock ==");

        uint256[3] memory shares = [STEADY_OPERATOR_SHARE, STEADY_BUYBACK_SHARE, STEADY_TREASURY_SHARE];
        bytes memory activateCalldata = abi.encodeCall(
            FeeRouter.setSharesAndDestinations,
            (shares, FeeRouter.ShareDestinations({ buybackBurner: address(burner), treasury: timelock }))
        );
        console2.log("1) target:", feeRouter);
        console2.log("   FeeRouter.setSharesAndDestinations([6000,3000,1000], {burner, timelock})");
        console2.logBytes(activateCalldata);

        bytes memory keeperCalldata = abi.encodeCall(BuybackBurnerBalancerV3.setKeeper, (keeper));
        console2.log("2) target:", address(burner));
        console2.log("   BuybackBurnerBalancerV3.setKeeper(keeper)");
        console2.logBytes(keeperCalldata);
    }
}
