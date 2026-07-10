// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Script, console2 } from "forge-std/Script.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

import { BuybackBurnerBalancerV3 } from "../src/BuybackBurnerBalancerV3.sol";
import { BuybackBurnerUniswapV3 } from "../src/BuybackBurnerUniswapV3.sol";
import { GuardedBuybackBurner } from "../src/GuardedBuybackBurner.sol";
import { FeeRouter } from "../src/FeeRouter.sol";
import { IBalancerV3Router } from "../src/interfaces/IBalancerV3Router.sol";
import { IUniswapV3SwapRouter } from "../src/interfaces/IUniswapV3SwapRouter.sol";

// This is an operator runbook script whose entire purpose is to print the
// deployed address and the governance calldata to schedule through the
// Timelock — console output is intentional here.
// solhint-disable no-console

/// @title ActivateBuyback — deploy a concrete BuybackBurner for the selected venue
///        and print the governance calldata that activates the buyback bucket.
/// @notice `DeployProtocol` intentionally leaves the buyback bucket dormant
///         (`FeeRouter.buybackBurner == address(0)`, shares `[9000, 0, 1000]`)
///         because activation is a deliberate, audited, post-deploy event gated
///         on a seeded live pool + private-RPC keeper + a calibrated per-epoch
///         cap (ADR 018 § Activation Criteria 1–7). This script performs the one
///         broadcast step that is safe to run ahead of time — deploying the
///         concrete `GuardedBuybackBurner` with `GOVERNANCE_ROLE`/`DEFAULT_ADMIN_ROLE`
///         held by the Timelock — then PRINTS (does not execute) the two
///         calldata blobs governance schedules through the 48h Timelock:
///           1. `FeeRouter.setSharesAndDestinations([6000, 3000, 1000], …)`
///              (steady-state split, ADR 016 § Deployment Order).
///           2. `GuardedBuybackBurner.setKeeper(keeper)`.
///         The script holds no privileged role and never touches FeeRouter, so
///         it cannot itself activate the bucket — the Timelock proposal does.
///
///         The venue is selected via `BUYBACK_VENUE` (`balancer` | `uniswap`,
///         default `uniswap` — consistent with `DeployProtocol`; set it explicitly
///         to match the pool actually deployed). The burner is venue-neutral behind
///         `GuardedBuybackBurner`; both concrete subclasses take the same roles
///         and produce the same two calldata blobs. This script assumes the pool
///         already exists and is seeded — for the deploy-time genesis convenience
///         that also creates + seeds the pool, see the `ACTIVATE_BUYBACK` flag on
///         `DeployProtocol` (ADR 018 § Deploy-time genesis activation).
///
///         Shared env vars:
///           - `TOKEN_ADDRESS`        — protocol TOKEN (ERC20Burnable)
///           - `USDC_ADDRESS`         — settlement token
///           - `GOVERNANCE_TIMELOCK`  — Timelock that holds the new burner's roles
///           - `FEE_ROUTER`           — deployed FeeRouter
///           - `BUYBACK_KEEPER`       — keeper EOA/bot to grant KEEPER_ROLE
///
///         Balancer venue env vars:
///           - `BALANCER_ROUTER`      — Balancer V3 Router (swap call target)
///           - `BALANCER_VAULT`       — Balancer V3 Vault (pool reads + registration)
///           - `BALANCER_POOL`        — 80/20 TOKEN/USDC weighted pool
///           - `PERMIT2_ADDRESS`          (optional; default canonical Permit2)
///           - `SUB_SWAP_COUNT`           (optional; default 4)
///           - `SUB_SWAP_MIN_BLOCK_GAP`   (optional; default 10)
///
///         Uniswap venue env vars:
///           - `UNISWAP_SWAP_ROUTER`  — Uniswap V3 SwapRouter02 (swap call target)
///           - `UNISWAP_POOL`         — seeded TOKEN/USDC V3 pool
///
///         Optional shared env vars (defaults from ADR 018 § Parameter Table):
///           - `MAX_BUYBACK_AMOUNT`        (default 10_000e6 USDC)
///           - `MIN_BUYBACK_AMOUNT`        (default 100e6 USDC)
///           - `SLIPPAGE_BPS`              (default 200)
///           - `EPOCH_CAP_FRACTION_BPS`    (default 1000 = 10%)
///           - `TWAP_MIN_WINDOW_SECS`      (default 1800)
contract ActivateBuyback is Script {
    // Steady-state FeeRouter split once buyback is live (ADR 016 § Deployment
    // Order): 60% operator / 30% buyback / 10% treasury.
    uint256 internal constant STEADY_OPERATOR_SHARE = 6000;
    uint256 internal constant STEADY_BUYBACK_SHARE = 3000;
    uint256 internal constant STEADY_TREASURY_SHARE = 1000;

    /// @dev Canonical Uniswap Permit2 (same CREATE2 address on every chain); the
    ///      Balancer V3 Router pulls the swap's input USDC through it.
    address internal constant CANONICAL_PERMIT2 = 0x000000000022D473030F116dDEE9F6B43aC78BA3;

    error UnknownBuybackVenue(string venue);

    function run() external returns (GuardedBuybackBurner burner) {
        address tokenAddr = vm.envAddress("TOKEN_ADDRESS");
        address usdcAddr = vm.envAddress("USDC_ADDRESS");
        address timelock = vm.envAddress("GOVERNANCE_TIMELOCK");
        address feeRouter = vm.envAddress("FEE_ROUTER");
        address keeper = vm.envAddress("BUYBACK_KEEPER");

        string memory venue = vm.envOr("BUYBACK_VENUE", string("uniswap"));
        bytes32 h = keccak256(bytes(venue));

        vm.startBroadcast();
        if (h == keccak256("balancer")) {
            burner = _deployBalancer(IERC20(usdcAddr), ERC20Burnable(tokenAddr), timelock);
        } else if (h == keccak256("uniswap")) {
            burner = _deployUniswap(IERC20(usdcAddr), ERC20Burnable(tokenAddr), timelock);
        } else {
            revert UnknownBuybackVenue(venue);
        }
        vm.stopBroadcast();

        console2.log("Venue:", venue);
        console2.log("BuybackBurner deployed at:", address(burner));
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

        bytes memory keeperCalldata = abi.encodeCall(GuardedBuybackBurner.setKeeper, (keeper));
        console2.log("2) target:", address(burner));
        console2.log("   GuardedBuybackBurner.setKeeper(keeper)");
        console2.logBytes(keeperCalldata);
    }

    function _deployBalancer(IERC20 usdc, ERC20Burnable token, address timelock)
        internal
        returns (GuardedBuybackBurner)
    {
        return new BuybackBurnerBalancerV3(
            usdc,
            token,
            timelock,
            BuybackBurnerBalancerV3.Config({
                swapRouter_: IBalancerV3Router(vm.envAddress("BALANCER_ROUTER")),
                pool_: vm.envAddress("BALANCER_POOL"),
                vault_: vm.envAddress("BALANCER_VAULT"),
                permit2_: vm.envOr("PERMIT2_ADDRESS", CANONICAL_PERMIT2),
                subSwapCount_: vm.envOr("SUB_SWAP_COUNT", uint256(4)),
                subSwapMinBlockGap_: vm.envOr("SUB_SWAP_MIN_BLOCK_GAP", uint256(10)),
                twapMinWindow_: vm.envOr("TWAP_MIN_WINDOW_SECS", uint256(1800)),
                maxBuybackAmount_: vm.envOr("MAX_BUYBACK_AMOUNT", uint256(10_000e6)),
                minBuybackAmount_: vm.envOr("MIN_BUYBACK_AMOUNT", uint256(100e6)),
                slippageBps_: vm.envOr("SLIPPAGE_BPS", uint256(200)),
                epochLiquidityCapFraction_: vm.envOr("EPOCH_CAP_FRACTION_BPS", uint256(1000))
            })
        );
    }

    function _deployUniswap(IERC20 usdc, ERC20Burnable token, address timelock)
        internal
        returns (GuardedBuybackBurner)
    {
        return new BuybackBurnerUniswapV3(
            usdc,
            token,
            timelock,
            BuybackBurnerUniswapV3.Config({
                swapRouter_: IUniswapV3SwapRouter(vm.envAddress("UNISWAP_SWAP_ROUTER")),
                pool_: vm.envAddress("UNISWAP_POOL"),
                twapMinWindow_: vm.envOr("TWAP_MIN_WINDOW_SECS", uint256(1800)),
                maxBuybackAmount_: vm.envOr("MAX_BUYBACK_AMOUNT", uint256(10_000e6)),
                minBuybackAmount_: vm.envOr("MIN_BUYBACK_AMOUNT", uint256(100e6)),
                slippageBps_: vm.envOr("SLIPPAGE_BPS", uint256(200)),
                epochLiquidityCapFraction_: vm.envOr("EPOCH_CAP_FRACTION_BPS", uint256(1000))
            })
        );
    }
}
