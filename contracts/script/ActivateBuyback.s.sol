// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Script, console2 } from "forge-std/Script.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

import { GuardedBuybackBurner } from "../src/GuardedBuybackBurner.sol";
import { FeeRouter } from "../src/FeeRouter.sol";
import { BuybackVenueLib } from "./lib/BuybackVenueLib.sol";

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
    function run() external returns (GuardedBuybackBurner burner) {
        address tokenAddr = vm.envAddress("TOKEN_ADDRESS");
        address usdcAddr = vm.envAddress("USDC_ADDRESS");
        address timelock = vm.envAddress("GOVERNANCE_TIMELOCK");
        address feeRouter = vm.envAddress("FEE_ROUTER");
        address keeper = vm.envAddress("BUYBACK_KEEPER");

        // Resolved before `startBroadcast` so an unknown venue aborts with no gas
        // spent. `BuybackVenueLib` is the same dispatch `DeployProtocol`'s genesis
        // path uses, so the two entry points cannot disagree about what "uniswap"
        // means (issue #1090).
        string memory venue = vm.envOr("BUYBACK_VENUE", string("uniswap"));
        BuybackVenueLib.Venue selected = BuybackVenueLib.parseVenue(venue);

        vm.startBroadcast();
        // Explicit else-revert, not a two-way ternary. This script and the genesis
        // path previously fell through to *different* venues, so an unhandled variant
        // would have wired different burners from the same env — the exact drift
        // `BuybackVenueLib` exists to prevent.
        if (selected == BuybackVenueLib.Venue.BALANCER) {
            burner = _deployBalancer(IERC20(usdcAddr), ERC20Burnable(tokenAddr), timelock);
        } else if (selected == BuybackVenueLib.Venue.UNISWAP) {
            burner = _deployUniswap(IERC20(usdcAddr), ERC20Burnable(tokenAddr), timelock);
        } else {
            revert BuybackVenueLib.UnknownVenueVariant(uint8(selected));
        }
        vm.stopBroadcast();

        console2.log("Venue:", venue);
        console2.log("BuybackBurner deployed at:", address(burner));
        console2.log("Roles (DEFAULT_ADMIN, GOVERNANCE) held by Timelock:", timelock);
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
    }

    function _deployBalancer(IERC20 usdc, ERC20Burnable token, address timelock)
        internal
        returns (GuardedBuybackBurner)
    {
        return BuybackVenueLib.deployBalancerBurner(
            usdc,
            token,
            timelock,
            BuybackVenueLib.BalancerWiring({
                swapRouter: vm.envAddress("BALANCER_ROUTER"),
                pool: vm.envAddress("BALANCER_POOL"),
                vault: vm.envAddress("BALANCER_VAULT"),
                permit2: vm.envOr("PERMIT2_ADDRESS", BuybackVenueLib.CANONICAL_PERMIT2),
                subSwapCount: vm.envOr("SUB_SWAP_COUNT", uint256(4)),
                subSwapMinBlockGap: vm.envOr("SUB_SWAP_MIN_BLOCK_GAP", uint256(10))
            }),
            _readGuardParams()
        );
    }

    function _deployUniswap(IERC20 usdc, ERC20Burnable token, address timelock)
        internal
        returns (GuardedBuybackBurner)
    {
        return BuybackVenueLib.deployUniswapBurner(
            usdc,
            token,
            timelock,
            vm.envAddress("UNISWAP_SWAP_ROUTER"),
            vm.envAddress("UNISWAP_POOL"),
            _readGuardParams()
        );
    }

    /// @dev The venue-independent MEV-defense guard band. Defaults track ADR 018
    ///      § Parameter Table and match `DeployProtocol._readBuybackActivation`'s,
    ///      so the runbook and the genesis path configure the same burner.
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
