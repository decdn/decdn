// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Script, console2 } from "forge-std/Script.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { TOKEN } from "../src/TOKEN.sol";
import { StakingRegistry } from "../src/StakingRegistry.sol";
import { StablePaymentChannel } from "../src/StablePaymentChannel.sol";
import { BuybackBurner } from "../src/BuybackBurner.sol";
import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { SlashJudge } from "../src/SlashJudge.sol";
import { IStakingRegistry } from "../src/interfaces/IStakingRegistry.sol";
import { IStablePaymentChannel } from "../src/interfaces/IStablePaymentChannel.sol";
import { Errors } from "../src/libraries/Errors.sol";
import { Roles } from "../src/libraries/Roles.sol";

import { MockUSDC } from "../test/mocks/MockUSDC.sol";

/// @title Deploy — deCDN PoC deployment script
/// @notice Deploys the full PoC contract set per ADR 016 §2 in the mandated
///         order, performs the atomic role wiring, and unpauses the registry.
/// @dev Environment variables (all optional except on live networks):
///      - USDC_ADDRESS       — existing USDC address; when unset, a MockUSDC
///                             is deployed for local/anvil use.
///      - TREASURY_ADDRESS   — treasury recipient for fee payments; defaults
///                             to the broadcasting EOA.
///      - BALANCER_V3_ROUTER — Balancer V3 Router for BuybackBurner. Ignored
///                             in PoC execution (executeBuyback reverts); can
///                             be the zero address on anvil.
///      - INITIAL_SUPPLY     — TOKEN initial supply to the deployer (defaults
///                             to 10M TOKEN — sufficient for PoC testing).
///
/// Post-deploy wiring that this script intentionally omits:
///   1. Grant `GOVERNANCE_ROLE` and `EMERGENCY_ROLE` on ContentBlacklist.
///      Until this happens `addHash`, `emergencyAdd`, `ejectOrigin`, and
///      `SlashJudge.submitBlacklistChallenge` are inert. The deployer EOA
///      holds admin, so this is a one-transaction setup task after deploy.
///
/// Production-only wiring (separate from the above):
///   2. Transfer `DEFAULT_ADMIN_ROLE` from the deployer EOA to the
///      `TimelockController` controlled by the Governor (ADR 009).
///   3. Revoke `DEFAULT_ADMIN_ROLE` from the deployer.
///   4. Register regional governance bodies on ContentBlacklist (ADR 011).
///
/// Production migrations (require follow-up deploys, not wiring changes):
///   5. Deploy `PaymentChannel` (multi-token) and decommission this contract
///      (ADR 010).
///   6. Deploy `WatchtowerEscrow` and enable the `cdn/watchtower/v1` ALPN
///      (ADR 007).
contract Deploy is Script {
    struct Deployment {
        TOKEN token;
        IERC20 usdc;
        StakingRegistry stakingRegistry;
        StablePaymentChannel paymentChannel;
        BuybackBurner buybackBurner;
        ContentBlacklist contentBlacklist;
        SlashJudge slashJudge;
    }

    function run() external returns (Deployment memory d) {
        // Resolve the broadcaster EOA. Priority: PRIVATE_KEY env (derive via
        // vm.addr) → DEPLOYER_ADDRESS env → tx.origin (foundry's default
        // sender under `forge script --sender`). `msg.sender` inside a script
        // points at the script contract, not the broadcaster — do not use it.
        (address deployer, uint256 privateKey, bool hasPrivateKey) = _resolveDeployer();
        address treasury = _envAddressOr("TREASURY_ADDRESS", deployer);
        address router = _envAddressOr("BALANCER_V3_ROUTER", address(0));
        uint256 initialSupply = _envUintOr("INITIAL_SUPPLY", 10_000_000e18);

        if (hasPrivateKey) {
            vm.startBroadcast(privateKey);
        } else {
            vm.startBroadcast(deployer);
        }
        d = _deployAndWire(deployer, treasury, router, initialSupply);
        vm.stopBroadcast();

        _printReceipt(d);
    }

    function _resolveDeployer()
        internal
        view
        returns (address deployer, uint256 privateKey, bool hasPrivateKey)
    {
        // Use vm.envExists to detect presence; a typo'd value surfaces a
        // concrete parse error rather than silently falling through.
        if (vm.envExists("PRIVATE_KEY")) {
            uint256 pk = vm.envUint("PRIVATE_KEY");
            return (vm.addr(pk), pk, true);
        }
        if (vm.envExists("DEPLOYER_ADDRESS")) {
            return (vm.envAddress("DEPLOYER_ADDRESS"), 0, false);
        }
        // Foundry scripts with no explicit broadcaster fall back to
        // `tx.origin` (Foundry's default CLI sender or --sender). This is
        // a script-only code path — `tx.origin` is fine here because the
        // Deploy contract is not part of the production call graph and
        // nothing on-chain trusts it for authentication.
        // solhint-disable-next-line avoid-tx-origin
        return (tx.origin, 0, false);
    }

    /// @notice Programmatic entry point used by the integration test. Same
    /// logic as `run()` but without broadcast so the test contract executes
    /// the transactions itself.
    function deployForTest(
        address deployer,
        address treasury,
        IERC20 usdcOverride,
        uint256 initialSupply
    ) external returns (Deployment memory d) {
        address router = address(0);
        if (address(usdcOverride) != address(0)) {
            return _deployAndWireWithUSDC(deployer, treasury, router, initialSupply, usdcOverride);
        }
        return _deployAndWire(deployer, treasury, router, initialSupply);
    }

    // ---------------------------------------------------------------------
    //  Deployment body
    // ---------------------------------------------------------------------

    function _deployAndWire(
        address deployer,
        address treasury,
        address router,
        uint256 initialSupply
    ) internal returns (Deployment memory d) {
        IERC20 usdc = _resolveUSDC();
        return _deployAndWireWithUSDC(deployer, treasury, router, initialSupply, usdc);
    }

    function _deployAndWireWithUSDC(
        address deployer,
        address treasury,
        address router,
        uint256 initialSupply,
        IERC20 usdc
    ) internal returns (Deployment memory d) {
        d.usdc = usdc;

        // All contracts are first deployed with `address(this)` as the admin
        // so this helper can perform the role-wiring transactions atomically.
        // At the end, admin is handed over to the requested `deployer`.
        address self = address(this);

        d.token = new TOKEN(deployer, initialSupply, self);

        d.stakingRegistry = new StakingRegistry(IERC20(address(d.token)), 1000e18, 7 days, self);

        d.paymentChannel = new StablePaymentChannel(
            d.usdc,
            IStakingRegistry(address(d.stakingRegistry)),
            treasury,
            self,
            300,
            150,
            10,
            48 hours,
            90 days,
            // Initial rate bounds per ADR 003 §Rate Bounds: $0.000001/MB
            // floor (anti-zero safeguard), $0.001/MB ceiling (100x market).
            1,
            1000
        );

        d.buybackBurner = new BuybackBurner(IERC20(address(d.token)), d.usdc, router, self);

        d.contentBlacklist =
            new ContentBlacklist(IStakingRegistry(address(d.stakingRegistry)), self);

        d.slashJudge = new SlashJudge(
            IStakingRegistry(address(d.stakingRegistry)),
            d.contentBlacklist,
            IStablePaymentChannel(address(d.paymentChannel)),
            IERC20(address(d.token)),
            self,
            100e18,
            24 hours
        );

        // Atomic role wiring per ADR 016 §2.
        d.stakingRegistry.grantRole(Roles.BLACKLIST_ROLE, address(d.contentBlacklist));
        d.stakingRegistry.grantRole(Roles.SLASH_ROLE, address(d.slashJudge));
        // PaymentChannel stamps `lastSettlementAt` after every paying settlement
        // so off-chain clients can bias cold-start bootstrap toward proven
        // deliverers (ADR 016 §3 Off-Chain Read API).
        d.stakingRegistry.grantRole(Roles.SETTLEMENT_REPORTER_ROLE, address(d.paymentChannel));
        d.stakingRegistry.unpause();

        // Hand admin over to the requested deployer and renounce from self.
        _transferAdmin(d, deployer);
    }

    function _transferAdmin(
        Deployment memory d,
        address to
    ) internal {
        // Guard against a half-bricked deployment: the first
        // transferOwnership(address(0)) would revert mid-handoff, leaving
        // TOKEN owned by deployer and subsequent contracts still in this
        // script's custody.
        if (to == address(0)) revert Errors.ZeroAddress();
        bytes32 adminRole = 0x00;
        d.token.transferOwnership(to);
        d.paymentChannel.transferOwnership(to);
        d.stakingRegistry.grantRole(adminRole, to);
        d.stakingRegistry.renounceRole(adminRole, address(this));
        d.buybackBurner.grantRole(adminRole, to);
        d.buybackBurner.renounceRole(adminRole, address(this));
        d.contentBlacklist.grantRole(adminRole, to);
        d.contentBlacklist.renounceRole(adminRole, address(this));
        d.slashJudge.grantRole(adminRole, to);
        d.slashJudge.renounceRole(adminRole, address(this));
    }

    // ---------------------------------------------------------------------
    //  Helpers
    // ---------------------------------------------------------------------

    function _resolveUSDC() internal returns (IERC20) {
        if (vm.envExists("USDC_ADDRESS")) {
            return IERC20(vm.envAddress("USDC_ADDRESS"));
        }
        console2.log("USDC_ADDRESS unset; deploying MockUSDC");
        MockUSDC m = new MockUSDC();
        return IERC20(address(m));
    }

    function _envAddressOr(
        string memory key,
        address fallback_
    ) internal view returns (address) {
        if (vm.envExists(key)) return vm.envAddress(key);
        return fallback_;
    }

    function _envUintOr(
        string memory key,
        uint256 fallback_
    ) internal view returns (uint256) {
        if (vm.envExists(key)) return vm.envUint(key);
        return fallback_;
    }

    function _printReceipt(
        Deployment memory d
    ) internal pure {
        console2.log("--- deCDN PoC deployed ---");
        console2.log("TOKEN:                ", address(d.token));
        console2.log("USDC:                 ", address(d.usdc));
        console2.log("StakingRegistry:      ", address(d.stakingRegistry));
        console2.log("StablePaymentChannel: ", address(d.paymentChannel));
        console2.log("BuybackBurner:        ", address(d.buybackBurner));
        console2.log("ContentBlacklist:     ", address(d.contentBlacklist));
        console2.log("SlashJudge:           ", address(d.slashJudge));
    }
}
