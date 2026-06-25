// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

/// @title Balancer V3 pool-creation + initialization surface (TEST-ONLY)
/// @notice Minimal vendored interfaces used by `BuybackBurnerBalancerV3.swapburn.fork.t.sol`
///         to stand up a real 80/20 TOKEN/USDC weighted pool on a live Balancer
///         V3 fork (Ethereum Sepolia) so the swap+burn path can run against real
///         weighted-pool math (issue #995).
/// @dev    Deliberately NOT placed under `src/interfaces/` — production code
///         never creates or seeds pools, so these signatures must not leak into
///         the deployed surface. Signatures are vendored verbatim from
///         balancer/balancer-v3-monorepo (`VaultTypes.sol`, `IRouter.sol`,
///         `WeightedPoolFactory.sol`) and Uniswap `permit2` (`IAllowanceTransfer`).

/// @notice `WeightedPoolFactory.create` + the `VaultTypes` structs it consumes.
interface IBalancerV3WeightedPoolFactory {
    /// @dev `STANDARD` legs carry no rate provider (`rateProvider == address(0)`).
    enum TokenType {
        STANDARD,
        WITH_RATE
    }

    /// @dev `rateProvider` is `IRateProvider` on-chain; ABI-encoded as `address`.
    struct TokenConfig {
        IERC20 token;
        TokenType tokenType;
        address rateProvider;
        bool paysYieldFees;
    }

    struct PoolRoleAccounts {
        address pauseManager;
        address swapFeeManager;
        address poolCreator;
    }

    /// @dev `tokens` MUST be sorted ascending by token address — the Vault's
    ///      `registerPool` reverts (`TokensNotSorted`) otherwise; `normalizedWeights`
    ///      align positionally with `tokens`. The factory registers the pool with
    ///      the Vault, so the returned pool is immediately `isPoolRegistered`.
    function create(
        string memory name,
        string memory symbol,
        TokenConfig[] memory tokens,
        uint256[] memory normalizedWeights,
        PoolRoleAccounts memory roleAccounts,
        uint256 swapFeePercentage,
        address poolHooksContract,
        bool enableDonation,
        bool disableUnbalancedLiquidity,
        bytes32 salt
    ) external returns (address pool);
}

/// @notice The V3 Router's pool-initialization entrypoint (seeds first liquidity).
/// @dev    The Router pulls `tokens` from the caller via Permit2, so the caller
///         must (1) ERC20-approve Permit2 and (2) set a Permit2 allowance for the
///         Router on each token before calling `initialize`.
interface IBalancerV3RouterInit {
    function initialize(
        address pool,
        IERC20[] memory tokens,
        uint256[] memory exactAmountsIn,
        uint256 minBptAmountOut,
        bool wethIsEth,
        bytes memory userData
    ) external payable returns (uint256 bptAmountOut);
}

/// @notice Uniswap Permit2 `AllowanceTransfer.approve` — the V3 Router's token-pull
///         authorization primitive.
interface IPermit2 {
    function approve(address token, address spender, uint160 amount, uint48 expiration) external;
}
