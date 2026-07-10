// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title Uniswap V3 pool-creation + seeding surface (DEPLOY-SCRIPT-ONLY)
/// @notice Minimal vendored interfaces used by the genesis buyback-activation
///         path in `BaseProtocolDeploy.s.sol` to stand up and seed a TOKEN/USDC
///         Uniswap V3 pool in-script. Uniswap V3 is live on Arbitrum Sepolia (the
///         initial-network testnet), where Balancer V3 is not, so this is the only
///         venue whose pool the genesis flag creates on-chain.
/// @dev    Deliberately NOT under `src/interfaces/` — the deployed contract surface
///         never creates or seeds pools; only the deploy script does. Signatures
///         are vendored verbatim from the Uniswap V3 periphery
///         (`INonfungiblePositionManager`, `IUniswapV3Factory`), reduced to the
///         `createAndInitializePoolIfNecessary` + `mint` seeding path this script
///         needs. The production burner reads the resulting pool through the
///         separate `src/interfaces/IUniswapV3Pool.sol` view surface.
interface IUniswapV3Factory {
    /// @return pool The deployed pool for `{tokenA, tokenB, fee}`, or the zero
    ///         address if none exists yet.
    function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address pool);
}

/// @notice The Uniswap V3 `NonfungiblePositionManager` create + mint surface.
/// @dev    `createAndInitializePoolIfNecessary` deploys the pool via the factory
///         (idempotent) and initializes its price in one call; `mint` seeds the
///         first liquidity position. Both pull `tokenIn` from `msg.sender` via a
///         standard ERC20 allowance, so the caller approves this manager directly
///         for both legs before seeding.
interface INonfungiblePositionManager {
    /// @param token0 Lower-sorted token address (`token0 < token1`).
    /// @param token1 Higher-sorted token address.
    /// @param fee Pool fee tier (e.g. `10000` = 1%).
    /// @param sqrtPriceX96 Initial price as a Q64.96 sqrt of `token1/token0` in
    ///        raw base units.
    function createAndInitializePoolIfNecessary(address token0, address token1, uint24 fee, uint160 sqrtPriceX96)
        external
        payable
        returns (address pool);

    /// @dev `token0`/`token1` MUST be sorted ascending; `amount{0,1}Desired` align
    ///      positionally. `recipient` receives the position NFT (POL custody).
    struct MintParams {
        address token0;
        address token1;
        uint24 fee;
        int24 tickLower;
        int24 tickUpper;
        uint256 amount0Desired;
        uint256 amount1Desired;
        uint256 amount0Min;
        uint256 amount1Min;
        address recipient;
        uint256 deadline;
    }

    function mint(MintParams calldata params)
        external
        payable
        returns (uint256 tokenId, uint128 liquidity, uint256 amount0, uint256 amount1);

    /// @return The Uniswap V3 factory this manager creates pools through.
    function factory() external view returns (address);
}
