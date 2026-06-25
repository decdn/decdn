// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IPermit2
/// @notice Minimal Uniswap Permit2 `AllowanceTransfer.approve` surface used by
///         `BuybackBurnerBalancerV3` to authorize the Balancer V3 Router to pull
///         the swap's input USDC.
/// @dev    Balancer V3 routers pull `tokenIn` via Permit2
///         (`permit2.transferFrom(owner, vault, amount, token)`), NOT via a direct
///         ERC20 allowance to the Vault. So the buyback must (1) ERC20-approve
///         Permit2 and (2) grant the Router a scoped Permit2 allowance before the
///         swap. Only `approve` is vendored — the Router itself calls
///         `transferFrom`; the buyback never does. Permit2 is deployed at the same
///         canonical address on every chain (Nick's-method CREATE2).
interface IPermit2 {
    /// @param expiration A `0` value is interpreted by Permit2 as `block.timestamp`
    ///        (i.e. valid only within the current block), which is exactly the
    ///        scope the buyback wants for its single same-transaction swap.
    function approve(address token, address spender, uint160 amount, uint48 expiration) external;
}
