# deCDN Contracts (PoC)

Solidity contracts for the deCDN PoC on Arbitrum Sepolia. See [ADR 016](../adr/016-contract-interactions.md) for the canonical contract interaction spec.

## Scope

PoC contracts (per [ADR 023](../adr/023-poc-production-seams.md) seam inventory):

| Contract | Purpose |
|----------|---------|
| `TOKEN` | ERC20 + ERC20Permit governance/staking token (mintable in PoC) |
| `StakingRegistry` | Node/client stake custody, slashing, ejection, binding signatures |
| `StablePaymentChannel` | USDC payment channels with EIP-712 vouchers |
| `ContentBlacklist` | Hash blacklist and origin ejection |
| `SlashJudge` | Four-way challenge arbitration and slash execution |
| `BuybackBurner` | USDC fee accumulation (execution disabled in PoC) |

All contracts inherit from audited OpenZeppelin v5 bases. No proxy patterns.

## Toolchain

- [Foundry](https://book.getfoundry.sh/) (`forge`, `cast`, `anvil`)
- Solidity 0.8.28, `via_ir = true`, `optimizer_runs = 1_000_000`, Cancun EVM

## Build / Test

```bash
forge build
forge test -vvv
forge coverage
forge fmt --check
```

## Deploy

```bash
# Local anvil (deploys MockUSDC)
anvil &
forge script script/Deploy.s.sol --rpc-url http://localhost:8545 --broadcast

# Arbitrum Sepolia
USDC_ADDRESS=0x... TREASURY_ADDRESS=0x... \
  forge script script/Deploy.s.sol \
    --rpc-url $ARB_SEPOLIA_RPC \
    --broadcast --verify \
    --etherscan-api-key $ARBISCAN_KEY
```

## Layout

```
src/            — contract sources
  interfaces/   — cross-contract ABIs
  libraries/    — Roles, Errors
script/         — deployment scripts
test/           — forge tests
  integration/  — end-to-end flow tests
  mocks/        — MockUSDC and helpers
lib/            — git submodules (openzeppelin-contracts, forge-std)
```
