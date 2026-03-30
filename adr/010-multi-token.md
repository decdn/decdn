# ADR 010: Multi-Token Payment Support

**Date:** 2026-03-30
**Status:** Proposed (Post-PoC)

## Context

ADR 003 hardcodes USDC as the payment token for the PoC. This was a deliberate scope reduction. For production, the payment protocol should be token-agnostic: the only requirement is that the token is an ERC-20 contract. What constitutes a "good" payment token — a widely-trusted stablecoin, a network-specific governance token, an operator-issued community coin — is a decision for nodes and their clients, not for the protocol.

Two motivating cases:

1. **Network heterogeneity.** A deployment on Arbitrum One may use USDC. A private operator running their own L2 may issue their own token and want to run the same CDN software with their token as the unit of account. A consortium may use DAI to avoid Circle counterparty risk. The protocol should accommodate all of these without code changes.

2. **Censorship resistance.** USDC can be frozen by Circle at the address level or the contract level. Operators who want resilience can configure DAI, LUSD, or any other token. The choice of token is an operational decision, not a protocol constraint.

The EIP-712 `token` field was retained in ADR 003 specifically for this extension. No voucher format changes are needed.

## Decision

The payment protocol is token-agnostic. Any ERC-20 address is valid as a payment token. There is no protocol-level allowlist. Each node independently configures which token addresses it accepts; each client selects from the intersection of what it holds and what the target node advertises.

Rate bounds, decimal handling, and gossip advertisements are all keyed by token address.

## What This Adds

### Contract: PaymentChannel

The PoC contract (`StablePaymentChannel`) is renamed `PaymentChannel` in production to reflect that it handles any ERC-20, not only stablecoins. The interface is otherwise structurally the same, with the following changes:

**`openChannel` accepts any ERC-20:**

```solidity
function openChannel(address provider, address token, uint256 deposit)
    external
    returns (bytes32 channelId);
```

The contract calls `IERC20(token).transferFrom(msg.sender, address(this), deposit)`. No allowlist check. Any token that implements `IERC20` is accepted. Tokens with fee-on-transfer or rebase mechanics are unsupported — the contract assumes `deposit` equals the amount actually received.

**Channel ID** incorporates the token address to allow the same client-provider pair to hold concurrent channels in different tokens:

```solidity
channelId = keccak256(abi.encodePacked(client, provider, token, nonce));
```

**Channel struct:**

```solidity
struct Channel {
    address client;
    address provider;
    address token;            // any ERC-20; set at open time, immutable
    uint256 deposit;          // in token's own base units
    uint256 claimedAmount;
    uint256 openedAt;
    uint256 expiresAt;
    uint8   status;
    uint256 disputeDeadline;
}
```

**Per-token rate bounds.** The governance-set floor and ceiling are per-token address. This allows sensible bounds in a token's own units regardless of its decimal count or value:

```solidity
struct RateBounds {
    uint256 deliveryFloor;    // min rate in token base units per MB
    uint256 deliveryCeiling;  // max rate in token base units per MB
}

mapping(address => RateBounds) public rateBounds;

// governance-only
function setRateBounds(address token, uint256 deliveryFloor, uint256 deliveryCeiling) external;
```

A zero `RateBounds` entry (the default) means no bounds are enforced for that token — the node's advertised rate is unconstrained. Governance sets bounds only for tokens where protocol-level enforcement is wanted.

**EIP-712 voucher type** is unchanged from ADR 003 — the `token` field already carries the token address:

```solidity
bytes32 constant VOUCHER_TYPEHASH = keccak256(
    "Voucher(bytes32 channelId,uint256 amount,uint256 nonce,address token)"
);
```

### Wire Protocol: cdn/client/v1

A `payment_token` field is added to `StreamRequest`. This tells the serving node which token the client intends to use for this channel:

```rust
struct StreamRequest {
    // ... existing fields ...
    channel_id: ChannelId,
    payment_token: Address,   // ERC-20 address; must match channel.token on-chain
}
```

The serving node validates that `payment_token` matches the on-chain channel's token before accepting the stream. If the node does not accept that token, it responds with `UnsupportedToken { accepted: Vec<Address> }`, giving the client the list of tokens the node accepts so it can retry with a compatible channel.

### Rust: incentive Crate

**Token descriptor** — replaces the PoC assumption of USDC:

```rust
struct TokenInfo {
    address: Address,
    decimals: u8,
    symbol: String,    // for display only; not used in signing
}
```

Nodes resolve `decimals` once at startup by calling `IERC20Metadata.decimals()` on each configured token address. The result is cached; it never changes for a given token contract.

**Node configuration:**

```toml
[[payment.accepted_tokens]]
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"   # USDC on Ethereum
decimals = 6

[[payment.accepted_tokens]]
address = "0x6B175474E89094C44Da98b954EedeAC495271d0F"   # DAI
decimals = 18

# A private deployment might have:
# [[payment.accepted_tokens]]
# address = "0xYourOwnToken"
# decimals = 18
```

Nodes with no `accepted_tokens` entry default to USDC on their configured chain (backward-compatible with ADR 003 deployments).

**Amount handling:** All internal arithmetic uses raw base units. Display formatting divides by `10^decimals`. No conversion happens in the voucher signing path.

### Gossip: Rate Advertisement

Gossip `CacheAnnounce` messages advertise rates as a list of `(token_address, rate_per_mb)` pairs:

```rust
// Before (ADR 003)
rate_per_mb: u64,

// After
token_rates: Vec<(Address, u64)>,   // rate in that token's base units per MB
```

Old nodes that do not understand `token_rates` ignore it and fall back to the legacy `rate_per_mb` field (still present as `Option<u64>` for backward compat). New nodes include both fields if they accept USDC, so old clients can still read their rate.

### Probe and Stream Responses

`ProbeResponse` and `StreamResponse` similarly carry per-token rates. The signed fields include `token_address` alongside `rate_per_mb` so the rate bait-and-switch protection (ADR 003 / ADR 005) extends to all tokens:

```rust
struct SignedRate {
    token: Address,
    rate_per_mb: u64,
    timestamp_us: u64,
}
```

## Scope of Changes

| Layer | Change | Breaking? |
|-------|--------|-----------|
| `PaymentChannel` contract | Accept any ERC-20 in `openChannel`, per-token `rateBounds`, token in channel ID | New deployment (not an upgrade of PoC contract) |
| EIP-712 voucher typehash | Already uses `address token` from ADR 003 | No |
| `cdn/client/v1` | Add `payment_token` to `StreamRequest` | No (unknown field ignored by old nodes; `UnsupportedToken` response is new but additive) |
| `incentive` crate | `TokenInfo` struct, `accepted_tokens` config, per-token rate map | No (defaults to USDC if unconfigured) |
| Gossip messages | Add `token_rates` alongside legacy `rate_per_mb` | No (additive field) |
| Probe / stream responses | Add `token` field to signed rate | New signature scope; old signed-rate slashing requires both sides on same version |

## Consequences

**Positive:**

- The protocol works identically on any EVM chain with any token. A private operator can run the entire CDN stack with their own token and zero changes to the core codebase.
- No issuer dependency. Nodes choose their own risk profile: trust Circle (USDC), trust MakerDAO (DAI), or trust no one (self-issued token on a private chain).
- The EIP-712 voucher format already carries the token address — no signature scheme migration needed.
- Operators advertising multiple tokens give clients the best chance of finding a compatible channel without pre-coordination.

**Negative:**

- **Decimal heterogeneity.** Tokens use 0–18 decimals. A node misconfiguring decimals silently misprices deliveries. The `TokenInfo.decimals` field must be validated against the on-chain `IERC20Metadata.decimals()` return value at startup.
- **No protocol-level price normalization.** A node advertising 1 base-unit/MB in USDC (= $0.000001/MB) and 1 base-unit/MB in a worthless token are indistinguishable at the wire level. Clients bear responsibility for evaluating whether a node's accepted token has value.
- **Garbage token griefing.** A node could accept a zero-value self-issued token, collect "payment" in it, and provide no real revenue. This only harms the node itself — it starves itself of real revenue. Not a protocol-level attack.
- **Per-token rate bounds governance burden.** Governance must set meaningful bounds for each token it wants to constrain. An unbounded token (zero `RateBounds` entry) has no floor or ceiling enforced.
- **Slashing cross-token complexity.** ADR 004's slashing schedule is denominated in TOKEN. Converting a slash penalty from the payment token to TOKEN requires a price reference. For PoC, slash penalties remain in TOKEN regardless of payment token; a production implementation may need an oracle or a fixed TOKEN-denominated slash amount.

## Migration from ADR 003

1. Deploy `PaymentChannel` (the production contract) alongside the PoC `StablePaymentChannel`; both coexist
2. Nodes add `accepted_tokens` to config; default is USDC (backward-compatible)
3. Clients begin negotiating token in `StreamRequest`; nodes on old software respond with `UnsupportedToken` and the client falls back to USDC
4. Gossip messages include `token_rates`; old nodes advertise only legacy `rate_per_mb`; new nodes advertise both
5. When the network has migrated sufficiently, the legacy `StablePaymentChannel` is retired by governance

## Open Questions

- **Decimal validation at runtime.** Should the node fail to start if a configured token's on-chain `decimals()` does not match the configured value, or warn and continue? Failing to start is safer but may cause operational disruption if a proxy token contract is upgraded (rare but possible).
- **Slash denomination.** When a node is slashed for misbehaviour (ADR 004), the penalty is in TOKEN. If the node earned payment in DAI or a custom token, there is no automatic conversion. Either the slash is always in TOKEN (simple, but the node must hold TOKEN to be slashable), or slashing needs a price reference for the payment token. This is an open design question for the staking/slashing contract.
- **Token metadata trust.** `IERC20Metadata` is not mandatory for ERC-20 tokens. Tokens without `decimals()` will cause a revert at startup. Should the contract use a try/catch and default to 18 decimals, or require the operator to always specify decimals explicitly in config?
