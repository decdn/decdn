# ADR 010: Multi-Token Payment Support

**Date:** 2026-03-30
**Status:** Proposed (Post-PoC)

## Context

ADR 003 hardcodes USDC as the payment token for the PoC. This was a deliberate scope reduction. For production, the payment protocol should support multiple tokens — but which tokens are acceptable is a governance decision, not an unconstrained per-node choice. The contract must gate token acceptance to protect against adversarial ERC-20 contracts and garbage tokens.

Two motivating cases:

1. **Network heterogeneity.** A deployment on Arbitrum One may use USDC. A private operator running their own L2 may issue their own token and want to run the same CDN software with their token as the unit of account. A consortium may use DAI to avoid Circle counterparty risk. The protocol should accommodate all of these without code changes.

2. **Censorship resistance.** USDC can be frozen by Circle at the address level or the contract level. Operators who want resilience can configure DAI, LUSD, or any other token. The choice of token is an operational decision, not a protocol constraint.

The EIP-712 `token` field was retained in ADR 003 specifically for this extension. No voucher format changes are needed.

## Decision

The payment protocol is token-agnostic but governed. The `PaymentChannel` contract maintains a governance-managed allowlist of approved ERC-20 token addresses. `openChannel` reverts if the token is not on the allowlist. This is consistent with how governance already controls rate bounds, fees, and staking parameters (ADR 004).

Within the set of allowed tokens, each node independently configures which it accepts; each client selects from the intersection of what it holds and what the target node advertises.

Rate bounds, decimal handling, and gossip advertisements are all keyed by token address.

## What This Adds

### Contract: PaymentChannel

The PoC contract (`StablePaymentChannel`) is renamed `PaymentChannel` in production to reflect that it handles any ERC-20, not only stablecoins. The interface is otherwise structurally the same, with the following changes:

**Token allowlist (governance-managed):**

```solidity
mapping(address => bool) public allowedTokens;
uint256 public allowedTokenCount;

event TokenAdded(address indexed token);
event TokenRemoved(address indexed token);

// Governance-only
function addToken(address token) external onlyGovernance {
    require(token != address(0), "Zero address");
    require(!allowedTokens[token], "Already allowed");
    allowedTokens[token] = true;
    allowedTokenCount++;
    emit TokenAdded(token);
}

function removeToken(address token) external onlyGovernance {
    require(allowedTokens[token], "Not allowed");
    allowedTokens[token] = false;
    allowedTokenCount--;
    emit TokenRemoved(token);
}
```

Governance (admin key for PoC, OpenZeppelin Governor for production) must call `addToken` before any channel can be opened in that token. `removeToken` prevents new channels from being opened in that token; existing open channels remain valid and can still be closed/disputed normally.

**`openChannel` accepts governance-approved ERC-20s:**

```solidity
function openChannel(address provider, address token, uint256 deposit)
    external
    returns (bytes32 channelId);
```

The contract validates the token against the allowlist, then calls `transferFrom`:

```solidity
require(allowedTokens[token], "Token not allowed");
SafeERC20.safeTransferFrom(IERC20(token), msg.sender, address(this), deposit);
```

Tokens with fee-on-transfer or rebase mechanics are unsupported — the contract assumes `deposit` equals the amount actually received. `SafeERC20` is used for all token interactions to handle ERC-20s that return `false` on failure instead of reverting.

The allowlist reduces exposure by letting governance reject tokens with known problematic behaviour (e.g., fee-on-transfer, pausable transfers, obvious reentrancy patterns) before they are used, but it does not by itself prevent reentrancy or other ERC-20-level attacks. The implementation must still use standard on-chain mitigations (`nonReentrant` guards, checks-effects-interactions pattern, `SafeERC20`), and governance should account for proxy/upgradability and admin controls when vetting tokens.

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

// governance-only; token must be on the allowlist
function setRateBounds(address token, uint256 deliveryFloor, uint256 deliveryCeiling) external;
// requires: allowedTokens[token]
```

A zero `RateBounds` entry (the default) means no bounds are enforced for that token — the node's advertised rate is unconstrained. Governance sets bounds only for tokens where protocol-level enforcement is wanted.

**EIP-712 voucher type** is unchanged from ADR 003 — the `token` field already carries the token address:

```solidity
bytes32 constant VOUCHER_TYPEHASH = keccak256(
    "Voucher(bytes32 channelId,uint256 amount,uint256 nonce,address token)"
);
```

### Wire Protocol: cdn/client/v1

A `payment_token` field is added to `StreamRequest` (extending the `{hash, channel_id, byte_offset, timestamp_us}` definition in ADR 005). This tells the serving node which token the client intends to use for this channel:

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

Nodes with no `accepted_tokens` entry default to USDC on their configured chain (backward-compatible with ADR 003 deployments during the coexistence phase — see [Migration from ADR 003](#migration-from-adr-003) for the full migration timeline including legacy contract retirement).

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
| `PaymentChannel` contract | Governance-managed token allowlist, accept approved ERC-20s in `openChannel`, per-token `rateBounds`, token in channel ID | New deployment (not an upgrade of PoC contract) |
| EIP-712 voucher typehash | Already uses `address token` from ADR 003 | No |
| `cdn/client/v1` | Add `payment_token` to `StreamRequest` | No (unknown field ignored by old nodes; `UnsupportedToken` response is new but additive) |
| `incentive` crate | `TokenInfo` struct, `accepted_tokens` config, per-token rate map | No (defaults to USDC if unconfigured) |
| Gossip messages | Add `token_rates` alongside legacy `rate_per_mb` | No (additive field) |
| Probe / stream responses | Add `token` field to signed rate | New signature scope; old signed-rate slashing requires both sides on same version |

## Consequences

**Positive:**

- The protocol works identically on any EVM chain with any governance-approved token. A private operator can run the entire CDN stack with their own token by adding it to the allowlist — zero changes to the core codebase.
- No issuer dependency. Governance can approve tokens with different trust profiles: Circle (USDC), MakerDAO (DAI), or operator-issued tokens on a private chain.
- The EIP-712 voucher format already carries the token address — no signature scheme migration needed.
- Operators advertising multiple tokens give clients the best chance of finding a compatible channel without pre-coordination.
- **Malicious token exposure reduction.** The allowlist lets governance reject known-problematic ERC-20 contracts before they interact with `PaymentChannel` funds. This is a first line of defence; on-chain mitigations (`nonReentrant`, `SafeERC20`, checks-effects-interactions) remain required.
- **Garbage token prevention.** Only governance-approved tokens can be used in channels, eliminating the attack surface of worthless self-issued tokens polluting the network.

**Negative:**

- **Decimal heterogeneity.** Tokens use 0–18 decimals. A node misconfiguring decimals silently misprices deliveries. The `TokenInfo.decimals` field must be validated against the on-chain `IERC20Metadata.decimals()` return value at startup.
- **No protocol-level price normalization.** A node advertising 1 base-unit/MB in USDC (= $0.000001/MB) and 1 base-unit/MB in a low-value token are indistinguishable at the wire level. Clients bear responsibility for evaluating whether a node's accepted token has value.
- **Governance bottleneck.** Adding a new payment token requires a governance action (admin call for PoC, Governor proposal for production). This adds latency for operators who want to use a token not yet approved. Mitigated by the fact that token additions are infrequent and low-risk governance actions.
- **Token removal complexity.** `removeToken` blocks new channels but existing open channels in that token remain valid. The network may carry "sunset" tokens for up to 30 days (channel auto-expiry) after removal.
- **Per-token rate bounds governance burden.** Governance must set meaningful bounds for each token it wants to constrain. An unbounded token (zero `RateBounds` entry) has no floor or ceiling enforced.
- **Slashing cross-token complexity.** ADR 004's slashing schedule is denominated in TOKEN. Converting a slash penalty from the payment token to TOKEN requires a price reference. For PoC, slash penalties remain in TOKEN regardless of payment token; a production implementation may need an oracle or a fixed TOKEN-denominated slash amount.

## Migration from ADR 003

Migration proceeds in three phases. Phase 3 is a **breaking change** for nodes that have not upgraded.

### Phase 1: Coexistence (backward-compatible)

1. Deploy `PaymentChannel` (the production contract) alongside the PoC `StablePaymentChannel`; both coexist. Governance immediately calls `addToken(USDC_ADDRESS)` so USDC is available from deployment
2. Governance calls `addToken` for any additional tokens the network wants to support (e.g., DAI)
3. Nodes add `accepted_tokens` to config; default is USDC (backward-compatible)
4. Clients begin negotiating token in `StreamRequest`; nodes on new software respond with `UnsupportedToken` if the requested token is not accepted, while nodes on old software ignore the `payment_token` field and therefore only operate USDC channels
5. Gossip messages include `token_rates`; old nodes advertise only legacy `rate_per_mb`; new nodes advertise both

### Phase 2: Deprecation

6. Governance announces deprecation of `StablePaymentChannel` — new channels should use `PaymentChannel`
7. Node software emits deprecation warnings when opening channels on the legacy contract

### Phase 3: Retirement (breaking)

8. Governance retires the legacy `StablePaymentChannel` — no new channels can be opened on it. Existing open channels settle normally until expiry (up to 30 days)
9. Nodes that have not upgraded to `PaymentChannel` can no longer participate in new payment channels — they cannot open channels on the new contract, and clients using the new contract cannot open channels with them. **This is a breaking change** — operators must upgrade before Phase 3 takes effect

## Open Questions

- **Decimal validation at runtime.** Should the node fail to start if a configured token's on-chain `decimals()` does not match the configured value, or warn and continue? Failing to start is safer but may cause operational disruption if a proxy token contract is upgraded (rare but possible).
- **Slash denomination.** When a node is slashed for misbehaviour (ADR 004), the penalty is in TOKEN. If the node earned payment in DAI or a custom token, there is no automatic conversion. Either the slash is always in TOKEN (simple, but the node must hold TOKEN to be slashable), or slashing needs a price reference for the payment token. This is an open design question for the staking/slashing contract.
- **Token metadata trust.** `IERC20Metadata` is not mandatory for ERC-20 tokens. Tokens without `decimals()` will cause a revert at startup. Should the contract use a try/catch and default to 18 decimals, or require the operator to always specify decimals explicitly in config?
- **Token removal semantics.** `removeToken` blocks new channel opens but existing channels remain valid until expiry (up to 30 days). Should governance also have the ability to force-close all channels in a removed token (e.g., if the token is discovered to be malicious), or is blocking new channels sufficient?
- **Token vetting criteria.** What due diligence should governance perform before calling `addToken`? At minimum: verify no fee-on-transfer, no rebase mechanics, no pausable transfers that could lock contract funds, and standard `IERC20` compliance. Should this be codified in a checklist or left to governance discretion?
