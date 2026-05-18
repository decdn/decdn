# ADR 010: Multi-Token Payment Support

**Date:** 2026-03-30
**Status:** Proposed (Post-PoC)

## Context

ADR 003 hardcodes USDC as the payment token for the PoC (deliberate scope reduction). Production should support multiple tokens — but which tokens are acceptable is a governance decision, not an unconstrained per-node choice. The contract must gate token acceptance to protect against adversarial ERC-20 contracts and garbage tokens.

Two motivating cases:

1. **Network heterogeneity.** Arbitrum One may use USDC; a private operator on their own L2 may issue their own token; a consortium may use DAI to avoid Circle counterparty risk. The protocol should accommodate all without code changes.

2. **Censorship resistance.** USDC can be frozen by Circle at the address or contract level. Operators wanting resilience can configure DAI, LUSD, or any other token. Token choice is an operational decision, not a protocol constraint.

The EIP-712 `token` field was retained in ADR 003 specifically for this extension. No voucher format changes are needed.

## Decision

The payment protocol is token-agnostic but governed. The `PaymentChannel` contract maintains a governance-managed allowlist of approved ERC-20 token addresses. `openChannel` reverts if the token is not on the allowlist. This is consistent with how governance already controls rate bounds, fees, and staking parameters ([ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds)).

Within the set of allowed tokens, each node independently configures which it accepts; each client selects from the intersection of what it holds and what the target node advertises.

Rate bounds, decimal handling, and gossip advertisements are all keyed by token address.

## What This Adds

### Contract: PaymentChannel

The production contract is named `PaymentChannel` (replacing the PoC's `StablePaymentChannel` from [ADR 003](003-payments.md)) because it handles any governance-approved ERC-20, not only stablecoins. This is a new deployment, not a rename — the PoC `StablePaymentChannel` is decommissioned (see [Migration from ADR 003](#migration-from-adr-003)). The interface is otherwise structurally the same, with the following changes:

**Token allowlist (governance-managed):**

```solidity
mapping(address => bool) public allowedTokens;
uint256 public allowedTokenCount;

event TokenAdded(address indexed token);
event TokenRemoved(address indexed token);

// Governance-only; rate bounds are mandatory to prevent zero-rate free-riding (ADR 009)
function addToken(address token, uint256 deliveryFloor, uint256 deliveryCeiling) external onlyGovernance {
    require(token != address(0), "Zero address");
    require(!allowedTokens[token], "Already allowed");
    require(deliveryFloor >= 1, "Floor must be >= 1 base unit");
    require(deliveryCeiling > deliveryFloor, "Ceiling must exceed floor");
    allowedTokens[token] = true;
    allowedTokenCount++;
    rateBounds[token] = RateBounds(deliveryFloor, deliveryCeiling);
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

#### Force-close channels in removed tokens

Once a token is removed, any address can force-close open channels in that token via `forceCloseChannel`. This avoids on-chain enumeration of channels per token — callers (governance bots, channel parties, third-party fraud detectors) provide the channel ID; the contract verifies the channel exists, then checks `!allowedTokens[channel.token]`:

```solidity
function forceCloseChannel(bytes32 channelId) external {
    Channel storage ch = channels[channelId];
    require(ch.openedAt != 0, "Channel does not exist"); // essential: Status.Open is the zero default, so status alone cannot distinguish non-existent from open
    require(ch.status == Status.Open, "Not open");
    require(!allowedTokens[ch.token], "Token still allowed");

    ch.status = Status.Closing;
    ch.claimedAmount = 0;
    ch.claimedNonce = 0;
    ch.disputeDeadline = block.timestamp + disputeWindow;

    emit ChannelForceClosedByTokenRemoval(channelId, ch.token, msg.sender, ch.disputeDeadline);
}

event ChannelForceClosedByTokenRemoval(
    bytes32 indexed channelId,
    address indexed token,
    address indexed caller,
    uint256 disputeDeadline
);
```

The force-close sets `claimedAmount = 0` and `claimedNonce = 0` (no voucher submitted, matching the zero-voucher close semantics in [ADR 003](003-payments.md)) and enters the standard Closing→dispute→settle flow. If the provider holds a valid voucher, they call `disputeChannel` during the dispute window to claim earned fees — any real voucher (nonce >= 1) satisfies the strictly-higher-nonce requirement against `claimedNonce = 0`. If nobody disputes, `settleChannel` returns the full deposit to the client. Providers get the same dispute opportunity as a normal close.

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

Tokens with fee-on-transfer or rebase mechanics are unsupported — the contract assumes `deposit` equals the amount actually received. `SafeERC20` is used for all token interactions because some widely-deployed ERC-20s (notably USDT) do not return a `bool` on `transfer`/`approve`, causing a raw `IERC20.transfer()` call to revert on missing return data; `SafeERC20` handles returning and non-returning tokens uniformly. This is separate from fee-on-transfer rejection, enforced by the governance allowlist vetting process.

The allowlist lets governance reject tokens with known problematic behavior (fee-on-transfer, pausable transfers, obvious reentrancy patterns) before use, but does not by itself prevent reentrancy or other ERC-20-level attacks. The implementation must still use standard on-chain mitigations (`nonReentrant` guards, checks-effects-interactions, `SafeERC20`), and governance should account for proxy/upgradability and admin controls when vetting tokens.

**Channel ID** incorporates the token address to allow the same client-provider pair to hold concurrent channels in different tokens:

```solidity
channelId = keccak256(abi.encodePacked(client, provider, token, channelNonce));
```

**Breaking change from ADR 003:** The PoC channel ID formula is `keccak256(client, provider, channelNonce)` (see [ADR 003](003-payments.md)). This production formula adds `token` to support concurrent channels in different tokens between the same client-provider pair. New contract deployment, not an upgrade — see [Migration from ADR 003](#migration-from-adr-003) below.

**Channel struct:**

```solidity
struct Channel {
    address client;
    address provider;
    address token;            // any ERC-20; set at open time, immutable
    uint256 deposit;          // in token's own base units
    uint256 claimedAmount;    // cumulative amount claimed via vouchers
    uint256 claimedNonce;     // nonce of the current best voucher, for dispute comparison
    uint256 openedAt;
    uint256 expiresAt;
    uint8   status;           // 0 = Open, 1 = Closing (dispute window active), 2 = Closed (settled)
    uint256 disputeDeadline;  // set when close is initiated; may be extended once via the forced-inclusion path ([ADR 003 § L2 sequencer censorship](003-payments.md#l2-sequencer-censorship))
    address lastDisputor;     // msg.sender of the most recent disputeChannel call
    bool    extended;         // true if disputeDeadline has been extended once via the forced-inclusion path; reset to false on closeChannel
}
```

#### Per-token rate bounds

The governance-set floor and ceiling are per-token address, allowing sensible bounds in a token's own units regardless of decimal count or value:

```solidity
struct RateBounds {
    uint256 deliveryFloor;    // min rate in token base units per MB
    uint256 deliveryCeiling;  // max rate in token base units per MB
}

mapping(address => RateBounds) public rateBounds;

// governance-only; token must be on the allowlist
function setRateBounds(address token, uint256 deliveryFloor, uint256 deliveryCeiling) external;
// requires: allowedTokens[token], deliveryFloor >= 1, deliveryCeiling > deliveryFloor
```

`addToken` requires `deliveryFloor` and `deliveryCeiling` parameters, so every allowed token has rate bounds from the moment it is added — there is no window where a token is allowed but unconstrained. `setRateBounds` can adjust bounds afterward, but the floor can never be set below 1 base unit, consistent with ADR 009's safety bound (`Floor ≥ 1 base unit`) that prevents zero-rate free-riding.

**EIP-712 voucher type** is unchanged from ADR 003 — the `token` field already carries the token address:

```solidity
bytes32 constant VOUCHER_TYPEHASH = keccak256(
    "Voucher(bytes32 channelId,uint256 amount,uint256 nonce,address token)"
);
```

### Wire Protocol: cdn/client/v1

A `payment_token` field is added to `StreamRequest` (extending the `{hash, channel_id, byte_offset, timestamp_us}` definition in ADR 005), telling the serving node which token the client intends to use for this channel:

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

At startup, the node calls `IERC20Metadata.decimals()` on each configured token address and compares against the config `decimals` field. **A mismatch is a fatal startup error** — the node refuses to start, because a misconfigured `decimals` silently misprices deliveries by orders of magnitude (see [Consequences § Decimal heterogeneity](#consequences) below). The on-chain value is cached for the process lifetime; it never changes for a given token contract.

Allowlisted tokens MUST implement `IERC20Metadata.decimals()` per the [Token Vetting Checklist](#token-vetting-checklist) below, so this cross-check always has an authoritative on-chain source. A token that does not expose `decimals()` cannot pass vetting and cannot be added to the allowlist; the config `decimals` field is mandatory and authoritative.

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

Nodes with no `accepted_tokens` entry default to USDC on their configured chain.

**Amount handling:** All internal arithmetic uses raw base units. Display formatting divides by `10^decimals`. No conversion happens in the voucher signing path.

### Gossip: Rate Advertisement

`ProbeResponse` messages advertise rates as a list of `(token_address, rate_per_mb)` pairs:

```rust
// Before (ADR 003)
rate_per_mb: u64,

// After
token_rates: Vec<(Address, u64)>,   // rate in that token's base units per MB
```

Nodes that accept only USDC may also include the `rate_per_mb: Option<u64>` field for simplicity; nodes accepting multiple tokens use `token_rates` exclusively.

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
| `cdn/client/v1` | Add `payment_token` to `StreamRequest` | No — added before first implementation. Nodes that do not accept the requested token respond with `UnsupportedToken { accepted: Vec<Address> }`. |
| `incentive` crate | `TokenInfo` struct, `accepted_tokens` config, per-token rate map | No (defaults to USDC if unconfigured) |
| Gossip messages | Add `token_rates` alongside `rate_per_mb` | No (additive field) |
| Probe / stream responses | Add `token` field to signed rate | New signature scope; old signed-rate slashing requires both sides on same version |

## Consequences

### Positive

- Works identically on any EVM chain with any governance-approved token. A private operator can run the entire CDN stack with their own token by adding it to the allowlist — zero core codebase changes.
- No issuer dependency. Governance can approve tokens with different trust profiles: Circle (USDC), MakerDAO (DAI), or operator-issued tokens on a private chain.
- The EIP-712 voucher format already carries the token address — no signature scheme migration needed.
- Operators advertising multiple tokens give clients the best chance of finding a compatible channel without pre-coordination.
- **Malicious token exposure reduction.** The allowlist lets governance reject known-problematic ERC-20 contracts before they touch `PaymentChannel` funds. First line of defense; on-chain mitigations (`nonReentrant`, `SafeERC20`, checks-effects-interactions) remain required.
- **Garbage token prevention.** Only governance-approved tokens can be used in channels, eliminating the attack surface of worthless self-issued tokens polluting the network.

### Negative

- **Decimal heterogeneity.** Tokens use 0–18 decimals; a node misconfiguring decimals silently misprices deliveries. The `TokenInfo.decimals` field must be validated against the on-chain `IERC20Metadata.decimals()` return value at startup.
- **No protocol-level price normalization.** A node advertising 1 base-unit/MB in USDC (= $0.000001/MB) and 1 base-unit/MB in a low-value token are indistinguishable at the wire level. Clients bear responsibility for evaluating whether a node's accepted token has value.
- **Governance bottleneck.** Adding a new payment token requires a governance action (admin call for PoC, Governor proposal for production), adding latency for operators wanting a not-yet-approved token. Mitigated: token additions are infrequent, low-risk.
- **Token removal complexity.** `removeToken` blocks new channels but existing open channels in that token remain valid until force-closed or expired. `forceCloseChannel` (see contract interface above) lets any address close these immediately, bounding the effective sunset to the dispute window (48h default) rather than `maxChannelDuration` (90 days). Because the contract provides no on-chain channel enumeration, callers must maintain an off-chain inventory of channel IDs (persisted from channel creation) to identify channels to force-close after a token is removed.
- **Per-token rate bounds governance burden.** Governance must set meaningful bounds for each token at `addToken` time. Bounds can be adjusted later via `setRateBounds`, but the floor can never drop below 1 base unit.
- **Slashing is always in TOKEN (resolved).** The slashing schedule per [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn) is denominated in TOKEN stake, unchanged with multi-token payments. Slashing operates on the `StakingRegistry` (TOKEN stake), not on payment channel deposits (which may be in any approved token). A node paid exclusively in DAI is still slashed in TOKEN — it must hold TOKEN stake to participate regardless of which payment tokens it accepts. No price oracle or cross-token conversion is needed. The slash amount is a percentage of TOKEN stake, not of delivery revenue.

## Token Vetting Checklist

`addToken` is the sole entry path for a new ERC-20 onto the payment-channel allowlist. The contract enforces only the structural invariants (non-zero address, rate-bounds shape); ERC-20 behavioural safety is a governance responsibility, gated by the checklist below.

Items 1–4 are **functional safety requirements**: a token failing any breaks `PaymentChannel` accounting and MUST NOT be approved. Items 5–7 are **trust-model disclosures**: governance retains discretion (a regulated-stablecoin pausable token is acceptable; an anonymous pausable token is not), but the disclosure itself is mandatory.

A governance proposal calling `addToken` MUST include the checklist results in the proposal body, with verification evidence (transaction hashes, block explorer links, or simulation traces) for items 1–4.

1. **Standard `IERC20` + `IERC20Metadata` compliance.** `transfer`, `transferFrom`, `approve`, `balanceOf`, `allowance`, `totalSupply`, and `decimals` are all implemented and behave per [EIP-20](https://eips.ethereum.org/EIPS/eip-20). `decimals()` is mandatory — the node's startup validation (see [Rust: incentive Crate](#rust-incentive-crate) above) treats its absence as a fatal error.
2. **No fee-on-transfer.** The amount credited to the recipient equals the amount debited from the sender for all transfer values. The contract assumes `deposit` equals the amount actually received (see [`openChannel` discussion](#what-this-adds) above); fee-on-transfer tokens cause silent loss of funds on every channel open. Verified by simulating `openChannel` against the candidate token and confirming `IERC20.balanceOf(channel)` increases by exactly `deposit`.
3. **No rebase mechanics.** Balances do not change without an explicit `transfer` / `transferFrom` / `burn`. Rebasing tokens (e.g., AMPL) silently change channel balances post-deposit and break voucher accounting against `claimedAmount`.
4. **No transfer hooks or arbitrary external calls in `transfer`/`transferFrom`.** Only the standard `Transfer` event is emitted; no ERC-777-style callbacks, no `_beforeTokenTransfer` hooks calling external contracts, no reentrancy vectors. `SafeERC20` and `nonReentrant` are defense-in-depth, not a substitute for excluding hook-bearing tokens at the allowlist boundary.
5. **Pausable transfers are disclosed and bounded.** A pausable token is acceptable only if the pause authority is a known, regulated entity (e.g., Circle for USDC) with publicly disclosed pause criteria. A token whose `pause()` is callable by an anonymous or unaudited EOA MUST NOT be approved.
6. **Proxy / upgradability is disclosed.** If the token is upgradeable, the upgrade authority (single key, multisig, governance contract) MUST be documented in the proposal. Tokens with anonymous or unaudited upgrade authorities MUST NOT be approved.
7. **Admin controls are disclosed.** Any admin function that can move user balances, freeze accounts, or change token semantics (e.g., USDC's `blacklist`, USDT's `addBlackList`) MUST be documented. Governance accepts these for regulated stablecoins where the trust model is explicit; opaque admin powers are grounds for rejection.

The checklist is the canonical governance prerequisite for `addToken`. Items 1–4 are not tradeoffs — they protect contract solvency. Items 5–7 are trust judgments delegated to governance; the disclosure requirement is not.

## Migration from ADR 003

The PoC uses `StablePaymentChannel` (USDC-only, ADR 003). Production deploys `PaymentChannel` (multi-token, defined above) as a direct replacement — not a parallel deployment. Since no real users or funds exist on the PoC contract, no phased migration is needed:

1. Deploy `PaymentChannel` with `addToken(USDC_ADDRESS, 1, 1000)` called at deployment (floor = 1 USDC base unit, ceiling = 1000 USDC base units = $0.001/MB, matching [ADR 003](003-payments.md) defaults)
2. Governance calls `addToken` with appropriate rate bounds in the token's own base units for any additional tokens (e.g., DAI). Each `addToken` call MUST be backed by a completed [Token Vetting Checklist](#token-vetting-checklist).
3. All nodes update config to point to the new contract
4. The PoC `StablePaymentChannel` is decommissioned
