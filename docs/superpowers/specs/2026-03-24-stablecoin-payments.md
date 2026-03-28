# Dual-Currency Payment Model: Stablecoin Payments

**Date:** 2026-03-24
**Status:** Draft
**Companion to:** [Main Design Spec](./2026-03-24-decentralized-storage-design.md), [Tokenomics Spec](./2026-03-24-tokenomics.md)
**Scope:** Production design. Not PoC — this is the solution to Tokenomics Spec Risk #1 (token price volatility). PoC remains single-token as currently specified.

---

## 1. Problem Statement

The tokenomics spec (Section 5) demonstrates the core problem:

| Token Price | Monthly Revenue (7,000 tokens) | Monthly Cost (USD) | Result |
|-------------|-------------------------------|-------------------|--------|
| $0.001 | $7 | $35-100 | Loss |
| $0.01 | $70 | $35-100 | Breakeven |
| $0.05 | $350 | $35-100 | Profitable |
| $0.10 | $700 | $35-100 | Very profitable |

Provider infrastructure costs are denominated in USD (VPS, bandwidth, storage hardware). Revenue is denominated in a volatile token. A 10x price drop turns a profitable provider into a losing one overnight, even with identical workload.

**Why reactive rate adjustment is insufficient:**

- Providers must continuously monitor token price and update gossip-advertised rates
- Rate changes propagate slowly via gossip — clients see stale rates
- Mid-session rate changes are impossible (vouchers are pre-signed at the old rate)
- Creates a poor UX: clients see different prices every time they open the app
- Providers competing on rate become a proxy for providers competing on price predictions

The inverse problem affects clients: a 10x token pump means their deposited channel funds become worth far more than intended, creating opportunity cost.

**The root cause:** Using a volatile asset as a medium of exchange for services with USD-denominated costs.

---

## 2. Dual-Currency Architecture

**Principle:** Use each currency for what it does best. Stablecoins for predictable value transfer. The native token (TOKEN) for network-specific economic functions that require aligned incentives.

| Function | Currency | Rationale |
|----------|----------|-----------|
| Provider staking | TOKEN | Stake must align provider incentives with network health. If the network thrives, stake appreciates. |
| Governance voting | TOKEN | Governance power should reflect network commitment, not purchasing power. |
| Challenge bonds | TOKEN | Security mechanism in the staking domain. |
| Slashing penalties | TOKEN | Already denominated in stake. No change. |
| Storage payments | Stablecoin (USDC) | Provider costs are in USD. Storage deals last days-to-months — too long to tolerate volatility. |
| Delivery payments | Stablecoin (USDC) | Per-MB delivery payments. Providers need predictable unit economics. |
| Protocol fees | Stablecoin (collected), partially converted to TOKEN (buyback) | Fees match payment currency to avoid forced conversion at collection time. |

### Supported Stablecoins

The `StablePaymentChannel` contract accepts any ERC-20 stablecoin on a governance-maintained allowlist:

| Stablecoin | Status | Notes |
|------------|--------|-------|
| USDC (Circle) | Allowed at launch | Most liquid on Arbitrum, regulated issuer |
| USDT (Tether) | Added via governance vote | High liquidity, different risk profile |
| DAI (MakerDAO) | Added via governance vote | Decentralized — mitigates USDC censorship risk |

Adding a stablecoin requires a governance vote to add its contract address to the allowlist. This prevents arbitrary tokens from entering the system while allowing expansion.

### Why Not an Oracle-Based Alternative?

An alternative keeps the native token for all payments but uses a Chainlink price oracle to auto-adjust rate bounds in real-time. This was rejected because:

1. **Dependency:** Oracle downtime freezes rate bounds. The payment system becomes dependent on external infrastructure.
2. **Manipulation:** Oracle price manipulation distorts the entire payment system. Flash loan attacks on the oracle's price feed could temporarily shift rates.
3. **Root cause unaddressed:** Providers still receive a volatile token and must sell to cover costs. The conversion friction remains, just hidden behind an oracle.
4. **Complexity:** Oracle integration adds gas costs to every rate check and introduces staleness windows.

The dual-currency model is more invasive but eliminates the volatility problem at its root.

---

## 3. Payment Channel Modifications

### New Contract: `StablePaymentChannel`

A **new contract**, not a modification of the existing `PaymentChannel`. Rationale: the existing contract is tightly coupled to the native token and will be audited for the PoC. Modifying it risks introducing bugs in the already-validated staking/slashing path. The new contract handles only stablecoin payment channels.

**Channel state:**

```solidity
struct Channel {
    address client;
    address provider;
    address stablecoin;       // ERC-20 address (e.g., USDC)
    uint256 deposit;          // in stablecoin base units (USDC: 6 decimals)
    uint256 claimedAmount;    // cumulative amount claimed via vouchers
    uint256 openedAt;
    uint256 expiresAt;
    uint8   status;           // Open, Closing, Closed
    uint256 disputeDeadline;  // set when close is initiated
}
```

**Channel ID derivation:** `channelId = keccak256(abi.encodePacked(client, provider, stablecoin, nonce))` where `nonce` is a per-client counter incremented on each `openChannel` call. This allows multiple channels between the same client-provider pair (e.g., one in USDC and one in USDT). The `openChannel` function returns the `channelId`.

**Key difference:** USDC uses 6 decimals, not 18. All arithmetic must account for this. The contract stores the stablecoin address per channel, allowing different channels to use different stablecoins.

### Voucher Format Change

Current (native token):
```
{channelId, amount, nonce, signature}
```

New (stablecoin):
```
{channelId, amount, nonce, stablecoin, signature}
```

The `stablecoin` field (ERC-20 contract address) is included in the signed EIP-712 typed data. This prevents cross-token replay attacks — a voucher signed for a USDC channel cannot be replayed against a USDT channel.

**Wire format discriminator:** The streaming protocol (`stream/v1` ALPN) must distinguish between native-token and stablecoin vouchers. The `Voucher` message in the `protocol` crate includes a `channel_type` discriminator:

```rust
enum ChannelType {
    NativeToken,                           // legacy: {channelId, amount, nonce, sig}
    Stablecoin { address: Address },       // new: {channelId, amount, nonce, stablecoin, sig}
}
```

Providers and clients negotiate the channel type during the `StreamRequest`/`StreamResponse` handshake. The `StreamRequest` includes the client's preferred `ChannelType`; the `StreamResponse` confirms or rejects it.

### Channel Lifecycle

Identical flow to the native token channel, different currency:

1. Client approves `StablePaymentChannel` to spend stablecoin: `USDC.approve(stablePaymentChannel, amount)`
2. Client calls `openChannel(provider, stablecoin, deposit)`. Contract transfers stablecoin from client.
3. Off-chain: client signs cumulative vouchers. Amounts are in stablecoin base units.
4. Close: either party submits latest voucher. Dispute window applies (24 hours).
5. Settlement: contract distributes stablecoin — provider gets `voucherAmount - protocolFee`, client reclaims `deposit - voucherAmount`, treasury gets `protocolFee`.

### Decimal Handling (Rust Side)

The `incentive` crate must handle both 6-decimal (USDC/USDT) and 18-decimal (TOKEN, DAI) tokens:

```rust
enum Currency {
    Native { decimals: u8 },      // TOKEN, 18 decimals
    Stable { address: Address, decimals: u8 }, // USDC (6), DAI (18), etc.
}
```

All amount formatting, parsing, and display must go through this abstraction. The voucher signing code uses raw base units — no decimal conversion in the signature path (avoids precision bugs).

### Legacy Token Channels

The existing `PaymentChannel` contract remains deployed and functional. During migration, both channel types coexist. After migration Phase 3 (see Section 8), new token channels are disabled but existing ones can still close and settle normally.

---

## 4. Rate Denomination

Rates are quoted in USD-stable terms. This is the core benefit.

### Rate Comparison

| Service | Current (token-denominated) | New (stablecoin-denominated) |
|---------|---------------------------|------------------------------|
| Storage | 10 tokens/GB/month | $0.10/GB/month (100,000 USDC base units) |
| Delivery | 0.001 tokens/MB | $0.00001/MB (10 USDC base units) |
| Indexer query (future) | 0.0001 tokens/query | $0.000001/query (1 USDC base unit) |

**Why these numbers:** They roughly match current cloud storage/CDN economics with a markup for decentralization overhead. A provider storing 500GB and serving 2TB/month earns $50 + $20 = $70/month in stablecoin — enough to cover a budget VPS ($35-100) with a margin, regardless of TOKEN price.

### Rate Advertisement in Gossip

Extend the existing `ContentAnnounce` message with stablecoin-specific fields:

```rust
struct ContentAnnounce {
    hash: Hash,
    available: bool,
    // Legacy fields (kept for backward compat during migration)
    storage_rate_per_gb_month: Option<u64>,
    delivery_rate_per_mb: Option<u64>,
    // New stablecoin fields
    stable_storage_rate_per_gb_month: Option<u64>,  // stablecoin base units
    stable_delivery_rate_per_mb: Option<u64>,       // stablecoin base units
    accepted_stablecoins: Vec<Address>,             // max 5 entries (see below)
}
```

Old fields remain for backward compatibility during migration. Nodes that don't understand the new fields ignore them (all fields are `Option`).

**Bounds on `accepted_stablecoins`:** Maximum 5 entries. Receiving nodes silently drop messages with more than 5 entries to prevent gossip bloat from malicious providers. In practice, 2-3 stablecoins (USDC, USDT, DAI) is sufficient.

### Rate Bounds (Governable)

| Parameter | Floor | Ceiling |
|-----------|-------|---------|
| Storage (USDC base units / GB / month) | 10,000 ($0.01) | 10,000,000 ($10.00) |
| Delivery (USDC base units / MB) | 1 ($0.000001) | 100,000 ($0.10) |

Bounds are wide to accommodate market variation. Floor prevents race-to-bottom, ceiling prevents gouging. Both stored in `StablePaymentChannel` contract, adjustable via governance.

### Rate Confirmation

The `StreamResponse` message includes the provider's current stablecoin delivery rate. The client confirms by sending the first stablecoin-denominated voucher, or disconnects. No surprise pricing.

---

## 5. Protocol Fee Handling

### Fee Collection

| Parameter | Value |
|-----------|-------|
| Fee percentage | 3% (governable, max 20%) — unchanged from tokenomics spec |
| Collection point | `StablePaymentChannel.closeChannel()` deducts fee before distributing |
| Currency | Same stablecoin as the channel |
| Destination | Protocol treasury address (governance-controlled) |

**Fee flow example:**

```
Client deposits 100 USDC into channel
  → Delivers content, signs vouchers totaling 80 USDC
  → Channel closes with final voucher of 80 USDC
  → Contract distributes:
      77.60 USDC → provider
       2.40 USDC → treasury
      20.00 USDC → client (reclaimed)
```

### Fee Allocation

| Use | % of Fees | Currency | Mechanism |
|-----|-----------|----------|-----------|
| Development fund | 40% | USDC | Held as stablecoin in treasury |
| Bug bounties & audits | 20% | USDC | Held as stablecoin in treasury |
| Ecosystem grants | 20% | USDC | Held as stablecoin in treasury |
| Token buyback & burn | 20% | USDC → TOKEN → burn | Via `BuybackBurner` contract |

The 80% non-buyback allocation stays as stablecoin in the treasury — no forced conversion. Governance directs spending.

### Buyback & Burn Mechanism

The `BuybackBurner` contract replaces the direct token burn from the tokenomics spec. Since fees are now in stablecoin, the equivalent deflationary mechanism is:

1. Treasury transfers accumulated stablecoin fees (20% allocation) to `BuybackBurner`
2. `BuybackBurner.executeBuyback()` swaps USDC for TOKEN via Uniswap V3 on Arbitrum
3. Purchased TOKEN is sent to a burn address (`0x000...dead`)

**Buyback parameters:**

| Parameter | Value | Governable |
|-----------|-------|------------|
| Minimum accumulation before buyback | 1,000 USDC | Yes |
| Maximum single buyback | 10,000 USDC | Yes |
| Slippage tolerance | 2% (200 bps) | Yes |
| DEX | Uniswap V3 TOKEN/USDC pool | Yes (pool address) |
| Execution | Governance-triggered or automated keeper | — |

**Slippage protection:** The caller provides `minAudioOut` to the `executeBuyback` function. This prevents sandwich attacks. A TWAP oracle could be added later for automated buybacks, but for initial deployment, governance/keeper sets `minAudioOut` based on current market price.

**Why buyback is better than direct burn:** The tokenomics spec burned 20% of fees collected in native token — those tokens were already held. The buyback mechanism creates *new buy pressure* from the open market, directly linking network usage to token demand. More delivery → more USDC fees → more TOKEN purchased and burned.

---

## 6. Token Utility Preservation

If payments move to stablecoin, why hold TOKEN? The token must have clear, non-substitutable utility.

### Six Demand Drivers

| Utility | Mechanism | Why USDC Can't Substitute |
|---------|-----------|--------------------------|
| **1. Provider staking** | Min 1,000 TOKEN to operate as provider. No TOKEN = no network access. | Stake must correlate with network health. USDC-staked providers have no skin in the game. |
| **2. Governance** | Token-weighted voting on all protocol parameters. | Governance power must reflect network commitment, not just capital. |
| **3. Fee discount** | Providers staking ≥10x minimum (10,000 TOKEN) get 50% protocol fee reduction: 1.5% instead of 3%. | Rewards long-term network commitment. Direct financial incentive to hold more TOKEN. |
| **4. Client priority staking** | Clients optionally stake TOKEN (no minimum, no slashing). Providers prioritize high-stake clients during congestion. | Creates demand-side utility. Clients who hold TOKEN get better service. |
| **5. Buyback pressure** | 20% of all stablecoin fees used to buy and burn TOKEN on open market. | Direct link: more delivery = more USDC fees = more TOKEN bought and burned = supply reduction. |
| **6. Bootstrap rewards** | Provider bootstrap fund (200M TOKEN from distribution) paid as bonuses on top of USDC payments. | Early providers earn USDC for costs + TOKEN for upside. |

### Fee Discount Details

The `StablePaymentChannel` contract checks provider stake at channel close:

```solidity
uint256 feePercent = baseFeePercent; // 300 = 3%
if (stakingRegistry.stakeOf(channel.provider) >= stakingRegistry.minStake() * 10) {
    feePercent = feePercent / 2; // 150 = 1.5%
}
uint256 fee = (amount * feePercent) / 10000;
```

This is a simple threshold check, not a continuous function. A provider either qualifies (≥10x min stake) or doesn't. Keeps the contract logic straightforward and gas-efficient.

**Economic impact:** At 3% fee, a provider earning $70/month pays $2.10 in fees. At 1.5%, they pay $1.05 — saving $1.05/month. To qualify, they must stake 10,000 TOKEN. If TOKEN is $0.01, that's $100 locked for a $12.60/year savings — a 12.6% yield on staked capital just from fee reduction. This creates real demand for TOKEN.

### Client Priority Staking

A lightweight, optional mechanism:

- Clients call `StakingRegistry.clientStake(amount)` to deposit TOKEN
- No minimum, no slashing, no unbonding period — just a deposit
- Providers check client stake via `StakingRegistry.clientStakeOf(address)`
- **NodeId-to-address mapping:** During the iroh connection handshake, the client's `NodeId` (ed25519 public key) is known. The client signs a message binding their `NodeId` to their Ethereum address and includes it in the `StreamRequest`. The provider verifies this signature and uses the Ethereum address to look up `clientStakeOf`. This mapping is ephemeral (per-session, not stored on-chain) to minimize complexity.
- During congestion, providers prioritize higher-staking clients in their connection queue
- Enforcement is off-chain (provider-side logic), not on-chain — keeps it simple
- Clients withdraw anytime: `StakingRegistry.clientUnstake(amount)`

This is a soft signal, not a hard gate. Non-staking clients still get served, just with lower priority during congestion.

### Token Demand Summary

The token transitions from "medium of exchange" (bad for volatile assets) to "productive capital asset" (good for volatile assets):

| Role | Why they hold TOKEN |
|------|-------------------|
| Provider | Must stake to operate. More stake = fee discount. |
| Client | Optional stake for priority access during congestion. |
| Governance participant | Must hold to vote on protocol parameters. |
| Market | Buyback creates continuous buy pressure proportional to usage. |

---

## 7. Smart Contract Changes

### Contract Summary

| Contract | Action | Details |
|----------|--------|---------|
| `Token` (ERC-20) | **No change** | TOKEN unchanged |
| `StakingRegistry` | **Minor modification** | Add `getStakeMultiple()` view, add client staking functions |
| `PaymentChannel` | **No change** (eventually deprecated) | Remains for legacy token channels |
| `StablePaymentChannel` | **New contract** | Stablecoin payment channels with fee discount |
| `BuybackBurner` | **New contract** | Fee accumulation, Uniswap swap, TOKEN burn |

### `StablePaymentChannel` Interface

**Constructor parameters:** `stakingRegistry` (address), `treasury` (address), `initialFeePercentage` (uint256), `initialDisputeWindow` (uint256), `governance` (address).

```solidity
interface IStablePaymentChannel {
    // Channel lifecycle
    function openChannel(address provider, address stablecoin, uint256 deposit) external returns (bytes32 channelId);
    function topUp(bytes32 channelId, uint256 additionalDeposit) external;
    function closeChannel(bytes32 channelId, uint256 amount, uint256 nonce, bytes calldata signature) external;
    function disputeChannel(bytes32 channelId, uint256 amount, uint256 nonce, bytes calldata signature) external;
    function reclaimExpired(bytes32 channelId) external;

    // Views
    function getChannel(bytes32 channelId) external view returns (Channel memory);
    function getEffectiveFee(address provider) external view returns (uint256 bps);
    function isAllowedStablecoin(address token) external view returns (bool);

    // Governance (all subject to safety bounds below)
    function setFeePercentage(uint256 bps) external;
    function setAllowedStablecoin(address token, bool allowed) external;
    function setTreasuryAddress(address treasury) external;
    function setMinDeposit(uint256 amount) external;
    function setDisputeWindow(uint256 seconds_) external;
    function setRateBounds(address stablecoin, uint256 storageFloor, uint256 storageCeiling, uint256 deliveryFloor, uint256 deliveryCeiling) external;
}
```

**Safety bounds (hardcoded — governance cannot exceed):**

| Parameter | Minimum | Maximum |
|-----------|---------|---------|
| Fee percentage | 0 bps (0%) | 2000 bps (20%) |
| Dispute window | 1800 seconds (30 min) | 604800 seconds (7 days) |
| Min deposit | 1 base unit | No max |
| Rate floor | 0 | Must be < ceiling |
| Rate ceiling | Must be > floor | No max |

These match the safety philosophy in the tokenomics spec. Even a compromised governance cannot set the fee to 100% or the dispute window to zero.

**Rate bounds are per-stablecoin.** The contract stores `mapping(address => RateBounds)` where `RateBounds` contains floor/ceiling for storage and delivery rates in that stablecoin's base units. This solves the decimal mismatch: USDC bounds are in 6-decimal units, DAI bounds are in 18-decimal units, each set independently via `setRateBounds`. Rate bounds are enforced off-chain (providers check before advertising) and optionally on-chain (providers can register their rates for transparency).

### `BuybackBurner` Interface

```solidity
interface IBuybackBurner {
    // Restricted: only callable by governance or authorized keeper
    function executeBuyback(
        address stablecoin,
        uint256 amount,
        uint256 minAudioOut
    ) external;

    // Governance only
    function setKeeper(address keeper) external;
    function setSwapRouter(address router) external;
    function setSlippageTolerance(uint256 bps) external;
    function setMinBuybackAmount(uint256 amount) external;
    function setMaxBuybackAmount(uint256 amount) external;
    function setAudioToken(address token) external;

    // Views
    function keeper() external view returns (address);
    function getAccumulatedFees(address stablecoin) external view returns (uint256);
}
```

**Access control:**

- `executeBuyback`: callable by governance multisig OR the authorized `keeper` address. Not publicly callable.
- `setKeeper`: governance only. The keeper is a single address (e.g., a Chainlink Automation-compatible contract or an EOA operated by the team). Set to `address(0)` to disable keeper access.
- All `set*` functions: governance only (behind timelock).

The `executeBuyback` function:
1. Validates `amount >= minBuybackAmount && amount <= maxBuybackAmount`
2. Approves Uniswap V3 router to spend `amount` of `stablecoin`
3. Swaps via `ISwapRouter.exactInputSingle()` with `amountOutMinimum = minAudioOut`
4. Sends received TOKEN to burn address (`0x000...dEaD`)
5. Reverts if `minAudioOut` is not met (prevents sandwich attacks)

**Thin liquidity handling:** If the TOKEN/USDC pool does not exist or has insufficient liquidity, the swap reverts due to `minAudioOut` check. Accumulated fees remain in the contract until liquidity improves. The keeper should monitor pool depth before triggering buybacks. The `maxBuybackAmount` (default 10,000 USDC) should be set conservatively relative to pool depth — governance should lower it if the pool is thin.

### `StakingRegistry` Modifications

Two additions to the existing contract. All state-changing functions must use a reentrancy guard (OpenZeppelin `ReentrancyGuard`) since they transfer ERC-20 tokens.

```solidity
// New view for fee discount check
function getStakeMultiple(address provider) external view returns (uint256) {
    return stakes[provider].amount / minStake;
}

// Client staking (optional, no slashing)
mapping(address => uint256) public clientStakes;

function clientStake(uint256 amount) external nonReentrant {
    token.transferFrom(msg.sender, address(this), amount);
    clientStakes[msg.sender] += amount;
}

function clientUnstake(uint256 amount) external nonReentrant {
    require(clientStakes[msg.sender] >= amount);
    clientStakes[msg.sender] -= amount;
    token.transfer(msg.sender, amount);
}

function clientStakeOf(address client) external view returns (uint256) {
    return clientStakes[client];
}
```

---

## 8. Migration Path

### Phase 1: Deploy Alongside (4-6 weeks)

No breaking changes. Both channel types coexist.

1. Deploy `StablePaymentChannel` and `BuybackBurner` on Arbitrum Sepolia
2. Add `stable_*` rate fields to gossip `ContentAnnounce` messages. Old fields remain. Backward compatible — nodes that don't understand new fields ignore them.
3. Update `incentive` crate to support both `PaymentChannel` (token) and `StablePaymentChannel` (stablecoin), selected by config
4. Providers opt in by setting `accepted_stablecoins` in config and advertising stablecoin rates
5. Clients detect stablecoin support and prefer stablecoin channels when available, fall back to token channels

### Phase 2: Stablecoin-Preferred (2-3 months)

1. Deploy to production L2
2. Default client behavior: open stablecoin channels if provider supports them, token channels otherwise
3. Provider bootstrap fund begins distributing TOKEN bonuses on top of USDC payments
4. Governance sets stablecoin rate bounds
5. Fee discount mechanism goes live (providers with ≥10x stake get 1.5% fee)
6. Monitor: track % of channels as stablecoin vs. token. **Target: >80% stablecoin within 3 months.**

### Phase 3: Token Channels Deprecated (1-2 months after Phase 2)

1. Governance vote to disable new token-denominated payment channels (`PaymentChannel.openChannel()` reverts)
2. Existing token channels settle normally — no funds trapped
3. After 60 days with no open token channels, `PaymentChannel` contract is effectively retired
4. Token utility is fully: staking, governance, fee discounts, client priority, buyback sink

### Rollback Plan

If stablecoin channels cause unforeseen problems during Phase 1-2:
- Providers remove stablecoin rates from gossip announcements
- Clients fall back to token channels automatically (already the fallback path)
- `StablePaymentChannel` contract remains deployed but unused
- No governance action needed — the migration is market-driven, not forced

---

## 9. Tradeoffs and Risks

### What We Gain

- **Provider revenue stability.** $70/month in USDC covers costs regardless of TOKEN price.
- **Client cost predictability.** ~$0.0000375 per 3.75MB blob ($0.00001/MB), period.
- **Simpler rate discovery.** Rates are directly comparable without mental token-price conversion.
- **Reduced rate churn.** Providers don't adjust rates when token price moves.
- **Cleaner token model.** TOKEN becomes a capital asset (staking, governance) not a payment token.

### What We Lose or Risk

| Risk | Severity | Mitigation |
|------|----------|------------|
| **Token demand reduction** | High | All 6 utility mechanisms (Section 6) must be live before or during migration. If payments move to stablecoin before fee discounts and buyback are active, the token loses its primary demand driver with nothing to replace it. **This is the critical sequencing constraint.** |
| **USDC regulatory/censorship risk** | Medium | Circle can freeze USDC at specific addresses or blacklist the contract itself. Mitigation: support multiple stablecoins including decentralized options (DAI, LUSD). If USDC is compromised, governance adds DAI as the primary stablecoin. |
| **Contract surface area increase** | Medium | Two new contracts (`StablePaymentChannel`, `BuybackBurner`) double the audit surface. `BuybackBurner` interacts with Uniswap, adding composability risk. Mitigation: `minAudioOut` parameter prevents sandwich attacks; timelock on buyback parameters; full audit before production. |
| **Decimal mismatch bugs** | Low-Medium | USDC (6 decimals) vs TOKEN (18 decimals). Off-by-12-orders-of-magnitude bugs possible. Mitigation: `Currency` enum in the `incentive` crate with explicit decimal handling. Extensive unit tests with both 6 and 18 decimal tokens. |
| **Liquidity fragmentation** | Low | Payment volume moves from TOKEN to USDC markets. The TOKEN/USDC pool may thin. Mitigation: buyback mechanism provides continuous buy-side demand. Protocol treasury can seed the TOKEN/USDC pool. |
| **User complexity** | Low | Users need USDC + TOKEN (for staking). Two tokens instead of one. Mitigation: client software can integrate a DEX swap (swap TOKEN↔USDC in one click). Clients who only consume content need only USDC — simpler than understanding a volatile token. |
| **Migration coordination** | Low | During Phase 1-2, mixed currency support. Mitigation: additive migration, never forced. Market preference drives adoption. Fallback to token channels always available. |

---

## 10. Parameter Summary

All new and modified parameters introduced by this spec:

| Parameter | Value | Contract | Governable |
|-----------|-------|----------|------------|
| **Stablecoin Allowlist** | | | |
| Initial stablecoins | [USDC] | StablePaymentChannel | Yes |
| **Stable Payment Channels** | | | |
| Minimum deposit | 1 USDC (1,000,000 base units). Must exceed close-channel gas cost (~$0.10 on Arbitrum). 1 USDC provides ~10x headroom. | StablePaymentChannel | Yes |
| Dispute window | 24 hours | StablePaymentChannel | Yes |
| Channel expiry | 30 days | StablePaymentChannel | Yes |
| **Fees** | | | |
| Fee percentage | 3% (300 bps) | StablePaymentChannel | Yes |
| Fee discount threshold | 10x minStake | StablePaymentChannel | Yes |
| Fee discount amount | 50% reduction (to 1.5%) | StablePaymentChannel | Yes |
| **Buyback** | | | |
| Min accumulation | 1,000 USDC | BuybackBurner | Yes |
| Max single buyback | 10,000 USDC | BuybackBurner | Yes |
| Slippage tolerance | 2% (200 bps) | BuybackBurner | Yes |
| Burn allocation (% of fees) | 20% | Treasury logic | Yes |
| **Rate Bounds (per-stablecoin)** | | | |
| Storage rate floor (USDC) | $0.01/GB/month (10,000 base units, 6 decimals) | StablePaymentChannel | Yes |
| Storage rate ceiling (USDC) | $10.00/GB/month (10,000,000 base units, 6 decimals) | StablePaymentChannel | Yes |
| Delivery rate floor (USDC) | $0.000001/MB (1 base unit, 6 decimals) | StablePaymentChannel | Yes |
| Delivery rate ceiling (USDC) | $0.10/MB (100,000 base units, 6 decimals) | StablePaymentChannel | Yes |
| *DAI, USDT bounds* | *Set via governance when those stablecoins are added, in their respective base units* | StablePaymentChannel | Yes |
| **Client Staking** | | | |
| Minimum | None (any amount) | StakingRegistry | No |
| Slashing | None | StakingRegistry | No |
| Unbonding | Instant withdrawal | StakingRegistry | No |

---

## 11. Open Questions

1. **TWAP oracle for buyback slippage protection?** Currently using a simple `minAudioOut` parameter set by the caller. A Uniswap V3 TWAP oracle would allow automated buybacks without human-set slippage bounds. Adds oracle dependency. **Recommendation:** Start with `minAudioOut`, add TWAP later if automated buybacks are needed.

2. **Can providers refuse native token channels entirely?** Simplifies their accounting but fragments the network during migration. **Recommendation:** Yes — providers choose what they accept via `accepted_stablecoins` in their config. If `accepted_stablecoins` is non-empty, the provider supports stablecoin channels for those currencies. If empty, the provider only supports legacy token channels. A provider can support both by advertising stablecoin rates alongside legacy token rates. The protocol never forces a switch.

3. **Client-side fee discounts?** Should clients who stake TOKEN also get reduced fees? Creates demand-side utility but complicates fee calculation (who gets the discount — client or provider?). **Recommendation:** Defer to v2. Provider-only fee discounts are simpler and create sufficient token demand.

4. **Watchtower fees in stablecoin?** Watchtowers monitor stablecoin channels, so their fees should logically be in stablecoin. **Recommendation:** Yes, but design when watchtowers are actually implemented (see Tokenomics Spec Section 8).

5. **Should the bootstrap fund pay in USDC, TOKEN, or both?** The tokenomics spec allocates 200M TOKEN for provider bootstrapping. With stablecoin payments, early providers already earn USDC. The bootstrap fund could pay TOKEN bonuses (upside exposure) or USDC (guaranteed value). **Recommendation:** TOKEN bonuses. Providers get USDC for costs, TOKEN for network-aligned upside. This also creates token distribution among active participants.
