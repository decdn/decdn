# Tokenomics: Decentralized Audio Storage & Delivery Network

**Date:** 2026-03-24
**Status:** Draft
**Companion to:** [Main Design Spec](./2026-03-24-decentralized-storage-design.md)
**Scope:** Defines all economic parameters for both PoC (implementable now) and production (design targets subject to simulation).

## Overview

This document specifies the token economics for a decentralized audio storage and delivery network. The system uses a single ERC-20 token to incentivize three roles: providers (store and serve audio), uploaders (push content into the network), and listeners (stream audio). Payments flow through off-chain unidirectional payment channels settled on an EVM L2. Providers stake tokens to join and face slashing for provable misbehavior. A gossip-based reputation system uses interaction-weighted scoring to rank providers.

For protocol mechanics (wire format, ALPN protocols, crate structure, content model), see the main design spec.

---

## 1. Token Fundamentals

### Standard & Chain

- **PoC:** ERC-20 on Arbitrum Sepolia (testnet). Freely mintable via a faucet function — no supply constraints.
- **Production:** ERC-20 on a production L2 (Arbitrum One, Base, or equivalent). L2 choice determines gas costs and bridging options.

### Supply Model (Production)

| Parameter | Value |
|-----------|-------|
| Total supply | 1,000,000,000 (1B) tokens, fixed at genesis |
| Decimals | 18 |
| Minting after genesis | None. Fixed supply. |
| Burn mechanism | 100% of slashed stake + 20% of protocol fees are burned (see Section 3 for fee allocation) |

**Why fixed supply with burn:** Simplicity. No inflation schedule to manage, no emission curve debates. The burn creates deflationary pressure proportional to network activity — more usage means more fees burned. This is easier to reason about than inflationary models and avoids the "provider reward halving" cliffs that plague emission-based systems.

**Tradeoff acknowledged:** Fixed supply means no protocol-level provider bootstrapping rewards. Early providers must be incentivized through the distribution (see bootstrap fund below) or through organic demand. If organic demand is insufficient at launch, a capped inflation mechanism (max 2% annual, governance-controlled) can be added via contract upgrade. This is documented as an open question.

### Distribution (Production)

| Allocation | Percentage | Tokens | Vesting |
|------------|-----------|--------|---------|
| Protocol treasury | 25% | 250M | 4-year linear, 6-month cliff |
| Provider bootstrap fund | 20% | 200M | Released on-demand via governance for provider incentive programs |
| Team & contributors | 15% | 150M | 4-year linear, 12-month cliff |
| Community & ecosystem grants | 20% | 200M | 3-year linear, no cliff |
| Liquidity (DEX + CEX) | 10% | 100M | Fully unlocked at genesis |
| Early supporters / seed | 10% | 100M | 2-year linear, 6-month cliff |

**Provider bootstrap fund:** Dedicated to attracting early providers before organic streaming revenue is sufficient. Distributed as bonus rewards on top of normal storage/delivery payments. Governed by token holders — proposals to release funds require a governance vote. Target: fund 2 years of above-market provider rewards.

### PoC Simplification

For PoC, the token contract includes a public `mint(address to, uint256 amount)` function callable by anyone. No supply cap, no distribution, no vesting. This lets testers freely acquire tokens.

---

## 2. Pricing Model

### PoC: Flat Rates

| Service | Rate | Example |
|---------|------|---------|
| Storage | 10 tokens / GB / month | 1 track (5MB FLAC) for 1 month = 0.05 tokens |
| Delivery | 0.001 tokens / MB streamed | 1 track (4 min, 128kbps) ~3.75MB = 0.00375 tokens |
| Indexer queries (future) | 0.0001 tokens / query | 100 searches = 0.01 tokens |

### Production: Provider-Set Rates

In production, providers set their own rates within protocol-defined bounds:

| Parameter | Floor | Ceiling | Rationale |
|-----------|-------|---------|-----------|
| Storage rate | 1 token / GB / month | 100 tokens / GB / month | Floor prevents race-to-bottom; ceiling prevents gouging |
| Delivery rate | 0.0001 tokens / MB | 0.01 tokens / MB | Same logic |

**Rate advertisement:** Providers include their rates in gossip announcements. Extend the existing `ContentAnnounce` message on the `content-routing/v1` gossip topic with optional rate fields:

```rust
struct ContentAnnounce {
    hash: Hash,
    available: bool,
    storage_rate_per_gb_month: Option<u64>,  // in token base units
    delivery_rate_per_mb: Option<u64>,       // in token base units
}
```

Clients see rates before connecting and choose providers by a combination of reputation, rate, and geography. No on-chain auction — the market discovers prices through competition.

**Rate confirmation:** The `StreamResponse` message includes the provider's current delivery rate. The client confirms (by sending the first voucher) or disconnects. No surprise pricing.

---

## 3. Protocol Fees

| Parameter | PoC | Production |
|-----------|-----|-----------|
| Fee percentage | 0% | 3% (governable, max 20%) |
| Collection point | N/A | `PaymentChannel.closeChannel()` deducts fee before distributing |
| Destination | N/A | Protocol treasury address (governance-controlled multisig) |

**Fee flow (production):**

```
Client deposits 100 tokens into channel
  → Streams audio, signs vouchers totaling 80 tokens
  → Channel closes with final voucher of 80 tokens
  → Contract distributes: 77.6 tokens to provider, 2.4 tokens to treasury
  → Client reclaims remaining 20 tokens
```

**Fee allocation (governed by token holders):**

| Use | Target % of fees |
|-----|-----------------|
| Development fund | 40% |
| Bug bounties & audits | 20% |
| Ecosystem grants | 20% |
| Burn (deflationary) | 20% |

The 20% burn is automatic (sent to `address(0)`). The remaining 80% accumulates in the treasury for governance-directed spending. The burn percentage is governable.

---

## 4. Staking & Slashing Schedule

### Staking

| Parameter | PoC | Production |
|-----------|-----|-----------|
| Minimum stake | 1,000 tokens | 1,000 tokens (governable) |
| Unbonding period | 7 days | 7 days (governable, min 3 days) |
| Slashable during unbonding | Yes | Yes |
| Max providers per node | 1 | 1 |

**Staking flow:**
1. Provider calls `StakingRegistry.stake(amount)` with `amount >= minStake`
2. Provider is registered and can announce content
3. To leave: calls `StakingRegistry.initiateUnbonding()`
4. After unbonding period: calls `StakingRegistry.withdraw()`
5. During unbonding, stake remains slashable — prevents slash-then-run

### Slashing Offenses

| Offense | Description | Proof Mechanism | Scope |
|---------|-------------|----------------|-------|
| **Wrong data** | Provider served bytes that don't match the expected BLAKE3 hash | Optimistic fraud proof: challenger submits `{blob_hash, chunk_index, received_bytes, expected_root}`. Provider has 24h to counter with correct chunk. | **PoC + Production** |
| **Prolonged downtime** | Provider unreachable for >24h while holding paid storage deals | Challenger submits signed ping failure logs + on-chain deal record. Provider has 24h to prove liveness (respond to a challenge ping). | **Production only** |
| **Double settlement** | Provider submits a voucher to two different close transactions for the same channel | On-chain provable: contract detects duplicate channelId settlements. Automatic slash, no challenge needed. | **Production only** |

For PoC, only the "wrong data" offense is implemented. Prolonged downtime is handled via reputation (not slashing) and double settlement is unlikely at PoC scale.

### Slash Amounts

| Scenario | PoC | Production |
|----------|-----|-----------|
| First offense | 10% of stake | 5% of stake |
| Second offense within 30 days | 10% of stake (no escalation) | 15% of stake |
| Third offense within 30 days | 10% of stake | 100% of stake (full ejection from network) |
| Offense counter reset | N/A | After 90 days without incidents |

### Slash Distribution

| Destination | Percentage |
|-------------|-----------|
| Burned | 50% |
| Challenger reward | 50% |

The challenger reward incentivizes watchtowers and honest nodes to monitor and report misbehavior. Without it, reporting is a public good with no private incentive.

### Challenge Bond

To prevent frivolous or malicious fraud proof submissions:

| Parameter | PoC | Production |
|-----------|-----|-----------|
| Challenge bond | N/A (not implemented) | 50 tokens |
| Bond return | N/A | Returned if challenge succeeds (provider slashed) |
| Bond forfeiture | N/A | Forfeited if provider successfully counters. 50% burned, 50% to provider. |

**This is a new mechanism not in the main design spec** — it should be added as a cross-reference. Without the challenge bond, anyone can grief providers with false fraud proofs at zero cost (only gas).

### Auto-Ejection

If a provider's stake drops below 50% of the minimum stake requirement due to accumulated slashing, they are automatically ejected:
- Removed from the staking registry
- Content routing announces their content as unavailable
- Remaining stake enters forced unbonding (standard unbonding period applies)
- Provider must re-stake at full minimum to rejoin

---

## 5. Provider Unit Economics

### Cost Model (Estimated)

Based on typical infrastructure costs for a small provider node:

| Cost Category | Monthly Estimate | Notes |
|---------------|-----------------|-------|
| VPS (4 vCPU, 8GB RAM) | $20-40 | Hetzner, OVH tier |
| Storage (1TB SSD) | $10-20 | Included in many VPS plans |
| Bandwidth (5TB egress) | $0-25 | Many providers include 5-20TB |
| L2 gas costs | $5-15 | ~10 channel settlements/month at ~$0.50-1.50 each |
| **Total monthly cost** | **$35-100** | |

### Revenue Model (PoC Rates)

| Revenue Source | Calculation | Monthly Revenue |
|----------------|-------------|----------------|
| Storage (500GB stored) | 500 * 10 tokens / month | 5,000 tokens |
| Delivery (2TB served) | 2,000,000 * 0.001 tokens / MB | 2,000 tokens |
| **Gross revenue** | | **7,000 tokens** |
| Protocol fee (0% PoC) | | 0 tokens |
| **Net revenue** | | **7,000 tokens** |

### Breakeven Analysis

At PoC rates, the breakeven depends on the token's notional value. Since PoC uses a freely mintable testnet token, real-dollar breakeven is not applicable. However, for production planning:

| Token Price | Monthly Revenue (7000 tokens) | Monthly Cost | Profit |
|-------------|-------------------------------|-------------|--------|
| $0.001 | $7 | $35-100 | Loss |
| $0.01 | $70 | $35-100 | Breakeven |
| $0.05 | $350 | $35-100 | Profitable |
| $0.10 | $700 | $35-100 | Very profitable |

**Key insight:** Provider profitability is highly sensitive to token price and utilization. At $0.01/token, a provider needs ~500GB stored and ~2TB/month served to break even on a budget VPS. This is achievable for a moderately popular music catalog.

### Gas Cost Breakdown (Arbitrum)

Estimated gas costs at typical Arbitrum L2 prices (~$0.01-0.10 per transaction):

| Operation | Estimated Gas | Cost at $0.05/tx |
|-----------|--------------|-------------------|
| `stake()` | ~100k gas | ~$0.05 |
| `openChannel()` | ~150k gas | ~$0.05 |
| `closeChannel()` | ~200k gas | ~$0.10 |
| `submitFraudProof()` | ~250k gas | ~$0.10 |
| `withdraw()` | ~80k gas | ~$0.05 |

**Payment channels amortize gas effectively.** A channel that stays open for 30 streaming sessions costs $0.15 total (open + close) = $0.005 per session. Without channels, 30 sessions would need 30 on-chain transactions = $1.50.

### Sensitivity Table

| Utilization | Storage Revenue | Delivery Revenue | Total (pre-fee) | Production (3% fee) |
|-------------|----------------|------------------|-----------------|---------------------|
| Low (100GB, 500GB/mo served) | 1,000 | 500 | 1,500 | 1,455 |
| Medium (500GB, 2TB/mo) | 5,000 | 2,000 | 7,000 | 6,790 |
| High (1TB, 10TB/mo) | 10,000 | 10,000 | 20,000 | 19,400 |

---

## 6. Payment Channel Economics

### Minimum Viable Deposit

A typical listening session: 30 tracks, 4 minutes each, 128kbps.

```
Per track: 4 min * 60s * 128kbps / 8 = 3.75 MB
Per session: 30 * 3.75 MB = 112.5 MB
Cost: 112.5 * 0.001 tokens/MB = 0.1125 tokens
```

**Recommended minimum deposit:** 1 token (covers ~8-9 sessions, reducing top-up frequency).

**Protocol-enforced minimum deposit:**
- **PoC:** 0.1 tokens (testnet tokens are free, gas cost irrelevant)
- **Production:** Governable, must exceed close-channel gas cost in token terms. At $0.10 close gas and $0.01/token, this would be ~10 tokens. The contract exposes a `minDeposit` parameter adjustable via governance to track gas costs over time.

### Channel Lifecycle Cost

| Event | Gas Cost | Token Cost (at $0.05/tx, $0.01/token) |
|-------|----------|--------------------------------------|
| Open channel | ~$0.05 | ~5 tokens |
| Close channel | ~$0.10 | ~10 tokens |
| **Total lifecycle** | **~$0.15** | **~15 tokens** |

**Per-session amortization:** If a channel stays open for 30 sessions, the on-chain overhead is 15 tokens / 30 = 0.5 tokens/session. The actual streaming cost for a session is ~0.1125 tokens, so the channel overhead dominates at low usage. This reinforces the design choice to keep channels open as long as possible. **Note:** The token-denominated gas cost (15 tokens) is highly sensitive to token price. At $0.10/token, the same gas costs only 1.5 tokens total — making the overhead negligible. The PoC uses free testnet tokens, so gas overhead is not a concern during testing.

**Implication:** Clients should keep channels open as long as possible. The protocol should not auto-close idle channels — only explicit close or expiry.

### Voucher Interval Tradeoff

At PoC delivery rate of 0.001 tokens/MB:

| Interval | Vouchers per Track (3.75MB) | Max Loss on Provider Failure | Signature Overhead |
|----------|-----------------------------|--------------------------|--------------------|
| 64KB | ~59 | 0.0000625 tokens (64KB * 0.001/MB) | High |
| 256KB (default) | ~15 | 0.000250 tokens (256KB * 0.001/MB) | Moderate |
| 1MB | ~4 | 0.001 tokens (1MB * 0.001/MB) | Low |

The default 256KB (1024 chunks) is a good balance. Maximum loss on provider failure is a fraction of a token. Finer granularity gives diminishing returns while increasing signature overhead.

### Channel Pool Capital Lockup

At PoC scale (pre-open channels with all ~10 providers):

```
10 channels * 1 token deposit = 10 tokens locked
```

At production scale (pre-open with top 3 providers):

```
3 channels * 1 token deposit = 3 tokens locked
```

This is minimal capital lockup. Not a concern for the economic model.

### Griefing Mitigation

**Attack:** Malicious client opens many channels with minimum deposit, never streams, forcing provider to pay gas to close them.

**Mitigation:** The production minimum deposit (governable `minDeposit`) is set to exceed close-channel gas cost. Channels also auto-expire after 30 days — providers don't need to actively close stale channels, just wait for expiry. Additionally, providers can set their own minimum deposit threshold above the protocol minimum.

---

## 7. Reputation Score Mechanics

### Score Model

| Parameter | Value |
|-----------|-------|
| Score range | 0.0 to 1.0 (stored as u32, 0 to 1,000,000, for 6-decimal precision) |
| Initial score (new provider) | 0.5 |
| Weight: local observations | 70% |
| Weight: network gossip | 30% |

### Local Score Calculation

After each interaction with a provider, the client updates its local score:

```
local_score = ewma(local_score, interaction_score, alpha=0.1)
```

Where `interaction_score` is derived from:

| Metric | Score Contribution | Weight |
|--------|-------------------|--------|
| Delivery speed (bytes/sec vs. expected) | 0.0-1.0 (linear scale) | 40% |
| Data correctness (BLAKE3 verified) | 0.0 or 1.0 (binary) | 40% |
| Connection success (reachable?) | 0.0 or 1.0 (binary) | 20% |

`interaction_score = 0.4 * speed_score + 0.4 * correctness + 0.2 * reachability`

The exponential weighted moving average (EWMA) with alpha=0.1 means recent interactions matter more but old interactions still contribute. A single bad interaction reduces the score but doesn't destroy it.

### Network Score Aggregation

Reports received via iroh-gossip are aggregated using an EWMA weighted by reporter credibility:

```
reporter_weight = settled_channels(reporter) / max_settled_channels_observed
network_score = ewma(network_score, report.score, alpha=0.05 * reporter_weight)
```

- `settled_channels(reporter)`: number of payment channels the reporter has settled on-chain (verifiable)
- `max_settled_channels_observed`: the highest settled-channel count among all known reporters
- Alpha is scaled by reporter weight: high-credibility reporters move the score faster

### Combined Score

```
final_score = 0.7 * local_score + 0.3 * network_score
```

If a node has no local observations for a provider (never interacted), it uses 100% network score.

### Decay

Scores decay toward neutral (0.5) over time without new data. Applied iteratively each week:

```
score_new = score_old + (0.5 - score_old) * decay_rate
```

With `decay_rate = 0.10` (10% per week), each week the score moves 10% of the remaining distance toward 0.5:

- Score 1.0: week 1 = 0.95, week 5 = 0.80, week 10 = 0.65, week 20 = 0.53
- Score 0.0: week 1 = 0.05, week 5 = 0.20, week 10 = 0.35, week 20 = 0.47

This is an exponential decay — scores converge to 0.5 asymptotically, reaching within 0.05 of neutral after ~30 weeks.

| Parameter | Value |
|-----------|-------|
| Decay rate | 10% per week (applied iteratively) |
| Decay starts after | 1 week with no new reports or interactions |
| Minimum score (floor) | 0.0 (fully untrusted providers are still scoreable) |
| Scope | **Production only** (PoC uses static scores, no decay) |

Inactive providers converge to "unknown" (neutral), preventing stale high scores from persisting indefinitely.

### Score Clamping

A single reputation report (local or network) can move a provider's score by at most **0.05** in either direction. This prevents:
- One bad interaction from destroying a good provider's reputation
- One fake report from inflating a sybil provider's score

### Tie-Breaking

When multiple providers have the same final score (within 0.01 tolerance), select by:

1. **Lower current load** — providers include approximate load in gossip announcements (number of active streams)
2. **Geographic diversity** — prefer providers in regions not already selected (if requesting multiple providers for replication)
3. **Higher stake** — more skin in the game, all else being equal
4. **Random** — final tiebreaker to prevent deterministic routing patterns

### Cold-Start Bootstrap

New providers face a chicken-and-egg problem: no reputation means no traffic, no traffic means no reputation.

**Mitigation:** During the first 7 days after staking (or first 50 completed interactions, whichever comes first), new providers receive a **10% selection bonus**. When a client ranks providers, new providers' scores are temporarily boosted by 0.05 (additive). This gives them enough traffic to build a real track record.

The bonus is local to each client (not on-chain) and decays linearly over the bootstrap period.

### Rate Limiting

To prevent spam and gaming, reputation reports are rate-limited:

- **Max 1 report per (reporter, provider) pair per hour** — prevents a single node from flooding the gossip topic with reports about one provider
- **Max 10 reports per reporter per hour** — prevents a single node from mass-rating all providers in a burst
- Reports that exceed the rate limit are silently dropped by receiving nodes
- Rate limiting is enforced locally by each node on received gossip messages (not by the gossip protocol itself)

---

## 8. Watchtower Economics

### Role

Watchtowers monitor on-chain channel close attempts and submit counter-vouchers on behalf of offline clients. Without watchtowers, a client offline for >1 hour (dispute window) during a stale voucher dispute loses funds.

### Payment Model

| Parameter | PoC | Production |
|-----------|-----|-----------|
| Implementation | Not implemented | Required |
| Fee structure | N/A | 0.1% of channel deposit per 30-day monitoring period |
| Payment method | N/A | Upfront fee deducted from channel deposit at open time |
| Minimum viable fee | N/A | Must exceed watchtower's expected gas cost for dispute tx |

**Example:** Client deposits 10 tokens, assigns a watchtower. Watchtower fee = 0.01 tokens (0.1%). If a dispute occurs, watchtower submits the counter-voucher and is reimbursed gas from the fee. If no disputes occur, the watchtower keeps the fee.

### Trust Model

- Client shares latest voucher state with the watchtower (necessary for it to function)
- Privacy impact is minimal: the provider already has all vouchers, and vouchers are not secret (they authorize payments to the provider)
- Watchtower cannot steal funds: vouchers pay the provider, not the watchtower
- Watchtower cannot grief: submitting a counter-voucher only helps the client
- Risk: watchtower goes offline during a dispute. Mitigated by allowing multiple watchtowers per channel (client sends voucher updates to 2-3 watchtowers)

### PoC Approach

No watchtowers. Clients must monitor disputes themselves. This is an accepted risk documented in the main spec. The 1-hour dispute window keeps the risk window small.

---

## 9. Governance Model

### PoC: Admin Key

A single deployer address (EOA or multisig) has admin rights on all contracts. Can update any parameter. No voting, no timelock. Appropriate for testnet iteration.

### Production: Token-Weighted Governance

Based on OpenZeppelin Governor with the following parameters:

| Parameter | Value |
|-----------|-------|
| Voting token | The network's ERC-20 token (staked or unstaked) |
| Proposal threshold | 100,000 tokens (0.01% of supply) to submit a proposal |
| Voting period | 3 days |
| Quorum | 4% of total supply |
| Timelock | 2 days between vote passing and execution |
| Vote delegation | Supported (delegate to another address) |

### Governable Parameters

| Parameter | Contract | Safety Bounds |
|-----------|----------|--------------|
| Protocol fee % | PaymentChannel | 0% to 20% |
| Minimum stake | StakingRegistry | 100 tokens to 100,000 tokens |
| Slash percentages | StakingRegistry | 1% to 100% |
| Unbonding period | StakingRegistry | 3 days to 30 days |
| Dispute window | PaymentChannel | 30 minutes to 7 days |
| Rate floor/ceiling | StakingRegistry | Floor > 0, ceiling > floor |
| Challenge bond | StakingRegistry | 1 token to 1,000 tokens |
| Burn percentage of fees | PaymentChannel | 0% to 100% |

**Safety bounds are hardcoded** in the contract — even governance cannot set parameters outside these ranges. This prevents a governance attack from destroying the protocol (e.g., setting fee to 100% or stake to 0).

### Emergency Multisig

- 3-of-5 multisig with known, trusted signers
- Can ONLY pause contracts (not change parameters or withdraw funds)
- Used for: exploit response, critical bug mitigation
- Sunset: after 12 months, the pause function is permanently disabled (or requires governance vote to extend)
- Signers should be geographically and organizationally diverse

---

## 10. Economic Attack Analysis

### Attack 1: Sybil Reputation Flood

| Aspect | Details |
|--------|---------|
| **Attack** | Create N fake providers, stake each, have them rate each other positively |
| **Cost** | N * 1,000 tokens (stake) + gas for channels between sybils + actual streaming activity (interaction-weighted scoring requires settled channels) |
| **Example** | 10 sybil nodes = 10,000 tokens stake + ~100 tokens in cross-channel settlements to build credibility |
| **Damage** | Sybil providers ranked higher than honest ones, receive traffic, deliver poor service |
| **Mitigation** | Interaction-weighted scoring: reports from nodes with few settled channels carry little weight. Score clamping limits impact of each report. Local observations (70%) dominate network gossip (30%). |
| **Residual risk** | Attacker with sufficient capital can build real interaction history. Cost scales linearly with desired influence. At production scale, this becomes prohibitively expensive. |

### Attack 2: False Fraud Proofs

| Aspect | Details |
|--------|---------|
| **Attack** | Submit fake fraud proofs to slash honest providers |
| **Cost** | Challenge bond (50 tokens) + gas. Bond forfeited if provider successfully counters. |
| **Damage** | If uncountered: 5-10% of provider's stake slashed. Provider must be offline for 24h to miss counter window. |
| **Mitigation** | Challenge bond makes spam expensive. 50% of forfeited bond goes to targeted provider as compensation. Provider only needs to respond once within 24h. |
| **Residual risk** | Attacker willing to lose 50 tokens per attempt can annoy providers who happen to be offline. Mitigated by provider monitoring/alerting. |

### Attack 3: Stake Grinding

| Aspect | Details |
|--------|---------|
| **Attack** | Provider stakes, builds reputation, unstakes, re-stakes on a new identity to shed bad reputation |
| **Cost** | 7-day unbonding period per cycle + reputation decay during unbonding + new stake deposit |
| **Damage** | Provider escapes accumulated negative reputation |
| **Mitigation** | Reputation decays toward neutral (0.5) during the unbonding period. Re-staking gives a neutral score, not a good one. The cold-start bootstrap bonus is small (0.05) and temporary. Net effect: attacker spends 7+ days with no revenue to reset to neutral — not advantageous. |
| **Residual risk** | Low. The attack costs more (lost revenue during unbonding) than it gains (neutral score vs. bad score). |

### Attack 4: Channel Exhaustion

| Aspect | Details |
|--------|---------|
| **Attack** | Open many channels with minimum deposit, never stream, forcing provider to track them and eventually pay gas to close |
| **Cost** | N * minDeposit + N * open gas |
| **Damage** | Provider memory/tracking overhead + gas to close stale channels |
| **Mitigation** | Minimum deposit must exceed close gas cost. Providers can set their own minimum deposit threshold (above protocol minimum). Channels auto-expire after 30 days — provider doesn't need to close them, just wait. |
| **Residual risk** | Low. Attack is expensive (capital locked) and self-limiting (channels expire). |

### Attack 5: Majority Stake Attack

| Aspect | Details |
|--------|---------|
| **Attack** | Acquire >50% of staked tokens to dominate reputation and governance |
| **Cost** | At 1B supply with 10% staked = 100M tokens staked. Attacker needs >50M tokens. |
| **Damage** | Control governance votes, manipulate reputation network-wide |
| **Mitigation** | Safety bounds prevent governance from setting destructive parameters. Local observations (70% weight) limit reputation manipulation. Emergency multisig can pause contracts. |
| **Residual risk** | Real but standard for any token-weighted governance system. Mitigation: distribute tokens widely, vest team/investor tokens. |

### Attack 6: Storage Withholding

| Aspect | Details |
|--------|---------|
| **Attack** | Accept storage deal, take uploader's payment, delete the data |
| **Cost** | Slash on detection (5-10% of stake on first offense) |
| **Revenue from attack** | Storage payment for the deleted data. E.g., 1GB for 1 month = 10 tokens. |
| **Profitability** | Slash = 5% of 1,000 tokens = 50 tokens. Revenue = 10 tokens. **Net loss of 40 tokens.** Attack is unprofitable. |
| **Mitigation** | Slash amount (50 tokens) exceeds maximum storage payment for any reasonable data size. Repeated offenses escalate to ejection. Uploaders detect via periodic ping and trigger re-replication. |
| **Residual risk** | Negligible at PoC rates. For production, ensure slash amount always exceeds maximum deal value — this should be a protocol invariant. |

---

## 11. Multi-Chain Bridging (High-Level)

Not in PoC scope. Brief outline for production planning.

### Approach

- Token is **canonical on one L2** (the production chain). All staking, channel settlements, and governance happen on this chain.
- For users on other chains: standard ERC-20 bridge (Arbitrum native bridge, or cross-chain protocol like LayerZero/Wormhole) to move tokens to the canonical chain before opening channels.
- **No cross-chain payment channels in v1.** Channels exist on one chain only. Cross-chain would require atomic swaps or a bridge-aware channel design — too complex for initial production.

### Decision Deferred

The production L2 choice determines:
- Available bridges and their trust assumptions
- Gas costs (affects unit economics in Section 5)
- Finality time (affects dispute window minimums)
- Tooling and ecosystem support

This decision should be made based on gas cost analysis and ecosystem fit, after PoC validation proves the core protocol works.

---

## 12. Parameter Summary

All economic parameters in one reference table:

| Parameter | PoC Value | Production Target | Contract/Component | Governable |
|-----------|-----------|-------------------|--------------------|-----------|
| **Token** | | | | |
| Total supply | Unlimited (faucet) | 1,000,000,000 | Token | No |
| Decimals | 18 | 18 | Token | No |
| **Staking** | | | | |
| Minimum stake | 1,000 tokens | 1,000 tokens | StakingRegistry | Yes |
| Unbonding period | 7 days | 7 days | StakingRegistry | Yes |
| Auto-ejection threshold | N/A | 50% of min stake | StakingRegistry | Yes |
| **Slashing** | | | | |
| First offense | 10% | 5% | StakingRegistry | Yes |
| Second offense (within 30d) | 10% | 15% | StakingRegistry | Yes |
| Third offense (within 30d) | 10% | 100% (ejection) | StakingRegistry | Yes |
| Offense counter reset | N/A | 90 days | StakingRegistry | Yes |
| Slash burn ratio | 50% | 50% | StakingRegistry | Yes |
| Challenge bond | N/A | 50 tokens | StakingRegistry | Yes |
| Challenge window | 24 hours | 24 hours | StakingRegistry | Yes |
| **Pricing** | | | | |
| Storage rate | 10 tokens/GB/mo | Provider-set | Off-chain | N/A |
| Storage rate floor | N/A | 1 token/GB/mo | StakingRegistry | Yes |
| Storage rate ceiling | N/A | 100 tokens/GB/mo | StakingRegistry | Yes |
| Delivery rate | 0.001 tokens/MB | Provider-set | Off-chain | N/A |
| Delivery rate floor | N/A | 0.0001 tokens/MB | StakingRegistry | Yes |
| Delivery rate ceiling | N/A | 0.01 tokens/MB | StakingRegistry | Yes |
| **Payment Channels** | | | | |
| Minimum deposit | 0.1 tokens | Governable (>close gas) | PaymentChannel | Yes |
| Dispute window | 1 hour | 24 hours | PaymentChannel | Yes |
| Channel expiry | 30 days | 30 days | PaymentChannel | Yes |
| Voucher interval | 256KB | 256KB | Off-chain | N/A |
| **Protocol Fees** | | | | |
| Fee percentage | 0% | 3% | PaymentChannel | Yes |
| Fee burn percentage | N/A | 20% | PaymentChannel | Yes |
| **Reputation** | | | | |
| Score range | 0.0-1.0 | 0.0-1.0 | Off-chain | N/A |
| Initial score | 0.5 | 0.5 | Off-chain | N/A |
| Local weight | 70% | 70% | Off-chain | N/A |
| Network weight | 30% | 30% | Off-chain | N/A |
| Decay rate | N/A (static scores) | 10%/week (iterative) | Off-chain | N/A |
| Max score change per report | N/A | 0.05 | Off-chain | N/A |
| Cold-start bonus | N/A | 0.05 for 7 days | Off-chain | N/A |
| Report rate limit | 1/reporter/provider/hr | 1/reporter/provider/hr | Off-chain | N/A |
| **Watchtower** | | | | |
| Monitoring fee | N/A | 0.1% of deposit/30d | Off-chain | N/A |
| **Governance** | | | | |
| Proposal threshold | N/A | 100,000 tokens | Governor | No |
| Voting period | N/A | 3 days | Governor | No |
| Quorum | N/A | 4% of supply | Governor | Yes |
| Timelock | N/A | 2 days | Timelock | Yes |

---

## 13. Open Questions & Risks

### Open Questions

1. **Exact token supply number:** 1B is a placeholder. Should be validated against expected network size, target token price, and comparable projects.
2. **Distribution percentages:** The 25/20/15/20/10/10 split is a starting point. Needs input from legal (team vesting), market (liquidity needs), and community (grant expectations).
3. **Production L2 choice:** Arbitrum One vs. Base vs. other. Affects gas costs, bridges, and ecosystem.
4. **Inflationary provider rewards:** Should there be a capped inflation mechanism for bootstrapping, or is the provider bootstrap fund (20% of supply) sufficient?
5. **Optimal slash percentages:** The 5/15/100% escalation is a design target. Should be validated through simulation before production.
6. **Dynamic fee adjustment:** Should the protocol fee auto-adjust based on network utilization, or only via governance votes?
7. **Token price oracle:** Unit economics depend on token price vs. fiat costs. Should the protocol use an oracle for rate bounds, or leave everything in token-denominated terms?

### Risks

1. **Token price volatility:** Provider profitability swings with token price. Mitigation: providers can adjust rates. Long-term: consider stablecoin-denominated channels as an alternative.
2. **Low initial demand:** Without listeners, providers have no delivery revenue. The bootstrap fund must be large enough to sustain providers until organic demand kicks in.
3. **Governance capture:** Large token holders can control parameter changes. Safety bounds limit damage but don't prevent rent-seeking (e.g., setting fees to 20%).
4. **Regulatory risk:** Tokens with economic utility may be classified as securities in some jurisdictions. Legal review required before production distribution.
5. **Smart contract risk:** Bugs in staking/payment contracts could lead to fund loss. Multiple audits required before production mainnet deployment.
