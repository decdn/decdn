# ADR 003: Payment Model

**Date:** 2026-03-28
**Status:** Draft

## Context

Nodes deliver bytes and need to be paid for it. The payment mechanism must work at per-MB granularity without an on-chain transaction per delivery, and must give delivering nodes immediate protection against non-payment.

Three constraints shape the design:

1. On-chain transactions on an L2 cost ~$0.05–0.10 each — acceptable per channel lifecycle, not per MB delivered.
2. A typical delivery session transfers a few MB. The payment per MB at market rates is on the order of $0.00001 — far below any on-chain transaction cost.
3. Node operators have real infrastructure costs (VPS, bandwidth, backend storage). Revenue denominated in a volatile native token creates unacceptable P&L risk: a 10× price drop turns a profitable operator into a loss.

## Decision

Payments use **unidirectional off-chain payment channels settled on an EVM L2, denominated in USDC**.

The same channel mechanism operates at two tiers:

- **Client → node**: a client opens a USDC channel with a node, signs cumulative vouchers as MB are delivered, and the node initiates channel close on-chain and settles to claim payment after the dispute window.
- **Node → node**: when a node pulls content from another node (typically an origin-backed node) for the first time, it pays via the same channel mechanism. The origin-backed node is paid wholesale; the pulling node recoups this by serving multiple clients from its cache at a markup.

A channel is opened by depositing USDC into the `StablePaymentChannel` contract. As content is delivered, the payer signs cumulative vouchers off-chain — one voucher per MB received. The delivering node holds the latest voucher and submits it on-chain to initiate channel close. A 24-hour dispute window allows either party to counter a stale or fraudulent close attempt. After the dispute window expires, the channel is settled and funds are distributed.

Key parameters:

- Voucher cadence: 1 MB delivered per voucher
- Minimum deposit: 1 USDC (covers ~100,000 MB at floor rate, far more than any practical session)
- Protocol fee: 3% deducted at channel settlement, sent to treasury
- Fee discount: nodes staking ≥10× the minimum TOKEN stake pay 1.5% instead of 3%

### Fee Calculation on Disputed Closes

The protocol fee is calculated **at final settlement**, after the dispute window expires, based on the highest valid voucher amount on-chain at that point. The three-step channel close lifecycle is:

1. **`closeChannel`** — records the submitted voucher's `amount` in `claimedAmount` and `nonce` in `claimedNonce`, sets status to `Closing`, starts the dispute window. **No fee is deducted.**
2. **`disputeChannel`** (during dispute window) — if the submitted voucher has a strictly higher nonce, updates both `claimedAmount` and `claimedNonce` to the new values. Still **no fee deduction**. Submissions with an equal or lower nonce revert with no state change and no fee implications.
3. **`settleChannel`** (after dispute window expires) — callable by anyone. Computes the fee on the final `claimedAmount`, distributes funds, and sets status to `Closed`:
   - Provider receives: `claimedAmount - fee`
   - Treasury receives: `fee = claimedAmount × feePercentage / 10000`
   - Client receives refund: `deposit - claimedAmount`

This means a dispute that increases the settlement amount (e.g., from 50 USDC to 80 USDC) automatically increases the protocol fee (from 1.50 USDC to 2.40 USDC at 3%). The fee is always computed once, on the final settled amount — never on intermediate values and never more than once per channel.

The native token (TOKEN) is not used for delivery payments. It is reserved for staking and fee discount qualification (see ADR 004) and governance (see [ADR 009](009-governance.md)).

**Rate setting is entirely up to each node.** Nodes advertise their `rate_per_mb` in probe responses and stream responses; the requester sees the rate before committing a voucher. There is no protocol-enforced rate beyond a governance-set floor and ceiling. This creates a market with natural arbitrage dynamics:

- Origin-backed nodes set a higher rate because they bear backend costs (storage + egress from their hidden backing store). They are the effective price ceiling for any blob they hold.
- A cache-only node that pays an origin-backed node to pull a blob can then serve that blob to many clients at a markup, recouping the origin cost across multiple deliveries.
- A node in a region where no peer has the content yet can charge a premium for that first delivery. Once it has the blob, other nearby nodes can pull from it at a competitive rate and compete for local clients.
- Nodes with cheaper bandwidth or better hardware can sustainably undercut others; nodes in high-demand regions can charge more and still win on latency.

This means the network self-balances: popular content gets replicated because caching it is profitable, competition drives prices down in well-served regions, and unpopular content stays at origin-backed node rates until demand justifies caching it. No central coordinator decides where to replicate what.

**Origin backend economics:** The choice of backing store directly affects an origin-backed node's viable rate. At the expected market rate of $0.01/GB, an S3-backed node paying $0.09/GB egress loses money on every cache miss and must amortize origin pulls across a high cache-hit ratio (or price above market rate). Zero-egress backends — Cloudflare R2 ($0.00/GB), Backblaze B2 ($0.00/GB via Bandwidth Alliance partners), and Wasabi ($0.00/GB) — allow origin-backed nodes to remain profitable at or near market rates. Operators choosing high-egress backends should expect to set higher `rate_per_mb` values to cover their costs, which the market tolerates for content that is not yet cached elsewhere.

## Consequences

**Positive:**

- On-chain costs are amortized across an entire channel lifetime — open + close + settle = three transactions regardless of how many MB are delivered (settle can be called by any address, allowing third-party settlement bots)
- USDC denomination gives node operators predictable unit economics: delivery revenue covers infrastructure costs without exposure to TOKEN price movements
- The voucher is the payment receipt; the BLAKE3 hash is the delivery receipt. Together they provide mutual protection: the client doesn't sign a voucher for bytes that fail hash verification; the node stops delivering if vouchers stop arriving
- Maximum risk per voucher interval (1 MB) at $0.00001/MB is $0.00001 — negligible
- Market-driven rate setting means replication happens organically: profitable content gets cached by more nodes, driving prices down without any coordination protocol
- The `StablePaymentChannel` contract is isolated from the staking/slashing contract (`PaymentChannel`), keeping the audit surface for each contract bounded

**Negative:**

- Clients must hold USDC to use the network; this adds an onboarding step compared to a single-token model
- Rate volatility: a node can change its advertised rate between a probe and a stream request; the `StreamResponse` rate is the binding one, but a client that probed at one rate and receives a higher rate in `StreamResponse` must disconnect and re-probe rather than having been deceived silently
- Two payment contracts coexist during migration (legacy `PaymentChannel` for TOKEN, `StablePaymentChannel` for USDC), doubling audit surface temporarily
- USDC is issued by Circle, which can freeze specific addresses or blacklist the contract. For the PoC this risk is accepted; multi-token payment support to mitigate it is deferred to [ADR 010](010-multi-token.md)
- BLAKE3 verification on EVM requires an intermediate Merkle proof scheme for PoC-era slash evidence; a client submitting a slash claim cannot directly prove BLAKE3 mismatch on-chain

## Attack Vectors

### Client-side

**Voucher withholding**
Client receives bytes but stops signing vouchers, getting content for free up to the last signed interval.

The self-enforcing stop is sufficient. Maximum loss is one interval (1 MB × rate ≈ $0.00001). No additional mechanism needed — this is fully addressed by the protocol design.

---

**Channel griefing**
Client opens many channels with minimum deposit and never streams, forcing nodes to track and eventually close stale channels.

The current mitigation (auto-expire + deposit > gas cost) limits financial loss to the attacker but does not bound the memory overhead on the node. An attacker with modest capital can hold thousands of open-but-idle channels in the node's tracking state for up to 30 days. Options:

- **Option A — Inactivity expiry.** Channels with no voucher submitted within the first 7 days auto-expire, rather than the full 30-day channel lifetime. Reduces the attack window significantly at no cost to normal users.
- **Option B — On-chain channel cap per address.** The `StablePaymentChannel` contract enforces a maximum number of open channels per client Ethereum address (e.g., 10). Hard to circumvent without new wallet addresses, each requiring on-chain funding.
- **Option C — Node-side filtering.** Nodes refuse `StreamRequest` from channels that have been open longer than N days with zero vouchers. Off-chain, no contract change needed, but relies on node operator implementation.

---

**Stale close**
Client submits an old voucher (lower amount) to close the channel, underpaying the node.

The 24-hour dispute window works if the node is online. The gap is liveness: if the node goes offline after a stale close is submitted and misses the dispute window, it loses the difference. Options:

- **Option A — Watchtowers.** A separate monitoring service holds the latest voucher and submits it on the node's behalf if a dispute is detected. Adds operational complexity but fully closes the gap.
- **Option B — Longer dispute window.** Increase from 24 hours to 7 days, giving operators more time to respond. Delays legitimate channel closes for everyone.
- **Option C — Persistent monitoring process.** The node binary runs a lightweight dispute monitor as a separate thread that only watches the chain for close events, independent of the serving process. Simpler than a watchtower but still single-node.

---

**Probe fishing**
Client sends probe requests to many nodes at high frequency to map the network or exhaust node resources without ever paying.

The current mitigation is weak. Clients are not staked — their NodeIds are free to rotate — so per-NodeId rate limiting is bypassable. The iroh connection setup cost is also low. Options:

- **Option A — IP-based rate limiting.** Rate limit probe requests by source IP rather than NodeId. Harder to rotate at scale, though not impossible with proxies or cloud infrastructure.
- **Option B — Require an open channel to probe.** Only clients with an existing open payment channel (any amount) can probe. Strong protection but adds friction for new users who haven't yet deposited.
- **Option C — Proof-of-work on probe requests.** Include a small PoW challenge in the probe request (e.g., find a nonce such that `hash(NodeId || nonce) < difficulty`). Adds CPU cost to bulk probing without affecting honest single-request clients noticeably.
- **Option D — Accept the risk.** A probe is a single message exchange. The cost to serve one is negligible; the attack only matters at extreme scale. Rate limit at the connection level (iroh handles this) and monitor for abuse rather than trying to prevent it at the protocol level.

**Note:** Probe responses are considered public information (see ADR 005). The concern here is resource exhaustion from bulk probing, not information leakage — content availability is already broadcast via gossip, and pricing is revealed in probe/stream responses by design.

---

**Double-spend across nodes**
Client opens channels with multiple nodes using the same USDC deposit via a race condition before the on-chain state settles.

Fully solved. Each `openChannel` call transfers USDC into the contract immediately; the client's wallet balance is debited on-chain before the transaction finalises. No credit facility exists.

---

### Node-side

**Data withholding**
Node accepts a stream request, receives a voucher, then stops delivering bytes.

Fully solved by the self-enforcing protocol. The node cannot extract more payment than the last acknowledged voucher. The client resumes from `byte_offset` on a different node.

---

**Corrupted delivery**
Node serves bytes that don't match the advertised BLAKE3 hash.

BLAKE3 verification catches this immediately at the client. The remaining gap is the slash evidence path: submitting the full bad bytes on-chain to prove a BLAKE3 mismatch is gas-expensive for large blobs, and the PoC Merkle proof scheme adds complexity. Options:

- **Option A — Optimistic challenge-response.** Client submits only a commitment (hash of received data) and the chunk index on-chain. The contract gives the node 24 hours to respond with the correct chunk and a Merkle proof. If it cannot, it is slashed. This avoids submitting full blob data on-chain.
- **Option B — Rely on reputation, not slash, for the common case.** Slash is a last resort for severe or repeated corruption. For a single incident, immediate session termination + reputation penalty is sufficient. Reserve the on-chain slash path for nodes with a history of corruption.
- **Option C — Off-chain fraud proof with a verifier role.** A designated verifier node (staked, incentivised by a cut of the slash) receives the disputed bytes off-chain, verifies the BLAKE3 mismatch, and submits a compact on-chain attestation. Adds a trusted verifier dependency.

---

**Rate bait-and-switch**
Node advertises a low rate in probe responses then returns a higher rate in `StreamResponse`.

**Resolved: slashable offense.** Both `ProbeResponse` and `StreamResponse` now include cryptographic signatures over the advertised rate (see ADR 005). If a client receives a signed `ProbeResponse` with rate X and a signed `StreamResponse` with rate Y > X from the same node within 30 seconds, the two signed messages constitute on-chain-verifiable evidence of rate manipulation. The node is slashed per the escalating schedule in ADR 004. The 30-second window allows legitimate rate changes between sessions while catching same-session bait-and-switch.

---

**Phantom blob announcement**
Node announces a blob as cached then fails or redirects on actual request.

**Resolved: slashable offense.** The `ProbeResponse` now includes a cryptographic signature over `{hash, has_blob, rate_per_mb, timestamp_us}` (see ADR 005). If a node signs `has_blob: true` but subsequently responds with `NotCached` or fails to deliver within 30 seconds, the signed probe response plus the delivery failure constitute on-chain-verifiable evidence. The node is slashed per the escalating schedule in ADR 004. The 30-second validity window accounts for the possibility that a blob is legitimately evicted between probe and request — 30 seconds is short enough to make eviction implausible but long enough for normal protocol flow. The challenged node has a 24-hour window to counter by proving it delivered the blob (signed delivery receipt from the same requester within the relevant time window).

---

**Channel close front-running**
Node monitors the mempool and front-runs a client's channel close with a higher voucher submission.

Not a real attack. The contract always settles the highest valid voucher, and only the client can sign a valid voucher. A node submitting the latest voucher before the client is the intended happy path. Fabricating a higher voucher requires forging the client's ECDSA signature, which is cryptographically infeasible.

---

### Network-level

**Eclipse attack**
Attacker surrounds a client with malicious nodes so all probe responses come from nodes under attacker control.

BLAKE3 verification catches data corruption regardless of which nodes are in the routing table. The remaining gap is a denial-of-service variant: an attacker controlling all of a client's known nodes can simply refuse to serve. Options:

- **Option A — Origin-backed nodes as fallback.** Clients can specifically query the registry for well-known origin-backed nodes for a given blob, bypassing the general routing table. An eclipse must also control all origin-backed nodes for the target content — which requires capital proportional to the number of origin-backed nodes for that content.
- **Option B — Multi-source bootstrap.** Clients discover initial peers from at least two independent sources (on-chain registry + a hardcoded DNS seed list). An attacker must compromise both to fully eclipse a client.
- **Option C — Minimum honest-peer diversity.** Clients maintain connections to at least N nodes discovered via different paths. All N would need to be attacker-controlled for a full eclipse.

---

**Gossip flooding**
Node sends high-volume `CacheAnnounce` messages to exhaust peer routing table memory or crowd out legitimate announcements.

Registry check + per-sender rate limiting is solid. The minor gap is that the local registry cache may be up to 10 minutes stale, briefly allowing recently-unstaked nodes to flood. Mostly solved; no strong alternative needed beyond tightening the registry cache refresh on high flood detection.

---

**Sybil nodes**
Attacker stakes many cheap nodes to dominate probe responses for popular content, controlling pricing in a region.

The core weakness is token-price dependency: at $0.001/TOKEN, a minimum stake of 1,000 TOKEN costs $1 per sybil node. The `rate_per_mb × rtt_ms` selection score helps — a sybil fleet must be real hardware in the right geography and competitively priced — but does not eliminate the risk when the token is cheap. Options:

- **Option A — Governance raises minimum stake if token price falls.** The minimum stake is governable. Token holders are incentivised to raise it to protect the network, since a sybil-dominated network reduces usage and token value. Reactive but aligned.
- **Option B — Minimum stake denominated in USD equivalent via oracle.** Requires a price oracle, which was rejected in ADR 004 for payment rate bounds. The same concerns (oracle downtime, manipulation) apply here, but the impact of oracle failure is lower (new stakers temporarily blocked, not payments broken).
- **Option C — Reputation as a second filter.** New nodes (low reputation, few settled channels) are deprioritised in client selection even if their `rate_per_mb × rtt_ms` score is competitive. A sybil fleet takes time to build reputation, limiting its effectiveness during that window.

---

**Rate manipulation cartel**
Colluding nodes in a region hold rates artificially high.

Origin-backed nodes set the effective price ceiling for any blob. Clients can always probe origin-backed nodes directly and pay their rates as a guaranteed fallback. Any node outside the cartel that undercuts wins all local traffic — the incentive to defect is strong. New entrants can join permissionlessly by staking.

---

**Content withholding**
A node stakes, announces content it holds, but refuses to serve it — collecting credibility in the routing tables without actually participating.

**Withholding is not a slashable offense** — operators may legitimately take content offline for maintenance, migration, or business reasons, and slashing for availability creates perverse incentives. Instead, withholding is handled through reputation and redundancy:

- **Multiple origin-backed nodes per blob.** Content owners configure multiple origin-backed nodes for important content. A single withholding node becomes irrelevant if others serve the same blob.
- **Reputation fast-path.** Nodes that fail to serve announced content accumulate reputation penalties at a steeper rate. A node with consistently poor availability is deprioritized in routing and loses delivery revenue.

---

**Replay attack on vouchers**
Attacker intercepts a signed voucher and attempts to replay it against a different channel or after close.

Fully solved. EIP-712 typed data over `{channelId, amount, nonce, token}` binds the voucher to a specific channel. The EIP-712 domain separator (see [EIP-712 Voucher Signature](#eip-712-voucher-signature)) further binds each voucher to a specific chain and contract deployment, preventing replay across different L2s, contract upgrades, or test vs production environments. The monotonically increasing nonce prevents resubmission after settlement.

## Contract Interfaces

### StablePaymentChannel

The `StablePaymentChannel` is a new contract separate from the existing `PaymentChannel`. It handles only stablecoin payment channels.

**Channel state:**

```solidity
struct Channel {
    address client;
    address provider;
    address token;            // hardcoded to USDC for PoC; any ERC-20 in production (see ADR 010)
    uint256 deposit;          // in token base units (USDC: 6 decimals for PoC)
    uint256 claimedAmount;    // cumulative amount claimed via vouchers
    uint256 claimedNonce;     // nonce of the current best voucher, for dispute comparison
    uint256 openedAt;
    uint256 expiresAt;
    uint8   status;           // 0 = Open, 1 = Closing (dispute window active), 2 = Closed (settled)
    uint256 disputeDeadline;  // set when close is initiated
}
```

**Channel ID:** `channelId = keccak256(abi.encodePacked(client, provider, nonce))` where `nonce` is a per-client counter. Allows multiple channels between the same client-node pair.

```solidity
interface IStablePaymentChannel {
    // Channel lifecycle
    function openChannel(address provider, uint256 deposit) external returns (bytes32 channelId);
    function topUp(bytes32 channelId, uint256 additionalDeposit) external;
    function closeChannel(bytes32 channelId, uint256 amount, uint256 nonce, bytes calldata signature) external;
    function disputeChannel(bytes32 channelId, uint256 amount, uint256 nonce, bytes calldata signature) external;
    function settleChannel(bytes32 channelId) external;
    function reclaimExpired(bytes32 channelId) external;

    // Views
    function getChannel(bytes32 channelId) external view returns (Channel memory);
    function getEffectiveFee(address provider) external view returns (uint256 bps);

    // Governance
    function setFeePercentage(uint256 bps) external;
    function setTreasuryAddress(address treasury) external;
    function setMinDeposit(uint256 amount) external;
    function setDisputeWindow(uint256 seconds_) external;
    function setRateBounds(uint256 deliveryFloor, uint256 deliveryCeiling) external;
}
```

**Channel close events:**

```solidity
event ChannelCloseInitiated(
    bytes32 indexed channelId,
    address indexed initiator,
    uint256 amount,
    uint256 nonce,
    uint256 disputeDeadline
);

event ChannelDisputed(
    bytes32 indexed channelId,
    address indexed disputor,
    uint256 newAmount,
    uint256 newNonce
);

event ChannelSettled(
    bytes32 indexed channelId,
    uint256 providerPayout,
    uint256 clientRefund,
    uint256 protocolFee
);
```

**Channel close lifecycle:**
- `closeChannel` → requires status `Open`. Sets status to `Closing`, records voucher, emits `ChannelCloseInitiated`. No fund transfers.
- `disputeChannel` → requires status `Closing` and `block.timestamp < disputeDeadline`. Accepts only vouchers with strictly higher nonce. Updates `claimedAmount`, emits `ChannelDisputed`. No fund transfers.
- `settleChannel` → requires status `Closing` and `block.timestamp >= disputeDeadline`. Computes fee on final `claimedAmount`, transfers funds to provider/treasury/client, sets status to `Closed`, emits `ChannelSettled`.

**Safety bounds (hardcoded):**

| Parameter | Minimum | Maximum |
| --- | --- | --- |
| Fee percentage | 0 bps (0%) | 2000 bps (20%) |
| Dispute window | 1800 seconds (30 min) | 604800 seconds (7 days) |
| Min deposit | 1 base unit | No max |
| Rate floor | 0 | Must be < ceiling |
| Rate ceiling | Must be > floor | No max |

**Rate bounds are in USDC base units (6 decimals) for the PoC.** The contract stores a single `RateBounds` struct with `deliveryFloor` and `deliveryCeiling`. Per-token rate bounds are deferred to [ADR 010](010-multi-token.md).

**Initial rate bounds (PoC):**

| Parameter | Value (USD/MB) | USDC base units | Rationale |
| --- | --- | --- | --- |
| `deliveryFloor` | $0.000001/MB | 1 | Anti-abuse minimum; 10× below expected market rate. Prevents zero-rate free-riding while imposing no practical constraint on legitimate pricing. |
| `deliveryCeiling` | $0.001/MB | 1,000 | 100× expected market rate. Accommodates origin-backed nodes with high-egress backends (e.g., S3 at $0.09/GB) while remaining well above any legitimate pricing scenario ($1.00/GB vs Akamai's ~$0.12–0.20/GB). |

The expected market rate is $0.00001/MB (10 USDC base units per MB, or $0.01/GB). This positions deCDN ~4–9× cheaper than major traditional CDNs (CloudFront at $0.085/GB, KeyCDN at $0.04/GB) and at parity with budget providers (Bunny.net at $0.01/GB). Both bounds are governable post-PoC within the hardcoded safety constraints above.

### BuybackBurner

```solidity
interface IBuybackBurner {
    function executeBuyback(address token, uint256 amount, uint256 minTokenOut) external;
    function setKeeper(address keeper) external;
    function setSwapRouter(address router) external;
    function setSlippageTolerance(uint256 bps) external;
    function setMinBuybackAmount(uint256 amount) external;
    function setMaxBuybackAmount(uint256 amount) external;
    function keeper() external view returns (address);
    function getAccumulatedFees(address token) external view returns (uint256);
}
```

`executeBuyback` is callable by governance multisig or the authorized `keeper` address. All `set*` functions are governance-only behind a timelock.

### EIP-712 Voucher Signature

All voucher signatures use [EIP-712](https://eips.ethereum.org/EIPS/eip-712) typed structured data to prevent cross-chain, cross-contract, and cross-environment replay.

**Domain separator:**

```solidity
bytes32 constant DOMAIN_TYPEHASH = keccak256(
    "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
);

bytes32 public immutable DOMAIN_SEPARATOR;

constructor() {
    DOMAIN_SEPARATOR = keccak256(abi.encode(
        DOMAIN_TYPEHASH,
        keccak256(bytes("StablePaymentChannel")),   // name
        keccak256(bytes("1")),                      // version
        block.chainid,                              // chainId (L2)
        address(this)                               // verifyingContract
    ));
}
```

The domain separator binds every voucher to a specific contract deployment on a specific chain. A voucher signed for Arbitrum Sepolia cannot be replayed on mainnet, and a voucher signed for one `StablePaymentChannel` deployment cannot be replayed against an upgraded or redeployed contract at a different address.

**Voucher type:**

```solidity
bytes32 constant VOUCHER_TYPEHASH = keccak256(
    "Voucher(bytes32 channelId,uint256 amount,uint256 nonce,address token)"
);
```

**Signature digest:**

```solidity
bytes32 digest = keccak256(abi.encodePacked(
    "\x19\x01",
    DOMAIN_SEPARATOR,
    keccak256(abi.encode(VOUCHER_TYPEHASH, channelId, amount, nonce, token))
));
```

**Verification:** Implementations must use a hardened ECDSA helper (e.g., OpenZeppelin's `ECDSA.recover`) or equivalent logic that rejects non-canonical `s` values and restricts `v` to `27`/`28`. The recovered signer must equal `channel.client`. The signature is encoded as 65 bytes (`r || s || v`), matching the format used by `eth_sign` and standard Ethereum libraries.

The `DOMAIN_SEPARATOR` is computed once in the constructor and stored as an immutable. If the contract is deployed behind a proxy and may be migrated to a different chain, it should be cached in a state variable and recomputed only when `block.chainid` changes (the pattern used by OpenZeppelin's `EIP712` base contract), rather than on every call.

### StakingRegistry Modifications

The full node registry interface (`NodeInfo`, `registerNode`, `getActiveNodes`, etc.) is defined in ADR 001. The additions below are payment-specific extensions:

```solidity
// Fee discount check — divides by minStake so the result scales with governance changes
function getStakeMultiple(address provider) external view returns (uint256) {
    return stakes[provider].amount / minStake;
}
```

`StablePaymentChannel.getEffectiveFee` calls this function and compares against the multiplier threshold (not a hardcoded absolute amount):

```solidity
uint256 constant DISCOUNT_MULTIPLE = 10;

function getEffectiveFee(address provider) external view returns (uint256 bps) {
    if (stakingRegistry.getStakeMultiple(provider) >= DISCOUNT_MULTIPLE) {
        return feePercentage / 2; // 1.5% when base fee is 3%
    }
    return feePercentage;
}
```

This ensures the discount threshold (currently 10 × 1,000 = 10,000 TOKEN) stays correct if governance changes `minStake`.

```solidity

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

## Client Priority Staking

A lightweight, optional mechanism for clients to signal commitment:

- Clients call `StakingRegistry.clientStake(amount)` to deposit TOKEN
- No minimum, no slashing, no unbonding period — just a deposit
- Nodes check client stake via `StakingRegistry.clientStakeOf(address)`
- During congestion, nodes prioritize higher-staking clients in their connection queue
- Enforcement is off-chain (node-side logic), not on-chain
- Clients withdraw anytime: `StakingRegistry.clientUnstake(amount)`

**NodeId-to-address mapping:** During the iroh connection handshake, the client's `NodeId` (ed25519 public key) is known. The client signs a message binding their `NodeId` to their Ethereum address and includes it in the `StreamRequest`. The node verifies this signature and uses the Ethereum address to look up `clientStakeOf`. This mapping is ephemeral (per-session, not stored on-chain).

This is a soft signal, not a hard gate. Non-staking clients still get served, just with lower priority during congestion.

## Decimal Handling

USDC uses 6 decimals; TOKEN uses 18 decimals. All payment amounts in the `incentive` crate use USDC base units (µUSDC). The voucher signing code uses raw base units — no decimal conversion in the signature path to avoid precision bugs.

Multi-token decimal abstraction (a `Currency` enum covering arbitrary ERC-20 decimals) is deferred to [ADR 010](010-multi-token.md).

**Voucher format:**

```
{channelId, amount, nonce, token, signature}
```

During delivery over `cdn/client/v1`, only `{signature, amount}` are transmitted on the wire; the remaining fields are derived from stream context. See [ADR 005](005-protocol.md) for wire protocol details.

The `token` field (ERC-20 address) is included in the signed EIP-712 typed data to prevent cross-token replay attacks. For the PoC, this field is hardcoded to the USDC contract address. The full EIP-712 type definition and domain separator are specified in [EIP-712 Voucher Signature](#eip-712-voucher-signature).

## Stablecoin Migration Path

### Phase 1: Deploy Alongside (4–6 weeks)

No breaking changes. Both channel types coexist.

1. Deploy `StablePaymentChannel` and `BuybackBurner` on Arbitrum Sepolia
2. Add stablecoin rate fields to gossip messages (backward compatible — old fields remain, new fields are `Option`)
3. Update `incentive` crate to support both `PaymentChannel` (token) and `StablePaymentChannel` (stablecoin), selected by config
4. Nodes opt in by setting `accepted_tokens` in config
5. Clients prefer USDC channels when available, fall back to TOKEN channels

### Phase 2: Stablecoin-Preferred (2–3 months)

1. Deploy to production L2
2. Default client behavior: stablecoin channels if node supports them, token channels otherwise
3. Node bootstrap fund begins distributing TOKEN bonuses on top of USDC payments
4. Governance sets stablecoin rate bounds
5. Fee discount mechanism goes live (nodes with ≥10× stake get 1.5% fee)
6. Target: >80% stablecoin channels within 3 months

### Phase 3: Token Channels Deprecated (1–2 months after Phase 2)

1. Governance vote to disable new token channels (`PaymentChannel.openChannel()` reverts)
2. Existing token channels settle normally — no funds trapped
3. After 60 days with no open token channels, `PaymentChannel` is effectively retired

### Rollback Plan

If stablecoin channels cause unforeseen problems during Phase 1–2:

- Nodes remove stablecoin rates from gossip announcements
- Clients fall back to token channels automatically
- `StablePaymentChannel` remains deployed but unused
- No governance action needed — migration is market-driven, not forced
