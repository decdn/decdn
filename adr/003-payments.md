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

A channel is opened by depositing USDC into the `StablePaymentChannel` contract. As content is delivered, the payer signs cumulative vouchers off-chain — one voucher per MB received (default cadence; negotiable for large transfers). The delivering node holds the latest voucher and submits it on-chain to initiate channel close. A dispute window (default 48 hours for PoC, governable within 12h–72h — see [ADR 009](009-governance.md)) allows either party to counter a stale or fraudulent close attempt. After the dispute window expires, the channel is settled and funds are distributed.

Key parameters:

- Voucher cadence: 1 MB delivered per voucher (default; negotiable up to `maxVoucherIntervalMb` for large transfers — see [Voucher Interval Negotiation](#voucher-interval-negotiation))
- Minimum deposit: 1 USDC (contract floor, governable); recommended practical minimum: 10 USDC (see [Deposit Economics](#deposit-economics))
- Protocol fee: 3% deducted at channel settlement, sent to treasury
- Fee discount: nodes staking ≥10× the minimum TOKEN stake pay 1.5% instead of 3%

### Deposit Economics

Opening, closing, and settling a channel requires three on-chain transactions totalling ~$0.23 on Arbitrum L2 (see [ADR 004](004-tokenomics.md) gas breakdown: `openChannel` ~$0.05, `closeChannel` ~$0.10, `settleChannel` ~$0.08). This estimate assumes an existing ERC-20 approval; first-time users incur an additional one-time `approve` transaction (~$0.03), bringing the true first-channel cost to ~$0.26. The table below uses the $0.23 lifecycle cost (excluding the one-time approval) as a percentage of various deposit sizes, with the optional watchtower minimum fee from [ADR 007](007-watchtower.md) modeled as a single 30-day monitoring period:

| Deposit | Lifecycle gas ($0.23) | Gas % of deposit | + Watchtower ($0.50 / 30 days, optional) | Total overhead % (1 monitoring period) |
|---------|----------------------|------------------|------------------------------------------|----------------------------------------|
| 1 USDC  | $0.23                | 23%              | $0.50                                    | 73%                                    |
| 5 USDC  | $0.23                | 4.6%             | $0.50                                    | 14.6%                                  |
| 10 USDC | $0.23                | 2.3%             | $0.50                                    | 7.3%                                   |
| 25 USDC | $0.23                | 0.92%            | $0.50                                    | 2.9%                                   |
| 100 USDC| $0.23                | 0.23%            | $0.50                                    | 0.73%                                  |

**Recommended practical minimum: 10 USDC.** Client software should default to a 10 USDC minimum deposit (user-overridable). At 10 USDC, gas overhead is 2.3% — acceptable for a payment channel that covers ~10,000,000 MB at the floor rate or ~1,000,000 MB (~1,000 GB) at the expected market rate ($0.01/GB), sufficient for weeks to months of casual use without top-up. The contract minimum (1 USDC, governable via `setMinDeposit`) remains a safety floor — it prevents dust channels that cost more to settle than they contain and preserves flexibility for testing and governance adjustment. Raising the contract minimum is not recommended because it would reduce governance flexibility and create a hard barrier for development/testing scenarios where small deposits are useful.

**Amortization.** The overhead percentages above represent worst-case single-session economics. Long-lived channels amortize open/settle costs across many sessions: a channel used for 30 sessions costs ~$0.008/session in gas ([ADR 004](004-tokenomics.md)). Channels extended via `topUp` amortize further since only the initial open and final settle incur gas.

#### Future: Gasless Channel Opens

Two standards can eliminate the requirement for clients to hold native L2 tokens (ETH on Arbitrum) for gas, improving onboarding:

- **ERC-2771 meta-transactions.** A relayer submits the `openChannel` transaction on behalf of the client, paying gas. The client signs an ERC-2771 forwarding request; the relayer recoups gas from the deposit or a separate sponsorship fund. Requires adding a trusted-forwarder check to the contract.
- **ERC-4337 account abstraction.** Smart contract wallets batch USDC approval + channel open into a single user operation. A paymaster can sponsor gas in USDC rather than ETH. Works with unmodified contracts — no changes to `StablePaymentChannel` needed.

Both are deferred to post-PoC. For the PoC, clients must hold both USDC and a small amount of ETH for gas.

### Voucher Interval Negotiation

At the default 1 MB cadence, a 10 GB blob requires 10,000 vouchers — each involving a sign, transmit, verify, and ack cycle. This overhead is unnecessary when the unacknowledged exposure per interval is negligible at typical rates.

**Parameter:** `maxVoucherIntervalMb` is a governable parameter on `StablePaymentChannel` defining the maximum allowed voucher interval in MB. Default: 1 MB. Hardcoded safety bounds: minimum 1 MB, maximum 1024 MB (~1 GB).

**Negotiation semantics:**

1. The client proposes a `voucher_interval_mb` in `StreamRequest` (see [ADR 005](005-protocol.md)).
2. The node responds with its accepted `voucher_interval_mb` in `StreamResponse`. The node may accept the client's proposal, reduce it, or omit the field to fall back to 1 MB.
3. The effective interval for the stream is `min(client_proposed, node_accepted, on-chain maxVoucherIntervalMb)`.

**Backward compatibility:** If `voucher_interval_mb` is absent from `StreamRequest` (older client), the default is 1 MB. If absent from `StreamResponse` (older node), the client assumes 1 MB. The field is optional in both messages.

**Node sovereignty:** A node can always enforce a smaller interval than the negotiated value by stopping delivery after that many MB without receiving a voucher. This uses the existing self-enforcing mechanism — no protocol change needed beyond the negotiation field.

**Enforcement model:** The on-chain `maxVoucherIntervalMb` parameter is advisory — vouchers contain no interval field, so the contract cannot verify what interval was used during off-chain delivery. Enforcement depends on honest client and node software querying the on-chain parameter and capping their negotiation accordingly. This is consistent with other off-chain protocol parameters (e.g., `rate_per_mb` is advertised off-chain and only becomes enforceable when both signed messages are submitted as slash evidence). The governance parameter serves as a coordination point and a signal to implementations, not a contract-level invariant.

**Risk analysis at negotiated intervals:**

| Interval | Floor rate ($0.000001/MB) | Market rate ($0.00001/MB) | Ceiling rate ($0.001/MB) |
| --- | --- | --- | --- |
| 1 MB (default) | $0.000001 | $0.00001 | $0.001 |
| 100 MB | $0.0001 | $0.001 | $0.10 |
| 1024 MB (max) | $0.001024 | $0.01024 | $1.024 |

Even the worst case (1024 MB at ceiling rate) exposes $1.024 — well below the recommended 10 USDC minimum deposit.

Voucher interval negotiation is complemented by per-node `max_blob_size` limits ([ADR 005](005-protocol.md#error-handling-and-retry-semantics)): while interval negotiation reduces per-voucher overhead for large blobs, `max_blob_size` allows nodes to refuse blobs that would create unacceptable resource pressure (cache exhaustion, extended origin pulls) regardless of voucher cadence.

### Concurrent Streams

When multiple streams share a single payment channel, they share a **single cumulative voucher counter**. The rules:

1. **Effective interval = minimum across all active streams.** If stream A negotiated 100 MB and stream B negotiated 1 MB, the channel operates at 1 MB cadence.
2. **Aggregate byte counter.** The client tracks total bytes received across all streams on the channel. A voucher is due whenever the aggregate crosses the next interval boundary.
3. **Interval shrink.** When a new stream joins with a smaller interval than the current effective interval, the client MUST immediately issue a cumulative voucher if the current unvouchered byte count exceeds the new effective interval. Failure to do so causes the node to pause **all** streams on the channel (the self-enforcing threshold is applied collectively, not per-stream).
4. **Voucher routing.** Vouchers are sent on any active stream sharing the channel — the node credits them against the channel-wide counter regardless of which stream carries the message.

**Example:** Client has stream A (blob X, 100 MB interval) and stream B (blob Y, 1 MB interval) on the same channel. Effective interval is 1 MB. After receiving 1 MB total (e.g., 0.7 MB from A + 0.3 MB from B), the client sends a cumulative voucher. If stream B ends, the effective interval rises to 100 MB for the remainder of stream A.

**Isolation recommendation:** Clients fetching a mix of small and large blobs from the same node may benefit from opening separate channels to isolate large-interval streams from small-interval ones.

See [ADR 005 — Payment channels and concurrent streams](005-protocol.md#payment-channels-and-concurrent-streams) for wire-level details.

### Fee Calculation on Disputed Closes

The protocol fee is calculated **at final settlement**, after the dispute window expires, based on the highest valid voucher amount on-chain at that point. The three-step channel close lifecycle is:

1. **`closeChannel`** — callable by client or provider only. Records the submitted voucher's `amount` in `claimedAmount` and `nonce` in `claimedNonce`, sets status to `Closing`, starts the dispute window. **No fee is deducted.** **Zero-voucher close:** when the provider calls `closeChannel` with `amount=0`, `nonce=0`, and an empty signature (`signature.length == 0`) on a channel with `claimedNonce == 0`, the signature verification is skipped — no client-signed voucher is needed. All other `closeChannel` calls — including `amount=0, nonce=0` by the client, or any call with `signature.length > 0` — require normal EIP-712/ECDSA voucher verification. This is safe because voucher nonces start at 1 (nonce 0 is the sentinel for "no voucher submitted"; see [Voucher Nonce Convention](#voucher-nonce-convention)), so any real voucher has nonce ≥ 1 and can always be submitted via `disputeChannel` (which requires strictly higher nonce than `claimedNonce`). The dispute window still applies: if a valid voucher exists, any party can submit it via `disputeChannel`. At settlement, `claimedAmount=0` means the full deposit is refunded to the client and the provider receives nothing.
2. **`disputeChannel`** (during dispute window) — callable by any address. If the submitted voucher has a strictly higher nonce, updates both `claimedAmount` and `claimedNonce` to the new values. Still **no fee deduction**. Submissions with an equal or lower nonce revert with no state change and no fee implications.
3. **`settleChannel`** (after dispute window expires) — callable by anyone. Computes the fee on the final `claimedAmount`, distributes funds, and sets status to `Closed`:
   - Provider receives: `claimedAmount - fee`
   - Treasury receives: `fee = claimedAmount × feePercentage / 10000`
   - Client receives refund: `deposit - claimedAmount`

   > **Invariants:** (1) `closeChannel` and `disputeChannel` MUST revert if the submitted voucher's `amount > channel.deposit`. This prevents client bugs or malicious over-deposit vouchers from causing an underflow revert in `settleChannel` that would lock the channel. (2) `disputeChannel` MUST revert if `newAmount < claimedAmount`. Since vouchers are cumulative, a higher nonce must correspond to a non-decreasing amount; this prevents a malicious client from reducing the provider's payout via a higher-nonce dispute with a lower amount.

   The treasury address receives the full protocol fee as a single transfer. The internal allocation across the four buckets (development fund, bug bounties & audits, ecosystem grants, token buyback & burn — see [ADR 004, Fee Allocation](004-tokenomics.md#fee-allocation)) is handled outside the payment channel contract: manually by the admin key holder in the PoC, and via governance-directed disbursement in production.

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
- Maximum risk per voucher interval at default cadence (1 MB) is $0.00001 at market rate — negligible. At the governance maximum interval (1024 MB) and ceiling rate ($0.001/MB), worst-case risk is $1.024 per interval — still small relative to the recommended 10 USDC minimum deposit (see [Voucher Interval Negotiation](#voucher-interval-negotiation))
- Market-driven rate setting means replication happens organically: profitable content gets cached by more nodes, driving prices down without any coordination protocol
- The `StablePaymentChannel` contract is functionally separated from the `StakingRegistry`, keeping the audit surface for each contract's core logic bounded

**Negative:**

- Clients must hold USDC and native L2 tokens for gas to use the network; this adds an onboarding step compared to a single-token model. At the recommended 10 USDC practical minimum, channel lifecycle gas ($0.23) is 2.3% overhead — acceptable but non-negligible for first-time users. Gasless channel opens via meta-transactions or account abstraction can eliminate the native token requirement post-PoC (see [Deposit Economics](#deposit-economics))
- Rate volatility: a node can change its advertised rate between a probe and a stream request; the `StreamResponse` rate is the binding one, but a client that probed at one rate and receives a higher rate in `StreamResponse` must disconnect and re-probe rather than having been deceived silently. Rate changes more than 30 seconds after the probe are not slashable; the 30-second window is precisely defined as `stream_response.timestamp_us >= probe_response.timestamp_us && stream_response.timestamp_us - probe_response.timestamp_us < 30_000_000` using requester-anchored timestamps in both signed messages (see ADR 005)
- USDC is issued by Circle, which can freeze specific addresses or blacklist the contract. For the PoC this risk is accepted; multi-token payment support to mitigate it is deferred to [ADR 010](010-multi-token.md)
- BLAKE3 verification on EVM requires an intermediate Merkle proof scheme for PoC-era slash evidence; a client submitting a slash claim cannot directly prove BLAKE3 mismatch on-chain

## Attack Vectors

### Client-side

**Voucher withholding**
Client receives bytes but stops signing vouchers, getting content for free up to the last signed interval.

The self-enforcing stop is sufficient. Maximum loss is one voucher interval at the negotiated cadence. At the default cadence (1 MB × market rate ≈ $0.00001), risk is negligible. At a negotiated interval of 100 MB at market rate, loss is ~$0.001. At the governance maximum (1024 MB) at ceiling rate, loss is ~$1.024 — still economically negligible relative to channel deposits. Nodes serving high-value content can unilaterally enforce smaller intervals regardless of what was negotiated. No additional mechanism needed — this is fully addressed by the protocol design.

---

**Channel griefing**
Client opens many channels with minimum deposit and never streams, forcing nodes to track and eventually close stale channels.

**Resolved: provider-initiated zero-voucher close.** The provider can call `closeChannel` with `amount=0, nonce=0`, and an empty signature (`signature.length == 0`) on any channel where no vouchers have been submitted (`claimedNonce == 0`), immediately entering the close→dispute→settle lifecycle. This bounds the maximum tracking duration to the dispute window (48 hours PoC default) rather than the full 90-day channel expiry. The dispute window protects clients — if a valid voucher exists, the client or a watchtower can submit it via `disputeChannel`. At settlement, the full deposit is refunded to the client. No additional inactivity timer or separate expiry mechanism beyond the existing channel expiry / `reclaimExpired` path is needed; that existing escape hatch remains required for cases where the provider disappears without initiating a close.

The financial cost to the attacker remains bounded: at the recommended 10 USDC practical minimum, an attacker spending $1,000 opens 100 channels; the provider closes them all immediately and each settles after the dispute window with full refund to the attacker (no profit motive) and ~$0.18 gas cost to the provider per channel (close + settle). The provider's total gas exposure is ~$18 for 100 griefing channels — significant enough to warrant additional mitigations for high-volume attacks:

- **Option A — On-chain channel cap per address.** The `StablePaymentChannel` contract enforces a maximum number of open channels per client Ethereum address (e.g., 10). Hard to circumvent without new wallet addresses, each requiring on-chain funding.
- **Option B — Node-side filtering.** Nodes refuse `StreamRequest` from channels that have been open longer than N days with zero vouchers. Off-chain, no contract change needed, but relies on node operator implementation.

---

**Stale close**
Client submits an old voucher (lower amount) to close the channel, underpaying the node.

The dispute window (default 48 hours for PoC — raised from 24 hours to account for L2 forced inclusion delay; see [ADR 007](007-watchtower.md#l2-sequencer-censorship)) works if the node is online. The gap is liveness: if the node goes offline after a stale close is submitted and misses the dispute window, it loses the difference. Production deployments add a forced-inclusion deadline extension mechanism ([ADR 007](007-watchtower.md#l2-sequencer-censorship)) that provides additional safety margin, though the dispute window must still exceed the L2's maximum forced-inclusion delay for the extension to be effective. Options:

- **Option A — Watchtowers.** A separate monitoring service holds the latest voucher and submits it on the node's behalf if a dispute is detected. Adds operational complexity but fully closes the gap.
- **Option B — Longer dispute window.** Increase beyond 48 hours (up to the 72h governance max), giving operators more time to respond. Delays legitimate channel closes for everyone.
- **Option C — Persistent monitoring process.** The node binary runs a lightweight dispute monitor as a separate thread that only watches the chain for close events, independent of the serving process. Simpler than a watchtower but still single-node.

---

**Probe fishing**
Client sends probe requests to many nodes at high frequency to map the network or exhaust node resources without ever paying.

The current mitigation is weak. Clients are not staked — their NodeIds are free to rotate — so per-NodeId rate limiting is bypassable. The iroh connection setup cost is also low. Options:

- **Option A — IP-based rate limiting.** Rate limit probe requests by source IP rather than NodeId. Harder to rotate at scale, though not impossible with proxies or cloud infrastructure.
- ~~**Option B — Require an open channel to probe.**~~ **Rejected.** This creates a bootstrap catch-22: clients need probe results (rate, latency) to choose a node before opening a channel, but Option B requires a channel before probing. Since probes happen before channel opens (see [ADR 005](005-protocol.md) probe flow), requiring a channel is architecturally incompatible with the protocol sequence. Probes are unauthenticated and free — see ADR 005's statement that "`ProbeRequest` requires no authentication."
- **Option C — Proof-of-work on probe requests.** Include a small PoW challenge in the probe request (e.g., find a nonce such that `hash(NodeId || nonce) < difficulty`). Adds CPU cost to bulk probing without affecting honest single-request clients noticeably.
- **Option D — Accept the risk (PoC default).** A probe is a single message exchange. The cost to serve one is negligible; the attack only matters at extreme scale. Rate limit at the connection level (iroh handles this) and monitor for abuse rather than trying to prevent it at the protocol level.

**Note:** Probe responses are considered public information (see ADR 005). The concern here is resource exhaustion from bulk probing, not information leakage — content availability is discoverable via probing (see ADR 005), and pricing is revealed in probe/stream responses by design.

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

**Resolved: slashable offense.** Both `ProbeResponse` and `StreamResponse` include cryptographic signatures over the advertised rate (see ADR 005). The on-chain verifier checks: (1) both signatures are valid and from the same NodeId, (2) `StreamResponse.rate_per_mb > ProbeResponse.rate_per_mb`, (3) `stream_response.timestamp_us >= probe_response.timestamp_us` (prevents unsigned underflow), and (4) `stream_response.timestamp_us - probe_response.timestamp_us < 30_000_000` (30 seconds in microseconds). Both `timestamp_us` values are requester-generated — the probe timestamp is echoed in `ProbeResponse`, and a separate requester timestamp from `StreamRequest` is echoed in `StreamResponse` — so the delta is computed from a single clock with no wall-clock reference or time oracle needed. **Clock skew immunity:** because both timestamps originate from the requester's clock (the node merely echoes them back in its signed response), clock skew between the requester and the node is irrelevant. The on-chain verifier never compares timestamps from different clocks — it only computes the delta between two requester-generated values extracted from signed messages. A node with a clock 5 minutes ahead or behind has zero effect on the 30-second window check. The node is slashed per the escalating schedule in ADR 004. The 30-second window allows legitimate rate changes between sessions while catching same-session bait-and-switch.

---

**Phantom blob announcement**
Node announces a blob as cached then fails or redirects on actual request.

**Resolved: slashable offense.** The `ProbeResponse` includes a cryptographic signature over `{hash, has_blob, rate_per_mb, timestamp_us}` (see ADR 005). Two evidence paths exist:

- **Signed refusal:** If the node returns a signed `StreamResponse` with `ok: false` or a redirect for the same blob hash, and `stream_response.timestamp_us >= probe_response.timestamp_us` and `stream_response.timestamp_us - probe_response.timestamp_us < 30_000_000` (30 seconds), the two signed messages constitute on-chain-verifiable evidence. Both `timestamp_us` values are requester-generated, so the delta is computed from a single clock with no wall-clock reference needed.
- **Timeout / non-response:** If the node accepts the connection but never sends a `StreamResponse` (or drops the QUIC stream), there is no second signed message. The signed `ProbeResponse` alone is not sufficient for on-chain slashing. This case is handled by reputation penalties (immediate score reduction) rather than on-chain slashing — the absence of a signed response is not provable on-chain.

The 30-second validity window is long enough for normal protocol flow (probe, selection, channel open, stream request). To prevent legitimate cache eviction from producing false slash evidence within this window, nodes MUST implement a **probe-triggered eviction hold**: when signing `has_blob: true` in a `ProbeResponse`, the node pins the blob against LRU/LFU eviction for at least `probe_hold_duration` (currently 35 seconds: the 30-second slashing window plus 5-second margin). A node that cannot guarantee the hold (hold budget exhausted or cache under extreme pressure) MUST respond `has_blob: false`. See [ADR 005, Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold) for implementation requirements. This design treats `has_blob: true` as a cryptographic availability commitment backed by a local resource reservation, consistent with the existing principle that nodes MUST NOT sign `has_blob: true` for blobs exceeding `max_blob_size` ([ADR 005](005-protocol.md#error-handling-and-retry-semantics)). The node is slashed per the escalating schedule in ADR 004. The challenged node has a 24-hour window to counter by proving it delivered the blob (signed delivery receipt from the same requester within the relevant time window). **Dependent parameters:** The probe cache TTL in [ADR 001](001-network.md) is set to half this 30-second window (15 seconds) to guarantee cached probes remain slashable. The `probe_hold_duration` ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)) is set to this window plus 5 seconds. Changes to this window must be coordinated with both the probe cache TTL and the hold duration.

**Edge case: hold violation.** If a node's eviction hold fails due to an implementation bug, operator misconfiguration, or extreme memory pressure (OOM), and the node signs `has_blob: true` but later returns `ok: false`, the existing 24-hour counter-window applies. However, eviction logs are self-generated and not on-chain verifiable, so the only valid counter-evidence remains proving delivery of the same blob to the same requester within the relevant time window. A node that experiences hold violations should increase its `max_probe_holds` budget, increase its cache size, or accept the slash as the cost of under-provisioning. This is intentional: the protocol does not subsidize under-provisioned nodes at the expense of slashing deterrence.

---

**Channel close front-running**
Node monitors the mempool and front-runs a client's channel close with a higher voucher submission.

Not a real attack. The contract always settles the highest valid voucher, and only the client can sign a valid voucher. A node submitting the latest voucher before the client is the intended happy path. Fabricating a higher voucher requires forging the client's ECDSA signature, which is cryptographically infeasible.

---

**Third-party forced channel close (DoS)**
A third party holding a valid voucher calls `closeChannel` to force the channel from `Open` to `Closing`, halting delivery.

**Resolved: access control restriction.** `closeChannel` requires `msg.sender == channel.client || msg.sender == channel.provider`. Third parties cannot initiate a close regardless of whether they hold a valid voucher. Watchtower functionality is unaffected — watchtowers operate via `disputeChannel` during the dispute window. The residual risk is a `disputeChannel` call with an intercepted voucher, which can only *improve* the settlement (higher nonce required). On-path network interception of vouchers is mitigated by QUIC transport (TLS 1.3), though this does not address endpoint compromise or other forms of leakage.

---

### Network-level

**Eclipse attack**
Attacker surrounds a client with malicious nodes so all probe responses come from nodes under attacker control.

BLAKE3 verification catches data corruption regardless of which nodes are in the peer table. The remaining gap is a denial-of-service variant: an attacker controlling all of a client's known nodes can simply refuse to serve. Options:

- **Option A — Origin-backed nodes as fallback.** Clients can specifically query the registry for well-known origin-backed nodes for a given blob, bypassing the general peer table. An eclipse must also control all origin-backed nodes for the target content — which requires capital proportional to the number of origin-backed nodes for that content.
- **Option B — Multi-source bootstrap.** Clients discover initial peers from at least two independent sources (on-chain registry + a hardcoded DNS seed list). An attacker must compromise both to fully eclipse a client.
- **Option C — Minimum honest-peer diversity.** Clients maintain connections to at least N nodes discovered via different paths. All N would need to be attacker-controlled for a full eclipse.

---

**Gossip flooding**
Node sends high-volume `NodeAnnounce` messages to exhaust peer table memory or crowd out legitimate announcements.

Registry check + per-sender rate limiting is solid. The minor gap is that the local registry cache may be up to 10 minutes stale, briefly allowing recently-unstaked nodes to flood. Mostly solved; no strong alternative needed beyond tightening the registry cache refresh on high flood detection.

---

**Sybil nodes**
Attacker stakes many cheap nodes to dominate probe responses for popular content, controlling pricing in a region.

The core weakness is token-price dependency: at $0.001/TOKEN, a minimum stake of 1,000 TOKEN costs $1 per sybil node. The unified selection score `rate_per_mb × rtt_ms × (1 / max(reputation, 0.1)²)` (see [ADR 001](001-network.md#node-selection-algorithm)) helps — a sybil fleet must be real hardware in the right geography, competitively priced, and build reputation over time — but does not eliminate the risk when the token is cheap. Options:

- **Option A — Governance raises minimum stake if token price falls.** The minimum stake is governable. Token holders are incentivised to raise it to protect the network, since a sybil-dominated network reduces usage and token value. Reactive but aligned.
- **Option B — Minimum stake denominated in USD equivalent via oracle.** Requires a price oracle, which introduces oracle dependency, manipulation, and downtime risks (see rate bounds discussion above). The same concerns apply here, but the impact of oracle failure is lower (new stakers temporarily blocked, not payments broken).
- **Option C — Reputation as a second filter.** New nodes (low reputation, few settled channels) are deprioritised in client selection even if their `rate_per_mb × rtt_ms × (1 / max(reputation, 0.1)²)` score is competitive. A sybil fleet takes time to build reputation, limiting its effectiveness during that window.

---

**Rate manipulation cartel**
Colluding nodes in a region hold rates artificially high.

Origin-backed nodes set the effective price ceiling for any blob. Clients can always probe origin-backed nodes directly and pay their rates as a guaranteed fallback. Any node outside the cartel that undercuts wins all local traffic — the incentive to defect is strong. New entrants can join permissionlessly by staking.

---

**Content withholding**
A node stakes, responds to probes with `has_blob: true`, but refuses to serve — collecting credibility in the peer table without actually participating.

**Withholding is not a slashable offense** — operators may legitimately take content offline for maintenance, migration, or business reasons, and slashing for availability creates perverse incentives. Instead, withholding is handled through reputation and redundancy:

- **Multiple origin-backed nodes per blob.** Content owners configure multiple origin-backed nodes for important content. A single withholding node becomes irrelevant if others serve the same blob.
- **Reputation fast-path.** Nodes that respond `has_blob: true` to probes but fail to deliver accumulate reputation penalties at a steeper rate. A node with consistently poor availability is deprioritized in provider selection and loses delivery revenue.

Note: the probe-triggered eviction hold ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)) addresses a related but distinct problem. Withholding is a node that has the blob but refuses to serve it (behavioral — handled by reputation). The eviction hold addresses a node that signed `has_blob: true` but lost the blob to cache pressure before the stream request (mechanical — prevented by the hold and, if the hold fails, treated as a slashable phantom announcement).

---

**Replay attack on vouchers**
Attacker intercepts a signed voucher and attempts to replay it against a different channel or after close.

Fully solved. EIP-712 typed data over `{channelId, amount, nonce, token}` binds the voucher to a specific channel. The EIP-712 domain separator (see [EIP-712 Voucher Signature](#eip-712-voucher-signature)) further binds each voucher to a specific chain and contract deployment, preventing replay across different L2s, contract upgrades, or test vs production environments. The monotonically increasing nonce (starting at 1; see [Voucher Nonce Convention](#voucher-nonce-convention)) prevents resubmission after settlement.

## Contract Interfaces

### StablePaymentChannel

The `StablePaymentChannel` is the PoC payment channel contract, handling USDC-only payment channels. In production, this contract is superseded by the multi-token `PaymentChannel` contract defined in [ADR 010](010-multi-token.md).

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

**Channel ID:** `channelId = keccak256(abi.encodePacked(client, provider, channelNonce))` where `channelNonce` is a monotonic per-client counter stored on-chain as `clientChannelNonce[msg.sender]`. **Ordering:** `openChannel` reads the current nonce, uses it to compute `channelId`, then increments: `n = clientChannelNonce[msg.sender]; channelId = keccak256(..., n); clientChannelNonce[msg.sender] = n + 1`. The client pre-computes the next channelId off-chain by reading `clientChannelNonce[client]` and using that value directly — no off-by-one because the contract uses the same value before incrementing. The `channelNonce` is global per-client (not per-provider), ensuring uniqueness across all of a client's channels.

> **Terminology:** `channelNonce` (the channel creation counter) is distinct from the voucher `nonce` (the monotonic sequence number within a channel used in EIP-712 voucher signatures). The former uniquely identifies channels; the latter orders vouchers within a channel. [ADR 010](010-multi-token.md) extends this formula to `keccak256(client, provider, token, channelNonce)` for multi-token support. In implementation, consider naming the on-chain mapping `clientChannelCounter` to avoid confusion with voucher nonces.

```solidity
interface IStablePaymentChannel {
    // Channel nonce tracking (see "Channel ID" above for terminology)
    function clientChannelNonce(address client) external view returns (uint256);

    // Channel lifecycle (openChannel increments clientChannelNonce[msg.sender] and uses it in channelId)
    function openChannel(address provider, uint256 deposit) external returns (bytes32 channelId);
    function topUp(bytes32 channelId, uint256 additionalDeposit) external;
    function closeChannel(bytes32 channelId, uint256 amount, uint256 nonce, bytes calldata signature) external;
    function disputeChannel(bytes32 channelId, uint256 amount, uint256 nonce, bytes calldata signature) external;
    function settleChannel(bytes32 channelId) external;
    function reclaimExpired(bytes32 channelId) external;

    // Views
    function getChannel(bytes32 channelId) external view returns (Channel memory);
    function getEffectiveFee(address provider) external view returns (uint256 bps);
    function getRateBounds() external view returns (uint256 deliveryFloor, uint256 deliveryCeiling);

    // Governance
    function setFeePercentage(uint256 bps) external;
    function setDiscountedFeePercentage(uint256 bps) external;
    function setTreasuryAddress(address treasury) external;
    function setMinDeposit(uint256 amount) external;
    function setDisputeWindow(uint256 seconds_) external;
    function setRateBounds(uint256 deliveryFloor, uint256 deliveryCeiling) external;
    function setMaxVoucherIntervalMb(uint256 mb) external;
}
```

> **Reentrancy protection:** All state-mutating functions that perform external calls (ERC-20 transfers) — `openChannel`, `topUp`, `settleChannel`, `reclaimExpired` — MUST use `nonReentrant` guards and follow checks-effects-interactions. This is especially critical for the production multi-token contract ([ADR 010](010-multi-token.md)) which accepts arbitrary governance-approved tokens.

**`topUp` behavior:** `topUp(channelId, additionalDeposit)` adds funds to an open channel:
- **Status precondition:** MUST require status `Open` and `block.timestamp < expiresAt` (reverts on `Closing`, `Closed`, or expired).
- **Caller:** client only (`require(msg.sender == channel.client)`).
- **Effects:** Transfers `additionalDeposit` from `msg.sender` to the contract via `safeTransferFrom`. Updates `channel.deposit += additionalDeposit`. Does NOT extend `expiresAt` (to prevent indefinite lock-in — the channel's utility is bounded by the initial `maxChannelDuration`).
- **Modifiers:** `nonReentrant`.
- **Emits:** `ChannelToppedUp(channelId, additionalDeposit, newDeposit)`.

**Initial deployment values.** The constructor (or initializer for proxy deployments) sets governable parameters to their PoC defaults. All values are within the hardcoded safety bounds table further below (see also [ADR 009](009-governance.md) for governance ranges):

```solidity
constructor(address usdc_, address treasury_, uint256 disputeWindow_) {
    require(disputeWindow_ >= 43200 && disputeWindow_ <= 259200, "out of bounds");
    usdc = usdc_;
    treasury = treasury_;
    disputeWindow = disputeWindow_;   // PoC default: 172800 (48 hours)
    feePercentage = 300;              // 3% (300 bps)
    discountedFeePercentage = 150;    // 1.5% (150 bps)
    maxVoucherIntervalMb = 1;         // 1 MB
    maxChannelDuration = 7776000;     // 90 days
}
```

Default PoC deployment value for `disputeWindow`: **172800 seconds (48 hours)** — raised from 24 hours to guarantee effective dispute response time under L2 sequencer censorship (see [ADR 007](007-watchtower.md#l2-sequencer-censorship)). Safety bounds per [ADR 009](009-governance.md): 43200–259200 seconds (12h–72h).

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

event ChannelExpiredReclaimed(
    bytes32 indexed channelId,
    address indexed client,
    uint256 deposit
);

event ChannelToppedUp(
    bytes32 indexed channelId,
    uint256 additionalDeposit,
    uint256 newDeposit
);

// Governance events (emitted by setRateBounds)
event RateBoundsUpdated(
    uint256 newDeliveryFloor,
    uint256 newDeliveryCeiling
);
```

**Channel expiry:** `expiresAt` is set at channel open: `expiresAt = block.timestamp + maxChannelDuration`. The `maxChannelDuration` parameter defaults to 90 days and is governable within hardcoded bounds (minimum 7 days, maximum 365 days). Channel expiry protects clients from indefinitely locked funds when a node disappears without closing the channel.

**Channel close lifecycle:**
- `closeChannel` → requires status `Open`. **Callable by `channel.client` or `channel.provider` only** (`require(msg.sender == channel.client || msg.sender == channel.provider)`). Sets status to `Closing`, records voucher, emits `ChannelCloseInitiated`. No fund transfers. Third parties (including watchtowers) cannot initiate a close — they act only via `disputeChannel` (during the dispute window) or `settleChannel` (after expiration). **Zero-voucher close:** when the provider calls with `amount == 0`, `nonce == 0`, an empty signature (`signature.length == 0`), and `channel.claimedNonce == 0`, the voucher signature is not verified — this is the provider's mechanism for releasing channels where no vouchers were ever signed. Since voucher nonces start at 1, any real voucher has a strictly higher nonce than the recorded `claimedNonce=0`, so `disputeChannel` works normally. The dispute window applies; a client or watchtower holding a real voucher can dispute.
- `disputeChannel` → requires status `Closing` and `block.timestamp < disputeDeadline`. Callable by any address holding a valid voucher with a strictly higher nonce. Updates `claimedAmount`, emits `ChannelDisputed`. No fund transfers. Unrestricted caller access is intentional: watchtowers and other third parties must be able to submit higher-nonce vouchers on behalf of an offline party during the dispute window.
- `settleChannel` → requires status `Closing` and `block.timestamp >= disputeDeadline`. Callable by any address. Computes fee on final `claimedAmount`, transfers funds to provider/treasury/client, sets status to `Closed`, emits `ChannelSettled`.
- `reclaimExpired` → requires status `Open` and `block.timestamp >= expiresAt`. Returns the full deposit to the client (no fee deducted — no voucher was submitted). Sets status to `Closed`, emits `ChannelExpiredReclaimed`. Callable by the client or the provider. Regardless of caller, the full deposit is returned to `channel.client` — the provider cannot claim funds via this path. This ensures abandoned channels where the client is absent can be cleaned up by the provider to free on-chain state.

**Safety bounds (hardcoded):**

| Parameter | Minimum | Maximum |
| --- | --- | --- |
| Fee percentage | 0 bps (0%) | 2000 bps (20%) |
| Dispute window | 43200 seconds (12 hours) | 259200 seconds (3 days) |
| Min deposit | 1 base unit | No max |
| Rate floor | 1 base unit | Must be < ceiling |
| Rate ceiling | Must be > floor | No max |
| Max voucher interval | 1 MB | 1024 MB (~1 GB) |
| Max channel duration | 604800 seconds (7 days) | 31536000 seconds (365 days) |

**Rate bounds are in USDC base units (6 decimals) for the PoC.** The contract stores a single `RateBounds` struct with `deliveryFloor` and `deliveryCeiling`. Per-token rate bounds are deferred to [ADR 010](010-multi-token.md).

**Initial rate bounds (PoC):**

| Parameter | Value (USD/MB) | USDC base units | Rationale |
| --- | --- | --- | --- |
| `deliveryFloor` | $0.000001/MB | 1 | Anti-abuse minimum; 10× below expected market rate. Prevents zero-rate free-riding while imposing no practical constraint on legitimate pricing. Nodes are expected to set rates well above this floor; the floor is purely an anti-zero safeguard, not a recommended price. |
| `deliveryCeiling` | $0.001/MB | 1,000 | 100× expected market rate. Accommodates origin-backed nodes with high-egress backends (e.g., S3 at $0.09/GB) while remaining well above any legitimate pricing scenario ($1.00/GB vs Akamai's ~$0.12–0.20/GB). |

The expected market rate is $0.00001/MB (10 USDC base units per MB, or $0.01/GB). This positions deCDN ~4–9× cheaper than major traditional CDNs (CloudFront at $0.085/GB, KeyCDN at $0.04/GB) and at parity with budget providers (Bunny.net at $0.01/GB). Both bounds are governable post-PoC within the hardcoded safety constraints above.

### Rate Bounds Refresh

Nodes must keep their local copy of `RateBounds` current so that advertised `rate_per_mb` values stay within governance-set bounds. Because rate bounds are advisory coordination parameters — the contract does not verify rate compliance during settlement or slashing — the refresh strategy is lighter-touch than the content blacklist ([ADR 011](011-content-takedown.md)), where serving blacklisted content is a slashable offense.

**Primary mechanism: event listening.** Nodes SHOULD subscribe to `RateBoundsUpdated` events on the `StablePaymentChannel` contract. On receiving the event, the node updates its local rate bounds cache immediately. Event listening is the recommended approach because governance actions are infrequent (days to weeks between changes), making high-frequency polling wasteful.

**Fallback mechanism: periodic polling.** Nodes MUST poll `getRateBounds()` at a configurable interval (`rate_bounds_poll_interval`, default **1 hour** for PoC). This guards against missed events due to RPC provider issues, WebSocket disconnections, or chain reorganizations. The 1-hour default is deliberately longer than the 10-minute intervals used for the on-chain registry ([ADR 001](001-network.md)) and content blacklist ([ADR 011](011-content-takedown.md)): registry freshness is connectivity-critical, blacklist freshness is slashing-critical, but rate bounds staleness only risks counterparties rejecting the node's advertised rate.

**Startup.** Nodes MUST call `getRateBounds()` before accepting connections, ensuring the node never operates without rate bounds. This follows the same pattern as the content blacklist initial sync ([ADR 011](011-content-takedown.md)). Because `getRateBounds()` returns `uint256` values but the wire protocol represents `rate_per_mb` as `u64` ([ADR 010](010-multi-token.md)), nodes MUST verify that both `deliveryFloor` and `deliveryCeiling` fit within `u64` on every refresh (startup and subsequent polls/events). If either bound exceeds `u64::MAX`, the node MUST refuse to start (or, on a mid-operation refresh, continue with its last valid bounds and log an error). In practice this is unreachable — the PoC ceiling is 1,000 base units — but the check guards against governance misconfiguration.

**Stale bounds.** If the event subscription is lost and RPC polling fails, the node SHOULD continue operating with its last-known bounds and log a warning. No service interruption is required. The worst-case consequence of stale bounds is that counterparties running compliant software reject the node's `rate_per_mb` as out-of-bounds — a revenue impact, not a safety violation.

**No version-based delta pattern.** Unlike the content blacklist (which uses `getBlacklistVersion()` for cheap change detection and incremental delta fetching), rate bounds are a single struct containing two `uint256` values. A version counter adds no value — the full state is readable in a single `eth_call` with negligible overhead. This is an intentional divergence from the ADR 011 pattern.

**Multi-token extension.** The PoC uses a single `RateBounds` struct. When per-token rate bounds are introduced ([ADR 010](010-multi-token.md)), the `RateBoundsUpdated` event will need a token parameter: `RateBoundsUpdated(address indexed token, uint256 newDeliveryFloor, uint256 newDeliveryCeiling)`. Nodes will subscribe with a token filter or listen for all tokens and update their local cache accordingly.

For how nodes validate `rate_per_mb` against cached bounds before signing protocol messages, see [ADR 005 — Rate Bounds Validation](005-protocol.md#rate-bounds-validation).

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

This interface is the canonical specification for `BuybackBurner`. [ADR 004](004-tokenomics.md) defines the economic parameters; parameter names in ADR 004 reference this interface (e.g., `slippageBps` corresponds to `setSlippageTolerance(uint256 bps)` above).

`executeBuyback` is callable by governance multisig or the authorized `keeper` address. All `set*` functions are governance-only behind a timelock.

**PoC note:** The `BuybackBurner` contract is deployed with the same interface, but `executeBuyback` is not called during the PoC. The admin key holder (PoC) or an authorized governance action (production) transfers the 20% buyback allocation to the contract periodically, but fees accumulate there without being swapped. See [ADR 004](004-tokenomics.md#buybackburner-contract) for activation criteria.

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

### Voucher Nonce Convention

Voucher nonces within a channel start at **1**. Nonce 0 is reserved as the sentinel value meaning "no voucher has been submitted" — it is the Solidity default for `claimedNonce` in a newly opened `Channel` struct. The first client-signed voucher in a channel uses `nonce=1`, the second uses `nonce=2`, and so on. This convention ensures:

- `claimedNonce == 0` reliably identifies channels where no voucher has ever been submitted, which is the guard condition for the provider-initiated zero-voucher close path (see [Fee Calculation on Disputed Closes](#fee-calculation-on-disputed-closes)).
- Any real voucher (nonce ≥ 1) can always be used to dispute a zero-voucher close (which records `claimedNonce=0`), since `disputeChannel` requires strictly higher nonce.

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
        return discountedFeePercentage; // e.g. 150 bps (1.5%) when base is 300 bps (3%)
    }
    return feePercentage;
}
```

`discountedFeePercentage` is a separate governable parameter (set via `setDiscountedFeePercentage(uint256 bps)`) with safety bounds: must be ≤ `feePercentage`, minimum 0 bps. This avoids integer truncation from dividing odd fee values and allows the discount to be tuned independently of the base fee. PoC default: 150 bps (half of the 300 bps base fee).

This ensures the discount threshold (currently 10 × 1,000 = 10,000 TOKEN) stays correct if governance changes `minStake`.

```solidity
using SafeERC20 for IERC20;

// Client staking (optional, no slashing)
mapping(address => uint256) public clientStakes;

event ClientStaked(address indexed client, uint256 amount, uint256 newTotal);
event ClientUnstaked(address indexed client, uint256 amount, uint256 newTotal);

function clientStake(uint256 amount) external nonReentrant {
    token.safeTransferFrom(msg.sender, address(this), amount);
    clientStakes[msg.sender] += amount;
    emit ClientStaked(msg.sender, amount, clientStakes[msg.sender]);
}

function clientUnstake(uint256 amount) external nonReentrant {
    require(clientStakes[msg.sender] >= amount);
    clientStakes[msg.sender] -= amount;
    token.safeTransfer(msg.sender, amount);
    emit ClientUnstaked(msg.sender, amount, clientStakes[msg.sender]);
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

**NodeId-to-address mapping:** See [NodeId-to-Ethereum Binding](#nodeid-to-ethereum-binding) for the full specification of how iroh NodeIds are bound to Ethereum addresses.

This is a soft signal, not a hard gate. Non-staking clients still get served, just with lower priority during congestion.

## NodeId-to-Ethereum Binding

The protocol requires a verifiable mapping between iroh NodeIds (ed25519 public keys) and Ethereum addresses (secp256k1-derived). This binding is used for client priority staking lookups, payment channel association, and slash evidence attribution.

### Binding Message Format

The binding uses EIP-712 typed structured data, signed by the Ethereum private key:

```solidity
bytes32 constant BIND_NODE_TYPEHASH = keccak256(
    "BindNodeId(bytes32 nodeId,uint64 nonce)"
);
```

Where:
- `nodeId`: the 32-byte ed25519 public key (iroh `NodeId`)
- `nonce`: a monotonic counter per Ethereum address, preventing replay of revoked bindings

The EIP-712 domain separator is the same as the `StakingRegistry` contract deployment (chain ID + contract address), preventing cross-chain and cross-contract replay.

### On-Chain Registration

Nodes register their binding on-chain via `StakingRegistry.bindNodeId()`. This is distinct from `StakingRegistry.registerNode()` ([ADR 001](001-network.md)), which handles mesh membership (NodeId, multiaddrs, region, stake validation). `bindNodeId()` establishes the cryptographic NodeId-to-Ethereum-address binding used for slash evidence and payment channel attribution. Nodes call both at registration time: `registerNode` to join the peer mesh, then `bindNodeId` to create the signed binding.

**Canonical source of truth:** The `nodeIdToAddress` / `addressToNodeId` mappings maintained by `bindNodeId` are the authoritative source for payment attribution and slashing. `NodeInfo.ethAddress` in ADR 001 is always `msg.sender` (the same address that calls `bindNodeId`), so the two are consistent by construction under the one-to-one constraint. If the implementation stores both, `NodeInfo.ethAddress` MUST equal `nodeIdToAddress[nodeId]` at all times.

This creates an authoritative, publicly queryable mapping:

```solidity
// StakingRegistry additions
mapping(bytes32 => address) public nodeIdToAddress;
mapping(address => bytes32) public addressToNodeId;
mapping(address => uint64) public bindingNonce;

function bindNodeId(bytes32 nodeId, bytes calldata signature) external {
    uint64 nonce = bindingNonce[msg.sender];
    bytes32 digest = keccak256(abi.encodePacked(
        "\x19\x01",
        DOMAIN_SEPARATOR,
        keccak256(abi.encode(BIND_NODE_TYPEHASH, nodeId, nonce))
    ));
    require(ECDSA.recover(digest, signature) == msg.sender, "invalid signature");

    // Reject if nodeId is already bound to a different address
    address existingOwner = nodeIdToAddress[nodeId];
    require(existingOwner == address(0) || existingOwner == msg.sender, "NodeId bound to another address");

    // Clear caller's previous binding if exists
    bytes32 oldNodeId = addressToNodeId[msg.sender];
    if (oldNodeId != bytes32(0)) {
        delete nodeIdToAddress[oldNodeId];
    }

    nodeIdToAddress[nodeId] = msg.sender;
    addressToNodeId[msg.sender] = nodeId;
    bindingNonce[msg.sender] = nonce + 1;

    emit NodeIdBound(msg.sender, nodeId, nonce);
}

function resolveNodeId(bytes32 nodeId) external view returns (address) {
    return nodeIdToAddress[nodeId];
}
```

> **Note on EIP-712 signature in `bindNodeId`:** The signature is technically redundant for direct on-chain calls (where `msg.sender` already authenticates the caller) but is retained to support future meta-transaction/relayer patterns where a third party submits the binding on behalf of the node operator.

### Off-Chain (Ephemeral) Binding for Clients

Clients who do not wish to register on-chain (e.g., for priority staking lookups only) include a signed binding in their `StreamRequest`. The node verifies the EIP-712 signature over `BindNodeId(nodeId, nonce=0)` using `ecrecover`, confirms the recovered address matches the claimed Ethereum address, and uses that address for `clientStakeOf` lookups. This ephemeral binding is not stored on-chain and is valid only for the session.

### Binding Requirements by Role

| Role | On-chain binding required? | Rationale |
| --- | --- | --- |
| Node (staked) | **Yes** — must call `bindNodeId` at registration | Slash evidence references on-chain NodeId→address mapping |
| Client (priority staking) | No — ephemeral binding in `StreamRequest` is sufficient | Priority staking is a soft signal; no on-chain enforcement needed |
| Client (opening channels) | No — channel `client` field is the Ethereum address directly | Channel operations use Ethereum addresses, not NodeIds |

### Rebinding

A node or client can rebind their Ethereum address to a new NodeId by calling `bindNodeId` again (the nonce increments, invalidating the old binding). The old NodeId→address mapping is deleted. This supports key rotation scenarios (e.g., compromised iroh key).

## Decimal Handling

USDC uses 6 decimals; TOKEN uses 18 decimals. All payment amounts in the `incentive` crate use USDC base units (µUSDC). The voucher signing code uses raw base units — no decimal conversion in the signature path to avoid precision bugs.

Multi-token decimal abstraction (a `Currency` enum covering arbitrary ERC-20 decimals) is deferred to [ADR 010](010-multi-token.md).

**Voucher format:**

```
{channelId, amount, nonce, token, signature}
```

During delivery over `cdn/client/v1`, `{signature, amount, nonce}` are transmitted on the wire; the remaining fields (`channelId`, `token`) are derived from stream context. The `nonce` is explicit to prevent desynchronization if a `VoucherAck` is dropped (it starts at 1 for the first voucher in a channel; 0 is reserved as a sentinel). See [ADR 005](005-protocol.md) for wire protocol details.

The `token` field (ERC-20 address) is included in the signed EIP-712 typed data to prevent cross-token replay attacks. For the PoC, this field is hardcoded to the USDC contract address. The full EIP-712 type definition and domain separator are specified in [EIP-712 Voucher Signature](#eip-712-voucher-signature).

## Slashing and Channel Interactions

Slashing and payment channels are independent by design. The following interactions apply regardless of which governance-approved tokens are in use (see [ADR 010](010-multi-token.md)).

**Slashing does not affect channel funds.** Slashing operates exclusively on TOKEN stake in the `StakingRegistry` (see [ADR 004](004-tokenomics.md#slash-amounts-escalating)). Funds deposited into payment channels are client deposits held in escrow — they are not stake and are never touched by slashing. This follows directly from the functional separation described in [Consequences](#consequences): payment channel contracts never hold or move TOKEN stake, cannot be called by `StakingRegistry` to slash or reassign stake, and any `StakingRegistry` interaction is read-only (e.g., computing fee discounts based on stake multiples).

**Slashing can drop a node below minimum stake while channels are open.** Because channel deposits are independent of stake, a node can be slashed below the minimum stake requirement (or even to zero) while it has open channels. The channels continue their normal lifecycle — close, dispute window, settle — regardless of the node's staking status. Channel settlement is purely a function of the voucher state, not the node's registry status.

**Auto-ejection does not interrupt open channels.** When a node's stake drops below 50% of the minimum and auto-ejection triggers (see [ADR 004](004-tokenomics.md#auto-ejection)):

- Open channels settle normally. Client funds are never trapped.
- The ejected node cannot participate in new channels (clients verify node registration before opening channels, and nodes verify counterparty status before accepting a `StreamRequest`).
- The ejected node is removed from gossip routing, so it receives no new client connections.
- `closeChannel` (client/provider only), `disputeChannel` (any address), and `settleChannel` (any address) remain callable on existing channels — these functions check channel state, not registry status.
- The node must re-stake at the full minimum and re-register to resume operations.
