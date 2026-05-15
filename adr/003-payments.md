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
- Fee routing: at settlement, the full operator USDC balance is forwarded to `FeeRouter.routeSettlement(operator, bytesDelivered, amount, epochId)` in a single transaction; the operator's 40% base share is paid out same-tx and the remaining buckets (gauge boost, delegator pool, buyback-and-burn, treasury, safety) are distributed per [ADR 026](026-gauge-boost-tokenomics.md). See [FeeRouter Integration](#feerouter-integration).
- Operator return is differentiated through ve-locked gauge boost per [ADR 026](026-gauge-boost-tokenomics.md), not via a fee-discount mechanic on the channel contract.

### Deposit Economics

Opening, closing, and settling a channel requires three on-chain transactions totalling ~$0.23 at the production L2's typical gas prices (`openChannel` ~$0.05, `closeChannel` ~$0.10, `settleChannel` ~$0.08). This estimate assumes an existing ERC-20 approval; first-time users incur an additional one-time `approve` transaction (~$0.03), bringing the true first-channel cost to ~$0.26. The table below uses the $0.23 lifecycle cost (excluding the one-time approval) as a percentage of various deposit sizes:

| Deposit | Lifecycle gas ($0.23) | Gas % of deposit |
|---------|----------------------|------------------|
| 1 USDC  | $0.23                | 23%              |
| 5 USDC  | $0.23                | 4.6%             |
| 10 USDC | $0.23                | 2.3%             |
| 25 USDC | $0.23                | 0.92%            |
| 100 USDC| $0.23                | 0.23%            |

**Recommended practical minimum: 10 USDC.** Client software should default to a 10 USDC minimum deposit (user-overridable). At 10 USDC, gas overhead is 2.3% — acceptable for a channel covering ~10,000,000 MB at the floor rate or ~1,000,000 MB (~1,000 GB) at the expected market rate ($0.01/GB), sufficient for weeks to months of casual use without top-up. The contract minimum (1 USDC, governable via `setMinDeposit`) remains a safety floor preventing dust channels that cost more to settle than they contain; it is deliberately not raised, since doing so would cut governance flexibility and create a hard barrier for development/testing scenarios where small deposits are useful.

#### Amortization

The overhead percentages above represent worst-case single-session economics. Long-lived channels amortize open/settle costs across many sessions: a channel used for 30 sessions costs ~$0.008/session in gas. Channels extended via `topUp` amortize further since only the initial open and final settle incur gas.

#### Smart Account Support and Gasless Channel Opens

All deCDN contracts use OpenZeppelin `SignatureChecker` for signature verification, supporting both EOA (via `ecrecover`) and smart account wallets (via ERC-1271 `isValidSignature`) from the PoC. Safe smart wallets are the recommended wallet type for both node operators and clients — see [ADR 024](024-account-abstraction.md).

Two standards can further eliminate the requirement for clients to hold the L2's native gas token:

- **ERC-2771 meta-transactions.** A relayer submits the `openChannel` transaction on behalf of the client, paying gas. The client signs an ERC-2771 forwarding request; the relayer recoups gas from the deposit or a separate sponsorship fund. Requires adding a trusted-forwarder check to the contract.
- **ERC-4337 account abstraction.** Smart contract wallets batch USDC approval + channel open into a single user operation. A paymaster can sponsor gas in USDC rather than ETH. Works with unmodified contracts — no changes to `StablePaymentChannel` needed.

Gas abstraction via ERC-2771 or ERC-4337 paymasters is deferred to production. For the PoC, clients must hold both USDC and a small amount of ETH for gas.

### Voucher Interval Negotiation

At the default 1 MB cadence, a 10 GB blob requires 10,000 vouchers — each involving a sign, transmit, verify, and ack cycle. This overhead is unnecessary when the unacknowledged exposure per interval is negligible at typical rates.

**Parameter:** `maxVoucherIntervalMb` is a governable parameter on `StablePaymentChannel` defining the maximum allowed voucher interval in MB. Default: 1 MB. Hardcoded safety bounds: minimum 1 MB, maximum 1024 MB (~1 GB).

**Negotiation semantics:**

1. The client proposes a `voucher_interval_mb` in `StreamRequest` (see [ADR 005](005-protocol.md)).
2. The node responds with its accepted `voucher_interval_mb` in `StreamResponse`. The node may accept the client's proposal, reduce it, or omit the field to fall back to 1 MB.
3. The effective interval for the stream is `min(client_proposed, node_accepted, on-chain maxVoucherIntervalMb)`.

**Default:** `voucher_interval_mb` is optional in both `StreamRequest` and `StreamResponse`; if absent, the default is 1 MB.

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

### Fee Routing on Disputed Closes

> **Fee routing model.** `settleChannel` does not skim a fee inline; it forwards the entire operator-bound balance to `FeeRouter.routeSettlement(operator, bytesDelivered, amount, epochId)` in the same transaction. Split details: [FeeRouter Integration](#feerouter-integration).

The settled amount is still calculated **at final settlement**, after the dispute window expires, based on the highest valid voucher amount on-chain at that point. The three-step channel close lifecycle is:

1. **`closeChannel`** — callable by client or provider only. Records the submitted voucher's `amount` in `claimedAmount`, `nonce` in `claimedNonce`, and `bytesDelivered` in `claimedBytes`, sets status to `Closing`, starts the dispute window. **No fee deduction, no router call.** **Zero-voucher close:** when **either party** calls `closeChannel` with `amount=0`, `nonce=0`, `bytesDelivered=0`, and an empty signature (`signature.length == 0`) on a channel with `claimedNonce == 0`, the signature verification is skipped — no client-signed voucher is needed. All other `closeChannel` calls — any call with `signature.length > 0`, or any call where `amount != 0`, `nonce != 0`, or `bytesDelivered != 0` — require normal EIP-712/ECDSA voucher verification. This is safe because voucher nonces start at 1 (nonce 0 is the sentinel for "no voucher submitted"; see [Voucher Nonce Convention](#voucher-nonce-convention)), so any real voucher has nonce ≥ 1 and can always be submitted via `disputeChannel` (which requires strictly higher nonce than `claimedNonce`). The dispute window still applies: if a valid voucher exists, any party can submit it via `disputeChannel`. At settlement, `claimedAmount=0` means the full deposit is refunded to the client and the provider receives nothing — no router call is made for a zero-amount settlement.
2. **`disputeChannel`** (during dispute window) — callable by any address. If the submitted voucher has a strictly higher nonce, updates both `claimedAmount`, `claimedNonce`, and `claimedBytes` (see [Voucher Bytes-Delivered Field](#voucher-bytes-delivered-field)) to the new values. Still **no fee deduction, no router call**. Submissions with an equal or lower nonce revert with no state change.
3. **`settleChannel`** (after dispute window expires) — callable by anyone. If `claimedAmount > 0`, computes `bytesDelivered` from the final voucher, transfers the full `claimedAmount` of USDC to the `FeeRouter`, and invokes `FeeRouter.routeSettlement(channel.provider, bytesDelivered, claimedAmount, voucher.epochId)` in the same transaction. Refunds `deposit - claimedAmount` to the client. Sets status to `Closed`. The router (not `StablePaymentChannel`) applies the six-bucket split and increments `bytesPerEpoch[operator][epochId]` for the gauge formula in [ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula) — split legs and bounds in [FeeRouter Integration](#feerouter-integration). The per-operator gauge-share cap from [ADR 026 §3](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) is the binding wash-trading defense; gauge bytes flow through `settleChannel` directly with no separate commit step.

   > **Invariants:**
   > 1. `closeChannel` and `disputeChannel` MUST revert if the submitted voucher's `amount > channel.deposit`. This prevents client bugs or malicious over-deposit vouchers from causing an underflow revert in `settleChannel` that would lock the channel.
   > 2. `disputeChannel` MUST revert if `newAmount < claimedAmount` or `newBytes < claimedBytes`. Vouchers are cumulative across both axes; a higher nonce must correspond to a non-decreasing amount and a non-decreasing byte count. This prevents a malicious client from reducing the provider's payout — or the provider's gauge-pool byte share — via a higher-nonce dispute.
   > 3. `settleChannel` MUST forward `claimedAmount` USDC to `FeeRouter` and call `routeSettlement` in the same transaction iff `claimedAmount > 0`. The provider's 40% base share lands in the operator's wallet in the same transaction as `settleChannel`; this is the cashflow guarantee that backs operator P&L Case A in [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake). Reverting after partial transfer is unacceptable — implementations MUST use checks-effects-interactions, MUST guard `settleChannel` and `disputeChannel` with a `nonReentrant` modifier (the `FeeRouter` call path crosses a contract boundary and is the new reentrancy surface), and the `FeeRouter` MUST hold a stable interface contract.

A dispute that raises the settlement amount (e.g., 50 → 80 USDC) raises every router-bucket allocation proportionally, and a higher `claimedBytes` raises the operator's gauge-pool weighting for the epoch. The router computes its split once, on the final settled amount and byte count — never on intermediate values, never more than once per channel.

The native token (TOKEN) is not used for delivery payments. It is reserved for staking, gauge-boost ve-locking (see [ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula) and [§4](026-gauge-boost-tokenomics.md#4-voting-escrow-votingescrow)), and governance (see [ADR 009](009-governance.md)).

**Rate setting is entirely up to each node.** Nodes advertise their `rate_per_mb` in probe responses and stream responses; the requester sees the rate before committing a voucher. There is no protocol-enforced rate beyond a governance-set floor and ceiling. This creates a market with natural arbitrage dynamics:

- Origin-backed nodes set a higher rate because they bear backend costs (storage + egress from their hidden backing store). They are the effective price ceiling for any blob they hold.
- A cache-only node that pays an origin-backed node to pull a blob can then serve that blob to many clients at a markup, recouping the origin cost across multiple deliveries.
- A node in a region where no peer has the content yet can charge a premium for that first delivery. Once it has the blob, other nearby nodes can pull from it at a competitive rate and compete for local clients.
- Nodes with cheaper bandwidth or better hardware can sustainably undercut others; nodes in high-demand regions can charge more and still win on latency.

The network self-balances with no central coordinator: profitable content gets replicated, competition drives prices down in well-served regions, and unpopular content stays at origin-backed node rates until demand justifies caching it.

**Origin backend economics:** The backing store choice directly affects an origin-backed node's viable rate. At the expected $0.01/GB market rate, an S3-backed node paying $0.09/GB egress loses money on every cache miss and must amortize origin pulls across a high cache-hit ratio (or price above market). Zero-egress backends — Cloudflare R2 ($0.00/GB), Backblaze B2 ($0.00/GB via Bandwidth Alliance partners), Wasabi ($0.00/GB) — keep origin-backed nodes profitable at or near market rates. High-egress backends imply higher `rate_per_mb`, which the market tolerates for content not yet cached elsewhere.

### Off-chain Voucher Rejections (Wire Encoding)

When a node rejects a voucher off-chain — before any gas would be spent — the rejection is returned **in-band** mid-stream as a `StreamError` message carrying `VoucherRejected { reason }` (per [ADR 005 § Stream Lifecycle State Machine](005-protocol.md#stream-lifecycle-state-machine), this transitions the stream `Streaming → Failed` cleanly without a QUIC stream reset). Voucher validation can only fire after at least one `Voucher`, necessarily after `StreamResponse { ok: true }` — so payment rejections never use the initial-response error path that delivery-side failures (`NotFound`, `Overloaded`, etc.) take. Full reason enum and per-reason retry semantics: [ADR 005 § VoucherRejected semantics](005-protocol.md#voucherrejected-semantics).

The eight `VoucherRejectReason` values mirror the off-chain validation enums `ChannelError` / `VoucherError` (in `crates/incentive/`) one-to-one, and each maps back to the on-chain invariant it protects:

| `VoucherRejectReason` | Off-chain trigger | On-chain invariant protected |
|---|---|---|
| `BadSignature` | Malformed signature bytes | EIP-712 `SignatureChecker` would revert at `closeChannel` (see [EIP-712 Voucher Signature](#eip-712-voucher-signature)) |
| `WrongSigner` | Signature recovers to the wrong address | `closeChannel` would revert when recovered signer ≠ `channel.client` |
| `WrongChannel` | `voucher.channel_id` mismatch | EIP-712 domain binds the voucher to a specific `channelId`; off-channel vouchers authorize nothing |
| `WrongToken` | `voucher.token` mismatch | Cross-token replay defense (see [Replay attack on vouchers](#replay-attack-on-vouchers)) |
| `StaleNonce` | `voucher.nonce ≤ last accepted nonce` | `disputeChannel` requires strictly higher nonce ([Voucher Nonce Convention](#voucher-nonce-convention)) |
| `AmountRegression` | `voucher.amount < last accepted amount` | Invariant 2 — `disputeChannel` reverts if `newAmount < claimedAmount` (see [Fee Routing on Disputed Closes](#fee-routing-on-disputed-closes)) |
| `BytesRegression` | `voucher.bytes_delivered < last accepted bytes_delivered` | Invariant 2 — `disputeChannel` reverts if `newBytes < claimedBytes` |
| `InsufficientDeposit` | `voucher.amount > channel.deposit` | Invariant 1 — `closeChannel` / `disputeChannel` revert if `voucher.amount > channel.deposit` |

Surfacing these reasons off-chain saves both parties the gas of a doomed on-chain submission and gives the payer enough detail to recover (e.g., refresh state and re-sign for `StaleNonce`, top up for `InsufficientDeposit`) instead of an opaque connection drop. Riding in-band rather than via a QUIC stream reset preserves the reason for client retry logic without burning [ADR 013](013-schema-evolution.md) application-error-code numbers for the structured-response case.

## Consequences

**Positive:**

- On-chain costs are amortized across an entire channel lifetime — open + close + settle = three transactions regardless of how many MB are delivered (settle can be called by any address, allowing third-party settlement bots)
- USDC denomination gives node operators predictable unit economics: delivery revenue covers infrastructure costs without exposure to TOKEN price movements
- The voucher is the payment receipt; the BLAKE3 hash is the delivery receipt. Together they provide mutual protection: the client doesn't sign a voucher for bytes that fail hash verification; the node stops delivering if vouchers stop arriving
- Maximum risk per voucher interval at default cadence (1 MB) is $0.00001 at market rate — negligible. At the governance maximum interval (1024 MB) and ceiling rate ($0.001/MB), worst-case risk is $1.024 per interval — still small relative to the recommended 10 USDC minimum deposit (see [Voucher Interval Negotiation](#voucher-interval-negotiation))
- Market-driven rate setting means replication happens organically: profitable content gets cached by more nodes, driving prices down without any coordination protocol
- The `StablePaymentChannel` contract is functionally separated from the `StakingRegistry`, keeping the audit surface for each contract's core logic bounded

**Negative:**

- Clients must hold USDC and native L2 tokens for gas to use the network; this adds an onboarding step compared to a single-token model. Gas-overhead percentages and the gasless-open deferral are quantified in [Deposit Economics](#deposit-economics)
- Rate volatility: a node can change its advertised rate between a probe and a stream request; the `StreamResponse` rate is the binding one, but a client that probed at one rate and receives a higher rate in `StreamResponse` must disconnect and re-probe rather than having been deceived silently. Rate changes more than 30 seconds after the probe are not slashable; the 30-second window is precisely defined as `stream_response.timestamp_us >= probe_response.timestamp_us && stream_response.timestamp_us - probe_response.timestamp_us < 30_000_000` using requester-anchored timestamps in both signed messages (see ADR 005)
- USDC is issued by Circle, which can freeze specific addresses or blacklist the contract. For the PoC this risk is accepted; multi-token payment support to mitigate it is deferred to [ADR 010](010-multi-token.md)

## Attack Vectors

### Client-side

#### Voucher withholding

Client receives bytes but stops signing vouchers, getting content for free up to the last signed interval.

The self-enforcing stop is sufficient. Maximum loss is one voucher interval at the negotiated cadence: default cadence (1 MB × market rate ≈ $0.00001) is negligible; 100 MB at market rate is ~$0.001; governance maximum (1024 MB) at ceiling rate is ~$1.024 — still negligible relative to channel deposits. Nodes serving high-value content can unilaterally enforce smaller intervals regardless of what was negotiated.

#### Channel griefing

Client opens many channels with minimum deposit and never streams, forcing nodes to track and eventually close stale channels.

**Resolved: zero-voucher close (either party).** The zero-voucher close mechanic — canonically specified in [Fee Routing on Disputed Closes](#fee-routing-on-disputed-closes) §1 — bounds the maximum tracking duration to the dispute window (48 hours PoC default) rather than the full 90-day channel expiry, and is permissionlessly disputable if the closing party actually signed a voucher off-chain. No additional inactivity timer or separate expiry mechanism beyond the existing channel expiry / `reclaimExpired` path is needed; that existing escape hatch remains as a final fallback for cases where the channel is abandoned without any close action at all.

**Why symmetric.** The provider needs the path to release abandoned channels they track. The client needs it so they aren't locked into 90 days of `reclaimExpired` waiting when a node fails before the first 1 MB voucher boundary — a routine ops failure with no malicious actor. Restricting the path to providers would create a structural liquidity-lock on every node-failure event, contrary to the intended failure-mode posture. The 48h dispute window plus permissionless `disputeChannel` cover the symmetric attack surface (a client signing vouchers off-chain then trying to repudiate them via zero-voucher close) exactly as they cover the analogous [stale close](#stale-close) attack.

The griefing attacker's financial cost stays bounded: at the recommended 10 USDC minimum, $1,000 opens 100 channels; the counterparty (or the attacker, to recover the deposit) closes them all and each settles after the dispute window with full client refund (no profit motive) and ~$0.18 gas per close+settle pair. Total gas exposure for 100 channels is ~$18 — enough to warrant additional mitigations for high-volume attacks:

- **Option A — On-chain channel cap per address.** The `StablePaymentChannel` contract enforces a maximum number of open channels per client Ethereum address (e.g., 10). Hard to circumvent without new wallet addresses, each requiring on-chain funding.
- **Option B — Node-side filtering.** Nodes refuse `StreamRequest` from channels that have been open longer than N days with zero vouchers. Off-chain, no contract change needed, but relies on node operator implementation.

#### Stale close

Client submits an old voucher (lower amount) to close the channel, underpaying the node.

The dispute window (default 48 hours for PoC, raised from 24 hours to account for L2 forced-inclusion delay; see [L2 sequencer censorship](#l2-sequencer-censorship) below) covers this if the node is online. **Defense layers:**

1. **In-process dispute monitor.** A lightweight thread inside the node binary watches the chain for `ChannelCloseInitiated` events on its channels and auto-submits the latest voucher via `disputeChannel`. Zero-latency to the local voucher store; handles the common case where the node is online. Implementation is a SHOULD for production node binaries.
2. **Operator-arranged redundancy.** Multi-instance deployments, hot-standby relays, peer agreements to relay vouchers. Out of protocol scope; the protocol does not define a wire format for voucher-relay arrangements between operators.
3. **Permissionless on-chain dispute submission.** `disputeChannel` accepts submissions from any address holding a higher-nonce voucher — operators with their own infrastructure or counterparties can submit directly. See [Appendix: Fraud Detection](appendix-fraud-detection.md).

The node-offline-for-the-full-48h case is a node-operations responsibility, not a protocol gap.

#### Probe fishing

Client sends probe requests to many nodes at high frequency to map the network or exhaust node resources without ever paying.

Per-NodeId rate limiting alone is bypassable: clients are not staked, NodeIds are free to rotate, and iroh connection setup is cheap. The mitigation is the layered token-bucket rate limit in [ADR 005 § Probe rate limiting](005-protocol.md#probe-rate-limiting): per-peer (NodeId) plus per-IP plus a global node cap, applied before any signature or hold-slot allocation. The per-IP layer raises the cost of bulk probing because IP rotation requires money (proxies, IPv6 delegation, cloud bills) while NodeId rotation does not; the global cap is defence in depth.

Alternatives considered:

- ~~**Option B — Require an open channel to probe.**~~ **Rejected.** Creates a bootstrap catch-22: clients need probe results (rate, latency) to choose a node before opening a channel, but Option B requires a channel before probing. Since probes happen before channel opens (see [ADR 005](005-protocol.md) probe flow), requiring a channel is architecturally incompatible with the protocol sequence. Probes are unauthenticated and free — see ADR 005's statement that "`ProbeRequest` requires no authentication."
- ~~**Option C — Proof-of-work on probe requests.**~~ **Rejected.** Two reasons: (a) probe latency is part of the unified node-selection score ([ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm)), so adding mandatory hashing on every probe degrades the selection signal the probe was meant to provide; (b) PoW is bypassable by an attacker with cheaper compute than the honest client (cloud GPU vs mobile CPU), inverting the intended cost asymmetry.
- ~~**Option D — Accept the risk and monitor only.**~~ **Rejected.** A probe response is a 200-byte signed message; per-probe cost is dominated by the EIP-712 signature (~1 ms CPU on a typical node). At scale this is enough that a Sybil attacker can saturate the signing path and exhaust the hold budget. Monitoring without enforcement is not sufficient — the locked mechanism is enforced rate limiting per [ADR 005 § Probe rate limiting](005-protocol.md#probe-rate-limiting).

**Note:** Probe responses are considered public information (see ADR 005). The concern here is resource exhaustion from bulk probing, not information leakage — content availability is discoverable via probing (see ADR 005), and pricing is revealed in probe/stream responses by design.

#### Double-spend across nodes

Client opens channels with multiple nodes using the same USDC deposit via a race condition before the on-chain state settles.

Each `openChannel` call transfers USDC into the contract immediately; the client's wallet balance is debited on-chain before the transaction finalises. No credit facility exists.

### Node-side

#### Data withholding

Node accepts a stream request, receives a voucher, then stops delivering bytes.

Self-enforcing: the node cannot extract more payment than the last acknowledged voucher. The client resumes from `byte_offset` on a different node.

#### Corrupted delivery

Node serves bytes that don't match the advertised BLAKE3 hash.

Absorbed at the wire by progressive BLAKE3 verification at the client (mandatory in `cdn/client/v1` per [ADR 002](002-content-addressing.md) and [ADR 005](005-protocol.md)). Vouchers are signed and sent only after the corresponding chunks have been verified — a corrupt window therefore yields no voucher. The client drops the connection, requests the blob from a different node, and recovers any unspent channel funds via channel-close. **Client monetary loss in the corruption case is zero**; the only cost is downstream bandwidth (sunk regardless of outcome).

No on-chain slash machinery is needed for content corruption. The threat is bounded in framing parallel to [§Voucher withholding](#voucher-withholding) above: per-encounter wasted bandwidth is capped at one `voucher_interval` on each side (the client's downstream cost for a corrupt window; the node's upstream cost when a correctly-withheld voucher leaves the window unpaid). Both sides set local acceptance policies — nodes refuse continued service to keys with elevated voucher-withhold rates and may cap total bytes for keys without established history; clients prefer nodes whose probe and delivery history they trust — without protocol-level coordination. Client reputation is a node-local concern; this ADR does not specify a wire format or on-chain surface for it.

#### Rate bait-and-switch

Node advertises a low rate in probe responses then returns a higher rate in `StreamResponse`.

**Resolved: slashable offense.** Both responses are signed over the advertised rate ([ADR 005](005-protocol.md)); a same-NodeId signed pair where `StreamResponse.rate_per_mb > ProbeResponse.rate_per_mb` and the requester-anchored timestamp delta is under 30 seconds is on-chain-verifiable evidence. Clock-skew immune (both timestamps originate from the requester's clock; the node echoes them back in its signed response). The slash schedule lives in [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn); see [ADR 014 § 1](014-on-chain-verification.md#1-slash-signatures--secp256k1-eip-712) for the on-chain verifier.

#### Phantom blob announcement

Node announces a blob as cached (`has_blob: true` in a signed `ProbeResponse`) then fails or redirects on actual request.

**Resolved: slashable offense.** A same-NodeId signed `ProbeResponse(has_blob: true)` paired with a signed `StreamResponse(ok: false)` or redirect for the same hash within a 30-second requester-anchored timestamp window is on-chain-verifiable evidence. The bare timeout / non-response case is reputation-only (no second signed message → not slashable on-chain). Slash schedule per [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn); on-chain verifier per [ADR 014 § 1](014-on-chain-verification.md#1-slash-signatures--secp256k1-eip-712); slash executes synchronously at submit time per [ADR 014 §Bond Handling](014-on-chain-verification.md#bond-handling).

To prevent legitimate cache eviction from producing false slash evidence inside the 30-second window, nodes MUST honor a **probe-triggered eviction hold** (35s, 30s slash window + 5s margin) — see [ADR 005 § Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold) for the requirement and dependent parameters (probe cache TTL, `probe_hold_duration`). Hold violations under OOM / under-provisioning fall to the same 24-hour counter-window; eviction logs are not on-chain verifiable, so only delivery-receipt counter-evidence rebuts. The protocol does not subsidize under-provisioning.

#### Channel close front-running

Node monitors the mempool and front-runs a client's channel close with a higher voucher submission.

Not an attack. The contract always settles the highest valid voucher, and only the client can sign a valid voucher; a node submitting the latest voucher before the client is the intended happy path. Fabricating a higher voucher requires forging the client's ECDSA signature, which is cryptographically infeasible.

#### Third-party forced channel close (DoS)

A third party holding a valid voucher calls `closeChannel` to force the channel from `Open` to `Closing`, halting delivery.

**Resolved: access control restriction.** `closeChannel` requires `msg.sender == channel.client || msg.sender == channel.provider`. Third parties cannot initiate a close regardless of whether they hold a valid voucher. Permissionless fraud detection is unaffected — third parties operate via `disputeChannel` during the dispute window ([Appendix: Fraud Detection](appendix-fraud-detection.md)). The residual risk is a `disputeChannel` call with an intercepted voucher, which can only *improve* the settlement (higher nonce required). On-path network interception of vouchers is mitigated by QUIC transport (TLS 1.3), though this does not address endpoint compromise or other forms of leakage.

### Network-level

#### Eclipse attack

Attacker surrounds a client with malicious nodes so all probe responses come from nodes under attacker control.

BLAKE3 verification catches data corruption regardless of peer-table composition; the remaining DoS variant (attacker-controlled peer set refuses to serve) is resolved in [ADR 012 § Bootstrap and Trust Model](012-client.md): production uses multi-source bootstrap (on-chain registry + hardcoded DNS seeds) so an attacker must compromise both to fully eclipse a client; minimum honest-peer diversity is a supplementary client-side policy. PoC is registry-only.

#### Gossip flooding

Node sends high-volume `NodeAnnounce` messages to exhaust peer table memory or crowd out legitimate announcements.

Registry check + per-sender rate limiting. Residual gap: the local registry cache may be up to 10 minutes stale, briefly allowing recently-unstaked nodes to flood; mitigated by tightening the registry cache refresh on high flood detection.

#### Sybil nodes

Attacker stakes many cheap nodes to dominate probe responses for popular content, controlling pricing in a region.

The core weakness is token-price dependency: at $0.001/TOKEN, a minimum stake of 1,000 TOKEN costs $1 per sybil node. The unified selection score `rate_per_mb × rtt_ms × (1 / max(reputation, 0.1)²)` (see [ADR 001](001-network.md#node-selection-algorithm)) helps — a sybil fleet must be real hardware in the right geography, competitively priced, and build reputation over time — but does not eliminate the risk when the token is cheap. Options:

- **Option A — Governance raises minimum stake if token price falls.** The minimum stake is governable. Token holders are incentivised to raise it to protect the network, since a sybil-dominated network reduces usage and token value. Reactive but aligned.
- **Option B — Minimum stake denominated in USD equivalent via oracle.** Requires a price oracle, which introduces oracle dependency, manipulation, and downtime risks (see rate bounds discussion above). The same concerns apply here, but the impact of oracle failure is lower (new stakers temporarily blocked, not payments broken).
- **Option C — Reputation as a second filter.** New nodes (low reputation, few settled channels) are deprioritised in client selection even if their `rate_per_mb × rtt_ms × (1 / max(reputation, 0.1)²)` score is competitive. A sybil fleet takes time to build reputation, limiting its effectiveness during that window.

#### Rate manipulation cartel

Colluding nodes in a region hold rates artificially high.

Origin-backed nodes set the effective price ceiling for any blob. Clients can always probe origin-backed nodes directly and pay their rates as a guaranteed fallback. Any node outside the cartel that undercuts wins all local traffic — the incentive to defect is strong. New entrants can join the cache-only role permissionlessly by staking; the origin role for content in registered namespaces requires `OriginAssignment` membership ([ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)), but cache-only competition is sufficient to discipline the rate cartel because cache delivery is interchangeable with origin delivery from the requester's perspective.

#### Content withholding

A node stakes, responds to probes with `has_blob: true`, but refuses to serve — collecting credibility in the peer table without actually participating.

**Withholding is not a slashable offense** — operators may legitimately take content offline for maintenance, migration, or business reasons, and slashing for availability creates perverse incentives. Instead, withholding is handled through reputation and DAO-supervised redundancy:

- **Minimum-redundancy invariant on registered namespaces.** Content owners register a publisher identity ([ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces)) and propose an origin operator set per namespace. Governance ratifies the proposal via the standard timelock path ([ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)). The `OriginAssignment` contract enforces a minimum-redundancy floor (default 3, governance-bounded — see [ADR 009](009-governance.md)) at activation. A single withholding origin becomes irrelevant when others in the assigned set serve the same blob.
- **Default-open content** is governed by the DAO-maintained default-open allow-list ([ADR 011 § Default-open allow-list](011-content-takedown.md#default-open-allow-list)) once governance has activated it for the first time; the allow-list enforces its own redundancy floor (default 10), so a single withholding origin is mitigated the same way as registered namespaces. During the bootstrap window before the allow-list is activated, default-open serving falls back to the legacy off-protocol model: any staked operator may serve as origin and content owners that have not registered carry the same withholding risk as before.
- **Reputation fast-path.** Nodes that respond `has_blob: true` to probes but fail to deliver accumulate reputation penalties at a steeper rate. A node with consistently poor availability is deprioritized in provider selection and loses delivery revenue. Publishers may use the reputation signal as input when proposing or revoking operators in their namespace's assignment.

Note: the probe-triggered eviction hold ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)) addresses a related but distinct problem. Withholding is a node that has the blob but refuses to serve it (behavioral — handled by reputation). The eviction hold addresses a node that signed `has_blob: true` but lost the blob to cache pressure before the stream request (mechanical — prevented by the hold and, if the hold fails, treated as a slashable phantom announcement).

#### Replay attack on vouchers

Attacker intercepts a signed voucher and attempts to replay it against a different channel or after close.

EIP-712 typed data over `{channelId, amount, nonce, bytesDelivered, token}` binds the voucher to a specific channel. The EIP-712 domain separator (see [EIP-712 Voucher Signature](#eip-712-voucher-signature)) further binds each voucher to a specific chain and contract deployment, preventing replay across different L2s, contract upgrades, or test vs production environments. The monotonically increasing nonce (starting at 1; see [Voucher Nonce Convention](#voucher-nonce-convention)) prevents resubmission after settlement.

#### Off-chain voucher state persistence

The on-chain protections in [Replay attack on vouchers](#replay-attack-on-vouchers) constrain only what the contract accepts at settlement. They do not prevent the **delivering node** from re-delivering bytes off-chain for a voucher it already honoured: a node holding voucher state only in memory will, after restart, re-accept any earlier-nonce voucher the client (or any wire observer) resubmits and serve the bytes again. Issue #527 is the canonical filing.

Required invariant: a node MUST persist `(last_nonce, last_amount, last_bytes_delivered)` per channel and durably commit (fsync, on disk-backed implementations) **before** sending `VoucherAck` or delivering any further bytes for that voucher. After a restart, voucher acceptance MUST resume from the persisted state — never from `last_nonce = 0`. An absent entry is semantically identical to a never-seen channel (`last_nonce == 0`, per [Voucher Nonce Convention](#voucher-nonce-convention)); a record exists iff the node ever advanced past the initial sentinel. Entries are dropped only when the node observes `ChannelSettled` on-chain.

A failed persist write MUST surface as a voucher-acceptance failure — the node returns a transient-failure rejection through the [Off-chain Voucher Rejections (Wire Encoding)](#off-chain-voucher-rejections-wire-encoding) channel, and MUST NOT send `VoucherAck`. The specific wire code for transient persistence failures is left to the `cdn/client/v1` handler implementation (issue #317); the existing `StaleNonce` / `InsufficientDeposit` codes are NOT appropriate substitutes because they would tell the client to refresh state or top up the deposit when in fact the same voucher should be retried unchanged. Persisting after acknowledgement re-opens the same replay window for the crash interval between the two writes.

Storage backend and trait shape are implementation concerns; the Rust implementation exposes a `ChannelStateStore` seam in `crates/incentive` with a `redb`-backed persistent implementation in `crates/node` (per the leaf-crate convention in [appendix-poc-production-seams.md §1](appendix-poc-production-seams.md#1-keystore--cratesincentive)). The protocol fixes only the ordering above.

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
    uint256 claimedBytes;     // cumulative bytes delivered per the current best voucher; forwarded to FeeRouter at settlement (see ADR 026)
    uint256 openedAt;
    uint256 expiresAt;
    uint8   status;           // 0 = Open, 1 = Closing (dispute window active), 2 = Closed (settled)
    uint256 disputeDeadline;  // set when close is initiated; may be extended once via the forced-inclusion path (see § L2 sequencer censorship)
    address lastDisputor;     // msg.sender of the most recent disputeChannel call
    bool    extended;         // true if disputeDeadline has been extended once via the forced-inclusion path; reset to false on closeChannel
}
```

**Channel ID:** `channelId = keccak256(abi.encodePacked(client, provider, channelNonce))` where `channelNonce` is a monotonic per-client counter stored on-chain as `clientChannelNonce[msg.sender]`. **Ordering:** `openChannel` reads the current nonce, uses it to compute `channelId`, then increments: `n = clientChannelNonce[msg.sender]; channelId = keccak256(..., n); clientChannelNonce[msg.sender] = n + 1`. The client pre-computes the next channelId off-chain by reading `clientChannelNonce[client]` and using that value directly — no off-by-one because the contract uses the same value before incrementing. The `channelNonce` is global per-client (not per-provider), ensuring uniqueness across all of a client's channels.

> **Terminology:** `channelNonce` (the channel creation counter) is distinct from the voucher `nonce` (the monotonic sequence number within a channel used in EIP-712 voucher signatures). The former uniquely identifies channels; the latter orders vouchers within a channel. [ADR 010](010-multi-token.md) extends this formula to `keccak256(client, provider, token, channelNonce)` for multi-token support. In implementation, consider naming the on-chain mapping `clientChannelCounter` to avoid confusion with voucher nonces.

| Group | Function | Purpose |
| --- | --- | --- |
| Nonce | `clientChannelNonce(client) → uint256` | Per-client monotonic counter used in `channelId` derivation. |
| Lifecycle | `openChannel(provider, deposit) → channelId` | Open a USDC channel; increments `clientChannelNonce[msg.sender]` then derives `channelId`; emits `ChannelOpened`. |
| Lifecycle | `topUp(channelId, additionalDeposit)` | Client-only: add funds to an open channel (does not extend `expiresAt`). |
| Lifecycle | `closeChannel(channelId, amount, nonce, bytesDelivered, signature)` | Client or provider: initiate close with the latest voucher; starts dispute window. |
| Lifecycle | `disputeChannel(channelId, amount, nonce, bytesDelivered, signature)` | Any address: submit a higher-nonce voucher during the dispute window. |
| Lifecycle | `settleChannel(channelId)` | Post-dispute-window: forward `claimedAmount` USDC to `FeeRouter`; refund unused deposit. |
| Lifecycle | `reclaimExpired(channelId)` | Client or provider: refund full deposit on an expired channel that was never closed. |
| View | `getChannel(channelId) → Channel` | Read the on-chain `Channel` struct. |
| View | `getRateBounds() → (floor, ceiling)` | Current `RateBounds` in token base units. |
| View | `feeRouter() → address` | Configured `FeeRouter` target ([ADR 026](026-gauge-boost-tokenomics.md)). |
| Governance | `setFeeRouter(addr)` | Replace router target. `GOVERNANCE_ROLE`-gated; routed through the standard 48h `TimelockController` delay; emits `FeeRouterUpdated(address oldRouter, address newRouter)`. See [§ Governance setter: setFeeRouter](#governance-setter-setfeerouter) below. |
| Governance | `setMinDeposit(amount)` | Minimum channel deposit. |
| Governance | `setDisputeWindow(seconds)` | Dispute window (bounded 43200–259200 — 12h–72h). |
| Governance | `setRateBounds(floor, ceiling)` | Rate floor and ceiling in token base units. |
| Governance | `setMaxVoucherIntervalMb(mb)` | Max negotiable voucher interval (bounded 1–1024 MB). |

Bucket shares (40/40/7/5/5/3) are governed on `FeeRouter`, not on `StablePaymentChannel`; the treasury share (5%) is configured on `FeeRouter`.

#### Governance setter: setFeeRouter

```solidity
function setFeeRouter(address newRouter) external onlyRole(GOVERNANCE_ROLE);

event FeeRouterUpdated(address indexed oldRouter, address indexed newRouter);
```

`setFeeRouter` re-points the configured `FeeRouter` for future `settleChannel` calls. Required because the audited contract surface is fixed at deploy time, yet the `FeeRouter` may need replacing (bug fix, structural upgrade) without redeploying `StablePaymentChannel` and forcing every open channel to re-issue vouchers.

**Authority and timelock.** Only callable by `GOVERNANCE_ROLE` (held by the `TimelockController` post-deploy per [ADR 016 § Post-Deployment Initialization](016-contract-interactions.md#post-deployment-initialization)). `DecdnGovernor` proposals to replace the router execute through the standard 48h timelock per [ADR 009](009-governance.md). Calls outside that path revert.

**Validation.** Reverts on `address(0)` and on the same address as the current `feeRouter`. The new router contract is not interrogated at the setter — the cross-validation invariants in [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics) live on `FeeRouter` itself; replacing the router with a misconfigured deployment surfaces at the next `settleChannel` rather than at the setter.

**Open channels are unaffected.** Vouchers signed against this `StablePaymentChannel` remain valid because the EIP-712 domain separator hashes the contract's own address, not the configured `FeeRouter`. Carve-out documented in [ADR 016 § No proxy deployment patterns](016-contract-interactions.md#no-proxy-deployment-patterns): helper-contract addresses are not domain-separator inputs and may be re-pointed via governance without invalidating signatures.

**Settlement during the swap.** Settlements beginning before the timelock executes use the previous router; those beginning after use the new one. `settleChannel` reads `feeRouter()` at call time, and `routeSettlement` is a single transaction, so no in-flight settlement splits across routers.

> **Reentrancy protection:** All state-mutating functions that perform external calls (ERC-20 transfers) — `openChannel`, `topUp`, `settleChannel`, `reclaimExpired` — MUST use `nonReentrant` guards and follow checks-effects-interactions. This is especially critical for the production multi-token contract ([ADR 010](010-multi-token.md)) which accepts arbitrary governance-approved tokens.

**`topUp` behavior:** `topUp(channelId, additionalDeposit)` adds funds to an open channel:

- **Status precondition:** MUST require status `Open` and `block.timestamp < expiresAt` (reverts on `Closing`, `Closed`, or expired).
- **Caller:** client only (`require(msg.sender == channel.client)`).
- **Effects:** Transfers `additionalDeposit` from `msg.sender` to the contract via `safeTransferFrom`. Updates `channel.deposit += additionalDeposit`. Does NOT extend `expiresAt` (to prevent indefinite lock-in — the channel's utility is bounded by the initial `maxChannelDuration`).
- **Modifiers:** `nonReentrant`.
- **Emits:** `ChannelToppedUp(channelId, additionalDeposit, newDeposit)`.

#### Initial deployment values

The constructor takes `(usdc, feeRouter, disputeWindow)` and sets the remaining governable parameters to their PoC defaults: `maxVoucherIntervalMb = 1` (1 MB) and `maxChannelDuration = 7776000` (90 days). All values are within the hardcoded safety bounds table further below (see also [ADR 009](009-governance.md) for governance ranges). The constructor MUST reject `feeRouter == address(0)` and a `feeRouter` whose code size is zero (EOA / undeployed address).

Default PoC deployment value for `disputeWindow`: **172800 seconds (48 hours)** — raised from 24 hours to guarantee effective dispute response time under L2 sequencer censorship (see [§ L2 sequencer censorship](#l2-sequencer-censorship) below). Safety bounds per [ADR 009](009-governance.md): 43200–259200 seconds (12h–72h). Under ADR 026 the `feePercentage` / `discountedFeePercentage` / treasury-address constructor parameters from earlier drafts are removed; bucket shares are governed on `FeeRouter` instead, and the treasury bucket is one of `FeeRouter`'s six buckets (see [FeeRouter Integration](#feerouter-integration)).

#### L2 sequencer censorship

A malicious closer (or colluding sequencer) submits `closeChannel` with a stale voucher and ensures all `disputeChannel` transactions are censored for the full dispute window. Counterparties fall back to L1 forced inclusion, but this takes up to ~24 hours on Arbitrum (similar paths on other OP-Stack chains). If the dispute window is also 24 hours, effective dispute response time is zero by the time the forced-inclusion transaction is processed.

**Mitigation — layered defense.** The dispute window default is **48 hours** (172800 seconds), which guarantees at least 24 hours of effective dispute response time on any L2 with a forced-inclusion delay ≤ 24 hours. The setting is L2-agnostic and stays within the [ADR 009](009-governance.md) governance bounds (12h–72h).

On top of that baseline, `disputeChannel` implements a **forced-inclusion deadline extension**: if a `disputeChannel` transaction arrives via L1 forced inclusion, the remaining dispute time is less than 24 hours, and `channel.extended == false`, `disputeDeadline` is set to `block.timestamp + 24 hours` and `channel.extended` is set to `true` — guaranteeing 24 hours of dispute time from the moment the forced-inclusion transaction is processed. `closeChannel` resets `channel.extended` to `false` so a new close cycle starts fresh.

Constraints on the extension mechanism:

- **One extension per close.** Enforced on-chain by the `channel.extended` flag: once set, subsequent forced-inclusion `disputeChannel` calls do not trigger a further extension. This bounds worst-case settlement delay to `disputeWindow + 24h`.
- **Only forced-inclusion transactions.** Normal sequencer-included `disputeChannel` calls do not trigger the extension, preventing abuse.
- **L2-specific detection.** Identifying a forced-inclusion transaction is L2-specific. On Arbitrum, this can be detected via `ArbSys` precompile or delayed-inbox origin; on OP Stack, via L1 message origin. Exact detection logic is finalized at L2 selection — see [Appendix: L2 Deployment](appendix-l2-deployment.md).
- **Governance must not set the dispute window below the L2's maximum forced-inclusion delay.** On an L2 with ~24h forced inclusion the 12h governance floor is not safe — the extension never executes because `settleChannel` becomes callable before the forced-inclusion `disputeChannel` arrives. The 12h floor remains as a hardcoded safety bound for L2s with shorter forced-inclusion paths.

#### Events

All events use indexed `channelId` plus an indexed actor field where applicable. `ChannelOpened` uses three indexed fields (`channelId`, `client`, `provider`) — the EVM maximum — so wallets and CLIs can `eth_getLogs` filter by either party without parsing tx history.

| Event | Emitted by | Non-indexed fields |
| --- | --- | --- |
| `ChannelOpened(channelId, client, provider, …)` | `openChannel` | `token` (USDC address for PoC; arbitrary ERC-20 in production per [ADR 010](010-multi-token.md)), `deposit`, `expiresAt` |
| `ChannelCloseInitiated(channelId, initiator, …)` | `closeChannel` | `amount, nonce, bytesDelivered, disputeDeadline` |
| `ChannelDisputed(channelId, disputor, …)` | `disputeChannel` | `newAmount, newNonce, newBytes` |
| `ChannelSettled(channelId, provider, …)` | `settleChannel` | `routedAmount` (USDC forwarded to `FeeRouter` = `claimedAmount`), `bytesDelivered` (counted toward operator's epoch byte counter), `clientRefund` |
| `ChannelExpiredReclaimed(channelId, client, …)` | `reclaimExpired` | `deposit` |
| `ChannelToppedUp(channelId, …)` | `topUp` | `additionalDeposit, newDeposit` |
| `ChannelForceClosedByTokenRemoval(channelId, token, caller, …)` | `forceCloseChannel` (production `PaymentChannel` only — see [ADR 010](010-multi-token.md)) | `disputeDeadline` |
| `RateBoundsUpdated` | `setRateBounds` | `newDeliveryFloor, newDeliveryCeiling` |

`ChannelOpened` is the entry point for off-chain channel enumeration: a client lists their channels via `eth_getLogs(topics=[ChannelOpened, *, paddedClientAddress])`; a provider does the same with their address in the third topic; an indexer keys on `channelId`. This ensures channels are discoverable via log scans even if they have not yet had any subsequent on-chain activity (no `topUp`, `closeChannel`, or `disputeChannel`).

`ChannelSettled` carries no `protocolFee` field — `settleChannel` does not skim a fee inline. The bucket distribution emits its own events from `FeeRouter` (see [FeeRouter Integration](#feerouter-integration)).

**Channel expiry:** `expiresAt` is set at channel open: `expiresAt = block.timestamp + maxChannelDuration`. The `maxChannelDuration` parameter defaults to 90 days and is governable within hardcoded bounds (minimum 7 days, maximum 365 days). Channel expiry protects clients from indefinitely locked funds when a node disappears without closing the channel.

**Channel close lifecycle:**

- `closeChannel` → requires status `Open`. **Callable by `channel.client` or `channel.provider` only** (`require(msg.sender == channel.client || msg.sender == channel.provider)`). Sets status to `Closing`, records `claimedAmount`, `claimedNonce`, and `claimedBytes` from the submitted voucher, emits `ChannelCloseInitiated`. No fund transfers. Third parties cannot initiate a close — they act only via `disputeChannel` (during the dispute window) or `settleChannel` (after expiration). **Zero-voucher close:** when **either party** calls with `amount == 0`, `nonce == 0`, `bytesDelivered == 0`, an empty signature (`signature.length == 0`), and `channel.claimedNonce == 0`, the voucher signature is not verified. Full mechanic, safety argument, and dispute symmetry: [Fee Routing on Disputed Closes](#fee-routing-on-disputed-closes) §1.
- `disputeChannel` → requires status `Closing` and `block.timestamp < disputeDeadline`. Callable by any address holding a valid voucher with a strictly higher nonce. Updates `claimedAmount`, `claimedNonce`, and `claimedBytes`, emits `ChannelDisputed`. No fund transfers. Unrestricted caller access is intentional: third-party fraud detectors ([Appendix: Fraud Detection](appendix-fraud-detection.md)) must be able to submit higher-nonce vouchers on behalf of an offline party during the dispute window.
- `settleChannel` → requires status `Closing` and `block.timestamp >= disputeDeadline`. Callable by any address. Refunds `deposit - claimedAmount` to the client and, if `claimedAmount > 0`, transfers `claimedAmount` USDC to the configured `FeeRouter` and invokes `FeeRouter.routeSettlement(channel.provider, claimedBytes, claimedAmount, voucher.epochId)` in the same transaction. Sets status to `Closed`, emits `ChannelSettled`. **No fee is computed or skimmed inside this contract** — the router applies the split, pays the operator's 40% base share same-tx, and increments `bytesPerEpoch[operator][epochId]` for the [ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula) gauge formula; see [FeeRouter Integration](#feerouter-integration). The per-operator gauge-share cap from [ADR 026 §3](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) is the binding wash-trading defense.
- `reclaimExpired` → requires status `Open` and `block.timestamp >= expiresAt`. Returns the full deposit to the client (no fee deducted — no voucher was submitted). Sets status to `Closed`, emits `ChannelExpiredReclaimed`. Callable by the client or the provider. Regardless of caller, the full deposit is returned to `channel.client` — the provider cannot claim funds via this path. This ensures abandoned channels where the client is absent can be cleaned up by the provider to free on-chain state.
- `forceCloseChannel` → **production `PaymentChannel` only (not part of the PoC `StablePaymentChannel` interface).** Requires status `Open`, `channel.openedAt != 0` (channel exists), and `!allowedTokens[channel.token]` (token has been removed by governance). Callable by any address. Sets status to `Closing`, `claimedAmount = 0`, `claimedNonce = 0` (no voucher submitted), starts the dispute window. Emits `ChannelForceClosedByTokenRemoval`. The provider (or any address holding a valid voucher) can dispute during the dispute window to claim earned fees; if nobody disputes, `settleChannel` returns the full deposit to the client. See [ADR 010](010-multi-token.md) for the full multi-token context.

**Safety bounds (hardcoded):**

| Parameter | Minimum | Maximum |
| --- | --- | --- |
| Dispute window | 43200 seconds (12 hours) | 259200 seconds (3 days) |
| Min deposit | 1 base unit | No max |
| Rate floor | 1 base unit | Must be < ceiling |
| Rate ceiling | Must be > floor | No max |
| Max voucher interval | 1 MB | 1024 MB (~1 GB) |
| Max channel duration | 604800 seconds (7 days) | 31536000 seconds (365 days) |

`StablePaymentChannel` does not hold a fee-percentage parameter. Bucket-share bounds (40/40/7/5/5/3 + `boostFloor`) are owned by `FeeRouter` per [ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds).

**Rate bounds are in USDC base units (6 decimals) for the PoC.** The contract stores a single `RateBounds` struct with `deliveryFloor` and `deliveryCeiling`. Per-token rate bounds are deferred to [ADR 010](010-multi-token.md).

**Initial rate bounds (PoC):**

| Parameter | Value (USD/MB) | USDC base units | Rationale |
| --- | --- | --- | --- |
| `deliveryFloor` | $0.000001/MB | 1 | Anti-abuse minimum; 10× below expected market rate. Prevents zero-rate free-riding while imposing no practical constraint on legitimate pricing. Nodes are expected to set rates well above this floor; the floor is purely an anti-zero safeguard, not a recommended price. |
| `deliveryCeiling` | $0.001/MB | 1,000 | 100× expected market rate. Accommodates origin-backed nodes with high-egress backends (e.g., S3 at $0.09/GB) while remaining well above any legitimate pricing scenario ($1.00/GB vs Akamai's ~$0.12–0.20/GB). |

The expected market rate is $0.00001/MB (10 USDC base units per MB, or $0.01/GB). This positions deCDN ~4–9× cheaper than major traditional CDNs (CloudFront at $0.085/GB, KeyCDN at $0.04/GB) and at parity with budget providers (Bunny.net at $0.01/GB). Both bounds are governable post-PoC within the hardcoded safety constraints above.

### Rate Bounds Refresh

Nodes must keep their local `RateBounds` copy current so advertised `rate_per_mb` stays within governance-set bounds. Because rate bounds are advisory coordination parameters — the contract does not verify rate compliance during settlement or slashing — the refresh strategy is lighter-touch than the content blacklist ([ADR 011](011-content-takedown.md)), where serving blacklisted content is a slashable offense.

**Primary mechanism: event listening.** Nodes SHOULD subscribe to `RateBoundsUpdated` events on the `StablePaymentChannel` contract and update the local cache immediately. Governance actions are infrequent (days to weeks), so high-frequency polling would be wasteful.

**Fallback mechanism: periodic polling.** Nodes MUST poll `getRateBounds()` at a configurable interval (`rate_bounds_poll_interval`, default **1 hour** for PoC), guarding against missed events from RPC provider issues, WebSocket disconnections, or chain reorganizations. The 1-hour default is deliberately longer than the 10-minute registry ([ADR 001](001-network.md)) / blacklist ([ADR 011](011-content-takedown.md)) intervals: registry freshness is connectivity-critical and blacklist freshness slashing-critical, but rate-bounds staleness only risks counterparties rejecting the node's advertised rate.

#### Startup

Nodes MUST call `getRateBounds()` before accepting connections, never operating without rate bounds (same pattern as the content blacklist initial sync, [ADR 011](011-content-takedown.md)). Because `getRateBounds()` returns `uint256` but the wire protocol represents `rate_per_mb` as `u64` ([ADR 010](010-multi-token.md)), nodes MUST verify both `deliveryFloor` and `deliveryCeiling` fit within `u64` on every refresh (startup and subsequent polls/events). If either bound exceeds `u64::MAX`, the node MUST refuse to start (or, on a mid-operation refresh, continue with its last valid bounds and log an error). Unreachable in practice — the PoC ceiling is 1,000 base units — but the check guards against governance misconfiguration.

#### Stale bounds

If the event subscription is lost and RPC polling fails, the node SHOULD continue operating with its last-known bounds and log a warning. No service interruption is required. The worst-case consequence of stale bounds is that counterparties running compliant software reject the node's `rate_per_mb` as out-of-bounds — a revenue impact, not a safety violation.

#### No version-based delta pattern

Unlike the content blacklist (which uses `getBlacklistVersion()` for cheap change detection and incremental delta fetching), rate bounds are a single struct containing two `uint256` values. A version counter adds no value — the full state is readable in a single `eth_call` with negligible overhead. This is an intentional divergence from the ADR 011 pattern.

#### Multi-token extension

The PoC uses a single `RateBounds` struct. When per-token rate bounds are introduced ([ADR 010](010-multi-token.md)), the `RateBoundsUpdated` event will need a token parameter: `RateBoundsUpdated(address indexed token, uint256 newDeliveryFloor, uint256 newDeliveryCeiling)`. Nodes will subscribe with a token filter or listen for all tokens and update their local cache accordingly.

For how nodes validate `rate_per_mb` against cached bounds before signing protocol messages, see [ADR 005 — Rate Bounds Validation](005-protocol.md#rate-bounds-validation).

### BuybackBurner

| Function | Purpose |
| --- | --- |
| `executeBuyback(token, amount, minTokenOut)` | Governance multisig or `keeper`: swap `amount` of `token` for ≥ `minTokenOut` TOKEN and burn the proceeds. |
| `setKeeper(addr)` / `setSwapRouter(addr)` / `setPool(addr)` | Governance: rotate the authorized keeper, swap router, or pool. |
| `setSlippageTolerance(bps)` / `setMinBuybackAmount(n)` / `setMaxBuybackAmount(n)` | Governance: per-call execution guards. |
| `keeper() → address` / `getAccumulatedFees(token) → uint256` | Views: current keeper and accumulated buyback inflow per token. |

This is the canonical `BuybackBurner` interface. [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) defines the economic parameters and the inflow source (5% router-fed). [ADR 018](018-liquidity-strategy.md) specifies the venue (Balancer V3 Router + 80/20 weighted pool) and how `setSwapRouter` / `setPool` are configured at deployment. **V3 integration note:** `setSwapRouter` holds the Balancer V3 **Router** address, but `BuybackBurner` MUST self-approve the Balancer V3 **Vault** address (a separate contract) during initialization — the Vault pulls input tokens from the `msg.sender` of the Router call. See [ADR 018 — Buyback execution via Balancer V3](018-liquidity-strategy.md#buyback-execution-via-balancer-v3).

All `set*` functions are governance-only behind a timelock.

**PoC note:** The `BuybackBurner` is deployed with the same interface, but `executeBuyback` is not called during the PoC. The buyback allocation is the 5% same-tx burn bucket in [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553); the inflow source is `FeeRouter` (per-settlement same-tx transfer), not manual treasury transfer. USDC accumulates in the contract without being swapped during the PoC.

### FeeRouter Integration

Under [ADR 026](026-gauge-boost-tokenomics.md), `StablePaymentChannel.settleChannel` does not split fees inline. The full operator-bound USDC balance is forwarded to a `FeeRouter` contract, which applies the canonical six-bucket split (40% node base / 40% gauge boost / 7% delegator pool / 5% buyback-and-burn / 5% treasury / 3% safety reserve — full table and bounds in [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) and [§11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds)). This ADR specifies the `FeeRouter` interface only as it relates to the settlement path; the gauge formula, ve-escrow mechanics, and bucket disbursement schedule live in ADR 026 and its source design spec.

#### Settlement-path interface

`StablePaymentChannel.settleChannel` MUST invoke `FeeRouter.routeSettlement(address operator, uint256 bytesDelivered, uint256 amount, uint64 epochId)` in the same transaction as the USDC `safeTransferFrom` to the router. The router pays the operator's 40% base share in that transaction, dispatches the 5% / 5% / 3% same-tx legs, and increments `bytesPerEpoch[operator][epochId]` for the gauge formula per [ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula). The full `IFeeRouter` interface is canonical in [ADR 016](016-contract-interactions.md).

#### Settlement-path invariants

1. **Atomic base-share payout.** The 40% base share MUST land in the operator's wallet in the same transaction as `settleChannel` — no claim step, no keeper, no off-chain queue. This is the Case A cashflow guarantee from [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake).
2. **No reentry.** `settleChannel` holds a `nonReentrant` guard for the duration of the router call.
3. **One settlement per channel.** Enforced by the existing `Closed` status; the router need only tolerate duplicate calls (idempotency or revert — pinned in [ADR 016](016-contract-interactions.md)).

Conservation, same-tx satellite legs (5%/5%/3%), and epoch-consistency invariants live with the router itself in [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) / [ADR 016](016-contract-interactions.md). The `SettlementRouted` event (operator + epoch + per-bucket deltas) is emitted by the router; full event set is in [ADR 016](016-contract-interactions.md).

#### Cache-miss bypass (node-to-node paid pulls)

**Node-to-node cache-miss paid pulls bypass the router entirely.** When node B pulls a blob from origin-backed node A and pays via a payment channel, that settlement is internal cost-recovery between two operators — not net protocol revenue. Routing it would double-charge the same revenue (once when B pays A, again when B's clients pay B for the same bytes). Per [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553):

- Node-to-node settlements use direct peer USDC payment with no router invocation.
- Implementations distinguish node-to-node from client-to-node settlements via the channel's `client` and `provider` fields cross-referenced against the on-chain registry: if both addresses have a registered NodeId binding (see [NodeId-to-Ethereum Binding](#nodeid-to-ethereum-binding)), the channel is node-to-node; otherwise it is client-to-node.
- The PoC `StablePaymentChannel` may implement the bypass either by exposing a separate `settleChannelNoRoute(channelId)` entry point usable only when both parties are registered nodes, or by having `settleChannel` detect the case and skip the `FeeRouter` call. Either way the operator-to-operator USDC transfer is direct and bypasses the router's per-epoch USDC accumulators (those bytes were already counted at the client-to-node settlement that paid for them downstream). Gauge eligibility for these bytes is naturally bounded by the [ADR 026 §3 per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) — even if a colluding operator pair routed bypassed bytes through the gauge counter, each operator-identity's share is capped at 5%.
- Permissionless fraud detectors ([Appendix: Fraud Detection](appendix-fraud-detection.md)) can observe node-to-node settlements for self-routed-traffic / wash-trading patterns despite the bypass. The on-chain remedy is the per-operator gauge-share cap; off-chain reputation gauges (ADR 008 §12) consume the observation as a soft signal.

#### Settlement sequence

End-to-end USDC flow (client→node settlement, then the parallel cache-miss bypass) is diagrammed in [ADR 016 §"FeeRouter integration"](016-contract-interactions.md). This ADR documents only the `StablePaymentChannel ↔ FeeRouter` interface contract.

The full six-bucket split applies to every network deployment from launch. Simplified launch configurations are expressed by setting non-active bucket shares to zero via `FeeRouter.setShares(...)` per [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics), not by deploying a reduced-surface stub. The cross-validation invariant in that section ensures any non-zero share has a wired non-zero destination, so the launch share configuration alone determines which downstream contracts must be ready at deploy time.

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

The domain separator binds every voucher to a specific contract deployment on a specific chain. A voucher signed for one chain cannot be replayed on another, and a voucher signed for one `StablePaymentChannel` deployment cannot be replayed against an upgraded or redeployed contract at a different address.

**Voucher type:**

```solidity
bytes32 constant VOUCHER_TYPEHASH = keccak256(
    "Voucher(bytes32 channelId,uint256 amount,uint256 nonce,uint256 bytesDelivered,address token,uint64 epochId)"
);
```

The `epochId` field is consumed by `FeeRouter.routeSettlement` for per-(operator, epoch) bytes attribution feeding [ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula)'s gauge formula; the client sets it at signing time. `FeeRouter` validates that `epochId` is current or recent (within `MAX_EPOCH_LAG`, default 4 epochs) and rejects future-dated values. For long-lived channels spanning multiple epochs, the client signs separate per-epoch vouchers (incrementing `voucherNonce` across them per [Voucher Nonce Convention](#voucher-nonce-convention)); the operator submits each at the relevant `settleChannel` call cadence — typically once per epoch boundary per active channel.

**Signature digest:**

```solidity
bytes32 digest = keccak256(abi.encodePacked(
    "\x19\x01",
    DOMAIN_SEPARATOR,
    keccak256(abi.encode(VOUCHER_TYPEHASH, channelId, amount, nonce, bytesDelivered, token, epochId))
));
```

**Verification:** Implementations must use OpenZeppelin's `SignatureChecker.isValidSignatureNow(channel.client, digest, signature)`, which transparently supports both EOA signers (via hardened `ECDSA.recover` that rejects non-canonical `s` values and restricts `v` to `27`/`28`) and smart account signers (via ERC-1271 `isValidSignature`). The signature is encoded as 65 bytes (`r || s || v`) for EOA signers; smart account signers may use longer signatures per their wallet implementation. See [ADR 024](024-account-abstraction.md) for the full account abstraction design.

The `DOMAIN_SEPARATOR` is computed once in the constructor and stored as an immutable. If the contract is deployed behind a proxy and may be migrated to a different chain, it should be cached in a state variable and recomputed only when `block.chainid` changes (the pattern used by OpenZeppelin's `EIP712` base contract), rather than on every call.

### Voucher Nonce Convention

Voucher nonces within a channel start at **1**. Nonce 0 is reserved as the sentinel value meaning "no voucher has been submitted" — it is the Solidity default for `claimedNonce` in a newly opened `Channel` struct. The first client-signed voucher in a channel uses `nonce=1`, the second uses `nonce=2`, and so on. This convention ensures:

- `claimedNonce == 0` reliably identifies channels where no voucher has ever been submitted, which is the guard condition for the zero-voucher close path callable by either party (see [Channel griefing](#channel-griefing) and the channel close lifecycle in [StablePaymentChannel](#stablepaymentchannel)).
- Any real voucher (nonce ≥ 1) can always be used to dispute a zero-voucher close (which records `claimedNonce=0`), since `disputeChannel` requires strictly higher nonce.

### StakingRegistry Modifications

The full node registry interface (`NodeInfo`, `registerNode` with atomic binding, `getActiveNodes`, etc.) is defined in ADR 001. The additions below are payment-specific extensions.

> **No on-channel fee-discount path.** Operator return is differentiated through ve-locked gauge boost ([ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula)), not via a stake-multiple fee toggle on the channel contract. `getEffectiveFee`, `getStakeMultiple`, `DISCOUNT_MULTIPLE`, `feePercentage`, and `discountedFeePercentage` are not part of the interface. `StakingRegistry` retains its slashing, registration, and stake-bookkeeping responsibilities; the minimum stake is **50,000 TOKEN** ([ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake)).

No payment-specific extensions to `StakingRegistry` are required beyond the registry interface defined in [ADR 001](001-network.md).

## Admission and Priority

Node admission and queueing policy — how a node orders incoming `StreamRequest`s under congestion — is implementation-defined and lives outside the protocol. The wire format carries no priority bits, the channel and voucher mechanisms encode no per-stream priority state, and different operators are expected to tune their policy differently. Two signals are available to any admission policy:

- **Committed voucher rate.** The advertised `rate_per_mb` in `ProbeResponse` / `StreamResponse` is a **floor**, not equality — nodes verify `amount_delta / bytes_delta >= rate_per_mb`. Clients MAY commit at higher rates; nodes MAY use the committed rate as a per-stream priority key, with the premium paid directly via [`FeeRouter.routeSettlement`](#feerouter-integration).
- **Registered node-stake.** `StakingRegistry.stakeOf(address)` is readable on-chain for any registered operator. Nodes MAY treat addresses with `stakeOf >= MIN_STAKE` ([ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake)) as eligible for a higher-priority admission lane.

## NodeId-to-Ethereum Binding

The protocol requires a verifiable mapping between iroh NodeIds (ed25519 public keys) and Ethereum addresses (secp256k1-derived). This binding is used for payment channel association and slash evidence attribution. Two orthogonal signature mechanisms protect this mapping: the EIP-712 `bindingSignature` (secp256k1) proves the Ethereum key holder consents to the association — preventing un-slashable registration; the `ed25519Signature` ([ADR 001, NodeId Ownership Verification](001-network.md#nodeid-ownership-verification)) proves the NodeId's private key holder authorized the registration — preventing NodeId squatting.

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

Node registration and NodeId binding are atomic. `StakingRegistry.registerNode()` ([ADR 001](001-network.md)) accepts a `bindingSignature` parameter — an EIP-712 signature over `BindNodeId(nodeId, bindingNonce[msg.sender])` — and an `ed25519Signature` parameter proving ownership of the NodeId's ed25519 private key (see [ADR 001, NodeId Ownership Verification](001-network.md#nodeid-ownership-verification)). It verifies both signatures, writes the `nodeIdToAddress`/`addressToNodeId` mappings, and increments `bindingNonce[msg.sender]` in the same transaction that adds the node to the mesh. The per-address nonce counter is shared with `bindNodeId`, giving replay protection across both paths. This eliminates the window in which a node could be active but not slashable.

The standalone `StakingRegistry.bindNodeId()` function below remains available for **rebinding only** (key rotation after initial registration). It is no longer needed at initial registration time.

**Canonical source of truth:** The `nodeIdToAddress` / `addressToNodeId` mappings — written atomically by `registerNode` at initial registration and by `bindNodeId` on rebinding — are the authoritative source for payment attribution and slashing. `NodeInfo.ethAddress` in ADR 001 is always `msg.sender`, so the two are consistent by construction under the one-to-one constraint. If the implementation stores both, `NodeInfo.ethAddress` MUST equal `nodeIdToAddress[nodeId]` at all times.

This creates an authoritative, publicly queryable mapping:

```solidity
// StakingRegistry additions
mapping(bytes32 => address) public nodeIdToAddress;
mapping(address => bytes32) public addressToNodeId;
mapping(address => uint64) public bindingNonce;

// Bundled per-operator binding + activity view (off-chain origin discovery;
// ADR 016 § Off-Chain Read API, ADR 022 § Origin discovery). `nodeId` is
// `bytes32(0)` if the operator never registered or cleared their binding via
// rebinding; `active` is `false` if the operator is unbound, in unbonding,
// auto-ejected, or below `minStake`.
function nodeIdOf(address operator) external view returns (bytes32 nodeId, bool active);

// Single-purpose per-operator activity check. Returns `true` iff `operator` is
// currently registered with active (non-unbonding) stake at or above
// `minStake`; `false` for unregistered addresses, stake below `minStake`,
// stake fully or partially in unbonding, and auto-ejected operators.
// SECURITY: operator-level blacklist status (ADR 011) is intentionally NOT
// consulted — this is a pure single-contract storage read; callers needing the
// combined "authorized origin" predicate filter against
// `ContentBlacklist.isOriginBlacklisted` themselves
// (per [ADR 011 § Interaction with ContentBlacklist](011-content-takedown.md#interaction-with-contentblacklist)).
// Equivalent to `(_, active) = nodeIdOf(operator)` without reading the binding
// slot. Consumed by `OriginAssignment.proposeAssignment` / `activateAssignment`
// / default-open allow-list setters per [ADR 011 § Origin Assignment
// Authority](011-content-takedown.md#origin-assignment-authority).
function isActive(address operator) external view returns (bool);

// Intended for rebinding (key rotation) only — initial binding is performed
// atomically inside registerNode(). No on-chain guard prevents calling this
// before registerNode, but doing so creates a binding without mesh membership
// or stake (harmless but useless). See ADR 001.
function bindNodeId(bytes32 nodeId, bytes calldata signature) external {
    uint64 nonce = bindingNonce[msg.sender];
    bytes32 digest = keccak256(abi.encodePacked(
        "\x19\x01",
        DOMAIN_SEPARATOR,
        keccak256(abi.encode(BIND_NODE_TYPEHASH, nodeId, nonce))
    ));
    require(SignatureChecker.isValidSignatureNow(msg.sender, digest, signature), "invalid signature");

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

> **Note on EIP-712 signature:** Redundant for direct on-chain calls (`msg.sender` already authenticates the caller) but retained for: (1) future meta-transaction/relayer patterns where a third party submits the binding on the operator's behalf; (2) atomic binding inside `registerNode`, where the signature supplies the explicit cryptographic consent to associate a specific NodeId with the calling address (`registerNode` writes the mapping on behalf of `msg.sender`).

### Off-Chain (Ephemeral) Binding for Clients

Clients without on-chain registration MAY include a signed binding in their `StreamRequest` to attest a NodeId↔Ethereum-address mapping for the connection's lifetime. The node verifies the EIP-712 signature over `BindNodeId(nodeId, nonce=0)` using `SignatureChecker` semantics: `ecrecover` for EOA clients, or an RPC call to `isValidSignature` for smart account clients ([ADR 024](024-account-abstraction.md#4-off-chain-erc-1271-verification)). The verified address is cached for the connection's lifetime and used for voucher attribution. This ephemeral binding is not stored on-chain and is valid only for the session. Wire-format details are in [ADR 005](005-protocol.md#client-identity-binding).

### Binding Requirements by Role

| Role | On-chain binding required? | Rationale |
| --- | --- | --- |
| Node (staked) | **Yes** — `registerNode` performs binding atomically via `bindingSignature` (EIP-712, proves Ethereum key consent) and `ed25519Signature` (proves NodeId ownership) | Slash evidence references on-chain NodeId→address mapping; atomic binding eliminates gap; ed25519 proof prevents NodeId squatting |
| Client (opening channels) | No — channel `client` field is the Ethereum address directly | Channel operations use Ethereum addresses, not NodeIds |

### Rebinding

A node or client can rebind their Ethereum address to a new NodeId by calling `bindNodeId` (the nonce increments, invalidating the old binding). The old NodeId→address mapping is deleted. This supports key rotation scenarios (e.g., compromised iroh key). Initial binding is handled atomically by `registerNode` and does not require a separate `bindNodeId` call.

## Decimal Handling

USDC uses 6 decimals; TOKEN uses 18 decimals. All payment amounts in the `incentive` crate use USDC base units (µUSDC). The voucher signing code uses raw base units — no decimal conversion in the signature path to avoid precision bugs.

Multi-token decimal abstraction (a `Currency` enum covering arbitrary ERC-20 decimals) is deferred to [ADR 010](010-multi-token.md).

**Voucher format:**

```
{channelId, amount, nonce, bytesDelivered, token, signature}
```

During delivery over `cdn/client/v1`, `{signature, amount, nonce, bytesDelivered}` are transmitted on the wire; the remaining fields (`channelId`, `token`) are derived from stream context. The `nonce` is explicit to prevent desynchronization if a `VoucherAck` is dropped (it starts at 1 for the first voucher in a channel; 0 is reserved as a sentinel). See [ADR 005](005-protocol.md) for wire protocol details.

The `token` field (ERC-20 address) is in the signed EIP-712 typed data to prevent cross-token replay; for the PoC it is hardcoded to the USDC contract address. Full EIP-712 type definition and domain separator: [EIP-712 Voucher Signature](#eip-712-voucher-signature).

#### Voucher Bytes-Delivered Field

`bytesDelivered` is a cumulative byte count signed alongside `amount` and `nonce`. It is the canonical settlement-record byte count carried in the `Voucher`, forwarded to `FeeRouter.routeSettlement`, and aggregated into `bytesPerEpoch[operator][epochId]` for the gauge formula in [ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula). Properties:

- **Cumulative, monotonic.** Like `amount` and `nonce`, `bytesDelivered` is strictly non-decreasing across vouchers within a channel. `disputeChannel` MUST revert if the new voucher's `bytesDelivered < claimedBytes`.
- **Derivable from MB-denominated voucher cadence.** Voucher cadence is MB-denominated (default 1 MB; see [Voucher Interval Negotiation](#voucher-interval-negotiation)) and `rate_per_mb` is MB-denominated. Clients computing `amount` from `bytesDelivered` use `amount = ⌈bytesDelivered / 1_048_576⌉ × rate_per_mb` (1 MB = 1,048,576 bytes per [ADR 005](005-protocol.md)); equivalently, `bytesDelivered = mb_delivered × 1_048_576` when delivery boundaries align with MB intervals. The unit conversion is purely an off-chain arithmetic concern; the voucher carries the byte count directly so the contract does not need to re-derive it.
- **Carried through `closeChannel` / `disputeChannel` to `settleChannel`.** Recorded in `channel.claimedBytes` and forwarded as the `bytesDelivered` argument to `FeeRouter.routeSettlement` at settlement.
- **Cross-channel consistency.** A voucher signed for one channel is bound by its EIP-712 typed data; `bytesDelivered` is part of that signed payload and cannot be replayed against a different channel.

The router does not validate `bytesDelivered` against any oracle of physical delivery — the value is whatever the client signed. The gauge-pool wash-trading defense is the [ADR 026 §3 per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) bounding per-operator extraction; it does not depend on byte-truth verification at settlement.

## Slashing and Channel Interactions

Slashing and payment channels are independent by design. The following interactions apply regardless of which governance-approved tokens are in use (see [ADR 010](010-multi-token.md)).

**Slashing does not affect channel funds.** Slashing operates exclusively on TOKEN stake in the `StakingRegistry` (schedule per [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn) — 5%/15%/50% escalation tiers, 50% challenger / 30% safety / 20% burn). Channel funds are client deposits held in escrow — not stake, never touched by slashing. This follows from the functional separation in [Consequences](#consequences): payment channel contracts never hold or move TOKEN stake, cannot be called by `StakingRegistry` to slash or reassign stake, and any `StakingRegistry` interaction is read-only (e.g., resolving NodeId↔address bindings).

**Slashing can drop a node below minimum stake while channels are open.** Channel deposits being independent of stake, a node can be slashed below the minimum (or to zero) with open channels. The channels continue their normal lifecycle — close, dispute window, settle — regardless of staking status; settlement is purely a function of voucher state, not registry status.

**Auto-ejection does not interrupt open channels.** When a node's stake drops below 50% of the minimum and auto-ejection triggers (see [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn)):

- Open channels settle normally. Client funds are never trapped.
- The ejected node cannot participate in new channels (clients verify node registration before opening channels, and nodes verify counterparty status before accepting a `StreamRequest`).
- The ejected node is removed from gossip routing, so it receives no new client connections.
- `closeChannel` (client/provider only), `disputeChannel` (any address), and `settleChannel` (any address) remain callable on existing channels — these functions check channel state, not registry status.
- The node must re-stake at the full minimum and re-register to resume operations.
