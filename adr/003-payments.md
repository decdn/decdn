# ADR 003: Payment Model

**Date:** 2026-03-28
**Status:** Draft

## Context

Nodes deliver bytes and need to be paid for it. The payment mechanism must work at per-MB granularity without an on-chain transaction per delivery, and must give delivering nodes immediate protection against non-payment.

Three constraints shape the design:

1. On-chain transactions on an L2 cost ~$0.05–0.10 each — acceptable per channel lifecycle, not per MB delivered.
2. A typical delivery session transfers a few MB. The payment per MB at market rates is on the order of $0.00001 — far below any on-chain transaction cost.
3. Node operators have real infrastructure costs (VPS, bandwidth, backend storage). Revenue denominated in a volatile governance token creates unacceptable P&L risk: a 10× price drop turns a profitable operator into a loss.

## Decision

Payments use **unidirectional off-chain payment channels settled on an EVM L2, denominated in the payment token** — **USDC**, fixed at contract deployment (immutable constructor argument, 6 decimals). The spec says "the payment token" for the channel-deposit / voucher / settlement currency and names USDC only where a USDC-specific property is load-bearing (decimals, Circle counterparty risk, on-chain identifiers, swap pairs, dollar-denominated constants).

The same channel mechanism operates at two tiers:

- **Client → node**: a client opens a payment-token channel with a node, signs cumulative vouchers as MB are delivered, and the node initiates channel close on-chain and settles to claim payment after the dispute window.
- **Node → node**: when a node pulls content from another node (typically an origin-backed node) for the first time, it pays via the same channel mechanism. The origin-backed node is paid wholesale; the pulling node recoups this by serving multiple clients from its cache at a markup.

A channel is opened by depositing the payment token into the `PaymentChannel` contract. As content is delivered, the payer signs cumulative vouchers off-chain — one voucher per MB received (default cadence; negotiable for large transfers). The delivering node holds the latest voucher and submits it on-chain to initiate channel close. A dispute window (default 48 hours, governable within 12h–72h — see [ADR 009](009-governance.md#adr-009-governance-model)) allows either party to counter a stale or fraudulent close attempt. After the dispute window expires, the channel is settled and funds are distributed.

Key parameters:

- Voucher cadence: 1 MB delivered per voucher (default; negotiable up to `maxVoucherIntervalMb` for large transfers — see [Voucher Interval Negotiation](#voucher-interval-negotiation))
- Minimum deposit: 1 USDC (contract floor, governable); recommended practical minimum: 10 USDC (see [Deposit Economics](#deposit-economics))
- Fee routing: at settlement, the full operator payment-token balance is forwarded to `FeeRouter.routeSettlement(operator, bytesDelivered, amount)` in a single transaction; the four-bucket split (60% operator base, 25% buyback, 10% treasury, 5% safety) is dispatched same-tx per [ADR 026](026-tokenomics.md#adr-026-tokenomics). See [FeeRouter Integration](#feerouter-integration).
- Operator return is differentiated through the `CapacityBond` lock-to-capacity curve per [ADR 026](026-tokenomics.md#adr-026-tokenomics), not via a fee-discount mechanic on the channel contract.

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

All deCDN contracts use OpenZeppelin `SignatureChecker` for signature verification, supporting both EOA (via `ecrecover`) and smart account wallets (via ERC-1271 `isValidSignature`). Safe smart wallets are the recommended wallet type for both node operators and clients — see [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support).

Two standards can further eliminate the requirement for clients to hold the L2's native gas currency:

- **ERC-2771 meta-transactions.** A relayer submits the `openChannel` transaction on behalf of the client, paying gas. The client signs an ERC-2771 forwarding request; the relayer recoups gas from the deposit or a separate sponsorship fund. Requires adding a trusted-forwarder check to the contract.
- **ERC-4337 account abstraction.** Smart contract wallets batch payment-token approval + channel open into a single user operation. A paymaster can sponsor gas in the payment token rather than ETH. Works with unmodified contracts — no changes to `PaymentChannel` needed.

Gas abstraction via ERC-2771 or ERC-4337 paymasters is targeted at production.

### Voucher Interval Negotiation

At the default 1 MB cadence, a 10 GB blob requires 10,000 vouchers — each involving a sign, transmit, verify, and ack cycle. This overhead is unnecessary when the unacknowledged exposure per interval is negligible at typical rates.

**Parameter:** `maxVoucherIntervalMb` is a governable parameter on `PaymentChannel` defining the maximum allowed voucher interval in MB. Default: 1 MB. Hardcoded safety bounds: minimum 1 MB, maximum 1024 MB (~1 GB).

**Negotiation semantics:**

1. The client proposes a `voucher_interval_mb` in `StreamRequest` (see [ADR 005](005-protocol.md#adr-005-wire-protocol)).
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

> **Fee routing model.** `settleChannel` does not skim a fee inline; it forwards the entire operator-bound balance to `FeeRouter.routeSettlement(operator, bytesDelivered, amount)` in the same transaction. Split details: [FeeRouter Integration](#feerouter-integration).

The settled amount is still calculated **at final settlement**, after the dispute window expires, based on the highest valid voucher amount on-chain at that point. The three-step channel close lifecycle is:

1. **`closeChannel`** — callable by client or provider only. Records the submitted voucher's `amount` in `claimedAmount`, `nonce` in `claimedNonce`, and `bytesDelivered` in `claimedBytes`, sets status to `Closing`, starts the dispute window. **No fee deduction, no router call.** **Zero-voucher close:** when **either party** calls `closeChannel` with `amount=0`, `nonce=0`, `bytesDelivered=0`, and an empty signature (`signature.length == 0`) on a channel with `claimedNonce == 0`, the signature verification is skipped — no client-signed voucher is needed. All other `closeChannel` calls — any call with `signature.length > 0`, or any call where `amount != 0`, `nonce != 0`, or `bytesDelivered != 0` — require normal EIP-712/ECDSA voucher verification. This is safe because voucher nonces start at 1 (nonce 0 is the sentinel for "no voucher submitted"; see [Voucher Nonce Convention](#voucher-nonce-convention)), so any real voucher has nonce ≥ 1 and can always be submitted via `disputeChannel` (which requires strictly higher nonce than `claimedNonce`). The dispute window still applies: if a valid voucher exists, any party can submit it via `disputeChannel`. At settlement, `claimedAmount=0` means the full deposit is refunded to the client and the provider receives nothing — no router call is made for a zero-amount settlement.
2. **`disputeChannel`** (during dispute window) — callable by any address. If the submitted voucher has a strictly higher nonce, updates both `claimedAmount`, `claimedNonce`, and `claimedBytes` (see [Voucher Bytes-Delivered Field](#voucher-bytes-delivered-field)) to the new values. Still **no fee deduction, no router call**. Submissions with an equal or lower nonce revert with no state change.
3. **`settleChannel`** (after dispute window expires) — callable by anyone. If `claimedAmount > 0`, computes `bytesDelivered` from the final voucher, transfers the full `claimedAmount` of USDC to the `FeeRouter`, and invokes `FeeRouter.routeSettlement(channel.provider, bytesDelivered, claimedAmount)` in the same transaction. Refunds `deposit - claimedAmount` to the client. Sets status to `Closed`. The router (not `PaymentChannel`) applies the four-bucket split and increments `bytesPerEpoch[operator][epoch]` — where `epoch = block.timestamp / EPOCH_LENGTH` is derived inside `routeSettlement` — as an analytics counter consumed by `OperatorEmissions` per [ADR 026 § Operator Service Emissions](026-tokenomics.md#operator-service-emissions). Split legs and bounds in [FeeRouter Integration](#feerouter-integration). The wash-trading defense is capacity-shortfall slashing on `CapacityBond` per [ADR 026 § Capacity-shortfall slashing](026-tokenomics.md#capacity-shortfall-slashing) — faking bytes does not increase revenue (operator base is per-byte at settlement, paid by the client) and capacity-shortfall slashing auto-downgrades operators with sustained delivery below `min_delivery_ratio × declared_capacity`.

   > **Invariants:**
   > 1. `closeChannel` and `disputeChannel` MUST revert if the submitted voucher's `amount > channel.deposit`. This prevents client bugs or malicious over-deposit vouchers from causing an underflow revert in `settleChannel` that would lock the channel.
   > 2. `disputeChannel` MUST revert if `newAmount < claimedAmount` or `newBytes < claimedBytes`. Vouchers are cumulative across both axes; a higher nonce must correspond to a non-decreasing amount and a non-decreasing byte count. This prevents a malicious client from reducing the provider's payout — or the provider's analytics-counter byte share — via a higher-nonce dispute.
   > 3. `settleChannel` MUST forward `claimedAmount` of the payment token to `FeeRouter` and call `routeSettlement` in the same transaction iff `claimedAmount > 0`. The provider's 60% base share lands in the operator's wallet in the same transaction as `settleChannel`; this is the cashflow guarantee that backs operator P&L Case A in [ADR 026 § Operator economics](026-tokenomics.md#operator-economics). Reverting after partial transfer is unacceptable — implementations MUST use checks-effects-interactions, MUST guard `settleChannel` and `disputeChannel` with a `nonReentrant` modifier (the `FeeRouter` call path crosses a contract boundary and is the new reentrancy surface), and the `FeeRouter` MUST hold a stable interface contract.

A dispute that raises the settlement amount (e.g., 50 → 80 payment-token units) raises every router-bucket allocation proportionally, and a higher `claimedBytes` raises the operator's analytics-counter byte share for the epoch (read by `OperatorEmissions`). The router computes its split once, on the final settled amount and byte count — never on intermediate values, never more than once per channel.

The governance token (TOKEN) is not used for delivery payments. It is reserved for operator capacity bonding (see [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)) and governance (see [ADR 009](009-governance.md#adr-009-governance-model)).

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

Surfacing these reasons off-chain saves both parties the gas of a doomed on-chain submission and gives the payer enough detail to recover (e.g., refresh state and re-sign for `StaleNonce`, top up for `InsufficientDeposit`) instead of an opaque connection drop. Riding in-band rather than via a QUIC stream reset preserves the reason for client retry logic without burning [ADR 013](013-schema-evolution.md#adr-013-schema-evolution) application-error-code numbers for the structured-response case.

## Consequences

### Positive

- On-chain costs are amortized across an entire channel lifetime — open + close + settle = three transactions regardless of how many MB are delivered (settle can be called by any address, allowing third-party settlement bots)
- USDC denomination gives node operators predictable unit economics: delivery revenue covers infrastructure costs without exposure to TOKEN price movements
- The voucher is the payment receipt; the BLAKE3 hash is the delivery receipt. Together they provide mutual protection: the client doesn't sign a voucher for bytes that fail hash verification; the node stops delivering if vouchers stop arriving
- Maximum risk per voucher interval at default cadence (1 MB) is $0.00001 at market rate — negligible. At the governance maximum interval (1024 MB) and ceiling rate ($0.001/MB), worst-case risk is $1.024 per interval — still small relative to the recommended 10 USDC minimum deposit (see [Voucher Interval Negotiation](#voucher-interval-negotiation))
- Market-driven rate setting means replication happens organically: profitable content gets cached by more nodes, driving prices down without any coordination protocol
- The `PaymentChannel` contract is functionally separated from the `CapacityBond`, keeping the audit surface for each contract's core logic bounded

### Negative

- Clients must hold the payment token and the L2's native gas currency to use the network; this adds an onboarding step compared to a single-currency model. Gas-overhead percentages and the gasless-open deferral are quantified in [Deposit Economics](#deposit-economics)
- Rate volatility: a node can change its advertised rate between a probe and a stream request; the `StreamResponse` rate is the binding one, but a client that probed at one rate and receives a higher rate in `StreamResponse` must disconnect and re-probe rather than having been deceived silently. Rate changes more than 30 seconds after the probe are not slashable; the 30-second window is precisely defined as `stream_response.timestamp_us >= probe_response.timestamp_us && stream_response.timestamp_us - probe_response.timestamp_us < 30_000_000` using requester-anchored timestamps in both signed messages (see [ADR 005](005-protocol.md#adr-005-wire-protocol))
- USDC is issued by Circle, which can freeze specific addresses or blacklist the contract. This counterparty risk is accepted: the payment token is fixed to USDC at deployment and the protocol does not implement payment-token substitution

## Attack Vectors

### Client-side

#### Voucher withholding

Client receives bytes but stops signing vouchers, getting content for free up to the last signed interval.

The self-enforcing stop is sufficient. Maximum loss is one voucher interval at the negotiated cadence: default cadence (1 MB × market rate ≈ $0.00001) is negligible; 100 MB at market rate is ~$0.001; governance maximum (1024 MB) at ceiling rate is ~$1.024 — still negligible relative to channel deposits. Nodes serving high-value content can unilaterally enforce smaller intervals regardless of what was negotiated.

#### Channel griefing

Client opens many channels with minimum deposit and never streams, forcing nodes to track and eventually close stale channels.

**Resolved: zero-voucher close (either party).** The zero-voucher close mechanic — canonically specified in [Fee Routing on Disputed Closes](#fee-routing-on-disputed-closes) — bounds the maximum tracking duration to the dispute window (48 hours default) rather than the full 90-day channel expiry, and is permissionlessly disputable if the closing party actually signed a voucher off-chain. No additional inactivity timer or separate expiry mechanism beyond the existing channel expiry / `reclaimExpired` path is needed; that existing escape hatch remains as a final fallback for cases where the channel is abandoned without any close action at all.

**Why symmetric.** The provider needs the path to release abandoned channels they track. The client needs it so they aren't locked into 90 days of `reclaimExpired` waiting when a node fails before the first 1 MB voucher boundary — a routine ops failure with no malicious actor. Restricting the path to providers would create a structural liquidity-lock on every node-failure event, contrary to the intended failure-mode posture. The 48h dispute window plus permissionless `disputeChannel` cover the symmetric attack surface (a client signing vouchers off-chain then trying to repudiate them via zero-voucher close) exactly as they cover the analogous [stale close](#stale-close) attack.

The griefing attacker's financial cost stays bounded: at the recommended 10 USDC minimum, $1,000 opens 100 channels; the counterparty (or the attacker, to recover the deposit) closes them all and each settles after the dispute window with full client refund (no profit motive) and ~$0.18 gas per close+settle pair. Total gas exposure for 100 channels is ~$18 — enough to warrant additional mitigations for high-volume attacks:

- **Option A — On-chain channel cap per address.** The `PaymentChannel` contract enforces a maximum number of open channels per client Ethereum address (e.g., 10). Hard to circumvent without new wallet addresses, each requiring on-chain funding.
- **Option B — Node-side filtering.** Nodes refuse `StreamRequest` from channels that have been open longer than N days with zero vouchers. Off-chain, no contract change needed, but relies on node operator implementation.

#### Stale close

Client submits an old voucher (lower amount) to close the channel, underpaying the node.

The dispute window (default 48 hours, raised from 24 hours to account for L2 forced-inclusion delay; see [L2 sequencer censorship](#l2-sequencer-censorship) below) covers this if the node is online. **Defense layers:**

1. **In-process dispute monitor.** A lightweight thread inside the node binary watches the chain for `ChannelCloseInitiated` events on its channels and auto-submits the latest voucher via `disputeChannel`. Zero-latency to the local voucher store; handles the common case where the node is online. Implementation is a SHOULD for production node binaries.
2. **Operator-arranged redundancy.** Multi-instance deployments, hot-standby relays, peer agreements to relay vouchers. Out of protocol scope; the protocol does not define a wire format for voucher-relay arrangements between operators.
3. **Permissionless on-chain dispute submission.** `disputeChannel` accepts submissions from any address holding a higher-nonce voucher — operators with their own infrastructure or counterparties can submit directly. See [Appendix: Fraud Detection](appendix-fraud-detection.md#appendix-permissionless-stale-close-detection).

The node-offline-for-the-full-48h case is a node-operations responsibility, not a protocol gap.

#### Probe fishing

Client sends probe requests to many nodes at high frequency to map the network or exhaust node resources without ever paying.

Per-NodeId rate limiting alone is bypassable: clients are not staked, NodeIds are free to rotate, and iroh connection setup is cheap. The mitigation is the layered token-bucket rate limit in [ADR 005 § Probe rate limiting](005-protocol.md#probe-rate-limiting): per-peer (NodeId) plus per-IP plus a global node cap, applied before any signature or hold-slot allocation. The per-IP layer raises the cost of bulk probing because IP rotation requires money (proxies, IPv6 delegation, cloud bills) while NodeId rotation does not; the global cap is defence in depth.

**Note:** Probe responses are considered public information (see [ADR 005](005-protocol.md#adr-005-wire-protocol)). The concern here is resource exhaustion from bulk probing, not information leakage — content availability is discoverable via probing (see [ADR 005](005-protocol.md#adr-005-wire-protocol)), and pricing is revealed in probe/stream responses by design.

#### Double-spend across nodes

Client opens channels with multiple nodes using the same payment-token deposit via a race condition before the on-chain state settles.

Each `openChannel` call transfers the payment token into the contract immediately; the client's wallet balance is debited on-chain before the transaction finalises. No credit facility exists.

### Node-side

#### Data withholding

Node accepts a stream request, receives a voucher, then stops delivering bytes.

Self-enforcing: the node cannot extract more payment than the last acknowledged voucher. The client resumes from `byte_offset` on a different node.

#### Corrupted delivery

Node serves bytes that don't match the advertised BLAKE3 hash.

Absorbed at the wire by progressive BLAKE3 verification at the client (mandatory in `cdn/client/v1` per [ADR 002](002-content-addressing.md#adr-002-content-addressing) and [ADR 005](005-protocol.md#adr-005-wire-protocol)). Vouchers are signed and sent only after the corresponding chunks have been verified — a corrupt window therefore yields no voucher. The client drops the connection, requests the blob from a different node, and recovers any unspent channel funds via channel-close. **Client monetary loss in the corruption case is zero**; the only cost is downstream bandwidth (sunk regardless of outcome).

No on-chain slash machinery is needed for content corruption. The threat is bounded in framing parallel to [§ Voucher withholding](#voucher-withholding) above: per-encounter wasted bandwidth is capped at one `voucher_interval` on each side (the client's downstream cost for a corrupt window; the node's upstream cost when a correctly-withheld voucher leaves the window unpaid). Both sides set local acceptance policies — nodes refuse continued service to keys with elevated voucher-withhold rates and may cap total bytes for keys without established history; clients prefer nodes whose probe and delivery history they trust — without protocol-level coordination. Client reputation is a node-local concern; this ADR does not specify a wire format or on-chain surface for it.

#### Rate bait-and-switch

Node advertises a low rate in probe responses then returns a higher rate in `StreamResponse`.

**Resolved: slashable offense.** Both responses are signed over the advertised rate ([ADR 005](005-protocol.md#adr-005-wire-protocol)); a same-NodeId signed pair where `StreamResponse.rate_per_mb > ProbeResponse.rate_per_mb` and the requester-anchored timestamp delta is under 30 seconds is on-chain-verifiable evidence. Clock-skew immune (both timestamps originate from the requester's clock; the node echoes them back in its signed response). The slash schedule lives in [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn); see [ADR 014 § Slash Signatures — secp256k1 EIP-712](014-on-chain-verification.md#slash-signatures--secp256k1-eip-712) for the on-chain verifier.

#### Phantom blob announcement

Node announces a blob as cached (`has_blob: true` in a signed `ProbeResponse`) then fails or redirects on actual request.

**Resolved: slashable offense.** A same-NodeId signed `ProbeResponse(has_blob: true)` paired with a signed `StreamResponse(ok: false)` or redirect for the same hash within a 30-second requester-anchored timestamp window is on-chain-verifiable evidence. The bare timeout / non-response case is reputation-only (no second signed message → not slashable on-chain). Slash schedule per [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn); on-chain verifier per [ADR 014 § Slash Signatures — secp256k1 EIP-712](014-on-chain-verification.md#slash-signatures--secp256k1-eip-712); slash executes synchronously at submit time per [ADR 014 § Bond Handling](014-on-chain-verification.md#bond-handling).

To prevent legitimate cache eviction from producing false slash evidence inside the 30-second window, nodes MUST honor a **probe-triggered eviction hold** (35s, 30s slash window + 5s margin) — see [ADR 005 § Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold) for the requirement and dependent parameters (probe cache TTL, `probe_hold_duration`). Hold violations under OOM / under-provisioning fall to the same 24-hour counter-window; eviction logs are not on-chain verifiable, so only delivery-receipt counter-evidence rebuts. The protocol does not subsidize under-provisioning.

#### Channel close front-running

Node monitors the mempool and front-runs a client's channel close with a higher voucher submission.

Not an attack. The contract always settles the highest valid voucher, and only the client can sign a valid voucher; a node submitting the latest voucher before the client is the intended happy path. Fabricating a higher voucher requires forging the client's ECDSA signature, which is cryptographically infeasible.

#### Third-party forced channel close (DoS)

A third party holding a valid voucher calls `closeChannel` to force the channel from `Open` to `Closing`, halting delivery.

**Resolved: access control restriction.** `closeChannel` requires `msg.sender == channel.client || msg.sender == channel.provider`. Third parties cannot initiate a close regardless of whether they hold a valid voucher. Permissionless fraud detection is unaffected — third parties operate via `disputeChannel` during the dispute window ([Appendix: Fraud Detection](appendix-fraud-detection.md#appendix-permissionless-stale-close-detection)). The residual risk is a `disputeChannel` call with an intercepted voucher, which can only *improve* the settlement (higher nonce required). On-path network interception of vouchers is mitigated by QUIC transport (TLS 1.3), though this does not address endpoint compromise or other forms of leakage.

### Network-level

#### Eclipse attack

Attacker surrounds a client with malicious nodes so all probe responses come from nodes under attacker control.

BLAKE3 verification catches data corruption regardless of peer-table composition; the remaining DoS variant (attacker-controlled peer set refuses to serve) is resolved in [ADR 012 § Bootstrap and Trust Model](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model): production uses multi-source bootstrap (on-chain registry + hardcoded DNS seeds) so an attacker must compromise both to fully eclipse a client; minimum honest-peer diversity is a supplementary client-side policy.

#### Gossip flooding

Node sends high-volume `NodeAnnounce` messages to exhaust peer table memory or crowd out legitimate announcements.

Registry check + per-sender rate limiting. Residual gap: the local registry cache may be up to 10 minutes stale, briefly allowing recently-unstaked nodes to flood; mitigated by tightening the registry cache refresh on high flood detection.

#### Sybil nodes

Attacker stakes many cheap nodes to dominate probe responses for popular content, controlling pricing in a region.

The core weakness is governance-token-price dependency: at $0.001/TOKEN, the 1 Gbps entry-tier capacity bond (~50,000 TOKEN at default `k=12.6`, `α=1.2` per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)) costs $50 per sybil node. Higher tiers are super-linearly more expensive (10 Gbps ≈ 795K TOKEN; 100 Gbps ≈ 12.6M TOKEN), but a sybil fleet can be dominated by many entry-tier nodes. The unified selection score `rate_per_mb × rtt_ms × (1 / max(reputation, 0.1)²)` (see [ADR 001](001-network.md#node-selection-algorithm)) helps — a sybil fleet must be real hardware in the right geography, competitively priced, and build reputation over time — but does not eliminate the risk when the token is cheap. Options:

- **Option A — Governance raises the 1 Gbps-tier bond via the `k`-bound mechanism in [ADR 026 § Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds).** The 1G-tier bond is governable within [10K, 200K TOKEN]; raising it lifts the whole curve. Governance is incentivised to do so when TOKEN price is low, since a sybil-dominated network reduces usage and TOKEN value. Reactive but aligned.
- **Option B — Bond denominated in USD equivalent via oracle.** Requires a price oracle, which introduces oracle dependency, manipulation, and downtime risks (see rate bounds discussion above). The same concerns apply here, but the impact of oracle failure is lower (new operators temporarily blocked, not payments broken).
- **Option C — Reputation as a second filter.** New nodes (low reputation, few settled channels) are deprioritised in client selection even if their `rate_per_mb × rtt_ms × (1 / max(reputation, 0.1)²)` score is competitive. A sybil fleet takes time to build reputation, limiting its effectiveness during that window.
- **Option D — Capacity-shortfall slashing.** Sybil nodes that don't deliver real bytes are auto-downgraded by the capacity-shortfall path per [ADR 026 § Capacity-shortfall slashing](026-tokenomics.md#capacity-shortfall-slashing); operators delivering below `min_delivery_ratio × declared_capacity` lose the bond delta to `SafetyReserve`. This forces sybils to either deliver real bytes (defeating the cheap-sybil premise) or eat continuous bond losses.

#### Rate manipulation cartel

Colluding nodes in a region hold rates artificially high.

Origin-backed nodes set the effective price ceiling for any blob. Clients can always probe origin-backed nodes directly and pay their rates as a guaranteed fallback. Any node outside the cartel that undercuts wins all local traffic — the incentive to defect is strong. New entrants can join the cache-only role permissionlessly by staking; the origin role for content in registered namespaces requires `OriginAssignment` membership ([ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)), but cache-only competition is sufficient to discipline the rate cartel because cache delivery is interchangeable with origin delivery from the requester's perspective.

#### Content withholding

A node stakes, responds to probes with `has_blob: true`, but refuses to serve — collecting credibility in the peer table without actually participating.

**Withholding is not a slashable offense** — operators may legitimately take content offline for maintenance, migration, or business reasons, and slashing for availability creates perverse incentives. The protocol does not guarantee availability; publishers who want fault tolerance opt into it by proposing multiple operators, and the network deprioritizes flaky nodes through reputation:

- **Publisher-chosen operator sets.** Content owners hold a publisher identity ([ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces)) and propose an origin operator set per namespace. Governance ratifies the proposal via the standard timelock path ([ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)). Set size is the publisher's call — a single trusted operator works for hobbyist publishers, multi-operator sets defuse single-point withholding for publishers who want it. Default-open content uses the DAO-maintained allow-list ([ADR 011 § Default-open allow-list](011-content-takedown.md#default-open-allow-list)) instead, with the same shape — governance picks how many operators to seat.
- **Reputation fast-path.** Nodes that respond `has_blob: true` to probes but fail to deliver accumulate reputation penalties at a steeper rate. A node with consistently poor availability is deprioritized in provider selection and loses delivery revenue. Publishers may use the reputation signal as input when proposing or revoking operators in their namespace's assignment.

Note: the probe-triggered eviction hold ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)) addresses a related but distinct problem. Withholding is a node that has the blob but refuses to serve it (behavioral — handled by reputation). The eviction hold addresses a node that signed `has_blob: true` but lost the blob to cache pressure before the stream request (mechanical — prevented by the hold and, if the hold fails, treated as a slashable phantom announcement).

#### Replay attack on vouchers

Attacker intercepts a signed voucher and attempts to replay it against a different channel or after close.

EIP-712 typed data over `{channelId, amount, nonce, bytesDelivered, token}` binds the voucher to a specific channel. The EIP-712 domain separator (see [EIP-712 Voucher Signature](#eip-712-voucher-signature)) further binds each voucher to a specific chain and contract deployment, preventing replay across different L2s, contract upgrades, or test vs production environments. The monotonically increasing nonce (starting at 1; see [Voucher Nonce Convention](#voucher-nonce-convention)) prevents resubmission after settlement.

#### Off-chain voucher state persistence

The on-chain protections in [Replay attack on vouchers](#replay-attack-on-vouchers) constrain only what the contract accepts at settlement. They do not prevent the **delivering node** from re-delivering bytes off-chain for a voucher it already honoured: a node holding voucher state only in memory will, after restart, re-accept any earlier-nonce voucher the client (or any wire observer) resubmits and serve the bytes again.

Required invariant: a node MUST persist `(last_nonce, last_amount, last_bytes_delivered)` per channel and durably commit (fsync, on disk-backed implementations) **before** sending `VoucherAck` or delivering any further bytes for that voucher. After a restart, voucher acceptance MUST resume from the persisted state — never from `last_nonce = 0`. An absent entry is semantically identical to a never-seen channel (`last_nonce == 0`, per [Voucher Nonce Convention](#voucher-nonce-convention)); a record exists iff the node ever advanced past the initial sentinel. Entries are dropped only when the node observes `ChannelSettled` on-chain.

A failed persist write MUST surface as a voucher-acceptance failure — the node returns a transient-failure rejection through the [Off-chain Voucher Rejections (Wire Encoding)](#off-chain-voucher-rejections-wire-encoding) channel, and MUST NOT send `VoucherAck`. The specific wire code for transient persistence failures is left to the `cdn/client/v1` handler implementation; the existing `StaleNonce` / `InsufficientDeposit` codes are NOT appropriate substitutes because they would tell the client to refresh state or top up the deposit when in fact the same voucher should be retried unchanged. Persisting after acknowledgement re-opens the same replay window for the crash interval between the two writes.

Storage backend and trait shape are implementation concerns; the Rust implementation exposes a `ChannelStateStore` seam in `crates/incentive` with a `redb`-backed persistent implementation in `crates/node`. The protocol fixes only the ordering above.

## Contract Interfaces

### PaymentChannel

The `PaymentChannel` is the payment-channel contract, handling payment-token channels. The payment token is USDC; its address is fixed at deployment as an immutable constructor argument.

**Channel state:**

```solidity
struct Channel {
    address client;
    address provider;
    address token;            // USDC; set once at deployment (immutable)
    uint256 deposit;          // in USDC base units (6 decimals)
    uint256 claimedAmount;    // cumulative amount claimed via vouchers
    uint256 claimedNonce;     // nonce of the current best voucher, for dispute comparison
    uint256 claimedBytes;     // cumulative bytes delivered per the current best voucher; forwarded to FeeRouter at settlement (see ADR 026)
    uint256 openedAt;
    uint256 expiresAt;
    uint8   status;           // 0 = Open, 1 = Closing (dispute window active), 2 = Closed (settled)
    uint256 disputeDeadline;  // set when close is initiated; may be extended once via the forced-inclusion path (see § L2 sequencer censorship)
    bool    extended;         // true if disputeDeadline has been extended once via the forced-inclusion path; reset to false on closeChannel
}
```

**Channel ID:** `channelId = keccak256(abi.encodePacked(client, provider, channelNonce))` where `channelNonce` is a monotonic per-client counter stored on-chain as `clientChannelNonce[msg.sender]`. **Ordering:** `openChannel` reads the current nonce, uses it to compute `channelId`, then increments: `n = clientChannelNonce[msg.sender]; channelId = keccak256(..., n); clientChannelNonce[msg.sender] = n + 1`. The client pre-computes the next channelId off-chain by reading `clientChannelNonce[client]` and using that value directly — no off-by-one because the contract uses the same value before incrementing. The `channelNonce` is global per-client (not per-provider), ensuring uniqueness across all of a client's channels.

> **Terminology:** `channelNonce` (the channel creation counter) is distinct from the voucher `nonce` (the monotonic sequence number within a channel used in EIP-712 voucher signatures). The former uniquely identifies channels; the latter orders vouchers within a channel. In implementation, consider naming the on-chain mapping `clientChannelCounter` to avoid confusion with voucher nonces.

| Group | Function | Purpose |
| --- | --- | --- |
| Nonce | `clientChannelNonce(client) → uint256` | Per-client monotonic counter used in `channelId` derivation. |
| Lifecycle | `openChannel(provider, deposit) → channelId` | Open a payment-token channel; increments `clientChannelNonce[msg.sender]` then derives `channelId`; emits `ChannelOpened`. |
| Lifecycle | `topUp(channelId, additionalDeposit)` | Client-only: add funds to an open channel (does not extend `expiresAt`). |
| Lifecycle | `closeChannel(channelId, amount, nonce, bytesDelivered, signature)` | Client or provider: initiate close with the latest voucher; starts dispute window. |
| Lifecycle | `disputeChannel(channelId, amount, nonce, bytesDelivered, signature)` | Any address: submit a higher-nonce voucher during the dispute window. |
| Lifecycle | `settleChannel(channelId)` | Post-dispute-window: forward `claimedAmount` of the payment token to `FeeRouter`; refund unused deposit. |
| Lifecycle | `reclaimExpired(channelId)` | Client or provider: refund full deposit on an expired channel that was never closed. |
| View | `getChannel(channelId) → Channel` | Read the on-chain `Channel` struct. |
| View | `getRateBounds() → (floor, ceiling)` | Current `RateBounds` in payment-token base units. |
| View | `feeRouter() → address` | Configured `FeeRouter` target ([ADR 026](026-tokenomics.md#adr-026-tokenomics)). |
| Governance | `setFeeRouter(addr)` | Replace router target. `GOVERNANCE_ROLE`-gated; routed through the standard 48h `TimelockController` delay; emits `FeeRouterUpdated(address oldRouter, address newRouter)`. See [§ Governance setter: setFeeRouter](#governance-setter-setfeerouter) below. |
| Governance | `setMinDeposit(amount)` | Minimum channel deposit. |
| Governance | `setDisputeWindow(seconds)` | Dispute window (bounded 43200–259200 — 12h–72h). |
| Governance | `setRateBounds(floor, ceiling)` | Rate floor and ceiling in payment-token base units. |
| Governance | `setMaxVoucherIntervalMb(mb)` | Max negotiable voucher interval (bounded 1–1024 MB). |

Bucket shares (60/25/10/5) are governed on `FeeRouter`, not on `PaymentChannel`; the treasury share (10%) is configured on `FeeRouter`.

#### Governance setter: setFeeRouter

```solidity
function setFeeRouter(address newRouter) external onlyRole(GOVERNANCE_ROLE);

event FeeRouterUpdated(address indexed oldRouter, address indexed newRouter);
```

`setFeeRouter` re-points the configured `FeeRouter` for future `settleChannel` calls. Required because the audited contract surface is fixed at deploy time, yet the `FeeRouter` may need replacing (bug fix, structural upgrade) without redeploying `PaymentChannel` and forcing every open channel to re-issue vouchers.

**Authority and timelock.** Only callable by `GOVERNANCE_ROLE` (held by the `TimelockController` post-deploy per [ADR 016 § Post-Deployment Initialization](016-contract-interactions.md#post-deployment-initialization)). `DecdnGovernor` proposals to replace the router execute through the standard 48h timelock per [ADR 009](009-governance.md#adr-009-governance-model). Calls outside that path revert.

**Validation.** Reverts on `address(0)` and on the same address as the current `feeRouter`. The new router contract is not interrogated at the setter — the cross-validation invariants in [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics) live on `FeeRouter` itself; replacing the router with a misconfigured deployment surfaces at the next `settleChannel` rather than at the setter.

**Open channels are unaffected.** Vouchers signed against this `PaymentChannel` remain valid because the EIP-712 domain separator hashes the contract's own address, not the configured `FeeRouter`. Carve-out documented in [ADR 016 § No proxy deployment patterns](016-contract-interactions.md#no-proxy-deployment-patterns): helper-contract addresses are not domain-separator inputs and may be re-pointed via governance without invalidating signatures.

**Settlement during the swap.** Settlements beginning before the timelock executes use the previous router; those beginning after use the new one. `settleChannel` reads `feeRouter()` at call time, and `routeSettlement` is a single transaction, so no in-flight settlement splits across routers.

> **Reentrancy protection:** All state-mutating functions that perform external calls (ERC-20 transfers) — `openChannel`, `topUp`, `settleChannel`, `reclaimExpired` — MUST use `nonReentrant` guards and follow checks-effects-interactions.

**`topUp` behavior:** `topUp(channelId, additionalDeposit)` adds funds to an open channel:

- **Status precondition:** MUST require status `Open` and `block.timestamp < expiresAt` (reverts on `Closing`, `Closed`, or expired).
- **Caller:** client only (`require(msg.sender == channel.client)`).
- **Effects:** Transfers `additionalDeposit` from `msg.sender` to the contract via `safeTransferFrom`. Updates `channel.deposit += additionalDeposit`. Does NOT extend `expiresAt` (to prevent indefinite lock-in — the channel's utility is bounded by the initial `maxChannelDuration`).
- **Modifiers:** `nonReentrant`.
- **Emits:** `ChannelToppedUp(channelId, additionalDeposit, newDeposit)`.

#### Initial deployment values

The constructor takes `(usdc, feeRouter, disputeWindow)` and sets the remaining governable parameters: `maxVoucherIntervalMb = 1` (1 MB) and `maxChannelDuration = 7776000` (90 days). All values are within the hardcoded safety bounds table further below (see also [ADR 009](009-governance.md#adr-009-governance-model) for governance ranges). The constructor MUST reject `feeRouter == address(0)` and a `feeRouter` whose code size is zero (EOA / undeployed address).

Default deployment value for `disputeWindow`: **172800 seconds (48 hours)** — raised from 24 hours to guarantee effective dispute response time under L2 sequencer censorship (see [§ L2 sequencer censorship](#l2-sequencer-censorship) below). Safety bounds per [ADR 009](009-governance.md#adr-009-governance-model): 43200–259200 seconds (12h–72h). Under [ADR 026](026-tokenomics.md#adr-026-tokenomics) the `feePercentage` / `discountedFeePercentage` / treasury-address constructor parameters from earlier drafts are removed; bucket shares are governed on `FeeRouter` instead, and the treasury bucket is one of `FeeRouter`'s four buckets (see [FeeRouter Integration](#feerouter-integration)).

#### L2 sequencer censorship

A malicious closer (or colluding sequencer) submits `closeChannel` with a stale voucher and ensures all `disputeChannel` transactions are censored for the full dispute window. Counterparties fall back to L1 forced inclusion, but this takes up to ~24 hours on Arbitrum (similar paths on other OP-Stack chains). If the dispute window is also 24 hours, effective dispute response time is zero by the time the forced-inclusion transaction is processed.

**Mitigation — layered defense.** The dispute window default is **48 hours** (172800 seconds), which guarantees at least 24 hours of effective dispute response time on any L2 with a forced-inclusion delay ≤ 24 hours. The setting is L2-agnostic and stays within the [ADR 009](009-governance.md#adr-009-governance-model) governance bounds (12h–72h).

On top of that baseline, `disputeChannel` implements a **forced-inclusion deadline extension**: if a `disputeChannel` transaction arrives via L1 forced inclusion, the remaining dispute time is less than 24 hours, and `channel.extended == false`, `disputeDeadline` is set to `block.timestamp + 24 hours` and `channel.extended` is set to `true` — guaranteeing 24 hours of dispute time from the moment the forced-inclusion transaction is processed. `closeChannel` resets `channel.extended` to `false` so a new close cycle starts fresh.

Constraints on the extension mechanism:

- **One extension per close.** Enforced on-chain by the `channel.extended` flag: once set, subsequent forced-inclusion `disputeChannel` calls do not trigger a further extension. This bounds worst-case settlement delay to `disputeWindow + 24h`.
- **Only forced-inclusion transactions.** Normal sequencer-included `disputeChannel` calls do not trigger the extension, preventing abuse.
- **L2-specific detection.** Identifying a forced-inclusion transaction is L2-specific. On Arbitrum, this can be detected via `ArbSys` precompile or delayed-inbox origin; on OP Stack, via L1 message origin. Exact detection logic is finalized at L2 selection — see [Appendix: L2 Deployment](appendix-l2-deployment.md#appendix-production-l2-deployment-target).
- **Governance must not set the dispute window below the L2's maximum forced-inclusion delay.** On an L2 with ~24h forced inclusion the 12h governance floor is not safe — the extension never executes because `settleChannel` becomes callable before the forced-inclusion `disputeChannel` arrives. The 12h floor remains as a hardcoded safety bound for L2s with shorter forced-inclusion paths.

#### Events

All events use indexed `channelId` plus an indexed actor field where applicable. `ChannelOpened` uses three indexed fields (`channelId`, `client`, `provider`) — the EVM maximum — so wallets and CLIs can `eth_getLogs` filter by either party without parsing tx history.

| Event | Emitted by | Non-indexed fields |
| --- | --- | --- |
| `ChannelOpened(channelId, client, provider, …)` | `openChannel` | `deposit`, `expiresAt` |
| `ChannelCloseInitiated(channelId, initiator, …)` | `closeChannel` | `amount, nonce, bytesDelivered, disputeDeadline` |
| `ChannelDisputed(channelId, disputor, …)` | `disputeChannel` | `newAmount, newNonce, newBytes` |
| `ChannelSettled(channelId, provider, …)` | `settleChannel` | `routedAmount` (payment token forwarded to `FeeRouter` = `claimedAmount`), `bytesDelivered` (counted toward operator's epoch byte counter), `clientRefund` |
| `ChannelExpiredReclaimed(channelId, client, …)` | `reclaimExpired` | `deposit` |
| `ChannelToppedUp(channelId, …)` | `topUp` | `additionalDeposit, newDeposit` |
| `RateBoundsUpdated` | `setRateBounds` | `newDeliveryFloor, newDeliveryCeiling` |

`ChannelOpened` is the entry point for off-chain channel enumeration: a client lists their channels via `eth_getLogs(topics=[ChannelOpened, *, paddedClientAddress])`; a provider does the same with their address in the third topic; an indexer keys on `channelId`. This ensures channels are discoverable via log scans even if they have not yet had any subsequent on-chain activity (no `topUp`, `closeChannel`, or `disputeChannel`).

`ChannelSettled` carries no `protocolFee` field — `settleChannel` does not skim a fee inline. The bucket distribution emits its own events from `FeeRouter` (see [FeeRouter Integration](#feerouter-integration)).

**Channel expiry:** `expiresAt` is set at channel open: `expiresAt = block.timestamp + maxChannelDuration`. The `maxChannelDuration` parameter defaults to 90 days and is governable within hardcoded bounds (minimum 7 days, maximum 365 days). Channel expiry protects clients from indefinitely locked funds when a node disappears without closing the channel.

**Channel close lifecycle:**

- `closeChannel` → requires status `Open`. **Callable by `channel.client` or `channel.provider` only** (`require(msg.sender == channel.client || msg.sender == channel.provider)`). Sets status to `Closing`, records `claimedAmount`, `claimedNonce`, and `claimedBytes` from the submitted voucher, emits `ChannelCloseInitiated`. No fund transfers. Third parties cannot initiate a close — they act only via `disputeChannel` (during the dispute window) or `settleChannel` (after expiration). **Zero-voucher close:** when **either party** calls with `amount == 0`, `nonce == 0`, `bytesDelivered == 0`, an empty signature (`signature.length == 0`), and `channel.claimedNonce == 0`, the voucher signature is not verified. Full mechanic, safety argument, and dispute symmetry: [Fee Routing on Disputed Closes](#fee-routing-on-disputed-closes).
- `disputeChannel` → requires status `Closing` and `block.timestamp < disputeDeadline`. Callable by any address holding a valid voucher with a strictly higher nonce. Updates `claimedAmount`, `claimedNonce`, and `claimedBytes`, emits `ChannelDisputed`. No fund transfers. Unrestricted caller access is intentional: third-party fraud detectors ([Appendix: Fraud Detection](appendix-fraud-detection.md#appendix-permissionless-stale-close-detection)) must be able to submit higher-nonce vouchers on behalf of an offline party during the dispute window.
- `settleChannel` → requires status `Closing` and `block.timestamp >= disputeDeadline`. Callable by any address. Refunds `deposit - claimedAmount` to the client and, if `claimedAmount > 0`, transfers `claimedAmount` of the payment token to the configured `FeeRouter` and invokes `FeeRouter.routeSettlement(channel.provider, claimedBytes, claimedAmount)` in the same transaction. Sets status to `Closed`, emits `ChannelSettled`. **No fee is computed or skimmed inside this contract** — the router applies the four-bucket split, pays the operator's 60% base share same-tx (alongside the 25%/10%/5% legs), and increments `bytesPerEpoch[operator][epoch]` (epoch derived from `block.timestamp`) as an analytics counter for `OperatorEmissions`; see [FeeRouter Integration](#feerouter-integration). The wash-trading defense is capacity-shortfall slashing per [ADR 026 § Capacity-shortfall slashing](026-tokenomics.md#capacity-shortfall-slashing).
- `reclaimExpired` → requires status `Open` and `block.timestamp >= expiresAt`. Returns the full deposit to the client (no fee deducted — no voucher was submitted). Sets status to `Closed`, emits `ChannelExpiredReclaimed`. Callable by the client or the provider. Regardless of caller, the full deposit is returned to `channel.client` — the provider cannot claim funds via this path. This ensures abandoned channels where the client is absent can be cleaned up by the provider to free on-chain state.

**Safety bounds (hardcoded):**

| Parameter | Minimum | Maximum |
| --- | --- | --- |
| Dispute window | 43200 seconds (12 hours) | 259200 seconds (3 days) |
| Min deposit | 1 base unit | No max |
| Rate floor | 1 base unit | Must be < ceiling |
| Rate ceiling | Must be > floor | No max |
| Max voucher interval | 1 MB | 1024 MB (~1 GB) |
| Max channel duration | 604800 seconds (7 days) | 31536000 seconds (365 days) |

`PaymentChannel` does not hold a fee-percentage parameter. Bucket-share bounds (60/25/10/5 with per-share bounds 40–90 / 5–50 / 0–30 / 0–20) are owned by `FeeRouter` per [ADR 026 § Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds).

**Rate bounds are in USDC base units (6 decimals).** The contract stores a single `RateBounds` struct with `deliveryFloor` and `deliveryCeiling`.

**Initial rate bounds:**

| Parameter | Value (USD/MB) | USDC base units | Rationale |
| --- | --- | --- | --- |
| `deliveryFloor` | $0.000001/MB | 1 | Anti-abuse minimum; 10× below expected market rate. Prevents zero-rate free-riding while imposing no practical constraint on legitimate pricing. Nodes are expected to set rates well above this floor; the floor is purely an anti-zero safeguard, not a recommended price. |
| `deliveryCeiling` | $0.001/MB | 1,000 | 100× expected market rate. Accommodates origin-backed nodes with high-egress backends (e.g., S3 at $0.09/GB) while remaining well above any legitimate pricing scenario ($1.00/GB vs Akamai's ~$0.12–0.20/GB). |

The expected market rate is $0.00001/MB (10 USDC base units per MB, or $0.01/GB). This positions deCDN ~4–9× cheaper than major traditional CDNs (CloudFront at $0.085/GB, KeyCDN at $0.04/GB) and at parity with budget providers (Bunny.net at $0.01/GB). Both bounds are governance-tunable from day one within the hardcoded safety constraints above — admin-key-gated in the PoC, DecdnGovernor in production (see [ADR 009](009-governance.md#adr-009-governance-model)).

### Rate Bounds Refresh

Nodes must keep their local `RateBounds` copy current so advertised `rate_per_mb` stays within governance-set bounds. Because rate bounds are advisory coordination parameters — the contract does not verify rate compliance during settlement or slashing — the refresh strategy is lighter-touch than the content blacklist ([ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)), where serving blacklisted content is a slashable offense.

**Primary mechanism: event listening.** Nodes SHOULD subscribe to `RateBoundsUpdated` events on the `PaymentChannel` contract and update the local cache immediately. Governance actions are infrequent (days to weeks), so high-frequency polling would be wasteful.

**Fallback mechanism: periodic polling.** Nodes MUST poll `getRateBounds()` at a configurable interval (`rate_bounds_poll_interval`, default **1 hour**), guarding against missed events from RPC provider issues, WebSocket disconnections, or chain reorganizations. The 1-hour default is deliberately longer than the 10-minute registry ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)) / blacklist ([ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)) intervals: registry freshness is connectivity-critical and blacklist freshness slashing-critical, but rate-bounds staleness only risks counterparties rejecting the node's advertised rate.

#### Startup

Nodes MUST call `getRateBounds()` before accepting connections, never operating without rate bounds (same pattern as the content blacklist initial sync, [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)). Because `getRateBounds()` returns `uint256` but the wire protocol represents `rate_per_mb` as `u64` ([ADR 005](005-protocol.md#adr-005-wire-protocol)), nodes MUST verify both `deliveryFloor` and `deliveryCeiling` fit within `u64` on every refresh (startup and subsequent polls/events). If either bound exceeds `u64::MAX`, the node MUST refuse to start (or, on a mid-operation refresh, continue with its last valid bounds and log an error). Unreachable in practice — default ceiling is 1,000 base units — but the check guards against governance misconfiguration.

#### Stale bounds

If the event subscription is lost and RPC polling fails, the node SHOULD continue operating with its last-known bounds and log a warning. No service interruption is required. The worst-case consequence of stale bounds is that counterparties running compliant software reject the node's `rate_per_mb` as out-of-bounds — a revenue impact, not a safety violation.

#### No version-based delta pattern

Unlike the content blacklist (which uses `getBlacklistVersion()` for cheap change detection and incremental delta fetching), rate bounds are a single struct containing two `uint256` values. A version counter adds no value — the full state is readable in a single `eth_call` with negligible overhead. This is an intentional divergence from the [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting) pattern.

For how nodes validate `rate_per_mb` against cached bounds before signing protocol messages, see [ADR 005 — Rate Bounds Validation](005-protocol.md#rate-bounds-validation).

### BuybackBurner

| Function | Purpose |
| --- | --- |
| `executeBuyback(amount, minTokenOut)` | Governance multisig or `keeper`: swap `amount` of USDC for ≥ `minTokenOut` TOKEN and burn the proceeds. |
| `setKeeper(addr)` / `setSwapRouter(addr)` / `setPool(addr)` | Governance: rotate the authorized keeper, swap router, or pool. |
| `setSlippageTolerance(bps)` / `setMinBuybackAmount(n)` / `setMaxBuybackAmount(n)` | Governance: per-call execution guards. |
| `keeper() → address` / `getAccumulatedFees() → uint256` | Views: current keeper and accumulated buyback inflow (USDC). |

This is the canonical `BuybackBurner` interface. [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split) defines the economic parameters and the inflow source (25% router-fed under v2.1). [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol) specifies the venue (Balancer V3 Router + 80/20 weighted pool) and how `setSwapRouter` / `setPool` are configured at deployment. **V3 integration note:** `setSwapRouter` holds the Balancer V3 **Router** address, but `BuybackBurner` MUST self-approve the Balancer V3 **Vault** address (a separate contract) during initialization — the Vault pulls input tokens from the `msg.sender` of the Router call. See [ADR 018 — Buyback execution via Balancer V3](018-liquidity-strategy.md#buyback-execution-via-balancer-v3).

All `set*` functions are governance-only behind a timelock.

### FeeRouter Integration

Under [ADR 026](026-tokenomics.md#adr-026-tokenomics), `PaymentChannel.settleChannel` does not split fees inline. The full operator-bound payment-token balance is forwarded to a `FeeRouter` contract, which applies the canonical four-bucket split (60% operator base / 25% buyback-and-burn / 10% treasury / 5% safety reserve — full table and bounds in [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split) and [§ Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds)). All four legs transfer in the settlement transaction. This ADR specifies the `FeeRouter` interface only as it relates to the settlement path; the per-bucket details live in [ADR 026](026-tokenomics.md#adr-026-tokenomics).

#### Settlement-path interface

`PaymentChannel.settleChannel` MUST invoke `FeeRouter.routeSettlement(address operator, uint256 bytesDelivered, uint256 amount)` in the same transaction as the payment-token `safeTransferFrom` to the router. The router pays the operator's 60% base share in that transaction, dispatches the 25% / 10% / 5% same-tx legs, derives the current epoch as `uint64(block.timestamp / EPOCH_LENGTH)`, and increments `bytesPerEpoch[operator][epoch]` as an analytics counter read by `OperatorEmissions` per [ADR 026 § Operator Service Emissions](026-tokenomics.md#operator-service-emissions). The full `IFeeRouter` interface is canonical in [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model).

#### Settlement-path invariants

1. **Atomic base-share payout.** The 60% base share MUST land in the operator's wallet in the same transaction as `settleChannel` — no claim step, no keeper, no off-chain queue. This is the Case A cashflow guarantee from [ADR 026 § Operator economics](026-tokenomics.md#operator-economics).
2. **No reentry.** `settleChannel` holds a `nonReentrant` guard for the duration of the router call.
3. **One settlement per channel.** Enforced by the existing `Closed` status; the router need only tolerate duplicate calls (idempotency or revert — pinned in [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model)).

Conservation and same-tx four-bucket invariants (60/25/10/5) live with the router itself in [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split) / [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model). The `Settled` event (operator + epoch + per-bucket deltas) is emitted by the router; full event set is in [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model).

#### Cache-miss bypass (node-to-node paid pulls)

**Node-to-node cache-miss paid pulls bypass the router entirely.** When node B pulls a blob from origin-backed node A and pays via a payment channel, that settlement is internal cost-recovery between two operators — not net protocol revenue. Routing it would double-charge the same revenue (once when B pays A, again when B's clients pay B for the same bytes). Per [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split):

- Node-to-node settlements use direct peer payment-token transfer with no router invocation.
- Implementations distinguish node-to-node from client-to-node settlements via the channel's `client` and `provider` fields cross-referenced against the on-chain registry: if both addresses have a registered NodeId binding (see [NodeId-to-Ethereum Binding](#nodeid-to-ethereum-binding)), the channel is node-to-node; otherwise it is client-to-node.
- The `PaymentChannel` may implement the bypass either by exposing a separate `settleChannelNoRoute(channelId)` entry point usable only when both parties are registered nodes, or by having `settleChannel` detect the case and skip the `FeeRouter` call. Either way the operator-to-operator USDC transfer is direct and bypasses the router's per-epoch byte accumulators (those bytes were already counted at the client-to-node settlement that paid for them downstream). The wash-trading defense is capacity-shortfall slashing per [ADR 026 § Capacity-shortfall slashing](026-tokenomics.md#capacity-shortfall-slashing) — auto-downgrade on sustained delivery below `min_delivery_ratio × declared_capacity` measured against probe data; routing bytes through node-to-node bypass settlements does not raise revenue and cannot extract from the four-bucket FeeRouter, since the bypass path does not invoke the router.
- Permissionless fraud detectors ([Appendix: Fraud Detection](appendix-fraud-detection.md#appendix-permissionless-stale-close-detection)) can observe node-to-node settlements for self-routed-traffic / wash-trading patterns despite the bypass. The on-chain remedy is capacity-shortfall slashing; off-chain reputation signals ([ADR 008](008-reputation.md#adr-008-reputation-system)) consume the observation as a soft signal feeding peer selection.

#### Settlement sequence

End-to-end payment-token flow (client→node settlement, then the parallel cache-miss bypass) is diagrammed in [ADR 016 §"FeeRouter integration"](016-contract-interactions.md#adr-016-smart-contract-interaction-model). This ADR documents only the `PaymentChannel ↔ FeeRouter` interface contract.

The full four-bucket split applies to every network deployment from launch. Simplified launch configurations are expressed by setting non-active bucket shares to zero via `FeeRouter.setShares(...)` per [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics), not by deploying a reduced-surface stub. The cross-validation invariant in that section ensures any non-zero share has a wired non-zero destination, so the launch share configuration alone determines which downstream contracts must be ready at deploy time.

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
        keccak256(bytes("PaymentChannel")),   // name
        keccak256(bytes("1")),                      // version
        block.chainid,                              // chainId (L2)
        address(this)                               // verifyingContract
    ));
}
```

The domain separator binds every voucher to a specific contract deployment on a specific chain. A voucher signed for one chain cannot be replayed on another, and a voucher signed for one `PaymentChannel` deployment cannot be replayed against an upgraded or redeployed contract at a different address.

**Voucher type:**

```solidity
bytes32 constant VOUCHER_TYPEHASH = keccak256(
    "Voucher(bytes32 channelId,uint256 amount,uint256 nonce,uint256 bytesDelivered,address token)"
);
```

Per-operator-epoch attribution is derived by `FeeRouter.routeSettlement` at settlement time as `epoch = uint64(block.timestamp / EPOCH_LENGTH)`; the voucher itself does not carry an epoch. All cumulative bytes from the final voucher are credited to the epoch the settlement transaction lands in. The operator chooses when to invoke `settleChannel` within `[disputeDeadline, expiresAt]` — `settleChannel` is permissionlessly callable but only the operator has an incentive to spend gas on it, since they receive the 60% base share. Practical bound on epoch-shifting is `maxChannelDuration` (default 90 days, governance-tuned). The effective gaming surface — shifting attribution across roughly 4–12 weekly epochs within a monthly emission distribution — is a second-order effect on emission share and shrinks further as the active-operator set grows.

**Signature digest:**

```solidity
bytes32 digest = keccak256(abi.encodePacked(
    "\x19\x01",
    DOMAIN_SEPARATOR,
    keccak256(abi.encode(VOUCHER_TYPEHASH, channelId, amount, nonce, bytesDelivered, token))
));
```

**Verification:** Implementations must use OpenZeppelin's `SignatureChecker.isValidSignatureNow(channel.client, digest, signature)`, which transparently supports both EOA signers (via hardened `ECDSA.recover` that rejects non-canonical `s` values and restricts `v` to `27`/`28`) and smart account signers (via ERC-1271 `isValidSignature`). The signature is encoded as 65 bytes (`r || s || v`) for EOA signers; smart account signers may use longer signatures per their wallet implementation. See [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support) for the full account abstraction design.

The `DOMAIN_SEPARATOR` is computed once in the constructor and stored as an immutable. If the contract is deployed behind a proxy and may be migrated to a different chain, it should be cached in a state variable and recomputed only when `block.chainid` changes (the pattern used by OpenZeppelin's `EIP712` base contract), rather than on every call.

### Voucher Nonce Convention

Voucher nonces within a channel start at **1**. Nonce 0 is reserved as the sentinel value meaning "no voucher has been submitted" — it is the Solidity default for `claimedNonce` in a newly opened `Channel` struct. The first client-signed voucher in a channel uses `nonce=1`, the second uses `nonce=2`, and so on. This convention ensures:

- `claimedNonce == 0` reliably identifies channels where no voucher has ever been submitted, which is the guard condition for the zero-voucher close path callable by either party (see [Channel griefing](#channel-griefing) and the channel close lifecycle in [PaymentChannel](#paymentchannel)).
- Any real voucher (nonce ≥ 1) can always be used to dispute a zero-voucher close (which records `claimedNonce=0`), since `disputeChannel` requires strictly higher nonce.

### Node Registry

The on-chain registry of staked nodes is part of the `CapacityBond` contract, not a separate contract. Staking is a prerequisite for registration ([ADR 026 § Operator economics](026-tokenomics.md#operator-economics)), so co-locating them avoids cross-contract calls and simplifies the atomic stake-then-register flow.

> **No on-channel fee-discount path.** Operator return is differentiated through the `CapacityBond` lock-to-capacity curve ([ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)), not via a stake-multiple fee toggle on the channel contract. `getEffectiveFee`, `getStakeMultiple`, `DISCOUNT_MULTIPLE`, `feePercentage`, and `discountedFeePercentage` are not part of the interface. `CapacityBond` carries the registration, bond-bookkeeping, capacity-shortfall, and slashing responsibilities; the operator bond is `bond = k × Mbps^α` with defaults `k=12.6`, `α=1.2` (≈50K TOKEN at 1 Gbps) per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve).

#### Data Structure

```solidity
struct NodeInfo {
    bytes32 nodeId;              // iroh NodeId (ed25519 public key, 32 bytes)
    address ethAddress;          // Ethereum address for payment channels (20 bytes)
    bool    active;              // false after deregistration or auto-ejection (1 byte)
                                 // ↑ ethAddress + active pack into one slot
    uint256 registeredAt;        // block.timestamp of current registration
    uint256 firstBondedAt;   // block.timestamp of first-ever registration (immutable once set)
    uint256 lastMultiaddrUpdate; // block.timestamp of last multiaddr change
    bytes   multiaddrs;          // packed QUIC multiaddrs (length-prefixed entries)
    string  regionHint;          // ISO 3166-1 alpha-2 code (self-reported, unverified)
}
```

Field order is chosen for storage packing: `ethAddress` (20 B) and `active` (1 B) share one 32-byte slot, dropping `NodeInfo` from 8 slots to 7. `multiaddrs` uses `bytes` rather than `string[]` for gas efficiency: a packed array of `(uint16 length, bytes data)` entries, parsed off-chain by clients. Maximum encoded size is bounded by the governable `maxMultiaddrSize` parameter (initial value 1024 bytes; safety bounds 64–1024 bytes per [ADR 009](009-governance.md#adr-009-governance-model)).

#### Interface (additions to CapacityBond)

```solidity
// --- Node Registry ---

// Registration (requires active bond >= bond_required(declared_capacity))
function registerNode(
    bytes32 nodeId,
    bytes calldata multiaddrs,
    string calldata regionHint,
    bytes calldata bindingSignature,
    bytes calldata ed25519Signature   // proves caller controls nodeId's ed25519 private key
) external;

function updateMultiaddrs(bytes calldata multiaddrs) external;

function deregisterNode() external;

// NodeId reclaim (production — legitimate owner reclaims a squatted NodeId)
function reclaimNodeId(
    bytes32 nodeId,
    bytes calldata ed25519Signature
) external;

// Views
function getNode(bytes32 nodeId) external view returns (NodeInfo memory);
function getNodeByAddress(address ethAddress) external view returns (NodeInfo memory);
function isActiveNode(bytes32 nodeId) external view returns (bool);
function getActiveNodeCount() external view returns (uint256);
function getActiveNodes(uint256 offset, uint256 limit)
    external view returns (NodeInfo[] memory);
function getFirstBondedAt(address ethAddress) external view returns (uint256);

// State — per-nodeId nonce for ed25519 registration replay protection
mapping(bytes32 => uint64) public registrationNonce;

// Events
event NodeRegistered(
    bytes32 indexed nodeId,
    address indexed ethAddress,
    bytes multiaddrs,
    string regionHint,
    uint64 bindingNonce,
    uint64 registrationNonce
);
event NodeMultiaddrUpdated(bytes32 indexed nodeId, bytes multiaddrs);
event NodeDeregistered(bytes32 indexed nodeId);
event NodeAutoEjected(bytes32 indexed nodeId, uint256 remainingStake);
event NodeIdReclaimed(bytes32 indexed nodeId, address indexed previousOwner);
```

`registerNode` emits both `NodeRegistered` and `NodeIdBound` ([§ On-Chain Registration](#on-chain-registration)) — the latter ensures off-chain indexers tracking the authoritative `nodeIdToAddress` mapping see initial registrations alongside rebindings.

#### Constraints

- **One-to-one mapping.** Each `nodeId` maps to exactly one `ethAddress` and vice versa. Enforced with `require(nodeByAddress[msg.sender].nodeId == bytes32(0))` and `require(nodes[nodeId].ethAddress == address(0))`, where `bytes32(0)` is the sentinel for "unregistered". This enforces a one-stake-position-per-node invariant.
- **`registerNode` rejects `nodeId == bytes32(0)`** (reserved as the unregistered sentinel). It binds `msg.sender` to `nodeId` — the caller's Ethereum address becomes `ethAddress`. This binding is on-chain and permanent until deregistration, distinct from the ephemeral per-session `NodeId`-to-address binding in [§ Off-Chain (Ephemeral) Binding for Clients](#off-chain-ephemeral-binding-for-clients). The function performs two signature verifications: (1) the `bindingSignature` parameter is an EIP-712 signature over `BindNodeId(nodeId, bindingNonce[msg.sender])` (see [§ Binding Message Format](#binding-message-format)); `registerNode` verifies this against the caller's current `bindingNonce`, then atomically writes the `nodeIdToAddress`/`addressToNodeId` mappings and increments `bindingNonce[msg.sender]`. (2) The `ed25519Signature` parameter proves ownership of the NodeId's ed25519 private key — see [§ NodeId Ownership Verification](#nodeid-ownership-verification) below. The shared per-address `bindingNonce` counter with `bindNodeId` ensures replay protection across both registration and rebinding. Every registered node is immediately slashable — there is no window in which a node is active in the mesh without a verifiable binding. The separate `CapacityBond.bindNodeId()` function in [§ On-Chain Registration](#on-chain-registration) remains available for rebinding (key rotation) after initial registration.
- **`deregisterNode` deactivates without touching the bond.** Sets `active = false`, removes the operator from the active set, and increments `registrationNonce[nodeId]` to invalidate any previously issued ed25519 registration signatures for this NodeId. It does **not** move the bond into unbonding — deactivation and bond exit are separate operations. The bond stays locked and fully slashable after deregistration (accountability is preserved), and an operator who changes their mind can re-register without re-funding. To withdraw, the operator calls `unbond()` (which starts the 14-day unbonding window per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)); the slash-then-run protection is the `MAX_EVIDENCE_AGE_US < unbondingPeriod` invariant ([ADR 014 § Interaction with unbonding period](014-on-chain-verification.md#interaction-with-unbonding-period)), which keys off the `unbond()` call rather than off deregistration, so it holds regardless. Separating the two lets an operator pause node duties (stop serving, leave the active set) without forcing a bond-return clock, while a full exit is just `deregisterNode` followed by `unbond()`.
- **Auto-ejection.** When slashing drops a node's bond below 50% of the minimum bond for its declared tier ([ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn)), the contract sets `active = false` and emits `NodeAutoEjected`. The node must re-bond at the full tier minimum to rejoin.
- **`firstBondedAt` is write-once.** `registerNode` sets `firstBondedAt = block.timestamp` only if the stored value is 0 (first-ever registration for this address). On re-registration after deregistration or auto-ejection it retains its original value; it is never cleared by `deregisterNode` or auto-ejection. Used by clients to determine cold-start bootstrap eligibility ([ADR 008](008-reputation.md#cold-start-bootstrap)).

#### Multiaddr Update Policy

A governable cooldown (0–86400 seconds, see [ADR 009](009-governance.md#adr-009-governance-model)) prevents a compromised node key from rapidly flipping multiaddrs to redirect traffic. The default is 0 (disabled) — `updateMultiaddrs` costs ~$0.03 per call at typical L2 gas prices, so a small mesh updating occasionally (IP change, port rotation) needs no rate limiting. Governance tightens the cooldown if abuse is seen.

#### Gas Costs

| Operation | Estimated Gas | Estimated Cost |
| --- | --- | --- |
| `registerNode()` | ~650k–1.15M gas | ~$0.26–$0.46 |
| `updateMultiaddrs()` | ~60k gas | ~$0.03 |
| `deregisterNode()` | ~80k gas | ~$0.05 |
| `reclaimNodeId()` | ~600k–1.1M gas | ~$0.24–$0.44 |

`registerNode` and `reclaimNodeId` include ~500k–1M gas for on-chain ed25519 signature verification (Solidity library) — a one-time cost per node lifetime, negligible vs. the entry-tier capacity bond. Estimates assume typical multiaddr sizes (2–4 addresses, ~200 bytes total); larger payloads increase storage gas proportionally.

#### Client Query Patterns

Three tiers, from simplest to most scalable:

1. **View functions.** `getActiveNodes(offset, limit)` with pagination. For tens of nodes, a single call with `limit = 100` returns the full node set. Clients call this on first startup to bootstrap their peer list, then rely on gossip for ongoing discovery (see [ADR 001 § Node Discovery (Gossip)](001-network.md#node-discovery-gossip)).

2. **Event logs.** Clients index `NodeRegistered`, `NodeMultiaddrUpdated`, `NodeDeregistered`, and `NodeAutoEjected` events (indexed by `nodeId`) to maintain a local cache. More efficient than repeated view calls for larger node sets.

3. **Subgraph (future production).** A Graph Protocol subgraph indexing registry events for complex queries (nodes by region, active node count over time, churn analysis).

#### NodeId Ownership Verification

`registerNode` requires an ed25519 signature proving the caller controls the private key corresponding to `nodeId`. Without this proof, an attacker could front-run legitimate registrations by calling `registerNode` with someone else's NodeId — the attacker gains no traffic (cannot complete iroh QUIC handshakes with that identity), but under the one-to-one uniqueness constraint the legitimate owner is permanently blocked from registering. Even with the ed25519 verification overhead, the total `registerNode` cost (~$0.26–$0.46 gas + recoverable entry-tier capacity bond) is low enough that squatting remains a cheap griefing/DoS vector without the ownership proof.

**Note:** The `bindingSignature` parameter proves the caller's Ethereum key signed the NodeId binding — it does not prove ownership of the ed25519 NodeId itself. These are orthogonal concerns: `bindingSignature` prevents un-slashable registration, while ed25519 ownership verification prevents NodeId squatting.

##### Signed message

The `ed25519Signature` parameter is an ed25519 signature over:

```
ed25519_sign(private_key, keccak256(abi.encodePacked(nodeId, msg.sender, block.chainid, registrationNonce[nodeId])))
```

Where `registrationNonce` is a per-`nodeId` counter (distinct from the per-address `bindingNonce` used for EIP-712 binding), incremented by `deregisterNode` on each deregistration. The nonce prevents replay of old signatures after a node deregisters and a different address attempts to re-register the same `nodeId`. The `block.chainid` binding prevents cross-chain signature replay.

##### On-chain verification

EVM has no native ed25519 precompile, and the RIP-7212 proposal is not yet deployed on the production L2 (see [Appendix: L2 Deployment](appendix-l2-deployment.md#appendix-production-l2-deployment-target) for chain and rollout status). The implementation uses a well-audited Solidity ed25519 verification library (e.g., `ed25519-sol`). This adds ~500k–1M gas to `registerNode`, a one-time cost per node lifetime — see [§ Gas Costs](#gas-costs) and the rationale in [ADR 014 § Slash Signatures — secp256k1 EIP-712](014-on-chain-verification.md#slash-signatures--secp256k1-eip-712) for why the secp256k1 `slash_sig` scheme used for routine slash evidence is not needed here.

##### Reclaim flow

If a NodeId was squatted (e.g., during a transition period or via a contract bug), the legitimate ed25519 key holder calls `reclaimNodeId(nodeId, ed25519Signature)`. This verifies the ed25519 signature over `keccak256(abi.encodePacked(nodeId, msg.sender, block.chainid, registrationNonce[nodeId]))`, deactivates the current holder's node if the reclaimed NodeId was its bound id (clearing the active flag and removing it from the active set — the bond is left locked and slashable, exactly as `deregisterNode`; the holder exits the bond separately via `unbond()`), clears the NodeId↔address mappings (and zeroes the holder's now-stale `NodeInfo.nodeId` so the read views stay consistent with the cleared binding), increments `registrationNonce[nodeId]`, and emits `NodeIdReclaimed`. The caller can then call `registerNode` under their own address. Reclaim does not require the caller to have stake — it only proves ed25519 key ownership and clears the squatter's binding. `reclaimNodeId` is the sole reclaim mechanism: there is no admin override. Reclaim authority is gated entirely by ed25519 wire-key ownership (the iroh NodeId private key), distinct from the secp256k1 on-chain signatures used for slash evidence ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)) and EIP-712 NodeId↔Ethereum binding ([§ NodeId-to-Ethereum Binding](#nodeid-to-ethereum-binding)).

## Admission and Priority

Node admission and queueing policy — how a node orders incoming `StreamRequest`s under congestion — is implementation-defined and lives outside the protocol. The wire format carries no priority bits, the channel and voucher mechanisms encode no per-stream priority state, and different operators are expected to tune their policy differently. Two signals are available to any admission policy:

- **Committed voucher rate.** The advertised `rate_per_mb` in `ProbeResponse` / `StreamResponse` is a **floor**, not equality — nodes verify `amount_delta / bytes_delta >= rate_per_mb`. Clients MAY commit at higher rates; nodes MAY use the committed rate as a per-stream priority key, with the premium paid directly via [`FeeRouter.routeSettlement`](#feerouter-integration).
- **Registered node-stake.** `CapacityBond.bondOf(address)` is readable on-chain for any registered operator. Nodes MAY treat addresses with `bondOf >= bond_required(declared_capacity)` ([ADR 026 § Operator economics](026-tokenomics.md#operator-economics)) as eligible for a higher-priority admission lane.

## NodeId-to-Ethereum Binding

The protocol requires a verifiable mapping between iroh NodeIds (ed25519 public keys) and Ethereum addresses (secp256k1-derived). This binding is used for payment channel association and slash evidence attribution. Two orthogonal signature mechanisms protect this mapping: the EIP-712 `bindingSignature` (secp256k1) proves the Ethereum key holder consents to the association — preventing un-slashable registration; the `ed25519Signature` ([§ NodeId Ownership Verification](#nodeid-ownership-verification)) proves the NodeId's private key holder authorized the registration — preventing NodeId squatting.

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

The EIP-712 domain separator is the same as the `CapacityBond` contract deployment (chain ID + contract address), preventing cross-chain and cross-contract replay.

### On-Chain Registration

Node registration and NodeId binding are atomic. `CapacityBond.registerNode()` ([§ Node Registry](#node-registry)) accepts a `bindingSignature` parameter — an EIP-712 signature over `BindNodeId(nodeId, bindingNonce[msg.sender])` — and an `ed25519Signature` parameter proving ownership of the NodeId's ed25519 private key (see [§ NodeId Ownership Verification](#nodeid-ownership-verification)). It verifies both signatures, writes the `nodeIdToAddress`/`addressToNodeId` mappings, and increments `bindingNonce[msg.sender]` in the same transaction that adds the node to the mesh. The per-address nonce counter is shared with `bindNodeId`, giving replay protection across both paths. This eliminates the window in which a node could be active but not slashable.

The standalone `CapacityBond.bindNodeId()` function below remains available for **rebinding only** (key rotation after initial registration). It is no longer needed at initial registration time.

**Canonical source of truth:** The `nodeIdToAddress` / `addressToNodeId` mappings — written atomically by `registerNode` at initial registration and by `bindNodeId` on rebinding — are the authoritative source for payment attribution and slashing. `NodeInfo.ethAddress` in [§ Data Structure](#data-structure) is always `msg.sender`, so the two are consistent by construction under the one-to-one constraint. If the implementation stores both, `NodeInfo.ethAddress` MUST equal `nodeIdToAddress[nodeId]` at all times.

This creates an authoritative, publicly queryable mapping:

```solidity
// CapacityBond additions
mapping(bytes32 => address) public nodeIdToAddress;
mapping(address => bytes32) public addressToNodeId;
mapping(address => uint64) public bindingNonce;

// Bundled per-operator binding + activity view (off-chain origin discovery;
// ADR 016 § Off-Chain Read API, ADR 022 § Origin discovery). `nodeId` is
// `bytes32(0)` if the operator never registered or cleared their binding via
// rebinding; `active` is `false` if the operator is unbound, in unbonding,
// auto-ejected, or below the tier minimum bond.
function nodeIdOf(address operator) external view returns (bytes32 nodeId, bool active);

// Single-purpose per-operator activity check. Returns `true` iff `operator` is
// currently registered with an active (non-unbonding) bond at or above
// `bond_required(declared_capacity)`; `false` for unregistered addresses,
// bond below the tier minimum, bond fully or partially in unbonding,
// and auto-ejected operators.
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
// atomically inside registerNode(). Like registerNode, bindNodeId requires
// BOTH the EIP-712 binding signature (Ethereum-key consent) AND an ed25519
// proof of the new NodeId's ownership. The ed25519 requirement is deliberate:
// without it, an EIP-712-only rebinding would re-open the squatting vector
// that registerNode's ed25519 check closes (an attacker could bind an unbound
// NodeId they don't own, blocking the legitimate owner's registerNode). With
// the proof required on every binding path, no NodeId can be bound without
// proving ownership, so squatting is impossible and reclaimNodeId is a
// defense-in-depth backstop (for legacy / buggy bindings) rather than a
// routine remedy. Calling this before registerNode is permitted but only
// records a binding without mesh membership or stake.
function bindNodeId(bytes32 nodeId, bytes calldata bindingSignature, bytes calldata ed25519Signature) external {
    uint64 nonce = bindingNonce[msg.sender];
    bytes32 digest = keccak256(abi.encodePacked(
        "\x19\x01",
        DOMAIN_SEPARATOR,
        keccak256(abi.encode(BIND_NODE_TYPEHASH, nodeId, nonce))
    ));
    require(SignatureChecker.isValidSignatureNow(msg.sender, digest, bindingSignature), "invalid binding signature");

    // Prove ownership of the new NodeId's ed25519 key (same message preimage
    // as registerNode; see § NodeId Ownership Verification).
    bytes32 ed25519Msg = keccak256(abi.encodePacked(nodeId, msg.sender, block.chainid, registrationNonce[nodeId]));
    require(ed25519Verify(nodeId, ed25519Msg, ed25519Signature), "invalid ed25519 signature");

    // Reject if nodeId is already bound to a different address
    address existingOwner = nodeIdToAddress[nodeId];
    require(existingOwner == address(0) || existingOwner == msg.sender, "NodeId bound to another address");

    // Clear caller's previous binding if rotating to a different NodeId, and
    // bump its registrationNonce so any pre-rotation ed25519 signature for the
    // released NodeId is invalidated (mirrors deregisterNode: every binding
    // exit requires a fresh ownership proof to re-bind).
    bytes32 oldNodeId = addressToNodeId[msg.sender];
    if (oldNodeId != bytes32(0) && oldNodeId != nodeId) {
        delete nodeIdToAddress[oldNodeId];
        registrationNonce[oldNodeId] += 1;
    }

    nodeIdToAddress[nodeId] = msg.sender;
    addressToNodeId[msg.sender] = nodeId;
    bindingNonce[msg.sender] = nonce + 1;

    // Keep the registration record consistent for an already-active node.
    if (nodes[msg.sender].active) {
        nodes[msg.sender].nodeId = nodeId;
    }

    emit NodeIdBound(msg.sender, nodeId, nonce);
}

function resolveNodeId(bytes32 nodeId) external view returns (address) {
    return nodeIdToAddress[nodeId];
}
```

> **Note on EIP-712 signature:** Redundant for direct on-chain calls (`msg.sender` already authenticates the caller) but retained for: (1) future meta-transaction/relayer patterns where a third party submits the binding on the operator's behalf; (2) atomic binding inside `registerNode`, where the signature supplies the explicit cryptographic consent to associate a specific NodeId with the calling address (`registerNode` writes the mapping on behalf of `msg.sender`).

### Off-Chain (Ephemeral) Binding for Clients

Clients without on-chain registration MAY include a signed binding in their `StreamRequest` to attest a NodeId↔Ethereum-address mapping for the connection's lifetime. The node verifies the EIP-712 signature over `BindNodeId(nodeId, nonce=0)` using `SignatureChecker` semantics: `ecrecover` for EOA clients, or an RPC call to `isValidSignature` for smart account clients ([ADR 024](024-account-abstraction.md#off-chain-erc-1271-verification)). The verified address is cached for the connection's lifetime and used for voucher attribution. This ephemeral binding is not stored on-chain and is valid only for the session. Wire-format details are in [ADR 005](005-protocol.md#client-identity-binding).

### Binding Requirements by Role

| Role | On-chain binding required? | Rationale |
| --- | --- | --- |
| Node (staked) | **Yes** — `registerNode` performs binding atomically via `bindingSignature` (EIP-712, proves Ethereum key consent) and `ed25519Signature` (proves NodeId ownership) | Slash evidence references on-chain NodeId→address mapping; atomic binding eliminates gap; ed25519 proof prevents NodeId squatting |
| Client (opening channels) | No — channel `client` field is the Ethereum address directly | Channel operations use Ethereum addresses, not NodeIds |

### Rebinding

A node or client can rebind their Ethereum address to a new NodeId by calling `bindNodeId` (the nonce increments, invalidating the old binding). The old NodeId→address mapping is deleted. This supports key rotation scenarios (e.g., compromised iroh key). Initial binding is handled atomically by `registerNode` and does not require a separate `bindNodeId` call.

## Decimal Handling

USDC uses 6 decimals; TOKEN uses 18 decimals. All payment amounts in the `incentive` crate use USDC base units (µUSDC). The voucher signing code uses raw base units — no decimal conversion in the signature path to avoid precision bugs.

**Voucher format:**

```
{channelId, amount, nonce, bytesDelivered, token, signature}
```

During delivery over `cdn/client/v1`, `{signature, amount, nonce, bytesDelivered}` are transmitted on the wire; the remaining fields (`channelId`, `token`) are derived from stream context. The `nonce` is explicit to prevent desynchronization if a `VoucherAck` is dropped (it starts at 1 for the first voucher in a channel; 0 is reserved as a sentinel). See [ADR 005](005-protocol.md#adr-005-wire-protocol) for wire protocol details.

The `token` field (ERC-20 address) is in the signed EIP-712 typed data to prevent cross-token replay; it is the USDC contract address, fixed at deployment. Full EIP-712 type definition and domain separator: [EIP-712 Voucher Signature](#eip-712-voucher-signature).

### Voucher Bytes-Delivered Field

`bytesDelivered` is a cumulative byte count signed alongside `amount` and `nonce`. It is the canonical settlement-record byte count carried in the `Voucher`, forwarded to `FeeRouter.routeSettlement`, and aggregated into `bytesPerEpoch[operator][epoch]` (where `epoch` is derived from `block.timestamp` at settlement time) — an analytics counter consumed by `OperatorEmissions.distribute(epoch)` per [ADR 026 § Operator Service Emissions](026-tokenomics.md#operator-service-emissions). Properties:

- **Cumulative, monotonic.** Like `amount` and `nonce`, `bytesDelivered` is strictly non-decreasing across vouchers within a channel. `disputeChannel` MUST revert if the new voucher's `bytesDelivered < claimedBytes`.
- **Derivable from MB-denominated voucher cadence.** Voucher cadence is MB-denominated (default 1 MB; see [Voucher Interval Negotiation](#voucher-interval-negotiation)) and `rate_per_mb` is MB-denominated. Clients computing `amount` from `bytesDelivered` use `amount = ⌈bytesDelivered / 1_048_576⌉ × rate_per_mb` (1 MB = 1,048,576 bytes per [ADR 005](005-protocol.md#adr-005-wire-protocol)); equivalently, `bytesDelivered = mb_delivered × 1_048_576` when delivery boundaries align with MB intervals. The unit conversion is purely an off-chain arithmetic concern; the voucher carries the byte count directly so the contract does not need to re-derive it.
- **Carried through `closeChannel` / `disputeChannel` to `settleChannel`.** Recorded in `channel.claimedBytes` and forwarded as the `bytesDelivered` argument to `FeeRouter.routeSettlement` at settlement.
- **Cross-channel consistency.** A voucher signed for one channel is bound by its EIP-712 typed data; `bytesDelivered` is part of that signed payload and cannot be replayed against a different channel.

The router does not validate `bytesDelivered` against any oracle of physical delivery — the value is whatever the client signed. The wash-trading defense is capacity-shortfall slashing per [ADR 026 § Capacity-shortfall slashing](026-tokenomics.md#capacity-shortfall-slashing) — operators with sustained delivery below `min_delivery_ratio × declared_capacity` are auto-downgraded, and per-byte settlement revenue requires real client USDC inflow rather than self-attested byte counts.

## Slashing and Channel Interactions

Slashing and payment channels are independent by design.

**Slashing does not affect channel funds.** Slashing operates exclusively on TOKEN stake in the `CapacityBond` (schedule per [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn) — 5%/15%/50% escalation tiers, 50% challenger / 30% safety / 20% burn). Channel funds are client deposits held in escrow — not stake, never touched by slashing. This follows from the functional separation in [Consequences](#consequences): payment channel contracts never hold or move TOKEN stake, cannot be called by `CapacityBond` to slash or reassign stake, and any `CapacityBond` interaction is read-only (e.g., resolving NodeId↔address bindings).

**Slashing can drop a node below its tier minimum bond while channels are open.** Channel deposits being independent of the bond, a node can be slashed below the tier minimum (or to zero) with open channels. The channels continue their normal lifecycle — close, dispute window, settle — regardless of bonding status; settlement is purely a function of voucher state, not registry status.

**Auto-ejection does not interrupt open channels.** When a node's stake drops below 50% of the minimum and auto-ejection triggers (see [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn)):

- Open channels settle normally. Client funds are never trapped.
- The ejected node cannot participate in new channels (clients verify node registration before opening channels, and nodes verify counterparty status before accepting a `StreamRequest`).
- The ejected node is removed from gossip routing, so it receives no new client connections.
- `closeChannel` (client/provider only), `disputeChannel` (any address), and `settleChannel` (any address) remain callable on existing channels — these functions check channel state, not registry status.
- The node must re-bond at the full tier minimum (`bond_required(declared_capacity)`) and re-register to resume operations.
