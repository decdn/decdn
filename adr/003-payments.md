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

A channel is opened by depositing the payment token into the `PaymentChannel` contract, naming both the provider and the address authorized to sign vouchers on the channel — the funder itself unless it delegates (see [Channel roles](#paymentchannel)). As content is delivered, that signer issues cumulative vouchers off-chain — one voucher per MB received (default cadence; negotiable for large transfers). The delivering node holds the latest voucher and submits it on-chain to initiate channel close. A delivering node may also `withdraw` its accrued earnings against the latest signed voucher **while the channel stays open** — redeeming a signed, monotonic claim needs no dispute window (see [`withdraw` behavior](#paymentchannel) and [Operator early withdrawal](#operator-early-withdrawal-no-dispute-window)). A dispute window (default 48 hours, governable within 48h–72h — see [ADR 009](009-governance.md#adr-009-governance-model)) allows either party to counter a stale or fraudulent close attempt. After the dispute window expires, the channel is settled and funds are distributed.

Key parameters:

- Voucher cadence: 1 MB delivered per voucher (default; negotiable up to the 1024 MB wire ceiling for large transfers — see [Voucher Interval Negotiation](#voucher-interval-negotiation))
- Minimum deposit: none on-chain beyond non-zero; recommended practical minimum: 10 USDC (see [Deposit Economics](#deposit-economics))
- Fee routing: the operator payment-token balance is forwarded to `FeeRouter.routeSettlement(operator, bytesDelivered, amount)` — at final settlement, and incrementally on each `withdraw` — and the three-bucket split (60% operator base, 30% buyback, 10% treasury) is dispatched same-tx per [ADR 026](026-tokenomics.md#adr-026-tokenomics). The per-call deltas partition the channel's lifetime claim, so each byte and USDC unit is split exactly once. See [FeeRouter Integration](#feerouter-integration).
- Operator return is differentiated through the `CapacityBond` lock-to-capacity curve per [ADR 026](026-tokenomics.md#adr-026-tokenomics), not via a fee-discount mechanic on the channel contract.

### Deposit Economics

Opening, closing, and settling a channel requires three on-chain transactions totalling ~$0.23 at the production L2's typical gas prices (`openChannel` ~$0.05, `closeChannel` ~$0.10, `settleChannel` ~$0.08). This estimate assumes an existing ERC-20 approval; first-time users incur an additional one-time `approve` transaction (~$0.03), bringing the true first-channel cost to ~$0.26. `openChannel` writes the pinned `voucherSigner` into a storage slot of its own (the packed header has no room left), and `closeChannelWithoutVoucher` substitutes for `closeChannel` at marginally lower cost since it verifies no signature; on an L2 whose per-transaction cost is dominated by data posting, neither shifts the figures above at this rounding. The table below uses the $0.23 lifecycle cost (excluding the one-time approval) as a percentage of various deposit sizes:

| Deposit | Lifecycle gas ($0.23) | Gas % of deposit |
|---------|----------------------|------------------|
| 1 USDC  | $0.23                | 23%              |
| 5 USDC  | $0.23                | 4.6%             |
| 10 USDC | $0.23                | 2.3%             |
| 25 USDC | $0.23                | 0.92%            |
| 100 USDC| $0.23                | 0.23%            |

**Recommended practical minimum: 10 USDC.** Client software should default to a 10 USDC minimum deposit (user-overridable). At 10 USDC, gas overhead is 2.3% — acceptable for a channel covering ~10,000,000 MB at the floor rate or ~1,000,000 MB (~1,000 GB) at the expected market rate ($0.01/GB), sufficient for weeks to months of casual use without top-up. This is a client-side recommendation, not a network floor: `openChannel` accepts any non-zero deposit. A network minimum would bound neither of the things it appears to — service is bounded by what the deposit funds (the seller refuses a request whose channel cannot cover the first credit window, see [Voucher withholding](#voucher-withholding)) and channel spam is bounded by gas (each `openChannel` costs gas and locks real funds, refundable only to the client) — while creating a hard barrier for development and testing, where small deposits are useful.

#### Amortization

The overhead percentages above represent worst-case single-session economics. Long-lived channels amortize open/settle costs across many sessions: a channel used for 30 sessions costs ~$0.008/session in gas. Channels extended via `topUp` amortize further since only the initial open and final settle incur gas.

#### Smart Account Support and Gasless Channel Opens

All deCDN contracts use OpenZeppelin `SignatureChecker` for signature verification, supporting both EOA (via `ecrecover`) and smart account wallets (via ERC-1271 `isValidSignature`). Safe and other ERC-1271 smart accounts are supported wallet types for node operators and clients; the encrypted EOA keystore is the documented default — see [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support).

Two standards can further eliminate the requirement for clients to hold the L2's native gas currency:

- **ERC-2771 meta-transactions.** A relayer submits the `openChannel` transaction on behalf of the client, paying gas. The client signs an ERC-2771 forwarding request; the relayer recoups gas from the deposit or a separate sponsorship fund. Requires adding a trusted-forwarder check to the contract.
- **ERC-4337 account abstraction.** Smart contract wallets batch payment-token approval + channel open into a single user operation. A paymaster can sponsor gas in the payment token rather than ETH. Works with unmodified contracts — no changes to `PaymentChannel` needed.

Gas abstraction via ERC-2771 or ERC-4337 paymasters is targeted at production.

### Voucher Interval Negotiation

At the default 1 MB cadence, a 10 GB blob requires 10,000 vouchers — each involving a sign, transmit, verify, and ack cycle. This overhead is unnecessary when the unacknowledged exposure per interval is negligible at typical rates.

**Wire bounds:** `voucher_interval_mb` MUST be in `1..=1024` (`MAX_VOUCHER_INTERVAL_MB`) wherever it appears, in both `StreamRequest` and `StreamResponse`. The bound is hardcoded in the wire schema, not governable — there is no on-chain counterpart, because vouchers carry no interval field and the contract therefore cannot verify what cadence was used during off-chain delivery. A peer that proposes or accepts a value outside the range commits a protocol error: the message is rejected, not clamped.

**Negotiation semantics:**

1. The client proposes a `voucher_interval_mb` in `StreamRequest` (see [ADR 005](005-protocol.md#adr-005-wire-protocol)).
2. The node responds with its accepted `voucher_interval_mb` in `StreamResponse`. The node may accept the client's proposal, reduce it, or omit the field to fall back to 1 MB.
3. The effective interval for the stream is `min(client_proposed, node_accepted)` — computed over values that have each already passed the wire-bounds check above, so the `min` narrows the cadence but never rescues an out-of-range message.

**Default:** `voucher_interval_mb` is optional in both `StreamRequest` and `StreamResponse`; if absent, the default is 1 MB.

**Node sovereignty:** A node can always enforce a smaller interval than the negotiated value by stopping delivery after that many MB without receiving a voucher. This uses the existing self-enforcing mechanism — no protocol change needed beyond the negotiation field.

**Risk analysis at negotiated intervals:**

| Interval | Floor rate ($0.000001/MB) | Market rate ($0.00001/MB) | Ceiling rate ($0.001/MB) |
| --- | --- | --- | --- |
| 1 MB (default) | $0.000001 | $0.00001 | $0.001 |
| 100 MB | $0.0001 | $0.001 | $0.10 |
| 1024 MB (max) | $0.001024 | $0.01024 | $1.024 |

Even the worst case (1024 MB at ceiling rate) exposes $1.024 — well below the recommended 10 USDC minimum deposit.

Voucher interval negotiation is complemented by per-node `max_blob_size` limits ([ADR 005](005-protocol.md#error-handling-and-retry-semantics)): while interval negotiation reduces per-voucher overhead for large blobs, `max_blob_size` allows nodes to refuse blobs that would create unacceptable resource pressure (cache exhaustion, extended origin pulls) regardless of voucher cadence.

### Credit Window

The voucher interval is the *billing* granularity, not the *delivery* granularity. A delivering node streams within a **credit window**: it keeps sending chunks while the unpaid balance `delivered − paid` stays within `credit_window` bytes and pauses a stream only when the next chunk would cross that bound — not at every interval boundary. Between one interval and the window, delivery and payment run concurrently. The node sends chunks ahead of the vouchers that pay for them; the payer issues a cumulative voucher at each interval and keeps receiving rather than waiting for the acknowledgement, so the acknowledgement is off the delivery critical path.

Decoupling the two rates is what keeps single-stream throughput link-bound rather than round-trip-bound. Collecting a voucher at every interval leaves the link idle for a full round trip — plus the node's durable-commit latency — once per interval, capping throughput at `interval / (RTT + service_time)` regardless of link capacity, and the repeated idle periods keep the transport's congestion window from reaching steady state. A window of several intervals keeps bytes in flight across the voucher round trip, so the link stays saturated.

**Exposure is one-sided.** The payer's exposure stays at zero: vouchers are cumulative over bytes already delivered, so the payer never signs for bytes it has not received. The node's credit exposure is exactly the window — unbilled egress already on the wire — bounded there and nowhere else. This is the same bounded-credit shape a node already fronts on the upstream leg of a cache-miss pull (`pull_ahead_bytes`, [ADR 037](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality)), and the downstream credit is strictly the cheaper of the two: egress it has already served, versus speculative USDC it pays an upstream provider. On the fused cache-miss serve path a single window bounds both quantities at once, since every pulled chunk is forwarded downstream immediately.

**Node-local policy, floored at one interval.** `credit_window` is node configuration, not a wire or governance parameter — like the voucher interval it has no on-chain counterpart and is never negotiated. It is floored at one voucher interval so a stream can always make progress (deliver a full interval, then recoup it), and a window at or below one interval reproduces the stop-and-wait cadence exactly. The self-enforcing threshold from [Voucher Interval Negotiation](#voucher-interval-negotiation) generalizes from "pause after one unvouchered interval" to "pause once the unpaid balance reaches the credit window"; the sovereignty guarantee is unchanged, since a node can always choose a smaller window (down to one interval) and pause sooner.

**Takedown latency.** The per-boundary in-flight takedown check ([ADR 011 § On Blacklist Event](011-content-takedown.md#on-blacklist-event)) runs after each collected voucher, so widening delivery to a credit window widens the window in which a takedown that lands mid-stream is first observed: the first check falls up to one credit window into the stream (steady state, roughly one interval, as later intervals recoup one at a time). This is bounded by the window and floored at one interval, and even a full window of further egress is negligible against the takedown compliance window, so it does not weaken the takedown guarantee — but it is why a node with a strict compliance target configures a smaller window rather than a larger one.

**Defaults.** The window defaults to 8 MiB, and a node's advertised interval defaults to 4 MiB — two intervals of headroom. (This node-configuration default is distinct from the 1 MB *wire* fallback of [Voucher Interval Negotiation](#voucher-interval-negotiation), which applies only when a message omits the field entirely; a node resolves its configured interval and advertises it, so the effective cadence is 4 MiB unless a peer negotiates lower.) Window and interval move independently because they bound different things: the window bounds credit exposure and how far delivery may run ahead of payment, while the interval bounds per-voucher overhead and the per-channel voucher-processing rate. Per-channel vouchers serialize behind one durable commit each (a few milliseconds of fsync), so a larger interval lifts that per-channel throughput ceiling linearly without raising exposure. At these values a single stream stays link-bound past ~100 ms round trips, where a 1 MiB stop-and-wait interval would cap it in the low tens of MiB/s.

**Durability is preserved.** The credit window does not weaken the replay guard of [Off-chain voucher state persistence](#off-chain-voucher-state-persistence): each accepted voucher's watermark is still durably committed before its `VoucherAck`, so after a restart voucher acceptance resumes from the persisted state. Delivering ahead of payment within the window is bounded *credit* risk (the window's worth of unbilled egress), not *replay* risk — the bytes streamed ahead are billed by later vouchers, each of which is committed when it arrives. The window is what lets the acknowledgement be delayed at no throughput cost, which in turn allows a node to amortize the per-voucher commit across a batch (one fsync for several vouchers, acknowledged after the commit) while still never acknowledging a voucher before it is durable.

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

1. **`closeChannel`** — callable by client or provider only. Records the submitted voucher's `amount` in `claimedAmount`, `nonce` in `claimedNonce`, and `bytesDelivered` in `claimedBytes`, sets status to `Closing`, starts the dispute window. **No fee deduction, no router call.** **Withdrawal watermark:** if the provider has already called `withdraw` (so `claimedNonce > 0`), the close voucher MUST be **non-regressing** against the recorded watermark — `nonce >= claimedNonce`, `amount >= claimedAmount`, `bytesDelivered >= claimedBytes`, `amount <= deposit`. Note this is `nonce >=`, not the strict `nonce >` that `disputeChannel`/`withdraw` require: a party MUST be able to close at the current watermark using the very voucher that was last withdrawn (`nonce == claimedNonce`), otherwise a channel with no further vouchers would be unclosable until `expiresAt`. Closing at the watermark records the same `claimed*`, so `settleChannel` routes a zero remainder; any strictly-higher voucher can still be brought in during the window via `disputeChannel` (which keeps `nonce >`). **Voucher-less close:** **either party** may close without presenting a voucher, either through the dedicated `closeChannelWithoutVoucher(channelId)` or by calling `closeChannel` with `amount=0`, `nonce=0`, `bytesDelivered=0` and an empty signature (`signature.length == 0`), which takes the same path. Both skip signature verification, and — this is the whole of the safety argument — both **advance no watermark**: the channel enters `Closing` at its recorded `claimed*`, whatever those already are. All other `closeChannel` calls — any call with `signature.length > 0`, or any call where `amount != 0`, `nonce != 0`, or `bytesDelivered != 0` — require normal EIP-712 voucher verification. A voucher is required only to *raise* the watermark, never to close.

   Closing this way is safe because it can only settle **at** the recorded watermark and never below it: `settleChannel` computes everything it pays out from `claimedAmount`/`claimedBytes` against `withdrawnAmount`/`withdrawnBytes` — the provider's remainder is the difference, the client's refund is `deposit - claimedAmount` — and nothing on the voucher-less path writes any of those four fields. So it cannot take back anything a `withdraw` or an earlier close already recorded, and the only thing it can do wrong is *understate* a claim the counterparty holds off-chain — which is exactly what the dispute window answers. Any voucher strictly above the watermark remains admissible via `disputeChannel` for the full window; voucher nonces start at 1 (nonce 0 is the sentinel for "no voucher recorded"; see [Voucher Nonce Convention](#voucher-nonce-convention)), so a real voucher always outranks a never-advanced watermark. At settlement the client is refunded `deposit - claimedAmount` — the whole deposit only where the watermark is still zero — and no router call is made when the routed remainder is zero.
2. **`disputeChannel`** (during dispute window) — callable by any address. If the submitted voucher has a strictly higher nonce, updates both `claimedAmount`, `claimedNonce`, and `claimedBytes` (see [Voucher Bytes-Delivered Field](#voucher-bytes-delivered-field)) to the new values. Still **no fee deduction, no router call**. Submissions with an equal or lower nonce revert with no state change.
3. **`settleChannel`** (after dispute window expires) — callable by anyone. Computes the un-withdrawn remainder `settleAmount = claimedAmount - withdrawnAmount` and `settleBytes = claimedBytes - withdrawnBytes`. If `settleAmount > 0`, transfers `settleAmount` of USDC to the `FeeRouter` and invokes `FeeRouter.routeSettlement(channel.provider, settleBytes, settleAmount)` in the same transaction (when all funds were already drawn via `withdraw`, `settleAmount == 0` and no router call is made). Refunds `deposit - claimedAmount` to the client. Sets status to `Closed`. The router (not `PaymentChannel`) applies the three-bucket split and increments `bytesPerEpoch[operator][epoch]` — where `epoch = block.timestamp / EPOCH_LENGTH` is derived inside `routeSettlement` — as the trailing-window served-bytes accumulator read by `DecdnGovernor._getVotes` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) as the governance vote-weight source. Split legs and bounds in [FeeRouter Integration](#feerouter-integration). Faking bytes does not increase revenue (operator base is per-byte at settlement, paid by the client), and because governance vote weight is sourced from the same per-byte counter ([ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)), over-declared capacity does not translate into governance influence either.

   > **Invariants:**
   > 1. `closeChannel` and `disputeChannel` MUST revert if the submitted voucher's `amount > channel.deposit`. This prevents client bugs or malicious over-deposit vouchers from causing an underflow revert in `settleChannel` that would lock the channel.
   > 2. `disputeChannel` MUST revert if `newAmount < claimedAmount` or `newBytes < claimedBytes`. Vouchers are cumulative across both axes; a higher nonce must correspond to a non-decreasing amount and a non-decreasing byte count. This prevents a malicious client from reducing the provider's payout — or the provider's analytics-counter byte share — via a higher-nonce dispute.
   > 3. `withdraw` and `settleChannel` MUST forward their routed delta to `FeeRouter` and call `routeSettlement` in the same transaction iff that delta `> 0` (`amount - withdrawnAmount` for `withdraw`, computed against the **pre-call** `withdrawnAmount` — the delta MUST be captured before the watermark is advanced, or it degenerates to zero; `claimedAmount - withdrawnAmount` for `settleChannel`). The provider's base share lands in the operator's wallet in the same transaction as each `withdraw` and as `settleChannel`; this is the cashflow guarantee that backs operator P&L Case A in [ADR 026 § Operator economics](026-tokenomics.md#operator-economics), now realizable incrementally rather than only at close. Reverting after partial transfer is unacceptable — implementations MUST use checks-effects-interactions, MUST guard `withdraw`, `settleChannel`, and `disputeChannel` with a `nonReentrant` modifier (the `FeeRouter` call path crosses a contract boundary and is the reentrancy surface — `withdraw` and `settleChannel` invoke `routeSettlement`; `disputeChannel` is guarded for defense-in-depth on the shared watermark), and the `FeeRouter` MUST hold a stable interface contract.
   > 4. `withdraw` MUST enforce the same monotonicity as `disputeChannel` against the shared `claimed*` watermark (`nonce > claimedNonce`, `amount >= claimedAmount`, `bytesDelivered >= claimedBytes`, `amount <= deposit`), and `withdrawnAmount` / `withdrawnBytes` MUST only ever increase. This makes `withdrawnAmount <= claimedAmount <= deposit` and `withdrawnBytes <= claimedBytes` global invariants, so no settlement path can refund or route a negative or double-counted amount.
   > 5. `closeChannel` and `disputeChannel` MUST revert a voucher that advances served bytes without advancing the routable amount past the withdrawal watermark (`claimedBytes > withdrawnBytes` while `claimedAmount == withdrawnAmount`). Since `settleChannel` forwards `settleBytes` only alongside a positive `settleAmount` (`FeeRouter` rejects a zero-amount stamp), such a voucher would otherwise have its `settleBytes` silently dropped from the per-epoch served-bytes counter that [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) uses for governance vote weight. `withdraw` is exempt by construction — it already reverts when `amount - withdrawnAmount == 0`.

A dispute that raises the settlement amount (e.g., 50 → 80 payment-token units) raises every router-bucket allocation proportionally, and a higher `claimedBytes` raises the operator's served-bytes share for the epoch (read by `DecdnGovernor` as the vote-weight source per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)). The router applies its split to each *increment* it is handed — one call per `withdraw` plus one final call at `settleChannel`, each on the delta routed at that point. Because the deltas partition the channel's lifetime claim (`Σ withdraw deltas + settle remainder = claimedAmount`, likewise for bytes), every payment-token unit and every byte is counted exactly once across the channel — never on stale intermediate watermarks, never double-counted. Byte accounting is exact (integer addition). The three-bucket *split* is computed per call, and the operator leg absorbs each call's truncation remainder (as `FeeRouter` already does for a single settlement); splitting a claim into N withdrawals therefore applies that truncation N times, so cumulative buyback/treasury can be at most a few base units lower — and the operator leg correspondingly higher — than a one-shot settlement of the same total. The drift is bounded by `N × (bucket count)` base units and is never economical to engineer (evading the buyback split requires sub-`⌈10000 / buybackBps⌉`-base-unit deltas, i.e. dust withdrawals whose gas dwarfs the rounding). A channel that never calls `withdraw` collapses to the original single settlement call.

The governance token (TOKEN) is not used for delivery payments. It is reserved for operator capacity bonding (see [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)) and governance (see [ADR 009](009-governance.md#adr-009-governance-model)).

**Rate setting is entirely up to each node.** Nodes advertise their `rate_per_mb` in probe responses and stream responses; the requester sees the rate before committing a voucher. There is no protocol-enforced rate beyond a governance-set floor and ceiling. This creates a market with natural arbitrage dynamics:

- Origin-backed nodes set a higher rate because they bear backend costs (storage + egress from their hidden backing store). They are the effective price ceiling for any blob they hold.
- A cache-only node that pays an origin-backed node to pull a blob can then serve that blob to many clients at a markup, recouping the origin cost across multiple deliveries.
- A node in a region where no peer has the content yet can charge a premium for that first delivery. Once it has the blob, other nearby nodes can pull from it at a competitive rate and compete for local clients.
- Nodes with cheaper bandwidth or better hardware can sustainably undercut others; nodes in high-demand regions can charge more and still win on latency.

The network self-balances with no central coordinator: profitable content gets replicated, competition drives prices down in well-served regions, and unpopular content stays at origin-backed node rates until demand justifies caching it.

**Origin backend economics:** The backing store choice directly affects an origin-backed node's viable rate. At the expected $0.01/GB market rate, an S3-backed node paying $0.09/GB egress loses money on every cache miss and must amortize origin pulls across a high cache-hit ratio (or price above market). Zero-egress backends — Cloudflare R2 ($0.00/GB), Backblaze B2 ($0.00/GB via Bandwidth Alliance partners), Wasabi ($0.00/GB) — keep origin-backed nodes profitable at or near market rates. High-egress backends imply higher `rate_per_mb`, which the market tolerates for content not yet cached elsewhere.

### Cooperative close (fast settle)

The `closeChannel` → dispute-window → `settleChannel` lifecycle exists to protect a party who is **offline** while the other closes: the window is the time in which a stale-low close can be countered with a higher voucher. When both parties are online and agree on the final state, that window is pure latency — and it is the **client's unused-deposit refund** that pays for it, since the provider already has a window-free path to its own earnings via `withdraw`. `cooperativeClose` removes that latency for the agreeing case.

`cooperativeClose(channelId, amount, nonce, bytesDelivered, clientVoucherSig, providerCloseSig)` settles a channel **in one transaction with no dispute window**, callable by either party or by the channel's pinned `voucherSigner`. It requires **two** signatures over the same final `(channelId, amount, nonce, bytesDelivered, token)` tuple:

- **`clientVoucherSig`** — an ordinary EIP-712 `Voucher` signature from the channel's pinned `voucherSigner`. It caps the amount the provider may claim; the provider cannot inflate `amount` past what was signed.
- **`providerCloseSig`** — the provider's EIP-712 `CooperativeClose` waiver (same field shape as a voucher, distinct typehash, recovered against the provider). By signing it the provider attests it holds no higher voucher and waives the window.

With both signatures over one tuple there is nothing left to dispute: a higher voucher would only ever benefit the provider, and the provider is the party signing the lower number. So the contract settles immediately — routing the provider's `amount − withdrawnAmount` through `FeeRouter` and refunding the client `deposit − amount` in the same transaction. The provider's fully-paid case (`amount == withdrawnAmount`, after the provider `withdraw`-drained the channel) collapses to an instant client refund with no router call.

**No funds are locked in the system.** Cooperative close is purely additive — if the node is offline, declines, or never answers, the client falls straight back to the existing `closeChannel` → window → `settleChannel` path. There is no on-chain enforcement, timeout, or escrow around the waiver request; its absence is the status quo, not a failure.

**Signer authority.** `cooperativeClose` is the one lifecycle entry point the pinned `voucherSigner` may call, and standing in that party check confers no authority it did not already have. Both signatures are verified independently of `msg.sender`, so any party holding a voucher and a matching `providerCloseSig` can already have the provider submit the identical call — the provider holds the vouchers it was paid with and signs its own waiver. Admitting the signer as a caller moves who pays the gas, nothing else, and it redirects no funds either way: the refund pays `ch.client` and the settlement pays `ch.provider`. The signer's real authority is the one the pin makes legible — its signature is what authorizes vouchers at all, up to `deposit`. The asymmetry elsewhere is deliberate — the signer is **not** a party to `closeChannel` or `closeChannelWithoutVoucher` (client or provider only), to `topUp` (client only), or to `withdraw` (provider only). The delegate's on-chain authority is confined to what it can already exercise by signing a voucher.

**Finality.** `cooperativeClose` sets the terminal `Closed` status atomically, and every entry point gates on status, so exactly one settlement wins any race (a concurrent `closeChannel`/`cooperativeClose`/second `cooperativeClose` reverts on the status check). The waiver need not be the globally-latest voucher — it must be **non-regressing against the on-chain watermark**, which the shared `claimed*` advance enforces (`amount ≥ claimedAmount`, `nonce ≥ claimedNonce`, `bytesDelivered ≥ claimedBytes`, `amount ≤ deposit`). A stale waiver below the watermark — e.g. the provider `withdraw`-advanced past it after signing — reverts (`AmountRegression`/`NonMonotonicNonce`) and the client falls back. The on-chain watermark, not the off-chain signature, is the finality anchor, so a stale waiver can never under-settle. Cooperative close is **`Open`-only**: a channel already in the dispute window settles through `settleChannel` (a party intending to ask for a waiver simply does not `closeChannel` first).

**Off-chain waiver exchange.** The client obtains `providerCloseSig` over the same `cdn/client/v1` connection vouchers already use (two new `ClientMessage` variants — see [ADR 005 § cdn/client/v1](005-protocol.md#adr-005-wire-protocol)): the client sends a `CooperativeCloseRequest { channel_id, client_signature }`, and the node replies with a `CooperativeCloseAuth { amount, nonce, bytes_delivered, signature }` declaring the final state it holds and its waiver over it. The client cross-checks the declared tuple against its own voucher store (it must match a voucher it actually signed) and submits both signatures on-chain. This is a standalone request/response, not tied to an active delivery stream — the client may dial fresh days after the last byte.

**Request authentication.** Signing a waiver is a durable, one-way commitment — the node then serves no further bytes on the channel (see Node-side discipline below) — and `channel_id = keccak256(client, provider, channelNonce)` is chain-derivable from the indexed `ChannelOpened` topics and the public `channelNonce` mapping. So the request MUST prove the requester controls the channel's pinned `voucherSigner` key, or any peer that can name a channel could force a waiver and permanently freeze the channel. `client_signature` is an EOA secp256k1 EIP-712 signature over `CooperativeCloseRequest(bytes32 channelId)` under the same `PaymentChannel` domain as vouchers; its distinct type-string keeps it from standing in for a voucher or a waiver. The node recovers it and signs only if it recovers to the channel's `voucherSigner` — the same identity the paid-delivery owner-match gate checks and the same one on-chain `cooperativeClose` recovers the client voucher against (never the funder `client`, which authorizes nothing by signature). This signature is off-chain only — it never reaches a contract; it authenticates the off-chain request, nothing more. A request whose signature is absent or does not recover to `voucherSigner` is declined by finishing the stream with no waiver — indistinguishable from the unknown-channel and no-accepted-voucher declines below, so it leaks neither channel existence nor the watermark, and the client falls back to `closeChannel`.

**Node-side discipline.** Signing a waiver is a commitment to settle at that `amount`, so a node MUST protect itself:

- **Do not sign while a delivery is in flight on that channel.** A waiver signed mid-stream would be stale the moment the next voucher is due; sign only when the node considers the channel done.
- **Stop accepting payment on the channel after signing.** Once a waiver is signed at `amount` X, the node refuses further vouchers and new streams on that `channel_id` (a local per-channel flag; the existing node-sovereignty stop in [Voucher Interval Negotiation](#voucher-interval-negotiation) already lets a node decline service). Otherwise the node could deliver past X while the client holds a waiver that settles at X, and the node would forfeit the difference.

Both are node-local policy with no contract or wire surface beyond the request/response above; honoring a request is a SHOULD (a node has no incentive to refuse — it is already paid — and the client's fallback covers a refusal). The pre-existing close-before-expiry obligation is unchanged: a node that signs a waiver the client never submits must still `withdraw`/`closeChannel` before `expiresAt` or forfeit the un-withdrawn claim at `reclaimExpired`.

### Off-chain Voucher Rejections (Wire Encoding)

When a node rejects a voucher off-chain — before any gas would be spent — the rejection is returned **in-band** mid-stream as a `StreamError` message carrying `VoucherRejected { reason }` (per [ADR 005 § Stream Lifecycle State Machine](005-protocol.md#stream-lifecycle-state-machine), this transitions the stream `Streaming → Failed` cleanly without a QUIC stream reset). Voucher validation can only fire after at least one `Voucher`, necessarily after `StreamResponse { ok: true }` — so payment rejections never use the initial-response error path that delivery-side failures (`NotFound`, `Overloaded`, etc.) take. Full reason enum and per-reason retry semantics: [ADR 005 § VoucherRejected semantics](005-protocol.md#voucherrejected-semantics).

Eight of the nine `VoucherRejectReason` values mirror the off-chain validation enums `ChannelError` / `VoucherError` (in `crates/incentive/`) one-to-one, and each maps back to the on-chain invariant it protects; the ninth, `RetryLater`, is the off-chain-only signal for a transient persist-write failure and protects no on-chain invariant (see [Off-chain voucher state persistence](#off-chain-voucher-state-persistence)):

| `VoucherRejectReason` | Off-chain trigger | On-chain invariant protected |
|---|---|---|
| `BadSignature` | Malformed signature bytes | EIP-712 `SignatureChecker` would revert at `closeChannel` (see [EIP-712 Voucher Signature](#eip-712-voucher-signature)) |
| `WrongSigner` | Signature recovers to the wrong address | `closeChannel` would revert when the recovered signer ≠ the channel's pinned `voucherSigner` |
| `WrongChannel` | `voucher.channel_id` mismatch | EIP-712 domain binds the voucher to a specific `channelId`; off-channel vouchers authorize nothing |
| `WrongToken` | `voucher.token` mismatch | Cross-token replay defense (see [Replay attack on vouchers](#replay-attack-on-vouchers)) |
| `StaleNonce` | `voucher.nonce ≤ last accepted nonce` | `disputeChannel` requires strictly higher nonce ([Voucher Nonce Convention](#voucher-nonce-convention)) |
| `AmountRegression` | `voucher.amount < last accepted amount` | Invariant 2 — `disputeChannel` reverts if `newAmount < claimedAmount` (see [Fee Routing on Disputed Closes](#fee-routing-on-disputed-closes)) |
| `BytesRegression` | `voucher.bytes_delivered < last accepted bytes_delivered` | Invariant 2 — `disputeChannel` reverts if `newBytes < claimedBytes` |
| `InsufficientDeposit` | `voucher.amount > channel.deposit` | Invariant 1 — `closeChannel` / `disputeChannel` revert if `voucher.amount > channel.deposit` |
| `RetryLater` | Transient persist-write failure (`ChannelError::Store`) | none — node-side store fault, not a voucher defect; the client resends the **same** voucher unchanged on a fresh stream |

Surfacing these reasons off-chain saves both parties the gas of a doomed on-chain submission and gives the payer enough detail to recover (e.g., refresh state and re-sign for `StaleNonce`, top up for `InsufficientDeposit`) instead of an opaque connection drop. A delegated payer — one whose channel was opened by a funder that pinned it as `voucherSigner` — cannot recover by those means directly: it holds no funds to `topUp` (funder-only) and cannot read its watermark from chain (the claim watermark is 0 until settlement), so the node attaches an authenticated watermark bundle to the regression/exhaustion rejections for self-heal, and defers a genuine top-up to the funder (see [ADR 005 § `VoucherRejected` semantics](005-protocol.md#voucherrejected-semantics)). Riding in-band rather than via a QUIC stream reset preserves the reason for client retry logic without burning [ADR 013](013-schema-evolution.md#adr-013-schema-evolution) application-error-code numbers for the structured-response case.

## Consequences

### Positive

- On-chain costs are amortized across an entire channel lifetime — open + close + settle = three transactions regardless of how many MB are delivered (settle can be called by any address, allowing third-party settlement bots)
- USDC denomination gives node operators predictable unit economics: delivery revenue covers infrastructure costs without exposure to TOKEN price movements
- The voucher is the payment receipt; the BLAKE3 hash is the delivery receipt. Together they provide mutual protection: the client doesn't sign a voucher for bytes that fail hash verification; the node stops delivering if vouchers stop arriving
- Maximum risk per voucher interval at default cadence (1 MB) is $0.00001 at market rate — negligible. At the wire ceiling (1024 MB) and ceiling rate ($0.001/MB), worst-case risk is $1.024 per interval — still small relative to the recommended 10 USDC minimum deposit (see [Voucher Interval Negotiation](#voucher-interval-negotiation))
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

The self-enforcing stop is sufficient. Maximum loss is one voucher interval at the negotiated cadence: default cadence (1 MB × market rate ≈ $0.00001) is negligible; 100 MB at market rate is ~$0.001; the wire ceiling (1024 MB) at the ceiling rate is ~$1.024 — still negligible relative to channel deposits. Nodes serving high-value content can unilaterally enforce smaller intervals regardless of what was negotiated.

That bound applies to a *funded* channel. A channel whose remaining deposit cannot cover even the first credit window is refused before the node signs a success `StreamResponse`, so it is never served the free interval at all — the seller-side pre-flight deposit guard of [ADR 037 § Implementation status](037-regional-proxy-warming.md#implementation-status-856), which fronts both the cache-miss and the direct-serve paths.

#### Stale close

Client submits an old voucher (lower amount) to close the channel, underpaying the node.

The dispute window (default 48 hours, sized above the L2 force-inclusion delay; see [L2 sequencer censorship](#l2-sequencer-censorship) below) covers this if the node is online. **Defense layers:**

1. **In-process dispute monitor.** A lightweight thread inside the node binary watches the chain for `ChannelCloseInitiated` events on its channels and auto-submits the latest voucher via `disputeChannel`. Zero-latency to the local voucher store; handles the common case where the node is online. Implementation is a SHOULD for production node binaries.
2. **Operator-arranged redundancy.** Multi-instance deployments, hot-standby relays, peer agreements to relay vouchers. Out of protocol scope; the protocol does not define a wire format for voucher-relay arrangements between operators.
3. **Permissionless on-chain dispute submission.** `disputeChannel` accepts submissions from any address holding a higher-nonce voucher — operators with their own infrastructure or counterparties can submit directly. See [Appendix: Fraud Detection](appendix-fraud-detection.md#appendix-permissionless-stale-close-detection).

The node-offline-for-the-full-48h case is a node-operations responsibility, not a protocol gap.

#### Probe fishing

Client sends probe requests to many nodes at high frequency to map the network or exhaust node resources without ever paying.

Per-NodeId rate limiting alone is bypassable: clients are not bonded, NodeIds are free to rotate, and iroh connection setup is cheap. The mitigation is the layered token-bucket rate limit in [ADR 005 § Probe rate limiting](005-protocol.md#probe-rate-limiting): per-peer (NodeId) plus per-IP plus a global node cap, applied before any signature or hold-slot allocation. The per-IP layer raises the cost of bulk probing because IP rotation requires money (proxies, IPv6 delegation, cloud bills) while NodeId rotation does not; the global cap is defence in depth.

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

#### Channel close front-running

Node monitors the mempool and front-runs a client's channel close with a higher voucher submission.

Not an attack. The contract always settles the highest valid voucher, and only the channel's pinned `voucherSigner` can sign a valid voucher; a node submitting the latest voucher before the client is the intended happy path. Fabricating a higher voucher requires forging that signer's ECDSA signature, which is cryptographically infeasible.

#### Third-party forced channel close (DoS)

A third party holding a valid voucher calls `closeChannel` to force the channel from `Open` to `Closing`, halting delivery.

**Resolved: access control restriction.** `closeChannel` requires `msg.sender == channel.client || msg.sender == channel.provider`. Third parties cannot initiate a close regardless of whether they hold a valid voucher. Permissionless fraud detection is unaffected — third parties operate via `disputeChannel` during the dispute window ([Appendix: Fraud Detection](appendix-fraud-detection.md#appendix-permissionless-stale-close-detection)). The residual risk is a `disputeChannel` call with an intercepted voucher, which can only *improve* the settlement (higher nonce required). On-path network interception of vouchers is mitigated by QUIC transport (TLS 1.3), though this does not address endpoint compromise or other forms of leakage.

#### Operator early withdrawal (no dispute window)

A provider calls `withdraw` to redeem accrued funds while the channel is still `Open`, instead of waiting for the `closeChannel` → dispute-window → `settleChannel` path. Could this let an operator steal from the client or evade the dispute protections?

**Not an attack — safe by construction.**

- **No dispute window is needed because there is nothing to dispute.** `withdraw` redeems a **signed**, cumulative, monotonic voucher, verified against the channel's pinned `voucherSigner`. The provider can never claim more than that signer authorized (and never more than `deposit`, by invariant 1), and the funder chose the signer irrevocably at `openChannel` — so the claim is bounded by an authority the funder itself fixed and cannot be surprised by. The provider can only harm *itself* by withdrawing against a stale lower voucher; the remainder is still claimable later. The dispute window exists to let an offline party replace a *stale low* voucher submitted by the other party at close; a provider redeeming the highest voucher it holds, against its own watermark, has no counterparty to be defended against. The un-withdrawn remainder still passes through the full dispute window at `closeChannel`.
- **Residual: the signing key is the blast radius.** A compromised `voucherSigner` key can authorize claims up to the channel's `deposit`, and `withdraw` will pay them out with no window in which to intervene. That exposure is exactly why the pin is immutable — a funder able to swap the signer mid-channel could equally void vouchers the provider had already earned — and why the deposit is the bound worth sizing: it caps what a delegated key can ever cost, and delegating a key confines the loss to the channels that pinned it rather than to the funder's balance.
- **No regression or double-spend.** `withdraw` shares the `claimed*` watermark with `disputeChannel`/`closeChannel` and enforces the same monotonicity (invariant 4); `withdrawnAmount`/`withdrawnBytes` only increase. A voucher cannot be re-withdrawn, and a later close cannot regress below the withdrawn point. `FeeRouter.routeSettlement` is purely additive, so the per-withdraw and final-settle deltas count each byte and each USDC unit exactly once.
- **Client-refund safety.** The client refund at close/expiry is always `deposit - claimedAmount` (or `deposit - withdrawnAmount` on `reclaimExpired`), which is `≥ 0` by the watermark invariant. Withdrawals never trap client funds or refund more than the deposit.
- **Voucher-less close cannot claw back a withdrawal.** A close presented without a voucher settles at the channel's *recorded* watermark, and a withdrawal has already advanced that watermark to the amount it paid out. The client's refund is `deposit - claimedAmount`, so what the provider drew is outside it; a voucher-less close can never regress the watermark or recover paid funds.
- **Governance-weight timing.** Withdrawn bytes are stamped into `bytesPerEpoch` in the epoch they are withdrawn (closer to actual delivery) rather than all at final settle. An operator's choice of *when* to withdraw shifts epoch attribution slightly, but this is no stronger than the settle-timing flexibility operators already have, the total served-bytes count is unchanged, and the count still reflects only real client-paid bytes — so it introduces no new wash-trading or vote-weight vector ([ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight); [ADR 016 § wash-trading](016-contract-interactions.md#tunable-economics)).

### Network-level

#### Eclipse attack

Attacker surrounds a client with malicious nodes so all probe responses come from nodes under attacker control.

BLAKE3 verification catches data corruption regardless of peer-table composition; the remaining DoS variant (attacker-controlled peer set refuses to serve) is resolved in [ADR 012 § Bootstrap and Trust Model](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model): production uses multi-source bootstrap (on-chain registry + hardcoded DNS seeds) so an attacker must compromise both to fully eclipse a client; minimum honest-peer diversity is a supplementary client-side policy.

#### Gossip flooding

Node sends high-volume `NodeAnnounce` messages to exhaust peer table memory or crowd out legitimate announcements.

Registry check + per-sender rate limiting. Residual gap: the local registry cache may be up to 10 minutes stale, briefly allowing recently-unbonded nodes to flood; mitigated by tightening the registry cache refresh on high flood detection.

#### Sybil nodes

Attacker bonds many cheap nodes to dominate probe responses for popular content, controlling pricing in a region.

The core weakness is governance-token-price dependency: at $0.001/TOKEN, the 1 Gbps entry-tier capacity bond (~50,000 TOKEN at default `k=12.6`, `α=1.2` per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)) costs $50 per sybil node. Higher tiers are super-linearly more expensive (10 Gbps ≈ 795K TOKEN; 100 Gbps ≈ 12.6M TOKEN), but a sybil fleet can be dominated by many entry-tier nodes. The unified selection score `rate_per_mb × rtt_ms × (1 / max(reputation, 0.1)²)` (see [ADR 001](001-network.md#node-selection-algorithm)) helps — a sybil fleet must be real hardware in the right geography, competitively priced, and build reputation over time — but does not eliminate the risk when the token is cheap. Options:

- **Option A — Governance raises the 1 Gbps-tier bond via the `k`-bound mechanism in [ADR 026 § Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds).** The 1G-tier bond is governable within [10K, 200K TOKEN]; raising it lifts the whole curve. Governance is incentivised to do so when TOKEN price is low, since a sybil-dominated network reduces usage and TOKEN value. Reactive but aligned.
- **Option B — Bond denominated in USD equivalent via oracle.** Requires a price oracle, which introduces oracle dependency, manipulation, and downtime risks (see the rate-floor discussion above). The same concerns apply here, but the impact of oracle failure is lower (new operators temporarily blocked, not payments broken).
- **Option C — Reputation as a second filter.** New nodes (low reputation, few settled channels) are deprioritised in client selection even if their `rate_per_mb × rtt_ms × (1 / max(reputation, 0.1)²)` score is competitive. A sybil fleet takes time to build reputation, limiting its effectiveness during that window.

#### Rate manipulation cartel

Colluding nodes in a region hold rates artificially high.

Origin-backed nodes set the effective price ceiling for any blob. Clients can always probe origin-backed nodes directly and pay their rates as a guaranteed fallback. Any node outside the cartel that undercuts wins all local traffic — the incentive to defect is strong. New entrants can join the cache-only role permissionlessly by bonding; the origin role for content in registered namespaces requires `OriginAssignment` membership ([ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)), but cache-only competition is sufficient to discipline the rate cartel because cache delivery is interchangeable with origin delivery from the requester's perspective.

#### Content withholding

A node bonds, responds to probes with `has_blob: true`, but refuses to serve — collecting credibility in the peer table without actually participating.

**Withholding is not a slashable offense** — operators may legitimately take content offline for maintenance, migration, or business reasons, and slashing for availability creates perverse incentives. The protocol does not guarantee availability; publishers who want fault tolerance opt into it by proposing multiple operators, and the network deprioritizes flaky nodes through reputation:

- **Publisher-chosen operator sets.** Content owners hold a publisher identity ([ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces)) and propose an origin operator set per namespace. Governance ratifies the proposal via the standard timelock path ([ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)). Set size is the publisher's call — a single trusted operator works for hobbyist publishers, multi-operator sets defuse single-point withholding for publishers who want it. Content served under namespace 0 has no authorized origins — it is served best-effort from cache/DHT only ([ADR 002 § Namespace 0](002-content-addressing.md#namespace-0)).
- **Reputation fast-path.** Nodes that respond `has_blob: true` to probes but fail to deliver accumulate reputation penalties at a steeper rate. A node with consistently poor availability is deprioritized in provider selection and loses delivery revenue. Publishers may use the reputation signal as input when proposing or revoking operators in their namespace's assignment.

Note: the probe-triggered eviction hold ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)) addresses a related but distinct problem. Withholding is a node that has the blob but refuses to serve it (behavioral — handled by reputation). The eviction hold addresses a node that advertised `has_blob: true` but lost the blob to cache pressure before the stream request (mechanical — kept resident by the hold so the follow-up pull succeeds).

#### Replay attack on vouchers

Attacker intercepts a signed voucher and attempts to replay it against a different channel or after close.

EIP-712 typed data over `{channelId, amount, nonce, bytesDelivered, token}` binds the voucher to a specific channel. The EIP-712 domain separator (see [EIP-712 Voucher Signature](#eip-712-voucher-signature)) further binds each voucher to a specific chain and contract deployment, preventing replay across different L2s, contract upgrades, or test vs production environments. The monotonically increasing nonce (starting at 1; see [Voucher Nonce Convention](#voucher-nonce-convention)) prevents resubmission after settlement.

#### Off-chain voucher state persistence

The on-chain protections in [Replay attack on vouchers](#replay-attack-on-vouchers) constrain only what the contract accepts at settlement. They do not prevent the **delivering node** from re-delivering bytes off-chain for a voucher it already honoured: a node holding voucher state only in memory will, after restart, re-accept any earlier-nonce voucher the client (or any wire observer) resubmits and serve the bytes again.

Required invariant: a node MUST persist `(last_nonce, last_amount, last_bytes_delivered)` per channel and durably commit (fsync, on disk-backed implementations) **before** sending `VoucherAck` or delivering any further bytes for that voucher. After a restart, voucher acceptance MUST resume from the persisted state — never from `last_nonce = 0`. An absent entry is semantically identical to a never-seen channel (`last_nonce == 0`, per [Voucher Nonce Convention](#voucher-nonce-convention)); a record exists iff the node ever advanced past the initial sentinel. Entries are dropped only when the node observes `ChannelSettled` on-chain.

A failed persist write MUST surface as a voucher-acceptance failure — the node returns a transient-failure rejection through the [Off-chain Voucher Rejections (Wire Encoding)](#off-chain-voucher-rejections-wire-encoding) channel, and MUST NOT send `VoucherAck`. The wire code for transient persistence failures is `VoucherRejectReason::RetryLater` (the ninth reason, added precisely for this case and carrying no on-chain-invariant meaning); the existing `StaleNonce` / `InsufficientDeposit` codes are NOT appropriate substitutes because they would tell the client to refresh state or top up the deposit when in fact the same voucher should be retried unchanged. The node finishes the stream cleanly after the rejection (no QUIC reset), so the client resends the same voucher on a fresh stream rather than treating an opaque connection drop as a permanent failure. Persisting after acknowledgement re-opens the same replay window for the crash interval between the two writes.

Storage backend and trait shape are implementation concerns; the Rust implementation exposes a `ChannelStateStore` seam in `crates/incentive` with a `redb`-backed persistent implementation in `crates/node`. The protocol fixes only the ordering above.

## Contract Interfaces

### PaymentChannel

The `PaymentChannel` is the payment-channel contract, handling payment-token channels. The payment token is USDC; its address is fixed at deployment as an immutable constructor argument.

**Channel state:**

```solidity
// Fields are ordered so each address shares a storage slot with a uint64
// timestamp (and status), packing the header into 3 slots instead of 4.
// `voucherSigner` takes a fourth slot of its own: `client`(20) + `openedAt`(8)
// + `status`(1) leaves only 3 free bytes, and every other packed slot is
// already at 28, so a 20-byte address cannot join one.
struct Channel {
    address client;           // funder: puts up the deposit, receives the refund, owns the channelNonce sequence
    uint64  openedAt;
    uint8   status;           // 0 = Open, 1 = Closing (dispute window active), 2 = Closed (settled)
    address voucherSigner;    // the address every settlement path verifies voucher signatures against; pinned at open
    address provider;
    uint64  expiresAt;
    address token;            // USDC; set once at deployment (immutable)
    uint64  disputeDeadline;  // set when close is initiated; fixed for the dispute window
    uint256 deposit;          // in USDC base units (6 decimals)
    uint256 claimedAmount;    // cumulative amount of the current best voucher; advanced by withdraw (while Open), closeChannel, and disputeChannel
    uint256 claimedNonce;     // nonce of the current best voucher, for dispute/withdraw comparison
    uint256 claimedBytes;     // cumulative bytes delivered per the current best voucher; forwarded to FeeRouter (see ADR 026)
    uint256 withdrawnAmount;  // cumulative USDC already paid out to the provider via withdraw while Open (≤ claimedAmount)
    uint256 withdrawnBytes;   // cumulative bytes already routed/counted via withdraw (≤ claimedBytes)
}
```

**Channel roles.** A channel is a three-role object:

- **`client` — the funder.** It transfers the deposit in, receives the `deposit - claimedAmount` refund at settlement (and the whole `deposit - withdrawnAmount` at `reclaimExpired`), owns the `channelNonce` sequence the `channelId` is derived from, is the only address that may `topUp`, and is the address the [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting) takedown and blacklist gates evaluate.
- **`provider` — the delivering operator.** It is the only address that may `withdraw`, the recipient of every routed settlement, and the address the `CooperativeClose` waiver is recovered against.
- **`voucherSigner` — the voucher authority.** It is the address `withdraw`, `closeChannel`, `disputeChannel`, and `cooperativeClose` verify EIP-712 voucher signatures against. It is chosen by the funder at `openChannel` and **pinned permanently**: there is no `setVoucherSigner`. Immutability is the security property — a mutable signer would let a funder retroactively void a voucher the provider had already earned. Passing `address(0)` resolves it to `msg.sender`, which is the ordinary self-signing case where funder and signer are the same address.

The delegate must not be the counterparty: pinning `provider` as `voucherSigner` hands the provider unilateral authority to sign vouchers draining the full deposit. This is the caller's responsibility, not an on-chain check, since any address may legitimately be delegated.

**Channel ID:** `channelId = keccak256(abi.encodePacked(client, provider, channelNonce))` where `channelNonce` is a monotonic per-client counter stored on-chain as `clientChannelNonce[msg.sender]`. `voucherSigner` is deliberately **not** an input to the derivation: the funder alone owns the nonce sequence and can therefore still pre-compute a channel's id before opening it, whichever signer it delegates. **Ordering:** `openChannel` reads the current nonce, uses it to compute `channelId`, then increments: `n = clientChannelNonce[msg.sender]; channelId = keccak256(..., n); clientChannelNonce[msg.sender] = n + 1`. The client pre-computes the next channelId off-chain by reading `clientChannelNonce[client]` and using that value directly — no off-by-one because the contract uses the same value before incrementing. The `channelNonce` is global per-client (not per-provider), ensuring uniqueness across all of a client's channels.

> **Terminology:** `channelNonce` (the channel creation counter) is distinct from the voucher `nonce` (the monotonic sequence number within a channel used in EIP-712 voucher signatures). The former uniquely identifies channels; the latter orders vouchers within a channel. In implementation, consider naming the on-chain mapping `clientChannelCounter` to avoid confusion with voucher nonces.

| Group | Function | Purpose |
| --- | --- | --- |
| Nonce | `clientChannelNonce(client) → uint256` | Per-client monotonic counter used in `channelId` derivation. |
| Lifecycle | `openChannel(provider, deposit, voucherSigner) → channelId` | Open a payment-token channel; derives `channelId` from the current `clientChannelNonce[msg.sender]` then increments it; pins `voucherSigner` (`address(0)` → `msg.sender`); emits `ChannelOpened`. |
| Lifecycle | `topUp(channelId, additionalDeposit)` | Client-only: add funds to an open channel (does not extend `expiresAt`). |
| Lifecycle | `withdraw(channelId, amount, nonce, bytesDelivered, signature)` | Provider-only: redeem the accrued delta of a voucher signed by the channel's `voucherSigner` while the channel stays `Open`; routes the delta through `FeeRouter` same-tx (no dispute window). |
| Lifecycle | `closeChannel(channelId, amount, nonce, bytesDelivered, signature)` | Client or provider: initiate close with the latest voucher; starts dispute window. All-zero arguments with an empty signature close at the recorded watermark instead. |
| Lifecycle | `closeChannelWithoutVoucher(channelId)` | Client or provider: close at the recorded claim watermark without presenting a voucher; starts the dispute window. |
| Lifecycle | `disputeChannel(channelId, amount, nonce, bytesDelivered, signature)` | Any address: submit a higher-nonce voucher during the dispute window. |
| Lifecycle | `settleChannel(channelId)` | Post-dispute-window: forward the un-withdrawn remainder (`claimedAmount - withdrawnAmount`) to `FeeRouter`; refund `deposit - claimedAmount`. |
| Lifecycle | `cooperativeClose(channelId, amount, nonce, bytesDelivered, clientVoucherSig, providerCloseSig)` | Client, provider, or the pinned `voucherSigner`: settle in one tx with **no dispute window**, given a voucher from the pinned signer and a matching provider `CooperativeClose` waiver over the same final tuple. See [§ Cooperative close](#cooperative-close-fast-settle). |
| Lifecycle | `reclaimExpired(channelId)` | Client or provider: refund `deposit - withdrawnAmount` on an expired channel that was never closed. |
| View | `getChannel(channelId) → Channel` | Read the on-chain `Channel` struct. |
| View | `getRateBounds() → floor` | Current `deliveryFloor` in payment-token base units. |
| View | `feeRouter() → address` | Configured `FeeRouter` target ([ADR 026](026-tokenomics.md#adr-026-tokenomics)). |
| Governance | `setFeeRouter(addr)` | Replace router target. `GOVERNANCE_ROLE`-gated; routed through the standard 48h `TimelockController` delay; emits `FeeRouterUpdated(address oldRouter, address newRouter)`. See [§ Governance setter: setFeeRouter](#governance-setter-setfeerouter) below. |
| Governance | `setDisputeWindow(seconds)` | Dispute window (bounded 172800–259200 — 48h–72h). |
| Governance | `setRateBounds(floor)` | Per-MB delivery-rate floor in payment-token base units. Capped at `MAX_RATE_PER_MB`. |

Bucket shares (60/30/10) are governed on `FeeRouter`, not on `PaymentChannel`; the treasury share (10%) is configured on `FeeRouter`.

#### Governance setter: setFeeRouter

```solidity
function setFeeRouter(address newRouter) external onlyRole(GOVERNANCE_ROLE);

event FeeRouterUpdated(address indexed oldRouter, address indexed newRouter);
```

`setFeeRouter` re-points the configured `FeeRouter` for future `settleChannel` calls. Required because the audited contract surface is fixed at deploy time, yet the `FeeRouter` may need replacing (bug fix, structural upgrade) without redeploying `PaymentChannel` and forcing every open channel to re-issue vouchers.

**Authority and timelock.** Only callable by `GOVERNANCE_ROLE` (held by the `TimelockController` post-deploy per [ADR 016 § Post-Deployment Initialization](016-contract-interactions.md#post-deployment-initialization)). `DecdnGovernor` proposals to replace the router execute through the standard 48h timelock per [ADR 009](009-governance.md#adr-009-governance-model). Calls outside that path revert.

**Validation.** Reverts on `address(0)`, on the same address as the current `feeRouter`, and on a `newRouter` whose code size is zero (EOA / undeployed address) — the same `code.length` invariant the constructor enforces, because `_route` would otherwise advance channel state while `routeSettlement` silently no-ops, desyncing settlement accounting and stranding claimed USDC. Beyond that code-size check the new router is not further interrogated — the deeper cross-validation invariants in [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics) live on `FeeRouter` itself, so re-pointing at a wrong-but-deployed contract still surfaces at the next `settleChannel` rather than at the setter.

**Open channels are unaffected.** Vouchers signed against this `PaymentChannel` remain valid because the EIP-712 domain separator hashes the contract's own address, not the configured `FeeRouter`. Carve-out documented in [ADR 016 § No proxy deployment patterns](016-contract-interactions.md#no-proxy-deployment-patterns): helper-contract addresses are not domain-separator inputs and may be re-pointed via governance without invalidating signatures.

**Settlement during the swap.** Settlements beginning before the timelock executes use the previous router; those beginning after use the new one. `settleChannel` reads `feeRouter()` at call time, and `routeSettlement` is a single transaction, so no in-flight settlement splits across routers.

> **Reentrancy protection:** All state-mutating functions that perform external calls (ERC-20 transfers) — `openChannel`, `topUp`, `withdraw`, `settleChannel`, `reclaimExpired` — MUST use `nonReentrant` guards and follow checks-effects-interactions. `withdraw` and `settleChannel` additionally cross the `FeeRouter` boundary, so the effects (watermark advance) MUST be committed before the `approve` + `routeSettlement` interaction.

**`topUp` behavior:** `topUp(channelId, additionalDeposit)` adds funds to an open channel:

- **Status precondition:** MUST require status `Open` and `block.timestamp < expiresAt` (reverts on `Closing`, `Closed`, or expired).
- **Caller:** client only (`require(msg.sender == channel.client)`).
- **Effects:** Transfers `additionalDeposit` from `msg.sender` to the contract via `safeTransferFrom`. Updates `channel.deposit += additionalDeposit`. Does NOT extend `expiresAt` (to prevent indefinite lock-in — the channel's utility is bounded by the initial `maxChannelDuration`).
- **Modifiers:** `nonReentrant`.
- **Emits:** `ChannelToppedUp(channelId, additionalDeposit, newDeposit)`.

**`withdraw` behavior:** `withdraw(channelId, amount, nonce, bytesDelivered, signature)` lets a provider redeem accrued funds **while the channel stays `Open`**, instead of waiting for `closeChannel` → dispute window → `settleChannel`. It is safe without its own dispute window because it can only ever redeem a **signed**, cumulative, monotonic voucher, verified against the channel's pinned `voucherSigner`: the provider can never claim more than that signer authorized, the funder fixed that signer irrevocably at open, and the provider can only harm itself by under-claiming (a later voucher still settles the rest). The residual is the signing key itself — a compromised key can authorize up to `deposit`, which is the bound the pin is designed to make legible (see [Operator early withdrawal](#operator-early-withdrawal-no-dispute-window)).

- **Status precondition:** MUST require status `Open` and `block.timestamp < expiresAt` (reverts on `Closing`, `Closed`, or expired), matching `topUp`. Does NOT extend `expiresAt`.
- **Caller:** provider only (`require(msg.sender == channel.provider)`).
- **Voucher validation (identical monotonicity to `disputeChannel`):** verify the EIP-712 signature against `channel.voucherSigner` (see [EIP-712 Voucher Signature](#eip-712-voucher-signature)); require `nonce > claimedNonce`, `amount >= claimedAmount`, `bytesDelivered >= claimedBytes`, and `amount <= deposit` (invariant 1). These advance the same best-voucher watermark (`claimedAmount` / `claimedNonce` / `claimedBytes`) that `closeChannel` and `disputeChannel` use — so a withdrawal can never regress below an already-recorded voucher, and a later close/dispute can never regress below a withdrawal.
- **Effects (checks-effects-interactions):** advance the watermark (`claimedAmount = amount`, `claimedNonce = nonce`, `claimedBytes = bytesDelivered`); compute `delta = amount - withdrawnAmount` and `bytesDelta = bytesDelivered - withdrawnBytes`; require `delta > 0` (reverts otherwise — there is nothing to withdraw and `FeeRouter` rejects a zero amount); set `withdrawnAmount = amount` and `withdrawnBytes = bytesDelivered`; then `approve(feeRouter, delta)` and call `FeeRouter.routeSettlement(channel.provider, bytesDelta, delta)` in the same transaction (the same approve-then-route pattern `settleChannel` uses). The router applies the three-bucket split, pays the operator's base share same-tx, and increments `bytesPerEpoch[operator][epoch]` by `bytesDelta` — `routeSettlement` is purely additive (`+=`) with no per-channel guard, so calling it once per withdrawal plus once at settlement counts each byte and each USDC unit **exactly once** (the per-call amounts and bytes telescope to the final `claimedAmount` / `claimedBytes`).
- **Modifiers:** `nonReentrant`.
- **Emits:** `ChannelWithdrawn(channelId, provider, withdrawnDelta, bytesDelta, newWithdrawnAmount)` (where `withdrawnDelta` is the just-routed `delta`).

Because `withdraw` advances the shared watermark, a subsequent voucher-less close settles at that advanced watermark rather than at zero — correct, since the provider has already drawn real funds and a client must not be able to close for a full refund afterwards.

`withdraw` is purely an operator-initiated on-chain action; it changes nothing about off-chain voucher exchange. Clients keep signing the same cumulative vouchers and the node keeps accepting them up to `deposit` (the `crates/incentive` voucher-acceptance path is unchanged). To submit a withdrawal the node reads the authoritative on-chain `withdrawnAmount` / `claimedNonce` and routes the delta of the highest voucher it holds; deciding *when* (or whether) to withdraw is a node operational policy, not a protocol requirement.

#### Initial deployment values

The constructor takes `(usdc, capacityBond, feeRouter, disputeWindow, maxChannelDuration, deliveryFloor, admin)` per [ADR 016 § Contract Inventory](016-contract-interactions.md#contract-inventory). Every governable parameter it exposes is a constructor argument; there are none it defaults. `disputeWindow` and `maxChannelDuration` are constructor arguments validated against the hardcoded safety bounds (deployment defaults: 48h and 90d respectively — see the bounds table below and [ADR 009](009-governance.md#adr-009-governance-model) for governance ranges). The constructor MUST reject any zero address among `(usdc, capacityBond, feeRouter, admin)` and a `feeRouter` whose code size is zero (EOA / undeployed address).

Default deployment value for `disputeWindow`: **172800 seconds (48 hours)** — sized to guarantee effective dispute response time under L2 sequencer censorship (see [§ L2 sequencer censorship](#l2-sequencer-censorship) below). Safety bounds per [ADR 009](009-governance.md#adr-009-governance-model): 172800–259200 seconds (48h–72h). Under [ADR 026](026-tokenomics.md#adr-026-tokenomics) the constructor carries no `feePercentage` / `discountedFeePercentage` / treasury-address parameters; bucket shares are governed on `FeeRouter`, and the treasury bucket is one of `FeeRouter`'s three buckets (see [FeeRouter Integration](#feerouter-integration)).

#### L2 sequencer censorship

A malicious closer (or colluding sequencer) submits `closeChannel` with a stale voucher and ensures all `disputeChannel` transactions are censored for the full dispute window. Counterparties fall back to L1 forced inclusion, but this takes up to ~24 hours on Arbitrum (similar paths on other OP-Stack chains). If the dispute window is no longer than that delay, effective dispute response time is zero by the time the forced-inclusion transaction is processed.

**Mitigation — baseline dispute window.** Censorship resistance comes solely from keeping the baseline dispute window above the L2's maximum force-inclusion delay: the dispute window default is **48 hours** (172800 seconds), which guarantees at least 24 hours of effective dispute response time on any L2 with a force-inclusion delay ≤ 24 hours. There is no on-chain forced-inclusion detection or deadline extension — a signed force-included `disputeChannel` is indistinguishable on-chain from a sequencer-included one, so the window itself carries the guarantee. The setting is L2-agnostic and the governance floor equals the 48h default (bounds 48h–72h per [ADR 009](009-governance.md#adr-009-governance-model)), so the baseline can only be tightened upward and never dropped below the force-inclusion delay.

#### Events

All events use indexed `channelId` plus an indexed actor field where applicable. `ChannelOpened` uses three indexed fields (`channelId`, `client`, `provider`) — the EVM maximum — so wallets and CLIs can `eth_getLogs` filter by either party without parsing tx history. `voucherSigner` rides in the same event as an **unindexed** field, since those three slots are already spent.

| Event | Emitted by | Non-indexed fields |
| --- | --- | --- |
| `ChannelOpened(channelId, client, provider, …)` | `openChannel` | `deposit`, `expiresAt`, `voucherSigner` |
| `ChannelCloseInitiated(channelId, initiator, …)` | `closeChannel`, `closeChannelWithoutVoucher` | `amount, nonce, bytesDelivered, disputeDeadline` |
| `ChannelDisputed(channelId, disputor, …)` | `disputeChannel` | `newAmount, newNonce, newBytes` |
| `ChannelSettled(channelId, provider, …)` | `settleChannel` | `routedAmount` (payment token forwarded to `FeeRouter` = the un-withdrawn remainder `claimedAmount - withdrawnAmount`; equals `claimedAmount` when no `withdraw` occurred), `bytesDelivered` (the remainder `claimedBytes - withdrawnBytes`, counted toward operator's epoch byte counter), `clientRefund` (= `deposit - claimedAmount`) |
| `ChannelExpiredReclaimed(channelId, client, …)` | `reclaimExpired` | `clientRefund` (= `deposit - withdrawnAmount`) |
| `ChannelToppedUp(channelId, …)` | `topUp` | `additionalDeposit, newDeposit` |
| `ChannelWithdrawn(channelId, provider, …)` | `withdraw` | `withdrawnDelta`, `bytesDelta`, `newWithdrawnAmount` |
| `RateBoundsUpdated` | `setRateBounds` | `newDeliveryFloor` |

`ChannelOpened` remains the log-scan entry point for external parties without a dedicated index: a client lists their channels via `eth_getLogs(topics=[ChannelOpened, *, paddedClientAddress])`; a provider does the same with their address in the third topic; an indexer keys on `channelId`. This keeps channels discoverable via log scans even before any subsequent on-chain activity (no `topUp`, `withdraw`, `closeChannel`, or `disputeChannel`).

A node reconciling its own channels after a restart does not scan logs, though: the contract exposes per-role enumeration views — `clientChannelNonce` / `clientChannels` for the funder side and `providerChannelCount` / `providerChannels` for the provider side — so the node reads its current channel set directly and re-hydrates each via `getChannel`, rather than replaying `ChannelOpened` from a block floor. The client-side count is the client's channel nonce itself (ids are recomputed per nonce, so there is no separate counter to drift); the provider side stores ids because a provider cannot reconstruct them.

**Signer-side enumeration is not topic-filterable.** Because `voucherSigner` sits in the event data rather than a topic, a delegated signer cannot ask an RPC node for "the channels that pinned me" — three indexed fields is the EVM ceiling, and `channelId`/`client`/`provider` are the three that earn their place. A signer learns its channels from the funder that delegated it (which knows the `channelId` before it even opens, since it owns the nonce sequence), or by decoding the data field of `ChannelOpened` logs it fetches on some other filter. Indexers that want a signer index build it at ingest.

`ChannelSettled` carries no `protocolFee` field — `settleChannel` does not skim a fee inline. The bucket distribution emits its own events from `FeeRouter` (see [FeeRouter Integration](#feerouter-integration)).

**Channel expiry:** `expiresAt` is set at channel open: `expiresAt = block.timestamp + maxChannelDuration`. The `maxChannelDuration` parameter defaults to 90 days and is governable within hardcoded bounds (minimum 7 days, maximum 365 days). Channel expiry protects clients from indefinitely locked funds when a node disappears without closing the channel.

**Channel close lifecycle:**

- `closeChannel` → requires status `Open`. **Callable by `channel.client` or `channel.provider` only** (`require(msg.sender == channel.client || msg.sender == channel.provider)`). Sets status to `Closing`, records `claimedAmount`, `claimedNonce`, and `claimedBytes` from the submitted voucher, emits `ChannelCloseInitiated`. No fund transfers. If a prior `withdraw` already advanced the watermark (`claimedNonce > 0`), the close voucher MUST be non-regressing (`nonce >= claimedNonce`, `amount >= claimedAmount`, `bytesDelivered >= claimedBytes`, `amount <= deposit`) — `nonce >=`, not the strict `nonce >` of `disputeChannel`/`withdraw`, so either party can always close at the current watermark voucher (`nonce == claimedNonce`) rather than being forced to wait for `expiresAt` when no newer voucher exists; settle then routes a zero remainder and a strictly-higher voucher still arrives via `disputeChannel`. Third parties cannot initiate a close — they act only via `disputeChannel` (during the dispute window) or `settleChannel` (after expiration). The pinned `voucherSigner` is likewise not a party to this path. **Voucher-less close:** when **either party** calls with `amount == 0`, `nonce == 0`, `bytesDelivered == 0` and an empty signature (`signature.length == 0`), the voucher signature is not verified and the channel closes at its recorded watermark; no watermark is advanced. Full mechanic, safety argument, and dispute symmetry: [Fee Routing on Disputed Closes](#fee-routing-on-disputed-closes).
- `closeChannelWithoutVoucher` → requires status `Open` and `block.timestamp < expiresAt`. **Callable by `channel.client` or `channel.provider` only.** A named alias for the voucher-less path above: it sets status to `Closing` at the channel's recorded `claimedAmount`/`claimedNonce`/`claimedBytes`, starts the dispute window, and emits `ChannelCloseInitiated` carrying those recorded values. No signature, no watermark advance, no fund transfers. It exists so a party that never received — or has lost — a voucher is not forced to wait out `expiresAt`, and so that intent is explicit at the call site rather than encoded as an all-zero argument tuple.
- `disputeChannel` → requires status `Closing` and `block.timestamp < disputeDeadline`. Callable by any address holding a valid voucher with a strictly higher nonce. Updates `claimedAmount`, `claimedNonce`, and `claimedBytes`, emits `ChannelDisputed`. No fund transfers. Unrestricted caller access is intentional: third-party fraud detectors ([Appendix: Fraud Detection](appendix-fraud-detection.md#appendix-permissionless-stale-close-detection)) must be able to submit higher-nonce vouchers on behalf of an offline party during the dispute window.
- `settleChannel` → requires status `Closing` and `block.timestamp >= disputeDeadline`. Callable by any address. Refunds `deposit - claimedAmount` to the client and, if the un-withdrawn remainder `claimedAmount - withdrawnAmount > 0`, transfers that remainder of the payment token to the configured `FeeRouter` and invokes `FeeRouter.routeSettlement(channel.provider, claimedBytes - withdrawnBytes, claimedAmount - withdrawnAmount)` in the same transaction (a channel whose claim was fully drawn via `withdraw` routes nothing here). Sets status to `Closed`, emits `ChannelSettled`. **No fee is computed or skimmed inside this contract** — the router applies the three-bucket split, pays the operator's 60% base share same-tx (alongside the 30%/10% legs), and increments `bytesPerEpoch[operator][epoch]` (epoch derived from `block.timestamp`) as the served-bytes accumulator consumed by `DecdnGovernor` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight); see [FeeRouter Integration](#feerouter-integration). Wash-trading defenses are layered on the per-byte accounting: real client USDC inflow is required to inflate either operator revenue or vote weight, since both derive from the same counter.
- `reclaimExpired` → requires status `Open` and `block.timestamp >= expiresAt`. Returns `deposit - withdrawnAmount` to the client — the full deposit when no withdrawal occurred, or the deposit minus what the provider already drew via `withdraw` (those funds left the contract and their bytes were already counted at withdraw time, so no `FeeRouter` call is made here). Sets status to `Closed`, emits `ChannelExpiredReclaimed`. Callable by the client or the provider. Regardless of caller, the refund goes to `channel.client` — the provider cannot claim further funds via this path. This ensures abandoned channels where the client is absent can be cleaned up by the provider to free on-chain state. Because the refund includes any **earned-but-un-withdrawn** voucher value (vouchers the provider holds off-chain but never submitted on-chain), a provider MUST `withdraw` or `closeChannel` before `expiresAt` to capture those funds. `withdraw` strictly *reduces* this forfeiture exposure — to whatever has accrued since the last withdrawal — but does not remove the pre-existing close-before-expiry obligation; an operator can minimize it by withdrawing frequently.

**Safety bounds (hardcoded):**

| Parameter | Minimum | Maximum |
| --- | --- | --- |
| Dispute window | 172800 seconds (48 hours) | 259200 seconds (3 days) |
| Rate floor | 1 base unit | `MAX_RATE_PER_MB` (10^12) |
| Max channel duration | 604800 seconds (7 days) | 31536000 seconds (365 days) |

`PaymentChannel` does not hold a fee-percentage parameter. Bucket-share bounds (60/30/10 with per-share bounds 40–90 / 5–50 / 0–30) are owned by `FeeRouter` per [ADR 026 § Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds).

**The rate floor is in USDC base units (6 decimals) per MB.** The contract stores `deliveryFloor`, the per-byte price floor **enforced at settlement** (see [Rate-floor enforcement](#rate-floor-enforcement) below). There is no governance ceiling: a seller self-clamping its own advertised rate downward buys no on-chain safety — a seller never wants to charge less — and the buyer's protection is seeing the signed rate in `StreamResponse` before it pays. The absolute upper bound is the wire constant `MAX_RATE_PER_MB` ([ADR 005](005-protocol.md#adr-005-wire-protocol)), which honest requesters reject above.

**Initial rate floor:**

| Parameter | Value (USD/MB) | USDC base units | Rationale |
| --- | --- | --- | --- |
| `deliveryFloor` | $0.000001/MB | 1 | Anti-abuse minimum; 10× below expected market rate. **Enforced at settlement** (#846) — the contract rejects any voucher whose cumulative `amount / bytesDelivered` falls below this floor, so claiming served bytes always costs proportional USDC. Prevents zero-rate free-riding and the served-byte vote-weight inflation of [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight), while imposing no practical constraint on legitimate pricing (nodes set rates well above it; the floor is an anti-zero safeguard, not a recommended price). |

The expected market rate is $0.00001/MB (10 USDC base units per MB, or $0.01/GB). This positions deCDN ~4–9× cheaper than major traditional CDNs (CloudFront at $0.085/GB, KeyCDN at $0.04/GB) and at parity with budget providers (Bunny.net at $0.01/GB). The floor is governance-tunable from day one within the hardcoded safety constraints above — admin-key-gated in the PoC, DecdnGovernor in production (see [ADR 009](009-governance.md#adr-009-governance-model)). Node pricing is otherwise a market outcome: nodes compete on the rate they advertise, and a node that overprices loses selection ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)).

### Rate-floor enforcement

`_advanceClaimWatermark` (the single voucher-validation chokepoint shared by `withdraw`, `closeChannel`, and `disputeChannel`) enforces the per-byte floor on the **cumulative** claim watermark:

```
require:  amount * BYTES_PER_MB >= bytesDelivered * deliveryFloor      (BYTES_PER_MB = 1_048_576, ADR 005)
```

evaluated overflow-safely as `bytesDelivered <= Math.mulDiv(amount, BYTES_PER_MB, deliveryFloor)` so a voucher carrying `bytesDelivered` near `type(uint256).max` reverts with `RateFloorViolation` rather than an arithmetic panic. Because `deliveryFloor >= 1` the divisor is non-zero, and `amount == 0` admits only `bytesDelivered == 0`, so the all-zero close arguments still clear the floor. The check uses **zero tolerance** — the floor sits 10× below the expected market rate, so honest traffic clears it by ≥10× and needs no rounding headroom (the off-chain 1% tolerance applies to the *advertised* `rate_per_mb`, not this floor).

This binds served bytes to real USDC: stamping `B` bytes requires cumulatively claiming `>= B / 1_048_576` base units, restoring the proportional-cost assumption [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) relies on (#846). Serving nodes mirror the same floor off-chain (a zero-tolerance `verify_rate` against `delivery_floor`) before countersigning, so an honest node never accepts a voucher the chain would reject. The floor is the only rate bound the protocol carries, and it is binding.

### Rate Bounds Refresh

Nodes must keep their local copy of `deliveryFloor` current so an advertised `rate_per_mb` never falls below it. Staleness is a revenue risk rather than a safety one — a node quoting under a raised floor signs vouchers the chain will reject at settlement — so the refresh strategy is lighter-touch than the content blacklist ([ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)), where serving blacklisted content is a slashable offense.

**Primary mechanism: event listening.** Nodes SHOULD subscribe to `RateBoundsUpdated` events on the `PaymentChannel` contract and update the local cache immediately. Governance actions are infrequent (days to weeks), so high-frequency polling would be wasteful.

**Fallback mechanism: periodic polling.** Nodes MUST poll `getRateBounds()` at a configurable interval (`rate_bounds_poll_interval`, default **1 hour**), guarding against missed events from RPC provider issues, WebSocket disconnections, or chain reorganizations. The 1-hour default is deliberately longer than the 10-minute registry ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)) / blacklist ([ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)) intervals: registry freshness is connectivity-critical and blacklist freshness slashing-critical, but a stale rate floor only risks the node quoting below it and signing unredeemable vouchers.

#### Startup

Nodes MUST call `getRateBounds()` before accepting connections, never operating without a floor (same pattern as the content blacklist initial sync, [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)). Because `getRateBounds()` returns `uint256` but the wire protocol represents `rate_per_mb` as `u64` ([ADR 005](005-protocol.md#adr-005-wire-protocol)), nodes MUST verify `deliveryFloor` fits within `u64` on every refresh (startup and subsequent polls/events). If it exceeds `u64::MAX`, the node MUST refuse to start (or, on a mid-operation refresh, continue with its last valid floor and log an error). Unreachable against a correctly-deployed contract — `setRateBounds` caps the floor at `MAX_RATE_PER_MB` (10^12), far below `u64::MAX` — but the check guards against a contract deployed without that cap.

#### Stale bounds

If the event subscription is lost and RPC polling fails, the node SHOULD continue operating with its last-known floor and log a warning. No service interruption is required. The worst-case consequence of a stale floor is that the node quotes below a raised floor and its vouchers are unredeemable at settlement — a revenue impact, not a safety violation.

#### No version-based delta pattern

Unlike the content blacklist (which uses `getBlacklistVersion()` for cheap change detection and incremental delta fetching), the rate floor is a single `uint256`. A version counter adds no value — the full state is readable in a single `eth_call` with negligible overhead. This is an intentional divergence from the [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting) pattern.

For how nodes validate `rate_per_mb` against the cached floor before signing protocol messages, see [ADR 005 — Rate Bounds Validation](005-protocol.md#rate-bounds-validation).

### BuybackBurner

| Function | Purpose |
| --- | --- |
| `executeBuyback(amount, minTokenOut)` | Governance multisig or `keeper`: swap `amount` of USDC for ≥ `minTokenOut` TOKEN and burn the proceeds. |
| `setKeeper(addr)` / `setSwapRouter(addr)` | Governance: rotate the authorized keeper or swap router. |
| `setSlippageTolerance(bps)` / `setMinBuybackAmount(n)` / `setMaxBuybackAmount(n)` | Governance: per-call execution guards. |
| `setEpochLiquidityCapFraction(bps)` | Governance: per-epoch USDC liquidity cap as a fraction of epoch-start pool depth (bounded `[1%, 30%]`, [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol)). |
| `setVault(addr)` / `setPool(addr)` | Governance: wire/rotate the Balancer V3 Vault (approval target) and pool. |
| `poke()` | Permissionless: advance the on-chain TWAP price accumulator that backs the `minOut` floor. |
| `keeper() → address` / `getAccumulatedFees() → uint256` | Views: current keeper and accumulated buyback inflow (USDC). |

This is the canonical `BuybackBurner` interface. [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split) defines the economic parameters and the 30% router-fed inflow source. [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol) specifies the venue (Balancer V3 Router + 80/20 weighted pool) and how `setSwapRouter` / `setPool` are configured at deployment. **V3 integration note:** `setSwapRouter` holds the Balancer V3 **Router** address, but `BuybackBurner` approves the Balancer V3 **Vault** address (a separate contract) — the Vault pulls input tokens from the `msg.sender` of the Router call. The concrete `BuybackBurnerBalancerV3` ([#686](https://github.com/decdn/decdn/issues/686)) issues a **scoped per-swap** `forceApprove(vault, amountIn)` reset to `0` after each swap (no standing allowance), rather than a max approval at initialization. See [ADR 018 — Buyback execution via Balancer V3](018-liquidity-strategy.md#buyback-execution-via-balancer-v3).

All `set*` functions are governance-only behind a timelock.

### FeeRouter Integration

Under [ADR 026](026-tokenomics.md#adr-026-tokenomics), `PaymentChannel.settleChannel` (and `withdraw`) does not split fees inline. The operator-bound payment-token balance is forwarded to a `FeeRouter` contract — in full at settlement when no withdrawal occurred, or as the routed delta on each `withdraw` plus the remainder at settle — which applies the canonical three-bucket split (60% operator base / 30% buyback-and-burn / 10% treasury — full table and bounds in [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split) and [§ Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds)). All three legs transfer in the same transaction as the routing call that fed them — each `withdraw` and the final `settleChannel`. This ADR specifies the `FeeRouter` interface only as it relates to the settlement path; the per-bucket details live in [ADR 026](026-tokenomics.md#adr-026-tokenomics).

#### Settlement-path interface

`PaymentChannel.settleChannel` — and `PaymentChannel.withdraw` for each mid-channel withdrawal — MUST invoke `FeeRouter.routeSettlement(address operator, uint256 bytesDelivered, uint256 amount)` in the same transaction as the payment-token `safeTransferFrom` to the router, passing the routed *delta* for that call (the un-withdrawn remainder at settle, or `amount - withdrawnAmount` at withdraw, where `withdrawnAmount` is its value **before** this withdrawal advances it). The router pays the operator's 60% base share in that transaction, dispatches the 30% / 10% same-tx legs, derives the current epoch as `uint64(block.timestamp / EPOCH_LENGTH)`, and increments `bytesPerEpoch[operator][epoch]` by the call's byte delta as the trailing-window served-bytes accumulator read by `DecdnGovernor._getVotes` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) as the governance vote-weight source. The router's `+=` accounting makes the per-call deltas sum to the channel's lifetime claim with no double-counting. The full `IFeeRouter` interface is canonical in [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model).

#### Settlement-path invariants

1. **Atomic base-share payout.** The 60% base share MUST land in the operator's wallet in the same transaction as each `withdraw` and as `settleChannel` — no claim step, no keeper, no off-chain queue. This is the Case A cashflow guarantee from [ADR 026 § Operator economics](026-tokenomics.md#operator-economics), realizable incrementally via `withdraw`.
2. **No reentry.** `withdraw` and `settleChannel` hold a `nonReentrant` guard for the duration of the router call.
3. **One final settlement per channel; routed deltas partition the claim.** The final `settleChannel` runs at most once, enforced by the `Closed` status; preceding `withdraw` calls each route a distinct, non-overlapping delta of the same lifetime claim, so the router sees no overlap and need only tolerate a duplicate *final* settle (idempotency or revert — pinned in [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model)).

Conservation and same-tx three-bucket invariants (60/30/10) live with the router itself in [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split) / [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model). The `Settled` event (operator + epoch + per-bucket deltas) is emitted by the router; full event set is in [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model).

#### Node-to-node settlements (no router bypass)

**Node-to-node cache-miss paid pulls route through `FeeRouter` identically to client-to-node settlements.** When node B pulls a blob from origin-backed node A and pays via a payment channel, that settlement is not special-cased: `settleChannel` routes it through the same `_route` → `FeeRouter` path as a client-to-node settlement, with no node-aware branch, and the settled amount takes the same three-bucket 60/30/10 split (60% to the operator base, 30% buyback-burn, 10% treasury) per [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split). There is no `settleChannelNoRoute` entry point and no detect-and-skip branch; the only routing conditionals (`settleAmount == 0` skip when the channel was already drained via `withdraw`, paused-router defer) are party-agnostic and still route the full amount and bytes when the deferred settlement flushes.

An earlier design considered a router bypass on the grounds that routing node-to-node settlements "double-charges" the same downstream bytes (once when B pays A, again when B's clients pay B). That bypass was **not adopted**: uniform routing keeps the contract surface minimal, matches the actual `PaymentChannel` implementation, and is exactly what makes the structural wash-trading deterrent hold — a self-routed channel pays the 40% non-base skim (30% burn + 10% treasury) on every cycle (see [ADR 036 § Wash-trading as vote-buying](036-served-bytes-voting-weight.md#wash-trading-as-vote-buying)).

- All `PaymentChannel` settlements forward to `FeeRouter.routeSettlement` regardless of whether the counterparties are operators or end clients. Node-to-node bytes therefore **do** accumulate in the router's per-epoch byte counters and **do** count toward governance vote weight ([ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) sources vote weight from `FeeRouter.bytesInWindow`), bounded by the per-operator vote cap and the [ADR 036 § Wash-trading as vote-buying](036-served-bytes-voting-weight.md#wash-trading-as-vote-buying) cost model.
- The on-chain registry distinction — a channel is node-to-node when both the `client` and `provider` addresses have a registered NodeId binding (see [NodeId-to-Ethereum Binding](#nodeid-to-ethereum-binding)) — still exists, but it drives **probe-acceptance priority** ([§ Admission and Priority](#admission-and-priority)), not routing. Settlement is routed the same way either way.
- Permissionless fraud detectors ([Appendix: Fraud Detection](appendix-fraud-detection.md#appendix-permissionless-stale-close-detection)) observe node-to-node settlements for self-routed-traffic / wash-trading patterns, feeding governance threshold-tuning — reinforcing, not replacing, the per-cycle skim cost.

#### Settlement sequence

End-to-end payment-token flow — client→node and node-to-node settlements both use the same `PaymentChannel → FeeRouter.routeSettlement` path — is diagrammed in [ADR 016 § USDC Flow (Payments)](016-contract-interactions.md#usdc-flow-payments). This ADR documents only the `PaymentChannel ↔ FeeRouter` interface contract.

The full three-bucket split applies to every network deployment from launch. Simplified launch configurations are expressed by setting non-active bucket shares to zero via `FeeRouter.setShares(...)` per [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics), not by deploying a reduced-surface stub. The cross-validation invariant in that section ensures any non-zero share has a wired non-zero destination, so the launch share configuration alone determines which downstream contracts must be ready at deploy time.

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

Per-operator-epoch attribution is derived by `FeeRouter.routeSettlement` at settlement time as `epoch = uint64(block.timestamp / EPOCH_LENGTH)`; the voucher itself does not carry an epoch. All cumulative bytes from the final voucher are credited to the epoch the settlement transaction lands in. The operator must call `closeChannel` before `expiresAt` (else the client may invoke `reclaimExpired` and the operator forfeits the claim); once the channel is in `Closing` status, `settleChannel` is callable at any time at or after `disputeDeadline` with no on-chain upper bound. `settleChannel` is permissionless but only the operator has an incentive to pay gas, since they receive the 60% base share. The practical bound on epoch-shifting is therefore `maxChannelDuration` (default 90 days, governance-tuned). The effective gaming surface — shifting attribution across roughly 4–12 weekly epochs within a monthly emission distribution — is a second-order effect on emission share and shrinks further as the active-operator set grows.

**Signature digest:**

```solidity
bytes32 digest = keccak256(abi.encodePacked(
    "\x19\x01",
    DOMAIN_SEPARATOR,
    keccak256(abi.encode(VOUCHER_TYPEHASH, channelId, amount, nonce, bytesDelivered, token))
));
```

**Verification:** Implementations must use OpenZeppelin's `SignatureChecker.isValidSignatureNow(channel.voucherSigner, digest, signature)` — the channel's pinned signer, which equals `channel.client` whenever no delegate was named at open. It transparently supports both EOA signers (via hardened `ECDSA.recover` that rejects non-canonical `s` values and restricts `v` to `27`/`28`) and smart account signers (via ERC-1271 `isValidSignature`). The signature is encoded as 65 bytes (`r || s || v`) for EOA signers; smart account signers may use longer signatures per their wallet implementation. See [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support) for the full account abstraction design.

The `DOMAIN_SEPARATOR` is computed once in the constructor and stored as an immutable. If the contract is deployed behind a proxy and may be migrated to a different chain, it should be cached in a state variable and recomputed only when `block.chainid` changes (the pattern used by OpenZeppelin's `EIP712` base contract), rather than on every call.

### Voucher Nonce Convention

Voucher nonces within a channel start at **1**. Nonce 0 is reserved as the sentinel value meaning "no voucher has been submitted" — it is the Solidity default for `claimedNonce` in a newly opened `Channel` struct. The first signed voucher in a channel uses `nonce=1`, the second uses `nonce=2`, and so on. This convention ensures:

- `claimedNonce == 0` reliably identifies channels where no voucher has ever been recorded on-chain, distinguishing "nothing claimed" from "claimed zero".
- Any real voucher (nonce ≥ 1) can always be used to dispute a close taken at a never-advanced watermark (`claimedNonce == 0`), since `disputeChannel` requires a strictly higher nonce (see [Fee Routing on Disputed Closes](#fee-routing-on-disputed-closes) and the channel close lifecycle in [PaymentChannel](#paymentchannel)).

### Node Registry

The on-chain registry of bonded nodes is part of the `CapacityBond` contract, not a separate contract. Bonding is a prerequisite for registration ([ADR 026 § Operator economics](026-tokenomics.md#operator-economics)), so co-locating them avoids cross-contract calls and simplifies the atomic bond-then-register flow.

> **No on-channel fee-discount path.** Operator return is differentiated through the `CapacityBond` lock-to-capacity curve ([ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)), not via a bond-multiple fee toggle on the channel contract. `getEffectiveFee`, `getBondMultiple`, `DISCOUNT_MULTIPLE`, `feePercentage`, and `discountedFeePercentage` are not part of the interface. `CapacityBond` carries the registration, bond-bookkeeping, and slashing responsibilities; the operator bond is `bond = k × Mbps^α` with defaults `k=12.6`, `α=1.2` (≈50K TOKEN at 1 Gbps) per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve).

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
function firstBondedAt(address operator) external view returns (uint64);

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
event NodeAutoEjected(bytes32 indexed nodeId, uint256 remainingBond);
event NodeIdReclaimed(bytes32 indexed nodeId, address indexed previousOwner);
```

`registerNode` emits both `NodeRegistered` and `NodeIdBound` ([§ On-Chain Registration](#on-chain-registration)) — the latter ensures off-chain indexers tracking the authoritative `nodeIdToAddress` mapping see initial registrations alongside rebindings.

#### Constraints

- **One-to-one mapping.** Each `nodeId` maps to exactly one `ethAddress` and vice versa. Enforced with `require(nodeByAddress[msg.sender].nodeId == bytes32(0))` and `require(nodes[nodeId].ethAddress == address(0))`, where `bytes32(0)` is the sentinel for "unregistered". This enforces a one-bond-position-per-node invariant.
- **`registerNode` rejects `nodeId == bytes32(0)`** (reserved as the unregistered sentinel). It binds `msg.sender` to `nodeId` — the caller's Ethereum address becomes `ethAddress`. This binding is on-chain and permanent until deregistration, distinct from the ephemeral per-session `NodeId`-to-address binding in [§ Off-Chain (Ephemeral) Binding for Clients](#off-chain-ephemeral-binding-for-clients). The function performs two signature verifications: (1) the `bindingSignature` parameter is an EIP-712 signature over `BindNodeId(nodeId, bindingNonce[msg.sender])` (see [§ Binding Message Format](#binding-message-format)); `registerNode` verifies this against the caller's current `bindingNonce`, then atomically writes the `nodeIdToAddress`/`addressToNodeId` mappings and increments `bindingNonce[msg.sender]`. (2) The `ed25519Signature` parameter proves ownership of the NodeId's ed25519 private key — see [§ NodeId Ownership Verification](#nodeid-ownership-verification) below. The shared per-address `bindingNonce` counter with `bindNodeId` ensures replay protection across both registration and rebinding. Every registered node is immediately slashable — there is no window in which a node is active in the mesh without a verifiable binding. The separate `CapacityBond.bindNodeId()` function in [§ On-Chain Registration](#on-chain-registration) remains available for rebinding (key rotation) after initial registration.
- **`deregisterNode` deactivates without touching the bond.** Sets `active = false`, removes the operator from the active set, increments `registrationNonce[nodeId]` to invalidate any previously issued ed25519 registration signatures for this NodeId, and clears `declaredMbps` (emitting `MbpsDeclared(operator, old, 0)`). It does **not** move the bond into unbonding — deactivation and bond exit are separate operations. The bond stays locked and fully slashable after deregistration (accountability is preserved), and an operator who changes their mind can re-register without re-funding — the retained bond clears `minBond` and, with the tier back at 0, the capacity curve as well; only the `declareMbps` call has to be repeated. Clearing the tier is what makes the full-exit sentence at the end of this bullet true: `requestUnbond`'s floor is `bond_required(declaredMbps)`, so an operator who left a tier standing would retain that much bond indefinitely. `deregisterNode` is the route to 0 for a *registered* node; an inactive operator (never registered, ejected, or displaced by `reclaimNodeId`) uses `declareMbps(0)`, which the contract accepts only from them ([ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)). To withdraw, the operator calls `requestUnbond(amount)` (which starts the 14-day unbonding window per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)) and then `unbond()` once it matures; the slash-then-run protection is the `MAX_EVIDENCE_AGE_US < unbondingPeriod` invariant ([ADR 014 § Interaction with unbonding period](014-on-chain-verification.md#interaction-with-unbonding-period)), which keys off the unbonding window rather than off deregistration, so it holds regardless. Separating the two lets an operator pause node duties (stop serving, leave the active set) without forcing a bond-return clock, while a full exit is just `deregisterNode` followed by `requestUnbond(activeBond)` + `unbond()`.
- **Auto-ejection.** When slashing drops a node's bond below 50% of the minimum bond for its declared tier ([ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn)), the contract sets `active = false` and emits `NodeAutoEjected`. The node must re-bond at the full tier minimum to rejoin.
- **`firstBondedAt` is write-once.** `registerNode` sets `firstBondedAt = block.timestamp` only if the stored value is 0 (first-ever registration for this address). On re-registration after deregistration or auto-ejection it retains its original value; it is never cleared by `deregisterNode` or auto-ejection. Used to anchor the operator's `age_ramp` governance weight ([ADR 026 § Governance](026-tokenomics.md#governance)).

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

EVM has no native ed25519 precompile, and no such precompile is deployed on the production L2 (see [Appendix: L2 Deployment](appendix-l2-deployment.md#appendix-production-l2-deployment-target) for chain and rollout status). Verification runs behind the `IEd25519Verifier` interface, implemented by a well-audited Solidity ed25519 library — the Smoo.th Crypto Lib EIP-6565 verifier — which performs the full RFC 8032 check (including in-EVM SHA-512, since no precompile is available) at strict-verification parity with `ed25519-dalek::verify_strict`, the check deCDN nodes run off-chain. A verifier more permissive than dalek would let an attacker bind a NodeId with a signature the network itself rejects, so parity is the security bar. This adds ~500k–1M gas to `registerNode`, a one-time cost per node lifetime — see [§ Gas Costs](#gas-costs) and the rationale in [ADR 014 § Slash Signatures — secp256k1 EIP-712](014-on-chain-verification.md#slash-signatures--secp256k1-eip-712) for why the secp256k1 `slash_sig` scheme used for routine slash evidence is not needed here.

##### Reclaim flow

If a NodeId was squatted (e.g., during a transition period or via a contract bug), the legitimate ed25519 key holder calls `reclaimNodeId(nodeId, ed25519Signature)`. This verifies the ed25519 signature over `keccak256(abi.encodePacked(nodeId, msg.sender, block.chainid, registrationNonce[nodeId]))`, deactivates the current holder's node if the reclaimed NodeId was its bound id (clearing the active flag and removing it from the active set — the bond is left locked and slashable, as with `deregisterNode`; the holder exits the bond separately via `requestUnbond` + `unbond()`. Unlike `deregisterNode`, reclaim does **not** clear the holder's `declaredMbps`; the displaced holder is now inactive, so they clear it with `declareMbps(0)` — see [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)), clears the NodeId↔address mappings (and zeroes the holder's now-stale `NodeInfo.nodeId` so the read views stay consistent with the cleared binding), increments `registrationNonce[nodeId]`, and emits `NodeIdReclaimed`. The caller can then call `registerNode` under their own address. Reclaim does not require the caller to have a bond — it only proves ed25519 key ownership and clears the squatter's binding. `reclaimNodeId` is the sole reclaim mechanism: there is no admin override. Reclaim authority is gated entirely by ed25519 wire-key ownership (the iroh NodeId private key), distinct from the secp256k1 on-chain signatures used for slash evidence ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)) and EIP-712 NodeId↔Ethereum binding ([§ NodeId-to-Ethereum Binding](#nodeid-to-ethereum-binding)).

## Admission and Priority

Node admission and queueing policy — how a node decides which requests to serve under congestion — is implementation-defined and lives outside the protocol. The wire format carries no priority bits, the channel and voucher mechanisms encode no per-stream priority state, and different operators are expected to tune their policy differently. There is deliberately **no probe-time congestion lever**. A probe answer states presence, not willingness to serve: a node advertises `has_blob: true` whenever it holds the blob and has not refused it, even when the `max_probe_holds` hold budget ([ADR 005 § Hold budget](005-protocol.md#hold-budget)) is exhausted — otherwise a probe flood that fills the hold cache could suppress truthful availability answers network-wide. Congestion is therefore expressed at **stream time**, via a signed `StreamResponse{ok: false, error: Overloaded}`. That is safe to sign freely: rate manipulation requires `ok == true` and blacklist violation requires a served claim ([ADR 014 § Evidence verification](014-on-chain-verification.md#evidence-verification-per-offense-type)), so a refusal is not slash evidence under any offense. Two signals inform an admission policy:

- **Committed voucher rate.** The advertised `rate_per_mb` in `ProbeResponse` / `StreamResponse` is a **floor**, not equality — nodes verify `amount_delta / bytes_delta >= rate_per_mb`. Clients MAY commit at higher rates; nodes MAY use the committed rate as a per-stream priority key for **ordering admitted, in-flight streams** — it is a mid-stream signal (the first `Voucher` arrives only after `StreamResponse{ok: true}`), not an admission key — with the premium paid directly via [`FeeRouter.routeSettlement`](#feerouter-integration).
- **Registered node-bond.** `CapacityBond.bondOf(address)` is readable on-chain, but only for requesters that are themselves registered operators — practically, node-to-node cache-miss probes. Nodes MAY prioritize *probe-acceptance* for addresses with `bondOf >= bond_required(declared_capacity)` ([ADR 026 § Operator economics](026-tokenomics.md#operator-economics)). A node bond is not a usable signal for end-client probes (clients are not registered); there, lane eligibility must come from reputation, region, or an allow-list.

## NodeId-to-Ethereum Binding

The protocol requires a verifiable mapping between iroh NodeIds (ed25519 public keys) and Ethereum addresses (secp256k1-derived). This binding is used for payment channel association and slash evidence attribution. Two orthogonal signature mechanisms protect this mapping: the EIP-712 `bindingSignature` (secp256k1) proves the Ethereum key holder consents to the association — preventing un-slashable registration; the `ed25519Signature` ([§ NodeId Ownership Verification](#nodeid-ownership-verification)) proves the NodeId's private key holder authorized the registration — preventing NodeId squatting.

### Binding Message Format

The binding uses EIP-712 typed structured data, signed by the Ethereum private key. Two typed structs share the per-address `nonce` counter: initial registration signs `RegisterNode`, which additionally binds the operator's acceptance of the current operator terms ([ADR 019 § Terms Acceptance](019-node-onboarding.md#operator-safety-obligations)); rebinding (key rotation) signs the narrower `BindNodeId`.

```solidity
bytes32 constant REGISTER_NODE_TYPEHASH = keccak256(
    "RegisterNode(bytes32 nodeId,uint64 nonce,bytes32 termsHash)"
);

bytes32 constant BIND_NODE_TYPEHASH = keccak256(
    "BindNodeId(bytes32 nodeId,uint64 nonce)"
);
```

Where:

- `nodeId`: the 32-byte ed25519 public key (iroh `NodeId`)
- `nonce`: a monotonic counter per Ethereum address, preventing replay of revoked bindings
- `termsHash`: the operator-terms hash the caller accepts, which must equal the governance-canonical `currentTermsHash` (registration only; see [ADR 019 § Terms Acceptance](019-node-onboarding.md#operator-safety-obligations))

The EIP-712 domain separator is the same as the `CapacityBond` contract deployment (chain ID + contract address), preventing cross-chain and cross-contract replay. Terms acceptance is enforced at registration only, so rotating a NodeId through `bindNodeId` neither carries nor re-checks `termsHash`.

### On-Chain Registration

Node registration and NodeId binding are atomic. `CapacityBond.registerNode()` ([§ Node Registry](#node-registry)) accepts a `termsHash` parameter, a `bindingSignature` parameter — an EIP-712 signature over `RegisterNode(nodeId, bindingNonce[msg.sender], termsHash)` — and an `ed25519Signature` parameter proving ownership of the NodeId's ed25519 private key (see [§ NodeId Ownership Verification](#nodeid-ownership-verification)). It requires `termsHash == currentTermsHash`, verifies both signatures, writes the `nodeIdToAddress`/`addressToNodeId` mappings, emits `TermsAccepted(nodeId, termsHash, timestamp)`, and increments `bindingNonce[msg.sender]` in the same transaction that adds the node to the mesh. The per-address nonce counter is shared with `bindNodeId`, giving replay protection across both paths; the distinct typehash keeps a registration signature from being replayed as a bare rebind. This eliminates the window in which a node could be active but not slashable.

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
// per [ADR 011 § Origin Assignment
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
// records a binding without mesh membership or bond.
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

Clients without on-chain registration MAY include a signed binding in their `StreamRequest` to attest a NodeId↔Ethereum-address mapping for the connection's lifetime. The node verifies the EIP-712 signature over `BindNodeId(nodeId, nonce=0)` by recovering the signer from the fixed 65-byte form and comparing it against the claimed address. Smart-account clients are rejected fail-closed: off-chain ERC-1271 verification is deferred to Production ([ADR 024](024-account-abstraction.md#off-chain-erc-1271-verification)). The verified address is cached for the connection's lifetime and acts as a gate, not as an attribution source: the node refuses a request naming a channel whose pinned `voucherSigner` ([§ PaymentChannel](#paymentchannel)) is not the bound address, before any bytes are delivered, since that channel's vouchers would fail verification anyway. The address a voucher must recover to is always the channel's on-chain `voucherSigner` pin. This ephemeral binding is not stored on-chain and is valid only for the session. Wire-format details are in [ADR 005](005-protocol.md#client-identity-binding).

### Binding Requirements by Role

| Role | On-chain binding required? | Rationale |
| --- | --- | --- |
| Node (bonded) | **Yes** — `registerNode` performs binding atomically via `bindingSignature` (EIP-712, proves Ethereum key consent) and `ed25519Signature` (proves NodeId ownership) | Slash evidence references on-chain NodeId→address mapping; atomic binding eliminates gap; ed25519 proof prevents NodeId squatting |
| Client (opening channels) | No — channel `client` field is the Ethereum address directly | Channel operations use Ethereum addresses, not NodeIds |

### Rebinding

A node or client can rebind their Ethereum address to a new NodeId by calling `bindNodeId` (the nonce increments, invalidating the old binding). The old NodeId→address mapping is deleted. This supports key rotation scenarios (e.g., compromised iroh key). Initial binding is handled atomically by `registerNode` and does not require a separate `bindNodeId` call.

## Decimal Handling

USDC uses 6 decimals; TOKEN uses 18 decimals. All payment amounts in the `incentive` crate use USDC base units (µUSDC). The voucher signing code uses raw base units — no decimal conversion in the signature path to avoid precision bugs.

**Voucher format:**

```
{channelId, amount, nonce, bytesDelivered, token, signature}
```

During delivery over `cdn/client/v1`, `{signature, amount, nonce}` are transmitted on the wire; the remaining fields (`channelId`, `token`, `bytesDelivered`) are derived from stream context — `channelId` from the `StreamRequest`, `token` fixed at channel open, and `bytesDelivered` the node's per-channel cumulative byte counter. The `nonce` is explicit to prevent desynchronization if a `VoucherAck` is dropped (it starts at 1 for the first voucher in a channel; 0 is reserved as a sentinel). See [ADR 005](005-protocol.md#adr-005-wire-protocol) for wire protocol details.

The `token` field (ERC-20 address) is in the signed EIP-712 typed data to prevent cross-token replay; it is the USDC contract address, fixed at deployment. Full EIP-712 type definition and domain separator: [EIP-712 Voucher Signature](#eip-712-voucher-signature).

### Voucher Bytes-Delivered Field

`bytesDelivered` is a cumulative byte count signed alongside `amount` and `nonce`. It is the canonical settlement-record byte count carried in the `Voucher`, forwarded to `FeeRouter.routeSettlement`, and aggregated into `bytesPerEpoch[operator][epoch]` (where `epoch` is derived from `block.timestamp` at settlement time) — the trailing-window served-bytes accumulator consumed by `DecdnGovernor._getVotes` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) as the governance vote-weight source. Properties:

- **Cumulative, monotonic.** Like `amount` and `nonce`, `bytesDelivered` is strictly non-decreasing across vouchers within a channel. `disputeChannel` MUST revert if the new voucher's `bytesDelivered < claimedBytes`.
- **Derivable from MB-denominated voucher cadence.** Voucher cadence is MB-denominated (default 1 MB; see [Voucher Interval Negotiation](#voucher-interval-negotiation)) and `rate_per_mb` is MB-denominated. Clients computing `amount` from `bytesDelivered` use `amount = ⌈bytesDelivered / 1_048_576⌉ × rate_per_mb` (1 MB = 1,048,576 bytes per [ADR 005](005-protocol.md#adr-005-wire-protocol)); equivalently, `bytesDelivered = mb_delivered × 1_048_576` when delivery boundaries align with MB intervals. The unit conversion is purely an off-chain arithmetic concern; the voucher carries the byte count directly so the contract does not need to re-derive it.
- **Carried through `closeChannel` / `disputeChannel` to `settleChannel`.** Recorded in `channel.claimedBytes` and forwarded as the `bytesDelivered` argument to `FeeRouter.routeSettlement` at settlement.
- **Cross-channel consistency.** A voucher signed for one channel is bound by its EIP-712 typed data; `bytesDelivered` is part of that signed payload and cannot be replayed against a different channel.

The router does not validate `bytesDelivered` against any oracle of physical delivery — the value is whatever the channel's voucher signer signed. The defense is twofold. **Structurally**, per-byte settlement revenue requires real client USDC inflow rather than self-attested byte counts (on-chain settlement is capped at `channel.deposit`, with `closeChannel` / `disputeChannel` reverting on `amount > deposit` per [§ Fee Routing on Disputed Closes](#fee-routing-on-disputed-closes)). **Quantitatively**, `_advanceClaimWatermark` enforces the `deliveryFloor` per-byte price floor (see [Rate-floor enforcement](#rate-floor-enforcement)), so a voucher cannot decouple a large `bytesDelivered` from a tiny `amount` — claiming `B` bytes costs `>= B / 1_048_576` base units regardless of the `channel.deposit` ceiling. Without the floor, the `amount <= deposit` cap alone is insufficient: an operator can deposit a small amount and still stamp arbitrarily many bytes at `amount = 1`. Governance vote weight is sourced from the same floor-bound per-byte counter ([ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)), so both wash-trade revenue and vote-buying are bound by proportional real USDC (#846).

## Slashing and Channel Interactions

Slashing and payment channels are independent by design.

**Slashing does not affect channel funds.** Slashing operates exclusively on TOKEN bond in the `CapacityBond` (schedule per [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn) — 5%/15%/50% escalation tiers; slashed bond is held in escrow-on-slash and distributed at finality 50% challenger / 50% burn). Channel funds are client deposits held in escrow — not bond, never touched by slashing. This follows from the functional separation in [Consequences](#consequences): payment channel contracts never hold or move TOKEN bond, cannot be called by `CapacityBond` to slash or reassign bond, and any `CapacityBond` interaction is read-only (e.g., resolving NodeId↔address bindings).

**Slashing can drop a node below its tier minimum bond while channels are open.** Channel deposits being independent of the bond, a node can be slashed below the tier minimum (or to zero) with open channels. The channels continue their normal lifecycle — close, dispute window, settle — regardless of bonding status; settlement is purely a function of voucher state, not registry status.

**Auto-ejection does not interrupt open channels.** When a node's bond drops below 50% of the minimum and auto-ejection triggers (see [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn)):

- Open channels settle normally. Client funds are never trapped.
- The ejected node cannot participate in new channels (clients verify node registration before opening channels, and nodes verify counterparty status before accepting a `StreamRequest`).
- The ejected node is removed from gossip routing, so it receives no new client connections.
- `withdraw` (provider only), `closeChannel` (client/provider only), `disputeChannel` (any address), and `settleChannel` (any address) remain callable on existing channels — these functions check channel state, not registry status, so a slashed or ejected operator can still redeem and settle revenue it already earned.
- The node must re-bond at the full tier minimum (`bond_required(declared_capacity)`) and re-register to resume operations.
