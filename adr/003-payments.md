# ADR 003: Payment Model

**Date:** 2026-03-28
**Status:** Draft

## Context

Nodes deliver bytes and need to be paid for it. The payment mechanism must work at per-MB granularity without an on-chain transaction per delivery, and must give delivering nodes immediate protection against non-payment.

Three constraints shape the design:

1. On-chain transactions cost far more than one MB of delivery. Settlement must amortize across a whole payment relationship, never one transaction per MB.
2. One payer serves many nodes, and one payer may fund many independent signers (per-device or per-user keys). A model that needs one channel per `(payer, node)` pair — with one voucher signer per channel — makes the channel count the product of payers and nodes. A payer with many signers across many nodes then faces an impractical number of on-chain opens and locked deposits.
3. Node operators have real infrastructure costs (VPS, bandwidth, backend storage). Revenue denominated in a volatile governance token creates unacceptable P&L risk: a 10× price drop turns a profitable operator into a loss.

## Decision

Payments use **off-chain vouchers backed by a shared on-chain pool, denominated in the payment token** — **USDC**, fixed at contract deployment (immutable constructor argument, 6 decimals). The text says "the payment token" for the pool-deposit / voucher / settlement currency and names USDC only where a USDC-specific property is load-bearing (decimals, Circle counterparty risk, on-chain identifiers, swap pairs, dollar-denominated constants).

One funded **pool** backs payments from **many independent capped signers** to **many nodes**. A pool is one deposit in the `PaymentPool` contract, opened once by its owner and reused. The pool replaces the per-pair channel: the owner opens a single pool and pays every node from it, and never opens a channel per node or per client.

The model uses two signed objects and an on-chain sharded register.

- **Capability** — signed off-chain by the pool owner over `{ signer, spending_cap, pool_id, expiry }`. It authorizes `signer` to spend up to `spending_cap` from `pool_id` until `expiry`. It is **node-agnostic**: one capability is valid at every node.
- **Voucher** — signed by the authorized `signer` over `{ pool_id, signer, provider, cumulative, bytes_delivered }`. It is **node-addressed**: it names the payee `provider`. `provider` is mandatory because `pool_id` does not encode the payee; without it the contract could not attribute a payment or keep independent per-node ordering, and one node could redeem a voucher meant for another.

The mechanism operates at two tiers, both on the same pool primitive:

- **Client → node**: a client opens a pool and issues capped capabilities to one or more signers (its own key, or per-device or per-session keys it delegates). Each signer streams cumulative vouchers to the nodes it fetches from, and a node redeems its own vouchers on-chain.
- **Node → node**: when a node pulls content from another node (typically an origin-backed node), it pays from its own pool via the same voucher mechanism. The origin-backed node is paid wholesale; the pulling node recoups this by serving multiple clients from its cache at a markup.

A node redeems a voucher on-chain at any time while the pool is open — redeeming a signed, monotone claim needs no dispute window (see [Redemption and Close](#redemption-and-close)). The owner closes the pool to reclaim the unspent remainder. A redemption grace window (default 48 hours, governable within 48h–72h — see [ADR 009](009-governance.md#adr-009-governance-model)) lets each node redeem outstanding vouchers before the owner reclaims. Reclaim returns the deposit minus the total redeemed across all lanes.

**Chain-agnostic.** The pool makes opens rare and moves them off the client's fetch path, so a pool open is not latency- or throughput-critical. The payment model therefore assumes no specific chain and does not require fast or cheap opens. The chain choice is a separate cost and neutrality decision (see [Appendix: L2 Deployment](appendix-l2-deployment.md#appendix-production-l2-deployment-target)).

Key parameters:

- Voucher granularity: 4 MiB delivered per voucher, a fixed constant every signer and node reads directly — no negotiation, no wire field
- Minimum deposit: none on-chain beyond non-zero. Deposit sizing is node-informed policy, not a contract floor (see [Deposit Economics](#deposit-economics))
- Fee routing: each redemption forwards the paid amount to `FeeRouter.routeSettlement(operator, bytesDelivered, amount)`, and the three-bucket split (60% operator base, 30% buyback, 10% treasury) is dispatched same-tx per [ADR 026](026-tokenomics.md#adr-026-tokenomics). See [FeeRouter Integration](#feerouter-integration).
- Operator return is differentiated through the `CapacityBond` lock-to-capacity curve per [ADR 026](026-tokenomics.md#adr-026-tokenomics), not via a fee-discount mechanic on the payment contract.

### Deposit Economics

A pool is opened once and reused. There is no per-node, per-fetch, or per-client open, and no pool close on the client's fetch path. The on-chain footprint of a pool is one open plus occasional top-ups, independent of how many nodes it pays or how many signers it delegates. A node registers each new signer's capability once on first redemption; every later redemption for that signer is voucher-only. On-chain cost is therefore independent of client count: many clients collapse to one pool plus lazy per-`(signer, provider)` register slots for active pairs only.

**Deposit sizing is node-informed policy, not a schedule.** The pool is oversubscribable — the sum of signer caps may exceed the deposit, which is what lets one signer draw heavily while others draw nothing. Nodes keep the pool solvent by reserving a refundable minimum-remaining-deposit `M` — they stop serving before the balance reaches `M`, so the reserve covers every in-flight voucher and refunds to the owner. The owner sizes the deposit to cover expected spend plus that reserve. `M` is node policy (see [Pool solvency and the refundable floor `M`](#pool-solvency-and-the-refundable-floor-m)); the contract enforces none of it and `openPool` accepts any non-zero deposit. A single user opens a small pool (for example $10), makes its own key the sole signer, caps it at the deposit, and tops up when low. A client that fans out to many signers opens one larger pool and issues each signer a small capped capability.

**Top-up.** The owner adds funds with `topUp` when the pool balance runs low, rather than opening a second pool. `topUp` spends only its own transaction. A buyer that runs its pool short mid-fetch tops up and resumes at the paid frontier, so no delivered byte is skipped or paid for twice. A network deposit minimum is unnecessary: service is bounded by what the deposit funds, and pool spam is bounded by gas — each `openPool` costs gas and locks real funds refundable only to the owner — while a floor would create a hard barrier for development and testing where small pools are useful.

#### Smart Account Support and Gasless Pool Opens

All deCDN contracts use OpenZeppelin `SignatureChecker` for signature verification, supporting both EOA (via `ecrecover`) and smart-account wallets (via ERC-1271 `isValidSignature`). Safe and other ERC-1271 smart accounts are supported wallet types for pool owners and node operators; the encrypted EOA keystore is the documented default — see [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support).

Because a pool is opened rarely and off the fetch path, gas-abstraction standards are optional conveniences rather than hot-path requirements:

- **ERC-2771 meta-transactions.** A relayer submits the `openPool` transaction on behalf of the owner, paying gas and recouping it from the deposit or a sponsorship fund. Requires a trusted-forwarder check on the contract.
- **ERC-4337 account abstraction.** A smart-contract wallet batches payment-token approval + pool open into one user operation, with a paymaster sponsoring gas in the payment token. Works with unmodified contracts.

Gas abstraction via ERC-2771 or ERC-4337 paymasters is targeted at production.

### Credit Window

This section is the **delivery** credit window — unbilled egress already on the wire. It is distinct from the refundable floor `M` a node reserves against a pool's on-chain drain ([Pool solvency and the refundable floor `M`](#pool-solvency-and-the-refundable-floor-m)). This one bounds bytes delivered ahead of the next voucher; `M` bounds how far a pool may be drawn down before nodes stop serving it.

The voucher interval is the *billing* granularity, not the *delivery* granularity. A delivering node streams within a **credit window**: it keeps sending chunks while the unpaid balance `delivered − paid` stays within the window and pauses a stream only when the next chunk would cross that bound — not at every interval boundary. Between one interval and the window, delivery and payment run concurrently. The node sends chunks ahead of the vouchers that pay for them; the payer issues a cumulative voucher at each interval and keeps receiving rather than waiting for the acknowledgement, so the acknowledgement is off the delivery critical path.

Decoupling the two rates is what keeps single-stream throughput link-bound rather than round-trip-bound. Collecting a voucher at every interval leaves the link idle for a full round trip — plus the node's durable-commit latency — once per interval, capping throughput at `interval / (RTT + service_time)` regardless of link capacity, and the repeated idle periods keep the transport's congestion window from reaching steady state. A window of several intervals keeps bytes in flight across the voucher round trip, so the link stays saturated.

**The window ramps with the stream's own payment.** The credit window for a stream is `min(credit_max, max(interval, paid / credit_ramp_divisor))`, where `paid` is that stream's own cumulative confirmed payment and `interval` is the voucher interval in force on the lane. A stream starts at the floor of one voucher interval and grows its window in proportion to `paid` as it pays, up to the `credit_max` ceiling. A stream that never pays stays pinned at the floor; nothing about the window depends on any other stream, signer, or the node's history with the counterparty.

**Exposure is one-sided and self-bounding.** The payer's exposure stays at zero: vouchers are cumulative over bytes already delivered, so the payer never signs for bytes it has not received. The node's exposure is the unbilled egress already on the wire, `delivered − paid`, and the ramp keeps that quantity at most `paid / credit_ramp_divisor` — a fixed fraction of the revenue the stream has already confirmed, never a fraction of some larger promise. With the default divisor of 2, a node never fronts more than half of what a stream has paid it so far. This is the same bounded-credit shape a node already fronts on the upstream leg of a cache-miss pull (the ramped credit window, [ADR 037](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality)), and the downstream credit is strictly the cheaper of the two: egress it has already served, versus speculative USDC it pays an upstream provider. On the fused cache-miss serve path a single window bounds both quantities at once, since every pulled chunk is forwarded downstream immediately.

**Node-local policy, floored at one interval.** `credit_max` and `credit_ramp_divisor` are node configuration, not wire or governance parameters — like the voucher interval they have no on-chain counterpart and are never negotiated. The window is floored at one voucher interval so a stream can always make progress (deliver a full interval, then recoup it), and a window at or below one interval reproduces the stop-and-wait cadence exactly. The self-enforcing threshold generalizes from "pause after one unvouchered interval" to "pause once the unpaid balance reaches the credit window"; the sovereignty guarantee is unchanged, since a node can always choose a smaller `credit_max` (down to one interval) and pause sooner.

**Takedown latency.** The per-boundary in-flight takedown check ([ADR 011 § On Blacklist Event](011-content-takedown.md#on-blacklist-event)) runs after each collected voucher, so widening delivery to a credit window widens the window in which a takedown that lands mid-stream is first observed: the first check falls up to one credit window into the stream (steady state, roughly one interval, as later intervals recoup one at a time). This is bounded by `credit_max` and floored at one interval — a stream still ramping up has not yet reached `credit_max`, so its exposure is smaller still — and even a full window of further egress is negligible against the takedown compliance window, so it does not weaken the takedown guarantee. This is why a node with a strict compliance target configures a smaller `credit_max` rather than a larger one.

**Defaults.** `credit_max` defaults to 64 MiB and `credit_ramp_divisor` to 2. The voucher interval is a fixed 4 MiB constant, which is also the window's floor. The ceiling and the interval move independently because they bound different things: `credit_max` bounds how far the ramp may widen credit exposure and how far delivery may run ahead of payment, while the interval bounds per-voucher overhead and the per-lane voucher-processing rate. Per-lane vouchers serialize behind one durable commit each (a few milliseconds of fsync), so the 4 MiB interval keeps that per-lane throughput ceiling well clear of the fsync cost, without raising exposure. At these values a fully-ramped stream stays link-bound past ~100 ms round trips, where a 1 MiB stop-and-wait interval would cap it in the low tens of MiB/s.

**Durability is preserved.** The credit window does not weaken the replay guard of [Off-chain voucher state persistence](#off-chain-voucher-state-persistence): each accepted voucher's watermark is durably committed before the node delivers any further bytes for it, so after a restart voucher acceptance resumes from the persisted state. Delivering ahead of payment within the window is bounded *credit* risk (the window's worth of unbilled egress), not *replay* risk — the bytes streamed ahead are billed by later vouchers, each committed when it arrives. There is no per-voucher acknowledgement on the wire; the node's continued delivery is the implicit acknowledgement, and it may amortize the per-voucher commit across a batch (one fsync for several vouchers) while still never delivering past a voucher before it is durable.

### Pool solvency and the refundable floor `M`

The contract enforces the per-signer cap, but the cap gives **isolation, not solvency**. The cap stops any one signer from over-drawing; it does not stop the *sum* of signers from exceeding the deposit. The flexibility the pool exists for — one signer draws heavily while others draw nothing — over-provisions the pool (`Σ caps > deposit`), so it can be drawn dry and the last outstanding vouchers eat the shortfall (the **tail**). Solvency is therefore a **node serving policy**, not a contract invariant. The contract is unchanged by any of this — it still just holds the deposit and pays `min(desired, remaining)`.

**The core fact — nodes serve partly blind.** A voucher is only certainly backed once redeemed. Before that a node cannot see other nodes' outstanding vouchers against the same pool, so it cannot know the pool is over-committed.

**The mechanism — a refundable minimum remaining deposit `M`.** A node stops serving a pool once its on-chain **remaining balance reaches `M`**. The reserved `M` then covers every outstanding in-flight voucher, so **no node is ever stiffed — the tail is structurally zero** — and whatever `M` is not needed **refunds to the owner** at close. `M` is *locked but returned*, not spent, so it can be sized generously at only a temporary-lock cost, never a real loss. That is strictly better than sizing the deposit large to *dilute* the tail (where the tail is a genuine loss you merely make a small fraction of): with `M` the tail is zero and the deposit stays withdrawable — the owner always recovers `≥ M − in-flight`.

**Node logic collapses to three lines:**

1. serve while remaining balance `> M`;
2. batch-redeem lanes when their unredeemed value crosses a redemption threshold `t`;
3. stop serving at remaining balance `≤ M`.

The threshold `t` is a **gas knob**, not a security parameter: a larger `t` means fewer, fatter redemptions. It is trust-graduated for gas efficiency — a trusted, never-draining pool redeems lazily (large `t`), a fresh pool eagerly (small `t`) — but trust is now purely a gas concern. **`M` is the security parameter.**

**Sizing.** `M` covers the worst-case total in-flight, `M ≥ N · t` (fan-out `N` × per-lane threshold `t`). Equivalently `M = k · ρ · B · Δ`, where `ρ` is the price per bandwidth-time (`$0.00125` per Gbps·s at the `$0.01/GB` market rate), `B` is the maximum aggregate bandwidth the pool is defended against (fan-out × per-node throughput), `Δ` is the **detection delay** (how fast nodes notice the balance crossing `M` and stop — chain-dependent; see [Appendix: L2 Deployment](appendix-l2-deployment.md#appendix-production-l2-deployment-target)), and `k ≈ 2` is a safety factor. The per-lane threshold is `t = ρ · V · Δ` for a lane serving at bandwidth `V`.

**`M` kills both the tail and node-vs-node racing.** Racing existed only because a *draining* pool had insufficient funds for everyone; with `M ≥` in-flight there is always enough to pay every outstanding voucher, so no node eats a shortfall and there is nothing to race for. One refundable reserve resolves both, which is why it replaces the whole dynamic-window apparatus.

**`M` is node policy, not a contract field.** The contract holds the deposit and pays `min(desired, remaining)` regardless; nodes enforce `M` by watching the on-chain remaining balance (`deposit − totalRedeemed`) and refusing to serve past it. A pool MAY declare its expected fan-out as an off-chain hint so the nodes serving it size `M` consistently.

**No per-relationship rate ramp — trust never gates delivery rate.** The security bound is the reserved floor `M`, not a per-relationship earned window. A node does **not** throttle a pool based on its history with that pool. A first-time pool and an established pool follow the same rules. Each stream ramps its own [credit window](#credit-window) from the floor of one voucher interval as that stream pays. The ramp resets on every new stream; it never carries over from the pool's or signer's past. A fresh stream and an established stream both start at the floor and ramp identically. Trust affects only redemption *cadence* (`t`, a gas concern), never delivery rate. The only caveat is fan-out *width*, not per-stream rate: a fresh pool spread across very many nodes at once can reopen the bounded tail below. We accept this residual; it is never a reason to slow any single stream. This removes the cold-start penalty of a dynamic per-relationship window, which throttled every fresh `(node, pool)` pair until it earned history.

**Residuals (honest):**

- **Fan-out.** `M` gives a zero tail only up to fan-out `N = M/t`; a pool fanned wider than the `M` it reserves reopens a bounded tail. We accept this residual rather than build machinery against it: each node's exposure to a fresh pool is only its own small redemption threshold `t`, and enlarging the aggregate tail forces the attacker to fan across proportionally many nodes while physically pulling real bytes from each (real egress cost), with self-dealing separately taxed by the FeeRouter cut. Sizing `M` for the expected fan-out closes it in the common case — honest clients are sticky and do not fan a brand-new pool across hundreds of nodes at once — so it is not driven to zero, and does not need to be. The same residual recurs at a single node: `paid / credit_ramp_divisor` bounds only the ramped regime, so a stream pinned at the floor costs one voucher interval no matter how the divisor is set, and the deposit guard checks the pool's remaining balance, not the number of streams already drawing on it. Aggregate floor exposure against one pool therefore scales with the count of concurrent floor-pinned streams, not with `credit_ramp_divisor`. The node accepts this fan-out residual the same way it accepts the one above, and does not restore a distribution-independent node-wide byte cap.
- **Self-dealing.** A client that also runs a node can serve itself below `M` and recover the deposit rather than spend it on honest service. Structurally unpreventable (no delivery oracle), but **taxed** by the FeeRouter cut — the 30% + 10% non-base legs are unrecoverable, so size the deposit against the recoverable fraction (`D ≥ V_max / cut ≈ 2.5 · V_max`). Escrow must be un-yankable — reclaim only after the close grace window — so the deposit cannot be pulled ahead of a redemption.

### Concurrent Streams

When multiple streams from the **same signer to the same node** run at once, they share that lane's **single cumulative voucher counter** (per `(pool_id, signer, provider)`). Streams to different nodes, or from different signers, are independent lanes. The rules within one lane:

1. **Aggregate byte counter.** The signer tracks total bytes received across all streams in the lane. A voucher is due whenever the aggregate crosses the next interval boundary.
2. **Voucher routing.** Vouchers are sent on any active stream in the lane — the node credits them against the lane-wide counter regardless of which stream carries the message.

**Example:** A signer has stream A (blob X) and stream B (blob Y) on the same lane. After receiving 4 MiB total across both (for example 3 MiB from A and 1 MiB from B), the signer sends a cumulative voucher.

See [ADR 005 — Payment lanes and concurrent streams](005-protocol.md#payment-lanes-and-concurrent-streams) for wire-level details.

### Redemption and Close

> **Fee routing model.** A redemption does not skim a fee inline; it forwards the paid amount to `FeeRouter.routeSettlement(operator, bytesDelivered, amount)` in the same transaction. Split details: [FeeRouter Integration](#feerouter-integration).

**Redemption is the settlement primitive.** A node redeems a voucher on-chain whenever it chooses, while the pool is `Open` or in the grace window. Redemption is provider-only (`msg.sender == voucher.provider`) and final — it needs no dispute window, because a voucher is a signed, cumulative, monotone claim by a capped signer and the node redeems only its own lane. `redeem(poolId, signer, provider, cumulative, bytesDelivered, voucherSig, capability)`:

1. **Register the signer once.** On the first redemption for `(poolId, signer)`, verify the owner's signature on `capability` and store `authorized[poolId][signer] = {cap, expiry, spent: 0}`. Every later redemption for that signer omits `capability` and rides the stored registration; a node reads `authorized[...]` by `eth_call` to confirm a signer and its cap without ever holding the capability. The owner-signature check happens once per signer, not per voucher.
2. **Check expiry.** Reject if `block.timestamp >= authorized[poolId][signer].expiry`. This is the only time bound that gates settlement, and it is safe because the node holds `expiry` in advance and stops serving before it (see [Revocation](#revocation)).
3. **Check payee.** Require `provider == msg.sender`. Redemption is cumulative — the voucher carries no nonce.
4. **Compute the payable amount.** With `w = watermark[poolId][signer][provider]`: `desired = cumulative − w.amount` (the still-unpaid portion this voucher authorizes), `capRoom = cap − spent`, and `remaining = deposit − totalRedeemed`. Then `paid = min(desired, capRoom, remaining)`. The floor check ([Rate-floor enforcement](#rate-floor-enforcement)) applies to `cumulative` / `bytesDelivered` here.
5. **Pay, or revert if nothing is payable.** If `paid == 0`, revert `NothingToRedeem` — the call writes **no** state and moves no funds, so the node simply retries later (after a top-up, or with a higher voucher). Otherwise: `bytesDelta = bytesDelivered > w.bytesDelivered ? bytesDelivered − w.bytesDelivered : 0` (clamped at zero); `bytesPaid = mulDiv(bytesDelta, paid, desired)` (the **paid-proportional** byte count — `== bytesDelta` when `paid == desired`); advance `w.amount += paid`, `w.bytesDelivered += bytesPaid`, `spent += paid`, `totalRedeemed += paid`; `safeTransfer` `paid` USDC to the `FeeRouter`; then call `FeeRouter.routeSettlement(provider, bytesPaid, paid)`. The `bytesDelta` clamp settles the money owed even when a voucher's `bytesDelivered` has not advanced past the lane's paid bytes: the signer already signed the higher cumulative `amount`, so the money is owed and settles, while the byte credit for that redemption is zero and the byte watermark holds — it recovers on a later voucher whose `bytesDelivered` advances again. This keeps the money axis (payment) independent of the served-bytes axis (vote weight) and denies any DoS from a bytes-regressed voucher.

**Partial redemption is retry-safe.** The lane watermark tracks the cumulative amount **actually paid**, not the voucher's claimed cumulative. When a drained pool covers only part of `desired`, `w.amount` advances by just `paid`, so re-presenting the *same* voucher after the owner tops up collects the remainder — `desired = cumulative − w.amount` is still positive. A node never loses an over-committed voucher to a transient drain; it loses value only if the owner closes and reclaims without ever topping up, bounded by the reserve `M` the node keeps against the pool ([Pool solvency and the refundable floor `M`](#pool-solvency-and-the-refundable-floor-m)). Because payment is `cumulative − paid`, replay is automatic: an already-paid or stale (lower-cumulative) voucher computes `paid == 0` and reverts, moving nothing. `min(desired, capRoom, remaining)` is the solvency backstop — the pool never goes negative, and no signer can cause more than `cap` USDC to leave the pool, since `spent` is the cumulative USDC actually paid on its behalf and `paid` is capped by `cap − spent`.

**The sharded register.** Two mappings, both written lazily on first touch:

- `watermark[poolId][signer][provider] = {amount, bytesDelivered}` — the cumulative USDC and bytes **paid** on that `(signer, node)` lane. Monotone and per-lane: independent accounting, automatic replay-safety (pay = claimed − paid), and `(payer, payee)` attribution. Each node redeems only its own lane, only vouchers naming it.
- `authorized[poolId][signer] = {cap, expiry, spent}` — the signer's registration and running **paid** total across all nodes, so the per-signer cap is enforced in aggregate without iterating lanes.

**Batch redemption.** `redeemMany(capabilities[], vouchers[])` registers signers and redeems lanes across many pools in one transaction. It takes two independent arrays: `capabilities` (each a `{poolId, signer, spendingCap, expiry, ownerSig}` registration) and `vouchers` (each a `{poolId, signer, provider, cumulative, bytesDelivered, voucherSig}` claim, applied with the single-`redeem` arithmetic above). The call registers every capability first — verifying the owner signature and storing `authorized[poolId][signer]`, idempotent for an already-registered signer — then redeems every voucher. Separating registration from the voucher decouples the two: a signer is registered by its own capability entry regardless of whether any voucher for it pays, so a skipped voucher never loses a registration. A voucher that would pay `0` — drained pool, already-paid or stale voucher, expired capability, cap reached, or a signer neither registered nor covered by this call's `capabilities` — is **skipped, not reverted**, so one empty lane never sinks the batch; the call emits one `PoolRedeemed` per paid voucher and returns the total paid. Only a structurally-invalid entry — a bad voucher signature, `provider != msg.sender`, or a bad capability owner-signature — reverts the whole call, since that is caller error, not transient pool state.

**Close and reclaim.** Redemption pays nodes; close returns the owner's unspent remainder. There is no adversarial close: the owner submits no vouchers on any node's behalf, so a close can never understate a node's earnings, and no third-party dispute path is needed. `closePool(poolId)` (owner only) sets status `Closing` and starts the grace window (`disputeDeadline = block.timestamp + disputeWindow`, default 48 hours). Nodes may still `redeem` during `Closing`. After `disputeDeadline`, `reclaim(poolId)` transfers `deposit - totalRedeemed` to the owner and sets status `Closed`. A node that has not redeemed by the deadline forfeits its outstanding vouchers. A pool does not expire, so a node's only deadlines are the grace window (after an owner close) and each signer's capability `expiry`; a diligent node redeems within its capability-bounded serving window, so this is the node's own cash-flow choice, not a theft surface.

**Served-bytes and vote weight are unaffected.** Each redemption forwards its paid-proportional byte count to `FeeRouter.routeSettlement`, which increments `bytesPerEpoch[operator][epoch]` — the trailing-window served-bytes accumulator read by `DecdnGovernor._getVotes` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight). Redemptions still fire the same routing call, and counting only paid bytes keeps per-byte revenue and governance vote weight both derived from real, floor-priced client USDC even in the drained-pool case. Faking bytes does not increase revenue (the operator base is per-byte, paid by the pool), and over-declared capacity does not translate into governance influence.

### Revocation

The contract cannot tell a voucher for *already-rendered* service from one for *future* service — a signed voucher is a signed voucher, and there is no delivery oracle. So any mechanism that refuses a validly-signed voucher risks stealing service a node already delivered. There is therefore **no redemption-gating epoch**: gating settlement on an epoch the owner can bump would let the owner refuse payment for vouchers already served — theft of rendered service. Revocation uses two safe mechanisms only:

- **Expiry (the settlement gate).** Safe because the node holds it in advance: a diligent node stops serving before `expiry` (with margin), so it never holds an un-redeemable voucher and controls its own exposure. This is the only time bound the contract enforces at redemption.
- **Stop-serving signal (not a settlement gate).** Revoking a signer tells nodes to refuse *future* service to that key; already-earned vouchers stay redeemable. It is enforced by node refusal, not by voiding claims. It is not instantaneous — nodes must learn of it — but the blast radius is already bounded by the pool balance and the per-signer cap, and a short-TTL `expiry` plus non-renewal retires a compromised key on its own.

The governance token (TOKEN) is not used for delivery payments. It is reserved for operator capacity bonding (see [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)) and governance (see [ADR 009](009-governance.md#adr-009-governance-model)).

**Rate setting is entirely up to each node.** Nodes advertise their `rate_per_mb` in probe responses and stream responses; the requester sees the rate before committing a voucher. The governance-set delivery-rate floor is the only rate bound the protocol enforces. There is no governance ceiling. A requester that finds a rate too expensive refuses the response as local policy ([ADR 005 § Requester-side validation](005-protocol.md#requester-side-validation-optional)), and the wire constant `MAX_RATE_PER_MB` bounds the field from above. This creates a market with natural arbitrage dynamics:

- Origin-backed nodes set a higher rate because they bear backend costs (storage + egress from their hidden backing store). They are the effective price ceiling for any blob they hold.
- A cache-only node that pays an origin-backed node to pull a blob can then serve that blob to many clients at a markup, recouping the origin cost across multiple deliveries.
- A node in a region where no peer has the content yet can charge a premium for that first delivery. Once it has the blob, other nearby nodes can pull from it at a competitive rate and compete for local clients.
- Nodes with cheaper bandwidth or better hardware can sustainably undercut others; nodes in high-demand regions can charge more and still win on latency.

The network self-balances with no central coordinator: profitable content gets replicated, competition drives prices down in well-served regions, and unpopular content stays at origin-backed node rates until demand justifies caching it.

**Origin backend economics:** The backing store choice directly affects an origin-backed node's viable rate. At the expected $0.01/GB market rate, an S3-backed node paying $0.09/GB egress loses money on every cache miss and must amortize origin pulls across a high cache-hit ratio (or price above market). Zero-egress backends — Cloudflare R2 ($0.00/GB), Backblaze B2 ($0.00/GB via Bandwidth Alliance partners), Wasabi ($0.00/GB) — keep origin-backed nodes profitable at or near market rates. High-egress backends imply higher `rate_per_mb`, which the market tolerates for content not yet cached elsewhere.

### Off-chain Voucher Rejections (Wire Encoding)

When a node rejects a voucher off-chain — before any gas would be spent — the rejection is returned **in-band** mid-stream as a `StreamError` message carrying `VoucherRejected { reason }` (per [ADR 005 § Stream Lifecycle State Machine](005-protocol.md#stream-lifecycle-state-machine), this transitions the stream `Streaming → Failed` cleanly without a QUIC stream reset). Voucher validation can only fire after at least one `Voucher`, necessarily after `StreamResponse { ok: true }` — so payment rejections never use the initial-response error path that delivery-side failures (`NotFound`, `Overloaded`, etc.) take. Full reason enum and per-reason retry semantics: [ADR 005 § VoucherRejected semantics](005-protocol.md#voucherrejected-semantics).

Some reasons map to an on-chain `redeem` revert the node avoids by rejecting early; others are the node's own off-chain ordering guards that on-chain would simply pay `0` (redemption is cumulative). `RetryLater` is the off-chain-only signal for a transient persist-write failure (see [Off-chain voucher state persistence](#off-chain-voucher-state-persistence)):

| `VoucherRejectReason` | Off-chain trigger | On-chain behaviour |
|---|---|---|
| `BadSignature` | Malformed signature bytes | EIP-712 `SignatureChecker` would revert at `redeem` (see [EIP-712 Voucher Signature](#eip-712-voucher-signature)) |
| `WrongSigner` | Signature recovers to an address other than the voucher's `signer` | `redeem` would revert when the recovered signer ≠ the authorized `signer` for `poolId` |
| `WrongPool` | `voucher.pool_id` mismatch | EIP-712 domain binds the voucher to a specific `poolId`; off-pool vouchers authorize nothing |
| `WrongProvider` | `voucher.provider` names another node | `redeem` requires `provider == msg.sender`; a node cannot redeem another node's lane (see [Redemption and Close](#redemption-and-close)) |
| `AmountRegression` | `voucher.amount ≤` the node's accepted cumulative | none on-chain (`redeem` pays `cumulative − paid`, so a stale voucher pays `0`) — the sole off-chain ordering/replay guard now that vouchers carry no nonce; keeps the node's own ledger consistent |
| `BytesRegression` | `voucher.bytes_delivered <` the node's accepted cumulative bytes | as for `AmountRegression`, on the byte axis |
| `CapExceeded` | the voucher would push the signer's paid total past `cap`, or the capability has expired | `redeem` caps `paid` at `cap − spent` and gates on `expiry` from `authorized[poolId][signer]` — a fully-uncoverable voucher reverts `NothingToRedeem` |
| `RetryLater` | Transient persist-write failure (`PoolError::Store`) | none — node-side store fault, not a voucher defect; the client resends the **same** voucher unchanged on a fresh stream |

Surfacing these reasons off-chain saves both parties the gas of a doomed on-chain submission and gives the payer enough detail to recover (refresh state and re-sign for `AmountRegression`, ask the owner to top up or raise the cap for `CapExceeded`) instead of an opaque connection drop. A delegated signer — one issued a capped capability by a pool owner — holds no funds to `topUp` (owner-only) and cannot read its lane watermark from chain until a redemption records it, so the node attaches an authenticated watermark bundle to the regression rejections for self-heal, and defers a genuine top-up or cap raise to the owner (see [ADR 005 § `VoucherRejected` semantics](005-protocol.md#voucherrejected-semantics)). Riding in-band rather than via a QUIC stream reset preserves the reason for client retry logic without burning [ADR 013](013-schema-evolution.md#adr-013-schema-evolution) application-error-code numbers for the structured-response case.

## Consequences

### Positive

- One pool is opened once and reused. On-chain cost is one open plus occasional top-ups, independent of how many nodes it pays and how many signers it delegates — many clients collapse to one pool plus lazy per-`(signer, provider)` slots. A node serves on the first byte with no open round-trip on the fetch path.
- USDC denomination gives node operators predictable unit economics: delivery revenue covers infrastructure costs without exposure to TOKEN price movements
- The voucher is the payment receipt; the BLAKE3 hash is the delivery receipt. Together they provide mutual protection: the signer doesn't sign a voucher for bytes that fail hash verification; the node stops delivering if vouchers stop arriving
- Maximum risk per voucher interval (4 MiB) is $0.00004 at market rate ($0.00001/MB) — negligible. At the ceiling rate ($0.001/MB), worst-case risk is $0.004 per interval — small against the reserve `M` a node keeps against a pool
- One fungible deposit backs every node, versus one earmarked deposit per node — capital-efficient, and wallet-less signers (ephemeral keys the owner delegates a capped capability to) never touch the chain
- Market-driven rate setting means replication happens organically: profitable content gets cached by more nodes, driving prices down without any coordination protocol
- The `PaymentPool` contract is functionally separated from the `CapacityBond`, keeping the audit surface for each contract's core logic bounded

### Negative

- Solvency is not a contract invariant. The per-signer cap gives isolation, not solvency; keeping `Σ spending ≤ deposit` on an oversubscribed pool relies on nodes reserving the refundable floor `M` (see [Pool solvency and the refundable floor `M`](#pool-solvency-and-the-refundable-floor-m)) — honored by self-interest (a node serving past `M` risks stiffing itself), but node behavior, not a contract guarantee.
- The tail is structurally zero for bounded fan-out: the reserved `M` covers all in-flight vouchers. It reopens only past fan-out `N = M/t` — a bounded residual we accept (each node risks only its threshold `t`, and enlarging the tail costs the attacker real bandwidth); self-dealing is separately bounded by `M` and taxed by the FeeRouter cut.
- Revocation is future-only. Settlement of rendered service can never be voided, so revocation is a short-TTL `expiry` plus a node-side stop-serving signal, not an on-chain switch that invalidates earned vouchers (see [Revocation](#revocation)).
- Close and reclaim finalize per `(signer, provider)` lane over the grace window rather than on one cumulative number — a sharding of the existing regular close.
- Clients must hold the payment token and the chain's native gas currency to use the network; this adds an onboarding step compared to a single-currency model. Gas abstraction is deferred to production (see [Deposit Economics](#deposit-economics)).
- Rate volatility: a node can change its advertised rate between a probe and a stream request; the `StreamResponse` rate is the binding one, but a client that probed at one rate and receives a higher rate in `StreamResponse` must disconnect and re-probe rather than having been deceived silently. Rate changes more than 30 seconds after the probe are not slashable; the 30-second window is precisely defined as `stream_response.timestamp_us >= probe_response.timestamp_us && stream_response.timestamp_us - probe_response.timestamp_us < 30_000_000` using requester-anchored timestamps in both signed messages (see [ADR 005](005-protocol.md#adr-005-wire-protocol))
- USDC is issued by Circle, which can freeze specific addresses or blacklist the contract. This counterparty risk is accepted: the payment token is fixed to USDC at deployment and the protocol does not implement payment-token substitution

## Attack Vectors

### Client-side

#### Voucher withholding

Client receives bytes but stops signing vouchers, getting content for free up to the last signed interval.

The self-enforcing stop is sufficient. Maximum loss is one voucher interval (4 MiB): at market rate (~$0.00004) it is negligible, and at the ceiling rate (~$0.004) it is still negligible relative to pool deposits. Nodes serving high-value content can pause after less than a full interval by configuring a smaller credit window.

The stop is per-lane and mechanical. The node pauses the moment the unpaid balance on a lane reaches that lane's ramped [credit window](#credit-window) — `min(credit_max, max(interval, paid / credit_ramp_divisor))` computed from the lane's own paid total — and waits for the next higher-cumulative voucher on the same lane; it never fronts more than one window against one lane. The pause keys on the lane, not the counterparty. A client that stalls or crashes mid-stream and reconnects is treated no differently from a first request: it resumes on the same lane under the same ramped window and pays only for the bytes it vouchers, never for the interrupted window. The node's own cost of the interruption — re-reading a held blob, or dropping and later re-pulling an unfinished cache-miss fill — is bounded to that one window either way, and no black mark follows the client into its next request. No history accumulates against a client across connections, and none needs to: a fresh signer from the same party starts its own lane at the one-interval floor and ramps up only as that lane itself pays, so it is bounded the same way. The bound is a property of each lane and its own paid total, so a new key buys an attacker nothing it did not already have — a fresh signer has zero `paid` and therefore starts back at the floor, not at whatever window an old signer had earned.

That bound applies to a *funded* lane. A request whose pool cannot cover even the first credit window is refused before the node signs a success `StreamResponse`, so it is never served the free interval at all — the seller-side pre-flight deposit guard of [ADR 037 § Implementation status](037-regional-proxy-warming.md#implementation-status), which fronts both the cache-miss and the direct-serve paths.

#### Owner reclaims before a node redeems

There is no stale-close vector: the owner submits no vouchers on any node's behalf, so a close cannot understate a node's earnings. The only residual is timing — the owner closes the pool and reclaims the remainder before a node redeems its outstanding vouchers.

The grace window covers this if the node is online: `closePool` starts a window (default 48 hours, sized above the force-inclusion delay; see [L2 sequencer censorship](#l2-sequencer-censorship) below) during which any node may still `redeem`, and only after the window does `reclaim` return the remainder to the owner. **Node defenses:**

1. **In-process redemption monitor.** A lightweight thread inside the node binary watches the chain for `PoolCloseInitiated` events on pools it holds vouchers against and redeems its highest voucher per lane before the window closes. Zero-latency to the local voucher store; handles the common case where the node is online. A SHOULD for production node binaries.
2. **Expiry margin.** A node stops serving a signer before the capability's `expiry`, so it always holds redeemable vouchers with time to redeem — the same discipline that makes expiry-based revocation safe (see [Revocation](#revocation)).

A node offline for the full grace window forfeits its unredeemed vouchers; this is a node-operations responsibility, not a protocol gap.

#### Probe fishing

Client sends probe requests to many nodes at high frequency to map the network or exhaust node resources without ever paying.

Per-NodeId rate limiting alone is bypassable: clients are not bonded, NodeIds are free to rotate, and iroh connection setup is cheap. The mitigation is the layered token-bucket rate limit in [ADR 005 § Probe rate limiting](005-protocol.md#probe-rate-limiting): per-peer (NodeId) plus per-IP plus a global node cap, applied before any signature or hold-slot allocation. The per-IP layer raises the cost of bulk probing because IP rotation requires money (proxies, IPv6 delegation, cloud bills) while NodeId rotation does not; the global cap is defence in depth.

**Note:** Probe responses are considered public information (see [ADR 005](005-protocol.md#adr-005-wire-protocol)). The concern here is resource exhaustion from bulk probing, not information leakage — content availability is discoverable via probing (see [ADR 005](005-protocol.md#adr-005-wire-protocol)), and pricing is revealed in probe/stream responses by design.

#### Pool oversubscription (one deposit backs many nodes)

One deposit deliberately backs vouchers to many nodes — that is the point of the pool. So the owner (or its signers) can commit more voucher value than the deposit covers, and a node that serves against an over-committed, drained pool is paid less than its voucher.

This is bounded, not a double-spend hole:

- **The contract never overpays.** `redeem` pays `min(desired, capRoom, remaining)`, so the pool never goes negative; the sum of all payouts never exceeds the deposit. A drained pool pays partially and the remainder stays claimable after a top-up, so the node is not forced to forfeit a voucher to a transient drain.
- **The per-signer cap isolates signers.** No signer can commit past its `cap`, so one compromised or greedy signer cannot drain the whole pool.
- **The node bounds its own exposure.** A node stops serving a pool once its remaining balance reaches the reserved floor `M`, which covers all outstanding in-flight vouchers, so the tail is zero (see [Pool solvency and the refundable floor `M`](#pool-solvency-and-the-refundable-floor-m)).
- **Self-dealing is taxed, not free.** An owner that redeems to its own node still pays the FeeRouter cut on every cycle, and the deposit is un-yankable until the grace window or `expiry`, so it cannot be pulled ahead of a redemption.

### Node-side

#### Data withholding

Node accepts a stream request, receives a voucher, then stops delivering bytes.

Self-enforcing: the node cannot extract more payment than the last acknowledged voucher. The client resumes from `byte_offset` on a different node.

#### Corrupted delivery

Node serves bytes that don't match the advertised BLAKE3 hash.

Absorbed at the wire by progressive BLAKE3 verification at the client (mandatory in `cdn/client/v1` per [ADR 002](002-content-addressing.md#adr-002-content-addressing) and [ADR 005](005-protocol.md#adr-005-wire-protocol)). Vouchers are signed and sent only after the corresponding chunks have been verified — a corrupt window therefore yields no voucher. The client drops the connection, requests the blob from a different node, and recovers any unspent channel funds via channel-close. **Client monetary loss in the corruption case is zero**; the only cost is downstream bandwidth (sunk regardless of outcome).

No on-chain slash machinery is needed for content corruption. The threat is bounded in framing parallel to [§ Voucher withholding](#voucher-withholding) above: per-encounter wasted bandwidth is capped at one voucher interval on each side (the client's downstream cost for a corrupt window; the node's upstream cost when a correctly-withheld voucher leaves the window unpaid). Both bounds are per-lane and mechanical — the same self-enforcing credit-window pause applies, keyed to the lane rather than the counterparty. Clients prefer nodes whose probe and delivery history they trust; the protocol coordinates neither side.

#### Rate bait-and-switch

Node advertises a low rate in probe responses then returns a higher rate in `StreamResponse`.

**Resolved: slashable offense.** Both responses are signed over the advertised rate ([ADR 005](005-protocol.md#adr-005-wire-protocol)); a same-NodeId signed pair where `StreamResponse.rate_per_mb > ProbeResponse.rate_per_mb` and the requester-anchored timestamp delta is under 30 seconds is on-chain-verifiable evidence. Clock-skew immune (both timestamps originate from the requester's clock; the node echoes them back in its signed response). The slash schedule lives in [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn); see [ADR 014 § Slash Signatures — secp256k1 EIP-712](014-on-chain-verification.md#slash-signatures--secp256k1-eip-712) for the on-chain verifier.

#### Redemption front-running

A node monitors the mempool and front-runs an owner's close with its own redemption.

Not an attack. A node redeeming its latest voucher before the owner reclaims is the intended happy path — redemption is provider-only and pays only up to the voucher the signer signed. Fabricating a higher voucher requires forging the signer's signature, which is cryptographically infeasible, and the per-signer cap bounds the total either way.

#### Third-party forced close (DoS)

A third party tries to force a pool from `Open` to `Closing` to disrupt it, or redeems a lane it does not own.

**Resolved by access control.** `closePool` is owner-only, so a third party cannot start the grace window. `redeem` requires `provider == msg.sender`, so a node can only redeem vouchers naming it — a third party cannot redeem another node's lane even holding the voucher. On-path voucher interception is mitigated by QUIC transport (TLS 1.3); it does not address endpoint compromise.

#### Redemption while the pool is Open (no dispute window)

A node redeems accrued funds while the pool is still `Open`. This is the ordinary settlement path, not an attack.

**Safe by construction.**

- **No dispute window is needed because there is nothing to dispute.** `redeem` pays a **signed** cumulative voucher from a capped signer, verified against the authorized `signer` for the pool. The node can never draw more than the signer committed, never past the signer's `cap`, and never past the pool balance. A stale lower voucher simply pays `0` (it is already covered by the lane's paid cumulative). No counterparty submits a competing number, so there is nothing an offline party would need a window to counter.
- **Residual: the signing key is the blast radius, capped.** A compromised signer key can authorize claims up to its `spending_cap`, redeemable with no window to intervene. The cap and the pool balance bound the loss; a short-TTL `expiry` plus non-renewal retires the key (see [Revocation](#revocation)). Delegating capped, expiring capabilities confines the loss to one signer's cap rather than the owner's whole balance.
- **No regression or double-spend.** The lane watermark tracks cumulative **paid** amount and only increases, as do `spent` / `totalRedeemed`. Redemption pays `cumulative − paid`, so re-submitting a voucher pays only what is still owed and an already-paid voucher pays `0`; `FeeRouter.routeSettlement` is purely additive, so each paid byte and USDC unit is counted exactly once across a lane's redemptions.
- **Owner-refund safety.** The owner reclaim is always `deposit − totalRedeemed ≥ 0`. Redemptions never trap owner funds or pay out more than the deposit.
- **Governance-weight timing.** Bytes are stamped into `bytesPerEpoch` in the epoch each redemption lands. A node's choice of *when* to redeem shifts epoch attribution slightly, but this is no stronger than the settle-timing flexibility operators already have, the total served-bytes count is unchanged, and the count still reflects only real client-paid bytes — so it introduces no new wash-trading or vote-weight vector ([ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight); [ADR 016 § wash-trading](016-contract-interactions.md#tunable-economics)).

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
- **Option C — Reputation as a second filter.** New nodes (low reputation, few settled pools) are deprioritised in client selection even if their `rate_per_mb × rtt_ms × (1 / max(reputation, 0.1)²)` score is competitive. A sybil fleet takes time to build reputation, limiting its effectiveness during that window.

#### Rate manipulation cartel

Colluding nodes in a region hold rates artificially high.

Origin-backed nodes set the effective price ceiling for any blob. Clients can always probe origin-backed nodes directly and pay their rates as a guaranteed fallback. Any node outside the cartel that undercuts wins all local traffic — the incentive to defect is strong. New entrants can join the cache-only role permissionlessly by bonding; the origin role for content in registered namespaces requires `OriginAssignment` membership ([ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)), but cache-only competition is sufficient to discipline the rate cartel because cache delivery is interchangeable with origin delivery from the requester's perspective.

#### Content withholding

A node bonds, responds to probes with `has_blob: true`, but refuses to serve — collecting credibility in the peer table without actually participating.

**Withholding is not a slashable offense** — operators may legitimately take content offline for maintenance, migration, or business reasons, and slashing for availability creates perverse incentives. The protocol does not guarantee availability; publishers who want fault tolerance opt into it by seating multiple operators, and the network deprioritizes flaky nodes through reputation:

- **Publisher-chosen operator sets.** Content owners hold a publisher identity ([ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces)). Governance vets the publisher wallet once via the standard timelock path, and the vetted publisher then seats origin operators per namespace itself ([ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)). Set size is the publisher's call — a single trusted operator works for hobbyist publishers, multi-operator sets defuse single-point withholding for publishers who want it. Content served under namespace 0 has no authorized origins — it is served best-effort from cache/DHT only ([ADR 002 § Namespace 0](002-content-addressing.md#namespace-0)).
- **Reputation fast-path.** Nodes that respond `has_blob: true` to probes but fail to deliver accumulate reputation penalties at a steeper rate. A node with consistently poor availability is deprioritized in provider selection and loses delivery revenue. Publishers may use the reputation signal as input when seating or unseating operators for their namespace.

Note: the probe-triggered eviction hold ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)) addresses a related but distinct problem. Withholding is a node that has the blob but refuses to serve it (behavioral — handled by reputation). The eviction hold addresses a node that advertised `has_blob: true` but lost the blob to cache pressure before the stream request (mechanical — kept resident by the hold so the follow-up pull succeeds).

#### Replay attack on vouchers

Attacker intercepts a signed voucher and attempts to replay it against a different pool, a different node, or after redemption.

EIP-712 typed data over `{poolId, signer, provider, amount, bytesDelivered}` binds the voucher to a specific pool, signer, and payee. `provider` is what stops one node from redeeming a voucher meant for another. The EIP-712 domain separator (see [EIP-712 Voucher Signature](#eip-712-voucher-signature)) further binds each voucher to a specific chain and contract deployment, preventing replay across different chains, contract upgrades, or test vs production environments. The contract settles in a single token fixed at deployment, so a voucher can only ever pay in that token — cross-token replay does not arise, and the voucher carries no token field. Resubmission after redemption is neutralized by the cumulative accounting: `redeem` pays `cumulative − paid`, so an already-redeemed voucher pays `0` and moves no funds.

#### Off-chain voucher state persistence

The on-chain protections in [Replay attack on vouchers](#replay-attack-on-vouchers) constrain only what the contract accepts at redemption. They do not prevent the **delivering node** from re-delivering bytes off-chain for a voucher it already honoured: a node holding voucher state only in memory will, after restart, re-accept any earlier (lower-cumulative) voucher the signer (or any wire observer) resubmits and serve the bytes again.

Required invariant: a node MUST persist `(last_amount, last_bytes_delivered)` **per `(poolId, signer, provider)` lane** and durably commit (fsync, on disk-backed implementations) **before** delivering any further bytes for that voucher. After a restart, voucher acceptance MUST resume from the persisted state — never from `last_amount = 0`. An absent entry is semantically identical to a never-seen lane (`last_amount == 0`); a record exists iff the node ever accepted a voucher on the lane. Entries are dropped only when the node has redeemed the lane's full cumulative and observed it on-chain.

A failed persist write MUST surface as a voucher-acceptance failure — the node returns a transient-failure rejection through the [Off-chain Voucher Rejections (Wire Encoding)](#off-chain-voucher-rejections-wire-encoding) channel, and MUST NOT deliver any further bytes for that voucher. The wire code for transient persistence failures is `VoucherRejectReason::RetryLater`, carrying no on-chain-invariant meaning; the `AmountRegression` / `CapExceeded` codes are NOT appropriate substitutes because they would tell the signer to refresh state or ask for more headroom when in fact the same voucher should be retried unchanged. The node finishes the stream cleanly after the rejection (no QUIC reset), so the signer resends the same voucher on a fresh stream rather than treating an opaque connection drop as a permanent failure. Persisting after delivering re-opens the same replay window for the crash interval between the two writes.

Storage backend and trait shape are implementation concerns; the Rust implementation exposes a `PoolStateStore` seam in `crates/incentive` with a `redb`-backed persistent implementation in `crates/node`. The protocol fixes only the ordering above.

## Contract Interfaces

### PaymentPool

The `PaymentPool` contract holds payment-token pools. The payment token is USDC; its address is fixed at deployment as an immutable constructor argument. A pool is one funded deposit that backs vouchers from many capped signers to many nodes. A two-dimensional sharded register records per-signer authorization and per-`(signer, provider)` redemption watermarks.

**Pool state:**

```solidity
struct Pool {
    address owner;            // funder: deposits, receives the reclaim, owns the poolNonce sequence
    uint64  openedAt;
    uint8   status;           // 0 = Open, 1 = Closing (grace window active), 2 = Closed
    address token;            // USDC; set once at deployment (immutable)
    uint64  disputeDeadline;  // set when close is initiated; fixed for the grace window
    uint256 deposit;          // total escrowed, USDC base units (6 decimals)
    uint256 totalRedeemed;    // cumulative USDC paid out across all lanes; remaining = deposit − totalRedeemed
}

struct Authorization {        // authorized[poolId][signer]
    uint256 cap;              // per-signer spending cap, from the owner-signed capability
    uint64  expiry;           // capability expiry; gates redemption
    uint256 spent;            // cumulative USDC actually paid on this signer's behalf across all providers
}

struct Lane {                 // watermark[poolId][signer][provider]
    uint256 amount;          // cumulative USDC actually paid on this lane (monotone)
    uint256 bytesDelivered;  // cumulative bytes actually paid on this lane (monotone)
}

// Set once on first redemption for the signer (owner signature verified there).
mapping(bytes32 => mapping(address => Authorization)) public authorized;
// Per (pool, signer, node) redemption lane.
mapping(bytes32 => mapping(address => mapping(address => Lane))) public watermark;
```

Both register mappings are written lazily on first touch, so an inactive `(signer, provider)` pair costs no storage. The pool header carries no per-payee or per-signer field — those live in the register. "The lane watermark" refers to `watermark[poolId][signer][provider]`, whose `.amount` and `.bytesDelivered` are the cumulative USDC and bytes **paid** on the lane and advance monotonically. Vouchers carry no nonce — on-chain redemption is purely cumulative (see [Voucher ordering](#voucher-ordering)).

**Roles.**

- **`owner` — the funder.** Transfers the deposit in, receives the `deposit − totalRedeemed` reclaim, owns the `poolNonce` sequence the `poolId` derives from, is the only address that may `topUp` and `closePool`, signs capabilities, and is the address the [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting) takedown and blacklist gates evaluate.
- **`signer` — a delegated voucher authority.** Authorized by an owner-signed capability up to `cap` until `expiry`. Node-agnostic: one capability spends at every node. The owner may be its own sole signer (the single-user case) or delegate many capped signers.
- **`provider` — a delivering node.** The only address that may `redeem` a given voucher (`provider == msg.sender`), the recipient of every routed payout, and the payee named in the voucher.

There is no pinned per-channel `voucherSigner`. Signers are authorized off-chain by capability and registered lazily on first redemption. Because a capability carries a `spending_cap` and an `expiry`, a compromised or delegated signer is bounded by its cap and retired by its expiry — the revocation an immutable pin could not give (see [Revocation](#revocation)).

**Pool ID:** `poolId = keccak256(abi.encodePacked(owner, poolNonce))` where `poolNonce` is a monotone per-owner counter stored as `ownerPoolNonce[msg.sender]`. Neither `provider` nor `signer` is an input — a pool is bound to no payee and no signer. **Ordering:** `openPool` reads the current nonce, computes `poolId`, then increments. The owner pre-computes the next `poolId` off-chain by reading `ownerPoolNonce[owner]` before opening.

> **Terminology:** `poolNonce` (the pool creation counter) is distinct from the voucher `nonce` (the monotone per-lane sequence number in EIP-712 voucher signatures). The former identifies pools; the latter orders vouchers within a `(signer, provider)` lane.

| Group | Function | Purpose |
| --- | --- | --- |
| Nonce | `ownerPoolNonce(owner) → uint256` | Per-owner monotone counter used in `poolId` derivation. |
| Lifecycle | `openPool(deposit) → poolId` | Open a pool; derives `poolId` from the current `ownerPoolNonce[msg.sender]` then increments it; escrows `deposit`; emits `PoolOpened`. Names no provider and no signer. |
| Lifecycle | `topUp(poolId, additionalDeposit)` | Owner-only: add funds to an open pool. |
| Lifecycle | `redeem(poolId, signer, provider, cumulative, bytesDelivered, voucherSig, capability)` | Provider-only (`provider == msg.sender`): register the signer on first use (verify the owner `capability`), then pay `min(desired, capRoom, remaining)` of a cumulative voucher — the still-unpaid portion, capped by the signer's remaining cap and the pool balance; routes the paid amount through `FeeRouter` same-tx. Reverts `NothingToRedeem` if nothing is payable (writes no state, so it is retried). No dispute window. See [Redemption and Close](#redemption-and-close). |
| Lifecycle | `redeemMany(capabilities[], vouchers[])` | Provider-only: register each capability in `capabilities` (idempotent — an already-registered signer is a no-op), then redeem each voucher in `vouchers`. The two arrays are independent: a node registers the signers it needs and redeems all its lanes in one transaction, and a skipped voucher never affects a registration. **Skip** (do not revert) any voucher that would pay `0` — drained pool, stale/already-paid voucher, expired capability, cap reached, or a signer neither registered nor present in `capabilities`. Reverts only on a bad voucher signature, `provider != msg.sender`, or a bad capability owner-signature. Returns the total paid. |
| Lifecycle | `closePool(poolId)` | Owner-only: start the grace window so nodes may redeem outstanding vouchers before reclaim; sets status `Closing`; emits `PoolCloseInitiated`. |
| Lifecycle | `reclaim(poolId)` | After the grace window: transfer `deposit − totalRedeemed` to the owner; sets status `Closed`; emits `PoolReclaimed`. Callable by anyone; the refund always goes to `owner`. |
| View | `getPool(poolId) → Pool` | Read the on-chain `Pool` struct. |
| View | `getAuthorization(poolId, signer) → Authorization` | Read a signer's `{cap, expiry, spent}`, so a later node confirms a signer and its cap without holding the capability. |
| View | `getRateBounds() → floor` | Current `deliveryFloor` in payment-token base units. |
| View | `feeRouter() → address` | Configured `FeeRouter` target ([ADR 026](026-tokenomics.md#adr-026-tokenomics)). |
| Governance | `setFeeRouter(addr)` | Replace router target. `GOVERNANCE_ROLE`-gated; routed through the standard 48h `TimelockController` delay; emits `FeeRouterUpdated(address oldRouter, address newRouter)`. See [§ Governance setter: setFeeRouter](#governance-setter-setfeerouter) below. |
| Governance | `setDisputeWindow(seconds)` | Grace window (bounded 172800–259200 — 48h–72h). |
| Governance | `setRateBounds(floor)` | Per-MB delivery-rate floor in payment-token base units. Capped at `MAX_RATE_PER_MB`. |

Bucket shares (60/30/10) are governed on `FeeRouter`, not on `PaymentPool`; the treasury share (10%) is configured on `FeeRouter`.

#### Governance setter: setFeeRouter

```solidity
function setFeeRouter(address newRouter) external onlyRole(GOVERNANCE_ROLE);

event FeeRouterUpdated(address indexed oldRouter, address indexed newRouter);
```

`setFeeRouter` re-points the configured `FeeRouter` for future `redeem` and `reclaim` calls. Required because the audited contract surface is fixed at deploy time, yet the `FeeRouter` may need replacing (bug fix, structural upgrade) without redeploying `PaymentPool` and forcing every open pool to re-issue vouchers.

**Authority and timelock.** Only callable by `GOVERNANCE_ROLE` (held by the `TimelockController` post-deploy per [ADR 016 § Post-Deployment Initialization](016-contract-interactions.md#post-deployment-initialization)). `DecdnGovernor` proposals to replace the router execute through the standard 48h timelock per [ADR 009](009-governance.md#adr-009-governance-model). Calls outside that path revert.

**Validation.** Reverts on `address(0)`, on the same address as the current `feeRouter`, and on a `newRouter` whose code size is zero (EOA / undeployed address) — the same `code.length` invariant the constructor enforces, because `_route` would otherwise advance pool state while `routeSettlement` silently no-ops, desyncing accounting and stranding paid USDC. Beyond that code-size check the new router is not further interrogated — the deeper cross-validation invariants in [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics) live on `FeeRouter` itself, so re-pointing at a wrong-but-deployed contract still surfaces at the next `redeem` rather than at the setter.

**Open pools are unaffected.** Vouchers signed against this `PaymentPool` remain valid because the EIP-712 domain separator hashes the contract's own address, not the configured `FeeRouter`. Carve-out documented in [ADR 016 § No proxy deployment patterns](016-contract-interactions.md#no-proxy-deployment-patterns): helper-contract addresses are not domain-separator inputs and may be re-pointed via governance without invalidating signatures.

**Routing during the swap.** Redemptions beginning before the timelock executes use the previous router; those beginning after use the new one. `redeem` reads `feeRouter()` at call time, and `routeSettlement` is a single transaction, so no in-flight redemption splits across routers.

> **Reentrancy protection:** All state-mutating functions that perform external calls (ERC-20 transfers) — `openPool`, `topUp`, `redeem`, `reclaim` — MUST use `nonReentrant` guards and follow checks-effects-interactions. `redeem` additionally crosses the `FeeRouter` boundary, so the effects (lane watermark, `spent`, and `totalRedeemed` advances) MUST be committed before the `safeTransfer` + `routeSettlement` interaction.

**`topUp` behavior:** `topUp(poolId, additionalDeposit)` adds funds to an open pool:

- **Status precondition:** MUST require status `Open` (reverts on `Closing` or `Closed`).
- **Caller:** owner only (`require(msg.sender == pool.owner)`).
- **Effects:** Transfers `additionalDeposit` from `msg.sender` to the contract via `safeTransferFrom`. Updates `pool.deposit += additionalDeposit`.
- **Modifiers:** `nonReentrant`.
- **Emits:** `PoolToppedUp(poolId, additionalDeposit, newDeposit)`.

**`redeem` behavior:** `redeem(poolId, signer, provider, cumulative, bytesDelivered, voucherSig, capability)` pays a node against a monotone voucher **while the pool is `Open` or in the grace window**. It is safe without a dispute window because a voucher is a signed, cumulative claim by a capped signer and a node redeems only its own lane. The residual is the signing key itself — a compromised key can authorize up to its `spending_cap`, retired by the capability's `expiry` (see [Redemption while the pool is Open](#redemption-while-the-pool-is-open-no-dispute-window)).

- **Status precondition:** status `Open` or `Closing` with `block.timestamp < disputeDeadline` (a node may redeem during the grace window). Reverts on `Closed`.
- **Caller:** the payee (`require(provider == msg.sender)`).
- **Register the signer once.** If `authorized[poolId][signer]` is unset, verify the owner's signature on `capability` (an EIP-712 `Capability` over `{signer, spending_cap, poolId, expiry}` recovered against `pool.owner`; see [EIP-712 Voucher Signature](#eip-712-voucher-signature)), then store `{cap: spending_cap, expiry, spent: 0}`. Later redemptions for that signer omit `capability`.
- **Voucher validation.** Verify `voucherSig` against `signer` (see [EIP-712 Voucher Signature](#eip-712-voucher-signature)); require `block.timestamp < authorized[poolId][signer].expiry` and the [rate floor](#rate-floor-enforcement) on `cumulative` / `bytesDelivered`. Redemption is cumulative; the voucher carries no nonce.
- **Effects (checks-effects-interactions):** let `w = watermark[poolId][signer][provider]`; compute `desired = cumulative − w.amount`, `capRoom = cap − spent`, and `paid = min(desired, capRoom, deposit − totalRedeemed)`; if `paid == 0`, revert `NothingToRedeem` (no state written). Otherwise compute `bytesDelta = bytesDelivered − w.bytesDelivered` and `bytesPaid = mulDiv(bytesDelta, paid, desired)`; set `w.amount += paid`, `w.bytesDelivered += bytesPaid`, `spent += paid`, `totalRedeemed += paid`; then `safeTransfer(feeRouter, paid)` and call `FeeRouter.routeSettlement(provider, bytesPaid, paid)` in the same transaction. `routeSettlement` is purely additive (`+=`); `bytesPaid` counts only paid bytes (`== bytesDelta` when the pool is solvent), so served bytes never outrun paid USDC and each paid byte and USDC unit is counted exactly once. Advancing `w` by `paid` (not to `cumulative`) is what makes a partially-paid draw retriable — re-presenting the same voucher after a top-up collects the rest.
- **Modifiers:** `nonReentrant`.
- **Emits:** `PoolRedeemed(poolId, signer, provider, paid, bytesPaid, newPaidCumulative)`.

`redeem` changes nothing about off-chain voucher exchange. Signers keep sending cumulative vouchers up to their cap, and the node keeps accepting them; the node reads the authoritative on-chain lane watermark (`w.amount`, the cumulative paid) and redeems the highest voucher it holds against it. Deciding *when* to redeem is node operational policy, bounded only by the capability `expiry` and the grace window. `redeemMany` registers each capability, then applies this per voucher and skips any that would pay `0`.

**Tracking owed vs. paid.** A node tracks the *paid* side by **consuming events, not by polling**. It subscribes to `PoolRedeemed` filtered on its own `provider` address (indexed) and, as each event arrives — including the ones its own `redeem` / `redeemMany` transactions emit — sets that lane's paid cumulative to the event's `newPaidCumulative`. That event stream is the single write path for the paid side, so a drained-pool partial pay is recorded exactly; the node never assumes its voucher cleared. It follows `PoolToppedUp` (indexed by `poolId`) the same way, to re-drive a lane that a dry pool left `owed > paid`. On startup or after a gap it reconciles like every other chain watcher — enumerate `PoolRedeemed` for its address from a pinned block, then tail live, resyncing on a missed range — so the watermark is never reconstructed by guesswork. *Owed* is the highest voucher cumulative the node has accepted per lane, persisted for replay-safety ([Off-chain voucher state persistence](#off-chain-voucher-state-persistence)). `unredeemed = owed − paid`, summed across a pool's lanes, is the in-flight value the reserved floor `M` must cover, and what keeps a lane pending until it is fully collected. A read-call (`getPool` → `deposit − totalRedeemed`; `getAuthorization` → `cap − spent`) is used only as an optional pre-flight to skip submitting a `redeem` that would revert `NothingToRedeem` against a dry pool — never to track paid state. All of this survives a restart: paid is rebuilt from the event log, owed from the persisted per-lane store.

#### Initial deployment values

The constructor takes `(usdc, capacityBond, feeRouter, disputeWindow, deliveryFloor, admin)` per [ADR 016 § Contract Inventory](016-contract-interactions.md#contract-inventory). Every governable parameter it exposes is a constructor argument; there are none it defaults. `disputeWindow` is a constructor argument validated against the hardcoded safety bounds (deployment default 48h — see the bounds table below and [ADR 009](009-governance.md#adr-009-governance-model) for governance ranges). A pool does not expire, so it configures no duration parameter. The constructor MUST reject any zero address among `(usdc, capacityBond, feeRouter, admin)` and a `feeRouter` whose code size is zero (EOA / undeployed address).

Default deployment value for `disputeWindow` (the redemption grace window): **172800 seconds (48 hours)** — sized to guarantee a node time to redeem under sequencer censorship (see [§ L2 sequencer censorship](#l2-sequencer-censorship) below). Safety bounds per [ADR 009](009-governance.md#adr-009-governance-model): 172800–259200 seconds (48h–72h). Under [ADR 026](026-tokenomics.md#adr-026-tokenomics) the constructor carries no `feePercentage` / `discountedFeePercentage` / treasury-address parameters; bucket shares are governed on `FeeRouter`, and the treasury bucket is one of `FeeRouter`'s three buckets (see [FeeRouter Integration](#feerouter-integration)).

#### L2 sequencer censorship

An owner (or colluding sequencer) calls `closePool` and ensures a node's `redeem` transactions are censored for the full grace window, so the owner can `reclaim` the remainder before the node is paid. Counterparties fall back to L1 forced inclusion, but this takes up to ~24 hours on Arbitrum (similar paths on other OP-Stack chains). If the grace window is no longer than that delay, the node's forced-included redemption lands too late.

**Mitigation — baseline grace window.** Censorship resistance comes solely from keeping the baseline grace window above the chain's maximum force-inclusion delay: the window default is **48 hours** (172800 seconds), which guarantees at least 24 hours of effective redemption time on any chain with a force-inclusion delay ≤ 24 hours. There is no on-chain forced-inclusion detection or deadline extension — a signed force-included `redeem` is indistinguishable on-chain from a sequencer-included one, so the window itself carries the guarantee. The setting is chain-agnostic and the governance floor equals the 48h default (bounds 48h–72h per [ADR 009](009-governance.md#adr-009-governance-model)), so the baseline can only be tightened upward and never dropped below the force-inclusion delay. A node also keeps a margin below the capability `expiry` (see [Revocation](#revocation)), so it is not depending on the grace window alone.

#### Events

All events use indexed `poolId` plus an indexed actor field where applicable.

| Event | Emitted by | Non-indexed fields |
| --- | --- | --- |
| `PoolOpened(poolId, owner, …)` | `openPool` | `deposit` |
| `PoolToppedUp(poolId, …)` | `topUp` | `additionalDeposit, newDeposit` |
| `PoolRedeemed(poolId, signer, provider, …)` | `redeem`, `redeemMany` | `paid` (USDC routed to `FeeRouter`), `bytesPaid` (paid-proportional bytes counted toward the operator's epoch byte counter), `newPaidCumulative` (the lane's cumulative paid amount after this redemption) |
| `PoolCloseInitiated(poolId, owner, …)` | `closePool` | `disputeDeadline` |
| `PoolReclaimed(poolId, owner, …)` | `reclaim` | `ownerRefund` (= `deposit − totalRedeemed`) |
| `RateBoundsUpdated` | `setRateBounds` | `newDeliveryFloor` |

`PoolOpened` indexes `poolId` and `owner`, so an owner lists its pools via `eth_getLogs(topics=[PoolOpened, *, paddedOwnerAddress])`; an indexer keys on `poolId`. `PoolRedeemed` indexes `poolId`, `signer`, and `provider` (the three-topic EVM maximum), so a node filters `PoolRedeemed` on its own `provider` address to follow every lane it is paid on without reading anything else.

An owner reconciling its pools after a restart reads its own `ownerPoolNonce` and recomputes each `poolId` (ids are derived per nonce, so there is no separate counter to drift), then re-hydrates each via `getPool`. A node does not enumerate pools from chain state — a pool names no provider — so it reconstructs its lanes from its own persisted `PoolStateStore` ([Off-chain voucher state persistence](#off-chain-voucher-state-persistence)) and confirms each on-chain via `watermark[poolId][signer][provider]`. A signer learns the `poolId` and its cap from the capability the owner issued it.

`PoolReclaimed` carries no `protocolFee` field, and `redeem` does not skim a fee inline. The bucket distribution emits its own events from `FeeRouter` (see [FeeRouter Integration](#feerouter-integration)).

**No pool expiry.** A pool has no lifetime cap and never expires; it is opened once and reused indefinitely. The money layer carries **no max-duration parameter** and no time-based reclaim — fund recovery is owner-initiated regular close (`closePool` then `reclaim` after the grace window), on demand, never time-triggered. Only **capabilities** carry an `expiry`, a short TTL that serves revocation (see [Revocation](#revocation)); the pool itself does not. Owner funds are therefore never stranded, and a node's serving window is bounded by each signer's capability `expiry` — the deadline `redeem` enforces per lane.

**Close and reclaim lifecycle:**

- `closePool(poolId)` → requires status `Open`. **Owner only** (`require(msg.sender == pool.owner)`). Sets status to `Closing`, sets `disputeDeadline = block.timestamp + disputeWindow`, emits `PoolCloseInitiated`. No fund transfers. It only starts the grace window; it moves no node's earnings, so it needs no voucher and cannot understate a lane.
- `redeem` → still callable while `Closing` and before `disputeDeadline`, so a node cashes outstanding vouchers after the owner closes. See [`redeem` behavior](#paymentpool).
- `reclaim(poolId)` → requires status `Closing` and `block.timestamp >= disputeDeadline`. Callable by any address; transfers `deposit − totalRedeemed` to `pool.owner`, sets status `Closed`, emits `PoolReclaimed`. The router is not called — payouts already happened at each `redeem`.

**Safety bounds (hardcoded):**

| Parameter | Minimum | Maximum |
| --- | --- | --- |
| Grace window (`disputeWindow`) | 172800 seconds (48 hours) | 259200 seconds (3 days) |
| Rate floor | 1 base unit | `MAX_RATE_PER_MB` (10^12) |

`PaymentPool` does not hold a fee-percentage parameter. Bucket-share bounds (60/30/10 with per-share bounds 40–90 / 5–50 / 0–30) are owned by `FeeRouter` per [ADR 026 § Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds).

**The rate floor is in USDC base units (6 decimals) per MB.** The contract stores `deliveryFloor`, the per-byte price floor **enforced at redemption** (see [Rate-floor enforcement](#rate-floor-enforcement) below). There is no governance ceiling: a seller self-clamping its own advertised rate downward buys no on-chain safety — a seller never wants to charge less — and the buyer's protection is seeing the signed rate in `StreamResponse` before it pays. The absolute upper bound is the wire constant `MAX_RATE_PER_MB` ([ADR 005](005-protocol.md#adr-005-wire-protocol)), which honest requesters reject above.

**Initial rate floor:**

| Parameter | Value (USD/MB) | USDC base units | Rationale |
| --- | --- | --- | --- |
| `deliveryFloor` | $0.000001/MB | 1 | Anti-abuse minimum; 10× below expected market rate. **Enforced at redemption** — the contract rejects any voucher whose cumulative `amount / bytesDelivered` falls below this floor, so claiming served bytes always costs proportional USDC. Prevents zero-rate free-riding and the served-byte vote-weight inflation of [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight), while imposing no practical constraint on legitimate pricing (nodes set rates well above it; the floor is an anti-zero safeguard, not a recommended price). |

The expected market rate is $0.00001/MB (10 USDC base units per MB, or $0.01/GB). This positions deCDN ~4–9× cheaper than major traditional CDNs (CloudFront at $0.085/GB, KeyCDN at $0.04/GB) and at parity with budget providers (Bunny.net at $0.01/GB). The floor is governance-tunable from day one within the hardcoded safety constraints above — admin-key-gated in the PoC, DecdnGovernor in production (see [ADR 009](009-governance.md#adr-009-governance-model)). Node pricing is otherwise a market outcome: nodes compete on the rate they advertise, and a node that overprices loses selection ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)).

### Rate-floor enforcement

`redeem` enforces the per-byte floor on the voucher's **cumulative** `amount` and `bytesDelivered` before advancing the lane watermark:

```
require:  amount * BYTES_PER_MB >= bytesDelivered * deliveryFloor      (BYTES_PER_MB = 1_048_576, ADR 005)
```

evaluated overflow-safely as `bytesDelivered <= Math.mulDiv(amount, BYTES_PER_MB, deliveryFloor)` so a voucher carrying `bytesDelivered` near `type(uint256).max` reverts with `RateFloorViolation` rather than an arithmetic panic. Because `deliveryFloor >= 1` the divisor is non-zero. The check runs on the cumulative `amount` / `bytesDelivered` in `redeem`, and uses **zero tolerance** — the floor sits 10× below the expected market rate, so honest traffic clears it by ≥10× and needs no rounding headroom (the off-chain 1% tolerance applies to the *advertised* `rate_per_mb`, not this floor).

This binds served bytes to real USDC: stamping `B` bytes requires cumulatively claiming `>= B / 1_048_576` base units, restoring the proportional-cost assumption [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) relies on. Serving nodes mirror the same floor off-chain (a zero-tolerance `verify_rate` against `delivery_floor`) before countersigning, so an honest node never accepts a voucher the chain would reject. The floor is the only rate bound the protocol carries, and it is binding.

### Rate Bounds Refresh

Nodes must keep their local copy of `deliveryFloor` current so an advertised `rate_per_mb` never falls below it. Staleness is a revenue risk rather than a safety one — a node quoting under a raised floor accepts vouchers the chain will reject at redemption — so the refresh strategy is lighter-touch than the content blacklist ([ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)), where serving blacklisted content is a slashable offense.

**Primary mechanism: event listening.** Nodes SHOULD subscribe to `RateBoundsUpdated` events on the `PaymentPool` contract and update the local cache immediately. Governance actions are infrequent (days to weeks), so high-frequency polling would be wasteful.

**Fallback mechanism: periodic polling.** Nodes MUST poll `getRateBounds()` at a configurable interval (`rate_bounds_poll_interval`, default **1 hour**), guarding against missed events from RPC provider issues, WebSocket disconnections, or chain reorganizations. The 1-hour default is deliberately longer than the 10-minute registry ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)) / blacklist ([ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)) intervals: registry freshness is connectivity-critical and blacklist freshness slashing-critical, but a stale rate floor only risks the node quoting below it and signing unredeemable vouchers.

#### Startup

Nodes MUST call `getRateBounds()` before accepting connections, never operating without a floor (same pattern as the content blacklist initial sync, [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)). Because `getRateBounds()` returns `uint256` but the wire protocol represents `rate_per_mb` as `u64` ([ADR 005](005-protocol.md#adr-005-wire-protocol)), nodes MUST verify `deliveryFloor` fits within `u64` on every refresh (startup and subsequent polls/events). If it exceeds `u64::MAX`, the node MUST refuse to start (or, on a mid-operation refresh, continue with its last valid floor and log an error). Unreachable against a correctly-deployed contract — `setRateBounds` caps the floor at `MAX_RATE_PER_MB` (10^12), far below `u64::MAX` — but the check guards against a contract deployed without that cap.

#### Stale bounds

If the event subscription is lost and RPC polling fails, the node SHOULD continue operating with its last-known floor and log a warning. No service interruption is required. The worst-case consequence of a stale floor is that the node quotes below a raised floor and its vouchers are unredeemable — a revenue impact, not a safety violation.

#### No version-based delta pattern

Unlike the content blacklist (which uses `getBlacklistVersion()` for cheap change detection and incremental delta fetching), the rate floor is a single `uint256`. A version counter adds no value — the full state is readable in a single `eth_call` with negligible overhead. This is an intentional divergence from the [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting) pattern.

For how nodes validate `rate_per_mb` against the cached floor before signing protocol messages, see [ADR 005 — Rate Bounds Validation](005-protocol.md#rate-bounds-validation).

### BuybackBurner

| Function | Purpose | Declared on |
| --- | --- | --- |
| `executeBuyback(amount, minTokenOut)` | Governance multisig or `keeper`: swap `amount` of USDC for ≥ `minTokenOut` TOKEN and burn the proceeds. | `BuybackBurner` (abstract base) |
| `setKeeper(addr)` | Governance: rotate the authorized keeper. | `GuardedBuybackBurner` |
| `setSlippageTolerance(bps)` / `setMinBuybackAmount(n)` / `setMaxBuybackAmount(n)` | Governance: per-call execution guards. | `GuardedBuybackBurner` |
| `setEpochLiquidityCapFraction(bps)` | Governance: per-epoch USDC liquidity cap as a fraction of epoch-start pool depth (bounded `[1%, 30%]`, [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol)). | `GuardedBuybackBurner` |
| `poke()` | Permissionless: advance the on-chain TWAP price accumulator that backs the `minOut` floor. | `GuardedBuybackBurner` |
| `keeper() → address` / `getAccumulatedFees() → uint256` | Views: current keeper and accumulated buyback inflow (USDC). | `GuardedBuybackBurner` |
| `setSwapRouter(addr)` / `setPool(addr)` | Governance: rotate the swap router or the pool. | venue subclass (`BuybackBurnerBalancerV3` / `BuybackBurnerUniswapV3`) |
| `setVault(addr)` | Governance: wire/rotate the Balancer V3 Vault. The Vault serves pool-registration and pool-state reads only; it is not an approval target. | `BuybackBurnerBalancerV3` only |

The members above are split across the three-layer `BuybackBurner` hierarchy — the abstract base, the `GuardedBuybackBurner` guard layer, and the concrete venue subclass — as the *Declared on* column records; there is no single flat contract, and `setVault` exists on the Balancer subclass only (`BuybackBurnerUniswapV3` has none). [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split) defines the economic parameters and the 30% router-fed inflow source. [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol) specifies the venue (Balancer V3 Router + 80/20 weighted pool) and how `setSwapRouter` / `setPool` are configured at deployment. **V3 integration note:** `setSwapRouter` holds the Balancer V3 **Router** address. The Router does not pull input tokens through a plain ERC20 allowance — it pulls them through **Permit2** (`permit2.transferFrom`), so Permit2 is the approval target and the Vault is never one. The concrete `BuybackBurnerBalancerV3` uses a scoped per-swap Permit2 approval and resets it to `0` after each swap, so no standing allowance survives. The Vault address is held separately and serves pool-registration and pool-state reads only. [ADR 018 § Buyback execution via Balancer V3](018-liquidity-strategy.md#buyback-execution-via-balancer-v3) is authoritative for the mechanism.

All `set*` functions are governance-only behind a timelock.

### FeeRouter Integration

Under [ADR 026](026-tokenomics.md#adr-026-tokenomics), `PaymentPool.redeem` does not split fees inline. The paid amount is forwarded to a `FeeRouter` contract on each redemption, which applies the canonical three-bucket split (60% operator base / 30% buyback-and-burn / 10% treasury — full table and bounds in [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split) and [§ Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds)). All three legs transfer in the same transaction as the redemption that fed them. This ADR specifies the `FeeRouter` interface only as it relates to the redemption path; the per-bucket details live in [ADR 026](026-tokenomics.md#adr-026-tokenomics).

#### Redemption-path interface

`PaymentPool.redeem` MUST invoke `FeeRouter.routeSettlement(address operator, uint256 bytesDelivered, uint256 amount)` in the same transaction as the payment-token `safeTransfer` to the router, passing the *paid* amount and the *paid-proportional* byte count for that redemption (`bytesPaid = mulDiv(bytesDelta, paid, delta)`, so a partially-paid draw counts only the bytes it paid for). The router pays the operator's 60% base share in that transaction, dispatches the 30% / 10% same-tx legs, derives the current epoch as `uint64(block.timestamp / EPOCH_LENGTH)`, and increments `bytesPerEpoch[operator][epoch]` by that paid-proportional byte count as the trailing-window served-bytes accumulator read by `DecdnGovernor._getVotes` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) as the governance vote-weight source. The router's `+=` accounting makes the per-redemption deltas sum to each lane's cumulative with no double-counting. The full `IFeeRouter` interface is canonical in [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model).

#### Redemption-path invariants

1. **Atomic base-share payout.** The 60% base share MUST land in the operator's wallet in the same transaction as each `redeem` — no claim step, no keeper, no off-chain queue. This is the Case A cashflow guarantee from [ADR 026 § Operator economics](026-tokenomics.md#operator-economics), realizable incrementally per redemption.
2. **No reentry.** `redeem` holds a `nonReentrant` guard for the duration of the router call.
3. **Routed deltas partition each lane's cumulative.** Each `redeem` routes a distinct, non-overlapping delta of the same lane, advancing the monotone watermark, so the router sees no overlap and each byte and USDC unit is counted exactly once.

Conservation and same-tx three-bucket invariants (60/30/10) live with the router itself in [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split) / [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model). The `Settled` event (operator + epoch + per-bucket deltas) is emitted by the router; full event set is in [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model).

#### Node-to-node redemptions (no router bypass)

**Node-to-node cache-miss paid pulls route through `FeeRouter` identically to client-to-node redemptions.** When node B pulls a blob from origin-backed node A and pays from its own pool, node A's redemption is not special-cased: `redeem` routes it through the same `_route` → `FeeRouter` path as a client-to-node redemption, with no node-aware branch, and the paid amount takes the same three-bucket 60/30/10 split per [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split). There is no `redeemNoRoute` entry point and no detect-and-skip branch; the only routing conditionals (paused-router defer) are party-agnostic.

A router bypass is not available, even though routing node-to-node payments "double-charges" the same downstream bytes (once when B pays A, again when B's clients pay B). Uniform routing keeps the contract surface minimal and is exactly what makes the structural wash-trading deterrent hold — a self-routed pool pays the 40% non-base skim (30% burn + 10% treasury) on every cycle (see [ADR 036 § Wash-trading as vote-buying](036-served-bytes-voting-weight.md#wash-trading-as-vote-buying)).

- All `PaymentPool` redemptions forward to `FeeRouter.routeSettlement` regardless of whether the counterparties are operators or end clients. Node-to-node bytes therefore **do** accumulate in the router's per-epoch byte counters and **do** count toward governance vote weight ([ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) sources vote weight from `FeeRouter.bytesInWindow`), bounded by the per-operator vote cap and the [ADR 036 § Wash-trading as vote-buying](036-served-bytes-voting-weight.md#wash-trading-as-vote-buying) cost model.
- The on-chain registry distinction — a pool is node-to-node when both the `owner` and the redeeming `provider` addresses have a registered NodeId binding (see [NodeId-to-Ethereum Binding](#nodeid-to-ethereum-binding)) — still exists, but it drives **probe-acceptance priority** ([§ Admission and Priority](#admission-and-priority)), not routing. Redemption is routed the same way either way.
- Permissionless settlement analyzers ([Appendix: Settlement Analysis](appendix-fraud-detection.md#appendix-permissionless-settlement-analysis)) observe node-to-node redemptions for self-routed-traffic / wash-trading patterns, feeding governance threshold-tuning — reinforcing, not replacing, the per-cycle skim cost.

#### Redemption sequence

End-to-end payment-token flow — client→node and node-to-node redemptions both use the same `PaymentPool → FeeRouter.routeSettlement` path — is diagrammed in [ADR 016 § USDC Flow (Payments)](016-contract-interactions.md#usdc-flow-payments). This ADR documents only the `PaymentPool ↔ FeeRouter` interface contract.

The full three-bucket split applies to every network deployment from launch. Simplified launch configurations are expressed by setting non-active bucket shares to zero via `FeeRouter.setShares(...)` per [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics), not by deploying a reduced-surface stub. The cross-validation invariant in that section ensures any non-zero share has a wired non-zero destination, so the launch share configuration alone determines which downstream contracts must be ready at deploy time.

### EIP-712 Voucher Signature

Capability and voucher signatures use [EIP-712](https://eips.ethereum.org/EIPS/eip-712) typed structured data to prevent cross-chain, cross-contract, and cross-environment replay.

**Domain separator:**

```solidity
bytes32 constant DOMAIN_TYPEHASH = keccak256(
    "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
);

bytes32 public immutable DOMAIN_SEPARATOR;

constructor() {
    DOMAIN_SEPARATOR = keccak256(abi.encode(
        DOMAIN_TYPEHASH,
        keccak256(bytes("PaymentPool")),   // name
        keccak256(bytes("1")),                      // version
        block.chainid,                              // chainId
        address(this)                               // verifyingContract
    ));
}
```

The domain separator binds every capability and voucher to a specific contract deployment on a specific chain. A signature made for one chain cannot be replayed on another, and one made for one `PaymentPool` deployment cannot be replayed against a redeployed contract at a different address.

**Capability type** (owner-signed, verified once per signer on first redemption):

```solidity
bytes32 constant CAPABILITY_TYPEHASH = keccak256(
    "Capability(address signer,uint256 spendingCap,bytes32 poolId,uint64 expiry)"
);
```

The capability names no provider — it is node-agnostic, valid at every node. `redeem` recovers it against `pool.owner` and stores `{cap: spendingCap, expiry, spent: 0}` in `authorized[poolId][signer]`.

**Voucher type** (signer-signed, node-addressed):

```solidity
bytes32 constant VOUCHER_TYPEHASH = keccak256(
    "Voucher(bytes32 poolId,address signer,address provider,uint256 amount,uint256 bytesDelivered)"
);
```

`signer` and `provider` are both in the signed payload: `signer` binds the voucher to the authorized key `redeem` validates against, and `provider` binds it to a single payee, so one node cannot redeem another node's voucher. `amount` and `bytesDelivered` are the lane's cumulatives. The voucher carries no epoch: per-operator-epoch attribution is derived by `FeeRouter.routeSettlement` at redemption time as `epoch = uint64(block.timestamp / EPOCH_LENGTH)`, so a redemption's bytes credit the epoch its transaction lands in. A node redeems whenever it chooses within the capability `expiry` (the per-signer redemption deadline) and the grace window, so the practical bound on epoch-shifting is that `expiry` — a second-order effect on emission share that shrinks as the active-operator set grows.

**Signature digests:**

```solidity
bytes32 voucherDigest = keccak256(abi.encodePacked(
    "\x19\x01",
    DOMAIN_SEPARATOR,
    keccak256(abi.encode(VOUCHER_TYPEHASH, poolId, signer, provider, amount, bytesDelivered))
));

bytes32 capabilityDigest = keccak256(abi.encodePacked(
    "\x19\x01",
    DOMAIN_SEPARATOR,
    keccak256(abi.encode(CAPABILITY_TYPEHASH, signer, spendingCap, poolId, expiry))
));
```

**Verification:** Implementations must use OpenZeppelin's `SignatureChecker.isValidSignatureNow(...)` — `signer` for the voucher, `pool.owner` for the capability. It transparently supports both EOA signers (via hardened `ECDSA.recover` that rejects non-canonical `s` values and restricts `v` to `27`/`28`) and smart account signers (via ERC-1271 `isValidSignature`). Signatures are 65 bytes (`r || s || v`) for EOA signers; smart account signers may use longer signatures per their wallet implementation. See [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support) for the full account abstraction design — session keys operate on *who may sign* a voucher (the `signer` key), while the multiple concurrent independent signers on one pool are an accounting property (the sharded register), not a signature-validation one.

The `DOMAIN_SEPARATOR` is computed once in the constructor and stored as an immutable. If the contract is deployed behind a proxy and may be migrated to a different chain, it should be cached in a state variable and recomputed only when `block.chainid` changes (the pattern used by OpenZeppelin's `EIP712` base contract), rather than on every call.

### Voucher ordering

Vouchers carry **no nonce**. A voucher is ordered and replay-checked entirely by its cumulative `amount`. The node tracks the cumulative it has accepted per lane and expects the next voucher to advance it by the rate times the newly delivered bytes — `expected = last_amount + rate_per_mb × ⌈new_bytes / 1_048_576⌉` — so it needs no separate sequence field. A voucher whose `amount` does not exceed the accepted cumulative is rejected (`AmountRegression`, [ADR 005](005-protocol.md#adr-005-wire-protocol)); that is the sole off-chain ordering/replay guard. On-chain redemption is likewise cumulative (`redeem` pays `cumulative − paid`), so an already-redeemed or stale voucher pays `0`, and the same voucher can be re-presented after a top-up to collect a drained-pool shortfall — only the unpaid `cumulative − paid` remains.

### Node Registry

The on-chain registry of bonded nodes is part of the `CapacityBond` contract, not a separate contract. Bonding is a prerequisite for registration ([ADR 026 § Operator economics](026-tokenomics.md#operator-economics)), so co-locating them avoids cross-contract calls and simplifies the atomic bond-then-register flow.

> **No fee-discount path on the pool contract.** Operator return is differentiated through the `CapacityBond` lock-to-capacity curve ([ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)), not via a bond-multiple fee toggle on the payment contract. `getEffectiveFee`, `getBondMultiple`, `DISCOUNT_MULTIPLE`, `feePercentage`, and `discountedFeePercentage` are not part of the interface. `CapacityBond` carries the registration, bond-bookkeeping, and slashing responsibilities; the operator bond is `bond = k × Mbps^α` with defaults `k=12.6`, `α=1.2` (≈50K TOKEN at 1 Gbps) per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve).

#### Data Structure

```solidity
struct NodeInfo {
    bytes32 nodeId;              // iroh NodeId (ed25519 public key, 32 bytes)
    address ethAddress;          // Ethereum address for payment pools (20 bytes)
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
function getRegisteredNodeCount() external view returns (uint256);
function getRegisteredNodes(uint256 offset, uint256 limit)
    external view returns (NodeInfo[] memory page, bool[] memory active);
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

1. **View functions.** `getRegisteredNodes(offset, limit)` with pagination. For tens of nodes, a single call with `limit = 100` returns the full node set. Clients call this on first startup to bootstrap their peer list, then rely on gossip for ongoing discovery (see [ADR 001 § Node Discovery (Gossip)](001-network.md#node-discovery-gossip)).

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

Node admission and queueing policy — how a node decides which requests to serve under congestion — is implementation-defined and lives outside the protocol. The wire format carries no priority bits, the pool and voucher mechanisms encode no per-stream priority state, and different operators are expected to tune their policy differently. There is deliberately **no probe-time congestion lever**. A probe answer states presence, not willingness to serve: a node advertises `has_blob: true` whenever it holds the blob and has not refused it, even when the `max_probe_holds` hold budget ([ADR 005 § Hold budget](005-protocol.md#hold-budget)) is exhausted — otherwise a probe flood that fills the hold cache could suppress truthful availability answers network-wide. Congestion is therefore expressed at **stream time**, via a signed `StreamResponse{ok: false, error: Overloaded}`. That is safe to sign freely: rate manipulation requires `ok == true` and blacklist violation requires a served claim ([ADR 014 § Evidence verification](014-on-chain-verification.md#evidence-verification-per-offense-type)), so a refusal is not slash evidence under any offense. Two signals inform an admission policy:

- **Committed voucher rate.** The advertised `rate_per_mb` in `ProbeResponse` / `StreamResponse` is a **floor**, not equality — nodes verify `amount_delta / bytes_delta >= rate_per_mb`. Clients MAY commit at higher rates; nodes MAY use the committed rate as a per-stream priority key for **ordering admitted, in-flight streams** — it is a mid-stream signal (the first `Voucher` arrives only after `StreamResponse{ok: true}`), not an admission key — with the premium paid directly via [`FeeRouter.routeSettlement`](#feerouter-integration).
- **Registered node-bond.** `CapacityBond.bondOf(address)` is readable on-chain, but only for requesters that are themselves registered operators — practically, node-to-node cache-miss probes. Nodes MAY prioritize *probe-acceptance* for addresses with `bondOf >= bond_required(declared_capacity)` ([ADR 026 § Operator economics](026-tokenomics.md#operator-economics)). A node bond is not a usable signal for end-client probes (clients are not registered); there, lane eligibility must come from reputation, region, or an allow-list.

## NodeId-to-Ethereum Binding

The protocol requires a verifiable mapping between iroh NodeIds (ed25519 public keys) and Ethereum addresses (secp256k1-derived). This binding is used for payment channel association and slash evidence attribution. Two orthogonal signature mechanisms protect this mapping: the EIP-712 `bindingSignature` (secp256k1) proves the Ethereum key holder consents to the association — preventing un-slashable registration; the `ed25519Signature` ([§ NodeId Ownership Verification](#nodeid-ownership-verification)) proves the NodeId's private key holder authorized the registration — preventing NodeId squatting.

### Binding Message Format

The binding uses EIP-712 typed structured data, signed by the Ethereum private key. Two typed structs share the per-address `nonce` counter: initial registration signs `RegisterNode`, which additionally binds the operator's acceptance of the current operator terms ([ADR 019 § Operator Safety Obligations](019-node-onboarding.md#operator-safety-obligations)); rebinding (key rotation) signs the narrower `BindNodeId`.

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
- `termsHash`: the operator-terms hash the caller accepts, which must equal the governance-canonical `currentTermsHash` (registration only; see [ADR 019 § Operator Safety Obligations](019-node-onboarding.md#operator-safety-obligations))

The EIP-712 domain separator is the same as the `CapacityBond` contract deployment (chain ID + contract address), preventing cross-chain and cross-contract replay. Terms acceptance is enforced at registration only, so rotating a NodeId through `bindNodeId` neither carries nor re-checks `termsHash`.

### On-Chain Registration

Node registration and NodeId binding are atomic. `CapacityBond.registerNode()` ([§ Node Registry](#node-registry)) accepts a `termsHash` parameter, a `bindingSignature` parameter — an EIP-712 signature over `RegisterNode(nodeId, bindingNonce[msg.sender], termsHash)` — and an `ed25519Signature` parameter proving ownership of the NodeId's ed25519 private key (see [§ NodeId Ownership Verification](#nodeid-ownership-verification)). It requires `termsHash == currentTermsHash`, verifies both signatures, writes the `nodeIdToAddress`/`addressToNodeId` mappings, emits `TermsAccepted(nodeId, termsHash, timestamp)`, and increments `bindingNonce[msg.sender]` in the same transaction that adds the node to the mesh. The per-address nonce counter is shared with `bindNodeId`, giving replay protection across both paths; the distinct typehash keeps a registration signature from being replayed as a bare rebind. This eliminates the window in which a node could be active but not slashable.

The standalone `CapacityBond.bindNodeId()` function below remains available for **rebinding only** (key rotation after initial registration). It is not needed at initial registration time.

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
// slot. Consumed by `OriginAssignment.addOrigin`
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

Clients without on-chain registration MAY include a signed binding in their `StreamRequest` to attest a NodeId↔Ethereum-address mapping for the connection's lifetime. The node verifies the EIP-712 signature over `BindNodeId(nodeId, nonce=0)` by recovering the signer from the fixed 65-byte form and comparing it against the claimed address. Smart-account clients are rejected fail-closed: off-chain ERC-1271 verification is deferred to Production ([ADR 024](024-account-abstraction.md#off-chain-erc-1271-verification)). The verified address is cached for the connection's lifetime and acts as a gate, not as an attribution source: the node refuses a request whose voucher `signer` is not the bound address, before any bytes are delivered, since that signer's vouchers would fail verification against the address it authenticated as. The address a voucher must recover to is the capability-authorized `signer` for the pool ([§ PaymentPool](#paymentpool)). This ephemeral binding is not stored on-chain and is valid only for the session. Wire-format details are in [ADR 005](005-protocol.md#client-identity-binding).

### Binding Requirements by Role

| Role | On-chain binding required? | Rationale |
| --- | --- | --- |
| Node (bonded) | **Yes** — `registerNode` performs binding atomically via `bindingSignature` (EIP-712, proves Ethereum key consent) and `ed25519Signature` (proves NodeId ownership) | Slash evidence references on-chain NodeId→address mapping; atomic binding eliminates gap; ed25519 proof prevents NodeId squatting |
| Client (opening a pool) | No — the pool `owner` and voucher `signer` fields are Ethereum addresses directly | Pool operations use Ethereum addresses, not NodeIds |

### Rebinding

A node or client can rebind their Ethereum address to a new NodeId by calling `bindNodeId` (the nonce increments, invalidating the old binding). The old NodeId→address mapping is deleted. This supports key rotation scenarios (e.g., compromised iroh key). Initial binding is handled atomically by `registerNode` and does not require a separate `bindNodeId` call.

## Decimal Handling

USDC uses 6 decimals; TOKEN uses 18 decimals. All payment amounts in the `incentive` crate use USDC base units (µUSDC). The voucher signing code uses raw base units — no decimal conversion in the signature path to avoid precision bugs.

**Voucher format:**

```
{poolId, signer, provider, amount, bytesDelivered, signature}
```

During delivery over `cdn/client/v1`, `{signature, amount}` are transmitted on the wire; the remaining fields are derived from stream context — `poolId` from the `StreamRequest`, `signer` the bound client key, `provider` the delivering node, and `bytesDelivered` the node's per-lane cumulative byte counter. See [ADR 005](005-protocol.md#adr-005-wire-protocol) for wire protocol details.

Full EIP-712 type definition and domain separator: [EIP-712 Voucher Signature](#eip-712-voucher-signature).

### Voucher Bytes-Delivered Field

`bytesDelivered` is a cumulative byte count signed alongside `amount`. It is the canonical served-bytes count carried in the `Voucher`, forwarded to `FeeRouter.routeSettlement` on each redemption, and aggregated into `bytesPerEpoch[operator][epoch]` (where `epoch` is derived from `block.timestamp` at redemption time) — the trailing-window served-bytes accumulator consumed by `DecdnGovernor._getVotes` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) as the governance vote-weight source. Properties:

- **Cumulative, monotonic per lane.** Like `amount`, `bytesDelivered` is strictly non-decreasing across vouchers within a `(poolId, signer, provider)` lane. `redeem` computes the byte delta against the lane's last redeemed cumulative.
- **Derivable from the fixed voucher granularity.** The voucher interval is a fixed 4 MiB, and `rate_per_mb` is MB-denominated. Signers computing `amount` from `bytesDelivered` use `amount = ⌈bytesDelivered / 1_048_576⌉ × rate_per_mb` (1 MB = 1,048,576 bytes per [ADR 005](005-protocol.md#adr-005-wire-protocol)). The voucher carries the byte count directly so the contract does not re-derive it.
- **Routed at redemption.** Forwarded as the paid-proportional byte count (`bytesPaid`) to `FeeRouter.routeSettlement` on each `redeem`; a fully-paid draw forwards the whole lane byte delta.
- **Cross-pool consistency.** A voucher signed for one pool, signer, and provider is bound by its EIP-712 typed data; `bytesDelivered` is part of that signed payload and cannot be replayed against a different lane.

The router does not validate `bytesDelivered` against any oracle of physical delivery — the value is whatever the signer signed. The defense is twofold. **Structurally**, per-byte revenue requires real client USDC inflow rather than self-attested byte counts (a redemption pays only what the pool holds, and forwards paid-proportional bytes). **Quantitatively**, `redeem` enforces the `deliveryFloor` per-byte price floor (see [Rate-floor enforcement](#rate-floor-enforcement)), so a voucher cannot decouple a large `bytesDelivered` from a tiny `amount` — claiming `B` bytes costs `>= B / 1_048_576` base units. Without the floor, the pool balance alone would be insufficient: a signer could stamp arbitrarily many bytes at `amount = 1`. Governance vote weight is sourced from the same floor-bound per-byte counter ([ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)), so both wash-trade revenue and vote-buying are bound by proportional real USDC.

## Slashing and Pool Interactions

Slashing and pools are independent by design.

**Slashing does not affect pool funds.** Slashing operates exclusively on TOKEN bond in the `CapacityBond` (schedule per [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn) — 5%/15%/50% escalation tiers; slashed bond is held in escrow-on-slash and distributed at finality 50% challenger / 50% burn). Pool funds are owner deposits held in escrow — not bond, never touched by slashing. This follows from the functional separation in [Consequences](#consequences): `PaymentPool` never holds or moves TOKEN bond, cannot be called by `CapacityBond` to slash or reassign bond, and any `CapacityBond` interaction is read-only (e.g., resolving NodeId↔address bindings).

**Slashing can drop a node below its tier minimum bond while it holds vouchers.** Pool deposits being independent of the bond, a node can be slashed below the tier minimum (or to zero) while holding unredeemed vouchers. Redemption continues regardless of bonding status; a payout is purely a function of voucher and register state, not registry status.

**Auto-ejection does not block redemption.** When a node's bond drops below 50% of the minimum and auto-ejection triggers (see [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn)):

- Outstanding vouchers redeem normally. Owner funds are never trapped.
- The ejected node cannot be selected for new service (clients verify node registration, and nodes verify counterparty status before accepting a `StreamRequest`).
- The ejected node is removed from gossip routing, so it receives no new client connections.
- `redeem` (provider only) remains callable on any pool it holds vouchers against — it checks pool and register state, not registry status, so a slashed or ejected operator can still redeem revenue it already earned. `closePool` / `reclaim` are unaffected on the owner side.
- The node must re-bond at the full tier minimum (`bond_required(declared_capacity)`) and re-register to resume operations.
