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
- **Voucher** — signed by the authorized `signer` over `{ pool_id, signer, provider, amount, bytes_delivered, chain_root, chunk_price }`. It is **node-addressed**: it names the payee `provider`. `provider` is mandatory because `pool_id` does not encode the payee; without it the contract could not attribute a payment or keep independent per-node ordering, and one node could redeem a voucher meant for another. `amount` is the cumulative settlement anchor. `chain_root` heads an optional hash chain that advances that anchor between signatures, and a `chain_root` of zero seals the voucher at exactly `amount` (see [Hash-chain metering (PayWord)](#hash-chain-metering-payword)). `chunk_price` is signed so the *claim arithmetic* is fixed at signing time; the floor clamp at redemption still reads the live `deliveryFloor`.

The mechanism operates at two tiers, both on the same pool primitive:

- **Client → node**: a client opens a pool and issues capped capabilities to one or more signers (its own key, or per-device or per-session keys it delegates). Each signer streams cumulative vouchers to the nodes it fetches from, and a node redeems its own vouchers on-chain.
- **Node → node**: when a node pulls content from another node (typically an origin-backed node), it pays from its own pool via the same voucher mechanism. The origin-backed node is paid wholesale; the pulling node recoups this by serving multiple clients from its cache at a markup.

A node redeems a voucher on-chain at any time while the pool is open — redeeming a signed, monotone claim needs no dispute window (see [Redemption and Close](#redemption-and-close)). The owner closes the pool to reclaim the unspent remainder. A redemption grace window (default 48 hours, governable within 48h–72h — see [ADR 009](009-governance.md#adr-009-governance-model)) lets each node redeem outstanding vouchers before the owner reclaims. Reclaim returns the deposit minus the total redeemed across all lanes.

**Chain-agnostic.** The pool makes opens rare and moves them off the client's fetch path, so a pool open is not latency- or throughput-critical. The payment model therefore assumes no specific chain and does not require fast or cheap opens. The chain choice is a separate cost and neutrality decision (see [Appendix: L2 Deployment](appendix-l2-deployment.md#appendix-production-l2-deployment-target)).

Key parameters:

- Payment quantum: `chunk_bytes` = 1 MiB delivered per meter tick, a fixed constant every signer and node reads directly — no negotiation, no wire field (see [Chunk Cadence](#chunk-cadence)). A tick is a released hash-chain preimage, not a signature: one signed voucher opens a 255-chunk chain and a second closes it, so signatures are O(1) per transfer rather than one per interval
- Minimum deposit: none on-chain beyond non-zero. Deposit sizing is node-informed policy, not a contract floor (see [Deposit Economics](#deposit-economics))
- Fee routing: each redemption forwards the paid amount to `FeeRouter.routeSettlement(operator, bytesDelivered, amount)`, and the three-bucket split (60% operator base, 30% buyback, 10% treasury) is dispatched same-tx per [ADR 026](026-tokenomics.md#adr-026-tokenomics). See [FeeRouter Integration](#feerouter-integration).
- Operator return is differentiated through the `CapacityBond` lock-to-capacity curve per [ADR 026](026-tokenomics.md#adr-026-tokenomics), not via a fee-discount mechanic on the payment contract.

### Capability delegation

The pool owner authorizes each voucher signer with a **capability**. The capability is an owner-signed object over `{ signer, spending_cap, pool_id, expiry }`. It lets `signer` spend up to `spending_cap` from `pool_id` until `expiry`. It names no node, so one capability is valid at every node.

The owner issues one capability for each key it authorizes. That key is the owner's own key, a per-device key, or a per-session key. Each signer draws against its own `spending_cap`. The sum of the caps can exceed the deposit, so one signer draws heavily while another draws nothing (see [Deposit Economics](#deposit-economics)).

Delegation bounds the damage a compromised key does. The `spending_cap` bounds what that key authorizes. The `expiry` retires the key when the owner does not renew it. The owner keeps sole control of `topUp` and `closePool`.

Three sections give the rules in full:

- [Redemption and Close](#redemption-and-close) — the first redemption for `(poolId, signer)` registers the signer. The contract verifies the owner signature and stores `{cap, expiry, spent}`. Each later redemption for that signer carries the voucher alone.
- [Revocation](#revocation) — the `expiry` is the only time bound the contract applies at redemption. A stop-serving signal refuses future service to a key, and it does not void a voucher the node already earned.
- [EIP-712 Voucher Signature](#eip-712-voucher-signature) — the `Capability` typehash, and how redemption recovers the capability against `pool.owner`.

### Deposit Economics

A pool is opened once and reused. There is no per-node, per-fetch, or per-client open, and no pool close on the client's fetch path. The on-chain footprint of a pool is one open plus occasional top-ups, independent of how many nodes it pays or how many signers it delegates. A node registers each new signer's capability once on first redemption; every later redemption for that signer is voucher-only. On-chain cost is therefore independent of client count: many clients collapse to one pool plus lazy per-`(signer, provider)` register slots for active pairs only.

**Deposit sizing is node-informed policy, not a schedule.** The pool is oversubscribable — the sum of signer caps may exceed the deposit, which is what lets one signer draw heavily while others draw nothing. Nodes keep the pool solvent by reserving a refundable minimum-remaining-deposit `M` — they stop serving before the balance reaches `M`, so the reserve covers every in-flight voucher and refunds to the owner. The owner sizes the deposit to cover expected spend plus that reserve. `M` is node policy (see [Pool solvency and the refundable floor `M`](#pool-solvency-and-the-refundable-floor-m)); the contract enforces none of it. `openPool` enforces one floor of its own: the credited deposit must meet the governed `minDeposit` (dormant at launch — see [Minimum deposit](#minimum-deposit)). A single user opens a small pool (for example $10), makes its own key the sole signer, caps it at the deposit, and tops up when low. A client that fans out to many signers opens one larger pool and issues each signer a small capped capability.

**The pool opens at its full sized deposit.** There is one deposit amount: the owner escrows it in full at open, and every top-up restores the balance toward it. The pool is fully refundable — the owner reclaims the unspent remainder at close — so opening below the sized deposit and enlarging later reduces no exposure; it only risks a stream running short. A shared pool is addressed by owner and node, not by counterparty, so opening against a specific node earns nothing either: the one deposit already backs payments to every node the owner fetches from. Opening at the full deposit is therefore the only sensible policy.

**Top-up.** The owner adds funds with `topUp` when the pool balance runs low, rather than opening a second pool. `topUp` extends a pool that has been drawn down toward its sized deposit; it does not exist to graduate an intentionally under-funded open. `topUp` spends only its own transaction. A buyer that runs its pool short mid-fetch tops up and resumes at the paid frontier, so no delivered byte is skipped or paid for twice. A serving node refuses a new stream once the pool's remaining balance, less its floor `M`, cannot cover a credit window, and it reports that refusal as a plain miss. A buyer that opens more than one stream in a fetch therefore tops up while its remaining balance still covers `M` plus the next voucher. A node buyer uses its own configured `M` as the estimate, because a node that runs the same software keeps the same default. This estimate only starts a top-up. It never stops a buyer from opening a stream, because a node with a smaller `M` still serves.

#### Minimum deposit

The contract stores `minDeposit`, a governance-set minimum for the credited `openPool` deposit. The check reads the received balance delta, not the requested amount, so a fee-on-transfer proxy cannot open a pool below the floor. An open below the floor reverts `BelowMinDeposit(received, minDeposit)`. `topUp` is exempt: the floor prices a new pool identity, not growth of an existing pool.

The knob is a Sybil defense. The per-pool floor accumulator `M` bounds loss per pool, and an attacker escapes it by fanning out across many pools. With the floor armed, N concurrent pools lock at least `N × minDeposit` of capital. The deposit stays fully refundable at close, so the floor taxes locked capital for simultaneous pools, not a per-pool sunk cost. It does not by itself close single-pool abuse (the `M` accumulator handles that) or cross-node fan-out.

Bounds are hardcoded: 0 to 100_000_000 base units ($100). There is no floor on the knob itself — `0` keeps it dormant, and any non-zero deposit opens a pool. The ceiling keeps governance from pricing small honest buyers out of opening a pool. The launch value is `0`: the knob ships dormant, which keeps small pools available for development and testing, and governance arms it with `setMinDeposit` when Sybil pressure warrants. Pool spam under a dormant floor is bounded by gas — each `openPool` costs gas and locks real funds refundable only to the owner.

#### Smart Account Support and Gasless Pool Opens

All deCDN contracts use OpenZeppelin `SignatureChecker` for signature verification, supporting both EOA (via `ecrecover`) and smart-account wallets (via ERC-1271 `isValidSignature`). The one exception is the redemption voucher, which recovers with plain `ecrecover` over a compact signature so that a lane stays cheap enough to redeem at small balances — see [Voucher signatures are compact, and their signers are EOAs](#voucher-signatures-are-compact-and-their-signers-are-eoas). Safe and other ERC-1271 smart accounts are supported wallet types for pool owners and node operators; the encrypted EOA keystore is the documented default — see [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support).

Because a pool is opened rarely and off the fetch path, gas-abstraction standards are optional conveniences rather than hot-path requirements:

- **ERC-2771 meta-transactions.** A relayer submits the `openPool` transaction on behalf of the owner, paying gas and recouping it from the deposit or a sponsorship fund. Requires a trusted-forwarder check on the contract.
- **ERC-4337 account abstraction.** A smart-contract wallet batches payment-token approval + pool open into one user operation, with a paymaster sponsoring gas in the payment token. Works with unmodified contracts.

Gas abstraction via ERC-2771 or ERC-4337 paymasters is targeted at production.

### Chunk Cadence

Delivery is metered in **chunks**. One chunk is `chunk_bytes` = **1 MiB (1,048,576 bytes)**, and that value is a protocol constant. No message carries it, no node advertises it, and governance does not move it.

**Why a constant.** A chunk is the unit one hash-chain tick pays for ([Hash-chain metering (PayWord)](#hash-chain-metering-payword)). Redemption converts a chain index into bytes and USDC with it. If the payer and the node held different values, one released preimage would be worth two different amounts, and the contract cannot learn which value the two sides meant. A fixed constant removes the disagreement instead of negotiating it away. A later change invalidates every voucher signed against a live chain, so it is a protocol-version change and not a knob.

**Why 1 MiB.** The payment token sets the floor. USDC has 6 decimals, so a chunk that prices below one base unit cannot be charged at all. 1 MiB prices to about ten base units at the expected market rate, and to exactly one at the governance floor. [Chunk sizing and the payability floor](#chunk-sizing-and-the-payability-floor) gives the full table.

**`chunk_price` is `rate_per_mb`.** `CHUNK_BYTES` equals `BYTES_PER_MB`, so a chunk costs what the node advertises per MB. The payer signs that price into the voucher, which fixes it for the chain that voucher opens. The chain therefore introduces no second price unit, and its payability floor is the `deliveryFloor` the contract already enforces ([Rate-floor enforcement](#rate-floor-enforcement)).

**The node MUST check the price it is being paid.** `chunk_price` is signed by the *payer*, and a preimage carries no price of its own — its whole value is inherited from the voucher that opened the chain. A voucher signed at the governance floor against a node quoting ten times that would meter every later chunk at a tenth of the quote, and no per-tick moment would reveal it. So on every metering voucher the node MUST require `chunk_price` to equal its own quoted `rate_per_mb`, and reject `ChunkPriceMismatch` otherwise. A node's rate is fixed for its lifecycle rather than hot-reloaded, so this is a plain equality check at accept time, not a tolerance band. A **sealed** voucher meters no chunk and MUST carry `chunk_price = 0` (see [The sealed voucher (zero-hash root)](#the-sealed-voucher-zero-hash-root)); the equality check applies only to metering vouchers.

**The cadence does not bound the node.** A node pauses when the unpaid balance on a lane reaches its [credit window](#credit-window), which is node-local policy. The chunk is the meter's resolution, not the node's exposure limit.

### Hash-chain metering (PayWord)

**A chain is not a second payment object.** A lane has one voucher, and its cumulative `amount` is the settlement anchor. The hash chain is an optional extension that advances that anchor without another signature. Redemption resolves both with one formula:

```
claimed = amount + chain_index × chunk_price
```

At `chain_index = 0` the formula returns `amount` and walks nothing, which is the pre-PayWord voucher exactly. At `chain_index > 0` the node presents a **preimage** for the chunks it streamed past the last signature, and the contract walks at most `MAX_CHAIN_LENGTH` hashes to check it. The type and the contract need one amount and one optional chain that extends it. Nothing else.

**Mechanics.** The payer draws a random seed `s` for the lane and commits `chain_root = keccak^255(s)`, where `N = MAX_CHAIN_LENGTH = 255`. It signs that root into the voucher. After the node delivers chunk `k`, and after the payer verifies those bytes against the BLAKE3 root, the payer releases `keccak^(N−k)(s)`. The node hashes the released value forward until it reaches a value it already trusts. The root never travels on the wire, because the signed voucher already carries it.

**A tick costs one hash.** Accepting a preimage needs no signature, no acknowledgement, no round trip, and no durable write before the node sends the next chunk. Preimage resistance also makes a preimage self-proving: nobody computes `keccak^(N−k−1)(s)` from `keccak^(N−k)(s)` without the seed, so a deeper preimage **is** the receipt for every chunk below it. This removes the per-interval secp256k1 signature from the payer's CPU and the matching `ecrecover` from the node's delivery path, and it leaves the node's lane advance as one in-memory index bump ([Off-chain voucher state persistence](#off-chain-voucher-state-persistence)).

#### Two payment resolutions

A chain meters bulk delivery. A signature settles exact values. The design keeps both, rather than making either do the other's work.

| Path | Resolves | Exact to | Cost |
| --- | --- | --- | --- |
| Preimage | whole chunks past the anchor | `chunk_bytes` (1 MiB) | one hash; no signature, no acknowledgement, nothing to persist |
| Voucher `amount` | any residual, including a partial trailing chunk | 1 USDC base unit | one signature |

A partial trailing chunk, or a transfer smaller than one chunk, settles through a fresh **amount-voucher** — a voucher with a zero root, redeemed at index 0. It does not settle through a preimage, because a preimage always advances the claim by a whole chunk. A 1.5 MiB transfer therefore pays one preimage for the first chunk plus one closing amount-voucher for the exact 1.5 MiB, and its redemption walks nothing.

| Transfer | Signatures | Walk at redemption |
| --- | --- | --- |
| Fits the credit window | 1 — deliver, then one closing amount-voucher | none |
| Large, payer closes cooperatively | 2, plus one per rollover — an opening voucher that carries `chain_root`, then a zero-root closing voucher | none |
| Large, payer disappears mid-chain | 1 — the opening voucher; the node walks the deepest preimage it holds | at most 255 hashes |

**The chain is the unilateral fallback. The amount-voucher is the cooperative close.** Signatures are O(1) per transfer plus one per rollover, and never one per chunk. The node redeems the strongest proof it holds, and cumulative `claimed − paid` accounting makes that choice idempotent: a weaker proof costs a retry and nothing more.

#### The sealed voucher (zero-hash root)

A closing voucher meters no chunk, so its whole chain section is zero: `chain_root == 0`, `chain_index == 0`, a zero preimage, and `chunk_price == 0`. Those zeros satisfy the ordinary `keccak^k(preimage) == chain_root` check. At any higher index the same check needs a value that hashes to `0`, and keccak preimage resistance makes that infeasible. The voucher is therefore **sealed at its `amount`**, and nothing can redeem more against it. Because the section is uniformly zero, `chain_root == 0 ⟺ index == 0 ∧ preimage == 0 ∧ chunk_price == 0` is what the node checks, and it is what makes the added redeem calldata compress to almost nothing on the cooperative path ([Chain walk and redemption cost](#chain-walk-and-redemption-cost)).

A real root is a keccak output, so it is `0` only with probability 2⁻²⁵⁶, and a payer that drew such a seed redraws. `0` is therefore a safe sentinel. The schema keeps one fixed shape, the field stays mandatory, and the contract gains no branch.

A voucher that carries a real root also settles exactly `amount` at index 0: the node submits the root as its own preimage, which it already holds. Both shapes reach "settle the signed amount" through the same check, so nothing tests `chainRoot == 0`.

#### Chunk sizing and the payability floor

`chunk_bytes` has three roles. It is the price quantum. It is the meter resolution. It is also the floor of the credit window. USDC's 6 decimals put a hard floor under it. A chunk that prices below one base unit rounds to zero, and a meter that ticks zero cannot charge.

The table prices chunks in the unit the contract itself uses — base units per MB, where MB is `BYTES_PER_MB` = 1,048,576 bytes — so it carries no ambiguity about whether a "GB" is decimal or binary. The governance floor is `deliveryFloor` = 1 base unit per MB; the expected market rate is 10.

| `chunk_bytes` | At the 10-unit/MB market rate | At the 1-unit/MB floor | Quantization at the floor | Verdict |
| --- | --- | --- | --- | --- |
| 100 KB | 0 base units | 0 base units | unpayable | rounds to zero at both rates |
| **1 MiB** | **10 base units** | **1 base unit** | **one whole tick** | **chosen** |
| 10 MiB | 100 base units | 10 base units | 10% | payable, but a 10 MiB meter tick |

**1 MiB is exactly one MB of price, by identity.** Because `CHUNK_BYTES` equals `BYTES_PER_MB`, a chunk prices to exactly the advertised per-MB rate — ten base units at the market rate, one at the governance floor — with no rounding at any rate. The payability floor is therefore the existing `deliveryFloor` ([Rate-floor enforcement](#rate-floor-enforcement)) and the chain adds no constant of its own. The honest caveat is at the floor itself: there a chunk costs one base unit, the smallest amount USDC can express, so the meter quantizes fully and a partial chunk cannot be priced by a preimage at all. That residual settles through a closing amount-voucher like any other partial chunk.

The meter is therefore ~1 MiB-grained. That is four times finer than the 4 MiB voucher interval it replaces, and much coarser than the 1024-byte BLAKE3 leaf. The token sets that limit, not the hash: hashes are cheap enough to tick far finer, but USDC cannot price the tick. A coarser chunk helps only a network that runs near the floor price, and any change breaks every signature made under the old value, so it is a protocol-version change coordinated across every implementation — not a parameter.

#### One chain per lane

A released preimage is a **bearer proof**. It names no payee. Its binding to a payee comes entirely from the voucher whose `chain_root` it satisfies. Two rules follow, and both bind the **payer**:

- A signer **MUST** draw an independent, well-seeded random seed for every chain it opens.
- A signer **MUST NOT** reuse a root across providers, across pools, across its own sibling signers, or across the successive chains of one lane.

The reason is direct. One root in two vouchers makes every preimage released to the first provider a valid extension of the second provider's voucher. The second node then claims chunks it never delivered, and the payer pays twice for one tick. Per-lane watermarks bound each claim independently, but they cannot see the collision: each claim is valid under its own signature, and the contract never holds the two together.

**There is no node-side rule here, and there should not be.** Root reuse costs the payer and *pays* the node, so a node that rejects a reused root only declines revenue — anyone who wanted the money would run a build that accepts it. A node-side check would also need an unbounded, never-expiring set of every root the node has ever seen, which no lane record can hold. Reuse-avoidance is therefore a payer-side invariant enforced by the payer's own seed derivation, not a wire rejection. Its adversarial form is in [Cross-lane preimage spend](#cross-lane-preimage-spend).

The chain's scope is the **lane**, not the stream. Concurrent streams on one lane share one root and one index — see [Concurrent Streams](#concurrent-streams).

**The seed is 32 random bytes, held only in memory.** A signer draws it when the chain opens, keeps it for as long as the chain meters, and drops it with the chain. Two draws collide with probability 2⁻²⁵⁶, so both rules above hold by the draw itself. Reuse is not a rule the payer enforces against its own state; it is an outcome the draw does not produce.

There is no derivation, no master secret, and no counter — and therefore nothing to persist, nothing to synchronize between devices, and no secret at rest. A payer that holds its signing key holds everything a lane needs.

This rests on one rule, stated next: a chain is only ever extended by the process that drew it.

#### Resumption folds

**A payer never restarts a chain it no longer holds in memory.** Crossing a process boundary, a device boundary, or a self-heal from a node's watermark bundle all take the same route: fold the frontier the node proved into a fresh signed `amount`, and open a fresh chain on a fresh seed.

The fold is the rollover fold ([Chain length and rollover](#chain-length-and-rollover)) with the frontier read from the node rather than from local state: `amount_old + verified_index × chunk_price`, `bytes_old + verified_index × CHUNK_BYTES`. It needs the signing key and nothing else, which is exactly what a resuming payer has.

**The frontier a payer folds MUST be one it can prove.** A watermark bundle's `verified_index` is a number the node writes and no signature covers, so a payer that folded it unchecked would sign for chunks a lying node never delivered. The bundle carries the antidote: `tip`, the deepest preimage the node was actually handed. Preimages are unforgeable, so a payer MUST accept `verified_index` only when

```
keccak^verified_index(tip) == chain_root
```

against the `chain_root` covered by its own signature on the bundle's anchor voucher. Local hashing, no round trip. Without the check the fold is an unauthenticated invoice; with it, `verified_index` is provable delivery depth.

Within a live process the rule does not apply, and must not: a sibling stream joining a lane whose chain the payer still holds re-anchors to that chain with a zero-delta voucher instead of folding, because folding there would supersede the siblings' in-flight reveals ([Concurrent Streams](#concurrent-streams)). The rule is: if the seed is in memory, join the chain; if it is not, fold.

The cost of always folding is one signature and one fresh ladder per boundary crossing. What it buys is that a chain never has to be reconstructed — which is why the seed can be random, why nothing is stored, and why a second device holding the same key needs no shared state at all.

#### Chain length and rollover

`chain_length` is not a voucher field. The chain index is a `u8`, so the index space is `0..=255` — 256 slots, exactly one byte. Index 0 names the root and resolves to the voucher's own `amount`: the base voucher is payable on its own, and that base may already fold in a whole retired chain through the rollover fold. Indices `1..=255` are the chain's **incremental** range — each adds one `chunk_price` over that anchor. `MAX_CHAIN_LENGTH = 255` is the highest index, so at 1 MiB per chunk a chain adds up to 255 MiB over its anchor.

**The index type is the chain length, not a bound on it.** The chain index travels in a single byte, and that byte's domain *is* the index space — nothing was rounded down to fit it. The message body is `preimage ‖ index` — 32 bytes plus that one — and the frame around it adds the varint length prefix and message discriminant of [ADR 013](013-schema-evolution.md#adr-013-schema-evolution) like every other frame ([ADR 005 § Payment quantum and credit window](005-protocol.md#payment-quantum-and-credit-window)). Choosing a wider index type is what would lengthen the chain: a `u16` index would amortize signatures much further, at the cost of a wider tag on every reveal and a proportionally longer worst-case walk. The `u8` gives a fixed-width message and a walk capped at 255 hashes.

**The `uint8` cast is the walk's only bound.** `PaymentPool` extracts `chainIndex` as a `uint8` from the packed `chainMeter` word, and that cast — not a range check — is what caps the on-chain keccak walk at 255 iterations (~16k gas at full depth). A wider index removes the bound along with the type, so it would have to be replaced by an explicit one; at `u16` an unbounded walk reaches ~4.2M gas, which a redeemer pays and cannot refuse. Widening also moves `chunkPrice` inside the packed word, costs the payer a pre-materialized ladder proportional to the index space, and turns `preimage ‖ index` from a fixed 33 bytes into a varint-tagged message. None of that is prohibitive, but it is a contract change and a wire change rather than a type change, and one signature per 255 MiB is already O(1) per transfer at any transfer size.

**The byte is the index.** `chain_root = keccak^255(s)` sits at index `0`, and index `k` reveals `keccak^(255−k)(s)`, which redemption checks as `keccak^k(preimage) == chain_root`. The wire byte carries the index as itself, and the same byte is the low byte of the packed `chainMeter` word at redemption ([Voucher signatures are compact, and their signers are EOAs](#voucher-signatures-are-compact-and-their-signers-are-eoas)) — one representation end to end, with no offset on send and no increment on receipt, so no off-by-one is possible.

Index `0` is **not a dead slot**. It is the settlement case, and it pays: it resolves to `cumulative`, the amount the voucher already carries. What it does not do is add an increment, which is why it never travels the wire — a reveal at index 0 would prove nothing the voucher does not already say. So the 256 slots are one payable base plus 255 incremental steps over it, not 256 minus a wasted one.

**Rollover.** A payer rolls a chain when it exhausts it, and may roll earlier at its own discretion. The fold is the same either way and is stated in terms of the frontier actually reached, never a flat 255: the new voucher's `amount` is `amount_old + verified_index × chunk_price` and its `bytes_delivered` is `bytes_old + verified_index × CHUNK_BYTES`, with a fresh independent `chain_root`. The index resets to 0.

Both directions of that equality matter. Folding **less** than `verified_index` discards value the node has already proved, because adopting the new root retires the old chain. Folding **more** makes the payer sign for chunks it has not received, which is the one thing [Credit Window](#credit-window) promises it never does. One signature per 255 MiB keeps signatures O(1) per transfer at any transfer size.

**An under-folding rollover is rejected, not salvaged.** A node that receives one refuses the voucher outright: nothing is adopted, nothing is displaced, and the lane's claim is exactly as strong after the rejection as before. The rejection is watermark-gated, so it carries the bundle that states the frontier the payer owes, and the payer folds correctly and continues. No honest payer reaches this — a payer serializes its own issuance, and a voucher that folds must also roll, so the folded amount covers the frontier by construction — which is why the loud answer is the right one: it surfaces a payer bug instead of silently continuing on a weaker claim.

### Credit Window

This section is the **delivery** credit window — unbilled egress already on the wire. It is distinct from the refundable floor `M` a node reserves against a pool's on-chain drain ([Pool solvency and the refundable floor `M`](#pool-solvency-and-the-refundable-floor-m)). This one bounds bytes delivered ahead of the next voucher; `M` bounds how far a pool may be drawn down before nodes stop serving it.

The payment quantum is the *billing* granularity, not the *delivery* granularity. A delivering node streams within a **credit window**: it keeps sending chunks while the unpaid balance `delivered − paid` stays within the window and pauses a stream only when the next chunk would cross that bound — not at every chunk boundary. Between one chunk and the window, delivery and payment run concurrently. The node sends chunks ahead of the proofs that pay for them; the payer releases a preimage at each chunk boundary and keeps receiving rather than waiting for the acknowledgement, so the acknowledgement is off the delivery critical path.

Decoupling the two rates is what keeps single-stream throughput link-bound rather than round-trip-bound. Collecting a proof at every chunk leaves the link idle for a full round trip once per chunk, capping throughput at `chunk_bytes / (RTT + service_time)` regardless of link capacity, and the repeated idle periods keep the transport's congestion window from reaching steady state. A window of several chunks keeps bytes in flight across the proof round trip, so the link stays saturated.

**The window ramps with the lane's own payment.** The credit window for a lane is `min(credit_max, max(chunk_bytes, paid / credit_ramp_divisor))`, where `paid` is that lane's cumulative confirmed payment. A lane starts at the floor of one chunk and grows its window in proportion to `paid` as it pays, up to the `credit_max` ceiling. The window is applied **collectively** across every stream on the lane — the node tracks total unpaid bytes across all of them and pauses all of them together, so opening more streams buys no extra credit. That has to be lane-wide under PayWord: a preimage advances the lane's chain and cannot be attributed to the stream that carried it. A lane that never pays stays pinned at the floor; nothing about the window depends on any other lane, signer, or the node's history with the counterparty.

**Exposure is one-sided and self-bounding.** The payer's exposure stays at zero: both proofs are cumulative over bytes already delivered, so the payer never signs a voucher or releases a preimage for bytes it has not received. The node's exposure is the unbilled egress already on the wire, `delivered − paid`, bounded by the ramped window. Once `paid / credit_ramp_divisor` clears the one-chunk floor the ramp holds that exposure to `paid / credit_ramp_divisor` — a fixed fraction of the revenue the stream has already confirmed, never a fraction of some larger promise; below that point, and at `paid = 0`, the bound is the one-chunk floor. With the default divisor of 2, a node never fronts more than half of what a stream has paid it, above the floor. This is the same bounded-credit shape a node already fronts on the upstream leg of a cache-miss pull (the ramped credit window, [ADR 037](037-regional-proxy-warming.md#adr-037-latency-driven-proxy-warming-for-regional-locality)), and the downstream credit is strictly the cheaper of the two: egress it has already served, versus speculative USDC it pays an upstream provider. On the fused cache-miss serve path a single window bounds both quantities at once, since every pulled chunk is forwarded downstream immediately.

**Node-local policy, floored at one chunk.** `credit_max` and `credit_ramp_divisor` are node configuration, not wire or governance parameters — like the payment quantum they have no on-chain counterpart and are never negotiated. The window is floored at one chunk so a stream can always make progress (deliver a full chunk, then recoup it), and a window at or below one chunk reproduces the stop-and-wait cadence exactly. The self-enforcing threshold generalizes from "pause after one unpaid chunk" to "pause once the unpaid balance reaches the credit window"; the sovereignty guarantee is unchanged, since a node can always choose a smaller `credit_max` (down to one chunk) and pause sooner.

**Takedown latency.** The per-boundary in-flight takedown check ([ADR 011 § On Blacklist Event](011-content-takedown.md#on-blacklist-event)) runs after each collected proof, so widening delivery to a credit window widens the window in which a takedown that lands mid-stream is first observed: the first check falls up to one credit window into the stream (steady state, roughly one chunk, as later chunks recoup one at a time). This is bounded by `credit_max` and floored at one chunk — a stream still ramping up has not yet reached `credit_max`, so its exposure is smaller still — and even a full window of further egress is negligible against the takedown compliance window, so it does not weaken the takedown guarantee. This is why a node with a strict compliance target configures a smaller `credit_max` rather than a larger one.

**Defaults.** `credit_max` defaults to 64 MiB and `credit_ramp_divisor` to 2. `chunk_bytes` is a fixed 1 MiB constant, which is also the window's floor. The ceiling and the quantum move independently because they bound different things: `credit_max` bounds how far the ramp may widen credit exposure and how far delivery may run ahead of payment, while the quantum bounds per-tick message overhead. Accepting a preimage costs one keccak and advances the lane's in-memory index, so a 1 MiB quantum carries neither a signature nor a disk commit of its own. At these values a fully-ramped stream stays link-bound past ~100 ms round trips, where a 1 MiB stop-and-wait cadence would cap it in the low tens of MiB/s.

**Durability is preserved.** The credit window does not weaken the replay guard of [Off-chain voucher state persistence](#off-chain-voucher-state-persistence): the node advances a lane's watermark and chain index in memory as soon as it accepts a proof, before it delivers any further bytes for that lane. Delivering ahead of payment within the window is bounded *credit* risk (the window's worth of unbilled egress), not *replay* risk — the bytes streamed ahead are billed by later proofs, each accepted in turn. There is no per-voucher acknowledgement on the wire; the node's continued delivery is the implicit acknowledgement. A crash can lose a watermark advance the background flush has not yet mirrored to disk, but that loss only widens the frontier an honest stream resumes across — it never lets a node collect an already-redeemed voucher twice.

### Pool solvency and the refundable floor `M`

The contract enforces the per-signer cap, but the cap gives **isolation, not solvency**. The cap stops any one signer from over-drawing; it does not stop the *sum* of signers from exceeding the deposit. The flexibility the pool exists for — one signer draws heavily while others draw nothing — over-provisions the pool (`Σ caps > deposit`), so it can be drawn dry and the last outstanding vouchers eat the shortfall (the **tail**). Solvency is therefore a **node serving policy**, not a contract invariant. The contract is unchanged by any of this — it still just holds the deposit and pays `min(desired, remaining)`.

**The core fact — nodes serve partly blind, but only to *other parties'* vouchers.** A voucher is only certainly backed once redeemed. Before that a node cannot see other nodes' or other lanes' outstanding vouchers against the same pool, so it cannot know the pool is over-committed. A node is never blind to the pool *itself*: it confirms the pool on-chain before it serves.

**Admission confirms the pool on-chain.** Before a node admits the first stream on a pool it has not yet confirmed, it reads the pool's on-chain state once — owner, remaining balance (`deposit − totalRedeemed`), and lifecycle — and caches it. A stream is refused when its pool has no on-chain record, is closed or expired, or already sits at or below the reserved floor `M`. The refusal is retryable: a client that opens or tops up its pool and retries is served. The read is one call on the connection-open path, never the byte path, and a later stream on a confirmed pool reuses the cached state, so a repeat fetch does no on-chain read. A node that cannot reach the chain to confirm an unknown pool refuses that pool and keeps serving the pools it has already confirmed. A node never serves an unconfirmed pool on faith; there is no serve-on-unknown-pool path.

**Capability validity is an admission gate.** A stream carries a pool-owner capability that delegates spend to the stream's voucher signer. The node admits only when the capability's owner signature recovers to the confirmed on-chain owner and the capability is unexpired. On-chain signer registration stays a redemption-time concern, not an admission gate.

**Admission confirms the signer's cap headroom.** A signer's `cap` is shared across every provider ([Deposit Economics](#deposit-economics)). A node reads the signer's on-chain `{cap, spent}` at admission and refuses when a registered signer's `cap − spent` cannot cover a serve floor. A signer that has spent its cap at other nodes is refused here, not served for vouchers the node can never redeem. An unregistered signer has spent nothing on-chain, so it admits on its off-chain capability. The read is one call on the connection-open path, cached briefly, so a repeat fetch does no on-chain read.

**The mechanism — a refundable minimum remaining deposit `M`.** A node stops serving a pool once its on-chain **remaining balance reaches `M`**. The reserved `M` then covers every outstanding in-flight voucher, so **no node is ever stiffed — the tail is structurally zero** — and whatever `M` is not needed **refunds to the owner** at close. `M` is *locked but returned*, not spent, so it can be sized generously at only a temporary-lock cost, never a real loss. That is strictly better than sizing the deposit large to *dilute* the tail (where the tail is a genuine loss you merely make a small fraction of): with `M` the tail is zero and the deposit stays withdrawable — the owner always recovers `≥ M − in-flight`.

**Node logic collapses to three lines:**

1. serve while remaining balance `> M`;
2. batch-redeem a chunk of lanes once its aggregate unredeemed value clears a redemption floor `t`;
3. stop serving at remaining balance `≤ M`.

A chunk is one `redeemMany` transaction. The node packs lanes into chunks and submits a chunk only once the aggregate unredeemed value across that chunk's lanes reaches `t`. Every lane in a submitted chunk settles, so a small (dust) lane rides alongside the larger lanes that cleared the floor. The redeemer also splits a sweep into at most `redeem_max_vouchers_per_tx` vouchers per transaction, so no single chunk exceeds the block gas limit.

The floor `t` is a **gas knob**, not a security parameter: a larger `t` means fewer, fatter redemptions. It is trust-graduated for gas efficiency — a trusted, never-draining pool redeems lazily (large `t`), a fresh pool eagerly (small `t`) — but trust is now purely a gas concern. **`M` is the security parameter.**

**Sizing.** Two tails need two bounds. `M` reserves the **proved-but-unredeemed** tail: every lane's unredeemed value between the moment it crosses its redemption threshold and the moment a node actually redeems. That value includes the chain frontier, not just the signed cumulative, so a lane metering against a live chain can owe up to `MAX_CHAIN_LENGTH × chunk_price` more than its last signature shows. `M` is sized against the extended claim. `M` covers the worst case of that tail, `M ≥ N · t` (fan-out `N` × per-lane threshold `t`). Equivalently `M = k · ρ · B · Δ`, where `ρ` is the price per bandwidth-time (`$0.00125` per Gbps·s at the `$0.01/GB` market rate), `B` is the maximum aggregate bandwidth the pool is defended against (fan-out × per-node throughput), `Δ` is the **detection delay** (how fast nodes notice the balance crossing `M` and stop — chain-dependent; see [Appendix: L2 Deployment](appendix-l2-deployment.md#appendix-production-l2-deployment-target)), and `k ≈ 2` is a safety factor. The per-lane threshold is `t = ρ · V · Δ` for a lane serving at bandwidth `V`. A separate floor-credit accumulator bounds the **un-vouchered floor** tail — bytes a node fronts before a lane sends its first voucher — to `remaining − M` per pool, and per signer to an absolute `k` credit windows of live reservation (see **Per-signer floor isolation** below); the two bounds cover the two tails independently.

**`M` kills both the tail and node-vs-node racing.** Racing existed only because a *draining* pool had insufficient funds for everyone; with `M ≥` in-flight there is always enough to pay every outstanding voucher, so no node eats a shortfall and there is nothing to race for. One refundable reserve resolves both, which is why it replaces the whole dynamic-window apparatus.

**`M` is node policy, not a contract field.** The contract holds the deposit and pays `min(desired, remaining)` regardless; nodes enforce `M` by watching the on-chain remaining balance (`deposit − totalRedeemed`) and refusing to serve past it. A pool MAY declare its expected fan-out as an off-chain hint so the nodes serving it size `M` consistently.

**Mid-stream re-check cadence.** A node re-reads a live stream's pool solvency on a bounded wall-clock cadence, not at every voucher boundary. The credit window bounds delivery ahead of the last *collected* voucher, not ahead of the last *solvency-verified* point, so a shared pool that other lanes drain `remaining` down mid-stream keeps collecting individually-valid but no-longer-redeemable vouchers until the node re-reads solvency. The re-read source refreshes only as the node folds on-chain redemptions, so re-reading it faster than that refresh returns the same value; and per-stream throughput is bounded, so a wall-clock interval `T` bounds worst-case over-delivery on a drained pool to `T ×` the per-stream rate. The node reacts to a mid-stream drain within one interval rather than within one voucher boundary; the widened margin is bounded and the on-chain `redeem` (which pays `min(desired, remaining)`, partial-on-drain) remains the backstop. This interval is a term of the detection delay `Δ` above.

**Mid-stream signer cap re-check.** A node re-reads a live stream's signer cap headroom on the same bounded wall-clock cadence. A signer's `cap` is shared across every provider ([Deposit Economics](#deposit-economics)). The pool solvency re-check above does not catch a drain of one signer's cap: the pool's `remaining` stays healthy on the other signers' budgets while this signer's `cap − spent` falls to zero. So the node tests one more quantity each interval — `held_cap − spent` — where `held_cap` is the cap the node holds on the lane from admission and `spent` is the signer's settled total across every provider. The node reads `spent` from the same folded on-chain redemptions the pool re-check reads, so the check adds no chain call. The node stops the stream when this headroom can no longer cover a serve floor. Without the re-check the node keeps delivering to a signer that can no longer pay, and the loss grows with the blob size — the large-file workload this network serves. The re-check under-counts `spent` for a signer whose pre-admission spend is not yet folded; it then over-states headroom and does not stop, which is the fail-toward-serving direction. This is safe: the admission cap check above already refuses an already-spent signer, so the re-check only catches a drain *after* admission, which the folded deltas show. The on-chain `redeem` (`min(desired, cap − spent)`) remains the backstop, so this re-check only bounds over-delivery; it is not a correctness gate. The bound it holds is the cross-node fan-out residual below, applied per stream rather than closed.

**No per-relationship rate ramp — trust never gates delivery rate.** The security bound is the reserved floor `M`, not a per-relationship earned window. A node does **not** throttle a pool based on its history with that pool. A first-time pool and an established pool follow the same rules. Each stream ramps its own [credit window](#credit-window) from the floor of one chunk as that stream pays. The ramp resets on every new stream; it never carries over from the pool's or signer's past. A fresh stream and an established stream both start at the floor and ramp identically. Trust affects only redemption *cadence* (`t`, a gas concern), never delivery rate. The only caveat is fan-out *width*, not per-stream rate: a fresh pool spread across very many nodes at once can reopen the bounded tail below. We accept this residual; it is never a reason to slow any single stream. This removes the cold-start penalty of a dynamic per-relationship window, which throttled every fresh `(node, pool)` pair until it earned history.

**Per-signer floor isolation.** The un-vouchered floor a signer's in-flight streams hold right now — bytes delivered ahead of payment, released as each stream pays — is bounded per signer as well as per pool. The per-pool ceiling bounds the sum of live floor across every signer to `remaining − M`, the hard solvency envelope and the one bound a free-to-mint signer cannot evade, because an owner spraying fresh signers still sums under it. Under it, one signer's live floor is capped at an **absolute** `k · one window`. This sub-cap protects the **named co-tenants of a shared pool** — the case the pool exists for, one publisher delegating many session keys — from any one key taking the whole live budget. It carries no memory: it is released on payment, so it never penalizes a signer for quitting, and the contract's per-signer `spending_cap` does not do its job (that bounds *settled* spend, and the floor is by definition un-vouchered).

Both bounds are **node policy**, like `M`. Neither needs pool-owner cooperation or a protocol field, because the floor is credit the node itself fronts — bounding it per counterparty is the node bounding its own bad debt. Both throttle **unpaid credit, not paid throughput**: a signer that pays its vouchers releases that stream's live reservation, and a stream already under way is never slowed.

**No per-signer *rate* memory.** The bounds above are on *live* (concurrent) un-vouchered floor; the node keeps no running tally of a signer's past abandonment. Sequential abuse — a key that opens a stream, takes one free floor, disconnects, and repeats — is not gated, because the honest defense against it is not durable: a signer identity is free to mint, so any rate-limit keyed to the signer is reset by rotating the key (the same reason [ADR 008](008-reputation.md#adr-008-reputation-system) keeps no wallet-keyed client ledger). What actually bounds sequential abuse is its **real cost**: each pull is real egress the attacker must draw byte for byte, and a self-dealing puller pays the FeeRouter cut on top. The residual is the slow trickle a patient attacker extracts, and it does not grow with the pool's size or the network's — so it earns no machinery.

**The unit is the credit window, and the live cap is absolute.** A signer's honest need for un-vouchered floor does not scale with the pool's size: every admission reserves at most one ramp-start credit window, and a paying stream releases it as soon as it covers it, so honest need is `concurrent un-vouchered streams × one window` whether the pool holds ten dollars or a hundred thousand. The live cap is therefore `k · one window`, an absolute node-local window count, lower-clamped to one window so a lone signer's first stream always admits on any solvent pool. It is **not** a fraction of the deposit. A fractional share of headroom is loose in both directions on a large pool: one key could hold a share worth millions of windows, and a constant `1/share` keys — about four at a quarter each — would strand the whole floor regardless of pool size, which is exactly wrong for the case a shared pool exists for, one publisher delegating many session keys. The absolute cap instead makes one key's live draw a constant `k` windows and the keys-to-strand count scale with the deposit (`(remaining − M) ÷ (k · window)`).

**Residuals (honest):**

- **Fan-out.** `M` gives a zero tail only up to fan-out `N = M/t`. A pool fanned wider than the `M` it reserves reopens a bounded tail. We accept this residual rather than build machinery against it: each node's exposure to a fresh pool is only its own small redemption threshold `t`. Enlarging the aggregate tail forces the attacker to fan across proportionally many nodes and to physically pull real bytes from each — a real egress cost. Self-dealing is separately taxed by the FeeRouter cut. Sizing `M` for the expected fan-out closes this residual in the common case: honest clients are sticky and do not fan a brand-new pool across hundreds of nodes at once. A node also bounds a second, separate tail: the **un-vouchered floor** bytes it fronts across every stream on one pool before that stream's first voucher lands. A floor-credit accumulator tracks this live, un-vouchered exposure at two levels — per pool (`remaining − M`) and per signer (an absolute live cap of `k · one window`, see **Per-signer floor isolation** above) — and refuses a new admission when either level is full. Both are in-memory: no stream is live at restart, so a restart correctly clears them. On one node, aggregate LIVE un-vouchered floor exposure against one pool therefore never exceeds `remaining − M`, no matter how many concurrent streams draw on it, and no single signer holds more than `k` windows of live floor at once. One residual survives *inside* a pool: a signer identity is free to mint, so an attacker holding the owner key can still reach the pool ceiling by spraying fresh signers. The sub-cap protects the **named co-tenants of a shared pool** — one publisher delegating many session keys — and the pool ceiling remains the bound against identity fan-out. The larger residual that survives is **cross-node fan-out**: each node bounds only its own exposure to the shared `remaining − M`. `K` uncoordinated nodes serving the same pool can together reach up to `K × (remaining − M)` — the same shape of residual this ADR already accepts for the vouchered tail above. Closing it fully needs cross-node coordination the protocol does not have. **Sequential** abuse on one signer — take one free floor, disconnect, repeat — is not separately rate-limited, because a free-to-mint signer defeats any signer-keyed tally by rotation; the bound is the real egress the attacker draws byte for byte, a slow trickle that grows with neither the pool nor the network (see **No per-signer *rate* memory** above).
- **Self-dealing.** A client that also runs a node can serve itself below `M` and recover the deposit rather than spend it on honest service. Structurally unpreventable (no delivery oracle), but **taxed** by the FeeRouter cut — the 30% + 10% non-base legs are unrecoverable, so size the deposit against the recoverable fraction (`D ≥ V_max / cut ≈ 2.5 · V_max`). Escrow must be un-yankable — reclaim only after the close grace window — so the deposit cannot be pulled ahead of a redemption.

### Concurrent Streams

When multiple streams from the **same signer to the same node** run at once, they share that lane's **single chain and cumulative counter** (per `(pool_id, signer, provider)`). Streams to different nodes, or from different signers, are independent lanes. The rules within one lane:

1. **Aggregate byte counter.** The signer tracks total bytes received across all streams in the lane. It releases the next preimage whenever the aggregate crosses the next `chunk_bytes` boundary.
2. **One chain at a time, shared by every stream.** A lane meters against one `chain_root` until it rolls. A stream that joins a live lane is anchored to that root and continues its index; it MUST NOT open a second root ([One chain per lane](#one-chain-per-lane)). The anchor itself is tracked per stream, so a rollover cannot strand a sibling mid-chain.
3. **Proof routing.** Vouchers and preimages both ride on any active stream in the lane. The node credits them against the lane-wide counter whichever stream carries them.

**Example:** A signer has stream A (blob X) and stream B (blob Y) on the same lane. After receiving 1 MiB in total (for example 0.7 MiB from A and 0.3 MiB from B), the signer releases the next preimage on either stream. The chunk size is a protocol constant, so every stream on the lane already agrees on the boundary. There is no per-stream cadence to reconcile and nothing to flush when a stream joins or ends.

A cumulative voucher is self-describing and therefore order-free: a higher `amount` supersedes a lower one, and a stale one is ignored. A preimage is not. It carries no amount, and its worth comes from its depth in the chain, so two rules place it.

**Rule 1 — each stream anchors itself.** A bare preimage does not name its chain, so the node needs a per-stream anchor to place it. Each stream therefore holds its own `(anchor voucher, chain_root, verified, tip)`, seeded when that stream receives a `chain_root` voucher: `verified = 0` and `tip = chain_root`. A preimage that arrives on a stream with no anchor is rejected `UnanchoredPreimage`; because the condition is per-stream it is decidable, which a lane-wide reading would not be.

> **On each stream, the epoch's `chain_root` voucher precedes that stream's preimages for that epoch.**

The payer sends the root voucher at stream open, and at every rollover it sends the new-root voucher on **every** active stream in the lane — not on one stream in the hope that the node propagates it. QUIC orders messages within a stream, so each stream's preimages for the old chain are read against that stream's own old root before that stream's own roll voucher arrives. A fast stream that has already adopted `R₂` cannot invalidate a slower sibling still finishing `R₁`. Nothing needs a rendezvous, and the payer never has to know how far the node has processed any stream.

An at-or-below-watermark voucher is not a rejection: the node treats it as **already satisfied** — it advances nothing and refuses nothing (see [Voucher ordering](#voucher-ordering)). Re-sending the current root voucher on a stream that has it is therefore free.

**Rule 2 — the deepest preimage wins, per stream.** On the anchored stream the node holds the deepest preimage it has verified (`tip`) and that preimage's index (`verified`). On a reveal at `index`:

- `index ≤ verified` — already covered. The node ignores it and hashes nothing.
- otherwise — the node accepts it when `keccak^(index − verified)(preimage) == tip`, then adopts it as the new `tip`.

There is no over-long-index check to make: the wire index is a `u8` and `MAX_CHAIN_LENGTH` is 255, so an index past the end of the chain cannot be encoded. The walk is bounded at 255 hashes by the type, which is a stronger guarantee than a runtime comparison. Placement is by index rather than arrival order, so a fast stream may skip indexes a slower one has not reached.

**The lane's claim is a maximum, never a sum.** With several streams anchored — possibly on different roots across a rollover — the lane's claim is the largest of the highest signed `amount` and, for each stream, that stream's anchor `amount + verified × chunk_price`. It is not a sum: a rollover voucher already folds the retired chain into its own `amount` ([Chain length and rollover](#chain-length-and-rollover)), so adding the streams together would count those chunks twice.

Per-stream anchoring is about *placing a preimage*. It does not shard the money: the lane keeps one chain at a time, one aggregate byte counter, and one credit window applied collectively across every stream ([Credit Window](#credit-window)). Per-stream chains would multiply rollovers and therefore signatures, which is the cost the chain exists to avoid. The payer is one process, so it serializes the chain index and the rollover decision under a local lock while bytes stream concurrently.

See [ADR 005 — Payment lanes and concurrent streams](005-protocol.md#payment-lanes-and-concurrent-streams) for wire-level details.

### Redemption and Close

> **Fee routing model.** A redemption does not skim a fee inline; it forwards the paid amount to `FeeRouter.routeSettlement(operator, bytesDelivered, amount)` in the same transaction. Split details: [FeeRouter Integration](#feerouter-integration).

**Redemption is the settlement primitive.** A node redeems its vouchers on-chain whenever it chooses, while the pool is `Open` or in the grace window. Redemption pays `msg.sender` and is final — it needs no dispute window, because a voucher is a signed, cumulative, monotone claim by a capped signer and a node redeems only its own lane. `redeemMany(batches[])` is the only entry point — one `{poolId, capabilities[], vouchers[]}` group per pool, and a single lane is a one-pool batch of one. Per voucher:

1. **Register the signer once.** On the first redemption for `(poolId, signer)`, verify the owner's signature on `capability` and store `authorized[poolId][signer] = {cap, expiry, spent: 0}`. Every later redemption for that signer omits `capability` and rides the stored registration; a node reads `authorized[...]` by `eth_call` to confirm a signer and its cap without ever holding the capability. The owner-signature check happens once per signer, not per voucher.
2. **Check expiry.** Reject if `block.timestamp >= authorized[poolId][signer].expiry`. This is the only time bound that gates settlement, and it is safe because the node holds `expiry` in advance and stops serving before it (see [Revocation](#revocation)).
3. **Bind the payee, then resolve the claim.** The voucher entry carries no payee field. The contract rebuilds the EIP-712 hash with `msg.sender` as the `provider`, so a voucher signed for another node recovers the wrong signer and reverts `InvalidVoucherSignature`. It then resolves the chain: split `chainMeter` into `chunkPrice` and `chainIndex`, require its reserved 23 bytes to be zero, apply `keccak256` to `preimage` exactly `chainIndex` times, and require the result to equal `chainRoot`, reverting `BadPreimage` otherwise. `chainIndex` needs no range check — it is extracted as a `uint8`, so it cannot exceed `MAX_CHAIN_LENGTH` (255) by construction. The claim is `claimed = cumulative + chainIndex × chunkPrice` over `claimedBytes = bytesDelivered + chainIndex × CHUNK_BYTES`. There is no branch and no optional field here: a sealed voucher (`chainRoot = 0`) at `chainIndex = 0` with a zero preimage satisfies the same check with zeros and resolves to exactly `cumulative`, and a real-root voucher at index 0 does the same with the root as its own preimage. Redemption is cumulative — the voucher carries no nonce.
4. **Compute the payable amount.** With `w = watermark[poolId][signer][provider]`: `desired = claimed − w.amount` (the still-unpaid portion this voucher and preimage authorize), `capRoom = cap − spent`, and `remaining = deposit − totalRedeemed`. Then `paid = min(desired, capRoom, remaining)`. The floor clamp ([Rate-floor enforcement](#rate-floor-enforcement)) applies to `claimed` / `claimedBytes` here — the chain-extended values — and caps the credited byte count without blocking the payment, so bytes proved by preimage carry the same per-byte price obligation as bytes proved by signature.
5. **Pay, or skip if nothing is payable.** If `paid == 0` the voucher is transient-empty: it writes **no** state and moves no funds, and the batch skips it rather than reverting, so the node simply retries later (after a top-up, or with a higher voucher). Otherwise: `bytesDelta = claimedBytes > w.bytesDelivered ? claimedBytes − w.bytesDelivered : 0` (clamped at zero); `bytesPaid = mulDiv(bytesDelta, paid, desired)` (the **paid-proportional** byte count — `== bytesDelta` when `paid == desired`); advance `w.amount += paid`, `w.bytesDelivered += bytesPaid`, `spent += paid`, `totalRedeemed += paid`, and add `paid` / `bytesPaid` to the batch totals the one settlement call carries. The `bytesDelta` clamp settles the money owed even when a claim's `claimedBytes` has not advanced past the lane's paid bytes: the signer already committed the higher cumulative value, so the money is owed and settles, while the byte credit for that redemption is zero and the byte watermark holds — it recovers on a later voucher whose `bytesDelivered` advances again. This keeps the money axis (payment) independent of the served-bytes axis (vote weight) and denies any DoS from a bytes-regressed voucher.

**Partial redemption is retry-safe.** The lane watermark tracks the cumulative amount **actually paid**, not the claim the node presented. When a drained pool covers only part of `desired`, `w.amount` advances by just `paid`, so re-presenting the *same* voucher and preimage after the owner tops up collects the remainder — `desired = claimed − w.amount` is still positive. A node never loses an over-committed voucher to a transient drain; it loses value only if the owner closes and reclaims without ever topping up, bounded by the reserve `M` the node keeps against the pool ([Pool solvency and the refundable floor `M`](#pool-solvency-and-the-refundable-floor-m)). Because payment is `claimed − paid`, replay is automatic: an already-paid or stale claim — a lower `cumulative`, a shallower `chainIndex`, or a superseded chain — computes `paid == 0` and is skipped, moving nothing. The claim is resolved fresh on every call, so the chain needs no anti-replay state of its own and none is stored. `min(desired, capRoom, remaining)` is the solvency backstop — the pool never goes negative, and no signer can cause more than `cap` USDC to leave the pool, since `spent` is the cumulative USDC actually paid on its behalf and `paid` is capped by `cap − spent`.

**The chain walk is fallback-only, not a per-redeem tax.** A sweep redeems the **strongest claim** it holds on a lane — the maximum over the latest signed voucher at index 0 and any retired voucher extended by the deepest preimage the node still holds. In the cooperative path the strongest claim is always a signed voucher: each rollover emits one whose `amount` already folds in the chain it retires, and the close emits the zero-root voucher. Both redeem at `chainIndex = 0` and walk nothing. The un-rolled tail since the last rollover defers to the next rollover or close, and cumulative `claimed − paid` loses none of it. A node walks a preimage only when the payer vanishes mid-chain and the node claims that sub-rollover tail — at most once per abandoned stream, capped at one chain — which is the case where the alternative is collecting nothing. Amortized across all deliveries, the walk adds a negligible amount of gas. What PayWord genuinely adds to every redemption is calldata, not compute; [Chain walk and redemption cost](#chain-walk-and-redemption-cost) prices both.

**Rollover is safe because payment is cumulative — provided the fold is right.** A signer ends a chain by signing the next voucher with `cumulative` set to the frontier that chain actually reached (`amount_old + verified_index × chunkPrice`), plus a fresh `chainRoot`. A node that is offered a voucher folding *less* than the index it has verified refuses it ([Chain length and rollover](#chain-length-and-rollover)), so it never has to hold a retired chain alongside a live one: the lane meters exactly one chain, and the only way a chain is retired is by a signature that already paid for everything it proved. Redemption needs no on-chain sequencing rule either way — payment is `claimed − paid`, so redeeming a superseded chain after a newer one computes `paid == 0` and is skipped.

**The sharded register.** Two mappings, both written lazily on first touch:

- `watermark[poolId][signer][provider] = {amount, bytesDelivered}` — the cumulative USDC and bytes **paid** on that `(signer, node)` lane. Monotone and per-lane: independent accounting, automatic replay-safety (pay = claimed − paid), and `(payer, payee)` attribution. Each node redeems only its own lane, only vouchers naming it.
- `authorized[poolId][signer] = {cap, expiry, spent}` — the signer's registration and running **paid** total across all nodes, so the per-signer cap is enforced in aggregate without iterating lanes.

**Batch redemption.** `redeemMany(batches[])` registers signers and redeems lanes across many pools in one transaction. Each `batches` entry is a `{poolId, capabilities[], vouchers[]}` group: `capabilities` (each a `{signer, spendingCap, expiry, ownerSig}` registration) and `vouchers` (each a `{signer, cumulative, bytesDelivered, r, vs, chainRoot, preimage, chainMeter}` claim — eight words, carrying an EIP-2098 compact signature and the packed `chunkPrice`/`chainIndex` word, applied with the arithmetic above). Within a group the call registers every capability first — verifying the owner signature and storing `authorized[poolId][signer]`, idempotent for an already-registered signer — then redeems every voucher. Separating registration from the voucher decouples the two: a signer is registered by its own capability entry regardless of whether any voucher for it pays, so a skipped voucher never loses a registration.

**Grouping by pool is what makes the per-pool work per-pool.** A pool names itself once for its whole group rather than once per entry, so the pool's status gate is read once, its `totalRedeemed` advances in one write at the end of the group, and it emits one `PoolRedeemed` carrying an entry per lane that paid — however many lanes it carries. Emitting once per group rather than once per lane matters more than it looks: a log costs a base charge plus a charge per topic before it carries any data at all, so a per-lane event would spend most of its gas re-stating the pool and the payee. The group's solvency bound is read once as `deposit − totalRedeemed` and drawn down in a local, so each voucher is bounded by what earlier vouchers in the same group already took and a batch can never overdraw the pool. This matters because a lane is free to open while a pool is not: an adversary funds one pool and spreads dust across many lanes, so the per-lane cost of a batch is what sets the floor on the balance a node can afford to collect. A voucher that would pay `0` — drained pool, already-paid or stale voucher, expired capability, cap reached, or a signer neither registered nor covered by this call's `capabilities` — is **skipped, not reverted**, so one empty lane never sinks the batch; a skipped voucher simply gets no `lanes` entry, and the call returns the total paid. **The batch settles once.** Every voucher is redeemed for `msg.sender`, so every voucher that pays pays the same payee. The loop advances the lane watermarks and accumulates the amounts and paid-proportional byte counts, then makes a single `FeeRouter.routeSettlement(msg.sender, totalBytes, totalPaid)` call for the whole batch — not one per voucher. An all-empty batch skips that call entirely, because the router rejects a zero amount. The three-bucket split therefore rounds once per batch instead of once per voucher, which moves at most one base unit per bucket and never changes what the pool pays or what a lane records. Only a structurally-invalid entry — a bad voucher signature (which is also how a voucher signed for another node surfaces), a bad capability owner-signature, a preimage that does not hash to `chainRoot`, a non-zero reserved span in the packed `chainMeter` word, or a chain-extended claim that does not fit `uint64` — reverts the whole call, since that is caller error, not transient pool state. Each entry carries its own walk, so a batch's walk cost is the sum of its entries' `chainIndex` values, and a batch of settlement vouchers walks nothing at all.

**Close and reclaim.** Redemption pays nodes; close returns the owner's unspent remainder. There is no adversarial close: the owner submits no vouchers on any node's behalf, so a close can never understate a node's earnings, and no third-party dispute path is needed. `closePool(poolId)` (owner only) sets status `Closing` and starts the grace window (`disputeDeadline = block.timestamp + disputeWindow`, default 48 hours). Nodes may still redeem during `Closing`. After `disputeDeadline`, `reclaim(poolId)` transfers `deposit - totalRedeemed` to the owner and sets status `Closed`. A node that has not redeemed by the deadline forfeits its outstanding vouchers. A pool does not expire, so a node's only deadlines are the grace window (after an owner close) and each signer's capability `expiry`; a diligent node redeems within its capability-bounded serving window, so this is the node's own cash-flow choice, not a theft surface.

**Served-bytes and vote weight are unaffected.** Every batch forwards the sum of its paid-proportional byte counts to `FeeRouter.routeSettlement`, which increments `bytesPerEpoch[operator][epoch]` — the trailing-window served-bytes accumulator read by `DecdnGovernor._getVotes` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight). Redemptions still fire the same routing call, and counting only paid bytes keeps per-byte revenue and governance vote weight both derived from real, floor-priced client USDC even in the drained-pool case. Faking bytes does not increase revenue (the operator base is per-byte, paid by the pool), and over-declared capacity does not translate into governance influence.

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

When a node rejects a payment proof off-chain — a `Voucher` or a `ChunkPreimage`, before any gas would be spent — the rejection is returned **in-band** mid-stream as a `StreamError` message carrying `VoucherRejected { reason }` (per [ADR 005 § Stream Lifecycle State Machine](005-protocol.md#stream-lifecycle-state-machine), this transitions the stream `Streaming → Failed` cleanly without a QUIC stream reset). Proof validation can only fire after at least one `Voucher` or `ChunkPreimage`, necessarily after `StreamResponse { ok: true }` — so payment rejections never use the initial-response error path that delivery-side failures (`NotFound`, `Overloaded`, etc.) take. Full reason enum and per-reason retry semantics: [ADR 005 § VoucherRejected semantics](005-protocol.md#voucherrejected-semantics).

Some reasons map to an on-chain redemption revert the node avoids by rejecting early; others are the node's own off-chain ordering guards that on-chain would simply pay `0` (redemption is cumulative):

| `VoucherRejectReason` | Off-chain trigger | On-chain behaviour |
|---|---|---|
| `BadSignature` | Malformed signature bytes | EIP-712 `SignatureChecker` would revert at redemption (see [EIP-712 Voucher Signature](#eip-712-voucher-signature)) |
| `WrongSigner` | Signature recovers to an address other than the voucher's `signer` | redemption would revert when the recovered signer ≠ the authorized `signer` for `poolId` |
| `WrongPool` | `voucher.pool_id` mismatch | EIP-712 domain binds the voucher to a specific `poolId`; off-pool vouchers authorize nothing |
| `WrongProvider` | `voucher.provider` names another node | redemption rebuilds the signed hash with `msg.sender`, so another node's voucher recovers the wrong signer and reverts; a node cannot redeem another node's lane (see [Redemption and Close](#redemption-and-close)) |
| `AmountRegression` | `voucher.amount ≤` the node's accepted cumulative | none on-chain (redemption pays `cumulative − paid`, so a stale voucher pays `0`) — the sole off-chain ordering/replay guard now that vouchers carry no nonce; keeps the node's own ledger consistent |
| `BytesRegression` | `voucher.bytes_delivered <` the node's accepted cumulative bytes | as for `AmountRegression`, on the byte axis |
| `SpendingCapExhausted` | the voucher would push the signer's paid total past `cap` | redemption caps `paid` at `cap − spent` from `authorized[poolId][signer]` — a fully-uncoverable voucher pays `0` and is skipped |
| `CapabilityExpired` | the signer's capability has passed its `expiry` | redemption gates on `expiry` from `authorized[poolId][signer]` — an expired capability pays `0` and is skipped |
| `PoolExhausted` | the pool's remaining deposit, minus the refundable floor `M` and already-committed concurrent floor credit across every signer, can no longer fund further credit | none on-chain — a node-side mid-stream solvency stop, not a voucher defect. The per-signer live cap never reports this reason: it refuses at admission, before the node signs a success response, as a wire-level `NotFound` |
| `BadPreimage` | the released value does not hash to the lane's verified tip in `index − verified` steps | redemption reverts `BadPreimage` — a preimage that does not reach `chainRoot` is caller error, not transient state |
| `ChainIndexZero` | `index == 0` on the wire — index 0 is the redeem-time settlement case and carries no chunk | none on-chain — `chainIndex = 0` is the *normal* settlement path and never reverts. The index is a `u8` on the wire and a `uint8` extracted from `chainMeter` on-chain, so `index > MAX_CHAIN_LENGTH` cannot be encoded at either layer and needs no runtime check. The reserved span of the packed word is a different matter: redemption reverts `ChainMeterReservedNonZero` on a non-zero byte there ([Voucher signatures are compact, and their signers are EOAs](#voucher-signatures-are-compact-and-their-signers-are-eoas)) |
| `UnanchoredPreimage` | a preimage arrived on a stream before that stream carried the epoch's `chain_root` voucher | none on-chain — the node cannot name the chain the reveal belongs to, so it cannot verify it (see [Concurrent Streams](#concurrent-streams)) |
| `ChunkPriceMismatch` | the voucher's `chunk_price` is not the node's quoted `rate_per_mb` (metering vouchers), or is non-zero on a sealed voucher | none on-chain — the contract settles whatever price the signer signed, so the node must refuse the under-priced voucher before it meters against it |

Surfacing these reasons off-chain saves both parties the gas of a doomed on-chain submission and gives the payer enough detail to recover (refresh state and re-sign for `AmountRegression`, raise the cap for `SpendingCapExhausted`, issue a fresh capability for `CapabilityExpired`, top up the deposit for `PoolExhausted`, resend the epoch's root voucher on that stream for `UnanchoredPreimage`) instead of an opaque connection drop. A delegated signer — one issued a capped capability by a pool owner — holds no funds to `topUp` (owner-only) and cannot read its lane watermark from chain until a redemption records it, so the node attaches an authenticated watermark bundle to the regression rejections for self-heal, and defers a genuine top-up or cap raise to the owner (see [ADR 005 § `VoucherRejected` semantics](005-protocol.md#voucherrejected-semantics)). Riding in-band rather than via a QUIC stream reset preserves the reason for client retry logic without burning [ADR 013](013-schema-evolution.md#adr-013-schema-evolution) application-error-code numbers for the structured-response case.

## Consequences

### Positive

- One pool is opened once and reused. On-chain cost is one open plus occasional top-ups, independent of how many nodes it pays and how many signers it delegates — many clients collapse to one pool plus lazy per-`(signer, provider)` slots. A node serves on the first byte with no open round-trip on the fetch path.
- USDC denomination gives node operators predictable unit economics: delivery revenue covers infrastructure costs without exposure to TOKEN price movements
- The voucher is the payment receipt; the BLAKE3 hash is the delivery receipt. Together they provide mutual protection: the signer doesn't sign a voucher or release a preimage for bytes that fail hash verification; the node stops delivering if proofs stop arriving
- A meter tick costs one keccak instead of one secp256k1 signature. The payer signs once to open a 255-chunk chain and once to close it, so a multi-gigabyte transfer needs a handful of signatures rather than one per interval, and the node needs no acknowledgement, no round trip, and no durable write between chunks
- The meter is four times finer than the interval it replaces (1 MiB, not 4 MiB), which tightens the withholding bound at the same time as it removes the per-interval signature
- Maximum risk per chunk (1 MiB) is $0.00001 at market rate ($0.00001/MB) — negligible. At the ceiling rate ($0.001/MB), worst-case risk is $0.001 per chunk — small against the reserve `M` a node keeps against a pool
- One fungible deposit backs every node, versus one earmarked deposit per node — capital-efficient, and wallet-less signers (ephemeral keys the owner delegates a capped capability to) never touch the chain
- Market-driven rate setting means replication happens organically: profitable content gets cached by more nodes, driving prices down without any coordination protocol
- The `PaymentPool` contract is functionally separated from the `CapacityBond`, keeping the audit surface for each contract's core logic bounded

### Negative

- Solvency is not a contract invariant. The per-signer cap gives isolation, not solvency; keeping `Σ spending ≤ deposit` on an oversubscribed pool relies on nodes reserving the refundable floor `M` (see [Pool solvency and the refundable floor `M`](#pool-solvency-and-the-refundable-floor-m)) — honored by self-interest (a node serving past `M` risks stiffing itself), but node behavior, not a contract guarantee. The node-side floor accumulator adds its own per-signer share underneath that pool bound, but it covers only un-vouchered credit and is likewise node policy, not a contract guarantee.
- The tail is structurally zero for bounded fan-out: the reserved `M` covers all in-flight vouchers. It reopens only past fan-out `N = M/t` — a bounded residual we accept (each node risks only its threshold `t`, and enlarging the tail costs the attacker real bandwidth); self-dealing is separately bounded by `M` and taxed by the FeeRouter cut.
- Revocation is future-only. Settlement of rendered service can never be voided, so revocation is a short-TTL `expiry` plus a node-side stop-serving signal, not an on-chain switch that invalidates earned vouchers (see [Revocation](#revocation)).
- Close and reclaim finalize per `(signer, provider)` lane over the grace window rather than on one cumulative number — a sharding of the existing regular close.
- A released preimage is a **bearer proof**: it names no payee, so a payer that reuses one `chain_root` across lanes pays twice for one tick. The defence is a payer-side MUST — an independent, well-seeded random root per lane. Neither a node nor the contract can check it: the node sees one lane, and the contract never holds two vouchers together (see [Cross-lane preimage spend](#cross-lane-preimage-spend))
- The signed voucher carries two more fields (`chainRoot`, `chunkPrice`) and redemption carries two more parameters (`chainIndex`, `preimage`), but `chunkPrice` and `chainIndex` share one packed word, so a lane widens from 5 words to **8 / 256 B** rather than 9. On the common closing path every added byte is zero, so the addition compresses to almost nothing and a cooperative redemption stays near the baseline; the bounded walk is confined to mid-chain abandonment
- The meter cannot go finer than 1 MiB, because USDC's 6 decimals cannot price a smaller tick. The limit is the token, not the hash
- Clients must hold the payment token and the chain's native gas currency to use the network; this adds an onboarding step compared to a single-currency model. Gas abstraction is deferred to production (see [Deposit Economics](#deposit-economics)).
- Rate volatility: a node can change its advertised rate between a probe and a stream request; the `StreamResponse` rate is the binding one, but a client that probed at one rate and receives a higher rate in `StreamResponse` must disconnect and re-probe rather than having been deceived silently. Rate changes more than 30 seconds after the probe are not slashable; the 30-second window is precisely defined as `stream_response.timestamp_us >= probe_response.timestamp_us && stream_response.timestamp_us - probe_response.timestamp_us < 30_000_000` using requester-anchored timestamps in both signed messages (see [ADR 005](005-protocol.md#adr-005-wire-protocol))
- USDC is issued by Circle, which can freeze specific addresses or blacklist the contract. This counterparty risk is accepted: the payment token is fixed to USDC at deployment and the protocol does not implement payment-token substitution

## Attack Vectors

### Client-side

#### Voucher withholding

Client receives bytes but stops releasing proofs, getting content for free up to the last accepted preimage.

The self-enforcing stop is sufficient. Maximum loss is one chunk (1 MiB): at market rate (~$0.00001) it is negligible, and at the ceiling rate (~$0.001) it is still negligible relative to pool deposits. The chunk is the meter's resolution and the *floor* of the credit window, not the node's exposure limit: a fully-ramped lane can front up to `credit_max` (64 MiB by default) before it pauses, and that window is the real withholding bound. What the finer chunk buys is a tighter floor — a stream at the floor now risks 1 MiB rather than 4 MiB.

The stop is per-lane and mechanical. The node pauses the moment the unpaid balance on a lane reaches that lane's ramped [credit window](#credit-window) — `min(credit_max, max(chunk_bytes, paid / credit_ramp_divisor))` computed from the lane's own paid total — and waits for the next proof on the same lane — a deeper preimage or a higher-cumulative voucher; it never fronts more than one window against one lane. The pause keys on the lane, not the counterparty. A client that stalls or crashes mid-stream and reconnects is treated no differently from a first request: it resumes on the same lane under the same ramped window and pays only for the bytes it proves, never for the interrupted window. The node's own cost of the interruption — re-reading a held blob, or dropping and later re-pulling an unfinished cache-miss fill — is bounded to that one window either way, and no black mark follows the client into its next request. No history accumulates against a client across connections, and none needs to: a fresh signer from the same party starts its own lane at the one-chunk floor and ramps up only as that lane itself pays, so it is bounded the same way. The bound is a property of each lane and its own paid total, so a new key buys an attacker nothing it did not already have — a fresh signer has zero `paid` and therefore starts back at the floor, not at whatever window an old signer had earned.

That bound applies to a *funded* lane. A request whose pool cannot cover even the first credit window is refused before the node signs a success `StreamResponse`, so it is never served the free chunk at all — the seller-side pre-flight deposit guard of [ADR 037 § Implementation status](037-regional-proxy-warming.md#implementation-status), which fronts both the cache-miss and the direct-serve paths.

The per-lane bound is one chunk, but the maximum **free floor** a node ever fronts against one pool is bounded per pool, not per lane: a floor-credit accumulator caps the sum of every stream's live un-vouchered exposure against that pool to `remaining − M`, and caps any one signer's live draw to `k` windows, so opening many lanes against the same under-funded pool does not multiply the free bytes an attacker collects, nor let one signer take the whole pool's free budget from its co-tenants (see [Pool solvency and the refundable floor `M`](#pool-solvency-and-the-refundable-floor-m)).

A node rejects a voucher for one of three distinct reasons, and reports which one. `PoolExhausted` means the pool's on-chain remaining balance cannot cover the stream mid-flight; the owner must top up the deposit, and the node signals this only mid-stream, after it has already accepted the stream (see [Off-chain Voucher Rejections](#off-chain-voucher-rejections-wire-encoding)). `SpendingCapExhausted` means the signer's delegated cap is used up; the owner must raise the cap. `CapabilityExpired` means the signer's capability has passed its `expiry`; the owner must re-mint a fresh capability. An open-time refusal — before the node has committed to serving — stays the wire-level `NotFound` used for any unservable request, so a probing client cannot distinguish "no such content" from "this pool cannot pay," which is the anti-enumeration property this ADR relies on elsewhere. `PoolExhausted` is the one reason that necessarily surfaces mid-stream and post-auth, because only a live stream can drain a pool below its floor while service is already underway.

#### Owner reclaims before a node redeems

There is no stale-close vector: the owner submits no vouchers on any node's behalf, so a close cannot understate a node's earnings. The only residual is timing — the owner closes the pool and reclaims the remainder before a node redeems its outstanding vouchers.

The grace window covers this if the node is online: `closePool` starts a window (default 48 hours, sized above the force-inclusion delay; see [L2 sequencer censorship](#l2-sequencer-censorship) below) during which any node may still redeem, and only after the window does `reclaim` return the remainder to the owner. **Node defenses:**

1. **Periodic redeem sweep.** A node redeems its outstanding vouchers on a fixed interval, capped well inside the grace window (at most 6 hours against the 48-hour floor), so a pool that closes between sweeps is still swept — many times over — before the owner can `reclaim`. The sweep's cadence needs no close event: any interval short of the window is sufficient, and the shorter default keeps earnings flowing regardless of closes. It does fold `PoolCloseInitiated` into its solvency view, so once a pool is `Closing` the sweep skips a lane whose pool is drained or already past the dispute deadline rather than spending gas on a `redeemMany` the contract would reject as `PoolClosed`. A node does not force sub-threshold dust through the sweep — a lane that never earned enough to justify its own redemption gas is left for the owner to reclaim rather than redeemed at a loss.
2. **Expiry margin.** A node stops serving a signer before the capability's `expiry`, so it always holds redeemable vouchers with time to redeem — the same discipline that makes expiry-based revocation safe (see [Revocation](#revocation)).

A node offline for the full grace window forfeits its unredeemed vouchers; this is a node-operations responsibility, not a protocol gap.

#### Probe fishing

Client sends probe requests to many nodes at high frequency to map the network or exhaust node resources without ever paying.

Per-NodeId rate limiting alone is bypassable: clients are not bonded, NodeIds are free to rotate, and iroh connection setup is cheap. The mitigation is the layered token-bucket rate limit in [ADR 005 § Probe rate limiting](005-protocol.md#probe-rate-limiting): per-peer (NodeId) plus per-IP plus a global node cap, applied before any signature or hold-slot allocation. The per-IP layer raises the cost of bulk probing because IP rotation requires money (proxies, IPv6 delegation, cloud bills) while NodeId rotation does not; the global cap is defence in depth.

**Note:** Probe responses are considered public information (see [ADR 005](005-protocol.md#adr-005-wire-protocol)). The concern here is resource exhaustion from bulk probing, not information leakage — content availability is discoverable via probing (see [ADR 005](005-protocol.md#adr-005-wire-protocol)), and pricing is revealed in probe/stream responses by design.

#### Pool oversubscription (one deposit backs many nodes)

One deposit deliberately backs vouchers to many nodes — that is the point of the pool. So the owner (or its signers) can commit more voucher value than the deposit covers, and a node that serves against an over-committed, drained pool is paid less than its voucher.

This is bounded, not a double-spend hole:

- **The contract never overpays.** Redemption pays `min(desired, capRoom, remaining)`, so the pool never goes negative; the sum of all payouts never exceeds the deposit. A drained pool pays partially and the remainder stays claimable after a top-up, so the node is not forced to forfeit a voucher to a transient drain.
- **The per-signer cap isolates signers.** No signer can commit past its `cap`, so one compromised or greedy signer cannot drain the whole pool.
- **The node bounds its own exposure.** A node stops serving a pool once its remaining balance reaches the reserved floor `M`, which covers all outstanding in-flight vouchers, so the tail is zero (see [Pool solvency and the refundable floor `M`](#pool-solvency-and-the-refundable-floor-m)).
- **Self-dealing is taxed, not free.** An owner that redeems to its own node still pays the FeeRouter cut on every cycle, and the deposit is un-yankable until the grace window or `expiry`, so it cannot be pulled ahead of a redemption.

### Node-side

#### Data withholding

Node accepts a stream request, receives a voucher, then stops delivering bytes.

Self-enforcing: the node cannot extract more payment than the last acknowledged voucher. The client resumes from `byte_offset` on a different node.

#### Cross-lane preimage spend

A signer reuses one `chain_root` across two providers. Every preimage it releases to node A is then a valid extension of node B's voucher, so B claims chunks it never delivered.

**The payer causes it, and the payer pays for it.** A preimage is a bearer proof: it names no payee, and its binding comes entirely from the voucher whose root it satisfies. Under a shared root, A hands preimage `k` to B, and B redeems at index `k` having delivered a fraction of those chunks. The signer pays for chunks it never received, bounded only by `MAX_CHAIN_LENGTH × chunk_price` per extra lane and ultimately by its own `cap`. This removes the zero-exposure property of the [Credit Window](#credit-window).

**No node can see it, and the contract cannot either.** A node sees only its own lane and cannot know another voucher shares its root. The contract never holds two vouchers together, so both claims are valid under their own signatures. The defence is therefore structural and payer-side: every chain opens on its own random seed, so a preimage's bearer scope is exactly the one node already entitled to it ([One chain per lane](#one-chain-per-lane)).

**There is no node-side backstop, by design.** A node that accepts a reused root is the party the reuse *pays*, so a rejection rule protects nobody who would choose to run it — and enforcing it needs an unbounded, never-expiring set of every root the node has ever seen. The protocol therefore states no such rule and defines no reject reason for it. The payer's seed derivation is the whole defence, which is honest about where the loss falls: on the payer.

**The reverse direction is not an attack.** Presenting one node's voucher at another node achieves nothing. The root is a signed field of a voucher that names `provider`, and the contract rebuilds the hash with `msg.sender`, so a node holding a voucher that names someone else recovers the wrong signer. It rejects the stream with `WrongProvider` rather than serving.

#### Corrupted delivery

Node serves bytes that don't match the advertised BLAKE3 hash.

Absorbed at the wire by progressive BLAKE3 verification at the client (mandatory in `cdn/client/v1` per [ADR 002](002-content-addressing.md#adr-002-content-addressing) and [ADR 005](005-protocol.md#adr-005-wire-protocol)). Vouchers are signed and sent only after the corresponding chunks have been verified — a corrupt window therefore yields no voucher. The client drops the connection, requests the blob from a different node, and recovers any unspent channel funds via channel-close. **Client monetary loss in the corruption case is zero**; the only cost is downstream bandwidth (sunk regardless of outcome).

No on-chain slash machinery is needed for content corruption. The threat is bounded in framing parallel to [§ Voucher withholding](#voucher-withholding) above: per-encounter wasted bandwidth is capped by the ramped credit window on each side, and at the floor by one chunk (the client's downstream cost for a corrupt chunk; the node's upstream cost when a correctly-withheld proof leaves the chunk unpaid). Both bounds are per-lane and mechanical — the same self-enforcing credit-window pause applies, keyed to the lane rather than the counterparty. Clients prefer nodes whose probe and delivery history they trust; the protocol coordinates neither side.

#### Rate bait-and-switch

Node advertises a low rate in probe responses then returns a higher rate in `StreamResponse`.

**Resolved: slashable offense.** Both responses are signed over the advertised rate ([ADR 005](005-protocol.md#adr-005-wire-protocol)); a same-NodeId signed pair where `StreamResponse.rate_per_mb > ProbeResponse.rate_per_mb` and the requester-anchored timestamp delta is under 30 seconds is on-chain-verifiable evidence. Clock-skew immune (both timestamps originate from the requester's clock; the node echoes them back in its signed response). The slash schedule lives in [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn); see [ADR 014 § Slash Signatures — secp256k1 EIP-712](014-on-chain-verification.md#slash-signatures--secp256k1-eip-712) for the on-chain verifier.

#### Redemption front-running

A node monitors the mempool and front-runs an owner's close with its own redemption.

Not an attack. A node redeeming its latest voucher before the owner reclaims is the intended happy path — redemption is provider-only and pays only up to the voucher the signer signed. Fabricating a higher voucher requires forging the signer's signature, which is cryptographically infeasible, and the per-signer cap bounds the total either way.

#### Third-party forced close (DoS)

A third party tries to force a pool from `Open` to `Closing` to disrupt it, or redeems a lane it does not own.

**Resolved by access control.** `closePool` is owner-only, so a third party cannot start the grace window. redemption rebuilds the signed voucher hash with `msg.sender`, so a node can only redeem vouchers naming it — a third party cannot redeem another node's lane even holding the voucher. On-path voucher interception is mitigated by QUIC transport (TLS 1.3); it does not address endpoint compromise.

#### Redemption while the pool is Open (no dispute window)

A node redeems accrued funds while the pool is still `Open`. This is the ordinary settlement path, not an attack.

**Safe by construction.**

- **No dispute window is needed because there is nothing to dispute.** Redemption pays a **signed** cumulative voucher from a capped signer, verified against the authorized `signer` for the pool. The node can never draw more than the signer committed, never past the signer's `cap`, and never past the pool balance. A stale lower voucher simply pays `0` (it is already covered by the lane's paid cumulative). No counterparty submits a competing number, so there is nothing an offline party would need a window to counter.
- **Residual: the signing key is the blast radius, capped.** A compromised signer key can authorize claims up to its `spending_cap`, redeemable with no window to intervene. The cap and the pool balance bound the loss; a short-TTL `expiry` plus non-renewal retires the key (see [Revocation](#revocation)). Delegating capped, expiring capabilities confines the loss to one signer's cap rather than the owner's whole balance.
- **No regression or double-spend.** The lane watermark tracks cumulative **paid** amount and only increases, as do `spent` / `totalRedeemed`. Redemption pays `cumulative − paid`, so re-submitting a voucher pays only what is still owed and an already-paid voucher pays `0`; `FeeRouter.routeSettlement` is purely additive, so each paid byte and USDC unit is counted exactly once across a lane's redemptions.
- **Owner-refund safety.** The owner reclaim is always `deposit − totalRedeemed ≥ 0`. Redemptions never trap owner funds or pay out more than the deposit.
- **Governance-weight timing.** Bytes are stamped into `bytesPerEpoch` in the epoch each redemption lands. A node's choice of *when* to redeem shifts epoch attribution slightly, but this is no stronger than the settle-timing flexibility operators already have, the total served-bytes count is unchanged, and the count still reflects only real client-paid bytes — so it introduces no new wash-trading or vote-weight vector ([ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight); [ADR 016 § wash-trading](016-contract-interactions.md#tunable-economics)).

### Network-level

#### Eclipse attack

Attacker surrounds a client with malicious nodes so all probe responses come from nodes under attacker control.

BLAKE3 verification catches data corruption regardless of node-set composition; the remaining DoS variant (attacker-controlled peer set refuses to serve) is resolved in [ADR 012 § Bootstrap and Trust Model](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model): production uses multi-source bootstrap (on-chain registry + hardcoded DNS seeds) so an attacker must compromise both to fully eclipse a client; minimum honest-peer diversity is a supplementary client-side policy.

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

A node bonds, responds to probes with `has_blob: true`, but refuses to serve — collecting credibility in clients' registry-derived node views without actually participating.

**Withholding is not a slashable offense** — operators may legitimately take content offline for maintenance, migration, or business reasons, and slashing for availability creates perverse incentives. The protocol does not guarantee availability; publishers who want fault tolerance opt into it by seating multiple operators, and the network deprioritizes flaky nodes through reputation:

- **Publisher-chosen operator sets.** Content owners hold a publisher identity ([ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces)). Governance vets the publisher wallet once via the standard timelock path, and the vetted publisher then seats origin operators per namespace itself ([ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)). Set size is the publisher's call — a single trusted operator works for hobbyist publishers, multi-operator sets defuse single-point withholding for publishers who want it. Content served under namespace 0 has no authorized origins — it is served best-effort from cache/DHT only ([ADR 002 § Namespace 0](002-content-addressing.md#namespace-0)).
- **Reputation fast-path.** Nodes that respond `has_blob: true` to probes but fail to deliver accumulate reputation penalties at a steeper rate. A node with consistently poor availability is deprioritized in provider selection and loses delivery revenue. Publishers may use the reputation signal as input when seating or unseating operators for their namespace.

Note: the probe-triggered eviction hold ([ADR 005](005-protocol.md#probe-triggered-eviction-hold)) addresses a related but distinct problem. Withholding is a node that has the blob but refuses to serve it (behavioral — handled by reputation). The eviction hold addresses a node that advertised `has_blob: true` but lost the blob to cache pressure before the stream request (mechanical — kept resident by the hold so the follow-up pull succeeds).

#### Replay attack on vouchers

Attacker intercepts a signed voucher and attempts to replay it against a different pool, a different node, or after redemption.

EIP-712 typed data over `{poolId, signer, provider, amount, bytesDelivered, chainRoot, chunkPrice}` binds the voucher to a specific pool, signer, and payee. `provider` is what stops one node from redeeming a voucher meant for another: it is signed but not sent, and the contract substitutes `msg.sender` when it rebuilds the hash. The EIP-712 domain separator (see [EIP-712 Voucher Signature](#eip-712-voucher-signature)) further binds each voucher to a specific chain and contract deployment, preventing replay across different chains, contract upgrades, or test vs production environments. The contract settles in a single token fixed at deployment, so a voucher can only ever pay in that token — cross-token replay does not arise, and the voucher carries no token field. Resubmission after redemption is neutralized by the cumulative accounting: redemption pays `claimed − paid`, so an already-redeemed voucher — at any chain index it has already been paid for — pays `0` and moves no funds. A released preimage is a bearer proof and carries no binding of its own, but it is worth nothing outside the lane whose voucher names its root, and reusing one root across lanes is the payer-side failure analysed in [Cross-lane preimage spend](#cross-lane-preimage-spend).

#### Off-chain voucher state persistence

The on-chain protections in [Replay attack on vouchers](#replay-attack-on-vouchers) constrain only what the contract accepts at redemption. They do not prevent the **delivering node** from re-delivering bytes off-chain for a voucher it already honoured: a node holding voucher state only in memory will, after restart, re-accept any earlier (lower-cumulative) voucher the signer (or any wire observer) resubmits and serve the bytes again.

A node holds each lane's voucher watermark in memory. The watermark is `(last_amount, last_bytes_delivered, chain_root, verified_index, tip)`, tracked per `(poolId, signer, provider)` lane. The last three fields are the chain state: the root the lane is currently metering against, the deepest index the node has verified under it, and the **preimage bytes** at that index. Storing `tip` is what makes the frontier redeemable — only the payer can produce a value at a given depth, so a node that kept the index alone would hold an unprovable claim after a restart, in exactly the abandonment case the chain exists to cover. Per-stream anchors ([Concurrent Streams](#concurrent-streams)) are in-memory only; a restart drops the streams, and the durable record keeps the strongest claim's chain state. The node MUST advance a lane's watermark as soon as it accepts a voucher or a preimage for that lane, before it delivers any further bytes for the lane. An absent entry is semantically identical to a never-seen lane (`last_amount == 0`). A record exists once the node has accepted the lane's first voucher. The node drops an entry only after it redeems the lane's full cumulative and observes the redemption on-chain.

The node mirrors the in-memory watermark to disk on a background timer, at a configurable interval. A crash between two flushes loses at most one flush interval's advance of the **frontier** — the un-redeemed watermark growth since the node's last on-chain redemption of that lane. Frontier loss is safe. Losing chain state loses at most the chunks metered since the lane's last signature: the node forfeits value it had already earned, which is the safe direction, and the payer's next voucher folds that frontier back into a signed `amount` on the next rollover or close. An honest signer's next voucher carries a higher cumulative than the node's stale on-disk watermark, so the node resumes forward from that voucher and re-serves nothing for free. A replay of the lost vouchers is still redeemable on-chain, because the on-chain cumulative accounting never lost anything; re-accepting and re-serving a replayed voucher still earns the node payment when it redeems.

The node MUST flush the store to disk before every on-chain redemption. This floors the **redeemed** watermark: after a crash, the on-disk cumulative is at least the value the node already submitted for redemption, so the node never re-serves already-redeemed bytes for free.

A failed background flush is a metric, not a voucher-acceptance failure. The node keeps the unflushed state buffered and retries the flush on the next tick; it does not reject the voucher that triggered the write and does not stop delivering.

A separate, rarer fault can occur when the node tries to advance the in-memory watermark itself. This fault (a rare internal error) MUST NOT deliver any further bytes for that voucher: the node aborts the stream cleanly instead, with no in-band voucher-rejection reason — the `AmountRegression` / `SpendingCapExhausted` codes are NOT appropriate substitutes, since they would tell the signer to refresh state or ask for more headroom when in fact the same voucher should be retried unchanged. The client resends the same voucher on a fresh stream rather than treating the clean abort as a permanent failure.

No positive voucher acknowledgement travels the wire. Voucher acceptance is implicit: the node's continued delivery is the acknowledgement. Only a rejection is signalled.

Storage backend and trait shape are implementation concerns; the Rust implementation exposes a `PoolStateStore` seam in `crates/incentive` with a `redb`-backed persistent implementation in `crates/node`. The protocol fixes only the frontier/redeemed durability split above.

## Contract Interfaces

### PaymentPool

The `PaymentPool` contract holds payment-token pools. The payment token is USDC; its address is fixed at deployment as an immutable constructor argument. A pool is one funded deposit that backs vouchers from many capped signers to many nodes. A two-dimensional sharded register records per-signer authorization and per-`(signer, provider)` redemption watermarks.

**Pool state:**

```solidity
struct Pool {                 // two slots
    address owner;            // funder: deposits, receives the reclaim, owns the poolNonce sequence
    uint8   status;           // 0 = Open, 1 = Closing (grace window active), 2 = Closed
    uint64  disputeDeadline;  // set when close is initiated; fixed for the grace window
    uint64  deposit;          // total escrowed, USDC base units (6 decimals)
    uint64  totalRedeemed;    // cumulative USDC paid out across all lanes; remaining = deposit − totalRedeemed
}

struct Authorization {        // authorized[poolId][signer] — one slot
    uint64  cap;              // per-signer spending cap, from the owner-signed capability
    uint64  expiry;           // capability expiry; gates redemption
    uint64  spent;            // cumulative USDC actually paid on this signer's behalf across all providers
}

struct Lane {                 // watermark[poolId][signer][provider] — one slot
    uint64 amount;           // cumulative USDC actually paid on this lane (monotone)
    uint64 bytesDelivered;   // cumulative bytes actually paid on this lane (monotone)
}

// Set once on first redemption for the signer (owner signature verified there).
mapping(bytes32 => mapping(address => Authorization)) public authorized;
// Per (pool, signer, node) redemption lane.
mapping(bytes32 => mapping(address => mapping(address => Lane))) public watermark;
```

The chain adds no storage. `chainRoot`, `chainIndex`, and `preimage` are calldata the contract resolves and discards; only the paid watermark persists, so a lane still occupies one slot and no chain state is ever stored on-chain.

`claimed` and `claimedBytes` are the exception to the width argument below. Both are *derived* from calldata (`cumulative + chainIndex × chunkPrice`), not read from a signed `uint64`, so neither is bounded by construction. Resolution happens in `uint256` and the claim is bounded by an explicit check rather than by the field width. Redemption reverts `ClaimOverflow` when either derived value does not fit `uint64`.

**Every USDC counter and both lane watermarks are `uint64`.** At USDC's six decimals that ceiling is about $18.4 trillion, roughly four hundred times the token's entire supply, and 18.4 exabytes for a byte count — so the width bounds nothing a real pool reaches, while each struct fits in a single storage slot. The values are bounded by construction rather than by a check: `spent <= cap`, `totalRedeemed <= deposit`, and both watermarks only ever advance toward a presented `uint64` cumulative — with the derived `claimed` / `claimedBytes` pair the one exception noted above, which is checked rather than constructed. The EIP-712 typehashes still declare `uint256`, because a narrower field hashes to the same 32-byte word — so the widths change no signature a client produces.

Both register mappings are written lazily on first touch, so an inactive `(signer, provider)` pair costs no storage. The pool header carries no per-payee or per-signer field — those live in the register. "The lane watermark" refers to `watermark[poolId][signer][provider]`, whose `.amount` and `.bytesDelivered` are the cumulative USDC and bytes **paid** on the lane and advance monotonically. Vouchers carry no nonce — on-chain redemption is purely cumulative (see [Voucher ordering](#voucher-ordering)).

**Roles.**

- **`owner` — the funder.** Transfers the deposit in, receives the `deposit − totalRedeemed` reclaim, owns the `poolNonce` sequence the `poolId` derives from, is the only address that may `topUp` and `closePool`, signs capabilities, and is the address the [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting) takedown and blacklist gates evaluate.
- **`signer` — a delegated voucher authority.** Authorized by an owner-signed capability up to `cap` until `expiry`. Node-agnostic: one capability spends at every node. The owner may be its own sole signer (the single-user case) or delegate many capped signers.
- **`provider` — a delivering node.** The payee named in the voucher's signed payload, the only address that can redeem it (the contract rebuilds the hash with `msg.sender`), and the recipient of every routed payout.

There is no pinned per-channel `voucherSigner`. Signers are authorized off-chain by capability and registered lazily on first redemption. Because a capability carries a `spending_cap` and an `expiry`, a compromised or delegated signer is bounded by its cap and retired by its expiry — the revocation an immutable pin could not give (see [Revocation](#revocation)).

**Pool ID:** `poolId = keccak256(abi.encodePacked(owner, poolNonce))` where `poolNonce` is a monotone per-owner counter stored as `ownerPoolNonce[msg.sender]`. Neither `provider` nor `signer` is an input — a pool is bound to no payee and no signer. **Ordering:** `openPool` reads the current nonce, computes `poolId`, then increments. The owner pre-computes the next `poolId` off-chain by reading `ownerPoolNonce[owner]` before opening.

> **Terminology:** `poolNonce` (the pool creation counter) is distinct from the voucher `nonce` (the monotone per-lane sequence number in EIP-712 voucher signatures). The former identifies pools; the latter orders vouchers within a `(signer, provider)` lane.

| Group | Function | Purpose |
| --- | --- | --- |
| Nonce | `ownerPoolNonce(owner) → uint256` | Per-owner monotone counter used in `poolId` derivation. |
| Lifecycle | `openPool(deposit) → poolId` | Open a pool; derives `poolId` from the current `ownerPoolNonce[msg.sender]` then increments it; escrows `deposit`, whose credited amount must meet `minDeposit` ([Minimum deposit](#minimum-deposit)); emits `PoolOpened`. Names no provider and no signer. |
| Lifecycle | `topUp(poolId, additionalDeposit)` | Owner-only: add funds to an open pool. |
| Lifecycle | `redeemMany(batches[])` | The sole redemption entry point; a single lane is a one-pool batch of one. Per `{poolId, capabilities[], vouchers[]}` group: gate the pool's status once, register each capability (idempotent — an already-registered signer is a no-op), then redeem each voucher for `msg.sender`, paying `min(desired, capRoom, remaining)` — the still-unpaid portion, capped by the signer's remaining cap and the pool balance still unclaimed by earlier vouchers in the same group. The two arrays are independent: a node registers the signers it needs and redeems all its lanes in one transaction, and a skipped voucher never affects a registration. **Skip** (do not revert) any voucher that would pay `0` — drained pool, stale/already-paid voucher, expired capability, cap reached, or a signer neither registered nor present in `capabilities`. Each voucher resolves its claim first: `claimed = cumulative + chainIndex × chunkPrice` after a bounded walk verifies `preimage` against `chainRoot` ([Chain walk and redemption cost](#chain-walk-and-redemption-cost)). Reverts only on a bad voucher signature (which is also how a voucher signed for another node surfaces), a bad capability owner-signature, `BadPreimage`, `ChainMeterReservedNonZero`, or `ClaimOverflow`. Routes the batch total through `FeeRouter` in one same-tx call and returns the total paid. No dispute window. See [Redemption and Close](#redemption-and-close). |
| Lifecycle | `closePool(poolId)` | Owner-only: start the grace window so nodes may redeem outstanding vouchers before reclaim; sets status `Closing`; emits `PoolCloseInitiated`. |
| Lifecycle | `reclaim(poolId)` | After the grace window: transfer `deposit − totalRedeemed` to the owner; sets status `Closed`; emits `PoolReclaimed`. Callable by anyone; the refund always goes to `owner`. |
| View | `getPool(poolId) → Pool` | Read the on-chain `Pool` struct. |
| View | `getAuthorization(poolId, signer) → Authorization` | Read a signer's `{cap, expiry, spent}`, so a later node confirms a signer and its cap without holding the capability. |
| View | `getRateBounds() → floor` | Current `deliveryFloor` in payment-token base units. |
| View | `minDeposit() → uint64` | Current minimum credited `openPool` deposit in payment-token base units; `0` = dormant ([Minimum deposit](#minimum-deposit)). |
| View | `feeRouter() → address` | The `FeeRouter` target, `immutable` and fixed at construction ([ADR 026](026-tokenomics.md#adr-026-tokenomics)). |
| Governance | `setDisputeWindow(seconds)` | Grace window (bounded 172800–259200 — 48h–72h). |
| Governance | `setRateBounds(floor)` | Per-MB delivery-rate floor in payment-token base units. Capped at `MAX_RATE_PER_MB`. |
| Governance | `setMinDeposit(newMin)` | Minimum credited `openPool` deposit; bounded [0, 100_000_000] ($100 ceiling); `0` returns the knob to dormant. Emits `MinDepositUpdated`. |

Bucket shares (60/30/10) are governed on `FeeRouter`, not on `PaymentPool`; the treasury share (10%) is configured on `FeeRouter`.

#### Immutable settlement router

`feeRouter` is `immutable`. The constructor takes the router address and fixes it for the contract's life. `PaymentPool` has no setter for it. `FeeRouter` records the `bytesPerEpoch` vote-weight feed that `DecdnGovernor` reads, and `DecdnGovernor.feeRouter` is `immutable` too. A re-pointable pool router could sever that feed and permanently brick governance, so both ends bind to one router at construction.

The constructor validates the router. It reverts on `address(0)`, on a router whose code size is zero (EOA or undeployed address), and on a router that does not expose the `paused()` view. A code-present router that later proves wrong surfaces at the next redemption, not at deploy. A router bug now needs a `PaymentPool` redeploy, which the immutable-Governor design already assumes.

> **Reentrancy protection:** All state-mutating functions that perform external calls (ERC-20 transfers) — `openPool`, `topUp`, `redeemMany`, `reclaim` — MUST use `nonReentrant` guards and follow checks-effects-interactions. `redeemMany` additionally crosses the `FeeRouter` boundary, so the effects (lane watermark, `spent`, and `totalRedeemed` advances) MUST be committed before the `safeTransfer` + `routeSettlement` interaction — a batch commits *every* entry's effects before its one settlement call.

**`topUp` behavior:** `topUp(poolId, additionalDeposit)` adds funds to an open pool:

- **Status precondition:** MUST require status `Open` (reverts on `Closing` or `Closed`).
- **Caller:** owner only (`require(msg.sender == pool.owner)`).
- **Effects:** Transfers `additionalDeposit` from `msg.sender` to the contract via `safeTransferFrom`. Updates `pool.deposit += additionalDeposit`.
- **Modifiers:** `nonReentrant`.
- **Emits:** `PoolToppedUp(poolId, additionalDeposit, newDeposit)`.

**Redemption behavior:** `redeemMany(batches[])` pays a node against monotone vouchers **while the pool is `Open` or in the grace window**. It is safe without a dispute window because a voucher is a signed, cumulative claim by a capped signer and a node redeems only its own lane. The residual is the signing key itself — a compromised key can authorize up to its `spending_cap`, retired by the capability's `expiry` (see [Redemption while the pool is Open](#redemption-while-the-pool-is-open-no-dispute-window)).

- **Status precondition:** status `Open` or `Closing` with `block.timestamp < disputeDeadline` (a node may redeem during the grace window). Reverts on `Closed`.
- **Payee:** `msg.sender`. A voucher entry carries no payee field; the contract substitutes `msg.sender` when it rebuilds the signed hash, so one node cannot present another's voucher.
- **Pool:** named once per group, not per entry. The status gate runs once for the group, before its registrations, so a closed pool rejects the whole call rather than registering signers against a pool that can no longer pay.
- **Register each signer once.** For each `capabilities` entry whose `authorized[poolId][signer]` is unset, verify the owner's signature (an EIP-712 `Capability` over `{signer, spending_cap, poolId, expiry}` recovered against `pool.owner`; see [EIP-712 Voucher Signature](#eip-712-voucher-signature)), then store `{cap: spending_cap, expiry, spent: 0}`. An entry for an already-registered signer is a no-op, so a stray capability can neither raise the cap nor extend the expiry.
- **Voucher validation.** Verify `voucherSig` against `signer` over `{poolId, signer, msg.sender, cumulative, bytesDelivered, chainRoot, chunkPrice}` (see [EIP-712 Voucher Signature](#eip-712-voucher-signature)); require `block.timestamp < authorized[poolId][signer].expiry`.
- **Claim resolution.** Split `chainMeter` into `chunkPrice = uint64(chainMeter >> 8)` and `chainIndex = uint8(chainMeter & 0xff)`, and revert `ChainMeterReservedNonZero` unless the reserved upper 23 bytes are zero. Then hash `preimage` forward `chainIndex` times and require the result to equal `chainRoot` (revert `BadPreimage`). The index needs no separate bound check: extracting it as a `uint8` caps it at `MAX_CHAIN_LENGTH` (255) by construction, the same bound the one-byte wire index carries. The reserved-span check is the only structural check the packed word adds. Then `claimed = cumulative + chainIndex × chunkPrice` over `claimedBytes = bytesDelivered + chainIndex × CHUNK_BYTES`, and the [rate floor](#rate-floor-enforcement) clamps that pair's credited bytes without blocking payment. A sealed voucher (`chainRoot = 0`, `chainIndex = 0`, zero preimage) resolves to exactly `cumulative` through the same check. Redemption is cumulative; the voucher carries no nonce.
- **Effects (checks-effects-interactions):** per group, read `remaining = deposit − totalRedeemed` once. Per voucher, let `w = watermark[poolId][signer][msg.sender]`; compute `desired = claimed − w.amount`, `capRoom = cap − spent`, and `paid = min(desired, capRoom, remaining − groupPaid)` where `groupPaid` is what earlier vouchers in this group already drew; if `paid == 0` the entry is skipped with no state written. Otherwise compute `bytesDelta = claimedBytes − w.bytesDelivered` and `bytesPaid = mulDiv(bytesDelta, paid, desired)`; set `w.amount += paid`, `w.bytesDelivered += bytesPaid`, `spent += paid`, and accumulate `paid` into `groupPaid` and `bytesPaid` into the call totals. At the group's end `totalRedeemed += groupPaid` in one write. After every group, `safeTransfer(feeRouter, totalPaid)` and call `FeeRouter.routeSettlement(msg.sender, totalBytes, totalPaid)` once, in the same transaction; a batch that paid nothing skips the call. `routeSettlement` is purely additive (`+=`); `bytesPaid` counts only paid bytes (`== bytesDelta` when the pool is solvent), so served bytes never outrun paid USDC and each paid byte and USDC unit is counted exactly once. Advancing `w` by `paid` (not to `claimed`) is what makes a partially-paid draw retriable — re-presenting the same voucher and preimage after a top-up collects the rest.
- **Modifiers:** `nonReentrant`.
- **Emits:** one `PoolRedeemed(poolId, provider, lanes[])` per group that paid, where each `lanes` entry is `{signer, newPaidCumulative, bytesPaid}`. A group that paid nothing emits nothing.

Redemption changes nothing about off-chain voucher exchange. Signers keep sending cumulative vouchers up to their cap, and the node keeps accepting them; the node reads the authoritative on-chain lane watermark (`w.amount`, the cumulative paid) and redeems the strongest claim it holds against it — the latest signed voucher, extended by a preimage only when no later signature supersedes it. Deciding *when* to redeem is node operational policy, bounded only by the capability `expiry` and the grace window.

**Tracking owed vs. paid.** A node tracks the *paid* side by **consuming events, not by polling**. It subscribes to `PoolRedeemed` filtered on its own `provider` address (indexed) and, as each event arrives — including the ones its own `redeemMany` transactions emit — walks its `lanes` array and sets each named lane's paid cumulative to that entry's `newPaidCumulative`. Carrying the cumulative rather than the amount paid is what makes the stream idempotent: a replayed or duplicated event writes the same value, and a missed one is repaired by the next. That event stream is the single write path for the paid side, so a drained-pool partial pay is recorded exactly; the node never assumes its voucher cleared. It follows `PoolToppedUp` (indexed by `poolId`) the same way, to re-drive a lane that a dry pool left `owed > paid`. On startup or after a gap it reconciles like every other chain watcher — enumerate `PoolRedeemed` for its address from a pinned block, then tail live, resyncing on a missed range — so the watermark is never reconstructed by guesswork. *Owed* is the strongest claim the node holds per lane — the highest accepted voucher cumulative, extended by any deeper preimage it has verified (`amount + verified_index × chunk_price`), persisted for replay-safety ([Off-chain voucher state persistence](#off-chain-voucher-state-persistence)). Counting only the signed cumulative would understate a lane by up to one whole chain. `unredeemed = owed − paid`, summed across a pool's lanes, is the in-flight value the reserved floor `M` must cover, and what keeps a lane pending until it is fully collected. A read-call (`getPool` → `deposit − totalRedeemed`; `getAuthorization` → `cap − spent`) is used only as an optional pre-flight to skip submitting a redemption that would pay nothing against a dry pool — never to track paid state. All of this survives a restart: paid is rebuilt from the event log, owed from the persisted per-lane store.

#### Chain walk and redemption cost

Redemption resolves a claim before it pays it, so the question is what that resolution costs relative to the pre-PayWord voucher. Two costs are separable: the walk, and the calldata.

**The walk is bounded and rare.** The contract always hashes forward from the submitted preimage to `chainRoot`. It never hashes from a stored intermediate, so it keeps no chain state and needs no root-matching branch. The cost is `chainIndex` keccaks, capped at `MAX_CHAIN_LENGTH` — about 16k gas at full depth, and zero at `chainIndex = 0`. A node reaches full depth only when a payer abandons a stream mid-chain: every rollover and every close emits a fresh signed voucher that folds the exhausted chain into `amount`, and those redeem at index 0. A finalized delivery therefore never walks.

**The calldata is flat and mostly zero.** `chainRoot` and `preimage` ride on every redemption whether it walks or not, and the packed `chainMeter` rides with them — three words, not four, because `chunkPrice` and `chainIndex` share one ([Voucher signatures are compact, and their signers are EOAs](#voucher-signatures-are-compact-and-their-signers-are-eoas)). On the compressing L2 calldata that dominates cost here, a **closing** voucher carries a zero root, a zero preimage, a zero index, and a small `chunkPrice` — nearly all zero bytes, which compress to almost nothing. A non-zero `chainRoot` costs 32 real bytes and appears only when a node redeems a mid-transfer rollover voucher, which it usually need not: it can redeem once at or after the close.

**Net.** A cooperative, finalized redemption costs approximately the pre-PayWord baseline. The walk and the non-zero-root calldata are confined to the mid-chain abandonment path, which is the path where the alternative is collecting nothing. Keeping `chain_length` and `chunk_bytes` out of the signed voucher ([EIP-712 Voucher Signature](#eip-712-voucher-signature)) offsets part of the flat calldata addition.

#### Initial deployment values

The constructor takes `(usdc, capacityBond, feeRouter, disputeWindow, deliveryFloor, minDeposit, admin)` per [ADR 016 § Contract Inventory](016-contract-interactions.md#contract-inventory). Every governable parameter it exposes is a constructor argument; there are none it defaults. `disputeWindow` is a constructor argument validated against the hardcoded safety bounds (deployment default 48h — see the bounds table below and [ADR 009](009-governance.md#adr-009-governance-model) for governance ranges). A pool does not expire, so it configures no duration parameter. The constructor MUST reject any zero address among `(usdc, capacityBond, feeRouter, admin)` and a `feeRouter` whose code size is zero (EOA / undeployed address).

Default deployment value for `disputeWindow` (the redemption grace window): **172800 seconds (48 hours)** — sized to guarantee a node time to redeem under sequencer censorship (see [§ L2 sequencer censorship](#l2-sequencer-censorship) below). Safety bounds per [ADR 009](009-governance.md#adr-009-governance-model): 172800–259200 seconds (48h–72h). Default deployment value for `minDeposit` (the minimum credited `openPool` deposit): **0** — the Sybil floor ships dormant and governance arms it post-deploy via `setMinDeposit` (see [Minimum deposit](#minimum-deposit)). Under [ADR 026](026-tokenomics.md#adr-026-tokenomics) the constructor carries no `feePercentage` / `discountedFeePercentage` / treasury-address parameters; bucket shares are governed on `FeeRouter`, and the treasury bucket is one of `FeeRouter`'s three buckets (see [FeeRouter Integration](#feerouter-integration)).

#### L2 sequencer censorship

An owner (or colluding sequencer) calls `closePool` and ensures a node's redemption transactions are censored for the full grace window, so the owner can `reclaim` the remainder before the node is paid. Counterparties fall back to L1 forced inclusion, but this takes up to ~24 hours on Arbitrum (similar paths on other OP-Stack chains). If the grace window is no longer than that delay, the node's forced-included redemption lands too late.

**Mitigation — baseline grace window.** Censorship resistance comes solely from keeping the baseline grace window above the chain's maximum force-inclusion delay: the window default is **48 hours** (172800 seconds), which guarantees at least 24 hours of effective redemption time on any chain with a force-inclusion delay ≤ 24 hours. There is no on-chain forced-inclusion detection or deadline extension — a signed force-included redemption is indistinguishable on-chain from a sequencer-included one, so the window itself carries the guarantee. The setting is chain-agnostic and the governance floor equals the 48h default (bounds 48h–72h per [ADR 009](009-governance.md#adr-009-governance-model)), so the baseline can only be tightened upward and never dropped below the force-inclusion delay. A node also keeps a margin below the capability `expiry` (see [Revocation](#revocation)), so it is not depending on the grace window alone.

#### Events

All events use indexed `poolId` plus an indexed actor field where applicable.

| Event | Emitted by | Non-indexed fields |
| --- | --- | --- |
| `PoolOpened(poolId, owner, …)` | `openPool` | `deposit` |
| `PoolToppedUp(poolId, …)` | `topUp` | `additionalDeposit, newDeposit` |
| `PoolRedeemed(poolId, provider, …)` | `redeemMany` | `lanes[]`, one `{signer, newPaidCumulative, bytesPaid}` entry per lane the group paid: `newPaidCumulative` is that lane's cumulative paid amount after the redemption, `bytesPaid` its paid-proportional byte count |
| `PoolCloseInitiated(poolId, owner, …)` | `closePool` | `disputeDeadline` |
| `PoolReclaimed(poolId, owner, …)` | `reclaim` | `ownerRefund` (= `deposit − totalRedeemed`) |
| `RateBoundsUpdated` | `setRateBounds` | `newDeliveryFloor` |
| `MinDepositUpdated` | `setMinDeposit` | `oldValue, newValue` |

`PoolOpened` indexes `poolId` and `owner`, so an owner lists its pools via `eth_getLogs(topics=[PoolOpened, *, paddedOwnerAddress])`; an indexer keys on `poolId`. `PoolRedeemed` indexes `poolId` and `provider`, so a node filters on its own `provider` address to follow every lane it is paid on without reading anything else; `signer` rides in the `lanes` array instead of a topic, which is what lets one event carry a whole group.

An owner reconciling its pools after a restart reads its own `ownerPoolNonce` and recomputes each `poolId` (ids are derived per nonce, so there is no separate counter to drift), then re-hydrates each via `getPool`. A node does not enumerate pools from chain state — a pool names no provider — so it reconstructs its lanes from its own persisted `PoolStateStore` ([Off-chain voucher state persistence](#off-chain-voucher-state-persistence)) and confirms each on-chain via `watermark[poolId][signer][provider]`. A signer learns the `poolId` and its cap from the capability the owner issued it.

`PoolReclaimed` carries no `protocolFee` field, and redemption does not skim a fee inline. The bucket distribution emits its own events from `FeeRouter` (see [FeeRouter Integration](#feerouter-integration)).

**No pool expiry.** A pool has no lifetime cap and never expires; it is opened once and reused indefinitely. The money layer carries **no max-duration parameter** and no time-based reclaim — fund recovery is owner-initiated regular close (`closePool` then `reclaim` after the grace window), on demand, never time-triggered. Only **capabilities** carry an `expiry`, a short TTL that serves revocation (see [Revocation](#revocation)); the pool itself does not. Owner funds are therefore never stranded, and a node's serving window is bounded by each signer's capability `expiry` — the deadline redemption enforces per lane.

**Close and reclaim lifecycle:**

- `closePool(poolId)` → requires status `Open`. **Owner only** (`require(msg.sender == pool.owner)`). Sets status to `Closing`, sets `disputeDeadline = block.timestamp + disputeWindow`, emits `PoolCloseInitiated`. No fund transfers. It only starts the grace window; it moves no node's earnings, so it needs no voucher and cannot understate a lane.
- `redeemMany` → still callable while `Closing` and before `disputeDeadline`, so a node cashes outstanding vouchers after the owner closes. See [Redemption behavior](#paymentpool).
- `reclaim(poolId)` → requires status `Closing` and `block.timestamp >= disputeDeadline`. Callable by any address; transfers `deposit − totalRedeemed` to `pool.owner`, sets status `Closed`, emits `PoolReclaimed`. The router is not called — payouts already happened at each redemption.

**Safety bounds (hardcoded):**

| Parameter | Minimum | Maximum |
| --- | --- | --- |
| Grace window (`disputeWindow`) | 172800 seconds (48 hours) | 259200 seconds (3 days) |
| Rate floor | 1 base unit | `MAX_RATE_PER_MB` (1000) |
| Minimum deposit (`minDeposit`) | 0 (dormant) | 100_000_000 base units ($100) |

`PaymentPool` does not hold a fee-percentage parameter. Bucket-share bounds (60/30/10 with per-share bounds 40–90 / 5–50 / 0–30) are owned by `FeeRouter` per [ADR 026 § Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds).

**The rate floor is in USDC base units (6 decimals) per MB.** The contract stores `deliveryFloor`, the per-byte price floor **enforced at redemption** (see [Rate-floor enforcement](#rate-floor-enforcement) below). It is a soft floor and never a quote gate: a node advertises whatever rate it configures, and a sub-floor rate still settles. There is no governance ceiling either — the buyer sees the signed rate in `StreamResponse` before it pays, so it protects itself by rejecting a rate it finds too expensive. The absolute upper bound is the wire constant `MAX_RATE_PER_MB` = 1000 base units per MB (~$1/GB, ~100× the expected market rate; [ADR 005](005-protocol.md#adr-005-wire-protocol)), which honest requesters reject above. The ceiling sits near real prices on purpose: a floor set close to it clamps credited bytes toward zero, so a far-above-market ceiling would let governance suppress ADR-036 vote-weight accrual, while a realistic one still catches below-market bytes and can never zero out the electorate's weight.

**Initial rate floor:**

| Parameter | Value (USD/MB) | USDC base units | Rationale |
| --- | --- | --- | --- |
| `deliveryFloor` | $0.000001/MB | 1 | Anti-abuse minimum; 10× below expected market rate. **Enforced at redemption as a soft floor** — a voucher whose cumulative `amount / bytesDelivered` falls below this floor still settles its `amount`, but the contract clamps the byte count it credits to `amount × BYTES_PER_MB / deliveryFloor`, so credited served bytes always cost proportional USDC. Prevents the served-byte vote-weight inflation of [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight), while imposing no practical constraint on legitimate pricing (nodes set rates well above it; the floor is an anti-inflation safeguard, not a recommended price). |

The expected market rate is $0.00001/MB (10 USDC base units per MB, or $0.01/GB). This positions deCDN ~4–9× cheaper than major traditional CDNs (CloudFront at $0.085/GB, KeyCDN at $0.04/GB) and at parity with budget providers (Bunny.net at $0.01/GB). The floor is governance-tunable from day one within the hardcoded safety constraints above — admin-key-gated in the PoC, DecdnGovernor in production (see [ADR 009](009-governance.md#adr-009-governance-model)). Node pricing is otherwise a market outcome: nodes compete on the rate they advertise, and a node that overprices loses selection ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)).

### Rate-floor enforcement

The delivery floor is a **soft floor**. Its job is to stop cheap bytes from inflating the served-bytes metric, not to police payment. So redemption never rejects a sub-floor voucher. It settles the voucher's **cumulative** `amount` in full and clamps the byte count it credits.

The floor sets a per-byte price. A cumulative `amount` justifies at most `amount * BYTES_PER_MB / deliveryFloor` bytes of delivery credit (`BYTES_PER_MB = 1_048_576`, [ADR 005](005-protocol.md#adr-005-wire-protocol)). Redemption credits the smaller of the claimed bytes and that ceiling:

```
creditedBytes = min(bytesDelivered, Math.mulDiv(amount, BYTES_PER_MB, deliveryFloor))
```

The lane's byte watermark advances by the credited delta, and only the credited bytes reach `FeeRouter.routeSettlement`. `deliveryFloor >= 1` keeps the divisor non-zero. The `min` runs at full width and is bounded above by the `uint64` `bytesDelivered`, so the credited value fits `uint64`. The clamp uses **zero tolerance** — the floor sits 10× below the expected market rate, so honest traffic stays under the ceiling and credits every delivered byte (the off-chain 1% tolerance applies to the *advertised* `rate_per_mb`, not this floor).

This binds credited bytes to real USDC: crediting `B` bytes requires cumulatively paying `>= B / 1_048_576` base units at the floor price. That restores the proportional-cost assumption [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) relies on. A voucher cannot decouple a large `bytesDelivered` from a tiny `amount`: the excess bytes earn no credit. The bound holds at the one place it matters — the served-bytes counter that feeds governance vote weight — while a voucher priced below the floor still pays for the bytes the node served.

The cumulative `amount` needs no separate bound. Credit is proportional to the USDC actually routed (`bytesPaid = mulDiv(bytesDelta, paid, desired)`), so a partial-drain payment credits only the bytes its money buys at the floor price. Overpaying for few bytes credits the honest, sub-ceiling byte count and moves only the payer's own USDC through the fee split; it inflates nothing. The floor is a price on vote weight, not a gate.

The node never mirrors the floor on the wire. It advertises its configured `rate_per_mb` verbatim and never raises its own quote to the floor. It checks each voucher pays that advertised rate for the bytes the voucher covers, which protects its own per-delta revenue. It does **not** gate a voucher on the floor. The floor is redemption-time contract state; the node reads it for no wire decision. A voucher whose cumulative dips below the floor still settles its cumulative, and redemption clamps only its byte credit. So a node that quotes below the floor keeps serving and keeps its money; it loses only vote-weight credit on the sub-floor bytes. Raising the floor never obliges a node to raise its price.

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

`PaymentPool` MUST invoke `FeeRouter.routeSettlement(address operator, uint256 bytesDelivered, uint256 amount)` in the same transaction as the payment-token `safeTransfer` to the router, passing the *paid* amount and the *paid-proportional* byte count (`bytesPaid = mulDiv(bytesDelta, paid, delta)`, so a partially-paid draw counts only the bytes it paid for). `redeemMany` passes the batch totals in one call, since every voucher in a batch is redeemed for the same payee. The router pays the operator's 60% base share in that transaction, dispatches the 30% / 10% same-tx legs, derives the current epoch as `uint64(block.timestamp / EPOCH_LENGTH)`, and increments `bytesPerEpoch[operator][epoch]` by that paid-proportional byte count as the trailing-window served-bytes accumulator read by `DecdnGovernor._getVotes` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) as the governance vote-weight source. The router's `+=` accounting makes the per-redemption deltas sum to each lane's cumulative with no double-counting. The full `IFeeRouter` interface is canonical in [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model).

#### Redemption-path invariants

1. **Atomic base-share payout.** The 60% base share MUST land in the operator's wallet in the same transaction as each redemption — no claim step, no keeper, no off-chain queue. This is the Case A cashflow guarantee from [ADR 026 § Operator economics](026-tokenomics.md#operator-economics), realizable incrementally per redemption.
2. **No reentry.** `redeemMany` holds a `nonReentrant` guard for the duration of the router call.
3. **Routed deltas partition each lane's cumulative.** Each redemption routes a distinct, non-overlapping delta of the same lane, advancing the monotone watermark, so the router sees no overlap and each byte and USDC unit is counted exactly once.

Conservation and same-tx three-bucket invariants (60/30/10) live with the router itself in [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split) / [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model). The `Settled` event (operator + epoch + per-bucket deltas) is emitted by the router; full event set is in [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model).

#### Node-to-node redemptions (no router bypass)

**Node-to-node cache-miss paid pulls route through `FeeRouter` identically to client-to-node redemptions.** When node B pulls a blob from origin-backed node A and pays from its own pool, node A's redemption is not special-cased: it routes through the same `_route` → `FeeRouter` path as a client-to-node redemption, with no node-aware branch, and the paid amount takes the same three-bucket 60/30/10 split per [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split). There is no route-skipping entry point and no detect-and-skip branch; the only routing conditionals (paused-router defer) are party-agnostic.

A router bypass is not available, even though routing node-to-node payments "double-charges" the same downstream bytes (once when B pays A, again when B's clients pay B). Uniform routing keeps the contract surface minimal and is exactly what makes the structural wash-trading deterrent hold — a self-routed pool pays the 40% non-base skim (30% burn + 10% treasury) on every cycle (see [ADR 036 § Wash-trading as vote-buying](036-served-bytes-voting-weight.md#wash-trading-as-vote-buying)).

- All `PaymentPool` redemptions forward to `FeeRouter.routeSettlement` regardless of whether the counterparties are operators or end clients. Node-to-node bytes therefore **do** accumulate in the router's per-epoch byte counters and **do** count toward governance vote weight ([ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) sources vote weight from `FeeRouter.bytesInWindow`), bounded by the per-operator vote cap and the [ADR 036 § Wash-trading as vote-buying](036-served-bytes-voting-weight.md#wash-trading-as-vote-buying) cost model.
- The on-chain registry distinction — a pool is node-to-node when both the `owner` and the redeeming `provider` addresses have a registered NodeId binding (see [NodeId-to-Ethereum Binding](#nodeid-to-ethereum-binding)) — still exists, but it drives **probe-acceptance priority** ([§ Admission and Priority](#admission-and-priority)), not routing. Redemption is routed the same way either way.
- Redemption and settlement events are public, so node-to-node self-routed-traffic / wash-trading patterns are observable on-chain by anyone, feeding governance threshold-tuning — reinforcing, not replacing, the per-cycle skim cost.

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

The capability names no provider — it is node-agnostic, valid at every node. Redemption recovers it against `pool.owner` and stores `{cap: spendingCap, expiry, spent: 0}` in `authorized[poolId][signer]`.

**Voucher type** (signer-signed, node-addressed):

```solidity
bytes32 constant VOUCHER_TYPEHASH = keccak256(
    "Voucher(bytes32 poolId,address signer,address provider,uint256 amount,"
    "uint256 bytesDelivered,bytes32 chainRoot,uint256 chunkPrice)"
);
```

`signer` and `provider` are both in the signed payload: `signer` binds the voucher to the authorized key redemption validates against, and `provider` binds it to a single payee, so one node cannot redeem another node's voucher — it is signed but not sent, and the contract substitutes `msg.sender` when it rebuilds the hash. `amount` and `bytesDelivered` are the lane's cumulatives, and the settlement anchor a chain extends. `chainRoot` heads that chain, or is `0` for a sealed voucher; `chunkPrice` is signed so the claim arithmetic is fixed at signing time. It does not make redemption independent of governance: `redeem` reads the live `deliveryFloor` with no per-lane snapshot, so a floor raise between signature and redemption clamps the credited bytes at the new floor while the payment still settles ([Rate-floor enforcement](#rate-floor-enforcement)). `chain_length` and `chunk_bytes` are **not** signed: both are protocol constants ([Chunk Cadence](#chunk-cadence), [Chain length and rollover](#chain-length-and-rollover)), so signing them would pay calldata for values every party already holds. The typehash declares `chunkPrice` as `uint256` and keeps it there: redemption carries it inside the packed `chainMeter` word, decodes it, and widens it back to `uint256` before rebuilding the struct hash, so the packing is invisible to every signer and changes no digest. Adding two signed fields is a Tier-3 change under [ADR 013 § Signed Field Freezing](013-schema-evolution.md#signed-field-freezing); pre-deployment it lands as one straight wire cut with no compatibility shim. The voucher carries no epoch: per-operator-epoch attribution is derived by `FeeRouter.routeSettlement` at redemption time as `epoch = uint64(block.timestamp / EPOCH_LENGTH)`, so a redemption's bytes credit the epoch its transaction lands in. A node redeems whenever it chooses within the capability `expiry` (the per-signer redemption deadline) and the grace window, so the practical bound on epoch-shifting is that `expiry` — a second-order effect on emission share that shrinks as the active-operator set grows.

**Signature digests:**

```solidity
bytes32 voucherDigest = keccak256(abi.encodePacked(
    "\x19\x01",
    DOMAIN_SEPARATOR,
    keccak256(abi.encode(VOUCHER_TYPEHASH, poolId, signer, provider, amount, bytesDelivered, chainRoot, chunkPrice))
));

bytes32 capabilityDigest = keccak256(abi.encodePacked(
    "\x19\x01",
    DOMAIN_SEPARATOR,
    keccak256(abi.encode(CAPABILITY_TYPEHASH, signer, spendingCap, poolId, expiry))
));
```

**Verification:** The **capability** is verified with OpenZeppelin's `SignatureChecker.isValidSignatureNow(pool.owner, ...)`, which transparently supports both EOA owners (via hardened `ECDSA.recover` that rejects non-canonical `s` values and restricts `v` to `27`/`28`) and smart-account owners (via ERC-1271 `isValidSignature`), at whatever signature length the wallet uses. Registration happens once per signer, so its width costs nothing at scale.

#### Voucher signatures are compact, and their signers are EOAs

The **voucher** is verified with `ECDSA.tryRecover(digest, r, vs)` against `signer` — a plain `ecrecover` over the [EIP-2098](https://eips.ethereum.org/EIPS/eip-2098) compact pair, where `vs` is `s` with the recovery bit in its top bit. A conforming signer always produces a low-`s` signature, which is what leaves that bit free, and a high-`s` signature is rejected — the same malleability bound the hardened `ECDSA.recover` enforces.

Two 32-byte words instead of a `bytes` blob is what makes `LaneVoucher` a **static** ABI struct: an array of it carries no per-element offset, no length word and no padding. Count the width in **words**, because that is what the struct's static layout makes exact — every field occupies one 32-byte word whatever its declared type, so a `uint8` costs exactly as much calldata as a `bytes32`.

`LaneVoucher` is 5 words today. PayWord adds three, for **8 words / 256 bytes**:

| # | Field | Type | Contents |
| --- | --- | --- | --- |
| 1 | `signer` | `address` | the capability-authorized spending key |
| 2 | `cumulative` | `uint64` | the lane's signed cumulative USDC |
| 3 | `bytesDelivered` | `uint64` | the lane's signed cumulative bytes |
| 4 | `r` | `bytes32` | EIP-2098 compact signature, first word |
| 5 | `vs` | `bytes32` | EIP-2098 compact signature, second word |
| 6 | `chainRoot` | `bytes32` | head of the chain this voucher opens; `0` seals the voucher |
| 7 | `preimage` | `bytes32` | the released value being redeemed; `0` at index 0 |
| 8 | `chainMeter` | `uint256` | **packed**: `chunkPrice` and `chainIndex` in one word |

Word 8 is the packing that keeps the struct at 8. `chunkPrice` needs 64 bits and `chainIndex` needs 8, so both fit one word with room to spare, and neither has to spend a word of its own:

```
chainMeter (32 bytes, big-endian)

 byte  0                     22 23            30 31
      +------------------------+----------------+--+
      |  reserved — MUST be 0  |   chunkPrice   |ci|
      +------------------------+----------------+--+
         23 bytes                 8 bytes (u64)  1 byte (u8)

chunkPrice = uint64(chainMeter >> 8)
chainIndex = uint8(chainMeter & 0xff)
```

The reserved 23 bytes MUST be zero, and redemption MUST revert `ChainMeterReservedNonZero` on a non-zero value there rather than mask it away. That keeps the word extendable — a later field can claim reserved bits without any voucher signed today being reinterpretable — and it costs one comparison. Because the reserved span is zero and `chunkPrice` is small, the packed word is nearly all zero bytes on the wire, so it compresses about as well as the two separate words it replaces.

**Packing changes no signature.** `chainIndex` is a redemption parameter, not a signed field: the signed voucher is still `{poolId, signer, provider, amount, bytesDelivered, chainRoot, chunkPrice}`. The contract decodes `chunkPrice` out of `chainMeter` and rebuilds the EIP-712 struct hash with it widened to `uint256`, exactly as the typehash declares ([EIP-712 Voucher Signature](#eip-712-voucher-signature)). A signer never sees the packed representation, so no client signing code changes and the digest is identical to the unpacked layout.

The struct stays static: `uint256` is a fixed-width type, so an array of `LaneVoucher` still carries no offsets, no length words and no padding. The added width is also mostly zero on the common path — a closing voucher carries a zero root, a zero preimage, and a `chainMeter` whose only non-zero span would be `chunkPrice`, which is itself zero on a sealed voucher. That is what keeps a cooperative redemption at roughly the pre-PayWord cost on a compressing L2 — see [Chain walk and redemption cost](#chain-walk-and-redemption-cost). Recovery also skips the `EXTCODESIZE` an ERC-1271 check would do on a cold signer address.

The cost is that a voucher signer must be an EOA. This is deliberate, and it is the same trade the rest of this section is built around: a lane is free to open while a pool is not, so an adversary funds one pool and spreads dust across many lanes, and the per-lane cost of a batch is what sets the floor on the balance a node can still afford to collect. A genuine publisher with millions of client lanes has exactly the same shape, so no per-lane minimum can separate the two — only a cheaper lane helps both. A pool *owner* is unaffected: the capability path keeps `SignatureChecker`, so a Safe can own a pool and delegate to EOA voucher signers. See [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support) for the full account abstraction design — session keys operate on *who may sign* a voucher (the `signer` key), while the multiple concurrent independent signers on one pool are an accounting property (the sharded register), not a signature-validation one.

The `DOMAIN_SEPARATOR` is computed once in the constructor and stored as an immutable. If the contract is deployed behind a proxy and may be migrated to a different chain, it should be cached in a state variable and recomputed only when `block.chainid` changes (the pattern used by OpenZeppelin's `EIP712` base contract), rather than on every call.

### Voucher ordering

Vouchers carry **no nonce**. A voucher is ordered and replay-checked entirely by its cumulative `amount`. The node tracks the cumulative it has accepted per lane and expects the next voucher to advance it by the rate times the newly delivered bytes — `expected = last_amount + chunk_price × ⌈new_bytes / CHUNK_BYTES⌉` — so it needs no separate sequence field. Preimages order on the other axis: a reveal is placed by its chain index, and the deepest verified index wins ([Concurrent Streams](#concurrent-streams)). The two axes meet at redemption, where `claimed = amount + chain_index × chunk_price` folds them into one monotone number. A voucher at or below the accepted cumulative is **already satisfied**, not a fault: a sibling stream on the same lane has settled that cumulative already, so the node advances nothing and refuses nothing. This is what makes a root-voucher resend free, and why re-anchoring a stream costs no round trip ([Concurrent Streams](#concurrent-streams)). The one exception is a **divergent** voucher — the same `amount` claiming more `bytes_delivered`, the same money for more bytes — which is rejected `BytesRegression`. On-chain redemption is likewise cumulative (it pays `claimed − paid`, which reduces to `cumulative − paid` at index 0), so an already-redeemed or stale claim pays `0`, and the same voucher can be re-presented after a top-up to collect a drained-pool shortfall — only the unpaid remainder is left.

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

1. **View functions.** `getRegisteredNodes(offset, limit)` with pagination. For tens of nodes, a single call with `limit = 100` returns the full node set. Clients call this on first startup to bootstrap their peer list, then track the registry active set for ongoing discovery (see [ADR 001 § Node Discovery](001-network.md#node-discovery-registry)).

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
{poolId, signer, provider, amount, bytesDelivered, chainRoot, chunkPrice, signature}
```

During delivery over `cdn/client/v1`, `{signature, amount, chainRoot, chunkPrice}` are transmitted on the wire; the remaining fields are derived from stream context — `poolId` from the `StreamRequest`, `signer` the bound client key, `provider` the delivering node, and `bytesDelivered` the node's per-lane cumulative byte counter. Between vouchers the payer sends `ChunkPreimage { preimage, index }` — 33 bytes, unsigned, one per delivered chunk. See [ADR 005](005-protocol.md#adr-005-wire-protocol) for wire protocol details.

Full EIP-712 type definition and domain separator: [EIP-712 Voucher Signature](#eip-712-voucher-signature).

### Voucher Bytes-Delivered Field

`bytesDelivered` is a cumulative byte count signed alongside `amount`. It is the canonical served-bytes count carried in the `Voucher`, forwarded to `FeeRouter.routeSettlement` on each redemption, and aggregated into `bytesPerEpoch[operator][epoch]` (where `epoch` is derived from `block.timestamp` at redemption time) — the trailing-window served-bytes accumulator consumed by `DecdnGovernor._getVotes` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) as the governance vote-weight source. Properties:

- **Cumulative, monotonic per lane.** Like `amount`, `bytesDelivered` is strictly non-decreasing across vouchers within a `(poolId, signer, provider)` lane, and a rollover folds the exhausted chain's bytes into the next voucher's cumulative. Redemption computes the byte delta against the lane's last redeemed cumulative.
- **Derivable from the fixed payment quantum.** `chunk_bytes` is a fixed 1 MiB and equals `BYTES_PER_MB`, so `chunk_price` is the MB-denominated `rate_per_mb`. Signers computing `amount` from `bytesDelivered` use `amount = ⌈bytesDelivered / 1_048_576⌉ × chunk_price` (1 MB = 1,048,576 bytes per [ADR 005](005-protocol.md#adr-005-wire-protocol)). The voucher carries the byte count directly so the contract does not re-derive it.
- **Extended by the chain at redemption.** A claim resolves over `claimedBytes = bytesDelivered + chainIndex × CHUNK_BYTES`, and the rate floor clamps the extended pair's credited bytes, so bytes proved by preimage carry the same per-byte price obligation as bytes proved by signature ([Redemption and Close](#redemption-and-close)).
- **Routed at redemption.** Forwarded as the paid-proportional, floor-clamped byte count (`bytesPaid`) to `FeeRouter.routeSettlement` on each redemption; a fully-paid draw forwards the whole credited byte delta.
- **Cross-pool consistency.** A voucher signed for one pool, signer, and provider is bound by its EIP-712 typed data; `bytesDelivered` is part of that signed payload and cannot be replayed against a different lane.

The router does not validate `bytesDelivered` against any oracle of physical delivery — the value is whatever the signer signed. The defense is twofold. **Structurally**, per-byte revenue requires real client USDC inflow rather than self-attested byte counts (a redemption pays only what the pool holds, and forwards paid-proportional bytes). **Quantitatively**, redemption clamps credited bytes to the `deliveryFloor` per-byte price ceiling (see [Rate-floor enforcement](#rate-floor-enforcement)), so a voucher cannot decouple a large *credited* `bytesDelivered` from a tiny `amount` — crediting `B` bytes costs `>= B / 1_048_576` base units. Without the clamp, a signer could stamp arbitrarily many bytes at `amount = 1`; the clamp caps the credit at the floor price and lets the `amount` settle. Governance vote weight is sourced from the same floor-bound per-byte counter ([ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)), so both wash-trade revenue and vote-buying are bound by proportional real USDC.

## Slashing and Pool Interactions

Slashing and pools are independent by design.

**Slashing does not affect pool funds.** Slashing operates exclusively on TOKEN bond in the `CapacityBond` (schedule per [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn) — 5%/15%/50% escalation tiers; slashed bond is held in escrow-on-slash and distributed at finality 50% challenger / 50% burn). Pool funds are owner deposits held in escrow — not bond, never touched by slashing. This follows from the functional separation in [Consequences](#consequences): `PaymentPool` never holds or moves TOKEN bond, cannot be called by `CapacityBond` to slash or reassign bond, and any `CapacityBond` interaction is read-only (e.g., resolving NodeId↔address bindings).

**Slashing can drop a node below its tier minimum bond while it holds vouchers.** Pool deposits being independent of the bond, a node can be slashed below the tier minimum (or to zero) while holding unredeemed vouchers. Redemption continues regardless of bonding status; a payout is purely a function of voucher and register state, not registry status.

**Auto-ejection does not block redemption.** When a node's bond drops below 50% of the minimum and auto-ejection triggers (see [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn)):

- Outstanding vouchers redeem normally. Owner funds are never trapped.
- The ejected node cannot be selected for new service (clients verify node registration, and nodes verify counterparty status before accepting a `StreamRequest`).
- The ejected node is removed from the registry active set, so it receives no new client connections.
- `redeemMany` remains callable on any pool a node holds vouchers against — it checks pool and register state, not registry status, so a slashed or ejected operator can still redeem revenue it already earned. `closePool` / `reclaim` are unaffected on the owner side.
- The node must re-bond at the full tier minimum (`bond_required(declared_capacity)`) and re-register to resume operations.
