# ADR 003: Payment Model

**Date:** 2026-03-28
**Status:** Draft

## Context

Vault nodes and edge nodes deliver bytes and need to be paid for it. The payment mechanism must work at per-MB granularity without an on-chain transaction per delivery, and must give delivering nodes immediate protection against non-payment.

Three constraints shape the design:

1. On-chain transactions on an L2 cost ~$0.05–0.10 each — acceptable per channel lifecycle, not per MB delivered.
2. A typical delivery session transfers a few MB. The payment per MB at market rates is on the order of $0.00001 — far below any on-chain transaction cost.
3. Node operators have real infrastructure costs (VPS, bandwidth, backend storage). Revenue denominated in a volatile native token creates unacceptable P&L risk: a 10× price drop turns a profitable operator into a loss.

## Decision

Payments use **unidirectional off-chain payment channels settled on an EVM L2, denominated in USDC**.

The same channel mechanism operates at two tiers:

- **Client → edge node**: a client opens a USDC channel with an edge node, signs cumulative vouchers as MB are delivered, and the edge node closes the channel on-chain to claim payment.
- **Edge node → vault node**: when an edge node pulls content from a vault node for the first time, it pays the vault node via the same channel mechanism. The vault node is paid wholesale; the edge node recoups this by serving multiple clients from its cache at a markup.

A channel is opened by depositing USDC into the `StablePaymentChannel` contract. As content is delivered, the payer signs cumulative vouchers off-chain — one voucher per MB received. The delivering node holds the latest voucher and submits it on-chain to close the channel. A 24-hour dispute window allows either party to counter a stale or fraudulent close attempt.

Key parameters:

- Voucher cadence: 1 MB delivered per voucher
- Minimum deposit: 1 USDC (covers ~100,000 MB at floor rate, far more than any practical session)
- Protocol fee: 3% deducted at channel close, sent to treasury
- Fee discount: nodes staking ≥10× the minimum TOKEN stake pay 1.5% instead of 3%

The native token (TOKEN) is not used for delivery payments. It is reserved for staking, governance, and fee discount qualification (see ADR 004).

**Rate setting is entirely up to each node.** Vault nodes and edge nodes advertise their `rate_per_mb` in probe responses and stream responses; the requester sees the rate before committing a voucher. There is no protocol-enforced rate beyond a governance-set floor and ceiling. This creates a two-tier market with natural arbitrage dynamics:

- Vault nodes set a higher rate because they bear backend costs (storage + egress from their hidden backing store). They are the effective price ceiling for any blob they hold.
- An edge node that pays a vault node to pull a blob can then serve that blob to many clients at a markup, recouping the vault cost across multiple deliveries.
- An edge node in a region where no peer has the content yet can charge a premium for that first delivery. Once it has the blob, other nearby edges pull from it for free (via `cdn/peer/v1`) and compete for local clients at lower rates.
- Nodes with cheaper bandwidth or better hardware can sustainably undercut others; nodes in high-demand regions can charge more and still win on latency.

This means the network self-balances: popular content gets replicated because caching it is profitable, competition drives prices down in well-served regions, and unpopular content stays at vault node rates until demand justifies caching it. No central coordinator decides where to replicate what.

## Consequences

**Positive:**

- On-chain costs are amortized across an entire channel lifetime — open + close = two transactions regardless of how many MB are delivered
- USDC denomination gives edge node operators predictable unit economics: delivery revenue covers infrastructure costs without exposure to TOKEN price movements
- The voucher is the payment receipt; the BLAKE3 hash is the delivery receipt. Together they provide mutual protection: the client doesn't sign a voucher for bytes that fail hash verification; the edge node stops delivering if vouchers stop arriving
- Maximum risk per voucher interval (1 MB) at $0.00001/MB is $0.00001 — negligible
- Market-driven rate setting means replication happens organically: profitable content gets cached by more nodes, driving prices down without any coordination protocol
- The `StablePaymentChannel` contract is isolated from the staking/slashing contract (`PaymentChannel`), keeping the audit surface for each contract bounded

**Negative:**

- Clients must hold USDC to use the network; this adds an onboarding step compared to a single-token model
- Rate volatility: a node can change its advertised rate between a probe and a stream request; the `StreamResponse` rate is the binding one, but a client that probed at one rate and receives a higher rate in `StreamResponse` must disconnect and re-probe rather than having been deceived silently
- Two payment contracts coexist during migration (legacy `PaymentChannel` for TOKEN, `StablePaymentChannel` for USDC), doubling audit surface temporarily
- USDC is issued by Circle, which can freeze specific addresses or blacklist the contract. This is mitigated by a governance-maintained allowlist that can add DAI or other stablecoins, but the risk is not eliminated
- BLAKE3 verification on EVM requires an intermediate Merkle proof scheme for PoC-era slash evidence; a client submitting a slash claim cannot directly prove BLAKE3 mismatch on-chain

## Attack Vectors

### Client-side

**Voucher withholding**
Client receives bytes but stops signing vouchers, getting content for free up to the last signed interval.

The self-enforcing stop is sufficient. Maximum loss is one interval (1 MB × rate ≈ $0.00001). No additional mechanism needed — this is fully addressed by the protocol design.

---

**Channel griefing**
Client opens many channels with minimum deposit and never streams, forcing edge nodes to track and eventually close stale channels.

The current mitigation (auto-expire + deposit > gas cost) limits financial loss to the attacker but does not bound the memory overhead on the edge node. An attacker with modest capital can hold thousands of open-but-idle channels in the node's tracking state for up to 30 days. Options:

- **Option A — Inactivity expiry.** Channels with no voucher submitted within the first 7 days auto-expire, rather than the full 30-day channel lifetime. Reduces the attack window significantly at no cost to normal users.
- **Option B — On-chain channel cap per address.** The `StablePaymentChannel` contract enforces a maximum number of open channels per client Ethereum address (e.g., 10). Hard to circumvent without new wallet addresses, each requiring on-chain funding.
- **Option C — Edge node-side filtering.** Edge nodes refuse `StreamRequest` from channels that have been open longer than N days with zero vouchers. Off-chain, no contract change needed, but relies on node operator implementation.

---

**Stale close**
Client submits an old voucher (lower amount) to close the channel, underpaying the edge node.

The 24-hour dispute window works if the edge node is online. The gap is liveness: if the node goes offline after a stale close is submitted and misses the dispute window, it loses the difference. Options:

- **Option A — Watchtowers.** A separate monitoring service holds the latest voucher and submits it on the node's behalf if a dispute is detected. Adds operational complexity but fully closes the gap.
- **Option B — Longer dispute window.** Increase from 24 hours to 7 days, giving operators more time to respond. Delays legitimate channel closes for everyone.
- **Option C — Persistent monitoring process.** The edge node binary runs a lightweight dispute monitor as a separate thread that only watches the chain for close events, independent of the serving process. Simpler than a watchtower but still single-node.

---

**Probe fishing**
Client sends probe requests to many nodes at high frequency to map the network or exhaust node resources without ever paying.

The current mitigation is weak. Clients are not staked — their NodeIds are free to rotate — so per-NodeId rate limiting is bypassable. The iroh connection setup cost is also low. Options:

- **Option A — IP-based rate limiting.** Rate limit probe requests by source IP rather than NodeId. Harder to rotate at scale, though not impossible with proxies or cloud infrastructure.
- **Option B — Require an open channel to probe.** Only clients with an existing open payment channel (any amount) can probe. Strong protection but adds friction for new users who haven't yet deposited.
- **Option C — Proof-of-work on probe requests.** Include a small PoW challenge in the probe request (e.g., find a nonce such that `hash(NodeId || nonce) < difficulty`). Adds CPU cost to bulk probing without affecting honest single-request clients noticeably.
- **Option D — Accept the risk.** A probe is a single message exchange. The cost to serve one is negligible; the attack only matters at extreme scale. Rate limit at the connection level (iroh handles this) and monitor for abuse rather than trying to prevent it at the protocol level.

---

**Double-spend across nodes**
Client opens channels with multiple nodes using the same USDC deposit via a race condition before the on-chain state settles.

Fully solved. Each `openChannel` call transfers USDC into the contract immediately; the client's wallet balance is debited on-chain before the transaction finalises. No credit facility exists.

---

### Edge node-side

**Data withholding**
Edge node accepts a stream request, receives a voucher, then stops delivering bytes.

Fully solved by the self-enforcing protocol. The edge node cannot extract more payment than the last acknowledged voucher. The client resumes from `byte_offset` on a different node.

---

**Corrupted delivery**
Edge node serves bytes that don't match the advertised BLAKE3 hash.

BLAKE3 verification catches this immediately at the client. The remaining gap is the slash evidence path: submitting the full bad bytes on-chain to prove a BLAKE3 mismatch is gas-expensive for large blobs, and the PoC Merkle proof scheme adds complexity. Options:

- **Option A — Optimistic challenge-response.** Client submits only a commitment (hash of received data) and the chunk index on-chain. The contract gives the node 24 hours to respond with the correct chunk and a Merkle proof. If it cannot, it is slashed. This avoids submitting full blob data on-chain.
- **Option B — Rely on reputation, not slash, for the common case.** Slash is a last resort for severe or repeated corruption. For a single incident, immediate session termination + reputation penalty is sufficient. Reserve the on-chain slash path for nodes with a history of corruption.
- **Option C — Off-chain fraud proof with a verifier role.** A designated verifier node (staked, incentivised by a cut of the slash) receives the disputed bytes off-chain, verifies the BLAKE3 mismatch, and submits a compact on-chain attestation. Adds a trusted verifier dependency.

---

**Rate bait-and-switch**
Edge node advertises a low rate in probe responses then returns a higher rate in `StreamResponse`.

**Resolved: slashable offense.** Both `ProbeResponse` and `StreamResponse` now include cryptographic signatures over the advertised rate (see ADR 005). If a client receives a signed `ProbeResponse` with rate X and a signed `StreamResponse` with rate Y > X from the same node within 30 seconds, the two signed messages constitute on-chain-verifiable evidence of rate manipulation. The edge node is slashed per the escalating schedule in ADR 004. The 30-second window allows legitimate rate changes between sessions while catching same-session bait-and-switch.

---

**Phantom blob announcement**
Edge node announces a blob as cached then fails or redirects on actual request.

**Resolved: slashable offense.** The `ProbeResponse` now includes a cryptographic signature over `{hash, has_blob, rate_per_mb, timestamp_us}` (see ADR 005). If an edge node signs `has_blob: true` but subsequently responds with `NotCached` or fails to deliver within 30 seconds, the signed probe response plus the delivery failure constitute on-chain-verifiable evidence. The edge node is slashed per the escalating schedule in ADR 004. The 30-second validity window accounts for the possibility that a blob is legitimately evicted between probe and request — 30 seconds is short enough to make eviction implausible but long enough for normal protocol flow. The challenged edge has a 24-hour window to counter by proving it delivered the blob (signed delivery receipt from the same requester within the relevant time window).

---

**Channel close front-running**
Edge node monitors the mempool and front-runs a client's channel close with a higher voucher submission.

Not a real attack. The contract always settles the highest valid voucher, and only the client can sign a valid voucher. An edge node submitting the latest voucher before the client is the intended happy path. Fabricating a higher voucher requires forging the client's ECDSA signature, which is cryptographically infeasible.

---

### Network-level

**Eclipse attack**
Attacker surrounds a client with malicious nodes so all probe responses come from nodes under attacker control.

BLAKE3 verification catches data corruption regardless of which nodes are in the routing table. The remaining gap is a denial-of-service variant: an attacker controlling all of a client's known nodes can simply refuse to serve. Options:

- **Option A — Vault nodes as typed fallback.** The staking registry exposes node roles. Clients can specifically query for vault nodes for a given blob, bypassing the general routing table. An eclipse must also control all vault nodes for the target content — which requires capital proportional to the number of vault nodes staked for that content.
- **Option B — Multi-source bootstrap.** Clients discover initial peers from at least two independent sources (on-chain registry + a hardcoded DNS seed list). An attacker must compromise both to fully eclipse a client.
- **Option C — Minimum honest-peer diversity.** Clients maintain connections to at least N nodes discovered via different paths. All N would need to be attacker-controlled for a full eclipse.

---

**Gossip flooding**
Node sends high-volume `CacheAnnounce` messages to exhaust peer routing table memory or crowd out legitimate announcements.

Registry check + per-sender rate limiting is solid. The minor gap is that the local registry cache may be up to 10 minutes stale, briefly allowing recently-unstaked nodes to flood. Mostly solved; no strong alternative needed beyond tightening the registry cache refresh on high flood detection.

---

**Sybil edge nodes**
Attacker stakes many cheap nodes to dominate probe responses for popular content, controlling pricing in a region.

The core weakness is token-price dependency: at $0.001/TOKEN, a minimum stake of 1,000 TOKEN costs $1 per sybil node. The `rate_per_mb × rtt_ms` selection score helps — a sybil fleet must be real hardware in the right geography and competitively priced — but does not eliminate the risk when the token is cheap. Options:

- **Option A — Governance raises minimum stake if token price falls.** The minimum stake is governable. Token holders are incentivised to raise it to protect the network, since a sybil-dominated network reduces usage and token value. Reactive but aligned.
- **Option B — Minimum stake denominated in USD equivalent via oracle.** Requires a price oracle, which was rejected in ADR 004 for payment rate bounds. The same concerns (oracle downtime, manipulation) apply here, but the impact of oracle failure is lower (new stakers temporarily blocked, not payments broken).
- **Option C — Reputation as a second filter.** New nodes (low reputation, few settled channels) are deprioritised in client selection even if their `rate_per_mb × rtt_ms` score is competitive. A sybil fleet takes time to build reputation, limiting its effectiveness during that window.

---

**Rate manipulation cartel**
Colluding edge nodes in a region hold rates artificially high.

Vault nodes set the effective price ceiling for any blob. Clients can always probe vault nodes directly and pay vault rates as a guaranteed fallback. Any edge node outside the cartel that undercuts wins all local traffic — the incentive to defect is strong. New entrants can join permissionlessly by staking.

---

**Vault node content withholding**
A vault node stakes, announces content it holds, but refuses to serve it — collecting credibility in the routing tables without actually participating.

This attack has no equivalent in a model with a public origin URL. **Withholding is not a slashable offense for vault nodes** — vault operators may legitimately take content offline for maintenance, migration, or business reasons, and slashing for availability creates perverse incentives (operators become afraid to perform necessary operations). Instead, withholding is handled through reputation and redundancy:

- **Multiple vault nodes per blob.** Content owners register more than one vault node for important content. A single withholding node becomes irrelevant if others serve the same blob. Staking cost is a natural limit on how many vault nodes an attacker can control across all content.
- **Reputation fast-path for vault nodes.** Vault nodes that fail to serve announced content accumulate reputation penalties at a steeper rate than edge nodes, since their role is canonical availability, not best-effort caching. A vault node with consistently poor availability is deprioritized in routing and loses delivery revenue.

---

**Replay attack on vouchers**
Attacker intercepts a signed voucher and attempts to replay it against a different channel or after close.

Fully solved. EIP-712 typed data over `{channelId, amount, nonce, stablecoin}` binds the voucher to a specific channel. The monotonically increasing nonce prevents resubmission after settlement.
