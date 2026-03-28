# ADR 003: Payment Model

**Date:** 2026-03-28
**Status:** Draft

## Context

Edge nodes deliver bytes to clients and need to be paid for it. The payment mechanism must work at per-MB granularity without an on-chain transaction per delivery, and must give edge nodes immediate protection against non-payment.

Three constraints shape the design:

1. On-chain transactions on an L2 cost ~$0.05–0.10 each — acceptable per channel lifecycle, not per MB delivered.
2. A typical delivery session transfers a few MB. The payment per MB at market rates is on the order of $0.00001 — far below any on-chain transaction cost.
3. Edge nodes are operators with real infrastructure costs (VPS, bandwidth). Revenue denominated in a volatile native token creates unacceptable P&L risk: a 10× price drop turns a profitable node into a loss.

## Decision

Payments use **unidirectional off-chain payment channels settled on an EVM L2, denominated in USDC**.

A client opens a channel by depositing USDC into the `StablePaymentChannel` contract. As the edge node delivers content, the client signs cumulative vouchers off-chain — one voucher per MB received. The edge node holds the latest voucher and submits it on-chain to close the channel and claim the accumulated payment. A 24-hour dispute window allows either party to counter a stale or fraudulent close attempt.

Key parameters:

- Voucher cadence: 1 MB delivered per voucher
- Minimum deposit: 1 USDC (covers ~100,000 MB at floor rate, far more than any practical session)
- Protocol fee: 3% deducted at channel close, sent to treasury
- Fee discount: providers staking ≥10× the minimum AUDIO stake pay 1.5% instead of 3%

The native token (AUDIO) is not used for delivery payments. It is reserved for staking, governance, and fee discount qualification (see ADR 004).

**Rate setting is entirely up to each edge node.** Nodes advertise their `rate_per_mb` in probe responses and stream responses; clients see the rate before committing a voucher. There is no protocol-enforced rate beyond a governance-set floor and ceiling. This creates a market with natural arbitrage dynamics:

- The origin gateway, as the node that always has the content and bears S3 egress cost, will set a higher rate — it is effectively the price ceiling for any given blob.
- An edge node that pulls from origin and caches the blob can undercut the origin rate, since its marginal cost of serving subsequent requests is just bandwidth.
- An edge node in a region where no peer has the content yet can charge a premium for that first delivery. Once it has cached the blob, other nearby nodes can pull from it (via `cdn/peer/v1`, free) and then compete for local clients at lower rates.
- Nodes with cheaper bandwidth or better hardware can sustainably undercut others; nodes in high-demand regions can charge more and still win on latency.

This means the network self-balances: popular content in a region gets replicated because serving it is profitable, and competition among edge nodes in that region drives prices down. Unpopular content stays at origin rates until demand justifies caching it. No central coordinator needs to decide where to replicate what.

## Consequences

**Positive:**

- On-chain costs are amortized across an entire channel lifetime — open + close = two transactions regardless of how many MB are delivered
- USDC denomination gives edge node operators predictable unit economics: delivery revenue covers infrastructure costs without exposure to AUDIO price movements
- The voucher is the payment receipt; the BLAKE3 hash is the delivery receipt. Together they provide mutual protection: the client doesn't sign a voucher for bytes that fail hash verification; the edge node stops delivering if vouchers stop arriving
- Maximum risk per voucher interval (1 MB) at $0.00001/MB is $0.00001 — negligible
- Market-driven rate setting means replication happens organically: profitable content gets cached by more nodes, driving prices down without any coordination protocol
- The `StablePaymentChannel` contract is isolated from the staking/slashing contract (`PaymentChannel`), keeping the audit surface for each contract bounded

**Negative:**

- Clients must hold USDC to use the network; this adds an onboarding step compared to a single-token model
- Rate volatility: a node can change its advertised rate between a probe and a stream request; the `StreamResponse` rate is the binding one, but a client that probed at one rate and receives a higher rate in `StreamResponse` must disconnect and re-probe rather than having been deceived silently
- Two payment contracts coexist during migration (legacy `PaymentChannel` for AUDIO, `StablePaymentChannel` for USDC), doubling audit surface temporarily
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

Disconnecting works per-incident. The gap is that reputation is local and slow — a new node can bait-and-switch many clients before its reputation degrades enough to matter, and there is no global signal. Options:

- **Option A — Signed, timestamped rate advertisement.** Nodes sign their advertised rate with their iroh private key and a timestamp. If the `StreamResponse` rate differs from the last signed probe rate, the discrepancy is cryptographically provable and can be reported on gossip as evidence, degrading the node's network-wide reputation immediately rather than only locally.
- **Option B — Short local blacklist.** On a bait-and-switch, the client blacklists the offending node for a cooldown period (e.g., 1 hour). Simple, no coordination required, limits repeated abuse from the same node against the same client.
- **Option C — On-chain rate registration.** Nodes publish their rate on-chain. Changes require a transaction, introducing gas cost and finality delay as a natural brake on rapid rate manipulation. Heavy-weight but auditable.

---

**Phantom blob announcement**
Edge node announces a blob as cached then fails or redirects on actual request.

Reputation is too slow here — a newly staked node can announce thousands of phantom blobs before penalties accumulate. The 200ms timeout bounds per-incident cost but the aggregate across many probe targets adds up. Options:

- **Option A — Proof-of-possession challenge.** During the probe exchange, the client requests a hash of a small random byte range within the blob (e.g., bytes 4096–8192). The node must return the correct value or `has_blob` is treated as false. This proves the node actually has the blob with one extra hash computation, at negligible cost for honest nodes.
- **Option B — Stake-weighted announcement trust.** Clients weight `has_blob` claims by the node's stake. A new node with minimum stake gets lower trust for its cache claims; clients probe but don't rely on it without a possession challenge. Higher-staked nodes get more trust implicitly.
- **Option C — Reputation fast-path for phantom detection.** Track `has_blob: true` claims that result in a redirect or `NotCached` response separately from other reputation signals, with a steeper penalty weight. A node that lies about cache hits is penalised faster than one that is simply slow.

---

**Channel close front-running**
Edge node monitors the mempool and front-runs a client's channel close with a higher voucher submission.

Not a real attack. The contract always settles the highest valid voucher, and only the client can sign a valid voucher. An edge node submitting the latest voucher before the client is the intended happy path. Fabricating a higher voucher requires forging the client's ECDSA signature, which is cryptographically infeasible.

---

### Network-level

**Eclipse attack**
Attacker surrounds a client with malicious nodes so all probe responses come from nodes under attacker control.

BLAKE3 verification prevents corrupted data from being accepted regardless of who serves it. The remaining gap is a denial-of-service variant: an attacker who controls all of a client's known nodes can simply refuse to serve, forcing the client to discover honest nodes it cannot currently reach. Options:

- **Option A — Origin gateway as unconditional fallback.** Clients always maintain a direct connection to the origin gateway, discovered independently of peer gossip. An eclipse cannot block origin access without also blocking internet access to the origin URL. This is already in the design but should be treated as a hard invariant, not a soft fallback.
- **Option B — Multi-source bootstrap.** Clients discover initial peers from at least two independent sources (on-chain registry + a hardcoded DNS seed list). An attacker must compromise both to fully eclipse a client.
- **Option C — Minimum honest-peer diversity.** Clients maintain connections to at least N nodes discovered via different paths (gossip, DHT, direct registry query). All N would need to be attacker-controlled for a full eclipse.

---

**Gossip flooding**
Node sends high-volume `CacheAnnounce` messages to exhaust peer routing table memory or crowd out legitimate announcements.

Registry check + per-sender rate limiting is solid. The minor gap is that the local registry cache may be up to 10 minutes stale, briefly allowing recently-unstaked nodes to flood. Mostly solved; no strong alternative needed beyond tightening the registry cache refresh on high flood detection.

---

**Sybil edge nodes**
Attacker stakes many cheap nodes to dominate probe responses for popular content, controlling pricing in a region.

The core weakness is token-price dependency: at $0.001/AUDIO, a minimum stake of 1,000 AUDIO costs $1 per sybil node. The `rate_per_mb × rtt_ms` selection score helps — a sybil fleet must be real hardware in the right geography and competitively priced — but does not eliminate the risk when the token is cheap. Options:

- **Option A — Governance raises minimum stake if token price falls.** The minimum stake is governable. Token holders are incentivised to raise it to protect the network, since a sybil-dominated network reduces usage and token value. Reactive but aligned.
- **Option B — Minimum stake denominated in USD equivalent via oracle.** Requires a price oracle, which was rejected in ADR 004 for payment rate bounds. The same concerns (oracle downtime, manipulation) apply here, but the impact of oracle failure is lower (new stakers temporarily blocked, not payments broken).
- **Option C — Reputation as a second filter.** New nodes (low reputation, few settled channels) are deprioritised in client selection even if their `rate_per_mb × rtt_ms` score is competitive. A sybil fleet takes time to build reputation, limiting its effectiveness during that window.

---

**Rate manipulation cartel**
Colluding edge nodes in a region hold rates artificially high.

Well-mitigated by the origin gateway ceiling and permissionless entry. The game theory strongly favours defection from a cartel. No meaningful alternative needed.

---

**Replay attack on vouchers**
Attacker intercepts a signed voucher and attempts to replay it against a different channel or after close.

Fully solved. EIP-712 typed data over `{channelId, amount, nonce, stablecoin}` binds the voucher to a specific channel. The monotonically increasing nonce prevents resubmission after settlement.
