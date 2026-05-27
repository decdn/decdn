# Glossary

Terms used across multiple ADRs without inline definition.

## Wire protocol & content

| Term | Definition |
| --- | --- |
| **Blob** | A content-addressed byte sequence identified by its BLAKE3 hash. |
| **Chunk** | The BLAKE3 hash-tree leaf size (1024 bytes). iroh-blobs uses this for verified streaming; on-chain Merkle proofs for slash evidence reference this leaf size — see [ADR 002](002-content-addressing.md#adr-002-content-addressing) for the addressing scheme and [ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence) for the slash-evidence verification flow. |
| **Hash sequence** | An ordered collection of blob hashes (iroh's equivalent of a directory/manifest). |
| **NodeId** | An iroh public-key identifier; the on-wire identity of a node. Bound to an Ethereum address on-chain via EIP-712 signature in `CapacityBond.registerNode` ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)). |
| **Node** | A staked participant that caches and serves blobs. Some nodes are configured with an origin backend; others are pure caches. |
| **Client** | A lightweight QUIC endpoint that streams content and pays per MB. |
| **Origin-backed node** | A node configured with an S3-compatible object store (S3/R2/B2/MinIO), NFS mount, or local disk. Can serve any blob in that store, never experiences a true cache miss. |
| **ALPN** | Application-Layer Protocol Negotiation — identifies which protocol a QUIC connection uses (e.g., `cdn/probe/v1`, `cdn/client/v1`). |

## Payments

| Term | Definition |
| --- | --- |
| **Payment token** | The dual-currency model's settlement asset: the currency clients pay in and operators are paid in. The concrete asset is **USDC**, fixed at contract deployment (immutable, 6 decimals) — see [ADR 003](003-payments.md#adr-003-payment-model). This is the canonical term used throughout the spec for the channel-deposit, voucher, and fee-distribution currency; "USDC" appears only where a USDC-specific property is load-bearing (decimals, Circle counterparty risk, on-chain identifiers, swap pairs, dollar-denominated constants). |
| **Governance token** | The dual-currency model's bonding/governance asset. The concrete asset is **TOKEN**, the protocol's native fixed-supply (1B) `ERC20Burnable` — see [ADR 026](026-tokenomics.md#adr-026-tokenomics). "TOKEN" is used as the asset symbol/unit; "governance token" is the role term. Distinct from the payment token; never interchangeable. |
| **Channel** | An off-chain payment channel between a client and a node. Funded with the payment token, settled on-chain after the dispute window — see [ADR 003](003-payments.md#adr-003-payment-model). |
| **Voucher** | A signed off-chain payment message: `{channelId, amount, nonce, token, signature}` (the `token` field is the payment-token contract address, bound for cross-contract replay protection). The bearer instrument for per-MB payments. |
| **TOKEN** | The concrete governance token: the protocol's native fixed-supply (1B) ERC-20. Used for capacity bonding, governance, and slashing — see [ADR 026](026-tokenomics.md#adr-026-tokenomics). There is no ongoing TOKEN-denominated service emission; year-1 operator bootstrap is the one-shot Genesis Bond Credits program per [ADR 026 § Genesis Bond Credits](026-tokenomics.md#genesis-bond-credits). |
| **USDC** | The concrete payment token. Fixed at deployment; all channel deposits, fee distribution, and the externally-raised pre-seed pool are USDC-denominated — see [ADR 003](003-payments.md#adr-003-payment-model), [ADR 026](026-tokenomics.md#adr-026-tokenomics). |

## Tokenomics & incentives

| Term | Definition |
| --- | --- |
| **Bond** | TOKEN deposited in `CapacityBond` proportional to declared bandwidth capacity: `bond = k × Mbps^α` (defaults `k=12.6`, `α=1.2`). The only TOKEN-side requirement on operators — no separate flat minimum stake — see [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve). |
| **CapacityBond** | The on-chain capacity-bonded operator-registry contract. Holds TOKEN bond, enforces `bond ≥ bond_required(declared_capacity)`, performs slashing, manages the NodeId↔Ethereum-address binding — see [ADR 026](026-tokenomics.md#adr-026-tokenomics) and [ADR 016 § Contract Inventory](016-contract-interactions.md#contract-inventory). |
| **Genesis Bond Credits** | A bounded, one-shot, retroactive `PendingCredit` allocation in `CapacityBond` to verified pre-launch incentivized-testnet operators. Sized at 5% of supply (50M TOKEN, carved from the DAO Treasury bucket at TGE), auto-bonded at grant, vests linearly over 24 months of continued operation — see [ADR 026 § Genesis Bond Credits](026-tokenomics.md#genesis-bond-credits). Funds the year-1 operator-tier-upgrade runway with no ongoing emission. |
| **App Incentives** | A 14%-of-supply (140M TOKEN) demand-side ecosystem bucket, Treasury-multisig administered, 4-year linear unlock. Funds publisher rebates and integration grants — see [ADR 026 § App Incentives](026-tokenomics.md#app-incentives). Disjoint with Genesis Bond Credits eligibility. |
| **PendingCredit** | The `CapacityBond` storage extension that holds and vests Genesis Bond Credits per operator. Slashable simultaneously with `bondedAmount` (the unvested portion is subject to slashing on the same offense) — see [ADR 016 § Contract: CapacityBond](016-contract-interactions.md#contract-capacitybond) and [ADR 028 § Reputation handling](028-slashing-appeals.md#reputation-handling). |
| **Slashing** | Punitive reduction of bonded TOKEN on detected protocol violations. Escalating tiers 5%/15%/50% by lifetime offense count; distribution 50% challenger / 30% safety reserve / 20% burn — see [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn). Capacity-shortfall slashing is a separate deterministic path (`min_delivery_ratio` violation → auto-downgrade with bond delta to `SafetyReserve`). Any slash additionally stamps `CapacityBond.slashedAtEpoch[op]` per [ADR 036 § Slashing zero-out](036-served-bytes-voting-weight.md#slashing-zero-out), zeroing the operator's vote weight while that watermark falls inside the trailing window. |
| **`slashedAtEpoch`** | A `CapacityBond` per-operator watermark stamped by any `slash()` call and cleared by `clearSlashedAtEpoch(op)` on a successful slash-appeal reversal via [ADR 028](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation). Read by `DecdnGovernor._getVotes` to zero an operator's vote weight while inside the trailing window — see [ADR 036 § Slashing zero-out](036-served-bytes-voting-weight.md#slashing-zero-out). |
| **`windowEpochs`** | The governable trailing-window size (in epochs) used by both `FeeRouter.bytesInWindow` (vote-weight numerator) and the slash-zero-out interval. Default 13 (~1 quarter at 1-week epochs); bounds `[4, 26]` — see [ADR 036 § Governable parameters with safety bounds](036-served-bytes-voting-weight.md#governable-parameters-with-safety-bounds). |
| **Served-bytes voting weight** | The governance vote-weight source under [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight): `vote_weight(op, t) = min(served_bytes_window(op, t), voteCapBps × total_bytes_window(t) / 10_000) × age_ramp(op, t)`, where `served_bytes_window(op, t)` is the trailing-`windowEpochs` sum of `FeeRouter.bytesPerEpoch[op]`. Zero while `CapacityBond.slashedAtEpoch[op]` falls inside the trailing window. `FeeRouter.bytesPerEpoch` is governance-canonical. |
| **Epoch** | The 1-week trailing-window unit. Per-operator `bytes_delivered` counters accumulate on the `FeeRouter` and are read both for the served-bytes voting weight ([ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)) and for trailing-window analytics. Counters do not drive bucket payouts (`FeeRouter` is fully same-tx). |
| **FeeRouter** | The settlement-time four-bucket USDC distributor. Split: 60 operator base / 25 burn / 10 treasury / 5 safety. All four legs transfer in the settlement transaction; no epoch buckets, no pull-claim windows — see [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split). Per-operator `bytesPerEpoch` accounting on this contract is the governance vote-weight source per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight). |
| **SafetyReserve** | A governance-gated USDC incident reserve (5% bucket). Payouts cover incorrect-slashing reversals, payment-channel downtime, and bad-data incidents — see [ADR 026 § Safety and insurance reserve (5% bucket)](026-tokenomics.md#safety-and-insurance-reserve-5-bucket). Holds `APPEAL_REVERSAL_ROLE` on `CapacityBond` so that successful slash reversals can clear `slashedAtEpoch[op]` per [ADR 036 § Slashing zero-out](036-served-bytes-voting-weight.md#slashing-zero-out). |

## On-chain enforcement

| Term | Definition |
| --- | --- |
| **`CapacityBond`** | The on-chain capacity-bonded operator-registry contract (formerly `StakingRegistry` in earlier drafts). Holds TOKEN bond, enforces `bond ≥ bond_required(declared_capacity)` via the lock-to-capacity curve `bond = k × Mbps^α`, performs slashing (including capacity-shortfall auto-downgrade), manages the NodeId↔Ethereum-address binding. |
| **`SlashJudge`** | The on-chain contract that adjudicates all slashable offenses: verifies slash signatures, manages challenge bonds, runs counter-evidence windows, calls `CapacityBond.slash()` — see [ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence). |
| **`BuybackBurner`** | The contract that swaps the 25% burn-bucket USDC into TOKEN via Balancer V3 80/20 weighted pool and burns the proceeds — see [ADR 003 § BuybackBurner](003-payments.md#buybackburner) and [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol). |
| **Slash signature** | An EIP-712 secp256k1 signature (`slash_sig`) on `ProbeResponse` / `StreamResponse`, used for on-chain slash evidence via `ecrecover` and as the message-body attribution signature for the paid-delivery path — see [ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence). |
| **Challenge bond** | The 100 TOKEN amount a challenger must post when submitting slash evidence. Returned on successful slash, forfeited on successful node counter (50% burned, 50% to node) — see [ADR 014 § Bond Handling](014-on-chain-verification.md#bond-handling). |
