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
| **TOKEN** | The concrete governance token: the protocol's native fixed-supply (1B) ERC-20. Used for capacity bonding, governance, service emissions, and slashing — see [ADR 026](026-tokenomics.md#adr-026-tokenomics). |
| **USDC** | The concrete payment token. Fixed at deployment; all channel deposits, fee distribution, and the externally-raised pre-seed pool are USDC-denominated — see [ADR 003](003-payments.md#adr-003-payment-model), [ADR 026](026-tokenomics.md#adr-026-tokenomics). |

## Tokenomics & incentives

| Term | Definition |
| --- | --- |
| **Bond** | TOKEN deposited in `CapacityBond` proportional to declared bandwidth capacity: `bond = k × Mbps^α` (defaults `k=12.6`, `α=1.2`). The only TOKEN-side requirement on operators — no separate flat minimum stake — see [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve). |
| **CapacityBond** | The on-chain capacity-bonded operator-registry contract. Holds TOKEN bond, enforces `bond ≥ bond_required(declared_capacity)`, performs slashing, manages the NodeId↔Ethereum-address binding. Replaces the prior `StakingRegistry`/`VotingEscrow` pair under the v2.1 work-token rewrite — see [ADR 026](026-tokenomics.md#adr-026-tokenomics) and [ADR 016 § Contract Inventory](016-contract-interactions.md#contract-inventory). |
| **Operator Service Emissions** | TOKEN granted to active operators for verified service delivery (probe-attested bytes × capacity tier × diminishing-returns curve). Auto-deposited into `CapacityBond`; not withdrawable as liquid until full unbond. Sized at 20% of supply (200M TOKEN) — see [ADR 026 § Operator Service Emissions](026-tokenomics.md#operator-service-emissions). Labelled "Staking Rewards" on fundraising-tool exports for recognizability. |
| **Slashing** | Punitive reduction of bonded TOKEN on detected protocol violations. Escalating tiers 5%/15%/50% by lifetime offense count; distribution 50% challenger / 30% safety reserve / 20% burn — see [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn). Capacity-shortfall slashing is a separate deterministic path (`min_delivery_ratio` violation → auto-downgrade with bond delta to `SafetyReserve`). |
| **Epoch** | The 1-week analytics and service-emission distribution window. Per-operator `bytes_delivered` counters reset at epoch rollover; counters feed the `OperatorEmissions` distribution but no longer drive bucket payouts (FeeRouter is fully same-tx under v2.1). |
| **FeeRouter** | The settlement-time four-bucket USDC distributor. Split: 60 operator base / 25 burn / 10 treasury / 5 safety. All four legs transfer in the settlement transaction; no epoch buckets, no pull-claim windows — see [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split). |
| **SafetyReserve** | A governance-gated USDC incident reserve (5% bucket). Payouts cover incorrect-slashing reversals, payment-channel downtime, and bad-data incidents — see [ADR 026 § Safety and insurance reserve (5% bucket)](026-tokenomics.md#safety-and-insurance-reserve-5-bucket). |

## On-chain enforcement

| Term | Definition |
| --- | --- |
| **`CapacityBond`** | The on-chain capacity-bonded operator-registry contract (formerly `StakingRegistry`, renamed under v2.1). Holds TOKEN bond, enforces `bond ≥ bond_required(declared_capacity)` via the lock-to-capacity curve `bond = k × Mbps^α`, performs slashing (including capacity-shortfall auto-downgrade), manages the NodeId↔Ethereum-address binding. |
| **`SlashJudge`** | The on-chain contract that adjudicates all slashable offenses: verifies slash signatures, manages challenge bonds, runs counter-evidence windows, calls `CapacityBond.slash()` — see [ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence). |
| **`BuybackBurner`** | The contract that swaps the 25% burn-bucket USDC into TOKEN via Balancer V3 80/20 weighted pool and burns the proceeds — see [ADR 003 § BuybackBurner](003-payments.md#buybackburner) and [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol). |
| **Slash signature** | An EIP-712 secp256k1 signature (`slash_sig`) on `ProbeResponse` / `StreamResponse`, used for on-chain slash evidence via `ecrecover` and as the message-body attribution signature for the paid-delivery path — see [ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence). |
| **Challenge bond** | The 100 TOKEN amount a challenger must post when submitting slash evidence. Returned on successful slash, forfeited on successful node counter (50% burned, 50% to node) — see [ADR 014 § Bond Handling](014-on-chain-verification.md#bond-handling). |
