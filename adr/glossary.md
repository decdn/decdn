# Glossary

Terms used across multiple ADRs without inline definition.

## Wire protocol & content

| Term | Definition |
| --- | --- |
| **Blob** | A content-addressed byte sequence identified by its BLAKE3 hash. |
| **Chunk** | The BLAKE3 hash-tree leaf size (1024 bytes). iroh-blobs uses this for verified streaming; on-chain Merkle proofs for slash evidence reference this leaf size — see [ADR 002](002-content-addressing.md) for the addressing scheme and [ADR 014](014-on-chain-verification.md) for the slash-evidence verification flow. |
| **Hash sequence** | An ordered collection of blob hashes (iroh's equivalent of a directory/manifest). |
| **NodeId** | An iroh public-key identifier; the on-wire identity of a node. Bound to an Ethereum address on-chain via EIP-712 signature in `StakingRegistry.registerNode` ([ADR 001](001-network.md)). |
| **Node** | A staked participant that caches and serves blobs. Some nodes are configured with an origin backend; others are pure caches. |
| **Client** | A lightweight QUIC endpoint that streams content and pays per MB. |
| **Origin-backed node** | A node configured with an S3-compatible object store (S3/R2/B2/MinIO), NFS mount, or local disk. Can serve any blob in that store, never experiences a true cache miss. |
| **ALPN** | Application-Layer Protocol Negotiation — identifies which protocol a QUIC connection uses (e.g., `cdn/probe/v1`, `cdn/client/v1`). |

## Payments

| Term | Definition |
| --- | --- |
| **Channel** | An off-chain payment channel between a client and a node. Funded with USDC (or a governance-approved ERC-20 in production), settled on-chain after the dispute window — see [ADR 003](003-payments.md). |
| **Voucher** | A signed off-chain payment message: `{channelId, amount, nonce, token, signature}`. The bearer instrument for per-MB payments. |
| **TOKEN** | The protocol's native fixed-supply (1B) ERC-20. Used for staking, governance, gauge-boost, and slashing — see [ADR 026](026-gauge-boost-tokenomics.md). |
| **USDC** | The payment-and-settlement currency. All channel deposits, fee distribution, and the externally-raised pre-seed pool are USDC-denominated — see [ADR 003](003-payments.md), [ADR 026](026-gauge-boost-tokenomics.md). |

## Tokenomics & incentives

| Term | Definition |
| --- | --- |
| **Stake** | TOKEN deposited in `StakingRegistry` as a prerequisite for node registration. Minimum: 50,000 TOKEN per node — see [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake). |
| **Slashing** | Punitive reduction of staked TOKEN on detected protocol violations. Escalating tiers 5%/15%/50% by lifetime offense count; distribution 50% challenger / 30% safety reserve / 20% burn — see [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn). |
| **Epoch** | The 1-week settlement and ve-snapshot window. Gauge buckets, delegator buckets, and `bytes_delivered` counters reset at epoch rollover — see [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553). |
| **Gauge / gauge-boost** | Curve-style mechanism that scales an operator's share of the 40% gauge pool by ve-weighted commitment, not raw bytes. The boost-floor parameter caps the worst-case ratio between an unboosted and fully-boosted operator. |
| **`working_bytes`** | The gauge-formula input. Per-operator: `min(bytes_i, 0.4 * bytes_i + 0.6 * (ve_i / total_ve) * total_bytes)`. Replaces Curve's LP-deposit primitive with verified-bytes-delivered. |
| **ve / VotingEscrow** | Vote-escrowed TOKEN: a non-transferable, time-decaying lock of underlying TOKEN that grants gauge-boost and governance weight. Opt-in (no auto-ve-lock-on-vest) — see [ADR 026 §4](026-gauge-boost-tokenomics.md#4-voting-escrow-votingescrow). |
| **FeeRouter** | The settlement-time six-bucket USDC distributor. Split: 40 node base / 40 gauge / 7 delegator / 5 burn / 5 treasury / 3 safety. Atomic same-tx for the 40+5+5+3 legs; epoch-bucketed for the 40 gauge / 7 delegator legs. |
| **SafetyReserve** | A governance-gated USDC incident reserve (3% bucket). Payouts cover incorrect-slashing reversals, payment-channel downtime, and bad-data incidents — see [ADR 026 §5](026-gauge-boost-tokenomics.md#5-safety-and-insurance-reserve-3-bucket). |

## On-chain enforcement

| Term | Definition |
| --- | --- |
| **`StakingRegistry`** | The on-chain stake + node registration contract. Holds TOKEN stake, enforces `stake ≥ minStake`, performs slashing, manages the NodeId↔Ethereum-address binding. |
| **`SlashJudge`** | The on-chain contract that adjudicates all slashable offenses: verifies slash signatures, manages challenge bonds, runs counter-evidence windows, calls `StakingRegistry.slash()` — see [ADR 014](014-on-chain-verification.md). |
| **`BuybackBurner`** | The contract that swaps the 5% burn-bucket USDC into TOKEN via Balancer V3 80/20 weighted pool and burns the proceeds — see [ADR 003 § BuybackBurner](003-payments.md#buybackburner) and [ADR 018](018-liquidity-strategy.md). |
| **Slash signature** | An EIP-712 secp256k1 signature (`slash_sig`) on `ProbeResponse` / `StreamResponse`, used for on-chain slash evidence via `ecrecover` and as the message-body attribution signature for the paid-delivery path — see [ADR 014](014-on-chain-verification.md). |
| **Challenge bond** | The 100 TOKEN amount a challenger must post when submitting slash evidence. Returned on successful slash, forfeited on successful node counter (50% burned, 50% to node) — see [ADR 014 § Bond Handling](014-on-chain-verification.md#bond-handling). |
