# Appendix: PoC/Production Seam Architecture (Rust implementation)

> **This is an appendix, not a core protocol ADR.** The principle "PoC and production must be visibly distinct, with mode selection happening at composition boundaries rather than threaded through domain logic" is a protocol design rule. The specific implementation pattern here — Rust leaf domain crates plus a `node`-crate wiring layer, `#[cfg(feature = "poc")]` placement rules, and the mechanical-deletion graduation path — applies to this codebase. Other implementations may express the same principle differently (Go build tags, Python factories, runtime configuration).

## Context

Every ADR from 001–022 contains a PoC vs production split: different contracts, constants, simplified stand-in components, admin-key shortcuts, and deferred features. Without a canonical pattern for these differences, implementation produces scattered `if mode == PoC` checks across every crate, making both paths harder to reason about, test, and eventually remove.

The goal is a clean mechanical answer to: **how does the codebase express the difference between PoC and production?**

> **Cross-reference:** [ADR 026 — Gauge-Boost Tokenomics](026-gauge-boost-tokenomics.md) introduces a new contract surface (`FeeRouter`, `VotingEscrow`, `SafetyReserve`, swap helpers shared by `BuybackBurner` and the delegator-pool path). Seams 8–11 below cover the wiring-layer selectors for those contracts. The leaf-crate principle in §"Wiring Conventions" applies to ADR 026's tokenomics with the same force as to ADRs 001–022: domain crates remain free of ADR-026-mode-branching logic.

### Inventory of PoC/Production differences (from prior ADRs)

| Dimension | PoC | Production | Source |
|-----------|-----|------------|--------|
| Payment contract | `StablePaymentChannel` (USDC-only) | `PaymentChannel` (multi-token allowlist) | ADR 003, 010 |
| Governance | Single admin key (`onlyOwner`) | OpenZeppelin Governor + 2-day Timelock | ADR 009 |
| Challenge bond | 100 TOKEN | 100 TOKEN | ADR 026 |
| Buyback | Accumulate-only (`executeBuyback` never called) | Active execution via `BuybackBurner` after activation criteria met | ADR 018, 026 |
| Key management (node) | File-based (`~/.decdn/node.key`) | Platform keychain + hardware wallet via delegated hot key | ADR 012 |
| Key management (client) | File-based | Platform keychain; hardware wallet + derived hot key for voucher signing | ADR 012 |
| Reputation engine | Simplified stand-in (local observations only, no gossip weighting) | Full gossip-weighted scoring (70% local / 30% gossip, decay, cold-start) | ADR 008 |
| RPC trust | Single RPC endpoint | Multi-source (registry + DNS seed fallback) | ADR 001, 012 |
| Regional body registration | None — admin key is sole governance for blacklist | Regional bodies registered; global governance override | ADR 011 |
| Default-open allow-list | Inactive (`defaultOpenAllowlistActive == false`) — permissive bootstrap window: any active staker may serve as origin for `namespaceId == 0` | Activated by governance; only allow-listed operators appear in `OriginAssignment.getOrigins(0)` for default-open content; off-chain consumers (clients) consult that view to filter unauthorized origins | ADR 011, ADR 016 |
| Treasury disbursement | Manual (admin key holder) | On-chain governance proposal | ADR 009 |
| Bootstrap peer source | On-chain registry only | Registry + DNS seed list + minimum peer diversity (Option B+C) | ADR 012 |
| Multi-token `token_rates` gossip field | Omitted (single `rate_per_mb`) | Present alongside `rate_per_mb` | ADR 010 |

## Decision

**Use trait-based seams, with the single Cargo feature `poc` applied only to the `node` crate (the wiring point).** Leaf crates (`protocol`, `cache`) contain no mode-conditional code. Mode-specific implementations live in sibling modules within each crate; the `node` crate selects which concrete types to wire via `#[cfg(feature = "poc")]` and `#[cfg(not(feature = "poc"))]` attributes on separate function definitions.

No `if mode == PoC` checks appear in internal crate logic — all branching resolves at compile time at the top level. PoC code is physically absent from a production binary: excluded from compilation, not merely optimized away.

The single-feature design makes PoC removal straightforward: delete all `#[cfg(feature = "poc")]` functions and the `poc` feature declaration; the `#[cfg(not(feature = "poc"))]` production functions become unconditional with no further edits.

## Seam Definitions

Each seam is a Rust trait in the crate that owns the abstraction. PoC and production implementations are concrete structs in `poc/` and `prod/` submodules of that crate.

### 1. `KeyStore` — `crates/incentive`

```rust
pub trait KeyStore: Send + Sync {
    /// Load or generate the node's iroh Ed25519 key.
    fn load_node_key(&self) -> Result<iroh::SecretKey>;
    /// Load the node's Ethereum key for signing vouchers and EIP-712 messages.
    fn load_eth_key(&self) -> Result<EthPrivateKey>;
    /// Sign a voucher with the Ethereum key (may prompt HW wallet in production).
    fn sign_voucher(&self, voucher: &Voucher) -> Result<Signature>;
}
```

| | PoC | Production |
|---|-----|------------|
| Node key | Read from `~/.decdn/node.key` (plaintext file, created if absent) | Read from platform keychain (macOS Keychain / Linux `secret-service`) |
| Ethereum key | Read from `~/.decdn/eth.key` (plaintext file) | Platform keychain; hardware wallet with derived hot key for voucher signing |
| Voucher signing | Direct in-process signing | Hot key derived per-session; parent key never in memory |

### 2. `ReputationEngine` — `crates/reputation`

```rust
pub trait ReputationEngine: Send + Sync {
    fn score(&self, node_id: &NodeId) -> f32; // 0.0–1.0
    fn record_delivery(&self, node_id: &NodeId, outcome: DeliveryOutcome);
    fn record_gossip_report(&self, report: &ReputationReport);
}
```

| | PoC | Production |
|---|-----|------------|
| Gossip weighting | Ignored — local observations only | 30% gossip / 70% local, reporter weight capped 3×, time-decay half-life ≈ 7 weeks |
| Cold-start | Not implemented | One-time traffic allocation per operator address (prevents re-staking abuse) |
| Score decay | Not implemented | Decays toward neutral without fresh data |

### 3. `PaymentChannelClient` — `crates/incentive`

```rust
pub trait PaymentChannelClient: Send + Sync {
    fn open_channel(&self, params: OpenChannelParams) -> Result<ChannelId>;
    fn issue_voucher(&self, channel: ChannelId, amount_usdc: u64) -> Result<Voucher>;
    fn close_channel(&self, channel: ChannelId, voucher: &Voucher) -> Result<TxHash>;
    fn dispute_channel(&self, channel: ChannelId, evidence: &SlashEvidence) -> Result<TxHash>;
}
```

| | PoC | Production |
|---|-----|------------|
| Contract | `StablePaymentChannel` (USDC-only, ADR 003) | `PaymentChannel` (multi-token allowlist, ADR 010) |
| Token field | Hardcoded USDC | `payment_token` from `StreamRequest` |
| `token_rates` gossip | Omitted | Present (nodes advertise per-token rates) |

### 4. `GovernanceClient` — `crates/incentive`

```rust
pub trait GovernanceClient: Send + Sync {
    fn get_rate_bounds(&self) -> Result<RateBounds>;
    fn get_slash_params(&self) -> Result<SlashParams>;
    fn get_challenge_bond(&self) -> Result<u64>; // TOKEN base units
}
```

| | PoC | Production |
|---|-----|------------|
| Source | Direct RPC call to `StakingRegistry` (admin key controls params) | RPC call to Governor-managed params via Timelock |
| Challenge bond | 100 TOKEN | 100 TOKEN |
| Buyback | `BuybackBurner` receives fees; `executeBuyback` never called | Called by keeper after activation criteria met (ADR 018) |

### 5. `NetworkConstants` — `crates/protocol`

Not a trait — a plain struct with a constructor per mode. All mode-dependent numeric constants live here and nowhere else.

```rust
pub struct NetworkConstants {
    /// Challenge bond required to submit a slash claim (TOKEN base units).
    pub challenge_bond_token: u64,
    /// Default NodeAnnounce interval.
    pub announce_interval_secs: u64,
    /// Minimum stake to register a node (TOKEN base units).
    pub min_stake_token: u64,
    /// Default dispute window for payment channels.
    pub dispute_window_secs: u64,
    /// Bootstrap: minimum distinct peers required before accepting paid delivery.
    pub min_bootstrap_peers: usize,
}

impl NetworkConstants {
    pub fn poc() -> Self { /* ... */ }
    pub fn production() -> Self { /* ... */ }
}
```

Concrete values:

| Constant | PoC | Production |
|----------|-----|------------|
| `challenge_bond_token` | 100 TOKEN (1e20 base units) | 100 TOKEN (1e20 base units) |
| `announce_interval_secs` | 60 | 60 (same; tunable by governance) |
| `min_stake_token` | Operator-configured | Operator-configured; min enforced by contract |
| `dispute_window_secs` | 48 × 3600 (172800) | Governable 12h–72h; default 48h at genesis |
| `min_bootstrap_peers` | 3 | 8 |

### 6. `FeeRouterClient` — `crates/incentive`

Introduced by [ADR 026](026-gauge-boost-tokenomics.md) §2. `FeeRouter` receives the full operator USDC balance from `PaymentChannel.settleChannel` and atomically splits it into the six buckets. The seam selects between the deployed `FeeRouter` contract (canonical for every network deployment per [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics), including Arbitrum Sepolia testnet) and an in-process mock for unit/integration tests with no contracts deployed.

```rust
pub trait FeeRouterClient: Send + Sync {
    /// Address of the FeeRouter contract for the active chain.
    /// Returns `None` for the in-process test mock; never for a network deployment.
    fn router_address(&self) -> Option<Address>;
    /// Voucher-payload byte counts are forwarded into the router by the
    /// settlement transaction; this hook lets observers (metrics) snapshot
    /// per-settlement byte deltas without reading chain state.
    fn on_settlement(&self, operator: &Address, bytes_delivered: u64, amount_usdc: u64);
}
```

| | Tests / local dev | Network deployment |
|---|-----|------------|
| Implementation | `MockFeeRouterClient` — in-process mock for harnesses with no deployed contracts; never a deployment-time backend | `OnchainFeeRouterClient` — deployed `FeeRouter` address per network; full production split per [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics) (simplified-launch configs set bucket shares via `FeeRouter.setShares(...)` on the same contract, not a reduced-surface stub) |
| Settlement path | `PaymentChannel.settleChannel` interacts with the mock router; downstream buckets simulated for assertion | `PaymentChannel.settleChannel` calls `FeeRouter.routeSettlement(operator, bytesDelivered, amount, epochId)` in the same transaction |
| Per-network config | N/A | Address sourced from chain-id-keyed config; sum-to-100% safety bounds enforced on chain |

### 7. `VotingEscrowReader` — `crates/incentive`

Introduced by [ADR 026](026-gauge-boost-tokenomics.md) §4. ve-balance lookups are load-bearing for the gauge-boost epoch snapshot (ADR 026 §3) and ve-weighted governance (ADR 026 §9). The seam selects between an in-memory fixture (deterministic ve-balances for tests / local dev) and an on-chain `VotingEscrow.balanceOfAt(user, ts)` reader.

```rust
pub trait VotingEscrowReader: Send + Sync {
    /// ve-balance of `user` at the epoch-boundary timestamp `ts`.
    fn balance_of_at(&self, user: &Address, ts: u64) -> Result<U256>;
    /// Total ve-supply at the epoch-boundary timestamp `ts`.
    fn total_supply_at(&self, ts: u64) -> Result<U256>;
}
```

| | PoC | Production |
|---|-----|------------|
| Source | `FixtureVotingEscrowReader` — in-memory ve-balances seeded from config or test scaffolding | `OnchainVotingEscrowReader` — calls `VotingEscrow.balanceOfAt` / `totalSupplyAt` |
| Determinism | Fully deterministic; no chain dependency | Reads checkpoint array on the deployed `VotingEscrow` contract |
| Use sites | Gauge-boost share computation; ve-weighted governance simulations | Same call sites; selection happens in the wiring layer |

### 8. `SwapHelper` — `crates/incentive`

Introduced by [ADR 026](026-gauge-boost-tokenomics.md) §6, consolidated with [ADR 018](018-liquidity-strategy.md). Both `BuybackBurner` (5% burn bucket) and the delegator-pool USDC→TOKEN path (7% bucket) require a swap backend with TWAP windows, `minOut` slippage protection, and per-epoch liquidity caps; a single seam reduces wiring surface.

```rust
pub trait SwapHelper: Send + Sync {
    /// Execute a USDC→TOKEN swap subject to TWAP + minOut + per-epoch cap.
    /// Used by both BuybackBurner and the delegator-pool buy path.
    fn swap_usdc_for_token(&self, params: SwapParams) -> Result<SwapReceipt>;
}
```

| | PoC | Production |
|---|-----|------------|
| Backend | `MockPoolSwapHelper` — deterministic local pool with configurable price + slippage; no chain interaction | `BalancerV3SwapHelper` — Balancer V3 80/20 pool per [ADR 018](018-liquidity-strategy.md); private-RPC routing (Flashbots-style bundles); per-epoch liquidity caps enforced |
| Used by | `BuybackBurner` (burn) and `DelegatorBuyer` (or `BuybackBurner` multi-output mode) | Same call sites |
| MEV protection | N/A (deterministic mock) | TWAP windows, `minOut`, private RPC, per-epoch caps (hard requirement, not optional) |

### 9. `SafetyReservePayout` — `crates/incentive`

Introduced by [ADR 026](026-gauge-boost-tokenomics.md) §5. The 3% safety bucket is governance-gated; payouts require an attested incident bundle, governance proposal (or fast-track multisig within hard caps), 48-hour appeal window, and post-incident reporting. The seam selects between a local single-step approval mock (tests / local dev) and the Governor-gated production path.

```rust
pub trait SafetyReservePayout: Send + Sync {
    /// Submit an incident bundle for payout. PoC variant approves immediately;
    /// production variant queues either a governance proposal or fast-track
    /// emergency-multisig authorization (within the hard caps per ADR 009),
    /// subject to all four gates (evidence bundle, authorization, 48h appeal,
    /// post-incident reporting).
    fn submit_payout(&self, bundle: IncidentBundle) -> Result<PayoutHandle>;
    /// Status of a previously submitted bundle.
    fn payout_status(&self, handle: &PayoutHandle) -> Result<PayoutStatus>;
}
```

| | PoC | Production |
|---|-----|------------|
| Approval flow | `LocalApprovalSafetyReservePayout` — single-step approval; bypasses governance, appeal window, registry write | `GovernorSafetyReservePayout` — calls `SafetyReserve.payout(bundle, recipient, amount)` which enforces evidence bundle + governance proposal + 48h appeal + public registry post |
| Governance dependency | None | OpenZeppelin Governor + Timelock per [ADR 009](009-governance.md); emergency multisig under hard caps |
| Registry | Optional in-memory log | Public on-chain registry maintained by `SafetyReserve` |

## Cargo Feature: `poc`

The `poc` Cargo feature is declared **only on the `node` crate**. Leaf crates (`protocol`, `cache`, `reputation`, `incentive`) do not declare or use it. Production is the default — no flag is needed for a production build.

```toml
# crates/node/Cargo.toml (additions to existing [features])
[features]
default = []
otlp = [...]          # existing — OpenTelemetry export
poc  = []             # opt-in; PoC concrete implementations at wiring point
```

Usage in `crates/node/src/wiring.rs`:

```rust
pub fn build_components(config: &Config) -> Components {
    Components {
        key_store:     key_store(config),
        reputation:    reputation_engine(config),
        payment:       payment_client(config),
        governance:    governance_client(config),
        challenger:    corruption_challenger(config),
        constants:     network_constants(),
    }
}

#[cfg(feature = "poc")]
fn network_constants() -> NetworkConstants {
    NetworkConstants::poc()
}

#[cfg(not(feature = "poc"))]
fn network_constants() -> NetworkConstants {
    NetworkConstants::production()
}

#[cfg(feature = "poc")]
fn key_store(config: &Config) -> Arc<dyn KeyStore> {
    Arc::new(FileKeyStore::new(&config.key_dir))
}

#[cfg(not(feature = "poc"))]
fn key_store(config: &Config) -> Arc<dyn KeyStore> {
    Arc::new(KeychainKeyStore::new(&config.keychain))
}

// ... same pattern for remaining seams
```

This is a stronger guarantee than `if cfg!(feature = "poc")`: with the attribute form the PoC branch is **excluded from compilation entirely** in a production build. `FileKeyStore` and other PoC types are not present in the production binary at all — not merely optimized away.

`#[cfg(feature = "poc")]` and `#[cfg(not(feature = "poc"))]` appear **only** in `crates/node/src/wiring.rs` and `crates/node/src/main.rs`; they are **banned** in all other crates via a `rustflags` lint (see Rules below).

## Wiring Conventions

### Where implementations live

```
crates/
  incentive/
    src/
      key_store/
        mod.rs         — KeyStore trait
        file.rs        — PoC: FileKeyStore
        keychain.rs    — Production: KeychainKeyStore
      payment/
        mod.rs         — PaymentChannelClient trait
        stable.rs      — PoC: StablePaymentChannelClient
        multi_token.rs — Production: MultiTokenPaymentChannelClient
      ...
  reputation/
    src/
      engine/
        mod.rs         — ReputationEngine trait
        simple.rs      — PoC: SimpleReputationEngine
        weighted.rs    — Production: WeightedReputationEngine
  protocol/
    src/
      constants.rs     — NetworkConstants (both modes, no cfg)
  node/
    src/
      wiring.rs        — ONLY place #[cfg(feature = "poc")] appears
      main.rs          — reads config, calls wiring::build_components()
```

### Rules

1. **No `#[cfg(feature = "poc")]` or `#[cfg(not(feature = "poc"))]` outside `crates/node/src/wiring.rs` and `crates/node/src/main.rs`.** Enforced via `rustflags = ["-D", "unexpected_cfgs"]` with an explicit `check-cfg` list in `.cargo/config.toml`, or a `#[forbid(unexpected_cfgs)]` crate-level attribute on leaf crates.
2. **No runtime `NetworkMode` enum.** All mode selection is compile-time; a PoC binary cannot accidentally run in production mode.
3. **`NetworkConstants` is the single source of truth for all numeric differences.** No magic numbers elsewhere — always reference `constants.challenge_bond_token`, never literal `1e20`.
4. **Both implementations must compile in CI.** The CI matrix builds with `--features poc` and without (production), preventing either path from rotting and catching type errors in both concrete implementations.
5. **PoC removal is mechanical.** To graduate to production-only: delete all `#[cfg(feature = "poc")]` functions, remove the `poc` feature from `Cargo.toml`, and strip the `#[cfg(not(feature = "poc"))]` attributes from the remaining functions. No logic changes required.
6. **Leaf-crate principle applies to ADR 026 tokenomics.** Domain crates (`cache`, `gossip`, `incentive`, `reputation`, `protocol`) MUST NOT contain mode-branching logic for the [ADR 026](026-gauge-boost-tokenomics.md) contract surface (`FeeRouter`, `VotingEscrow`, `SafetyReserve`, swap helpers). All mode selection between PoC stubs / fixtures / mocks and production contracts lives in the `node` crate's wiring layer behind seams 8–11 above — the same rule that governs seams 1–7. Adding `if production_enabled` checks inside domain crate logic is forbidden.

## Consequences

### Positive

- Zero mode-conditional branches in internal crate logic
- Both modes tested in CI continuously — no surprise at production migration time
- PoC code is **physically absent** from a production binary (excluded at compile time, not just optimized away) — a hard security boundary
- Production is the default compile target — no flag needed, no accidental PoC deployment
- Removing PoC later is mechanical: delete `#[cfg(feature = "poc")]` functions, drop the feature, strip `#[cfg(not(feature = "poc"))]` attributes — no logic changes
- `NetworkConstants` gives operators a single reference for all tunable differences

### Negative

- Two concrete implementations must be maintained for each seam until production migration
- Adding a new seam requires registering it in `wiring.rs`; easy to forget

### Neutral

- Solidity contract selection is outside Rust's feature system — managed via separate Foundry deploy scripts, which is already the standard Foundry pattern

## Cross-ADR Impact

| ADR | Seam used | Notes |
|-----|-----------|-------|
| ADR 003 | `PaymentChannelClient` | `StablePaymentChannel` is PoC concrete impl |
| ADR 008 | `ReputationEngine` | `SimpleReputationEngine` for PoC |
| ADR 009 | `GovernanceClient` | Admin key vs Governor |
| ADR 010 | `PaymentChannelClient` | Multi-token client for production |
| ADR 012 | `KeyStore` | File-based vs keychain/HW wallet |
| ADR 018 | `SwapHelper` | Mock pool (PoC) vs Balancer V3 (production); shared by `BuybackBurner` and delegator-pool path |
| ADR 026 | `FeeRouterClient`, `VotingEscrowReader`, `SwapHelper`, `SafetyReservePayout` | production contract surface; PoC stubs / fixtures / mocks vs deployed contracts per network |
