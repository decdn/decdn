# ADR 023 — PoC/Production Seam Architecture

**Status:** Accepted  
**Date:** 2026-04-08  
**Deciders:** Core team

---

## Context

Every ADR from 001–022 contains a PoC vs production split: different contracts, different constants, simplified stand-in components, admin-key shortcuts, and deferred features. Without a canonical architectural pattern for managing these differences, implementation will produce scattered `if mode == Poc` checks throughout every crate, making both the PoC and the production path harder to reason about, test, and eventually remove.

The goal is a clean mechanical answer to: **how does the codebase express the difference between PoC and production?**

### Inventory of PoC/Production differences (from prior ADRs)

| Dimension | PoC | Production | Source |
|-----------|-----|------------|--------|
| Payment contract | `StablePaymentChannel` (USDC-only) | `PaymentChannel` (multi-token allowlist) | ADR 003, 010 |
| Governance | Single admin key (`onlyOwner`) | OpenZeppelin Governor + 2-day Timelock | ADR 009 |
| Challenge bond | 100 TOKEN | 50 TOKEN | ADR 004 |
| Corruption verification | Optimistic challenge-response (signed `StreamResponse`) | Interactive keccak256 Merkle proof over 1 KiB chunks | ADR 014 |
| Buyback | Accumulate-only (`executeBuyback` never called) | Active execution via `BuybackBurner` after activation criteria met | ADR 004, 018 |
| Key management (node) | File-based (`~/.decdn/node.key`) | Platform keychain + hardware wallet via delegated hot key | ADR 012 |
| Key management (client) | File-based | Platform keychain; hardware wallet + derived hot key for voucher signing | ADR 012 |
| Reputation engine | Simplified stand-in (local observations only, no gossip weighting) | Full gossip-weighted scoring (70% local / 30% gossip, decay, cold-start) | ADR 008 |
| Watchtower | Simplified — client monitors its own channels | Full — dedicated `WatchtowerEscrow` + `cdn/watchtower/v1` | ADR 007 |
| RPC trust | Single RPC endpoint | Multi-source (registry + DNS seed fallback) | ADR 001, 012 |
| `popular_hashes` cardinality | 20 hashes per `NodeAnnounce` | 5 hashes (reduces content inventory leakage — ADR 017) | ADR 001, 017 |
| Regional body registration | None — admin key is sole governance for blacklist | Regional bodies registered; global governance override | ADR 011 |
| Treasury disbursement | Manual (admin key holder) | On-chain governance proposal | ADR 009 |
| `adminReclaimNodeId` | Present — `onlyOwner` fallback for NodeId squatting during early testing | Removed from contract | ADR 001 |
| Bootstrap peer source | On-chain registry only | Registry + DNS seed list + minimum peer diversity (Option B+C) | ADR 012 |
| `cdn/watchtower/v1` ALPN | Not used | Active — nodes register with watchtowers | ADR 007 |
| Multi-token `token_rates` gossip field | Omitted (single `rate_per_mb`) | Present alongside `rate_per_mb` | ADR 010 |

---

## Decision

**Use trait-based seams, with the single Cargo feature `poc` applied only to the `node` crate (the wiring point).** Leaf crates (`protocol`, `cache`) contain no mode-conditional code. Mode-specific implementations live in sibling modules within each crate; the `node` crate selects which concrete types to wire based on the `poc` feature.

No `if mode == PoC` checks appear in internal crate logic. All branching is resolved at compile time at the top level.

---

## Seam Definitions

Each seam is a Rust trait in the crate that owns the abstraction. The PoC and production implementations are concrete structs in `poc/` and `prod/` submodules of that crate.

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
| Challenge bond | 100 TOKEN | 50 TOKEN |
| Buyback | `BuybackBurner` receives fees; `executeBuyback` never called | Called by keeper after activation criteria met (ADR 018) |

### 5. `WatchtowerClient` — `crates/incentive`

```rust
pub trait WatchtowerClient: Send + Sync {
    /// Register a channel with one or more watchtowers.
    fn register_channel(&self, channel: ChannelId, watchtower: &NodeId) -> Result<()>;
    /// Called when a `ChannelCloseInitiated` event is observed.
    fn on_close_initiated(&self, channel: ChannelId, event: &CloseEvent) -> Result<()>;
}
```

| | PoC | Production |
|---|-----|------------|
| Implementation | `NoopWatchtowerClient` — client monitors its own channels directly | `cdn/watchtower/v1` ALPN; watchtower nodes use `WatchtowerEscrow` contract |
| `WatchtowerAnnounce` gossip | Not emitted or processed | Emitted by watchtower nodes; subscribed to on `cdn/global/v1` |

### 6. `CorruptionChallenger` — `crates/incentive`

```rust
pub trait CorruptionChallenger: Send + Sync {
    fn submit_challenge(&self, evidence: &CorruptionEvidence) -> Result<TxHash>;
}
```

| | PoC | Production |
|---|-----|------------|
| Evidence | Signed `StreamResponse` + 100 TOKEN bond; 24h counter-evidence window | Interactive keccak256 Merkle proof over 1 KiB chunks; requires keeper to resolve |

### 7. `NetworkConstants` — `crates/protocol`

Not a trait — a plain struct with a constructor per mode. All mode-dependent numeric constants live here and nowhere else.

```rust
pub struct NetworkConstants {
    /// Maximum hashes in a NodeAnnounce popular_hashes field.
    pub popular_hashes_max: usize,
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
| `popular_hashes_max` | 20 | 5 (ADR 017 — reduces content inventory leakage) |
| `challenge_bond_token` | 100 TOKEN (1e20 base units) | 50 TOKEN |
| `announce_interval_secs` | 60 | 60 (same; tunable by governance) |
| `min_stake_token` | Operator-configured | Operator-configured; min enforced by contract |
| `dispute_window_secs` | 48 × 3600 (172800) | Governable 12h–72h; default 48h at genesis |
| `min_bootstrap_peers` | 3 | 8 |

---

## Cargo Feature: `poc`

The `poc` Cargo feature is declared **only on the `node` crate**. Leaf crates (`protocol`, `cache`, `reputation`, `incentive`) do not declare or use it.

```toml
# crates/node/Cargo.toml
[features]
default = []          # production is the default — no flag required
poc = []              # opt-in; enables PoC concrete implementations at wiring point
```

Usage in `crates/node/src/wiring.rs`:

```rust
pub fn build_components(config: &Config) -> Components {
    Components {
        key_store:     key_store(config),
        reputation:    reputation_engine(config),
        payment:       payment_client(config),
        governance:    governance_client(config),
        watchtower:    watchtower_client(config),
        challenger:    corruption_challenger(config),
        constants:     network_constants(),
    }
}

fn network_constants() -> NetworkConstants {
    #[cfg(feature = "poc")]
    return NetworkConstants::poc();
    #[cfg(not(feature = "poc"))]
    return NetworkConstants::production();
}

fn key_store(config: &Config) -> Arc<dyn KeyStore> {
    #[cfg(feature = "poc")]
    return Arc::new(FileKeyStore::new(&config.key_dir));
    #[cfg(not(feature = "poc"))]
    return Arc::new(KeychainKeyStore::new(&config.keychain));
}

// ... same pattern for remaining seams
```

`#[cfg(feature = "poc")]` appears **only** in `crates/node/src/wiring.rs` and `crates/node/src/main.rs`. It is **banned** in all other crates via a `rustflags` lint (see Enforcement below).

### Contracts: `unsafe-admin` feature

The Solidity `adminReclaimNodeId` function (ADR 001) is removed in production contracts. Foundry controls this via a separate deploy script — not a Rust feature flag. The PoC deploy script (`script/DeployPoc.s.sol`) deploys `PocStakingRegistry`, which extends `StakingRegistry` with `adminReclaimNodeId`. The production deploy script (`script/DeployProduction.s.sol`) deploys `StakingRegistry` directly.

No Rust `unsafe-admin` compile flag is needed — the function simply does not exist on the production contract ABI.

---

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
      watchtower/
        mod.rs         — WatchtowerClient trait
        noop.rs        — PoC: NoopWatchtowerClient
        live.rs        — Production: LiveWatchtowerClient
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

1. **No `#[cfg(feature = "poc")]` outside `crates/node/src/wiring.rs` and `crates/node/src/main.rs`.** Enforced via `rustflags = ["-D", "unexpected_cfgs"]` with an explicit `check-cfg` list in `.cargo/config.toml`, or a `#[forbid(unexpected_cfgs)]` crate-level attribute on leaf crates.
2. **No runtime `NetworkMode` enum.** All mode selection is compile-time. A PoC binary cannot accidentally run in production mode.
3. **`NetworkConstants` is the single source of truth for all numeric differences.** No magic numbers elsewhere — always reference `constants.popular_hashes_max`, never literal `20`.
4. **Both implementations must compile in CI.** The CI matrix builds with and without `--features poc`. This prevents PoC-only code rot and production-only breaks.
5. **PoC implementations may panic on unimplemented production paths** (e.g., `KeychainKeyStore` is not compiled into a PoC binary), but must not `todo!()` on PoC paths that could be triggered at runtime.

---

## Consequences

### Positive
- Zero mode-conditional branches in internal crate logic
- Both modes are tested in CI continuously — no surprise at production migration time
- Production is the default compile target — no flag needed, no accidental PoC deployment
- Removing PoC support later is a mechanical delete: remove `poc` feature, delete `file.rs`/`simple.rs`/`noop.rs`, remove `wiring.rs` `cfg` blocks
- `NetworkConstants` gives operators a single reference for all tunable differences

### Negative
- Two concrete implementations must be maintained for each seam until production migration
- Adding a new seam requires registering it in `wiring.rs`; easy to forget

### Neutral
- Solidity contract selection is outside Rust's feature system — managed via separate Foundry deploy scripts, which is already the standard Foundry pattern

---

## Alternatives Considered

### Runtime `NetworkMode` enum throughout
Rejected. Leads to `if mode == Poc` branches scattered across all crates. Makes it impossible to statically verify that no PoC code runs in a production binary.

### Single implementation with `Option`-typed production fields
Rejected. `Option<WatchtowerClient>` forces every call site to unwrap and handle the None case, which is just a verbose runtime mode-check with worse ergonomics.

### Two separate repositories
Rejected. Shared protocol types, cache logic, and contract interaction code is large enough that duplication would create divergence. The trait abstraction achieves the same clean separation within a monorepo.

### Compile-time `#[cfg(feature = "poc")]` throughout all crates
Rejected. Scatters the PoC/production boundary into every crate, making it hard to track all the differences and audit the production surface. Centralizing in `wiring.rs` gives a single readable inventory.

---

## Cross-ADR Consistency

| ADR | Seam used | Notes |
|-----|-----------|-------|
| ADR 003 | `PaymentChannelClient` | `StablePaymentChannel` is PoC concrete impl |
| ADR 004 | `GovernanceClient`, `NetworkConstants` | Challenge bond, dispute window, buyback |
| ADR 007 | `WatchtowerClient` | `NoopWatchtowerClient` for PoC |
| ADR 008 | `ReputationEngine` | `SimpleReputationEngine` for PoC |
| ADR 009 | `GovernanceClient` | Admin key vs Governor |
| ADR 010 | `PaymentChannelClient` | Multi-token client for production |
| ADR 012 | `KeyStore` | File-based vs keychain/HW wallet |
| ADR 014 | `CorruptionChallenger` | Optimistic vs Merkle proof |
| ADR 017 | `NetworkConstants.popular_hashes_max` | 20 (PoC) vs 5 (production) |
