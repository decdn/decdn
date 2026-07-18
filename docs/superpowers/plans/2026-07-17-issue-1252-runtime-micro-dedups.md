# Issue #1252 Runtime Micro-Deduplications Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish and verify the inherited shared `PruneGuard`, single-source the runtime EIP-712 domains, and centralize HTTP provider construction.

**Architecture:** The node crate owns one panic-safe prune guard. `run()` constructs its three immutable signing domains once and clones them into consumers. A private `ProviderFactory` supplies role-specific Alloy providers while preserving per-role polling and nonce policy.

**Tech Stack:** Rust 2024, Alloy providers/signers/EIP-712 types, Cargo Nextest, rustfmt, Clippy.

## Global Constraints

- Preserve Claude's inherited edits and harden them; the user explicitly waived recreating them through strict test-first development.
- Rename the generic inherited branch to `refactor/1252-runtime-micro-dedups` and rebase it onto current local `main` before verification.
- Do not change limiter compare-exchange or sweep behavior.
- Construct `slash_judge_domain`, `voucher_domain`, and `bind_node_id_domain` once each from the same chain ID and non-zero addresses used today.
- Preserve separate seller and buyer provider instances, simple nonce management, pending-transaction poll intervals, and the plain shared-head provider.
- Do not expand scope into address parsing or watcher ownership changes.

---

### Task 1: Rebase and name the inherited worktree safely

**Files:**

- Preserve: `crates/node/src/dispatch.rs`
- Preserve: `crates/node/src/lib.rs`
- Preserve: `crates/node/src/rate_limit.rs`
- Preserve: `crates/node/src/runtime/mod.rs`
- Preserve: `crates/node/src/prune_guard.rs`

**Interfaces:**

- Consumes: Claude's dirty worktree and current local `main`.
- Produces: the inherited patch on a correctly named, current branch.

- [ ] **Step 1: Inspect and stash the inherited production files**

Run:

```bash
git status --short --branch
git diff --check
git stash push -u -m codex-1252-inherited -- crates/node/src/dispatch.rs crates/node/src/lib.rs crates/node/src/rate_limit.rs crates/node/src/runtime/mod.rs crates/node/src/prune_guard.rs
```

Expected: the production work is in one stash and the worktree is clean.

- [ ] **Step 2: Rename and rebase the branch**

Run:

```bash
git branch -m refactor/1252-runtime-micro-dedups
git rebase main
git stash pop
git diff --check
```

Expected: the branch is based on current `main`; the five inherited production paths are restored without whitespace errors.

### Task 2: Validate the shared `PruneGuard`

**Files:**

- Create: `crates/node/src/prune_guard.rs`
- Modify: `crates/node/src/lib.rs`
- Modify: `crates/node/src/dispatch.rs`
- Modify: `crates/node/src/rate_limit.rs`
- Test: `crates/node/src/dispatch.rs`
- Test: `crates/node/src/rate_limit.rs`

**Interfaces:**

- Produces: `pub(crate) struct PruneGuard<'a>(pub(crate) &'a AtomicBool)` with a `Drop` implementation that stores `false` using `Ordering::Release`.
- Consumes: both limiters' existing single-flight `AtomicBool` fields.

- [ ] **Step 1: Run both existing unwind tests against the inherited extraction**

Run:

```bash
cargo nextest run -p decdn-node -E 'test(prune_guard_resets_flag_on_panic)'
```

Expected: both dispatch and rate-limit tests pass and prove the shared guard releases each limiter's flag during unwind.

- [ ] **Step 2: Audit that only one production guard definition remains**

Run:

```bash
rg -n 'struct PruneGuard|impl Drop for PruneGuard' crates/node/src
rg -n 'prune_guard::PruneGuard' crates/node/src/dispatch.rs crates/node/src/rate_limit.rs
```

Expected: the definition and `Drop` implementation appear only in `prune_guard.rs`; both consumers import it.

### Task 3: Compute the EIP-712 domains once

**Files:**

- Modify: `crates/node/src/runtime/mod.rs`

**Interfaces:**

- Produces: local `slash_domain`, `voucher_domain`, and `bind_domain` values constructed once.
- Consumes: parsed `slash_judge_addr`, `payment_channel_addr`, `capacity_bond_addr`, and `cfg.blockchain.chain_id`.

- [ ] **Step 1: Keep the three canonical constructions**

Use these constructions after their verifying addresses are available:

```rust
let slash_domain =
    decdn_incentive::slash_judge_domain(cfg.blockchain.chain_id, slash_judge_addr);
let voucher_domain =
    decdn_incentive::voucher_domain(cfg.blockchain.chain_id, payment_channel_addr);
let bind_domain =
    decdn_incentive::bind_node_id_domain(cfg.blockchain.chain_id, capacity_bond_addr);
```

- [ ] **Step 2: Replace later re-derivations with clones or final moves**

Use clones for the probe/client consumers and move the remaining values into the background buyer/node-origin setup:

```rust
slash_domain.clone()
voucher_domain.clone()
bind_domain.clone()
```

Set the background values from the existing domains:

```rust
let buyer_voucher_domain = voucher_domain;
let node_origin_slash_domain = slash_domain;
let node_origin_bind_domain = bind_domain;
```

Do not change any consumer's domain type or verifying address.

- [ ] **Step 3: Audit production construction counts**

Run:

```bash
rg -n 'decdn_incentive::(slash_judge_domain|voucher_domain|bind_node_id_domain)\(' crates/node/src/runtime/mod.rs
```

Expected: exactly three production matches, one for each domain function; test-only matches outside `run()` may remain.

### Task 4: Introduce and test the provider factory

**Files:**

- Modify: `crates/node/src/runtime/mod.rs`
- Test: `crates/node/src/runtime/mod.rs` test module

**Interfaces:**

- Produces: `ProviderFactory::{read_only, seller_wallet, buyer_wallet, shared_head}`.
- Consumes: Alloy HTTP URLs, `PrivateKeySigner`, and the configured `Duration` poll interval.

- [ ] **Step 1: Add the private role-based constructors**

Add this private shape near `with_poll_interval`, retaining that helper as the single poll setter:

```rust
type HttpUrl = alloy::transports::http::reqwest::Url;

struct ProviderFactory;

impl ProviderFactory {
    fn read_only(url: HttpUrl, interval: Duration) -> impl Provider + Clone {
        with_poll_interval(ProviderBuilder::new().connect_http(url), interval)
    }

    fn shared_head(url: HttpUrl) -> impl Provider + Clone {
        ProviderBuilder::new().connect_http(url)
    }

    fn seller_wallet(
        url: HttpUrl,
        signer: PrivateKeySigner,
        interval: Duration,
    ) -> impl Provider + Clone {
        Self::wallet(url, signer, interval)
    }

    fn buyer_wallet(
        url: HttpUrl,
        signer: PrivateKeySigner,
        interval: Duration,
    ) -> impl Provider + Clone {
        Self::wallet(url, signer, interval)
    }

    fn wallet(
        url: HttpUrl,
        signer: PrivateKeySigner,
        interval: Duration,
    ) -> impl Provider + Clone {
        with_poll_interval(
            ProviderBuilder::new()
                .with_simple_nonce_management()
                .wallet(EthereumWallet::from(signer))
                .connect_http(url),
            interval,
        )
    }
}
```

- [ ] **Step 2: Pin the four role policies in a unit test**

Add alongside `with_poll_interval_overrides_alloy_local_default`:

```rust
#[test]
fn provider_factory_preserves_role_polling_policy() {
    let url: HttpUrl = "http://localhost:8545".parse().expect("valid URL");
    let interval = Duration::from_secs(7);

    let read = ProviderFactory::read_only(url.clone(), interval);
    let head = ProviderFactory::shared_head(url.clone());
    let seller = ProviderFactory::seller_wallet(
        url.clone(),
        PrivateKeySigner::random(),
        interval,
    );
    let buyer = ProviderFactory::buyer_wallet(url, PrivateKeySigner::random(), interval);

    assert_eq!(read.client().poll_interval(), interval);
    assert_eq!(head.client().poll_interval(), Duration::from_millis(250));
    assert_eq!(seller.client().poll_interval(), interval);
    assert_eq!(buyer.client().poll_interval(), interval);
}
```

- [ ] **Step 3: Run the new policy test before migrating call sites**

Run:

```bash
cargo nextest run -p decdn-node provider_factory_preserves_role_polling_policy
```

Expected: the factory test passes; it uses no network I/O because `poll_interval()` reads local client state.

- [ ] **Step 4: Migrate every production provider in `run()`**

Replace read-only watcher/indexer clients with `ProviderFactory::read_only`, the two wallet clients with their seller/buyer methods, and the `SharedHead` client with `ProviderFactory::shared_head`. Preserve the explicit cloned URLs and do not share returned provider instances.

- [ ] **Step 5: Audit direct builder sites**

Run:

```bash
rg -n 'ProviderBuilder::new\(\).*connect_http|ProviderBuilder::new\(\)' crates/node/src/runtime/mod.rs
```

Expected: production builder calls are confined to `ProviderFactory`; test-only construction may remain in the runtime test module.

### Task 5: Verify and commit the complete micro-dedup refactor

**Files:**

- Verify all paths from Tasks 2-4.

- [ ] **Step 1: Run formatting, focused tests, the node suite, and Clippy**

Run:

```bash
cargo fmt -- --check
cargo nextest run -p decdn-node -E 'test(prune_guard_resets_flag_on_panic|provider_factory_preserves_role_polling_policy|with_poll_interval_overrides_alloy_local_default)'
cargo nextest run -p decdn-node
cargo clippy -p decdn-node --all-targets -- -D warnings
```

Expected: all commands exit 0 with no warnings.

- [ ] **Step 2: Commit the production changes**

Run:

```bash
git add crates/node/src/dispatch.rs crates/node/src/lib.rs crates/node/src/prune_guard.rs crates/node/src/rate_limit.rs crates/node/src/runtime/mod.rs
git commit -m "refactor(node): dedupe runtime helpers (#1252)"
```

Expected: branch status is clean and contains separate design and implementation commits.
