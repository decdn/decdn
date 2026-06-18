# In-Process Voucher Ledger (Parallel Bundle Pulls) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let one process pull multiple blobs concurrently over a single reused payment channel (a bundle) without voucher-nonce collisions or double-counted bytes, by replacing the per-stream voucher snapshot with one shared, serialized channel ledger.

**Architecture:** A payment channel's voucher state (`nonce`, cumulative `bytes`, cumulative `amount`) is a single serial ledger. Today each stream snapshots it (`ChannelContext.prior_*` + a per-stream `VoucherProgress`), so two concurrent streams on one channel both sign `prior_nonce + 1` and both add their bytes to the same prior — collision + double-count. The fix introduces a `ChannelLedger`: one `Arc<Mutex<Cumulative>>` per channel that every concurrent stream draws from. Each voucher is issued under the lock (compute next from the live cumulative → sign → send → await ack → commit), so vouchers reach the node strictly monotonically while the byte transfers stay parallel. Single-blob pulls are just a ledger with one user.

**Tech Stack:** Rust (edition 2024), `alloy` (`U256`, EIP-712 signing), `tokio::sync::Mutex`, `decdn-protocol` framing, the existing `crates/node/tests/client_loopback.rs` harness for the concurrent integration test.

---

## File Structure

- `crates/client-pull/src/ledger.rs` — **new.** `Cumulative`, the pure `next_voucher` math, and the `ChannelLedger` (shared cumulative + serialized `issue`). This is the whole fix; isolating it in its own module keeps the large `lib.rs` focused and makes the money-math unit-testable without any network.
- `crates/client-pull/src/lib.rs` — declare `mod ledger;` / re-export; rewire `self_pay` + `fetch_inner` + `stream_fetch_tracked` to take an `Arc<ChannelLedger>` instead of `&ChannelContext` + `&mut VoucherProgress`.
- `crates/cli/src/commands/fetch.rs` — update the single-blob caller to build a `ChannelLedger` and read its final cumulative for persistence.
- `crates/node/tests/client_loopback.rs` — flip `client_concurrent_same_channel_accepts_one_voucher` to "both succeed"; add a 2-blob concurrent-success test.

---

## Task 1: Pure voucher math

**Files:**

- Create: `crates/client-pull/src/ledger.rs`
- Modify: `crates/client-pull/src/lib.rs` (add `mod ledger;`)

- [ ] **Step 1: Declare the module**

In `crates/client-pull/src/lib.rs`, add near the other module/use declarations (top of file):

```rust
mod ledger;
```

- [ ] **Step 2: Write the failing math tests**

Create `crates/client-pull/src/ledger.rs`:

```rust
//! Per-channel voucher ledger: the single serialized source of cumulative
//! voucher state shared by all concurrent streams on one payment channel.

use alloy::primitives::U256;
use decdn_protocol::MB_BYTES;

/// A channel's cumulative voucher state: the absolute totals carried by the most
/// recent voucher. All three advance monotonically over the channel's lifetime.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cumulative {
    /// Last voucher nonce (next voucher uses `nonce + 1`). `ZERO` for a fresh channel.
    pub nonce: U256,
    /// Cumulative channel bytes paid for.
    pub bytes: U256,
    /// Cumulative channel amount paid (token base units).
    pub amount: U256,
}

/// Compute the next voucher's absolute totals from the live cumulative and the
/// `delta_bytes` newly delivered (on any stream) since the last voucher. The
/// amount delta is `ceil(delta_bytes * rate_per_mb / 1 MiB)` so each voucher's
/// own delta covers its own bytes at the advertised rate (the node checks deltas).
#[must_use]
pub fn next_voucher(cur: &Cumulative, delta_bytes: u64, rate_per_mb: u64) -> Cumulative {
    let amount_delta = U256::from(delta_bytes)
        .saturating_mul(U256::from(rate_per_mb))
        .div_ceil(U256::from(MB_BYTES));
    Cumulative {
        nonce: cur.nonce.saturating_add(U256::from(1u64)),
        bytes: cur.bytes.saturating_add(U256::from(delta_bytes)),
        amount: cur.amount.saturating_add(amount_delta),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonce_increments_by_one() {
        let cur = Cumulative {
            nonce: U256::from(5u64),
            bytes: U256::ZERO,
            amount: U256::ZERO,
        };
        assert_eq!(next_voucher(&cur, 0, 10).nonce, U256::from(6u64));
    }

    #[test]
    fn bytes_accumulate_by_delta() {
        let cur = Cumulative {
            nonce: U256::ZERO,
            bytes: U256::from(1000u64),
            amount: U256::ZERO,
        };
        assert_eq!(next_voucher(&cur, 500, 10).bytes, U256::from(1500u64));
    }

    #[test]
    fn amount_rounds_up_per_voucher() {
        // 1 byte at rate 10/MiB rounds up to 1 (not 0).
        let cur = Cumulative::default();
        assert_eq!(next_voucher(&cur, 1, 10).amount, U256::from(1u64));
        // A full MiB at rate 10 costs exactly 10.
        let mib = u64::try_from(MB_BYTES).unwrap_or(u64::MAX);
        assert_eq!(next_voucher(&cur, mib, 10).amount, U256::from(10u64));
    }

    #[test]
    fn zero_delta_only_bumps_nonce() {
        let cur = Cumulative {
            nonce: U256::from(2u64),
            bytes: U256::from(7u64),
            amount: U256::from(3u64),
        };
        let next = next_voucher(&cur, 0, 99);
        assert_eq!(next.nonce, U256::from(3u64));
        assert_eq!(next.bytes, U256::from(7u64));
        assert_eq!(next.amount, U256::from(3u64));
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo nextest run -p decdn-client-pull ledger::tests`
Expected: FAIL — compile error until the module is wired, then on first build the tests run; if `mod ledger;` was added in Step 1 they compile and pass. To see a real red first, temporarily change `nonce + 1` to `nonce + 2` is NOT needed — instead confirm red by running before Step 2's body exists. (If already green, that is acceptable for a pure-function extraction; proceed.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo nextest run -p decdn-client-pull ledger::tests`
Expected: PASS — all four math tests green.

- [ ] **Step 5: Commit**

```bash
git add crates/client-pull/src/ledger.rs crates/client-pull/src/lib.rs
git commit -m "feat(client-pull): pure cumulative voucher math (next_voucher)"
```

---

## Task 2: The shared, serialized `ChannelLedger`

**Files:**

- Modify: `crates/client-pull/src/ledger.rs`

- [ ] **Step 1: Write the failing concurrency test**

Append to `crates/client-pull/src/ledger.rs` (inside the existing `#[cfg(test)] mod tests`, add these tests; they reference `ChannelLedger` which does not exist yet):

```rust
    #[tokio::test]
    async fn concurrent_issue_is_monotonic_and_exact() -> anyhow::Result<()> {
        use std::sync::Arc;

        let ledger = Arc::new(ChannelLedger::new(Cumulative::default()));
        let mut handles = Vec::new();
        // 50 concurrent issuers, each paying for 100 bytes at rate 10.
        for _ in 0..50u32 {
            let l = Arc::clone(&ledger);
            handles.push(tokio::spawn(async move {
                // Fake exchange: yield once (to interleave) then "ack".
                l.issue(100, 10, |_signed| async {
                    tokio::task::yield_now().await;
                    Ok(())
                })
                .await
            }));
        }

        let mut nonces = Vec::new();
        for h in handles {
            nonces.push(h.await??.nonce);
        }
        nonces.sort_unstable();
        // Nonces are exactly 1..=50, no gaps, no duplicates.
        let expected: Vec<U256> = (1..=50u64).map(U256::from).collect();
        assert_eq!(nonces, expected);

        let final_cum = ledger.snapshot().await;
        assert_eq!(final_cum.nonce, U256::from(50u64));
        assert_eq!(final_cum.bytes, U256::from(5000u64)); // 50 * 100
        Ok(())
    }

    #[tokio::test]
    async fn failed_exchange_does_not_commit() -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative::default());
        let result = ledger
            .issue(100, 10, |_signed| async { anyhow::bail!("ack lost") })
            .await;
        assert!(result.is_err(), "a failed exchange must surface the error");
        // Watermark unmoved: a signed-but-unacked voucher never advances state.
        assert_eq!(ledger.snapshot().await, Cumulative::default());
        Ok(())
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo nextest run -p decdn-client-pull ledger::tests::concurrent_issue`
Expected: FAIL — `ChannelLedger` not found.

- [ ] **Step 3: Implement `ChannelLedger`**

Insert above the `#[cfg(test)]` block in `crates/client-pull/src/ledger.rs`:

```rust
use std::future::Future;

use tokio::sync::Mutex;

/// One channel's live voucher ledger, shared by every concurrent stream on that
/// channel. Voucher issuance is serialized through the inner mutex: the lock is
/// held across the whole compute → sign → send → await-ack → commit cycle, so
/// vouchers reach the node in strict nonce order even while byte transfers run
/// in parallel on other streams.
#[derive(Debug)]
pub struct ChannelLedger {
    cumulative: Mutex<Cumulative>,
}

impl ChannelLedger {
    /// Build a ledger seeded from the channel's persisted cumulative state (the
    /// last voucher acked on earlier streams/invocations). Pass `Cumulative::default()`
    /// for a brand-new channel.
    #[must_use]
    pub const fn new(seed: Cumulative) -> Self {
        Self {
            cumulative: Mutex::new(seed),
        }
    }

    /// Read the current cumulative (for persistence after a pull completes).
    pub async fn snapshot(&self) -> Cumulative {
        *self.cumulative.lock().await
    }

    /// Issue one voucher for `delta_bytes` newly delivered since the last voucher.
    ///
    /// Holds the channel lock across `exchange`, which performs the actual I/O
    /// (write the signed voucher, await the node's ack). The next cumulative is
    /// computed from the live watermark *before* the I/O and committed *only* if
    /// `exchange` succeeds — a rejected or lost ack leaves the watermark unmoved.
    /// Returns the committed cumulative.
    ///
    /// `exchange` receives the next [`Cumulative`] (the values the caller must
    /// sign and send); the caller owns signing + framing so this module stays
    /// free of EIP-712 / wire types.
    pub async fn issue<F, Fut>(
        &self,
        delta_bytes: u64,
        rate_per_mb: u64,
        exchange: F,
    ) -> anyhow::Result<Cumulative>
    where
        F: FnOnce(Cumulative) -> Fut,
        Fut: Future<Output = anyhow::Result<()>>,
    {
        let mut guard = self.cumulative.lock().await;
        let next = next_voucher(&guard, delta_bytes, rate_per_mb);
        exchange(next).await?;
        *guard = next;
        Ok(next)
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo nextest run -p decdn-client-pull ledger::tests`
Expected: PASS — concurrency and failed-exchange tests green alongside the math tests.

- [ ] **Step 5: Lint**

Run: `cargo clippy -p decdn-client-pull --all-targets`
Expected: no warnings (no `unwrap`/`expect`/`panic`/indexing in non-test code).

- [ ] **Step 6: Commit**

```bash
git add crates/client-pull/src/ledger.rs
git commit -m "feat(client-pull): ChannelLedger serializes concurrent voucher issuance"
```

---

## Task 3: Rewire the pull to the ledger + concurrent integration test

This task replaces the per-stream `ChannelContext.prior_*` + `VoucherProgress` voucher path with a shared `Arc<ChannelLedger>`. The single-blob behavior must not change (existing `client_loopback.rs` tests are the regression guard); concurrent behavior becomes correct (new test).

**Files:**

- Modify: `crates/client-pull/src/lib.rs` (`self_pay`, `fetch_inner`, `stream_fetch_tracked`, `stream_fetch`)
- Modify: `crates/cli/src/commands/fetch.rs` (caller)
- Modify: `crates/node/tests/client_loopback.rs` (tests)

- [ ] **Step 1: Replace `self_pay`'s body to issue through the ledger**

In `crates/client-pull/src/lib.rs`, `self_pay` (currently `crates/client-pull/src/lib.rs:871`) signs and sends inline using `ctx.prior_* + progress`. Change its signature and body so it takes the ledger and the `delta_bytes` newly delivered since the last voucher, and issues through `ChannelLedger::issue`:

```rust
use crate::ledger::{ChannelLedger, Cumulative};

/// Sign and send a cumulative voucher covering `delta_bytes` of newly delivered
/// bytes, then await `VoucherAck`. Issuance is serialized per channel by the
/// ledger; the lock is held across the send→ack so concurrent streams stay
/// strictly ordered. The signing context (channel_id, token, signer, domain)
/// travels in `ctx`.
async fn self_pay(
    send: &mut SendStream,
    recv: &mut RecvStream,
    ctx: &ChannelContext,
    ledger: &ChannelLedger,
    rate_per_mb: u64,
    delta_bytes: u64,
) -> anyhow::Result<()> {
    ledger
        .issue(delta_bytes, rate_per_mb, |next: Cumulative| async move {
            let signed = Voucher {
                channel_id: ctx.channel_id,
                amount: next.amount,
                nonce: next.nonce,
                bytes_delivered: next.bytes,
                token: ctx.token,
            }
            .sign(ctx.client_signer.as_ref(), &ctx.voucher_domain)
            .map_err(|e| anyhow::anyhow!("voucher signing failed: {e}"))?;

            write_message(send, &ClientMessage::Voucher(signed_to_wire_voucher(&signed))).await?;
            match read_client_message(recv).await? {
                ClientMessage::VoucherAck => Ok(()),
                ClientMessage::StreamError(StreamError::VoucherRejected { reason }) => {
                    Err(anyhow::Error::new(UpstreamVoucherRejected { reason }))
                }
                ClientMessage::StreamError(e) => {
                    anyhow::bail!("unexpected stream error awaiting voucher ack: {e:?}")
                }
                other => anyhow::bail!("expected VoucherAck, got {}", variant_name(&other)),
            }
        })
        .await
        .map(|_committed| ())
}
```

- [ ] **Step 2: Track `delta_bytes` in the receive loop and pass the ledger**

In `fetch_inner` (`crates/client-pull/src/lib.rs:362`), the receive loop currently passes the stream's running total (`stream_bytes`) to `self_pay`. Change it to track bytes delivered since the last voucher and pass that delta. Replace the loop's `self_pay(...)` call site:

```rust
// `bytes_since_voucher` accumulates received bytes; reset to 0 after each
// voucher so the ledger gets per-voucher deltas (it owns the cumulative).
self_pay(&mut send, &mut recv, ctx, ledger, rate_per_mb, bytes_since_voucher).await?;
bytes_since_voucher = 0;
```

Declare `let mut bytes_since_voucher: u64 = 0;` before the loop, and add the chunk size to it on each received chunk (where the old code advanced `stream_bytes`). The voucher-cadence trigger (`DEFAULT_VOUCHER_INTERVAL_MB`, `crates/client-pull/src/lib.rs:43`) is unchanged — it just now fires on `bytes_since_voucher`.

- [ ] **Step 3: Thread `Arc<ChannelLedger>` through the public entrypoints**

Change `fetch_inner`, `stream_fetch_tracked`, and `stream_fetch` to take `ledger: &ChannelLedger` in place of the `progress: &mut VoucherProgress` out-param. The caller reads the final cumulative from the ledger via `ledger.snapshot().await` after the pull (replacing `progress.acked()`). `ChannelContext` keeps only the signing fields (`channel_id`, `token`, `client_signer`, `voucher_domain`); the `prior_*` fields move into the ledger seed (built by `for_buyer_channel` → `Cumulative { nonce: state.last_nonce, bytes: state.last_bytes_delivered, amount: state.last_amount }`).

- [ ] **Step 4: Update the single-blob caller**

In `crates/cli/src/commands/fetch.rs` (around `crates/cli/src/commands/fetch.rs:394`), replace the `VoucherProgress` plumbing:

```rust
let seed = Cumulative {
    nonce: ctx.prior_nonce,
    bytes: ctx.prior_bytes_delivered,
    amount: ctx.prior_amount,
};
let ledger = std::sync::Arc::new(ChannelLedger::new(seed));
let result = stream_fetch_tracked(
    &endpoint, target, &ctx, &ledger, &slash_dom, provider, hash, 0,
    timestamp_us, Duration::from_millis(args.timeout_ms), max_blob_bytes,
)
.await;

let final_cum = ledger.snapshot().await;
if final_cum.nonce > ctx.prior_nonce {
    // Something was acked; persist the new watermark (same as the old `acked()` path).
    if let Err(e) = store.advance_progress(
        provider, channel_id, final_cum.nonce, final_cum.bytes, final_cum.amount,
    ) {
        eprintln!("warning: failed to persist voucher watermark for channel {channel_id}: {e}");
    }
}
```

- [ ] **Step 5: Run the existing single-blob tests (regression guard)**

Run: `cargo nextest run -p decdn-node --test client_loopback`
Expected: the existing single-blob delivery + watermark tests PASS unchanged (behavior preserved for N=1).

- [ ] **Step 6: Flip the concurrent test and add a success test**

In `crates/node/tests/client_loopback.rs`, the test `client_concurrent_same_channel_accepts_one_voucher` (`crates/node/tests/client_loopback.rs:1639`) asserts exactly one of two concurrent fetches succeeds. Replace it with a test that drives both fetches against **one shared `Arc<ChannelLedger>`** and asserts both succeed:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn client_concurrent_same_channel_both_succeed() -> anyhow::Result<()> {
    let payload_a = vec![0xA1u8; 4096];
    let payload_b = vec![0xB2u8; 4096];
    let (store, signer, deposit) = seeded_store()?;

    // ONE ledger shared by both concurrent pulls (seed = fresh channel).
    let ledger = std::sync::Arc::new(ChannelLedger::new(Cumulative::default()));
    let ctx = channel_context(std::sync::Arc::clone(&signer), deposit);

    let (ra, rb) = tokio::join!(
        stream_fetch(&client_ep, target.clone(), &ctx, &ledger, /* …hash_a… */),
        stream_fetch(&client_ep, target.clone(), &ctx, &ledger, /* …hash_b… */),
    );
    ra?;
    rb?;

    // Both paid: cumulative advanced by both payloads, nonce ran past 1.
    let cum = ledger.snapshot().await;
    anyhow::ensure!(cum.nonce >= U256::from(2u64), "both vouchers must be accepted");
    anyhow::ensure!(
        cum.bytes == U256::from((payload_a.len() + payload_b.len()) as u64),
        "cumulative bytes must cover both blobs"
    );
    Ok(())
}
```

Fill the `/* …hash… */` placeholders with the two blobs' hashes using the harness's existing serve-setup helpers (mirror the setup in the test you are replacing). Keep the rest of the harness (endpoint, target, seeded channel) identical to the original test.

- [ ] **Step 7: Run the concurrent test**

Run: `cargo nextest run -p decdn-node --test client_loopback client_concurrent_same_channel_both_succeed`
Expected: PASS — both pulls complete, `nonce >= 2`, cumulative bytes == both payloads.

- [ ] **Step 8: Full crate check + lint + fmt**

Run: `cargo nextest run -p decdn-client-pull -p decdn-node`
Run: `cargo clippy --all-targets`
Run: `cargo fmt -- --check`
Expected: all green, no warnings, no diff.

- [ ] **Step 9: Commit**

```bash
git add crates/client-pull/src/lib.rs crates/cli/src/commands/fetch.rs crates/node/tests/client_loopback.rs
git commit -m "feat(client-pull): share one voucher ledger across concurrent pulls"
```

---

## Self-Review

- **Spec coverage:** The deliverable is "in-process voucher coordination for parallel pulls over one reused channel." `next_voucher` (Task 1) is the cumulative math; `ChannelLedger` (Task 2) serializes issuance with the lock held across send→ack; Task 3 wires it into the real pull path and proves both single-blob (regression) and concurrent (new) correctness. ✅
- **Placeholder scan:** one intentional `/* …hash… */` in the integration test — it must be filled from the harness's serve-setup helpers, which differ by however `seeded_store`/`channel_context` are defined in the current `client_loopback.rs`; the surrounding assertions and structure are complete. Everything else is concrete.
- **Type consistency:** `Cumulative { nonce, bytes, amount }` and `ChannelLedger::{new, snapshot, issue}` names match across Tasks 1–3 and the caller. `self_pay` takes `delta_bytes: u64`; the loop passes `bytes_since_voucher: u64`. `ChannelContext` loses `prior_*` (moved to the ledger seed) — every reader of those fields is updated in Task 3/4.
- **Scope:** single subsystem (the client pull's voucher path); no daemon, no IPC, no UI. Parallel-bundle *orchestration* (spawning N pulls over one ledger) is exercised by the test and left to the bundle-fetch caller as a thin `tokio::join!`/`JoinSet` layer — not new infrastructure.

---

## Execution Handoff

After the three tasks are green, parallel pulls over one channel are correct in-process. The concurrent integration test replaces the old "exactly one succeeds" assertion that encoded the bug. No cross-process machinery is introduced — per the issue decision, multi-instance parallelism is handled by per-context wallet derivation, not coordination.
