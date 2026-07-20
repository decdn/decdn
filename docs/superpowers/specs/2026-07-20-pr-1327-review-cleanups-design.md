# PR #1327 Review Cleanups Design

## Goal

Apply the actionable review feedback to the idle-close regression tests in
`crates/node/tests/client_loopback.rs` without changing production behavior or
expanding the PR beyond its existing test-only scope.

## Changes

1. Derive the sequential stream activity cadence from the idle window with
   `idle.mul_f64(0.6)`. With a one-shot re-arm mutant, the stale timer then
   expires during the second explicit `conn.closed()` guard. The failure points
   at the broken re-arm round instead of relying only on the final timing floor.
2. Replace the three `VoucherTotals` saturating additions with checked additions.
   Overflow becomes a descriptive `anyhow::Error` propagated by
   `pay_and_finish`, consistent with the workspace anti-panic policy.
3. Remove the two client-only assertions that merely re-check values just
   computed in `VoucherTotals`. Assert the persisted server channel directly
   against nonce `2` and the sum of both blob wire lengths.
4. Add short comments to both new timing-floor assertions explaining that the
   server re-arms after writing `StreamEnd`. Because the client records its
   completion timestamp when it reads that message, the measured interval can
   only over-report the server's own idle delay.

## Error Handling

`U256` and `u64` overflow in voucher accumulation is treated as a test-fixture
error. Each `checked_add` converts `None` into a field-specific `anyhow` error;
the test exits cleanly instead of silently saturating or panicking.

## Validation

- Temporarily apply a one-shot idle re-arm mutant and confirm the sequential
  regression fails at the explicit round guard, then restore production code.
- Run the two new targeted idle tests.
- Run all six idle-filtered `client_loopback` tests.
- Run the complete `client_loopback` integration target.
- Run `cargo clippy -p decdn-node --test client_loopback -- -D warnings`.
- Run `cargo fmt -- --check` and `git diff --check`.

The localhost runtime tests require socket-binding access. If the sandbox denies
that access, use the exact-head GitHub CI result as the runtime evidence and
report the local limitation explicitly.

## Non-Goals

- No production idle-reaper changes.
- No new test helper abstractions.
- No protocol, timeout, or payment-channel behavior changes.
