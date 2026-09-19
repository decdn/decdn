# Stream Bounded / Resumed Cache-Miss Requests Through the Two-Leg Spine

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every authorized `cdn/client/v1` cache miss — whole blob, bounded range, or resumed tail — streams to the paying client while the node fills from origin or from a peer, so time-to-first-byte never waits on a whole-blob download and the node never holds a whole requested span in RAM.

**Architecture:** The node already has a "serve while filling" spine (`serve_via_backend_origin` for the node's own fs/http/s3 origin, `serve_via_window_pull_through` for node→node), built from a range-aware fill registry (`claim_fill`), a gap-driven pull leg (`drive` over `missing_ranges(offset, len)`), and a serve leg that clamps delivery to `[offset, offset+len)`. Dispatch only routes `byte_offset == 0 && byte_len == 0` into it; everything else goes to `try_range_pull_through`, which buffers the whole requested span (download → verify → import → *then* sign `StreamResponse`) with no timeout and no keepalive. This plan first fixes the spine's per-draw origin cost (#2061 §1–§2, a **blocker**), hardens the three whole-blob assumptions still inside the spine (ramp input, unaligned offset, out-of-bounds range), then removes the routing gate, pins the CLI's throwaway-open-then-real-open sequence, deletes the buffered range tier, and updates ADR 037.

**Tech Stack:** Rust 2024 / MSRV 1.95, tokio, iroh QUIC, bao-tree, wiremock (tests), `cargo nextest`.

**Spec:** [decdn/decdn#2060](https://github.com/decdn/decdn/issues/2060) (diagnosis + option B) and [decdn/decdn#2061](https://github.com/decdn/decdn/issues/2061) (spine pull-leg cost). ADR 037 §Origin-tier pull-through / §Implementation status is the design being completed.

## Blocking dependency — #2061 lands in this PR, before the gate moves

Today the CLI touches the spine only with a throwaway `(0,0)` open it drops at once, so the spine's pull-leg cost is invisible. Routing large whole-blob and resumed requests into it (Task 5) makes two #2061 defects the dominant cost:

- **§1 outboard re-fetched per draw** — `BackendSource::open` → `origin_encode_range` → `Origin::fetch_range` downloads the whole `{H}.obao4` on every draw. A 13 GB blob has a ~50 MB outboard; at ~1 MiB draws that is hundreds of GB of outboard egress per cold pull.
- **§2 no minimum draw size** — `WindowPacer` draws whatever room a voucher opened, so steady-state draws collapse to ~1 MiB and a 13 GB pull is ~13,000 sequential origin GETs.

Task 3 fixes both. **Task 5 (the gate removal) must not be merged, and must not be committed ahead of Task 3 in the series,** or option B ships as an origin-egress regression. #2061 §3 (prefetch the next draw) and §4 (origin-leg timeout) stay open in #2061 as follow-ups.

## Global Constraints

- Clippy denies `unwrap_used`, `expect_used`, `panic`, `indexing_slicing`, `todo`, `unimplemented` workspace-wide (tests opt out with `#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests` at module level, as existing test modules do).
- `cast_possible_truncation`, `cast_sign_loss`, `cast_precision_loss` are `deny`; use `u64::try_from(..)`.
- `missing_docs` is `warn` and CI runs `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --document-private-items`: every new `pub`/`pub(crate)`/`pub(super)` item, field, and variant gets a doc comment; every `` [`Type`] `` link must resolve.
- CI lint is `cargo clippy --workspace --all-targets -- -D warnings` — run exactly that before every commit.
- Present-tense canon: no "replaced", "used to", "previously", "#NNNN landed" in docs/comments. Describe what the code does now.
- ADR text is ASD-STE100 Simplified Technical English: short sentences, active voice, present tense, one idea per sentence.
- Pre-launch: no compatibility shims. Delete the old shape in the same PR.
- Pre-commit hooks can move `HEAD` (`reset: moving to FETCH_HEAD`); run the gate manually and commit with `--no-verify`.
- On macOS use `cargo nextest run --workspace --no-fail-fast` (issue #697 case-insensitive APFS failure fail-fasts the run otherwise).
- nextest positional filters miss file stems: use `--test <stem>` and `-E 'test(<name>)'`.

---

## File map

| File | Responsibility in this change |
|---|---|
| `crates/client-pull/src/pacer.rs` | `RampPacer` gains `paid_base`; `WindowPacer` gains a minimum draw |
| `crates/cache/src/fill_session.rs` | `FillSession` records and exposes `served_start`; registry tests for claim-vs-teardown ordering |
| `crates/cache/src/origin/{mod,http,fs,s3}.rs` | `Origin::fetch_range` returns data only; the outboard comes from `fetch_outboard` |
| `crates/cache/src/engine.rs` | `origin_encode_range` takes a pre-fetched outboard; later: delete `pull_through_range` / `range_pull_attempt` / `RangePullOutcome` / `origin_fetch_range_bytes` |
| `crates/node/src/node_origin/backend_source.rs` | `BackendSource` holds the outboard for the whole fill |
| `crates/node/src/node_origin/pull_leg.rs` | both `RampPacer` constructions pass `paid_base: session.served_start()` |
| `crates/node/src/handlers/client/dispatch.rs` | drop the whole-blob routing gates; hand the probed outboard to the spine; delete the `try_range_pull_through` call + `range_pulled_size`; extract `range_out_of_bounds`; `aligned_span` becomes `pub(super)` |
| `crates/node/src/handlers/client/window.rs` | span-capped floor guard; pre-signature bounds check; `outboard` parameter; doc comments |
| `crates/node/src/handlers/client/serve_leg.rs` | group-aligned `fetch_start`; guarded frontier extension for attached observers |
| `crates/node/src/handlers/client/fill.rs` | delete `try_range_pull_through` |
| `crates/node/src/handlers/client/mod.rs` | drop the `RangePullOutcome` import |
| `crates/cache/src/lib.rs`, `crates/cache/src/range_pull.rs` | fix doc mentions |
| `crates/cache/tests/{pull_through,present_ranges,s3_origin}.rs` | delete the `range_pull_*` tests; re-base `present_ranges.rs` fixtures on `admit_bao`; `fetch_range` tests match the data-only shape |
| `crates/node/tests/origin_range_pull.rs`, `partial_serve.rs`, `node_origin_pull.rs` | new + flipped integration tests |
| `adr/037-regional-proxy-warming.md` | range tier description → spine; deferral bullet; acceptance criteria |

---

### Task 1: `RampPacer` ramps on the stream's own paid bytes

The pull leg's `RampPacer` widens the pull window as `served_paid / divisor`. `served_paid` is an **absolute** content frontier that `FillSession::starting_at` seeds at the request's `byte_offset`. For a request resuming at 5 GiB the window would open at `credit_max` before the first voucher. The pacer must ramp on `served_paid − served_start`.

**Files:**
- Modify: `crates/client-pull/src/pacer.rs:325-346` (struct + `decide`) and its test module (`:687-720` area)
- Modify: `crates/cache/src/fill_session.rs:365-455` (struct, `starting_at`), test module at `:1414+`
- Modify: `crates/node/src/node_origin/pull_leg.rs:536-540` and `:1076-1080`

**Interfaces:**
- Produces: `RampPacer { divisor, floor, credit_max, paid_base: u64 }`; `FillSession::served_start(&self) -> u64`.

- [ ] **Step 1: Write the failing pacer test**

Append to the `#[cfg(test)]` module in `crates/client-pull/src/pacer.rs`, next to `ramp_pacer_widens_the_pull_window_as_served_paid_advances`:

```rust
    #[test]
    fn ramp_pacer_measures_paid_from_the_session_start() {
        // A request resuming at 32 groups has an ABSOLUTE served-paid frontier of
        // 32 groups before it pays a byte. The ramp must read that as "0 paid",
        // so the window stays at the floor and a pull already `floor` ahead Waits.
        let floor = 4 * CHUNK_GROUP_BYTES;
        let start = 32 * CHUNK_GROUP_BYTES;
        let pacer = RampPacer {
            divisor: 2,
            floor,
            credit_max: 64 * CHUNK_GROUP_BYTES,
            paid_base: start,
        };
        let mut s = healthy();
        s.downstream.served_paid = start;
        s.pulled_frontier = start + floor;
        assert_eq!(pacer.decide(&s), PaceDecision::Wait);

        // Once the stream has paid 32 groups PAST its start, the window is 16
        // groups (> floor), so the same pull may Draw again.
        s.downstream.served_paid = start + 32 * CHUNK_GROUP_BYTES;
        assert!(matches!(pacer.decide(&s), PaceDecision::Draw { .. }));
    }
```

Also add `paid_base: 0,` to every existing `RampPacer { .. }` literal in that test module (grep `RampPacer {` in the file).

- [ ] **Step 2: Run it to see it fail to compile**

Run: `cargo nextest run -p decdn-client-pull -E 'test(ramp_pacer_measures_paid_from_the_session_start)'`
Expected: compile error `struct RampPacer has no field named paid_base`.

- [ ] **Step 3: Add the field and use it**

In `crates/client-pull/src/pacer.rs` replace the struct and `decide`:

```rust
pub struct RampPacer {
    /// Ramp divisor: the window is `paid / divisor`. `0` opens the full
    /// `credit_max` immediately.
    pub divisor: u64,
    /// Smallest window the ramp may produce, so the pull can always make
    /// progress. In practice [`PULL_WINDOW_FLOOR`] — one chunk plus three
    /// chunk groups; see that constant for why one chunk alone deadlocks.
    pub floor: u64,
    /// Ceiling the ramp climbs toward.
    pub credit_max: u64,
    /// The ABSOLUTE content offset the downstream stream starts paying from —
    /// the fill session's served start. `served_paid` is an absolute frontier,
    /// so the ramp input is `served_paid − paid_base`: what THIS stream has
    /// paid, not where in the blob it happens to sit. A request resuming at a
    /// multi-GiB offset therefore ramps from the floor like any other stream.
    pub paid_base: u64,
}

impl Pacer for RampPacer {
    fn decide(&self, s: &PaceState) -> PaceDecision {
        let paid = s.downstream.served_paid.saturating_sub(self.paid_base);
        let window =
            decdn_incentive::ramped_credit_window(self.divisor, self.floor, self.credit_max, paid);
        WindowPacer::new(window).decide(s)
    }
}
```

- [ ] **Step 4: Run the pacer tests**

Run: `cargo nextest run -p decdn-client-pull -E 'test(ramp_pacer)'`
Expected: all three `ramp_pacer_*` tests PASS.

- [ ] **Step 5: Write the failing `FillSession::served_start` test**

In `crates/cache/src/fill_session.rs`, inside the `#[cfg(test)]` module that already constructs sessions with `FillSession::starting_at(hb(0x4E), 8 * G, 2 * G)` (around `:1587`), add:

```rust
    #[test]
    fn starting_at_records_the_served_start() {
        let at_zero = FillSession::new(hb(0x60), 8 * G);
        assert_eq!(at_zero.served_start(), 0);
        let resumed = FillSession::starting_at(hb(0x61), 8 * G, 3 * G);
        assert_eq!(resumed.served_start(), 3 * G);
        // Advancing the paid frontier never moves the start.
        resumed.advance_served(5 * G);
        assert_eq!(resumed.served_start(), 3 * G);
    }
```

- [ ] **Step 6: Run it to see it fail**

Run: `cargo nextest run -p decdn-cache -E 'test(starting_at_records_the_served_start)'`
Expected: compile error `no method named served_start`.

- [ ] **Step 7: Record the start on the session**

In `crates/cache/src/fill_session.rs`:

Add the field to `pub struct FillSession` right after `served_paid: Frontier,`:

```rust
    /// The ABSOLUTE content offset the owning request starts at — the value
    /// `served_paid` is seeded with. A pull leg's `RampPacer` ramps on
    /// `served_paid − served_start`, so a resumed request ramps from the floor.
    served_start: u64,
```

In `starting_at`, add `served_start,` to the `Self { .. }` literal (the parameter is already named `served_start`).

Add the getter next to `served_paid()`:

```rust
    /// The ABSOLUTE content offset this session's owning request starts at.
    #[must_use]
    pub fn served_start(&self) -> u64 {
        self.served_start
    }
```

- [ ] **Step 8: Run the cache unit tests**

Run: `cargo nextest run -p decdn-cache -E 'test(starting_at_records_the_served_start)'`
Expected: PASS.

- [ ] **Step 9: Pass `paid_base` from both pull legs**

In `crates/node/src/node_origin/pull_leg.rs`, both `RampPacer { .. }` literals (`run_pull_leg` ~`:536`, `run_local_pull_leg` ~`:1076`) become:

```rust
    let pacer = RampPacer {
        divisor: credit_ramp_divisor,
        floor: credit_floor,
        credit_max,
        paid_base: session.served_start(),
    };
```

(`session: Arc<FillSession>` is already a parameter of both functions.)

- [ ] **Step 10: Build + lint + run the existing spine tests**

Run:
```bash
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: clean.

Run: `cargo nextest run -p decdn-node --test node_origin_pull -E 'test(window_pull_through_serves_and_caches_full_blob) | test(window_pull_through_completes_a_blob_past_the_ramp_floor)'`
Expected: PASS (offset-0 streams are unchanged: `paid_base == 0`).

- [ ] **Step 11: Commit**

```bash
git add crates/client-pull/src/pacer.rs crates/cache/src/fill_session.rs crates/node/src/node_origin/pull_leg.rs
git commit --no-verify -m "fix(client-pull): RampPacer ramps on the stream's own paid bytes

The pull leg's window is paid / divisor, but served_paid is an absolute
content frontier seeded at the request's byte_offset. Subtract the
session's served start so a resumed request ramps from the floor instead
of opening credit_max before its first voucher.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: One bounds check for every serve tier

Dispatch refuses an out-of-bounds range with `RangeNotSatisfiable` only on the direct-serve path, after the fill tiers. The spine signs `ok: true` before `serve_leg` can notice. Extract the predicate so the spine can refuse before it signs.

**Files:**
- Modify: `crates/node/src/handlers/client/dispatch.rs:1229-1246` (inline predicate → helper), `:1430` (`aligned_span` visibility), test module `aligned_span_tests`

**Interfaces:**
- Produces: `pub(super) fn range_out_of_bounds(byte_offset: u64, byte_len: u64, total_bytes: u64) -> bool` and `pub(super) fn aligned_span(..)` in `dispatch.rs`, reachable from `window.rs` as `super::dispatch::range_out_of_bounds` / `super::dispatch::aligned_span`.

- [ ] **Step 1: Write the failing unit test**

In `crates/node/src/handlers/client/dispatch.rs`, inside `mod aligned_span_tests` (rename the module to `mod range_helper_tests` while there), add:

```rust
    #[test]
    fn range_out_of_bounds_mirrors_align_range() {
        let total = 100 * 1024;
        // Whole blob and in-bounds tails / bounds are satisfiable.
        assert!(!range_out_of_bounds(0, 0, total));
        assert!(!range_out_of_bounds(16 * 1024, 0, total));
        assert!(!range_out_of_bounds(16 * 1024, 32 * 1024, total));
        assert!(!range_out_of_bounds(0, total, total));
        // Offset at/past the end, an end past the blob, or an overflowing end.
        assert!(range_out_of_bounds(total, 0, total));
        assert!(range_out_of_bounds(total + 1, 0, total));
        assert!(range_out_of_bounds(16 * 1024, total, total));
        assert!(range_out_of_bounds(u64::MAX, 1, total));
        assert!(range_out_of_bounds(1, u64::MAX, total));
        // The empty blob is addressable only as (0, 0).
        assert!(!range_out_of_bounds(0, 0, 0));
        assert!(range_out_of_bounds(0, 1, 0));
    }
```

- [ ] **Step 2: Run it to see it fail**

Run: `cargo nextest run -p decdn-node -E 'test(range_out_of_bounds_mirrors_align_range)'`
Expected: compile error `cannot find function range_out_of_bounds`.

- [ ] **Step 3: Extract the helper and use it in dispatch**

Add next to `aligned_span` in `dispatch.rs`:

```rust
/// Whether `[byte_offset, byte_offset + byte_len)` (`byte_len == 0` = to the
/// blob end) lies outside a `total_bytes`-byte blob. Mirrors
/// [`decdn_bao_range::align_range`]'s bound check so every serve tier — the
/// direct-serve gate and the two-leg spine — refuses the same ranges with
/// `RangeNotSatisfiable` BEFORE it signs a response. A whole-blob request
/// (`0, 0`) is always in bounds, including for the empty blob.
pub(super) fn range_out_of_bounds(byte_offset: u64, byte_len: u64, total_bytes: u64) -> bool {
    if byte_offset == 0 && byte_len == 0 {
        return false;
    }
    byte_offset >= total_bytes
        || (byte_len > 0
            && byte_offset
                .checked_add(byte_len)
                .is_none_or(|end| end > total_bytes))
}
```

Change `fn aligned_span(` to `pub(super) fn aligned_span(`.

Replace the inline predicate at `dispatch.rs:1229-1246` with:

```rust
        if range_out_of_bounds(req.byte_offset, req.byte_len, total_bytes) {
            return self
                .respond_error(
                    &mut send,
                    &req,
                    ServeRejectReason::RangeNotSatisfiable,
                    rate_per_mb,
                )
                .await;
        }
```

Keep the comment above it; trim the sentence "Mirrors `range_pull::align_range`'s bound check on the origin tier" to "Mirrors `align_range`'s bound check, shared with the two-leg spine via `range_out_of_bounds`."

- [ ] **Step 4: Run the unit tests + the existing out-of-bounds integration test**

Run: `cargo nextest run -p decdn-node -E 'test(range_helper_tests)'`
Expected: PASS.

Run: `cargo nextest run -p decdn-node --test origin_range_pull -E 'test(out_of_bounds_range_is_rejected_before_delivery)'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/node/src/handlers/client/dispatch.rs
git commit --no-verify -m "refactor(node): share the range bounds predicate across serve tiers

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3 (BLOCKER, #2061 §1–§2): fetch the outboard once per fill; draw at least half a window

Two changes, each with its own test cycle, one commit each. Both must be in the series **before** Task 5.

#### 3a — `WindowPacer` minimum draw

`WindowPacer::decide` draws `min(up_to_bytes, room)` whenever `room > 0`. Every voucher opens ~1 MiB of room and wakes the pull, so a fast origin is driven in ~1 MiB draws. Add a minimum: wait until `room ≥ min(window / 2, up_to_bytes)` unless a serve leg is parked at the frontier (the existing `serve_demand` liveness case). The exposure bound is unchanged — `pulled − served_paid ≤ window` still holds — only the draw count changes, from `total / CHUNK_BYTES` to about `2 · total / window`.

**Files:**
- Modify: `crates/client-pull/src/pacer.rs:264-313` (`WindowPacer::decide`) and its test module

**Interfaces:**
- Produces: no signature change. `WindowPacer::new(window_bytes)` keeps its shape; the minimum is derived from `window_bytes`.

- [ ] **Step 1: Write the failing tests**

In the `pacer.rs` test module add (read `BudgetPacer::decide` first: it returns `Draw { up_to_bytes: requested_bytes − cleared_bytes }` for a healthy state — set `cleared_bytes` explicitly so each test's `up_to_bytes` is what the comment says):

```rust
    #[test]
    fn window_pacer_waits_until_half_the_window_is_free() {
        // Window = 64 groups. The pull is 60 groups ahead: 4 groups of room, less
        // than the 32-group minimum draw, and no serve leg is parked → Wait.
        let window = 64 * CHUNK_GROUP_BYTES;
        let pacer = WindowPacer::new(window);
        let mut s = healthy();
        s.requested_bytes = 1_000 * CHUNK_GROUP_BYTES;
        s.cleared_bytes = 0;
        s.downstream.served_paid = 0;
        s.pulled_frontier = 60 * CHUNK_GROUP_BYTES;
        assert_eq!(pacer.decide(&s), PaceDecision::Wait);

        // 32 groups of room (exactly the minimum) → Draw, capped to the room.
        s.pulled_frontier = 32 * CHUNK_GROUP_BYTES;
        assert_eq!(
            pacer.decide(&s),
            PaceDecision::Draw {
                up_to_bytes: 32 * CHUNK_GROUP_BYTES
            }
        );
    }

    #[test]
    fn window_pacer_final_draw_ignores_the_minimum() {
        // Only 3 groups remain in the gap (`up_to_bytes` from the budget pacer is
        // the gap remainder). 4 groups of room is enough for the last draw even
        // though it is below half the window.
        let window = 64 * CHUNK_GROUP_BYTES;
        let pacer = WindowPacer::new(window);
        let mut s = healthy();
        s.requested_bytes = 63 * CHUNK_GROUP_BYTES;
        s.cleared_bytes = 60 * CHUNK_GROUP_BYTES;
        s.downstream.served_paid = 0;
        s.pulled_frontier = 60 * CHUNK_GROUP_BYTES;
        assert!(matches!(pacer.decide(&s), PaceDecision::Draw { .. }));
    }

    #[test]
    fn window_pacer_serve_demand_overrides_the_minimum() {
        // A serve leg parked at the frontier still gets one floor even when the
        // room is below the minimum draw.
        let window = 64 * CHUNK_GROUP_BYTES;
        let pacer = WindowPacer::new(window);
        let mut s = healthy();
        s.requested_bytes = 1_000 * CHUNK_GROUP_BYTES;
        s.cleared_bytes = 0;
        s.downstream.served_paid = 0;
        s.pulled_frontier = 60 * CHUNK_GROUP_BYTES;
        s.downstream.serve_demand = 60 * CHUNK_GROUP_BYTES + 1;
        assert_eq!(
            pacer.decide(&s),
            PaceDecision::Draw {
                up_to_bytes: PULL_WINDOW_FLOOR
            }
        );
    }
```

- [ ] **Step 2: Run them to see the first fail**

Run: `cargo nextest run -p decdn-client-pull -E 'test(window_pacer_)'`
Expected: `window_pacer_waits_until_half_the_window_is_free` FAILS (today it draws 4 groups); the other two pass already.

- [ ] **Step 3: Implement the minimum draw**

In `WindowPacer::decide`, replace the tail from `let room = room.max(demanded);` to the end of the `Draw` arm with:

```rust
                // Minimum draw (#2061). A voucher opens about one chunk of room and
                // wakes the pull, so without a floor a fast origin is driven in
                // one-chunk draws — one origin round trip per chunk for the whole
                // blob. Wait until at least half the window is free, so the draw
                // count is bounded by `2 · total / window` instead of
                // `total / CHUNK_BYTES`. The bound `pulled − served_paid ≤ window`
                // is untouched: waiting only ever draws LESS. Two exceptions keep
                // liveness: the serve-demand floor above (a parked serve leg gets
                // its one floor regardless), and the final draw (a gap remainder
                // smaller than the minimum is drawn as soon as it fits).
                let half = self.window_bytes / 2;
                let min_draw = (half - half % CHUNK_GROUP_BYTES).min(up_to_bytes);
                if demanded == 0 && room < min_draw {
                    return PaceDecision::Wait;
                }
                let room = room.max(demanded);
                if room == 0 {
                    PaceDecision::Wait
                } else {
                    PaceDecision::Draw {
                        up_to_bytes: up_to_bytes.min(room),
                    }
                }
```

Update the `WindowPacer` doc comment: after "When the window is already full and no serve leg is parked there, wait instead of drawing zero bytes." add "It also waits while less than half the window is free, so draws stay large (#2061)."

- [ ] **Step 4: Run the pacer tests and the spine integration tests**

Run: `cargo nextest run -p decdn-client-pull -E 'test(window_pacer_) | test(ramp_pacer) | test(pull_window_floor)'`
Expected: PASS.

Run: `cargo nextest run -p decdn-node --test node_origin_pull --test origin_range_pull -E 'test(window_pull_through_) | test(own_origin) | test(backend_origin)'`
Expected: PASS. If `window_pull_through_completes_a_blob_past_the_ramp_floor` or the `PULL_WINDOW_FLOOR` liveness proof in `pacer.rs` fails, the minimum is starving the floor case: at the ramp floor `window_bytes == PULL_WINDOW_FLOOR`, so `min_draw` is half a floor (~540 KiB) and one voucher (1 MiB) of room satisfies it — re-check the arithmetic before changing the rule.

- [ ] **Step 5: Commit**

```bash
git add crates/client-pull/src/pacer.rs
git commit --no-verify -m "fix(client-pull): WindowPacer waits for half a window before it draws

Every voucher opens about one chunk of room and wakes the pull, so a fast
origin was driven in one-chunk draws. Wait until half the window is free
(unless a serve leg is parked at the frontier, or the gap remainder is
smaller); the exposure bound is unchanged and the draw count drops from
total / CHUNK_BYTES to about 2 * total / window. Refs #2061.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

#### 3b — outboard fetched once per fill

`Origin::fetch_range` returns `(data, outboard)` and fetches the outboard first on every call. Reshape it to **data only**; the spine fetches `{H}.obao4` once (dispatch already does, at `dispatch.rs:1001`, and throws it away) and hands it to `BackendSource`, which passes it to `origin_encode_range` on every draw.

**Files:**
- Modify: `crates/cache/src/origin/mod.rs:233-250` (`OriginRangeFetch::Ranged`), `:384-391` (trait `fetch_range`)
- Modify: `crates/cache/src/origin/http.rs:385-436`, `crates/cache/src/origin/fs.rs:271-400`, `crates/cache/src/origin/s3.rs:742-830` (drop the outboard sub-fetch from each `fetch_range`)
- Modify: `crates/cache/src/engine.rs:3481-3513` (`origin_fetch_range_bytes`), `:3530-3569` (`origin_fetch_outboard_bytes` metering), `:3605-3638` (`origin_encode_range`)
- Modify: `crates/node/src/node_origin/backend_source.rs:60-157` (`BackendSource` field + `open`), test double at `:316-330`
- Modify: `crates/node/src/handlers/client/window.rs:488-501` (`serve_via_backend_origin` signature), `:648-653` (`BackendSource::new` call)
- Modify: `crates/node/src/handlers/client/dispatch.rs:1001-1027` (bind the probed outboard, pass it)
- Modify: tests `crates/cache/tests/s3_origin.rs:1012-1180`, `crates/cache/src/origin/fs.rs:977+` (`fetch_range_returns_span_and_outboard`), `crates/cli/tests/origin_import.rs` (one `Ranged` match)
- Test: `crates/node/tests/origin_range_pull.rs` (new multi-draw test)

**Interfaces:**
- Produces: `Origin::fetch_range(&self, hash: Hash, req: OriginRangeRequest) -> … Result<OriginRangeFetch, OriginPullError>` with `OriginRangeFetch::Ranged { data: Bytes }`; `CacheEngine::origin_encode_range(&self, hash: Hash, aligned: &AlignedRange, outboard: Bytes) -> CacheResult<Option<Bytes>>`; `BackendSource::new(engine, root, total_bytes, outboard: Bytes, self_pay)`; `serve_via_backend_origin(.., total_bytes: u64, outboard: Bytes, ..)`.

- [ ] **Step 1: Write the failing integration test (one `.obao4` GET across a multi-draw pull)**

In `crates/node/tests/origin_range_pull.rs` add a 3 MiB fixture and the test:

```rust
/// A distinctive 3 MiB payload plus its outboard: larger than `PULL_WINDOW_FLOOR`,
/// so a whole-blob own-origin miss takes several pull-leg draws.
fn large_blob_with_outboard() -> (Vec<u8>, Vec<u8>, Hash) {
    let blob: Vec<u8> = (0..3 * 1024 * 1024u32)
        .map(|i| u8::try_from(i.wrapping_mul(2_654_435_761) >> 24).unwrap_or(0))
        .collect();
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let hash = Hash::from_bytes(*ob.root.as_bytes());
    (blob, ob.data, hash)
}

/// #2061 §1: the pull leg fetches `{H}.obao4` ONCE per fill, not once per draw.
/// A 3 MiB blob is several draws at the ramp floor; the origin must see exactly
/// one outboard GET (the serviceability probe's, handed to the pull leg) and at
/// least two ranged data GETs (proof the pull really was multi-draw).
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn own_origin_miss_fetches_the_outboard_once_across_draws() -> anyhow::Result<()> {
    let (blob, outboard, hash) = large_blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path(format!("/{hex}")))
        .respond_with(
            ResponseTemplate::new(200).insert_header("Content-Length", blob_size.to_string()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    // Dynamic 206 responder: whatever aligned span a draw asks for.
    let blob_for_resp = blob.clone();
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header_exists("range"))
        .respond_with(move |req: &Request| {
            let span = req
                .headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_byte_range)
                .and_then(|(s, e)| Some((usize::try_from(s).ok()?, usize::try_from(e).ok()?)))
                .and_then(|(s, e)| blob_for_resp.get(s..=e));
            match span {
                Some(body) => ResponseTemplate::new(206).set_body_bytes(body.to_vec()),
                None => ResponseTemplate::new(416),
            }
        })
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x61);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, _cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let got = ranged_paid_pull(
        &client_ep,
        target,
        client_node_id,
        &client_eth,
        pool_id,
        provider,
        hash,
        0,
        0,
        RATE_PER_MB,
    )
    .await?;
    anyhow::ensure!(got.as_slice() == blob.as_slice(), "multi-draw delivery mismatch");
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "the own-origin two-leg tier must serve this miss"
    );

    let outboard_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET" && r.url.path() == format!("/{hex}.obao4")
    })
    .await?;
    let ranged_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && r.headers.contains_key("range")
    })
    .await?;
    anyhow::ensure!(
        ranged_gets >= 2,
        "test premise: a 3 MiB blob must take several draws, saw {ranged_gets}"
    );
    anyhow::ensure!(
        outboard_gets == 1,
        "the outboard must be fetched once per fill, saw {outboard_gets} GET(s) across {ranged_gets} draws"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}
```

(`parse_byte_range` and `header_exists` are already in this file — used by `interior_hold_own_origin_miss_pulls_only_the_gaps`.)

- [ ] **Step 2: Run it to see it fail**

Run: `cargo nextest run -p decdn-node --test origin_range_pull -E 'test(own_origin_miss_fetches_the_outboard_once_across_draws)'`
Expected: FAIL on `outboard_gets == 1` with `outboard_gets == ranged_gets + 1`.

- [ ] **Step 3: Reshape `Origin::fetch_range` to data only**

`crates/cache/src/origin/mod.rs`:

```rust
pub enum OriginRangeFetch {
    /// The origin served the requested byte span. `data` covers exactly
    /// `[aligned.fetch_start(), aligned.fetch_end())` (the chunk-group-aligned
    /// span the engine asked for) and is untrusted until
    /// [`crate::range_pull::encode_verified_range`] verifies it against `H`
    /// with the outboard from [`Origin::fetch_outboard`].
    Ranged {
        /// The aligned data bytes, exactly `aligned.fetch_len()` long.
        data: Bytes,
    },
    /// The origin reported the object (data key) does not exist.
    NotFound,
    /// The range read is not available for this fetch — no `Range`/`206`
    /// support, or a body of the wrong length. The engine degrades to a
    /// whole-blob pull.
    Unsupported,
}
```

Trait method:

```rust
    /// Fetch the chunk-group-aligned byte span `req` of the blob `hash`. The
    /// sibling `{H}.obao4` outboard is a separate, one-time
    /// [`Self::fetch_outboard`]; a fill that draws many spans reuses that one
    /// outboard for every draw ([ADR 037 §Origin-tier pull-through](../../../adr/037-regional-proxy-warming.md)).
    ///
    /// The default implementation returns [`OriginRangeFetch::Unsupported`],
    /// so a custom [`Origin`] needs no change and the engine degrades to a
    /// whole-blob [`Self::fetch`] pull. The three shipped adapters override it.
    ///
    /// Like [`Self::fetch`], the origin is a dumb byte store: the returned
    /// `data` is **untrusted** and verified against the root `H` by the
    /// engine via [`crate::range_pull::encode_verified_range`] before any byte
    /// is imported.
    ///
    /// Returning [`OriginRangeFetch::Unsupported`] is the correct, expected
    /// answer whenever the range read can't apply (no `Range`/`206`, wrong
    /// body length) — it is not an error. Only genuine transport / permission
    /// failures surface as [`OriginPullError`].
    fn fetch_range(
        &self,
        _hash: Hash,
        _req: OriginRangeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async { Ok(OriginRangeFetch::Unsupported) })
    }
```

`crates/cache/src/origin/http.rs` `fetch_range` body — delete the "Outboard first" block (the `obao4_url` build and `get_bounded` call) and the `outboard_max_bytes` parameter; return `OriginRangeFetch::Ranged { data }` in both places. Apply the same deletion to `fs.rs` (`:271-400`: drop the outboard read; keep the symlink/escape checks on the data path) and `s3.rs` (`:742-830`: drop the `{key}.obao4` `GetObject`). Each adapter already implements `fetch_outboard`; leave those untouched.

Fix the matchers: `crates/cli/tests/origin_import.rs` (one `Ranged { data, outboard }` → `Ranged { data }`), `crates/node/src/node_origin/backend_source.rs:327` test double, and the adapter unit tests: `fs.rs::fetch_range_returns_span_and_outboard` → rename to `fetch_range_returns_the_span` and assert on `data` only; in `crates/cache/tests/s3_origin.rs` rename `fetch_range_returns_span_and_outboard` the same way, delete `fetch_range_missing_outboard_is_unsupported` and `fetch_range_oversized_outboard_degrades_without_buffering` (those behaviours belong to `fetch_outboard`, which has its own tests at `:1250+`), and drop the third argument from every `origin.fetch_range(hash, req, ..)` call.

- [ ] **Step 4: Thread the outboard through the engine**

`crates/cache/src/engine.rs`:

`origin_fetch_range_bytes` (the buffered tier's helper, deleted in Task 7) composes the two fetches so that tier keeps working until then. Read `OriginOutboardFetch` (`origin/mod.rs:305+`) for the success variant's real name and use it below:

```rust
    async fn origin_fetch_range_bytes(
        &self,
        origin: &Arc<dyn Origin>,
        hash: Hash,
        blob_size: u64,
        req: OriginRangeRequest,
    ) -> CacheResult<Option<(Bytes, Bytes)>> {
        let outboard_max = expected_outboard_len(blob_size).saturating_add(64);
        let outboard = match origin
            .fetch_outboard(hash, outboard_max)
            .await
            .map_err(|e| CacheError::OriginError { hash, source: e.into_inner() })?
        {
            OriginOutboardFetch::Outboard(bytes) => bytes,
            _ => return Ok(None),
        };
        let data = match origin
            .fetch_range(hash, req)
            .await
            .map_err(|e| CacheError::OriginError { hash, source: e.into_inner() })?
        {
            OriginRangeFetch::Ranged { data } => data,
            OriginRangeFetch::Unsupported | OriginRangeFetch::NotFound => return Ok(None),
        };
        if let Some(m) = &self.inner.metrics {
            let pulled = u64::try_from(data.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(outboard.len()).unwrap_or(u64::MAX));
            m.pull_through_bytes.inc_by(pulled);
        }
        Ok(Some((data, outboard)))
    }
```

`origin_encode_range` takes the outboard and fetches data only:

```rust
    pub async fn origin_encode_range(
        &self,
        hash: Hash,
        aligned: &AlignedRange,
        outboard: Bytes,
    ) -> CacheResult<Option<Bytes>> {
        let root = *hash.as_bytes();
        let req = OriginRangeRequest {
            fetch_start: aligned.fetch_start(),
            fetch_end: aligned.fetch_end(),
        };
        for origin in &self.inner.origins {
            let data = match origin
                .fetch_range(hash, req)
                .await
                .map_err(|e| CacheError::OriginError { hash, source: e.into_inner() })?
            {
                OriginRangeFetch::Ranged { data } => data,
                OriginRangeFetch::Unsupported | OriginRangeFetch::NotFound => continue,
            };
            if let Some(m) = &self.inner.metrics {
                m.pull_through_bytes
                    .inc_by(u64::try_from(data.len()).unwrap_or(u64::MAX));
            }
            return match encode_verified_range(root, aligned, &data, outboard.clone()) {
                Ok(wire) => Ok(Some(wire)),
                Err(err) => {
                    tracing::warn!(
                        %hash,
                        kind = ?origin.kind(),
                        error = %err,
                        "own origin served a range that failed bao verification against H; \
                         hard local-origin fault (no degrade — committed to serving under H)",
                    );
                    Err(CacheError::VerifyFailed { expected: hash })
                }
            };
        }
        Ok(None)
    }
```

Update its doc comment: "`outboard` is the untrusted `{H}.obao4` the caller fetched once (`origin_fetch_outboard_bytes`) and reuses for every draw of the fill." In `origin_fetch_outboard_bytes` (`:3530-3569`) meter the outboard bytes once with `pull_through_bytes.inc_by(outboard.len())` at the success arm if it does not already.

- [ ] **Step 5: `BackendSource` holds the outboard; the spine and dispatch hand it over**

`crates/node/src/node_origin/backend_source.rs`: add a field and constructor parameter:

```rust
    /// The untrusted `{H}.obao4` the serviceability probe fetched, reused for
    /// every draw's `origin_encode_range` (#2061): one outboard read per fill,
    /// not one per draw.
    outboard: Bytes,
```

`pub(crate) fn new(engine, root, total_bytes, outboard: Bytes, self_pay)` (drop `const` — `Bytes` is not const-constructible). In `open`, call `self.engine.origin_encode_range(Hash::from(hash), &range, self.outboard.clone())`. Update the module docs' mention of the per-open fetch.

`crates/node/src/handlers/client/window.rs`: add `outboard: Bytes` to `serve_via_backend_origin`'s parameters right after `total_bytes: u64`, and pass it to `BackendSource::new(engine.clone(), *hash.as_bytes(), total_bytes, outboard, ledger)`.

`crates/node/src/handlers/client/dispatch.rs:1001-1027`: change `Ok(Some(_)) => {` to `Ok(Some(outboard)) => {` and pass `outboard` into `serve_via_backend_origin(.., total, outboard, ..)`. Extend the comment "Serviceable: size known and an origin publishes the outboard. …" with "The outboard fetched here is the one the pull leg reuses for every draw."

- [ ] **Step 6: Build, lint, run the affected suites**

Run:
```bash
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: clean.

Run: `cargo nextest run -p decdn-cache -p decdn-node -p decdn-cli --no-fail-fast -E 'test(fetch_range) | test(fetch_outboard) | test(own_origin) | test(backend_origin) | test(origin_import) | test(range_pull)'`
Expected: PASS, including `own_origin_miss_fetches_the_outboard_once_across_draws`.

- [ ] **Step 7: Commit**

```bash
git add crates/cache crates/node crates/cli/tests/origin_import.rs
git commit --no-verify -m "fix(node,cache): fetch the origin outboard once per fill, not once per draw

Origin::fetch_range returns the data span only; the outboard comes from
fetch_outboard, which the dispatch serviceability probe already performs.
BackendSource carries that outboard and hands it to origin_encode_range on
every draw. Refs #2061.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: `serve_leg` and the spine accept any request shape

Harden the three places inside the spine that assume `byte_offset == 0`. No routing change yet, so no integration test can reach these paths; the tests land with Task 5. This task's gate is the existing suites staying green.

**Files:**
- Modify: `crates/node/src/handlers/client/serve_leg.rs:130-140`, `:329-347`
- Modify: `crates/node/src/handlers/client/window.rs` (both spine fns: doc comments, floor guard, bounds check)

**Interfaces:**
- Consumes: `range_out_of_bounds` / `aligned_span` (Task 2), `FillSession::extend_served_from` (exists).

- [ ] **Step 1: `serve_leg` — group-aligned fetch start, guarded frontier extension**

In `crates/node/src/handlers/client/serve_leg.rs`, after `let offset = offset.min(end);` add:

```rust
        // The wire this leg delivers starts at the chunk-group floor of `offset`
        // (`align_range` snaps the fetch start down), so paid wire maps back to
        // content from THAT boundary — `content_paid_frontier` requires a
        // group-aligned fetch start. Equal to `offset` for an aligned request.
        let fetch_start = (offset / CHUNK_GROUP_BYTES).saturating_mul(CHUNK_GROUP_BYTES);
```

Replace the paid-frontier publication (from `let served = content_paid_frontier(offset, total_bytes, paid);` through the `for extra in also_pace { .. }` loop) with:

```rust
                            // Publish the PAID CONTENT frontier for the pull leg's
                            // `WindowPacer`, mapping paid WIRE back into content space (the
                            // largest chunk-group boundary provably inside the paid wire
                            // prefix — conservative, so the pull never overshoots its
                            // window). One contiguous delivery from `fetch_start`, so it is
                            // the single fetch-start.
                            let served = content_paid_frontier(fetch_start, total_bytes, paid);
                            // Forward-only, and guarded on THIS leg's start: an owning
                            // session starts AT `fetch_start`, so the guard is a no-op and
                            // N whole-range observers advance the SHARED frontier with the
                            // pull's `WindowPacer` binding on the MAX-over-observers paid
                            // frontier (DECISION-B). An observer ATTACHED at an offset the
                            // owner's paid prefix has not reached yet must not lift that
                            // prefix past bytes nobody paid for; its payment extends the
                            // frontier once the prefix reaches it
                            // (`FillSession::extend_served_from`).
                            session.extend_served_from(fetch_start, served);
                            // Under partial-overlap coalescing each attached sibling pull
                            // produces the OVERLAP this leg also consumes and bills; the
                            // same guard applies to every sibling.
                            for extra in also_pace {
                                extra.extend_served_from(fetch_start, served);
                            }
                            continue 'chunk;
```

Add `CHUNK_GROUP_BYTES` to the `use super::{ .. }` import list in `serve_leg.rs`.

- [ ] **Step 2: Both spine functions in `window.rs`**

At the top of the file add `use super::dispatch::{aligned_span, range_out_of_bounds};`.

**Peer path `serve_via_window_pull_through`:**

Replace the doc sentence at `:59-60` "The caller has already proven channel ownership and confirmed `byte_offset == 0`." with "The caller has already proven channel ownership. The request may be whole-blob, bounded, or resumed: the serve leg clamps delivery to `[byte_offset, end)` and the pull leg fills only that span's missing chunk groups."

Replace the floor-guard block (from `if let Some(remaining) = pool_remaining` through its closing `}`) with:

```rust
        // Pre-flight floor-M guard (shared-payment-pool model) — the pull-through
        // twin of the `dispatch.rs` direct-serve gate. Refuse the speculative pull
        // when the pool's on-chain remaining (`getPool.deposit − totalRedeemed`)
        // minus the refundable floor `M` can no longer cover the reserved floor,
        // so the node never fronts upstream USDC for a pool that cannot cover it.
        // The floor is one credit window, CAPPED BY THE REQUEST'S ALIGNED SPAN
        // when the request bounds itself — the same pricing `dispatch.rs` reserved,
        // so a request accepted there is never refused here for a sub-window span.
        // `pool_remaining` is the cached `getPool.remaining` threaded from the
        // serve gate; `None` (no pool-view, unknown pool, or a read fault) fails
        // open — the on-chain `redeem` is the backstop.
        let guard_bytes = if req.byte_len > 0 {
            aligned_span(req.byte_offset, req.byte_len, u64::MAX).min(credit_floor)
        } else {
            credit_floor
        };
        if let Some(remaining) = pool_remaining
            && !self.pool_remaining_covers_window(remaining, guard_bytes, rate_per_mb)
        {
            let headroom = remaining.saturating_sub(self.pool_min_remaining_deposit);
            self.log_deposit_refusal(
                B256::from(req.pool_id),
                hash,
                headroom,
                decdn_incentive::min_payment(guard_bytes, rate_per_mb),
            );
            release_reservation_unspent(floor_reservation.as_ref());
            return self
                .respond_error(
                    &mut send,
                    req,
                    ServeRejectReason::InsufficientDeposit,
                    rate_per_mb,
                )
                .await;
        }
```

Directly after the `let total_bytes = match self.cache.in_flight_total(hash) { .. };` block, insert:

```rust
        // (3b) Bounds gate on the now-known geometry, BEFORE the claim and the
        // signature: an offset at or past the end, or an end past the blob, is
        // `RangeNotSatisfiable` here exactly as on the direct-serve path. Signing
        // `ok: true` first would turn a bad range into a stream failure.
        if range_out_of_bounds(req.byte_offset, req.byte_len, total_bytes) {
            release_reservation_unspent(floor_reservation.as_ref());
            return self
                .respond_error(
                    &mut send,
                    req,
                    ServeRejectReason::RangeNotSatisfiable,
                    rate_per_mb,
                )
                .await;
        }
```

**Own-origin path `serve_via_backend_origin`:**

Replace the doc paragraph at `:463-467` with "`total_bytes` is the origin-probe size the caller already confirmed serviceable, and `outboard` is the `{H}.obao4` that probe fetched. The request may be whole-blob, bounded, or resumed: the serve leg clamps delivery to `[byte_offset, end)` and the local pull leg fills only that span's missing chunk groups, so a bounded request pulls exactly its aligned span from origin." Delete the sentence at `:475-476` ("Range-aware own-origin de-dup is a deferred follow-up …").

Apply the identical `guard_bytes` rewrite to its floor-guard block (keep "the own-origin twin of the peer path and of `dispatch.rs`" and add the "CAPPED BY THE REQUEST'S ALIGNED SPAN" sentence). Insert the same bounds check immediately after that guard (before the `// (3) No size gate` comment), numbered `(2b)`.

In the `(5a)` comment, change "Two concurrent whole-blob own-origin misses therefore drive ONE origin fetch." to "Two concurrent own-origin misses for overlapping spans therefore drive ONE origin fetch for the overlap."

- [ ] **Step 3: Build, lint, run the spine suites**

Run:
```bash
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: clean.

Run: `cargo nextest run -p decdn-node --test origin_range_pull --test node_origin_pull --test partial_serve --no-fail-fast`
Expected: PASS (offset-0 behaviour is unchanged: `fetch_start == 0`, the guard is a no-op, `guard_bytes == credit_floor` for `byte_len == 0`).

- [ ] **Step 4: Commit**

```bash
git add crates/node/src/handlers/client/serve_leg.rs crates/node/src/handlers/client/window.rs
git commit --no-verify -m "fix(node): two-leg spine accounts correctly for a non-zero offset

serve_leg maps paid wire back to content from the group-aligned fetch
start and guards an attached observer's frontier extension; both spine
functions refuse an out-of-bounds range before signing and price their
floor guard at the request's aligned span.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 5: Route every authorized miss through the spine; delete the buffered range tier

**Do not start this task before Task 3 is committed** (see the blocker section). Tests go in first; they fail because dispatch still routes bounded requests to `try_range_pull_through` (so `local_outboard_serves_total` stays 0, and the mixed-range test re-fetches the held group).

**Files:**
- Modify: `crates/node/tests/origin_range_pull.rs` (new tests after `whole_blob_own_origin_miss_serves_via_backend_origin`; assertions added to `cold_range_request_pulls_only_the_range_from_origin`, `resume_to_end_range_pull_serves_tail`; doc fix on `two_concurrent_disjoint_own_origin_misses_two_fetches_no_wedge`)
- Modify: `crates/node/tests/partial_serve.rs:358-490` (`partial_serve_mixed_range_falls_through_to_fill` → `partial_serve_mixed_range_pulls_only_the_missing_group`)
- Modify: `crates/node/tests/node_origin_pull.rs:8125-8240` (`window_pull_through_resumed_offset_falls_back_not_fused` → `window_pull_through_resumed_offset_is_served_by_the_fused_path`)
- Modify: `crates/node/src/handlers/client/dispatch.rs:660-664`, `:804-1162`, `:1170-1220`
- Delete from: `crates/node/src/handlers/client/fill.rs:200-267` (`try_range_pull_through` + its doc block)
- Modify: `crates/node/src/handlers/client/mod.rs:38` (drop `RangePullOutcome`)

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces: dispatch routes any `pull_authorized` miss with an origin size + outboard (own-origin) or a window provider (peer) into `serve_via_backend_origin` / `serve_via_window_pull_through`, regardless of `byte_offset` / `byte_len`.

- [ ] **Step 1: Add the CLI-shaped regression test (bounded whole blob) and the unaligned test**

In `crates/node/tests/origin_range_pull.rs`, directly after `whole_blob_own_origin_miss_serves_via_backend_origin`, add:

```rust
/// The shape `decdn fetch` / `decdn bundle pull` actually send on a cold miss:
/// `byte_offset == 0, byte_len == total_bytes` — a BOUNDED request that covers the
/// whole blob. It must take the same two-leg streaming tier as the unbounded
/// `(0, 0)` request: the signed response goes out before the origin download
/// finishes, so a multi-GiB blob does not trip the client's stall clock. The tier
/// counter `local_outboard_serves_total` firing once is the proof of routing; the
/// byte-exact delivery is the proof the range-clamped serve leg is correct.
#[tokio::test(flavor = "multi_thread")]
async fn bounded_whole_blob_own_origin_miss_streams_via_backend_origin() -> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let aligned = align_range(0, blob_size, blob_size)?;
    let (a_start, a_end) = (aligned.fetch_start(), aligned.fetch_end());
    let span = blob
        .get(usize::try_from(a_start)?..usize::try_from(a_end)?)
        .ok_or_else(|| anyhow::anyhow!("aligned span out of bounds"))?
        .to_vec();
    let range_val = format!("bytes={a_start}-{}", a_end - 1);

    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path(format!("/{hex}")))
        .respond_with(
            ResponseTemplate::new(200).insert_header("Content-Length", blob_size.to_string()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header("range", range_val.as_str()))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(span.clone()))
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x62);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, _cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let got = ranged_paid_pull(
        &client_ep,
        target,
        client_node_id,
        &client_eth,
        pool_id,
        provider,
        hash,
        0,
        blob_size,
        RATE_PER_MB,
    )
    .await?;

    anyhow::ensure!(
        got.as_slice() == blob.as_slice(),
        "bounded whole-blob delivery mismatch: got {} bytes, want {}",
        got.len(),
        blob.len()
    );
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "a bounded whole-blob miss must take the own-origin two-leg tier"
    );
    let wholeblob_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && !r.headers.contains_key("range")
    })
    .await?;
    anyhow::ensure!(
        wholeblob_gets == 0,
        "the spine pulls ranged spans only, saw {wholeblob_gets} un-ranged GET(s)"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// An UNALIGNED bounded request (`byte_offset` inside a chunk group) through the
/// spine. The serve leg must map paid wire back to content from the group-aligned
/// fetch start, not the raw offset, and the client must receive exactly the bytes
/// it asked for (the range decoder trims the aligned superset).
#[tokio::test(flavor = "multi_thread")]
async fn bounded_unaligned_offset_own_origin_miss_streams_the_exact_bytes()
-> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    // 20 KiB is NOT a 16 KiB group boundary; 30 KiB ends mid-group too.
    let (req_off, req_len) = (20 * 1024u64, 30 * 1024u64);
    let aligned = align_range(req_off, req_len, blob_size)?;
    let (a_start, a_end) = (aligned.fetch_start(), aligned.fetch_end());
    anyhow::ensure!(a_start == 16 * 1024 && a_end == 64 * 1024, "test premise: aligned span");
    let span = blob
        .get(usize::try_from(a_start)?..usize::try_from(a_end)?)
        .ok_or_else(|| anyhow::anyhow!("aligned span out of bounds"))?
        .to_vec();
    let range_val = format!("bytes={a_start}-{}", a_end - 1);

    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path(format!("/{hex}")))
        .respond_with(
            ResponseTemplate::new(200).insert_header("Content-Length", blob_size.to_string()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header("range", range_val.as_str()))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(span.clone()))
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x63);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, _cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let got = ranged_paid_pull(
        &client_ep,
        target,
        client_node_id,
        &client_eth,
        pool_id,
        provider,
        hash,
        req_off,
        req_len,
        RATE_PER_MB,
    )
    .await?;

    let want = blob
        .get(usize::try_from(req_off)?..usize::try_from(req_off + req_len)?)
        .ok_or_else(|| anyhow::anyhow!("requested range out of bounds"))?;
    anyhow::ensure!(
        got.as_slice() == want,
        "unaligned range mismatch: got {} bytes, want {}",
        got.len(),
        want.len()
    );
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "an unaligned bounded miss must take the own-origin two-leg tier"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}
```

- [ ] **Step 2: Tighten the two existing bounded-miss tests to assert the tier**

In `cold_range_request_pulls_only_the_range_from_origin`: change `let (handler, cache, _metrics, _cache_tmp)` to `let (handler, cache, metrics, _cache_tmp)` and add, right before the `shutdown(...)` call:

```rust
    // Routing proof: the bounded miss took the own-origin two-leg streaming tier.
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "a bounded cold miss must take the own-origin two-leg tier"
    );
```

In `resume_to_end_range_pull_serves_tail`: replace its leading comment with:

```rust
    // A resume request (`byte_offset > 0`, `byte_len == 0`) is a whole-tail
    // read: `byte_len == 0` means "to end-of-blob" (ADR 005), NOT zero bytes.
    // The two-leg spine resolves `0` to the blob end in both legs (`missing_ranges`
    // for the pull, the serve leg's clamp for delivery) — this proves the tail is
    // fetched and served, end to end, and that a resumed miss streams rather than
    // buffering.
```

and make the same `metrics` + `local_outboard_serves_total == 1` addition before its `shutdown`.

On `two_concurrent_disjoint_own_origin_misses_two_fetches_no_wedge`, replace the doc sentence "Own-origin serving is whole-blob-gated today, so disjoint RANGES of one hash are not expressible through dispatch; two distinct hashes are the integration-level stand-in (per B3.4c's adaptation note)." with "Two distinct hashes keep the two fills independent at the registry layer; disjoint RANGES of one hash coalesce per `fill_session.rs`'s `claim_disjoint_both_own` / `disjoint_halves_do_not_attach`."

- [ ] **Step 3: Flip the mixed-range partial-serve test**

In `crates/node/tests/partial_serve.rs`, rename `partial_serve_mixed_range_falls_through_to_fill` to `partial_serve_mixed_range_pulls_only_the_missing_group`. Replace the block from `// The requested span (group 0 + group 1) — group 0 is already cached,` through the `.mount(&server).await;` of the ranged GET with:

```rust
    // The requested span is group 0 + group 1. Group 0 is already cached, group 1
    // is not, so the pull leg's `missing_ranges` is exactly group 1 — and that is
    // the ONLY ranged GET the origin may see. Mount a 206 for group 1 alone; a
    // request for the whole span (a re-fetch of the held group) 404s and fails.
    let (req_off, req_len) = (0u64, 2 * group);
    let gap = align_range(group, group, total)?;
    let gap_bytes = plaintext
        .get(usize::try_from(gap.fetch_start())?..usize::try_from(gap.fetch_end())?)
        .ok_or_else(|| anyhow::anyhow!("gap span out of bounds"))?
        .to_vec();
    let range_val = format!("bytes={}-{}", gap.fetch_start(), gap.fetch_end() - 1);
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(wiremock::matchers::header("range", range_val.as_str()))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(gap_bytes))
        .mount(&server)
        .await;
```

Replace the final assertion block (from `// The mixed request did NOT short-circuit as a cache hit:` to the `anyhow::ensure!(ranged_gets == 1, ...)`) with:

```rust
    // The mixed request neither short-circuited as a cache hit nor re-fetched the
    // held group: exactly one ranged GET, and it is the gap (group 1) alone.
    let ranged_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && r.headers.contains_key("range")
    })
    .await?;
    anyhow::ensure!(
        ranged_gets == 1,
        "expected exactly one ranged GET for the missing group, got {ranged_gets}"
    );
```

- [ ] **Step 4: Flip the resumed-offset node→node test**

In `crates/node/tests/node_origin_pull.rs`, replace the doc comment and name of `window_pull_through_resumed_offset_falls_back_not_fused` with:

```rust
/// A resumed cache-miss request (`byte_offset > 0`) IS served by the fused window
/// path: the serve leg clamps delivery to `[offset, end)`, the pull leg fills only
/// `missing_ranges(offset, 0)`, and every chunk group verifies against the root
/// independently (ADR 038), so no tier needs byte 0. The proof is the signed
/// `ok: true` response carrying the whole-blob `total_bytes` — a buffered fallback
/// on B's empty cache would have refused.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn window_pull_through_resumed_offset_is_served_by_the_fused_path() -> Result<()> {
```

Keep the body up to and including reading `resp`. Replace everything from `// The resumed miss is refused by the buffered fallback` through the last `anyhow::ensure!(!cache_b.has(hash)...)` with:

```rust
    anyhow::ensure!(
        resp.body.ok,
        "a resumed offset>0 miss must be served by the fused window path"
    );
    anyhow::ensure!(
        usize::try_from(resp.body.total_bytes)? == PAYLOAD_LEN,
        "the fused path commits to the whole-blob total, got {}",
        resp.body.total_bytes
    );
    conn.close(0u32.into(), b"done");
```

- [ ] **Step 5: Run the new/flipped tests to see them fail**

Run:
```bash
cargo nextest run -p decdn-node --test origin_range_pull --test partial_serve --test node_origin_pull -E 'test(bounded_whole_blob_own_origin_miss_streams_via_backend_origin) | test(bounded_unaligned_offset_own_origin_miss_streams_the_exact_bytes) | test(cold_range_request_pulls_only_the_range_from_origin) | test(resume_to_end_range_pull_serves_tail) | test(partial_serve_mixed_range_pulls_only_the_missing_group) | test(window_pull_through_resumed_offset_is_served_by_the_fused_path)'
```
Expected: all six FAIL — the four own-origin ones on `local_outboard_serves_total == 1` (the buffered tier served them), the mixed-range one because the buffered tier requests the whole span and gets a 404, the node→node one on `resp.body.ok`.

- [ ] **Step 6: Re-route dispatch and delete the buffered range tier**

In `crates/node/src/handlers/client/dispatch.rs`:

1. Delete `let mut range_pulled_size: Option<u64> = None;` (`:664`) and the comment lines `:660-663` that introduce it.
2. Delete the block at `:952-959` (the `if (req.byte_offset > 0 || req.byte_len > 0) && self.pull_authorized(..) { try_range_pull_through .. }`), keeping just `let mut fault_seen = false;`.
3. In the comment block above it, replace the paragraph starting "Origin-tier range pull-through (#823, ADR 037 §Origin-tier pull-through). When the request is a bounded/offset range …" through "… (ADR 037 §\"Fallback is always correct\")." with:
   ```
   // Every fill tier below is range-aware: a bounded or resumed request
   // (`byte_offset > 0 || byte_len > 0`) takes the same two-leg spine as a
   // whole-blob request, and the spine's pull leg fetches only the requested
   // span's missing chunk groups (ADR 037 §Origin-tier pull-through). No tier
   // buffers a requested span before it signs the response.
   ```
   Replace "The fault latch (#1129). Declared BEFORE the range tier, not after it: … Latch every tier." with "The fault latch (#1129): declared before the first tier so every tier's `CacheError::Store` lands in it."
   Replace "Pre-spend deposit floor (#1519). Every fill tier below spends: the range and local tiers front the operator's own origin egress, and the buffered tier's `cache.populate` walks the paid `Peer` origin and fronts real upstream USDC. (The range tier is own-egress-only … Nothing would fail.) All three are gated" with "Pre-spend deposit floor (#1519). Every fill tier below spends: the own-origin spine and the local tier front the operator's own origin egress, and the peer spine and the buffered tier front real upstream USDC. All are gated".
4. Own-origin spine gate (`:993-998`): change to `if !locally_filled && self.pull_authorized(&req, verified_client) {`. Rewrite the comment lines `:972-974` to: "Any request shape routes here: the serve leg clamps to `[byte_offset, end)` and the pull leg fills only that span's missing chunk groups, so a bounded request costs exactly its aligned span plus one outboard read in origin egress."
5. Local-populate gate (`:1080-1084`): drop `range_pulled_size.is_none() &&`.
6. The `if range_pulled_size.is_some() || locally_filled {` arm (`:1109`): change to `if locally_filled {` and rewrite its comment to "The whole blob just filled from a local origin (#1116). Skip the node→node fill and fall through to the size gate + delivery."
7. Peer spine gate (`:1115-1118`): drop `&& req.byte_offset == 0 && req.byte_len == 0`. Rewrite the comment at `:1099-1108` to: "Any request shape routes here. `serve_leg` clamps delivery to `[offset, offset + len)` and bills only the wire it delivers; the pull leg pulls only `missing_ranges(offset, len)` upstream, so a bounded or resumed request fronts exactly its span."
8. The buffered `else` arm comment (`:1145-1146`): "used when the window provider is unset or for a resumed request." → "used when no window provider is set."
9. Size gate (`:1186-1190`): drop the `range_pulled_size` arm, keep `hit_size` / `inspect`; delete the comment sentences at `:1170-1174` about "An origin-tier range pull (#823) imported only a *partial* blob …".

In `crates/node/src/handlers/client/fill.rs`: delete `try_range_pull_through` and its doc comment (`:200-267`); drop `RangePullOutcome` from the `use super::{ .. }` at `:5`.

In `crates/node/src/handlers/client/mod.rs:38`: `use decdn_cache::{CHUNK_GROUP_BYTES, CacheEngine, CacheError, Hash};`.

- [ ] **Step 7: Build, lint, run the six tests**

Run:
```bash
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: clean (an unused-import or dead-code warning here means a leftover reference to the deleted tier — remove it).

Run the same six-test command from Step 5.
Expected: all six PASS.

- [ ] **Step 8: Run the full node + cache + client-pull suites**

Run:
```bash
cargo nextest run -p decdn-node -p decdn-cache -p decdn-client-pull --no-fail-fast
```
Expected: PASS. Watch specifically:
- `origin_range_pull::range_request_without_outboard_falls_back_to_whole_blob` — still passes (no outboard ⇒ the spine's serviceability probe declines ⇒ `try_local_populate` / `try_pull_through` do the whole-blob GET).
- `origin_range_pull::interior_hold_own_origin_miss_pulls_only_the_gaps`, `concurrent_whole_blob_own_origin_misses_coalesce_to_one_pull`, `own_origin_miss_fetches_the_outboard_once_across_draws` — unchanged.
- `client_loopback::client_resumed_range_is_priced_on_the_tail_not_the_whole_blob` and the `#1519` pre-spend tests — unchanged (hit path).
- `crates/cache/tests/pull_through.rs::range_pull_*` and `present_ranges.rs` — still pass (the engine method is deleted in Task 7).

- [ ] **Step 9: Commit**

```bash
git add crates/node/src/handlers/client crates/node/tests/origin_range_pull.rs crates/node/tests/partial_serve.rs crates/node/tests/node_origin_pull.rs
git commit --no-verify -m "feat(node): stream bounded and resumed cache-miss requests through the two-leg spine

Dispatch routes every authorized miss — whole blob, bounded range, or
resumed tail — into serve_via_backend_origin / serve_via_window_pull_through.
The signed StreamResponse goes out before the fill; the pull leg fetches
only the span's missing chunk groups; the serve leg streams them as they
land. The buffered try_range_pull_through tier, which downloaded, verified
and imported a whole requested span before answering, is gone.

Closes #2060

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 6: The CLI's throwaway open must never leave the real open attached to a dying fill

`decdn fetch` / `bundle pull` open every entry twice on the node: a throwaway `(0, 0)` open (`open_fetch_prelude`, `crates/cli/src/commands/fetch.rs:1944-1980`) that is dropped as soon as the signed header arrives, then the real `(0, total_bytes)` open. With Task 5 the throwaway open **claims a fill** on the node and starts a pull; the real open arrives while that fill's serve leg is tearing down (its write fails once the client's `Connection::close` lands). `claim_fill` must not hand the real open an observer lease on a session that is about to be cancelled.

What the registry does today (`crates/cache/src/fill_session.rs`): `claim` and `ObserverLease::teardown` take the **same** map lock (`:1152`, `:882`); `teardown` decrements, removes the session from the map, and cancels it under that lock (`:886-901`, the #1610 resurrection-race fix); `claim` skips any session whose `is_dead()` (`outcome().is_some() || cancel.is_cancelled()`, `:559-561`) holds (`:1167-1170`). So the two orderings are: (a) the real claim lands **before** the throwaway's teardown — it attaches, the teardown then sees `prev == 2` and does not cancel, the pull keeps filling for the real client; (b) the real claim lands **after** — the session is gone, the real open is `Owner` of a fresh session. There is no third state. This task pins both orderings with registry unit tests and two integration tests that drive the CLI's exact sequence. **If either integration test fails, the fix belongs in `FillRegistry::claim` / `ObserverLease::teardown`, not in dispatch.**

**Files:**
- Modify: `crates/cache/src/fill_session.rs` test module (`:1414+`)
- Modify: `crates/node/tests/origin_range_pull.rs` (helper + two tests)

**Interfaces:**
- Consumes: Task 5 routing (the throwaway `(0,0)` and the real `(0,total)` both reach the spine).

- [ ] **Step 1: Registry unit tests — both orderings**

In `crates/cache/src/fill_session.rs`'s registry test module (next to `last_observer_leaving_cancels_and_removes`; `G`, `root`, `store_hash`, `mapped`, `observer_count`, `is_cancelled` are the helpers that test already uses — copy their exact names from it):

```rust
    #[test]
    fn claim_after_last_out_release_owns_a_fresh_session() {
        // The CLI's throwaway open: claim, then the ONLY observer leaves. The
        // registry cancels and unmaps that session under the lock. The real open's
        // claim must then OWN a fresh session — never attach to the cancelled one.
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0xD9);

        let FillClaim::Owner {
            session: first,
            lease,
        } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0xD9), total))
        else {
            panic!("owns");
        };
        assert!(lease.release().is_none(), "no parked pull handle to join");
        assert!(first.is_cancelled(), "last-out release cancels the throwaway's fill");
        assert!(!mapped(&reg, hash), "the cancelled session is unmapped");

        let FillClaim::Owner {
            session: second,
            lease: _second_lease,
        } = reg.claim(hash, 0, total, total, || FillSession::new(root(0xD9), total))
        else {
            panic!("the real open must own, not attach");
        };
        assert!(!Arc::ptr_eq(&first, &second), "a fresh session, not the cancelled one");
        assert!(!second.is_cancelled());
        assert_eq!(second.observer_count(), 1);
    }

    #[test]
    fn claim_before_last_out_release_keeps_the_fill_alive() {
        // The other ordering: the real open attaches BEFORE the throwaway's
        // teardown. The teardown then is not last-out, so it must not cancel.
        let total = 8 * G;
        let reg = Arc::new(FillRegistry::new());
        let hash = store_hash(0xDA);

        let FillClaim::Owner {
            session,
            lease: throwaway,
        } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0xDA), total))
        else {
            panic!("owns");
        };
        let FillClaim::Attach {
            session: attached,
            lease: real,
        } = reg.claim(hash, 0, total, total, || panic!("attaches"))
        else {
            panic!("the real open attaches to the live fill");
        };
        assert!(Arc::ptr_eq(&session, &attached));

        assert!(throwaway.release().is_none());
        assert!(!session.is_cancelled(), "the real observer keeps the fill alive");
        assert!(mapped(&reg, hash));
        assert_eq!(session.observer_count(), 1);

        assert!(real.release().is_none());
        assert!(session.is_cancelled(), "the real observer's exit is last-out");
        assert!(!mapped(&reg, hash));
    }
```

- [ ] **Step 2: Run them**

Run: `cargo nextest run -p decdn-cache -E 'test(claim_after_last_out_release_owns_a_fresh_session) | test(claim_before_last_out_release_keeps_the_fill_alive)'`
Expected: PASS (the registry already behaves this way; the tests pin it). If either fails, stop and fix `claim` / `teardown` before continuing.

- [ ] **Step 3: Integration helper — the throwaway open**

In `crates/node/tests/origin_range_pull.rs`, after `ranged_paid_pull`, add:

```rust
/// The CLI's throwaway handshake (`open_fetch_prelude`): a bound `(0, 0)` request,
/// read the signed `StreamResponse` for `total_bytes`, then close the connection
/// without paying or reading a byte. Returns the advertised total.
async fn throwaway_open(
    client_ep: &iroh::Endpoint,
    target: EndpointAddr,
    client_node_id: B256,
    client_eth: &Arc<PrivateKeySigner>,
    pool_id: B256,
    hash: Hash,
) -> anyhow::Result<u64> {
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
    let binding_hash =
        binding_signing_hash(client_node_id, EPHEMERAL_BINDING_NONCE, &binding_dom());
    let binding_signature = client_eth
        .sign_hash_sync(&binding_hash)?
        .as_bytes()
        .to_vec();
    let ext = StreamRequestExt {
        binding: Some(ClientBinding {
            ethereum_address: client_eth.address().into(),
            binding_signature,
        }),
        capability: None,
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id.into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x9002,
    };
    let payload =
        encode_stream_request(&req, Some(&ext)).map_err(|e| anyhow::anyhow!("encode req: {e}"))?;
    write_frame_to(&mut send, &payload).await?;
    let (resp, resp_ext) = read_stream_response(&mut recv).await?;
    anyhow::ensure!(resp.body.ok, "throwaway open refused: {:?}", resp_ext.error);
    // Exactly what `UpstreamPull::abort` / `Drop` do on the CLI: close the
    // connection; the node's serve leg learns of it on its next write.
    conn.close(0u32.into(), b"client-abandoned");
    Ok(resp.body.total_bytes)
}
```

- [ ] **Step 4: Integration test — real open lands while the throwaway's fill is still live**

Delay the origin's ranged 206 by 500 ms so the throwaway's serve leg is still parked on its first frame (it has nothing to write, so it has not yet noticed the close) when the real open arrives. The real open must attach, the throwaway's later teardown must not cancel, and the origin must see exactly one ranged GET.

```rust
/// The CLI's real sequence on a cold miss: a throwaway `(0, 0)` open dropped as soon
/// as the header lands, then the real `(0, total)` open. Here the origin is slow, so
/// the throwaway's fill is still LIVE (its serve leg parked on the first frame) when
/// the real open claims: it attaches, the throwaway's teardown is not last-out, and
/// ONE origin pull serves the real client byte-exact.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn throwaway_open_then_real_open_coalesces_onto_the_live_fill() -> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let aligned = align_range(0, 0, blob_size)?;
    let (a_start, a_end) = (aligned.fetch_start(), aligned.fetch_end());
    let span = blob
        .get(usize::try_from(a_start)?..usize::try_from(a_end)?)
        .ok_or_else(|| anyhow::anyhow!("aligned span out of bounds"))?
        .to_vec();
    let range_val = format!("bytes={a_start}-{}", a_end - 1);

    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path(format!("/{hex}")))
        .respond_with(
            ResponseTemplate::new(200).insert_header("Content-Length", blob_size.to_string()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    // Slow origin: the first draw takes 500 ms, so the throwaway's serve leg is
    // still parked on frame 1 when the real open arrives.
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header("range", range_val.as_str()))
        .respond_with(
            ResponseTemplate::new(206)
                .set_body_bytes(span.clone())
                .set_delay(Duration::from_millis(500)),
        )
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x64);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let total = throwaway_open(
        &client_ep,
        target.clone(),
        client_node_id,
        &client_eth,
        pool_id,
        hash,
    )
    .await?;
    anyhow::ensure!(total == blob_size, "throwaway header total mismatch");

    // Immediately — no sleep — the real open, exactly as `drive` does.
    let got = ranged_paid_pull(
        &client_ep,
        target,
        client_node_id,
        &client_eth,
        pool_id,
        provider,
        hash,
        0,
        total,
        RATE_PER_MB,
    )
    .await?;
    anyhow::ensure!(
        got.as_slice() == blob.as_slice(),
        "real open after a throwaway must deliver byte-exact"
    );

    // Both opens took the own-origin tier; the origin served ONE ranged span; the
    // blob is complete in cache (the real client paid the fill to the end).
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 2,
        "both the throwaway and the real open must enter the two-leg tier"
    );
    let ranged_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && r.headers.contains_key("range")
    })
    .await?;
    anyhow::ensure!(
        ranged_gets == 1,
        "the real open must attach to the throwaway's live fill, not start a second pull; saw {ranged_gets} ranged GET(s)"
    );
    anyhow::ensure!(cache.has(hash).await?, "the coalesced fill completes the blob");

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}
```

- [ ] **Step 5: Integration test — real open lands after the throwaway's fill is torn down**

No origin delay, and a 300 ms pause between the two opens so the throwaway's serve leg has noticed the close and its last-out teardown has cancelled + unmapped the fill (or the tiny blob has already completed). The real open must be served byte-exact either way and must never hang or attach to the dead session.

```rust
/// The other ordering: the throwaway's fill is already TORN DOWN (last-out cancel,
/// or complete for a small blob) when the real open arrives. The real open must own
/// a fresh fill or hit the cache — never park on the dead session — and deliver
/// byte-exact within a hard timeout.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn throwaway_open_torn_down_before_real_open_still_serves() -> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path(format!("/{hex}")))
        .respond_with(
            ResponseTemplate::new(200).insert_header("Content-Length", blob_size.to_string()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    let blob_for_resp = blob.clone();
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header_exists("range"))
        .respond_with(move |req: &Request| {
            let span = req
                .headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_byte_range)
                .and_then(|(s, e)| Some((usize::try_from(s).ok()?, usize::try_from(e).ok()?)))
                .and_then(|(s, e)| blob_for_resp.get(s..=e));
            match span {
                Some(body) => ResponseTemplate::new(206).set_body_bytes(body.to_vec()),
                None => ResponseTemplate::new(416),
            }
        })
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x65);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, _cache, _metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let total = throwaway_open(
        &client_ep,
        target.clone(),
        client_node_id,
        &client_eth,
        pool_id,
        hash,
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let got = tokio::time::timeout(
        Duration::from_secs(20),
        ranged_paid_pull(
            &client_ep,
            target,
            client_node_id,
            &client_eth,
            pool_id,
            provider,
            hash,
            0,
            total,
            RATE_PER_MB,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("real open hung after the throwaway's teardown"))??;
    anyhow::ensure!(
        got.as_slice() == blob.as_slice(),
        "real open after a torn-down throwaway must deliver byte-exact"
    );

    // Never a whole-blob GET, and at most one extra ranged draw for the span the
    // cancelled fill was mid-way through.
    let wholeblob_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && !r.headers.contains_key("range")
    })
    .await?;
    anyhow::ensure!(wholeblob_gets == 0, "saw {wholeblob_gets} un-ranged GET(s)");
    let ranged_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && r.headers.contains_key("range")
    })
    .await?;
    anyhow::ensure!(
        ranged_gets <= 2,
        "a torn-down throwaway costs at most one duplicate draw, saw {ranged_gets}"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}
```

- [ ] **Step 6: Run both integration tests (three times, to catch timing flakes)**

Run:
```bash
for i in 1 2 3; do cargo nextest run -p decdn-node --test origin_range_pull -E 'test(throwaway_open_)' || break; done
```
Expected: PASS every run. A failure of `..._coalesces_onto_the_live_fill` on `ranged_gets == 1` means the real open started a second pull — the registry attached to nothing or the throwaway's teardown cancelled a fill the real client was observing; fix in `FillRegistry::claim` / `ObserverLease::teardown` (`crates/cache/src/fill_session.rs`), then add a registry unit test for the exact interleaving you found. A timeout in `..._torn_down_before_real_open_still_serves` means the real open parked on a dead session — same place to fix.

- [ ] **Step 7: Commit**

```bash
git add crates/cache/src/fill_session.rs crates/node/tests/origin_range_pull.rs
git commit --no-verify -m "test(node,cache): pin the CLI's throwaway-then-real open against the fill registry

The throwaway (0,0) open claims a fill and drops it; the real (0,total)
open must attach only to a live fill (its teardown is then not last-out)
or own a fresh one after the teardown — never park on a dying session.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 7: Delete the dead engine range tier

`CacheEngine::pull_through_range` / `range_pull_attempt` / `RangePullOutcome` / `origin_fetch_range_bytes` have no production caller after Task 5. `origin_encode_range`, `origin_fetch_outboard_bytes`, `OriginRangeRequest`, `Origin::fetch_range`, and `Origin::fetch_outboard` stay — the spine uses them.

**Files:**
- Modify: `crates/cache/src/engine.rs:1132-1143` (`RangePullOutcome`), `:3321-3513` (`pull_through_range`, `range_pull_attempt`, `origin_fetch_range_bytes`), doc mentions at `:159`, `:1123`, `:2627`, `:3574-3587`, `:3642`, `:3839-3856`, `:4187`
- Modify: `crates/cache/src/lib.rs:7`, `:43`; `crates/cache/src/range_pull.rs:4-5`; `crates/cache/src/origin/mod.rs:424`
- Modify: `crates/cache/tests/pull_through.rs:4080-4460` (delete the eight `range_pull_*` tests + any helper only they use)
- Modify: `crates/cache/tests/present_ranges.rs:31-110` (three fixtures)

- [ ] **Step 1: Re-base the `present_ranges.rs` fixtures on `admit_bao` (they must pass before and after the deletion)**

Add a helper at the bottom of `crates/cache/tests/present_ranges.rs`:

```rust
/// Admit the chunk-group-aligned span `[off, off + len)` of `payload` as a verified
/// partial blob, the way a pull leg's admission does, without any origin.
async fn admit_span(
    engine: &decdn_cache::CacheEngine,
    payload: &[u8],
    off: u64,
    len: u64,
) -> anyhow::Result<(Hash, decdn_cache::range_pull::AlignedRange)> {
    let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(
        payload,
        decdn_cache::range_pull::IROH_BLOCK_SIZE,
    );
    let hash = Hash::from_bytes(*ob.root.as_bytes());
    let total = u64::try_from(payload.len())?;
    let aligned = align_range(off, len, total)?;
    let slice = payload
        .get(usize::try_from(aligned.fetch_start())?..usize::try_from(aligned.fetch_end())?)
        .ok_or_else(|| anyhow::anyhow!("aligned span out of bounds"))?;
    let encoded = decdn_cache::range_pull::encode_verified_range(
        *hash.as_bytes(),
        &aligned,
        slice,
        bytes::Bytes::from(ob.data),
    )?;
    engine
        .admit_bao(hash, aligned.chunk_ranges().clone(), encoded)
        .await?;
    Ok((hash, aligned))
}
```

Then rewrite the three tests:

```rust
#[tokio::test]
async fn partial_import_reports_only_the_imported_span() -> anyhow::Result<()> {
    let payload = util::make_blob(200 * 1024);
    let blob_size = u64::try_from(payload.len())?;
    let (engine, _tmp) = util::empty_engine().await?;

    // Admit a middle span only (chunk-group aligned).
    let (hash, aligned) = admit_span(&engine, &payload, 64 * 1024, 32 * 1024).await?;

    let pr = engine.present_ranges(hash).await?;
    assert!(!pr.is_complete(), "a middle span is not the whole blob");
    assert!(!pr.is_empty(), "the admitted span is present");
    let cr = pr.chunk_ranges();
    let group_chunks = bao_tree::ChunkNum::chunks(aligned.fetch_end()).0
        - bao_tree::ChunkNum::chunks(aligned.fetch_start()).0;
    assert!(group_chunks > 0);
    let whole_chunks = bao_tree::ChunkNum::chunks(blob_size).0;
    let present_chunk_count: u64 = cr
        .boundaries()
        .chunks(2)
        .filter_map(|w| match w {
            [a, b] => Some(b.0 - a.0),
            _ => None,
        })
        .sum();
    assert!(present_chunk_count < whole_chunks);
    assert!(present_chunk_count >= group_chunks);
    Ok(())
}

#[tokio::test]
async fn missing_ranges_empty_when_span_present() -> anyhow::Result<()> {
    let payload = util::make_blob(200 * 1024);
    let blob_size = u64::try_from(payload.len())?;
    let (engine, _tmp) = util::empty_engine().await?;

    let (req_start, req_len) = (64 * 1024, 32 * 1024);
    let (hash, _aligned) = admit_span(&engine, &payload, req_start, req_len).await?;

    let missing = engine
        .missing_ranges(hash, req_start, req_len, blob_size)
        .await?;
    assert!(missing.is_empty());
    Ok(())
}

#[tokio::test]
async fn missing_ranges_covers_absent_span() -> anyhow::Result<()> {
    let payload = util::make_blob(200 * 1024);
    let blob_size = u64::try_from(payload.len())?;
    let (engine, _tmp) = util::empty_engine().await?;

    let (hash, _aligned) = admit_span(&engine, &payload, 0, 32 * 1024).await?;
    let missing = engine
        .missing_ranges(hash, 128 * 1024, 32 * 1024, blob_size)
        .await?;
    assert!(!missing.is_empty(), "the later span was never admitted");
    Ok(())
}
```

If `util::engine_with_range_origin` is now unused, delete it and its `RangeResponder` / `parse_range_header` helpers from `crates/cache/tests/util/mod.rs` (they carry `#[allow(dead_code)]`, so the compiler will not tell you — grep for callers).

Run: `cargo nextest run -p decdn-cache --test present_ranges`
Expected: PASS.

- [ ] **Step 2: Delete the engine methods and their tests**

In `crates/cache/src/engine.rs`: delete `pub enum RangePullOutcome` and its doc (`:1132-1143`), `pub async fn pull_through_range` (with its doc block and the `origin_range_pull` span), `async fn range_pull_attempt`, and `async fn origin_fetch_range_bytes`. Grep the file for `pull_through_range`, `range_pull_attempt`, `origin_fetch_range_bytes` and fix every remaining doc mention so each describes `origin_encode_range` / the spine instead.

In `crates/cache/src/lib.rs`: drop `RangePullOutcome` from the re-export at `:43`; fix the mention at `:7`. In `crates/cache/src/range_pull.rs:4-5`: "origin-import (`engine::pull_through_range`)" → "origin range encode (`engine::origin_encode_range`)". In `crates/cache/src/origin/mod.rs:424`: same substitution.

In `crates/cache/tests/pull_through.rs`: delete the eight tests `range_pull_serves_subrange_without_whole_blob_fetch`, `range_pull_degrades_when_no_outboard_published`, `range_pull_degrades_when_origin_ignores_range`, `range_pull_rejects_out_of_bounds_range`, `range_pull_no_origin_configured_errors`, `range_pull_falls_back_to_second_origin_outboard`, `range_pull_zero_len_reads_to_end`, `range_pull_refuses_logically_evicted_hash`, and the `serve_range_origin` helper if nothing else uses it (grep).

- [ ] **Step 3: Build, lint, test the cache crate and the crate-edge check**

Run:
```bash
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: clean.

Run: `cargo nextest run -p decdn-cache --no-fail-fast`
Expected: PASS.

Run: `python3 .github/scripts/check_crate_edges.py`
Expected: no change (no dependency edge moved).

- [ ] **Step 4: Commit**

```bash
git add crates/cache
git commit --no-verify -m "refactor(cache): delete the buffered origin range tier

pull_through_range / range_pull_attempt / RangePullOutcome have no caller:
the node serves every ranged miss through the two-leg spine, whose
BackendSource uses origin_encode_range. present_ranges tests admit their
partial spans directly.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 8: ADR 037, doc gate, PR

**Files:**
- Modify: `adr/037-regional-proxy-warming.md` (§Origin-tier pull-through `:74-83`, §Origin-tier whole-blob miss `:85-95`, §Implementation status `:120-135`, §Cross-ADR Impact `:163`, §Acceptance Criteria `:182-183`)
- Delete: `docs/superpowers/plans/2026-09-19-stream-bounded-miss-through-spine.md` (working specs stay out of the merged tree)

- [ ] **Step 1: Rewrite the range-tier paragraphs in STE**

§ "Origin-tier pull-through: ranged fetch + external outboard" — replace the last two bullets (`- **Verification is self-validating …**` and `- **Fallback is always correct.** …`) with:

```markdown
- **Verification is self-validating against `H`.** The node verifies each fetched chunk group against the root `H` with the supplied outboard (`bao-tree` range validation over a pre-order outboard anchored at `root = H`). The outboard is **untrusted**. A tampered outboard, a tampered data range, or a wrong root fails verification, because every parent hash must chain up to `H` and every leaf must hash to its anchored parent. The origin is a dumb byte store, exactly as for whole-blob pulls ([ADR 002 § Content Addressing](002-content-addressing.md#adr-002-content-addressing)).
- **The node streams while it fills.** A cache miss for any request shape — whole blob, bounded range, or resumed tail — runs two legs. The pull leg fetches `{H}.obao4` once, then fetches only the chunk groups of the requested span that the cache does not hold, in draws of at least half the credit window, and admits each verified group as a partial blob. The serve leg signs `StreamResponse` before the first draw and streams each group to the paying client as it lands. Time-to-first-byte does not wait for the span to download. The node never holds a requested span in memory.
- **Fallback is always correct.** When the origin does not publish `{H}.obao4` or does not report a size, the node degrades to the whole-blob origin pull: import, materialize the outboard, then serve. The optimization is a cost reduction on the origin hop. Its absence is never a correctness or availability failure.
```

§ "Origin-tier whole-blob miss: stream-while-store" — rename the heading to "### Origin-tier miss: stream-while-store" (update every intra-document anchor `#origin-tier-whole-blob-miss-stream-while-store` → `#origin-tier-miss-stream-while-store`; grep). Replace its first paragraph with:

```markdown
A cache miss against an origin that publishes `{H}.obao4` streams origin bytes to the paying client while the node admits the same bytes into its store. This holds for a whole-blob request (`byte_offset == 0`, `byte_len == 0`), a bounded request, and a resumed tail. The pull leg draws `[a, b)` spans of the missing chunk groups; the serve leg delivers `[byte_offset, end)` from the filling cache.
```

Delete the paragraph "This path applies only to unbounded whole-blob requests; a bounded/ranged request stays on the range-scoped origin pull above. When the origin publishes no `{H}.obao4`, …" and replace it with: "When the origin publishes no `{H}.obao4`, the miss falls back to the buffered path: import the whole blob, then serve."

§ "Implementation status", first paragraph: replace "(`window.rs`: `serve_via_window_pull_through` / `window_forward_loop`). It fuses a progressive upstream pull (`crate::client_requester::open_progressive_pull`, driven via `crate::node_origin::NodeOrigin::open_progressive_pull`) with downstream delivery: each upstream chunk is forwarded to the paying client and tee'd into the cache (`decdn_cache::CacheEngine::open_tee_sink`), and the pull pauses" with "(`window.rs`: `serve_via_window_pull_through` for a peer upstream, `serve_via_backend_origin` for the node's own origin). Each runs a pull leg (`node_origin::pull_leg`, driving `decdn_client_pull::drive` over `missing_ranges(byte_offset, byte_len)`) beside a serve leg (`serve_leg`, streaming `[byte_offset, end)` from the filling cache through the coherent bao encoder). The pull pauses". After "The pull leg's `decdn_client_pull::RampPacer` computes that same window from the stream's own paid frontier" add " — the paid bytes past the request's own start, so a resumed request ramps from the floor —". After the sentence ending "the pull paces on the paid content frontier." add: "The pull waits until at least half its window is free before it draws, so a fast origin is drawn in large spans, not one chunk per voucher."

Replace the deferral bullet "**Partial-blob (bao range) store and serving** — … falls back to the buffered whole-blob path. Range-addressed availability …" with:

```markdown
- **Partial-blob (bao range) store and serving** — specified in [ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1) and landed: the two-leg tiers serve every request shape (whole blob, bounded, resumed) from a peer upstream or from the node's own origin, and each chunk group verifies against `H` on its own. A miss whose source publishes no `{H}.obao4` falls back to the buffered whole-blob path. Range-addressed availability in `cdn/dht/v1` is carried by the coverage bitmap (§ DHT advertising is range-keyed).
```

Replace the "**Range-scoped origin pull** … — **landed**. …" bullet with:

```markdown
- **Range-scoped origin pull** (§ [Origin-tier pull-through](#origin-tier-pull-through-ranged-fetch--external-outboard)) — **landed** as the own-origin two-leg tier. `dispatch.rs` routes an authorized cache miss of any shape to `serve_via_backend_origin` when `CacheEngine::origin_size` and `CacheEngine::origin_fetch_outboard_bytes` both succeed. The outboard from that probe is handed to the pull leg's `BackendSource`, which calls `CacheEngine::origin_encode_range` per draw (`Range` data only, verified against `H` with the one outboard), and the serve leg streams the admitted groups. End-to-end coverage is in `crates/node/tests/origin_range_pull.rs` and `partial_serve.rs`.
```

§ Cross-ADR Impact, ADR 005 bullet: "so a node can scope its origin fetch to exactly `[byte_offset, byte_offset + byte_len)`" → "so a node's pull leg fetches only the missing chunk groups of `[byte_offset, byte_offset + byte_len)`".

§ Acceptance Criteria: replace items 12 and 13 with:

```markdown
12. A cache-miss serve of any request shape (whole blob, bounded range, resumed tail) against an origin that publishes `{H}.obao4` signs `StreamResponse` before the first origin draw, fetches the outboard once and only the missing chunk groups of the requested span, verifies each group against the root `H` with the untrusted outboard (rejecting a tampered range, a tampered outboard, or a wrong root), streams each verified group to the paying client as it lands, and counts only the pulled bytes against the ramped credit window. The node holds no requested span in memory. An origin without the outboard degrades to a whole-blob pull with no correctness or availability change.
13. A bounded or resumed cache-miss request with an offset at or past the blob end, or an end past the blob, is refused with `RangeNotSatisfiable` before any response is signed, on every serve tier.
```

- [ ] **Step 2: Grep for stale mentions repo-wide**

Run:
```bash
grep -rn "try_range_pull_through\|pull_through_range\|range_pull_attempt\|RangePullOutcome\|origin_fetch_range_bytes\|window_forward_loop\|open_tee_sink" --include=*.rs --include=*.md crates adr docs CONTRIBUTING.md CLAUDE.md
```
Expected: no output. Fix any hit.

- [ ] **Step 3: Run the full local gate**

Run:
```bash
cargo fmt -- --check && cargo clippy --workspace --all-targets -- -D warnings && RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --document-private-items && cargo nextest run --workspace --no-fail-fast && python3 .github/scripts/check_crate_edges.py
```
Expected: all green (on macOS the known #697 `bundle_create` case-insensitivity failure is the only acceptable red).

- [ ] **Step 4: Commit the ADR**

```bash
git add adr/037-regional-proxy-warming.md
git commit --no-verify -m "docs(adr): ADR 037 describes the two-leg spine for every request shape

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Drop the plan file and open the PR**

```bash
git rm docs/superpowers/plans/2026-09-19-stream-bounded-miss-through-spine.md
git commit --no-verify -m "docs: drop the #2060 implementation plan from the repo

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
git push -u origin feat/2060-stream-bounded-miss
gh pr create --title "feat(node): stream bounded and resumed cache-miss requests through the two-leg spine" --body "Closes #2060. Refs #2061 — §1 (outboard once per fill) and §2 (minimum draw) land here; §3 (prefetch) and §4 (origin-leg timeout) stay open there. Follow-ups filed: #2062, #2063.

On a cold miss the CLI's real request is \`byte_len = total_bytes\`, which dispatch routed to \`try_range_pull_through\`: download the whole span + outboard into memory, verify, import, and only then sign \`StreamResponse\`. No timeout, no keepalive — a multi-GiB file trips the client's 30 s stall clock while the node is still buffering.

Every authorized miss — whole blob, bounded, resumed — now takes the two-leg spine (\`serve_via_backend_origin\` / \`serve_via_window_pull_through\`): sign first, pull only the span's missing chunk groups in credit-window draws, stream each verified group as it lands. The buffered range tier and \`CacheEngine::pull_through_range\` are deleted.

Spine cost fixes that had to land with the gate (#2061):
- \`Origin::fetch_range\` returns data only; the outboard is fetched once by the dispatch probe and carried by \`BackendSource\` for every draw.
- \`WindowPacer\` waits until half its window is free before drawing, so a fast origin is drawn in large spans instead of one chunk per voucher.

Spine hardening that the offset-0 gate had been hiding:
- \`RampPacer\` ramps on \`served_paid − served_start\`, so a resumed request does not open \`credit_max\` before its first voucher.
- \`serve_leg\` maps paid wire back to content from the group-aligned fetch start and guards an attached observer's frontier extension.
- Both spine functions refuse an out-of-bounds range with \`RangeNotSatisfiable\` before signing and price their floor guard at the request's aligned span.

The CLI's throwaway \`(0,0)\` open followed by the real \`(0,total)\` open is pinned in both orderings against the fill registry (attach to a live fill; own a fresh one after teardown; never park on a dying session).

Tests: CLI-shaped \`(0, total)\` and unaligned-offset misses stream via the own-origin tier; a 3 MiB pull fetches the outboard once across draws; the mixed present/absent range pulls only the missing group; a resumed node→node miss is served by the fused path; the resumed-offset-refused and range-tier cache tests are gone.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

## Follow-ups (filed; nothing here needs a second PR)

- [#2062](https://github.com/decdn/decdn/issues/2062) — an observer attached ahead of a stalled owner's paid frontier parks until its own stall budget expires (the `extend_served_from` guard Task 4 keeps).
- [#2063](https://github.com/decdn/decdn/issues/2063) — the CLI's throwaway `(0, 0)` prelude open claims a fill it abandons (one wasted draw per entry; a duplicate draw in Task 6's torn-down ordering).
- [#2061](https://github.com/decdn/decdn/issues/2061) §3 / §4 — prefetch the next draw; per-draw origin-leg timeout. Recorded there as a comment; this PR references #2061 and does not close it.

## Verification (end-to-end)

1. `cargo nextest run -p decdn-node --test origin_range_pull --test partial_serve --test node_origin_pull` — the routing tests from Task 5, the outboard-once test from Task 3b, and both throwaway-open tests from Task 6 pass.
2. Rebuild both binaries and run the anvil e2e CLI journey (`cargo nextest run -p decdn-e2e --features anvil-e2e --test <journey stem>`, see `crates/e2e`) with a blob larger than one credit window through a cold node: `decdn bundle pull` completes with the default `--stall-timeout-ms 30000`, and the node's `decdn_local_outboard_serves_total` increments twice per entry (throwaway + real).
3. Node RSS during that pull stays flat relative to blob size (the spine holds at most one credit-window draw), and the origin's access log shows one `.obao4` GET per entry.
