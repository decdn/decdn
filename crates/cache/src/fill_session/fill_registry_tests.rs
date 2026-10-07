use std::sync::Arc;
use std::sync::atomic::Ordering;

use bao_tree::io::fsm::Outboard;
use bao_tree::{BaoTree, ChunkRanges, blake3};
use decdn_bao_range::{IROH_BLOCK_SIZE, align_range};

use super::{FillClaim, FillError, FillRegistry, FillSession};
use crate::{CHUNK_GROUP_BYTES, Hash};

/// One chunk group of bytes, the alignment granularity `claim` snaps to.
const G: u64 = CHUNK_GROUP_BYTES;

fn store_hash(byte: u8) -> Hash {
    Hash::from_bytes([byte; 32])
}

/// Whether the registry map still holds an entry (a non-empty session Vec) for
/// `hash`. Reaches the private `map` field directly — the discriminating check
/// for last-observer removal, which a coverage query alone cannot distinguish
/// from the `is_dead` skip.
fn mapped(reg: &FillRegistry, hash: Hash) -> bool {
    reg.map
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&hash)
        .is_some_and(|entry| !entry.sessions.is_empty())
}

fn root(byte: u8) -> blake3::Hash {
    blake3::Hash::from([byte; 32])
}

fn hb(byte: u8) -> blake3::Hash {
    blake3::Hash::from([byte; 32])
}

/// The chunk ranges covering the byte span `[start, start+len)` of a `total`
/// blob (`len == 0` = to end), built via the same `align_range` the registry
/// uses so `covered` and the expected splits cannot drift.
fn ranges(start: u64, len: u64, total: u64) -> ChunkRanges {
    align_range(start, len, total)
        .expect("aligned range")
        .chunk_ranges()
        .clone()
}

/// The first interior node whose byte span lies wholly inside the chunk range
/// `[start_group, end_group)` (in groups), for exercising per-hash proof reads.
fn interior_node_in(total: u64, start_group: u64, end_group: u64) -> bao_tree::TreeNode {
    let tree = BaoTree::new(total, IROH_BLOCK_SIZE);
    let want = ranges(start_group * G, (end_group - start_group) * G, total);
    tree.pre_order_nodes_iter()
        .find(|n| {
            tree.pre_order_offset(*n).is_some()
                && (&ChunkRanges::from(n.chunk_range()) - &want).is_empty()
        })
        .expect("an interior node inside the range")
}

/// A registered session is bound to the registry, so `range_still_live` consults
/// the whole registry (not just the session's own liveness).
fn register(
    reg: &Arc<FillRegistry>,
    hash: Hash,
    root: blake3::Hash,
    total: u64,
    cov: ChunkRanges,
) -> (Arc<FillSession>, super::ObserverLease) {
    // As the node builds an owner: the paid frontier starts at the request's
    // own start, so a fill for `[half, total)` is reachable by a claim at
    // `half` (`FillRegistry::claim` attaches only at or behind the frontier).
    let start = cov
        .boundaries()
        .first()
        .map_or(0, bao_tree::ChunkNum::to_bytes);
    let s = FillSession::starting_at(root, total, start);
    s.set_covered(cov);
    let lease = reg.register_fill(hash, &s);
    (s, lease)
}

/// A proof read parked on an uncaptured node records the first byte of that
/// node's range as what it waits on, so a starved frame consumer can demand it
/// from a pull whose window has closed (#1893). The park alone raises no serve
/// demand: an encoder reads ahead of its consumer, and only the consumer knows
/// when it is stuck.
#[tokio::test]
async fn a_parked_proof_read_records_the_first_byte_of_its_node() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x4A);
    let (owner, _ol) = register(&reg, hash, hb(0x4A), total, ranges(0, 0, total));

    let node = interior_node_in(total, 4, 8);
    let node_start = node.chunk_range().start.to_bytes();
    assert!(node_start >= 4 * G, "the node lies past the first half");
    let mut reader = owner.outboard_reader();
    let parked_on = reader.parked_on();

    let load = tokio::spawn(async move { reader.load(node).await });
    tokio::task::yield_now().await;
    assert!(!load.is_finished(), "load parks until the node is captured");
    assert_eq!(
        parked_on.load(Ordering::Acquire),
        node_start + 1,
        "the parked read records one byte into the node's range"
    );
    assert_eq!(
        owner.serve_demand().get(),
        0,
        "the park alone raises no serve demand"
    );

    let pair = (hb(3), hb(4));
    owner.capture(node, pair);
    assert_eq!(load.await.unwrap().unwrap(), Some(pair));
}

/// A serve leg's demand reaches the live fill that produces the awaited byte and
/// no other: under coalescing that fill may be a sibling of the leg's own
/// session. A dead fill produces nothing, so it never holds a demand.
#[test]
fn demand_reaches_the_live_fill_that_produces_the_byte() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x4B);
    let (a, _al) = register(&reg, hash, hb(0x4B), total, ranges(0, 4 * G, total));
    let (b, _bl) = register(&reg, hash, hb(0x4B), total, ranges(4 * G, 4 * G, total));
    let (dead, _dl) = register(&reg, hash, hb(0x4B), total, ranges(4 * G, 2 * G, total));
    dead.mark_ended(Err(FillError::new("ended before the demand")));

    let leg = a.demand_slot();
    leg.stand(5 * G);
    assert_eq!(
        b.serve_demand().get(),
        5 * G,
        "the sibling produces the byte"
    );
    assert_eq!(a.serve_demand().get(), 0, "the leg's own fill does not");
    assert_eq!(dead.serve_demand().get(), 0, "a dead fill is skipped");

    leg.stand(3 * G);
    assert_eq!(a.serve_demand().get(), 3 * G, "the demand follows the leg");
    assert_eq!(
        b.serve_demand().get(),
        0,
        "and leaves the fill it no longer awaits"
    );
}

/// A serve leg parked in a sibling fill's range, further down the blob, does
/// not hide a nearer parked leg from the fill that produces its bytes. A
/// masked demand leaves that fill's pull waiting on a payment its parked serve
/// leg cannot collect, so neither leg moves.
#[test]
fn a_sibling_fills_far_demand_does_not_hide_a_near_one() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x4E);
    let (near, _nl) = register(&reg, hash, hb(0x4E), total, ranges(0, 4 * G, total));
    let (far, _fl) = register(&reg, hash, hb(0x4E), total, ranges(4 * G, 4 * G, total));

    let far_leg = far.demand_slot();
    far_leg.stand(6 * G);
    let near_leg = near.demand_slot();
    near_leg.stand(G);

    assert_eq!(
        near.serve_demand().get(),
        G,
        "the near fill's pull sees the leg parked in its own range"
    );
    assert_eq!(
        far.serve_demand().get(),
        6 * G,
        "the far fill's pull sees only the leg parked in its range"
    );
}

/// Within one fill, the pull reads the NEAREST standing demand: a leg attached
/// further down the fill's range does not hide a leg parked at the pull's
/// frontier. A withdrawn or dropped slot leaves nothing standing.
#[test]
fn a_fill_reads_its_nearest_standing_demand() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x4F);
    let (fill, _l) = register(&reg, hash, hb(0x4F), total, ranges(0, total, total));

    let far = fill.demand_slot();
    far.stand(6 * G);
    let near = fill.demand_slot();
    near.stand(G);
    assert_eq!(fill.serve_demand().get(), G, "the nearer leg wins");

    near.withdraw();
    assert_eq!(fill.serve_demand().get(), 6 * G, "a moving leg withdraws");

    drop(far);
    assert_eq!(fill.serve_demand().get(), 0, "a dropped slot withdraws");
}

/// Serve demand never exceeds the blob: `stand` clamps `end` to the session's
/// total bytes, so a producer that reports a span past the end cannot place a
/// pull's demand beyond the content it can fetch. An `end` of `0` withdraws.
#[test]
fn demand_is_clamped_to_the_blob_size() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x4C);
    let (a, _al) = register(&reg, hash, hb(0x4C), total, ranges(0, total, total));
    let leg = a.demand_slot();
    leg.stand(total + 5 * G);
    assert_eq!(a.serve_demand().get(), total);
    leg.stand(0);
    assert_eq!(a.serve_demand().get(), 0);

    let standalone = FillSession::new(hb(0x4D), total);
    let leg = standalone.demand_slot();
    leg.stand(u64::MAX);
    assert_eq!(standalone.serve_demand().get(), total);
}

/// A served-frontier advance is forward-only and wakes a parked watch only when it
/// moves the frontier: a lower or an equal value wakes nothing.
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

#[test]
fn advance_served_is_forward_only() {
    use futures_util::FutureExt;

    let session = FillSession::starting_at(hb(0x4E), 8 * G, 2 * G);
    let watch = session.downstream_watch();
    assert_eq!(watch.served_paid(), 2 * G);

    let past = watch.past(2 * G, 0);
    futures_util::pin_mut!(past);
    assert!(past.as_mut().now_or_never().is_none(), "nothing moved yet");

    session.advance_served(G);
    assert_eq!(watch.served_paid(), 2 * G, "a lower value is ignored");
    assert!(
        past.as_mut().now_or_never().is_none(),
        "a lower value wakes nothing"
    );

    session.advance_served(2 * G);
    assert!(
        past.as_mut().now_or_never().is_none(),
        "an equal value wakes nothing"
    );

    session.advance_served(3 * G);
    assert_eq!(watch.served_paid(), 3 * G);
    assert!(
        past.now_or_never().is_some(),
        "an advance wakes the parked watch"
    );
}

/// A sibling's paid frontier extends only from a prefix that already reaches the
/// observer's start: below it the bytes are unpaid and the frontier stays put, at
/// it the frontier moves, and a frontier already past `served` never regresses.
#[test]
fn extend_served_from_requires_a_paid_prefix_to_the_offset() {
    let below = FillSession::starting_at(hb(0x4F), 8 * G, 0);
    below.extend_served_from(4 * G, 6 * G);
    assert_eq!(
        below.served_frontier().get(),
        0,
        "an unpaid gap blocks the raise"
    );

    let at = FillSession::starting_at(hb(0x50), 8 * G, 4 * G);
    at.extend_served_from(4 * G, 6 * G);
    assert_eq!(
        at.served_frontier().get(),
        6 * G,
        "a prefix at the offset extends"
    );

    let ahead = FillSession::starting_at(hb(0x51), 8 * G, 7 * G);
    ahead.extend_served_from(4 * G, 6 * G);
    assert_eq!(ahead.served_frontier().get(), 7 * G, "never regresses");
}

/// A watch wakes on either frontier: a standing demand with no payment resolves
/// a watch parked on the paid frontier, and so does its withdrawal.
#[test]
fn a_watch_wakes_on_a_demand_raise() {
    use futures_util::FutureExt;

    let session = FillSession::new(hb(0x52), 8 * G);
    let watch = session.downstream_watch();
    let past = watch.past(0, 0);
    futures_util::pin_mut!(past);
    assert!(past.as_mut().now_or_never().is_none());

    let leg = session.demand_slot();
    leg.stand(G);
    assert!(
        past.now_or_never().is_some(),
        "a standing demand wakes the watch"
    );

    let past = watch.past(0, G);
    futures_util::pin_mut!(past);
    assert!(past.as_mut().now_or_never().is_none());
    leg.withdraw();
    assert!(
        past.now_or_never().is_some(),
        "a withdrawn demand wakes the watch"
    );
}

/// Two sessions for one hash share the ONE per-hash outboard: a node captured via
/// the first session is loadable through a reader minted from the second. This is
/// the precondition for partial-overlap serving — a serve leg reads proof no
/// matter which pull captured it.
#[tokio::test]
async fn siblings_share_one_per_hash_outboard() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x40);

    let (a, _al) = register(&reg, hash, hb(0x40), total, ranges(0, 4 * G, total));
    let (b, _bl) = register(&reg, hash, hb(0x40), total, ranges(4 * G, 4 * G, total));

    // Capture an interior node through A; a reader minted from B must load it.
    let node = interior_node_in(total, 0, 8);
    let pair = (hb(1), hb(2));
    a.capture(node, pair);

    let mut reader_b = b.outboard_reader();
    assert_eq!(
        reader_b.load(node).await.unwrap(),
        Some(pair),
        "B's reader sees a node A captured — one shared per-hash outboard"
    );
}

/// A reader minted from a session that does NOT cover a node's range must FAIL
/// when the sibling that DOES cover it dies — even though the minting session is
/// still live. This is the N-fill termination the coherent encoder needs so a
/// partial-overlap serve fails (never hangs) if a coalesced sibling pull dies.
#[tokio::test]
async fn reader_fails_when_the_only_covering_sibling_dies() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x41);

    // Owner covers [0,4g); sibling covers [4g,8g).
    let (owner, _ol) = register(&reg, hash, hb(0x41), total, ranges(0, 4 * G, total));
    let (sibling, _sl) = register(&reg, hash, hb(0x41), total, ranges(4 * G, 4 * G, total));

    // A node wholly inside [4g,8g) — covered only by the sibling.
    let node = interior_node_in(total, 4, 8);
    let mut reader = owner.outboard_reader();

    let load = tokio::spawn(async move { reader.load(node).await });
    tokio::task::yield_now().await;
    assert!(
        !load.is_finished(),
        "load parks until the sibling supplies it"
    );

    // The sibling dies without capturing the node; the owner (which does not
    // cover it) is still live. The read must fail, not hang.
    sibling.mark_ended(Err(FillError::new("sibling upstream died")));
    let err = load
        .await
        .unwrap()
        .expect_err("no live fill covers the node — the read must fail");
    assert!(err.to_string().contains("no live fill covers"));
}

/// (a) Same range: a second serve-miss for the exact range an in-flight pull
/// covers attaches wholly and raises the count to 2 (`claim`).
#[test]
fn claim_same_range_attaches() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0xA1);

    let FillClaim::Owner {
        session,
        lease: _owner,
    } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0xA1), total))
    else {
        panic!("first whole-range claim owns");
    };
    let FillClaim::Attach {
        session: attached,
        lease: _l,
    } = reg.claim(hash, 0, 0, total, || {
        panic!("attach must not build a session")
    })
    else {
        panic!("second identical claim attaches");
    };
    assert!(Arc::ptr_eq(&attached, &session), "binds the same session");
    assert_eq!(session.observer_count(), 2, "owner + one attached observer");
}

/// (b) Disjoint halves: a claim for the second half of a pull that only covers
/// the first half OWNS its own pull; no coalescing.
#[test]
fn claim_disjoint_halves_both_own() {
    let total = 8 * G;
    let half = 4 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0xB2);

    let FillClaim::Owner {
        session: lo,
        lease: _lo,
    } = reg.claim(hash, 0, half, total, || FillSession::new(root(0xB2), total))
    else {
        panic!("first disjoint claim owns");
    };
    let FillClaim::Owner {
        session: hi,
        lease: _hi,
    } = reg.claim(hash, half, total - half, total, || {
        FillSession::new(root(0xB2), total)
    })
    else {
        panic!("disjoint second claim owns its own pull");
    };
    assert!(!Arc::ptr_eq(&lo, &hi), "distinct owner sessions");
    assert_eq!(lo.observer_count(), 1, "no cross-attach");
}

/// (c) Partial overlap (prefix): a sibling covers `[0,3g)`; a request for
/// `[2g,5g)` MIXES — owns a pull for the contiguous remainder `[3g,5g)` and
/// attaches the sibling for the `[2g,3g)` overlap.
#[test]
fn claim_partial_prefix_overlap_mixes() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0xC3);

    let (sibling, _sl) = register(&reg, hash, hb(0xC3), total, ranges(0, 3 * G, total));
    // The sibling's payer has paid up to the request's start, so the request
    // is at (not ahead of) its paid frontier and may share its overlap.
    sibling.advance_served(2 * G);

    let claim = reg.claim(hash, 2 * G, 3 * G, total, || {
        FillSession::starting_at(hb(0xC3), total, 2 * G)
    });
    let FillClaim::Mixed {
        owner,
        attach,
        remainder_offset,
        remainder_len,
        ..
    } = claim
    else {
        panic!("a prefix overlap with a contiguous remainder mixes");
    };
    assert!(
        Arc::ptr_eq(&attach, &sibling),
        "attaches the overlapping sibling"
    );
    assert_eq!(
        owner.covered_ranges(),
        ranges(3 * G, 2 * G, total),
        "owner covers the remainder [3g,5g)"
    );
    assert_eq!((remainder_offset, remainder_len), (3 * G, 2 * G));
    assert_eq!(sibling.observer_count(), 2, "sibling owner + our attach");
    assert_eq!(owner.observer_count(), 1, "our own remainder pull");
}

/// An interior overlap splits the remainder into two disjoint pieces, which one
/// `[offset, len)` pull cannot express — so `claim` conservatively OWNS the whole
/// request rather than double-pull or wedge.
#[test]
fn claim_interior_overlap_falls_back_to_whole_owner() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0xC4);

    // Sibling covers a MIDDLE slice [3g,4g).
    let (sibling, _sl) = register(&reg, hash, hb(0xC4), total, ranges(3 * G, G, total));

    // Request [0,8g): overlap [3g,4g), remainder [0,3g) ∪ [4g,8g) — two pieces.
    let FillClaim::Owner { session, lease: _l } =
        reg.claim(hash, 0, 0, total, || FillSession::new(hb(0xC4), total))
    else {
        panic!("a split remainder falls back to a whole-request owner");
    };
    assert_eq!(
        session.covered_ranges(),
        ranges(0, 0, total),
        "the fallback owner covers the whole request"
    );
    assert_eq!(
        sibling.observer_count(),
        1,
        "no attach on the fallback path"
    );
}

/// A subset claim (its `R` fully inside a live pull's `covered`) ATTACHES.
#[test]
fn claim_subset_attaches() {
    let total = 8 * G;
    let half = 4 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x32);

    let FillClaim::Owner {
        session: owner,
        lease: _o,
    } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0x32), total))
    else {
        panic!("first whole-range claim owns");
    };
    let FillClaim::Attach {
        session: attached,
        lease: _l,
    } = reg.claim(hash, 0, half, total, || panic!("subset must attach"))
    else {
        panic!("a subset of the covered range attaches");
    };
    assert!(Arc::ptr_eq(&attached, &owner), "attaches to the owner");
    assert_eq!(owner.observer_count(), 2, "owner + attached");
}

/// `make_session` MUST NOT run on the attach branch — a coalescing serve leg
/// allocates no outboard buffer.
#[test]
fn claim_make_session_not_called_on_attach() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x34);

    let owner = reg.claim(hash, 0, 0, total, || FillSession::new(root(0x34), total));
    assert!(matches!(owner, FillClaim::Owner { .. }), "first claim owns");
    let attach = reg.claim(hash, 0, 0, total, || {
        panic!("make_session must not be called on the attach branch")
    });
    assert!(
        matches!(attach, FillClaim::Attach { .. }),
        "second attaches"
    );
}

/// (d) Lease teardown: the last observer leaving before the pull ends cancels
/// the pull and removes the session from the map; an earlier leaver does not.
#[test]
fn last_observer_leaving_cancels_and_removes() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0xD4);

    let FillClaim::Owner {
        session,
        lease: owner,
    } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0xD4), total))
    else {
        panic!("owns");
    };
    assert_eq!(session.observer_count(), 1, "owner is one observer");
    assert!(mapped(&reg, hash), "registered session is mapped");

    let FillClaim::Attach {
        session: _a,
        lease: second,
    } = reg.claim(hash, 0, 0, total, || panic!("attach"))
    else {
        panic!("attaches");
    };
    assert_eq!(session.observer_count(), 2);

    drop(second);
    assert!(!session.is_cancelled(), "owner still waits — no cancel");
    assert_eq!(session.observer_count(), 1);
    assert!(mapped(&reg, hash), "non-last leaver keeps it mapped");

    drop(owner);
    assert!(
        session.is_cancelled(),
        "last observer left — pull cancelled"
    );
    assert_eq!(session.observer_count(), 0);
    assert!(!mapped(&reg, hash), "last observer left — session removed");
}

#[test]
fn claim_ahead_of_a_live_owners_paid_frontier_owns_its_own_span() {
    // A whole-blob owner whose client has paid nothing (a header-only open,
    // such as the CLI's multi-source size probe) has its paid frontier at 0.
    // A request that starts at 4 groups is AHEAD of that frontier: attaching would park it on a pull that only
    // advances as the owner pays, and the owner never will. It must OWN its
    // own span instead. Both sessions stay live under the hash.
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0xDB);

    let FillClaim::Owner {
        session: whole,
        lease: _whole_lease,
    } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0xDB), total))
    else {
        panic!("owns");
    };
    let FillClaim::Owner {
        session: tail,
        lease: _tail_lease,
    } = reg.claim(hash, 4 * G, 0, total, || {
        FillSession::starting_at(root(0xDB), total, 4 * G)
    })
    else {
        panic!("a request ahead of the owner's paid frontier owns its own span");
    };
    assert!(!Arc::ptr_eq(&whole, &tail));
    assert_eq!(whole.observer_count(), 1, "the tail did not attach");
    assert_eq!(tail.observer_count(), 1);
    assert!(mapped(&reg, hash));

    // Once the owner's client has paid past the tail's start, a request at
    // that start attaches again — its payments can extend the owner's paid
    // prefix at once.
    whole.advance_served(5 * G);
    let FillClaim::Attach {
        session: attached,
        lease: _a,
    } = reg.claim(hash, 5 * G, 0, total, || panic!("attaches"))
    else {
        panic!("a request at or behind the paid frontier attaches");
    };
    assert!(
        Arc::ptr_eq(&attached, &whole) || Arc::ptr_eq(&attached, &tail),
        "attaches to a live session whose paid frontier reaches it"
    );
}

/// `fill_not_coalesced` counts a claim that skips a live session it overlaps,
/// once per claim however many it skips, and never a skip of a session whose
/// covered range the request does not touch.
#[test]
fn fill_not_coalesced_counts_overlapping_skips_once_per_claim() {
    let total = 8 * G;
    let metrics = Arc::new(crate::metrics::CacheMetrics::default());
    let reg = Arc::new(FillRegistry::with_metrics(Some(Arc::clone(&metrics))));

    // Disjoint: a head fill [0, 2G) at paid frontier 0, and a tail claim at 4G
    // ahead of it. The skip duplicates nothing.
    let head_hash = store_hash(0xE1);
    let FillClaim::Owner { lease: _head, .. } = reg.claim(head_hash, 0, 2 * G, total, || {
        FillSession::new(root(0xE1), total)
    }) else {
        panic!("owns");
    };
    let FillClaim::Owner { lease: _tail, .. } = reg.claim(head_hash, 4 * G, 0, total, || {
        FillSession::starting_at(root(0xE1), total, 4 * G)
    }) else {
        panic!("a disjoint claim owns");
    };
    assert_eq!(metrics.fill_not_coalesced.get(), 0, "a disjoint skip");

    // Overlapping: a whole-blob fill at paid frontier 0.
    let hash = store_hash(0xE2);
    let FillClaim::Owner { lease: _whole, .. } =
        reg.claim(hash, 0, 0, total, || FillSession::new(root(0xE2), total))
    else {
        panic!("owns");
    };
    // A tail claim at 4G skips the whole fill it overlaps.
    let FillClaim::Owner { lease: _tail, .. } = reg.claim(hash, 4 * G, 0, total, || {
        FillSession::starting_at(root(0xE2), total, 4 * G)
    }) else {
        panic!("a claim ahead of the paid frontier owns");
    };
    assert_eq!(metrics.fill_not_coalesced.get(), 1);
    // A claim at 6G skips both live fills; it counts once.
    let FillClaim::Owner { lease: _late, .. } = reg.claim(hash, 6 * G, 0, total, || {
        FillSession::starting_at(root(0xE2), total, 6 * G)
    }) else {
        panic!("a claim ahead of both paid frontiers owns");
    };
    assert_eq!(metrics.fill_not_coalesced.get(), 2, "one claim, one count");
}

#[test]
fn claim_after_last_out_release_owns_a_fresh_session() {
    // A header-only open: claim, then the ONLY observer leaves. The
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
    assert!(
        first.is_cancelled(),
        "last-out release cancels the throwaway's fill"
    );
    assert!(!mapped(&reg, hash), "the cancelled session is unmapped");

    let FillClaim::Owner {
        session: second,
        lease: _second_lease,
    } = reg.claim(hash, 0, total, total, || {
        FillSession::new(root(0xD9), total)
    })
    else {
        panic!("the real open must own, not attach");
    };
    assert!(
        !Arc::ptr_eq(&first, &second),
        "a fresh session, not the cancelled one"
    );
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
    assert!(
        !session.is_cancelled(),
        "the real observer keeps the fill alive"
    );
    assert!(mapped(&reg, hash));
    assert_eq!(session.observer_count(), 1);

    assert!(real.release().is_none());
    assert!(
        session.is_cancelled(),
        "the real observer's exit is last-out"
    );
    assert!(!mapped(&reg, hash));
}

/// A sibling session for the same hash survives when one session's last observer
/// leaves: removal is by pointer identity, and the hash key (and its shared
/// outboard) stay while any session remains under it.
#[test]
fn removing_one_session_keeps_a_sibling_for_the_same_hash() {
    let total = 8 * G;
    let half = 4 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x29);

    let (first, first_owner) = register(&reg, hash, hb(0x29), total, ranges(0, half, total));
    let (second, _second_owner) = register(
        &reg,
        hash,
        hb(0x29),
        total,
        ranges(half, total - half, total),
    );

    drop(first_owner);
    assert!(mapped(&reg, hash), "sibling keeps the hash key alive");
    assert!(first.is_cancelled(), "the emptied session cancelled");

    // A serve-miss for the second half still attaches to the surviving sibling.
    let FillClaim::Attach {
        session: attached,
        lease: _l,
    } = reg.claim(hash, half, total - half, total, || panic!("attach"))
    else {
        panic!("attaches to the surviving sibling");
    };
    assert!(Arc::ptr_eq(&attached, &second), "the sibling is bound");
}

/// A cancelled session is dead: `claim` must NOT attach to it, and its `covered`
/// must NOT suppress a fresh pull.
#[test]
fn claim_skips_cancelled_session() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0xE5);

    let FillClaim::Owner {
        session,
        lease: owner,
    } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0xE5), total))
    else {
        panic!("owns");
    };
    drop(owner);
    assert!(session.is_cancelled(), "cancel fired on last-out drop");

    let FillClaim::Owner {
        session: fresh,
        lease: _l,
    } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0xE5), total))
    else {
        panic!("a dead session must not block a fresh owner");
    };
    assert!(!Arc::ptr_eq(&fresh, &session), "a genuinely fresh session");
}

/// `range_still_live` is false once every covering session is dead, and true
/// while any live session still covers the range.
#[test]
fn range_still_live_tracks_covering_sessions() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x50);
    let probe = ranges(4 * G, 2 * G, total); // [4g,6g)

    let (a, _al) = register(&reg, hash, hb(0x50), total, ranges(0, 6 * G, total));
    let (b, _bl) = register(&reg, hash, hb(0x50), total, ranges(4 * G, 4 * G, total));
    assert!(reg.range_still_live(hash, &probe), "two live coverers");

    a.mark_ended(Ok(()));
    assert!(reg.range_still_live(hash, &probe), "b still covers [4g,6g)");
    b.mark_ended(Ok(()));
    assert!(!reg.range_still_live(hash, &probe), "no live coverer left");
}

/// `FillSession::total_bytes` reports the blob length the session was built with,
/// so the attach path can sign its `StreamResponse` without a header handshake.
#[test]
fn total_bytes_reports_blob_length() {
    let total = 5 * G + 321;
    let session = FillSession::new(root(0x35), total);
    assert_eq!(session.total_bytes(), total);
}

/// The pull-thread handle is parked on the session and handed back to whichever
/// observer leaves LAST. An owner whose own client finishes first (a non-last-out
/// release) gets `None`, so its accept task returns at once while the pull keeps
/// filling; the remaining observer, on its last-out release, takes the handle to
/// join. This is the owner-join hand-off (#1664): the finished owner is no longer
/// parked in the join while another observer streams.
#[test]
fn last_out_release_takes_the_pull_handle() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x40);

    let FillClaim::Owner {
        session,
        lease: owner,
    } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0x40), total))
    else {
        panic!("owns");
    };
    // Owner parks the pull-thread handle on the shared session after spawning it.
    session.set_pull_handle(std::thread::spawn(|| {}));

    // A second observer attaches.
    let FillClaim::Attach {
        session: _a,
        lease: observer,
    } = reg.claim(hash, 0, 0, total, || panic!("attach"))
    else {
        panic!("attaches");
    };
    assert_eq!(session.observer_count(), 2, "owner + attached");

    // Owner leaves FIRST (count 2 → 1, not last-out): no handle handed back, so its
    // accept task is freed immediately; the pull is NOT cancelled.
    assert!(
        owner.release().is_none(),
        "a non-last-out release must not take the pull handle"
    );
    assert_eq!(session.observer_count(), 1);
    assert!(
        !session.is_cancelled(),
        "an observer still streams — no cancel"
    );

    // The last observer leaving (count 1 → 0) takes the handle to join off-task and
    // cancels the pull.
    let handle = observer
        .release()
        .expect("the last-out release must hand back the pull handle");
    handle.join().expect("the parked pull thread joins");
    assert_eq!(session.observer_count(), 0);
    assert!(
        session.is_cancelled(),
        "last observer left — pull cancelled"
    );
}

/// The spawn-failure path parks no handle, so a last-out release must still be
/// safe: it runs the decrement + cancel and simply returns `None` (nothing to
/// join). Guards the owner error arm that releases its lease without ever storing
/// a pull thread.
#[test]
fn last_out_release_without_a_parked_handle_returns_none() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x41);

    let FillClaim::Owner {
        session,
        lease: owner,
    } = reg.claim(hash, 0, 0, total, || FillSession::new(root(0x41), total))
    else {
        panic!("owns");
    };
    assert!(
        owner.release().is_none(),
        "no handle parked → nothing to join"
    );
    assert!(
        session.is_cancelled(),
        "sole observer left — pull cancelled"
    );
}

/// `in_flight_total` peeks a LIVE fill's blob length so the serve-miss path can
/// skip its upstream header handshake when it will coalesce onto that pull:
/// `None` on an empty registry, `Some(total)` while a live pull runs, `None`
/// once that pull's last observer leaves (the session is removed).
#[test]
fn in_flight_total_peeks_live_fill_length() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x40);

    assert_eq!(
        reg.in_flight_total(hash),
        None,
        "an empty registry has no in-flight fill"
    );

    let session = FillSession::new(root(0x40), total);
    session.set_covered(ranges(0, 0, total));
    let owner = reg.register_fill(hash, &session);
    assert_eq!(
        reg.in_flight_total(hash),
        Some(total),
        "a live fill reports its blob length"
    );

    // Last observer leaves -> the session is removed from the map -> nothing in
    // flight, so a fresh serve-miss must handshake.
    drop(owner);
    assert_eq!(
        reg.in_flight_total(hash),
        None,
        "a removed fill is not in flight"
    );
}

/// An ENDED session is dead: `in_flight_total` skips it even while it is still
/// mapped, because there is nothing live to coalesce onto -- the caller must
/// handshake.
#[test]
fn in_flight_total_skips_ended_session() {
    let total = 8 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x41);

    let session = FillSession::new(root(0x41), total);
    session.set_covered(ranges(0, 0, total));
    let _owner = reg.register_fill(hash, &session);
    assert_eq!(reg.in_flight_total(hash), Some(total));

    session.mark_ended(Err(FillError::new("upstream died")));
    assert_eq!(
        reg.in_flight_total(hash),
        None,
        "an ended session is not attachable in flight"
    );
}

/// With a dead session and a LIVE sibling for the same hash, `in_flight_total`
/// reports the live one's length -- the dead session is skipped, not the whole
/// hash.
#[test]
fn in_flight_total_reports_live_sibling_past_a_dead_one() {
    let total = 8 * G;
    let half = 4 * G;
    let reg = Arc::new(FillRegistry::new());
    let hash = store_hash(0x42);

    let dead = FillSession::new(root(0x42), total);
    dead.set_covered(ranges(0, half, total));
    let _dead_owner = reg.register_fill(hash, &dead);
    dead.mark_ended(Err(FillError::new("first pull died")));

    let live = FillSession::new(root(0x42), total);
    live.set_covered(ranges(half, total - half, total));
    let _live_owner = reg.register_fill(hash, &live);

    assert_eq!(
        reg.in_flight_total(hash),
        Some(total),
        "a live sibling is reported past the dead session"
    );
}
