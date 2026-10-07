use super::*;

/// A fresh handle has never been stamped, so it reads stale — a node that has
/// not yet confirmed its deny-set must not serve.
#[test]
fn unstamped_reads_stale() {
    let f = ChainFreshness::new(Duration::from_mins(30));
    assert!(f.is_stale(), "a never-stamped handle is stale");
}

/// A just-stamped handle is fresh, and a clone shares the same cell — the
/// watcher's stamp is visible to the handler's read.
#[test]
fn stamped_reads_fresh_across_clones() {
    let f = ChainFreshness::new(Duration::from_mins(30));
    let reader = f.clone();
    f.stamp();
    assert!(
        !reader.is_stale(),
        "a clone sees the stamp through the shared cell"
    );
}

/// A stamp older than the grace window reads stale; the same stamp under a
/// larger window reads fresh. This is the relativity a large window relies on
/// to opt out — set it well past any real outage and no gap ever trips it.
#[test]
fn staleness_is_relative_to_the_grace_window() {
    let ten_min_ago = live_secs().saturating_sub(600);

    let tight = ChainFreshness::new(Duration::from_mins(1));
    tight.last_ok_secs.store(ten_min_ago, Ordering::Relaxed);
    assert!(tight.is_stale(), "a 10 min gap exceeds a 1 min grace");

    let generous = ChainFreshness::new(Duration::from_mins(30));
    generous.last_ok_secs.store(ten_min_ago, Ordering::Relaxed);
    assert!(
        !generous.is_stale(),
        "the same 10 min gap is fresh under a 30 min window"
    );
}
