use super::*;
use alloy::primitives::Address;

/// A lane's widen hooks grant only the provider's permits that are free
/// now, beside the lane's own lease, and give each one back. At a cap of
/// 3 or more, extra streams leave the last free permit for a sibling
/// entry's first stream; a restart takes it (#2252).
#[tokio::test]
async fn a_lane_widens_only_into_free_permits_and_gives_them_back() {
    use crate::GrowFor::{Extra, Restart};
    let p1 = Address::repeat_byte(1);
    let cap = LaneStreamCap::new(3);
    let lease = cap.try_permit(p1).await.expect("the lane's own permit");
    let extras = cap.extra_permits(p1).await;
    assert!(extras.grant(Extra), "one extra stream");
    assert!(!extras.grant(Extra), "the last free permit is kept back");
    assert!(extras.grant(Restart), "a restart takes the last permit");
    assert!(!extras.grant(Restart), "the cap is full");
    extras.release_one();
    assert!(extras.grant(Restart), "a released permit is free again");
    extras.release_one();
    extras.release_one();
    drop(lease);
    let all: Vec<_> = futures_util::future::join_all((0..3).map(|_| cap.try_permit(p1)))
        .await
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(all.len(), 3, "every permit came back");
}

/// At a cap of 2 the lease and one extra stream fill the cap, so an extra
/// stream takes the last free permit: keeping it back would leave a lane
/// no extra stream at all.
#[tokio::test]
async fn a_cap_of_two_still_grants_an_extra_stream() {
    let p1 = Address::repeat_byte(1);
    let cap = LaneStreamCap::new(2);
    let _lease = cap.try_permit(p1).await.expect("the lane's own permit");
    let extras = cap.extra_permits(p1).await;
    assert!(extras.grant(crate::GrowFor::Extra), "the extra stream");
    assert!(cap.try_permit(p1).await.is_none(), "the cap is full");
    extras.release_one();
}

/// A lane that `grow` refused claims the provider's next free permit. A
/// sibling lane that still runs an extra stream cannot take it, by
/// restart or by extra stream, before the claimant's next ask (#2341).
#[tokio::test]
async fn a_lane_running_a_stream_cannot_restart_into_a_claimed_permit() {
    use crate::GrowFor::{Extra, Restart};
    let p1 = Address::repeat_byte(1);
    let cap = LaneStreamCap::new(4);
    let _b_lease = cap.try_permit(p1).await.expect("lane B's own permit");
    let a_lease = cap.try_permit(p1).await.expect("lane A's own permit");
    let a = cap.extra_permits(p1).await;
    let b = cap.extra_permits(p1).await;
    assert!(a.grant(Extra), "lane A's extra stream");
    let c_lease = cap.try_permit(p1).await.expect("lane C's own permit");
    assert!(!b.grant(Extra), "the cap is full: lane B claims");
    drop(a_lease);
    assert!(
        !a.grant(Restart),
        "lane A, still on its extra stream, waits behind lane B's claim"
    );
    assert!(!a.grant(Extra), "an extra stream waits too");
    drop(c_lease);
    assert!(
        !a.grant(Restart),
        "lane B's claim and the kept permit hold both free permits"
    );
    assert!(b.grant(Extra), "lane B takes the permit it claimed");
    assert!(
        a.grant(Restart),
        "with lane B served, lane A's own claim comes first"
    );
    a.release_one();
    a.release_one();
    b.release_one();
}

/// A lane that runs no stream starts again ahead of a claim for one more
/// stream: the claimant runs one, and the free permit cannot serve it
/// yet.
#[tokio::test]
async fn a_lane_that_runs_no_stream_goes_ahead_of_a_claim_for_one_more() {
    use crate::GrowFor::{Extra, Restart};
    let p1 = Address::repeat_byte(1);
    let cap = LaneStreamCap::new(4);
    let _b_lease = cap.try_permit(p1).await.expect("lane B's own permit");
    let b = cap.extra_permits(p1).await;
    assert!(b.grant(Extra), "lane B's first extra stream");
    assert!(b.grant(Extra), "lane B's second extra stream");
    assert!(
        !b.grant(Extra),
        "the last free permit is kept: lane B claims"
    );
    let a = cap.extra_permits(p1).await;
    assert!(
        a.grant(Restart),
        "lane A, which gave its lease back, starts again on the free permit"
    );
    a.release_one();
    b.release_one();
    b.release_one();
}

/// A lane's claim ends with the lane, and the other claims stay.
#[tokio::test]
async fn an_ended_lane_claims_nothing() {
    use crate::GrowFor::{Extra, Restart};
    let p1 = Address::repeat_byte(1);
    let cap = LaneStreamCap::new(5);
    let a_lease = cap.try_permit(p1).await.expect("lane A's own permit");
    let _b_lease = cap.try_permit(p1).await.expect("lane B's own permit");
    let _c_lease = cap.try_permit(p1).await.expect("lane C's own permit");
    let a = cap.extra_permits(p1).await;
    let b = cap.extra_permits(p1).await;
    let c = cap.extra_permits(p1).await;
    assert!(a.grant(Extra), "lane A's extra stream");
    assert!(!b.grant(Extra), "lane B claims");
    assert!(!c.grant(Extra), "lane C claims");
    drop(a_lease);
    assert!(
        !a.grant(Restart),
        "two claims and the kept permit hold three"
    );
    drop(b);
    assert!(
        !a.grant(Restart),
        "lane B ended, and lane C's claim still holds a permit"
    );
    drop(c);
    assert!(a.grant(Restart), "no claim is left ahead of lane A");
    a.release_one();
    a.release_one();
}

/// `n` permits of `streams`, held.
fn fill(streams: &ProviderStreams, n: usize) -> Vec<tokio::sync::OwnedSemaphorePermit> {
    (0..n)
        .map(|_| {
            Arc::clone(&streams.semaphore)
                .try_acquire_owned()
                .expect("a free permit")
        })
        .collect()
}

/// Each claim ahead of an ask holds back one free permit, and the
/// claims are served in the order they were made.
#[test]
fn each_claim_ahead_holds_back_one_permit() {
    let (b, c, d) = (1, 2, 3);
    let streams = ProviderStreams::new(4);
    let mut held = fill(&streams, 4);
    assert!(streams.take(b, 0, false).is_none(), "lane B claims first");
    assert!(streams.take(c, 0, false).is_none(), "lane C claims second");
    held.truncate(2);
    assert!(
        streams.take(d, 0, false).is_none(),
        "two claims ahead hold both free permits"
    );
    let mut reheld = fill(&streams, 1);
    assert!(
        streams.take(c, 0, false).is_none(),
        "lane C waits behind lane B"
    );
    assert!(
        streams.take(b, 0, false).is_some(),
        "lane B is served first"
    );
    reheld.clear();
    assert!(streams.take(c, 0, false).is_some(), "lane C is served next");
}

/// The claims ahead hold back their permits and the largest `keep`
/// among them once, not each claim's `keep`: a later ask takes a permit
/// the claims ahead do not need.
#[test]
fn a_later_ask_takes_only_what_the_claims_ahead_leave() {
    let (b, c, d) = (1, 2, 3);
    let streams = ProviderStreams::new(5);
    let mut held = fill(&streams, 5);
    assert!(streams.take(b, 1, false).is_none(), "lane B claims");
    assert!(streams.take(c, 1, false).is_none(), "lane C claims");
    held.truncate(1);
    assert!(
        streams.take(d, 0, false).is_some(),
        "two claims and one kept permit leave a fourth free permit"
    );
    assert!(
        streams.take(b, 1, false).is_some(),
        "lane B is still served"
    );
    assert!(streams.take(c, 1, false).is_some(), "and lane C after it");
}

/// A lane that asks again keeps its place in the claims.
#[test]
fn a_renewed_claim_keeps_its_place() {
    let (b, c) = (1, 2);
    let streams = ProviderStreams::new(2);
    let mut held = fill(&streams, 2);
    assert!(streams.take(b, 0, false).is_none(), "lane B claims first");
    assert!(streams.take(c, 0, false).is_none(), "lane C claims second");
    assert!(streams.take(b, 0, false).is_none(), "lane B asks again");
    held.truncate(1);
    assert!(
        streams.take(c, 0, false).is_none(),
        "lane C still waits behind lane B"
    );
    assert!(
        streams.take(b, 0, false).is_some(),
        "lane B is served first"
    );
}

/// A renewed claim holds back the `keep` of the lane's last ask.
#[test]
fn a_renewed_claim_takes_its_last_asks_keep() {
    let (b, d) = (1, 2);
    for (first, last, granted) in [(1, 0, true), (0, 1, false)] {
        let streams = ProviderStreams::new(3);
        let mut held = fill(&streams, 3);
        assert!(streams.take(b, first, false).is_none(), "lane B claims");
        assert!(streams.take(b, last, false).is_none(), "lane B asks again");
        held.truncate(1);
        assert_eq!(
            streams.take(d, 0, false).is_some(),
            granted,
            "lane B's claim keeps {last} after its own permit"
        );
    }
}

/// An ask from a lane that runs no stream waits only behind the claims
/// of such lanes, in their order.
#[test]
fn a_bare_ask_waits_only_behind_bare_claims() {
    let (b, x, y) = (1, 2, 3);
    let streams = ProviderStreams::new(3);
    let mut held = fill(&streams, 3);
    assert!(
        streams.take(b, 1, false).is_none(),
        "lane B claims one more"
    );
    assert!(
        streams.take(x, 0, true).is_none(),
        "lane X, with none, claims"
    );
    held.truncate(2);
    assert!(
        streams.take(y, 0, true).is_none(),
        "lane Y, with none, waits behind lane X"
    );
    assert!(
        streams.take(x, 0, true).is_some(),
        "lane X goes ahead of lane B's claim"
    );
}

/// A lane's own claim never holds a permit back from the lane itself.
#[test]
fn a_lane_is_not_held_back_by_its_own_claim() {
    let a = 1;
    let streams = ProviderStreams::new(2);
    let _held = fill(&streams, 1);
    assert!(
        streams.take(a, 1, false).is_none(),
        "the kept permit is not an extra stream's"
    );
    assert!(
        streams.take(a, 0, false).is_some(),
        "lane A's restart takes it past its own claim"
    );
}

/// A claim that is not renewed lapses after [`CLAIM_TTL`]; each ask
/// renews it.
#[tokio::test(start_paused = true)]
async fn a_claim_lapses_when_its_lane_stops_asking() {
    let (a, b) = (1, 2);
    let streams = ProviderStreams::new(2);
    let mut held = fill(&streams, 2);
    assert!(streams.take(b, 1, false).is_none(), "lane B claims");
    held.truncate(1);
    assert!(
        streams.take(a, 0, false).is_none(),
        "lane B's claim holds the free permit"
    );
    tokio::time::advance(CLAIM_TTL / 2).await;
    assert!(
        streams.take(b, 1, false).is_none(),
        "lane B asks again and renews its claim"
    );
    tokio::time::advance(CLAIM_TTL / 2 + std::time::Duration::from_millis(1)).await;
    assert!(
        streams.take(a, 0, false).is_none(),
        "a renewed claim holds past the first ask's lapse"
    );
    tokio::time::advance(CLAIM_TTL).await;
    assert!(
        streams.take(a, 0, false).is_some(),
        "a claim not renewed lapses"
    );
}

#[tokio::test]
async fn lane_stream_cap_serializes_one_provider_and_frees_the_rest() {
    let p1 = Address::repeat_byte(1);
    let p2 = Address::repeat_byte(2);

    // n == 1: while the first permit for P1 is held, no second one is free.
    let cap = LaneStreamCap::new(1);
    let held = cap.try_permit(p1).await.unwrap();
    assert!(
        cap.try_permit(p1).await.is_none(),
        "a second same-provider permit is taken while the first is held"
    );
    // A different provider never contends.
    assert!(
        cap.try_permit(p2).await.is_some(),
        "a distinct provider does not share P1's permit"
    );
    // Dropping the first frees it.
    drop(held);
    assert!(
        cap.try_permit(p1).await.is_some(),
        "the permit is free once the first drops"
    );

    // n == 2: two permits for the same provider coexist.
    let cap2 = LaneStreamCap::new(2);
    let a = cap2.try_permit(p1).await.unwrap();
    let b = cap2
        .try_permit(p1)
        .await
        .expect("two same-provider permits coexist at n == 2");
    drop((a, b));
}
