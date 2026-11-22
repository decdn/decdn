use super::*;

fn op(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

fn ns(n: u64) -> U256 {
    U256::from(n)
}

#[test]
fn an_empty_set_seats_nobody() {
    let set = SeatedOriginSet::new();
    assert!(!set.is_seated_in(ns(1), &op(1)));
    assert_eq!(set.first_namespace(&op(1)), None);
    assert!(set.is_empty());
}

#[test]
fn a_seat_counts_in_its_own_namespace_only() {
    let set = SeatedOriginSet::from_seats([(ns(1), op(1))]);
    assert!(set.is_seated_in(ns(1), &op(1)));
    assert!(!set.is_seated_in(ns(2), &op(1)));
}

#[test]
fn unseating_one_namespace_keeps_the_others() {
    let set = SeatedOriginSet::new();
    set.seat(ns(1), op(1));
    set.seat(ns(2), op(1));
    set.unseat(ns(1), op(1));
    assert!(!set.is_seated_in(ns(1), &op(1)));
    assert!(
        set.is_seated_in(ns(2), &op(1)),
        "still seated in namespace 2"
    );
    set.unseat(ns(2), op(1));
    assert!(!set.is_seated_in(ns(2), &op(1)));
    assert_eq!(set.len(), 0);
}

#[test]
fn first_namespace_is_the_lowest_seat() {
    let set = SeatedOriginSet::from_seats([(ns(7), op(1)), (ns(3), op(1)), (ns(5), op(1))]);
    assert_eq!(set.first_namespace(&op(1)), Some(ns(3)));
    set.unseat(ns(3), op(1));
    assert_eq!(set.first_namespace(&op(1)), Some(ns(5)));
}

#[test]
fn unseating_an_unknown_seat_changes_nothing() {
    let set = SeatedOriginSet::from_seats([(ns(1), op(1))]);
    set.unseat(ns(9), op(1));
    set.unseat(ns(1), op(2));
    assert!(set.is_seated_in(ns(1), &op(1)));
}

#[test]
fn replace_drops_seats_the_enumeration_omits() {
    let set = SeatedOriginSet::from_seats([(ns(1), op(1)), (ns(1), op(2))]);
    set.replace([(ns(3), op(2))]);
    assert!(!set.is_seated_in(ns(1), &op(1)));
    assert!(!set.is_seated_in(ns(1), &op(2)));
    assert!(set.is_seated_in(ns(3), &op(2)));
    assert_eq!(set.len(), 1);
}
