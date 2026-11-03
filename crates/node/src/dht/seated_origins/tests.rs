use super::*;

fn op(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

#[test]
fn an_empty_set_seats_nobody() {
    let set = SeatedOriginSet::new();
    assert!(!set.is_seated(&op(1)));
    assert!(set.is_empty());
}

#[test]
fn an_operator_stays_seated_until_its_last_seat_goes() {
    let set = SeatedOriginSet::new();
    set.seat(U256::from(1), op(1));
    set.seat(U256::from(2), op(1));
    set.unseat(U256::from(1), op(1));
    assert!(set.is_seated(&op(1)), "still seated in namespace 2");
    set.unseat(U256::from(2), op(1));
    assert!(!set.is_seated(&op(1)));
    assert_eq!(set.len(), 0);
}

#[test]
fn unseating_an_unknown_seat_changes_nothing() {
    let set = SeatedOriginSet::from_seats([(U256::from(1), op(1))]);
    set.unseat(U256::from(9), op(1));
    set.unseat(U256::from(1), op(2));
    assert!(set.is_seated(&op(1)));
}

#[test]
fn replace_drops_seats_the_enumeration_omits() {
    let set = SeatedOriginSet::from_seats([(U256::from(1), op(1)), (U256::from(1), op(2))]);
    set.replace([(U256::from(3), op(2))]);
    assert!(!set.is_seated(&op(1)));
    assert!(set.is_seated(&op(2)));
    assert_eq!(set.len(), 1);
}
