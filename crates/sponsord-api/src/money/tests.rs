use super::*;

#[test]
fn arithmetic_saturates() {
    assert_eq!(
        MicroUsdc(u64::MAX).saturating_add(MicroUsdc(1)),
        MicroUsdc(u64::MAX)
    );
    assert_eq!(MicroUsdc(1).saturating_sub(MicroUsdc(2)), MicroUsdc::ZERO);
}

#[test]
fn is_a_bare_integer_on_the_wire() {
    assert_eq!(serde_json::to_string(&MicroUsdc(5)).unwrap(), "5");
    assert_eq!(
        serde_json::from_str::<MicroUsdc>("7").unwrap(),
        MicroUsdc(7)
    );
}
