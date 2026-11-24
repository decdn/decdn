use super::*;

#[test]
fn is_a_bare_integer_on_the_wire() {
    assert_eq!(serde_json::to_string(&MicroUsdc(5)).unwrap(), "5");
    assert_eq!(
        serde_json::from_str::<MicroUsdc>("7").unwrap(),
        MicroUsdc(7)
    );
}
