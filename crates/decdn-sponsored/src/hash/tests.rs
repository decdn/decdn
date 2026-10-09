use super::*;

const HEX: &str = "9194000d7b356650e6924a7746ec4afb0b705838b65913c0e46cba6b15af69e5";

#[test]
fn normalize_accepts_website_forms() {
    assert_eq!(normalize_hash(&format!("b3:{HEX}")).unwrap(), HEX);
    assert_eq!(normalize_hash(&format!("0x{HEX}")).unwrap(), HEX);
    assert_eq!(normalize_hash(&HEX.to_uppercase()).unwrap(), HEX);
}

#[test]
fn normalize_rejects_non_hashes() {
    assert!(normalize_hash("mistral-7b").is_err());
    assert!(normalize_hash(&format!("b3:{}", &HEX[..63])).is_err());
    assert!(normalize_hash(&format!("b3:{}g", &HEX[..63])).is_err());
}
