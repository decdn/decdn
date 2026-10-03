//! BLAKE3 bundle hashes as the website prints them.

/// Normalize a BLAKE3 hash as the website prints it (`b3:<hex>`, `0x<hex>`,
/// or bare hex) to 64 lowercase hex characters.
///
/// # Errors
///
/// Returns an error unless the remainder is exactly 64 hex characters.
pub fn normalize_hash(raw: &str) -> anyhow::Result<String> {
    let trimmed = raw.trim();
    let hex = trimmed
        .strip_prefix("b3:")
        .or_else(|| trimmed.strip_prefix("0x"))
        .unwrap_or(trimmed)
        .to_ascii_lowercase();
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        anyhow::bail!(
            "not a BLAKE3 hash (expected 64 hex characters, optionally b3:-prefixed): {raw}"
        );
    }
    Ok(hex)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
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
}
