//! Keystore address lookup for the sponsor CLI wrapper.
//!
//! `read_address` reads the plaintext `address` field straight out of the
//! v3 keystore JSON — no password needed, since the address isn't secret.

use std::path::Path;
use std::str::FromStr;

use alloy::primitives::Address;

/// Read the plaintext `address` field out of a v3 keystore JSON file.
///
/// No password is required: the address is not secret (it's derived from
/// the public key and stored in the clear in every standard Web3 Secret
/// Storage v3 keystore).
///
/// # Errors
///
/// Returns an error if the file cannot be read, is not valid JSON, has no
/// `address` field, or the field's value doesn't parse as an `Address`.
pub fn read_address(keystore_path: &Path) -> anyhow::Result<Address> {
    let bytes = std::fs::read(keystore_path)?;
    let json: serde_json::Value = serde_json::from_slice(&bytes)?;
    let raw = json
        .get("address")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("keystore missing address field"))?;
    let with_prefix = if raw.starts_with("0x") {
        raw.to_owned()
    } else {
        format!("0x{raw}")
    };
    let address = Address::from_str(&with_prefix)?;
    Ok(address)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn address_reads_from_generated_keystore() {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let addr = decdn_incentive::eth_identity::generate_and_persist(dir.path(), "pw", false).unwrap();
        let ks = decdn_incentive::eth_identity::keystore_path(dir.path());
        assert_eq!(read_address(&ks).unwrap(), addr);
    }
}
