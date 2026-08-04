//! Keystore address lookup and top-up-authorization signing for the
//! sponsor CLI wrapper.
//!
//! `read_address` reads the plaintext `address` field straight out of the
//! v3 keystore JSON — no password needed, since the address isn't secret.
//! `sign_topup` decrypts the keystore (via `decdn_incentive::eth_identity`)
//! and produces an EIP-191 `personal_sign` signature over the same
//! challenge-message format the sponsor server expects in
//! `topup_auth::challenge_message` (Task 5): the two literals must stay
//! byte-identical, or the server will never recover the sponsor's address
//! from a wrapper-produced signature.

use std::path::Path;
use std::str::FromStr;

use alloy::primitives::{Address, B256};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;

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

/// Decrypt the keystore at `keystore_path` and sign a top-up authorization
/// message for `channel_id` at `timestamp`.
///
/// The signed message MUST byte-match the sponsor server's
/// `topup_auth::challenge_message` (Task 5): both format
/// `"decdn-sponsor topup {channel_id} {timestamp}"`, with `channel_id` as
/// alloy's `B256` `Display` (0x-prefixed lowercase hex). Do not change this
/// format without updating the server side in the same change.
///
/// Decryption runs the keystore KDF (scrypt/argon2), which blocks for
/// hundreds of milliseconds — offloaded to `spawn_blocking` so it doesn't
/// stall the async runtime.
///
/// # Errors
///
/// Returns an error if the keystore can't be decrypted with `password`, or
/// if the blocking task itself fails to join.
pub async fn sign_topup(
    keystore_path: &Path,
    password: &str,
    channel_id: B256,
    timestamp: u64,
) -> anyhow::Result<String> {
    let ks = keystore_path.to_path_buf();
    let pw = password.to_owned();
    let signer: PrivateKeySigner =
        tokio::task::spawn_blocking(move || decdn_incentive::eth_identity::load_signer(&ks, &pw))
            .await??;

    let msg = format!("decdn-sponsor topup {channel_id} {timestamp}");
    let sig = signer.sign_message_sync(msg.as_bytes())?;
    Ok(format!("0x{}", hex::encode(sig.as_bytes())))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use alloy::primitives::b256;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn address_reads_and_signature_recovers() {
        let dir = tempfile::tempdir().unwrap();
        // `TempDir` defaults can leave group/other read+execute bits set
        // depending on the process umask (observed 0o755 in this sandbox).
        // decdn's `eth_identity::ensure_data_dir` requires 0o700, so pin it
        // explicitly, mirroring decdn's own `eth_identity` test helper.
        #[cfg(unix)]
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        // create a keystore using decdn's generator
        let addr =
            decdn_incentive::eth_identity::generate_and_persist(dir.path(), "pw", false).unwrap();
        let ks = decdn_incentive::eth_identity::keystore_path(dir.path());

        assert_eq!(read_address(&ks).unwrap(), addr);

        let id = b256!("33333333333333333333333333333333333333333333333333333333333333ff");
        let ts = 1_769_904_000u64;
        let sig_hex = sign_topup(&ks, "pw", id, ts).await.unwrap();

        // recovers to addr over the shared challenge message
        let msg = format!("decdn-sponsor topup {id} {ts}");
        let sig = alloy::signers::Signature::from_str(&sig_hex).unwrap();
        assert_eq!(sig.recover_address_from_msg(msg.as_bytes()).unwrap(), addr);
    }
}
