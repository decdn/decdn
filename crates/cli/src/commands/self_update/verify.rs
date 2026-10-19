//! Trust decisions for a release: the minisign signature over `SHA256SUMS`
//! and the archive digest the signed manifest lists.
//!
//! The keys in [`TRUSTED_KEYS`] are the `minisign*.pub` files committed in
//! this crate, embedded at build time. A release whose `SHA256SUMS.minisig`
//! no key in that set verifies is refused outright. To rotate the key, a
//! release signed by the current key ships a binary that embeds both the
//! current and the next key; releases after it sign with the next key.

use anyhow::{Context, bail};
use minisign_verify::{Error as MinisignError, PublicKey, Signature};
use sha2::{Digest, Sha256};

use super::release::Manifest;

/// The minisign public keys this build trusts, one `minisign*.pub` file each.
pub(crate) const TRUSTED_KEYS: &[&str] = &[include_str!("../../../minisign.pub")];

/// Decode every embedded key.
pub(crate) fn trusted_keys() -> anyhow::Result<Vec<PublicKey>> {
    TRUSTED_KEYS
        .iter()
        .map(|text| PublicKey::decode(text).context("embedded minisign public key is malformed"))
        .collect()
}

/// Check that `minisig` is a valid minisign signature over `sums` by one of
/// `keys`. A signature from a key outside the set is refused by that fact
/// alone; a signature that names a trusted key but does not verify is a
/// tampered manifest or signature.
pub(crate) fn verify_manifest_with(
    keys: &[PublicKey],
    sums: &[u8],
    minisig: &str,
) -> anyhow::Result<()> {
    let signature = Signature::decode(minisig)
        .map_err(|e| anyhow::anyhow!("SHA256SUMS.minisig is not a minisign signature: {e}"))?;
    for key in keys {
        match key.verify(sums, &signature, false) {
            Ok(()) => return Ok(()),
            Err(MinisignError::UnexpectedKeyId) => {}
            Err(e) => bail!("SHA256SUMS does not verify against SHA256SUMS.minisig: {e}"),
        }
    }
    bail!("SHA256SUMS.minisig is not signed by a key this decdn build trusts")
}

/// Lowercase hex SHA-256 of `bytes`, as `sha256sum` prints it.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    alloy::hex::encode(Sha256::digest(bytes))
}

/// Check `bytes` against the digest the signed manifest lists for `name`.
pub(crate) fn verify_archive(manifest: &Manifest, name: &str, bytes: &[u8]) -> anyhow::Result<()> {
    let expected = manifest
        .digest_for(name)
        .with_context(|| format!("{name} is not listed in SHA256SUMS"))?;
    let actual = sha256_hex(bytes);
    if actual != expected {
        bail!("{name} does not match SHA256SUMS (expected {expected}, got {actual})");
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
