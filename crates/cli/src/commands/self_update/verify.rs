//! Trust decisions for a release: the minisign signature over `SHA256SUMS`
//! and the archive digest the signed manifest lists.
//!
//! The keys in [`TRUSTED_KEYS`] are the `minisign-<maintainer>.pub` files
//! committed in this crate, one per maintainer, embedded at build time. A
//! release signed by any of them verifies; one whose `SHA256SUMS.minisig`
//! verifies under no key in the set is refused outright. A binary trusts a
//! key from the first release built after the file lands, which is why a
//! maintainer rotating their key adds the new file in one release and
//! removes the old one in a later release.

use anyhow::{Context, bail};
use minisign_verify::{Error as MinisignError, PublicKey, Signature};
use sha2::{Digest, Sha256};

use super::release::Manifest;

/// The minisign public keys this build trusts: every `crates/cli/minisign*.pub`,
/// one `include_str!` per file. A unit test keeps this list equal to the files.
pub(crate) const TRUSTED_KEYS: &[&str] = &[include_str!("../../../minisign-thiras.pub")];

/// Decode every embedded key.
pub(crate) fn trusted_keys() -> anyhow::Result<Vec<PublicKey>> {
    TRUSTED_KEYS
        .iter()
        .map(|text| PublicKey::decode(text).context("embedded minisign public key is malformed"))
        .collect()
}

/// Check that `minisig` is a valid minisign signature over `sums` by one of
/// `keys`, and parse the manifest those bytes hold. A signature from a key
/// outside the set is refused by that fact alone; a signature that names a
/// trusted key but does not verify is a tampered manifest or signature. The
/// parse runs only after the signature holds, so a [`Manifest`] value is
/// always one a trusted key signed.
pub(crate) fn verify_manifest_with(
    keys: &[PublicKey],
    sums: &[u8],
    minisig: &str,
) -> anyhow::Result<Manifest> {
    let signature = Signature::decode(minisig)
        .map_err(|e| anyhow::anyhow!("SHA256SUMS.minisig is not a minisign signature: {e}"))?;
    for key in keys {
        match key.verify(sums, &signature, false) {
            Ok(()) => {
                return Manifest::parse(
                    std::str::from_utf8(sums).context("SHA256SUMS is not UTF-8")?,
                );
            }
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
