//! The key a client data dir signs its owned lanes with (ADR 003 § Capability
//! delegation, ADR 012).
//!
//! A pool owner can authorize any number of voucher signers, and every signer
//! draws on its own `(pool, signer, provider)` lanes. A client uses that to
//! keep two data dirs on one wallet apart: each data dir signs with a key
//! derived from the owner key and a random [`LaneSalt`] the data dir's buyer
//! store holds. Two data dirs that adopt one pool therefore sign on disjoint
//! lanes, and neither folds over the other's watermark.
//!
//! The derived key is never stored. It is recomputed from the unlocked owner
//! key on each run, so the data dir holds no secret the keystore does not
//! already guard. The owner key stays the pool owner: it sends the pool
//! transactions and signs the capability that authorizes the derived key.

use alloy::primitives::{B256, Keccak256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use rand::Rng;
use zeroize::Zeroizing;

/// Domain prefix of the lane-signer derivation. Frozen: a change derives a
/// different key for every data dir, which leaves each recorded lane under a
/// signer that no longer signs.
pub const LANE_SIGNER_DOMAIN: &[u8] = b"decdn/client-lane-signer/v1";

/// Width of a [`LaneSalt`] in bytes.
pub const LANE_SALT_LEN: usize = 32;

/// The per-data-dir salt a lane signer derives from. Not secret: the salt
/// alone derives nothing without the owner key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneSalt([u8; LANE_SALT_LEN]);

impl LaneSalt {
    /// A fresh salt from the thread's cryptographic random source.
    #[must_use]
    pub fn random() -> Self {
        let mut bytes = [0u8; LANE_SALT_LEN];
        rand::rng().fill_bytes(&mut bytes);
        Self(bytes)
    }

    /// The salt with these exact bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; LANE_SALT_LEN]) -> Self {
        Self(bytes)
    }

    /// The salt's bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; LANE_SALT_LEN] {
        &self.0
    }
}

/// Derive the key a data dir signs its owned lanes with:
/// `keccak256(LANE_SIGNER_DOMAIN ‖ owner_key ‖ salt)` as a secp256k1 scalar.
///
/// The same owner and salt always give the same key. Another salt or another
/// owner gives an unrelated key.
///
/// # Errors
///
/// The digest is not a valid secp256k1 scalar (zero or at least the curve
/// order). The chance is about 2⁻¹²⁸ per salt.
pub fn derive_lane_signer(
    owner: &PrivateKeySigner,
    salt: &LaneSalt,
) -> anyhow::Result<PrivateKeySigner> {
    let owner_key = Zeroizing::new(owner.to_bytes().0);
    let mut hasher = Keccak256::new();
    hasher.update(LANE_SIGNER_DOMAIN);
    hasher.update(owner_key.as_slice());
    hasher.update(salt.as_bytes());
    let digest = Zeroizing::new(hasher.finalize().0);
    PrivateKeySigner::from_bytes(&B256::from(*digest))
        .context("the lane-signer digest is not a valid secp256k1 key")
}

#[cfg(test)]
mod tests;
