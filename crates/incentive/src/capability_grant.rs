//! `dcap1:` — the transferable encoding of a pool owner's spending
//! [`crate::capability::SignedCapability`].
//!
//! A pool owner delegates spend to a signer off-chain (see
//! [`crate::capability`]). To hand that grant to a delegated client, the owner
//! serializes it as a single copy-pasteable token: `dcap1:` followed by the
//! base64url (no padding) of the postcard-encoded [`CapabilityGrant`]. The
//! client decodes the token, reconstructs the [`SignedCapability`], and presents
//! it to the serving node, which registers the signer on its first on-chain
//! redemption.
//!
//! The token is self-contained: it carries the four signed capability fields and
//! the owner's raw EIP-712 signature, so the client needs nothing else to rebuild
//! and use the grant. It is NOT secret to the pool — anyone holding it can only
//! spend as the named `signer`, and only that signer's key can sign vouchers
//! under it.

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

use crate::capability::{Capability, CapabilityError, SignedCapability};

/// The `dcap1:` token prefix — a version tag, so a future encoding change is a
/// visible break rather than a silent misdecode.
const TOKEN_PREFIX: &str = "dcap1:";

/// A pool owner's [`SignedCapability`] in its transferable, self-contained form.
///
/// Postcard-encoded and base64url-wrapped by [`Self::to_token`]. The owner
/// signature is held as raw bytes (65-byte `r‖s‖v`, exactly
/// [`Signature::as_bytes`]) rather than an `alloy` `Signature` so the wire shape
/// is a plain byte string that postcard round-trips without a human-readable
/// branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityGrant {
    /// The on-chain pool the grant draws from.
    pub pool_id: B256,
    /// The delegate voucher-signing key the owner authorizes.
    pub signer: Address,
    /// Cumulative spend ceiling (token base units) for `signer` under this
    /// grant. A `u64`, matching [`Capability::spending_cap`] and the
    /// `PaymentPool.spendingCap` on-chain width.
    pub spending_cap: u64,
    /// Unix-seconds expiry after which vouchers under this grant stop being
    /// redeemable.
    pub expiry: u64,
    /// The owner's raw EIP-712 signature over the capability (65 bytes,
    /// `r‖s‖v`).
    pub owner_signature: Vec<u8>,
}

/// Failure modes for decoding a `dcap1:` token.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GrantError {
    /// The string does not start with the `dcap1:` prefix — not a delegation
    /// token (or a future/foreign version).
    #[error("not a dcap1 capability token (missing the `dcap1:` prefix)")]
    BadPrefix,
    /// The body is not valid base64url.
    #[error("capability token body is not valid base64url")]
    BadBase64,
    /// The decoded bytes are not a well-formed postcard `CapabilityGrant`
    /// (truncated or corrupt).
    #[error("capability token payload is malformed (truncated or corrupt)")]
    BadPayload,
    /// The embedded owner signature is not a well-formed 65-byte ECDSA
    /// signature, so no owner can be recovered.
    #[error("capability token owner signature is malformed")]
    BadSignature,
}

impl CapabilityGrant {
    /// Build a grant from an already-signed capability, copying its owner
    /// signature into the token's raw-bytes field.
    #[must_use]
    pub fn from_signed_capability(signed: &SignedCapability) -> Self {
        Self {
            pool_id: signed.capability.pool_id,
            signer: signed.capability.signer,
            spending_cap: signed.capability.spending_cap,
            expiry: signed.capability.expiry,
            owner_signature: signed.signature.as_bytes().to_vec(),
        }
    }

    /// Encode the grant as a `dcap1:` token: the prefix followed by the
    /// base64url (no padding) of its postcard encoding.
    ///
    /// # Panics
    ///
    /// Never — postcard encoding of a fixed-shape struct into a growable `Vec`
    /// does not fail, so the `Result` is unwrapped to an empty body on the
    /// unreachable error rather than propagated.
    #[must_use]
    pub fn to_token(&self) -> String {
        // Postcard serialization of an owned, fixed-shape struct into a `Vec`
        // has no fallible step; on the unreachable error emit an empty body,
        // which `from_token` then rejects as `BadPayload` rather than crashing.
        let bytes = postcard::to_allocvec(self).unwrap_or_default();
        format!("{TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes))
    }

    /// Parse and validate a `dcap1:` token back into a grant.
    ///
    /// # Errors
    ///
    /// - [`GrantError::BadPrefix`] — the `dcap1:` prefix is absent.
    /// - [`GrantError::BadBase64`] — the body is not valid base64url.
    /// - [`GrantError::BadPayload`] — the decoded bytes are not a postcard
    ///   `CapabilityGrant`.
    pub fn from_token(s: &str) -> Result<Self, GrantError> {
        let body = s.strip_prefix(TOKEN_PREFIX).ok_or(GrantError::BadPrefix)?;
        let bytes = URL_SAFE_NO_PAD
            .decode(body.as_bytes())
            .map_err(|_| GrantError::BadBase64)?;
        postcard::from_bytes(&bytes).map_err(|_| GrantError::BadPayload)
    }

    /// Reconstruct the [`SignedCapability`] this grant carries, ready for
    /// `PoolContext::with_capability`.
    ///
    /// # Errors
    ///
    /// [`GrantError::BadSignature`] when `owner_signature` is not a well-formed
    /// 65-byte ECDSA signature.
    pub fn to_signed_capability(&self) -> Result<SignedCapability, GrantError> {
        let signature =
            Signature::from_raw(&self.owner_signature).map_err(|_| GrantError::BadSignature)?;
        Ok(SignedCapability {
            capability: Capability {
                signer: self.signer,
                spending_cap: self.spending_cap,
                pool_id: self.pool_id,
                expiry: self.expiry,
            },
            signature,
        })
    }

    /// Recover the pool owner that signed this grant under `domain`, for display
    /// and validation.
    ///
    /// # Errors
    ///
    /// - [`GrantError::BadSignature`] — the embedded signature is malformed.
    /// - [`CapabilityError::InvalidSignature`] via [`GrantOwnerError`] — the
    ///   signature is non-canonical (high-`s`) or unrecoverable.
    pub fn owner(&self, domain: &Eip712Domain) -> Result<Address, GrantOwnerError> {
        let signed = self.to_signed_capability()?;
        signed
            .recover_owner(domain)
            .map_err(GrantOwnerError::Recover)
    }
}

/// Failure of [`CapabilityGrant::owner`]: either the token's signature bytes are
/// malformed, or a well-formed signature does not recover an owner.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GrantOwnerError {
    /// The token's raw signature bytes are not a 65-byte ECDSA signature.
    #[error(transparent)]
    Decode(#[from] GrantError),
    /// A well-formed signature failed owner recovery (non-canonical or invalid).
    #[error(transparent)]
    Recover(CapabilityError),
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use alloy::primitives::{address, b256};
    use alloy::signers::local::PrivateKeySigner;

    fn sample_domain() -> Eip712Domain {
        crate::capability::voucher_domain(
            421_614,
            address!("0000000000000000000000000000000000001234"),
        )
    }

    fn sample_capability() -> Capability {
        Capability {
            signer: address!("00000000000000000000000000000000000000a1"),
            spending_cap: 10_000_000u64,
            pool_id: b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff"),
            expiry: 1_900_000_000,
        }
    }

    fn signed_grant() -> (SignedCapability, CapabilityGrant, Address) {
        let owner = PrivateKeySigner::random();
        let signed = sample_capability().sign(&owner, &sample_domain()).unwrap();
        let grant = CapabilityGrant::from_signed_capability(&signed);
        (signed, grant, owner.address())
    }

    #[test]
    fn token_round_trip_is_lossless() {
        let (_, grant, _) = signed_grant();
        let token = grant.to_token();
        assert!(
            token.starts_with("dcap1:"),
            "token carries the prefix: {token}"
        );
        let decoded = CapabilityGrant::from_token(&token).expect("round-trips");
        assert_eq!(decoded, grant, "decode must reproduce the grant exactly");
    }

    #[test]
    fn reconstructed_capability_recovers_the_same_owner() {
        let (signed, grant, owner) = signed_grant();
        let domain = sample_domain();
        // The token's reconstructed capability recovers to the SAME owner as the
        // original, so a node validating either sees one pool owner.
        let rebuilt = grant.to_signed_capability().expect("rebuild");
        assert_eq!(
            rebuilt, signed,
            "rebuilt SignedCapability must equal the original"
        );
        assert_eq!(rebuilt.recover_owner(&domain).unwrap(), owner);
        assert_eq!(grant.owner(&domain).unwrap(), owner);
    }

    /// Fixed-vector: a deterministic owner key + capability must recover a stable
    /// owner through the whole token path, catching a codec drift before a node
    /// does.
    #[test]
    fn fixed_vector_token_recovers_expected_owner() {
        let pk_hex = "2222222222222222222222222222222222222222222222222222222222222222";
        let owner: PrivateKeySigner = pk_hex.parse().unwrap();
        let domain = sample_domain();
        let signed = sample_capability().sign(&owner, &domain).unwrap();
        let grant = CapabilityGrant::from_signed_capability(&signed);
        let token = grant.to_token();
        let decoded = CapabilityGrant::from_token(&token).unwrap();
        assert_eq!(decoded.owner(&domain).unwrap(), owner.address());
    }

    #[test]
    fn bad_prefix_is_rejected() {
        let (_, grant, _) = signed_grant();
        let body = grant.to_token();
        let no_prefix = body.trim_start_matches("dcap1:");
        assert_eq!(
            CapabilityGrant::from_token(no_prefix),
            Err(GrantError::BadPrefix)
        );
        assert_eq!(
            CapabilityGrant::from_token("dcap2:whatever"),
            Err(GrantError::BadPrefix)
        );
    }

    #[test]
    fn truncated_body_is_rejected_without_panicking() {
        let (_, grant, _) = signed_grant();
        let token = grant.to_token();
        // Lop off the trailing half of the base64 body: still valid-ish base64url
        // characters, but a truncated postcard payload.
        let cut = token.len() - (token.len() - "dcap1:".len()) / 2;
        let truncated = &token[..cut];
        let err = CapabilityGrant::from_token(truncated)
            .expect_err("a truncated token must be rejected, not panic");
        assert!(
            matches!(err, GrantError::BadPayload | GrantError::BadBase64),
            "unexpected error for truncated token: {err:?}"
        );
    }

    #[test]
    fn non_base64_body_is_rejected() {
        // `*` is outside the base64url alphabet.
        assert_eq!(
            CapabilityGrant::from_token("dcap1:not*base64*"),
            Err(GrantError::BadBase64)
        );
    }

    #[test]
    fn malformed_signature_bytes_fail_reconstruction() {
        let (_, mut grant, _) = signed_grant();
        grant.owner_signature = vec![0u8; 10]; // not 65 bytes
        assert_eq!(grant.to_signed_capability(), Err(GrantError::BadSignature));
        assert!(matches!(
            grant.owner(&sample_domain()),
            Err(GrantOwnerError::Decode(GrantError::BadSignature))
        ));
    }
}
