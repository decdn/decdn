//! EIP-712 payment vouchers for the `StablePaymentChannel` contract.
//!
//! Vouchers are off-chain signed messages a client (payer) issues to a node
//! (payee) as content is delivered. Each voucher carries a cumulative `amount`
//! of `USDC` base units (`µUSDC`) and a cumulative `bytes_delivered` count.
//! The delivering node holds the latest voucher and submits it on-chain to
//! close and settle the channel.
//!
//! The signed payload follows ADR 003 §EIP-712 Voucher Signature exactly so
//! that an off-chain Rust signature byte-matches what the on-chain contract's
//! `closeChannel` / `disputeChannel` will accept.
//!
//! # Domain
//!
//! ```text
//! EIP712Domain {
//!     name: "StablePaymentChannel",
//!     version: "1",
//!     chainId: <L2 chain id>,
//!     verifyingContract: <StablePaymentChannel deployment address>,
//! }
//! ```
//!
//! # Voucher type
//!
//! ```text
//! Voucher(bytes32 channelId,uint256 amount,uint256 nonce,
//!         uint256 bytesDelivered,address token)
//! ```
//!
//! Nonces start at 1; nonce 0 is the on-chain sentinel for "no voucher
//! submitted" (ADR 003 §Voucher Nonce Convention).

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature, U256};
use alloy::signers::SignerSync;
use alloy::sol_types::{SolStruct, eip712_domain};

/// EIP-712 domain `name` field. Must match the `StablePaymentChannel`
/// contract's domain exactly — a mismatch produces a different digest and
/// every signature fails on-chain.
pub const DOMAIN_NAME: &str = "StablePaymentChannel";

/// EIP-712 domain `version` field.
pub const DOMAIN_VERSION: &str = "1";

// Solidity struct mirroring ADR 003 §EIP-712 Voucher Signature. Field names
// and order are part of the signed type — both must match the on-chain
// contract verbatim, hence the camelCase. The `sol!` macro generates a
// `SolStruct` impl whose `eip712_signing_hash` produces the same 32-byte
// digest the contract recovers signatures against.
//
// Wrapped in a private module because the `sol!` macro uses the Rust struct
// name as the on-chain Solidity type name in the EIP-712 type-string. The
// contract's struct is `Voucher`, so the Rust struct must be `Voucher` too —
// the wrapping module avoids the name clash with our public Rust [`Voucher`].
mod sol_types {
    alloy::sol! {
        #[allow(non_snake_case, missing_debug_implementations)]
        struct Voucher {
            bytes32 channelId;
            uint256 amount;
            uint256 nonce;
            uint256 bytesDelivered;
            address token;
        }
    }
}

use sol_types::Voucher as VoucherSol;

/// Construct the EIP-712 domain used to sign vouchers for a given
/// `StablePaymentChannel` deployment.
#[must_use]
pub fn voucher_domain(chain_id: u64, verifying_contract: Address) -> Eip712Domain {
    eip712_domain! {
        name: DOMAIN_NAME,
        version: DOMAIN_VERSION,
        chain_id: chain_id,
        verifying_contract: verifying_contract,
    }
}

/// A payment voucher in its unsigned form.
///
/// All amounts are cumulative across the channel's lifetime — a voucher with
/// `amount = 100` does not mean "pay 100 more" but "the total claimable is
/// 100." `nonce` and `bytes_delivered` are cumulative likewise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Voucher {
    /// `channelId = keccak256(client, provider, channelNonce)` per ADR 003.
    pub channel_id: B256,
    /// Cumulative payment in token base units (`µUSDC` for `USDC`).
    pub amount: U256,
    /// Voucher sequence number within the channel; starts at 1.
    pub nonce: U256,
    /// Cumulative bytes delivered against this channel.
    pub bytes_delivered: U256,
    /// `ERC-20` token address (`USDC` for the `PoC` `StablePaymentChannel`).
    pub token: Address,
}

impl Voucher {
    const fn to_sol(&self) -> VoucherSol {
        VoucherSol {
            channelId: self.channel_id,
            amount: self.amount,
            nonce: self.nonce,
            bytesDelivered: self.bytes_delivered,
            token: self.token,
        }
    }

    /// EIP-712 signing hash bound to `domain`. This is the 32-byte digest
    /// passed into `ecrecover` on-chain — identical for any signer.
    #[must_use]
    pub fn signing_hash(&self, domain: &Eip712Domain) -> B256 {
        self.to_sol().eip712_signing_hash(domain)
    }

    /// Sign the voucher with `signer` for the given EIP-712 `domain`.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the underlying signer (key locked,
    /// remote signer offline, etc.).
    pub fn sign<S: SignerSync>(
        self,
        signer: &S,
        domain: &Eip712Domain,
    ) -> Result<SignedVoucher, alloy::signers::Error> {
        let hash = self.signing_hash(domain);
        let signature = signer.sign_hash_sync(&hash)?;
        Ok(SignedVoucher {
            voucher: self,
            signature,
        })
    }
}

/// A voucher together with its EIP-712 signature.
///
/// Holding a `SignedVoucher` is sufficient for a node to settle the channel —
/// the signature recovers the client's address, which the contract then
/// matches against `channel.client`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedVoucher {
    pub voucher: Voucher,
    pub signature: Signature,
}

impl SignedVoucher {
    /// Recover the address that signed this voucher under `domain`.
    ///
    /// # Errors
    ///
    /// Returns [`VoucherError::InvalidSignature`] if the signature is
    /// malformed (non-canonical `s`, invalid recovery id, etc.).
    pub fn recover_signer(&self, domain: &Eip712Domain) -> Result<Address, VoucherError> {
        let hash = self.voucher.signing_hash(domain);
        self.signature
            .recover_address_from_prehash(&hash)
            .map_err(|_| VoucherError::InvalidSignature)
    }

    /// Verify the signature was produced by `expected` for `domain`.
    ///
    /// # Errors
    ///
    /// - [`VoucherError::InvalidSignature`] — signature is malformed.
    /// - [`VoucherError::WrongSigner`] — recovered address differs from
    ///   `expected`.
    pub fn verify_signer(
        &self,
        expected: Address,
        domain: &Eip712Domain,
    ) -> Result<(), VoucherError> {
        let recovered = self.recover_signer(domain)?;
        if recovered == expected {
            Ok(())
        } else {
            Err(VoucherError::WrongSigner {
                expected,
                recovered,
            })
        }
    }
}

/// Failure modes for voucher verification.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VoucherError {
    /// The signature is malformed — non-canonical `s`, invalid recovery id,
    /// or a corrupted byte. The voucher is unusable as-is.
    #[error("voucher signature is malformed")]
    InvalidSignature,
    /// The signature is well-formed but recovers to an address that does not
    /// match the expected signer — e.g., the channel's client field.
    #[error("voucher signed by {recovered}, expected {expected}")]
    WrongSigner {
        expected: Address,
        recovered: Address,
    },
}

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed and address/addr pair up clearly here
mod tests {
    use super::*;
    use alloy::primitives::{address, b256};
    use alloy::signers::local::PrivateKeySigner;

    fn sample_voucher() -> Voucher {
        Voucher {
            channel_id: b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff"),
            amount: U256::from(10_000_000u64), // 10 USDC at 6 decimals
            nonce: U256::from(1u64),
            bytes_delivered: U256::from(1_048_576u64), // 1 MB
            token: address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"), // USDC mainnet
        }
    }

    fn sample_domain() -> Eip712Domain {
        // Arbitrum Sepolia chain id; address is a deterministic test fixture.
        voucher_domain(
            421_614,
            address!("0000000000000000000000000000000000001234"),
        )
    }

    /// Helper: extract the error from a `Result`, returning a typed error if
    /// the value was unexpectedly `Ok`.
    fn err_of<T: std::fmt::Debug, E>(r: Result<T, E>) -> anyhow::Result<E> {
        r.err()
            .ok_or_else(|| anyhow::anyhow!("expected error, got Ok"))
    }

    #[test]
    fn round_trip_sign_verify() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let address = signer.address();
        let domain = sample_domain();

        let signed = sample_voucher().sign(&signer, &domain)?;
        signed.verify_signer(address, &domain)?;
        Ok(())
    }

    #[test]
    fn wrong_signer_address_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let other = PrivateKeySigner::random().address();
        let domain = sample_domain();

        let signed = sample_voucher().sign(&signer, &domain)?;
        let err = err_of(signed.verify_signer(other, &domain))?;
        anyhow::ensure!(
            matches!(err, VoucherError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn different_chain_id_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain_a = voucher_domain(
            421_614,
            address!("0000000000000000000000000000000000001234"),
        );
        let domain_b = voucher_domain(1, address!("0000000000000000000000000000000000001234"));

        let signed = sample_voucher().sign(&signer, &domain_a)?;
        // Recovered signer differs from the actual one because the digest
        // changed, so we get a WrongSigner — not InvalidSignature.
        let err = err_of(signed.verify_signer(signer.address(), &domain_b))?;
        anyhow::ensure!(
            matches!(err, VoucherError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn different_verifying_contract_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain_a = voucher_domain(
            421_614,
            address!("0000000000000000000000000000000000001234"),
        );
        let domain_b = voucher_domain(
            421_614,
            address!("0000000000000000000000000000000000005678"),
        );

        let signed = sample_voucher().sign(&signer, &domain_a)?;
        let err = err_of(signed.verify_signer(signer.address(), &domain_b))?;
        anyhow::ensure!(
            matches!(err, VoucherError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn tampered_amount_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let mut signed = sample_voucher().sign(&signer, &domain)?;

        signed.voucher.amount += U256::from(1u64);

        let err = err_of(signed.verify_signer(signer.address(), &domain))?;
        anyhow::ensure!(
            matches!(err, VoucherError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn tampered_nonce_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let mut signed = sample_voucher().sign(&signer, &domain)?;

        signed.voucher.nonce += U256::from(1u64);

        let err = err_of(signed.verify_signer(signer.address(), &domain))?;
        anyhow::ensure!(
            matches!(err, VoucherError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn tampered_bytes_delivered_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let mut signed = sample_voucher().sign(&signer, &domain)?;

        signed.voucher.bytes_delivered += U256::from(1u64);

        let err = err_of(signed.verify_signer(signer.address(), &domain))?;
        anyhow::ensure!(
            matches!(err, VoucherError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn tampered_channel_id_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let mut signed = sample_voucher().sign(&signer, &domain)?;

        signed.voucher.channel_id = B256::ZERO;

        let err = err_of(signed.verify_signer(signer.address(), &domain))?;
        anyhow::ensure!(
            matches!(err, VoucherError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn tampered_token_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let mut signed = sample_voucher().sign(&signer, &domain)?;

        signed.voucher.token = address!("0000000000000000000000000000000000000000");

        let err = err_of(signed.verify_signer(signer.address(), &domain))?;
        anyhow::ensure!(
            matches!(err, VoucherError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    /// Lock the EIP-712 type hash to the exact ADR 003 wording. If this
    /// breaks, either the ADR changed or the `sol!` macro's canonical
    /// encoding shifted — both warrant a coordinated update with the
    /// Solidity contract.
    #[test]
    fn voucher_type_hash_matches_adr_003() -> anyhow::Result<()> {
        use alloy::primitives::keccak256;
        // Canonical EIP-712 type-string per ADR 003 §EIP-712 Voucher
        // Signature. Single space between Solidity type and field name; no
        // other whitespace; fields in declaration order.
        let canonical: &[u8] = b"Voucher(bytes32 channelId,uint256 amount,uint256 nonce,uint256 bytesDelivered,address token)";
        let expected = keccak256(canonical);
        let actual = VoucherSol::eip712_type_hash(&sample_voucher().to_sol());
        anyhow::ensure!(
            actual == expected,
            "voucher type hash drifted: actual={actual} expected={expected}"
        );
        Ok(())
    }

    /// Lock the domain separator to the canonical EIP-712 form. If the
    /// alloy `Eip712Domain` ever changes its separator computation, this
    /// surfaces it before signatures stop being recoverable on-chain.
    #[test]
    fn domain_separator_matches_eip712_canonical() -> anyhow::Result<()> {
        use alloy::primitives::keccak256;
        use alloy::sol_types::SolValue;

        let domain_typehash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        let chain_id: u64 = 421_614;
        let verifying = address!("0000000000000000000000000000000000001234");

        let domain = voucher_domain(chain_id, verifying);
        let actual = domain.separator();

        // separator = keccak256(abi.encode(typehash, hash(name), hash(version), chainId, addr))
        let expected = keccak256(
            (
                domain_typehash,
                keccak256(DOMAIN_NAME.as_bytes()),
                keccak256(DOMAIN_VERSION.as_bytes()),
                U256::from(chain_id),
                verifying,
            )
                .abi_encode(),
        );
        anyhow::ensure!(
            actual == expected,
            "domain separator drift: actual={actual} expected={expected}"
        );
        Ok(())
    }

    /// Fixed-vector regression: a deterministic key + voucher must produce a
    /// specific recovered address. If the digest computation drifts, this
    /// catches it before the contract does.
    #[test]
    fn fixed_vector_recovers_expected_signer() -> anyhow::Result<()> {
        // Test key — never used for anything but this fixture.
        let pk_hex = "1111111111111111111111111111111111111111111111111111111111111111";
        let signer: PrivateKeySigner = pk_hex
            .parse()
            .map_err(|e| anyhow::anyhow!("parse test key: {e}"))?;
        let address = signer.address();

        let voucher = Voucher {
            channel_id: B256::repeat_byte(0xAA),
            amount: U256::from(1_000_000u64),
            nonce: U256::from(1u64),
            bytes_delivered: U256::from(1_048_576u64),
            token: address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
        };
        let domain = voucher_domain(
            421_614,
            address!("00000000000000000000000000000000deadbeef"),
        );

        let signed = voucher.sign(&signer, &domain)?;
        let recovered = signed.recover_signer(&domain)?;
        anyhow::ensure!(
            recovered == address,
            "recovered {recovered}, expected {address}"
        );
        Ok(())
    }
}
