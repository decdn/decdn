//! EIP-712 payment vouchers for the `PaymentPool` contract.
//!
//! Vouchers are off-chain signed messages a client (payer) issues to a node
//! (provider) as content is delivered. Each voucher carries a cumulative
//! `amount` of `USDC` base units (`µUSDC`) and a cumulative `bytes_delivered`
//! count, scoped to a `(pool_id, signer, provider)` lane. The delivering node
//! holds the latest voucher and redeems it on-chain against the pool.
//!
//! The signed payload follows ADR 003 §EIP-712 Voucher Signature exactly so
//! that an off-chain Rust signature byte-matches what the on-chain
//! `PaymentPool.redeem` accepts.
//!
//! # Domain
//!
//! ```text
//! EIP712Domain {
//!     name: "PaymentPool",
//!     version: "1",
//!     chainId: <L2 chain id>,
//!     verifyingContract: <PaymentPool deployment address>,
//! }
//! ```
//!
//! # Voucher type
//!
//! ```text
//! Voucher(bytes32 poolId,address signer,address provider,
//!         uint256 amount,uint256 bytesDelivered,
//!         bytes32 chainRoot,uint256 chunkPrice)
//! ```
//!
//! There is no nonce: `amount` is the sole monotone ordering and replay key —
//! a voucher whose `amount` is no greater than the highest already accepted is
//! stale (ADR 003 §Voucher ordering).
//!
//! `amount` is the settlement anchor and `chain_root` heads the optional
//! `PayWord` hash chain that advances it between signatures, so redemption
//! resolves both as `claimed = amount + chain_index × chunk_price` (ADR 003
//! §Hash-chain metering (`PayWord`); the chain primitive lives in
//! [`crate::chain`]). Neither `chain_length` nor `chunk_bytes` is signed —
//! both are protocol constants, so signing them would pay calldata for values
//! every party already holds.

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature, U256};
use alloy::signers::SignerSync;
use alloy::sol_types::{SolStruct, eip712_domain};

/// EIP-712 domain `name` field. Must match the `PaymentPool` contract's
/// domain exactly — a mismatch produces a different digest and every
/// signature fails on-chain.
pub const DOMAIN_NAME: &str = "PaymentPool";

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
            bytes32 poolId;
            address signer;
            address provider;
            uint256 amount;
            uint256 bytesDelivered;
            bytes32 chainRoot;
            uint256 chunkPrice;
        }
    }
}

use sol_types::Voucher as VoucherSol;

/// Construct the EIP-712 domain used to sign vouchers for a given
/// `PaymentPool` deployment.
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
/// All amounts are cumulative across the lane's lifetime — a voucher with
/// `amount = 100` does not mean "pay 100 more" but "the total claimable on
/// this lane is 100." `bytes_delivered` is cumulative likewise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Voucher {
    /// The pool this voucher draws from — the on-chain `PaymentPool` deposit id.
    pub pool_id: B256,
    /// The address whose EIP-712 signature authorizes this voucher (the
    /// capability signer the pool owner delegated spend to).
    pub signer: Address,
    /// The provider node this voucher pays — a capability voucher is scoped to
    /// one provider and is invalid if redeemed against another.
    pub provider: Address,
    /// Cumulative payment in token base units (`µUSDC` for `USDC`). The
    /// settlement anchor a hash chain extends.
    pub amount: U256,
    /// Cumulative bytes delivered against this lane.
    pub bytes_delivered: U256,
    /// Head of the hash chain this voucher opens —
    /// [`crate::chain::root_from_seed`] of the payer's per-lane seed. Zero
    /// **seals** the voucher at exactly `amount`: nothing hashes to zero, so no
    /// index above 0 can redeem against it (ADR 003 §The sealed voucher).
    pub chain_root: B256,
    /// What one chunk of delivery adds over `amount`, in token base units.
    /// Signed so the claim arithmetic is fixed at signing time; the floor clamp
    /// at redemption still reads the live `deliveryFloor`. Zero on a sealed
    /// voucher, which meters no chunk.
    pub chunk_price: U256,
}

impl Voucher {
    const fn to_sol(&self) -> VoucherSol {
        VoucherSol {
            poolId: self.pool_id,
            signer: self.signer,
            provider: self.provider,
            amount: self.amount,
            bytesDelivered: self.bytes_delivered,
            chainRoot: self.chain_root,
            chunkPrice: self.chunk_price,
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
/// Holding a `SignedVoucher` is sufficient for a node to redeem against the
/// pool — the signature recovers the capability signer's address, which the
/// contract then matches against the registered capability.
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
        // Reject non-canonical high-`s` up front so the off-chain accept-set
        // matches the on-chain verifiable-set (#836); alloy would otherwise
        // silently normalize and accept it.
        if crate::sig_canon::is_high_s(&self.signature) {
            return Err(VoucherError::InvalidSignature);
        }
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
    /// match the expected signer — e.g., the lane's pinned capability signer.
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
            pool_id: b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff"),
            signer: address!("00000000000000000000000000000000000000a1"),
            provider: address!("00000000000000000000000000000000000000b2"),
            amount: U256::from(10_000_000u64), // 10 USDC at 6 decimals
            bytes_delivered: U256::from(1_048_576u64), // 1 MB
            chain_root: B256::repeat_byte(0xA7),
            chunk_price: U256::from(10u64), // one chunk == one MB at 10 µUSDC/MB
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
    fn unrecoverable_signature_is_invalid_signature() -> anyhow::Result<()> {
        // A structurally well-formed signature (valid parity byte) whose `r`/`s`
        // do not recover to any point surfaces as `InvalidSignature` — the only
        // producer of that variant. `r = s = 0` is the canonical unrecoverable
        // case (ECDSA recovery requires `r, s ∈ [1, n)`). This is the one
        // negative path the tamper tests can't reach: they all start from a
        // valid signature and only perturb the digest, yielding `WrongSigner`.
        let signed = SignedVoucher {
            voucher: sample_voucher(),
            signature: Signature::new(U256::ZERO, U256::ZERO, false),
        };
        let err = err_of(signed.recover_signer(&sample_domain()))?;
        anyhow::ensure!(
            matches!(err, VoucherError::InvalidSignature),
            "expected InvalidSignature, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn high_s_voucher_signature_rejected() -> anyhow::Result<()> {
        // A malicious client can flip a valid voucher signature to its
        // non-canonical high-`s` twin. alloy's recovery would normalize and
        // accept it, but the on-chain `PaymentPool` reverts (#836). The
        // off-chain accept-set must match: reject the twin, accept the original.
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let signed = sample_voucher().sign(&signer, &domain)?;
        signed.verify_signer(signer.address(), &domain)?; // low-s original still valid

        let twin = SignedVoucher {
            signature: crate::sig_canon::high_s_twin(&signed.signature),
            ..signed
        };
        anyhow::ensure!(
            twin.recover_signer(&domain) == Err(VoucherError::InvalidSignature),
            "high-s voucher twin must be rejected as InvalidSignature"
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
    fn tampered_signer_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let mut signed = sample_voucher().sign(&signer, &domain)?;

        signed.voucher.signer = address!("000000000000000000000000000000000000dead");

        let err = err_of(signed.verify_signer(signer.address(), &domain))?;
        anyhow::ensure!(
            matches!(err, VoucherError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn tampered_provider_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let mut signed = sample_voucher().sign(&signer, &domain)?;

        signed.voucher.provider = address!("000000000000000000000000000000000000beef");

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
    fn tampered_pool_id_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let mut signed = sample_voucher().sign(&signer, &domain)?;

        signed.voucher.pool_id = B256::ZERO;

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
        let canonical: &[u8] = b"Voucher(bytes32 poolId,address signer,address provider,uint256 amount,uint256 bytesDelivered,bytes32 chainRoot,uint256 chunkPrice)";
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
            pool_id: B256::repeat_byte(0xAA),
            signer: address,
            provider: address!("00000000000000000000000000000000000000b2"),
            amount: U256::from(1_000_000u64),
            bytes_delivered: U256::from(1_048_576u64),
            chain_root: B256::ZERO,
            chunk_price: U256::ZERO,
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

    /// Pin the EIP-712 domain `name`/`version` to the on-chain contract's
    /// constructor args — `PaymentPool.sol` calls `EIP712("PaymentPool",
    /// "1")`. A divergence here makes every `redeem` revert with the
    /// contract's `InvalidVoucherSignature` even though off-chain
    /// `verify_signer` passes, so the literal is asserted directly rather than
    /// derived from `DOMAIN_NAME` (which would let a regression slip through).
    #[test]
    fn domain_matches_payment_pool_contract() {
        assert_eq!(
            DOMAIN_NAME, "PaymentPool",
            "voucher domain name must match PaymentPool.sol EIP712 ctor"
        );
        assert_eq!(
            DOMAIN_VERSION, "1",
            "voucher domain version must match PaymentPool.sol EIP712 ctor"
        );
    }
}

/// Property-based tests for the EIP-712 voucher core (#740). The example-based
/// `tests` module above pins fixed vectors and single-field tamper cases with
/// small values; these sweep the full `U256` keyspace for `amount` /
/// `bytes_delivered` with the dangerous boundaries (`0`, `u64::MAX`,
/// `U256::MAX`) heavily over-sampled (see [`any_u256`]), to catch any silent
/// truncation or accept-invalid path before mainnet. We trust `alloy` for the
/// EIP-712
/// primitive and assert the invariants the protocol depends on: sign→recover is
/// total over that range, the digest is deterministic and covers every field,
/// and the domain is binding.
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod prop_tests {
    use super::*;
    use alloy::signers::local::PrivateKeySigner;
    use proptest::prelude::*;

    /// A `U256` strategy that sweeps the full keyspace via uniform 32-byte
    /// fills while heavily over-sampling the boundary values — these are exactly
    /// where a `u64` truncation or an off-by-one would hide. Each boundary `Just`
    /// arm carries weight 1 against the random arm's 8, so over the default 256
    /// cases every boundary is hit with overwhelming (not certain) probability,
    /// while the random sweep still dominates.
    fn any_u256() -> impl Strategy<Value = U256> {
        prop_oneof![
            8 => proptest::array::uniform32(any::<u8>()).prop_map(U256::from_be_bytes),
            1 => Just(U256::ZERO),
            1 => Just(U256::from(1u64)),
            1 => Just(U256::from(u64::MAX)),
            1 => Just(U256::MAX),
        ]
    }

    fn any_address() -> impl Strategy<Value = Address> {
        proptest::array::uniform20(any::<u8>()).prop_map(Address::from)
    }

    fn any_b256() -> impl Strategy<Value = B256> {
        proptest::array::uniform32(any::<u8>()).prop_map(B256::from)
    }

    /// A valid secp256k1 signer from a random 32-byte scalar. Zero and
    /// out-of-range scalars fail `from_slice` and are dropped by
    /// `prop_filter_map`; both are vanishingly rare over a uniform draw, so this
    /// does not starve the case budget.
    fn any_signer() -> impl Strategy<Value = PrivateKeySigner> {
        proptest::array::uniform32(any::<u8>())
            .prop_filter_map("scalar must be a valid, non-zero secp256k1 key", |bytes| {
                PrivateKeySigner::from_slice(&bytes).ok()
            })
    }

    fn any_voucher() -> impl Strategy<Value = Voucher> {
        (
            any_b256(),
            any_address(),
            any_address(),
            any_u256(),
            any_u256(),
            any_b256(),
            any_u256(),
        )
            .prop_map(
                |(pool_id, signer, provider, amount, bytes_delivered, chain_root, chunk_price)| {
                    Voucher {
                        pool_id,
                        signer,
                        provider,
                        amount,
                        bytes_delivered,
                        chain_root,
                        chunk_price,
                    }
                },
            )
    }

    proptest! {
        /// Signing then recovering returns the signer's own address, and
        /// `verify_signer` accepts it — for every voucher across the full
        /// `U256` range, including `amount = 0` and
        /// `bytes_delivered = u64::MAX`. A truncation in the digest path would
        /// surface here as a recovered-address mismatch.
        #[test]
        fn sign_then_recover_is_the_signer(
            voucher in any_voucher(),
            signer in any_signer(),
            chain_id in any::<u64>(),
            verifying in any_address(),
        ) {
            let domain = voucher_domain(chain_id, verifying);
            let signed = voucher.sign(&signer, &domain).unwrap();
            prop_assert_eq!(signed.recover_signer(&domain).unwrap(), signer.address());
            prop_assert!(signed.verify_signer(signer.address(), &domain).is_ok());
        }

        /// The signing hash depends only on the field *values*, not on object
        /// identity: an independently reconstructed domain and a clone carrying
        /// the same values produce the same digest. This is referential
        /// transparency for `signing_hash` over the boundary-laden keyspace — a
        /// stronger statement than calling it twice on one struct.
        #[test]
        fn signing_hash_depends_only_on_field_values(
            voucher in any_voucher(),
            chain_id in any::<u64>(),
            verifying in any_address(),
        ) {
            let domain_a = voucher_domain(chain_id, verifying);
            let domain_b = voucher_domain(chain_id, verifying);
            let voucher_b = voucher.clone();
            prop_assert_eq!(voucher.signing_hash(&domain_a), voucher_b.signing_hash(&domain_b));
        }

        /// Changing any single field to a different value changes the digest —
        /// no field is silently dropped from the signed payload. Each mutated
        /// value is `prop_assume!`d distinct from the original up front, so
        /// every surviving case asserts all five bindings unconditionally (no
        /// field is skipped on a value collision).
        #[test]
        fn every_field_is_bound_into_the_digest(
            voucher in any_voucher(),
            chain_id in any::<u64>(),
            verifying in any_address(),
            other_pool in any_b256(),
            other_signer in any_address(),
            other_provider in any_address(),
            other_amount in any_u256(),
            other_bytes in any_u256(),
            other_root in any_b256(),
            other_price in any_u256(),
        ) {
            prop_assume!(other_pool != voucher.pool_id);
            prop_assume!(other_signer != voucher.signer);
            prop_assume!(other_provider != voucher.provider);
            prop_assume!(other_amount != voucher.amount);
            prop_assume!(other_bytes != voucher.bytes_delivered);
            prop_assume!(other_root != voucher.chain_root);
            prop_assume!(other_price != voucher.chunk_price);

            let domain = voucher_domain(chain_id, verifying);
            let base = voucher.signing_hash(&domain);

            let with_pool = Voucher { pool_id: other_pool, ..voucher.clone() };
            prop_assert_ne!(with_pool.signing_hash(&domain), base, "pool_id not bound");

            let with_signer = Voucher { signer: other_signer, ..voucher.clone() };
            prop_assert_ne!(with_signer.signing_hash(&domain), base, "signer not bound");

            let with_provider = Voucher { provider: other_provider, ..voucher.clone() };
            prop_assert_ne!(with_provider.signing_hash(&domain), base, "provider not bound");

            let with_amount = Voucher { amount: other_amount, ..voucher.clone() };
            prop_assert_ne!(with_amount.signing_hash(&domain), base, "amount not bound");

            let with_bytes = Voucher { bytes_delivered: other_bytes, ..voucher.clone() };
            prop_assert_ne!(with_bytes.signing_hash(&domain), base, "bytes_delivered not bound");

            // The two PayWord fields carry real money: `chain_root` decides
            // which released preimages extend this voucher at all, and
            // `chunk_price` decides what each one is worth. An unbound
            // `chunk_price` would let a payer re-price every metered chunk
            // after the fact against a signature the node already accepted.
            let with_root = Voucher { chain_root: other_root, ..voucher.clone() };
            prop_assert_ne!(with_root.signing_hash(&domain), base, "chain_root not bound");

            let with_price = Voucher { chunk_price: other_price, ..voucher.clone() };
            prop_assert_ne!(with_price.signing_hash(&domain), base, "chunk_price not bound");
        }

        /// The domain is binding: a voucher signed under one `(chain_id,
        /// verifying_contract)` does not verify under a different one. This is
        /// the "malformed / mismatched domain separator" guard from #740 —
        /// the domain is implicit in the EIP-712 digest, never serialized, so a
        /// domain change must move the digest and surface as `WrongSigner`.
        #[test]
        fn domain_is_binding(
            voucher in any_voucher(),
            signer in any_signer(),
            chain_a in any::<u64>(),
            verifying_a in any_address(),
            chain_b in any::<u64>(),
            verifying_b in any_address(),
        ) {
            prop_assume!((chain_a, verifying_a) != (chain_b, verifying_b));
            let domain_a = voucher_domain(chain_a, verifying_a);
            let domain_b = voucher_domain(chain_b, verifying_b);

            let signed = voucher.sign(&signer, &domain_a).unwrap();
            // The two domains differ, so the digests must differ. Assert that
            // explicitly: it makes the binding chain (different domain ⇒
            // different digest ⇒ different recovered address) visible, and turns
            // a cryptographically-negligible address collision into a clear
            // digest-equality failure rather than a confusing `WrongSigner` flake.
            prop_assert_ne!(
                signed.voucher.signing_hash(&domain_a),
                signed.voucher.signing_hash(&domain_b)
            );
            // A different digest means recovery lands on some other address —
            // `WrongSigner`, not `InvalidSignature` (the signature itself is
            // well-formed, so recovery always succeeds).
            let err = signed.verify_signer(signer.address(), &domain_b).unwrap_err();
            prop_assert!(
                matches!(err, VoucherError::WrongSigner { .. }),
                "expected WrongSigner under a mismatched domain, got {err:?}"
            );
        }
    }
}
