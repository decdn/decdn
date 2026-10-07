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
