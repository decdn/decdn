use super::*;
use crate::voucher::voucher_domain;
use alloy::primitives::address;
use alloy::signers::local::PrivateKeySigner;
use proptest::prelude::*;

const PROVIDER: Address = address!("00000000000000000000000000000000000000b2");
const VERIFYING: Address = address!("0000000000000000000000000000000000001234");

fn any_signer() -> impl Strategy<Value = PrivateKeySigner> {
    proptest::array::uniform32(any::<u8>())
        .prop_filter_map("scalar must be a valid, non-zero secp256k1 key", |bytes| {
            PrivateKeySigner::from_slice(&bytes).ok()
        })
}

proptest! {
    /// `signed → wire → signed` reproduces the signed voucher exactly when
    /// the off-wire context (`pool_id`, `signer`, `provider`) is
    /// re-supplied — proving the `U256 ↔ u64` narrowing/widening is
    /// lossless across the whole `u64` range, for `chunk_price` as much as
    /// for the two cumulatives, and that `chain_root` survives the
    /// `B256 ↔ [u8; 32]` hop untouched.
    #[test]
    fn wire_round_trip_is_lossless(
        pool_id in proptest::array::uniform32(any::<u8>()).prop_map(B256::from),
        amount in any::<u64>().prop_map(U256::from),
        bytes_delivered in any::<u64>().prop_map(U256::from),
        chain_root in proptest::array::uniform32(any::<u8>()).prop_map(B256::from),
        chunk_price in any::<u64>().prop_map(U256::from),
        signer in any_signer(),
    ) {
        let domain = voucher_domain(421_614, VERIFYING);
        let signed = Voucher {
            pool_id,
            signer: signer.address(),
            provider: PROVIDER,
            amount,
            bytes_delivered,
            chain_root,
            chunk_price,
        }
            .sign(&signer, &domain)
            .unwrap();

        let wire = signed_to_wire_voucher(&signed).unwrap();
        prop_assert_eq!(wire.amount, u64::try_from(amount).unwrap());
        prop_assert_eq!(wire.bytes_delivered, u64::try_from(bytes_delivered).unwrap());
        prop_assert_eq!(B256::from(wire.chain_root), chain_root);
        prop_assert_eq!(wire.chunk_price, u64::try_from(chunk_price).unwrap());

        let rebuilt =
            wire_voucher_to_signed(&wire, pool_id, signer.address(), PROVIDER)
                .unwrap();
        prop_assert_eq!(&rebuilt, &signed, "bridge must preserve the signed voucher");
        prop_assert!(rebuilt.verify_signer(signer.address(), &domain).is_ok());
    }

    /// Decoding never panics on arbitrary input, and any signature whose
    /// length is not `VOUCHER_SIG_LEN` is rejected as `BadSignature` rather
    /// than parsed or crashed.
    #[test]
    fn malformed_wire_signature_is_rejected_not_panicked(
        signature in prop::collection::vec(any::<u8>(), 0..200),
        amount in any::<u64>(),
    ) {
        let wire = WireVoucher {
            signature: signature.clone(),
            amount,
            bytes_delivered: 0u64,
            chain_root: [0u8; 32],
            chunk_price: 0u64,
        };
        let result = wire_voucher_to_signed(&wire, B256::ZERO, PROVIDER, PROVIDER);
        if signature.len() != decdn_protocol::VOUCHER_SIG_LEN {
            prop_assert_eq!(result.err(), Some(WireVoucherError::BadSignature));
        }
    }
}
