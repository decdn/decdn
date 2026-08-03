use alloy::primitives::{Address, B256};
use alloy::signers::Signature;
use std::str::FromStr;

#[derive(Debug, PartialEq, Eq)]
pub enum AuthError {
    Stale,
    BadSignature,
}

pub fn challenge_message(channel_id: B256, timestamp: u64) -> String {
    format!("decdn-sponsor topup {channel_id} {timestamp}")
}

pub fn verify(
    channel_id: B256,
    timestamp: u64,
    now: u64,
    sig_hex: &str,
    expected_signer: Address,
    max_skew_secs: u64,
) -> Result<(), AuthError> {
    let skew = now.abs_diff(timestamp);
    if skew > max_skew_secs {
        return Err(AuthError::Stale);
    }
    let sig = Signature::from_str(sig_hex).map_err(|_| AuthError::BadSignature)?;
    let msg = challenge_message(channel_id, timestamp);
    let recovered = sig
        .recover_address_from_msg(msg.as_bytes())
        .map_err(|_| AuthError::BadSignature)?;
    if recovered == expected_signer {
        Ok(())
    } else {
        Err(AuthError::BadSignature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::b256;
    use alloy::signers::{local::PrivateKeySigner, SignerSync};

    #[test]
    fn accepts_fresh_signature_from_expected_signer_rejects_others() {
        let signer = PrivateKeySigner::random();
        let addr = signer.address();
        let id = b256!("22222222222222222222222222222222222222222222222222222222222222ff");
        let ts = 1_769_904_000u64;
        let msg = challenge_message(id, ts);
        let sig = signer.sign_message_sync(msg.as_bytes()).unwrap();
        let sig_hex = format!("0x{}", hex::encode(sig.as_bytes()));

        // fresh + correct signer
        assert!(verify(id, ts, ts + 5, &sig_hex, addr, 120).is_ok());
        // stale
        assert!(matches!(verify(id, ts, ts + 999, &sig_hex, addr, 120), Err(AuthError::Stale)));
        // wrong expected signer
        let other = PrivateKeySigner::random().address();
        assert!(matches!(verify(id, ts, ts + 5, &sig_hex, other, 120), Err(AuthError::BadSignature)));
    }
}
