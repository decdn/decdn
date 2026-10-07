use super::*;

fn sample_body() -> ProbeResponseBody {
    ProbeResponseBody {
        hash: [7u8; 32],
        has_blob: true,
        rate_per_mb: 10,
        timestamp_us: 1_700_000_000_000_000,
    }
}

fn sample_response() -> ProbeResponse {
    ProbeResponse {
        body: sample_body(),
        slash_sig: vec![0xABu8; SLASH_SIG_LEN],
    }
}

fn sample_ext() -> ProbeResponseExt {
    ProbeResponseExt {
        total_bytes: Some(4096),
        coverage: crate::Coverage::from_block_indices(2, [0].into_iter()),
    }
}

#[test]
fn probe_request_roundtrip() -> Result<(), postcard::Error> {
    let req = ProbeRequest {
        hash: [9u8; 32],
        timestamp_us: 0xdead_beef,
    };
    let bytes = postcard::to_allocvec(&req)?;
    let decoded: ProbeRequest = postcard::from_bytes(&bytes)?;
    assert_eq!(req, decoded);
    Ok(())
}

#[test]
fn probe_response_roundtrip() -> Result<(), postcard::Error> {
    let resp = sample_response();
    let bytes = postcard::to_allocvec(&resp)?;
    let decoded: ProbeResponse = postcard::from_bytes(&bytes)?;
    assert_eq!(resp, decoded);
    Ok(())
}

/// Two-phase with an extension present: the base decodes, and the remainder
/// carries the ext.
#[test]
fn probe_response_two_phase_with_ext() -> Result<(), postcard::Error> {
    let resp = sample_response();
    let ext = sample_ext();
    let buf = encode_probe_response(&resp, Some(&ext))?;
    let (msg, remainder) = postcard::take_from_bytes::<ProbeMessage>(&buf)?;
    assert_eq!(msg, ProbeMessage::Response(resp));
    assert!(
        !remainder.is_empty(),
        "the ext must travel as trailing bytes"
    );
    assert_eq!(parse_probe_response_ext(remainder)?, ext);
    Ok(())
}

/// A sender that writes no extension leaves an empty remainder, which reads
/// back as the default rather than an error. This is the case an OLD sender
/// produces against a NEW receiver, and it is the whole reason the ext is a
/// separate postcard value.
#[test]
fn probe_response_two_phase_no_ext() -> Result<(), postcard::Error> {
    let resp = sample_response();
    let buf = encode_probe_response(&resp, None)?;
    let (msg, remainder) = postcard::take_from_bytes::<ProbeMessage>(&buf)?;
    assert_eq!(msg, ProbeMessage::Response(resp));
    assert!(remainder.is_empty());
    assert_eq!(
        parse_probe_response_ext(remainder)?,
        ProbeResponseExt::default()
    );
    Ok(())
}

/// The Tier-1 property itself: a NEWER sender appends a field this build does
/// not know, and the ext still parses. Without this the extension seam buys
/// nothing.
#[test]
fn probe_response_ext_tolerates_future_trailing_bytes() -> Result<(), postcard::Error> {
    let ext = sample_ext();
    let mut bytes = postcard::to_allocvec(&ext)?;
    bytes.extend_from_slice(&[0xAAu8, 0xBB, 0xCC]);
    assert_eq!(parse_probe_response_ext(&bytes)?, ext);
    Ok(())
}

#[test]
fn probe_message_request_discriminant_is_zero() -> Result<(), postcard::Error> {
    let msg = ProbeMessage::Request(ProbeRequest {
        hash: [0u8; 32],
        timestamp_us: 1,
    });
    let bytes = postcard::to_allocvec(&msg)?;
    assert_eq!(bytes.first().copied(), Some(0u8));
    let decoded: ProbeMessage = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn probe_message_response_discriminant_is_one() -> Result<(), postcard::Error> {
    let msg = ProbeMessage::Response(sample_response());
    let bytes = postcard::to_allocvec(&msg)?;
    assert_eq!(bytes.first().copied(), Some(1u8));
    let decoded: ProbeMessage = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn consistent_with_accepts_matching_pairs() {
    assert!(
        ProbeResponseExt {
            total_bytes: Some(4096),
            coverage: crate::Coverage::full(1),
        }
        .consistent_with(true),
        "has_blob:true + non-empty coverage must be consistent"
    );
    assert!(
        ProbeResponseExt::default().consistent_with(false),
        "has_blob:false + empty (default) coverage must be consistent"
    );
}

#[test]
fn consistent_with_rejects_mismatched_pairs() {
    assert!(
        !ProbeResponseExt {
            total_bytes: Some(4096),
            coverage: crate::Coverage::full(1),
        }
        .consistent_with(false),
        "has_blob:false with non-empty coverage must be flagged inconsistent"
    );
    assert!(
        !ProbeResponseExt::default().consistent_with(true),
        "has_blob:true with empty coverage must be flagged inconsistent"
    );
}

#[test]
fn probe_message_rejects_unknown_discriminant() {
    let bytes = [99u8, 0, 0, 0, 0];
    let r: Result<ProbeMessage, _> = postcard::from_bytes(&bytes);
    assert!(r.is_err());
}

// Pins `TopLevelEnum::VARIANT_COUNT` to the actual highest discriminant so a
// future variant addition (which shifts the unknown/known boundary the ADR
// 013 classifier relies on) must update the count in lockstep.
#[test]
fn probe_message_variant_count_matches_discriminants() -> Result<(), postcard::Error> {
    use crate::framing::TopLevelEnum;
    assert_eq!(ProbeMessage::VARIANT_COUNT, 2);
    // The last declared variant (`Response`) must encode to discriminant
    // VARIANT_COUNT - 1. Compare against postcard's own varint encoding of
    // that index (not `bytes.first()`) so the pin stays correct even if the
    // enum ever grows a multi-byte discriminant (> 127 variants).
    let last = ProbeMessage::Response(sample_response());
    let bytes = postcard::to_allocvec(&last)?;
    let expected_disc = postcard::to_allocvec(&(ProbeMessage::VARIANT_COUNT - 1))?;
    assert!(bytes.starts_with(&expected_disc));
    Ok(())
}

#[test]
fn probe_message_unknown_discriminant_is_flagged_unsupported() {
    // Discriminant 2 is the first index past the known set → UNSUPPORTED.
    assert!(crate::is_unknown_variant::<ProbeMessage>(&[2u8, 0, 0]));
    // Discriminant 1 (Response) is known — an over-cap-rate decode failure
    // on it must stay MALFORMED, not flip to UNSUPPORTED.
    assert!(!crate::is_unknown_variant::<ProbeMessage>(&[1u8, 0xFF]));
}

// Issue #378: the wire boundary MUST reject `rate_per_mb` above
// MAX_RATE_PER_MB so a malicious peer cannot feed an overflow-inducing
// value into the client selection score. The hook lives on the
// signed body field.
#[test]
fn probe_response_body_decode_rejects_rate_above_max() -> Result<(), postcard::Error> {
    let body = ProbeResponseBody {
        rate_per_mb: MAX_RATE_PER_MB + 1,
        ..sample_body()
    };
    let bytes = postcard::to_allocvec(&body)?;
    let decoded: Result<ProbeResponseBody, _> = postcard::from_bytes(&bytes);
    assert!(decoded.is_err(), "expected decode rejection");
    Ok(())
}

#[test]
fn probe_response_decode_rejects_u64_max_rate() -> Result<(), postcard::Error> {
    let resp = ProbeResponse {
        body: ProbeResponseBody {
            rate_per_mb: u64::MAX,
            ..sample_body()
        },
        ..sample_response()
    };
    let bytes = postcard::to_allocvec(&resp)?;
    let decoded: Result<ProbeResponse, _> = postcard::from_bytes(&bytes);
    assert!(decoded.is_err(), "expected decode rejection for u64::MAX");
    Ok(())
}

// Boundary holds: exactly MAX_RATE_PER_MB must round-trip cleanly,
// confirming the rejection above is on `>`, not `>=`.
#[test]
fn probe_response_decode_accepts_rate_at_max() -> Result<(), postcard::Error> {
    let resp = ProbeResponse {
        body: ProbeResponseBody {
            rate_per_mb: MAX_RATE_PER_MB,
            ..sample_body()
        },
        ..sample_response()
    };
    let bytes = postcard::to_allocvec(&resp)?;
    let decoded: ProbeResponse = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded.body.rate_per_mb, MAX_RATE_PER_MB);
    Ok(())
}

#[test]
fn probe_message_response_decode_rejects_oversize_rate() -> Result<(), postcard::Error> {
    let resp = ProbeResponse {
        body: ProbeResponseBody {
            rate_per_mb: MAX_RATE_PER_MB + 1,
            ..sample_body()
        },
        ..sample_response()
    };
    let msg = ProbeMessage::Response(resp);
    let bytes = postcard::to_allocvec(&msg)?;
    let decoded: Result<ProbeMessage, _> = postcard::from_bytes(&bytes);
    assert!(
        decoded.is_err(),
        "ProbeMessage decode must propagate ProbeResponseBody validation"
    );
    Ok(())
}

#[test]
fn probe_response_validate_is_consistent_with_decode() {
    let bad = ProbeResponse {
        body: ProbeResponseBody {
            rate_per_mb: MAX_RATE_PER_MB + 1,
            ..sample_body()
        },
        ..sample_response()
    };
    assert_eq!(
        bad.validate(),
        Err(MessageValidationError::RateTooLarge {
            rate: MAX_RATE_PER_MB + 1,
        })
    );

    let good = sample_response();
    assert_eq!(good.validate(), Ok(()));
}

// #252: a requester calling `validate()` must reject a zero rate. The
// decode path accepts it (zero is a valid u64 ≤ MAX), so the obligation
// lives in the requester-side `validate()` — pin it here.
#[test]
fn probe_response_validate_rejects_zero_rate() {
    let resp = ProbeResponse {
        body: ProbeResponseBody {
            rate_per_mb: 0,
            ..sample_body()
        },
        ..sample_response()
    };
    assert_eq!(resp.validate(), Err(MessageValidationError::RateIsZero));
}

#[test]
fn probe_response_validate_rejects_empty_slash_sig() {
    let resp = ProbeResponse {
        slash_sig: Vec::new(),
        ..sample_response()
    };
    assert_eq!(
        resp.validate(),
        Err(MessageValidationError::InvalidSlashSigLen { len: 0 })
    );
}

#[test]
fn probe_response_validate_rejects_wrong_length_slash_sig() {
    // A non-empty but too-short signature must also be rejected — the
    // public helper has to be as strict as the wire invariant
    // (SLASH_SIG_LEN), not merely "non-empty".
    let resp = ProbeResponse {
        slash_sig: vec![0xAB; SLASH_SIG_LEN - 1],
        ..sample_response()
    };
    assert_eq!(
        resp.validate(),
        Err(MessageValidationError::InvalidSlashSigLen {
            len: SLASH_SIG_LEN - 1
        })
    );
}

#[test]
fn probe_response_trailing_bytes_tolerated() -> Result<(), postcard::Error> {
    // ADR 013: `take_from_bytes` silently ignores trailing bytes so future
    // unknown unsigned fields don't break old decoders.
    let resp = sample_response();
    let mut bytes = postcard::to_allocvec(&resp)?;
    bytes.extend_from_slice(&[0xAAu8, 0xBB, 0xCC]);
    let (decoded, tail) = postcard::take_from_bytes::<ProbeResponse>(&bytes)?;
    assert_eq!(decoded, resp);
    assert_eq!(tail, &[0xAAu8, 0xBB, 0xCC]);
    Ok(())
}

// Wire-format guard: the signed-body split must not silently change the
// on-wire layout. If postcard's bytes for a ProbeResponse change, this
// fixed-byte assertion catches it before the change ships.
// SLASH_SIG_LEN (65) fits a u8 and a single postcard varint byte; the
// cast is exact and asserted by this very test.
#[allow(clippy::cast_possible_truncation)]
#[test]
fn probe_response_wire_format_is_stable() -> Result<(), postcard::Error> {
    let resp = ProbeResponse {
        body: ProbeResponseBody {
            hash: [3u8; 32],
            has_blob: true,
            rate_per_mb: 4,
            timestamp_us: 5,
        },
        slash_sig: vec![0xABu8; SLASH_SIG_LEN],
    };
    let bytes = postcard::to_allocvec(&resp)?;
    // postcard layout: body{ hash=32 raw, has_blob=1 byte (0x01),
    // rate_per_mb=4 (1-byte varint), timestamp_us=5 (1-byte varint) },
    // slash_sig Vec (len varint 65=0x41, then 65 bytes). Nothing unsigned
    // rides here — the extension is a SEPARATE postcard value appended by
    // `encode_probe_response`, asserted below.
    let mut expected = Vec::with_capacity(32 + 1 + 1 + 1 + 1 + SLASH_SIG_LEN);
    expected.extend_from_slice(&[3u8; 32]); // body.hash
    expected.push(1u8); // body.has_blob = true
    expected.push(4u8); // body.rate_per_mb varint
    expected.push(5u8); // body.timestamp_us varint
    expected.push(SLASH_SIG_LEN as u8); // slash_sig length prefix (65)
    expected.extend_from_slice(&[0xABu8; SLASH_SIG_LEN]); // slash_sig bytes
    assert_eq!(bytes, expected);

    // The full frame is base ‖ ext, with the ext contributing exactly its own
    // bytes and nothing to the base. A reader that stops after the base sees
    // a byte-identical message either way, which is the forward-compatibility
    // guarantee stated as bytes.
    let framed = encode_probe_response(
        &resp,
        Some(&ProbeResponseExt {
            total_bytes: Some(7),
            coverage: crate::Coverage::empty(),
        }),
    )?;
    let mut expected_framed = vec![1u8]; // ProbeMessage::Response discriminant
    expected_framed.extend_from_slice(&expected);
    expected_framed.push(1u8); // total_bytes = Some
    expected_framed.push(7u8); // total_bytes varint
    expected_framed.push(0u8); // coverage: empty Vec<u8> length-prefix (0)
    assert_eq!(framed, expected_framed);
    Ok(())
}
