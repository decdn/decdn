use super::*;
use crate::framing::{decode_message, encode_message, read_frame, write_frame};
use std::assert_matches;

fn sample_body() -> StreamResponseBody {
    StreamResponseBody {
        hash: [7u8; 32],
        ok: true,
        rate_per_mb: 10,
        total_bytes: 4096,
        pool_id: [9u8; 32],
        timestamp_us: 1_700_000_000_000_000,
    }
}

fn sample_response() -> StreamResponse {
    StreamResponse {
        body: sample_body(),
        slash_sig: vec![0xABu8; SLASH_SIG_LEN],
    }
}

fn sample_request() -> StreamRequest {
    StreamRequest {
        hash: [1u8; 32],
        namespace_id: [3u8; 32],
        pool_id: [2u8; 32],
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0xdead_beef,
    }
}

fn sample_binding() -> ClientBinding {
    ClientBinding {
        ethereum_address: [0xEEu8; 20],
        binding_signature: vec![0x01u8; BINDING_SIG_LEN],
    }
}

fn sample_ext() -> StreamRequestExt {
    StreamRequestExt {
        binding: Some(sample_binding()),
        capability: Some(sample_capability()),
    }
}

fn sample_capability() -> WireCapability {
    WireCapability {
        spending_cap: 0x2222_2222_2222_2222,
        expiry: 1_800_000_000,
        owner_signature: vec![0x03u8; VOUCHER_SIG_LEN],
    }
}

fn sample_voucher() -> Voucher {
    Voucher {
        signature: vec![0xCDu8; VOUCHER_SIG_LEN],
        amount: 0x1111_1111_1111_1111u64,
        bytes_delivered: 0x2222_2222_2222_2222u64,
        chain_root: [0x55u8; 32],
        chunk_price: 0x6666_6666_6666_6666u64,
    }
}

fn sample_preimage() -> ChunkPreimage {
    ChunkPreimage {
        preimage: [0x77u8; 32],
        index: 42,
    }
}

// --- Roundtrips ----------------------------------------------------------

#[test]
fn stream_request_roundtrip() -> Result<(), postcard::Error> {
    let req = sample_request();
    let bytes = postcard::to_allocvec(&req)?;
    let decoded: StreamRequest = postcard::from_bytes(&bytes)?;
    assert_eq!(req, decoded);
    Ok(())
}

/// Two-phase: a request with no ext encodes to just the `ClientMessage`,
/// and `decode_message` yields an empty remainder → `Ext::default()`.
#[test]
fn stream_request_two_phase_no_ext() -> Result<(), crate::framing::FrameError> {
    let req = sample_request();
    let payload = encode_stream_request(&req, None)?;
    let (msg, remainder) = decode_message::<ClientMessage>(&payload)?;
    assert_eq!(msg, ClientMessage::StreamRequest(req));
    assert!(remainder.is_empty(), "no ext ⇒ no trailing bytes");
    assert_eq!(
        parse_stream_request_ext(remainder)?,
        StreamRequestExt::default()
    );
    Ok(())
}

/// Two-phase: a request with ext appends the extension bytes after the
/// message; `decode_message` returns them as the remainder and
/// `parse_stream_request_ext` recovers the ext.
#[test]
fn stream_request_two_phase_with_ext() -> Result<(), crate::framing::FrameError> {
    let req = sample_request();
    let ext = sample_ext();
    let payload = encode_stream_request(&req, Some(&ext))?;
    let (msg, remainder) = decode_message::<ClientMessage>(&payload)?;
    assert_eq!(msg, ClientMessage::StreamRequest(req));
    assert!(!remainder.is_empty(), "ext present ⇒ trailing bytes");
    assert_eq!(parse_stream_request_ext(remainder)?, ext);
    Ok(())
}

/// Forward compatibility (ADR 013 §Tier 1): a future field appended to
/// `StreamRequestExt` shows up as extra trailing bytes; an old parser reads
/// the known fields and ignores the rest rather than failing on EOF. This is
/// exactly what embedding `ext` as a struct field would have broken.
#[test]
fn stream_request_ext_tolerates_future_trailing_bytes() -> Result<(), postcard::Error> {
    let ext = sample_ext();
    let mut bytes = postcard::to_allocvec(&ext)?;
    bytes.extend_from_slice(&[0xAAu8, 0xBB, 0xCC]); // simulated future field
    assert_eq!(parse_stream_request_ext(&bytes)?, ext);
    Ok(())
}

/// `capability` round-trips both present and absent, independent of
/// `binding` (the wire fields are orthogonal — a registered on-chain
/// client can still carry a capability, and vice versa).
#[test]
fn stream_request_ext_capability_roundtrip() -> Result<(), postcard::Error> {
    let with_cap = StreamRequestExt {
        binding: None,
        capability: Some(sample_capability()),
    };
    let bytes = postcard::to_allocvec(&with_cap)?;
    let decoded: StreamRequestExt = postcard::from_bytes(&bytes)?;
    assert_eq!(with_cap, decoded);

    let without_cap = StreamRequestExt {
        capability: None,
        ..with_cap
    };
    let bytes = postcard::to_allocvec(&without_cap)?;
    let decoded: StreamRequestExt = postcard::from_bytes(&bytes)?;
    assert_eq!(without_cap, decoded);
    Ok(())
}

#[test]
fn wire_capability_rejects_empty_signature() {
    let cap = WireCapability {
        spending_cap: 0,
        expiry: 0,
        owner_signature: Vec::new(),
    };
    assert_eq!(
        cap.validate(),
        Err(MessageValidationError::EmptyCapabilitySignature)
    );
}

#[test]
fn stream_request_ext_validate_rejects_empty_capability_signature() {
    let ext = StreamRequestExt {
        binding: None,
        capability: Some(WireCapability {
            spending_cap: 0,
            expiry: 0,
            owner_signature: Vec::new(),
        }),
    };
    assert_eq!(
        ext.validate(),
        Err(MessageValidationError::EmptyCapabilitySignature)
    );
}

#[test]
fn stream_response_roundtrip() -> Result<(), postcard::Error> {
    let resp = sample_response();
    let bytes = postcard::to_allocvec(&resp)?;
    let decoded: StreamResponse = postcard::from_bytes(&bytes)?;
    assert_eq!(resp, decoded);
    Ok(())
}

#[test]
fn stream_response_error_roundtrip() -> Result<(), postcard::Error> {
    let resp = StreamResponse {
        body: StreamResponseBody {
            ok: false,
            ..sample_body()
        },
        ..sample_response()
    };
    let ext = StreamResponseExt {
        error: Some(StreamError::Declined),
    };
    let buf = encode_stream_response(&resp, Some(&ext))?;
    let (decoded, remainder) = postcard::take_from_bytes::<ClientMessage>(&buf)?;
    assert_eq!(decoded, ClientMessage::StreamResponse(resp));
    assert_eq!(parse_stream_response_ext(remainder)?, ext);
    Ok(())
}

#[test]
fn chunk_data_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let chunk = ChunkData::new(vec![0x42u8; 4096])?;
    let bytes = postcard::to_allocvec(&chunk)?;
    let decoded: ChunkData = postcard::from_bytes(&bytes)?;
    assert_eq!(chunk, decoded);
    Ok(())
}

#[test]
fn voucher_roundtrip() -> Result<(), postcard::Error> {
    let v = sample_voucher();
    let bytes = postcard::to_allocvec(&v)?;
    let decoded: Voucher = postcard::from_bytes(&bytes)?;
    assert_eq!(v, decoded);
    Ok(())
}

#[test]
fn chunk_preimage_roundtrip() -> Result<(), postcard::Error> {
    let p = sample_preimage();
    let bytes = postcard::to_allocvec(&p)?;
    let decoded: ChunkPreimage = postcard::from_bytes(&bytes)?;
    assert_eq!(p, decoded);
    Ok(())
}

/// ADR 005 §Payment quantum: `preimage ‖ index`, **33 bytes flat** — no
/// length prefix and no varint. The byte IS the index. This is the property
/// that lets the wire index and the on-chain packed `chainMeter` low byte
/// agree with no offset on send and no increment on receipt, so pin the
/// exact bytes rather than only the round-trip.
#[test]
fn chunk_preimage_wire_format_is_stable() -> Result<(), postcard::Error> {
    let p = ChunkPreimage {
        preimage: [0xA5u8; 32],
        index: MAX_CHAIN_LENGTH,
    };
    let bytes = postcard::to_allocvec(&p)?;
    let mut expected = Vec::new();
    expected.extend_from_slice(&[0xA5u8; 32]); // preimage (raw, no prefix)
    expected.push(255u8); // index (one byte, as itself)
    assert_eq!(bytes, expected);
    assert_eq!(bytes.len(), 33);
    Ok(())
}

/// `index == 0` decodes cleanly and passes `validate()`. The rejection is
/// the delivery handler's, in band as `ChainIndexZero` — if this ever
/// starts failing at decode, the payer loses the reason and sees only a
/// closed stream.
#[test]
fn chunk_preimage_index_zero_decodes_and_validates() -> Result<(), postcard::Error> {
    let p = ChunkPreimage {
        preimage: [0u8; 32],
        index: 0,
    };
    let bytes = postcard::to_allocvec(&ClientMessage::ChunkPreimage(p))?;
    let decoded: ClientMessage = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded.validate(), Ok(()));
    Ok(())
}

#[test]
fn stream_error_voucher_rejected_roundtrip() -> Result<(), postcard::Error> {
    let e = StreamError::VoucherRejected {
        reason: VoucherRejectReason::SpendingCapExhausted,
        bundle: None,
    };
    let bytes = postcard::to_allocvec(&e)?;
    let decoded: StreamError = postcard::from_bytes(&bytes)?;
    assert_eq!(e, decoded);
    Ok(())
}

/// The gated case: a `WatermarkBundle` rides alongside the reject reason
/// (issue #1481). Round-trips `Some` distinctly from the `None` case above
/// — this is the shape a wallet-less client actually receives on a gated
/// regression/exhaustion reject.
#[test]
fn stream_error_voucher_rejected_with_bundle_roundtrip() -> Result<(), postcard::Error> {
    let e = StreamError::VoucherRejected {
        reason: VoucherRejectReason::SpendingCapExhausted,
        bundle: Some(WatermarkBundle {
            amount: 0x1111_1111_1111_1111u64,
            bytes_delivered: 0x3333_3333_3333_3333u64,
            chain_root: [0x55u8; 32],
            verified_index: 7,
            tip: [0x66u8; 32],
            chunk_price: 0x7777_7777_7777_7777u64,
            last_signature: vec![0x44u8; VOUCHER_SIG_LEN],
        }),
    };
    let bytes = postcard::to_allocvec(&e)?;
    let decoded: StreamError = postcard::from_bytes(&bytes)?;
    assert_eq!(e, decoded);
    Ok(())
}

// --- Discriminant pins (FROZEN order) ------------------------------------

fn first_byte<T: Serialize>(v: &T) -> Result<u8, postcard::Error> {
    Ok(postcard::to_allocvec(v)?.first().copied().unwrap_or(0xFF))
}

#[test]
fn client_message_discriminants_are_frozen() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        first_byte(&ClientMessage::StreamRequest(sample_request()))?,
        0
    );
    assert_eq!(
        first_byte(&ClientMessage::StreamResponse(sample_response()))?,
        1
    );
    assert_eq!(
        first_byte(&ClientMessage::ChunkData(ChunkData::new(vec![0x1u8])?))?,
        2
    );
    assert_eq!(first_byte(&ClientMessage::Voucher(sample_voucher()))?, 3);
    assert_eq!(
        first_byte(&ClientMessage::ChunkPreimage(sample_preimage()))?,
        4
    );
    assert_eq!(first_byte(&ClientMessage::StreamEnd)?, 5);
    assert_eq!(
        first_byte(&ClientMessage::StreamError(StreamError::NotFound))?,
        6
    );
    Ok(())
}

#[test]
fn stream_error_variant_order_is_frozen() -> Result<(), postcard::Error> {
    for (i, e) in [
        StreamError::NotFound,
        StreamError::Declined,
        StreamError::Unfunded,
        StreamError::VoucherRejected {
            reason: VoucherRejectReason::BadSignature,
            bundle: None,
        },
    ]
    .into_iter()
    .enumerate()
    {
        // Pin the discriminant AND decode-roundtrip each variant's payload —
        // a first-byte-only check would miss a payload-shape regression.
        let bytes = postcard::to_allocvec(&e)?;
        assert_eq!(
            bytes.first().copied(),
            Some(u8::try_from(i).unwrap_or(0xFF))
        );
        let decoded: StreamError = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, e);
    }
    Ok(())
}

#[test]
fn voucher_reject_reason_variant_order_is_frozen() -> Result<(), postcard::Error> {
    for (i, r) in [
        VoucherRejectReason::BadSignature,
        VoucherRejectReason::WrongSigner,
        VoucherRejectReason::WrongPool,
        VoucherRejectReason::WrongProvider,
        VoucherRejectReason::AmountRegression,
        VoucherRejectReason::BytesRegression,
        VoucherRejectReason::SpendingCapExhausted,
        VoucherRejectReason::CapabilityExpired,
        VoucherRejectReason::PoolExhausted,
        VoucherRejectReason::SignerCapExhausted,
        VoucherRejectReason::BadPreimage,
        VoucherRejectReason::ChainIndexZero,
        VoucherRejectReason::UnanchoredPreimage,
        VoucherRejectReason::ChunkPriceMismatch,
        VoucherRejectReason::Underpaid,
        VoucherRejectReason::UnderFold,
    ]
    .into_iter()
    .enumerate()
    {
        let bytes = postcard::to_allocvec(&r)?;
        assert_eq!(
            bytes.first().copied(),
            Some(u8::try_from(i).unwrap_or(0xFF))
        );
        let decoded: VoucherRejectReason = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, r);
    }
    Ok(())
}

/// Exactly the reasons a watermark resync can repair carry a bundle.
/// `Underpaid` is one: an honest payer reaches it only when its local
/// watermark ran ahead of the node's.
#[test]
fn watermark_gated_reasons_are_the_resyncable_ones() {
    use VoucherRejectReason as R;
    for r in [
        R::AmountRegression,
        R::BytesRegression,
        R::SpendingCapExhausted,
        R::Underpaid,
        R::UnderFold,
    ] {
        assert!(r.is_watermark_gated(), "{r:?} must be watermark-gated");
    }
    for r in [
        R::BadSignature,
        R::WrongSigner,
        R::WrongPool,
        R::WrongProvider,
        R::CapabilityExpired,
        R::PoolExhausted,
        R::SignerCapExhausted,
        R::BadPreimage,
        R::ChainIndexZero,
        R::UnanchoredPreimage,
        R::ChunkPriceMismatch,
    ] {
        assert!(!r.is_watermark_gated(), "{r:?} must not be watermark-gated");
    }
}

#[test]
fn client_message_rejects_unknown_discriminant() {
    let bytes = [99u8, 0, 0, 0, 0];
    let r: Result<ClientMessage, _> = postcard::from_bytes(&bytes);
    assert!(r.is_err());
}

// Pins `TopLevelEnum::VARIANT_COUNT` to the highest discriminant so a future
// variant addition must update the count the ADR 013 unknown/known
// classifier relies on.
#[test]
fn client_message_variant_count_matches_discriminants() -> Result<(), postcard::Error> {
    use crate::framing::TopLevelEnum;
    assert_eq!(ClientMessage::VARIANT_COUNT, 7);
    // The last declared variant (`StreamError`) must encode to discriminant
    // VARIANT_COUNT - 1. Compare against postcard's own varint encoding of
    // that index (not `first_byte`/`bytes.first()`) so the pin survives a
    // future multi-byte discriminant (> 127 variants).
    let last = ClientMessage::StreamError(StreamError::NotFound);
    let bytes = postcard::to_allocvec(&last)?;
    let expected_disc = postcard::to_allocvec(&(ClientMessage::VARIANT_COUNT - 1))?;
    assert!(bytes.starts_with(&expected_disc));
    Ok(())
}

#[test]
fn client_message_unknown_discriminant_is_flagged_unsupported() {
    // Discriminant 7 is the first index past the known set → UNSUPPORTED.
    assert!(crate::is_unknown_variant::<ClientMessage>(&[7u8, 0, 0]));
    // A known in-range discriminant (1 = StreamResponse) with a bad payload
    // stays MALFORMED.
    assert!(!crate::is_unknown_variant::<ClientMessage>(&[1u8, 0xFF]));
}

// --- Wire-format stability (fixed bytes) ---------------------------------

#[allow(clippy::cast_possible_truncation)]
#[test]
fn stream_response_wire_format_is_stable() -> Result<(), postcard::Error> {
    let resp = StreamResponse {
        body: StreamResponseBody {
            hash: [3u8; 32],
            ok: true,
            rate_per_mb: 4,
            total_bytes: 5,
            pool_id: [6u8; 32],
            timestamp_us: 7,
        },
        slash_sig: vec![0xABu8; SLASH_SIG_LEN],
    };
    let bytes = postcard::to_allocvec(&resp)?;
    let mut expected = Vec::new();
    expected.extend_from_slice(&[3u8; 32]); // body.hash
    expected.push(1u8); // body.ok = true
    expected.push(4u8); // body.rate_per_mb varint
    expected.push(5u8); // body.total_bytes varint
    expected.extend_from_slice(&[6u8; 32]); // body.pool_id
    expected.push(7u8); // body.timestamp_us varint
    expected.push(SLASH_SIG_LEN as u8); // slash_sig length prefix (65)
    expected.extend_from_slice(&[0xABu8; SLASH_SIG_LEN]); // slash_sig bytes
    assert_eq!(bytes, expected);

    // Base ‖ ext, with the ext contributing only its own bytes: a reader that
    // stops after the base sees identical bytes whether or not an extension
    // followed.
    let framed = encode_stream_response(
        &resp,
        Some(&StreamResponseExt {
            error: Some(StreamError::NotFound),
        }),
    )?;
    let mut expected_framed = vec![1u8]; // ClientMessage::StreamResponse discriminant
    expected_framed.extend_from_slice(&expected);
    expected_framed.push(1u8); // error = Some
    expected_framed.push(0u8); // StreamError::NotFound discriminant
    assert_eq!(framed, expected_framed);
    Ok(())
}

#[allow(clippy::cast_possible_truncation)]
#[test]
fn voucher_wire_format_is_stable() -> Result<(), postcard::Error> {
    let v = Voucher {
        signature: vec![0xCDu8; VOUCHER_SIG_LEN],
        amount: 1u64,
        bytes_delivered: 2u64,
        chain_root: [0xABu8; 32],
        chunk_price: 3u64,
    };
    let bytes = postcard::to_allocvec(&v)?;
    let mut expected = Vec::new();
    expected.push(VOUCHER_SIG_LEN as u8); // signature length prefix (65)
    expected.extend_from_slice(&[0xCDu8; VOUCHER_SIG_LEN]); // signature
    expected.push(1u8); // amount (varint)
    expected.push(2u8); // bytes_delivered (varint)
    expected.extend_from_slice(&[0xABu8; 32]); // chain_root (raw, no prefix)
    expected.push(3u8); // chunk_price (varint)
    assert_eq!(bytes, expected);
    Ok(())
}

// --- Validation ----------------------------------------------------------

#[test]
fn stream_response_validate_accepts_sample() {
    assert_eq!(sample_response().validate(), Ok(()));
}

#[test]
fn stream_response_validate_rejects_zero_rate() {
    let resp = StreamResponse {
        body: StreamResponseBody {
            rate_per_mb: 0,
            ..sample_body()
        },
        ..sample_response()
    };
    assert_eq!(resp.validate(), Err(MessageValidationError::RateIsZero));
}

#[test]
fn stream_response_validate_rejects_empty_slash_sig() {
    let resp = StreamResponse {
        slash_sig: Vec::new(),
        ..sample_response()
    };
    assert_eq!(
        resp.validate(),
        Err(MessageValidationError::InvalidSlashSigLen { len: 0 })
    );
}

#[test]
fn stream_response_validate_rejects_wrong_len_slash_sig() {
    let resp = StreamResponse {
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
fn stream_response_decode_rejects_oversize_rate() -> Result<(), postcard::Error> {
    let resp = StreamResponse {
        body: StreamResponseBody {
            rate_per_mb: MAX_RATE_PER_MB + 1,
            ..sample_body()
        },
        ..sample_response()
    };
    let bytes = postcard::to_allocvec(&resp)?;
    let decoded: Result<StreamResponse, _> = postcard::from_bytes(&bytes);
    assert!(decoded.is_err(), "decode must reject rate above MAX");
    Ok(())
}

#[test]
fn voucher_validate_rejects_wrong_len_signature() {
    let v = Voucher {
        signature: vec![0xCD; VOUCHER_SIG_LEN - 1],
        ..sample_voucher()
    };
    assert_eq!(
        v.validate(),
        Err(MessageValidationError::InvalidVoucherSigLen {
            len: VOUCHER_SIG_LEN - 1
        })
    );
    assert_eq!(sample_voucher().validate(), Ok(()));
}

/// A malformed/truncated bundle must fail its own shape check before any
/// caller trusts it enough to reseed a ledger (issue #1481 review).
#[test]
fn watermark_bundle_validate_rejects_wrong_len_signature() {
    let b = WatermarkBundle {
        amount: 0u64,
        bytes_delivered: 0u64,
        chain_root: [0u8; 32],
        verified_index: 0,
        tip: [0u8; 32],
        chunk_price: 0u64,
        last_signature: vec![0xCDu8; VOUCHER_SIG_LEN - 1],
    };
    assert_eq!(
        b.validate(),
        Err(MessageValidationError::InvalidVoucherSigLen {
            len: VOUCHER_SIG_LEN - 1
        })
    );
    let ok = WatermarkBundle {
        last_signature: vec![0xCDu8; VOUCHER_SIG_LEN],
        ..b
    };
    assert_eq!(ok.validate(), Ok(()));
}

// --- Edge-case roundtrips ------------------------------------------------

#[test]
fn stream_request_nonzero_byte_offset_roundtrip() -> Result<(), postcard::Error> {
    // byte_offset != 0 exercises the multi-byte varint path (resume/seek);
    // the shared sample uses 0, a single byte.
    let req = StreamRequest {
        byte_offset: 1_048_576,
        ..sample_request()
    };
    let bytes = postcard::to_allocvec(&req)?;
    let decoded: StreamRequest = postcard::from_bytes(&bytes)?;
    assert_eq!(req, decoded);
    assert_eq!(decoded.byte_offset, 1_048_576);
    Ok(())
}

#[test]
fn stream_request_bounded_byte_len_roundtrip() -> Result<(), postcard::Error> {
    // A bounded range [byte_offset, byte_offset + byte_len) (ADR 005 §Bounded
    // byte ranges) — byte_len != 0 distinguishes a scoped origin range pull
    // from the whole-tail default (byte_len == 0).
    let req = StreamRequest {
        byte_offset: 1_048_576,
        byte_len: 262_144,
        ..sample_request()
    };
    let bytes = postcard::to_allocvec(&req)?;
    let decoded: StreamRequest = postcard::from_bytes(&bytes)?;
    assert_eq!(req, decoded);
    assert_eq!(decoded.byte_len, 262_144);
    Ok(())
}

#[test]
fn an_empty_chunk_cannot_be_decoded_at_all() -> Result<(), Box<dyn std::error::Error>> {
    // An empty chunk is not a legal frame (#1088), and since the #1145 review it is
    // not a REPRESENTABLE one: rejecting it is the decoder's job, not a receive
    // loop's obligation to remember.
    //
    // Decode-time rejection is what closes the hole. If an empty frame decoded cleanly
    // and only a later `validate()` call refused it, a receive loop that forgot to call
    // `validate()` would spin on empty frames forever — each empty frame advances neither
    // the cumulative byte count nor the stall deadline, so nothing ever breaks the loop.
    // Making the decoder refuse the frame lifts that standing obligation off every loop.
    //
    // So: forge the bytes an adversary would send (a length-prefix of 0, which no
    // constructor will produce) and require the decoder to refuse them.
    // postcard encodes `Vec<u8>` as a varint length then the bytes, so `[0x00]` is a
    // length of 0 and nothing else.
    assert!(
        postcard::from_bytes::<ChunkData>(&[0x00]).is_err(),
        "an empty ChunkData must not decode — the receive loops' stall detection, and \
         through it the reputation system, rest on every frame carrying bytes"
    );
    // And the same through the message enum, which is what the wire actually carries.
    let mut msg = encode_message(&ClientMessage::ChunkData(ChunkData::new(vec![0x00])?))?;
    assert_eq!(msg.pop(), Some(0x00), "payload byte");
    assert_eq!(msg.pop(), Some(0x01), "length prefix of 1");
    msg.push(0x00); // rewrite the length to 0
    assert!(
        decode_message::<ClientMessage>(&msg).is_err(),
        "an empty ChunkData must not decode inside a ClientMessage either"
    );
    Ok(())
}

#[test]
fn chunk_data_new_is_the_only_way_in_and_it_enforces_the_floor() {
    // The floor is what makes every frame a unit of progress (#1088). There is no
    // ceiling: frame size is the sender's choice, bounded above by the framing
    // layer's MAX_MESSAGE_SIZE, which runs before the receiver allocates.
    assert_eq!(
        ChunkData::new(Vec::new()),
        Err(MessageValidationError::EmptyChunk)
    );
    assert!(ChunkData::new(vec![0u8]).is_ok());
    assert!(ChunkData::new(vec![0u8; 1024]).is_ok());
    assert!(ChunkData::new(vec![0u8; 1024 * 1024]).is_ok());
}

#[test]
fn encode_chunk_frame_matches_the_generic_encoder() -> Result<(), Box<dyn std::error::Error>> {
    // `encode_chunk_frame` builds a frame body without a `ChunkData`, bypassing
    // postcard entirely, and the hot-path header encoders are pinned against it.
    // That is only sound while it and the generic encoder produce the same bytes.
    // Sizes straddle every varint width boundary of the length prefix: 1 byte
    // below 128, 2 below 16384, 3 above it.
    for len in [
        1usize,
        2,
        127,
        128,
        129,
        16_383,
        16_384,
        16_385,
        1024,
        1024 * 1024,
    ] {
        let payload = vec![0xABu8; len];
        let via_helper = crate::client::encode_chunk_frame(&payload)?;
        let via_postcard = crate::framing::encode_message(&ClientMessage::ChunkData(
            ChunkData::new(payload.clone())?,
        ))?;
        assert_eq!(
            via_helper, via_postcard,
            "encode_chunk_frame diverged from the generic encoder at len {len}"
        );
    }
    Ok(())
}

#[test]
fn encode_chunk_frame_golden_bytes() -> Result<(), Box<dyn std::error::Error>> {
    // An exact-byte pin, independent of both encoders: discriminant 2, then the
    // payload's postcard length varint, then the payload. If postcard ever changed
    // its sequence-length encoding, the agreement test above would still pass while
    // the wire silently moved; this catches that.
    assert_eq!(
        crate::client::encode_chunk_frame(&[0xDE, 0xAD, 0xBE, 0xEF])?,
        vec![0x02, 0x04, 0xDE, 0xAD, 0xBE, 0xEF]
    );
    // 300 bytes: length 300 = 0b100_101100 → varint [0xAC, 0x02].
    let big = crate::client::encode_chunk_frame(&[0x11u8; 300])?;
    assert_eq!(big.get(..3), Some(&[0x02, 0xAC, 0x02][..]));
    assert_eq!(big.len(), 3 + 300);
    Ok(())
}

#[test]
fn encode_chunk_frame_rejects_an_empty_payload() {
    // The floor moved off `ChunkData::new` for this path, so it needs its own pin:
    // ADR 005 §Non-empty chunk must hold for every frame the crate can emit.
    assert_eq!(
        crate::client::encode_chunk_frame(&[]),
        Err(MessageValidationError::EmptyChunk)
    );
}

/// The public single-buffer door carries the ADR 013 ceiling too, and reports the
/// FRAME length it refused rather than the payload length it was handed.
///
/// Without this the ceiling is only pinned on the two hot-path encoders, and this
/// is the one an out-of-tree caller reaches. It is also the boundary where the
/// equivalence with `encode_message` stops holding: the generic path builds an
/// oversized frame happily and only `write_frame` refuses it.
#[test]
fn encode_chunk_frame_rejects_a_frame_past_max_message_size() {
    let max = crate::framing::MAX_MESSAGE_SIZE as usize;
    // A 2^24 cap gives a 4-byte payload varint, so the header is 5 and the largest
    // frame that fits carries 5 fewer payload bytes.
    let largest = max - 5;
    assert_eq!(chunk_frame_postcard_len(largest), max);

    let over = vec![0u8; largest + 1];
    assert_matches!(
        crate::client::encode_chunk_frame(&over),
        Err(MessageValidationError::ChunkTooLarge { frame_len }) if frame_len == max + 1
    );
}

#[tokio::test]
async fn encode_chunk_frame_headers_matches_framed_encode_chunk_frame()
-> Result<(), Box<dyn std::error::Error>> {
    // Hot-path headers must be wire-identical to the generic
    // `encode_chunk_frame` + `write_frame` pair. This pins both
    // `encode_chunk_data_header` and `encode_chunk_frame_headers` across every
    // varint width boundary of BOTH varints the pair emits: the payload varint
    // (127/128, 16_383/16_384, 2_097_151/2_097_152) and the framing varint over
    // `postcard_len`, which widens at a different payload length because the
    // header sits in front of it (125/126, 16_381/16_382, 2_097_149/2_097_150).
    for len in [
        1usize,
        2,
        125,
        126,
        127,
        128,
        129,
        300,
        1024,
        16_381,
        16_382,
        16_383,
        16_384,
        16_385,
        64 * 1024,
        1024 * 1024,
        2_097_149,
        2_097_150,
        2_097_151,
        2_097_152,
    ] {
        let payload = vec![0xABu8; len];
        let mut frame_hdr = [0u8; crate::client::CHUNK_FRAME_HEADERS_MAX];
        let frame_hdr_len =
            crate::client::encode_chunk_frame_headers(payload.len(), &mut frame_hdr)?;
        let mut via_headers = frame_hdr.get(..frame_hdr_len).unwrap_or(&[]).to_vec();
        via_headers.extend_from_slice(&payload);
        let postcard = crate::client::encode_chunk_frame(&payload)?;
        let mut via_framing = Vec::new();
        crate::framing::write_frame(&mut via_framing, &postcard).await?;
        assert_eq!(
            via_headers, via_framing,
            "encode_chunk_frame_headers diverged at len {len}"
        );
        // Also pin the bare ChunkData header against the postcard prefix.
        let mut data_hdr = [0u8; crate::client::CHUNK_DATA_HEADER_MAX];
        let data_hdr_len = crate::client::encode_chunk_data_header(payload.len(), &mut data_hdr)?;
        assert_eq!(
            data_hdr.get(..data_hdr_len).unwrap_or(&[]),
            postcard.get(..data_hdr_len).unwrap_or(&[]),
            "encode_chunk_data_header diverged at len {len}"
        );
    }
    Ok(())
}

/// The floor and the ceiling of the hot-path encoders, which inherit neither
/// from `encode_chunk_frame`.
///
/// The floor is ADR 005 §Non-empty chunk (#1088): `encode_chunk_frame_headers` is
/// the door the serve paths use and it is built on `encode_chunk_data_header`, so
/// the invariant the pull-side inactivity deadline rests on is only as good as
/// their own rejection.
///
/// The ceiling is a *frame* length, not a payload length — `ChunkTooLarge`
/// reports `postcard_len` (header + payload), so the largest accepted payload is
/// `MAX_MESSAGE_SIZE` minus its own header.
#[test]
fn the_hot_path_encoders_reject_empty_and_oversized_frames() {
    let mut data_hdr = [0u8; CHUNK_DATA_HEADER_MAX];
    let mut frame_hdr = [0u8; CHUNK_FRAME_HEADERS_MAX];

    assert_matches!(
        encode_chunk_data_header(0, &mut data_hdr),
        Err(MessageValidationError::EmptyChunk)
    );
    assert_matches!(
        encode_chunk_frame_headers(0, &mut frame_hdr),
        Err(MessageValidationError::EmptyChunk)
    );

    // `MAX_MESSAGE_SIZE` is 2^24, whose payload varint is 4 bytes, so the header
    // is 5 and the largest frame that fits carries 5 fewer payload bytes.
    let max = crate::framing::MAX_MESSAGE_SIZE as usize;
    let largest = max - 5;
    assert_eq!(chunk_frame_postcard_len(largest), max);
    assert!(encode_chunk_data_header(largest, &mut data_hdr).is_ok());
    assert!(encode_chunk_frame_headers(largest, &mut frame_hdr).is_ok());

    // One byte past it, both doors report the FRAME length they refused.
    let over = largest + 1;
    assert_matches!(
        encode_chunk_data_header(over, &mut data_hdr),
        Err(MessageValidationError::ChunkTooLarge { frame_len }) if frame_len == max + 1
    );
    assert_matches!(
        encode_chunk_frame_headers(over, &mut frame_hdr),
        Err(MessageValidationError::ChunkTooLarge { frame_len }) if frame_len == max + 1
    );
}

/// `chunk_frame_postcard_len` is the size gate `encode_chunk_frame` and
/// `encode_chunk_data_header` both admit on, but it computes the varint width
/// itself rather than encoding one. If the two ever disagree, the gate accepts
/// or rejects at the wrong boundary.
#[test]
fn chunk_frame_postcard_len_matches_the_encoded_header() -> Result<(), MessageValidationError> {
    for len in [
        1usize,
        127,
        128,
        16_383,
        16_384,
        2_097_151,
        2_097_152,
        1024 * 1024,
    ] {
        let mut hdr = [0u8; CHUNK_DATA_HEADER_MAX];
        let hdr_len = encode_chunk_data_header(len, &mut hdr)?;
        assert_eq!(
            chunk_frame_postcard_len(len),
            hdr_len + len,
            "postcard length disagrees with the encoded header at len {len}"
        );
    }
    // Every caller rejects 0 first, but the sizing answer for it is still the
    // true encoded length of an empty `ChunkData` body: discriminant + `0x00`.
    assert_eq!(chunk_frame_postcard_len(0), 2);
    Ok(())
}

#[test]
fn chunk_data_validate_agrees_with_the_constructor() -> Result<(), MessageValidationError> {
    // `validate` survives only as the arm `ClientMessage::validate` dispatches to, so
    // the aggregate validator stays total over the enum. It cannot FAIL — a
    // `ChunkData` that exists came through `new` or the decode gate — and that is the
    // assertion worth making: if this ever returns `Err`, some construction path has
    // gone around the constructor.
    assert_eq!(ChunkData::new(vec![0u8])?.validate(), Ok(()));
    assert_eq!(ChunkData::new(vec![0u8; 1024 * 1024])?.validate(), Ok(()));
    Ok(())
}

#[test]
fn client_binding_roundtrip() -> Result<(), postcard::Error> {
    let b = sample_binding();
    let bytes = postcard::to_allocvec(&b)?;
    let decoded: ClientBinding = postcard::from_bytes(&bytes)?;
    assert_eq!(b, decoded);
    Ok(())
}

/// A non-empty-but-malformed remainder MUST error, not silently degrade to
/// `default()`. `take_from_bytes` rejects a truncated encoding on EOF. This
/// pins the contract a future `unwrap_or_default()` refactor would break.
#[test]
fn parse_stream_request_ext_rejects_malformed_remainder() {
    assert!(parse_stream_request_ext(&[0x01]).is_err());
}

// --- Extension / binding validation --------------------------------------

#[test]
fn stream_request_ext_validate_accepts_sample_and_default() {
    assert_eq!(sample_ext().validate(), Ok(()));
    assert_eq!(StreamRequestExt::default().validate(), Ok(()));
}

#[test]
fn stream_request_ext_validate_rejects_wrong_len_binding_sig() {
    let ext = StreamRequestExt {
        binding: Some(ClientBinding {
            ethereum_address: [0u8; 20],
            binding_signature: vec![0x01; BINDING_SIG_LEN - 1],
        }),
        capability: None,
    };
    assert_eq!(
        ext.validate(),
        Err(MessageValidationError::InvalidBindingSigLen {
            len: BINDING_SIG_LEN - 1
        })
    );
}

#[test]
fn client_binding_validate_rejects_wrong_len() {
    let b = ClientBinding {
        ethereum_address: [0u8; 20],
        binding_signature: Vec::new(),
    };
    assert_eq!(
        b.validate(),
        Err(MessageValidationError::InvalidBindingSigLen { len: 0 })
    );
    assert_eq!(sample_binding().validate(), Ok(()));
}

// --- StreamResponse ok/error consistency ---------------------------------

#[test]
fn stream_response_ext_validate_rejects_error_with_ok() {
    let ext = StreamResponseExt {
        error: Some(StreamError::NotFound),
    };
    assert_eq!(
        ext.validate(true),
        Err(MessageValidationError::StreamErrorWithOk)
    );
}

#[test]
fn stream_response_ext_validate_rejects_failure_without_error() {
    assert_eq!(
        StreamResponseExt::default().validate(false),
        Err(MessageValidationError::MissingStreamError)
    );
}

#[test]
fn stream_response_ext_validate_accepts_failure_with_delivery_error() {
    let ext = StreamResponseExt {
        error: Some(StreamError::Unfunded),
    };
    assert_eq!(ext.validate(false), Ok(()));
}

#[test]
fn stream_response_ext_validate_rejects_voucher_rejected_in_error() {
    // VoucherRejected is mid-stream-only; it must never ride in the response.
    let ext = StreamResponseExt {
        error: Some(StreamError::VoucherRejected {
            reason: VoucherRejectReason::SpendingCapExhausted,
            bundle: None,
        }),
    };
    assert_eq!(
        ext.validate(false),
        Err(MessageValidationError::VoucherRejectedInResponse)
    );
}

/// The base validator cannot see `error`, so an `ok == true` response with a
/// stray error code passes it. That is not a hole — it is why a receiver must
/// call BOTH halves — but it is worth pinning so nobody "simplifies" the
/// receive path down to one call.
#[test]
fn stream_response_base_validate_cannot_see_the_ext() -> Result<(), MessageValidationError> {
    sample_response().validate()?;
    let stray = StreamResponseExt {
        error: Some(StreamError::NotFound),
    };
    assert_eq!(
        stray.validate(sample_response().body.ok),
        Err(MessageValidationError::StreamErrorWithOk)
    );
    Ok(())
}

#[test]
fn stream_response_two_phase_with_ext() -> Result<(), postcard::Error> {
    let resp = sample_response();
    let ext = StreamResponseExt::default();
    let buf = encode_stream_response(&resp, Some(&ext))?;
    let (msg, remainder) = postcard::take_from_bytes::<ClientMessage>(&buf)?;
    assert_eq!(msg, ClientMessage::StreamResponse(resp));
    assert_eq!(parse_stream_response_ext(remainder)?, ext);
    Ok(())
}

#[test]
fn stream_response_two_phase_no_ext() -> Result<(), postcard::Error> {
    let resp = sample_response();
    let buf = encode_stream_response(&resp, None)?;
    let (msg, remainder) = postcard::take_from_bytes::<ClientMessage>(&buf)?;
    assert_eq!(msg, ClientMessage::StreamResponse(resp));
    assert!(remainder.is_empty());
    assert_eq!(
        parse_stream_response_ext(remainder)?,
        StreamResponseExt::default()
    );
    Ok(())
}

#[test]
fn stream_response_ext_tolerates_future_trailing_bytes() -> Result<(), postcard::Error> {
    let ext = StreamResponseExt {
        error: Some(StreamError::Declined),
    };
    let mut bytes = postcard::to_allocvec(&ext)?;
    bytes.extend_from_slice(&[0xAAu8, 0xBB, 0xCC]);
    assert_eq!(parse_stream_response_ext(&bytes)?, ext);
    Ok(())
}

// --- ClientMessage dispatcher + StreamError domain split -----------------

#[test]
fn client_message_validate_dispatches_to_payload() -> Result<(), MessageValidationError> {
    // Valid payloads pass through.
    assert_eq!(
        ClientMessage::StreamResponse(sample_response()).validate(),
        Ok(())
    );
    assert_eq!(ClientMessage::Voucher(sample_voucher()).validate(), Ok(()));
    // Invalid payloads propagate their own error through the dispatcher.
    let bad_resp = StreamResponse {
        slash_sig: Vec::new(),
        ..sample_response()
    };
    assert_eq!(
        ClientMessage::StreamResponse(bad_resp).validate(),
        Err(MessageValidationError::InvalidSlashSigLen { len: 0 })
    );
    let bad_voucher = Voucher {
        signature: Vec::new(),
        ..sample_voucher()
    };
    assert_eq!(
        ClientMessage::Voucher(bad_voucher).validate(),
        Err(MessageValidationError::InvalidVoucherSigLen { len: 0 })
    );
    // The aggregate seam dispatches to `ChunkData::validate` rather than waving the
    // variant through (#1088). It cannot catch an empty frame — none can be
    // built to hand it — but the arm must stay, so the validator remains total over
    // the enum and a future payload invariant on this variant is not silently skipped.
    assert_eq!(
        ClientMessage::ChunkData(ChunkData::new(vec![7])?).validate(),
        Ok(())
    );
    // Variants carrying no value invariants are unconditionally Ok.
    assert_eq!(
        ClientMessage::StreamRequest(sample_request()).validate(),
        Ok(())
    );
    assert_eq!(ClientMessage::StreamEnd.validate(), Ok(()));
    assert_eq!(
        ClientMessage::StreamError(StreamError::NotFound).validate(),
        Ok(())
    );
    Ok(())
}

#[test]
fn stream_error_domain_split_matches_variants() {
    for e in [
        StreamError::NotFound,
        StreamError::Declined,
        StreamError::Unfunded,
    ] {
        assert!(e.is_delivery_side(), "{e:?} is delivery-side");
        assert!(!e.is_mid_stream(), "{e:?} is not mid-stream");
    }
    let v = StreamError::VoucherRejected {
        reason: VoucherRejectReason::WrongSigner,
        bundle: None,
    };
    assert!(v.is_mid_stream());
    assert!(!v.is_delivery_side());
}

// --- Full framing stack --------------------------------------------------

#[tokio::test]
async fn client_message_full_stack_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let messages = [
        ClientMessage::StreamRequest(sample_request()),
        ClientMessage::StreamResponse(sample_response()),
        ClientMessage::ChunkData(ChunkData::new(vec![0x7u8; 1000])?),
        ClientMessage::Voucher(sample_voucher()),
        ClientMessage::ChunkPreimage(sample_preimage()),
        ClientMessage::StreamEnd,
        ClientMessage::StreamError(StreamError::VoucherRejected {
            reason: VoucherRejectReason::SpendingCapExhausted,
            bundle: Some(WatermarkBundle {
                amount: 0x0101_0101_0101_0101u64,
                bytes_delivered: 0x0303_0303_0303_0303u64,
                chain_root: [0x05u8; 32],
                verified_index: 9,
                tip: [0x06u8; 32],
                chunk_price: 0x0707_0707_0707_0707u64,
                last_signature: vec![0x04u8; VOUCHER_SIG_LEN],
            }),
        }),
    ];
    for msg in messages {
        let payload = encode_message(&msg)?;
        let mut buf = Vec::new();
        write_frame(&mut buf, &payload).await?;
        let mut cursor = std::io::Cursor::new(buf);
        let frame = read_frame(&mut cursor).await?;
        let (decoded, tail) = decode_message::<ClientMessage>(&frame)?;
        assert_eq!(decoded, msg);
        assert!(tail.is_empty(), "no extension bytes expected");
    }
    Ok(())
}
