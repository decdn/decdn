use super::*;

#[tokio::test]
async fn parse_frame_incomplete_header_and_body_return_none() -> Result<(), FrameError> {
    // A complete frame: varint(len) || payload.
    let payload = b"batched-voucher".to_vec();
    let payload_len = u32::try_from(payload.len()).map_err(|_| FrameError::Varint)?;
    let mut hdr = Vec::new();
    write_varint_u32(&mut hdr, payload_len).await?;
    let mut framed = hdr.clone();
    framed.extend_from_slice(&payload);

    // Empty buffer: no varint yet.
    assert_eq!(parse_frame(&[])?, None);
    // Header present but body short by one byte: incomplete.
    let short = framed.get(..framed.len() - 1).unwrap_or_default();
    assert_eq!(parse_frame(short)?, None);
    // Complete frame: exact split point (header_len, payload_len).
    let got = parse_frame(&framed)?.ok_or(FrameError::Varint)?;
    assert_eq!(got, (hdr.len(), payload.len()));
    // Extra trailing bytes (a second frame's start) do not confuse the split.
    let mut with_tail = framed.clone();
    with_tail.extend_from_slice(b"\x03ab");
    assert_eq!(parse_frame(&with_tail)?, Some((hdr.len(), payload.len())));
    Ok(())
}

#[tokio::test]
async fn parse_frame_rejects_oversized_length() -> Result<(), FrameError> {
    let mut hdr = Vec::new();
    write_varint_u32(&mut hdr, MAX_MESSAGE_SIZE + 1).await?;
    assert!(matches!(
        parse_frame(&hdr),
        Err(FrameError::TooLarge(n)) if n == MAX_MESSAGE_SIZE + 1
    ));
    Ok(())
}

async fn roundtrip_varint(v: u32) -> Result<(), FrameError> {
    let mut buf = Vec::new();
    write_varint_u32(&mut buf, v).await?;
    let mut cursor = std::io::Cursor::new(buf);
    let decoded = read_varint_u32(&mut cursor).await?;
    assert_eq!(decoded, v, "varint roundtrip {v}");
    Ok(())
}

#[tokio::test]
async fn varint_edges() -> Result<(), FrameError> {
    for v in [
        0u32,
        1,
        127,
        128,
        16_383,
        16_384,
        2_097_151,
        2_097_152,
        u32::MAX,
    ] {
        roundtrip_varint(v).await?;
    }
    Ok(())
}

#[tokio::test]
async fn varint_rejects_oversize_continuation() {
    // 5 bytes all with continuation set → overflow
    let bytes = [0xFFu8, 0xFF, 0xFF, 0xFF, 0xFF];
    let mut cursor = std::io::Cursor::new(bytes);
    let r = read_varint_u32(&mut cursor).await;
    assert!(matches!(r, Err(FrameError::Varint)));
}

#[tokio::test]
async fn varint_rejects_5th_byte_overflow_bits() {
    // 5 bytes, last byte has high data bits set beyond u32 range.
    let bytes = [0x80u8, 0x80, 0x80, 0x80, 0x10];
    let mut cursor = std::io::Cursor::new(bytes);
    let r = read_varint_u32(&mut cursor).await;
    assert!(matches!(r, Err(FrameError::Varint)));
}

#[tokio::test]
async fn frame_roundtrip() -> Result<(), FrameError> {
    let payload = b"hello deCDN".to_vec();
    let mut buf = Vec::new();
    write_frame(&mut buf, &payload).await?;
    let mut cursor = std::io::Cursor::new(buf);
    let got = read_frame(&mut cursor).await?;
    assert_eq!(got, payload);
    Ok(())
}

#[tokio::test]
async fn frame_rejects_too_large_without_reading_payload() -> Result<(), FrameError> {
    // Encode a varint for MAX+1 and follow with NO payload bytes.
    let mut header = Vec::new();
    write_varint_u32(&mut header, MAX_MESSAGE_SIZE + 1).await?;
    let header_len = header.len();
    let mut cursor = std::io::Cursor::new(header);
    let r = read_frame(&mut cursor).await;
    assert!(matches!(r, Err(FrameError::TooLarge(n)) if n == MAX_MESSAGE_SIZE + 1));
    // No payload bytes were read.
    assert_eq!(usize::try_from(cursor.position()).ok(), Some(header_len));
    Ok(())
}

#[tokio::test]
async fn frame_short_read_errors() -> Result<(), FrameError> {
    // Varint says 10 bytes, but stream only has 3.
    let mut buf = Vec::new();
    write_varint_u32(&mut buf, 10).await?;
    buf.extend_from_slice(b"abc");
    let mut cursor = std::io::Cursor::new(buf);
    let r = read_frame(&mut cursor).await;
    assert!(matches!(r, Err(FrameError::Io(_))));
    Ok(())
}

#[tokio::test]
async fn encode_decode_roundtrip() -> Result<(), FrameError> {
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
    struct M {
        a: u64,
        b: String,
    }
    let m = M {
        a: 42,
        b: "hi".into(),
    };
    let bytes = encode_message(&m)?;
    let (decoded, rest) = decode_message::<M>(&bytes)?;
    assert_eq!(decoded, m);
    assert!(rest.is_empty());
    Ok(())
}

#[tokio::test]
async fn frame_matches_postcard_length() -> Result<(), FrameError> {
    // Compatibility sanity: postcard serializing a u32 as a standalone value
    // uses the same varint encoding we emit for the length prefix.
    let mut ours = Vec::new();
    write_varint_u32(&mut ours, 300).await?;
    let theirs = postcard::to_allocvec(&300u32)?;
    assert_eq!(ours, theirs);
    Ok(())
}

#[tokio::test]
async fn empty_payload_encodes_single_zero_byte() -> Result<(), FrameError> {
    let mut buf: Vec<u8> = Vec::new();
    write_frame(&mut buf, &[]).await?;
    assert_eq!(buf, vec![0u8]);
    Ok(())
}

#[tokio::test]
async fn decode_message_rejects_garbage() {
    use crate::ProbeMessage;
    // Discriminant 99 has no matching `ProbeMessage` variant → Decode error.
    let garbage = [99u8, 0, 0, 0];
    let r = decode_message::<ProbeMessage>(&garbage);
    assert!(
        matches!(r, Err(FrameError::Decode(_))),
        "expected FrameError::Decode"
    );
}

#[tokio::test]
async fn varint_eof_mid_stream() {
    // Continuation bit set, no further bytes. `read_exact` surfaces this as
    // `UnexpectedEof` → `FrameError::Io`, not `FrameError::Varint` — pinning
    // the classification so a future refactor can't silently change it.
    let bytes = [0x80u8];
    let mut cursor = std::io::Cursor::new(bytes);
    let r = read_varint_u32(&mut cursor).await;
    assert!(
        matches!(&r, Err(FrameError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof),
        "expected Io(UnexpectedEof), got {r:?}"
    );
}

// A minimal 2-variant top-level enum standing in for the real ALPN enums,
// so the framing-layer classifier can be tested without pulling in the
// message/client/dht modules.
struct TwoVariant;
impl TopLevelEnum for TwoVariant {
    const VARIANT_COUNT: u32 = 2;
}

#[test]
fn leading_varint_reads_single_byte_discriminants() {
    assert_eq!(leading_varint_u32(&[0u8]), Some(0));
    assert_eq!(leading_varint_u32(&[1u8, 0xAB, 0xCD]), Some(1));
    assert_eq!(leading_varint_u32(&[99u8, 0, 0]), Some(99));
}

#[test]
fn leading_varint_reads_multibyte_discriminant() {
    // 300 = 0xAC 0x02 in postcard varint; must match the length-prefix codec.
    assert_eq!(leading_varint_u32(&[0xACu8, 0x02, 0xFF]), Some(300));
}

#[test]
fn leading_varint_rejects_malformed() {
    // Empty frame → no discriminant.
    assert_eq!(leading_varint_u32(&[]), None);
    // Continuation bit set with no terminating byte.
    assert_eq!(leading_varint_u32(&[0x80u8]), None);
    assert_eq!(leading_varint_u32(&[0x80u8, 0x80, 0x80, 0x80, 0x80]), None);
    // 5th byte carries overflow bits beyond u32 range.
    assert_eq!(leading_varint_u32(&[0x80u8, 0x80, 0x80, 0x80, 0x10]), None);
}

#[test]
fn unknown_variant_flags_out_of_range_discriminant() {
    // Discriminant 2 is the first index past a 2-variant enum → unknown.
    assert!(is_unknown_variant::<TwoVariant>(&[2u8, 0, 0]));
    assert!(is_unknown_variant::<TwoVariant>(&[99u8]));
}

#[test]
fn unknown_variant_false_for_in_range_discriminant() {
    // In-range discriminants (0, 1) are known variants — a decode failure
    // on their payload is MALFORMED, not UNSUPPORTED.
    assert!(!is_unknown_variant::<TwoVariant>(&[0u8]));
    assert!(!is_unknown_variant::<TwoVariant>(&[1u8, 0xFF]));
}

#[test]
fn unknown_variant_false_for_malformed_leading_varint() {
    // A frame that cannot even yield a discriminant is a genuine parse
    // fault → stays MALFORMED (classifier returns false).
    assert!(!is_unknown_variant::<TwoVariant>(&[]));
    assert!(!is_unknown_variant::<TwoVariant>(&[0x80u8]));
}

#[tokio::test]
async fn probe_message_full_stack_roundtrip() -> Result<(), FrameError> {
    use crate::ProbeMessage;
    use crate::message::{ProbeRequest, ProbeResponse, ProbeResponseBody};

    let req = ProbeMessage::Request(ProbeRequest {
        hash: [5u8; 32],
        timestamp_us: 0xfeed_face,
    });
    let resp = ProbeMessage::Response(ProbeResponse {
        body: ProbeResponseBody {
            hash: [5u8; 32],
            has_blob: true,
            rate_per_mb: 7,
            timestamp_us: 0xfeed_face,
        },
        slash_sig: vec![0x3u8; crate::SLASH_SIG_LEN],
    });

    for msg in [req, resp.clone()] {
        let payload = encode_message(&msg)?;
        let mut buf = Vec::new();
        write_frame(&mut buf, &payload).await?;
        let mut cursor = std::io::Cursor::new(buf);
        let frame = read_frame(&mut cursor).await?;
        let (decoded, tail) = decode_message::<ProbeMessage>(&frame)?;
        assert_eq!(decoded, msg);
        assert!(tail.is_empty(), "no extension bytes expected");
    }

    // The same round trip carrying a Tier-1 extension: the framing layer is
    // agnostic to it, so the base still decodes and the remainder is the ext
    // — which is the property the two-phase pattern rests on end to end.
    let ext = crate::message::ProbeResponseExt {
        total_bytes: Some(1_700_000),
        coverage: crate::Coverage::empty(),
    };
    let ProbeMessage::Response(body) = &resp else {
        unreachable!("resp is a Response")
    };
    let payload = crate::message::encode_probe_response(body, Some(&ext))?;
    let mut buf = Vec::new();
    write_frame(&mut buf, &payload).await?;
    let mut cursor = std::io::Cursor::new(buf);
    let frame = read_frame(&mut cursor).await?;
    let (decoded, tail) = decode_message::<ProbeMessage>(&frame)?;
    assert_eq!(decoded, resp);
    assert!(!tail.is_empty(), "extension bytes expected");
    assert_eq!(
        crate::message::parse_probe_response_ext(tail).map_err(FrameError::from)?,
        ext
    );
    Ok(())
}
