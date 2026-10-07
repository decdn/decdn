use super::*;
use proptest::prelude::*;

fn block_on<F>(fut: F) -> Result<(), TestCaseError>
where
    F: std::future::Future<Output = Result<(), TestCaseError>>,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(fut)
}

proptest! {
    #[test]
    fn varint_roundtrip(v: u32) {
        block_on(async {
            let mut buf = Vec::new();
            write_varint_u32(&mut buf, v).await.unwrap();
            let mut cursor = std::io::Cursor::new(buf);
            let decoded = read_varint_u32(&mut cursor).await.unwrap();
            prop_assert_eq!(decoded, v);
            Ok(())
        })?;
    }

    #[test]
    fn frame_roundtrip_arbitrary_payload(payload in proptest::collection::vec(any::<u8>(), 0..4096)) {
        block_on(async {
            let mut buf = Vec::new();
            write_frame(&mut buf, &payload).await.unwrap();
            let mut cursor = std::io::Cursor::new(buf);
            let decoded = read_frame(&mut cursor).await.unwrap();
            prop_assert_eq!(decoded, payload);
            Ok(())
        })?;
    }

    #[test]
    fn garbage_bytes_never_panic(data in proptest::collection::vec(any::<u8>(), 0..256)) {
        block_on(async {
            let mut cursor = std::io::Cursor::new(&data);
            let _ = read_frame(&mut cursor).await;
            Ok(()) as Result<(), TestCaseError>
        })?;
    }

    #[test]
    fn varint_garbage_never_panics(data in proptest::collection::vec(any::<u8>(), 0..10)) {
        block_on(async {
            let mut cursor = std::io::Cursor::new(&data);
            let _ = read_varint_u32(&mut cursor).await;
            Ok(()) as Result<(), TestCaseError>
        })?;
    }

    #[test]
    fn decode_message_garbage_never_panics(data in proptest::collection::vec(any::<u8>(), 0..256)) {
        use crate::ProbeMessage;
        let _ = decode_message::<ProbeMessage>(&data);
    }
}
