use super::*;
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn counter_advances_by_bytes_read() -> std::io::Result<()> {
    let mut src: &[u8] = b"hello world"; // 11 bytes
    let counter = Arc::new(AtomicU64::new(0));
    let mut r = ProgressReader::new(&mut src, Arc::clone(&counter));
    let mut buf = [0u8; 5];
    r.read_exact(&mut buf).await?;
    assert_eq!(&buf, b"hello");
    assert_eq!(counter.load(Ordering::Relaxed), 5);
    let mut rest = Vec::new();
    r.read_to_end(&mut rest).await?;
    assert_eq!(counter.load(Ordering::Relaxed), 11);
    Ok(())
}
