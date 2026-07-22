//! Async polling helper shared by the anvil-backed journeys.

use std::time::Duration;

/// Poll `f` until it yields `Some`, or `timeout` elapses. A closure error aborts
/// the poll immediately with that error (so a real RPC/contract failure surfaces
/// instead of a generic timeout).
pub async fn poll<T, F, Fut>(timeout: Duration, mut f: F) -> anyhow::Result<Option<T>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Option<T>>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f().await? {
            return Ok(Some(v));
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
