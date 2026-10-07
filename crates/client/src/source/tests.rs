use super::{BlobSource, Funder};
use crate::sink::StashedFault;
use alloy::primitives::U256;
use decdn_bao_range::{AlignedRange, align_range};
use decdn_incentive::DepositOutcome;
use iroh_io::AsyncStreamReader;

fn blob(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// The scripted source yields wire that the store's decoder can verify, and
/// reports the blob's true `total_bytes`.
#[tokio::test]
async fn scripted_source_opens_a_verifiable_range() -> anyhow::Result<()> {
    let data = blob(200 * 1024 + 7);
    let source = super::ScriptedSource::new(data.clone())?;
    let range = align_range(0, 0, data.len() as u64)?;
    let (header, mut reader) = source.open(source.root(), range).await?;
    assert_eq!(header.total_bytes, data.len() as u64);
    let first = reader.read_bytes(64).await?;
    assert!(!first.is_empty(), "a non-empty blob must yield wire bytes");
    assert!(
        reader.take_fault().is_none(),
        "no fault was scripted, so none should be parked"
    );
    Ok(())
}

/// A scripted mid-range fault is parked on the reader for the decode loop to
/// surface, exactly as a real stalled peer would.
#[tokio::test]
async fn scripted_source_parks_a_mid_range_fault() -> anyhow::Result<()> {
    let data = blob(200 * 1024 + 7);
    let source = super::ScriptedSource::new(data.clone())?
        .with_fault_after(4096, || anyhow::anyhow!("scripted stall"));
    let range = align_range(0, 0, data.len() as u64)?;
    assert!(
        drain(&source, &range).await.is_err(),
        "the scripted fault must be parked once the truncated wire is drained"
    );
    Ok(())
}

/// Open `range` on `src` and read its wire to the end. Returns the wire
/// bytes read, or the fault the reader parked.
async fn drain(src: &super::ScriptedSource, range: &AlignedRange) -> anyhow::Result<u64> {
    let (_header, mut reader) = src.open(src.root(), range.clone()).await?;
    let mut read = 0u64;
    loop {
        let chunk = reader.read_bytes(64 * 1024).await?;
        if chunk.is_empty() {
            break;
        }
        read += chunk.len() as u64;
    }
    reader.take_fault().map_or(Ok(read), Err)
}

#[tokio::test]
async fn fault_once_after_fires_on_the_first_reader_only() -> anyhow::Result<()> {
    let src = super::ScriptedSource::new(vec![3u8; 64 * 1024])?
        .fault_once_after(4096, || anyhow::anyhow!("scripted reset"));
    let range = align_range(0, 64 * 1024, 64 * 1024)?;
    let first = drain(&src, &range).await;
    assert!(first.is_err(), "the first reader faults");
    let second = drain(&src, &range).await;
    assert!(second.is_ok(), "the second reader delivers");
    Ok(())
}

/// `PeerSource` implements `BlobSource` — checked at compile time rather than
/// exercised end-to-end, since a real run needs a live `Endpoint`/connection.
/// The driver tests exercise the trait's behavior against `ScriptedSource`;
/// the loopback suites (`open_progressive_pull` under the `stream_fetch*`
/// wrappers, the `byte_len` plumbing above) cover `PeerSource`'s own
/// building blocks. `'static` is just a concrete lifetime to instantiate the
/// generic type parameter with — no value is constructed.
#[test]
fn peer_source_is_a_blob_source() {
    fn assert_impl<T: BlobSource>() {}
    assert_impl::<super::PeerSource<'static>>();
}

/// The fake funder records amounts and echoes its scripted outcome.
#[tokio::test]
async fn fake_funder_records_and_returns() -> anyhow::Result<()> {
    let funder = super::FakeFunder::new(3, DepositOutcome::Added(U256::from(100u64)));
    assert_eq!(funder.max_topups(), 3);
    let out = funder.top_up(U256::from(40u64)).await?;
    assert_eq!(out, DepositOutcome::Added(U256::from(100u64)));
    assert_eq!(funder.calls(), vec![U256::from(40u64)]);
    Ok(())
}

/// A signer registry that answers every read with the authorization it
/// holds, or fails it when it holds `None`, and counts its reads.
#[derive(Default)]
struct FixedAuthorization {
    auth: Option<decdn_incentive::payment_pool::SignerAuthorization>,
    reads: std::sync::atomic::AtomicUsize,
}

impl super::SignerRegistry for FixedAuthorization {
    fn read(
        &self,
        _pool_id: alloy::primitives::B256,
        _signer: alloy::primitives::Address,
    ) -> super::SourceFuture<'_, decdn_incentive::payment_pool::SignerAuthorization> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let auth = self.auth;
        Box::pin(async move { auth.ok_or_else(|| anyhow::anyhow!("rpc down")) })
    }
}

/// A signer registry whose read never answers.
struct SilentRegistry;

impl super::SignerRegistry for SilentRegistry {
    fn read(
        &self,
        _pool_id: alloy::primitives::B256,
        _signer: alloy::primitives::Address,
    ) -> super::SourceFuture<'_, decdn_incentive::payment_pool::SignerAuthorization> {
        Box::pin(std::future::pending())
    }
}

const DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

const NOW: u64 = 1_700_000_000;

/// An open-stage refusal with `error`, signed at `rate_per_mb`.
fn open_refusal(error: decdn_protocol::client::StreamError, rate_per_mb: u64) -> anyhow::Error {
    use decdn_protocol::client::{StreamResponse, StreamResponseBody};
    crate::UpstreamRefused::open(
        StreamResponse {
            body: StreamResponseBody {
                hash: [0x5Au8; 32],
                ok: false,
                rate_per_mb,
                total_bytes: 0,
                pool_id: [0x77u8; 32],
                timestamp_us: 0,
            },
            slash_sig: vec![0u8; decdn_protocol::message::SLASH_SIG_LEN],
        },
        &decdn_protocol::StreamResponseExt { error: Some(error) },
    )
}

fn registered(cap: u64, spent: u64) -> decdn_incentive::payment_pool::SignerAuthorization {
    decdn_incentive::payment_pool::SignerAuthorization::Registered {
        cap,
        expiry: NOW + 3_600,
        spent,
    }
}

/// Run `confirm_refusal` for `err` against a registry answering `auth`.
/// Returns the result and how many reads it made.
async fn confirm(
    err: anyhow::Error,
    auth: Option<decdn_incentive::payment_pool::SignerAuthorization>,
) -> (anyhow::Error, usize) {
    let ctx = super::ctx_with(0xa1, U256::from(1_000u64));
    let check = FixedAuthorization {
        auth,
        ..FixedAuthorization::default()
    };
    let err = super::confirm_refusal(Some(&check), err, &ctx, NOW, DEADLINE).await;
    (err, check.reads.load(std::sync::atomic::Ordering::Relaxed))
}

/// A signed `NotFound` to a signer whose `cap − spent` is one below a
/// chunk at the signed rate ends as `SignerCapDrained`, with the numbers
/// the node refused on (#2338).
#[tokio::test]
async fn a_not_found_to_a_drained_signer_becomes_signer_cap_drained() -> anyhow::Result<()> {
    let (err, _) = confirm(
        open_refusal(decdn_protocol::client::StreamError::NotFound, 10),
        Some(registered(40, 31)),
    )
    .await;
    let drained = err
        .downcast_ref::<super::SignerCapDrained>()
        .ok_or_else(|| anyhow::anyhow!("a drained signer is named: {err:#}"))?;
    assert_eq!(drained.remaining, 9);
    assert_eq!(drained.floor(), U256::from(10u64));
    assert_eq!(drained.rate_per_mb, 10);
    assert!(!drained.expired);
    assert_eq!(
        crate::classify(&err),
        crate::Fault::Source,
        "a cheaper provider can still serve 9 µUSDC of headroom"
    );
    Ok(())
}

/// A registration past its expiry is drained whatever its headroom.
#[tokio::test]
async fn a_not_found_to_an_expired_signer_becomes_signer_cap_drained() -> anyhow::Result<()> {
    let expired = decdn_incentive::payment_pool::SignerAuthorization::Registered {
        cap: 1_000,
        expiry: NOW,
        spent: 0,
    };
    let (err, _) = confirm(
        open_refusal(decdn_protocol::client::StreamError::NotFound, 10),
        Some(expired),
    )
    .await;
    let drained = err
        .downcast_ref::<super::SignerCapDrained>()
        .ok_or_else(|| anyhow::anyhow!("an expired signer is named: {err:#}"))?;
    assert!(drained.expired);
    assert_eq!(
        crate::classify(&err),
        crate::Fault::Fatal(crate::FatalScope::Command)
    );
    Ok(())
}

/// Every refusal the chain does not explain comes back as it arrived: a
/// signer that still covers one chunk, an unregistered signer, a failed
/// read, a refusal other than `NotFound`, an unsigned mid-stream
/// `NotFound`, and a refusal signed at a zero rate. Only an open-stage
/// `NotFound` signed at a non-zero rate costs a read.
#[tokio::test]
async fn an_unexplained_refusal_stands() {
    use decdn_protocol::client::StreamError;
    let cases = [
        (
            open_refusal(StreamError::NotFound, 10),
            Some(registered(40, 30)),
            1,
        ),
        (
            open_refusal(StreamError::NotFound, 10),
            Some(decdn_incentive::payment_pool::SignerAuthorization::Unregistered),
            1,
        ),
        (open_refusal(StreamError::NotFound, 10), None, 1),
        (
            open_refusal(StreamError::Overloaded, 10),
            Some(registered(40, 40)),
            0,
        ),
        (
            anyhow::Error::new(crate::UpstreamRefused::mid_stream(StreamError::NotFound)),
            Some(registered(40, 40)),
            0,
        ),
        (
            open_refusal(StreamError::NotFound, 0),
            Some(registered(40, 40)),
            0,
        ),
    ];
    for (i, (err, auth, want_reads)) in cases.into_iter().enumerate() {
        let (err, reads) = confirm(err, auth).await;
        assert_eq!(reads, want_reads, "case {i} reads");
        assert!(
            err.downcast_ref::<super::SignerCapDrained>().is_none(),
            "case {i}: {err:#}"
        );
        assert!(
            err.downcast_ref::<crate::UpstreamRefused>().is_some(),
            "case {i} keeps the refusal: {err:#}"
        );
    }
}

/// A source with no signer check keeps the refusal.
#[tokio::test]
async fn no_check_leaves_the_refusal() {
    let ctx = super::ctx_with(0xa1, U256::from(1_000u64));
    let err = super::confirm_refusal(
        None,
        open_refusal(decdn_protocol::client::StreamError::NotFound, 10),
        &ctx,
        NOW,
        DEADLINE,
    )
    .await;
    assert!(err.downcast_ref::<crate::UpstreamRefused>().is_some());
}

/// A read that never answers gives way at the deadline, and the refusal
/// stands: a stalled RPC cannot hold the lane's open.
#[tokio::test(start_paused = true)]
async fn a_stalled_read_gives_way_at_the_deadline() {
    let ctx = super::ctx_with(0xa1, U256::from(1_000u64));
    let started = tokio::time::Instant::now();
    let err = super::confirm_refusal(
        Some(&SilentRegistry),
        open_refusal(decdn_protocol::client::StreamError::NotFound, 10),
        &ctx,
        NOW,
        DEADLINE,
    )
    .await;
    assert_eq!(started.elapsed(), DEADLINE);
    assert!(err.downcast_ref::<crate::UpstreamRefused>().is_some());
}
