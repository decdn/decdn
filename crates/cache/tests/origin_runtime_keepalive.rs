//! Network origins keep their pooled connections alive across the throwaway
//! runtimes that call them (#1673, #1675).
//!
//! The node runs each serve-miss pull leg on its own current-thread runtime
//! and drops that runtime when the serve ends. [`HttpOrigin`] and
//! [`S3Origin`] share one keep-alive pool across every caller, and run each
//! request send on the runtime that built the origin, so a pooled connection
//! outlives the pull leg that opened it.
//!
//! Each test builds the origin on the test's runtime, then fetches from
//! threads that each own a current-thread runtime, the way a pull leg does. A
//! raw HTTP/1.1 server counts the TCP connections it accepts, so a test can
//! tell a reused keep-alive connection from a fresh one.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use decdn_cache::{
    Hash, HttpOrigin, Origin, OriginFetch, S3Credentials, S3Origin, S3OriginConfig,
    parse_origin_url,
};
use futures_util::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Notify, oneshot};

/// The body every response carries. Large enough that the held response can
/// split it across two writes.
const BODY: &[u8] = &[0x5A; 64 * 1024];

/// How long to let a finished fetch hand its connection back to the pool
/// before the next fetch checks one out. hyper returns the connection on a
/// background task, so a fetch that starts at once can race it and dial a
/// second connection.
const POOL_SETTLE: Duration = Duration::from_millis(250);

/// Ceiling on leg B's body read once the server releases it. A connection
/// that dies with leg A's runtime can leave the read waiting forever.
const BODY_DEADLINE: Duration = Duration::from_secs(10);

/// A keep-alive HTTP/1.1 server that answers every request with `200` and
/// [`BODY`], whatever the method or path.
struct KeepAliveServer {
    url: String,
    accepts: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

impl KeepAliveServer {
    /// Start the server. The response to request number `hold` (0-based,
    /// counted across all connections) sends its headers and the first half
    /// of the body, then waits for [`Self::release_held`] before it sends the
    /// rest.
    async fn start(hold: Option<usize>) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", listener.local_addr()?);
        let accepts = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let (accepts_srv, release_srv) = (Arc::clone(&accepts), Arc::clone(&release));
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                accepts_srv.fetch_add(1, Ordering::SeqCst);
                let requests = Arc::clone(&requests);
                let release = Arc::clone(&release_srv);
                tokio::spawn(serve_connection(sock, requests, hold, release));
            }
        });
        Ok(Self {
            url,
            accepts,
            release,
        })
    }

    fn accepts(&self) -> usize {
        self.accepts.load(Ordering::SeqCst)
    }

    fn release_held(&self) {
        self.release.notify_one();
    }
}

/// Answer requests on one connection until the client closes it.
async fn serve_connection(
    mut sock: tokio::net::TcpStream,
    requests: Arc<AtomicUsize>,
    hold: Option<usize>,
    release: Arc<Notify>,
) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        // Read up to the end of one request head. Every request here is a
        // body-less GET or HEAD.
        let head_end = loop {
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
            match sock.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(chunk.get(..n).unwrap_or(&[])),
            }
        };
        let is_head = buf.starts_with(b"HEAD ");
        buf.drain(..head_end);
        let index = requests.fetch_add(1, Ordering::SeqCst);
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\n\r\n",
            BODY.len()
        );
        if sock.write_all(head.as_bytes()).await.is_err() {
            return;
        }
        if is_head {
            continue;
        }
        let (first, rest) = BODY.split_at(BODY.len() / 2);
        if sock.write_all(first).await.is_err() {
            return;
        }
        if hold == Some(index) {
            release.notified().await;
        }
        if sock.write_all(rest).await.is_err() {
            return;
        }
    }
}

fn http_origin(server: &KeepAliveServer) -> anyhow::Result<Arc<dyn Origin>> {
    Ok(Arc::new(HttpOrigin::parse(&server.url)?))
}

/// A real `S3Origin` (path-style, dummy static credentials) pointed at the
/// server. The server ignores the path and the signature.
async fn s3_origin(server: &KeepAliveServer) -> anyhow::Result<Arc<dyn Origin>> {
    let cfg = S3OriginConfig {
        bucket: "decdn-blobs".to_string(),
        region: "us-east-1".to_string(),
        endpoint_url: Some(parse_origin_url(&server.url)?),
        path_style: true,
        prefix: String::new(),
        credentials: Some(S3Credentials::Static {
            access_key_id: "AKIA-test-fake".to_string(),
            secret_access_key: "secret-fake".to_string(),
            session_token: None,
        }),
    };
    Ok(Arc::new(S3Origin::new(&cfg).await?))
}

/// Fetch `hash` and wait for the response headers. Returns the body stream.
async fn open(
    origin: &dyn Origin,
    hash: Hash,
) -> anyhow::Result<decdn_cache::origin::OriginByteStream> {
    match origin.fetch(hash, 1 << 30).await? {
        OriginFetch::Found { stream, .. } => Ok(stream),
        OriginFetch::NotFound => anyhow::bail!("origin answered NotFound"),
        OriginFetch::AlreadyAdmitted => anyhow::bail!("origin answered AlreadyAdmitted"),
    }
}

async fn drain(mut stream: decdn_cache::origin::OriginByteStream) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk?);
    }
    Ok(out)
}

fn pull_runtime() -> anyhow::Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?)
}

/// Fetch and drain `hash` on a new current-thread runtime on its own thread,
/// then drop that runtime — one whole pull leg.
async fn fetch_on_throwaway_runtime(
    origin: Arc<dyn Origin>,
    hash: Hash,
) -> anyhow::Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || {
        let rt = pull_runtime()?;
        let body = rt.block_on(async { drain(open(&*origin, hash).await?).await });
        drop(rt);
        body
    })
    .await?
}

/// Two pull legs in turn, each on its own runtime that drops when it ends,
/// share one keep-alive connection: the connection lives on the runtime that
/// built the origin, so the first leg's runtime drop does not close it.
async fn keep_alive_survives_a_dropped_pull_runtime(
    server: &KeepAliveServer,
    origin: Arc<dyn Origin>,
) -> anyhow::Result<()> {
    let hash = Hash::new(b"keep-alive-marker");
    for leg in 0..2 {
        let body = fetch_on_throwaway_runtime(Arc::clone(&origin), hash).await?;
        anyhow::ensure!(body == BODY, "leg {leg} got {} bytes", body.len());
        tokio::time::sleep(POOL_SETTLE).await;
    }
    anyhow::ensure!(
        server.accepts() == 1,
        "the second leg dialled a new connection instead of reusing the pooled one \
         ({} accepted)",
        server.accepts()
    );
    Ok(())
}

/// The #1673 shape. Leg A opens the connection and finishes, but its runtime
/// stays up. Leg B reuses the pooled connection and is mid-body when A's
/// runtime drops. B must still read every byte.
async fn in_flight_fetch_survives_its_connection_openers_runtime_drop(
    server: &KeepAliveServer,
    origin: Arc<dyn Origin>,
) -> anyhow::Result<()> {
    let hash = Hash::new(b"in-flight-marker");

    // Leg A: fetch on its own runtime, report, then keep the runtime alive
    // until told to drop it.
    let (a_done_tx, a_done_rx) = oneshot::channel::<anyhow::Result<Vec<u8>>>();
    let (drop_a_tx, drop_a_rx) = std::sync::mpsc::channel::<()>();
    let origin_a = Arc::clone(&origin);
    let leg_a = std::thread::spawn(move || -> anyhow::Result<()> {
        let rt = pull_runtime()?;
        let body = rt.block_on(async { drain(open(&*origin_a, hash).await?).await });
        let _ = a_done_tx.send(body);
        let _ = drop_a_rx.recv();
        drop(rt);
        Ok(())
    });
    let body_a = a_done_rx.await??;
    anyhow::ensure!(body_a == BODY, "leg A got {} bytes", body_a.len());
    tokio::time::sleep(POOL_SETTLE).await;

    // Leg B: open the held response, report once the headers are in, then
    // drain.
    let (b_open_tx, b_open_rx) = oneshot::channel::<()>();
    let origin_b = Arc::clone(&origin);
    let leg_b = std::thread::spawn(move || -> anyhow::Result<Vec<u8>> {
        let rt = pull_runtime()?;
        rt.block_on(async {
            let stream = open(&*origin_b, hash).await?;
            let _ = b_open_tx.send(());
            tokio::time::timeout(BODY_DEADLINE, drain(stream))
                .await
                .map_err(|_| {
                    anyhow::anyhow!("leg B's body stalled after leg A's runtime dropped")
                })?
        })
    });
    b_open_rx.await?;

    // Drop leg A's runtime while B's body is still in flight, then let the
    // server finish B's body.
    drop_a_tx.send(())?;
    tokio::task::spawn_blocking(move || leg_a.join())
        .await?
        .map_err(|_| anyhow::anyhow!("leg A panicked"))??;
    server.release_held();

    let body_b = tokio::task::spawn_blocking(move || leg_b.join())
        .await?
        .map_err(|_| anyhow::anyhow!("leg B panicked"))??;
    anyhow::ensure!(body_b == BODY, "leg B got {} bytes", body_b.len());
    anyhow::ensure!(
        server.accepts() == 1,
        "leg B did not reuse leg A's connection ({} accepted), so the test proved nothing",
        server.accepts()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_keep_alive_survives_a_dropped_pull_runtime() -> anyhow::Result<()> {
    let server = KeepAliveServer::start(None).await?;
    let origin = http_origin(&server)?;
    keep_alive_survives_a_dropped_pull_runtime(&server, origin).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_in_flight_fetch_survives_its_connection_openers_runtime_drop() -> anyhow::Result<()> {
    let server = KeepAliveServer::start(Some(1)).await?;
    let origin = http_origin(&server)?;
    in_flight_fetch_survives_its_connection_openers_runtime_drop(&server, origin).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_keep_alive_survives_a_dropped_pull_runtime() -> anyhow::Result<()> {
    let server = KeepAliveServer::start(None).await?;
    let origin = s3_origin(&server).await?;
    keep_alive_survives_a_dropped_pull_runtime(&server, origin).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_in_flight_fetch_survives_its_connection_openers_runtime_drop() -> anyhow::Result<()> {
    let server = KeepAliveServer::start(Some(1)).await?;
    let origin = s3_origin(&server).await?;
    in_flight_fetch_survives_its_connection_openers_runtime_drop(&server, origin).await
}
