//! Network origins keep their pooled connections alive across the throwaway
//! runtimes that call them (#1673, #1675).
//!
//! The node runs each serve-miss pull leg on its own current-thread runtime
//! and drops that runtime when the leg returns. [`HttpOrigin`] and
//! [`S3Origin`] share one keep-alive pool across every caller, and run each
//! request send on the runtime that built the origin, so a pooled connection
//! outlives the pull leg that opened it.
//!
//! Each test builds the origin on the test's runtime, then calls it from
//! threads that each own a current-thread runtime, the way a pull leg does. A
//! raw HTTP/1.1 server counts the TCP connections it accepts, so a test can
//! tell a reused keep-alive connection from a fresh one. Every origin
//! operation that sends a request ([`Op`]) is tested as the one that opens
//! the connection, because the opener decides which runtime owns it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use decdn_cache::{
    Hash, HttpOrigin, Origin, OriginFetch, OriginRangeFetch, OriginRangeRequest, OutboardFetch,
    S3Credentials, S3Origin, S3OriginConfig, parse_origin_url,
};
use futures_util::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;

/// The object every response serves, in full or as the requested range.
/// Large enough that a held response can split its body across two writes.
const BODY: &[u8] = &[0x5A; 64 * 1024];

/// How long to let a finished leg hand its connection back to the pool
/// before the next leg checks one out. hyper-util returns the connection on
/// a background task, so a leg that starts at once can race it and dial a
/// second connection.
const POOL_SETTLE: Duration = Duration::from_secs(1);

/// Ceiling on one leg's whole operation. A connection that dies with another
/// leg's runtime can leave a request waiting forever, and the S3 path has no
/// headers timeout of its own.
const LEG_DEADLINE: Duration = Duration::from_secs(10);

/// A keep-alive HTTP/1.1 server. Every request reads the same object,
/// [`BODY`], whatever the path: a `Range: bytes=a-b` request gets `206` and
/// that slice, any other GET gets `200` and the whole object, and a HEAD gets
/// the `200` headers alone.
struct KeepAliveServer {
    url: String,
    accepts: Arc<AtomicUsize>,
    hold: Arc<Hold>,
}

/// The server's hold point: the response to one request sends its headers
/// and the first half of its body, signals [`Self::holding`], then waits for
/// [`Self::release`] before it sends the rest.
struct Hold {
    index: Option<usize>,
    holding: Notify,
    release: Notify,
}

impl KeepAliveServer {
    /// Start the server. `hold` is the 0-based index, counted across all
    /// connections, of the request whose response body the server holds.
    async fn start(hold: Option<usize>) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", listener.local_addr()?);
        let accepts = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let hold = Arc::new(Hold {
            index: hold,
            holding: Notify::new(),
            release: Notify::new(),
        });
        let (accepts_srv, hold_srv) = (Arc::clone(&accepts), Arc::clone(&hold));
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                accepts_srv.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(serve_connection(
                    sock,
                    Arc::clone(&requests),
                    Arc::clone(&hold_srv),
                ));
            }
        });
        Ok(Self { url, accepts, hold })
    }

    fn accepts(&self) -> usize {
        self.accepts.load(Ordering::SeqCst)
    }
}

/// Parse a `Range: bytes=a-b` value out of a request head.
fn requested_range(head: &str) -> Option<(usize, usize)> {
    let value = head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.trim()
            .eq_ignore_ascii_case("range")
            .then(|| value.trim())
    })?;
    let (start, end) = value.strip_prefix("bytes=")?.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

/// Answer requests on one connection until the client closes it.
async fn serve_connection(
    mut sock: tokio::net::TcpStream,
    requests: Arc<AtomicUsize>,
    hold: Arc<Hold>,
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
        let head = String::from_utf8_lossy(buf.get(..head_end).unwrap_or(&[])).into_owned();
        buf.drain(..head_end);
        let index = requests.fetch_add(1, Ordering::SeqCst);
        let (status, extra, body) = match requested_range(&head) {
            Some((start, end)) => match BODY.get(start..=end) {
                Some(slice) => (
                    "206 Partial Content",
                    format!("Content-Range: bytes {start}-{end}/{}\r\n", BODY.len()),
                    slice,
                ),
                None => return,
            },
            None => ("200 OK", String::new(), BODY),
        };
        let response_head = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\n{extra}\
             Content-Type: application/octet-stream\r\n\r\n",
            body.len()
        );
        if sock.write_all(response_head.as_bytes()).await.is_err() {
            return;
        }
        if head.starts_with("HEAD ") {
            continue;
        }
        let (first, rest) = body.split_at(body.len() / 2);
        if sock.write_all(first).await.is_err() {
            return;
        }
        if hold.index == Some(index) {
            hold.holding.notify_one();
            hold.release.notified().await;
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

/// Every origin operation that sends a request.
#[derive(Debug, Clone, Copy)]
enum Op {
    /// [`Origin::fetch`]: a whole-object GET, body streamed.
    Fetch,
    /// [`Origin::fetch_range_data`]: a ranged GET, body buffered. The
    /// own-origin serve-miss pull leg reads through this.
    Range,
    /// [`Origin::fetch_outboard`]: a GET of the sibling outboard, buffered.
    Outboard,
    /// [`Origin::size`]: a HEAD.
    Size,
}

/// The range [`Op::Range`] asks for: most of the object, so a held response
/// still splits its body across two writes.
const RANGE: OriginRangeRequest = OriginRangeRequest {
    fetch_start: 1024,
    fetch_end: 60 * 1024,
};

/// Run `op` to completion and check what it returned.
async fn run_op(origin: &dyn Origin, op: Op, hash: Hash) -> anyhow::Result<()> {
    match op {
        Op::Fetch => {
            let OriginFetch::Found { mut stream, .. } = origin.fetch(hash, 1 << 30).await? else {
                anyhow::bail!("fetch did not find the object");
            };
            let mut body = Vec::new();
            while let Some(chunk) = stream.next().await {
                body.extend_from_slice(&chunk?);
            }
            anyhow::ensure!(body == BODY, "fetch got {} bytes", body.len());
        }
        Op::Range => {
            let OriginRangeFetch::Ranged { data } = origin.fetch_range_data(hash, RANGE).await?
            else {
                anyhow::bail!("the origin declined the range");
            };
            let want = usize::try_from(RANGE.fetch_start)?..usize::try_from(RANGE.fetch_end)?;
            anyhow::ensure!(
                Some(data.as_ref()) == BODY.get(want),
                "range got {} bytes",
                data.len()
            );
        }
        Op::Outboard => {
            let OutboardFetch::Found(data) = origin.fetch_outboard(hash, 1 << 20).await? else {
                anyhow::bail!("fetch_outboard did not find the outboard");
            };
            anyhow::ensure!(data.as_ref() == BODY, "outboard got {} bytes", data.len());
        }
        Op::Size => {
            let size = origin.size(hash).await?;
            anyhow::ensure!(size == Some(u64::try_from(BODY.len())?), "size {size:?}");
        }
    }
    Ok(())
}

fn pull_runtime() -> anyhow::Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?)
}

/// Run `op` on `rt` within [`LEG_DEADLINE`].
fn run_leg(
    rt: &tokio::runtime::Runtime,
    origin: &dyn Origin,
    op: Op,
    hash: Hash,
) -> anyhow::Result<()> {
    rt.block_on(async {
        tokio::time::timeout(LEG_DEADLINE, run_op(origin, op, hash))
            .await
            .map_err(|_| anyhow::anyhow!("{op:?} hung"))?
    })
}

/// Run `op` on a new current-thread runtime on its own thread, then drop
/// that runtime: one whole pull leg.
async fn leg_on_throwaway_runtime(
    origin: Arc<dyn Origin>,
    op: Op,
    hash: Hash,
) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || {
        let rt = pull_runtime()?;
        let outcome = run_leg(&rt, &*origin, op, hash);
        drop(rt);
        outcome
    })
    .await?
}

/// Leg A runs `opener` on its own runtime and opens the connection, then its
/// runtime drops. Leg B runs a fetch on a new runtime and must reuse the
/// pooled connection: it lives on the runtime that built the origin, so A's
/// runtime drop does not close it.
async fn keep_alive_survives_a_dropped_pull_runtime(
    origin_for: OriginFor,
    opener: Op,
) -> anyhow::Result<()> {
    let server = KeepAliveServer::start(None).await?;
    let origin = origin_for.build(&server).await?;
    let hash = Hash::new(b"keep-alive-marker");
    leg_on_throwaway_runtime(Arc::clone(&origin), opener, hash).await?;
    tokio::time::sleep(POOL_SETTLE).await;
    leg_on_throwaway_runtime(origin, Op::Fetch, hash).await?;
    anyhow::ensure!(
        server.accepts() == 1,
        "the second leg dialled a new connection instead of reusing the one {opener:?} \
         opened ({} accepted)",
        server.accepts()
    );
    Ok(())
}

/// The #1673 shape. Leg A fetches, opening the connection, and its runtime
/// stays up. Leg B runs `op` on the pooled connection, and the server holds
/// B's response mid-body while A's runtime drops. B must still get every byte.
async fn in_flight_request_survives_its_connection_openers_runtime_drop(
    origin_for: OriginFor,
    op: Op,
) -> anyhow::Result<()> {
    let server = KeepAliveServer::start(Some(1)).await?;
    let origin = origin_for.build(&server).await?;
    let hash = Hash::new(b"in-flight-marker");

    // Leg A: fetch on its own runtime, report, then keep the runtime alive
    // until told to drop it.
    let (a_done_tx, a_done_rx) = tokio::sync::oneshot::channel::<anyhow::Result<()>>();
    let (drop_a_tx, drop_a_rx) = std::sync::mpsc::channel::<()>();
    let origin_a = Arc::clone(&origin);
    let leg_a = std::thread::spawn(move || -> anyhow::Result<()> {
        let rt = pull_runtime()?;
        let _ = a_done_tx.send(run_leg(&rt, &*origin_a, Op::Fetch, hash));
        let _ = drop_a_rx.recv();
        drop(rt);
        Ok(())
    });
    a_done_rx.await??;
    tokio::time::sleep(POOL_SETTLE).await;

    // Leg B: run `op`; the server holds its response mid-body.
    let origin_b = Arc::clone(&origin);
    let leg_b = std::thread::spawn(move || -> anyhow::Result<()> {
        run_leg(&pull_runtime()?, &*origin_b, op, hash)
    });
    let join_b = || async move {
        tokio::task::spawn_blocking(move || leg_b.join())
            .await?
            .map_err(|_| anyhow::anyhow!("leg B panicked"))?
    };
    if tokio::time::timeout(LEG_DEADLINE, server.hold.holding.notified())
        .await
        .is_err()
    {
        // B never reached the hold point: surface its own error.
        join_b().await?;
        anyhow::bail!("leg B finished without reaching the server's hold point");
    }

    // Drop leg A's runtime while B's body is in flight, then let the server
    // finish it.
    drop_a_tx.send(())?;
    tokio::task::spawn_blocking(move || leg_a.join())
        .await?
        .map_err(|_| anyhow::anyhow!("leg A panicked"))??;
    server.hold.release.notify_one();

    join_b().await?;
    anyhow::ensure!(
        server.accepts() == 1,
        "leg B did not reuse leg A's connection ({} accepted), so the test proved nothing",
        server.accepts()
    );
    Ok(())
}

/// Which origin backend a test builds.
#[derive(Debug, Clone, Copy)]
enum OriginFor {
    Http,
    S3,
}

impl OriginFor {
    async fn build(self, server: &KeepAliveServer) -> anyhow::Result<Arc<dyn Origin>> {
        match self {
            Self::Http => http_origin(server),
            Self::S3 => s3_origin(server).await,
        }
    }
}

macro_rules! keep_alive_tests {
    ($($name:ident: $origin:ident, $op:ident;)*) => {$(
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $name() -> anyhow::Result<()> {
            keep_alive_survives_a_dropped_pull_runtime(OriginFor::$origin, Op::$op).await
        }
    )*};
}

macro_rules! in_flight_tests {
    ($($name:ident: $origin:ident, $op:ident;)*) => {$(
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $name() -> anyhow::Result<()> {
            in_flight_request_survives_its_connection_openers_runtime_drop(
                OriginFor::$origin,
                Op::$op,
            )
            .await
        }
    )*};
}

keep_alive_tests! {
    http_keep_alive_survives_a_dropped_fetch_runtime: Http, Fetch;
    http_keep_alive_survives_a_dropped_range_runtime: Http, Range;
    http_keep_alive_survives_a_dropped_outboard_runtime: Http, Outboard;
    http_keep_alive_survives_a_dropped_size_runtime: Http, Size;
    s3_keep_alive_survives_a_dropped_fetch_runtime: S3, Fetch;
    s3_keep_alive_survives_a_dropped_range_runtime: S3, Range;
    s3_keep_alive_survives_a_dropped_outboard_runtime: S3, Outboard;
    s3_keep_alive_survives_a_dropped_size_runtime: S3, Size;
}

in_flight_tests! {
    http_in_flight_fetch_survives_its_connection_openers_runtime_drop: Http, Fetch;
    http_in_flight_range_survives_its_connection_openers_runtime_drop: Http, Range;
    s3_in_flight_fetch_survives_its_connection_openers_runtime_drop: S3, Fetch;
    s3_in_flight_range_survives_its_connection_openers_runtime_drop: S3, Range;
}
