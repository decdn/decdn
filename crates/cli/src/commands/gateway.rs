//! `decdn gateway`: serve `decdn:` URIs (#2449) to a browser over loopback
//! HTTP.
//!
//! **Content.** `GET /<hash>[/<path>][?ns=<id>]` serves
//! `decdn:<hash>[/<path>][?ns=<id>]`. A blob or a bundle entry streams its
//! verified bytes in order as they arrive; a whole bundle (`/<hash>/`) gets an
//! HTML index of its entries. A content request carries [`TOKEN_HEADER`] (the
//! gateway token) and [`HOST_HEADER`] (the host of the page it serves); the
//! browser extension adds both, so page scripts never see the token. The
//! gateway pays with the registered capability whose scope covers that host:
//! an exact host first, then `*.<domain>`, then `*`; between equal scopes, the
//! token holder's own registration before a page's, then the earlier
//! registration. A capability whose pull stops for funding before its first
//! byte is retired and the next covering one takes the request. `HEAD` gets
//! 405, since answering it would cost a paid pull. A bundle request without
//! the token redirects to the bundle's site, at no cost.
//!
//! **Sites.** Every bundle is also a website on its own browser origin,
//! `http://[<ns>.]<hash32>.localhost:<port>/` (`site`), so its pages run
//! isolated from every other bundle and from the gateway, with no extension.
//! `/` serves the bundle's `index.html` (or a file list), `/<path>` the entry
//! at `<path>`, and a directory path its `index.html`; HTML, SVG and scripts
//! are served as themselves, under a policy that confines the page to its
//! origin. A site's `decdn:` URIs are rewritten as the page streams
//! (`rewrite`) to `/_decdn/<hash>…` on the site's own origin, which serves
//! that content to the page's own requests and redirects a navigation to the
//! other bundle's site. The site origin takes no token: the browser's
//! `Sec-Fetch-*` headers decide what may spend (`site::may_spend`), and a
//! site pays with the capability that covers its host, as a page's request
//! does. `/_delegate` and `/_capability` answer on the gateway origin only.
//!
//! **Capabilities.** The gateway holds one delegate key per capability, in
//! memory only. `GET /_delegate` mints a fresh key and returns its address, so
//! no two callers see the same address and none can follow the user across
//! sites; a key no capability claims within [`PENDING_TTL`] is dropped. A
//! caller is a web page by its `http(s)` `Origin`, or the gateway token holder
//! from outside any page. The pages of one site (a registrable domain by the
//! Public Suffix List, so subdomains share it) hold at most a few keys,
//! pending or registered, within a bound shared by all pages; the token
//! holder mints outside both bounds.
//! `PUT /_capability` registers a `dcap1:` capability issued to a minted
//! address, with a scope. The capability's owner signature must recover the
//! on-chain owner of its pool on the gateway's deployment. A page registers
//! through CORS, and its scope must cover its own origin's host, whether or
//! not the request also carries the token: `example.com` may register
//! `example.com`, `*.example.com` or `*`, never another host, and never a
//! wildcard over a public suffix such as `*.com` or `*.github.io`. The token
//! holder registers any scope. `GET /_capability` lists the registrations to
//! the token holder. A restart drops every key, and with it every capability
//! issued to one.
//!
//! **Boundaries.** On a loopback gateway only `/_delegate` and `/_capability`
//! answer cross-origin; content never does, so a page can neither read a
//! response nor set the token or host headers itself. The token lives in `<data-dir>/token`,
//! owner-only. The gateway listens on loopback only and checks `Host` against
//! DNS rebinding: it answers its own loopback names and well-formed site
//! names on its port, nothing else.
//!
//! **Public.** With `--public-url <origin>` the gateway serves anyone at that
//! origin, behind a proxy that terminates TLS: `<origin>/<hash>[/<path>]`,
//! every bundle as a site at `<scheme>://[<ns>.]<hash32>.<host>/`, and a
//! namespace at `<scheme>://<ns>.<host>/` once it has a pointer here. There
//! is no token and no capability: every request pays from the keystore's own
//! pool (`worker::Payment::Pool`), and the allowance (`allowance`) bounds the
//! bytes the gateway delivers per day over every request; a namespace the
//! operator gave `--allowance <ns>=<bytes>` spends that on the requests its
//! pointer serves instead. A request past it,
//! or one whose known size does not fit what is left, answers `402` with the
//! canonical `decdn:` URI in a `Link` header and a JSON body, so a paying
//! client continues on its own; a stream of unknown size is cut when the
//! allowance runs out; a pool that cannot pay answers `503` as a `402` does.
//! An entry's pull is capped at the size its manifest declares, so a manifest
//! cannot make the gateway pay for more than it says, and a declared size is
//! what `Content-Length` and the allowance count on. A sized entry takes one
//! `Range`: the response is `206` over exactly that window, the pull pays
//! only for the chunk-group-aligned span around it (ADR 038), the allowance
//! admits and charges the request by that span, and a window past the end is
//! `416`; a range the gateway cannot honour (several, a
//! rewritten page, a blob of unknown size) is ignored, as HTTP allows, so a
//! `curl -r`, a resumed download and a BEP 19 webseed all work. `HEAD` is answered from
//! the bundle manifest, at no pull of the entry. The API paths answer `404`,
//! and a site's `Sec-Fetch-*` headers decide nothing: the operator pays,
//! within the allowance. Every answer is readable by every origin
//! (`readable_anywhere`), and `OPTIONS` is a preflight
//! (`public_preflight`). A pull runs a small read-ahead
//! (`PUBLIC_READ_AHEAD_BYTES`), and a response that does not end cleanly (a
//! client that drops it, a cut, a fault) is charged what the pull may have
//! paid past the frames it delivered (`DROP_PENALTY_BYTES`), so a client
//! that connects and drops spends no more than it is charged. An upstream
//! failure answers a bare `502`, with the detail in the log. The buyer store
//! lives in `<data-dir>/store`, so the pool and its lanes survive a restart.
//!
//! **State.** Every gateway counts what it served, refused and failed
//! (`metrics`). `GET /_status` returns that, the pulls in flight, the
//! capabilities, a public gateway's allowance and pointers, and what the
//! buyer-pool store holds, as one JSON document (`status`). The dashboard
//! (`dashboard`) is a page that polls it. A loopback gateway answers both on
//! its own host, `/_status` behind the token; a public gateway answers
//! neither on its public host. `--metrics-listen` serves `GET /metrics`
//! (Prometheus text), `/_status` and the dashboard on a loopback address,
//! with no token, for the operator.
//!
//! **Pointers.** `PUT /_pointer` on the bare host takes a namespace pointer
//! (`pointer`): an EIP-712 record, signed by the namespace's owner on the
//! `PublisherRegistry` the gateway runs against, that names the bundle
//! `<ns>.<host>` serves. The gateway recovers the signer, reads the owner
//! on-chain, and stores the pointer under `<data-dir>/pointers/<ns>.json`
//! when its `seq` is above every one stored from that signer. The owner
//! lookups come out of a budget per minute (`POINTER_LOOKUPS_PER_MINUTE`),
//! so an anonymous flood of pointers costs the operator's RPC no more than
//! that; a signer the gateway stored a pointer from before draws on a budget
//! of its own per namespace (`KNOWN_SIGNER_LOOKUPS_PER_MINUTE`), which a
//! stranger's keys cannot spend, so the flood does not keep a publisher from
//! updating. A first pointer, or a new owner's, waits for the shared budget;
//! limits per client belong on the proxy, which knows the client. Pointer
//! writes run one at a time, lookup included, so a stale owner read never
//! overtakes a fresher one. `decdn publish point` signs and sends one.
//! Sites are separate browser origins; they are separate sites, with their
//! own cookies and site-scoped state, only when the public host is a public
//! suffix: the operator registers it on the Public Suffix List, as
//! `github.io` is.
//!
//! **Pulls.** Every pull runs the blob fetch of `decdn get` with its bytes
//! going to the HTTP response, on one long-lived worker thread: the endpoint,
//! the buyer-pool store and the connections to nodes are built once per
//! gateway run, and up to 16 pulls stream at once, so a response whose reader
//! stops holds only its own pull.

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::Eip712Domain;
use anyhow::Context as _;
use bytes::Bytes;
use decdn_client::bundle::BundleManifest;
use decdn_client::session::ChainConfig;
use decdn_client::uri::{DecdnUri, UriTarget};
use decdn_common::address::parse_nonzero_address;
use decdn_common::cli::{self, ClientFetchArgs, GatewayArgs};
use decdn_common::config::{FileConfig, load_file_config};
use decdn_common::redact::{sanitize_err_chain, sanitize_error_sources};
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::publisher_registry::PublisherRegistry;
use decdn_incentive::{CapabilityGrant, PoolId, voucher_domain};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt as _, Full, Limited, StreamBody};
use hyper::body::{Body as HttpBody, Frame};
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt as _, DuplexStream};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};

use super::fetch::{
    BlobFetch, Credential, MAX_MANIFEST_BYTES, manifest_cap_mb, manifest_parse_context,
    resolve_chain,
};
use crate::known_chains;

mod allowance;
mod dashboard;
mod metrics;
pub(crate) mod pointer;
mod rewrite;
mod site;
mod status;
mod worker;

use allowance::Allowance;
use metrics::Metrics;
use pointer::{Pointers, SetError, SignedPointer, pointer_domain};
use rewrite::Rewrite;
use site::{Route, Site, SiteDomain};
use worker::{FetchBackend, Job, Payment, PullWorker};

/// The request header carrying the gateway token.
pub const TOKEN_HEADER: &str = "x-decdn-token";

/// The request header naming the host of the page a content request serves.
pub const HOST_HEADER: &str = "x-decdn-host";

/// How long a minted delegate waits for a capability before it is dropped.
pub const PENDING_TTL: Duration = Duration::from_mins(15);

/// The most live delegates, pending or registered, the pages of one site
/// hold, so no site can use up another's room or stock capabilities without
/// bound, however many subdomains it serves pages from.
const MAX_DELEGATES_PER_SITE: usize = 8;

/// The most live delegates all pages hold together: the gateway holds page
/// keys within a fixed bound.
const MAX_PAGE_DELEGATES: usize = 1024;

/// The largest `PUT /_capability` or `PUT /_pointer` body.
const MAX_BODY_BYTES: usize = 64 * 1024;

/// The most on-chain owner lookups `PUT /_pointer` makes per minute. A
/// publisher points a namespace a few times a day; past the budget the
/// answer is 429 until the minute turns, and the operator's RPC is not an
/// anonymous caller's to spend.
const POINTER_LOOKUPS_PER_MINUTE: u32 = 60;

/// The owner lookups per minute a signer the gateway stored a pointer from
/// before gets on that namespace, apart from the shared budget. Such a
/// signer was the owner once, so a stranger's keys cannot spend this; a
/// publisher updates a few times a day.
const KNOWN_SIGNER_LOOKUPS_PER_MINUTE: u32 = 10;

/// How long an owner lookup may take. The lookup runs under the pointer
/// write lock, so a hung RPC would otherwise hold every pointer write.
const OWNER_LOOKUP_TIMEOUT: Duration = Duration::from_secs(15);

/// A gateway pull that makes no verified progress for this long gives up, so
/// a stuck request does not hold its pull slot.
const GIVE_UP_AFTER_SECS: u64 = 120;

/// The most pulls that run at once; a request past it waits for a slot. Each
/// pull stripes across up to `--max-sources` holders, so this also bounds the
/// streams the gateway opens on one node's connection.
const MAX_PULLS: usize = 16;

/// The most parsed manifests the gateway keeps.
const MAX_CACHED_MANIFESTS: usize = 16;

/// The manifest bytes the cache keeps across all its entries. The newest
/// entry stays even when it alone exceeds this.
const MANIFEST_CACHE_BYTES: usize = 64 << 20;

/// The duplex buffer between a pull and its HTTP response.
const PIPE_BYTES: usize = 1 << 20;

/// The read size for response chunks.
const CHUNK_BYTES: usize = 256 * 1024;

/// The read-ahead window of a public gateway's pulls: how far a pull runs,
/// and pays, ahead of what its response has taken. Small, so a dropped
/// response leaves little paid and undelivered; the client raises it to its
/// pull-window floor when this is below it.
const PUBLIC_READ_AHEAD_BYTES: u64 = 2 << 20;

/// The most a pull may have paid for beyond the frames its response
/// delivered: the read-ahead window, the pipe, and one frame in flight. A
/// response that does not end cleanly is charged this against the allowance,
/// since nothing reports what the pull paid.
const DROP_PENALTY_BYTES: u64 = {
    let window = if PUBLIC_READ_AHEAD_BYTES > decdn_client::PULL_WINDOW_FLOOR {
        PUBLIC_READ_AHEAD_BYTES
    } else {
        decdn_client::PULL_WINDOW_FLOOR
    };
    window + PIPE_BYTES as u64 + CHUNK_BYTES as u64
};

type Body = BoxBody<Bytes, std::io::Error>;

/// Run the gateway until Ctrl-C.
///
/// # Errors
///
/// When `--listen` is not a loopback address and `--public-url` is not set,
/// when a loopback gateway is given keystore or capability flags, when
/// `--public-url` is not an `http(s)` origin, when the data dir, token or
/// listener cannot be set up, when a public gateway's keystore cannot be
/// unlocked, when the chain coordinates or a public gateway's
/// `PublisherRegistry` cannot be resolved, or when a stored pointer is
/// damaged.
pub async fn gateway(args: &GatewayArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let public_url = args.public.public_url.clone();
    if let Some(url) = &public_url {
        anyhow::ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some()
                && url.path() == "/"
                && url.query().is_none()
                && url.fragment().is_none(),
            "--public-url must be an http(s) origin with no path, such as https://decdn.link"
        );
    } else {
        anyhow::ensure!(
            args.listen.ip().is_loopback(),
            "--listen must be a loopback address unless --public-url is set"
        );
    }
    let mut common = fetch_flags(&args.common, public_url.is_some())?;
    let file = load_file_config(config_path)?;
    fill_chain_defaults(&mut common, &file)?;
    let resolved = resolve_chain(&common, &file)?;
    if public_url.is_some() && common.keystore.is_none() {
        // The keystore resolves under the data dir; the buyer store moves
        // under it below, so the keystore is pinned first.
        common.keystore = Some(resolved.keystore.clone());
    }
    let chain = resolved.config;
    common.give_up_after_secs = common.give_up_after_secs.or(Some(GIVE_UP_AFTER_SECS));
    let root = common
        .data_dir
        .clone()
        .unwrap_or_else(|| cli::common::expand_tilde(Path::new("~/.decdn/gateway")));
    private_dir(&root)?;
    let token = match &public_url {
        Some(_) => None,
        None => Some(load_or_create_token(&root.join("token"))?),
    };
    let (store_dir, temp_store) = store_dir(&root, public_url.is_some())?;
    common.data_dir = Some(store_dir);
    let (sites, public) = mode(args, &root, chain.chain_id, &file)?;

    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("listen on {}", args.listen))?;
    let pulls = PullWorker::start(
        FetchBackend {
            common: common.clone(),
            config_path: config_path.map(Path::to_path_buf),
            pool: public_url.is_some(),
        },
        MAX_PULLS,
    )
    .await
    .context("start the gateway's pull worker")?;
    let gateway = Arc::new(Gateway {
        common,
        token,
        domain: voucher_domain(chain.chain_id, chain.payment_pool),
        chain,
        delegates: Mutex::new(HashMap::new()),
        pulls,
        manifests: std::sync::Mutex::new(ManifestCache::default()),
        manifest_fill: Mutex::new(()),
        hosts: allowed_hosts(args.listen),
        sites,
        metrics: Metrics::default(),
        active: std::sync::Mutex::new(HashMap::new()),
        next_pull: AtomicU64::new(0),
        started: Instant::now(),
        started_unix: now_secs(),
        listen: args.listen,
        public,
    });

    if let Some(metrics) = args.public.metrics_listen {
        anyhow::ensure!(
            metrics.ip().is_loopback(),
            "--metrics-listen must be a loopback address: it serves the operator, with no token, \
             not the public"
        );
        let listener = TcpListener::bind(metrics)
            .await
            .with_context(|| format!("listen for metrics on {metrics}"))?;
        tokio::spawn(serve_metrics(Arc::clone(&gateway), listener));
    }
    announce(&gateway, &root);
    let served = serve(&gateway, &listener).await;
    // The worker's queued lane writes land before the store dir is removed.
    gateway.pulls.stop().await;
    drop(temp_store);
    served
}

/// Print where the gateway serves, and on loopback its token and the
/// dashboard link that hands the token to the page.
fn announce(gateway: &Gateway, root: &Path) {
    match (&gateway.sites, &gateway.token) {
        (SiteDomain::Public { origin }, _) => println!(
            "decdn gateway serving {origin} on {} (Ctrl-C to stop)",
            gateway.listen
        ),
        (SiteDomain::Loopback { .. }, token) => {
            let token = token.as_deref().unwrap_or_default();
            println!(
                "decdn gateway on http://{}/ (Ctrl-C to stop)\ngateway token: {token} (also in \
                 {})\ndashboard: http://{}/#token={token}",
                gateway.listen,
                root.join("token").display(),
                gateway.listen,
            );
            if !dashboard::built() {
                println!(
                    "this binary has no dashboard built in; the page at / says how to build it"
                );
            }
        }
    }
}

/// Where the buyer store lives, created owner-only. A public gateway's pool
/// and lanes outlive the process, so its store stays under `root`. A loopback
/// gateway's keys die with the process, so their lane records do too: its
/// store lives in a directory removed when the returned guard drops.
fn store_dir(root: &Path, public: bool) -> anyhow::Result<(PathBuf, Option<tempfile::TempDir>)> {
    let (dir, guard) = if public {
        (root.join("store"), None)
    } else {
        let dir = tempfile::Builder::new()
            .prefix("store-")
            .tempdir_in(root)
            .context("create the gateway's buyer store dir")?;
        (dir.path().to_path_buf(), Some(dir))
    };
    private_dir(&dir)?;
    Ok((dir, guard))
}

/// Where `args` put the sites, and what a public gateway holds: its
/// allowance, its pointers under `<root>/pointers`, and the registry on
/// `chain_id` it verifies them against.
///
/// # Errors
///
/// A public gateway's registry cannot be resolved, or its pointer store
/// cannot be read.
fn mode(
    args: &GatewayArgs,
    root: &Path,
    chain_id: u64,
    file: &FileConfig,
) -> anyhow::Result<(SiteDomain, Option<Public>)> {
    let Some(origin) = args.public.public_url.clone() else {
        return Ok((
            SiteDomain::Loopback {
                listen: args.listen,
            },
            None,
        ));
    };
    let registry = registry_address(
        args.public.publisher_registry_address.as_deref(),
        file,
        chain_id,
    )?;
    let pointers = Pointers::load(&root.join("pointers"), chain_id, registry)?;
    Ok((
        SiteDomain::Public { origin },
        Some(Public {
            allowance: Allowance::with_namespaces(
                args.public.allowance_bytes_per_day,
                args.public.allowance.iter().copied(),
            ),
            pointers,
            registry,
            lookups: std::sync::Mutex::new(Budgets::default()),
            pointer_writes: Mutex::new(()),
        }),
    ))
}

/// The `PublisherRegistry` a public gateway verifies pointers against: the
/// `--publisher-registry-address` flag, else
/// `blockchain.publisher_registry_address` in the config file, else the
/// built-in testnet's when the gateway runs on that chain. On another chain
/// nothing is assumed: the operator names it.
fn registry_address(
    flag: Option<&str>,
    file: &FileConfig,
    chain_id: u64,
) -> anyhow::Result<Address> {
    let from_file = file
        .blockchain
        .as_ref()
        .and_then(|b| b.publisher_registry_address.as_deref());
    if let Some(given) = flag.or(from_file) {
        return parse_nonzero_address(given, "publisher_registry_address");
    }
    if let Some(known) = known_chains::resolve(None)?
        && known.chain_id == chain_id
    {
        return parse_nonzero_address(
            &known.addresses()?.publisher_registry,
            "publisher_registry_address",
        );
    }
    anyhow::bail!(
        "a public gateway verifies namespace pointers against the PublisherRegistry, and none \
         is known for chain {chain_id}: pass --publisher-registry-address or set \
         blockchain.publisher_registry_address"
    )
}

/// Serve the metrics exposition on `listener`, a loopback address, for as
/// long as the gateway runs: `GET /metrics` answers Prometheus text, every
/// other request 404.
async fn serve_metrics(gateway: Arc<Gateway>, listener: TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let gateway = Arc::clone(&gateway);
        tokio::spawn(async move {
            let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                let gateway = Arc::clone(&gateway);
                async move {
                    let response = match (req.method(), req.uri().path()) {
                        (&Method::GET, "/metrics") => typed(
                            StatusCode::OK,
                            gateway.metrics.render(),
                            "text/plain; version=0.0.4; charset=utf-8",
                        ),
                        (&Method::GET, path) => {
                            gateway.operator(path, true).await.unwrap_or_else(|| {
                                text(StatusCode::NOT_FOUND, "GET /, /_status, /metrics\n")
                            })
                        }
                        _ => text(StatusCode::NOT_FOUND, "GET /, /_status, /metrics\n"),
                    };
                    Ok::<_, Infallible>(response)
                }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(Duration::from_secs(10))
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                tracing::debug!(error = %e, "metrics connection ended");
            }
        });
    }
}

/// Accept connections on `listener` until Ctrl-C.
async fn serve(gateway: &Arc<Gateway>, listener: &TcpListener) -> anyhow::Result<()> {
    loop {
        let (stream, _) = tokio::select! {
            accepted = listener.accept() => accepted.context("accept a connection")?,
            _ = tokio::signal::ctrl_c() => return Ok(()),
        };
        let gateway = Arc::clone(gateway);
        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let gateway = Arc::clone(&gateway);
                async move { Ok::<_, Infallible>(gateway.handle(req).await) }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(Duration::from_secs(10))
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                tracing::debug!(error = %e, "gateway connection ended");
            }
        });
    }
}

/// The fetch flags the gateway pulls with. A public gateway pays from the
/// keystore the flags name (a `--capability` makes it a delegate of that
/// pool), so it keeps them all. A loopback gateway holds its own delegate
/// keys, so its key and capability flags are refused, and a keystore
/// password file (often `DECDN_KEYSTORE_PASSWORD_FILE` in the environment)
/// is dropped unread.
fn fetch_flags(common: &ClientFetchArgs, public: bool) -> anyhow::Result<ClientFetchArgs> {
    if public {
        return Ok(common.clone());
    }
    anyhow::ensure!(
        common.keystore.is_none()
            && common.capability.is_none()
            && common.capability_file.is_none(),
        "the gateway holds its own delegate keys: register capabilities through \
         `PUT /_capability` instead of --keystore / --capability / --capability-file"
    );
    let mut common = common.clone();
    common.keystore_password_file = None;
    Ok(common)
}

/// Chain coordinates neither a flag nor the config file sets come from the
/// built-in testnet, so `decdn gateway` runs with no config.
fn fill_chain_defaults(common: &mut ClientFetchArgs, file: &FileConfig) -> anyhow::Result<()> {
    let bc = file.blockchain.as_ref();
    let Some(chain) = known_chains::resolve(None)? else {
        return Ok(());
    };
    let addresses = chain.addresses()?;
    if common.rpc_url.is_none() && bc.and_then(|b| b.rpc_url.as_ref()).is_none() {
        common.rpc_url = Some(chain.public_rpc.to_owned());
    }
    if common.chain_id.is_none() && bc.and_then(|b| b.chain_id).is_none() {
        common.chain_id = Some(chain.chain_id);
    }
    if common.payment_pool_address.is_none()
        && bc.and_then(|b| b.payment_pool_address.as_ref()).is_none()
    {
        common.payment_pool_address = Some(addresses.payment_pool);
    }
    if common.slash_judge_address.is_none()
        && bc.and_then(|b| b.slash_judge_address.as_ref()).is_none()
    {
        common.slash_judge_address = Some(addresses.slash_judge);
    }
    if common.capacity_bond_address.is_none()
        && bc.and_then(|b| b.capacity_bond_address.as_ref()).is_none()
    {
        common.capacity_bond_address = Some(addresses.capacity_bond);
    }
    Ok(())
}

/// The `Host` header values the gateway answers: its own loopback address
/// under each name a browser may use. Anything else is a DNS-rebinding attempt.
fn allowed_hosts(listen: SocketAddr) -> Vec<String> {
    let port = listen.port();
    vec![
        listen.to_string(),
        format!("localhost:{port}"),
        format!("127.0.0.1:{port}"),
        format!("[::1]:{port}"),
    ]
}

/// Create `dir` owner-only.
fn private_dir(dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("restrict {}", dir.display()))?;
    }
    Ok(())
}

/// The gateway token in `path`, or a fresh random one written there
/// owner-only.
fn load_or_create_token(path: &Path) -> anyhow::Result<String> {
    if let Ok(token) = std::fs::read_to_string(path) {
        let token = token.trim().to_owned();
        anyhow::ensure!(
            token.len() == 64 && token.bytes().all(|b| b.is_ascii_hexdigit()),
            "{} is not a gateway token; delete it to make a new one",
            path.display()
        );
        return Ok(token);
    }
    let token = alloy::hex::encode(rand::random::<[u8; 32]>());
    std::fs::write(path, &token).with_context(|| format!("write {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("restrict {}", path.display()))?;
    }
    Ok(token)
}

/// Compare `given` with the token without leaking where they differ.
fn token_matches(token: &str, given: &str) -> bool {
    token.len() == given.len()
        && token
            .bytes()
            .zip(given.bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// The hosts a capability pays for.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Scope {
    /// Every host (`*`).
    Any,
    /// A domain and its subdomains (`*.example.com`).
    Domain(String),
    /// One host (`example.com`).
    Host(String),
}

impl Scope {
    fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim().to_ascii_lowercase();
        if s == "*" {
            return Ok(Self::Any);
        }
        if let Some(domain) = s.strip_prefix("*.") {
            return valid_host(domain).map(|()| Self::Domain(domain.to_owned()));
        }
        valid_host(&s).map(|()| Self::Host(s))
    }

    fn covers(&self, host: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Domain(domain) => {
                host == domain
                    || host
                        .strip_suffix(domain.as_str())
                        .is_some_and(|prefix| prefix.ends_with('.'))
            }
            Self::Host(h) => host == h,
        }
    }

    /// More specific scopes win when several cover a host.
    const fn rank(&self) -> u8 {
        match self {
            Self::Host(_) => 2,
            Self::Domain(_) => 1,
            Self::Any => 0,
        }
    }
}

impl std::fmt::Display for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Any => f.write_str("*"),
            Self::Domain(d) => write!(f, "*.{d}"),
            Self::Host(h) => f.write_str(h),
        }
    }
}

fn valid_host(host: &str) -> Result<(), String> {
    let ok = !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        });
    if ok {
        Ok(())
    } else {
        Err(format!("`{host}` is not a host name"))
    }
}

/// The host of a web page's `Origin` header (`https://example.com:8443` gives
/// `example.com`). `None` for an opaque origin and for every scheme other
/// than `http` and `https`, such as an extension's.
fn origin_host(origin: &str) -> Option<String> {
    let rest = origin
        .strip_prefix("https://")
        .or_else(|| origin.strip_prefix("http://"))?;
    let host = rest.split(['/', ':']).next()?;
    let host = host.to_ascii_lowercase();
    valid_host(&host).ok().map(|()| host)
}

/// Who calls `/_delegate` or `/_capability`.
#[derive(Debug, PartialEq, Eq)]
enum Caller {
    /// A web page, by its `Origin` and that origin's host. A page stays a
    /// page when its request also carries the token.
    Page { origin: String, host: String },
    /// The token holder, from outside any web page.
    Token,
}

impl Caller {
    fn of(origin: Option<&str>, authorized: bool) -> Result<Self, &'static str> {
        match origin {
            Some(origin) => {
                if let Some(host) = origin_host(origin) {
                    Ok(Self::Page {
                        origin: origin.to_owned(),
                        host,
                    })
                } else if origin == "null" {
                    Err("an opaque origin may not call the gateway\n")
                } else if authorized {
                    Ok(Self::Token)
                } else {
                    Err("token required\n")
                }
            }
            None if authorized => Ok(Self::Token),
            None => Err("a page calls with its Origin; use the gateway token otherwise\n"),
        }
    }

    /// The page origin, or `None` for the token holder.
    fn origin(&self) -> Option<&str> {
        match self {
            Self::Page { origin, .. } => Some(origin),
            Self::Token => None,
        }
    }

    /// The page's site, or `None` for the token holder.
    fn site(&self) -> Option<String> {
        match self {
            Self::Page { host, .. } => Some(site(host)),
            Self::Token => None,
        }
    }
}

/// The site `host` belongs to: its registrable domain by the Public Suffix
/// List (`a.b.example.co.uk` gives `example.co.uk`), or the host itself for an
/// IP address and a name with no registrable domain, such as `localhost`.
fn site(host: &str) -> String {
    if host.parse::<std::net::IpAddr>().is_ok() {
        return host.to_owned();
    }
    psl::domain_str(host).unwrap_or(host).to_owned()
}

/// Whether `caller` may register `scope`: a page only a scope that covers its
/// own host and is no wildcard over a public suffix (`*.io`, `*.github.io`),
/// the token holder any scope.
fn may_register(caller: &Caller, scope: &Scope) -> Result<(), String> {
    let Caller::Page { host, .. } = caller else {
        return Ok(());
    };
    if !scope.covers(host) {
        return Err(format!(
            "{host} may not register scope `{scope}`: it does not cover {host}"
        ));
    }
    if let Scope::Domain(domain) = scope
        && psl::suffix_str(domain) == Some(domain.as_str())
    {
        return Err(format!(
            "{host} may not register scope `{scope}`: {domain} is a public suffix"
        ));
    }
    Ok(())
}

/// Whether a pool whose on-chain owner is `on_chain` accepts a capability its
/// signature recovers to `signed_by`.
fn owner_matches(on_chain: Address, signed_by: Address, pool_id: PoolId) -> Result<(), String> {
    if on_chain.is_zero() {
        Err(format!(
            "pool {pool_id} does not exist on this deployment\n"
        ))
    } else if on_chain != signed_by {
        Err(format!(
            "the capability is not signed by the owner of pool {pool_id}\n"
        ))
    } else {
        Ok(())
    }
}

/// A delegate key and the capability registered to it, if any.
struct Delegate {
    signer: Arc<PrivateKeySigner>,
    minted: Instant,
    /// The site of the page that minted it, or `None` for the token holder.
    site: Option<String>,
    capability: Option<Registered>,
}

/// Whether a page of `site` may mint one more delegate beside `delegates`.
/// Pages share [`MAX_PAGE_DELEGATES`] and each site holds at most
/// [`MAX_DELEGATES_PER_SITE`], pending or registered; the token holder
/// (`None`) mints outside both bounds, so no page can stop the user's own
/// handshake.
fn may_mint(delegates: &HashMap<Address, Delegate>, site: Option<&str>) -> Result<(), String> {
    let Some(site) = site else {
        return Ok(());
    };
    let pages = || delegates.values().filter(|d| d.site.is_some());
    if pages().count() >= MAX_PAGE_DELEGATES {
        return Err("too many pages hold delegates\n".to_owned());
    }
    if pages().filter(|d| d.site.as_deref() == Some(site)).count() >= MAX_DELEGATES_PER_SITE {
        return Err(format!(
            "pages of {site} hold {MAX_DELEGATES_PER_SITE} delegates\n"
        ));
    }
    Ok(())
}

/// One registered capability as `GET /_capability` and `/_status` list it.
/// The fields are in key order: the TypeScript SDK reads this body, and it
/// stays the one `serde_json`'s sorted maps wrote before it had a type.
#[derive(serde::Serialize)]
struct Capability {
    /// The spending cap, in micro-USDC.
    cap: u64,
    /// The capability's expiry, Unix seconds.
    expiry: u64,
    /// The origin that registered it, or `None` for the token holder.
    origin: Option<String>,
    /// The pool it spends, `0x` hex.
    pool: String,
    /// The hosts it pays for.
    scope: String,
    /// The delegate key, EIP-55 hex.
    signer: String,
    /// Whether a pull ran out of its cap.
    spent: bool,
}

struct Registered {
    grant: CapabilityGrant,
    scope: Scope,
    /// The origin that registered it, or `None` for the token holder.
    origin: Option<String>,
    /// When it was registered.
    since: Instant,
    spent: bool,
}

impl Registered {
    /// The order among capabilities that cover one host, highest first: the
    /// more specific scope, then the token holder's own registration over a
    /// page's, then the earlier registration, so a later one never shadows
    /// a capability already in use.
    const fn preference(&self) -> (u8, bool, std::cmp::Reverse<Instant>) {
        (
            self.scope.rank(),
            self.origin.is_none(),
            std::cmp::Reverse(self.since),
        )
    }
}

#[derive(serde::Deserialize)]
struct RegisterRequest {
    capability: String,
    scope: String,
}

struct Gateway {
    common: ClientFetchArgs,
    /// The gateway token a loopback gateway's content requests carry;
    /// `None` on a public gateway, which takes no token.
    token: Option<String>,
    /// The chain a registered capability's pool must live on.
    chain: ChainConfig,
    /// The EIP-712 domain a capability's owner signs under.
    domain: Eip712Domain,
    delegates: Mutex<HashMap<Address, Delegate>>,
    /// Runs every pull, on one endpoint and one buyer-pool store.
    pulls: PullWorker,
    manifests: std::sync::Mutex<ManifestCache>,
    /// Held while a missing manifest is pulled, so concurrent requests for it
    /// pull and pay once. Cache hits never wait on it.
    manifest_fill: Mutex<()>,
    hosts: Vec<String>,
    /// Where site origins live, and what a `Host` names.
    sites: SiteDomain,
    /// What it served, refused and failed, for `/_status` and
    /// `--metrics-listen`.
    metrics: Metrics,
    /// The pulls in flight, by the id their guard took from `next_pull`.
    active: std::sync::Mutex<HashMap<u64, ActivePull>>,
    next_pull: AtomicU64,
    /// When the gateway started, for its uptime.
    started: Instant,
    /// When the gateway started, Unix seconds.
    started_unix: u64,
    /// The address its main listener is bound to.
    listen: SocketAddr,
    /// What a public gateway holds beyond a loopback one; `None` on loopback.
    public: Option<Public>,
}

/// What a public gateway holds beyond a loopback one.
struct Public {
    /// The bytes the gateway delivers per day from its pool.
    allowance: Allowance,
    /// The namespace pointers it serves `<ns>.<host>` from.
    pointers: Pointers,
    /// The `PublisherRegistry` a pointer's signer must own its namespace on.
    registry: Address,
    /// The owner lookups made this minute: the shared budget against
    /// [`POINTER_LOOKUPS_PER_MINUTE`], and each namespace's budget for its
    /// known signers against [`KNOWN_SIGNER_LOOKUPS_PER_MINUTE`].
    lookups: std::sync::Mutex<Budgets>,
    /// Held from a pointer's owner lookup through its store, so two pointers
    /// for one namespace resolve in the order their owners were read, and a
    /// read from before a transfer cannot overtake one from after it.
    pointer_writes: Mutex<()>,
}

/// The on-chain owner lookups `PUT /_pointer` made in one minute.
#[derive(Debug, Default)]
struct Lookups {
    minute: u64,
    used: u32,
}

impl Lookups {
    /// Take one lookup of `per_minute` in the minute of `now_secs`, or the
    /// seconds until the minute turns and one is free again.
    const fn take(&mut self, per_minute: u32, now_secs: u64) -> Result<(), u64> {
        let minute = now_secs / 60;
        if self.minute != minute {
            *self = Self { minute, used: 0 };
        }
        if self.used >= per_minute {
            return Err(60 - now_secs % 60);
        }
        self.used += 1;
        Ok(())
    }
}

/// The lookup budgets: one shared by every stranger, one per namespace for
/// the signers the gateway stored a pointer from before.
#[derive(Debug, Default)]
struct Budgets {
    shared: Lookups,
    known: HashMap<u64, Lookups>,
}

impl Public {
    /// Take one owner lookup from this minute's budget: the namespace's own
    /// when `known` names the namespace the signer is known on, else the
    /// shared one. `Err` is the seconds until the minute turns and one is
    /// free again.
    fn take_lookup(&self, known: Option<u64>, now_secs: u64) -> Result<(), u64> {
        let mut budgets = self
            .lookups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match known {
            Some(ns) => budgets
                .known
                .entry(ns)
                .or_default()
                .take(KNOWN_SIGNER_LOOKUPS_PER_MINUTE, now_secs),
            None => budgets.shared.take(POINTER_LOOKUPS_PER_MINUTE, now_secs),
        }
    }

    /// [`Self::take_lookup`], with the 429 to answer when the budget is
    /// spent.
    fn take_lookup_or_refuse(
        &self,
        known: Option<u64>,
        now_secs: u64,
    ) -> Result<(), Box<Response<Body>>> {
        self.take_lookup(known, now_secs).map_err(|retry_after| {
            let mut response = text(
                StatusCode::TOO_MANY_REQUESTS,
                "the gateway's pointer lookups for this minute are spent\n",
            );
            if let Ok(retry) = header::HeaderValue::from_str(&retry_after.to_string()) {
                response.headers_mut().insert(header::RETRY_AFTER, retry);
            }
            Box::new(response)
        })
    }
}

/// Parsed bundle manifests by hash, least recently used first out, within
/// [`MAX_CACHED_MANIFESTS`] entries and [`MANIFEST_CACHE_BYTES`].
#[derive(Default)]
struct ManifestCache {
    /// Oldest first: each entry is the hash, the manifest and its JSON size.
    entries: std::collections::VecDeque<([u8; 32], Arc<BundleManifest>, usize)>,
    bytes: usize,
}

impl ManifestCache {
    fn get(&mut self, hash: &[u8; 32]) -> Option<Arc<BundleManifest>> {
        let at = self.entries.iter().position(|(h, _, _)| h == hash)?;
        let entry = self.entries.remove(at)?;
        let manifest = Arc::clone(&entry.1);
        self.entries.push_back(entry);
        Some(manifest)
    }

    fn insert(&mut self, hash: [u8; 32], manifest: Arc<BundleManifest>, size: usize) {
        if let Some(at) = self.entries.iter().position(|(h, _, _)| *h == hash)
            && let Some((_, _, old)) = self.entries.remove(at)
        {
            self.bytes = self.bytes.saturating_sub(old);
        }
        self.entries.push_back((hash, manifest, size));
        self.bytes = self.bytes.saturating_add(size);
        while self.entries.len() > 1
            && (self.entries.len() > MAX_CACHED_MANIFESTS || self.bytes > MANIFEST_CACHE_BYTES)
        {
            if let Some((_, _, evicted)) = self.entries.pop_front() {
                self.bytes = self.bytes.saturating_sub(evicted);
            }
        }
    }
}

/// What a pull that ended badly means for the gateway.
enum PullError {
    /// The capability cannot pay for the rest.
    Funding(anyhow::Error),
    /// The public allowance ran out while the request waited for its pull
    /// slot or read a manifest.
    Allowance,
    /// The blob cannot be read as a bundle manifest: it is over the manifest
    /// cap, or its bytes do not parse as one. Nothing upstream failed.
    NotManifest(anyhow::Error),
    Other(anyhow::Error),
}

/// How a response presents what it serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Present {
    /// On the gateway origin: active types as plain text, a bundle as a list
    /// of `decdn:` links.
    Bare,
    /// On a site origin: each type as itself, `decdn:` URIs in HTML and CSS
    /// rewritten, a bundle as its `index.html` or a list of relative links.
    Site,
    /// As a file to save.
    Download,
}

impl Gateway {
    /// Whether this is a public gateway: no token, no capabilities, every
    /// request paid from the gateway's own pool within its allowance.
    const fn public(&self) -> bool {
        self.public.is_some()
    }

    /// Answer `req` and count the answer. On a public gateway, make it
    /// readable by every origin ([`readable_anywhere`]).
    async fn handle<B>(self: Arc<Self>, req: Request<B>) -> Response<Body>
    where
        B: HttpBody<Data = Bytes>,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let public = self.public.is_some();
        let gateway = Arc::clone(&self);
        let response = self.answer(req).await;
        gateway.metrics.request(response.status());
        if public {
            return readable_anywhere(response);
        }
        response
    }

    /// Count `bytes` delivered against the public allowance `bucket` spends
    /// (a pointer's namespace, or `None` for the shared one). `false` once
    /// the day's allowance is reached. A loopback gateway has none.
    fn charge(&self, bucket: Option<u64>, bytes: usize) -> bool {
        self.public.as_ref().is_none_or(|public| {
            let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
            public.allowance.charge(bucket, bytes, now_secs())
        })
    }

    /// Count what a pull that did not end cleanly may have paid past the
    /// frames its response delivered ([`DROP_PENALTY_BYTES`]) against
    /// `bucket`.
    fn charge_drop(&self, bucket: Option<u64>) {
        if let Some(public) = &self.public {
            tracing::info!(
                penalty = DROP_PENALTY_BYTES,
                "a pull ended short of clean; its read-ahead counts against the allowance"
            );
            public
                .allowance
                .charge(bucket, DROP_PENALTY_BYTES, now_secs());
        }
    }

    /// The 402 for `uri` when `size` (its length, when known) does not fit
    /// what is left of the public allowance `bucket` today, else `None`.
    /// Refusing before the pull costs nothing; cutting it after would waste
    /// what it paid for.
    fn too_large(
        &self,
        bucket: Option<u64>,
        uri: &DecdnUri,
        size: Option<u64>,
    ) -> Option<Response<Body>> {
        let public = self.public.as_ref()?;
        let size = size?;
        let (left, retry_after) = match public.allowance.remaining(bucket, now_secs()) {
            Ok(left) => (left, 0),
            Err(retry_after) => (0, retry_after),
        };
        (size > left).then(|| {
            tracing::info!(%uri, size, left, "the entry does not fit today's allowance");
            pay_elsewhere(
                StatusCode::PAYMENT_REQUIRED,
                uri,
                retry_after,
                "the entry does not fit what is left of the gateway's allowance today",
            )
        })
    }

    async fn answer<B>(self: Arc<Self>, req: Request<B>) -> Response<Body>
    where
        B: HttpBody<Data = Bytes>,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let host = req
            .headers()
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");
        let gateway = Arc::clone(&self);
        let route = self.sites.route(host);
        if route.is_none() && !self.hosts.iter().any(|allowed| allowed == host) {
            return text(StatusCode::FORBIDDEN, "unknown Host\n");
        }
        if self.public() && req.method() == Method::OPTIONS {
            return public_preflight(
                req.uri().path(),
                !matches!(route, Some(Route::Site(_) | Route::Namespace(_))),
            );
        }
        match route {
            Some(Route::Site(site)) => {
                let response = self.site(req, site, None).await;
                return gateway.with_policy(response);
            }
            Some(Route::Namespace(ns)) => {
                let Some(root) = self.public.as_ref().and_then(|p| p.pointers.root(ns)) else {
                    return text(
                        StatusCode::NOT_FOUND,
                        &format!("namespace {ns} has no pointer on this gateway\n"),
                    );
                };
                let site = Site {
                    hash: root,
                    namespace: Some(ns),
                };
                let response = self.site(req, site, Some(ns)).await;
                return gateway.with_policy(response);
            }
            Some(Route::Bare) | None => {}
        }
        let origin = req
            .headers()
            .get(header::ORIGIN)
            .and_then(|o| o.to_str().ok())
            .map(str::to_owned);
        let authorized = self.authorized(req.headers());
        let path = req.uri().path().to_owned();
        let method = req.method().clone();
        let target = req
            .uri()
            .path_and_query()
            .map_or("/", |pq| pq.as_str())
            .to_owned();
        let req = match self.public_bare(req, &path, &target).await {
            Ok(response) => return response,
            Err(req) => *req,
        };
        if !self.public()
            && method == Method::GET
            && let Some(response) = self.operator(&path, authorized).await
        {
            return response;
        }
        match (method, path.as_str()) {
            (Method::OPTIONS, "/_delegate" | "/_capability") => preflight(origin.as_deref()),
            (Method::OPTIONS, _) => text(StatusCode::FORBIDDEN, "no cross-origin access\n"),
            (Method::GET, "/") => html(
                StatusCode::OK,
                "<h1>decdn gateway</h1><p>Running. Open a bundle as a website at \
                 <code>/&lt;hash&gt;/</code>. Connect the deCDN browser extension with the \
                 token <code>decdn gateway</code> printed when it started.</p>",
            ),
            (Method::GET, "/favicon.ico") => text(StatusCode::NOT_FOUND, ""),
            (Method::GET, "/_delegate") => {
                let response = match Caller::of(origin.as_deref(), authorized) {
                    Ok(caller) => self.mint(&caller).await,
                    Err(e) => text(StatusCode::FORBIDDEN, e),
                };
                with_cors(response, origin.as_deref())
            }
            (Method::PUT, "/_capability") => {
                let response = match Caller::of(origin.as_deref(), authorized) {
                    Ok(caller) => self.register(req, &caller).await,
                    Err(e) => text(StatusCode::FORBIDDEN, e),
                };
                with_cors(response, origin.as_deref())
            }
            (Method::GET, "/_capability") if authorized => self.list().await,
            (Method::GET, "/_capability") => text(StatusCode::UNAUTHORIZED, "token required\n"),
            (Method::PUT, "/_pointer") => text(
                StatusCode::NOT_FOUND,
                "a loopback gateway holds no namespace pointers\n",
            ),
            (Method::HEAD, _) => head_refused(),
            (Method::GET, _) => {
                self.content(req.headers(), authorized, target.trim_start_matches('/'))
                    .await
            }
            _ => text(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n"),
        }
    }

    /// What a public gateway answers on its bare host before the routes it
    /// shares with a loopback one: no API but `PUT /_pointer`, no dashboard,
    /// and `HEAD` of content. `Err(req)` when the request is not one of those,
    /// and on a loopback gateway.
    async fn public_bare<B>(
        &self,
        req: Request<B>,
        path: &str,
        target: &str,
    ) -> Result<Response<Body>, Box<Request<B>>>
    where
        B: HttpBody<Data = Bytes>,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        if !self.public() {
            return Err(Box::new(req));
        }
        Ok(match (req.method(), path) {
            (_, "/_delegate" | "/_capability" | "/_status") => {
                text(StatusCode::NOT_FOUND, "a public gateway has no API\n")
            }
            (_, p) if p.starts_with("/_dashboard/") => text(
                StatusCode::NOT_FOUND,
                "the dashboard is on --metrics-listen\n",
            ),
            (&Method::PUT, "/_pointer") => self.put_pointer(req).await,
            (&Method::HEAD, "/" | "/favicon.ico") => text(StatusCode::OK, ""),
            (&Method::HEAD, _) => self.head_bare(target.trim_start_matches('/')),
            _ => return Err(Box::new(req)),
        })
    }

    /// The operator's routes at `path`: the dashboard, its assets, and
    /// `/_status` when `authorized`. `None` for any other path.
    async fn operator(&self, path: &str, authorized: bool) -> Option<Response<Body>> {
        match path {
            "/_status" if authorized => Some(json(StatusCode::OK, &self.status().await)),
            "/_status" => Some(text(StatusCode::UNAUTHORIZED, "token required\n")),
            "/" => Some(dashboard::page()),
            _ => dashboard::asset(path),
        }
    }

    /// The site policy on `response`.
    fn with_policy(&self, mut response: Response<Body>) -> Response<Body> {
        let policy = site::content_security_policy(&self.sites);
        if let Ok(policy) = header::HeaderValue::from_str(&policy) {
            response
                .headers_mut()
                .insert(header::CONTENT_SECURITY_POLICY, policy);
        }
        response
    }

    /// A request on a site origin: the site's own entries, and `/_decdn/` for
    /// content outside its bundle. `pointer` is the namespace whose pointer
    /// named the site, when one did: the request is on that namespace's host
    /// and spends its allowance.
    async fn site<B>(
        self: Arc<Self>,
        req: Request<B>,
        site: Site,
        pointer: Option<u64>,
    ) -> Response<Body> {
        let path = req.uri().path();
        if matches!(
            path,
            "/_delegate" | "/_capability" | "/_pointer" | "/_status"
        ) {
            return text(
                StatusCode::NOT_FOUND,
                "the gateway API answers on the gateway origin only\n",
            );
        }
        let head = match *req.method() {
            Method::GET => false,
            Method::HEAD if self.public() => true,
            Method::HEAD => return head_refused(),
            _ => return text(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n"),
        };
        let headers = req.headers();
        // On a public gateway the operator pays for every request, within
        // the allowance: there is no capability for the browser's headers to
        // protect, and `curl` sends none.
        if !self.public()
            && let Err((status, why)) = site::may_spend(headers, self.authorized(headers))
        {
            return text(status, why);
        }
        let page_host = match pointer {
            Some(ns) => self.sites.namespace_host(ns),
            None => self.sites.site_host(&site),
        };
        let target = req.uri().path_and_query().map_or(path, |pq| pq.as_str());
        if head {
            return self.head_site(&site, path, pointer).await;
        }
        let range = range_header(headers);
        if let Some(rest) = target.strip_prefix("/_decdn/") {
            let uri: DecdnUri = match format!("decdn:{rest}").parse() {
                Ok(uri) => uri,
                Err(e) => return text(StatusCode::BAD_REQUEST, &format!("{e}\n")),
            };
            // The namespace's allowance pays for its own bundle alone: other
            // content on its host spends the shared one.
            let bucket = pointer.filter(|_| uri.hash == site.hash);
            if !site::is_navigation(headers) {
                return self
                    .paid(&page_host, bucket, &uri, Present::Site, range)
                    .await;
            }
            // Another bundle's pages run on their own origin; a blob has none.
            return match uri.target {
                UriTarget::Blob => {
                    self.paid(&page_host, bucket, &uri, Present::Download, range)
                        .await
                }
                _ => self.to_site(&uri),
            };
        }
        // The query belongs to the page's scripts; a directory serves its
        // `index.html`.
        let entry = if path.len() > 1 && path.ends_with('/') {
            format!("{path}index.html")
        } else {
            path.to_owned()
        };
        let hex = alloy::hex::encode(site.hash);
        let uri = match format!("decdn:{hex}{entry}").parse::<DecdnUri>() {
            Ok(uri) => DecdnUri::new(uri.hash, uri.target, site.namespace),
            Err(e) => return text(StatusCode::NOT_FOUND, &format!("{e}\n")),
        };
        self.paid(&page_host, pointer, &uri, Present::Site, range)
            .await
    }

    /// `PUT /_pointer`: store a namespace pointer signed by the namespace's
    /// owner on this deployment's `PublisherRegistry`, when its `seq` is
    /// above every one stored from that signer. A replay is refused on that
    /// high water before the chain is read, and the lookups that do reach
    /// the chain come out of a budget per minute, so no anonymous caller
    /// makes the gateway spend its RPC.
    async fn put_pointer<B>(&self, req: Request<B>) -> Response<Body>
    where
        B: HttpBody<Data = Bytes>,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let Some(public) = &self.public else {
            return text(
                StatusCode::NOT_FOUND,
                "a loopback gateway holds no namespace pointers\n",
            );
        };
        let body = match Limited::new(req.into_body(), MAX_BODY_BYTES)
            .collect()
            .await
        {
            Ok(body) => body.to_bytes(),
            Err(_) => return text(StatusCode::PAYLOAD_TOO_LARGE, "body too large\n"),
        };
        let pointer: SignedPointer = match serde_json::from_slice(&body) {
            Ok(pointer) => pointer,
            Err(e) => {
                return text(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    &format!("expected {{\"namespace\",\"root\",\"seq\",\"signature\"}}: {e}\n"),
                );
            }
        };
        let ns = pointer.namespace;
        let domain = pointer_domain(self.chain.chain_id, public.registry);
        let signer = match pointer.signer(&domain) {
            Ok(signer) => signer,
            Err(e) => return text(StatusCode::UNPROCESSABLE_ENTITY, &format!("{e}\n")),
        };
        // From any signer, only a `seq` above what it had stored is new: a
        // replay, and an old pointer of an owner the namespace came back to,
        // cost no lookup.
        let high_water = public.pointers.high_water(ns, signer);
        if let Some(current) = high_water
            && pointer.seq <= current
        {
            return stale_pointer(current);
        }
        // A signer stored before was the owner once: it has the namespace's
        // own budget. The budget is taken before the lock, so a caller past
        // it waits on nothing. One pointer at a time from here: the owner
        // read and the store are one step, so a read from before a transfer
        // cannot land after one from after it.
        let known = high_water.map(|_| ns);
        if let Err(response) = public.take_lookup_or_refuse(known, now_secs()) {
            return *response;
        }
        let _writes = public.pointer_writes.lock().await;
        let owner = match self.namespace_owner(public, ns).await {
            Ok(owner) => owner,
            Err(response) => return *response,
        };
        if owner != signer {
            return text(
                StatusCode::FORBIDDEN,
                &format!("the pointer is signed by {signer}; namespace {ns} is owned by {owner}\n"),
            );
        }
        let (root, seq) = (pointer.root, pointer.seq);
        match public.pointers.set(pointer, signer) {
            Ok(()) => {}
            Err(SetError::Stale { current }) => return stale_pointer(current),
            Err(SetError::Io(e)) => {
                tracing::error!(
                    error = format!("{e:#}"),
                    namespace = ns,
                    "a pointer was not stored"
                );
                return text(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "the pointer could not be stored\n",
                );
            }
        }
        self.metrics.pointer_updated();
        tracing::info!(namespace = ns, root = %root, seq, "a namespace pointer was stored");
        json(
            StatusCode::CREATED,
            &serde_json::json!({
                "namespace": ns,
                "root": alloy::hex::encode(root),
                "seq": seq,
                "url": format!("{}/", self.sites.origin(&self.sites.namespace_host(ns))),
            }),
        )
    }

    /// A redirect to the site that serves bundle `uri`, at its entry path.
    fn to_site(&self, uri: &DecdnUri) -> Response<Body> {
        let site = Site {
            hash: uri.hash,
            namespace: uri.namespace,
        };
        let path = match &uri.target {
            UriTarget::Bundle => String::new(),
            UriTarget::BundleEntry(path) => entry_link(uri.hash, path),
            _ => return text(StatusCode::BAD_REQUEST, "a blob has no site\n"),
        };
        redirect(&format!(
            "{}/{path}",
            self.sites.origin(&self.sites.site_host(&site))
        ))
    }

    fn authorized(&self, headers: &HeaderMap) -> bool {
        let Some(token) = self.token.as_deref() else {
            return false;
        };
        headers
            .get(TOKEN_HEADER)
            .and_then(|t| t.to_str().ok())
            .is_some_and(|given| token_matches(token, given))
    }

    /// Drop delegates that waited too long for a capability, and those whose
    /// capability expired.
    fn prune(delegates: &mut HashMap<Address, Delegate>) {
        let now = now_secs();
        delegates.retain(|_, d| match &d.capability {
            None => d.minted.elapsed() < PENDING_TTL,
            Some(registered) => registered.grant.expiry > now,
        });
    }

    async fn mint(&self, caller: &Caller) -> Response<Body> {
        let mut delegates = self.delegates.lock().await;
        Self::prune(&mut delegates);
        let site = caller.site();
        if let Err(e) = may_mint(&delegates, site.as_deref()) {
            return text(StatusCode::TOO_MANY_REQUESTS, &e);
        }
        let signer = PrivateKeySigner::random();
        let address = signer.address();
        delegates.insert(
            address,
            Delegate {
                signer: Arc::new(signer),
                minted: Instant::now(),
                site,
                capability: None,
            },
        );
        json(
            StatusCode::OK,
            &serde_json::json!({ "address": address.to_string() }),
        )
    }

    async fn register<B>(&self, req: Request<B>, caller: &Caller) -> Response<Body>
    where
        B: HttpBody<Data = Bytes>,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let body = match Limited::new(req.into_body(), MAX_BODY_BYTES)
            .collect()
            .await
        {
            Ok(body) => body.to_bytes(),
            Err(_) => return text(StatusCode::PAYLOAD_TOO_LARGE, "body too large\n"),
        };
        let request: RegisterRequest = match serde_json::from_slice(&body) {
            Ok(request) => request,
            Err(e) => {
                return text(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    &format!("expected {{\"capability\", \"scope\"}}: {e}\n"),
                );
            }
        };
        let scope = match Scope::parse(&request.scope) {
            Ok(scope) => scope,
            Err(e) => return text(StatusCode::UNPROCESSABLE_ENTITY, &format!("{e}\n")),
        };
        if let Err(e) = may_register(caller, &scope) {
            return text(StatusCode::FORBIDDEN, &format!("{e}\n"));
        }
        let grant = match CapabilityGrant::from_token(request.capability.trim()) {
            Ok(grant) => grant,
            Err(e) => return text(StatusCode::UNPROCESSABLE_ENTITY, &format!("{e}\n")),
        };
        if grant.expiry <= now_secs() {
            return text(
                StatusCode::UNPROCESSABLE_ENTITY,
                "the capability has expired\n",
            );
        }
        if let Err((status, e)) = self.verify_owner(&grant).await {
            return text(status, &e);
        }
        let mut delegates = self.delegates.lock().await;
        Self::prune(&mut delegates);
        let Some(delegate) = delegates.get_mut(&grant.signer) else {
            return text(
                StatusCode::CONFLICT,
                "the capability names no delegate this gateway is holding: get one from \
                 GET /_delegate\n",
            );
        };
        if delegate.capability.is_some() {
            return text(
                StatusCode::CONFLICT,
                "that delegate already holds a capability: get a new one from GET /_delegate\n",
            );
        }
        let response = serde_json::json!({
            "signer": grant.signer.to_string(),
            "scope": scope.to_string(),
            "expiry": grant.expiry,
        });
        delegate.capability = Some(Registered {
            grant,
            scope,
            origin: caller.origin().map(str::to_owned),
            since: Instant::now(),
            spent: false,
        });
        json(StatusCode::CREATED, &response)
    }

    /// Check that `grant` is signed by the on-chain owner of its pool on this
    /// gateway's deployment. Whether the pool can pay shows only when a pull
    /// draws on it: a funding stop retires the capability.
    async fn verify_owner(&self, grant: &CapabilityGrant) -> Result<(), (StatusCode, String)> {
        let unprocessable = |e: String| (StatusCode::UNPROCESSABLE_ENTITY, e);
        let signed_by = grant
            .owner(&self.domain)
            .map_err(|e| unprocessable(format!("{e}\n")))?;
        let unreachable = |e: String| {
            (
                StatusCode::BAD_GATEWAY,
                format!("could not read the capability's pool on-chain: {e}\n"),
            )
        };
        let rpc = decdn_client::provider::build_read_provider(&self.chain.rpc_url)
            .map_err(|e| unreachable(sanitize_err_chain(&e)))?;
        let pool = PaymentPool::new(self.chain.payment_pool, rpc)
            .getPool(grant.pool_id)
            .call()
            .await
            .map_err(|e| unreachable(sanitize_error_sources(&e)))?;
        owner_matches(pool.owner, signed_by, grant.pool_id).map_err(unprocessable)
    }

    async fn list(&self) -> Response<Body> {
        json(StatusCode::OK, &self.capabilities().await)
    }

    /// Every capability registered and not yet expired.
    async fn capabilities(&self) -> Vec<Capability> {
        let mut delegates = self.delegates.lock().await;
        Self::prune(&mut delegates);
        delegates
            .values()
            .filter_map(|d| d.capability.as_ref())
            .map(|r| Capability {
                cap: r.grant.spending_cap,
                expiry: r.grant.expiry,
                origin: r.origin.clone(),
                pool: r.grant.pool_id.to_string(),
                scope: r.scope.to_string(),
                signer: r.grant.signer.to_string(),
                spent: r.spent,
            })
            .collect()
    }

    /// The owner of namespace `ns` on this deployment's registry, read
    /// on-chain within [`OWNER_LOOKUP_TIMEOUT`].
    ///
    /// # Errors
    ///
    /// The answer to send instead: 502 when the chain cannot be read in
    /// time, 404 when the namespace does not exist.
    async fn namespace_owner(
        &self,
        public: &Public,
        ns: u64,
    ) -> Result<Address, Box<Response<Body>>> {
        let unreachable = |e: String| {
            Box::new(text(
                StatusCode::BAD_GATEWAY,
                &format!("could not read the namespace's owner on-chain: {e}\n"),
            ))
        };
        let rpc = decdn_client::provider::build_read_provider(&self.chain.rpc_url)
            .map_err(|e| unreachable(sanitize_err_chain(&e)))?;
        let registry = PublisherRegistry::new(public.registry, rpc);
        let lookup = registry.ownerOf(U256::from(ns));
        let owner = tokio::time::timeout(OWNER_LOOKUP_TIMEOUT, lookup.call())
            .await
            .map_err(|_| unreachable("the lookup timed out".to_owned()))?
            .map_err(|e| unreachable(sanitize_error_sources(&e)))?;
        if owner.is_zero() {
            return Err(Box::new(text(
                StatusCode::NOT_FOUND,
                &format!("namespace {ns} does not exist on this deployment\n"),
            )));
        }
        Ok(owner)
    }

    /// Who pays a request for `host`: the gateway's own pool on a public
    /// gateway, against the allowance of `pointer`'s namespace when the
    /// request is on its host, else the live capability whose scope covers
    /// `host` most specifically, by [`Registered::preference`].
    async fn paying_for(&self, host: &str, pointer: Option<u64>) -> Option<Payment> {
        if self.public() {
            return Some(Payment::Pool { namespace: pointer });
        }
        let mut delegates = self.delegates.lock().await;
        Self::prune(&mut delegates);
        delegates
            .values()
            .filter_map(|d| d.capability.as_ref().map(|r| (d, r)))
            .filter(|(_, r)| !r.spent && r.scope.covers(host))
            .max_by_key(|(_, r)| r.preference())
            .map(|(d, r)| {
                Payment::Held(Credential {
                    signer: Arc::clone(&d.signer),
                    grant: r.grant.clone(),
                })
            })
    }

    /// Retire the capability `signer` pays with after a funding stop. A page's
    /// is dropped, freeing its origin's room; the token holder's stays listed
    /// as spent.
    async fn mark_spent(&self, signer: Address) {
        let mut delegates = self.delegates.lock().await;
        let Some(registered) = delegates
            .get_mut(&signer)
            .and_then(|d| d.capability.as_mut())
        else {
            return;
        };
        if registered.origin.is_some() {
            delegates.remove(&signer);
        } else {
            registered.spent = true;
        }
    }

    async fn content(
        self: &Arc<Self>,
        headers: &HeaderMap,
        authorized: bool,
        rest: &str,
    ) -> Response<Body> {
        let uri: DecdnUri = match format!("decdn:{rest}").parse() {
            Ok(uri) => uri,
            Err(e) => return text(StatusCode::BAD_REQUEST, &format!("{e}\n")),
        };
        let range = range_header(headers);
        if !authorized {
            // A browser without the extension opens a bundle as its site. On
            // a public gateway a blob streams from the bare host as bytes.
            return match uri.target {
                UriTarget::Bundle | UriTarget::BundleEntry(_) => self.to_site(&uri),
                _ if self.public() => self.paid("", None, &uri, Present::Bare, range).await,
                _ => text(StatusCode::UNAUTHORIZED, "token required\n"),
            };
        }
        let Some(page_host) = headers
            .get(HOST_HEADER)
            .and_then(|h| h.to_str().ok())
            .map(str::to_ascii_lowercase)
        else {
            return text(StatusCode::UNAUTHORIZED, "page host required\n");
        };
        self.paid(&page_host, None, &uri, Present::Bare, range)
            .await
    }

    /// Serve `uri` as `present` says, paid with the capability that covers
    /// `page_host`, or on a public gateway from the pool against the
    /// allowance of `pointer`'s namespace when the request is on its host.
    async fn paid(
        self: &Arc<Self>,
        page_host: &str,
        pointer: Option<u64>,
        uri: &DecdnUri,
        present: Present,
        range: Option<&str>,
    ) -> Response<Body> {
        if let Some(refused) = self.allowance_refusal(pointer, uri) {
            return refused;
        }
        // A funding stop retires the capability and the next covering one
        // takes the request, so a capability that cannot pay costs a retry,
        // never a failed load. Each pass retires one, so the loop ends.
        loop {
            let Some(paying) = self.paying_for(page_host, pointer).await else {
                return text(
                    StatusCode::PAYMENT_REQUIRED,
                    &format!("no capability covers {page_host}\n"),
                );
            };
            match self.serve(uri, &paying, present, range).await {
                Ok(response) => return response,
                Err(PullError::Allowance) => return self.allowance_spent(pointer, uri),
                Err(PullError::Funding(e)) => match &paying {
                    Payment::Held(credential) => {
                        tracing::info!(error = %e, host = %page_host, "a capability ran out");
                        self.mark_spent(credential.signer.address()).await;
                    }
                    Payment::Pool { .. } => {
                        return pull_failed(PullError::Funding(e), uri, page_host, true);
                    }
                },
                Err(e) => return pull_failed(e, uri, page_host, self.public()),
            }
        }
    }

    /// The 402 for `uri` once the public allowance `bucket` ran out under a
    /// request already admitted.
    fn allowance_spent(&self, bucket: Option<u64>, uri: &DecdnUri) -> Response<Body> {
        self.allowance_refusal(bucket, uri).unwrap_or_else(|| {
            pay_elsewhere(
                StatusCode::PAYMENT_REQUIRED,
                uri,
                0,
                "the gateway's allowance is spent",
            )
        })
    }

    /// The 402 for `uri` when the public allowance `bucket` it spends is
    /// gone for the day, else `None`.
    fn allowance_refusal(&self, bucket: Option<u64>, uri: &DecdnUri) -> Option<Response<Body>> {
        let public = self.public.as_ref()?;
        let retry_after = public.allowance.remaining(bucket, now_secs()).err()?;
        tracing::info!(%uri, "the allowance is spent for today");
        Some(pay_elsewhere(
            StatusCode::PAYMENT_REQUIRED,
            uri,
            retry_after,
            "the gateway's allowance is spent for today",
        ))
    }

    /// Serve what `uri` names, as `present` says, paid by `paying`; `range` is
    /// the request's `Range` header, honoured on a sized entry.
    async fn serve(
        self: &Arc<Self>,
        uri: &DecdnUri,
        paying: &Payment,
        present: Present,
        range: Option<&str>,
    ) -> Result<Response<Body>, PullError> {
        match &uri.target {
            UriTarget::Blob => {
                // A blob's size is nobody's to declare, so a range on it is
                // ignored.
                self.stream(
                    uri.hash,
                    uri.namespace,
                    paying,
                    None,
                    Extent::UNSIZED,
                    present,
                )
                .await
            }
            UriTarget::BundleEntry(path) => {
                self.entry(uri.hash, uri.namespace, path, paying, present, range)
                    .await
            }
            UriTarget::Bundle => {
                self.index(uri.hash, uri.namespace, paying, present, range)
                    .await
            }
            _ => Ok(text(StatusCode::BAD_REQUEST, "unsupported URI target\n")),
        }
    }

    /// One bundle entry, by its literal manifest path. A path the manifest
    /// lacks answers 404, and so does a blob that is not a manifest or a
    /// malformed one ([`Self::manifest`]). On a site, a path that names a
    /// directory with an `index.html` redirects to the directory.
    async fn entry(
        self: &Arc<Self>,
        manifest_hash: [u8; 32],
        namespace: Option<u64>,
        path: &str,
        paying: &Payment,
        present: Present,
        range: Option<&str>,
    ) -> Result<Response<Body>, PullError> {
        let manifest = self.manifest(manifest_hash, namespace, paying).await?;
        if let Some(entry) = manifest.entries.iter().find(|e| e.path == path) {
            let uri = DecdnUri::new(
                manifest_hash,
                UriTarget::BundleEntry(path.to_owned()),
                namespace,
            );
            let extent = match Extent::of(entry.size, range, rewrites(present, path)) {
                Ok(extent) => extent,
                Err(size) => return Ok(not_satisfiable(size)),
            };
            if let Some(refused) = self.too_large(paying.bucket(), &uri, extent.billable()) {
                return Ok(refused);
            }
            if entry.size == Some(0) {
                return Ok(empty_entry(path, entry.hash, present));
            }
            return self
                .stream(entry.hash, namespace, paying, Some(path), extent, present)
                .await;
        }
        let index = format!("{path}/index.html");
        if present == Present::Site && manifest.entries.iter().any(|e| e.path == index) {
            // Relative to the request, so it holds under `/_decdn/` too.
            let link = entry_link(manifest_hash, path);
            let last = link.rsplit('/').next().unwrap_or(&link);
            return Ok(redirect(&format!("{last}/")));
        }
        Ok(text(
            StatusCode::NOT_FOUND,
            &format!("the bundle has no entry {path:?}\n"),
        ))
    }

    /// A bundle as a whole: on a site, its `index.html` when it has one;
    /// otherwise an HTML index of its entries, each linking to its URL.
    async fn index(
        self: &Arc<Self>,
        manifest_hash: [u8; 32],
        namespace: Option<u64>,
        paying: &Payment,
        present: Present,
        range: Option<&str>,
    ) -> Result<Response<Body>, PullError> {
        let manifest = self.manifest(manifest_hash, namespace, paying).await?;
        if present == Present::Site
            && let Some(entry) = manifest.entries.iter().find(|e| e.path == "index.html")
        {
            let uri = DecdnUri::new(manifest_hash, UriTarget::Bundle, namespace);
            let extent = match Extent::of(entry.size, range, rewrites(present, &entry.path)) {
                Ok(extent) => extent,
                Err(size) => return Ok(not_satisfiable(size)),
            };
            if let Some(refused) = self.too_large(paying.bucket(), &uri, extent.billable()) {
                return Ok(refused);
            }
            if entry.size == Some(0) {
                return Ok(empty_entry(&entry.path, entry.hash, present));
            }
            return self
                .stream(
                    entry.hash,
                    namespace,
                    paying,
                    Some(&entry.path),
                    extent,
                    present,
                )
                .await;
        }
        let mut rows = String::new();
        for entry in &manifest.entries {
            let link = if present == Present::Site {
                format!("./{}", entry_link(manifest_hash, &entry.path))
            } else {
                DecdnUri::new(
                    manifest_hash,
                    UriTarget::BundleEntry(entry.path.clone()),
                    namespace,
                )
                .to_string()
            };
            let size = entry.size.map_or_else(String::new, human_bytes);
            let _ = write!(
                rows,
                "<tr><td><a href=\"{}\">{}</a></td><td>{size}</td></tr>",
                escape(&link),
                escape(&entry.path),
            );
        }
        let uri = DecdnUri::new(manifest_hash, UriTarget::Bundle, namespace);
        let page = format!(
            "<h1>Bundle</h1><p><code>{}</code></p><table>{rows}</table>",
            escape(&uri.to_string())
        );
        // The listing has a length too: one that does not fit is refused.
        let length = u64::try_from(page.len()).unwrap_or(u64::MAX);
        if let Some(refused) = self.too_large(paying.bucket(), &uri, Some(length)) {
            return Ok(refused);
        }
        self.charge(paying.bucket(), page.len());
        Ok(html(StatusCode::OK, &page))
    }

    fn cached_manifest(&self, hash: &[u8; 32]) -> Option<Arc<BundleManifest>> {
        self.manifests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(hash)
    }

    /// The bundle manifest `hash`, from the cache or pulled. The pull requests,
    /// and so pays for, at most the manifest cap. Only a blob that parses as a
    /// manifest is cached: a blob that does not is pulled, and paid for, again
    /// on every request.
    async fn manifest(
        self: &Arc<Self>,
        hash: [u8; 32],
        namespace: Option<u64>,
        paying: &Payment,
    ) -> Result<Arc<BundleManifest>, PullError> {
        if let Some(manifest) = self.cached_manifest(&hash) {
            return Ok(manifest);
        }
        let _fill = self.manifest_fill.lock().await;
        // A request that waited on the fill lock finds what the one before it
        // pulled.
        if let Some(manifest) = self.cached_manifest(&hash) {
            return Ok(manifest);
        }
        let cap_mb = manifest_cap_mb(self.common.max_blob_mb);
        let raw = raw_blob_hint(hash, namespace);
        // The raw blob pulls under the gateway's own cap, so it is a way past
        // the manifest cap only when that cap is higher.
        let raw_past_cap = (self.common.max_blob_mb == 0 || cap_mb < self.common.max_blob_mb)
            .then_some(raw.as_str());
        let (first, mut reader, done, mut guard) = self
            .start_pull(hash, namespace, paying, cap_mb, None, None)
            .await
            .map_err(|e| over_manifest_cap(e, cap_mb, raw_past_cap))?;
        // Every byte pulled is charged as it lands, whether or not it parses:
        // a blob that is no manifest was paid for all the same. The read stops
        // where the allowance does, instead of draining to the manifest cap.
        let bucket = paying.bucket();
        if !self.charge(bucket, first.len()) {
            return Err(PullError::Allowance);
        }
        let mut bytes = first.to_vec();
        let read = async {
            let mut buf = vec![0u8; CHUNK_BYTES];
            loop {
                let n = reader
                    .read(&mut buf)
                    .await
                    .map_err(|e| PullError::Other(e.into()))?;
                if n == 0 {
                    break;
                }
                if !self.charge(bucket, n) {
                    return Err(PullError::Allowance);
                }
                bytes.extend_from_slice(buf.get(..n).unwrap_or_default());
                // A backstop: the manifest cap ends the pull before this.
                if bytes.len() > MAX_MANIFEST_BYTES {
                    return Err(PullError::NotManifest(anyhow::anyhow!(
                        "the blob is larger than {MAX_MANIFEST_BYTES} bytes, the most this \
                         gateway reads as a bundle manifest"
                    )));
                }
            }
            finished(done).await
        }
        .await;
        // The pull ended whole: nothing past the bytes read was paid for. A
        // read that ended short leaves the guard armed, and its drop charges
        // the bound of what the pull may have paid past them.
        read.map_err(|e| over_manifest_cap(e, cap_mb, raw_past_cap))?;
        guard.disarm();
        // The blob fits under the manifest cap, so it fits under the gateway's.
        let manifest = Arc::new(
            BundleManifest::parse(&bytes)
                .map_err(|e| manifest_parse_context(e, format!("not a bundle manifest; {raw}")))
                .map_err(PullError::NotManifest)?,
        );
        self.manifests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(hash, Arc::clone(&manifest), bytes.len());
        Ok(manifest)
    }

    /// A response streaming `hash`'s verified bytes, presented as `present`
    /// says. Errors that come before the first byte become the error status;
    /// a later one aborts the body.
    async fn stream(
        self: &Arc<Self>,
        hash: [u8; 32],
        namespace: Option<u64>,
        paying: &Payment,
        path: Option<&str>,
        extent: Extent,
        present: Present,
    ) -> Result<Response<Body>, PullError> {
        let name = path
            .and_then(|p| p.rsplit('/').next())
            .map_or_else(|| alloy::hex::encode(hash), str::to_owned);
        let (content_type, rewrite) = match present {
            Present::Bare | Present::Download => (content_type(&name), None),
            Present::Site => {
                let content_type = site::content_type(&name);
                let essence = content_type.split(';').next().unwrap_or_default();
                let rewrite = Rewrite::for_type(essence);
                (content_type, rewrite)
            }
        };
        // The pull holds no more than the size the manifest declares, and
        // takes only the window a range asked for.
        let (first, reader, done, guard) = self
            .start_pull(
                hash,
                namespace,
                paying,
                self.common.max_blob_mb,
                extent.size,
                extent.window,
            )
            .await?;
        // The frames charge the window as they go; the rest of the span the
        // pull pays for is charged now.
        self.charge(paying.bucket(), extent.surcharge());
        let size = extent.length();
        let state = Streaming {
            first: Some(first),
            reader,
            done: Some(done),
            rewrite,
            cut: false,
            clean: false,
            delivered: 0,
            size,
            guard,
        };
        let signer = match paying {
            Payment::Held(credential) => Some(credential.signer.address()),
            Payment::Pool { .. } => None,
        };
        let body = futures_util::stream::unfold(state, move |s| next_frame(s, signer));
        let disposition = if present == Present::Download {
            "attachment"
        } else {
            "inline"
        };
        let status = if extent.window.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        let mut response = Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, content_type)
            .header(
                header::CONTENT_DISPOSITION,
                format!(
                    "{disposition}; filename=\"{}\"",
                    name.replace(['"', '\\'], "_")
                ),
            )
            .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff");
        if let Some(size) = size {
            response = response.header(header::CONTENT_LENGTH, size);
        }
        if extent.fixed {
            response = response.header(header::ACCEPT_RANGES, "bytes");
        }
        if let (Some((offset, len)), Some(total)) = (extent.window, extent.size) {
            response = response.header(
                header::CONTENT_RANGE,
                format!(
                    "bytes {offset}-{}/{total}",
                    offset.saturating_add(len).saturating_sub(1)
                ),
            );
        }
        response
            .body(StreamBody::new(body).boxed())
            .map_err(|e| PullError::Other(e.into()))
    }

    /// `HEAD` on the public bare host: a blob's headers with no pull and no
    /// length (nothing knows its size before a pull); a bundle or an entry
    /// redirects to its site, as `GET` does.
    fn head_bare(&self, rest: &str) -> Response<Body> {
        let uri: DecdnUri = match format!("decdn:{rest}").parse() {
            Ok(uri) => uri,
            Err(e) => return text(StatusCode::BAD_REQUEST, &format!("{e}\n")),
        };
        match uri.target {
            UriTarget::Blob => {
                head_response(&content_type(&alloy::hex::encode(uri.hash)), None, uri.hash)
            }
            UriTarget::Bundle | UriTarget::BundleEntry(_) => self.to_site(&uri),
            _ => text(StatusCode::BAD_REQUEST, "unsupported URI target\n"),
        }
    }

    /// `HEAD` on a public site: the entry's headers from the bundle manifest,
    /// which the gateway pulls once and caches, so the request costs no pull
    /// of the entry. A directory answers for its `index.html`, and the root
    /// for the bundle's; a response the gateway would rewrite has no length.
    /// `pointer` is the namespace whose host the request is on, if any.
    async fn head_site(
        self: &Arc<Self>,
        site: &Site,
        path: &str,
        pointer: Option<u64>,
    ) -> Response<Body> {
        // The same resolution as `GET`: a directory is its `index.html`, and
        // the path decodes as a `decdn:` URI's does.
        let entry = if path.len() > 1 && path.ends_with('/') {
            format!("{path}index.html")
        } else {
            path.to_owned()
        };
        let hex = alloy::hex::encode(site.hash);
        let uri = match format!("decdn:{hex}{entry}").parse::<DecdnUri>() {
            Ok(uri) => DecdnUri::new(uri.hash, uri.target, site.namespace),
            Err(e) => return text(StatusCode::NOT_FOUND, &format!("{e}\n")),
        };
        // The manifest is a paid pull.
        if let Some(refused) = self.allowance_refusal(pointer, &uri) {
            return refused;
        }
        let paying = Payment::Pool { namespace: pointer };
        let manifest = match self.manifest(site.hash, site.namespace, &paying).await {
            Ok(manifest) => manifest,
            Err(e) => return pull_failed(e, &uri, path, true),
        };
        let path = match &uri.target {
            // The root is the bundle's `index.html`, else its listing: a page
            // with no fixed length.
            UriTarget::Bundle => {
                if manifest.entries.iter().any(|e| e.path == "index.html") {
                    "index.html"
                } else {
                    return head_response("text/html; charset=utf-8", None, site.hash);
                }
            }
            UriTarget::BundleEntry(path) => path.as_str(),
            _ => return text(StatusCode::BAD_REQUEST, "unsupported URI target\n"),
        };
        let Some(entry) = manifest.entries.iter().find(|e| e.path == path) else {
            let index = format!("{path}/index.html");
            if manifest.entries.iter().any(|e| e.path == index) {
                let link = entry_link(site.hash, path);
                let last = link.rsplit('/').next().unwrap_or(&link);
                return redirect(&format!("{last}/"));
            }
            return text(
                StatusCode::NOT_FOUND,
                &format!("the bundle has no entry {path:?}\n"),
            );
        };
        let name = path.rsplit('/').next().unwrap_or(path);
        let content_type = site::content_type(name);
        let essence = content_type.split(';').next().unwrap_or_default();
        let length = entry.size.filter(|_| Rewrite::for_type(essence).is_none());
        head_response(&content_type, length, entry.hash)
    }

    /// Hand the pull of `hash` to the worker, capped at `max_blob_mb` and at
    /// `size_cap` bytes when the blob's size is known, over the byte window
    /// `range` or the whole blob, and wait for its first verified bytes. The
    /// pull writes into a pipe the response reads from.
    async fn start_pull(
        self: &Arc<Self>,
        hash: [u8; 32],
        namespace: Option<u64>,
        paying: &Payment,
        max_blob_mb: u64,
        size_cap: Option<u64>,
        range: Option<(u64, u64)>,
    ) -> Result<
        (
            Bytes,
            DuplexStream,
            oneshot::Receiver<anyhow::Result<()>>,
            PullGuard,
        ),
        PullError,
    > {
        let (writer, mut reader) = tokio::io::duplex(PIPE_BYTES);
        let (done_tx, done_rx) = oneshot::channel();
        let mut common = self.common.clone();
        common.max_blob_mb = max_blob_mb;
        // Armed before the worker has the job: a request dropped while it
        // waits for its first byte still charges the pull's read-ahead.
        let bucket = paying.bucket();
        let mut guard = PullGuard::new(
            Arc::clone(self),
            bucket,
            hash,
            namespace,
            range,
            PayerLabel::of(paying),
        );
        // Asked once the pull has its slot: the allowance the requests before
        // it spent while it waited refuses it before any paid work.
        let admit = self.public().then(|| {
            let gateway = Arc::clone(self);
            Box::new(move || gateway.allowance_admits(bucket)) as Box<dyn FnOnce() -> bool + Send>
        });
        self.pulls
            .pull(Job {
                blob: BlobFetch {
                    estimate: false,
                    hash,
                    output: PathBuf::from("-"),
                    namespace,
                    common,
                    read_ahead: self.public().then_some(PUBLIC_READ_AHEAD_BYTES),
                    size_cap,
                    range,
                },
                paying: paying.clone(),
                writer,
                done: done_tx,
                admit,
            })
            .map_err(PullError::Other)?;
        let (first, done_rx) = match first_chunk(&mut reader, done_rx).await {
            Ok(first) => first,
            Err(e) => {
                // The pull ended before a byte reached the response: it paid
                // for nothing it could deliver, so it owes nothing. (A request
                // dropped mid-wait never reaches here, and stays armed.)
                guard.disarm();
                return Err(e);
            }
        };
        Ok((first, reader, done_rx, guard))
    }

    /// Whether the public allowance `bucket` admits a request right now.
    fn allowance_admits(&self, bucket: Option<u64>) -> bool {
        self.public
            .as_ref()
            .is_none_or(|public| public.allowance.remaining(bucket, now_secs()).is_ok())
    }
}

/// The next frame of a streaming body, or `None` at its end: the pull's first
/// bytes, then each chunk off the pipe, charged against the allowance and
/// passed through the rewrite. A pull error after the first byte ends the
/// body with an error, so a browser never takes a verified prefix for the
/// whole file; `signer` is the capability to retire on a funding stop.
async fn next_frame(
    mut s: Streaming,
    signer: Option<Address>,
) -> Option<(Result<Frame<Bytes>, std::io::Error>, Streaming)> {
    let gateway = Arc::clone(&s.guard.gateway);
    let chunk = if let Some(first) = s.first.take() {
        first
    } else {
        let mut buf = vec![0u8; CHUNK_BYTES];
        match s.reader.read(&mut buf).await {
            Ok(0) => {
                if let Err(e) = finished(s.done.take()?).await {
                    if let (PullError::Funding(_), Some(signer)) = (&e, signer) {
                        gateway.mark_spent(signer).await;
                    }
                    s.rewrite = None;
                    let e = std::io::Error::other(pull_error_text(&e));
                    return Some((Err(e), s));
                }
                // The pull is whole: the rewrite lets out its tail.
                s.clean = true;
                let tail = s.rewrite.take()?.finish().map(Frame::data);
                return Some((tail, s));
            }
            Ok(_) if s.cut => {
                // The frame that reached the allowance is out and more
                // follows: the body ends with an error, not as a whole file.
                s.done = None;
                s.rewrite = None;
                let e = std::io::Error::other("the gateway's allowance ran out mid-stream");
                return Some((Err(e), s));
            }
            Ok(n) => {
                buf.truncate(n);
                Bytes::from(buf)
            }
            Err(e) => {
                s.done = None;
                return Some((Err(e), s));
            }
        }
    };
    s.delivered = s
        .delivered
        .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
    s.guard.delivered.store(s.delivered, Ordering::Relaxed);
    s.cut = !gateway.charge(s.guard.bucket, chunk.len());
    gateway.metrics.bytes(chunk.len());
    if s.cut {
        tracing::info!("the allowance is reached; this stream ends with its current frame");
    }
    let out = match s.rewrite.as_mut() {
        Some(rewrite) => rewrite.feed(&chunk),
        None => Ok(chunk),
    };
    if out.is_err() {
        s.done = None;
    }
    Some((out.map(Frame::data), s))
}

/// A response body in flight: the pull's first bytes until they go out, the
/// pipe the rest arrives on, the pull's result until it is read, and the
/// rewrite the bytes pass through, if any.
struct Streaming {
    first: Option<Bytes>,
    reader: DuplexStream,
    done: Option<oneshot::Receiver<anyhow::Result<()>>>,
    rewrite: Option<Rewrite>,
    /// Set once a frame reached the public allowance: the next frame, if
    /// any, ends the body with an error instead.
    cut: bool,
    /// Set once the pull ended whole and its bytes are out.
    clean: bool,
    /// Content bytes handed to the response so far.
    delivered: u64,
    /// The response's declared length, when it has one. The server drops the
    /// body once that many bytes are out, with no final poll, so reaching it
    /// is as clean an end as the pull's own.
    size: Option<u64>,
    /// Armed since before the pull started; disarmed only by a whole end.
    guard: PullGuard,
}

impl Drop for Streaming {
    /// A body that ended whole owes the allowance nothing more; one dropped
    /// before that (a client that left, a cut, a fault) is charged, by the
    /// guard, what the pull may have paid past the frames it delivered.
    fn drop(&mut self) {
        if self.clean || self.size.is_some_and(|size| self.delivered >= size) {
            self.guard.disarm();
        }
    }
}

/// Who pays for a pull, as `/_status` lists it.
#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PayerLabel {
    /// The gateway's own pool.
    Pool,
    /// A held capability, by its delegate key.
    Capability {
        /// The delegate key.
        signer: Address,
    },
}

impl PayerLabel {
    fn of(paying: &Payment) -> Self {
        match paying {
            Payment::Held(credential) => Self::Capability {
                signer: credential.signer.address(),
            },
            Payment::Pool { .. } => Self::Pool,
        }
    }
}

/// A pull in flight, held by its [`PullGuard`].
struct ActivePull {
    hash: [u8; 32],
    namespace: Option<u64>,
    range: Option<(u64, u64)>,
    payer: PayerLabel,
    since: Instant,
    /// Content bytes its response delivered so far.
    delivered: Arc<AtomicU64>,
}

/// One pull in flight as `/_status` lists it.
#[derive(Debug, serde::Serialize)]
struct ActivePullView {
    /// The blob, hex.
    hash: String,
    /// The namespace the request named, if any.
    namespace: Option<u64>,
    /// The byte window, `[offset, length]`, of a ranged pull.
    range: Option<(u64, u64)>,
    payer: PayerLabel,
    /// Seconds since the pull started.
    since_secs: u64,
    /// Content bytes its response delivered so far.
    delivered_bytes: u64,
}

impl Gateway {
    /// The pulls in flight, oldest first.
    fn active_pulls(&self) -> Vec<ActivePullView> {
        let active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut pulls: Vec<_> = active.iter().collect();
        pulls.sort_by_key(|(id, pull)| (pull.since, **id));
        pulls
            .into_iter()
            .map(|(_, pull)| ActivePullView {
                hash: alloy::hex::encode(pull.hash),
                namespace: pull.namespace,
                range: pull.range,
                payer: pull.payer,
                since_secs: pull.since.elapsed().as_secs(),
                delivered_bytes: pull.delivered.load(Ordering::Relaxed),
            })
            .collect()
    }
}

/// What a pull in flight owes the public allowance if it ends short of
/// whole: the bound of what it may have paid past the frames its response
/// delivered ([`DROP_PENALTY_BYTES`]). Armed from before the job reaches the
/// worker, so a request dropped at any point (waiting for its first byte
/// included) charges it; disarmed once the pull delivered whole, or ended
/// before it could have paid for anything.
struct PullGuard {
    gateway: Arc<Gateway>,
    armed: bool,
    /// The allowance bucket the pull spends.
    bucket: Option<u64>,
    /// The pull's entry in [`Gateway::active`].
    id: u64,
    /// Content bytes its response delivered so far, shared with that entry.
    delivered: Arc<AtomicU64>,
}

impl PullGuard {
    /// A guard for a pull `gateway` is about to start against `bucket`:
    /// armed on a public gateway, counted among its pulls in flight, and
    /// listed in [`Gateway::active`] until it drops.
    fn new(
        gateway: Arc<Gateway>,
        bucket: Option<u64>,
        hash: [u8; 32],
        namespace: Option<u64>,
        range: Option<(u64, u64)>,
        payer: PayerLabel,
    ) -> Self {
        gateway.metrics.pull_started();
        let armed = gateway.public();
        let id = gateway.next_pull.fetch_add(1, Ordering::Relaxed);
        let delivered = Arc::new(AtomicU64::new(0));
        gateway
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                id,
                ActivePull {
                    hash,
                    namespace,
                    range,
                    payer,
                    since: Instant::now(),
                    delivered: Arc::clone(&delivered),
                },
            );
        Self {
            gateway,
            armed,
            bucket,
            id,
            delivered,
        }
    }

    const fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PullGuard {
    fn drop(&mut self) {
        self.gateway.metrics.pull_ended();
        self.gateway
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
        if self.armed {
            self.gateway.charge_drop(self.bucket);
        }
    }
}

/// The pull's first verified bytes, or the error it ended with before any.
///
/// The pull closes the pipe before it reports its result, so an end of
/// stream with no bytes waits for that result: a failed pull is an error
/// here, and a successful one is an empty body.
async fn first_chunk(
    reader: &mut DuplexStream,
    done_rx: oneshot::Receiver<anyhow::Result<()>>,
) -> Result<(Bytes, oneshot::Receiver<anyhow::Result<()>>), PullError> {
    let mut first = vec![0u8; CHUNK_BYTES];
    let n = reader
        .read(&mut first)
        .await
        .map_err(|e| PullError::Other(e.into()))?;
    if n > 0 {
        first.truncate(n);
        return Ok((Bytes::from(first), done_rx));
    }
    finished(done_rx).await?;
    // The body's end-of-stream check then finds the pull already done.
    let (spent_tx, spent_rx) = oneshot::channel();
    let _ = spent_tx.send(Ok(()));
    Ok((Bytes::new(), spent_rx))
}

/// The pull's own result once its bytes are drained.
async fn finished(done: oneshot::Receiver<anyhow::Result<()>>) -> Result<(), PullError> {
    match done.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(classify(e)),
        // The pull thread ended without a result: it panicked, so the bytes
        // it wrote may be short.
        Err(_) => Err(PullError::Other(anyhow::anyhow!(
            "the pull ended without a result"
        ))),
    }
}

/// A pull error is a funding stop when [`decdn_client::FundingNeeded`] is
/// attached anywhere on it, as context or as the error itself, and an
/// allowance stop when the worker refused the pull at its slot.
fn classify(e: anyhow::Error) -> PullError {
    if e.downcast_ref::<decdn_client::FundingNeeded>().is_some() {
        PullError::Funding(e)
    } else if e.downcast_ref::<worker::NotAdmitted>().is_some() {
        PullError::Allowance
    } else {
        PullError::Other(e)
    }
}

/// Mark a manifest pull that the cap of `cap_mb` ended as
/// [`PullError::NotManifest`]: the gateway reads no larger blob as a manifest.
/// `raw` is the hint to fetch the raw blob, when the gateway serves it.
fn over_manifest_cap(e: PullError, cap_mb: u64, raw: Option<&str>) -> PullError {
    match e {
        PullError::Other(e) if e.downcast_ref::<decdn_client::BlobTooLarge>().is_some() => {
            let hint = raw.map_or_else(String::new, |raw| format!("; {raw}"));
            PullError::NotManifest(e.context(format!(
                "the blob is larger than {cap_mb} MiB, the most this gateway reads as a \
                 bundle manifest{hint}"
            )))
        }
        e => e,
    }
}

/// Entry `path` of bundle `hash` as a relative URL path, each segment
/// percent-encoded as in its `decdn:` URI.
fn entry_link(hash: [u8; 32], path: &str) -> String {
    let uri = DecdnUri::new(hash, UriTarget::BundleEntry(path.to_owned()), None).to_string();
    // `decdn:<hash>/` precedes the path.
    uri.get(decdn_client::uri::SCHEME.len() + 65..)
        .unwrap_or_default()
        .to_owned()
}

/// The hint to fetch blob `hash` raw, at its gateway path.
fn raw_blob_hint(hash: [u8; 32], namespace: Option<u64>) -> String {
    let uri = DecdnUri::new(hash, UriTarget::Blob, namespace).to_string();
    let path = uri.strip_prefix(decdn_client::uri::SCHEME).unwrap_or(&uri);
    format!("fetch `/{path}` for the raw blob")
}

fn pull_error_text(e: &PullError) -> String {
    match e {
        PullError::Funding(e) | PullError::NotManifest(e) | PullError::Other(e) => {
            format!("{e:#}")
        }
        PullError::Allowance => "the gateway's allowance is spent".to_owned(),
    }
}

/// The CORS preflight answer for `/_delegate` and `/_capability`: any origin
/// may call them, with a JSON body.
fn preflight(origin: Option<&str>) -> Response<Body> {
    let Some(origin) = origin else {
        return text(StatusCode::FORBIDDEN, "no Origin\n");
    };
    let mut response = with_cors(text(StatusCode::NO_CONTENT, ""), Some(origin));
    let headers = response.headers_mut();
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        header::HeaderValue::from_static("GET, PUT"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        header::HeaderValue::from_static("content-type"),
    );
    headers.insert(
        header::ACCESS_CONTROL_MAX_AGE,
        header::HeaderValue::from_static("600"),
    );
    // Chrome asks before a public page reaches a loopback address.
    headers.insert(
        "access-control-allow-private-network",
        header::HeaderValue::from_static("true"),
    );
    response
}

/// The response headers a page on another origin may read from a public
/// gateway: the body's length and type, the hash (`ETag`), the window
/// (`Content-Range`, `Accept-Ranges`) and, on a 402 or 503, when to retry and
/// where to fetch instead (`Retry-After`, `Link`).
const PUBLIC_EXPOSED: header::HeaderValue = header::HeaderValue::from_static(
    "Content-Length, Content-Range, Accept-Ranges, Content-Type, Content-Disposition, ETag, \
     Retry-After, Link",
);

/// The origins that may read a public gateway's answers: all of them.
const ANY_ORIGIN: header::HeaderValue = header::HeaderValue::from_static("*");

/// The preflight answer of a public gateway for `path`, `bare` when on its
/// own host rather than a site or namespace host. The gateway asks for no
/// credentials and holds no token, so any origin may read its content, with a
/// `Range`, and `PUT /_pointer` on the bare host with a JSON body. The API
/// paths answer 404 on every other host, and `/_delegate` and `/_capability`
/// on the bare host too. It sets no `Access-Control-Allow-Origin`:
/// [`readable_anywhere`] does, in [`Gateway::handle`].
fn public_preflight(path: &str, bare: bool) -> Response<Body> {
    let (methods, headers) = match path {
        "/_delegate" | "/_capability" | "/_pointer" | "/_status" if !bare => {
            return text(
                StatusCode::NOT_FOUND,
                "the gateway API answers on the gateway origin only\n",
            );
        }
        "/_delegate" | "/_capability" | "/_status" => {
            return text(StatusCode::NOT_FOUND, "a public gateway has no API\n");
        }
        "/_pointer" => ("PUT", "content-type"),
        _ => ("GET, HEAD", "range"),
    };
    let mut response = text(StatusCode::NO_CONTENT, "");
    let fields = response.headers_mut();
    fields.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        header::HeaderValue::from_static(methods),
    );
    fields.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        header::HeaderValue::from_static(headers),
    );
    fields.insert(
        header::ACCESS_CONTROL_MAX_AGE,
        header::HeaderValue::from_static("600"),
    );
    response
}

/// `response` of a public gateway, readable by every origin.
fn readable_anywhere(mut response: Response<Body>) -> Response<Body> {
    let headers = response.headers_mut();
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, ANY_ORIGIN);
    headers.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, PUBLIC_EXPOSED);
    response
}

/// `response` readable by `origin`.
fn with_cors(mut response: Response<Body>, origin: Option<&str>) -> Response<Body> {
    if let Some(value) = origin.and_then(|o| header::HeaderValue::from_str(o).ok()) {
        let headers = response.headers_mut();
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
        headers.insert(header::VARY, header::HeaderValue::from_static("Origin"));
    }
    response
}

/// Types a browser runs or renders as a document. The gateway serves them as
/// plain text, so fetched content never runs on the gateway's origin.
const ACTIVE_TYPES: &[&str] = &[
    "application/xhtml+xml",
    "image/svg+xml",
    "application/xml",
    "application/javascript",
    "application/ecmascript",
];

/// The `Content-Type` for an entry, guessed from its file name. Every `text/*`
/// type and every type in [`ACTIVE_TYPES`] is served as plain text; a name
/// with no known extension is served as bytes.
fn content_type(name: &str) -> String {
    let guess = mime_guess::from_path(name).first_or_octet_stream();
    if guess.type_() == mime_guess::mime::TEXT || ACTIVE_TYPES.contains(&guess.essence_str()) {
        "text/plain; charset=utf-8".to_owned()
    } else {
        guess.essence_str().to_owned()
    }
}

/// An entry its manifest declares empty: its headers and an empty body, with
/// no pull. There is nothing to deliver, and a declared size of zero must not
/// become an uncapped pull of whatever the hash really names.
fn empty_entry(path: &str, hash: [u8; 32], present: Present) -> Response<Body> {
    let name = path.rsplit('/').next().unwrap_or(path);
    let content_type = match present {
        Present::Bare | Present::Download => content_type(name),
        Present::Site => site::content_type(name),
    };
    head_response(&content_type, Some(0), hash)
}

/// What a response delivers of a blob: the size its manifest declares, when
/// one does, the byte window a `Range` asked for within it, and whether the
/// response has a fixed length at all (a rewritten page does not).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Extent {
    /// The blob's size as the manifest declares it.
    size: Option<u64>,
    /// The `(offset, len)` window served, or the whole blob.
    window: Option<(u64, u64)>,
    /// Whether the response's length is the window's or the size: a rewrite
    /// changes the length, so a rewritten response has none, and takes no
    /// range.
    fixed: bool,
}

impl Extent {
    /// A blob whose size nothing declares: whole, with no fixed length.
    const UNSIZED: Self = Self {
        size: None,
        window: None,
        fixed: false,
    };

    /// The extent of an entry of `size` for a request with the `Range`
    /// header `range`, `rewritten` as it streams. A range on a sized entry
    /// that is not rewritten is honoured; one past the end is refused with
    /// the size; any other is ignored.
    ///
    /// # Errors
    ///
    /// The entry's size, when the range is unsatisfiable.
    fn of(size: Option<u64>, range: Option<&str>, rewritten: bool) -> Result<Self, u64> {
        let Some(declared) = size.filter(|_| !rewritten) else {
            return Ok(Self {
                size,
                window: None,
                fixed: false,
            });
        };
        let window = match range {
            Some(range) => byte_window(range, declared).map_err(|()| declared)?,
            None => None,
        };
        Ok(Self {
            size,
            window,
            fixed: true,
        })
    }

    /// The length the response declares, when it has one.
    fn length(&self) -> Option<u64> {
        if !self.fixed {
            return None;
        }
        self.window.map_or(self.size, |(_, len)| Some(len))
    }

    /// The bytes the pull pays for, as far as anything knows: the window
    /// widened to whole chunk groups (ADR 038), clipped to the entry; the
    /// declared size for the whole. What the allowance admits by, so two
    /// bytes inside a chunk group cost what the pull pays for them.
    fn billable(&self) -> Option<u64> {
        let size = self.size?;
        let Some((offset, len)) = self.window else {
            return Some(size);
        };
        let group = decdn_bao_range::CHUNK_GROUP_BYTES;
        let start = offset - offset % group;
        let end = offset
            .saturating_add(len)
            .div_ceil(group)
            .saturating_mul(group)
            .min(size);
        Some(end.saturating_sub(start))
    }

    /// What a window costs beyond the bytes it delivers: the rest of the
    /// chunk groups the pull pays for around it.
    fn surcharge(&self) -> usize {
        let Some((_, len)) = self.window else {
            return 0;
        };
        let extra = self.billable().unwrap_or(len).saturating_sub(len);
        usize::try_from(extra).unwrap_or(usize::MAX)
    }
}

/// Whether a response for entry `path` presented as `present` is rewritten
/// as it streams, so its length is not the entry's.
fn rewrites(present: Present, path: &str) -> bool {
    if present != Present::Site {
        return false;
    }
    let name = path.rsplit('/').next().unwrap_or(path);
    let content_type = site::content_type(name);
    let essence = content_type.split(';').next().unwrap_or_default();
    Rewrite::for_type(essence).is_some()
}

/// The request's `Range` header, when it has one the gateway can read.
fn range_header(headers: &HeaderMap) -> Option<&str> {
    headers.get(header::RANGE).and_then(|v| v.to_str().ok())
}

/// The byte window `(offset, len)` of a `size`-byte entry that the `Range`
/// header `range` asks for: `bytes=a-b`, `bytes=a-` or `bytes=-n`, with the
/// end clipped to the entry. `Ok(None)` is a range the gateway ignores, as
/// HTTP allows: several ranges, another unit, a malformed or inverted one.
///
/// # Errors
///
/// A range that starts at or past the end, or a suffix of nothing: the
/// request is not satisfiable.
fn byte_window(range: &str, size: u64) -> Result<Option<(u64, u64)>, ()> {
    let Some(spec) = range.trim().strip_prefix("bytes") else {
        return Ok(None);
    };
    let Some(spec) = spec.trim_start().strip_prefix('=') else {
        return Ok(None);
    };
    if spec.contains(',') {
        return Ok(None);
    }
    let Some((start, end)) = spec.split_once('-') else {
        return Ok(None);
    };
    let (start, end) = (start.trim(), end.trim());
    let number = |s: &str| s.parse::<u64>().ok();
    if start.is_empty() {
        let Some(suffix) = number(end) else {
            return Ok(None);
        };
        if suffix == 0 || size == 0 {
            return Err(());
        }
        let len = suffix.min(size);
        return Ok(Some((size - len, len)));
    }
    let Some(first) = number(start) else {
        return Ok(None);
    };
    if first >= size {
        return Err(());
    }
    let last = if end.is_empty() {
        size - 1
    } else {
        let Some(last) = number(end) else {
            return Ok(None);
        };
        if last < first {
            return Ok(None);
        }
        last.min(size - 1)
    };
    Ok(Some((first, last - first + 1)))
}

/// The answer to a range no `size`-byte entry can satisfy.
fn not_satisfiable(size: u64) -> Response<Body> {
    let mut response = text(StatusCode::RANGE_NOT_SATISFIABLE, "");
    if let Ok(value) = header::HeaderValue::from_str(&format!("bytes */{size}")) {
        response.headers_mut().insert(header::CONTENT_RANGE, value);
    }
    response.headers_mut().insert(
        header::ACCEPT_RANGES,
        header::HeaderValue::from_static("bytes"),
    );
    response
}

/// A `HEAD` answer: the type, the length when known (and with it that ranges
/// are taken), and the blob's hash as the entity tag, with no body.
fn head_response(content_type: &str, size: Option<u64>, hash: [u8; 32]) -> Response<Body> {
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::ETAG, format!("\"{}\"", alloy::hex::encode(hash)))
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff");
    if let Some(size) = size {
        response = response
            .header(header::CONTENT_LENGTH, size)
            .header(header::ACCEPT_RANGES, "bytes");
    }
    response
        .body(full(String::new()))
        .unwrap_or_else(|_| text(StatusCode::INTERNAL_SERVER_ERROR, "bad head response\n"))
}

/// The answer to a request for `uri` (on `page_host`) whose pull ended with
/// `e`: a blob that is no manifest is not found, a pool that cannot pay asks
/// the client to pay elsewhere, and anything else failed upstream. A
/// `public` gateway keeps the upstream detail (node ids, addresses, chain
/// errors) in its log and answers a bare 502; a loopback gateway shows its
/// own user the detail.
fn pull_failed(e: PullError, uri: &DecdnUri, page_host: &str, public: bool) -> Response<Body> {
    match e {
        PullError::Allowance => pay_elsewhere(
            StatusCode::PAYMENT_REQUIRED,
            uri,
            0,
            "the gateway's allowance is spent",
        ),
        PullError::NotManifest(e) => {
            let error = format!("{e:#}");
            tracing::info!(%error, host = %page_host, "a bundle URI names no manifest");
            text(StatusCode::NOT_FOUND, &format!("{error}\n"))
        }
        PullError::Funding(e) => {
            tracing::warn!(error = %e, "the gateway's pool cannot pay");
            pool_cannot_pay(uri)
        }
        PullError::Other(e) if public => {
            tracing::warn!(error = format!("{e:#}"), %uri, "an upstream pull failed");
            text(StatusCode::BAD_GATEWAY, "the upstream pull failed\n")
        }
        PullError::Other(e) => text(StatusCode::BAD_GATEWAY, &format!("{e:#}\n")),
    }
}

/// The answer to a pointer whose `seq` is not above `current`, the one held:
/// the client signs a fresh one above it.
fn stale_pointer(current: u64) -> Response<Body> {
    json(
        StatusCode::CONFLICT,
        &serde_json::json!({
            "error": "the gateway holds a pointer at or above this seq",
            "seq": current,
        }),
    )
}

/// The answer when the gateway's own pool cannot pay for `uri` right now: the
/// client did nothing wrong, so it may retry, or fetch the URI with a paying
/// client of its own.
fn pool_cannot_pay(uri: &DecdnUri) -> Response<Body> {
    pay_elsewhere(
        StatusCode::SERVICE_UNAVAILABLE,
        uri,
        60,
        "the gateway's pool cannot pay right now",
    )
}

/// The answer when the gateway does not pay for `uri` now, for `why`: `402`
/// for a spent allowance, `503` for a pool that cannot pay. The client
/// retries after `retry_after_secs`, or fetches the canonical URI with a
/// paying client of its own; the `Link` header and the JSON body carry it.
fn pay_elsewhere(
    status: StatusCode,
    uri: &DecdnUri,
    retry_after_secs: u64,
    why: &str,
) -> Response<Body> {
    let mut response = json(
        status,
        &serde_json::json!({
            "error": why,
            "uri": uri.to_string(),
            "retry_after_secs": retry_after_secs,
        }),
    );
    let headers = response.headers_mut();
    if let Ok(retry) = header::HeaderValue::from_str(&retry_after_secs.to_string()) {
        headers.insert(header::RETRY_AFTER, retry);
    }
    if let Ok(link) = header::HeaderValue::from_str(&format!("<{uri}>; rel=\"alternate\"")) {
        headers.insert(header::LINK, link);
    }
    response
}

/// The answer to `HEAD`, which would cost a paid pull and then drop its bytes.
fn head_refused() -> Response<Body> {
    let mut response = text(StatusCode::METHOD_NOT_ALLOWED, "");
    response
        .headers_mut()
        .insert(header::ALLOW, header::HeaderValue::from_static("GET"));
    response
}

fn redirect(location: &str) -> Response<Body> {
    let mut response = text(StatusCode::FOUND, "");
    if let Ok(location) = header::HeaderValue::from_str(location) {
        response.headers_mut().insert(header::LOCATION, location);
    }
    response
}

fn full(body: String) -> Body {
    Full::new(Bytes::from(body))
        .map_err(|never| match never {})
        .boxed()
}

fn text(status: StatusCode, body: &str) -> Response<Body> {
    typed(status, body.to_owned(), "text/plain; charset=utf-8")
}

fn json<T: serde::Serialize + ?Sized>(status: StatusCode, value: &T) -> Response<Body> {
    let body = serde_json::to_string(value).unwrap_or_else(|_| "null".into());
    typed(status, body, "application/json")
}

fn html(status: StatusCode, body: &str) -> Response<Body> {
    let page = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" \
         content=\"width=device-width\"><title>decdn gateway</title><style>body{{font-family:\
         system-ui,sans-serif;max-width:48rem;margin:3rem auto;padding:0 1rem}}td{{padding:\
         .2rem 1rem .2rem 0}}</style></head><body>{body}</body></html>"
    );
    typed(status, page, "text/html; charset=utf-8")
}

fn typed(status: StatusCode, body: String, content_type: &'static str) -> Response<Body> {
    let mut response = Response::new(full(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static(content_type),
    );
    response
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn human_bytes(n: u64) -> String {
    indicatif::HumanBytes(n).to_string()
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
