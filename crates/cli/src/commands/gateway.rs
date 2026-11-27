//! `decdn gateway`: serve `decdn:` URIs (#2449) to a browser over loopback
//! HTTP.
//!
//! **Content.** `GET /<hash>[/<path>][?ns=<id>]` serves
//! `decdn:<hash>[/<path>][?ns=<id>]`. A blob or a bundle entry streams its
//! verified bytes in order as they arrive; a whole bundle (`/<hash>/`) gets an
//! HTML index of its entries. The gateway pays for it with the capabilities
//! it holds (**Capabilities**). `HEAD` gets 405, since answering it would
//! cost a paid pull. A browser's navigation to a bundle or an entry
//! redirects to the bundle's site, at no cost.
//!
//! **Sites.** Every bundle is also a website on its own browser origin,
//! `http://[<ns>.]<hash32>.localhost:<port>/` (`site`), so its pages run
//! isolated from every other bundle and from the gateway, with no extension.
//! `/` serves the bundle's `index.html` (or a file list), `/<path>` the entry
//! at `<path>`, and a directory path its `index.html`; HTML, SVG and scripts
//! are served as themselves, under a policy that confines the page to its
//! origin. A site's `decdn:` URIs are rewritten as the page streams
//! (`rewrite`) to `/_decdn/<hash>…` on the site's own origin, which serves
//! that content (and, for a directory, its `index.html`) to the page's own
//! requests and redirects a navigation to the other bundle's site. The
//! browser's `Sec-Fetch-*` headers decide what may spend on a site origin
//! (`site::may_spend`), and a site pays with the capabilities the gateway
//! holds, as a content request does. On a loopback gateway, a top-level
//! navigation to a site from a page on another site gets an interstitial
//! that names the site, and the site serves nothing until the user
//! continues; a typed URL, a bookmark and the site's own links open it
//! directly (`consent`). The API paths answer on the gateway origin only.
//!
//! **Capabilities.** A capability (ADR 003 § Capability delegation) lets a
//! delegate key the gateway holds spend from a pool, so a pool owner funds a
//! gateway with no funded wallet on its machine. The gateway holds at most one
//! pending key, which no capability claims yet: `GET /_delegate` returns its
//! address and creates it when there is none, so repeated calls get the same
//! address until a capability claims the key. `PUT /_capability` takes a
//! `dcap1:` capability issued to a key the gateway holds, and its owner
//! signature must recover the on-chain owner of its pool on one of the
//! configured `PaymentPool` contracts, which every pull it pays for then
//! pays through (`capability_pool`). It also takes the capability's scope,
//! always stated: `"any"` pays for any request, and a namespace scope its
//! pool owner signs pays only for those namespaces' content (`scope`). A key
//! holds at most one capability per pool, a pool id on one contract: a key
//! the gateway does not hold, or a second capability from one pool, answers
//! 409.
//! A zero cap, or an expiry already inside the expiry margin nodes apply
//! ([`EXPIRY_MARGIN_SECS`]), is refused before the chain is read.
//! `GET /_capability` lists the capabilities in the order they were
//! registered, which is the order the gateway pays with them: a pull goes to
//! the first one that no funding stop retired and that is outside the expiry
//! margin, and one whose pull stops for funding before its first byte is
//! retired and the next one takes the request. The gateway holds several at
//! once, so an operator registers the next capability before the current one
//! runs low. These routes take no credential: nothing they do spends money
//! without a pool owner's signature, and an operator who wants
//! authentication puts a proxy in front. Keys and capabilities live in memory
//! only: a restart drops every one, and the operator provisions them again.
//!
//! **Boundaries.** A loopback gateway takes no token. Every request passes
//! a browser-header check instead (`site::browser_gate`). A top-level `GET`
//! navigation passes from anywhere, so a link on any page opens content.
//! Every other request from a page on another site (`Sec-Fetch-Site:
//! cross-site`), from a page whose `Origin` is none of the gateway's own
//! origins, or from a browser that sends no `Sec-Fetch-Site` (one with an
//! `Origin`, or a `User-Agent` that starts `Mozilla/`) gets 403. A client
//! outside a browser sends none of these and passes. So another page can
//! make the gateway pay only for content the browser opens as a whole page.
//! A link on another site opens a site's pages, which spend on what they
//! load, only once the user continues past its interstitial (`consent`).
//! It answers nothing cross-origin, so a page reads no response. The gateway listens on loopback only and checks
//! `Host` against DNS rebinding: it answers its own loopback names and
//! well-formed site names on its port, nothing else.
//!
//! **Public.** With `--public-url <origin>` the gateway serves anyone at that
//! origin, behind a proxy that terminates TLS: `<origin>/<hash>[/<path>]`,
//! every bundle as a site at `<scheme>://[<ns>.]<hash32>.<host>/`, and a
//! namespace at `<scheme>://<ns>.<host>/` once it has a pointer here. There
//! is no token. It pays with the capabilities it holds, as a loopback gateway
//! does, and with nothing else: a gateway holds no keystore and no pool of
//! its own, so an operator funds it from outside and replaces its machine with
//! nothing to tear down. The capabilities' caps bound what it spends. A
//! request nothing can pay for answers `503` with the canonical `decdn:` URI
//! in a `Link` header and a JSON body, so a paying client continues on its
//! own.
//! An entry's pull is capped at the size its manifest declares, so a manifest
//! cannot make the gateway pay for more than it says, and a declared size is
//! what `Content-Length` counts on. A raw blob's size is
//! one a holder proves: every node that states a size sends a size proof,
//! which the fetch verifies against the hash before it uses the size (ADR
//! 005 § Bounded byte ranges). Before the response, the gateway
//! resolves the holders of a raw blob of unknown size and takes the first
//! size one proves, paying for one chunk group at most. A loopback gateway
//! does the same, on the capability that pays. A sized response takes one
//! `Range`: the response is `206` over exactly that window, the pull pays
//! only for the chunk-group-aligned span around it (ADR 038), and a window
//! past the end is `416`; a range the gateway cannot honour (several, a
//! rewritten page, a raw blob of unknown size) is ignored, as HTTP
//! allows, so a `curl -r`, a resumed download and a BEP 19 webseed all work.
//! `HEAD` on an entry is answered from the bundle manifest, at no pull of the
//! entry, and on a raw blob from its verified size, at no pull at all; a
//! raw blob of unknown size first learns its size, as its `GET` would. The
//! API paths answer on the bare host alone and `404` on every other,
//! and a site's `Sec-Fetch-*` headers decide nothing: the gateway pays.
//! Every answer is readable by every origin (`readable_anywhere`), and
//! `OPTIONS` is a preflight (`public_preflight`). A pull runs a small
//! read-ahead (`PUBLIC_READ_AHEAD_BYTES`), so a client that connects and
//! drops leaves little paid and undelivered. An upstream failure answers a
//! bare `502`, with the detail in the log.
//!
//! Every gateway keeps its buyer store in a directory that ends with the
//! process, as the in-memory keys whose lanes it records do.
//!
//! **State.** Every gateway counts what it served, refused and failed
//! (`metrics`). `GET /_status` returns that, the pulls in flight, the
//! capabilities, a public gateway's pointers, and what the
//! buyer-pool store holds, with each capability key's spend, as one JSON
//! document (`status`), which `decdn dashboard` polls and an operator's
//! controller reads to see what is left of each capability. Every gateway
//! answers it on its bare host, with no token. `--metrics-listen` serves
//! `GET /metrics` (Prometheus text) and `/_status` on a loopback address, for
//! the operator. The dashboard's own polls are not counted.
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
//!
//! **Cache.** Every pull streams into a [`CacheEngine`] under
//! `<data-dir>/cache` (`fetch::CacheStores`), which keeps each chunk group
//! verified against the blob's hash. A request whose bytes the cache holds,
//! the whole blob or its one `Range`, at the size a holder proved, is served
//! out of the cache (`Gateway::cache_hit`): no pull and no capability spent,
//! so a gateway that holds nothing that can pay still serves it
//! (`Gateway::cached_only`). A manifest the cache holds whole is
//! read from it with no pull. A blob the cache holds whole answers `HEAD` and gives
//! its length with no size lookup. A request the cache holds only part of
//! streams as a pull, which fetches only what the cache lacks. A hit counts as a use for eviction, and no entry a pull is
//! filling or a hit is reading is released. The eviction loop holds the cache under `--cache-mb`, and
//! under what keeps the node's default free-disk margin on the volume;
//! `--cache-mb 0` turns the cache off and each pull fills a scratch directory
//! that ends with it.

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, B256, U256};
use anyhow::Context as _;
use bytes::Bytes;
use decdn_client::bundle::{BundleManifest, MalformedManifest};
use decdn_client::session::ChainConfig;
use decdn_client::uri::{DecdnUri, UriTarget};
use decdn_common::address::parse_nonzero_address;
use decdn_common::cli::{self, ClientFetchArgs, GatewayArgs};
use decdn_common::config::{FileConfig, inside_capability_expiry_margin, load_file_config};
use decdn_common::redact::{sanitize_err_chain, sanitize_error_sources};
use decdn_incentive::publisher_registry::PublisherRegistry;
use decdn_incentive::{CapabilityGrant, Deployment, PoolId};
use http_body_util::{BodyExt as _, Limited, StreamBody};
use hyper::body::{Body as HttpBody, Frame};
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt as _, DuplexStream};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Semaphore, SemaphorePermit, mpsc, oneshot};

use decdn_cache::{CacheEngine, CacheMetrics, Flights};

use super::capability_pool::{CapabilityPoolError, capability_pool, read_pool_owner};
use super::fetch::{
    BlobFetch, CacheStores, Credential, MALFORMED_MANIFEST, MAX_MANIFEST_BYTES, manifest_cap_mb,
    resolve_chain,
};
use super::local_http::{Body, allowed_hosts, full, host_header, json, private_dir, text, typed};
use crate::known_chains;

mod consent;
mod delegates;
mod metrics;
pub(crate) mod pointer;
mod rewrite;
pub(crate) mod scope;
mod site;
mod status;
mod worker;

use consent::Consents;
use delegates::{Capability, Delegates};
use metrics::Metrics;
use pointer::{Pointers, SetError, SignedPointer, pointer_domain};
use rewrite::Rewrite;
use scope::{Scope, SignedScope, scope_domain};
use site::{Route, Site, SiteDomain};
use worker::{FetchBackend, Job, PullWorker};

/// The capability expiry margin nodes apply at the default redeem interval
/// (ADR 003 § Revocation): inside it a node accepts no voucher under the
/// capability, so the gateway pays with it no more and refuses to register
/// one already inside it.
pub const EXPIRY_MARGIN_SECS: u64 = decdn_common::config::capability_expiry_margin_secs(
    decdn_common::config::DEFAULT_REDEEM_INTERVAL_SECS,
);

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
/// a stuck request does not hold its pull slot. It is the gateway's default
/// for `--give-up-after-secs`. The same limit bounds how long a pull's write
/// waits on a response that does not read, so a client that stops reading
/// frees its slot.
const GIVE_UP_AFTER_SECS: u64 = 120;

/// The most pulls that run at once; a request past it waits for a slot. Each
/// pull stripes across up to `--max-sources` holders, so this also bounds the
/// streams the gateway opens on one node's connection. A public gateway sees
/// its proxy, not the client, so a per-client limit belongs on the proxy.
const MAX_PULLS: usize = 16;

/// The most parsed manifests the gateway keeps.
const MAX_CACHED_MANIFESTS: usize = 16;

/// The manifest bytes the cache keeps across all its entries. The newest
/// entry stays even when it alone exceeds this.
const MANIFEST_CACHE_BYTES: usize = 64 << 20;

/// The most hashes the gateway remembers as no bundle manifest.
const MAX_NOT_MANIFESTS: usize = 1024;

/// The manifest bytes the gateway buffers at once over every fill in flight:
/// four buffers at the manifest cap ([`MAX_MANIFEST_BYTES`]). A fill takes
/// budget for its buffer as the buffer grows, so a pull still looking for a
/// holder, or one whose bytes trickle in, holds only what it has buffered. A
/// fill that would pass the budget fails at once instead of waiting: a fill
/// that waited would keep its pull's worker slot idle, and two fills that
/// each hold part of the budget would wait on each other. The budget counts
/// manifest buffers only: a fill's worker slot and its first-chunk read
/// buffer fall outside it.
const MANIFEST_BUFFER_BYTES: usize = 4 * MAX_MANIFEST_BYTES;

/// The most bytes of text a not-manifest verdict keeps. A parse error quotes
/// the publisher's paths and hash strings, which a manifest under the cap can
/// make megabytes long.
const MAX_VERDICT_TEXT: usize = 1024;

/// The duplex buffer between a pull and its HTTP response.
const PIPE_BYTES: usize = 1 << 20;

/// The read size for response chunks.
const CHUNK_BYTES: usize = 256 * 1024;

/// The read-ahead window of a public gateway's pulls: how far a pull runs,
/// and pays, ahead of what its response has taken. Small, so a dropped
/// response leaves little paid and undelivered; the client raises it to its
/// pull-window floor when this is below it.
const PUBLIC_READ_AHEAD_BYTES: u64 = 2 << 20;

/// Run the gateway until Ctrl-C.
///
/// # Errors
///
/// When `--listen` is not a loopback address and `--public-url` is not set,
/// when the gateway is given keystore or capability flags, when
/// `--public-url` is not an `http(s)` origin, when the data dir or listener
/// cannot be set up, when the chain coordinates or a public
/// gateway's `PublisherRegistry` cannot be resolved, or when a stored pointer
/// is damaged.
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
    let mut common = fetch_flags(&args.common)?;
    let file = load_file_config(config_path)?;
    fill_chain_defaults(&mut common, &file)?;
    let chain = resolve_chain(&common, &file)?.config;
    let stall = Duration::from_secs(*common.give_up_after_secs.get_or_insert(GIVE_UP_AFTER_SECS));
    let root = common
        .data_dir
        .clone()
        .unwrap_or_else(|| cli::common::expand_tilde(Path::new("~/.decdn/gateway")));
    private_dir(&root)?;
    let temp_store = store_dir(&root)?;
    common.data_dir = Some(temp_store.path().to_path_buf());
    let (sites, public) = mode(args, &root, chain.governance.chain_id, &file)?;

    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("listen on {}", args.listen))?;
    let cache = open_cache(&root, args.cache_mb, common.max_blob_mb).await?;
    let eviction = cache
        .as_ref()
        .map(|(engine, metrics)| start_eviction(engine, metrics, args.cache_mb, &root));
    let cache = cache.map(|(engine, _)| engine);
    let pulls = PullWorker::start(
        FetchBackend {
            common: common.clone(),
            config_path: config_path.map(Path::to_path_buf),
            cache: cache.clone().map(CacheStores::new),
        },
        MAX_PULLS,
        stall,
    )
    .await
    .context("start the gateway's pull worker")?;
    let gateway = Arc::new(Gateway {
        common,
        chain,
        delegates: std::sync::Mutex::default(),
        pulls,
        cache: cache.clone(),
        manifests: std::sync::Mutex::new(ManifestCache::default()),
        not_manifests: std::sync::Mutex::new(NotManifests::default()),
        manifest_fills: Flights::default(),
        manifest_buffer: Semaphore::new(MANIFEST_BUFFER_BYTES),
        hosts: allowed_hosts(args.listen),
        sites,
        consents: Consents::default(),
        metrics: Metrics::default(),
        active: std::sync::Mutex::new(HashMap::new()),
        next_pull: AtomicU64::new(0),
        started: Instant::now(),
        started_unix: now_secs(),
        listen: args.listen,
        public,
    });

    if let Some(metrics) = args.metrics_listen {
        let listener = TcpListener::bind(metrics)
            .await
            .with_context(|| format!("listen for metrics on {metrics}"))?;
        tokio::spawn(serve_metrics(Arc::clone(&gateway), listener));
    }
    announce(&gateway);
    let served = serve(&gateway, &listener).await;
    // The worker's queued lane writes land before the store dir is removed.
    gateway.pulls.stop().await;
    drop(temp_store);
    close_cache(eviction, cache.as_ref()).await;
    served
}

/// Stop the eviction loop, wait for its sweep to end, then shut `cache` down,
/// so no release races the cache's last writes and those writes reach its
/// database before the process exits.
async fn close_cache(
    eviction: Option<(oneshot::Sender<()>, tokio::task::JoinHandle<()>)>,
    cache: Option<&CacheEngine>,
) {
    if let Some((stop, sweeping)) = eviction {
        let _ = stop.send(());
        if let Err(e) = sweeping.await {
            tracing::warn!(error = %e, "the gateway cache's eviction loop did not end cleanly");
        }
    }
    if let Some(engine) = cache
        && let Err(e) = engine.shutdown().await
    {
        tracing::warn!(error = %e, "the gateway cache did not shut down cleanly");
    }
}

/// The cache directory under the gateway's data dir.
const CACHE_DIR: &str = "cache";

/// Open the gateway's cache under `root`, or `None` when `cache_mb` is `0`.
/// It has no origin: only the gateway's own pulls fill it.
///
/// # Errors
///
/// The directory cannot be created, or the store cannot open: another
/// gateway on the same data dir holds it.
async fn open_cache(
    root: &Path,
    cache_mb: u64,
    max_blob_mb: u64,
) -> anyhow::Result<Option<(CacheEngine, Arc<CacheMetrics>)>> {
    if cache_mb == 0 {
        return Ok(None);
    }
    let dir = root.join(CACHE_DIR);
    private_dir(&dir)?;
    let metrics = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        &dir,
        Vec::new(),
        max_blob_mb,
        decdn_cache::PinnedHashes::empty(),
        decdn_cache::RetryPolicy::default(),
        decdn_cache::CircuitBreakerPolicy::default(),
        Some(Arc::clone(&metrics)),
        Duration::from_secs(decdn_common::config::DEFAULT_GC_INTERVAL_SEC),
    )
    .await
    .with_context(|| format!("open the gateway cache at {}", dir.display()))?;
    Ok(Some((engine, metrics)))
}

/// Run the cache's eviction loop (#1173, ADR 040) under `cache_mb`, with the
/// node's default marks, sweep budget, tick and free-disk margin. The loop
/// stops at its next tick once the returned sender fires or drops; the
/// returned task ends with it.
fn start_eviction(
    engine: &CacheEngine,
    metrics: &Arc<CacheMetrics>,
    cache_mb: u64,
    root: &Path,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    use decdn_common::config as defaults;
    let (stop, stopped) = oneshot::channel();
    let params = decdn_cache::EvictionParams {
        cache_size_mb: cache_mb,
        disk_headroom_mb: defaults::DEFAULT_DISK_HEADROOM_MB,
        high_water_pct: defaults::DEFAULT_EVICTION_HIGH_WATER_PCT,
        target_pct: defaults::DEFAULT_EVICTION_TARGET_PCT,
        per_sweep_budget: defaults::DEFAULT_EVICTION_PER_SWEEP_BUDGET,
        tick: Duration::from_secs(defaults::DEFAULT_EVICTION_TICK_SECS),
    };
    let dir = root.join(CACHE_DIR);
    let sweeping = tokio::spawn(decdn_cache::eviction_driver::run(
        engine.clone(),
        Arc::clone(metrics),
        params,
        Arc::new(decdn_cache::LruEviction),
        move || decdn_common::disk::statvfs_target(&dir).map(|space| space.avail),
        |_| {},
        stopped,
    ));
    (stop, sweeping)
}

/// Print where the gateway serves, and on loopback the command that shows
/// its state.
fn announce(gateway: &Gateway) {
    match &gateway.sites {
        SiteDomain::Public { origin } => println!(
            "decdn gateway serving {origin} on {} (Ctrl-C to stop)",
            gateway.listen
        ),
        SiteDomain::Loopback { .. } => println!(
            "decdn gateway on http://{}/ (Ctrl-C to stop)\nsee it with: decdn dashboard",
            gateway.listen,
        ),
    }
}

/// The buyer store's directory, created owner-only under `root` and removed
/// when the returned guard drops. The delegate keys die with the process, so
/// their lane records do too.
fn store_dir(root: &Path) -> anyhow::Result<tempfile::TempDir> {
    let dir = tempfile::Builder::new()
        .prefix("store-")
        .tempdir_in(root)
        .context("create the gateway's buyer store dir")?;
    private_dir(dir.path())?;
    Ok(dir)
}

/// Where `args` put the sites, and what a public gateway holds: its pointers
/// under `<root>/pointers`, and the registry on `chain_id` it verifies them
/// against.
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

/// Serve the operator's routes on `listener`, a loopback address, for as
/// long as the gateway runs: `GET /metrics` answers Prometheus text, and
/// `/_status` answers as on the gateway's bare host. A `Host` other than the
/// listener's own loopback names is refused, so a page whose name was
/// rebound to this address reads nothing.
async fn serve_metrics(gateway: Arc<Gateway>, listener: TcpListener) {
    let hosts = Arc::new(listener.local_addr().map(allowed_hosts).unwrap_or_default());
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let gateway = Arc::clone(&gateway);
        let hosts = Arc::clone(&hosts);
        tokio::spawn(async move {
            let service =
                service_fn(move |req: Request<hyper::body::Incoming>| {
                    let gateway = Arc::clone(&gateway);
                    let known = hosts
                        .iter()
                        .any(|allowed| allowed == host_header(req.headers()));
                    async move {
                        if !known {
                            return Ok(text(StatusCode::FORBIDDEN, "unknown Host\n"));
                        }
                        let response = match (req.method(), req.uri().path()) {
                            (&Method::GET, "/metrics") => Some(typed(
                                StatusCode::OK,
                                gateway.metrics.render(),
                                "text/plain; version=0.0.4; charset=utf-8",
                            )),
                            (&Method::GET, "/_status") => {
                                Some(json(StatusCode::OK, &gateway.status().await))
                            }
                            _ => None,
                        };
                        Ok::<_, Infallible>(response.unwrap_or_else(|| {
                            text(StatusCode::NOT_FOUND, "GET /_status, /metrics\n")
                        }))
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

/// The fetch flags the gateway pulls with. The gateway holds its own delegate
/// keys and pays with nothing else, so its key and capability flags are
/// refused, and a keystore password file (often
/// `DECDN_KEYSTORE_PASSWORD_FILE` in the environment) is dropped unread.
fn fetch_flags(common: &ClientFetchArgs) -> anyhow::Result<ClientFetchArgs> {
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
    if common.governance.governance_rpc_url.is_none()
        && bc.and_then(|b| b.rpc_url.as_ref()).is_none()
    {
        common.governance.governance_rpc_url = Some(chain.public_rpc.to_owned());
    }
    if common.governance.governance_chain_id.is_none() && bc.and_then(|b| b.chain_id).is_none() {
        common.governance.governance_chain_id = Some(chain.chain_id);
    }
    // With no configured payment chain and no `--pool`, the gateway pays
    // through the known target's first pool; a config file that names any
    // payment chain supplies its own pools.
    let configured_payment = bc.is_some_and(|b| !b.payment_chains.is_empty());
    if let Some(payment) = chain.payment_chains()?.into_iter().next()
        && !configured_payment
        && common.payment.pool.is_none()
        && let Some(pool) = payment.payment_pools.first()
    {
        let pool = parse_nonzero_address(pool, "payment_pools")?;
        common.payment.pool = Some(
            Deployment {
                chain_id: payment.chain_id,
                payment_pool: pool,
            }
            .pool_account()
            .to_string(),
        );
        common.payment.pool_rpc_url = Some(payment.public_rpc.to_owned());
    }
    if common.capacity_bond_address.is_none()
        && bc.and_then(|b| b.capacity_bond_address.as_ref()).is_none()
    {
        common.capacity_bond_address = Some(addresses.capacity_bond);
    }
    Ok(())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// The body of `PUT /_capability`.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterRequest {
    /// The `dcap1:` capability.
    capability: String,
    /// What the capability pays for: `"any"`, or the pool owner's signed
    /// namespace scope.
    scope: Scope,
}

struct Gateway {
    common: ClientFetchArgs,
    /// The chains a registered capability's pool must live on: one of the
    /// configured payment pools. Registry reads use its governance chain.
    chain: ChainConfig,
    /// The delegate keys and the capabilities registered to them.
    delegates: std::sync::Mutex<Delegates>,
    /// Runs every pull, on one endpoint and one buyer-pool store.
    pulls: PullWorker,
    /// What the pulls filled, for a later request to read with no pull;
    /// `None` with `--cache-mb 0`.
    cache: Option<CacheEngine>,
    manifests: std::sync::Mutex<ManifestCache>,
    /// Hashes a pull found to be no manifest, so a repeat request pays for
    /// no second pull.
    not_manifests: std::sync::Mutex<NotManifests>,
    /// The fill locks of the manifests being pulled, one per hash and routing
    /// namespace: held while that missing manifest is pulled, so concurrent
    /// requests for it pull and pay once, and a stalled pull delays no other
    /// hash or namespace. Cache hits never wait on a fill.
    manifest_fills: Flights<FlightKey, Flight>,
    /// The manifest bytes left to buffer, out of [`MANIFEST_BUFFER_BYTES`]:
    /// one permit per byte of a fill buffer's capacity.
    manifest_buffer: Semaphore,
    hosts: Vec<String>,
    /// Where site origins live, and what a `Host` names.
    sites: SiteDomain,
    /// The sites the user continued to from another site's link (`consent`).
    consents: Consents,
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

/// Why a blob is no bundle manifest. The hash fixes the bytes, and a cap
/// stop comes from verified bytes or a proven size (a size with no valid
/// proof never reaches the cap), so the verdict holds for the hash while the
/// manifest cap stays the same: the whole run. It holds no request's raw-blob
/// hint: [`Self::error`] adds the asker's.
#[derive(Clone, Debug)]
enum NotManifest {
    /// The manifest cap ended the pull: the cap stop's text.
    OverCap(String),
    /// The bytes do not parse as a manifest: the parse error's text.
    Unparsed(String),
    /// The whole text, which names no raw blob.
    Plain(String),
}

impl NotManifest {
    /// The verdict on bytes whose parse failed with `err`. A malformed
    /// manifest's error names its publisher and no raw blob: the raw blob is
    /// the same manifest bytes.
    fn unparsed(err: anyhow::Error) -> Self {
        if err.is::<MalformedManifest>() {
            Self::Plain(clip(format!("{:#}", err.context(MALFORMED_MANIFEST))))
        } else {
            Self::Unparsed(clip(format!("{err:#}")))
        }
    }

    /// The error a request gets for the verdict. `raw` is the request's hint
    /// to fetch the raw blob; a cap stop names it only when `raw_past_cap`,
    /// as the raw blob pulls under the gateway's own cap.
    fn error(&self, cap_mb: u64, raw: &str, raw_past_cap: bool) -> anyhow::Error {
        match self {
            Self::OverCap(cause) => {
                let hint = if raw_past_cap {
                    format!("; {raw}")
                } else {
                    String::new()
                };
                anyhow::Error::msg(cause.clone()).context(format!(
                    "the blob is larger than {cap_mb} MiB, the most this gateway reads as a \
                     bundle manifest{hint}"
                ))
            }
            Self::Unparsed(cause) => {
                anyhow::Error::msg(cause.clone()).context(format!("not a bundle manifest; {raw}"))
            }
            Self::Plain(text) => anyhow::Error::msg(text.clone()),
        }
    }
}

/// `text` cut to at most [`MAX_VERDICT_TEXT`] bytes, on a character
/// boundary, with `…` where it was cut.
fn clip(mut text: String) -> String {
    const MARK: &str = "…";
    if text.len() > MAX_VERDICT_TEXT {
        text.truncate(text.floor_char_boundary(MAX_VERDICT_TEXT - MARK.len()));
        text.push_str(MARK);
    }
    text
}

/// A manifest's hash and the namespace its pull routes through.
type FlightKey = ([u8; 32], Option<u64>);

/// The fill lock of a manifest, by [`FlightKey`]. On a public gateway,
/// requests that waited on a fill which failed upstream share its failure:
/// one pool pays them all, so their pulls would end the same. A request that
/// comes after the failure pulls again.
#[derive(Default)]
struct Flight {
    /// How many fills under this lock have ended, bumped under the lock as
    /// each ends. A request reads it before it waits, so it tells a fill
    /// that ended while it waited from one that ended before it came.
    ended: AtomicU64,
    /// The lock a fill holds while it pulls, with the upstream failure of the
    /// fill that ended last, when that failure is shared.
    failed: Mutex<Option<String>>,
}

/// Hashes found to be no bundle manifest, with their verdicts, least recently
/// used first out, within [`MAX_NOT_MANIFESTS`] entries.
#[derive(Default)]
struct NotManifests {
    /// Oldest first.
    entries: std::collections::VecDeque<([u8; 32], NotManifest)>,
}

impl NotManifests {
    fn get(&mut self, hash: &[u8; 32]) -> Option<NotManifest> {
        let at = self.entries.iter().position(|(h, _)| h == hash)?;
        let entry = self.entries.remove(at)?;
        let verdict = entry.1.clone();
        self.entries.push_back(entry);
        Some(verdict)
    }

    fn insert(&mut self, hash: [u8; 32], verdict: NotManifest) {
        self.entries.retain(|(h, _)| *h != hash);
        self.entries.push_back((hash, verdict));
        while self.entries.len() > MAX_NOT_MANIFESTS {
            self.entries.pop_front();
        }
    }
}

/// How a manifest pull ends short of a manifest.
enum ManifestStop {
    /// The blob is no manifest, for good.
    Not(NotManifest),
    /// The pull ended for a reason that says nothing of the blob (upstream,
    /// funding); a later request pulls again.
    Pull(PullError),
}

impl From<PullError> for ManifestStop {
    fn from(e: PullError) -> Self {
        Self::Pull(e)
    }
}

/// What a pull that ended badly means for the gateway.
enum PullError {
    /// The capability cannot pay for the rest.
    Funding(anyhow::Error),
    /// The blob cannot be read as a bundle manifest: it is over the manifest
    /// cap, or its bytes do not parse as one. Nothing upstream failed.
    NotManifest(anyhow::Error),
    Other(anyhow::Error),
}

/// How a response presents what it serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Present {
    /// On the gateway origin: active types as plain text, a bundle as a list
    /// of `decdn:` links. The origin has no site policy, so a blob response
    /// carries `Content-Security-Policy: sandbox`: a type
    /// [`is_active_type`] misses still cannot run on the gateway's origin.
    Bare,
    /// On a site origin: each type as itself, `decdn:` URIs in HTML and CSS
    /// rewritten, a bundle as its `index.html` or a list of relative links.
    Site,
    /// As a file to save.
    Download,
}

impl Gateway {
    /// Whether this is a public gateway: no token, and its content readable
    /// by every origin.
    const fn public(&self) -> bool {
        self.public.is_some()
    }

    /// Answer `req` and count the answer, unless it is a dashboard's poll
    /// ([`Self::operator_request`]). On a public gateway, make it readable by
    /// every origin ([`readable_anywhere`]).
    async fn handle<B>(self: Arc<Self>, req: Request<B>) -> Response<Body>
    where
        B: HttpBody<Data = Bytes>,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let public = self.public.is_some();
        let counted = !self.operator_request(&req);
        let gateway = Arc::clone(&self);
        let response = self.answer(req).await;
        if counted {
            gateway.metrics.request(response.status());
        }
        if public {
            return readable_anywhere(response);
        }
        response
    }

    /// A 413 when `span`, the bytes a response pulls (the whole blob, or the
    /// span of its one `Range`), passes `--max-blob-mb` (`0` for no cap): the
    /// pull would end at the cap, so the gateway pays for no byte of it.
    fn over_blob_cap(&self, span: Option<u64>) -> Option<Response<Body>> {
        let cap = self.common.max_blob_mb.saturating_mul(1024 * 1024);
        let span = span?;
        (cap != 0 && span > cap).then(|| {
            text(
                StatusCode::PAYLOAD_TOO_LARGE,
                &format!(
                    "the response pulls {span} bytes, past this gateway's {} MiB cap\n",
                    self.common.max_blob_mb
                ),
            )
        })
    }

    async fn answer<B>(self: Arc<Self>, req: Request<B>) -> Response<Body>
    where
        B: HttpBody<Data = Bytes>,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let host = host_header(req.headers());
        let route = self.sites.route(host);
        if route.is_none() && !self.hosts.iter().any(|allowed| allowed == host) {
            return text(StatusCode::FORBIDDEN, "unknown Host\n");
        }
        if !self.public()
            && let Err((status, why)) = site::browser_gate(req.method(), req.headers(), |origin| {
                self.hosts.iter().any(|own| own == origin) || self.sites.route(origin).is_some()
            })
        {
            return text(status, why);
        }
        if self.public() && req.method() == Method::OPTIONS {
            return public_preflight(
                req.uri().path(),
                !matches!(route, Some(Route::Site(_) | Route::Namespace(_))),
            );
        }
        match route {
            Some(Route::Site(site)) => {
                return Self::with_policy(self.site(req, site, None).await);
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
                return Self::with_policy(self.site(req, site, Some(ns)).await);
            }
            Some(Route::Bare) | None => {}
        }
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
        match (method, path.as_str()) {
            (Method::OPTIONS, _) => text(StatusCode::FORBIDDEN, "no cross-origin access\n"),
            (Method::GET, "/") => html(
                StatusCode::OK,
                "<h1>decdn gateway</h1><p>Running. Open a bundle as a website at \
                 <code>/&lt;hash&gt;/</code>. Run <code>decdn dashboard</code> to see what \
                 the gateway holds and did.</p>",
            ),
            (Method::GET, "/favicon.ico") => text(StatusCode::NOT_FOUND, ""),
            (Method::GET, "/_delegate") => self.pending(),
            (Method::PUT, "/_capability") => self.register(req).await,
            (Method::GET, "/_capability") => self.list(),
            (Method::GET, "/_status") => json(StatusCode::OK, &self.status().await),
            (Method::PUT, "/_pointer") => text(
                StatusCode::NOT_FOUND,
                "a loopback gateway holds no namespace pointers\n",
            ),
            (Method::HEAD, _) => head_refused(),
            (Method::GET, _) => {
                self.content(req.headers(), target.trim_start_matches('/'))
                    .await
            }
            _ => text(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n"),
        }
    }

    /// What a public gateway answers on its bare host before the routes it
    /// shares with a loopback one: `PUT /_pointer`, and `HEAD` of content.
    /// `Err(req)` when the request is not one of those, and on a loopback
    /// gateway.
    async fn public_bare<B>(
        self: &Arc<Self>,
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
            (&Method::PUT, "/_pointer") => self.put_pointer(req).await,
            (&Method::HEAD, "/" | "/favicon.ico") => text(StatusCode::OK, ""),
            (&Method::HEAD, _) => {
                self.head_blob(
                    target.trim_start_matches('/'),
                    Present::Bare,
                    None,
                    Named::AsIs,
                )
                .await
            }
            _ => return Err(Box::new(req)),
        })
    }

    /// Whether `req` is for `/_status` on a loopback gateway's own host: the
    /// dashboard's polls, which the counters it shows leave out.
    fn operator_request<B>(&self, req: &Request<B>) -> bool {
        let host = host_header(req.headers());
        let path = req.uri().path();
        !self.public()
            && req.method() == Method::GET
            && self.hosts.iter().any(|allowed| allowed == host)
            && path == "/_status"
    }

    /// The site policy on `response`, unless it carries its own (the
    /// interstitial's, `consent::POLICY`).
    fn with_policy(mut response: Response<Body>) -> Response<Body> {
        response
            .headers_mut()
            .entry(header::CONTENT_SECURITY_POLICY)
            .or_insert(header::HeaderValue::from_static(
                site::CONTENT_SECURITY_POLICY,
            ));
        response
    }

    /// A request on a site origin: the site's own entries, and `/_decdn/` for
    /// any `decdn:` content, the site's own bundle included. `pointer` is the
    /// namespace whose pointer
    /// named the site, when one did: a capability scoped to it pays for the
    /// site's own bundle, and for no other content on its host.
    async fn site<B>(
        self: Arc<Self>,
        req: Request<B>,
        site: Site,
        pointer: Option<u64>,
    ) -> Response<Body>
    where
        B: HttpBody<Data = Bytes>,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        if req.method() == Method::POST && !self.public() {
            return self.consent(req, site).await;
        }
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
        // A public gateway serves anyone: the browser's headers decide
        // nothing there, and `curl` sends none.
        if !self.public() {
            if consent::asks(headers) && !self.consents.given(&site) {
                return self.consents.page(&site, &self.sites.site_host(&site));
            }
            if let Err((status, why)) = site::may_spend(headers) {
                return text(status, why);
            }
        }
        let target = req.uri().path_and_query().map_or(path, |pq| pq.as_str());
        // The pointer's own bundle, which a capability scoped to it pays for.
        let scoped_bundle = pointer.map(|ns| (ns, site.hash));
        let decdn = target.strip_prefix("/_decdn/").map(directory_index);
        let query = req.uri().query();
        if head {
            return match decdn {
                Some((rest, named)) => {
                    self.head_blob(&rest, Present::Site, scoped_bundle, named)
                        .await
                }
                None => self.head_site(&site, path, query, pointer).await,
            };
        }
        let range = range_header(headers);
        if let Some((rest, named)) = decdn {
            let uri: DecdnUri = match format!("decdn:{rest}").parse() {
                Ok(uri) => uri,
                Err(e) => return text(StatusCode::BAD_REQUEST, &format!("{e}\n")),
            };
            let pointer = pointer.filter(|_| uri.hash == site.hash);
            let get = Ask::Get { range, query };
            if !site::is_navigation(headers) {
                return self.paid(pointer, &uri, Present::Site, get).await;
            }
            // Another bundle's pages run on their own origin; a blob has none.
            return match uri.target {
                UriTarget::Blob => self.paid(pointer, &uri, Present::Download, get).await,
                _ => self.to_site(&uri, named),
            };
        }
        // The query belongs to the page's scripts: the gateway reads none of
        // it, and a directory's redirect keeps it. A directory serves its
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
        self.paid(pointer, &uri, Present::Site, Ask::Get { range, query })
            .await
    }

    /// A `POST` on a loopback site origin: the interstitial's Continue
    /// (`consent`). From the site's own page with its token, it records the
    /// user's consent and sends the browser back to the page it asked for.
    async fn consent<B>(&self, req: Request<B>, site: Site) -> Response<Body>
    where
        B: HttpBody<Data = Bytes>,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let host = self.sites.site_host(&site);
        let origin = self.sites.origin(&host);
        if !consent::from_own_page(req.headers(), &origin) {
            return text(
                StatusCode::FORBIDDEN,
                "only the site's own interstitial may continue to it\n",
            );
        }
        // The origin prefix keeps a target such as `//elsewhere` on the site.
        let back = format!(
            "{origin}{}",
            req.uri().path_and_query().map_or("/", |pq| pq.as_str())
        );
        let form = match Limited::new(req.into_body(), MAX_BODY_BYTES)
            .collect()
            .await
        {
            Ok(body) => body.to_bytes(),
            Err(_) => return text(StatusCode::PAYLOAD_TOO_LARGE, "body too large\n"),
        };
        if !self.consents.give(site, &host, &form) {
            return text(StatusCode::FORBIDDEN, "the interstitial's token is wrong\n");
        }
        let mut response = text(StatusCode::SEE_OTHER, "");
        match header::HeaderValue::from_str(&back) {
            Ok(location) => {
                response.headers_mut().insert(header::LOCATION, location);
            }
            Err(_) => *response.status_mut() = StatusCode::BAD_REQUEST,
        }
        response
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
        let domain = pointer_domain(self.chain.governance.chain_id, public.registry);
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
    /// A request that `named` a directory, whose `index.html` `uri` names,
    /// goes to the directory itself.
    fn to_site(&self, uri: &DecdnUri, named: Named) -> Response<Body> {
        let site = Site {
            hash: uri.hash,
            namespace: uri.namespace,
        };
        let path = match &uri.target {
            UriTarget::Bundle => String::new(),
            UriTarget::BundleEntry(path) => {
                let link = entry_link(uri.hash, path);
                match link.strip_suffix("index.html") {
                    Some(dir) if named == Named::Directory => dir.to_owned(),
                    _ => link,
                }
            }
            _ => return text(StatusCode::BAD_REQUEST, "a blob has no site\n"),
        };
        redirect(&format!(
            "{}/{path}",
            self.sites.origin(&self.sites.site_host(&site))
        ))
    }

    /// The delegate keys, with every capability expired by now dropped. No
    /// caller holds the lock across an await.
    fn delegates(&self) -> std::sync::MutexGuard<'_, Delegates> {
        let mut delegates = self
            .delegates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        delegates.prune(now_secs());
        delegates
    }

    /// `GET /_delegate`: the pending key's address.
    fn pending(&self) -> Response<Body> {
        let address = self.delegates().pending();
        json(
            StatusCode::OK,
            &serde_json::json!({ "address": address.to_string() }),
        )
    }

    /// `PUT /_capability`: register a capability to a key the gateway holds.
    /// Every check that needs no chain read runs first, so a capability that
    /// cannot register costs the operator's RPC nothing. A key that already
    /// holds a capability from its pool id on every configured contract is
    /// refused here; otherwise the read names the pool's contract, and the
    /// registration checks the key on that contract.
    async fn register<B>(&self, req: Request<B>) -> Response<Body>
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
        let signed_scope = match request.scope {
            Scope::Any => None,
            Scope::Namespaces(scope) => Some(scope),
        };
        if signed_scope
            .as_ref()
            .is_some_and(|scope| scope.namespaces.is_empty())
        {
            return text(
                StatusCode::UNPROCESSABLE_ENTITY,
                "a scope names at least one namespace\n",
            );
        }
        let grant = match CapabilityGrant::from_token(request.capability.trim()) {
            Ok(grant) => grant,
            Err(e) => return text(StatusCode::UNPROCESSABLE_ENTITY, &format!("{e}\n")),
        };
        if grant.spending_cap == 0 {
            return text(
                StatusCode::UNPROCESSABLE_ENTITY,
                "the capability's cap is zero\n",
            );
        }
        if inside_capability_expiry_margin(grant.expiry, EXPIRY_MARGIN_SECS, now_secs()) {
            return text(
                StatusCode::UNPROCESSABLE_ENTITY,
                &format!(
                    "the capability expires within {EXPIRY_MARGIN_SECS} s, inside the margin \
                     nodes stop accepting it\n"
                ),
            );
        }
        let contracts: Vec<Deployment> = self
            .chain
            .payment_pools
            .iter()
            .map(|pool| pool.deployment)
            .collect();
        if let Err(refusal) = self.delegates().admits(&grant, &contracts) {
            return text(StatusCode::CONFLICT, refusal.text());
        }
        let pool = match self.verify_owner(&grant, signed_scope.as_ref()).await {
            Ok(pool) => pool,
            Err((status, e)) => return text(status, &e),
        };
        let namespaces = signed_scope.map(|scope| scope.namespaces);
        let mut response = serde_json::json!({
            "expiry": grant.expiry,
            "pool": grant.pool_id.to_string(),
            "signer": grant.signer.to_string(),
        });
        if let (Some(namespaces), Some(fields)) = (&namespaces, response.as_object_mut()) {
            fields.insert("namespaces".into(), serde_json::json!(namespaces));
        }
        let (signer, pool_id) = (grant.signer, grant.pool_id);
        if let Err(refusal) = self.delegates().register(grant, pool, namespaces) {
            return text(StatusCode::CONFLICT, refusal.text());
        }
        tracing::info!(
            %signer,
            pool = %pool_id,
            chain_id = pool.chain_id,
            payment_pool = %pool.payment_pool,
            "a capability was registered"
        );
        json(StatusCode::CREATED, &response)
    }

    /// The configured `PaymentPool` contract whose on-chain owner of
    /// `grant`'s pool signed it, and signed its `scope` when it has one
    /// ([`capability_pool`]), or the status and text to answer with: 422 for
    /// a malformed signature, a scope no candidate pool's owner signed, or no
    /// matching pool; 502 when no pool matches and a read failed. The scope
    /// is checked against each pool's domains before any chain read. Whether
    /// the pool can pay shows only when a pull draws on it: a funding stop
    /// retires the capability.
    async fn verify_owner(
        &self,
        grant: &CapabilityGrant,
        scope: Option<&SignedScope>,
    ) -> Result<Deployment, (StatusCode, String)> {
        let unprocessable = |e: String| (StatusCode::UNPROCESSABLE_ENTITY, e);
        let candidates = match scope {
            None => self.chain.payment_pools.clone(),
            Some(scope) => {
                let mut agree = Vec::new();
                for pool in &self.chain.payment_pools {
                    let deployment = pool.deployment;
                    let signed_by = grant
                        .owner(&deployment.voucher_domain())
                        .map_err(|e| unprocessable(format!("{e}\n")))?;
                    let domain = scope_domain(deployment.chain_id, deployment.payment_pool);
                    let scoped_by =
                        scope
                            .owner(grant.signer, grant.pool_id, &domain)
                            .map_err(|_| {
                                unprocessable("the scope signature is malformed\n".to_owned())
                            })?;
                    if scoped_by == signed_by {
                        agree.push(pool.clone());
                    }
                }
                if agree.is_empty() {
                    return Err(unprocessable(
                        "the scope is not signed by the capability's pool owner\n".to_owned(),
                    ));
                }
                agree
            }
        };
        capability_pool(grant, &candidates, |pool, pool_id| {
            let pool = pool.clone();
            async move {
                read_pool_owner(&pool, pool_id)
                    .await
                    .map_err(|e| sanitize_err_chain(&e))
            }
        })
        .await
        .map_err(|e| match e {
            CapabilityPoolError::Malformed(text) | CapabilityPoolError::NoMatch(text) => {
                unprocessable(text)
            }
            CapabilityPoolError::Unread(text) => (StatusCode::BAD_GATEWAY, text),
        })
    }

    /// `GET /_capability`: every capability not yet expired, in the order
    /// the gateway pays with them.
    fn list(&self) -> Response<Body> {
        json(StatusCode::OK, &self.capabilities())
    }

    /// Every capability registered and not yet expired, in registration
    /// order.
    fn capabilities(&self) -> Vec<Capability> {
        self.delegates().list()
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
        let rpc = decdn_client::provider::build_read_provider(&self.chain.governance.rpc_url)
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

    /// The capability the next pull pays with, for a request of `namespace`
    /// (as [`Self::paid`] selects it) or, with `None`, for any other
    /// request: the first one the gateway holds that can pay
    /// ([`Delegates::paying`]), or `None`.
    fn paying(&self, namespace: Option<u64>) -> Option<Credential> {
        self.delegates()
            .paying(now_secs(), EXPIRY_MARGIN_SECS, namespace)
    }

    /// Retire the capability `credential` pays with after a funding stop. It
    /// stays listed, as spent, until it expires.
    fn mark_spent(&self, credential: &Credential) {
        self.delegates().retire(
            credential.signer.address(),
            credential.pool,
            credential.grant.pool_id,
        );
    }

    /// Content on the bare host. A bundle or an entry opens as its site for
    /// a browser's navigation, and on a public gateway for every request; a
    /// loopback gateway serves anything else (a client outside a browser) at
    /// the bare host. A blob streams from the bare host as bytes.
    async fn content(self: &Arc<Self>, headers: &HeaderMap, rest: &str) -> Response<Body> {
        let uri: DecdnUri = match format!("decdn:{rest}").parse() {
            Ok(uri) => uri,
            Err(e) => return text(StatusCode::BAD_REQUEST, &format!("{e}\n")),
        };
        let navigation = headers
            .get("sec-fetch-mode")
            .is_some_and(|mode| mode == "navigate");
        if matches!(uri.target, UriTarget::Bundle | UriTarget::BundleEntry(_))
            && (navigation || self.public())
        {
            return self.to_site(&uri, Named::AsIs);
        }
        let get = Ask::Get {
            range: range_header(headers),
            query: None,
        };
        self.paid(None, &uri, Present::Bare, get).await
    }

    /// Serve `uri` as `present` says, paid by [`Self::paying`]. On a public
    /// gateway, a capability scoped to a namespace pays only for a request
    /// the pointer of namespace `pointer` serves (`None` for any other
    /// request). A loopback gateway answers only the user's own clients, so
    /// there the namespace `uri` names (`?ns=`, or a site's label) selects
    /// it.
    async fn paid(
        self: &Arc<Self>,
        pointer: Option<u64>,
        uri: &DecdnUri,
        present: Present,
        ask: Ask<'_>,
    ) -> Response<Body> {
        let namespace = if self.public() {
            pointer
        } else {
            uri.namespace
        };
        // A funding stop retires the capability and the next one takes the
        // request, so a capability that cannot pay costs a retry, never a
        // failed load. Each pass retires one, so the loop ends. With nothing
        // left that can pay, a response the cache holds whole is still served.
        loop {
            let Some(paying) = self.paying(namespace) else {
                if let Some(response) = self.cached_only(uri, present, ask).await {
                    return response;
                }
                return self.nothing_pays(uri);
            };
            match self.serve(uri, &paying, present, ask).await {
                Ok(response) => return response,
                Err(PullError::Funding(e)) => {
                    tracing::info!(
                        error = %e,
                        signer = %paying.signer.address(),
                        pool = %paying.grant.pool_id,
                        "a capability ran out"
                    );
                    self.mark_spent(&paying);
                }
                Err(e) => return pull_failed(e, uri, self.public()),
            }
        }
    }

    /// The answer for `uri` when nothing the gateway holds can pay: a
    /// loopback gateway's user registers a capability; a public gateway's
    /// client retries, or fetches the URI with a paying client of its own.
    fn nothing_pays(&self, uri: &DecdnUri) -> Response<Body> {
        if self.public() {
            tracing::warn!(%uri, "nothing the gateway holds can pay");
            return cannot_pay(uri);
        }
        text(
            StatusCode::PAYMENT_REQUIRED,
            "the gateway holds no capability that can pay: register one with PUT /_capability\n",
        )
    }

    /// `uri` answered out of the cache alone, at no pull: a blob, a bundle
    /// entry, or a site's `index.html`, whose response bytes the cache holds
    /// ([`Self::cache_hit`]), at a size the gateway knows with no pull. The
    /// manifest comes from what the gateway already holds
    /// ([`Self::known_manifest`]). An empty blob and an unsatisfiable range
    /// are answered too, as they cost nothing. `None` for anything else: a
    /// `HEAD`, a listing, a manifest or a byte the cache lacks.
    async fn cached_only(
        self: &Arc<Self>,
        uri: &DecdnUri,
        present: Present,
        ask: Ask<'_>,
    ) -> Option<Response<Body>> {
        let Ask::Get { range, .. } = ask else {
            return None;
        };
        let manifest;
        let (hash, path, size) = if matches!(uri.target, UriTarget::Blob) {
            (uri.hash, None, Some(self.blob_size(uri.hash).await?))
        } else {
            let wanted = match &uri.target {
                UriTarget::BundleEntry(path) => path.as_str(),
                UriTarget::Bundle if present == Present::Site => "index.html",
                _ => return None,
            };
            let cap_mb = manifest_cap_mb(self.common.max_blob_mb);
            manifest = self.known_manifest(uri.hash, cap_mb).await?.ok()?;
            let entry = manifest.entries.iter().find(|e| e.path == wanted)?;
            (entry.hash, Some(entry.path.as_str()), entry.size)
        };
        let name = served_name(hash, path);
        let extent = match Extent::of(size, range, rewrites(present, path.unwrap_or(&name))) {
            Ok(extent) => extent,
            Err(size) => return Some(not_satisfiable(size)),
        };
        if size == Some(0) {
            return Some(empty_entry(&name, hash, present));
        }
        let engine = self.cache_hit(hash, &extent).await?;
        self.from_cache(engine, hash, path, &extent, present).ok()
    }

    /// Serve what `uri` names, as `present` says, paid by `paying`, to
    /// `ask`: a `GET` and its `Range` header, honoured on a blob or entry of
    /// known size, or a blob's `HEAD`.
    async fn serve(
        self: &Arc<Self>,
        uri: &DecdnUri,
        paying: &Credential,
        present: Present,
        ask: Ask<'_>,
    ) -> Result<Response<Body>, PullError> {
        let range = match ask {
            Ask::Get { range, .. } => range,
            Ask::Head => None,
        };
        match &uri.target {
            UriTarget::Blob => self.blob(uri, paying, present, ask).await,
            UriTarget::BundleEntry(path) => {
                self.entry(uri.hash, uri.namespace, path, paying, present, ask)
                    .await
            }
            UriTarget::Bundle => {
                self.index(uri.hash, uri.namespace, paying, present, range)
                    .await
            }
            _ => Ok(text(StatusCode::BAD_REQUEST, "unsupported URI target\n")),
        }
    }

    /// A blob by its hash. Its verified size ([`Self::blob_size`], else
    /// [`Self::learn_size`]) gives the response its length, caps the pull and
    /// lets one `Range` through, as an entry's declared size does. A response
    /// whose pull
    /// (the whole blob, or the span of its `Range`) passes `--max-blob-mb`
    /// answers 413 before any pull. A blob the gateway
    /// rewrites streams whole, with no length. A `HEAD` answers with the
    /// headers once the size is known.
    async fn blob(
        self: &Arc<Self>,
        uri: &DecdnUri,
        paying: &Credential,
        present: Present,
        ask: Ask<'_>,
    ) -> Result<Response<Body>, PullError> {
        let name = alloy::hex::encode(uri.hash);
        let rewritten = rewrites(present, &name);
        let mut size = self.blob_size(uri.hash).await;
        if size.is_none() && !rewritten {
            size = self.learn_size(uri, paying).await?;
        }
        let range = match ask {
            Ask::Get { range, .. } => range,
            Ask::Head => {
                return Ok(head_response(
                    &blob_type(present, &name),
                    size,
                    uri.hash,
                    present,
                ));
            }
        };
        let extent = match Extent::of(size, range, rewritten) {
            Ok(extent) => extent,
            Err(size) => return Ok(not_satisfiable(size)),
        };
        if size != Some(0)
            && let Some(engine) = self.cache_hit(uri.hash, &extent).await
        {
            return self.from_cache(engine, uri.hash, None, &extent, present);
        }
        if let Some(refused) = self.over_blob_cap(extent.billable()) {
            return Ok(refused);
        }
        if size == Some(0) {
            return Ok(empty_entry(&name, uri.hash, present));
        }
        self.stream(uri.hash, uri.namespace, paying, None, extent, present)
            .await
    }

    /// The verified size of the blob `hash` that a fetch here learned
    /// ([`PullWorker::size`]), or that of a blob the cache holds whole
    /// ([`Self::cached_size`]); the empty root's is zero. Every size a node
    /// states arrives with its size proof, which the fetch verified (ADR 005 §
    /// Bounded byte ranges), so the server can stop reading a body at
    /// its length.
    async fn blob_size(&self, hash: [u8; 32]) -> Option<u64> {
        if hash == decdn_protocol::EMPTY_ROOT {
            return Some(0);
        }
        if let Some(size) = self.pulls.size(hash).await {
            return Some(size);
        }
        self.cached_size(hash).await
    }

    /// The size of `hash` when the cache holds the whole blob: its last chunk
    /// verified, so the size is the blob's own.
    async fn cached_size(&self, hash: [u8; 32]) -> Option<u64> {
        let present = self
            .cache
            .as_ref()?
            .present_ranges(decdn_cache::Hash::from_bytes(hash))
            .await
            .ok()?;
        present.is_complete().then(|| present.size())
    }

    /// Learn the verified size of the blob `uri` names before its response,
    /// on `paying`'s lanes: the worker runs a fetch that resolves the holders
    /// with discovery opens that name `paying`'s pool, takes the size the
    /// winning holder's answer proves, cancels every open, and writes no
    /// byte. The size joins the verified sizes ([`PullWorker::size`]). When
    /// no answer proves the size, it opens the blob's first byte, which pays
    /// for one chunk group at most.
    ///
    /// # Errors
    ///
    /// What ends the request as it ends a pull: a funding stop, or a fault
    /// before any holder states a size (such as no holder).
    async fn learn_size(
        self: &Arc<Self>,
        uri: &DecdnUri,
        paying: &Credential,
    ) -> Result<Option<u64>, PullError> {
        // The lookup writes nothing; the reader is held to its end, so the
        // pipe never reads as closed.
        let (writer, _reader) = tokio::io::duplex(1);
        let (done, ended) = oneshot::channel();
        self.pulls
            .pull(Job {
                blob: BlobFetch {
                    estimate: false,
                    progress: None,
                    hash: uri.hash,
                    output: PathBuf::from("-"),
                    namespace: uri.namespace,
                    common: self.common.clone(),
                    read_ahead: None,
                    size_cap: None,
                    range: None,
                    size_only: true,
                },
                paying: paying.clone(),
                writer,
                done,
                // A size lookup is not a pull in flight.
                listing: Box::new(()),
            })
            .map_err(PullError::Other)?;
        match ended.await {
            Ok(Ok(())) => Ok(self.pulls.size(uri.hash).await),
            Ok(Err(e)) => Err(classify(e)),
            Err(_) => Err(PullError::Other(anyhow::anyhow!(
                "the size lookup ended without a result"
            ))),
        }
    }

    /// One bundle entry, by its literal manifest path. A path the manifest
    /// lacks answers 404, and so does a blob that is not a manifest or a
    /// malformed one ([`Self::manifest`]). On a site, a path that names a
    /// directory with an `index.html` redirects to the directory, with the
    /// request's query kept ([`to_directory`]). An entry
    /// whose pull, by its declared size, passes `--max-blob-mb` answers 413
    /// before any pull, as a raw blob does ([`Self::over_blob_cap`]).
    async fn entry(
        self: &Arc<Self>,
        manifest_hash: [u8; 32],
        namespace: Option<u64>,
        path: &str,
        paying: &Credential,
        present: Present,
        ask: Ask<'_>,
    ) -> Result<Response<Body>, PullError> {
        let (range, query) = match ask {
            Ask::Get { range, query } => (range, query),
            Ask::Head => (None, None),
        };
        let manifest = self.manifest(manifest_hash, namespace, paying).await?;
        if let Some(entry) = manifest.entries.iter().find(|e| e.path == path) {
            let extent = match Extent::of(entry.size, range, rewrites(present, path)) {
                Ok(extent) => extent,
                Err(size) => return Ok(not_satisfiable(size)),
            };
            if entry.size != Some(0)
                && let Some(engine) = self.cache_hit(entry.hash, &extent).await
            {
                return self.from_cache(engine, entry.hash, Some(path), &extent, present);
            }
            if let Some(refused) = self.over_blob_cap(extent.billable()) {
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
            return Ok(to_directory(manifest_hash, path, query));
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
        paying: &Credential,
        present: Present,
        range: Option<&str>,
    ) -> Result<Response<Body>, PullError> {
        let manifest = self.manifest(manifest_hash, namespace, paying).await?;
        if present == Present::Site
            && let Some(entry) = manifest.entries.iter().find(|e| e.path == "index.html")
        {
            let extent = match Extent::of(entry.size, range, rewrites(present, &entry.path)) {
                Ok(extent) => extent,
                Err(size) => return Ok(not_satisfiable(size)),
            };
            if entry.size != Some(0)
                && let Some(engine) = self.cache_hit(entry.hash, &extent).await
            {
                return self.from_cache(engine, entry.hash, Some(&entry.path), &extent, present);
            }
            if let Some(refused) = self.over_blob_cap(extent.billable()) {
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
        let uri = DecdnUri::new(manifest_hash, UriTarget::Bundle, namespace);
        let mut page = String::new();
        let _ = write_listing(
            &mut page,
            &manifest,
            manifest_hash,
            namespace,
            present,
            &uri,
        );
        Ok(html(StatusCode::OK, &page))
    }

    /// What the gateway knows of `hash` as a manifest: the parsed manifest,
    /// or why it is none.
    /// What the gateway knows of `hash` as a manifest with no pull: a parsed
    /// manifest or a verdict it kept ([`Self::cached_manifest`]), else the
    /// blob the cache holds whole ([`Self::manifest_from_cache`]).
    async fn known_manifest(
        &self,
        hash: [u8; 32],
        cap_mb: u64,
    ) -> Option<Result<Arc<BundleManifest>, NotManifest>> {
        match self.cached_manifest(&hash) {
            Some(known) => Some(known),
            None => self.manifest_from_cache(hash, cap_mb).await,
        }
    }

    /// The manifest `hash` read out of the cache, when the cache holds the
    /// blob whole and it fits the manifest cap `cap_mb`: parsed with no pull,
    /// and kept as a pulled manifest or verdict is. A blob past the cap is
    /// left to a pull, whose cap stop names the raw blob.
    async fn manifest_from_cache(
        &self,
        hash: [u8; 32],
        cap_mb: u64,
    ) -> Option<Result<Arc<BundleManifest>, NotManifest>> {
        let engine = self.cache.as_ref()?;
        let size = self.cached_size(hash).await?;
        if size == 0 || size > cap_mb.saturating_mul(1024 * 1024) {
            return None;
        }
        let bytes = engine
            .export_range(decdn_cache::Hash::from_bytes(hash), 0, size)
            .await
            .ok()?;
        let verdict = BundleManifest::parse(&bytes)
            .map(Arc::new)
            .map_err(NotManifest::unparsed);
        match &verdict {
            Ok(manifest) => self
                .manifests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(hash, Arc::clone(manifest), bytes.len()),
            Err(not) => self
                .not_manifests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(hash, not.clone()),
        }
        Some(verdict)
    }

    fn cached_manifest(&self, hash: &[u8; 32]) -> Option<Result<Arc<BundleManifest>, NotManifest>> {
        let manifest = self
            .manifests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(hash);
        if let Some(manifest) = manifest {
            return Some(Ok(manifest));
        }
        self.not_manifests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(hash)
            .map(Err)
    }

    /// The bundle manifest `hash`, from the cache or pulled. The pull requests,
    /// and so pays for, at most the manifest cap. A blob that is no manifest
    /// is remembered as such (up to [`MAX_NOT_MANIFESTS`] hashes), so a repeat
    /// request answers without a pull. A pull that ends for any other reason
    /// is not, and a later request pulls again. On a public gateway, the
    /// requests that waited on a fill which failed upstream share its failure
    /// ([`Flight`]).
    async fn manifest(
        self: &Arc<Self>,
        hash: [u8; 32],
        namespace: Option<u64>,
        paying: &Credential,
    ) -> Result<Arc<BundleManifest>, PullError> {
        let cap_mb = manifest_cap_mb(self.common.max_blob_mb);
        let raw = raw_blob_hint(hash, namespace);
        // The raw blob pulls under the gateway's own cap, so it is a way past
        // the manifest cap only when that cap is higher.
        let raw_past_cap = self.common.max_blob_mb == 0 || cap_mb < self.common.max_blob_mb;
        let not_manifest = |verdict: &NotManifest| {
            PullError::NotManifest(verdict.error(cap_mb, &raw, raw_past_cap))
        };
        if let Some(known) = self.known_manifest(hash, cap_mb).await {
            return known.map_err(|verdict| not_manifest(&verdict));
        }
        let flight = self.manifest_flight((hash, namespace));
        let seen = flight.ended.load(Ordering::Relaxed);
        let mut failed = flight.failed.lock().await;
        // A request that waited on the fill lock finds what the one before it
        // pulled.
        if let Some(known) = self.known_manifest(hash, cap_mb).await {
            return known.map_err(|verdict| not_manifest(&verdict));
        }
        if flight.ended.load(Ordering::Relaxed) > seen
            && let Some(cause) = failed.as_deref()
        {
            return Err(PullError::Other(anyhow::anyhow!("{cause}")));
        }
        let pulled = self.pull_manifest(hash, namespace, paying, cap_mb).await;
        *failed = match &pulled {
            Err(ManifestStop::Pull(PullError::Other(cause))) if self.public() => {
                Some(format!("{cause:#}"))
            }
            _ => None,
        };
        flight.ended.fetch_add(1, Ordering::Relaxed);
        match pulled {
            Ok((manifest, size)) => {
                self.manifests
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(hash, Arc::clone(&manifest), size);
                Ok(manifest)
            }
            Err(ManifestStop::Not(verdict)) => {
                let error = not_manifest(&verdict);
                self.not_manifests
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(hash, verdict);
                Err(error)
            }
            Err(ManifestStop::Pull(e)) => Err(e),
        }
    }

    /// The fill lock that serializes fills of the manifest `key`
    /// ([`Gateway::manifest_fills`]). The caller locks it, re-checks the
    /// cache, and only then pulls.
    fn manifest_flight(&self, key: FlightKey) -> Arc<Flight> {
        self.manifest_fills.get(key)
    }

    /// `n` bytes of the manifest buffer budget ([`MANIFEST_BUFFER_BYTES`]),
    /// or an error at once when the budget is spent.
    fn manifest_buffer_bytes(&self, n: usize) -> Result<SemaphorePermit<'_>, PullError> {
        u32::try_from(n)
            .ok()
            .and_then(|n| self.manifest_buffer.try_acquire_many(n).ok())
            .ok_or_else(|| {
                PullError::Other(anyhow::anyhow!(
                    "the gateway's manifest buffer is full; retry the request"
                ))
            })
    }

    /// Pull `hash` under the manifest cap of `cap_mb` and parse it. Returns
    /// the manifest and its JSON length, the weight the manifest cache counts.
    async fn pull_manifest(
        self: &Arc<Self>,
        hash: [u8; 32],
        namespace: Option<u64>,
        paying: &Credential,
        cap_mb: u64,
    ) -> Result<(Arc<BundleManifest>, usize), ManifestStop> {
        let (first, mut reader, done, _delivered) = self
            .start_pull(hash, namespace, paying, cap_mb, None, None)
            .await
            .map_err(|e| over_manifest_cap(e.into()))?;
        // The buffer's capacity, held against the budget until the fill ends.
        let mut held = self.manifest_buffer_bytes(first.len())?;
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
                // A backstop: the manifest cap ends the pull before this.
                let len = bytes.len().saturating_add(n);
                if len > MAX_MANIFEST_BYTES {
                    return Err(ManifestStop::Not(NotManifest::Plain(format!(
                        "the blob is larger than {MAX_MANIFEST_BYTES} bytes, the most this \
                         gateway reads as a bundle manifest"
                    ))));
                }
                // Grown by hand so the buffer's capacity stays within the cap.
                if len > bytes.capacity() {
                    let grown = bytes
                        .capacity()
                        .saturating_mul(2)
                        .max(len)
                        .min(MAX_MANIFEST_BYTES);
                    held.merge(self.manifest_buffer_bytes(grown.saturating_sub(bytes.capacity()))?);
                    bytes.reserve_exact(grown.saturating_sub(bytes.len()));
                }
                bytes.extend_from_slice(buf.get(..n).unwrap_or_default());
            }
            finished(done).await.map_err(ManifestStop::from)
        }
        .await;
        read.map_err(over_manifest_cap)?;
        // The blob fits under the manifest cap, so it fits under the gateway's.
        let manifest = BundleManifest::parse(&bytes)
            .map_err(|e| ManifestStop::Not(NotManifest::unparsed(e)))?;
        Ok((Arc::new(manifest), bytes.len()))
    }

    /// A response streaming `hash`'s verified bytes, presented as `present`
    /// says. Errors that come before the first byte become the error status;
    /// a later one aborts the body.
    async fn stream(
        self: &Arc<Self>,
        hash: [u8; 32],
        namespace: Option<u64>,
        paying: &Credential,
        path: Option<&str>,
        extent: Extent,
        present: Present,
    ) -> Result<Response<Body>, PullError> {
        let name = served_name(hash, path);
        let (content_type, rewrite) = presentation(present, &name);
        // The pull holds no more than the size the manifest declares or a
        // pull verified, and takes only the window a range asked for.
        let (first, reader, done, delivered_at) = self
            .start_pull(
                hash,
                namespace,
                paying,
                self.common.max_blob_mb,
                extent.size,
                extent.window,
            )
            .await?;
        let state = Streaming {
            first: Some(first),
            reader,
            done: Some(done),
            rewrite,
            delivered: 0,
            delivered_at,
            gateway: Arc::clone(self),
        };
        let held = Arc::new(paying.clone());
        let body = futures_util::stream::unfold(state, move |s| next_frame(s, Arc::clone(&held)));
        streamed_response(&name, &content_type, present, &extent, body)
    }

    /// The cache entry of `hash` when it holds every byte `extent` streams,
    /// at the size `extent` names; `None` with no cache, or an extent of
    /// unknown size. The entry's size is the size its fill opened with, which
    /// a holder proved, so a declared size that differs is no hit.
    async fn cache_hit(&self, hash: [u8; 32], extent: &Extent) -> Option<CacheEngine> {
        let engine = self.cache.as_ref()?;
        let size = extent.size?;
        let present = engine
            .present_ranges(decdn_cache::Hash::from_bytes(hash))
            .await
            .ok()?;
        if present.size() != size {
            return None;
        }
        let (offset, len) = extent.window.unwrap_or((0, size));
        let end = offset.saturating_add(len);
        let needed = bao_tree::ChunkRanges::from(
            bao_tree::ChunkNum(offset / BAO_CHUNK_BYTES)
                ..bao_tree::ChunkNum(end.div_ceil(BAO_CHUNK_BYTES)),
        );
        needed
            .is_subset(present.chunk_ranges())
            .then(|| engine.clone())
    }

    /// A response streaming `hash`'s bytes straight out of `engine`, which
    /// holds every one of them ([`Self::cache_hit`]): no pull. A read that
    /// fails mid-body (the entry evicted under it) ends the body with an
    /// error.
    fn from_cache(
        self: &Arc<Self>,
        engine: CacheEngine,
        hash: [u8; 32],
        path: Option<&str>,
        extent: &Extent,
        present: Present,
    ) -> Result<Response<Body>, PullError> {
        let name = served_name(hash, path);
        let (content_type, rewrite) = presentation(present, &name);
        let size = extent.size.unwrap_or_default();
        let (offset, len) = extent.window.unwrap_or((0, size));
        self.metrics.cache_hit();
        let state = CachedBody {
            chunks: read_cached(
                engine,
                decdn_cache::Hash::from_bytes(hash),
                offset,
                offset.saturating_add(len),
            ),
            rewrite,
            gateway: Arc::clone(self),
            ended: false,
        };
        let body = futures_util::stream::unfold(state, next_cached_frame);
        streamed_response(&name, &content_type, present, extent, body)
    }

    /// `HEAD` on the public bare host, or under a site's `/_decdn/`: a blob's
    /// headers, with its length when a pull here verified it
    /// ([`Self::blob_size`]), at no pull; a blob of unknown size learns its
    /// size as its `GET` would ([`Self::learn_size`]), paid as [`Self::paid`]
    /// pays. A bundle or an entry redirects to its site, at no pull, where
    /// `GET` under `/_decdn/` would serve a page's request for it inline; a
    /// request that `named` a directory (`rest` names its `index.html`)
    /// redirects to the directory on the site.
    ///
    /// `scoped_bundle` is a pointer's namespace and the bundle it names, on
    /// that namespace's host: a capability scoped to the namespace pays for
    /// the size lookup of that bundle's own hash.
    async fn head_blob(
        self: &Arc<Self>,
        rest: &str,
        present: Present,
        scoped_bundle: Option<(u64, [u8; 32])>,
        named: Named,
    ) -> Response<Body> {
        let uri: DecdnUri = match format!("decdn:{rest}").parse() {
            Ok(uri) => uri,
            Err(e) => return text(StatusCode::BAD_REQUEST, &format!("{e}\n")),
        };
        let namespace = scoped_bundle
            .filter(|(_, bundle)| *bundle == uri.hash)
            .map(|(ns, _)| ns);
        match uri.target {
            UriTarget::Blob => match self.blob_size(uri.hash).await {
                Some(size) => {
                    let name = alloy::hex::encode(uri.hash);
                    head_response(&blob_type(present, &name), Some(size), uri.hash, present)
                }
                None => self.paid(namespace, &uri, present, Ask::Head).await,
            },
            UriTarget::Bundle | UriTarget::BundleEntry(_) => self.to_site(&uri, named),
            _ => text(StatusCode::BAD_REQUEST, "unsupported URI target\n"),
        }
    }

    /// `HEAD` on a public site: the entry's headers from the bundle manifest,
    /// which the gateway pulls once and caches, so the request costs no pull
    /// of the entry. A directory answers for its `index.html`, and the root
    /// for the bundle's; a response the gateway would rewrite has no length.
    /// A path that names a directory redirects to it with `query` kept, as
    /// `GET` does. `pointer` is the namespace whose pointer named the site,
    /// if any.
    async fn head_site(
        self: &Arc<Self>,
        site: &Site,
        path: &str,
        query: Option<&str>,
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
        // The manifest is a paid pull, unless the gateway holds it. As in
        // `paid`: a funding stop retires the capability and the next one
        // takes the pull.
        let cap_mb = manifest_cap_mb(self.common.max_blob_mb);
        let manifest = loop {
            let Some(paying) = self.paying(pointer) else {
                if let Some(Ok(manifest)) = self.known_manifest(site.hash, cap_mb).await {
                    break manifest;
                }
                return self.nothing_pays(&uri);
            };
            match self.manifest(site.hash, site.namespace, &paying).await {
                Ok(manifest) => break manifest,
                Err(PullError::Funding(_)) => self.mark_spent(&paying),
                Err(e) => return pull_failed(e, &uri, true),
            }
        };
        let path = match &uri.target {
            // The root is the bundle's `index.html`, else its listing: a page
            // with no fixed length.
            UriTarget::Bundle => {
                if manifest.entries.iter().any(|e| e.path == "index.html") {
                    "index.html"
                } else {
                    return head_response(
                        "text/html; charset=utf-8",
                        None,
                        site.hash,
                        Present::Site,
                    );
                }
            }
            UriTarget::BundleEntry(path) => path.as_str(),
            _ => return text(StatusCode::BAD_REQUEST, "unsupported URI target\n"),
        };
        let Some(entry) = manifest.entries.iter().find(|e| e.path == path) else {
            let index = format!("{path}/index.html");
            if manifest.entries.iter().any(|e| e.path == index) {
                return to_directory(site.hash, path, query);
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
        head_response(&content_type, length, entry.hash, Present::Site)
    }

    /// Hand the pull of `hash` to the worker, capped at `max_blob_mb` and at
    /// `size_cap` bytes when the blob's size is known, over the byte window
    /// `range` or the whole blob, and wait for its first verified bytes. The
    /// pull writes into a pipe the response reads from. It is listed as in
    /// flight until the worker's pull ends; the last value returned is its
    /// listing's count of delivered bytes.
    async fn start_pull(
        self: &Arc<Self>,
        hash: [u8; 32],
        namespace: Option<u64>,
        paying: &Credential,
        max_blob_mb: u64,
        size_cap: Option<u64>,
        range: Option<(u64, u64)>,
    ) -> Result<
        (
            Bytes,
            DuplexStream,
            oneshot::Receiver<anyhow::Result<()>>,
            Arc<AtomicU64>,
        ),
        PullError,
    > {
        let (writer, mut reader) = tokio::io::duplex(PIPE_BYTES);
        let (done_tx, done_rx) = oneshot::channel();
        let mut common = self.common.clone();
        common.max_blob_mb = max_blob_mb;
        // Listed before the worker has the job, so a pull waiting for its
        // slot shows as in flight.
        let guard = PullGuard::new(Arc::clone(self), paying, hash, namespace, range);
        let delivered = Arc::clone(&guard.delivered);
        self.pulls
            .pull(Job {
                blob: BlobFetch {
                    estimate: false,
                    progress: None,
                    hash,
                    output: PathBuf::from("-"),
                    namespace,
                    common,
                    read_ahead: self.public().then_some(PUBLIC_READ_AHEAD_BYTES),
                    size_cap,
                    range,
                    size_only: false,
                },
                paying: paying.clone(),
                writer,
                done: done_tx,
                listing: Box::new(guard),
            })
            .map_err(PullError::Other)?;
        let (first, done_rx) = first_chunk(&mut reader, done_rx).await?;
        Ok((first, reader, done_rx, delivered))
    }
}

/// The next frame of a streaming body, or `None` at its end: the pull's first
/// bytes, then each chunk off the pipe, passed through the rewrite. A pull
/// error after the first byte ends the body with an error, so a browser never
/// takes a verified prefix for the whole file; `held` is the capability to
/// retire on a funding stop.
async fn next_frame(
    mut s: Streaming,
    held: Arc<Credential>,
) -> Option<(Result<Frame<Bytes>, std::io::Error>, Streaming)> {
    let gateway = Arc::clone(&s.gateway);
    let chunk = if let Some(first) = s.first.take() {
        first
    } else {
        let mut buf = vec![0u8; CHUNK_BYTES];
        match s.reader.read(&mut buf).await {
            Ok(0) => {
                if let Err(e) = finished(s.done.take()?).await {
                    if let PullError::Funding(_) = &e {
                        gateway.mark_spent(&held);
                    }
                    s.rewrite = None;
                    let e = std::io::Error::other(pull_error_text(&e));
                    return Some((Err(e), s));
                }
                // The pull is whole: the rewrite lets out its tail.
                let tail = s.rewrite.take()?.finish().map(Frame::data);
                return Some((tail, s));
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
    s.delivered_at.store(s.delivered, Ordering::Relaxed);
    gateway.metrics.bytes(chunk.len());
    let out = match s.rewrite.as_mut() {
        Some(rewrite) => rewrite.feed(&chunk),
        None => Ok(chunk),
    };
    if out.is_err() {
        s.done = None;
    }
    Some((out.map(Frame::data), s))
}

/// One bao chunk, the unit the cache records presence in.
const BAO_CHUNK_BYTES: u64 = 1024;

/// The name a response for `hash` carries: the last segment of its bundle
/// `path`, or the hash in hex.
fn served_name(hash: [u8; 32], path: Option<&str>) -> String {
    path.and_then(|p| p.rsplit('/').next())
        .map_or_else(|| alloy::hex::encode(hash), str::to_owned)
}

/// The content type a response named `name` carries, presented as
/// `present` says, and the rewrite its bytes pass through on a site.
fn presentation(present: Present, name: &str) -> (String, Option<Rewrite>) {
    match present {
        Present::Bare | Present::Download => (content_type(name), None),
        Present::Site => {
            let content_type = site::content_type(name);
            let essence = content_type.split(';').next().unwrap_or_default();
            let rewrite = Rewrite::for_type(essence);
            (content_type, rewrite)
        }
    }
}

/// The response that streams `body`, a blob named `name` presented as
/// `present` says, over `extent`.
fn streamed_response<S>(
    name: &str,
    content_type: &str,
    present: Present,
    extent: &Extent,
    body: S,
) -> Result<Response<Body>, PullError>
where
    S: futures_util::Stream<Item = Result<Frame<Bytes>, std::io::Error>> + Send + Sync + 'static,
{
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
    if present == Present::Bare {
        response = response.header(header::CONTENT_SECURITY_POLICY, "sandbox");
    }
    if let Some(size) = extent.length() {
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

/// Read `[offset, end)` of `hash` out of `engine` on a task of its own, one
/// [`CHUNK_BYTES`] read at a time, into the channel it returns. A failed or
/// short read sends its error and ends the channel; a dropped receiver stops
/// the reads. The hit counts as a use of the entry for eviction, and the
/// task holds the entry against eviction until its reads end.
fn read_cached(
    engine: CacheEngine,
    hash: decdn_cache::Hash,
    offset: u64,
    end: u64,
) -> mpsc::Receiver<std::io::Result<Bytes>> {
    let (tx, rx) = mpsc::channel(2);
    engine.observe_hit(hash);
    let held = engine.hold_in_use(hash);
    tokio::spawn(async move {
        let _held = held;
        let chunk = u64::try_from(CHUNK_BYTES).unwrap_or(u64::MAX);
        let mut next = offset;
        while next < end {
            let len = (end - next).min(chunk);
            let read = match engine.export_range(hash, next, len).await {
                Ok(bytes) => bytes,
                Err(e) => {
                    let e = std::io::Error::other(format!("read the gateway cache: {e}"));
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            if read.is_empty() {
                let e = std::io::Error::other("the cache entry ended short");
                let _ = tx.send(Err(e)).await;
                return;
            }
            next = next.saturating_add(u64::try_from(read.len()).unwrap_or(u64::MAX));
            if tx.send(Ok(read)).await.is_err() {
                return;
            }
        }
    });
    rx
}

/// A response body read out of the cache: the chunks [`read_cached`] sends,
/// passed through the rewrite.
struct CachedBody {
    chunks: mpsc::Receiver<std::io::Result<Bytes>>,
    rewrite: Option<Rewrite>,
    gateway: Arc<Gateway>,
    /// Set once the body sent its last frame.
    ended: bool,
}

/// The next frame of a body read out of the cache, or `None` at its end.
async fn next_cached_frame(
    mut s: CachedBody,
) -> Option<(Result<Frame<Bytes>, std::io::Error>, CachedBody)> {
    if s.ended {
        return None;
    }
    let out = match s.chunks.recv().await {
        Some(Ok(chunk)) => {
            s.gateway.metrics.bytes(chunk.len());
            match s.rewrite.as_mut() {
                Some(rewrite) => rewrite.feed(&chunk),
                None => Ok(chunk),
            }
        }
        Some(Err(e)) => Err(e),
        // Every byte is out: the rewrite lets out its tail.
        None => {
            s.ended = true;
            return s
                .rewrite
                .take()
                .map(|rewrite| (rewrite.finish().map(Frame::data), s));
        }
    };
    s.ended = out.is_err();
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
    /// Content bytes handed to the response so far.
    delivered: u64,
    /// The pull's listing's count of delivered bytes, which `/_status` reads
    /// while the worker runs the pull.
    delivered_at: Arc<AtomicU64>,
    gateway: Arc<Gateway>,
}

/// Serialize `address` EIP-55 checksummed: every address in `/_status` and
/// `GET /_capability` has this one spelling.
fn eip55<S: serde::Serializer>(address: &Address, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(address)
}

/// Serialize a content hash as bare lowercase hex, the spelling a gateway URL
/// and a `decdn:` URI carry.
fn bare_hex<S: serde::Serializer>(hash: &B256, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&alloy::hex::encode(hash))
}

/// The capability that pays for a pull, as `/_status` lists it.
#[derive(Debug, Clone, Copy, serde::Serialize)]
struct PayerLabel {
    /// The delegate key.
    #[serde(serialize_with = "eip55")]
    signer: Address,
    /// The pool the capability spends, `0x` hex.
    pool: PoolId,
}

impl PayerLabel {
    fn of(paying: &Credential) -> Self {
        Self {
            signer: paying.signer.address(),
            pool: paying.grant.pool_id,
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

/// A pull in flight, counted among the gateway's pulls and listed in
/// [`Gateway::active`] from before the job reaches the worker until the
/// worker's pull ends: the job carries the guard, and the worker drops it.
/// A response the client stops reading thus leaves the count when its pull
/// gives up, though its connection may stay open.
struct PullGuard {
    gateway: Arc<Gateway>,
    /// The pull's entry in [`Gateway::active`].
    id: u64,
    /// Content bytes its response delivered so far, shared with that entry.
    delivered: Arc<AtomicU64>,
}

impl PullGuard {
    /// A guard for a pull `gateway` is about to start, paid by `paying`.
    fn new(
        gateway: Arc<Gateway>,
        paying: &Credential,
        hash: [u8; 32],
        namespace: Option<u64>,
        range: Option<(u64, u64)>,
    ) -> Self {
        gateway.metrics.pull_started();
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
                    payer: PayerLabel::of(paying),
                    since: Instant::now(),
                    delivered: Arc::clone(&delivered),
                },
            );
        Self {
            gateway,
            id,
            delivered,
        }
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
/// attached anywhere on it, as context or as the error itself.
fn classify(e: anyhow::Error) -> PullError {
    if e.downcast_ref::<decdn_client::FundingNeeded>().is_some() {
        PullError::Funding(e)
    } else {
        PullError::Other(e)
    }
}

/// Mark a manifest pull that the manifest cap ended as
/// [`NotManifest::OverCap`]: the gateway reads no larger blob as a manifest.
/// Only [`PullError::Other`] carrying [`decdn_client::BlobTooLarge`] is one;
/// a funding stop that carries it stays a funding stop.
fn over_manifest_cap(stop: ManifestStop) -> ManifestStop {
    match stop {
        ManifestStop::Pull(PullError::Other(e))
            if e.downcast_ref::<decdn_client::BlobTooLarge>().is_some() =>
        {
            ManifestStop::Not(NotManifest::OverCap(clip(format!("{e:#}"))))
        }
        stop => stop,
    }
}

/// Entry `path` of bundle `hash` as a relative URL path, each segment
/// percent-encoded as in its `decdn:` URI.
fn entry_link(hash: [u8; 32], path: &str) -> String {
    let uri = DecdnUri::new(hash, UriTarget::BundleEntry(path.to_owned()), None).to_string();
    uri.get(ENTRY_PATH_OFFSET..).unwrap_or_default().to_owned()
}

/// What a request's path names, where a redirect to a site keeps its form.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Named {
    /// A directory (`<dir>/`), which the gateway reads as its `index.html`.
    Directory,
    /// What the path spells out.
    AsIs,
}

/// A site's `/_decdn/<rest>` as the rest of a `decdn:` URI, and what it
/// names: a directory in a bundle (`<hash>/<dir>/`) names its `index.html`,
/// as a site's own directory paths do. The bundle root (`<hash>/`) and the
/// query stay as they are.
fn directory_index(rest: &str) -> (String, Named) {
    let (path, query) = match rest.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (rest, None),
    };
    let directory = path
        .split_once('/')
        .is_some_and(|(_, entry)| !entry.is_empty() && entry.ends_with('/'));
    let index = if directory { "index.html" } else { "" };
    let rest = match query {
        Some(query) => format!("{path}{index}?{query}"),
        None => format!("{path}{index}"),
    };
    let named = if directory {
        Named::Directory
    } else {
        Named::AsIs
    };
    (rest, named)
}

/// A redirect from entry `path` of bundle `hash` to the directory it names,
/// with the request's `query` kept. The location is relative to the request,
/// so it holds on a site's own path and under `/_decdn/` alike.
fn to_directory(hash: [u8; 32], path: &str, query: Option<&str>) -> Response<Body> {
    let link = entry_link(hash, path);
    let last = link.rsplit('/').next().unwrap_or(&link);
    redirect(&match query {
        Some(query) => format!("{last}/?{query}"),
        None => format!("{last}/"),
    })
}

/// Where an entry's path starts in its `decdn:` URI: past `decdn:<hash>/`.
const ENTRY_PATH_OFFSET: usize = decdn_client::uri::SCHEME.len() + 65;

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
    }
}

/// The response headers a page on another origin may read from a public
/// gateway: the body's length and type, the hash (`ETag`), the window
/// (`Content-Range`, `Accept-Ranges`) and, on a 503, when to retry and
/// where to fetch instead (`Retry-After`, `Link`). `Content-Encoding` tells the
/// page that a proxy in front encoded the body, so `Content-Length` counts the
/// encoded bytes rather than the decoded body.
const PUBLIC_EXPOSED: header::HeaderValue = header::HeaderValue::from_static(
    "Content-Length, Content-Encoding, Content-Range, Accept-Ranges, Content-Type, \
     Content-Disposition, ETag, Retry-After, Link",
);

/// The origins that may read a public gateway's answers: all of them.
const ANY_ORIGIN: header::HeaderValue = header::HeaderValue::from_static("*");

/// The preflight answer of a public gateway for `path`, `bare` when on its
/// own host rather than a site or namespace host. The gateway asks for no
/// credentials and holds no token, so any origin may read its content, with a
/// `Range`, and call the API on the bare host: `PUT /_pointer` and
/// `PUT /_capability` with a JSON body, `GET /_delegate`, `GET /_capability`
/// and `GET /_status`. The API paths answer 404 on every other host. It sets
/// no `Access-Control-Allow-Origin`: [`readable_anywhere`] does, in
/// [`Gateway::handle`].
fn public_preflight(path: &str, bare: bool) -> Response<Body> {
    let (methods, headers) = match path {
        "/_delegate" | "/_capability" | "/_pointer" | "/_status" if !bare => {
            return text(
                StatusCode::NOT_FOUND,
                "the gateway API answers on the gateway origin only\n",
            );
        }
        "/_delegate" | "/_status" => ("GET", "content-type"),
        "/_capability" => ("GET, PUT", "content-type"),
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

/// The types with no `+xml` suffix that a browser runs or renders as a
/// document. [`is_active_type`] checks them along with every `+xml` type.
const ACTIVE_TYPES: &[&str] = &[
    "application/xml",
    "application/javascript",
    "application/ecmascript",
];

/// Whether a browser runs or renders `essence` as a document: a type in
/// [`ACTIVE_TYPES`], or any `+xml` type. A browser renders every XML type as
/// an XML document, which can embed and run an XHTML-namespaced `<script>`.
/// The gateway serves an active type as plain text, so fetched content never
/// runs on the gateway's origin.
fn is_active_type(essence: &str) -> bool {
    ACTIVE_TYPES.contains(&essence) || essence.ends_with("+xml")
}

/// The `Content-Type` for an entry, guessed from its file name. Every `text/*`
/// type and every active type ([`is_active_type`]) is served as plain text; a
/// name with no known extension is served as bytes.
fn content_type(name: &str) -> String {
    let guess = mime_guess::from_path(name).first_or_octet_stream();
    if guess.type_() == mime_guess::mime::TEXT || is_active_type(guess.essence_str()) {
        "text/plain; charset=utf-8".to_owned()
    } else {
        guess.essence_str().to_owned()
    }
}

/// An entry its manifest declares empty, or the empty root: its headers and
/// an empty body, with no pull. There is nothing to deliver, and a declared
/// size of zero must not become an uncapped pull of whatever the hash really
/// names.
fn empty_entry(path: &str, hash: [u8; 32], present: Present) -> Response<Body> {
    let name = path.rsplit('/').next().unwrap_or(path);
    let content_type = match present {
        Present::Bare | Present::Download => content_type(name),
        Present::Site => site::content_type(name),
    };
    head_response(&content_type, Some(0), hash, present)
}

/// What a response delivers of a blob: its size, when the manifest declares
/// it or a pull verified it, the byte window a `Range`
/// asked for within it, and whether the response has a fixed length at all
/// (a rewritten page does not).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Extent {
    /// The blob's size, as the manifest declares it or a pull verified it.
    size: Option<u64>,
    /// The `(offset, len)` window served, or the whole blob.
    window: Option<(u64, u64)>,
    /// Whether the response's length is the window's or the size: a rewrite
    /// changes the length, so a rewritten response has none, and takes no
    /// range.
    fixed: bool,
}

impl Extent {
    /// The extent of a blob of `size` for a request with the `Range` header
    /// `range`, `rewritten` as it streams. A range on a sized blob that is
    /// not rewritten is honoured; one past the end is refused with the size;
    /// any other is ignored.
    ///
    /// # Errors
    ///
    /// The blob's size, when the range is unsatisfiable.
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
    /// declared size for the whole. What `--max-blob-mb` caps.
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

/// What a request asks of the content it names.
#[derive(Clone, Copy)]
enum Ask<'a> {
    /// A `GET`.
    Get {
        /// Its `Range` header.
        range: Option<&'a str>,
        /// Its query, which a directory's redirect keeps.
        query: Option<&'a str>,
    },
    /// A `HEAD` of a blob: its headers alone.
    Head,
}

/// The type a raw blob named `name` is served as, where `present` shows it.
fn blob_type(present: Present, name: &str) -> String {
    match present {
        Present::Bare | Present::Download => content_type(name),
        Present::Site => site::content_type(name),
    }
}

/// A `HEAD` answer: the type, the length when known (and with it that ranges
/// are taken), and the blob's hash as the entity tag, with no body. A bare
/// response carries the `sandbox` policy its `GET` does ([`Present::Bare`]).
fn head_response(
    content_type: &str,
    size: Option<u64>,
    hash: [u8; 32],
    present: Present,
) -> Response<Body> {
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::ETAG, format!("\"{}\"", alloy::hex::encode(hash)))
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff");
    if present == Present::Bare {
        response = response.header(header::CONTENT_SECURITY_POLICY, "sandbox");
    }
    if let Some(size) = size {
        response = response
            .header(header::CONTENT_LENGTH, size)
            .header(header::ACCEPT_RANGES, "bytes");
    }
    response
        .body(full(String::new()))
        .unwrap_or_else(|_| text(StatusCode::INTERNAL_SERVER_ERROR, "bad head response\n"))
}

/// The answer to a request for `uri` whose pull ended with `e`: a blob that
/// is no manifest is not found, a capability that cannot pay asks the client
/// to pay elsewhere, and anything else failed upstream. A `public` gateway
/// keeps the upstream detail (node ids, addresses, chain errors) in its log
/// and answers a bare 502; a loopback gateway shows its own user the detail.
fn pull_failed(e: PullError, uri: &DecdnUri, public: bool) -> Response<Body> {
    match e {
        PullError::NotManifest(e) => {
            let error = format!("{e:#}");
            tracing::info!(%error, %uri, "a bundle URI names no manifest");
            text(StatusCode::NOT_FOUND, &format!("{error}\n"))
        }
        PullError::Funding(e) => {
            tracing::warn!(error = %e, "the gateway's capability cannot pay");
            cannot_pay(uri)
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

/// The seconds a client waits before it retries a request the gateway
/// cannot pay for: time for an operator to register a capability.
const CANNOT_PAY_RETRY_SECS: u64 = 60;

/// The `503` when the gateway cannot pay for `uri` right now: the client did
/// nothing wrong, so it retries after [`CANNOT_PAY_RETRY_SECS`], or fetches
/// the canonical URI with a paying client of its own; the `Link` header and
/// the JSON body carry it.
fn cannot_pay(uri: &DecdnUri) -> Response<Body> {
    let retry_after_secs = CANNOT_PAY_RETRY_SECS;
    let mut response = json(
        StatusCode::SERVICE_UNAVAILABLE,
        &serde_json::json!({
            "error": "the gateway cannot pay right now",
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

fn html(status: StatusCode, body: &str) -> Response<Body> {
    let page = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" \
         content=\"width=device-width\"><title>decdn gateway</title><style>body{{font-family:\
         system-ui,sans-serif;max-width:48rem;margin:3rem auto;padding:0 1rem}}td{{padding:\
         .2rem 1rem .2rem 0}}</style></head><body>{body}</body></html>"
    );
    typed(status, page, "text/html; charset=utf-8")
}

/// Writes `s` into `out` HTML-escaped: `&`, `<`, `>`, `"` and `'` become
/// entities, every other character passes through.
fn write_escaped(out: &mut impl std::fmt::Write, s: &str) -> std::fmt::Result {
    for c in s.chars() {
        match c {
            '&' => out.write_str("&amp;")?,
            '<' => out.write_str("&lt;")?,
            '>' => out.write_str("&gt;")?,
            '"' => out.write_str("&quot;")?,
            '\'' => out.write_str("&#39;")?,
            other => out.write_char(other)?,
        }
    }
    Ok(())
}

/// Writes the link to bundle entry `path` into `out`: on a site, `./` and
/// the path relative to the bundle root (as [`entry_link`] gives it);
/// otherwise the entry's `decdn:` URI. The URI streams out of its `Display`
/// as it encodes, so a sink that stops early stops the encoding too, and no
/// encoded copy of the path is held whole.
fn write_link(
    out: &mut impl std::fmt::Write,
    manifest_hash: [u8; 32],
    path: &str,
    namespace: Option<u64>,
    present: Present,
) -> std::fmt::Result {
    if present == Present::Site {
        out.write_str("./")?;
        let uri = DecdnUri::new(manifest_hash, UriTarget::BundleEntry(path.to_owned()), None);
        let mut rest = Skipping {
            out,
            skip: ENTRY_PATH_OFFSET,
        };
        write!(rest, "{uri}")
    } else {
        let uri = DecdnUri::new(
            manifest_hash,
            UriTarget::BundleEntry(path.to_owned()),
            namespace,
        );
        write!(out, "{uri}")
    }
}

/// A [`std::fmt::Write`] adapter that HTML-escapes ([`write_escaped`]) all
/// it is given into the sink it wraps.
struct Escaping<'a, W>(&'a mut W);

impl<W: std::fmt::Write> std::fmt::Write for Escaping<'_, W> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        write_escaped(self.0, s)
    }
}

/// A [`std::fmt::Write`] adapter that drops the first `skip` bytes it is
/// given and passes the rest to `out`. A skip that ends inside a character
/// fails the write.
struct Skipping<'a, W> {
    /// The sink the bytes past the skip go to.
    out: &'a mut W,
    /// Bytes still to drop.
    skip: usize,
}

impl<W: std::fmt::Write> std::fmt::Write for Skipping<'_, W> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        if let Some(left) = self.skip.checked_sub(s.len()) {
            self.skip = left;
            return Ok(());
        }
        let rest = s.get(self.skip..).ok_or(std::fmt::Error)?;
        self.skip = 0;
        self.out.write_str(rest)
    }
}

/// Writes the HTML listing of a bundle's entries into `out` — the bundle URI,
/// then one row per entry linking to its URL — which [`Gateway::index`]
/// answers when it does not serve the bundle's `index.html`.
fn write_listing(
    out: &mut impl std::fmt::Write,
    manifest: &BundleManifest,
    manifest_hash: [u8; 32],
    namespace: Option<u64>,
    present: Present,
    uri: &DecdnUri,
) -> std::fmt::Result {
    out.write_str("<h1>Bundle</h1><p><code>")?;
    write!(Escaping(&mut *out), "{uri}")?;
    out.write_str("</code></p><table>")?;
    for entry in &manifest.entries {
        out.write_str("<tr><td><a href=\"")?;
        write_link(
            &mut Escaping(&mut *out),
            manifest_hash,
            &entry.path,
            namespace,
            present,
        )?;
        out.write_str("\">")?;
        write_escaped(out, &entry.path)?;
        out.write_str("</a></td><td>")?;
        if let Some(size) = entry.size {
            out.write_str(&human_bytes(size))?;
        }
        out.write_str("</td></tr>")?;
    }
    out.write_str("</table>")
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
