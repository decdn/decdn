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
//! an exact host first, then `*.<domain>`, then `*`.
//!
//! **Capabilities.** The gateway holds one delegate key per capability, in
//! memory only. `GET /_delegate` mints a fresh key and returns its address, so
//! no two callers see the same address and none can follow the user across
//! sites; a key no capability claims within [`PENDING_TTL`] is dropped.
//! `PUT /_capability` registers a `dcap1:` capability issued to a minted
//! address, with a scope. A page registers through CORS, and its scope must
//! cover its own origin's host: `example.com` may register `example.com`,
//! `*.example.com` or `*`, never another host. The gateway token registers any
//! scope. `GET /_capability` lists the registrations to the token holder. A
//! restart drops every key, and with it every capability issued to one.
//!
//! **Boundaries.** Only `/_delegate` and `/_capability` answer cross-origin;
//! content never does, so a page can neither read a response nor set the
//! token or host headers itself. The token lives in `<data-dir>/token`,
//! owner-only. The gateway listens on loopback only and checks `Host` against
//! DNS rebinding. Every pull runs the `decdn fetch` path with its bytes going
//! to the HTTP response, one pull at a time.

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context as _;
use bytes::Bytes;
use decdn_client::bundle::{BundleManifest, parse_hash};
use decdn_client::uri::{DecdnUri, UriTarget};
use decdn_common::cli::{self, ClientFetchArgs, FetchArgs, GatewayArgs};
use decdn_common::config::load_file_config;
use decdn_incentive::CapabilityGrant;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt as _, Full, Limited, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt as _, DuplexStream};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};

use super::fetch::{Credential, Sink, fetch_into};
use crate::known_chains;

/// The request header carrying the gateway token.
pub const TOKEN_HEADER: &str = "x-decdn-token";

/// The request header naming the host of the page a content request serves.
pub const HOST_HEADER: &str = "x-decdn-host";

/// How long a minted delegate waits for a capability before it is dropped.
pub const PENDING_TTL: Duration = Duration::from_mins(15);

/// The most minted delegates waiting for a capability at once: a page cannot
/// make the gateway hold keys without bound.
const MAX_PENDING: usize = 32;

/// The largest `PUT /_capability` body.
const MAX_BODY_BYTES: usize = 64 * 1024;

/// A gateway pull that makes no verified progress for this long gives up, so
/// a stuck request does not hold the one pull slot.
const GIVE_UP_AFTER_SECS: u64 = 120;

/// The largest bundle manifest the gateway reads into memory.
const MAX_MANIFEST_BYTES: usize = 64 << 20;

/// The most parsed manifests the gateway keeps.
const MAX_CACHED_MANIFESTS: usize = 16;

/// The manifest bytes the cache keeps across all its entries. The newest
/// entry stays even when it alone exceeds this.
const MANIFEST_CACHE_BYTES: usize = 64 << 20;

/// The duplex buffer between a pull and its HTTP response.
const PIPE_BYTES: usize = 1 << 20;

/// The read size for response chunks.
const CHUNK_BYTES: usize = 256 * 1024;

type Body = BoxBody<Bytes, std::io::Error>;

/// Run the gateway until Ctrl-C.
///
/// # Errors
///
/// When `--listen` is not a loopback address, when keystore or capability
/// flags are given, when the data dir, token or listener cannot be set up, or
/// when the chain coordinates cannot be resolved.
pub async fn gateway(args: &GatewayArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.listen.ip().is_loopback(),
        "--listen must be a loopback address"
    );
    let mut common = args.common.clone();
    anyhow::ensure!(
        common.keystore.is_none()
            && common.keystore_password_file.is_none()
            && common.capability.is_none()
            && common.capability_file.is_none(),
        "the gateway holds its own delegate keys: register capabilities through \
         `PUT /_capability` instead of --keystore / --capability"
    );
    fill_chain_defaults(&mut common, config_path)?;
    common.give_up_after_secs = common.give_up_after_secs.or(Some(GIVE_UP_AFTER_SECS));
    let root = common
        .data_dir
        .clone()
        .unwrap_or_else(|| cli::common::expand_tilde(Path::new("~/.decdn/gateway")));
    private_dir(&root)?;
    let token = load_or_create_token(&root.join("token"))?;
    // The keys die with the process, so their lane records do too: the buyer
    // store lives in a directory removed when the gateway stops.
    let store_dir = tempfile::Builder::new()
        .prefix("store-")
        .tempdir_in(&root)
        .context("create the gateway's buyer store dir")?;
    private_dir(store_dir.path())?;
    common.data_dir = Some(store_dir.path().to_path_buf());

    let gateway = Arc::new(Gateway {
        common,
        config_path: config_path.map(Path::to_path_buf),
        token,
        delegates: Mutex::new(HashMap::new()),
        pull: Arc::new(Mutex::new(())),
        manifests: std::sync::Mutex::new(ManifestCache::default()),
        manifest_fill: Mutex::new(()),
        hosts: allowed_hosts(args.listen),
    });

    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("listen on {}", args.listen))?;
    println!(
        "decdn gateway on http://{}/ (Ctrl-C to stop)\ngateway token: {} (also in {})",
        args.listen,
        gateway.token,
        root.join("token").display()
    );
    loop {
        let (stream, _) = tokio::select! {
            accepted = listener.accept() => accepted.context("accept a connection")?,
            _ = tokio::signal::ctrl_c() => return Ok(()),
        };
        let gateway = Arc::clone(&gateway);
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

/// Chain coordinates neither a flag nor the config file sets come from the
/// built-in testnet, so `decdn gateway` runs with no config.
fn fill_chain_defaults(
    common: &mut ClientFetchArgs,
    config_path: Option<&Path>,
) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
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

/// The host of a browser `Origin` header (`https://example.com:8443` gives
/// `example.com`). `None` for an opaque origin.
fn origin_host(origin: &str) -> Option<String> {
    let rest = origin.split_once("://")?.1;
    let host = rest.split(['/', ':']).next()?;
    let host = host.to_ascii_lowercase();
    valid_host(&host).ok().map(|()| host)
}

/// Whether the page at `origin` may register `scope`: the scope must cover
/// the origin's own host.
fn may_register(origin: Option<&str>, scope: &Scope) -> Result<(), String> {
    let host = origin.and_then(origin_host).ok_or_else(|| {
        "a page registers with its Origin; use the gateway token otherwise".to_owned()
    })?;
    if scope.covers(&host) {
        Ok(())
    } else {
        Err(format!(
            "{host} may not register scope `{scope}`: it does not cover {host}"
        ))
    }
}

/// A delegate key and the capability registered to it, if any.
struct Delegate {
    signer: Arc<PrivateKeySigner>,
    minted: Instant,
    capability: Option<Registered>,
}

struct Registered {
    grant: CapabilityGrant,
    scope: Scope,
    /// The origin that registered it, or `None` for the token holder.
    origin: Option<String>,
    spent: bool,
}

#[derive(serde::Deserialize)]
struct RegisterRequest {
    capability: String,
    scope: String,
}

struct Gateway {
    common: ClientFetchArgs,
    config_path: Option<PathBuf>,
    token: String,
    delegates: Mutex<HashMap<Address, Delegate>>,
    /// One pull at a time: every pull opens the one buyer-pool store.
    pull: Arc<Mutex<()>>,
    manifests: std::sync::Mutex<ManifestCache>,
    /// Held while a missing manifest is pulled, so concurrent requests for it
    /// pull and pay once. Cache hits never wait on it.
    manifest_fill: Mutex<()>,
    hosts: Vec<String>,
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
    Other(anyhow::Error),
}

/// The capability a content request pays with.
struct Paying {
    signer: Arc<PrivateKeySigner>,
    grant: CapabilityGrant,
}

impl Gateway {
    async fn handle(self: Arc<Self>, req: Request<Incoming>) -> Response<Body> {
        let host = req
            .headers()
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");
        if !self.hosts.iter().any(|allowed| allowed == host) {
            return text(StatusCode::FORBIDDEN, "unknown Host\n");
        }
        let origin = req
            .headers()
            .get(header::ORIGIN)
            .and_then(|o| o.to_str().ok())
            .map(str::to_owned);
        let authorized = self.authorized(req.headers());
        let path = req.uri().path().to_owned();
        let method = req.method().clone();
        match (method, path.as_str()) {
            (Method::OPTIONS, "/_delegate" | "/_capability") => preflight(origin.as_deref()),
            (Method::OPTIONS, _) => text(StatusCode::FORBIDDEN, "no cross-origin access\n"),
            (Method::GET, "/") => html(
                StatusCode::OK,
                "<h1>decdn gateway</h1><p>Running. Connect the deCDN browser extension with \
                 the token <code>decdn gateway</code> printed when it started.</p>",
            ),
            (Method::GET, "/favicon.ico") => text(StatusCode::NOT_FOUND, ""),
            (Method::GET, "/_delegate") => with_cors(self.mint().await, origin.as_deref()),
            (Method::PUT, "/_capability") => {
                let response = self.register(req, origin.as_deref(), authorized).await;
                with_cors(response, origin.as_deref())
            }
            (Method::GET, "/_capability") if authorized => self.list().await,
            (Method::GET, "/_capability") => text(StatusCode::UNAUTHORIZED, "token required\n"),
            (Method::GET | Method::HEAD, _) => {
                let target = req
                    .uri()
                    .path_and_query()
                    .map_or("/", |pq| pq.as_str())
                    .to_owned();
                self.content(req.headers(), authorized, target.trim_start_matches('/'))
                    .await
            }
            _ => text(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n"),
        }
    }

    fn authorized(&self, headers: &HeaderMap) -> bool {
        headers
            .get(TOKEN_HEADER)
            .and_then(|t| t.to_str().ok())
            .is_some_and(|given| token_matches(&self.token, given))
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

    async fn mint(&self) -> Response<Body> {
        let mut delegates = self.delegates.lock().await;
        Self::prune(&mut delegates);
        let pending = delegates
            .values()
            .filter(|d| d.capability.is_none())
            .count();
        if pending >= MAX_PENDING {
            return text(
                StatusCode::TOO_MANY_REQUESTS,
                "too many delegates are waiting for a capability\n",
            );
        }
        let signer = PrivateKeySigner::random();
        let address = signer.address();
        delegates.insert(
            address,
            Delegate {
                signer: Arc::new(signer),
                minted: Instant::now(),
                capability: None,
            },
        );
        json(
            StatusCode::OK,
            &serde_json::json!({ "address": address.to_string() }),
        )
    }

    async fn register(
        &self,
        req: Request<Incoming>,
        origin: Option<&str>,
        authorized: bool,
    ) -> Response<Body> {
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
        if !authorized && let Err(e) = may_register(origin, &scope) {
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
        if let Err(e) = grant.to_signed_capability() {
            return text(StatusCode::UNPROCESSABLE_ENTITY, &format!("{e}\n"));
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
            origin: if authorized {
                None
            } else {
                origin.map(str::to_owned)
            },
            spent: false,
        });
        json(StatusCode::CREATED, &response)
    }

    async fn list(&self) -> Response<Body> {
        let mut delegates = self.delegates.lock().await;
        Self::prune(&mut delegates);
        let list: Vec<_> = delegates
            .values()
            .filter_map(|d| d.capability.as_ref())
            .map(|r| {
                serde_json::json!({
                    "signer": r.grant.signer.to_string(),
                    "pool": r.grant.pool_id.to_string(),
                    "scope": r.scope.to_string(),
                    "origin": r.origin,
                    "cap": r.grant.spending_cap,
                    "expiry": r.grant.expiry,
                    "spent": r.spent,
                })
            })
            .collect();
        json(StatusCode::OK, &serde_json::Value::Array(list))
    }

    /// The live capability whose scope covers `host` most specifically.
    async fn paying_for(&self, host: &str) -> Option<Paying> {
        let mut delegates = self.delegates.lock().await;
        Self::prune(&mut delegates);
        delegates
            .values()
            .filter_map(|d| d.capability.as_ref().map(|r| (d, r)))
            .filter(|(_, r)| !r.spent && r.scope.covers(host))
            .max_by_key(|(_, r)| (r.scope.rank(), std::cmp::Reverse(r.grant.expiry)))
            .map(|(d, r)| Paying {
                signer: Arc::clone(&d.signer),
                grant: r.grant.clone(),
            })
    }

    async fn mark_spent(&self, signer: Address) {
        if let Some(registered) = self
            .delegates
            .lock()
            .await
            .get_mut(&signer)
            .and_then(|d| d.capability.as_mut())
        {
            registered.spent = true;
        }
    }

    async fn content(
        self: &Arc<Self>,
        headers: &HeaderMap,
        authorized: bool,
        rest: &str,
    ) -> Response<Body> {
        if !authorized {
            return text(StatusCode::UNAUTHORIZED, "token required\n");
        }
        let Some(page_host) = headers
            .get(HOST_HEADER)
            .and_then(|h| h.to_str().ok())
            .map(str::to_ascii_lowercase)
        else {
            return text(StatusCode::UNAUTHORIZED, "page host required\n");
        };
        let uri: DecdnUri = match format!("decdn:{rest}").parse() {
            Ok(uri) => uri,
            Err(e) => return text(StatusCode::BAD_REQUEST, &format!("{e}\n")),
        };
        let Some(paying) = self.paying_for(&page_host).await else {
            return text(
                StatusCode::PAYMENT_REQUIRED,
                &format!("no capability covers {page_host}\n"),
            );
        };
        let signer = paying.signer.address();
        let result = match &uri.target {
            UriTarget::Blob => {
                self.stream(uri.hash, uri.namespace, &paying, None, None)
                    .await
            }
            UriTarget::BundleEntry(path) => {
                self.entry(uri.hash, uri.namespace, path, &paying).await
            }
            UriTarget::Bundle => self.index(uri.hash, uri.namespace, &paying).await,
            _ => return text(StatusCode::BAD_REQUEST, "unsupported URI target\n"),
        };
        match result {
            Ok(response) => response,
            Err(PullError::Funding(e)) => {
                self.mark_spent(signer).await;
                text(
                    StatusCode::PAYMENT_REQUIRED,
                    &format!("the capability for {page_host} ran out: {e:#}\n"),
                )
            }
            Err(PullError::Other(e)) => text(StatusCode::BAD_GATEWAY, &format!("{e:#}\n")),
        }
    }

    /// One bundle entry, by its literal manifest path.
    async fn entry(
        self: &Arc<Self>,
        manifest_hash: [u8; 32],
        namespace: Option<u64>,
        path: &str,
        paying: &Paying,
    ) -> Result<Response<Body>, PullError> {
        let manifest = self.manifest(manifest_hash, namespace, paying).await?;
        let Some(entry) = manifest.entries.iter().find(|e| e.path == path) else {
            return Ok(text(
                StatusCode::NOT_FOUND,
                &format!("the bundle has no entry {path:?}\n"),
            ));
        };
        let hash = parse_hash(&entry.hash).map_err(PullError::Other)?;
        self.stream(hash, namespace, paying, Some(path), entry.size)
            .await
    }

    /// An HTML index of a bundle's entries, each linking to its gateway URL.
    async fn index(
        self: &Arc<Self>,
        manifest_hash: [u8; 32],
        namespace: Option<u64>,
        paying: &Paying,
    ) -> Result<Response<Body>, PullError> {
        let manifest = self.manifest(manifest_hash, namespace, paying).await?;
        let mut rows = String::new();
        for entry in &manifest.entries {
            let link = DecdnUri::new(
                manifest_hash,
                UriTarget::BundleEntry(entry.path.clone()),
                namespace,
            )
            .to_string();
            let size = entry.size.map_or_else(String::new, human_bytes);
            let _ = write!(
                rows,
                "<tr><td><a href=\"{}\">{}</a></td><td>{size}</td></tr>",
                escape(&link),
                escape(&entry.path),
            );
        }
        Ok(html(
            StatusCode::OK,
            &format!(
                "<h1>Bundle</h1><p><code>{}</code></p><table>{rows}</table>",
                escape(&DecdnUri::new(manifest_hash, UriTarget::Bundle, namespace).to_string())
            ),
        ))
    }

    fn cached_manifest(&self, hash: &[u8; 32]) -> Option<Arc<BundleManifest>> {
        self.manifests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(hash)
    }

    /// The bundle manifest `hash`, from the cache or pulled once.
    async fn manifest(
        self: &Arc<Self>,
        hash: [u8; 32],
        namespace: Option<u64>,
        paying: &Paying,
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
        let (first, mut reader, done) = self.start_pull(hash, namespace, paying).await?;
        let mut bytes = first.to_vec();
        let mut buf = vec![0u8; CHUNK_BYTES];
        loop {
            let n = reader
                .read(&mut buf)
                .await
                .map_err(|e| PullError::Other(e.into()))?;
            if n == 0 {
                break;
            }
            bytes.extend_from_slice(buf.get(..n).unwrap_or_default());
            if bytes.len() > MAX_MANIFEST_BYTES {
                return Err(PullError::Other(anyhow::anyhow!(
                    "the manifest is larger than {MAX_MANIFEST_BYTES} bytes"
                )));
            }
        }
        finished(done).await?;
        let manifest = Arc::new(
            BundleManifest::parse(&bytes)
                .context("not a bundle manifest; drop the trailing `/` to fetch the raw blob")
                .map_err(PullError::Other)?,
        );
        self.manifests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(hash, Arc::clone(&manifest), bytes.len());
        Ok(manifest)
    }

    /// A response streaming `hash`'s verified bytes. Errors that come before
    /// the first byte become the error status; a later one aborts the body.
    async fn stream(
        self: &Arc<Self>,
        hash: [u8; 32],
        namespace: Option<u64>,
        paying: &Paying,
        path: Option<&str>,
        size: Option<u64>,
    ) -> Result<Response<Body>, PullError> {
        let (first, reader, done) = self.start_pull(hash, namespace, paying).await?;
        let state = (Some(first), reader, Some(done));
        let body = futures_util::stream::unfold(state, |(first, mut reader, done)| async move {
            if let Some(first) = first {
                return Some((Ok(Frame::data(first)), (None, reader, done)));
            }
            let mut buf = vec![0u8; CHUNK_BYTES];
            match reader.read(&mut buf).await {
                Ok(0) => match finished(done?).await {
                    Ok(()) => None,
                    // The verified prefix is already out: abort the body so the
                    // browser does not take it for the whole file.
                    Err(e) => Some((
                        Err(std::io::Error::other(pull_error_text(&e))),
                        (None, reader, None),
                    )),
                },
                Ok(n) => {
                    buf.truncate(n);
                    Some((Ok(Frame::data(Bytes::from(buf))), (None, reader, done)))
                }
                Err(e) => Some((Err(e), (None, reader, None))),
            }
        });
        let name = path
            .and_then(|p| p.rsplit('/').next())
            .map_or_else(|| alloy::hex::encode(hash), str::to_owned);
        let mut response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, content_type(&name))
            .header(
                header::CONTENT_DISPOSITION,
                format!("inline; filename=\"{}\"", name.replace(['"', '\\'], "_")),
            )
            .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff");
        if let Some(size) = size {
            response = response.header(header::CONTENT_LENGTH, size);
        }
        response
            .body(StreamBody::new(body).boxed())
            .map_err(|e| PullError::Other(e.into()))
    }

    /// Start the pull of `hash` on its own thread, holding the pull slot until
    /// it ends, and wait for its first verified bytes.
    async fn start_pull(
        self: &Arc<Self>,
        hash: [u8; 32],
        namespace: Option<u64>,
        paying: &Paying,
    ) -> Result<(Bytes, DuplexStream, oneshot::Receiver<anyhow::Result<()>>), PullError> {
        let args = FetchArgs {
            hash: alloy::hex::encode(hash),
            output: PathBuf::from("-"),
            namespace,
            common: self.common.clone(),
        };
        let credential = Credential {
            signer: Arc::clone(&paying.signer),
            grant: paying.grant.clone(),
        };
        let config_path = self.config_path.clone();
        let slot = Arc::clone(&self.pull).lock_owned().await;
        let (writer, mut reader) = tokio::io::duplex(PIPE_BYTES);
        let (done_tx, mut done_rx) = oneshot::channel();
        // The fetch future is not `Send`, so it runs on a thread of its own
        // with a current-thread runtime, and talks to the response through the
        // duplex pipe.
        std::thread::Builder::new()
            .name("decdn-gateway-pull".to_owned())
            .spawn(move || {
                let _slot = slot;
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(anyhow::Error::from)
                    .and_then(|runtime| {
                        let mut sink = Sink::Writer(Box::new(writer));
                        runtime.block_on(fetch_into(
                            &args,
                            config_path.as_deref(),
                            &mut sink,
                            Some(credential),
                        ))
                    });
                let _ = done_tx.send(result);
            })
            .map_err(|e| PullError::Other(e.into()))?;

        let mut first = vec![0u8; CHUNK_BYTES];
        let mut done: Option<anyhow::Result<()>> = None;
        let n = loop {
            tokio::select! {
                biased;
                n = reader.read(&mut first) => break n.map_err(|e| PullError::Other(e.into()))?,
                result = &mut done_rx, if done.is_none() => {
                    let result = result.unwrap_or_else(|_| Err(anyhow::anyhow!("the pull ended")));
                    if let Err(e) = result {
                        return Err(classify(e));
                    }
                    done = Some(Ok(()));
                }
            }
        };
        first.truncate(n);
        // A pull that already ended hands back a spent receiver; the body's
        // end-of-stream check then finds it closed and the bytes complete.
        let (spent_tx, spent_rx) = oneshot::channel();
        let done_rx = if done.is_some() {
            let _ = spent_tx.send(Ok(()));
            spent_rx
        } else {
            done_rx
        };
        Ok((Bytes::from(first), reader, done_rx))
    }
}

/// The pull's own result once its bytes are drained.
async fn finished(done: oneshot::Receiver<anyhow::Result<()>>) -> Result<(), PullError> {
    match done.await {
        Ok(Ok(())) | Err(_) => Ok(()),
        Ok(Err(e)) => Err(classify(e)),
    }
}

fn classify(e: anyhow::Error) -> PullError {
    if e.chain().any(|cause| {
        cause
            .downcast_ref::<decdn_client::FundingNeeded>()
            .is_some()
    }) {
        PullError::Funding(e)
    } else {
        PullError::Other(e)
    }
}

fn pull_error_text(e: &PullError) -> String {
    match e {
        PullError::Funding(e) | PullError::Other(e) => format!("{e:#}"),
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

/// `response` readable by `origin`.
fn with_cors(mut response: Response<Body>, origin: Option<&str>) -> Response<Body> {
    if let Some(value) = origin.and_then(|o| header::HeaderValue::from_str(o).ok()) {
        let headers = response.headers_mut();
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
        headers.insert(header::VARY, header::HeaderValue::from_static("Origin"));
    }
    response
}

/// The `Content-Type` for an entry by its file name. HTML and SVG are served
/// as text, so a fetched page never runs on the gateway's origin.
fn content_type(name: &str) -> &'static str {
    let ext = name
        .rsplit_once('.')
        .map(|(_, ext)| ext.to_ascii_lowercase())
        .unwrap_or_default();
    match ext.as_str() {
        "mp4" | "m4v" => "video/mp4",
        "webm" => "video/webm",
        "mkv" => "video/x-matroska",
        "mov" => "video/quicktime",
        "mp3" => "audio/mpeg",
        "m4a" => "audio/mp4",
        "ogg" | "oga" => "audio/ogg",
        "wav" => "audio/wav",
        "flac" => "audio/flac",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "pdf" => "application/pdf",
        "json" => "application/json",
        "txt" | "md" | "csv" | "html" | "htm" | "svg" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn full(body: String) -> Body {
    Full::new(Bytes::from(body))
        .map_err(|never| match never {})
        .boxed()
}

fn text(status: StatusCode, body: &str) -> Response<Body> {
    typed(status, body.to_owned(), "text/plain; charset=utf-8")
}

fn json(status: StatusCode, value: &serde_json::Value) -> Response<Body> {
    typed(status, value.to_string(), "application/json")
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
