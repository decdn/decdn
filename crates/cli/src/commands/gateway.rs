//! `decdn gateway`: serve `decdn:` URIs (#2449) to a browser over loopback
//! HTTP, paid from a sponsored allowance.
//!
//! `GET /<hash>[/<path>][?ns=<id>]` serves `decdn:<hash>[/<path>][?ns=<id>]`.
//! A blob or a bundle entry streams its verified bytes in order as they
//! arrive; a whole bundle (`/<hash>/`) gets an HTML index of its entries. `/`
//! shows the allowance and links to the onramp's check.
//!
//! The gateway pays as a delegate, the way the onramp's `decdn.sh` installer
//! does: a throwaway key under `<data-dir>/key`, and the capability the onramp
//! issues to it once the person passes the check in the browser. A capability
//! within [`EXPIRY_MARGIN_SECS`] of expiry, or one a pull reports as spent,
//! gives way to a fresh key. `--capability`/`--capability-file` with
//! `--keystore` pay from a capability the caller holds instead.
//!
//! Every request runs the `decdn fetch` path with its bytes going to the HTTP
//! response, one pull at a time: the buyer-pool store is one database file.
//! The gateway listens on loopback only and checks the `Host` header against
//! DNS rebinding. Any page the browser loads can still request a gateway URL
//! and spend the allowance; a per-install token is the planned guard.

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use bytes::Bytes;
use decdn_client::bundle::{BundleManifest, parse_hash};
use decdn_client::uri::{DecdnUri, UriTarget};
use decdn_common::cli::{self, ClientFetchArgs, FetchArgs, GatewayArgs, KeyGenArgs};
use decdn_common::config::load_file_config;
use decdn_incentive::eth_identity::load_signer;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt as _, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt as _, DuplexStream};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};

use super::fetch::{Sink, fetch_into};
use crate::known_chains;

/// A saved capability with less life than this is replaced before a pull, as
/// `decdn.sh` does: a pull must not start on an allowance about to lapse.
pub const EXPIRY_MARGIN_SECS: u64 = 4020;

/// A gateway pull that makes no verified progress for this long gives up, so
/// a stuck request does not hold the one pull slot.
const GIVE_UP_AFTER_SECS: u64 = 120;

/// The largest bundle manifest the gateway reads into memory.
const MAX_MANIFEST_BYTES: usize = 64 << 20;

/// The duplex buffer between a pull and its HTTP response.
const PIPE_BYTES: usize = 1 << 20;

/// The read size for response chunks.
const CHUNK_BYTES: usize = 256 * 1024;

type Body = BoxBody<Bytes, std::io::Error>;

/// Run the gateway until Ctrl-C.
///
/// # Errors
///
/// When `--listen` is not a loopback address, when the data dir or listener
/// cannot be set up, or when the chain coordinates cannot be resolved.
pub async fn gateway(args: &GatewayArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.listen.ip().is_loopback(),
        "--listen must be a loopback address: the gateway spends its allowance for any request \
         it serves"
    );
    let mut common = args.common.clone();
    fill_chain_defaults(&mut common, config_path)?;
    common.give_up_after_secs = common.give_up_after_secs.or(Some(GIVE_UP_AFTER_SECS));
    let root = common
        .data_dir
        .clone()
        .unwrap_or_else(|| cli::common::expand_tilde(Path::new("~/.decdn/gateway")));
    private_dir(&root)?;
    common.data_dir = Some(root.clone());

    let funding = if common.capability.is_some() || common.capability_file.is_some() {
        Funding::Held
    } else {
        Funding::Sponsored(Sponsor {
            dir: root.join("key"),
            onramp: args.onramp.trim_end_matches('/').to_owned(),
            http: reqwest::Client::new(),
        })
    };
    let gateway = Arc::new(Gateway {
        common,
        config_path: config_path.map(Path::to_path_buf),
        funding,
        pull: Arc::new(Mutex::new(())),
        manifests: Mutex::new(HashMap::new()),
        hosts: allowed_hosts(args.listen),
    });

    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("listen on {}", args.listen))?;
    println!("decdn gateway on http://{}/ (Ctrl-C to stop)", args.listen);
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

/// Create `dir` owner-only, as the buyer-pool store requires.
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

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// How the gateway pays.
enum Funding {
    /// A throwaway key funded through the onramp.
    Sponsored(Sponsor),
    /// The capability and keystore the flags name.
    Held,
}

/// The throwaway key and its onramp-issued capability, kept under `dir` the
/// way `decdn.sh` keeps them: `keystore.json`, `password`, `address`,
/// `capability` and `expiry`.
struct Sponsor {
    dir: PathBuf,
    onramp: String,
    http: reqwest::Client,
}

/// A usable allowance: the key and the capability issued to it.
#[derive(Clone)]
struct Allowance {
    keystore: PathBuf,
    password: PathBuf,
    token: String,
    expiry: u64,
}

/// The sponsored key's state, for the status page.
struct SponsorStatus {
    address: String,
    allowance: Option<Allowance>,
    fund_url: String,
}

#[derive(serde::Deserialize)]
struct CapabilityResponse {
    token: String,
    expiry: u64,
}

impl Sponsor {
    /// The key's address and its allowance. With none saved (or one near
    /// expiry, which rotates the key first), asks the onramp once.
    async fn status(&self) -> anyhow::Result<SponsorStatus> {
        let saved = self.saved_allowance();
        if let Some(allowance) = &saved
            && allowance.expiry <= now_secs().saturating_add(EXPIRY_MARGIN_SECS)
        {
            // A capability's terms are fixed once issued: renewal is a new key.
            self.rotate();
        }
        let address = self.ensure_key()?;
        let fund_url = format!("{}/fund?client={address}", self.onramp);
        if let Some(allowance) = self
            .saved_allowance()
            .filter(|a| a.expiry > now_secs().saturating_add(EXPIRY_MARGIN_SECS))
        {
            return Ok(SponsorStatus {
                address,
                allowance: Some(allowance),
                fund_url,
            });
        }
        let allowance = match self.poll_onramp(&address).await {
            Ok(Some(cap)) => {
                write_private(&self.dir.join("capability"), &cap.token)?;
                write_private(&self.dir.join("expiry"), &cap.expiry.to_string())?;
                self.saved_allowance()
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(error = %e, "onramp capability poll failed");
                None
            }
        };
        Ok(SponsorStatus {
            address,
            allowance,
            fund_url,
        })
    }

    fn saved_allowance(&self) -> Option<Allowance> {
        let token = std::fs::read_to_string(self.dir.join("capability")).ok()?;
        let expiry = std::fs::read_to_string(self.dir.join("expiry"))
            .ok()?
            .trim()
            .parse()
            .ok()?;
        Some(Allowance {
            keystore: self.dir.join("keystore.json"),
            password: self.dir.join("password"),
            token: token.trim().to_owned(),
            expiry,
        })
    }

    /// The throwaway key's address, generating the key and its random
    /// password first when either is missing.
    fn ensure_key(&self) -> anyhow::Result<String> {
        if let Ok(address) = std::fs::read_to_string(self.dir.join("address"))
            && self.dir.join("keystore.json").is_file()
            && self.dir.join("password").is_file()
        {
            return Ok(address.trim().to_owned());
        }
        self.rotate();
        private_dir(&self.dir)?;
        let password_file = self.dir.join("password");
        let password = alloy::hex::encode(rand::random::<[u8; 32]>());
        write_private(&password_file, &password)?;
        super::key_gen::key_gen(&KeyGenArgs {
            output_dir: Some(self.dir.clone()),
            force: true,
            keystore_password_file: Some(password_file),
        })?;
        let address = load_signer(&self.dir.join("keystore.json"), &password)?
            .address()
            .to_string();
        write_private(&self.dir.join("address"), &address)?;
        Ok(address)
    }

    /// Drop the key and its allowance; the next status makes a fresh key.
    fn rotate(&self) {
        if self.dir.exists()
            && let Err(e) = std::fs::remove_dir_all(&self.dir)
        {
            tracing::warn!(error = %e, dir = %self.dir.display(), "drop the spent key");
        }
    }

    async fn poll_onramp(&self, address: &str) -> anyhow::Result<Option<CapabilityResponse>> {
        let url = format!("{}/v1/capability?client={address}", self.onramp);
        let response = self.http.get(&url).send().await?;
        match response.status() {
            reqwest::StatusCode::OK => Ok(Some(response.json().await?)),
            reqwest::StatusCode::NO_CONTENT | reqwest::StatusCode::TOO_MANY_REQUESTS => Ok(None),
            status => anyhow::bail!("onramp answered {status}"),
        }
    }
}

fn write_private(path: &Path, contents: &str) -> anyhow::Result<()> {
    std::fs::write(path, contents).with_context(|| format!("write {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("restrict {}", path.display()))?;
    }
    Ok(())
}

struct Gateway {
    common: ClientFetchArgs,
    config_path: Option<PathBuf>,
    funding: Funding,
    /// One pull at a time: every pull opens the one buyer-pool store.
    pull: Arc<Mutex<()>>,
    manifests: Mutex<HashMap<[u8; 32], BundleManifest>>,
    hosts: Vec<String>,
}

/// What a pull that ended badly means for the gateway.
enum PullError {
    /// The allowance cannot pay for it: a fresh key and capability can.
    Funding(anyhow::Error),
    Other(anyhow::Error),
}

impl Gateway {
    async fn handle(self: Arc<Self>, req: Request<Incoming>) -> Response<Body> {
        if req.method() != Method::GET && req.method() != Method::HEAD {
            return text(StatusCode::METHOD_NOT_ALLOWED, "GET only\n");
        }
        let host = req
            .headers()
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");
        if !self.hosts.iter().any(|allowed| allowed == host) {
            return text(StatusCode::FORBIDDEN, "unknown Host\n");
        }
        let target = req
            .uri()
            .path_and_query()
            .map_or("/", |pq| pq.as_str())
            .to_owned();
        match target.as_str() {
            "/" => self.status_page().await,
            "/_allowance" => self.allowance_json().await,
            "/favicon.ico" => text(StatusCode::NOT_FOUND, ""),
            _ => self.content(target.trim_start_matches('/')).await,
        }
    }

    /// The allowance to pay with, or `None` while the sponsored key has none.
    async fn allowance(&self) -> anyhow::Result<Option<Allowance>> {
        match &self.funding {
            Funding::Held => Ok(None),
            Funding::Sponsored(sponsor) => Ok(sponsor.status().await?.allowance),
        }
    }

    async fn content(self: &Arc<Self>, rest: &str) -> Response<Body> {
        let uri: DecdnUri = match format!("decdn:{rest}").parse() {
            Ok(uri) => uri,
            Err(e) => return text(StatusCode::BAD_REQUEST, &format!("{e}\n")),
        };
        let allowance = match self.allowance().await {
            Ok(allowance) => allowance,
            Err(e) => return text(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e:#}\n")),
        };
        if allowance.is_none() && matches!(self.funding, Funding::Sponsored(_)) {
            return html(
                StatusCode::PAYMENT_REQUIRED,
                "<p>No allowance yet. <a href=\"/\">Get one</a>, then reload this page.</p>",
            );
        }
        let result = match &uri.target {
            UriTarget::Blob => {
                self.stream(uri.hash, uri.namespace, allowance.as_ref(), None, None)
                    .await
            }
            UriTarget::BundleEntry(path) => {
                self.entry(uri.hash, uri.namespace, path, allowance.as_ref())
                    .await
            }
            UriTarget::Bundle => {
                self.index(uri.hash, uri.namespace, allowance.as_ref())
                    .await
            }
            _ => return text(StatusCode::BAD_REQUEST, "unsupported URI target\n"),
        };
        match result {
            Ok(response) => response,
            Err(PullError::Funding(e)) => {
                if let Funding::Sponsored(sponsor) = &self.funding {
                    sponsor.rotate();
                }
                html(
                    StatusCode::PAYMENT_REQUIRED,
                    &format!(
                        "<p>The allowance ran out: {}. <a href=\"/\">Get a new one</a>, then \
                         reload this page; the download resumes.</p>",
                        escape(&format!("{e:#}"))
                    ),
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
        allowance: Option<&Allowance>,
    ) -> Result<Response<Body>, PullError> {
        let manifest = self.manifest(manifest_hash, namespace, allowance).await?;
        let Some(entry) = manifest.entries.iter().find(|e| e.path == path) else {
            return Ok(text(
                StatusCode::NOT_FOUND,
                &format!("the bundle has no entry {path:?}\n"),
            ));
        };
        let hash = parse_hash(&entry.hash).map_err(PullError::Other)?;
        self.stream(hash, namespace, allowance, Some(path), entry.size)
            .await
    }

    /// An HTML index of a bundle's entries, each linking to its gateway URL.
    async fn index(
        self: &Arc<Self>,
        manifest_hash: [u8; 32],
        namespace: Option<u64>,
        allowance: Option<&Allowance>,
    ) -> Result<Response<Body>, PullError> {
        let manifest = self.manifest(manifest_hash, namespace, allowance).await?;
        let mut rows = String::new();
        for entry in &manifest.entries {
            let link = DecdnUri::new(
                manifest_hash,
                UriTarget::BundleEntry(entry.path.clone()),
                namespace,
            )
            .to_string();
            let href = link.trim_start_matches(decdn_client::uri::SCHEME);
            let size = entry.size.map_or_else(String::new, human_bytes);
            let _ = write!(
                rows,
                "<tr><td><a href=\"/{}\">{}</a></td><td>{size}</td></tr>",
                escape(href),
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

    /// The bundle manifest `hash`, read once and kept.
    async fn manifest(
        self: &Arc<Self>,
        hash: [u8; 32],
        namespace: Option<u64>,
        allowance: Option<&Allowance>,
    ) -> Result<BundleManifest, PullError> {
        if let Some(manifest) = self.manifests.lock().await.get(&hash) {
            return Ok(manifest.clone());
        }
        let (first, mut reader, done) = self.start_pull(hash, namespace, allowance).await?;
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
        let manifest = BundleManifest::parse(&bytes)
            .context("not a bundle manifest; drop the trailing `/` to fetch the raw blob")
            .map_err(PullError::Other)?;
        self.manifests.lock().await.insert(hash, manifest.clone());
        Ok(manifest)
    }

    /// A response streaming `hash`'s verified bytes. Errors that come before
    /// the first byte become the error status; a later one aborts the body.
    async fn stream(
        self: &Arc<Self>,
        hash: [u8; 32],
        namespace: Option<u64>,
        allowance: Option<&Allowance>,
        path: Option<&str>,
        size: Option<u64>,
    ) -> Result<Response<Body>, PullError> {
        let (first, reader, done) = self.start_pull(hash, namespace, allowance).await?;
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
            .map_or_else(|| decdn_hex(hash), str::to_owned);
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
        allowance: Option<&Allowance>,
    ) -> Result<(Bytes, DuplexStream, oneshot::Receiver<anyhow::Result<()>>), PullError> {
        let mut common = self.common.clone();
        if let Some(allowance) = allowance {
            common.keystore = Some(allowance.keystore.clone());
            common.keystore_password_file = Some(allowance.password.clone());
            common.capability = Some(allowance.token.clone());
            common.capability_file = None;
        }
        let args = FetchArgs {
            hash: decdn_hex(hash),
            output: PathBuf::from("-"),
            namespace,
            common,
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
                        runtime.block_on(fetch_into(&args, config_path.as_deref(), &mut sink))
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

    async fn status_page(&self) -> Response<Body> {
        let status = match &self.funding {
            Funding::Held => {
                "<p>Paying from the capability given on the command line.</p>".to_owned()
            }
            Funding::Sponsored(sponsor) => match sponsor.status().await {
                Ok(SponsorStatus {
                    address,
                    allowance: Some(allowance),
                    ..
                }) => format!(
                    "<p>Sponsored allowance for key <code>{}</code>, valid for {} more.</p>",
                    escape(&address),
                    human_secs(allowance.expiry.saturating_sub(now_secs()))
                ),
                Ok(SponsorStatus {
                    address, fund_url, ..
                }) => format!(
                    "<p>Key <code>{}</code> has no allowance yet.</p><p><a href=\"{}\" \
                     target=\"_blank\" rel=\"noopener\">Get an allowance</a>: pass the check \
                     on the page that opens, then come back here. This page updates by \
                     itself.</p>",
                    escape(&address),
                    escape(&fund_url)
                ),
                Err(e) => format!(
                    "<p>Allowance unavailable: {}</p>",
                    escape(&format!("{e:#}"))
                ),
            },
        };
        html(
            StatusCode::OK,
            &format!(
                "<h1>decdn gateway</h1>{status}\
                 <form onsubmit=\"location='/'+this.u.value.trim().replace(/^decdn:/,'');\
                 return false\"><input name=\"u\" size=\"80\" placeholder=\"decdn:&lt;hash&gt;\
                 [/&lt;path&gt;][?ns=&lt;id&gt;]\"> <button>Open</button></form>\
                 <script>let s=null;setInterval(async()=>{{const r=await fetch('/_allowance');\
                 const j=await r.json();if(s!==null&&s!==j.funded)location.reload();s=j.funded}},\
                 2000)</script>"
            ),
        )
    }

    async fn allowance_json(&self) -> Response<Body> {
        let json = match &self.funding {
            Funding::Held => serde_json::json!({ "funded": true }),
            Funding::Sponsored(sponsor) => match sponsor.status().await {
                Ok(status) => serde_json::json!({
                    "address": status.address,
                    "funded": status.allowance.is_some(),
                    "expiry": status.allowance.map(|a| a.expiry),
                    "fund_url": status.fund_url,
                }),
                Err(e) => serde_json::json!({ "funded": false, "error": format!("{e:#}") }),
            },
        };
        let mut response = Response::new(full(json.to_string()));
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
        response
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

fn decdn_hex(hash: [u8; 32]) -> String {
    alloy::hex::encode(hash)
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
    let mut response = Response::new(full(body.to_owned()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

fn html(status: StatusCode, body: &str) -> Response<Body> {
    let page = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" \
         content=\"width=device-width\"><title>decdn gateway</title><style>body{{font-family:\
         system-ui,sans-serif;max-width:48rem;margin:3rem auto;padding:0 1rem}}td{{padding:\
         .2rem 1rem .2rem 0}}</style></head><body>{body}</body></html>"
    );
    let mut response = Response::new(full(page));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/html; charset=utf-8"),
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

fn human_secs(secs: u64) -> String {
    let hours = secs / 3600;
    let (n, unit) = if hours >= 24 {
        (hours / 24, "day")
    } else if hours > 0 {
        (hours, "hour")
    } else {
        (secs / 60, "minute")
    };
    if n == 1 {
        format!("1 {unit}")
    } else {
        format!("{n} {unit}s")
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
