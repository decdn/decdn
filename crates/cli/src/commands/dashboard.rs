//! `decdn dashboard`: one page, on a loopback address, of what this host
//! does with deCDN.
//!
//! The page shows three sources, each only as far as it answers:
//!
//! - the `decdn get` runs on the host, from the records each run writes
//!   (`activity`);
//! - the gateway at `--gateway`, from its `GET /_status`;
//! - the node at `--admin-url` and `--metrics-url`, from its admin RPC and
//!   its `/metrics`.
//!
//! The server reads every source itself (`sources`) and answers the page's
//! one same-origin `GET /_status` from what it last read. The page so reads
//! nothing from another origin: the gateway grants no cross-origin read, and
//! the node's unauthenticated admin port never meets a browser.
//!
//! `/_status` takes the dashboard's own token, in `~/.decdn/dashboard/token`:
//! the page shows node earnings and pool balances that another local user
//! must not read. The link the command prints hands the token to the page in
//! its fragment. A `Host` other than the listener's own loopback names is
//! refused, so a page whose name was rebound to this address reads nothing.
//!
//! The dashboard only reads: no route changes a source.

use std::convert::Infallible;
use std::path::Path;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use decdn_common::cli::{self, DashboardArgs};
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use super::local_http::{
    Body, allowed_hosts, carries_token, host_header, json, load_or_create_token, private_dir, text,
};
use super::node::resolve_admin_url;
use super::node_top::{append_metrics_path, resolve_metrics_url};

mod embed;
mod sources;
mod status;

use sources::{Sources, Targets};
use status::Status;

/// Run the dashboard until Ctrl-C.
///
/// # Errors
///
/// The node's URLs cannot be resolved, the token cannot be read or made, or
/// the listener cannot bind.
pub async fn dashboard(args: &DashboardArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let config_path = args.config.as_deref().or(config_path);
    let admin_url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;
    let metrics_url = append_metrics_path(&resolve_metrics_url(
        args.metrics_url.as_deref(),
        config_path,
    )?);
    let root = args
        .data_dir
        .clone()
        .unwrap_or_else(|| cli::common::expand_tilde(Path::new("~/.decdn/dashboard")));
    private_dir(&root)?;
    let token = load_or_create_token(&root.join("token"))?;
    let token_file = args
        .gateway_token_file
        .clone()
        .unwrap_or_else(|| cli::common::expand_tilde(Path::new("~/.decdn/gateway/token")));
    let targets = Targets {
        pulls_dir: super::activity::pulls_dir(),
        gateway: args.gateway.clone(),
        gateway_token_file: token_file,
        admin_url,
        metrics_url,
    };

    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("listen on {}", args.listen))?;
    announce(args, &root, &token, &targets);
    let sources = Arc::new(Sources::new(&targets));
    sources::spawn(&sources, targets)?;
    let dashboard = Arc::new(Dashboard {
        token,
        hosts: allowed_hosts(listener.local_addr().unwrap_or(args.listen)),
        sources,
        started_unix: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
    });
    serve(&dashboard, &listener).await
}

/// Print where the page answers, with the link that hands it the token, and
/// where it looks for each source.
fn announce(args: &DashboardArgs, root: &Path, token: &str, targets: &Targets) {
    println!(
        "decdn dashboard on http://{listen}/#token={token} (Ctrl-C to stop)\n\
         dashboard token: {token} (also in {file})\n\
         pulls: {pulls}\n\
         gateway: {gateway}\n\
         node: {admin} (admin), {metrics}",
        listen = args.listen,
        file = root.join("token").display(),
        pulls = targets.pulls_dir.as_deref().map_or_else(
            || "(no home directory)".to_owned(),
            |d| d.display().to_string()
        ),
        gateway = targets.gateway,
        admin = targets.admin_url,
        metrics = targets.metrics_url,
    );
    if !embed::built() {
        println!("this binary has no dashboard built in; the page at / says how to build it");
    }
}

/// The running dashboard.
struct Dashboard {
    token: String,
    /// The `Host` values the listener answers.
    hosts: Vec<String>,
    sources: Arc<Sources>,
    started_unix: u64,
}

impl Dashboard {
    /// Answer one request.
    fn answer(&self, method: &Method, path: &str, headers: &HeaderMap) -> Response<Body> {
        if !self
            .hosts
            .iter()
            .any(|allowed| allowed == host_header(headers))
        {
            return text(StatusCode::FORBIDDEN, "unknown Host\n");
        }
        if method != Method::GET {
            return text(StatusCode::NOT_FOUND, "GET /, /_status\n");
        }
        match path {
            "/_status" if carries_token(headers, &self.token) => {
                json(StatusCode::OK, &self.status())
            }
            "/_status" => text(StatusCode::UNAUTHORIZED, "token required\n"),
            "/" => embed::page(),
            path => embed::asset(path)
                .unwrap_or_else(|| text(StatusCode::NOT_FOUND, "GET /, /_status\n")),
        }
    }

    /// What every source last showed.
    fn status(&self) -> Status {
        Status {
            version: env!("CARGO_PKG_VERSION"),
            dashboard_built: embed::built(),
            started_unix: self.started_unix,
            pulls: snapshot(&self.sources.pulls),
            gateway: snapshot(&self.sources.gateway),
            node: snapshot(&self.sources.node),
        }
    }
}

fn snapshot<T: Clone>(lock: &RwLock<T>) -> T {
    lock.read().unwrap_or_else(PoisonError::into_inner).clone()
}

/// Accept connections on `listener` until Ctrl-C.
async fn serve(dashboard: &Arc<Dashboard>, listener: &TcpListener) -> anyhow::Result<()> {
    loop {
        let (stream, _) = tokio::select! {
            accepted = listener.accept() => accepted.context("accept a connection")?,
            _ = tokio::signal::ctrl_c() => return Ok(()),
        };
        let dashboard = Arc::clone(dashboard);
        tokio::spawn(async move {
            let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                let response = dashboard.answer(req.method(), req.uri().path(), req.headers());
                async move { Ok::<_, Infallible>(response) }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(Duration::from_secs(10))
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                tracing::debug!(error = %e, "dashboard connection ended");
            }
        });
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests;
