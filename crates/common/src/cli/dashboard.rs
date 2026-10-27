//! Arguments for the `decdn dashboard` subcommand.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Args;
use url::Url;

use super::gateway::parse_loopback_addr;

/// Serve one page, on a loopback address, of this host's `decdn get` pulls,
/// its gateway and its node.
#[derive(Args, Debug, Clone)]
pub struct DashboardArgs {
    /// Loopback address the page answers on.
    #[arg(long, value_name = "ADDR", default_value = "127.0.0.1:8485", value_parser = parse_loopback_addr)]
    pub listen: SocketAddr,

    /// Base URL of the gateway whose `GET /_status` the page shows: a
    /// loopback gateway's own address, or any gateway's `--metrics-listen`.
    #[arg(long, value_name = "URL", default_value = "http://127.0.0.1:8484")]
    pub gateway: Url,

    /// File holding the gateway token that a loopback gateway's `/_status`
    /// asks for, read before each ask. Defaults to `~/.decdn/gateway/token`,
    /// where no file sends no token, as a public gateway's `/_status` takes
    /// none; a named file that cannot be read is an error. The token goes to
    /// a loopback `--gateway` only.
    #[arg(long, value_name = "PATH")]
    pub gateway_token_file: Option<PathBuf>,

    /// Base URL of the node's admin HTTP surface. Also read from
    /// `DECDN_ADMIN_URL`; if still unset, derived from
    /// `observability.admin_port` in the config file, as
    /// `decdn node status` does.
    #[arg(long, value_name = "URL", env = "DECDN_ADMIN_URL")]
    pub admin_url: Option<String>,

    /// Base URL of the node's metrics endpoint. Also read from
    /// `DECDN_METRICS_URL`; if still unset, derived from
    /// `observability.metrics_port` in the config file, as `decdn node top`
    /// does.
    #[arg(long, value_name = "URL", env = "DECDN_METRICS_URL")]
    pub metrics_url: Option<String>,

    /// The node's TOML config file, read for its admin and metrics ports when
    /// no URL names them. Takes precedence over the top-level
    /// `decdn --config`; if neither is set, `~/.decdn/node.toml`, then the
    /// built-in default ports.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Where the dashboard keeps its token. Defaults to `~/.decdn/dashboard`.
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,
}
