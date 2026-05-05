//! Arguments for the `decdn node` subcommand group.
//!
//! `node` is the operator-local admin namespace: subcommands talk to the
//! loopback admin HTTP surface (ADR 025) exposed by a running node.

use std::path::PathBuf;

use clap::{Args, Subcommand};

/// Operator-local admin commands that query a running deCDN node.
#[derive(Args, Debug)]
pub struct NodeArgs {
    #[command(subcommand)]
    pub cmd: NodeCommand,
}

/// Subcommands under `decdn node`.
#[derive(Subcommand, Debug)]
pub enum NodeCommand {
    /// List gossip peers currently known to a running node.
    Peers(PeersArgs),
    /// Print a running node's identity and process uptime.
    Health(HealthArgs),
    /// Forcibly remove a single blob from the local cache (issue #279).
    /// Useful for DMCA takedown, corruption recovery, and storage
    /// reclamation.
    Evict(EvictArgs),
    /// Publish a one-shot `NodeAnnounce` to gossip peers immediately
    /// rather than waiting for the periodic announce interval (issue
    /// #280). Useful after a config edit changes a field carried in the
    /// announce body — see `decdn-protocol::NodeAnnounceBody` — or as a
    /// post-restart "I'm here" nudge so peers don't wait the full
    /// `announce_interval_sec` to learn about us. The trigger is a
    /// queue-and-coalesce signal: rapid back-to-back invocations within a
    /// single publisher cycle fold into one extra broadcast (see
    /// `decdn-gossip::AnnounceTrigger`), and a successful response means
    /// the request was queued, not that it has hit the wire.
    Announce(AnnounceArgs),
    /// Re-read the running node's config file and apply hot-reloadable
    /// fields (issue #373). Equivalent to `kill -HUP <pid>` but goes
    /// through the loopback admin surface, so operator tooling that
    /// already speaks JSON-RPC doesn't need to also know which PID to
    /// signal. Currently `payment.rate_per_mb` and
    /// `observability.log_level` are reloadable; other fields are logged
    /// as ignored. Both paths share the same internal mutex, so a
    /// concurrent SIGHUP and `decdn node reload` queue rather than
    /// race. Requires the node to have been started with `decdn run
    /// --config <path>` — without a path on disk there's nothing to
    /// re-read.
    Reload(ReloadArgs),
    /// Trigger graceful shutdown of the running node via `admin_v1_drain`
    /// (issue #244, ADR 025). Equivalent to `kill -TERM <pid>` but goes
    /// through the loopback admin surface, so operator tooling that already
    /// speaks JSON-RPC doesn't need to also know which PID to signal. The
    /// runtime begins the same graceful shutdown sequence SIGTERM triggers
    /// (stops accept loops, waits for in-flight transfers, flushes cache).
    ///
    /// **Fire-and-forget semantics**: this command returns `drain_initiated=true`
    /// as soon as the trigger is queued. The response does *not* mean shutdown
    /// is complete — it means the runtime has been asked to shut down, and
    /// the connection may close before the node fully exits because the
    /// admin server is one of the first surfaces to stop. Observe completion
    /// via process exit (systemd/K8s will notice) or by polling `decdn
    /// node health` until the connection is refused.
    Drain(DrainArgs),
}

/// `decdn node health` — report identity (hex `node_id`) and process
/// uptime via `admin_v1_health`.
#[derive(Args, Debug)]
pub struct HealthArgs {
    /// Base URL of the node's admin HTTP surface.
    ///
    /// Also read from `DECDN_ADMIN_URL` when unset; clap folds the env
    /// var into this field. If still unset, the admin port is derived
    /// from `observability.admin_port` in the config file (see
    /// `--config`). Example: `http://127.0.0.1:9191`.
    #[arg(long, value_name = "URL", env = "DECDN_ADMIN_URL")]
    pub admin_url: Option<String>,

    /// Path to the TOML config file used to derive the admin URL when
    /// `--admin-url` / `DECDN_ADMIN_URL` are unset. Takes precedence
    /// over the top-level `decdn --config`; if neither is set,
    /// resolution falls through to `~/.decdn/node.toml` and then the
    /// built-in default port.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Emit the admin response body as JSON instead of two human-
    /// readable lines.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}

/// `decdn node evict` — forcibly remove a blob from the local cache via
/// `admin_v1_evict` (issue #279).
///
/// The eviction is *logical* in this version (the iroh-blobs store still
/// holds the bytes — see issue #233 for the disk-reclaim follow-up) but
/// is persisted to `<cache_dir>/evicted.log` so it survives restarts.
/// Operators using this for DMCA takedowns can rely on the takedown
/// being durable across `decdn run` invocations.
///
/// Pass `--dry-run` (issue #379) to preview what the evict would touch
/// — blob size, last-access elapsed time, pin status, and already-
/// evicted flag — without mutating any cache state. Useful as a
/// pre-flight check before a DMCA takedown so operators can confirm
/// the right blob and notice cases like "this hash is pinned" or
/// "this is an idempotent re-run".
#[derive(Args, Debug)]
pub struct EvictArgs {
    /// BLAKE3 hash of the blob to evict, encoded as 64 hex characters.
    /// Optional `0x` / `0X` prefix is tolerated; mixed case is accepted.
    /// Operators paste this from access logs / takedown notices.
    #[arg(value_name = "HASH")]
    pub hash: String,

    /// Report what *would* be evicted (blob size, last-access elapsed
    /// time, pin status, already-evicted flag) without mutating cache
    /// state (issue #379). Use this as a pre-flight check before
    /// running the real takedown — operators want to confirm the right
    /// blob and spot cases like "this hash is pinned" or "this is an
    /// idempotent re-run" before committing to the durable eviction.
    #[arg(long)]
    pub dry_run: bool,

    /// Base URL of the node's admin HTTP surface. See `health --admin-url`
    /// for resolution precedence (flag → env → config → default).
    #[arg(long, value_name = "URL", env = "DECDN_ADMIN_URL")]
    pub admin_url: Option<String>,

    /// Path to the TOML config file used to derive the admin URL when
    /// `--admin-url` / `DECDN_ADMIN_URL` are unset.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Emit the admin response body as JSON instead of a human-readable line.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}

/// `decdn node announce` — publish a one-shot `NodeAnnounce` to gossip
/// peers via `admin_v1_announce` (issue #280).
#[derive(Args, Debug)]
pub struct AnnounceArgs {
    /// Base URL of the node's admin HTTP surface. See `health --admin-url`
    /// for resolution precedence.
    #[arg(long, value_name = "URL", env = "DECDN_ADMIN_URL")]
    pub admin_url: Option<String>,

    /// Path to the TOML config file used to derive the admin URL when
    /// `--admin-url` / `DECDN_ADMIN_URL` are unset.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Emit the admin response as JSON instead of a one-line confirmation.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}

/// `decdn node reload` — re-read the running node's config file via
/// `admin_v1_reload` (issue #373) and print the post-reload `rate_per_mb`
/// and `log_level`.
#[derive(Args, Debug)]
pub struct ReloadArgs {
    /// Base URL of the node's admin HTTP surface. See `health --admin-url`
    /// for resolution precedence (flag → env → config → default).
    #[arg(long, value_name = "URL", env = "DECDN_ADMIN_URL")]
    pub admin_url: Option<String>,

    /// Path to the TOML config file used to derive the admin URL when
    /// `--admin-url` / `DECDN_ADMIN_URL` are unset. Note: this argument
    /// only locates the *running node's admin port*; the node re-reads
    /// the path it was started with, not this one — passing a different
    /// file here will not redirect the reload.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Emit the admin response as JSON instead of two human-readable lines.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}

/// `decdn node drain` — trigger graceful shutdown of the running node via
/// `admin_v1_drain` (issue #244, ADR 025). Fires the same runtime shutdown
/// path as SIGTERM without needing the process PID. The response
/// (`drain_initiated=true`) means "shutdown has been requested", not that
/// it has completed — the admin server is one of the first surfaces to
/// stop (metrics is signalled first, then admin, both before
/// `router.shutdown`), so the connection will close before the node
/// fully exits.
#[derive(Args, Debug)]
pub struct DrainArgs {
    /// Base URL of the node's admin HTTP surface. See `health --admin-url`
    /// for resolution precedence (flag → env → config → default).
    #[arg(long, value_name = "URL", env = "DECDN_ADMIN_URL")]
    pub admin_url: Option<String>,

    /// Path to the TOML config file used to derive the admin URL when
    /// `--admin-url` / `DECDN_ADMIN_URL` are unset.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Emit the admin response as JSON instead of a one-line confirmation.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}

/// `decdn node peers` — list the gossip peer table of a running node.
#[derive(Args, Debug)]
pub struct PeersArgs {
    /// Base URL of the node's admin HTTP surface.
    ///
    /// Also read from `DECDN_ADMIN_URL` when unset; clap folds the env
    /// var into this field. If still unset, the admin port is derived
    /// from `observability.admin_port` in the config file (see
    /// `--config`). Example: `http://127.0.0.1:9191`.
    #[arg(long, value_name = "URL", env = "DECDN_ADMIN_URL")]
    pub admin_url: Option<String>,

    /// Path to the TOML config file used to derive the admin URL when
    /// `--admin-url` / `DECDN_ADMIN_URL` are unset. Takes precedence
    /// over the top-level `decdn --config`; if neither is set,
    /// resolution falls through to `~/.decdn/node.toml` and then the
    /// built-in default port.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Filter output to peers whose region matches exactly
    /// (case-insensitive). Applied client-side after fetching.
    #[arg(long, value_name = "CODE")]
    pub region: Option<String>,

    /// Emit the admin response body as JSON instead of a human table.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}
