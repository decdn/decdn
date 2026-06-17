//! Arguments for the `decdn node` subcommand group.
//!
//! `node` is the operator-local admin namespace: subcommands talk to the
//! loopback admin HTTP surface (ADR 025) exposed by a running node.

use std::num::NonZeroU64;
use std::path::PathBuf;

use clap::{Args, Subcommand};

/// Default `--wait-timeout-secs`. Wrapped here as a `const` so the
/// `NonZeroU64` constructor can be evaluated at compile time.
const DEFAULT_WAIT_TIMEOUT_SECS: NonZeroU64 = match NonZeroU64::new(30) {
    Some(v) => v,
    None => unreachable!(),
};

/// Default `--wait-poll-ms`. Wrapped here as a `const` for the same
/// reason as [`DEFAULT_WAIT_TIMEOUT_SECS`].
const DEFAULT_WAIT_POLL_MS: NonZeroU64 = match NonZeroU64::new(250) {
    Some(v) => v,
    None => unreachable!(),
};

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
    /// Print a snapshot of the running node's DHT participation health
    /// via `admin_v1_status` (issue #741): Kademlia routing-table bucket
    /// fill rates, the network-wide last bucket-refresh time, the
    /// active-staker count, provider-record store utilization, and
    /// republish-scheduler depth. Lets operators diagnose cold-start or
    /// routing-table degradation without scraping Prometheus or reading
    /// logs.
    Status(StatusArgs),
    /// Print a live snapshot of the running node's open payment channels
    /// via `admin_v1_channels` (issue #749): per channel the last-accepted
    /// nonce, outstanding accrued claim (micro-USDC), escrowed deposit,
    /// time since the last voucher, and whether the accrued claim has
    /// reached the configured redemption threshold. Lets operators spot
    /// channels approaching settlement, stale channels, or unusually high
    /// outstanding balances before they become a liquidity risk — without
    /// scraping metrics or reading logs. Same admin-URL resolution and
    /// timeout semantics as `decdn node health`.
    Channels(ChannelsArgs),
    /// Show cumulative per-region bandwidth (bytes in/out) from the running
    /// node via `admin_v1_regionStats` (issue #750). Same admin-URL
    /// resolution and timeout semantics as `decdn node health`.
    #[command(name = "region-stats")]
    RegionStats(RegionStatsArgs),
    /// Show a peer's network reputation score + per-region coverage from the
    /// running node via `admin_v1_reputation` (#326, ADR 008). Same admin-URL
    /// resolution and timeout semantics as `decdn node health`.
    Reputation(ReputationArgs),
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
    /// race. Requires the node to have been started with `decdn-node run
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
    /// Render a live (every `--interval-ms`) view of node activity by
    /// scraping the running node's `/metrics` HTTP endpoint (issue
    /// #275). Shows active streams, cache hit rate, and cache bytes
    /// returned — the operator-visible signal the issue calls "bytes
    /// served" maps to `decdn_cache_bytes_returned_total` until the
    /// network-side `cdn/client/v1` egress counter (#317) lands.
    Top(TopArgs),
    /// Register this node on-chain via `CapacityBond.registerNode` (ADR 019
    /// § Step 2.3). Loads the local iroh node key and Ethereum keystore,
    /// builds the EIP-712 `bindingSignature` and the ed25519 ownership
    /// signature locally, and submits the registration transaction. This
    /// is the only `node` subcommand that talks to the chain rather than a
    /// running node's admin RPC.
    ///
    /// Phase 2.1/2.2 (`approve` + `bond` + `declareMbps`) are a
    /// precondition: the contract reverts if the operator's bond does not
    /// cover `minBond` / the declared-capacity curve. Pass `--dry-run` to
    /// print the parameters and signatures without submitting.
    Register(RegisterArgs),
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

/// `decdn node status` — report DHT participation health via
/// `admin_v1_status` (issue #741). Same admin-URL resolution and timeout
/// semantics as `decdn node health`.
#[derive(Args, Debug)]
pub struct StatusArgs {
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

    /// Emit the admin response body as JSON instead of the human-readable
    /// summary + bucket table.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}

/// `decdn node channels` — list open payment channels via
/// `admin_v1_channels` (issue #749). Same admin-URL resolution and
/// timeout semantics as `decdn node health`.
#[derive(Args, Debug)]
pub struct ChannelsArgs {
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

    /// Emit the admin response body as JSON instead of the human-readable
    /// summary + channel table.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}

/// `decdn node region-stats` — show cumulative per-region bandwidth via
/// `admin_v1_regionStats` (issue #750). Same admin-URL resolution and
/// timeout semantics as `decdn node health`.
#[derive(Args, Debug)]
pub struct RegionStatsArgs {
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

    /// Emit the admin response body as JSON instead of the human-readable
    /// per-region table.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}

/// `decdn node evict` — forcibly remove a blob from the local cache via
/// `admin_v1_evict` (issue #279).
///
/// The eviction is *logical*: the iroh-blobs store still holds the bytes
/// until the next periodic GC sweep reclaims them (#518; cadence controlled
/// by `cache.gc_interval_sec`). The takedown is persisted to
/// `<cache_dir>/evicted.log` so it survives restarts even if the next sweep
/// hasn't run yet.
/// Operators using this for DMCA takedowns can rely on the takedown
/// being durable across `decdn-node run` invocations.
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

/// `decdn node reputation <node-id>` — report a peer's network reputation
/// score + regional coverage via `admin_v1_reputation` (#326).
#[derive(Args, Debug)]
pub struct ReputationArgs {
    /// Hex `NodeId` (64 hex chars) of the peer to query. Optional `0x` / `0X`
    /// prefix is tolerated; mixed case is accepted.
    #[arg(value_name = "NODE_ID")]
    pub node_id: String,

    /// Base URL of the node's admin HTTP surface. See `health --admin-url`
    /// for resolution precedence (flag → env → config → default).
    #[arg(long, value_name = "URL", env = "DECDN_ADMIN_URL")]
    pub admin_url: Option<String>,

    /// Path to the TOML config file used to derive the admin URL when
    /// `--admin-url` / `DECDN_ADMIN_URL` are unset.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Emit the admin response body as JSON instead of the human-readable form.
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
/// path as SIGTERM without needing the process PID.
///
/// Without `--wait`, the command is fire-and-forget: it returns
/// `drain_initiated=true` as soon as the trigger lands and the admin
/// server begins its early-stop sequence (metrics, then admin, both
/// before `router.shutdown`). The connection will close before the
/// node fully exits — observe completion via process exit
/// (systemd/K8s) or `decdn node health` until ECONNREFUSED.
///
/// Pass `--wait` (issue #604) to opt the runtime into keeping the
/// admin server alive *through* `router.shutdown` and have the CLI
/// poll `admin_v1_health.in_flight_streams` until it reaches 0 (or
/// the wait budget expires). On a clean drain the command returns
/// success once the count is zero; on overrun it prints
/// `drain_timeout=true in_flight_streams=N` to stderr and exits
/// non-zero so operator scripts can fail closed during a stuck
/// rolling upgrade.
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

    /// Roundtrip timeout in milliseconds for each individual RPC
    /// (the initial `drain` call and each poll under `--wait`).
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,

    /// Block until all in-flight client streams complete (or the wait
    /// budget expires) before returning. Opt-in seam for #604: when set,
    /// the server keeps admin alive through `router.shutdown` so the CLI
    /// can poll `admin_v1_health.in_flight_streams` to observe
    /// completion. Without it, the original SIGTERM-equivalent
    /// ordering applies.
    #[arg(long)]
    pub wait: bool,

    /// Wall-clock budget (seconds) for the `--wait` polling loop. On
    /// overrun the CLI exits non-zero and prints `drain_timeout=true`.
    /// Ignored without `--wait`. Default 30s covers the runtime's 15s
    /// `SHUTDOWN_DEADLINE` plus typical settle time. `NonZeroU64` so
    /// clap rejects `0` at parse time — a zero-second budget would
    /// deadline-overrun on the first iteration with no signal of why.
    #[arg(long, value_name = "SECS", default_value_t = DEFAULT_WAIT_TIMEOUT_SECS)]
    pub wait_timeout_secs: NonZeroU64,

    /// Cadence (milliseconds) at which `--wait` polls
    /// `admin_v1_health`. Lower values converge faster on short
    /// drains; higher values reduce admin churn on long ones. Ignored
    /// without `--wait`. `NonZeroU64` so clap rejects `0` at parse
    /// time — a zero-millisecond poll interval would spin a busy loop
    /// against the loopback admin port.
    #[arg(long, value_name = "MS", default_value_t = DEFAULT_WAIT_POLL_MS)]
    pub wait_poll_ms: NonZeroU64,
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

/// `decdn node register` — submit `CapacityBond.registerNode` (ADR 019
/// § Step 2.3).
///
/// Unlike the other `node` subcommands this performs an on-chain
/// transaction, so it needs the blockchain coordinates (`rpc_url`,
/// `chain_id`, `capacity_bond_address`) and the operator's keys rather than
/// an admin URL. Each is taken from a flag when present, otherwise from the
/// `[blockchain]` / `[identity]` tables of the TOML config (same file the
/// daemon reads). `rpc_url` and `capacity_bond_address` are required (no
/// default — registration errors if neither flag nor config supplies them);
/// `chain_id`, `keystore`, and `data_dir` fall back to built-in defaults
/// (see each field).
#[derive(Args, Debug)]
pub struct RegisterArgs {
    /// ISO 3166-1 alpha-2 country code submitted as the on-chain
    /// `regionHint` (ADR 030). Self-reported, accepted at face value.
    #[arg(long, value_name = "CODE")]
    pub region: String,

    /// QUIC multiaddr to register, e.g.
    /// `/ip4/203.0.113.10/udp/4433/quic-v1`. Repeatable. NAT'd nodes may
    /// register a relay placeholder and promote direct addresses later via
    /// `updateMultiaddrs`; omitting it entirely registers an empty set and
    /// relies on gossip / iroh discovery for reachability.
    #[arg(long = "multiaddr", value_name = "MA")]
    pub multiaddrs: Vec<String>,

    /// Path to the TOML config file supplying `[blockchain]` / `[identity]`
    /// fields not passed as flags. Takes precedence over the top-level
    /// `decdn --config`; falls through to `~/.decdn/node.toml`.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// JSON-RPC endpoint URL. Overrides `blockchain.rpc_url`.
    #[arg(long, value_name = "URL")]
    pub rpc_url: Option<String>,

    /// `CapacityBond` contract address. Overrides
    /// `blockchain.capacity_bond_address`.
    #[arg(long, value_name = "ADDR")]
    pub capacity_bond_address: Option<String>,

    /// EIP-712 `chainId` for the binding-signature domain and the ed25519
    /// ownership digest. Overrides `blockchain.chain_id`; must match the
    /// `CapacityBond` deployment chain or the signatures are rejected
    /// on-chain.
    #[arg(long, value_name = "ID")]
    pub chain_id: Option<u64>,

    /// Ethereum keystore file. Overrides `blockchain.eth_keystore`; defaults
    /// to `<data_dir>/keystore.json`.
    #[arg(long, value_name = "PATH")]
    pub keystore: Option<PathBuf>,

    /// Data directory holding `node.secret`. Overrides `identity.data_dir`;
    /// defaults to `~/.decdn`.
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,

    /// File whose contents are the keystore password. Consulted after the
    /// `DECDN_KEYSTORE_PASSWORD` env var and before an interactive prompt.
    #[arg(long, value_name = "PATH", env = "DECDN_KEYSTORE_PASSWORD_FILE")]
    pub keystore_password_file: Option<PathBuf>,

    /// Build and print the registration parameters and signatures without
    /// submitting the transaction.
    #[arg(long)]
    pub dry_run: bool,

    /// Emit the result as JSON instead of human-readable `key=value` lines.
    #[arg(long)]
    pub json: bool,
}

/// `decdn node top` — live metrics view (issue #275).
///
/// Polls the daemon's loopback `/metrics` HTTP endpoint every
/// `--interval-ms` and redraws a fixed-column table with current
/// gauges, per-second counter deltas, and a derived cache hit-rate.
/// The metrics port is loopback-only by default
/// (`observability.metrics_bind = 127.0.0.1`); operators who exposed
/// it elsewhere should pass `--metrics-url`.
#[derive(Args, Debug)]
pub struct TopArgs {
    /// Base URL of the node's metrics HTTP endpoint, e.g.
    /// `http://127.0.0.1:9090`. Also read from `DECDN_METRICS_URL`
    /// when unset. If still unset, the port is derived from
    /// `observability.metrics_port` in the config file (see
    /// `--config`); the host is always `127.0.0.1` because the
    /// metrics endpoint is loopback-only by default.
    #[arg(long, value_name = "URL", env = "DECDN_METRICS_URL")]
    pub metrics_url: Option<String>,

    /// Path to the TOML config file used to derive the metrics URL
    /// when `--metrics-url` / `DECDN_METRICS_URL` are unset. Same
    /// resolution semantics as `decdn node peers --config`.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Refresh interval in milliseconds. Default 1000 (one Hz, matches
    /// the issue title). Values below 100ms are accepted but unlikely
    /// to outpace the operator's terminal redraw budget.
    #[arg(long, value_name = "MS", default_value_t = 1_000)]
    pub interval_ms: u64,

    /// Single-shot mode: fetch once, emit the snapshot as pretty JSON,
    /// and exit. Useful for scripts or one-off captures. Mutually
    /// exclusive with the live redraw loop (which runs forever in
    /// plain mode until Ctrl-C).
    #[arg(long)]
    pub json: bool,

    /// HTTP roundtrip timeout in milliseconds for each `/metrics`
    /// scrape. A scrape that times out is logged and the loop
    /// continues — the displayed counters keep their last value
    /// rather than the table going blank on a single hiccup.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}
