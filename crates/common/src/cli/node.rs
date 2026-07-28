//! Arguments for the `decdn node` subcommand group.
//!
//! `node` is the operator-local admin namespace: subcommands talk to the
//! loopback admin HTTP surface (ADR 025) exposed by a running node.

use std::num::NonZeroU64;
use std::path::PathBuf;

use clap::{ArgGroup, Args, Subcommand};

use super::common::CommonChainArgs;

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

/// Accepted `--swap-venue` values. A `ValueEnum` so clap rejects typos at
/// parse time and `--help` lists the supported venues. The TOML config
/// `swap_venue` string is validated later when the venue is constructed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum SwapVenueArg {
    #[value(name = "uniswap-v3")]
    UniswapV3,
    #[value(name = "balancer-v3")]
    BalancerV3,
}

impl SwapVenueArg {
    /// Canonical wire string consumed by `resolve_swap`/`from_config`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UniswapV3 => "uniswap-v3",
            Self::BalancerV3 => "balancer-v3",
        }
    }
}

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
    /// Stake to a capacity tier on-chain via `CapacityBond.bond` +
    /// `declareMbps` (ADR 019 § Step 2.1–2.2). Idempotent: tops the active
    /// bond up to `max(minBond, bondRequired(mbps))`, approving and bonding
    /// only the shortfall, so re-running after a partial failure converges
    /// rather than over-bonding. Run this before `decdn node register`. Pass
    /// `--dry-run` to print the plan without submitting.
    Bond(BondArgs),
    /// Lower the on-chain bond via `CapacityBond.requestUnbond` + `unbond`
    /// (ADR 026 § Capacity-bond curve) — the reverse of `decdn node bond`.
    /// State-aware: with no request in flight it declares the tier down (if
    /// needed) and starts the unbonding window; while one is maturing it
    /// reports the unlock time and exits non-zero; once matured it withdraws.
    /// Note that a request in flight makes the node INACTIVE for the whole
    /// window. Pass `--dry-run` to print the plan without submitting.
    Unbond(UnbondArgs),
    /// Leave the active node set on-chain via `CapacityBond.deregisterNode`
    /// (ADR 003 § Node Registry) — the reverse of `decdn node register`, and
    /// the first leg of a full bond exit. Clears the declared capacity tier,
    /// which is what releases the `bondRequired(declaredMbps)` floor that
    /// `decdn node unbond` enforces.
    ///
    /// This does NOT return the bond: it stays deposited and fully slashable.
    /// Follow with `decdn node unbond --all` to start the unbonding window,
    /// then re-run it after the window to withdraw. Pass `--dry-run` to print
    /// the plan without submitting.
    Deregister(DeregisterArgs),
    /// Unpaid client-side discovery of active nodes via
    /// `CapacityBond.getActiveNodes` (#1481). Maps node-ids/regions to
    /// operator Ethereum addresses — the input `decdn channel open
    /// --provider-address` needs — without spending anything: it builds a
    /// signer-less read-only provider and never loads a keystore, unlike
    /// every other on-chain `node` subcommand above.
    ///
    /// With neither `--node-id` nor `--region`, lists every active node.
    /// Pass `--probe` to additionally rank the (region-shortlisted)
    /// candidates by measured `cdn/probe/v1` round-trip time; without it,
    /// candidates are listed with no RTT.
    Lookup(LookupArgs),
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

/// Shared blockchain coordinates + keys for the on-chain `node` subcommands
/// (`register`, `bond`). Flattened into each command's args so they expose
/// an identical flag group. The common coordinates live in
/// [`CommonChainArgs`]; this struct adds the `CapacityBond` address and the
/// swap flags. Each field is taken from a flag when present, otherwise the
/// `[blockchain]` / `[identity]` tables of the TOML config (same file the
/// daemon reads). `rpc_url` and `capacity_bond_address` are required (no
/// default — the command errors if neither flag nor config supplies them);
/// `chain_id`, `keystore`, and `data_dir` fall back to built-in defaults.
#[derive(Args, Debug)]
pub struct ChainArgs {
    #[command(flatten)]
    pub common: CommonChainArgs,

    /// `CapacityBond` contract address. Overrides
    /// `blockchain.capacity_bond_address`.
    #[arg(long, value_name = "ADDR")]
    pub capacity_bond_address: Option<String>,

    /// DEX venue for `--pay-bond-with usdc`: `uniswap-v3` or `balancer-v3`.
    #[arg(long = "swap-venue", value_enum)]
    pub swap_venue: Option<SwapVenueArg>,

    /// Exact-out swap router address (Uniswap `SwapRouter02` / Balancer `Router`).
    #[arg(long = "swap-router-address", value_name = "ADDR")]
    pub swap_router_address: Option<String>,

    /// Quoter address (Uniswap `QuoterV2`; Balancer uses the router's query).
    #[arg(long = "swap-quoter-address", value_name = "ADDR")]
    pub swap_quoter_address: Option<String>,

    /// USDC token address to spend on the swap.
    #[arg(long = "usdc-address", value_name = "ADDR")]
    pub usdc_address: Option<String>,

    /// Uniswap V3 pool fee tier (e.g. 3000 = 0.3%). Uniswap venue only.
    #[arg(long = "swap-fee-tier", value_name = "FEE")]
    pub swap_fee_tier: Option<u32>,

    /// Balancer V3 pool address (Balancer V3 addresses pools directly, not by
    /// bytes32 id). Balancer venue only.
    #[arg(long = "swap-balancer-pool", value_name = "ADDR")]
    pub swap_balancer_pool: Option<String>,

    /// Uniswap V3 TOKEN/USDC pool address, used for the price-impact `slot0`
    /// read that enables the advisory price-impact warning. Uniswap venue only
    /// (distinct from `--swap-balancer-pool`).
    #[arg(long = "swap-pool-address", value_name = "ADDR")]
    pub swap_pool_address: Option<String>,
}

/// `decdn node register` — submit `CapacityBond.registerNode` (ADR 019
/// § Step 2.3). Performs an on-chain transaction rather than talking to a
/// running node's admin RPC.
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

    /// Accept the network's current operator terms non-interactively (ADR 019
    /// § Terms Acceptance). On a terminal the terms are shown and confirmed
    /// interactively; in automation/headless contexts this flag records the
    /// operator's acceptance (registration will not proceed without it).
    #[arg(long = "accept-terms")]
    pub accept_terms: bool,

    #[command(flatten)]
    pub chain: ChainArgs,
}

/// `decdn node bond` — stake to a capacity tier via `CapacityBond.bond` +
/// `declareMbps` (ADR 019 § Step 2.1–2.2). Idempotent: it tops the active
/// bond up to `max(minBond, bondRequired(mbps))`, approving and bonding only
/// the shortfall, so a re-run after a partial failure converges instead of
/// over-bonding. A precondition for `decdn node register`.
#[derive(Args, Debug)]
pub struct BondArgs {
    /// Declared serving capacity in Mbps. The TOKEN bond is computed from the
    /// on-chain `bondRequired(mbps)` curve — you do not pass a token amount.
    /// Must fall within the governable `[minCapacityMbps, maxCapacityMbps]`
    /// band or the on-chain `declareMbps` reverts.
    #[arg(long, value_name = "MBPS")]
    pub mbps: u64,

    #[command(flatten)]
    pub chain: ChainArgs,
}

/// `decdn node unbond` — lower the bond via `CapacityBond.requestUnbond` +
/// `unbond` (ADR 026 § Capacity-bond curve). One command covers all three
/// phases of the window; which one runs is read from chain state, not from a
/// flag. A run that lost its `requestUnbond` after `declareMbps` landed resumes
/// on re-run, the way [`BondArgs`] converges; once `requestUnbond` HAS landed a
/// re-run with an amount flag is a deliberate error, since the request can no
/// longer be changed.
///
/// The amount flags apply only when starting a request — passing one while a
/// request is already in flight is an error rather than a silent no-op. All
/// three are mutually exclusive; exactly one is required to start a request.
#[derive(Args, Debug)]
#[command(group(ArgGroup::new("unbond_amount").args(["to_mbps", "all", "amount"]).multiple(false)))]
pub struct UnbondArgs {
    /// Reduce the declared capacity tier to MBPS and release the surplus
    /// bond. The released amount is derived from the on-chain
    /// `bondRequired` curve — you do not pass a token amount. The retained
    /// bond is `max(minBond, bondRequired(MBPS))`, so the node stays
    /// eligible at the new tier. Must not exceed the current declared tier
    /// (raise with `decdn node bond --mbps`); passing the tier the node is
    /// already at is accepted and skips the redundant `declareMbps`, which is
    /// how a partially-failed run resumes. A tier that will actually be
    /// declared must be inside the governable
    /// `[minCapacityMbps, maxCapacityMbps]` band.
    #[arg(long = "to-mbps", value_name = "MBPS")]
    pub to_mbps: Option<u64>,

    /// Release everything the bond curve permits, retaining only
    /// `bondRequired(declaredMbps)`. Unlike `--to-mbps` this ignores the
    /// `minBond` floor, so at low declared tiers the retained bond can land
    /// below `minBond` and the node stays inactive until it re-bonds. Whether
    /// it does is tier-dependent; the command reports `below_min_bond` before
    /// submitting.
    ///
    /// For an operator that is NOT in the registered set — ejected, or bonded
    /// and declared but never registered — this additionally clears the
    /// declared tier (`declareMbps(0)`), making it a full exit: nothing is
    /// retained. That is the only route to the bond those operators have, since
    /// `decdn node deregister` requires an active registration. A registered
    /// node exits by running `decdn node deregister` first.
    #[arg(long)]
    pub all: bool,

    /// Release exactly this many TOKEN base units (1 TOKEN = 1e18).
    /// Escape hatch for an exact figure; the curve is still pre-checked, so
    /// an amount the contract would reject fails with the `decdn node bond
    /// --mbps` invocation needed to make it legal.
    #[arg(long, value_name = "BASE_UNITS")]
    pub amount: Option<u128>,

    /// Skip the interactive confirmation. Required when *starting* a request
    /// from a non-interactive shell, since that deactivates the node for the
    /// full window; the withdrawal phase never prompts. The consequences are
    /// printed either way — this suppresses the prompt, not the warning.
    #[arg(long = "yes", short = 'y')]
    pub yes: bool,

    #[command(flatten)]
    pub chain: ChainArgs,
}

/// `decdn node deregister` — leave the active set via
/// `CapacityBond.deregisterNode` (ADR 003 § Node Registry, #1359).
///
/// Deliberately flagless beyond the confirmation gate: `deregisterNode()` takes
/// no arguments and needs no EIP-712 or ed25519 signature — unlike
/// [`RegisterArgs`], which builds both — so the operator signer is the whole
/// input. There is no state-driven phase selection either, the way
/// [`UnbondArgs`] has: deregistration is a single transaction that either
/// applies or reverts `NodeNotActive`.
///
/// It is nonetheless at least as consequential as starting an unbonding window,
/// which is why it carries the same `--yes` gate: it drops the node from the
/// active set, bumps `registrationNonce[nodeId]` (invalidating any
/// previously-signed registration signature), and clears the declared tier, so
/// re-entry costs a fresh `declareMbps` + `registerNode`.
#[derive(Args, Debug)]
pub struct DeregisterArgs {
    /// Skip the interactive confirmation. Required from a non-interactive
    /// shell, since deregistration takes the node out of the active set
    /// immediately. The consequences are printed either way — this suppresses
    /// the prompt, not the warning.
    #[arg(long = "yes", short = 'y')]
    pub yes: bool,

    #[command(flatten)]
    pub chain: ChainArgs,
}

/// `decdn node lookup` — unpaid client-side discovery of active nodes via
/// `CapacityBond.getActiveNodes` (#1481). See [`NodeCommand::Lookup`] for the
/// full description.
#[derive(Args, Debug)]
pub struct LookupArgs {
    /// Filter to the single node with this exact iroh node id (the same form
    /// `decdn probe --node-id` accepts). Combinable with `--region`; with
    /// neither flag every active node is listed.
    #[arg(long = "node-id", value_name = "ID")]
    pub node_id: Option<String>,

    /// Filter to nodes whose self-attested region hint (ADR 030) matches this
    /// ISO 3166-1 alpha-2 code exactly (case-insensitive — normalized the
    /// same way as `decdn node register --region`).
    #[arg(long, value_name = "CODE")]
    pub region: Option<String>,

    /// Probe each matching candidate over `cdn/probe/v1` and sort ascending
    /// by measured round-trip time. Candidates that don't answer are kept at
    /// the end (rather than dropped) with no RTT. Without this flag, no
    /// network probing happens and candidates carry no RTT.
    #[arg(long)]
    pub probe: bool,

    /// Emit the result as a JSON array instead of a human table.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds for each `--probe` probe. Ignored
    /// without `--probe`.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,

    #[command(flatten)]
    pub chain: ChainArgs,
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
