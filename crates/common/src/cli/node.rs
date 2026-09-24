//! Arguments for the `decdn node` subcommand group.
//!
//! `node` is the operator-local admin namespace: subcommands talk to the
//! loopback admin HTTP surface (`adr/appendix-local-admin-http.md`) exposed by a running node.

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

/// Operator-local admin commands that query a running deCDN node.
#[derive(Args, Debug)]
pub struct NodeArgs {
    /// The `decdn node` subcommand to run.
    #[command(subcommand)]
    pub cmd: NodeCommand,
}

/// Subcommands under `decdn node`.
#[derive(Subcommand, Debug)]
pub enum NodeCommand {
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
    /// Print a live snapshot of the running node's lanes via
    /// `admin_v1_lanes` (issue #749): per lane the outstanding accrued
    /// claim (micro-USDC), time since the last voucher, and whether the
    /// accrued claim has reached the configured redemption threshold.
    /// Lets operators spot lanes approaching settlement, stale lanes, or
    /// unusually high outstanding balances before they become a
    /// liquidity risk — without scraping metrics or reading logs. Same
    /// admin-URL resolution and timeout semantics as `decdn node health`.
    Lanes(LanesArgs),
    /// Print every slash the node's watcher has detected against its own
    /// operator via `admin_v1_slashes` (#1032, G-NODE-05): per slash the
    /// `slashId`, offense type, amount, evidence digest, block, and the
    /// 30-day appeal-window close time. Lets operators (or keeper scripts)
    /// notice a slash and file `decdn appeal slash` within the window
    /// without watching the chain directly. Same admin-URL resolution and
    /// timeout semantics as `decdn node health`.
    Slashes(SlashesArgs),
    /// Print the running node's buyer-side `PaymentPool` state via
    /// `admin_v1_pools` (#2078): every pool the node's buy leg tracks, the
    /// deposit it believes each holds, and the per-lane amount it has already
    /// signed away to each provider. This is the node's *spending* side — the
    /// mirror of `decdn node lanes`, which reports what it has earned.
    ///
    /// The only way to read this state on a running node: the daemon holds an
    /// exclusive lock on its `buyer.redb` for its whole lifetime, so no
    /// command can open that file from disk while the node is up. `decdn pool
    /// list` manages a **separate**, client-owned store and says nothing about
    /// this one. Same admin-URL resolution and timeout semantics as
    /// `decdn node health`.
    Pools(PoolsArgs),
    /// Forcibly remove a single blob from the local cache (issue #279).
    /// The removal is permanent: the node never serves or re-fetches the hash
    /// again. Useful for DMCA takedown and storage reclamation.
    Evict(EvictArgs),
    /// Re-read the running node's config file and apply hot-reloadable
    /// fields (issue #373). Equivalent to `kill -HUP <pid>` but goes
    /// through the loopback admin surface, so operator tooling that
    /// already speaks JSON-RPC doesn't need to also know which PID to
    /// signal. `observability.log_level`, `cache.pinned_hashes`,
    /// `security.*`, the `[content]` denylist, and `[load_shed]` are
    /// reloadable; other fields are logged as ignored. The authoritative
    /// list is the module doc of the node's `runtime/reload.rs` — when a
    /// section is added there, update this help text too. Both paths
    /// share the same internal mutex, so a concurrent SIGHUP and
    /// `decdn node reload` queue rather than race. Requires the node to
    /// have been started with `decdn-node run --config <path>` — without
    /// a path on disk there's nothing to re-read.
    Reload(ReloadArgs),
    /// Trigger graceful shutdown of the running node via `admin_v1_drain`
    /// (issue #244, `adr/appendix-local-admin-http.md`). Equivalent to `kill -TERM <pid>` but goes
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
    /// Rotate an operator key and keep the on-chain binding in step
    /// (`adr/appendix-operator-key-rotation.md`, #1034). `--key` chooses which
    /// key, because the two paths are not comparable in cost or effect.
    ///
    /// `--key iroh` rebinds the wire node id through
    /// `CapacityBond.bindNodeId`. One transaction, no downtime beyond a
    /// restart, and the Ethereum address — with the bond, the declared tier,
    /// `firstBondedAt`, and every lane funded against this operator — is untouched. The old
    /// and new node ids swap slashability in the same block, so there is no
    /// window in which the operator cannot be slashed.
    ///
    /// `--key eth` moves the whole on-chain identity, because no rebinding API
    /// exists for the address: deregister, wait out the full unbonding window,
    /// withdraw, then re-bond and re-register from a new address.
    /// `firstBondedAt` — and with it the governance age-ramp — resets. Like
    /// `decdn node unbond`, the phase is read from chain state rather than
    /// passed as a flag, so one command drives the whole window.
    ///
    /// Neither path restarts the daemon: the iroh key is not hot-reloadable,
    /// so drain (`decdn node drain`), stop, rotate, restart stays the
    /// operator's sequence. Pass `--dry-run` to print the plan — and, on the
    /// iroh path, both signatures — without submitting or touching any key
    /// file.
    RotateKey(RotateKeyArgs),
    /// (Re)publish this node's dialable QUIC addresses on-chain via
    /// `CapacityBond.updateMultiaddrs` (#1908). The one-command fix for a node
    /// that registered an empty multiaddr set (`decdn node register` with no
    /// `--multiaddr`) and is therefore stranded on the iroh relay path: it
    /// promotes direct addresses without a destructive deregister/re-register
    /// cycle (which `register` alone cannot do — a second `register` reverts
    /// `NodeAlreadyRegistered`).
    ///
    /// Like the other on-chain `node` subcommands this talks to the chain, not
    /// a running node's admin RPC, and signs with the operator Ethereum key.
    /// The node must be active; the contract's guardrails — the
    /// `maxMultiaddrSize` byte ceiling and the `multiaddrUpdateCooldown`
    /// between updates — are pre-checked and reported before the send. Pass
    /// `--dry-run` to print the plan (packed size, ceiling, cooldown state)
    /// without submitting.
    UpdateMultiaddrs(UpdateMultiaddrsArgs),
    /// (Re)attest this node's region on-chain via `CapacityBond.updateRegion`
    /// (ADR 030). The one-command fix for a node that registered the wrong
    /// `regionHint` (`decdn node register --region`) or whose operator has
    /// physically relocated: it corrects the region without a destructive
    /// deregister/re-register cycle (which `register` alone cannot do — a second
    /// `register` reverts `NodeAlreadyRegistered`).
    ///
    /// Like the other on-chain `node` subcommands this talks to the chain, not
    /// a running node's admin RPC, and signs with the operator Ethereum key. The
    /// region is validated as an ISO 3166-1 alpha-2 code CLI-side — the contract
    /// only length-checks and would otherwise store a garbage string. The node
    /// must be active; the contract's `regionStabilityWindow` cooldown between
    /// updates is pre-checked and reported before the send. Pass `--dry-run` to
    /// print the plan (current region, new region, cooldown state) without
    /// submitting.
    UpdateRegion(UpdateRegionArgs),
    /// Unpaid client-side discovery of active nodes via
    /// `CapacityBond.getRegisteredNodes` (#1481). Maps node-ids/regions to
    /// operator Ethereum addresses — the input `decdn fetch
    /// --provider-address` needs — without spending anything: it builds a
    /// signer-less read-only provider and never loads a keystore, unlike
    /// every other on-chain `node` subcommand above.
    ///
    /// With neither `--node-id` nor `--region`, lists every active node.
    /// Pass `--probe` to additionally rank the (region-shortlisted)
    /// candidates by measured `cdn/probe/v1` round-trip time; without it,
    /// candidates are listed with no RTT.
    Lookup(LookupArgs),
    /// Diagnose config, on-disk state, disk budget, and reachability.
    // Boxed to keep `NodeCommand`'s variants close in size (`DoctorArgs` is by
    // far the largest); mirrors `Command::Run(Box<RunArgs>)`.
    Doctor(Box<DoctorArgs>),
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

/// `decdn node lanes` — list open payment lanes via
/// `admin_v1_lanes` (issue #749). Same admin-URL resolution and
/// timeout semantics as `decdn node health`.
#[derive(Args, Debug)]
pub struct LanesArgs {
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
    /// summary + lane table.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}

/// `decdn node slashes` — list detected slashes against this node's
/// operator via `admin_v1_slashes` (#1032). Same admin-URL resolution and
/// timeout semantics as `decdn node health`.
#[derive(Args, Debug)]
pub struct SlashesArgs {
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
    /// summary + slash table.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}

/// `decdn node pools` — read the node's buyer-side `PaymentPool` state via
/// `admin_v1_pools` (#2078). Same admin-URL resolution and timeout semantics as
/// `decdn node health`.
#[derive(Args, Debug)]
pub struct PoolsArgs {
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
    /// summary + pool table.
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

/// `decdn node reload` — re-read the running node's config file via
/// `admin_v1_reload` (issue #373) and print the post-reload `log_level`.
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

    /// Emit the admin response as JSON instead of the `log_level=` line.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}

/// `decdn node drain` — trigger graceful shutdown of the running node via
/// `admin_v1_drain` (issue #244, `adr/appendix-local-admin-http.md`). Fires the same runtime shutdown
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

/// Shared blockchain coordinates + keys for the on-chain `node` subcommands
/// (`register`, `bond`). Flattened into each command's args so they expose
/// an identical flag group. The common coordinates live in
/// [`CommonChainArgs`]; this struct adds the `CapacityBond` address. Each field
/// is taken from a flag when present, otherwise the `[blockchain]` /
/// `[identity]` tables of the TOML config (same file the daemon reads).
/// `rpc_url` and `capacity_bond_address` are required (no default — the command
/// errors if neither flag nor config supplies them); `chain_id`, `keystore`,
/// and `data_dir` fall back to built-in defaults.
#[derive(Args, Debug)]
pub struct ChainArgs {
    /// RPC endpoint, chain id, keystore, and data-dir flags shared by every
    /// chain-touching command.
    #[command(flatten)]
    pub common: CommonChainArgs,

    /// `CapacityBond` contract address. Overrides
    /// `blockchain.capacity_bond_address`.
    #[arg(long, value_name = "ADDR")]
    pub capacity_bond_address: Option<String>,
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
    /// relies on `cdn/dht/v1` discovery (ADR 022) for reachability.
    #[arg(long = "multiaddr", value_name = "MA")]
    pub multiaddrs: Vec<String>,

    /// Accept the network's current operator terms non-interactively (ADR 019
    /// § Terms Acceptance). On a terminal the terms are shown and confirmed
    /// interactively; in automation/headless contexts this flag records the
    /// operator's acceptance (registration will not proceed without it).
    #[arg(long = "accept-terms")]
    pub accept_terms: bool,

    /// Chain coordinates: RPC endpoint, contract addresses, and keystore.
    #[command(flatten)]
    pub chain: ChainArgs,
}

/// Which of the operator's keys `decdn node rotate-key` rotates.
///
/// A `ValueEnum` with **no default**: the two paths differ by orders of
/// magnitude in cost and are not interchangeable, so the operator names one.
/// These are the operator's only two keys. There is no separate slash-sig
/// signing key: an operator serves from the EOA it registered, so `slash_sig`
/// is signed by the Ethereum key above. Serving from a smart account while a
/// hot key signs `slash_sig` is unimplemented
/// (`adr/024-account-abstraction.md` § Unimplemented — Operator Custody While
/// Serving), so there is no third variant to rotate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum RotateKeyTarget {
    /// The iroh Ed25519 node key — the wire `NodeId`. One `bindNodeId`
    /// transaction; the Ethereum address, bond, declared tier,
    /// `firstBondedAt`, and every lane funded against this operator survive untouched.
    #[value(name = "iroh")]
    Iroh,
    /// The secp256k1 Ethereum signing key. No rebinding API exists for the
    /// on-chain address, so this moves the whole identity: deregister →
    /// unbond window → re-bond and re-register from a new address. Costs the
    /// full unbonding window of downtime and resets `firstBondedAt`.
    #[value(name = "eth")]
    Eth,
}

/// `decdn node rotate-key` — the operator key-rotation runbook
/// (`adr/appendix-operator-key-rotation.md`) as a command (#1034).
///
/// Rotation is the one operator procedure where a half-completed run is worse
/// than no run: a node whose local key is not the on-chain-bound one is
/// **un-slashable**, which is a protocol-level fault, not merely an outage. So
/// both paths order their steps to make that state unreachable — the iroh path
/// commits `node.secret` only after `bindNodeId` confirms, and the eth path
/// reads chain state to pick its next phase rather than trusting a flag.
///
/// Neither path restarts the daemon. The node key is not hot-reloadable
/// (`admin_v1_reload` and SIGHUP cover mutable config fields — see the
/// node's `runtime/reload.rs` for the set — not the iroh endpoint), so the
/// runbook's drain → stop → rotate → restart sequence stays the operator's
/// to drive, with `decdn node drain` for the first step.
#[derive(Args, Debug)]
pub struct RotateKeyArgs {
    /// Which key to rotate: `iroh` or `eth`. Required, because rotating the
    /// wrong one is not recoverable in the same afternoon.
    #[arg(long = "key", value_name = "KEY", value_enum)]
    pub key: RotateKeyTarget,

    /// (`--key iroh`) Bind the node key ALREADY ON DISK instead of
    /// generating a fresh one.
    ///
    /// Two jobs, both from the runbook's § Failure modes and rollback: repair a
    /// node whose `node.secret` was replaced by hand (leaving it un-slashable),
    /// and roll back a rotation by re-binding a restored `.bak` archive. Since
    /// nothing is generated, nothing is archived — this only submits the
    /// transaction.
    #[arg(long = "bind-existing")]
    pub bind_existing: bool,

    /// (`--key eth`) Keystore holding the NEW Ethereum key to migrate onto.
    /// Required to reach the re-onboarding phase; the earlier phases
    /// (deregister, unbond request, window wait, withdraw) all run against the
    /// current keystore and do not need it. Passing it earlier is still useful:
    /// the address is validated against the one being migrated away from on
    /// every invocation, so a same-address mistake is caught on the first run
    /// rather than after the unbonding window.
    ///
    /// Both keystores are unlocked from the same password source
    /// (`DECDN_KEYSTORE_PASSWORD`, then `--keystore-password-file`, then a
    /// prompt), so a headless migration needs them to share a password.
    #[arg(long = "new-keystore", value_name = "PATH")]
    pub new_keystore: Option<PathBuf>,

    /// (`--key eth`) Capacity tier to re-declare on the new address. Required
    /// at the re-onboarding phase: `deregisterNode` clears the declared tier and
    /// it cannot be read back afterwards, so record the value the deregister
    /// phase prints and pass it here.
    #[arg(long = "mbps", value_name = "MBPS")]
    pub mbps: Option<u64>,

    /// (`--key eth`) `regionHint` for the re-registration. Defaults to the old
    /// address's registered region.
    #[arg(long, value_name = "CODE")]
    pub region: Option<String>,

    /// (`--key eth`) QUIC multiaddr for the re-registration. Repeatable.
    /// Defaults to the multiaddrs the old address registered.
    #[arg(long = "multiaddr", value_name = "MA")]
    pub multiaddrs: Vec<String>,

    /// (`--key eth`) Accept the network's current operator terms
    /// non-interactively. Re-registration from a new address is a fresh
    /// registration, so it re-accepts terms (ADR 019 § Terms Acceptance);
    /// `--key iroh` never does, because rebinding signs the terms-free
    /// `BindNodeId` payload.
    #[arg(long = "accept-terms")]
    pub accept_terms: bool,

    /// Skip the interactive confirmation. Required from a non-interactive
    /// shell. The consequences are printed either way — this suppresses the
    /// prompt, not the warning.
    #[arg(long = "yes", short = 'y')]
    pub yes: bool,

    // `--dry-run` and `--json` come from the flattened `ChainArgs`
    // (`CommonChainArgs`); declaring them here too would collide.
    /// Chain coordinates: RPC endpoint, contract addresses, and keystore.
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

    /// Chain coordinates: RPC endpoint, contract addresses, and keystore.
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

    /// Chain coordinates: RPC endpoint, contract addresses, and keystore.
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

    /// Chain coordinates: RPC endpoint, contract addresses, and keystore.
    #[command(flatten)]
    pub chain: ChainArgs,
}

/// `decdn node update-multiaddrs` — (re)publish the operator's dialable QUIC
/// addresses via `CapacityBond.updateMultiaddrs` (#1908). See
/// [`NodeCommand::UpdateMultiaddrs`] for the full description. Performs an
/// on-chain transaction rather than talking to a running node's admin RPC.
#[derive(Args, Debug)]
pub struct UpdateMultiaddrsArgs {
    /// QUIC multiaddr to publish, e.g. `/ip4/203.0.113.10/udp/4433/quic-v1`.
    /// Repeatable; at least one is required — the whole point of this command
    /// is to advertise a non-empty set, so clearing addresses back to empty is
    /// deliberately not offered here (it would re-create the relay-pinned
    /// footgun). The set fully replaces whatever is on-chain (the contract
    /// stores the field verbatim, it does not merge).
    #[arg(long = "multiaddr", value_name = "MA", required = true)]
    pub multiaddrs: Vec<String>,

    /// Chain coordinates: RPC endpoint, contract addresses, and keystore.
    #[command(flatten)]
    pub chain: ChainArgs,
}

/// `decdn node update-region` — (re)attest the operator's region via
/// `CapacityBond.updateRegion` (ADR 030). See [`NodeCommand::UpdateRegion`] for
/// the full description. Performs an on-chain transaction rather than talking to
/// a running node's admin RPC.
#[derive(Args, Debug)]
pub struct UpdateRegionArgs {
    /// New region to attest, as an ISO 3166-1 alpha-2 country code (e.g. `DE`).
    /// Validated CLI-side and normalized (trimmed, uppercased) before the send —
    /// the contract only length-checks, so an unvalidated code would be stored
    /// verbatim and read back as "no locality information" by every downstream
    /// consumer. The value fully replaces the on-chain `regionHint`.
    #[arg(long = "region", value_name = "CODE")]
    pub region: String,

    /// Chain coordinates: RPC endpoint, contract addresses, and keystore.
    #[command(flatten)]
    pub chain: ChainArgs,
}

/// `decdn node lookup` — unpaid client-side discovery of active nodes via
/// `CapacityBond.getRegisteredNodes` (#1481). See [`NodeCommand::Lookup`] for the
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

    /// Roundtrip timeout in milliseconds for each `--probe` probe. Ignored
    /// without `--probe`.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,

    // `--json` is supplied by the flattened `ChainArgs` (`CommonChainArgs.json`);
    // declaring it here too would collide (clap requires unique arg names).
    /// Chain coordinates: RPC endpoint, contract addresses, and keystore.
    #[command(flatten)]
    pub chain: ChainArgs,
}

/// `decdn node doctor` arguments.
#[derive(Args, Debug)]
pub struct DoctorArgs {
    /// Config overrides, resolved exactly as `decdn-node run` would.
    #[command(flatten)]
    pub run: crate::cli::run::RunArgs,

    /// Emit the full report as JSON instead of the checklist.
    #[arg(long)]
    pub json: bool,

    /// Treat warnings as failures for the exit code.
    #[arg(long)]
    pub strict: bool,

    /// Skip all network and live-daemon probes (config + disk + files only).
    #[arg(long)]
    pub offline: bool,

    /// Per-probe timeout for network checks, in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5000)]
    pub timeout_ms: u64,

    /// Admin RPC URL for live enrichment [default: from config / `DECDN_ADMIN_PORT`].
    #[arg(long, env = "DECDN_ADMIN_URL", value_name = "URL")]
    pub admin_url: Option<String>,
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
    /// resolution semantics as `decdn node health --config`.
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
