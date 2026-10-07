//! `decdn node ...` — operator-local admin commands that talk to a running
//! node over its loopback JSON-RPC admin surface (`appendix-local-admin-http`).

use std::io;
use std::io::Write as _;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use alloy::primitives::Address;
use anyhow::Context;
use iroh::{EndpointAddr, PublicKey};
use jsonrpsee::core::client::Error as JsonRpcClientError;
use jsonrpsee::http_client::HttpClientBuilder;
use serde::{Deserialize, Serialize};

use decdn_client::discovery::{self, NodeCandidate, SELECT_K, select_candidates};
use decdn_client::endpoint as client_endpoint;
use decdn_client::probe::probe_once;
use decdn_common::admin::{
    AdminRpcClient, BindingStatus, BuyerPoolsResponse, DrainRequest, DrainResponse, EvictRequest,
    EvictResponse, HealthResponse, LaneSnapshot, LanesResponse, ReloadResponse, SlashesResponse,
    StatusResponse,
};
use decdn_common::cli;
use decdn_common::cli::ConfigPathSource;
use decdn_common::cli::common::expand_tilde;
use decdn_common::config::DEFAULT_ADMIN_PORT;
use decdn_protocol::Region;

use crate::commands::chain_ctx;

/// Partial deserializer for the TOML config — only the path
/// `observability.admin_port` is interesting to the `node` admin
/// subcommands. Kept private here (rather than reusing
/// `decdn_common::config::FileConfig`) so an operator's typo in an
/// unrelated section can't make these commands unusable. `serde(default)`
/// and serde-toml's default
/// "ignore unknown fields" together guarantee that any other valid
/// TOML — including missing tables — round-trips through with no
/// effect.
#[derive(Debug, Default, Deserialize)]
struct AdminPortConfig {
    observability: Option<AdminPortObservability>,
}

#[derive(Debug, Default, Deserialize)]
struct AdminPortObservability {
    admin_port: Option<u16>,
}

/// Dispatch a `decdn node <sub>` invocation.
///
/// `global_config` is the path (if any) from the top-level `decdn
/// --config` flag. It's consulted as a fallback when the subcommand
/// didn't set its own `--config`, so `decdn --config foo.toml node
/// health` works the way the help text implies.
pub async fn node_dispatch(
    args: &cli::NodeArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    match &args.cmd {
        cli::NodeCommand::Health(h) => health(h, global_config).await,
        cli::NodeCommand::Status(s) => status(s, global_config).await,
        cli::NodeCommand::Lanes(c) => lanes(c, global_config).await,
        cli::NodeCommand::Slashes(s) => slashes(s, global_config).await,
        cli::NodeCommand::Pools(p) => pools(p, global_config).await,
        cli::NodeCommand::Evict(e) => evict(e, global_config).await,
        cli::NodeCommand::Reload(r) => reload(r, global_config).await,
        cli::NodeCommand::Drain(d) => drain(d, global_config).await,
        cli::NodeCommand::Top(t) => crate::commands::node_top::run(t, global_config).await,
        cli::NodeCommand::Register(r) => crate::commands::register::run(r, global_config).await,
        cli::NodeCommand::Bond(b) => crate::commands::bond::run(b, global_config).await,
        cli::NodeCommand::Unbond(u) => crate::commands::unbond::run(u, global_config).await,
        cli::NodeCommand::Deregister(d) => crate::commands::deregister::run(d, global_config).await,
        cli::NodeCommand::RotateKey(r) => crate::commands::rotate_key::run(r, global_config).await,
        cli::NodeCommand::UpdateMultiaddrs(u) => {
            crate::commands::update_multiaddrs::run(u, global_config).await
        }
        cli::NodeCommand::UpdateRegion(u) => {
            crate::commands::update_region::run(u, global_config).await
        }
        cli::NodeCommand::Lookup(l) => lookup(l, global_config).await,
        cli::NodeCommand::Doctor(d) => crate::commands::doctor::run(d, global_config).await,
    }
}

/// `decdn node health`: call `admin_v1_health` on the running node and
/// print the result. Two-line plain text by default (`node_id=…` /
/// `uptime_s=…`), or pretty JSON with `--json`.
pub async fn health(args: &cli::HealthArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: HealthResponse = client
        .health()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&resp).context("failed to encode health as JSON")?;
        println!("{pretty}");
    } else {
        // Stable, grep-friendly lines so `decdn node health | grep node_id=`
        // works in operator scripts without `--json`.
        let mut out = io::stdout().lock();
        write_health(&mut out, &resp).context("failed to write health output")?;
    }

    Ok(())
}

/// Render `admin_v1_health` as `key=value` lines. Pure (`&mut impl Write`) so
/// the shape — in particular the binding lines, which an operator script keys
/// on — is unit-testable without a running node.
fn write_health(w: &mut impl io::Write, resp: &HealthResponse) -> io::Result<()> {
    writeln!(w, "node_id={}", resp.node_id)?;
    writeln!(w, "uptime_s={}", resp.uptime_s)?;
    // Always printed, including `unknown`. An absent line would read as "fine"
    // to a script grepping for a problem, and `unknown` means the opposite:
    // the node's slashability was never verified (#1034).
    writeln!(w, "binding={}", binding_label(resp.binding))?;
    // Only meaningful when there IS a binding; on a mismatch this names the
    // key to restore, which is the whole reason the field is carried.
    if let Some(bound) = &resp.bound_node_id {
        writeln!(w, "bound_node_id={bound}")?;
    }
    // Always printed (#1030). `false` is the answer to "the node is up and
    // healthy, why is it earning nothing": it is absent from the on-chain
    // registry, so it accrues no governance weight (its declared capacity caps
    // credited bytes at zero) and peers have no reason to route to a node they
    // cannot slash. Nothing on the wire says so — the node simply gets no
    // requests — which is exactly why this line exists.
    writeln!(w, "registry_active={}", resp.registry_active)?;
    Ok(())
}

/// Wire spelling of [`BindingStatus`], matching its `snake_case` serde
/// representation so the plain and `--json` renderings agree.
const fn binding_label(status: BindingStatus) -> &'static str {
    match status {
        BindingStatus::Bound => "bound",
        BindingStatus::Mismatch => "mismatch",
        BindingStatus::Unbound => "unbound",
        BindingStatus::Unknown => "unknown",
    }
}

/// `decdn node evict`: call `admin_v1_evict` on the running node to
/// forcibly remove a single blob from the local cache (issue #279), or
/// preview what the evict would touch when `--dry-run` is set (issue
/// #379).
pub async fn evict(args: &cli::EvictArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: EvictResponse = client
        .evict(EvictRequest {
            hash: args.hash.clone(),
            dry_run: args.dry_run,
        })
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&resp).context("failed to encode evict response")?;
        println!("{pretty}");
    } else if args.dry_run {
        // Multi-line, grep-friendly so operators can pipe through
        // `grep size_bytes=` / `grep already_evicted=` in scripts. Each
        // line stands on its own; the "dry_run=true" tag is the
        // load-bearing indicator that no state was mutated.
        write_dry_run_human(&mut io::stdout().lock(), &args.hash, &resp)
            .context("failed to write dry-run preview")?;
    } else {
        let presence = if resp.was_present {
            "evicted"
        } else {
            // Operator ran evict on a hash the node never held — log it
            // explicitly rather than printing nothing, otherwise scripts
            // can't tell success-with-no-effect from a hung command.
            "not present"
        };
        println!("hash={} status={presence}", args.hash);
    }

    Ok(())
}

/// Format the dry-run preview as a multi-line plain-text block. Pure
/// function (takes `&mut impl Write`) so unit tests can assert exact
/// output without an HTTP round-trip — same pattern as
/// [`write_status`].
fn write_dry_run_human(w: &mut impl io::Write, hash: &str, resp: &EvictResponse) -> io::Result<()> {
    writeln!(w, "hash={hash}")?;
    writeln!(w, "dry_run=true")?;
    writeln!(w, "was_present={}", resp.was_present)?;
    writeln!(w, "pinned={}", resp.preview.pinned)?;
    writeln!(w, "already_evicted={}", resp.preview.already_evicted)?;
    match resp.preview.size_bytes {
        Some(b) => writeln!(w, "size_bytes={b}")?,
        // Distinct from "0" (a legitimately empty blob): the iroh-blobs
        // status reported `NotFound`, or a partial blob whose last chunk has
        // not arrived (`Partial { size: None }`), not `Complete { size: 0 }`.
        None => writeln!(w, "size_bytes=not_stored")?,
    }
    match resp.preview.last_accessed_us_ago {
        Some(us) => writeln!(w, "last_accessed={}", format_age(us))?,
        // Distinct from "<1s ago"; operators want to know the
        // engine has *no* access record since the node started vs a very
        // recent one. A blob hot before a restart also reads this way.
        None => writeln!(w, "last_accessed=none_since_start")?,
    }
    // Origin egress-cost cue (#439). Distinct from omitting the line
    // when the engine has no origins: operators evaluating disk-reclaim
    // potential against re-fetch cost want this signal explicitly,
    // not buried in "the field is missing because there's no origin
    // at all". `none` matches the JSON serialisation skip-condition
    // semantically (`Vec::is_empty` is omitted in JSON, surfaced as
    // `none` here). Multi-origin chains (#284) render as a
    // comma-separated list in declared order.
    if resp.preview.origin_kinds.is_empty() {
        writeln!(w, "origin_kinds=none")?;
    } else {
        let joined = resp
            .preview
            .origin_kinds
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        writeln!(w, "origin_kinds={joined}")?;
    }
    Ok(())
}

/// `decdn node reload`: call `admin_v1_reload` on the running node to
/// re-read the config file it was started with and apply the
/// hot-reloadable subset (issue #373). Equivalent to `kill -HUP <pid>`
/// for operators who'd rather not stat the PID — and shares the same
/// internal mutex, so concurrent SIGHUPs queue rather than race.
pub async fn reload(args: &cli::ReloadArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: ReloadResponse = client
        .reload()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&resp).context("failed to encode reload response")?;
        println!("{pretty}");
    } else {
        // Stable, grep-friendly line so an operator can do
        // `decdn node reload | grep log_level=` without `--json`.
        // Same shape as `decdn node health`'s plain output.
        println!("log_level={}", resp.log_level);
    }

    Ok(())
}

/// `decdn node drain`: call `admin_v1_drain` on the running node to trigger
/// graceful shutdown (issue #244, `appendix-local-admin-http.md`).
///
/// Without `--wait`: fire-and-forget — returns `drain_initiated=true` as
/// soon as the trigger is queued; observe completion via process exit
/// or `decdn node health` until ECONNREFUSED.
///
/// With `--wait` (issue #604): asks the runtime to keep the admin
/// server alive through `router.shutdown` (`DrainRequest { wait_admin:
/// true }`), then polls `admin_v1_health` for `in_flight_streams == 0`.
/// Returns `Ok` once the count reaches 0 or admin closes
/// (ECONNREFUSED — drain finished and tore admin down). Returns `Err`
/// with `drain_timeout=true in_flight_streams=N` on stderr if
/// `--wait-timeout-secs` elapses first.
pub async fn drain(args: &cli::DrainArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );
    // `wait_timeout_secs` and `wait_poll_ms` are `NonZeroU64` — clap
    // rejects `0` at parse time, so no further runtime check needed.

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: DrainResponse = client
        .drain(Some(DrainRequest {
            wait_admin: args.wait,
        }))
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if !args.wait {
        if args.json {
            let pretty =
                serde_json::to_string_pretty(&resp).context("failed to encode drain response")?;
            println!("{pretty}");
        } else {
            // One stable, grep-friendly line. "Initiated", not "completed":
            // fire-and-forget semantics mean the process is still running when
            // this prints. Observe completion via `decdn node health` until
            // ECONNREFUSED, or let the process supervisor (systemd/K8s) notify.
            println!("drain_initiated={}", resp.initiated);
        }
        return Ok(());
    }

    // Cross-version safety: `--wait` is only safe when the server is
    // actually keeping admin alive through `router.shutdown`. A server
    // that doesn't honor `wait_admin` (older binary, future bug,
    // anything in between) lacks the `wait_admin_honored` field, which
    // serde-defaults to `false` on this side. Refuse to enter the
    // polling loop in that case — otherwise the imminent ECONNREFUSED
    // from the early admin tear-down would be misread as drain
    // completion while in-flight streams are still running, defeating
    // the zero-payment-loss guarantee.
    if !resp.wait_admin_honored {
        // The drain has *already fired* server-side at this point —
        // fire-and-forget semantics, so the node is now mid-shutdown
        // via the legacy SIGTERM-equivalent ordering. The operator
        // can't undo that; the actionable advice is to either let it
        // finish via the legacy path (process exit / health-until-
        // ECONNREFUSED) or upgrade the node so `--wait` works next time.
        return Err(anyhow::anyhow!(
            "admin at {url} did not honor --wait (wait_admin_honored=false). \
             Drain has already been initiated server-side and the node is \
             shutting down via the legacy SIGTERM-equivalent path; observe \
             completion via process exit or `decdn node health` until \
             ECONNREFUSED. The server is likely older than the CLI or does \
             not implement #604 — upgrade the node to use --wait safely",
        ));
    }

    // `--wait` path: drain has been initiated with `wait_admin=true`,
    // and the server acked it. Poll until `in_flight_streams == 0`,
    // admin closes (ECONNREFUSED — the runtime tore admin down after
    // `router.shutdown` returned), or the wall-clock budget elapses.
    // Both terminal-success paths emit the same JSON shape so machine
    // consumers see a single schema regardless of which branch fires.
    let deadline = std::time::Instant::now() + Duration::from_secs(args.wait_timeout_secs.get());
    let poll_interval = Duration::from_millis(args.wait_poll_ms.get());
    loop {
        let h = match client.health().await {
            Ok(h) => h,
            Err(JsonRpcClientError::Transport(inner))
                if is_admin_closed_transport_error(inner.as_ref()) =>
            {
                // Admin closed — runtime finished `router.shutdown` and
                // moved on. In-flight streams are necessarily zero
                // (router awaited them all). Treat
                // ConnectionRefused/Reset/Aborted and UnexpectedEof
                // all the same: each is a way for a TCP connection to
                // observe "the listener / accepted socket went away."
                emit_drain_complete(&mut io::stdout().lock(), args.json, true)
                    .context("failed to write drain-complete output")?;
                return Ok(());
            }
            // Transient errors during polling shouldn't end the wait
            // — the polling loop has its own wall-clock deadline. A
            // single 5s RPC timeout against a momentarily-busy admin
            // would otherwise collapse a 30s drain budget after one
            // tick. Retry until the deadline check fires; the
            // deadline is the single authority on "give up".
            // Bundling `RequestTimeout` and non-close `Transport`
            // into the same arm — they have the same retry policy.
            Err(JsonRpcClientError::RequestTimeout | JsonRpcClientError::Transport(_)) => {
                check_deadline_and_sleep(deadline, poll_interval, args.wait_timeout_secs).await?;
                continue;
            }
            // Server-side JSON-RPC errors during polling indicate
            // something genuinely unexpected (MethodNotFound here
            // would have been caught upstream by the
            // `wait_admin_honored=false` guard, so any `Call(...)` is
            // novel). Bail with a clear diagnostic.
            Err(JsonRpcClientError::Call(obj)) => {
                return Err(anyhow::anyhow!(
                    "admin at {url} returned JSON-RPC error {} during --wait poll: {}",
                    obj.code(),
                    obj.message(),
                ));
            }
            Err(other) => {
                return Err(classify_client_error(&url, args.timeout_ms, other));
            }
        };
        if h.in_flight_streams == 0 {
            emit_drain_complete(&mut io::stdout().lock(), args.json, false)
                .context("failed to write drain-complete output")?;
            return Ok(());
        }
        check_deadline_and_sleep_with_count(
            deadline,
            poll_interval,
            args.wait_timeout_secs,
            h.in_flight_streams,
        )
        .await?;
    }
}

/// Sleep until `poll_interval` elapses or `deadline` is reached,
/// whichever fires first; on deadline overrun, return `Err` with the
/// "no in-flight count known" timeout shape. Sleep is racy with
/// `ctrl_c` so an operator can interrupt a long wait without
/// `SIGKILL`ing the process.
async fn check_deadline_and_sleep(
    deadline: std::time::Instant,
    poll_interval: Duration,
    wait_timeout_secs: NonZeroU64,
) -> anyhow::Result<()> {
    if std::time::Instant::now() >= deadline {
        let _ = writeln!(
            io::stderr().lock(),
            "drain_timeout=true wait_timeout_secs={wait_timeout_secs}"
        );
        return Err(anyhow::anyhow!(
            "drain --wait timed out after {wait_timeout_secs}s"
        ));
    }
    sleep_or_ctrl_c(poll_interval).await
}

/// Variant of [`check_deadline_and_sleep`] used when the last
/// successful health response is available, so the timeout diagnostic
/// can report the last-known in-flight count to the operator.
async fn check_deadline_and_sleep_with_count(
    deadline: std::time::Instant,
    poll_interval: Duration,
    wait_timeout_secs: NonZeroU64,
    in_flight: u64,
) -> anyhow::Result<()> {
    if std::time::Instant::now() >= deadline {
        let _ = writeln!(
            io::stderr().lock(),
            "drain_timeout=true in_flight_streams={in_flight} \
             wait_timeout_secs={wait_timeout_secs}"
        );
        return Err(anyhow::anyhow!(
            "drain --wait timed out after {wait_timeout_secs}s; \
             {in_flight} stream(s) still in flight"
        ));
    }
    sleep_or_ctrl_c(poll_interval).await
}

/// Sleep for `dur` but resolve early on Ctrl-C, returning `Err` so
/// the polling loop can short-circuit with a clean exit. Without the
/// `ctrl_c` race, a long `--wait-poll-ms` would leave the operator
/// wedged for the remainder of the tick before they could escape.
async fn sleep_or_ctrl_c(dur: Duration) -> anyhow::Result<()> {
    tokio::select! {
        () = tokio::time::sleep(dur) => Ok(()),
        result = tokio::signal::ctrl_c() => {
            result.context("ctrl_c handler failed")?;
            let _ = writeln!(
                io::stderr().lock(),
                "drain --wait interrupted by Ctrl-C; drain continues server-side \
                 (use `decdn node health` to observe completion)",
            );
            Err(anyhow::anyhow!("drain --wait interrupted"))
        }
    }
}

/// Write the unified terminal-success record for `decdn node drain
/// --wait` to `w`. Both the polled-to-zero and ECONNREFUSED branches
/// share the same `{drain_complete, in_flight_streams, admin_closed}`
/// shape so machine consumers don't have to parse two unrelated
/// schemas depending on which path fired (#662 review). Same
/// `&mut impl io::Write` pattern as [`write_status`] so tests
/// can assert exact bytes without a stdout capture.
fn emit_drain_complete(w: &mut impl io::Write, json: bool, admin_closed: bool) -> io::Result<()> {
    if json {
        let value = serde_json::json!({
            "drain_complete": true,
            "in_flight_streams": 0,
            "admin_closed": admin_closed,
        });
        writeln!(w, "{value}")
    } else {
        writeln!(
            w,
            "drain_complete=true in_flight_streams=0 admin_closed={admin_closed}"
        )
    }
}

/// `decdn node status`: call `admin_v1_status` on the running node and
/// print a snapshot of its DHT participation health (issue #741).
pub async fn status(args: &cli::StatusArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let parsed: StatusResponse = client
        .status()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&parsed).context("failed to encode status as JSON")?;
        println!("{pretty}");
    } else {
        let mut stdout = io::stdout().lock();
        write_status(&mut stdout, &parsed, wall_clock_us())
            .context("failed to write status report")?;
    }

    Ok(())
}

/// `decdn node lanes`: call `admin_v1_lanes` on the running node and
/// print a snapshot of its open payment lanes (issue #749).
pub async fn lanes(args: &cli::LanesArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let parsed: LanesResponse = client
        .lanes()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&parsed).context("failed to encode lanes as JSON")?;
        println!("{pretty}");
    } else {
        let mut stdout = io::stdout().lock();
        write_lanes_table(&mut stdout, &parsed).context("failed to write lanes table")?;
    }

    Ok(())
}

/// Write the lane table to `w`. Pure function (takes `&mut impl
/// Write`) so the formatting is unit-testable without an HTTP hop,
/// mirroring [`write_status`]. A summary line
/// carries the redemption threshold as a stable `key=value` token; the
/// per-lane table follows. USDC amounts are rendered from micro-USDC.
fn write_lanes_table(w: &mut impl io::Write, resp: &LanesResponse) -> io::Result<()> {
    writeln!(
        w,
        "redeem_threshold={} lanes={}",
        format_usdc(resp.redeem_threshold_micro_usdc),
        resp.lanes.len(),
    )?;
    if resp.lanes.is_empty() {
        return writeln!(w, "(no open lanes)");
    }
    // Fixed-column layout: lane preview | counterparty preview | voucher
    // signer preview | nonce | outstanding | deposit | last-voucher age |
    // eligible.
    writeln!(
        w,
        "{:<14} {:<14} {:<14} {:>6} {:>12} {:>12} {:>12} ELIGIBLE",
        "LANE", "COUNTERPARTY", "SIGNER", "NONCE", "OUTSTANDING", "DEPOSIT", "LAST_VOUCHER",
    )?;
    for c in &resp.lanes {
        write_lane_row(w, c)?;
    }
    Ok(())
}

/// Render one lane as a fixed-column row. Split out so the column
/// formatting stays in one place and the loop body reads as a single call.
fn write_lane_row(w: &mut impl io::Write, c: &LaneSnapshot) -> io::Result<()> {
    let lane = short_node_id(&c.pool_id);
    let counterparty = short_node_id(&c.counterparty);
    // A pre-delegation server omits `voucher_signer` entirely (the DTO field is
    // `#[serde(default)]`), so an empty string means "this node cannot tell" —
    // rendered as `?` rather than silently echoing the funder.
    let signer = if c.voucher_signer.is_empty() {
        "?".to_string()
    } else {
        short_node_id(&c.voucher_signer)
    };
    let last_voucher = match c.seconds_since_last_voucher {
        // No voucher seen since this process started — distinct from
        // "<1s ago" so operators know the activity clock has no record
        // (a freshly-restarted node, or a lane that has never billed).
        None => "never".to_string(),
        // Reuse `format_age` (microsecond input) by scaling the whole-second
        // wire value; `saturating_mul` clamps the (unrealistic) overflow on an
        // absurd age rather than panicking. Boundary buckets are identical
        // (covered by `format_age_units`).
        Some(secs) => format_age(secs.saturating_mul(1_000_000)),
    };
    let eligible = if c.settlement_eligible { "yes" } else { "no" };
    writeln!(
        w,
        "{lane:<14} {counterparty:<14} {signer:<14} {:>6} {:>12} {:>12} {last_voucher:>12} \
         {eligible}",
        c.last_nonce,
        format_usdc(c.outstanding_micro_usdc),
        format_usdc(c.deposit_micro_usdc),
    )
}

/// Format a micro-USDC amount as a `"N.NNNNNN"` USDC string. USDC has 6
/// decimals (ADR 003), so 1 USDC == `1_000_000` micro-USDC. Trailing
/// fractional zeros are kept fixed-width (six places) so columns align and
/// a script parsing the value sees a stable shape.
fn format_usdc(micro: u64) -> String {
    let whole = micro / 1_000_000;
    let frac = micro % 1_000_000;
    format!("{whole}.{frac:06}")
}

/// `decdn node slashes`: call `admin_v1_slashes` on the running node and
/// print every detected slash against its operator (#1032). Plain text by
/// default (a summary line + a per-slash table), or pretty JSON with
/// `--json`.
pub async fn slashes(args: &cli::SlashesArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let parsed: SlashesResponse = client
        .slashes()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&parsed).context("failed to encode slashes as JSON")?;
        println!("{pretty}");
    } else {
        let mut stdout = io::stdout().lock();
        write_slashes_table(&mut stdout, &parsed).context("failed to write slashes table")?;
    }

    Ok(())
}

/// `decdn node pools`: call `admin_v1_pools` on the running node and print its
/// buyer-side `PaymentPool` state — the pools it pays upstream providers from.
///
/// This is the only read path to the daemon's `buyer.redb`: redb holds a
/// process-exclusive lock on that file for the node's lifetime, so nothing can
/// open it from disk while the node is up (#2078).
pub async fn pools(args: &cli::PoolsArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let parsed: BuyerPoolsResponse = client
        .pools()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&parsed).context("failed to encode pools as JSON")?;
        println!("{pretty}");
    } else {
        crate::commands::pool::write_skipped_buyer_pools(&mut io::stderr().lock(), &parsed.skipped)
            .context("failed to write skipped pools")?;
        let mut stdout = io::stdout().lock();
        // Name the daemon that answered, for the same reason `pool list` names
        // the file it read: an unattributed `pools=0` is the defect (#2078).
        writeln!(stdout, "store={url} (admin RPC)").context("failed to write pools header")?;
        crate::commands::pool::write_buyer_pools(&mut stdout, &parsed)
            .context("failed to write pools table")?;
    }

    Ok(())
}

/// Write the slash table to `w`. Pure function (takes `&mut impl
/// Write`) so the formatting is unit-testable without an HTTP hop,
/// mirroring [`write_lanes_table`]. A summary line carries the count as a
/// stable `key=value` token; the per-slash table follows.
fn write_slashes_table(w: &mut impl io::Write, resp: &SlashesResponse) -> io::Result<()> {
    writeln!(w, "slashes={}", resp.slashes.len())?;
    if resp.slashes.is_empty() {
        return writeln!(w, "(no slashes detected)");
    }
    writeln!(
        w,
        "{:<20} {:>6} {:>14} {:>10} {:>10} APPEAL_CLOSE",
        "SLASH_ID", "TYPE", "AMOUNT", "BLOCK", "EVIDENCE",
    )?;
    for s in &resp.slashes {
        let block = s
            .block_number
            .map_or_else(|| "?".to_string(), |b| b.to_string());
        let close = s
            .appeal_window_close
            .map_or_else(|| "?".to_string(), |c| c.to_string());
        writeln!(
            w,
            "{:<20} {:>6} {:>14} {:>10} {:>10} {close}",
            s.slash_id,
            s.offense_type,
            s.amount,
            block,
            short_node_id(&s.evidence_hash),
        )?;
    }
    Ok(())
}

/// Map a `jsonrpsee` client error into the three operator-actionable
/// classes the previous reqwest path exposed:
///
/// - `Transport` with an `ECONNREFUSED` in the source chain → "admin
///   isn't there" (check it's running / port).
/// - `RequestTimeout` → "admin is slow" (stuck lock, overloaded).
/// - `Call` → the server returned a JSON-RPC application error; surface
///   the code and message so the operator can tell a misconfigured
///   method name from a real server failure.
/// - Anything else → passed through with the URL as context.
pub(crate) fn classify_client_error(
    url: &str,
    timeout_ms: u64,
    err: JsonRpcClientError,
) -> anyhow::Error {
    match err {
        JsonRpcClientError::RequestTimeout => anyhow::anyhow!(
            "admin at {url} did not respond within {timeout_ms}ms; \
             the node may be overloaded or blocked on a long lock hold",
        ),
        JsonRpcClientError::Transport(inner) => {
            if is_connection_refused(inner.as_ref()) {
                anyhow::anyhow!(
                    "admin at {url} refused the connection ({inner}); is the node \
                     running, and is admin_port configured correctly?",
                )
            } else {
                anyhow::anyhow!("admin request to {url} failed: {inner}")
            }
        }
        JsonRpcClientError::Call(obj) => anyhow::anyhow!(
            "admin at {url} returned JSON-RPC error {code}: {msg}",
            code = obj.code(),
            msg = obj.message(),
        ),
        other => anyhow::Error::new(other).context(format!("admin request to {url} failed")),
    }
}

/// Walk the `source()` chain looking for an `std::io::Error` of kind
/// `ConnectionRefused`. The hyper/jsonrpsee error hierarchy is several
/// layers deep and the exact intermediate types are implementation
/// details, so match on the innermost `io::Error` kind instead of any
/// particular transport type.
pub(crate) fn is_connection_refused(err: &(dyn std::error::Error + 'static)) -> bool {
    io_error_kind_in_source_chain(err, |kind| kind == std::io::ErrorKind::ConnectionRefused)
}

/// Walk the `source()` chain looking for any `std::io::Error` whose
/// kind indicates the admin server's listener (or an already-accepted
/// connection) has gone away. Used by the `--wait` polling loop to
/// detect "admin closed after `router.shutdown` returned" — which is
/// the late-stop success terminal. `ConnectionRefused` is the most
/// common (new connection rejected because listener dropped), but
/// `ConnectionReset` / `ConnectionAborted` / `UnexpectedEof` also
/// occur when a poll lands mid-response as the server tears down.
/// Without these, the polling loop would treat a race-window error
/// as fatal and turn drain completion into a non-zero exit.
fn is_admin_closed_transport_error(err: &(dyn std::error::Error + 'static)) -> bool {
    io_error_kind_in_source_chain(err, |kind| {
        matches!(
            kind,
            std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::UnexpectedEof
        )
    })
}

/// Helper: walk the `source()` chain, return `true` if any layer is
/// an `io::Error` whose kind matches `predicate`.
fn io_error_kind_in_source_chain(
    err: &(dyn std::error::Error + 'static),
    predicate: impl Fn(std::io::ErrorKind) -> bool,
) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = current {
        if let Some(io_err) = e.downcast_ref::<std::io::Error>()
            && predicate(io_err.kind())
        {
            return true;
        }
        current = e.source();
    }
    false
}

/// Resolve the admin URL in this precedence order:
///
/// 1. `--admin-url` flag or the `DECDN_ADMIN_URL` env var (clap folds the
///    env into `args.admin_url`).
/// 2. `DECDN_ADMIN_PORT` — the same env var the daemon reads for its admin
///    bind (`common/src/cli/run.rs`). An env-only deployment (port set via
///    env, no config file) would otherwise be unreachable: the CLI would
///    dial the default 9191 and report "is the node running?" (#864). A
///    malformed or `0` value errors rather than silently falling through.
/// 3. `observability.admin_port` from the TOML config file — either the
///    caller's explicit path or the default `~/.decdn/node.toml`. An
///    explicit path that doesn't exist is an error; a missing default
///    path falls through. `admin_port = 0` in the file is an operator
///    opt-out and errors here rather than silently probing the default.
/// 4. Default `http://127.0.0.1:9191`.
pub(crate) fn resolve_admin_url(
    flag: Option<&str>,
    config_path: Option<&Path>,
) -> anyhow::Result<String> {
    if let Some(url) = flag {
        return Ok(url.to_string());
    }
    if let Some(url) = admin_url_from_env(std::env::var("DECDN_ADMIN_PORT").ok())? {
        return Ok(url);
    }
    let (resolved_path, source) = match config_path {
        Some(p) => (Some(expand_tilde(p)), ConfigPathSource::Explicit),
        None => (
            cli::common::default_config_path(),
            ConfigPathSource::Default,
        ),
    };
    let port =
        port_from_config_file(resolved_path.as_deref(), source)?.unwrap_or(DEFAULT_ADMIN_PORT);
    Ok(format!("http://127.0.0.1:{port}"))
}

/// Build the loopback admin URL from the raw `DECDN_ADMIN_PORT` env value, or
/// `None` when the var is unset. Pure (takes the value rather than reading the
/// environment) so the parse/validation logic is testable without mutating
/// process-global state. A non-numeric value or `0` (the daemon's "admin
/// disabled" sentinel) is an error — both indicate misconfiguration the
/// operator should see rather than have masked by a fall-through to the
/// config file or the built-in default.
fn admin_url_from_env(raw: Option<String>) -> anyhow::Result<Option<String>> {
    let Some(raw) = raw else { return Ok(None) };
    let port: u16 = raw
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("DECDN_ADMIN_PORT is not a valid port number: {raw:?}"))?;
    if port == 0 {
        anyhow::bail!(
            "DECDN_ADMIN_PORT=0 disables the admin server; pass --admin-url or set a non-zero port"
        );
    }
    Ok(Some(format!("http://127.0.0.1:{port}")))
}

/// Read `observability.admin_port` from a TOML config file. Returns:
/// - `Ok(None)` when `path` is `None`, or when `path` is the *default*
///   path and the file doesn't exist (operator hasn't set up a config
///   file yet — fall through to the built-in default).
/// - `Ok(Some(port))` for a positive port value.
/// - `Err` if the file exists but can't be parsed, if the operator
///   explicitly set `admin_port = 0` (which disables the server), or if
///   `source` is `Explicit` and the file is missing/unreadable.
///
/// Callers are expected to have already tilde-expanded the path;
/// `resolve_admin_url` is the only intended caller and does so via
/// [`expand_tilde`] / [`cli::common::default_config_path`].
fn port_from_config_file(
    path: Option<&Path>,
    source: ConfigPathSource,
) -> anyhow::Result<Option<u16>> {
    let Some(path) = path else { return Ok(None) };
    let path: PathBuf = path.to_path_buf();
    match std::fs::read_to_string(&path) {
        Ok(contents) => {
            // Deliberately a partial deserializer: parsing the full
            // `FileConfig` would couple these commands to every
            // unrelated field's well-formedness. An operator with a
            // typo'd `[payments]` table shouldn't lose the ability to
            // resolve the admin port. Serde's TOML mode ignores unknown
            // fields by default, so this only fails on (a) genuinely
            // malformed TOML or (b) a wrong type for
            // `observability.admin_port` itself — both of which we
            // genuinely want to surface.
            let parsed: AdminPortConfig = toml::from_str(&contents)
                .with_context(|| format!("failed to parse config file {}", path.display()))?;
            match parsed.observability.and_then(|o| o.admin_port) {
                Some(0) => anyhow::bail!(
                    "config {} disables the admin server (observability.admin_port = 0); \
                     pass --admin-url or enable the admin port",
                    path.display()
                ),
                other => Ok(other),
            }
        }
        Err(err) => match (err.kind(), source) {
            // Default path not yet created: fall through to the built-in.
            (io::ErrorKind::NotFound, ConfigPathSource::Default) => Ok(None),
            // Permission problems usually point at file mode / ownership,
            // not a wrong path — give the operator that nudge rather than
            // a generic "failed to read".
            (io::ErrorKind::PermissionDenied, _) => Err(anyhow::anyhow!(
                "cannot read config file {}: permission denied; check file mode and ownership",
                path.display()
            )),
            _ => Err(anyhow::anyhow!(
                "failed to read config file {}: {err}",
                path.display()
            )),
        },
    }
}

fn short_node_id(hex: &str) -> String {
    // Unicode '…' (U+2026) rather than "..." so a pasted preview is
    // unambiguously a preview and never parses as hex.
    let mut chars = hex.chars();
    let prefix: String = chars.by_ref().take(12).collect();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

fn wall_clock_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

/// Coarse operator-facing age bucket. On-call use case is "is this peer
/// fresh in the last N {s,m,h,d}?"; sub-second precision would only add
/// noise to the table.
fn relative_age(now_us: u64, last_seen_us: u64) -> String {
    if last_seen_us == 0 {
        return "unknown".to_string();
    }
    if last_seen_us > now_us {
        // Clock skew: report as "in the future" rather than a huge negative
        // unsigned delta. Honest about the condition without panicking.
        return "in future".to_string();
    }
    let delta_us = now_us - last_seen_us;
    format_age(delta_us)
}

fn format_age(delta_us: u64) -> String {
    const US_PER_SEC: u64 = 1_000_000;
    const US_PER_MIN: u64 = 60 * US_PER_SEC;
    const US_PER_HOUR: u64 = 60 * US_PER_MIN;
    const US_PER_DAY: u64 = 24 * US_PER_HOUR;
    if delta_us < US_PER_SEC {
        return "<1s ago".to_string();
    }
    if delta_us < US_PER_MIN {
        return format!("{}s ago", delta_us / US_PER_SEC);
    }
    if delta_us < US_PER_HOUR {
        return format!("{}m ago", delta_us / US_PER_MIN);
    }
    if delta_us < US_PER_DAY {
        return format!("{}h ago", delta_us / US_PER_HOUR);
    }
    format!("{}d ago", delta_us / US_PER_DAY)
}

/// Write the DHT status report to `w`. Pure function (takes `&mut impl
/// Write`) so the formatting is unit-testable without an HTTP hop,
/// mirroring [`write_lanes_table`]. The summary block uses stable
/// `key=value` tokens so operator scripts can `grep` them without
/// `--json`; the per-non-empty-bucket fill table follows.
fn write_status(w: &mut impl io::Write, s: &StatusResponse, now_us: u64) -> io::Result<()> {
    writeln!(w, "node_id={}", s.node_id)?;
    // The operator wallet this node runs under. `None` when the node reports no
    // resolvable operator (no chain wiring); render an explicit sentinel rather
    // than omitting the line, so the field is always present for scripts.
    writeln!(
        w,
        "operator_address={}",
        s.operator_address.as_deref().unwrap_or("(unknown)")
    )?;
    writeln!(w, "known_stakers={}", s.known_stakers)?;
    let last_refresh = match s.routing.last_refresh_us {
        // No bucket-refresh pass has completed yet (node up < one interval).
        None => "never".to_string(),
        Some(us) => relative_age(now_us, us),
    };
    writeln!(
        w,
        "routing total_peers={} non_empty_buckets={} refresh_interval={} last_refresh={}",
        s.routing.total_peers,
        s.routing.non_empty_buckets,
        format_interval(s.routing.refresh_interval_s),
        last_refresh,
    )?;
    writeln!(
        w,
        "record_store records={}/{} ({})",
        s.record_store.records,
        s.record_store.capacity,
        percent(s.record_store.records, s.record_store.capacity),
    )?;
    writeln!(
        w,
        "republish scheduled_records={}",
        s.republish.scheduled_records
    )?;
    writeln!(w, "chain_denied_origins={}", s.chain_denied_origins)?;

    if s.routing.buckets.is_empty() {
        return writeln!(w, "(routing table empty — no buckets populated)");
    }
    // Capacity is the same K for every bucket — carried once on RoutingHealth.
    let capacity = s.routing.bucket_capacity;
    writeln!(w)?;
    writeln!(w, "{:<8} {:<8} FILL%", "BUCKET", "FILL")?;
    for b in &s.routing.buckets {
        let fill = format!("{}/{capacity}", b.fill);
        let pct = percent(u64::from(b.fill), u64::from(capacity));
        writeln!(w, "{:<8} {fill:<8} {pct}", b.index)?;
    }
    Ok(())
}

/// Integer percentage of `num/den` as an `"NN%"` string. A `den` of 0
/// (which should never happen for a Kademlia bucket capacity or the
/// record-store global cap) renders `"n/a"` rather than dividing by zero.
/// `saturating_mul` guards the (unrealistic) `num * 100` overflow.
fn percent(num: u64, den: u64) -> String {
    if den == 0 {
        return "n/a".to_string();
    }
    format!("{}%", num.saturating_mul(100) / den)
}

/// Format a whole-second interval as a coarse human string (e.g. `"1h"`,
/// `"30m"`, `"45s"`). Uses the same unit set as `format_age` (s/m/h/d) but
/// selects the coarsest unit that divides *exactly* — so a non-round
/// interval like 90 minutes renders `"90m"`, not `"1h"` — keeping the
/// reported bucket-refresh cadence precise.
fn format_interval(secs: u64) -> String {
    const SEC_PER_MIN: u64 = 60;
    const SEC_PER_HOUR: u64 = 60 * SEC_PER_MIN;
    const SEC_PER_DAY: u64 = 24 * SEC_PER_HOUR;
    if secs == 0 {
        return "0s".to_string();
    }
    if secs.is_multiple_of(SEC_PER_DAY) {
        return format!("{}d", secs / SEC_PER_DAY);
    }
    if secs.is_multiple_of(SEC_PER_HOUR) {
        return format!("{}h", secs / SEC_PER_HOUR);
    }
    if secs.is_multiple_of(SEC_PER_MIN) {
        return format!("{}m", secs / SEC_PER_MIN);
    }
    format!("{secs}s")
}

/// `decdn node lookup` — unpaid client-side discovery of active nodes via
/// `CapacityBond.getRegisteredNodes` (#1481). Unlike `register`/`bond`/`unbond`/
/// `deregister`, this never loads a keystore: [`discovery::active_nodes`]
/// builds its own signer-less read-only provider, so `chain_ctx::resolve`'s
/// `keystore`/`data_dir` outputs are simply unused here.
///
/// Filters (`--node-id`, `--region`) are applied by the pure, network-free
/// `filter_candidates`. With `--probe`, the result, sampled down to `SELECT_K`, is
/// ranked by measured `cdn/probe/v1` round-trip time via `probe_and_rank`;
/// without it, candidates are printed as listed, with no RTT.
pub async fn lookup(args: &cli::LookupArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let config_path = args.chain.common.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve(&args.chain, &file)?;

    let node_id = args
        .node_id
        .as_deref()
        .map(|s| {
            PublicKey::from_str(s).map_err(|e| anyhow::anyhow!("invalid --node-id {s:?}: {e}"))
        })
        .transpose()?;
    let region = args
        .region
        .as_deref()
        .map(|raw| {
            Region::parse(raw).ok_or_else(|| {
                anyhow::anyhow!("invalid --region {raw:?}: not an accepted ISO 3166-1 alpha-2 code")
            })
        })
        .transpose()?;

    let candidates = discovery::active_nodes(&resolved.rpc_url, resolved.capacity_bond_address)
        .await
        .with_context(|| {
            format!(
                "failed to read active nodes from CapacityBond at {}",
                resolved.capacity_bond_address
            )
        })?;
    let filtered = filter_candidates(candidates, node_id, region);

    let rows = if args.probe {
        // Cap probe fan-out the same way the paid discovery path does
        // (`select_candidates`, `SELECT_K`) — probing every active node on
        // the network does not scale. The shortlist is a random sample of
        // `SELECT_K`, not the registry's leading entries. The region sort is
        // always a no-op here: with `--region` every survivor of
        // `filter_candidates` already matches, and without it no client
        // region is passed. Only the shuffle and the cap apply.
        let shortlisted = select_candidates(filtered, args.region.as_deref(), SELECT_K);
        probe_and_rank(shortlisted, config_path, args.timeout_ms).await?
    } else {
        filtered.into_iter().map(LookupRow::from).collect()
    };

    let mut out = io::stdout().lock();
    if args.chain.common.json {
        let json_rows: Vec<LookupJson> = rows.iter().map(LookupJson::from).collect();
        serde_json::to_writer_pretty(&mut out, &json_rows)
            .context("failed to encode lookup result as JSON")?;
        writeln!(out)?;
    } else {
        write_lookup_table(&mut out, &rows).context("failed to write lookup table")?;
    }
    Ok(())
}

/// Network-free filter over active-node candidates: exact `node_id` match
/// and/or exact `region` match. `None` on either axis means "no filter on
/// that axis" — with both `None` every candidate passes through unchanged.
/// Kept pure and separate from [`lookup`] so it is unit-testable without a
/// chain or network (#1481).
fn filter_candidates(
    candidates: Vec<NodeCandidate>,
    node_id: Option<PublicKey>,
    region: Option<Region>,
) -> Vec<NodeCandidate> {
    candidates
        .into_iter()
        .filter(|c| node_id.is_none_or(|id| c.node_id == id))
        .filter(|c| region.is_none_or(|r| c.region_hint == Some(r)))
        .collect()
}

/// One row of `decdn node lookup` output: a candidate plus its measured RTT,
/// when probed. `rtt_ms` is `None` both for the unprobed listing path and for
/// a probed candidate that did not answer.
struct LookupRow {
    node_id: PublicKey,
    eth_address: Address,
    region_hint: Option<Region>,
    rtt_ms: Option<f64>,
}

impl From<NodeCandidate> for LookupRow {
    fn from(c: NodeCandidate) -> Self {
        Self {
            node_id: c.node_id,
            eth_address: c.eth_address,
            region_hint: c.region_hint,
            rtt_ms: None,
        }
    }
}

/// Probe each of `candidates` over `cdn/probe/v1` for a fixed sentinel hash
/// and sort ascending by measured RTT (decision per task brief: no blob needs
/// to exist at the sentinel hash — an absent-hash probe still returns a full
/// signed response, so any hash works for RTT). A candidate that cannot be
/// reached (offline, no route, timeout) is kept at the end with `rtt_ms:
/// None` and a warning on stderr rather than dropped or failing the whole
/// lookup — the point of `--probe` is to rank the reachable subset, not to
/// require every candidate to answer.
async fn probe_and_rank(
    candidates: Vec<NodeCandidate>,
    config_path: Option<&Path>,
    timeout_ms: u64,
) -> anyhow::Result<Vec<LookupRow>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    // Same client-endpoint construction as `decdn probe`: discovery-enabled,
    // so a target resolves by node-id via `[network.discovery]` / the n0
    // default even without an explicit relay/addr.
    let relays = client_endpoint::resolve_relays(None, config_path)?;
    let discovery_cfg = client_endpoint::client_discovery(config_path)?;
    let endpoint = client_endpoint::client_endpoint(&relays, &discovery_cfg).await?;
    let timeout = Duration::from_millis(timeout_ms);
    let hash = *blake3::hash(b"decdn-node-lookup-sentinel/v1").as_bytes();

    let mut rows = Vec::with_capacity(candidates.len());
    for c in candidates {
        let mut target = EndpointAddr::new(c.node_id);
        if let Some(url) = relays.first() {
            target = target.with_relay_url(url.clone());
        }
        let target = discovery::with_dial_addrs(target, &c);
        let timestamp_us = wall_clock_us();
        match probe_once(&endpoint, target, hash, timestamp_us, timeout).await {
            Ok((_, _, rtt)) => rows.push(LookupRow {
                node_id: c.node_id,
                eth_address: c.eth_address,
                region_hint: c.region_hint,
                rtt_ms: Some(rtt.ms),
            }),
            Err(e) => {
                eprintln!(
                    "warning: probe of node {} failed: {e}",
                    short_node_id(&node_id_hex(&c.node_id))
                );
                rows.push(LookupRow {
                    node_id: c.node_id,
                    eth_address: c.eth_address,
                    region_hint: c.region_hint,
                    rtt_ms: None,
                });
            }
        }
    }
    endpoint.close().await;

    // Ascending by RTT; unreachable candidates (`None`) sort last, ties among
    // them preserving probe order.
    rows.sort_by(|a, b| match (a.rtt_ms, b.rtt_ms) {
        (Some(x), Some(y)) => x.total_cmp(&y),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    Ok(rows)
}

/// `0x`-prefixed hex encoding of an iroh node id, matching the `0x…` shape
/// [`LookupJson::node_id`] emits.
fn node_id_hex(node_id: &PublicKey) -> String {
    format!("0x{}", alloy::primitives::hex::encode(node_id.as_bytes()))
}

/// Write the `decdn node lookup` result as a human table. Pure (`&mut impl
/// Write`) so the layout is unit-testable without a chain or network,
/// mirroring [`write_lanes_table`]. The RTT column
/// is only rendered when at least one row was probed, so the unprobed listing
/// path doesn't show a column of nothing.
fn write_lookup_table(w: &mut impl io::Write, rows: &[LookupRow]) -> io::Result<()> {
    writeln!(w, "nodes={}", rows.len())?;
    if rows.is_empty() {
        return writeln!(w, "(no matching active nodes)");
    }
    let probed = rows.iter().any(|r| r.rtt_ms.is_some());
    if probed {
        writeln!(
            w,
            "{:<14} {:<44} {:<8} {:>12}",
            "NODE_ID", "ETH_ADDRESS", "REGION", "RTT_MS"
        )?;
    } else {
        writeln!(w, "{:<14} {:<44} {:<8}", "NODE_ID", "ETH_ADDRESS", "REGION")?;
    }
    for r in rows {
        let node_preview = short_node_id(&node_id_hex(&r.node_id));
        let region = r
            .region_hint
            .map_or_else(|| "?".to_string(), |x| x.to_string());
        if probed {
            let rtt = r
                .rtt_ms
                .map_or_else(|| "unreachable".to_string(), |ms| format!("{ms:.1}"));
            writeln!(
                w,
                "{node_preview:<14} {:<44} {region:<8} {rtt:>12}",
                r.eth_address
            )?;
        } else {
            writeln!(w, "{node_preview:<14} {:<44} {region:<8}", r.eth_address)?;
        }
    }
    Ok(())
}

/// `--json` array element for `decdn node lookup` — mirrors the
/// `lane list --json` convention (stable string encoding, one Serialize
/// view per row). `node_id`/`eth_address` are `0x…` hex strings;
/// `region_hint` is `null` for a node with no (or unparseable) region hint;
/// `rtt_ms` is omitted entirely from the object whenever it is unknown — both
/// the unprobed listing path and a probed-but-unreachable candidate leave it
/// unset, so a consumer cannot distinguish "not probed" from "probed and
/// didn't answer" from the JSON alone (both print a stderr warning in the
/// latter case on the human path; there is currently no JSON-side signal).
#[derive(Serialize)]
struct LookupJson {
    node_id: String,
    eth_address: String,
    region_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rtt_ms: Option<f64>,
}

impl From<&LookupRow> for LookupJson {
    fn from(r: &LookupRow) -> Self {
        Self {
            node_id: node_id_hex(&r.node_id),
            eth_address: r.eth_address.to_string(),
            region_hint: r.region_hint.map(|region| region.to_string()),
            rtt_ms: r.rtt_ms,
        }
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
