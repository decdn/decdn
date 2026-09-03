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

use decdn_client_pull::discovery::{self, NodeCandidate, SELECT_K, select_candidates};
use decdn_client_pull::endpoint as client_endpoint;
use decdn_client_pull::probe::probe_once;
use decdn_common::admin::{
    AdminRpcClient, BindingStatus, DrainRequest, DrainResponse, EvictRequest, EvictResponse,
    HealthResponse, LaneSnapshot, LanesResponse, ReloadResponse, SlashesResponse, StatusResponse,
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
        // Distinct from "0" (a legitimately empty blob); operators
        // seeing `not_stored` know the iroh-blobs status reported
        // `NotFound`, not `Complete { size: 0 }`.
        None => writeln!(w, "size_bytes=not_stored")?,
    }
    match resp.preview.last_accessed_us_ago {
        Some(us) => writeln!(w, "last_accessed={}", format_age(us))?,
        // Distinct from "<1s ago"; operators want to know the
        // engine has *no* access record vs a very recent one.
        None => writeln!(w, "last_accessed=never")?,
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
/// graceful shutdown (issue #244, ADR 025).
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
fn classify_client_error(url: &str, timeout_ms: u64, err: JsonRpcClientError) -> anyhow::Error {
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
fn is_connection_refused(err: &(dyn std::error::Error + 'static)) -> bool {
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
/// `filter_candidates`. With `--probe`, the (region-shortlisted) result is
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
        // the network does not scale. The region reorder is a no-op here
        // when `--region` was also passed (already exact-filtered above);
        // it only matters when `--node-id`/`--region` left more than
        // `SELECT_K` candidates.
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
        let timestamp_us = wall_clock_us();
        match probe_once(&endpoint, target, hash, timestamp_us, timeout).await {
            Ok((_, _, rtt_ms)) => rows.push(LookupRow {
                node_id: c.node_id,
                eth_address: c.eth_address,
                region_hint: c.region_hint,
                rtt_ms: Some(rtt_ms),
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
mod tests {
    use super::*;
    use decdn_common::admin::SlashRecordDto;

    fn health_response(binding: BindingStatus, bound: Option<&str>) -> HealthResponse {
        health_response_with_registry(binding, bound, true)
    }

    fn health_response_with_registry(
        binding: BindingStatus,
        bound: Option<&str>,
        registry_active: bool,
    ) -> HealthResponse {
        HealthResponse {
            node_id: "aa".repeat(32),
            uptime_s: 42,
            in_flight_streams: 0,
            binding,
            bound_node_id: bound.map(str::to_string),
            registry_active,
        }
    }

    fn health_lines(resp: &HealthResponse) -> String {
        let mut buf = Vec::new();
        write_health(&mut buf, resp).expect("write to a Vec cannot fail");
        String::from_utf8(buf).expect("output is ASCII")
    }

    /// The pre-#1034 two-line contract still holds — operator scripts grep
    /// these — and the binding line is additive.
    #[test]
    fn health_keeps_node_id_and_uptime_lines() {
        let s = health_lines(&health_response(
            BindingStatus::Bound,
            Some(&"aa".repeat(32)),
        ));
        assert!(s.contains(&format!("node_id={}", "aa".repeat(32))), "{s}");
        assert!(s.contains("uptime_s=42"), "{s}");
    }

    /// A mismatch is the unslashable state, and the bound id is what the
    /// operator needs to act on it — printing the status without the id would
    /// report a problem and withhold the fix.
    #[test]
    fn health_names_the_bound_id_on_a_mismatch() {
        let bound = "bb".repeat(32);
        let s = health_lines(&health_response(BindingStatus::Mismatch, Some(&bound)));
        assert!(s.contains("binding=mismatch"), "{s}");
        assert!(s.contains(&format!("bound_node_id={bound}")), "{s}");
    }

    /// `registry_active` must print in BOTH states, for the same reason
    /// `binding` does: a script grepping for the failure has to be able to tell
    /// "checked, and this node cannot sell" from "the line is missing" (#1030).
    /// The two fields are independent — a node can be correctly `bound` and
    /// still be out of the active set (deregistered, ejected, unbonding, or
    /// bond below `minBond`), which is exactly the case an operator misreads
    /// without this line.
    #[test]
    fn health_prints_registry_active_in_both_states() {
        let serving = health_lines(&health_response_with_registry(
            BindingStatus::Bound,
            Some(&"aa".repeat(32)),
            true,
        ));
        assert!(serving.contains("registry_active=true"), "{serving}");

        let idle = health_lines(&health_response_with_registry(
            BindingStatus::Bound,
            Some(&"aa".repeat(32)),
            false,
        ));
        assert!(
            idle.contains("registry_active=false"),
            "a bound node that is out of the active set must still say so: {idle}"
        );
    }

    /// `unknown` must print. Omitting the line for the not-checked case would
    /// let a script that greps for `binding=mismatch` read "we never checked"
    /// as "all clear" — the exact conflation the status enum exists to stop.
    #[test]
    fn health_prints_unknown_rather_than_omitting_the_line() {
        let s = health_lines(&health_response(BindingStatus::Unknown, None));
        assert!(s.contains("binding=unknown"), "{s}");
        assert!(
            !s.contains("bound_node_id="),
            "there is no bound id to name: {s}"
        );
    }

    /// The plain and `--json` renderings must spell the status the same way,
    /// or an operator switching between them sees two vocabularies.
    #[test]
    fn binding_label_matches_the_serde_spelling() {
        for status in [
            BindingStatus::Bound,
            BindingStatus::Mismatch,
            BindingStatus::Unbound,
            BindingStatus::Unknown,
        ] {
            let json = serde_json::to_string(&status).expect("status serializes");
            assert_eq!(
                json.trim_matches('"'),
                binding_label(status),
                "plain and JSON renderings disagree for {status:?}"
            );
        }
    }

    /// `emit_drain_complete` JSON shape, polled-to-zero branch:
    /// `admin_closed=false`. Asserts the exact JSON object so
    /// machine consumers can rely on a fixed schema across both
    /// terminal-success paths (#662 review).
    #[test]
    fn emit_drain_complete_json_admin_closed_false() {
        let mut buf = Vec::new();
        emit_drain_complete(&mut buf, true, false).expect("write succeeds");
        let s = String::from_utf8(buf).expect("utf8");
        let parsed: serde_json::Value = serde_json::from_str(s.trim()).expect("valid json");
        assert_eq!(
            parsed,
            serde_json::json!({
                "drain_complete": true,
                "in_flight_streams": 0,
                "admin_closed": false,
            })
        );
    }

    /// JSON shape, ECONNREFUSED branch: `admin_closed=true`.
    #[test]
    fn emit_drain_complete_json_admin_closed_true() {
        let mut buf = Vec::new();
        emit_drain_complete(&mut buf, true, true).expect("write succeeds");
        let s = String::from_utf8(buf).expect("utf8");
        let parsed: serde_json::Value = serde_json::from_str(s.trim()).expect("valid json");
        assert_eq!(
            parsed,
            serde_json::json!({
                "drain_complete": true,
                "in_flight_streams": 0,
                "admin_closed": true,
            })
        );
    }

    /// Plain-text shape, both branches. Operator scripts parse the
    /// `key=value` form via grep, so the exact byte layout is a
    /// public contract — guard it with a literal assertion.
    #[test]
    fn emit_drain_complete_plain_shapes() {
        let mut buf = Vec::new();
        emit_drain_complete(&mut buf, false, false).expect("write succeeds");
        assert_eq!(
            String::from_utf8(buf).expect("utf8"),
            "drain_complete=true in_flight_streams=0 admin_closed=false\n",
        );

        let mut buf = Vec::new();
        emit_drain_complete(&mut buf, false, true).expect("write succeeds");
        assert_eq!(
            String::from_utf8(buf).expect("utf8"),
            "drain_complete=true in_flight_streams=0 admin_closed=true\n",
        );
    }

    /// `is_admin_closed_transport_error` walks the `source()` chain
    /// and matches the four io kinds that indicate the admin
    /// listener / accepted socket went away. A regression that
    /// dropped one of the kinds would falsely turn a race-window
    /// close into a non-zero exit from the `--wait` polling loop.
    #[test]
    fn is_admin_closed_transport_error_matches_close_like_kinds() {
        use std::io::{Error, ErrorKind};
        for kind in [
            ErrorKind::ConnectionRefused,
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::UnexpectedEof,
        ] {
            let io_err = Error::new(kind, "boom");
            // Wrap to exercise the source-chain walk, not a direct
            // downcast — production errors are typically several
            // layers deep (hyper / jsonrpsee / tower).
            let wrapped: Box<dyn std::error::Error + 'static> = Box::new(io_err);
            assert!(
                is_admin_closed_transport_error(wrapped.as_ref()),
                "kind {kind:?} must be classified as admin-closed",
            );
        }
        // Unrelated kinds must not match.
        for kind in [
            ErrorKind::Other,
            ErrorKind::PermissionDenied,
            ErrorKind::TimedOut,
        ] {
            let io_err = Error::new(kind, "boom");
            let wrapped: Box<dyn std::error::Error + 'static> = Box::new(io_err);
            assert!(
                !is_admin_closed_transport_error(wrapped.as_ref()),
                "kind {kind:?} must not be classified as admin-closed",
            );
        }
    }

    /// `is_connection_refused` keeps the original narrow semantics
    /// — only `ConnectionRefused` matches. Used by the non-wait
    /// `classify_client_error` path; the wider helper is only for
    /// the `--wait` poll loop.
    #[test]
    fn is_connection_refused_only_matches_refused() {
        use std::io::{Error, ErrorKind};
        let refused: Box<dyn std::error::Error + 'static> =
            Box::new(Error::new(ErrorKind::ConnectionRefused, "x"));
        assert!(is_connection_refused(refused.as_ref()));
        let reset: Box<dyn std::error::Error + 'static> =
            Box::new(Error::new(ErrorKind::ConnectionReset, "x"));
        assert!(!is_connection_refused(reset.as_ref()));
    }

    fn mk_status(last_refresh_us: Option<u64>) -> StatusResponse {
        use decdn_common::admin::{BucketStat, RecordStoreHealth, RepublishHealth, RoutingHealth};
        StatusResponse {
            node_id: "ab".repeat(32),
            operator_address: Some("0x52908400098527886E0F7030069857D2E4169EE7".to_string()),
            routing: RoutingHealth {
                total_peers: 21,
                non_empty_buckets: 2,
                buckets: vec![
                    BucketStat { index: 0, fill: 1 },
                    BucketStat {
                        index: 255,
                        fill: 20,
                    },
                ],
                bucket_capacity: 20,
                refresh_interval_s: 3_600,
                last_refresh_us,
            },
            known_stakers: 7,
            record_store: RecordStoreHealth {
                records: 50_000,
                capacity: 100_000,
            },
            republish: RepublishHealth {
                scheduled_records: 5,
            },
            chain_denied_origins: 3,
        }
    }

    #[test]
    fn write_status_renders_summary_and_bucket_table() -> anyhow::Result<()> {
        // last_seen 1s past epoch, now 1s later → "1s ago".
        let status = mk_status(Some(1_000_000));
        let mut buf = Vec::<u8>::new();
        write_status(&mut buf, &status, 2_000_000)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains(&format!("node_id={}", "ab".repeat(32))), "{s}");
        assert!(
            s.contains("operator_address=0x52908400098527886E0F7030069857D2E4169EE7"),
            "{s}"
        );
        assert!(s.contains("known_stakers=7"), "{s}");
        assert!(s.contains("total_peers=21"), "{s}");
        assert!(s.contains("non_empty_buckets=2"), "{s}");
        assert!(s.contains("refresh_interval=1h"), "{s}");
        assert!(s.contains("last_refresh=1s ago"), "{s}");
        // Record-store utilization: 50000/100000 → 50%.
        assert!(s.contains("record_store records=50000/100000 (50%)"), "{s}");
        assert!(s.contains("republish scheduled_records=5"), "{s}");
        assert!(s.contains("chain_denied_origins=3"), "{s}");
        // Bucket table: header + a full bucket at 100%.
        assert!(s.contains("BUCKET"), "{s}");
        assert!(s.contains("FILL%"), "{s}");
        assert!(s.contains("20/20"), "{s}");
        assert!(s.contains("100%"), "{s}");
        Ok(())
    }

    /// A node that reports no resolvable operator address (no chain wiring)
    /// renders an explicit sentinel rather than dropping the line, so scripts
    /// can always find the field.
    #[test]
    fn write_status_absent_operator_renders_unknown_sentinel() -> anyhow::Result<()> {
        let mut status = mk_status(Some(1_000_000));
        status.operator_address = None;
        let mut buf = Vec::<u8>::new();
        write_status(&mut buf, &status, 2_000_000)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("operator_address=(unknown)"), "{s}");
        Ok(())
    }

    #[test]
    fn write_status_never_refreshed_renders_never() -> anyhow::Result<()> {
        let status = mk_status(None);
        let mut buf = Vec::<u8>::new();
        write_status(&mut buf, &status, 2_000_000)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("last_refresh=never"), "{s}");
        Ok(())
    }

    /// Cold-start: a node that has joined no buckets yet renders the
    /// empty-table sentinel and omits the bucket table header entirely —
    /// the precise scenario `decdn node status` exists to diagnose.
    #[test]
    fn write_status_empty_routing_table_renders_sentinel() -> anyhow::Result<()> {
        let mut status = mk_status(None);
        status.routing.buckets.clear();
        status.routing.non_empty_buckets = 0;
        status.routing.total_peers = 0;
        let mut buf = Vec::<u8>::new();
        write_status(&mut buf, &status, 2_000_000)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("(routing table empty"), "{s}");
        assert!(
            !s.contains("BUCKET"),
            "empty table must omit the header: {s}"
        );
        assert!(!s.contains("FILL%"), "{s}");
        Ok(())
    }

    /// Clock skew between the node's stamp and the CLI's wall clock must
    /// render "in future" via `relative_age`, not a huge wrapped age.
    #[test]
    fn write_status_future_last_refresh_renders_in_future() -> anyhow::Result<()> {
        let status = mk_status(Some(5_000_000));
        let mut buf = Vec::<u8>::new();
        // now_us earlier than the stamp.
        write_status(&mut buf, &status, 1_000_000)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("last_refresh=in future"), "{s}");
        Ok(())
    }

    #[test]
    fn percent_handles_zero_denominator() {
        assert_eq!(percent(0, 0), "n/a");
        assert_eq!(percent(5, 0), "n/a");
        assert_eq!(percent(1, 20), "5%");
        assert_eq!(percent(20, 20), "100%");
    }

    #[test]
    fn format_interval_uses_coarsest_exact_unit() {
        assert_eq!(format_interval(0), "0s");
        assert_eq!(format_interval(45), "45s");
        assert_eq!(format_interval(30 * 60), "30m");
        assert_eq!(format_interval(3_600), "1h");
        assert_eq!(format_interval(2 * 86_400), "2d");
        // 90 minutes isn't a whole number of hours → falls back to minutes.
        assert_eq!(format_interval(90 * 60), "90m");
    }

    #[test]
    fn resolve_admin_url_prefers_flag() {
        let got = resolve_admin_url(Some("http://custom:1234"), None).expect("flag path ok");
        assert_eq!(got, "http://custom:1234");
    }

    #[test]
    fn admin_url_from_env_unset_is_none() {
        assert_eq!(admin_url_from_env(None).expect("unset ok"), None);
    }

    #[test]
    fn admin_url_from_env_valid_port_builds_loopback_url() {
        assert_eq!(
            admin_url_from_env(Some("9999".to_string())).expect("valid ok"),
            Some("http://127.0.0.1:9999".to_string())
        );
        // Surrounding whitespace is tolerated (env values can carry it).
        assert_eq!(
            admin_url_from_env(Some(" 9999 ".to_string())).expect("trimmed ok"),
            Some("http://127.0.0.1:9999".to_string())
        );
    }

    #[test]
    fn admin_url_from_env_zero_and_malformed_error() {
        assert!(admin_url_from_env(Some("0".to_string())).is_err());
        assert!(admin_url_from_env(Some("notaport".to_string())).is_err());
        // Out of u16 range.
        assert!(admin_url_from_env(Some("70000".to_string())).is_err());
    }

    #[test]
    fn port_from_config_file_missing_default_returns_none() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("absent.toml");
        assert_eq!(
            port_from_config_file(Some(&path), ConfigPathSource::Default)?,
            None
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_missing_explicit_errors() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("absent.toml");
        let err = port_from_config_file(Some(&path), ConfigPathSource::Explicit)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected explicit-missing error"))?
            .to_string();
        assert!(
            err.contains("failed to read config file"),
            "missing context: {err}"
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_reads_admin_port_default_path() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("node.toml");
        std::fs::write(&path, b"[observability]\nadmin_port = 12345\n")?;
        assert_eq!(
            port_from_config_file(Some(&path), ConfigPathSource::Default)?,
            Some(12345)
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_reads_admin_port_explicit_path() -> anyhow::Result<()> {
        // Explicit-source + valid file must succeed the same way as the
        // default-source case. Without this test, a regression that
        // broadened the explicit-source error arm to swallow successes
        // would still pass CI.
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("node.toml");
        std::fs::write(&path, b"[observability]\nadmin_port = 7777\n")?;
        assert_eq!(
            port_from_config_file(Some(&path), ConfigPathSource::Explicit)?,
            Some(7777)
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_errors_on_explicit_zero() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("node.toml");
        std::fs::write(&path, b"[observability]\nadmin_port = 0\n")?;
        let err = port_from_config_file(Some(&path), ConfigPathSource::Default)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected error for explicit 0"))?
            .to_string();
        assert!(
            err.contains("disables the admin server"),
            "missing context: {err}"
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_errors_on_invalid_toml() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("broken.toml");
        std::fs::write(&path, b"not = valid = toml")?;
        let err = port_from_config_file(Some(&path), ConfigPathSource::Default)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected parse error"))?
            .to_string();
        assert!(err.contains("parse"), "missing context: {err}");
        Ok(())
    }

    // Locks in the partial-deserializer choice: a wrong type in some
    // unrelated section (here, a malformed `[network]` field that the
    // full FileConfig would reject) must not stop these commands from
    // resolving the admin port. If a future refactor reverts to
    // parsing FileConfig, this test fails.
    #[test]
    fn port_from_config_file_ignores_unrelated_field_errors() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("node.toml");
        // `network.bind_addr` would be a string in the real schema;
        // making it an integer is a guaranteed type-mismatch for the
        // full FileConfig, but the narrow AdminPortConfig deserializer
        // never sees `network` so it must round-trip fine.
        std::fs::write(
            &path,
            b"[observability]\nadmin_port = 4242\n[network]\nbind_addr = 7\n",
        )?;
        assert_eq!(
            port_from_config_file(Some(&path), ConfigPathSource::Default)?,
            Some(4242)
        );
        Ok(())
    }

    fn mk_lane(
        pool_id: &str,
        counterparty: &str,
        last_nonce: u64,
        outstanding: u64,
        deposit: u64,
        secs_since: Option<u64>,
        eligible: bool,
    ) -> LaneSnapshot {
        LaneSnapshot {
            pool_id: pool_id.to_string(),
            counterparty: counterparty.to_string(),
            // Self-signing default; the delegated and legacy-empty renderings
            // get their own dedicated test rather than an eighth parameter.
            voucher_signer: counterparty.to_string(),
            last_nonce,
            outstanding_micro_usdc: outstanding,
            deposit_micro_usdc: deposit,
            seconds_since_last_voucher: secs_since,
            settlement_eligible: eligible,
        }
    }

    #[test]
    fn write_lanes_table_empty_emits_sentinel() -> anyhow::Result<()> {
        let resp = LanesResponse {
            lanes: Vec::new(),
            redeem_threshold_micro_usdc: 1_000_000,
        };
        let mut buf = Vec::<u8>::new();
        write_lanes_table(&mut buf, &resp)?;
        let s = String::from_utf8(buf)?;
        // Summary line still prints the threshold, then the sentinel.
        assert!(s.contains("redeem_threshold=1.000000"), "{s}");
        assert!(s.contains("lanes=0"), "{s}");
        assert!(s.contains("(no open lanes)"), "{s}");
        // No table header when there are no rows.
        assert!(!s.contains("LANE"), "header must be omitted: {s}");
        Ok(())
    }

    #[test]
    fn write_lanes_table_renders_header_and_rows() -> anyhow::Result<()> {
        // The DTO documents `counterparty` as an EIP-55 mixed-case
        // checksummed address (`alloy`'s `Address` Display), so the
        // fixture must be a real checksummed string — a lowercase
        // placeholder wouldn't exercise the mixed-case rendering the
        // table inherits verbatim. Derive it via `alloy` so the literal
        // is provably the canonical checksum, not a hand-typed guess.
        let counterparty =
            alloy::primitives::address!("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed").to_string();
        // Sanity-guard the fixture itself: the checksum is genuinely
        // mixed-case (some hex letters upper, some lower), so a regression
        // that lowercased it before rendering would be caught below.
        assert_ne!(
            counterparty,
            counterparty.to_lowercase(),
            "fixture must be a mixed-case EIP-55 address: {counterparty}"
        );
        let resp = LanesResponse {
            redeem_threshold_micro_usdc: 1_000_000,
            lanes: vec![
                mk_lane(
                    &format!("0x{}", "a".repeat(64)),
                    &counterparty,
                    7,
                    2_500_000,
                    10_000_000,
                    Some(90),
                    true,
                ),
                mk_lane(
                    &format!("0x{}", "c".repeat(64)),
                    &format!("0x{}", "d".repeat(40)),
                    0,
                    0,
                    5_000_000,
                    None,
                    false,
                ),
            ],
        };
        let mut buf = Vec::<u8>::new();
        write_lanes_table(&mut buf, &resp)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("lanes=2"), "{s}");
        assert!(s.contains("LANE"), "header missing: {s}");
        assert!(s.contains("OUTSTANDING"), "header missing: {s}");
        assert!(s.contains("ELIGIBLE"), "header missing: {s}");
        // First row: USDC-formatted amounts, 90s → "1m ago", eligible "yes".
        assert!(s.contains("2.500000"), "outstanding USDC missing: {s}");
        assert!(s.contains("10.000000"), "deposit USDC missing: {s}");
        assert!(s.contains("1m ago"), "last-voucher age missing: {s}");
        // Lane id preview is the short form.
        assert!(s.contains("0xaaaaaaaaaa"), "lane preview missing: {s}");
        // Counterparty preview is the short form AND preserves the EIP-55
        // mixed case verbatim — `short_node_id` truncates to 12 chars, so
        // assert the row carries that checksummed prefix unchanged (a
        // regression that lowercased the address would miss this).
        let cp_preview = short_node_id(&counterparty);
        assert!(
            cp_preview.chars().any(|ch| ch.is_ascii_uppercase()),
            "expected mixed-case counterparty preview: {cp_preview}"
        );
        assert!(
            s.contains(&cp_preview),
            "checksummed counterparty preview missing: {s}"
        );
        // Second row: no activity → "never", not eligible → "no".
        assert!(s.contains("never"), "never sentinel missing: {s}");
        Ok(())
    }

    #[test]
    fn write_slashes_table_empty_emits_sentinel() -> anyhow::Result<()> {
        let resp = SlashesResponse {
            slashes: Vec::new(),
        };
        let mut buf = Vec::<u8>::new();
        write_slashes_table(&mut buf, &resp)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("slashes=0"), "{s}");
        assert!(s.contains("(no slashes detected)"), "{s}");
        // No table header when there are no rows.
        assert!(!s.contains("SLASH_ID"), "header must be omitted: {s}");
        Ok(())
    }

    #[test]
    fn write_slashes_table_renders_header_and_rows() -> anyhow::Result<()> {
        let resp = SlashesResponse {
            slashes: vec![
                SlashRecordDto {
                    slash_id: "42".to_string(),
                    offense_type: 0,
                    amount: "1_000_000".to_string(),
                    evidence_hash: format!("0x{}", "a".repeat(64)),
                    block_number: Some(1_234_567),
                    appeal_window_close: Some(1_700_000_000),
                },
                SlashRecordDto {
                    slash_id: "43".to_string(),
                    offense_type: 1,
                    amount: "2_500_000".to_string(),
                    evidence_hash: format!("0x{}", "c".repeat(64)),
                    block_number: None,
                    appeal_window_close: None,
                },
            ],
        };
        let mut buf = Vec::<u8>::new();
        write_slashes_table(&mut buf, &resp)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("slashes=2"), "{s}");
        assert!(s.contains("SLASH_ID"), "header missing: {s}");
        assert!(s.contains("APPEAL_CLOSE"), "header missing: {s}");
        // First row: slash id, offense type, amount, block, evidence preview.
        assert!(s.contains("42"), "slash id missing: {s}");
        assert!(s.contains("1_000_000"), "amount missing: {s}");
        assert!(s.contains("1234567"), "block number missing: {s}");
        assert!(s.contains("0xaaaaaaaaaa"), "evidence preview missing: {s}");
        // Second row: missing block / appeal close render as "?".
        assert!(s.contains('?'), "missing-value sentinel missing: {s}");
        Ok(())
    }

    /// The SIGNER column: a delegated lane renders the delegate (not the
    /// funder), and a pre-delegation server — which omits `voucher_signer`
    /// entirely — renders `?` rather than echoing the funder.
    #[test]
    fn write_lanes_table_renders_the_voucher_signer_column() -> anyhow::Result<()> {
        let funder = format!("0x{}", "1".repeat(40));
        let delegate = format!("0x{}", "2".repeat(40));
        let mut delegated = mk_lane(
            &format!("0x{}", "a".repeat(64)),
            &funder,
            1,
            1,
            2,
            Some(1),
            false,
        );
        delegated.voucher_signer = delegate.clone();
        let mut legacy = mk_lane(
            &format!("0x{}", "b".repeat(64)),
            &funder,
            1,
            1,
            2,
            Some(1),
            false,
        );
        legacy.voucher_signer = String::new();

        let mut buf = Vec::<u8>::new();
        write_lanes_table(
            &mut buf,
            &LanesResponse {
                redeem_threshold_micro_usdc: 1_000_000,
                lanes: vec![delegated, legacy],
            },
        )?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("SIGNER"), "SIGNER header missing: {s}");
        assert!(
            s.contains(&short_node_id(&delegate)),
            "delegate signer preview missing: {s}"
        );
        let legacy_row = s
            .lines()
            .find(|l| l.starts_with("0xbbbbbbbbbb"))
            .unwrap_or_default();
        assert!(
            legacy_row.contains(" ? "),
            "an omitted voucher_signer must render as `?`: {legacy_row}"
        );
        assert_eq!(
            legacy_row.matches(&short_node_id(&funder)).count(),
            1,
            "the funder must appear once (COUNTERPARTY only) — an omitted \
             voucher_signer must not be echoed into SIGNER: {legacy_row}"
        );
        Ok(())
    }

    #[test]
    fn format_usdc_renders_six_decimals() {
        assert_eq!(format_usdc(0), "0.000000");
        assert_eq!(format_usdc(1_000_000), "1.000000");
        assert_eq!(format_usdc(2_500_000), "2.500000");
        assert_eq!(format_usdc(1), "0.000001");
        assert_eq!(format_usdc(12_345_678), "12.345678");
    }

    /// `decdn node lanes --json` serializes the `LanesResponse`
    /// DTO with `serde_json::to_string_pretty` (the seam the `--json`
    /// branch in [`lanes`] uses). Assert the pretty encoding (a)
    /// round-trips back to the same value and (b) carries every
    /// load-bearing field with its wire key, so a rename or a
    /// skipped-field regression on the DTO breaks here rather than only
    /// at the shell. The counterparty is a real EIP-55 checksummed
    /// address so the JSON reflects production output.
    #[test]
    fn lanes_json_pretty_roundtrips_and_carries_fields() -> anyhow::Result<()> {
        let counterparty =
            alloy::primitives::address!("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed").to_string();
        let resp = LanesResponse {
            redeem_threshold_micro_usdc: 1_000_000,
            lanes: vec![mk_lane(
                &format!("0x{}", "a".repeat(64)),
                &counterparty,
                7,
                2_500_000,
                10_000_000,
                Some(42),
                true,
            )],
        };

        // Exact seam the `--json` branch uses.
        let pretty = serde_json::to_string_pretty(&resp)?;
        // Pretty form is multi-line (indented) — guards against an
        // accidental switch to the compact encoder.
        assert!(
            pretty.contains('\n'),
            "pretty JSON must be multi-line: {pretty}"
        );

        // Round-trips back through the DTO with no lossy field — the
        // generated client deserializes this exact shape. (The DTO doesn't
        // derive `PartialEq`, so assert the reconstructed fields directly
        // rather than comparing whole structs.)
        let back: LanesResponse = serde_json::from_str(&pretty)?;
        assert_eq!(back.redeem_threshold_micro_usdc, 1_000_000);
        assert_eq!(back.lanes.len(), 1);
        let bc = back.lanes.first().expect("one lane");
        assert_eq!(bc.counterparty, counterparty);
        assert_eq!(bc.last_nonce, 7);
        assert_eq!(bc.outstanding_micro_usdc, 2_500_000);
        assert_eq!(bc.deposit_micro_usdc, 10_000_000);
        assert_eq!(bc.seconds_since_last_voucher, Some(42));
        assert!(bc.settlement_eligible);

        // Each wire key is present with the expected value, including the
        // checksummed counterparty verbatim (mixed-case preserved).
        let value: serde_json::Value = serde_json::from_str(&pretty)?;
        assert_eq!(value["redeem_threshold_micro_usdc"], 1_000_000);
        let chans = value["lanes"].as_array().expect("lanes array");
        assert_eq!(chans.len(), 1);
        let c0 = &chans[0];
        assert_eq!(c0["counterparty"].as_str(), Some(counterparty.as_str()));
        assert_eq!(c0["last_nonce"], 7);
        assert_eq!(c0["outstanding_micro_usdc"], 2_500_000);
        assert_eq!(c0["deposit_micro_usdc"], 10_000_000);
        assert_eq!(c0["seconds_since_last_voucher"], 42);
        assert_eq!(c0["settlement_eligible"], true);
        Ok(())
    }

    #[test]
    fn format_age_units() {
        assert_eq!(format_age(500_000), "<1s ago");
        assert_eq!(format_age(2_000_000), "2s ago");
        assert_eq!(format_age(90 * 1_000_000), "1m ago");
        assert_eq!(format_age(2 * 3600 * 1_000_000), "2h ago");
        assert_eq!(format_age(36 * 3600 * 1_000_000), "1d ago");
    }

    #[test]
    fn relative_age_handles_future_and_zero() {
        assert_eq!(relative_age(100, 200), "in future");
        assert_eq!(relative_age(100, 0), "unknown");
    }

    #[test]
    fn short_node_id_trims_long_hex() {
        let full = "a".repeat(64);
        let s = short_node_id(&full);
        assert_eq!(s.chars().count(), 13); // 12 hex + ellipsis
        assert!(s.ends_with('…'));
    }

    #[test]
    fn short_node_id_passthrough_when_already_short() {
        let s = short_node_id("abcd");
        assert_eq!(s, "abcd");
    }

    /// `--dry-run` plain output is multi-line, key=value, grep-friendly.
    /// Asserts every load-bearing field appears on its own line so a
    /// regression that collapsed the table back to one line (or dropped
    /// e.g. `pinned=`) breaks here rather than silently eating the
    /// information the operator needs to decide whether to run the real
    /// evict.
    #[test]
    fn write_dry_run_human_emits_all_fields() -> anyhow::Result<()> {
        use decdn_common::admin::EvictPreview;
        let resp = EvictResponse {
            was_present: true,
            dry_run: true,
            preview: EvictPreview {
                size_bytes: Some(1024),
                last_accessed_us_ago: Some(2_000_000), // 2s ago via format_age
                pinned: true,
                already_evicted: false,
                origin_kinds: vec![decdn_cache::OriginKind::Http],
            },
        };
        let mut buf = Vec::<u8>::new();
        write_dry_run_human(&mut buf, "abcd", &resp)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("hash=abcd"), "missing hash: {s}");
        assert!(s.contains("dry_run=true"), "missing dry_run tag: {s}");
        assert!(s.contains("was_present=true"), "missing was_present: {s}");
        assert!(s.contains("pinned=true"), "missing pinned: {s}");
        assert!(
            s.contains("already_evicted=false"),
            "missing already_evicted: {s}"
        );
        assert!(s.contains("size_bytes=1024"), "missing size_bytes: {s}");
        assert!(
            s.contains("last_accessed=2s ago"),
            "expected formatted last_accessed, got: {s}"
        );
        assert!(
            s.contains("origin_kinds=http"),
            "missing origin_kinds (#439, #284): {s}"
        );
        Ok(())
    }

    /// Sentinels for the "no information available" cases:
    /// `size_bytes=not_stored` and `last_accessed=never`. Distinct from
    /// "0" / "<1s ago" so an operator can tell "the engine has no
    /// record" from "the record is at the floor".
    #[test]
    fn write_dry_run_human_uses_sentinels_for_absent_fields() -> anyhow::Result<()> {
        use decdn_common::admin::EvictPreview;
        let resp = EvictResponse {
            was_present: false,
            dry_run: true,
            preview: EvictPreview {
                size_bytes: None,
                last_accessed_us_ago: None,
                pinned: false,
                already_evicted: false,
                // Cache-only mode: no origin configured, so the
                // dry-run reports `none` rather than omitting the
                // line entirely (#439).
                origin_kinds: Vec::new(),
            },
        };
        let mut buf = Vec::<u8>::new();
        write_dry_run_human(&mut buf, "deadbeef", &resp)?;
        let s = String::from_utf8(buf)?;
        assert!(
            s.contains("size_bytes=not_stored"),
            "expected not_stored sentinel, got: {s}"
        );
        assert!(
            s.contains("origin_kinds=none"),
            "expected origin_kinds=none sentinel for cache-only mode, got: {s}"
        );
        assert!(
            s.contains("last_accessed=never"),
            "expected never sentinel, got: {s}"
        );
        Ok(())
    }

    /// Multi-origin chain (#284): the dry-run preview surfaces every
    /// configured backend kind, comma-separated in declared order, so
    /// operators evaluating worst-case egress cost across a fallback
    /// chain see the full chain length and composition rather than
    /// only the primary's kind.
    #[test]
    fn write_dry_run_human_renders_multi_origin_kinds_in_declared_order() -> anyhow::Result<()> {
        use decdn_common::admin::EvictPreview;
        let resp = EvictResponse {
            was_present: true,
            dry_run: true,
            preview: EvictPreview {
                size_bytes: Some(512),
                last_accessed_us_ago: Some(1_500_000),
                pinned: false,
                already_evicted: false,
                origin_kinds: vec![
                    decdn_cache::OriginKind::Http,
                    decdn_cache::OriginKind::S3,
                    decdn_cache::OriginKind::Filesystem,
                ],
            },
        };
        let mut buf = Vec::<u8>::new();
        write_dry_run_human(&mut buf, "cafebabe", &resp)?;
        let s = String::from_utf8(buf)?;
        // Order is operator-controlled and load-bearing — assert the
        // exact comma-separated sequence rather than just substring
        // matches, so a regression that sorts or dedupes the list is
        // caught here.
        assert!(
            s.contains("origin_kinds=http,s3,filesystem"),
            "expected ordered comma-separated chain, got: {s}"
        );
        Ok(())
    }

    /// `filter_candidates` (#1481): network-free filtering behind `decdn node
    /// lookup`, unit-tested without a chain or network per the task brief.
    fn lookup_candidate(seed: u8, region: &str) -> decdn_client_pull::discovery::NodeCandidate {
        decdn_client_pull::discovery::NodeCandidate {
            node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            eth_address: alloy::primitives::Address::repeat_byte(seed),
            region_hint: decdn_protocol::Region::parse(region),
        }
    }

    #[test]
    fn filter_candidates_no_filter_returns_all() {
        let cands = vec![lookup_candidate(1, "US"), lookup_candidate(2, "EU")];
        let out = filter_candidates(cands.clone(), None, None);
        assert_eq!(out, cands);
    }

    #[test]
    fn filter_candidates_exact_node_id_match() {
        let a = lookup_candidate(1, "US");
        let b = lookup_candidate(2, "EU");
        let out = filter_candidates(vec![a.clone(), b], Some(a.node_id), None);
        assert_eq!(out, vec![a]);
    }

    #[test]
    fn filter_candidates_region_match() {
        let a = lookup_candidate(1, "US");
        let b = lookup_candidate(2, "EU");
        let c = lookup_candidate(3, "US");
        let out = filter_candidates(
            vec![a.clone(), b, c.clone()],
            None,
            decdn_protocol::Region::parse("US"),
        );
        assert_eq!(out, vec![a, c]);
    }

    #[test]
    fn filter_candidates_node_id_and_region_combine() {
        let a = lookup_candidate(1, "US");
        let b = lookup_candidate(2, "US");
        let out = filter_candidates(
            vec![a.clone(), b],
            Some(a.node_id),
            decdn_protocol::Region::parse("US"),
        );
        assert_eq!(out, vec![a]);
    }
}
