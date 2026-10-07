//! `decdn setup` — guided node onboarding wizard (ADR 019, #933).
//!
//! Thin orchestration over the existing primitives — `key-gen`, `node bond`,
//! `node register` — with the pre-flight checks that catch the
//! mis-ordered-step / under-funded-wallet failure class ADR 019 § Context
//! calls out *before* any transaction is submitted, plus a final on-chain
//! readiness summary. It introduces **no new on-chain logic**: every contract
//! call here is one the primitives already make (`bond::execute`,
//! `register::submit_registration`, and read-only `CapacityBond` views).
//!
//! Flow: pre-flight → confirm → bond → register → readiness. The signer is
//! built up front and shared across bond + register, so the keystore is
//! decrypted once for the on-chain phase. (On a first run that *generates*
//! keys, `key-gen` sets the password and the subsequent signer load decrypts
//! with it — two prompts only when the password isn't supplied via
//! `DECDN_KEYSTORE_PASSWORD` / `--keystore-password-file`.)

use std::io::{self, Write};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use anyhow::Context;
use decdn_common::cli;
use decdn_common::identity;
use decdn_incentive::Erc20;
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_incentive::eth_identity;

use crate::commands::{bond, chain_ctx, key_gen, register, terms};

/// Clock synchronization (ADR 019): the local clock should be within
/// 10 s of UTC before onboarding, catching a misconfigured host clock before
/// it affects time-sensitive on-chain interactions.
const CLOCK_SKEW_LIMIT_SECS: i64 = 10;

/// Conservative upper bound on the total gas for the on-chain onboarding transactions
/// (`approve` + `bond` + `declareMbps` + `registerNode`). Used only to size
/// the native-gas pre-flight check, so an over-estimate is the safe direction.
const PREFLIGHT_GAS_UNITS: u64 = 600_000;

/// Tri-state pre-flight outcome. `Warn` is non-blocking — it neither passes
/// silently (it renders distinctly from `Ok`) nor aborts a live run. Used for
/// the clock check when the reference time can't be determined.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CheckStatus {
    Ok,
    Warn,
    Fail,
}

impl CheckStatus {
    /// Human-mode marker.
    const fn mark(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warn => "WARN",
            Self::Fail => "FAIL",
        }
    }

    /// Whether this status blocks a live run. Only `Fail` blocks.
    const fn blocks(self) -> bool {
        matches!(self, Self::Fail)
    }
}

/// What the key pre-check decided. `Generate` means both keys are absent and
/// the default keystore path is in use (so `key-gen` can provision them).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum KeyAction {
    Present,
    Generate,
}

/// Raw pre-flight readings. Predicates are *derived* (methods), so a check flag
/// can never disagree with the value it tests — that's why this struct exists
/// rather than passing nine loose scalars + flags through the summary emitter.
struct Preflight {
    rpc_chain_id: u64,
    signing_chain_id: u64,
    clock_skew: Option<i64>,
    token_balance: U256,
    shortfall: U256,
    native_balance: U256,
    gas_needed: U256,
}

impl Preflight {
    /// Signing chain id matches the RPC's — a mismatch silently dooms every
    /// signature (they're signed for `signing_chain_id`).
    const fn chain_id_ok(&self) -> bool {
        self.rpc_chain_id == self.signing_chain_id
    }

    /// Clock check: within ±limit passes, beyond fails, undetermined warns.
    const fn clock_status(&self) -> CheckStatus {
        match self.clock_skew {
            None => CheckStatus::Warn,
            Some(s) if s.abs() <= CLOCK_SKEW_LIMIT_SECS => CheckStatus::Ok,
            Some(_) => CheckStatus::Fail,
        }
    }

    /// TOKEN balance covers the bond shortfall.
    fn token_ok(&self) -> bool {
        self.token_balance >= self.shortfall
    }

    /// Native-gas balance covers the estimated on-chain onboarding gas cost.
    fn native_ok(&self) -> bool {
        self.native_balance >= self.gas_needed
    }

    /// Every *blocking* check passes (a `Warn` does not block).
    fn all_ok(&self) -> bool {
        self.chain_id_ok() && !self.clock_status().blocks() && self.token_ok() && self.native_ok()
    }
}

/// Entry point for `decdn setup`.
// `run` is a linear onboarding script (pre-flight → confirm → bond → register →
// readiness); the branch count reads as complex to clippy but the control flow
// is top-to-bottom with no shared mutable state, so splitting it further would
// scatter the sequence rather than clarify it.
#[allow(clippy::too_many_lines, clippy::cognitive_complexity)]
pub async fn run(args: &cli::SetupArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let config_path = args.chain.common.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve(&args.chain, &file)?;
    let cb_addr = resolved.capacity_bond_address;
    let json = args.chain.common.json;
    let dry_run = args.chain.common.dry_run;

    // ---- Keys (idempotent — generate only on a clean slate). ----
    let key_path = identity::key_path(&resolved.data_dir);
    let default_keystore = eth_identity::keystore_path(&resolved.data_dir);
    let mut keys_generated = false;
    match precheck_keys(
        key_path.exists(),
        resolved.keystore.exists(),
        &resolved.keystore,
        &default_keystore,
    )? {
        KeyAction::Present => hline(json, "keys: present"),
        KeyAction::Generate => {
            if dry_run {
                hline(
                    json,
                    "keys: would generate (node key + eth keystore absent)",
                );
                return dry_run_without_keys(&resolved, cb_addr, args.mbps, json).await;
            }
            hline(json, "keys: generating (node key + eth keystore absent)");
            let kg = cli::KeyGenArgs {
                output_dir: Some(resolved.data_dir.clone()),
                force: false,
                keystore_password_file: args.chain.common.keystore_password_file.clone(),
            };
            key_gen::key_gen(&kg).context("key generation failed")?;
            keys_generated = true;
        }
    }

    // ---- Load the signer once; share it across bond + register. Derive the
    //      local node id for the register-divergence guard below. ----
    let signer = chain_ctx::load_operator_signer(&args.chain.common, &resolved.keystore).await?;
    let operator = signer.address();
    let provider = decdn_client::provider::build_provider(&resolved.rpc_url, &signer)?;
    let bond_contract = CapacityBond::new(cb_addr, &provider);
    let node_secret = identity::load_or_generate(&resolved.data_dir).with_context(|| {
        format!(
            "failed to load node key from {}",
            resolved.data_dir.display()
        )
    })?;
    let local_node_id = B256::from_slice(node_secret.public().as_bytes());

    // ---- Pre-flight reads (read-only; abort before any tx). ----
    if !json {
        println!("pre-flight checks:");
    }
    // RPC reachability is implicit in the first read; a failure here surfaces
    // as "rpc_url is not reachable" before anything else.
    let rpc_chain_id = provider
        .get_chain_id()
        .await
        .context("rpc_url is not reachable (failed to read chainId)")?;
    // `build_plan` validates the tier band and reads `bondRequired`/target/shortfall.
    let plan = bond::build_plan(&bond_contract, operator, U256::from(args.mbps), cb_addr).await?;
    let token_balance = Erc20::new(plan.token, &provider)
        .balanceOf(operator)
        .call()
        .await
        .with_context(|| format!("failed to read TOKEN balance from {}", plan.token))?;
    let native_balance = provider
        .get_balance(operator)
        .await
        .context("failed to read native-gas balance")?;
    let gas_price = provider
        .get_gas_price()
        .await
        .context("failed to read gas price")?;
    let clock_skew = measure_clock_skew(&resolved.rpc_url).await;

    // The bond-phase transactions. Over-estimate is the safe direction.
    let gas_needed = U256::from(PREFLIGHT_GAS_UNITS) * U256::from(gas_price);

    let pf = Preflight {
        rpc_chain_id,
        signing_chain_id: resolved.chain_id,
        clock_skew,
        token_balance,
        shortfall: plan.shortfall,
        native_balance,
        gas_needed,
    };

    check_line(
        json,
        "rpc",
        status(pf.chain_id_ok()),
        &format!("chainId {rpc_chain_id}, signing for {}", resolved.chain_id),
    );
    check_line(json, "clock", pf.clock_status(), &clock_detail(clock_skew));

    check_line(
        json,
        "token_balance",
        status(pf.token_ok()),
        &format!("need {} base units, hold {token_balance}", plan.shortfall),
    );
    check_line(
        json,
        "native_gas",
        status(pf.native_ok()),
        &format!("est need ~{gas_needed} wei, hold {native_balance}"),
    );

    // ---- Dry run: report and stop before submitting anything. ----
    if dry_run {
        // Human mode prints the bond plan inline; JSON mode emits a single
        // aggregated object below (no second JSON object from `write_plan`).
        if !json {
            println!("bond (dry-run):");
            let mut out = io::stdout().lock();
            bond::write_plan(&mut out, &plan, false, &bond::Outcome::default(), true)
                .context("failed to write dry-run bond plan")?;
        }
        let readiness = read_readiness(&bond_contract, operator).await?;
        let multiaddr_count = register::registration_multiaddrs(&args.multiaddrs, &file).len();
        emit_readiness(json, &readiness, &args.region, multiaddr_count);
        if json {
            println!(
                "{}",
                build_summary(
                    &pf,
                    keys_generated,
                    &plan,
                    &bond::Outcome::default(),
                    None,
                    true,
                    Some(&readiness),
                    None,
                    None,
                    &args.region,
                    multiaddr_count,
                )
            );
        }
        anyhow::ensure!(
            pf.all_ok(),
            "pre-flight checks failed (see above); resolve before a live run"
        );
        return Ok(());
    }

    // ---- Gate the live run on the pre-flight result. ----
    anyhow::ensure!(
        pf.all_ok(),
        "pre-flight checks failed (see above); nothing was submitted",
    );

    // ---- Register-divergence guard (before any tx). The operator must be
    //      unbound, or bound to *this* node key — never silently register a
    //      different key while reporting success. ----
    let bound = bond_contract
        .nodeIdOf(operator)
        .call()
        .await
        .with_context(|| format!("failed to read nodeIdOf from CapacityBond at {cb_addr}"))?;
    let already_registered = match registration_status(bound.nodeId, bound.active, local_node_id) {
        RegistrationStatus::Divergent => anyhow::bail!(
            "operator {operator:#x} is already bound on-chain to node {:#x}, but the local \
                 node key is {local_node_id:#x}; setup will not register a different key. Restore \
                 the bound key under {}, or run `decdn node deregister` first",
            bound.nodeId,
            resolved.data_dir.display(),
        ),
        RegistrationStatus::AlreadyRegistered => true,
        RegistrationStatus::Due => false,
    };

    // ---- ADR 019 § Terms Acceptance — accept the current operator terms
    //      *before* any transaction, so a refusal / stale-client abort happens
    //      before the bond is spent. `Some(hash)` means "a fresh registration is
    //      due and the operator accepted"; `None` means already registered. ----
    let terms_hash = accept_terms_if_registering(
        &bond_contract,
        cb_addr,
        already_registered,
        args.accept_terms,
    )
    .await?;

    // ---- Confirm before submitting (unless --yes). ----
    if !args.yes
        && !confirm(plan.shortfall, plan.target, args.mbps)
            .context("failed to read confirmation from stdin")?
    {
        anyhow::bail!(
            "aborted by operator (no confirmation); re-run with --yes for non-interactive use"
        );
    }

    // ---- Live phases (bond → register → read-back). ----
    //
    // Every outcome is owned HERE, outside the fallible section, so a sequence
    // that fails part-way is still reported (#1355). This matters most under
    // `--json`, where the aggregated summary below is the *only* carrier of the
    // bond transaction hashes: propagating straight out of a phase printed
    // nothing at all, leaving an operator whose TOKEN had been bonded with no
    // record of it. One reporting site rather than a bail-out per phase, so it
    // cannot go stale when a phase is added (#1355 review).
    let mut bond_outcome = bond::Outcome::default();
    let mut register_outcome: Option<register::RegisterOutcome> = None;
    // Separate from `register_outcome` because `submit_registration` returns its
    // outcome by value: on an unreadable receipt the `Err` takes the outcome
    // with it, and this slot is the only thing that survives to name the
    // in-flight registration in the partial summary.
    let mut register_tx: Option<B256> = None;
    let mut bond_reported = false;
    // Whether the bond phase itself completed. Gates the re-run guidance below:
    // after a successful bond, a later phase failing carries no bond hazard.
    let mut bond_done = false;

    let live: anyhow::Result<Readiness> = async {
        // ---- Bond (idempotent stake-to-tier). ----
        if !json {
            println!("bond:");
        }
        bond::execute(
            &bond_contract,
            &provider,
            &plan,
            operator,
            cb_addr,
            U256::from(args.mbps),
            &mut bond_outcome,
        )
        .await?;
        bond_done = true;
        if !json {
            let mut out = io::stdout().lock();
            bond::write_plan(&mut out, &plan, false, &bond_outcome, false)
                .context("failed to write bond result")?;
            bond_reported = true;
        }

        // ---- Register (skipped only when *this* key is already bound *and*
        //      active; `terms_hash` is `Some` iff a fresh registration is due —
        //      which includes a deregistered node whose binding still stands). ----
        if let Some(terms_hash) = terms_hash {
            if !json {
                println!("register:");
            }
            let multiaddrs = register::registration_multiaddrs(&args.multiaddrs, &file);
            let outcome = register::submit_registration(
                &provider,
                &signer,
                &resolved.data_dir,
                cb_addr,
                resolved.chain_id,
                &args.region,
                &multiaddrs,
                terms_hash,
                false,
                &mut register_tx,
            )
            .await?;
            if !json {
                let mut out = io::stdout().lock();
                register::write_outcome(&mut out, &outcome, false)
                    .context("failed to write register result")?;
            }
            register_outcome = Some(outcome);
        } else {
            hline(
                json,
                "register: skipped (this node key already registered on-chain)",
            );
        }

        // ---- Readiness summary (read back on-chain state). ----
        read_readiness(&bond_contract, operator).await
    }
    .await;

    // What registration actually submitted, defaulted multiaddrs included.
    let multiaddr_count = register_outcome
        .as_ref()
        .map_or(args.multiaddrs.len(), |o| o.multiaddr_count);
    let summary = |readiness: Option<&Readiness>| {
        build_summary(
            &pf,
            keys_generated,
            &plan,
            &bond_outcome,
            register_outcome.as_ref(),
            false,
            readiness,
            Some(terms_hash.is_some()),
            register_tx,
            &args.region,
            multiaddr_count,
        )
    };

    let readiness = match live {
        Ok(readiness) => readiness,
        Err(err) => {
            // The single partial-report site. In `--json` the summary carries
            // every landed hash (swap, bond, register) with `partial: true`; in
            // human mode each phase already printed its own result as it landed,
            // so the only gap is a bond that failed before reaching its writer.
            //
            // The write is NOT `?`-ed: a broken stdout must never displace the
            // chain error, which is the whole payload here (same discipline as
            // `bond::run`, and `println!` is avoided because it *panics* on
            // EPIPE — `decdn setup --json | head` would otherwise abort instead
            // of reporting).
            let write_result = if json {
                writeln!(io::stdout().lock(), "{}", summary(None))
            } else if bond_reported {
                Ok(())
            } else {
                bond::write_plan(&mut io::stdout().lock(), &plan, false, &bond_outcome, false)
            };
            if let Err(write_err) = write_result {
                eprintln!("warning: failed to write the partial setup report: {write_err}");
            }
            // Same guidance `bond::run` attaches, and only when the BOND phase
            // failed: `setup` reuses `bond::execute` so it carries the identical
            // re-run hazard, and it is the documented onboarding entry point
            // people blind-retry. A later phase failing after a confirmed bond
            // has no such hazard, and the hint would misdescribe it.
            if bond_done {
                return Err(err);
            }
            return Err(err).context(bond::resume_hint(&bond_outcome));
        }
    };

    emit_readiness(json, &readiness, &args.region, multiaddr_count);
    if json {
        println!("{}", summary(Some(&readiness)));
    }

    Ok(())
}

/// Decide what to do about the key material before any signer load. Pure
/// (no I/O beyond the `exists()` results the caller passes in) so the
/// branching — clean-slate generate, partial-material error, custom-keystore
/// error — is unit-testable.
fn precheck_keys(
    node_present: bool,
    keystore_present: bool,
    keystore: &Path,
    default_keystore: &Path,
) -> anyhow::Result<KeyAction> {
    if node_present && keystore_present {
        return Ok(KeyAction::Present);
    }
    anyhow::ensure!(
        node_present == keystore_present,
        "partial key material (node key {}, eth keystore {}); resolve manually before setup — \
         restore the missing file, or run `decdn key-gen --force` to regenerate both",
        present_label(node_present),
        present_label(keystore_present),
    );
    // Both absent. `key-gen` writes the keystore to `<data_dir>/keystore.json`;
    // a custom `--keystore` path can't be auto-provisioned here.
    anyhow::ensure!(
        keystore == default_keystore,
        "no keys yet and a custom keystore path is set ({}); run `decdn key-gen` to create keys \
         at that path first, then re-run setup",
        keystore.display(),
    );
    Ok(KeyAction::Generate)
}

/// Whether the operator's on-chain binding calls for a fresh registration,
/// derived from `CapacityBond.nodeIdOf` (the bound node id and its `active`
/// flag) against the local node key.
enum RegistrationStatus {
    /// The operator is bound to a *different*, non-zero node key than the local
    /// one. Setup refuses rather than silently overwrite that binding with a new
    /// key (which would strand the previously bound key).
    Divergent,
    /// This node key is already bound and active; registration is skipped.
    AlreadyRegistered,
    /// A (re-)registration is due: the operator is unbound, or bound to this key
    /// but currently inactive. `deregisterNode` clears `active` yet leaves the
    /// address↔nodeId binding in place, so a deregistered node keeps its binding
    /// and still needs to re-register to become active again.
    Due,
}

/// Classify the operator's binding for the register-divergence guard. Pure over
/// the on-chain read so the same-node inactive (deregistered) case is covered by
/// unit tests. A binding to a different, non-zero node id is `Divergent`
/// regardless of `active`; the same node id is `AlreadyRegistered` only while it
/// is `active`, and `Due` once it is not.
fn registration_status(
    bound_node_id: B256,
    bound_active: bool,
    local_node_id: B256,
) -> RegistrationStatus {
    if bound_node_id != B256::ZERO && bound_node_id != local_node_id {
        return RegistrationStatus::Divergent;
    }
    if bound_node_id == local_node_id && bound_active {
        RegistrationStatus::AlreadyRegistered
    } else {
        RegistrationStatus::Due
    }
}

/// ADR 019 § Terms Acceptance for `setup`: when a fresh registration is due,
/// read the network's current terms hash and require the operator to accept it
/// *before* any transaction (so a refusal / stale-client abort happens before
/// the bond is spent). Returns `Some(hash)` to feed registration, or `None` when
/// this key is already registered (acceptance was recorded at its own
/// registration).
async fn accept_terms_if_registering<P: Provider + Clone>(
    bond_contract: &CapacityBond::CapacityBondInstance<P>,
    cb_addr: Address,
    already_registered: bool,
    accept_flag: bool,
) -> anyhow::Result<Option<B256>> {
    if already_registered {
        return Ok(None);
    }
    let hash = bond_contract
        .currentTermsHash()
        .call()
        .await
        .with_context(|| {
            format!("failed to read currentTermsHash from CapacityBond at {cb_addr}")
        })?;
    terms::ensure_accepted_async(hash, accept_flag).await?;
    Ok(Some(hash))
}

/// On-chain registration state, read back for the readiness summary.
struct Readiness {
    active: bool,
    active_bond: U256,
    declared_mbps: U256,
    node_id: B256,
}

/// Read the operator's current `CapacityBond` registration state.
async fn read_readiness<P: Provider + Clone>(
    bond_contract: &CapacityBond::CapacityBondInstance<P>,
    operator: Address,
) -> anyhow::Result<Readiness> {
    let ctx = || "failed to read CapacityBond readiness state".to_string();
    let active = bond_contract
        .isActive(operator)
        .call()
        .await
        .with_context(ctx)?;
    let active_bond = bond_contract
        .activeBond(operator)
        .call()
        .await
        .with_context(ctx)?;
    let declared_mbps = bond_contract
        .declaredMbps(operator)
        .call()
        .await
        .with_context(ctx)?;
    let bound = bond_contract
        .nodeIdOf(operator)
        .call()
        .await
        .with_context(ctx)?;
    Ok(Readiness {
        active,
        active_bond,
        declared_mbps,
        node_id: bound.nodeId,
    })
}

/// A keys-absent dry run can't decrypt a keystore (none exists yet), so it
/// reads only the operator-independent curve through a read-only provider and
/// reports what *would* run. It still validates the tier band so an out-of-band
/// `--mbps` is flagged here exactly as `build_plan` would on the keyed path.
async fn dry_run_without_keys(
    resolved: &chain_ctx::Resolved,
    cb_addr: Address,
    mbps: u64,
    json: bool,
) -> anyhow::Result<()> {
    let provider =
        ProviderBuilder::new().connect_http(resolved.rpc_url.parse().with_context(|| {
            // An rpc_url secret commonly lives in the path/query (Infura/Alchemy
            // keys), which userinfo redaction wouldn't scrub — so hide the value
            // entirely, matching `config validate`'s `<redacted> (N chars)`.
            format!(
                "rpc_url is not a valid URL (<redacted>, {} chars)",
                resolved.rpc_url.len()
            )
        })?);
    let bond_contract = CapacityBond::new(cb_addr, &provider);
    let m = U256::from(mbps);
    let min_cap = bond_contract
        .minCapacityMbps()
        .call()
        .await
        .context("failed to read minCapacityMbps (RPC reachable?)")?;
    let max_cap = bond_contract
        .maxCapacityMbps()
        .call()
        .await
        .context("failed to read maxCapacityMbps")?;
    anyhow::ensure!(
        m >= min_cap && m <= max_cap,
        "declared capacity {mbps} Mbps is outside the on-chain band [{min_cap}, {max_cap}]; \
         declareMbps would revert",
    );
    let required = bond_contract
        .bondRequired(m)
        .call()
        .await
        .context("failed to read bondRequired")?;
    let min_bond = bond_contract
        .minBond()
        .call()
        .await
        .context("failed to read minBond")?;
    let target = min_bond.max(required);
    if json {
        let value = serde_json::json!({
            "dry_run": true,
            "keys_generated": false,
            "would_generate_keys": true,
            "mbps": mbps,
            "bond_required_base": required.to_string(),
            "target_bond_base": target.to_string(),
        });
        println!("{value}");
    } else {
        println!("bond (dry-run, no keys yet):");
        println!("  mbps={mbps}");
        println!("  bond_required_base={required}");
        println!("  target_bond_base={target}");
        println!("note: run setup without --dry-run to generate keys and submit");
    }
    Ok(())
}

/// Print one human-readable line, suppressed in `--json` mode.
fn hline(json: bool, line: &str) {
    if !json {
        println!("{line}");
    }
}

/// `Ok`/`Fail` from a boolean predicate (for the binary checks).
const fn status(ok: bool) -> CheckStatus {
    if ok {
        CheckStatus::Ok
    } else {
        CheckStatus::Fail
    }
}

/// Print an indented `  <label>: ok|WARN|FAIL (detail)` pre-flight line (human
/// mode). `WARN` renders distinctly from `ok` so a non-verified-but-tolerated
/// check (an undetermined clock) isn't mistaken for a pass.
fn check_line(json: bool, label: &str, status: CheckStatus, detail: &str) {
    if json {
        return;
    }
    let mark = status.mark();
    if detail.is_empty() {
        println!("  {label}: {mark}");
    } else {
        println!("  {label}: {mark} ({detail})");
    }
}

/// `present`/`absent` label for the partial-key-material error.
const fn present_label(present: bool) -> &'static str {
    if present { "present" } else { "absent" }
}

/// Human-readable clock-skew detail.
fn clock_detail(skew: Option<i64>) -> String {
    match skew {
        Some(s) => format!("local clock offset {s}s vs reference"),
        None => "reference time unavailable — could not verify, proceeding".to_string(),
    }
}

/// Measure local-vs-reference clock skew (local − reference, seconds) using the
/// RFC 7231 `Date` header of an HTTP response from the RPC host. Best-effort:
/// returns `None` if no trusted `Date` could be read (the check then renders
/// `WARN` rather than blocking onboarding). Avoids a new NTP dependency.
async fn measure_clock_skew(rpc_url: &str) -> Option<i64> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;
    // HEAD first; fall back to GET for servers that reject HEAD. Either way the
    // standard `Date` response header is what we read — non-2xx is fine.
    let date = match fetch_date_header(&client, rpc_url, true).await {
        Some(d) => d,
        None => fetch_date_header(&client, rpc_url, false).await?,
    };
    let reference = parse_http_date(&date)?;
    let local = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    let local = i64::try_from(local).unwrap_or(i64::MAX);
    Some(local - reference)
}

/// Issue a single HEAD/GET and return the `Date` header value, if present.
async fn fetch_date_header(client: &reqwest::Client, url: &str, head: bool) -> Option<String> {
    let req = if head {
        client.head(url)
    } else {
        client.get(url)
    };
    let resp = req.send().await.ok()?;
    resp.headers()
        .get(reqwest::header::DATE)?
        .to_str()
        .ok()
        .map(str::to_string)
}

/// Parse an RFC 7231 IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`) to a Unix
/// timestamp (seconds). Returns `None` if the day/month/year/time fields are
/// missing or out of range. Deliberately lenient rather than a strict format
/// check: the leading weekday token is ignored and the trailing zone token is
/// trusted as GMT (neither is validated). The obsolete RFC 850 / asctime forms
/// still fail because their field layout differs — good enough for a
/// best-effort skew probe against a server that should emit IMF-fixdate.
fn parse_http_date(s: &str) -> Option<i64> {
    let tokens: Vec<&str> = s.split_whitespace().collect();
    let day: i64 = tokens.get(1)?.parse().ok()?;
    let month = month_num(tokens.get(2)?)?;
    let year: i64 = tokens.get(3)?.parse().ok()?;
    let mut hms = tokens.get(4)?.split(':');
    let hour: i64 = hms.next()?.parse().ok()?;
    let min: i64 = hms.next()?.parse().ok()?;
    let sec: i64 = hms.next()?.parse().ok()?;
    if !(0..=23).contains(&hour)
        || !(0..=59).contains(&min)
        || !(0..=60).contains(&sec)
        || !(1..=31).contains(&day)
        // Bound the year so `days_from_civil(...) * 86_400` can't overflow i64
        // from a malformed/adversarial Date header. IMF-fixdate years are
        // 4-digit, so this rejects nothing valid.
        || !(1970..=9999).contains(&year)
    {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(days * 86_400 + hour * 3_600 + min * 60 + sec)
}

/// Three-letter English month → 1..=12.
fn month_num(m: &str) -> Option<i64> {
    Some(match m {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    })
}

/// Days from 1970-01-01 to the given proleptic-Gregorian date (Howard
/// Hinnant's `days_from_civil`, public domain). Pure integer math, no casts.
const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = if month > 2 { month - 3 } else { month + 9 }; // [0, 11]
    let doy = (153 * mp + 2) / 5 + day - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Prompt `[y/N]` for the bond submission. Returns `false` on EOF / a
/// non-affirmative answer, and reads stdin unconditionally so a piped answer
/// (`echo y | decdn setup …`) is honored regardless of whether stdout is a TTY;
/// `--yes` remains the documented fully non-interactive path.
fn confirm(shortfall: U256, target: U256, mbps: u64) -> io::Result<bool> {
    if shortfall == U256::ZERO {
        // Already bonded to target (e.g. `node bond` was run manually) — no
        // deposit, but `declareMbps` and/or `registerNode` may still submit.
        print!(
            "Bond already at target {target} (tier {mbps} Mbps); proceed with on-chain setup \
             (declare tier and/or register, as needed)? [y/N] "
        );
    } else {
        print!(
            "Submit bond of {shortfall} base units (target {target}, tier {mbps} Mbps) and complete \
             on-chain setup (declare + register, as needed)? [y/N] "
        );
    }
    io::stdout().flush()?;
    let mut line = String::new();
    if io::stdin().read_line(&mut line)? == 0 {
        return Ok(false);
    }
    Ok(matches!(line.trim(), "y" | "Y" | "yes" | "Yes" | "YES"))
}

/// Print the readiness summary (human mode). The on-chain subset
/// reachable without a running daemon — registry-active, bond, declared tier,
/// node id — plus the region/multiaddrs as submitted.
fn emit_readiness(json: bool, r: &Readiness, region: &str, multiaddrs: usize) {
    if json {
        return;
    }
    println!("readiness:");
    println!("  registry_active={}", r.active);
    println!("  active_bond_base={}", r.active_bond);
    println!("  declared_mbps={}", r.declared_mbps);
    println!("  node_id={:#x}", r.node_id);
    println!("  region={region}");
    println!("  multiaddrs={multiaddrs}");
}

/// Build the single aggregated `--json` summary object. Pure (returns the
/// `Value`) so the output contract — the `register` skipped/submitted
/// discriminant, the `clock_skew_secs: null` warn signal — is unit-testable.
#[allow(clippy::too_many_arguments)]
fn build_summary(
    pf: &Preflight,
    keys_generated: bool,
    plan: &bond::Plan,
    bond_outcome: &bond::Outcome,
    register_outcome: Option<&register::RegisterOutcome>,
    dry_run: bool,
    // `None` when a live phase failed before the readiness read-back; the
    // object is then flagged `partial` rather than being a different shape.
    readiness: Option<&Readiness>,
    // Whether a fresh registration was owed at all. Distinguishes "skipped
    // because this key is already registered" from "owed but never completed".
    // `None` on the dry-run path, which returns before due-ness is determined.
    register_due: Option<bool>,
    // A `registerNode` that was broadcast but whose outcome is unresolved. Only
    // ever `Some` alongside `register_outcome: None` — a resolved registration
    // reports its hash through the outcome instead.
    register_tx: Option<B256>,
    region: &str,
    multiaddrs: usize,
) -> serde_json::Value {
    let tx_hex = |h: Option<B256>| h.map(|v| format!("{v:#x}"));
    serde_json::json!({
        "dry_run": dry_run,
        "preflight": {
            "rpc_chain_id": pf.rpc_chain_id,
            "signing_chain_id": pf.signing_chain_id,
            "chain_id_ok": pf.chain_id_ok(),
            "clock_skew_secs": pf.clock_skew,
            "clock_ok": !pf.clock_status().blocks(),
            "token_balance_base": pf.token_balance.to_string(),
            "token_ok": pf.token_ok(),
            "native_balance_wei": pf.native_balance.to_string(),
            "native_gas_needed_wei": pf.gas_needed.to_string(),
            "native_ok": pf.native_ok(),
        },
        "keys_generated": keys_generated,
        "bond": {
            "mbps": plan.mbps,
            "target_bond_base": plan.target.to_string(),
            "bonded_base": plan.shortfall.to_string(),
            "approve_tx": tx_hex(bond_outcome.approve),
            "bond_tx": tx_hex(bond_outcome.bond),
            "declare_tx": tx_hex(bond_outcome.declare),
        },
        // `skipped` alone cannot distinguish three states: not due, not reached,
        // and attempted-and-failed. `due` disambiguates — `skipped: true, due: true` is a
        // registration that was owed and did not complete — and `tx` carries the
        // hash when one was broadcast without a readable receipt, which is the
        // case where the operator most needs it.
        "register": match register_outcome {
            Some(o) => serde_json::json!({
                "skipped": false,
                "due": true,
                "submitted": o.tx.is_some(),
                "tx": tx_hex(o.tx),
                "node_id": format!("{:#x}", o.node_id),
            }),
            None => serde_json::json!({
                "skipped": true,
                "due": register_due,
                "tx": tx_hex(register_tx),
            }),
        },
        // `null` on the partial-failure path, where the run never reached the
        // read-back. `partial` below is the flag a consumer should branch on.
        "readiness": match readiness {
            Some(r) => serde_json::json!({
                "registry_active": r.active,
                "active_bond_base": r.active_bond.to_string(),
                "declared_mbps": r.declared_mbps.to_string(),
                "node_id": format!("{:#x}", r.node_id),
                "region": region,
                "multiaddrs": multiaddrs,
            }),
            None => serde_json::Value::Null,
        },
        // True when a live phase failed part-way: the transaction fields above
        // record what landed, and the process exits non-zero. Emitting the SAME
        // object shape on both paths is deliberate — a `--json` consumer parses
        // one schema and branches on this flag, rather than having to recognise
        // a differently-shaped error object (#1355 review).
        "partial": readiness.is_none(),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests;
