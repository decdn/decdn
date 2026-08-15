//! `decdn pool` — client-side payment-pool lifecycle.
//!
//! One `PaymentPool` deposit fans out to every provider its owner pays
//! (ADR 003) — there is no per-provider pool. `open` escrows a fresh deposit,
//! `top-up` adds funds to a pool the caller owns, `close` starts the
//! grace-window close, and `reclaim` refunds the residual once that window has
//! elapsed. `list`/`status` is a read-only dump of the tracked buyer pools and
//! their per-lane voucher watermark.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_client_pull::buyer_pool::{ensure_allowance, open_pool, top_up};
use decdn_common::cli::{self, common::expand_tilde};
use decdn_common::config::{DEFAULT_CHAIN_ID, FileConfig, load_file_config};
use decdn_incentive::buyer_pool::{BuyerLoad, BuyerPoolState, BuyerPoolStore, DepositOutcome};
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity::{self, PasswordSource, load_signer, read_password};
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{Capability, CapabilityGrant, PoolId, voucher_domain};
use serde::Serialize;

use decdn_client_pull::provider;

/// Dispatch `decdn pool <subcommand>`.
pub async fn pool_dispatch(args: &cli::PoolArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    match &args.command {
        cli::PoolCommand::List(a) => list(a, config_path),
        cli::PoolCommand::Open(a) => open(a, config_path).await,
        cli::PoolCommand::TopUp(a) => top_up_cmd(a, config_path).await,
        cli::PoolCommand::Close(a) => close(a, config_path).await,
        cli::PoolCommand::Reclaim(a) => reclaim(a, config_path).await,
        cli::PoolCommand::Assign(a) => assign(a, config_path).await,
    }
}

/// Chain coordinates resolved flag > `[blockchain]`/`[identity]` config >
/// default. Pure (parse-only) so the precedence is unit-testable.
#[derive(Debug)]
struct ResolvedChain {
    rpc_url: String,
    payment_pool: Address,
    chain_id: u64,
    data_dir: PathBuf,
    keystore: PathBuf,
}

/// Resolve the buyer-store data dir: flag > `[identity]` config > client-scoped
/// `~/.decdn/client` default. Shared by the read-only `list` path (which needs
/// nothing else) and [`resolve_chain`].
fn resolve_data_dir(data_dir: Option<PathBuf>, file: &FileConfig) -> anyhow::Result<PathBuf> {
    data_dir
        .or_else(|| file.identity.as_ref().and_then(|i| i.data_dir.clone()))
        .map(|p| expand_tilde(&p))
        .or_else(cli::default_client_data_dir)
        .ok_or_else(|| {
            anyhow::anyhow!("data_dir not set and no default available (pass --data-dir)")
        })
}

fn resolve_chain(args: &cli::PoolChainArgs, file: &FileConfig) -> anyhow::Result<ResolvedChain> {
    let bc = file.blockchain.as_ref();
    let rpc_url = args
        .rpc_url
        .clone()
        .or_else(|| bc.and_then(|b| b.rpc_url.clone()))
        .ok_or_else(|| anyhow::anyhow!("rpc_url not set (--rpc-url or blockchain.rpc_url)"))?;
    let payment_pool_raw = args
        .payment_pool_address
        .clone()
        .or_else(|| bc.and_then(|b| b.payment_pool_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "payment_pool_address not set (--payment-pool-address or \
                 blockchain.payment_pool_address)"
            )
        })?;
    let payment_pool =
        super::chain_ctx::parse_nonzero_address(&payment_pool_raw, "payment_pool_address")?;
    let chain_id = args
        .chain_id
        .or_else(|| bc.and_then(|b| b.chain_id))
        .unwrap_or(DEFAULT_CHAIN_ID);
    let data_dir = resolve_data_dir(args.data_dir.clone(), file)?;
    let keystore = args
        .keystore
        .clone()
        .or_else(|| bc.and_then(|b| b.eth_keystore.clone()))
        .map_or_else(
            || eth_identity::keystore_path(&data_dir),
            |p| expand_tilde(&p),
        );
    Ok(ResolvedChain {
        rpc_url,
        payment_pool,
        chain_id,
        data_dir,
        keystore,
    })
}

/// Buyer signer for the on-chain `pool` commands (vouchers +
/// open/top-up/close/reclaim txs). Password from `KEYSTORE_PASSWORD_ENV`, else
/// TTY.
fn load_buyer_signer(keystore: &Path) -> anyhow::Result<PrivateKeySigner> {
    let password = read_password(
        &[
            PasswordSource::Env(eth_identity::KEYSTORE_PASSWORD_ENV),
            PasswordSource::Prompt { confirm: false },
        ],
        "eth keystore password",
    )?;
    load_signer(keystore, &password)
}

/// Parse a user-supplied `--pool`: 64 hex chars, optionally `0x`-prefixed (the
/// on-chain `poolId` is `keccak256(owner, ownerPoolNonce)`, a `bytes32`).
fn parse_pool_id(s: &str) -> anyhow::Result<PoolId> {
    B256::from_str(s)
        .map_err(|e| anyhow::anyhow!("invalid --pool {s:?}: expected a 32-byte hex hash: {e}"))
}

/// Reject a `getPool` read whose on-chain `owner` isn't this keystore — a wrong
/// keystore/contract or a stale `--pool` id (a zero `owner` means the pool was
/// never opened on this contract).
fn ensure_owned(on_chain_owner: Address, ours: Address, pool_id: PoolId) -> anyhow::Result<()> {
    anyhow::ensure!(
        on_chain_owner == ours,
        "pool {pool_id} on-chain owner {on_chain_owner} is not this keystore's address {ours} \
         — wrong keystore/contract, or a mistyped --pool"
    );
    Ok(())
}

/// `decdn pool open`: escrow a fresh `PaymentPool` deposit (`openPool`). The
/// resulting pool fans out to every provider the owner later pays — there is no
/// per-provider open.
async fn open(args: &cli::PoolOpenArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let chain = resolve_chain(&args.chain, &file)?;

    let store = RedbBuyerPoolStore::open(&chain.data_dir)?;
    let signer = Arc::new(load_buyer_signer(&chain.keystore)?);
    let owner = signer.address();
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentPool::new(chain.payment_pool, rpc.clone());
    let domain = voucher_domain(chain.chain_id, chain.payment_pool);

    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentPool.usdc(): {e}"))?;
    let deposit = U256::from(args.deposit_micro_usdc);

    ensure_allowance(&rpc, token, owner, chain.payment_pool, Some(deposit)).await?;

    let opened = open_pool(
        &contract,
        Arc::clone(&signer),
        &domain,
        token,
        owner,
        deposit,
    )
    .await?;

    // The deposit is escrowed on-chain; a failed local record leaves it
    // untracked (reconcile against the tx).
    store.record(&opened.state).map_err(|e| {
        anyhow::anyhow!(
            "buyer pool opened on-chain (tx {}) but persisting it failed; the deposit is \
             escrowed but untracked — reconcile manually: {e}",
            opened.tx
        )
    })?;

    let out = PoolOpenJson {
        pool_id: format!("{:#x}", opened.state.pool_id),
    };
    serde_json::to_writer_pretty(std::io::stdout(), &out)?;
    println!();
    Ok(())
}

/// `decdn pool open` machine-readable output.
#[derive(Serialize)]
struct PoolOpenJson {
    #[serde(rename = "poolId")]
    pool_id: String,
}

/// `decdn pool top-up`: add funds to a pool the caller owns (`topUp`).
async fn top_up_cmd(args: &cli::PoolTopUpArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let chain = resolve_chain(&args.chain, &file)?;
    let pool_id = parse_pool_id(&args.pool)?;

    let store = RedbBuyerPoolStore::open(&chain.data_dir)?;
    let signer = Arc::new(load_buyer_signer(&chain.keystore)?);
    let owner = signer.address();
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentPool::new(chain.payment_pool, rpc.clone());

    let pool = contract
        .getPool(pool_id)
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("getPool failed: {e}"))?;
    ensure_owned(pool.owner, owner, pool_id)?;

    let additional = U256::from(args.amount_micro_usdc);
    // The pool no longer stores its token — it is the contract's immutable
    // `usdc()`, the same address the open path reads.
    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentPool.usdc(): {e}"))?;
    ensure_allowance(&rpc, token, owner, chain.payment_pool, Some(additional)).await?;

    let credited = top_up(&contract, pool_id, additional).await?;

    match store.add_deposit(owner, pool_id, credited) {
        Ok(DepositOutcome::Added(new_deposit)) => {
            println!(
                "topped up pool {pool_id} by {credited} µUSDC; deposit now {new_deposit} µUSDC"
            );
        }
        Ok(other) => eprintln!(
            "topped up pool {pool_id} on-chain (+{credited} µUSDC), but the local record was \
             not updated: {other:?}"
        ),
        Err(e) => eprintln!(
            "topped up pool {pool_id} on-chain (+{credited} µUSDC), but persisting it locally \
             failed: {e}"
        ),
    }
    Ok(())
}

/// Terminal outcome of a single close/reclaim tx.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxOutcome {
    /// Mined with a success receipt.
    Landed,
    /// Deterministically reverted (caught at estimation or a failing receipt).
    Reverted,
}

/// A mined-but-reverted tx returns `Ok(receipt)` with `status() == false`, so map
/// the receipt status to the outcome.
const fn receipt_outcome(landed: bool) -> TxOutcome {
    if landed {
        TxOutcome::Landed
    } else {
        TxOutcome::Reverted
    }
}

/// `decdn pool close`: start the grace-window close on a pool the caller owns
/// (`closePool`). Redemptions stay valid until the dispute deadline; `pool
/// reclaim` refunds the residual after it elapses.
async fn close(args: &cli::PoolCloseArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let chain = resolve_chain(&args.chain, &file)?;
    let pool_id = parse_pool_id(&args.pool)?;

    let store = RedbBuyerPoolStore::open(&chain.data_dir)?;
    let signer = Arc::new(load_buyer_signer(&chain.keystore)?);
    let owner = signer.address();
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentPool::new(chain.payment_pool, rpc);

    let pool = contract
        .getPool(pool_id)
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("getPool failed: {e}"))?;
    ensure_owned(pool.owner, owner, pool_id)?;
    anyhow::ensure!(
        matches!(pool.status, PaymentPool::Status::Open),
        "pool {pool_id} is not Open — nothing to close (already closing or closed)"
    );

    let pending = match contract.closePool(pool_id).send().await {
        Ok(pending) => pending,
        Err(e) if e.as_revert_data().is_some() => {
            anyhow::bail!("closePool reverted on-chain for pool {pool_id}")
        }
        Err(e) => anyhow::bail!("closePool send failed: {e}"),
    };
    let receipt = pending
        .get_receipt()
        .await
        .map_err(|e| anyhow::anyhow!("closePool receipt failed: {e}"))?;

    match receipt_outcome(receipt.status()) {
        TxOutcome::Landed => {
            // The dispute deadline is only known post-close; read it best-effort
            // and never print the pre-close `0` if that read fails. The local
            // record is dropped so a later `fetch`/`bundle pull` opens a fresh
            // pool rather than reusing one that is winding down — `--pool`
            // still names this one explicitly for `pool reclaim`.
            let deadline_note = match contract.getPool(pool_id).call().await {
                Ok(p) => format!("after Unix {}", p.disputeDeadline),
                Err(e) => {
                    format!("after the dispute window (couldn't read the exact deadline: {e})")
                }
            };
            if let Err(e) = store.forget_if_pool(owner, pool_id) {
                eprintln!(
                    "warning: pool {pool_id} closed on-chain but clearing it from the local \
                     store failed: {e}"
                );
            }
            println!(
                "closed pool {pool_id}; dispute window open — run `decdn pool reclaim --pool \
                 {pool_id}` {deadline_note}"
            );
            Ok(())
        }
        TxOutcome::Reverted => anyhow::bail!(
            "closePool reverted on-chain for pool {pool_id} (it may have raced a concurrent \
             close)"
        ),
    }
}

/// `decdn pool reclaim`: refund the residual deposit of a pool once its grace
/// window has elapsed (`reclaim`; permissionless — callable by anyone, but only
/// the owner receives funds).
async fn reclaim(args: &cli::PoolReclaimArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let chain = resolve_chain(&args.chain, &file)?;
    let pool_id = parse_pool_id(&args.pool)?;

    let store = RedbBuyerPoolStore::open(&chain.data_dir)?;
    let signer = Arc::new(load_buyer_signer(&chain.keystore)?);
    let owner = signer.address();
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentPool::new(chain.payment_pool, rpc);

    let pending = match contract.reclaim(pool_id).send().await {
        Ok(pending) => pending,
        Err(e) if e.as_revert_data().is_some() => {
            anyhow::bail!(
                "reclaim reverted on-chain for pool {pool_id} (not closed, or its dispute \
                 window has not elapsed yet)"
            )
        }
        Err(e) => anyhow::bail!("reclaim send failed: {e}"),
    };
    let receipt = pending
        .get_receipt()
        .await
        .map_err(|e| anyhow::anyhow!("reclaim receipt failed: {e}"))?;

    match receipt_outcome(receipt.status()) {
        TxOutcome::Landed => {
            // Best-effort — the row may already be gone (dropped by `pool close`
            // or a prior reclaim), so a failed clear here costs nothing.
            if let Err(e) = store.forget_if_pool(owner, pool_id) {
                eprintln!(
                    "warning: pool {pool_id} reclaimed on-chain but clearing it from the local \
                     store failed: {e}"
                );
            }
            println!("reclaimed pool {pool_id}; residual deposit refunded to its owner");
            Ok(())
        }
        TxOutcome::Reverted => anyhow::bail!(
            "reclaim reverted on-chain for pool {pool_id} (not closed, or its dispute window \
             has not elapsed yet)"
        ),
    }
}

/// Resolve the capability's absolute Unix-seconds expiry from the mutually
/// exclusive `--expiry-secs` (relative to now) / `--expiry-at` (absolute)
/// flags. Exactly one is required, and the result must lie in the future — a
/// capability that expires at or before now is dead on arrival (the node stops
/// accepting its vouchers immediately).
fn resolve_expiry(
    now: u64,
    expiry_secs: Option<u64>,
    expiry_at: Option<u64>,
) -> anyhow::Result<u64> {
    let expiry = match (expiry_secs, expiry_at) {
        (Some(secs), None) => now.checked_add(secs).ok_or_else(|| {
            anyhow::anyhow!("--expiry-secs {secs} overflows the Unix epoch from now ({now})")
        })?,
        (None, Some(at)) => at,
        // clap `conflicts_with` rules out `(Some, Some)`; this leaves `(None, None)`.
        _ => anyhow::bail!("exactly one of --expiry-secs or --expiry-at is required"),
    };
    anyhow::ensure!(
        expiry > now,
        "capability expiry {expiry} is not in the future (now is {now}); it would be dead on \
         arrival — pick a later --expiry-at or a positive --expiry-secs"
    );
    Ok(expiry)
}

/// Current Unix time in whole seconds.
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Render an absolute Unix-seconds expiry as `"<ts> (in Nd Nh Nm)"` relative to
/// `now`. The relative tail is what an operator actually reasons about; the raw
/// timestamp is kept for an exact, timezone-free reference.
fn format_expiry(expiry: u64, now: u64) -> String {
    let remaining = expiry.saturating_sub(now);
    let days = remaining / 86_400;
    let hours = (remaining % 86_400) / 3_600;
    let mins = (remaining % 3_600) / 60;
    format!("Unix {expiry} (in {days}d {hours}h {mins}m)")
}

/// `decdn pool assign`: issue an owner-signed spending capability delegating a
/// bounded spend on a pool the caller owns to a delegate `--signer` key, and
/// print the `dcap1:` token to hand to that delegated client (ADR 003
/// §Capability delegation).
///
/// The capability is signed offline with the owner keystore against the pool's
/// EIP-712 domain (`PaymentPool` address + chain id). The on-chain owner check
/// is best-effort: a mismatch (or an unreachable RPC) only warns, because
/// offline issuance is valid — the node is the one that enforces the owner
/// signature against the pool's on-chain owner at redemption time.
async fn assign(args: &cli::PoolAssignArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let chain = resolve_chain(&args.chain, &file)?;
    let pool_id = parse_pool_id(&args.pool)?;
    let signer_addr = super::chain_ctx::parse_nonzero_address(&args.signer, "--signer")?;

    let now = unix_now();
    let expiry = resolve_expiry(now, args.expiry_secs, args.expiry_at)?;

    let owner_signer = load_buyer_signer(&chain.keystore)?;
    let owner = owner_signer.address();
    let domain = voucher_domain(chain.chain_id, chain.payment_pool);
    let spending_cap = U256::from(args.cap_micro_usdc);

    let capability = Capability {
        signer: signer_addr,
        spending_cap,
        pool_id,
        expiry,
    };
    let signed = capability.sign(&owner_signer, &domain)?;
    let grant = CapabilityGrant::from_signed_capability(&signed);
    let token = grant.to_token();

    // The owner the node will recover from the token — by construction this is
    // the keystore we just signed with, so it doubles as a codec round-trip
    // check before the token ever leaves the machine.
    let recovered = grant
        .owner(&domain)
        .map_err(|e| anyhow::anyhow!("re-recovering the owner from the fresh token failed: {e}"))?;

    // Best-effort on-chain owner check. Offline issuance is valid, so an
    // unreachable RPC or a mismatch only warns — the serving node enforces the
    // owner signature against the pool's real owner at redemption.
    match provider::build_provider(&chain.rpc_url, &owner_signer) {
        Ok(rpc) => {
            let contract = PaymentPool::new(chain.payment_pool, rpc);
            warn_if_not_on_chain_owner(&contract, pool_id, owner).await;
        }
        Err(e) => eprintln!(
            "warning: could not build an RPC provider to check the on-chain owner of pool \
             {pool_id} ({e}); issuing anyway — the node verifies the owner signature at redemption"
        ),
    }

    println!("pool:         {pool_id}");
    println!("delegate:     {signer_addr} (the voucher-signing key this authorizes)");
    println!(
        "spending cap: {} USDC ({} µUSDC)",
        format_usdc_u256(spending_cap),
        args.cap_micro_usdc
    );
    println!("expiry:       {}", format_expiry(expiry, now));
    println!("owner:        {recovered} (recovered from the signature)");
    println!("Token (give this to the delegated client):");
    println!("{token}");
    Ok(())
}

/// Warn on stderr when the pool's on-chain `owner` is not `expected`, or when
/// the read cannot be performed. Never fails the command — offline/degraded
/// issuance stays valid.
async fn warn_if_not_on_chain_owner<P>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    pool_id: PoolId,
    expected: Address,
) where
    P: alloy::providers::Provider + Clone,
{
    match contract.getPool(pool_id).call().await {
        Ok(pool) if pool.owner == expected => {}
        Ok(pool) => eprintln!(
            "warning: pool {pool_id} on-chain owner {} is not this keystore's address {expected} \
             — a capability signed by a non-owner is rejected at redemption; issuing anyway",
            pool.owner
        ),
        Err(e) => eprintln!(
            "warning: could not read pool {pool_id} on-chain to confirm ownership ({e}); issuing \
             anyway — the node verifies the owner signature at redemption"
        ),
    }
}

/// `decdn pool list` / `status`: read-only dump of the tracked buyer pools and
/// their per-lane voucher watermark. Reads only the buyer store — no chain,
/// keystore, or network access.
fn list(args: &cli::PoolListArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let data_dir = match args.data_dir.clone() {
        Some(dir) => expand_tilde(&dir),
        None => resolve_data_dir(None, &load_file_config(config_path)?)?,
    };

    let store = RedbBuyerPoolStore::open(&data_dir)?;
    let BuyerLoad {
        mut pools,
        mut skipped,
    } = store.load_all()?;
    // Stable output regardless of the store's internal key order.
    pools.sort_by_key(|p| p.pool_id);
    skipped.sort_unstable();
    write_skipped_pools(&mut std::io::stderr().lock(), &skipped)?;

    let mut out = std::io::stdout().lock();
    if args.json {
        let view = PoolListJson {
            pools: pools.iter().map(PoolJson::from).collect(),
            skipped: skipped.iter().map(|p| format!("{p:#x}")).collect(),
        };
        serde_json::to_writer_pretty(&mut out, &view)?;
        writeln!(out)?;
    } else {
        write_pools(&mut out, &pools, &skipped)?;
    }
    Ok(())
}

/// Warn about every undecodable buyer row on stderr. The `pool_id` (the store's
/// primary key) is the only available repair handle.
fn write_skipped_pools(w: &mut impl Write, skipped: &[PoolId]) -> std::io::Result<()> {
    for pool_id in skipped {
        writeln!(
            w,
            "warning: buyer pool {pool_id:#x} could not be decoded; its deposit remains \
             escrowed but untracked until the record is repaired"
        )?;
    }
    Ok(())
}

/// Render the tracked buyer pools as an aligned table. Pure (writes to any
/// sink) so the layout is unit-testable without a store.
fn write_pools(
    w: &mut impl Write,
    pools: &[BuyerPoolState],
    skipped: &[PoolId],
) -> std::io::Result<()> {
    writeln!(w, "pools={}", pools.len())?;
    if pools.is_empty() {
        if skipped.is_empty() {
            writeln!(w, "(no tracked pools)")?;
        }
        return Ok(());
    }
    writeln!(
        w,
        "{:<14} {:<14} {:<14} {:>14} {:>8}",
        "POOL", "OWNER", "TOKEN", "DEPOSIT", "LANES"
    )?;
    for p in pools {
        writeln!(
            w,
            "{:<14} {:<14} {:<14} {:>14} {:>8}",
            short_hex(&format!("{:#x}", p.pool_id)),
            short_hex(&format!("{:#x}", p.owner)),
            short_hex(&format!("{:#x}", p.token)),
            format_usdc_u256(p.deposit),
            p.lane_count(),
        )?;
    }
    Ok(())
}

/// Top-level `--json` document.
#[derive(Serialize)]
struct PoolListJson {
    pools: Vec<PoolJson>,
    skipped: Vec<String>,
}

/// Serializable view for `--json`, including the per-lane watermark so a
/// programmatic consumer sees the same fan-out the table's `LANES` count only
/// summarizes.
#[derive(Serialize)]
struct PoolJson {
    pool_id: String,
    owner: String,
    token: String,
    deposit_usdc: String,
    lanes: Vec<LaneJson>,
}

#[derive(Serialize)]
struct LaneJson {
    signer: String,
    provider: String,
    last_amount_usdc: String,
    last_bytes_delivered: String,
}

impl From<&BuyerPoolState> for PoolJson {
    fn from(p: &BuyerPoolState) -> Self {
        Self {
            pool_id: format!("{:#x}", p.pool_id),
            owner: format!("{:#x}", p.owner),
            token: format!("{:#x}", p.token),
            deposit_usdc: format_usdc_u256(p.deposit),
            lanes: p
                .lanes()
                .map(|(lane, progress)| LaneJson {
                    signer: format!("{:#x}", lane.signer),
                    provider: format!("{:#x}", lane.provider),
                    last_amount_usdc: format_usdc_u256(progress.last_amount),
                    last_bytes_delivered: progress.last_bytes.to_string(),
                })
                .collect(),
        }
    }
}

/// Format a `U256` micro-USDC amount as `"N.NNNNNN"`. USDC has 6 decimals
/// (ADR 003), so 1 USDC == `1_000_000` micro-USDC.
fn format_usdc_u256(micro: U256) -> String {
    let scale = U256::from(1_000_000u64);
    let whole = micro / scale;
    // `micro % scale < 1_000_000`, so the `u64` narrowing is always exact.
    let frac = (micro % scale).to::<u64>();
    format!("{whole}.{frac:06}")
}

/// Abbreviate a `0x`-prefixed hex string to `0x` + the first 10 nibbles + `…`
/// for table display; short inputs pass through unchanged. Uses `get(..)` to
/// stay within the workspace `indexing_slicing` clippy denial.
fn short_hex(hex: &str) -> String {
    match hex.get(..12) {
        Some(prefix) if hex.len() > 12 => format!("{prefix}…"),
        _ => hex.to_string(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn resolve_expiry_absolute_and_relative_agree() {
        let now = 1_000_000u64;
        // Relative: now + secs.
        assert_eq!(resolve_expiry(now, Some(3_600), None).unwrap(), now + 3_600);
        // Absolute: used verbatim when in the future.
        assert_eq!(resolve_expiry(now, None, Some(now + 10)).unwrap(), now + 10);
    }

    #[test]
    fn resolve_expiry_rejects_missing_and_past() {
        let now = 1_000_000u64;
        // Neither flag: clap normally guards this, but the handler still refuses.
        let missing = resolve_expiry(now, None, None).unwrap_err();
        assert!(missing.to_string().contains("exactly one"), "{missing}");
        // An absolute expiry at/behind now is dead on arrival.
        let past = resolve_expiry(now, None, Some(now)).unwrap_err();
        assert!(past.to_string().contains("not in the future"), "{past}");
    }

    #[test]
    fn format_expiry_breaks_out_days_hours_minutes() {
        let now = 1_000_000u64;
        // 1 day + 2 hours + 3 minutes ahead.
        let expiry = now + 86_400 + 2 * 3_600 + 3 * 60;
        let rendered = format_expiry(expiry, now);
        assert!(rendered.contains(&format!("Unix {expiry}")), "{rendered}");
        assert!(rendered.contains("in 1d 2h 3m"), "{rendered}");
    }

    fn args() -> cli::PoolChainArgs {
        cli::PoolChainArgs {
            rpc_url: None,
            payment_pool_address: None,
            chain_id: None,
            keystore: None,
            data_dir: Some(PathBuf::from("/tmp/d")),
        }
    }

    fn config(body: &str) -> FileConfig {
        toml::from_str(body).unwrap()
    }

    #[test]
    fn flags_override_config() {
        let mut a = args();
        a.rpc_url = Some("http://flag:8545".into());
        a.chain_id = Some(99);
        let pp = "0x1111111111111111111111111111111111111111";
        a.payment_pool_address = Some(pp.into());
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\nchain_id = 1\n\
             payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        let r = resolve_chain(&a, &file).unwrap();
        assert_eq!(r.rpc_url, "http://flag:8545");
        assert_eq!(r.chain_id, 99);
        assert_eq!(r.payment_pool, Address::from_str(pp).unwrap());
    }

    #[test]
    fn config_fills_unset_flags_and_chain_id_defaults() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\n\
             payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        let r = resolve_chain(&args(), &file).unwrap();
        assert_eq!(r.rpc_url, "http://config:8545");
        assert_eq!(r.chain_id, DEFAULT_CHAIN_ID);
        assert_eq!(
            r.keystore,
            eth_identity::keystore_path(&PathBuf::from("/tmp/d"))
        );
    }

    #[test]
    fn explicit_data_dir_not_client_scoped() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\n\
             payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        let r = resolve_chain(&args(), &file).unwrap();
        assert_eq!(r.data_dir, PathBuf::from("/tmp/d"));
        assert_eq!(
            r.keystore,
            eth_identity::keystore_path(&PathBuf::from("/tmp/d"))
        );
    }

    #[test]
    fn missing_payment_pool_errors() {
        let file = config("[blockchain]\nrpc_url = \"http://config:8545\"\n");
        let err = resolve_chain(&args(), &file).unwrap_err();
        assert!(
            err.to_string().contains("payment_pool_address not set"),
            "{err}"
        );
    }

    /// A present-but-zero `payment_pool_address` fails fast via the shared
    /// `parse_nonzero_address` guard rather than as an opaque on-chain revert.
    #[test]
    fn rejects_zero_payment_pool() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\n\
             payment_pool_address = \"0x0000000000000000000000000000000000000000\"\n",
        );
        let err = resolve_chain(&args(), &file).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("payment_pool_address"), "{err}");
        assert!(msg.contains("must not be the zero address"), "{err}");
    }

    #[test]
    fn parse_pool_id_accepts_hex_and_rejects_garbage() {
        let id = "0x1111111111111111111111111111111111111111111111111111111111111111";
        assert_eq!(parse_pool_id(id).unwrap(), B256::from_str(id).unwrap());
        assert!(parse_pool_id("not-a-hash").is_err());
    }

    #[test]
    fn ensure_owned_accepts_matching_and_rejects_mismatch() {
        let ours = Address::repeat_byte(1);
        let pool_id = B256::repeat_byte(0xab);
        assert!(ensure_owned(ours, ours, pool_id).is_ok());
        let err = ensure_owned(Address::repeat_byte(2), ours, pool_id).unwrap_err();
        assert!(err.to_string().contains("not this keystore's address"));
    }

    fn mk_state(byte: u8, deposit_micro: u64) -> BuyerPoolState {
        BuyerPoolState::new(
            B256::repeat_byte(byte),
            Address::repeat_byte(byte),
            Address::repeat_byte(0xcd),
            U256::from(deposit_micro),
        )
    }

    #[test]
    fn write_pools_empty_emits_sentinel() {
        let mut buf = Vec::new();
        write_pools(&mut buf, &[], &[]).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("pools=0"), "{out}");
        assert!(out.contains("(no tracked pools)"), "{out}");
    }

    #[test]
    fn write_pools_renders_deposit_and_lane_count() {
        let state = mk_state(1, 1_500_000);
        let mut buf = Vec::new();
        write_pools(&mut buf, std::slice::from_ref(&state), &[]).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("1.500000"), "{out}");
        assert!(
            out.contains(" 0\n") || out.trim_end().ends_with(" 0"),
            "{out}"
        );
    }

    #[test]
    fn format_usdc_u256_pads_fractional_digits() {
        assert_eq!(format_usdc_u256(U256::from(1_000_000u64)), "1.000000");
        assert_eq!(format_usdc_u256(U256::from(500_000u64)), "0.500000");
    }

    #[test]
    fn short_hex_truncates_long_hashes() {
        let long = "0x1111111111111111111111111111111111111111";
        assert!(short_hex(long).ends_with('…'));
        assert_eq!(short_hex("0x01"), "0x01");
    }
}
