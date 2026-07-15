//! `decdn channel` — client-side payment-channel lifecycle.
//!
//! `coop-close` cooperatively settles the channel tracked for a provider
//! (#971): it dials the provider over `cdn/client/v1`, requests a
//! `CooperativeClose` waiver over the channel's final watermark, and submits
//! `cooperativeClose` on-chain — one tx, no 48h dispute window (ADR 003
//! §Cooperative close). The reusable request→verify→submit core lives in
//! [`decdn_client_pull::cooperative_close`]; this command is the thin
//! flag/store/endpoint wiring around it. The node→node buyer reconcile (#972)
//! drives the same core on a timer.
//!
//! If the provider has no channel / no accepted voucher it declines and the
//! channel must be closed the ordinary way (`closeChannel` → dispute window →
//! `settleChannel`); auto-fallback is out of scope here.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, U256};
use decdn_client_pull::cooperative_close::{
    AuthorizedWatermark, CooperativeCloseOutcome, cooperative_close,
};
use decdn_common::cli::{self, common::expand_tilde};
use decdn_common::config::{DEFAULT_CHAIN_ID, FileConfig, load_file_config};
use decdn_incentive::buyer_channel::{BuyerChannelState, BuyerChannelStore};
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity::{self, PasswordSource, load_signer, read_password};
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::voucher_domain;
use iroh::{EndpointAddr, PublicKey};
use serde::Serialize;

use super::chain_ctx;
use decdn_client_pull::endpoint as client_endpoint;
use decdn_client_pull::provider;

/// Dispatch `decdn channel <subcommand>`.
pub async fn channel_dispatch(
    args: &cli::ChannelArgs,
    config_path: Option<&Path>,
) -> anyhow::Result<()> {
    match &args.command {
        cli::ChannelCommand::List(a) => list(a, config_path),
        cli::ChannelCommand::CoopClose(a) => coop_close(a, config_path).await,
    }
}

/// Chain coordinates resolved flag > `[blockchain]`/`[identity]` config >
/// default. Pure (parse-only) so the precedence is unit-testable.
#[derive(Debug)]
struct ResolvedChain {
    rpc_url: String,
    payment_channel: Address,
    chain_id: u64,
    data_dir: PathBuf,
    keystore: PathBuf,
}

/// Resolve the buyer-store data dir: flag > `[identity]` config > client-scoped
/// `~/.decdn/client` default. Shared by the read-only `list` path (which needs
/// nothing else) and [`resolve_chain`].
fn resolve_data_dir(data_dir: Option<PathBuf>, file: &FileConfig) -> anyhow::Result<PathBuf> {
    // Mirror the fetch client's path resolution: an explicit data dir wins,
    // otherwise the client-scoped `~/.decdn/client` home.
    data_dir
        .or_else(|| file.identity.as_ref().and_then(|i| i.data_dir.clone()))
        .map(|p| expand_tilde(&p))
        .or_else(cli::default_client_data_dir)
        .ok_or_else(|| {
            anyhow::anyhow!("data_dir not set and no default available (pass --data-dir)")
        })
}

fn resolve_chain(args: &cli::ChannelChainArgs, file: &FileConfig) -> anyhow::Result<ResolvedChain> {
    let bc = file.blockchain.as_ref();
    let rpc_url = args
        .rpc_url
        .clone()
        .or_else(|| bc.and_then(|b| b.rpc_url.clone()))
        .ok_or_else(|| anyhow::anyhow!("rpc_url not set (--rpc-url or blockchain.rpc_url)"))?;
    let payment_channel_raw = args
        .payment_channel_address
        .clone()
        .or_else(|| bc.and_then(|b| b.payment_channel_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "payment_channel_address not set (--payment-channel-address or \
                 blockchain.payment_channel_address)"
            )
        })?;
    let payment_channel =
        chain_ctx::parse_address(&payment_channel_raw, "payment_channel_address")?;
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
        payment_channel,
        chain_id,
        data_dir,
        keystore,
    })
}

async fn coop_close(args: &cli::CoopCloseArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let chain = resolve_chain(&args.chain, &file)?;

    let node_id = PublicKey::from_str(&args.node_id)
        .map_err(|e| anyhow::anyhow!("invalid --node-id {:?}: {e}", args.node_id))?;
    let provider = chain_ctx::parse_address(&args.provider_address, "--provider-address")?;

    // The channel to close is the one tracked for this provider; its watermark is
    // exactly what this client paid, and the cap on what we'll sign away.
    let store = RedbBuyerChannelStore::open(&chain.data_dir)?;
    let state = store.get_by_provider(provider)?.ok_or_else(|| {
        anyhow::anyhow!("no buyer channel tracked for provider {provider} — nothing to close")
    })?;
    let authorized = AuthorizedWatermark {
        amount: state.last_amount,
        nonce: state.last_nonce,
        bytes_delivered: state.last_bytes_delivered,
    };

    // Buyer signer (the client voucher + the cooperativeClose tx). Password from
    // env, else TTY.
    let password = read_password(
        &[
            PasswordSource::Env(eth_identity::KEYSTORE_PASSWORD_ENV),
            PasswordSource::Prompt { confirm: false },
        ],
        "eth keystore password",
    )?;
    let signer = Arc::new(load_signer(&chain.keystore, &password)?);
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentChannel::new(chain.payment_channel, rpc);
    let domain = voucher_domain(chain.chain_id, chain.payment_channel);

    let relays = client_endpoint::resolve_relays(args.relay_url.as_deref(), config_path)?;
    let disc = client_endpoint::client_discovery(config_path)?;
    let endpoint = client_endpoint::client_endpoint(&relays, &disc).await?;
    let mut target = EndpointAddr::new(node_id);
    if let Some(addr) = args.addr {
        target = target.with_ip_addr(addr);
    }
    if let Some(url) = relays.first() {
        target = target.with_relay_url(url.clone());
    }

    let outcome = cooperative_close(
        &endpoint,
        target,
        &contract,
        state.channel_id,
        provider,
        state.token,
        authorized,
        &signer,
        &domain,
        Duration::from_millis(args.timeout_ms),
    )
    .await?;

    match outcome {
        CooperativeCloseOutcome::Settled => {
            // Deposit refunded on-chain; drop the now-terminal channel so a later
            // fetch opens a fresh one. A failed forget only risks a stale reuse
            // attempt (rejected on-chain), so warn rather than fail the close.
            if let Err(e) = store.forget_if_channel(provider, state.channel_id) {
                eprintln!(
                    "warning: channel {} settled on-chain but clearing it from the buyer store \
                     failed: {e}",
                    state.channel_id
                );
            }
            println!(
                "cooperatively closed channel {} (provider {provider}); deposit refunded",
                state.channel_id
            );
            Ok(())
        }
        CooperativeCloseOutcome::Declined => {
            anyhow::bail!(
                "provider {provider} declined the cooperative close (no channel or no voucher \
                 yet); close it the ordinary way with closeChannel"
            )
        }
        CooperativeCloseOutcome::Reverted => {
            anyhow::bail!(
                "cooperativeClose reverted on-chain for channel {} (it may have raced a \
                 withdraw/close, or the watermark regressed); close it the ordinary way",
                state.channel_id
            )
        }
    }
}

/// `decdn channel list` / `status` (#1133): read-only dump of the tracked buyer
/// channels and their local voucher watermark. Reads only the buyer store — no
/// chain, keystore, or network access — so a client can see its own last nonce /
/// amount / bytes next to the deposit (e.g. to spot a voucher desync).
fn list(args: &cli::ChannelListArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    // Only the data dir is needed. An explicit `--data-dir` skips config loading
    // entirely so this read-only command can't fail on an unrelated `${VAR}` in
    // the config (`load_file_config` env-expands `blockchain.*` too); config is
    // read only for the `[identity].data_dir` fallback when the flag is absent.
    let data_dir = match args.data_dir.clone() {
        Some(dir) => expand_tilde(&dir),
        None => resolve_data_dir(None, &load_file_config(config_path)?)?,
    };

    let store = RedbBuyerChannelStore::open(&data_dir)?;
    let mut channels = store.load_all()?;
    // Stable output regardless of the store's internal key order.
    channels.sort_by_key(|c| c.provider);

    // Write to locked stdout so a `BrokenPipe` (e.g. piping to `head`) surfaces
    // as a propagated error rather than a `println!` panic, and the JSON isn't
    // buffered into one allocation.
    let mut out = std::io::stdout().lock();
    if args.json {
        let view: Vec<ChannelJson> = channels.iter().map(ChannelJson::from).collect();
        serde_json::to_writer_pretty(&mut out, &view)?;
        writeln!(out)?;
    } else {
        write_channels(&mut out, &channels)?;
    }
    Ok(())
}

/// Render the tracked buyer channels as an aligned table. Pure (writes to any
/// sink) so the layout is unit-testable without a store. Mirrors the operator
/// side's `decdn node channels` style: a `key=value` summary line, a `(no ...)`
/// sentinel when empty, then fixed-width columns.
fn write_channels(w: &mut impl Write, channels: &[BuyerChannelState]) -> std::io::Result<()> {
    writeln!(w, "channels={}", channels.len())?;
    if channels.is_empty() {
        return writeln!(w, "(no tracked channels)");
    }
    writeln!(
        w,
        "{:<14} {:<14} {:>8} {:>14} {:>14} {:>16} {:<14}",
        "PROVIDER", "CHANNEL", "NONCE", "LAST_AMOUNT", "DEPOSIT", "BYTES", "TOKEN"
    )?;
    for c in channels {
        writeln!(
            w,
            "{:<14} {:<14} {:>8} {:>14} {:>14} {:>16} {:<14}",
            short_hex(&format!("{:#x}", c.provider)),
            short_hex(&format!("{:#x}", c.channel_id)),
            c.last_nonce,
            format_usdc_u256(c.last_amount),
            format_usdc_u256(c.deposit),
            c.last_bytes_delivered,
            short_hex(&format!("{:#x}", c.token)),
        )?;
    }
    Ok(())
}

/// Serializable view for `--json`. String-encodes the 256-bit fields (hex for
/// addresses / channel id, decimal for the counters) so the output shape is
/// stable and lossless regardless of the reader's integer width.
#[derive(Serialize)]
struct ChannelJson {
    provider: String,
    channel_id: String,
    token: String,
    deposit_usdc: String,
    last_amount_usdc: String,
    last_nonce: String,
    last_bytes_delivered: String,
    expires_at: u64,
}

impl From<&BuyerChannelState> for ChannelJson {
    fn from(c: &BuyerChannelState) -> Self {
        Self {
            provider: format!("{:#x}", c.provider),
            channel_id: format!("{:#x}", c.channel_id),
            token: format!("{:#x}", c.token),
            deposit_usdc: format_usdc_u256(c.deposit),
            last_amount_usdc: format_usdc_u256(c.last_amount),
            last_nonce: c.last_nonce.to_string(),
            last_bytes_delivered: c.last_bytes_delivered.to_string(),
            expires_at: c.expires_at,
        }
    }
}

/// Format a `U256` micro-USDC amount as `"N.NNNNNN"`. USDC has 6 decimals
/// (ADR 003), so 1 USDC == `1_000_000` micro-USDC; the six fractional places are
/// kept fixed-width so columns align and a parser sees a stable shape. Mirrors
/// `node.rs::format_usdc`, but over the store's `U256` amounts.
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
    use alloy::primitives::B256;

    use super::*;

    fn args() -> cli::CoopCloseArgs {
        cli::CoopCloseArgs {
            node_id: "n".into(),
            addr: None,
            relay_url: None,
            provider_address: "0x0000000000000000000000000000000000000001".into(),
            chain: cli::ChannelChainArgs {
                rpc_url: None,
                payment_channel_address: None,
                chain_id: None,
                keystore: None,
                data_dir: Some(PathBuf::from("/tmp/d")),
            },
            timeout_ms: 30_000,
        }
    }

    fn config(body: &str) -> FileConfig {
        toml::from_str(body).unwrap()
    }

    #[test]
    fn flags_override_config() {
        let mut a = args();
        a.chain.rpc_url = Some("http://flag:8545".into());
        a.chain.chain_id = Some(99);
        let pc = "0x1111111111111111111111111111111111111111";
        a.chain.payment_channel_address = Some(pc.into());
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\nchain_id = 1\n\
             payment_channel_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        let r = resolve_chain(&a.chain, &file).unwrap();
        assert_eq!(r.rpc_url, "http://flag:8545");
        assert_eq!(r.chain_id, 99);
        assert_eq!(r.payment_channel, Address::from_str(pc).unwrap());
    }

    #[test]
    fn config_fills_unset_flags_and_chain_id_defaults() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\n\
             payment_channel_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        let r = resolve_chain(&args().chain, &file).unwrap();
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
             payment_channel_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        let r = resolve_chain(&args().chain, &file).unwrap();
        assert_eq!(r.data_dir, PathBuf::from("/tmp/d"));
        assert_eq!(
            r.keystore,
            eth_identity::keystore_path(&PathBuf::from("/tmp/d"))
        );
    }

    #[test]
    fn missing_payment_channel_errors() {
        let file = config("[blockchain]\nrpc_url = \"http://config:8545\"\n");
        let err = resolve_chain(&args().chain, &file).unwrap_err();
        assert!(
            err.to_string().contains("payment_channel_address not set"),
            "{err}"
        );
    }

    /// Build a `BuyerChannelState` for the formatter tests. Fields chosen so the
    /// USDC scaling and hex abbreviation are both exercised.
    fn mk_state(provider_byte: u8, nonce: u64, deposit_micro: u64) -> BuyerChannelState {
        BuyerChannelState {
            channel_id: B256::repeat_byte(0xab),
            provider: Address::repeat_byte(provider_byte),
            token: Address::repeat_byte(0xcd),
            deposit: U256::from(deposit_micro),
            last_amount: U256::from(1_500_000u64),
            last_nonce: U256::from(nonce),
            last_bytes_delivered: U256::from(4096u64),
            expires_at: 0,
        }
    }

    #[test]
    fn write_channels_empty_emits_sentinel() {
        let mut buf = Vec::new();
        write_channels(&mut buf, &[]).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("channels=0"), "{out}");
        assert!(out.contains("(no tracked channels)"), "{out}");
    }

    #[test]
    fn write_channels_renders_summary_header_and_rows() {
        let mut buf = Vec::new();
        write_channels(&mut buf, &[mk_state(0x11, 7, 2_000_000)]).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("channels=1"), "{out}");
        assert!(out.contains("PROVIDER"), "{out}");
        assert!(out.contains("DEPOSIT"), "{out}");
        // Deposit 2_000_000 micro-USDC renders as 2.000000, watermark as 1.500000.
        assert!(out.contains("2.000000"), "{out}");
        assert!(out.contains("1.500000"), "{out}");
        assert!(!out.contains("(no tracked channels)"), "{out}");
    }

    #[test]
    fn json_view_string_encodes_fields() {
        let view = ChannelJson::from(&mk_state(0x22, 3, 5_000_000));
        assert_eq!(view.deposit_usdc, "5.000000");
        assert_eq!(view.last_amount_usdc, "1.500000");
        assert_eq!(view.last_nonce, "3");
        assert_eq!(view.last_bytes_delivered, "4096");
        assert!(view.provider.starts_with("0x"), "{}", view.provider);
    }

    #[test]
    fn format_usdc_u256_pads_six_fractional_places() {
        assert_eq!(format_usdc_u256(U256::from(0u64)), "0.000000");
        assert_eq!(format_usdc_u256(U256::from(1u64)), "0.000001");
        assert_eq!(format_usdc_u256(U256::from(1_000_000u64)), "1.000000");
        assert_eq!(format_usdc_u256(U256::from(12_345_678u64)), "12.345678");
    }

    #[test]
    fn short_hex_abbreviates_only_long_input() {
        assert_eq!(short_hex("0x0102"), "0x0102");
        assert_eq!(
            short_hex("0x1111111111111111111111111111111111111111"),
            "0x1111111111…"
        );
    }
}
