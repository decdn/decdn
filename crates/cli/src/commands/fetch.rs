//! `decdn fetch` — standalone client-side paid pull of a single
//! content-addressed blob over `cdn/client/v1` (issue #391).
//!
//! The paid sibling of [`super::probe`]: dial a node by explicit
//! `--node-id`/`--addr`/`--relay-url`, then run one full delivery exchange via
//! the shared [`decdn_client_pull::stream_fetch`] requester — signing
//! cumulative vouchers against `--channel-id` as bytes arrive and verifying the
//! delivery `slash_sig` recovers to `--provider-address` (ADR 014 §1). The
//! requester also BLAKE3-checks the whole blob before we ever touch disk, so a
//! corrupt delivery fails the command rather than writing bad bytes.
//!
//! Scope (v1): `--channel-id` must be an already-open, **unused** channel — the
//! requester starts vouchers at nonce 1 and there is no client-side voucher
//! watermark store yet, so a partially-spent channel would re-sign a stale
//! nonce and be rejected. Opening/funding the channel and provider discovery
//! are separate concerns (their own issues), mirroring how `bundle create`
//! defers publishing.

use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, B256, U256};
use decdn_client_pull::{ChannelContext, stream_fetch};
use decdn_common::cli;
use decdn_common::identity::fresh_secret_key;
use decdn_incentive::eth_identity::{PasswordSource, load_signer, read_password};
use decdn_incentive::{slash_judge_domain, voucher_domain};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, PublicKey};

use super::client_endpoint;

/// Parse a user-supplied BLAKE3 hash (64 hex chars, optional `0x` prefix) into
/// raw bytes. Mirrors `probe::parse_hash`.
fn parse_hash(s: &str) -> anyhow::Result<[u8; 32]> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    let h = blake3::Hash::from_hex(hex).map_err(|e| {
        anyhow::anyhow!("invalid --hash {s:?}: expected 64 hex chars (BLAKE3 digest): {e}")
    })?;
    Ok(*h.as_bytes())
}

fn parse_address(label: &str, s: &str) -> anyhow::Result<Address> {
    Address::from_str(s).map_err(|e| {
        anyhow::anyhow!("invalid {label} {s:?}: expected 0x-prefixed 20-byte hex: {e}")
    })
}

/// Fetch a single blob over `cdn/client/v1` and write it atomically to
/// `--output`. `config_path` (the global `--config`) supplies the relay set
/// when `--relay-url` is not given (#935).
pub async fn fetch(args: &cli::FetchArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let hash = parse_hash(&args.hash)?;
    let provider = parse_address("--provider-address", &args.provider_address)?;
    let token = parse_address("--token", &args.token)?;
    let payment_channel =
        parse_address("--payment-channel-address", &args.payment_channel_address)?;
    let slash_judge = parse_address("--slash-judge-address", &args.slash_judge_address)?;
    let channel_id = B256::from_str(&args.channel_id).map_err(|e| {
        anyhow::anyhow!(
            "invalid --channel-id {:?}: expected 0x-prefixed 32-byte hex: {e}",
            args.channel_id
        )
    })?;

    let node_id = PublicKey::from_str(&args.node_id)
        .map_err(|e| anyhow::anyhow!("invalid --node-id {:?}: {e}", args.node_id))?;

    // Relays come from `network.relay_urls` in config; `--relay-url` overrides (#935).
    let relays = client_endpoint::resolve_relays(args.relay_url.as_deref(), config_path)?;
    if args.addr.is_none() && relays.is_empty() {
        anyhow::bail!(
            "no way to reach the node: pass --addr, or set network.relay_urls in config \
             (or --relay-url)"
        );
    }

    // Load the buyer's voucher-signing key. Password from env, else TTY prompt.
    let password = read_password(
        &[
            PasswordSource::Env("DECDN_KEYSTORE_PASSWORD"),
            PasswordSource::Prompt { confirm: false },
        ],
        "eth keystore",
    )?;
    let signer = load_signer(&args.keystore, &password)?;

    // EIP-712 domains: vouchers are signed against the PaymentChannel, the
    // delivery slash_sig is verified against the SlashJudge.
    let voucher_dom = voucher_domain(args.chain_id, payment_channel);
    let slash_dom = slash_judge_domain(args.chain_id, slash_judge);

    // Fresh, unused channel: the requester resumes from these `prior_*`, so a
    // never-used channel starts at zero (see module scope note).
    let ctx = ChannelContext {
        channel_id,
        token,
        deposit: U256::ZERO,
        client_signer: Arc::new(signer),
        voucher_domain: voucher_dom,
        prior_nonce: U256::ZERO,
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
    };

    // Client endpoint — same minimal one-shot setup as `probe`.
    let bind_addr = std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0);
    let relay_mode = client_endpoint::relay_mode(&relays);
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(fresh_secret_key())
        .relay_mode(relay_mode)
        .max_tls_tickets(decdn_protocol::SESSION_TICKET_CACHE_SIZE)
        .bind_addr(bind_addr)
        .map_err(|e| anyhow::anyhow!("invalid bind addr {bind_addr}: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("endpoint bind failed: {e}"))?;

    let mut target = EndpointAddr::new(node_id);
    if let Some(addr) = args.addr {
        target = target.with_ip_addr(addr);
    }
    // Attach a relay hint so a node dialed without --addr is reachable via relay.
    if let Some(url) = relays.first() {
        target = target.with_relay_url(url.clone());
    }

    let timestamp_us = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros()),
    )
    .unwrap_or(u64::MAX);

    let blob = stream_fetch(
        &endpoint,
        target,
        &ctx,
        &slash_dom,
        provider,
        hash,
        0,
        timestamp_us,
        Duration::from_millis(args.timeout_ms),
    )
    .await?;

    write_blob_atomic(&args.output, &blob)
        .map_err(|e| anyhow::anyhow!("write {}: {e}", args.output.display()))?;

    println!("fetched {} bytes -> {}", blob.len(), args.output.display());
    Ok(())
}

/// Write `bytes` to `target` atomically: a unique `O_CREAT|O_EXCL` temp in the
/// destination directory, then an atomic rename-replace. Same pattern (and the
/// same symlink-clobber rationale) as `bundle::write_bundle`.
fn write_blob_atomic(target: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let parent = target.parent().filter(|p| !p.as_os_str().is_empty());
    let mut tmp = match parent {
        Some(p) => tempfile::NamedTempFile::new_in(p)?,
        None => tempfile::NamedTempFile::new_in(".")?,
    };
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(target).map_err(|e| e.error)?;
    Ok(())
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

    #[test]
    fn write_blob_atomic_replaces_existing_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out.bin");
        std::fs::write(&path, b"old contents that are longer").expect("seed");

        write_blob_atomic(&path, b"new").expect("write");

        let got = std::fs::read(&path).expect("read back");
        assert_eq!(got, b"new", "atomic write must replace, not append");
    }

    #[test]
    fn parse_hash_round_trips_with_and_without_0x() {
        let digest = blake3::hash(b"payload");
        let hex = digest.to_hex();
        let with = parse_hash(&format!("0x{hex}")).expect("0x form");
        let without = parse_hash(&hex).expect("bare form");
        assert_eq!(with, *digest.as_bytes());
        assert_eq!(without, with);
    }

    #[test]
    fn parse_hash_rejects_short_input() {
        assert!(parse_hash("deadbeef").is_err());
    }
}
