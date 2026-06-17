//! `decdn node register` — on-chain node registration (ADR 019 § Step 2.3).
//!
//! Unlike the other `node` subcommands (which talk to a running node's
//! loopback admin RPC), this performs Phase 2 Step 2.3 of operator
//! onboarding: it loads the local iroh node key and Ethereum keystore,
//! reads the operator's on-chain nonces, builds the EIP-712
//! `bindingSignature` and the ed25519 ownership signature locally, and
//! submits `CapacityBond.registerNode`.
//!
//! Phase 2.1/2.2 (`approve` + `bond` + `declareMbps`) are a precondition:
//! the contract reverts `BondBelowMinimum` / `BondBelowCurve` if the bond
//! is not already posted (run `decdn node bond` first). `--dry-run` builds
//! and prints everything (it still reads the chain for the nonces the
//! signatures depend on) but does not submit.

use std::io;
use std::path::Path;

use alloy::primitives::{Address, B256, Bytes};
use alloy::signers::SignerSync;
use anyhow::Context;
use decdn_common::cli;
use decdn_common::identity;
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_incentive::{bind_sig, node_register};

use crate::commands::chain_ctx;

/// Entry point for `decdn node register`.
pub async fn run(args: &cli::RegisterArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let config_path = args.chain.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve(&args.chain, &file)?;
    let cb_addr =
        chain_ctx::parse_address(&resolved.capacity_bond_address, "capacity_bond_address")?;

    // Load the iroh node key. Require it to already exist — `load_or_generate`
    // would otherwise mint a *fresh* identity and register that, silently
    // diverging from the key the daemon serves under. Operators create it with
    // `decdn key-gen`.
    anyhow::ensure!(
        identity::key_path(&resolved.data_dir).exists(),
        "no node key at {}; run `decdn key-gen` first (register binds the existing iroh identity)",
        identity::key_path(&resolved.data_dir).display(),
    );
    let node_secret = identity::load_or_generate(&resolved.data_dir).with_context(|| {
        format!(
            "failed to load node key from {}",
            resolved.data_dir.display()
        )
    })?;
    let node_id = B256::from_slice(node_secret.public().as_bytes());

    let signer = chain_ctx::load_operator_signer(&args.chain, &resolved.keystore).await?;
    let operator = signer.address();
    let provider = chain_ctx::build_provider(&resolved.rpc_url, &signer)?;
    let bond = CapacityBond::new(cb_addr, &provider);

    // Nonces feed both signature digests, so they are read even on a dry run.
    let binding_nonce: u64 =
        bond.bindingNonce(operator).call().await.with_context(|| {
            format!("failed to read bindingNonce from CapacityBond at {cb_addr}")
        })?;
    let registration_nonce: u64 =
        bond.registrationNonce(node_id)
            .call()
            .await
            .with_context(|| {
                format!("failed to read registrationNonce from CapacityBond at {cb_addr}")
            })?;

    // EIP-712 BindNodeId signature (Ethereum key). `sign_hash_sync` yields a
    // low-s, 27/28-`v` 65-byte signature accepted by the on-chain OZ
    // `SignatureChecker` (same path as `decdn_incentive::bind_sig`).
    let domain = bind_sig::bind_node_id_domain(resolved.chain_id, cb_addr);
    let bind_hash = bind_sig::binding_signing_hash(node_id, binding_nonce, &domain);
    let binding_sig = signer.sign_hash_sync(&bind_hash)?.as_bytes().to_vec();

    // ed25519 ownership signature (iroh node key) over the contract's
    // ownership digest.
    let digest = node_register::ownership_message_digest(
        node_id,
        operator,
        resolved.chain_id,
        registration_nonce,
    );
    let ed25519_sig = node_secret.sign(digest.as_slice()).to_bytes().to_vec();

    let multiaddrs = node_register::pack_multiaddrs(&args.multiaddrs)?;

    let params = Params {
        node_id,
        operator,
        chain_id: resolved.chain_id,
        capacity_bond: cb_addr,
        region: &args.region,
        binding_nonce,
        registration_nonce,
        multiaddr_count: args.multiaddrs.len(),
        binding_sig: &binding_sig,
        ed25519_sig: &ed25519_sig,
    };

    if args.chain.dry_run {
        let mut out = io::stdout().lock();
        write_params(&mut out, &params, args.chain.json, None)
            .context("failed to write dry-run output")?;
        return Ok(());
    }

    let pending = bond
        .registerNode(
            node_id,
            Bytes::from(multiaddrs),
            args.region.clone(),
            Bytes::from(binding_sig.clone()),
            Bytes::from(ed25519_sig.clone()),
        )
        .send()
        .await
        .context("registerNode transaction failed to send (check RPC, gas, and that the bond covers minBond / the declared-capacity curve)")?;
    let receipt = pending
        .get_receipt()
        .await
        .context("registerNode sent but the receipt could not be fetched")?;

    let tx = receipt.transaction_hash;
    anyhow::ensure!(
        receipt.status(),
        "registerNode reverted (tx {tx}); most likely the bond does not cover minBond / the \
         declared-capacity curve, the nodeId/address is already bound, or a signature was rejected",
    );

    let mut out = io::stdout().lock();
    write_params(&mut out, &params, args.chain.json, Some(tx)).context("failed to write result")?;
    Ok(())
}

/// Registration parameters, for printing in both dry-run and post-submit paths.
struct Params<'a> {
    node_id: B256,
    operator: Address,
    chain_id: u64,
    capacity_bond: Address,
    region: &'a str,
    binding_nonce: u64,
    registration_nonce: u64,
    multiaddr_count: usize,
    binding_sig: &'a [u8],
    ed25519_sig: &'a [u8],
}

/// Write the parameters as JSON or grep-friendly `key=value` lines. `tx` is
/// `Some` after a successful submit, `None` for a dry run. Pure (`&mut impl
/// Write`) so the output shape is unit-testable without a chain.
fn write_params(
    w: &mut impl io::Write,
    p: &Params<'_>,
    json: bool,
    tx: Option<B256>,
) -> io::Result<()> {
    if json {
        let value = serde_json::json!({
            "submitted": tx.is_some(),
            "tx": tx.map(|h| format!("{h:#x}")),
            "node_id": format!("{:#x}", p.node_id),
            "operator": format!("{:#x}", p.operator),
            "chain_id": p.chain_id,
            "capacity_bond": format!("{:#x}", p.capacity_bond),
            "region": p.region,
            "binding_nonce": p.binding_nonce,
            "registration_nonce": p.registration_nonce,
            "multiaddrs": p.multiaddr_count,
            "binding_sig": format!("0x{}", chain_ctx::hex_encode(p.binding_sig)),
            "ed25519_sig": format!("0x{}", chain_ctx::hex_encode(p.ed25519_sig)),
        });
        return writeln!(w, "{value}");
    }
    writeln!(w, "node_id={:#x}", p.node_id)?;
    writeln!(w, "operator={:#x}", p.operator)?;
    writeln!(w, "chain_id={}", p.chain_id)?;
    writeln!(w, "capacity_bond={:#x}", p.capacity_bond)?;
    writeln!(w, "region={}", p.region)?;
    writeln!(w, "binding_nonce={}", p.binding_nonce)?;
    writeln!(w, "registration_nonce={}", p.registration_nonce)?;
    writeln!(w, "multiaddrs={}", p.multiaddr_count)?;
    writeln!(w, "binding_sig=0x{}", chain_ctx::hex_encode(p.binding_sig))?;
    writeln!(w, "ed25519_sig=0x{}", chain_ctx::hex_encode(p.ed25519_sig))?;
    match tx {
        Some(h) => writeln!(w, "submitted=true tx={h:#x}"),
        None => writeln!(w, "submitted=false dry_run=true"),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn dry_run_output_has_signatures() {
        let p = Params {
            node_id: B256::repeat_byte(0xAB),
            operator: Address::repeat_byte(0xCD),
            chain_id: 31337,
            capacity_bond: Address::repeat_byte(0x01),
            region: "DE",
            binding_nonce: 0,
            registration_nonce: 0,
            multiaddr_count: 1,
            binding_sig: &[0x11; 65],
            ed25519_sig: &[0x22; 64],
        };
        let mut buf = Vec::new();
        write_params(&mut buf, &p, false, None).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("submitted=false dry_run=true"), "{s}");
        assert!(s.contains("ed25519_sig=0x2222"), "{s}");
        assert!(s.contains("region=DE"), "{s}");
    }
}
