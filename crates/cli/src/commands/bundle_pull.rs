//! `decdn bundle pull` — fetch every blob a bundle manifest references into an
//! output directory over the paid `cdn/client/v1` path (issue #391).
//!
//! The manifest comes from a local file (`-i`) or is fetched first by its own
//! BLAKE3 hash (`--hash`); either way each entry is then fetched independently.
//! Node selection is per-entry (#936): with an explicit `--node-id` every entry
//! is pulled from that one node, otherwise each entry discovers its own holder
//! among the region-nearest active nodes. Entries are fetched with `--jobs`
//! concurrency.
//!
//! **Voucher-nonce safety.** A payment channel's vouchers use a strictly
//! increasing nonce, so two in-flight fetches sharing one channel would race it.
//! Concurrency is therefore bounded two ways: `--jobs` caps total in-flight
//! entries, and a per-provider async mutex serializes fetches that land on the
//! same provider's channel (which also makes the lazy open-or-reuse first-touch
//! race-free). Distinct providers proceed in parallel.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context as _, anyhow, bail};
use decdn_common::cli::{BundlePullArgs, ClientFetchArgs};
use decdn_common::config::load_file_config;
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity::{self, PasswordSource, load_signer, read_password};
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{slash_judge_domain, voucher_domain};
use futures_util::StreamExt as _;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};
use serde::{Deserialize, Serialize};

use super::discovery::{self, NodeCandidate};
use super::fetch;
use super::{chain_ctx, client_endpoint};

/// Read-side bundle manifest (the write-side lives in [`super::bundle`]). `size`
/// is optional on the wire (per `appendix-bundles.md`); it is informational for
/// the dry-run plan and not required to fetch.
#[derive(Debug, Deserialize)]
struct Manifest {
    version: u32,
    entries: Vec<ManifestEntry>,
}

#[derive(Debug, Deserialize)]
struct ManifestEntry {
    path: String,
    hash: String,
    #[serde(default)]
    size: Option<u64>,
}

/// Per-entry result, kept (never short-circuited) so one failure doesn't abort
/// the others — re-running resumes via skip-existing.
enum EntryOutcome {
    Fetched(u64),
    Skipped,
    Failed { path: String, err: String },
}

/// One-line `--json` summary.
#[derive(Serialize)]
struct PullReport {
    output: String,
    fetched: u64,
    skipped: u64,
    failed: u64,
    total_bytes: u64,
}

/// Entry point for `decdn bundle pull`.
pub async fn bundle_pull(args: &BundlePullArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    // Dry-run short-circuits before any network/chain/keystore activity.
    if args.dry_run {
        return dry_run(args);
    }

    // A local manifest is read up front (no network): an empty bundle then needs
    // no endpoint or keystore password at all.
    let local_manifest = match &args.input {
        Some(path) => Some(read_local_manifest(path)?),
        None => None,
    };
    if let Some(m) = &local_manifest
        && m.entries.is_empty()
    {
        println!("bundle has no entries; nothing to fetch");
        return Ok(());
    }

    let common = &args.common;
    let relays = client_endpoint::resolve_relays(common.relay_url.as_deref(), config_path)?;
    let disc = client_endpoint::client_discovery(config_path)?;
    let file = load_file_config(config_path)?;
    let chain = fetch::resolve_chain(common, &file)?;
    let store = RedbBuyerChannelStore::open(&chain.data_dir)?;
    let endpoint = client_endpoint::client_endpoint(&relays, &disc).await?;

    // Selection: explicit single node for every entry, or a per-entry discovery
    // candidate list read from `CapacityBond` once.
    let explicit = explicit_target(common)?;
    let candidates = if explicit.is_some() {
        None
    } else {
        let capacity_bond = chain.capacity_bond.ok_or_else(|| {
            anyhow!(
                "auto-discovery needs capacity_bond_address (--capacity-bond-address or \
                 blockchain.capacity_bond_address), or pass --node-id to pull from one node"
            )
        })?;
        let all = discovery::active_nodes(&chain.rpc_url, capacity_bond).await?;
        if all.is_empty() {
            bail!("no active nodes in the CapacityBond registry at {capacity_bond}");
        }
        Some(discovery::select_candidates(
            all,
            chain.region.as_deref(),
            discovery::SELECT_K,
        ))
    };

    // Buyer signer (vouchers + any openChannel tx). Prompted only once, after we
    // know there is work to do. Password from env, else TTY.
    let password = read_password(
        &[
            PasswordSource::Env(eth_identity::KEYSTORE_PASSWORD_ENV),
            PasswordSource::Prompt { confirm: false },
        ],
        "eth keystore password",
    )?;
    let signer = Arc::new(load_signer(&chain.keystore, &password)?);
    let self_address = signer.address();
    let rpc = chain_ctx::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentChannel::new(chain.payment_channel, rpc.clone());
    let voucher_dom = voucher_domain(chain.chain_id, chain.payment_channel);
    let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);

    let ctx = PullCtx {
        endpoint: &endpoint,
        store: &store,
        contract: &contract,
        rpc: &rpc,
        signer: &signer,
        self_address,
        voucher_dom: &voucher_dom,
        slash_dom: &slash_dom,
        chain: &chain,
        relays: &relays,
        explicit,
        candidates,
        common,
        locks: RefCell::new(HashMap::new()),
    };

    // Obtain the manifest: the pre-read local one, or fetch the bundle blob.
    let manifest = if let Some(m) = local_manifest {
        m
    } else {
        let raw = args
            .hash
            .as_deref()
            .ok_or_else(|| anyhow!("no bundle source (expected -i or --hash)"))?;
        let bytes = ctx
            .fetch(fetch::parse_hash(raw)?)
            .await
            .context("fetch bundle manifest blob")?;
        parse_manifest(&bytes)?
    };
    if manifest.entries.is_empty() {
        println!("bundle has no entries; nothing to fetch");
        return Ok(());
    }

    // Fetch entries concurrently. `buffer_unordered` polls up to `--jobs`
    // futures in this one task (no `tokio::spawn`: `probe_once` is not `Send`);
    // real parallelism comes from concurrent in-flight network I/O, while the
    // per-provider locks inside `ctx.fetch` serialize same-channel access.
    let jobs = args.jobs.max(1);
    let outcomes: Vec<EntryOutcome> = futures_util::stream::iter(manifest.entries.iter())
        .map(|entry| ctx.fetch_entry(entry, &args.output, args.overwrite))
        .buffer_unordered(jobs)
        .collect()
        .await;

    report(&outcomes, &args.output, args.json)
}

/// Shared, by-reference state for the entry fetch loop. Borrowed by every
/// in-flight entry future; single-task `buffer_unordered` means interior
/// mutability (`RefCell`/`Rc`) is sufficient — no `Send`/`Sync` needed.
struct PullCtx<'a, P: Provider + Clone> {
    endpoint: &'a Endpoint,
    store: &'a RedbBuyerChannelStore,
    contract: &'a PaymentChannel::PaymentChannelInstance<P>,
    rpc: &'a P,
    signer: &'a Arc<PrivateKeySigner>,
    self_address: Address,
    voucher_dom: &'a Eip712Domain,
    slash_dom: &'a Eip712Domain,
    chain: &'a fetch::ResolvedChain,
    relays: &'a [RelayUrl],
    /// `Some((node_id, provider))` pins every entry to one node (`--node-id`);
    /// `None` discovers per entry against `candidates`.
    explicit: Option<(PublicKey, Address)>,
    candidates: Option<Vec<NodeCandidate>>,
    common: &'a ClientFetchArgs,
    /// Per-provider locks: serialize fetches sharing one channel's voucher
    /// nonce. Lazily created; held only across one entry's fetch.
    locks: RefCell<HashMap<Address, Rc<tokio::sync::Mutex<()>>>>,
}

impl<P: Provider + Clone> PullCtx<'_, P> {
    /// The per-provider lock, created on first use.
    fn provider_lock(&self, provider: Address) -> Rc<tokio::sync::Mutex<()>> {
        let mut map = self.locks.borrow_mut();
        Rc::clone(
            map.entry(provider)
                .or_insert_with(|| Rc::new(tokio::sync::Mutex::new(()))),
        )
    }

    /// Select the node to fetch `hash` from: the pinned explicit node, or
    /// per-entry discovery over the shared candidate list.
    async fn pick(&self, hash: [u8; 32]) -> anyhow::Result<(PublicKey, Address)> {
        if let Some(pinned) = self.explicit {
            return Ok(pinned);
        }
        let candidates = self
            .candidates
            .as_deref()
            .ok_or_else(|| anyhow!("no discovery candidates available"))?;
        let picked = fetch::probe_and_rank(
            self.endpoint,
            self.store,
            candidates,
            self.relays.first(),
            hash,
        )
        .await?;
        Ok((picked.node_id, picked.eth_address))
    }

    /// Fetch one blob: pick a provider, then under that provider's lock
    /// open-or-reuse its channel and stream the blob (persisting the watermark).
    async fn fetch(&self, hash: [u8; 32]) -> anyhow::Result<Vec<u8>> {
        let (node_id, provider) = self.pick(hash).await?;

        // Serialize all access to this provider's channel: the open-or-reuse +
        // voucher-signing critical section must be atomic per channel.
        let lock = self.provider_lock(provider);
        let _guard = lock.lock().await;

        let ctx = fetch::open_or_reuse(
            self.store,
            self.contract,
            self.rpc,
            self.signer,
            self.voucher_dom,
            provider,
            self.self_address,
            self.chain.payment_channel,
            self.chain.deposit,
            self.chain.max_approve,
        )
        .await?;

        let mut target = EndpointAddr::new(node_id);
        // `--addr` only applies to the explicit-node path (clap requires
        // `--node-id`); a discovered node is reached via its resolved address.
        if self.explicit.is_some()
            && let Some(addr) = self.common.addr
        {
            target = target.with_ip_addr(addr);
        }
        if let Some(url) = self.relays.first() {
            target = target.with_relay_url(url.clone());
        }

        let max_blob_bytes = self.common.max_blob_mb.saturating_mul(1024 * 1024);
        fetch::fetch_blob(
            self.endpoint,
            target,
            &ctx,
            self.slash_dom,
            provider,
            self.store,
            hash,
            Duration::from_millis(self.common.timeout_ms),
            max_blob_bytes,
        )
        .await
    }

    /// Fetch one manifest entry and write it under `out_root`. Never panics or
    /// short-circuits — every failure becomes an [`EntryOutcome::Failed`].
    async fn fetch_entry(
        &self,
        entry: &ManifestEntry,
        out_root: &Path,
        overwrite: bool,
    ) -> EntryOutcome {
        let dest = match safe_join(out_root, &entry.path) {
            Ok(d) => d,
            Err(e) => return EntryOutcome::failed(&entry.path, &e),
        };
        // Skip-existing (default): a present final file is verified-good (it was
        // only renamed into place after a BLAKE3 check), so re-runs resume.
        if !overwrite && dest.try_exists().unwrap_or(false) {
            return EntryOutcome::Skipped;
        }
        let hash = match fetch::parse_hash(&entry.hash) {
            Ok(h) => h,
            Err(e) => return EntryOutcome::failed(&entry.path, &e),
        };
        let bytes = match self.fetch(hash).await {
            Ok(b) => b,
            Err(e) => return EntryOutcome::failed(&entry.path, &e),
        };
        if let Some(parent) = dest.parent()
            && let Err(e) = std::fs::create_dir_all(parent)
        {
            return EntryOutcome::failed(&entry.path, &anyhow!("create {}: {e}", parent.display()));
        }
        if let Err(e) = fetch::write_blob_atomic(&dest, &bytes) {
            return EntryOutcome::failed(&entry.path, &anyhow!("write {}: {e}", dest.display()));
        }
        EntryOutcome::Fetched(bytes.len() as u64)
    }
}

impl EntryOutcome {
    fn failed(path: &str, err: &anyhow::Error) -> Self {
        Self::Failed {
            path: path.to_string(),
            err: format!("{err:#}"),
        }
    }
}

/// `--node-id` (with its clap-required `--provider-address`) → a pinned target
/// for every entry; otherwise `None` (discover per entry).
fn explicit_target(common: &ClientFetchArgs) -> anyhow::Result<Option<(PublicKey, Address)>> {
    let Some(raw) = &common.node_id else {
        return Ok(None);
    };
    let node_id =
        PublicKey::from_str(raw).map_err(|e| anyhow!("invalid --node-id {raw:?}: {e}"))?;
    let provider_raw = common
        .provider_address
        .as_deref()
        .ok_or_else(|| anyhow!("--provider-address is required with --node-id"))?;
    let provider = chain_ctx::parse_address(provider_raw, "--provider-address")?;
    Ok(Some((node_id, provider)))
}

/// Read + parse a local bundle manifest file.
fn read_local_manifest(path: &Path) -> anyhow::Result<Manifest> {
    let bytes =
        std::fs::read(path).with_context(|| format!("read bundle file {}", path.display()))?;
    parse_manifest(&bytes)
}

/// Parse + version-gate bundle manifest bytes.
fn parse_manifest(bytes: &[u8]) -> anyhow::Result<Manifest> {
    let manifest: Manifest =
        serde_json::from_slice(bytes).context("parse bundle JSON (expected v1 manifest)")?;
    if manifest.version != 1 {
        bail!(
            "unsupported bundle version {} (this build supports v1)",
            manifest.version
        );
    }
    Ok(manifest)
}

/// Resolve a bundle entry's POSIX relative `path` to a filesystem path under
/// `root`, rejecting anything that isn't a sequence of plain filename
/// components — absolute paths, `.`/`..`, drive prefixes, empty components
/// (leading/trailing/double slash). With `..` rejected, the result cannot
/// escape `root`. (`appendix-bundles.md` § Path-safety rules.)
fn safe_join(root: &Path, rel: &str) -> anyhow::Result<PathBuf> {
    if rel.is_empty() {
        bail!("empty entry path");
    }
    let mut out = root.to_path_buf();
    for seg in rel.split('/') {
        // Each `/`-split segment must be exactly one `Normal` path component.
        // This rejects "", ".", "..", "/", and a Windows drive prefix uniformly
        // and cross-platform.
        let mut comps = Path::new(seg).components();
        match (comps.next(), comps.next()) {
            (Some(Component::Normal(s)), None) => out.push(s),
            _ => bail!("rejected unsafe component {seg:?} in entry path {rel:?}"),
        }
    }
    Ok(out)
}

/// Print the would-fetch plan and exit (no network/chain/keystore activity).
/// With `--hash` the entries can't be enumerated offline, so only the intent is
/// reported.
fn dry_run(args: &BundlePullArgs) -> anyhow::Result<()> {
    let out = args.output.display();
    match (&args.input, &args.hash) {
        (Some(path), _) => {
            let manifest = read_local_manifest(path)?;
            if args.json {
                let plan = serde_json::json!({
                    "output": args.output.display().to_string(),
                    "count": manifest.entries.len(),
                    "entries": manifest.entries.iter().map(|e| serde_json::json!({
                        "path": e.path, "hash": e.hash, "size": e.size,
                    })).collect::<Vec<_>>(),
                });
                println!("{plan}");
            } else {
                println!(
                    "would fetch {} entr(ies) into {out}:",
                    manifest.entries.len()
                );
                for e in &manifest.entries {
                    match e.size {
                        Some(n) => println!("  {} ({n} bytes)", e.path),
                        None => println!("  {}", e.path),
                    }
                }
            }
        }
        (None, Some(h)) => {
            println!(
                "--dry-run with --hash: would fetch bundle {h} then its entries into {out} \
                 (entries are not enumerable without fetching the manifest)"
            );
        }
        (None, None) => bail!("no bundle source (expected -i or --hash)"),
    }
    Ok(())
}

/// Summarize outcomes; return an error if any entry failed (after reporting all).
fn report(outcomes: &[EntryOutcome], output: &Path, json: bool) -> anyhow::Result<()> {
    let mut fetched = 0u64;
    let mut skipped = 0u64;
    let mut failed = 0u64;
    let mut total_bytes = 0u64;
    for o in outcomes {
        match o {
            EntryOutcome::Fetched(n) => {
                fetched += 1;
                total_bytes = total_bytes.saturating_add(*n);
            }
            EntryOutcome::Skipped => skipped += 1,
            EntryOutcome::Failed { path, err } => {
                failed += 1;
                eprintln!("failed: {path}: {err}");
            }
        }
    }

    let rep = PullReport {
        output: output.display().to_string(),
        fetched,
        skipped,
        failed,
        total_bytes,
    };
    if json {
        let line = serde_json::to_string(&rep).map_err(|e| anyhow!("serialize report: {e}"))?;
        println!("{line}");
    } else {
        println!(
            "pulled {} -> {} ({fetched} fetched, {skipped} skipped, {failed} failed, {total_bytes} bytes)",
            rep.output,
            output.display()
        );
    }

    if failed > 0 {
        bail!("{failed} entr(ies) failed to fetch");
    }
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
    fn safe_join_builds_nested_path_under_root() {
        let root = Path::new("/out");
        let p = safe_join(root, "assets/app.js").unwrap();
        assert_eq!(p, Path::new("/out/assets/app.js"));
    }

    #[test]
    fn safe_join_rejects_parent_dir() {
        let err = safe_join(Path::new("/out"), "a/../../etc/passwd").unwrap_err();
        assert!(format!("{err:#}").contains(".."), "{err:#}");
    }

    #[test]
    fn safe_join_rejects_absolute_and_empty_components() {
        // Leading slash → empty first component.
        assert!(safe_join(Path::new("/out"), "/etc/passwd").is_err());
        // Double slash → empty middle component.
        assert!(safe_join(Path::new("/out"), "a//b").is_err());
        // Trailing slash → empty last component.
        assert!(safe_join(Path::new("/out"), "a/").is_err());
        // Bare current-dir component.
        assert!(safe_join(Path::new("/out"), "./a").is_err());
        // Empty.
        assert!(safe_join(Path::new("/out"), "").is_err());
    }

    #[test]
    fn parse_manifest_accepts_v1_with_optional_size() {
        let json = br#"{"version":1,"entries":[{"path":"a.txt","hash":"b3:ab","size":4},{"path":"b","hash":"b3:cd"}]}"#;
        let m = parse_manifest(json).unwrap();
        assert_eq!(m.entries.len(), 2);
        assert_eq!(m.entries[0].size, Some(4));
        assert_eq!(m.entries[1].size, None);
    }

    #[test]
    fn parse_manifest_rejects_unsupported_version() {
        let json = br#"{"version":2,"entries":[]}"#;
        let err = parse_manifest(json).unwrap_err();
        assert!(
            format!("{err:#}").contains("unsupported bundle version 2"),
            "{err:#}"
        );
    }

    #[test]
    fn report_errors_when_any_failed() {
        let outcomes = vec![
            EntryOutcome::Fetched(10),
            EntryOutcome::Skipped,
            EntryOutcome::Failed {
                path: "x".into(),
                err: "boom".into(),
            },
        ];
        let err = report(&outcomes, Path::new("/out"), true).unwrap_err();
        assert!(format!("{err:#}").contains("1 entr"), "{err:#}");
    }

    #[test]
    fn report_ok_when_none_failed() {
        let outcomes = vec![EntryOutcome::Fetched(10), EntryOutcome::Skipped];
        assert!(report(&outcomes, Path::new("/out"), false).is_ok());
    }
}
