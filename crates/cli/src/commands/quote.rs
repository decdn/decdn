//! `decdn quote <URI>` (#2600): what a pull of a `decdn:` URI costs, from the
//! rates the nodes sign, before any paid byte.
//!
//! The command plans what a `decdn get` of the URI downloads, runs one unpaid
//! probe round ([`decdn_client::quote::census`]) over every node the pull can
//! draw from, and prints:
//!
//! ```text
//! bundle   29.0 GB in 4 entries
//! nodes    6 of 8 hold it   rates 0.0095–0.0114 USDC/GB   (ceiling: 0.0191 USDC/GB)
//! estimate 0.28–0.33 USDC   likely 0.31 USDC
//! ```
//!
//! A blob's size is the nearest holder's report. A bundle's size is its
//! manifest's planned download for a fresh pull: a blob two paths name, and
//! a chunk two blobs share, count once, and a file already on disk still
//! counts. The manifest comes from the output directory's cache, or from a
//! small paid pull that caches it.
//!
//! `decdn get --dry-run` prints the same block for a blob, or for a bundle
//! from a cached manifest (`dry_run_quote`), and `decdn get` prints a
//! one-line estimate before it pulls (`estimate_line`).

use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};

use alloy::primitives::U256;
use anyhow::Context as _;
use decdn_client::bundle::{BundleManifest, PlannedBlob, planned_downloads};
use decdn_client::endpoint as client_endpoint;
use decdn_client::quote::{
    CENSUS_ENTRY_SAMPLE, Census, PriceRange, PricedBlob, RateHint, census, hint_rates, price,
    wire_bytes,
};
use decdn_client::session::ProxyWarmingParams;
use decdn_client::targets::ResolveConfigFault;
use decdn_client::uri::{DecdnUri, UriTarget};
use decdn_common::cli::{ClientFetchArgs, QuoteArgs};
use decdn_common::config::load_file_config;
use iroh::{Endpoint, RelayUrl};

use super::bundle_pull::{self, human_bytes};
use super::fetch;

/// Run `decdn quote`: plan the pull `args.uri` names, probe its nodes, and
/// print the quote.
///
/// # Errors
///
/// The URI does not parse, a flag combination is invalid, the chain
/// coordinates or relays do not resolve, the manifest fetch fails, or no
/// node answers the probe round.
pub async fn quote(args: &QuoteArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let uri: DecdnUri = args
        .uri
        .parse()
        .with_context(|| format!("parse URI {:?}", args.uri))?;
    let mut host = QuoteHost::open(&args.common, config_path).await?;
    let result = quote_on(&mut host, args, &uri).await;
    host.close().await;
    let view = result?;
    let mut stdout = std::io::stdout().lock();
    if args.json {
        writeln!(stdout, "{}", json_quote(&args.uri, &view))?;
    } else {
        write_quote(&mut stdout, &view)?;
    }
    Ok(())
}

/// The plan of `uri` on `host`, priced. A bundle's manifest comes from the
/// cache in `args.output`, or from a paid fetch that caches it there.
async fn quote_on(
    host: &mut QuoteHost,
    args: &QuoteArgs,
    uri: &DecdnUri,
) -> anyhow::Result<QuoteView> {
    let out = args.output.clone().unwrap_or_else(|| PathBuf::from("."));
    let plan = if uri.target == UriTarget::Blob {
        Plan::Blob(uri.hash)
    } else {
        // A cache file that cannot be read is fetched over, as `get` does;
        // the fetch reports it.
        let cached = BundleManifest::cached(&out, uri.hash).ok().flatten();
        let manifest = if let Some(manifest) = cached {
            manifest
        } else {
            eprintln!(
                "note: no cached manifest in {}; fetching it, a small paid pull that opens a \
                 pool if you have none",
                out.display()
            );
            bundle_pull::fetch_manifest_paid(
                &args.common,
                &out,
                uri,
                &mut host.chain,
                (&host.endpoint, &host.relays),
            )
            .await?
        };
        Plan::of_target(&manifest, &uri.target)?
    };
    host.price(&args.common, &plan).await
}

/// What a quote prices on: the chain, the relays and one endpoint for the
/// probe round and any manifest fetch.
pub(crate) struct QuoteHost {
    chain: fetch::ResolvedChain,
    relays: Vec<RelayUrl>,
    endpoint: Endpoint,
}

impl QuoteHost {
    /// Resolve `common` and the config at `config_path`, and bind the
    /// endpoint. Opens no buyer store and no keystore.
    ///
    /// # Errors
    ///
    /// A flag combination clap cannot express, unresolvable chain
    /// coordinates or relays, or an endpoint that cannot bind.
    pub(crate) async fn open(
        common: &ClientFetchArgs,
        config_path: Option<&Path>,
    ) -> anyhow::Result<Self> {
        common.validate()?;
        let relays = client_endpoint::resolve_relays(common.relay_url.as_deref(), config_path)?;
        let disc = client_endpoint::client_discovery(config_path)?;
        let file = load_file_config(config_path)?;
        let chain = fetch::resolve_chain(common, &file)?;
        let endpoint = client_endpoint::client_endpoint(&relays, &disc).await?;
        Ok(Self {
            chain,
            relays,
            endpoint,
        })
    }

    /// Probe the nodes for `plan`'s sampled blobs and price it.
    ///
    /// # Errors
    ///
    /// Discovery fails, or no node answers.
    pub(crate) async fn price(
        &self,
        common: &ClientFetchArgs,
        plan: &Plan,
    ) -> anyhow::Result<QuoteView> {
        let discovery = fetch::discovery_options(common)?;
        let census = census(
            &self.endpoint,
            (&self.chain.config, &self.chain.site),
            &self.relays,
            &discovery,
            &plan.sample(),
        )
        .await?;
        view(
            plan,
            &census,
            common.max_rate_per_mb,
            discovery.proxy_warming,
            common.max_sources,
        )
    }

    /// Close the endpoint, so its connections end cleanly.
    pub(crate) async fn close(self) {
        self.endpoint.close().await;
    }
}

/// What a URI names, for the first line of a quote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Target {
    /// A raw blob.
    Blob,
    /// A whole bundle of `entries` entries.
    Bundle {
        /// The manifest's entry count.
        entries: usize,
    },
    /// One bundle entry.
    Entry {
        /// The entry's manifest path.
        path: String,
    },
}

/// What a pull downloads.
#[derive(Debug, Clone)]
pub(crate) enum Plan {
    /// The blob with this hash. Its size comes from the probe round.
    Blob([u8; 32]),
    /// A bundle, or one of its entries.
    Bundle {
        /// [`Target::Bundle`] or [`Target::Entry`].
        target: Target,
        /// The distinct blobs, with each one's planned download.
        blobs: Vec<PlannedBlob>,
    },
}

impl Plan {
    /// The plan of the bundle whose manifest is `manifest`: every entry, or
    /// only the one whose manifest path is `entry`.
    ///
    /// # Errors
    ///
    /// The manifest has no entry at `entry`.
    pub(crate) fn bundle(manifest: &BundleManifest, entry: Option<&str>) -> anyhow::Result<Self> {
        let Some(path) = entry else {
            return Ok(Self::Bundle {
                target: Target::Bundle {
                    entries: manifest.entries.len(),
                },
                blobs: planned_downloads(&manifest.entries),
            });
        };
        let kept: Vec<_> = manifest
            .entries
            .iter()
            .filter(|e| e.manifest_path() == path)
            .cloned()
            .collect();
        anyhow::ensure!(
            !kept.is_empty(),
            "the bundle has no entry {path:?}; a URI path names one manifest `path` exactly"
        );
        Ok(Self::Bundle {
            target: Target::Entry {
                path: path.to_owned(),
            },
            blobs: planned_downloads(&kept),
        })
    }

    /// [`Self::bundle`] for a bundle URI's `target`: its entry for an entry
    /// URI, else every entry.
    ///
    /// # Errors
    ///
    /// The manifest has no entry at the URI's path.
    pub(crate) fn of_target(manifest: &BundleManifest, target: &UriTarget) -> anyhow::Result<Self> {
        let entry = match target {
            UriTarget::BundleEntry(path) => Some(path.as_str()),
            _ => None,
        };
        Self::bundle(manifest, entry)
    }

    /// What the URI names.
    fn target(&self) -> Target {
        match self {
            Self::Blob(_) => Target::Blob,
            Self::Bundle { target, .. } => target.clone(),
        }
    }

    /// The blobs the probe round asks about: a blob URI's one blob, or the
    /// [`CENSUS_ENTRY_SAMPLE`] largest planned downloads. Rates are per node,
    /// so the sample's rates stand in for the rest of a bundle.
    fn sample(&self) -> Vec<[u8; 32]> {
        match self {
            Self::Blob(hash) => vec![*hash],
            Self::Bundle { blobs, .. } => {
                let mut sized: Vec<&PlannedBlob> =
                    blobs.iter().filter(|b| b.download > 0).collect();
                sized.sort_by_key(|b| std::cmp::Reverse(b.download));
                sized
                    .into_iter()
                    .take(CENSUS_ENTRY_SAMPLE)
                    .map(|b| b.hash)
                    .collect()
            }
        }
    }
}

/// A bundle quote's probe sample: how many of its blobs it probed, and how
/// much of its download they carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Sample {
    /// The blobs probed.
    pub(crate) blobs: usize,
    /// The bundle's distinct blobs with a planned download.
    pub(crate) sized: usize,
    /// The probed blobs' planned download, in content bytes.
    pub(crate) bytes: u64,
}

/// A priced plan, ready to print.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QuoteView {
    /// What the URI names.
    pub(crate) target: Target,
    /// The content bytes a fresh pull downloads; `None` for a blob no
    /// holder reported a size for.
    pub(crate) content_bytes: Option<u64>,
    /// The wire bytes it pays for, bao proof included.
    pub(crate) wire_bytes: Option<u64>,
    /// How many nodes the round probed for each blob.
    pub(crate) probed: usize,
    /// The fewest and most holders of any probed blob; `None` when the
    /// round probed no blob.
    pub(crate) holders: Option<(usize, usize)>,
    /// The most probes of one blob that got no answer.
    pub(crate) unanswered: usize,
    /// The most probes of one blob a node kept shedding.
    pub(crate) rate_limited: usize,
    /// The node list is the peer store's, because the registry read failed.
    pub(crate) outage_fallback: bool,
    /// The lowest and highest rate of the nodes the pull would draw from,
    /// in micro-USDC per MiB.
    pub(crate) rates: Option<(u64, u64)>,
    /// The `--max-rate-per-mb` ceiling; `0` sets none.
    pub(crate) ceiling: u64,
    /// Answering nodes above the ceiling, which the pull refuses.
    pub(crate) above_ceiling: usize,
    /// The cost; `None` when the size or every rate is unknown.
    pub(crate) price: Option<PriceRange>,
    /// A bundle's probe sample; `None` for a blob.
    pub(crate) sample: Option<Sample>,
    /// Probed blobs no usable node answered for. Their bytes are priced at
    /// the other blobs' rates.
    pub(crate) unrated_blobs: usize,
    /// Distinct blobs whose manifest entries declare no size, which the
    /// quote cannot price.
    pub(crate) unsized_blobs: usize,
}

/// The `(min, max)` of `range` and `x`.
fn widen<T: Copy + Ord>(range: Option<(T, T)>, x: T) -> (T, T) {
    range.map_or((x, x), |(lo, hi)| (lo.min(x), hi.max(x)))
}

/// Price `plan` from `census`, ranking each blob's answers as the pull does
/// under `ceiling`, `warming` and `max_sources`.
///
/// # Errors
///
/// The bao wire length of a size cannot be computed.
pub(crate) fn view(
    plan: &Plan,
    census: &Census,
    ceiling: u64,
    warming: ProxyWarmingParams,
    max_sources: usize,
) -> anyhow::Result<QuoteView> {
    let mut priced = Vec::new();
    let mut rates = None;
    let mut holders = None;
    let mut above = HashSet::new();
    let (mut unanswered, mut rate_limited, mut unrated_blobs) = (0, 0, 0);
    let mut blob_size = None;
    for probed in &census.hashes {
        let content = match plan {
            Plan::Blob(_) => {
                blob_size = probed.size_hint();
                blob_size
            }
            Plan::Bundle { blobs, .. } => blobs
                .iter()
                .find(|b| b.hash == probed.hash)
                .map(|b| b.download),
        };
        let order: Vec<u64> = probed
            .serve_order(ceiling, warming)
            .iter()
            .map(|a| a.rate_per_mb)
            .collect();
        for &rate in &order {
            rates = Some(widen(rates, rate));
        }
        // A blob URI's one blob has no price at all, not a borrowed one.
        if order.is_empty() && matches!(plan, Plan::Bundle { .. }) {
            unrated_blobs += 1;
        }
        holders = Some(widen(holders, probed.holders()));
        unanswered = unanswered.max(probed.unreachable);
        rate_limited = rate_limited.max(probed.rate_limited);
        for answer in &probed.answers {
            if ceiling != 0 && answer.rate_per_mb > ceiling {
                above.insert(answer.candidate.node_id);
            }
        }
        if let Some(content) = content {
            priced.push(PricedBlob {
                wire: wire_bytes(content)?,
                rates: order,
            });
        }
    }
    let (content_bytes, unsampled_wire, sample, unsized_blobs) = match plan {
        Plan::Blob(_) => (blob_size, 0, None, 0),
        Plan::Bundle { blobs, .. } => {
            let sampled: HashSet<[u8; 32]> = census.hashes.iter().map(|h| h.hash).collect();
            let mut unsampled = 0u64;
            let mut sampled_bytes = 0u64;
            let mut total = 0u64;
            for blob in blobs {
                total = total.saturating_add(blob.download);
                if sampled.contains(&blob.hash) {
                    sampled_bytes = sampled_bytes.saturating_add(blob.download);
                } else {
                    unsampled = unsampled.saturating_add(wire_bytes(blob.download)?);
                }
            }
            let sample = Sample {
                blobs: census.hashes.len(),
                sized: blobs.iter().filter(|b| b.download > 0).count(),
                bytes: sampled_bytes,
            };
            let unsized_blobs = blobs.iter().filter(|b| b.size.is_none()).count();
            (Some(total), unsampled, Some(sample), unsized_blobs)
        }
    };
    let price = content_bytes.and_then(|_| price(&priced, unsampled_wire, max_sources));
    Ok(QuoteView {
        target: plan.target(),
        content_bytes,
        wire_bytes: content_bytes.map(wire_bytes).transpose()?,
        probed: census.probed,
        holders,
        unanswered,
        rate_limited,
        outage_fallback: census.outage_fallback,
        rates,
        ceiling,
        above_ceiling: above.len(),
        price,
        sample,
        unrated_blobs,
        unsized_blobs,
    })
}

/// Write the quote's text block to `w`.
///
/// # Errors
///
/// The write fails.
pub(crate) fn write_quote(w: &mut impl Write, view: &QuoteView) -> std::io::Result<()> {
    let size = view
        .content_bytes
        .map_or_else(|| "size unknown".to_owned(), human_bytes);
    match &view.target {
        Target::Blob => writeln!(w, "blob     {size}")?,
        Target::Bundle { entries } => writeln!(w, "bundle   {size} in {entries} entries")?,
        Target::Entry { path } => writeln!(w, "entry    {path}  {size}")?,
    }
    if let Some((lo, hi)) = view.holders {
        let held = if hi == 0 {
            format!(
                "none of {} hold it; they pull it through from origin",
                view.probed
            )
        } else if lo == hi {
            format!("{hi} of {} hold it", view.probed)
        } else {
            format!("{lo}–{hi} of {} hold each blob", view.probed)
        };
        let silent = match (view.unanswered, view.rate_limited) {
            (0, 0) => String::new(),
            (n, 0) => format!(" ({n} did not answer)"),
            (0, m) => format!(" ({m} rate-limited the probe)"),
            (n, m) => format!(" ({n} did not answer, {m} rate-limited the probe)"),
        };
        let rates = view.rates.map_or_else(
            || "no usable rate".to_owned(),
            |(lo, hi)| format!("rates {} USDC/GB", rate_range(lo, hi)),
        );
        let ceiling = if view.ceiling == 0 {
            "ceiling: none".to_owned()
        } else {
            format!("ceiling: {} USDC/GB", usdc_per_gb(view.ceiling))
        };
        writeln!(w, "nodes    {held}{silent}   {rates}   ({ceiling})")?;
    }
    match (&view.price, view.content_bytes) {
        (Some(price), _) => writeln!(
            w,
            "estimate {}   likely {} USDC",
            usdc_range(price.low, price.high),
            usdc_short(price.likely)
        )?,
        (None, None) => writeln!(w, "estimate unknown: no holder reported the size")?,
        (None, Some(0)) if view.unsized_blobs > 0 => {
            writeln!(w, "estimate unknown: no blob declares a size")?;
        }
        (None, Some(0)) => writeln!(w, "estimate 0.00 USDC: nothing to download")?,
        (None, Some(_)) if view.above_ceiling > 0 => writeln!(
            w,
            "estimate none: every answering node is above the ceiling"
        )?,
        (None, Some(_)) => writeln!(w, "estimate none: no node answered with a rate")?,
    }
    if view.above_ceiling > 0 {
        writeln!(
            w,
            "note     {} node(s) above the ceiling are left out; the pull refuses them",
            view.above_ceiling
        )?;
    }
    if let (Some(sample), Some(total)) = (view.sample, view.content_bytes)
        && sample.blobs < sample.sized
    {
        writeln!(
            w,
            "sampled  the {} largest of {} blobs ({}% of the bytes); the rest are priced at \
             their mean rate",
            sample.blobs,
            sample.sized,
            percent(sample.bytes, total)
        )?;
    }
    if view.unrated_blobs > 0 && view.price.is_some() {
        writeln!(
            w,
            "note     {} probed blob(s) got no usable rate; their bytes are priced at the \
             others' rates, but the pull may not get them",
            view.unrated_blobs
        )?;
    }
    if view.unsized_blobs > 0 {
        writeln!(
            w,
            "note     {} blob(s) declare no size in the manifest and are not priced",
            view.unsized_blobs
        )?;
    }
    if view.outage_fallback {
        writeln!(
            w,
            "note     the registry read failed; the nodes are the peer store's last-known ones"
        )?;
    }
    Ok(())
}

/// The quote as JSON: sizes in bytes, rates in micro-USDC per MiB, and
/// prices in micro-USDC as decimal strings.
pub(crate) fn json_quote(uri: &str, view: &QuoteView) -> serde_json::Value {
    let (target, entries, path) = match &view.target {
        Target::Blob => ("blob", None, None),
        Target::Bundle { entries } => ("bundle", Some(*entries), None),
        Target::Entry { path } => ("entry", None, Some(path.as_str())),
    };
    serde_json::json!({
        "uri": uri,
        "target": target,
        "entries": entries,
        "path": path,
        "content_bytes": view.content_bytes,
        "wire_bytes": view.wire_bytes,
        "nodes_probed": view.probed,
        "holders_min": view.holders.map(|h| h.0),
        "holders_max": view.holders.map(|h| h.1),
        "unanswered": view.unanswered,
        "rate_limited": view.rate_limited,
        "outage_fallback": view.outage_fallback,
        "rate_min_per_mib": view.rates.map(|r| r.0),
        "rate_max_per_mib": view.rates.map(|r| r.1),
        "ceiling_per_mib": (view.ceiling != 0).then_some(view.ceiling),
        "above_ceiling": view.above_ceiling,
        "price_micro_usdc": view.price.map(|p| serde_json::json!({
            "low": p.low.to_string(),
            "high": p.high.to_string(),
            "likely": p.likely.to_string(),
        })),
        "sampled_blobs": view.sample.map(|s| s.blobs),
        "sampled_content_bytes": view.sample.map(|s| s.bytes),
        "unrated_blobs": view.unrated_blobs,
        "unsized_blobs": view.unsized_blobs,
    })
}

/// The one-line estimate `decdn get` prints before it pulls `content` bytes
/// from nodes at `rates`, in the order the pull draws from them, or `None`
/// with no rate.
pub(crate) fn estimate_line(content: u64, rates: &[u64], max_sources: usize) -> Option<String> {
    let wire = wire_bytes(content).ok()?;
    let priced = PricedBlob {
        wire,
        rates: rates.to_vec(),
    };
    let price = price(&[priced], 0, max_sources)?;
    let lo = rates.iter().copied().min()?;
    let hi = rates.iter().copied().max()?;
    let cost = if price.low == price.high {
        format!("{} USDC", usdc_short(price.likely))
    } else {
        format!(
            "{}, likely {} USDC",
            usdc_range(price.low, price.high),
            usdc_short(price.likely)
        )
    };
    Some(format!(
        "Estimate: {cost} ({} at {} USDC/GB)",
        human_bytes(content),
        rate_range(lo, hi)
    ))
}

/// Print `decdn get`'s one-line estimate ([`estimate_line`]) on stderr for a
/// pull of `content` bytes from the nodes of `hints`, in the order the pull
/// draws from them, under `common`'s ceiling and `--max-sources`. Nothing
/// prints when no hint has a usable rate.
pub(crate) fn print_estimate(content: u64, hints: &[RateHint], common: &ClientFetchArgs) {
    let rates = hint_rates(hints, common.max_rate_per_mb);
    if let Some(line) = estimate_line(content, &rates, common.max_sources) {
        eprintln!("{line}");
    }
}

/// `decdn get --dry-run`'s quote of `plan`: the view, or the reason there
/// is none, to print in its place. A dry run lists its plan with no chain
/// configured, so a quote it cannot set up is a reason, not an error.
///
/// # Errors
///
/// A configuration fault the real run fails on the same way
/// ([`ResolveConfigFault`]): a malformed `--node-id` or `--provider-address`,
/// or nodes that answered but whose probe signatures did not recover to
/// their operators, because the configured chain is not theirs.
pub(crate) async fn dry_run_quote(
    common: &ClientFetchArgs,
    config_path: Option<&Path>,
    plan: &Plan,
) -> anyhow::Result<Result<QuoteView, String>> {
    let quoted = async {
        let host = QuoteHost::open(common, config_path).await?;
        let view = host.price(common, plan).await;
        host.close().await;
        view
    }
    .await;
    match quoted {
        Ok(view) => Ok(Ok(view)),
        Err(e)
            if e.chain()
                .any(<dyn std::error::Error>::is::<ResolveConfigFault>) =>
        {
            Err(e)
        }
        Err(e) => Ok(Err(format!("quote unavailable: {e:#}"))),
    }
}

/// `decdn get --dry-run decdn:<hash>`: print where the blob would land and
/// its quote, and exit. Nothing is paid and no keystore is opened.
///
/// # Errors
///
/// Writing to stdout fails, or a fault [`dry_run_quote`] ends on. Any other
/// quote failure prints a warning instead, and `quote_error` under `--json`.
pub(crate) async fn dry_run_blob(
    args: &decdn_common::cli::GetArgs,
    output: &Path,
    uri: &DecdnUri,
    config_path: Option<&Path>,
) -> anyhow::Result<()> {
    let quoted = dry_run_quote(&args.common, config_path, &Plan::Blob(uri.hash)).await?;
    let hash = blake3::Hash::from_bytes(uri.hash).to_hex();
    if let Err(warning) = &quoted {
        eprintln!("warning: {warning}");
    }
    let mut stdout = std::io::stdout().lock();
    if args.json {
        let plan = serde_json::json!({
            "output": output.display().to_string(),
            "hash": hash.as_str(),
            "quote": quoted.as_ref().ok().map(|view| json_quote(&args.uri, view)),
            "quote_error": quoted.as_ref().err(),
        });
        writeln!(stdout, "{plan}")?;
        return Ok(());
    }
    writeln!(
        stdout,
        "--dry-run: would fetch blob {hash} into {}",
        output.display()
    )?;
    if let Ok(view) = quoted {
        write_quote(&mut stdout, &view)?;
    }
    Ok(())
}

/// `arg` quoted for a POSIX shell: in single quotes, each `'` closed, escaped
/// and reopened.
pub(crate) fn shell_quote(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', r"'\''"))
}

/// A rate range in USDC per GB, one figure when both ends agree.
fn rate_range(lo: u64, hi: u64) -> String {
    if lo == hi {
        usdc_per_gb(lo)
    } else {
        format!("{}–{}", usdc_per_gb(lo), usdc_per_gb(hi))
    }
}

/// A price range in USDC, one figure when both ends agree.
fn usdc_range(lo: U256, hi: U256) -> String {
    if lo == hi {
        format!("{} USDC", usdc_short(lo))
    } else {
        format!("{}–{} USDC", usdc_short(lo), usdc_short(hi))
    }
}

/// A per-MiB rate in micro-USDC as USDC per (SI) GB, to four decimals, the
/// unit `human_bytes` prints sizes in.
pub(crate) fn usdc_per_gb(rate_per_mib: u64) -> String {
    // micro-USDC per GB = rate × 10^9 / 2^20, then in units of 10^-4 USDC
    // (100 micro-USDC), rounded half up.
    let per_gb = u128::from(rate_per_mib) * 1_000_000_000;
    let tenth_milli = (per_gb + 50 * 1_048_576) / (100 * 1_048_576);
    format!("{}.{:04}", tenth_milli / 10_000, tenth_milli % 10_000)
}

/// A micro-USDC amount in USDC with two decimals, or more (at most six)
/// until two significant figures show, rounded half up.
pub(crate) fn usdc_short(micro: U256) -> String {
    let micro: u128 = micro.saturating_to();
    let mut decimals = 2u32;
    loop {
        let unit = 10u128.pow(6 - decimals);
        let scaled = (micro + unit / 2) / unit;
        if decimals == 6 || scaled >= 10 {
            let scale = 10u128.pow(decimals);
            return format!(
                "{}.{:0width$}",
                scaled / scale,
                scaled % scale,
                width = usize::try_from(decimals).unwrap_or(6)
            );
        }
        decimals += 1;
    }
}

/// `part` as a whole percentage of `whole`, rounded down; `100` for an empty
/// whole.
fn percent(part: u64, whole: u64) -> u64 {
    if whole == 0 {
        return 100;
    }
    u64::try_from(u128::from(part) * 100 / u128::from(whole)).unwrap_or(100)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
