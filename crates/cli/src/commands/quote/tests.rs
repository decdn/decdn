use alloy::primitives::Address;
use decdn_client::bundle::ManifestEntry;
use decdn_client::discovery::NodeCandidate;
use decdn_client::quote::{HashCensus, NodeAnswer};

use clap::Parser as _;

use super::*;

const GB: u64 = 1_000_000_000;

fn node(seed: u8) -> NodeCandidate {
    NodeCandidate {
        node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
        eth_address: Address::repeat_byte(seed),
        region_hint: None,
        multiaddrs: alloy::primitives::Bytes::new(),
    }
}

fn answer(seed: u8, rtt_ms: f64, rate_per_mb: u64, has_blob: bool, size: u64) -> NodeAnswer {
    NodeAnswer {
        candidate: node(seed),
        rtt_ms,
        rate_per_mb,
        has_blob,
        total_bytes: has_blob.then_some(size),
        coverage: decdn_protocol::Coverage::empty(),
    }
}

fn hash_census(hash: [u8; 32], answers: Vec<NodeAnswer>) -> HashCensus {
    HashCensus {
        hash,
        answers,
        unreachable: 0,
        rate_limited: 0,
        clock_skewed: 0,
        unverifiable: 0,
        unusable: 0,
    }
}

const fn no_warming() -> ProxyWarmingParams {
    ProxyWarmingParams {
        enabled: false,
        rtt_threshold_ms: 150.0,
        margin_ms: 30.0,
    }
}

fn text(view: &QuoteView) -> String {
    let mut out = Vec::new();
    write_quote(&mut out, view).unwrap();
    String::from_utf8(out).unwrap()
}

fn entry(path: &str, hash: u8, size: Option<u64>) -> ManifestEntry {
    ManifestEntry::new(path, [hash; 32], size)
}

/// A blob quote takes its size from the nearest holder and prices the
/// holders it would draw from.
#[test]
fn a_blob_quote_prices_its_holders() {
    let hash = [9; 32];
    let census = Census {
        probed: 8,
        hashes: vec![hash_census(
            hash,
            vec![
                answer(1, 10.0, 10, true, 15 * GB),
                answer(2, 20.0, 11, true, 15 * GB),
                answer(3, 5.0, 3, false, 0),
            ],
        )],
        outage_fallback: false,
    };
    let view = view(&Plan::Blob(hash), &census, 20, no_warming(), 4).unwrap();
    assert_eq!(view.content_bytes, Some(15 * GB));
    assert_eq!(view.holders, Some((2, 2)));
    assert_eq!(view.rates, Some((10, 11)));
    assert_eq!(view.above_ceiling, 0);
    let wire = decdn_client::quote::wire_bytes(15 * GB).unwrap();
    let price = view.price.unwrap();
    assert_eq!(
        price.low,
        decdn_incentive::rate::min_payment(wire, 10),
        "{price:?}"
    );
    assert_eq!(price.high, decdn_incentive::rate::min_payment(wire, 11));
    assert_eq!(
        text(&view),
        "blob     15.0 GB\n\
         nodes    2 of 8 hold it   rates 0.0095–0.0105 USDC/GB   (ceiling: 0.0191 USDC/GB)\n\
         estimate 0.14–0.16 USDC   likely 0.15 USDC\n"
    );
}

/// A node above the ceiling is left out of the price and named in a note.
#[test]
fn a_node_above_the_ceiling_is_left_out() {
    let hash = [9; 32];
    let census = Census {
        probed: 2,
        hashes: vec![hash_census(
            hash,
            vec![answer(1, 10.0, 50, true, GB), answer(2, 20.0, 10, true, GB)],
        )],
        outage_fallback: false,
    };
    let view = view(&Plan::Blob(hash), &census, 20, no_warming(), 4).unwrap();
    assert_eq!(view.rates, Some((10, 10)));
    assert_eq!(view.above_ceiling, 1);
    let out = text(&view);
    assert!(out.contains("1 node(s) above the ceiling"), "{out}");

    let all_above = view_all_above(hash);
    let out = text(&all_above);
    assert!(
        out.contains("estimate none: every answering node is above the ceiling"),
        "{out}"
    );
}

fn view_all_above(hash: [u8; 32]) -> QuoteView {
    let census = Census {
        probed: 1,
        hashes: vec![hash_census(hash, vec![answer(1, 10.0, 50, true, GB)])],
        outage_fallback: false,
    };
    view(&Plan::Blob(hash), &census, 20, no_warming(), 4).unwrap()
}

/// A blob no node holds has no size yet, so the quote gives its rates and
/// no price.
#[test]
fn a_cold_blob_has_rates_but_no_size() {
    let hash = [9; 32];
    let census = Census {
        probed: 3,
        hashes: vec![hash_census(hash, vec![answer(1, 10.0, 10, false, 0)])],
        outage_fallback: false,
    };
    let view = view(&Plan::Blob(hash), &census, 0, no_warming(), 4).unwrap();
    assert_eq!(view.content_bytes, None);
    assert_eq!(view.price, None);
    assert_eq!(
        text(&view),
        "blob     size unknown\n\
         nodes    none of 3 hold it; they pull it through from origin   rates 0.0095 USDC/GB   \
         (ceiling: none)\n\
         estimate unknown: no holder reported the size\n"
    );
}

/// A bundle quote probes its largest blobs, prices the rest at their mean
/// rate, counts a blob two paths name once, and reports the sample.
#[test]
fn a_bundle_quote_samples_its_largest_blobs() {
    let entries: Vec<ManifestEntry> = (1..=6u8)
        .map(|i| entry(&format!("f{i}"), i, Some(u64::from(i) * GB)))
        .chain([entry("again", 6, Some(6 * GB)), entry("unsized", 7, None)])
        .collect();
    let manifest = BundleManifest::new([0xbb; 32], entries);
    let plan = Plan::bundle(&manifest, None).unwrap();
    let sample = plan.sample();
    assert_eq!(sample, vec![[6; 32], [5; 32], [4; 32], [3; 32]]);
    let census = Census {
        probed: 3,
        hashes: sample
            .iter()
            .map(|&h| {
                hash_census(
                    h,
                    vec![answer(1, 10.0, 10, true, 0), answer(2, 20.0, 10, true, 0)],
                )
            })
            .collect(),
        outage_fallback: false,
    };
    let view = view(&plan, &census, 0, no_warming(), 4).unwrap();
    assert_eq!(view.content_bytes, Some(21 * GB));
    assert_eq!(
        view.sample,
        Some(Sample {
            blobs: 4,
            sized: 6,
            bytes: 18 * GB
        })
    );
    assert_eq!(view.unsized_blobs, 1);
    let out = text(&view);
    assert!(out.starts_with("bundle   21.0 GB in 8 entries\n"), "{out}");
    assert!(out.contains("2 of 3 hold it"), "{out}");
    assert!(
        out.contains("the 4 largest of 6 blobs (85% of the bytes)"),
        "{out}"
    );
    assert!(out.contains("1 blob(s) declare no size"), "{out}");
    // Every rate is 10, so every figure prices the whole 21 GB, the 3 GB of
    // the two unsampled blobs included, and the unsized blob not at all.
    let price = view.price.unwrap();
    let wire = |gb: u64| wire_bytes(gb * GB).unwrap();
    let sampled: u64 = [6, 5, 4, 3].map(wire).iter().sum();
    let all = sampled + wire(2) + wire(1);
    assert_eq!(price.low, decdn_incentive::rate::min_payment(all, 10));
    assert_eq!(price.high, price.low);
    assert!(price.likely <= price.high && price.likely >= price.low);
    // The wire total sums each blob's own wire bytes, not the content total
    // encoded as one blob.
    assert_eq!(view.wire_bytes, Some(all));
    assert_ne!(view.wire_bytes, Some(wire(21)));
    let json = json_quote("decdn:b/", &view);
    assert_eq!(json["target"], "bundle");
    assert_eq!(json["entries"], 8);
    assert_eq!(json["sampled_blobs"], 4);
    assert_eq!(json["sampled_content_bytes"], 18 * GB);
    assert_eq!(json["unsized_blobs"], 1);
}

/// A probed blob no usable node answered for still costs its bytes, at the
/// other blobs' rates, and the quote says so. Nodes that did not answer are
/// named apart from non-holders.
#[test]
fn a_blob_with_no_usable_rate_is_priced_and_named() {
    let manifest = BundleManifest::new(
        [0xbb; 32],
        vec![entry("big", 1, Some(3 * GB)), entry("small", 2, Some(GB))],
    );
    let plan = Plan::bundle(&manifest, None).unwrap();
    let census = Census {
        probed: 4,
        hashes: vec![
            HashCensus {
                unreachable: 3,
                rate_limited: 1,
                ..hash_census([1; 32], Vec::new())
            },
            hash_census([2; 32], vec![answer(1, 10.0, 10, true, 0)]),
        ],
        outage_fallback: true,
    };
    let view = view(&plan, &census, 0, no_warming(), 4).unwrap();
    assert_eq!(view.unrated_blobs, 1);
    assert_eq!(view.holders, Some((0, 1)));
    let price = view.price.unwrap();
    let all = wire_bytes(3 * GB).unwrap() + wire_bytes(GB).unwrap();
    assert_eq!(price.low, decdn_incentive::rate::min_payment(all, 10));
    let out = text(&view);
    assert!(
        out.contains("0–1 of 4 hold each blob (3 did not answer, 1 rate-limited the probe)"),
        "{out}"
    );
    assert!(out.contains("1 probed blob(s) got no usable rate"), "{out}");
    assert!(out.contains("the registry read failed"), "{out}");
    let json = json_quote("decdn:b/", &view);
    assert_eq!(json["unanswered"], 3);
    assert_eq!(json["rate_limited"], 1);
    assert_eq!(json["outage_fallback"], true);
    assert_eq!(json["unrated_blobs"], 1);
}

/// A bundle with nothing to probe makes no claim about its nodes: one whose
/// blobs declare no size has an unknown price, and an empty one costs
/// nothing.
#[test]
fn a_bundle_with_nothing_to_probe_makes_no_node_claim() {
    let empty = Census {
        probed: 5,
        hashes: Vec::new(),
        outage_fallback: false,
    };
    let no_sizes = BundleManifest::new([0xbb; 32], vec![entry("a", 1, None)]);
    let plan = Plan::bundle(&no_sizes, None).unwrap();
    assert!(plan.sample().is_empty());
    let view = view(&plan, &empty, 0, no_warming(), 4).unwrap();
    assert_eq!(view.holders, None);
    assert_eq!(
        text(&view),
        "bundle   0 B in 1 entries\n\
         estimate unknown: no blob declares a size\n\
         note     1 blob(s) declare no size in the manifest and are not priced\n"
    );
    let nothing = BundleManifest::new([0xbb; 32], Vec::new());
    let view = view_of(&Plan::bundle(&nothing, None).unwrap(), &empty);
    assert_eq!(
        text(&view),
        "bundle   0 B in 0 entries\nestimate 0.00 USDC: nothing to download\n"
    );
    assert_eq!(
        json_quote("decdn:b/", &view)["holders_min"],
        serde_json::Value::Null
    );
}

fn view_of(plan: &Plan, census: &Census) -> QuoteView {
    view(plan, census, 0, no_warming(), 4).unwrap()
}

/// An entry URI plans its one entry, and a missing entry is an error.
#[test]
fn an_entry_plan_keeps_its_entry() {
    let manifest = BundleManifest::new(
        [0xbb; 32],
        vec![entry("a", 1, Some(10)), entry("b", 2, Some(20))],
    );
    let plan = Plan::bundle(&manifest, Some("b")).unwrap();
    assert_eq!(
        plan.target(),
        Target::Entry {
            path: "b".to_owned()
        }
    );
    assert_eq!(plan.sample(), vec![[2; 32]]);
    let census = Census {
        probed: 1,
        hashes: vec![hash_census([2; 32], vec![answer(1, 10.0, 10, true, 0)])],
        outage_fallback: false,
    };
    let view = view_of(&plan, &census);
    assert!(
        text(&view).starts_with(
            "entry    b  20 B
"
        ),
        "{}",
        text(&view)
    );
    let json = json_quote("decdn:b/b", &view);
    assert_eq!(json["target"], "entry");
    assert_eq!(json["path"], "b");
    // An entry `get` lands at its name still plans by its manifest path.
    let landed = BundleManifest::new([0xbb; 32], vec![entry("dir/b", 2, Some(20)).land_at("b")]);
    let plan = Plan::bundle(&landed, Some("dir/b")).unwrap();
    assert_eq!(plan.sample(), vec![[2; 32]]);
    let err = Plan::bundle(&manifest, Some("c")).unwrap_err();
    assert!(format!("{err}").contains("no entry \"c\""), "{err}");
}

/// The JSON quote carries exact micro-USDC strings and the raw rates.
#[test]
fn the_json_quote_is_exact() {
    let hash = [9; 32];
    let census = Census {
        probed: 1,
        hashes: vec![hash_census(hash, vec![answer(1, 10.0, 8, true, 1 << 20)])],
        outage_fallback: false,
    };
    let view = view(&Plan::Blob(hash), &census, 0, no_warming(), 4).unwrap();
    let json = json_quote("decdn:x", &view);
    assert_eq!(json["target"], "blob");
    assert_eq!(json["content_bytes"], 1 << 20);
    assert_eq!(json["rate_min_per_mib"], 8);
    assert_eq!(json["ceiling_per_mib"], serde_json::Value::Null);
    let wire = view.wire_bytes.unwrap();
    let low = decdn_incentive::rate::min_payment(wire, 8).to_string();
    assert_eq!(json["holders_min"], 1);
    assert_eq!(json["unanswered"], 0);
    assert_eq!(json["price_micro_usdc"]["low"], low.as_str());
    assert_eq!(json["price_micro_usdc"]["likely"], low.as_str());
}

/// The one-line estimate gives one figure when every rate agrees, and a
/// range otherwise; with no rate it is absent.
#[test]
fn the_estimate_line_reads_one_figure_or_a_range() {
    assert_eq!(
        estimate_line((29 * GB, wire_bytes(29 * GB).unwrap()), &[10, 10], 4).unwrap(),
        "Estimate: 0.28 USDC (29.0 GB at 0.0095 USDC/GB)"
    );
    assert_eq!(
        estimate_line((29 * GB, wire_bytes(29 * GB).unwrap()), &[10, 12], 4).unwrap(),
        "Estimate: 0.28–0.33 USDC, likely 0.31 USDC (29.0 GB at 0.0095–0.0114 USDC/GB)"
    );
    assert_eq!(estimate_line((GB, GB), &[], 4), None);
}

/// Rates convert from micro-USDC per MiB to USDC per SI GB, rounded.
#[test]
fn rates_read_in_usdc_per_gb() {
    assert_eq!(usdc_per_gb(1), "0.0010");
    assert_eq!(usdc_per_gb(10), "0.0095");
    assert_eq!(usdc_per_gb(1000), "0.9537");
}

/// Amounts keep two decimals, or more until two significant figures show.
#[test]
fn amounts_show_two_significant_figures_at_least() {
    let usdc = |micro: u64| usdc_short(U256::from(micro));
    assert_eq!(usdc(0), "0.000000");
    assert_eq!(usdc(290_000), "0.29");
    assert_eq!(usdc(15_300_000), "15.30");
    assert_eq!(usdc(123), "0.00012");
    assert_eq!(usdc(7), "0.000007");
    assert_eq!(usdc(2_994_999), "2.99");
}

/// A shell-quoted argument survives a single quote of its own.
#[test]
fn shell_quote_escapes_single_quotes() {
    assert_eq!(shell_quote("decdn:ab/?ns=1"), "'decdn:ab/?ns=1'");
    assert_eq!(shell_quote("it's"), r"'it'\''s'");
}

/// A dry run rejects flags the real run rejects before any quote set-up,
/// with or without a chain configured: a dangling `--provider-address`, and
/// a malformed `--node-id`.
#[tokio::test]
async fn a_dry_run_quote_rejects_bad_pin_flags_first() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(&config, "").unwrap();
    let provider = "0x0000000000000000000000000000000000000001";
    let parse = |extra: &[&str]| {
        let argv = ["decdn", "quote", "decdn:00"]
            .into_iter()
            .chain(extra.iter().copied());
        match decdn_common::cli::Cli::try_parse_from(argv)
            .unwrap()
            .command
        {
            decdn_common::cli::Command::Quote(quote) => quote.common,
            other => panic!("parsed {other:?}"),
        }
    };
    for (extra, want) in [
        (
            vec!["--provider-address", provider],
            "--provider-address requires --node-id",
        ),
        (
            vec!["--node-id", "garbage", "--provider-address", provider],
            "invalid --node-id",
        ),
    ] {
        let common = parse(&extra);
        let err = dry_run_quote(&common, Some(&config), &Plan::Blob([1; 32]))
            .await
            .expect_err("a bad pin flag fails the dry run");
        assert!(format!("{err:#}").contains(want), "{err:#}");
    }
}

/// The `QuoteArgs` of `decdn quote <argv>`.
fn quote_args(argv: &[&str]) -> QuoteArgs {
    let argv = ["decdn", "quote"].into_iter().chain(argv.iter().copied());
    match decdn_common::cli::Cli::try_parse_from(argv)
        .unwrap()
        .command
    {
        decdn_common::cli::Command::Quote(quote) => *quote,
        other => panic!("parsed {other:?}"),
    }
}

/// #2675: a quote that would open a pool without `--open-pool` names what
/// opening one costs, says the refused `openPool` and its approve were not
/// sent (a `topUp` of the run may have sent an approve before it), and names both
/// ways on: the same command with `--open-pool` added, and `decdn pool open`.
/// It prints no command line to paste, which would drop the flags that pick
/// the wallet, the chain and the deposit.
#[test]
fn a_refused_open_names_the_cost_and_both_ways_on() {
    let refused = PoolOpenRefused {
        deposit: U256::from(1_000_000u64),
    };
    let msg = open_refused(&refused);
    for part in [
        "no open pool pays for the manifest fetch",
        "escrows the working deposit (1.00 USDC)",
        "pays gas for the openPool (and an approve when the allowance is short)",
        "this openPool and its approve were not sent",
        "Rerun the same `decdn quote` command with `--open-pool` added",
        "`decdn pool open --deposit-micro-usdc <amount>`",
    ] {
        assert!(msg.contains(part), "{part:?} missing from: {msg}");
    }
    assert!(!msg.contains("decdn quote '"), "{msg}");
    assert!(quote_args(&["decdn:ab/", "--open-pool"]).open_pool);
    assert!(!quote_args(&["decdn:ab/"]).open_pool);
}
