//! Arguments for `decdn quote <URI>` (#2600): price a pull of what a `decdn:`
//! URI names from the rates the nodes sign in their probe answers, before any
//! paid byte.

use std::path::PathBuf;

use clap::Args;

use super::fetch::ClientFetchArgs;

/// `decdn quote` — price a pull of what a `decdn:` URI names.
///
/// A probe round over every node a pull can draw from returns each node's
/// signed per-MiB rate. The quote prints the size, the nodes that hold the
/// content and their rates, the `--max-rate-per-mb` ceiling, and the cost of
/// a fresh pull (files already on disk still count): on the cheapest node,
/// on the most expensive, and the likely cost over the nodes a pull that
/// probed every node would use. The probes are unpaid.
///
/// A blob's size is the nearest holder's report. A bundle's size comes from
/// its manifest. A manifest an earlier pull or quote cached in the output
/// directory costs nothing; otherwise the quote fetches it, which is a small
/// paid pull and can open a pool, and caches it for the `decdn get` that
/// follows.
#[derive(Args, Debug)]
pub struct QuoteArgs {
    /// The `decdn:` URI to price: `decdn:<hash>[/<path>][?ns=<id>]`.
    #[arg(value_name = "URI")]
    pub uri: String,

    /// The output directory of the `decdn get` this quote prices: where a
    /// bundle's cached manifest is read from, and cached to when the quote
    /// fetches it. The default is `decdn get`'s: `.`.
    #[arg(short = 'o', long, value_name = "DIR")]
    pub output: Option<PathBuf>,

    /// Print the quote as one line of JSON, with every amount in
    /// micro-USDC.
    #[arg(long)]
    pub json: bool,

    /// Retrieval flags: peer discovery, chain coordinates, the rate ceiling,
    /// and `--max-sources` and the proxy-warming flags, which set the order
    /// and the likely cost. The keystore and pool flags apply only when a
    /// bundle's manifest must be fetched.
    #[command(flatten)]
    pub common: ClientFetchArgs,
}
