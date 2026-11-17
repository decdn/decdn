//! Arguments for `decdn quote <URI>` (#2600): price a pull of what a `decdn:`
//! URI names from the rates the nodes sign in their answers to stream opens,
//! before any paid byte.

use std::path::PathBuf;

use clap::Args;

use super::fetch::ClientFetchArgs;

/// `decdn quote` — price a pull of what a `decdn:` URI names.
///
/// The quote unlocks your keystore and names your pool as `decdn get` does,
/// then opens one `cdn/client/v1` stream to every node a pull can draw from
/// for each blob it prices. Each node answers with its signed per-MiB rate,
/// the blob's proven size, and the blocks it holds, and the quote cancels the
/// stream before any voucher: the opens are unpaid and send no transaction.
/// The quote prints the size, the nodes that hold the content and their
/// rates, the `--max-rate-per-mb` ceiling, and the cost of a fresh pull
/// (files already on disk still count): on the cheapest node, on the most
/// expensive, and the likely cost over the nodes a pull that opened every
/// node would use.
///
/// A blob's size is the nearest holder's report. A bundle's size comes from
/// its manifest. A manifest an earlier pull or quote cached in the output
/// directory costs nothing; otherwise the quote fetches it with a small paid
/// pull from your open pool, and caches it for the `decdn get` that follows.
/// With no open pool, the quote sends no approve or `openPool` unless
/// `--open-pool` lets it open one. With or without the flag, an open pool of
/// your own gets a `topUp` back to the working deposit when it runs low or a
/// node refuses it for funds, which escrows USDC and pays gas.
#[derive(Args, Debug)]
pub struct QuoteArgs {
    /// The `decdn:` URI to price: `decdn:<hash>[/<path>][?ns=<id>]`.
    #[arg(value_name = "URI")]
    pub uri: String,

    /// The output directory of the `decdn get` this quote prices: where a
    /// bundle's cached manifest is read from, and cached to when the quote
    /// fetches it. The default is `decdn get`'s: `./<hash>/` for a whole
    /// bundle, `.` for one entry.
    #[arg(short = 'o', long, value_name = "DIR")]
    pub output: Option<PathBuf>,

    /// Print the quote as one line of JSON, with every amount in
    /// micro-USDC.
    #[arg(long)]
    pub json: bool,

    /// With no open pool, let the manifest fetch open one. Opening escrows
    /// the working deposit (`--working-deposit-micro-usdc` or
    /// `blockchain.buyer_working_deposit_micro_usdc`, 10 USDC by default) and
    /// pays gas for the `openPool`, and for an approve when the allowance is
    /// short. Without the flag, a quote that must fetch a manifest and has no
    /// open pool stops before either. `decdn pool open` is the other way to
    /// fund a pool first. The flag has no effect with a capability
    /// (`--capability` or `--capability-file`), which opens no pool.
    #[arg(long)]
    pub open_pool: bool,

    /// Retrieval flags: peer discovery, chain coordinates, the keystore and
    /// pool flags that name the payer every open carries, the rate ceiling,
    /// and `--max-sources` and the proxy-warming flags, which set the order
    /// and the likely cost.
    #[command(flatten)]
    pub common: ClientFetchArgs,
}
