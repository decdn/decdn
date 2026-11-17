//! Arguments for the `decdn probe` subcommand.

use clap::Args;

use super::fetch::ClientFetchArgs;

/// Ask a running deCDN node about one blob: open one `cdn/client/v1` stream,
/// print the node's signed answer, and cancel the stream before any voucher.
///
/// The open is the one a pull's discovery sends. It names your pool, your
/// lane key and its identity binding, and asks for the whole blob with no
/// pull-through, so a node that misses the blob answers a signed `NotFound`
/// at once and fetches nothing. The command unlocks the keystore and names
/// the pool as `decdn get` does, and pays nothing and sends no transaction.
/// The answer's signature must recover to `--provider-address`, its echoed
/// fields must match the request, and a size it states must carry its proof.
#[derive(Args, Debug)]
pub struct ProbeArgs {
    /// BLAKE3 hash of the blob to ask about: 64 hex chars (optional `0x`
    /// prefix), the same form `cache.pinned_hashes` accepts.
    #[arg(long, value_name = "HASH")]
    pub hash: String,

    /// Bound on the dial, and again on the exchange, in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5000)]
    pub open_timeout_ms: u64,

    /// Print the answer as a single line of JSON instead of a pretty block.
    #[arg(long)]
    pub json: bool,

    /// The node and the payer: `--node-id` with `--provider-address` names
    /// the node (both required), `--addr` and `--relay-url` reach it, and the
    /// chain, keystore and pool flags name the payer as for `decdn get`. The
    /// discovery, rate and transfer flags have no effect here.
    #[command(flatten)]
    pub common: ClientFetchArgs,
}
