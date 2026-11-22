//! Arguments for the `decdn probe` subcommand.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Args;

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
/// The answer names the `PaymentPool` contracts the node accepts. The chain,
/// keystore and data-dir flags resolve as for `decdn get`: flag >
/// `[blockchain]`/`[identity]` config > default.
#[derive(Args, Debug)]
pub struct ProbeArgs {
    /// Target node id (iroh `EndpointId`, z-base32).
    #[arg(long, value_name = "ID")]
    pub node_id: String,

    /// The node operator's Ethereum address (0x-prefixed hex): the answer's
    /// `operator_sig` must recover to it (ADR 005 § Signer binding).
    #[arg(long, value_name = "0xADDR")]
    pub provider_address: String,

    /// BLAKE3 hash of the blob to ask about: 64 hex chars (optional `0x`
    /// prefix), the same form `cache.pinned_hashes` accepts.
    #[arg(long, value_name = "HASH")]
    pub hash: String,

    /// Direct socket address of the target node (e.g. `127.0.0.1:4433`).
    #[arg(long, value_name = "HOST:PORT")]
    pub addr: Option<SocketAddr>,

    /// iroh relay URL to use for discovery-based resolution. Overrides
    /// `network.relay_urls` from config when set (#935).
    #[arg(long, value_name = "URL")]
    pub relay_url: Option<String>,

    /// Bound on the open, in milliseconds: on the dial, and again on the
    /// exchange.
    #[arg(long, value_name = "MS", default_value_t = 5000)]
    pub timeout_ms: u64,

    /// Print the answer as a single line of JSON instead of a pretty block.
    #[arg(long)]
    pub json: bool,

    /// The governance chain: its RPC endpoint and chain id (the domain the
    /// answer's signature verifies under).
    #[command(flatten)]
    pub governance: super::common::GovernanceChainArgs,

    /// The `PaymentPool` the open names. Without `--pool` the open names the
    /// first pool of the route `decdn get` pays through.
    #[command(flatten)]
    pub payment: super::common::PoolSelectArgs,

    /// `CapacityBond` contract address (0x hex): the domain the answer's
    /// signature verifies under, and the domain of the open's identity
    /// binding. Overrides `blockchain.capacity_bond_address`.
    #[arg(long, value_name = "0xADDR")]
    pub capacity_bond_address: Option<String>,

    /// Ethereum keystore file. Overrides `blockchain.eth_keystore`; defaults
    /// to `<data_dir>/keystore.json`.
    #[arg(long, value_name = "PATH")]
    pub keystore: Option<PathBuf>,

    /// File whose contents are the keystore password. Consulted after the
    /// `DECDN_KEYSTORE_PASSWORD` env var and before an interactive prompt on a
    /// TTY.
    #[arg(long, value_name = "PATH", env = "DECDN_KEYSTORE_PASSWORD_FILE")]
    pub keystore_password_file: Option<PathBuf>,

    /// Client data directory holding the buyer-pool store. Overrides
    /// `identity.data_dir`; defaults to `~/.decdn/client`.
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,

    /// Name a delegated pool: the `dcap1:` token `decdn pool assign` printed
    /// for this keystore's key. Mutually exclusive with `--capability-file`.
    #[arg(long, value_name = "TOKEN", conflicts_with = "capability_file")]
    pub capability: Option<String>,

    /// Read the `dcap1:` capability token from a file. Mutually exclusive
    /// with `--capability`.
    #[arg(long, value_name = "PATH", conflicts_with = "capability")]
    pub capability_file: Option<PathBuf>,
}
