//! CLI argument parsing for `decdn publish` — the origin-publisher control
//! plane (issue #1029, ADR 002 / ADR 011). Three on-chain writes:
//! `namespace create` (`PublisherRegistry.createNamespace`), `claim`
//! (`claimContent`), and `assign` (`OriginAssignment.proposeAssignment`,
//! propose-only; governance activates separately).

use std::path::PathBuf;

use clap::{Args, Subcommand};

/// `decdn publish <subcommand>`.
#[derive(Args, Debug)]
pub struct PublishArgs {
    /// Publisher control-plane subcommand.
    #[command(subcommand)]
    pub command: PublishCommand,
}

/// Publisher control-plane operations.
#[derive(Subcommand, Debug)]
pub enum PublishCommand {
    /// Create a new namespace owned by the signer (`createNamespace`).
    Namespace(NamespaceArgs),
    /// Claim a BLAKE3 content hash into a namespace (`claimContent`).
    Claim(ClaimArgs),
    /// Propose an authorized-origin operator set for a namespace
    /// (`proposeAssignment`). Propose-only: inert until the DAO ratifies.
    Assign(AssignArgs),
}

/// `decdn publish namespace <subcommand>` (nested to match the issue's
/// `namespace create` phrasing and leave room for future `namespace transfer`).
#[derive(Args, Debug)]
pub struct NamespaceArgs {
    /// Namespace lifecycle subcommand.
    #[command(subcommand)]
    pub command: NamespaceCommand,
}

/// Namespace lifecycle operations.
#[derive(Subcommand, Debug)]
pub enum NamespaceCommand {
    /// Mint a fresh namespace; prints its id.
    Create(NamespaceCreateArgs),
}

/// `decdn publish namespace create` flags.
#[derive(Args, Debug)]
pub struct NamespaceCreateArgs {
    #[command(flatten)]
    pub chain: PublishChainArgs,
}

/// `decdn publish claim <hash> --namespace <id>` flags.
#[derive(Args, Debug)]
pub struct ClaimArgs {
    /// BLAKE3 content hash to claim. Accepts `0x…`, `b3:…`, or bare 64-hex.
    #[arg(value_name = "HASH")]
    pub hash: String,

    /// Namespace id to claim into. Must be owned by the signer.
    #[arg(long, value_name = "ID")]
    pub namespace: u64,

    #[command(flatten)]
    pub chain: PublishChainArgs,
}

/// `decdn publish assign <namespace> <ops…>` flags.
#[derive(Args, Debug)]
pub struct AssignArgs {
    /// Namespace id whose authorized-origin set is being proposed.
    #[arg(value_name = "NAMESPACE")]
    pub namespace: u64,

    /// Operator Ethereum addresses to authorize. Each must be an active
    /// bonded node. At least one; order-insensitive; no duplicates.
    #[arg(value_name = "OPERATORS", required = true, num_args = 1..)]
    pub operators: Vec<String>,

    #[command(flatten)]
    pub chain: PublishChainArgs,
}

/// Chain coordinates shared by the publish subcommands. Mirrors `ChainArgs`
/// but targets the publisher contracts (`PublisherRegistry`,
/// `OriginAssignment`) instead of `CapacityBond`.
#[derive(Args, Debug)]
pub struct PublishChainArgs {
    /// Path to the TOML config supplying `[blockchain]` / `[identity]` fields.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// JSON-RPC endpoint URL. Overrides `blockchain.rpc_url`.
    #[arg(long, value_name = "URL")]
    pub rpc_url: Option<String>,

    /// Expected chain id of `--rpc-url`; the submit fails if the RPC reports a
    /// different one. Overrides `blockchain.chain_id`; default Arbitrum Sepolia.
    #[arg(long, value_name = "ID")]
    pub chain_id: Option<u64>,

    /// `PublisherRegistry` address (namespace/claim). Overrides
    /// `blockchain.publisher_registry_address`.
    #[arg(long, value_name = "ADDR")]
    pub publisher_registry_address: Option<String>,

    /// `OriginAssignment` address (assign). Overrides
    /// `blockchain.origin_assignment_address`.
    #[arg(long, value_name = "ADDR")]
    pub origin_assignment_address: Option<String>,

    /// Ethereum keystore file. Overrides `blockchain.eth_keystore`;
    /// defaults to `<data_dir>/keystore.json`.
    #[arg(long, value_name = "PATH")]
    pub keystore: Option<PathBuf>,

    /// Data directory. Overrides `identity.data_dir`; defaults to `~/.decdn`.
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,

    /// File whose contents are the keystore password (after
    /// `DECDN_KEYSTORE_PASSWORD`, before an interactive prompt).
    #[arg(long, value_name = "PATH", env = "DECDN_KEYSTORE_PASSWORD_FILE")]
    pub keystore_password_file: Option<PathBuf>,

    /// Build and print what would be submitted without sending a transaction.
    #[arg(long)]
    pub dry_run: bool,

    /// Emit the result as JSON instead of `key=value` lines.
    #[arg(long)]
    pub json: bool,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use clap::Parser;

    use crate::cli::Cli;

    #[test]
    fn claim_requires_namespace_and_parses_hash() {
        let cli = Cli::try_parse_from(["decdn", "publish", "claim", "0xdead", "--namespace", "7"])
            .unwrap();
        // Structural assertion: the command tree resolves to publish → claim.
        assert!(format!("{:?}", cli.command).contains("Claim"));
    }

    #[test]
    fn claim_without_namespace_errors() {
        let err = Cli::try_parse_from(["decdn", "publish", "claim", "0xdead"]);
        assert!(err.is_err());
    }

    #[test]
    fn assign_needs_at_least_one_operator() {
        let err = Cli::try_parse_from(["decdn", "publish", "assign", "7"]);
        assert!(err.is_err());
    }
}
