//! CLI argument parsing for `decdn publish` — the origin-publisher control
//! plane (issue #1029, ADR 002 / ADR 011). Three on-chain writes:
//! `namespace create` (`PublisherRegistry.createNamespace`), `claim`
//! (`claimContent`), and `assign` (`OriginAssignment.proposeAssignment`,
//! propose-only; governance activates separately).

use clap::{Args, Subcommand};

use super::common::CommonChainArgs;

/// Parse a namespace id, rejecting the reserved `0`. Namespace ids start at 1
/// (`PublisherRegistry` pre-increments from 0, and id 0 is the governance-only
/// default-open list), so `0` can never be owned by a publisher — failing fast
/// here saves the gas of a guaranteed-revert `claimContent` / `proposeAssignment`
/// transaction, matching the client-side duplicate-operator guard.
fn parse_namespace_id(s: &str) -> Result<u64, String> {
    let id: u64 = s
        .parse()
        .map_err(|_| format!("invalid namespace id {s:?}: expected a non-negative integer"))?;
    if id == 0 {
        return Err("namespace id must be >= 1 (0 is reserved)".to_string());
    }
    Ok(id)
}

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
    #[arg(long, value_name = "ID", value_parser = parse_namespace_id)]
    pub namespace: u64,

    #[command(flatten)]
    pub chain: PublishChainArgs,
}

/// `decdn publish assign <namespace> <ops…>` flags.
#[derive(Args, Debug)]
pub struct AssignArgs {
    /// Namespace id whose authorized-origin set is being proposed.
    #[arg(value_name = "NAMESPACE", value_parser = parse_namespace_id)]
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
/// `OriginAssignment`) instead of `CapacityBond`. The common coordinates live
/// in [`CommonChainArgs`]; this struct adds the two publisher-contract address
/// flags.
#[derive(Args, Debug)]
pub struct PublishChainArgs {
    #[command(flatten)]
    pub common: CommonChainArgs,

    /// `PublisherRegistry` address (namespace/claim). Overrides
    /// `blockchain.publisher_registry_address`.
    #[arg(long, value_name = "ADDR")]
    pub publisher_registry_address: Option<String>,

    /// `OriginAssignment` address (assign). Overrides
    /// `blockchain.origin_assignment_address`.
    #[arg(long, value_name = "ADDR")]
    pub origin_assignment_address: Option<String>,
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

    #[test]
    fn claim_rejects_reserved_namespace_zero() {
        let err = Cli::try_parse_from(["decdn", "publish", "claim", "0xdead", "--namespace", "0"])
            .unwrap_err()
            .to_string();
        assert!(err.contains("0 is reserved"), "{err}");
    }

    #[test]
    fn assign_rejects_reserved_namespace_zero() {
        let err = Cli::try_parse_from(["decdn", "publish", "assign", "0", "0xabc"])
            .unwrap_err()
            .to_string();
        assert!(err.contains("0 is reserved"), "{err}");
    }
}
