//! Arguments for the `decdn setup` onboarding wizard (ADR 019 Phases 1–2).
//!
//! `setup` is thin orchestration over the existing primitives — `key-gen`,
//! `node bond`, `node register` — gated by pre-flight checks that catch the
//! mis-ordered-step / under-funded-wallet failure class ADR 019 § Context
//! calls out, *before* any transaction is submitted. It introduces no new
//! on-chain logic of its own (#933).

use clap::Args;

use super::node::ChainArgs;

/// Bond funding source for `decdn setup`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum PayBondWith {
    /// Operator already holds TOKEN (today's behavior).
    Token,
    /// Swap USDC→TOKEN for the exact bond top-up before bonding.
    Usdc,
}

/// `decdn setup --mbps <N> --region <CC>` — run the operator through Phase 1
/// pre-flight checks and Phase 2 on-chain setup, then print a go/no-go
/// readiness summary. The flattened [`ChainArgs`] supplies the same
/// `--rpc-url` / `--capacity-bond-address` / `--keystore` / `--dry-run` /
/// `--json` flags as `node bond` / `node register`.
#[derive(Args, Debug)]
pub struct SetupArgs {
    /// Declared serving capacity in Mbps. The TOKEN bond is read from the
    /// on-chain `bondRequired(mbps)` curve (same as `decdn node bond`); you do
    /// not pass a token amount. Must fall within the governable
    /// `[minCapacityMbps, maxCapacityMbps]` band.
    #[arg(long, value_name = "MBPS")]
    pub mbps: u64,

    /// ISO 3166-1 alpha-2 country code submitted as the on-chain `regionHint`
    /// (ADR 030) at registration — e.g. `US`, `DE`. Self-reported.
    #[arg(long, value_name = "CODE")]
    pub region: String,

    /// QUIC multiaddr to register, e.g. `/ip4/203.0.113.10/udp/4433/quic-v1`.
    /// Repeatable. Omitting it registers an empty set and relies on gossip /
    /// iroh discovery for reachability.
    #[arg(long = "multiaddr", value_name = "MA")]
    pub multiaddrs: Vec<String>,

    /// Skip the interactive confirmation of the bond amount and submit without
    /// prompting (for non-interactive / CI use). The keystore password still
    /// comes from `DECDN_KEYSTORE_PASSWORD` / `--keystore-password-file`.
    #[arg(long)]
    pub yes: bool,

    /// Accept the network's current operator terms non-interactively (ADR 019
    /// § Terms Acceptance). Separate from `--yes` (which only skips the bond
    /// confirmation): terms acceptance is always its own explicit act. On a
    /// terminal the terms are shown and confirmed interactively instead.
    #[arg(long = "accept-terms")]
    pub accept_terms: bool,

    /// How to fund the capacity bond. `token` (default) requires you already
    /// hold TOKEN. `usdc` exact-out swaps USDC→TOKEN for the exact bond top-up
    /// on the configured DEX venue before bonding (requires the swap-venue
    /// config below).
    #[arg(long = "pay-bond-with", value_enum, default_value_t = PayBondWith::Token)]
    pub pay_bond_with: PayBondWith,

    /// Max slippage for the USDC→TOKEN swap, in basis points (1% = 100).
    /// Bounds `amountInMaximum`. Ignored unless `--pay-bond-with usdc`.
    #[arg(long = "max-slippage-bps", value_name = "BPS", default_value_t = 300)]
    pub max_slippage_bps: u16,

    #[command(flatten)]
    pub chain: ChainArgs,
}
