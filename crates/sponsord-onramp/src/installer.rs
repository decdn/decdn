//! The installer scripts served at `GET /decdn.sh` (macOS/Linux) and
//! `GET /decdn.ps1` (Windows). Each embeds its `assets/` file at compile time
//! and is rendered once at startup: the onramp URL, the pinned `decdn`
//! release and the download profile are baked in, so the user sets nothing
//! themselves.

use alloy_primitives::Address;

use crate::config::OnrampConfig;

const DECDN_SH_TEMPLATE: &str = include_str!("../assets/decdn.sh");
const DECDN_PS1_TEMPLATE: &str = include_str!("../assets/decdn.ps1");

/// The chains and contracts a download runs `decdn` against, baked into the
/// installers: the governance chain, where `decdn` finds nodes and checks
/// their signatures, and the payment chain of the pool that pays them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Profile {
    /// EIP-155 id of the governance chain.
    pub governance_chain_id: u64,
    /// JSON-RPC endpoint `decdn` reads the governance chain through.
    pub governance_rpc_url: String,
    /// The `CapacityBond` on the governance chain, for node discovery.
    pub capacity_bond: Address,
    /// The `SlashJudge` on the governance chain, when the onramp names one.
    pub slash_judge: Option<Address>,
    /// EIP-155 id of the payment chain the pool is on, from the daemon.
    pub pool_chain_id: u64,
    /// JSON-RPC endpoint `decdn` reads the payment chain through.
    pub pool_rpc_url: String,
    /// The `PaymentPool` the capabilities are signed against, from the
    /// daemon.
    pub payment_pool: Address,
}

/// Both installers, rendered.
#[derive(Clone, Debug)]
pub struct Installers {
    /// The POSIX shell installer, served at `GET /decdn.sh`.
    pub sh: String,
    /// The PowerShell installer, served at `GET /decdn.ps1`.
    pub ps1: String,
}

impl Installers {
    /// Render both templates from `cfg` and `profile`. The release's version
    /// is passed alongside its tag rather than cut out of it by the scripts,
    /// so the scripts hold no assumption about how decdn tags. Every
    /// substituted value was checked at startup to hold nothing a quoted
    /// shell or PowerShell string could misread (`config::script_safe_url`,
    /// `ReleasePin::new`), or is a number or a hex address. A missing
    /// `SlashJudge` renders as an empty string.
    #[must_use]
    pub fn render(cfg: &OnrampConfig, profile: &Profile) -> Self {
        let slash_judge = profile
            .slash_judge
            .map(|a| a.to_string())
            .unwrap_or_default();
        let render = |template: &str| {
            template
                .replace("{{ONRAMP_URL}}", &cfg.public_url)
                .replace("{{RELEASES_BASE}}", &cfg.releases_base)
                .replace("{{DECDN_RELEASE}}", &cfg.decdn_release.tag)
                .replace("{{DECDN_VERSION}}", &cfg.decdn_release.version)
                .replace("{{DECDN_SUMS_SHA256}}", &cfg.decdn_release.sums_sha256)
                .replace(
                    "{{GOVERNANCE_CHAIN_ID}}",
                    &profile.governance_chain_id.to_string(),
                )
                .replace("{{GOVERNANCE_RPC_URL}}", &profile.governance_rpc_url)
                .replace("{{CAPACITY_BOND}}", &profile.capacity_bond.to_string())
                .replace("{{SLASH_JUDGE}}", &slash_judge)
                .replace("{{POOL_CHAIN_ID}}", &profile.pool_chain_id.to_string())
                .replace("{{POOL_RPC_URL}}", &profile.pool_rpc_url)
                .replace("{{PAYMENT_POOL}}", &profile.payment_pool.to_string())
        };
        Self {
            sh: render(DECDN_SH_TEMPLATE),
            ps1: render(DECDN_PS1_TEMPLATE),
        }
    }
}
