//! The installer scripts served at `GET /decdn.sh` (macOS/Linux) and
//! `GET /decdn.ps1` (Windows). Each embeds its `assets/` file at compile time
//! and is rendered once at startup: the onramp URL, the pinned `decdn`
//! release and the download profile are baked in, so the user sets nothing
//! themselves.

use alloy_primitives::Address;

use crate::config::OnrampConfig;

const DECDN_SH_TEMPLATE: &str = include_str!("../assets/decdn.sh");
const DECDN_PS1_TEMPLATE: &str = include_str!("../assets/decdn.ps1");

/// The chain and contracts a download runs `decdn` against, baked into the
/// installers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Profile {
    /// EIP-155 id of the chain the contracts are on, from the daemon.
    pub chain_id: u64,
    /// JSON-RPC endpoint `decdn` reads the chain through.
    pub rpc_url: String,
    /// The `PaymentPool` the capabilities are signed against, from the
    /// daemon.
    pub payment_pool: Address,
    /// The `CapacityBond`, for node discovery.
    pub capacity_bond: Address,
    /// The `SlashJudge`, when the onramp names one.
    pub slash_judge: Option<Address>,
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
                .replace("{{CHAIN_ID}}", &profile.chain_id.to_string())
                .replace("{{RPC_URL}}", &profile.rpc_url)
                .replace("{{PAYMENT_POOL}}", &profile.payment_pool.to_string())
                .replace("{{CAPACITY_BOND}}", &profile.capacity_bond.to_string())
                .replace("{{SLASH_JUDGE}}", &slash_judge)
        };
        Self {
            sh: render(DECDN_SH_TEMPLATE),
            ps1: render(DECDN_PS1_TEMPLATE),
        }
    }
}
