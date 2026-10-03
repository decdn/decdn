//! The installer scripts served at `GET /decdn.sh` (macOS/Linux) and
//! `GET /decdn.ps1` (Windows). Each embeds its `assets/` file at compile time
//! and is rendered once at startup: the onramp URL and the pinned releases
//! are baked in, so the user sets nothing themselves.

use crate::config::OnrampConfig;

const DECDN_SH_TEMPLATE: &str = include_str!("../assets/decdn.sh");
const DECDN_PS1_TEMPLATE: &str = include_str!("../assets/decdn.ps1");

/// Both installers, rendered.
#[derive(Clone, Debug)]
pub struct Installers {
    pub sh: String,
    pub ps1: String,
}

impl Installers {
    /// Render both templates from `cfg`. Every substituted value was checked
    /// at startup to hold nothing a quoted shell or PowerShell string could
    /// misread (`config::script_safe_url`, `ReleasePin::new`).
    #[must_use]
    pub fn render(cfg: &OnrampConfig) -> Self {
        let render = |template: &str| {
            template
                .replace("{{ONRAMP_URL}}", &cfg.public_url)
                .replace("{{RELEASES_BASE}}", &cfg.releases_base)
                .replace("{{DECDN_RELEASE}}", &cfg.decdn_release.tag)
                .replace("{{DECDN_SUMS_SHA256}}", &cfg.decdn_release.sums_sha256)
                .replace("{{CLI_RELEASE}}", &cfg.cli_release.tag)
                .replace("{{CLI_SUMS_SHA256}}", &cfg.cli_release.sums_sha256)
        };
        Self {
            sh: render(DECDN_SH_TEMPLATE),
            ps1: render(DECDN_PS1_TEMPLATE),
        }
    }
}
