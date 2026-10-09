//! Secrets in server configuration: given inline or as a file, never shown
//! by `Debug`, and wiped from memory on drop.

use std::path::Path;

use zeroize::Zeroizing;

/// A secret string. `Debug` prints `<redacted>`.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(Zeroizing<String>);

impl Secret {
    /// Wrap `value`; it is wiped from memory when the secret is dropped.
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(Zeroizing::new(value))
    }

    /// The secret itself.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Resolve a secret given either inline (`value`) or as a file holding it
    /// (`file`, read with one trailing newline trimmed, as `echo` and most
    /// secret stores write it). `name` names the setting in errors. Exactly
    /// one source is expected; the CLI parser enforces that.
    ///
    /// # Errors
    ///
    /// Neither source is set, the file cannot be read, or the secret is
    /// empty.
    pub fn resolve(name: &str, value: Option<String>, file: Option<&Path>) -> anyhow::Result<Self> {
        let secret = match (value, file) {
            (Some(v), _) => v,
            (None, Some(path)) => {
                let mut s = std::fs::read_to_string(path)
                    .map_err(|e| anyhow::anyhow!("{name}_FILE {}: {e}", path.display()))?;
                if s.ends_with('\n') {
                    s.pop();
                    if s.ends_with('\r') {
                        s.pop();
                    }
                }
                s
            }
            (None, None) => anyhow::bail!("{name} or {name}_FILE is required"),
        };
        anyhow::ensure!(!secret.is_empty(), "{name} is empty");
        Ok(Self::new(secret))
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

impl From<&str> for Secret {
    fn from(s: &str) -> Self {
        Self::new(s.to_owned())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
