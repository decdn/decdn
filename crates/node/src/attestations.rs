//! The origin attestations this node holds (ADR 022 § Origin Attestation).
//!
//! A node publishes a DHT record for a hash only with a valid origin
//! attestation for it. It gets one of two ways:
//!
//! - **Relay.** The pull leg hands it the upstream's attestation, and
//!   [`Attestations::keep`] stores it once it verifies. The node forwards it
//!   in `StreamResponseExt.origin_attestation` when it serves the hash, and
//!   puts it in each record it publishes.
//! - **Origin.** A node whose operator holds an origin seat signs its own
//!   ([`Attestations::for_publish`]) with its quoted rate, the blob size and
//!   its clock, then keeps it like a relayed one.
//!
//! Held attestations persist under `<data_dir>/attestations/`, one file per
//! hash, so a restarted relay republishes what it holds without pulling it
//! again. A file that does not decode is skipped at load.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError, RwLock};

use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use decdn_protocol::OriginAttestation;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::dht::AttestationVerifier;

/// Directory under the node's data dir that holds the attestation files.
pub const ATTESTATIONS_DIR: &str = "attestations";

/// This node's own origin identity, for self-signed attestations.
#[derive(Debug, Clone)]
pub struct SelfOrigin {
    /// The operator key that signs `StreamResponse`s.
    pub signer: Arc<PrivateKeySigner>,
    /// The node's quoted rate, signed into the attestation like any response.
    pub rate_per_mb: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Held {
    hash: [u8; 32],
    attestation: OriginAttestation,
    signer: Address,
}

/// The attestations this node holds, keyed by hash.
#[derive(Debug)]
pub struct Attestations {
    verifier: AttestationVerifier,
    held: RwLock<HashMap<[u8; 32], Held>>,
    dir: Option<PathBuf>,
    origin: Option<SelfOrigin>,
}

impl Attestations {
    /// Open the store under `data_dir`, loading every attestation file. A
    /// directory that cannot be created leaves the store memory-only, with a
    /// warning.
    #[must_use]
    pub fn open(
        verifier: AttestationVerifier,
        data_dir: &Path,
        origin: Option<SelfOrigin>,
    ) -> Self {
        let dir = data_dir.join(ATTESTATIONS_DIR);
        let dir = match std::fs::create_dir_all(&dir) {
            Ok(()) => Some(dir),
            Err(err) => {
                warn!(
                    error = %err,
                    dir = %dir.display(),
                    "origin attestations: cannot create the store directory; holding them in memory only"
                );
                None
            }
        };
        let held = dir.as_deref().map(load_dir).unwrap_or_default();
        Self {
            verifier,
            held: RwLock::new(held),
            dir,
            origin,
        }
    }

    /// A memory-only store, for tests and for a node without a data dir.
    #[must_use]
    pub fn in_memory(verifier: AttestationVerifier, origin: Option<SelfOrigin>) -> Self {
        Self {
            verifier,
            held: RwLock::new(HashMap::new()),
            dir: None,
            origin,
        }
    }

    /// The verifier attestations are checked with.
    #[must_use]
    pub const fn verifier(&self) -> &AttestationVerifier {
        &self.verifier
    }

    /// Keep `attestation` for `hash` when it is valid. Returns whether it was
    /// kept. A valid attestation replaces one already held, so a relay moves
    /// off an origin that lost its seat as soon as it pulls from another.
    pub fn keep(&self, hash: [u8; 32], attestation: OriginAttestation) -> bool {
        if self
            .held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&hash)
            .is_some_and(|h| h.attestation == attestation)
        {
            return true;
        }
        let Some(signer) = self.verifier.verify(hash, &attestation) else {
            return false;
        };
        let entry = Held {
            hash,
            attestation,
            signer,
        };
        self.persist(&entry);
        self.held
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(hash, entry);
        true
    }

    /// Keep the attestation an upstream offered for `hash` on a pull leg
    /// (`UpstreamPullHeader::origin_attestation`), when it is valid.
    pub fn keep_offered(&self, hash: [u8; 32], offered: Option<&OriginAttestation>) {
        if let Some(attestation) = offered {
            self.keep(hash, attestation.clone());
        }
    }

    /// The attestation held for `hash`, to forward on a serve. Not
    /// re-checked: the receiving relay verifies it.
    #[must_use]
    pub fn held(&self, hash: &[u8; 32]) -> Option<OriginAttestation> {
        self.held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(hash)
            .map(|h| h.attestation.clone())
    }

    /// A valid attestation for `hash` to publish with: the held one while its
    /// signer still holds a seat, else a fresh self-signed one when this
    /// node's operator holds a seat. `None` means the node publishes no
    /// record for `hash`.
    #[must_use]
    pub fn for_publish(&self, hash: [u8; 32]) -> Option<OriginAttestation> {
        if let Some(held) = self
            .held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&hash)
            .filter(|h| self.verifier.admits(&h.signer))
        {
            return Some(held.attestation.clone());
        }
        let origin = self.origin.as_ref()?;
        if !self.verifier.admits(&origin.signer.address()) {
            return None;
        }
        let signed = crate::dht::attestation::sign_attestation(
            origin.signer.as_ref(),
            self.verifier.domain(),
            hash,
            origin.rate_per_mb,
            crate::handlers::timestamp_skew::now_us(),
        );
        match signed {
            Ok(attestation) => {
                self.keep(hash, attestation.clone());
                Some(attestation)
            }
            Err(err) => {
                warn!(error = %err, "origin attestation: self-sign failed");
                None
            }
        }
    }

    /// Drop the attestation for `hash`, once the node no longer holds it.
    pub fn forget(&self, hash: &[u8; 32]) {
        let removed = self
            .held
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(hash);
        if let (Some(_), Some(dir)) = (removed, &self.dir) {
            let _ = std::fs::remove_file(dir.join(file_name(hash)));
        }
    }

    /// Write `entry` to its file. A failed write leaves the attestation in
    /// memory for this process, with a warning.
    fn persist(&self, entry: &Held) {
        let Some(dir) = &self.dir else {
            return;
        };
        let path = dir.join(file_name(&entry.hash));
        let result = postcard::to_allocvec(entry)
            .map_err(std::io::Error::other)
            .and_then(|bytes| {
                let tmp = path.with_extension("tmp");
                std::fs::write(&tmp, bytes)?;
                std::fs::rename(&tmp, &path)
            });
        if let Err(err) = result {
            warn!(
                error = %err,
                path = %path.display(),
                "origin attestation: write failed; held in memory only"
            );
        }
    }
}

fn file_name(hash: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    hash.iter().fold(String::with_capacity(64), |mut name, b| {
        let _ = write!(name, "{b:02x}");
        name
    })
}

fn load_dir(dir: &Path) -> HashMap<[u8; 32], Held> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return HashMap::new();
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|e| std::fs::read(e.path()).ok())
        .filter_map(|bytes| postcard::from_bytes::<Held>(&bytes).ok())
        .map(|h| (h.hash, h))
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
