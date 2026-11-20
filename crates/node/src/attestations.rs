//! The origin attestations this node holds (ADR 022 § Origin Attestation).
//!
//! A node serves and publishes a hash only while it holds a live, valid origin
//! attestation for it. It gets one in three ways:
//!
//! - **Relay.** The pull leg hands it the upstream's attestation, and
//!   [`Attestations::keep`] stores it once it verifies. The node forwards it
//!   in `StreamResponseExt.origin_attestation` when it serves the hash, and
//!   puts it in each record it publishes.
//! - **Origin.** A node whose operator holds an origin seat signs its own for
//!   a hash its own backend holds ([`Attestations::attest_own`]), in the
//!   lowest namespace it holds a seat in, with its configured lifetime.
//! - **Refresh.** From half its lifetime on, plus a per-hash jitter, a held
//!   attestation is due for refresh ([`Attestations::refresh_due`]). The
//!   refresh task replaces it with a fresher one from the DHT or from a seated
//!   origin (`crate::node_origin::attestation_refresh`).
//!
//! The store keeps one attestation per hash: the valid one that expires last.
//! Held attestations persist under `<data_dir>/attestations/`, one file per
//! hash, so a restarted node serves and republishes what it holds without
//! pulling it again. A file that does not decode is skipped at load.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError, RwLock};

use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use decdn_protocol::OriginAttestation;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::dht::AttestationVerifier;
use crate::handlers::timestamp_skew::now_us;

/// Directory under the node's data dir that holds the attestation files.
pub const ATTESTATIONS_DIR: &str = "attestations";

/// How long a refresh that found no fresher attestation waits before it
/// tries again, in microseconds (10 minutes).
pub const REFRESH_RETRY_US: u64 = 600_000_000;

/// This node's own origin identity, for self-signed attestations.
#[derive(Debug, Clone)]
pub struct SelfOrigin {
    /// The operator key that signs attestations.
    pub signer: Arc<PrivateKeySigner>,
    /// The lifetime this origin signs into its attestations, in seconds.
    pub ttl_sec: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Held {
    hash: [u8; 32],
    attestation: OriginAttestation,
    signer: Address,
}

/// A held attestation whose refresh is due.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshDue {
    /// The hash the attestation is for.
    pub hash: [u8; 32],
    /// The attestation the node holds now.
    pub attestation: OriginAttestation,
}

/// The attestations this node holds, keyed by hash.
#[derive(Debug)]
pub struct Attestations {
    verifier: AttestationVerifier,
    held: RwLock<HashMap<[u8; 32], Held>>,
    /// When a refresh that failed may run again, per hash. Memory only: a
    /// restarted node tries each due hash once at once.
    retry_at: RwLock<HashMap<[u8; 32], u64>>,
    /// Seeds the per-hash refresh jitter. Random per process, so holders of
    /// one hash spread their refreshes over the jitter window.
    jitter: std::collections::hash_map::RandomState,
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
        Self::with(verifier, held, dir, origin)
    }

    /// A memory-only store, for tests and for a node without a data dir.
    #[must_use]
    pub fn in_memory(verifier: AttestationVerifier, origin: Option<SelfOrigin>) -> Self {
        Self::with(verifier, HashMap::new(), None, origin)
    }

    fn with(
        verifier: AttestationVerifier,
        held: HashMap<[u8; 32], Held>,
        dir: Option<PathBuf>,
        origin: Option<SelfOrigin>,
    ) -> Self {
        Self {
            verifier,
            held: RwLock::new(held),
            retry_at: RwLock::new(HashMap::new()),
            jitter: std::collections::hash_map::RandomState::new(),
            dir,
            origin,
        }
    }

    /// The verifier attestations are checked with.
    #[must_use]
    pub const fn verifier(&self) -> &AttestationVerifier {
        &self.verifier
    }

    /// Whether this node's operator may sign its own attestations: it is
    /// configured to, and it holds an origin seat.
    #[must_use]
    pub fn can_attest_own(&self) -> bool {
        self.origin.as_ref().is_some_and(|origin| {
            self.verifier
                .attesting_namespace(&origin.signer.address())
                .is_some()
        })
    }

    /// Keep `attestation` for `hash` when it is valid now. Returns whether it
    /// is valid. It replaces the held one when that one is no longer valid or
    /// expires earlier, so a relay moves to the freshest attestation it is
    /// offered and off an origin that lost its seat.
    pub fn keep(&self, hash: [u8; 32], attestation: OriginAttestation) -> bool {
        let now = now_us();
        let Some(signer) = self.verifier.verify_at(hash, &attestation, now) else {
            return false;
        };
        let replaces = self
            .held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&hash)
            .is_none_or(|held| {
                held.attestation.expires_at_us() < attestation.expires_at_us()
                    || self
                        .verifier
                        .verify_at(hash, &held.attestation, now)
                        .is_none()
            });
        if replaces {
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
            self.retry_at
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&hash);
        }
        true
    }

    /// Keep the attestation an upstream offered for `hash` on a pull leg
    /// (`UpstreamPullHeader::origin_attestation`). Returns whether the node
    /// holds a live attestation for `hash` afterwards: a pull from an
    /// upstream that offered none, or an invalid one, leaves the node unable
    /// to serve what it pulls unless it already held one.
    pub fn keep_offered(&self, hash: [u8; 32], offered: Option<&OriginAttestation>) -> bool {
        if offered.is_some_and(|attestation| self.keep(hash, attestation.clone())) {
            return true;
        }
        self.live(&hash).is_some()
    }

    /// The live, valid attestation held for `hash`, to serve, forward and
    /// publish with. `None` means the node neither serves nor publishes
    /// `hash`.
    #[must_use]
    pub fn live(&self, hash: &[u8; 32]) -> Option<OriginAttestation> {
        self.held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(hash)
            .filter(|held| self.verifier.verify(*hash, &held.attestation).is_some())
            .map(|held| held.attestation.clone())
    }

    /// The attestation held for `hash`, valid or not.
    #[must_use]
    pub fn held(&self, hash: &[u8; 32]) -> Option<OriginAttestation> {
        self.held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(hash)
            .map(|h| h.attestation.clone())
    }

    /// Sign and keep this node's own attestation for `hash`, which its own
    /// backend holds. The caller checks the backend. A held attestation still
    /// short of its half-life is returned as it is, so a busy origin signs
    /// once per half-life. `None` when the node holds no origin seat.
    pub fn attest_own(&self, hash: [u8; 32]) -> Option<OriginAttestation> {
        let now = now_us();
        if let Some(live) = self.live(&hash)
            && now < live.half_life_at_us()
        {
            return Some(live);
        }
        let origin = self.origin.as_ref()?;
        let namespace = self
            .verifier
            .attesting_namespace(&origin.signer.address())?;
        match crate::dht::attestation::sign_attestation(
            origin.signer.as_ref(),
            self.verifier.domain(),
            hash,
            namespace,
            now,
            origin.ttl_sec,
        ) {
            Ok(attestation) => self.keep(hash, attestation.clone()).then_some(attestation),
            Err(err) => {
                warn!(error = %err, "origin attestation: self-sign failed");
                None
            }
        }
    }

    /// The held attestations whose refresh is due at `now_us`: half their
    /// lifetime has passed, plus a per-hash jitter of up to a quarter of the
    /// lifetime, and no failed refresh asked to wait longer.
    #[must_use]
    pub fn refresh_due(&self, now_us: u64) -> Vec<RefreshDue> {
        let retry_at = self.retry_at.read().unwrap_or_else(PoisonError::into_inner);
        self.held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter(|held| {
                let due = held
                    .attestation
                    .half_life_at_us()
                    .saturating_add(self.jitter_us(&held.hash, &held.attestation));
                now_us >= due && retry_at.get(&held.hash).is_none_or(|at| now_us >= *at)
            })
            .map(|held| RefreshDue {
                hash: held.hash,
                attestation: held.attestation.clone(),
            })
            .collect()
    }

    /// Hold off the next refresh of `hash` for [`REFRESH_RETRY_US`] after a
    /// refresh found nothing fresher.
    pub fn defer_refresh(&self, hash: [u8; 32], now_us: u64) {
        self.retry_at
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(hash, now_us.saturating_add(REFRESH_RETRY_US));
    }

    /// This hash's refresh jitter: up to a quarter of the attestation's
    /// lifetime, fixed per hash for this process.
    fn jitter_us(&self, hash: &[u8; 32], attestation: &OriginAttestation) -> u64 {
        let window = u64::from(attestation.ttl_sec)
            .saturating_mul(1_000_000)
            .saturating_div(4);
        self.jitter
            .hash_one(hash)
            .checked_rem(window.saturating_add(1))
            .unwrap_or(0)
    }

    /// Drop the attestation for `hash`, once the node no longer holds the
    /// blob or the attestation expired with no fresher one found.
    pub fn forget(&self, hash: &[u8; 32]) {
        let removed = self
            .held
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(hash);
        self.retry_at
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
