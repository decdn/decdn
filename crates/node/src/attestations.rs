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
//!   refresh task replaces it with a fresher one: its own signature when the
//!   node is a seated origin whose backend holds the hash, else one from the
//!   DHT or from a seated origin (`crate::node_origin::attestation_refresh`).
//!   An attestation that expired stays held, unserved, and due for refresh
//!   while the node holds the blob, so the hash serves again once its origins
//!   return.
//! - **Wanted.** A node asked to serve a hash it holds with no attestation
//!   at all marks the hash wanted ([`Attestations::want`]). The refresh task
//!   looks the hash up in the DHT and keeps the freshest valid attestation it
//!   finds, so a blob whose attestation was lost serves again.
//!
//! The store keeps one attestation per hash: the valid one that expires last.
//! It decides and replaces under one lock, so a concurrent offer of an older
//! attestation never displaces a newer one. Held attestations persist under
//! `<data_dir>/attestations/`, one file per hash, so a restarted node serves
//! and republishes what it holds without pulling it again. A file that does
//! not decode, or whose signer does not recover from its signature, is skipped
//! at load.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

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
/// tries again, in microseconds (10 minutes). A wanted hash waits as long
/// between lookups.
pub const REFRESH_RETRY_US: u64 = 600_000_000;

/// The most wanted hashes the store tracks. A request for a further hash is
/// dropped until one of them is attested or forgotten.
pub const MAX_WANTED: usize = 4_096;

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

/// A held attestation with its refresh state. The refresh state lives in
/// memory only: a restarted node retries every due hash on its first tick.
#[derive(Debug, Clone)]
struct Entry {
    held: Held,
    /// When a refresh that failed may run again.
    retry_at: Option<u64>,
    /// Whether a failed refresh already found this attestation expired.
    lapsed: bool,
}

impl Entry {
    const fn new(held: Held) -> Self {
        Self {
            held,
            retry_at: None,
            lapsed: false,
        }
    }
}

/// What a failed refresh left held for its hash
/// ([`Attestations::refresh_failed`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshFailed {
    /// The held attestation is still live. Its refresh retries later.
    Live,
    /// The held attestation expired. The node does not serve the hash, and the
    /// refresh retries later. `first` is set on the first failed refresh that
    /// found it expired.
    Lapsed {
        /// The first failed refresh since the attestation expired.
        first: bool,
    },
    /// The node holds no attestation for the hash any more.
    Gone,
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
    held: RwLock<HashMap<[u8; 32], Entry>>,
    /// Serializes the file writes, so the file of a hash always ends at the
    /// entry held in memory ([`Attestations::sync_file`]).
    persist: Mutex<()>,
    /// Hashes the node holds with no attestation, each with the time its next
    /// DHT lookup may run. Memory only.
    wanted: Mutex<HashMap<[u8; 32], u64>>,
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
        let held = dir
            .as_deref()
            .map(|dir| load_dir(dir, &verifier))
            .unwrap_or_default();
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
            held: RwLock::new(held.into_iter().map(|(k, h)| (k, Entry::new(h))).collect()),
            persist: Mutex::new(()),
            wanted: Mutex::new(HashMap::new()),
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
        let replaced = {
            let mut held = self.held.write().unwrap_or_else(PoisonError::into_inner);
            let replaces = held.get(&hash).is_none_or(|entry| {
                entry.held.attestation.expires_at_us() < attestation.expires_at_us()
                    || !self
                        .verifier
                        .still_valid(&entry.held.attestation, &entry.held.signer, now)
            });
            if replaces {
                held.insert(
                    hash,
                    Entry::new(Held {
                        hash,
                        attestation,
                        signer,
                    }),
                );
            }
            replaces
        };
        if replaced {
            self.sync_file(&hash);
        }
        self.wanted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&hash);
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
    /// `hash`. The signer recovered once, when the store kept the
    /// attestation, so this reads no signature and is cheap enough for every
    /// frame a delivery sends.
    #[must_use]
    pub fn live(&self, hash: &[u8; 32]) -> Option<OriginAttestation> {
        let now = now_us();
        self.held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(hash)
            .filter(|entry| {
                self.verifier
                    .still_valid(&entry.held.attestation, &entry.held.signer, now)
            })
            .map(|entry| entry.held.attestation.clone())
    }

    /// The attestation held for `hash`, valid or not.
    #[must_use]
    pub fn held(&self, hash: &[u8; 32]) -> Option<OriginAttestation> {
        self.held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(hash)
            .map(|entry| entry.held.attestation.clone())
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
    /// lifetime, and no failed refresh asked to wait longer. The live ones
    /// come first, the soonest to expire first, so expired ones the node
    /// keeps retrying never hold back a refresh that keeps a hash served.
    #[must_use]
    pub fn refresh_due(&self, now_us: u64) -> Vec<RefreshDue> {
        let mut due: Vec<RefreshDue> = self
            .held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter(|entry| {
                let held = &entry.held;
                let due = held
                    .attestation
                    .half_life_at_us()
                    .saturating_add(self.jitter_us(&held.hash, &held.attestation));
                now_us >= due && entry.retry_at.is_none_or(|at| now_us >= at)
            })
            .map(|entry| RefreshDue {
                hash: entry.held.hash,
                attestation: entry.held.attestation.clone(),
            })
            .collect();
        due.sort_by_key(|d| {
            (
                !d.attestation.is_live_at(now_us),
                d.attestation.expires_at_us(),
            )
        });
        due
    }

    /// Record a refresh of `hash` that found nothing fresher at `now_us`: hold
    /// off its next try for [`REFRESH_RETRY_US`], and report what the node
    /// holds now. It reads the entry held now, not the one the refresh
    /// started from, so a fresh attestation kept while the refresh ran counts.
    pub fn refresh_failed(&self, hash: [u8; 32], now_us: u64) -> RefreshFailed {
        let mut held = self.held.write().unwrap_or_else(PoisonError::into_inner);
        let Some(entry) = held.get_mut(&hash) else {
            return RefreshFailed::Gone;
        };
        entry.retry_at = Some(now_us.saturating_add(REFRESH_RETRY_US));
        if entry.held.attestation.is_live_at(now_us) {
            return RefreshFailed::Live;
        }
        let first = !entry.lapsed;
        entry.lapsed = true;
        RefreshFailed::Lapsed { first }
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
    /// blob.
    pub fn forget(&self, hash: &[u8; 32]) {
        let removed = self
            .held
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(hash);
        self.unwant(hash);
        if removed.is_some() {
            self.sync_file(hash);
        }
    }

    /// Drop the attestation for `hash` when the one held now is still
    /// `attestation`, and say whether it did. A different attestation kept
    /// since the caller read `attestation` stays.
    pub fn forget_if_held(&self, hash: &[u8; 32], attestation: &OriginAttestation) -> bool {
        let removed = {
            let mut held = self.held.write().unwrap_or_else(PoisonError::into_inner);
            let unchanged = held
                .get(hash)
                .is_some_and(|entry| entry.held.attestation == *attestation);
            unchanged && held.remove(hash).is_some()
        };
        if removed {
            self.sync_file(hash);
        }
        removed
    }

    /// Mark `hash` wanted: the node holds it with no attestation at all, so
    /// the refresh task looks for one in the DHT. A held attestation, live or
    /// expired, has its own refresh, so this does nothing for it. Beyond
    /// [`MAX_WANTED`] hashes, a new one is dropped.
    pub fn want(&self, hash: [u8; 32]) {
        if self
            .held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&hash)
        {
            return;
        }
        let mut wanted = self.wanted.lock().unwrap_or_else(PoisonError::into_inner);
        if wanted.len() < MAX_WANTED {
            wanted.entry(hash).or_insert(0);
        }
    }

    /// Up to `limit` wanted hashes whose lookup may run at `now_us`. Each one
    /// returned waits [`REFRESH_RETRY_US`] before it is returned again.
    pub fn wanted_due(&self, now_us: u64, limit: usize) -> Vec<[u8; 32]> {
        let mut wanted = self.wanted.lock().unwrap_or_else(PoisonError::into_inner);
        let due: Vec<[u8; 32]> = wanted
            .iter()
            .filter(|(_, at)| now_us >= **at)
            .map(|(hash, _)| *hash)
            .take(limit)
            .collect();
        for hash in &due {
            wanted.insert(*hash, now_us.saturating_add(REFRESH_RETRY_US));
        }
        due
    }

    /// Stop wanting `hash`, once the node no longer holds the blob.
    pub fn unwant(&self, hash: &[u8; 32]) {
        self.wanted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(hash);
    }

    /// Bring the file of `hash` to the entry held in memory now: write it, or
    /// remove the file when the store holds none. The writes are serialized,
    /// and each reads the entry under the lock, so the last write leaves the
    /// file at the last change. A failed write leaves the attestation in
    /// memory for this process, with a warning.
    fn sync_file(&self, hash: &[u8; 32]) {
        let Some(dir) = &self.dir else {
            return;
        };
        let _serial = self.persist.lock().unwrap_or_else(PoisonError::into_inner);
        let current = self
            .held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(hash)
            .map(|entry| entry.held.clone());
        let path = dir.join(file_name(hash));
        let Some(entry) = current else {
            remove_file_logged(&path);
            return;
        };
        let result = postcard::to_allocvec(&entry)
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

/// Remove `path`, warning on any failure but a missing file. A file left
/// behind would bring a dropped attestation back at the next start.
fn remove_file_logged(path: &Path) {
    if let Err(err) = std::fs::remove_file(path)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        warn!(
            error = %err,
            path = %path.display(),
            "origin attestation: cannot remove the file of a dropped attestation"
        );
    }
}

/// Load the attestation files under `dir`. Only a file named for its hash
/// loads, and only when it decodes, names that hash, and its signer recovers
/// from its signature. A `.tmp` file a crash left between write and rename,
/// and a file that fails those checks, is removed, with one warning for the
/// lot.
fn load_dir(dir: &Path, verifier: &AttestationVerifier) -> HashMap<[u8; 32], Held> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) => {
            warn!(
                error = %err,
                dir = %dir.display(),
                "origin attestations: cannot read the store directory; starting empty"
            );
            return HashMap::new();
        }
    };
    let mut held = HashMap::new();
    let mut removed = 0usize;
    for path in entries.filter_map(Result::ok).map(|e| e.path()) {
        let loaded = path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| name.len() == 64 && name.bytes().all(|b| b.is_ascii_hexdigit()))
            .and_then(|name| {
                let bytes = std::fs::read(&path).ok()?;
                let h = postcard::from_bytes::<Held>(&bytes).ok()?;
                (file_name(&h.hash) == name.to_ascii_lowercase()
                    && verifier.recover(h.hash, &h.attestation) == Some(h.signer))
                .then_some(h)
            });
        if let Some(h) = loaded {
            held.insert(h.hash, h);
        } else {
            removed = removed.saturating_add(1);
            remove_file_logged(&path);
        }
    }
    if removed > 0 {
        warn!(
            removed,
            dir = %dir.display(),
            "origin attestations: removed files that did not load (a leftover temporary file, an old format, or a signer that does not recover)"
        );
    }
    held
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
