//! Namespace pointers: the record that makes `<ns>.<host>` on a public
//! gateway name a bundle, and the store of them.
//!
//! A pointer is an owner-signed EIP-712 record over the `PublisherRegistry`
//! deployment the gateway runs against:
//!
//! ```text
//! Pointer(uint256 namespaceId,bytes32 root,uint64 seq)
//! ```
//!
//! The domain is `decdn-pointer` version `1` with the chain id and the
//! registry as `verifyingContract`, so a pointer signed for one deployment
//! means nothing on another: namespace ids repeat across chains and
//! redeploys. The gateway accepts a pointer whose signature recovers the
//! namespace's on-chain owner and whose `seq` is above every one it has
//! stored from that signer, so a replayed or reordered pointer changes
//! nothing, and a namespace that came back to a former owner cannot be rolled
//! back to a pointer they signed before. A pointer from another signer who
//! owns the namespace now replaces the held one whatever its own `seq`: a
//! namespace that changed hands is not held by its former owner's sequence.
//! `decdn publish point` signs one with the publisher's keystore and `PUT`s
//! it.
//!
//! Pointers live above the protocol ([ADR 002](https://github.com/decdn/decdn/blob/main/adr/002-content-addressing.md)):
//! nodes, the wire and the contracts never see them. The store is one JSON
//! file per namespace, `<dir>/<ns>.json`, holding the pointer, the signer it
//! recovered to, and the highest `seq` stored from each signer; written whole
//! through a temp file in the same directory, and read once at start.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Bytes, Signature, U256};
use alloy::signers::SignerSync;
use alloy::sol_types::{SolStruct as _, eip712_domain};
use anyhow::Context as _;

// The Rust struct name is the EIP-712 type name, so it is `Pointer` verbatim.
mod sol_types {
    alloy::sol! {
        #[allow(non_snake_case, missing_debug_implementations)]
        struct Pointer {
            uint256 namespaceId;
            bytes32 root;
            uint64 seq;
        }
    }
}

/// The EIP-712 domain a pointer is signed under, for the `PublisherRegistry`
/// at `registry` on `chain_id`.
pub(crate) const fn pointer_domain(chain_id: u64, registry: Address) -> Eip712Domain {
    eip712_domain! {
        name: "decdn-pointer",
        version: "1",
        chain_id: chain_id,
        verifying_contract: registry,
    }
}

/// A pointer with its owner signature, as `decdn publish point` sends it and
/// the gateway stores it: `{"namespace", "root", "seq", "signature"}`, the
/// ids as numbers, the root and the 65-byte `r‖s‖v` signature as `0x` hex.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct SignedPointer {
    /// The namespace the pointer names a bundle for.
    pub(crate) namespace: u64,
    /// The manifest hash of the bundle.
    pub(crate) root: B256,
    /// The pointer's place in the namespace's sequence. The gateway takes a
    /// pointer only above the one it holds.
    pub(crate) seq: u64,
    /// The owner's EIP-712 signature over the three fields.
    pub(crate) signature: Bytes,
}

/// A pointer signature that is no signature: the wrong length, an invalid
/// recovery id, or a non-canonical high `s`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MalformedSignature;

impl std::fmt::Display for MalformedSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the pointer signature is malformed")
    }
}

impl std::error::Error for MalformedSignature {}

fn signing_hash(namespace: u64, root: B256, seq: u64, domain: &Eip712Domain) -> B256 {
    sol_types::Pointer {
        namespaceId: U256::from(namespace),
        root,
        seq,
    }
    .eip712_signing_hash(domain)
}

impl SignedPointer {
    /// Sign a pointer from `namespace` to the bundle whose manifest hash is
    /// `root`, at `seq`, with the namespace owner's `signer` under `domain`.
    ///
    /// # Errors
    ///
    /// The signer's own error.
    pub(crate) fn sign<S: SignerSync>(
        signer: &S,
        namespace: u64,
        root: [u8; 32],
        seq: u64,
        domain: &Eip712Domain,
    ) -> Result<Self, alloy::signers::Error> {
        let root = B256::from(root);
        let hash = signing_hash(namespace, root, seq, domain);
        let signature = signer.sign_hash_sync(&hash)?;
        Ok(Self {
            namespace,
            root,
            seq,
            signature: Bytes::from(signature.as_bytes().to_vec()),
        })
    }

    /// The address that signed the pointer under `domain`.
    ///
    /// # Errors
    ///
    /// [`MalformedSignature`]: the bytes are no signature, or a high-`s` one,
    /// which the contracts would refuse and so the gateway does too.
    pub(crate) fn signer(&self, domain: &Eip712Domain) -> Result<Address, MalformedSignature> {
        let signature =
            Signature::try_from(self.signature.as_ref()).map_err(|_| MalformedSignature)?;
        if decdn_incentive::sig_canon::is_high_s(&signature) {
            return Err(MalformedSignature);
        }
        let hash = signing_hash(self.namespace, self.root, self.seq, domain);
        signature
            .recover_address_from_prehash(&hash)
            .map_err(|_| MalformedSignature)
    }
}

/// A stored pointer: the record, the owner it recovered to, and the highest
/// `seq` stored from each signer the namespace ever had.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Record {
    #[serde(flatten)]
    pointer: SignedPointer,
    signer: Address,
    /// The highest `seq` stored from each signer, holder and former holders.
    high_water: HashMap<Address, u64>,
}

/// Why a pointer was not stored.
#[derive(Debug)]
pub(crate) enum SetError {
    /// The gateway stored a pointer from the same signer at `current`, and
    /// the new one's `seq` is not above it.
    Stale {
        /// The `seq` of the pointer held.
        current: u64,
    },
    /// The record could not be written.
    Io(anyhow::Error),
}

/// The pointers a gateway holds, by namespace, and the directory that keeps
/// them across restarts.
pub(crate) struct Pointers {
    dir: PathBuf,
    live: Mutex<HashMap<u64, Record>>,
}

impl Pointers {
    /// The pointers in `dir`, created owner-only when missing. Every
    /// `<ns>.json` in it is read; a file that does not parse, or whose name
    /// is not its record's namespace, fails the load, so a damaged store
    /// stops the gateway instead of serving a stranger's bundle. Anything
    /// else in the directory (a temp file a crash left behind) is skipped.
    ///
    /// # Errors
    ///
    /// The directory cannot be created or read, or a record is damaged.
    pub(crate) fn load(dir: &Path) -> anyhow::Result<Self> {
        super::private_dir(dir)?;
        let mut live = HashMap::new();
        for entry in std::fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
            let path = entry
                .with_context(|| format!("read {}", dir.display()))?
                .path();
            let Some(stem) = path
                .extension()
                .is_some_and(|ext| ext == "json")
                .then(|| path.file_stem().and_then(|s| s.to_str()))
                .flatten()
            else {
                continue;
            };
            let bytes = std::fs::read(&path).with_context(|| format!("read {}", path.display()))?;
            let record: Record = serde_json::from_slice(&bytes)
                .with_context(|| format!("{} is not a namespace pointer", path.display()))?;
            anyhow::ensure!(
                stem == record.pointer.namespace.to_string(),
                "{} holds the pointer of namespace {}; delete it to start the gateway",
                path.display(),
                record.pointer.namespace
            );
            live.insert(record.pointer.namespace, record);
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            live: Mutex::new(live),
        })
    }

    /// The manifest hash namespace `namespace` points at, if it has a pointer.
    pub(crate) fn root(&self, namespace: u64) -> Option<[u8; 32]> {
        self.live
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&namespace)
            .map(|r| r.pointer.root.0)
    }

    /// The highest `seq` ever stored for `namespace` from `signer`, holder or
    /// not. A pointer from that signer is new only above it.
    pub(crate) fn high_water(&self, namespace: u64, signer: Address) -> Option<u64> {
        self.live
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&namespace)
            .and_then(|r| r.high_water.get(&signer).copied())
    }

    /// Store `pointer`, signed by `signer`, whom the caller verified as the
    /// namespace's owner on-chain. It replaces the pointer held when its
    /// `seq` is above every one stored from `signer`: from the holder, that
    /// is the pointer served; from another signer, whatever the holder's
    /// `seq`, since the namespace changed hands. The record is written whole
    /// before the live map changes, under the lock.
    ///
    /// # Errors
    ///
    /// [`SetError::Stale`] when a pointer from `signer` at or above this
    /// `seq` was stored before; [`SetError::Io`] when the record cannot be
    /// written.
    pub(crate) fn set(&self, pointer: SignedPointer, signer: Address) -> Result<(), SetError> {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        let mut high_water = live
            .get(&pointer.namespace)
            .map(|r| r.high_water.clone())
            .unwrap_or_default();
        if let Some(current) = high_water.get(&signer).copied()
            && pointer.seq <= current
        {
            return Err(SetError::Stale { current });
        }
        high_water.insert(signer, pointer.seq);
        let record = Record {
            pointer,
            signer,
            high_water,
        };
        self.write(&record).map_err(SetError::Io)?;
        live.insert(record.pointer.namespace, record);
        Ok(())
    }

    fn write(&self, record: &Record) -> anyhow::Result<()> {
        let path = self.dir.join(format!("{}.json", record.pointer.namespace));
        let bytes = serde_json::to_vec(record).context("encode the pointer")?;
        let mut file = tempfile::NamedTempFile::new_in(&self.dir)
            .with_context(|| format!("create a temp file in {}", self.dir.display()))?;
        std::io::Write::write_all(&mut file, &bytes)
            .with_context(|| format!("write {}", path.display()))?;
        file.as_file()
            .sync_all()
            .with_context(|| format!("sync {}", path.display()))?;
        file.persist(&path)
            .with_context(|| format!("replace {}", path.display()))?;
        // The rename is durable only once the directory is: a 201 answered
        // before that could serve the old pointer again after a power loss.
        std::fs::File::open(&self.dir)
            .and_then(|dir| dir.sync_all())
            .with_context(|| format!("sync {}", self.dir.display()))?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::indexing_slicing)]
mod tests;
