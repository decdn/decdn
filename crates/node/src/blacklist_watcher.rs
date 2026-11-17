//! Blacklist sync (ADR 011 § Node Behavior).
//!
//! Every paid-delivery node keeps a local deny index that mirrors
//! `ContentBlacklist` and enforces it: it refuses and evicts every hash that is
//! live in a region in scope for this operator (global ∪ current region ∪
//! ripening previous region), and it refuses every pool funded by a blacklisted
//! origin or operator. The node reads the contract only by `eth_call`; it reads
//! no event log.
//!
//! **Boot snapshot.** At the governance chain's `safe` block the node reads
//! `mutationCount` as its ledger cursor, pages the address union and each
//! in-scope region's hash set from the enumeration views, gives every live
//! emergency entry its local `expiresAt` from `getHashEntry` /
//! `getEmergencyOrigin`, and reads the regional-body state of each region in
//! scope and both compliance windows. Both enumeration sets remove by
//! swap-and-pop, so every page and its count come from the one pinned block,
//! and a `seen != count` mismatch aborts the snapshot rather than seat a
//! partial set. A fail-closed readiness gate keeps every ALPN listener shut
//! until the first snapshot is enforced.
//!
//! **Ledger poll.** Every `content_blacklist_poll_interval_sec` (at most five
//! minutes) the node pins the `safe` block again and:
//!
//! 1. reads `mutationCount`; a count below the cursor means the chain
//!    reorganised past the `safe` block, so the node takes a new snapshot;
//! 2. reads `getScopeRegions(operator)` and pages each region that entered the
//!    scope for the first time since the snapshot, replacing what the index
//!    holds for it;
//! 3. reads `getMutations(cursor, …)` up to the count and applies the records
//!    in ledger order (`DenyIndex::apply`), then moves the cursor.
//!
//! Each record overwrites the whole state of its key. Body and compliance-window
//! records change the per-region body state and the windows, never the hash
//! index. The node expires an entry locally once its clock passes the entry's
//! `expiresAt`. A successful poll stamps the chain-freshness clock whether or
//! not the count moved.
//!
//! **Enforcement** runs after every snapshot and poll: deny every live in-scope
//! hash and evict it (the eviction is durable and sticky, and cascades to DHT
//! announce suppression, probe `has_blob: false` and delivery refusal), lift the
//! governance deny on a hash that is no longer live in scope, and publish the
//! live address union to the delivery path's origin deny-set.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use anyhow::{Context as _, Result};
use decdn_cache::{CacheEngine, Hash};
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::content_blacklist::ContentBlacklist;
use decdn_incentive::content_blacklist::ContentBlacklist::MutationKind;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain_events::boot_retry::{BootFault, BootRetry};
use crate::chain_events::timed;
use crate::chain_freshness::ChainFreshness;
use crate::content_deny::ContentDenylist;
use crate::metrics::Metrics;
use crate::warming_allowance::WarmingAllowance;

/// Page size for the enumeration views and the ledger. Bounded so one large
/// set cannot ask for an unbounded array in a single `eth_call`.
const PAGE_SIZE: u64 = 100;

/// The contract's global region key: `bytes32("GLOBAL")`. A ledger record
/// writes the global scope as region `0`; the index files it under this key.
pub(crate) const GLOBAL_REGION: B256 =
    B256::new(*b"GLOBAL\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");

/// Emergency auto-expiry term of a `GENERAL` entry, mirroring the contract's
/// `EMERGENCY_EXPIRY_GENERAL`.
const EMERGENCY_EXPIRY_GENERAL_SECS: u64 = 14 * 24 * 60 * 60;

/// Emergency auto-expiry term of a `CSAM` or `TERRORIST` entry, mirroring the
/// contract's `EMERGENCY_EXPIRY_SEVERE`. The contract maps every category but
/// `GENERAL` to it.
const EMERGENCY_EXPIRY_SEVERE_SECS: u64 = 90 * 24 * 60 * 60;

/// Result reported exactly once when the first snapshot either establishes
/// compliance or proves startup cannot safely continue.
pub(crate) type InitialSyncResult = std::result::Result<(), String>;

/// The `expiresAt` of an emergency entry added at `added_at` in `category`,
/// as the contract computes it.
const fn emergency_expires_at(added_at: u64, category: u8) -> u64 {
    let term = if category == 0 {
        EMERGENCY_EXPIRY_GENERAL_SECS
    } else {
        EMERGENCY_EXPIRY_SEVERE_SECS
    };
    added_at.saturating_add(term)
}

/// Whether state with `expires_at` holds at `now`: `0` never lapses, else the
/// state holds while `now <= expires_at`, the contract's own deadline.
const fn is_live(expires_at: u64, now: u64) -> bool {
    expires_at == 0 || now <= expires_at
}

/// A regional body's suspension.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Suspension {
    /// An unratified suspension that lapses after this Unix second.
    Until(u64),
    /// A ratified suspension that stands until an unsuspend.
    Ratified,
}

/// One region's body state: the body address and its suspension, if any.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BodyState {
    /// The registered body.
    pub(crate) body: Address,
    /// The body's suspension, if one stands.
    pub(crate) suspension: Option<Suspension>,
}

/// One ledger record, decoded from `ContentBlacklist.Mutation`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Record {
    /// What the record does.
    pub(crate) kind: RecordKind,
    /// `0` when nothing lapses, else the moment the set state lapses.
    pub(crate) expires_at: u64,
    /// The region of a regional hash or body record, `0` otherwise.
    pub(crate) region: B256,
    /// The hash, the address left-padded, or the window in seconds.
    pub(crate) key: B256,
}

/// The ledger's record kinds (ADR 011 § Following the mutation ledger).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub(crate) enum RecordKind {
    HashAdd,
    HashEmergencyAdd,
    HashRatify,
    HashRemove,
    HashExpire,
    OriginSet,
    OriginClear,
    OriginEmergencyAdd,
    OriginExpire,
    OperatorAdd,
    OperatorRemove,
    BodyRegister,
    BodyDeregister,
    BodySuspend,
    BodySuspensionRatify,
    BodyUnsuspend,
    ComplianceWindowSet,
    EmergencyComplianceWindowSet,
}

impl TryFrom<&ContentBlacklist::Mutation> for Record {
    type Error = anyhow::Error;

    fn try_from(m: &ContentBlacklist::Mutation) -> Result<Self> {
        let kind = match m.kind {
            MutationKind::HASH_ADD => RecordKind::HashAdd,
            MutationKind::HASH_EMERGENCY_ADD => RecordKind::HashEmergencyAdd,
            MutationKind::HASH_RATIFY => RecordKind::HashRatify,
            MutationKind::HASH_REMOVE => RecordKind::HashRemove,
            MutationKind::HASH_EXPIRE => RecordKind::HashExpire,
            MutationKind::ORIGIN_SET => RecordKind::OriginSet,
            MutationKind::ORIGIN_CLEAR => RecordKind::OriginClear,
            MutationKind::ORIGIN_EMERGENCY_ADD => RecordKind::OriginEmergencyAdd,
            MutationKind::ORIGIN_EXPIRE => RecordKind::OriginExpire,
            MutationKind::OPERATOR_ADD => RecordKind::OperatorAdd,
            MutationKind::OPERATOR_REMOVE => RecordKind::OperatorRemove,
            MutationKind::BODY_REGISTER => RecordKind::BodyRegister,
            MutationKind::BODY_DEREGISTER => RecordKind::BodyDeregister,
            MutationKind::BODY_SUSPEND => RecordKind::BodySuspend,
            MutationKind::BODY_SUSPENSION_RATIFY => RecordKind::BodySuspensionRatify,
            MutationKind::BODY_UNSUSPEND => RecordKind::BodyUnsuspend,
            MutationKind::COMPLIANCE_WINDOW_SET => RecordKind::ComplianceWindowSet,
            MutationKind::EMERGENCY_COMPLIANCE_WINDOW_SET => {
                RecordKind::EmergencyComplianceWindowSet
            }
            // A kind this binary does not know is an ABI drift. Applying around
            // it would leave the index short a change, so the poll fails and
            // the node goes stale rather than serve on a partial index.
            MutationKind::__Invalid => anyhow::bail!("unknown ContentBlacklist mutation kind"),
        };
        Ok(Self {
            kind,
            expires_at: m.expiresAt,
            region: m.region,
            key: m.key,
        })
    }
}

/// The local deny index (ADR 011 § Enumerating the deny-set). It holds raw
/// state with each entry's `expiresAt`; scope and liveness are evaluated at
/// lookup.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DenyIndex {
    /// Region key → hash → `expiresAt`. The global scope is [`GLOBAL_REGION`].
    pub(crate) hashes: HashMap<B256, HashMap<Hash, u64>>,
    /// Address → `expiresAt` of its origin leg.
    pub(crate) origin_leg: HashMap<Address, u64>,
    /// Addresses whose operator leg is set. The operator leg never lapses.
    pub(crate) operator_leg: HashSet<Address>,
    /// Region key → its regional body.
    pub(crate) bodies: HashMap<B256, BodyState>,
    /// `complianceWindow`, seconds.
    pub(crate) compliance_window: u64,
    /// `emergencyComplianceWindow`, seconds.
    pub(crate) emergency_compliance_window: u64,
}

/// The address a ledger `key` carries, left-padded to 32 bytes.
fn key_address(key: B256) -> Address {
    Address::from_word(key)
}

/// A ledger window record carries its seconds in `key`.
fn key_seconds(key: B256) -> u64 {
    u64::try_from(U256::from_be_bytes(key.0)).unwrap_or(u64::MAX)
}

impl DenyIndex {
    /// Apply one ledger record. Each record overwrites the whole state of its
    /// key, so a record a snapshot already reflects leaves the index correct.
    /// Body and window records change only the body state and the windows,
    /// never the hash index or the address legs.
    pub(crate) fn apply(&mut self, record: &Record) {
        let region = if record.region == B256::ZERO {
            GLOBAL_REGION
        } else {
            record.region
        };
        match record.kind {
            RecordKind::HashAdd | RecordKind::HashEmergencyAdd | RecordKind::HashRatify => {
                self.hashes
                    .entry(region)
                    .or_default()
                    .insert(Hash::from_bytes(record.key.0), record.expires_at);
            }
            RecordKind::HashRemove | RecordKind::HashExpire => {
                if let Some(set) = self.hashes.get_mut(&region) {
                    set.remove(&Hash::from_bytes(record.key.0));
                }
            }
            RecordKind::OriginSet | RecordKind::OriginEmergencyAdd => {
                self.origin_leg
                    .insert(key_address(record.key), record.expires_at);
            }
            RecordKind::OriginClear | RecordKind::OriginExpire => {
                self.origin_leg.remove(&key_address(record.key));
            }
            RecordKind::OperatorAdd => {
                self.operator_leg.insert(key_address(record.key));
            }
            RecordKind::OperatorRemove => {
                self.operator_leg.remove(&key_address(record.key));
            }
            RecordKind::BodyRegister => {
                self.bodies.insert(
                    region,
                    BodyState {
                        body: key_address(record.key),
                        suspension: None,
                    },
                );
            }
            RecordKind::BodyDeregister => {
                self.bodies.remove(&region);
            }
            RecordKind::BodySuspend => {
                self.set_suspension(
                    region,
                    record.key,
                    Some(Suspension::Until(record.expires_at)),
                );
            }
            RecordKind::BodySuspensionRatify => {
                self.set_suspension(region, record.key, Some(Suspension::Ratified));
            }
            RecordKind::BodyUnsuspend => self.set_suspension(region, record.key, None),
            RecordKind::ComplianceWindowSet => self.compliance_window = key_seconds(record.key),
            RecordKind::EmergencyComplianceWindowSet => {
                self.emergency_compliance_window = key_seconds(record.key);
            }
        }
    }

    /// Overwrite `region`'s body state with `suspension`, naming the body the
    /// record carries.
    fn set_suspension(&mut self, region: B256, key: B256, suspension: Option<Suspension>) {
        self.bodies.insert(
            region,
            BodyState {
                body: key_address(key),
                suspension,
            },
        );
    }

    /// Every hash live at `now` in a region of `scope`.
    pub(crate) fn denied_hashes(&self, scope: &[B256], now: u64) -> HashSet<Hash> {
        scope
            .iter()
            .filter_map(|region| self.hashes.get(region))
            .flat_map(|set| set.iter())
            .filter(|(_, expires_at)| is_live(**expires_at, now))
            .map(|(hash, _)| *hash)
            .collect()
    }

    /// Every address whose origin leg is live at `now` or whose operator leg
    /// is set: the union the delivery path refuses on.
    pub(crate) fn denied_addresses(&self, now: u64) -> HashSet<Address> {
        self.origin_leg
            .iter()
            .filter(|(_, expires_at)| is_live(**expires_at, now))
            .map(|(addr, _)| *addr)
            .chain(self.operator_leg.iter().copied())
            .collect()
    }
}

/// The chain reads the sync makes, behind a trait so the snapshot, poll and
/// enforcement logic are unit-testable against a scripted chain. Every read
/// but [`Self::safe_block`] is pinned to the block it is given.
///
/// RPITIT with an explicit `+ Send`, because the poll runs on a spawned task.
pub(crate) trait BlacklistReads: Send + Sync {
    /// The number of the governance chain's `safe` block.
    fn safe_block(&self) -> impl Future<Output = Result<u64>> + Send;
    /// `mutationCount`.
    fn mutation_count(&self, at: u64) -> impl Future<Output = Result<u64>> + Send;
    /// `getMutations(from, limit)`.
    fn mutations(
        &self,
        from: u64,
        limit: u64,
        at: u64,
    ) -> impl Future<Output = Result<Vec<Record>>> + Send;
    /// `getScopeRegions(operator)`: `GLOBAL` first, one to three keys.
    fn scope_regions(
        &self,
        operator: Address,
        at: u64,
    ) -> impl Future<Output = Result<Vec<B256>>> + Send;
    /// `blacklistedHashCount(region)`.
    fn hash_count(&self, region: B256, at: u64) -> impl Future<Output = Result<u64>> + Send;
    /// A page of `blacklistedHashes(region, offset, limit)`.
    fn hash_page(
        &self,
        region: B256,
        offset: u64,
        limit: u64,
        at: u64,
    ) -> impl Future<Output = Result<Vec<B256>>> + Send;
    /// The live `expiresAt` of `(region, hash)` from `getHashEntry`: `None` for
    /// an absent entry, `Some(0)` for a non-emergency entry.
    fn hash_expiry(
        &self,
        region: B256,
        hash: B256,
        at: u64,
    ) -> impl Future<Output = Result<Option<u64>>> + Send;
    /// `blacklistedAddressCount`.
    fn address_count(&self, at: u64) -> impl Future<Output = Result<u64>> + Send;
    /// A page of `blacklistedAddresses(offset, limit)`.
    fn address_page(
        &self,
        offset: u64,
        limit: u64,
        at: u64,
    ) -> impl Future<Output = Result<Vec<Address>>> + Send;
    /// The two legs of `addr`: the origin leg's `expiresAt` when
    /// `isOriginBlacklisted` holds (from `getEmergencyOrigin`), and
    /// `isOperatorBlacklisted`.
    fn address_legs(
        &self,
        addr: Address,
        at: u64,
    ) -> impl Future<Output = Result<(Option<u64>, bool)>> + Send;
    /// `getRegionalBody(region)`, or `None` when no body is registered.
    fn regional_body(
        &self,
        region: B256,
        at: u64,
    ) -> impl Future<Output = Result<Option<BodyState>>> + Send;
    /// `complianceWindow` and `emergencyComplianceWindow`.
    fn compliance_windows(&self, at: u64) -> impl Future<Output = Result<(u64, u64)>> + Send;
}

/// Production [`BlacklistReads`] over the live contract. Every read is bounded
/// by the shared [`timed`] per-call timeout.
pub(crate) struct ContractReads<P: Provider + Clone> {
    contract: ContentBlacklist::ContentBlacklistInstance<P>,
}

impl<P: Provider + Clone> BlacklistReads for ContractReads<P> {
    async fn safe_block(&self) -> Result<u64> {
        let block = timed(
            None,
            "eth_getBlockByNumber(safe)",
            self.contract
                .provider()
                .get_block_by_number(BlockNumberOrTag::Safe),
        )
        .await?
        .context("the RPC returned no safe block")?;
        Ok(block.header.number)
    }

    async fn mutation_count(&self, at: u64) -> Result<u64> {
        let count = timed(
            None,
            "mutationCount",
            self.contract
                .mutationCount()
                .block(BlockId::number(at))
                .call(),
        )
        .await?;
        u64::try_from(count).context("mutationCount exceeds u64")
    }

    async fn mutations(&self, from: u64, limit: u64, at: u64) -> Result<Vec<Record>> {
        let page = timed(
            None,
            "getMutations",
            self.contract
                .getMutations(U256::from(from), U256::from(limit))
                .block(BlockId::number(at))
                .call(),
        )
        .await?;
        page.iter().map(Record::try_from).collect()
    }

    async fn scope_regions(&self, operator: Address, at: u64) -> Result<Vec<B256>> {
        timed(
            None,
            "getScopeRegions",
            self.contract
                .getScopeRegions(operator)
                .block(BlockId::number(at))
                .call(),
        )
        .await
    }

    async fn hash_count(&self, region: B256, at: u64) -> Result<u64> {
        let count = timed(
            None,
            "blacklistedHashCount",
            self.contract
                .blacklistedHashCount(region)
                .block(BlockId::number(at))
                .call(),
        )
        .await?;
        u64::try_from(count).context("blacklistedHashCount exceeds u64")
    }

    async fn hash_page(&self, region: B256, offset: u64, limit: u64, at: u64) -> Result<Vec<B256>> {
        timed(
            None,
            "blacklistedHashes",
            self.contract
                .blacklistedHashes(region, U256::from(offset), U256::from(limit))
                .block(BlockId::number(at))
                .call(),
        )
        .await
    }

    async fn hash_expiry(&self, region: B256, hash: B256, at: u64) -> Result<Option<u64>> {
        let entry = timed(
            None,
            "getHashEntry",
            self.contract
                .getHashEntry(region, hash)
                .block(BlockId::number(at))
                .call(),
        )
        .await?;
        Ok(match (entry.addedAt, entry.emergency) {
            (0, _) => None,
            (_, false) => Some(0),
            (added_at, true) => Some(emergency_expires_at(added_at, entry.category)),
        })
    }

    async fn address_count(&self, at: u64) -> Result<u64> {
        let count = timed(
            None,
            "blacklistedAddressCount",
            self.contract
                .blacklistedAddressCount()
                .block(BlockId::number(at))
                .call(),
        )
        .await?;
        u64::try_from(count).context("blacklistedAddressCount exceeds u64")
    }

    async fn address_page(&self, offset: u64, limit: u64, at: u64) -> Result<Vec<Address>> {
        timed(
            None,
            "blacklistedAddresses",
            self.contract
                .blacklistedAddresses(U256::from(offset), U256::from(limit))
                .block(BlockId::number(at))
                .call(),
        )
        .await
    }

    async fn address_legs(&self, addr: Address, at: u64) -> Result<(Option<u64>, bool)> {
        let block = BlockId::number(at);
        let origin = timed(
            None,
            "isOriginBlacklisted",
            self.contract.isOriginBlacklisted(addr).block(block).call(),
        )
        .await?;
        let origin = if origin {
            let emergency = timed(
                None,
                "getEmergencyOrigin",
                self.contract.getEmergencyOrigin(addr).block(block).call(),
            )
            .await?;
            Some(if emergency.addedAt == 0 {
                0
            } else {
                emergency_expires_at(emergency.addedAt, emergency.category)
            })
        } else {
            None
        };
        let operator = timed(
            None,
            "isOperatorBlacklisted",
            self.contract
                .isOperatorBlacklisted(addr)
                .block(block)
                .call(),
        )
        .await?;
        Ok((origin, operator))
    }

    async fn regional_body(&self, region: B256, at: u64) -> Result<Option<BodyState>> {
        let rb = timed(
            None,
            "getRegionalBody",
            self.contract
                .getRegionalBody(region)
                .block(BlockId::number(at))
                .call(),
        )
        .await?;
        if rb.body == Address::ZERO {
            return Ok(None);
        }
        let suspension = match (rb.suspendedAt, rb.suspensionRatified) {
            (0, _) => None,
            (_, true) => Some(Suspension::Ratified),
            (suspended_at, false) => Some(Suspension::Until(
                suspended_at.saturating_add(BODY_SUSPENSION_RATIFICATION_WINDOW_SECS),
            )),
        };
        Ok(Some(BodyState {
            body: rb.body,
            suspension,
        }))
    }

    async fn compliance_windows(&self, at: u64) -> Result<(u64, u64)> {
        let block = BlockId::number(at);
        let window = timed(
            None,
            "complianceWindow",
            self.contract.complianceWindow().block(block).call(),
        )
        .await?;
        let emergency = timed(
            None,
            "emergencyComplianceWindow",
            self.contract
                .emergencyComplianceWindow()
                .block(block)
                .call(),
        )
        .await?;
        Ok((window, emergency))
    }
}

/// How long an unratified body suspension stands, mirroring the contract's
/// `BODY_SUSPENSION_RATIFICATION_WINDOW`. A `BODY_SUSPEND` record carries the
/// lapse moment itself; only the boot read of `getRegionalBody` needs this.
const BODY_SUSPENSION_RATIFICATION_WINDOW_SECS: u64 = 14 * 24 * 60 * 60;

/// Page `region`'s hash set at `at`, count-checked, and keep each hash whose
/// entry is live at `now` with its `expiresAt`.
async fn read_region<R: BlacklistReads>(
    reads: &R,
    region: B256,
    at: u64,
    now: u64,
) -> Result<HashMap<Hash, u64>> {
    let count = reads
        .hash_count(region, at)
        .await
        .with_context(|| format!("blacklistedHashCount({region})"))?;
    let mut raw: HashSet<B256> = HashSet::new();
    let mut offset = 0u64;
    while offset < count {
        let page = reads
            .hash_page(region, offset, PAGE_SIZE, at)
            .await
            .with_context(|| format!("blacklistedHashes(region={region}, offset={offset})"))?;
        if page.is_empty() {
            break;
        }
        offset = offset.saturating_add(page.len() as u64);
        raw.extend(page);
    }
    anyhow::ensure!(
        raw.len() as u64 == count,
        "region {region} hash enumeration read {} of {count} entries at block {at}; the pinned \
         set was read inconsistently, so the snapshot would miss a hash — aborting rather than \
         seating a partial set",
        raw.len()
    );
    let mut live = HashMap::with_capacity(raw.len());
    for hash in raw {
        if let Some(expires_at) = reads
            .hash_expiry(region, hash, at)
            .await
            .with_context(|| format!("getHashEntry({region}, {hash})"))?
            && is_live(expires_at, now)
        {
            live.insert(Hash::from_bytes(hash.0), expires_at);
        }
    }
    Ok(live)
}

/// A full snapshot: the index, the ledger cursor and the scope it was read
/// for, all at one `safe` block.
#[derive(Debug)]
pub(crate) struct Snapshot {
    /// The pinned block.
    pub(crate) block: u64,
    /// `mutationCount` at the block.
    pub(crate) cursor: u64,
    /// The scope regions at the block; every one of them is paged.
    pub(crate) scope: Vec<B256>,
    /// The index the snapshot read.
    pub(crate) index: DenyIndex,
}

/// Read a full snapshot at the `safe` block (ADR 011 § Enumerating the
/// deny-set).
pub(crate) async fn read_snapshot<R: BlacklistReads>(
    reads: &R,
    operator: Address,
    now: u64,
) -> Result<Snapshot> {
    let at = reads.safe_block().await.context("read the safe block")?;
    let cursor = reads.mutation_count(at).await.context("mutationCount")?;
    let mut index = DenyIndex::default();

    let count = reads
        .address_count(at)
        .await
        .context("blacklistedAddressCount")?;
    let mut raw: HashSet<Address> = HashSet::new();
    let mut offset = 0u64;
    while offset < count {
        let page = reads
            .address_page(offset, PAGE_SIZE, at)
            .await
            .with_context(|| format!("blacklistedAddresses(offset={offset})"))?;
        if page.is_empty() {
            break;
        }
        offset = offset.saturating_add(page.len() as u64);
        raw.extend(page);
    }
    anyhow::ensure!(
        raw.len() as u64 == count,
        "address enumeration read {} of {count} entries at block {at}; the pinned set was read \
         inconsistently, so the snapshot would miss an address — aborting rather than seating a \
         partial set",
        raw.len()
    );
    for addr in raw {
        let (origin, operator_leg) = reads
            .address_legs(addr, at)
            .await
            .with_context(|| format!("address legs of {addr}"))?;
        if let Some(expires_at) = origin.filter(|e| is_live(*e, now)) {
            index.origin_leg.insert(addr, expires_at);
        }
        if operator_leg {
            index.operator_leg.insert(addr);
        }
    }

    let scope = reads
        .scope_regions(operator, at)
        .await
        .with_context(|| format!("getScopeRegions({operator})"))?;
    for region in &scope {
        index
            .hashes
            .insert(*region, read_region(reads, *region, at, now).await?);
        if let Some(body) = reads
            .regional_body(*region, at)
            .await
            .with_context(|| format!("getRegionalBody({region})"))?
        {
            index.bodies.insert(*region, body);
        }
    }
    let (window, emergency_window) = reads
        .compliance_windows(at)
        .await
        .context("compliance windows")?;
    index.compliance_window = window;
    index.emergency_compliance_window = emergency_window;
    Ok(Snapshot {
        block: at,
        cursor,
        scope,
        index,
    })
}

/// What one ledger poll did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct PollOutcome {
    /// The count fell below the cursor, so the poll took a new snapshot.
    pub(crate) resnapshot: bool,
    /// Records applied.
    pub(crate) applied: u64,
    /// Regions paged because they entered the scope.
    pub(crate) paged_regions: usize,
}

/// The deny index, its ledger cursor and the scope it enforces.
#[derive(Debug)]
pub(crate) struct LedgerState {
    /// The index.
    pub(crate) index: DenyIndex,
    /// Records applied so far: the next ledger index to read.
    pub(crate) cursor: u64,
    /// The operator's scope at the last poll.
    pub(crate) scope: Vec<B256>,
    /// The regions paged since the last snapshot.
    pub(crate) paged: HashSet<B256>,
}

impl LedgerState {
    /// The state a snapshot seats.
    pub(crate) fn from_snapshot(snapshot: Snapshot) -> Self {
        Self {
            paged: snapshot.scope.iter().copied().collect(),
            index: snapshot.index,
            cursor: snapshot.cursor,
            scope: snapshot.scope,
        }
    }

    /// One ledger poll at the `safe` block (ADR 011 § Following the mutation
    /// ledger). On `Err` the state is unchanged except for the regions it
    /// already paged.
    pub(crate) async fn poll<R: BlacklistReads>(
        &mut self,
        reads: &R,
        operator: Address,
        now: u64,
    ) -> Result<PollOutcome> {
        let at = reads.safe_block().await.context("read the safe block")?;
        let count = reads.mutation_count(at).await.context("mutationCount")?;
        if count < self.cursor {
            // The chain reorganised past the `safe` block.
            warn!(
                count,
                cursor = self.cursor,
                "blacklist ledger count fell below the cursor; taking a new snapshot"
            );
            *self = Self::from_snapshot(read_snapshot(reads, operator, now).await?);
            return Ok(PollOutcome {
                resnapshot: true,
                ..PollOutcome::default()
            });
        }
        let scope = reads
            .scope_regions(operator, at)
            .await
            .with_context(|| format!("getScopeRegions({operator})"))?;
        let mut paged_regions = 0usize;
        for region in &scope {
            if self.paged.contains(region) {
                continue;
            }
            let hashes = read_region(reads, *region, at, now).await?;
            self.index.hashes.insert(*region, hashes);
            self.paged.insert(*region);
            paged_regions += 1;
        }
        // Read every record before applying any, so a failed page leaves the
        // cursor and the index as they were.
        let mut records = Vec::new();
        let mut from = self.cursor;
        while from < count {
            let page = reads
                .mutations(from, PAGE_SIZE.min(count - from), at)
                .await
                .with_context(|| format!("getMutations(from={from})"))?;
            if page.is_empty() {
                anyhow::bail!("getMutations(from={from}) returned no record below count {count}");
            }
            from = from.saturating_add(page.len() as u64);
            records.extend(page);
        }
        for record in &records {
            self.index.apply(record);
        }
        self.cursor = count;
        self.scope = scope;
        Ok(PollOutcome {
            resnapshot: false,
            applied: records.len() as u64,
            paged_regions,
        })
    }
}

/// The cache and deny-set side of the sync: what it enforces and where.
pub(crate) struct Enforcer {
    cache: CacheEngine,
    warming: Arc<WarmingAllowance>,
    denylist: Arc<ContentDenylist>,
    /// The hashes the last pass denied, so a pass can lift the deny on a hash
    /// that left the live in-scope set.
    denied: HashSet<Hash>,
}

impl Enforcer {
    /// An enforcer that has denied nothing yet.
    pub(crate) fn new(
        cache: CacheEngine,
        warming: Arc<WarmingAllowance>,
        denylist: Arc<ContentDenylist>,
    ) -> Self {
        Self {
            cache,
            warming,
            denylist,
            denied: HashSet::new(),
        }
    }

    /// Deny and evict every live in-scope hash of `state` at `now`, lift the
    /// deny on a hash no longer in that set (its eviction stays sticky), and
    /// publish the live address union. Returns the hashes whose eviction
    /// failed; the next pass retries them, and they stay refused through the
    /// deny meanwhile.
    pub(crate) async fn enforce(&mut self, state: &LedgerState, now: u64) -> Vec<(Hash, String)> {
        let denied = state.index.denied_hashes(&state.scope, now);
        let mut failed = Vec::new();
        let mut evicted = 0usize;
        for hash in &denied {
            match self.deny_and_evict(*hash).await {
                Ok(true) => evicted += 1,
                Ok(false) => {}
                Err(err) => failed.push((*hash, err)),
            }
        }
        for hash in self.denied.difference(&denied) {
            self.cache.set_chain_denied_one(*hash, false);
        }
        self.denied = denied;
        self.denylist
            .set_chain_origins(state.index.denied_addresses(now));
        if evicted > 0 {
            info!(evicted, "blacklist sync evicted blacklisted blobs");
        }
        failed
    }

    /// Deny `hash`, then evict it unless it is already evicted. Returns
    /// whether this call evicted it, or the eviction error.
    async fn deny_and_evict(&self, hash: Hash) -> Result<bool, String> {
        // Deny before evicting: the deny alone stops the serving, so an
        // eviction that fails on a disk error does not leave the hash servable.
        self.cache.set_chain_denied_one(hash, true);
        if self.cache.is_evicted(hash) {
            return Ok(false);
        }
        match self.cache.evict(hash).await {
            Ok(()) => {
                info!(%hash, "evicted blacklisted blob (ADR 011 compliance)");
                // ADR 041: drop the warming tag, so a later reuse of the slot
                // never credits a stale source's allowance.
                self.warming.forget(hash);
                Ok(true)
            }
            Err(err) => {
                let err = anyhow::Error::new(err).context(format!("evict {hash}"));
                warn!(%hash, error = %sanitize_err_chain(&err), "blacklist sync: evict failed; will retry");
                Err(format!("{err:#}"))
            }
        }
    }
}

/// Current wall-clock Unix seconds, the clock local expiry runs on. A
/// pre-epoch clock reads `0`, under which nothing has expired: the safe
/// direction for a compliance gate.
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// The running sync: the state, its reads and its enforcement.
pub(crate) struct BlacklistSync<R: BlacklistReads> {
    reads: R,
    operator: Address,
    state: LedgerState,
    enforcer: Enforcer,
    freshness: ChainFreshness,
    metrics: Arc<Metrics>,
}

impl<R: BlacklistReads> BlacklistSync<R> {
    /// One poll and one enforcement pass. A failed poll keeps the current index
    /// and still enforces it, so a local expiry takes effect during an outage.
    pub(crate) async fn tick(&mut self) {
        let now = unix_now();
        match self.state.poll(&self.reads, self.operator, now).await {
            Ok(outcome) => {
                self.freshness.stamp();
                self.metrics.blacklist_watcher_tick();
                debug!(
                    cursor = self.state.cursor,
                    applied = outcome.applied,
                    paged_regions = outcome.paged_regions,
                    resnapshot = outcome.resnapshot,
                    bodies = self.state.index.bodies.len(),
                    compliance_window = self.state.index.compliance_window,
                    emergency_compliance_window = self.state.index.emergency_compliance_window,
                    "blacklist ledger poll"
                );
            }
            Err(err) => {
                self.metrics.blacklist_ledger_poll_failure();
                warn!(
                    error = %sanitize_err_chain(&err),
                    "blacklist ledger poll failed; keeping the current deny index"
                );
            }
        }
        let failed = self.enforcer.enforce(&self.state, now).await;
        if !failed.is_empty() {
            self.metrics
                .blacklist_enforcement_failure(failed.len() as u64);
        }
    }
}

/// Take the boot snapshot at the `safe` block and enforce it, then return the
/// running sync. `initial_sync_tx` fires once: `Ok` after a clean pass, so the
/// runtime can gate the ALPN router on blacklist enforcement being live, or
/// `Err` on a deterministic fault or an exhausted boot budget. That failure also
/// returns `Err`, so the router never opens on an un-vetted deny-set.
///
/// The snapshot read retries on `boot`'s budget. A failed eviction is a local
/// disk fault, which a retry does not repair, so it fails boot at once.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn bootstrap<P>(
    provider: P,
    contract_addr: Address,
    operator: Address,
    cache: CacheEngine,
    warming: Arc<WarmingAllowance>,
    initial_sync_tx: oneshot::Sender<InitialSyncResult>,
    metrics: &Arc<Metrics>,
    denylist: Arc<ContentDenylist>,
    chain_freshness: ChainFreshness,
    boot: &BootRetry,
) -> Result<BlacklistSync<ContractReads<P>>>
where
    P: Provider + Clone + 'static,
{
    let reads = ContractReads {
        contract: ContentBlacklist::new(contract_addr, provider),
    };
    let result = boot_sync(
        reads,
        operator,
        Enforcer::new(cache, warming, denylist),
        chain_freshness,
        Arc::clone(metrics),
        boot,
    )
    .await;
    match result {
        Ok(sync) => {
            let _ = initial_sync_tx.send(Ok(()));
            info!(
                %contract_addr,
                %operator,
                cursor = sync.state.cursor,
                regions = sync.state.scope.len(),
                origins = sync.state.index.origin_leg.len(),
                operators = sync.state.index.operator_leg.len(),
                "blacklist sync seated its boot snapshot"
            );
            Ok(sync)
        }
        Err(err) => {
            // Fail CLOSED: the runtime keeps every ALPN listener shut, then
            // aborts startup — a node that cannot vet the takedown set must not
            // serve.
            let _ = initial_sync_tx.send(Err(format!(
                "initial ContentBlacklist sync failed: {}",
                sanitize_err_chain(&err)
            )));
            Err(err)
        }
    }
}

/// [`bootstrap`] over any [`BlacklistReads`].
pub(crate) async fn boot_sync<R: BlacklistReads>(
    reads: R,
    operator: Address,
    mut enforcer: Enforcer,
    freshness: ChainFreshness,
    metrics: Arc<Metrics>,
    boot: &BootRetry,
) -> Result<BlacklistSync<R>> {
    let snapshot = boot
        .run("ContentBlacklist boot snapshot", || {
            read_snapshot(&reads, operator, unix_now())
        })
        .await
        .context("read the ContentBlacklist boot snapshot")?;
    debug!(
        safe_block = snapshot.block,
        cursor = snapshot.cursor,
        "blacklist boot snapshot read"
    );
    let state = LedgerState::from_snapshot(snapshot);
    let failed = enforcer.enforce(&state, unix_now()).await;
    if let Some((hash, cause)) = failed.first() {
        metrics.blacklist_enforcement_failure(failed.len() as u64);
        return Err(
            anyhow::Error::from(BootFault(format!("evict {hash}: {cause}")))
                .context("initial ContentBlacklist enforcement could not evict every entry"),
        );
    }
    // A node that has just vetted its deny-set is never treated as stale.
    freshness.stamp();
    metrics.blacklist_watcher_tick();
    Ok(BlacklistSync {
        reads,
        operator,
        state,
        enforcer,
        freshness,
        metrics,
    })
}

/// Run the ledger poll every `interval` until `shutdown` is cancelled. The
/// loop exists because the deny index must follow every takedown within the
/// compliance window.
pub(crate) async fn run<R: BlacklistReads + 'static>(
    sync: BlacklistSync<R>,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let sync = Arc::new(tokio::sync::Mutex::new(sync));
    crate::chain_events::refresh::run_refresh_loop(interval, interval, None, shutdown, move || {
        let sync = Arc::clone(&sync);
        async move { sync.lock().await.tick().await }
    })
    .await;
}

#[cfg(test)]
#[allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing
)]
mod tests;
