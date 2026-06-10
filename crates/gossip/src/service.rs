//! Gossip publisher + subscriber service wiring iroh-gossip to the peer table.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use bytes::Bytes;
use decdn_protocol::{
    GOSSIP_VERSION, GossipEnvelope, GossipPayload, NodeAnnounce, NodeAnnounceBody,
    ReputationReport, ReputationReportBody, TOPIC_GLOBAL, TOPIC_REGION_PREFIX, TOPIC_REPUTATION,
};
use iroh::{Endpoint, SecretKey};
use iroh_gossip::api::{GossipReceiver, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::reputation::{
    MAX_REPORTS_PER_REPORTER_PER_HR, ReportDrain, ReputationRateLimiter, ReputationSink,
    StakedReporterSet, validate_reputation_envelope,
};
use crate::{AnnounceReject, GossipMetrics, InsertOutcome, PeerTable, validate_envelope};

/// Label passed to [`GossipMetrics::inc_rejected`] when a topic subscribe
/// call fails at startup. Pinned by the label-stability test in
/// `validation::tests` so a rename fails CI.
pub(crate) const SUBSCRIBE_FAILED_LABEL: &str = "subscribe_failed";

/// Label passed to [`GossipMetrics::inc_rejected`] when an inbound
/// signature-valid announce is dropped because the peer table is at its
/// hard cap and the inline TTL sweep couldn't free a slot (#577 H3).
/// Pinned by the label-stability test in `validation::tests`.
pub(crate) const PEER_TABLE_FULL_LABEL: &str = "peer_table_full";

/// Label passed to [`GossipMetrics::inc_rejected`] when a subscriber's
/// reconnect attempt to `gossip.subscribe(topic_id, ...)` fails (#577 H2
/// follow-up). The existing `tracing::error!` is informative but
/// unscrapable; this counter lets operators alert on a node stuck in a
/// reconnect loop. Pinned by the label-stability test in `validation::tests`.
pub(crate) const RESUBSCRIBE_FAILED_LABEL: &str = "resubscribe_failed";

/// Per-topic plumbing produced by `GossipService::spawn` and handed to
/// the matching `subscriber_task`. Bundling these together is the single
/// point where the subscriber's view of a topic is constructed from
/// `topic.split()` — the `sender_slot` here is the same `Arc` that the
/// publisher's `senders` vec also holds, so the "subscriber writes the
/// sender the publisher reads" pairing is enforced by construction
/// rather than by parallel-vec-by-index discipline (#577 H2).
struct TopicWiring {
    name: String,
    id: TopicId,
    receiver: GossipReceiver,
    sender_slot: Arc<ArcSwap<GossipSender>>,
}

/// Configuration handed to [`GossipService::spawn`] by the consumer. The
/// peer-table TTL is configured on the `PeerTable` itself at construction
/// time, so it doesn't appear here.
#[derive(Debug, Clone)]
pub struct GossipRuntimeConfig {
    pub announce_interval_sec: u64,
    pub subscribe_global: bool,
    /// Optional region code (ISO 3166-1 alpha-2). If `Some`, the service
    /// publishes and subscribes on `cdn/region/{code}/v1` in addition to the
    /// global topic. Callers that want to publish MUST set this — the
    /// publisher is disabled (with a WARN) otherwise, since peers would
    /// reject a region-less announce at validation time. `decdn-node`
    /// enforces this at config resolution.
    pub region: Option<String>,
    /// Hex-validated set of permitted announcer node IDs. Empty = accept any.
    pub allowlist: HashSet<[u8; 32]>,
    /// Subscribe to (and, when a report drain is supplied, publish on) the
    /// global `cdn/reputation/v1` topic (ADR 008). Independent of `region`.
    pub subscribe_reputation: bool,
    /// Interval between reputation-report publish ticks (seconds). Matches the
    /// ADR 008 1-hour per-(reporter, node) rate limit by default.
    pub reputation_publish_interval_sec: u64,
}

/// Operator-facing handle to fire an immediate `NodeAnnounce` outside the
/// publisher task's normal interval (issue #280). Created by
/// [`GossipService::spawn`] when a region is configured (the publisher
/// runs); `None` when no region is set, since publishing without a region
/// fails peer-side validation and there's nothing useful for the trigger
/// to fire.
///
/// Backed by a [`Notify`]: `notify_one()` is a fire-and-forget signal that
/// the publisher task observes via `tokio::select!`. If the publisher is
/// already inside an interval-driven publish when the trigger fires, the
/// notification is queued for one additional pass (Notify's permit slot)
/// so a fast-firing operator script can't drop a manual announce on the
/// floor.
#[derive(Debug)]
pub struct AnnounceTrigger {
    notify: Arc<Notify>,
}

impl AnnounceTrigger {
    /// Ask the publisher task to broadcast a fresh `NodeAnnounce` on its
    /// next select boundary. Idempotent: if multiple calls land between
    /// publisher select boundaries, [`Notify`] coalesces them into a single
    /// extra publish.
    pub fn announce_now(&self) {
        self.notify.notify_one();
    }

    /// Construct an `AnnounceTrigger` around a caller-supplied
    /// [`Notify`]. Exposed so consumers (notably the node admin RPC unit
    /// tests) can observe that `announce_now` fires the underlying
    /// notify without spinning up a real gossip publisher. Outside of
    /// tests, callers should use [`GossipService::spawn`] which returns
    /// a fully-wired trigger.
    #[doc(hidden)]
    pub const fn for_test(notify: Arc<Notify>) -> Self {
        Self { notify }
    }
}

/// Handle to force an immediate reputation-report publish outside the
/// publisher's normal interval (admin / testing). Mirrors [`AnnounceTrigger`];
/// `None` when the reputation publisher isn't running (no report drain wired).
#[derive(Debug)]
pub struct ReputationPublishTrigger {
    notify: Arc<Notify>,
}

impl ReputationPublishTrigger {
    /// Ask the reputation publisher to drain and broadcast pending reports on
    /// its next select boundary. Coalesces like [`AnnounceTrigger::announce_now`].
    pub fn publish_now(&self) {
        self.notify.notify_one();
    }

    /// Construct around a caller-supplied [`Notify`] (test seam).
    #[doc(hidden)]
    pub const fn for_test(notify: Arc<Notify>) -> Self {
        Self { notify }
    }
}

/// Owns the gossip publisher/subscriber tasks.
#[derive(Debug)]
pub struct GossipService;

/// Reasons [`GossipService::spawn`] can fail in a way the caller should
/// treat as a startup error rather than soldiering on into a degraded
/// state. Distinguished from the "intentional subscribe-only" case
/// (caller configured neither global nor region) which still returns
/// `Ok` with empty handles.
#[derive(Debug, thiserror::Error)]
pub enum GossipSpawnError {
    /// At least one topic was configured, but every `subscribe()` call
    /// failed. The caller should bail rather than continue: a runtime
    /// that proceeds with no topics would later report
    /// `PUBLISHER_DISABLED` from `admin_v1_announce` for the wrong
    /// reason (region IS configured; gossip is just dead).
    #[error("gossip: every topic subscribe failed ({attempted} attempted); service cannot start")]
    AllSubscribesFailed {
        /// Number of distinct topics the caller asked to subscribe to.
        attempted: usize,
    },
}

/// Construct the per-node iroh-gossip actor with the deCDN frame
/// ceiling ([`crate::GOSSIP_MAX_FRAME`]) wired through
/// `iroh_gossip::net::Gossip::builder().max_message_size(...)` (ADR
/// 013 §Gossip Framing, #660). Pinning the cap here — rather than
/// inline at every call site — gives the regression test in this
/// module a single seam to assert against, so deleting the
/// `.max_message_size(...)` step fails the test loudly instead of
/// silently reverting to the iroh-gossip upstream default
/// (`iroh_gossip::proto::DEFAULT_MAX_MESSAGE_SIZE`, 4 KiB in iroh-
/// gossip 0.98 — below our `MAX_TRAILING_BYTES + envelope + wrappers`
/// floor).
pub fn build_gossip(endpoint: Endpoint) -> Gossip {
    Gossip::builder()
        .max_message_size(crate::GOSSIP_MAX_FRAME)
        .spawn(endpoint)
}

/// Returned by [`GossipService::spawn`]: the spawned task handles plus an
/// optional [`AnnounceTrigger`] for the publisher task. The trigger is
/// `None` when the publisher is disabled (no region configured) — see
/// [`GossipRuntimeConfig::region`].
#[derive(Debug)]
pub struct GossipHandles {
    /// Background tasks owned by the gossip service. They exit cooperatively
    /// when the [`CancellationToken`] passed to [`GossipService::spawn`] is
    /// cancelled; the caller cancels that token and then **awaits** these to
    /// confirm a clean drain (rather than reaching in with `.abort()`).
    pub tasks: Vec<JoinHandle<()>>,
    /// One-shot announce trigger for the publisher. `None` iff the
    /// publisher task wasn't spawned (region-less subscribe-only mode).
    pub announce_trigger: Option<Arc<AnnounceTrigger>>,
    /// Immediate-publish trigger for the reputation publisher. `None` iff that
    /// task wasn't spawned (reputation disabled or no report drain wired).
    pub reputation_publish_trigger: Option<Arc<ReputationPublishTrigger>>,
}

impl GossipService {
    /// Spawn publisher + subscriber tasks for the configured topics and
    /// return their `JoinHandle`s for the caller's shutdown path.
    ///
    /// Each topic is subscribed to exactly once; the returned
    /// [`iroh_gossip::api::GossipTopic`] is split into sender + receiver, so
    /// the publisher and subscriber loops share a single gossip state
    /// machine per topic. Subscribe failures are surfaced via
    /// [`GossipMetrics::inc_rejected`] with the `subscribe_failed` label and
    /// an error-level log line; the old code merely `warn!`'d and left the
    /// subscriber task silently dead.
    #[allow(
        clippy::too_many_arguments,
        clippy::needless_pass_by_value,
        clippy::cognitive_complexity
    )]
    pub async fn spawn(
        _endpoint: Endpoint,
        secret_key: SecretKey,
        gossip: Gossip,
        cfg: GossipRuntimeConfig,
        peer_table: Arc<RwLock<PeerTable>>,
        metrics: Arc<dyn GossipMetrics>,
        shutdown: CancellationToken,
        reputation: ReputationWiring,
    ) -> Result<GossipHandles, GossipSpawnError> {
        let topics = build_topic_list(&cfg);
        // The reputation topic is a single global topic independent of the
        // NodeAnnounce topology; spawn it on its own so the NodeAnnounce path
        // (and its `AllSubscribesFailed` contract) is untouched.
        let (reputation_tasks, reputation_publish_trigger) =
            spawn_reputation_tasks(&secret_key, &gossip, &cfg, &metrics, &shutdown, reputation)
                .await;

        if topics.is_empty() {
            // Caller configured neither global nor region — intentional
            // subscribe-only mode for NodeAnnounce. The reputation tasks (if
            // any) still run.
            if reputation_tasks.is_empty() {
                tracing::warn!("gossip: no topics to subscribe; service is idle");
            }
            return Ok(GossipHandles {
                tasks: reputation_tasks,
                announce_trigger: None,
                reputation_publish_trigger,
            });
        }

        let allowlist = Arc::new(cfg.allowlist.clone());
        let announce_interval = Duration::from_secs(cfg.announce_interval_sec);
        let region = cfg.region.clone();
        let attempted = topics.len();

        // Open one subscription per topic up front. Failures bump a metric
        // and emit an error log; the topic is then skipped so the caller
        // isn't left with half-wired state.
        //
        // Each topic owns one `Arc<ArcSwap<GossipSender>>` slot shared by
        // the subscriber's `TopicWiring` and the publisher's `senders`
        // vec (#577 H2). The subscriber's reconnect path stores the
        // fresh sender into its slot, and the publisher reads through
        // the slot on each broadcast — so the two halves can't
        // desynchronise after a stream reset. Pairing is single-site
        // here: both pushes derive from the same `Arc::clone(&slot)`.
        let mut senders: Vec<(String, Arc<ArcSwap<GossipSender>>)> = Vec::new();
        let mut wirings: Vec<TopicWiring> = Vec::new();
        for (topic_name, topic_id) in topics {
            match gossip.subscribe(topic_id, Vec::new()).await {
                Ok(topic) => {
                    let (sender, receiver) = topic.split();
                    let slot = Arc::new(ArcSwap::from_pointee(sender));
                    senders.push((topic_name.clone(), Arc::clone(&slot)));
                    wirings.push(TopicWiring {
                        name: topic_name,
                        id: topic_id,
                        receiver,
                        sender_slot: slot,
                    });
                }
                Err(err) => {
                    metrics.inc_rejected(SUBSCRIBE_FAILED_LABEL);
                    tracing::error!(
                        %err,
                        topic = %topic_name,
                        "gossip subscribe failed; topic skipped"
                    );
                }
            }
        }

        if wirings.is_empty() {
            // Caller asked for at least one topic and got none. This is
            // a startup failure the runtime must surface, not a soft
            // degraded state — see `GossipSpawnError::AllSubscribesFailed`.
            return Err(GossipSpawnError::AllSubscribesFailed { attempted });
        }

        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for wiring in wirings {
            handles.push(subscriber_task(
                wiring,
                gossip.clone(),
                Arc::clone(&allowlist),
                Arc::clone(&peer_table),
                Arc::clone(&metrics),
                shutdown.clone(),
            ));
        }

        // Skip publishing entirely when no region is configured: an announce
        // without a region fails peer-side validation, so emitting silence
        // beats emitting announces everyone drops.
        let announce_trigger = if let Some(region_code) = region {
            let notify = Arc::new(Notify::new());
            handles.push(publisher_task(
                secret_key,
                region_code,
                announce_interval,
                senders,
                Arc::clone(&metrics),
                Arc::clone(&notify),
                shutdown.clone(),
            ));
            Some(Arc::new(AnnounceTrigger { notify }))
        } else {
            tracing::warn!(
                "gossip: identity.region not set; publisher disabled (subscribe-only mode)"
            );
            None
        };

        handles.push(ttl_sweeper_task(
            Arc::clone(&peer_table),
            Arc::clone(&metrics),
            shutdown.clone(),
        ));

        handles.extend(reputation_tasks);

        Ok(GossipHandles {
            tasks: handles,
            announce_trigger,
            reputation_publish_trigger,
        })
    }
}

/// Optional reputation-topic wiring handed to [`GossipService::spawn`]. All
/// three are `None` to run without reputation gossip. The subscriber needs both
/// `sink` and `staked` to run; the publisher needs `report_drain`.
#[derive(Default)]
pub struct ReputationWiring {
    /// Consumer of validated inbound reports (the aggregator).
    pub sink: Option<Arc<dyn ReputationSink>>,
    /// Staked-reporter membership gate for inbound reports.
    pub staked: Option<Arc<dyn StakedReporterSet>>,
    /// Source of pending outbound reports for the publisher.
    pub report_drain: Option<Arc<dyn ReportDrain>>,
}

impl std::fmt::Debug for ReputationWiring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The trait-object fields don't implement Debug; report presence only.
        f.debug_struct("ReputationWiring")
            .field("sink", &self.sink.is_some())
            .field("staked", &self.staked.is_some())
            .field("report_drain", &self.report_drain.is_some())
            .finish()
    }
}

/// Subscribe to the reputation topic and spawn its subscriber (+ publisher when
/// a report drain is wired). Returns the spawned tasks and an optional publish
/// trigger. Failures to subscribe are logged + metered and yield no tasks —
/// reputation gossip is best-effort and never blocks node startup.
async fn spawn_reputation_tasks(
    secret_key: &SecretKey,
    gossip: &Gossip,
    cfg: &GossipRuntimeConfig,
    metrics: &Arc<dyn GossipMetrics>,
    shutdown: &CancellationToken,
    reputation: ReputationWiring,
) -> (Vec<JoinHandle<()>>, Option<Arc<ReputationPublishTrigger>>) {
    let ReputationWiring {
        sink,
        staked,
        report_drain,
    } = reputation;
    if !cfg.subscribe_reputation {
        return (Vec::new(), None);
    }
    let (Some(sink), Some(staked)) = (sink, staked) else {
        tracing::warn!(
            "gossip: subscribe_reputation set but no reputation sink/staker wired; \
             reputation topic not joined"
        );
        return (Vec::new(), None);
    };

    let id = topic_id(TOPIC_REPUTATION);
    let topic = match gossip.subscribe(id, Vec::new()).await {
        Ok(t) => t,
        Err(err) => {
            metrics.inc_rejected(SUBSCRIBE_FAILED_LABEL);
            tracing::error!(%err, topic = TOPIC_REPUTATION, "gossip reputation subscribe failed");
            return (Vec::new(), None);
        }
    };
    let (sender, receiver) = topic.split();
    let slot = Arc::new(ArcSwap::from_pointee(sender));

    let mut tasks = Vec::new();
    tasks.push(reputation_subscriber_task(
        receiver,
        Arc::clone(&slot),
        id,
        gossip.clone(),
        sink,
        staked,
        Arc::clone(metrics),
        shutdown.clone(),
    ));

    let trigger = report_drain.map(|drain| {
        let notify = Arc::new(Notify::new());
        tasks.push(reputation_publisher_task(
            secret_key.clone(),
            Duration::from_secs(cfg.reputation_publish_interval_sec),
            slot,
            drain,
            Arc::clone(metrics),
            Arc::clone(&notify),
            shutdown.clone(),
        ));
        Arc::new(ReputationPublishTrigger { notify })
    });

    (tasks, trigger)
}

/// Insert (or refresh) an already-validated announce into `table` and
/// emit the matching metric. Pulled out of [`subscriber_task`] so the
/// subscriber loop and the contract test exercise the *same* dispatch
/// code; a future change here (new `InsertOutcome` variant, metric
/// rename, gauge re-emission on a rejected arm) cannot drift between
/// production and test.
///
/// Caller holds the `PeerTable` write lock for the duration of this call.
pub(crate) fn dispatch_insert(
    table: &mut PeerTable,
    announce: decdn_protocol::NodeAnnounce,
    now_us: u64,
    metrics: &dyn GossipMetrics,
) {
    match table.insert_or_refresh(announce, now_us) {
        Ok(InsertOutcome::Inserted | InsertOutcome::Refreshed) => {
            metrics.set_peer_table_size(i64::try_from(table.len()).unwrap_or(i64::MAX));
        }
        // #577 H3: peer table is at its hard cap and the inline TTL
        // sweep couldn't free a slot. Drop the announce, bump the
        // rejection metric (no size update — the table didn't change).
        Ok(InsertOutcome::RejectedFull) => {
            metrics.inc_rejected(PEER_TABLE_FULL_LABEL);
        }
        // Exhaustive match on the typed error: adding a new
        // `PeerTable` failure mode is a compile error here, so a
        // future variant can't be silently mislabeled as
        // `stale_timestamp`.
        Err(crate::StaleTimestamp { .. }) => {
            metrics.inc_rejected(AnnounceReject::StaleTimestamp.label());
        }
    }
}

/// Build `(topic_name, TopicId)` pairs to subscribe to. `TopicId` is the
/// blake3 hash of the topic name, matching iroh-gossip's own convention for
/// deriving topic IDs from a string namespace.
fn build_topic_list(cfg: &GossipRuntimeConfig) -> Vec<(String, TopicId)> {
    let mut out = Vec::new();
    if cfg.subscribe_global {
        out.push((TOPIC_GLOBAL.to_string(), topic_id(TOPIC_GLOBAL)));
    }
    if let Some(region) = cfg.region.as_deref() {
        let name = format!("{TOPIC_REGION_PREFIX}{region}/v1");
        let id = topic_id(&name);
        out.push((name, id));
    }
    out
}

fn topic_id(name: &str) -> TopicId {
    let hash = blake3::hash(name.as_bytes());
    TopicId::from_bytes(*hash.as_bytes())
}

/// Maximum backoff between reconnection attempts (1 minute).
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_mins(1);

/// Initial backoff after the first stream drop (1 second).
const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_secs(1);

fn subscriber_task(
    wiring: TopicWiring,
    gossip: Gossip,
    allowlist: Arc<HashSet<[u8; 32]>>,
    peer_table: Arc<RwLock<PeerTable>>,
    metrics: Arc<dyn GossipMetrics>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    // Destructure once so the rest of the body reads as if the fields had
    // always been positional args. `sender_slot` is the publisher's view
    // of this topic's `GossipSender`; storing into it on reconnect is the
    // #577 H2 fix.
    let TopicWiring {
        name: topic_name,
        id: topic_id,
        receiver: initial_receiver,
        sender_slot,
    } = wiring;
    tokio::spawn(async move {
        let mut receiver = initial_receiver;
        let mut backoff = RECONNECT_INITIAL_BACKOFF;

        loop {
            // Consume events until the stream terminates or shutdown is
            // requested. `biased` checks cancellation first so a pending
            // shutdown wins over a ready event and the task exits at a clean
            // boundary instead of being aborted mid-await.
            loop {
                let event = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        tracing::debug!(topic = %topic_name, "gossip subscriber stopping on shutdown");
                        return;
                    }
                    ev = receiver.next() => ev,
                };
                let Some(event) = event else { break };
                // Reset backoff on any successful receive — the connection is healthy.
                backoff = RECONNECT_INITIAL_BACKOFF;

                let msg = match event {
                    Ok(iroh_gossip::api::Event::Received(m)) => m,
                    Ok(
                        iroh_gossip::api::Event::NeighborUp(_)
                        | iroh_gossip::api::Event::NeighborDown(_)
                        | iroh_gossip::api::Event::Lagged,
                    ) => continue,
                    Err(err) => {
                        tracing::debug!(%err, topic = %topic_name, "gossip receive error");
                        continue;
                    }
                };
                metrics.inc_received(&topic_name);
                // Snapshot the clock once so validation and peer-table insert
                // share the same timestamp (avoids a race if the system clock
                // moves between the two reads) and halves the syscall cost.
                let now = now_us();
                match validate_envelope(&msg.content, now, allowlist.as_ref()) {
                    Ok(announce) => {
                        let mut table = peer_table.write().await;
                        dispatch_insert(&mut table, announce, now, metrics.as_ref());
                    }
                    Err(reject) => {
                        // #577 M3: oversize-trailing-bytes is an active-attack
                        // signal (amplification probe). Surface forensic
                        // context — size + threshold + topic — alongside the
                        // metric so operators investigating the counter spike
                        // have correlation data without enabling debug logs.
                        // Other reject reasons stay metric-only to avoid log
                        // floods on routine per-peer wire faults.
                        if matches!(reject, AnnounceReject::OversizeTrailingBytes) {
                            tracing::warn!(
                                topic = %topic_name,
                                content_len = msg.content.len(),
                                max_trailing = crate::validation::MAX_TRAILING_BYTES,
                                "gossip envelope rejected: oversize trailing bytes (#577 M3) — possible amplification probe"
                            );
                        }
                        metrics.inc_rejected(reject.label());
                    }
                }
            }

            // Stream terminated — enter reconnection loop.
            tracing::warn!(topic = %topic_name, "gossip subscription stream ended; reconnecting");

            loop {
                // Cancellable backoff: without this arm a shutdown during the
                // reconnect wait would stall up to RECONNECT_MAX_BACKOFF (1 min)
                // before the task could exit. `biased` favours cancellation.
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        tracing::debug!(
                            topic = %topic_name,
                            "gossip subscriber stopping on shutdown during reconnect backoff"
                        );
                        return;
                    }
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);

                match gossip.subscribe(topic_id, Vec::new()).await {
                    Ok(topic) => {
                        // #577 H2: store the *new* sender into the shared slot
                        // before resuming. The publisher's next broadcast loads
                        // through the slot and sees this sender; a regression
                        // that drops this `store` is caught by the
                        // `publisher_sees_swapped_sender_after_reconnect` test
                        // in this module.
                        let (new_sender, new_receiver) = topic.split();
                        sender_slot.store(Arc::new(new_sender));
                        receiver = new_receiver;
                        metrics.inc_reconnected(&topic_name);
                        tracing::info!(topic = %topic_name, "gossip subscription reconnected");
                        break; // Exit reconnection loop, back to event processing.
                    }
                    Err(err) => {
                        // Bump the rejection counter alongside the error log
                        // so operators can alert on a node stuck in a
                        // reconnect loop without scraping logs. Pre-fix
                        // this path was log-only — diagnosable but not
                        // operationally observable.
                        metrics.inc_rejected(RESUBSCRIBE_FAILED_LABEL);
                        tracing::error!(
                            %err,
                            topic = %topic_name,
                            "gossip resubscribe failed; will retry"
                        );
                    }
                }
            }
        }
    })
}

fn publisher_task(
    secret_key: SecretKey,
    region_code: String,
    interval: Duration,
    // Per-topic sender slots shared with the subscriber tasks. Each
    // broadcast iteration calls `slot.load_full()` to pick up the latest
    // sender, so a subscriber-driven reconnect (which swaps a fresh
    // sender into the slot) is observed on the next publish without
    // dropping a manual `announce_now()` trigger on the floor — #577 H2.
    senders: Vec<(String, Arc<ArcSwap<GossipSender>>)>,
    metrics: Arc<dyn GossipMetrics>,
    announce_now: Arc<Notify>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let node_id = *secret_key.public().as_bytes();

        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            // Both arms drive the same publish; the trigger arm exists so
            // operators running `decdn node announce` (issue #280) can push
            // a fresh announce immediately rather than waiting up to
            // `interval` seconds for the next periodic broadcast. Only
            // `timestamp_us` varies between iterations, but a forced
            // re-announce still matters — it refreshes peers' TTL on this
            // node's entry and lets a freshly-started operator surface in
            // peer tables without waiting a full interval. `region_code`
            // is captured by-value above and does not re-read from config
            // inside this loop — changing region requires a restart,
            // which respawns the publisher and obviates the trigger
            // anyway.
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    tracing::debug!("gossip publisher stopping on shutdown");
                    return;
                }
                _ = ticker.tick() => {}
                () = announce_now.notified() => {}
            }
            let ts = now_us();
            let body = NodeAnnounceBody {
                node_id,
                region: region_code.clone(),
                timestamp_us: ts,
            };
            let signing_bytes = match body.signing_bytes() {
                Ok(b) => b,
                Err(err) => {
                    tracing::warn!(%err, "gossip publisher body encode failed");
                    continue;
                }
            };
            let signature = secret_key.sign(&signing_bytes).to_bytes().to_vec();
            let env = GossipEnvelope {
                version: GOSSIP_VERSION,
                payload: GossipPayload::NodeAnnounce(NodeAnnounce { body, signature }),
            };
            let encoded = match postcard::to_allocvec(&env) {
                Ok(b) => Bytes::from(b),
                Err(err) => {
                    tracing::warn!(%err, "gossip publisher envelope encode failed");
                    continue;
                }
            };
            for (name, slot) in &senders {
                // Load through the slot every iteration so a subscriber-
                // driven reconnect (which swaps a fresh `GossipSender`
                // into this slot) is picked up here without a publisher
                // restart. Pre-#577-H2, the publisher captured the
                // original sender at spawn and silently kept broadcasting
                // through it even after the iroh-gossip-side receiver
                // disappeared.
                let sender = slot.load_full();
                if let Err(err) = sender.broadcast(encoded.clone()).await {
                    // `warn!` rather than `debug!`: a broadcast failure on
                    // the manual-trigger path (operator running `decdn node
                    // announce`) would otherwise be invisible at default
                    // log levels, while the admin RPC happily returned
                    // `triggered: true`. Operators investigating "why
                    // didn't my announce land" need this in the default
                    // log stream.
                    tracing::warn!(%err, topic = %name, "gossip publish failed");
                } else {
                    metrics.inc_published(name);
                }
            }
        }
    })
}

/// Subscriber for the `cdn/reputation/v1` topic. Mirrors [`subscriber_task`]'s
/// receive + reconnect structure, but validates with
/// [`validate_reputation_envelope`], receiver-enforces the ADR 008 rate limits
/// via a task-owned [`ReputationRateLimiter`], and forwards accepted reports to
/// the [`ReputationSink`] instead of the peer table.
#[allow(clippy::too_many_arguments)]
fn reputation_subscriber_task(
    initial_receiver: GossipReceiver,
    sender_slot: Arc<ArcSwap<GossipSender>>,
    topic_id: TopicId,
    gossip: Gossip,
    sink: Arc<dyn ReputationSink>,
    staked: Arc<dyn StakedReporterSet>,
    metrics: Arc<dyn GossipMetrics>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut receiver = initial_receiver;
        let mut backoff = RECONNECT_INITIAL_BACKOFF;
        let mut rate_limiter = ReputationRateLimiter::new();

        loop {
            loop {
                let event = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        tracing::debug!(topic = TOPIC_REPUTATION, "reputation subscriber stopping on shutdown");
                        return;
                    }
                    ev = receiver.next() => ev,
                };
                let Some(event) = event else { break };
                backoff = RECONNECT_INITIAL_BACKOFF;

                let msg = match event {
                    Ok(iroh_gossip::api::Event::Received(m)) => m,
                    Ok(
                        iroh_gossip::api::Event::NeighborUp(_)
                        | iroh_gossip::api::Event::NeighborDown(_)
                        | iroh_gossip::api::Event::Lagged,
                    ) => continue,
                    Err(err) => {
                        tracing::debug!(%err, topic = TOPIC_REPUTATION, "reputation receive error");
                        continue;
                    }
                };
                metrics.inc_received(TOPIC_REPUTATION);
                let now = now_secs();
                match validate_reputation_envelope(&msg.content, now, staked.as_ref()) {
                    Ok(report) => {
                        // ADR 008 §Rate Limiting: enforce on the receiver before
                        // the report reaches the aggregator. Dropped reports are
                        // metered, never forwarded.
                        match rate_limiter.check_and_record(report.reporter, report.provider, now) {
                            Ok(()) => sink.accept(report),
                            Err(reject) => metrics.inc_rejected(reject.label()),
                        }
                    }
                    Err(reject) => metrics.inc_rejected(reject.label()),
                }
            }

            tracing::warn!(
                topic = TOPIC_REPUTATION,
                "reputation subscription stream ended; reconnecting"
            );
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        tracing::debug!(topic = TOPIC_REPUTATION, "reputation subscriber stopping during reconnect backoff");
                        return;
                    }
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);
                match gossip.subscribe(topic_id, Vec::new()).await {
                    Ok(topic) => {
                        let (new_sender, new_receiver) = topic.split();
                        sender_slot.store(Arc::new(new_sender));
                        receiver = new_receiver;
                        metrics.inc_reconnected(TOPIC_REPUTATION);
                        tracing::info!(
                            topic = TOPIC_REPUTATION,
                            "reputation subscription reconnected"
                        );
                        break;
                    }
                    Err(err) => {
                        metrics.inc_rejected(RESUBSCRIBE_FAILED_LABEL);
                        tracing::error!(%err, topic = TOPIC_REPUTATION, "reputation resubscribe failed; will retry");
                    }
                }
            }
        }
    })
}

/// Publisher for the `cdn/reputation/v1` topic. On each interval tick or
/// immediate trigger it drains the [`ReportDrain`], caps the batch to the ADR
/// 008 per-reporter hourly limit, signs each [`ReputationReportBody`], and
/// broadcasts it. Mirrors [`publisher_task`]'s load-through-slot reconnect
/// behaviour (#577 H2).
fn reputation_publisher_task(
    secret_key: SecretKey,
    interval: Duration,
    sender_slot: Arc<ArcSwap<GossipSender>>,
    drain: Arc<dyn ReportDrain>,
    metrics: Arc<dyn GossipMetrics>,
    publish_now: Arc<Notify>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let reporter = *secret_key.public().as_bytes();
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    tracing::debug!("reputation publisher stopping on shutdown");
                    return;
                }
                _ = ticker.tick() => {}
                () = publish_now.notified() => {}
            }
            let mut pending = drain.drain();
            if pending.len() > MAX_REPORTS_PER_REPORTER_PER_HR {
                // Honour the 10/reporter/hr cap when the tick equals the 1h
                // window; surface the drop so a saturated buffer is observable.
                let dropped = pending.len() - MAX_REPORTS_PER_REPORTER_PER_HR;
                tracing::warn!(
                    dropped,
                    cap = MAX_REPORTS_PER_REPORTER_PER_HR,
                    "reputation publisher capped outbound batch"
                );
                pending.truncate(MAX_REPORTS_PER_REPORTER_PER_HR);
            }
            let ts = now_secs();
            for (provider, metrics_payload) in pending {
                let body = ReputationReportBody {
                    provider,
                    reporter,
                    metrics: metrics_payload,
                    timestamp_secs: ts,
                };
                let signing_bytes = match body.signing_bytes() {
                    Ok(b) => b,
                    Err(err) => {
                        tracing::warn!(%err, "reputation publisher body encode failed");
                        continue;
                    }
                };
                let signature = secret_key.sign(&signing_bytes).to_bytes().to_vec();
                let env = GossipEnvelope {
                    version: GOSSIP_VERSION,
                    payload: GossipPayload::ReputationReport(ReputationReport { body, signature }),
                };
                let encoded = match postcard::to_allocvec(&env) {
                    Ok(b) => Bytes::from(b),
                    Err(err) => {
                        tracing::warn!(%err, "reputation publisher envelope encode failed");
                        continue;
                    }
                };
                let sender = sender_slot.load_full();
                if let Err(err) = sender.broadcast(encoded).await {
                    tracing::warn!(%err, topic = TOPIC_REPUTATION, "reputation publish failed");
                } else {
                    metrics.inc_published(TOPIC_REPUTATION);
                }
            }
        }
    })
}

fn ttl_sweeper_task(
    peer_table: Arc<RwLock<PeerTable>>,
    metrics: Arc<dyn GossipMetrics>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    tracing::debug!("gossip TTL sweeper stopping on shutdown");
                    return;
                }
                _ = ticker.tick() => {}
            }
            let mut table = peer_table.write().await;
            let evicted = table.evict_expired(now_us());
            if evicted > 0 {
                metrics.set_peer_table_size(i64::try_from(table.len()).unwrap_or(i64::MAX));
            }
        }
    })
}

/// Set on the first observed `SystemTime::duration_since(UNIX_EPOCH)` failure
/// so a broken clock only logs once rather than at every publish/receive.
static CLOCK_ERROR_LOGGED: AtomicBool = AtomicBool::new(false);

fn now_us() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => u64::try_from(d.as_micros()).unwrap_or(u64::MAX),
        Err(err) => {
            if !CLOCK_ERROR_LOGGED.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    %err,
                    "gossip: system clock is before UNIX epoch; announces will be rejected as ClockSkew by peers"
                );
            }
            0
        }
    }
}

/// Wall-clock seconds since the Unix epoch, for reputation report timestamps
/// and the receiver-side recency / rate-limit windows (ADR 008 works in
/// seconds, unlike `NodeAnnounce`'s microseconds).
fn now_secs() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(err) => {
            if !CLOCK_ERROR_LOGGED.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    %err,
                    "gossip: system clock is before UNIX epoch; reputation reports will be rejected by peers"
                );
            }
            0
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    fn cfg(subscribe_global: bool, region: Option<&str>) -> GossipRuntimeConfig {
        GossipRuntimeConfig {
            announce_interval_sec: 60,
            subscribe_global,
            region: region.map(String::from),
            allowlist: HashSet::new(),
            subscribe_reputation: false,
            reputation_publish_interval_sec: 3600,
        }
    }

    #[test]
    fn build_topic_list_empty_when_neither_global_nor_region() {
        // `GossipService::spawn` short-circuits to an empty handle vec when
        // `build_topic_list` returns empty. This test locks that input
        // behavior without needing a live `Gossip` instance.
        assert!(build_topic_list(&cfg(false, None)).is_empty());
    }

    #[test]
    fn build_topic_list_global_only() {
        let topics = build_topic_list(&cfg(true, None));
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].0, "cdn/global/v1");
    }

    #[test]
    fn build_topic_list_region_only() {
        let topics = build_topic_list(&cfg(false, Some("US")));
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].0, "cdn/region/US/v1");
    }

    #[test]
    fn build_topic_list_both() {
        let topics = build_topic_list(&cfg(true, Some("US")));
        assert_eq!(topics.len(), 2);
        assert_eq!(topics[0].0, "cdn/global/v1");
        assert_eq!(topics[1].0, "cdn/region/US/v1");
    }

    /// #577 H2 — locks the architectural property the fix relies on:
    /// the publisher reads its sender *through* the per-topic
    /// `Arc<ArcSwap<...>>` slot on every iteration, so a subscriber-
    /// driven reconnect that swaps a fresh value into the slot is
    /// observed on the next publisher iteration. A regression that
    /// captured the sender by value at spawn (as the pre-fix code did,
    /// silently dropping manual / interval announces after a stream
    /// reset) would fail this test.
    ///
    /// Stand-in `&'static str` instead of a real `GossipSender` because
    /// the latter can't be constructed without a live
    /// `iroh_gossip::net::Gossip` instance. This test covers the
    /// load-through-slot primitive only; a full integration test would
    /// spin up two `Gossip` instances, force a stream reset, and assert
    /// the next publish lands — that's tracked separately, not blocking
    /// this PR.
    #[test]
    fn publisher_sees_swapped_sender_after_reconnect() {
        let slot: Arc<ArcSwap<&'static str>> = Arc::new(ArcSwap::from_pointee("initial"));

        // Simulate the publisher's first iteration after spawn.
        assert_eq!(*slot.load_full(), "initial");

        // Simulate the subscriber's reconnect arm storing a fresh value
        // into the same slot. The `Arc::clone(&slot)` mirrors how the
        // spawn code hands the same Arc to both tasks.
        let subscriber_view = Arc::clone(&slot);
        subscriber_view.store(Arc::new("after_reconnect"));

        // Publisher's next iteration must observe the swap. A regression
        // that captured the sender into a local would still return
        // "initial" here, demonstrating the bug.
        assert_eq!(
            *slot.load_full(),
            "after_reconnect",
            "publisher must read through the slot, not capture the sender at spawn"
        );
    }

    /// `AnnounceTrigger::announce_now()` must actually wake a waiting
    /// `notified()` future on the same `Notify`. This is the contract
    /// `publisher_task`'s `tokio::select!` arm depends on — a regression
    /// that re-defined `announce_now` to (say) call `notify_waiters()`
    /// or do nothing would silently break manual announces.
    #[tokio::test]
    async fn announce_trigger_announce_now_wakes_notified() {
        let notify = Arc::new(Notify::new());
        let trigger = AnnounceTrigger::for_test(Arc::clone(&notify));

        // Park a `notified()` future on a separate task so the trigger
        // sees a registered waiter when `announce_now` calls `notify_one`.
        let waiter_notify = Arc::clone(&notify);
        let waiter = tokio::spawn(async move { waiter_notify.notified().await });

        // Yield so the spawned task reaches `notified().await`.
        tokio::time::sleep(Duration::from_millis(10)).await;

        trigger.announce_now();

        tokio::time::timeout(Duration::from_millis(200), waiter)
            .await
            .expect("waiter should resolve within 200ms of announce_now")
            .expect("waiter task should complete cleanly");
    }

    /// #577 H3 — locks the metric contract of [`dispatch_insert`]:
    /// (a) `RejectedFull` bumps `inc_rejected(PEER_TABLE_FULL_LABEL)`
    /// exactly once per drop and (b) does NOT re-publish the size
    /// gauge. The subscriber loop calls the same `dispatch_insert`
    /// function, so the test exercises production code directly — no
    /// inline mirror to drift.
    #[test]
    fn peer_table_full_dispatch_increments_reject_label_and_skips_size_gauge() {
        use std::sync::Mutex;

        use decdn_protocol::{NodeAnnounce, NodeAnnounceBody};

        use crate::PeerTable;

        #[derive(Debug, Default)]
        struct Recorder {
            inner: Mutex<RecorderState>,
        }
        #[derive(Debug, Default)]
        struct RecorderState {
            reject_labels: Vec<&'static str>,
            size_gauge_writes: Vec<i64>,
        }
        impl GossipMetrics for Recorder {
            fn inc_published(&self, _topic: &str) {}
            fn inc_received(&self, _topic: &str) {}
            fn inc_rejected(&self, reason: &'static str) {
                // The test module already opts into `unwrap_used`. Don't
                // swallow `PoisonError` with `if let Ok`: silently dropping
                // recorded events would surface as a confusing "wrong
                // event count" later instead of pointing at the original
                // panic that poisoned the mutex.
                self.inner.lock().unwrap().reject_labels.push(reason);
            }
            fn set_peer_table_size(&self, n: i64) {
                self.inner.lock().unwrap().size_gauge_writes.push(n);
            }
            fn inc_reconnected(&self, _topic: &str) {}
        }

        fn mk_announce(id: u8, ts_us: u64) -> NodeAnnounce {
            NodeAnnounce {
                body: NodeAnnounceBody {
                    node_id: [id; 32],
                    region: "US".to_string(),
                    timestamp_us: ts_us,
                },
                signature: vec![0u8; 64],
            }
        }

        let mut table = PeerTable::new(0, 2);
        let metrics = Recorder::default();
        // ttl=0 isolates the cap branch from any inline-sweep
        // interaction. Three distinct ids; the third hits the cap.
        dispatch_insert(&mut table, mk_announce(1, 1), 100, &metrics);
        dispatch_insert(&mut table, mk_announce(2, 1), 100, &metrics);
        dispatch_insert(&mut table, mk_announce(3, 1), 100, &metrics);

        let state = metrics.inner.lock().unwrap();
        assert_eq!(
            state.reject_labels.as_slice(),
            &[PEER_TABLE_FULL_LABEL],
            "exactly one rejection with the peer_table_full label"
        );
        assert_eq!(
            state.size_gauge_writes,
            vec![1, 2],
            "the size gauge fires only on the two accepted inserts (1 then 2); the rejected \
             third insert must not re-publish the gauge"
        );
        assert_eq!(table.len(), 2);
        assert!(table.get(&[1u8; 32]).is_some());
        assert!(table.get(&[2u8; 32]).is_some());
        assert!(table.get(&[3u8; 32]).is_none());
    }

    /// Back-to-back `announce_now()` calls before the publisher consumes
    /// the permit must coalesce into a single extra publish — Notify's
    /// stored-permit semantics. Locks in the doc-comment claim on
    /// `AnnounceTrigger` so an operator who automation-loops
    /// `decdn node announce` 100x doesn't get 100 broadcasts.
    #[tokio::test]
    async fn announce_trigger_coalesces_back_to_back_calls() {
        let notify = Arc::new(Notify::new());
        let trigger = AnnounceTrigger::for_test(Arc::clone(&notify));

        // Three triggers before any waiter — Notify stores at most one
        // permit. The first `notified()` claims the permit; the second
        // would have to wait for a fresh `notify_one()`.
        trigger.announce_now();
        trigger.announce_now();
        trigger.announce_now();

        let first = tokio::time::timeout(Duration::from_millis(50), notify.notified()).await;
        assert!(
            first.is_ok(),
            "first notified() should claim the stored permit"
        );

        let second = tokio::time::timeout(Duration::from_millis(50), notify.notified()).await;
        assert!(
            second.is_err(),
            "second notified() must NOT resolve — three calls coalesce to one permit"
        );
    }

    /// #660 — exercises [`build_gossip`], the single seam every
    /// production-mode caller uses to construct an iroh-gossip actor,
    /// and asserts the resulting `Gossip` reports the deCDN-controlled
    /// [`GOSSIP_MAX_FRAME`] through `Gossip::max_message_size()`. The
    /// runtime calls the same helper (`crates/node/src/runtime/mod.rs`),
    /// so a regression that drops `.max_message_size(...)` from the
    /// helper fails this test rather than silently reverting to
    /// `iroh_gossip::proto::DEFAULT_MAX_MESSAGE_SIZE` (below our
    /// `MAX_TRAILING_BYTES + envelope + wrappers` floor).
    ///
    /// Also pins both iroh-gossip 0.98 API surfaces this PR depends on
    /// (`Builder::max_message_size` setter and `Gossip::max_message_size`
    /// accessor) at compile/run time — an upstream rename or removal
    /// fails compilation here. A full multi-node behavioral test of
    /// `read_lp` rejecting an oversized frame is intentionally out of
    /// scope: iroh-gossip 0.98 exposes no in-process seam for that
    /// path (the `net::util` module is `pub(crate)`), so it requires
    /// the two-`Gossip`-instance harness deferred alongside other
    /// integration tests. The compile-time `const _: () = assert!`
    /// in `validation.rs` already enforces the floor invariant.
    #[tokio::test]
    async fn build_gossip_wires_gossip_max_frame() {
        use iroh::Endpoint;
        use iroh::endpoint::presets;
        use iroh_gossip::proto::MIN_MAX_MESSAGE_SIZE;

        let ep = Endpoint::builder(presets::Minimal)
            .bind()
            .await
            .expect("bind minimal endpoint");
        let gossip = build_gossip(ep);

        assert_eq!(
            gossip.max_message_size(),
            crate::GOSSIP_MAX_FRAME,
            "build_gossip must thread GOSSIP_MAX_FRAME into iroh-gossip's builder"
        );

        // Compile-time sanity: above iroh-gossip's panic floor and
        // strictly below the 16 MiB mental model PR #659 mistakenly
        // anchored on (deCDN's `MAX_MESSAGE_SIZE`, which applies to
        // `cdn/probe/v1` / `cdn/client/v1` — *not* gossip).
        const {
            assert!(crate::GOSSIP_MAX_FRAME >= MIN_MAX_MESSAGE_SIZE);
            assert!(crate::GOSSIP_MAX_FRAME < 16 * 1024 * 1024);
        }
    }

    /// Spawn the gossip service for `cfg`, let the tasks reach their await
    /// points, cancel the token, and assert every handle joins cleanly
    /// within 2s. Returns the number of tasks spawned. Shared by the two
    /// cooperative-shutdown (#805) cases below. Before the fix the loops
    /// never returned, so the join would hang until the 2s timeout.
    async fn assert_all_tasks_drain_on_cancel(cfg: GossipRuntimeConfig) -> usize {
        use iroh::endpoint::presets;

        let ep = Endpoint::builder(presets::Minimal)
            .bind()
            .await
            .expect("bind minimal endpoint");
        let gossip = build_gossip(ep.clone());
        let peer_table = Arc::new(RwLock::new(PeerTable::new(60_000_000, 128)));
        let metrics: Arc<dyn GossipMetrics> = Arc::new(crate::metrics::NoopMetrics);
        let shutdown = CancellationToken::new();

        let handles = GossipService::spawn(
            ep,
            SecretKey::generate(),
            gossip,
            cfg,
            peer_table,
            metrics,
            shutdown.clone(),
            ReputationWiring::default(),
        )
        .await
        .expect("gossip service should start")
        .tasks;
        let spawned = handles.len();

        // Let the tasks reach their await points, then request shutdown.
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.cancel();

        for handle in handles {
            tokio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("gossip task must exit promptly after cancel, not hang")
                .expect("gossip task must exit cleanly, not panic");
        }
        spawned
    }

    /// #805 — cooperative shutdown, full topology (global + region):
    /// cancelling the [`CancellationToken`] must make every spawned task
    /// (one subscriber per topic, the publisher, and the TTL sweeper)
    /// return at its next await boundary, so the runtime can join the
    /// handles on drain instead of reaching in with `.abort()`.
    ///
    /// Coverage note: with a peer-less Minimal endpoint the subscription
    /// stream never terminates, so each subscriber is cancelled at its
    /// steady-state `receiver.next()` await. The reconnect-backoff cancel
    /// arm uses the identical `biased; cancelled => return` shape but is
    /// not driven directly here — forcing a mid-reconnect cancel needs the
    /// two-`Gossip` harness deferred alongside the other integration tests
    /// (see `build_gossip_wires_gossip_max_frame`).
    #[tokio::test]
    async fn cancelling_token_stops_all_gossip_tasks() {
        let spawned = assert_all_tasks_drain_on_cancel(cfg(true, Some("US"))).await;
        assert!(
            spawned >= 3,
            "expected a subscriber per topic + publisher + TTL sweeper, got {spawned}"
        );
    }

    /// Reputation wiring spawns the subscriber + publisher tasks alongside the
    /// `NodeAnnounce` topology, returns a publish trigger, and every task drains
    /// cooperatively on cancel (#805 discipline extended to the reputation
    /// tasks). Stub sink/staker/drain stand in for the node-side impls.
    #[tokio::test]
    async fn reputation_tasks_spawn_and_drain() {
        use iroh::endpoint::presets;

        use crate::reputation::ValidatedReport;

        struct Sink;
        impl ReputationSink for Sink {
            fn accept(&self, _report: ValidatedReport) {}
        }
        struct Staked;
        impl StakedReporterSet for Staked {
            fn contains(&self, _reporter: &[u8; 32]) -> bool {
                true
            }
        }
        struct Drain;
        impl ReportDrain for Drain {
            fn drain(&self) -> Vec<([u8; 32], decdn_protocol::ReportMetrics)> {
                Vec::new()
            }
        }

        let ep = Endpoint::builder(presets::Minimal)
            .bind()
            .await
            .expect("bind minimal endpoint");
        let gossip = build_gossip(ep.clone());
        let peer_table = Arc::new(RwLock::new(PeerTable::new(60_000_000, 128)));
        let metrics: Arc<dyn GossipMetrics> = Arc::new(crate::metrics::NoopMetrics);
        let shutdown = CancellationToken::new();
        let mut config = cfg(true, Some("US"));
        config.subscribe_reputation = true;
        let wiring = ReputationWiring {
            sink: Some(Arc::new(Sink)),
            staked: Some(Arc::new(Staked)),
            report_drain: Some(Arc::new(Drain)),
        };

        let handles = GossipService::spawn(
            ep,
            SecretKey::generate(),
            gossip,
            config,
            peer_table,
            metrics,
            shutdown.clone(),
            wiring,
        )
        .await
        .expect("gossip service should start");

        assert!(
            handles.reputation_publish_trigger.is_some(),
            "publisher wired ⇒ trigger returned"
        );
        // global sub + region sub + publisher + TTL sweeper + reputation sub +
        // reputation publisher.
        assert_eq!(
            handles.tasks.len(),
            6,
            "expected the two reputation tasks alongside the NodeAnnounce topology"
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.cancel();
        for handle in handles.tasks {
            tokio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("gossip task must exit promptly after cancel, not hang")
                .expect("gossip task must exit cleanly, not panic");
        }
    }

    /// #805 — cooperative shutdown of the subscribe-only topology
    /// (`region: None` ⇒ the publisher is not spawned). This is the
    /// default node configuration and the one the `sighup_signal`
    /// integration tests do *not* exercise (they configure no topics, so
    /// `spawn` short-circuits to zero handles). The task set differs from
    /// the full case — no publisher — so this guards that cancellation
    /// still drains every handle. Exactly two tasks: the global subscriber
    /// and the TTL sweeper.
    #[tokio::test]
    async fn cancelling_token_stops_subscribe_only_gossip() {
        let spawned = assert_all_tasks_drain_on_cancel(cfg(true, None)).await;
        assert_eq!(
            spawned, 2,
            "subscribe-only mode must spawn the global subscriber + TTL sweeper only (no publisher)"
        );
    }
}
