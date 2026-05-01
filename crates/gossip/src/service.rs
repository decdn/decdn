//! Gossip publisher + subscriber service wiring iroh-gossip to the peer table.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use decdn_protocol::{
    GOSSIP_VERSION, GossipEnvelope, GossipPayload, LoadHint, NodeAnnounce, NodeAnnounceBody,
    TOPIC_GLOBAL, TOPIC_REGION_PREFIX,
};
use iroh::{Endpoint, SecretKey};
use iroh_gossip::api::{GossipReceiver, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use crate::{AnnounceReject, GossipMetrics, InsertOutcome, PeerTable, validate_envelope};

/// Label passed to [`GossipMetrics::inc_rejected`] when a topic subscribe
/// call fails at startup. Pinned by the label-stability test in
/// `validation::tests` so a rename fails CI.
pub(crate) const SUBSCRIBE_FAILED_LABEL: &str = "subscribe_failed";

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

/// Returned by [`GossipService::spawn`]: the spawned task handles plus an
/// optional [`AnnounceTrigger`] for the publisher task. The trigger is
/// `None` when the publisher is disabled (no region configured) — see
/// [`GossipRuntimeConfig::region`].
#[derive(Debug)]
pub struct GossipHandles {
    /// Background tasks owned by the gossip service. Caller is responsible
    /// for awaiting / aborting these during shutdown.
    pub tasks: Vec<JoinHandle<()>>,
    /// One-shot announce trigger for the publisher. `None` iff the
    /// publisher task wasn't spawned (region-less subscribe-only mode).
    pub announce_trigger: Option<Arc<AnnounceTrigger>>,
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
    ) -> Result<GossipHandles, GossipSpawnError> {
        let topics = build_topic_list(&cfg);
        if topics.is_empty() {
            // Caller configured neither global nor region — intentional
            // subscribe-only mode. Returning `Ok` with empty handles
            // matches the documented "subscribe-only by config" state;
            // operators see one warn at startup and `admin_v1_announce`
            // returns `PUBLISHER_DISABLED` later (the right reason).
            tracing::warn!("gossip: no topics to subscribe; service is idle");
            return Ok(GossipHandles {
                tasks: Vec::new(),
                announce_trigger: None,
            });
        }

        let allowlist = Arc::new(cfg.allowlist.clone());
        let announce_interval = Duration::from_secs(cfg.announce_interval_sec);
        let region = cfg.region.clone();
        let attempted = topics.len();

        // Open one subscription per topic up front. Failures bump a metric
        // and emit an error log; the topic is then skipped so the caller
        // isn't left with half-wired state.
        let mut senders: Vec<(String, GossipSender)> = Vec::new();
        let mut receivers: Vec<(String, TopicId, GossipReceiver)> = Vec::new();
        for (topic_name, topic_id) in topics {
            match gossip.subscribe(topic_id, Vec::new()).await {
                Ok(topic) => {
                    let (sender, receiver) = topic.split();
                    senders.push((topic_name.clone(), sender));
                    receivers.push((topic_name, topic_id, receiver));
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

        if receivers.is_empty() {
            // Caller asked for at least one topic and got none. This is
            // a startup failure the runtime must surface, not a soft
            // degraded state — see `GossipSpawnError::AllSubscribesFailed`.
            return Err(GossipSpawnError::AllSubscribesFailed { attempted });
        }

        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for (topic_name, tid, receiver) in receivers {
            handles.push(subscriber_task(
                topic_name,
                receiver,
                gossip.clone(),
                tid,
                Arc::clone(&allowlist),
                Arc::clone(&peer_table),
                Arc::clone(&metrics),
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
        ));

        Ok(GossipHandles {
            tasks: handles,
            announce_trigger,
        })
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

/// Maximum backoff between reconnection attempts (60 seconds).
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Initial backoff after the first stream drop (1 second).
const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_secs(1);

fn subscriber_task(
    topic_name: String,
    initial_receiver: GossipReceiver,
    gossip: Gossip,
    topic_id: TopicId,
    allowlist: Arc<HashSet<[u8; 32]>>,
    peer_table: Arc<RwLock<PeerTable>>,
    metrics: Arc<dyn GossipMetrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut receiver = initial_receiver;
        let mut backoff = RECONNECT_INITIAL_BACKOFF;

        loop {
            // Consume events until the stream terminates.
            while let Some(event) = receiver.next().await {
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
                        match table.insert_or_refresh(announce, now) {
                            Ok(InsertOutcome::Inserted | InsertOutcome::Refreshed) => {
                                metrics.set_peer_table_size(
                                    i64::try_from(table.len()).unwrap_or(i64::MAX),
                                );
                            }
                            // Exhaustive match on the typed error: adding a new
                            // `PeerTable` failure mode is a compile error here,
                            // so a future variant can't be silently mislabeled as
                            // `stale_timestamp`.
                            Err(crate::StaleTimestamp { .. }) => {
                                metrics.inc_rejected(AnnounceReject::StaleTimestamp.label());
                            }
                        }
                    }
                    Err(reject) => metrics.inc_rejected(reject.label()),
                }
            }

            // Stream terminated — enter reconnection loop.
            tracing::warn!(topic = %topic_name, "gossip subscription stream ended; reconnecting");

            loop {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);

                match gossip.subscribe(topic_id, Vec::new()).await {
                    Ok(topic) => {
                        let (_sender, new_receiver) = topic.split();
                        receiver = new_receiver;
                        metrics.inc_reconnected(&topic_name);
                        tracing::info!(topic = %topic_name, "gossip subscription reconnected");
                        break; // Exit reconnection loop, back to event processing.
                    }
                    Err(err) => {
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
    senders: Vec<(String, GossipSender)>,
    metrics: Arc<dyn GossipMetrics>,
    announce_now: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let node_id = *secret_key.public().as_bytes();

        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            // Both arms drive the same publish; the trigger arm exists so
            // operators running `decdn node announce` (issue #280) can push
            // a fresh announce immediately rather than waiting up to
            // `interval` seconds for peers to see a refreshed `LoadHint` /
            // `popular_hashes` (the fields in `NodeAnnounceBody` that vary
            // between iterations). `region_code` is captured by-value
            // above and does not re-read from config inside this loop —
            // changing region requires a restart, which respawns the
            // publisher and obviates the trigger anyway.
            tokio::select! {
                _ = ticker.tick() => {}
                () = announce_now.notified() => {}
            }
            let ts = now_us();
            let body = NodeAnnounceBody {
                node_id,
                region: region_code.clone(),
                load: LoadHint {
                    active_streams: 0,
                    bandwidth_utilization: 0,
                },
                popular_hashes: Vec::new(),
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
            for (name, sender) in &senders {
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

fn ttl_sweeper_task(
    peer_table: Arc<RwLock<PeerTable>>,
    metrics: Arc<dyn GossipMetrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
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
}
