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
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use crate::{AnnounceReject, GossipMetrics, InsertOutcome, PeerTable, validate_envelope};

/// Label used for both subscribe failures and, transitively, any per-topic
/// rejection tied to the subscription itself. Kept here so the string is
/// defined in one place for the `label()` stability contract.
const SUBSCRIBE_FAILED_LABEL: &str = "subscribe_failed";

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

/// Owns the gossip publisher/subscriber tasks.
#[derive(Debug)]
pub struct GossipService;

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
    ) -> Vec<JoinHandle<()>> {
        let topics = build_topic_list(&cfg);
        if topics.is_empty() {
            tracing::warn!("gossip: no topics to subscribe; service is idle");
            return Vec::new();
        }

        let allowlist = Arc::new(cfg.allowlist.clone());
        let announce_interval = Duration::from_secs(cfg.announce_interval_sec);
        let region = cfg.region.clone();

        // Open one subscription per topic up front. Failures bump a metric
        // and emit an error log; the topic is then skipped so the caller
        // isn't left with half-wired state.
        let mut senders: Vec<(String, GossipSender)> = Vec::new();
        let mut receivers: Vec<(String, GossipReceiver)> = Vec::new();
        for (topic_name, topic_id) in topics {
            match gossip.subscribe(topic_id, Vec::new()).await {
                Ok(topic) => {
                    let (sender, receiver) = topic.split();
                    senders.push((topic_name.clone(), sender));
                    receivers.push((topic_name, receiver));
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
            tracing::error!("gossip: every topic subscribe failed; service exiting");
            return Vec::new();
        }

        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for (topic_name, receiver) in receivers {
            handles.push(subscriber_task(
                topic_name,
                receiver,
                Arc::clone(&allowlist),
                Arc::clone(&peer_table),
                Arc::clone(&metrics),
            ));
        }

        // Skip publishing entirely when no region is configured: an announce
        // without a region fails peer-side validation, so emitting silence
        // beats emitting announces everyone drops.
        if let Some(region_code) = region {
            handles.push(publisher_task(
                secret_key,
                region_code,
                announce_interval,
                senders,
                Arc::clone(&metrics),
            ));
        } else {
            tracing::warn!(
                "gossip: identity.region not set; publisher disabled (subscribe-only mode)"
            );
        }

        handles.push(ttl_sweeper_task(
            Arc::clone(&peer_table),
            Arc::clone(&metrics),
        ));

        handles
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

fn subscriber_task(
    topic_name: String,
    mut receiver: GossipReceiver,
    allowlist: Arc<HashSet<[u8; 32]>>,
    peer_table: Arc<RwLock<PeerTable>>,
    metrics: Arc<dyn GossipMetrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = receiver.next().await {
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
    })
}

fn publisher_task(
    secret_key: SecretKey,
    region_code: String,
    interval: Duration,
    senders: Vec<(String, GossipSender)>,
    metrics: Arc<dyn GossipMetrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let node_id = *secret_key.public().as_bytes();

        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
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
                    tracing::debug!(%err, topic = %name, "gossip publish failed");
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
