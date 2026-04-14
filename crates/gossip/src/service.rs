//! Gossip publisher + subscriber service wiring iroh-gossip to the peer table.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use decdn_protocol::{
    GOSSIP_VERSION, GossipEnvelope, GossipPayload, LoadHint, NodeAnnounce, NodeAnnounceBody,
    TOPIC_GLOBAL, TOPIC_REGION_PREFIX,
};
use iroh::{Endpoint, SecretKey};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use crate::{AnnounceReject, GossipMetrics, InsertOutcome, PeerTable, validate_envelope};

/// Configuration handed to [`GossipService::spawn`] by the consumer.
#[derive(Debug, Clone)]
pub struct GossipRuntimeConfig {
    pub announce_interval_sec: u64,
    pub peer_ttl_sec: u64,
    pub subscribe_global: bool,
    /// Optional region code (ISO 3166-1 alpha-2). If `Some`, the service
    /// publishes and subscribes on `cdn/region/{code}/v1` in addition to
    /// the global topic.
    pub region: Option<String>,
    /// Hex-validated set of permitted announcer node IDs. Empty = accept any.
    pub allowlist: HashSet<[u8; 32]>,
}

/// Owns the gossip publisher/subscriber tasks.
#[derive(Debug)]
pub struct GossipService;

impl GossipService {
    /// Spawn publisher + subscriber tasks for the configured topics and
    /// return their `JoinHandle`s for the caller's `JoinSet`.
    #[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
    pub fn spawn(
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

        let mut handles = Vec::new();

        for (topic_name, topic_id) in &topics {
            let sub = subscriber_task(
                gossip.clone(),
                *topic_id,
                topic_name.clone(),
                Arc::clone(&allowlist),
                Arc::clone(&peer_table),
                Arc::clone(&metrics),
            );
            handles.push(sub);
        }

        handles.push(publisher_task(
            gossip,
            secret_key,
            topics,
            region,
            announce_interval,
            Arc::clone(&metrics),
        ));

        handles.push(ttl_sweeper_task(
            Arc::clone(&peer_table),
            Arc::clone(&metrics),
        ));

        handles
    }
}

/// Build `(topic_name, TopicId)` pairs to subscribe to. `TopicId` is the
/// blake3 hash of the topic name — the convention iroh-gossip uses in its
/// own examples.
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

fn publisher_task(
    gossip: Gossip,
    secret_key: SecretKey,
    topics: Vec<(String, TopicId)>,
    region: Option<String>,
    interval: Duration,
    metrics: Arc<dyn GossipMetrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Subscribe once per topic (with no bootstrap for PoC) so we have a
        // sender. The GossipTopic's receiver half is dropped; the subscriber
        // tasks hold their own.
        let mut senders = Vec::new();
        for (name, id) in &topics {
            match gossip.subscribe(*id, Vec::new()).await {
                Ok(topic) => {
                    let (sender, _recv) = topic.split();
                    senders.push((name.clone(), sender));
                }
                Err(err) => {
                    tracing::warn!(%err, topic = %name, "gossip publisher subscribe failed");
                }
            }
        }

        let node_id = *secret_key.public().as_bytes();
        // Default to "ZZ" when unset so operators see a clearly fake code
        // in announces rather than panicking at signing time. Real operators
        // should set identity.region.
        let region_code = region.as_deref().unwrap_or("ZZ").to_string();

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

fn subscriber_task(
    gossip: Gossip,
    topic_id: TopicId,
    topic_name: String,
    allowlist: Arc<HashSet<[u8; 32]>>,
    peer_table: Arc<RwLock<PeerTable>>,
    metrics: Arc<dyn GossipMetrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let topic = match gossip.subscribe(topic_id, Vec::new()).await {
            Ok(t) => t,
            Err(err) => {
                tracing::warn!(%err, topic = %topic_name, "gossip subscriber subscribe failed");
                return;
            }
        };
        let (_sender, mut receiver) = topic.split();

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
            match validate_envelope(&msg.content, now_us(), allowlist.as_ref()) {
                Ok(announce) => {
                    let mut table = peer_table.write().await;
                    match table.insert_or_refresh(announce, now_us()) {
                        Ok(InsertOutcome::Inserted | InsertOutcome::Refreshed) => {
                            metrics.set_peer_table_size(
                                i64::try_from(table.len()).unwrap_or(i64::MAX),
                            );
                        }
                        Err(_) => metrics.inc_rejected(AnnounceReject::StaleTimestamp.label()),
                    }
                }
                Err(reject) => metrics.inc_rejected(reject.label()),
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

fn now_us() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros())
            .unwrap_or(0),
    )
    .unwrap_or(u64::MAX)
}
