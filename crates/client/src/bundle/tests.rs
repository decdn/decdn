use std::net::SocketAddr;
use std::time::Duration;

use alloy::primitives::Address;

use super::*;

fn pinned(b: u8) -> PinnedNode {
    PinnedNode {
        node_id: iroh::SecretKey::from_bytes(&[b; 32]).public(),
        provider: Address::repeat_byte(b),
        addr: Some(SocketAddr::from(([127, 0, 0, b], 4000))),
    }
}

/// Discovery options with every field but `pinned` off its default, so a
/// test sees any field `applied_to` drops.
fn tuned(pinned: Option<PinnedNode>) -> DiscoveryOptions {
    DiscoveryOptions {
        pinned,
        rediscover: true,
        max_sources: 7,
        registry_timeout: Duration::from_secs(9),
        ..DiscoveryOptions::default()
    }
}

/// A pinned selection pins the run's discovery to the same node, dial address
/// included, whatever `discovery.pinned` held.
#[test]
fn a_pinned_selection_replaces_the_discovery_pinned_node() {
    let nodes = NodeSelection {
        selection: Selection::Pinned(pinned(1)),
    };
    assert_eq!(nodes.applied_to(tuned(None)), tuned(Some(pinned(1))));
    assert_eq!(
        nodes.applied_to(tuned(Some(pinned(2)))),
        tuned(Some(pinned(1)))
    );
}

/// A registry selection unpins the run's discovery, so its rediscovery reads
/// the registry and no lane dials a stray pinned address.
#[test]
fn a_registry_selection_clears_the_discovery_pinned_node() {
    let nodes = NodeSelection {
        selection: Selection::Registry(engine::Registry {
            nodes: Vec::new(),
            outage_fallback: false,
        }),
    };
    assert_eq!(nodes.applied_to(tuned(Some(pinned(2)))), tuned(None));
}
