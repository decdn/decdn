use iroh::endpoint::{ConnectionError, StoppedError, VarInt};

use super::client_left_error;
use crate::handlers::client::wire::{PeerFault, is_peer_attributable};

/// Only the client leaving is an abandon. A local close is attributed like the
/// write path attributes it, and a stream this node already closed is a node
/// fault.
#[test]
fn only_the_client_leaving_is_an_abandon() {
    let (stop, abandoned) = client_left_error(Ok(Some(VarInt::from_u32(7))));
    assert!(abandoned && stop.is::<PeerFault>());

    let (lost, abandoned) =
        client_left_error(Err(StoppedError::ConnectionLost(ConnectionError::TimedOut)));
    assert!(abandoned && lost.is::<PeerFault>());
    assert!(
        format!("{lost:#}").contains("timed out"),
        "the connection error stays in the chain: {lost:#}"
    );

    let (local, abandoned) = client_left_error(Err(StoppedError::ConnectionLost(
        ConnectionError::LocallyClosed,
    )));
    assert!(!abandoned && local.is::<PeerFault>());

    let (closed, abandoned) = client_left_error(Ok(None));
    assert!(!abandoned && !is_peer_attributable(&closed));

    let (zero_rtt, abandoned) = client_left_error(Err(StoppedError::ZeroRttRejected));
    assert!(!abandoned && !is_peer_attributable(&zero_rtt));
}
