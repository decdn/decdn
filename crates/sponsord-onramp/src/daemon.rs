//! Where capabilities come from: the `sponsord` daemon, behind
//! `CapabilitySource` so the HTTP layer can take a fake in tests.

use async_trait::async_trait;
use sponsord_api::client::{DaemonClient, DaemonError};
use sponsord_api::daemon::{Info, IssueRequest, IssueResponse};

/// Something that issues capabilities: [`DaemonClient`] in production, a
/// fake in tests.
#[async_trait]
pub trait CapabilitySource: Send + Sync {
    /// A capability for `req.signer`: the daemon's `POST /v1/capabilities`.
    async fn issue(&self, req: &IssueRequest) -> Result<IssueResponse, DaemonError>;

    /// The daemon's chain, pool and maximum terms, read once at startup.
    async fn info(&self) -> Result<Info, DaemonError>;
}

#[async_trait]
impl CapabilitySource for DaemonClient {
    async fn issue(&self, req: &IssueRequest) -> Result<IssueResponse, DaemonError> {
        DaemonClient::issue(self, req).await
    }

    async fn info(&self) -> Result<Info, DaemonError> {
        DaemonClient::info(self).await
    }
}
