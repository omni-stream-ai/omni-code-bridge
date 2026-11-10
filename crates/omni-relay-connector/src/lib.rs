//! Embed the connector in Core/Bridge, using the host's Tokio runtime.
//!
//! Call [`start`] when remote access is enabled, observe [`ConnectorHandle::subscribe`],
//! then await [`ConnectorHandle::shutdown`] before stopping the local HTTP service.
//! Credentials are never included in status events or connector diagnostics.
mod client;
mod connector;
mod host;
pub mod protocol;
pub mod util;
pub use client::{LoginFlow, LoginStatus, RelayClient, Session};
pub use connector::{start, Config, ConnectorHandle, ConnectorState, ConnectorStatus};
pub use host::{host_cli, Host, UPSTREAM_CLIENT_ID};
