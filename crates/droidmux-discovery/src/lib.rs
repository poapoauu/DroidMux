//! mDNS discovery for Android wireless-debugging endpoints.
//!
//! Discovery is deliberately separate from authentication and connection
//! setup. A caller can inspect the returned endpoints and then choose between
//! an authorized ADB connection and a normal connection that may request user
//! approval.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::mpsc::{self, Receiver as StdReceiver, Sender},
    time::{Duration, Instant},
};

use mdns_sd::{Receiver as MdnsReceiver, ResolvedService, ScopedIp, ServiceDaemon, ServiceEvent};
use thiserror::Error;

/// mDNS service advertised by Android wireless debugging for ADB connections.
pub const ADB_TLS_CONNECT_SERVICE: &str = "_adb-tls-connect._tcp.local.";

/// Legacy mDNS service advertised by adbd TCP endpoints.
pub const ADB_LEGACY_SERVICE: &str = "_adb._tcp.local.";

const ADB_SERVICES: [&str; 2] = [ADB_TLS_CONNECT_SERVICE, ADB_LEGACY_SERVICE];

/// Errors returned by mDNS discovery.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MdnsDiscoveryError {
    /// The mDNS daemon could not be created or controlled.
    #[error("mDNS discovery failed: {0}")]
    Daemon(#[from] mdns_sd::Error),

    /// The discovery worker could not be joined.
    #[error("mDNS discovery worker failed: {0}")]
    Worker(String),
}

/// A wireless ADB endpoint discovered through mDNS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdbMdnsDevice {
    /// Fully qualified mDNS service name.
    pub fullname: String,
    /// Resolved addresses for the service.
    addresses: BTreeSet<IpAddr>,
    /// TCP port advertised by the service.
    pub port: u16,
}

impl AdbMdnsDevice {
    fn from_service(service: ResolvedService) -> Option<Self> {
        let port = service.port;
        if port == 0 {
            return None;
        }
        Some(Self {
            fullname: service.fullname,
            addresses: service.addresses.iter().map(ScopedIp::to_ip_addr).collect(),
            port,
        })
    }

    /// Returns all resolved addresses.
    #[must_use]
    pub fn addresses(&self) -> BTreeSet<IpAddr> {
        self.addresses.clone()
    }

    /// Returns all resolved IPv4 addresses.
    #[must_use]
    pub fn ipv4_addresses(&self) -> BTreeSet<Ipv4Addr> {
        self.addresses
            .iter()
            .filter_map(|address| match address {
                IpAddr::V4(address) => Some(*address),
                IpAddr::V6(_) => None,
            })
            .collect()
    }

    /// Returns all resolved IPv6 addresses.
    #[must_use]
    pub fn ipv6_addresses(&self) -> BTreeSet<Ipv6Addr> {
        self.addresses
            .iter()
            .filter_map(|address| match address {
                IpAddr::V4(_) => None,
                IpAddr::V6(address) => Some(*address),
            })
            .collect()
    }
}

/// A running mDNS browser for wireless ADB endpoints.
pub struct MdnsDiscovery {
    daemon: ServiceDaemon,
}

impl std::fmt::Debug for MdnsDiscovery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MdnsDiscovery")
            .field("metrics", &self.daemon.get_metrics())
            .finish()
    }
}

impl MdnsDiscovery {
    /// Creates an mDNS discovery daemon.
    ///
    /// # Errors
    ///
    /// Returns an error when the platform mDNS daemon cannot be initialized.
    pub fn new() -> Result<Self, MdnsDiscoveryError> {
        Ok(Self {
            daemon: ServiceDaemon::new()?,
        })
    }

    /// Starts browsing modern and legacy ADB mDNS services.
    ///
    /// # Errors
    ///
    /// Returns an error when the service browser or forwarding worker cannot
    /// be started.
    pub fn browse(&self) -> Result<StdReceiver<AdbMdnsDevice>, MdnsDiscoveryError> {
        let events = ADB_SERVICES
            .iter()
            .map(|service| self.daemon.browse(service))
            .collect::<Result<Vec<_>, _>>()?;
        let (sender, receiver) = mpsc::channel();
        for (index, events) in events.into_iter().enumerate() {
            let sender = sender.clone();
            std::thread::Builder::new()
                .name(format!("droidmux-mdns-{index}"))
                .spawn(move || forward_resolved_events(events, sender))
                .map_err(|error| MdnsDiscoveryError::Worker(error.to_string()))?;
        }
        Ok(receiver)
    }

    /// Discovers all endpoints observed during `timeout`.
    ///
    /// # Errors
    ///
    /// Returns an error when mDNS initialization, browsing, or the worker
    /// fails.
    pub async fn discover_once(
        timeout: Duration,
    ) -> Result<Vec<AdbMdnsDevice>, MdnsDiscoveryError> {
        let discovery = Self::new()?;
        let receiver = discovery.browse()?;
        let result = tokio::task::spawn_blocking(move || {
            let mut devices = BTreeMap::new();
            let started = Instant::now();
            loop {
                let remaining = timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    break;
                }
                match receiver.recv_timeout(remaining) {
                    Ok(device) => {
                        devices.insert(device.fullname.clone(), device);
                    }
                    Err(mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected) => {
                        break;
                    }
                }
            }
            devices.into_values().collect()
        })
        .await
        .map_err(|error| MdnsDiscoveryError::Worker(error.to_string()))?;
        discovery.shutdown()?;
        Ok(result)
    }

    /// Stops the mDNS daemon and its browser.
    ///
    /// # Errors
    ///
    /// Returns an error when the mDNS daemon cannot be shut down.
    pub fn shutdown(&self) -> Result<(), MdnsDiscoveryError> {
        self.daemon.shutdown().map(|_| ()).map_err(Into::into)
    }
}

impl Drop for MdnsDiscovery {
    fn drop(&mut self) {
        let _ = self.daemon.shutdown();
    }
}

#[allow(clippy::needless_pass_by_value)]
fn forward_resolved_events(events: MdnsReceiver<ServiceEvent>, sender: Sender<AdbMdnsDevice>) {
    while let Ok(event) = events.recv() {
        if let ServiceEvent::ServiceResolved(service) = event
            && let Some(device) = AdbMdnsDevice::from_service(*service)
            && sender.send(device).is_err()
        {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separates_resolved_address_families() {
        let device = AdbMdnsDevice {
            fullname: "adb-test._adb-tls-connect._tcp.local.".to_owned(),
            addresses: [
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                IpAddr::V6(Ipv6Addr::LOCALHOST),
            ]
            .into_iter()
            .collect(),
            port: 37_001,
        };

        assert_eq!(
            device.ipv4_addresses(),
            [Ipv4Addr::LOCALHOST].into_iter().collect()
        );
        assert_eq!(
            device.ipv6_addresses(),
            [Ipv6Addr::LOCALHOST].into_iter().collect()
        );
    }

    #[test]
    fn uses_android_wireless_debugging_service_name() {
        assert_eq!(ADB_TLS_CONNECT_SERVICE, "_adb-tls-connect._tcp.local.");
        assert_eq!(ADB_LEGACY_SERVICE, "_adb._tcp.local.");
    }
}
