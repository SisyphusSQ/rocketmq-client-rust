// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::time::Duration;

use crate::config::{SocketOptions, TlsConfig};
#[cfg(feature = "socks")]
use crate::socks::SocksProxyConfig;

const CLIENT_CONNECT_TIMEOUT: &str = "com.rocketmq.rocketmq-remoting.client.connect.timeout";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectConfig {
    pub timeout: Duration,
}

impl Default for ConnectConfig {
    fn default() -> Self {
        let timeout_millis = env::var(CLIENT_CONNECT_TIMEOUT)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(3_000)
            .max(1);
        Self {
            timeout: Duration::from_millis(timeout_millis),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceConfig {
    pub idle_scan_interval: Option<Duration>,
    /// Custom fork: Java's max idle age is independent of its scan interval; None disables idle eviction.
    pub idle_timeout: Option<Duration>,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            idle_scan_interval: Some(Duration::from_secs(60)),
            idle_timeout: Some(Duration::from_secs(120)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransportClientConfig {
    /// Custom: native reply-bearing request budget (sync and async share one owner).
    pub client_async_semaphore_value: u32,
    /// Custom: one-way requests in progress until the sole writer finishes.
    pub client_oneway_semaphore_value: u32,
    /// Custom fork: credential ownership and pre-admission security handshake.
    pub authentication: crate::connection_authentication::ConnectionAuthentication,
    pub connect: ConnectConfig,
    pub maintenance: MaintenanceConfig,
    pub socket_options: SocketOptions,
    /// Java clientCloseSocketIfTimeout; the timed out request still has an unknown result.
    pub close_socket_if_timeout: bool,
    pub tls: TlsConfig,
    #[cfg(feature = "socks")]
    pub socks_proxy: SocksProxyConfig,
}

impl Default for TransportClientConfig {
    fn default() -> Self {
        Self {
            client_async_semaphore_value: 65535,
            client_oneway_semaphore_value: 65535,
            authentication: Default::default(),
            connect: Default::default(),
            maintenance: Default::default(),
            socket_options: SocketOptions::java_4_6_1(),
            close_socket_if_timeout: false,
            tls: Default::default(),
            #[cfg(feature = "socks")]
            socks_proxy: Default::default(),
        }
    }
}

impl TransportClientConfig {
    pub fn validate(&self) -> Result<(), rocketmq_error::SharedError> {
        self.authentication.validate()?;
        if self.client_async_semaphore_value > i32::MAX as u32 || self.client_oneway_semaphore_value > i32::MAX as u32 {
            return Err(std::sync::Arc::new(rocketmq_error::Error::new(
                &rocketmq_error::CORE_CONFIGURATION_INVALID,
            )));
        }
        if self.connect.timeout.is_zero()
            || self.maintenance.idle_scan_interval.is_some_and(|value| value.is_zero())
            || self.maintenance.idle_timeout.is_some_and(|value| value.is_zero())
        {
            return Err(std::sync::Arc::new(rocketmq_error::Error::new(
                &rocketmq_error::CORE_CONFIGURATION_INVALID,
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_bounded_typed_durations() {
        let config = TransportClientConfig::default();

        assert!(!config.connect.timeout.is_zero());
        assert_eq!(config.maintenance.idle_scan_interval, Some(Duration::from_secs(60)));
    }
}
