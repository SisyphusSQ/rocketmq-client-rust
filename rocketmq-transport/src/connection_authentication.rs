// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.

//! Dedicated Java 4.6.1 Group/Token authentication before connection admission.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::{Error, ErrorContext, SharedError};
use rocketmq_protocol::protocol::remoting_command_defaults::{RemotingCommandFactory, AUTH_RESULT_KEY};
use rocketmq_runtime::OperationContext;
use zeroize::Zeroizing;

use crate::connection::Connection;
use crate::deadline::RequestDeadline;

static NEXT_IDENTITY: AtomicU64 = AtomicU64::new(1);

/// Immutable-by-owner credential configuration. Clones share the same opaque identity;
/// changing either credential selects a new identity for subsequently built clients.
/// Default/empty credentials preserve Java's explicit anonymous mode.
#[derive(Clone)]
pub struct ConnectionAuthentication {
    group_name: Option<CheetahString>,
    token: Option<Arc<Zeroizing<String>>>,
    identity: u64,
    command_factory: RemotingCommandFactory,
}

impl Default for ConnectionAuthentication {
    fn default() -> Self {
        Self {
            group_name: None,
            token: None,
            identity: 0,
            command_factory: RemotingCommandFactory::new(Default::default()),
        }
    }
}

impl fmt::Debug for ConnectionAuthentication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionAuthentication")
            .field("group_name", &self.group_name)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .field("identity", &self.identity)
            .finish()
    }
}

impl PartialEq for ConnectionAuthentication {
    fn eq(&self, other: &Self) -> bool {
        self.group_name == other.group_name
            && self.token() == other.token()
            && self.command_factory == other.command_factory
    }
}
impl Eq for ConnectionAuthentication {}

impl ConnectionAuthentication {
    /// Creates a credential pair. Empty/incomplete explicit pairs are invalid.
    pub fn group_token(group: impl Into<CheetahString>, token: &str) -> Result<Self, SharedError> {
        let mut config = Self::default();
        config.set_group_name(Some(group.into()));
        config.set_token(Some(token));
        config.validate()?;
        if !config.enabled() {
            return Err(invalid_configuration());
        }
        Ok(config)
    }

    pub fn group_name(&self) -> Option<&str> {
        self.group_name.as_deref()
    }

    /// Explicit sensitive access, equivalent to Java getToken(). Never log this value.
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref().map(|value| value.as_str())
    }

    pub fn set_group_name(&mut self, group: Option<CheetahString>) {
        if self.group_name != group {
            self.group_name = group;
            self.identity = NEXT_IDENTITY.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn set_token(&mut self, token: Option<&str>) {
        if self.token() != token {
            self.token = token.map(|value| Arc::new(Zeroizing::new(value.to_owned())));
            self.identity = NEXT_IDENTITY.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn identity(&self) -> u64 {
        self.identity
    }

    pub fn enabled(&self) -> bool {
        self.group_name().is_some_and(|group| !group.is_empty()) && self.token().is_some_and(|token| !token.is_empty())
    }

    /// Rejects partial credentials instead of silently disabling configured authentication.
    pub fn validate(&self) -> Result<(), SharedError> {
        let group = self.group_name().is_some_and(|value| !value.is_empty());
        let token = self.token().is_some_and(|value| !value.is_empty());
        if group != token {
            return Err(invalid_configuration());
        }
        Ok(())
    }

    pub fn with_command_factory(mut self, command_factory: RemotingCommandFactory) -> Self {
        self.command_factory = command_factory;
        self
    }

    pub(crate) async fn authenticate(
        &self,
        connection: &mut Connection,
        deadline: RequestDeadline,
        operation: &OperationContext,
    ) -> Result<(), SharedError> {
        self.validate()?;
        if !self.enabled() {
            return Ok(());
        }
        // Java AUTH_TIMEOUT_MILLIS=5000; never extend the original request deadline.
        let deadline = deadline.capped(Duration::from_secs(5));
        let cancellation = operation.cancellation_token();
        let handshake = async {
            let request = self
                .command_factory
                .create_security_request(self.group_name(), self.token());
            let opaque = request.opaque();
            connection
                .send_command_with_deadline(request, deadline, "connection-authentication")
                .await?;
            let response = connection.receive_command().await.ok_or_else(|| {
                crate::error_helpers::connection_failed_without_source(crate::error_helpers::TransportStage::Closed)
            })??;
            if !response.is_security_type()
                || !response.is_response_type()
                || response.opaque() != opaque
                || response.code() != 0
            {
                return Err(Arc::new(Error::new(&rocketmq_error::PROTOCOL_HEADER_INVALID)));
            }
            let accepted = response
                .ext_fields()
                .and_then(|fields| fields.get(AUTH_RESULT_KEY))
                .is_some_and(|value| value.eq_ignore_ascii_case("true"));
            if !accepted {
                // Broker remarks may contain supplied material; retain a typed failure without echoing it.
                return Err(Arc::new(Error::new(&rocketmq_error::AUTH_CREDENTIALS_INVALID)));
            }
            Ok(())
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(crate::error_helpers::connection_failed_without_source(crate::error_helpers::TransportStage::Closed)),
            result = deadline.timeout(handshake) => result.unwrap_or_else(|_| Err(crate::error_helpers::operation_timed_out("connection_authentication", deadline.budget_millis()))),
        };
        if result.is_err() {
            connection.close();
        }
        result
    }
}

fn invalid_configuration() -> SharedError {
    Arc::new(
        Error::new(&rocketmq_error::CORE_CONFIGURATION_INVALID)
            .with_context(ErrorContext::new().with_text(rocketmq_error::fields::KEY, "group_name/token")),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_protocol::protocol::remoting_command_defaults::{PASSWORD_KEY, USERNAME_KEY};
    use rocketmq_protocol::protocol::{LanguageCode, RemotingCommandType};
    use rocketmq_runtime::TaskKind;

    fn pair() -> (Connection, Connection) {
        let (client, server) = tokio::io::duplex(8192);
        (
            Connection::new_with_plaintext_stream(client),
            Connection::new_with_plaintext_stream(server),
        )
    }

    #[test]
    fn nullable_credentials_and_secret_debug_follow_the_explicit_configuration_contract() {
        let anonymous = ConnectionAuthentication::default();
        assert!(anonymous.validate().is_ok());
        assert!(!anonymous.enabled());
        let mut config = ConnectionAuthentication::group_token("PT_group", "test-token-sensitive").unwrap();
        assert!(!format!("{config:?}").contains("test-token-sensitive"));
        let cloned = config.clone();
        assert_eq!(cloned.identity(), config.identity());
        config.set_token(Some("another-test-token"));
        assert_ne!(cloned.identity(), config.identity());
        config.set_token(None);
        assert!(config.validate().is_err());
        assert_eq!(config.token(), None);
        config.set_group_name(None);
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn security_handshake_correlates_opaque_and_repeats_for_each_connection() {
        let config = ConnectionAuthentication::group_token("PT_group", "test-token-sensitive").unwrap();
        for _ in 0..2 {
            let (mut client, mut server) = pair();
            let peer = tokio::spawn(async move {
                let request = server.receive_command().await.unwrap().unwrap();
                assert_eq!(request.get_type(), RemotingCommandType::SECURITY);
                assert_eq!(request.flag(), 4);
                assert_eq!(request.version(), 335);
                assert_eq!(request.language(), LanguageCode::OTHER);
                assert_eq!(
                    request.ext_fields().unwrap().get(USERNAME_KEY).unwrap().as_str(),
                    "PT_group"
                );
                assert_eq!(
                    request.ext_fields().unwrap().get(PASSWORD_KEY).unwrap().as_str(),
                    "test-token-sensitive"
                );
                assert!(!format!("{request}").contains("test-token-sensitive"));
                let response = RemotingCommandFactory::new(Default::default())
                    .create_security_response(true, None)
                    .mark_response_type()
                    .set_opaque(request.opaque());
                server.send_command(response).await.unwrap();
            });
            config
                .authenticate(
                    &mut client,
                    RequestDeadline::after(Duration::from_secs(10)),
                    &OperationContext::without_deadline(TaskKind::Service),
                )
                .await
                .unwrap();
            peer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn denial_mismatched_opaque_and_malformed_results_close_the_handshake() {
        let config = ConnectionAuthentication::group_token("CT_group", "test-token-sensitive").unwrap();
        for scenario in 0..5 {
            let (mut client, mut server) = pair();
            let peer = tokio::spawn(async move {
                let request = server.receive_command().await.unwrap().unwrap();
                let mut response = RemotingCommandFactory::new(Default::default())
                    .create_security_response(scenario != 0, Some("do not echo test-token-sensitive"))
                    .mark_response_type()
                    .set_opaque(request.opaque());
                match scenario {
                    1 => response.set_opaque_mut(request.opaque().wrapping_add(1)),
                    2 => response = response.set_flag(1),
                    3 => response = response.set_flag(4),
                    4 => response = response.set_ext_fields(Default::default()),
                    _ => {}
                }
                server.send_command(response).await.unwrap();
            });
            let error = config
                .authenticate(
                    &mut client,
                    RequestDeadline::after(Duration::from_secs(10)),
                    &OperationContext::without_deadline(TaskKind::Service),
                )
                .await
                .unwrap_err();
            assert!(!format!("{error:?}").contains("test-token-sensitive"));
            assert!(!client.is_healthy());
            peer.await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn authentication_has_javas_five_second_ceiling_and_keeps_the_callers_earlier_deadline() {
        let config = ConnectionAuthentication::group_token("PT_group", "test-token").unwrap();
        for budget in [Duration::from_millis(20), Duration::from_secs(10)] {
            let (mut client, mut server) = pair();
            let peer = tokio::spawn(async move {
                let _ = server.receive_command().await.unwrap().unwrap();
                assert!(server.receive_command().await.is_none());
            });
            let start = tokio::time::Instant::now();
            let error = config
                .authenticate(
                    &mut client,
                    RequestDeadline::after(budget),
                    &OperationContext::without_deadline(TaskKind::Service),
                )
                .await
                .unwrap_err();
            assert_eq!(error.descriptor(), &rocketmq_error::CORE_OPERATION_TIMED_OUT);
            assert_eq!(tokio::time::Instant::now() - start, budget.min(Duration::from_secs(5)));
            drop(client);
            peer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn cancelling_authentication_drops_the_connection_without_business_frames() {
        let config = ConnectionAuthentication::group_token("PT_group", "test-token").unwrap();
        let (mut client, mut server) = pair();
        let operation = OperationContext::without_deadline(TaskKind::Service);
        let worker_operation = operation.clone();
        let caller = tokio::spawn(async move {
            config
                .authenticate(
                    &mut client,
                    RequestDeadline::after(Duration::from_secs(30)),
                    &worker_operation,
                )
                .await
        });
        let first = server.receive_command().await.unwrap().unwrap();
        assert!(first.is_security_type());
        operation.cancel();
        assert!(caller.await.unwrap().is_err());
        assert!(server.receive_command().await.is_none());
    }
}
