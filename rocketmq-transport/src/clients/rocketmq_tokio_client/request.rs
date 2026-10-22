// Copyright 2026 The RocketMQ Rust Authors
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

// Custom fork: Java 4.6.1 network lifecycle and preserved physical request failures.

//! Canonical request and one-way execution for the Tokio transport client.

use cheetah_string::CheetahString;
use rocketmq_error::SharedError;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ResourcePermit;
use std::sync::Arc;
use tokio::time;
use tracing::debug;
use tracing::error;
use tracing::warn;

use super::RequestTarget;
use super::SendReceipt;
use super::TransportClient;
use crate::clients::TransportSession;
use crate::deadline::RequestDeadline;
use crate::error::RequestOperation;
use crate::error::TransportError;
use crate::error_helpers::client_not_started;
use crate::error_helpers::connection_failed_without_source_for_remote;
use crate::error_helpers::TransportStage;
use crate::request_outcome::OutboundRequestContract;
use crate::request_outcome::OutboundRequestOutcome;
use crate::request_outcome::OutboundRequestRejection;
use crate::request_outcome::OutboundRequestStage;
use crate::telemetry::TransportGoAwayOutcome;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

impl<PR: Send + Sync + Clone + 'static> TransportClient<PR> {
    pub(super) fn session_cache_identity(
        &self,
        requested_addr: Option<&CheetahString>,
        session: &TransportSession<PR>,
    ) -> CheetahString {
        requested_addr
            .cloned()
            .or_else(|| self.connection_registry.session_identity(session))
            .or_else(|| self.endpoint_state.load().chosen().cloned())
            .unwrap_or_else(|| CheetahString::from_string(session.remote_address().to_string()))
    }

    pub(super) fn remove_cached_session_if_matches(
        &self,
        identity: &CheetahString,
        expected: &TransportSession<PR>,
    ) -> bool {
        self.connection_registry
            .remove_session_if_matches(identity, expected)
            .is_some()
    }

    fn start_go_away_drain(&self, _identity: CheetahString, session: TransportSession<PR>) {
        session.begin_drain();
        let drain_timeout = session.max_pending_request_age();
        let spawned = self.spawn_worker_task("rocketmq.transport.go-away-drain", async move {
            let report = session.drain_and_close(drain_timeout).await;
            if !report.is_healthy() {
                warn!(report = %report.to_json(), "GO_AWAY session drain was unhealthy");
            }
        });
        if spawned.is_none() {
            warn!("GO_AWAY session drain could not be scheduled because the client is shutting down");
        }
    }

    // Custom: waits are cancellable by the same worker generation as connection
    // work, including the valid Java zero-permit configuration and restarts.
    async fn acquire_request_slot(
        &self,
        slots: &Arc<tokio::sync::Semaphore>,
        deadline: RequestDeadline,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, rocketmq_error::SharedError> {
        let owner = self.capture_worker_task_owner().ok_or_else(client_not_started)?;
        let stop = owner.cancellation();
        tokio::select! {
            biased;
            _ = stop.cancelled() => Err(client_not_started()),
            permit = deadline.timeout(slots.clone().acquire_owned()) => {
                permit.map_err(|_| deadline.elapsed_error())?.map_err(|_| client_not_started())
            }
        }
    }

    pub(super) async fn invoke_oneway_until(
        &self,
        addr: &CheetahString,
        request: RemotingCommand,
        deadline: RequestDeadline,
        permit: Option<ResourcePermit>,
    ) -> Result<(), rocketmq_error::SharedError> {
        deadline.ensure_before_send()?;
        let _slot = self.acquire_request_slot(&self.oneway_slots, deadline).await?;
        if self.is_stopping() {
            return Err(client_not_started());
        }
        let Some(mut client) = self.get_and_create_client_until(Some(addr), deadline).await? else {
            return Err(connection_failed_without_source_for_remote(
                addr,
                TransportStage::Closed,
            ));
        };
        if self.is_stopping() {
            return Err(client_not_started());
        }

        let mut request = request;
        let remote_address = client.remote_address();
        if let Some(hooks) = self.cmd_handler.hook_snapshot() {
            request.make_custom_header_to_net();
            self.cmd_handler.do_before_rpc_hooks_with_snapshot(
                Some(hooks.as_ref()),
                remote_address,
                Some(&mut request),
            )?;
        }
        deadline.ensure_before_send()?;
        request.mark_oneway_rpc_ref();
        match permit {
            Some(permit) => client.send_until_with_permit(request, deadline, permit).await,
            None => client.send_until(request, deadline).await,
        }
    }

    /// Sends one canonical request under an absolute deadline.
    pub(super) async fn request_inner(
        &self,
        target: RequestTarget,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> Result<OutboundRequestOutcome, TransportError> {
        match target {
            RequestTarget::Endpoint(endpoint) => {
                self.invoke_request_with_deadline(Some(&endpoint), request, deadline)
                    .await
            }
            RequestTarget::NameServer => self.invoke_request_with_deadline(None, request, deadline).await,
        }
    }

    /// Sends one command and resolves only after the sole writer has completed it.
    pub(super) async fn send_oneway_inner(
        &self,
        target: RequestTarget,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> Result<SendReceipt, rocketmq_error::SharedError> {
        match target {
            RequestTarget::Endpoint(endpoint) => {
                self.invoke_oneway_until(&endpoint, request, deadline, None).await?;
                Ok(SendReceipt {
                    endpoint,
                    written_at_millis: current_millis(),
                })
            }
            RequestTarget::NameServer => {
                let _slot = self.acquire_request_slot(&self.oneway_slots, deadline).await?;
                let started_at = time::Instant::now();
                deadline.ensure_before_send()?;
                let Some(selection) = self.get_and_create_nameserver_client_until(deadline).await? else {
                    return Err(connection_failed_without_source_for_remote(
                        "<nameserver>",
                        TransportStage::Closed,
                    ));
                };
                let metric_identity = selection.identity.clone();
                let metric_lease = selection.lease.clone();
                let selection_generation = selection.state.generation();
                let mut client = selection.session;
                debug!(
                    generation = selection_generation,
                    "Sending one-way request to selected nameserver"
                );
                let result = async {
                    let endpoint = CheetahString::from_string(client.remote_address().to_string());
                    let mut request = request;
                    if let Some(hooks) = self.cmd_handler.hook_snapshot() {
                        request.make_custom_header_to_net();
                        self.cmd_handler.do_before_rpc_hooks_with_snapshot(
                            Some(hooks.as_ref()),
                            client.remote_address(),
                            Some(&mut request),
                        )?;
                    }
                    request.mark_oneway_rpc_ref();
                    client.send_until(request, deadline).await?;
                    Ok(SendReceipt {
                        endpoint,
                        written_at_millis: current_millis(),
                    })
                }
                .await;
                self.record_nameserver_outcome(
                    Some(&metric_identity),
                    Some(&metric_lease),
                    started_at.elapsed(),
                    result.is_ok(),
                );
                result
            }
        }
    }

    /// Sends a request under the caller's absolute deadline.
    ///
    /// Normal deadline, lifecycle, admission, endpoint-availability, and
    /// request-contract outcomes are returned as typed values. A deadline
    /// observed after a response arrives is returned as a deadline rejection
    /// at [`OutboundRequestStage::ResponseReceived`].
    ///
    /// # Errors
    ///
    /// Returns an operational Transport failure when connection setup, request
    /// signing, socket I/O, response correlation, or a final response hook
    /// fails. A final-hook failure records
    /// [`OutboundRequestStage::ResponseReceived`].
    pub async fn invoke_request_with_deadline(
        &self,
        addr: Option<&CheetahString>,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> Result<OutboundRequestOutcome, TransportError> {
        self.invoke_request_with_response_future(addr, request, deadline, None)
            .await
    }

    /// Custom: native async form of Java invokeAsync. The caller's task owns this
    /// future; all I/O still uses the canonical transport and correlation table.
    pub async fn invoke_async_callback(
        &self,
        addr: Option<&CheetahString>,
        request: RemotingCommand,
        timeout_millis: u64,
        callback: Arc<dyn crate::response_future::InvokeCallback>,
    ) -> Arc<crate::response_future::ResponseFuture> {
        let future = Arc::new(crate::response_future::ResponseFuture::new(
            None,
            request.opaque(),
            i64::try_from(timeout_millis).unwrap_or(i64::MAX),
            Some(callback),
            None,
        ));
        let result = self
            .invoke_request_with_response_future(
                addr,
                request,
                RequestDeadline::from_timeout_millis(timeout_millis),
                Some(&future),
            )
            .await;
        future.complete(&result);
        let executor = self.callback_executor.read().clone();
        if let Some(executor) = executor {
            let in_executor = future.clone();
            if executor
                .spawn_io("remoting.invoke-callback", move || {
                    in_executor.execute_invoke_callback()
                })
                .await
                .is_ok()
            {
                return future;
            }
        }
        // Java also falls back to the request owner when its callback executor
        // rejects work. The once guard prevents a panicking callback from replay.
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.execute_invoke_callback())).is_err() {
            warn!("remoting callback panicked");
        }
        future
    }

    pub fn set_callback_executor(&self, executor: Option<rocketmq_runtime::BlockingExecutor>) {
        *self.callback_executor.write() = executor;
    }
    pub fn get_callback_executor(&self) -> Option<rocketmq_runtime::BlockingExecutor> {
        self.callback_executor.read().clone()
    }

    async fn invoke_request_with_response_future(
        &self,
        addr: Option<&CheetahString>,
        request: RemotingCommand,
        deadline: RequestDeadline,
        response_future: Option<&crate::response_future::ResponseFuture>,
    ) -> Result<OutboundRequestOutcome, TransportError> {
        let nameserver_request = addr.is_none_or(CheetahString::is_empty);
        let start = time::Instant::now();
        let timeout_millis = deadline.budget_millis();
        let target = if nameserver_request {
            "<nameserver>".to_string()
        } else {
            addr.map_or_else(|| "<nameserver>".to_string(), ToString::to_string)
        };
        if deadline.is_expired() {
            return Ok(OutboundRequestOutcome::Rejected(
                OutboundRequestRejection::deadline_expired(
                    OutboundRequestStage::BeforeWrite,
                    timeout_millis,
                    !nameserver_request,
                ),
            ));
        }
        let _slot = match self.acquire_request_slot(&self.request_slots, deadline).await {
            Ok(slot) => slot,
            Err(error) => return classify_before_write_error(error, deadline, !nameserver_request),
        };
        let nameserver_diagnostics = nameserver_request.then(|| self.endpoint_state.load());
        let nameserver_selection = if nameserver_request {
            match self.get_and_create_nameserver_client_until(deadline).await {
                Ok(selection) => selection,
                Err(error) => return classify_before_write_error(error, deadline, false),
            }
        } else {
            None
        };
        let nameserver_metric_addr = nameserver_selection
            .as_ref()
            .map(|selection| selection.identity.clone());
        let nameserver_lease = nameserver_selection.as_ref().map(|selection| selection.lease.clone());
        let nameserver_generation = nameserver_selection
            .as_ref()
            .map(|selection| selection.state.generation());
        let client = match nameserver_selection {
            Some(selection) => Some(selection.session),
            None if nameserver_request => None,
            None => match self.get_and_create_client_until(addr, deadline).await {
                Ok(session) => session,
                Err(error) => return classify_before_write_error(error, deadline, true),
            },
        };
        let Some(mut client) = client else {
            if target == "<nameserver>" {
                if let Some(state) = nameserver_diagnostics.as_ref() {
                    error!(
                        configured = state.endpoints().len(),
                        available = state.available().len(),
                        cached_choice = state.chosen().is_some(),
                        connections = self.connection_registry.len(),
                        "Failed to get client for <nameserver>"
                    );
                }
            } else {
                error!("Failed to get client for direct endpoint");
            }

            if nameserver_request
                && nameserver_diagnostics
                    .as_ref()
                    .is_some_and(|state| state.endpoints().is_empty())
            {
                return Ok(OutboundRequestOutcome::Contract(
                    OutboundRequestContract::name_server_endpoint_missing(),
                ));
            }
            return Ok(OutboundRequestOutcome::Rejected(
                OutboundRequestRejection::endpoint_unavailable(OutboundRequestStage::BeforeWrite, !nameserver_request),
            ));
        };

        if self.is_stopping() {
            return Ok(OutboundRequestOutcome::Rejected(
                OutboundRequestRejection::client_stopping(OutboundRequestStage::BeforeWrite, true),
            ));
        }

        if let Some(future) = response_future {
            future.set_process_channel(client.session_view());
        }
        let mut request = request;
        let initial_remote_address = client.remote_address();
        if deadline.is_expired() {
            return Ok(OutboundRequestOutcome::Rejected(
                OutboundRequestRejection::deadline_expired(OutboundRequestStage::BeforeWrite, timeout_millis, true),
            ));
        }
        let hooks = self.cmd_handler.hook_snapshot();
        let request_for_after = if let Some(hooks) = hooks {
            request.make_custom_header_to_net();
            if let Err(error) = self.cmd_handler.do_before_rpc_hooks_with_snapshot(
                Some(hooks.as_ref()),
                initial_remote_address,
                Some(&mut request),
            ) {
                return Err(request_transport_error(
                    RequestOperation::BeforeHook,
                    OutboundRequestStage::BeforeWrite,
                    error,
                ));
            }
            if deadline.is_expired() {
                return Ok(OutboundRequestOutcome::Rejected(
                    OutboundRequestRejection::deadline_expired(OutboundRequestStage::BeforeWrite, timeout_millis, true),
                ));
            }
            Some((request.clone(), hooks))
        } else {
            None
        };
        let apply_final_hooks = |mut response: RemotingCommand,
                                 remote_address: std::net::SocketAddr|
         -> Result<OutboundRequestOutcome, TransportError> {
            if let Some((request, hooks)) = request_for_after.as_ref() {
                if let Err(error) = self.cmd_handler.do_after_rpc_hooks_with_snapshot(
                    Some(hooks.as_ref()),
                    remote_address,
                    request,
                    Some(&mut response),
                ) {
                    return Err(request_transport_error(
                        RequestOperation::AfterHook,
                        OutboundRequestStage::ResponseReceived,
                        error,
                    ));
                }
            }
            if deadline.is_expired() {
                return Ok(OutboundRequestOutcome::Rejected(
                    OutboundRequestRejection::deadline_expired(
                        OutboundRequestStage::ResponseReceived,
                        timeout_millis,
                        true,
                    ),
                ));
            }
            Ok(OutboundRequestOutcome::Response(response))
        };
        let remote_address = client.remote_address();
        let identity = if nameserver_request {
            self.connection_registry
                .session_identity(&client)
                .or_else(|| nameserver_metric_addr.clone())
                .unwrap_or_else(|| CheetahString::from_string(remote_address.to_string()))
        } else {
            self.session_cache_identity(addr, &client)
        };

        let outcome = match client.send_read(request, deadline).await {
            Ok(OutboundRequestOutcome::Response(response)) => {
                if response.code() == ResponseCode::GoAway.to_i32() {
                    self.telemetry.record_go_away(TransportGoAwayOutcome::Received);
                    self.remove_cached_session_if_matches(&identity, &client);
                    self.start_go_away_drain(identity, client);
                }
                apply_final_hooks(response, remote_address)
            }
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                if self.tokio_client_config.close_socket_if_timeout
                    && rejection.reason() == crate::request_outcome::OutboundRequestRejectionReason::DeadlineExpired
                    && matches!(
                        rejection.stage(),
                        OutboundRequestStage::Writing | OutboundRequestStage::AwaitingResponse
                    )
                {
                    client.retire_after_timeout().await;
                    self.remove_cached_session_if_matches(&identity, &client);
                }
                Ok(OutboundRequestOutcome::Rejected(rejection))
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => Ok(OutboundRequestOutcome::Contract(contract)),
            Err(error) => {
                if self.tokio_client_config.close_socket_if_timeout
                    && matches!(
                        error.descriptor().code(),
                        code if code == rocketmq_error::TRANSPORT_WRITE_TIMEOUT.code()
                            || code == rocketmq_error::TRANSPORT_RESPONSE_TIMEOUT.code()
                    )
                {
                    client.retire_after_timeout().await;
                    self.remove_cached_session_if_matches(&identity, &client);
                }
                Err(error)
            }
        };
        let latency = start.elapsed();
        self.record_nameserver_outcome(
            nameserver_metric_addr.as_ref(),
            nameserver_lease.as_ref(),
            latency,
            matches!(&outcome, Ok(OutboundRequestOutcome::Response(_))),
        );
        match &outcome {
            Ok(OutboundRequestOutcome::Response(_)) => debug!(
                endpoint_kind = if nameserver_request { "nameserver" } else { "direct" },
                nameserver_generation = ?nameserver_generation,
                elapsed_ms = latency.as_millis() as u64,
                "request completed"
            ),
            Ok(OutboundRequestOutcome::Rejected(_) | OutboundRequestOutcome::Contract(_)) | Err(_) => warn!(
                endpoint_kind = if nameserver_request { "nameserver" } else { "direct" },
                elapsed_ms = latency.as_millis() as u64,
                "request did not produce a response"
            ),
        }
        outcome
    }
}

fn classify_before_write_error(
    error: SharedError,
    deadline: RequestDeadline,
    remote_addr_present: bool,
) -> Result<OutboundRequestOutcome, TransportError> {
    if error.descriptor() == &rocketmq_error::CORE_OPERATION_TIMED_OUT {
        Ok(OutboundRequestOutcome::Rejected(
            OutboundRequestRejection::deadline_expired(
                OutboundRequestStage::BeforeWrite,
                deadline.budget_millis(),
                remote_addr_present,
            ),
        ))
    } else if error.descriptor() == &rocketmq_error::CLIENT_LIFECYCLE_NOT_STARTED {
        Ok(OutboundRequestOutcome::Rejected(
            OutboundRequestRejection::client_stopping(OutboundRequestStage::BeforeWrite, remote_addr_present),
        ))
    } else {
        Err(TransportError::request(
            RequestOperation::Connect,
            OutboundRequestStage::BeforeWrite,
            error,
        ))
    }
}

fn request_transport_error(
    operation: RequestOperation,
    stage: OutboundRequestStage,
    error: SharedError,
) -> TransportError {
    TransportError::request(operation, stage, error)
}

#[cfg(test)]
mod java_slot_tests {
    use super::*;
    use crate::request_processor::default_request_processor::DefaultRequestProcessor;
    use crate::runtime::config::client_config::TransportClientConfig;
    use rocketmq_runtime::ShutdownDeadline;
    use std::time::Duration;
    #[tokio::test]
    async fn zero_java_request_slots_end_on_shutdown_and_restart_keeps_deadlines() {
        let root = rocketmq_runtime::RuntimeContext::from_current("java-zero-slot-contract");
        let config = TransportClientConfig {
            client_async_semaphore_value: 0,
            client_oneway_semaphore_value: 0,
            ..Default::default()
        };
        let client = Arc::new(
            TransportClient::builder(
                Arc::new(config),
                DefaultRequestProcessor,
                root.service_context("client"),
            )
            .build()
            .unwrap(),
        );
        let waiting = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .acquire_request_slot(&client.request_slots, RequestDeadline::from_timeout_millis(5000))
                    .await
            }
        });
        tokio::task::yield_now().await;
        let report = client.shutdown_now();
        assert!(tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        let _ = report;
        client.start().await.unwrap();
        assert!(client
            .acquire_request_slot(&client.oneway_slots, RequestDeadline::from_timeout_millis(10))
            .await
            .is_err());
        let report = client
            .shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert!(report.is_healthy());
        assert!(root.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
    }
}
