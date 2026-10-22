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

#![allow(deprecated)]

use std::collections::HashSet;
use std::sync::Arc;

use crate::ClientError;
use crate::ClientResult;
use cheetah_string::CheetahString;
#[cfg(test)]
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;

#[cfg(test)]
use crate::producer::local_transaction_state::LocalTransactionState;
use crate::runtime::ClientRuntime;

pub use crate::consumer::consumer_impl::default_mq_pull_consumer_impl::DefaultMQPullConsumerImpl;
pub use crate::consumer::default_mq_pull_consumer::DefaultMQPullConsumer;
pub use crate::consumer::default_mq_pull_consumer::MQPullConsumer;

const MODERN_TRACE_HOOKS: &str = "RocketMQ trace hooks";

fn unsupported_legacy_api(api: &str, replacement: &str) -> ClientError {
    ClientError::illegal_argument(format!(
        "{api} is deprecated in the RocketMQ Java client and is not supported by rocketmq-client-rust; use \
         {replacement} instead"
    ))
}

fn unsupported_impl_api(api: &str) -> ClientError {
    ClientError::illegal_argument(format!(
        "{api} is a RocketMQ Java impl-package type and is not part of the rocketmq-client-rust public API"
    ))
}

// Custom fork: functional Classic Pull scheduling preserves the legacy export paths.
pub use crate::consumer::mq_pull_consumer_schedule_service::{
    MQPullConsumerScheduleService, PullTaskCallback, PullTaskContext, PullTaskImpl,
};

#[derive(Clone, Default)]
pub struct RebalancePullImpl {
    inner: Option<Arc<crate::consumer::consumer_impl::re_balance::rebalance_lite_pull_impl::RebalanceLitePullImpl>>,
}
impl std::fmt::Debug for RebalancePullImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebalancePullImpl")
            .field("attached", &self.inner.is_some())
            .finish()
    }
}
impl RebalancePullImpl {
    pub fn new() -> ClientResult<Self> {
        Ok(Self::default())
    }
    pub(crate) fn from_inner(
        inner: Arc<crate::consumer::consumer_impl::re_balance::rebalance_lite_pull_impl::RebalanceLitePullImpl>,
    ) -> Self {
        Self { inner: Some(inner) }
    }
    fn inner(
        &self,
    ) -> ClientResult<&Arc<crate::consumer::consumer_impl::re_balance::rebalance_lite_pull_impl::RebalanceLitePullImpl>>
    {
        self.inner
            .as_ref()
            .ok_or_else(|| ClientError::not_initialized("RebalancePullImpl is detached"))
    }
    pub async fn message_queue_changed(
        &self,
        topic: &str,
        all: &HashSet<MessageQueue>,
        divided: &HashSet<MessageQueue>,
    ) -> ClientResult<()> {
        use crate::consumer::consumer_impl::re_balance::Rebalance;
        self.inner()?.message_queue_changed(topic, all, divided).await;
        Ok(())
    }
    pub async fn do_rebalance(&self) -> ClientResult<bool> {
        Ok(self.inner()?.rebalance_impl_inner.do_rebalance(false).await)
    }
    pub async fn remove_unnecessary_message_queue(&self, queue: &MessageQueue) -> ClientResult<bool> {
        let store = self
            .inner()?
            .offset_store
            .load_full()
            .ok_or_else(|| ClientError::not_initialized("OffsetStore"))?;
        store.persist_all_checked(&HashSet::from([queue.clone()])).await?;
        store.remove_offset(queue).await;
        Ok(true)
    }
    pub async fn remove_dirty_offset(&self, queue: &MessageQueue) -> ClientResult<()> {
        let store = self
            .inner()?
            .offset_store
            .load_full()
            .ok_or_else(|| ClientError::not_initialized("OffsetStore"))?;
        store.remove_offset(queue).await;
        Ok(())
    }
    pub fn consume_type(&self) -> rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType {
        rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType::ConsumeActively
    }
    pub fn compute_pull_from_where(&self, _queue: &MessageQueue) -> i64 {
        0
    }
    /// Java Classic Pull deliberately does not dispatch automatic fetching.
    pub fn dispatch_pull_request(&self, _requests: Vec<crate::consumer::consumer_impl::pull_request::PullRequest>) {}
}

#[deprecated(since = "0.9.0", note = "Java MQHelper depends on DefaultMQPullConsumer")]
#[derive(Debug, Clone, Default)]
pub struct MQHelper;

impl MQHelper {
    /// Retains the Java-shaped helper signature without creating a hidden runtime.
    ///
    /// Use [`Self::reset_offset_by_timestamp_with_client_runtime`] for the runnable equivalent.
    ///
    /// # Errors
    ///
    /// Always returns an initialization error because this compatibility signature has no
    /// application-owned [`ClientRuntime`].
    pub fn reset_offset_by_timestamp(
        _message_model: impl Into<CheetahString>,
        _consumer_group: impl Into<CheetahString>,
        _topic: impl Into<CheetahString>,
        _timestamp: u64,
    ) -> ClientResult<()> {
        Err(ClientError::not_initialized(
            "MQHelper requires an application-owned ClientRuntime; use reset_offset_by_timestamp_with_client_runtime",
        ))
    }

    /// Resets every queue offset for a topic to the position nearest the timestamp.
    ///
    /// The temporary Classic Pull consumer persists updated offsets during its bounded shutdown.
    ///
    /// # Errors
    ///
    /// Returns a configuration, startup, route, offset-query, offset-store, or shutdown error.
    pub async fn reset_offset_by_timestamp_with_client_runtime(
        client_runtime: Arc<ClientRuntime>,
        message_model: MessageModel,
        consumer_group: impl Into<CheetahString>,
        topic: impl Into<CheetahString>,
        timestamp: u64,
    ) -> ClientResult<()> {
        let timestamp =
            i64::try_from(timestamp).map_err(|_| ClientError::illegal_argument("timestamp exceeds Java long range"))?;
        Self::reset_offset_by_timestamp_with_options(
            client_runtime,
            crate::ClientOptions::legacy(crate::ClientConfig::default()),
            message_model,
            "DEFAULT",
            consumer_group,
            topic,
            timestamp,
        )
        .await
    }

    /// Both Java overloads share this runnable, owned entry. Set instance_name to
    /// DEFAULT for the short overload; options retain explicit authentication,
    /// discovery and protocol ownership instead of relying on global configuration.
    pub async fn reset_offset_by_timestamp_with_options(
        client_runtime: Arc<ClientRuntime>,
        options: crate::ClientOptions,
        message_model: MessageModel,
        instance_name: impl Into<CheetahString>,
        consumer_group: impl Into<CheetahString>,
        topic: impl Into<CheetahString>,
        timestamp: i64,
    ) -> ClientResult<()> {
        let consumer = DefaultMQPullConsumer::builder(client_runtime)
            .client_options(options.with_instance_name(instance_name))
            .consumer_group(consumer_group)
            .message_model(message_model)
            .build()?;
        let topic = topic.into();
        consumer.start().await?;

        let operation = async {
            let mut queues = consumer.fetch_subscribe_message_queues(topic.as_str()).await?;
            queues.sort();
            for queue in queues {
                let offset = consumer.search_offset_signed(&queue, timestamp).await?;
                if offset >= 0 {
                    consumer.update_consume_offset(&queue, offset).await?;
                }
            }
            Ok(())
        }
        .await;
        let shutdown = consumer.shutdown().await;

        match operation {
            Ok(()) => shutdown,
            Err(error) => {
                if let Err(shutdown_error) = shutdown {
                    tracing::warn!(%shutdown_error, "MQHelper consumer shutdown failed after offset reset error");
                }
                Err(error)
            }
        }
    }
}

// Custom: retain the old module path with the functional, independent SPI.
pub use crate::producer::transaction_listener::TransactionCheckListener;

#[deprecated(
    since = "0.9.0",
    note = "Java OpenTracing hooks are not part of the modern RocketMQ client trace API"
)]
#[derive(Debug, Clone, Default)]
pub struct SendMessageOpenTracingHookImpl;

impl SendMessageOpenTracingHookImpl {
    pub fn new<T>(_tracer: T) -> Self {
        Self
    }

    pub fn hook_name(&self) -> &'static str {
        "SendMessageOpenTracingHook"
    }

    pub fn unsupported(&self) -> ClientResult<()> {
        Err(unsupported_legacy_api(
            "SendMessageOpenTracingHookImpl",
            MODERN_TRACE_HOOKS,
        ))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java OpenTracing hooks are not part of the modern RocketMQ client trace API"
)]
#[derive(Debug, Clone, Default)]
pub struct ConsumeMessageOpenTracingHookImpl;

impl ConsumeMessageOpenTracingHookImpl {
    pub fn new<T>(_tracer: T) -> Self {
        Self
    }

    pub fn hook_name(&self) -> &'static str {
        "ConsumeMessageOpenTracingHook"
    }

    pub fn unsupported(&self) -> ClientResult<()> {
        Err(unsupported_legacy_api(
            "ConsumeMessageOpenTracingHookImpl",
            MODERN_TRACE_HOOKS,
        ))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java OpenTracing hooks are not part of the modern RocketMQ client trace API"
)]
#[derive(Debug, Clone, Default)]
pub struct EndTransactionOpenTracingHookImpl;

impl EndTransactionOpenTracingHookImpl {
    pub fn new<T>(_tracer: T) -> Self {
        Self
    }

    pub fn hook_name(&self) -> &'static str {
        "EndTransactionOpenTracingHook"
    }

    pub fn unsupported(&self) -> ClientResult<()> {
        Err(unsupported_legacy_api(
            "EndTransactionOpenTracingHookImpl",
            MODERN_TRACE_HOOKS,
        ))
    }
}

#[derive(Debug, Clone, Default)]
pub struct DoNothingClientRemotingProcessor;

impl DoNothingClientRemotingProcessor {
    pub fn new() -> Self {
        Self
    }

    pub fn process_request(&self) -> Option<()> {
        None
    }
}

// Custom: abstract Java rebalance and LitePull consume requests use functional native types.
pub use crate::consumer::consumer_impl::lite_pull_consume_request::LitePullConsumeRequest as ConsumeRequest;
pub use crate::consumer::consumer_impl::re_balance::Rebalance as RebalanceImpl;

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use crate::test_support::error_assertions::assert_not_initialized;

    #[tokio::test]
    async fn legacy_pull_consumer_fails_closed_without_runtime() {
        let consumer = DefaultMQPullConsumer::with_consumer_group("LegacyGroup");

        assert_eq!(
            consumer.consumer_group().map(|group| group.as_str()),
            Some("LegacyGroup")
        );
        let error = consumer
            .start()
            .await
            .expect_err("detached pull consumer should not start");

        assert_not_initialized(
            &error,
            "DefaultMQPullConsumer has no ClientRuntime; create it with DefaultMQPullConsumer::builder",
        );
    }

    #[test]
    fn pull_task_context_preserves_java_default_delay() {
        let mut context = PullTaskContext::new();

        assert_eq!(context.get_pull_next_delay_time_millis(), 200);
        context.set_pull_next_delay_time_millis(500);
        assert_eq!(context.pull_next_delay_time_millis(), 500);
    }

    #[test]
    fn detached_schedule_service_requires_runtime_for_callbacks() {
        struct Callback;
        impl PullTaskCallback for Callback {}

        let service = MQPullConsumerScheduleService::new("LegacyGroup");
        let error = service
            .register_pull_task_callback("TopicA", Callback)
            .expect_err("detached schedule service should require runtime");

        assert_not_initialized(
            &error,
            "MQPullConsumerScheduleService has no ClientRuntime; use with_client_runtime",
        );
    }

    #[test]
    fn legacy_open_tracing_hooks_keep_java_hook_names() {
        assert_eq!(
            SendMessageOpenTracingHookImpl::new(()).hook_name(),
            "SendMessageOpenTracingHook"
        );
        assert_eq!(
            ConsumeMessageOpenTracingHookImpl::new(()).hook_name(),
            "ConsumeMessageOpenTracingHook"
        );
        assert_eq!(
            EndTransactionOpenTracingHookImpl::new(()).hook_name(),
            "EndTransactionOpenTracingHook"
        );
    }

    #[test]
    fn legacy_transaction_check_listener_is_callable() {
        struct Listener;
        impl TransactionCheckListener for Listener {
            fn check_local_transaction_state(&self, _: &MessageExt) -> LocalTransactionState {
                LocalTransactionState::CommitMessage
            }
        }
        assert_eq!(
            Listener.check_local_transaction_state(&MessageExt::default()),
            LocalTransactionState::CommitMessage
        );
    }

    #[test]
    fn impl_package_markers_do_not_panic() {
        assert!(DoNothingClientRemotingProcessor::new().process_request().is_none());
    }
}
