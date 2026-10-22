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

#![allow(deprecated)]

use crate::consumer::message_queue_listener::ArcMessageQueueListener;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::ClientError;
use crate::ClientResult;
use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_queue::MessageQueue;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::default_mq_pull_consumer_impl::DefaultMQPullConsumerImpl;
use crate::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use crate::consumer::default_mq_pull_consumer_builder::DefaultMQPullConsumerBuilder;
use crate::consumer::message_queue_listener::MessageQueueListener;
use crate::consumer::message_selector::MessageSelector;
use crate::consumer::pull_result::PullResult;
use crate::runtime::ClientRuntime;
use tokio_util::sync::CancellationToken;

/// Explicit cancellation/observation for one owned callback request. Dropping this handle
/// retains the Java callback contract; call `cancel` to interrupt a long poll.
#[derive(Clone)]
pub struct ClassicPullRequest {
    cancellation: CancellationToken,
    finished: Arc<std::sync::atomic::AtomicBool>,
}

impl ClassicPullRequest {
    pub(crate) fn new(cancellation: CancellationToken, finished: Arc<std::sync::atomic::AtomicBool>) -> Self {
        Self { cancellation, finished }
    }
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
    pub fn is_finished(&self) -> bool {
        self.finished.load(std::sync::atomic::Ordering::Acquire)
    }
}

/// Validated parameters for one Classic Pull request.
#[derive(Debug, Clone)]
pub struct PullOptions {
    message_queue: MessageQueue,
    selector: MessageSelector,
    offset: i64,
    max_messages: i32,
    max_size_in_bytes: i32,
    timeout: Duration,
    broker_suspend_timeout: Duration,
    block_if_not_found: bool,
}

impl PullOptions {
    /// Creates validated pull options with Java-compatible ordinary-pull defaults.
    ///
    /// # Errors
    ///
    /// Returns an error when the queue has no topic or broker, the offset is negative, or the
    /// requested message count is not positive.
    pub fn new(
        message_queue: MessageQueue,
        selector: MessageSelector,
        offset: i64,
        max_messages: i32,
    ) -> ClientResult<Self> {
        let options = Self {
            message_queue,
            selector,
            offset,
            max_messages,
            max_size_in_bytes: i32::MAX,
            timeout: Duration::from_secs(10),
            broker_suspend_timeout: Duration::from_secs(20),
            block_if_not_found: false,
        };
        options.validate()?;
        Ok(options)
    }

    /// Sets the maximum response payload size accepted from the broker.
    pub fn max_size_in_bytes(mut self, max_size_in_bytes: i32) -> Self {
        self.max_size_in_bytes = max_size_in_bytes;
        self
    }

    /// Sets the client-side request timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the broker-side long-poll suspension timeout.
    pub fn broker_suspend_timeout(mut self, timeout: Duration) -> Self {
        self.broker_suspend_timeout = timeout;
        self
    }

    /// Enables or disables block-if-not-found long polling.
    pub fn block_if_not_found(mut self, block_if_not_found: bool) -> Self {
        self.block_if_not_found = block_if_not_found;
        self
    }

    /// Validates the complete option set.
    ///
    /// # Errors
    ///
    /// Returns an error for an incomplete queue, negative offset, non-positive count or size,
    /// zero timeout, or a block-if-not-found timeout below broker suspension.
    pub fn validate(&self) -> ClientResult<()> {
        if self.message_queue.topic().is_empty() {
            return Err(crate::mq_client_err!("message queue topic is empty"));
        }
        if self.message_queue.broker_name().is_empty() {
            return Err(crate::mq_client_err!("message queue broker name is empty"));
        }
        if self.offset < 0 {
            return Err(crate::mq_client_err!("offset < 0"));
        }
        if self.max_messages <= 0 {
            return Err(crate::mq_client_err!("maxNums <= 0"));
        }
        if self.max_size_in_bytes <= 0 {
            return Err(crate::mq_client_err!("maxSizeInBytes <= 0"));
        }
        if self.timeout.is_zero() {
            return Err(crate::mq_client_err!("pull timeout must be positive"));
        }
        if self.broker_suspend_timeout.is_zero() {
            return Err(crate::mq_client_err!("broker suspend timeout must be positive"));
        }
        // Custom fork: fixed Java permits equality (its check uses strictly less than).
        if self.block_if_not_found && self.timeout < self.broker_suspend_timeout {
            return Err(crate::mq_client_err!(
                "block-if-not-found timeout must exceed broker suspend timeout"
            ));
        }
        Ok(())
    }

    /// Returns the target message queue.
    pub fn message_queue(&self) -> &MessageQueue {
        &self.message_queue
    }

    /// Returns the server-side selector.
    pub fn selector(&self) -> &MessageSelector {
        &self.selector
    }

    /// Returns the requested queue offset.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the maximum number of messages requested.
    pub fn max_messages(&self) -> i32 {
        self.max_messages
    }

    /// Returns the maximum response payload size.
    pub fn max_size_in_bytes_value(&self) -> i32 {
        self.max_size_in_bytes
    }

    /// Returns the client-side timeout.
    pub fn timeout_value(&self) -> Duration {
        self.timeout
    }

    /// Returns the broker-side suspension timeout.
    pub fn broker_suspend_timeout_value(&self) -> Duration {
        self.broker_suspend_timeout
    }

    /// Returns whether block-if-not-found is enabled.
    pub fn is_block_if_not_found(&self) -> bool {
        self.block_if_not_found
    }

    pub(crate) fn with_message_queue(mut self, message_queue: MessageQueue) -> Self {
        self.message_queue = message_queue;
        self
    }
}

/// Callback for an asynchronous Classic Pull request.
pub trait ClassicPullCallback: Send + Sync + 'static {
    /// Receives one fully decoded and filtered pull result.
    fn on_success(&self, pull_result: PullResult);

    /// Receives a transport, broker, timeout, or validation error.
    fn on_exception(&self, error: ClientError);
}

/// Function callback accepted by [`DefaultMQPullConsumer::pull_async_with_options`].
impl<F> ClassicPullCallback for F
where
    F: Fn(Result<PullResult, ClientError>) + Send + Sync + 'static,
{
    fn on_success(&self, pull_result: PullResult) {
        self(Ok(pull_result));
    }

    fn on_exception(&self, error: ClientError) {
        self(Err(error));
    }
}

/// Java Classic Pull compatibility facade backed by the shared Rust client runtime.
#[deprecated(
    since = "0.9.0",
    note = "Classic Pull is retained for compatibility; prefer DefaultLitePullConsumer for new applications"
)]
#[derive(Clone, Default)]
pub struct DefaultMQPullConsumer {
    consumer_group: Option<CheetahString>,
    implementation: Option<DefaultMQPullConsumerImpl>,
}

#[allow(deprecated)]
impl DefaultMQPullConsumer {
    pub(crate) fn from_implementation(implementation: DefaultMQPullConsumerImpl) -> ClientResult<Self> {
        Ok(Self {
            consumer_group: Some(implementation.lite_consumer()?.consumer_group()),
            implementation: Some(implementation),
        })
    }
    /// Creates a detached compatibility value.
    ///
    /// Use [`Self::builder`] to obtain a runnable consumer. The detached constructor is retained
    /// for source compatibility and fails closed on runtime operations.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a detached compatibility value with a group name.
    pub fn with_consumer_group(consumer_group: impl Into<CheetahString>) -> Self {
        Self {
            consumer_group: Some(consumer_group.into()),
            implementation: None,
        }
    }

    /// Returns a builder that requires an application-owned runtime.
    pub fn builder(client_runtime: Arc<ClientRuntime>) -> DefaultMQPullConsumerBuilder {
        DefaultMQPullConsumerBuilder::new(client_runtime)
    }

    pub(crate) fn from_lite_consumer(
        runtime: Arc<ClientRuntime>,
        consumer_group: CheetahString,
        lite_consumer: DefaultLitePullConsumer,
        consumer_pull_timeout: Duration,
        broker_suspend_timeout: Duration,
        consumer_timeout_when_suspend: Duration,
    ) -> ClientResult<Self> {
        Ok(Self {
            consumer_group: Some(consumer_group),
            implementation: Some(DefaultMQPullConsumerImpl::from_lite_consumer(
                runtime,
                lite_consumer,
                consumer_pull_timeout,
                broker_suspend_timeout,
                consumer_timeout_when_suspend,
            )?),
        })
    }

    fn implementation(&self) -> ClientResult<&DefaultMQPullConsumerImpl> {
        self.implementation.as_ref().ok_or_else(|| {
            ClientError::not_initialized(
                "DefaultMQPullConsumer has no ClientRuntime; create it with DefaultMQPullConsumer::builder",
            )
        })
    }

    // Custom fork: protocol/runtime reuse does not expose LitePull's polling contract.
    pub(crate) fn shared_consumer(&self) -> ClientResult<&DefaultLitePullConsumer> {
        self.implementation()?.lite_consumer()
    }

    pub async fn pull_with_cancellation(
        &self,
        options: PullOptions,
        cancellation: &CancellationToken,
    ) -> ClientResult<PullResult> {
        self.implementation()?
            .pull_with_cancellation(options, cancellation)
            .await
    }

    pub async fn start_pull_request<C: ClassicPullCallback>(
        &self,
        options: PullOptions,
        callback: C,
    ) -> ClientResult<ClassicPullRequest> {
        self.implementation()?.start_pull_request(options, callback).await
    }

    pub async fn pull_batch(
        &self,
        options: PullOptions,
    ) -> ClientResult<crate::consumer::classic_pull_business::ClassicPullBatch> {
        let queue = options.message_queue().clone();
        let result = self.pull_with_options(options).await?;
        crate::consumer::classic_pull_business::ClassicPullBatch::new(self, queue, result)
    }

    pub async fn fetch_message_queues_in_balance(&self, topic: &str) -> ClientResult<HashSet<MessageQueue>> {
        self.implementation()?.fetch_message_queues_in_balance(topic).await
    }

    pub async fn persist_consumer_offset(&self) -> ClientResult<()> {
        self.implementation()?.persist_consumer_offset().await
    }

    pub async fn update_consume_offset_to_broker(
        &self,
        queue: &MessageQueue,
        offset: i64,
        oneway: bool,
    ) -> ClientResult<()> {
        self.implementation()?
            .update_consume_offset_to_broker(queue, offset, oneway)
            .await
    }

    pub async fn send_message_back(
        &self,
        message: &mut rocketmq_model::common::message::message_ext::MessageExt,
        delay_level: i32,
        broker: Option<&str>,
        group: Option<&str>,
    ) -> ClientResult<crate::consumer::classic_pull_business::ClassicSendBackOutcome> {
        self.implementation()?
            .send_message_back(message, delay_level, broker, group)
            .await
    }

    pub fn max_reconsume_times(&self) -> ClientResult<i32> {
        self.implementation()?.max_reconsume_times()
    }

    pub fn message_queue_listener(&self) -> ClientResult<Option<ArcMessageQueueListener>> {
        self.implementation()?.message_queue_listener()
    }
    pub fn set_message_queue_listener(&self, listener: Option<ArcMessageQueueListener>) -> ClientResult<()> {
        self.implementation()?.set_message_queue_listener(listener)
    }
    pub fn set_max_reconsume_times(&self, value: i32) -> ClientResult<()> {
        self.implementation()?.set_max_reconsume_times(value)
    }

    pub async fn register_consume_message_hook(
        &self,
        hook: Arc<dyn crate::hook::consume_message_hook::ConsumeMessageHook + Send + Sync>,
    ) -> ClientResult<()> {
        self.shared_consumer()?
            .get_or_init_impl()
            .await?
            .register_consume_message_hook(hook);
        Ok(())
    }

    pub async fn register_filter_message_hook(
        &self,
        hook: Arc<dyn crate::hook::filter_message_hook::FilterMessageHook + Send + Sync>,
    ) -> ClientResult<()> {
        self.shared_consumer()?
            .get_or_init_impl()
            .await?
            .register_classic_filter_hook(hook);
        Ok(())
    }

    /// Custom: native configuration collection for Java get/setRegisterTopics.
    pub async fn register_topics(&self) -> ClientResult<HashSet<CheetahString>> {
        Ok(self
            .shared_consumer()?
            .get_or_init_impl()
            .await?
            .classic_registered_topics())
    }
    pub async fn set_register_topics(&self, topics: HashSet<CheetahString>) -> ClientResult<()> {
        for topic in &topics {
            crate::Validators::check_topic(topic)?;
        }
        let previous = self.register_topics().await?;
        for topic in previous.difference(&topics) {
            self.unsubscribe(topic).await?;
        }
        for topic in topics.difference(&previous) {
            self.register_topic(topic).await?;
        }
        Ok(())
    }

    /// Registers the topic even when Java's optional listener is absent.
    pub async fn register_topic(&self, topic: &str) -> ClientResult<()> {
        let consumer = self.shared_consumer()?;
        let topic = consumer.classic_topic_with_namespace(topic);
        consumer.register_classic_pull_subscription(&topic).await
    }

    pub async fn unsubscribe(&self, topic: &str) -> ClientResult<()> {
        let consumer = self.shared_consumer()?;
        let topic = consumer.classic_topic_with_namespace(topic);
        consumer.get_or_init_impl().await?.unsubscribe(topic).await
    }

    pub async fn message_model(
        &self,
    ) -> ClientResult<rocketmq_protocol::protocol::heartbeat::message_model::MessageModel> {
        Ok(self.shared_consumer()?.message_model().await)
    }
    pub async fn set_message_model(
        &self,
        value: rocketmq_protocol::protocol::heartbeat::message_model::MessageModel,
    ) -> ClientResult<()> {
        self.shared_consumer()?.set_message_model(value).await;
        Ok(())
    }
    pub async fn is_unit_mode(&self) -> ClientResult<bool> {
        Ok(self.shared_consumer()?.is_unit_mode().await)
    }
    pub async fn set_unit_mode(&self, value: bool) -> ClientResult<()> {
        self.shared_consumer()?.set_unit_mode(value).await;
        Ok(())
    }
    pub async fn offset_store(&self) -> ClientResult<Option<Arc<crate::consumer::store::offset_store::OffsetStore>>> {
        Ok(self.shared_consumer()?.offset_store().await)
    }
    pub async fn set_offset_store(
        &self,
        store: Option<Arc<crate::consumer::store::offset_store::OffsetStore>>,
    ) -> ClientResult<()> {
        self.shared_consumer()?.set_offset_store(store).await
    }
    pub async fn allocate_message_queue_strategy(
        &self,
    ) -> ClientResult<
        Arc<dyn crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy + Send + Sync>,
    > {
        Ok(self.shared_consumer()?.allocate_message_queue_strategy().await)
    }
    pub async fn set_allocate_message_queue_strategy(
        &self,
        strategy: Arc<dyn crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy + Send + Sync>,
    ) -> ClientResult<()> {
        self.shared_consumer()?
            .set_allocate_message_queue_strategy(strategy)
            .await;
        Ok(())
    }

    pub fn get_consumer_group(&self) -> ClientResult<CheetahString> {
        Ok(self.shared_consumer()?.consumer_group())
    }
    pub fn set_consumer_group(&self, group: impl Into<CheetahString>) -> ClientResult<()> {
        self.shared_consumer()?.set_consumer_group(group)
    }

    pub async fn broker_suspend_max_time_millis(&self) -> ClientResult<u64> {
        Ok(self.shared_consumer()?.broker_suspend_max_time_millis().await)
    }
    pub async fn set_broker_suspend_max_time_millis(&self, value: u64) -> ClientResult<()> {
        self.shared_consumer()?.set_broker_suspend_max_time_millis(value).await;
        Ok(())
    }
    pub async fn consumer_timeout_millis_when_suspend(&self) -> ClientResult<u64> {
        Ok(self.shared_consumer()?.consumer_timeout_millis_when_suspend().await)
    }
    pub async fn set_consumer_timeout_millis_when_suspend(&self, value: u64) -> ClientResult<()> {
        self.shared_consumer()?
            .set_consumer_timeout_millis_when_suspend(value)
            .await;
        Ok(())
    }
    pub async fn consumer_pull_timeout_millis(&self) -> ClientResult<u64> {
        Ok(self.shared_consumer()?.consumer_pull_timeout_millis().await)
    }
    pub async fn set_consumer_pull_timeout_millis(&self, value: u64) -> ClientResult<()> {
        self.shared_consumer()?.set_consumer_pull_timeout_millis(value).await;
        Ok(())
    }

    pub async fn earliest_msg_store_time(&self, queue: &MessageQueue) -> ClientResult<i64> {
        self.shared_consumer()?.earliest_msg_store_time(queue).await
    }
    pub async fn create_topic(&self, key: &str, topic: &str, queues: i32, topic_sys_flag: i32) -> ClientResult<()> {
        self.shared_consumer()?
            .create_topic_with_flag(key, topic, queues, topic_sys_flag, Default::default())
            .await
    }
    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max: i32,
        begin: u64,
        end: u64,
    ) -> ClientResult<crate::base::query_result::QueryResult> {
        self.shared_consumer()?.query_message(topic, key, max, begin, end).await
    }
    pub async fn view_message(
        &self,
        topic: &str,
        id: &str,
    ) -> ClientResult<rocketmq_model::common::message::message_ext::MessageExt> {
        self.shared_consumer()?.view_message(topic, id).await
    }

    /// Returns the initial constructor group. Use `get_consumer_group` for the effective
    /// shared configuration, including namespace or a subsequent group setter.
    pub fn consumer_group(&self) -> Option<&CheetahString> {
        self.consumer_group.as_ref()
    }

    /// Returns the client configuration used by this consumer.
    ///
    /// # Errors
    ///
    /// Returns an initialization error for a detached compatibility value.
    pub fn client_config(&self) -> ClientResult<Arc<ClientConfig>> {
        self.implementation()?.client_config()
    }

    /// Starts the consumer.
    ///
    /// # Errors
    ///
    /// Returns an initialization error for a detached value, a stable lifecycle error when the
    /// consumer was already started or shut down, or the underlying client startup error.
    pub async fn start(&self) -> ClientResult<()> {
        self.implementation()?.start().await
    }

    /// Shuts the consumer down and awaits its runtime-owned client and rebalance tasks.
    ///
    /// # Errors
    ///
    /// Returns an initialization error for a detached value or an underlying client shutdown
    /// error. Repeated shutdown is idempotent.
    pub async fn shutdown(&self) -> ClientResult<()> {
        self.implementation()?.shutdown().await
    }

    /// Returns whether the consumer is running.
    pub async fn is_running(&self) -> bool {
        match &self.implementation {
            Some(implementation) => implementation.is_running().await,
            None => false,
        }
    }

    /// Pulls by tag expression using the default ordinary-pull timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is not running, the arguments are invalid, route or
    /// transport resolution fails, the broker rejects the request, or the request times out.
    pub async fn pull(
        &self,
        message_queue: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_messages: i32,
    ) -> ClientResult<PullResult> {
        let implementation = self.implementation()?;
        let options = PullOptions::new(
            message_queue.clone(),
            MessageSelector::by_tag(sub_expression),
            offset,
            max_messages,
        )?
        .timeout(implementation.consumer_pull_timeout())
        .broker_suspend_timeout(implementation.broker_suspend_timeout());
        implementation.pull_with_options(options).await
    }

    /// Pulls with an explicit TAG or SQL selector.
    ///
    /// # Errors
    ///
    /// Returns the same validation, lifecycle, transport, broker, and timeout errors as
    /// [`Self::pull`].
    pub async fn pull_with_selector(
        &self,
        message_queue: &MessageQueue,
        selector: MessageSelector,
        offset: i64,
        max_messages: i32,
    ) -> ClientResult<PullResult> {
        let implementation = self.implementation()?;
        implementation
            .pull_with_options(
                PullOptions::new(message_queue.clone(), selector, offset, max_messages)?
                    .timeout(implementation.consumer_pull_timeout())
                    .broker_suspend_timeout(implementation.broker_suspend_timeout()),
            )
            .await
    }

    /// Starts an asynchronous tag pull using the Java-compatible narrow signature.
    ///
    /// # Errors
    ///
    /// Returns validation, lifecycle or admission errors before the owned task is accepted.
    /// Route, transport, Broker and timeout failures are delivered through the callback;
    /// acceptance does not mean a frame was sent or the Broker accepted it.
    pub async fn pull_async<C>(
        &self,
        message_queue: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_messages: i32,
        callback: C,
    ) -> ClientResult<()>
    where
        C: ClassicPullCallback,
    {
        let implementation = self.implementation()?;
        let options = PullOptions::new(
            message_queue.clone(),
            MessageSelector::by_tag(sub_expression),
            offset,
            max_messages,
        )?
        .timeout(implementation.consumer_pull_timeout())
        .broker_suspend_timeout(implementation.broker_suspend_timeout());
        implementation.pull_async_with_options(options, callback).await
    }

    /// Starts an asynchronous pull with an explicit TAG or SQL selector.
    ///
    /// # Errors
    ///
    /// Returns validation, lifecycle or admission errors before the owned task is accepted.
    /// Route, transport, Broker and timeout failures are delivered through the callback;
    /// acceptance does not mean a frame was sent or the Broker accepted it.
    pub async fn pull_async_with_selector<C>(
        &self,
        message_queue: &MessageQueue,
        selector: MessageSelector,
        offset: i64,
        max_messages: i32,
        callback: C,
    ) -> ClientResult<()>
    where
        C: ClassicPullCallback,
    {
        let implementation = self.implementation()?;
        let options = PullOptions::new(message_queue.clone(), selector, offset, max_messages)?
            .timeout(implementation.consumer_pull_timeout())
            .broker_suspend_timeout(implementation.broker_suspend_timeout());
        implementation.pull_async_with_options(options, callback).await
    }

    /// Pulls using a fully typed option set.
    ///
    /// # Errors
    ///
    /// Returns a validation, lifecycle, route, transport, broker, or timeout error.
    pub async fn pull_with_options(&self, options: PullOptions) -> ClientResult<PullResult> {
        self.implementation()?.pull_with_options(options).await
    }

    /// Pulls with broker suspension when no message is immediately available.
    ///
    /// # Errors
    ///
    /// Returns a validation, lifecycle, route, transport, broker, or timeout error.
    pub async fn pull_block_if_not_found(
        &self,
        message_queue: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_messages: i32,
    ) -> ClientResult<PullResult> {
        let implementation = self.implementation()?;
        let options = PullOptions::new(
            message_queue.clone(),
            MessageSelector::by_tag(sub_expression),
            offset,
            max_messages,
        )?
        .timeout(implementation.consumer_timeout_when_suspend())
        .broker_suspend_timeout(implementation.broker_suspend_timeout())
        .block_if_not_found(true);
        implementation.pull_with_options(options).await
    }

    /// Starts an asynchronous pull whose completion runs on the client's callback executor.
    ///
    /// # Errors
    ///
    /// Returns validation, lifecycle or admission errors before the owned task is accepted.
    /// Route, transport, Broker and timeout failures are delivered through the callback;
    /// acceptance does not mean a frame was sent or the Broker accepted it.
    pub async fn pull_async_with_options<C>(&self, options: PullOptions, callback: C) -> ClientResult<()>
    where
        C: ClassicPullCallback,
    {
        self.implementation()?.pull_async_with_options(options, callback).await
    }

    /// Starts an asynchronous block-if-not-found pull.
    ///
    /// # Errors
    ///
    /// Returns validation, lifecycle or admission errors before the owned task is accepted.
    /// Route, transport, Broker and timeout failures are delivered through the callback;
    /// acceptance does not mean a frame was sent or the Broker accepted it.
    pub async fn pull_block_if_not_found_async<C>(
        &self,
        message_queue: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_messages: i32,
        callback: C,
    ) -> ClientResult<()>
    where
        C: ClassicPullCallback,
    {
        let implementation = self.implementation()?;
        let options = PullOptions::new(
            message_queue.clone(),
            MessageSelector::by_tag(sub_expression),
            offset,
            max_messages,
        )?
        .timeout(implementation.consumer_timeout_when_suspend())
        .broker_suspend_timeout(implementation.broker_suspend_timeout())
        .block_if_not_found(true);
        implementation.pull_async_with_options(options, callback).await
    }

    /// Fetches all readable queues for a topic.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is not running or topic route lookup fails.
    pub async fn fetch_subscribe_message_queues(&self, topic: &str) -> ClientResult<Vec<MessageQueue>> {
        self.implementation()?.fetch_subscribe_message_queues(topic).await
    }

    /// Registers a listener for stable topic queue-set changes.
    ///
    /// # Errors
    ///
    /// Returns an error for a blank topic, a detached consumer, or a failed running-consumer
    /// subscription update.
    pub async fn register_message_queue_listener<L>(&self, topic: &str, listener: L) -> ClientResult<()>
    where
        L: MessageQueueListener + 'static,
    {
        self.implementation()?
            .register_message_queue_listener(topic, Arc::new(listener))
            .await
    }

    /// Updates the local consume offset for a queue.
    /// Signed values follow fixed Java's offset-store update contract. This does not change an
    /// explicit pull request's cursor; pull requests validate their offsets separately.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is not running or the offset store is unavailable.
    pub async fn update_consume_offset(&self, message_queue: &MessageQueue, offset: i64) -> ClientResult<()> {
        self.implementation()?
            .update_consume_offset(message_queue, offset)
            .await
    }

    /// Fetches a queue offset from memory or the configured offset store.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is not running, the offset store is unavailable, or a
    /// broker-backed read fails.
    pub async fn fetch_consume_offset(&self, message_queue: &MessageQueue, from_store: bool) -> ClientResult<i64> {
        self.implementation()?
            .fetch_consume_offset(message_queue, from_store)
            .await
    }

    /// Searches the queue offset nearest to a store timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is not running or the broker query fails.
    pub async fn search_offset(&self, message_queue: &MessageQueue, timestamp: u64) -> ClientResult<i64> {
        self.implementation()?.search_offset(message_queue, timestamp).await
    }

    /// Complete Java long range, including negative timestamp sentinels.
    pub async fn search_offset_signed(&self, message_queue: &MessageQueue, timestamp: i64) -> ClientResult<i64> {
        self.implementation()?
            .search_offset_signed(message_queue, timestamp)
            .await
    }

    /// Returns the maximum queue offset.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is not running or the broker query fails.
    pub async fn max_offset(&self, message_queue: &MessageQueue) -> ClientResult<i64> {
        self.implementation()?.max_offset(message_queue).await
    }

    /// Returns the minimum queue offset.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is not running or the broker query fails.
    pub async fn min_offset(&self, message_queue: &MessageQueue) -> ClientResult<i64> {
        self.implementation()?.min_offset(message_queue).await
    }

    /// Returns the functional implementation handle.
    ///
    /// # Errors
    ///
    /// Returns an initialization error for a detached compatibility value.
    pub fn default_mq_pull_consumer_impl(&self) -> ClientResult<DefaultMQPullConsumerImpl> {
        self.implementation().cloned()
    }
}

/// Common async surface implemented by Classic Pull facades.
#[allow(async_fn_in_trait)]
#[deprecated(
    since = "0.9.0",
    note = "Classic Pull is retained for compatibility; prefer DefaultLitePullConsumer for new applications"
)]
pub trait MQPullConsumer: Send + Sync {
    /// Canonical entry for every Java selector, timeout and long-poll overload.
    async fn pull_with_options(&self, options: PullOptions) -> ClientResult<PullResult>;
    async fn start_pull_request<C: ClassicPullCallback>(
        &self,
        options: PullOptions,
        callback: C,
    ) -> ClientResult<ClassicPullRequest>;
    async fn register_message_queue_listener<L: MessageQueueListener + 'static>(
        &self,
        topic: &str,
        listener: L,
    ) -> ClientResult<()>;
    async fn update_consume_offset(&self, queue: &MessageQueue, offset: i64) -> ClientResult<()>;
    async fn fetch_consume_offset(&self, queue: &MessageQueue, from_store: bool) -> ClientResult<i64>;
    async fn fetch_message_queues_in_balance(&self, topic: &str) -> ClientResult<HashSet<MessageQueue>>;
    async fn send_message_back(
        &self,
        message: &mut rocketmq_model::common::message::message_ext::MessageExt,
        delay_level: i32,
        broker: Option<&str>,
        group: Option<&str>,
    ) -> ClientResult<crate::consumer::classic_pull_business::ClassicSendBackOutcome>;
    /// Starts the consumer.
    async fn start(&self) -> ClientResult<()>;

    /// Shuts the consumer down.
    async fn shutdown(&self) -> ClientResult<()>;

    /// Pulls one queue by tag expression.
    async fn pull(
        &self,
        message_queue: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_messages: i32,
    ) -> ClientResult<PullResult>;
}

#[allow(deprecated)]
impl MQPullConsumer for DefaultMQPullConsumer {
    async fn pull_with_options(&self, options: PullOptions) -> ClientResult<PullResult> {
        DefaultMQPullConsumer::pull_with_options(self, options).await
    }
    async fn start_pull_request<C: ClassicPullCallback>(
        &self,
        options: PullOptions,
        callback: C,
    ) -> ClientResult<ClassicPullRequest> {
        DefaultMQPullConsumer::start_pull_request(self, options, callback).await
    }
    async fn register_message_queue_listener<L: MessageQueueListener + 'static>(
        &self,
        topic: &str,
        listener: L,
    ) -> ClientResult<()> {
        DefaultMQPullConsumer::register_message_queue_listener(self, topic, listener).await
    }
    async fn update_consume_offset(&self, queue: &MessageQueue, offset: i64) -> ClientResult<()> {
        DefaultMQPullConsumer::update_consume_offset(self, queue, offset).await
    }
    async fn fetch_consume_offset(&self, queue: &MessageQueue, from_store: bool) -> ClientResult<i64> {
        DefaultMQPullConsumer::fetch_consume_offset(self, queue, from_store).await
    }
    async fn fetch_message_queues_in_balance(&self, topic: &str) -> ClientResult<HashSet<MessageQueue>> {
        DefaultMQPullConsumer::fetch_message_queues_in_balance(self, topic).await
    }
    async fn send_message_back(
        &self,
        message: &mut rocketmq_model::common::message::message_ext::MessageExt,
        delay_level: i32,
        broker: Option<&str>,
        group: Option<&str>,
    ) -> ClientResult<crate::consumer::classic_pull_business::ClassicSendBackOutcome> {
        DefaultMQPullConsumer::send_message_back(self, message, delay_level, broker, group).await
    }
    async fn start(&self) -> ClientResult<()> {
        DefaultMQPullConsumer::start(self).await
    }

    async fn shutdown(&self) -> ClientResult<()> {
        DefaultMQPullConsumer::shutdown(self).await
    }

    async fn pull(
        &self,
        message_queue: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_messages: i32,
    ) -> ClientResult<PullResult> {
        DefaultMQPullConsumer::pull(self, message_queue, sub_expression, offset, max_messages).await
    }
}

// Custom fork: all fixed Java MQAdmin overloads through one native async trait.
impl crate::MQAdmin for DefaultMQPullConsumer {
    fn admin_context(&self) -> crate::ClientResult<crate::base::mq_admin::MQAdminContext> {
        self.shared_consumer()?.admin_context()
    }
}
