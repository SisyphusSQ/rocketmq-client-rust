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

use std::collections::HashSet;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::RwLock as StdRwLock;
use std::time::Duration;

use crate::runtime::{spawn_client_tracked_task_with_context, ClientRuntime, ClientTrackedTaskHandle};
use crate::ClientError;
use crate::ClientResult;
use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_protocol::protocol::namespace_util::NamespaceUtil;
use rocketmq_runtime::ChildServiceContext;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::ServiceState;
use crate::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use crate::consumer::default_mq_pull_consumer::ClassicPullCallback;
use crate::consumer::default_mq_pull_consumer::PullOptions;
use crate::consumer::message_queue_listener::ArcMessageQueueListener;
use crate::consumer::pull_result::PullResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassicPullServiceState {
    Created,
    Running,
    Failed,
    Shutdown,
}

struct ClassicPullCore {
    // Custom fork: every callback pull belongs to this consumer, including shared connections.
    service_context: ChildServiceContext,
    stop: CancellationToken,
    requests: StdMutex<Vec<ClientTrackedTaskHandle>>,
    async_admission: Arc<Semaphore>,
    max_reconsume_times: AtomicI32,
    start_timestamp: i64,
    lite_consumer: DefaultLitePullConsumer,
    state: Mutex<ClassicPullServiceState>,
    listeners: Arc<StdRwLock<Option<ArcMessageQueueListener>>>,
}

struct RequestCompletion(Arc<std::sync::atomic::AtomicBool>);
impl Drop for RequestCompletion {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

struct ClassicPullQueueListener {
    listeners: Arc<StdRwLock<Option<ArcMessageQueueListener>>>,
    namespace: Option<CheetahString>,
}

impl ClassicPullQueueListener {
    fn new(listeners: Arc<StdRwLock<Option<ArcMessageQueueListener>>>) -> Self {
        Self {
            listeners,
            namespace: None,
        }
    }

    fn with_namespace(mut self, namespace: Option<CheetahString>) -> Self {
        self.namespace = namespace.filter(|namespace| !namespace.is_empty());
        self
    }

    fn without_namespace(&self, resource: &str) -> CheetahString {
        match &self.namespace {
            Some(namespace) => NamespaceUtil::without_namespace_with_namespace(resource, namespace.as_str()).into(),
            None => NamespaceUtil::without_namespace(resource).into(),
        }
    }

    fn queues_without_namespace(&self, queues: &HashSet<MessageQueue>) -> HashSet<MessageQueue> {
        queues
            .iter()
            .map(|queue| {
                MessageQueue::from_parts(
                    self.without_namespace(queue.topic()),
                    queue.broker_name().clone(),
                    queue.queue_id(),
                )
            })
            .collect()
    }
}

impl crate::consumer::message_queue_listener::MessageQueueListener for ClassicPullQueueListener {
    fn message_queue_changed(&self, topic: &str, mq_all: &HashSet<MessageQueue>, mq_divided: &HashSet<MessageQueue>) {
        let topic = self.without_namespace(topic);
        let listener = self
            .listeners
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(listener) = listener {
            let mq_all = self.queues_without_namespace(mq_all);
            let mq_divided = self.queues_without_namespace(mq_divided);
            listener.message_queue_changed(topic.as_str(), &mq_all, &mq_divided);
        }
    }
}

impl ClassicPullCore {
    async fn start(self: &Arc<Self>) -> ClientResult<()> {
        let mut state = self.state.lock().await;
        match *state {
            ClassicPullServiceState::Created => {}
            ClassicPullServiceState::Running => {
                return Err(crate::mq_client_err!("DefaultMQPullConsumer already started"));
            }
            ClassicPullServiceState::Failed => {
                return Err(crate::mq_client_err!(
                    "DefaultMQPullConsumer start previously failed; create a new consumer"
                ));
            }
            ClassicPullServiceState::Shutdown => {
                return Err(crate::mq_client_err!(
                    "DefaultMQPullConsumer has been shut down; create a new consumer"
                ));
            }
        }

        if let Err(error) = self.lite_consumer.start().await {
            *state = ClassicPullServiceState::Failed;
            return Err(error);
        }
        *state = ClassicPullServiceState::Running;
        Ok(())
    }

    async fn shutdown(&self) -> ClientResult<()> {
        let mut state = self.state.lock().await;
        match *state {
            ClassicPullServiceState::Shutdown => return Ok(()),
            ClassicPullServiceState::Created => {
                *state = ClassicPullServiceState::Shutdown;
                return Ok(());
            }
            ClassicPullServiceState::Running | ClassicPullServiceState::Failed => {}
        }
        self.stop.cancel();
        let requests = std::mem::take(&mut *self.requests.lock().unwrap_or_else(|p| p.into_inner()));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let mut stopped = true;
        for request in requests {
            stopped &= request
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await
                .is_healthy();
        }
        let persisted = self.persist_offsets().await;
        self.lite_consumer.shutdown().await;
        *state = ClassicPullServiceState::Shutdown;
        if !stopped {
            return Err(ClientError::timeout("classic pull callback shutdown", 1000));
        }
        persisted
    }

    async fn ensure_running(&self) -> ClientResult<()> {
        let root = self.service_context.task_group().cancellation_token();
        // A shutdown holds the lifecycle lock while joining callback requests.
        // A request already waiting for that lock must still observe cancellation.
        let state = tokio::select! {
            biased;
            _ = self.stop.cancelled() => return Err(ClientError::invalid_state("running Classic Pull consumer", "cancelled")),
            _ = root.cancelled() => return Err(ClientError::invalid_state("running Classic Pull consumer", "runtime cancelled")),
            state = self.state.lock() => state,
        };
        if *state != ClassicPullServiceState::Running {
            return Err(ClientError::not_initialized(
                "DefaultMQPullConsumer not started. Call start() first.",
            ));
        }
        self.lite_consumer.try_impl_()?.ensure_classic_pull_running()
    }

    async fn persist_offsets(&self) -> ClientResult<()> {
        let inner = self.lite_consumer.try_impl_()?;
        let queues = inner
            .classic_rebalance()
            .rebalance_impl_inner
            .process_queue_table
            .read()
            .await
            .keys()
            .cloned()
            .collect();
        match inner.offset_store() {
            Some(store) => store.persist_all_checked(&queues).await,
            None => Ok(()),
        }
    }

    async fn pull(&self, options: PullOptions, cancel: &CancellationToken) -> ClientResult<PullResult> {
        self.ensure_running().await?;
        options.validate()?;
        let queue = self.lite_consumer.queue_with_namespace(options.message_queue());
        let options = options.with_message_queue(queue);
        let root = self.service_context.task_group().cancellation_token();
        tokio::select! {
            biased;
            _ = self.stop.cancelled() => Err(ClientError::invalid_state("active Classic Pull request", "consumer stopped")),
            _ = root.cancelled() => Err(ClientError::invalid_state("active Classic Pull request", "runtime stopped")),
            _ = cancel.cancelled() => Err(ClientError::invalid_state("active Classic Pull request", "caller cancelled")),
            result = self.lite_consumer.try_impl_()?.classic_pull(&options) => result,
        }
    }
}

/// Functional implementation handle retained under the Java compatibility name.
#[deprecated(
    since = "0.9.0",
    note = "Classic Pull is retained for compatibility; prefer DefaultLitePullConsumer for new applications"
)]
#[derive(Clone, Default)]
pub struct DefaultMQPullConsumerImpl {
    core: Option<Arc<ClassicPullCore>>,
}

#[allow(deprecated)]
impl DefaultMQPullConsumerImpl {
    // Custom fork: Java impl overloads share the same complete public consumer handle.
    pub fn as_consumer(&self) -> ClientResult<crate::DefaultMQPullConsumer> {
        crate::DefaultMQPullConsumer::from_implementation(self.clone())
    }
    pub async fn pull_api_wrapper(
        &self,
    ) -> ClientResult<Option<Arc<crate::consumer::consumer_impl::pull_api_wrapper::PullAPIWrapper>>> {
        Ok(self.lite_consumer()?.get_or_init_impl().await?.pull_api_wrapper())
    }
    pub async fn set_pull_api_wrapper(
        &self,
        wrapper: Arc<crate::consumer::consumer_impl::pull_api_wrapper::PullAPIWrapper>,
    ) -> ClientResult<()> {
        self.lite_consumer()?
            .get_or_init_impl()
            .await?
            .set_pull_api_wrapper(wrapper)
    }
    /// Java's mutable state bean is expressed through resource-owning transitions.
    pub async fn set_service_state(&self, state: ClassicPullServiceState) -> ClientResult<()> {
        match state {
            ClassicPullServiceState::Running => self.start().await,
            ClassicPullServiceState::Shutdown => self.shutdown().await,
            other if self.service_state().await? == other => Ok(()),
            _ => Err(ClientError::illegal_argument(
                "cannot reset a resource-owning consumer state",
            )),
        }
    }
    pub async fn prepare_components(&self) -> ClientResult<()> {
        self.lite_consumer()?.get_or_init_impl().await?;
        Ok(())
    }
    pub async fn service_state(&self) -> ClientResult<ClassicPullServiceState> {
        Ok(*self.core()?.state.lock().await)
    }
    pub fn consumer_start_timestamp(&self) -> ClientResult<i64> {
        Ok(self.core()?.start_timestamp)
    }
    pub fn group_name(&self) -> ClientResult<CheetahString> {
        Ok(self.lite_consumer()?.consumer_group())
    }
    pub fn message_model(&self) -> ClientResult<rocketmq_protocol::protocol::heartbeat::message_model::MessageModel> {
        Ok(self.lite_consumer()?.consumer_config().message_model)
    }
    pub fn consume_type(&self) -> rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType {
        rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType::ConsumeActively
    }
    pub fn consume_from_where(&self) -> rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere {
        rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere::ConsumeFromLastOffset
    }
    pub fn is_unit_mode(&self) -> ClientResult<bool> {
        Ok(self.lite_consumer()?.consumer_config().unit_mode)
    }
    pub fn subscriptions(
        &self,
    ) -> ClientResult<HashSet<rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData>> {
        use crate::consumer::mq_consumer_inner::MQConsumerInner;
        Ok(self
            .lite_consumer()?
            .try_impl_()
            .map_or_else(|_| HashSet::new(), |inner| inner.subscriptions()))
    }
    pub async fn do_rebalance(&self) -> ClientResult<()> {
        use crate::consumer::mq_consumer_inner::MQConsumerInner;
        let core = self.core()?;
        core.ensure_running().await?;
        core.lite_consumer.try_impl_()?.do_rebalance().await;
        Ok(())
    }
    pub async fn subscription_automatically(&self, topic: &str) -> ClientResult<()> {
        let consumer = self.lite_consumer()?;
        consumer
            .get_or_init_impl()
            .await?
            .classic_subscription_automatically(&consumer.classic_topic_with_namespace(topic))
    }
    pub fn parse_subscribe_message_queues(
        &self,
        queues: &HashSet<MessageQueue>,
    ) -> ClientResult<HashSet<MessageQueue>> {
        let namespace = self
            .lite_consumer()?
            .client_config()
            .resolved_namespace()
            .unwrap_or_default();
        Ok(queues
            .iter()
            .map(|queue| {
                MessageQueue::from_parts(
                    NamespaceUtil::without_namespace_with_namespace(queue.topic(), &namespace),
                    queue.broker_name().clone(),
                    queue.queue_id(),
                )
            })
            .collect())
    }
    pub fn reset_topic(
        &self,
        messages: &mut [rocketmq_model::common::message::message_ext::MessageExt],
    ) -> ClientResult<()> {
        let namespace = self
            .lite_consumer()?
            .client_config()
            .resolved_namespace()
            .unwrap_or_default();
        for message in messages {
            let topic = NamespaceUtil::without_namespace_with_namespace(message.topic(), &namespace);
            message.set_topic(CheetahString::from_string(topic));
        }
        Ok(())
    }
    pub fn execute_hook_before(&self, context: &mut crate::ConsumeMessageContext) -> ClientResult<()> {
        self.execute_hooks(context, true)
    }
    pub fn execute_hook_after(&self, context: &mut crate::ConsumeMessageContext) -> ClientResult<()> {
        self.execute_hooks(context, false)
    }
    fn execute_hooks(&self, context: &mut crate::ConsumeMessageContext, before: bool) -> ClientResult<()> {
        for hook in self.lite_consumer()?.try_impl_()?.classic_consume_hooks() {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if before {
                    hook.consume_message_before(context);
                } else {
                    hook.consume_message_after(context);
                }
            }))
            .is_err()
            {
                tracing::warn!("Classic Pull hook panicked");
            }
        }
        Ok(())
    }
    pub async fn update_topic_subscribe_info(&self, topic: &str, queues: &HashSet<MessageQueue>) -> ClientResult<()> {
        use crate::consumer::mq_consumer_inner::MQConsumerInner;
        let consumer = self.lite_consumer()?;
        let queues = queues
            .iter()
            .map(|queue| consumer.queue_with_namespace(queue))
            .collect();
        consumer
            .get_or_init_impl()
            .await?
            .update_topic_subscribe_info(consumer.classic_topic_with_namespace(topic), &queues)
            .await;
        Ok(())
    }
    pub async fn is_subscribe_topic_need_update(&self, topic: &str) -> ClientResult<bool> {
        use crate::consumer::mq_consumer_inner::MQConsumerInner;
        let consumer = self.lite_consumer()?;
        Ok(consumer
            .get_or_init_impl()
            .await?
            .is_subscribe_topic_need_update(&consumer.classic_topic_with_namespace(topic))
            .await)
    }
    pub fn mq_client_factory(&self) -> ClientResult<Arc<crate::factory::mq_client_instance::MQClientInstance>> {
        self.lite_consumer()?.try_impl_()?.classic_client_instance()
    }
    /// Creates a detached implementation marker for source compatibility.
    pub fn new() -> ClientResult<Self> {
        Ok(Self::default())
    }

    pub(crate) fn from_lite_consumer(
        runtime: Arc<ClientRuntime>,
        lite_consumer: DefaultLitePullConsumer,
        _consumer_pull_timeout: Duration,
        _broker_suspend_timeout: Duration,
        _consumer_timeout_when_suspend: Duration,
    ) -> ClientResult<Self> {
        let listeners = Arc::new(StdRwLock::new(None));
        let namespace = lite_consumer.client_config().resolved_namespace();
        lite_consumer.set_classic_pull_message_queue_listener(Some(Arc::new(
            ClassicPullQueueListener::new(listeners.clone()).with_namespace(namespace),
        )))?;
        Ok(Self {
            core: Some(Arc::new(ClassicPullCore {
                service_context: runtime.component("classic-pull-requests"),
                stop: CancellationToken::new(),
                requests: StdMutex::new(Vec::new()),
                // Fixed Java Netty's default asynchronous request semaphore.
                async_admission: Arc::new(Semaphore::new(65535)),
                max_reconsume_times: AtomicI32::new(16),
                start_timestamp: rocketmq_runtime::common::time_utils::current_millis() as i64,
                lite_consumer,
                state: Mutex::new(ClassicPullServiceState::Created),
                listeners,
            })),
        })
    }

    fn core(&self) -> ClientResult<&Arc<ClassicPullCore>> {
        self.core.as_ref().ok_or_else(|| {
            ClientError::not_initialized(
                "DefaultMQPullConsumerImpl is detached; create a consumer with DefaultMQPullConsumer::builder",
            )
        })
    }

    /// Starts the implementation.
    ///
    /// # Errors
    ///
    /// Returns an initialization, lifecycle, or underlying client startup error.
    pub async fn start(&self) -> ClientResult<()> {
        self.core()?.start().await
    }

    /// Shuts the implementation down and awaits its runtime-owned client tasks.
    ///
    /// # Errors
    ///
    /// Returns an initialization or underlying client shutdown error.
    pub async fn shutdown(&self) -> ClientResult<()> {
        self.core()?.shutdown().await
    }

    /// Returns whether the implementation is running.
    pub async fn is_running(&self) -> bool {
        let Ok(core) = self.core() else {
            return false;
        };
        *core.state.lock().await == ClassicPullServiceState::Running
            && core
                .lite_consumer
                .try_impl_()
                .is_ok_and(|implementation| implementation.service_state() == ServiceState::Running)
    }

    pub(crate) fn consumer_pull_timeout(&self) -> Duration {
        self.core.as_ref().map_or(Duration::from_secs(10), |core| {
            Duration::from_millis(core.lite_consumer.consumer_config().consumer_pull_timeout_millis)
        })
    }

    pub(crate) fn client_config(&self) -> ClientResult<Arc<ClientConfig>> {
        Ok(self.core()?.lite_consumer.client_config())
    }

    pub(crate) fn broker_suspend_timeout(&self) -> Duration {
        self.core.as_ref().map_or(Duration::from_secs(20), |core| {
            Duration::from_millis(core.lite_consumer.consumer_config().broker_suspend_max_time_millis)
        })
    }

    pub(crate) fn consumer_timeout_when_suspend(&self) -> Duration {
        self.core.as_ref().map_or(Duration::from_secs(30), |core| {
            Duration::from_millis(
                core.lite_consumer
                    .consumer_config()
                    .consumer_timeout_millis_when_suspend,
            )
        })
    }

    pub(crate) async fn pull_with_options(&self, options: PullOptions) -> ClientResult<PullResult> {
        self.core()?.pull(options, &CancellationToken::new()).await
    }

    pub async fn pull_with_cancellation(
        &self,
        options: PullOptions,
        cancel: &CancellationToken,
    ) -> ClientResult<PullResult> {
        self.core()?.pull(options, cancel).await
    }

    pub(crate) async fn pull_async_with_options<C>(&self, options: PullOptions, callback: C) -> ClientResult<()>
    where
        C: ClassicPullCallback,
    {
        self.start_pull_request(options, callback).await.map(|_| ())
    }

    pub async fn start_pull_request<C: ClassicPullCallback>(
        &self,
        options: PullOptions,
        callback: C,
    ) -> ClientResult<crate::consumer::default_mq_pull_consumer::ClassicPullRequest> {
        let core = self.core()?.clone();
        core.ensure_running().await?;
        options.validate()?;
        let permit = core
            .async_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| ClientError::invalid_state("available asynchronous pull capacity", "exhausted"))?;
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let task_core = core.clone();
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let completion = RequestCompletion(done.clone());
        let mut requests = core.requests.lock().unwrap_or_else(|p| p.into_inner());
        requests.retain(|request| !request.is_finished());
        if core.stop.is_cancelled() {
            return Err(ClientError::not_started());
        }
        let handle = spawn_client_tracked_task_with_context(
            &core.service_context,
            "classic-pull-callback",
            Box::pin(async move {
                let _completion = completion;
                let _permit = permit;
                let result = task_core.pull(options, &task_cancel).await;
                // A user panic must not poison the request registry or reveal its payload.
                let called = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match result {
                    Ok(result) => callback.on_success(result),
                    Err(error) => callback.on_exception(error),
                }));
                if called.is_err() {
                    tracing::warn!("Classic Pull callback panicked");
                }
            }),
        )
        .map_err(|source| ClientError::service_source("classic_pull_callback", source))?;
        requests.push(handle);
        Ok(crate::consumer::default_mq_pull_consumer::ClassicPullRequest::new(
            cancel, done,
        ))
    }

    pub(crate) fn lite_consumer(&self) -> ClientResult<&DefaultLitePullConsumer> {
        Ok(&self.core()?.lite_consumer)
    }

    pub fn max_reconsume_times(&self) -> ClientResult<i32> {
        Ok(self.core()?.max_reconsume_times.load(Ordering::Acquire))
    }

    pub fn message_queue_listener(&self) -> ClientResult<Option<ArcMessageQueueListener>> {
        Ok(self.core()?.listeners.read().unwrap_or_else(|p| p.into_inner()).clone())
    }
    pub fn set_message_queue_listener(&self, listener: Option<ArcMessageQueueListener>) -> ClientResult<()> {
        *self.core()?.listeners.write().unwrap_or_else(|p| p.into_inner()) = listener;
        Ok(())
    }
    pub fn set_max_reconsume_times(&self, value: i32) -> ClientResult<()> {
        self.core()?.max_reconsume_times.store(value, Ordering::Release);
        Ok(())
    }

    pub async fn persist_consumer_offset(&self) -> ClientResult<()> {
        let core = self.core()?;
        core.ensure_running().await?;
        core.persist_offsets().await
    }

    pub(crate) async fn fetch_subscribe_message_queues(&self, topic: &str) -> ClientResult<Vec<MessageQueue>> {
        let core = self.core()?;
        core.ensure_running().await?;
        // Custom fork: fixed Classic Pull reads its subscription cache first.
        // LitePull's explicitly fresh discovery must not make a warm Classic lookup
        // fail merely because NameServer is temporarily unavailable.
        let wire_topic = core.lite_consumer.classic_topic_with_namespace(topic);
        let rebalance = core.lite_consumer.try_impl_()?.classic_rebalance();
        let cached = rebalance
            .rebalance_impl_inner
            .topic_subscribe_info_table
            .read()
            .await
            .get(&wire_topic)
            .cloned();
        if let Some(queues) = cached {
            return Ok(self.parse_subscribe_message_queues(&queues)?.into_iter().collect());
        }
        core.lite_consumer.fetch_message_queues(topic).await
    }

    pub(crate) async fn register_message_queue_listener(
        &self,
        topic: &str,
        listener: ArcMessageQueueListener,
    ) -> ClientResult<()> {
        if topic.trim().is_empty() {
            return Err(crate::mq_client_err!("topic is blank"));
        }
        let core = self.core()?;
        // Fixed Java owns one global listener; the last non-null registration is
        // used for every registered topic, rather than retaining per-topic listeners.
        *core.listeners.write().unwrap_or_else(|p| p.into_inner()) = Some(listener);
        let wrapped_topic = core.lite_consumer.classic_topic_with_namespace(topic);
        core.lite_consumer
            .register_classic_pull_subscription(&wrapped_topic)
            .await
    }

    pub(crate) async fn update_consume_offset(&self, message_queue: &MessageQueue, offset: i64) -> ClientResult<()> {
        let core = self.core()?;
        core.ensure_running().await?;
        let queue = core.lite_consumer.queue_with_namespace(message_queue);
        core.lite_consumer
            .try_impl_()?
            .update_classic_pull_offset(&queue, offset)
            .await
    }

    pub(crate) async fn fetch_consume_offset(
        &self,
        message_queue: &MessageQueue,
        from_store: bool,
    ) -> ClientResult<i64> {
        let core = self.core()?;
        core.ensure_running().await?;
        let queue = core.lite_consumer.queue_with_namespace(message_queue);
        core.lite_consumer
            .try_impl_()?
            .fetch_classic_pull_offset(&queue, from_store)
            .await
    }

    pub(crate) async fn search_offset(&self, message_queue: &MessageQueue, timestamp: u64) -> ClientResult<i64> {
        let core = self.core()?;
        core.ensure_running().await?;
        core.lite_consumer.offset_for_timestamp(message_queue, timestamp).await
    }

    pub async fn search_offset_signed(&self, message_queue: &MessageQueue, timestamp: i64) -> ClientResult<i64> {
        let core = self.core()?;
        core.ensure_running().await?;
        let queue = core.lite_consumer.queue_with_namespace(message_queue);
        core.lite_consumer
            .try_impl_()?
            .classic_client_instance()?
            .mq_admin_impl
            .search_offset_signed(&queue, timestamp)
            .await
    }

    pub(crate) async fn max_offset(&self, message_queue: &MessageQueue) -> ClientResult<i64> {
        let core = self.core()?;
        core.ensure_running().await?;
        core.lite_consumer.max_offset(message_queue).await
    }

    pub(crate) async fn min_offset(&self, message_queue: &MessageQueue) -> ClientResult<i64> {
        let core = self.core()?;
        core.ensure_running().await?;
        core.lite_consumer.min_offset(message_queue).await
    }

    // Custom fork: these are the actual current assignments, not all route queues.
    pub async fn fetch_message_queues_in_balance(&self, topic: &str) -> ClientResult<HashSet<MessageQueue>> {
        let core = self.core()?;
        core.ensure_running().await?;
        let topic = core.lite_consumer.classic_topic_with_namespace(topic);
        let rebalance = core.lite_consumer.try_impl_()?.classic_rebalance();
        let queues = rebalance.rebalance_impl_inner.process_queue_table.read().await;
        let namespace = core
            .lite_consumer
            .client_config()
            .resolved_namespace()
            .unwrap_or_default();
        Ok(queues
            .keys()
            .filter(|queue| queue.topic() == &topic)
            .map(|queue| {
                MessageQueue::from_parts(
                    NamespaceUtil::without_namespace_with_namespace(queue.topic(), &namespace),
                    queue.broker_name().clone(),
                    queue.queue_id(),
                )
            })
            .collect())
    }

    pub async fn update_consume_offset_to_broker(
        &self,
        queue: &MessageQueue,
        offset: i64,
        oneway: bool,
    ) -> ClientResult<()> {
        let core = self.core()?;
        core.ensure_running().await?;
        let queue = core.lite_consumer.queue_with_namespace(queue);
        core.lite_consumer
            .try_impl_()?
            .offset_store()
            .ok_or_else(|| ClientError::not_initialized("OffsetStore"))?
            .update_consume_offset_to_broker(&queue, offset, oneway)
            .await
    }

    pub async fn send_message_back(
        &self,
        message: &mut rocketmq_model::common::message::message_ext::MessageExt,
        delay_level: i32,
        broker: Option<&str>,
        group: Option<&str>,
    ) -> ClientResult<crate::consumer::classic_pull_business::ClassicSendBackOutcome> {
        use crate::consumer::classic_pull_business::{ClassicSendBackFailure, ClassicSendBackOutcome};
        use rocketmq_model::common::message::{
            message_accessor::MessageAccessor, message_single::Message, MessageConst, MessageTrait,
        };
        let core = self.core()?;
        core.ensure_running().await?;
        let consumer = &core.lite_consumer;
        // Custom fork: an async future may be dropped before a Java-style finally
        // block runs. Put namespace on the owned wire copy, never on caller state.
        let namespace = consumer.client_config().resolved_namespace().unwrap_or_default();
        let mut wire_message = message.clone();
        wire_message.set_topic(consumer.classic_topic_with_namespace(message.topic()));
        let logical_topic = NamespaceUtil::without_namespace_with_namespace(message.topic(), &namespace);
        message.set_topic(CheetahString::from_string(logical_topic));
        let message = &mut wire_message;
        let result = async {
            let client = consumer.try_impl_()?.classic_client_instance()?;
            let configured_group = consumer.consumer_group();
            let requested_group = group
                .filter(|g| !g.trim().is_empty())
                .unwrap_or(configured_group.as_str());
            let max_reconsume = self.max_reconsume_times()?;
            let address = match broker {
                Some(name) => client
                    .find_broker_address_in_publish(&CheetahString::from_slice(name))
                    .ok_or_else(|| ClientError::invalid_state("Broker master address", "unavailable")),
                None => Ok(CheetahString::from_string(message.store_host.to_string())),
            };
            let primary = match address {
                Ok(address) => {
                    client
                        .consumer_send_message_back(
                            &address,
                            broker,
                            message,
                            requested_group,
                            delay_level,
                            3000,
                            max_reconsume,
                        )
                        .await
                }
                Err(error) => Err(error),
            };
            match primary {
                Ok(()) => Ok(ClassicSendBackOutcome::BrokerAcknowledged),
                Err(primary) => {
                    // Fixed Java retries by publishing to the configured group's retry topic,
                    // including when the direct request specified another consumer group.
                    let retry_topic = rocketmq_model::common::mix_all::get_retry_topic(&configured_group);
                    let mut retry = match message.get_body().cloned() {
                        Some(body) => Message::builder().topic(retry_topic.as_str()).body(body).build()?,
                        None => Message::builder().topic(retry_topic.as_str()).empty_body().build()?,
                    };
                    MessageAccessor::set_properties(&mut retry, message.get_properties().clone());
                    let origin = MessageAccessor::get_origin_message_id(message)
                        .filter(|id| !id.trim().is_empty())
                        .unwrap_or_else(|| message.msg_id.clone());
                    MessageAccessor::set_origin_message_id(&mut retry, origin);
                    retry.set_flag(message.get_flag());
                    MessageAccessor::put_property(
                        &mut retry,
                        CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC),
                        message.topic().clone(),
                    );
                    MessageAccessor::set_reconsume_time(
                        &mut retry,
                        CheetahString::from_string(message.reconsume_times.wrapping_add(1).to_string()),
                    );
                    MessageAccessor::set_max_reconsume_times(
                        &mut retry,
                        CheetahString::from_string(max_reconsume.to_string()),
                    );
                    retry.set_delay_time_level(3i32.wrapping_add(message.reconsume_times));
                    match client.send_with_default_producer(retry).await {
                        Ok(Some(result)) => Ok(ClassicSendBackOutcome::RetryPublished {
                            primary_error: primary,
                            result,
                        }),
                        Ok(None) => Err(ClientError::service_source(
                            "classic_send_back",
                            ClassicSendBackFailure {
                                primary,
                                fallback: ClientError::invalid_state("retry publish SendResult", "missing result"),
                            },
                        )),
                        Err(fallback) => Err(ClientError::service_source(
                            "classic_send_back",
                            ClassicSendBackFailure { primary, fallback },
                        )),
                    }
                }
            }
        }
        .await;
        result
    }

    /// Returns a live rebalance compatibility marker.
    ///
    /// # Errors
    ///
    /// Returns an initialization error for a detached implementation marker.
    pub fn rebalance_impl(&self) -> ClientResult<crate::legacy::RebalancePullImpl> {
        Ok(crate::legacy::RebalancePullImpl::from_inner(
            self.lite_consumer()?.try_impl_()?.classic_rebalance(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::consumer::message_queue_listener::MessageQueueListener;

    struct AssignmentListener {
        calls: Arc<AtomicUsize>,
        assigned: Arc<AtomicUsize>,
    }

    impl MessageQueueListener for AssignmentListener {
        fn message_queue_changed(
            &self,
            _topic: &str,
            _mq_all: &HashSet<MessageQueue>,
            mq_divided: &HashSet<MessageQueue>,
        ) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.assigned.store(mq_divided.len(), Ordering::SeqCst);
        }
    }

    #[test]
    fn classic_queue_listener_uses_java_global_listener_for_every_topic() {
        let listeners = Arc::new(StdRwLock::new(None));
        let calls = Arc::new(AtomicUsize::new(0));
        let assigned = Arc::new(AtomicUsize::new(0));
        *listeners.write().expect("listener registry should be writable") = Some(Arc::new(AssignmentListener {
            calls: calls.clone(),
            assigned: assigned.clone(),
        }) as ArcMessageQueueListener);
        let dispatcher = ClassicPullQueueListener::new(listeners);
        let all = HashSet::from([
            MessageQueue::from_parts("TopicA", "broker-a", 0),
            MessageQueue::from_parts("TopicA", "broker-a", 1),
        ]);
        let divided = HashSet::from([MessageQueue::from_parts("TopicA", "broker-a", 1)]);

        dispatcher.message_queue_changed("TopicA", &all, &divided);
        dispatcher.message_queue_changed("OtherTopic", &all, &divided);

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(assigned.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn classic_lifecycle_lock_wait_observes_consumer_and_root_cancellation() {
        for root_cancel in [false, true] {
            let runtime = crate::runtime::test_client_runtime(if root_cancel {
                "classic-root-lock-wait"
            } else {
                "classic-consumer-lock-wait"
            });
            let consumer = crate::DefaultMQPullConsumer::builder(runtime)
                .consumer_group("classic-lock-wait")
                .build()
                .unwrap();
            let implementation = consumer.default_mq_pull_consumer_impl().unwrap();
            let core = implementation.core().unwrap();
            let _lifecycle = core.state.lock().await;
            let waiting = core.ensure_running();
            tokio::pin!(waiting);
            tokio::select! {
                result = &mut waiting => panic!("lifecycle lock was held: {result:?}"),
                _ = tokio::task::yield_now() => {},
            }
            if root_cancel {
                core.service_context.task_group().cancellation_token().cancel();
            } else {
                core.stop.cancel();
            }
            let error = tokio::time::timeout(Duration::from_millis(100), waiting)
                .await
                .expect("cancellation must not wait for the lifecycle lock")
                .unwrap_err();
            assert!(error.is(&rocketmq_error::CLIENT_LIFECYCLE_INVALID_STATE));
        }
    }
}
