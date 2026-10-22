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

//! Java 4.6.1 Classic Pull scheduling on an explicitly owned native runtime.
#![allow(deprecated)]

use crate::consumer::default_mq_pull_consumer::DefaultMQPullConsumer;
use crate::consumer::default_mq_pull_consumer::MQPullConsumer;
use crate::consumer::message_queue_listener::MessageQueueListener;
use crate::runtime::{spawn_client_tracked_task_with_context, ClientRuntime, ClientTrackedTaskHandle};
use crate::{ClientError, ClientResult};
use cheetah_string::CheetahString;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_transport::api::RPCHook;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock, Weak};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const CREATED: u8 = 0;
const STARTING: u8 = 1;
const RUNNING: u8 = 2;
const FAILED: u8 = 3;
const CLOSED: u8 = 4;

pub type PullTaskCallbackTable = HashMap<CheetahString, Arc<dyn PullTaskCallback>>;

/// Native async callbacks keep long polling cancellable and avoid blocking a scheduler thread.
/// The default method is retained for existing Rust implementations; a real callback should
/// implement it and explicitly persist its business result before updating an offset.
pub trait PullTaskCallback: Send + Sync + 'static {
    fn do_pull_task<'a>(
        &'a self,
        queue: &'a MessageQueue,
        context: &'a mut PullTaskContext,
    ) -> Pin<Box<dyn Future<Output = ClientResult<()>> + Send + 'a>> {
        let _ = (queue, context);
        Box::pin(async {
            Err(ClientError::not_initialized(
                "PullTaskCallback.do_pull_task is not implemented",
            ))
        })
    }
}

/// Each invocation receives Java's default next delay of 200 ms.
#[derive(Clone)]
pub struct PullTaskContext {
    pull_next_delay_time_millis: i32,
    pull_consumer: Option<Arc<dyn Any + Send + Sync>>,
    cancellation: CancellationToken,
}
impl Default for PullTaskContext {
    fn default() -> Self {
        Self {
            pull_next_delay_time_millis: 200,
            pull_consumer: None,
            cancellation: CancellationToken::new(),
        }
    }
}
impl std::fmt::Debug for PullTaskContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PullTaskContext")
            .field("pull_next_delay_time_millis", &self.pull_next_delay_time_millis)
            .field("has_pull_consumer", &self.pull_consumer.is_some())
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish()
    }
}
impl PullTaskContext {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn pull_next_delay_time_millis(&self) -> i32 {
        self.pull_next_delay_time_millis
    }
    pub fn get_pull_next_delay_time_millis(&self) -> i32 {
        self.pull_next_delay_time_millis
    }
    /// Negative Java executor delays execute immediately; retain the value, clamp only when scheduling.
    pub fn set_pull_next_delay_time_millis(&mut self, delay: i32) {
        self.pull_next_delay_time_millis = delay;
    }
    pub fn get_pull_consumer(&self) -> ClientResult<DefaultMQPullConsumer> {
        self.pull_consumer_as::<DefaultMQPullConsumer>()
            .map(|consumer| (*consumer).clone())
    }
    /// Rust's typed equivalent of Java's arbitrary MQPullConsumer reference.
    pub fn pull_consumer_as<C: MQPullConsumer + 'static>(&self) -> ClientResult<Arc<C>> {
        self.pull_consumer
            .clone()
            .ok_or_else(|| ClientError::not_initialized("PullTaskContext has no pull consumer"))?
            .downcast::<C>()
            .map_err(|_| ClientError::illegal_argument("PullTaskContext consumer concrete type differs"))
    }
    pub fn set_pull_consumer<C: MQPullConsumer + 'static>(&mut self, consumer: C) {
        self.pull_consumer = Some(Arc::new(consumer));
    }
    pub fn set_shared_pull_consumer<C: MQPullConsumer + 'static>(&mut self, consumer: Arc<C>) {
        self.pull_consumer = Some(consumer);
    }
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PullScheduleSnapshot {
    pub queue_count: usize,
    pub callback_count: usize,
    pub configured_concurrency: usize,
    pub effective_concurrency: usize,
    pub inflight: usize,
    pub peak_inflight: usize,
    pub completed_calls: u64,
    pub failed_calls: u64,
    pub panicked_calls: u64,
    pub cancelled_calls: u64,
    pub missing_callbacks: u64,
    pub owned_tasks: usize,
    pub closed: bool,
}

struct TaskState {
    queue: MessageQueue,
    cancelled: AtomicBool,
    cancellation: StdMutex<CancellationToken>,
    due: StdMutex<Instant>,
    running: AtomicBool,
}
impl TaskState {
    fn new(queue: MessageQueue) -> Self {
        Self {
            queue,
            cancelled: AtomicBool::new(false),
            cancellation: StdMutex::new(CancellationToken::new()),
            due: StdMutex::new(Instant::now()),
            running: AtomicBool::new(false),
        }
    }
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.cancellation.lock().unwrap_or_else(|p| p.into_inner()).cancel();
    }
}

struct PullScheduleCore {
    consumer: StdRwLock<DefaultMQPullConsumer>,
    context: ChildServiceContext,
    callbacks: StdRwLock<PullTaskCallbackTable>,
    tasks: StdMutex<HashMap<MessageQueue, Arc<TaskState>>>,
    state: AtomicU8,
    transition: Mutex<()>,
    stop: CancellationToken,
    notify: Notify,
    worker: StdMutex<Option<ClientTrackedTaskHandle>>,
    thread_nums: AtomicUsize,
    effective_concurrency: AtomicUsize,
    refresh_interval: StdRwLock<Duration>,
    inflight: AtomicUsize,
    peak: AtomicUsize,
    completed: AtomicU64,
    failed: AtomicU64,
    panicked: AtomicU64,
    cancelled: AtomicU64,
    missing: AtomicU64,
}

struct InvocationGuard {
    core: Arc<PullScheduleCore>,
    task: Arc<TaskState>,
    finished: bool,
}
impl Drop for InvocationGuard {
    fn drop(&mut self) {
        self.task.running.store(false, Ordering::Release);
        self.core.inflight.fetch_sub(1, Ordering::AcqRel);
        if !self.finished {
            self.core.cancelled.fetch_add(1, Ordering::AcqRel);
        }
        self.core.notify.notify_one();
    }
}

impl PullScheduleCore {
    fn is_closed(&self) -> bool {
        self.stop.is_cancelled() || self.context.task_group().cancellation_token().is_cancelled()
    }
    fn consumer(&self) -> DefaultMQPullConsumer {
        self.consumer.read().unwrap_or_else(|p| p.into_inner()).clone()
    }
    fn put_task(&self, topic: &str, queues: &HashSet<MessageQueue>) -> ClientResult<()> {
        if self.is_closed() {
            return Err(ClientError::not_started());
        }
        if queues.iter().any(|queue| queue.topic_str() != topic) {
            return Err(ClientError::illegal_argument("putTask queues must belong to its topic"));
        }
        let mut tasks = self.tasks.lock().unwrap_or_else(|p| p.into_inner());
        tasks.retain(|queue, task| {
            let retain = queue.topic_str() != topic || queues.contains(queue);
            if !retain {
                task.cancel();
            }
            retain
        });
        for queue in queues {
            tasks
                .entry(queue.clone())
                .or_insert_with(|| Arc::new(TaskState::new(queue.clone())));
        }
        drop(tasks);
        self.notify.notify_one();
        Ok(())
    }

    async fn register_topics(self: &Arc<Self>, registered: &mut HashSet<CheetahString>) -> ClientResult<()> {
        let topics: Vec<_> = self
            .callbacks
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .cloned()
            .collect();
        for topic in topics {
            if registered.contains(&topic) {
                continue;
            }
            self.consumer()
                .register_message_queue_listener(
                    &topic,
                    AssignmentListener {
                        core: Arc::downgrade(self),
                    },
                )
                .await?;
            registered.insert(topic);
        }
        Ok(())
    }

    async fn start(self: &Arc<Self>) -> ClientResult<()> {
        let _transition = self.transition.lock().await;
        match self.state.load(Ordering::Acquire) {
            CREATED => {}
            RUNNING | STARTING => return Err(crate::mq_client_err!("MQPullConsumerScheduleService already started")),
            FAILED => {
                return Err(crate::mq_client_err!(
                    "MQPullConsumerScheduleService start previously failed; create a new service"
                ))
            }
            _ => {
                return Err(crate::mq_client_err!(
                    "MQPullConsumerScheduleService has been shut down"
                ))
            }
        }
        if self.context.task_group().cancellation_token().is_cancelled() {
            return Err(ClientError::not_started());
        }
        self.state.store(STARTING, Ordering::Release);
        self.effective_concurrency
            .store(self.thread_nums.load(Ordering::Acquire).max(1), Ordering::Release);
        let mut registered = HashSet::new();
        if let Err(error) = self.register_topics(&mut registered).await {
            self.state.store(FAILED, Ordering::Release);
            return Err(error);
        }
        if let Err(error) = self.consumer().start().await {
            self.state.store(FAILED, Ordering::Release);
            return Err(error);
        }
        let core = self.clone();
        let handle = spawn_client_tracked_task_with_context(
            &self.context,
            "classic-pull-schedule",
            Box::pin(async move {
                core.run(registered).await;
            }),
        );
        match handle {
            Ok(handle) => *self.worker.lock().unwrap_or_else(|p| p.into_inner()) = Some(handle),
            Err(source) => {
                self.state.store(FAILED, Ordering::Release);
                self.consumer().shutdown().await?;
                return Err(ClientError::service_source("classic_pull_schedule", source));
            }
        }
        self.state.store(RUNNING, Ordering::Release);
        self.notify.notify_one();
        Ok(())
    }

    async fn run(self: Arc<Self>, mut registered: HashSet<CheetahString>) {
        let root = self.context.task_group().cancellation_token();
        let mut jobs: FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send>>> = FuturesUnordered::new();
        let mut registration_due = Instant::now();
        loop {
            if self.is_closed() || root.is_cancelled() {
                break;
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Enable before observing state so no assignment update is lost.
            notified.as_mut().enable();
            if Instant::now() >= registration_due {
                tokio::select! {
                    biased;
                    _ = self.stop.cancelled() => break,
                    _ = root.cancelled() => break,
                    result = self.register_topics(&mut registered) => {
                        if result.is_err() { tracing::warn!("Classic Pull scheduled topic registration failed"); }
                    }
                }
                registration_due = Instant::now() + *self.refresh_interval.read().unwrap_or_else(|p| p.into_inner());
            }
            let mut tasks: Vec<_> = self
                .tasks
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .values()
                .cloned()
                .collect();
            // The fixed Java scheduled executor orders by due time; HashMap iteration
            // would starve an older ready queue when another callback requests zero delay.
            tasks.sort_by_key(|task| *task.due.lock().unwrap_or_else(|p| p.into_inner()));
            let concurrency = self.effective_concurrency.load(Ordering::Acquire);
            for task in tasks {
                if jobs.len() >= concurrency {
                    break;
                }
                if task.cancelled.load(Ordering::Acquire)
                    || task.running.load(Ordering::Acquire)
                    || *task.due.lock().unwrap_or_else(|p| p.into_inner()) > Instant::now()
                {
                    continue;
                }
                let callback = self
                    .callbacks
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(task.queue.topic())
                    .cloned();
                let Some(callback) = callback else {
                    task.cancel();
                    self.missing.fetch_add(1, Ordering::AcqRel);
                    continue;
                };
                task.running.store(true, Ordering::Release);
                let count = self.inflight.fetch_add(1, Ordering::AcqRel) + 1;
                self.peak.fetch_max(count, Ordering::AcqRel);
                let core = self.clone();
                let cancellation = task.cancellation.lock().unwrap_or_else(|p| p.into_inner()).clone();
                // Construct the guard before enqueueing: even an unpolled dropped future releases it.
                let guard = InvocationGuard {
                    core: core.clone(),
                    task: task.clone(),
                    finished: false,
                };
                jobs.push(Box::pin(async move {
                    // Capture the entire Drop guard, not just its bool via disjoint capture.
                    let mut guard = guard;
                    let mut context = PullTaskContext { pull_consumer: Some(Arc::new(core.consumer())), cancellation: cancellation.clone(), ..Default::default() };
                    let result = tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => return,
                        result = std::panic::AssertUnwindSafe(async { callback.do_pull_task(&task.queue, &mut context).await }).catch_unwind() => result,
                    };
                    let delay = match result {
                        Ok(Ok(())) => { core.completed.fetch_add(1, Ordering::AcqRel); context.pull_next_delay_time_millis.max(0) as u64 },
                        Ok(Err(_)) => { core.failed.fetch_add(1, Ordering::AcqRel); 1000 },
                        Err(_) => { core.panicked.fetch_add(1, Ordering::AcqRel); tracing::warn!("Classic Pull scheduled callback panicked"); 1000 },
                    };
                    *task.due.lock().unwrap_or_else(|p| p.into_inner()) = Instant::now() + Duration::from_millis(delay);
                    guard.finished = true;
                }));
            }
            let mut next_due = registration_due;
            if jobs.len() < concurrency {
                for task in self.tasks.lock().unwrap_or_else(|p| p.into_inner()).values() {
                    if !task.cancelled.load(Ordering::Acquire) && !task.running.load(Ordering::Acquire) {
                        next_due = next_due.min(*task.due.lock().unwrap_or_else(|p| p.into_inner()));
                    }
                }
            }
            tokio::select! {
                biased;
                _ = self.stop.cancelled() => break,
                _ = root.cancelled() => break,
                _ = jobs.next(), if !jobs.is_empty() => {},
                _ = &mut notified => {},
                _ = tokio::time::sleep_until(next_due) => {},
            }
        }
        for task in self.tasks.lock().unwrap_or_else(|p| p.into_inner()).values() {
            task.cancel();
        }
        drop(jobs);
        self.stop.cancel();
    }

    async fn shutdown(&self) -> ClientResult<()> {
        let _transition = self.transition.lock().await;
        if self.state.swap(CLOSED, Ordering::AcqRel) == CLOSED {
            return Ok(());
        }
        self.stop.cancel();
        self.notify.notify_waiters();
        let worker = self.worker.lock().unwrap_or_else(|p| p.into_inner()).take();
        let stopped = match worker {
            Some(worker) => worker.shutdown(Duration::from_secs(1)).await.is_healthy(),
            None => true,
        };
        self.tasks.lock().unwrap_or_else(|p| p.into_inner()).clear();
        let result = self.consumer().shutdown().await;
        if !stopped {
            return Err(ClientError::timeout("classic pull schedule shutdown", 1000));
        }
        result
    }
}

struct AssignmentListener {
    core: Weak<PullScheduleCore>,
}
impl MessageQueueListener for AssignmentListener {
    fn message_queue_changed(&self, topic: &str, all: &HashSet<MessageQueue>, divided: &HashSet<MessageQueue>) {
        if let Some(core) = self.core.upgrade() {
            let model = core
                .consumer()
                .shared_consumer()
                .map(|consumer| consumer.consumer_config().message_model);
            let queues = if matches!(model, Ok(MessageModel::Broadcasting)) {
                all
            } else {
                divided
            };
            if core.put_task(topic, queues).is_err() && !core.is_closed() {
                tracing::warn!("Classic Pull assignment update rejected");
            }
        }
    }
}

/// One coordinator owns at most `pull_thread_nums` callback futures. Queue membership
/// comes from rebalance (all queues in BROADCASTING; assigned queues in CLUSTERING).
#[derive(Clone)]
pub struct MQPullConsumerScheduleService {
    consumer_group: CheetahString,
    core: Option<Arc<PullScheduleCore>>,
}
impl MQPullConsumerScheduleService {
    pub fn new(group: impl Into<CheetahString>) -> Self {
        Self {
            consumer_group: group.into(),
            core: None,
        }
    }
    pub fn with_client_runtime(runtime: Arc<ClientRuntime>, group: impl Into<CheetahString>) -> ClientResult<Self> {
        Self::with_rpc_hook(runtime, group, None)
    }
    pub fn with_rpc_hook(
        runtime: Arc<ClientRuntime>,
        group: impl Into<CheetahString>,
        hook: Option<Arc<dyn RPCHook>>,
    ) -> ClientResult<Self> {
        let group = group.into();
        let mut builder = DefaultMQPullConsumer::builder(runtime.clone()).consumer_group(group.clone());
        if let Some(hook) = hook {
            builder = builder.rpc_hook(hook);
        }
        let consumer = builder.build()?;
        Ok(Self {
            consumer_group: group,
            core: Some(Arc::new(PullScheduleCore {
                consumer: StdRwLock::new(consumer),
                context: runtime.component("classic-pull-schedule"),
                callbacks: StdRwLock::new(HashMap::new()),
                tasks: StdMutex::new(HashMap::new()),
                state: AtomicU8::new(CREATED),
                transition: Mutex::new(()),
                stop: CancellationToken::new(),
                notify: Notify::new(),
                worker: StdMutex::new(None),
                thread_nums: AtomicUsize::new(20),
                effective_concurrency: AtomicUsize::new(20),
                refresh_interval: StdRwLock::new(Duration::from_secs(1)),
                inflight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                completed: AtomicU64::new(0),
                failed: AtomicU64::new(0),
                panicked: AtomicU64::new(0),
                cancelled: AtomicU64::new(0),
                missing: AtomicU64::new(0),
            })),
        })
    }
    fn core(&self) -> ClientResult<&Arc<PullScheduleCore>> {
        self.core.as_ref().ok_or_else(|| {
            ClientError::not_initialized("MQPullConsumerScheduleService has no ClientRuntime; use with_client_runtime")
        })
    }
    pub fn consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }
    pub async fn start(&self) -> ClientResult<()> {
        self.core()?.start().await
    }
    pub async fn shutdown(&self) -> ClientResult<()> {
        self.core()?.shutdown().await
    }
    pub fn set_refresh_interval(&self, interval: Duration) -> ClientResult<()> {
        if interval.is_zero() {
            return Err(ClientError::illegal_argument(
                "schedule refresh interval must be positive",
            ));
        }
        *self.core()?.refresh_interval.write().unwrap_or_else(|p| p.into_inner()) = interval;
        self.core()?.notify.notify_one();
        Ok(())
    }
    pub fn register_pull_task_callback<C: PullTaskCallback>(
        &self,
        topic: impl Into<CheetahString>,
        callback: C,
    ) -> ClientResult<()> {
        self.register_pull_task_callback_arc(topic, Arc::new(callback))
    }
    pub fn register_pull_task_callback_arc(
        &self,
        topic: impl Into<CheetahString>,
        callback: Arc<dyn PullTaskCallback>,
    ) -> ClientResult<()> {
        let topic = topic.into();
        if topic.is_empty() {
            return Err(ClientError::illegal_argument("topic is blank"));
        }
        let core = self.core()?;
        if core.is_closed() {
            return Err(ClientError::not_started());
        }
        core.callbacks
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(topic, callback);
        core.notify.notify_one();
        Ok(())
    }
    pub fn callback_table(&self) -> ClientResult<PullTaskCallbackTable> {
        Ok(self.core()?.callbacks.read().unwrap_or_else(|p| p.into_inner()).clone())
    }
    pub fn set_callback_table(&self, callbacks: PullTaskCallbackTable) -> ClientResult<()> {
        if callbacks.keys().any(|topic| topic.is_empty()) {
            return Err(ClientError::illegal_argument("topic is blank"));
        }
        let core = self.core()?;
        if core.is_closed() {
            return Err(ClientError::not_started());
        }
        *core.callbacks.write().unwrap_or_else(|p| p.into_inner()) = callbacks;
        core.notify.notify_one();
        Ok(())
    }
    pub fn pull_thread_nums(&self) -> ClientResult<usize> {
        Ok(self.core()?.thread_nums.load(Ordering::Acquire))
    }
    pub fn set_pull_thread_nums(&self, count: usize) -> ClientResult<()> {
        let core = self.core()?;
        if count > i32::MAX as usize {
            return Err(ClientError::illegal_argument("pullThreadNums exceeds Java int range"));
        }
        // Java's bean setter does not resize the already-created executor.
        core.thread_nums.store(count, Ordering::Release);
        Ok(())
    }
    pub fn default_mq_pull_consumer(&self) -> ClientResult<DefaultMQPullConsumer> {
        Ok(self.core()?.consumer())
    }
    pub async fn set_default_mq_pull_consumer(&self, consumer: DefaultMQPullConsumer) -> ClientResult<()> {
        let core = self.core()?;
        let _transition = core.transition.lock().await;
        if core.state.load(Ordering::Acquire) != CREATED {
            return Err(ClientError::invalid_state(
                "unstarted pull scheduler",
                "already started",
            ));
        }
        *core.consumer.write().unwrap_or_else(|p| p.into_inner()) = consumer;
        Ok(())
    }
    pub async fn message_model(&self) -> ClientResult<MessageModel> {
        self.core()?.consumer().message_model().await
    }
    pub async fn set_message_model(&self, model: MessageModel) -> ClientResult<()> {
        self.core()?.consumer().set_message_model(model).await
    }
    pub fn put_task(&self, topic: &str, queues: &HashSet<MessageQueue>) -> ClientResult<()> {
        self.core()?.put_task(topic, queues)
    }
    pub fn task(&self, queue: &MessageQueue) -> ClientResult<Option<PullTaskImpl>> {
        let core = self.core()?;
        Ok(core
            .tasks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(queue)
            .cloned()
            .map(|state| PullTaskImpl {
                state: Some(state),
                core: Arc::downgrade(core),
            }))
    }
    pub fn snapshot(&self) -> ClientResult<PullScheduleSnapshot> {
        let core = self.core()?;
        Ok(PullScheduleSnapshot {
            queue_count: core.tasks.lock().unwrap_or_else(|p| p.into_inner()).len(),
            callback_count: core.callbacks.read().unwrap_or_else(|p| p.into_inner()).len(),
            configured_concurrency: core.thread_nums.load(Ordering::Acquire),
            effective_concurrency: core.effective_concurrency.load(Ordering::Acquire),
            inflight: core.inflight.load(Ordering::Acquire),
            peak_inflight: core.peak.load(Ordering::Acquire),
            completed_calls: core.completed.load(Ordering::Acquire),
            failed_calls: core.failed.load(Ordering::Acquire),
            panicked_calls: core.panicked.load(Ordering::Acquire),
            cancelled_calls: core.cancelled.load(Ordering::Acquire),
            missing_callbacks: core.missing.load(Ordering::Acquire),
            owned_tasks: core
                .worker
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
                .map_or(0, ClientTrackedTaskHandle::task_count),
            closed: core.is_closed(),
        })
    }
}

/// A real schedule task, including the public Java nested task's cancellation surface.
/// A detached `new(queue)` retains metadata but cannot claim to run a missing callback.
#[derive(Clone, Default)]
pub struct PullTaskImpl {
    state: Option<Arc<TaskState>>,
    core: Weak<PullScheduleCore>,
}
impl std::fmt::Debug for PullTaskImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PullTaskImpl")
            .field("message_queue", &self.message_queue())
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}
impl PullTaskImpl {
    pub fn new(queue: MessageQueue) -> Self {
        Self {
            state: Some(Arc::new(TaskState::new(queue))),
            core: Weak::new(),
        }
    }
    pub fn with_service(service: &MQPullConsumerScheduleService, queue: MessageQueue) -> ClientResult<Self> {
        if let Some(task) = service.task(&queue)? {
            return Ok(task);
        }
        Ok(Self {
            state: Some(Arc::new(TaskState::new(queue))),
            core: Arc::downgrade(service.core()?),
        })
    }
    pub fn message_queue(&self) -> Option<&MessageQueue> {
        self.state.as_ref().map(|state| &state.queue)
    }
    pub fn is_cancelled(&self) -> bool {
        self.state
            .as_ref()
            .is_none_or(|state| state.cancelled.load(Ordering::Acquire))
    }
    pub fn set_cancelled(&self, cancelled: bool) {
        if let Some(state) = &self.state {
            if cancelled {
                state.cancel();
            } else {
                *state.cancellation.lock().unwrap_or_else(|p| p.into_inner()) = CancellationToken::new();
                state.cancelled.store(false, Ordering::Release);
            }
        }
        if let Some(core) = self.core.upgrade() {
            core.notify.notify_one();
        }
    }
    /// Enqueues the task on its owned service. It never reports a detached no-op as success.
    pub fn run(&self) -> ClientResult<()> {
        let core = self
            .core
            .upgrade()
            .ok_or_else(|| ClientError::not_initialized("PullTaskImpl has no schedule service"))?;
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| ClientError::not_initialized("PullTaskImpl has no message queue"))?;
        if core.is_closed() {
            return Err(ClientError::not_started());
        }
        if state.cancelled.load(Ordering::Acquire) {
            return Ok(());
        }
        *state.due.lock().unwrap_or_else(|p| p.into_inner()) = Instant::now();
        let mut tasks = core.tasks.lock().unwrap_or_else(|p| p.into_inner());
        match tasks.entry(state.queue.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(state.clone());
            }
            std::collections::hash_map::Entry::Occupied(entry) if Arc::ptr_eq(entry.get(), state) => {}
            _ => {
                return Err(ClientError::invalid_state(
                    "current PullTaskImpl generation",
                    "stale task",
                ))
            }
        }
        drop(tasks);
        core.notify.notify_one();
        Ok(())
    }
}
