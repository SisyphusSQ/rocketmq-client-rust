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

// Custom fork: fixed Java4.6.1 encoding, dedicated identity, bounded structured dispatch.
use crate::base::{access_channel::AccessChannel, client_config::ClientConfig};
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::producer::{
    default_mq_producer::DefaultMQProducer, producer_impl::default_mq_producer_impl::DefaultMQProducerImpl,
};
use crate::runtime::{spawn_client_tracked_task_with_context, ClientRuntime, ClientTrackedTaskHandle};
use crate::trace::{
    trace_constants::TraceConstants,
    trace_context::TraceContext,
    trace_data_encoder::TraceDataEncoder,
    trace_dispatcher::{TraceDispatcher, Type},
    trace_transfer_bean::TraceTransferBean,
};
use crate::{ClientError, ClientResult};
use bytes::Bytes;
use cheetah_string::CheetahString;
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use parking_lot::{Mutex, RwLock};
use rocketmq_model::common::{
    message::{message_queue::MessageQueue, message_single::Message},
    topic::TopicValidator,
};
use rocketmq_runtime::{ChildServiceContext, ShutdownReport};
use rocketmq_transport::api::{ConnectionAuthentication, RPCHook};
use serde::Serialize;
use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    mpsc as std_mpsc, Arc, Weak,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, OnceCell};

const TRACE_WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(500);
const TRACE_WORKER_FLUSH_TIMEOUT: Duration = Duration::from_millis(500);
#[derive(Clone)]
struct TraceDispatcherConfig {
    group: String,
    type_: Type,
    batch_num: usize,
    max_msg_size: usize,
    flush_interval: Duration,
    trace_topic_name: String,
    rpc_hook: Option<Arc<dyn RPCHook>>,
}
#[derive(Clone, Debug, Serialize)]
pub struct StandardTraceFailure {
    pub descriptor: String,
    pub failure_kind: String,
    pub broker_response_code: Option<i32>,
    pub request_stage: Option<String>,
}
#[derive(Clone, Debug, Serialize)]
pub struct StandardTraceSnapshot {
    pub started: bool,
    pub owned_tasks: usize,
    pub worker_stopped: bool,
    pub ready: bool,
    pub stopped: bool,
    pub accepted: u64,
    pub settled: u64,
    pub pending: u64,
    pub in_flight: usize,
    pub acknowledged_contexts: u64,
    pub acknowledged_messages: u64,
    pub failed_contexts: u64,
    pub invalid_contexts: u64,
    pub rejected_full: u64,
    pub rejected_closed: u64,
    pub rejected_invalid: u64,
    pub appender_full: u64,
    pub discarded_after_accept: u64,
    pub write_outcome_unknown: u64,
    pub storage_warnings: u64,
    pub server_records: Option<u64>,
    pub last_failure: Option<StandardTraceFailure>,
}
struct DispatcherState {
    is_started: AtomicBool,
    is_stopped: AtomicBool,
    queued_count: AtomicUsize,
    discard_count: AtomicU64,
    last_flush_time: AtomicU64,
    send_which_queue: AtomicUsize,
    access_channel: RwLock<AccessChannel>,
    client_host: RwLock<Option<CheetahString>>,
    namespace_v2: RwLock<Option<CheetahString>>,
    topic_override: RwLock<Option<String>>,
    host_group: RwLock<Option<String>>,
    rejected_invalid: AtomicU64,
    admission: Mutex<()>,
    accepted: AtomicU64,
    settled: AtomicU64,
    in_flight: AtomicUsize,
    acknowledged: AtomicU64,
    acknowledged_messages: AtomicU64,
    failed: AtomicU64,
    invalid: AtomicU64,
    closed: AtomicU64,
    discarded: AtomicU64,
    appender_full: AtomicU64,
    unknown: AtomicU64,
    storage_warnings: AtomicU64,
    ready: RwLock<Option<ClientResult<()>>>,
    ready_wake: tokio::sync::Notify,
    last_failure: RwLock<Option<StandardTraceFailure>>,
    shutdown_hook: AtomicBool,
    root_stop: tokio_util::sync::CancellationToken,
}
impl DispatcherState {
    fn new() -> Self {
        Self {
            is_started: AtomicBool::new(false),
            is_stopped: AtomicBool::new(false),
            queued_count: AtomicUsize::new(0),
            discard_count: AtomicU64::new(0),
            last_flush_time: AtomicU64::new(0),
            send_which_queue: AtomicUsize::new(0),
            access_channel: RwLock::new(AccessChannel::Local),
            client_host: RwLock::new(None),
            namespace_v2: RwLock::new(None),
            topic_override: RwLock::new(None),
            host_group: RwLock::new(None),
            rejected_invalid: AtomicU64::new(0),
            admission: Mutex::new(()),
            accepted: AtomicU64::new(0),
            settled: AtomicU64::new(0),
            in_flight: AtomicUsize::new(0),
            acknowledged: AtomicU64::new(0),
            acknowledged_messages: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            invalid: AtomicU64::new(0),
            closed: AtomicU64::new(0),
            discarded: AtomicU64::new(0),
            appender_full: AtomicU64::new(0),
            unknown: AtomicU64::new(0),
            storage_warnings: AtomicU64::new(0),
            ready: RwLock::new(None),
            ready_wake: tokio::sync::Notify::new(),
            last_failure: RwLock::new(None),
            shutdown_hook: AtomicBool::new(false),
            root_stop: tokio_util::sync::CancellationToken::new(),
        }
    }
    fn failure(&self, error: &ClientError) {
        *self.last_failure.write() = Some(StandardTraceFailure {
            descriptor: error.descriptor().code().to_string(),
            failure_kind: format!("{:?}", error.failure_kind()),
            broker_response_code: error.broker_response_code(),
            request_stage: error.request_stage().map(|s| format!("{s:?}")),
        });
    }
    fn set_ready(&self, result: ClientResult<()>) {
        if let Err(error) = &result {
            self.failure(error);
        }
        *self.ready.write() = Some(result);
        self.ready_wake.notify_waiters();
    }
    fn finish(&self, _ticket: u64) {
        self.settled.fetch_add(1, Ordering::AcqRel);
    }
}
#[derive(Clone, Copy)]
enum Phase {
    Waiting,
    Sending,
    Settled,
}
struct TraceWork {
    context: TraceContext,
    state: Arc<DispatcherState>,
    ticket: u64,
    queued: bool,
    phase: Phase,
}
impl TraceWork {
    fn dequeue(&mut self) {
        if self.queued {
            decrement_queued_trace_count(&self.state);
            self.queued = false;
        }
    }
    fn begin(&mut self) {
        self.phase = Phase::Sending;
        self.state.in_flight.fetch_add(1, Ordering::AcqRel);
    }
    fn complete(&mut self, success: bool) {
        self.state.in_flight.fetch_sub(1, Ordering::AcqRel);
        if success {
            self.state.acknowledged.fetch_add(1, Ordering::AcqRel);
        } else {
            self.state.failed.fetch_add(1, Ordering::AcqRel);
        }
        self.phase = Phase::Settled;
    }
    fn invalid(&mut self) {
        self.state.invalid.fetch_add(1, Ordering::AcqRel);
        self.phase = Phase::Settled;
    }
}
impl Drop for TraceWork {
    fn drop(&mut self) {
        self.dequeue();
        match self.phase {
            Phase::Waiting => {
                self.state.discarded.fetch_add(1, Ordering::AcqRel);
            }
            Phase::Sending => {
                self.state.unknown.fetch_add(1, Ordering::AcqRel);
                self.state.in_flight.fetch_sub(1, Ordering::AcqRel);
            }
            Phase::Settled => {}
        }
        self.state.finish(self.ticket);
    }
}
enum TraceFlushResponder {
    Blocking(std_mpsc::Sender<ClientResult<()>>),
    Async(oneshot::Sender<ClientResult<()>>),
}
impl TraceFlushResponder {
    fn send(self, result: ClientResult<()>) {
        match self {
            Self::Blocking(s) => {
                let _ = s.send(result);
            }
            Self::Async(s) => {
                let _ = s.send(result);
            }
        }
    }
}
enum TraceWorkerCommand {
    Trace(TraceWork),
    Flush(TraceFlushResponder),
    Shutdown,
}
enum TraceTaskHandle {
    Tracked(ClientTrackedTaskHandle),
}
impl TraceTaskHandle {
    fn abort(self) {
        match self {
            Self::Tracked(h) => {
                h.shutdown_now();
            }
        }
    }
    async fn shutdown_async(self, timeout: Duration) -> bool {
        self.shutdown_report_async(timeout).await.is_healthy()
    }
    fn shutdown(self, timeout: Duration) -> bool {
        let (r, c) = self.shutdown_report(timeout);
        c && r.is_healthy()
    }
    async fn shutdown_report_async(self, timeout: Duration) -> ShutdownReport {
        match self {
            Self::Tracked(h) => h.shutdown(timeout).await,
        }
    }
    fn shutdown_report(self, timeout: Duration) -> (ShutdownReport, bool) {
        match self {
            Self::Tracked(h) => h.shutdown_blocking(timeout),
        }
    }
    fn task_count(&self) -> usize {
        match self {
            Self::Tracked(h) => h.task_count(),
        }
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct TraceWorkerLifecycleProbe {
    pub task_count_before_shutdown: usize,
    pub remaining_tasks_after_shutdown: usize,
    pub completed: usize,
    pub cancelled: usize,
    pub aborted: usize,
    pub timed_out: usize,
    pub shutdown_elapsed_us: u128,
    pub healthy: bool,
}
/// Fixed Java4.6.1 standard Trace Topic dispatcher. All tasks belong to the injected runtime.
/// Input capacity1024; appender2048 batches; concurrency10 growing to20; native flush is bounded.
pub struct AsyncTraceDispatcher {
    client_runtime: Arc<ClientRuntime>,
    service_context: ChildServiceContext,
    config: TraceDispatcherConfig,
    state: Arc<DispatcherState>,
    use_tls: AtomicBool,
    tx: mpsc::Sender<TraceWorkerCommand>,
    rx: Mutex<Option<mpsc::Receiver<TraceWorkerCommand>>>,
    worker_handle: Mutex<Option<TraceTaskHandle>>,
    shutdown_completion: OnceCell<()>,
    trace_producer: RwLock<Option<Arc<tokio::sync::Mutex<DefaultMQProducer>>>>,
    producer_config: RwLock<ClientConfig>,
    host_producer: RwLock<Weak<DefaultMQProducerImpl>>,
    host_consumer: RwLock<Weak<DefaultMQPushConsumerImpl>>,
}
impl AsyncTraceDispatcher {
    pub fn new(
        client_runtime: Arc<ClientRuntime>,
        group: &str,
        type_: Type,
        batch_num: usize,
        trace_topic_name: &str,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1024);
        let service_context = client_runtime.component("trace-dispatcher");
        let state = Arc::new(DispatcherState {
            root_stop: service_context.task_group().cancellation_token(),
            ..DispatcherState::new()
        });
        let topic=if trace_topic_name.chars().all(|c|matches!(c,'\u{9}'..='\u{d}'|'\u{1c}'..='\u{20}'|'\u{1680}'|'\u{180e}'|'\u{2000}'..='\u{2006}'|'\u{2008}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{205f}'|'\u{3000}')) {TopicValidator::RMQ_SYS_TRACE_TOPIC.to_string()}else{trace_topic_name.to_string()};
        Self {
            service_context,
            client_runtime,
            config: TraceDispatcherConfig {
                group: group.into(),
                type_,
                batch_num: batch_num.max(1),
                max_msg_size: 128_000,
                flush_interval: Duration::from_millis(5),
                trace_topic_name: topic,
                rpc_hook,
            },
            state,
            use_tls: AtomicBool::new(false),
            tx,
            rx: Mutex::new(Some(rx)),
            worker_handle: Mutex::new(None),
            shutdown_completion: OnceCell::new(),
            trace_producer: RwLock::new(None),
            producer_config: RwLock::new(ClientConfig::default()),
            host_producer: RwLock::new(Weak::new()),
            host_consumer: RwLock::new(Weak::new()),
        }
    }
    fn gen_group_name(&self) -> String {
        format!(
            "{}-{}-{}",
            TraceConstants::GROUP_NAME_PREFIX,
            self.state.host_group.read().as_deref().unwrap_or(&self.config.group),
            match self.config.type_ {
                Type::Produce => "PRODUCE",
                Type::Consume => "CONSUME",
            }
        )
    }
    pub(crate) fn set_host_group(&self, group: &str) {
        *self.state.host_group.write() = Some(group.into());
    }
    /// Copy network/discovery settings and explicitly injected trace credentials; never reuse the business Token.
    pub fn set_client_config(&self, mut config: ClientConfig) {
        self.use_tls.store(config.use_tls, Ordering::Release);
        // Java's separate internal Producer does not inherit business resource namespaces/unit mode.
        config.namespace = None;
        config.namespace_v2 = None;
        config.namespace_initialized = Arc::new(AtomicBool::new(false));
        config.unit_mode = false;
        config.unit_name = None;
        let mut current = self.producer_config.write();
        config.authentication = config
            .trace_authentication
            .clone()
            .unwrap_or_else(|| current.authentication.clone());
        *current = config;
    }
    pub fn set_trace_authentication(&self, auth: ConnectionAuthentication) {
        self.producer_config.write().authentication = auth;
    }
    pub fn get_host_producer(&self) -> Option<Arc<DefaultMQProducerImpl>> {
        self.host_producer.read().upgrade()
    }
    pub fn set_host_producer(&self, producer: &Arc<DefaultMQProducerImpl>) {
        *self.host_producer.write() = Arc::downgrade(producer);
    }
    pub fn get_host_consumer(&self) -> Option<Arc<DefaultMQPushConsumerImpl>> {
        self.host_consumer.read().upgrade()
    }
    pub fn set_host_consumer(&self, consumer: &Arc<DefaultMQPushConsumerImpl>) {
        *self.host_consumer.write() = Arc::downgrade(consumer);
    }
    pub fn get_access_channel(&self) -> AccessChannel {
        *self.state.access_channel.read()
    }
    pub fn set_access_channel(&self, channel: AccessChannel) {
        *self.state.access_channel.write() = channel;
    }
    pub fn get_trace_topic_name(&self) -> String {
        self.state
            .topic_override
            .read()
            .clone()
            .unwrap_or_else(|| self.config.trace_topic_name.clone())
    }
    pub fn set_trace_topic_name(&self, topic: impl Into<String>) {
        *self.state.topic_override.write() = Some(topic.into());
    }
    pub fn get_discard_count(&self) -> u64 {
        self.state.discard_count.load(Ordering::Acquire)
    }
    pub fn queue_size(&self) -> usize {
        self.state.queued_count.load(Ordering::Acquire)
    }
    pub fn is_started(&self) -> bool {
        self.state.is_started.load(Ordering::Acquire)
    }
    pub fn is_stopped(&self) -> bool {
        self.state.is_stopped.load(Ordering::Acquire)
    }
    pub fn batch_num(&self) -> usize {
        self.config.batch_num
    }
    pub fn set_client_host(&self, host: Option<CheetahString>) {
        *self.state.client_host.write() = host;
    }
    pub fn set_namespace_v2(&self, namespace: Option<CheetahString>) {
        *self.state.namespace_v2.write() = namespace;
    }
    pub fn set_use_tls(&self, use_tls: bool) {
        self.use_tls.store(use_tls, Ordering::Release);
    }
    pub fn is_use_tls(&self) -> bool {
        self.use_tls.load(Ordering::Acquire)
    }
    /// Java getTraceProducer pre-start configuration, expressed as an owned Rust builder.
    pub fn trace_producer_builder(&self) -> crate::producer::default_mq_produce_builder::DefaultMQProducerBuilder {
        let mut config = self.producer_config.read().clone();
        config.set_enable_trace(false);
        config.set_vip_channel_enabled(false);
        config.set_use_tls(self.is_use_tls());
        if config.group_name().is_none() {
            config.set_group_name(Some(self.gen_group_name().into()));
        }
        let mut builder = DefaultMQProducer::builder(self.client_runtime.clone())
            .client_config(config)
            .producer_group(self.gen_group_name())
            .send_msg_timeout(5000)
            .max_message_size((self.config.max_msg_size - 10_000) as u32);
        if let Some(hook) = self.config.rpc_hook.clone() {
            builder = builder.rpc_hook(hook);
        }
        builder
    }
    pub fn set_trace_producer(&self, producer: DefaultMQProducer) -> ClientResult<()> {
        let _guard = self.state.admission.lock();
        if self.is_started() || self.is_stopped() {
            return Err(ClientError::invalid_state(
                "unstarted trace dispatcher",
                "started or closed",
            ));
        }
        if producer.trace_dispatcher().is_some() {
            return Err(ClientError::illegal_argument(
                "trace producer cannot recursively own a dispatcher",
            ));
        }
        *self.trace_producer.write() = Some(Arc::new(tokio::sync::Mutex::new(producer)));
        Ok(())
    }
    pub fn get_trace_producer(&self) -> Option<Arc<tokio::sync::Mutex<DefaultMQProducer>>> {
        self.trace_producer.read().clone()
    }
    pub fn trace_topic_name(&self) -> &str {
        &self.config.trace_topic_name
    }
    pub fn snapshot(&self) -> StandardTraceSnapshot {
        let _guard = self.state.admission.lock();
        let r = |a: &AtomicU64| a.load(Ordering::Acquire);
        let accepted = r(&self.state.accepted);
        let settled = r(&self.state.settled);
        StandardTraceSnapshot {
            started: self.is_started(),
            owned_tasks: self.service_context.task_group().task_count(),
            worker_stopped: self.service_context.task_group().task_count() == 0,
            ready: self.is_started() && !self.is_stopped() && matches!(*self.state.ready.read(), Some(Ok(()))),
            stopped: self.is_stopped(),
            accepted,
            settled,
            pending: accepted.saturating_sub(settled),
            in_flight: self.state.in_flight.load(Ordering::Acquire),
            acknowledged_contexts: r(&self.state.acknowledged),
            acknowledged_messages: r(&self.state.acknowledged_messages),
            failed_contexts: r(&self.state.failed),
            invalid_contexts: r(&self.state.invalid),
            rejected_full: r(&self.state.discard_count),
            rejected_closed: r(&self.state.closed),
            rejected_invalid: r(&self.state.rejected_invalid),
            appender_full: r(&self.state.appender_full),
            discarded_after_accept: r(&self.state.discarded),
            write_outcome_unknown: r(&self.state.unknown),
            storage_warnings: r(&self.state.storage_warnings),
            server_records: None,
            last_failure: self.state.last_failure.read().clone(),
        }
    }
    pub async fn wait_ready(&self, timeout: Duration) -> ClientResult<()> {
        tokio::time::timeout(timeout, async {
            loop {
                let wake = self.state.ready_wake.notified();
                tokio::pin!(wake);
                wake.as_mut().enable();
                if let Some(result) = self.state.ready.read().clone() {
                    return result;
                }
                wake.await;
            }
        })
        .await
        .map_err(|_| ClientError::timeout("trace_ready", timeout.as_millis() as u64))?
    }
    pub async fn flush_async(&self) -> ClientResult<()> {
        if !self.is_started() {
            return Err(ClientError::not_initialized("Dispatcher not started"));
        }
        // The deadline includes queue admission, not only the acknowledgement wait.
        tokio::time::timeout(TRACE_WORKER_FLUSH_TIMEOUT, async {
            let (tx, rx) = oneshot::channel();
            self.tx
                .send(TraceWorkerCommand::Flush(TraceFlushResponder::Async(tx)))
                .await
                .map_err(|_| trace_dispatcher_interrupted("worker stopped"))?;
            rx.await
                .map_err(|_| trace_dispatcher_interrupted("worker dropped flush"))?
        })
        .await
        .map_err(|_| trace_dispatcher_flush_timeout())?
    }
    pub async fn shutdown_async(&self) {
        self.shutdown_completion
            .get_or_init(|| self.shutdown_async_once())
            .await;
    }
    async fn shutdown_async_once(&self) {
        // Stop admission before flushing; all accepted work remains ahead of the barrier.
        {
            let _guard = self.state.admission.lock();
            self.state.is_stopped.store(true, Ordering::Release);
        }
        if self.is_started() {
            if let Err(e) = self.flush_async().await {
                self.state.failure(&e);
            }
        }
        self.request_worker_shutdown();
        let handle = self.worker_handle.lock().take();
        if let Some(h) = handle {
            h.shutdown_async(Duration::from_millis(100)).await;
        }
        if let Some(mut rx) = self.rx.lock().take() {
            rx.close();
            while rx.try_recv().is_ok() {}
        }
        let producer = self.trace_producer.write().take();
        if let Some(producer) = producer {
            Box::pin(producer.lock().await.shutdown()).await;
        }
        self.state.is_started.store(false, Ordering::Release);
        self.remove_shutdown_hook();
    }
    pub fn register_shutdown_hook(&self) {
        self.state.shutdown_hook.store(true, Ordering::Release);
    }
    pub fn register_shut_down_hook(&self) {
        self.register_shutdown_hook();
    }
    pub fn remove_shutdown_hook(&self) {
        self.state.shutdown_hook.store(false, Ordering::Release);
    }
    fn request_worker_shutdown(&self) {
        self.state.is_stopped.store(true, Ordering::Release);
        let _ = self.tx.try_send(TraceWorkerCommand::Shutdown);
    }
}
impl TraceDispatcher for AsyncTraceDispatcher {
    fn start(&self, name_srv_addr: &str, access_channel: AccessChannel) -> ClientResult<()> {
        let _guard = self.state.admission.lock();
        if self.is_stopped() || self.tx.is_closed() || self.state.root_stop.is_cancelled() {
            return Err(ClientError::invalid_state("open trace dispatcher", "closed"));
        }
        if self.is_started() {
            return Ok(());
        }
        let producer_group = self.gen_group_name();
        let injected = self.trace_producer.read().clone();
        let mut config = if let Some(producer) = &injected {
            producer
                .try_lock()
                .map_err(|_| ClientError::invalid_state("unlocked trace producer configuration", "locked"))?
                .client_config_snapshot()
                .as_ref()
                .clone()
        } else {
            self.producer_config.read().clone()
        };
        // An empty native argument keeps the configured discovery/HTTP profile.
        if !name_srv_addr.is_empty() {
            config.set_namesrv_addr(name_srv_addr.into());
        }
        config.set_enable_trace(false);
        if injected.is_none() {
            config.set_vip_channel_enabled(false);
        }
        if injected.is_none() {
            config.set_use_tls(self.is_use_tls());
        }
        config.instance_name = format!(
            "{}_{}",
            TraceConstants::TRACE_INSTANCE_NAME,
            config.namesrv_addr.as_deref().unwrap_or("null")
        )
        .into();
        if config.group_name().is_none() {
            config.set_group_name(Some(producer_group.clone().into()));
        }
        let validation = config
            .authentication
            .validate()
            .map_err(ClientError::from)
            .and_then(|_| {
                if config.token().is_none() {
                    Err(ClientError::illegal_argument(
                        "dedicated trace Token must be safely injected",
                    ))
                } else {
                    Ok(())
                }
            });
        if let Err(error) = validation {
            self.state.is_started.store(false, Ordering::Release);
            self.state.is_stopped.store(true, Ordering::Release);
            self.state.set_ready(Err(error.clone()));
            return Err(error);
        }
        let producer = if let Some(producer) = injected {
            producer
                .try_lock()
                .map_err(|_| ClientError::invalid_state("unlocked trace producer configuration", "locked"))?
                .set_client_config(config);
            producer
        } else {
            let mut builder = DefaultMQProducer::builder(self.client_runtime.clone())
                .producer_group(&producer_group)
                .client_config(config)
                .send_msg_timeout(5000)
                .max_message_size((self.config.max_msg_size - 10_000) as u32);
            if let Some(hook) = self.config.rpc_hook.clone() {
                builder = builder.rpc_hook(hook);
            }
            Arc::new(tokio::sync::Mutex::new(builder.build()))
        };
        *self.trace_producer.write() = Some(producer.clone());
        self.set_access_channel(access_channel);
        let rx = self
            .rx
            .lock()
            .take()
            .ok_or_else(|| ClientError::invalid_state("unused dispatcher receiver", "consumed"))?;
        self.state.is_started.store(true, Ordering::Release);
        let state = self.state.clone();
        let config = self.config.clone();
        let worker_state = state.clone();
        let handle = spawn_trace_task(&self.service_context, "rocketmq-client-trace-worker", async move {
            let result = worker_loop(rx, state, producer, config).await;
            if let Err(error) = result {
                worker_state.failure(&error);
            }
            worker_state.is_stopped.store(true, Ordering::Release);
            worker_state.is_started.store(false, Ordering::Release);
        });
        match handle {
            Ok(h) => {
                *self.worker_handle.lock() = Some(h);
                self.register_shutdown_hook();
                Ok(())
            }
            Err(e) => {
                self.state.set_ready(Err(e.clone()));
                self.state.is_stopped.store(true, Ordering::Release);
                self.state.is_started.store(false, Ordering::Release);
                Err(e)
            }
        }
    }
    fn append(&self, context: &dyn Any) -> bool {
        let Some(context) = context.downcast_ref::<TraceContext>() else {
            self.state.rejected_invalid.fetch_add(1, Ordering::AcqRel);
            return false;
        };
        let _guard = self.state.admission.lock();
        if self.is_stopped() || self.state.root_stop.is_cancelled() {
            self.state.closed.fetch_add(1, Ordering::AcqRel);
            return false;
        }
        let permit = match self.tx.try_reserve() {
            Ok(p) => p,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.state.discard_count.fetch_add(1, Ordering::AcqRel);
                return false;
            }
            Err(_) => {
                self.state.closed.fetch_add(1, Ordering::AcqRel);
                return false;
            }
        };
        let ticket = self.state.accepted.fetch_add(1, Ordering::AcqRel) + 1;
        self.state.queued_count.fetch_add(1, Ordering::AcqRel);
        permit.send(TraceWorkerCommand::Trace(TraceWork {
            context: context.clone(),
            state: self.state.clone(),
            ticket,
            queued: true,
            phase: Phase::Waiting,
        }));
        true
    }
    fn flush(&self) -> ClientResult<()> {
        if !self.is_started() {
            return Err(ClientError::not_initialized("Dispatcher not started"));
        }
        let (tx, rx) = std_mpsc::channel();
        self.tx
            .try_send(TraceWorkerCommand::Flush(TraceFlushResponder::Blocking(tx)))
            .map_err(trace_dispatcher_interrupted_source)?;
        rx.recv_timeout(TRACE_WORKER_FLUSH_TIMEOUT).map_err(|e| match e {
            std_mpsc::RecvTimeoutError::Timeout => trace_dispatcher_flush_timeout(),
            _ => trace_dispatcher_interrupted("flush closed"),
        })?
    }
    fn shutdown(&self) {
        // Synchronous callers use the bounded blocking API outside Tokio workers.
        {
            let _guard = self.state.admission.lock();
            self.state.is_stopped.store(true, Ordering::Release);
        }
        let _ = self.flush();
        self.request_worker_shutdown();
        if let Some(h) = self.worker_handle.lock().take() {
            h.shutdown(TRACE_WORKER_SHUTDOWN_TIMEOUT);
        }
        if let Some(mut rx) = self.rx.lock().take() {
            rx.close();
            while rx.try_recv().is_ok() {}
        }
        if let Some(producer) = self.trace_producer.write().take() {
            match spawn_trace_task(&self.service_context, "standard-trace-producer-shutdown", async move {
                Box::pin(producer.lock().await.shutdown()).await;
            }) {
                Ok(handle) => {
                    handle.shutdown(TRACE_WORKER_SHUTDOWN_TIMEOUT);
                }
                Err(error) => self.state.failure(&error),
            }
        }
        self.state.is_started.store(false, Ordering::Release);
        self.remove_shutdown_hook();
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
    fn trace_topic_name(&self) -> Option<&str> {
        Some(&self.config.trace_topic_name)
    }
    fn resolved_trace_topic_name(&self) -> Option<CheetahString> {
        Some(self.get_trace_topic_name().into())
    }
    fn trace_client_host(&self) -> Option<CheetahString> {
        self.state.client_host.read().clone()
    }
}
impl Drop for AsyncTraceDispatcher {
    fn drop(&mut self) {
        self.request_worker_shutdown();
        if let Some(h) = self.worker_handle.lock().take() {
            h.abort();
        }
    }
}
fn spawn_trace_task<F>(context: &ChildServiceContext, name: &'static str, task: F) -> ClientResult<TraceTaskHandle>
where
    F: Future<Output = ()> + Send + 'static,
{
    spawn_client_tracked_task_with_context(context, name, Box::pin(task))
        .map(TraceTaskHandle::Tracked)
        .map_err(|e| trace_dispatcher_startup_failed(name, e))
}
fn trace_dispatcher_interrupted(_reason: impl Into<String>) -> ClientError {
    ClientError::service_failed("trace_dispatcher_interrupted")
}
fn trace_dispatcher_interrupted_source(source: impl std::error::Error + Send + Sync + 'static) -> ClientError {
    ClientError::service_source("trace_dispatcher_interrupted", source)
}
fn trace_dispatcher_flush_timeout() -> ClientError {
    ClientError::timeout("trace_dispatcher_flush", TRACE_WORKER_FLUSH_TIMEOUT.as_millis() as u64)
}
fn trace_dispatcher_startup_failed(
    name: &'static str,
    e: impl std::error::Error + Send + Sync + 'static,
) -> ClientError {
    ClientError::service_source(name, e)
}
fn decrement_queued_trace_count(s: &DispatcherState) {
    let _ = s
        .queued_count
        .try_update(Ordering::AcqRel, Ordering::Acquire, |c| c.checked_sub(1));
}
async fn worker_loop(
    mut rx: mpsc::Receiver<TraceWorkerCommand>,
    state: Arc<DispatcherState>,
    producer: Arc<tokio::sync::Mutex<DefaultMQProducer>>,
    config: TraceDispatcherConfig,
) -> ClientResult<()> {
    let result = tokio::select! {r=async{producer.lock().await.start().await}=>r,_=state.root_stop.cancelled()=>Err(ClientError::service_failed("trace startup cancelled"))};
    state.set_ready(result.clone());
    result?;
    let sender = Arc::new(producer.lock().await.clone());
    let mut jobs: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();
    let mut pending = VecDeque::new();
    let mut limit = 10;
    let mut buffer = Vec::new();
    let mut barrier: Option<TraceFlushResponder> = None;
    let mut closed = false;
    let mut stop = false;
    let root = state.root_stop.clone();
    let mut root_deadline = None;
    loop {
        while jobs.len() < limit {
            if let Some(batch) = pending.pop_front() {
                jobs.push(send_work(batch, state.clone(), sender.clone(), config.clone()));
            } else {
                break;
            }
        }
        if buffer.len() >= config.batch_num || (closed && !buffer.is_empty()) || barrier.is_some() && !buffer.is_empty()
        {
            let batch = std::mem::take(&mut buffer);
            if jobs.len() < limit {
                jobs.push(send_work(batch, state.clone(), sender.clone(), config.clone()));
            } else if pending.len() < 2048 {
                pending.push_back(batch);
            } else if limit < 20 {
                limit += 1;
                jobs.push(send_work(batch, state.clone(), sender.clone(), config.clone()));
            } else {
                state.appender_full.fetch_add(batch.len() as u64, Ordering::AcqRel);
                drop(batch);
            }
        }
        if buffer.is_empty() && jobs.is_empty() && pending.is_empty() {
            if let Some(b) = barrier.take() {
                record_flush_completion(&state);
                let failed = state.failed.load(Ordering::Acquire)
                    + state.unknown.load(Ordering::Acquire)
                    + state.invalid.load(Ordering::Acquire)
                    + state.discarded.load(Ordering::Acquire)
                    > 0;
                b.send(if failed {
                    Err(ClientError::service_failed("standard_trace_incomplete"))
                } else {
                    Ok(())
                });
            }
            if closed || stop {
                break;
            }
            limit = 10;
        }
        tokio::select! {
            _=root.cancelled(),if root_deadline.is_none()=>{state.is_stopped.store(true,Ordering::Release);rx.close();if !state.shutdown_hook.load(Ordering::Acquire){break;}root_deadline=Some(tokio::time::Instant::now()+TRACE_WORKER_SHUTDOWN_TIMEOUT);},
            _=async{if let Some(deadline)=root_deadline{tokio::time::sleep_until(deadline).await;}else{std::future::pending::<()>().await;}}=>break,
            _=jobs.next(),if !jobs.is_empty()=>{},
            _=tokio::time::sleep(config.flush_interval),if !buffer.is_empty()=>{let batch=std::mem::take(&mut buffer);if jobs.len()<limit{jobs.push(send_work(batch,state.clone(),sender.clone(),config.clone()));}else if pending.len()<2048{pending.push_back(batch);}else if limit<20{limit+=1;jobs.push(send_work(batch,state.clone(),sender.clone(),config.clone()));}else{state.appender_full.fetch_add(batch.len() as u64,Ordering::AcqRel);}},
            item=rx.recv(),if barrier.is_none()&&!closed&&!stop=>match item{
                Some(TraceWorkerCommand::Trace(mut work))=>{work.dequeue();buffer.push(work);},
                Some(TraceWorkerCommand::Flush(b))=>barrier=Some(b),
                Some(TraceWorkerCommand::Shutdown)=>{rx.close();closed=true;stop=true;},None=>closed=true,
            },
        }
    }
    drop(jobs);
    drop(pending);
    drop(buffer);
    drop(rx);
    Box::pin(producer.lock().await.shutdown()).await;
    Ok(())
}
fn uncertain(error: &ClientError) -> bool {
    match error.request_stage() {
        Some(
            rocketmq_transport::api::OutboundRequestStage::Writing
            | rocketmq_transport::api::OutboundRequestStage::AwaitingResponse,
        ) => true,
        Some(_) => false,
        None => error.is(&rocketmq_error::CORE_OPERATION_TIMED_OUT),
    }
}
fn send_work(
    mut works: Vec<TraceWork>,
    state: Arc<DispatcherState>,
    producer: Arc<DefaultMQProducer>,
    config: TraceDispatcherConfig,
) -> BoxFuture<'static, ()> {
    Box::pin(async move {
        let mut groups: HashMap<(String, String), Vec<(TraceTransferBean, usize)>> = HashMap::new();
        for (index, work) in works.iter_mut().enumerate() {
            let ctx = &work.context;
            let channel = ctx.access_channel.unwrap_or(*state.access_channel.read());
            let topic = if channel == AccessChannel::Cloud {
                format!("{}{}", TraceConstants::TRACE_TOPIC_PREFIX, ctx.region_id.as_str())
            } else {
                state
                    .topic_override
                    .read()
                    .clone()
                    .unwrap_or_else(|| config.trace_topic_name.clone())
            };
            if let Some(beans) = ctx.trace_beans.as_ref().filter(|beans| !beans.is_empty()) {
                if let Some(encoded) = TraceDataEncoder::encoder_from_context_bean(ctx) {
                    groups
                        .entry((beans[0].topic.to_string(), topic))
                        .or_default()
                        .push((encoded, index));
                    continue;
                }
            }
            work.invalid();
        }
        for ((_data_topic, trace_topic), beans) in groups {
            let mut payload = String::new();
            let mut keys = HashSet::new();
            let mut indices = Vec::new();
            for (bean, index) in beans {
                // Count UTF-8 bytes and split before overflowing the Producer's118000-byte body limit.
                if !payload.is_empty() && payload.len() + bean.trans_data.len() > producer.max_message_size() as usize {
                    send_payload(&mut works, &indices, &keys, &payload, &trace_topic, &producer, &state).await;
                    payload.clear();
                    keys.clear();
                    indices.clear();
                }
                if bean.trans_data.len() > producer.max_message_size() as usize {
                    works[index].invalid();
                    continue;
                }
                payload.push_str(&bean.trans_data);
                keys.extend(bean.trans_key);
                indices.push(index);
            }
            if !payload.is_empty() {
                send_payload(&mut works, &indices, &keys, &payload, &trace_topic, &producer, &state).await;
            }
        }
    })
}
async fn send_payload(
    works: &mut [TraceWork],
    indices: &[usize],
    keys: &HashSet<CheetahString>,
    data: &str,
    topic: &str,
    producer: &DefaultMQProducer,
    state: &Arc<DispatcherState>,
) {
    for &i in indices {
        works[i].begin();
    }
    let message = Message::builder()
        .topic(topic)
        .body(Bytes::copy_from_slice(data.as_bytes()))
        .keys(keys.iter().map(ToString::to_string).collect::<Vec<_>>())
        .build();
    let queue_state = state.clone();
    let timeout_millis = producer.send_msg_timeout() as u64;
    let result = match message {
        Ok(message) => tokio::time::timeout(
            Duration::from_millis(timeout_millis),
            producer.send_with_selector_timeout(
                message,
                move |queues: &[MessageQueue], _: &Message, _: &()| {
                    let i = queue_state.send_which_queue.fetch_add(1, Ordering::AcqRel);
                    queues
                        .get(if queues.is_empty() { 0 } else { i % queues.len() })
                        .cloned()
                },
                (),
                timeout_millis,
            ),
        )
        .await
        .map_err(|_| ClientError::timeout("standard_trace_send", timeout_millis))
        .and_then(|r| r),
        Err(e) => Err(e.into()),
    };
    match result {
        Ok(Some(result)) => {
            state.acknowledged_messages.fetch_add(1, Ordering::AcqRel);
            let ok = result.send_status == crate::SendStatus::SendOk;
            if !ok {
                state.storage_warnings.fetch_add(1, Ordering::AcqRel);
            }
            for &i in indices {
                works[i].complete(ok);
            }
        }
        Ok(None) => {
            let e = ClientError::service_failed("missing trace send acknowledgement");
            state.failure(&e);
            for &i in indices {
                works[i].complete(false);
            }
        }
        Err(e) => {
            state.failure(&e);
            if !uncertain(&e) {
                for &i in indices {
                    works[i].complete(false);
                }
            }
        }
    }
}
fn record_flush_completion(s: &DispatcherState) {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or_default();
    s.last_flush_time.store(ms, Ordering::Release);
}
// Existing diagnostic/unit helper for an empty buffer; production work uses RAII tickets above.
#[cfg(test)]
async fn flush_buffer(
    buffer: &mut Vec<TraceContext>,
    state: &Arc<DispatcherState>,
    _producer: &Arc<tokio::sync::Mutex<DefaultMQProducer>>,
    _config: &TraceDispatcherConfig,
) -> ClientResult<()> {
    if !buffer.is_empty() {
        return Err(ClientError::illegal_argument("helper accepts an empty buffer"));
    }
    record_flush_completion(state);
    Ok(())
}

#[doc(hidden)]
pub fn run_trace_queue_depth_accounting_probe(client_runtime: Arc<ClientRuntime>, queued_count: usize) -> usize {
    let dispatcher = AsyncTraceDispatcher::new(
        client_runtime,
        "TraceProbeGroup",
        Type::Produce,
        20,
        "TRACE_TOPIC",
        None,
    );
    let context = TraceContext::default();

    for _ in 0..queued_count {
        assert!(dispatcher.append(&context));
    }

    dispatcher.queue_size()
}

#[doc(hidden)]
pub async fn run_trace_worker_lifecycle_probe(service_context: ChildServiceContext) -> TraceWorkerLifecycleProbe {
    let started = Arc::new(AtomicBool::new(false));
    let stopped = Arc::new(AtomicBool::new(false));
    let started_in_task = started.clone();
    let stopped_in_task = stopped.clone();

    let handle = spawn_trace_task(&service_context, "rocketmq-client-trace-worker-probe", async move {
        started_in_task.store(true, Ordering::Release);
        while !stopped_in_task.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .expect("trace worker lifecycle probe should spawn");

    for _ in 0..100 {
        if started.load(Ordering::Acquire) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let task_count_before_shutdown = handle.task_count();
    stopped.store(true, Ordering::Release);
    let shutdown_started_at = Instant::now();
    let report = handle.shutdown_report_async(Duration::from_secs(1)).await;
    let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
    let remaining_tasks_after_shutdown = report.remaining_tasks.len();
    let healthy = started.load(Ordering::Acquire)
        && task_count_before_shutdown > 0
        && remaining_tasks_after_shutdown == 0
        && report.is_healthy();

    TraceWorkerLifecycleProbe {
        task_count_before_shutdown,
        remaining_tasks_after_shutdown,
        completed: report.completed,
        cancelled: report.cancelled,
        aborted: report.aborted,
        timed_out: report.timed_out,
        shutdown_elapsed_us,
        healthy,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::error_assertions::assert_context_field;
    use crate::test_support::error_assertions::assert_error;

    fn test_runtime() -> Arc<ClientRuntime> {
        crate::runtime::test_client_runtime("async-trace-dispatcher-test")
    }

    fn test_context() -> ChildServiceContext {
        crate::runtime::test_service_context("async-trace-dispatcher-task-test")
    }

    #[test]
    fn trace_dispatcher_interrupted_uses_service_descriptor() {
        let error = trace_dispatcher_interrupted("worker is stopped");

        assert_eq!(error.descriptor().code(), rocketmq_error::CORE_SERVICE_FAILED.code());
    }

    #[test]
    fn trace_dispatcher_interrupted_source_preserves_cause() {
        let error = trace_dispatcher_interrupted_source(std::io::Error::other("flush queue closed"));

        assert_eq!(error.descriptor().code(), rocketmq_error::CORE_SERVICE_FAILED.code());
        assert!(error.source_ref::<std::io::Error>().is_some());
    }

    #[test]
    fn trace_dispatcher_flush_timeout_uses_timeout_descriptor() {
        let error = trace_dispatcher_flush_timeout();

        assert_error(&error, &rocketmq_error::CORE_OPERATION_TIMED_OUT);
        assert_context_field(
            &error,
            "operation",
            rocketmq_error::ViewValueRef::Text("trace_dispatcher_flush"),
        );
        assert_context_field(
            &error,
            "timeout_ms",
            rocketmq_error::ViewValueRef::U64(TRACE_WORKER_FLUSH_TIMEOUT.as_millis() as u64),
        );
    }

    #[test]
    fn trace_dispatcher_startup_failed_uses_service_descriptor() {
        let error =
            trace_dispatcher_startup_failed("rocketmq-client-trace-test", std::io::Error::other("task group closed"));

        assert_eq!(error.descriptor().code(), rocketmq_error::CORE_SERVICE_FAILED.code());
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn spawn_trace_task_uses_injected_runtime_context() {
        let (tx, rx) = std::sync::mpsc::channel();

        let handle = spawn_trace_task(&test_context(), "rocketmq-client-trace-test", async move {
            tx.send(std::thread::current().name().unwrap_or_default().to_string())
                .expect("test receiver should be alive");
        })
        .expect("trace task should start on the injected runtime");

        let thread_name = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("trace task should run on the injected runtime");
        assert_eq!(thread_name, "rocketmq-client-unit-test");
        assert!(handle.shutdown(Duration::from_secs(1)));
    }

    #[test]
    fn trace_task_shutdown_waits_for_completed_worker() {
        let handle = spawn_trace_task(&test_context(), "rocketmq-client-trace-complete-test", async move {})
            .expect("trace task should start on the injected runtime");

        assert!(handle.shutdown(Duration::from_secs(1)));
    }

    #[test]
    fn trace_task_shutdown_aborts_worker_after_timeout() {
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = spawn_trace_task(&test_context(), "rocketmq-client-trace-timeout-test", async move {
            tx.send(()).expect("test receiver should be alive");
            std::future::pending::<()>().await;
        })
        .expect("trace task should start on the injected runtime");

        rx.recv_timeout(Duration::from_secs(2))
            .expect("trace task should start");

        assert!(!handle.shutdown(Duration::from_millis(20)));
    }

    #[tokio::test]
    async fn trace_task_shutdown_async_aborts_worker_after_timeout() {
        struct DropFlag(Arc<AtomicBool>);

        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let started = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let started_in_task = started.clone();
        let dropped_in_task = dropped.clone();
        let handle = spawn_trace_task(
            &test_context(),
            "rocketmq-client-trace-timeout-async-test",
            async move {
                let _drop_flag = DropFlag(dropped_in_task);
                started_in_task.store(true, Ordering::Release);
                std::future::pending::<()>().await;
            },
        )
        .expect("trace task should spawn");

        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("trace task should start before shutdown");

        assert!(!handle.shutdown_async(Duration::from_millis(20)).await);
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn concurrent_shutdown_async_flushes_and_joins_the_worker_once() {
        let dispatcher = Arc::new(AsyncTraceDispatcher::new(
            test_runtime(),
            "TestGroup",
            Type::Produce,
            20,
            "TRACE_TOPIC",
            None,
        ));
        let mut rx = dispatcher
            .rx
            .lock()
            .take()
            .expect("test worker receiver should be available");
        let flush_count = Arc::new(AtomicUsize::new(0));
        let worker_flush_count = Arc::clone(&flush_count);
        let release_first_flush = Arc::new(tokio::sync::Notify::new());
        let worker_release_first_flush = Arc::clone(&release_first_flush);
        let (first_flush_started_tx, first_flush_started_rx) = oneshot::channel();

        let handle = spawn_trace_task(
            &dispatcher.service_context,
            "rocketmq-client-trace-idempotent-shutdown-test",
            async move {
                let mut first_flush_started_tx = Some(first_flush_started_tx);
                while let Some(command) = rx.recv().await {
                    match command {
                        TraceWorkerCommand::Trace(_) => {}
                        TraceWorkerCommand::Flush(responder) => {
                            let flush_index = worker_flush_count.fetch_add(1, Ordering::AcqRel);
                            if flush_index == 0 {
                                let sender = first_flush_started_tx
                                    .take()
                                    .expect("the first flush should signal exactly once");
                                let _ = sender.send(());
                                worker_release_first_flush.notified().await;
                            }
                            responder.send(Ok(()));
                        }
                        TraceWorkerCommand::Shutdown => break,
                    }
                }
            },
        )
        .expect("test trace worker should spawn");
        *dispatcher.worker_handle.lock() = Some(handle);
        dispatcher.state.is_started.store(true, Ordering::Release);

        let first_dispatcher = Arc::clone(&dispatcher);
        let first_shutdown = tokio::spawn(async move {
            first_dispatcher.shutdown_async().await;
        });
        first_flush_started_rx
            .await
            .expect("the first shutdown should reach the worker flush");

        let second_shutdown = dispatcher.shutdown_async();
        tokio::pin!(second_shutdown);
        assert!(
            futures::poll!(second_shutdown.as_mut()).is_pending(),
            "the concurrent shutdown should wait for the in-flight shutdown"
        );

        release_first_flush.notify_one();
        let (first_result, ()) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(first_shutdown, second_shutdown)
        })
        .await
        .expect("both shutdown callers should complete");
        first_result.expect("the first shutdown task should not panic");

        dispatcher.shutdown_async().await;
        assert_eq!(flush_count.load(Ordering::Acquire), 1);
        assert!(!dispatcher.is_started());
        assert!(dispatcher.is_stopped());
        assert_eq!(dispatcher.service_context.task_group().task_count(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drop_requests_nonblocking_worker_cancellation() {
        let dispatcher = AsyncTraceDispatcher::new(test_runtime(), "TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);
        let (worker_started_tx, worker_started_rx) = oneshot::channel();
        let handle = spawn_trace_task(
            &dispatcher.service_context,
            "rocketmq-client-trace-drop-test",
            async move {
                let _ = worker_started_tx.send(());
                std::future::pending::<()>().await;
            },
        )
        .expect("test trace worker should spawn");
        *dispatcher.worker_handle.lock() = Some(handle);
        dispatcher.state.is_started.store(true, Ordering::Release);
        let worker_group = dispatcher.service_context.task_group().clone();
        worker_started_rx.await.expect("test trace worker should start");

        let (ticker_tx, ticker_rx) = oneshot::channel();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            let _ = ticker_tx.send(());
        });

        let drop_started = Instant::now();
        drop(dispatcher);
        assert!(
            drop_started.elapsed() < Duration::from_millis(500),
            "dropping a trace dispatcher must not synchronously flush or await its worker"
        );
        tokio::time::timeout(Duration::from_secs(1), ticker_rx)
            .await
            .expect("the current-thread runtime should keep advancing after drop")
            .expect("the ticker task should complete");
        // An abort is confirmed only when this current-thread runtime next polls the worker, and
        // the ticker completing does not order that poll before this assertion. Drain the group by
        // waiting for the task to leave its registry instead of assuming one scheduler turn.
        let drain_deadline = Instant::now() + Duration::from_secs(5);
        while worker_group.task_count() != 0 && Instant::now() < drain_deadline {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            worker_group.task_count(),
            0,
            "the dropped trace worker must leave its task group"
        );
    }

    #[tokio::test]
    async fn trace_worker_lifecycle_probe_reports_clean_shutdown() {
        let probe = run_trace_worker_lifecycle_probe(test_context()).await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.remaining_tasks_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.aborted, 0, "{probe:?}");
        assert_eq!(probe.timed_out, 0, "{probe:?}");
    }

    #[test]
    fn test_new_dispatcher() {
        let dispatcher = AsyncTraceDispatcher::new(test_runtime(), "TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        assert!(!dispatcher.is_started());
        assert!(!dispatcher.is_stopped());
        assert_eq!(dispatcher.get_discard_count(), 0);
        assert_eq!(dispatcher.queue_size(), 0);
    }

    #[test]
    fn queue_size_tracks_accepted_trace_contexts() {
        let dispatcher = AsyncTraceDispatcher::new(test_runtime(), "TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        let ctx = TraceContext::default();

        assert!(dispatcher.append(&ctx));
        assert_eq!(dispatcher.queue_size(), 1);
    }

    #[test]
    fn queue_size_ignores_rejected_context_types() {
        let dispatcher = AsyncTraceDispatcher::new(test_runtime(), "TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        assert!(!dispatcher.append(&"not a trace context"));
        assert_eq!(dispatcher.queue_size(), 0);
    }

    #[test]
    fn queued_trace_count_decrement_saturates_at_zero() {
        let state = DispatcherState::new();
        state.queued_count.store(1, Ordering::Release);

        decrement_queued_trace_count(&state);
        assert_eq!(state.queued_count.load(Ordering::Acquire), 0);

        decrement_queued_trace_count(&state);
        assert_eq!(state.queued_count.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_gen_group_name() {
        let dispatcher = AsyncTraceDispatcher::new(test_runtime(), "TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        let name1 = dispatcher.gen_group_name();
        let name2 = dispatcher.gen_group_name();

        assert!(name1.starts_with(TraceConstants::GROUP_NAME_PREFIX));
        assert!(name1.contains("TestGroup"));
        assert_eq!(name1, name2); // Fixed Java group is stable per host group and role.
        assert_eq!(name1, "_INNER_TRACE_PRODUCER-TestGroup-PRODUCE");
    }

    #[test]
    fn test_append_before_start() {
        let dispatcher = AsyncTraceDispatcher::new(test_runtime(), "TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        let ctx = TraceContext::default();

        // Should succeed even before start (queue is available)
        let result = dispatcher.append(&ctx);
        assert!(result);
        assert_eq!(dispatcher.queue_size(), 1);
    }

    #[test]
    fn test_batch_num_capping() {
        // Test that batch_num is capped at 20
        let dispatcher = AsyncTraceDispatcher::new(
            test_runtime(),
            "TestGroup",
            Type::Produce,
            100, // Request 100
            "TRACE_TOPIC",
            None,
        );

        assert_eq!(dispatcher.config.batch_num, 100); // Fixed Java batch size is100, no20 cap.
    }

    #[test]
    fn test_default_trace_topic() {
        // Test default topic when empty string provided
        let dispatcher = AsyncTraceDispatcher::new(
            test_runtime(),
            "TestGroup",
            Type::Produce,
            20,
            "", // Empty topic
            None,
        );

        assert_eq!(dispatcher.config.trace_topic_name, TopicValidator::RMQ_SYS_TRACE_TOPIC);
    }

    #[test]
    fn test_config_values() {
        let dispatcher =
            AsyncTraceDispatcher::new(test_runtime(), "TestGroup", Type::Consume, 15, "CUSTOM_TOPIC", None);

        assert_eq!(dispatcher.config.group, "TestGroup");
        assert_eq!(dispatcher.config.type_, Type::Consume);
        assert_eq!(dispatcher.config.batch_num, 15);
        assert_eq!(dispatcher.config.max_msg_size, 128_000);
        assert_eq!(dispatcher.config.flush_interval, Duration::from_millis(5));
        assert_eq!(dispatcher.config.trace_topic_name, "CUSTOM_TOPIC");
    }

    #[test]
    fn test_set_client_host() {
        let dispatcher = AsyncTraceDispatcher::new(test_runtime(), "TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        assert!(dispatcher.trace_client_host().is_none());

        dispatcher.set_client_host(Some(CheetahString::from_static_str("127.0.0.1@trace-client")));

        assert_eq!(
            dispatcher.trace_client_host().as_deref(),
            Some("127.0.0.1@trace-client")
        );
    }

    #[test]
    fn test_set_namespace_v2() {
        let dispatcher = AsyncTraceDispatcher::new(test_runtime(), "TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        dispatcher.set_namespace_v2(Some(CheetahString::from_static_str("test-namespace")));

        let ns = dispatcher.state.namespace_v2.read();
        assert!(ns.is_some());
        assert_eq!(ns.as_ref().unwrap().to_string(), "test-namespace");
    }

    #[test]
    fn test_flush_before_start() {
        let dispatcher = AsyncTraceDispatcher::new(test_runtime(), "TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        // Should return error since not started
        let result = dispatcher.flush();
        assert!(result.is_err());
    }

    #[test]
    fn java_shutdown_hook_aliases_are_noop_under_drop_based_shutdown() {
        let dispatcher = AsyncTraceDispatcher::new(test_runtime(), "TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        dispatcher.register_shutdown_hook();
        dispatcher.register_shut_down_hook();
        dispatcher.remove_shutdown_hook();

        assert!(!dispatcher.is_started());
        assert!(!dispatcher.is_stopped());
    }

    #[tokio::test]
    async fn flush_buffer_empty_records_completion_time() {
        let state = Arc::new(DispatcherState::new());
        let producer = Arc::new(tokio::sync::Mutex::new(
            DefaultMQProducer::builder(test_runtime()).build(),
        ));
        let config = TraceDispatcherConfig {
            group: "Test".to_string(),
            type_: Type::Produce,
            batch_num: 5,
            max_msg_size: 128_000,
            flush_interval: Duration::from_millis(100),
            trace_topic_name: "TRACE".to_string(),
            rpc_hook: None,
        };
        let mut buffer = Vec::new();

        assert_eq!(state.last_flush_time.load(Ordering::Acquire), 0);
        flush_buffer(&mut buffer, &state, &producer, &config)
            .await
            .expect("empty flush should complete");

        assert!(state.last_flush_time.load(Ordering::Acquire) > 0);
    }

    #[tokio::test]
    async fn test_worker_loop_basic() {
        // This test verifies the worker loop structure without actually starting
        // the producer (which requires a real broker connection).
        // The worker_loop will return an error because producer.start() fails,
        // but that's expected in a unit test environment.

        let (tx, rx) = mpsc::channel(10);
        let state = Arc::new(DispatcherState::new());
        let producer = Arc::new(tokio::sync::Mutex::new(
            DefaultMQProducer::builder(test_runtime()).build(),
        ));
        let config = TraceDispatcherConfig {
            group: "Test".to_string(),
            type_: Type::Produce,
            batch_num: 5,
            max_msg_size: 128_000,
            flush_interval: Duration::from_millis(100),
            trace_topic_name: "TRACE".to_string(),
            rpc_hook: None,
        };

        // Close the channel immediately
        drop(tx);

        // Worker will try to start producer and fail (expected in test environment)
        // The test just verifies no panic occurs
        let result = worker_loop(rx, state, producer, config).await;

        // In unit test environment, start() will fail due to missing broker
        assert!(
            result.is_err(),
            "Expected error from producer start in test environment"
        );
    }
}
