// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.
//! Bounded Custom consumption tracking. A successful oneway write is not a Broker record ACK.
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::{consume_message_context::ConsumeMessageContext, consume_message_hook::ConsumeMessageHook};
use crate::runtime::{spawn_client_tracked_task_with_context, ClientTrackedTaskHandle};
use crate::{ClientError, ClientResult};
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
pub use rocketmq_protocol::track::*;
use rocketmq_runtime::ChildServiceContext;
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
    time::Duration,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrackReporterConfig {
    pub context_capacity: usize,
    pub appender_capacity: usize,
    pub batch_size: usize,
    pub core_concurrency: usize,
    pub max_concurrency: usize,
    pub send_timeout_millis: u64,
    pub drain_timeout_millis: u64,
}
impl Default for TrackReporterConfig {
    fn default() -> Self {
        Self {
            context_capacity: 1024,
            appender_capacity: 2048,
            batch_size: 5,
            core_concurrency: 10,
            max_concurrency: 20,
            send_timeout_millis: 1000,
            drain_timeout_millis: 500,
        }
    }
}
impl TrackReporterConfig {
    pub const ASYNC_BUFFER_SIZE: &'static str = "TRACK_ASYNC_BUFFER_SIZE";
    pub const MAX_BATCH_NUM: &'static str = "TRACK_MAX_BATCH_NUM";
    /// Native equivalent of Java system properties. Called explicitly by the application.
    pub fn from_env() -> ClientResult<Self> {
        fn read(name: &str) -> ClientResult<Option<String>> {
            match std::env::var(name) {
                Ok(value) => Ok(Some(value)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(std::env::VarError::NotUnicode(_)) => {
                    Err(ClientError::illegal_argument("track setting is not valid Unicode"))
                }
            }
        }
        Self::from_properties(
            read(Self::ASYNC_BUFFER_SIZE)?.as_deref(),
            read(Self::MAX_BATCH_NUM)?.as_deref(),
        )
    }
    pub fn from_properties(buffer_size: Option<&str>, batch_size: Option<&str>) -> ClientResult<Self> {
        let mut config = Self::default();
        if let Some(value) = buffer_size {
            let value: i32 = value
                .parse()
                .map_err(|_| ClientError::illegal_argument("invalid track buffer capacity"))?;
            // Fixed Java's shift expression maps nonpositive values to capacity1.
            config.appender_capacity = if value <= 0 { 1 } else { value as usize };
        }
        if let Some(value) = batch_size {
            config.batch_size = value
                .parse()
                .map_err(|_| ClientError::illegal_argument("invalid track batch size"))?;
        }
        config.normalized()
    }
    pub fn normalized(mut self) -> ClientResult<Self> {
        let invalid = self.context_capacity == 0
            || self.batch_size == 0
            || self.core_concurrency == 0
            || self.max_concurrency < self.core_concurrency
            || self.send_timeout_millis == 0
            || self.drain_timeout_millis == 0
            || [
                self.context_capacity,
                self.appender_capacity,
                self.batch_size,
                self.max_concurrency,
            ]
            .iter()
            .any(|value| *value > i32::MAX as usize);
        if invalid {
            return Err(ClientError::illegal_argument("invalid track reporter limits"));
        }
        self.appender_capacity = self
            .appender_capacity
            .max(1)
            .checked_next_power_of_two()
            .filter(|value| *value <= i32::MAX as usize)
            .ok_or_else(|| ClientError::illegal_argument("track buffer power-of-two overflow"))?;
        Ok(self)
    }
}
/// Bounded diagnostic metadata; never retains arbitrary error text or credentials.
#[derive(Clone, Debug, Serialize)]
pub struct TrackFailureObservation {
    pub failure_kind: String,
    pub descriptor: String,
    pub request_stage: Option<String>,
}
impl TrackFailureObservation {
    fn from_error(error: &ClientError) -> Self {
        Self {
            failure_kind: format!("{:?}", error.failure_kind()),
            descriptor: error.descriptor().code().as_str().to_string(),
            request_stage: error.request_stage().map(|stage| format!("{stage:?}")),
        }
    }
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct TrackReporterSnapshot {
    pub accepted: u64,
    pub settled: u64,
    pub pending: u64,
    pub in_flight: u64,
    pub locally_written: u64,
    pub failed: u64,
    pub invalid: u64,
    pub rejected_full: u64,
    pub appender_full: u64,
    pub rejected_closed: u64,
    pub discarded_after_accept: u64,
    pub write_outcome_unknown: u64,
    pub business_outcome_unknown: u64,
    /// The oneway protocol provides no observation of final Broker records.
    pub server_recorded: Option<u64>,
    pub last_failure: Option<TrackFailureObservation>,
}
#[derive(Clone, Debug, Serialize)]
pub struct TrackFlushReport {
    pub target: u64,
    pub settled_through: u64,
    pub complete: bool,
    pub snapshot: TrackReporterSnapshot,
}
#[derive(Clone, Debug, Serialize)]
pub struct TrackShutdownReport {
    pub drain: TrackFlushReport,
    pub worker_stopped: bool,
}
#[derive(Default)]
struct Watermark {
    through: u64,
    later: BTreeSet<u64>,
}
#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    settled: AtomicU64,
    in_flight: AtomicU64,
    written: AtomicU64,
    failed: AtomicU64,
    invalid: AtomicU64,
    full: AtomicU64,
    appender_full: AtomicU64,
    closed: AtomicU64,
    discarded: AtomicU64,
    unknown_write: AtomicU64,
    unknown_business: AtomicU64,
    watermark: Mutex<Watermark>,
    last_failure: Mutex<Option<TrackFailureObservation>>,
    wake: tokio::sync::Notify,
}
impl Counters {
    fn finish(&self, ticket: u64) {
        self.settled.fetch_add(1, Ordering::AcqRel);
        let mut watermark = self.watermark.lock().unwrap_or_else(|p| p.into_inner());
        watermark.later.insert(ticket);
        loop {
            let next = watermark.through + 1;
            if !watermark.later.remove(&next) {
                break;
            }
            watermark.through = next;
        }
        drop(watermark);
        self.wake.notify_waiters();
    }
    fn through(&self) -> u64 {
        self.watermark.lock().unwrap_or_else(|p| p.into_inner()).through
    }
    fn snapshot(&self) -> TrackReporterSnapshot {
        let read = |value: &AtomicU64| value.load(Ordering::Acquire);
        let accepted = read(&self.accepted);
        let settled = read(&self.settled);
        TrackReporterSnapshot {
            accepted,
            settled,
            pending: accepted.saturating_sub(settled),
            in_flight: read(&self.in_flight),
            locally_written: read(&self.written),
            failed: read(&self.failed),
            invalid: read(&self.invalid),
            rejected_full: read(&self.full),
            appender_full: read(&self.appender_full),
            rejected_closed: read(&self.closed),
            discarded_after_accept: read(&self.discarded),
            write_outcome_unknown: read(&self.unknown_write),
            business_outcome_unknown: read(&self.unknown_business),
            server_recorded: None,
            last_failure: self.last_failure.lock().unwrap_or_else(|p| p.into_inner()).clone(),
        }
    }
}
#[derive(Clone, Copy)]
enum WorkPhase {
    Waiting,
    InFlight,
    Settled,
}
struct Work {
    context: TrackContext,
    ticket: u64,
    admitted: bool,
    phase: WorkPhase,
    counters: Arc<Counters>,
}
impl Work {
    fn begin(&mut self) {
        self.phase = WorkPhase::InFlight;
        self.counters.in_flight.fetch_add(1, Ordering::AcqRel);
    }
    fn finish(&mut self, succeeded: bool) {
        self.counters.in_flight.fetch_sub(1, Ordering::AcqRel);
        if succeeded {
            self.counters.written.fetch_add(1, Ordering::AcqRel);
        } else {
            self.counters.failed.fetch_add(1, Ordering::AcqRel);
        }
        self.phase = WorkPhase::Settled;
    }
    fn invalid(&mut self) {
        self.counters.invalid.fetch_add(1, Ordering::AcqRel);
        self.phase = WorkPhase::Settled;
    }
}
impl Drop for Work {
    fn drop(&mut self) {
        if !self.admitted {
            return;
        }
        match self.phase {
            WorkPhase::Waiting => {
                self.counters.discarded.fetch_add(1, Ordering::AcqRel);
            }
            WorkPhase::InFlight => {
                self.counters.unknown_write.fetch_add(1, Ordering::AcqRel);
                self.counters.in_flight.fetch_sub(1, Ordering::AcqRel);
            }
            WorkPhase::Settled => {}
        }
        self.counters.finish(self.ticket);
    }
}

/// Portable equivalent of the Java MQClientInstance sender binding. Custom
/// implementations run on the reporter task, never the business callback lane.
pub trait TrackReportSender: Send + Sync {
    fn report_track(&self, transfer: TrackTransferBean, timeout_millis: u64) -> BoxFuture<'_, ClientResult<()>>;
}
/// Authenticated, lifecycle-bound access to custom tracking. Cloning this handle
/// does not retain the client or create an independent connection pool.
#[derive(Clone)]
pub struct CustomTrackClient(Weak<MQClientInstance>);
impl CustomTrackClient {
    pub(crate) fn for_instance(instance: &Arc<MQClientInstance>) -> Self {
        Self(Arc::downgrade(instance))
    }
    /// Choose replica ID1, falling back to the master, as Java MQClientInstance does.
    pub async fn report(&self, transfer: &TrackTransferBean, timeout_millis: u64) -> ClientResult<()> {
        self.0
            .upgrade()
            .ok_or_else(ClientError::not_started)?
            .report_track(transfer, timeout_millis)
            .await
    }
    /// Java MQClientAPIImpl.reportTrack: send directly to the supplied address,
    /// with the owning client's authentication and without VIP port rewriting.
    /// Success means the oneway frame was locally written, not recorded by Broker.
    pub async fn report_to(
        &self,
        broker_address: &str,
        transfer: &TrackTransferBean,
        timeout_millis: u64,
    ) -> ClientResult<()> {
        self.0
            .upgrade()
            .ok_or_else(ClientError::not_started)?
            .get_mq_client_api_impl()?
            .report_track(&broker_address.into(), transfer, timeout_millis)
            .await
    }
}
impl TrackReportSender for CustomTrackClient {
    fn report_track(&self, transfer: TrackTransferBean, timeout_millis: u64) -> BoxFuture<'_, ClientResult<()>> {
        Box::pin(async move { self.report(&transfer, timeout_millis).await })
    }
}

pub trait AsyncReporter: Send + Sync {
    fn start(&self) -> ClientResult<()>;
    fn append(&self, context: TrackContext) -> bool;
    fn flush(&self) -> BoxFuture<'_, TrackFlushReport>;
    fn shutdown(&self) -> BoxFuture<'_, TrackShutdownReport>;
}
pub struct AsyncTrackReporter {
    self_reference: Weak<Self>,
    service_context: ChildServiceContext,
    config: TrackReporterConfig,
    counters: Arc<Counters>,
    sender: RwLock<Arc<dyn TrackReportSender>>,
    admission: Mutex<Option<tokio::sync::mpsc::Sender<Work>>>,
    receiver: Mutex<Option<tokio::sync::mpsc::Receiver<Work>>>,
    task: Mutex<Option<ClientTrackedTaskHandle>>,
    started: AtomicBool,
    shutdown_hook: AtomicBool,
    shutdown_transition: tokio::sync::Mutex<()>,
}
impl AsyncTrackReporter {
    pub fn new(
        service_context: ChildServiceContext,
        config: TrackReporterConfig,
        sender: Arc<dyn TrackReportSender>,
    ) -> ClientResult<Arc<Self>> {
        let config = config.normalized()?;
        let (tx, rx) = tokio::sync::mpsc::channel(config.context_capacity);
        Ok(Arc::new_cyclic(|weak| Self {
            self_reference: weak.clone(),
            service_context,
            config,
            sender: RwLock::new(sender),
            counters: Arc::new(Counters::default()),
            admission: Mutex::new(Some(tx)),
            receiver: Mutex::new(Some(rx)),
            task: Mutex::new(None),
            started: AtomicBool::new(false),
            shutdown_hook: AtomicBool::new(false),
            shutdown_transition: tokio::sync::Mutex::new(()),
        }))
    }
    pub(crate) fn for_instance(
        context: ChildServiceContext,
        config: TrackReporterConfig,
        instance: &Arc<MQClientInstance>,
    ) -> ClientResult<Arc<Self>> {
        Self::new(context, config, Arc::new(CustomTrackClient::for_instance(instance)))
    }
    pub fn config(&self) -> &TrackReporterConfig {
        &self.config
    }
    pub fn sender(&self) -> Arc<dyn TrackReportSender> {
        self.sender.read().unwrap_or_else(|p| p.into_inner()).clone()
    }
    pub fn set_sender(&self, sender: Arc<dyn TrackReportSender>) {
        *self.sender.write().unwrap_or_else(|p| p.into_inner()) = sender;
    }
    /// Structured root cancellation replaces the JVM process shutdown hook.
    pub fn register_shutdown_hook(&self) {
        self.shutdown_hook.store(true, Ordering::Release);
    }
    pub fn remove_shutdown_hook(&self) {
        self.shutdown_hook.store(false, Ordering::Release);
    }
    pub fn snapshot(&self) -> TrackReporterSnapshot {
        // Serialize with admission so a fast completed send cannot appear before its accepted count.
        let _admission = self.admission.lock().unwrap_or_else(|p| p.into_inner());
        self.counters.snapshot()
    }
    pub fn start(&self) -> ClientResult<()> {
        let mut task = self.task.lock().unwrap_or_else(|p| p.into_inner());
        if self
            .admission
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_none_or(|sender| sender.is_closed())
        {
            return Err(ClientError::invalid_state("open track reporter", "closed"));
        }
        if self.started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let receiver = self
            .receiver
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .ok_or_else(|| ClientError::invalid_state("unstarted track reporter", "closed"))?;
        self.register_shutdown_hook();
        let owner = self.self_reference.upgrade().ok_or_else(ClientError::not_started)?;
        match spawn_client_tracked_task_with_context(
            &self.service_context,
            "client.custom-track",
            Box::pin(async move {
                owner.run(receiver).await;
            }),
        ) {
            Ok(handle) => {
                *task = Some(handle);
                Ok(())
            }
            Err(error) => {
                self.admission.lock().unwrap_or_else(|p| p.into_inner()).take();
                Err(ClientError::service_source("start Custom track reporter", error))
            }
        }
    }
    pub fn append(&self, context: TrackContext) -> bool {
        let admission = self.admission.lock().unwrap_or_else(|p| p.into_inner());
        if self.service_context.task_group().cancellation_token().is_cancelled() {
            self.counters.closed.fetch_add(1, Ordering::AcqRel);
            return false;
        }
        let Some(sender) = admission.as_ref() else {
            self.counters.closed.fetch_add(1, Ordering::AcqRel);
            return false;
        };
        // Custom fork: a stalled low ticket must also bound the out-of-order settlement ledger.
        let maximum = self.config.context_capacity.saturating_add(
            self.config
                .appender_capacity
                .saturating_add(self.config.max_concurrency)
                .saturating_mul(self.config.batch_size),
        );
        let accepted = self.counters.accepted.load(Ordering::Acquire);
        if accepted.saturating_sub(self.counters.through()) >= maximum as u64 {
            self.counters.full.fetch_add(1, Ordering::AcqRel);
            return false;
        }
        let ticket = accepted + 1;
        let work = Work {
            context,
            ticket,
            admitted: true,
            phase: WorkPhase::Waiting,
            counters: self.counters.clone(),
        };
        match sender.try_send(work) {
            Ok(()) => {
                self.counters.accepted.store(ticket, Ordering::Release);
                true
            }
            Err(error) => {
                let full = matches!(&error, tokio::sync::mpsc::error::TrySendError::Full(_));
                let mut work = error.into_inner();
                work.admitted = false;
                if full {
                    self.counters.full.fetch_add(1, Ordering::AcqRel);
                } else {
                    self.counters.closed.fetch_add(1, Ordering::AcqRel);
                }
                false
            }
        }
    }
    async fn wait_through(&self, target: u64) -> TrackFlushReport {
        let settled = tokio::time::timeout(Duration::from_millis(self.config.drain_timeout_millis), async {
            loop {
                let wake = self.counters.wake.notified();
                tokio::pin!(wake);
                wake.as_mut().enable();
                if self.counters.through() >= target {
                    break;
                }
                wake.await;
            }
        })
        .await
        .is_ok();
        TrackFlushReport {
            target,
            settled_through: self.counters.through(),
            complete: settled,
            snapshot: self.snapshot(),
        }
    }
    /// A precise prefix barrier: later out-of-order completions cannot substitute
    /// for an unfinished record accepted before this call.
    pub async fn flush(&self) -> TrackFlushReport {
        let target = {
            let _admission = self.admission.lock().unwrap_or_else(|p| p.into_inner());
            self.counters.accepted.load(Ordering::Acquire)
        };
        self.wait_through(target).await
    }
    pub async fn shutdown(&self) -> TrackShutdownReport {
        let _transition = self.shutdown_transition.lock().await;
        self.admission.lock().unwrap_or_else(|p| p.into_inner()).take();
        // If start was never called, close and release its bounded input queue.
        self.receiver.lock().unwrap_or_else(|p| p.into_inner()).take();
        let drain = self.flush().await;
        let handle = self.task.lock().unwrap_or_else(|p| p.into_inner()).take();
        let worker_stopped = if let Some(handle) = handle {
            if drain.complete {
                handle.shutdown(Duration::from_millis(100)).await.is_healthy()
            } else {
                handle.abort_and_wait(Duration::from_millis(100)).await
            }
        } else {
            true
        };
        self.remove_shutdown_hook();
        TrackShutdownReport {
            drain: TrackFlushReport {
                snapshot: self.snapshot(),
                ..drain
            },
            worker_stopped,
        }
    }
    async fn run(self: Arc<Self>, mut receiver: tokio::sync::mpsc::Receiver<Work>) {
        let root_stop = self.service_context.task_group().cancellation_token();
        let mut jobs: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();
        let mut pending = VecDeque::new();
        let mut limit = self.config.core_concurrency;
        let mut input_closed = false;
        let mut root_deadline = None;
        loop {
            while jobs.len() < limit {
                if let Some(batch) = pending.pop_front() {
                    jobs.push(self.clone().send_batch(batch));
                } else {
                    break;
                }
            }
            if input_closed && pending.is_empty() && jobs.is_empty() {
                break;
            }
            tokio::select! {
                _ = root_stop.cancelled(), if root_deadline.is_none() => {
                    self.admission.lock().unwrap_or_else(|p| p.into_inner()).take(); receiver.close();
                    if !self.shutdown_hook.load(Ordering::Acquire) { break; }
                    root_deadline = Some(tokio::time::Instant::now() + Duration::from_millis(self.config.drain_timeout_millis));
                }
                _ = async { if let Some(deadline) = root_deadline { tokio::time::sleep_until(deadline).await; } else { std::future::pending::<()>().await; } } => break,
                _ = jobs.next(), if !jobs.is_empty() => {},
                work = receiver.recv(), if !input_closed => {
                    match work {
                        Some(work) => {
                            let mut batch = vec![work];
                            while batch.len() < self.config.batch_size { if let Ok(work) = receiver.try_recv() { batch.push(work); } else { break; } }
                            if jobs.len() < limit { jobs.push(self.clone().send_batch(batch)); }
                            else if pending.len() < self.config.appender_capacity { pending.push_back(batch); }
                            else if limit < self.config.max_concurrency { limit += 1; jobs.push(self.clone().send_batch(batch)); }
                            else { self.counters.appender_full.fetch_add(batch.len() as u64, Ordering::AcqRel); }
                            // Dropping this already-admitted batch is counted separately from input rejection.
                        }
                        None => input_closed = true,
                    }
                }
            }
            if jobs.is_empty() && pending.is_empty() {
                limit = self.config.core_concurrency;
            }
        }
    }
    fn send_batch(self: Arc<Self>, batch: Vec<Work>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let mut groups: BTreeMap<Option<cheetah_string::CheetahString>, Vec<Work>> = BTreeMap::new();
            for work in batch {
                groups.entry(work.context.broker_name.clone()).or_default().push(work);
            }
            for (broker, mut works) in groups {
                let contexts: Vec<_> = works.iter().map(|work| work.context.clone()).collect();
                let transfer = TrackEncoder::generate(&contexts);
                let mut transfer = match transfer {
                    Ok(Some(transfer)) => transfer,
                    _ => {
                        *self.counters.last_failure.lock().unwrap_or_else(|p| p.into_inner()) =
                            Some(TrackFailureObservation::from_error(&ClientError::illegal_argument(
                                "unencodable tracking context",
                            )));
                        for work in &mut works {
                            work.invalid();
                        }
                        continue;
                    }
                };
                transfer.broker_name = broker;
                for work in &mut works {
                    work.begin();
                }
                let sender = self.sender();
                let result = tokio::time::timeout(
                    Duration::from_millis(self.config.send_timeout_millis),
                    sender.report_track(transfer, self.config.send_timeout_millis),
                )
                .await;
                let observation = match &result {
                    Ok(Err(error)) => Some(TrackFailureObservation::from_error(error)),
                    Err(_) => Some(TrackFailureObservation::from_error(&ClientError::timeout(
                        "reportTrack",
                        self.config.send_timeout_millis,
                    ))),
                    _ => None,
                };
                if let Some(observation) = observation {
                    *self.counters.last_failure.lock().unwrap_or_else(|p| p.into_inner()) = Some(observation);
                }
                let success = matches!(result, Ok(Ok(())));
                // A cancelled send may already have written bytes. RAII records
                // unknown instead of claiming it was definitely not transmitted.
                let uncertain = match &result {
                    Err(_) => true,
                    Ok(Err(error)) => matches!(
                        error.request_stage(),
                        Some(
                            rocketmq_transport::api::OutboundRequestStage::Writing
                                | rocketmq_transport::api::OutboundRequestStage::AwaitingResponse
                        )
                    ),
                    _ => false,
                };
                if !uncertain {
                    for work in &mut works {
                        work.finish(success);
                    }
                }
                if !success {
                    tracing::warn!("Custom track batch was not locally confirmed written; inspect reporter counters");
                }
            }
        })
    }
}
impl AsyncReporter for AsyncTrackReporter {
    fn start(&self) -> ClientResult<()> {
        AsyncTrackReporter::start(self)
    }
    fn append(&self, context: TrackContext) -> bool {
        AsyncTrackReporter::append(self, context)
    }
    fn flush(&self) -> BoxFuture<'_, TrackFlushReport> {
        Box::pin(AsyncTrackReporter::flush(self))
    }
    fn shutdown(&self) -> BoxFuture<'_, TrackShutdownReport> {
        Box::pin(AsyncTrackReporter::shutdown(self))
    }
}

pub struct ReportConsumeStageHook {
    reporter: Arc<AsyncTrackReporter>,
    host: cheetah_string::CheetahString,
}
impl ReportConsumeStageHook {
    pub fn new(reporter: Arc<AsyncTrackReporter>, host: impl Into<cheetah_string::CheetahString>) -> Self {
        Self {
            reporter,
            host: host.into(),
        }
    }
}
impl ConsumeMessageHook for ReportConsumeStageHook {
    fn hook_name(&self) -> &'static str {
        "Custom.ReportConsumeStageHook"
    }
    fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
        context.start_time = rocketmq_runtime::common::time_utils::current_millis() as i64;
    }
    fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
        if !context.track_enabled || context.msg_list.is_empty() {
            return;
        }
        context.end_time = rocketmq_runtime::common::time_utils::current_millis() as i64;
        let cost = (context.end_time.wrapping_sub(context.start_time) / context.msg_list.len() as i64) as i32;
        let code = match context
            .props
            .get(rocketmq_model::common::mix_all::CONSUME_CONTEXT_TYPE)
            .map(|value| value.as_str())
        {
            Some("SUCCESS") => "0",
            Some("TIME_OUT") => "1",
            Some("EXCEPTION") => "2",
            Some("RETURNNULL") => "3",
            Some("FAILED") => "4",
            _ => "-1",
        };
        if context
            .message_outcomes
            .as_ref()
            .is_some_and(|outcomes| outcomes.len() != context.msg_list.len())
        {
            self.reporter.counters.invalid.fetch_add(1, Ordering::AcqRel);
            return;
        }
        let mut outcomes: BTreeMap<(cheetah_string::CheetahString, bool, &str), Vec<TrackBean>> = BTreeMap::new();
        use rocketmq_model::common::message::{MessageConst, MessageTrait};
        for (index, message) in context.msg_list.iter().enumerate() {
            if message.delivery_track_enabled == Some(false) {
                continue;
            }
            use crate::hook::consume_message_context::ConsumeBusinessOutcome;
            let explicit = context.message_outcomes.as_ref().map(|outcomes| outcomes[index]);
            let success = explicit.map_or(context.success && index as i64 <= context.ack_index as i64, |outcome| {
                outcome == ConsumeBusinessOutcome::Success
            });
            let unknown = explicit.map_or(context.status == "CONSUME_UNKNOWN", |outcome| {
                outcome == ConsumeBusinessOutcome::Unknown
            });
            if unknown {
                self.reporter.counters.unknown_business.fetch_add(1, Ordering::AcqRel);
            }
            let code = match explicit {
                Some(ConsumeBusinessOutcome::Success) => "0",
                Some(ConsumeBusinessOutcome::Failure) => "4",
                Some(ConsumeBusinessOutcome::Unknown) => "-1",
                None => code,
            };
            let broker = if message.broker_name.is_empty() {
                context.broker_name.clone()
            } else {
                message.broker_name.clone()
            };
            outcomes.entry((broker, success, code)).or_default().push(TrackBean {
                msg_id: Some(
                    message
                        .property(&MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX.into())
                        .unwrap_or_else(|| message.msg_id.clone()),
                ),
                offset_msg_id: Some(message.msg_id.clone()),
                retry_times: Some(message.reconsume_times.to_string().into()),
                status: Some(if success { "1" } else { "0" }.into()),
            });
        }
        for ((broker, success, code), track_beans) in outcomes {
            // Fixed Broker ignores each bean.status. Homogeneous contexts retain
            // correct per-message results without requiring a server patch.
            let record = TrackContext {
                group_name: Some(context.consumer_group.clone()),
                topic: context.mq.as_ref().map(|queue| queue.topic().clone()),
                broker_name: Some(broker),
                host: Some(self.host.clone()),
                start_time: Some(context.start_time.to_string().into()),
                track_type: Some(TrackType::Sub),
                cost_time: Some(cost.to_string().into()),
                status: Some(if success { "1" } else { "0" }.into()),
                return_type_code: Some(code.into()),
                track_beans,
            };
            self.reporter.append(record);
        }
    }
}

/// Native equivalents of the Java Tracker overloads. Logging is opt-in.
pub trait Tracker: Send + Sync {
    fn is_enabled(&self) -> bool;
    fn record_produce(&self, record: &ProduceStageRecord);
    fn record_store(&self, record: &StoreStageRecord);
    fn record_consume(&self, record: &ConsumeStageRecord);
}
pub struct LogTracker {
    enabled: AtomicBool,
    broker_ip: RwLock<cheetah_string::CheetahString>,
}
impl LogTracker {
    pub fn new(enabled: bool, broker_ip: impl Into<cheetah_string::CheetahString>) -> Self {
        Self {
            enabled: AtomicBool::new(enabled),
            broker_ip: RwLock::new(broker_ip.into()),
        }
    }
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Release);
    }
    pub fn set_broker_ip(&self, ip: impl Into<cheetah_string::CheetahString>) {
        *self.broker_ip.write().unwrap_or_else(|p| p.into_inner()) = ip.into();
    }
    fn text(value: &Option<cheetah_string::CheetahString>) -> &str {
        value.as_deref().unwrap_or("null")
    }
    fn blank(value: &Option<cheetah_string::CheetahString>) -> &str {
        value.as_deref().unwrap_or("")
    }
    pub fn format_produce(record: &ProduceStageRecord) -> String {
        format!(
            "id={},oid={},mt={},pg={},ip={},tp={},ts={},cd={},rmk={},-_-,key={},-_-,tag={}",
            Self::text(&record.message_id),
            Self::text(&record.offset_msg_id),
            record.msg_type,
            Self::text(&record.producer_group),
            Self::text(&record.host),
            Self::text(&record.topic),
            record.create_time,
            record.code,
            Self::blank(&record.remark),
            Self::blank(&record.key),
            Self::blank(&record.tag)
        )
    }
    pub fn format_store(&self, record: &StoreStageRecord) -> String {
        let fallback = self.broker_ip.read().unwrap_or_else(|p| p.into_inner()).clone();
        let host = record
            .host
            .as_deref()
            .filter(|value| !value.is_empty())
            .unwrap_or(fallback.as_str());
        format!(
            "id={},oid={},mt={},bn={},bid={},ip={},sts={},cst={},tp={},qid={},pr={},rtt={}",
            Self::text(&record.msg_id),
            Self::blank(&record.offset_msg_id),
            record.msg_type,
            Self::text(&record.broker),
            Self::text(&record.broker_id),
            host,
            record.start_time,
            record.cost_time,
            Self::text(&record.topic),
            Self::text(&record.queue_id),
            record.put_result().map_or("null", |value| value.as_str()),
            record.retry_times
        )
    }
    pub fn format_consume(record: &ConsumeStageRecord) -> String {
        format!(
            "id={},oid={},cg={},ip={},sts={},cst={},tp={},rtt={},st={},rtc={}",
            Self::text(&record.msg_id),
            Self::text(&record.offset_msg_id),
            Self::text(&record.consumer_group),
            Self::text(&record.host),
            Self::text(&record.create_time),
            Self::text(&record.cost_time),
            Self::text(&record.topic),
            Self::text(&record.retry_times),
            Self::text(&record.status),
            Self::text(&record.return_type_code)
        )
    }
}
impl Tracker for LogTracker {
    fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }
    fn record_produce(&self, record: &ProduceStageRecord) {
        if self.is_enabled() {
            tracing::info!(target: "custom::track::produce", "{}", Self::format_produce(record));
        }
    }
    fn record_store(&self, record: &StoreStageRecord) {
        if self.is_enabled() {
            tracing::info!(target: "custom::track::store", "{}", self.format_store(record));
        }
    }
    fn record_consume(&self, record: &ConsumeStageRecord) {
        if self.is_enabled() {
            tracing::info!(target: "custom::track::consume", "{}", Self::format_consume(record));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClientRuntime, ClientRuntimeConfig, TelemetryHandle};
    use rocketmq_runtime::RuntimeContext;
    use std::sync::atomic::AtomicUsize;
    struct Sink {
        mode: u8,
        entered: AtomicUsize,
        first_gate: tokio::sync::Semaphore,
        records: Mutex<Vec<TrackTransferBean>>,
    }
    impl Sink {
        fn new(mode: u8) -> Arc<Self> {
            Arc::new(Self {
                mode,
                entered: AtomicUsize::new(0),
                first_gate: tokio::sync::Semaphore::new(0),
                records: Mutex::new(Vec::new()),
            })
        }
    }
    impl TrackReportSender for Sink {
        fn report_track(&self, transfer: TrackTransferBean, _: u64) -> BoxFuture<'_, ClientResult<()>> {
            Box::pin(async move {
                self.entered.fetch_add(1, Ordering::AcqRel);
                if self.mode == 1 {
                    return Err(ClientError::service_failed("controlled track sink"));
                }
                if self.mode == 2 {
                    std::future::pending::<()>().await;
                }
                if self.mode == 3 && transfer.trans_data.as_deref().unwrap_or_default().contains("first") {
                    self.first_gate.acquire().await.unwrap().forget();
                }
                self.records.lock().unwrap().push(transfer);
                Ok(())
            })
        }
    }
    fn context(id: &str) -> TrackContext {
        TrackContext {
            group_name: Some("group".into()),
            topic: Some("topic".into()),
            broker_name: Some("broker".into()),
            host: Some("127.0.0.1".into()),
            start_time: Some("1".into()),
            track_type: Some(TrackType::Sub),
            cost_time: Some("1".into()),
            status: Some("1".into()),
            return_type_code: Some("0".into()),
            track_beans: vec![TrackBean {
                msg_id: Some(id.into()),
                offset_msg_id: Some("physical".into()),
                retry_times: Some("0".into()),
                status: Some("1".into()),
            }],
        }
    }
    fn fixture(
        config: TrackReporterConfig,
        sink: Arc<Sink>,
    ) -> (RuntimeContext, Arc<ClientRuntime>, Arc<AsyncTrackReporter>) {
        let root = RuntimeContext::from_current("custom-track-unit");
        let runtime = ClientRuntime::try_new(
            root.service_context("sdk"),
            ClientRuntimeConfig::default(),
            TelemetryHandle::noop(),
        )
        .unwrap();
        let reporter = AsyncTrackReporter::new(runtime.component("track"), config, sink).unwrap();
        (root, runtime, reporter)
    }
    async fn until(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !predicate() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    async fn stop(root: RuntimeContext, runtime: Arc<ClientRuntime>, reporter: Arc<AsyncTrackReporter>) {
        assert!(reporter.shutdown().await.worker_stopped);
        assert!(runtime.shutdown().await.is_healthy());
        assert!(root.shutdown_tasks(Duration::from_secs(2)).await.is_healthy());
    }
    #[tokio::test]
    async fn unrepresentable_fields_are_counted_without_emitting_forged_records() {
        let sink = Sink::new(0);
        let (root, runtime, reporter) = fixture(TrackReporterConfig::default(), sink.clone());
        reporter.start().unwrap();
        assert!(reporter.append(context("id\u{4}forged")));
        let report = reporter.flush().await;
        assert!(report.complete);
        assert_eq!(report.snapshot.invalid, 1);
        assert_eq!(report.snapshot.locally_written, 0);
        assert!(report.snapshot.last_failure.is_some());
        assert!(sink.records.lock().unwrap().is_empty());
        stop(root, runtime, reporter).await;
    }
    #[tokio::test]
    async fn bounded_input_rejects_and_never_calls_a_closed_reporter_successful() {
        let sink = Sink::new(0);
        let config = TrackReporterConfig {
            context_capacity: 2,
            batch_size: 1,
            ..Default::default()
        };
        let (root, runtime, reporter) = fixture(config, sink.clone());
        assert!(reporter.append(context("a")));
        assert!(reporter.append(context("b")));
        assert!(!reporter.append(context("c")));
        assert_eq!(reporter.snapshot().rejected_full, 1);
        reporter.start().unwrap();
        let flushed = reporter.flush().await;
        assert!(flushed.complete);
        assert_eq!(flushed.snapshot.locally_written, 2);
        assert_eq!(flushed.snapshot.server_recorded, None);
        reporter.shutdown().await;
        assert!(!reporter.append(context("closed")));
        assert!(reporter.start().is_err());
        stop(root, runtime, reporter).await;
    }
    #[tokio::test]
    async fn appender_overflow_is_bounded_and_counted_after_admission() {
        let sink = Sink::new(3);
        let config = TrackReporterConfig {
            context_capacity: 1,
            appender_capacity: 1,
            batch_size: 1,
            core_concurrency: 1,
            max_concurrency: 1,
            ..Default::default()
        };
        let (root, runtime, reporter) = fixture(config, sink.clone());
        reporter.start().unwrap();
        assert!(reporter.append(context("first")));
        until(|| reporter.snapshot().in_flight == 1).await;
        assert!(reporter.append(context("second")));
        until(|| reporter.append(context("third"))).await;
        until(|| reporter.snapshot().appender_full == 1).await;
        sink.first_gate.add_permits(1);
        let flushed = reporter.flush().await;
        assert!(flushed.complete);
        assert_eq!(flushed.snapshot.accepted, 3);
        assert_eq!(flushed.snapshot.settled, 3);
        assert_eq!(flushed.snapshot.locally_written, 2);
        assert_eq!(flushed.snapshot.discarded_after_accept, 1);
        stop(root, runtime, reporter).await;
    }
    #[tokio::test]
    async fn flush_waits_for_its_prefix_even_when_later_sends_finish_first() {
        let sink = Sink::new(3);
        let config = TrackReporterConfig {
            batch_size: 1,
            core_concurrency: 2,
            max_concurrency: 2,
            ..Default::default()
        };
        let (root, runtime, reporter) = fixture(config, sink.clone());
        reporter.start().unwrap();
        reporter.append(context("first"));
        until(|| sink.entered.load(Ordering::Acquire) == 1).await;
        let flush = reporter.flush();
        tokio::pin!(flush);
        assert!(tokio::time::timeout(Duration::from_millis(10), flush.as_mut())
            .await
            .is_err());
        reporter.append(context("later"));
        until(|| reporter.snapshot().settled == 1).await;
        assert_eq!(reporter.counters.through(), 0);
        assert!(tokio::time::timeout(Duration::from_millis(10), flush.as_mut())
            .await
            .is_err());
        sink.first_gate.add_permits(1);
        let flushed = flush.await;
        assert!(flushed.complete);
        assert_eq!(flushed.target, 1);
        assert!(reporter.flush().await.complete);
        stop(root, runtime, reporter.clone()).await;
    }
    #[tokio::test]
    async fn stalled_prefix_bounds_the_settlement_ledger_even_when_later_sends_complete() {
        let sink = Sink::new(3);
        let config = TrackReporterConfig {
            context_capacity: 1,
            appender_capacity: 1,
            batch_size: 1,
            core_concurrency: 2,
            max_concurrency: 2,
            ..Default::default()
        };
        let (root, runtime, reporter) = fixture(config, sink.clone());
        reporter.start().unwrap();
        assert!(reporter.append(context("first")));
        until(|| sink.entered.load(Ordering::Acquire) == 1).await;
        for n in 2..=4 {
            assert!(reporter.append(context("later")));
            until(|| reporter.snapshot().settled == n - 1).await;
        }
        assert_eq!(reporter.counters.through(), 0);
        assert!(!reporter.append(context("bounded")));
        assert_eq!(reporter.snapshot().accepted, 4);
        sink.first_gate.add_permits(1);
        assert!(reporter.flush().await.complete);
        assert!(reporter.append(context("resumed")));
        assert!(reporter.flush().await.complete);
        stop(root, runtime, reporter).await;
    }
    #[tokio::test]
    async fn shutdown_deadline_cancels_sends_and_preserves_unknown_write_outcome() {
        let sink = Sink::new(2);
        let config = TrackReporterConfig {
            drain_timeout_millis: 20,
            ..Default::default()
        };
        let (root, runtime, reporter) = fixture(config, sink.clone());
        reporter.start().unwrap();
        reporter.append(context("first"));
        until(|| sink.entered.load(Ordering::Acquire) == 1).await;
        let start = std::time::Instant::now();
        let report = reporter.shutdown().await;
        assert!(start.elapsed() < Duration::from_millis(400));
        assert!(!report.drain.complete);
        assert!(report.worker_stopped);
        assert_eq!(report.drain.snapshot.pending, 0);
        assert_eq!(report.drain.snapshot.write_outcome_unknown, 1);
        assert_eq!(report.drain.snapshot.failed, 0);
        stop(root, runtime, reporter).await;
    }
    #[tokio::test]
    async fn send_timeout_is_unknown_and_a_typed_rejection_is_failed() {
        for mode in [1, 2] {
            let sink = Sink::new(mode);
            let config = TrackReporterConfig {
                send_timeout_millis: 20,
                ..Default::default()
            };
            let (root, runtime, reporter) = fixture(config, sink);
            reporter.start().unwrap();
            reporter.append(context("record"));
            let flushed = reporter.flush().await;
            assert!(flushed.complete);
            assert_eq!(flushed.snapshot.locally_written, 0);
            assert!(flushed.snapshot.last_failure.is_some());
            assert_eq!(flushed.snapshot.failed, u64::from(mode == 1));
            assert_eq!(flushed.snapshot.write_outcome_unknown, u64::from(mode == 2));
            stop(root, runtime, reporter).await;
        }
    }
    #[tokio::test]
    async fn root_cancellation_drains_or_accounts_for_remaining_records() {
        let sink = Sink::new(2);
        let config = TrackReporterConfig {
            drain_timeout_millis: 20,
            ..Default::default()
        };
        let (root, runtime, reporter) = fixture(config, sink.clone());
        reporter.start().unwrap();
        reporter.append(context("first"));
        until(|| sink.entered.load(Ordering::Acquire) == 1).await;
        assert!(root.shutdown_tasks(Duration::from_secs(2)).await.is_healthy());
        assert_eq!(reporter.snapshot().pending, 0);
        assert_eq!(reporter.snapshot().write_outcome_unknown, 1);
        assert!(runtime.shutdown().await.is_healthy());
    }
    #[tokio::test]
    async fn hook_splits_partial_ack_for_the_actual_broker_projection_and_keeps_disabled_records_out() {
        use crate::{Message, MessageExt, MessageQueue, MessageTrait};
        let sink = Sink::new(0);
        let (root, runtime, reporter) = fixture(TrackReporterConfig::default(), sink.clone());
        let hook = ReportConsumeStageHook::new(reporter.clone(), "127.0.0.1");
        let mut messages = Vec::new();
        for i in 0..3 {
            let mut message = MessageExt::default();
            message.set_message_inner(Message::builder().topic("topic").body("body").build_unchecked());
            message.msg_id = format!("physical-{i}").into();
            if i == 0 {
                message.put_property(
                    rocketmq_model::common::message::MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX.into(),
                    "client-unique".into(),
                );
            }
            message.broker_name = "broker".into();
            message.delivery_track_enabled = Some(i != 2);
            messages.push(Arc::new(message));
        }
        let mut business = ConsumeMessageContext::new("group".into(), &messages)
            .with_mq(MessageQueue::from_parts("topic", "broker", 0));
        hook.consume_message_before(&mut business);
        business.success = true;
        business.ack_index = 0;
        business.props.insert(
            rocketmq_model::common::mix_all::CONSUME_CONTEXT_TYPE.into(),
            "SUCCESS".into(),
        );
        hook.consume_message_after(&mut business);
        assert_eq!(reporter.snapshot().accepted, 2);
        reporter.start().unwrap();
        assert!(reporter.flush().await.complete);
        let mut statuses = BTreeMap::new();
        for transfer in sink.records.lock().unwrap().iter() {
            for record in TrackEncoder::decode(transfer.trans_data.as_deref().unwrap()).unwrap() {
                for bean in record.track_beans {
                    assert_eq!(bean.status, record.status);
                    if bean.msg_id.as_deref() == Some("client-unique") {
                        assert_eq!(bean.offset_msg_id.as_deref(), Some("physical-0"));
                    }
                    statuses.insert(
                        bean.msg_id.unwrap().to_string(),
                        record.status.clone().unwrap().to_string(),
                    );
                }
            }
        }
        assert_eq!(
            statuses,
            BTreeMap::from([("client-unique".into(), "1".into()), ("physical-1".into(), "0".into())])
        );
        stop(root, runtime, reporter).await;
    }
    #[test]
    fn reporter_defaults_limits_and_java_log_fields_are_preserved() {
        let config = TrackReporterConfig::default();
        assert_eq!(
            TrackReporterConfig::from_properties(Some("0"), None)
                .unwrap()
                .appender_capacity,
            1
        );
        assert_eq!(
            TrackReporterConfig::from_properties(Some("-1"), None)
                .unwrap()
                .appender_capacity,
            1
        );
        assert!(TrackReporterConfig::from_properties(Some("2147483647"), None).is_err());
        assert_eq!(
            (
                config.context_capacity,
                config.appender_capacity,
                config.batch_size,
                config.core_concurrency,
                config.max_concurrency
            ),
            (1024, 2048, 5, 10, 20)
        );
        assert_eq!(
            TrackReporterConfig {
                appender_capacity: 3,
                ..config.clone()
            }
            .normalized()
            .unwrap()
            .appender_capacity,
            4
        );
        assert!(TrackReporterConfig {
            batch_size: 0,
            ..config
        }
        .normalized()
        .is_err());
        let mut store = StoreStageRecord::default();
        assert_eq!(store.msg_type, -1);
        store.set_put_result(Some("PUT_OK".into()));
        assert_eq!(store.put_result().map(|v| v.as_str()), Some(""));
        store.set_put_result(Some("FLUSH_DISK_TIMEOUT".into()));
        store.set_put_result(Some("PUT_OK".into()));
        assert_eq!(store.put_result().map(|v| v.as_str()), Some("FLUSH_DISK_TIMEOUT"));
        let tracker = LogTracker::new(false, "127.0.0.9");
        assert!(!tracker.is_enabled());
        tracker.set_enabled(true);
        assert!(tracker.is_enabled());
        assert!(tracker.format_store(&store).contains("ip=127.0.0.9"));
        tracker.set_broker_ip("127.0.0.8");
        assert!(tracker.format_store(&store).contains("ip=127.0.0.8"));
        assert_eq!(
            LogTracker::format_produce(&ProduceStageRecord::default()),
            "id=null,oid=null,mt=0,pg=null,ip=null,tp=null,ts=0,cd=0,rmk=,-_-,key=,-_-,tag="
        );
    }
}
