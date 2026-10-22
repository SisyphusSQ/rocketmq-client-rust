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

// Custom fork: synchronous scoped removal makes dropped futures and shutdown release every index.
use crate::producer::request_response_future::RequestResponseFuture;
use crate::runtime::{schedule_client_fixed_delay_task_with_context, ClientScheduledTaskHandle};
use crate::{factory::mq_client_instance::MQClientInstance, ClientError};
use parking_lot::Mutex as TableMutex;
use rocketmq_runtime::{ChildServiceContext, ScheduledTaskSnapshot};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{error, warn};
const REQUEST_SCAN_INITIAL_DELAY: Duration = Duration::from_millis(3_000);
const REQUEST_SCAN_INTERVAL: Duration = Duration::from_millis(1_000);
const REQUEST_SCAN_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
type DeadlineKey = (Instant, u64);
struct Entry {
    request: Arc<RequestResponseFuture>,
    deadline: DeadlineKey,
}
#[derive(Default)]
struct Table {
    entries: HashMap<String, Entry>,
    deadlines: BTreeMap<DeadlineKey, String>,
}
pub struct RequestFutureHolder {
    service_context: ChildServiceContext,
    table: TableMutex<Table>,
    deadline_sequence: AtomicU64,
    producer_set: Mutex<HashSet<String>>,
    scheduled_task: Mutex<Option<ClientScheduledTaskHandle>>,
}
struct HolderCleanup(Weak<RequestFutureHolder>);
impl Drop for HolderCleanup {
    fn drop(&mut self) {
        if let Some(holder) = self.0.upgrade() {
            holder.fail_all(ClientError::shutting_down());
        }
    }
}
/// An admitted asynchronous request. Dropping this handle leaves the owned operation active.
/// Cancellation removes its association; it cannot retract a request already written to the Broker.
#[derive(Clone)]
pub struct RequestHandle {
    holder: Weak<RequestFutureHolder>,
    request: Arc<RequestResponseFuture>,
}
impl RequestHandle {
    pub fn correlation_id(&self) -> &str {
        self.request.get_correlation_id()
    }
    pub fn is_finished(&self) -> bool {
        self.request.is_finished()
    }
    pub(crate) fn fail(&self, error: ClientError) -> bool {
        self.holder
            .upgrade()
            .and_then(|holder| holder.remove_if(&self.request))
            .is_some_and(|request| request.complete_send_error(error))
    }
    pub(crate) fn future(&self) -> &Arc<RequestResponseFuture> {
        &self.request
    }
    pub fn cancel(&self) -> bool {
        if let Some(holder) = self.holder.upgrade() {
            return holder
                .remove_if(&self.request)
                .is_some_and(|request| request.complete_error(request_cancelled()));
        }
        false
    }
}
pub(crate) struct RequestRegistration {
    handle: RequestHandle,
    armed: bool,
}
impl RequestRegistration {
    pub(crate) fn handle(&self) -> RequestHandle {
        self.handle.clone()
    }
    pub(crate) fn detach(mut self) -> RequestHandle {
        self.armed = false;
        self.handle.clone()
    }
}
impl Drop for RequestRegistration {
    fn drop(&mut self) {
        if self.armed {
            self.handle.cancel();
        }
    }
}
pub(crate) fn request_cancelled() -> ClientError {
    ClientError::invalid_state("active request", "cancelled")
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestFutureHolderLifecycleProbe {
    pub task_count_before_shutdown: usize,
    pub task_count_after_shutdown: usize,
    pub scheduled_runs: u64,
    pub scheduled_skips: u64,
    pub scheduled_overlaps: u64,
    pub scheduled_failures: u64,
    pub shutdown_elapsed_us: u128,
    pub healthy: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestFutureHolderScanProbe {
    pub pending_requests: usize,
    pub expired_requests: usize,
    pub callbacks: usize,
    pub remaining_requests: usize,
    pub scan_elapsed_us: u128,
}

impl RequestFutureHolder {
    pub fn new(service_context: ChildServiceContext) -> Self {
        Self {
            service_context,
            table: TableMutex::new(Table::default()),
            deadline_sequence: AtomicU64::new(0),
            producer_set: Mutex::new(HashSet::new()),
            scheduled_task: Mutex::new(None),
        }
    }
    pub fn pending_count(&self) -> usize {
        self.table.lock().entries.len()
    }
    pub fn pending_requests(&self) -> Vec<Arc<RequestResponseFuture>> {
        self.table
            .lock()
            .entries
            .values()
            .map(|entry| entry.request.clone())
            .collect()
    }
    pub(crate) fn register(self: &Arc<Self>, request: Arc<RequestResponseFuture>) -> RequestRegistration {
        self.insert(request.get_correlation_id().to_owned(), request.clone());
        RequestRegistration {
            handle: RequestHandle {
                holder: Arc::downgrade(self),
                request,
            },
            armed: true,
        }
    }
    pub(crate) fn track(self: &Arc<Self>, request: RequestResponseFuture) -> crate::ClientResult<RequestHandle> {
        let request = Arc::new(request);
        let id = request.get_correlation_id().to_owned();
        let deadline = (
            request_deadline(&request),
            self.deadline_sequence.fetch_add(1, Ordering::Relaxed),
        );
        let mut table = self.table.lock();
        if table.entries.contains_key(&id) {
            return Err(ClientError::illegal_argument(
                "request correlation id is already tracked",
            ));
        }
        table.deadlines.insert(deadline, id.clone());
        table.entries.insert(
            id,
            Entry {
                request: request.clone(),
                deadline,
            },
        );
        Ok(RequestHandle {
            holder: Arc::downgrade(self),
            request,
        })
    }
    pub(crate) fn handle_if_owned(self: &Arc<Self>, id: &str, owner: &str) -> Option<RequestHandle> {
        self.table
            .lock()
            .entries
            .get(id)
            .filter(|entry| entry.request.owned_by(owner))
            .map(|entry| RequestHandle {
                holder: Arc::downgrade(self),
                request: entry.request.clone(),
            })
    }
    fn insert(&self, correlation_id: String, request: Arc<RequestResponseFuture>) {
        let deadline = (
            request_deadline(&request),
            self.deadline_sequence.fetch_add(1, Ordering::Relaxed),
        );
        let mut table = self.table.lock();
        if let Some(old) = table.entries.remove(&correlation_id) {
            table.deadlines.remove(&old.deadline);
        }
        table.deadlines.insert(deadline, correlation_id.clone());
        table.entries.insert(correlation_id, Entry { request, deadline });
    }
    fn take_locked(table: &mut Table, id: &str) -> Option<Arc<RequestResponseFuture>> {
        let entry = table.entries.remove(id)?;
        table.deadlines.remove(&entry.deadline);
        Some(entry.request)
    }
    fn remove_if(&self, request: &Arc<RequestResponseFuture>) -> Option<Arc<RequestResponseFuture>> {
        let mut table = self.table.lock();
        let id = request.get_correlation_id();
        if table
            .entries
            .get(id)
            .is_some_and(|entry| Arc::ptr_eq(&entry.request, request))
        {
            Self::take_locked(&mut table, id)
        } else {
            None
        }
    }
    pub(crate) fn remove_for_factory(
        &self,
        id: &str,
        factory: &Arc<MQClientInstance>,
    ) -> Option<Arc<RequestResponseFuture>> {
        let mut table = self.table.lock();
        if table
            .entries
            .get(id)
            .is_some_and(|entry| entry.request.accepts_factory(factory))
        {
            Self::take_locked(&mut table, id)
        } else {
            None
        }
    }
    pub(crate) fn fail_owner(&self, owner: &str, error: ClientError) {
        let requests = {
            let mut table = self.table.lock();
            let ids: Vec<_> = table
                .entries
                .iter()
                .filter(|(_, entry)| entry.request.owned_by(owner))
                .map(|(id, _)| id.clone())
                .collect();
            ids.into_iter()
                .filter_map(|id| Self::take_locked(&mut table, &id))
                .collect::<Vec<_>>()
        };
        for request in requests {
            request.complete_error(error.clone());
        }
    }
    fn fail_all(&self, error: ClientError) {
        let requests = {
            let mut table = self.table.lock();
            table.deadlines.clear();
            table
                .entries
                .drain()
                .map(|(_, entry)| entry.request)
                .collect::<Vec<_>>()
        };
        for request in requests {
            request.complete_error(error.clone());
        }
    }
    pub async fn scan_expired_request(&self) {
        let expired = {
            let mut table = self.table.lock();
            let mut expired = Vec::new();
            while let Some((key, _)) = table.deadlines.first_key_value() {
                if key.0 > Instant::now() {
                    break;
                }
                let (_, id) = table.deadlines.pop_first().unwrap();
                if let Some(entry) = table.entries.remove(&id) {
                    expired.push(entry.request);
                }
            }
            expired
        };
        for request in expired {
            request.complete_error(ClientError::request_timeout(request.get_timeout_millis()));
        }
    }
    pub async fn start_scheduled_task(self: &Arc<Self>, producer_id: impl Into<String>) {
        self.start_scheduled_task_with_schedule(producer_id, REQUEST_SCAN_INITIAL_DELAY, REQUEST_SCAN_INTERVAL)
            .await;
    }
    async fn start_scheduled_task_with_schedule(
        self: &Arc<Self>,
        producer_id: impl Into<String>,
        initial_delay: Duration,
        scan_interval: Duration,
    ) {
        let mut scheduled = self.scheduled_task.lock().await;
        self.producer_set.lock().await.insert(producer_id.into());
        if scheduled.as_ref().is_some_and(ClientScheduledTaskHandle::is_running) {
            return;
        }
        let holder = self.clone();
        let cleanup = HolderCleanup(Arc::downgrade(self));
        match schedule_client_fixed_delay_task_with_context(
            &self.service_context,
            "rocketmq-client-request-future-scan",
            initial_delay,
            scan_interval.max(Duration::from_millis(1)),
            move || {
                let _keep_alive = &cleanup;
                let holder = holder.clone();
                async move {
                    holder.scan_expired_request().await;
                }
            },
        ) {
            Ok(handle) => *scheduled = Some(handle),
            Err(error) => error!(%error,"failed to spawn request future scan task"),
        }
    }
    pub async fn shutdown(&self, producer_id: &str) {
        self.fail_owner(producer_id, ClientError::shutting_down());
        let mut scheduled = self.scheduled_task.lock().await;
        let mut producers = self.producer_set.lock().await;
        producers.remove(producer_id);
        let last = producers.is_empty();
        drop(producers);
        if last {
            self.fail_all(ClientError::shutting_down());
            if let Some(task) = scheduled.take() {
                let report = task.shutdown(REQUEST_SCAN_SHUTDOWN_TIMEOUT).await;
                if !report.is_healthy() {
                    warn!(report=%report.to_json(),"request scanner did not stop cleanly");
                }
            }
        }
    }
    pub async fn put_request(&self, correlation_id: String, request: Arc<RequestResponseFuture>) {
        self.insert(correlation_id, request);
    }
    pub async fn remove_request(&self, id: &str) {
        Self::take_locked(&mut self.table.lock(), id);
    }
    pub async fn remove_request_and_get(&self, id: &str) -> Option<Arc<RequestResponseFuture>> {
        Self::take_locked(&mut self.table.lock(), id)
    }
    #[cfg(test)]
    pub(crate) fn fail_request(&self, id: String) {
        let request = Self::take_locked(&mut self.table.lock(), &id);
        if let Some(request) = request {
            request.set_send_request_ok(false);
            request.put_response_message(None);
            request.execute_request_callback();
        }
    }
    pub async fn get_request(&self, id: &str) -> Option<Arc<RequestResponseFuture>> {
        self.table.lock().entries.get(id).map(|entry| entry.request.clone())
    }
    async fn task_count(&self) -> usize {
        self.scheduled_task
            .lock()
            .await
            .as_ref()
            .map(ClientScheduledTaskHandle::task_count)
            .unwrap_or_default()
    }
    async fn schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.scheduled_task
            .lock()
            .await
            .as_ref()
            .map(ClientScheduledTaskHandle::schedule_snapshot)
            .unwrap_or_default()
    }
}
fn request_deadline(request: &RequestResponseFuture) -> Instant {
    request
        .get_begin_timestamp()
        .checked_add(Duration::from_millis(request.get_timeout_millis()))
        .unwrap_or_else(Instant::now)
}

#[doc(hidden)]
pub async fn run_request_future_holder_lifecycle_probe(
    service_context: ChildServiceContext,
) -> RequestFutureHolderLifecycleProbe {
    let holder = Arc::new(RequestFutureHolder::new(service_context));
    holder
        .start_scheduled_task_with_schedule("producer-a", Duration::ZERO, Duration::from_millis(1))
        .await;

    let mut snapshots = holder.schedule_snapshot().await;
    for _ in 0..100 {
        if snapshots
            .iter()
            .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
        snapshots = holder.schedule_snapshot().await;
    }

    let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
    let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
    let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
    let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
    let task_count_before_shutdown = holder.task_count().await;
    let shutdown_started_at = std::time::Instant::now();
    holder.shutdown("producer-a").await;
    let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
    let task_count_after_shutdown = holder.task_count().await;
    let healthy = scheduled_runs > 0
        && scheduled_overlaps == 0
        && scheduled_failures == 0
        && task_count_before_shutdown > 0
        && task_count_after_shutdown == 0;

    RequestFutureHolderLifecycleProbe {
        task_count_before_shutdown,
        task_count_after_shutdown,
        scheduled_runs,
        scheduled_skips,
        scheduled_overlaps,
        scheduled_failures,
        shutdown_elapsed_us,
        healthy,
    }
}

#[doc(hidden)]
pub async fn run_request_future_holder_scan_probe(
    service_context: ChildServiceContext,
    pending_requests: usize,
    expired_percent: usize,
) -> RequestFutureHolderScanProbe {
    let holder = RequestFutureHolder::new(service_context);
    let expired_requests = pending_requests.saturating_mul(expired_percent.min(100)) / 100;
    let callbacks = Arc::new(AtomicUsize::new(0));

    for index in 0..pending_requests {
        let callback_count = Arc::clone(&callbacks);
        let callback = Arc::new(
            move |_response: Option<&dyn rocketmq_model::common::message::MessageTrait>,
                  _error: Option<&crate::ClientError>| {
                callback_count.fetch_add(1, Ordering::Relaxed);
            },
        );
        let timeout_millis = if index < expired_requests { 0 } else { 60_000 };
        let correlation_id = format!("corr-{index}");
        let request = Arc::new(RequestResponseFuture::new(
            correlation_id.clone().into(),
            timeout_millis,
            Some(callback),
        ));
        holder.put_request(correlation_id, request).await;
    }

    if expired_requests > 0 {
        tokio::task::yield_now().await;
    }

    let scan_started_at = Instant::now();
    holder.scan_expired_request().await;
    let scan_elapsed_us = scan_started_at.elapsed().as_micros();
    let remaining_requests = holder.pending_count();

    RequestFutureHolderScanProbe {
        pending_requests,
        expired_requests,
        callbacks: callbacks.load(Ordering::Relaxed),
        remaining_requests,
        scan_elapsed_us,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    use rocketmq_model::common::message::MessageTrait;

    use super::*;

    impl RequestFutureHolder {
        async fn producer_count(&self) -> usize {
            self.producer_set.lock().await.len()
        }

        async fn scheduled_task_active(&self) -> bool {
            self.scheduled_task
                .lock()
                .await
                .as_ref()
                .is_some_and(ClientScheduledTaskHandle::is_running)
        }

        async fn request_count(&self) -> usize {
            self.pending_count()
        }
    }

    #[tokio::test]
    async fn scheduled_task_lifecycle_matches_java_reference_counting() {
        let holder = Arc::new(RequestFutureHolder::new(crate::runtime::test_service_context(
            "request-future-holder-test",
        )));

        holder.start_scheduled_task("producer-a").await;
        assert_eq!(holder.producer_count().await, 1);
        assert!(holder.scheduled_task_active().await);

        holder.start_scheduled_task("producer-a").await;
        assert_eq!(holder.producer_count().await, 1);
        assert!(holder.scheduled_task_active().await);

        holder.start_scheduled_task("producer-b").await;
        assert_eq!(holder.producer_count().await, 2);
        assert!(holder.scheduled_task_active().await);

        holder.shutdown("producer-a").await;
        assert_eq!(holder.producer_count().await, 1);
        assert!(holder.scheduled_task_active().await);

        holder.shutdown("producer-b").await;
        assert_eq!(holder.producer_count().await, 0);
        assert!(!holder.scheduled_task_active().await);
    }

    #[tokio::test]
    async fn scheduled_task_shutdown_interrupts_initial_delay() {
        let holder = Arc::new(RequestFutureHolder::new(crate::runtime::test_service_context(
            "request-future-holder-test",
        )));

        holder.start_scheduled_task("producer-a").await;
        assert!(holder.scheduled_task_active().await);

        let started = Instant::now();
        holder.shutdown("producer-a").await;

        assert!(
            started.elapsed() < Duration::from_millis(500),
            "shutdown should not wait for the full initial scan delay"
        );
        assert!(!holder.scheduled_task_active().await);
    }

    #[tokio::test]
    async fn scan_expired_request_removes_future_and_executes_timeout_callback() {
        let holder = RequestFutureHolder::new(crate::runtime::test_service_context("request-future-holder-test"));
        let callback_called = Arc::new(AtomicBool::new(false));
        let callback_called_inner = Arc::clone(&callback_called);
        let callback = Arc::new(
            move |response: Option<&dyn MessageTrait>, error: Option<&crate::ClientError>| {
                assert!(response.is_none());
                let error = error.expect("timeout scan should pass timeout cause");
                assert!(error.is(&rocketmq_error::CORE_OPERATION_TIMED_OUT));
                callback_called_inner.store(true, Ordering::SeqCst);
            },
        );
        let request = Arc::new(RequestResponseFuture::new("corr-timeout".into(), 0, Some(callback)));

        holder.put_request("corr-timeout".to_string(), request).await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        holder.scan_expired_request().await;

        assert!(holder.get_request("corr-timeout").await.is_none());
        assert!(callback_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn scan_expired_request_skips_removed_lazy_deadline_entry() {
        let holder = RequestFutureHolder::new(crate::runtime::test_service_context("request-future-holder-test"));
        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_inner = Arc::clone(&callback_count);
        let callback = Arc::new(
            move |_response: Option<&dyn MessageTrait>, _error: Option<&crate::ClientError>| {
                callback_count_inner.fetch_add(1, Ordering::SeqCst);
            },
        );
        let request = Arc::new(RequestResponseFuture::new("corr-removed".into(), 0, Some(callback)));

        holder.put_request("corr-removed".to_string(), request).await;
        holder.remove_request("corr-removed").await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        holder.scan_expired_request().await;

        assert_eq!(callback_count.load(Ordering::SeqCst), 0);
        assert_eq!(holder.request_count().await, 0);
    }

    #[tokio::test]
    async fn scan_expired_request_skips_stale_deadline_after_replace() {
        let holder = RequestFutureHolder::new(crate::runtime::test_service_context("request-future-holder-test"));
        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_inner = Arc::clone(&callback_count);
        let old_callback = Arc::new(
            move |_response: Option<&dyn MessageTrait>, _error: Option<&crate::ClientError>| {
                callback_count_inner.fetch_add(1, Ordering::SeqCst);
            },
        );
        let old_request = Arc::new(RequestResponseFuture::new(
            "corr-replaced".into(),
            0,
            Some(old_callback),
        ));
        let new_request = Arc::new(RequestResponseFuture::new("corr-replaced".into(), 60_000, None));

        holder.put_request("corr-replaced".to_string(), old_request).await;
        holder
            .put_request("corr-replaced".to_string(), Arc::clone(&new_request))
            .await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        holder.scan_expired_request().await;

        assert_eq!(callback_count.load(Ordering::SeqCst), 0);
        let stored = holder
            .get_request("corr-replaced")
            .await
            .expect("replacement request should remain");
        assert!(Arc::ptr_eq(&stored, &new_request));
    }

    #[tokio::test]
    async fn duplicate_deadline_entries_execute_timeout_callback_once() {
        let holder = RequestFutureHolder::new(crate::runtime::test_service_context("request-future-holder-test"));
        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_inner = Arc::clone(&callback_count);
        let callback = Arc::new(
            move |_response: Option<&dyn MessageTrait>, _error: Option<&crate::ClientError>| {
                callback_count_inner.fetch_add(1, Ordering::SeqCst);
            },
        );
        let request = Arc::new(RequestResponseFuture::new("corr-duplicate".into(), 0, Some(callback)));

        holder
            .put_request("corr-duplicate".to_string(), Arc::clone(&request))
            .await;
        holder.put_request("corr-duplicate".to_string(), request).await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        holder.scan_expired_request().await;

        assert_eq!(callback_count.load(Ordering::SeqCst), 1);
        assert!(holder.get_request("corr-duplicate").await.is_none());
    }

    #[tokio::test]
    async fn remove_request_and_get_returns_and_removes_future() {
        let holder = RequestFutureHolder::new(crate::runtime::test_service_context("request-future-holder-test"));
        let request = Arc::new(RequestResponseFuture::new("corr-remove".into(), 3_000, None));

        holder.put_request("corr-remove".to_string(), request.clone()).await;

        let removed = holder
            .remove_request_and_get("corr-remove")
            .await
            .expect("request should be removed");

        assert!(Arc::ptr_eq(&removed, &request));
        assert!(holder.get_request("corr-remove").await.is_none());
    }

    #[tokio::test]
    async fn request_future_holder_lifecycle_probe_reports_clean_shutdown() {
        let probe = run_request_future_holder_lifecycle_probe(crate::runtime::test_service_context(
            "request-future-holder-probe",
        ))
        .await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }
    #[tokio::test]
    async fn cancel_releases_both_indexes_without_waiting_for_long_ttl() {
        let holder = Arc::new(RequestFutureHolder::new(crate::runtime::test_service_context(
            "reply-cancel-index",
        )));
        for index in 0..1000 {
            let request = Arc::new(RequestResponseFuture::new(
                format!("cancel-{index}").into(),
                i64::MAX as u64,
                None,
            ));
            let registration = holder.register(request);
            let handle = registration.detach();
            assert!(handle.cancel());
            assert!(!handle.cancel());
            assert!(handle.is_finished());
        }
        assert_eq!(holder.pending_count(), 0);
        assert!(holder.table.lock().deadlines.is_empty());
        let request = Arc::new(RequestResponseFuture::new("dropped".into(), 60000, None));
        let registration = holder.register(request.clone());
        drop(registration);
        assert!(request.is_finished());
        assert_eq!(holder.pending_count(), 0);
    }
    #[tokio::test]
    async fn root_cancellation_completes_pending_callbacks_and_clears_indexes() {
        let root = rocketmq_runtime::RuntimeContext::from_current("reply-root-cancel");
        let holder = Arc::new(RequestFutureHolder::new(root.service_context("reply")));
        holder.start_scheduled_task("owner").await;
        let request = Arc::new(RequestResponseFuture::new("pending".into(), 60000, None));
        let handle = holder.register(request.clone()).detach();
        let report = root.shutdown_tasks(Duration::from_secs(1)).await;
        assert_eq!(report.leaked, 0);
        assert!(handle.is_finished());
        assert_eq!(holder.pending_count(), 0);
        assert!(holder.table.lock().deadlines.is_empty());
    }
    #[tokio::test]
    async fn callback_panic_does_not_stop_other_expired_requests() {
        let holder = Arc::new(RequestFutureHolder::new(crate::runtime::test_service_context(
            "reply-callback-panic",
        )));
        holder
            .put_request(
                "panic".into(),
                Arc::new(RequestResponseFuture::new(
                    "panic".into(),
                    0,
                    Some(Arc::new(|_, _| panic!("callback"))),
                )),
            )
            .await;
        let count = Arc::new(AtomicUsize::new(0));
        let cb_count = count.clone();
        holder
            .put_request(
                "next".into(),
                Arc::new(RequestResponseFuture::new(
                    "next".into(),
                    0,
                    Some(Arc::new(move |_, _| {
                        cb_count.fetch_add(1, Ordering::SeqCst);
                    })),
                )),
            )
            .await;
        holder.scan_expired_request().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(holder.pending_count(), 0);
    }
}
