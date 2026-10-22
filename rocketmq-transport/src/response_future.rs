// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.
//! Native, owned equivalents of Java's low-level remoting callback utilities.

use crate::{
    error::TransportError,
    request_outcome::{
        OutboundRequestContract, OutboundRequestOutcome, OutboundRequestRejection, OutboundRequestStage,
    },
    session_view::SessionView,
};
use parking_lot::{Mutex, RwLock};
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

/// Owns an already acquired permit; explicit release and destruction release it once.
pub struct SemaphoreReleaseOnlyOnce {
    semaphore: Arc<Semaphore>,
    permit: Mutex<Option<OwnedSemaphorePermit>>,
}
impl SemaphoreReleaseOnlyOnce {
    pub fn new(permit: OwnedSemaphorePermit) -> Self {
        Self {
            semaphore: permit.semaphore().clone(),
            permit: Mutex::new(Some(permit)),
        }
    }
    pub fn release(&self) {
        self.permit.lock().take();
    }
    pub fn get_semaphore(&self) -> Arc<Semaphore> {
        self.semaphore.clone()
    }
}

/// A callback borrows its completed response and cannot transfer transport ownership.
pub trait InvokeCallback: Send + Sync {
    fn operation_complete(&self, response: &ResponseFuture);
}
impl<F: Fn(&ResponseFuture) + Send + Sync> InvokeCallback for F {
    fn operation_complete(&self, response: &ResponseFuture) {
        self(response);
    }
}

/// Preserves typed non-response outcomes without converting a refusal into success.
#[derive(Debug, Clone)]
pub enum ResponseFailure {
    Transport(TransportError),
    Rejected(OutboundRequestRejection),
    Contract(OutboundRequestContract),
}
impl fmt::Display for ResponseFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => fmt::Display::fmt(error, f),
            Self::Rejected(value) => write!(
                f,
                "remoting request rejected: {:?} at {:?}",
                value.reason(),
                value.stage()
            ),
            Self::Contract(value) => write!(
                f,
                "remoting request contract: {:?} at {:?}",
                value.reason(),
                value.stage()
            ),
        }
    }
}
impl std::error::Error for ResponseFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            _ => None,
        }
    }
}

/// Response state with Java's separate assignment, completion, callback and release operations.
/// Waiting is asynchronous; no OS thread, detached task, runtime or socket is created.
pub struct ResponseFuture {
    opaque: i32,
    process_channel: RwLock<Option<SessionView>>,
    timeout_millis: i64,
    begin_timestamp: u64,
    started: Instant,
    callback: Option<Arc<dyn InvokeCallback>>,
    callback_executed: AtomicBool,
    once: Option<Arc<SemaphoreReleaseOnlyOnce>>,
    completed: AtomicBool,
    notify: Notify,
    response: Mutex<Option<RemotingCommand>>,
    send_request_ok: AtomicBool,
    cause: RwLock<Option<Arc<dyn std::error::Error + Send + Sync>>>,
}
impl ResponseFuture {
    pub fn new(
        process_channel: Option<SessionView>,
        opaque: i32,
        timeout_millis: i64,
        callback: Option<Arc<dyn InvokeCallback>>,
        once: Option<Arc<SemaphoreReleaseOnlyOnce>>,
    ) -> Self {
        Self {
            opaque,
            process_channel: RwLock::new(process_channel),
            timeout_millis,
            begin_timestamp: rocketmq_runtime::common::time_utils::current_millis(),
            started: Instant::now(),
            callback,
            callback_executed: AtomicBool::new(false),
            once,
            completed: AtomicBool::new(false),
            notify: Notify::new(),
            response: Mutex::new(None),
            send_request_ok: AtomicBool::new(true),
            cause: RwLock::new(None),
        }
    }
    pub fn execute_invoke_callback(&self) {
        if let Some(callback) = &self.callback {
            if self
                .callback_executed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                callback.operation_complete(self);
            }
        }
    }
    pub fn release(&self) {
        if let Some(once) = &self.once {
            once.release();
        }
    }
    pub fn is_timeout(&self) -> bool {
        self.timeout_millis < 0 || self.started.elapsed().as_millis() > self.timeout_millis as u128
    }
    pub async fn wait_response(&self, timeout_millis: u64) -> Option<RemotingCommand> {
        let wait = async {
            loop {
                let notified = self.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.completed.load(Ordering::Acquire) {
                    break;
                }
                notified.await;
            }
        };
        let _ = tokio::time::timeout(Duration::from_millis(timeout_millis), wait).await;
        self.get_response_command()
    }
    pub fn put_response(&self, response: Option<RemotingCommand>) {
        self.set_response_command(response);
        self.completed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
    pub fn get_begin_timestamp(&self) -> u64 {
        self.begin_timestamp
    }
    pub fn get_timeout_millis(&self) -> i64 {
        self.timeout_millis
    }
    pub fn get_opaque(&self) -> i32 {
        self.opaque
    }
    pub fn is_send_request_ok(&self) -> bool {
        self.send_request_ok.load(Ordering::Acquire)
    }
    pub fn set_send_request_ok(&self, value: bool) {
        self.send_request_ok.store(value, Ordering::Release);
    }
    pub fn get_invoke_callback(&self) -> Option<Arc<dyn InvokeCallback>> {
        self.callback.clone()
    }
    pub fn get_cause(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        self.cause.read().clone()
    }
    pub fn set_cause(&self, value: Option<Arc<dyn std::error::Error + Send + Sync>>) {
        *self.cause.write() = value;
    }
    pub fn get_response_command(&self) -> Option<RemotingCommand> {
        self.response.lock().clone()
    }
    pub fn set_response_command(&self, value: Option<RemotingCommand>) {
        *self.response.lock() = value;
    }
    pub fn get_process_channel(&self) -> Option<SessionView> {
        self.process_channel.read().clone()
    }
    pub(crate) fn set_process_channel(&self, value: SessionView) {
        *self.process_channel.write() = Some(value);
    }
    pub(crate) fn complete(&self, result: &Result<OutboundRequestOutcome, TransportError>) {
        match result {
            Ok(OutboundRequestOutcome::Response(response)) => self.put_response(Some(response.clone())),
            other => {
                let (failure, stage) = match other {
                    Err(error) => (ResponseFailure::Transport(error.clone()), error.request_stage()),
                    Ok(OutboundRequestOutcome::Rejected(value)) => {
                        (ResponseFailure::Rejected(value.clone()), Some(value.stage()))
                    }
                    Ok(OutboundRequestOutcome::Contract(value)) => {
                        (ResponseFailure::Contract(value.clone()), Some(value.stage()))
                    }
                    _ => unreachable!(),
                };
                self.set_send_request_ok(matches!(
                    stage,
                    Some(OutboundRequestStage::AwaitingResponse | OutboundRequestStage::ResponseReceived)
                ));
                self.set_cause(Some(Arc::new(failure)));
                self.put_response(None);
            }
        }
    }
}
impl fmt::Debug for ResponseFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Java's full toString includes message bodies; native diagnostics expose presence only.
        f.debug_struct("ResponseFuture")
            .field("opaque", &self.opaque)
            .field("timeout_millis", &self.timeout_millis)
            .field("begin_timestamp", &self.begin_timestamp)
            .field("send_request_ok", &self.is_send_request_ok())
            .field("response_present", &self.response.lock().is_some())
            .field("cause_present", &self.cause.read().is_some())
            .finish()
    }
}


