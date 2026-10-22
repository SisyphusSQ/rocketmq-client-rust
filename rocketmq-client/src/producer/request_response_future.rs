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

// Custom fork: one terminal completion, durable notification and callbacks outside locks.
use crate::producer::request_callback::RequestCallbackFn;
use crate::{factory::mq_client_instance::MQClientInstance, ClientError};
use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_model::common::message::message_single::Message;
use rocketmq_model::common::message::MessageTrait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

type ResponseMessage = Box<dyn MessageTrait + Send>;
type RequestCause = Arc<ClientError>;
#[derive(Default)]
struct Completion {
    finished: bool,
    response: Option<ResponseMessage>,
    callback_response: Option<ResponseMessage>,
    cause: Option<RequestCause>,
}
pub struct RequestResponseFuture {
    correlation_id: CheetahString,
    request_callback: Option<RequestCallbackFn>,
    begin_timestamp: Instant,
    request_msg: Option<Message>,
    timeout_millis: i64,
    notify: Arc<Notify>,
    completion: Mutex<Completion>,
    send_request_ok: AtomicBool,
    callback_executed: AtomicBool,
    owner: Option<(String, Weak<MQClientInstance>)>,
}
impl RequestResponseFuture {
    pub fn new(correlation_id: CheetahString, timeout_millis: u64, callback: Option<RequestCallbackFn>) -> Self {
        Self::new_signed(
            correlation_id,
            i64::try_from(timeout_millis).unwrap_or(i64::MAX),
            callback,
        )
    }
    pub fn new_signed(
        correlation_id: CheetahString,
        timeout_millis: i64,
        request_callback: Option<RequestCallbackFn>,
    ) -> Self {
        Self {
            correlation_id,
            request_callback,
            begin_timestamp: Instant::now(),
            request_msg: None,
            timeout_millis,
            notify: Arc::new(Notify::new()),
            completion: Mutex::new(Completion::default()),
            send_request_ok: AtomicBool::new(true),
            callback_executed: AtomicBool::new(false),
            owner: None,
        }
    }
    pub(crate) fn set_owner(&mut self, owner: String, factory: &Arc<MQClientInstance>) {
        self.owner = Some((owner, Arc::downgrade(factory)));
    }
    pub(crate) fn owned_by(&self, owner: &str) -> bool {
        self.owner.as_ref().is_some_and(|(id, _)| id == owner)
    }
    pub(crate) fn accepts_factory(&self, factory: &Arc<MQClientInstance>) -> bool {
        self.owner
            .as_ref()
            .is_none_or(|(_, owner)| owner.upgrade().is_some_and(|owner| Arc::ptr_eq(&owner, factory)))
    }
    pub fn is_finished(&self) -> bool {
        self.completion.lock().finished
    }
    pub fn execute_request_callback(&self) {
        let Some(callback) = &self.request_callback else {
            return;
        };
        // Callback owns a snapshot independent of the waiter's owned response extraction.
        let (response, cause) = {
            let mut state = self.completion.lock();
            if !state.finished || self.callback_executed.swap(true, Ordering::AcqRel) {
                return;
            }
            (state.callback_response.take(), state.cause.clone())
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if self.send_request_ok.load(Ordering::Acquire) && cause.is_none() {
                callback(response.as_deref().map(|m| m as &dyn MessageTrait), None);
            } else {
                callback(None, cause.as_deref());
            }
        }));
        if let Err(payload) = result {
            // User callback panic must not kill the shared scanner or callback receiver.
            std::mem::forget(payload);
            tracing::warn!("request/reply user callback panicked");
        }
    }
    pub fn on_success(&self) {
        self.execute_request_callback();
    }
    pub fn is_timeout(&self) -> bool {
        self.timeout_millis < 0 || self.begin_timestamp.elapsed() > Duration::from_millis(self.get_timeout_millis())
    }
    pub async fn wait_response_message(&self, timeout: Duration) -> Option<ResponseMessage> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_finished() {
                return self.get_response_msg();
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                self.complete_error(ClientError::request_timeout(
                    timeout.as_millis().min(u64::MAX as u128) as u64
                ));
                return self.get_response_msg();
            }
        }
    }
    pub fn put_response_message(&self, response: Option<ResponseMessage>) {
        let mut state = self.completion.lock();
        if state.finished {
            return;
        }
        if self.request_callback.is_some() {
            state.callback_response = response.as_deref().map(callback_snapshot);
        }
        state.response = response;
        state.finished = true;
        drop(state);
        self.notify.notify_waiters();
    }
    pub(crate) fn complete_error(&self, cause: ClientError) -> bool {
        self.complete_failure(cause, false)
    }
    pub(crate) fn complete_send_error(&self, cause: ClientError) -> bool {
        self.complete_failure(cause, true)
    }
    fn complete_failure(&self, cause: ClientError, send_failed: bool) -> bool {
        let mut state = self.completion.lock();
        if state.finished {
            return false;
        }
        // Reply timeout/cancellation cannot rewrite an observed send ACK as send failure.
        if send_failed {
            self.send_request_ok.store(false, Ordering::Release);
        }
        state.cause = Some(Arc::new(cause));
        state.finished = true;
        drop(state);
        self.notify.notify_waiters();
        self.execute_request_callback();
        true
    }
    pub fn get_correlation_id(&self) -> &str {
        &self.correlation_id
    }
    pub fn get_timeout_millis(&self) -> u64 {
        self.timeout_millis.max(0) as u64
    }
    pub fn timeout_millis_signed(&self) -> i64 {
        self.timeout_millis
    }
    pub fn set_timeout_millis(&mut self, timeout: u64) {
        self.timeout_millis = timeout.min(i64::MAX as u64) as i64;
    }
    pub fn set_timeout_millis_signed(&mut self, timeout: i64) {
        self.timeout_millis = timeout;
    }
    pub fn get_request_callback(&self) -> Option<&RequestCallbackFn> {
        self.request_callback.as_ref()
    }
    pub fn get_begin_timestamp(&self) -> Instant {
        self.begin_timestamp
    }
    pub fn get_notify(&self) -> Arc<Notify> {
        self.notify.clone()
    }
    pub fn set_notify(&mut self, notify: Arc<Notify>) {
        self.notify = notify;
    }
    pub fn get_response_msg(&self) -> Option<ResponseMessage> {
        self.completion.lock().response.take()
    }
    pub fn set_response_msg(&self, message: ResponseMessage) {
        self.put_response_message(Some(message));
    }
    pub async fn is_send_request_ok(&self) -> bool {
        self.send_request_ok.load(Ordering::Acquire)
    }
    pub fn set_send_request_ok(&self, ok: bool) {
        self.send_request_ok.store(ok, Ordering::Release);
    }
    pub fn get_request_msg(&self) -> Option<&Message> {
        self.request_msg.as_ref()
    }
    pub fn get_cause(&self) -> Option<RequestCause> {
        self.completion.lock().cause.clone()
    }
    pub fn set_cause(&self, cause: ClientError) {
        let mut state = self.completion.lock();
        if !state.finished {
            state.cause = Some(Arc::new(cause));
        }
    }
}

fn callback_snapshot(message: &(dyn MessageTrait + Send)) -> ResponseMessage {
    if let Some(message) = message
        .as_any()
        .downcast_ref::<rocketmq_model::common::message::message_ext::MessageExt>()
    {
        return Box::new(message.clone());
    }
    if let Some(message) = message.as_any().downcast_ref::<Message>() {
        return Box::new(message.clone());
    }
    // Custom trait implementations normalize the same public fields as MessageTrait::into_message.
    let mut value = Message::default();
    value.set_topic(message.topic().clone());
    value.set_flag(message.get_flag());
    value.set_body(message.get_body().cloned());
    value.set_properties(message.get_properties().clone());
    if let Some(id) = message.transaction_id() {
        value.set_transaction_id(id.clone());
    }
    Box::new(value)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use bytes::Bytes;

    use super::*;

    fn response_message() -> Box<dyn MessageTrait + Send> {
        Box::new(
            Message::builder()
                .topic("reply_topic")
                .body(Bytes::from_static(b"ok"))
                .build_unchecked(),
        )
    }

    #[tokio::test]
    async fn default_send_request_ok_matches_java() {
        let future = RequestResponseFuture::new("corr".into(), 3_000, None);
        assert!(future.is_send_request_ok().await);

        future.set_send_request_ok(false);
        assert!(!future.is_send_request_ok().await);

        future.set_send_request_ok(true);
        assert!(future.is_send_request_ok().await);
    }

    #[test]
    fn success_callback_accepts_missing_response_without_panic() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = Arc::clone(&calls);
        let callback: RequestCallbackFn = Arc::new(move |response, error| {
            assert!(response.is_none());
            assert!(error.is_none());
            calls_inner.fetch_add(1, Ordering::SeqCst);
        });

        let future = RequestResponseFuture::new("corr".into(), 3_000, Some(callback));
        future.put_response_message(None);
        future.execute_request_callback();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn callback_can_read_response_more_than_once_without_consuming_it() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = Arc::clone(&calls);
        let callback: RequestCallbackFn = Arc::new(move |response, error| {
            assert!(response.is_some());
            assert!(error.is_none());
            calls_inner.fetch_add(1, Ordering::SeqCst);
        });

        let future = RequestResponseFuture::new("corr".into(), 3_000, Some(callback));
        future.put_response_message(Some(response_message()));

        future.on_success();
        future.execute_request_callback();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(future.get_response_msg().is_some());
        assert!(future.get_response_msg().is_none());
    }

    #[test]
    fn failure_callback_passes_none_when_cause_is_absent_like_java_null() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = Arc::clone(&calls);
        let callback: RequestCallbackFn = Arc::new(move |response, error| {
            assert!(response.is_none());
            assert!(error.is_none());
            calls_inner.fetch_add(1, Ordering::SeqCst);
        });

        let future = RequestResponseFuture::new("corr".into(), 3_000, Some(callback));
        future.set_send_request_ok(false);
        future.put_response_message(None);
        future.execute_request_callback();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn reply_before_wait_is_durable_and_returns_immediately() {
        let request = RequestResponseFuture::new("early".into(), 1000, None);
        request.put_response_message(Some(response_message()));
        let response = tokio::time::timeout(
            Duration::from_millis(50),
            request.wait_response_message(Duration::from_secs(1)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.topic().as_str(), "reply_topic");
    }
    #[test]
    fn terminal_races_and_reentrant_callback_complete_once() {
        let count = Arc::new(AtomicUsize::new(0));
        let slot = Arc::new(std::sync::OnceLock::<std::sync::Weak<RequestResponseFuture>>::new());
        let callback: RequestCallbackFn = {
            let count = count.clone();
            let slot = slot.clone();
            Arc::new(move |_, _| {
                let request = slot.get().unwrap().upgrade().unwrap();
                assert!(request.is_finished());
                let _ = request.get_cause();
                count.fetch_add(1, Ordering::SeqCst);
            })
        };
        let request = Arc::new(RequestResponseFuture::new("race".into(), 1000, Some(callback)));
        assert!(slot.set(Arc::downgrade(&request)).is_ok());
        std::thread::scope(|scope| {
            for index in 0..24 {
                let request = request.clone();
                scope.spawn(move || {
                    if index % 2 == 0 {
                        request.put_response_message(Some(response_message()));
                        request.on_success();
                    } else {
                        request.complete_error(ClientError::timeout("race", 1000));
                    }
                });
            }
        });
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn signed_timeout_and_notify_adapter_preserve_java_bean_values() {
        let mut request = RequestResponseFuture::new_signed("negative".into(), -1, None);
        assert_eq!(request.timeout_millis_signed(), -1);
        assert!(request.is_timeout());
        request.set_timeout_millis_signed(i64::MAX);
        assert!(!request.is_timeout());
        let notify = Arc::new(Notify::new());
        request.set_notify(notify.clone());
        assert!(Arc::ptr_eq(&notify, &request.get_notify()));
        assert!(request.get_request_msg().is_none());
    }
    #[tokio::test]
    async fn reply_timeout_preserves_send_status_and_java_local_code() {
        let request = RequestResponseFuture::new("timeout-code".into(), 1, None);
        request.set_send_request_ok(true);
        assert!(request.wait_response_message(Duration::ZERO).await.is_none());
        assert!(request.is_send_request_ok().await);
        let error = request.get_cause().unwrap();
        assert_eq!(error.broker_response_code(), None);
        assert_eq!(
            crate::RequestTimeoutException::from_client_error(&error)
                .unwrap()
                .response_code(),
            10006
        );
        let failed = RequestResponseFuture::new("failed".into(), 1, None);
        failed.complete_send_error(ClientError::shutting_down());
        assert!(!failed.is_send_request_ok().await);
    }
    #[tokio::test]
    async fn callback_snapshot_and_waiter_keep_the_same_native_reply_metadata() {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let callback: RequestCallbackFn = Arc::new(move |message, error| {
            assert!(error.is_none());
            let reply = message
                .unwrap()
                .as_any()
                .downcast_ref::<rocketmq_model::common::message::message_ext::MessageExt>()
                .unwrap();
            assert_eq!(reply.queue_id(), 7);
            count.fetch_add(1, Ordering::SeqCst);
        });
        let future = RequestResponseFuture::new("snapshot".into(), 1000, Some(callback));
        let mut reply = rocketmq_model::common::message::message_ext::MessageExt::default();
        reply.set_queue_id(7);
        future.put_response_message(Some(Box::new(reply)));
        assert_eq!(
            future
                .wait_response_message(Duration::ZERO)
                .await
                .unwrap()
                .as_any()
                .downcast_ref::<rocketmq_model::common::message::message_ext::MessageExt>()
                .unwrap()
                .queue_id(),
            7
        );
        future.execute_request_callback();
        future.execute_request_callback();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
