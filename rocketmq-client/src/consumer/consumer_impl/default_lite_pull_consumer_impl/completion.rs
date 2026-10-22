// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;

/// A fetched batch whose business work has not yet been acknowledged.
/// Dropping it never commits its messages. Retry business work while retaining the
/// batch, or seek/restart after dropping it to replay from the committed offset.
#[must_use = "complete the batch only after business success; dropping it retains the offset barrier"]
pub struct LitePullBatch {
    pub(super) owner: Weak<DefaultLitePullConsumerImpl>,
    pub(super) entry: Weak<AssignmentEntry>,
    pub(super) epoch: u64,
    pub(super) started_at: u64,
    pub(super) hook_state: std::sync::Mutex<Option<BatchHookState>>,
    pub(super) queue: MessageQueue,
    pub(super) process_queue: Arc<ProcessQueue>,
    pub(super) messages: Vec<Arc<MessageExt>>,
}
// The borrowed message slice is rebuilt, while the actual before/after Hook state
// and time remain owned by the business receipt across the caller's await points.
pub(super) struct BatchHookState {
    pub(super) start_time: i64,
    pub(super) track_enabled: bool,
    pub(super) props: std::collections::HashMap<CheetahString, CheetahString>,
    pub(super) trace: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

impl std::fmt::Debug for LitePullBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LitePullBatch")
            .field("queue", &self.queue)
            .field("messages", &self.messages.len())
            .finish_non_exhaustive()
    }
}
pub use crate::hook::consume_message_context::ConsumeBusinessOutcome as LitePullBusinessOutcome;

impl LitePullBatch {
    pub fn messages(&self) -> &[Arc<MessageExt>] {
        &self.messages
    }
    pub fn message_queue(&self) -> &MessageQueue {
        &self.queue
    }
    /// Reports the same explicit outcome for every message. Only successful
    /// messages advance the completion barrier; seek/restart to replay a gap.
    pub async fn report_outcome(self, outcome: LitePullBusinessOutcome) -> ClientResult<()> {
        let owner = self.owner.upgrade().ok_or_else(ClientError::not_started)?;
        owner.report_incomplete_batch(&self, outcome).await
    }
    /// Reports one outcome per original message. Successful later messages cannot
    /// move the offset past an earlier failed/unknown message.
    pub async fn complete_with_outcomes(self, outcomes: Vec<LitePullBusinessOutcome>) -> ClientResult<i64> {
        let owner = self.owner.upgrade().ok_or_else(ClientError::not_started)?;
        owner.complete_batch_with_outcomes(&self, &outcomes).await
    }
    pub async fn fail(self) -> ClientResult<()> {
        self.report_outcome(LitePullBusinessOutcome::Failure).await
    }
    pub async fn unknown(self) -> ClientResult<()> {
        self.report_outcome(LitePullBusinessOutcome::Unknown).await
    }
    /// Acknowledges business success in memory; persist using an explicit commit.
    /// A later batch completing first cannot cross earlier unfinished messages.
    pub async fn complete(self) -> ClientResult<i64> {
        let owner = self.owner.upgrade().ok_or_else(ClientError::not_started)?;
        owner.complete_batch(&self).await
    }
}
impl Drop for LitePullBatch {
    fn drop(&mut self) {
        if let Some(entry) = self.entry.upgrade() {
            entry.finish_batch();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::consumer_impl::process_queue::ProcessQueue;

    async fn fixture() -> (
        Arc<DefaultLitePullConsumerImpl>,
        MessageQueue,
        Arc<OffsetStore>,
        Arc<ProcessQueue>,
    ) {
        let owner = Arc::new(DefaultLitePullConsumerImpl::new(
            crate::runtime::test_client_runtime("lite-pull-business-completion"),
            Arc::new(ClientConfig::default()),
            Arc::new(LitePullConsumerConfig {
                auto_commit: false,
                ..Default::default()
            }),
        ));
        owner.bind_self();
        let queue = MessageQueue::from_parts("topic", "broker", 0);
        owner.assignment_registry.ensure(queue.clone()).await.unwrap();
        owner.assigned_message_queue.put(queue.clone()).await;
        let process_queue = owner.assigned_message_queue.get_process_queue(&queue).await.unwrap();
        let store = Arc::new(OffsetStore::new_test());
        store.update_offset(&queue, 10, false).await;
        owner.set_offset_store(Some(store.clone())).unwrap();
        owner.set_service_state(ServiceState::Running);
        for offset in 10..12 {
            let message = Arc::new(MessageExt {
                queue_offset: offset,
                ..Default::default()
            });
            process_queue.put_message(&[message.clone()]).await;
            owner
                .enqueue_consume_request(LitePullConsumeRequest::new(
                    vec![message],
                    queue.clone(),
                    process_queue.clone(),
                ))
                .unwrap();
        }
        (owner, queue, store, process_queue)
    }

    #[tokio::test]
    async fn later_business_completion_cannot_cross_the_unfinished_earlier_batch() {
        let (owner, queue, store, pending) = fixture().await;
        let first = owner.poll_batch(0).await.unwrap().unwrap();
        let second = owner.poll_batch(0).await.unwrap().unwrap();
        owner.commit_all().await.unwrap();
        assert_eq!(pending.msg_count(), 2);
        assert_eq!(store.read_offset(&queue, ReadOffsetType::ReadFromMemory).await, 10);
        assert_eq!(second.complete().await.unwrap(), 10);
        assert!(owner.commit(HashMap::from([(queue.clone(), 12)]), true).await.is_err());
        owner.commit_all().await.unwrap();
        assert_eq!(store.read_offset(&queue, ReadOffsetType::ReadFromMemory).await, 10);
        assert_eq!(first.complete().await.unwrap(), 12);
        owner.commit_sync(&queue, true).await.unwrap();
        assert_eq!(store.test_persisted_offset(&queue), Some(12));
        assert_eq!(pending.msg_count(), 0);
    }

    #[tokio::test]
    async fn dropped_batches_stay_pending_and_legacy_poll_cannot_bypass_the_barrier() {
        let (owner, queue, store, pending) = fixture().await;
        let first = owner.poll_batch(0).await.unwrap().unwrap();
        drop(first);
        let second = owner.poll_batch(0).await.unwrap().unwrap();
        second.complete().await.unwrap();
        assert!(owner.poll(0).await.is_err());
        owner.commit_all().await.unwrap();
        assert_eq!(pending.msg_count(), 1);
        assert_eq!(store.read_offset(&queue, ReadOffsetType::ReadFromMemory).await, 10);
    }

    #[tokio::test]
    async fn seek_rejects_inflight_business_and_retired_receipt_cannot_ack_a_new_assignment() {
        let (owner, queue, store, pending) = fixture().await;
        let first = owner.poll_batch(0).await.unwrap().unwrap();
        assert!(owner.seek_internal(&queue, 10, false).await.is_err());
        owner.assignment_registry.remove(&queue).await.unwrap();
        owner.assignment_registry.ensure(queue.clone()).await.unwrap();
        assert!(first.complete().await.is_err());
        assert_eq!(pending.msg_count(), 2);
        assert_eq!(store.read_offset(&queue, ReadOffsetType::ReadFromMemory).await, 10);
    }

    #[tokio::test]
    async fn background_permission_failure_wakes_poll_and_preserves_its_typed_error() {
        let (owner, queue, _store, pending) = fixture().await;
        owner.consume_requests.retain(|_| false);
        let waiting = owner.poll_batch(60_000);
        tokio::pin!(waiting);
        tokio::select! {_=&mut waiting=>panic!("unexpected immediate result"),_=tokio::task::yield_now()=>{}}
        *owner.last_pull_error.lock().unwrap() = Some((
            Arc::downgrade(&pending),
            ClientError::broker_operation_failed("PULL_MESSAGE", 16, "fixture denial"),
        ));
        owner.pull_error_notify.notify_one();
        let error = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.failure_kind(), crate::ClientFailureKind::PermissionDenied);
        assert_eq!(error.broker_response_code(), Some(16));
        assert_eq!(owner.assigned_message_queue.get_consume_offset(&queue).await, -1);
    }

    #[tokio::test]
    async fn concurrent_seek_does_not_block_the_pull_response_needed_to_finish_poll() {
        let (owner, queue, _store, pending) = fixture().await;
        owner.consume_requests.retain(|_| false);
        let poll = tokio::spawn({
            let owner = owner.clone();
            async move { owner.poll_batch(5000).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while owner.poll_lock.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let seek = tokio::spawn({
            let owner = owner.clone();
            let queue = queue.clone();
            async move { owner.seek_internal(&queue, 10, false).await }
        });
        tokio::task::yield_now().await;
        let entry = owner.assignment_registry.get(&queue).await.unwrap();
        let lock = entry.operation_lock();
        tokio::time::timeout(Duration::from_secs(1), async {
            let _guard = lock.lock().await;
            let message = Arc::new(MessageExt {
                queue_offset: 10,
                ..Default::default()
            });
            owner
                .enqueue_consume_request(LitePullConsumeRequest::new(vec![message], queue, pending))
                .unwrap();
        })
        .await
        .expect("seek must leave pull-response admission free while poll is waiting");
        let batch = poll.await.unwrap().unwrap().unwrap();
        assert_eq!(batch.messages()[0].queue_offset(), 10);
        assert!(
            seek.await.unwrap().is_err(),
            "seek rejects the newly in-flight business batch"
        );
    }
    #[tokio::test]
    async fn receipt_keeps_before_hook_state_until_explicit_business_result() {
        struct StateHook(Arc<std::sync::atomic::AtomicUsize>);
        impl crate::hook::consume_message_hook::ConsumeMessageHook for StateHook {
            fn hook_name(&self) -> &'static str {
                "receipt-state"
            }
            fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
                context.start_time = 123;
                context.props.insert("marker".into(), "retained".into());
                context.mq_trace_context = Some(Arc::new(42u32));
            }
            fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
                assert_eq!(context.start_time, 123);
                assert_eq!(context.props.get("marker").unwrap(), "retained");
                assert_eq!(
                    context.mq_trace_context.as_ref().unwrap().downcast_ref::<u32>(),
                    Some(&42)
                );
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }
        let (owner, _queue, _store, _pending) = fixture().await;
        let observed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        owner.register_consume_message_hook(Arc::new(StateHook(observed.clone())));
        let batch = owner.poll_batch(10).await.unwrap().unwrap();
        assert_eq!(observed.load(Ordering::Acquire), 0);
        batch.complete().await.unwrap();
        assert_eq!(observed.load(Ordering::Acquire), 1);
    }
}
