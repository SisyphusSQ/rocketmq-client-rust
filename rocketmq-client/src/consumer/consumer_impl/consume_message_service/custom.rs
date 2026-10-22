// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.
//! Object-safe ownership adapter for the public native consumption-service SPI.
use super::*;
use futures::future::BoxFuture;
use futures::FutureExt;

trait ServiceOperations: Send + Sync {
    fn start(&self);
    fn shutdown(&self, millis: u64) -> BoxFuture<'_, ()>;
    fn update_core_pool_size(&self, size: usize);
    fn inc_core_pool_size(&self);
    fn dec_core_pool_size(&self);
    fn get_core_pool_size(&self) -> usize;
    fn consume_message_directly(
        &self,
        message: MessageExt,
        broker: Option<CheetahString>,
    ) -> BoxFuture<'_, ConsumeMessageDirectlyResult>;
    fn submit_consume_request(
        &self,
        messages: Vec<Arc<MessageExt>>,
        process: Arc<ProcessQueue>,
        queue: MessageQueue,
        dispatch: bool,
    ) -> BoxFuture<'_, ()>;
}
struct Adapter<S>(Arc<S>);
impl<S: ConsumeMessageServiceTrait + Send + Sync + 'static> ServiceOperations for Adapter<S> {
    fn start(&self) {
        self.0.start(self.0.clone());
    }
    fn shutdown(&self, millis: u64) -> BoxFuture<'_, ()> {
        Box::pin(self.0.shutdown(millis))
    }
    fn update_core_pool_size(&self, size: usize) {
        self.0.update_core_pool_size(size);
    }
    fn inc_core_pool_size(&self) {
        self.0.inc_core_pool_size();
    }
    fn dec_core_pool_size(&self) {
        self.0.dec_core_pool_size();
    }
    fn get_core_pool_size(&self) -> usize {
        self.0.get_core_pool_size()
    }
    fn consume_message_directly(
        &self,
        message: MessageExt,
        broker: Option<CheetahString>,
    ) -> BoxFuture<'_, ConsumeMessageDirectlyResult> {
        Box::pin(self.0.consume_message_directly(message, broker))
    }
    fn submit_consume_request(
        &self,
        messages: Vec<Arc<MessageExt>>,
        process: Arc<ProcessQueue>,
        queue: MessageQueue,
        dispatch: bool,
    ) -> BoxFuture<'_, ()> {
        Box::pin(
            self.0
                .submit_consume_request(self.0.clone(), messages, process, queue, dispatch),
        )
    }
}

/// Shared service owner. The implementation owns its business work, offset
/// coordination and any tasks in an application-injected ChildServiceContext.
/// The SDK invokes the same implementation for lifecycle, pool controls, actual
/// delivery and direct-consume diagnostics; it never creates a hidden runtime.
#[derive(Clone)]
pub struct ConsumeMessageServiceHandle {
    operations: Arc<dyn ServiceOperations>,
    context: rocketmq_runtime::ChildServiceContext,
    shutdown_report: Arc<parking_lot::Mutex<Option<Arc<rocketmq_runtime::ShutdownReport>>>>,
}
impl ConsumeMessageServiceHandle {
    /// The service must use this same application-owned scope for background work.
    pub fn new<S: ConsumeMessageServiceTrait + Send + Sync + 'static>(
        context: rocketmq_runtime::ChildServiceContext,
        service: Arc<S>,
    ) -> Self {
        Self {
            operations: Arc::new(Adapter(service)),
            context,
            shutdown_report: Arc::new(parking_lot::Mutex::new(None)),
        }
    }
    pub fn start(&self) -> crate::ClientResult<()> {
        if self.context.task_group().lifecycle_state() != rocketmq_runtime::TaskGroupLifecycleState::Open {
            return Err(crate::ClientError::invalid_state(
                "open custom consumption service scope",
                "closed",
            ));
        }
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.operations.start())).map_err(|_| {
            crate::ClientError::response_process_failed("consume_service.start", "custom service panicked")
        })
    }
    /// Bounded business shutdown and the declared scope's actual completion report.
    pub async fn shutdown(&self, millis: u64) -> Arc<rocketmq_runtime::ShutdownReport> {
        let deadline = rocketmq_runtime::ShutdownDeadline::after(std::time::Duration::from_millis(millis));
        // Construct and poll inside the unwind boundary: user code may panic
        // before returning its future. Cleanup of the declared scope must run.
        let business = tokio::time::timeout(
            deadline.remaining(),
            std::panic::AssertUnwindSafe(async { self.operations.shutdown(millis).await }).catch_unwind(),
        )
        .await;
        let mut report = self.context.task_group().shutdown_until(deadline).await;
        match business {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                report.failed += 1;
                report.panicked += 1;
                report.annotations.push(rocketmq_runtime::ShutdownAnnotation::new(
                    "custom service shutdown panicked; declared scope was reclaimed",
                ));
            }
            Err(_) => {
                report.timed_out += 1;
                report.failed += 1;
                report.annotations.push(rocketmq_runtime::ShutdownAnnotation::new(
                    "custom service shutdown future exceeded its declared deadline",
                ));
            }
        }
        let report = Arc::new(report);
        *self.shutdown_report.lock() = Some(report.clone());
        report
    }
    pub fn last_shutdown_report(&self) -> Option<Arc<rocketmq_runtime::ShutdownReport>> {
        self.shutdown_report.lock().clone()
    }
    pub fn update_core_pool_size(&self, size: usize) {
        self.operations.update_core_pool_size(size);
    }
    pub fn inc_core_pool_size(&self) {
        self.operations.inc_core_pool_size();
    }
    pub fn dec_core_pool_size(&self) {
        self.operations.dec_core_pool_size();
    }
    pub fn get_core_pool_size(&self) -> usize {
        self.operations.get_core_pool_size()
    }
    pub async fn consume_message_directly(
        &self,
        message: MessageExt,
        broker: Option<CheetahString>,
    ) -> ConsumeMessageDirectlyResult {
        let stop = self.context.task_group().cancellation_token();
        tokio::select! { biased;
            _=stop.cancelled()=>ConsumeMessageDirectlyResult::new(false,true,CMResult::CRThrowException,"custom service stopped".into(),0),
            result=self.operations.consume_message_directly(message, broker)=>result,
        }
    }
    pub async fn submit_consume_request(
        &self,
        messages: Vec<Arc<MessageExt>>,
        process: Arc<ProcessQueue>,
        queue: MessageQueue,
        dispatch: bool,
    ) {
        let stop = self.context.task_group().cancellation_token();
        tokio::select! { biased;
            _=stop.cancelled()=>{},
            _=self.operations.submit_consume_request(messages, process, queue, dispatch)=>{},
        }
    }
}
