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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Bounded owner for every user callback invoked by one client API instance.
#[derive(Clone)]
pub(super) struct ClientCallbackExecutor {
    permits: Arc<Semaphore>,
    blocking: Option<rocketmq_runtime::BlockingExecutor>,
    tasks: TaskTracker,
    shutdown: CancellationToken,
}

impl ClientCallbackExecutor {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(limit.max(1))),
            blocking: None,
            tasks: TaskTracker::new(),
            shutdown: CancellationToken::new(),
        }
    }

    pub(super) fn with_blocking(mut self, blocking: Option<rocketmq_runtime::BlockingExecutor>) -> Self {
        self.blocking = blocking;
        self
    }
    pub(super) async fn execute_blocking<F>(&self, callback: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let blocking = self.blocking.clone();
        let _ = self
            .execute(async move {
                if let Some(blocking) = blocking {
                    // Custom: Java falls back to its request owner if a custom
                    // executor refuses submission. Retain the FnOnce until an
                    // executor actually takes it; a panicking callback is never replayed.
                    let callback = Arc::new(parking_lot::Mutex::new(Some(callback)));
                    let submitted = callback.clone();
                    if blocking
                        .spawn_io("client.send.callback", move || {
                            let work = submitted.lock().take();
                            if let Some(work) = work {
                                work();
                            }
                        })
                        .await
                        .is_err()
                    {
                        let work = callback.lock().take();
                        if let Some(work) = work {
                            work();
                        }
                    }
                } else {
                    callback();
                }
            })
            .await;
    }

    pub(super) async fn execute<F, T>(&self, callback: F) -> Option<T>
    where
        F: Future<Output = T>,
    {
        if self.shutdown.is_cancelled() {
            return None;
        }
        let permit = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => return None,
            permit = self.permits.clone().acquire_owned() => permit.ok()?,
        };
        if self.shutdown.is_cancelled() {
            return None;
        }
        let output = self.tasks.track_future(callback).await;
        drop(permit);
        Some(output)
    }

    pub(super) fn close(&self) {
        self.shutdown.cancel();
        self.tasks.close();
    }

    pub(super) async fn shutdown(&self, timeout: Duration) -> bool {
        self.close();
        tokio::time::timeout(timeout, self.tasks.wait()).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use futures::future::join_all;
    use tokio::sync::Semaphore;

    use super::ClientCallbackExecutor;

    #[tokio::test]
    async fn refused_custom_executor_delivers_callback_once_on_request_owner() {
        let root = rocketmq_runtime::RuntimeContext::from_current("refused-callback-executor");
        let custom = root.service_context("custom");
        assert!(custom.task_group().shutdown(Duration::from_secs(1)).await.is_healthy());
        let executor = ClientCallbackExecutor::new(1).with_blocking(Some(custom.metadata_io().clone()));
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        executor
            .execute_blocking(move || {
                seen.fetch_add(1, Ordering::SeqCst);
            })
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(executor.shutdown(Duration::from_secs(1)).await);
        assert!(root.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
    }

    #[tokio::test]
    async fn configured_limit_bounds_concurrent_callbacks() {
        let executor = ClientCallbackExecutor::new(2);
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let callbacks = (0..6).map(|_| {
            let executor = executor.clone();
            let active = active.clone();
            let peak = peak.clone();
            let started = started.clone();
            let release = release.clone();
            async move {
                executor
                    .execute(async move {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(current, Ordering::SeqCst);
                        started.add_permits(1);
                        let permit = release.acquire().await.expect("release semaphore");
                        permit.forget();
                        active.fetch_sub(1, Ordering::SeqCst);
                    })
                    .await
            }
        });
        let joined = tokio::spawn(join_all(callbacks));

        let first_two = started.acquire_many(2).await.expect("two callbacks started");
        first_two.forget();
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        release.add_permits(6);

        let results = joined.await.expect("callback join");
        assert!(results.into_iter().all(|result| result.is_some()));
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn shutdown_rejects_new_callbacks_and_waits_for_active_work() {
        let executor = ClientCallbackExecutor::new(1);
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let callback = {
            let executor = executor.clone();
            let started = started.clone();
            let release = release.clone();
            tokio::spawn(async move {
                executor
                    .execute(async move {
                        started.add_permits(1);
                        let permit = release.acquire().await.expect("release callback");
                        permit.forget();
                    })
                    .await
            })
        };
        let started_callback = started.acquire().await.expect("callback started");
        started_callback.forget();

        executor.close();
        assert!(executor.execute(async {}).await.is_none());
        release.add_permits(1);
        assert!(callback.await.expect("callback task").is_some());
        assert!(executor.shutdown(Duration::from_secs(1)).await);
    }
}
