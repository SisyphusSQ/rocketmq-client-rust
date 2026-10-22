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

//! Explicit business outcomes for Classic Pull; fetching never acknowledges work.
#![allow(deprecated)]

use crate::consumer::default_mq_pull_consumer::DefaultMQPullConsumer;
use crate::consumer::pull_result::PullResult;
use crate::hook::consume_message_context::{ConsumeBusinessOutcome, ConsumeMessageContext};
use crate::hook::consume_message_hook::ConsumeMessageHook;
use crate::producer::send_result::SendResult;
use crate::{ClientError, ClientResult};
use cheetah_string::CheetahString;
use rocketmq_model::common::message::{message_ext::MessageExt, message_queue::MessageQueue};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// A direct send-back ACK and the Java retry-publish fallback remain distinguishable.
pub enum ClassicSendBackOutcome {
    BrokerAcknowledged,
    RetryPublished {
        primary_error: ClientError,
        result: SendResult,
    },
}

/// Both actual failures remain available through `ClientError::source_ref`.
#[derive(Debug)]
pub struct ClassicSendBackFailure {
    pub primary: ClientError,
    pub fallback: ClientError,
}
impl std::fmt::Display for ClassicSendBackFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Classic Pull direct send-back and retry publish failed")
    }
}
impl std::error::Error for ClassicSendBackFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.fallback)
    }
}

/// Reporting does not update or persist an offset. Call the consumer explicitly after
/// durable business success; partial/unknown results stop at the first unfinished record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassicPullBusinessReceipt {
    pub queue: MessageQueue,
    pub safe_offset: i64,
    pub successful: usize,
    pub failed: usize,
    pub unknown: usize,
}

#[must_use = "report actual business outcomes; dropping a batch never reports success or commits"]
pub struct ClassicPullBatch {
    result: PullResult,
    messages: Vec<Arc<MessageExt>>,
    queue: MessageQueue,
    group: CheetahString,
    namespace: CheetahString,
    hooks: Vec<Arc<dyn ConsumeMessageHook + Send + Sync>>,
    trace: Option<Arc<dyn Any + Send + Sync>>,
    props: HashMap<CheetahString, CheetahString>,
    start_time: i64,
}

impl ClassicPullBatch {
    pub(crate) fn new(consumer: &DefaultMQPullConsumer, queue: MessageQueue, result: PullResult) -> ClientResult<Self> {
        let shared = consumer.shared_consumer()?;
        let hooks = shared.try_impl_()?.classic_consume_hooks();
        let messages: Vec<_> = result
            .msg_found_list()
            .unwrap_or_default()
            .iter()
            .cloned()
            .map(Arc::new)
            .collect();
        let group = shared.consumer_group();
        let namespace = shared.client_config().resolved_namespace().unwrap_or_default();
        let mut context = ConsumeMessageContext {
            consumer_group: group.clone(),
            namespace: namespace.clone(),
            mq: Some(queue.clone()),
            msg_list: &messages,
            broker_name: queue.broker_name().clone(),
            start_time: rocketmq_runtime::common::time_utils::current_millis() as i64,
            track_enabled: messages.iter().any(|m| m.delivery_track_enabled == Some(true)),
            ..Default::default()
        };
        for hook in &hooks {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hook.consume_message_before(&mut context)
            }))
            .is_err()
            {
                tracing::warn!("Classic Pull before hook panicked");
            }
        }
        let trace = context.mq_trace_context.take();
        let props = std::mem::take(&mut context.props);
        let start_time = context.start_time;
        drop(context);
        Ok(Self {
            result,
            messages,
            queue,
            group,
            namespace,
            hooks,
            trace,
            props,
            start_time,
        })
    }

    pub fn pull_result(&self) -> &PullResult {
        &self.result
    }
    pub fn messages(&self) -> &[Arc<MessageExt>] {
        &self.messages
    }

    pub fn complete(self) -> ClientResult<ClassicPullBusinessReceipt> {
        let outcomes = vec![ConsumeBusinessOutcome::Success; self.messages.len()];
        self.finish(outcomes)
    }

    pub fn finish(self, outcomes: Vec<ConsumeBusinessOutcome>) -> ClientResult<ClassicPullBusinessReceipt> {
        if outcomes.len() != self.messages.len() {
            return Err(ClientError::illegal_argument(
                "one business outcome is required per pulled message",
            ));
        }
        let next = i64::try_from(self.result.next_begin_offset()).map_err(ClientError::illegal_argument_source)?;
        let safe_offset = self
            .messages
            .iter()
            .zip(&outcomes)
            .find_map(|(message, outcome)| {
                (*outcome != ConsumeBusinessOutcome::Success).then_some(message.queue_offset)
            })
            .unwrap_or(next);
        let successful = outcomes
            .iter()
            .filter(|v| **v == ConsumeBusinessOutcome::Success)
            .count();
        let failed = outcomes
            .iter()
            .filter(|v| **v == ConsumeBusinessOutcome::Failure)
            .count();
        let unknown = outcomes.len() - successful - failed;
        let success = failed == 0 && unknown == 0;
        let mut context = ConsumeMessageContext {
            consumer_group: self.group,
            namespace: self.namespace,
            mq: Some(self.queue.clone()),
            msg_list: &self.messages,
            broker_name: self.queue.broker_name().clone(),
            start_time: self.start_time,
            end_time: rocketmq_runtime::common::time_utils::current_millis() as i64,
            track_enabled: self.messages.iter().any(|m| m.delivery_track_enabled == Some(true)),
            message_outcomes: Some(outcomes),
            success,
            status: CheetahString::from_static_str(if success {
                "CONSUME_SUCCESS"
            } else if unknown > 0 {
                "UNKNOWN"
            } else {
                "RECONSUME_LATER"
            }),
            mq_trace_context: self.trace,
            props: self.props,
            ..Default::default()
        };
        for hook in &self.hooks {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hook.consume_message_after(&mut context)
            }))
            .is_err()
            {
                tracing::warn!("Classic Pull after hook panicked");
            }
        }
        Ok(ClassicPullBusinessReceipt {
            queue: self.queue,
            safe_offset,
            successful,
            failed,
            unknown,
        })
    }
}
