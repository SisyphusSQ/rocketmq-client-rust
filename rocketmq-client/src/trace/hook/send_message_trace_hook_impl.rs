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

// Custom fork: exact standard-trace destination and fixed Java fields.
use rocketmq_protocol::protocol::namespace_util::NamespaceUtil;
use rocketmq_runtime::common::time_utils::current_millis;

use crate::hook::send_message_context::SendMessageContext;
use crate::hook::send_message_context::SendMessageTraceSnapshot;
use crate::hook::send_message_hook::SendMessageHook;
use crate::producer::send_status::SendStatus;
use crate::trace::trace_bean::TraceBean;
use crate::trace::trace_context::TraceContext;
use crate::trace::trace_dispatcher::ArcTraceDispatcher;
use crate::trace::trace_type::TraceType;

pub struct SendMessageTraceHookImpl {
    trace_dispatcher: ArcTraceDispatcher,
}

impl SendMessageTraceHookImpl {
    pub fn new(trace_dispatcher: ArcTraceDispatcher) -> Self {
        Self { trace_dispatcher }
    }
}
impl SendMessageHook for SendMessageTraceHookImpl {
    fn hook_name(&self) -> &'static str {
        "SendMessageTraceHook"
    }

    fn send_message_before(&self, context: &Option<SendMessageContext<'_>>) {
        if let Some(context) = context {
            if let Some(prepared) = prepare_context(context, &self.trace_dispatcher) {
                let _ = context.standard_trace_context.set(prepared);
            }
        }
    }

    fn send_message_after(&self, context: &Option<SendMessageContext<'_>>) {
        let Some(context) = context.as_ref() else {
            return;
        };
        let Some(send_result) = context.send_result else {
            return;
        };
        if send_result.region_id.is_none() || !send_result.trace_on {
            return;
        }
        let Some(mut trace_context) = context
            .standard_trace_context
            .get()
            .cloned()
            .or_else(|| prepare_context(context, &self.trace_dispatcher))
        else {
            return;
        };
        let Some(snapshot) = message_trace_snapshot(context) else {
            return;
        };
        if is_trace_topic(&self.trace_dispatcher, snapshot.topic.as_str()) {
            return;
        }
        let now = current_millis() as i64;
        let cost_time = now.wrapping_sub(trace_context.time_stamp) as i32;
        trace_context.cost_time = cost_time;
        trace_context.region_id = send_result
            .region_id
            .as_ref()
            .map(|value| value.clone().into())
            .unwrap_or_default();
        trace_context.is_success = send_result.send_status == SendStatus::SendOk;
        let bean = &mut trace_context.trace_beans.as_mut().unwrap()[0];
        bean.msg_id = send_result.msg_id.clone().unwrap_or_else(|| "null".into());
        bean.offset_msg_id = send_result
            .offset_msg_id
            .as_ref()
            .map(|value| value.clone().into())
            .unwrap_or_else(|| "null".into());
        bean.store_time = trace_context.time_stamp.wrapping_add((cost_time / 2) as i64);

        self.trace_dispatcher.append(&trace_context);
    }
}

fn prepare_context(context: &SendMessageContext<'_>, dispatcher: &ArcTraceDispatcher) -> Option<TraceContext> {
    let snapshot = message_trace_snapshot(context)?;
    if is_trace_topic(dispatcher, snapshot.topic.as_str()) {
        return None;
    }
    let mut trace = TraceContext::new();
    trace.trace_type = Some(TraceType::Pub);
    trace.time_stamp = context.trace_start_time.unwrap_or_else(current_millis) as i64;
    trace.group_name = context
        .producer_group
        .as_ref()
        .map(|group| NamespaceUtil::without_namespace(group.as_str()).into())
        .unwrap_or_else(|| "null".into());
    trace.trace_beans = Some(vec![TraceBean {
        topic: NamespaceUtil::without_namespace(snapshot.topic.as_str()).into(),
        tags: if snapshot.tags_is_null {
            "null".into()
        } else {
            snapshot.tags
        },
        keys_is_null: snapshot.keys_is_null,
        keys: if snapshot.keys_is_null {
            "null".into()
        } else {
            snapshot.keys
        },
        store_host: context.broker_addr.clone().unwrap_or_else(|| "null".into()),
        body_length: snapshot.body_length,
        msg_type: Some(
            context
                .msg_type
                .unwrap_or(rocketmq_model::common::message::message_enum::MessageType::NormalMsg),
        ),
        ..Default::default()
    }]);
    Some(trace)
}

fn message_trace_snapshot(context: &SendMessageContext<'_>) -> Option<SendMessageTraceSnapshot> {
    context
        .message_trace_snapshot
        .clone()
        .or_else(|| context.message().map(SendMessageTraceSnapshot::from_message))
}

fn is_trace_topic(trace_dispatcher: &ArcTraceDispatcher, topic: &str) -> bool {
    trace_dispatcher
        .resolved_trace_topic_name()
        .is_some_and(|trace_topic| topic.starts_with(trace_topic.as_str()))
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::Mutex;

    use rocketmq_model::common::message::message_enum::MessageType;
    use rocketmq_runtime::common::time_utils::current_millis;

    use super::*;
    use crate::base::access_channel::AccessChannel;
    use crate::implementation::communication_mode::CommunicationMode;
    use crate::producer::send_result::SendResult;
    use crate::trace::trace_dispatcher::TraceDispatcher;

    struct CapturingTraceDispatcher {
        trace_topic_name: &'static str,
        contexts: Mutex<Vec<TraceContext>>,
    }

    impl CapturingTraceDispatcher {
        fn new(trace_topic_name: &'static str) -> Self {
            Self {
                trace_topic_name,
                contexts: Mutex::new(Vec::new()),
            }
        }
    }

    impl TraceDispatcher for CapturingTraceDispatcher {
        fn start(&self, _name_srv_addr: &str, _access_channel: AccessChannel) -> crate::ClientResult<()> {
            Ok(())
        }

        fn append(&self, ctx: &dyn Any) -> bool {
            let Some(trace_context) = ctx.downcast_ref::<TraceContext>() else {
                return false;
            };
            self.contexts.lock().expect("capture lock").push(trace_context.clone());
            true
        }

        fn flush(&self) -> crate::ClientResult<()> {
            Ok(())
        }

        fn shutdown(&self) {}

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_mut_any(&mut self) -> &mut dyn Any {
            self
        }

        fn trace_topic_name(&self) -> Option<&str> {
            Some(self.trace_topic_name)
        }
    }

    fn send_result(trace_on: bool) -> SendResult {
        SendResult {
            send_status: SendStatus::SendOk,
            msg_id: Some(CheetahString::from_static_str("UNIQ_ID_001")),
            offset_msg_id: Some("OFFSET_ID_001".to_string()),
            region_id: Some("DefaultRegion".to_string()),
            trace_on,
            ..Default::default()
        }
    }

    fn context<'a>(
        snapshot: SendMessageTraceSnapshot,
        send_result: &'a SendResult,
        trace_start_time: u64,
    ) -> Option<SendMessageContext<'a>> {
        Some(SendMessageContext {
            producer_group: Some(CheetahString::from_static_str("ns%ProducerGroup")),
            message_trace_snapshot: Some(snapshot),
            broker_addr: Some(CheetahString::from_static_str("broker-a:10911")),
            communication_mode: Some(CommunicationMode::Sync),
            send_result: Some(send_result),
            trace_start_time: Some(trace_start_time),
            msg_type: Some(MessageType::NormalMsg),
            ..Default::default()
        })
    }

    #[test]
    fn send_trace_hook_appends_java_pub_context_from_snapshot() {
        let dispatcher = Arc::new(CapturingTraceDispatcher::new("TRACE_TOPIC"));
        let hook = SendMessageTraceHookImpl::new(dispatcher.clone());
        let result = send_result(true);
        let trace_start_time = current_millis();
        let snapshot = SendMessageTraceSnapshot {
            topic: CheetahString::from_static_str("ns%TopicA"),
            tags: CheetahString::from_static_str("tag-a"),
            keys: CheetahString::from_static_str("key-a"),
            body_length: 7,
            ..Default::default()
        };

        hook.send_message_after(&context(snapshot, &result, trace_start_time));

        let contexts = dispatcher.contexts.lock().expect("capture lock");
        assert_eq!(contexts.len(), 1);
        let trace_context = &contexts[0];
        assert_eq!(trace_context.trace_type, Some(TraceType::Pub));
        assert_eq!(trace_context.group_name.as_str(), "ProducerGroup");
        assert_eq!(trace_context.region_id.as_str(), "DefaultRegion");
        assert!(trace_context.is_success);
        assert_eq!(trace_context.time_stamp, trace_start_time as i64);
        assert!(trace_context.cost_time >= 0);

        let bean = &trace_context.trace_beans.as_ref().expect("trace beans")[0];
        assert_eq!(bean.topic.as_str(), "TopicA");
        assert_eq!(bean.tags.as_str(), "tag-a");
        assert_eq!(bean.keys.as_str(), "key-a");
        assert_eq!(bean.store_host.as_str(), "broker-a:10911");
        assert_eq!(bean.body_length, 7);
        assert_eq!(bean.msg_type, Some(MessageType::NormalMsg));
        assert_eq!(bean.msg_id.as_str(), "UNIQ_ID_001");
        assert_eq!(bean.offset_msg_id.as_str(), "OFFSET_ID_001");
        assert!(bean.store_time >= trace_start_time as i64);
    }

    #[test]
    fn send_trace_hook_skips_trace_topic_like_java() {
        let dispatcher = Arc::new(CapturingTraceDispatcher::new("TRACE_TOPIC"));
        let hook = SendMessageTraceHookImpl::new(dispatcher.clone());
        let result = send_result(true);
        let snapshot = SendMessageTraceSnapshot {
            topic: CheetahString::from_static_str("TRACE_TOPIC"),
            tags: CheetahString::new(),
            keys: CheetahString::new(),
            body_length: 0,
            ..Default::default()
        };

        hook.send_message_after(&context(snapshot, &result, current_millis()));

        assert!(dispatcher.contexts.lock().expect("capture lock").is_empty());
    }
}
