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

use std::collections::HashMap;
use std::sync::Arc;

use crate::ClientError;
use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_enum::MessageType;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::MessageTrait;

use crate::implementation::communication_mode::CommunicationMode;
use crate::producer::send_result::SendResult;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SendMessageTraceSnapshot {
    pub topic: CheetahString,
    pub tags: CheetahString,
    pub keys: CheetahString,
    pub body_length: i32,
    // Custom fork: missing properties encode Java's literal null, distinct from an explicit empty string.
    pub tags_is_null: bool,
    pub keys_is_null: bool,
}

impl SendMessageTraceSnapshot {
    pub(crate) fn owned_message<M: MessageTrait>(
        message: &M,
    ) -> Arc<rocketmq_model::common::message::message_single::Message> {
        use rocketmq_model::common::message::{message_accessor::MessageAccessor, message_single::Message};
        let mut copy = message
            .as_any()
            .downcast_ref::<Message>()
            .cloned()
            .unwrap_or_else(|| MessageAccessor::clone_message(message));
        if let Some(transaction) = message.transaction_id() {
            copy.set_transaction_id(transaction.clone());
        }
        if let Some(compressed) = message.get_compressed_body() {
            copy.set_compressed_body_mut(compressed.clone());
        }
        Arc::new(copy)
    }

    pub fn from_message(message: &(dyn MessageTrait + Send + Sync)) -> Self {
        let tags = message.tags();
        let keys = message.get_keys();
        Self {
            topic: message.topic().clone(),
            tags_is_null: tags.is_none(),
            keys_is_null: keys.is_none(),
            tags: tags.unwrap_or_default(),
            keys: keys.unwrap_or_default(),
            // Java calls the before Hook after compressing the outbound body.
            body_length: message
                .get_compressed_body()
                .or_else(|| message.get_body())
                .map_or(0, |body| body.len() as i32),
        }
    }
}

pub struct SendMessageContext<'a> {
    pub producer: Option<Arc<crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl>>,
    pub producer_group: Option<CheetahString>,
    pub message: Option<&'a (dyn MessageTrait + Send + Sync)>,
    /// Custom: owned protocol fields for asynchronous and readonly Hook adapters.
    pub message_snapshot: Option<Arc<rocketmq_model::common::message::message_single::Message>>,
    pub message_trace_snapshot: Option<SendMessageTraceSnapshot>,
    pub mq: Option<&'a MessageQueue>,
    pub broker_addr: Option<CheetahString>,
    pub born_host: Option<CheetahString>,
    pub communication_mode: Option<CommunicationMode>,
    pub send_result: Option<&'a SendResult>,
    pub exception: Option<Arc<ClientError>>,
    pub mq_trace_context: Option<Arc<Box<dyn std::any::Any + Send + Sync>>>,
    /// Custom fork: typed before/after state writable through the immutable legacy Hook ABI.
    pub standard_trace_context: std::sync::OnceLock<crate::trace::trace_context::TraceContext>,
    pub trace_start_time: Option<u64>,
    pub props: HashMap<CheetahString, CheetahString>,
    pub msg_type: Option<MessageType>,
    pub namespace: Option<CheetahString>,
}

// Custom: Java initializes the message type to Normal_Msg, even for a user-created context.
impl Default for SendMessageContext<'_> {
    fn default() -> Self {
        Self {
            producer: None,
            producer_group: None,
            message: None,
            message_snapshot: None,
            message_trace_snapshot: None,
            mq: None,
            broker_addr: None,
            born_host: None,
            communication_mode: None,
            send_result: None,
            exception: None,
            mq_trace_context: None,
            standard_trace_context: Default::default(),
            trace_start_time: None,
            props: HashMap::new(),
            msg_type: Some(MessageType::NormalMsg),
            namespace: None,
        }
    }
}

impl SendMessageContext<'_> {
    /// Actual borrowed value when available, otherwise an owned protocol snapshot.
    pub fn message(&self) -> Option<&(dyn MessageTrait + Send + Sync)> {
        self.message.or_else(|| {
            self.message_snapshot
                .as_deref()
                .map(|message| message as &(dyn MessageTrait + Send + Sync))
        })
    }
}
