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

// Custom fork: native async MQAdmin shared by all producer/consumer facades.
use crate::base::client_config::ClientConfig;
use crate::implementation::mq_admin_impl::MQAdminImpl;
use crate::{ClientResult, MQAdminQueryOptions, MQAdminQueryReport, QueryResult};
use rocketmq_model::common::message::{message_ext::MessageExt, message_queue::MessageQueue};
use std::{collections::HashMap, sync::Arc};

/// A per-call snapshot of the facade's running owner and effective namespace.
/// Constructed only after that producer/consumer passes its lifecycle check.
#[doc(hidden)]
#[derive(Clone)]
pub struct MQAdminContext {
    pub(crate) admin: Arc<MQAdminImpl>,
    pub(crate) config: Arc<ClientConfig>,
}

impl MQAdminContext {
    fn topic(&self, topic: impl Into<cheetah_string::CheetahString>) -> cheetah_string::CheetahString {
        let topic: cheetah_string::CheetahString = topic.into();
        rocketmq_protocol::protocol::namespace_util::NamespaceUtil::wrap_namespace(
            self.config.resolved_namespace().unwrap_or_default(),
            topic,
        )
    }
}

pub trait MQAdmin: Send + Sync {
    #[doc(hidden)]
    fn admin_context(&self) -> ClientResult<MQAdminContext>;

    fn get_admin_timeout_millis(&self) -> ClientResult<i64> {
        Ok(self.admin_context()?.admin.get_timeout_millis())
    }
    fn set_admin_timeout_millis(&self, timeout: i64) -> ClientResult<()> {
        self.admin_context()?.admin.set_timeout_millis(timeout);
        Ok(())
    }
    fn create_topic<'a>(
        &'a self,
        key: &'a str,
        topic: &'a str,
        queues: i32,
        attributes: HashMap<String, String>,
    ) -> impl std::future::Future<Output = ClientResult<()>> + Send + 'a {
        async move { self.create_topic_with_flag(key, topic, queues, 0, attributes).await }
    }
    fn create_topic_with_flag<'a>(
        &'a self,
        key: &'a str,
        topic: &'a str,
        queues: i32,
        flags: i32,
        attributes: HashMap<String, String>,
    ) -> impl std::future::Future<Output = ClientResult<()>> + Send + 'a {
        async move {
            let ctx = self.admin_context()?;
            let topic = ctx.topic(topic);
            ctx.admin.create_topic(key, &topic, queues, flags, attributes).await
        }
    }
    fn search_offset<'a>(
        &'a self,
        queue: &'a MessageQueue,
        timestamp: u64,
    ) -> impl std::future::Future<Output = ClientResult<i64>> + Send + 'a {
        async move {
            let timestamp = i64::try_from(timestamp)
                .map_err(|_| crate::ClientError::illegal_argument("timestamp exceeds Java long range"))?;
            self.search_offset_signed(queue, timestamp).await
        }
    }
    fn search_offset_signed<'a>(
        &'a self,
        queue: &'a MessageQueue,
        timestamp: i64,
    ) -> impl std::future::Future<Output = ClientResult<i64>> + Send + 'a {
        async move {
            let ctx = self.admin_context()?;
            ctx.admin
                .search_offset_signed(&ctx.config.queue_with_resolved_namespace(queue.clone()), timestamp)
                .await
        }
    }
    fn max_offset<'a>(
        &'a self,
        queue: &'a MessageQueue,
    ) -> impl std::future::Future<Output = ClientResult<i64>> + Send + 'a {
        async move {
            let ctx = self.admin_context()?;
            ctx.admin
                .max_offset(&ctx.config.queue_with_resolved_namespace(queue.clone()))
                .await
        }
    }
    fn min_offset<'a>(
        &'a self,
        queue: &'a MessageQueue,
    ) -> impl std::future::Future<Output = ClientResult<i64>> + Send + 'a {
        async move {
            let ctx = self.admin_context()?;
            ctx.admin
                .min_offset(&ctx.config.queue_with_resolved_namespace(queue.clone()))
                .await
        }
    }
    fn earliest_msg_store_time<'a>(
        &'a self,
        queue: &'a MessageQueue,
    ) -> impl std::future::Future<Output = ClientResult<i64>> + Send + 'a {
        async move {
            let ctx = self.admin_context()?;
            ctx.admin
                .earliest_msg_store_time(&ctx.config.queue_with_resolved_namespace(queue.clone()))
                .await
        }
    }
    fn query_message<'a>(
        &'a self,
        topic: &'a str,
        key: &'a str,
        max: i32,
        begin: u64,
        end: u64,
    ) -> impl std::future::Future<Output = ClientResult<QueryResult>> + Send + 'a {
        async move {
            let begin = i64::try_from(begin)
                .map_err(|_| crate::ClientError::illegal_argument("begin exceeds Java long range"))?;
            let end =
                i64::try_from(end).map_err(|_| crate::ClientError::illegal_argument("end exceeds Java long range"))?;
            self.query_message_signed(topic, key, max, begin, end, false).await
        }
    }
    fn query_message_signed<'a>(
        &'a self,
        topic: &'a str,
        key: &'a str,
        max: i32,
        begin: i64,
        end: i64,
        unique: bool,
    ) -> impl std::future::Future<Output = ClientResult<QueryResult>> + Send + 'a {
        async move {
            self.query_message_report(MQAdminQueryOptions::new(topic, key, max, begin, end).unique_key(unique))
                .await?
                .into_complete_result()
        }
    }
    fn query_message_report<'a>(
        &'a self,
        mut options: MQAdminQueryOptions,
    ) -> impl std::future::Future<Output = ClientResult<MQAdminQueryReport>> + Send + 'a {
        async move {
            let ctx = self.admin_context()?;
            options.topic = ctx.topic(options.topic);
            ctx.admin.query_message_report(options).await
        }
    }
    fn query_message_by_uniq_key<'a>(
        &'a self,
        topic: &'a str,
        id: &'a str,
    ) -> impl std::future::Future<Output = ClientResult<MessageExt>> + Send + 'a {
        async move {
            let ctx = self.admin_context()?;
            ctx.admin.query_message_by_uniq_key(&ctx.topic(topic), id).await
        }
    }
    fn view_message<'a>(
        &'a self,
        topic: &'a str,
        id: &'a str,
    ) -> impl std::future::Future<Output = ClientResult<MessageExt>> + Send + 'a {
        async move {
            let ctx = self.admin_context()?;
            ctx.admin.view_message_by_topic_id(&ctx.topic(topic), id).await
        }
    }
    fn view_message_by_offset_id<'a>(
        &'a self,
        id: &'a str,
    ) -> impl std::future::Future<Output = ClientResult<MessageExt>> + Send + 'a {
        async move { self.admin_context()?.admin.view_message_by_offset_id(id).await }
    }
    fn fetch_publish_message_queues<'a>(
        &'a self,
        topic: &'a str,
    ) -> impl std::future::Future<Output = ClientResult<Vec<MessageQueue>>> + Send + 'a {
        async move {
            let ctx = self.admin_context()?;
            let client = ctx.admin.bound_client()?;
            let api = client.get_mq_client_api_impl()?;
            ctx.admin
                .fetch_publish_message_queues(&ctx.topic(topic), api, &ctx.config)
                .await
        }
    }
    fn fetch_subscribe_message_queues<'a>(
        &'a self,
        topic: &'a str,
    ) -> impl std::future::Future<Output = ClientResult<Vec<MessageQueue>>> + Send + 'a {
        async move {
            let ctx = self.admin_context()?;
            let queues = ctx.admin.fetch_subscribe_message_queues(&ctx.topic(topic)).await?;
            Ok(ctx.admin.parse_publish_message_queues(&queues, &ctx.config))
        }
    }
}
