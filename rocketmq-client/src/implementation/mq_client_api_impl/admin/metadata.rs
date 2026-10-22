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

// Custom fork: Java 4.6.1 network lifecycle and preserved physical request failures.

//! Fixed Java 4.6.1 bulk configuration reads.
use super::*;
use rocketmq_protocol::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper;
use rocketmq_protocol::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper;

impl MQClientAPIImpl {
    pub(crate) async fn get_all_topic_config(
        &self,
        addr: &CheetahString,
        timeout: u64,
    ) -> ClientResult<TopicConfigSerializeWrapper> {
        let request = self.create_remoting_command(RequestCode::GetAllTopicConfig);
        let address = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .resource_response("getAllTopicConfig", Some(&address), request, timeout)
            .await?;
        let wrapper: TopicConfigSerializeWrapper = resource_body(&response, "getAllTopicConfig")?;
        // Custom: a legal empty table is Some(empty); missing metadata is not an absent Topic.
        if wrapper.topic_config_table().is_none() || wrapper.data_version().is_none() {
            return Err(ClientError::response_process_source(
                "getAllTopicConfig",
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "topicConfigTable/dataVersion missing or null",
                ),
            ));
        }
        Ok(wrapper)
    }
    pub(crate) async fn get_all_subscription_group_config(
        &self,
        addr: &CheetahString,
        timeout: u64,
    ) -> ClientResult<SubscriptionGroupWrapper> {
        let request = self.create_remoting_command(RequestCode::GetAllSubscriptionGroupConfig);
        let address = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .resource_response("getAllSubscriptionGroup", Some(&address), request, timeout)
            .await?;
        resource_body(&response, "getAllSubscriptionGroup")
    }
}
