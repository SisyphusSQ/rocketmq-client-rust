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

use super::*;
use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;

impl MQClientAPIImpl {
    #[inline]
    pub(super) fn create_remoting_command(&self, code: impl Into<i32>) -> RemotingCommand {
        self.command_factory.create_remoting_command(code)
    }

    #[inline]
    pub(super) fn create_request(&self, code: impl Into<i32>, body: impl Into<bytes::Bytes>) -> RemotingCommand {
        self.command_factory.create_request(code, body)
    }

    #[inline]
    pub(super) fn create_request_command<T>(&self, code: impl Into<i32>, header: T) -> RemotingCommand
    where
        T: CommandCustomHeader + Send + Sync + 'static,
    {
        self.command_factory.create_request_command(code, header)
    }
}

#[cfg(feature = "admin-mutation")]
pub(super) fn lite_subscription_ctl_request(
    command_factory: &RemotingCommandFactory,
    lite_subscription_dto: LiteSubscriptionDTO,
) -> ClientResult<RemotingCommand> {
    let mut request_body = LiteSubscriptionCtlRequestBody::new();
    request_body.set_subscription_set(vec![lite_subscription_dto]);
    let mut request = command_factory.create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {});
    request.set_body_mut_ref(request_body.encode()?);
    Ok(request)
}

pub(super) fn notification_request(
    command_factory: &RemotingCommandFactory,
    request_header: NotificationRequestHeader,
) -> RemotingCommand {
    command_factory.create_request_command(RequestCode::Notification, request_header)
}

#[cfg(feature = "admin-mutation")]
pub(super) fn create_and_update_plain_access_config_request(
    command_factory: &RemotingCommandFactory,
    plain_access_config: &PlainAccessConfig,
) -> ClientResult<RemotingCommand> {
    // Custom: 4.6.1 CreateAccessConfigRequestHeader carries every field in extFields, with no body.
    let mut request = command_factory.create_remoting_command(RequestCode::UpdateAndCreateAclConfig);
    for (name, value) in [
        ("accessKey", plain_access_config.access_key.as_ref()),
        ("secretKey", plain_access_config.secret_key.as_ref()),
        ("whiteRemoteAddress", plain_access_config.white_remote_address.as_ref()),
        ("defaultTopicPerm", plain_access_config.default_topic_perm.as_ref()),
        ("defaultGroupPerm", plain_access_config.default_group_perm.as_ref()),
    ] {
        if let Some(value) = value {
            request.add_ext_field(name, value.clone());
        }
    }
    request.add_ext_field("admin", plain_access_config.admin.to_string());
    for (name, values) in [
        ("topicPerms", &plain_access_config.topic_perms),
        ("groupPerms", &plain_access_config.group_perms),
    ] {
        if let Some(values) = values.as_ref().filter(|values| !values.is_empty()) {
            request.add_ext_field(
                name,
                values.iter().map(|value| value.as_str()).collect::<Vec<_>>().join(","),
            );
        }
    }
    Ok(request)
}

pub(super) fn get_acl_request(command_factory: &RemotingCommandFactory, subject: CheetahString) -> RemotingCommand {
    command_factory.create_request_command(RequestCode::AuthGetAcl, GetAclRequestHeader { subject })
}

#[cfg(feature = "admin-mutation")]
pub(super) fn delete_plain_access_config_request(
    command_factory: &RemotingCommandFactory,
    access_key: &CheetahString,
) -> RemotingCommand {
    // Custom: Java 4.6.1 DeleteAccessConfigRequestHeader, never an untyped body.
    let mut request = command_factory.create_remoting_command(RequestCode::DeleteAclConfig);
    request.add_ext_field("accessKey", access_key.clone());
    request
}

pub(super) fn heartbeat_request(
    command_factory: &RemotingCommandFactory,
    heartbeat_data: &HeartbeatData,
    language: LanguageCode,
) -> ClientResult<RemotingCommand> {
    Ok(command_factory
        .create_request_command(RequestCode::HeartBeat, HeartbeatRequestHeader::default())
        .set_language(language)
        .set_body(heartbeat_data.encode()?))
}

pub(super) fn get_all_consumer_offset_request(command_factory: &RemotingCommandFactory) -> RemotingCommand {
    command_factory.create_remoting_command(RequestCode::GetAllConsumerOffset)
}

#[cfg(feature = "admin-mutation")]
pub(super) fn create_topic_list_request(
    command_factory: &RemotingCommandFactory,
    topic_config_list: Vec<TopicConfig>,
) -> ClientResult<RemotingCommand> {
    let body = CreateTopicListRequestBody { topic_config_list };
    Ok(command_factory
        .create_request_command(
            RequestCode::UpdateAndCreateTopicList,
            CreateTopicListRequestHeader::default(),
        )
        .set_body(body.encode()?))
}

#[cfg(feature = "admin-mutation")]
pub(super) fn create_subscription_group_list_request(
    command_factory: &RemotingCommandFactory,
    configs: Vec<SubscriptionGroupConfig>,
) -> ClientResult<RemotingCommand> {
    let body = SubscriptionGroupList {
        group_config_list: configs,
    };
    Ok(command_factory
        .create_request_command(RequestCode::UpdateAndCreateSubscriptionGroupList, EmptyHeader {})
        .set_body(body.encode()?))
}

#[cfg(feature = "admin-mutation")]
pub(super) fn delete_topic_list_request(
    command_factory: &RemotingCommandFactory,
    topic_list: Vec<CheetahString>,
) -> ClientResult<RemotingCommand> {
    let body = DeleteTopicListRequestBody { topic_list };
    Ok(command_factory
        .create_request_command(RequestCode::DeleteTopicInBrokerList, EmptyHeader {})
        .set_body(body.encode()?))
}

#[cfg(feature = "admin-mutation")]
pub(super) fn delete_subscription_group_list_request(
    command_factory: &RemotingCommandFactory,
    group_name_list: Vec<CheetahString>,
    clean_offset: bool,
) -> ClientResult<RemotingCommand> {
    let body = DeleteSubscriptionGroupListRequestBody {
        group_name_list,
        clean_offset,
    };
    Ok(command_factory
        .create_request_command(RequestCode::DeleteSubscriptionGroupList, EmptyHeader {})
        .set_body(body.encode()?))
}
