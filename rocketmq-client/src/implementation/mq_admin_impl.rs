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

use crate::base::admin_query::{MQAdminQueryFailure, MQAdminQueryOptions, MQAdminQueryReport};
use futures::{stream, StreamExt};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;

use crate::base::client_config::ClientConfig;
use crate::base::query_result::QueryResult;
use crate::base::validators::Validators;
use crate::common::retry_policy::RetryInput;
use crate::factory::mq_client_instance;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::implementation::mq_client_api_impl::MQClientAPIImpl;
use crate::ClientError;
use cheetah_string::CheetahString;
use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_model::common::attribute::attribute_parser::AttributeParser;
use rocketmq_model::common::boundary_type::BoundaryType;
use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::TopicFilterType;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
use rocketmq_protocol::protocol::header::query_message_request_header::QueryMessageRequestHeader;
use rocketmq_protocol::protocol::header::view_message_request_header::ViewMessageRequestHeader;
use rocketmq_protocol::protocol::namespace_util::NamespaceUtil;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::route_facade::BrokerDataExt;
use rocketmq_transport::api::OutboundRequestContractReason;
use rocketmq_transport::api::OutboundRequestRejectionReason;
use rocketmq_transport::api::OutboundRequestStage;
use rocketmq_transport::api::RequestDeadline;

const CREATE_TOPIC_ATTEMPTS: u32 = 5;

fn create_topic_retry_error(input: RetryInput) -> ClientError {
    match input {
        RetryInput::Transport(error) => ClientError::from_transport(error),
        RetryInput::Rejected(rejection) => match rejection.reason() {
            OutboundRequestRejectionReason::DeadlineExpired => {
                let mut context = ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, "create_topic");
                if let Some(timeout_millis) = rejection.timeout_millis() {
                    context = context.with_u64(fields::TIMEOUT_MS, timeout_millis);
                }
                ClientError::from_shared(Arc::new(
                    Error::new(&rocketmq_error::CORE_OPERATION_TIMED_OUT).with_context(context),
                ))
            }
            OutboundRequestRejectionReason::ClientStopping => ClientError::not_started(),
            OutboundRequestRejectionReason::QueueSaturated => ClientError::from_shared(Arc::new(Error::new(
                &rocketmq_error::TRANSPORT_ADMISSION_QUEUE_SATURATED,
            ))),
            OutboundRequestRejectionReason::Cancelled
            | OutboundRequestRejectionReason::SessionClosed
            | OutboundRequestRejectionReason::EndpointUnavailable => {
                let phase = match rejection.reason() {
                    OutboundRequestRejectionReason::SessionClosed => "closed",
                    _ => create_topic_stage_phase(rejection.stage()),
                };
                let mut context = ErrorContext::new().with_text(fields::PHASE, phase);
                if rejection.remote_addr_present() {
                    context = context.with_secret_presence(fields::REMOTE_ADDR_PRESENT);
                }
                ClientError::from_shared(Arc::new(
                    Error::new(&rocketmq_error::TRANSPORT_CONNECTION_FAILED).with_context(context),
                ))
            }
        }
        .with_request_stage(rejection.stage()),
        RetryInput::Contract(contract) => match contract.reason() {
            OutboundRequestContractReason::NameServerEndpointMissing => {
                ClientError::from_shared(Arc::new(Error::new(&rocketmq_error::TRANSPORT_CONNECTION_FAILED)))
            }
        },
        RetryInput::Response { terminal_error, .. } | RetryInput::BusinessError(terminal_error) => terminal_error,
        // The create-topic request path cannot construct producer send status or route-policy inputs.
        RetryInput::SendStatus(_) | RetryInput::RouteUnavailable => {
            unreachable!("create-topic request returned an unrelated retry input")
        }
    }
}

const fn create_topic_stage_phase(stage: OutboundRequestStage) -> &'static str {
    match stage {
        OutboundRequestStage::BeforeWrite => "before_write",
        OutboundRequestStage::Writing => "writing",
        OutboundRequestStage::AwaitingResponse => "awaiting_response",
        OutboundRequestStage::ResponseReceived => "response_received",
    }
}

pub struct MQAdminImpl {
    // Custom fork: fixed Java6000ms, mutable without replacing the identity owner.
    timeout_millis: AtomicI64,
    client: OnceLock<Weak<MQClientInstance>>,
}

impl MQAdminImpl {
    pub fn new() -> Self {
        MQAdminImpl {
            timeout_millis: AtomicI64::new(6000),
            client: OnceLock::new(),
        }
    }

    pub fn get_timeout_millis(&self) -> i64 {
        self.timeout_millis.load(Ordering::Acquire)
    }
    pub fn set_timeout_millis(&self, timeout: i64) {
        self.timeout_millis.store(timeout, Ordering::Release);
    }
    fn timeout_millis(&self) -> u64 {
        self.get_timeout_millis().max(0) as u64
    }

    pub fn set_client(&self, client: &Arc<MQClientInstance>) -> bool {
        self.client.set(Arc::downgrade(client)).is_ok()
    }

    pub(crate) fn bound_client(&self) -> crate::ClientResult<Arc<MQClientInstance>> {
        self.client
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| ClientError::not_initialized("MQClientInstance"))
    }

    fn encode_topic_attributes(attributes: HashMap<String, String>) -> Option<CheetahString> {
        if attributes.is_empty() {
            return None;
        }
        let encoded = AttributeParser::parse_to_string(&attributes);
        if encoded.is_empty() {
            None
        } else {
            Some(CheetahString::from_string(encoded))
        }
    }

    fn message_matches_query(
        topic: &CheetahString,
        key: &CheetahString,
        msg: &MessageExt,
        unique_key_flag: bool,
    ) -> bool {
        let topic_matches = msg.topic().as_str() == topic.as_str();
        if !topic_matches {
            return false;
        }

        if unique_key_flag {
            if let Some(uniq_id) = MessageClientIDSetter::get_uniq_id(msg) {
                return uniq_id.as_str() == key.as_str();
            }
            return msg.msg_id.as_str() == key.as_str();
        }

        let Some(keys) = MessageTrait::get_keys(msg) else {
            return false;
        };
        // Java String.split discards every trailing empty token, but keeps a
        // leading empty token and the special empty-input token.
        if keys.is_empty() {
            return key.is_empty();
        }
        let keys = keys.trim_end_matches(MessageConst::KEY_SEPARATOR);
        !keys.is_empty()
            && keys
                .split(MessageConst::KEY_SEPARATOR)
                .any(|candidate| candidate == key.as_str())
    }

    fn strip_namespace_from_message(client_config: &ClientConfig, msg: &mut MessageExt) {
        if let Some(namespace) = client_config.resolved_namespace() {
            let topic = NamespaceUtil::without_namespace_with_namespace(msg.topic().as_str(), namespace.as_str());
            msg.set_topic(topic.into());
        }
    }

    #[cfg(test)]
    fn query_message_request_header(
        topic: CheetahString,
        key: CheetahString,
        max_num: i32,
        begin: u64,
        end: u64,
        _unique_key_flag: bool,
    ) -> crate::ClientResult<QueryMessageRequestHeader> {
        let begin_timestamp = i64::try_from(begin)
            .map_err(|_| ClientError::illegal_argument("queryMessage begin timestamp exceeds Java long range"))?;
        let end_timestamp = i64::try_from(end)
            .map_err(|_| ClientError::illegal_argument("queryMessage end timestamp exceeds Java long range"))?;
        Ok(Self::query_message_header_signed(
            topic,
            key,
            max_num,
            begin_timestamp,
            end_timestamp,
        ))
    }

    fn query_message_header_signed(
        topic: CheetahString,
        key: CheetahString,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> QueryMessageRequestHeader {
        // Custom fork: fixed4.6.1 has five fields. Unique selection is an ext flag;
        // indexType/lastKey are later protocol fields, never set by this SDK path.
        QueryMessageRequestHeader {
            topic,
            key,
            max_num,
            begin_timestamp: begin,
            end_timestamp: end,
            index_type: None,
            last_key: None,
            topic_request_header: None,
        }
    }

    fn timestamp_to_java_long(operation: &'static str, timestamp: u64) -> crate::ClientResult<i64> {
        i64::try_from(timestamp)
            .map_err(|_| ClientError::illegal_argument(format!("{operation} timestamp exceeds Java long range")))
    }
}

impl MQAdminImpl {
    pub async fn create_topic(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        attributes: HashMap<String, String>,
    ) -> crate::ClientResult<()> {
        Validators::check_topic(new_topic)?;

        let client = self.bound_client()?;
        let api_impl = client
            .mq_client_api_impl
            .load_full()
            .ok_or_else(|| ClientError::not_initialized("MQClientAPIImpl"))?;
        let route_data = api_impl
            .get_topic_route_info_from_name_server(key, self.timeout_millis())
            .await?
            .ok_or_else(|| mq_client_err!("Not found broker, maybe key is wrong"))?;
        if route_data.broker_datas.is_empty() {
            return Err(mq_client_err!("Not found broker, maybe key is wrong"));
        }

        let mut broker_datas = route_data.broker_datas;
        broker_datas.sort();
        let mut create_ok_at_least_once = false;
        let mut last_error: Option<crate::ClientError> = None;
        for broker_data in broker_datas {
            let Some(addr) = broker_data.broker_addrs().get(&mix_all::MASTER_ID).cloned() else {
                continue;
            };
            let mut create_ok = false;
            // Custom fork: Java makes up to five attempts, each with its own
            // timeout. Do not apply producer retry policy or a shared deadline.
            for _ in 0..CREATE_TOPIC_ATTEMPTS {
                let deadline = RequestDeadline::from_timeout_millis(self.timeout_millis());
                let request_header = CreateTopicRequestHeader {
                    topic: CheetahString::from_slice(new_topic),
                    default_topic: CheetahString::from_slice(key),
                    read_queue_nums: queue_num,
                    write_queue_nums: queue_num,
                    perm: (PermName::PERM_READ | PermName::PERM_WRITE) as i32,
                    topic_filter_type: CheetahString::from_static_str(TopicFilterType::SingleTag.as_str()),
                    topic_sys_flag: Some(topic_sys_flag),
                    order: false,
                    attributes: Self::encode_topic_attributes(attributes.clone()),
                    force: None,
                    topic_request_header: None,
                };
                match api_impl
                    .update_or_create_topic_once(&addr, request_header, deadline)
                    .await
                {
                    Ok(()) => {
                        create_ok = true;
                        create_ok_at_least_once = true;
                        break;
                    }
                    Err(input) => {
                        last_error = Some(create_topic_retry_error(input));
                    }
                }
            }
            if !create_ok {
                tracing::warn!(
                    "create topic {} on broker {} failed after retries",
                    new_topic,
                    broker_data.broker_name()
                );
            }
        }

        if create_ok_at_least_once {
            Ok(())
        } else {
            Err(last_error.unwrap_or_else(|| mq_client_err!("create topic to broker exception")))
        }
    }

    pub fn parse_publish_message_queues(
        &self,
        message_queue_array: &[MessageQueue],
        client_config: &ClientConfig,
    ) -> Vec<MessageQueue> {
        let mut message_queues = Vec::new();
        for message_queue in message_queue_array {
            let user_topic = NamespaceUtil::without_namespace_with_namespace(
                message_queue.topic_str(),
                client_config.resolved_namespace().unwrap_or_default().as_str(),
            );

            let message_queue =
                MessageQueue::from_parts(user_topic, message_queue.broker_name(), message_queue.queue_id());
            message_queues.push(message_queue);
        }
        message_queues
    }

    pub async fn fetch_publish_message_queues(
        &self,
        topic: &str,
        mq_client_api_impl: Arc<MQClientAPIImpl>,
        client_config: &ClientConfig,
    ) -> crate::ClientResult<Vec<MessageQueue>> {
        let topic_route_data = mq_client_api_impl
            .get_topic_route_info_from_name_server_detail(topic, self.timeout_millis(), true)
            .await?;
        if let Some(mut topic_route_data) = topic_route_data {
            let topic_publish_info =
                mq_client_instance::topic_route_data2topic_publish_info(topic, &mut topic_route_data);
            if topic_publish_info.ok() {
                return Ok(self.parse_publish_message_queues(&topic_publish_info.message_queue_list, client_config));
            }
        }
        Err(mq_client_err!(format!(
            "Unknow why, Can not find Message Queue for this topic, {}",
            topic
        )))
    }

    /// Queries the maximum offset of the given message queue from the broker.
    ///
    /// Retries the broker address lookup via the name server when it is not cached locally.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker address cannot be resolved or the remote call fails.
    pub async fn max_offset(&self, mq: &MessageQueue) -> crate::ClientResult<i64> {
        let client = self.bound_client()?;
        let broker_name = client.get_broker_name_from_message_queue(mq).await;
        let mut broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        if broker_addr.is_none() {
            client.refresh_topic_route_data(mq.topic()).await?;
            let broker_name = client.get_broker_name_from_message_queue(mq).await;
            broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        }
        if let Some(ref broker_addr) = broker_addr {
            return client
                .mq_client_api_impl
                .load_full()
                .ok_or_else(|| ClientError::not_initialized("MQClientAPIImpl"))?
                .get_max_offset(broker_addr, mq, self.timeout_millis())
                .await;
        }
        Err(mq_client_err!(format!("The broker[{}] not exist", mq.broker_name())))
    }

    /// Searches for the queue offset whose store timestamp is closest to `timestamp`.
    ///
    /// Defaults to [`BoundaryType::Lower`], returning the earliest offset whose store
    /// timestamp is greater than or equal to `timestamp`, matching the behaviour of
    /// `MQAdminImpl.searchOffset(MessageQueue, long)` in the Java implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker address cannot be resolved or the remote call fails.
    pub async fn search_offset(&self, mq: &MessageQueue, timestamp: u64) -> crate::ClientResult<i64> {
        self.search_offset_signed(mq, Self::timestamp_to_java_long("searchOffset", timestamp)?)
            .await
    }

    pub async fn search_offset_signed(&self, mq: &MessageQueue, timestamp: i64) -> crate::ClientResult<i64> {
        let client = self.bound_client()?;
        let broker_name = client.get_broker_name_from_message_queue(mq).await;
        let mut broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        if broker_addr.is_none() {
            client.refresh_topic_route_data(mq.topic()).await?;
            let broker_name = client.get_broker_name_from_message_queue(mq).await;
            broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        }
        if let Some(ref broker_addr) = broker_addr {
            return client
                .mq_client_api_impl
                .load_full()
                .ok_or_else(|| ClientError::not_initialized("MQClientAPIImpl"))?
                .search_offset_by_timestamp(broker_addr, mq, timestamp, BoundaryType::Lower, self.timeout_millis())
                .await;
        }
        Err(mq_client_err!(format!("The broker[{}] not exist", mq.broker_name())))
    }

    pub async fn min_offset(&self, mq: &MessageQueue) -> crate::ClientResult<i64> {
        let client = self.bound_client()?;
        let broker_name = client.get_broker_name_from_message_queue(mq).await;
        let mut broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        if broker_addr.is_none() {
            client.refresh_topic_route_data(mq.topic()).await?;
            let broker_name = client.get_broker_name_from_message_queue(mq).await;
            broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        }
        if let Some(ref broker_addr) = broker_addr {
            return client
                .mq_client_api_impl
                .load_full()
                .ok_or_else(|| ClientError::not_initialized("MQClientAPIImpl"))?
                .get_min_offset(broker_addr, mq, self.timeout_millis())
                .await;
        }
        Err(mq_client_err!(format!("The broker[{}] not exist", mq.broker_name())))
    }

    pub async fn earliest_msg_store_time(&self, mq: &MessageQueue) -> crate::ClientResult<i64> {
        let client = self.bound_client()?;
        let broker_name = client.get_broker_name_from_message_queue(mq).await;
        let mut broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        if broker_addr.is_none() {
            client.refresh_topic_route_data(mq.topic()).await?;
            let broker_name = client.get_broker_name_from_message_queue(mq).await;
            broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        }
        if let Some(ref broker_addr) = broker_addr {
            return client
                .mq_client_api_impl
                .load_full()
                .ok_or_else(|| ClientError::not_initialized("MQClientAPIImpl"))?
                .get_earliest_msg_store_time(broker_addr, mq, self.timeout_millis())
                .await;
        }
        Err(mq_client_err!(format!("The broker[{}] not exist", mq.broker_name())))
    }

    pub async fn view_message(&self, topic: &str, msg_id: &str) -> crate::ClientResult<MessageExt> {
        let _ = topic; // Compatibility argument; fixed Java's physical lookup has no topic.
        self.view_message_by_offset_id(msg_id).await
    }

    pub async fn view_message_by_offset_id(&self, msg_id: &str) -> crate::ClientResult<MessageExt> {
        let message_id = MessageDecoder::decode_message_id(msg_id).map_err(|_| {
            mq_client_err!(
                rocketmq_protocol::code::response_code::ResponseCode::NoMessage as i32,
                "query message by id finished, but no message."
            )
        })?;
        let broker_addr = CheetahString::from_string(message_id.address.to_string());
        let request_header = ViewMessageRequestHeader {
            topic: None,
            offset: message_id.offset,
        };
        self.bound_client()?
            .mq_client_api_impl
            .load_full()
            .ok_or_else(|| ClientError::not_initialized("MQClientAPIImpl"))?
            .view_message(&broker_addr, request_header, self.timeout_millis())
            .await
    }

    /// Java's Topic+ID overload first tries physical lookup, then unique ID.
    /// Preserve explicit authorization errors instead of hiding them in fallback.
    pub async fn view_message_by_topic_id(&self, topic: &str, id: &str) -> crate::ClientResult<MessageExt> {
        if MessageDecoder::decode_message_id(id).is_ok() {
            match self.view_message_by_offset_id(id).await {
                Ok(message) => return Ok(message),
                Err(error)
                    if matches!(
                        error.failure_kind(),
                        crate::ClientFailureKind::PermissionDenied | crate::ClientFailureKind::Authentication
                    ) =>
                {
                    return Err(error)
                }
                Err(_) => {}
            }
        }
        self.query_message_by_uniq_key(topic, id).await
    }

    pub async fn fetch_subscribe_message_queues(&self, topic: &str) -> crate::ClientResult<Vec<MessageQueue>> {
        let client = self.bound_client()?;
        let api = client.get_mq_client_api_impl()?;
        let route = api
            .get_topic_route_info_from_name_server(topic, self.timeout_millis())
            .await?
            .ok_or_else(|| mq_client_err!("topic route missing"))?;
        let queues = mq_client_instance::topic_route_data2topic_subscribe_info(topic, &route);
        if queues.is_empty() {
            return Err(mq_client_err!("no readable queue for topic"));
        }
        let mut queues: Vec<_> = queues.into_iter().collect();
        queues.sort();
        Ok(queues)
    }

    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
    ) -> crate::ClientResult<QueryResult> {
        self.query_message_with_unique_flag(topic, key, max_num, begin, end, false)
            .await
    }

    pub async fn query_message_with_unique_flag(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
        unique_key: bool,
    ) -> crate::ClientResult<QueryResult> {
        let begin = Self::timestamp_to_java_long("queryMessage begin", begin)?;
        let end = Self::timestamp_to_java_long("queryMessage end", end)?;
        self.query_message_signed(topic, key, max_num, begin, end, unique_key)
            .await
    }

    pub async fn query_message_signed(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: i64,
        end: i64,
        unique_key: bool,
    ) -> crate::ClientResult<QueryResult> {
        self.query_message_report(MQAdminQueryOptions::new(topic, key, max_num, begin, end).unique_key(unique_key))
            .await?
            .into_complete_result()
    }

    pub async fn query_message_by_uniq_key(&self, topic: &str, key: &str) -> crate::ClientResult<MessageExt> {
        let begin = MessageClientIDSetter::get_nearly_time_from_id(key)
            .map_err(ClientError::illegal_argument_source)?
            .timestamp_millis()
            .saturating_sub(1000);
        let result = self.query_message_signed(topic, key, 32, begin, i64::MAX, true).await?;
        result.message_list().first().cloned().ok_or_else(|| {
            ClientError::broker_operation_failed(
                "queryMessageByUniqKey",
                rocketmq_protocol::code::response_code::ResponseCode::NoMessage as i32,
                "no matching unique ID",
            )
        })
    }

    /// Owned concurrent fan-out. No spawned tasks outlive this future, and a
    /// caller may retain Java-style partial data together with every Broker failure.
    pub async fn query_message_report(&self, options: MQAdminQueryOptions) -> crate::ClientResult<MQAdminQueryReport> {
        let client = self.bound_client()?;
        let api = client
            .mq_client_api_impl
            .load_full()
            .ok_or_else(|| ClientError::not_initialized("MQClientAPIImpl"))?;
        let cached = client
            .topic_route_table
            .get(&options.topic)
            .map(|entry| TopicRouteData::from_existing(entry.value()));
        let route = match cached {
            Some(route) => Some(route),
            None => client.refresh_topic_route_data(&options.topic).await?,
        }
        .ok_or_else(|| {
            ClientError::broker_operation_failed(
                "queryMessage route",
                rocketmq_protocol::code::response_code::ResponseCode::TopicNotExist as i32,
                "topic route missing",
            )
        })?;
        let addresses: Vec<_> = route
            .broker_datas
            .into_iter()
            .filter_map(|broker| broker.select_broker_addr())
            .collect();
        if addresses.is_empty() {
            return Err(ClientError::broker_operation_failed(
                "queryMessage route",
                rocketmq_protocol::code::response_code::ResponseCode::TopicNotExist as i32,
                "route has no Broker address",
            ));
        }
        let timeout = self.timeout_millis();
        let aggregate_timeout = timeout.saturating_mul(4);
        let deadline = RequestDeadline::from_timeout_millis(aggregate_timeout);
        let mut pending: BTreeMap<_, _> = addresses.iter().cloned().enumerate().collect();
        let requests = addresses.iter().cloned().enumerate().map(|(index, address)| {
            let api = api.clone();
            let header = Self::query_message_header_signed(
                options.topic.clone(),
                options.key.clone(),
                options.max_num,
                options.begin_timestamp,
                options.end_timestamp,
            );
            let unique = options.unique_key;
            async move {
                (
                    index,
                    MQClientAPIImpl::query_message(&api, &address, header, unique, timeout.saturating_mul(3)).await,
                )
            }
        });
        // Fixed Java's default asynchronous semaphore. Transport admission also
        // enforces the injected runtime's budgets; there is no per-Broker task spawn.
        let mut requests = stream::iter(requests).buffer_unordered(65535);
        let mut messages = Vec::new();
        let mut timestamp = 0_i64;
        let mut completed = 0;
        let mut empty = 0;
        let mut failures = Vec::new();
        let mut deadline_reached = false;
        while !pending.is_empty() {
            let response = tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline.instant()) => { deadline_reached = true; None },
                response = requests.next() => response,
            };
            let Some((index, response)) = response else {
                break;
            };
            let Some(address) = pending.remove(&index) else {
                continue;
            };
            completed += 1;
            match response {
                Ok(Some((header, body))) => {
                    let decoded = body
                        .ok_or_else(|| ClientError::response_process_failed("queryMessage", "response has no body"))
                        .and_then(|mut body| {
                            MessageDecoder::decode_batch_checked(&mut body, true, true, |_| true)
                                .map_err(ClientError::from)
                        });
                    match decoded {
                        Ok(batch) => {
                            timestamp = timestamp.max(header.index_last_update_timestamp);
                            for mut message in batch {
                                if Self::message_matches_query(
                                    &options.topic,
                                    &options.key,
                                    &message,
                                    options.unique_key,
                                ) {
                                    Self::strip_namespace_from_message(&client.client_config, &mut message);
                                    messages.push(message);
                                }
                            }
                        }
                        Err(error) => failures.push(MQAdminQueryFailure {
                            broker_address: address,
                            error,
                        }),
                    }
                }
                Ok(None) => empty += 1,
                Err(error) => failures.push(MQAdminQueryFailure {
                    broker_address: address,
                    error,
                }),
            }
        }
        drop(requests);
        for (_, address) in pending {
            failures.push(MQAdminQueryFailure {
                broker_address: address,
                error: ClientError::timeout("queryMessage aggregate", aggregate_timeout),
            });
        }
        if options.unique_key {
            // Fixed Java returns only the earliest stored copy of a unique ID.
            messages = messages
                .into_iter()
                .min_by_key(|message| message.store_timestamp)
                .into_iter()
                .collect();
        }
        Ok(MQAdminQueryReport {
            result: QueryResult::new(timestamp, messages),
            brokers_queried: addresses.len(),
            brokers_completed: completed,
            brokers_empty: empty,
            failures,
            deadline_reached,
        })
    }
}


