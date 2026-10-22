// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.

//! Local reference bean for all 81 fixed Java 4.6.1 BrokerConfig properties.
//! This is never used to infer a deployed Broker value or to enable a server feature.

use crate::{ClientError, ClientResult};
use cheetah_string::CheetahString;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BrokerPropertyKind {
    String,
    I32,
    I64,
    Boolean,
}

#[derive(Clone, Copy, Debug)]
pub struct BrokerConfigProperty {
    pub name: &'static str,
    pub kind: BrokerPropertyKind,
    pub java_default: &'static str,
}

pub const BROKER_CONFIG_PROPERTIES: &[BrokerConfigProperty] = &[
    BrokerConfigProperty {
        name: "rocketmqHome",
        kind: BrokerPropertyKind::String,
        java_default: "System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV))",
    },
    BrokerConfigProperty {
        name: "namesrvAddr",
        kind: BrokerPropertyKind::String,
        java_default: "System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV))",
    },
    BrokerConfigProperty {
        name: "brokerIP1",
        kind: BrokerPropertyKind::String,
        java_default: "RemotingUtil.getLocalAddress()",
    },
    BrokerConfigProperty {
        name: "brokerIP2",
        kind: BrokerPropertyKind::String,
        java_default: "RemotingUtil.getLocalAddress()",
    },
    BrokerConfigProperty {
        name: "brokerName",
        kind: BrokerPropertyKind::String,
        java_default: "localHostName()",
    },
    BrokerConfigProperty {
        name: "brokerClusterName",
        kind: BrokerPropertyKind::String,
        java_default: "\"DefaultCluster\"",
    },
    BrokerConfigProperty {
        name: "brokerId",
        kind: BrokerPropertyKind::I64,
        java_default: "MixAll.MASTER_ID",
    },
    BrokerConfigProperty {
        name: "brokerPermission",
        kind: BrokerPropertyKind::I32,
        java_default: "PermName.PERM_READ | PermName.PERM_WRITE",
    },
    BrokerConfigProperty {
        name: "defaultTopicQueueNums",
        kind: BrokerPropertyKind::I32,
        java_default: "8",
    },
    BrokerConfigProperty {
        name: "autoCreateTopicEnable",
        kind: BrokerPropertyKind::Boolean,
        java_default: "true",
    },
    BrokerConfigProperty {
        name: "clusterTopicEnable",
        kind: BrokerPropertyKind::Boolean,
        java_default: "true",
    },
    BrokerConfigProperty {
        name: "brokerTopicEnable",
        kind: BrokerPropertyKind::Boolean,
        java_default: "true",
    },
    BrokerConfigProperty {
        name: "autoCreateSubscriptionGroup",
        kind: BrokerPropertyKind::Boolean,
        java_default: "true",
    },
    BrokerConfigProperty {
        name: "messageStorePlugIn",
        kind: BrokerPropertyKind::String,
        java_default: "\"\"",
    },
    BrokerConfigProperty {
        name: "msgTraceTopicName",
        kind: BrokerPropertyKind::String,
        java_default: "MixAll.RMQ_SYS_TRACE_TOPIC",
    },
    BrokerConfigProperty {
        name: "traceTopicEnable",
        kind: BrokerPropertyKind::Boolean,
        java_default: "false",
    },
    BrokerConfigProperty {
        name: "sendMessageThreadPoolNums",
        kind: BrokerPropertyKind::I32,
        java_default: "1",
    },
    BrokerConfigProperty {
        name: "pullMessageThreadPoolNums",
        kind: BrokerPropertyKind::I32,
        java_default: "16 + Runtime.getRuntime().availableProcessors() * 2",
    },
    BrokerConfigProperty {
        name: "processReplyMessageThreadPoolNums",
        kind: BrokerPropertyKind::I32,
        java_default: "16 + Runtime.getRuntime().availableProcessors() * 2",
    },
    BrokerConfigProperty {
        name: "queryMessageThreadPoolNums",
        kind: BrokerPropertyKind::I32,
        java_default: "8 + Runtime.getRuntime().availableProcessors()",
    },
    BrokerConfigProperty {
        name: "adminBrokerThreadPoolNums",
        kind: BrokerPropertyKind::I32,
        java_default: "16",
    },
    BrokerConfigProperty {
        name: "clientManageThreadPoolNums",
        kind: BrokerPropertyKind::I32,
        java_default: "32",
    },
    BrokerConfigProperty {
        name: "consumerManageThreadPoolNums",
        kind: BrokerPropertyKind::I32,
        java_default: "32",
    },
    BrokerConfigProperty {
        name: "reportStageThreadPoolNums",
        kind: BrokerPropertyKind::I32,
        java_default: "32",
    },
    BrokerConfigProperty {
        name: "heartbeatThreadPoolNums",
        kind: BrokerPropertyKind::I32,
        java_default: "Math.min(32, Runtime.getRuntime().availableProcessors())",
    },
    BrokerConfigProperty {
        name: "endTransactionThreadPoolNums",
        kind: BrokerPropertyKind::I32,
        java_default: "8 + Runtime.getRuntime().availableProcessors() * 2",
    },
    BrokerConfigProperty {
        name: "flushConsumerOffsetInterval",
        kind: BrokerPropertyKind::I32,
        java_default: "1000 * 5",
    },
    BrokerConfigProperty {
        name: "flushConsumerOffsetHistoryInterval",
        kind: BrokerPropertyKind::I32,
        java_default: "1000 * 60",
    },
    BrokerConfigProperty {
        name: "rejectTransactionMessage",
        kind: BrokerPropertyKind::Boolean,
        java_default: "false",
    },
    BrokerConfigProperty {
        name: "fetchNamesrvAddrByAddressServer",
        kind: BrokerPropertyKind::Boolean,
        java_default: "false",
    },
    BrokerConfigProperty {
        name: "sendThreadPoolQueueCapacity",
        kind: BrokerPropertyKind::I32,
        java_default: "10000",
    },
    BrokerConfigProperty {
        name: "pullThreadPoolQueueCapacity",
        kind: BrokerPropertyKind::I32,
        java_default: "100000",
    },
    BrokerConfigProperty {
        name: "replyThreadPoolQueueCapacity",
        kind: BrokerPropertyKind::I32,
        java_default: "10000",
    },
    BrokerConfigProperty {
        name: "queryThreadPoolQueueCapacity",
        kind: BrokerPropertyKind::I32,
        java_default: "20000",
    },
    BrokerConfigProperty {
        name: "clientManagerThreadPoolQueueCapacity",
        kind: BrokerPropertyKind::I32,
        java_default: "1000000",
    },
    BrokerConfigProperty {
        name: "consumerManagerThreadPoolQueueCapacity",
        kind: BrokerPropertyKind::I32,
        java_default: "1000000",
    },
    BrokerConfigProperty {
        name: "reportStageThreadPoolQueueCapacity",
        kind: BrokerPropertyKind::I32,
        java_default: "10000",
    },
    BrokerConfigProperty {
        name: "heartbeatThreadPoolQueueCapacity",
        kind: BrokerPropertyKind::I32,
        java_default: "50000",
    },
    BrokerConfigProperty {
        name: "endTransactionPoolQueueCapacity",
        kind: BrokerPropertyKind::I32,
        java_default: "100000",
    },
    BrokerConfigProperty {
        name: "clusterId",
        kind: BrokerPropertyKind::I32,
        java_default: "Integer.parseInt(System.getProperty(MixAll.CLUSTER_ID_PROPERTY,\"0\"))",
    },
    BrokerConfigProperty {
        name: "authServiceDomain",
        kind: BrokerPropertyKind::String,
        java_default: "System.getProperty(MixAll.AUTH_SERVICE_DOMAIN_PROPERTY)",
    },
    BrokerConfigProperty {
        name: "filterServerNums",
        kind: BrokerPropertyKind::I32,
        java_default: "0",
    },
    BrokerConfigProperty {
        name: "longPollingEnable",
        kind: BrokerPropertyKind::Boolean,
        java_default: "true",
    },
    BrokerConfigProperty {
        name: "shortPollingTimeMills",
        kind: BrokerPropertyKind::I64,
        java_default: "1000",
    },
    BrokerConfigProperty {
        name: "notifyConsumerIdsChangedEnable",
        kind: BrokerPropertyKind::Boolean,
        java_default: "true",
    },
    BrokerConfigProperty {
        name: "highSpeedMode",
        kind: BrokerPropertyKind::Boolean,
        java_default: "false",
    },
    BrokerConfigProperty {
        name: "commercialEnable",
        kind: BrokerPropertyKind::Boolean,
        java_default: "true",
    },
    BrokerConfigProperty {
        name: "commercialTimerCount",
        kind: BrokerPropertyKind::I32,
        java_default: "1",
    },
    BrokerConfigProperty {
        name: "commercialTransCount",
        kind: BrokerPropertyKind::I32,
        java_default: "1",
    },
    BrokerConfigProperty {
        name: "commercialBigCount",
        kind: BrokerPropertyKind::I32,
        java_default: "1",
    },
    BrokerConfigProperty {
        name: "commercialBaseCount",
        kind: BrokerPropertyKind::I32,
        java_default: "1",
    },
    BrokerConfigProperty {
        name: "transferMsgByHeap",
        kind: BrokerPropertyKind::Boolean,
        java_default: "true",
    },
    BrokerConfigProperty {
        name: "maxDelayTime",
        kind: BrokerPropertyKind::I32,
        java_default: "40",
    },
    BrokerConfigProperty {
        name: "regionId",
        kind: BrokerPropertyKind::String,
        java_default: "MixAll.DEFAULT_TRACE_REGION_ID",
    },
    BrokerConfigProperty {
        name: "registerBrokerTimeoutMills",
        kind: BrokerPropertyKind::I32,
        java_default: "6000",
    },
    BrokerConfigProperty {
        name: "slaveReadEnable",
        kind: BrokerPropertyKind::Boolean,
        java_default: "false",
    },
    BrokerConfigProperty {
        name: "disableConsumeIfConsumerReadSlowly",
        kind: BrokerPropertyKind::Boolean,
        java_default: "false",
    },
    BrokerConfigProperty {
        name: "consumerFallbehindThreshold",
        kind: BrokerPropertyKind::I64,
        java_default: "1024L * 1024 * 1024 * 16",
    },
    BrokerConfigProperty {
        name: "brokerFastFailureEnable",
        kind: BrokerPropertyKind::Boolean,
        java_default: "true",
    },
    BrokerConfigProperty {
        name: "waitTimeMillsInSendQueue",
        kind: BrokerPropertyKind::I64,
        java_default: "200",
    },
    BrokerConfigProperty {
        name: "waitTimeMillsInPullQueue",
        kind: BrokerPropertyKind::I64,
        java_default: "5 * 1000",
    },
    BrokerConfigProperty {
        name: "waitTimeMillsInHeartbeatQueue",
        kind: BrokerPropertyKind::I64,
        java_default: "31 * 1000",
    },
    BrokerConfigProperty {
        name: "waitTimeMillsInTransactionQueue",
        kind: BrokerPropertyKind::I64,
        java_default: "3 * 1000",
    },
    BrokerConfigProperty {
        name: "startAcceptSendRequestTimeStamp",
        kind: BrokerPropertyKind::I64,
        java_default: "0L",
    },
    BrokerConfigProperty {
        name: "traceOn",
        kind: BrokerPropertyKind::Boolean,
        java_default: "true",
    },
    BrokerConfigProperty {
        name: "enableCalcFilterBitMap",
        kind: BrokerPropertyKind::Boolean,
        java_default: "false",
    },
    BrokerConfigProperty {
        name: "expectConsumerNumUseFilter",
        kind: BrokerPropertyKind::I32,
        java_default: "32",
    },
    BrokerConfigProperty {
        name: "maxErrorRateOfBloomFilter",
        kind: BrokerPropertyKind::I32,
        java_default: "20",
    },
    BrokerConfigProperty {
        name: "filterDataCleanTimeSpan",
        kind: BrokerPropertyKind::I64,
        java_default: "24 * 3600 * 1000",
    },
    BrokerConfigProperty {
        name: "filterSupportRetry",
        kind: BrokerPropertyKind::Boolean,
        java_default: "false",
    },
    BrokerConfigProperty {
        name: "enablePropertyFilter",
        kind: BrokerPropertyKind::Boolean,
        java_default: "false",
    },
    BrokerConfigProperty {
        name: "authSwitchEnabled",
        kind: BrokerPropertyKind::Boolean,
        java_default: "true",
    },
    BrokerConfigProperty {
        name: "trackEnabled",
        kind: BrokerPropertyKind::Boolean,
        java_default: "false",
    },
    BrokerConfigProperty {
        name: "compressedRegister",
        kind: BrokerPropertyKind::Boolean,
        java_default: "false",
    },
    BrokerConfigProperty {
        name: "forceRegister",
        kind: BrokerPropertyKind::Boolean,
        java_default: "true",
    },
    BrokerConfigProperty {
        name: "registerNameServerPeriod",
        kind: BrokerPropertyKind::I32,
        java_default: "1000 * 30",
    },
    BrokerConfigProperty {
        name: "transactionTimeOut",
        kind: BrokerPropertyKind::I64,
        java_default: "6 * 1000",
    },
    BrokerConfigProperty {
        name: "transactionCheckMax",
        kind: BrokerPropertyKind::I32,
        java_default: "15",
    },
    BrokerConfigProperty {
        name: "transactionCheckInterval",
        kind: BrokerPropertyKind::I64,
        java_default: "60 * 1000",
    },
    BrokerConfigProperty {
        name: "aclEnable",
        kind: BrokerPropertyKind::Boolean,
        java_default: "false",
    },
    BrokerConfigProperty {
        name: "storeReplyMessageEnable",
        kind: BrokerPropertyKind::Boolean,
        java_default: "true",
    },
];

/// Explicit host/CPU/environment inputs allow deterministic reference defaults.
#[derive(Clone)]
pub struct BrokerConfigDefaults {
    pub logical_processors: i32,
    pub rocketmq_home: Option<CheetahString>,
    pub namesrv_addr: Option<CheetahString>,
    pub local_address: Option<CheetahString>,
    pub host_name: CheetahString,
    pub cluster_id: i32,
    pub auth_service_domain: CheetahString,
}

impl BrokerConfigDefaults {
    pub fn from_process() -> ClientResult<Self> {
        let property = |name, fallback| {
            std::env::var(name)
                .ok()
                .or_else(|| std::env::var(fallback).ok())
                .map(CheetahString::from)
        };
        let cluster_id = std::env::var("rocketmq.clusterId")
            .unwrap_or_else(|_| "0".to_string())
            .parse::<i32>()
            .map_err(|error| ClientError::config_parse_source("rocketmq.clusterId", error))?;
        Ok(Self {
            logical_processors: num_cpus::get() as i32,
            rocketmq_home: property("rocketmq.home.dir", "ROCKETMQ_HOME"),
            namesrv_addr: property("rocketmq.namesrv.addr", "NAMESRV_ADDR"),
            local_address: rocketmq_transport::api::NetworkUtil::get_local_address().map(CheetahString::from),
            host_name: BrokerConfig::local_host_name(),
            cluster_id,
            auth_service_domain: std::env::var("rocketmq.auth.service.domain")
                .map_err(|_| ClientError::config_missing("rocketmq.auth.service.domain"))?
                .into(),
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    #[serde(rename = "rocketmqHome")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rocketmq_home: Option<CheetahString>,
    #[serde(rename = "namesrvAddr")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namesrv_addr: Option<CheetahString>,
    #[serde(rename = "brokerIP1")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_ip1: Option<CheetahString>,
    #[serde(rename = "brokerIP2")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_ip2: Option<CheetahString>,
    #[serde(rename = "brokerName")]
    pub broker_name: CheetahString,
    #[serde(rename = "brokerClusterName")]
    pub broker_cluster_name: CheetahString,
    #[serde(rename = "brokerId")]
    pub broker_id: i64,
    #[serde(rename = "brokerPermission")]
    pub broker_permission: i32,
    #[serde(rename = "defaultTopicQueueNums")]
    pub default_topic_queue_nums: i32,
    #[serde(rename = "autoCreateTopicEnable")]
    pub auto_create_topic_enable: bool,
    #[serde(rename = "clusterTopicEnable")]
    pub cluster_topic_enable: bool,
    #[serde(rename = "brokerTopicEnable")]
    pub broker_topic_enable: bool,
    #[serde(rename = "autoCreateSubscriptionGroup")]
    pub auto_create_subscription_group: bool,
    #[serde(rename = "messageStorePlugIn")]
    pub message_store_plug_in: CheetahString,
    #[serde(rename = "msgTraceTopicName")]
    pub msg_trace_topic_name: CheetahString,
    #[serde(rename = "traceTopicEnable")]
    pub trace_topic_enable: bool,
    #[serde(rename = "sendMessageThreadPoolNums")]
    pub send_message_thread_pool_nums: i32,
    #[serde(rename = "pullMessageThreadPoolNums")]
    pub pull_message_thread_pool_nums: i32,
    #[serde(rename = "processReplyMessageThreadPoolNums")]
    pub process_reply_message_thread_pool_nums: i32,
    #[serde(rename = "queryMessageThreadPoolNums")]
    pub query_message_thread_pool_nums: i32,
    #[serde(rename = "adminBrokerThreadPoolNums")]
    pub admin_broker_thread_pool_nums: i32,
    #[serde(rename = "clientManageThreadPoolNums")]
    pub client_manage_thread_pool_nums: i32,
    #[serde(rename = "consumerManageThreadPoolNums")]
    pub consumer_manage_thread_pool_nums: i32,
    #[serde(rename = "reportStageThreadPoolNums")]
    pub report_stage_thread_pool_nums: i32,
    #[serde(rename = "heartbeatThreadPoolNums")]
    pub heartbeat_thread_pool_nums: i32,
    #[serde(rename = "endTransactionThreadPoolNums")]
    pub end_transaction_thread_pool_nums: i32,
    #[serde(rename = "flushConsumerOffsetInterval")]
    pub flush_consumer_offset_interval: i32,
    #[serde(rename = "flushConsumerOffsetHistoryInterval")]
    pub flush_consumer_offset_history_interval: i32,
    #[serde(rename = "rejectTransactionMessage")]
    pub reject_transaction_message: bool,
    #[serde(rename = "fetchNamesrvAddrByAddressServer")]
    pub fetch_namesrv_addr_by_address_server: bool,
    #[serde(rename = "sendThreadPoolQueueCapacity")]
    pub send_thread_pool_queue_capacity: i32,
    #[serde(rename = "pullThreadPoolQueueCapacity")]
    pub pull_thread_pool_queue_capacity: i32,
    #[serde(rename = "replyThreadPoolQueueCapacity")]
    pub reply_thread_pool_queue_capacity: i32,
    #[serde(rename = "queryThreadPoolQueueCapacity")]
    pub query_thread_pool_queue_capacity: i32,
    #[serde(rename = "clientManagerThreadPoolQueueCapacity")]
    pub client_manager_thread_pool_queue_capacity: i32,
    #[serde(rename = "consumerManagerThreadPoolQueueCapacity")]
    pub consumer_manager_thread_pool_queue_capacity: i32,
    #[serde(rename = "reportStageThreadPoolQueueCapacity")]
    pub report_stage_thread_pool_queue_capacity: i32,
    #[serde(rename = "heartbeatThreadPoolQueueCapacity")]
    pub heartbeat_thread_pool_queue_capacity: i32,
    #[serde(rename = "endTransactionPoolQueueCapacity")]
    pub end_transaction_pool_queue_capacity: i32,
    #[serde(rename = "clusterId")]
    pub cluster_id: i32,
    #[serde(rename = "authServiceDomain")]
    pub auth_service_domain: CheetahString,
    #[serde(rename = "filterServerNums")]
    pub filter_server_nums: i32,
    #[serde(rename = "longPollingEnable")]
    pub long_polling_enable: bool,
    #[serde(rename = "shortPollingTimeMills")]
    pub short_polling_time_mills: i64,
    #[serde(rename = "notifyConsumerIdsChangedEnable")]
    pub notify_consumer_ids_changed_enable: bool,
    #[serde(rename = "highSpeedMode")]
    pub high_speed_mode: bool,
    #[serde(rename = "commercialEnable")]
    pub commercial_enable: bool,
    #[serde(rename = "commercialTimerCount")]
    pub commercial_timer_count: i32,
    #[serde(rename = "commercialTransCount")]
    pub commercial_trans_count: i32,
    #[serde(rename = "commercialBigCount")]
    pub commercial_big_count: i32,
    #[serde(rename = "commercialBaseCount")]
    pub commercial_base_count: i32,
    #[serde(rename = "transferMsgByHeap")]
    pub transfer_msg_by_heap: bool,
    #[serde(rename = "maxDelayTime")]
    pub max_delay_time: i32,
    #[serde(rename = "regionId")]
    pub region_id: CheetahString,
    #[serde(rename = "registerBrokerTimeoutMills")]
    pub register_broker_timeout_mills: i32,
    #[serde(rename = "slaveReadEnable")]
    pub slave_read_enable: bool,
    #[serde(rename = "disableConsumeIfConsumerReadSlowly")]
    pub disable_consume_if_consumer_read_slowly: bool,
    #[serde(rename = "consumerFallbehindThreshold")]
    pub consumer_fallbehind_threshold: i64,
    #[serde(rename = "brokerFastFailureEnable")]
    pub broker_fast_failure_enable: bool,
    #[serde(rename = "waitTimeMillsInSendQueue")]
    pub wait_time_mills_in_send_queue: i64,
    #[serde(rename = "waitTimeMillsInPullQueue")]
    pub wait_time_mills_in_pull_queue: i64,
    #[serde(rename = "waitTimeMillsInHeartbeatQueue")]
    pub wait_time_mills_in_heartbeat_queue: i64,
    #[serde(rename = "waitTimeMillsInTransactionQueue")]
    pub wait_time_mills_in_transaction_queue: i64,
    #[serde(rename = "startAcceptSendRequestTimeStamp")]
    pub start_accept_send_request_time_stamp: i64,
    #[serde(rename = "traceOn")]
    pub trace_on: bool,
    #[serde(rename = "enableCalcFilterBitMap")]
    pub enable_calc_filter_bit_map: bool,
    #[serde(rename = "expectConsumerNumUseFilter")]
    pub expect_consumer_num_use_filter: i32,
    #[serde(rename = "maxErrorRateOfBloomFilter")]
    pub max_error_rate_of_bloom_filter: i32,
    #[serde(rename = "filterDataCleanTimeSpan")]
    pub filter_data_clean_time_span: i64,
    #[serde(rename = "filterSupportRetry")]
    pub filter_support_retry: bool,
    #[serde(rename = "enablePropertyFilter")]
    pub enable_property_filter: bool,
    #[serde(rename = "authSwitchEnabled")]
    pub auth_switch_enabled: bool,
    #[serde(rename = "trackEnabled")]
    pub track_enabled: bool,
    #[serde(rename = "compressedRegister")]
    pub compressed_register: bool,
    #[serde(rename = "forceRegister")]
    pub force_register: bool,
    #[serde(rename = "registerNameServerPeriod")]
    pub register_name_server_period: i32,
    #[serde(rename = "transactionTimeOut")]
    pub transaction_time_out: i64,
    #[serde(rename = "transactionCheckMax")]
    pub transaction_check_max: i32,
    #[serde(rename = "transactionCheckInterval")]
    pub transaction_check_interval: i64,
    #[serde(rename = "aclEnable")]
    pub acl_enable: bool,
    #[serde(rename = "storeReplyMessageEnable")]
    pub store_reply_message_enable: bool,
    /// New properties, including SDK maintenance replyMessageProducerGroups, remain intact.
    #[serde(flatten)]
    pub extra_properties: HashMap<CheetahString, CheetahString>,
}

impl fmt::Debug for BrokerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerConfig")
            .field("known_properties", &BROKER_CONFIG_PROPERTIES.len())
            .field("extra_properties", &self.extra_properties.len())
            .finish()
    }
}

impl BrokerConfig {
    pub fn local_host_name() -> CheetahString {
        sysinfo::System::host_name()
            .unwrap_or_else(|| "DEFAULT_BROKER".to_string())
            .into()
    }
    pub fn from_process_defaults() -> ClientResult<Self> {
        Ok(Self::with_defaults(&BrokerConfigDefaults::from_process()?))
    }
    pub fn with_defaults(context: &BrokerConfigDefaults) -> Self {
        Self {
            rocketmq_home: context.rocketmq_home.clone(),
            namesrv_addr: context.namesrv_addr.clone(),
            broker_ip1: context.local_address.clone(),
            broker_ip2: context.local_address.clone(),
            broker_name: context.host_name.clone(),
            broker_cluster_name: CheetahString::from_static_str("DefaultCluster"),
            broker_id: 0,
            broker_permission: 6,
            default_topic_queue_nums: 8,
            auto_create_topic_enable: true,
            cluster_topic_enable: true,
            broker_topic_enable: true,
            auto_create_subscription_group: true,
            message_store_plug_in: CheetahString::from_static_str(""),
            msg_trace_topic_name: CheetahString::from_static_str("RMQ_SYS_TRACE_TOPIC"),
            trace_topic_enable: false,
            send_message_thread_pool_nums: 1,
            pull_message_thread_pool_nums: 16 + context.logical_processors * 2,
            process_reply_message_thread_pool_nums: 16 + context.logical_processors * 2,
            query_message_thread_pool_nums: 8 + context.logical_processors,
            admin_broker_thread_pool_nums: 16,
            client_manage_thread_pool_nums: 32,
            consumer_manage_thread_pool_nums: 32,
            report_stage_thread_pool_nums: 32,
            heartbeat_thread_pool_nums: context.logical_processors.min(32),
            end_transaction_thread_pool_nums: 8 + context.logical_processors * 2,
            flush_consumer_offset_interval: 5000,
            flush_consumer_offset_history_interval: 60000,
            reject_transaction_message: false,
            fetch_namesrv_addr_by_address_server: false,
            send_thread_pool_queue_capacity: 10000,
            pull_thread_pool_queue_capacity: 100000,
            reply_thread_pool_queue_capacity: 10000,
            query_thread_pool_queue_capacity: 20000,
            client_manager_thread_pool_queue_capacity: 1000000,
            consumer_manager_thread_pool_queue_capacity: 1000000,
            report_stage_thread_pool_queue_capacity: 10000,
            heartbeat_thread_pool_queue_capacity: 50000,
            end_transaction_pool_queue_capacity: 100000,
            cluster_id: context.cluster_id,
            auth_service_domain: context.auth_service_domain.clone(),
            filter_server_nums: 0,
            long_polling_enable: true,
            short_polling_time_mills: 1000,
            notify_consumer_ids_changed_enable: true,
            high_speed_mode: false,
            commercial_enable: true,
            commercial_timer_count: 1,
            commercial_trans_count: 1,
            commercial_big_count: 1,
            commercial_base_count: 1,
            transfer_msg_by_heap: true,
            max_delay_time: 40,
            region_id: CheetahString::from_static_str("DefaultRegion"),
            register_broker_timeout_mills: 6000,
            slave_read_enable: false,
            disable_consume_if_consumer_read_slowly: false,
            consumer_fallbehind_threshold: 17179869184,
            broker_fast_failure_enable: true,
            wait_time_mills_in_send_queue: 200,
            wait_time_mills_in_pull_queue: 5000,
            wait_time_mills_in_heartbeat_queue: 31000,
            wait_time_mills_in_transaction_queue: 3000,
            start_accept_send_request_time_stamp: 0,
            trace_on: true,
            enable_calc_filter_bit_map: false,
            expect_consumer_num_use_filter: 32,
            max_error_rate_of_bloom_filter: 20,
            filter_data_clean_time_span: 86400000,
            filter_support_retry: false,
            enable_property_filter: false,
            auth_switch_enabled: true,
            track_enabled: false,
            compressed_register: false,
            force_register: true,
            register_name_server_period: 30000,
            transaction_time_out: 6000,
            transaction_check_max: 15,
            transaction_check_interval: 60000,
            acl_enable: false,
            store_reply_message_enable: true,
            extra_properties: HashMap::new(),
        }
    }

    pub fn to_properties(&self) -> ClientResult<HashMap<CheetahString, CheetahString>> {
        let value = serde_json::to_value(self)
            .map_err(|error| ClientError::response_process_source("encode BrokerConfig", error))?;
        Ok(value
            .as_object()
            .expect("BrokerConfig serializes as an object")
            .iter()
            .filter(|(_, value)| !value.is_null())
            .map(|(key, value)| {
                (
                    key.clone().into(),
                    value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string())
                        .into(),
                )
            })
            .collect())
    }

    /// Apply all keys atomically. Java Boolean.parseBoolean is true only for case-insensitive "true".
    pub fn apply_properties(&mut self, properties: &HashMap<CheetahString, CheetahString>) -> ClientResult<()> {
        let mut json = serde_json::to_value(&*self)
            .map_err(|error| ClientError::response_process_source("encode BrokerConfig", error))?;
        let values = json.as_object_mut().expect("BrokerConfig serializes as an object");
        for (name, value) in properties {
            let kind = BROKER_CONFIG_PROPERTIES
                .iter()
                .find(|property| property.name == name.as_str())
                .map(|property| property.kind);
            let parsed = match kind {
                Some(BrokerPropertyKind::I32) => serde_json::Value::from(
                    value
                        .parse::<i32>()
                        .map_err(|error| ClientError::config_parse_source(name.as_str(), error))?,
                ),
                Some(BrokerPropertyKind::I64) => serde_json::Value::from(
                    value
                        .parse::<i64>()
                        .map_err(|error| ClientError::config_parse_source(name.as_str(), error))?,
                ),
                Some(BrokerPropertyKind::Boolean) => serde_json::Value::Bool(value.eq_ignore_ascii_case("true")),
                _ => serde_json::Value::String(value.to_string()),
            };
            values.insert(name.to_string(), parsed);
        }
        let updated = serde_json::from_value(json)
            .map_err(|error| ClientError::response_process_source("decode BrokerConfig", error))?;
        *self = updated;
        Ok(())
    }
}
