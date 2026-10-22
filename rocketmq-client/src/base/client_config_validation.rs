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

//! Client configuration validation module
//!
//! This module provides validation functions for ClientConfig fields,
//! ensuring that configuration values are within acceptable bounds and
//! properly formatted.

use crate::ClientError;
use crate::ClientResult;

use crate::base::client_config::ClientConfig;

/// Validator for ClientConfig fields
///
/// Provides validation methods for all configuration parameters,
/// ensuring they meet RocketMQ requirements.
pub struct ClientConfigValidator;

impl ClientConfigValidator {
    // =========================================================================
    // Validation Constants
    // =========================================================================

    /// Custom: Java scheduled periods accept any positive millisecond value.
    pub const MIN_POLL_NAME_SERVER_INTERVAL: u32 = 1;

    /// Maximum representable Java int period.
    pub const MAX_POLL_NAME_SERVER_INTERVAL: u32 = i32::MAX as u32;

    /// Minimum positive heartbeat period in milliseconds.
    pub const MIN_HEARTBEAT_BROKER_INTERVAL: u32 = 1;

    /// Maximum representable Java int period.
    pub const MAX_HEARTBEAT_BROKER_INTERVAL: u32 = i32::MAX as u32;

    /// Minimum positive persistence period in milliseconds.
    pub const MIN_PERSIST_CONSUMER_OFFSET_INTERVAL: u32 = 1;

    /// Maximum representable Java int period.
    pub const MAX_PERSIST_CONSUMER_OFFSET_INTERVAL: u32 = i32::MAX as u32;

    /// Minimum trace message batch number
    pub const MIN_TRACE_MSG_BATCH_NUM: usize = 1;

    /// Maximum trace message batch number
    pub const MAX_TRACE_MSG_BATCH_NUM: usize = 10_000;

    /// Minimum max page size in get metadata
    pub const MIN_MAX_PAGE_SIZE_IN_GET_METADATA: usize = 1;

    /// Maximum max page size in get metadata
    pub const MAX_MAX_PAGE_SIZE_IN_GET_METADATA: usize = 100_000;

    /// Minimum MQ client API timeout (100ms)
    pub const MIN_MQ_CLIENT_API_TIMEOUT: u64 = 100;

    /// Maximum MQ client API timeout (60 seconds)
    pub const MAX_MQ_CLIENT_API_TIMEOUT: u64 = 60_000;

    /// Maximum PullMessageService shard multiplier.
    pub const MAX_PULL_MESSAGE_SERVICE_SHARDS_PER_CPU: usize = 8;

    // =========================================================================
    // Validation Methods
    // =========================================================================

    /// Validates every runtime-affecting public client setting.
    pub fn validate_config(config: &ClientConfig) -> ClientResult<()> {
        config.validate_remoting_configuration()?;
        config.authentication.validate().map_err(ClientError::from)?;
        // Custom fork: reject newer wire names before admitting a client into the pool.
        if rocketmq_protocol::protocol::LanguageCode::java_4_6_1_value_of(config.language.get_code()).is_none() {
            return Err(ClientError::config_invalid(
                "language",
                config.language.to_string(),
                "not supported by Java 4.6.1; use OTHER for Rust",
            ));
        }
        if config.client_async_semaphore_value > i32::MAX as u32
            || config.client_oneway_semaphore_value > i32::MAX as u32
        {
            return Err(ClientError::config_invalid(
                "client_semaphore_value",
                "out of range",
                "must fit Java nonnegative int",
            ));
        }
        Self::validate_poll_name_server_interval(config.poll_name_server_interval)?;
        Self::validate_heartbeat_broker_interval(config.heartbeat_broker_interval)?;
        Self::validate_persist_consumer_offset_interval(config.persist_consumer_offset_interval)?;
        Self::validate_mq_client_api_timeout(config.mq_client_api_timeout)?;
        Self::validate_trace_msg_batch_num(config.trace_msg_batch_num)?;
        Self::validate_max_page_size_in_get_metadata(config.max_page_size_in_get_metadata)?;
        Self::validate_client_callback_executor_threads(config.client_callback_executor_threads)?;
        Self::validate_pull_message_service_shards(config.pull_message_service_shards)?;
        if config.enable_concurrent_heartbeat {
            Self::validate_concurrent_heartbeat_thread_pool_size(config.concurrent_heartbeat_thread_pool_size)?;
        }
        Ok(())
    }

    /// Validate poll name server interval
    ///
    /// Uses the positive Java int range required by scheduleAtFixedRate.
    pub fn validate_poll_name_server_interval(interval: u32) -> ClientResult<()> {
        if !(Self::MIN_POLL_NAME_SERVER_INTERVAL..=Self::MAX_POLL_NAME_SERVER_INTERVAL).contains(&interval) {
            return Err(ClientError::config_invalid(
                "poll_name_server_interval",
                interval.to_string(),
                format!(
                    "must be between {} and {} milliseconds",
                    Self::MIN_POLL_NAME_SERVER_INTERVAL,
                    Self::MAX_POLL_NAME_SERVER_INTERVAL
                ),
            ));
        }
        Ok(())
    }

    /// Validate heartbeat broker interval
    ///
    /// Uses the positive Java int range required by scheduleAtFixedRate.
    pub fn validate_heartbeat_broker_interval(interval: u32) -> ClientResult<()> {
        if !(Self::MIN_HEARTBEAT_BROKER_INTERVAL..=Self::MAX_HEARTBEAT_BROKER_INTERVAL).contains(&interval) {
            return Err(ClientError::config_invalid(
                "heartbeat_broker_interval",
                interval.to_string(),
                format!(
                    "must be between {} and {} milliseconds",
                    Self::MIN_HEARTBEAT_BROKER_INTERVAL,
                    Self::MAX_HEARTBEAT_BROKER_INTERVAL
                ),
            ));
        }
        Ok(())
    }

    /// Validate persist consumer offset interval
    ///
    /// Uses the positive Java int range required by scheduleAtFixedRate.
    pub fn validate_persist_consumer_offset_interval(interval: u32) -> ClientResult<()> {
        if !(Self::MIN_PERSIST_CONSUMER_OFFSET_INTERVAL..=Self::MAX_PERSIST_CONSUMER_OFFSET_INTERVAL)
            .contains(&interval)
        {
            return Err(ClientError::config_invalid(
                "persist_consumer_offset_interval",
                interval.to_string(),
                format!(
                    "must be between {} and {} milliseconds",
                    Self::MIN_PERSIST_CONSUMER_OFFSET_INTERVAL,
                    Self::MAX_PERSIST_CONSUMER_OFFSET_INTERVAL
                ),
            ));
        }
        Ok(())
    }

    /// Validate trace message batch number
    ///
    /// Ensures the batch number is between 1 and 10,000.
    pub fn validate_trace_msg_batch_num(num: usize) -> ClientResult<()> {
        if !(Self::MIN_TRACE_MSG_BATCH_NUM..=Self::MAX_TRACE_MSG_BATCH_NUM).contains(&num) {
            return Err(ClientError::config_invalid(
                "trace_msg_batch_num",
                num.to_string(),
                format!(
                    "must be between {} and {}",
                    Self::MIN_TRACE_MSG_BATCH_NUM,
                    Self::MAX_TRACE_MSG_BATCH_NUM
                ),
            ));
        }
        Ok(())
    }

    /// Validate max page size in get metadata
    ///
    /// Ensures the page size is between 1 and 100,000.
    pub fn validate_max_page_size_in_get_metadata(size: usize) -> ClientResult<()> {
        if !(Self::MIN_MAX_PAGE_SIZE_IN_GET_METADATA..=Self::MAX_MAX_PAGE_SIZE_IN_GET_METADATA).contains(&size) {
            return Err(ClientError::config_invalid(
                "max_page_size_in_get_metadata",
                size.to_string(),
                format!(
                    "must be between {} and {}",
                    Self::MIN_MAX_PAGE_SIZE_IN_GET_METADATA,
                    Self::MAX_MAX_PAGE_SIZE_IN_GET_METADATA
                ),
            ));
        }
        Ok(())
    }

    /// Validate MQ client API timeout
    ///
    /// Ensures the timeout is between 100ms and 60 seconds.
    pub fn validate_mq_client_api_timeout(timeout: u64) -> ClientResult<()> {
        if !(Self::MIN_MQ_CLIENT_API_TIMEOUT..=Self::MAX_MQ_CLIENT_API_TIMEOUT).contains(&timeout) {
            return Err(ClientError::config_invalid(
                "mq_client_api_timeout",
                timeout.to_string(),
                format!(
                    "must be between {} and {} milliseconds",
                    Self::MIN_MQ_CLIENT_API_TIMEOUT,
                    Self::MAX_MQ_CLIENT_API_TIMEOUT
                ),
            ));
        }
        Ok(())
    }

    /// Validate concurrent heartbeat thread pool size
    ///
    /// Ensures the thread pool size is greater than 0 and doesn't exceed
    /// a reasonable maximum (CPU cores * 4).
    pub fn validate_concurrent_heartbeat_thread_pool_size(size: usize) -> ClientResult<()> {
        if size == 0 {
            return Err(ClientError::config_invalid(
                "concurrent_heartbeat_thread_pool_size",
                size.to_string(),
                "must be greater than 0",
            ));
        }

        // Get CPU count with a reasonable default if num_cpus fails
        #[allow(clippy::redundant_closure)]
        let cpu_count = std::panic::catch_unwind(|| num_cpus::get()).unwrap_or(4);
        let max_size = cpu_count * 4;

        if size > max_size {
            return Err(ClientError::config_invalid(
                "concurrent_heartbeat_thread_pool_size",
                size.to_string(),
                format!("should not exceed {} (CPU cores * 4)", max_size),
            ));
        }

        Ok(())
    }

    /// Validate client callback executor threads
    ///
    /// Zero uses Java NettyRemotingClient's fallback of four callback workers.
    pub fn validate_client_callback_executor_threads(threads: usize) -> ClientResult<()> {
        // Custom: Java accepts positive executor sizes independent of CPU count.
        if threads > i32::MAX as usize || threads > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(ClientError::config_invalid(
                "client_callback_executor_threads",
                threads.to_string(),
                "exceeds Java int or native semaphore capacity",
            ));
        }

        Ok(())
    }

    /// Validate PullMessageService shard count.
    ///
    /// Ensures the shard count is greater than 0 and stays within a reasonable
    /// CPU-scaled upper bound.
    pub fn validate_pull_message_service_shards(shards: usize) -> ClientResult<()> {
        if shards == 0 {
            return Err(ClientError::config_invalid(
                "pull_message_service_shards",
                shards.to_string(),
                "must be greater than 0",
            ));
        }

        #[allow(clippy::redundant_closure)]
        let cpu_count = std::panic::catch_unwind(|| num_cpus::get()).unwrap_or(4);
        let max_shards = cpu_count * Self::MAX_PULL_MESSAGE_SERVICE_SHARDS_PER_CPU;

        if shards > max_shards {
            return Err(ClientError::config_invalid(
                "pull_message_service_shards",
                shards.to_string(),
                format!("should not exceed {} (CPU cores * 8)", max_shards),
            ));
        }

        Ok(())
    }
}


