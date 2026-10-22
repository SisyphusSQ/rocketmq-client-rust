// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

//! Complete Java query parameters and explicit per-Broker partial results.
use crate::base::query_result::QueryResult;
use crate::{ClientError, ClientResult};
use cheetah_string::CheetahString;

#[derive(Clone, Debug)]
pub struct MQAdminQueryOptions {
    pub topic: CheetahString,
    pub key: CheetahString,
    pub max_num: i32,
    pub begin_timestamp: i64,
    pub end_timestamp: i64,
    pub unique_key: bool,
}
impl MQAdminQueryOptions {
    pub fn new(
        topic: impl Into<CheetahString>,
        key: impl Into<CheetahString>,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Self {
        Self {
            topic: topic.into(),
            key: key.into(),
            max_num,
            begin_timestamp: begin,
            end_timestamp: end,
            unique_key: false,
        }
    }
    pub fn unique_key(mut self, unique: bool) -> Self {
        self.unique_key = unique;
        self
    }
}

#[derive(Clone, Debug)]
pub struct MQAdminQueryFailure {
    pub broker_address: CheetahString,
    pub error: ClientError,
}

/// Java can return records from successful Brokers after another query fails.
/// This native result retains both; the convenience result requires completeness.
#[derive(Debug)]
pub struct MQAdminQueryReport {
    pub result: QueryResult,
    pub brokers_queried: usize,
    pub brokers_completed: usize,
    pub brokers_empty: usize,
    pub failures: Vec<MQAdminQueryFailure>,
    pub deadline_reached: bool,
}
impl MQAdminQueryReport {
    pub fn is_complete(&self) -> bool {
        self.failures.is_empty() && !self.deadline_reached
    }
    pub fn into_complete_result(self) -> ClientResult<QueryResult> {
        // Permission/authentication must remain explicit even when another shard
        // fails differently; the detailed report still preserves every failure.
        let decisive = self
            .failures
            .iter()
            .find(|failure| {
                matches!(
                    failure.error.failure_kind(),
                    crate::ClientFailureKind::PermissionDenied | crate::ClientFailureKind::Authentication
                )
            })
            .or_else(|| self.failures.first());
        if let Some(failure) = decisive {
            return Err(failure.error.clone());
        }
        if self.deadline_reached {
            return Err(ClientError::timeout("queryMessage aggregate", 0));
        }
        if self.result.message_list().is_empty() {
            return Err(ClientError::broker_operation_failed(
                "queryMessage",
                rocketmq_protocol::code::response_code::ResponseCode::NoMessage as i32,
                "query finished without a matching message",
            ));
        }
        Ok(self.result)
    }
}
