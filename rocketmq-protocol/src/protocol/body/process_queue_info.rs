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

// Custom fork: preserve Java signed beans, field spelling and diagnostic display.
use chrono::{Local, TimeZone};
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessQueueInfo {
    pub commit_offset: i64,
    pub cached_msg_min_offset: i64,
    pub cached_msg_max_offset: i64,
    pub cached_msg_count: i32,
    #[serde(rename = "cachedMsgSizeInMiB")]
    pub cached_msg_size_in_mib: i32,

    pub transaction_msg_min_offset: i64,
    pub transaction_msg_max_offset: i64,
    pub transaction_msg_count: i32,

    pub locked: bool,
    pub try_unlock_times: i64,
    pub last_lock_timestamp: i64,

    pub droped: bool,
    pub last_pull_timestamp: i64,
    pub last_consume_timestamp: i64,
}

impl std::fmt::Display for ProcessQueueInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ProcessQueueInfo [commitOffset={}, cachedMsgMinOffset={}, cachedMsgMaxOffset={}, \
             cachedMsgCount={}, cachedMsgSizeInMiB={}, transactionMsgMinOffset={}, \
             transactionMsgMaxOffset={}, transactionMsgCount={}, locked={}, tryUnlockTimes={}, \
             lastLockTimestamp={}, droped={}, lastPullTimestamp={}, lastConsumeTimestamp={}]",
            self.commit_offset,
            self.cached_msg_min_offset,
            self.cached_msg_max_offset,
            self.cached_msg_count,
            self.cached_msg_size_in_mib,
            self.transaction_msg_min_offset,
            self.transaction_msg_max_offset,
            self.transaction_msg_count,
            self.locked,
            self.try_unlock_times,
            human_time(self.last_lock_timestamp),
            self.droped,
            human_time(self.last_pull_timestamp),
            human_time(self.last_consume_timestamp)
        )
    }
}

fn human_time(timestamp: i64) -> String {
    Local
        .timestamp_millis_opt(timestamp)
        .single()
        .map(|time| time.format("%Y%m%d%H%M%S%3f").to_string())
        .unwrap_or_else(|| timestamp.to_string())
}


