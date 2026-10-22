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

use std::fmt::Display;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::cm_result::CMResult;

// Custom fork: Java callback JSON uses camelCase and a signed long duration.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ConsumeMessageDirectlyResult {
    order: bool,
    auto_commit: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    consume_result: Option<CMResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remark: Option<CheetahString>,
    spent_time_mills: i64,
}

impl Default for ConsumeMessageDirectlyResult {
    #[inline]
    fn default() -> Self {
        Self {
            order: false,
            auto_commit: true,
            consume_result: None,
            remark: None,
            spent_time_mills: 0,
        }
    }
}

impl Display for ConsumeMessageDirectlyResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConsumeMessageDirectlyResult [order={}, autoCommit={}, consumeResult={}, remark={}, spentTimeMills={}]",
            self.order,
            self.auto_commit,
            self.consume_result
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "null".into()),
            self.remark.as_deref().unwrap_or("null"),
            self.spent_time_mills
        )
    }
}

impl ConsumeMessageDirectlyResult {
    pub fn new(
        order: bool,
        auto_commit: bool,
        consume_result: CMResult,
        remark: CheetahString,
        spent_time_mills: i64,
    ) -> Self {
        Self {
            order,
            auto_commit,
            consume_result: Some(consume_result),
            remark: Some(remark),
            spent_time_mills,
        }
    }

    #[inline]
    pub fn order(&self) -> bool {
        self.order
    }

    #[inline]
    pub fn set_order(&mut self, order: bool) {
        self.order = order;
    }

    #[inline]
    pub fn auto_commit(&self) -> bool {
        self.auto_commit
    }

    #[inline]
    pub fn set_auto_commit(&mut self, auto_commit: bool) {
        self.auto_commit = auto_commit;
    }

    #[inline]
    pub fn consume_result(&self) -> Option<&CMResult> {
        self.consume_result.as_ref()
    }

    #[inline]
    pub fn set_consume_result(&mut self, consume_result: impl Into<Option<CMResult>>) {
        self.consume_result = consume_result.into();
    }

    #[inline]
    pub fn remark(&self) -> Option<&CheetahString> {
        self.remark.as_ref()
    }

    #[inline]
    pub fn set_remark(&mut self, remark: impl Into<Option<CheetahString>>) {
        self.remark = remark.into();
    }

    #[inline]
    pub fn spent_time_mills(&self) -> i64 {
        self.spent_time_mills
    }

    #[inline]
    pub fn set_spent_time_mills(&mut self, spent_time_mills: i64) {
        self.spent_time_mills = spent_time_mills;
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::body::cm_result::CMResult;

    #[test]
    fn null_setters_restore_java_unset_beans_and_signed_duration() {
        let mut result = ConsumeMessageDirectlyResult::new(false, true, CMResult::CRSuccess, "remark".into(), 17);
        result.set_consume_result(None);
        result.set_remark(None);
        result.set_spent_time_mills(-1);
        assert_eq!(
            serde_json::to_value(result).unwrap(),
            serde_json::json!({"order":false,"autoCommit":true,"spentTimeMills":-1})
        );
    }

    #[test]
    fn consume_message_directly_result_default_initializes_correctly() {
        let result = ConsumeMessageDirectlyResult::default();
        assert!(!result.order());
        assert!(result.auto_commit());
        assert!(result.consume_result().is_none());
        assert!(result.remark().is_none());
        assert_eq!(result.spent_time_mills(), 0);
    }

    #[test]
    fn consume_message_directly_result_new_initializes_correctly() {
        let consume_result = CMResult::default();
        let remark = CheetahString::from_static_str("test remark");
        let result = ConsumeMessageDirectlyResult::new(true, false, consume_result, remark.clone(), 12345);
        assert!(result.order());
        assert!(!result.auto_commit());
        assert_eq!(result.consume_result().unwrap(), &consume_result);
        assert_eq!(result.remark(), Some(&remark));
        assert_eq!(result.spent_time_mills(), 12345);
    }

    #[test]
    fn consume_message_directly_result_setters_work_correctly() {
        let mut result = ConsumeMessageDirectlyResult::default();
        result.set_order(true);
        result.set_auto_commit(false);
        let consume_result = CMResult::default();
        result.set_consume_result(consume_result);
        let remark = CheetahString::from_static_str("updated remark");
        result.set_remark(remark.clone());
        result.set_spent_time_mills(67890);

        assert!(result.order());
        assert!(!result.auto_commit());
        assert_eq!(result.consume_result().unwrap(), &consume_result);
        assert_eq!(result.remark(), Some(&remark));
        assert_eq!(result.spent_time_mills(), 67890);
    }

    #[test]
    fn consume_message_directly_result_display_formats_correctly() {
        let consume_result = CMResult::default();
        let remark = CheetahString::from_static_str("test remark");
        let result = ConsumeMessageDirectlyResult::new(true, false, consume_result, remark.clone(), 12345);
        let display = format!("{}", result);
        let expected = "ConsumeMessageDirectlyResult [order=true, autoCommit=false, consumeResult=CR_SUCCESS, remark=test remark, spentTimeMills=12345]";
        assert_eq!(display, expected);
    }
}
