// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.
//! Custom 4.6.1 request-code 1000 tracking. Independent of Apache Trace Topic.
use cheetah_string::CheetahString;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TrackType {
    Sub,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i32)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TrackMsgType {
    NormalMsg = 0,
    TransactionHalfMsg = 1,
    TransactionCommitMsg = 2,
    TransactionRollbackMsg = 3,
    DelayTimeLevelMsg = 4,
    DelayTimeInSecondsMsg = 5,
    RetryMsg = 6,
    ConsumerSendBackMsg = 7,
}
impl TryFrom<i32> for TrackMsgType {
    type Error = TrackCodecError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::NormalMsg),
            1 => Ok(Self::TransactionHalfMsg),
            2 => Ok(Self::TransactionCommitMsg),
            3 => Ok(Self::TransactionRollbackMsg),
            4 => Ok(Self::DelayTimeLevelMsg),
            5 => Ok(Self::DelayTimeInSecondsMsg),
            6 => Ok(Self::RetryMsg),
            7 => Ok(Self::ConsumerSendBackMsg),
            _ => Err(TrackCodecError("message type ordinal")),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackBean {
    pub msg_id: Option<CheetahString>,
    pub offset_msg_id: Option<CheetahString>,
    pub retry_times: Option<CheetahString>,
    pub status: Option<CheetahString>,
}
impl TrackBean {
    pub const STATUS_OF_SUCCESS: &'static str = "1";
    pub const STATUS_OF_FAILURE: &'static str = "0";
}
impl Default for TrackBean {
    fn default() -> Self {
        Self {
            msg_id: Some("".into()),
            offset_msg_id: Some("".into()),
            retry_times: None,
            status: None,
        }
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackContext {
    pub group_name: Option<CheetahString>,
    pub topic: Option<CheetahString>,
    pub broker_name: Option<CheetahString>,
    pub host: Option<CheetahString>,
    pub start_time: Option<CheetahString>,
    pub track_type: Option<TrackType>,
    pub cost_time: Option<CheetahString>,
    pub status: Option<CheetahString>,
    pub return_type_code: Option<CheetahString>,
    pub track_beans: Vec<TrackBean>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackTransferBean {
    pub trans_data: Option<CheetahString>,
    pub host: Option<CheetahString>,
    pub broker_name: Option<CheetahString>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrackCodecError(pub &'static str);
impl std::fmt::Display for TrackCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid Custom track {}", self.0)
    }
}
impl std::error::Error for TrackCodecError {}

pub struct TrackEncoder;
impl TrackEncoder {
    pub const CONTENT_SEPARATOR: char = '\u{1}';
    pub const FIELD_SEPARATOR: char = '\u{2}';
    pub const HEADER_SEPARATOR: char = '\u{3}';
    pub const MESSAGE_SEPARATOR: char = '\u{4}';
    pub fn encode(context: &TrackContext) -> Result<String, TrackCodecError> {
        if context.track_type != Some(TrackType::Sub) {
            return Err(TrackCodecError("context type"));
        }
        let mut encoded = String::from("SUB\u{1}");
        for field in [
            &context.topic,
            &context.cost_time,
            &context.start_time,
            &context.return_type_code,
            &context.status,
            &context.group_name,
            &context.return_type_code,
        ] {
            Self::push_field(&mut encoded, field)?;
        }
        encoded.push(Self::HEADER_SEPARATOR);
        for message in &context.track_beans {
            for field in [
                &message.msg_id,
                &message.retry_times,
                &message.offset_msg_id,
                &message.status,
            ] {
                Self::push_field(&mut encoded, field)?;
            }
            encoded.push(Self::MESSAGE_SEPARATOR);
        }
        encoded.push(Self::FIELD_SEPARATOR);
        Ok(encoded)
    }
    fn push_field(encoded: &mut String, field: &Option<CheetahString>) -> Result<(), TrackCodecError> {
        let value = java_text(field);
        // This wire format has no escaping. Embedded delimiters cannot represent
        // one field and would otherwise forge extra records at the target decoder.
        if value.bytes().any(|byte| (1..=4).contains(&byte)) {
            return Err(TrackCodecError("unescaped separator in field"));
        }
        encoded.push_str(value);
        encoded.push(Self::CONTENT_SEPARATOR);
        Ok(())
    }
    pub fn generate(contexts: &[TrackContext]) -> Result<Option<TrackTransferBean>, TrackCodecError> {
        if contexts.is_empty() {
            return Ok(None);
        }
        let mut encoded = String::new();
        for context in contexts {
            encoded.push_str(&Self::encode(context)?);
        }
        Ok(Some(TrackTransferBean {
            trans_data: Some(encoded.into()),
            host: contexts.last().unwrap().host.clone(),
            broker_name: None,
        }))
    }
    /// Valid Java records decode identically. Malformed/truncated records return
    /// an error instead of reproducing Java's unchecked array/null failures.
    pub fn decode(data: &str) -> Result<Vec<TrackContext>, TrackCodecError> {
        let mut result = Vec::new();
        for field in java_split(data, Self::FIELD_SEPARATOR) {
            if java_blank(field) {
                continue;
            }
            let mut parts = field.split(Self::HEADER_SEPARATOR);
            let header = parts.next().unwrap_or_default();
            let body = parts.next().ok_or(TrackCodecError("header separator"))?;
            if java_blank(header) || java_blank(body) {
                continue;
            }
            if parts.next().is_some() {
                return Err(TrackCodecError("header count"));
            }
            let fields = java_split(header, Self::CONTENT_SEPARATOR);
            if fields.len() < 8 || fields[0] != "SUB" {
                return Err(TrackCodecError("header fields"));
            }
            let mut context = TrackContext {
                track_type: Some(TrackType::Sub),
                topic: Some(fields[1].into()),
                cost_time: Some(fields[2].into()),
                start_time: Some(fields[3].into()),
                status: Some(fields[5].into()),
                group_name: Some(fields[6].into()),
                return_type_code: Some(fields[7].into()),
                ..Default::default()
            };
            for message in java_split(body, Self::MESSAGE_SEPARATOR) {
                if java_blank(message) {
                    continue;
                }
                let fields = java_split(message, Self::CONTENT_SEPARATOR);
                if fields.len() < 4 {
                    return Err(TrackCodecError("message fields"));
                }
                context.track_beans.push(TrackBean {
                    msg_id: Some(fields[0].into()),
                    retry_times: (!java_blank(fields[1])).then(|| fields[1].into()),
                    offset_msg_id: Some(fields[2].into()),
                    status: Some(fields[3].into()),
                });
            }
            result.push(context);
        }
        Ok(result)
    }
}
fn java_split(value: &str, separator: char) -> Vec<&str> {
    let mut parts: Vec<_> = value.split(separator).collect();
    while parts.last() == Some(&"") {
        parts.pop();
    }
    parts
}
fn java_blank(value: &str) -> bool {
    value.trim_matches(|c: char| c <= '\u{20}').is_empty()
}
fn java_text(value: &Option<CheetahString>) -> &str {
    value.as_ref().map_or("null", |value| value.as_str())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeStageRecord {
    pub consumer_group: Option<CheetahString>,
    pub host: Option<CheetahString>,
    pub create_time: Option<CheetahString>,
    pub cost_time: Option<CheetahString>,
    pub status: Option<CheetahString>,
    pub retry_times: Option<CheetahString>,
    pub topic: Option<CheetahString>,
    pub msg_id: Option<CheetahString>,
    pub offset_msg_id: Option<CheetahString>,
    pub return_type_code: Option<CheetahString>,
}
impl Default for ConsumeStageRecord {
    fn default() -> Self {
        Self {
            consumer_group: None,
            host: None,
            create_time: None,
            cost_time: None,
            status: None,
            retry_times: None,
            topic: None,
            msg_id: None,
            offset_msg_id: None,
            return_type_code: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProduceStageRecord {
    pub producer_group: Option<CheetahString>,
    pub host: Option<CheetahString>,
    pub create_time: i64,
    pub message_id: Option<CheetahString>,
    pub offset_msg_id: Option<CheetahString>,
    pub topic: Option<CheetahString>,
    pub code: i32,
    pub msg_type: i32,
    pub delay_time: Option<CheetahString>,
    pub key: Option<CheetahString>,
    pub tag: Option<CheetahString>,
    pub remark: Option<CheetahString>,
}
impl Default for ProduceStageRecord {
    fn default() -> Self {
        Self {
            producer_group: None,
            host: None,
            create_time: 0,
            message_id: None,
            offset_msg_id: None,
            topic: None,
            code: 0,
            msg_type: 0,
            delay_time: Some("".into()),
            key: None,
            tag: None,
            remark: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreStageRecord {
    pub broker: Option<CheetahString>,
    pub broker_id: Option<CheetahString>,
    pub host: Option<CheetahString>,
    pub start_time: i64,
    pub cost_time: i64,
    pub msg_id: Option<CheetahString>,
    pub topic: Option<CheetahString>,
    pub queue_id: Option<CheetahString>,
    pub offset_msg_id: Option<CheetahString>,
    pub msg_type: i32,
    put_result: Option<CheetahString>,
    pub retry_times: i32,
}
impl Default for StoreStageRecord {
    fn default() -> Self {
        Self {
            broker: None,
            broker_id: None,
            host: None,
            start_time: 0,
            cost_time: 0,
            msg_id: None,
            topic: None,
            queue_id: None,
            offset_msg_id: None,
            msg_type: -1,
            put_result: Some("".into()),
            retry_times: 0,
        }
    }
}

impl StoreStageRecord {
    pub fn put_result(&self) -> Option<&CheetahString> {
        self.put_result.as_ref()
    }
    /// Java deliberately leaves PUT_OK as the empty default.
    pub fn set_put_result(&mut self, result: Option<CheetahString>) {
        if result.as_deref() != Some("PUT_OK") {
            self.put_result = result;
        }
    }
}
