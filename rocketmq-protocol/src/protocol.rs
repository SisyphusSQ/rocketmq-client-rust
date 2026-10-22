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

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

use bytes::BytesMut;
use cheetah_string::CheetahString;
use serde::ser::SerializeStruct;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;

use self::rocketmq_serializable::RocketMQSerializable;

pub mod admin;
pub mod bodies;
pub mod body;
pub mod broker_sync_info;
pub mod command_custom_header;
pub mod encoded_frame;
pub mod filter;
pub mod forbidden_type;
pub mod header;
pub mod header_codec;
pub mod header_facade;
pub(crate) mod header_field_merge;
pub mod headers;
pub mod heartbeat;
pub mod heartbeat_facade;
pub mod namespace_util;
pub mod namesrv;
pub mod remoting_command;
pub mod remoting_command_compat;
pub mod remoting_command_defaults;
pub mod request_source;
pub mod request_type;
pub mod rocketmq_serializable;
pub mod route;
pub mod route_facade;
pub mod static_topic;
pub mod subscription;
pub mod topic;

pub use command_custom_header::CommandCustomHeader;
pub use command_custom_header::FromMap;
pub use command_custom_header::HeaderEncodeCapability;
pub use data_version_compat as data_version_facade;
pub use encoded_frame::EncodedFrame;
pub use encoded_frame::EncodedFrameHead;
pub use remoting_command::RemotingCommand;
pub use remoting_command_compat as remoting_command_facade;

pub mod data_version_compat;

#[derive(Debug)]
pub struct DataVersion {
    state_version: i64,
    timestamp: i64,
    counter: AtomicI64,
    // Custom: Java AtomicLong may be explicitly null on the wire.
    counter_present: std::sync::atomic::AtomicBool,
}

impl Serialize for DataVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state =
            serializer.serialize_struct("DataVersion", if self.counter_nullable().is_some() { 2 } else { 1 })?;
        state.serialize_field("timestamp", &self.timestamp)?;
        if let Some(counter) = self.counter_nullable() {
            state.serialize_field("counter", &counter)?;
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for DataVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Helper {
            // Custom: Java 4.6.1 has counter/timestamp only.
            #[serde(default)]
            state_version: i64,
            #[serde(default = "default_timestamp")]
            timestamp: i64,
            #[serde(default = "default_counter")]
            counter: Option<i64>,
        }
        fn default_counter() -> Option<i64> {
            Some(0)
        }
        fn default_timestamp() -> i64 {
            DataVersion::default().get_timestamp()
        }
        let helper = Helper::deserialize(deserializer)?;
        let value = Self::with_values(helper.state_version, helper.timestamp, helper.counter.unwrap_or(0));
        value.counter_present.store(helper.counter.is_some(), Ordering::Relaxed);
        Ok(value)
    }
}

impl Clone for DataVersion {
    fn clone(&self) -> Self {
        let copy = Self::with_values(self.state_version, self.timestamp, self.get_counter());
        copy.counter_present
            .store(self.counter_nullable().is_some(), Ordering::Relaxed);
        copy
    }
}

impl PartialEq for DataVersion {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.counter_nullable() == other.counter_nullable()
    }
}

impl Eq for DataVersion {}

impl PartialOrd for DataVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then_with(|| self.counter_nullable().cmp(&other.counter_nullable()))
    }
}

impl Default for DataVersion {
    fn default() -> Self {
        crate::protocol::data_version_compat::new_data_version()
    }
}

impl DataVersion {
    pub const fn with_values(state_version: i64, timestamp: i64, counter: i64) -> Self {
        Self {
            state_version,
            timestamp,
            counter: AtomicI64::new(counter),
            counter_present: std::sync::atomic::AtomicBool::new(true),
        }
    }

    pub fn assign_new_one(&mut self, data_version: &Self) {
        self.timestamp = data_version.timestamp;
        self.state_version = data_version.state_version;
        self.counter.store(data_version.get_counter(), Ordering::SeqCst);
        self.counter_present
            .store(data_version.counter_nullable().is_some(), Ordering::Release);
    }

    /// Java assignNewOne updates the timestamp before a null AtomicLong fails.
    /// Clone/assign_new_one remain native value-copy operations, including null.
    pub fn assign_new_one_checked(&mut self, source: &Self) -> rocketmq_error::Result<()> {
        self.timestamp = source.timestamp;
        match (self.counter_nullable(), source.counter_nullable()) {
            (Some(_), Some(value)) => {
                self.counter.store(value, Ordering::SeqCst);
                Ok(())
            }
            _ => Err(crate::error::invalid_argument("DataVersion counter is null")),
        }
    }

    pub fn get_state_version(&self) -> i64 {
        self.state_version
    }
    pub fn set_state_version(&mut self, value: i64) {
        self.state_version = value;
    }
    pub fn get_timestamp(&self) -> i64 {
        self.timestamp
    }
    pub fn set_timestamp(&mut self, value: i64) {
        self.timestamp = value;
    }
    pub fn get_counter(&self) -> i64 {
        self.counter.load(Ordering::Relaxed)
    }
    pub fn set_counter(&self, value: i64) {
        self.counter.store(value, Ordering::Relaxed);
        self.counter_present.store(true, Ordering::Release);
    }
    pub fn counter_nullable(&self) -> Option<i64> {
        self.counter_present
            .load(Ordering::Acquire)
            .then(|| self.counter.load(Ordering::Relaxed))
    }
    pub fn set_counter_nullable(&self, value: Option<i64>) {
        self.counter.store(value.unwrap_or(0), Ordering::Relaxed);
        self.counter_present.store(value.is_some(), Ordering::Release);
    }
    /// Checked Java nextVersion: null counter is an error, never a success.
    pub fn next_version_checked(&mut self) -> rocketmq_error::Result<()> {
        self.timestamp = crate::protocol::data_version_compat::new_data_version().timestamp();
        if self.counter_nullable().is_none() {
            return Err(crate::error::invalid_argument("DataVersion counter is null"));
        }
        self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    pub fn java_hash_code(&self) -> i32 {
        fn hash(value: i64) -> i32 {
            (value as u64 ^ (value as u64 >> 32)) as i32
        }
        match self.counter_nullable() {
            Some(counter) => hash(self.timestamp).wrapping_mul(31).wrapping_add(hash(counter)),
            None => hash(self.timestamp),
        }
    }
    pub fn increment_counter(&self) -> i64 {
        self.counter.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
    }
    pub fn state_version(&self) -> i64 {
        self.state_version
    }
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }
    pub fn counter(&self) -> i64 {
        self.get_counter()
    }
    pub fn next_version_with_timestamp(&mut self, state_version: i64, timestamp: i64) {
        self.timestamp = timestamp;
        self.state_version = state_version;
        self.counter.fetch_add(1, Ordering::SeqCst);
    }
}

impl Display for DataVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DataVersion[timestamp={}, counter={}]",
            self.timestamp,
            self.counter_nullable()
                .map(|counter| counter.to_string())
                .unwrap_or_else(|| "null".into())
        )
    }
}

pub trait RemotingSerializable {
    fn encode(&self) -> rocketmq_error::Result<Vec<u8>>;
    fn serialize_json(&self) -> rocketmq_error::Result<String>;
    fn serialize_json_pretty(&self) -> rocketmq_error::Result<String>;
}

pub trait RemotingDeserializable {
    type Output;
    fn decode(bytes: &[u8]) -> rocketmq_error::Result<Self::Output>;
    fn decode_str(value: &str) -> rocketmq_error::Result<Self::Output> {
        Self::decode(value.as_bytes())
    }
    fn decode_string(value: String) -> rocketmq_error::Result<Self::Output> {
        Self::decode_str(&value)
    }
}

impl<T: Serialize> RemotingSerializable for T {
    fn encode(&self) -> rocketmq_error::Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|error| crate::error::serialization_source("serialize", "JSON", error))
    }
    fn serialize_json(&self) -> rocketmq_error::Result<String> {
        serde_json::to_string(self).map_err(|error| crate::error::serialization_source("serialize", "JSON", error))
    }
    fn serialize_json_pretty(&self) -> rocketmq_error::Result<String> {
        serde_json::to_string_pretty(self)
            .map_err(|error| crate::error::serialization_source("serialize", "JSON", error))
    }
}

impl<T: serde::de::DeserializeOwned> RemotingDeserializable for T {
    type Output = T;
    fn decode(bytes: &[u8]) -> rocketmq_error::Result<Self::Output> {
        serde_json::from_slice(bytes).map_err(|error| crate::error::serialization_source("deserialize", "JSON", error))
    }
}

/// Legacy handwritten fast-header codec surface.
///
/// Production encoding uses
/// [`command_custom_header::CommandCustomHeader::encode_capability`] and
/// [`command_custom_header::CommandCustomHeader::encode_direct_binary`].
#[deprecated(note = "use CommandCustomHeader typed map/direct-binary APIs")]
pub trait FastCodesHeader {
    fn write_if_not_null(out: &mut BytesMut, key: &str, value: &str) {
        if !value.is_empty() {
            RocketMQSerializable::write_str(out, true, key);
            RocketMQSerializable::write_str(out, false, value);
        }
    }
    fn encode_fast(&mut self, out: &mut BytesMut);
    fn decode_fast(&mut self, fields: &HashMap<CheetahString, CheetahString>);
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq)]
pub enum RemotingCommandType {
    REQUEST,
    RESPONSE,
    // Custom fork: security bit is independent of request/response and oneway bits.
    SECURITY,
}

impl RemotingCommandType {
    pub fn value_of(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::REQUEST),
            1 => Some(Self::RESPONSE),
            2 => Some(Self::SECURITY),
            _ => None,
        }
    }

    pub fn get_code(&self) -> u8 {
        match self {
            Self::REQUEST => 0,
            Self::RESPONSE => 1,
            Self::SECURITY => 2,
        }
    }

    pub fn get_type_from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "REQUEST" => Some(Self::REQUEST),
            "RESPONSE" => Some(Self::RESPONSE),
            "SECURITY" => Some(Self::SECURITY),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Default, Hash, Copy)]
pub enum LanguageCode {
    JAVA,
    CPP,
    DOTNET,
    PYTHON,
    DELPHI,
    ERLANG,
    RUBY,
    // Custom fork: Java 4.6.1 has no RUST language code.
    #[default]
    OTHER,
    HTTP,
    GO,
    PHP,
    OMS,
    RUST,
    #[allow(non_camel_case_types, reason = "matches the Java remoting wire language name")]
    NODE_JS,
}

impl fmt::Display for LanguageCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<LanguageCode> for u8 {
    fn from(code: LanguageCode) -> Self {
        code.get_code()
    }
}

impl From<LanguageCode> for i32 {
    fn from(code: LanguageCode) -> Self {
        code.get_code() as i32
    }
}

impl From<u32> for LanguageCode {
    fn from(code: u32) -> Self {
        u8::try_from(code).map_or(Self::OTHER, Self::from)
    }
}

impl From<i32> for LanguageCode {
    fn from(code: i32) -> Self {
        u8::try_from(code).map_or(Self::OTHER, Self::from)
    }
}

impl From<u8> for LanguageCode {
    fn from(code: u8) -> Self {
        Self::value_of(code).unwrap_or(Self::OTHER)
    }
}

impl LanguageCode {
    /// Custom fork: exact Java 4.6.1 `valueOf(byte)` domain; unknown/newer codes return None.
    ///
    /// The inherited codec enum retains newer diagnostic names, while target client
    /// configuration and the Java API mapping use this legacy-domain conversion.
    pub fn java_4_6_1_value_of(code: u8) -> Option<Self> {
        if code > Self::OMS.get_code() {
            return None;
        }
        Self::value_of(code)
    }

    pub fn value_of(code: u8) -> Option<Self> {
        Some(match code {
            0 => Self::JAVA,
            1 => Self::CPP,
            2 => Self::DOTNET,
            3 => Self::PYTHON,
            4 => Self::DELPHI,
            5 => Self::ERLANG,
            6 => Self::RUBY,
            7 => Self::OTHER,
            8 => Self::HTTP,
            9 => Self::GO,
            10 => Self::PHP,
            11 => Self::OMS,
            12 => Self::RUST,
            13 => Self::NODE_JS,
            _ => Self::OTHER,
        })
    }

    pub fn get_code(&self) -> u8 {
        *self as u8
    }

    pub fn get_code_from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "JAVA" => Some(Self::JAVA),
            "CPP" => Some(Self::CPP),
            "DOTNET" => Some(Self::DOTNET),
            "PYTHON" => Some(Self::PYTHON),
            "DELPHI" => Some(Self::DELPHI),
            "ERLANG" => Some(Self::ERLANG),
            "RUBY" => Some(Self::RUBY),
            "OTHER" => Some(Self::OTHER),
            "HTTP" => Some(Self::HTTP),
            "GO" => Some(Self::GO),
            "PHP" => Some(Self::PHP),
            "OMS" => Some(Self::OMS),
            "RUST" => Some(Self::RUST),
            "NODE_JS" => Some(Self::NODE_JS),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Deserialize, Serialize)]
pub enum SerializeType {
    JSON,
    ROCKETMQ,
}

impl fmt::Display for SerializeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl SerializeType {
    pub fn value_of(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::JSON),
            1 => Some(Self::ROCKETMQ),
            _ => None,
        }
    }

    pub fn get_code(&self) -> u8 {
        *self as u8
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use serde::Deserialize;
    use serde::Serialize;
    use serde::Serializer;

    use super::RemotingDeserializable;
    use super::RemotingSerializable;

    struct FailingJsonSerialize;

    impl Serialize for FailingJsonSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Err(<S::Error as serde::ser::Error>::custom(
                "forced JSON serialization failure",
            ))
        }
    }

    #[derive(Debug, Deserialize)]
    struct DecodedValue;

    fn assert_json_source(error: &rocketmq_error::Error) {
        let direct_source = StdError::source(error).expect("JSON error source");
        assert!(direct_source.downcast_ref::<serde_json::Error>().is_some());
    }

    #[test]
    fn protocol_json_encode_preserves_source_and_public_boundary() {
        let error = FailingJsonSerialize.encode().unwrap_err();

        assert_eq!(error.descriptor(), &rocketmq_error::CORE_SERIALIZATION_FAILED);
        assert_eq!(error.descriptor().public_message(), "Serialization failed");
        assert_json_source(&error);
    }

    #[test]
    fn protocol_json_decode_preserves_source_and_public_boundary() {
        let error = DecodedValue::decode(b"invalid").unwrap_err();

        assert_eq!(error.descriptor(), &rocketmq_error::CORE_SERIALIZATION_FAILED);
        assert_eq!(error.descriptor().public_message(), "Serialization failed");
        assert_json_source(&error);
    }
}
