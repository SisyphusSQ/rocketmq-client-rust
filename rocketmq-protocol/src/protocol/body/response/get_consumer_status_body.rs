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

use cheetah_string::CheetahString;
use rocketmq_model::message::MessageQueue;
use serde::Deserialize;
use serde::Serialize;
use serde_json_any_key::*;

use crate::protocol::admin::consume_stats::append_message_queue_object_key;
// Custom fork: nested consumerTable uses the same Java MessageQueue object keys.
use crate::protocol::admin::consume_stats::normalize_java_object_keys;
use crate::protocol::RemotingDeserializable;

/// Response body for get consumer status operation.
/// Maps client ID to their message queue offset table.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetConsumerStatusBody {
    /// Maps MessageQueue to offset (used for single client response)
    #[serde(default, with = "any_key_map")]
    pub message_queue_table: HashMap<MessageQueue, i64>,

    /// Maps client ID to their MessageQueue offset table (used for aggregated response)
    #[serde(default, with = "nested_offsets")]
    pub consumer_table: HashMap<CheetahString, HashMap<MessageQueue, i64>>,
}

impl GetConsumerStatusBody {
    pub fn new() -> Self {
        Self {
            message_queue_table: HashMap::new(),
            consumer_table: HashMap::new(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        self.encode_java_compatible().unwrap_or_default()
    }

    pub fn encode_java_compatible(&self) -> rocketmq_error::Result<Vec<u8>> {
        Ok(self.to_java_compatible_json()?.into_bytes())
    }

    pub fn to_java_compatible_json(&self) -> rocketmq_error::Result<String> {
        let mut body = String::new();
        body.push_str("{\"messageQueueTable\":{");
        append_offset_map(&mut body, &self.message_queue_table)?;
        body.push_str("},\"consumerTable\":{");

        for (index, (client_id, offsets)) in self.consumer_table.iter().enumerate() {
            if index > 0 {
                body.push(',');
            }
            body.push_str(
                &serde_json::to_string(client_id.as_str())
                    .map_err(|error| crate::error::serialization_source("serialize", "JSON", error))?,
            );
            body.push_str(":{");
            append_offset_map(&mut body, offsets)?;
            body.push('}');
        }

        body.push_str("}}");
        Ok(body)
    }

    pub fn decode(body: &[u8]) -> Option<Self> {
        match <Self as RemotingDeserializable>::decode(body) {
            Ok(decoded) => Some(decoded),
            Err(_) => {
                let raw = std::str::from_utf8(body).ok()?;
                let normalized = normalize_nonstandard_message_queue_table_keys(raw);
                <Self as RemotingDeserializable>::decode_str(&normalized)
                    .ok()
                    .or_else(|| {
                        let root: serde_json::Value = serde_json::from_str(&normalized).ok()?;
                        decode_java_referenced_status(&root)
                    })
            }
        }
    }
}

// Custom fork: actual fixed Fastjson reuses both whole offset maps and their
// MessageQueue object keys. Resolve this typed DTO, without expanding arbitrary
// object graphs or silently accepting an ambiguous key reference.
fn java_path(path: &str) -> Option<Vec<String>> {
    let path = path.strip_prefix("$.")?;
    let mut parts = Vec::new();
    let mut part = String::new();
    let mut escaped = false;
    for value in path.chars() {
        if escaped {
            part.push(value);
            escaped = false;
        } else if value == '\\' {
            escaped = true;
        } else if value == '.' {
            parts.push(std::mem::take(&mut part));
        } else {
            part.push(value);
        }
    }
    if escaped {
        return None;
    }
    parts.push(part);
    Some(parts)
}
fn java_ref_node<'a>(
    root: &'a serde_json::Value,
    node: &'a serde_json::Value,
    hops: usize,
) -> Option<&'a serde_json::Value> {
    if hops >= 64 {
        return None;
    }
    let map = node.as_object()?;
    if map.len() == 1 {
        if let Some(reference) = map.get("$ref") {
            return java_path_node(root, &java_path(reference.as_str()?)?, hops + 1);
        }
    }
    Some(node)
}
fn java_path_node<'a>(root: &'a serde_json::Value, path: &[String], hops: usize) -> Option<&'a serde_json::Value> {
    if hops >= 64 {
        return None;
    }
    let mut node = root;
    for part in path {
        node = java_ref_node(root, node, hops + 1)?.get(part)?;
    }
    java_ref_node(root, node, hops + 1)
}
fn java_queue_key(root: &serde_json::Value, key: &str, hops: usize) -> Option<MessageQueue> {
    if hops >= 64 {
        return None;
    }
    if let Ok(queue) = serde_json::from_str(key) {
        return Some(queue);
    }
    let value: serde_json::Value = serde_json::from_str(key).ok()?;
    let mut path = java_path(value.get("$ref")?.as_str()?)?;
    if path.pop()?.as_str() != "null" {
        return None;
    }
    let map = java_path_node(root, &path, hops + 1)?.as_object()?;
    // Fastjson's object-key context uses the placeholder "null". More than
    // one candidate is not recoverable from that path; never guess an offset.
    if map.len() != 1 {
        return None;
    }
    java_queue_key(root, map.keys().next()?, hops + 1)
}
fn java_offset_map(root: &serde_json::Value, node: &serde_json::Value) -> Option<HashMap<MessageQueue, i64>> {
    let map = java_ref_node(root, node, 0)?.as_object()?;
    let mut offsets = HashMap::with_capacity(map.len());
    for (key, value) in map {
        let queue = java_queue_key(root, key, 0)?;
        if offsets.insert(queue, value.as_i64()?).is_some() {
            return None;
        }
    }
    Some(offsets)
}
fn decode_java_referenced_status(root: &serde_json::Value) -> Option<GetConsumerStatusBody> {
    root.as_object()?;
    let mut body = GetConsumerStatusBody::new();
    if let Some(table) = root.get("messageQueueTable") {
        body.message_queue_table = java_offset_map(root, table)?;
    }
    if let Some(table) = root.get("consumerTable") {
        for (client, offsets) in java_ref_node(root, table, 0)?.as_object()? {
            body.consumer_table
                .insert(client.clone().into(), java_offset_map(root, offsets)?);
        }
    }
    Some(body)
}

fn append_offset_map(output: &mut String, offsets: &HashMap<MessageQueue, i64>) -> rocketmq_error::Result<()> {
    for (index, (queue, offset)) in offsets.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        append_message_queue_object_key(output, queue)?;
        output.push(':');
        output.push_str(&offset.to_string());
    }
    Ok(())
}

fn normalize_nonstandard_message_queue_table_keys(input: &str) -> String {
    normalize_java_object_keys(input)
}

mod nested_offsets {
    use super::*;
    use serde::ser::SerializeMap;
    #[derive(Serialize, Deserialize)]
    #[serde(transparent)]
    struct Offsets(#[serde(with = "any_key_map")] HashMap<MessageQueue, i64>);
    pub fn serialize<S: serde::Serializer>(
        table: &HashMap<CheetahString, HashMap<MessageQueue, i64>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(table.len()))?;
        for (client, offsets) in table {
            map.serialize_entry(client, &Offsets(offsets.clone()))?;
        }
        map.end()
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<HashMap<CheetahString, HashMap<MessageQueue, i64>>, D::Error> {
        let map = HashMap::<CheetahString, Offsets>::deserialize(deserializer)?;
        Ok(map.into_iter().map(|(client, offsets)| (client, offsets.0)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_are_resolved_without_accepting_cycles_or_primitive_roots() {
        let raw = r#"{"consumerTable":{"client-a":{}},"messageQueueTable":{"$ref":"$.consumerTable.client-a"}}"#;
        assert!(GetConsumerStatusBody::decode(raw.as_bytes()).is_some());
        assert!(GetConsumerStatusBody::decode(br#"{"messageQueueTable":{"$ref":"$.messageQueueTable"}}"#).is_none());
        assert!(GetConsumerStatusBody::decode(b"null").is_none());
        assert!(GetConsumerStatusBody::decode(b"7").is_none());
    }

    #[test]
    fn get_consumer_status_body_encode_and_decode() {
        let mut body = GetConsumerStatusBody::new();
        let mq = MessageQueue::from_parts(CheetahString::from("topic"), CheetahString::from("broker"), 1);
        body.message_queue_table.insert(mq.clone(), 123);

        let encoded = body.encode();
        assert!(!encoded.is_empty());
        let decoded: GetConsumerStatusBody =
            GetConsumerStatusBody::decode(&encoded).expect("Failed to deserialize GetConsumerStatusBody");
        assert_eq!(decoded.message_queue_table.get(&mq), Some(&123));
    }

    #[test]
    fn get_consumer_status_body_serialization_and_deserialization() {
        let mut body = GetConsumerStatusBody::new();
        let mq = MessageQueue::from_parts(CheetahString::from("topic"), CheetahString::from("broker"), 1);
        body.message_queue_table.insert(mq.clone(), 123);

        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("messageQueueTable"));
        assert!(json.contains("consumerTable"));
        let deserialized: GetConsumerStatusBody = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.message_queue_table.get(&mq), Some(&123));
    }

    #[test]
    fn get_consumer_status_body_encode_uses_java_object_keys() {
        let mut body = GetConsumerStatusBody::new();
        let mq = MessageQueue::from_parts(CheetahString::from("topic"), CheetahString::from("broker"), 1);
        body.message_queue_table.insert(mq.clone(), 123);
        let mut consumer_offsets = HashMap::new();
        consumer_offsets.insert(mq, 456);
        body.consumer_table
            .insert(CheetahString::from("client-a"), consumer_offsets);

        let encoded = String::from_utf8(body.encode()).expect("get consumer status body should be utf8");

        assert!(encoded.contains(r#""messageQueueTable":{{"topic":"topic","brokerName":"broker","queueId":1}:123}"#));
        assert!(encoded.contains(r#""client-a":{{"topic":"topic","brokerName":"broker","queueId":1}:456}"#));
        assert!(!encoded.contains(r#""{\"topic\""#));
    }

    #[test]
    fn get_consumer_status_body_decode_accepts_java_message_queue_table_keys() {
        let body =
            r#"{"messageQueueTable":{{"topic":"topic","brokerName":"broker","queueId":1}:123},"consumerTable":{}}"#;

        let decoded = GetConsumerStatusBody::decode(body.as_bytes()).expect("decode get consumer status body");

        let mq = MessageQueue::from_parts(CheetahString::from("topic"), CheetahString::from("broker"), 1);
        assert_eq!(decoded.message_queue_table.get(&mq), Some(&123));
    }
}
