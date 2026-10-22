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

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::common::wire_constants::MASTER_ID;
use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BrokerData {
    // Custom: preserve the nullable Java bean state; legacy borrowed getters expose empty views.
    #[serde(skip_serializing_if = "Option::is_none")]
    cluster: Option<CheetahString>,
    #[serde(rename = "brokerName", skip_serializing_if = "Option::is_none")]
    broker_name: Option<CheetahString>,
    #[serde(rename = "brokerAddrs", skip_serializing_if = "Option::is_none")]
    broker_addrs: Option<HashMap<u64 /* broker id */, CheetahString /* broker ip */>>,
    #[serde(rename = "zoneName", skip_serializing_if = "Option::is_none")]
    zone_name: Option<CheetahString>,
    // Custom fork: this newer optional field is absent from Java 4.6.1 routes.
    #[serde(
        rename = "enableActingMaster",
        default,
        skip_serializing_if = "acting_master_disabled"
    )]
    enable_acting_master: bool,
}

impl PartialEq for BrokerData {
    fn eq(&self, other: &Self) -> bool {
        self.broker_name == other.broker_name && self.broker_addrs == other.broker_addrs
    }
}

impl Eq for BrokerData {}

// Custom: Hash uses the same name/address fields as Java equality, independent of map order.
impl std::hash::Hash for BrokerData {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.broker_name.hash(state);
        self.broker_addrs.is_some().hash(state);
        let mut entries = self.broker_addrs().iter().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.0);
        entries.hash(state);
    }
}

// Custom: native diagnostic text for Java toString; no wire serialization depends on it.
impl std::fmt::Display for BrokerData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BrokerData [brokerName={}, brokerAddrs={:?}]",
            self.broker_name.as_deref().unwrap_or("null"),
            self.broker_addrs
        )
    }
}

impl PartialOrd for BrokerData {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BrokerData {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.broker_name.cmp(&other.broker_name)
    }
}

fn acting_master_disabled(value: &bool) -> bool {
    !*value
}
static EMPTY_BROKER_NAME: std::sync::LazyLock<CheetahString> = std::sync::LazyLock::new(CheetahString::default);
static EMPTY_BROKER_ADDRESSES: std::sync::LazyLock<HashMap<u64, CheetahString>> =
    std::sync::LazyLock::new(HashMap::new);

impl BrokerData {
    pub fn optional_cluster(&self) -> Option<&CheetahString> {
        self.cluster.as_ref()
    }
    pub fn optional_broker_name(&self) -> Option<&CheetahString> {
        self.broker_name.as_ref()
    }
    pub fn optional_broker_addrs(&self) -> Option<&HashMap<u64, CheetahString>> {
        self.broker_addrs.as_ref()
    }
    pub fn set_optional_cluster(&mut self, cluster: Option<CheetahString>) {
        self.cluster = cluster;
    }
    pub fn set_optional_broker_name(&mut self, name: Option<CheetahString>) {
        self.broker_name = name;
    }
    pub fn set_optional_broker_addrs(&mut self, addresses: Option<HashMap<u64, CheetahString>>) {
        self.broker_addrs = addresses;
    }

    pub fn new(
        cluster: CheetahString,
        broker_name: CheetahString,
        broker_addrs: HashMap<u64, CheetahString>,
        zone_name: Option<CheetahString>,
    ) -> BrokerData {
        BrokerData {
            cluster: Some(cluster),
            broker_name: Some(broker_name),
            broker_addrs: Some(broker_addrs),
            zone_name,
            enable_acting_master: false,
        }
    }

    #[inline]
    pub fn set_cluster(&mut self, cluster: CheetahString) {
        self.cluster = Some(cluster);
    }

    #[inline]
    pub fn set_broker_name(&mut self, broker_name: CheetahString) {
        self.broker_name = Some(broker_name);
    }

    #[inline]
    pub fn set_broker_addrs(&mut self, broker_addrs: HashMap<u64, CheetahString>) {
        self.broker_addrs = Some(broker_addrs);
    }

    #[inline]
    pub fn set_zone_name(&mut self, zone_name: Option<CheetahString>) {
        self.zone_name = zone_name;
    }

    #[inline]
    pub fn set_enable_acting_master(&mut self, enable_acting_master: bool) {
        self.enable_acting_master = enable_acting_master;
    }

    #[inline]
    pub fn cluster(&self) -> &str {
        self.cluster.as_deref().unwrap_or("")
    }

    #[inline]
    pub fn broker_name(&self) -> &CheetahString {
        self.broker_name.as_ref().unwrap_or(&EMPTY_BROKER_NAME)
    }

    #[inline]
    pub fn broker_addrs(&self) -> &HashMap<u64, CheetahString> {
        self.broker_addrs.as_ref().unwrap_or(&EMPTY_BROKER_ADDRESSES)
    }

    #[inline]
    pub fn broker_addrs_mut(&mut self) -> &mut HashMap<u64, CheetahString> {
        self.broker_addrs.get_or_insert_with(HashMap::new)
    }

    #[inline]
    pub fn remove_broker_by_addr(&mut self, broker_id: u64, broker_addr: &CheetahString) {
        self.broker_addrs_mut()
            .retain(|key, value| value != broker_addr || *key == broker_id);
    }

    #[inline]
    pub fn zone_name(&self) -> Option<&CheetahString> {
        self.zone_name.as_ref()
    }

    #[inline]
    pub fn enable_acting_master(&self) -> bool {
        self.enable_acting_master
    }

    pub fn select_broker_addr_with_index(&self, index: usize) -> Option<CheetahString> {
        if let Some(master_address) = self.broker_addrs().get(&MASTER_ID) {
            return Some(master_address.clone());
        }
        let mut slaves = self.broker_addrs().iter().collect::<Vec<_>>();
        slaves.sort_unstable_by_key(|(id, _)| **id);
        if slaves.is_empty() {
            return None;
        }
        slaves.get(index % slaves.len()).map(|(_, value)| (*value).clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Default, Hash)]
pub struct QueueData {
    #[serde(rename = "brokerName")]
    pub broker_name: CheetahString,
    #[serde(rename = "readQueueNums")]
    pub read_queue_nums: i32,
    #[serde(rename = "writeQueueNums")]
    pub write_queue_nums: i32,
    #[serde(
        serialize_with = "serialize_java_int_bits",
        deserialize_with = "deserialize_java_int_bits"
    )]
    pub perm: u32,
    // Custom fork: the fixed 4.6.1 wire name is topicSynFlag; retain the newer decode alias.
    #[serde(rename = "topicSynFlag", alias = "topicSysFlag")]
    #[serde(
        serialize_with = "serialize_java_int_bits",
        deserialize_with = "deserialize_java_int_bits"
    )]
    pub topic_sys_flag: u32,
}

impl QueueData {
    pub fn new(
        broker_name: CheetahString,
        read_queue_nums: u32,
        write_queue_nums: u32,
        perm: u32,
        topic_sys_flag: u32,
    ) -> Self {
        Self {
            broker_name,
            read_queue_nums: read_queue_nums as i32,
            write_queue_nums: write_queue_nums as i32,
            perm,
            topic_sys_flag,
        }
    }

    #[inline]
    pub fn broker_name(&self) -> &CheetahString {
        &self.broker_name
    }

    #[inline]
    pub fn read_queue_nums(&self) -> i32 {
        self.read_queue_nums
    }

    #[inline]
    pub fn write_queue_nums(&self) -> i32 {
        self.write_queue_nums
    }

    #[inline]
    pub fn perm(&self) -> u32 {
        self.perm
    }

    #[inline]
    pub fn topic_sys_flag(&self) -> u32 {
        self.topic_sys_flag
    }

    /// Custom: Java compareTo uses only the broker's UTF-16 name.
    pub fn java_compare_to(&self, other: &Self) -> i32 {
        let left: Vec<u16> = self.broker_name.encode_utf16().collect();
        let right: Vec<u16> = other.broker_name.encode_utf16().collect();
        for (a, b) in left.iter().zip(&right) {
            if a != b {
                return i32::from(*a) - i32::from(*b);
            }
        }
        left.len() as i32 - right.len() as i32
    }
    pub fn java_hash_code(&self) -> i32 {
        let name = self
            .broker_name
            .encode_utf16()
            .fold(0_i32, |h, c| h.wrapping_mul(31).wrapping_add(i32::from(c)));
        [
            name,
            self.perm as i32,
            self.read_queue_nums,
            self.write_queue_nums,
            self.topic_sys_flag as i32,
        ]
        .into_iter()
        .fold(1_i32, |h, v| h.wrapping_mul(31).wrapping_add(v))
    }
}

// Custom: retain native unsigned bit operations while preserving Java's signed int wire value.
fn serialize_java_int_bits<S: serde::Serializer>(value: &u32, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_i32(*value as i32)
}
fn deserialize_java_int_bits<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    i32::deserialize(deserializer).map(|value| value as u32)
}

impl PartialOrd for QueueData {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for QueueData {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Rust Ord must agree with Eq. Java's broker-only comparator remains explicit.
        self.java_compare_to(other).cmp(&0).then_with(|| {
            (
                self.read_queue_nums,
                self.write_queue_nums,
                self.perm,
                self.topic_sys_flag,
            )
                .cmp(&(
                    other.read_queue_nums,
                    other.write_queue_nums,
                    other.perm,
                    other.topic_sys_flag,
                ))
        })
    }
}
impl std::fmt::Display for QueueData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QueueData [brokerName={}, readQueueNums={}, writeQueueNums={}, perm={}, topicSynFlag={}]",
            self.broker_name, self.read_queue_nums, self.write_queue_nums, self.perm as i32, self.topic_sys_flag as i32
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn broker_data_methods_preserve_route_fields() {
        let mut broker_data = BrokerData::new(
            CheetahString::from("cluster1"),
            CheetahString::from("broker1"),
            HashMap::new(),
            None,
        );
        assert_eq!(broker_data.cluster(), "cluster1");
        assert_eq!(broker_data.broker_name(), "broker1");
        assert!(broker_data.broker_addrs().is_empty());
        assert_eq!(broker_data.zone_name(), None);
        assert!(!broker_data.enable_acting_master());

        broker_data.set_cluster(CheetahString::from("cluster2"));
        broker_data.set_broker_name(CheetahString::from("broker2"));
        broker_data.set_broker_addrs(HashMap::from([(1, CheetahString::from("127.0.0.1"))]));
        broker_data
            .broker_addrs_mut()
            .insert(2, CheetahString::from("127.0.0.2"));
        broker_data.set_zone_name(Some(CheetahString::from("zone1")));
        broker_data.set_enable_acting_master(true);

        assert_eq!(broker_data.cluster(), "cluster2");
        assert_eq!(broker_data.broker_name(), "broker2");
        assert_eq!(
            broker_data.broker_addrs(),
            &HashMap::from([
                (1, CheetahString::from("127.0.0.1")),
                (2, CheetahString::from("127.0.0.2")),
            ])
        );
        assert_eq!(broker_data.zone_name(), Some(&CheetahString::from("zone1")));
        assert!(broker_data.enable_acting_master());
    }

    #[test]
    fn remove_broker_by_addr_keeps_the_current_id_only() {
        let mut broker_data = BrokerData::new(
            CheetahString::from("cluster1"),
            CheetahString::from("broker1"),
            HashMap::from([
                (1, CheetahString::from("127.0.0.1")),
                (2, CheetahString::from("127.0.0.1")),
                (3, CheetahString::from("127.0.0.3")),
            ]),
            None,
        );

        broker_data.remove_broker_by_addr(1, &"127.0.0.1".into());
        assert_eq!(
            broker_data.broker_addrs(),
            &HashMap::from([
                (1, CheetahString::from("127.0.0.1")),
                (3, CheetahString::from("127.0.0.3")),
            ])
        );
    }

    #[test]
    fn select_broker_addr_prefers_master_and_indexes_slaves() {
        let mut broker_data = BrokerData::new(
            CheetahString::from("cluster1"),
            CheetahString::from("broker1"),
            HashMap::new(),
            None,
        );
        assert_eq!(broker_data.select_broker_addr_with_index(0), None);

        broker_data.set_broker_addrs(HashMap::from([
            (4, CheetahString::from("127.0.0.4")),
            (2, CheetahString::from("127.0.0.2")),
        ]));
        assert_eq!(
            broker_data.select_broker_addr_with_index(0),
            Some(CheetahString::from("127.0.0.2"))
        );
        assert_eq!(
            broker_data.select_broker_addr_with_index(1),
            Some(CheetahString::from("127.0.0.4"))
        );
        assert_eq!(
            broker_data.select_broker_addr_with_index(2),
            Some(CheetahString::from("127.0.0.2"))
        );

        broker_data
            .broker_addrs_mut()
            .insert(MASTER_ID, CheetahString::from("127.0.0.1"));
        assert_eq!(
            broker_data.select_broker_addr_with_index(1),
            Some(CheetahString::from("127.0.0.1"))
        );
    }

    #[test]
    fn queue_data_methods_preserve_route_fields() {
        let queue_data = QueueData::new(CheetahString::from("broker1"), 4, 4, 6, 0);

        assert_eq!(queue_data.broker_name(), "broker1");
        assert_eq!(queue_data.read_queue_nums(), 4);
        assert_eq!(queue_data.write_queue_nums(), 4);
        assert_eq!(queue_data.perm(), 6);
        assert_eq!(queue_data.topic_sys_flag(), 0);
    }
}
