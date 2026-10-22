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
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::DataVersion;

// Custom: the 4.6.1 bean is nullable and has one DataVersion; the map is a native extension.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default, rename_all = "camelCase")]
pub struct ClusterAclVersionInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_name: Option<CheetahString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_addr: Option<CheetahString>,
    //Deprecated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acl_config_data_version: Option<DataVersion>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub all_acl_config_data_version: HashMap<CheetahString, DataVersion>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_name: Option<CheetahString>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;

    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::DataVersion;

    #[test]
    fn serialize_cluster_acl_version_info() {
        let mut all_acl_config_data_version = HashMap::new();
        all_acl_config_data_version.insert(CheetahString::from("key1"), DataVersion::default());

        let info = ClusterAclVersionInfo {
            broker_name: Some(CheetahString::from("broker1")),
            broker_addr: Some(CheetahString::from("addr1")),
            acl_config_data_version: Some(DataVersion::default()),
            all_acl_config_data_version,
            cluster_name: Some(CheetahString::from("cluster1")),
        };
        let serialized = serde_json::to_string(&info).unwrap();
        assert!(serialized.contains("\"brokerName\":\"broker1\""));
        assert!(serialized.contains("\"brokerAddr\":\"addr1\""));
        assert!(serialized.contains("\"aclConfigDataVersion\":"));
        assert!(serialized.contains("\"allAclConfigDataVersion\":"));
        assert!(serialized.contains("\"clusterName\":\"cluster1\""));
    }

    #[test]
    fn deserialize_cluster_acl_version_info_with_data_version() {
        let json = r#"{
            "brokerName": "broker1",
            "brokerAddr": "addr1",
            "aclConfigDataVersion": {"counter": 1, "timestamp": 123456789,"stateVersion": 0},
            "allAclConfigDataVersion": {"key1": {"counter": 1, "timestamp": 123456789,"stateVersion": 0}},
            "clusterName": "cluster1"
        }"#;
        let deserialized: ClusterAclVersionInfo = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.broker_name.as_deref(), Some("broker1"));
        assert_eq!(deserialized.broker_addr.as_deref(), Some("addr1"));
        assert!(deserialized.acl_config_data_version.is_some());
        assert_eq!(
            deserialized
                .acl_config_data_version
                .as_ref()
                .unwrap()
                .counter
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            deserialized.acl_config_data_version.as_ref().unwrap().timestamp,
            123456789
        );
        assert_eq!(
            deserialized
                .all_acl_config_data_version
                .get(&CheetahString::from("key1"))
                .unwrap()
                .counter
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            deserialized
                .all_acl_config_data_version
                .get(&CheetahString::from("key1"))
                .unwrap()
                .timestamp,
            123456789
        );
        assert_eq!(deserialized.cluster_name.as_deref(), Some("cluster1"));
    }
}
