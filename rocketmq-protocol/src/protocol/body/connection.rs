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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::LanguageCode;

#[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq, Hash, Default)]
#[serde(rename_all = "camelCase")]
pub struct Connection {
    // Custom: preserve the fixed Java bean null value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_id: Option<CheetahString>,
    // Custom: preserve the fixed Java bean null value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_addr: Option<CheetahString>,
    // Custom: preserve the fixed Java bean null value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    language: Option<LanguageCode>,
    version: i32,
}

impl Connection {
    pub fn optional_language(&self) -> Option<&LanguageCode> {
        self.language.as_ref()
    }
    pub fn set_optional_language(&mut self, value: Option<LanguageCode>) {
        self.language = value;
    }

    pub fn optional_client_addr(&self) -> Option<&CheetahString> {
        self.client_addr.as_ref()
    }
    pub fn set_optional_client_addr(&mut self, value: Option<CheetahString>) {
        self.client_addr = value;
    }

    pub fn optional_client_id(&self) -> Option<&CheetahString> {
        self.client_id.as_ref()
    }
    pub fn set_optional_client_id(&mut self, value: Option<CheetahString>) {
        self.client_id = value;
    }

    pub fn new() -> Self {
        Connection {
            client_id: None,
            client_addr: None,
            language: None,
            version: 0,
        }
    }
}

impl Connection {
    pub fn get_client_id(&self) -> CheetahString {
        self.client_id.clone().unwrap_or_default()
    }

    pub fn set_client_id(&mut self, client_id: CheetahString) {
        self.client_id = Some(client_id);
    }

    pub fn get_client_addr(&self) -> CheetahString {
        self.client_addr.clone().unwrap_or_default()
    }

    pub fn set_client_addr(&mut self, client_addr: CheetahString) {
        self.client_addr = Some(client_addr);
    }

    pub fn get_language(&self) -> LanguageCode {
        self.language.unwrap_or_default()
    }

    pub fn set_language(&mut self, language: LanguageCode) {
        self.language = Some(language);
    }

    pub fn get_version(&self) -> i32 {
        self.version
    }

    pub fn set_version(&mut self, version: i32) {
        self.version = version;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors_and_serde_preserve_the_connection_contract() {
        let mut conn = Connection::new();
        conn.set_client_id(CheetahString::from("id"));
        conn.set_client_addr(CheetahString::from("addr"));
        conn.set_language(LanguageCode::RUST);
        conn.set_version(100);

        assert_eq!(conn.get_client_id(), "id");
        assert_eq!(conn.get_client_addr(), "addr");
        assert_eq!(conn.get_language(), LanguageCode::RUST);
        assert_eq!(conn.get_version(), 100);

        let value = serde_json::to_value(&conn).expect("serialize connection");
        assert_eq!(
            value,
            serde_json::json!({
                "clientId": "id",
                "clientAddr": "addr",
                "language": "RUST",
                "version": 100
            })
        );

        let deserialized: Connection = serde_json::from_value(value).expect("deserialize connection");
        assert_eq!(conn, deserialized);
    }
}
