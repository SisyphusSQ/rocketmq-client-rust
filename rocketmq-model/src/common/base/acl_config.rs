// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.

use cheetah_string::CheetahString;
use serde::{Deserialize, Serialize};

use super::plain_access_config::PlainAccessConfig;

/// Java 4.6.1 ACL configuration. Debug output inherits credential redaction.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct AclConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub global_white_addrs: Option<Vec<CheetahString>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plain_access_configs: Option<Vec<PlainAccessConfig>>,
}
