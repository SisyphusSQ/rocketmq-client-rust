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

use std::fs;
use std::path::PathBuf;

use crate::NameServerResult;
use config::Config;
use tracing::info;

use crate::config::validate_namesrv_config_source;
use crate::NamesrvConfig;

pub fn parse_command_and_config_file(config_file: PathBuf) -> NameServerResult<NamesrvConfig> {
    let source = fs::read_to_string(&config_file)
        .map_err(|error| crate::namesrv_error::invalid_configuration_source("configFile", error))?;
    validate_namesrv_config_source(&source)?;

    let namesrv_config = Config::builder()
        .add_source(config::File::from(config_file.clone()))
        .build()
        .and_then(Config::try_deserialize::<NamesrvConfig>)
        .map_err(|error| crate::namesrv_error::invalid_configuration_source("configFile", error))?;
    namesrv_config.validate_domains()?;
    info!(
        cluster_test = namesrv_config.cluster_test,
        support_acting_master = namesrv_config.support_acting_master,
        embedded_controller_enabled = namesrv_config.enable_controller_in_namesrv,
        client_request_threads = namesrv_config.client_request_thread_pool_nums,
        default_threads = namesrv_config.default_thread_pool_nums,
        "NameServer configuration loaded"
    );
    Ok(namesrv_config)
}


