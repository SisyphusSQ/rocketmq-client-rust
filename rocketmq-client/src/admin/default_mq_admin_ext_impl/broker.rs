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

// Custom: retain fixed Java maintenance node outcomes and raw failures.

use super::*;
#[allow(deprecated)]
pub(super) fn broker_operator_result(
    success_list: Vec<CheetahString>,
    failure_list: Vec<CheetahString>,
) -> BrokerOperatorResult {
    let mut result = BrokerOperatorResult::new();
    result.set_success_list(success_list);
    result.set_failure_list(failure_list);
    result
}

pub(super) fn controller_servers_or_namesrv(
    controller_servers: Vec<CheetahString>,
    name_server_address_list: &[CheetahString],
) -> Vec<CheetahString> {
    if controller_servers.is_empty() {
        name_server_address_list.to_vec()
    } else {
        controller_servers
    }
}

pub(super) fn optional_non_empty(value: Option<CheetahString>) -> Option<CheetahString> {
    value.filter(|value| !value.is_empty())
}

pub(super) fn notify_min_broker_id_change_request_header(
    min_broker_id: u64,
    min_broker_addr: CheetahString,
    offline_broker_addr: Option<CheetahString>,
    ha_broker_addr: Option<CheetahString>,
) -> crate::ClientResult<NotifyMinBrokerIdChangeRequestHeader> {
    if min_broker_addr.is_empty() {
        return Err(ClientError::illegal_argument(
            "notifyMinBrokerIdChanged requires minBrokerAddr",
        ));
    }

    Ok(NotifyMinBrokerIdChangeRequestHeader::new(
        Some(min_broker_id),
        None,
        Some(min_broker_addr),
        optional_non_empty(offline_broker_addr),
        optional_non_empty(ha_broker_addr),
    ))
}

pub(super) fn choose_min_broker_notify_addrs(
    broker_addrs: &HashMap<u64, CheetahString>,
    min_broker_id: u64,
    offline_broker_addr: Option<&CheetahString>,
) -> Vec<CheetahString> {
    let notify_all = broker_addrs.len() == 1
        || offline_broker_addr
            .map(|broker_addr| !broker_addr.is_empty())
            .unwrap_or(false);
    let mut entries = broker_addrs.iter().collect::<Vec<_>>();
    entries.sort_by_key(|entry| *entry.0);
    entries
        .into_iter()
        .filter(|entry| notify_all || *entry.0 != min_broker_id)
        .map(|entry| entry.1.clone())
        .collect()
}

#[derive(Debug, Clone, Copy)]
pub(super) enum BrokerCleanupOperation {
    CleanExpiredConsumerQueue,
    DeleteExpiredCommitLog,
    CleanUnusedTopic,
}

impl BrokerCleanupOperation {
    async fn execute(
        self,
        api: &Arc<MQClientAPIImpl>,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> crate::ClientResult<bool> {
        match self {
            BrokerCleanupOperation::CleanExpiredConsumerQueue => {
                api.clean_expired_consume_queue(addr, timeout_millis).await
            }
            BrokerCleanupOperation::DeleteExpiredCommitLog => api.delete_expired_commit_log(addr, timeout_millis).await,
            BrokerCleanupOperation::CleanUnusedTopic => api.clean_unused_topic(addr, timeout_millis).await,
        }
    }
}

pub(super) fn cluster_names_for_admin_operation(
    cluster_info: &ClusterInfo,
    cluster: Option<CheetahString>,
) -> Vec<CheetahString> {
    if let Some(cluster) = cluster.filter(|cluster| !cluster.is_empty()) {
        return vec![cluster];
    }

    cluster_info
        .cluster_addr_table
        .as_ref()
        .map(|table| table.keys().cloned().collect())
        .unwrap_or_default()
}

pub(super) fn broker_addrs_for_cluster(cluster_info: &ClusterInfo, cluster: &CheetahString) -> Vec<CheetahString> {
    let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() else {
        return Vec::new();
    };
    let Some(broker_names) = cluster_addr_table.get(cluster) else {
        return Vec::new();
    };
    let Some(broker_addr_table) = cluster_info.broker_addr_table.as_ref() else {
        return Vec::new();
    };

    let mut addrs = Vec::new();
    for broker_name in broker_names {
        if let Some(broker_data) = broker_addr_table.get(broker_name) {
            addrs.extend(broker_data.broker_addrs().values().cloned());
        }
    }
    addrs
}

impl DefaultMQAdminExtImpl {
    pub(super) async fn execute_broker_cleanup_operation(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
        operation: BrokerCleanupOperation,
    ) -> crate::ClientResult<bool> {
        let timeout = self.remoting_timeout_millis()?;
        let api = self.mq_client_api()?;
        let addresses = if let Some(address) = addr.filter(|address| !address.is_empty()) {
            vec![address]
        } else {
            let cluster_info = api.get_broker_cluster_info(timeout).await?;
            cluster_names_for_admin_operation(&cluster_info, cluster)
                .into_iter()
                .flat_map(|cluster| broker_addrs_for_cluster(&cluster_info, &cluster))
                .collect()
        };
        let mut report = crate::admin::AdminNodeReport {
            nodes: Vec::new(),
            not_attempted: Vec::new(),
        };
        let mut addresses = addresses.into_iter();
        while let Some(address) = addresses.next() {
            let result = operation.execute(&api, &address, timeout).await;
            let failed = result.is_err();
            report.nodes.push(crate::admin::AdminNodeResult { address, result });
            if failed {
                report.not_attempted.extend(addresses);
                break;
            }
        }
        Ok(report.into_result()?.last().is_some_and(|(_, result)| *result))
    }
}
