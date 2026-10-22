// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::admin::AclConfig;
#[cfg(feature = "admin-mutation")]
use crate::admin::{AdminNodeReport, AdminNodeResult};

impl MQClientAPIImpl {
    /// One Java resource RPC, without an invented retry or a shared cross-node timeout.
    pub(super) async fn resource_response(
        &self,
        operation: &'static str,
        address: Option<&CheetahString>,
        request: RemotingCommand,
        timeout: u64,
    ) -> ClientResult<RemotingCommand> {
        let response = match self.remoting_client.invoke_request(address, request, timeout).await {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(value)) => {
                return Err(admin_request_error(operation, RetryInput::Rejected(value)))
            }
            Ok(OutboundRequestOutcome::Contract(value)) => {
                return Err(admin_request_error(operation, RetryInput::Contract(value)))
            }
            Err(error) => return Err(ClientError::from_transport(error)),
        };
        if response.code() != ResponseCode::Success as i32 {
            return Err(admin_broker_response_error(
                operation,
                &response,
                address.map_or("NameServer", |value| value.as_str()),
            ));
        }
        Ok(response)
    }

    #[cfg(feature = "admin-mutation")]
    pub(super) async fn resource_write_nodes(
        &self,
        operation: &'static str,
        addresses: Vec<CheetahString>,
        request: RemotingCommand,
        timeout: u64,
    ) -> AdminNodeReport<()> {
        self.resource_write_nodes_with_policy(operation, addresses, request, timeout, false)
            .await
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn resource_write_nodes_with_policy(
        &self,
        operation: &'static str,
        addresses: Vec<CheetahString>,
        request: RemotingCommand,
        timeout: u64,
        stop_on_server_error: bool,
    ) -> AdminNodeReport<()> {
        let mut report = AdminNodeReport {
            nodes: Vec::new(),
            not_attempted: Vec::new(),
        };
        let mut addresses = addresses.into_iter();
        while let Some(address) = addresses.next() {
            let result = self
                .resource_response(operation, Some(&address), request.clone(), timeout)
                .await
                .map(|_| ());
            // Java continues after server responses, and stops on transport/decoding exceptions.
            let stop = result
                .as_ref()
                .err()
                .is_some_and(|error| stop_on_server_error || error.broker_response_code().is_none());
            report.nodes.push(AdminNodeResult { address, result });
            if stop {
                report.not_attempted.extend(addresses);
                break;
            }
        }
        report
    }

    pub(crate) async fn get_broker_cluster_acl_config(
        &self,
        address: CheetahString,
        timeout: u64,
    ) -> ClientResult<AclConfig> {
        let request = self.create_remoting_command(RequestCode::GetBrokerClusterAclConfig);
        let address = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, address.as_str());
        let response = self
            .resource_response("getBrokerClusterAclConfig", Some(&address), request, timeout)
            .await?;
        resource_body(&response, "getBrokerClusterAclConfig")
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn delete_resource_topic(
        &self,
        addresses: Option<Vec<CheetahString>>,
        topic: CheetahString,
        broker: bool,
        timeout: u64,
    ) -> ClientResult<()> {
        let addresses = addresses.unwrap_or_else(|| self.get_name_server_address_list());
        let addresses = addresses
            .into_iter()
            .map(|address| {
                if broker {
                    mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, address.as_str())
                } else {
                    address
                }
            })
            .collect();
        let mut request = self.create_remoting_command(if broker {
            RequestCode::DeleteTopicInBroker
        } else {
            RequestCode::DeleteTopicInNamesrv
        });
        request.add_ext_field("topic", topic);
        self.resource_write_nodes_with_policy("deleteTopic", addresses, request, timeout, true)
            .await
            .into_result()
            .map(|_| ())
    }
}

pub(super) fn resource_body<T: DeserializeOwned>(response: &RemotingCommand, operation: &str) -> ClientResult<T> {
    let body = response
        .body()
        .ok_or_else(|| ClientError::response_process_failed(operation, "successful response has no body"))?;
    decode_admin_json(body)
}

pub(super) fn acl_version_header(response: &RemotingCommand) -> ClientResult<ClusterAclVersionInfo> {
    let fields = response.ext_fields();
    let get = |name: &str| {
        fields.and_then(|fields| fields.get(name)).cloned().ok_or_else(|| {
            ClientError::response_process_failed("decode ACL version header", format!("missing required {name}"))
        })
    };
    let version = serde_json::from_str(get("version")?.as_str())
        .map_err(|error| ClientError::response_process_source("decode ACL version header", error))?;
    Ok(ClusterAclVersionInfo {
        broker_name: Some(get("brokerName")?),
        broker_addr: Some(get("brokerAddr")?),
        cluster_name: Some(get("clusterName")?),
        acl_config_data_version: version,
        all_acl_config_data_version: HashMap::new(),
    })
}

pub(super) fn resource_properties(
    response: &RemotingCommand,
    operation: &str,
) -> ClientResult<HashMap<CheetahString, CheetahString>> {
    let body = response
        .body()
        .ok_or_else(|| ClientError::response_process_failed(operation, "successful response has no body"))?;
    let text = std::str::from_utf8(body).map_err(|error| ClientError::response_process_source(operation, error))?;
    mix_all::string_to_properties(text)
        .ok_or_else(|| ClientError::response_process_failed(operation, "invalid Java Properties data"))
}


