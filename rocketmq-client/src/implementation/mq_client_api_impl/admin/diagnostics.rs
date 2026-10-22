// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.

use super::*;

pub(super) fn diagnostic_response_body<'a>(response: &'a RemotingCommand, operation: &str) -> ClientResult<&'a [u8]> {
    response.body().map(|body| body.as_ref()).ok_or_else(|| {
        ClientError::response_process_failed(operation, "successful response has no body")
            .with_request_stage(OutboundRequestStage::ResponseReceived)
    })
}

impl MQClientAPIImpl {
    /// The fixed Java diagnostic/maintenance calls all use the Broker VIP mapping.
    pub(crate) async fn diagnostic_broker_response(
        &self,
        operation: &'static str,
        address: &CheetahString,
        request: RemotingCommand,
        timeout: u64,
    ) -> ClientResult<RemotingCommand> {
        let address = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, address.as_str());
        self.resource_response(operation, Some(&address), request, timeout)
            .await
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn invoke_broker_to_reset_offset_461(
        &self,
        address: &CheetahString,
        topic: CheetahString,
        group: CheetahString,
        timestamp: i64,
        force: bool,
        is_c: bool,
        timeout: u64,
    ) -> ClientResult<HashMap<MessageQueue, i64>> {
        let mut request = self.create_remoting_command(RequestCode::InvokeBrokerToResetOffset);
        request.add_ext_field("topic", topic);
        request.add_ext_field("group", group);
        request.add_ext_field("timestamp", timestamp.to_string());
        request.add_ext_field("isForce", force.to_string());
        if is_c {
            request = request.set_language(rocketmq_protocol::protocol::LanguageCode::CPP);
        }
        let response = self
            .diagnostic_broker_response("resetOffsetByTimestamp", address, request, timeout)
            .await?;
        let body = diagnostic_response_body(&response, "resetOffsetByTimestamp")?;
        ResetOffsetBody::decode(body)
            .map(|body| body.offset_table)
            .ok_or_else(|| {
                ClientError::response_process_failed("resetOffsetByTimestamp", "malformed offset response")
                    .with_request_stage(OutboundRequestStage::ResponseReceived)
            })
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn update_consumer_offset_461(
        &self,
        address: &CheetahString,
        header: UpdateConsumerOffsetRequestHeader,
        timeout: u64,
    ) -> ClientResult<()> {
        let mut request = self.create_remoting_command(RequestCode::UpdateConsumerOffset);
        request.add_ext_field("consumerGroup", header.consumer_group);
        request.add_ext_field("topic", header.topic);
        request.add_ext_field("queueId", header.queue_id.to_string());
        request.add_ext_field("commitOffset", header.commit_offset.to_string());
        self.diagnostic_broker_response("updateConsumeOffset", address, request, timeout)
            .await?;
        Ok(())
    }
}
