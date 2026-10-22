// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.

use super::group::is_consumer_not_online_error;
use super::topic::admin_route_not_found;
use super::*;
use crate::admin::{AdminNodeReport, AdminNodeResult};

impl DefaultMQAdminExtImpl {
    pub(super) fn remoting_timeout_times(&self, multiplier: u64) -> crate::ClientResult<u64> {
        self.remoting_timeout_millis()?.checked_mul(multiplier).ok_or_else(|| {
            ClientError::illegal_argument("admin timeout multiplier exceeds the supported millisecond range")
        })
    }

    /// Fixed Java signed timestamp, force and isC overloads. Successful prior nodes stay in typed errors.
    pub async fn reset_offset_by_timestamp_signed(
        &self,
        cluster: Option<CheetahString>,
        topic: CheetahString,
        group: CheetahString,
        timestamp: i64,
        force: bool,
        is_c: bool,
    ) -> crate::ClientResult<HashMap<MessageQueue, i64>> {
        let api = self.mq_client_api()?;
        let timeout = self.remoting_timeout_millis()?;
        let route = self
            .examine_topic_route_info(topic.clone())
            .await?
            .ok_or_else(|| admin_route_not_found(&topic))?;
        let addresses = route
            .broker_datas
            .iter()
            .filter(|broker| {
                cluster
                    .as_ref()
                    .is_none_or(|cluster| broker.cluster() == cluster.as_str())
            })
            .filter_map(|broker| broker.select_broker_addr())
            .collect::<Vec<_>>();
        let mut report = AdminNodeReport {
            nodes: Vec::new(),
            not_attempted: Vec::new(),
        };
        let mut addresses = addresses.into_iter();
        while let Some(address) = addresses.next() {
            let result = api
                .invoke_broker_to_reset_offset_461(
                    &address,
                    topic.clone(),
                    group.clone(),
                    timestamp,
                    force,
                    is_c,
                    timeout,
                )
                .await;
            let failed = result.is_err();
            report.nodes.push(AdminNodeResult { address, result });
            if failed {
                report.not_attempted.extend(addresses);
                break;
            }
        }
        Ok(report
            .into_result()?
            .into_iter()
            .flat_map(|(_, offsets)| offsets)
            .collect())
    }

    /// Old timestamp reset is an administrative write. The caller must keep consumers offline.
    pub async fn reset_offset_by_timestamp_old_signed(
        &self,
        cluster: Option<CheetahString>,
        group: CheetahString,
        topic: CheetahString,
        timestamp: i64,
        force: bool,
    ) -> crate::ClientResult<Vec<RollbackStats>> {
        self.mq_client_api()?;
        let route = self
            .examine_topic_route_info(topic.clone())
            .await?
            .ok_or_else(|| admin_route_not_found(&topic))?;
        let mut brokers = route
            .broker_datas
            .iter()
            .filter(|broker| {
                cluster
                    .as_ref()
                    .is_none_or(|cluster| broker.cluster() == cluster.as_str())
            })
            .filter_map(|broker| broker.select_broker_addr().map(|address| (broker, address)));
        let mut report = AdminNodeReport {
            nodes: Vec::new(),
            not_attempted: Vec::new(),
        };
        while let Some((broker, address)) = brokers.next() {
            let result = match route
                .queue_datas
                .iter()
                .find(|queues| queues.broker_name() == broker.broker_name())
            {
                Some(queues) => {
                    self.reset_offset_by_timestamp_old_on_broker(
                        address.clone(),
                        queues,
                        group.clone(),
                        topic.clone(),
                        timestamp,
                        force,
                    )
                    .await
                }
                None => Err(ClientError::response_process_failed(
                    "resetOffsetByTimestampOld",
                    "route has no matching queue configuration",
                )),
            };
            let failed = result.is_err();
            report.nodes.push(AdminNodeResult { address, result });
            if failed {
                report.not_attempted.extend(brokers.map(|(_, address)| address));
                break;
            }
        }
        Ok(report.into_result()?.into_iter().flat_map(|(_, rows)| rows).collect())
    }

    pub async fn reset_offset_new_signed(
        &self,
        group: CheetahString,
        topic: CheetahString,
        timestamp: i64,
    ) -> crate::ClientResult<()> {
        match self
            .reset_offset_by_timestamp_signed(None, topic.clone(), group.clone(), timestamp, true, false)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_consumer_not_online_error(&error) => {
                self.reset_offset_by_timestamp_old_signed(None, group, topic, timestamp, true)
                    .await?;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    pub async fn get_consume_status_signed(
        &self,
        topic: CheetahString,
        group: CheetahString,
        client: CheetahString,
    ) -> crate::ClientResult<HashMap<CheetahString, HashMap<MessageQueue, i64>>> {
        let api = self.mq_client_api()?;
        let route = self
            .examine_topic_route_info(topic.clone())
            .await?
            .ok_or_else(|| admin_route_not_found(&topic))?;
        let Some(address) = route
            .broker_datas
            .first()
            .and_then(|broker| broker.select_broker_addr())
        else {
            return Ok(HashMap::new());
        };
        api.invoke_broker_to_get_consumer_status(
            address.as_str(),
            topic,
            group,
            client,
            self.remoting_timeout_millis()?,
        )
        .await
    }

    pub async fn update_consume_offset_signed(
        &self,
        broker: CheetahString,
        group: CheetahString,
        queue: MessageQueue,
        offset: i64,
    ) -> crate::ClientResult<()> {
        self.mq_client_api()?
            .update_consumer_offset_461(
                &broker,
                UpdateConsumerOffsetRequestHeader {
                    consumer_group: group,
                    topic: queue.topic().clone(),
                    queue_id: queue.queue_id(),
                    commit_offset: offset,
                    topic_request_header: None,
                },
                self.remoting_timeout_millis()?,
            )
            .await
    }

    pub async fn query_consume_queue_signed(
        &self,
        broker: CheetahString,
        topic: CheetahString,
        queue_id: i32,
        index: i64,
        count: i32,
        group: CheetahString,
    ) -> crate::ClientResult<QueryConsumeQueueResponseBody> {
        self.mq_client_api()?
            .query_consume_queue(
                &broker,
                topic,
                queue_id,
                index,
                count,
                group,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    pub(super) async fn diagnostic_message(
        &self,
        topic: Option<CheetahString>,
        id: &CheetahString,
    ) -> crate::ClientResult<MessageExt> {
        self.mq_client_api()?;
        let admin = &self
            .client_instance
            .as_ref()
            .ok_or(ClientError::not_started())?
            .mq_admin_impl;
        match topic {
            Some(topic) => admin.view_message_by_topic_id(&topic, id).await,
            None => admin.view_message_by_offset_id(id).await,
        }
    }

    pub async fn consume_message_directly_by_offset_id(
        &self,
        group: CheetahString,
        client: CheetahString,
        id: CheetahString,
    ) -> crate::ClientResult<ConsumeMessageDirectlyResult> {
        self.consume_message_directly_461(group, client, None, id).await
    }

    pub(super) async fn consume_message_directly_461(
        &self,
        group: CheetahString,
        client: CheetahString,
        topic: Option<CheetahString>,
        id: CheetahString,
    ) -> crate::ClientResult<ConsumeMessageDirectlyResult> {
        let message = self.diagnostic_message(topic, &id).await?;
        let broker = CheetahString::from(message.store_host().to_string());
        let physical = if message
            .property(&MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX.into())
            .is_some()
        {
            message.msg_id().clone()
        } else {
            id
        };
        self.mq_client_api()?
            .consume_message_directly(
                &broker,
                ConsumeMessageDirectlyResultRequestHeader {
                    consumer_group: group,
                    client_id: Some(client),
                    msg_id: Some(physical),
                    broker_name: None,
                    topic: None,
                    topic_sys_flag: None,
                    group_sys_flag: None,
                    topic_request_header: None,
                },
                &message,
                self.remoting_timeout_times(3)?,
            )
            .await
    }

    pub async fn resume_check_half_message_by_offset_id(&self, id: CheetahString) -> crate::ClientResult<bool> {
        self.resume_check_half_message_461(None, id).await
    }

    pub(super) async fn resume_check_half_message_461(
        &self,
        topic: Option<CheetahString>,
        id: CheetahString,
    ) -> crate::ClientResult<bool> {
        let message = self.diagnostic_message(topic, &id).await?;
        let physical = if message
            .property(&MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX.into())
            .is_some()
        {
            message.msg_id().clone()
        } else {
            id
        };
        self.mq_client_api()?
            .resume_check_half_message(
                &message.store_host().to_string().into(),
                CheetahString::default(),
                physical,
                self.remoting_timeout_millis()?,
            )
            .await
    }
}

// Java MQAdminExt extends MQAdmin. Reuse the complete shared native contract, including signed queries.
impl crate::MQAdmin for crate::DefaultMQAdminExt {
    fn admin_context(&self) -> crate::ClientResult<crate::base::mq_admin::MQAdminContext> {
        let inner = self.inner();
        inner.mq_client_api()?;
        Ok(crate::base::mq_admin::MQAdminContext {
            admin: inner
                .client_instance
                .as_ref()
                .ok_or(ClientError::not_started())?
                .mq_admin_impl
                .clone(),
            config: Arc::new(inner.client_config.clone()),
        })
    }
}
