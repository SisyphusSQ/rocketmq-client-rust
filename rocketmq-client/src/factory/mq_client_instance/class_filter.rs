// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.

use super::*;

// Identity is already fixed by the owning MQClientInstance; source itself stays out of diagnostics.
type RegistrationKey = (CheetahString, CheetahString, CheetahString);
pub(super) type RegistrationCache = HashMap<RegistrationKey, (CheetahString, CheetahString)>;

impl MQClientInstance {
    fn class_subscription(&self, group: &CheetahString, topic: &CheetahString) -> Option<SubscriptionData> {
        self.consumer_table.get(group).and_then(|consumer| {
            consumer
                .subscriptions()
                .into_iter()
                .find(|sub| sub.topic == *topic && sub.class_filter_mode)
        })
    }

    pub(crate) async fn ensure_class_filter_registered(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        address: &CheetahString,
        force: bool,
    ) -> ClientResult<()> {
        // Async owner lock serializes snapshot, register and ACK publication. No DashMap guard crosses await.
        let mut cache = self.class_filter_registrations.lock().await;
        let subscription = self.class_subscription(group, topic).ok_or_else(|| {
            ClientError::response_process_failed("registerMessageFilterClass", "current class subscription is absent")
        })?;
        let key = (group.clone(), topic.clone(), address.clone());
        let Some(body) = subscription.filter_class_source else {
            cache.remove(&key);
            return Ok(());
        };
        let source = (subscription.sub_string, body);
        if !force && cache.get(&key) == Some(&source) {
            return Ok(());
        }
        let crc = rocketmq_model::utils::crc32_utils::crc32(source.1.as_bytes()) as i32;
        let result = self
            .get_mq_client_api_impl()?
            .register_message_filter_class(
                address,
                group.clone(),
                topic.clone(),
                source.0.clone(),
                crc,
                source.1.as_bytes(),
                5000,
            )
            .await;
        match result {
            Ok(()) => {
                cache.insert(key, source);
                Ok(())
            }
            Err(error) => {
                cache.remove(&key);
                Err(error)
            }
        }
    }

    pub(crate) async fn upload_filter_class_sources_for_group(&self, group: &CheetahString) -> ClientResult<()> {
        let topics = self
            .consumer_table
            .get(group)
            .map(|consumer| {
                consumer
                    .subscriptions()
                    .into_iter()
                    .filter(|sub| sub.class_filter_mode && sub.filter_class_source.is_some())
                    .map(|sub| sub.topic)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut first_error = None;
        for topic in topics {
            let mut addresses = self
                .topic_route_table
                .get(&topic)
                .map(|route| {
                    route
                        .filter_server_table
                        .values()
                        .flatten()
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            addresses.sort();
            addresses.dedup();
            if addresses.is_empty() {
                first_error.get_or_insert_with(|| {
                    ClientError::response_process_failed(
                        "registerMessageFilterClass",
                        "topic route has no FilterServer for the class subscription",
                    )
                });
            }
            for address in addresses {
                if let Err(error) = self.ensure_class_filter_registered(group, &topic, &address, true).await {
                    first_error.get_or_insert(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) async fn upload_filter_class_sources(&self) -> ClientResult<()> {
        let groups = self.consumer_snapshot();
        let valid = groups
            .iter()
            .flat_map(|(group, consumer)| {
                consumer
                    .subscriptions()
                    .into_iter()
                    .filter(|sub| sub.class_filter_mode && sub.filter_class_source.is_some())
                    .flat_map(|sub| {
                        self.topic_route_table
                            .get(&sub.topic)
                            .map(|route| {
                                route
                                    .filter_server_table
                                    .values()
                                    .flatten()
                                    .map(|address| (group.clone(), sub.topic.clone(), address.clone()))
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<HashSet<_>>();
        self.class_filter_registrations
            .lock()
            .await
            .retain(|key, _| valid.contains(key));
        let mut first_error = None;
        for (group, _) in groups {
            if let Err(error) = self.upload_filter_class_sources_for_group(&group).await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}
