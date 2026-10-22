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

use super::*;
#[cfg(feature = "observability")]
use tracing::Instrument;
#[allow(unused_must_use)]
#[allow(unused_assignments)]
impl DefaultMQProducerImpl {
    #[inline]
    pub(super) fn request_correlation_id<M: MessageTrait>(msg: &M) -> crate::ClientResult<CheetahString> {
        msg.property(&CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID))
            .ok_or_else(|| mq_client_err!("Request correlation id was not set before sending request message"))
    }

    #[inline]
    pub async fn send_with_timeout<T>(&self, msg: &mut T, timeout: u64) -> crate::ClientResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        let deadline = RequestDeadline::from_timeout_millis(timeout);
        self.send_default_impl(msg, CommunicationMode::Sync, None, deadline)
            .await
    }

    #[inline]
    pub async fn send<T>(&self, msg: &mut T) -> crate::ClientResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        let runtime = self.runtime_snapshot();
        let deadline = RequestDeadline::from_timeout_millis(runtime.producer_config.send_msg_timeout() as u64);
        self.send_default_impl_with_runtime(msg, CommunicationMode::Sync, None, deadline, &runtime)
            .await
    }

    #[inline]
    pub async fn async_send_with_callback<T>(
        &self,
        msg: T,
        send_callback: Option<ArcSendCallback>,
    ) -> crate::ClientResult<()>
    where
        T: MessageTrait + Send + Sync,
    {
        let runtime = self.runtime_snapshot();
        self.async_send_with_callback_timeout(msg, send_callback, runtime.producer_config.send_msg_timeout() as u64)
            .await
    }

    #[inline]
    pub async fn sync_send_with_message_queue<T>(
        &self,
        msg: T,
        mq: MessageQueue,
    ) -> crate::ClientResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        let runtime = self.runtime_snapshot();
        self.sync_send_with_message_queue_timeout(msg, mq, runtime.producer_config.send_msg_timeout() as u64)
            .await
    }

    #[inline]
    pub async fn send_oneway<T>(&self, mut msg: T) -> crate::ClientResult<()>
    where
        T: MessageTrait + Send + Sync,
    {
        let runtime = self.runtime_snapshot();
        let deadline = RequestDeadline::from_timeout_millis(runtime.producer_config.send_msg_timeout() as u64);
        self.send_default_impl_with_runtime(&mut msg, CommunicationMode::Oneway, None, deadline, &runtime)
            .await?;
        Ok(())
    }

    pub async fn send_oneway_with_message_queue<T>(&self, mut msg: T, mq: MessageQueue) -> crate::ClientResult<()>
    where
        T: MessageTrait + Send + Sync,
    {
        self.make_sure_state_ok()?;
        let runtime = self.runtime_snapshot();
        let deadline = RequestDeadline::from_timeout_millis(runtime.producer_config.send_msg_timeout() as u64);
        Validators::check_message(Some(&msg), runtime.producer_config.as_ref())?;

        self.send_kernel_impl_with_runtime(&mut msg, &mq, CommunicationMode::Oneway, None, None, deadline, &runtime)
            .await
            .map_err(|input| producer_retry_input_error(input, None))?;
        Ok(())
    }

    /// Admits a batch of messages into the bounded one-way egress.
    ///
    /// Accepted messages are processed by the producer's fixed worker set. Each message keeps its
    /// byte reservation until the transport writer completes, so the batch cannot create one task
    /// or one independent memory allocation budget per message.
    ///
    /// # Arguments
    /// * `msgs` - Iterator of messages to send
    ///
    /// The return value is the number of messages accepted by the egress. Invalid messages and
    /// messages without a current publish route are skipped. Capacity or lifecycle rejection is
    /// returned immediately and no request is built for that rejected message.
    ///
    /// # Errors
    ///
    /// Returns an error if the producer is not running, request construction fails, the deadline
    /// has already expired, or the bounded egress rejects admission.
    ///
    /// # Example
    /// ```rust,ignore
    /// let messages = vec![msg1, msg2, msg3];
    /// producer.send_oneway_batch(messages).await?;
    /// ```
    pub async fn send_oneway_batch<T>(&self, msgs: impl IntoIterator<Item = T>) -> crate::ClientResult<usize>
    where
        T: MessageTrait + Send + Sync + 'static,
    {
        self.make_sure_state_ok()?;

        let runtime = self.runtime_snapshot();
        let timeout = runtime.producer_config.send_msg_timeout() as u64;
        let mut sent_count = 0;

        for mut msg in msgs {
            // Validate each message
            if let Err(e) = Validators::check_message(Some(&msg), runtime.producer_config.as_ref()) {
                tracing::debug!("Message validation failed in batch oneway: {:?}", e);
                continue;
            }

            let topic = msg.topic().clone();
            let topic_publish_info = self.try_to_find_topic_publish_info_with_runtime(&topic, &runtime).await;

            if let Some(info) = topic_publish_info {
                if info.ok() {
                    if let Some(mq) = self.select_one_message_queue(&info, None, false) {
                        let client_instance = self.client_instance()?;
                        let broker_name = client_instance.get_broker_name_from_message_queue(&mq).await;
                        let Some(broker_addr) = client_instance.find_broker_address_in_publish(broker_name.as_ref())
                        else {
                            continue;
                        };
                        let broker_addr = mix_all::broker_vip_channel(
                            runtime.client_config.vip_channel_enabled,
                            broker_addr.as_str(),
                        );
                        let mq_client_api = client_instance.get_mq_client_api_impl()?;
                        let command_factory = mq_client_api.remoting_command_factory();
                        let send_config = runtime.send_config.clone();
                        let namespace = runtime.client_config.namespace.clone();
                        let retained_bytes = Self::message_body_len_for_backpressure(&msg).saturating_add(4 * 1024);
                        let deadline = rocketmq_transport::api::RequestDeadline::from_timeout_millis(timeout);
                        let admission = self.oneway_egress()?.try_admit(retained_bytes, deadline, || {
                            let request = build_oneway_request_internal(
                                &mut msg,
                                &mq,
                                &broker_name,
                                &send_config,
                                command_factory,
                                namespace.as_deref(),
                            )?;
                            Ok(OnewayEnvelope {
                                broker_addr,
                                deadline,
                                send: Box::new(move |broker_addr, deadline, permit| {
                                    Box::pin(async move {
                                        mq_client_api
                                            .send_oneway_with_permit(&broker_addr, request, deadline, permit)
                                            .await
                                    })
                                }),
                            })
                        })?;
                        match admission {
                            OnewayAdmissionOutcome::Accepted => sent_count += 1,
                            OnewayAdmissionOutcome::Rejected(rejection) => {
                                let _ = rejection;
                            }
                        }
                    }
                }
            }
        }

        Ok(sent_count)
    }

    #[inline]
    pub async fn sync_send_with_message_queue_timeout<T>(
        &self,
        mut msg: T,
        mq: MessageQueue,
        timeout: u64,
    ) -> crate::ClientResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        let deadline = RequestDeadline::from_timeout_millis(timeout);
        self.make_sure_state_ok()?;
        let runtime = self.runtime_snapshot();
        Validators::check_message(Some(&msg), runtime.producer_config.as_ref())?;

        if msg.topic() != mq.topic_str() {
            return Err(mq_client_err!("message's topic not equal mq's topic"));
        }
        if deadline.is_expired() {
            return Err(crate::ClientError::timeout("send_with_timeout", timeout));
        }
        self.send_kernel_impl_with_runtime(&mut msg, &mq, CommunicationMode::Sync, None, None, deadline, &runtime)
            .await
            .map_err(|input| producer_retry_input_error(input, None))
    }

    #[inline]
    pub async fn async_send_with_message_queue_callback<T>(
        &self,
        msg: T,
        mq: MessageQueue,
        send_callback: Option<ArcSendCallback>,
    ) -> crate::ClientResult<()>
    where
        T: MessageTrait + Send + Sync,
    {
        let runtime = self.runtime_snapshot();
        self.async_send_batch_to_queue_with_callback_timeout(
            msg,
            mq,
            send_callback,
            runtime.producer_config.send_msg_timeout() as u64,
        )
        .await
    }

    pub async fn send_with_selector_callback_timeout<M, S, T>(
        &self,
        msg: M,
        selector: S,
        arg: T,
        send_callback: Option<ArcSendCallback>,
        timeout: u64,
    ) -> crate::ClientResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        let deadline = RequestDeadline::from_timeout_millis(timeout);
        let producer_impl = self.self_reference()?;
        let msg_len = Self::message_body_len_for_backpressure(&msg);
        let send_callback_clone = send_callback.clone();
        let future = async move {
            if deadline.is_expired() {
                producer_impl
                    .notify_callback_exception(&send_callback_clone, &Self::async_send_rejected_error("call timeout"))
                    .await;
                return Ok(None);
            }

            // Custom: pinned Java notifies the callback when accepted selector work
            // fails before the transport owns the callback (for example, None selection).
            let result = producer_impl
                .send_select_impl(
                    msg,
                    selector,
                    arg,
                    CommunicationMode::Async,
                    send_callback_clone.clone(),
                    deadline,
                )
                .await;
            if let Err(error) = &result {
                producer_impl
                    .notify_callback_exception(&send_callback_clone, error)
                    .await;
            }
            result
        };
        self.execute_async_message_send(future, send_callback, deadline, msg_len)
            .await
    }

    pub async fn send_oneway_with_selector<M, S, T>(&self, msg: M, selector: S, arg: T) -> crate::ClientResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync,
    {
        let runtime = self.runtime_snapshot();
        let deadline = RequestDeadline::from_timeout_millis(runtime.producer_config.send_msg_timeout() as u64);
        self.send_select_impl(msg, selector, arg, CommunicationMode::Oneway, None, deadline)
            .await?;
        Ok(())
    }

    pub async fn send_select_impl<M, S, T>(
        &self,
        mut msg: M,
        selector: S,
        arg: T,
        communication_mode: CommunicationMode,
        send_message_callback: Option<ArcSendCallback>,
        deadline: RequestDeadline,
    ) -> crate::ClientResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync,
        T: Send + Sync,
    {
        let runtime = self.runtime_snapshot();
        self.make_sure_state_ok()?;
        Validators::check_message(Some(&msg), runtime.producer_config.as_ref())?;
        let topic_publish_info = self
            .try_to_find_topic_publish_info_with_runtime(msg.topic(), &runtime)
            .await;
        if let Some(topic_publish_info) = topic_publish_info {
            if topic_publish_info.ok() {
                let client_instance = self.client_instance()?;
                let message_queue_list = client_instance
                    .mq_admin_impl
                    .parse_publish_message_queues(&topic_publish_info.message_queue_list, &runtime.client_config);
                let message_queue = Self::select_message_queue_with_user_message(
                    &runtime.client_config,
                    &message_queue_list,
                    &mut msg,
                    &selector,
                    &arg,
                );
                if deadline.is_expired() {
                    return Err(crate::ClientError::timeout("sendSelectImpl", deadline.budget_millis()));
                }
                if let Some(message_queue) = message_queue {
                    return self
                        .send_kernel_impl_with_runtime(
                            &mut msg,
                            &message_queue,
                            communication_mode,
                            send_message_callback,
                            None,
                            deadline,
                            &runtime,
                        )
                        .await
                        .map_err(|input| producer_retry_input_error(input, None));
                }
                return Err(mq_client_err!("select message queue return null."));
            }
        }
        self.validate_name_server_setting()?;
        Err(mq_client_err!(format!("No route info for this topic, {}", msg.topic())))
    }

    #[inline]
    pub async fn async_send_batch_to_queue_with_callback_timeout<T>(
        &self,
        mut msg: T,
        mq: MessageQueue,
        send_callback: Option<ArcSendCallback>,
        timeout: u64,
    ) -> crate::ClientResult<()>
    where
        T: MessageTrait + Send + Sync,
    {
        let producer_impl = self.self_reference()?;
        let deadline = RequestDeadline::from_timeout_millis(timeout);
        let send_callback_inner = send_callback.clone();
        let msg_len = Self::message_body_len_for_backpressure(&msg);
        let future = async move {
            if let Err(err) = producer_impl.make_sure_state_ok() {
                producer_impl
                    .notify_callback_exception(&send_callback_inner, &err)
                    .await;
                return;
            }
            let runtime = producer_impl.runtime_snapshot();
            if let Err(err) = Validators::check_message(Some(&msg), runtime.producer_config.as_ref()) {
                producer_impl
                    .notify_callback_exception(&send_callback_inner, &err)
                    .await;
                return;
            }
            if msg.topic() != mq.topic_str() {
                let err = mq_client_err!("Topic of the message does not match its target message queue");
                producer_impl
                    .notify_callback_exception(&send_callback_inner, &err)
                    .await;
                return;
            }

            if deadline.is_expired() {
                producer_impl
                    .notify_callback_exception(&send_callback_inner, &Self::async_send_rejected_error("call timeout"))
                    .await;
                return;
            }
            let result = producer_impl
                .send_kernel_impl_with_runtime(
                    &mut msg,
                    &mq,
                    CommunicationMode::Async,
                    send_callback_inner.clone(),
                    None,
                    deadline,
                    &runtime,
                )
                .await;
            match result {
                Ok(_) => {}
                Err(input) => {
                    let err = producer_retry_input_error(input, None);
                    producer_impl
                        .notify_callback_exception(&send_callback_inner, &err)
                        .await;
                }
            }
        };

        self.execute_async_message_send(future, send_callback, deadline, msg_len)
            .await
    }

    pub async fn async_send_with_callback_timeout<T>(
        &self,
        mut msg: T,
        send_callback: Option<ArcSendCallback>,
        timeout: u64,
    ) -> crate::ClientResult<()>
    where
        T: MessageTrait + Send + Sync,
    {
        let producer_impl = self.self_reference()?;
        let deadline = RequestDeadline::from_timeout_millis(timeout);
        let send_callback_inner = send_callback.clone();
        let msg_len = Self::message_body_len_for_backpressure(&msg);
        let future = async move {
            if deadline.is_expired() {
                producer_impl
                    .notify_callback_exception(
                        &send_callback_inner,
                        &Self::async_send_rejected_error("asyncSend call timeout"),
                    )
                    .await;
                return;
            }

            let result = Box::pin(producer_impl.send_default_impl(
                &mut msg,
                CommunicationMode::Async,
                send_callback_inner.clone(),
                deadline,
            ))
            .await;
            match result {
                Ok(_) => {}
                Err(err) => {
                    producer_impl
                        .notify_callback_exception(&send_callback_inner, &err)
                        .await;
                }
            }
        };

        self.execute_async_message_send(future, send_callback, deadline, msg_len)
            .await
    }

    pub(super) async fn execute_async_message_send<F>(
        &self,
        f: F,
        send_callback: Option<ArcSendCallback>,
        deadline: RequestDeadline,
        msg_len: usize,
    ) -> crate::ClientResult<()>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let runtime = self.runtime_snapshot();
        let is_enable_backpressure_for_async_mode = runtime.producer_config.enable_backpressure_for_async_mode();

        let (acquire_value_num, acquire_value_size) = if is_enable_backpressure_for_async_mode {
            //back pressure
            if deadline.is_expired() {
                self.notify_callback_exception(
                    &send_callback,
                    &Self::async_send_rejected_error("send message tryAcquire semaphoreAsyncNum timeout"),
                )
                .await;
                return Ok(());
            }
            let result = deadline
                .timeout(self.semaphore_async_send_num.clone().acquire_owned())
                .await;
            let acquire_value_num = match result {
                Ok(acquire_value) => match acquire_value {
                    Ok(value) => Some(value),
                    Err(_) => {
                        self.notify_callback_exception(
                            &send_callback,
                            &Self::async_send_rejected_error("send message tryAcquire semaphoreAsyncNum timeout"),
                        )
                        .await;
                        return Ok(());
                    }
                },
                Err(_) => {
                    self.notify_callback_exception(
                        &send_callback,
                        &Self::async_send_rejected_error("send message tryAcquire semaphoreAsyncNum timeout"),
                    )
                    .await;
                    return Ok(());
                }
            };

            //message size
            if deadline.is_expired() {
                self.notify_callback_exception(
                    &send_callback,
                    &Self::async_send_rejected_error("send message tryAcquire semaphoreAsyncSize timeout"),
                )
                .await;
                return Ok(());
            }
            let result = deadline
                .timeout(
                    self.semaphore_async_send_size
                        .clone()
                        .acquire_many_owned(msg_len as u32),
                )
                .await;
            let acquire_value_size = match result {
                Ok(acquire_value) => match acquire_value {
                    Ok(value) => Some(value),
                    Err(_) => {
                        self.notify_callback_exception(
                            &send_callback,
                            &Self::async_send_rejected_error("send message tryAcquire semaphoreAsyncSize timeout"),
                        )
                        .await;
                        return Ok(());
                    }
                },
                Err(_) => {
                    self.notify_callback_exception(
                        &send_callback,
                        &Self::async_send_rejected_error("send message tryAcquire semaphoreAsyncSize timeout"),
                    )
                    .await;
                    return Ok(());
                }
            };
            (acquire_value_num, acquire_value_size)
        } else {
            (None, None)
        };
        let task = async move {
            let _acquire_value_num = acquire_value_num;
            let _acquire_value_size = acquire_value_size;
            f.await;
        };
        if let Err(error) = self.spawn_tracked_task("rocketmq-client-producer-async-send", task) {
            let error = ClientError::service_source("producer_async_send", error);
            self.notify_callback_exception(&send_callback, &error).await;
            return Err(error);
        }
        Ok(())
    }

    pub(super) async fn send_default_impl<T>(
        &self,
        msg: &mut T,
        communication_mode: CommunicationMode,
        send_callback: Option<ArcSendCallback>,
        deadline: RequestDeadline,
    ) -> crate::ClientResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        let runtime = self.runtime_snapshot();
        self.send_default_impl_with_runtime(msg, communication_mode, send_callback, deadline, &runtime)
            .await
    }

    pub(super) async fn send_default_impl_with_runtime<T>(
        &self,
        msg: &mut T,
        communication_mode: CommunicationMode,
        send_callback: Option<ArcSendCallback>,
        deadline: RequestDeadline,
        runtime: &ProducerRuntimeSnapshot,
    ) -> crate::ClientResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        self.make_sure_state_ok()?;
        Validators::check_message(Some(&*msg), runtime.producer_config.as_ref())?;

        let topic = msg.topic().clone();
        if communication_mode == CommunicationMode::Oneway {
            if let Some(topic_publish_info) = self.try_to_find_topic_publish_info_with_runtime(&topic, runtime).await {
                if topic_publish_info.ok() {
                    let ctx = SendContext::new(deadline, communication_mode);
                    return self
                        .send_with_retry(msg, &topic, &topic_publish_info, send_callback, ctx, runtime)
                        .await;
                }
            }
            self.validate_name_server_setting()?;
            return Err(mq_client_err!(
                ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION,
                format!(
                    "No route info of this topic:{},{}",
                    topic,
                    FAQUrl::suggest_todo(FAQUrl::NO_TOPIC_ROUTE_INFO)
                )
            ));
        }
        let topic_publish_info = self
            .topic_publish_info_table
            .get(&topic)
            .map(|entry| Arc::clone(entry.value()))
            .unwrap_or_else(|| Arc::new(TopicPublishInfo::new()));
        let ctx = SendContext::new(deadline, communication_mode);
        self.send_with_retry(msg, &topic, &topic_publish_info, send_callback, ctx, runtime)
            .await
    }
}

#[allow(unused_must_use)]
#[allow(unused_assignments)]
impl DefaultMQProducerImpl {
    pub(super) async fn send_kernel_impl<T>(
        &self,
        msg: &mut T,
        mq: &MessageQueue,
        communication_mode: CommunicationMode,
        send_callback: Option<ArcSendCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        timeout: u64,
    ) -> crate::ClientResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        let deadline = RequestDeadline::from_timeout_millis(timeout);
        let runtime = self.runtime_snapshot();
        #[cfg(feature = "observability")]
        {
            let client_instance = self.client_instance()?;
            let span = rocketmq_observability::trace::client::producer_send_span(client_instance.telemetry_handle());
            return self
                .send_kernel_impl_with_runtime(
                    msg,
                    mq,
                    communication_mode,
                    send_callback,
                    topic_publish_info,
                    deadline,
                    &runtime,
                )
                .instrument(span)
                .await
                .map_err(|input| producer_retry_input_error(input, None));
        }

        #[cfg(not(feature = "observability"))]
        self.send_kernel_impl_with_runtime(
            msg,
            mq,
            communication_mode,
            send_callback,
            topic_publish_info,
            deadline,
            &runtime,
        )
        .await
        .map_err(|input| producer_retry_input_error(input, None))
    }

    pub(super) async fn send_kernel_impl_with_runtime<T>(
        &self,
        msg: &mut T,
        mq: &MessageQueue,
        communication_mode: CommunicationMode,
        send_callback: Option<ArcSendCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        deadline: RequestDeadline,
        runtime: &ProducerRuntimeSnapshot,
    ) -> Result<Option<SendResult>, RetryInput>
    where
        T: MessageTrait + Send + Sync,
    {
        let begin_start_time = Instant::now();

        // Custom fork: public setters may update compression after start; never encode an unsupported body.
        runtime.producer_config.validate_compression()?;
        if runtime.client_config.send_latency_enable {
            MQFaultStrategy::validate_java_latency_policy(
                runtime.producer_config.latency_max(),
                runtime.producer_config.not_available_duration(),
            )?;
        }
        let client_instance = self.client_instance()?;
        #[cfg(feature = "observability")]
        let telemetry_handle = client_instance.telemetry_handle();

        let mut broker_name = client_instance.get_broker_name_from_message_queue(mq).await;
        let mut broker_addr = client_instance.find_broker_address_in_publish(broker_name.as_ref());
        if broker_addr.is_none() && topic_publish_info.is_none() {
            // Explicit queues can be sent before this producer has discovered any routes.
            // Refresh once within the send deadline, keeping the caller's queue and leaving
            // route retries for automatically selected queues to the existing retry owner.
            client_instance
                .refresh_topic_route_info_once(msg.topic(), deadline)
                .await?;
            broker_name = client_instance.get_broker_name_from_message_queue(mq).await;
            broker_addr = client_instance.find_broker_address_in_publish(broker_name.as_ref());
        }
        let Some(mut broker_addr) = broker_addr else {
            return Err(RetryInput::RouteUnavailable);
        };
        broker_addr = mix_all::broker_vip_channel(runtime.client_config.vip_channel_enabled, broker_addr.as_str());

        let batch = msg.as_any().downcast_ref::<MessageBatch>().is_some();
        if !batch {
            MessageClientIDSetter::set_uniq_id(msg);
        }
        #[cfg(feature = "observability")]
        rocketmq_observability::trace::record_current_message_properties_with_handle(
            telemetry_handle,
            msg.get_properties(),
            msg.get_body().map(|body| body.len()),
        );

        let namespace = runtime.client_config.resolved_namespace();
        let mut topic_with_namespace = false;
        if let Some(ref ns) = namespace {
            msg.set_instance_id(ns.clone());
            topic_with_namespace = true;
        }

        let mut sys_flag = 0i32;
        if self.try_to_compress_message(msg, &runtime.send_config)? {
            sys_flag |= MessageSysFlag::COMPRESSED_FLAG;
            // Custom fork: 4.6.1 uses bit 0 for ZLIB, without the 5.x compression-type bits.
        }

        let tran_msg_property = msg.property_ref(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TRANSACTION_PREPARED,
        ));
        // Java's wire flag uses Boolean.parseBoolean, while its trace label
        // deliberately tests the exact lowercase literal.
        let is_transaction_prepared = tran_msg_property.is_some_and(|value| value.eq_ignore_ascii_case("true"));
        let is_trace_transaction_prepared = tran_msg_property.is_some_and(|value| value.as_str() == "true");

        if is_transaction_prepared {
            sys_flag |= MessageSysFlag::TRANSACTION_PREPARED_TYPE;
        }

        if self.has_check_forbidden_hook() {
            let mut check_forbidden_context = CheckForbiddenContext {
                name_srv_addr: runtime.client_config.get_namesrv_addr(),
                group: Some(runtime.send_config.producer_group.clone()),
                communication_mode: Some(communication_mode),
                broker_addr: Some(broker_addr.clone()),
                message: None, // Actual mutable message is the second Hook argument.
                mq: Some(mq),
                unit_mode: runtime.send_config.unit_mode,
                ..Default::default()
            };
            self.execute_check_forbidden_hook_mut(&mut check_forbidden_context, msg)?;
        }

        // Build send message request header
        #[cfg(feature = "observability")]
        {
            let mut properties = msg.get_properties().clone();
            rocketmq_observability::inject_current_context_with_handle(telemetry_handle, &mut properties);
            msg.set_properties(properties);
        }

        let producer_group = &runtime.send_config.producer_group;
        let topic = msg.topic();
        let create_topic_key = &runtime.send_config.create_topic_key;

        let mut request_header = SendMessageRequestHeader {
            producer_group: producer_group.clone(),
            topic: topic.clone(),
            default_topic: create_topic_key.clone(),
            default_topic_queue_nums: runtime.send_config.default_topic_queue_nums,
            queue_id: mq.queue_id(),
            sys_flag,
            born_timestamp: current_millis() as i64,
            flag: msg.get_flag(),
            properties: Some(MessageDecoder::message_properties_to_string(msg.get_properties())),
            reconsume_times: Some(0),
            unit_mode: Some(runtime.send_config.unit_mode),
            batch: Some(batch),
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(broker_name.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        if request_header.topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            let reconsume_times = MessageAccessor::get_reconsume_time(msg);
            if let Some(value) = reconsume_times {
                request_header.reconsume_times = value.parse::<i32>().map_or(Some(0), Some);
                MessageAccessor::clear_property(msg, MessageConst::PROPERTY_RECONSUME_TIME);
            }

            let max_reconsume_times = MessageAccessor::get_max_reconsume_times(msg);
            if let Some(value) = max_reconsume_times {
                request_header.max_reconsume_times = value.parse::<i32>().map_or(Some(0), Some);
                MessageAccessor::clear_property(msg, MessageConst::PROPERTY_MAX_RECONSUME_TIMES);
            }
        }

        // Helper macro to create send_message_context for a message
        macro_rules! create_send_context {
            ($msg_ref:expr) => {
                if self.has_send_message_hook() {
                    let born_host = runtime.client_config.client_ip.clone();

                    // Check all delay message properties (aligned with Java implementation)
                    let has_delay_property = $msg_ref
                        .property_ref(&CheetahString::from_static_str(
                            MessageConst::PROPERTY_STARTDE_LIVER_TIME,
                        ))
                        .is_some()
                        || $msg_ref
                            .property_ref(&CheetahString::from_static_str(
                                MessageConst::PROPERTY_DELAY_TIME_LEVEL,
                            ))
                            .is_some()
                        || $msg_ref
                            .property_ref(&CheetahString::from_static_str(
                                MessageConst::PROPERTY_TIMER_DELIVER_MS,
                            ))
                            .is_some()
                        || $msg_ref
                            .property_ref(&CheetahString::from_static_str(
                                MessageConst::PROPERTY_TIMER_DELAY_SEC,
                            ))
                            .is_some()
                        || $msg_ref
                            .property_ref(&CheetahString::from_static_str(
                                MessageConst::PROPERTY_TIMER_DELAY_MS,
                            ))
                            .is_some();

                    let mut send_message_context = SendMessageContext {
                        producer: self.self_reference().ok(),
                        producer_group: Some(producer_group.clone()),
                        communication_mode: Some(communication_mode),
                        born_host,
                        broker_addr: Some(broker_addr.clone()),
                        message: None,
                        message_snapshot: Some(SendMessageTraceSnapshot::owned_message($msg_ref)),
                        message_trace_snapshot: Some(SendMessageTraceSnapshot::from_message($msg_ref)),
                        mq: Some(mq),
                        namespace: namespace.clone(),
                        // Custom fork: Java SendMessageContext defaults ordinary sends to Normal_Msg.
                        msg_type: Some(MessageType::NormalMsg),
                        trace_start_time: Some(current_millis()),
                        ..Default::default()
                    };

                    if is_trace_transaction_prepared {
                        send_message_context.msg_type = Some(MessageType::TransMsgHalf);
                    }
                    if has_delay_property {
                        send_message_context.msg_type = Some(MessageType::DelayMsg);
                    }

                    Some(send_message_context)
                } else {
                    None
                }
            };
        }

        let mut send_message_context = create_send_context!(msg);
        self.execute_send_message_hook_before_mut(&mut send_message_context, msg);
        // Custom: hooks may change properties; refresh the wire header after callbacks.
        request_header.properties = Some(MessageDecoder::message_properties_to_string(msg.get_properties()));
        request_header.topic = msg.topic().clone();
        request_header.flag = msg.get_flag();
        // Mutable native hooks edit the uncompressed message. Body setters clear
        // the cache, so refresh compression and its flag before constructing the frame.
        if self.has_send_message_hook() || self.has_check_forbidden_hook() {
            request_header.sys_flag &= !MessageSysFlag::COMPRESSED_FLAG;
            if msg.get_compressed_body().is_some() || self.try_to_compress_message(msg, &runtime.send_config)? {
                request_header.sys_flag |= MessageSysFlag::COMPRESSED_FLAG;
            }
        }
        if topic_with_namespace {
            // Restore original topic without namespace
            let origin_topic = NamespaceUtil::without_namespace_with_namespace(
                msg.topic(),
                runtime.client_config.resolved_namespace().unwrap_or_default().as_str(),
            );
            msg.set_topic(origin_topic.into());
        }

        let send_result = match communication_mode {
            CommunicationMode::Async => {
                if deadline.is_expired() {
                    return Err(crate::ClientError::timeout("sendKernelImpl", deadline.budget_millis()).into());
                }
                client_instance
                    .get_mq_client_api_impl()?
                    .send_message(
                        &broker_addr,
                        &broker_name,
                        msg,
                        request_header,
                        deadline,
                        communication_mode,
                        send_callback,
                        topic_publish_info,
                        Some(Arc::clone(&client_instance)),
                        runtime.producer_config.retry_times_when_send_async_failed(),
                        &mut send_message_context,
                        self,
                    )
                    .await
            }
            CommunicationMode::Oneway | CommunicationMode::Sync => {
                if deadline.is_expired() {
                    return Err(crate::ClientError::timeout("sendKernelImpl", deadline.budget_millis()).into());
                }
                client_instance
                    .get_mq_client_api_impl()?
                    .send_message_simple(
                        &broker_addr,
                        &broker_name,
                        msg,
                        request_header,
                        deadline,
                        communication_mode,
                        &mut send_message_context,
                        self,
                    )
                    .await
            }
        };

        client_instance.client_metrics().record_send(begin_start_time.elapsed());

        match send_result {
            Ok(result) => {
                if self.has_send_message_hook() {
                    if let Some(smc) = send_message_context.as_mut() {
                        smc.send_result = result.as_ref();
                    }
                    self.execute_send_message_hook_after_mut(&mut send_message_context);
                }
                Ok(result)
            }
            Err(err) => {
                if self.has_send_message_hook() {
                    if let Some(smc) = send_message_context.as_mut() {
                        smc.exception = Some(producer_retry_hook_error(&err));
                    }
                    self.execute_send_message_hook_after_mut(&mut send_message_context);
                }
                Err(err)
            }
        }
        // Message state is guaranteed to be restored before function returns
    }

    /// Custom: callable mutable counterpart of the Java public before-hook helper.
    pub fn execute_send_message_hook_before_mut<M: MessageTrait + Send + Sync>(
        &self,
        context: &mut Option<SendMessageContext<'_>>,
        msg: &mut M,
    ) {
        for hook in self.send_message_hooks().iter() {
            if let Some(context) = context.as_mut() {
                context.message_snapshot = Some(SendMessageTraceSnapshot::owned_message(msg));
                context.message_trace_snapshot = Some(SendMessageTraceSnapshot::from_message(msg));
            }
            // Java logs observer exceptions and continues; forbidden hooks are separate.
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hook.send_message_before_mut(context, msg);
            }))
            .is_err()
            {
                tracing::warn!(hook = hook.hook_name(), "send before hook panicked");
            }
        }
        if let Some(context) = context.as_mut() {
            context.message_trace_snapshot = Some(SendMessageTraceSnapshot::from_message(msg));
            context.message_snapshot = Some(SendMessageTraceSnapshot::owned_message(msg));
        }
    }

    pub fn execute_send_message_hook_before(&self, context: &Option<SendMessageContext<'_>>) {
        let hooks = self.send_message_hooks();
        for hook in hooks.iter() {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hook.send_message_before(context))).is_err() {
                tracing::warn!(hook = hook.hook_name(), "send before hook panicked");
            }
        }
    }

    pub fn execute_send_message_hook_after(&self, context: &Option<SendMessageContext<'_>>) {
        for hook in self.send_message_hooks().iter() {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hook.send_message_after(context))).is_err() {
                tracing::warn!(hook = hook.hook_name(), "send after hook panicked");
            }
        }
    }

    pub fn execute_send_message_hook_after_mut(&self, context: &mut Option<SendMessageContext<'_>>) {
        let hooks = self.send_message_hooks();
        for hook in hooks.iter() {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hook.send_message_after_mut(context))).is_err()
            {
                tracing::warn!(hook = hook.hook_name(), "send after hook panicked");
            }
        }
    }

    #[inline]
    pub(crate) fn send_message_hooks(&self) -> Arc<[Arc<dyn SendMessageHook>]> {
        Arc::clone(&self.send_message_hook_list.read())
    }

    #[inline]
    pub fn has_send_message_hook(&self) -> bool {
        !self.send_message_hook_list.read().is_empty()
    }

    pub(super) fn context_error(message: String) -> Arc<ClientError> {
        Arc::new(ClientError::response_process_failed("send_message", message))
    }

    #[inline]
    pub fn has_check_forbidden_hook(&self) -> bool {
        !self.check_forbidden_hook_list.read().is_empty()
    }

    #[inline]
    pub fn has_end_transaction_hook(&self) -> bool {
        !self.end_transaction_hook_list.read().is_empty()
    }

    pub fn execute_check_forbidden_hook(&self, context: &CheckForbiddenContext) -> crate::ClientResult<()> {
        let hooks = Arc::clone(&self.check_forbidden_hook_list.read());
        for hook in hooks.iter() {
            hook.check_forbidden(context)?;
        }
        Ok(())
    }

    /// Custom: preserve registration order when readonly and mutable adapters coexist.
    pub fn execute_check_forbidden_hook_mut(
        &self,
        context: &mut CheckForbiddenContext<'_>,
        message: &mut dyn MessageTrait,
    ) -> crate::ClientResult<()> {
        let hooks = Arc::clone(&self.check_forbidden_hook_list.read());
        for hook in hooks.iter() {
            let readable = CheckForbiddenContext {
                name_srv_addr: context.name_srv_addr.clone(),
                group: context.group.clone(),
                message: Some(message),
                mq: context.mq,
                broker_addr: context.broker_addr.clone(),
                communication_mode: context.communication_mode,
                send_result: context.send_result.clone(),
                exception: context.exception.clone(),
                arg: context.arg.take(),
                unit_mode: context.unit_mode,
            };
            let result = hook.check_forbidden(&readable);
            context.arg = readable.arg;
            result?;
            hook.check_forbidden_mut(context, message)?;
        }
        Ok(())
    }

    pub(super) fn try_to_compress_message<T: MessageTrait>(
        &self,
        msg: &mut T,
        send_config: &ProducerSendConfigSnapshot,
    ) -> crate::ClientResult<bool> {
        // Custom: Java compresses every ordinary Message subtype, not only the
        // concrete Message type. MessageBatch is the sole fixed-Java exclusion.
        if msg.as_any().downcast_ref::<MessageBatch>().is_some() {
            return Ok(false);
        }
        let Some(body) = msg.get_body() else {
            return Ok(false);
        };
        if body.len() < send_config.compress_msg_body_over_howmuch {
            return Ok(false);
        }
        // Java Deflater throws IllegalArgumentException here; it is not the
        // IOException fallback to an uncompressed message. Never send silently.
        if !(-1..=9).contains(&send_config.compress_level) {
            return Err(crate::ClientError::illegal_argument("ZLIB level must be -1 or 0..9"));
        }
        let Some(compressor) = send_config.compressor else {
            if self
                .compressor_missing_logged
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                tracing::warn!("tryToCompressMessage skipped: compressor is not configured");
            } else {
                tracing::debug!("tryToCompressMessage skipped: compressor is not configured");
            }
            return Ok(false);
        };
        match compressor.compress(body, send_config.compress_level) {
            Ok(data) => {
                msg.set_compressed_body_mut(data);
                Ok(true)
            }
            Err(error) => {
                tracing::error!("tryToCompressMessage exception: {:?}", error);
                Ok(false)
            }
        }
    }

    #[inline]
    pub fn select_one_message_queue(
        &self,
        tp_info: &TopicPublishInfo,
        last_broker_name: Option<&CheetahString>,
        reset_index: bool,
    ) -> Option<MessageQueue> {
        self.mq_fault_strategy
            .read()
            .select_one_message_queue(tp_info, last_broker_name, reset_index)
    }

    pub(super) fn validate_name_server_setting(&self) -> crate::ClientResult<()> {
        let binding = self.client_instance()?.get_mq_client_api_impl()?;
        let ns_list = binding.get_name_server_address_list();
        if ns_list.is_empty() {
            return Err(mq_client_err!(
                ClientErrorCode::NO_NAME_SERVER_EXCEPTION,
                format!(
                    "No name remoting_server address, please set it. {}",
                    FAQUrl::suggest_todo(FAQUrl::NAME_SERVER_ADDR_NOT_EXIST_URL)
                )
            ));
        }
        Ok(())
    }

    pub(super) async fn try_to_find_topic_publish_info(&self, topic: &Topic) -> Option<TopicPublishInfoSnapshot> {
        let runtime = self.runtime_snapshot();
        self.try_to_find_topic_publish_info_with_runtime(topic, &runtime).await
    }

    pub(super) async fn try_to_find_topic_publish_info_with_runtime(
        &self,
        topic: &Topic,
        runtime: &ProducerRuntimeSnapshot,
    ) -> Option<TopicPublishInfoSnapshot> {
        let mut topic_publish_info = self
            .topic_publish_info_table
            .get(topic)
            .map(|entry| Arc::clone(entry.value()));
        if !topic_publish_info.as_ref().is_some_and(|info| info.ok()) {
            self.topic_publish_info_table
                .insert(topic.clone(), Arc::new(TopicPublishInfo::new()));
            let Ok(client_instance) = self.client_instance() else {
                tracing::debug!(
                    "Skip topic route refresh for {} because MQClientInstance is not available",
                    topic
                );
                return self
                    .topic_publish_info_table
                    .get(topic)
                    .map(|entry| Arc::clone(entry.value()));
            };
            client_instance
                .update_topic_route_info_from_name_server_topic(topic)
                .await;
            topic_publish_info = self
                .topic_publish_info_table
                .get(topic)
                .map(|entry| Arc::clone(entry.value()));
        }

        let topic_publish_info_ref = topic_publish_info.as_ref()?;
        if topic_publish_info_ref.have_topic_router_info || topic_publish_info_ref.ok() {
            return topic_publish_info;
        }

        let Ok(client_instance) = self.client_instance() else {
            tracing::debug!(
                "Skip default topic route refresh for {} because MQClientInstance is not available",
                topic
            );
            return topic_publish_info;
        };
        client_instance
            .update_topic_route_info_from_name_server_default(topic, true, Some(&runtime.producer_config))
            .await;
        self.topic_publish_info_table
            .get(topic)
            .map(|entry| Arc::clone(entry.value()))
    }

    pub(super) fn make_sure_state_ok(&self) -> crate::ClientResult<()> {
        let current_state = ProducerState::from_u8(self.state.load(Ordering::Acquire));
        if current_state != ProducerState::Running {
            return Err(mq_client_err!(format!(
                "The producer service state not OK, {:?} {}",
                current_state,
                FAQUrl::suggest_todo(FAQUrl::CLIENT_SERVICE_NOT_OK)
            )));
        }
        Ok(())
    }

    /// Ensure producer is in running state (atomic check)
    /// Freeze hook lists from mutable to immutable (called once during start)
    pub(super) fn freeze_hook_lists(&self) {
        // Take ownership of pending hooks and convert to Arc<[_]>
        if let Some(send_hooks) = self.pending_send_hooks.lock().take() {
            if !send_hooks.is_empty() {
                let hooks: Arc<[Arc<dyn SendMessageHook>]> = send_hooks.into();
                tracing::info!("Frozen {} send message hooks", hooks.len());
                *self.send_message_hook_list.write() = hooks;
            }
        }

        if let Some(end_hooks) = self.pending_end_transaction_hooks.lock().take() {
            if !end_hooks.is_empty() {
                let hooks: Arc<[Arc<dyn EndTransactionHook>]> = end_hooks.into();
                tracing::info!("Frozen {} end transaction hooks", hooks.len());
                *self.end_transaction_hook_list.write() = hooks;
            }
        }

        if let Some(forbidden_hooks) = self.pending_forbidden_hooks.lock().take() {
            if !forbidden_hooks.is_empty() {
                let hooks: Arc<[Arc<dyn CheckForbiddenHook>]> = forbidden_hooks.into();
                tracing::info!("Frozen {} check forbidden hooks", hooks.len());
                *self.check_forbidden_hook_list.write() = hooks;
            }
        }
    }

    #[inline]
    pub(super) fn ensure_running(&self) -> crate::ClientResult<()> {
        if self.state.load(Ordering::Acquire) != ProducerState::Running as u8 {
            return Err(mq_client_err!(format!(
                "Producer is not running, current state: {:?}",
                ProducerState::from_u8(self.state.load(Ordering::Relaxed))
            )));
        }
        Ok(())
    }

    pub async fn invoke_message_queue_selector<M, S, T>(
        &self,
        msg: &mut M,
        selector: S,
        arg: &T,
        timeout: u64,
    ) -> crate::ClientResult<MessageQueue>
    where
        M: MessageTrait,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync,
        T: Send,
    {
        let begin_start_time = Instant::now();
        let runtime = self.runtime_snapshot();
        self.make_sure_state_ok()?;
        Validators::check_message(Some(msg), runtime.producer_config.as_ref())?;
        let topic_publish_info = self
            .try_to_find_topic_publish_info_with_runtime(msg.topic(), &runtime)
            .await;
        if let Some(topic_publish_info) = topic_publish_info {
            if topic_publish_info.ok() {
                let client_instance = self.client_instance()?;
                let message_queue_list = client_instance
                    .mq_admin_impl
                    .parse_publish_message_queues(&topic_publish_info.message_queue_list, &runtime.client_config);
                let message_queue = Self::select_message_queue_with_user_message(
                    &runtime.client_config,
                    &message_queue_list,
                    msg,
                    &selector,
                    arg,
                );
                let cost_time = begin_start_time.elapsed().as_millis() as u64;
                if timeout < cost_time {
                    return Err(crate::ClientError::timeout("sendSelectImpl", timeout));
                }
                if let Some(message_queue) = message_queue {
                    return Ok(runtime.client_config.queue_with_resolved_namespace(message_queue));
                }
                return Err(mq_client_err!("select message queue return None."));
            }
        }
        self.validate_name_server_setting()?;
        Err(mq_client_err!("select message queue return null."))
    }

    pub async fn send_with_selector_timeout<M, S, T>(
        &self,
        msg: M,
        selector: S,
        arg: T,
        timeout: u64,
    ) -> crate::ClientResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync,
        T: Send + Sync,
    {
        self.send_select_impl(
            msg,
            selector,
            arg,
            CommunicationMode::Sync,
            None,
            RequestDeadline::from_timeout_millis(timeout),
        )
        .await
    }

    pub async fn fetch_publish_message_queues(&self, topic: &CheetahString) -> crate::ClientResult<Vec<MessageQueue>> {
        self.make_sure_state_ok()?;
        let runtime = self.runtime_snapshot();
        let client_instance = self.client_instance()?;
        let mq_client_api_impl = client_instance
            .mq_client_api_impl
            .load_full()
            .ok_or_else(|| mq_client_err!("MQClientAPIImpl is not available; producer has not been started"))?;
        client_instance
            .mq_admin_impl
            .fetch_publish_message_queues(topic, mq_client_api_impl, &runtime.client_config)
            .await
    }

    pub async fn create_topic(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        attributes: HashMap<String, String>,
    ) -> crate::ClientResult<()> {
        self.make_sure_state_ok()?;
        self.client_instance()?
            .mq_admin_impl
            .create_topic(key, new_topic, queue_num, topic_sys_flag, attributes)
            .await
    }

    pub async fn search_offset(&self, mq: &MessageQueue, timestamp: u64) -> crate::ClientResult<i64> {
        self.make_sure_state_ok()?;
        let mq = self
            .runtime_snapshot()
            .client_config
            .queue_with_resolved_namespace(mq.clone());
        self.client_instance()?
            .mq_admin_impl
            .search_offset(&mq, timestamp)
            .await
    }

    pub async fn max_offset(&self, mq: &MessageQueue) -> crate::ClientResult<i64> {
        self.make_sure_state_ok()?;
        let mq = self
            .runtime_snapshot()
            .client_config
            .queue_with_resolved_namespace(mq.clone());
        self.client_instance()?.mq_admin_impl.max_offset(&mq).await
    }

    pub async fn min_offset(&self, mq: &MessageQueue) -> crate::ClientResult<i64> {
        self.make_sure_state_ok()?;
        let mq = self
            .runtime_snapshot()
            .client_config
            .queue_with_resolved_namespace(mq.clone());
        self.client_instance()?.mq_admin_impl.min_offset(&mq).await
    }

    pub async fn earliest_msg_store_time(&self, mq: &MessageQueue) -> crate::ClientResult<i64> {
        self.make_sure_state_ok()?;
        let mq = self
            .runtime_snapshot()
            .client_config
            .queue_with_resolved_namespace(mq.clone());
        self.client_instance()?.mq_admin_impl.earliest_msg_store_time(&mq).await
    }

    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
    ) -> crate::ClientResult<QueryResult> {
        self.make_sure_state_ok()?;
        self.client_instance()?
            .mq_admin_impl
            .query_message(topic, key, max_num, begin, end)
            .await
    }

    pub async fn query_message_by_uniq_key(&self, topic: &str, uniq_key: &str) -> crate::ClientResult<MessageExt> {
        // Custom fork: ID-derived month window, minus1s, not a rolling three-day cutoff.
        self.make_sure_state_ok()?;
        self.client_instance()?
            .mq_admin_impl
            .query_message_by_uniq_key(topic, uniq_key)
            .await
    }

    pub async fn view_message(&self, topic: &str, msg_id: &str) -> crate::ClientResult<MessageExt> {
        self.make_sure_state_ok()?;
        self.client_instance()?
            .mq_admin_impl
            .view_message_by_topic_id(topic, msg_id)
            .await
    }

    pub fn request_futures(&self) -> Vec<Arc<RequestResponseFuture>> {
        self.request_future_holder
            .pending_requests()
            .into_iter()
            .filter(|request| request.owned_by(&self.request_owner_id))
            .collect()
    }
    pub fn request_future(&self, id: &str) -> Option<Arc<RequestResponseFuture>> {
        self.request_future_holder
            .handle_if_owned(id, &self.request_owner_id)
            .map(|handle| handle.future().clone())
    }
    pub fn cancel_request_future(&self, id: &str) -> bool {
        self.request_future_holder
            .handle_if_owned(id, &self.request_owner_id)
            .is_some_and(|handle| handle.cancel())
    }
    pub fn track_request_future(
        &self,
        mut request: RequestResponseFuture,
    ) -> crate::ClientResult<crate::producer::request_future_holder::RequestHandle> {
        let _admission = self.task_admission.lock();
        self.make_sure_state_ok()?;
        if self.service_context.task_group().cancellation_token().is_cancelled() {
            return Err(ClientError::shutting_down());
        }
        request.set_owner(self.request_owner_id.clone(), &self.client_instance()?);
        self.request_future_holder.track(request)
    }
    pub fn pending_request_count(&self) -> usize {
        self.request_future_holder
            .pending_requests()
            .iter()
            .filter(|request| request.owned_by(&self.request_owner_id))
            .count()
    }
    // Custom fork: every overload shares the same owned registration and terminal state.
    async fn register_request<M: MessageTrait>(
        &self,
        msg: &mut M,
        timeout: u64,
        deadline: RequestDeadline,
        callback: Option<RequestCallbackFn>,
    ) -> crate::ClientResult<crate::producer::request_future_holder::RequestRegistration> {
        self.make_sure_state_ok()?;
        Validators::check_message(Some(msg), self.runtime_snapshot().producer_config.as_ref())?;
        deadline
            .timeout(self.prepare_send_request(msg, timeout))
            .await
            .map_err(|_| ClientError::timeout("prepare_send_request", timeout))??;
        let factory = self.client_instance()?;
        let id = Self::request_correlation_id(msg)?;
        let remaining = deadline.remaining().as_millis().saturating_add(1).min(i64::MAX as u128) as u64;
        let mut future = RequestResponseFuture::new(id, remaining, callback);
        future.set_owner(self.request_owner_id.clone(), &factory);
        let _admission = self.task_admission.lock();
        self.make_sure_state_ok()?;
        if self.service_context.task_group().cancellation_token().is_cancelled() {
            return Err(ClientError::shutting_down());
        }
        Ok(self.request_future_holder.register(Arc::new(future)))
    }
    fn request_send_callback(handle: crate::producer::request_future_holder::RequestHandle) -> ArcSendCallback {
        Arc::new(move |result: Option<&SendResult>, error: Option<&ClientError>| {
            if let Some(error) = error {
                handle.fail(error.clone());
            } else if result.is_some() {
                handle.future().set_send_request_ok(true);
            }
        })
    }
    async fn finish_request(
        &self,
        registration: crate::producer::request_future_holder::RequestRegistration,
        deadline: RequestDeadline,
    ) -> crate::ClientResult<Box<dyn MessageTrait + Send>> {
        let request = registration.handle().future().clone();
        let response = request.wait_response_message(deadline.remaining()).await;
        if let Some(response) = response {
            Ok(response)
        } else if let Some(cause) = request.get_cause() {
            Err((*cause).clone())
        } else {
            Err(ClientError::timeout("request_reply", deadline.budget_millis()))
        }
    }
    pub async fn request_with_selector<M, S, T>(
        &self,
        mut msg: M,
        selector: S,
        arg: T,
        timeout: u64,
    ) -> crate::ClientResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync,
    {
        let deadline = RequestDeadline::from_timeout_millis(timeout);
        let registration = self.register_request(&mut msg, timeout, deadline, None).await?;
        let callback = Self::request_send_callback(registration.handle());
        let result = deadline
            .timeout(self.send_select_impl(msg, selector, arg, CommunicationMode::Async, Some(callback), deadline))
            .await
            .map_err(|_| ClientError::timeout("request_send", timeout))
            .and_then(|result| result);
        if let Err(error) = result {
            registration.handle().fail(error.clone());
            return Err(error);
        }
        self.finish_request(registration, deadline).await
    }
    pub async fn request_with_selector_callback_handle<M, S, T>(
        &self,
        mut msg: M,
        selector: S,
        arg: T,
        request_callback: RequestCallbackFn,
        timeout: u64,
    ) -> crate::ClientResult<crate::producer::request_future_holder::RequestHandle>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync,
    {
        let deadline = RequestDeadline::from_timeout_millis(timeout);
        let registration = self
            .register_request(&mut msg, timeout, deadline, Some(request_callback))
            .await?;
        let callback = Self::request_send_callback(registration.handle());
        let result = deadline
            .timeout(self.send_select_impl(msg, selector, arg, CommunicationMode::Async, Some(callback), deadline))
            .await
            .map_err(|_| ClientError::timeout("request_send", timeout))
            .and_then(|result| result);
        if let Err(error) = result {
            registration.handle().fail(error.clone());
            return Err(error);
        }
        Ok(registration.detach())
    }
    pub async fn request_with_selector_callback<M, S, T>(
        &self,
        msg: M,
        selector: S,
        arg: T,
        request_callback: RequestCallbackFn,
        timeout: u64,
    ) -> crate::ClientResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync,
    {
        self.request_with_selector_callback_handle(msg, selector, arg, request_callback, timeout)
            .await
            .map(|_| ())
    }
    pub async fn request_to_queue<M>(
        &self,
        mut msg: M,
        mq: MessageQueue,
        timeout: u64,
    ) -> crate::ClientResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
    {
        let deadline = RequestDeadline::from_timeout_millis(timeout);
        let registration = self.register_request(&mut msg, timeout, deadline, None).await?;
        let callback = Self::request_send_callback(registration.handle());
        let runtime = self.runtime_snapshot();
        let result = deadline
            .timeout(self.send_kernel_impl_with_runtime(
                &mut msg,
                &mq,
                CommunicationMode::Async,
                Some(callback),
                None,
                deadline,
                &runtime,
            ))
            .await
            .map_err(|_| ClientError::timeout("request_send", timeout))
            .and_then(|result| result.map_err(|input| producer_retry_input_error(input, None)));
        if let Err(error) = result {
            registration.handle().fail(error.clone());
            return Err(error);
        }
        self.finish_request(registration, deadline).await
    }
    pub async fn request_to_queue_with_callback_handle<M>(
        &self,
        mut msg: M,
        mq: MessageQueue,
        request_callback: RequestCallbackFn,
        timeout: u64,
    ) -> crate::ClientResult<crate::producer::request_future_holder::RequestHandle>
    where
        M: MessageTrait + Send + Sync,
    {
        let deadline = RequestDeadline::from_timeout_millis(timeout);
        let registration = self
            .register_request(&mut msg, timeout, deadline, Some(request_callback))
            .await?;
        let callback = Self::request_send_callback(registration.handle());
        let runtime = self.runtime_snapshot();
        let result = deadline
            .timeout(self.send_kernel_impl_with_runtime(
                &mut msg,
                &mq,
                CommunicationMode::Async,
                Some(callback),
                None,
                deadline,
                &runtime,
            ))
            .await
            .map_err(|_| ClientError::timeout("request_send", timeout))
            .and_then(|result| result.map_err(|input| producer_retry_input_error(input, None)));
        if let Err(error) = result {
            registration.handle().fail(error.clone());
            return Err(error);
        }
        Ok(registration.detach())
    }
    pub async fn request_to_queue_with_callback<M>(
        &self,
        msg: M,
        mq: MessageQueue,
        request_callback: RequestCallbackFn,
        timeout: u64,
    ) -> crate::ClientResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        self.request_to_queue_with_callback_handle(msg, mq, request_callback, timeout)
            .await
            .map(|_| ())
    }
    pub async fn request<M>(&self, mut msg: M, timeout: u64) -> crate::ClientResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
    {
        let deadline = RequestDeadline::from_timeout_millis(timeout);
        let registration = self.register_request(&mut msg, timeout, deadline, None).await?;
        let callback = Self::request_send_callback(registration.handle());
        let result = deadline
            .timeout(self.send_default_impl(&mut msg, CommunicationMode::Async, Some(callback), deadline))
            .await
            .map_err(|_| ClientError::timeout("request_send", timeout))
            .and_then(|result| result);
        if let Err(error) = result {
            registration.handle().fail(error.clone());
            return Err(error);
        }
        self.finish_request(registration, deadline).await
    }
    pub async fn request_with_callback_handle<M>(
        &self,
        mut msg: M,
        request_callback: RequestCallbackFn,
        timeout: u64,
    ) -> crate::ClientResult<crate::producer::request_future_holder::RequestHandle>
    where
        M: MessageTrait + Send + Sync,
    {
        let deadline = RequestDeadline::from_timeout_millis(timeout);
        let registration = self
            .register_request(&mut msg, timeout, deadline, Some(request_callback))
            .await?;
        let callback = Self::request_send_callback(registration.handle());
        let result = deadline
            .timeout(self.send_default_impl(&mut msg, CommunicationMode::Async, Some(callback), deadline))
            .await
            .map_err(|_| ClientError::timeout("request_send", timeout))
            .and_then(|result| result);
        if let Err(error) = result {
            registration.handle().fail(error.clone());
            return Err(error);
        }
        Ok(registration.detach())
    }
    pub async fn request_with_callback<M>(
        &self,
        msg: M,
        request_callback: RequestCallbackFn,
        timeout: u64,
    ) -> crate::ClientResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        self.request_with_callback_handle(msg, request_callback, timeout)
            .await
            .map(|_| ())
    }
    pub(super) async fn wait_response(
        &self,
        _topic: &CheetahString,
        timeout: u64,
        request: Arc<RequestResponseFuture>,
        remaining: u64,
    ) -> crate::ClientResult<Box<dyn MessageTrait + Send>> {
        if let Some(message) = request.wait_response_message(Duration::from_millis(remaining)).await {
            return Ok(message);
        }
        if let Some(cause) = request.get_cause() {
            return Err((*cause).clone());
        }
        Err(ClientError::timeout("request_reply", timeout))
    }

    pub(super) async fn prepare_send_request<M>(&self, msg: &mut M, timeout: u64) -> crate::ClientResult<()>
    where
        M: MessageTrait,
    {
        let correlation_id = CorrelationIdUtil::create_correlation_id();
        let client_instance = self.client_instance()?;
        let request_client_id = client_instance.client_id.clone();
        MessageAccessor::put_property(
            msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID),
            CheetahString::from_string(correlation_id),
        );
        MessageAccessor::put_property(
            msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT),
            request_client_id,
        );
        MessageAccessor::put_property(
            msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_TTL),
            CheetahString::from_string(timeout.to_string()),
        );
        let has_route_data = client_instance.topic_route_table.contains_key(msg.topic().as_str());
        if !has_route_data {
            let begin_timestamp = Instant::now();
            self.try_to_find_topic_publish_info(msg.topic()).await;
            client_instance.send_heartbeat_to_all_broker_with_lock().await;
            let cost = begin_timestamp.elapsed().as_millis() as u64;
            if cost > 500 {
                warn!(
                    topic = %msg.topic(),
                    elapsed_ms = cost,
                    "prepare send request slow"
                );
            }
        }
        Ok(())
    }
}

#[allow(unused_must_use)]
#[allow(unused_assignments)]
impl DefaultMQProducerImpl {
    pub async fn recall_message(
        &self,
        topic: impl Into<CheetahString>,
        recall_handle: impl Into<CheetahString>,
    ) -> crate::ClientResult<String> {
        let topic = topic.into();
        let recall_handle = recall_handle.into();

        self.make_sure_state_ok()?;
        Validators::check_topic(&topic)?;

        if recall_handle.is_empty() {
            return Err(mq_client_err!("Recall handle cannot be empty"));
        }

        if NamespaceUtil::is_retry_topic(&topic) || NamespaceUtil::is_dlq_topic(&topic) {
            return Err(mq_client_err!("topic is not supported"));
        }

        let handle_entity = RecallMessageHandle::decode_handle(&recall_handle).map_err(ClientError::from_error)?;

        self.try_to_find_topic_publish_info(&topic).await;

        let broker_name_cs = CheetahString::from_slice(handle_entity.broker_name());
        let runtime = self.runtime_snapshot();
        let client_instance = self.client_instance()?;
        let mut broker_addr = client_instance.find_broker_address_in_publish(&broker_name_cs);

        if broker_addr.is_none() {
            broker_addr = client_instance.find_broker_addr_by_topic(&topic).await;
        }

        let broker_addr = broker_addr.ok_or_else(|| {
            warn!(
                "Can't find broker service address for broker: {}",
                handle_entity.broker_name()
            );
            mq_client_err!("The broker service address not found")
        })?;

        let mut request_header = RecallMessageRequestHeader::new(
            topic,
            recall_handle,
            Some(runtime.producer_config.producer_group().clone()),
        );

        request_header.topic_request_header = Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(broker_name_cs.clone()),
                namespace: None,
                namespaced: None,
                oneway: None,
            }),
            lo: None,
        });

        client_instance
            .get_mq_client_api_impl()?
            .recall_message(
                &broker_addr,
                request_header,
                runtime.producer_config.send_msg_timeout() as u64,
            )
            .await
    }
}

pub(crate) struct DefaultServiceDetector {
    pub(super) client_instance: Weak<MQClientInstance>,
    pub(super) topic_publish_info_table: Arc<DashMap<CheetahString /* topic */, TopicPublishInfoSnapshot>>,
}

impl ServiceDetector for DefaultServiceDetector {
    async fn detect(&self, endpoint: &str, timeout_millis: u64) -> bool {
        let topic = match self
            .topic_publish_info_table
            .iter()
            .next()
            .map(|entry| entry.key().clone())
        {
            Some(t) => t,
            None => return false,
        };

        let mq = MessageQueue::from_parts(topic.as_str(), endpoint, 0);
        let Some(client_instance) = self.client_instance.upgrade() else {
            return false;
        };

        let result = tokio::time::timeout(Duration::from_millis(timeout_millis), async move {
            match client_instance.mq_client_api_impl.load_full() {
                Some(api) => api.get_max_offset(endpoint, &mq, timeout_millis).await.is_ok(),
                None => false,
            }
        })
        .await;

        matches!(result, Ok(true))
    }
}

/// Helper function to build oneway request (simplified version for performance).
///
/// This is used internally by batch oneway to avoid code duplication.
pub(super) fn build_oneway_request_internal<T>(
    msg: &mut T,
    mq: &MessageQueue,
    broker_name: &CheetahString,
    send_config: &ProducerSendConfigSnapshot,
    command_factory: rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory,
    _namespace: Option<&str>,
) -> crate::ClientResult<RemotingCommand>
where
    T: MessageTrait,
{
    use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;

    // Set message ID
    MessageClientIDSetter::set_uniq_id(msg);

    // Build request header (simplified for oneway)
    let request_header = SendMessageRequestHeader {
        producer_group: send_config.producer_group.clone(),
        topic: msg.topic().clone(),
        default_topic: send_config.create_topic_key.clone(),
        default_topic_queue_nums: send_config.default_topic_queue_nums,
        queue_id: mq.queue_id(),
        sys_flag: 0,
        born_timestamp: current_millis() as i64,
        flag: msg.get_flag(),
        properties: Some(MessageDecoder::message_properties_to_string(msg.get_properties())),
        reconsume_times: Some(0),
        unit_mode: Some(send_config.unit_mode),
        batch: Some(false),
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(broker_name.clone()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    // Build command
    let reply = msg
        .property(&CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_TYPE))
        .is_some_and(|kind| kind == mix_all::REPLY_MESSAGE_FLAG);
    let mut request =
        crate::implementation::mq_client_api_impl::send_message_command(command_factory, request_header, reply, false);

    // Set body (zero-copy: Bytes is reference-counted)
    if let Some(body) = msg.get_body() {
        request.set_body_mut_ref(body.clone());
    } else {
        return Err(mq_client_err!(-1, "Message body is None"));
    }

    Ok(request)
}

pub(crate) struct DefaultResolver {
    pub(super) client_instance: Weak<MQClientInstance>,
}

impl Resolver for DefaultResolver {
    async fn resolve(&self, name: &CheetahString) -> Option<CheetahString> {
        self.client_instance
            .upgrade()
            .and_then(|client_instance| client_instance.find_broker_address_in_publish(name))
    }
}

// Custom fork: complete MQAdmin entry checks this consumer/producer lifecycle.
impl DefaultMQProducerImpl {
    pub(crate) fn admin_context(&self) -> crate::ClientResult<crate::base::mq_admin::MQAdminContext> {
        self.make_sure_state_ok()?;
        Ok(crate::base::mq_admin::MQAdminContext {
            admin: self.client_instance()?.mq_admin_impl.clone(),
            config: Arc::new(self.runtime_snapshot().client_config.clone()),
        })
    }
}
