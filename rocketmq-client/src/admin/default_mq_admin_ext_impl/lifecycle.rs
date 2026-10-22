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

use std::env;

use rocketmq_model::common::FAQUrl;
use tracing::info;

use super::*;

impl DefaultMQAdminExtImpl {
    pub fn new(
        client_runtime: Arc<ClientRuntime>,
        rpc_hook: Option<Arc<dyn RPCHook>>,
        timeout_millis: Duration,
        client_config: ClientConfig,
        admin_ext_group: CheetahString,
    ) -> Self {
        DefaultMQAdminExtImpl {
            client_pool: client_runtime.pool().clone(),
            admin_lease: None,
            service_state: ServiceState::CreateJust,
            client_instance: None,
            rpc_hook,
            timeout_millis,
            kv_namespace_to_delete_list: vec![CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG)],
            client_config,
            admin_ext_group,
            create_topic_key: CheetahString::from_static_str(
                rocketmq_model::common::topic::TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC,
            ),
        }
    }

    /// Returns whether the facade has a usable concrete implementation.
    ///
    /// The implementation is now owned directly, so this compatibility query is always true.
    pub fn has_inner(&self) -> bool {
        true
    }

    #[inline]
    pub fn client_config(&self) -> &ClientConfig {
        &self.client_config
    }

    #[inline]
    pub fn client_config_mut(&mut self) -> &mut ClientConfig {
        &mut self.client_config
    }

    #[inline]
    pub fn is_use_tls(&self) -> bool {
        self.client_config.is_use_tls()
    }

    #[inline]
    pub fn set_use_tls(&mut self, use_tls: bool) {
        self.client_config.set_use_tls(use_tls);
    }

    pub(in crate::admin) fn mq_client_api(&self) -> crate::ClientResult<Arc<MQClientAPIImpl>> {
        if self.service_state != ServiceState::Running {
            return Err(crate::ClientError::not_started());
        }
        let lease = self.admin_lease.as_ref().ok_or(crate::ClientError::not_started())?;
        self.client_pool.configuration_matches(
            lease.token.as_ref().ok_or(crate::ClientError::not_started())?,
            &self.client_config,
            self.rpc_hook.as_ref(),
        )?;
        self.client_instance
            .as_ref()
            .ok_or(crate::ClientError::not_started())?
            .get_mq_client_api_impl()
    }

    /// None uses the current NameServer list; Some(empty) is the Java explicit empty-set no-op.
    #[cfg(feature = "admin-full")]
    pub async fn delete_topic_in_name_servers(
        &self,
        addresses: Option<std::collections::HashSet<CheetahString>>,
        topic: CheetahString,
    ) -> crate::ClientResult<()> {
        let addresses = addresses.map(|addresses| {
            let mut values = addresses.into_iter().collect::<Vec<_>>();
            values.sort();
            values
        });
        self.mq_client_api()?
            .delete_resource_topic(addresses, topic, false, self.remoting_timeout_millis()?)
            .await
    }

    pub fn admin_ext_group(&self) -> &CheetahString {
        &self.admin_ext_group
    }
    pub fn set_admin_ext_group(&mut self, group: impl Into<CheetahString>) -> crate::ClientResult<()> {
        if self.service_state != ServiceState::CreateJust {
            return Err(crate::ClientError::already_started());
        }
        self.admin_ext_group = group.into();
        Ok(())
    }
    pub fn timeout(&self) -> Duration {
        self.timeout_millis
    }
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout_millis = timeout;
    }
    pub fn create_topic_key(&self) -> &CheetahString {
        &self.create_topic_key
    }
    pub fn set_create_topic_key(&mut self, key: impl Into<CheetahString>) {
        self.create_topic_key = key.into();
    }
    pub fn kv_namespace_to_delete_list(&self) -> &[CheetahString] {
        &self.kv_namespace_to_delete_list
    }
    pub fn set_kv_namespace_to_delete_list(&mut self, namespaces: Vec<CheetahString>) {
        self.kv_namespace_to_delete_list = namespaces;
    }

    pub(in crate::admin) fn remoting_timeout_millis(&self) -> crate::ClientResult<u64> {
        u64::try_from(self.timeout_millis.as_millis()).map_err(|_| {
            crate::ClientError::illegal_argument("admin timeout exceeds the supported u64 millisecond range")
        })
    }

    pub(in crate::admin) async fn start_admin(&mut self) -> crate::ClientResult<()> {
        if self.service_state != ServiceState::CreateJust {
            return Err(crate::ClientError::already_started());
        }
        self.service_state = ServiceState::StartFailed;
        self.client_config.change_instance_name_to_pid();
        if "{}".eq(&self.client_config.socks_proxy_config) {
            self.client_config.socks_proxy_config =
                env::var(SOCKS_PROXY_JSON).unwrap_or_else(|_| "{}".to_string()).into();
        }
        let (instance, token) = self
            .client_pool
            .get_or_create(self.client_config.clone(), self.rpc_hook.clone())?
            .into_parts();
        let owner = NEXT_ADMIN_OWNER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut lease = AdminLease {
            pool: self.client_pool.clone(),
            instance: Arc::clone(&instance),
            token: Some(token),
            group: self.admin_ext_group.clone(),
            owner,
            registered: false,
        };
        if !instance.register_admin_ext_owned(&self.admin_ext_group, owner) {
            lease.close().await;
            return Err(crate::ClientError::illegal_argument(format!(
                "The adminExt group has created already.{}",
                FAQUrl::suggest_todo(FAQUrl::GROUP_NAME_DUPLICATE_URL)
            )));
        }
        lease.registered = true;
        if let Err(error) = instance.start().await {
            lease.close().await;
            return Err(error);
        }
        self.client_instance = Some(instance);
        self.admin_lease = Some(lease);
        self.service_state = ServiceState::Running;
        info!("the adminExt [{}] start OK", self.admin_ext_group);
        Ok(())
    }

    pub(in crate::admin) async fn shutdown_admin(&mut self) {
        self.service_state = ServiceState::ShutdownAlready;
        self.client_instance = None;
        if let Some(lease) = self.admin_lease.take() {
            lease.close().await;
        }
    }
}

// Custom: the guard stays on the startup future's stack until startup succeeds.
static NEXT_ADMIN_OWNER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

pub(super) struct AdminLease {
    pool: ClientPool,
    instance: Arc<MQClientInstance>,
    token: Option<ClientPoolToken>,
    group: CheetahString,
    owner: u64,
    registered: bool,
}
impl AdminLease {
    fn unregister(&mut self) {
        if self.registered {
            self.instance
                .unregister_admin_ext_owned(self.group.as_str(), self.owner);
            self.registered = false;
        }
    }
    async fn close(mut self) {
        self.unregister();
        // Custom: retain the token until the await completes. If shutdown is canceled, Drop
        // resumes the already-retiring instance through the Root-owned cleanup task.
        if let Some(token) = self.token.as_ref() {
            self.pool.release(token.clone()).await;
        }
        self.token = None;
    }
}
impl Drop for AdminLease {
    fn drop(&mut self) {
        self.unregister();
        if let Some(token) = self.token.take() {
            self.pool.release_deferred(token);
        }
    }
}
