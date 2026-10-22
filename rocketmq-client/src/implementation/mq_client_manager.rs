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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use parking_lot::RwLock;
use rocketmq_observability::metrics::client::ClientMetrics;
use rocketmq_observability::TelemetryHandle;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_transport::api::RPCHook;
use tracing::info;

use crate::base::client_config::ClientConfig;
use crate::base::client_options::ClientOptions;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::nameserver_discovery::NameServerDiscoveryConfig;
use crate::producer::produce_accumulator::ProduceAccumulator;
use crate::producer::request_future_holder::RequestFutureHolder;

struct ClientPoolEntry {
    generation: u64,
    leases: usize,
    identity: ClientPoolIdentity,
    instance: Arc<MQClientInstance>,
}

#[derive(Clone, Eq, PartialEq)]
struct ClientPoolIdentity {
    // Custom fork: credential identity is part of the shared client lease contract.
    authentication: rocketmq_transport::api::ConnectionAuthentication,
    tls_config: rocketmq_transport::api::TlsConfig,
    connect: rocketmq_transport::api::ConnectConfig,
    maintenance: rocketmq_transport::api::MaintenanceConfig,
    socket_options: rocketmq_transport::api::SocketOptions,
    close_socket_if_timeout: bool,
    socks_proxy_config: CheetahString,
    namesrv_addr: Option<CheetahString>,
    discovery_fingerprint: Option<CheetahString>,
    use_tls: bool,
    vip_channel_enabled: bool,
    api_timeout_millis: u64,
    client_async_semaphore_value: u32,
    client_oneway_semaphore_value: u32,
    rpc_hook_identity: Option<usize>,
    remoting_command_defaults: RemotingCommandDefaults,
}

impl ClientPoolIdentity {
    fn new(
        client_config: &ClientConfig,
        discovery: Option<&NameServerDiscoveryConfig>,
        rpc_hook: Option<&Arc<dyn RPCHook>>,
        remoting_command_factory: RemotingCommandFactory,
    ) -> Self {
        Self {
            authentication: client_config.authentication.clone(),
            tls_config: client_config.tls_config.clone(),
            connect: client_config.remoting_connect,
            maintenance: client_config.remoting_maintenance,
            socket_options: client_config.socket_options,
            close_socket_if_timeout: client_config.close_socket_if_timeout,
            socks_proxy_config: client_config.socks_proxy_config.clone(),
            namesrv_addr: client_config.namesrv_addr.clone(),
            discovery_fingerprint: discovery.map(|config| CheetahString::from_string(config.fingerprint())),
            use_tls: client_config.use_tls,
            vip_channel_enabled: client_config.vip_channel_enabled,
            api_timeout_millis: client_config.mq_client_api_timeout,
            client_async_semaphore_value: client_config.client_async_semaphore_value,
            client_oneway_semaphore_value: client_config.client_oneway_semaphore_value,
            rpc_hook_identity: rpc_hook.map(|hook| Arc::as_ptr(hook).cast::<()>() as usize),
            remoting_command_defaults: remoting_command_factory.defaults(),
        }
    }
}

type ClientInstanceHashMap = DashMap<CheetahString /* clientId */, ClientPoolEntry>;
type AccumulatorHashMap = DashMap<CheetahString /* clientId */, Arc<ProduceAccumulator>>;

#[derive(Clone)]
pub struct ClientPool {
    inner: Arc<ClientPoolInner>,
}

struct ClientPoolInner {
    service_context: ChildServiceContext,
    resource_budget: ResourceBudget,
    telemetry_handle: TelemetryHandle,
    client_metrics: ClientMetrics,
    factory_table: Arc<ClientInstanceHashMap>,
    // Custom: cancellation of asynchronous release must not orphan a retiring instance.
    retiring: DashMap<u64, Arc<MQClientInstance>>,
    accumulator_table: Arc<AccumulatorHashMap>,
    request_future_holder: Arc<RequestFutureHolder>,
    remoting_command_factory: RemotingCommandFactory,
    next_generation: AtomicU64,
    closed: AtomicBool,
    admission: RwLock<()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientPoolToken {
    client_id: CheetahString,
    generation: u64,
}

pub struct PooledClient {
    instance: Arc<MQClientInstance>,
    token: ClientPoolToken,
}

impl PooledClient {
    pub fn instance(&self) -> &Arc<MQClientInstance> {
        &self.instance
    }

    pub fn into_parts(self) -> (Arc<MQClientInstance>, ClientPoolToken) {
        (self.instance, self.token)
    }
}

impl ClientPool {
    pub(crate) fn new(
        service_context: ChildServiceContext,
        resource_budget: ResourceBudget,
        telemetry_handle: TelemetryHandle,
        client_metrics: ClientMetrics,
        remoting_command_factory: RemotingCommandFactory,
    ) -> Self {
        let request_future_holder = Arc::new(RequestFutureHolder::new(service_context.component("request-futures")));
        Self {
            inner: Arc::new(ClientPoolInner {
                service_context,
                resource_budget,
                telemetry_handle,
                client_metrics,
                factory_table: Arc::new(DashMap::with_capacity(128)),
                retiring: DashMap::new(),
                accumulator_table: Arc::new(DashMap::with_capacity(128)),
                request_future_holder,
                remoting_command_factory,
                next_generation: AtomicU64::new(0),
                closed: AtomicBool::new(false),
                admission: RwLock::new(()),
            }),
        }
    }

    pub fn instance_count(&self) -> usize {
        self.inner.factory_table.len()
    }

    pub(crate) fn request_future_holder(&self) -> Arc<RequestFutureHolder> {
        Arc::clone(&self.inner.request_future_holder)
    }

    pub fn get_or_create(
        &self,
        client_config: ClientConfig,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> crate::ClientResult<PooledClient> {
        self.get_or_create_with_options(ClientOptions::legacy(client_config), rpc_hook)
    }

    pub fn get_or_create_with_options(
        &self,
        options: ClientOptions,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> crate::ClientResult<PooledClient> {
        let _admission = self.inner.admission.read();
        if self.inner.closed.load(Ordering::Acquire) {
            // Custom: preserve the shutdown cause when rejecting new ownership.
            return Err(crate::ClientError::shutting_down());
        }
        let (client_config, discovery, command_factory_override) = options.into_normalized_parts()?;
        client_config.authentication.validate()?;
        client_config.validate_remoting_configuration()?;
        let remoting_command_factory = command_factory_override.unwrap_or(self.inner.remoting_command_factory);
        let client_id = CheetahString::from_string(client_config.build_mq_client_id());
        let identity = ClientPoolIdentity::new(
            &client_config,
            discovery.as_ref(),
            rpc_hook.as_ref(),
            remoting_command_factory,
        );

        let mut entry = self.inner.factory_table.entry(client_id.clone()).or_insert_with(|| {
            let generation = self.inner.next_generation.fetch_add(1, Ordering::AcqRel) + 1;
            info!(
                client_id = %client_id,
                generation,
                "Created new MQClientInstance in ClientPool"
            );
            let instance = MQClientInstance::new_arc_with_resource_budget_and_discovery(
                client_config,
                discovery,
                remoting_command_factory,
                generation,
                client_id.clone(),
                rpc_hook,
                self.inner.service_context.component(
                    rocketmq_runtime::ScopeId::try_new(format!("instance-{generation}"))
                        .expect("the client instance scope has a fixed nonblank prefix"),
                ),
                Arc::clone(&self.inner.request_future_holder),
                self.inner.resource_budget.clone(),
                self.inner.telemetry_handle.clone(),
                self.inner.client_metrics.clone(),
            );
            ClientPoolEntry {
                generation,
                leases: 0,
                identity: identity.clone(),
                instance,
            }
        });
        if entry.identity != identity {
            return Err(mq_client_err!(
                "ClientPool configuration conflicts with an existing client-id owner"
            ));
        }
        entry.leases += 1;

        Ok(PooledClient {
            instance: Arc::clone(&entry.instance),
            token: ClientPoolToken {
                client_id,
                generation: entry.generation,
            },
        })
    }

    pub fn get_or_create_produce_accumulator(&self, client_config: ClientConfig) -> Option<Arc<ProduceAccumulator>> {
        let _admission = self.inner.admission.read();
        if self.inner.closed.load(Ordering::Acquire) {
            return None;
        }
        let client_id = CheetahString::from_string(client_config.build_mq_client_id());

        Some(
            self.inner
                .accumulator_table
                .entry(client_id.clone())
                .or_insert_with(|| {
                    info!("Created new ProduceAccumulator for clientId:[{}]", client_id);
                    Arc::new(ProduceAccumulator::with_resource_budget(
                        self.inner.service_context.component(
                            rocketmq_runtime::ScopeId::try_new(format!("accumulator-{}", client_id))
                                .expect("the accumulator scope has a fixed nonblank prefix"),
                        ),
                        client_id.as_str(),
                        self.inner.resource_budget.clone(),
                    ))
                })
                .clone(),
        )
    }

    pub(crate) fn configuration_matches(
        &self,
        token: &ClientPoolToken,
        config: &ClientConfig,
        hook: Option<&Arc<dyn RPCHook>>,
    ) -> crate::ClientResult<()> {
        let (config, discovery, override_factory) = ClientOptions::legacy(config.clone()).into_normalized_parts()?;
        let identity = ClientPoolIdentity::new(
            &config,
            discovery.as_ref(),
            hook,
            override_factory.unwrap_or(self.inner.remoting_command_factory),
        );
        let current = self
            .inner
            .factory_table
            .get(&token.client_id)
            .ok_or(crate::ClientError::not_started())?;
        if current.generation != token.generation {
            return Err(crate::ClientError::not_started());
        }
        if token.client_id.as_str() != config.build_mq_client_id() || current.identity != identity {
            return Err(crate::ClientError::config_invalid(
                "admin.client_config",
                "changed after start",
                "create a new facade for another identity or transport configuration",
            ));
        }
        Ok(())
    }

    fn release_instance(&self, token: ClientPoolToken) -> Option<(u64, Arc<MQClientInstance>)> {
        // Serialize retirement with Root's complete active/retiring snapshot.
        let _admission = self.inner.admission.read();
        let should_remove = self
            .inner
            .factory_table
            .get_mut(&token.client_id)
            .is_some_and(|mut current| {
                if current.generation != token.generation || current.leases == 0 {
                    return false;
                }
                current.leases -= 1;
                current.leases == 0
            });
        if !should_remove {
            return None;
        }
        let instance = self
            .inner
            .factory_table
            .remove_if(&token.client_id, |_, current| {
                current.generation == token.generation && current.leases == 0
            })
            .map(|(_, entry)| entry.instance)?;
        self.inner.retiring.insert(token.generation, Arc::clone(&instance));
        Some((token.generation, instance))
    }

    pub async fn release(&self, token: ClientPoolToken) -> bool {
        let Some((generation, instance)) = self.release_instance(token) else {
            return false;
        };
        instance.shutdown().await;
        self.inner.retiring.remove(&generation);
        true
    }

    /// Custom: Drop releases the cache lease synchronously; the owned Root performs async cleanup.
    pub(crate) fn release_deferred(&self, token: ClientPoolToken) {
        let retired = self.release_instance(token.clone()).or_else(|| {
            self.inner
                .retiring
                .get(&token.generation)
                .map(|instance| (token.generation, Arc::clone(instance.value())))
        });
        let Some((generation, instance)) = retired else {
            return;
        };
        let pool = self.clone();
        let result = crate::runtime::spawn_background_client_task_with_context(
            &self.inner.service_context,
            "admin.release-client",
            Box::pin(async move {
                instance.shutdown().await;
                pool.inner.retiring.remove(&generation);
            }),
        );
        if result.is_err() {
            // Root retains the retiring instance even if task admission has already closed.
            tracing::warn!("deferred Admin cleanup retained for ClientRuntime shutdown");
        }
    }

    #[cfg(test)]
    pub(crate) fn inner_instance_for_test(&self, config: &ClientConfig) -> Option<Arc<MQClientInstance>> {
        self.inner
            .factory_table
            .get(config.build_mq_client_id().as_str())
            .map(|entry| Arc::clone(&entry.instance))
    }

    #[cfg(test)]
    pub(crate) fn retiring_count(&self) -> usize {
        self.inner.retiring.len()
    }

    pub(crate) async fn shutdown_until(&self, deadline: ShutdownDeadline) {
        let instances = {
            let _admission = self.inner.admission.write();
            self.inner.closed.store(true, Ordering::Release);
            let keys = self
                .inner
                .factory_table
                .iter()
                .map(|entry| entry.key().clone())
                .collect::<Vec<_>>();
            let mut instances = Vec::with_capacity(keys.len());
            for key in keys {
                if let Some((_, entry)) = self.inner.factory_table.remove(&key) {
                    instances.push(entry.instance);
                }
            }
            let retiring = self
                .inner
                .retiring
                .iter()
                .map(|entry| (*entry.key(), Arc::clone(entry.value())))
                .collect::<Vec<_>>();
            for (generation, instance) in retiring {
                self.inner.retiring.remove(&generation);
                instances.push(instance);
            }
            self.inner.accumulator_table.clear();
            instances
        };

        for instance in instances {
            if deadline.is_expired() {
                break;
            }
            let _ = tokio::time::timeout(deadline.remaining(), instance.shutdown()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_support::error_assertions::client_exception;
    use cheetah_string::CheetahString;
    use rocketmq_protocol::protocol::SerializeType;

    use super::*;

    #[tokio::test]
    async fn stopped_runtime_rejects_new_admin_with_lifecycle_error_before_allocation() {
        let runtime = crate::runtime::test_client_runtime("stopped-admin-pool");
        assert!(runtime.shutdown().await.is_healthy());
        let mut admin = crate::DefaultMQAdminExt::new(runtime.clone());
        let error = admin.start().await.unwrap_err();
        assert_eq!(error.failure_kind(), crate::ClientFailureKind::Lifecycle);
        assert_eq!(runtime.pool().instance_count(), 0);
    }

    #[tokio::test]
    async fn authenticated_owners_isolate_groups_credentials_and_release() {
        let runtime = crate::runtime::test_client_runtime("authenticated-pool-identity");
        let mut first_config = ClientConfig::default();
        first_config.set_instance_name("same-owner-name".into());
        first_config.authentication =
            rocketmq_transport::api::ConnectionAuthentication::group_token("PT_group_a", "secret-a").unwrap();
        let mut second_config = first_config.clone();
        second_config.set_group_name(Some("CT_group_b".into()));
        let mut third_config = first_config.clone();
        third_config.set_token(Some("secret-b"));
        assert!(!format!("{:?}", first_config.authentication).contains("secret-a"));
        assert!(!first_config.build_mq_client_id().contains("secret-a"));
        let first = runtime.pool().get_or_create(first_config.clone(), None).unwrap();
        let clone = runtime.pool().get_or_create(first_config, None).unwrap();
        let second = runtime.pool().get_or_create(second_config, None).unwrap();
        let third = runtime.pool().get_or_create(third_config, None).unwrap();
        assert!(Arc::ptr_eq(first.instance(), clone.instance()));
        assert!(!Arc::ptr_eq(first.instance(), second.instance()));
        assert!(!Arc::ptr_eq(first.instance(), third.instance()));
        assert_eq!(runtime.pool().instance_count(), 3);
        let (_, first_token) = first.into_parts();
        let (_, clone_token) = clone.into_parts();
        assert!(!runtime.pool().release(first_token).await);
        assert!(runtime.pool().release(clone_token).await);
        assert_eq!(runtime.pool().instance_count(), 2);
        let (_, token) = second.into_parts();
        assert!(runtime.pool().release(token).await);
        let (_, token) = third.into_parts();
        assert!(runtime.pool().release(token).await);
        assert_eq!(runtime.pool().instance_count(), 0);
    }

    #[test]
    fn tcp_and_tls_configuration_cannot_silently_reuse_conflicting_instances() {
        for tls in [false, true] {
            let runtime = crate::runtime::test_client_runtime("transport-configuration-pool-identity");
            let config = ClientConfig::default();
            let first = runtime.pool().get_or_create(config.clone(), None).unwrap();
            let mut different = config;
            if tls {
                different.tls_config.client.trust_cert_path = Some("explicit-other-ca.pem".into());
            } else {
                different.socket_options.tcp_nodelay = false;
            }
            assert!(runtime.pool().get_or_create(different, None).is_err());
            assert_eq!(runtime.pool().instance_count(), 1);
            drop(first);
        }
    }

    #[test]
    fn invalid_nameserver_address_is_rejected_before_pool_admission() {
        let runtime = crate::runtime::test_client_runtime("invalid-nameserver-pool-test");
        let config = ClientConfig {
            namesrv_addr: Some(CheetahString::from_static_str("missing-port")),
            ..Default::default()
        };

        let error = match runtime.pool().get_or_create(config, None) {
            Ok(_) => panic!("invalid NameServer address must be rejected"),
            Err(error) => error,
        };

        assert!(error.is(&rocketmq_error::CORE_CONFIGURATION_INVALID));
        assert_eq!(runtime.pool().instance_count(), 0);
    }

    #[test]
    fn canonical_nameserver_address_drives_pool_identity() {
        let runtime = crate::runtime::test_client_runtime("canonical-nameserver-pool-test");
        let first_config = ClientConfig {
            namesrv_addr: Some(CheetahString::from_static_str(" NS-A:9876 ; ns-a:9876 ")),
            ..Default::default()
        };
        let first = runtime
            .pool()
            .get_or_create(first_config, None)
            .expect("first pooled client");

        let equivalent_config = ClientConfig {
            namesrv_addr: Some(CheetahString::from_static_str("ns-a:9876")),
            ..Default::default()
        };
        let equivalent = runtime
            .pool()
            .get_or_create(equivalent_config, None)
            .expect("equivalent canonical target must share the pool entry");
        assert!(Arc::ptr_eq(first.instance(), equivalent.instance()));

        let different_config = ClientConfig {
            namesrv_addr: Some(CheetahString::from_static_str("ns-b:9876")),
            ..Default::default()
        };
        assert!(runtime.pool().get_or_create(different_config, None).is_err());
    }

    #[test]
    fn remoting_defaults_participate_in_pool_identity() {
        let runtime = crate::runtime::test_client_runtime("remoting-defaults-pool-identity-test");
        let mut config = ClientConfig::default();
        config.set_instance_name("remoting-defaults-owner".into());
        let json = RemotingCommandFactory::new(RemotingCommandDefaults::new(501, SerializeType::JSON));
        let binary = RemotingCommandFactory::new(RemotingCommandDefaults::new(502, SerializeType::ROCKETMQ));

        runtime
            .pool()
            .get_or_create_with_options(
                ClientOptions::legacy(config.clone()).with_remoting_command_factory(json),
                None,
            )
            .expect("first command owner");
        let error = match runtime.pool().get_or_create_with_options(
            ClientOptions::legacy(config).with_remoting_command_factory(binary),
            None,
        ) {
            Ok(_) => panic!("different remoting defaults must not share one client-id owner"),
            Err(error) => error,
        };

        assert!(client_exception(&error)
            .to_string()
            .contains("ClientPool configuration conflicts with an existing client-id owner"));
    }
}
