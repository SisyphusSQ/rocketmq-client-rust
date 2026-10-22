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

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crate::NameServerResult;
use cheetah_string::CheetahString;
use rocketmq_error::SharedError;
use rocketmq_protocol::protocol::body::kv_table::KVTable;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::MetadataDeadline;
use rocketmq_runtime::MetadataIoActor;
use rocketmq_runtime::MetadataIoShutdownReport;
#[cfg(test)]
use rocketmq_runtime::MetadataIoSnapshot;
use rocketmq_runtime::RuntimeError;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::bootstrap::NameServerRuntimeHandle;
use crate::kvconfig::persistence::KvMutation;
use crate::kvconfig::persistence::KvMutationService;
use crate::kvconfig::persistence::KvMutationSnapshot;
use crate::kvconfig::KVConfigSerializeWrapper;
use crate::NamesrvConfig;

/// Type alias for namespace in the KV configuration
pub(super) type Namespace = CheetahString;
/// Type alias for key in the KV configuration
pub(super) type Key = CheetahString;
/// Type alias for value in the KV configuration
pub(super) type Value = CheetahString;
/// Type alias for the configuration map structure
pub(super) type ConfigMap = HashMap<Key, Value>;
pub(super) type ConfigTable = dashmap::DashMap<Namespace, ConfigMap>;

fn load_config_table_with(
    config_path: &Path,
    read: impl FnOnce(&Path) -> io::Result<String>,
) -> NameServerResult<Option<ConfigTable>> {
    let content = match read(config_path) {
        Ok(content) if content.is_empty() => return Ok(None),
        Ok(content) => content,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(crate::namesrv_error::storage_read(error));
        }
    };

    KVConfigSerializeWrapper::decode(content.as_bytes())
        .map(|wrapper| wrapper.config_table)
        .map_err(|error| crate::namesrv_error::serialization("decode-kv-config", "json", error))
}

/// KV Configuration Manager
///
/// Manages key-value configurations with namespace support.
pub struct KVConfigManager {
    /// Configuration storage: namespace -> (key -> value)
    pub(crate) config_table: Arc<dashmap::DashMap<Namespace, ConfigMap>>,
    /// Runtime inner for accessing configuration
    pub(crate) name_server_runtime_inner: NameServerRuntimeHandle,
    /// Bounded, lifecycle-owned durable-before-publish mutation service.
    mutation_service: Result<KvMutationService, SharedError>,
    /// Production persistence owner. Absence is retained only for legacy
    /// builders that do not inject a service lifecycle.
    metadata_io: Option<Result<MetadataIoActor, RuntimeError>>,
}

impl KVConfigManager {
    /// Creates a new `KVConfigManager` instance.
    ///
    /// # Arguments
    ///
    /// * `namesrv_config` - The configuration for the Namesrv.
    ///
    /// # Returns
    ///
    /// A new `KVConfigManager` instance.
    pub(crate) fn new(
        name_server_runtime_inner: NameServerRuntimeHandle,
        metadata_io: Option<Result<MetadataIoActor, RuntimeError>>,
        service_context: &ChildServiceContext,
        target: PathBuf,
        queue_capacity: usize,
        batch_size: usize,
        max_pending_bytes: usize,
        metrics: rocketmq_observability::metrics::namesrv::NameServerMetrics,
    ) -> Self {
        let config_table = Arc::new(dashmap::DashMap::with_capacity(64));
        let mutation_service = match metadata_io.as_ref() {
            None => Err(crate::namesrv_error::storage_write(io::Error::new(
                io::ErrorKind::NotConnected,
                "metadata I/O actor is unavailable",
            ))),
            Some(Err(error)) => Err(crate::runtime_error(error.clone())),
            Some(Ok(actor)) => KvMutationService::start(
                service_context,
                Arc::clone(&config_table),
                actor.clone(),
                target,
                queue_capacity,
                batch_size,
                max_pending_bytes,
                metrics,
            )
            .map_err(crate::runtime_error),
        };
        Self {
            config_table,
            name_server_runtime_inner,
            mutation_service,
            metadata_io,
        }
    }

    /// Gets a reference to the configuration table.
    ///
    /// # Returns
    ///
    /// A reference to the configuration table.
    #[inline]
    pub fn get_config_table(&self) -> &dashmap::DashMap<Namespace, ConfigMap> {
        &self.config_table
    }

    #[cfg(test)]
    pub(crate) fn metadata_io_snapshot(&self) -> Option<MetadataIoSnapshot> {
        self.metadata_io
            .as_ref()
            .and_then(|result| result.as_ref().ok())
            .map(MetadataIoActor::snapshot)
    }

    pub(crate) fn mutation_snapshot(&self) -> Option<KvMutationSnapshot> {
        self.mutation_service.as_ref().ok().map(KvMutationService::snapshot)
    }

    pub(crate) fn validate_persistence_owner(&self) -> NameServerResult<()> {
        self.mutation_service().map(|_| ())
    }

    /// Gets a reference to the Namesrv configuration.
    ///
    /// # Returns
    ///
    /// A reference to the Namesrv configuration.
    #[inline]
    pub fn get_namesrv_config(&self) -> Arc<NamesrvConfig> {
        self.name_server_runtime_inner.name_server_config()
    }

    /// Gets the number of pending changes since last persistence
    #[inline]
    pub fn pending_changes(&self) -> usize {
        self.mutation_snapshot().map_or(0, |snapshot| snapshot.pending_commands)
    }

    fn mutation_service(&self) -> NameServerResult<&KvMutationService> {
        self.mutation_service.as_ref().map_err(Arc::clone)
    }
}

impl KVConfigManager {
    /// Loads key-value configurations from a file.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if loading succeeds or file doesn't exist
    /// - `Err(SharedError)` if deserialization fails
    pub fn load(&self) -> NameServerResult<()> {
        let namesrv_config = self.name_server_runtime_inner.name_server_config();
        let config_path = Path::new(namesrv_config.kv_config_path.as_str());
        let config_table =
            load_config_table_with(config_path, |path| std::fs::read_to_string(path)).inspect_err(|error| {
                error!(
                    path = %config_path.display(),
                    error_code = %error.descriptor().code(),
                    "Failed to load KV config"
                );
            })?;

        if let Some(config_table) = config_table {
            let entry_count = config_table.len();
            for (namespace, kv_map) in config_table {
                self.config_table.insert(namespace, kv_map);
            }
            info!("Loaded {} namespace(s) from KV config file", entry_count);
        } else {
            debug!("KV config file is missing or empty, starting with an empty table");
        }

        Ok(())
    }

    /// Updates the Namesrv configuration.
    ///
    /// # Arguments
    ///
    /// * `updates` - Map of configuration key-value pairs to update
    ///
    /// # Returns
    ///
    /// - `Ok(())` if update succeeds
    /// - `Err(SharedError)` if validation fails
    pub fn update_namesrv_config(&self, updates: HashMap<CheetahString, CheetahString>) -> NameServerResult<()> {
        let result = self.name_server_runtime_inner.update_name_server_config(updates);

        if result.is_ok() {
            debug!("Namesrv configuration updated successfully");
        }

        result
    }

    /// Persists the current key-value configurations to a file.
    ///
    /// This method submits an immutable snapshot to the owned metadata actor
    /// and waits for durable completion.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if persistence succeeds
    /// - `Err(SharedError)` if serialization or file write fails
    pub async fn persist_until(&self, deadline: MetadataDeadline) -> NameServerResult<()> {
        self.mutation_service()?
            .submit(KvMutation::Persist, deadline)?
            .wait_until(deadline)
            .await?;
        debug!(operation = "persist", "KV config persisted successfully");
        Ok(())
    }

    /// Conditionally persists configuration based on pending changes and time threshold.
    ///
    /// This method implements deferred persistence to reduce I/O operations:
    /// - Persists if pending changes exceed AUTO_PERSIST_THRESHOLD
    /// - Persists if MIN_PERSIST_INTERVAL has elapsed since last persistence
    ///
    /// # Returns
    ///
    /// - `Ok(true)` if persistence was performed
    /// - `Ok(false)` if persistence was skipped
    /// - `Err(SharedError)` if persistence fails
    pub async fn persist_if_needed(&self, deadline: MetadataDeadline) -> NameServerResult<bool> {
        if self.pending_changes() > 0 {
            self.persist_until(deadline).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Forces immediate persistence regardless of thresholds.
    ///
    /// This should be called when shutting down or when immediate
    /// durability is required.
    #[inline]
    pub async fn force_persist(&self, deadline: MetadataDeadline) -> NameServerResult<()> {
        self.persist_until(deadline).await
    }

    /// Stops admission and drains the owned persistence actor.
    pub async fn shutdown_metadata_io(&self, deadline: MetadataDeadline) -> Option<MetadataIoShutdownReport> {
        if let Ok(service) = self.mutation_service.as_ref() {
            let report = service.shutdown_until(deadline).await;
            if report.timed_out || report.snapshot.pending_commands != 0 {
                error!(
                    timed_out = report.timed_out,
                    pending = report.snapshot.pending_commands,
                    desired_generation = report.snapshot.desired_generation,
                    durable_generation = report.snapshot.durable_generation,
                    applied_generation = report.snapshot.applied_generation,
                    "KV mutation service did not drain cleanly"
                );
            }
        }
        match self.metadata_io.as_ref() {
            Some(Ok(metadata_io)) => Some(metadata_io.shutdown_until(deadline).await),
            Some(Err(_)) | None => None,
        }
    }

    /// Adds or updates a key-value configuration.
    ///
    /// The mutation returns only after its generation is durable.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace for the configuration
    /// * `key` - The configuration key
    /// * `value` - The configuration value
    pub async fn put_kv_config(
        &self,
        namespace: Namespace,
        key: Key,
        value: Value,
        deadline: MetadataDeadline,
    ) -> NameServerResult<()> {
        let receipt = self
            .mutation_service()?
            .submit(KvMutation::Put { namespace, key, value }, deadline)?
            .wait_until(deadline)
            .await?;

        debug!(
            operation = "put",
            outcome = if receipt.changed_entries == 0 {
                "unchanged"
            } else {
                "changed"
            },
            "KV config mutated"
        );
        Ok(())
    }

    /// Deletes a key-value configuration.
    ///
    /// The mutation returns only after its generation is durable.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace for the configuration
    /// * `key` - The configuration key to delete
    ///
    /// # Returns
    ///
    /// - `Ok(true)` if the key was deleted
    /// - `Ok(false)` if the key didn't exist
    pub async fn delete_kv_config(
        &self,
        namespace: &Namespace,
        key: &Key,
        deadline: MetadataDeadline,
    ) -> NameServerResult<bool> {
        let receipt = self
            .mutation_service()?
            .submit(
                KvMutation::Delete {
                    namespace: namespace.clone(),
                    key: key.clone(),
                },
                deadline,
            )?
            .wait_until(deadline)
            .await?;
        if receipt.changed_entries > 0 {
            debug!(operation = "delete", outcome = "deleted", "KV config mutated");
            Ok(true)
        } else {
            debug!(
                operation = "delete",
                outcome = "not-found",
                "KV config mutation skipped"
            );
            Ok(false)
        }
    }

    /// Batch update multiple key-value configurations in a namespace.
    ///
    /// This is more efficient than calling `put_kv_config` multiple times
    /// as it performs a single persistence check at the end.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace for all configurations
    /// * `kv_pairs` - Map of key-value pairs to insert/update
    ///
    /// # Returns
    ///
    /// - `Ok(usize)` - Number of entries updated
    pub async fn batch_put_kv_config(
        &self,
        namespace: Namespace,
        kv_pairs: HashMap<Key, Value>,
        deadline: MetadataDeadline,
    ) -> NameServerResult<usize> {
        if kv_pairs.is_empty() {
            return Ok(0);
        }

        let receipt = self
            .mutation_service()?
            .submit(
                KvMutation::BatchPut {
                    namespace,
                    values: kv_pairs,
                },
                deadline,
            )?
            .wait_until(deadline)
            .await?;

        debug!(
            operation = "batch-put",
            changed_entries = receipt.changed_entries,
            "KV config batch mutated"
        );
        Ok(receipt.changed_entries)
    }

    /// Batch delete multiple key-value configurations in a namespace.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace for the configurations
    /// * `keys` - Keys to delete
    ///
    /// # Returns
    ///
    /// - `Ok(usize)` - Number of entries deleted
    pub async fn batch_delete_kv_config(
        &self,
        namespace: &Namespace,
        keys: &[Key],
        deadline: MetadataDeadline,
    ) -> NameServerResult<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        let receipt = self
            .mutation_service()?
            .submit(
                KvMutation::BatchDelete {
                    namespace: namespace.clone(),
                    keys: keys.to_vec(),
                },
                deadline,
            )?
            .wait_until(deadline)
            .await?;
        if receipt.changed_entries > 0 {
            debug!(
                operation = "batch-delete",
                changed_entries = receipt.changed_entries,
                "KV config batch mutated"
            );
        }
        Ok(receipt.changed_entries)
    }

    /// Deletes an entire namespace and all its key-value configurations.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to delete
    ///
    /// # Returns
    ///
    /// - `Ok(usize)` - Number of keys deleted
    pub async fn delete_namespace(&self, namespace: &Namespace, deadline: MetadataDeadline) -> NameServerResult<usize> {
        let receipt = self
            .mutation_service()?
            .submit(
                KvMutation::DeleteNamespace {
                    namespace: namespace.clone(),
                },
                deadline,
            )?
            .wait_until(deadline)
            .await?;
        if receipt.changed_entries > 0 {
            info!(
                operation = "delete-namespace",
                changed_entries = receipt.changed_entries,
                "KV namespace deleted"
            );
            Ok(receipt.changed_entries)
        } else {
            debug!(
                operation = "delete-namespace",
                outcome = "not-found",
                "KV namespace mutation skipped"
            );
            Ok(0)
        }
    }

    /// Gets the key-value list for a specific namespace.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to retrieve
    ///
    /// # Returns
    ///
    /// - `Some(Vec<u8>)` - Encoded KV table if namespace exists
    /// - `None` - If namespace doesn't exist
    pub fn get_kv_list_by_namespace(&self, namespace: &Namespace) -> Option<Vec<u8>> {
        self.config_table.get(namespace).and_then(|kv_table| {
            let table = KVTable {
                table: kv_table.clone(),
            };
            match table.encode() {
                Ok(encoded) => Some(encoded),
                Err(e) => {
                    error!(operation = "encode-namespace", error = %e, "Failed to encode KV table");
                    None
                }
            }
        })
    }

    /// Gets the value for a specific key in a namespace.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace
    /// * `key` - The configuration key
    ///
    /// # Returns
    ///
    /// - `Some(Value)` - The value if found
    /// - `None` - If namespace or key doesn't exist
    #[inline]
    pub fn get_kvconfig(&self, namespace: &Namespace, key: &Key) -> Option<Value> {
        self.config_table
            .get(namespace)
            .and_then(|kv_table| kv_table.get(key).cloned())
    }

    /// Checks if a namespace exists.
    #[inline]
    pub fn namespace_exists(&self, namespace: &Namespace) -> bool {
        self.config_table.contains_key(namespace)
    }

    /// Gets all namespaces.
    ///
    /// # Returns
    ///
    /// A vector of all namespace names
    pub fn get_all_namespaces(&self) -> Vec<Namespace> {
        self.config_table.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Gets the number of key-value pairs in a namespace.
    ///
    /// # Returns
    ///
    /// - `Some(usize)` - Number of keys if namespace exists
    /// - `None` - If namespace doesn't exist
    pub fn get_namespace_size(&self, namespace: &Namespace) -> Option<usize> {
        self.config_table.get(namespace).map(|kv_map| kv_map.len())
    }

    /// Gets all key-value pairs in a namespace.
    ///
    /// # Returns
    ///
    /// - `Some(HashMap)` - Clone of the KV map if namespace exists
    /// - `None` - If namespace doesn't exist
    pub fn get_all_in_namespace(&self, namespace: &Namespace) -> Option<ConfigMap> {
        self.config_table.get(namespace).map(|kv_map| kv_map.clone())
    }

    /// Gets statistics about the configuration manager.
    ///
    /// # Returns
    ///
    /// A tuple of (namespace_count, total_kv_pairs, pending_changes)
    pub fn get_statistics(&self) -> (usize, usize, usize) {
        let namespace_count = self.config_table.len();
        let total_kv_pairs: usize = self.config_table.iter().map(|entry| entry.value().len()).sum();
        let pending = self.pending_changes();

        (namespace_count, total_kv_pairs, pending)
    }
}


