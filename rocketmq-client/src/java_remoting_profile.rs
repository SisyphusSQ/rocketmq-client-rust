// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.
//! Explicit application-owned equivalents of Java Netty configuration and logging.

/// Java's worker count belongs to the application RuntimeOwner in native Rust.
/// The fixed client's pooled allocator flag is a bean property only: the pinned
/// NettyRemotingClient never reads it. It is retained here without inventing a pool.
#[derive(Clone, Debug)]
pub struct JavaRemotingProfile {
    pub client_worker_threads: usize,
    pub client_callback_executor_threads: i32,
    pub client_oneway_semaphore_value: u32,
    pub client_async_semaphore_value: u32,
    pub client_pooled_byte_buf_allocator_enable: bool,
    pub socket_sndbuf_size: usize,
    pub socket_rcvbuf_size: usize,
}
impl Default for JavaRemotingProfile {
    fn default() -> Self {
        Self {
            client_worker_threads: 4,
            client_callback_executor_threads: num_cpus::get() as i32,
            client_oneway_semaphore_value: 65535,
            client_async_semaphore_value: 65535,
            client_pooled_byte_buf_allocator_enable: false,
            socket_sndbuf_size: 65535,
            socket_rcvbuf_size: 65535,
        }
    }
}
impl JavaRemotingProfile {
    pub const COM_ROCKETMQ_REMOTING_NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE: &'static str =
        "com.rocketmq.remoting.nettyPooledByteBufAllocatorEnable";
    pub const COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE: &'static str = "com.rocketmq.remoting.socket.sndbuf.size";
    pub const COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE: &'static str = "com.rocketmq.remoting.socket.rcvbuf.size";
    pub const COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE: &'static str =
        "com.rocketmq.remoting.clientAsyncSemaphoreValue";
    pub const COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE: &'static str =
        "com.rocketmq.remoting.clientOnewaySemaphoreValue";
    /// Parses an explicit property snapshot; never mutates process environment.
    pub fn from_properties(properties: &std::collections::HashMap<String, String>) -> crate::ClientResult<Self> {
        let mut result = Self::default();
        if let Some(value) = properties.get(Self::COM_ROCKETMQ_REMOTING_NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE) {
            result.client_pooled_byte_buf_allocator_enable = value.eq_ignore_ascii_case("true");
        }
        for (key, target) in [
            (
                Self::COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE,
                &mut result.client_async_semaphore_value,
            ),
            (
                Self::COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE,
                &mut result.client_oneway_semaphore_value,
            ),
        ] {
            if let Some(value) = properties.get(key) {
                // Negative Java semaphore debt never admits a request; the native
                // equivalent is a zero-capacity gate with the same deadline/stop.
                *target = value
                    .parse::<i32>()
                    .map_err(|e| crate::ClientError::config_parse_source(key, e))?
                    .max(0) as u32;
            }
        }
        for (key, target) in [
            (
                Self::COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE,
                &mut result.socket_sndbuf_size,
            ),
            (
                Self::COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE,
                &mut result.socket_rcvbuf_size,
            ),
        ] {
            if let Some(value) = properties.get(key) {
                let parsed = value
                    .parse::<i32>()
                    .map_err(|e| crate::ClientError::config_parse_source(key, e))?;
                *target = usize::try_from(parsed).map_err(|e| crate::ClientError::config_parse_source(key, e))?;
            }
        }
        Ok(result)
    }
    pub fn runtime_config(&self) -> rocketmq_runtime::RuntimeConfig {
        let mut config = crate::ClientRuntime::java_4_6_1_runtime_config();
        config.worker_threads = self.client_worker_threads;
        config
    }
    pub fn apply_to(&self, client: &mut crate::ClientConfig) -> crate::ClientResult<()> {
        self.runtime_config()
            .validate()
            .map_err(crate::ClientError::illegal_argument_source)?;
        client.set_java_client_callback_executor_threads(self.client_callback_executor_threads);
        client.client_async_semaphore_value = self.client_async_semaphore_value;
        client.client_oneway_semaphore_value = self.client_oneway_semaphore_value;
        client.socket_options.send_buffer_bytes = std::num::NonZeroUsize::new(self.socket_sndbuf_size);
        client.socket_options.receive_buffer_bytes = std::num::NonZeroUsize::new(self.socket_rcvbuf_size);
        crate::base::client_config_validation::ClientConfigValidator::validate_config(client)
    }
}

/// Native logger is the application's tracing dispatcher. The SDK creates no
/// hidden file, appender, global subscriber or logger thread. Level, file rotation,
/// root path, additive routing and async queue policy belong to that dispatcher.
pub struct ClientLogger;
impl ClientLogger {
    pub const CLIENT_LOG_USESLF4J: &'static str = "rocketmq.client.logUseSlf4j";
    pub const CLIENT_LOG_ROOT: &'static str = "rocketmq.client.logRoot";
    pub const CLIENT_LOG_MAXINDEX: &'static str = "rocketmq.client.logFileMaxIndex";
    pub const CLIENT_LOG_FILESIZE: &'static str = "rocketmq.client.logFileMaxSize";
    pub const CLIENT_LOG_LEVEL: &'static str = "rocketmq.client.logLevel";
    pub const CLIENT_LOG_ADDITIVE: &'static str = "rocketmq.client.log.additive";
    pub const CLIENT_LOG_FILENAME: &'static str = "rocketmq.client.logFileName";
    pub const CLIENT_LOG_ASYNC_QUEUESIZE: &'static str = "rocketmq.client.logAsyncQueueSize";
    pub const ROCKETMQ_CLIENT_APPENDER_NAME: &'static str = "RocketmqClientAppender";
    pub fn get_log() -> tracing::Dispatch {
        tracing::dispatcher::get_default(Clone::clone)
    }
}
