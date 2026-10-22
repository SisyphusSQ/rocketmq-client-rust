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

use std::sync::atomic::{AtomicI32, Ordering};

use rocketmq_model::common::message::message_queue::MessageQueue;

// Custom fork: shared listener context must support Java partial ACK and retry delay mutation.
#[derive(Debug)]
pub struct ConsumeConcurrentlyContext {
    pub(crate) message_queue: MessageQueue,
    delay_level_when_next_consume: AtomicI32,
    ack_index: AtomicI32,
}

impl ConsumeConcurrentlyContext {
    pub fn new(message_queue: MessageQueue) -> Self {
        Self {
            message_queue,
            delay_level_when_next_consume: AtomicI32::new(0),
            ack_index: AtomicI32::new(i32::MAX),
        }
    }

    pub fn get_delay_level_when_next_consume(&self) -> i32 {
        self.delay_level_when_next_consume.load(Ordering::Acquire)
    }

    pub fn set_delay_level_when_next_consume(&self, delay_level_when_next_consume: i32) {
        self.delay_level_when_next_consume
            .store(delay_level_when_next_consume, Ordering::Release);
    }

    pub fn get_message_queue(&self) -> &MessageQueue {
        &self.message_queue
    }

    pub fn get_ack_index(&self) -> i32 {
        self.ack_index.load(Ordering::Acquire)
    }

    pub fn set_ack_index(&self, ack_index: i32) {
        self.ack_index.store(ack_index, Ordering::Release);
    }
}
