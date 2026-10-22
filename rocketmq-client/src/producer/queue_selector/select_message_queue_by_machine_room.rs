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

use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::MessageTrait;

use crate::producer::message_queue_selector::MessageQueueSelector;

#[derive(Debug, Clone, Default)]
pub struct SelectMessageQueueByMachineRoom {
    consumer_idcs: HashSet<CheetahString>,
}

impl SelectMessageQueueByMachineRoom {
    pub fn new(consumer_idcs: HashSet<CheetahString>) -> Self {
        Self { consumer_idcs }
    }

    #[inline]
    pub fn consumer_idcs(&self) -> &HashSet<CheetahString> {
        &self.consumer_idcs
    }

    #[inline]
    pub fn set_consumer_idcs(&mut self, consumer_idcs: HashSet<CheetahString>) {
        self.consumer_idcs = consumer_idcs;
    }
}

impl<M, A> MessageQueueSelector<M, A> for SelectMessageQueueByMachineRoom
where
    M: MessageTrait,
{
    fn select(&self, _mqs: &[MessageQueue], _msg: &M, _arg: &A) -> Option<MessageQueue> {
        // Custom: pinned Java 4.6.1 always returns null; do not invent routing behavior.
        None
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_model::common::message::message_single::Message;
    use rocketmq_model::hashset;

    use super::*;

    #[test]
    fn fixed_java_machine_room_selector_returns_null_even_for_matching_room() {
        let selector = SelectMessageQueueByMachineRoom::new(hashset!(CheetahString::from("room-a")));
        let queues = vec![
            MessageQueue::from_parts("topic", "room-b@broker", 0),
            MessageQueue::from_parts("topic", "room-a@broker", 1),
        ];
        let msg = Message::builder().topic("topic").build().unwrap();

        let selected = selector.select(&queues, &msg, &());

        assert_eq!(selected, None);
    }

    #[test]
    fn select_message_queue_by_machine_room_returns_none_without_match() {
        let selector = SelectMessageQueueByMachineRoom::new(hashset!(CheetahString::from("room-a")));
        let queues = vec![MessageQueue::from_parts("topic", "broker", 0)];
        let msg = Message::builder().topic("topic").build().unwrap();

        assert_eq!(selector.select(&queues, &msg, &()), None);
    }
}
