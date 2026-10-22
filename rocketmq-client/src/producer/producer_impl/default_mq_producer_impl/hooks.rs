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

impl DefaultMQProducerImpl {
    pub fn register_end_transaction_hook(&self, hook: Arc<dyn EndTransactionHook>) {
        let mut pending = self.pending_end_transaction_hooks.lock();
        if let Some(hooks) = pending.as_mut() {
            hooks.push(hook);
            *self.end_transaction_hook_list.write() = hooks.clone().into();
        } else {
            let mut current = self.end_transaction_hook_list.write();
            let mut hooks = current.to_vec();
            hooks.push(hook);
            *current = hooks.into();
        }
    }

    pub fn register_check_forbidden_hook(&self, hook: Arc<dyn CheckForbiddenHook>) {
        let mut pending = self.pending_forbidden_hooks.lock();
        if let Some(hooks) = pending.as_mut() {
            hooks.push(hook);
            *self.check_forbidden_hook_list.write() = hooks.clone().into();
        } else {
            let mut current = self.check_forbidden_hook_list.write();
            let mut hooks = current.to_vec();
            hooks.push(hook);
            *current = hooks.into();
        }
    }

    pub fn register_send_message_hook(&self, hook: Arc<dyn SendMessageHook>) {
        let mut pending = self.pending_send_hooks.lock();
        if let Some(hooks) = pending.as_mut() {
            hooks.push(hook);
            *self.send_message_hook_list.write() = hooks.clone().into();
        } else {
            let mut current = self.send_message_hook_list.write();
            let mut hooks = current.to_vec();
            hooks.push(hook);
            *current = hooks.into();
        }
    }

    pub fn set_rpc_hook(&self, rpc_hook: Arc<dyn RPCHook>) {
        let mut current_hook = self.rpc_hook.write();
        let current_state = ProducerState::from_u8(self.state.load(Ordering::Relaxed));
        if current_state != ProducerState::Created {
            tracing::warn!(
                "Cannot update RPC hook after producer started (state: {:?})",
                current_state
            );
            return;
        }

        *current_hook = Some(rpc_hook);
    }
}
