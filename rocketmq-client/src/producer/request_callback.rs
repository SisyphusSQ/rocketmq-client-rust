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

use std::sync::Arc;

use crate::ClientError;
use rocketmq_model::common::message::message_single::Message;
use rocketmq_model::common::message::MessageTrait;

pub type RequestCallbackFn = Arc<dyn Fn(Option<&dyn MessageTrait>, Option<&ClientError>) + Send + Sync>;

pub trait RequestCallback: Sync + Send {
    fn on_success(&self, response: &Message);
    fn on_exception(&self, e: &ClientError);
}

// Custom fork: Java-style trait callbacks adapt to the same single-completion closure path.
pub fn request_callback_fn(callback: Arc<dyn RequestCallback>) -> RequestCallbackFn {
    Arc::new(move |message, error| {
        if let Some(error) = error {
            callback.on_exception(error);
        } else if let Some(message) = message {
            if let Some(message) = message.as_any().downcast_ref::<Message>() {
                callback.on_success(message);
            } else if let Some(message) = message
                .as_any()
                .downcast_ref::<rocketmq_model::common::message::message_ext::MessageExt>()
            {
                callback.on_success(&message.message);
            } else {
                let mut value = Message::default();
                value.set_topic(message.topic().clone());
                value.set_flag(message.get_flag());
                value.set_body(message.get_body().cloned());
                value.set_properties(message.get_properties().clone());
                callback.on_success(&value);
            }
        }
    })
}

#[cfg(test)]
mod custom_callback_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Callback(Arc<AtomicUsize>);
    impl RequestCallback for Callback {
        fn on_success(&self, message: &Message) {
            assert_eq!(message.topic(), "callback-topic");
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn on_exception(&self, error: &ClientError) {
            assert_eq!(error.failure_kind(), crate::ClientFailureKind::Timeout);
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn java_trait_adapter_keeps_message_ext_fields_and_completes_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let callback = request_callback_fn(Arc::new(Callback(calls.clone())));
        let future = crate::RequestResponseFuture::new("trait".into(), 1000, Some(callback.clone()));
        let mut response = crate::MessageExt::default();
        response.set_topic("callback-topic".into());
        future.put_response_message(Some(Box::new(response)));
        future.on_success();
        future.execute_request_callback();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(future.get_response_msg().is_some());
        let future = crate::RequestResponseFuture::new("failure".into(), 1000, Some(callback));
        future.complete_error(ClientError::request_timeout(1000));
        future.execute_request_callback();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
