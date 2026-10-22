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

use cheetah_string::CheetahString;
use rocketmq_model::common::filter::expression_type::ExpressionType;
use rocketmq_model::common::hasher::string_hasher::JavaStringHasher;

use crate::protocol::heartbeat::subscription_data::SubscriptionData;

pub struct FilterAPI;

impl FilterAPI {
    /// Custom: native resource-root equivalent of Java ClassLoader's simple-name .java lookup.
    pub fn simple_class_name(class_name: &str) -> &str {
        class_name.rsplit('.').next().unwrap_or(class_name)
    }

    pub fn class_file(
        class_name: &str,
        resource_root: &std::path::Path,
    ) -> std::io::Result<Option<std::path::PathBuf>> {
        let simple = Self::simple_class_name(class_name);
        if simple.is_empty() || simple.contains(['/', '\\']) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid Java resource name",
            ));
        }
        let path = resource_root.join(format!("{simple}.java"));
        match std::fs::metadata(&path) {
            Ok(value) => Ok(value.is_file().then_some(path)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// The source is retained locally and excluded from heartbeat serialization.
    pub fn build_class_filter_subscription(
        topic: CheetahString,
        class_name: CheetahString,
        source: Option<CheetahString>,
    ) -> SubscriptionData {
        SubscriptionData {
            topic,
            sub_string: class_name,
            class_filter_mode: true,
            filter_class_source: source,
            sub_version: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            ..SubscriptionData::default()
        }
    }

    pub fn build_subscription_data(
        topic: &CheetahString,
        sub_string: &CheetahString,
    ) -> Result<SubscriptionData, String> {
        let mut subscription_data = SubscriptionData {
            topic: topic.clone(),
            sub_string: sub_string.clone(),
            ..Default::default()
        };

        if sub_string.is_empty() || sub_string == SubscriptionData::SUB_ALL {
            subscription_data.sub_string = CheetahString::from_static_str(SubscriptionData::SUB_ALL);
            return Ok(subscription_data);
        }

        // Custom fork: Java String.split drops trailing empty segments.
        let mut tags: Vec<&str> = sub_string.split_str("||").collect();
        while tags.last().is_some_and(|tag| tag.is_empty()) {
            tags.pop();
        }
        if tags.is_empty() {
            return Err("subString split error".to_string());
        }

        for tag in tags {
            // Java String.trim removes only code points <= U+0020.
            let trimmed_tag = tag.trim_matches(|value: char| value <= '\u{20}');
            if !trimmed_tag.is_empty() {
                subscription_data.tags_set.insert(trimmed_tag.into());
                subscription_data
                    .code_set
                    .insert(JavaStringHasher::hash_str(trimmed_tag));
            }
        }

        Ok(subscription_data)
    }

    pub fn build_subscription_data_with_expression_type(
        topic: &CheetahString,
        sub_string: &CheetahString,
        expression_type: Option<CheetahString>,
    ) -> Result<SubscriptionData, CheetahString> {
        FilterAPI::build(topic, sub_string, expression_type).map_err(Into::into)
    }

    pub fn build(
        topic: &CheetahString,
        sub_string: &CheetahString,
        type_: Option<CheetahString>,
    ) -> Result<SubscriptionData, String> {
        let Some(expression_type) = type_ else {
            return FilterAPI::build_subscription_data(topic, sub_string);
        };

        if expression_type.as_str() == ExpressionType::TAG {
            return FilterAPI::build_subscription_data(topic, sub_string);
        }

        if sub_string.is_empty() {
            return Err(format!("Expression can't be null! {expression_type}"));
        }

        let mut subscription_data = SubscriptionData {
            topic: topic.clone(),
            sub_string: sub_string.clone(),
            ..Default::default()
        };
        subscription_data.expression_type = expression_type;
        Ok(subscription_data)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn build_subscription_data_creates_correct_subscription_data() {
        let topic = "test_topic".into();
        let sub_string = "tag1||tag2".into();
        let subscription_data = FilterAPI::build_subscription_data(&topic, &sub_string).unwrap();

        assert_eq!(subscription_data.topic.as_str(), topic.as_str());
        assert_eq!(subscription_data.sub_string.as_str(), sub_string.as_str());
        assert!(subscription_data.tags_set.contains("tag1"));
        assert!(subscription_data.tags_set.contains("tag2"));
    }

    #[test]
    fn build_subscription_data_with_empty_sub_string_creates_subscription_data_with_sub_all() {
        let topic = "test_topic".into();
        let sub_string = "".into();
        let subscription_data = FilterAPI::build_subscription_data(&topic, &sub_string).unwrap();

        assert_eq!(subscription_data.topic.as_str(), topic.as_str());
        assert_eq!(subscription_data.sub_string, SubscriptionData::SUB_ALL);
    }

    #[test]
    fn build_subscription_data_with_expression_type_sets_expression_type() {
        let topic = "test_topic".into();
        let sub_string = "tag1||tag2".into();
        let expression_type: CheetahString = "SQL92".into();
        let subscription_data =
            FilterAPI::build_subscription_data_with_expression_type(&topic, &sub_string, Some(expression_type.clone()))
                .unwrap();

        assert_eq!(subscription_data.topic.as_str(), topic.as_str());
        assert_eq!(subscription_data.sub_string.as_str(), sub_string.as_str());
        assert_eq!(subscription_data.expression_type, expression_type);
    }

    #[test]
    fn build_creates_correct_subscription_data_for_tag_expression_type() {
        let topic = "test_topic".into();
        let sub_string = "tag1||tag2".into();
        let type_ = Some(ExpressionType::TAG.into());
        let subscription_data = FilterAPI::build(&topic, &sub_string, type_).unwrap();

        assert_eq!(subscription_data.topic.as_str(), topic);
        assert_eq!(subscription_data.sub_string.as_str(), sub_string.as_str());
    }

    #[test]
    fn build_creates_tag_subscription_for_none_expression_type_like_java() {
        let topic = "test_topic".into();
        let sub_string = "tag1||tag2".into();
        let subscription_data = FilterAPI::build(&topic, &sub_string, None).unwrap();

        assert_eq!(subscription_data.topic.as_str(), topic);
        assert_eq!(subscription_data.sub_string.as_str(), sub_string.as_str());
        assert!(subscription_data.tags_set.contains("tag1"));
        assert!(subscription_data.tags_set.contains("tag2"));
    }

    #[test]
    fn build_treats_empty_tag_expression_as_sub_all_like_java() {
        let topic = "test_topic".into();
        let sub_string = "".into();
        let type_ = Some(ExpressionType::TAG.into());
        let subscription_data = FilterAPI::build(&topic, &sub_string, type_).unwrap();

        assert_eq!(subscription_data.topic.as_str(), topic);
        assert_eq!(subscription_data.sub_string, SubscriptionData::SUB_ALL);
    }

    #[test]
    fn build_returns_error_for_empty_sub_string_and_non_tag_expression_type() {
        let topic = "test_topic".into();
        let sub_string = "".into();
        let type_ = Some(ExpressionType::SQL92.into());
        let result = FilterAPI::build(&topic, &sub_string, type_);

        assert!(result.is_err());
    }
}
