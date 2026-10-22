// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.
use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodec;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Serialize, Deserialize, RequestHeaderCodec)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::report_consume_stage_request_header::ReportConsumeStageRequestHeader",
    java_class = "org.apache.rocketmq.common.protocol.header.ReportConsumeStageRequestHeader"
)]
pub struct ReportConsumeStageRequestHeader {
    #[header(required)]
    pub host: CheetahString,
}
