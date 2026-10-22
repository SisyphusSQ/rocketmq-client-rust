// Copyright 2026 IQIYI
// Licensed under the Apache License, Version 2.0.

use std::error::Error as StdError;
use std::fmt;

use cheetah_string::CheetahString;

use crate::{ClientError, ClientFailureKind, ClientResult};

/// Canonical raw Java Properties, preserving unknown server keys.
pub type AdminProperties = std::collections::HashMap<CheetahString, CheetahString>;

/// One physical node attempt. A failed write can have an unknown outcome; inspect its request_stage.
pub struct AdminNodeResult<T> {
    pub address: CheetahString,
    pub result: ClientResult<T>,
}

/// Preserves successful nodes and failures instead of presenting a partial operation as complete.
pub struct AdminNodeReport<T> {
    pub nodes: Vec<AdminNodeResult<T>>,
    pub not_attempted: Vec<CheetahString>,
}

impl<T> fmt::Debug for AdminNodeReport<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Do not expose configuration values or ACL credentials through derived Debug output.
        f.debug_struct("AdminNodeReport")
            .field("attempted", &self.nodes.len())
            .field(
                "succeeded",
                &self.nodes.iter().filter(|node| node.result.is_ok()).count(),
            )
            .field("not_attempted", &self.not_attempted.len())
            .finish()
    }
}

impl<T> AdminNodeReport<T> {
    pub fn is_complete(&self) -> bool {
        self.not_attempted.is_empty() && self.nodes.iter().all(|node| node.result.is_ok())
    }
}

/// Typed error retained by convenience APIs; callers can downcast through ClientError::source_ref.
pub struct AdminPartialFailure<T> {
    pub report: AdminNodeReport<T>,
    selected: ClientError,
}

impl<T> fmt::Debug for AdminPartialFailure<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AdminPartialFailure").field(&self.report).finish()
    }
}
impl<T> fmt::Display for AdminPartialFailure<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "administration did not complete on every requested node")
    }
}
impl<T> StdError for AdminPartialFailure<T> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.selected)
    }
}

impl<T: Send + Sync + 'static> AdminNodeReport<T> {
    pub fn into_result(self) -> ClientResult<Vec<(CheetahString, T)>> {
        let failure = self.nodes.iter().filter_map(|node| node.result.as_ref().err());
        let decisive = failure
            .clone()
            .find(|error| {
                matches!(
                    error.failure_kind(),
                    ClientFailureKind::Authentication | ClientFailureKind::PermissionDenied
                )
            })
            .or_else(|| failure.into_iter().next())
            .cloned();
        if let Some(selected) = decisive {
            let stage = selected.request_stage();
            let error = rocketmq_error::Error::caused_by(
                selected.descriptor(),
                AdminPartialFailure {
                    report: self,
                    selected: selected.clone(),
                },
            )
            .with_context(selected.shared_error().context().clone());
            let error = ClientError::from_error(error);
            return Err(match stage {
                Some(stage) => error.with_request_stage(stage),
                None => error,
            });
        }
        if !self.not_attempted.is_empty() {
            return Err(ClientError::not_started());
        }
        Ok(self
            .nodes
            .into_iter()
            .filter_map(|node| node.result.ok().map(|value| (node.address, value)))
            .collect())
    }
}
