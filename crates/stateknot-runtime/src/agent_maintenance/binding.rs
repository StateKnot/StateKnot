// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::{AgentMaintenanceError, AgentMaintenanceJob, AgentMaintenanceJobReport};
use crate::{
    AgentDeadlineSweepCursor, ChildJoinPublicationCursor, ChildReconciliationCursor,
    DurableAgentDeadlineReconciler, DurableChildJoinPublisher, DurableChildReconciler,
    DurableChildReconcilerOptions, DurableGraphLifecycleOptions, DurableRunFailureCloser,
    JsonSchemaRegistry, RunFailureCloseSweepCursor,
};
use stateknot_core::{CancellationSignal, TenantId};
use stateknot_store_postgres::{PostgresStore, StoreError};

/// Existing validated mutation retry policies, shared by the relevant jobs.
#[derive(Clone, Copy, Debug, Default)]
pub struct AgentMaintenanceMutationOptions {
    /// Deadline and failure-close retries.
    pub lifecycle: DurableGraphLifecycleOptions,
    /// Child cancellation/settlement and Join publication retries.
    pub child: DurableChildReconcilerOptions,
}

/// Exclusive ownership of concrete maintainers over the same store and frozen
/// schemas. The host must authorize every tenant before construction.
pub struct AgentMaintenanceBinding {
    store: PostgresStore,
    tenants: Vec<TenantId>,
    deadline: DurableAgentDeadlineReconciler,
    child: DurableChildReconciler,
    join: DurableChildJoinPublisher,
    close: DurableRunFailureCloser,
}
impl AgentMaintenanceBinding {
    /// Requires 1–128 unique tenants and all four exact standard event schemas.
    /// Sorting is deterministic; duplicates fail closed rather than weighting work.
    pub fn new(
        store: PostgresStore,
        schemas: JsonSchemaRegistry,
        mut tenants: Vec<TenantId>,
        options: AgentMaintenanceMutationOptions,
    ) -> Result<Self, AgentMaintenanceError> {
        if tenants.is_empty() || tenants.len() > 128 {
            return Err(AgentMaintenanceError::InvalidTenants);
        }
        tenants.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        if tenants.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AgentMaintenanceError::InvalidTenants);
        }
        Ok(Self {
            deadline: DurableAgentDeadlineReconciler::new(
                store.clone(),
                schemas.clone(),
                options.lifecycle,
            )
            .map_err(|_| AgentMaintenanceError::SchemaUnavailable)?,
            child: DurableChildReconciler::new(store.clone(), schemas.clone(), options.child)
                .map_err(|_| AgentMaintenanceError::SchemaUnavailable)?,
            join: DurableChildJoinPublisher::new(store.clone(), schemas.clone(), options.child)
                .map_err(|_| AgentMaintenanceError::SchemaUnavailable)?,
            close: DurableRunFailureCloser::new(store.clone(), schemas, options.lifecycle)
                .map_err(|_| AgentMaintenanceError::SchemaUnavailable)?,
            store,
            tenants,
        })
    }
    pub(super) async fn check(&self) -> Result<(), AgentMaintenanceError> {
        self.store
            .verify_schema()
            .await
            .map_err(|_| AgentMaintenanceError::Unavailable)
    }
    pub(super) fn rotation(&self) -> Vec<Progress> {
        self.tenants
            .iter()
            .flat_map(|tenant| {
                [
                    Progress::Deadline(tenant.clone(), None),
                    Progress::Child(tenant.clone(), None),
                    Progress::Join(tenant.clone(), None),
                    Progress::Close(tenant.clone(), None),
                ]
            })
            .collect()
    }
    pub(super) async fn tick(
        &self,
        progress: &mut Progress,
        signal: CancellationSignal,
    ) -> Result<AgentMaintenanceJobReport, StoreError> {
        // Each returned cursor advances past item errors, and resets at exhaustion.
        // A discovery error leaves the old cursor intact; restart starts at None.
        let (items, failures) = match progress {
            Progress::Deadline(tenant, cursor) => {
                let tick = self
                    .deadline
                    .tick(tenant.clone(), cursor.clone(), signal)
                    .await?;
                *cursor = Some(tick.cursor().clone());
                (
                    tick.items().len(),
                    tick.items()
                        .iter()
                        .filter(|item| item.result().is_err())
                        .count(),
                )
            }
            Progress::Child(tenant, cursor) => {
                let tick = self
                    .child
                    .tick(tenant.clone(), cursor.as_deref().cloned(), signal)
                    .await?;
                *cursor = Some(Box::new(tick.cursor().clone()));
                (
                    tick.items().len(),
                    tick.items()
                        .iter()
                        .filter(|item| item.result().is_err())
                        .count(),
                )
            }
            Progress::Join(tenant, cursor) => {
                let tick = self
                    .join
                    .tick(tenant.clone(), cursor.clone(), signal)
                    .await?;
                *cursor = Some(tick.cursor().clone());
                (
                    tick.items().len(),
                    tick.items()
                        .iter()
                        .filter(|item| item.result().is_err())
                        .count(),
                )
            }
            Progress::Close(tenant, cursor) => {
                let tick = self
                    .close
                    .tick(tenant.clone(), cursor.clone(), signal)
                    .await?;
                *cursor = Some(tick.cursor().clone());
                (
                    tick.items().len(),
                    tick.items()
                        .iter()
                        .filter(|item| item.result().is_err())
                        .count(),
                )
            }
        };
        Ok(AgentMaintenanceJobReport {
            completed_ticks: 1,
            items: items as u64,
            item_failures: failures as u64,
        })
    }
}
pub(super) enum Progress {
    Deadline(TenantId, Option<AgentDeadlineSweepCursor>),
    Child(TenantId, Option<Box<ChildReconciliationCursor>>),
    Join(TenantId, Option<ChildJoinPublicationCursor>),
    Close(TenantId, Option<RunFailureCloseSweepCursor>),
}
impl Progress {
    pub(super) const fn job(&self) -> AgentMaintenanceJob {
        match self {
            Self::Deadline(..) => AgentMaintenanceJob::Deadline,
            Self::Child(..) => AgentMaintenanceJob::Child,
            Self::Join(..) => AgentMaintenanceJob::Join,
            Self::Close(..) => AgentMaintenanceJob::FailureClose,
        }
    }
}
