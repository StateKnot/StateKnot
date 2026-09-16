// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::AgentWorkerError;
use stateknot_core::{CancellationSignal, TenantId};
use stateknot_runtime::{
    DurableFairScheduler, DurableFairSchedulerBuildError, DurableFairSchedulerOptions,
    DurableGraphDriverOptions, DurableGraphLifecycleOptions, DurableTenantScheduler,
    DurableTenantSchedulerBuildError, DurableTenantSchedulerOptions, ExecutableGraphRegistry,
    GraphExecutionActivity, GraphLifecycleEvidenceProvider, TenantSchedulerTick,
    WeightedFairnessPolicy,
};
use stateknot_store_postgres::PostgresStore;
use std::sync::Arc;

/// Existing validated driver, lifecycle, scan and reservation policies.
#[derive(Clone, Copy, Debug, Default)]
pub struct AgentWorkerExecutionOptions {
    /// Lease renewal, node deadline, replay and parallel-node limits.
    pub driver: DurableGraphDriverOptions,
    /// Bounded lifecycle mutation retries.
    pub lifecycle: DurableGraphLifecycleOptions,
    /// Bounded per-tenant selection and claim retries.
    pub tenant: DurableTenantSchedulerOptions,
    /// Bounded fair-slot reservation retries; unused by a tenant binding.
    pub fairness: DurableFairSchedulerOptions,
}

enum Scheduler {
    Tenant {
        scheduler: DurableTenantScheduler,
        tenant: TenantId,
    },
    Fair(DurableFairScheduler),
}

/// Non-cloneable ownership of a fresh concrete scheduler and its exact dependencies.
/// No scheduler handle escapes; stopping the role stops every tick producer.
pub struct AgentWorkerBinding {
    store: PostgresStore,
    registry: ExecutableGraphRegistry,
    scheduler: Scheduler,
}

impl AgentWorkerBinding {
    /// Binds one explicitly authorized tenant without claiming work.
    pub fn tenant(
        store: PostgresStore,
        registry: ExecutableGraphRegistry,
        evidence: Arc<dyn GraphLifecycleEvidenceProvider>,
        tenant: TenantId,
        options: AgentWorkerExecutionOptions,
    ) -> Result<Self, DurableTenantSchedulerBuildError> {
        let scheduler = DurableTenantScheduler::new(
            store.clone(),
            registry.clone(),
            evidence,
            options.driver,
            options.lifecycle,
            options.tenant,
        )?;
        Ok(Self {
            store,
            registry,
            scheduler: Scheduler::Tenant { scheduler, tenant },
        })
    }

    /// Registers an immutable fair policy but does not reserve slots or claim runs.
    /// Use a host startup deadline; cancelled registration can have committed and
    /// is safe to retry with the identical immutable policy.
    pub async fn fair(
        store: PostgresStore,
        registry: ExecutableGraphRegistry,
        evidence: Arc<dyn GraphLifecycleEvidenceProvider>,
        policy: WeightedFairnessPolicy,
        options: AgentWorkerExecutionOptions,
    ) -> Result<Self, DurableFairSchedulerBuildError> {
        let scheduler = DurableFairScheduler::register(
            store.clone(),
            registry.clone(),
            evidence,
            options.driver,
            options.lifecycle,
            options.tenant,
            policy,
            options.fairness,
        )
        .await?;
        Ok(Self {
            store,
            registry,
            scheduler: Scheduler::Fair(scheduler),
        })
    }

    pub(super) fn activity(&self) -> GraphExecutionActivity {
        match &self.scheduler {
            Scheduler::Tenant { scheduler, .. } => scheduler.execution_activity(),
            Scheduler::Fair(scheduler) => scheduler.execution_activity(),
        }
    }

    pub(super) async fn check(&self) -> Result<(), AgentWorkerError> {
        if self.registry.is_empty() {
            return Err(AgentWorkerError::Unavailable);
        }
        self.store
            .verify_schema()
            .await
            .map_err(|_| AgentWorkerError::Unavailable)?;
        if let Scheduler::Fair(scheduler) = &self.scheduler {
            scheduler
                .check_readiness()
                .await
                .map_err(|_| AgentWorkerError::Unavailable)?;
        }
        Ok(())
    }

    pub(super) async fn tick(
        &self,
        signal: CancellationSignal,
    ) -> Result<TenantSchedulerTick, AgentWorkerError> {
        match &self.scheduler {
            Scheduler::Tenant { scheduler, tenant } => scheduler
                .tick(tenant.clone(), signal)
                .await
                .map_err(|_| AgentWorkerError::Unavailable),
            Scheduler::Fair(scheduler) => scheduler
                .tick(signal)
                .await
                .map(|tick| tick.into_parts().4)
                .map_err(|_| AgentWorkerError::Unavailable),
        }
    }
}
