// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Concrete parent scope shared by child admission, ownership and Join.
#[allow(clippy::wildcard_imports)]
use super::*;
use stateknot_core::{ChildRunAdmissionIntent, ChildRunBudgetAccount, GraphFrameCheckpoint};

pub(super) enum ParentScope {
    Root(Box<Checkpoint>),
    Frame {
        checkpoint: Box<GraphFrameCheckpoint>,
        suspended: Vec<Uuid>,
    },
}

pub(super) struct Preflight {
    pub(super) admission: StoredAgentAdmission,
    pub(super) scope: ParentScope,
    pub(super) root_graph: CompiledGraph,
    pub(super) owner_graph: CompiledGraph,
    pub(super) observed_at: Timestamp,
}

pub(super) async fn graph(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    reference: &GraphReference,
) -> Result<CompiledGraph, StoreError> {
    let row = load_graph_definition_row(tx, tenant, reference)
        .await?
        .ok_or(StoreError::GraphDefinitionNotFound)?;
    let graph = decode_graph_definition(row)?.graph().clone();
    if graph.reference() != *reference {
        return Err(StoreError::corrupt("child scope graph reference"));
    }
    Ok(graph)
}

impl PostgresStore {
    pub(super) async fn child_scope_preflight(
        &self,
        intent: &ChildRunAdmissionIntent,
        append: &JournalAppend,
    ) -> Result<Preflight, StoreError> {
        let activation = intent.key().parent();
        let mut tx = self
            .begin_repeatable_read("child scope schema preflight")
            .await?;
        let row = query_as::<_, RunRow>(SELECT_RUN)
            .bind(activation.tenant_id().as_str())
            .bind(*activation.run_id().as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|source| StoreError::database("child scope preflight Run", source))?
            .ok_or(StoreError::AgentAdmissionNotFound)?;
        let run = decode_run(row)?;
        verify_current_wait_set(&mut tx, &run).await?;
        root_graph_barriers::verify_run(&mut tx, &run).await?;
        if append.expectation().head() != run.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }
        let row = load_agent_admission_row(&mut tx, activation.tenant_id(), activation.run_id())
            .await?
            .ok_or(StoreError::AgentAdmissionNotFound)?;
        let admission = verify_stored_agent_admission(&mut tx, run, row).await?;
        let scope = current(&mut tx, admission.run(), activation).await?;
        scope.validate_intent(admission.admission(), intent)?;
        let root_graph = graph(
            &mut tx,
            activation.tenant_id(),
            admission.admission().intent().graph(),
        )
        .await?;
        let owner_graph =
            graph(&mut tx, activation.tenant_id(), scope.checkpoint().graph()).await?;
        scope.validate_policy(&root_graph, &owner_graph, intent)?;
        let observed_at = database_now(&mut tx, "child scope preflight clock").await?;
        tx.commit()
            .await
            .map_err(|source| StoreError::database("child scope preflight commit", source))?;
        Ok(Preflight {
            admission,
            scope,
            root_graph,
            owner_graph,
            observed_at,
        })
    }
}

impl ParentScope {
    pub(super) fn checkpoint(&self) -> &Checkpoint {
        match self {
            Self::Root(checkpoint) => checkpoint,
            Self::Frame { checkpoint, .. } => checkpoint.checkpoint(),
        }
    }

    pub(super) fn suspended_callers(&self) -> &[Uuid] {
        match self {
            Self::Root(_) => &[],
            Self::Frame { suspended, .. } => suspended,
        }
    }

    fn matches(&self, activation: &NodeActivation) -> bool {
        let actual = match self {
            Self::Root(checkpoint) => {
                NodeActivation::for_ready_root(checkpoint, activation.node_id().clone()).ok()
            }
            Self::Frame { checkpoint, .. } => {
                checkpoint.activation(activation.node_id().clone()).ok()
            }
        };
        actual.is_some_and(|actual| &actual == activation)
    }

    pub(super) fn validate_schemas<V: GraphSchemaValidator + ?Sized>(
        &self,
        parent: &AgentAdmission,
        intent: &ChildRunAdmissionIntent,
        schemas: &V,
        at: Timestamp,
    ) -> Result<(), StoreError> {
        let result = match self {
            Self::Root(checkpoint) => intent.validate_for(parent, checkpoint, schemas, at),
            Self::Frame { checkpoint, .. } => {
                intent.validate_for_frame(parent, checkpoint, schemas, at)
            }
        };
        result.map_err(|_| StoreError::ChildRunRejected)
    }

    // Repeat all intrinsic parent/principal/narrowing/full-activation checks
    // against this transaction's authenticated scope, without calling schemas.
    pub(super) fn validate_intent(
        &self,
        parent: &AgentAdmission,
        intent: &ChildRunAdmissionIntent,
    ) -> Result<(), StoreError> {
        let restored = match self {
            Self::Root(_) => ChildRunAdmissionIntent::new(
                parent,
                intent.key().clone(),
                intent.child().clone(),
                intent.child_graph().clone(),
                intent.initial_state().clone(),
            ),
            Self::Frame { checkpoint, .. } => ChildRunAdmissionIntent::for_frame(
                parent,
                checkpoint,
                intent.key().clone(),
                intent.child().clone(),
                intent.child_graph().clone(),
                intent.initial_state().clone(),
            ),
        }
        .map_err(|_| StoreError::ChildRunRejected)?;
        if restored != *intent || !self.matches(intent.key().parent()) {
            return Err(StoreError::ChildRunRejected);
        }
        Ok(())
    }

    pub(super) fn reserve(
        &self,
        account: &ChildRunBudgetAccount,
        intent: &ChildRunAdmissionIntent,
        graph: &CompiledGraph,
        at: Timestamp,
    ) -> Result<ChildRunBudgetAccount, StoreError> {
        let result = match self {
            Self::Root(_) => account.reserve(intent, graph, at),
            Self::Frame { checkpoint, .. } => {
                account.reserve_for_frame(intent, checkpoint, graph, at)
            }
        };
        result.map_err(|_| StoreError::ChildRunRejected)
    }

    pub(super) fn validate_policy(
        &self,
        root: &CompiledGraph,
        owner: &CompiledGraph,
        intent: &ChildRunAdmissionIntent,
    ) -> Result<(), StoreError> {
        if owner.reference() != *self.checkpoint().graph() {
            return Err(StoreError::ChildRunRejected);
        }
        let root = root
            .child_runs()
            .ok_or(StoreError::ChildRunRejected)?
            .limits();
        let leaf = owner
            .child_runs()
            .ok_or(StoreError::ChildRunRejected)?
            .limits();
        if leaf.maximum_descendant_depth() > root.maximum_descendant_depth()
            || leaf.maximum_active_descendants() > root.maximum_active_descendants()
            || leaf.maximum_children_per_run() > root.maximum_children_per_run()
        {
            return Err(StoreError::ChildRunRejected);
        }
        intent
            .validate_declaration(owner)
            .map_err(|_| StoreError::ChildRunRejected)
    }
}

// Fresh scope is the complete live leaf or the complete Root with no open
// stack. A namespace string or independently read checkpoint grants nothing.
pub(super) fn current<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    stored: &'a StoredRun,
    activation: &'a NodeActivation,
) -> stateknot_core::BoxFuture<'a, Result<ParentScope, StoreError>> {
    Box::pin(async move {
        let active = graph_frames::verified_active_snapshot(
            tx,
            activation.tenant_id(),
            activation.run_id(),
            stored.clone(),
        )
        .await?;
        let scope = match active {
            Some(active) if !activation.graph_namespace().is_root() => ParentScope::Frame {
                checkpoint: Box::new(active.checkpoint().clone()),
                suspended: active
                    .open_frames()
                    .iter()
                    .map(|frame| *frame.caller().attempt_id().as_uuid())
                    .collect(),
            },
            None if activation.graph_namespace().is_root() => ParentScope::Root(Box::new(
                load_locked_current_checkpoint(
                    tx,
                    stored,
                    activation.tenant_id(),
                    activation.run_id(),
                )
                .await?
                .ok_or(StoreError::StaleCheckpointHead)?,
            )),
            _ => return Err(StoreError::StaleCheckpointHead),
        };
        if !scope.matches(activation) {
            return Err(StoreError::StaleCheckpointHead);
        }
        Ok(scope)
    })
}

// ACK and accounting restoration authenticate the original scope even after
// Return/closure. They never require a historical frame to be today's leaf.
pub(super) fn historical<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    activation: &'a NodeActivation,
) -> stateknot_core::BoxFuture<'a, Result<ParentScope, StoreError>> {
    Box::pin(async move {
        let scope = if activation.graph_namespace().is_root() {
            let row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
                .bind(activation.tenant_id().as_str())
                .bind(*activation.run_id().as_uuid())
                .bind(*activation.base_checkpoint().checkpoint_id().as_uuid())
                .fetch_optional(&mut **tx)
                .await
                .map_err(|source| StoreError::database("child original Root checkpoint", source))?
                .ok_or_else(|| StoreError::corrupt("child original Root missing"))?;
            let checkpoint = decode_checkpoint(row)?;
            verify_checkpoint_anchor(tx, &checkpoint).await?;
            ParentScope::Root(Box::new(checkpoint))
        } else {
            ParentScope::Frame {
                checkpoint: Box::new(
                    Box::pin(graph_frames::bound_checkpoint(
                        tx,
                        activation.tenant_id(),
                        activation.run_id(),
                        activation.graph_namespace(),
                        activation.base_checkpoint(),
                        false,
                    ))
                    .await?,
                ),
                suspended: Vec::new(),
            }
        };
        if !scope.matches(activation) {
            return Err(StoreError::corrupt("child original scope activation"));
        }
        Ok(scope)
    })
}
