// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Delayed gates release ownership; only a subsequent durable Start grants work.
#[allow(clippy::wildcard_imports)]
use super::*;
use stateknot_core::GraphFrameCheckpoint;

impl PostgresStore {
    /// Releases the live worker lease until the exact active leaf's earliest retry.
    ///
    /// The Run lock protects whole active-frame proof and a fresh bounded replay
    /// of actual results and physical histories using the original retry policy.
    /// Root projection, journal, queue age and shared accounting remain unchanged.
    /// `Due` keeps the lease for replanning. `Idempotent` observes the same epoch,
    /// current head, journal and gate without granting launch or proving the
    /// original worker attempt or complete original plan. No intent is persisted.
    /// Only the dedicated scoped durable Start may authorize a later retry.
    ///
    /// # Errors
    /// Rejects Root/crossed/actionable/fabricated plans, stale scope or journal,
    /// unavailable or expired ownership, clock regression and durable corruption.
    /// Use the original claimed corruption context to quarantine only corruption.
    pub async fn schedule_graph_frame_delayed_retry_wakeup(
        &self,
        plan: &ReadyNodeRecoveryPlan,
    ) -> Result<DelayedRetryScheduleOutcome, StoreError> {
        if plan.frame().is_none_or(|frame| frame.namespace().is_root()) {
            return Err(StoreError::InvalidDelayedRetryPlan);
        }
        Box::pin(schedule(self, plan)).await
    }
}

struct RetryScope {
    root: Checkpoint,
    frame: Option<GraphFrameCheckpoint>,
    admission_digest: Option<Digest>,
}

fn validate_plan(plan: &ReadyNodeRecoveryPlan) -> Result<Timestamp, StoreError> {
    let not_before = plan
        .earliest_deferred_at()
        .ok_or(StoreError::InvalidDelayedRetryPlan)?;
    if plan.nodes().is_empty()
        || plan.nodes().iter().any(|node| {
            !matches!(
                node.kind(),
                RecoveryNodeKind::Completed | RecoveryNodeKind::Deferred
            )
        })
        || not_before <= plan.observed_at()
        || plan.checkpoint().tenant_id() != plan.fence().tenant_id()
        || plan.checkpoint().run_id() != plan.fence().run_id()
        || plan.journal_head().tenant_id() != plan.fence().tenant_id()
        || plan.journal_head().run_id() != plan.fence().run_id()
    {
        return Err(StoreError::InvalidDelayedRetryPlan);
    }
    Ok(not_before)
}

// Erase the complete scheduler proof at this private module boundary.
// Box::pin without a concrete Send type still exposes deep inferred futures.
pub(super) fn schedule<'a>(
    store: &'a PostgresStore,
    plan: &'a ReadyNodeRecoveryPlan,
) -> stateknot_core::BoxFuture<'a, Result<DelayedRetryScheduleOutcome, StoreError>> {
    Box::pin(async move {
        let not_before = validate_plan(plan)?;
        let fence = plan.fence();
        let mut tx = store.begin_mutation("delayed retry wakeup").await?;
        let stored =
            decode_run(fetch_locked_run_row(&mut tx, fence.tenant_id(), fence.run_id()).await?)?;
        validate_runnable(&stored)?;
        if stored.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::RunNotRunnable);
        }
        if stored.journal_head() != Some(plan.journal_head()) {
            return Err(StoreError::StaleJournalHead);
        }
        let scope = Box::pin(verify_scope(&mut tx, &stored, plan)).await?;
        // Flush deferred guards while ownership remains live, before the final clock.
        query("SET CONSTRAINTS ALL IMMEDIATE")
            .execute(&mut *tx)
            .await
            .map_err(|e| StoreError::database("retry component guards", e))?;
        let now = database_now(&mut tx, "delayed retry wakeup clock").await?;
        if now < plan.observed_at() {
            return Err(StoreError::DatabaseClockRegression);
        }
        let outcome = if stored.lease().is_none()
            && stored.last_fencing_epoch() == Some(fence.epoch())
            && stored.scheduler_not_before() == Some(not_before)
        {
            DelayedRetryScheduleOutcome::Idempotent { not_before }
        } else {
            let now = Box::pin(authorize_fresh_plan(&mut tx, &stored, plan, &scope, now)).await?;
            if now >= not_before {
                DelayedRetryScheduleOutcome::Due { not_before }
            } else {
                Box::pin(write_gate(&mut tx, &stored, plan, &scope, not_before, now)).await?
            }
        };
        tx.commit()
            .await
            .map_err(|e| StoreError::database("delayed retry wakeup commit", e))?;
        Ok(outcome)
    })
}

// Scope proof above is unconditional, including for a lost ACK: actual Root,
// whole active stack, exact leaf checkpoint, namespace and exact journal. An
// ACK observes a released gate, so it cannot prove an erased worker attempt or
// original complete plan. Only fresh scheduling enters physical-history replay.
async fn authorize_fresh_plan(
    tx: &mut Transaction<'_, Postgres>,
    stored: &StoredRun,
    plan: &ReadyNodeRecoveryPlan,
    scope: &RetryScope,
    previous_now: Timestamp,
) -> Result<Timestamp, StoreError> {
    // Authenticate the real worker before using caller fence in the planner.
    // A fabricated same-epoch attempt is operational StaleFence, not corruption.
    authorize_worker(stored, plan.fence(), previous_now)?;
    Box::pin(rederive_plan(tx, scope, plan)).await?;
    let now = database_now(tx, "verified retry plan clock").await?;
    if now < previous_now {
        return Err(StoreError::DatabaseClockRegression);
    }
    authorize_worker(stored, plan.fence(), now)?;
    Ok(now)
}

// A Run update trigger can consume time after the CTE clock observation.
// Keep the original ownership under the Run lock and undo only this gate write
// when the inclusive retry boundary arrives before final validation.
async fn write_gate(
    tx: &mut Transaction<'_, Postgres>,
    stored: &StoredRun,
    plan: &ReadyNodeRecoveryPlan,
    scope: &RetryScope,
    not_before: Timestamp,
    previous_now: Timestamp,
) -> Result<DelayedRetryScheduleOutcome, StoreError> {
    query("SAVEPOINT retry_gate_write")
        .execute(&mut **tx)
        .await
        .map_err(|e| StoreError::database("retry gate savepoint", e))?;
    let updated = update_gate(tx, plan, scope, not_before).await?;
    if updated {
        verify_released_projection(tx, stored, plan, scope, not_before).await?;
    }
    let final_now = database_now(tx, "delayed retry wakeup final clock").await?;
    if final_now < previous_now {
        return Err(StoreError::DatabaseClockRegression);
    }
    // The lease was authenticated before release, and the Run lock excludes
    // claims or other protocol writes. Check its original exclusive expiry.
    authorize_worker(stored, plan.fence(), final_now)?;
    if final_now >= not_before {
        query("ROLLBACK TO SAVEPOINT retry_gate_write")
            .execute(&mut **tx)
            .await
            .map_err(|e| StoreError::database("due retry gate rollback", e))?;
        Box::pin(verify_retained_projection(tx, stored, plan, scope)).await?;
        let retained_now = database_now(tx, "retained retry ownership clock").await?;
        if retained_now < final_now {
            return Err(StoreError::DatabaseClockRegression);
        }
        authorize_worker(stored, plan.fence(), retained_now)?;
        return Ok(DelayedRetryScheduleOutcome::Due { not_before });
    }
    if !updated {
        return Err(StoreError::corrupt("delayed retry wakeup row count"));
    }
    Ok(DelayedRetryScheduleOutcome::Scheduled { not_before })
}

async fn verify_retained_projection(
    tx: &mut Transaction<'_, Postgres>,
    stored: &StoredRun,
    plan: &ReadyNodeRecoveryPlan,
    scope: &RetryScope,
) -> Result<(), StoreError> {
    let retained = decode_run(
        fetch_locked_run_row(tx, plan.fence().tenant_id(), plan.fence().run_id()).await?,
    )?;
    if retained.lease() != stored.lease()
        || retained.last_fencing_epoch() != stored.last_fencing_epoch()
        || retained.scheduler_ready_at() != stored.scheduler_ready_at()
        || retained.scheduler_not_before() != stored.scheduler_not_before()
        || retained.checkpoint() != stored.checkpoint()
        || retained.journal_head() != stored.journal_head()
        || retained.lifecycle().status() != stored.lifecycle().status()
        || retained.lifecycle().revision() != stored.lifecycle().revision()
        || retained.is_quarantined()
    {
        return Err(StoreError::corrupt("retained retry ownership projection"));
    }
    let current = Box::pin(verify_scope(tx, &retained, plan)).await?;
    if current.root != scope.root
        || current.frame != scope.frame
        || current.admission_digest != scope.admission_digest
    {
        return Err(StoreError::StaleCheckpointHead);
    }
    Ok(())
}

async fn verify_released_projection(
    tx: &mut Transaction<'_, Postgres>,
    stored: &StoredRun,
    plan: &ReadyNodeRecoveryPlan,
    scope: &RetryScope,
    not_before: Timestamp,
) -> Result<(), StoreError> {
    let released = decode_run(
        fetch_locked_run_row(tx, plan.fence().tenant_id(), plan.fence().run_id()).await?,
    )?;
    if released.lease().is_some()
        || released.last_fencing_epoch() != Some(plan.fence().epoch())
        || released.scheduler_not_before() != Some(not_before)
        || released.scheduler_ready_at() != stored.scheduler_ready_at()
        || released.checkpoint() != stored.checkpoint()
        || released.journal_head() != stored.journal_head()
        || released.lifecycle().status() != stored.lifecycle().status()
        || released.lifecycle().revision() != stored.lifecycle().revision()
        || released.is_quarantined()
    {
        return Err(StoreError::corrupt("released retry gate projection"));
    }
    if let Some(frame) = &scope.frame {
        let matches = query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM stateknot.graph_frame_stacks s JOIN stateknot.graph_frame_heads h ON h.tenant_id=s.tenant_id AND h.run_id=s.run_id AND h.graph_namespace=s.active_namespace WHERE s.tenant_id=$1 AND s.run_id=$2 AND s.active_namespace=$3 AND s.active_frame_identity_digest=$4 AND s.admission_digest=$5 AND h.frame_identity_digest=$4 AND h.checkpoint_id=$6 AND h.superstep=$7 AND h.checkpoint_digest=$8 AND h.frame_checkpoint_digest=$9)"
        ).bind(plan.fence().tenant_id().as_str()).bind(*plan.fence().run_id().as_uuid())
            .bind(frame.frame().namespace().as_str()).bind(frame.frame().digest().as_bytes())
            .bind(scope.admission_digest.map(|d| d.as_bytes().to_vec()))
            .bind(*frame.checkpoint().checkpoint_id().as_uuid())
            .bind(i64::try_from(frame.checkpoint().superstep().get()).map_err(|_|StoreError::StaleCheckpointHead)?)
            .bind(frame.checkpoint().digest().as_bytes()).bind(frame.digest().as_bytes())
            .fetch_one(&mut **tx).await.map_err(|e|StoreError::database("released retry frame projection", e))?;
        if !matches {
            return Err(StoreError::StaleCheckpointHead);
        }
    }
    Ok(())
}

async fn verify_scope(
    tx: &mut Transaction<'_, Postgres>,
    stored: &StoredRun,
    plan: &ReadyNodeRecoveryPlan,
) -> Result<RetryScope, StoreError> {
    let fence = plan.fence();
    let root = load_locked_current_checkpoint(tx, stored, fence.tenant_id(), fence.run_id())
        .await?
        .ok_or(StoreError::ReadyNodeRecoveryCheckpointMissing)?;
    let active = Box::pin(graph_frames::verified_active_snapshot(
        tx,
        fence.tenant_id(),
        fence.run_id(),
        stored.clone(),
    ))
    .await?;
    let Some(frame) = plan.frame() else {
        if active.is_some() {
            return Err(StoreError::GraphFrameCompoundRequired);
        }
        if root != *plan.checkpoint() {
            return Err(StoreError::StaleCheckpointHead);
        }
        return Ok(RetryScope {
            root,
            frame: None,
            admission_digest: None,
        });
    };
    let active = active.ok_or(StoreError::StaleCheckpointHead)?;
    let checkpoint = active.checkpoint();
    if checkpoint.frame() != frame || checkpoint.checkpoint() != plan.checkpoint() {
        return Err(StoreError::StaleCheckpointHead);
    }
    let admission = query_scalar::<_, Vec<u8>>("SELECT admission_digest FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&mut **tx).await
        .map_err(|e| StoreError::database("retry admission binding", e))?;
    Ok(RetryScope {
        root,
        frame: Some(checkpoint.clone()),
        admission_digest: Some(decode_digest(&admission, "retry admission binding")?),
    })
}

// Keep complete history replay behind a concrete Send future so Runtime
// does not recursively expand every owning proof through its scheduler.
fn rederive_plan<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    scope: &'a RetryScope,
    expected: &'a ReadyNodeRecoveryPlan,
) -> stateknot_core::BoxFuture<'a, Result<(), StoreError>> {
    Box::pin(async move {
        let mut planner = match &scope.frame {
            Some(frame) => {
                ReadyNodeRecoveryPlanner::for_frame(frame.clone(), expected.fence().clone())
            }
            None => ReadyNodeRecoveryPlanner::new(scope.root.clone(), expected.fence().clone()),
        }
        .map_err(|_| StoreError::InvalidDelayedRetryPlan)?;
        let checkpoint = scope
            .frame
            .as_ref()
            .map_or(&scope.root, GraphFrameCheckpoint::checkpoint);
        let namespace = scope
            .frame
            .as_ref()
            .map_or_else(GraphNamespace::root, |frame| {
                frame.frame().namespace().clone()
            });
        let activations = planner.activations();
        verify_inventory(tx, checkpoint, &namespace, &activations).await?;
        for activation in activations {
            if let Some(row) = load_pending_node_result_row(tx, &activation).await? {
                let result = decode_pending_node_result(&row)?;
                if result.intent().activation() != &activation {
                    return Err(StoreError::corrupt("retry result activation"));
                }
                Box::pin(verify_pending_node_result_components(tx, &result)).await?;
                planner
                    .observe_result(&result)
                    .map_err(|_| StoreError::corrupt("retry result history"))?;
            }
            Box::pin(observe_attempts(tx, &activation, &mut planner)).await?;
        }
        let actual = planner
            .finish(expected.journal_head().clone(), expected.observed_at())
            .map_err(|_| StoreError::corrupt("retry physical history plan"))?;
        if actual.nodes().len() != expected.nodes().len()
            || actual
                .nodes()
                .iter()
                .zip(expected.nodes())
                .any(|(actual, expected)| {
                    actual.activation() != expected.activation()
                        || actual.kind() != expected.kind()
                        || actual.result() != expected.result()
                        || actual.not_before() != expected.not_before()
                })
            || actual.earliest_deferred_at() != expected.earliest_deferred_at()
        {
            return Err(StoreError::InvalidDelayedRetryPlan);
        }
        Ok(())
    })
}

// Compact predicates prevent filtering away contradictory projected history.
async fn verify_inventory(
    tx: &mut Transaction<'_, Postgres>,
    checkpoint: &Checkpoint,
    namespace: &GraphNamespace,
    activations: &[NodeActivation],
) -> Result<(), StoreError> {
    let base = checkpoint.head();
    let nodes: Vec<_> = activations
        .iter()
        .map(|a| a.node_id().as_str().to_owned())
        .collect();
    for table in ["node_attempts", "pending_node_results"] {
        let invalid = query_scalar::<_, bool>(&format!(
            "SELECT EXISTS(SELECT 1 FROM stateknot.{table} WHERE tenant_id=$1 AND run_id=$2 AND base_checkpoint_id=$3 AND (graph_namespace<>$4 OR base_superstep<>$5 OR base_checkpoint_digest<>$6 OR NOT (node_id=ANY($7))))"
        )).bind(base.tenant_id().as_str()).bind(*base.run_id().as_uuid())
            .bind(*base.checkpoint_id().as_uuid()).bind(namespace.as_str())
            .bind(i64::try_from(base.superstep().get()).map_err(|_|StoreError::StaleCheckpointHead)?)
            .bind(base.digest().as_bytes()).bind(&nodes)
            .fetch_one(&mut **tx).await.map_err(|e|StoreError::database("retry compact inventory", e))?;
        if invalid {
            return Err(StoreError::corrupt("retry projected scope"));
        }
        for activation in activations {
            let invalid = query_scalar::<_, bool>(&format!(
                "SELECT EXISTS(SELECT 1 FROM stateknot.{table} WHERE tenant_id=$1 AND run_id=$2 AND base_checkpoint_id=$3 AND node_id=$4 AND activation_input_digest<>$5)"
            )).bind(base.tenant_id().as_str()).bind(*base.run_id().as_uuid())
                .bind(*base.checkpoint_id().as_uuid()).bind(activation.node_id().as_str())
                .bind(activation.input_digest().as_bytes()).fetch_one(&mut **tx).await
                .map_err(|e|StoreError::database("retry activation inventory", e))?;
            if invalid {
                return Err(StoreError::corrupt("retry projected activation"));
            }
        }
    }
    Ok(())
}

async fn observe_attempts(
    tx: &mut Transaction<'_, Postgres>,
    activation: &NodeActivation,
    planner: &mut ReadyNodeRecoveryPlanner,
) -> Result<(), StoreError> {
    let count = count_node_attempts(tx, activation).await?;
    if count > ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE {
        return Err(StoreError::corrupt("retry physical history bound"));
    }
    let base = activation.base_checkpoint();
    let rows = query_as::<_, NodeAttemptStartRow>(SELECT_NODE_ATTEMPT_HISTORY)
        .bind(activation.tenant_id().as_str())
        .bind(*activation.run_id().as_uuid())
        .bind(*base.checkpoint_id().as_uuid())
        .bind(i64::try_from(base.superstep().get()).map_err(|_| StoreError::StaleCheckpointHead)?)
        .bind(base.digest().as_bytes())
        .bind(activation.graph_namespace().as_str())
        .bind(activation.node_id().as_str())
        .bind(activation.input_digest().as_bytes())
        .bind(0_i64)
        .bind(
            i64::try_from(ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE)
                .map_err(|_| StoreError::InvalidDelayedRetryPlan)?,
        )
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| StoreError::database("retry physical histories", e))?;
    if rows.len() != count {
        return Err(StoreError::corrupt("retry history row count"));
    }
    for row in rows {
        let attempt = load_node_attempt_from_start_row(tx, row).await?;
        if attempt.start().activation() != activation {
            return Err(StoreError::corrupt("retry history activation"));
        }
        // The whole current Root/frame and this canonical activation were proved
        // once above. Retain every anchor without replaying the checkpoint prefix.
        Box::pin(verify_attempt(tx, activation, &attempt)).await?;
        planner
            .observe_attempt(&attempt)
            .map_err(|_| StoreError::corrupt("retry physical attempt chain"))?;
    }
    Ok(())
}

// The current Root/full active proof authenticates this immutable base once in
// the same locked transaction. Framework completion retains whole return proof.
async fn verify_attempt(
    tx: &mut Transaction<'_, Postgres>,
    activation: &NodeActivation,
    attempt: &NodeAttempt,
) -> Result<(), StoreError> {
    let start = attempt.start();
    if start.activation() != activation {
        return Err(StoreError::corrupt("retry canonical attempt base"));
    }
    if Box::pin(graph_frames::recognize_start(tx, start))
        .await?
        .is_some()
    {
        Box::pin(verify_node_attempt(tx, attempt)).await?;
        return Ok(());
    }
    verify_node_attempt_anchor(
        tx,
        start.journal_head(),
        start.fence(),
        start.digest(),
        "retry attempt start anchor",
    )
    .await?;
    let Some(completion) = attempt.completion() else {
        return Ok(());
    };
    verify_node_attempt_anchor(
        tx,
        completion.journal_head(),
        start.fence(),
        completion.digest(),
        "retry attempt completion anchor",
    )
    .await?;
    if let NodeAttemptOutcome::Succeeded { result } = completion.outcome() {
        let row = load_pending_node_result_row(tx, result.activation())
            .await?
            .ok_or_else(|| StoreError::corrupt("retry successful result"))?;
        let durable = decode_pending_node_result(&row)?;
        if row.node_attempt_id != Some(*start.attempt_id().as_uuid())
            || durable.head() != **result
            || durable.intent().activation() != start.activation()
        {
            return Err(StoreError::corrupt("retry successful result owner"));
        }
        Box::pin(verify_pending_node_result_components(tx, &durable)).await?;
    }
    Ok(())
}

const UPDATE_GATE: &str = r"
WITH observation AS MATERIALIZED (SELECT clock_timestamp() AS observed_at)
UPDATE stateknot.runs r
SET lease_attempt_id=NULL, lease_acquired_at=NULL, lease_renewed_at=NULL,
    lease_expires_at=NULL, scheduler_not_before=$5, updated_at=observation.observed_at
FROM observation
WHERE r.tenant_id=$1 AND r.run_id=$2 AND r.lease_attempt_id=$3 AND r.fencing_epoch=$4
  AND r.lease_expires_at>observation.observed_at AND observation.observed_at<$5
  AND r.journal_sequence=$6 AND r.journal_event_id=$7 AND r.journal_recorded_at=$8 AND r.journal_digest=$9
  AND r.checkpoint_id=$10 AND r.checkpoint_superstep=$11 AND r.checkpoint_digest=$12
  AND r.lifecycle_status='active' AND r.quarantined_at IS NULL
  AND ($13::text IS NULL OR EXISTS (
    SELECT 1 FROM stateknot.graph_frame_stacks s JOIN stateknot.graph_frame_heads h
      ON h.tenant_id=s.tenant_id AND h.run_id=s.run_id AND h.graph_namespace=s.active_namespace
    WHERE s.tenant_id=r.tenant_id AND s.run_id=r.run_id AND s.active_namespace=$13
      AND s.active_frame_identity_digest=$14 AND s.admission_digest=$15
      AND h.frame_identity_digest=$14 AND h.checkpoint_id=$16 AND h.superstep=$17
      AND h.checkpoint_digest=$18 AND h.frame_checkpoint_digest=$19))
RETURNING observation.observed_at
";

async fn update_gate(
    tx: &mut Transaction<'_, Postgres>,
    plan: &ReadyNodeRecoveryPlan,
    scope: &RetryScope,
    not_before: Timestamp,
) -> Result<bool, StoreError> {
    let fence = plan.fence();
    let frame = scope.frame.as_ref();
    let root = scope.root.head();
    let updated = query_scalar::<_, DateTime<Utc>>(UPDATE_GATE)
        .bind(fence.tenant_id().as_str())
        .bind(*fence.run_id().as_uuid())
        .bind(*fence.attempt_id().as_uuid())
        .bind(i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?)
        .bind(to_database_time(not_before)?)
        .bind(
            i64::try_from(plan.journal_head().sequence().get())
                .map_err(|_| StoreError::JournalSequenceExhausted)?,
        )
        .bind(*plan.journal_head().event_id().as_uuid())
        .bind(to_database_time(plan.journal_head().recorded_at())?)
        .bind(plan.journal_head().digest().as_bytes())
        .bind(*root.checkpoint_id().as_uuid())
        .bind(i64::try_from(root.superstep().get()).map_err(|_| StoreError::StaleCheckpointHead)?)
        .bind(root.digest().as_bytes())
        .bind(frame.map(|f| f.frame().namespace().as_str()))
        .bind(frame.map(|f| f.frame().digest().as_bytes().to_vec()))
        .bind(scope.admission_digest.map(|d| d.as_bytes().to_vec()))
        .bind(frame.map(|f| *f.checkpoint().checkpoint_id().as_uuid()))
        .bind(
            frame
                .map(|f| i64::try_from(f.checkpoint().superstep().get()))
                .transpose()
                .map_err(|_| StoreError::StaleCheckpointHead)?,
        )
        .bind(frame.map(|f| f.checkpoint().digest().as_bytes().to_vec()))
        .bind(frame.map(|f| f.digest().as_bytes().to_vec()))
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| StoreError::database("delayed retry wakeup update", e))?;
    Ok(updated.is_some())
}
