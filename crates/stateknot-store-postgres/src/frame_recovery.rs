// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Claimed leaf planning shares bounded history with Root recovery.
#[allow(clippy::wildcard_imports)]
use super::*;
use stateknot_core::{GraphFrameCheckpoint, GraphFrameCheckpointHead};

impl ClaimedRunRecovery<'_> {
    /// Plans the exact active graph frame under this claimed journal observation.
    ///
    /// Whole stack/admission/caller/head proofs, bounded result pages and complete
    /// physical histories are verified before a final database-time fence check.
    /// Completed results are reused, and a terminal leaf may have no ready nodes.
    /// The plan grants no dispatch; use the dedicated scoped durable start.
    /// Application schema/reducer replay remains a separate pre-dispatch check.
    ///
    /// # Errors
    /// Rejects crossed sessions, changed leaf/head/journal, expired or superseded
    /// fences, unavailable reads and bounded replay errors. Durable contradictions
    /// are quarantined through this session's original fenced context.
    pub async fn plan_graph_frame_ready_nodes(
        &self,
        expected: &GraphFrameCheckpointHead,
    ) -> Result<ReadyNodeRecoveryPlan, StoreError> {
        self.validate_checkpoint_scope(expected.checkpoint())?;
        Box::pin(self.read(self.plan_graph_frame_ready_nodes_inner(expected))).await
    }

    async fn plan_graph_frame_ready_nodes_inner(
        &self,
        expected: &GraphFrameCheckpointHead,
    ) -> Result<ReadyNodeRecoveryPlan, StoreError> {
        let checkpoint = Box::pin(self.load_claimed_frame_checkpoint(expected)).await?;
        self.load_graph_for_checkpoint(checkpoint.checkpoint())
            .await?;
        let mut planner = ReadyNodeRecoveryPlanner::for_frame(checkpoint, self.fence.clone())
            .map_err(|_| StoreError::corrupt("frame recovery activation"))?;
        Box::pin(self.observe_ready_node_history(
            &mut planner,
            expected.checkpoint(),
            Some(expected),
        ))
        .await?;
        Box::pin(self.load_claimed_frame_checkpoint(expected)).await?;
        let observation = self
            .store
            .load_claimed_run_recovery_snapshot(&self.fence, &self.context)
            .await?;
        if observation.run.checkpoint() != self.initial_run.checkpoint() {
            return Err(StoreError::corrupt("frame recovery Root projection"));
        }
        let journal = self
            .context
            .expectation()
            .head()
            .cloned()
            .ok_or_else(|| StoreError::corrupt("frame recovery journal observation"))?;
        planner
            .finish(journal, observation.observed_at)
            .map_err(|_| StoreError::corrupt("frame recovery plan"))
    }

    async fn load_claimed_frame_checkpoint(
        &self,
        expected: &GraphFrameCheckpointHead,
    ) -> Result<GraphFrameCheckpoint, StoreError> {
        let active = Box::pin(
            self.store
                .load_active_graph_frame(self.fence.tenant_id(), self.fence.run_id()),
        )
        .await?
        .ok_or(StoreError::StaleCheckpointHead)?;
        if active.run().journal_head() != self.context.expectation().head() {
            return Err(StoreError::StaleClaimedRunRecoveryObservation);
        }
        if active.checkpoint().head() != *expected {
            return Err(StoreError::StaleCheckpointHead);
        }
        if active.run().checkpoint() != self.initial_run.checkpoint() {
            return Err(StoreError::corrupt("frame recovery Root projection"));
        }
        Ok(active.checkpoint().clone())
    }

    pub(super) async fn observe_ready_node_history(
        &self,
        planner: &mut ReadyNodeRecoveryPlanner,
        base: &CheckpointHead,
        frame: Option<&GraphFrameCheckpointHead>,
    ) -> Result<(), StoreError> {
        let result_page_size = PendingNodeResultPageSize::new(PendingNodeResultPageSize::MAX)?;
        let mut result_cursor = None;
        loop {
            let page = if let Some(frame) = frame {
                Box::pin(self.store.load_graph_frame_pending_node_result_page(
                    frame,
                    result_cursor.as_ref(),
                    result_page_size,
                ))
                .await?
            } else {
                self.store
                    .load_unconsumed_pending_node_result_page(
                        base,
                        result_cursor.as_ref(),
                        result_page_size,
                    )
                    .await?
            };
            if self.context.expectation().head() != Some(page.snapshot_journal_head()) {
                return Err(StoreError::StaleClaimedRunRecoveryObservation);
            }
            for result in page.records() {
                planner
                    .observe_result(result)
                    .map_err(|_| StoreError::corrupt("ready node recovery result set"))?;
            }
            if !page.has_more() {
                break;
            }
            result_cursor = Some(
                page.next_cursor()
                    .ok_or_else(|| StoreError::corrupt("ready node recovery result cursor"))?,
            );
        }

        let attempt_page_size = NodeAttemptHistoryPageSize::new(NodeAttemptHistoryPageSize::MAX)?;
        for activation in planner.activations() {
            let mut attempt_cursor = None;
            loop {
                let page = self
                    .store
                    .load_node_attempt_history_page(
                        &activation,
                        attempt_cursor.as_ref(),
                        attempt_page_size,
                    )
                    .await?;
                for attempt in page.records() {
                    planner
                        .observe_attempt(attempt)
                        .map_err(|_| StoreError::corrupt("ready node recovery attempt history"))?;
                }
                if !page.has_more() {
                    break;
                }
                attempt_cursor =
                    Some(page.next_cursor().ok_or_else(|| {
                        StoreError::corrupt("ready node recovery attempt cursor")
                    })?);
            }
        }

        Ok(())
    }
}

impl PostgresStore {
    /// Loads one bounded page of unconsumed results for an exact active leaf.
    ///
    /// The frame, full ancestor stack and current head are authenticated in the
    /// same repeatable-read snapshot as every returned result and invocation
    /// binding. Cursor scope and the snapshot journal remain exact. Root reads
    /// retain their separate interface; this read grants no execution authority.
    ///
    /// # Errors
    /// Rejects a changed/inactive frame, crossed/stale cursor, corrupt durable
    /// facts, replay bounds or database failures.
    pub async fn load_graph_frame_pending_node_result_page(
        &self,
        frame: &GraphFrameCheckpointHead,
        cursor: Option<&PendingNodeResultPageCursor>,
        page_size: PendingNodeResultPageSize,
    ) -> Result<PendingNodeResultPage, StoreError> {
        Box::pin(self.load_pending_node_result_page_inner(
            frame.checkpoint(),
            Some(frame),
            cursor,
            page_size,
        ))
        .await
    }
}

// The caller has already authenticated the exact Root/leaf and cursor scope.
// Query only compact heads with one look-ahead row; full decoding stays bounded.
pub(super) async fn load_pending_node_result_head_page(
    transaction: &mut Transaction<'_, Postgres>,
    base: &CheckpointHead,
    namespace: &GraphNamespace,
    cursor: Option<&PendingNodeResultPageCursor>,
    page_size: PendingNodeResultPageSize,
) -> Result<Vec<PendingNodeResultHeadRow>, StoreError> {
    let tenant_id = base.tenant_id();
    let run_id = base.run_id();
    // Checkpoint IDs are unique within a tenant/Run, including sibling frames.
    // Filtering for the expected namespace must not hide a corrupt projection.
    // EXISTS examines only compact identity columns under that exact key.
    let crossed_namespace = query_scalar::<_, bool>(
        r"SELECT EXISTS (
            SELECT 1 FROM stateknot.pending_node_results
            WHERE tenant_id=$1 AND run_id=$2 AND base_checkpoint_id=$3
              AND graph_namespace <> $4
        )",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*base.checkpoint_id().as_uuid())
    .bind(namespace.as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("pending result namespace proof", source))?;
    if crossed_namespace {
        return Err(StoreError::corrupt("pending result checkpoint namespace"));
    }
    let base_superstep = i64::try_from(base.superstep().get())
        .map_err(|_| StoreError::InvalidPendingNodeResultCursor)?;
    let query_limit = i64::from(page_size.get()) + 1;
    let rows = if let Some(cursor) = cursor {
        query_as::<_, PendingNodeResultHeadRow>(SELECT_UNCONSUMED_PENDING_NODE_RESULT_HEADS_AFTER)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*base.checkpoint_id().as_uuid())
            .bind(base_superstep)
            .bind(base.digest().as_bytes())
            .bind(namespace.as_str())
            .bind(cursor.after().activation().node_id().as_str())
            .bind(query_limit)
            .fetch_all(&mut **transaction)
            .await
            .map_err(|source| {
                StoreError::database("unconsumed pending result continuation", source)
            })?
    } else {
        query_as::<_, PendingNodeResultHeadRow>(SELECT_UNCONSUMED_PENDING_NODE_RESULT_HEADS)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*base.checkpoint_id().as_uuid())
            .bind(base_superstep)
            .bind(base.digest().as_bytes())
            .bind(namespace.as_str())
            .bind(query_limit)
            .fetch_all(&mut **transaction)
            .await
            .map_err(|source| {
                StoreError::database("unconsumed pending result first page", source)
            })?
    };
    Ok(rows)
}
