// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Complete Root prefix proofs owned by the shared component transaction.
#[allow(clippy::wildcard_imports)]
use super::*;

const MAX_PROOF_BYTES: usize = 64 * 1024 * 1024;

// No constructor outside this module. A head enters only after the complete
// predecessor and owning edge have authenticated in the same transaction.
// A clone in the application Manifest is only a bounded transcript inventory
// for equality checks after callbacks. It is never installed into another
// transaction's Replay and cannot grant that transaction owning authority.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct RootProof {
    heads: BTreeMap<(TenantId, RunId, CheckpointId), CheckpointHead>,
    bytes: usize,
}
impl RootProof {
    pub(super) fn contains(&self, head: &CheckpointHead) -> Result<bool, StoreError> {
        let saved = self.heads.get(&(
            head.tenant_id().clone(),
            head.run_id(),
            head.checkpoint_id(),
        ));
        if saved.is_some_and(|saved| saved != head) {
            return Err(StoreError::corrupt("Root prefix head substitution"));
        }
        Ok(saved.is_some())
    }
    pub(super) const fn bytes(&self) -> usize {
        self.bytes
    }
    fn remember(&mut self, head: CheckpointHead) -> Result<(), StoreError> {
        if self.contains(&head)? {
            return Ok(());
        }
        let bytes = serde_json_canonicalizer::to_vec(&head)
            .map_err(|_| StoreError::corrupt("Root prefix head encoding"))?
            .len();
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(StoreError::GraphReplayResourceLimit)?;
        if self.bytes > MAX_PROOF_BYTES {
            return Err(StoreError::GraphReplayResourceLimit);
        }
        self.heads.insert(
            (
                head.tenant_id().clone(),
                head.run_id(),
                head.checkpoint_id(),
            ),
            head,
        );
        Ok(())
    }
}

async fn checkpoint(
    tx: &mut Transaction<'_, Postgres>,
    head: &CheckpointHead,
) -> Result<Checkpoint, StoreError> {
    let checkpoint = decode_checkpoint(
        checkpoint_row(
            tx,
            head.tenant_id(),
            head.run_id(),
            &GraphNamespace::root(),
            head.checkpoint_id(),
        )
        .await?,
    )?;
    if checkpoint.head() != *head {
        return Err(StoreError::corrupt("Root prefix canonical checkpoint"));
    }
    Ok(checkpoint)
}

pub(super) async fn verify(
    replay: &mut Replay<'_, '_>,
    value: &Checkpoint,
) -> Result<(), StoreError> {
    if replay.root_proof.contains(&value.head())? {
        return Ok(());
    }
    let mut current = value.clone();
    let mut missing = Vec::new();
    let mut bytes = 0_usize;
    while !replay.root_proof.contains(&current.head())? {
        if let Some(parent) = current.parent() {
            let mut counter = CompactByteCounter::default();
            serde_json::to_writer(&mut counter, &current.head())
                .map_err(|_| StoreError::corrupt("Root prefix inventory encoding"))?;
            bytes = bytes
                .checked_add(counter.bytes)
                .ok_or(StoreError::GraphReplayResourceLimit)?;
            if bytes > MAX_PROOF_BYTES {
                return Err(StoreError::GraphReplayResourceLimit);
            }
            let base = checkpoint(replay.tx, parent).await?;
            if base.superstep().get().checked_add(1) != Some(current.superstep().get())
                || base.journal_head().sequence() >= current.journal_head().sequence()
            {
                return Err(StoreError::corrupt("Root prefix lineage"));
            }
            missing.push(current.head());
            current = base;
        } else {
            // The initial checkpoint has no historical edge to abbreviate.
            // Retain the original complete verifier even for an empty lineage.
            Box::pin(verify_checkpoint_anchor(replay.tx, &current)).await?;
            replay.root_proof.remember(current.head())?;
        }
    }
    for head in missing.into_iter().rev() {
        let successor = checkpoint(replay.tx, &head).await?;
        if successor.parent() != Some(&current.head()) {
            return Err(StoreError::corrupt("Root prefix forward coverage"));
        }
        let heads = load_locked_barrier_result_heads(replay.tx, &current.head()).await?;
        let mut results = Vec::with_capacity(heads.len());
        let mut result_bytes = 0_usize;
        for head in &heads {
            let row = load_pending_node_result_row(replay.tx, head.activation())
                .await?
                .ok_or_else(|| StoreError::corrupt("Root prefix result owner"))?;
            let result = decode_pending_node_result(&row)?;
            if result.head() != *head
                || !pending_node_result_activation_is_ready(&current, result.intent())
            {
                return Err(StoreError::corrupt("Root prefix result activation"));
            }
            let mut counter = CompactByteCounter::default();
            serde_json::to_writer(&mut counter, &result)
                .map_err(|_| StoreError::corrupt("Root prefix result encoding"))?;
            result_bytes = result_bytes
                .checked_add(counter.bytes)
                .ok_or(StoreError::GraphReplayResourceLimit)?;
            if result_bytes > replay.limits().maximum_barrier_result_bytes() {
                return Err(StoreError::GraphReplayResourceLimit);
            }
            if Box::pin(returns::recognize_result_replay(replay, &result))
                .await?
                .is_none()
            {
                Box::pin(verify_ordinary_pending_node_result_components(
                    replay.tx, &result,
                ))
                .await?;
            }
            results.push(result);
        }
        let (event, _) = child_runs::anchored_event(
            replay.tx,
            successor.tenant_id(),
            successor.run_id(),
            i64::try_from(successor.journal_head().sequence().get())
                .map_err(|_| StoreError::GraphReplayResourceLimit)?,
        )
        .await?;
        if event.head() != *successor.journal_head()
            || event.payload().kind().as_str() == GraphFrameEntryPlan::EVENT_KIND
        {
            return Err(StoreError::corrupt("Root prefix journal anchor"));
        }
        Box::pin(root_graph_barriers::authenticated_edge(
            replay.tx, &current, &successor, &results,
        ))
        .await?;
        replay.visit_edge()?;
        replay.root_proof.remember(successor.head())?;
        current = successor;
    }
    if current.head() != value.head() {
        return Err(StoreError::corrupt("Root prefix final coverage"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The fixture writer is the pre-Root34 generic barrier API. Three legacy
    // control histories must retain their operational disposition gap; the
    // ordinary Continue histories must still be replayable by the public seam.
    #[tokio::test]
    #[ignore = "requires separately generated legacy generic disposition corpus"]
    async fn complete_root_prefix_preserves_real_legacy_generic_histories() {
        let url = std::env::var("STATEKNOT_TEST_DATABASE_URL").expect("explicit corpus URL");
        let pool = sqlx_postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        let mut tx = pool.begin().await.unwrap();
        let whole: i64 = query_scalar("SELECT count(*) FROM stateknot.root_graph_barriers")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let returned: i64 = query_scalar("SELECT count(*) FROM stateknot.graph_frame_returns")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let quarantine: i64 = query_scalar("SELECT count(*) FROM stateknot.run_quarantines")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!((whole, returned, quarantine), (0, 0, 0));
        let ids = query_as::<_, (String, Uuid, Uuid)>("SELECT tenant_id,run_id,checkpoint_id FROM stateknot.run_checkpoints WHERE graph_namespace='' ORDER BY tenant_id,run_id,superstep")
            .fetch_all(&mut *tx).await.unwrap();
        // Continue: 8/16/32 edges plus three initials. Historical Wait,
        // Terminal and changed control projection: one edge and initial each.
        assert_eq!(ids.len(), 65);
        let mut replay = Replay::new(&mut tx);
        let mut initials = 0;
        let mut edges = 0;
        for (tenant, run, id) in &ids {
            let tenant = TenantId::try_from(tenant.clone()).unwrap();
            let run = RunId::from_uuid(*run).unwrap();
            let id = CheckpointId::from_uuid(*id).unwrap();
            let value = decode_checkpoint(
                checkpoint_row(replay.tx, &tenant, run, &GraphNamespace::root(), id)
                    .await
                    .unwrap(),
            )
            .unwrap();
            let original = Box::pin(verify_checkpoint_anchor(replay.tx, &value)).await;
            let candidate = Box::pin(verify(&mut replay, &value)).await;
            assert!(
                original.is_ok(),
                "original {tenant} {}: {original:?}",
                value.superstep()
            );
            assert!(
                candidate.is_ok(),
                "prefix {tenant} {}: {candidate:?}",
                value.superstep()
            );
            assert!(replay.root_proof.contains(&value.head()).unwrap());
            if value.parent().is_none() {
                initials += 1;
            } else {
                edges += 1;
            }
        }
        assert_eq!((initials, edges), (6, 59));
        assert_eq!(replay.root_proof.heads.len(), ids.len());
        assert_eq!(replay.edge_visits(), edges);
        eprintln!(
            "actual legacy generic corpus original/prefix accept 65 Root checkpoints: 6 initials, 59 edges; public Continue replay succeeds; Wait/Terminal/changed projection remain operational without quarantine"
        );
        drop(replay);
        tx.rollback().await.unwrap();
        pool.close().await;
    }

    // Run explicitly against a new task-owned corpus containing real Root34
    // Wait and returned-sibling commits. Every destructive fixture rolls back.
    #[tokio::test]
    #[ignore = "requires the independently generated Root34/returned-frame corpus"]
    #[allow(clippy::too_many_lines)] // One corpus plus independent rollback corruption probes.
    async fn complete_root_prefix_matches_original_on_real_corpus_and_corruption() {
        let url = std::env::var("STATEKNOT_TEST_DATABASE_URL").expect("explicit corpus URL");
        let pool = sqlx_postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        let mut tx = pool.begin().await.unwrap();
        let returns: i64 = query_scalar("SELECT count(*) FROM stateknot.graph_frame_returns")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let whole: i64 = query_scalar("SELECT count(*) FROM stateknot.root_graph_barriers")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert!(
            returns >= 2 && whole >= 3,
            "real returned siblings and Wait corpus required"
        );
        let ids = query_as::<_, (String, Uuid, Uuid)>("SELECT tenant_id,run_id,checkpoint_id FROM stateknot.run_checkpoints WHERE graph_namespace='' ORDER BY tenant_id,run_id,superstep").fetch_all(&mut *tx).await.unwrap();
        let mut replay = Replay::new(&mut tx);
        let mut initial = 0;
        let mut edges = 0;
        for (tenant, run, id) in &ids {
            let tenant = TenantId::try_from(tenant.clone()).unwrap();
            let run = RunId::from_uuid(*run).unwrap();
            let id = CheckpointId::from_uuid(*id).unwrap();
            let value = decode_checkpoint(
                checkpoint_row(replay.tx, &tenant, run, &GraphNamespace::root(), id)
                    .await
                    .unwrap(),
            )
            .unwrap();
            Box::pin(verify_checkpoint_anchor(replay.tx, &value))
                .await
                .unwrap();
            Box::pin(verify(&mut replay, &value)).await.unwrap();
            assert!(replay.root_proof.contains(&value.head()).unwrap());
            if value.parent().is_none() {
                initial += 1;
            } else {
                edges += 1;
            }
        }
        assert!(initial >= 2 && edges >= 3);
        assert_eq!(replay.root_proof.heads.len(), ids.len());
        // This exact corpus has two sibling terminal Frame edges in addition
        // to its three Root edges. Return ownership verifies both complete
        // terminals in this same Replay; do not label those visits as Root.
        assert_eq!(returns, 2);
        assert_eq!(
            replay.edge_visits(),
            edges + u64::try_from(returns).unwrap()
        );
        eprintln!(
            "actual Root corpus checkpoints={} initial={} Root-edges={} Root-plus-return-terminal-visits={} returns={} whole={}",
            ids.len(),
            initial,
            edges,
            replay.edge_visits(),
            returns,
            whole
        );
        drop(replay);
        tx.rollback().await.unwrap();

        let target = query_as::<_, (String, Uuid, Uuid)>("SELECT b.tenant_id,b.run_id,b.successor_checkpoint_id FROM stateknot.root_graph_barriers b WHERE EXISTS(SELECT 1 FROM stateknot.graph_frame_returns r WHERE r.tenant_id=b.tenant_id AND r.run_id=b.run_id) ORDER BY b.successor_superstep DESC LIMIT 1").fetch_one(&pool).await.unwrap();
        for (table, mutation) in [
            (
                "agent_admissions",
                "UPDATE stateknot.agent_admissions SET admission_bytes=set_byte(admission_bytes,0,32) WHERE tenant_id=$1 AND run_id=$2",
            ),
            (
                "root_graph_barriers",
                "UPDATE stateknot.root_graph_barriers SET intent_digest=decode(repeat('00',32),'hex') WHERE tenant_id=$1 AND run_id=$2",
            ),
            (
                "graph_frame_returns",
                "UPDATE stateknot.graph_frame_returns SET caller_start_digest=decode(repeat('00',32),'hex') WHERE tenant_id=$1 AND run_id=$2",
            ),
            (
                "run_checkpoints",
                "UPDATE stateknot.run_checkpoints SET checkpoint_bytes=set_byte(checkpoint_bytes,0,32) WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace='' AND superstep=0",
            ),
        ] {
            let mut tx = pool.begin().await.unwrap();
            let tenant = TenantId::try_from(target.0.clone()).unwrap();
            let run = RunId::from_uuid(target.1).unwrap();
            let value = decode_checkpoint(
                checkpoint_row(
                    &mut tx,
                    &tenant,
                    run,
                    &GraphNamespace::root(),
                    CheckpointId::from_uuid(target.2).unwrap(),
                )
                .await
                .unwrap(),
            )
            .unwrap();
            query(&format!(
                "ALTER TABLE stateknot.{table} DISABLE TRIGGER USER"
            ))
            .execute(&mut *tx)
            .await
            .unwrap();
            assert!(
                query(mutation)
                    .bind(&target.0)
                    .bind(target.1)
                    .execute(&mut *tx)
                    .await
                    .unwrap()
                    .rows_affected()
                    > 0
            );
            let original = Box::pin(verify_checkpoint_anchor(&mut tx, &value)).await;
            let mut replay = Replay::new(&mut tx);
            let cached = Box::pin(verify(&mut replay, &value)).await;
            assert!(
                matches!(original, Err(StoreError::CorruptData { .. })),
                "original {table}: {original:?}"
            );
            assert!(
                matches!(cached, Err(StoreError::CorruptData { .. })),
                "prefix {table}: {cached:?}"
            );
            eprintln!("actual original and complete prefix both reject {table} corruption");
            drop(replay);
            tx.rollback().await.unwrap();
        }
        pool.close().await;
    }
}
