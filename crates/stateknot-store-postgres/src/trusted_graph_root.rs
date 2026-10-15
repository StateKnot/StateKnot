// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Positive original creation proof for the trusted low-level graph contract.
#[allow(clippy::wildcard_imports)]
use super::*;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Source {
    TrustedGraph { creation: Box<Creation> },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Creation {
    provenance: AgentResultProvenance,
    admitted_at: Timestamp,
    first_journal: JournalHead,
    start_journal: JournalHead,
    started_at: Timestamp,
    start_projection_digest: Digest,
    initial_checkpoint: CheckpointHead,
    initial_journal: JournalHead,
}

pub(super) enum Origin {
    Agent(Box<StoredAgentAdmission>),
    TrustedGraph(Box<Source>),
}

// Every query is constrained by the complete owner, with an existing owner
// prefix index. Missing schema/privilege failures remain database errors.
async fn agent_witness(
    tx: &mut Transaction<'_, Postgres>,
    base: &CheckpointHead,
    first: &JournalEvent,
) -> Result<bool, StoreError> {
    if first.payload().kind().as_str() == AgentAdmission::JOURNAL_EVENT_KIND {
        return Ok(true);
    }
    query_scalar("SELECT (SELECT agent_deadline_at IS NOT NULL FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2) OR EXISTS (SELECT 1 FROM stateknot.agent_admissions WHERE tenant_id=$1 AND run_id=$2) OR EXISTS (SELECT 1 FROM stateknot.agent_submission_keys WHERE tenant_id=$1 AND run_id=$2) OR EXISTS (SELECT 1 FROM stateknot.graph_frame_entries WHERE tenant_id=$1 AND run_id=$2) OR EXISTS (SELECT 1 FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2) OR EXISTS (SELECT 1 FROM stateknot.child_run_budget_accounts WHERE tenant_id=$1 AND parent_run_id=$2) OR EXISTS (SELECT 1 FROM stateknot.child_run_ownership WHERE tenant_id=$1 AND (parent_run_id=$2 OR child_run_id=$2))")
        .bind(base.tenant_id().as_str()).bind(*base.run_id().as_uuid())
        .fetch_one(&mut **tx).await.map_err(|source| StoreError::database("Root creation Agent witnesses",source))
}

async fn first_event(
    tx: &mut Transaction<'_, Postgres>,
    base: &CheckpointHead,
) -> Result<(JournalEvent, Option<Digest>), StoreError> {
    let (first, projection) =
        child_runs::anchored_event(tx, base.tenant_id(), base.run_id(), 1).await?;
    if first.sequence() != JournalSequence::FIRST || first.previous_digest().is_some() {
        return Err(StoreError::corrupt("Root creation first journal"));
    }
    Ok((first, projection))
}

async fn original_profile(
    tx: &mut Transaction<'_, Postgres>,
    base: &CheckpointHead,
) -> Result<Option<Box<Source>>, StoreError> {
    let id: Option<Uuid> = query_scalar("SELECT base_checkpoint_id FROM stateknot.root_graph_barriers WHERE tenant_id=$1 AND run_id=$2 ORDER BY base_superstep LIMIT 1")
        .bind(base.tenant_id().as_str()).bind(*base.run_id().as_uuid()).fetch_optional(&mut **tx).await.map_err(|source| StoreError::database("Root original source profile lookup",source))?;
    let Some(id) = id else {
        return Ok(None);
    };
    let initial_base = checkpoint(
        tx,
        base.tenant_id(),
        base.run_id(),
        CheckpointId::from_uuid(id).map_err(|_| StoreError::corrupt("Root source profile base"))?,
    )
    .await?;
    let row = row(tx, &initial_base.head())
        .await?
        .ok_or_else(|| StoreError::corrupt("Root original source profile"))?;
    let wire = decode(&row.bytes)?;
    if Digest::sha256(&row.bytes) != decode_digest(&row.checksum, "Root original source checksum")?
        || wire.intent.base != initial_base.head()
    {
        return Err(StoreError::corrupt("Root original source profile anchor"));
    }
    wire.intent
        .source
        .clone()
        .map(Some)
        .ok_or_else(|| StoreError::corrupt("Root ordinary source contradicts Agent bundle"))
}

pub(super) async fn authenticate(
    tx: &mut Transaction<'_, Postgres>,
    base: &CheckpointHead,
    before: &RunLifecycle,
) -> Result<Origin, StoreError> {
    let (first, projection) = first_event(tx, base).await?;
    if agent_witness(tx, base, &first).await? {
        return Ok(Origin::Agent(authenticated_admission(tx, base).await?));
    }
    let source = if let Some(source) = original_profile(tx, base).await? {
        source
    } else {
        // Only this actual Active revision retains the original Start time.
        // Later mutable lifecycle snapshots cannot reconstruct a lost preimage.
        if before.status() != RunStatus::Active || before.revision() != RunRevision::new(1) {
            return Err(StoreError::GraphReplayHistoricalDispositionUnavailable);
        }
        let started_at = before.changed_at();
        let start_projection_digest = projection_digest(&RunProjection::transition(
            RunRevision::ZERO,
            RunTransition::Start { started_at },
        ))?;
        if projection != Some(start_projection_digest) {
            return Err(StoreError::GraphReplayHistoricalDispositionUnavailable);
        }
        let id:Uuid=query_scalar("SELECT checkpoint_id FROM stateknot.run_checkpoints WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace='' AND superstep=0")
            .bind(base.tenant_id().as_str()).bind(*base.run_id().as_uuid()).fetch_optional(&mut **tx).await.map_err(|source|StoreError::database("Root creation initial checkpoint",source))?.ok_or_else(||StoreError::corrupt("Root creation initial checkpoint"))?;
        let initial = checkpoint(
            tx,
            base.tenant_id(),
            base.run_id(),
            CheckpointId::from_uuid(id)
                .map_err(|_| StoreError::corrupt("Root initial checkpoint identity"))?,
        )
        .await?;
        Box::new(Source::TrustedGraph {
            creation: Box::new(Creation {
                provenance: before.provenance().clone(),
                admitted_at: before.admitted_at(),
                first_journal: first.head(),
                start_journal: first.head(),
                started_at,
                start_projection_digest,
                initial_checkpoint: initial.head(),
                initial_journal: initial.journal_head().clone(),
            }),
        })
    };
    verify(tx, base, before, &source).await?;
    Ok(Origin::TrustedGraph(source))
}

pub(super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    base: &CheckpointHead,
    before: &RunLifecycle,
    source: &Source,
) -> Result<(), StoreError> {
    let Source::TrustedGraph { creation } = source;
    let (first, projection) = first_event(tx, base).await?;
    if agent_witness(tx, base, &first).await? {
        return Err(StoreError::corrupt(
            "Root trusted creation contradicts Agent ownership",
        ));
    }
    let current = query_as::<_, RunRow>(SELECT_RUN)
        .bind(base.tenant_id().as_str())
        .bind(*base.run_id().as_uuid())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|source| StoreError::database("Root original creating Run", source))?
        .ok_or_else(|| StoreError::corrupt("Root original creating Run"))?;
    let current = decode_run(current)?;
    let initial = checkpoint(
        tx,
        base.tenant_id(),
        base.run_id(),
        creation.initial_checkpoint.checkpoint_id(),
    )
    .await?;
    let (initial_event, initial_projection) = child_runs::anchored_event(
        tx,
        base.tenant_id(),
        base.run_id(),
        i64::try_from(creation.initial_journal.sequence().get())
            .map_err(|_| StoreError::corrupt("Root creation checkpoint sequence"))?,
    )
    .await?;
    let started = RunLifecycle::admitted(creation.provenance.clone(), creation.admitted_at)
        .apply(RunTransition::Start {
            started_at: creation.started_at,
        })
        .map_err(|_| StoreError::corrupt("Root original Start lifecycle"))?;
    let actual_start = projection_digest(&RunProjection::transition(
        RunRevision::ZERO,
        RunTransition::Start {
            started_at: creation.started_at,
        },
    ))?;
    if creation.provenance != *before.provenance()
        || creation.admitted_at != before.admitted_at()
        || creation.provenance != *current.lifecycle().provenance()
        || creation.admitted_at != current.lifecycle().admitted_at()
        || creation.first_journal != first.head()
        || creation.start_journal != first.head()
        || creation.start_projection_digest != actual_start
        || projection != Some(actual_start)
        || creation.started_at > first.recorded_at()
        || started.status() != RunStatus::Active
        || started.revision() != RunRevision::new(1)
        || initial.head() != creation.initial_checkpoint
        || initial.superstep() != Superstep::INITIAL
        || initial.parent().is_some()
        || initial.graph() != base.graph()
        || initial.journal_head() != &creation.initial_journal
        || initial_event.head() != creation.initial_journal
        || initial_event.sequence() < first.sequence()
        || initial_event.recorded_at() < first.recorded_at()
        || initial_event.payload().kind().as_str() == EVENT_KIND
        || initial_event.payload().kind().as_str() == GraphFrameEntryPlan::EVENT_KIND
        || if initial_event.head() == first.head() {
            initial_projection != Some(actual_start)
        } else {
            initial_projection != Some(projection_digest(&RunProjection::unchanged())?)
        }
    {
        return Err(StoreError::corrupt(
            "Root positive original creation binding",
        ));
    }
    Box::pin(verify_checkpoint(tx, &initial)).await?;
    let graph = load_graph_definition_row(tx, base.tenant_id(), base.graph())
        .await?
        .ok_or_else(|| StoreError::corrupt("Root creation pinned graph"))?;
    let graph = decode_graph_definition(graph)?;
    if graph.graph().reference() != *base.graph() || graph.tenant_id() != base.tenant_id() {
        return Err(StoreError::corrupt("Root creation pinned graph"));
    }
    Ok(())
}
