// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Whole scoped barriers; component projections alone never authorize replay.
#[allow(clippy::wildcard_imports)]
use super::*;
use sqlx_core::row::Row as _;
use stateknot_core::{
    GraphBarrierDisposition, GraphFrameBarrier, GraphFrameBarrierPlan, NodeTerminalOutput,
    NodeWaits,
};

const INTENT_DOMAIN: &[u8] = b"stateknot-postgres-frame-barrier-scope-v1\0";
const COMPOUND_DOMAIN: &[u8] = b"stateknot-postgres-frame-barrier-compound-v1\0";
const MAX_BARRIER_BYTES: usize = 4 * 1024 * 1024;
const EVENT_KIND: &str = "graph-frame-barrier-committed";

/// Authenticated scoped checkpoint advance and complete result consumption.
#[derive(Clone, Debug)]
pub struct StoredGraphFrameBarrier {
    event: JournalEvent,
    barrier: GraphFrameBarrier,
    checkpoint: GraphFrameCheckpoint,
    disposition: Disposition,
    scope: EntryScope,
    budget: EntryBudget,
    scope_intent_digest: Digest,
    digest: Digest,
}

impl StoredGraphFrameBarrier {
    /// Returns the one journal anchor of the complete barrier transaction.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        &self.event
    }
    /// Returns the complete scoped intent and canonical result set.
    #[must_use]
    pub const fn barrier(&self) -> &GraphFrameBarrier {
        &self.barrier
    }
    /// Returns the complete authenticated successor in this frame.
    #[must_use]
    pub const fn checkpoint(&self) -> &GraphFrameCheckpoint {
        &self.checkpoint
    }
    /// Returns the validated semantic outcome; this grants no Run transition.
    #[must_use]
    pub fn disposition(&self) -> GraphBarrierDisposition {
        self.disposition.to_core()
    }
    /// Returns the whole compound journal projection digest.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
    /// Returns authenticated complete DIRECT usage after this scoped barrier.
    ///
    /// # Errors
    /// Rejects canonical encoding failure or counter overflow.
    pub fn direct_usage_after(&self) -> Result<BudgetUsage, StoreError> {
        charged(self)
    }
}

/// An idempotent barrier consumes no result twice and grants no fresh launch.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum GraphFrameBarrierCommitOutcome {
    /// Every scoped barrier component committed in one transaction.
    Committed(StoredGraphFrameBarrier),
    /// The original complete transaction was reloaded and authenticated.
    Idempotent(StoredGraphFrameBarrier),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Disposition {
    Continue,
    Wait { waits: NodeWaits },
    Terminal { output: NodeTerminalOutput },
}
impl Disposition {
    fn from_core(value: &GraphBarrierDisposition) -> Result<Self, StoreError> {
        match value {
            GraphBarrierDisposition::Continue => Ok(Self::Continue),
            GraphBarrierDisposition::Wait { waits } => Ok(Self::Wait {
                waits: waits.clone(),
            }),
            GraphBarrierDisposition::Terminal { output } => Ok(Self::Terminal {
                output: output.clone(),
            }),
            _ => Err(StoreError::GraphFrameRejected),
        }
    }
    fn to_core(&self) -> GraphBarrierDisposition {
        match self {
            Self::Continue => GraphBarrierDisposition::Continue,
            Self::Wait { waits } => GraphBarrierDisposition::Wait {
                waits: waits.clone(),
            },
            Self::Terminal { output } => GraphBarrierDisposition::Terminal {
                output: output.clone(),
            },
        }
    }
}

// Store-only wire. Its bound includes the complete bounded result-head set and
// successor intent, not merely one legacy checkpoint or result projection.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    version: u8,
    barrier: GraphFrameBarrier,
    checkpoint: GraphFrameCheckpointHead,
    disposition: Disposition,
    scope: EntryScope,
    budget: EntryBudget,
    scope_intent_digest: Digest,
    compound_digest: Digest,
}

#[derive(Serialize)]
struct ScopePreimage<'a> {
    version: u8,
    barrier_intent_digest: Digest,
    disposition: &'a Disposition,
    scope: &'a EntryScope,
    budget: &'a EntryBudget,
}
#[derive(Serialize)]
struct CompoundPreimage<'a> {
    version: u8,
    scope_intent_digest: Digest,
    event: &'a JournalEvent,
    checkpoint: &'a GraphFrameCheckpointHead,
}

fn domain_digest<T: Serialize>(domain: &[u8], value: &T) -> Result<Digest, StoreError> {
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| StoreError::encoding("frame barrier canonical preimage"))?;
    let mut bytes = Vec::with_capacity(domain.len() + canonical.len());
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(&canonical);
    Ok(Digest::sha256(bytes))
}

fn scope_intent(
    barrier: &GraphFrameBarrier,
    disposition: &Disposition,
    scope: &EntryScope,
    budget: &EntryBudget,
) -> Result<Digest, StoreError> {
    domain_digest(
        INTENT_DOMAIN,
        &ScopePreimage {
            version: 1,
            barrier_intent_digest: barrier.intent_digest(),
            disposition,
            scope,
            budget,
        },
    )
}
fn compound(
    scope: Digest,
    event: &JournalEvent,
    checkpoint: &GraphFrameCheckpointHead,
) -> Result<Digest, StoreError> {
    domain_digest(
        COMPOUND_DOMAIN,
        &CompoundPreimage {
            version: 1,
            scope_intent_digest: scope,
            event,
            checkpoint,
        },
    )
}

fn wire(record: &StoredGraphFrameBarrier) -> Wire {
    Wire {
        version: 1,
        barrier: record.barrier.clone(),
        checkpoint: record.checkpoint.head(),
        disposition: record.disposition.clone(),
        scope: record.scope.clone(),
        budget: record.budget.clone(),
        scope_intent_digest: record.scope_intent_digest,
        compound_digest: record.digest,
    }
}
fn encode(record: &StoredGraphFrameBarrier) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(&wire(record))
        .map_err(|_| StoreError::encoding("frame barrier record"))?;
    if bytes.is_empty() || bytes.len() > MAX_BARRIER_BYTES {
        return Err(StoreError::GraphFrameLimitExceeded);
    }
    Ok(bytes)
}
fn decode_wire(bytes: &[u8]) -> Result<Wire, StoreError> {
    if bytes.is_empty() || bytes.len() > MAX_BARRIER_BYTES {
        return Err(StoreError::corrupt("frame barrier byte bound"));
    }
    // Bounded Core readers cap the result collection and nested state. Strict
    // struct/enum readers reject unknown/duplicate fields; exact canonical bytes
    // additionally reject alternate number, order, whitespace and escape forms.
    let value: Wire =
        serde_json::from_slice(bytes).map_err(|_| StoreError::corrupt("frame barrier wire"))?;
    if value.version != 1
        || serde_json_canonicalizer::to_vec(&value)
            .map_err(|_| StoreError::corrupt("frame barrier canonical wire"))?
            != bytes
    {
        return Err(StoreError::corrupt(
            "frame barrier version or canonical bytes",
        ));
    }
    Ok(value)
}

fn bind_record(
    plan: &GraphFrameBarrierPlan,
    event: JournalEvent,
    checkpoint: GraphFrameCheckpoint,
    scope: EntryScope,
    budget: EntryBudget,
) -> Result<StoredGraphFrameBarrier, StoreError> {
    let barrier = plan.barrier().clone();
    let disposition = Disposition::from_core(plan.disposition())?;
    if checkpoint.frame() != barrier.base_checkpoint().frame()
        || !checkpoint.checkpoint().matches_write(barrier.successor())
        || checkpoint.checkpoint().journal_head() != &event.head()
        || event.source().worker_fence().is_none()
        || event.payload().kind().as_str() != EVENT_KIND
    {
        return Err(StoreError::GraphFrameConflict);
    }
    let scope_intent_digest = scope_intent(&barrier, &disposition, &scope, &budget)?;
    let digest = compound(scope_intent_digest, &event, &checkpoint.head())?;
    Ok(StoredGraphFrameBarrier {
        event,
        barrier,
        checkpoint,
        disposition,
        scope,
        budget,
        scope_intent_digest,
        digest,
    })
}

const SCHEMA_ID: &str = "https://stknot.com/schemas/store/graph-frame-barrier-event/1.0.0";
static EVENT_SCHEMA: LazyLock<
    Result<(stateknot_core::SchemaReference, serde_json::Value), &'static str>,
> = LazyLock::new(|| {
    let digest = serde_json::json!({"type":"string","minLength":71,"maxLength":71,"pattern":"^sha256:[0-9a-f]{64}$"});
    let id = serde_json::json!({"type":"string","minLength":36,"maxLength":36,"pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"});
    let document = serde_json::json!({
        "$schema":"https://json-schema.org/draft/2020-12/schema","$id":SCHEMA_ID,
        "type":"object","additionalProperties":false,
        "properties":{
            "version":{"const":1},
            "graph_namespace":{"type":"string","minLength":64,"maxLength":454,"pattern":"^[0-9a-f]{64}(/[0-9a-f]{64}){0,6}$"},
            "frame_identity_digest":digest,"base_checkpoint_id":id,"base_checkpoint_digest":digest,
            "successor_checkpoint_id":id,"successor_intent_digest":digest,
            "barrier_intent_digest":digest,"scope_intent_digest":digest
        },
        "required":["version","graph_namespace","frame_identity_digest","base_checkpoint_id","base_checkpoint_digest","successor_checkpoint_id","successor_intent_digest","barrier_intent_digest","scope_intent_digest"]
    });
    let bytes =
        serde_json_canonicalizer::to_vec(&document).map_err(|_| "frame barrier event schema")?;
    let reference = stateknot_core::SchemaReference::new(
        SCHEMA_ID.parse().map_err(|_| "frame barrier schema ID")?,
        stateknot_core::Version::new(1, 0, 0),
        Digest::sha256(bytes),
    );
    Ok((reference, document))
});

#[derive(Serialize)]
struct EventWire<'a> {
    version: u8,
    graph_namespace: &'a GraphNamespace,
    frame_identity_digest: Digest,
    base_checkpoint_id: CheckpointId,
    base_checkpoint_digest: Digest,
    successor_checkpoint_id: CheckpointId,
    successor_intent_digest: Digest,
    barrier_intent_digest: Digest,
    scope_intent_digest: Digest,
}
fn payload(barrier: &GraphFrameBarrier, scope: Digest) -> Result<JournalPayload, StoreError> {
    let base = barrier.base_checkpoint();
    let bytes = serde_json_canonicalizer::to_vec(&EventWire {
        version: 1,
        graph_namespace: base.frame().namespace(),
        frame_identity_digest: base.frame().digest(),
        base_checkpoint_id: base.checkpoint().checkpoint_id(),
        base_checkpoint_digest: base.checkpoint().digest(),
        successor_checkpoint_id: barrier.successor().checkpoint_id(),
        successor_intent_digest: barrier.successor().intent_digest(),
        barrier_intent_digest: barrier.intent_digest(),
        scope_intent_digest: scope,
    })
    .map_err(|_| StoreError::encoding("frame barrier payload"))?;
    let schema = EVENT_SCHEMA
        .as_ref()
        .map_err(|_| StoreError::encoding("frame barrier schema"))?
        .0
        .clone();
    JournalPayload::new(
        schema,
        EVENT_KIND
            .parse()
            .map_err(|_| StoreError::encoding("frame barrier kind"))?,
        BoundedJson::from_slice(&bytes)
            .map_err(|_| StoreError::encoding("frame barrier payload bound"))?,
    )
    .map_err(|_| StoreError::encoding("frame barrier payload"))
}

struct Row {
    frame_identity_digest: Vec<u8>,
    base_superstep: i64,
    base_checkpoint_digest: Vec<u8>,
    base_frame_checkpoint_digest: Vec<u8>,
    successor_checkpoint_id: Uuid,
    successor_superstep: i64,
    successor_checkpoint_digest: Vec<u8>,
    successor_frame_checkpoint_digest: Vec<u8>,
    result_count: i32,
    barrier_intent_digest: Vec<u8>,
    scope_intent_digest: Vec<u8>,
    compound_digest: Vec<u8>,
    barrier_bytes: Vec<u8>,
    barrier_checksum: Vec<u8>,
    journal_sequence: i64,
    journal_event_id: Uuid,
    journal_recorded_at: DateTime<Utc>,
    journal_digest: Vec<u8>,
}
impl<'r> FromRow<'r, PgRow> for Row {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx_core::error::Error> {
        Ok(Self {
            frame_identity_digest: row.try_get("frame_identity_digest")?,
            base_superstep: row.try_get("base_superstep")?,
            base_checkpoint_digest: row.try_get("base_checkpoint_digest")?,
            base_frame_checkpoint_digest: row.try_get("base_frame_checkpoint_digest")?,
            successor_checkpoint_id: row.try_get("successor_checkpoint_id")?,
            successor_superstep: row.try_get("successor_superstep")?,
            successor_checkpoint_digest: row.try_get("successor_checkpoint_digest")?,
            successor_frame_checkpoint_digest: row.try_get("successor_frame_checkpoint_digest")?,
            result_count: row.try_get("result_count")?,
            barrier_intent_digest: row.try_get("barrier_intent_digest")?,
            scope_intent_digest: row.try_get("scope_intent_digest")?,
            compound_digest: row.try_get("compound_digest")?,
            barrier_bytes: row.try_get("barrier_bytes")?,
            barrier_checksum: row.try_get("barrier_checksum")?,
            journal_sequence: row.try_get("journal_sequence")?,
            journal_event_id: row.try_get("journal_event_id")?,
            journal_recorded_at: row.try_get("journal_recorded_at")?,
            journal_digest: row.try_get("journal_digest")?,
        })
    }
}
async fn row(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
    namespace: &GraphNamespace,
    base: CheckpointId,
) -> Result<Option<Row>, StoreError> {
    query_as::<_,Row>("SELECT frame_identity_digest,base_superstep,base_checkpoint_digest,base_frame_checkpoint_digest,successor_checkpoint_id,successor_superstep,successor_checkpoint_digest,successor_frame_checkpoint_digest,result_count,barrier_intent_digest,scope_intent_digest,compound_digest,barrier_bytes,barrier_checksum,journal_sequence,journal_event_id,journal_recorded_at,journal_digest FROM stateknot.graph_frame_barriers WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3 AND base_checkpoint_id=$4")
        .bind(tenant.as_str()).bind(*run.as_uuid()).bind(namespace.as_str()).bind(*base.as_uuid()).fetch_optional(&mut **tx).await
        .map_err(|e|StoreError::database("frame barrier row",e))
}

fn charged(record: &StoredGraphFrameBarrier) -> Result<BudgetUsage, StoreError> {
    use stateknot_core::{ByteCount, ExecutionCount};
    let checkpoint_bytes = encode_checkpoint(record.checkpoint.checkpoint())?
        .len()
        .checked_add(
            serde_json_canonicalizer::to_vec(&record.checkpoint.head())
                .map_err(|_| StoreError::encoding("barrier checkpoint head"))?
                .len(),
        )
        .ok_or(StoreError::GraphFrameLimitExceeded)?;
    let event_bytes = serde_json_canonicalizer::to_vec(&record.event)
        .map_err(|_| StoreError::encoding("barrier event charge"))?
        .len();
    let delta = BudgetUsage::builder()
        .graph_steps(ExecutionCount::new(1))
        .checkpoint_bytes(ByteCount::new(
            u64::try_from(checkpoint_bytes).map_err(|_| StoreError::GraphFrameLimitExceeded)?,
        ))
        .event_bytes(ByteCount::new(
            u64::try_from(event_bytes).map_err(|_| StoreError::GraphFrameLimitExceeded)?,
        ))
        .build()
        .map_err(|_| StoreError::GraphFrameRejected)?;
    record
        .budget
        .direct_usage
        .checked_accumulate(&delta)
        .map_err(|_| StoreError::GraphFrameLimitExceeded)
}

async fn results_on_checkpoint(
    tx: &mut Transaction<'_, Postgres>,
    base: &GraphFrameCheckpoint,
    heads: &[PendingNodeResultHead],
) -> Result<Vec<PendingNodeResult>, StoreError> {
    let durable = load_locked_barrier_result_heads(tx, &base.checkpoint().head()).await?;
    validate_complete_barrier_result_heads(&durable, heads)?;
    let mut results = Vec::with_capacity(heads.len());
    let mut compact_bytes = 0_usize;
    for head in heads {
        if !base
            .activation(head.activation().node_id().clone())
            .is_ok_and(|activation| activation == *head.activation())
        {
            return Err(StoreError::corrupt("barrier result activation"));
        }
        let row = load_pending_node_result_row(tx, head.activation())
            .await?
            .ok_or(StoreError::CheckpointBarrierIncomplete)?;
        let result = decode_pending_node_result(&row)?;
        if result.head() != *head {
            return Err(StoreError::CheckpointBarrierResultConflict);
        }
        verify_pending_node_result_components(tx, &result).await?;
        let mut counter = CompactByteCounter::default();
        serde_json::to_writer(&mut counter, &result)
            .map_err(|_| StoreError::corrupt("frame barrier result encoding"))?;
        compact_bytes = compact_bytes.saturating_add(counter.bytes);
        if compact_bytes > GraphReplayLimits::default().maximum_barrier_result_bytes() {
            return Err(StoreError::GraphReplayResourceLimit);
        }
        results.push(result);
    }
    Ok(results)
}

// Forward streaming releases each previous state/result set before the next
// edge. It never recursively reloads barrier history through a result owner.
// The actual admitted graph and shared Run budget bound the number of edges.
pub(super) async fn checkpoint_at(
    tx: &mut Transaction<'_, Postgres>,
    entry: &StoredGraphFrameEntry,
    admission: &StoredAgentAdmission,
    graphs: &BTreeMap<Digest, CompiledGraph>,
    expected: &CheckpointHead,
) -> Result<(GraphFrameCheckpoint, BudgetUsage), StoreError> {
    let mut current = entry.entry.checkpoint().clone();
    if current.checkpoint().tenant_id() != expected.tenant_id()
        || current.checkpoint().run_id() != expected.run_id()
        || current.checkpoint().graph() != expected.graph()
    {
        return Err(StoreError::GraphFrameConflict);
    }
    let target = graphs
        .get(&current.frame().target().definition_digest())
        .ok_or(StoreError::GraphFrameRejected)?;
    if expected.superstep().get() > target.limits().maximum_supersteps().get()
        || expected.superstep().get() > admission.admission().intent().budget().graph_steps().get()
    {
        return Err(StoreError::GraphFrameLimitExceeded);
    }
    let mut floor = entry.direct_usage_after()?;
    while current.checkpoint().superstep() < expected.superstep() {
        let row = row(
            tx,
            expected.tenant_id(),
            expected.run_id(),
            current.frame().namespace(),
            current.checkpoint().checkpoint_id(),
        )
        .await?
        .ok_or_else(|| StoreError::corrupt("frame barrier lineage missing"))?;
        let record = verify_edge(tx, entry, admission, &current, &floor, row).await?;
        floor = charged(&record)?;
        current = record.checkpoint;
    }
    if current.checkpoint().head() != *expected {
        return Err(StoreError::corrupt("frame checkpoint head substitution"));
    }
    Ok((current, floor))
}

pub(super) async fn current_checkpoint(
    tx: &mut Transaction<'_, Postgres>,
    entry: &StoredGraphFrameEntry,
    admission: &StoredAgentAdmission,
    graphs: &BTreeMap<Digest, CompiledGraph>,
) -> Result<(GraphFrameCheckpoint, BudgetUsage), StoreError> {
    let initial = entry.entry.checkpoint();
    let tenant = initial.checkpoint().tenant_id();
    let run = initial.checkpoint().run_id();
    let namespace = initial.frame().namespace();
    let pointer=query_as::<_,(Uuid,i64,Vec<u8>,Vec<u8>)>("SELECT checkpoint_id,superstep,checkpoint_digest,frame_checkpoint_digest FROM stateknot.graph_frame_heads WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3")
        .bind(tenant.as_str()).bind(*run.as_uuid()).bind(namespace.as_str()).fetch_optional(&mut **tx).await.map_err(|e|StoreError::database("barrier frame head",e))?.ok_or_else(||StoreError::corrupt("barrier frame head missing"))?;
    let checkpoint = decode_frame_checkpoint(
        checkpoint_row(
            tx,
            tenant,
            run,
            namespace,
            CheckpointId::from_uuid(pointer.0)
                .map_err(|_| StoreError::corrupt("barrier head ID"))?,
        )
        .await?,
    )?;
    if nonnegative_superstep(pointer.1)? != checkpoint.checkpoint().superstep()
        || decode_digest(&pointer.2, "barrier pointer checkpoint")?
            != checkpoint.checkpoint().digest()
        || decode_digest(&pointer.3, "barrier pointer frame")? != checkpoint.digest()
    {
        return Err(StoreError::corrupt("barrier frame pointer"));
    }
    let (checkpoint, floor) = checkpoint_at(
        tx,
        entry,
        admission,
        graphs,
        &checkpoint.checkpoint().head(),
    )
    .await?;
    let through = admission
        .run()
        .journal_head()
        .ok_or(StoreError::StaleJournalHead)?
        .sequence()
        .get();
    let floor = caller_bindings::usage_floor_before(tx, entry, admission, through, &floor).await?;
    Ok((checkpoint, floor))
}

#[allow(clippy::too_many_lines)]
async fn verify_edge(
    tx: &mut Transaction<'_, Postgres>,
    entry: &StoredGraphFrameEntry,
    admission: &StoredAgentAdmission,
    base: &GraphFrameCheckpoint,
    floor: &BudgetUsage,
    row: Row,
) -> Result<StoredGraphFrameBarrier, StoreError> {
    let wire = decode_wire(&row.barrier_bytes)?;
    if Digest::sha256(&row.barrier_bytes)
        != decode_digest(&row.barrier_checksum, "barrier bytes checksum")?
        || wire.scope != entry.scope
        || wire.barrier.base_checkpoint() != &base.head()
        || wire.barrier.base_ready_nodes() != base.checkpoint().ready_nodes()
        || row.frame_identity_digest.as_slice() != base.frame().digest().as_bytes()
        || nonnegative_superstep(row.base_superstep)? != base.checkpoint().superstep()
        || decode_digest(&row.base_checkpoint_digest, "barrier base checkpoint")?
            != base.checkpoint().digest()
        || decode_digest(&row.base_frame_checkpoint_digest, "barrier base frame")? != base.digest()
        || i32::try_from(wire.barrier.result_heads().len())
            .map_err(|_| StoreError::corrupt("barrier result count"))?
            != row.result_count
        || decode_digest(&row.barrier_intent_digest, "barrier intent")?
            != wire.barrier.intent_digest()
        || decode_digest(&row.scope_intent_digest, "barrier scope intent")?
            != wire.scope_intent_digest
        || decode_digest(&row.compound_digest, "barrier compound")? != wire.compound_digest
    {
        return Err(StoreError::corrupt("barrier component projection"));
    }
    if matches!(wire.disposition, Disposition::Wait { .. }) {
        return Err(StoreError::corrupt(
            "barrier wait has no whole scoped suspension",
        ));
    }
    let cp = decode_frame_checkpoint(
        checkpoint_row(
            tx,
            base.checkpoint().tenant_id(),
            base.checkpoint().run_id(),
            base.frame().namespace(),
            CheckpointId::from_uuid(row.successor_checkpoint_id)
                .map_err(|_| StoreError::corrupt("barrier successor ID"))?,
        )
        .await?,
    )?;
    if cp.head() != wire.checkpoint
        || !cp.checkpoint().matches_write(wire.barrier.successor())
        || nonnegative_superstep(row.successor_superstep)? != cp.checkpoint().superstep()
        || decode_digest(
            &row.successor_checkpoint_digest,
            "barrier successor checkpoint",
        )? != cp.checkpoint().digest()
        || decode_digest(
            &row.successor_frame_checkpoint_digest,
            "barrier successor frame",
        )? != cp.digest()
    {
        return Err(StoreError::corrupt("barrier successor projection"));
    }
    base.verify_successor(&cp)
        .map_err(|_| StoreError::corrupt("barrier successor lineage"))?;
    let mut event_row = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
        .bind(base.checkpoint().tenant_id().as_str())
        .bind(*base.checkpoint().run_id().as_uuid())
        .bind(row.journal_sequence)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| StoreError::database("barrier event", e))?
        .ok_or_else(|| StoreError::corrupt("barrier event missing"))?;
    let projection = event_row
        .projection_digest
        .take()
        .map(|p| decode_digest(&p, "barrier event projection"))
        .transpose()?;
    let event = decode_event(event_row)?;
    if event.head() != *cp.checkpoint().journal_head()
        || event.event_id().as_uuid() != &row.journal_event_id
        || event.recorded_at() != from_database_time(row.journal_recorded_at)?
        || event.digest() != decode_digest(&row.journal_digest, "barrier journal")?
        || event.source().worker_fence().is_none()
        || projection != Some(wire.compound_digest)
        || event.payload() != &payload(&wire.barrier, wire.scope_intent_digest)?
        || scope_intent(&wire.barrier, &wire.disposition, &wire.scope, &wire.budget)?
            != wire.scope_intent_digest
        || compound(wire.scope_intent_digest, &event, &wire.checkpoint)? != wire.compound_digest
    {
        return Err(StoreError::corrupt("barrier whole event binding"));
    }
    let before = decode_event(
        query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
            .bind(base.checkpoint().tenant_id().as_str())
            .bind(*base.checkpoint().run_id().as_uuid())
            .bind(
                row.journal_sequence
                    .checked_sub(1)
                    .ok_or_else(|| StoreError::corrupt("barrier journal predecessor"))?,
            )
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| StoreError::database("barrier predecessor", e))?
            .ok_or_else(|| StoreError::corrupt("barrier predecessor missing"))?,
    )?;
    if wire.budget.observed_head != before.head()
        || event.previous_digest() != Some(before.digest())
        || event.recorded_at() < before.recorded_at()
        || before.sequence() < base.checkpoint().journal_head().sequence()
        || before.recorded_at() < base.checkpoint().journal_head().recorded_at()
    {
        return Err(StoreError::corrupt("barrier observation"));
    }
    let results = results_on_checkpoint(tx, base, wire.barrier.result_heads()).await?;
    if results.iter().any(|result| {
        result.journal_head().sequence() > before.sequence()
            || result.journal_head().recorded_at() > before.recorded_at()
    }) {
        return Err(StoreError::corrupt("barrier result journal order"));
    }
    verify_barrier_consumption_parts(
        tx,
        &base.checkpoint().head(),
        wire.barrier.result_heads(),
        cp.checkpoint(),
    )
    .await?;
    let floor =
        caller_bindings::usage_floor_before(tx, entry, admission, before.sequence().get(), floor)
            .await?;
    wire.budget
        .direct_usage
        .validate_monotonic_after(&floor)
        .map_err(|_| StoreError::corrupt("barrier shared usage regression"))?;
    let record = StoredGraphFrameBarrier {
        event,
        barrier: wire.barrier,
        checkpoint: cp,
        disposition: wire.disposition,
        scope: wire.scope,
        budget: wire.budget,
        scope_intent_digest: wire.scope_intent_digest,
        digest: wire.compound_digest,
    };
    let total = charged(&record)?
        .checked_accumulate(&record.budget.delegated_usage)
        .map_err(|_| StoreError::corrupt("barrier accounting"))?;
    admission
        .admission()
        .intent()
        .budget()
        .remaining(&total, record.event.recorded_at())
        .map_err(|_| StoreError::corrupt("barrier budget limit"))?;
    Ok(record)
}

fn verify_retry(
    record: &StoredGraphFrameBarrier,
    plan: &GraphFrameBarrierPlan,
) -> Result<(), StoreError> {
    if record.barrier != *plan.barrier()
        || record.disposition != Disposition::from_core(plan.disposition())?
    {
        return Err(StoreError::GraphFrameConflict);
    }
    Ok(())
}

async fn admission_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
) -> Result<StoredAgentAdmission, StoreError> {
    let run_row = query_as::<_, RunRow>(SELECT_RUN)
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| StoreError::database("frame barrier Run snapshot", e))?
        .ok_or(StoreError::RunNotFound)?;
    let admission_row = load_agent_admission_row(tx, tenant, run)
        .await?
        .ok_or(StoreError::AgentAdmissionConflict)?;
    verify_stored_agent_admission(tx, decode_run(run_row)?, admission_row).await
}

async fn verified_record(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
    namespace: &GraphNamespace,
    admission: &StoredAgentAdmission,
    graphs: &BTreeMap<Digest, CompiledGraph>,
    row: Row,
) -> Result<StoredGraphFrameBarrier, StoreError> {
    let entry = Box::pin(verified_entry(
        tx, tenant, run, namespace, admission, graphs,
    ))
    .await?;
    let wire = decode_wire(&row.barrier_bytes)?;
    let (base, floor) = checkpoint_at(
        tx,
        &entry,
        admission,
        graphs,
        wire.barrier.base_checkpoint().checkpoint(),
    )
    .await?;
    verify_edge(tx, &entry, admission, &base, &floor, row).await
}

impl PostgresStore {
    /// Returns the exact closed, offline schema and pin for scoped barrier events.
    ///
    /// # Errors
    /// Rejects a local canonical schema encoding failure.
    pub fn graph_frame_barrier_event_schema()
    -> Result<(stateknot_core::SchemaReference, serde_json::Value), StoreError> {
        EVENT_SCHEMA
            .as_ref()
            .cloned()
            .map_err(|_| StoreError::encoding("frame barrier event schema"))
    }

    /// Restores one whole immutable barrier, including its bounded ancestry,
    /// full successor, result owners, consumption rows and exact journal anchor.
    /// Recovery is independent of current lease ownership and the active leaf.
    ///
    /// # Errors
    /// Rejects Root scope, missing admission, corruption or bounded replay limits.
    pub async fn load_graph_frame_barrier(
        &self,
        tenant: &TenantId,
        run: RunId,
        namespace: &GraphNamespace,
        base: CheckpointId,
    ) -> Result<Option<StoredGraphFrameBarrier>, StoreError> {
        Box::pin(self.load_graph_frame_barrier_inner(tenant, run, namespace, base)).await
    }

    async fn load_graph_frame_barrier_inner(
        &self,
        tenant: &TenantId,
        run: RunId,
        namespace: &GraphNamespace,
        base: CheckpointId,
    ) -> Result<Option<StoredGraphFrameBarrier>, StoreError> {
        if namespace.is_root() {
            return Err(StoreError::GraphFrameConflict);
        }
        let mut tx = self.begin_repeatable_read("frame barrier snapshot").await?;
        let Some(row) = row(&mut tx, tenant, run, namespace, base).await? else {
            tx.commit()
                .await
                .map_err(|e| StoreError::database("empty frame barrier snapshot commit", e))?;
            return Ok(None);
        };
        let wire = decode_wire(&row.barrier_bytes)?;
        if wire.barrier.base_checkpoint().checkpoint().checkpoint_id() != base
            || wire.barrier.base_checkpoint().frame().namespace() != namespace
        {
            return Err(StoreError::corrupt("frame barrier lookup identity"));
        }
        let admission = admission_snapshot(&mut tx, tenant, run).await?;
        let graphs = closure(&mut tx, tenant, admission.admission().intent().graph()).await?;
        let record = Box::pin(verified_record(
            &mut tx, tenant, run, namespace, &admission, &graphs, row,
        ))
        .await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::database("frame barrier snapshot commit", e))?;
        Ok(Some(record))
    }

    /// Restores a historical scoped checkpoint through its whole entry and
    /// every preceding barrier; a component row alone grants no authority.
    ///
    /// # Errors
    /// Rejects Root scope, missing history, substitutions or replay limits.
    pub async fn load_graph_frame_checkpoint(
        &self,
        tenant: &TenantId,
        run: RunId,
        namespace: &GraphNamespace,
        checkpoint_id: CheckpointId,
    ) -> Result<GraphFrameCheckpoint, StoreError> {
        let mut tx = self
            .begin_repeatable_read("frame checkpoint snapshot")
            .await?;
        if namespace.is_root() {
            return Err(StoreError::GraphFrameConflict);
        }
        let checkpoint = decode_frame_checkpoint(
            checkpoint_row(&mut tx, tenant, run, namespace, checkpoint_id).await?,
        )?;
        let restored = Box::pin(bound_checkpoint(
            &mut tx,
            tenant,
            run,
            namespace,
            &checkpoint.checkpoint().head(),
            false,
        ))
        .await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::database("frame checkpoint snapshot commit", e))?;
        Ok(restored)
    }

    /// Atomically advances the active leaf and consumes its complete result set.
    ///
    /// The actual admitted graph, full base/results and supplied pinned schema
    /// validator/reducer derive the plan before locking the mutation transaction.
    /// The locked transaction repeats durable evidence, leaf, complete DIRECT
    /// usage, settled child account, observation, fence and database-clock budget.
    /// Exact acknowledgment-loss retries authenticate the original whole record
    /// before consulting callbacks, fresh observation or lease ownership.
    /// A terminal scoped checkpoint suspends the caller until a whole return;
    /// scoped waits require their separate whole suspension transaction.
    ///
    /// # Errors
    /// Rejects invalid plans, incomplete results/accounting, stale authority,
    /// unresolved effects, unsupported scoped waits, corruption or SQL failures.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_graph_frame_barrier<
        V: GraphSchemaValidator + ?Sized,
        R: GraphReducer + ?Sized,
    >(
        &self,
        plan: GraphFrameBarrierPlan,
        event_id: EventId,
        fence: RunFence,
        observed: JournalHead,
        direct_usage: BudgetUsage,
        schemas: &V,
        reducer: &R,
    ) -> Result<GraphFrameBarrierCommitOutcome, StoreError> {
        Box::pin(self.commit_graph_frame_barrier_inner(
            plan,
            event_id,
            fence,
            observed,
            direct_usage,
            schemas,
            reducer,
        ))
        .await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn commit_graph_frame_barrier_inner<
        V: GraphSchemaValidator + ?Sized,
        R: GraphReducer + ?Sized,
    >(
        &self,
        plan: GraphFrameBarrierPlan,
        event_id: EventId,
        fence: RunFence,
        observed: JournalHead,
        direct_usage: BudgetUsage,
        schemas: &V,
        reducer: &R,
    ) -> Result<GraphFrameBarrierCommitOutcome, StoreError> {
        let barrier = plan.barrier();
        let head = barrier.base_checkpoint();
        let base = head.checkpoint();
        let tenant = base.tenant_id();
        let run = base.run_id();
        let namespace = head.frame().namespace();
        if let Some(record) = Box::pin(self.load_graph_frame_barrier_inner(
            tenant,
            run,
            namespace,
            base.checkpoint_id(),
        ))
        .await?
        {
            verify_retry(&record, &plan)?;
            return Ok(GraphFrameBarrierCommitOutcome::Idempotent(record));
        }
        if matches!(plan.disposition(), GraphBarrierDisposition::Wait { .. }) {
            return Err(StoreError::GraphFrameCompoundRequired);
        }
        if fence.tenant_id() != tenant || fence.run_id() != run {
            return Err(StoreError::GraphFrameConflict);
        }
        let mut snapshot = self
            .begin_repeatable_read("frame barrier planning snapshot")
            .await?;
        let admission = admission_snapshot(&mut snapshot, tenant, run).await?;
        let graphs = closure(
            &mut snapshot,
            tenant,
            admission.admission().intent().graph(),
        )
        .await?;
        let entry = Box::pin(verified_entry(
            &mut snapshot,
            tenant,
            run,
            namespace,
            &admission,
            &graphs,
        ))
        .await?;
        let (checkpoint, _) =
            checkpoint_at(&mut snapshot, &entry, &admission, &graphs, base).await?;
        let results =
            results_on_checkpoint(&mut snapshot, &checkpoint, barrier.result_heads()).await?;
        let graph = graphs
            .get(&checkpoint.checkpoint().graph().definition_digest())
            .ok_or(StoreError::GraphFrameRejected)?;
        snapshot
            .commit()
            .await
            .map_err(|e| StoreError::database("frame barrier planning snapshot commit", e))?;
        let actual = catch_unwind(AssertUnwindSafe(|| {
            graph.plan_frame_barrier(
                &checkpoint,
                &results,
                barrier.successor().checkpoint_id(),
                schemas,
                reducer,
            )
        }))
        .map_err(|_| StoreError::GraphReplayDependencyUnavailable)?
        .map_err(|error| map_graph_replay_plan_error(&error))?;
        if actual.barrier() != barrier || actual.disposition() != plan.disposition() {
            return Err(StoreError::GraphFrameConflict);
        }
        drop(results);
        drop(checkpoint);
        drop(entry);
        drop(graphs);
        drop(admission);

        let mut tx = self.begin_mutation("whole frame barrier").await?;
        let admission = load_locked_agent_admission(&mut tx, tenant, run)
            .await?
            .ok_or(StoreError::AgentAdmissionConflict)?;
        let graphs = closure(&mut tx, tenant, admission.admission().intent().graph()).await?;
        if let Some(existing) = row(&mut tx, tenant, run, namespace, base.checkpoint_id()).await? {
            let record = Box::pin(verified_record(
                &mut tx, tenant, run, namespace, &admission, &graphs, existing,
            ))
            .await?;
            verify_retry(&record, &plan)?;
            tx.commit()
                .await
                .map_err(|e| StoreError::database("frame barrier retry commit", e))?;
            return Ok(GraphFrameBarrierCommitOutcome::Idempotent(record));
        }
        let stored = admission.run();
        validate_runnable(stored)?;
        if stored.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::RunNotRunnable);
        }
        if stored.journal_head() != Some(&observed) {
            return Err(StoreError::StaleJournalHead);
        }
        let now = database_now(&mut tx, "frame barrier authority clock").await?;
        authorize_worker(stored, &fence, now)?;
        let entry = Box::pin(verified_entry(
            &mut tx, tenant, run, namespace, &admission, &graphs,
        ))
        .await?;
        let (checkpoint, floor) =
            Box::pin(current_checkpoint(&mut tx, &entry, &admission, &graphs)).await?;
        if checkpoint.head() != *head {
            return Err(StoreError::StaleCheckpointHead);
        }
        // This authenticates the current active leaf and exact requested base.
        let active = Box::pin(bound_checkpoint(
            &mut tx, tenant, run, namespace, base, true,
        ))
        .await?;
        if active != checkpoint {
            return Err(StoreError::GraphFrameConflict);
        }
        direct_usage
            .validate_monotonic_after(&floor)
            .map_err(|_| StoreError::IncompleteChildAccounting)?;
        ensure_no_unsettled_tool_invocations(&mut tx, checkpoint.checkpoint()).await?;
        ensure_no_unsettled_model_invocations(&mut tx, checkpoint.checkpoint()).await?;
        if !load_barrier_consumption_rows(&mut tx, base)
            .await?
            .is_empty()
        {
            return Err(StoreError::CheckpointBarrierResultConflict);
        }
        let results = results_on_checkpoint(&mut tx, &checkpoint, barrier.result_heads()).await?;
        if results.iter().any(|result| {
            result.journal_head().sequence() > observed.sequence()
                || result.journal_head().recorded_at() > observed.recorded_at()
        }) {
            return Err(StoreError::CheckpointBarrierResultConflict);
        }
        let account = Box::pin(child_runs::load_account_inner(&mut tx, tenant, run, false)).await?;
        let (delegated_usage, child_account_digest) = if let Some(account) = account {
            child_runs::ensure_settled(&account)?;
            direct_usage
                .validate_monotonic_after(account.direct_usage())
                .map_err(|_| StoreError::IncompleteChildAccounting)?;
            (
                account
                    .delegated_usage()
                    .map_err(|_| StoreError::IncompleteChildAccounting)?,
                Some(account.digest()),
            )
        } else {
            (BudgetUsage::zero(), None)
        };
        let budget = EntryBudget {
            observed_head: observed.clone(),
            direct_usage,
            delegated_usage,
            child_account_digest,
        };
        let disposition = Disposition::from_core(plan.disposition())?;
        let intent_digest = scope_intent(barrier, &disposition, &entry.scope, &budget)?;
        let intent = JournalEventIntent::worker(
            tenant.clone(),
            run,
            event_id,
            fence.clone(),
            payload(barrier, intent_digest)?,
        )
        .map_err(|_| StoreError::GraphFrameRejected)?;
        let append = JournalAppend::new(
            stateknot_core::JournalExpectation::exact(observed.clone()),
            intent,
        )
        .map_err(|_| StoreError::GraphFrameRejected)?;
        let event = JournalEvent::commit(append, now.max(observed.recorded_at()))
            .map_err(|e| map_event_commit_error(&e))?;
        let successor = Checkpoint::commit(barrier.successor().clone(), event.head())
            .map_err(|_| StoreError::GraphFrameRejected)?;
        let successor = GraphFrameCheckpoint::new(checkpoint.frame().clone(), successor)
            .map_err(|_| StoreError::GraphFrameRejected)?;
        checkpoint
            .verify_successor(&successor)
            .map_err(|_| StoreError::GraphFrameConflict)?;
        let record = bind_record(&plan, event, successor, entry.scope, budget)?;
        let total = charged(&record)?
            .checked_accumulate(&record.budget.delegated_usage)
            .map_err(|_| StoreError::IncompleteChildAccounting)?;
        admission
            .admission()
            .intent()
            .budget()
            .remaining(&total, record.event.recorded_at())
            .map_err(|_| StoreError::GraphFrameLimitExceeded)?;
        insert_event_components(&mut tx, &record.event, record.digest).await?;
        insert_checkpoint_components(
            &mut tx,
            record.checkpoint.checkpoint(),
            record.event.source(),
            Some(&record.checkpoint),
        )
        .await?;
        insert_barrier_consumption_parts(
            &mut tx,
            base,
            barrier.result_heads(),
            record.checkpoint.checkpoint(),
            record.event.source(),
        )
        .await?;
        insert_record(&mut tx, &record).await?;
        advance_head(&mut tx, base, &record.checkpoint).await?;
        update_run_head(&mut tx, &record.event, None).await?;
        revalidate_worker_after_components(&mut tx, &fence).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::database("whole frame barrier commit", e))?;
        Ok(GraphFrameBarrierCommitOutcome::Committed(record))
    }
}

async fn insert_record(
    tx: &mut Transaction<'_, Postgres>,
    record: &StoredGraphFrameBarrier,
) -> Result<(), StoreError> {
    let base = record.barrier.base_checkpoint();
    let next = record.checkpoint.checkpoint();
    let journal = record.event.head();
    query("INSERT INTO stateknot.graph_frame_barriers (tenant_id,run_id,graph_namespace,frame_identity_digest,base_checkpoint_id,base_superstep,base_checkpoint_digest,base_frame_checkpoint_digest,successor_checkpoint_id,successor_superstep,successor_checkpoint_digest,successor_frame_checkpoint_digest,result_count,barrier_intent_digest,scope_intent_digest,compound_digest,barrier_bytes,journal_sequence,journal_event_id,journal_recorded_at,journal_digest) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)")
        .bind(next.tenant_id().as_str()).bind(*next.run_id().as_uuid()).bind(record.checkpoint.frame().namespace().as_str()).bind(record.checkpoint.frame().digest().as_bytes())
        .bind(*base.checkpoint().checkpoint_id().as_uuid()).bind(i64::try_from(base.checkpoint().superstep().get()).map_err(|_|StoreError::GraphFrameLimitExceeded)?)
        .bind(base.checkpoint().digest().as_bytes()).bind(base.digest().as_bytes())
        .bind(*next.checkpoint_id().as_uuid()).bind(i64::try_from(next.superstep().get()).map_err(|_|StoreError::GraphFrameLimitExceeded)?)
        .bind(next.digest().as_bytes()).bind(record.checkpoint.digest().as_bytes())
        .bind(i32::try_from(record.barrier.result_heads().len()).map_err(|_|StoreError::GraphFrameLimitExceeded)?)
        .bind(record.barrier.intent_digest().as_bytes()).bind(record.scope_intent_digest.as_bytes()).bind(record.digest.as_bytes()).bind(encode(record)?)
        .bind(i64::try_from(journal.sequence().get()).map_err(|_|StoreError::JournalSequenceExhausted)?).bind(*journal.event_id().as_uuid())
        .bind(to_database_time(journal.recorded_at())?).bind(journal.digest().as_bytes()).execute(&mut **tx).await
        .map_err(|e|StoreError::database("frame barrier insert",e))?;
    Ok(())
}

async fn advance_head(
    tx: &mut Transaction<'_, Postgres>,
    base: &CheckpointHead,
    next: &GraphFrameCheckpoint,
) -> Result<(), StoreError> {
    let cp = next.checkpoint();
    let changed=query("UPDATE stateknot.graph_frame_heads SET checkpoint_id=$5,superstep=$6,checkpoint_digest=$7,frame_checkpoint_digest=$8 WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3 AND frame_identity_digest=$4 AND checkpoint_id=$9 AND superstep=$10 AND checkpoint_digest=$11")
        .bind(cp.tenant_id().as_str()).bind(*cp.run_id().as_uuid()).bind(next.frame().namespace().as_str()).bind(next.frame().digest().as_bytes())
        .bind(*cp.checkpoint_id().as_uuid()).bind(i64::try_from(cp.superstep().get()).map_err(|_|StoreError::GraphFrameLimitExceeded)?).bind(cp.digest().as_bytes()).bind(next.digest().as_bytes())
        .bind(*base.checkpoint_id().as_uuid()).bind(i64::try_from(base.superstep().get()).map_err(|_|StoreError::GraphFrameLimitExceeded)?).bind(base.digest().as_bytes())
        .execute(&mut **tx).await.map_err(|e|StoreError::database("frame barrier head advance",e))?.rows_affected();
    if changed != 1 {
        return Err(StoreError::StaleCheckpointHead);
    }
    Ok(())
}
