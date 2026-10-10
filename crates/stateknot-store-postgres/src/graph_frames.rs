// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Compound frame entry storage. Only verified bundles cross this boundary.
#[allow(clippy::wildcard_imports)]
use super::*;
use stateknot_core::{
    GraphFrameCheckpoint, GraphFrameCheckpointHead, GraphFrameEntry, GraphFrameIdentity,
};

#[path = "graph_frames/active.rs"]
mod active;
pub use active::{StoredActiveGraphFrame, StoredOpenGraphFrame};

pub(super) async fn verified_active_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
    stored: StoredRun,
) -> Result<Option<StoredActiveGraphFrame>, StoreError> {
    Box::pin(active::verified_snapshot(tx, tenant, run, stored)).await
}

#[path = "graph_frames/closures.rs"]
mod closures;
pub use closures::{GraphFrameClosureCommitOutcome, StoredGraphFrameClosure};
pub(super) async fn closed_completion(
    tx: &mut Transaction<'_, Postgres>,
    start: &NodeAttemptStart,
) -> Result<Option<NodeAttemptCompletion>, StoreError> {
    Box::pin(closures::load_completion(tx, start)).await
}

#[path = "graph_frames/barriers.rs"]
mod barriers;
pub use barriers::{GraphFrameBarrierCommitOutcome, StoredGraphFrameBarrier};

#[path = "graph_frames/caller_bindings.rs"]
mod caller_bindings;

#[path = "graph_frames/replay.rs"]
mod replay;
use replay::Replay;
#[path = "graph_frames/returns.rs"]
mod returns;
pub(super) async fn recognize_completion(
    tx: &mut Transaction<'_, Postgres>,
    attempt: &NodeAttempt,
) -> Result<JournalEvent, StoreError> {
    if let Some(event) = Box::pin(closures::recognize_completion(tx, attempt)).await? {
        return Ok(event);
    }
    Box::pin(returns::recognize_completion(tx, attempt)).await
}
pub use returns::{GraphFrameReturnCommitOutcome, StoredGraphFrameReturn};

pub(super) async fn recognize_result(
    tx: &mut Transaction<'_, Postgres>,
    result: &PendingNodeResult,
) -> Result<Option<JournalEvent>, StoreError> {
    Box::pin(returns::recognize_result_replay(
        &mut Replay::new(tx),
        result,
    ))
    .await
}

pub(super) fn is_framework_kind(kind: &str) -> bool {
    caller_bindings::is_framework_kind(kind) || kind == "graph-frame-returned"
}

const SCOPE_DOMAIN: &[u8] = b"stateknot-postgres-frame-entry-scope-v1\0";
const COMPOUND_DOMAIN: &[u8] = b"stateknot-postgres-frame-entry-compound-v1\0";
const MAX_ENTRY_BYTES: usize = 65_536;

/// An authenticated immutable entry, including its complete compound journal binding.
#[derive(Clone, Debug)]
pub struct StoredGraphFrameEntry {
    event: JournalEvent,
    entry: GraphFrameEntry,
    scope: EntryScope,
    budget: EntryBudget,
    digest: Digest,
}
impl StoredGraphFrameEntry {
    /// Returns the one journal fact shared by the framework start and checkpoint.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        &self.event
    }
    /// Returns the fully verified framework start and initial scoped checkpoint.
    #[must_use]
    pub const fn entry(&self) -> &GraphFrameEntry {
        &self.entry
    }
    /// Returns the whole `PostgreSQL` compound projection digest.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
    /// Returns authenticated direct usage immediately after this entry.
    /// Later node/provider usage must be accumulated before another mutation.
    ///
    /// # Errors
    /// Rejects counter overflow or canonical encoding failure.
    pub fn direct_usage_after(&self) -> Result<BudgetUsage, StoreError> {
        charged_usage(&self.budget, &self.entry, &self.event)
    }
    /// Returns the Run's durable lifetime frame ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> u16 {
        self.scope.ordinal
    }
}

/// A retry never grants permission to launch another logical frame.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum GraphFrameEntryCommitOutcome {
    /// Every entry component committed in one transaction.
    Committed(StoredGraphFrameEntry),
    /// The existing whole bundle was reloaded and authenticated.
    Idempotent(StoredGraphFrameEntry),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EntryScope {
    admission_digest: Digest,
    ordinal: u16,
    maximum_depth: u8,
    maximum_frame_starts: u16,
    parent_namespace: GraphNamespace,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EntryWire {
    version: u8,
    scope: EntryScope,
    budget: EntryBudget,
    scope_intent_digest: Digest,
    core_intent_digest: Digest,
    initial_checkpoint: GraphFrameCheckpointHead,
    core_record_digest: Digest,
    compound_digest: Digest,
}
struct EntryRow {
    graph_namespace: String,
    frame_identity_digest: Vec<u8>,
    ordinal: i32,
    parent_namespace: String,
    parent_checkpoint_id: Uuid,
    caller_attempt_id: Uuid,
    initial_checkpoint_id: Uuid,
    initial_checkpoint_digest: Vec<u8>,
    core_entry_digest: Vec<u8>,
    compound_digest: Vec<u8>,
    entry_bytes: Vec<u8>,
    entry_checksum: Vec<u8>,
    journal_sequence: i64,
    journal_event_id: Uuid,
    journal_recorded_at: DateTime<Utc>,
    journal_digest: Vec<u8>,
}
struct StackRow {
    admission_digest: Vec<u8>,
    lifetime_starts: i32,
    active_namespace: String,
    active_frame_identity_digest: Option<Vec<u8>>,
}

fn digest_wire(domain: &[u8], wire: &impl Serialize) -> Result<Digest, StoreError> {
    let canonical = serde_json_canonicalizer::to_vec(wire)
        .map_err(|_| StoreError::encoding("frame compound"))?;
    let mut preimage = Vec::with_capacity(domain.len() + canonical.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}
fn scope_intent(
    plan: &GraphFrameEntryPlan,
    scope: &EntryScope,
    budget: &EntryBudget,
) -> Result<Digest, StoreError> {
    #[derive(Serialize)]
    struct Wire<'a> {
        core_intent_digest: Digest,
        scope: &'a EntryScope,
        budget: &'a EntryBudget,
    }
    digest_wire(
        SCOPE_DOMAIN,
        &Wire {
            core_intent_digest: plan.intent_digest(),
            scope,
            budget,
        },
    )
}
fn compound_digest(
    scope_intent_digest: Digest,
    core_record_digest: Digest,
) -> Result<Digest, StoreError> {
    #[derive(Serialize)]
    struct Wire {
        scope_intent_digest: Digest,
        core_record_digest: Digest,
    }
    digest_wire(
        COMPOUND_DOMAIN,
        &Wire {
            scope_intent_digest,
            core_record_digest,
        },
    )
}
fn encode_entry(
    record: &StoredGraphFrameEntry,
    plan: &GraphFrameEntryPlan,
) -> Result<Vec<u8>, StoreError> {
    let wire = EntryWire {
        version: 1,
        scope: record.scope.clone(),
        budget: record.budget.clone(),
        scope_intent_digest: scope_intent(plan, &record.scope, &record.budget)?,
        core_intent_digest: plan.intent_digest(),
        initial_checkpoint: record.entry.checkpoint().head(),
        core_record_digest: record.entry.digest(),
        compound_digest: record.digest,
    };
    let bytes =
        serde_json_canonicalizer::to_vec(&wire).map_err(|_| StoreError::encoding("frame entry"))?;
    if bytes.is_empty() || bytes.len() > MAX_ENTRY_BYTES {
        return Err(StoreError::encoding("frame entry size"));
    }
    Ok(bytes)
}
fn decode_entry_wire(row: &EntryRow) -> Result<EntryWire, StoreError> {
    if row.entry_bytes.is_empty()
        || row.entry_bytes.len() > MAX_ENTRY_BYTES
        || Digest::sha256(&row.entry_bytes).as_bytes() != row.entry_checksum.as_slice()
    {
        return Err(StoreError::corrupt("frame entry bytes"));
    }
    let value = BoundedJson::from_slice(&row.entry_bytes)
        .map_err(|_| StoreError::corrupt("frame entry JSON"))?
        .into_value();
    if !value.is_object()
        || (!value.get("scope").is_some_and(serde_json::Value::is_object)
            || !value
                .get("budget")
                .is_some_and(serde_json::Value::is_object))
    {
        return Err(StoreError::corrupt("frame entry object shape"));
    }
    let wire: EntryWire =
        serde_json::from_value(value).map_err(|_| StoreError::corrupt("frame entry value"))?;
    if wire.version != 1
        || wire.scope.ordinal == 0
        || wire.scope.ordinal > 4096
        || wire.scope.maximum_depth == 0
        || wire.scope.maximum_depth > 7
        || wire.scope.maximum_frame_starts == 0
        || wire.scope.maximum_frame_starts > 4096
        || wire.scope.ordinal > wire.scope.maximum_frame_starts
        || serde_json_canonicalizer::to_vec(&wire).ok().as_deref()
            != Some(row.entry_bytes.as_slice())
    {
        return Err(StoreError::corrupt("frame entry canonical projection"));
    }
    let frame = wire.initial_checkpoint.frame();
    let base = frame.origin().base_checkpoint();
    if frame.namespace().as_str() != row.graph_namespace
        || frame.digest() != decode_digest(&row.frame_identity_digest, "frame identity")?
        || i32::from(wire.scope.ordinal) != row.ordinal
        || wire.scope.parent_namespace.as_str() != row.parent_namespace
        || frame.origin().graph_namespace() != &wire.scope.parent_namespace
        || *base.checkpoint_id().as_uuid() != row.parent_checkpoint_id
        || *wire
            .initial_checkpoint
            .checkpoint()
            .checkpoint_id()
            .as_uuid()
            != row.initial_checkpoint_id
        || wire.initial_checkpoint.checkpoint().digest()
            != decode_digest(&row.initial_checkpoint_digest, "frame initial checkpoint")?
        || wire.core_record_digest != decode_digest(&row.core_entry_digest, "frame Core compound")?
        || wire.compound_digest != decode_digest(&row.compound_digest, "frame storage compound")?
    {
        return Err(StoreError::corrupt("frame entry columns"));
    }
    Ok(wire)
}

fn decode_frame_checkpoint(row: CheckpointRow) -> Result<GraphFrameCheckpoint, StoreError> {
    let namespace = row.graph_namespace.clone();
    let identity = decode_digest(
        row.frame_identity_digest
            .as_deref()
            .ok_or_else(|| StoreError::corrupt("frame checkpoint identity"))?,
        "frame checkpoint identity",
    )?;
    let digest = decode_digest(
        row.frame_checkpoint_digest
            .as_deref()
            .ok_or_else(|| StoreError::corrupt("frame checkpoint digest"))?,
        "frame checkpoint digest",
    )?;
    let bytes = row
        .frame_checkpoint_head_bytes
        .as_deref()
        .ok_or_else(|| StoreError::corrupt("frame checkpoint header"))?;
    if namespace.is_empty() || bytes.is_empty() || bytes.len() > MAX_ENTRY_BYTES {
        return Err(StoreError::corrupt("frame checkpoint header size"));
    }
    let head: GraphFrameCheckpointHead = serde_json::from_slice(bytes)
        .map_err(|_| StoreError::corrupt("frame checkpoint header value"))?;
    if serde_json_canonicalizer::to_vec(&head).ok().as_deref() != Some(bytes)
        || head.frame().namespace().as_str() != namespace
        || head.frame().digest() != identity
        || head.digest() != digest
    {
        return Err(StoreError::corrupt("frame checkpoint header projection"));
    }
    let frame = head.frame().clone();
    let checkpoint = decode_checkpoint_components(row)?;
    let record = GraphFrameCheckpoint::new(frame, checkpoint)
        .map_err(|_| StoreError::corrupt("frame checkpoint binding"))?;
    if record.head() != head {
        return Err(StoreError::corrupt("frame checkpoint complete head"));
    }
    Ok(record)
}

async fn entry_row(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
    namespace: &GraphNamespace,
) -> Result<Option<EntryRow>, StoreError> {
    query_as::<_,EntryRow>("SELECT graph_namespace,frame_identity_digest,ordinal,parent_namespace,parent_checkpoint_id,caller_attempt_id,initial_checkpoint_id,initial_checkpoint_digest,core_entry_digest,compound_digest,entry_bytes,entry_checksum,journal_sequence,journal_event_id,journal_recorded_at,journal_digest FROM stateknot.graph_frame_entries WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3")
        .bind(tenant.as_str()).bind(*run.as_uuid()).bind(namespace.as_str()).fetch_optional(&mut **tx).await
        .map_err(|e|StoreError::database("frame entry load",e))
}
async fn checkpoint_row(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
    namespace: &GraphNamespace,
    checkpoint: CheckpointId,
) -> Result<CheckpointRow, StoreError> {
    // The legacy query retains its explicit root predicate. This private query
    // selects one exact non-root scope and still decodes every legacy column.
    let statement = SELECT_CHECKPOINT_BY_ID.replace("graph_namespace = ''", "graph_namespace = $4");
    query_as::<_, CheckpointRow>(&statement)
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .bind(*checkpoint.as_uuid())
        .bind(namespace.as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| StoreError::database("scoped checkpoint load", e))?
        .ok_or_else(|| StoreError::corrupt("frame checkpoint missing"))
}
async fn graph(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    reference: &GraphReference,
) -> Result<CompiledGraph, StoreError> {
    let row = load_graph_definition_row(tx, tenant, reference)
        .await?
        .ok_or(StoreError::GraphDefinitionNotFound)?;
    let stored = decode_graph_definition(row)?;
    if &stored.graph().reference() != reference {
        return Err(StoreError::GraphDefinitionConflict);
    }
    Ok(stored.graph().clone())
}

async fn closure(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    root: &GraphReference,
) -> Result<BTreeMap<Digest, CompiledGraph>, StoreError> {
    let mut pending = vec![root.clone()];
    let mut graphs = BTreeMap::new();
    let mut bytes = 0_usize;
    while let Some(reference) = pending.pop() {
        if let Some(existing) = graphs.get(&reference.definition_digest()) {
            let existing: &CompiledGraph = existing;
            if existing.reference() != reference {
                return Err(StoreError::GraphDefinitionConflict);
            }
            continue;
        }
        if graphs.len() == 1024 {
            return Err(StoreError::GraphFrameRejected);
        }
        let compiled = graph(tx, tenant, &reference).await?;
        bytes = bytes
            .checked_add(encode_graph_definition(&compiled)?.len())
            .ok_or(StoreError::GraphFrameRejected)?;
        if bytes > 16 * 1024 * 1024 {
            return Err(StoreError::GraphFrameRejected);
        }
        if let Some(policy) = compiled.frame_calls() {
            // Distinct definitions are queued once: repeated DAG edges cannot
            // allocate an unbounded work queue before the byte/graph limits.
            for call in policy.calls() {
                if !graphs.contains_key(&call.target().definition_digest())
                    && !pending.contains(call.target())
                {
                    if pending.len() + graphs.len() >= 1024 {
                        return Err(StoreError::GraphFrameRejected);
                    }
                    pending.push(call.target().clone());
                }
            }
        }
        graphs.insert(reference.definition_digest(), compiled);
    }
    let mut heights = BTreeMap::new();
    for _ in 0..=7 {
        for (digest, compiled) in &graphs {
            if heights.contains_key(digest) {
                continue;
            }
            let mut height = 0_u8;
            let mut ready = true;
            if let Some(policy) = compiled.frame_calls() {
                for call in policy.calls() {
                    let target = graphs
                        .get(&call.target().definition_digest())
                        .ok_or(StoreError::GraphFrameRejected)?;
                    call.validate_target(target)
                        .map_err(|_| StoreError::GraphFrameRejected)?;
                    let Some(target_height) =
                        heights.get(&call.target().definition_digest()).copied()
                    else {
                        ready = false;
                        break;
                    };
                    height = height.max(target_height + 1);
                }
                if height > policy.maximum_depth() {
                    return Err(StoreError::GraphFrameRejected);
                }
            }
            if ready {
                heights.insert(*digest, height);
            }
        }
    }
    if heights.len() != graphs.len() {
        return Err(StoreError::GraphFrameRejected);
    }
    Ok(graphs)
}
fn inherited_scope(
    admission: &AgentAdmission,
    parent: &CompiledGraph,
    frame: &GraphFrameIdentity,
    ancestor: Option<&EntryScope>,
    ordinal: u16,
) -> Result<EntryScope, StoreError> {
    let policy = parent.frame_calls().ok_or(StoreError::GraphFrameRejected)?;
    let parent_depth = if frame.origin().graph_namespace().is_root() {
        0
    } else {
        frame.origin().graph_namespace().as_str().split('/').count()
    };
    let depth = parent_depth + 1;
    let root_max = admission
        .intent()
        .budget()
        .graph_depth()
        .get()
        .saturating_sub(1)
        .min(7) as u8;
    let inherited_max = ancestor.map_or(root_max, |scope| scope.maximum_depth.min(root_max));
    let maximum_depth = inherited_max.min(
        u8::try_from(parent_depth)
            .map_err(|_| StoreError::GraphFrameRejected)?
            .saturating_add(policy.maximum_depth()),
    );
    let maximum_frame_starts = ancestor.map_or(policy.maximum_frame_starts(), |scope| {
        scope
            .maximum_frame_starts
            .min(policy.maximum_frame_starts())
    });
    if depth > usize::from(maximum_depth) || ordinal == 0 || ordinal > maximum_frame_starts {
        return Err(StoreError::GraphFrameLimitExceeded);
    }
    Ok(EntryScope {
        admission_digest: admission.digest(),
        ordinal,
        maximum_depth,
        maximum_frame_starts,
        parent_namespace: frame.origin().graph_namespace().clone(),
    })
}

// A verified base carries actual state. Compact heads alone do not authorize a call.
enum Parent {
    Root(Box<Checkpoint>),
    Frame(Box<GraphFrameCheckpoint>),
}
impl Parent {
    fn checkpoint(&self) -> &Checkpoint {
        match self {
            Self::Root(cp) => cp,
            Self::Frame(cp) => cp.checkpoint(),
        }
    }
    fn prepare(
        &self,
        parent: &CompiledGraph,
        target: &CompiledGraph,
        frame: &GraphFrameIdentity,
        checkpoint_id: CheckpointId,
        attempt: AttemptId,
        fence: RunFence,
    ) -> Result<GraphFrameEntryPlan, StoreError> {
        let call = parent
            .frame_calls()
            .and_then(|p| p.call(frame.origin().node_id()))
            .ok_or(StoreError::GraphFrameRejected)?;
        let plan = match self {
            Self::Root(cp) => GraphFrameEntryPlan::for_root(
                call,
                parent,
                cp,
                target,
                checkpoint_id,
                attempt,
                fence,
            ),
            Self::Frame(cp) => GraphFrameEntryPlan::for_frame(
                call,
                parent,
                cp,
                target,
                checkpoint_id,
                attempt,
                fence,
            ),
        }
        .map_err(|_| StoreError::GraphFrameRejected)?;
        if plan.frame() != frame {
            return Err(StoreError::GraphFrameConflict);
        }
        Ok(plan)
    }
}

#[allow(clippy::too_many_lines)]
async fn verified_entry(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
    namespace: &GraphNamespace,
    admission: &StoredAgentAdmission,
    graphs: &BTreeMap<Digest, CompiledGraph>,
) -> Result<StoredGraphFrameEntry, StoreError> {
    let mut replay = Replay::new(tx);
    Box::pin(verified_entry_replay(
        &mut replay,
        tenant,
        run,
        namespace,
        admission,
        graphs,
    ))
    .await
}

#[allow(clippy::too_many_lines)] // One root-to-leaf authentication chain.
async fn verified_entry_replay(
    replay: &mut Replay<'_, '_>,
    tenant: &TenantId,
    run: RunId,
    namespace: &GraphNamespace,
    admission: &StoredAgentAdmission,
    graphs: &BTreeMap<Digest, CompiledGraph>,
) -> Result<StoredGraphFrameEntry, StoreError> {
    // Load at most seven compact records first, then authenticate root-to-leaf.
    // Full state buffers are released at each edge; no recursive future owns
    // an entire stack of maximum-sized checkpoints.
    let mut chain = Vec::with_capacity(7);
    let mut next = namespace.clone();
    while !next.is_root() {
        if chain.len() == 7 {
            return Err(StoreError::corrupt("frame ancestor depth"));
        }
        let row = entry_row(replay.tx, tenant, run, &next)
            .await?
            .ok_or(StoreError::GraphFrameNotFound)?;
        let wire = decode_entry_wire(&row)?;
        next = wire.scope.parent_namespace.clone();
        chain.push((row, wire));
    }
    let through = chain
        .first()
        .and_then(|(row, _)| row.journal_sequence.checked_sub(1))
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| StoreError::corrupt("frame replay entry observation"))?;
    Box::pin(returns::usage_floor_before(
        replay,
        admission,
        graphs,
        through,
        &admission_usage_floor(admission)?,
    ))
    .await?;
    let mut last: Option<StoredGraphFrameEntry> = None;
    for (row, wire) in chain.into_iter().rev() {
        let frame = wire.initial_checkpoint.frame();
        if frame.origin().tenant_id() != tenant || frame.origin().run_id() != run {
            return Err(StoreError::corrupt("frame tenant/run"));
        }
        let base = frame.origin().base_checkpoint();
        let (base_parent, usage_floor) = if frame.origin().graph_namespace().is_root() {
            let cp = decode_checkpoint(
                checkpoint_row(
                    replay.tx,
                    tenant,
                    run,
                    &GraphNamespace::root(),
                    base.checkpoint_id(),
                )
                .await?,
            )?;
            verify_checkpoint_anchor(replay.tx, &cp).await?;
            (
                Parent::Root(Box::new(cp)),
                admission_usage_floor(admission)?,
            )
        } else {
            let ancestor = last
                .as_ref()
                .ok_or_else(|| StoreError::corrupt("frame parent continuation"))?;
            if ancestor.entry.checkpoint().frame().namespace() != frame.origin().graph_namespace() {
                return Err(StoreError::corrupt("frame parent namespace"));
            }
            let (cp, floor) = Box::pin(barriers::checkpoint_at_replay(
                replay, ancestor, admission, graphs, base,
            ))
            .await?;
            let through = u64::try_from(
                row.journal_sequence
                    .checked_sub(1)
                    .ok_or_else(|| StoreError::corrupt("frame ancestor observation"))?,
            )
            .map_err(|_| StoreError::corrupt("frame ancestor sequence"))?;
            let floor = caller_bindings::usage_floor_before(
                replay.tx, ancestor, admission, through, &floor,
            )
            .await?;
            (Parent::Frame(Box::new(cp)), floor)
        };
        if base_parent.checkpoint().head() != *base {
            return Err(StoreError::corrupt("frame parent head"));
        }
        let parent_graph = graphs
            .get(&base_parent.checkpoint().graph().definition_digest())
            .ok_or(StoreError::GraphFrameRejected)?;
        let target = graphs
            .get(&frame.target().definition_digest())
            .ok_or(StoreError::GraphFrameRejected)?;
        let expected_scope = inherited_scope(
            admission.admission(),
            parent_graph,
            frame,
            last.as_ref().map(|entry| &entry.scope),
            wire.scope.ordinal,
        )?;
        if wire.scope != expected_scope
            || last
                .as_ref()
                .is_some_and(|entry| entry.scope.ordinal >= wire.scope.ordinal)
        {
            return Err(StoreError::corrupt("frame inherited scope"));
        }
        let mut event_row = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
            .bind(tenant.as_str())
            .bind(*run.as_uuid())
            .bind(row.journal_sequence)
            .fetch_optional(&mut **replay.tx)
            .await
            .map_err(|e| StoreError::database("frame entry event", e))?
            .ok_or_else(|| StoreError::corrupt("frame entry event"))?;
        let projection = event_row
            .projection_digest
            .take()
            .map(|p| decode_digest(&p, "frame event compound"))
            .transpose()?;
        let event = decode_event(event_row)?;
        if event.head().event_id().as_uuid() != &row.journal_event_id
            || event.recorded_at() != from_database_time(row.journal_recorded_at)?
            || event.digest() != decode_digest(&row.journal_digest, "frame journal")?
            || projection != Some(wire.compound_digest)
        {
            return Err(StoreError::corrupt("frame event anchor"));
        }
        let fence = event
            .source()
            .worker_fence()
            .ok_or_else(|| StoreError::corrupt("frame event worker"))?
            .clone();
        let attempt = AttemptId::from_uuid(row.caller_attempt_id)
            .map_err(|_| StoreError::corrupt("frame caller attempt"))?;
        let plan = base_parent.prepare(
            parent_graph,
            target,
            frame,
            wire.initial_checkpoint.checkpoint().checkpoint_id(),
            attempt,
            fence,
        )?;
        let predecessor_sequence = event
            .sequence()
            .get()
            .checked_sub(1)
            .ok_or_else(|| StoreError::corrupt("frame predecessor"))?;
        let predecessor = decode_event(
            query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
                .bind(tenant.as_str())
                .bind(*run.as_uuid())
                .bind(
                    i64::try_from(predecessor_sequence)
                        .map_err(|_| StoreError::corrupt("frame predecessor"))?,
                )
                .fetch_optional(&mut **replay.tx)
                .await
                .map_err(|e| StoreError::database("frame predecessor", e))?
                .ok_or_else(|| StoreError::corrupt("frame predecessor"))?,
        )?;
        let start = load_node_attempt_record(replay.tx, tenant, &run, attempt)
            .await?
            .ok_or_else(|| StoreError::corrupt("frame caller start"))?;
        returns::verify_completion_anchor(replay.tx, &start).await?;
        let checkpoint = decode_frame_checkpoint(
            checkpoint_row(
                replay.tx,
                tenant,
                run,
                frame.namespace(),
                wire.initial_checkpoint.checkpoint().checkpoint_id(),
            )
            .await?,
        )?;
        let entry = plan
            .verify_committed(
                &predecessor.head(),
                &event,
                start.start(),
                &checkpoint,
                wire.core_record_digest,
            )
            .map_err(|_| StoreError::corrupt("frame Core compound"))?;
        if wire.core_intent_digest != plan.intent_digest()
            || wire.scope_intent_digest != scope_intent(&plan, &wire.scope, &wire.budget)?
            || wire.compound_digest != compound_digest(wire.scope_intent_digest, entry.digest())?
            || entry.checkpoint().head() != wire.initial_checkpoint
        {
            return Err(StoreError::corrupt("frame storage compound"));
        }
        if wire.budget.observed_head != predecessor.head() {
            return Err(StoreError::corrupt("frame budget observation"));
        }
        let usage_floor = Box::pin(returns::usage_floor_before(
            replay,
            admission,
            graphs,
            predecessor_sequence,
            &usage_floor,
        ))
        .await?;
        wire.budget
            .direct_usage
            .validate_monotonic_after(&usage_floor)
            .map_err(|_| StoreError::corrupt("frame ancestor or Root usage regression"))?;
        let direct_after = charged_usage(&wire.budget, &entry, &event)?;
        let total = direct_after
            .checked_accumulate(&wire.budget.delegated_usage)
            .map_err(|_| StoreError::corrupt("frame budget accounting"))?;
        admission
            .admission()
            .intent()
            .budget()
            .remaining(&total, event.recorded_at())
            .map_err(|_| StoreError::corrupt("frame budget limits"))?;
        last = Some(StoredGraphFrameEntry {
            event,
            entry,
            scope: wire.scope,
            budget: wire.budget,
            digest: wire.compound_digest,
        });
    }
    last.ok_or(StoreError::GraphFrameNotFound)
}

impl PostgresStore {
    /// Reloads a complete immutable entry and its bounded authenticated ancestry.
    ///
    /// # Errors
    /// Returns explicit missing, scope, pin, canonical-byte or compound failures.
    pub async fn load_graph_frame_entry(
        &self,
        tenant: &TenantId,
        run: RunId,
        namespace: &GraphNamespace,
    ) -> Result<StoredGraphFrameEntry, StoreError> {
        Box::pin(self.load_graph_frame_entry_inner(tenant, run, namespace))
            .await?
            .ok_or(StoreError::GraphFrameNotFound)
    }

    async fn load_graph_frame_entry_inner(
        &self,
        tenant: &TenantId,
        run: RunId,
        namespace: &GraphNamespace,
    ) -> Result<Option<StoredGraphFrameEntry>, StoreError> {
        if namespace.is_root() {
            return Ok(None);
        }
        let mut tx = self.begin_repeatable_read("frame entry snapshot").await?;
        let exists = query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM stateknot.graph_frame_entries WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3)"
        ).bind(tenant.as_str()).bind(*run.as_uuid()).bind(namespace.as_str())
            .fetch_one(&mut *tx).await
            .map_err(|source| StoreError::database("frame entry snapshot lookup", source))?;
        if !exists {
            tx.commit().await.map_err(|source| {
                StoreError::database("empty frame entry snapshot commit", source)
            })?;
            return Ok(None);
        }
        let run_row = query_as::<_, RunRow>(SELECT_RUN)
            .bind(tenant.as_str())
            .bind(*run.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StoreError::database("frame run snapshot", e))?
            .ok_or(StoreError::RunNotFound)?;
        let admission_row = load_agent_admission_row(&mut tx, tenant, run)
            .await?
            .ok_or(StoreError::AgentAdmissionConflict)?;
        let admission =
            verify_stored_agent_admission(&mut tx, decode_run(run_row)?, admission_row).await?;
        let graphs = closure(&mut tx, tenant, admission.admission().intent().graph()).await?;
        let record = verified_entry(&mut tx, tenant, run, namespace, &admission, &graphs).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::database("frame entry snapshot commit", e))?;
        Ok(Some(record))
    }
}

impl<'row> FromRow<'row, PgRow> for EntryRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            graph_namespace: row.try_get("graph_namespace")?,
            frame_identity_digest: row.try_get("frame_identity_digest")?,
            ordinal: row.try_get("ordinal")?,
            parent_namespace: row.try_get("parent_namespace")?,
            parent_checkpoint_id: row.try_get("parent_checkpoint_id")?,
            caller_attempt_id: row.try_get("caller_attempt_id")?,
            initial_checkpoint_id: row.try_get("initial_checkpoint_id")?,
            initial_checkpoint_digest: row.try_get("initial_checkpoint_digest")?,
            core_entry_digest: row.try_get("core_entry_digest")?,
            compound_digest: row.try_get("compound_digest")?,
            entry_bytes: row.try_get("entry_bytes")?,
            entry_checksum: row.try_get("entry_checksum")?,
            journal_sequence: row.try_get("journal_sequence")?,
            journal_event_id: row.try_get("journal_event_id")?,
            journal_recorded_at: row.try_get("journal_recorded_at")?,
            journal_digest: row.try_get("journal_digest")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for StackRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            admission_digest: row.try_get("admission_digest")?,
            lifetime_starts: row.try_get("lifetime_starts")?,
            active_namespace: row.try_get("active_namespace")?,
            active_frame_identity_digest: row.try_get("active_frame_identity_digest")?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EntryBudget {
    observed_head: JournalHead,
    direct_usage: BudgetUsage,
    delegated_usage: BudgetUsage,
    child_account_digest: Option<Digest>,
}

fn admission_usage_floor(admission: &StoredAgentAdmission) -> Result<BudgetUsage, StoreError> {
    use stateknot_core::{ByteCount, ExecutionCount};
    let event_bytes = serde_json_canonicalizer::to_vec(admission.event())
        .map_err(|_| StoreError::encoding("frame root admission event"))?
        .len();
    let checkpoint_bytes = encode_checkpoint(admission.checkpoint())?.len();
    BudgetUsage::builder()
        .graph_depth(ExecutionCount::new(1))
        .event_bytes(ByteCount::new(
            u64::try_from(event_bytes).map_err(|_| StoreError::GraphFrameLimitExceeded)?,
        ))
        .checkpoint_bytes(ByteCount::new(
            u64::try_from(checkpoint_bytes).map_err(|_| StoreError::GraphFrameLimitExceeded)?,
        ))
        .build()
        .map_err(|_| StoreError::GraphFrameRejected)
}

fn charged_usage(
    budget: &EntryBudget,
    entry: &GraphFrameEntry,
    event: &JournalEvent,
) -> Result<BudgetUsage, StoreError> {
    use stateknot_core::{ByteCount, ExecutionCount};
    let checkpoint_bytes = encode_checkpoint(entry.checkpoint().checkpoint())?
        .len()
        .checked_add(
            serde_json_canonicalizer::to_vec(&entry.checkpoint().head())
                .map_err(|_| StoreError::encoding("frame head"))?
                .len(),
        )
        .ok_or(StoreError::GraphFrameLimitExceeded)?;
    let event_bytes = serde_json_canonicalizer::to_vec(event)
        .map_err(|_| StoreError::encoding("frame event bytes"))?
        .len();
    let depth = entry
        .checkpoint()
        .frame()
        .namespace()
        .as_str()
        .split('/')
        .count()
        + 1;
    let delta = BudgetUsage::builder()
        .graph_depth(ExecutionCount::new(
            u64::try_from(depth).map_err(|_| StoreError::GraphFrameLimitExceeded)?,
        ))
        .graph_steps(ExecutionCount::new(1))
        .checkpoint_bytes(ByteCount::new(
            u64::try_from(checkpoint_bytes).map_err(|_| StoreError::GraphFrameLimitExceeded)?,
        ))
        .event_bytes(ByteCount::new(
            u64::try_from(event_bytes).map_err(|_| StoreError::GraphFrameLimitExceeded)?,
        ))
        .build()
        .map_err(|_| StoreError::GraphFrameRejected)?;
    budget
        .direct_usage
        .checked_accumulate(&delta)
        .map_err(|_| StoreError::GraphFrameLimitExceeded)
}

impl PostgresStore {
    /// Commits one declared frame under the existing Run's admission and budget.
    ///
    /// `direct_usage` is a trusted, complete Run-wide DIRECT-only observation at
    /// `observed`: it includes previous frame entry charges and excludes child
    /// Run subtree charges. The locked store reloads the actual child account,
    /// rejects outstanding owned children, and adds settled delegated usage.
    /// Recovery authenticates existing evidence before any schema callback.
    /// Fresh schema callbacks finish before the mutation transaction, which repeats
    /// the actual graph closure, caller base, active leaf, inherited ceilings,
    /// journal expectation and live database-clock fence. Retries authenticate
    /// the original whole bundle before testing fresh lease or readiness.
    ///
    /// # Errors
    /// Returns explicit scope, immutable intent, pin, budget, fence or SQL errors.
    pub async fn enter_graph_frame<V: GraphSchemaValidator + ?Sized>(
        &self,
        plan: GraphFrameEntryPlan,
        event_id: EventId,
        observed: JournalHead,
        direct_usage: BudgetUsage,
        schemas: &V,
    ) -> Result<GraphFrameEntryCommitOutcome, StoreError> {
        Box::pin(self.enter_graph_frame_inner(plan, event_id, observed, direct_usage, schemas))
            .await
    }

    #[allow(clippy::too_many_lines)]
    async fn enter_graph_frame_inner<V: GraphSchemaValidator + ?Sized>(
        &self,
        plan: GraphFrameEntryPlan,
        event_id: EventId,
        observed: JournalHead,
        direct_usage: BudgetUsage,
        schemas: &V,
    ) -> Result<GraphFrameEntryCommitOutcome, StoreError> {
        let tenant = plan.frame().origin().tenant_id();
        let run = plan.frame().origin().run_id();
        // A lost acknowledgment authenticates durable evidence before consulting
        // live callbacks, a new candidate event, the current leaf or its lease.
        if let Some(record) =
            Box::pin(self.load_graph_frame_entry_inner(tenant, run, plan.frame().namespace()))
                .await?
        {
            verify_retry(&record, &plan)?;
            return Ok(GraphFrameEntryCommitOutcome::Idempotent(record));
        }
        catch_unwind(AssertUnwindSafe(|| {
            schemas.validate(
                plan.checkpoint().graph().state_schema(),
                plan.checkpoint().state().data(),
            )
        }))
        .map_err(|_| StoreError::GraphReplayDependencyUnavailable)?
        .map_err(|error| match error {
            stateknot_core::GraphSchemaValidationError::Rejected => StoreError::GraphFrameRejected,
            _ => StoreError::GraphReplayDependencyUnavailable,
        })?;
        let append = plan
            .append(event_id, observed.clone())
            .map_err(|_| StoreError::GraphFrameRejected)?;
        let mut tx = self.begin_mutation("compound frame entry").await?;
        let admission = load_locked_agent_admission(&mut tx, tenant, run)
            .await?
            .ok_or(StoreError::AgentAdmissionConflict)?;
        let graphs = closure(&mut tx, tenant, admission.admission().intent().graph()).await?;
        if entry_row(&mut tx, tenant, run, plan.frame().namespace())
            .await?
            .is_some()
        {
            let record = verified_entry(
                &mut tx,
                tenant,
                run,
                plan.frame().namespace(),
                &admission,
                &graphs,
            )
            .await?;
            verify_retry(&record, &plan)?;
            tx.commit()
                .await
                .map_err(|e| StoreError::database("frame entry retry commit", e))?;
            return Ok(GraphFrameEntryCommitOutcome::Idempotent(record));
        }
        let stored = admission.run();
        validate_runnable(stored)?;
        if stored.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::RunNotRunnable);
        }
        if stored.journal_head() != Some(&observed) {
            return Err(StoreError::StaleJournalHead);
        }
        let now = database_now(&mut tx, "frame entry authority clock").await?;
        authorize_worker(stored, plan.fence(), now)?;
        let root_floor = Box::pin(returns::usage_floor_before(
            &mut Replay::new(&mut tx),
            &admission,
            &graphs,
            observed.sequence().get(),
            &admission_usage_floor(&admission)?,
        ))
        .await?;
        direct_usage
            .validate_monotonic_after(&root_floor)
            .map_err(|_| StoreError::IncompleteChildAccounting)?;
        let stack=query_as::<_,StackRow>("SELECT admission_digest,lifetime_starts,active_namespace,active_frame_identity_digest FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2")
            .bind(tenant.as_str()).bind(*run.as_uuid()).fetch_optional(&mut *tx).await.map_err(|e|StoreError::database("frame stack",e))?;
        let (active, starts) = if let Some(stack) = &stack {
            if decode_digest(&stack.admission_digest, "frame stack admission")?
                != admission.admission().digest()
            {
                return Err(StoreError::corrupt("frame stack admission"));
            }
            let active = GraphNamespace::new(stack.active_namespace.clone())
                .map_err(|_| StoreError::corrupt("frame stack namespace"))?;
            if active.is_root() != stack.active_frame_identity_digest.is_none() {
                return Err(StoreError::corrupt("frame stack identity shape"));
            }
            (
                active,
                u16::try_from(stack.lifetime_starts)
                    .map_err(|_| StoreError::corrupt("frame stack starts"))?,
            )
        } else {
            (GraphNamespace::root(), 0)
        };
        if &active != plan.frame().origin().graph_namespace() {
            return Err(StoreError::GraphFrameConflict);
        }
        let (parent, ancestor) = if active.is_root() {
            (
                Parent::Root(Box::new(
                    load_locked_current_checkpoint(&mut tx, stored, tenant, run)
                        .await?
                        .ok_or(StoreError::StaleCheckpointHead)?,
                )),
                None,
            )
        } else {
            let record = verified_entry(&mut tx, tenant, run, &active, &admission, &graphs).await?;
            if stack
                .as_ref()
                .and_then(|s| s.active_frame_identity_digest.as_deref())
                != Some(record.entry.checkpoint().frame().digest().as_bytes())
            {
                return Err(StoreError::corrupt("active frame identity"));
            }
            let (cp, floor) = Box::pin(barriers::current_checkpoint(
                &mut tx, &record, &admission, &graphs,
            ))
            .await?;
            direct_usage
                .validate_monotonic_after(&floor)
                .map_err(|_| StoreError::IncompleteChildAccounting)?;
            (Parent::Frame(Box::new(cp)), Some(record.scope))
        };
        let parent_graph = graphs
            .get(&plan.parent_graph().definition_digest())
            .ok_or(StoreError::GraphFrameRejected)?;
        let target = graphs
            .get(&plan.frame().target().definition_digest())
            .ok_or(StoreError::GraphFrameRejected)?;
        let actual = parent.prepare(
            parent_graph,
            target,
            plan.frame(),
            plan.checkpoint().checkpoint_id(),
            plan.attempt_id(),
            plan.fence().clone(),
        )?;
        if actual != plan {
            return Err(StoreError::GraphFrameConflict);
        }
        let ordinal = starts
            .checked_add(1)
            .ok_or(StoreError::GraphFrameLimitExceeded)?;
        let scope = inherited_scope(
            admission.admission(),
            parent_graph,
            plan.frame(),
            ancestor.as_ref(),
            ordinal,
        )?;
        if count_node_attempts(&mut tx, plan.frame().origin()).await? != 0 {
            return Err(StoreError::GraphFrameConflict);
        }
        reject_reused_node_worker_attempt(&mut tx, plan.frame().origin(), plan.fence()).await?;
        let has_result=query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM stateknot.pending_node_results WHERE tenant_id=$1 AND run_id=$2 AND base_checkpoint_id=$3 AND graph_namespace=$4 AND node_id=$5 AND activation_input_digest=$6)")
            .bind(tenant.as_str()).bind(*run.as_uuid()).bind(*plan.frame().origin().base_checkpoint().checkpoint_id().as_uuid()).bind(plan.frame().origin().graph_namespace().as_str()).bind(plan.frame().origin().node_id().as_str()).bind(plan.frame().origin().input_digest().as_bytes()).fetch_one(&mut *tx).await.map_err(|e|StoreError::database("frame caller result",e))?;
        if has_result {
            return Err(StoreError::GraphFrameConflict);
        }
        let unresolved_effects = query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM stateknot.tool_invocations WHERE tenant_id=$1 AND run_id=$2 AND current_status NOT IN ('committed','failed')) OR EXISTS(SELECT 1 FROM stateknot.model_invocations WHERE tenant_id=$1 AND run_id=$2 AND current_status NOT IN ('committed','failed'))"
        ).bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(&mut *tx).await
            .map_err(|source| StoreError::database("frame unresolved effects", source))?;
        if unresolved_effects {
            return Err(StoreError::GraphFrameRejected);
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
        let event = JournalEvent::commit(append, now.max(observed.recorded_at()))
            .map_err(|e| map_event_commit_error(&e))?;
        let entry = plan
            .materialize(&observed, &event)
            .map_err(|_| StoreError::GraphFrameRejected)?;
        let direct_after = charged_usage(&budget, &entry, &event)?;
        let total = direct_after
            .checked_accumulate(&budget.delegated_usage)
            .map_err(|_| StoreError::IncompleteChildAccounting)?;
        admission
            .admission()
            .intent()
            .budget()
            .remaining(&total, event.recorded_at())
            .map_err(|_| StoreError::GraphFrameLimitExceeded)?;
        let digest = compound_digest(scope_intent(&plan, &scope, &budget)?, entry.digest())?;
        let record = StoredGraphFrameEntry {
            event,
            entry,
            scope,
            budget,
            digest,
        };
        insert_event_components(&mut tx, &record.event, record.digest).await?;
        insert_node_attempt_claim(&mut tx, record.entry.start()).await?;
        insert_node_attempt_start(&mut tx, record.entry.start()).await?;
        insert_checkpoint_components(
            &mut tx,
            record.entry.checkpoint().checkpoint(),
            record.event.source(),
            Some(record.entry.checkpoint()),
        )
        .await?;
        insert_entry(&mut tx, &record, &plan).await?;
        update_run_head(&mut tx, &record.event, None).await?;
        // Repeat live clock and the unchanged parent/root pointers immediately
        // before commit; a lease that expired during any insert rolls back all.
        revalidate_worker_after_components(&mut tx, plan.fence()).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::database("compound frame entry commit", e))?;
        Ok(GraphFrameEntryCommitOutcome::Committed(record))
    }
}

fn verify_retry(
    record: &StoredGraphFrameEntry,
    plan: &GraphFrameEntryPlan,
) -> Result<(), StoreError> {
    let actual = record.entry.checkpoint().checkpoint();
    if record.entry.checkpoint().frame() != plan.frame()
        || actual.state() != plan.checkpoint().state()
        || actual.ready_nodes() != plan.checkpoint().ready_nodes()
        || actual.graph() != plan.checkpoint().graph()
    {
        return Err(StoreError::GraphFrameConflict);
    }
    Ok(())
}

async fn insert_entry(
    tx: &mut Transaction<'_, Postgres>,
    record: &StoredGraphFrameEntry,
    plan: &GraphFrameEntryPlan,
) -> Result<(), StoreError> {
    let frame = plan.frame();
    let origin = frame.origin();
    let cp = record.entry.checkpoint();
    let event = &record.event;
    let bytes = encode_entry(record, plan)?;
    query("INSERT INTO stateknot.graph_frame_entries (tenant_id,run_id,graph_namespace,frame_identity_digest,ordinal,parent_namespace,parent_checkpoint_id,caller_attempt_id,initial_checkpoint_id,initial_checkpoint_digest,core_entry_digest,compound_digest,entry_bytes,journal_sequence,journal_event_id,journal_recorded_at,journal_digest) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)")
        .bind(origin.tenant_id().as_str()).bind(*origin.run_id().as_uuid()).bind(frame.namespace().as_str()).bind(frame.digest().as_bytes()).bind(i32::from(record.scope.ordinal))
        .bind(origin.graph_namespace().as_str()).bind(*origin.base_checkpoint().checkpoint_id().as_uuid()).bind(*plan.attempt_id().as_uuid()).bind(*cp.checkpoint().checkpoint_id().as_uuid())
        .bind(cp.checkpoint().digest().as_bytes()).bind(record.entry.digest().as_bytes()).bind(record.digest.as_bytes()).bind(bytes)
        .bind(i64::try_from(event.sequence().get()).map_err(|_|StoreError::JournalSequenceExhausted)?).bind(*event.event_id().as_uuid()).bind(to_database_time(event.recorded_at())?).bind(event.digest().as_bytes())
        .execute(&mut **tx).await.map_err(|e|StoreError::database("frame immutable entry insert",e))?;
    query("INSERT INTO stateknot.graph_frame_heads (tenant_id,run_id,graph_namespace,frame_identity_digest,checkpoint_id,superstep,checkpoint_digest,frame_checkpoint_digest) VALUES ($1,$2,$3,$4,$5,0,$6,$7)")
        .bind(origin.tenant_id().as_str()).bind(*origin.run_id().as_uuid()).bind(frame.namespace().as_str()).bind(frame.digest().as_bytes()).bind(*cp.checkpoint().checkpoint_id().as_uuid())
        .bind(cp.checkpoint().digest().as_bytes()).bind(cp.digest().as_bytes()).execute(&mut **tx).await.map_err(|e|StoreError::database("frame initial head",e))?;
    let updated=query("INSERT INTO stateknot.graph_frame_stacks (tenant_id,run_id,admission_digest,lifetime_starts,active_namespace,active_frame_identity_digest) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (tenant_id,run_id) DO UPDATE SET lifetime_starts=EXCLUDED.lifetime_starts,active_namespace=EXCLUDED.active_namespace,active_frame_identity_digest=EXCLUDED.active_frame_identity_digest WHERE graph_frame_stacks.admission_digest=EXCLUDED.admission_digest AND graph_frame_stacks.lifetime_starts+1=EXCLUDED.lifetime_starts AND graph_frame_stacks.active_namespace=$7")
        .bind(origin.tenant_id().as_str()).bind(*origin.run_id().as_uuid()).bind(record.scope.admission_digest.as_bytes()).bind(i32::from(record.scope.ordinal))
        .bind(frame.namespace().as_str()).bind(frame.digest().as_bytes()).bind(origin.graph_namespace().as_str()).execute(&mut **tx).await.map_err(|e|StoreError::database("frame stack push",e))?.rows_affected();
    if updated != 1 {
        return Err(StoreError::GraphFrameConflict);
    }
    Ok(())
}

pub(super) async fn recognize_start(
    tx: &mut Transaction<'_, Postgres>,
    start: &NodeAttemptStart,
) -> Result<Option<JournalEvent>, StoreError> {
    let tenant = start.activation().tenant_id();
    let run = start.activation().run_id();
    let (event, _) = child_runs::anchored_event(
        tx,
        tenant,
        run,
        i64::try_from(start.journal_head().sequence().get())
            .map_err(|_| StoreError::corrupt("frame start sequence"))?,
    )
    .await?;
    if event.payload().kind().as_str() == "graph-frame-caller-rebound" {
        return Box::pin(caller_bindings::recognize_start(tx, start, &event))
            .await
            .map(Some);
    }
    if event.payload().kind().as_str() != GraphFrameEntryPlan::EVENT_KIND {
        return Ok(None);
    }
    let namespace=query_scalar::<_,String>("SELECT graph_namespace FROM stateknot.graph_frame_entries WHERE tenant_id=$1 AND run_id=$2 AND caller_attempt_id=$3 AND journal_sequence=$4")
        .bind(tenant.as_str()).bind(*run.as_uuid()).bind(*start.attempt_id().as_uuid()).bind(i64::try_from(start.journal_head().sequence().get()).map_err(|_|StoreError::corrupt("frame start sequence"))?)
        .fetch_optional(&mut **tx).await.map_err(|e|StoreError::database("frame start compound",e))?.ok_or_else(||StoreError::corrupt("frame start missing compound"))?;
    let run_row = query_as::<_, RunRow>(SELECT_RUN)
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| StoreError::database("frame start run", e))?
        .ok_or(StoreError::RunNotFound)?;
    let admission_row = load_agent_admission_row(tx, tenant, run)
        .await?
        .ok_or(StoreError::AgentAdmissionConflict)?;
    let admission = verify_stored_agent_admission(tx, decode_run(run_row)?, admission_row).await?;
    let graphs = closure(tx, tenant, admission.admission().intent().graph()).await?;
    let namespace =
        GraphNamespace::new(namespace).map_err(|_| StoreError::corrupt("frame start namespace"))?;
    let record = verified_entry(tx, tenant, run, &namespace, &admission, &graphs).await?;
    if record.entry.start() != start || record.event != event {
        return Err(StoreError::corrupt("frame caller start compound"));
    }
    Ok(Some(record.event))
}

pub(super) async fn verify_schema(pool: &PgPool) -> Result<(), StoreError> {
    let actual = query_scalar::<_, String>(include_str!("graph_frame_catalog.sql"))
        .fetch_one(pool)
        .await
        .map_err(|source| StoreError::database("compound frame catalog", source))?;
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("graph_frame_catalog.json"))
            .map_err(|_| StoreError::IncompleteSchema)?;
    if serde_json::from_str::<serde_json::Value>(&actual)
        .map_err(|_| StoreError::IncompleteSchema)?
        != expected
    {
        return Err(StoreError::IncompleteSchema);
    }
    Ok(())
}

// Every scoped checkpoint is authenticated through its whole entry and
// bounded forward barrier lineage; the legacy Root decoder stays Root-only.
pub(super) async fn bound_checkpoint(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
    namespace: &GraphNamespace,
    expected: &CheckpointHead,
    require_active: bool,
) -> Result<GraphFrameCheckpoint, StoreError> {
    if namespace.is_root() {
        return Err(StoreError::GraphFrameConflict);
    }
    let row = query_as::<_, RunRow>(SELECT_RUN)
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| StoreError::database("frame node Run", e))?
        .ok_or(StoreError::RunNotFound)?;
    let admission_row = load_agent_admission_row(tx, tenant, run)
        .await?
        .ok_or(StoreError::AgentAdmissionConflict)?;
    let admission = verify_stored_agent_admission(tx, decode_run(row)?, admission_row).await?;
    let graphs = closure(tx, tenant, admission.admission().intent().graph()).await?;
    let entry = verified_entry(tx, tenant, run, namespace, &admission, &graphs).await?;
    let (checkpoint, _) =
        barriers::checkpoint_at(tx, &entry, &admission, &graphs, expected).await?;
    if require_active {
        verify_active_checkpoint(tx, &entry, &checkpoint).await?;
    }
    Ok(checkpoint.clone())
}

// Use only after authenticating both values in this transaction. Keeping
// the active-stack predicate separate avoids replaying the same ancestry
// while a writer already holds its verified entry and checkpoint.
pub(super) async fn verify_active_checkpoint(
    tx: &mut Transaction<'_, Postgres>,
    entry: &StoredGraphFrameEntry,
    checkpoint: &GraphFrameCheckpoint,
) -> Result<(), StoreError> {
    let tenant = checkpoint.checkpoint().tenant_id();
    let run = checkpoint.checkpoint().run_id();
    let namespace = checkpoint.frame().namespace();
    let active = query_as::<_,StackRow>("SELECT admission_digest,lifetime_starts,active_namespace,active_frame_identity_digest FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2")
        .bind(tenant.as_str()).bind(*run.as_uuid()).fetch_optional(&mut **tx).await
        .map_err(|e|StoreError::database("frame node stack",e))?.ok_or(StoreError::GraphFrameConflict)?;
    if active.active_namespace != namespace.as_str()
        || active.active_frame_identity_digest.as_deref()
            != Some(checkpoint.frame().digest().as_bytes())
        || decode_digest(&active.admission_digest, "frame node admission")?
            != entry.scope.admission_digest
        || active.lifetime_starts < i32::from(entry.scope.ordinal)
    {
        return Err(StoreError::GraphFrameConflict);
    }
    let head = query_as::<_,(Uuid,i64,Vec<u8>,Vec<u8>)>("SELECT checkpoint_id,superstep,checkpoint_digest,frame_checkpoint_digest FROM stateknot.graph_frame_heads WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3")
        .bind(tenant.as_str()).bind(*run.as_uuid()).bind(namespace.as_str())
        .fetch_optional(&mut **tx).await.map_err(|e|StoreError::database("frame node head",e))?
        .ok_or(StoreError::GraphFrameConflict)?;
    if head.0 != *checkpoint.checkpoint().checkpoint_id().as_uuid()
        || nonnegative_superstep(head.1)? != checkpoint.checkpoint().superstep()
        || decode_digest(&head.2, "frame node checkpoint")? != checkpoint.checkpoint().digest()
        || decode_digest(&head.3, "frame node scoped head")? != checkpoint.digest()
    {
        return Err(StoreError::StaleCheckpointHead);
    }
    Ok(())
}

// Flush deferred compound/scope guards before the final database-clock check.
// A slow deferred trigger must not let a lease expire between validation and
// the commit of newly granted launch authority.
pub(super) async fn revalidate_worker_after_components(
    tx: &mut Transaction<'_, Postgres>,
    fence: &RunFence,
) -> Result<(), StoreError> {
    Box::pin(revalidate_scoped_worker_after_components(tx, fence, true)).await
}

// Known in-flight invocation outcomes grant no new execution authority, but
// still require the exact live fence after every deferred component guard.
pub(super) async fn revalidate_scoped_worker_after_components(
    tx: &mut Transaction<'_, Postgres>,
    fence: &RunFence,
    requires_deadline: bool,
) -> Result<(), StoreError> {
    query("SET CONSTRAINTS ALL IMMEDIATE")
        .execute(&mut **tx)
        .await
        .map_err(|e| StoreError::database("frame component guards", e))?;
    let valid = query_scalar::<_, bool>(
        "SELECT lease_attempt_id=$3 AND fencing_epoch=$4 AND lease_expires_at>clock_timestamp() AND (NOT $5 OR agent_deadline_at>clock_timestamp()) FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2"
    ).bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid())
        .bind(*fence.attempt_id().as_uuid())
        .bind(i64::try_from(fence.epoch().get()).map_err(|_|StoreError::StaleFence)?)
        .bind(requires_deadline)
        .fetch_one(&mut **tx).await
        .map_err(|e|StoreError::database("frame final authority",e))?;
    if !valid {
        return Err(StoreError::LeaseExpired);
    }
    Ok(())
}

pub(super) async fn reject_framework_call(
    tx: &mut Transaction<'_, Postgres>,
    checkpoint: &GraphFrameCheckpoint,
    node: &NodeId,
) -> Result<(), StoreError> {
    let graph = graph(
        tx,
        checkpoint.checkpoint().tenant_id(),
        checkpoint.checkpoint().graph(),
    )
    .await?;
    if graph
        .frame_calls()
        .is_some_and(|policy| policy.calls().iter().any(|call| call.node_id() == node))
    {
        return Err(StoreError::GraphFrameCompoundRequired);
    }
    Ok(())
}

pub(super) fn verify_wait_anchor<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    wait: &'a DurableWait,
    event: &'a JournalEvent,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), StoreError>> + Send + 'a>> {
    barriers::verify_wait_anchor(tx, wait, event)
}

// A sealed failure may suspend actual framework callers only after the entire
// open stack and all ordinary/provider work are authenticated in this Run lock.
pub(super) async fn prepare_failure_close(
    tx: &mut Transaction<'_, Postgres>,
    stored: &StoredRun,
    tenant: &TenantId,
    run: RunId,
    direct: &BudgetUsage,
) -> Result<bool, StoreError> {
    let open: bool = query_scalar("SELECT EXISTS(SELECT 1 FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2 AND active_namespace<>'')").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(&mut **tx).await.map_err(|e|StoreError::database("failure frame boundary",e))?;
    if !open {
        return Ok(false);
    }
    let snapshot = Box::pin(active::verified_snapshot(tx, tenant, run, stored.clone()))
        .await?
        .ok_or(StoreError::GraphFrameConflict)?;
    direct
        .validate_monotonic_after(snapshot.minimum_direct_usage())
        .map_err(|_| StoreError::IncompleteChildAccounting)?;
    closures::ensure_settled_work(tx, tenant, run).await?;
    Ok(true)
}
pub(super) async fn validate_closed_direct_usage(
    tx: &mut Transaction<'_, Postgres>,
    event: &JournalEvent,
    direct: &BudgetUsage,
) -> Result<(), StoreError> {
    let open: bool = query_scalar("SELECT EXISTS(SELECT 1 FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2 AND active_namespace<>'')")
        .bind(event.tenant_id().as_str()).bind(*event.run_id().as_uuid())
        .fetch_one(&mut **tx).await.map_err(|e|StoreError::database("terminal frame boundary",e))?;
    if open {
        return Err(StoreError::GraphFrameCompoundRequired);
    }
    let Some(saved) = closures::row(tx, event.tenant_id(), event.run_id()).await? else {
        return Ok(());
    };
    let current = query_as::<_, RunRow>(SELECT_RUN)
        .bind(event.tenant_id().as_str())
        .bind(*event.run_id().as_uuid())
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| StoreError::database("closed terminal Run", e))?;
    let record = Box::pin(closures::verified_record(tx, &decode_run(current)?, saved)).await?;
    if record.direct_usage() != direct {
        return Err(StoreError::IncompleteChildAccounting);
    }
    Ok(())
}
