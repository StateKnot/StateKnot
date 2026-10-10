// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! An immutable child terminal proof is settled once with its parent result.
#[allow(clippy::wildcard_imports)]
use super::*;
use stateknot_core::{
    GraphFrameBarrier, GraphFrameBarrierPlan, NodeControl, NodeInvocationBindings, NodeStateChange,
    NodeStateUpdate, NodeTerminalOutput,
};

const EVENT_KIND: &str = "graph-frame-returned";
const SCHEMA_ID: &str = "https://stknot.com/schemas/store/graph-frame-return-event/1.0.0";
const INTENT_DOMAIN: &[u8] = b"stateknot-postgres-frame-return-intent-v1\0";
const COMPOUND_DOMAIN: &[u8] = b"stateknot-postgres-frame-return-compound-v1\0";
const MAX_RETURN_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    version: u8,
    entry_digest: Digest,
    terminal_checkpoint: GraphFrameCheckpointHead,
    terminal_base_checkpoint_id: CheckpointId,
    terminal_barrier_digest: Digest,
    terminal_barrier: GraphFrameBarrier,
    terminal_output: NodeTerminalOutput,
    caller: stateknot_core::NodeAttemptStartHead,
    caller_compound_digest: Digest,
    result_intent_digest: Digest,
    budget: EntryBudget,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    version: u8,
    // Preserve canonical wire bytes while keeping the large immutable intent
    // out of the Serde visitor stack during complete ancestor authentication.
    intent: Box<Intent>,
    intent_digest: Digest,
    result: PendingNodeResult,
    completion: NodeAttemptCompletion,
    compound_digest: Digest,
}

/// Fully authenticated child settlement, parent result and physical completion.
#[derive(Clone, Debug)]
pub struct StoredGraphFrameReturn {
    event: JournalEvent,
    intent: Intent,
    intent_digest: Digest,
    result: PendingNodeResult,
    completion: NodeAttemptCompletion,
    digest: Digest,
}
impl StoredGraphFrameReturn {
    /// Returns the single settlement journal fact.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        &self.event
    }
    /// Returns the exact immutable child terminal checkpoint.
    #[must_use]
    pub const fn terminal_checkpoint(&self) -> &GraphFrameCheckpointHead {
        &self.intent.terminal_checkpoint
    }
    /// Returns the declared-route result in the suspended parent's namespace.
    #[must_use]
    pub const fn result(&self) -> &PendingNodeResult {
        &self.result
    }
    /// Returns the physical framework caller's once-only successful completion.
    #[must_use]
    pub const fn completion(&self) -> &NodeAttemptCompletion {
        &self.completion
    }
    /// Returns the whole transaction projection digest.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
    /// Returns the authenticated complete DIRECT floor after this return.
    ///
    /// # Errors
    /// Rejects canonical encoding failure or counter overflow.
    pub fn direct_usage_after(&self) -> Result<BudgetUsage, StoreError> {
        charged(self)
    }
}

/// A recovered return grants no fresh child or application dispatch.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum GraphFrameReturnCommitOutcome {
    /// Settlement, completion, result, pop and journal committed together.
    Committed(StoredGraphFrameReturn),
    /// The original complete return was authenticated and recovered.
    Idempotent(StoredGraphFrameReturn),
}

fn intent_digest(intent: &Intent) -> Result<Digest, StoreError> {
    digest_wire(INTENT_DOMAIN, intent)
}
fn compound(
    intent: Digest,
    event: &JournalEvent,
    result: &PendingNodeResult,
    completion: &NodeAttemptCompletion,
) -> Result<Digest, StoreError> {
    #[derive(Serialize)]
    struct Preimage<'a> {
        version: u8,
        intent_digest: Digest,
        event: &'a JournalEvent,
        result: &'a PendingNodeResult,
        completion: &'a NodeAttemptCompletion,
    }
    digest_wire(
        COMPOUND_DOMAIN,
        &Preimage {
            version: 1,
            intent_digest: intent,
            event,
            result,
            completion,
        },
    )
}
fn encode(record: &StoredGraphFrameReturn) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(&Wire {
        version: 1,
        intent: Box::new(record.intent.clone()),
        intent_digest: record.intent_digest,
        result: record.result.clone(),
        completion: record.completion.clone(),
        compound_digest: record.digest,
    })
    .map_err(|_| StoreError::encoding("frame return record"))?;
    if bytes.is_empty() || bytes.len() > MAX_RETURN_BYTES {
        return Err(StoreError::GraphFrameLimitExceeded);
    }
    Ok(bytes)
}
fn decode(bytes: &[u8]) -> Result<Wire, StoreError> {
    if bytes.is_empty() || bytes.len() > MAX_RETURN_BYTES {
        return Err(StoreError::corrupt("frame return byte bound"));
    }
    let wire: Wire =
        serde_json::from_slice(bytes).map_err(|_| StoreError::corrupt("frame return wire"))?;
    if wire.version != 1
        || wire.intent.version != 1
        || serde_json_canonicalizer::to_vec(&wire)
            .map_err(|_| StoreError::corrupt("frame return canonicalization"))?
            != bytes
    {
        return Err(StoreError::corrupt(
            "frame return version or canonical bytes",
        ));
    }
    Ok(wire)
}
fn charged(record: &StoredGraphFrameReturn) -> Result<BudgetUsage, StoreError> {
    use stateknot_core::{ByteCount, ExecutionCount};
    let bytes = serde_json_canonicalizer::to_vec(&record.event)
        .map_err(|_| StoreError::encoding("frame return event"))?
        .len();
    record
        .intent
        .budget
        .direct_usage
        .checked_accumulate(
            &BudgetUsage::builder()
                .graph_steps(ExecutionCount::new(1))
                .event_bytes(ByteCount::new(
                    u64::try_from(bytes).map_err(|_| StoreError::GraphFrameLimitExceeded)?,
                ))
                .build()
                .map_err(|_| StoreError::GraphFrameLimitExceeded)?,
        )
        .map_err(|_| StoreError::GraphFrameLimitExceeded)
}
static EVENT_SCHEMA: LazyLock<
    Result<(stateknot_core::SchemaReference, serde_json::Value), &'static str>,
> = LazyLock::new(|| {
    let digest = serde_json::json!({"type":"string","minLength":71,"maxLength":71,"pattern":"^sha256:[0-9a-f]{64}$"});
    let id = serde_json::json!({"type":"string","minLength":36,"maxLength":36,"pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"});
    let document = serde_json::json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$id":SCHEMA_ID,"type":"object","additionalProperties":false,
        "properties":{"version":{"const":1},"graph_namespace":{"type":"string","minLength":64,"maxLength":454,"pattern":"^[0-9a-f]{64}(/[0-9a-f]{64}){0,6}$"},"frame_identity_digest":digest,"terminal_checkpoint_id":id,"caller_attempt_id":id,"intent_digest":digest},
        "required":["version","graph_namespace","frame_identity_digest","terminal_checkpoint_id","caller_attempt_id","intent_digest"]});
    let bytes = serde_json_canonicalizer::to_vec(&document).map_err(|_| "frame return schema")?;
    Ok((
        stateknot_core::SchemaReference::new(
            SCHEMA_ID.parse().map_err(|_| "frame return schema ID")?,
            stateknot_core::Version::new(1, 0, 0),
            Digest::sha256(bytes),
        ),
        document,
    ))
});
fn payload(intent: &Intent) -> Result<JournalPayload, StoreError> {
    #[derive(Serialize)]
    struct Data<'a> {
        version: u8,
        graph_namespace: &'a GraphNamespace,
        frame_identity_digest: Digest,
        terminal_checkpoint_id: CheckpointId,
        caller_attempt_id: AttemptId,
        intent_digest: Digest,
    }
    let frame = intent.terminal_checkpoint.frame();
    let bytes = serde_json_canonicalizer::to_vec(&Data {
        version: 1,
        graph_namespace: frame.namespace(),
        frame_identity_digest: frame.digest(),
        terminal_checkpoint_id: intent.terminal_checkpoint.checkpoint().checkpoint_id(),
        caller_attempt_id: intent.caller.attempt_id(),
        intent_digest: intent_digest(intent)?,
    })
    .map_err(|_| StoreError::encoding("frame return payload"))?;
    JournalPayload::new(
        EVENT_SCHEMA
            .as_ref()
            .map_err(|_| StoreError::encoding("frame return schema"))?
            .0
            .clone(),
        EVENT_KIND
            .parse()
            .map_err(|_| StoreError::encoding("frame return kind"))?,
        BoundedJson::from_slice(&bytes)
            .map_err(|_| StoreError::encoding("frame return payload bound"))?,
    )
    .map_err(|_| StoreError::encoding("frame return payload"))
}
fn result_intent(
    entry: &StoredGraphFrameEntry,
    terminal: &barriers::StoredGraphFrameBarrier,
    graphs: &BTreeMap<Digest, CompiledGraph>,
) -> Result<PendingNodeResultIntent, StoreError> {
    let origin = entry.entry.checkpoint().frame().origin();
    let parent = graphs
        .get(&origin.base_checkpoint().graph().definition_digest())
        .ok_or(StoreError::GraphFrameRejected)?;
    let call = parent
        .frame_calls()
        .and_then(|calls| calls.call(origin.node_id()))
        .ok_or(StoreError::GraphFrameRejected)?;
    let disposition = terminal.disposition();
    let output = disposition
        .terminal_output()
        .filter(|output| output.schema() == call.output_schema())
        .ok_or(StoreError::GraphFrameConflict)?;
    PendingNodeResultIntent::new(
        origin.clone(),
        NodeStateChange::Update {
            update: NodeStateUpdate::new(output.schema().clone(), output.data().clone())
                .map_err(|_| StoreError::GraphFrameRejected)?,
        },
        NodeControl::Route {
            route_id: call.return_route().clone(),
        },
        NodeInvocationBindings::empty(),
    )
    .map_err(|_| StoreError::GraphFrameRejected)
}
async fn row(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
    namespace: &GraphNamespace,
) -> Result<Option<PgRow>, StoreError> {
    query("SELECT * FROM stateknot.graph_frame_returns WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3").bind(tenant.as_str()).bind(*run.as_uuid()).bind(namespace.as_str()).fetch_optional(&mut **tx).await.map_err(|e|StoreError::database("frame return row",e))
}
fn digest_column(row: &PgRow, name: &str, expected: Digest) -> Result<(), StoreError> {
    let bytes: Vec<u8> = row
        .try_get(name)
        .map_err(|e| StoreError::database("frame return digest column", e))?;
    if decode_digest(&bytes, "frame return projection")? != expected {
        return Err(StoreError::corrupt("frame return digest projection"));
    }
    Ok(())
}
#[allow(clippy::too_many_lines)]
fn verify_columns(row: &PgRow, record: &StoredGraphFrameReturn) -> Result<(), StoreError> {
    let i = &record.intent;
    let head = i.terminal_checkpoint.checkpoint();
    let frame = i.terminal_checkpoint.frame();
    let origin = frame.origin();
    let base = origin.base_checkpoint();
    let journal = record.event.head();
    for (name, digest) in [
        ("frame_identity_digest", frame.digest()),
        ("entry_digest", i.entry_digest),
        ("terminal_checkpoint_digest", head.digest()),
        (
            "terminal_frame_checkpoint_digest",
            i.terminal_checkpoint.digest(),
        ),
        ("terminal_barrier_digest", i.terminal_barrier_digest),
        ("caller_start_digest", i.caller.digest()),
        ("caller_compound_digest", i.caller_compound_digest),
        ("parent_checkpoint_digest", base.digest()),
        ("activation_input_digest", origin.input_digest()),
        (
            "result_intent_digest",
            record.result.intent().intent_digest(),
        ),
        ("result_record_digest", record.result.digest()),
        ("completion_digest", record.completion.digest()),
        ("intent_digest", record.intent_digest),
        ("compound_digest", record.digest),
        ("journal_digest", journal.digest()),
    ] {
        digest_column(row, name, digest)?;
    }
    let get = |e| StoreError::database("frame return scalar projection", e);
    if row.try_get::<String, _>("tenant_id").map_err(get)? != origin.tenant_id().as_str()
        || row.try_get::<Uuid, _>("run_id").map_err(get)? != *origin.run_id().as_uuid()
        || row.try_get::<String, _>("graph_namespace").map_err(get)? != frame.namespace().as_str()
        || row
            .try_get::<Uuid, _>("terminal_base_checkpoint_id")
            .map_err(get)?
            != *i.terminal_base_checkpoint_id.as_uuid()
        || row
            .try_get::<Uuid, _>("terminal_checkpoint_id")
            .map_err(get)?
            != *head.checkpoint_id().as_uuid()
        || nonnegative_superstep(row.try_get("terminal_superstep").map_err(get)?)?
            != head.superstep()
        || row.try_get::<Uuid, _>("caller_attempt_id").map_err(get)?
            != *i.caller.attempt_id().as_uuid()
        || row.try_get::<String, _>("parent_namespace").map_err(get)?
            != origin.graph_namespace().as_str()
        || row
            .try_get::<Uuid, _>("parent_checkpoint_id")
            .map_err(get)?
            != *base.checkpoint_id().as_uuid()
        || nonnegative_superstep(row.try_get("parent_superstep").map_err(get)?)? != base.superstep()
        || row.try_get::<String, _>("node_id").map_err(get)? != origin.node_id().as_str()
        || row.try_get::<i64, _>("journal_sequence").map_err(get)?
            != i64::try_from(journal.sequence().get())
                .map_err(|_| StoreError::corrupt("frame return sequence"))?
        || row.try_get::<Uuid, _>("journal_event_id").map_err(get)? != *journal.event_id().as_uuid()
        || from_database_time(row.try_get("journal_recorded_at").map_err(get)?)?
            != journal.recorded_at()
    {
        return Err(StoreError::corrupt("frame return scalar projection"));
    }
    Ok(())
}
async fn anchored_record(
    tx: &mut Transaction<'_, Postgres>,
    row: &PgRow,
) -> Result<StoredGraphFrameReturn, StoreError> {
    let get = |e| StoreError::database("frame return record column", e);
    let bytes: Vec<u8> = row.try_get("return_bytes").map_err(get)?;
    digest_column(row, "return_checksum", Digest::sha256(&bytes))?;
    let wire = decode(&bytes)?;
    let origin = wire.intent.terminal_checkpoint.frame().origin().clone();
    let (event, projection) = child_runs::anchored_event(
        tx,
        origin.tenant_id(),
        origin.run_id(),
        row.try_get("journal_sequence").map_err(get)?,
    )
    .await?;
    let record = StoredGraphFrameReturn {
        event,
        intent: *wire.intent,
        intent_digest: wire.intent_digest,
        result: wire.result,
        completion: wire.completion,
        digest: wire.compound_digest,
    };
    verify_columns(row, &record)?;
    if projection != Some(record.digest)
        || record.event.source().worker_fence() != Some(record.intent.caller.fence())
        || record.event.payload() != &payload(&record.intent)?
        || record.result.journal_head() != &record.event.head()
        || record.result.fence() != record.intent.caller.fence()
        || record.completion.journal_head() != &record.event.head()
        || record.completion.start() != &record.intent.caller
        || record.completion.outcome().result() != Some(&record.result.head())
        || record.completion.usage() != &BudgetUsage::zero()
        || record.result.intent().activation() != &origin
        || record.result.intent().intent_digest() != record.intent.result_intent_digest
        || intent_digest(&record.intent)? != record.intent_digest
        || compound(
            record.intent_digest,
            &record.event,
            &record.result,
            &record.completion,
        )? != record.digest
    {
        return Err(StoreError::corrupt("frame return whole event/components"));
    }
    Ok(record)
}

// Entry/start replay may observe a completed caller without recursively loading
// its own terminal history. This proves only the completion's whole anchor;
// result and return readers separately authenticate entry, terminal and usage.
pub(super) fn verify_completion_anchor<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    attempt: &'a NodeAttempt,
) -> stateknot_core::BoxFuture<'a, Result<(), StoreError>> {
    Box::pin(verify_completion_anchor_inner(tx, attempt))
}

// Keep the whole return/closure anchor off every ancestor's inline Future.
// Both branches retain their original immutable component authentication.
async fn verify_completion_anchor_inner(
    tx: &mut Transaction<'_, Postgres>,
    attempt: &NodeAttempt,
) -> Result<(), StoreError> {
    let Some(completion) = attempt.completion() else {
        return Ok(());
    };
    if Box::pin(closures::verify_completion_anchor(tx, attempt)).await? {
        return Ok(());
    }
    let start = attempt.start();
    let origin = start.activation();
    let row=query("SELECT * FROM stateknot.graph_frame_returns WHERE tenant_id=$1 AND run_id=$2 AND caller_attempt_id=$3").bind(origin.tenant_id().as_str()).bind(*origin.run_id().as_uuid()).bind(*start.attempt_id().as_uuid()).fetch_optional(&mut **tx).await.map_err(|e|StoreError::database("framework completion witness",e))?.ok_or_else(||StoreError::corrupt("framework completion whole return missing"))?;
    let record = anchored_record(tx, &row).await?;
    if record.intent.caller != start.head()
        || encode_node_attempt_completion(&record.completion)?
            != encode_node_attempt_completion(completion)?
    {
        return Err(StoreError::corrupt(
            "framework completion return substitution",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)] // Keep whole proof verification in dependency order.
// A private erased Send future closes the ancestry/result verification cycle.
// The explicit transaction-owned proof bounds remain enforced by the body.
pub(super) fn verified_record_replay<'a>(
    replay: &'a mut Replay<'_, '_>,
    admission: &'a StoredAgentAdmission,
    graphs: &'a BTreeMap<Digest, CompiledGraph>,
    row: PgRow,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<StoredGraphFrameReturn, StoreError>> + Send + 'a>,
> {
    Box::pin(async move {
        let record = anchored_record(replay.tx, &row).await?;
        let head = &record.intent.terminal_checkpoint;
        let frame = head.frame();
        let origin = frame.origin();
        let tenant = origin.tenant_id();
        let run = origin.run_id();
        let before_sequence = record
            .event
            .sequence()
            .get()
            .checked_sub(1)
            .ok_or_else(|| StoreError::corrupt("return predecessor sequence"))?;
        if head.checkpoint().journal_head().sequence().get() > before_sequence
            || record.intent.caller.journal_head().sequence().get() > before_sequence
        {
            return Err(StoreError::corrupt("return proof journal order"));
        }
        replay.enter_return(tenant, run, record.event.sequence().get())?;
        // Authenticate earlier accounting facts before traversing the terminal's
        // result owners. Those owners can then reuse their compact whole proofs.
        let previous_return_floor = Box::pin(usage_floor_before(
            replay,
            admission,
            graphs,
            before_sequence,
            &admission_usage_floor(admission)?,
        ))
        .await?;
        let entry = Box::pin(verified_entry_replay(
            replay,
            tenant,
            run,
            frame.namespace(),
            admission,
            graphs,
        ))
        .await?;
        let terminal = Box::pin(barriers::terminal_replay(
            replay,
            &entry,
            admission,
            graphs,
            head,
            record.intent.terminal_base_checkpoint_id,
        ))
        .await?;
        let history = Box::pin(caller_bindings::verified_history_replay(
            replay, &entry, admission, graphs, None,
        ))
        .await?;
        if record.intent.entry_digest != entry.digest
            || record.intent.terminal_barrier_digest != terminal.digest()
            || record.intent.terminal_barrier != *terminal.barrier()
            || Some(&record.intent.terminal_output) != terminal.disposition().terminal_output()
            || record.intent.caller != history.current.head()
            || record.intent.caller_compound_digest != history.digest
            || record.result.intent() != &result_intent(&entry, &terminal, graphs)?
        {
            return Err(StoreError::corrupt("frame return complete lineage"));
        }
        let actual =
            load_node_attempt_record(replay.tx, tenant, &run, record.intent.caller.attempt_id())
                .await?
                .ok_or_else(|| StoreError::corrupt("returned caller missing"))?;
        if actual.start() != &history.current
            || actual
                .completion()
                .map(encode_node_attempt_completion)
                .transpose()?
                != Some(encode_node_attempt_completion(&record.completion)?)
        {
            return Err(StoreError::corrupt("returned caller completion components"));
        }
        let result_row = load_pending_node_result_row(replay.tx, origin)
            .await?
            .ok_or_else(|| StoreError::corrupt("returned parent result missing"))?;
        if result_row.node_attempt_id != Some(*record.intent.caller.attempt_id().as_uuid())
            || decode_pending_node_result(&result_row)? != record.result
        {
            return Err(StoreError::corrupt("returned parent result components"));
        }
        verify_pending_node_result_bindings(replay.tx, &record.result).await?;
        let (before, _) = child_runs::anchored_event(
            replay.tx,
            tenant,
            run,
            i64::try_from(before_sequence)
                .map_err(|_| StoreError::corrupt("return predecessor sequence"))?,
        )
        .await?;
        if record.intent.budget.observed_head != before.head()
            || record.event.previous_digest() != Some(before.digest())
            || record.event.recorded_at() < before.recorded_at()
            || terminal.event().sequence() > before.sequence()
            || terminal.event().recorded_at() > before.recorded_at()
            || history.current.journal_head().sequence() > before.sequence()
        {
            return Err(StoreError::corrupt("return observation order"));
        }
        let floor = caller_bindings::usage_floor_before(
            replay.tx,
            &entry,
            admission,
            before_sequence,
            &terminal.direct_usage_after()?,
        )
        .await?;
        let floor = if floor
            .validate_monotonic_after(&previous_return_floor)
            .is_ok()
        {
            floor
        } else {
            previous_return_floor
                .validate_monotonic_after(&floor)
                .map_err(|_| StoreError::corrupt("return shared floor comparison"))?;
            previous_return_floor
        };
        record
            .intent
            .budget
            .direct_usage
            .validate_monotonic_after(&floor)
            .map_err(|_| StoreError::corrupt("return shared usage regression"))?;
        let total = charged(&record)?
            .checked_accumulate(&record.intent.budget.delegated_usage)
            .map_err(|_| StoreError::corrupt("return accounting"))?;
        admission
            .admission()
            .intent()
            .budget()
            .remaining(&total, record.event.recorded_at())
            .map_err(|_| StoreError::corrupt("return budget limit"))?;
        replay.remember_return(
            tenant,
            run,
            record.event.sequence().get(),
            record.digest,
            &charged(&record)?,
        )?;
        replay.leave_return(tenant, run, record.event.sequence().get());
        Ok(record)
    })
}

pub(super) async fn usage_floor_before(
    replay: &mut Replay<'_, '_>,
    admission: &StoredAgentAdmission,
    graphs: &BTreeMap<Digest, CompiledGraph>,
    through: u64,
    floor: &BudgetUsage,
) -> Result<BudgetUsage, StoreError> {
    let tenant = admission.event().tenant_id();
    let run = admission.event().run_id();
    let latest=query_as::<_,(i64,Vec<u8>)>("SELECT journal_sequence,compound_digest FROM stateknot.graph_frame_returns WHERE tenant_id=$1 AND run_id=$2 AND journal_sequence<=$3 ORDER BY journal_sequence DESC LIMIT 1").bind(tenant.as_str()).bind(*run.as_uuid()).bind(i64::try_from(through).map_err(|_|StoreError::corrupt("return usage sequence"))?).fetch_optional(&mut **replay.tx).await.map_err(|e|StoreError::database("shared return usage floor",e))?;
    let Some((sequence, digest)) = latest else {
        return Ok(floor.clone());
    };
    let sequence =
        u64::try_from(sequence).map_err(|_| StoreError::corrupt("return usage sequence"))?;
    let digest = decode_digest(&digest, "return usage compound")?;
    let mut after = replay.latest_return_before(tenant, run, sequence);
    // Verify missing Run-wide accounting facts in journal order. Every recursive
    // edge then finds its earlier accounting prefix already authenticated; no
    // recursion holds an entire lifetime of full return/state buffers.
    while after < sequence {
        let row=query("SELECT * FROM stateknot.graph_frame_returns WHERE tenant_id=$1 AND run_id=$2 AND journal_sequence>$3 AND journal_sequence<=$4 ORDER BY journal_sequence LIMIT 1").bind(tenant.as_str()).bind(*run.as_uuid()).bind(i64::try_from(after).map_err(|_|StoreError::corrupt("return prefix sequence"))?).bind(i64::try_from(sequence).map_err(|_|StoreError::corrupt("return prefix sequence"))?).fetch_optional(&mut **replay.tx).await.map_err(|e|StoreError::database("return accounting prefix",e))?.ok_or_else(||StoreError::corrupt("return accounting prefix missing"))?;
        let record = Box::pin(verified_record_replay(replay, admission, graphs, row)).await?;
        after = record.event.sequence().get();
    }
    let return_floor = replay
        .return_usage(tenant, run, sequence, digest)?
        .ok_or_else(|| StoreError::corrupt("return accounting proof missing"))?;
    if floor.validate_monotonic_after(&return_floor).is_ok() {
        return Ok(floor.clone());
    }
    return_floor
        .validate_monotonic_after(floor)
        .map_err(|_| StoreError::corrupt("incomparable shared return floor"))?;
    Ok(return_floor)
}

pub(super) async fn recognize_result_replay(
    replay: &mut Replay<'_, '_>,
    result: &PendingNodeResult,
) -> Result<Option<JournalEvent>, StoreError> {
    let journal = result.journal_head();
    let (event, _) = child_runs::anchored_event(
        replay.tx,
        journal.tenant_id(),
        journal.run_id(),
        i64::try_from(journal.sequence().get())
            .map_err(|_| StoreError::corrupt("return result sequence"))?,
    )
    .await?;
    if event.payload().kind().as_str() != EVENT_KIND {
        return Ok(None);
    }
    let row=query("SELECT * FROM stateknot.graph_frame_returns WHERE tenant_id=$1 AND run_id=$2 AND journal_sequence=$3").bind(journal.tenant_id().as_str()).bind(*journal.run_id().as_uuid()).bind(i64::try_from(journal.sequence().get()).map_err(|_|StoreError::corrupt("return result sequence"))?).fetch_optional(&mut **replay.tx).await.map_err(|e|StoreError::database("returned result whole witness",e))?.ok_or_else(||StoreError::corrupt("returned result witness missing"))?;
    let anchored = anchored_record(replay.tx, &row).await?;
    if replay
        .return_usage(
            journal.tenant_id(),
            journal.run_id(),
            anchored.event.sequence().get(),
            anchored.digest,
        )?
        .is_some()
    {
        if anchored.result != *result {
            return Err(StoreError::corrupt("returned result cached substitution"));
        }
        return Ok(Some(anchored.event));
    }
    drop(anchored);
    let admission =
        barriers::admission_snapshot(replay.tx, journal.tenant_id(), journal.run_id()).await?;
    let graphs = closure(
        replay.tx,
        journal.tenant_id(),
        admission.admission().intent().graph(),
    )
    .await?;
    let record = Box::pin(verified_record_replay(replay, &admission, &graphs, row)).await?;
    if record.result != *result {
        return Err(StoreError::corrupt("returned result substitution"));
    }
    Ok(Some(record.event))
}

pub(super) async fn recognize_completion(
    tx: &mut Transaction<'_, Postgres>,
    attempt: &NodeAttempt,
) -> Result<JournalEvent, StoreError> {
    let start = attempt.start();
    let activation = start.activation();
    let row=query("SELECT * FROM stateknot.graph_frame_returns WHERE tenant_id=$1 AND run_id=$2 AND caller_attempt_id=$3").bind(activation.tenant_id().as_str()).bind(*activation.run_id().as_uuid()).bind(*start.attempt_id().as_uuid()).fetch_optional(&mut **tx).await.map_err(|e|StoreError::database("returned completion",e))?.ok_or_else(||StoreError::corrupt("returned completion missing"))?;
    let admission =
        barriers::admission_snapshot(tx, activation.tenant_id(), activation.run_id()).await?;
    let graphs = closure(
        tx,
        activation.tenant_id(),
        admission.admission().intent().graph(),
    )
    .await?;
    let mut replay = Replay::new(tx);
    let record = Box::pin(verified_record_replay(
        &mut replay,
        &admission,
        &graphs,
        row,
    ))
    .await?;
    if record.intent.caller != start.head()
        || attempt
            .completion()
            .map(encode_node_attempt_completion)
            .transpose()?
            != Some(encode_node_attempt_completion(&record.completion)?)
    {
        return Err(StoreError::corrupt("returned completion substitution"));
    }
    Ok(record.event)
}

impl PostgresStore {
    /// Returns the exact closed offline schema and pin of framework returns.
    ///
    /// # Errors
    /// Rejects a local canonical encoding failure.
    pub fn graph_frame_return_event_schema()
    -> Result<(stateknot_core::SchemaReference, serde_json::Value), StoreError> {
        EVENT_SCHEMA
            .as_ref()
            .cloned()
            .map_err(|_| StoreError::encoding("frame return schema"))
    }

    /// Restores the whole return independently of current lease or active leaf.
    ///
    /// # Errors
    /// Rejects Root scope, missing admission, corruption or bounded replay limits.
    pub async fn load_graph_frame_return(
        &self,
        tenant: &TenantId,
        run: RunId,
        namespace: &GraphNamespace,
    ) -> Result<Option<StoredGraphFrameReturn>, StoreError> {
        Box::pin(self.load_graph_frame_return_inner(tenant, run, namespace)).await
    }
    async fn load_graph_frame_return_inner(
        &self,
        tenant: &TenantId,
        run: RunId,
        namespace: &GraphNamespace,
    ) -> Result<Option<StoredGraphFrameReturn>, StoreError> {
        if namespace.is_root() {
            return Err(StoreError::GraphFrameConflict);
        }
        let mut tx = self.begin_repeatable_read("whole return snapshot").await?;
        let Some(row) = row(&mut tx, tenant, run, namespace).await? else {
            tx.commit()
                .await
                .map_err(|e| StoreError::database("empty return snapshot commit", e))?;
            return Ok(None);
        };
        let admission = barriers::admission_snapshot(&mut tx, tenant, run).await?;
        let graphs = closure(&mut tx, tenant, admission.admission().intent().graph()).await?;
        let record = Box::pin(verified_record_replay(
            &mut Replay::new(&mut tx),
            &admission,
            &graphs,
            row,
        ))
        .await?;
        if record.intent.terminal_checkpoint.frame().namespace() != namespace
            || record.event.tenant_id() != tenant
            || record.event.run_id() != run
        {
            return Err(StoreError::corrupt("whole return lookup scope"));
        }
        tx.commit()
            .await
            .map_err(|e| StoreError::database("return snapshot commit", e))?;
        Ok(Some(record))
    }
}

fn verify_retry(
    record: &StoredGraphFrameReturn,
    plan: &GraphFrameBarrierPlan,
) -> Result<(), StoreError> {
    if record.intent.terminal_barrier != *plan.barrier()
        || Some(&record.intent.terminal_output) != plan.disposition().terminal_output()
        || record.intent.terminal_checkpoint.frame() != plan.barrier().base_checkpoint().frame()
        || record.intent.terminal_base_checkpoint_id
            != plan
                .barrier()
                .base_checkpoint()
                .checkpoint()
                .checkpoint_id()
        || record
            .intent
            .terminal_checkpoint
            .checkpoint()
            .checkpoint_id()
            != plan.barrier().successor().checkpoint_id()
    {
        return Err(StoreError::GraphFrameConflict);
    }
    Ok(())
}

impl PostgresStore {
    /// Settles an already durable terminal child proof and resumes its parent.
    ///
    /// The terminal barrier remains immutable. This transaction atomically binds
    /// its exact proof to the current framework caller, parent result/completion,
    /// stack pop and one journal fact. Actual pinned schema/reducer callbacks run
    /// before the mutation lock. Whole acknowledgment-loss recovery precedes new
    /// callbacks, observations or lease checks. DIRECT usage is a trusted complete
    /// Run-wide observation, including the entry, caller and terminal charges.
    ///
    /// # Errors
    /// Rejects nonterminal/substituted proofs, stale authority, open effects,
    /// incomplete accounting, exhausted budget, corruption and database failures.
    #[allow(clippy::too_many_arguments)]
    pub async fn return_graph_frame<V: GraphSchemaValidator + ?Sized, R: GraphReducer + ?Sized>(
        &self,
        terminal: GraphFrameBarrierPlan,
        event_id: EventId,
        fence: RunFence,
        observed: JournalHead,
        direct_usage: BudgetUsage,
        schemas: &V,
        reducer: &R,
    ) -> Result<GraphFrameReturnCommitOutcome, StoreError> {
        let frame = terminal.barrier().base_checkpoint().frame();
        let origin = frame.origin();
        if let Some(record) = Box::pin(self.load_graph_frame_return_inner(
            origin.tenant_id(),
            origin.run_id(),
            frame.namespace(),
        ))
        .await?
        {
            verify_retry(&record, &terminal)?;
            return Ok(GraphFrameReturnCommitOutcome::Idempotent(record));
        }
        if terminal.disposition().terminal_output().is_none()
            || fence.tenant_id() != origin.tenant_id()
            || fence.run_id() != origin.run_id()
        {
            return Err(StoreError::GraphFrameConflict);
        }
        let prepared =
            Box::pin(self.prepare_graph_frame_return(&terminal, schemas, reducer)).await?;
        Box::pin(self.return_graph_frame_inner(
            terminal,
            event_id,
            fence,
            observed,
            direct_usage,
            prepared,
        ))
        .await
    }
    async fn prepare_graph_frame_return<
        V: GraphSchemaValidator + ?Sized,
        R: GraphReducer + ?Sized,
    >(
        &self,
        plan: &GraphFrameBarrierPlan,
        schemas: &V,
        reducer: &R,
    ) -> Result<PendingNodeResultIntent, StoreError> {
        let head = plan.barrier().base_checkpoint();
        let base = head.checkpoint();
        let frame = head.frame();
        let tenant = base.tenant_id();
        let run = base.run_id();
        let namespace = frame.namespace();
        let mut snapshot = self
            .begin_repeatable_read("return planning snapshot")
            .await?;
        let admission = barriers::admission_snapshot(&mut snapshot, tenant, run).await?;
        let graphs = closure(
            &mut snapshot,
            tenant,
            admission.admission().intent().graph(),
        )
        .await?;
        let mut replay = Replay::new(&mut snapshot);
        Box::pin(usage_floor_before(
            &mut replay,
            &admission,
            &graphs,
            admission
                .run()
                .journal_head()
                .ok_or(StoreError::StaleJournalHead)?
                .sequence()
                .get(),
            &admission_usage_floor(&admission)?,
        ))
        .await?;
        let entry = Box::pin(verified_entry_replay(
            &mut replay,
            tenant,
            run,
            namespace,
            &admission,
            &graphs,
        ))
        .await?;
        let (child_base, _) = Box::pin(barriers::checkpoint_at_replay(
            &mut replay,
            &entry,
            &admission,
            &graphs,
            base,
        ))
        .await?;
        let results = Box::pin(barriers::results_on_checkpoint_replay(
            &mut replay,
            &child_base,
            plan.barrier().result_heads(),
        ))
        .await?;
        let target = graphs
            .get(&frame.target().definition_digest())
            .ok_or(StoreError::GraphFrameRejected)?;
        let parent_graph = graphs
            .get(&frame.origin().base_checkpoint().graph().definition_digest())
            .ok_or(StoreError::GraphFrameRejected)?;
        let call = parent_graph
            .frame_calls()
            .and_then(|calls| calls.call(frame.origin().node_id()))
            .ok_or(StoreError::GraphFrameRejected)?;
        let parent = load_parent(&mut replay, &entry, &admission, &graphs).await?;
        snapshot
            .commit()
            .await
            .map_err(|e| StoreError::database("return planning snapshot commit", e))?;
        let actual = catch_unwind(AssertUnwindSafe(|| {
            target.plan_frame_barrier(
                &child_base,
                &results,
                plan.barrier().successor().checkpoint_id(),
                schemas,
                reducer,
            )
        }))
        .map_err(|_| StoreError::GraphReplayDependencyUnavailable)?
        .map_err(|e| map_graph_replay_plan_error(&e))?;
        if &actual != plan {
            return Err(StoreError::GraphFrameConflict);
        }
        let prepared = match &parent {
            Parent::Root(cp) => {
                call.prepare_root_return(parent_graph, cp, target, &child_base, &actual)
            }
            Parent::Frame(cp) => {
                call.prepare_frame_return(parent_graph, cp, target, &child_base, &actual)
            }
        }
        .map_err(|_| StoreError::GraphFrameConflict)?;
        Ok(prepared)
    }
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn return_graph_frame_inner(
        &self,
        plan: GraphFrameBarrierPlan,
        event_id: EventId,
        fence: RunFence,
        observed: JournalHead,
        direct_usage: BudgetUsage,
        prepared: PendingNodeResultIntent,
    ) -> Result<GraphFrameReturnCommitOutcome, StoreError> {
        let head = plan.barrier().base_checkpoint();
        let base = head.checkpoint();
        let frame = head.frame();
        let tenant = base.tenant_id();
        let run = base.run_id();
        let namespace = frame.namespace();
        let mut tx = self.begin_mutation("whole graph frame return").await?;
        let admission = load_locked_agent_admission(&mut tx, tenant, run)
            .await?
            .ok_or(StoreError::AgentAdmissionConflict)?;
        let graphs = closure(&mut tx, tenant, admission.admission().intent().graph()).await?;
        let mut replay = Replay::new(&mut tx);
        Box::pin(usage_floor_before(
            &mut replay,
            &admission,
            &graphs,
            admission
                .run()
                .journal_head()
                .ok_or(StoreError::StaleJournalHead)?
                .sequence()
                .get(),
            &admission_usage_floor(&admission)?,
        ))
        .await?;
        if let Some(existing) = row(replay.tx, tenant, run, namespace).await? {
            let record = Box::pin(verified_record_replay(
                &mut replay,
                &admission,
                &graphs,
                existing,
            ))
            .await?;
            verify_retry(&record, &plan)?;
            tx.commit()
                .await
                .map_err(|e| StoreError::database("whole return retry commit", e))?;
            return Ok(GraphFrameReturnCommitOutcome::Idempotent(record));
        }
        let stored = admission.run();
        validate_runnable(stored)?;
        if stored.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::RunNotRunnable);
        }
        if stored.journal_head() != Some(&observed) {
            return Err(StoreError::StaleJournalHead);
        }
        let now = database_now(replay.tx, "return authority clock").await?;
        authorize_worker(stored, &fence, now)?;
        let entry = Box::pin(verified_entry_replay(
            &mut replay,
            tenant,
            run,
            namespace,
            &admission,
            &graphs,
        ))
        .await?;
        let terminal = Box::pin(barriers::terminal_by_base_replay(
            &mut replay,
            &entry,
            &admission,
            &graphs,
            base.checkpoint_id(),
        ))
        .await?;
        if terminal.barrier() != plan.barrier()
            || terminal.disposition() != *plan.disposition()
            || prepared != result_intent(&entry, &terminal, &graphs)?
        {
            return Err(StoreError::GraphFrameConflict);
        }
        Box::pin(verify_active_checkpoint(
            replay.tx,
            &entry,
            terminal.checkpoint(),
        ))
        .await?;
        let history = Box::pin(caller_bindings::verified_history_replay(
            &mut replay,
            &entry,
            &admission,
            &graphs,
            None,
        ))
        .await?;
        if history.current.fence() != &fence {
            return Err(StoreError::StaleFence);
        }
        let current =
            load_node_attempt_record(replay.tx, tenant, &run, history.current.attempt_id())
                .await?
                .ok_or(StoreError::NodeAttemptNotFound)?;
        if current.completion().is_some() {
            return Err(StoreError::GraphFrameConflict);
        }
        if load_pending_node_result_row(replay.tx, frame.origin())
            .await?
            .is_some()
        {
            return Err(StoreError::PendingNodeResultConflict);
        }
        let parent = load_parent(&mut replay, &entry, &admission, &graphs).await?;
        verify_parent_head(replay.tx, &parent).await?;
        let floor = caller_bindings::usage_floor_before(
            replay.tx,
            &entry,
            &admission,
            observed.sequence().get(),
            &terminal.direct_usage_after()?,
        )
        .await?;
        let floor = Box::pin(usage_floor_before(
            &mut replay,
            &admission,
            &graphs,
            observed.sequence().get(),
            &floor,
        ))
        .await?;
        direct_usage
            .validate_monotonic_after(&floor)
            .map_err(|_| StoreError::IncompleteChildAccounting)?;
        ensure_no_unsettled_tool_invocations(replay.tx, terminal.checkpoint().checkpoint()).await?;
        ensure_no_unsettled_model_invocations(replay.tx, terminal.checkpoint().checkpoint())
            .await?;
        let account = Box::pin(child_runs::load_account_inner(
            replay.tx, tenant, run, false,
        ))
        .await?;
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
        let intent = Intent {
            version: 1,
            entry_digest: entry.digest,
            terminal_checkpoint: terminal.checkpoint().head(),
            terminal_base_checkpoint_id: base.checkpoint_id(),
            terminal_barrier_digest: terminal.digest(),
            terminal_barrier: terminal.barrier().clone(),
            terminal_output: terminal
                .disposition()
                .terminal_output()
                .ok_or(StoreError::GraphFrameConflict)?
                .clone(),
            caller: history.current.head(),
            caller_compound_digest: history.digest,
            result_intent_digest: prepared.intent_digest(),
            budget: EntryBudget {
                observed_head: observed.clone(),
                direct_usage,
                delegated_usage,
                child_account_digest,
            },
        };
        let append = JournalAppend::new(
            stateknot_core::JournalExpectation::exact(observed.clone()),
            JournalEventIntent::worker(
                tenant.clone(),
                run,
                event_id,
                fence.clone(),
                payload(&intent)?,
            )
            .map_err(|_| StoreError::GraphFrameConflict)?,
        )
        .map_err(|_| StoreError::GraphFrameConflict)?;
        let event = JournalEvent::commit(append, now.max(observed.recorded_at()))
            .map_err(|e| map_event_commit_error(&e))?;
        let result = PendingNodeResult::commit(prepared, fence.clone(), event.head())
            .map_err(|_| StoreError::GraphFrameConflict)?;
        let completion =
            NodeAttemptCompletion::succeed(&history.current, result.head(), BudgetUsage::zero())
                .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
        let intent_digest = intent_digest(&intent)?;
        let digest = compound(intent_digest, &event, &result, &completion)?;
        let record = StoredGraphFrameReturn {
            event,
            intent,
            intent_digest,
            result,
            completion,
            digest,
        };
        let total = charged(&record)?
            .checked_accumulate(&record.intent.budget.delegated_usage)
            .map_err(|_| StoreError::IncompleteChildAccounting)?;
        admission
            .admission()
            .intent()
            .budget()
            .remaining(&total, record.event.recorded_at())
            .map_err(|_| StoreError::GraphFrameLimitExceeded)?;
        insert_event_components(replay.tx, &record.event, record.digest).await?;
        insert_record(replay.tx, &record).await?;
        pop(replay.tx, &record, &parent).await?;
        insert_pending_node_result(
            replay.tx,
            &record.result,
            history.current.attempt_id(),
            &fence,
        )
        .await?;
        insert_pending_node_result_bindings(replay.tx, &record.result, &fence).await?;
        insert_node_attempt_completion(replay.tx, &history.current, &record.completion).await?;
        update_run_head(replay.tx, &record.event, None).await?;
        revalidate_worker_after_components(replay.tx, &fence).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::database("whole return commit", e))?;
        Ok(GraphFrameReturnCommitOutcome::Committed(record))
    }
}
async fn load_parent(
    replay: &mut Replay<'_, '_>,
    entry: &StoredGraphFrameEntry,
    admission: &StoredAgentAdmission,
    graphs: &BTreeMap<Digest, CompiledGraph>,
) -> Result<Parent, StoreError> {
    let origin = entry.entry.checkpoint().frame().origin();
    let tenant = origin.tenant_id();
    let run = origin.run_id();
    let base = origin.base_checkpoint();
    if origin.graph_namespace().is_root() {
        let cp = decode_checkpoint(
            checkpoint_row(
                replay.tx,
                tenant,
                run,
                origin.graph_namespace(),
                base.checkpoint_id(),
            )
            .await?,
        )?;
        verify_checkpoint_anchor(replay.tx, &cp).await?;
        if cp.head() != *base {
            return Err(StoreError::corrupt("return parent root substitution"));
        }
        Ok(Parent::Root(Box::new(cp)))
    } else {
        let parent_entry = Box::pin(verified_entry_replay(
            replay,
            tenant,
            run,
            origin.graph_namespace(),
            admission,
            graphs,
        ))
        .await?;
        let (cp, _) = Box::pin(barriers::checkpoint_at_replay(
            replay,
            &parent_entry,
            admission,
            graphs,
            base,
        ))
        .await?;
        Ok(Parent::Frame(Box::new(cp)))
    }
}
async fn verify_parent_head(
    tx: &mut Transaction<'_, Postgres>,
    parent: &Parent,
) -> Result<(), StoreError> {
    let cp = parent.checkpoint();
    let head=match parent{
        Parent::Root(_)=>query_as::<_,(Option<Uuid>,Option<i64>,Option<Vec<u8>>)>("SELECT checkpoint_id,checkpoint_superstep,checkpoint_digest FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2").bind(cp.tenant_id().as_str()).bind(*cp.run_id().as_uuid()).fetch_one(&mut **tx).await.map_err(|e|StoreError::database("return root head",e))?,
        Parent::Frame(frame)=>{
            let row=query_as::<_,(Uuid,i64,Vec<u8>,Vec<u8>,Vec<u8>)>("SELECT checkpoint_id,superstep,checkpoint_digest,frame_identity_digest,frame_checkpoint_digest FROM stateknot.graph_frame_heads WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3").bind(cp.tenant_id().as_str()).bind(*cp.run_id().as_uuid()).bind(frame.frame().namespace().as_str()).fetch_one(&mut **tx).await.map_err(|e|StoreError::database("return scoped parent head",e))?;
            if decode_digest(&row.3,"return parent frame head")?!=frame.frame().digest()||decode_digest(&row.4,"return parent scoped head")?!=frame.digest(){return Err(StoreError::StaleCheckpointHead);} (Some(row.0),Some(row.1),Some(row.2))
        }
    };
    if head.0 != Some(*cp.checkpoint_id().as_uuid())
        || head.1
            != Some(
                i64::try_from(cp.superstep().get())
                    .map_err(|_| StoreError::GraphFrameLimitExceeded)?,
            )
        || head.2.as_deref() != Some(cp.digest().as_bytes())
    {
        return Err(StoreError::StaleCheckpointHead);
    }
    Ok(())
}
async fn pop(
    tx: &mut Transaction<'_, Postgres>,
    record: &StoredGraphFrameReturn,
    parent: &Parent,
) -> Result<(), StoreError> {
    let frame = record.intent.terminal_checkpoint.frame();
    let parent_digest = match parent {
        Parent::Root(_) => None,
        Parent::Frame(cp) => Some(cp.frame().digest().as_bytes().to_vec()),
    };
    let result=query("UPDATE stateknot.graph_frame_stacks SET active_namespace=$3,active_frame_identity_digest=$4 WHERE tenant_id=$1 AND run_id=$2 AND active_namespace=$5 AND active_frame_identity_digest=$6").bind(frame.origin().tenant_id().as_str()).bind(*frame.origin().run_id().as_uuid()).bind(frame.origin().graph_namespace().as_str()).bind(parent_digest).bind(frame.namespace().as_str()).bind(frame.digest().as_bytes()).execute(&mut **tx).await.map_err(|e|StoreError::database("whole frame pop",e))?;
    if result.rows_affected() != 1 {
        return Err(StoreError::GraphFrameConflict);
    }
    Ok(())
}
async fn insert_record(
    tx: &mut Transaction<'_, Postgres>,
    record: &StoredGraphFrameReturn,
) -> Result<(), StoreError> {
    let i = &record.intent;
    let head = i.terminal_checkpoint.checkpoint();
    let frame = i.terminal_checkpoint.frame();
    let origin = frame.origin();
    let base = origin.base_checkpoint();
    let event = &record.event;
    query("INSERT INTO stateknot.graph_frame_returns(tenant_id,run_id,graph_namespace,frame_identity_digest,entry_digest,terminal_base_checkpoint_id,terminal_checkpoint_id,terminal_superstep,terminal_checkpoint_digest,terminal_frame_checkpoint_digest,terminal_barrier_digest,caller_attempt_id,caller_start_digest,caller_compound_digest,parent_namespace,parent_checkpoint_id,parent_superstep,parent_checkpoint_digest,node_id,activation_input_digest,result_intent_digest,result_record_digest,completion_digest,intent_digest,compound_digest,return_bytes,journal_sequence,journal_event_id,journal_recorded_at,journal_digest) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30)")
        .bind(origin.tenant_id().as_str()).bind(*origin.run_id().as_uuid()).bind(frame.namespace().as_str()).bind(frame.digest().as_bytes()).bind(i.entry_digest.as_bytes()).bind(*i.terminal_base_checkpoint_id.as_uuid()).bind(*head.checkpoint_id().as_uuid()).bind(i64::try_from(head.superstep().get()).map_err(|_|StoreError::GraphFrameLimitExceeded)?).bind(head.digest().as_bytes()).bind(i.terminal_checkpoint.digest().as_bytes()).bind(i.terminal_barrier_digest.as_bytes())
        .bind(*i.caller.attempt_id().as_uuid()).bind(i.caller.digest().as_bytes()).bind(i.caller_compound_digest.as_bytes()).bind(origin.graph_namespace().as_str()).bind(*base.checkpoint_id().as_uuid()).bind(i64::try_from(base.superstep().get()).map_err(|_|StoreError::GraphFrameLimitExceeded)?).bind(base.digest().as_bytes()).bind(origin.node_id().as_str()).bind(origin.input_digest().as_bytes()).bind(record.result.intent().intent_digest().as_bytes()).bind(record.result.digest().as_bytes()).bind(record.completion.digest().as_bytes()).bind(record.intent_digest.as_bytes()).bind(record.digest.as_bytes()).bind(encode(record)?).bind(i64::try_from(event.sequence().get()).map_err(|_|StoreError::JournalSequenceExhausted)?).bind(*event.event_id().as_uuid()).bind(to_database_time(event.recorded_at())?).bind(event.digest().as_bytes()).execute(&mut **tx).await.map_err(|e|StoreError::database("whole return insert",e))?;
    Ok(())
}
