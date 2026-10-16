// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Framework-only physical starts preserve the existing logical child.
#[allow(clippy::wildcard_imports)]
use super::*;

const EVENT_KIND: &str = "graph-frame-caller-rebound";
const SCHEMA_ID: &str = "https://stknot.com/schemas/store/graph-frame-caller-event/1.0.0";
const INTENT_DOMAIN: &[u8] = b"stateknot-postgres-frame-caller-intent-v1\0";
const COMPOUND_DOMAIN: &[u8] = b"stateknot-postgres-frame-caller-compound-v1\0";
const MAX_BINDING_BYTES: usize = MAX_NODE_ATTEMPT_START_BYTES + 2 * MAX_ENTRY_BYTES;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    version: u8,
    entry_digest: Digest,
    frame_checkpoint: GraphFrameCheckpointHead,
    previous: stateknot_core::NodeAttemptStartHead,
    previous_compound_digest: Digest,
    caller_attempt_id: AttemptId,
    fence: RunFence,
    budget: EntryBudget,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    version: u8,
    intent: Intent,
    intent_digest: Digest,
    start: NodeAttemptStart,
    compound_digest: Digest,
}
struct Binding {
    event: JournalEvent,
    intent: Intent,
    intent_digest: Digest,
    start: NodeAttemptStart,
    digest: Digest,
}
fn intent_digest(intent: &Intent) -> Result<Digest, StoreError> {
    digest_wire(INTENT_DOMAIN, intent)
}
fn compound(
    intent_digest: Digest,
    event: &JournalEvent,
    start: &NodeAttemptStart,
) -> Result<Digest, StoreError> {
    #[derive(Serialize)]
    struct Preimage<'a> {
        version: u8,
        intent_digest: Digest,
        event: &'a JournalEvent,
        start: &'a NodeAttemptStart,
    }
    digest_wire(
        COMPOUND_DOMAIN,
        &Preimage {
            version: 1,
            intent_digest,
            event,
            start,
        },
    )
}
fn decode(bytes: &[u8]) -> Result<Wire, StoreError> {
    if bytes.is_empty() || bytes.len() > MAX_BINDING_BYTES {
        return Err(StoreError::corrupt("framework caller binding byte bound"));
    }
    let wire: Wire = serde_json::from_slice(bytes)
        .map_err(|_| StoreError::corrupt("framework caller binding wire"))?;
    if wire.version != 1
        || wire.intent.version != 1
        || serde_json_canonicalizer::to_vec(&wire)
            .map_err(|_| StoreError::corrupt("framework caller binding canonicalization"))?
            != bytes
    {
        return Err(StoreError::corrupt(
            "framework caller binding version/canonical bytes",
        ));
    }
    Ok(wire)
}
fn encode(binding: &Binding) -> Result<Vec<u8>, StoreError> {
    let wire = Wire {
        version: 1,
        intent: binding.intent.clone(),
        intent_digest: binding.intent_digest,
        start: binding.start.clone(),
        compound_digest: binding.digest,
    };
    let bytes = serde_json_canonicalizer::to_vec(&wire)
        .map_err(|_| StoreError::encoding("framework caller binding"))?;
    if bytes.is_empty() || bytes.len() > MAX_BINDING_BYTES {
        return Err(StoreError::GraphFrameLimitExceeded);
    }
    Ok(bytes)
}

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
            "frame_identity_digest":digest,"previous_attempt_id":id,"previous_start_digest":digest,
            "caller_attempt_id":id,"intent_digest":digest
        },
        "required":["version","graph_namespace","frame_identity_digest","previous_attempt_id","previous_start_digest","caller_attempt_id","intent_digest"]
    });
    let bytes =
        serde_json_canonicalizer::to_vec(&document).map_err(|_| "framework caller event schema")?;
    let reference = stateknot_core::SchemaReference::new(
        SCHEMA_ID
            .parse()
            .map_err(|_| "framework caller schema ID")?,
        stateknot_core::Version::new(1, 0, 0),
        Digest::sha256(bytes),
    );
    Ok((reference, document))
});
fn payload(intent: &Intent) -> Result<JournalPayload, StoreError> {
    #[derive(Serialize)]
    struct Data<'a> {
        version: u8,
        graph_namespace: &'a GraphNamespace,
        frame_identity_digest: Digest,
        previous_attempt_id: AttemptId,
        previous_start_digest: Digest,
        caller_attempt_id: AttemptId,
        intent_digest: Digest,
    }
    let frame = intent.frame_checkpoint.frame();
    let bytes = serde_json_canonicalizer::to_vec(&Data {
        version: 1,
        graph_namespace: frame.namespace(),
        frame_identity_digest: frame.digest(),
        previous_attempt_id: intent.previous.attempt_id(),
        previous_start_digest: intent.previous.digest(),
        caller_attempt_id: intent.caller_attempt_id,
        intent_digest: intent_digest(intent)?,
    })
    .map_err(|_| StoreError::encoding("framework caller event payload"))?;
    JournalPayload::new(
        EVENT_SCHEMA
            .as_ref()
            .map_err(|_| StoreError::encoding("framework caller schema"))?
            .0
            .clone(),
        EVENT_KIND
            .parse()
            .map_err(|_| StoreError::encoding("framework caller event kind"))?,
        BoundedJson::from_slice(&bytes)
            .map_err(|_| StoreError::encoding("framework caller payload bound"))?,
    )
    .map_err(|_| StoreError::encoding("framework caller payload"))
}
fn materialize(intent: Intent, event: JournalEvent) -> Result<Binding, StoreError> {
    if event.source().worker_fence() != Some(&intent.fence) || event.payload() != &payload(&intent)?
    {
        return Err(StoreError::GraphFrameConflict);
    }
    let start = NodeAttemptStart::new(
        intent.previous.activation().clone(),
        intent.caller_attempt_id,
        intent.fence.clone(),
        event.head(),
    )
    .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
    let previous = NodeAttemptStart::new(
        intent.previous.activation().clone(),
        intent.previous.attempt_id(),
        intent.previous.fence().clone(),
        intent.previous.journal_head().clone(),
    )
    .map_err(|_| StoreError::corrupt("framework previous start"))?;
    if previous.head() != intent.previous {
        return Err(StoreError::corrupt("framework previous start digest"));
    }
    let mut history = NodeAttemptHistoryVerifier::after(NodeAttempt::executing(previous));
    history
        .verify_next(&NodeAttempt::executing(start.clone()))
        .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
    let intent_digest = intent_digest(&intent)?;
    let digest = compound(intent_digest, &event, &start)?;
    Ok(Binding {
        event,
        intent,
        intent_digest,
        start,
        digest,
    })
}
fn charged(binding: &Binding) -> Result<BudgetUsage, StoreError> {
    use stateknot_core::{ByteCount, ExecutionCount};
    let event_bytes = serde_json_canonicalizer::to_vec(&binding.event)
        .map_err(|_| StoreError::encoding("framework caller event bytes"))?
        .len();
    let delta = BudgetUsage::builder()
        .graph_steps(ExecutionCount::new(1))
        .retries(ExecutionCount::new(1))
        .event_bytes(ByteCount::new(
            u64::try_from(event_bytes).map_err(|_| StoreError::GraphFrameLimitExceeded)?,
        ))
        .build()
        .map_err(|_| StoreError::GraphFrameRejected)?;
    binding
        .intent
        .budget
        .direct_usage
        .checked_accumulate(&delta)
        .map_err(|_| StoreError::GraphFrameLimitExceeded)
}

pub(super) struct History {
    pub(super) current: NodeAttemptStart,
    pub(super) event: JournalEvent,
    pub(super) digest: Digest,
    // Compact heads and counters only. Whole checkpoint buffers are released
    // after each independent lineage verification.
    checkpoints: Vec<(GraphFrameCheckpointHead, BudgetUsage)>,
    matched: Option<(JournalEvent, NodeAttemptStart)>,
    pub(super) floor: BudgetUsage,
}

// Authenticate the bounded physical chain without calling checkpoint replay.
// Barrier replay uses this direction too, so result ownership cannot recursively
// re-enter the same full checkpoint history.
#[allow(clippy::too_many_lines)]
async fn history_anchors(
    tx: &mut Transaction<'_, Postgres>,
    entry: &StoredGraphFrameEntry,
    admission: &StoredAgentAdmission,
    through: u64,
    requested: Option<&RunFence>,
) -> Result<History, StoreError> {
    let initial = entry.entry.checkpoint();
    let tenant = initial.checkpoint().tenant_id();
    let run = initial.checkpoint().run_id();
    let namespace = initial.frame().namespace();
    let mut history = History {
        current: entry.entry.start().clone(),
        event: entry.event.clone(),
        digest: entry.digest,
        matched: requested
            .filter(|f| *f == entry.entry.start().fence())
            .map(|_| (entry.event.clone(), entry.entry.start().clone())),
        checkpoints: Vec::new(),
        floor: entry.direct_usage_after()?,
    };
    verify_claim(tx, &history.current).await?;
    let mut verifier =
        NodeAttemptHistoryVerifier::after(NodeAttempt::executing(history.current.clone()));
    let mut bytes_seen = 0usize;
    let mut count = 1usize;
    let through =
        i64::try_from(through).map_err(|_| StoreError::corrupt("framework history sequence"))?;
    loop {
        let row=query("SELECT * FROM stateknot.graph_frame_caller_bindings WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3 AND worker_epoch>$4 AND journal_sequence<=$5 ORDER BY worker_epoch LIMIT 1")
            .bind(tenant.as_str()).bind(*run.as_uuid()).bind(namespace.as_str())
            .bind(i64::try_from(history.current.fence().epoch().get()).map_err(|_|StoreError::corrupt("framework history epoch"))?).bind(through)
            .fetch_optional(&mut **tx).await.map_err(|e|StoreError::database("framework history row",e))?;
        let Some(row) = row else {
            break;
        };
        count = count
            .checked_add(1)
            .ok_or(StoreError::NodeAttemptLimitExceeded)?;
        if count > ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE {
            return Err(StoreError::corrupt("framework attempt history bound"));
        }
        let get_error = |e| StoreError::database("framework history column", e);
        let bytes: Vec<u8> = row.try_get("binding_bytes").map_err(get_error)?;
        bytes_seen = bytes_seen.saturating_add(bytes.len());
        if bytes_seen > GraphReplayLimits::default().maximum_barrier_result_bytes() {
            return Err(StoreError::GraphReplayResourceLimit);
        }
        let wire = decode(&bytes)?;
        let intent = &wire.intent;
        let head = &intent.frame_checkpoint;
        if Digest::sha256(&bytes)
            != decode_digest(
                &row.try_get::<Vec<u8>, _>("binding_checksum")
                    .map_err(get_error)?,
                "framework binding checksum",
            )?
            || intent.entry_digest != entry.digest
            || head.frame() != initial.frame()
            || intent.previous != history.current.head()
            || intent.previous_compound_digest != history.digest
            || intent.caller_attempt_id != wire.start.attempt_id()
            || intent.fence != *wire.start.fence()
            || intent.budget.observed_head.sequence().get()
                >= u64::try_from(
                    row.try_get::<i64, _>("journal_sequence")
                        .map_err(get_error)?,
                )
                .map_err(|_| StoreError::corrupt("framework journal sequence"))?
        {
            return Err(StoreError::corrupt("framework history chain"));
        }
        let actual = load_node_attempt_record(tx, tenant, &run, wire.start.attempt_id())
            .await?
            .ok_or_else(|| StoreError::corrupt("framework start missing"))?;
        returns::verify_completion_anchor(tx, &actual).await?;
        if actual.start() != &wire.start {
            return Err(StoreError::corrupt("framework start components"));
        }
        verify_claim(tx, &wire.start).await?;
        let checkpoint = decode_frame_checkpoint(
            checkpoint_row(
                tx,
                tenant,
                run,
                namespace,
                head.checkpoint().checkpoint_id(),
            )
            .await?,
        )?;
        if checkpoint.head() != *head
            || head.checkpoint().journal_head().sequence() > intent.budget.observed_head.sequence()
            || head.checkpoint().journal_head().recorded_at()
                > intent.budget.observed_head.recorded_at()
        {
            return Err(StoreError::corrupt("framework active checkpoint"));
        }
        drop(checkpoint);
        let row_head = child_runs::row_head(&row, tenant, run)?;
        let (event, projection) = child_runs::anchored_event(
            tx,
            tenant,
            run,
            i64::try_from(row_head.sequence().get())
                .map_err(|_| StoreError::corrupt("framework event sequence"))?,
        )
        .await?;
        let binding = materialize(intent.clone(), event)
            .map_err(|_| StoreError::corrupt("framework binding materialization"))?;
        if binding.event.head() != row_head
            || binding.start != wire.start
            || binding.intent_digest != wire.intent_digest
            || binding.digest != wire.compound_digest
            || projection != Some(binding.digest)
            || binding.start.journal_head() != &row_head
        {
            return Err(StoreError::corrupt("framework whole event binding"));
        }
        verify_columns(&row, &binding)?;
        let (before, _) = child_runs::anchored_event(
            tx,
            tenant,
            run,
            i64::try_from(binding.event.sequence().get() - 1)
                .map_err(|_| StoreError::corrupt("framework predecessor"))?,
        )
        .await?;
        if intent.budget.observed_head != before.head()
            || binding.event.previous_digest() != Some(before.digest())
            || binding.event.recorded_at() < before.recorded_at()
            || before.sequence() < history.event.sequence()
        {
            return Err(StoreError::corrupt("framework observation"));
        }
        verifier
            .verify_next(&actual)
            .map_err(|_| StoreError::corrupt("framework physical history"))?;
        intent
            .budget
            .direct_usage
            .validate_monotonic_after(&history.floor)
            .map_err(|_| StoreError::corrupt("framework shared usage regression"))?;
        let floor = charged(&binding)?;
        let total = floor
            .checked_accumulate(&intent.budget.delegated_usage)
            .map_err(|_| StoreError::corrupt("framework accounting"))?;
        admission
            .admission()
            .intent()
            .budget()
            .remaining(&total, binding.event.recorded_at())
            .map_err(|_| StoreError::corrupt("framework budget limit"))?;
        if requested == Some(binding.start.fence()) {
            history.matched = Some((binding.event.clone(), binding.start.clone()));
        }
        history
            .checkpoints
            .push((head.clone(), intent.budget.direct_usage.clone()));
        history.current = binding.start;
        history.event = binding.event;
        history.digest = binding.digest;
        history.floor = floor;
    }
    // A chain beginning before the original caller epoch cannot be hidden by
    // the ordered seek; every physical attempt belongs to exactly this chain.
    let total=query_scalar::<_,i64>("SELECT count(*) FROM stateknot.graph_frame_caller_bindings WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3 AND journal_sequence<=$4")
        .bind(tenant.as_str()).bind(*run.as_uuid()).bind(namespace.as_str()).bind(through).fetch_one(&mut **tx).await.map_err(|e|StoreError::database("framework history count",e))?;
    if total != i64::try_from(count - 1).map_err(|_| StoreError::corrupt("framework count"))? {
        return Err(StoreError::corrupt("framework hidden history"));
    }
    Ok(history)
}

#[allow(clippy::too_many_lines)]
fn verify_columns(row: &PgRow, binding: &Binding) -> Result<(), StoreError> {
    let get_error = |e| StoreError::database("framework binding column", e);
    let intent = &binding.intent;
    let head = &intent.frame_checkpoint;
    let digests = [
        ("frame_identity_digest", head.frame().digest()),
        ("previous_start_digest", intent.previous.digest()),
        ("caller_start_digest", binding.start.digest()),
        ("active_checkpoint_digest", head.checkpoint().digest()),
        ("active_frame_checkpoint_digest", head.digest()),
        ("previous_compound_digest", intent.previous_compound_digest),
        ("compound_digest", binding.digest),
    ];
    for (column, expected) in digests {
        if decode_digest(
            &row.try_get::<Vec<u8>, _>(column).map_err(get_error)?,
            "framework binding projection",
        )? != expected
        {
            return Err(StoreError::corrupt("framework binding digest column"));
        }
    }
    let ids = [
        (
            "previous_attempt_id",
            *intent.previous.attempt_id().as_uuid(),
        ),
        ("caller_attempt_id", *intent.caller_attempt_id.as_uuid()),
        ("worker_attempt_id", *intent.fence.attempt_id().as_uuid()),
        (
            "active_checkpoint_id",
            *head.checkpoint().checkpoint_id().as_uuid(),
        ),
    ];
    for (column, expected) in ids {
        if row.try_get::<Uuid, _>(column).map_err(get_error)? != expected {
            return Err(StoreError::corrupt("framework binding ID column"));
        }
    }
    if row
        .try_get::<String, _>("graph_namespace")
        .map_err(get_error)?
        != head.frame().namespace().as_str()
        || row.try_get::<i64, _>("worker_epoch").map_err(get_error)?
            != i64::try_from(intent.fence.epoch().get())
                .map_err(|_| StoreError::corrupt("framework epoch"))?
        || row
            .try_get::<i64, _>("active_superstep")
            .map_err(get_error)?
            != i64::try_from(head.checkpoint().superstep().get())
                .map_err(|_| StoreError::corrupt("framework superstep"))?
    {
        return Err(StoreError::corrupt("framework binding projection"));
    }
    Ok(())
}

async fn verified_history(
    tx: &mut Transaction<'_, Postgres>,
    entry: &StoredGraphFrameEntry,
    admission: &StoredAgentAdmission,
    graphs: &BTreeMap<Digest, CompiledGraph>,
    requested: Option<&RunFence>,
) -> Result<History, StoreError> {
    let mut replay = Replay::new(tx);
    Box::pin(verified_history_replay(
        &mut replay,
        entry,
        admission,
        graphs,
        requested,
    ))
    .await
}

pub(super) async fn verified_history_replay(
    replay: &mut Replay<'_, '_>,
    entry: &StoredGraphFrameEntry,
    admission: &StoredAgentAdmission,
    graphs: &BTreeMap<Digest, CompiledGraph>,
    requested: Option<&RunFence>,
) -> Result<History, StoreError> {
    let through = admission
        .run()
        .journal_head()
        .ok_or(StoreError::StaleJournalHead)?
        .sequence()
        .get();
    let history = Box::pin(history_anchors(
        replay.tx, entry, admission, through, requested,
    ))
    .await?;
    for (head, direct) in &history.checkpoints {
        let (_, floor) =
            barriers::checkpoint_at_replay(replay, entry, admission, graphs, head.checkpoint())
                .await?;
        direct
            .validate_monotonic_after(&floor)
            .map_err(|_| StoreError::corrupt("framework checkpoint usage regression"))?;
    }
    let count = count_node_attempts(replay.tx, entry.entry.start().activation()).await?;
    if count != history.checkpoints.len() + 1 {
        return Err(StoreError::corrupt("framework unrelated physical attempt"));
    }
    Ok(history)
}

pub(super) async fn usage_floor_before(
    tx: &mut Transaction<'_, Postgres>,
    entry: &StoredGraphFrameEntry,
    admission: &StoredAgentAdmission,
    through: u64,
    checkpoint_floor: &BudgetUsage,
) -> Result<BudgetUsage, StoreError> {
    let history = Box::pin(history_anchors(tx, entry, admission, through, None)).await?;
    if checkpoint_floor
        .validate_monotonic_after(&history.floor)
        .is_ok()
    {
        return Ok(checkpoint_floor.clone());
    }
    history
        .floor
        .validate_monotonic_after(checkpoint_floor)
        .map_err(|_| StoreError::corrupt("framework incomparable usage floor"))?;
    Ok(history.floor)
}

pub(super) fn is_framework_kind(kind: &str) -> bool {
    kind == GraphFrameEntryPlan::EVENT_KIND || kind == EVENT_KIND
}

pub(super) async fn recognize_start(
    tx: &mut Transaction<'_, Postgres>,
    start: &NodeAttemptStart,
    event: &JournalEvent,
) -> Result<JournalEvent, StoreError> {
    let tenant = start.activation().tenant_id();
    let run = start.activation().run_id();
    let namespace=query_scalar::<_,String>("SELECT graph_namespace FROM stateknot.graph_frame_caller_bindings WHERE tenant_id=$1 AND run_id=$2 AND caller_attempt_id=$3 AND journal_sequence=$4")
        .bind(tenant.as_str()).bind(*run.as_uuid()).bind(*start.attempt_id().as_uuid()).bind(i64::try_from(event.sequence().get()).map_err(|_|StoreError::corrupt("framework start sequence"))?).fetch_optional(&mut **tx).await.map_err(|e|StoreError::database("framework start binding",e))?.ok_or_else(||StoreError::corrupt("framework start compound missing"))?;
    let run_row = query_as::<_, RunRow>(SELECT_RUN)
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| StoreError::database("framework Run", e))?
        .ok_or(StoreError::RunNotFound)?;
    let admission_row = load_agent_admission_row(tx, tenant, run)
        .await?
        .ok_or(StoreError::AgentAdmissionConflict)?;
    let admission = verify_stored_agent_admission(tx, decode_run(run_row)?, admission_row).await?;
    let graphs = closure(tx, tenant, admission.admission().intent().graph()).await?;
    let namespace =
        GraphNamespace::new(namespace).map_err(|_| StoreError::corrupt("framework namespace"))?;
    let entry = Box::pin(verified_entry(
        tx, tenant, run, &namespace, &admission, &graphs,
    ))
    .await?;
    let history = Box::pin(verified_history(
        tx,
        &entry,
        &admission,
        &graphs,
        Some(start.fence()),
    ))
    .await?;
    let (actual_event, actual_start) = history
        .matched
        .ok_or_else(|| StoreError::corrupt("framework start epoch missing"))?;
    if actual_start != *start || actual_event != *event {
        return Err(StoreError::corrupt("framework start substitution"));
    }
    Ok(actual_event)
}

impl PostgresStore {
    /// Returns the closed offline schema and immutable pin for caller rebinding.
    ///
    /// # Errors
    /// Rejects a local canonical schema encoding failure.
    pub fn graph_frame_caller_event_schema()
    -> Result<(stateknot_core::SchemaReference, serde_json::Value), StoreError> {
        EVENT_SCHEMA
            .as_ref()
            .cloned()
            .map_err(|_| StoreError::encoding("framework caller schema"))
    }

    /// Atomically binds a new physical framework caller to the existing child.
    ///
    /// The logical activation, child identity and child checkpoint do not change.
    /// A committed epoch is authenticated before new observation, usage or lease
    /// checks; `Idempotent` grants no permission to dispatch application code.
    /// A fresh epoch shares the original Run budget and bounded attempt history.
    /// `direct_usage` is a trusted complete Run-wide DIRECT-only observation.
    ///
    /// # Errors
    /// Rejects substituted frames, stale authority, incomplete accounting,
    /// exhausted retries, unresolved effects, corruption and database failures.
    #[allow(clippy::too_many_arguments)]
    pub async fn rebind_graph_frame_caller(
        &self,
        frame: &GraphFrameIdentity,
        caller_attempt_id: AttemptId,
        event_id: EventId,
        fence: RunFence,
        observed: JournalHead,
        direct_usage: BudgetUsage,
    ) -> Result<NodeAttemptCommitOutcome, StoreError> {
        Box::pin(self.rebind_graph_frame_caller_inner(
            frame,
            caller_attempt_id,
            event_id,
            fence,
            observed,
            direct_usage,
        ))
        .await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn rebind_graph_frame_caller_inner(
        &self,
        frame: &GraphFrameIdentity,
        caller_attempt_id: AttemptId,
        event_id: EventId,
        fence: RunFence,
        observed: JournalHead,
        direct_usage: BudgetUsage,
    ) -> Result<NodeAttemptCommitOutcome, StoreError> {
        let tenant = frame.origin().tenant_id();
        let run = frame.origin().run_id();
        let namespace = frame.namespace();
        if fence.tenant_id() != tenant || fence.run_id() != run {
            return Err(StoreError::GraphFrameConflict);
        }
        let mut tx = self.begin_mutation("framework caller rebinding").await?;
        let admission = load_locked_agent_admission(&mut tx, tenant, run)
            .await?
            .ok_or(StoreError::AgentAdmissionConflict)?;
        let graphs = closure(&mut tx, tenant, admission.admission().intent().graph()).await?;
        let entry = Box::pin(verified_entry(
            &mut tx, tenant, run, namespace, &admission, &graphs,
        ))
        .await?;
        if entry.entry.checkpoint().frame() != frame {
            return Err(StoreError::GraphFrameConflict);
        }
        let history = Box::pin(verified_history(
            &mut tx,
            &entry,
            &admission,
            &graphs,
            Some(&fence),
        ))
        .await?;
        if let Some((event, start)) = history.matched {
            let attempt = load_node_attempt_record(&mut tx, tenant, &run, start.attempt_id())
                .await?
                .ok_or(StoreError::NodeAttemptNotFound)?;
            if attempt.completion().is_some() {
                Box::pin(super::recognize_completion(&mut tx, &attempt)).await?;
            }
            tx.commit()
                .await
                .map_err(|e| StoreError::database("framework rebinding retry", e))?;
            return Ok(NodeAttemptCommitOutcome::Idempotent { event, attempt });
        }
        let stored = admission.run();
        validate_runnable(stored)?;
        if stored.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::RunNotRunnable);
        }
        if stored.journal_head() != Some(&observed) {
            return Err(StoreError::StaleJournalHead);
        }
        let now = database_now(&mut tx, "framework rebinding authority").await?;
        authorize_worker(stored, &fence, now)?;
        if history.checkpoints.len() + 1 >= ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE {
            return Err(StoreError::NodeAttemptLimitExceeded);
        }
        let (checkpoint, floor) = Box::pin(barriers::current_checkpoint(
            &mut tx, &entry, &admission, &graphs,
        ))
        .await?;
        Box::pin(verify_active_checkpoint(&mut tx, &entry, &checkpoint)).await?;
        direct_usage
            .validate_monotonic_after(&floor)
            .map_err(|_| StoreError::IncompleteChildAccounting)?;
        direct_usage
            .validate_monotonic_after(&history.floor)
            .map_err(|_| StoreError::IncompleteChildAccounting)?;
        let unresolved=query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM stateknot.tool_invocations WHERE tenant_id=$1 AND run_id=$2 AND current_status NOT IN ('committed','failed')) OR EXISTS(SELECT 1 FROM stateknot.model_invocations WHERE tenant_id=$1 AND run_id=$2 AND current_status NOT IN ('committed','failed'))")
            .bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(&mut *tx).await.map_err(|e|StoreError::database("framework unresolved effects",e))?;
        if unresolved {
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
        let intent = Intent {
            version: 1,
            entry_digest: entry.digest,
            frame_checkpoint: checkpoint.head(),
            previous: history.current.head(),
            previous_compound_digest: history.digest,
            caller_attempt_id,
            fence: fence.clone(),
            budget: EntryBudget {
                observed_head: observed.clone(),
                direct_usage,
                delegated_usage,
                child_account_digest,
            },
        };
        let event_intent = JournalEventIntent::worker(
            tenant.clone(),
            run,
            event_id,
            fence.clone(),
            payload(&intent)?,
        )
        .map_err(|_| StoreError::GraphFrameConflict)?;
        let append = JournalAppend::new(
            stateknot_core::JournalExpectation::exact(observed.clone()),
            event_intent,
        )
        .map_err(|_| StoreError::GraphFrameConflict)?;
        let event = JournalEvent::commit(append, now.max(observed.recorded_at()))
            .map_err(|e| map_event_commit_error(&e))?;
        let binding = materialize(intent, event)?;
        let total = charged(&binding)?
            .checked_accumulate(&binding.intent.budget.delegated_usage)
            .map_err(|_| StoreError::IncompleteChildAccounting)?;
        admission
            .admission()
            .intent()
            .budget()
            .remaining(&total, binding.event.recorded_at())
            .map_err(|_| StoreError::GraphFrameLimitExceeded)?;
        reject_reused_node_worker_attempt(&mut tx, frame.origin(), &fence).await?;
        insert_event_components(&mut tx, &binding.event, binding.digest).await?;
        insert_binding(&mut tx, &binding).await?;
        insert_node_attempt_claim(&mut tx, &binding.start).await?;
        insert_node_attempt_start(&mut tx, &binding.start).await?;
        update_run_head(&mut tx, &binding.event, None).await?;
        revalidate_worker_after_components(&mut tx, &fence).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::database("framework rebinding commit", e))?;
        Ok(NodeAttemptCommitOutcome::Committed {
            event: binding.event,
            attempt: NodeAttempt::executing(binding.start),
        })
    }
}

async fn insert_binding(
    tx: &mut Transaction<'_, Postgres>,
    binding: &Binding,
) -> Result<(), StoreError> {
    let intent = &binding.intent;
    let head = &intent.frame_checkpoint;
    let event = &binding.event;
    query("INSERT INTO stateknot.graph_frame_caller_bindings(tenant_id,run_id,graph_namespace,frame_identity_digest,previous_attempt_id,previous_start_digest,caller_attempt_id,caller_start_digest,worker_attempt_id,worker_epoch,active_checkpoint_id,active_superstep,active_checkpoint_digest,active_frame_checkpoint_digest,previous_compound_digest,compound_digest,binding_bytes,journal_sequence,journal_event_id,journal_recorded_at,journal_digest) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)")
        .bind(head.checkpoint().tenant_id().as_str()).bind(*head.checkpoint().run_id().as_uuid()).bind(head.frame().namespace().as_str()).bind(head.frame().digest().as_bytes())
        .bind(*intent.previous.attempt_id().as_uuid()).bind(intent.previous.digest().as_bytes()).bind(*binding.start.attempt_id().as_uuid()).bind(binding.start.digest().as_bytes())
        .bind(*intent.fence.attempt_id().as_uuid()).bind(i64::try_from(intent.fence.epoch().get()).map_err(|_|StoreError::StaleFence)?)
        .bind(*head.checkpoint().checkpoint_id().as_uuid()).bind(i64::try_from(head.checkpoint().superstep().get()).map_err(|_|StoreError::GraphFrameLimitExceeded)?)
        .bind(head.checkpoint().digest().as_bytes()).bind(head.digest().as_bytes()).bind(intent.previous_compound_digest.as_bytes()).bind(binding.digest.as_bytes()).bind(encode(binding)?)
        .bind(i64::try_from(event.sequence().get()).map_err(|_|StoreError::JournalSequenceExhausted)?).bind(*event.event_id().as_uuid()).bind(to_database_time(event.recorded_at())?).bind(event.digest().as_bytes())
        .execute(&mut **tx).await.map_err(|e|StoreError::database("framework binding insert",e))?;
    Ok(())
}

async fn verify_claim(
    tx: &mut Transaction<'_, Postgres>,
    start: &NodeAttemptStart,
) -> Result<(), StoreError> {
    let journal = start.journal_head();
    let valid=query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM stateknot.run_attempt_claims WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3 AND claim_kind='node_attempt' AND activation_digest=$4 AND invocation_id IS NULL AND invocation_revision IS NULL AND journal_sequence=$5 AND journal_event_id=$6 AND journal_recorded_at=$7 AND journal_digest=$8 AND claimed_at=$7)")
        .bind(start.activation().tenant_id().as_str()).bind(*start.activation().run_id().as_uuid()).bind(*start.attempt_id().as_uuid()).bind(start.activation_digest().as_bytes())
        .bind(i64::try_from(journal.sequence().get()).map_err(|_|StoreError::corrupt("framework claim sequence"))?).bind(*journal.event_id().as_uuid()).bind(to_database_time(journal.recorded_at())?).bind(journal.digest().as_bytes())
        .fetch_one(&mut **tx).await.map_err(|e|StoreError::database("framework start claim",e))?;
    if !valid {
        return Err(StoreError::corrupt("framework claim binding"));
    }
    Ok(())
}
