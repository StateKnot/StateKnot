// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Whole control-plane aborts preserve physical callers and sealed decisions.
#[allow(clippy::wildcard_imports)]
use super::*;
use stateknot_core::{
    Failure, FailureCategory, FailureCode, FailureId, FailureMessage, FailureOrigin,
};

const EVENT_KIND: &str = "graph-frames-closed";
const MAX_BYTES: usize = 4_194_304;
const INTENT_DOMAIN: &[u8] = b"stateknot-postgres-frame-closure-intent-v1\0";
const COMPOUND_DOMAIN: &[u8] = b"stateknot-postgres-frame-closure-compound-v1\0";

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Frame {
    checkpoint: GraphFrameCheckpointHead,
    entry_digest: Digest,
    caller: NodeAttemptStartHead,
    caller_binding_digest: Digest,
}
impl Frame {
    fn matches(&self, actual: &StoredOpenGraphFrame) -> bool {
        &self.checkpoint == actual.checkpoint()
            && self.entry_digest == actual.entry_digest()
            && &self.caller == actual.caller()
            && self.caller_binding_digest == actual.caller_binding_digest()
    }
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AccountObservation {
    digest: Digest,
    direct_usage: BudgetUsage,
    direct_head: JournalHead,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    version: u8,
    admission_digest: Digest,
    root: CheckpointHead,
    observed_head: JournalHead,
    lifecycle: RunLifecycle,
    failure: Failure,
    failure_close: Option<JournalHead>,
    caller_failure: Failure,
    lifetime_starts: u16,
    #[serde(deserialize_with = "bounded_seven")]
    frames: Vec<Frame>,
    direct_usage: BudgetUsage,
    delegated_usage: BudgetUsage,
    child_account: Option<AccountObservation>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    version: u8,
    intent: Intent,
    event: JournalEvent,
    #[serde(deserialize_with = "bounded_seven")]
    completions: Vec<NodeAttemptCompletion>,
}
fn bounded_seven<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Vec<T>, D::Error> {
    struct Seven<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Seven<T> {
        type Value = Vec<T>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("one to seven frame components")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let mut result = Vec::with_capacity(7);
            loop {
                if result.len() == 7 {
                    if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                        return Err(serde::de::Error::custom("frame closure component bound"));
                    }
                    break;
                }
                let Some(value) = sequence.next_element()? else {
                    break;
                };
                result.push(value);
            }
            if result.is_empty() {
                return Err(serde::de::Error::custom("empty frame closure"));
            }
            Ok(result)
        }
    }
    deserializer.deserialize_seq(Seven(std::marker::PhantomData))
}

/// An immutable whole-stack abort, preserving its original close decision.
///
/// Framework completions are separate control-plane facts. Ordinary node
/// completions retain their existing worker-event and physical-fence constraints.
#[derive(Clone)]
pub struct StoredGraphFrameClosure {
    intent: Intent,
    event: JournalEvent,
    completions: Vec<NodeAttemptCompletion>,
    digest: Digest,
}
impl fmt::Debug for StoredGraphFrameClosure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredGraphFrameClosure")
            .field("event", &self.event.head())
            .field("frame_count", &self.completions.len())
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}
impl StoredGraphFrameClosure {
    /// Exact control-plane journal anchor shared by every closed caller.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        &self.event
    }
    /// Whole compound proof digest.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
    /// Original cancellation or sealed failure, retained without replacement.
    #[must_use]
    pub const fn failure(&self) -> &Failure {
        &self.intent.failure
    }
    /// Complete DIRECT usage frozen before this maintenance transaction.
    ///
    /// Closing performs no application work; its audit does not consume a new
    /// execution budget or require a deadline that may already have expired.
    #[must_use]
    pub const fn direct_usage(&self) -> &BudgetUsage {
        &self.intent.direct_usage
    }
    /// Actual framework completions, ordered root-to-leaf and retaining old fences.
    #[must_use]
    pub fn completions(&self) -> &[NodeAttemptCompletion] {
        &self.completions
    }
}
/// A first whole abort or recovery of the exact already committed decision.
#[derive(Clone, Debug)]
pub enum GraphFrameClosureCommitOutcome {
    /// Complete stack/caller/journal facts committed atomically.
    Committed(StoredGraphFrameClosure),
    /// Original facts authenticated before evaluating a new candidate.
    Existing(StoredGraphFrameClosure),
}
impl GraphFrameClosureCommitOutcome {
    /// Whole original evidence for either outcome.
    #[must_use]
    pub const fn record(&self) -> &StoredGraphFrameClosure {
        match self {
            Self::Committed(record) | Self::Existing(record) => record,
        }
    }
}
fn intent_digest(intent: &Intent) -> Result<Digest, StoreError> {
    let mut bytes = INTENT_DOMAIN.to_vec();
    bytes.extend(
        serde_json_canonicalizer::to_vec(intent)
            .map_err(|_| StoreError::encoding("frame closure intent"))?,
    );
    Ok(Digest::sha256(bytes))
}
fn compound(
    intent: &Intent,
    event: &JournalEvent,
    completions: &[NodeAttemptCompletion],
) -> Result<Digest, StoreError> {
    let mut bytes = COMPOUND_DOMAIN.to_vec();
    bytes.extend_from_slice(intent_digest(intent)?.as_bytes());
    bytes.extend_from_slice(event.digest().as_bytes());
    for completion in completions {
        bytes.extend_from_slice(completion.digest().as_bytes());
    }
    Ok(Digest::sha256(bytes))
}
fn encode(record: &StoredGraphFrameClosure) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(&Wire {
        version: 1,
        intent: record.intent.clone(),
        event: record.event.clone(),
        completions: record.completions.clone(),
    })
    .map_err(|_| StoreError::encoding("frame closure wire"))?;
    if bytes.is_empty() || bytes.len() > MAX_BYTES {
        return Err(StoreError::GraphFrameLimitExceeded);
    }
    Ok(bytes)
}
static EVENT_SCHEMA: LazyLock<
    Result<(stateknot_core::SchemaReference, serde_json::Value), &'static str>,
> = LazyLock::new(|| {
    let document = serde_json::json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"https://stknot.com/schemas/store/graph-frame-closure-event/1.0.0","type":"object","additionalProperties":false,"properties":{"version":{"const":1},"intent_digest":{"type":"string","minLength":71,"maxLength":71,"pattern":"^sha256:[0-9a-f]{64}$"},"frame_count":{"type":"integer","minimum":1,"maximum":7}},"required":["version","intent_digest","frame_count"]});
    let bytes = serde_json_canonicalizer::to_vec(&document).map_err(|_| "frame closure schema")?;
    Ok((
        stateknot_core::SchemaReference::new(
            "https://stknot.com/schemas/store/graph-frame-closure-event/1.0.0"
                .parse()
                .map_err(|_| "frame closure schema ID")?,
            stateknot_core::Version::new(1, 0, 0),
            Digest::sha256(bytes),
        ),
        document,
    ))
});
fn payload(intent: &Intent) -> Result<JournalPayload, StoreError> {
    JournalPayload::new(EVENT_SCHEMA.as_ref().map_err(|_| StoreError::encoding("frame closure schema"))?.0.clone(),stateknot_core::JournalEventKind::new(EVENT_KIND).map_err(|_| StoreError::GraphFrameRejected)?,BoundedJson::try_from(serde_json::json!({"version":1,"intent_digest":intent_digest(intent)?,"frame_count":intent.frames.len()})).map_err(|_| StoreError::GraphFrameRejected)?).map_err(|_| StoreError::GraphFrameRejected)
}
fn caller_failure(event_id: EventId) -> Result<Failure, StoreError> {
    Ok(Failure::new(
        FailureId::from_uuid(*event_id.as_uuid()).map_err(|_| StoreError::GraphFrameRejected)?,
        FailureCategory::Cancelled,
        FailureCode::new("graph-frame-closed").map_err(|_| StoreError::GraphFrameRejected)?,
        FailureOrigin::new("stateknot.graph-frame").map_err(|_| StoreError::GraphFrameRejected)?,
        FailureMessage::new("Framework call stopped after the Run close decision.")
            .map_err(|_| StoreError::GraphFrameRejected)?,
        RetryAdvice::Never,
    )
    .map_err(|_| StoreError::GraphFrameRejected)?
    .with_caused_by_event(event_id))
}
fn canonical_equal<T: Serialize, U: Serialize>(left: &T, right: &U) -> Result<bool, StoreError> {
    Ok(serde_json_canonicalizer::to_vec(left)
        .map_err(|_| StoreError::corrupt("closure canonical component"))?
        == serde_json_canonicalizer::to_vec(right)
            .map_err(|_| StoreError::corrupt("closure canonical component"))?)
}
pub(super) async fn row(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
) -> Result<Option<PgRow>, StoreError> {
    query("SELECT * FROM stateknot.graph_frame_closures WHERE tenant_id=$1 AND run_id=$2")
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| StoreError::database("frame closure row", e))
}
fn ancestors(namespace: &GraphNamespace) -> Vec<String> {
    let mut prefixes = Vec::with_capacity(7);
    let mut prefix = String::new();
    for segment in namespace.as_str().split('/') {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(segment);
        prefixes.push(prefix.clone());
    }
    prefixes
}
async fn decision(
    tx: &mut Transaction<'_, Postgres>,
    run: &StoredRun,
) -> Result<(Failure, Option<JournalHead>), StoreError> {
    if run.lifecycle().status() == RunStatus::CancellationRequested {
        return Ok((
            run.lifecycle()
                .cancellation_request()
                .ok_or_else(|| StoreError::corrupt("frame cancellation decision"))?
                .failure()
                .clone(),
            None,
        ));
    }
    let sealed = failure_closes::load(tx, run)
        .await?
        .ok_or(StoreError::RunNotRunnable)?;
    if run.lifecycle().status() != RunStatus::Active || sealed.completed_at().is_some() {
        return Err(StoreError::RunNotRunnable);
    }
    Ok((sealed.failure().clone(), Some(sealed.registration().head())))
}
// No unfinished ordinary physical work or unknown external outcome is erased.
// Earlier physical starts may remain in-flight after a later verified takeover;
// only the most recent start for each exact activation must have completed.
pub(super) async fn ensure_settled_work(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
) -> Result<(), StoreError> {
    let tool:bool=query_scalar("SELECT EXISTS(SELECT 1 FROM stateknot.tool_invocations WHERE tenant_id=$1 AND run_id=$2 AND current_status NOT IN ('committed','failed'))").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(&mut **tx).await.map_err(|e|StoreError::database("frame closure Tool work",e))?;
    if tool {
        return Err(StoreError::CheckpointBlockedByToolInvocation);
    }
    let model:bool=query_scalar("SELECT EXISTS(SELECT 1 FROM stateknot.model_invocations WHERE tenant_id=$1 AND run_id=$2 AND current_status NOT IN ('committed','failed'))").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(&mut **tx).await.map_err(|e|StoreError::database("frame closure Model work",e))?;
    if model {
        return Err(StoreError::CheckpointBlockedByModelInvocation);
    }
    let unfinished:bool=query_scalar("SELECT EXISTS(SELECT 1 FROM stateknot.node_attempts n WHERE n.tenant_id=$1 AND n.run_id=$2 AND NOT EXISTS(SELECT 1 FROM stateknot.node_attempts later WHERE later.tenant_id=n.tenant_id AND later.run_id=n.run_id AND later.activation_digest=n.activation_digest AND later.journal_sequence>n.journal_sequence) AND NOT EXISTS(SELECT 1 FROM stateknot.node_attempt_completions c WHERE c.tenant_id=n.tenant_id AND c.run_id=n.run_id AND c.attempt_id=n.attempt_id) AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=n.tenant_id AND e.run_id=n.run_id AND e.caller_attempt_id=n.attempt_id) AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_caller_bindings b WHERE b.tenant_id=n.tenant_id AND b.run_id=n.run_id AND b.caller_attempt_id=n.attempt_id))").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(&mut **tx).await.map_err(|e|StoreError::database("frame closure ordinary physical work",e))?;
    if unfinished {
        return Err(StoreError::InvalidRunFailureClose);
    }
    Ok(())
}

fn digest_column(row: &PgRow, name: &str, expected: Digest) -> Result<(), StoreError> {
    let bytes: Vec<u8> = row
        .try_get(name)
        .map_err(|e| StoreError::database("frame closure digest column", e))?;
    if decode_digest(&bytes, "frame closure digest projection")? != expected {
        return Err(StoreError::corrupt("frame closure digest projection"));
    }
    Ok(())
}
#[allow(clippy::too_many_lines)]
async fn anchored_record(
    tx: &mut Transaction<'_, Postgres>,
    row: PgRow,
) -> Result<StoredGraphFrameClosure, StoreError> {
    let bytes: Vec<u8> = row
        .try_get("closure_bytes")
        .map_err(|e| StoreError::database("frame closure bytes", e))?;
    if bytes.is_empty() || bytes.len() > MAX_BYTES {
        return Err(StoreError::corrupt("frame closure byte bound"));
    }
    let wire: Wire =
        serde_json::from_slice(&bytes).map_err(|_| StoreError::corrupt("frame closure wire"))?;
    if wire.version != 1
        || wire.intent.version != 1
        || wire.intent.frames.len() != wire.completions.len()
    {
        return Err(StoreError::corrupt(
            "frame closure version or component count",
        ));
    }
    let digest = compound(&wire.intent, &wire.event, &wire.completions)?;
    let record = StoredGraphFrameClosure {
        intent: wire.intent,
        event: wire.event,
        completions: wire.completions,
        digest,
    };
    if encode(&record)? != bytes {
        return Err(StoreError::corrupt("frame closure canonical bytes"));
    }
    let intent = &record.intent;
    let tenant = intent.root.tenant_id();
    let run = intent.root.run_id();
    let leaf = intent
        .frames
        .last()
        .ok_or_else(|| StoreError::corrupt("frame closure leaf"))?;
    for (name, value) in [
        ("admission_digest", intent.admission_digest),
        ("root_checkpoint_digest", intent.root.digest()),
        (
            "active_frame_identity_digest",
            leaf.checkpoint.frame().digest(),
        ),
        ("intent_digest", intent_digest(intent)?),
        ("compound_digest", record.digest),
        ("closure_checksum", Digest::sha256(&bytes)),
        ("journal_digest", record.event.digest()),
    ] {
        digest_column(&row, name, value)?;
    }
    if row
        .try_get::<String, _>("tenant_id")
        .map_err(|e| StoreError::database("closure tenant", e))?
        != tenant.as_str()
        || row
            .try_get::<Uuid, _>("run_id")
            .map_err(|e| StoreError::database("closure Run", e))?
            != *run.as_uuid()
        || row
            .try_get::<Uuid, _>("root_checkpoint_id")
            .map_err(|e| StoreError::database("closure Root", e))?
            != *intent.root.checkpoint_id().as_uuid()
        || row
            .try_get::<i64, _>("root_superstep")
            .map_err(|e| StoreError::database("closure Root position", e))?
            != i64::try_from(intent.root.superstep().get())
                .map_err(|_| StoreError::corrupt("closure Root position"))?
        || row
            .try_get::<String, _>("active_namespace")
            .map_err(|e| StoreError::database("closure leaf scope", e))?
            != leaf.checkpoint.frame().namespace().as_str()
        || row
            .try_get::<i32, _>("lifetime_starts")
            .map_err(|e| StoreError::database("closure lifetime", e))?
            != i32::from(intent.lifetime_starts)
        || row
            .try_get::<i32, _>("frame_count")
            .map_err(|e| StoreError::database("closure frame count", e))?
            != i32::try_from(intent.frames.len())
                .map_err(|_| StoreError::corrupt("closure frame count"))?
        || row
            .try_get::<i64, _>("journal_sequence")
            .map_err(|e| StoreError::database("closure sequence", e))?
            != i64::try_from(record.event.sequence().get())
                .map_err(|_| StoreError::corrupt("closure sequence"))?
        || row
            .try_get::<Uuid, _>("journal_event_id")
            .map_err(|e| StoreError::database("closure event", e))?
            != *record.event.event_id().as_uuid()
        || row
            .try_get::<DateTime<Utc>, _>("journal_recorded_at")
            .map_err(|e| StoreError::database("closure clock", e))?
            != to_database_time(record.event.recorded_at())?
    {
        return Err(StoreError::corrupt("frame closure scalar projection"));
    }
    let actual = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .bind(
            i64::try_from(record.event.sequence().get())
                .map_err(|_| StoreError::corrupt("closure event sequence"))?,
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| StoreError::database("frame closure event anchor", e))?
        .ok_or_else(|| StoreError::corrupt("frame closure event missing"))?;
    if actual.projection_digest.as_deref() != Some(record.digest.as_bytes())
        || decode_event(actual)? != record.event
        || record.event.source() != &JournalEventSource::ControlPlane
        || record.event.payload() != &payload(intent)?
    {
        return Err(StoreError::corrupt("frame closure journal binding"));
    }
    let (before, _) = child_runs::anchored_event(
        tx,
        tenant,
        run,
        i64::try_from(intent.observed_head.sequence().get())
            .map_err(|_| StoreError::corrupt("closure predecessor sequence"))?,
    )
    .await?;
    if before.head() != intent.observed_head
        || record.event.sequence().get()
            != before
                .sequence()
                .get()
                .checked_add(1)
                .ok_or(StoreError::JournalSequenceExhausted)?
        || record.event.previous_digest() != Some(before.digest())
        || record.event.recorded_at() < before.recorded_at()
    {
        return Err(StoreError::corrupt("frame closure predecessor"));
    }
    Ok(record)
}

#[allow(clippy::too_many_lines)]
pub(super) async fn verified_record(
    tx: &mut Transaction<'_, Postgres>,
    stored: &StoredRun,
    row: PgRow,
) -> Result<StoredGraphFrameClosure, StoreError> {
    let record = Box::pin(anchored_record(tx, row)).await?;
    let intent = &record.intent;
    let tenant = intent.root.tenant_id();
    let run = intent.root.run_id();
    let leaf = intent
        .frames
        .last()
        .ok_or_else(|| StoreError::corrupt("frame closure leaf"))?;
    verify_decision(tx, stored, intent).await?;
    if !canonical_equal(
        &intent.caller_failure,
        &caller_failure(record.event.event_id())?,
    )? {
        return Err(StoreError::corrupt("closed caller failure binding"));
    }
    let stack=query_as::<_,StackRow>("SELECT admission_digest,lifetime_starts,active_namespace,active_frame_identity_digest FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_optional(&mut **tx).await.map_err(|e|StoreError::database("closed stack",e))?.ok_or_else(||StoreError::corrupt("closed stack missing"))?;
    let(total,maximum):(i64,Option<i32>)=query_as("SELECT count(*),max(ordinal) FROM stateknot.graph_frame_entries WHERE tenant_id=$1 AND run_id=$2").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(&mut **tx).await.map_err(|e|StoreError::database("closed lifetime inventory",e))?;
    if !stack.active_namespace.is_empty()
        || stack.active_frame_identity_digest.is_some()
        || stack.lifetime_starts != i32::from(intent.lifetime_starts)
        || total != i64::from(intent.lifetime_starts)
        || maximum != Some(i32::from(intent.lifetime_starts))
        || decode_digest(&stack.admission_digest, "closed admission")? != intent.admission_digest
        || stored.lease().is_some()
    {
        return Err(StoreError::corrupt("closed stack projection"));
    }
    let root = load_locked_current_checkpoint(tx, stored, tenant, run)
        .await?
        .ok_or_else(|| StoreError::corrupt("closed Root checkpoint"))?;
    if root.head() != intent.root {
        return Err(StoreError::corrupt("closed Root substitution"));
    }
    drop(root);
    let expected = ancestors(leaf.checkpoint.frame().namespace());
    if expected.len() != intent.frames.len() {
        return Err(StoreError::corrupt("closed ancestor count"));
    }
    let recovered = Box::pin(active::verify_chain(
        tx,
        tenant,
        run,
        stored.clone(),
        expected,
        false,
        intent.admission_digest,
    ))
    .await?
    .ok_or_else(|| StoreError::corrupt("closed chain missing"))?;
    if recovered
        .open_frames()
        .iter()
        .zip(&intent.frames)
        .any(|(actual, claim)| !claim.matches(actual))
    {
        return Err(StoreError::corrupt(
            "closed frame head or caller substitution",
        ));
    }
    intent
        .direct_usage
        .validate_monotonic_after(recovered.minimum_direct_usage())
        .map_err(|_| StoreError::corrupt("closed direct usage regression"))?;
    let count:i64=query_scalar("SELECT count(*) FROM stateknot.graph_frame_closed_callers WHERE tenant_id=$1 AND run_id=$2").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(&mut **tx).await.map_err(|e|StoreError::database("closed caller inventory",e))?;
    let hidden:bool=query_scalar("SELECT EXISTS(SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=$1 AND e.run_id=$2 AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_returns x WHERE x.tenant_id=e.tenant_id AND x.run_id=e.run_id AND x.graph_namespace=e.graph_namespace) AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_closed_callers c WHERE c.tenant_id=e.tenant_id AND c.run_id=e.run_id AND c.graph_namespace=e.graph_namespace))").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(&mut **tx).await.map_err(|e|StoreError::database("closed open inventory",e))?;
    if count
        != i64::try_from(intent.frames.len())
            .map_err(|_| StoreError::corrupt("closed inventory"))?
        || hidden
    {
        return Err(StoreError::corrupt("closed caller completeness"));
    }
    for (frame, completion) in intent.frames.iter().zip(&record.completions) {
        let saved=query("SELECT * FROM stateknot.graph_frame_closed_callers WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3").bind(tenant.as_str()).bind(*run.as_uuid()).bind(frame.checkpoint.frame().namespace().as_str()).fetch_one(&mut **tx).await.map_err(|e|StoreError::database("closed caller",e))?;
        for (name, value) in [
            ("frame_identity_digest", frame.checkpoint.frame().digest()),
            ("entry_digest", frame.entry_digest),
            ("checkpoint_digest", frame.checkpoint.checkpoint().digest()),
            ("frame_checkpoint_digest", frame.checkpoint.digest()),
            ("caller_start_digest", frame.caller.digest()),
            ("caller_compound_digest", frame.caller_binding_digest),
            ("completion_digest", completion.digest()),
            (
                "completion_checksum",
                Digest::sha256(encode_node_attempt_completion(completion)?),
            ),
            ("journal_digest", record.event.digest()),
        ] {
            digest_column(&saved, name, value)?;
        }
        if saved
            .try_get::<Uuid, _>("caller_attempt_id")
            .map_err(|e| StoreError::database("closed caller id", e))?
            != *frame.caller.attempt_id().as_uuid()
            || saved
                .try_get::<Uuid, _>("checkpoint_id")
                .map_err(|e| StoreError::database("closed checkpoint id", e))?
                != *frame.checkpoint.checkpoint().checkpoint_id().as_uuid()
            || saved
                .try_get::<i64, _>("checkpoint_superstep")
                .map_err(|e| StoreError::database("closed checkpoint position", e))?
                != i64::try_from(frame.checkpoint.checkpoint().superstep().get())
                    .map_err(|_| StoreError::corrupt("closed checkpoint position"))?
            || saved
                .try_get::<Vec<u8>, _>("completion_bytes")
                .map_err(|e| StoreError::database("closed completion bytes", e))?
                != encode_node_attempt_completion(completion)?
            || saved
                .try_get::<i64, _>("journal_sequence")
                .map_err(|e| StoreError::database("closed caller sequence", e))?
                != i64::try_from(record.event.sequence().get())
                    .map_err(|_| StoreError::corrupt("closed caller sequence"))?
            || saved
                .try_get::<Uuid, _>("journal_event_id")
                .map_err(|e| StoreError::database("closed caller event", e))?
                != *record.event.event_id().as_uuid()
            || saved
                .try_get::<DateTime<Utc>, _>("journal_recorded_at")
                .map_err(|e| StoreError::database("closed caller clock", e))?
                != to_database_time(record.event.recorded_at())?
            || completion.start() != &frame.caller
            || completion.journal_head() != &record.event.head()
            || completion.usage() != &BudgetUsage::zero()
            || !canonical_equal(
                &completion.outcome().failure(),
                &Some(&intent.caller_failure),
            )?
            || frame.caller.journal_head().sequence() > intent.observed_head.sequence()
            || frame.checkpoint.checkpoint().journal_head().sequence()
                > intent.observed_head.sequence()
        {
            return Err(StoreError::corrupt("closed caller component substitution"));
        }
        let duplicate:bool=query_scalar("SELECT EXISTS(SELECT 1 FROM stateknot.node_attempt_completions WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3)").bind(tenant.as_str()).bind(*run.as_uuid()).bind(*frame.caller.attempt_id().as_uuid()).fetch_one(&mut **tx).await.map_err(|e|StoreError::database("closed caller duplicate completion",e))?;
        if duplicate {
            return Err(StoreError::corrupt("closed caller double completion"));
        }
    }
    verify_account(tx, intent).await?;
    ensure_settled_work(tx, tenant, run).await?;
    Ok(record)
}
async fn verify_decision(
    tx: &mut Transaction<'_, Postgres>,
    stored: &StoredRun,
    intent: &Intent,
) -> Result<(), StoreError> {
    if intent.direct_usage.unpriced_cost_events().get() != 0
        || intent.caller_failure.category() != FailureCategory::Cancelled
        || intent.caller_failure.retry_advice() != RetryAdvice::Never
        || intent.caller_failure.caused_by_event_id().is_none()
    {
        return Err(StoreError::corrupt("closed direct usage or caller failure"));
    }
    if let Some(head) = &intent.failure_close {
        let sealed = failure_closes::load(tx, stored)
            .await?
            .ok_or_else(|| StoreError::corrupt("closed failure decision missing"))?;
        if sealed.registration().head() != *head
            || !canonical_equal(sealed.failure(), &intent.failure)?
            || sealed.direct_usage() != &intent.direct_usage
            || !canonical_equal(sealed.lifecycle(), &intent.lifecycle)?
            || sealed.checkpoint() != &intent.root
        {
            return Err(StoreError::corrupt("closed failure decision substitution"));
        }
    } else {
        if intent.lifecycle.status() != RunStatus::CancellationRequested
            || !canonical_equal(
                &intent.lifecycle.cancellation_request(),
                &stored.lifecycle().cancellation_request(),
            )?
            || !canonical_equal(
                &intent
                    .lifecycle
                    .cancellation_request()
                    .map(stateknot_core::RunCancellationRequest::failure),
                &Some(&intent.failure),
            )?
            || !matches!(
                stored.lifecycle().status(),
                RunStatus::CancellationRequested | RunStatus::Cancelled
            )
        {
            return Err(StoreError::corrupt("closed cancellation substitution"));
        }
        let original = serde_json::to_value(&intent.lifecycle)
            .map_err(|_| StoreError::corrupt("closed original lifecycle"))?;
        let current = serde_json::to_value(stored.lifecycle())
            .map_err(|_| StoreError::corrupt("closed current lifecycle"))?;
        let same = original
            .as_object()
            .ok_or_else(|| StoreError::corrupt("closed lifecycle object"))?
            .iter()
            .filter(|(key, _)| key.as_str() != "state" && key.as_str() != "revision")
            .all(|(key, value)| current.get(key) == Some(value));
        let terminal = stored.lifecycle().status() == RunStatus::Cancelled;
        let expected_revision = intent
            .lifecycle
            .revision()
            .get()
            .checked_add(u64::from(terminal))
            .ok_or_else(|| StoreError::corrupt("closed lifecycle revision"))?;
        let total = intent
            .direct_usage
            .checked_accumulate(&intent.delegated_usage)
            .map_err(|_| StoreError::corrupt("closed total usage"))?;
        if !same
            || stored.lifecycle().revision().get() != expected_revision
            || (terminal && stored.lifecycle().terminal_usage() != Some(&total))
        {
            return Err(StoreError::corrupt("closed lifecycle binding"));
        }
    }
    Ok(())
}
async fn verify_account(
    tx: &mut Transaction<'_, Postgres>,
    intent: &Intent,
) -> Result<(), StoreError> {
    let account =
        child_runs::load_account_inner(tx, intent.root.tenant_id(), intent.root.run_id(), false)
            .await?;
    match (account, intent.child_account.as_ref()) {
        (None, None) if intent.delegated_usage == BudgetUsage::zero() => Ok(()),
        (Some(account), Some(saved)) => {
            child_runs::ensure_settled(&account)?;
            if account
                .delegated_usage()
                .map_err(|_| StoreError::corrupt("closed delegated usage"))?
                != intent.delegated_usage
            {
                return Err(StoreError::corrupt("closed child settlement substitution"));
            }
            // Final Run projection advances only the account's direct observation.
            // Reconstruct its sealed head/usage from the same verified immutable
            // child ownership and settlement facts, then check the original pin.
            let mut account_value = serde_json::to_value(&account)
                .map_err(|_| StoreError::corrupt("closed child account encoding"))?;
            for (key, value) in [
                ("direct_head", serde_json::to_value(&saved.direct_head)),
                ("direct_usage", serde_json::to_value(&saved.direct_usage)),
                ("digest", serde_json::to_value(saved.digest)),
            ] {
                value_insert(
                    &mut account_value,
                    key,
                    value.map_err(|_| StoreError::corrupt("closed account observation"))?,
                )?;
            }
            let restored: stateknot_core::ChildRunBudgetAccount =
                serde_json::from_value(account_value)
                    .map_err(|_| StoreError::corrupt("closed historical child account"))?;
            if restored.digest() != saved.digest {
                return Err(StoreError::corrupt(
                    "closed historical child account digest",
                ));
            }
            intent
                .direct_usage
                .validate_monotonic_after(restored.direct_usage())
                .map_err(|_| StoreError::corrupt("closed child direct usage"))
        }
        _ => Err(StoreError::corrupt("closed child account shape")),
    }
}
fn value_insert(
    value: &mut serde_json::Value,
    key: &str,
    item: serde_json::Value,
) -> Result<(), StoreError> {
    value
        .as_object_mut()
        .ok_or_else(|| StoreError::corrupt("closed account object"))?
        .insert(key.to_owned(), item);
    Ok(())
}

impl PostgresStore {
    /// Exact closed event schema for whole framework aborts.
    ///
    /// # Errors
    /// Rejects local canonical schema encoding failure.
    pub fn graph_frame_closure_event_schema()
    -> Result<(stateknot_core::SchemaReference, serde_json::Value), StoreError> {
        EVENT_SCHEMA
            .as_ref()
            .cloned()
            .map_err(|_| StoreError::encoding("frame closure schema"))
    }
    /// Authenticates an immutable whole closure without acquiring execution authority.
    ///
    /// # Errors
    /// Rejects missing Runs, corruption, replay bounds or database errors.
    pub fn load_graph_frame_closure<'a>(
        &'a self,
        tenant: &'a TenantId,
        run: RunId,
    ) -> stateknot_core::BoxFuture<'a, Result<Option<StoredGraphFrameClosure>, StoreError>> {
        Box::pin(async move {
            let mut tx = self
                .begin_repeatable_read("whole frame closure snapshot")
                .await?;
            let stored = query_as::<_, RunRow>(SELECT_RUN)
                .bind(tenant.as_str())
                .bind(*run.as_uuid())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| StoreError::database("closed Run snapshot", e))?
                .ok_or(StoreError::RunNotFound)?;
            let stored = decode_run(stored)?;
            let record = if let Some(row) = row(&mut tx, tenant, run).await? {
                Some(Box::pin(verified_record(&mut tx, &stored, row)).await?)
            } else {
                None
            };
            tx.commit()
                .await
                .map_err(|e| StoreError::database("whole frame closure snapshot commit", e))?;
            Ok(record)
        })
    }
    /// Closes every open frame after an immutable cancellation or sealed failure.
    ///
    /// Complete priced DIRECT usage and settled ordinary/provider/child work are
    /// required. This trusted control-plane maintenance API grants no execution
    /// lease, creates no parent result and does not require an unexpired deadline.
    /// Every actual current physical caller receives a non-retryable completion
    /// at its original fence. Exact committed recovery precedes fresh candidates.
    ///
    /// # Errors
    /// Rejects undecided/unfinished/unpriced work, changed observations, corruption,
    /// quarantine, replay bounds and database failures with atomic rollback.
    pub fn close_graph_frames<'a>(
        &'a self,
        tenant: &'a TenantId,
        run: RunId,
        event_id: EventId,
        observed: JournalHead,
        revision: RunRevision,
        direct_usage: BudgetUsage,
    ) -> stateknot_core::BoxFuture<'a, Result<GraphFrameClosureCommitOutcome, StoreError>> {
        Box::pin(self.close_graph_frames_inner(
            tenant,
            run,
            event_id,
            observed,
            revision,
            direct_usage,
        ))
    }
    #[allow(clippy::too_many_lines)]
    async fn close_graph_frames_inner(
        &self,
        tenant: &TenantId,
        run: RunId,
        event_id: EventId,
        observed: JournalHead,
        revision: RunRevision,
        direct_usage: BudgetUsage,
    ) -> Result<GraphFrameClosureCommitOutcome, StoreError> {
        let mut tx = self.begin_mutation("whole graph frame closure").await?;
        let stored = decode_run(fetch_locked_run_row(&mut tx, tenant, run).await?)?;
        if let Some(saved) = row(&mut tx, tenant, run).await? {
            return Ok(GraphFrameClosureCommitOutcome::Existing(
                Box::pin(verified_record(&mut tx, &stored, saved)).await?,
            ));
        }
        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if stored.journal_head() != Some(&observed) || stored.lifecycle().revision() != revision {
            return Err(StoreError::StaleJournalHead);
        }
        if direct_usage.unpriced_cost_events().get() != 0 {
            return Err(StoreError::InvalidRunFailureClose);
        }
        let (failure, failure_close) = decision(&mut tx, &stored).await?;
        let active = Box::pin(active::verified_snapshot(
            &mut tx,
            tenant,
            run,
            stored.clone(),
        ))
        .await?
        .ok_or(StoreError::GraphFrameConflict)?;
        direct_usage
            .validate_monotonic_after(active.minimum_direct_usage())
            .map_err(|_| StoreError::GraphFrameConflict)?;
        ensure_settled_work(&mut tx, tenant, run).await?;
        let account = child_runs::load_account_inner(&mut tx, tenant, run, false).await?;
        let (delegated_usage, child_account) = if let Some(account) = account {
            child_runs::ensure_settled(&account)?;
            direct_usage
                .validate_monotonic_after(account.direct_usage())
                .map_err(|_| StoreError::IncompleteChildAccounting)?;
            (
                account
                    .delegated_usage()
                    .map_err(|_| StoreError::IncompleteChildAccounting)?,
                Some(AccountObservation {
                    digest: account.digest(),
                    direct_usage: account.direct_usage().clone(),
                    direct_head: account.direct_head().clone(),
                }),
            )
        } else {
            (BudgetUsage::zero(), None)
        };
        if failure_close.is_some() {
            let sealed = failure_closes::load(&mut tx, &stored)
                .await?
                .ok_or(StoreError::InvalidRunFailureClose)?;
            if sealed.direct_usage() != &direct_usage {
                return Err(StoreError::InvalidRunFailureClose);
            }
        }
        let root = load_locked_current_checkpoint(&mut tx, &stored, tenant, run)
            .await?
            .ok_or(StoreError::StaleCheckpointHead)?
            .head();
        let lifetime:i32=query_scalar("SELECT lifetime_starts FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(&mut *tx).await.map_err(|e|StoreError::database("close lifetime snapshot",e))?;
        let caller_failure = caller_failure(event_id)?;
        let frames = active
            .open_frames()
            .iter()
            .map(|frame| Frame {
                checkpoint: frame.checkpoint().clone(),
                entry_digest: frame.entry_digest(),
                caller: frame.caller().clone(),
                caller_binding_digest: frame.caller_binding_digest(),
            })
            .collect();
        let admission_digest = active.entry().scope.admission_digest;
        drop(active);
        let intent = Intent {
            version: 1,
            admission_digest,
            root,
            observed_head: observed.clone(),
            lifecycle: stored.lifecycle().clone(),
            failure,
            failure_close,
            caller_failure,
            lifetime_starts: u16::try_from(lifetime)
                .map_err(|_| StoreError::GraphFrameLimitExceeded)?,
            frames,
            direct_usage,
            delegated_usage,
            child_account,
        };
        let append = JournalAppend::new(
            stateknot_core::JournalExpectation::exact(observed.clone()),
            JournalEventIntent::control_plane(tenant.clone(), run, event_id, payload(&intent)?)
                .map_err(|_| StoreError::GraphFrameRejected)?,
        )
        .map_err(|_| StoreError::GraphFrameRejected)?;
        let now = database_now(&mut tx, "frame closure database clock")
            .await?
            .max(observed.recorded_at());
        let event = JournalEvent::commit(append, now).map_err(|e| map_event_commit_error(&e))?;
        let mut completions = Vec::with_capacity(intent.frames.len());
        for frame in &intent.frames {
            let attempt =
                load_node_attempt_record(&mut tx, tenant, &run, frame.caller.attempt_id())
                    .await?
                    .ok_or(StoreError::NodeAttemptNotFound)?;
            if attempt.start().head() != frame.caller || attempt.completion().is_some() {
                return Err(StoreError::GraphFrameConflict);
            }
            completions.push(
                NodeAttemptCompletion::fail(
                    attempt.start(),
                    intent.caller_failure.clone(),
                    BudgetUsage::zero(),
                    event.head(),
                )
                .map_err(|_| StoreError::GraphFrameRejected)?,
            );
        }
        let digest = compound(&intent, &event, &completions)?;
        let record = StoredGraphFrameClosure {
            intent,
            event,
            completions,
            digest,
        };
        let bytes = encode(&record)?;
        insert_event(&mut tx, &record.event, digest).await?;
        insert_record(&mut tx, &record, &bytes).await?;
        for (frame, completion) in record.intent.frames.iter().zip(&record.completions) {
            insert_caller(&mut tx, frame, completion).await?;
        }
        let leaf = record
            .intent
            .frames
            .last()
            .ok_or(StoreError::GraphFrameConflict)?;
        let updated=query("UPDATE stateknot.graph_frame_stacks SET active_namespace='',active_frame_identity_digest=NULL WHERE tenant_id=$1 AND run_id=$2 AND active_namespace=$3 AND active_frame_identity_digest=$4 AND lifetime_starts=$5").bind(tenant.as_str()).bind(*run.as_uuid()).bind(leaf.checkpoint.frame().namespace().as_str()).bind(leaf.checkpoint.frame().digest().as_bytes()).bind(i32::from(record.intent.lifetime_starts)).execute(&mut *tx).await.map_err(|e|StoreError::database("whole frame stack closure",e))?.rows_affected();
        if updated != 1 {
            return Err(StoreError::GraphFrameConflict);
        }
        query("UPDATE stateknot.runs SET lease_attempt_id=NULL,lease_acquired_at=NULL,lease_renewed_at=NULL,lease_expires_at=NULL,scheduler_not_before=NULL WHERE tenant_id=$1 AND run_id=$2").bind(tenant.as_str()).bind(*run.as_uuid()).execute(&mut *tx).await.map_err(|e|StoreError::database("frame closure lease release",e))?;
        update_run_head(&mut tx, &record.event, None).await?;

        query("SET CONSTRAINTS ALL IMMEDIATE")
            .execute(&mut *tx)
            .await
            .map_err(|e| StoreError::database("whole frame closure deferred components", e))?;
        let current = decode_run(fetch_locked_run_row(&mut tx, tenant, run).await?)?;
        let saved = row(&mut tx, tenant, run)
            .await?
            .ok_or(StoreError::GraphFrameConflict)?;
        let verified = Box::pin(verified_record(&mut tx, &current, saved)).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::database("whole frame closure commit", e))?;
        Ok(GraphFrameClosureCommitOutcome::Committed(verified))
    }
}
async fn insert_record(
    tx: &mut Transaction<'_, Postgres>,
    record: &StoredGraphFrameClosure,
    bytes: &[u8],
) -> Result<(), StoreError> {
    let i = &record.intent;
    let e = &record.event;
    let leaf = i.frames.last().ok_or(StoreError::GraphFrameConflict)?;
    query("INSERT INTO stateknot.graph_frame_closures(tenant_id,run_id,admission_digest,root_checkpoint_id,root_superstep,root_checkpoint_digest,active_namespace,active_frame_identity_digest,lifetime_starts,frame_count,intent_digest,compound_digest,closure_bytes,journal_sequence,journal_event_id,journal_recorded_at,journal_digest) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)").bind(i.root.tenant_id().as_str()).bind(*i.root.run_id().as_uuid()).bind(i.admission_digest.as_bytes()).bind(*i.root.checkpoint_id().as_uuid()).bind(i64::try_from(i.root.superstep().get()).map_err(|_|StoreError::GraphFrameLimitExceeded)?).bind(i.root.digest().as_bytes()).bind(leaf.checkpoint.frame().namespace().as_str()).bind(leaf.checkpoint.frame().digest().as_bytes()).bind(i32::from(i.lifetime_starts)).bind(i32::try_from(i.frames.len()).map_err(|_|StoreError::GraphFrameLimitExceeded)?).bind(intent_digest(i)?.as_bytes()).bind(record.digest.as_bytes()).bind(bytes).bind(i64::try_from(e.sequence().get()).map_err(|_|StoreError::JournalSequenceExhausted)?).bind(*e.event_id().as_uuid()).bind(to_database_time(e.recorded_at())?).bind(e.digest().as_bytes()).execute(&mut **tx).await.map_err(|e|StoreError::database("whole frame closure insert",e))?;
    Ok(())
}
async fn insert_caller(
    tx: &mut Transaction<'_, Postgres>,
    frame: &Frame,
    completion: &NodeAttemptCompletion,
) -> Result<(), StoreError> {
    let cp = frame.checkpoint.checkpoint();
    let e = completion.journal_head();
    query("INSERT INTO stateknot.graph_frame_closed_callers(tenant_id,run_id,graph_namespace,frame_identity_digest,entry_digest,checkpoint_id,checkpoint_superstep,checkpoint_digest,frame_checkpoint_digest,caller_attempt_id,caller_start_digest,caller_compound_digest,completion_digest,completion_bytes,journal_sequence,journal_event_id,journal_recorded_at,journal_digest) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)").bind(cp.tenant_id().as_str()).bind(*cp.run_id().as_uuid()).bind(frame.checkpoint.frame().namespace().as_str()).bind(frame.checkpoint.frame().digest().as_bytes()).bind(frame.entry_digest.as_bytes()).bind(*cp.checkpoint_id().as_uuid()).bind(i64::try_from(cp.superstep().get()).map_err(|_|StoreError::GraphFrameLimitExceeded)?).bind(cp.digest().as_bytes()).bind(frame.checkpoint.digest().as_bytes()).bind(*frame.caller.attempt_id().as_uuid()).bind(frame.caller.digest().as_bytes()).bind(frame.caller_binding_digest.as_bytes()).bind(completion.digest().as_bytes()).bind(encode_node_attempt_completion(completion)?).bind(i64::try_from(e.sequence().get()).map_err(|_|StoreError::JournalSequenceExhausted)?).bind(*e.event_id().as_uuid()).bind(to_database_time(e.recorded_at())?).bind(e.digest().as_bytes()).execute(&mut **tx).await.map_err(|e|StoreError::database("whole closed caller insert",e))?;
    Ok(())
}

// Low-level component restoration. Public readers additionally authenticate the
// complete owning closure before exposing this as a terminal physical attempt.
pub(super) async fn load_completion(
    tx: &mut Transaction<'_, Postgres>,
    start: &NodeAttemptStart,
) -> Result<Option<NodeAttemptCompletion>, StoreError> {
    let a = start.activation();
    let Some(saved)=query("SELECT completion_bytes,completion_checksum,completion_digest,caller_start_digest,journal_sequence,journal_event_id,journal_recorded_at,journal_digest FROM stateknot.graph_frame_closed_callers WHERE tenant_id=$1 AND run_id=$2 AND caller_attempt_id=$3").bind(a.tenant_id().as_str()).bind(*a.run_id().as_uuid()).bind(*start.attempt_id().as_uuid()).fetch_optional(&mut **tx).await.map_err(|e|StoreError::database("closed physical completion",e))? else{return Ok(None);};
    let bytes: Vec<u8> = saved
        .try_get("completion_bytes")
        .map_err(|e| StoreError::database("closed physical bytes", e))?;
    if bytes.is_empty() || bytes.len() > MAX_BYTES {
        return Err(StoreError::corrupt("closed physical byte bound"));
    }
    let completion: NodeAttemptCompletion = serde_json::from_slice(&bytes)
        .map_err(|_| StoreError::corrupt("closed physical completion value"))?;
    if encode_node_attempt_completion(&completion)? != bytes
        || completion.start() != &start.head()
        || completion.status() != NodeAttemptStatus::Failed
        || completion.usage() != &BudgetUsage::zero()
    {
        return Err(StoreError::corrupt("closed physical completion shape"));
    }
    for (name, value) in [
        ("completion_checksum", Digest::sha256(&bytes)),
        ("completion_digest", completion.digest()),
        ("caller_start_digest", start.digest()),
        ("journal_digest", completion.journal_head().digest()),
    ] {
        digest_column(&saved, name, value)?;
    }
    if saved
        .try_get::<i64, _>("journal_sequence")
        .map_err(|e| StoreError::database("closed physical sequence", e))?
        != i64::try_from(completion.journal_head().sequence().get())
            .map_err(|_| StoreError::corrupt("closed physical sequence"))?
        || saved
            .try_get::<Uuid, _>("journal_event_id")
            .map_err(|e| StoreError::database("closed physical event", e))?
            != *completion.journal_head().event_id().as_uuid()
        || saved
            .try_get::<DateTime<Utc>, _>("journal_recorded_at")
            .map_err(|e| StoreError::database("closed physical clock", e))?
            != to_database_time(completion.journal_head().recorded_at())?
    {
        return Err(StoreError::corrupt("closed physical journal projection"));
    }
    Ok(Some(completion))
}
pub(super) async fn recognize_completion(
    tx: &mut Transaction<'_, Postgres>,
    attempt: &NodeAttempt,
) -> Result<Option<JournalEvent>, StoreError> {
    if load_completion(tx, attempt.start()).await?.is_none() {
        return Ok(None);
    }
    let a = attempt.start().activation();
    let run = query_as::<_, RunRow>(SELECT_RUN)
        .bind(a.tenant_id().as_str())
        .bind(*a.run_id().as_uuid())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| StoreError::database("closed completion Run", e))?
        .ok_or(StoreError::RunNotFound)?;
    let run = decode_run(run)?;
    let saved = row(tx, a.tenant_id(), a.run_id())
        .await?
        .ok_or_else(|| StoreError::corrupt("closed completion owner missing"))?;
    let record = Box::pin(verified_record(tx, &run, saved)).await?;
    let actual = record
        .completions
        .iter()
        .find(|completion| completion.start() == &attempt.start().head());
    if !canonical_equal(&actual, &attempt.completion())? {
        return Err(StoreError::corrupt("closed completion substitution"));
    }
    Ok(Some(record.event))
}

// A direction-only completion anchor used while the owning full frame proof is
// being traversed. No portable/cached proof or public terminal read uses it.
pub(super) async fn verify_completion_anchor(
    tx: &mut Transaction<'_, Postgres>,
    attempt: &NodeAttempt,
) -> Result<bool, StoreError> {
    let Some(completion) = load_completion(tx, attempt.start()).await? else {
        return Ok(false);
    };
    let a = attempt.start().activation();
    let saved = row(tx, a.tenant_id(), a.run_id())
        .await?
        .ok_or_else(|| StoreError::corrupt("closed caller whole anchor missing"))?;
    let record = Box::pin(anchored_record(tx, saved)).await?;
    let actual = record
        .completions
        .iter()
        .find(|value| value.start() == &attempt.start().head());
    if !canonical_equal(&actual, &Some(&completion))?
        || !canonical_equal(&actual, &attempt.completion())?
        || completion.journal_head() != &record.event.head()
    {
        return Err(StoreError::corrupt(
            "closed caller whole anchor substitution",
        ));
    }
    Ok(true)
}
