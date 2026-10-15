// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Root graph dispositions bound to the original whole journal transaction.
#[allow(clippy::wildcard_imports)]
use super::*;
use sqlx_core::row::Row as _;
use stateknot_core::{
    AgentResult, AgentTerminalValidationInputs, GraphBarrierDisposition, GraphBarrierPlan,
    NodeControl, NodeWaits,
};

#[path = "trusted_graph_root.rs"]
mod trusted_graph;

pub(super) const EVENT_KIND: &str = "root-graph-barrier-committed";
const INTENT_DOMAIN: &[u8] = b"stateknot-postgres-root-graph-barrier-intent-v1\0";
const DISPOSITION_DOMAIN: &[u8] = b"stateknot-postgres-root-graph-disposition-v1\0";
const HEADS_DOMAIN: &[u8] = b"stateknot-postgres-root-graph-result-heads-v1\0";
const COMPOUND_DOMAIN: &[u8] = b"stateknot-postgres-root-graph-barrier-compound-v1\0";
const TRUSTED_INTENT_DOMAIN: &[u8] = b"stateknot-postgres-trusted-root-graph-barrier-intent-v2\0";
const TRUSTED_COMPOUND_DOMAIN: &[u8] =
    b"stateknot-postgres-trusted-root-graph-barrier-compound-v2\0";
// Each lifecycle keeps its existing 2 MiB bound. A head cannot exceed its
// complete checkpoint's existing bound. The remaining fixed fields and at
// most 64 UUID/digest wait references fit the reserved 36 KiB allowance.
const MAX_BUNDLE_BYTES: usize =
    2 * JsonLimits::MAXIMUM.max_bytes() + 2 * MAX_CHECKPOINT_BYTES + 36 * 1024;
// The complete original validation snapshot keeps its Core bound. The original
// initial head fits its complete checkpoint; three journal heads, provenance
// and fixed source metadata fit the additional 16 KiB allowance.
const MAX_TRUSTED_BUNDLE_BYTES: usize = MAX_BUNDLE_BYTES
    + AgentTerminalValidationInputs::MAX_SNAPSHOT_BYTES
    + MAX_CHECKPOINT_BYTES
    + 16 * 1024;

/// Immutable complete graph plan and original lifecycle inputs for one commit.
/// Construction grants no database or dispatch authority.
#[derive(Clone, Debug)]
pub struct RootGraphBarrierIntent {
    data: Box<IntentData>,
}
#[derive(Clone, Debug)]
struct IntentData {
    plan: GraphBarrierPlan,
    before: RunLifecycle,
    event_id: EventId,
    terminal_result: Option<AgentResult>,
    wire: IntentWire,
    digest: Digest,
}

impl RootGraphBarrierIntent {
    /// Prepares the closed Continue, Wait, or successful Terminal projection.
    ///
    /// # Errors
    /// Rejects crossed ownership, inactive source lifecycle, missing or extra
    /// terminal evidence, output disagreement, or canonical encoding failure.
    pub fn new(
        plan: GraphBarrierPlan,
        original_lifecycle: RunLifecycle,
        event_id: EventId,
        terminal_result: Option<AgentResult>,
    ) -> Result<Self, StoreError> {
        let barrier = plan.barrier();
        let base = barrier.base_checkpoint();
        let owner = original_lifecycle.provenance();
        if original_lifecycle.status() != RunStatus::Active
            || owner.tenant_id() != base.tenant_id()
            || owner.run_id() != base.run_id()
            || barrier
                .result_heads()
                .iter()
                .any(|head| !head.activation().graph_namespace().is_root())
        {
            return Err(StoreError::InvalidCheckpointBarrier);
        }
        encode_lifecycle(&original_lifecycle)?;
        let disposition = compact_disposition(plan.disposition(), base, event_id)?;
        let original_projection_digest = match (plan.disposition(), terminal_result.as_ref()) {
            (GraphBarrierDisposition::Continue, None) => {
                barrier_projection_digest(&RunProjection::unchanged(), barrier.intent_digest())?
            }
            (GraphBarrierDisposition::Wait { waits }, None) => wait_barrier_projection_digest(
                original_lifecycle.revision(),
                barrier.intent_digest(),
                &waits
                    .registration_intents(base.tenant_id(), base.run_id(), event_id)
                    .map_err(|_| StoreError::InvalidWaitRegistrationBatch)?,
            )?,
            (GraphBarrierDisposition::Terminal { output }, Some(result))
                if result.provenance() == owner
                    && result.output_schema() == output.schema()
                    && result.output() == output.data() =>
            {
                barrier_projection_digest(
                    &RunProjection::transition(
                        original_lifecycle.revision(),
                        RunTransition::Succeed {
                            result: result.clone(),
                        },
                    ),
                    barrier.intent_digest(),
                )?
            }
            _ => return Err(StoreError::InvalidLifecycleTransition),
        };
        let wire = IntentWire {
            version: 1,
            source: None,
            terminal_validation: None,
            base: base.clone(),
            successor_checkpoint_id: barrier.successor().checkpoint_id(),
            successor_intent_digest: barrier.successor().intent_digest(),
            barrier_intent_digest: barrier.intent_digest(),
            result_count: barrier.result_heads().len(),
            result_heads_digest: domain_digest(HEADS_DOMAIN, &barrier.result_heads())?,
            disposition,
            lifecycle_before: Box::new(original_lifecycle.clone()),
            event_id,
            terminal_result_digest: terminal_result
                .as_ref()
                .map(|result| domain_digest(DISPOSITION_DOMAIN, result))
                .transpose()?,
            original_projection_digest,
        };
        let digest = intent_digest(&wire)?;
        Ok(Self {
            data: Box::new(IntentData {
                plan,
                before: original_lifecycle,
                event_id,
                terminal_result,
                wire,
                digest,
            }),
        })
    }

    /// Returns the Store-owned fixed payload referencing the exact intent.
    ///
    /// # Errors
    /// Rejects fixed schema or canonical payload encoding failure.
    pub fn payload(&self) -> Result<JournalPayload, StoreError> {
        payload(&self.data.wire, self.data.digest)
    }

    /// Returns the original semantic plan, without granting fresh execution.
    #[must_use]
    pub fn plan(&self) -> &GraphBarrierPlan {
        &self.data.plan
    }

    /// Returns the complete original source lifecycle retained across retries.
    #[must_use]
    pub fn original_lifecycle(&self) -> &RunLifecycle {
        &self.data.before
    }

    fn with_trusted_source(
        mut self,
        source: Box<trusted_graph::Source>,
        validation: Option<AgentTerminalValidationInputs>,
    ) -> Result<Self, StoreError> {
        match (&self.data.terminal_result, &validation) {
            (Some(result), Some(inputs)) => result
                .validate_for_terminal_inputs(self.data.before.provenance(), inputs)
                .map_err(|_| StoreError::InvalidLifecycleTransition)?,
            (None, None) => (),
            _ => return Err(StoreError::InvalidLifecycleTransition),
        }
        self.data.wire.version = 2;
        self.data.wire.source = Some(source);
        self.data.wire.terminal_validation = validation.map(Box::new);
        self.data.digest = intent_digest(&self.data.wire)?;
        Ok(self)
    }
}

/// Complete authenticated Root barrier, including original lifecycle evidence.
#[derive(Clone, Debug)]
pub struct StoredRootGraphBarrier {
    event: JournalEvent,
    barrier: CheckpointBarrier,
    checkpoint: Checkpoint,
    disposition: GraphBarrierDisposition,
    waits: Vec<DurableWait>,
    wire: Box<Wire>,
}
impl StoredRootGraphBarrier {
    /// Returns the original complete journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        &self.event
    }
    /// Returns the complete authenticated result barrier.
    #[must_use]
    pub const fn barrier(&self) -> &CheckpointBarrier {
        &self.barrier
    }
    /// Returns the complete committed successor.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
    /// Returns the complete historical semantic disposition.
    #[must_use]
    pub const fn disposition(&self) -> &GraphBarrierDisposition {
        &self.disposition
    }
    /// Returns the canonical lifecycle before the original commit.
    #[must_use]
    pub fn original_lifecycle(&self) -> &RunLifecycle {
        &self.wire.intent.lifecycle_before
    }
    /// Returns the canonical lifecycle committed by the original transaction.
    #[must_use]
    pub fn committed_lifecycle(&self) -> &RunLifecycle {
        &self.wire.lifecycle_after
    }
    /// Returns the original lifecycle/control projection digest.
    #[must_use]
    pub fn original_projection_digest(&self) -> Digest {
        self.wire.intent.original_projection_digest
    }
    /// Returns the complete compound projection digest.
    #[must_use]
    pub fn digest(&self) -> Digest {
        self.wire.compound_digest
    }
    /// Returns the immutable exact registration batch; terminal status is separate.
    #[must_use]
    pub fn waits(&self) -> &[DurableWait] {
        &self.waits
    }
    /// Returns the exact journal observation used by the original append.
    #[must_use]
    pub fn source_journal(&self) -> &JournalHead {
        &self.wire.source_journal
    }

    /// Returns original trusted-graph terminal validation inputs, if present.
    /// Agent V1 obtains these inputs from its authenticated admission instead.
    #[must_use]
    pub fn terminal_validation_inputs(&self) -> Option<&AgentTerminalValidationInputs> {
        self.wire.intent.terminal_validation.as_deref()
    }
}

/// A recovered original commit grants no fresh result execution or dispatch.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum RootGraphBarrierCommitOutcome {
    /// All Root barrier components committed in one transaction.
    Committed(StoredRootGraphBarrier),
    /// The original complete transaction was reloaded and authenticated.
    Idempotent(StoredRootGraphBarrier),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WaitReference {
    Interrupt {
        wait_id: InterruptId,
        intent_digest: Digest,
    },
    Timer {
        wait_id: TimerId,
        intent_digest: Digest,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Disposition {
    Continue,
    Wait {
        digest: Digest,
        #[serde(deserialize_with = "bounded_wait_references")]
        registrations: Vec<WaitReference>,
    },
    Terminal {
        output_digest: Digest,
    },
}
fn bounded_wait_references<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<WaitReference>, D::Error> {
    struct Visitor;
    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = Vec<WaitReference>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("at most 64 complete wait references")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let mut values =
                Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(NodeWaits::MAX_LEN));
            while values.len() < NodeWaits::MAX_LEN {
                let Some(value) = sequence.next_element()? else {
                    return Ok(values);
                };
                values.push(value);
            }
            if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom("Root wait reference bound"));
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Visitor)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IntentWire {
    version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source: Option<Box<trusted_graph::Source>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_validation: Option<Box<AgentTerminalValidationInputs>>,
    base: CheckpointHead,
    successor_checkpoint_id: CheckpointId,
    successor_intent_digest: Digest,
    barrier_intent_digest: Digest,
    result_count: usize,
    result_heads_digest: Digest,
    disposition: Disposition,
    lifecycle_before: Box<RunLifecycle>,
    event_id: EventId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_result_digest: Option<Digest>,
    original_projection_digest: Digest,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    version: u8,
    intent: Box<IntentWire>,
    intent_digest: Digest,
    source_journal: JournalHead,
    checkpoint: CheckpointHead,
    lifecycle_after: Box<RunLifecycle>,
    compound_digest: Digest,
}
#[derive(Serialize)]
struct Compound<'a> {
    version: u8,
    intent_digest: Digest,
    source_journal: &'a JournalHead,
    event: &'a JournalEvent,
    checkpoint: &'a CheckpointHead,
    lifecycle_after: &'a RunLifecycle,
}
fn domain_digest<T: Serialize>(domain: &[u8], value: &T) -> Result<Digest, StoreError> {
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| StoreError::encoding("Root barrier canonical preimage"))?;
    let mut bytes = Vec::with_capacity(domain.len() + canonical.len());
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(&canonical);
    Ok(Digest::sha256(bytes))
}
fn intent_digest(intent: &IntentWire) -> Result<Digest, StoreError> {
    domain_digest(
        if intent.version == 1 {
            INTENT_DOMAIN
        } else {
            TRUSTED_INTENT_DOMAIN
        },
        intent,
    )
}
fn compound(wire: &Wire, event: &JournalEvent) -> Result<Digest, StoreError> {
    domain_digest(
        if wire.version == 1 {
            COMPOUND_DOMAIN
        } else {
            TRUSTED_COMPOUND_DOMAIN
        },
        &Compound {
            version: wire.version,
            intent_digest: wire.intent_digest,
            source_journal: &wire.source_journal,
            event,
            checkpoint: &wire.checkpoint,
            lifecycle_after: &wire.lifecycle_after,
        },
    )
}
fn compact_disposition(
    disposition: &GraphBarrierDisposition,
    base: &CheckpointHead,
    event_id: EventId,
) -> Result<Disposition, StoreError> {
    match disposition {
        GraphBarrierDisposition::Continue => Ok(Disposition::Continue),
        GraphBarrierDisposition::Wait { waits } => {
            let intents = waits
                .registration_intents(base.tenant_id(), base.run_id(), event_id)
                .map_err(|_| StoreError::InvalidWaitRegistrationBatch)?;
            Ok(Disposition::Wait {
                digest: domain_digest(DISPOSITION_DOMAIN, waits)?,
                registrations: intents
                    .iter()
                    .map(|intent| match intent {
                        WaitRegistrationIntent::Interrupt { request } => WaitReference::Interrupt {
                            wait_id: request.interrupt_id(),
                            intent_digest: request.intent_digest(),
                        },
                        WaitRegistrationIntent::Timer { timer } => WaitReference::Timer {
                            wait_id: timer.timer_id(),
                            intent_digest: timer.intent_digest(),
                        },
                    })
                    .collect(),
            })
        }
        GraphBarrierDisposition::Terminal { output } => Ok(Disposition::Terminal {
            output_digest: output.digest(),
        }),
        _ => Err(StoreError::InvalidCheckpointBarrier),
    }
}
fn disposition_from_results(
    results: &[PendingNodeResult],
) -> Result<GraphBarrierDisposition, StoreError> {
    let mut waits = Vec::new();
    let mut output = None;
    for result in results {
        match result.intent().control() {
            NodeControl::Continue | NodeControl::Route { .. } => (),
            NodeControl::Wait { waits: batch } => waits.extend(batch.iter().cloned()),
            NodeControl::Terminal { output: value } => {
                if output.replace(value.clone()).is_some() {
                    return Err(StoreError::corrupt("Root terminal control exclusivity"));
                }
            }
        }
    }
    if let Some(output) = output {
        if results.len() != 1 || !waits.is_empty() {
            return Err(StoreError::corrupt("Root terminal control exclusivity"));
        }
        return Ok(GraphBarrierDisposition::Terminal { output });
    }
    if !waits.is_empty() {
        return Ok(GraphBarrierDisposition::Wait {
            waits: NodeWaits::try_new(waits)
                .map_err(|_| StoreError::corrupt("Root barrier complete waits"))?,
        });
    }
    Ok(GraphBarrierDisposition::Continue)
}

const SCHEMA_ID: &str = "https://stknot.com/schemas/store/root-graph-barrier-event/1.0.0";
const TRUSTED_SCHEMA_ID: &str = "https://stknot.com/schemas/store/root-graph-barrier-event/2.0.0";
static EVENT_SCHEMA: LazyLock<Result<stateknot_core::SchemaReference, &'static str>> =
    LazyLock::new(|| {
        let digest = serde_json::json!({"type":"string","minLength":71,"maxLength":71,"pattern":"^sha256:[0-9a-f]{64}$"});
        let id = serde_json::json!({"type":"string","minLength":36,"maxLength":36,"pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"});
        let document = serde_json::json!({ "$schema":"https://json-schema.org/draft/2020-12/schema", "$id":SCHEMA_ID, "type":"object", "additionalProperties":false,
        "properties":{"version":{"const":1},"graph_namespace":{"const":""},"base_checkpoint_id":id,"base_checkpoint_digest":digest,"successor_checkpoint_id":id,"successor_intent_digest":digest,"barrier_intent_digest":digest,"intent_digest":digest},
        "required":["version","graph_namespace","base_checkpoint_id","base_checkpoint_digest","successor_checkpoint_id","successor_intent_digest","barrier_intent_digest","intent_digest"] });
        let bytes =
            serde_json_canonicalizer::to_vec(&document).map_err(|_| "Root barrier schema")?;
        Ok(stateknot_core::SchemaReference::new(
            SCHEMA_ID.parse().map_err(|_| "Root barrier schema ID")?,
            stateknot_core::Version::new(1, 0, 0),
            Digest::sha256(bytes),
        ))
    });
static TRUSTED_EVENT_SCHEMA: LazyLock<Result<stateknot_core::SchemaReference, &'static str>> =
    LazyLock::new(|| {
        let digest = serde_json::json!({"type":"string","minLength":71,"maxLength":71,"pattern":"^sha256:[0-9a-f]{64}$"});
        let id = serde_json::json!({"type":"string","minLength":36,"maxLength":36,"pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"});
        let document = serde_json::json!({ "$schema":"https://json-schema.org/draft/2020-12/schema", "$id":TRUSTED_SCHEMA_ID, "type":"object", "additionalProperties":false,
        "properties":{"version":{"const":2},"source":{"const":"trusted_graph"},"graph_namespace":{"const":""},"base_checkpoint_id":id,"base_checkpoint_digest":digest,"successor_checkpoint_id":id,"successor_intent_digest":digest,"barrier_intent_digest":digest,"intent_digest":digest},
        "required":["version","source","graph_namespace","base_checkpoint_id","base_checkpoint_digest","successor_checkpoint_id","successor_intent_digest","barrier_intent_digest","intent_digest"] });
        let bytes = serde_json_canonicalizer::to_vec(&document)
            .map_err(|_| "Trusted Root barrier schema")?;
        Ok(stateknot_core::SchemaReference::new(
            TRUSTED_SCHEMA_ID
                .parse()
                .map_err(|_| "Trusted Root barrier schema ID")?,
            stateknot_core::Version::new(2, 0, 0),
            Digest::sha256(bytes),
        ))
    });
fn payload(intent: &IntentWire, digest: Digest) -> Result<JournalPayload, StoreError> {
    let (schema, data) = if intent.version == 1 {
        (
            &EVENT_SCHEMA,
            serde_json::json!({"version":1,"graph_namespace":"","base_checkpoint_id":intent.base.checkpoint_id(),"base_checkpoint_digest":intent.base.digest(),"successor_checkpoint_id":intent.successor_checkpoint_id,"successor_intent_digest":intent.successor_intent_digest,"barrier_intent_digest":intent.barrier_intent_digest,"intent_digest":digest}),
        )
    } else {
        (
            &TRUSTED_EVENT_SCHEMA,
            serde_json::json!({"version":2,"source":"trusted_graph","graph_namespace":"","base_checkpoint_id":intent.base.checkpoint_id(),"base_checkpoint_digest":intent.base.digest(),"successor_checkpoint_id":intent.successor_checkpoint_id,"successor_intent_digest":intent.successor_intent_digest,"barrier_intent_digest":intent.barrier_intent_digest,"intent_digest":digest}),
        )
    };
    let bytes = serde_json_canonicalizer::to_vec(&data)
        .map_err(|_| StoreError::encoding("Root barrier event payload"))?;
    JournalPayload::new(
        schema
            .as_ref()
            .map_err(|_| StoreError::encoding("Root barrier event schema"))?
            .clone(),
        EVENT_KIND
            .parse()
            .map_err(|_| StoreError::encoding("Root barrier event kind"))?,
        BoundedJson::from_slice(&bytes)
            .map_err(|_| StoreError::encoding("Root barrier event bound"))?,
    )
    .map_err(|_| StoreError::encoding("Root barrier event payload"))
}

fn encode(wire: &Wire) -> Result<Vec<u8>, StoreError> {
    encode_lifecycle(&wire.intent.lifecycle_before)?;
    encode_lifecycle(&wire.lifecycle_after)?;
    let bytes = serde_json_canonicalizer::to_vec(wire)
        .map_err(|_| StoreError::encoding("Root barrier bundle"))?;
    let maximum = if wire.version == 1 {
        MAX_BUNDLE_BYTES
    } else {
        MAX_TRUSTED_BUNDLE_BYTES
    };
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(StoreError::InvalidCheckpointBarrier);
    }
    Ok(bytes)
}
fn decode(bytes: &[u8]) -> Result<Box<Wire>, StoreError> {
    if bytes.is_empty() || bytes.len() > MAX_TRUSTED_BUNDLE_BYTES {
        return Err(StoreError::corrupt("Root barrier byte bound"));
    }
    let wire: Box<Wire> =
        serde_json::from_slice(bytes).map_err(|_| StoreError::corrupt("Root barrier wire"))?;
    let source_shape = match wire.version {
        1 => {
            wire.intent.version == 1
                && wire.intent.source.is_none()
                && wire.intent.terminal_validation.is_none()
        }
        2 => {
            wire.intent.version == 2
                && wire.intent.source.is_some()
                && (wire.intent.terminal_validation.is_some()
                    == matches!(wire.intent.disposition, Disposition::Terminal { .. }))
        }
        _ => false,
    };
    if !source_shape
        || wire.intent.result_count == 0
        || wire.intent.result_count > BarrierResultHeads::MAX_LEN
        || matches!(&wire.intent.disposition, Disposition::Wait { registrations, .. } if registrations.is_empty() || registrations.len() > NodeWaits::MAX_LEN)
        || encode(&wire).map_err(|_| StoreError::corrupt("Root barrier canonical wire"))? != bytes
    {
        return Err(StoreError::corrupt(
            "Root barrier version or canonical wire",
        ));
    }
    Ok(wire)
}

struct Row {
    bytes: Vec<u8>,
    checksum: Vec<u8>,
    projection: Vec<u8>,
}
impl<'r> FromRow<'r, PgRow> for Row {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx_core::error::Error> {
        Ok(Self {
            bytes: row.try_get("bundle_bytes")?,
            checksum: row.try_get("bundle_checksum")?,
            projection: row.try_get("compound_digest")?,
        })
    }
}
async fn row(
    tx: &mut Transaction<'_, Postgres>,
    base: &CheckpointHead,
) -> Result<Option<Row>, StoreError> {
    let length: Option<i32> = query_scalar("SELECT octet_length(bundle_bytes) FROM stateknot.root_graph_barriers WHERE tenant_id=$1 AND run_id=$2 AND base_checkpoint_id=$3")
        .bind(base.tenant_id().as_str()).bind(*base.run_id().as_uuid()).bind(*base.checkpoint_id().as_uuid()).fetch_optional(&mut **tx).await.map_err(|source| StoreError::database("Root barrier compact lookup", source))?;
    if length.is_some_and(|len| {
        usize::try_from(len).map_or(true, |len| len == 0 || len > MAX_TRUSTED_BUNDLE_BYTES)
    }) {
        return Err(StoreError::corrupt("Root barrier byte bound"));
    }
    if length.is_none() {
        return Ok(None);
    }
    query_as("SELECT bundle_bytes,bundle_checksum,compound_digest FROM stateknot.root_graph_barriers WHERE tenant_id=$1 AND run_id=$2 AND base_checkpoint_id=$3")
        .bind(base.tenant_id().as_str()).bind(*base.run_id().as_uuid()).bind(*base.checkpoint_id().as_uuid()).fetch_optional(&mut **tx).await.map_err(|source| StoreError::database("Root barrier bundle load", source))
}
async fn checkpoint(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
    id: CheckpointId,
) -> Result<Checkpoint, StoreError> {
    let row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .bind(*id.as_uuid())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|source| StoreError::database("Root barrier checkpoint", source))?
        .ok_or_else(|| StoreError::corrupt("Root barrier checkpoint"))?;
    decode_checkpoint(row)
}

// Local complete edge only. The caller authenticates the base prefix once.
// No application callback and no recursive historical prefix traversal.
#[allow(clippy::too_many_lines)] // One local edge keeps every authentication step in the same snapshot.
pub(super) fn authenticated_edge<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    base: &'a Checkpoint,
    successor: &'a Checkpoint,
    results: &'a [PendingNodeResult],
) -> stateknot_core::BoxFuture<'a, Result<Option<StoredRootGraphBarrier>, StoreError>> {
    Box::pin(async move {
        let event_row = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
            .bind(base.tenant_id().as_str())
            .bind(*base.run_id().as_uuid())
            .bind(
                i64::try_from(successor.journal_head().sequence().get())
                    .map_err(|_| StoreError::corrupt("Root barrier sequence"))?,
            )
            .fetch_optional(&mut **tx)
            .await
            .map_err(|source| StoreError::database("Root barrier journal", source))?
            .ok_or_else(|| StoreError::corrupt("Root barrier journal"))?;
        let projection = event_row
            .projection_digest
            .as_deref()
            .map(|bytes| decode_digest(bytes, "Root barrier event projection"))
            .transpose()?;
        let event = decode_event(event_row)?;
        let row = row(tx, &base.head()).await?;
        if event.payload().kind().as_str() != EVENT_KIND {
            if row.is_some() {
                return Err(StoreError::corrupt(
                    "Root barrier row without its reserved event",
                ));
            }
            return Ok(None);
        }
        let row = row.ok_or_else(|| StoreError::corrupt("Root barrier missing whole bundle"))?;
        if Digest::sha256(&row.bytes) != decode_digest(&row.checksum, "Root barrier checksum")? {
            return Err(StoreError::corrupt("Root barrier checksum"));
        }
        let wire = decode(&row.bytes)?;
        if wire.intent.base != base.head()
            || wire.checkpoint != successor.head()
            || event.head() != *successor.journal_head()
            || event.source().worker_fence().is_none()
            || wire.intent.event_id != event.event_id()
            || successor.parent() != Some(&base.head())
            || wire.source_journal.tenant_id() != base.tenant_id()
            || wire.source_journal.run_id() != base.run_id()
            || wire.source_journal.sequence().get().checked_add(1) != Some(event.sequence().get())
            || event.previous_digest() != Some(wire.source_journal.digest())
            || wire.source_journal.recorded_at() > event.recorded_at()
            || payload(&wire.intent, wire.intent_digest)? != *event.payload()
            || intent_digest(&wire.intent)? != wire.intent_digest
            || compound(&wire, &event)? != wire.compound_digest
            || projection != Some(wire.compound_digest)
            || decode_digest(&row.projection, "Root barrier compound column")?
                != wire.compound_digest
        {
            return Err(StoreError::corrupt("Root barrier journal binding"));
        }
        let (previous, _) = child_runs::anchored_event(
            tx,
            base.tenant_id(),
            base.run_id(),
            i64::try_from(wire.source_journal.sequence().get())
                .map_err(|_| StoreError::corrupt("Root source sequence"))?,
        )
        .await?;
        if previous.head() != wire.source_journal {
            return Err(StoreError::corrupt("Root barrier source journal"));
        }
        let barrier = CheckpointBarrier::new(
            base,
            successor.write_intent(),
            results.iter().map(PendingNodeResult::head),
        )
        .map_err(|_| StoreError::corrupt("Root barrier complete intent"))?;
        if wire.intent.successor_checkpoint_id != successor.checkpoint_id()
            || wire.intent.successor_intent_digest != successor.write_intent().intent_digest()
            || wire.intent.barrier_intent_digest != barrier.intent_digest()
            || wire.intent.result_count != results.len()
            || wire.intent.result_heads_digest
                != domain_digest(HEADS_DOMAIN, &barrier.result_heads())?
        {
            return Err(StoreError::corrupt("Root barrier complete result heads"));
        }
        let disposition = disposition_from_results(results)?;
        let result = wire.lifecycle_after.result().cloned();
        let expected_compact = compact_disposition(&disposition, &base.head(), event.event_id())
            .map_err(|_| StoreError::corrupt("Root historical disposition encoding"))?;
        if wire.intent.disposition != expected_compact {
            return Err(StoreError::corrupt("Root barrier historical disposition"));
        }
        let (after, waits, original_projection_digest) = materialize_lifecycle(
            &wire.intent.lifecycle_before,
            &barrier,
            &disposition,
            result.as_ref(),
            &event,
        )
        .map_err(|_| StoreError::corrupt("Root barrier historical lifecycle projection"))?;
        if encode_lifecycle(&after)? != encode_lifecycle(&wire.lifecycle_after)?
            || original_projection_digest != wire.intent.original_projection_digest
            || result
                .as_ref()
                .map(|value| domain_digest(DISPOSITION_DOMAIN, value))
                .transpose()?
                != wire.intent.terminal_result_digest
        {
            return Err(StoreError::corrupt("Root barrier historical lifecycle"));
        }
        verify_barrier_consumptions(tx, &barrier, successor)
            .await
            .map_err(map_graph_replay_consumption_error)?;
        verify_wait_registration_set(tx, &event, &waits)
            .await
            .map_err(|error| match error {
                StoreError::WaitRegistrationCommitConflict => {
                    StoreError::corrupt("Root barrier complete wait set")
                }
                other => other,
            })?;
        verify_columns(tx, &wire, &event).await?;
        verify_wait_terminals(tx, &event, &waits).await?;
        authenticate_intent_source(tx, &wire.intent).await?;
        if let Some(result) = result.as_ref() {
            validate_original_terminal(tx, &wire.intent, result)
                .await
                .map_err(|error| match error {
                    StoreError::InvalidLifecycleTransition => {
                        StoreError::corrupt("Root historical complete terminal inputs")
                    }
                    other => other,
                })?;
            if let Some(account) = Box::pin(child_runs::load_account_inner(
                tx,
                event.tenant_id(),
                event.run_id(),
                false,
            ))
            .await?
            {
                child_runs::ensure_settled(&account).map_err(|_| {
                    StoreError::corrupt("Root historical terminal child settlement")
                })?;
                let direct = result
                    .usage()
                    .checked_subtract_cumulative(
                        &account
                            .delegated_usage()
                            .map_err(|_| StoreError::corrupt("Root historical delegated usage"))?,
                    )
                    .map_err(|_| StoreError::corrupt("Root historical terminal child usage"))?;
                if account.direct_head() != &event.head() || account.direct_usage() != &direct {
                    return Err(StoreError::corrupt(
                        "Root historical terminal direct anchor",
                    ));
                }
            }
        }
        Ok(Some(StoredRootGraphBarrier {
            event,
            barrier,
            checkpoint: successor.clone(),
            disposition,
            waits,
            wire,
        }))
    })
}

fn authenticated_admission<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    base: &'a CheckpointHead,
) -> stateknot_core::BoxFuture<'a, Result<Box<StoredAgentAdmission>, StoreError>> {
    Box::pin(async move {
        let run = query_as::<_, RunRow>(SELECT_RUN)
            .bind(base.tenant_id().as_str())
            .bind(*base.run_id().as_uuid())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|source| StoreError::database("Root admission run", source))?
            .ok_or_else(|| StoreError::corrupt("Root admission run"))?;
        let row = load_agent_admission_row(tx, base.tenant_id(), base.run_id())
            .await?
            .ok_or_else(|| StoreError::corrupt("Root admission missing"))?;
        let admission =
            Box::new(Box::pin(verify_stored_agent_admission(tx, decode_run(run)?, row)).await?);
        if admission.admission().intent().graph() != base.graph() {
            return Err(StoreError::corrupt("Root admission graph"));
        }
        Ok(admission)
    })
}

fn authenticate_intent_source<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    intent: &'a IntentWire,
) -> stateknot_core::BoxFuture<'a, Result<(), StoreError>> {
    Box::pin(async move {
        if let Some(source) = intent.source.as_deref() {
            return trusted_graph::verify(tx, &intent.base, &intent.lifecycle_before, source).await;
        }
        let admission = authenticated_admission(tx, &intent.base).await?;
        if intent.lifecycle_before.provenance() != admission.admission().intent().provenance()
            || intent.lifecycle_before.admitted_at() != admission.admission().admitted_at()
        {
            return Err(StoreError::corrupt("Root historical admission lifecycle"));
        }
        Ok(())
    })
}

fn validate_original_terminal<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    intent: &'a IntentWire,
    result: &'a AgentResult,
) -> stateknot_core::BoxFuture<'a, Result<(), StoreError>> {
    Box::pin(async move {
        if intent.source.is_some() {
            let inputs = intent
                .terminal_validation
                .as_deref()
                .ok_or(StoreError::InvalidLifecycleTransition)?;
            return result
                .validate_for_terminal_inputs(intent.lifecycle_before.provenance(), inputs)
                .map_err(|_| StoreError::InvalidLifecycleTransition);
        }
        let admission = authenticated_admission(tx, &intent.base).await?;
        let source = admission.admission().intent();
        result
            .validate_for(
                source.provenance(),
                source.request(),
                source.descriptor(),
                source.budget(),
            )
            .map_err(|_| StoreError::InvalidLifecycleTransition)
    })
}

fn materialize_lifecycle(
    before: &RunLifecycle,
    barrier: &CheckpointBarrier,
    disposition: &GraphBarrierDisposition,
    result: Option<&AgentResult>,
    event: &JournalEvent,
) -> Result<(RunLifecycle, Vec<DurableWait>, Digest), StoreError> {
    if before.status() != RunStatus::Active
        || before.provenance().tenant_id() != event.tenant_id()
        || before.provenance().run_id() != event.run_id()
    {
        return Err(StoreError::InvalidLifecycleTransition);
    }
    let (after, waits, digest) = match (disposition, result) {
        (GraphBarrierDisposition::Continue, None) => (
            before.clone(),
            Vec::new(),
            barrier_projection_digest(&RunProjection::unchanged(), barrier.intent_digest())?,
        ),
        (GraphBarrierDisposition::Wait { waits }, None) => {
            let intents = waits
                .registration_intents(event.tenant_id(), event.run_id(), event.event_id())
                .map_err(|_| StoreError::InvalidWaitRegistrationBatch)?;
            let digest = wait_barrier_projection_digest(
                before.revision(),
                barrier.intent_digest(),
                &intents,
            )?;
            let waits = materialize_wait_registrations(intents, event)?;
            let markers = RunWaits::try_new(waits.iter().map(DurableWait::marker))
                .map_err(|_| StoreError::InvalidWaitRegistrationBatch)?;
            let after = before
                .clone()
                .apply(RunTransition::Wait { waits: markers })
                .map_err(|_| StoreError::InvalidLifecycleTransition)?;
            (after, waits, digest)
        }
        (GraphBarrierDisposition::Terminal { output }, Some(result))
            if result.provenance() == before.provenance()
                && result.output_schema() == output.schema()
                && result.output() == output.data() =>
        {
            let projection = RunProjection::transition(
                before.revision(),
                RunTransition::Succeed {
                    result: result.clone(),
                },
            );
            let digest = barrier_projection_digest(&projection, barrier.intent_digest())?;
            (
                before
                    .clone()
                    .apply(RunTransition::Succeed {
                        result: result.clone(),
                    })
                    .map_err(|_| StoreError::InvalidLifecycleTransition)?,
                Vec::new(),
                digest,
            )
        }
        _ => return Err(StoreError::InvalidLifecycleTransition),
    };
    if after.changed_at() > event.recorded_at() {
        return Err(StoreError::LifecycleObservationAfterCommit);
    }
    Ok((after, waits, digest))
}

async fn verify_columns(
    tx: &mut Transaction<'_, Postgres>,
    wire: &Wire,
    event: &JournalEvent,
) -> Result<(), StoreError> {
    let base = &wire.intent.base;
    let kind = disposition_kind(&wire.intent.disposition);
    let correct: bool = query_scalar("SELECT graph_namespace='' AND base_superstep=$4 AND base_checkpoint_digest=$5 AND successor_checkpoint_id=$6 AND successor_superstep=$7 AND successor_checkpoint_digest=$8 AND result_count=$9 AND result_heads_digest=$10 AND barrier_intent_digest=$11 AND disposition_kind=$12 AND disposition_digest=$13 AND original_projection_digest=$14 AND lifecycle_before_revision=$15::numeric AND lifecycle_after_revision=$16::numeric AND lifecycle_before_bytes=$17 AND lifecycle_after_bytes=$18 AND intent_digest=$19 AND compound_digest=$20 AND journal_sequence=$21 AND journal_event_id=$22 AND journal_recorded_at=$23 AND journal_digest=$24 FROM stateknot.root_graph_barriers WHERE tenant_id=$1 AND run_id=$2 AND base_checkpoint_id=$3")
        .bind(base.tenant_id().as_str()).bind(*base.run_id().as_uuid()).bind(*base.checkpoint_id().as_uuid())
        .bind(i64::try_from(base.superstep().get()).map_err(|_| StoreError::corrupt("Root base superstep"))?).bind(base.digest().as_bytes())
        .bind(*wire.checkpoint.checkpoint_id().as_uuid()).bind(i64::try_from(wire.checkpoint.superstep().get()).map_err(|_| StoreError::corrupt("Root successor superstep"))?).bind(wire.checkpoint.digest().as_bytes())
        .bind(i32::try_from(wire.intent.result_count).map_err(|_| StoreError::corrupt("Root result count"))?).bind(wire.intent.result_heads_digest.as_bytes()).bind(wire.intent.barrier_intent_digest.as_bytes())
        .bind(kind).bind(domain_digest(DISPOSITION_DOMAIN,&wire.intent.disposition)?.as_bytes()).bind(wire.intent.original_projection_digest.as_bytes())
        .bind(wire.intent.lifecycle_before.revision().to_string()).bind(wire.lifecycle_after.revision().to_string())
        .bind(encode_lifecycle(&wire.intent.lifecycle_before)?).bind(encode_lifecycle(&wire.lifecycle_after)?)
        .bind(wire.intent_digest.as_bytes()).bind(wire.compound_digest.as_bytes())
        .bind(i64::try_from(event.sequence().get()).map_err(|_| StoreError::corrupt("Root journal sequence"))?).bind(*event.event_id().as_uuid()).bind(to_database_time(event.recorded_at())?).bind(event.digest().as_bytes())
        .fetch_one(&mut **tx).await.map_err(|source| StoreError::database("Root barrier complete columns",source))?;
    if !correct {
        return Err(StoreError::corrupt("Root barrier complete columns"));
    }
    Ok(())
}
fn disposition_kind(disposition: &Disposition) -> &'static str {
    match disposition {
        Disposition::Continue => "continue",
        Disposition::Wait { .. } => "wait",
        Disposition::Terminal { .. } => "terminal",
    }
}
async fn insert(
    tx: &mut Transaction<'_, Postgres>,
    record: &StoredRootGraphBarrier,
) -> Result<(), StoreError> {
    let wire = &record.wire;
    let base = &wire.intent.base;
    let event = &record.event;
    let worker = event
        .source()
        .worker_fence()
        .ok_or(StoreError::WrongAppendAuthority)?;
    let count = query("INSERT INTO stateknot.root_graph_barriers (tenant_id,run_id,base_checkpoint_id,base_superstep,base_checkpoint_digest,successor_checkpoint_id,successor_superstep,successor_checkpoint_digest,result_count,result_heads_digest,barrier_intent_digest,disposition_kind,disposition_digest,original_projection_digest,lifecycle_before_revision,lifecycle_after_revision,lifecycle_before_bytes,lifecycle_after_bytes,intent_digest,compound_digest,bundle_bytes,journal_sequence,journal_event_id,journal_recorded_at,journal_digest) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::numeric,$16::numeric,$17,$18,$19,$20,$21,$22,$23,$24,$25 WHERE EXISTS (SELECT 1 FROM stateknot.runs r WHERE r.tenant_id=$1 AND r.run_id=$2 AND r.lease_attempt_id=$26 AND r.fencing_epoch=$27 AND r.lease_expires_at>clock_timestamp() AND (($28::integer=1 AND r.agent_deadline_at>clock_timestamp()) OR ($28::integer=2 AND r.agent_deadline_at IS NULL)) AND r.lifecycle_status='active')")
        .bind(base.tenant_id().as_str()).bind(*base.run_id().as_uuid()).bind(*base.checkpoint_id().as_uuid()).bind(i64::try_from(base.superstep().get()).map_err(|_| StoreError::InvalidCheckpointBarrier)?).bind(base.digest().as_bytes())
        .bind(*wire.checkpoint.checkpoint_id().as_uuid()).bind(i64::try_from(wire.checkpoint.superstep().get()).map_err(|_| StoreError::InvalidCheckpointBarrier)?).bind(wire.checkpoint.digest().as_bytes())
        .bind(i32::try_from(wire.intent.result_count).map_err(|_| StoreError::InvalidCheckpointBarrier)?).bind(wire.intent.result_heads_digest.as_bytes()).bind(wire.intent.barrier_intent_digest.as_bytes())
        .bind(disposition_kind(&wire.intent.disposition)).bind(domain_digest(DISPOSITION_DOMAIN,&wire.intent.disposition)?.as_bytes()).bind(wire.intent.original_projection_digest.as_bytes())
        .bind(wire.intent.lifecycle_before.revision().to_string()).bind(wire.lifecycle_after.revision().to_string()).bind(encode_lifecycle(&wire.intent.lifecycle_before)?).bind(encode_lifecycle(&wire.lifecycle_after)?)
        .bind(wire.intent_digest.as_bytes()).bind(wire.compound_digest.as_bytes()).bind(encode(wire)?)
        .bind(i64::try_from(event.sequence().get()).map_err(|_| StoreError::JournalSequenceExhausted)?).bind(*event.event_id().as_uuid()).bind(to_database_time(event.recorded_at())?).bind(event.digest().as_bytes())
        .bind(*worker.attempt_id().as_uuid()).bind(i64::try_from(worker.epoch().get()).map_err(|_| StoreError::StaleFence)?)
        .bind(i32::from(wire.version))
        .execute(&mut **tx).await.map_err(|source| StoreError::database("Root barrier whole insert",source))?.rows_affected();
    if count != 1 {
        return Err(StoreError::LeaseExpired);
    }
    Ok(())
}

pub(super) async fn results(
    tx: &mut Transaction<'_, Postgres>,
    base: &Checkpoint,
) -> Result<Vec<PendingNodeResult>, StoreError> {
    let heads = load_locked_barrier_result_heads(tx, &base.head()).await?;
    let mut results = Vec::with_capacity(heads.len());
    for head in &heads {
        let activation = head.activation();
        if !activation.graph_namespace().is_root() || activation.base_checkpoint() != &base.head() {
            return Err(StoreError::corrupt("Root barrier result namespace"));
        }
        let row = load_pending_node_result_row(tx, activation)
            .await?
            .ok_or_else(|| StoreError::corrupt("Root barrier result"))?;
        let result = decode_pending_node_result(&row)?;
        if result.head() != *head || !pending_node_result_activation_is_ready(base, result.intent())
        {
            return Err(StoreError::corrupt("Root barrier result activation"));
        }
        Box::pin(verify_pending_node_result_components(tx, &result)).await?;
        results.push(result);
    }
    Ok(results)
}

fn local_record<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    base: &'a Checkpoint,
) -> stateknot_core::BoxFuture<'a, Result<Option<StoredRootGraphBarrier>, StoreError>> {
    Box::pin(async move {
        let next: Option<Uuid> = query_scalar("SELECT checkpoint_id FROM stateknot.run_checkpoints WHERE tenant_id=$1 AND run_id=$2 AND parent_checkpoint_id=$3 AND graph_namespace='' ORDER BY superstep LIMIT 1")
            .bind(base.tenant_id().as_str()).bind(*base.run_id().as_uuid()).bind(*base.checkpoint_id().as_uuid()).fetch_optional(&mut **tx).await.map_err(|source| StoreError::database("Root barrier successor",source))?;
        let Some(next) = next else {
            if row(tx, &base.head()).await?.is_some() {
                return Err(StoreError::corrupt("Root barrier missing successor"));
            }
            return Ok(None);
        };
        let successor = Box::new(
            checkpoint(
                tx,
                base.tenant_id(),
                base.run_id(),
                CheckpointId::from_uuid(next)
                    .map_err(|_| StoreError::corrupt("Root successor identity"))?,
            )
            .await?,
        );
        let results = Box::pin(results(tx, base)).await?;
        Box::pin(authenticated_edge(tx, base, &successor, &results)).await
    })
}

// Public/component reads authenticate every new-format ancestor with an
// iterative walk. The application pass uses authenticated_edge once per edge.
pub(super) fn verify_checkpoint<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    checkpoint_value: &'a Checkpoint,
) -> stateknot_core::BoxFuture<'a, Result<(), StoreError>> {
    Box::pin(async move {
        let mut current = Box::new(checkpoint_value.clone());
        loop {
            let (event, _) = child_runs::anchored_event(
                tx,
                current.tenant_id(),
                current.run_id(),
                i64::try_from(current.journal_head().sequence().get())
                    .map_err(|_| StoreError::corrupt("Root checkpoint sequence"))?,
            )
            .await?;
            if event.head() != *current.journal_head()
                || event.payload().kind().as_str() == GraphFrameEntryPlan::EVENT_KIND
            {
                return Err(StoreError::corrupt("Root checkpoint journal anchor"));
            }
            let Some(parent) = current.parent().cloned() else {
                if event.payload().kind().as_str() == EVENT_KIND
                    || current.superstep() != Superstep::INITIAL
                {
                    return Err(StoreError::corrupt("Root barrier parent"));
                }
                return Ok(());
            };
            let base = Box::new(
                checkpoint(
                    tx,
                    parent.tenant_id(),
                    parent.run_id(),
                    parent.checkpoint_id(),
                )
                .await?,
            );
            if base.head() != parent
                || base.superstep().get().checked_add(1) != Some(current.superstep().get())
                || base.journal_head().sequence() >= current.journal_head().sequence()
            {
                return Err(StoreError::corrupt("Root barrier parent"));
            }
            if event.payload().kind().as_str() == EVENT_KIND {
                let results = Box::pin(results(tx, &base)).await?;
                Box::pin(authenticated_edge(tx, &base, &current, &results))
                    .await?
                    .ok_or_else(|| StoreError::corrupt("Root barrier missing bundle"))?;
            } else if row(tx, &base.head()).await?.is_some() {
                return Err(StoreError::corrupt(
                    "Root barrier legacy event contradiction",
                ));
            }
            current = base;
        }
    })
}

impl PostgresStore {
    /// Verifies the actual creating source before any new Root graph dispatch.
    /// This reads one repeatable-read snapshot and grants no permission or
    /// reusable authority; every subsequent write retains its original fence.
    ///
    /// # Errors
    /// Returns stale snapshot/fence, unavailable legacy creation, complete
    /// owning corruption, expired Agent deadline or database failures.
    pub async fn validate_worker_graph_creating_source(
        &self,
        fence: &RunFence,
        base: &CheckpointHead,
        observed: &JournalHead,
    ) -> Result<(), StoreError> {
        if base.tenant_id() != fence.tenant_id()
            || base.run_id() != fence.run_id()
            || observed.tenant_id() != fence.tenant_id()
            || observed.run_id() != fence.run_id()
        {
            return Err(StoreError::StaleClaimedRunRecoveryObservation);
        }
        let mut tx = self
            .begin_repeatable_read("Root creating source before dispatch")
            .await?;
        let row = query_as::<_, RunRow>(SELECT_RUN)
            .bind(fence.tenant_id().as_str())
            .bind(*fence.run_id().as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|source| StoreError::database("Root creating source Run", source))?
            .ok_or(StoreError::RunNotFound)?;
        let stored = decode_run(row)?;
        validate_runnable(&stored)?;
        if stored.journal_head() != Some(observed) {
            return Err(StoreError::StaleClaimedRunRecoveryObservation);
        }
        let now = database_now(&mut tx, "Root creating source fence clock").await?;
        authorize_worker(&stored, fence, now)?;
        let actual = checkpoint(
            &mut tx,
            fence.tenant_id(),
            fence.run_id(),
            base.checkpoint_id(),
        )
        .await?;
        if actual.head() != *base
            || stored.checkpoint().is_none_or(|pointer| {
                pointer.checkpoint_id() != base.checkpoint_id()
                    || pointer.superstep() != base.superstep()
                    || pointer.digest() != base.digest()
            })
        {
            return Err(StoreError::StaleCheckpointHead);
        }
        verify_current_wait_set(&mut tx, &stored).await?;
        Box::pin(verify_run(&mut tx, &stored)).await?;
        let pinned = load_graph_definition_row(&mut tx, base.tenant_id(), base.graph())
            .await?
            .ok_or_else(|| StoreError::corrupt("Root dispatch pinned graph"))?;
        if decode_graph_definition(pinned)?.graph().reference() != *base.graph() {
            return Err(StoreError::corrupt("Root dispatch pinned graph"));
        }
        let agent = matches!(
            trusted_graph::authenticate(&mut tx, base, stored.lifecycle()).await?,
            trusted_graph::Origin::Agent(_)
        );
        let final_now = database_now(&mut tx, "Root creating source final clock").await?;
        authorize_worker(&stored, fence, final_now)?;
        let final_valid: bool = query_scalar("SELECT lease_attempt_id=$3 AND fencing_epoch=$4 AND lease_expires_at>clock_timestamp() AND journal_sequence=$5 AND journal_event_id=$6 AND journal_recorded_at=$7 AND journal_digest=$8 AND CASE WHEN $9 THEN agent_deadline_at>clock_timestamp() ELSE agent_deadline_at IS NULL END FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2")
            .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).bind(*fence.attempt_id().as_uuid())
            .bind(i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?)
            .bind(i64::try_from(observed.sequence().get()).map_err(|_| StoreError::StaleClaimedRunRecoveryObservation)?)
            .bind(*observed.event_id().as_uuid()).bind(to_database_time(observed.recorded_at())?).bind(observed.digest().as_bytes())
            .bind(agent).fetch_one(&mut *tx).await.map_err(|source| StoreError::database("Root dispatch final fence", source))?;
        if !final_valid {
            return Err(StoreError::LeaseExpired);
        }
        tx.commit()
            .await
            .map_err(|source| StoreError::database("Root creating source read commit", source))?;
        Ok(())
    }

    /// Prepares a Root plan using its authenticated original creating source.
    /// Agent V1 keeps its admission inputs. Trusted ordinary Graph V2 captures
    /// its positive Start/checkpoint source and, for Terminal, exact original
    /// validation inputs. This grants no fresh fence or dispatch authority.
    ///
    /// # Errors
    /// Rejects crossed ownership, corrupt sources, unavailable original legacy
    /// creation inputs or invalid terminal evidence, and database failures.
    pub async fn prepare_worker_graph_barrier(
        &self,
        plan: GraphBarrierPlan,
        original_lifecycle: RunLifecycle,
        event_id: EventId,
        terminal_result: Option<AgentResult>,
        terminal_validation: Option<AgentTerminalValidationInputs>,
    ) -> Result<RootGraphBarrierIntent, StoreError> {
        let intent =
            RootGraphBarrierIntent::new(plan, original_lifecycle, event_id, terminal_result)?;
        let base = intent.plan().barrier().base_checkpoint();
        let mut tx = self
            .begin_repeatable_read("Root graph creating source preflight")
            .await?;
        let checkpoint = checkpoint(
            &mut tx,
            base.tenant_id(),
            base.run_id(),
            base.checkpoint_id(),
        )
        .await?;
        if checkpoint.head() != *base {
            return Err(StoreError::StaleCheckpointHead);
        }
        Box::pin(verify_checkpoint(&mut tx, &checkpoint)).await?;
        let origin =
            trusted_graph::authenticate(&mut tx, base, intent.original_lifecycle()).await?;
        let intent = match origin {
            trusted_graph::Origin::Agent(admission) => {
                if intent.original_lifecycle().provenance()
                    != admission.admission().intent().provenance()
                    || intent.original_lifecycle().admitted_at()
                        != admission.admission().admitted_at()
                {
                    return Err(StoreError::InvalidLifecycleTransition);
                }
                intent
            }
            trusted_graph::Origin::TrustedGraph(source) => {
                intent.with_trusted_source(source, terminal_validation)?
            }
        };
        tx.commit().await.map_err(|source| {
            StoreError::database("Root graph creating source preflight commit", source)
        })?;
        Ok(intent)
    }

    /// Reads a complete journal-bound Root disposition for this exact base.
    /// `None` denotes an authenticated legacy edge, or an uncommitted base.
    ///
    /// # Errors
    /// Rejects crossed/stale heads, malformed whole bundles and missing
    /// recognized new-format evidence, or database failures.
    pub async fn load_root_graph_barrier(
        &self,
        base: &CheckpointHead,
    ) -> Result<Option<StoredRootGraphBarrier>, StoreError> {
        let mut tx = self
            .begin_repeatable_read("Root graph barrier read")
            .await?;
        let current = checkpoint(
            &mut tx,
            base.tenant_id(),
            base.run_id(),
            base.checkpoint_id(),
        )
        .await?;
        if current.head() != *base {
            return Err(StoreError::StaleCheckpointHead);
        }
        Box::pin(verify_checkpoint(&mut tx, &current)).await?;
        let record = Box::pin(local_record(&mut tx, &current)).await?;
        tx.commit()
            .await
            .map_err(|source| StoreError::database("Root graph barrier read commit", source))?;
        Ok(record)
    }

    /// Authenticates an exact original commit without deployed callbacks.
    /// `None` requires the caller to use the fresh complete-plan writer.
    ///
    /// # Errors
    /// Rejects changed retry inputs, incomplete recognized commits, corruption,
    /// or database failures. It grants no current-fence dispatch authority.
    pub async fn recover_worker_graph_barrier(
        &self,
        append: &JournalAppend,
        intent: &RootGraphBarrierIntent,
    ) -> Result<Option<StoredRootGraphBarrier>, StoreError> {
        let data = &intent.data;
        let base = data.plan.barrier().base_checkpoint();
        if append.worker_fence().is_none()
            || append.intent().tenant_id() != base.tenant_id()
            || append.intent().run_id() != base.run_id()
            || append.intent().event_id() != data.event_id
            || append.intent().payload() != &intent.payload()?
            || append.expectation().head().is_none()
        {
            return Err(StoreError::InvalidCheckpointBarrier);
        }
        let mut tx = self
            .begin_repeatable_read("Root barrier exact retry")
            .await?;
        let existing = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(base.tenant_id().as_str())
            .bind(*base.run_id().as_uuid())
            .bind(*data.event_id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|source| StoreError::database("Root barrier exact retry event", source))?;
        let record = if existing.is_some() {
            let parent = checkpoint(
                &mut tx,
                base.tenant_id(),
                base.run_id(),
                base.checkpoint_id(),
            )
            .await?;
            Box::pin(verify_checkpoint(&mut tx, &parent)).await?;
            let record = Box::pin(local_record(&mut tx, &parent))
                .await?
                .ok_or_else(|| StoreError::corrupt("Root barrier exact retry whole record"))?;
            verify_retry(&record, append, intent)?;
            Some(record)
        } else {
            None
        };
        tx.commit()
            .await
            .map_err(|source| StoreError::database("Root barrier exact retry commit", source))?;
        Ok(record)
    }

    /// Commits a real complete graph plan, closed disposition projection and
    /// original lifecycle in the same fenced transaction as all components.
    /// Exact lost-ACK recovery precedes fresh deployed callbacks.
    ///
    /// # Errors
    /// Rejects incomplete/substituted plans, crossed/stale/fenced inputs,
    /// unavailable callbacks, invalid terminal admission/accounting, corrupt
    /// historical evidence, or database failures.
    pub async fn append_worker_graph_barrier<V, R>(
        &self,
        append: JournalAppend,
        intent: RootGraphBarrierIntent,
        schemas: &V,
        reducer: &R,
    ) -> Result<RootGraphBarrierCommitOutcome, StoreError>
    where
        V: GraphSchemaValidator + ?Sized,
        R: GraphReducer + ?Sized,
    {
        Box::pin(self.append_root_graph_barrier_inner(append, intent, schemas, reducer))
            .await
            .map_err(|error| match error {
                StoreError::GraphFrameCompoundRequired => {
                    StoreError::RootGraphBarrierCompoundRequired
                }
                other => other,
            })
    }

    #[allow(clippy::too_many_lines)] // The whole fenced commit remains one explicit transaction protocol.
    async fn append_root_graph_barrier_inner<V, R>(
        &self,
        append: JournalAppend,
        intent: RootGraphBarrierIntent,
        schemas: &V,
        reducer: &R,
    ) -> Result<RootGraphBarrierCommitOutcome, StoreError>
    where
        V: GraphSchemaValidator + ?Sized,
        R: GraphReducer + ?Sized,
    {
        let data = &intent.data;
        let barrier = data.plan.barrier();
        let base = barrier.base_checkpoint();
        if append.worker_fence().is_none()
            || append.intent().tenant_id() != base.tenant_id()
            || append.intent().run_id() != base.run_id()
            || append.intent().event_id() != data.event_id
            || append.intent().payload() != &intent.payload()?
            || append.expectation().head().is_none()
        {
            return Err(StoreError::InvalidCheckpointBarrier);
        }
        // Recover before invoking user code, even after the Run resumed or
        // another owner advanced it. No current-fence dispatch is authorized.
        let mut tx = self
            .begin_repeatable_read("Root barrier existing preflight")
            .await?;
        let existing = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(base.tenant_id().as_str())
            .bind(*base.run_id().as_uuid())
            .bind(*data.event_id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|source| StoreError::database("Root barrier existing event", source))?;
        if existing.is_some() {
            let parent = checkpoint(
                &mut tx,
                base.tenant_id(),
                base.run_id(),
                base.checkpoint_id(),
            )
            .await?;
            Box::pin(verify_checkpoint(&mut tx, &parent)).await?;
            let record = Box::pin(local_record(&mut tx, &parent))
                .await?
                .ok_or_else(|| StoreError::corrupt("Root barrier existing whole record"))?;
            verify_retry(&record, &append, &intent)?;
            tx.commit()
                .await
                .map_err(|source| StoreError::database("Root barrier existing commit", source))?;
            return Ok(RootGraphBarrierCommitOutcome::Idempotent(record));
        }
        tx.commit().await.map_err(|source| {
            StoreError::database("Root barrier existing lookup commit", source)
        })?;
        self.verify_checkpoint_barrier_preflight(barrier).await?;
        let mut tx = self
            .begin_repeatable_read("Root barrier full plan preflight")
            .await?;
        let parent = checkpoint(
            &mut tx,
            base.tenant_id(),
            base.run_id(),
            base.checkpoint_id(),
        )
        .await?;
        if parent.head() != *base {
            return Err(StoreError::StaleCheckpointHead);
        }
        Box::pin(verify_checkpoint(&mut tx, &parent)).await?;
        let full_results = Box::pin(results(&mut tx, &parent)).await?;
        let graph_row = load_graph_definition_row(&mut tx, base.tenant_id(), base.graph())
            .await?
            .ok_or(StoreError::GraphReplayDependencyUnavailable)?;
        let graph = decode_graph_definition(graph_row)?;
        authenticate_intent_source(&mut tx, &data.wire).await?;
        let terminal_admission = if data.terminal_result.is_some() && data.wire.source.is_none() {
            Some(authenticated_admission(&mut tx, base).await?)
        } else {
            None
        };
        tx.commit()
            .await
            .map_err(|source| StoreError::database("Root barrier full preflight commit", source))?;
        let actual = graph
            .graph()
            .plan_barrier(
                &parent,
                &full_results,
                barrier.successor().checkpoint_id(),
                schemas,
                reducer,
            )
            .map_err(|error| map_graph_replay_plan_error(&error))?;
        if actual != data.plan {
            return Err(StoreError::InvalidCheckpointBarrier);
        }
        if let Some(result) = &data.terminal_result {
            if let Some(admission) = terminal_admission.as_ref() {
                let source = admission.admission().intent();
                result
                    .validate_for(
                        source.provenance(),
                        source.request(),
                        source.descriptor(),
                        source.budget(),
                    )
                    .map_err(|_| StoreError::InvalidLifecycleTransition)?;
            } else {
                let inputs = data
                    .wire
                    .terminal_validation
                    .as_deref()
                    .ok_or(StoreError::InvalidLifecycleTransition)?;
                result
                    .validate_for_terminal_inputs(data.before.provenance(), inputs)
                    .map_err(|_| StoreError::InvalidLifecycleTransition)?;
            }
            if let Some(inputs) = data.wire.terminal_validation.as_deref() {
                match catch_unwind(AssertUnwindSafe(|| {
                    schemas.validate(inputs.request().input_schema(), inputs.request().input())
                })) {
                    Ok(Ok(())) => (),
                    Err(_) | Ok(Err(GraphSchemaValidationError::Unavailable)) => {
                        return Err(StoreError::GraphReplayDependencyUnavailable);
                    }
                    Ok(Err(_)) => return Err(StoreError::InvalidLifecycleTransition),
                }
            }
            match catch_unwind(AssertUnwindSafe(|| {
                schemas.validate(result.output_schema(), result.output())
            })) {
                Ok(Ok(())) => (),
                Err(_) | Ok(Err(GraphSchemaValidationError::Unavailable)) => {
                    return Err(StoreError::GraphReplayDependencyUnavailable);
                }
                Ok(Err(_)) => return Err(StoreError::InvalidLifecycleTransition),
            }
        }
        let mut tx = self.begin_mutation("Root graph barrier append").await?;
        let stored =
            decode_run(fetch_locked_run_row(&mut tx, base.tenant_id(), base.run_id()).await?)?;
        let existing = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(base.tenant_id().as_str())
            .bind(*base.run_id().as_uuid())
            .bind(*data.event_id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|source| StoreError::database("Root barrier locked existing event", source))?;
        if existing.is_some() {
            let record = Box::pin(local_record(&mut tx, &parent))
                .await?
                .ok_or_else(|| StoreError::corrupt("Root barrier locked existing whole record"))?;
            verify_retry(&record, &append, &intent)?;
            tx.commit().await.map_err(|source| {
                StoreError::database("Root barrier locked existing commit", source)
            })?;
            return Ok(RootGraphBarrierCommitOutcome::Idempotent(record));
        }
        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if stored.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::RunNotRunnable);
        }
        if encode_lifecycle(stored.lifecycle())? != encode_lifecycle(&data.before)? {
            return Err(StoreError::StaleLifecycleRevision);
        }
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }
        let current =
            load_locked_current_checkpoint(&mut tx, &stored, base.tenant_id(), base.run_id())
                .await?
                .ok_or(StoreError::StaleCheckpointHead)?;
        if current.head() != *base {
            return Err(StoreError::StaleCheckpointHead);
        }
        verify_current_wait_set(&mut tx, &stored).await?;
        ensure_no_unsettled_tool_invocations(&mut tx, &current).await?;
        ensure_no_unsettled_model_invocations(&mut tx, &current).await?;
        if !load_barrier_consumption_rows(&mut tx, base)
            .await?
            .is_empty()
        {
            return Err(StoreError::CheckpointBarrierResultConflict);
        }
        validate_complete_barrier_result_heads(
            &load_locked_barrier_result_heads(&mut tx, base).await?,
            barrier.result_heads().as_slice(),
        )?;
        if query_scalar::<_,bool>("SELECT EXISTS (SELECT 1 FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2 AND active_namespace<>'')").bind(base.tenant_id().as_str()).bind(*base.run_id().as_uuid()).fetch_one(&mut *tx).await.map_err(|source| StoreError::database("Root active frame check",source))? { return Err(StoreError::GraphFrameConflict); }
        let source_journal = stored
            .journal_head()
            .cloned()
            .ok_or_else(|| StoreError::corrupt("Root source journal"))?;
        if barrier.result_heads().iter().any(|head| {
            head.journal_head().sequence() > source_journal.sequence()
                || head.journal_head().recorded_at() > source_journal.recorded_at()
        }) {
            return Err(StoreError::CheckpointBarrierResultConflict);
        }
        let now = database_now(&mut tx, "Root barrier database clock").await?;
        authorize_worker(
            &stored,
            append
                .worker_fence()
                .ok_or(StoreError::WrongAppendAuthority)?,
            now,
        )?;
        let event = JournalEvent::commit(append, now.max(source_journal.recorded_at()))
            .map_err(|error| map_event_commit_error(&error))?;
        let successor = Checkpoint::commit(barrier.successor().clone(), event.head())
            .map_err(|_| StoreError::InvalidCheckpointBarrier)?;
        let (after, waits, original_projection_digest) = materialize_lifecycle(
            &data.before,
            barrier,
            data.plan.disposition(),
            data.terminal_result.as_ref(),
            &event,
        )?;
        if original_projection_digest != data.wire.original_projection_digest {
            return Err(StoreError::ProjectionIntentConflict);
        }
        let prepared = if encode_lifecycle(&after)? == encode_lifecycle(&data.before)? {
            None
        } else {
            Some(prepared_projection(&after)?)
        };
        let mut wire = Box::new(Wire {
            version: data.wire.version,
            intent: Box::new(data.wire.clone()),
            intent_digest: data.digest,
            source_journal,
            checkpoint: successor.head(),
            lifecycle_after: Box::new(after),
            compound_digest: Digest::sha256([]),
        });
        wire.compound_digest = compound(&wire, &event)?;
        let record = StoredRootGraphBarrier {
            event,
            barrier: barrier.clone(),
            checkpoint: successor,
            disposition: data.plan.disposition().clone(),
            waits,
            wire,
        };
        insert_event_components(&mut tx, &record.event, record.digest()).await?;
        insert_checkpoint(&mut tx, &record.checkpoint, record.event.source()).await?;
        for wait in &record.waits {
            insert_wait_registration(&mut tx, wait).await?;
        }
        insert_barrier_consumptions(
            &mut tx,
            &record.barrier,
            &record.checkpoint,
            record.event.source(),
        )
        .await?;
        insert(&mut tx, &record).await?;
        update_checkpoint_pointer(&mut tx, &record.checkpoint, record.event.source()).await?;
        update_run_head(&mut tx, &record.event, prepared.as_ref()).await?;
        query("SET CONSTRAINTS ALL IMMEDIATE")
            .execute(&mut *tx)
            .await
            .map_err(|source| {
                StoreError::database("Root barrier complete deferred guards", source)
            })?;
        // Authenticate what the database actually stored after every deferred
        // guard, before returning a receipt or releasing the transaction lock.
        let durable = Box::pin(authenticated_edge(
            &mut tx,
            &parent,
            &record.checkpoint,
            &full_results,
        ))
        .await?
        .ok_or_else(|| StoreError::corrupt("Root commit durable whole bundle"))?;
        if durable.event != record.event
            || durable.barrier != record.barrier
            || durable.disposition != record.disposition
            || encode(&durable.wire)? != encode(&record.wire)?
        {
            return Err(StoreError::corrupt("Root commit durable whole input"));
        }
        let final_run =
            decode_run(fetch_locked_run_row(&mut tx, base.tenant_id(), base.run_id()).await?)?;
        verify_current_wait_set(&mut tx, &final_run).await?;
        if final_run.is_quarantined()
            || final_run.journal_head() != Some(&durable.event.head())
            || final_run.checkpoint().is_none_or(|pointer| {
                pointer.checkpoint_id() != durable.checkpoint.checkpoint_id()
                    || pointer.superstep() != durable.checkpoint.superstep()
                    || pointer.digest() != durable.checkpoint.digest()
            })
            || encode_lifecycle(final_run.lifecycle())?
                != encode_lifecycle(durable.committed_lifecycle())?
            || final_run.last_fencing_epoch() != Some(append_fence_epoch(&durable.event)?)
            || match durable.disposition {
                GraphBarrierDisposition::Continue => {
                    final_run.lease() != stored.lease()
                        || final_run.scheduler_ready_at() != stored.scheduler_ready_at()
                        || final_run.scheduler_not_before() != stored.scheduler_not_before()
                }
                _ => {
                    final_run.lease().is_some()
                        || final_run.scheduler_ready_at().is_some()
                        || final_run.scheduler_not_before().is_some()
                }
            }
        {
            return Err(StoreError::corrupt("Root commit final Run projection"));
        }
        let final_now = database_now(&mut tx, "Root barrier final fence clock").await?;
        authorize_worker(
            &stored,
            record
                .event
                .source()
                .worker_fence()
                .ok_or(StoreError::WrongAppendAuthority)?,
            final_now,
        )?;
        let within_deadline: bool = query_scalar("SELECT CASE WHEN $5::integer=1 THEN agent_deadline_at > clock_timestamp() WHEN $5::integer=2 THEN agent_deadline_at IS NULL ELSE false END FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2 AND journal_sequence=$3 AND journal_digest=$4")
            .bind(base.tenant_id().as_str()).bind(*base.run_id().as_uuid()).bind(i64::try_from(record.event.sequence().get()).map_err(|_| StoreError::JournalSequenceExhausted)?).bind(record.event.digest().as_bytes()).bind(i32::from(record.wire.version)).fetch_one(&mut *tx).await.map_err(|source| StoreError::database("Root barrier final deadline",source))?;
        if !within_deadline {
            return Err(StoreError::LeaseExpired);
        }
        tx.commit()
            .await
            .map_err(|source| StoreError::database("Root graph barrier commit", source))?;
        Ok(RootGraphBarrierCommitOutcome::Committed(durable))
    }
}

impl ClaimedRunRecovery<'_> {
    /// Checks the creating source for the exact observed Root checkpoint.
    /// Unavailable legitimate history is operational; owning corruption uses
    /// this session's existing exact-journal, live-fence quarantine boundary.
    /// No node start, callback, counter or reusable authority is produced.
    ///
    /// # Errors
    /// Returns source availability, stale snapshot/fence, quarantine or
    /// database failures while retaining the full owning reader contract.
    pub async fn validate_graph_creating_source(
        &self,
        base: &CheckpointHead,
        observed: &JournalHead,
    ) -> Result<(), StoreError> {
        if base.tenant_id() != self.fence.tenant_id()
            || base.run_id() != self.fence.run_id()
            || self.initial_run.journal_head() != Some(observed)
        {
            return Err(StoreError::StaleClaimedRunRecoveryObservation);
        }
        if self.initial_run.checkpoint().is_none_or(|pointer| {
            pointer.checkpoint_id() != base.checkpoint_id()
                || pointer.superstep() != base.superstep()
                || pointer.digest() != base.digest()
        }) {
            return Err(StoreError::StaleCheckpointHead);
        }
        self.store
            .with_corruption_quarantine(
                self.context.clone(),
                self.store
                    .validate_worker_graph_creating_source(&self.fence, base, observed),
            )
            .await
    }
}
fn append_fence_epoch(event: &JournalEvent) -> Result<FencingEpoch, StoreError> {
    event
        .source()
        .worker_fence()
        .map(RunFence::epoch)
        .ok_or(StoreError::WrongAppendAuthority)
}
fn verify_retry(
    record: &StoredRootGraphBarrier,
    append: &JournalAppend,
    intent: &RootGraphBarrierIntent,
) -> Result<(), StoreError> {
    if !record.event.matches_intent(append.intent()) {
        return Err(StoreError::EventIdConflict);
    }
    if append.expectation().head() != Some(record.source_journal())
        || record.wire.intent_digest != intent.data.digest
        || record.barrier != *intent.data.plan.barrier()
        || record.disposition != *intent.data.plan.disposition()
        || encode_lifecycle(record.original_lifecycle())?
            != encode_lifecycle(intent.original_lifecycle())?
        || record.committed_lifecycle().result() != intent.data.terminal_result.as_ref()
    {
        return Err(StoreError::ProjectionIntentConflict);
    }
    Ok(())
}

async fn verify_wait_terminals(
    tx: &mut Transaction<'_, Postgres>,
    event: &JournalEvent,
    waits: &[DurableWait],
) -> Result<(), StoreError> {
    for wait in waits {
        let row = query_as::<_, WaitRegistrationRow>(SELECT_WAIT_REGISTRATION_BY_ID.as_str())
            .bind(event.tenant_id().as_str())
            .bind(*event.run_id().as_uuid())
            .bind(durable_wait_identity(wait))
            .fetch_optional(&mut **tx)
            .await
            .map_err(|source| StoreError::database("Root historical wait terminal", source))?
            .ok_or_else(|| StoreError::corrupt("Root historical wait registration"))?;
        let outcome = if row.status == "abandoned" {
            verify_wait_abandonment_components(tx, &row, wait.clone())
                .await
                .map(|_| ())
        } else {
            match wait {
                DurableWait::Interrupt { request } => {
                    verify_interrupt_record_components(tx, &row, request.clone())
                        .await
                        .map(|_| ())
                }
                DurableWait::Timer { timer } => {
                    verify_timer_record_components(tx, &row, timer.clone())
                        .await
                        .map(|_| ())
                }
            }
        };
        outcome.map_err(|error| match error {
            StoreError::WaitAbandonmentNotFound
            | StoreError::WaitRegistrationKindMismatch
            | StoreError::InterruptResolutionCommitConflict
            | StoreError::TimerFiringCommitConflict
            | StoreError::WaitWasAbandoned => {
                StoreError::corrupt("Root historical wait terminal components")
            }
            other => other,
        })?;
    }
    Ok(())
}

pub(super) fn verify_event<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    event: &'a JournalEvent,
) -> stateknot_core::BoxFuture<'a, Result<(), StoreError>> {
    Box::pin(async move {
        if event.payload().kind().as_str() != EVENT_KIND {
            return Ok(());
        }
        let id: Uuid = query_scalar("SELECT base_checkpoint_id FROM stateknot.root_graph_barriers WHERE tenant_id=$1 AND run_id=$2 AND journal_sequence=$3")
            .bind(event.tenant_id().as_str()).bind(*event.run_id().as_uuid()).bind(i64::try_from(event.sequence().get()).map_err(|_| StoreError::corrupt("Root event sequence"))?).fetch_optional(&mut **tx).await.map_err(|source| StoreError::database("Root event owning bundle",source))?.ok_or_else(|| StoreError::corrupt("Root event missing whole bundle"))?;
        let base = Box::new(
            checkpoint(
                tx,
                event.tenant_id(),
                event.run_id(),
                CheckpointId::from_uuid(id)
                    .map_err(|_| StoreError::corrupt("Root event base identity"))?,
            )
            .await?,
        );
        Box::pin(verify_checkpoint(tx, &base)).await?;
        let record = Box::pin(local_record(tx, &base))
            .await?
            .ok_or_else(|| StoreError::corrupt("Root event missing whole bundle"))?;
        if record.event() != event {
            return Err(StoreError::corrupt("Root event owning bundle anchor"));
        }
        Ok(())
    })
}

pub(super) fn verify_run<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    stored: &'a StoredRun,
) -> stateknot_core::BoxFuture<'a, Result<(), StoreError>> {
    Box::pin(async move {
        let Some(pointer) = stored.checkpoint() else {
            return Ok(());
        };
        let owner = stored.lifecycle().provenance();
        let checkpoint = Box::new(
            checkpoint(
                tx,
                owner.tenant_id(),
                owner.run_id(),
                pointer.checkpoint_id(),
            )
            .await?,
        );
        if checkpoint.superstep() != pointer.superstep() || checkpoint.digest() != pointer.digest()
        {
            return Err(StoreError::corrupt("Root run checkpoint projection"));
        }
        Box::pin(verify_checkpoint(tx, &checkpoint)).await?;
        let journal = stored
            .journal_head()
            .ok_or_else(|| StoreError::corrupt("Root run journal projection"))?;
        if journal.sequence() < checkpoint.journal_head().sequence() {
            return Err(StoreError::corrupt("Root run journal before checkpoint"));
        }
        if journal == checkpoint.journal_head() {
            if let Some(parent) = checkpoint.parent() {
                let base = Box::new(
                    self::checkpoint(
                        tx,
                        owner.tenant_id(),
                        owner.run_id(),
                        parent.checkpoint_id(),
                    )
                    .await?,
                );
                if let Some(record) = Box::pin(local_record(tx, &base)).await? {
                    if encode_lifecycle(stored.lifecycle())?
                        != encode_lifecycle(record.committed_lifecycle())?
                    {
                        return Err(StoreError::corrupt("Root current lifecycle projection"));
                    }
                }
            }
        }
        Ok(())
    })
}

pub(super) async fn verify_mutation_source(
    tx: &mut Transaction<'_, Postgres>,
    stored: &StoredRun,
) -> Result<(), StoreError> {
    let Some(pointer) = stored.checkpoint() else {
        return Ok(());
    };
    let owner = stored.lifecycle().provenance();
    let checkpoint = checkpoint(
        tx,
        owner.tenant_id(),
        owner.run_id(),
        pointer.checkpoint_id(),
    )
    .await?;
    if checkpoint.superstep() != pointer.superstep() || checkpoint.digest() != pointer.digest() {
        return Err(StoreError::corrupt("Root mutation checkpoint projection"));
    }
    let Some(source) = stored.journal_head() else {
        if checkpoint.superstep() != Superstep::INITIAL
            || checkpoint.parent().is_some()
            || checkpoint.journal_head().sequence() != JournalSequence::FIRST
        {
            return Err(StoreError::corrupt("Root initial mutation checkpoint"));
        }
        return Box::pin(verify_checkpoint(tx, &checkpoint)).await;
    };
    if checkpoint.journal_head().sequence() > source.sequence() {
        if checkpoint.journal_head().sequence().get()
            != source
                .sequence()
                .get()
                .checked_add(1)
                .ok_or_else(|| StoreError::corrupt("Root mutation sequence"))?
        {
            return Err(StoreError::corrupt("Root mutation checkpoint sequence"));
        }
        let parent = checkpoint
            .parent()
            .ok_or_else(|| StoreError::corrupt("Root mutation parent"))?;
        let previous = self::checkpoint(
            tx,
            owner.tenant_id(),
            owner.run_id(),
            parent.checkpoint_id(),
        )
        .await?;
        if previous.head() != *parent {
            return Err(StoreError::corrupt("Root mutation parent"));
        }
        Box::pin(verify_checkpoint(tx, &previous)).await
    } else {
        Box::pin(verify_run(tx, stored)).await
    }
}

// Only the fresh initial-checkpoint writers reach this boundary. They pass
// the original complete checkpoint and projection intent, before the Run's
// journal pointer moves; a missing parent is not a historical exception.
pub(super) async fn verify_initial_mutation_source(
    tx: &mut Transaction<'_, Postgres>,
    stored: &StoredRun,
    expected: &Checkpoint,
    event: &JournalEvent,
    projection: Digest,
) -> Result<(), StoreError> {
    let owner = stored.lifecycle().provenance();
    let pointer = stored
        .checkpoint()
        .ok_or_else(|| StoreError::corrupt("Root initial mutation pointer"))?;
    let durable = checkpoint(
        tx,
        owner.tenant_id(),
        owner.run_id(),
        pointer.checkpoint_id(),
    )
    .await?;
    if durable != *expected
        || durable.tenant_id() != owner.tenant_id()
        || durable.run_id() != owner.run_id()
        || durable.superstep() != Superstep::INITIAL
        || durable.parent().is_some()
        || durable.superstep() != pointer.superstep()
        || durable.digest() != pointer.digest()
        || durable.journal_head() != &event.head()
    {
        return Err(StoreError::corrupt("Root initial mutation checkpoint"));
    }
    let (owning, actual_projection) = child_runs::anchored_event(
        tx,
        owner.tenant_id(),
        owner.run_id(),
        i64::try_from(event.sequence().get())
            .map_err(|_| StoreError::corrupt("Root initial mutation sequence"))?,
    )
    .await?;
    if owning != *event || actual_projection != Some(projection) {
        return Err(StoreError::corrupt("Root initial mutation event"));
    }
    if let Some(source) = stored.journal_head() {
        let (previous, _) = child_runs::anchored_event(
            tx,
            owner.tenant_id(),
            owner.run_id(),
            i64::try_from(source.sequence().get())
                .map_err(|_| StoreError::corrupt("Root initial source sequence"))?,
        )
        .await?;
        if previous.head() != *source
            || source.sequence().get().checked_add(1) != Some(owning.sequence().get())
            || owning.previous_digest() != Some(source.digest())
            || owning.recorded_at() < source.recorded_at()
        {
            return Err(StoreError::corrupt("Root initial mutation predecessor"));
        }
    } else if owning.sequence() != JournalSequence::FIRST || owning.previous_digest().is_some() {
        return Err(StoreError::corrupt("Root initial mutation first event"));
    }
    Box::pin(verify_checkpoint(tx, &durable)).await
}

pub(super) async fn verify_schema(pool: &PgPool) -> Result<(), StoreError> {
    let installed = query_scalar::<_, String>(include_str!("root_graph_barrier_catalog.sql"))
        .fetch_one(pool)
        .await
        .map_err(|source| StoreError::database("whole Root barrier catalog", source))?;
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("root_graph_barrier_catalog.json"))
            .map_err(|_| StoreError::IncompleteSchema)?;
    if serde_json::from_str::<serde_json::Value>(&installed)
        .map_err(|_| StoreError::IncompleteSchema)?
        != expected
    {
        return Err(StoreError::IncompleteSchema);
    }
    Ok(())
}
