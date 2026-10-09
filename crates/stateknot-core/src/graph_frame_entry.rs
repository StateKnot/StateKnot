// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Closed compound entry planning for experimental RFC-0022.
//!
//! These Rust-only values do not grant storage or dispatch authority. A store
//! must reload the admitted closure, actual active leaf and inherited limits,
//! repeat the live fence, and persist every component in one transaction.

use std::sync::LazyLock;

use schemars::{JsonSchema, Schema, SchemaGenerator, generate::SchemaSettings, json_schema};
use serde::Serialize;
use thiserror::Error;

use crate::{
    AttemptId, BoundedJson, Checkpoint, CheckpointId, CheckpointWrite, CompiledGraph, Digest,
    EventId, GraphFrameCall, GraphFrameCheckpoint, GraphFrameIdentity, GraphReference,
    JournalAppend, JournalEvent, JournalEventIntent, JournalExpectation, JournalHead,
    JournalPayload, JsonLimits, NodeAttemptStart, RunFence, SchemaReference, Version,
};

const INTENT_DOMAIN: &[u8] = b"stateknot-graph-frame-entry-intent-v1\0";
const RECORD_DOMAIN: &[u8] = b"stateknot-graph-frame-entry-record-v1\0";
const SCHEMA_ID: &str = "https://stknot.com/schemas/core/graph-frame-entry-event/1.0.0";
static EVENT_SCHEMA: LazyLock<Result<(SchemaReference, serde_json::Value), GraphFrameEntryError>> =
    LazyLock::new(generate_event_schema);

/// Prepared framework start and isolated checkpoint, bound before journal commit.
///
/// The payload contains compact identity/digest data and never copied child
/// state. Physical attempt/fence changes alter this entry intent; recovering an
/// existing frame must load its original entry, not allocate another one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphFrameEntryPlan {
    parent_graph: GraphReference,
    frame: GraphFrameIdentity,
    checkpoint: CheckpointWrite,
    attempt_id: AttemptId,
    fence: RunFence,
    intent_digest: Digest,
}

impl GraphFrameEntryPlan {
    /// Reserved kind for a complete compound entry, never an ordinary append.
    pub const EVENT_KIND: &'static str = "graph-frame-entered";

    /// Prepares a compound entry from the exact declared root caller.
    ///
    /// # Errors
    /// Rejects caller/target/ready-set drift, exhausted graph limits, crossed
    /// fences and worker/node attempt identity reuse.
    pub fn for_root(
        call: &GraphFrameCall,
        parent_graph: &CompiledGraph,
        parent: &Checkpoint,
        target: &CompiledGraph,
        checkpoint_id: CheckpointId,
        attempt_id: AttemptId,
        fence: RunFence,
    ) -> Result<Self, GraphFrameEntryError> {
        let (frame, checkpoint) = call
            .prepare_root_entry(parent_graph, parent, target, checkpoint_id)
            .map_err(|_| GraphFrameEntryError::InvalidCall)?;
        Self::new(
            parent_graph.reference(),
            frame,
            checkpoint,
            attempt_id,
            fence,
        )
    }

    /// Prepares entry while retaining an exact outer frame's scoped activation.
    ///
    /// # Errors
    /// Rejects the same call/fence/identity failures as root entry and an eighth
    /// frame before constructing an append or any durable component.
    pub fn for_frame(
        call: &GraphFrameCall,
        parent_graph: &CompiledGraph,
        parent: &GraphFrameCheckpoint,
        target: &CompiledGraph,
        checkpoint_id: CheckpointId,
        attempt_id: AttemptId,
        fence: RunFence,
    ) -> Result<Self, GraphFrameEntryError> {
        let (frame, checkpoint) = call
            .prepare_frame_entry(parent_graph, parent, target, checkpoint_id)
            .map_err(|_| GraphFrameEntryError::InvalidCall)?;
        Self::new(
            parent_graph.reference(),
            frame,
            checkpoint,
            attempt_id,
            fence,
        )
    }

    fn new(
        parent_graph: GraphReference,
        frame: GraphFrameIdentity,
        checkpoint: CheckpointWrite,
        attempt_id: AttemptId,
        fence: RunFence,
    ) -> Result<Self, GraphFrameEntryError> {
        if frame.origin().tenant_id() != fence.tenant_id()
            || frame.origin().run_id() != fence.run_id()
            || attempt_id == fence.attempt_id()
        {
            return Err(GraphFrameEntryError::InvalidFence);
        }
        let intent_digest = domain_digest(
            INTENT_DOMAIN,
            &EntryIntentWire {
                version: 1,
                parent_graph: &parent_graph,
                frame: &frame,
                checkpoint_intent_digest: checkpoint.intent_digest(),
                caller_attempt_id: attempt_id,
                fence: &fence,
            },
        )?;
        Ok(Self {
            parent_graph,
            frame,
            checkpoint,
            attempt_id,
            fence,
            intent_digest,
        })
    }

    fn intent_wire(&self) -> EntryIntentWire<'_> {
        EntryIntentWire {
            version: 1,
            parent_graph: &self.parent_graph,
            frame: &self.frame,
            checkpoint_intent_digest: self.checkpoint.intent_digest(),
            caller_attempt_id: self.attempt_id,
            fence: &self.fence,
        }
    }

    /// Returns the complete pinned parent definition.
    #[must_use]
    pub const fn parent_graph(&self) -> &GraphReference {
        &self.parent_graph
    }
    /// Returns the stable child identity and exact caller activation.
    #[must_use]
    pub const fn frame(&self) -> &GraphFrameIdentity {
        &self.frame
    }
    /// Returns the isolated initial checkpoint intent, including its copied state.
    #[must_use]
    pub const fn checkpoint(&self) -> &CheckpointWrite {
        &self.checkpoint
    }
    /// Returns the distinct framework node-attempt identity.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }
    /// Returns the worker fence bound into this physical entry intent.
    #[must_use]
    pub const fn fence(&self) -> &RunFence {
        &self.fence
    }
    /// Returns the pre-event domain-separated compound intent digest.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }

    /// Returns the exact versioned output schema of this compact event profile.
    ///
    /// Register this local document before dispatch; its identity is not a URL
    /// to fetch. The digest pins RFC 8785 document bytes.
    ///
    /// # Errors
    /// Returns an internal encoding error if the release cannot generate its
    /// shipped typed output schema.
    pub fn event_schema() -> Result<(SchemaReference, serde_json::Value), GraphFrameEntryError> {
        (*EVENT_SCHEMA).clone()
    }

    /// Produces the only payload accepted when materializing this exact entry.
    ///
    /// # Errors
    /// Returns an internal encoding or bounded-payload error.
    pub fn payload(&self) -> Result<JournalPayload, GraphFrameEntryError> {
        let intent = self.intent_wire();
        let wire = EntryEventWire {
            version: intent.version,
            parent_graph: intent.parent_graph,
            frame: intent.frame,
            checkpoint_intent_digest: intent.checkpoint_intent_digest,
            caller_attempt_id: intent.caller_attempt_id,
            fence: intent.fence,
            intent_digest: self.intent_digest,
        };
        let bytes =
            serde_json_canonicalizer::to_vec(&wire).map_err(|_| GraphFrameEntryError::Encoding)?;
        let data = BoundedJson::from_slice_with_limits(&bytes, JsonLimits::DEFAULT)
            .map_err(|_| GraphFrameEntryError::Encoding)?;
        let schema = EVENT_SCHEMA.as_ref().map_err(Clone::clone)?.0.clone();
        JournalPayload::new(
            schema,
            Self::EVENT_KIND
                .parse()
                .map_err(|_| GraphFrameEntryError::Encoding)?,
            data,
        )
        .map_err(|_| GraphFrameEntryError::Encoding)
    }

    /// Builds a worker append against an exact observed Run journal head.
    ///
    /// The observation may include unrelated completed sibling/lease facts
    /// after the caller's checkpoint. It cannot precede or cross that base.
    /// The store still repeats this expectation under the locked Run row.
    ///
    /// # Errors
    /// Rejects a crossed, older or substituted base observation.
    pub fn append(
        &self,
        event_id: EventId,
        observed: JournalHead,
    ) -> Result<JournalAppend, GraphFrameEntryError> {
        self.validate_observation(&observed)?;
        let intent = JournalEventIntent::worker(
            self.frame.origin().tenant_id().clone(),
            self.frame.origin().run_id(),
            event_id,
            self.fence.clone(),
            self.payload()?,
        )
        .map_err(|_| GraphFrameEntryError::Encoding)?;
        JournalAppend::new(JournalExpectation::exact(observed), intent)
            .map_err(|_| GraphFrameEntryError::InvalidObservation)
    }

    fn validate_observation(&self, observed: &JournalHead) -> Result<(), GraphFrameEntryError> {
        let base = self.frame.origin().base_checkpoint().journal_head();
        if observed.tenant_id() != base.tenant_id()
            || observed.run_id() != base.run_id()
            || observed.sequence() < base.sequence()
            || observed.recorded_at() < base.recorded_at()
            || (observed.sequence() == base.sequence() && observed != base)
        {
            return Err(GraphFrameEntryError::InvalidObservation);
        }
        Ok(())
    }

    /// Materializes every component against one exact committed event.
    ///
    /// The compound projection digest binds the intent, full event, framework
    /// start and scoped checkpoint. It is neither component's legacy digest.
    /// This is data validation; no database clock, lease, policy or active-leaf
    /// authority is established by constructing these values.
    ///
    /// # Errors
    /// Rejects payload/schema/source/scope/ordering substitution and invalid
    /// component construction.
    pub fn materialize(
        &self,
        observed: &JournalHead,
        event: &JournalEvent,
    ) -> Result<GraphFrameEntry, GraphFrameEntryError> {
        let append = self.append(event.event_id(), observed.clone())?;
        if !event.matches_intent(append.intent())
            || observed.sequence().checked_next() != Some(event.sequence())
            || event.previous_digest() != Some(observed.digest())
            || event.recorded_at() < observed.recorded_at()
        {
            return Err(GraphFrameEntryError::EventMismatch);
        }
        let start = NodeAttemptStart::new(
            self.frame.origin().clone(),
            self.attempt_id,
            self.fence.clone(),
            event.head(),
        )
        .map_err(|_| GraphFrameEntryError::ComponentMismatch)?;
        let checkpoint = Checkpoint::commit(self.checkpoint.clone(), event.head())
            .map_err(|_| GraphFrameEntryError::ComponentMismatch)?;
        let checkpoint = GraphFrameCheckpoint::new(self.frame.clone(), checkpoint)
            .map_err(|_| GraphFrameEntryError::ComponentMismatch)?;
        let digest = domain_digest(
            RECORD_DOMAIN,
            &EntryRecordWire {
                intent_digest: self.intent_digest,
                event: &event.head(),
                caller_start_digest: start.digest(),
                checkpoint: &checkpoint.head(),
            },
        )?;
        Ok(GraphFrameEntry {
            intent_digest: self.intent_digest,
            start,
            checkpoint,
            digest,
        })
    }

    /// Reload-verifies the complete compound and every supplied component.
    ///
    /// A store must call this when recognizing the entry event from any one
    /// component's anchor; accepting only a matching component digest would
    /// leave the other facts unauthenticated.
    ///
    /// # Errors
    /// Rejects any event, start, frame/checkpoint or projection substitution.
    pub fn verify_committed(
        &self,
        observed: &JournalHead,
        event: &JournalEvent,
        start: &NodeAttemptStart,
        checkpoint: &GraphFrameCheckpoint,
        projection_digest: Digest,
    ) -> Result<GraphFrameEntry, GraphFrameEntryError> {
        let record = self.materialize(observed, event)?;
        if record.start() != start
            || record.checkpoint() != checkpoint
            || record.digest() != projection_digest
        {
            return Err(GraphFrameEntryError::ComponentMismatch);
        }
        Ok(record)
    }
}

/// Complete integrity-verified entry components, awaiting an atomic store commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphFrameEntry {
    intent_digest: Digest,
    start: NodeAttemptStart,
    checkpoint: GraphFrameCheckpoint,
    digest: Digest,
}
impl GraphFrameEntry {
    /// Returns the pre-event entry intent checksum.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }
    /// Returns the framework-owned physical caller start.
    #[must_use]
    pub const fn start(&self) -> &NodeAttemptStart {
        &self.start
    }
    /// Returns the initial isolated checkpoint with the same event anchor.
    #[must_use]
    pub const fn checkpoint(&self) -> &GraphFrameCheckpoint {
        &self.checkpoint
    }
    /// Returns the single compound projection checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

#[derive(Serialize)]
struct EntryIntentWire<'a> {
    version: u8,
    parent_graph: &'a GraphReference,
    frame: &'a GraphFrameIdentity,
    checkpoint_intent_digest: Digest,
    caller_attempt_id: AttemptId,
    fence: &'a RunFence,
}
#[derive(JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct EntryEventWire<'a> {
    #[schemars(range(min = 1, max = 1))]
    version: u8,
    parent_graph: &'a GraphReference,
    #[schemars(schema_with = "entry_frame_schema")]
    frame: &'a GraphFrameIdentity,
    checkpoint_intent_digest: Digest,
    caller_attempt_id: AttemptId,
    fence: &'a RunFence,
    intent_digest: Digest,
}
#[derive(Serialize)]
struct EntryRecordWire<'a> {
    intent_digest: Digest,
    event: &'a JournalHead,
    caller_start_digest: Digest,
    checkpoint: &'a crate::GraphFrameCheckpointHead,
}
fn domain_digest(domain: &[u8], value: &impl Serialize) -> Result<Digest, GraphFrameEntryError> {
    let canonical =
        serde_json_canonicalizer::to_vec(value).map_err(|_| GraphFrameEntryError::Encoding)?;
    let mut preimage = Vec::with_capacity(domain.len() + canonical.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn entry_frame_schema(generator: &mut SchemaGenerator) -> Schema {
    let identity = generator.subschema_for::<GraphFrameIdentity>();
    json_schema!({
        "allOf": [identity, {
            "properties": {"namespace": {
                "type": "string", "minLength": 64, "maxLength": 454,
                "pattern": "^[0-9a-f]{64}(/[0-9a-f]{64}){0,6}$"
            }}
        }]
    })
}

fn generate_event_schema() -> Result<(SchemaReference, serde_json::Value), GraphFrameEntryError> {
    let mut document = serde_json::to_value(
        SchemaSettings::draft2020_12()
            .for_serialize()
            .into_generator()
            .into_root_schema_for::<EntryEventWire<'_>>(),
    )
    .map_err(|_| GraphFrameEntryError::Encoding)?;
    document["$id"] = serde_json::Value::String(SCHEMA_ID.into());
    let bytes =
        serde_json_canonicalizer::to_vec(&document).map_err(|_| GraphFrameEntryError::Encoding)?;
    let reference = SchemaReference::new(
        SCHEMA_ID
            .parse()
            .map_err(|_| GraphFrameEntryError::Encoding)?,
        Version::new(1, 0, 0),
        Digest::sha256(bytes),
    );
    Ok((reference, document))
}

/// Payload-redacted compound entry planning or integrity failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphFrameEntryError {
    /// The actual caller, target or local readiness failed call preparation.
    #[error("graph frame entry call is invalid")]
    InvalidCall,
    /// The fence is crossed or reuses its worker attempt as a node attempt.
    #[error("graph frame entry fence is invalid")]
    InvalidFence,
    /// The Run journal observation crosses or precedes the caller base.
    #[error("graph frame entry journal observation is invalid")]
    InvalidObservation,
    /// The event is not the exact next record for this closed entry payload.
    #[error("graph frame entry event does not match its intent")]
    EventMismatch,
    /// The complete entry does not bind every exact supplied component.
    #[error("graph frame entry component does not match its compound record")]
    ComponentMismatch,
    /// An internal bounded canonical/schema construction failed.
    #[error("graph frame entry encoding failed")]
    Encoding,
}
