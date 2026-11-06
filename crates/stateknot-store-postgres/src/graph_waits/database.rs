// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Local condition persistence; no historical walk or database authority is
//! inferred here. Whole writers/readers supply already authenticated originals.

use chrono::{DateTime, Utc};
use sqlx_core::{
    from_row::FromRow, query::query, query_as::query_as, row::Row as _, transaction::Transaction,
};
use sqlx_postgres::{PgRow, Postgres};
use stateknot_core::{Digest, NodeWait, Timestamp};
use thiserror::Error;

use super::{
    ConditionObservation, GraphWaitPreparationError, PreparedConditionRecord,
    PreparedGraphWaitCondition, PreparedGraphWaitInventory, PreparedGraphWaits,
};

#[derive(Debug, Error)]
pub(in super::super) enum GraphWaitDatabaseError {
    #[error("graph Wait database operation failed")]
    Database {
        #[source]
        source: sqlx_core::error::Error,
    },
    #[error("graph Wait identity is already owned by another original fact")]
    IdentityConflict,
    #[error("graph Wait original-condition inventory is missing or differs")]
    InventoryMismatch,
    #[error("graph Wait original-condition inputs could not be encoded")]
    Preparation {
        #[source]
        source: GraphWaitPreparationError,
    },
}

impl From<GraphWaitPreparationError> for GraphWaitDatabaseError {
    fn from(source: GraphWaitPreparationError) -> Self {
        Self::Preparation { source }
    }
}

struct ConditionRow {
    metadata: serde_json::Value,
    bytes: Option<Vec<u8>>,
}
impl<'r> FromRow<'r, PgRow> for ConditionRow {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx_core::error::Error> {
        Ok(Self {
            metadata: serde_json::from_str(&row.try_get::<String, _>("metadata")?)
                .map_err(|source| sqlx_core::error::Error::Decode(Box::new(source)))?,
            bytes: row.try_get("condition_bytes")?,
        })
    }
}

// Explicit UTC and hex renderings keep proof independent of connection GUCs.
// This is one bounded direct-edge load, never a fresh ancestor walk per row.
const LOAD: &str = r#"
SELECT jsonb_build_object(
 'tenant_id',c.tenant_id,'run_id',c.run_id,'wait_id',c.wait_id,'profile',c.profile,
 'wait_kind',c.wait_kind,'interrupt_kind',c.interrupt_kind,'timer_kind',c.timer_kind,
 'due_at',CASE WHEN c.due_at IS NULL THEN NULL ELSE to_char(c.due_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END,
 'expires_at',CASE WHEN c.expires_at IS NULL THEN NULL ELSE to_char(c.expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END,
 'intent_digest','sha256:'||encode(c.intent_digest,'hex'),
 'registration_digest',CASE WHEN c.registration_digest IS NULL THEN NULL ELSE 'sha256:'||encode(c.registration_digest,'hex') END,
 'journal_sequence',c.journal_sequence::text,'journal_event_id',c.journal_event_id,
 'journal_recorded_at',to_char(c.journal_recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'journal_digest','sha256:'||encode(c.journal_digest,'hex'),
 'graph_namespace',c.graph_namespace,'base_checkpoint_id',c.base_checkpoint_id,'base_superstep',c.base_superstep::text,'base_checkpoint_digest','sha256:'||encode(c.base_checkpoint_digest,'hex'),
 'successor_checkpoint_id',c.successor_checkpoint_id,'successor_superstep',c.successor_superstep::text,'successor_checkpoint_digest','sha256:'||encode(c.successor_checkpoint_digest,'hex'),
 'source_sequence',c.source_sequence::text,'source_event_id',c.source_event_id,
 'source_recorded_at',to_char(c.source_recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'source_digest','sha256:'||encode(c.source_digest,'hex'),
 'owner_node_id',c.owner_node_id,'owner_record_digest','sha256:'||encode(c.owner_record_digest,'hex'),'owner_intent_digest','sha256:'||encode(c.owner_intent_digest,'hex'),
 'result_wait_index',c.result_wait_index,'condition_ordinal',c.condition_ordinal,
 'specification_digest','sha256:'||encode(c.specification_digest,'hex'),'condition_checksum','sha256:'||encode(c.condition_checksum,'hex')
)::text AS metadata, c.condition_bytes
FROM stateknot.graph_wait_conditions c
WHERE c.tenant_id=$1 AND c.run_id=$2 AND c.journal_sequence=$3 AND c.profile<>'legacy_registration'
ORDER BY c.condition_ordinal LIMIT 65
"#;

const INSERT: &str = r"
INSERT INTO stateknot.graph_wait_conditions (
 tenant_id,wait_id,run_id,profile,wait_kind,interrupt_kind,timer_kind,due_at,expires_at,
 intent_digest,registration_digest,journal_sequence,journal_event_id,journal_recorded_at,journal_digest,
 graph_namespace,base_checkpoint_id,base_superstep,base_checkpoint_digest,
 successor_checkpoint_id,successor_superstep,successor_checkpoint_digest,
 source_sequence,source_event_id,source_recorded_at,source_digest,
 owner_node_id,owner_record_digest,owner_intent_digest,result_wait_index,condition_ordinal,specification_digest,condition_bytes
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33)
";

struct ConditionMetadata {
    wait_id: uuid::Uuid,
    kind: &'static str,
    interrupt_kind: Option<String>,
    timer_kind: Option<String>,
    due_at: Option<Timestamp>,
    expires_at: Option<Timestamp>,
    profile: &'static str,
    registration_digest: Option<Digest>,
}

impl ConditionMetadata {
    fn derive(condition: &PreparedGraphWaitCondition<'_>) -> Result<Self, GraphWaitDatabaseError> {
        let (wait_id, kind, interrupt_kind, timer_kind, due_at, expires_at) =
            match condition.specification {
                NodeWait::Timer {
                    timer_id,
                    timer_kind,
                    due_at,
                } => (
                    *timer_id.as_uuid(),
                    "timer",
                    None,
                    Some(kind_text(timer_kind)?),
                    Some(*due_at),
                    None,
                ),
                NodeWait::Interrupt {
                    interrupt_id,
                    interrupt_kind,
                    expires_at,
                    ..
                } => (
                    *interrupt_id.as_uuid(),
                    "interrupt",
                    Some(kind_text(interrupt_kind)?),
                    None,
                    None,
                    *expires_at,
                ),
            };
        let (profile, registration_digest) = match &condition.observation {
            ConditionObservation::Registered(wait) => (
                "graph_registered",
                Some(match wait.as_ref() {
                    stateknot_core::DurableWait::Timer { timer } => timer.digest(),
                    stateknot_core::DurableWait::Interrupt { request } => request.digest(),
                }),
            ),
            ConditionObservation::ElapsedTimer { .. } => ("graph_elapsed", None),
        };
        Ok(Self {
            wait_id,
            kind,
            interrupt_kind,
            timer_kind,
            due_at,
            expires_at,
            profile,
            registration_digest,
        })
    }
}

fn kind_text<T: serde::Serialize>(kind: &T) -> Result<String, GraphWaitDatabaseError> {
    match serde_json::to_value(kind).map_err(|_| GraphWaitPreparationError::Encoding)? {
        serde_json::Value::String(text) => Ok(text),
        _ => Err(GraphWaitPreparationError::Encoding.into()),
    }
}

fn database_time(time: Timestamp) -> Result<DateTime<Utc>, GraphWaitDatabaseError> {
    DateTime::from_timestamp_micros(time.unix_micros())
        .ok_or_else(|| GraphWaitPreparationError::Encoding.into())
}

fn sequence(value: u64) -> Result<i64, GraphWaitDatabaseError> {
    i64::try_from(value).map_err(|_| GraphWaitPreparationError::Encoding.into())
}

fn exact_original<'a, 'input: 'a>(
    prepared: &'a PreparedGraphWaits<'input>,
    inventory: &'a PreparedGraphWaitInventory,
) -> Result<
    impl Iterator<
        Item = (
            &'a PreparedGraphWaitCondition<'input>,
            &'a PreparedConditionRecord,
        ),
    > + 'a,
    GraphWaitDatabaseError,
> {
    if prepared.conditions.len() != inventory.records.len()
        || prepared.base != inventory.wire.base
        || prepared.namespace != inventory.wire.namespace
        || prepared.source != inventory.wire.source
        || prepared.event != inventory.wire.event
        || prepared.state() != inventory.wire.state
        || prepared
            .conditions
            .iter()
            .zip(&inventory.records)
            .any(|(original, stored)| {
                original.owner != stored.wire.owner
                    || original.result_wait_index != stored.wire.result_wait_index
            })
    {
        return Err(GraphWaitDatabaseError::InventoryMismatch);
    }
    for (ordinal, (original, stored)) in prepared
        .conditions
        .iter()
        .zip(&inventory.records)
        .enumerate()
    {
        let (intent, observation) = original.reference(&prepared.event)?;
        if stored.wire.ordinal as usize != ordinal
            || stored.wire.specification_digest
                != super::domain_digest(super::SPEC_DOMAIN, original.specification)?
            || stored.wire.registration_intent_digest != intent
            || stored.wire.observation != observation
            || stored.wire.source != prepared.source
            || stored.wire.successor != inventory.wire.successor
        {
            return Err(GraphWaitDatabaseError::InventoryMismatch);
        }
    }
    Ok(prepared.conditions.iter().zip(&inventory.records))
}

/// Inserts every complete original condition before future registrations. It
/// never commits, projects a lifecycle, fires a Timer or grants dispatch.
pub(in super::super) async fn insert(
    tx: &mut Transaction<'_, Postgres>,
    prepared: &PreparedGraphWaits<'_>,
    inventory: &PreparedGraphWaitInventory,
) -> Result<(), GraphWaitDatabaseError> {
    for (condition, record) in exact_original(prepared, inventory)? {
        let metadata = ConditionMetadata::derive(condition)?;
        let wire = &record.wire;
        let base = wire.owner.activation().base_checkpoint();
        let event = wire.successor.journal_head();
        let result = query(INSERT)
            .bind(base.tenant_id().as_str())
            .bind(metadata.wait_id)
            .bind(*base.run_id().as_uuid())
            .bind(metadata.profile)
            .bind(metadata.kind)
            .bind(metadata.interrupt_kind.as_deref())
            .bind(metadata.timer_kind.as_deref())
            .bind(metadata.due_at.map(database_time).transpose()?)
            .bind(metadata.expires_at.map(database_time).transpose()?)
            .bind(wire.registration_intent_digest.as_bytes())
            .bind(metadata.registration_digest.as_ref().map(Digest::as_bytes))
            .bind(sequence(event.sequence().get())?)
            .bind(*event.event_id().as_uuid())
            .bind(database_time(event.recorded_at())?)
            .bind(event.digest().as_bytes())
            .bind(wire.owner.activation().graph_namespace().as_str())
            .bind(*base.checkpoint_id().as_uuid())
            .bind(sequence(base.superstep().get())?)
            .bind(base.digest().as_bytes())
            .bind(*wire.successor.checkpoint_id().as_uuid())
            .bind(sequence(wire.successor.superstep().get())?)
            .bind(wire.successor.digest().as_bytes())
            .bind(sequence(wire.source.sequence().get())?)
            .bind(*wire.source.event_id().as_uuid())
            .bind(database_time(wire.source.recorded_at())?)
            .bind(wire.source.digest().as_bytes())
            .bind(wire.owner.activation().node_id().as_str())
            .bind(wire.owner.digest().as_bytes())
            .bind(wire.owner.intent_digest().as_bytes())
            .bind(i16::from(wire.result_wait_index))
            .bind(i16::from(wire.ordinal))
            .bind(wire.specification_digest.as_bytes())
            .bind(record.bytes())
            .execute(&mut **tx)
            .await;
        match result {
            Ok(result) if result.rows_affected() == 1 => (),
            Ok(_) => return Err(GraphWaitDatabaseError::InventoryMismatch),
            Err(source)
                if source
                    .as_database_error()
                    .and_then(sqlx_core::error::DatabaseError::code)
                    .as_deref()
                    == Some("23505") =>
            {
                return Err(GraphWaitDatabaseError::IdentityConflict);
            }
            Err(source) => return Err(GraphWaitDatabaseError::Database { source }),
        }
    }
    Ok(())
}

/// Verifies exact bytes plus every physical ownership/projection column with
/// one direct, capped load. The caller selects new-format qualification; valid
/// old history must not be sent here as though it promised new inventory.
pub(in super::super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    prepared: &PreparedGraphWaits<'_>,
    inventory: &PreparedGraphWaitInventory,
) -> Result<(), GraphWaitDatabaseError> {
    let rows = query_as::<_, ConditionRow>(LOAD)
        .bind(prepared.base.tenant_id().as_str())
        .bind(*prepared.base.run_id().as_uuid())
        .bind(sequence(prepared.event.sequence().get())?)
        .fetch_all(&mut **tx)
        .await
        .map_err(|source| GraphWaitDatabaseError::Database { source })?;
    if rows.len() != inventory.records.len() {
        return Err(GraphWaitDatabaseError::InventoryMismatch);
    }
    for ((condition, record), row) in exact_original(prepared, inventory)?.zip(rows) {
        let metadata = ConditionMetadata::derive(condition)?;
        let wire = &record.wire;
        let base = wire.owner.activation().base_checkpoint();
        let event = wire.successor.journal_head();
        let expected = serde_json::json!({
            "tenant_id":base.tenant_id(),"run_id":base.run_id(),"wait_id":metadata.wait_id.to_string(),"profile":metadata.profile,
            "wait_kind":metadata.kind,"interrupt_kind":metadata.interrupt_kind,"timer_kind":metadata.timer_kind,"due_at":metadata.due_at,"expires_at":metadata.expires_at,
            "intent_digest":wire.registration_intent_digest,"registration_digest":metadata.registration_digest,
            "journal_sequence":event.sequence(),"journal_event_id":event.event_id(),"journal_recorded_at":event.recorded_at(),"journal_digest":event.digest(),
            "graph_namespace":wire.owner.activation().graph_namespace(),"base_checkpoint_id":base.checkpoint_id(),"base_superstep":base.superstep(),"base_checkpoint_digest":base.digest(),
            "successor_checkpoint_id":wire.successor.checkpoint_id(),"successor_superstep":wire.successor.superstep(),"successor_checkpoint_digest":wire.successor.digest(),
            "source_sequence":wire.source.sequence(),"source_event_id":wire.source.event_id(),"source_recorded_at":wire.source.recorded_at(),"source_digest":wire.source.digest(),
            "owner_node_id":wire.owner.activation().node_id(),"owner_record_digest":wire.owner.digest(),"owner_intent_digest":wire.owner.intent_digest(),
            "result_wait_index":wire.result_wait_index,"condition_ordinal":wire.ordinal,"specification_digest":wire.specification_digest,"condition_checksum":record.checksum(),
        });
        if row.metadata != expected {
            return Err(GraphWaitDatabaseError::InventoryMismatch);
        }
        record.validate_bytes(
            row.bytes
                .as_deref()
                .ok_or(GraphWaitDatabaseError::InventoryMismatch)?,
        )?;
    }
    Ok(())
}
