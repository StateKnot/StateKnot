// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Checked accounting state transitions; not a database ledger or spawn API.

use std::collections::BTreeSet;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    AgentAdmission, AgentResultProvenance, BudgetNarrowingError, BudgetRemaining, BudgetUsage,
    BudgetUsageError, ChildRunAdmissionIntent, ChildRunKey, ChildRunPolicyError, CompiledGraph,
    CumulativeBudgetReservation, CumulativeBudgetReservationError, Digest, ExecutionCount,
    GraphReference, JournalHead, ResolvedBudget, RunLifecycle, RunStatus, Timestamp,
};

const DOMAIN: &[u8] = b"stateknot.child-run-budget-account.v1\0";

/// Exact, fully priced terminal contribution of one immediate child subtree.
///
/// Construction binds trusted admission, lifecycle and terminal journal metadata.
/// The adapter must independently verify their durable provenance, complete
/// accounting (including grandchildren exactly once), and all child joins.
/// A checksum is not proof that any external work finished or was paid for.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunBudgetSettlement {
    child: AgentResultProvenance,
    intent_digest: Digest,
    admission_digest: Digest,
    admitted_at: Timestamp,
    terminal: JournalHead,
    #[schemars(schema_with = "terminal_status_schema")]
    status: RunStatus,
    outcome_digest: Digest,
    #[schemars(schema_with = "priced_usage_schema")]
    usage: BudgetUsage,
}

fn terminal_status_schema(_: &mut SchemaGenerator) -> Schema {
    json_schema!({"type":"string","enum":["succeeded","failed","cancelled"]})
}

fn priced_usage_schema(generator: &mut SchemaGenerator) -> Schema {
    json_schema!({"allOf":[generator.subschema_for::<BudgetUsage>(),
        {"properties":{"unpriced_cost_events":{"const":"0"}}}]})
}

impl ChildRunBudgetSettlement {
    /// Prepares a settlement from independently verified terminal evidence.
    ///
    /// Failed and cancelled children still consume their complete known usage.
    /// Deadline expiry and a known budget overrun do not erase actual charges.
    /// Unpriced usage cannot settle a reservation as if its cost were zero.
    pub fn new(
        admission: &AgentAdmission,
        lifecycle: &RunLifecycle,
        terminal: JournalHead,
    ) -> Result<Self, ChildRunBudgetError> {
        if lifecycle.provenance() != admission.intent().provenance()
            || lifecycle.admitted_at() != admission.admitted_at()
            || lifecycle.changed_at() > terminal.recorded_at()
        {
            return Err(ChildRunBudgetError::TerminalMismatch);
        }
        let usage = lifecycle
            .terminal_usage()
            .ok_or(ChildRunBudgetError::NotTerminal)?;
        let mut preimage = b"stateknot.child-run-budget-terminal.v1\0".to_vec();
        preimage.extend(
            crate::agent_admission::canonical_bytes(lifecycle)
                .map_err(|_| ChildRunBudgetError::Encoding)?,
        );
        let outcome_digest = Digest::sha256(preimage);
        let value = Self {
            child: admission.intent().provenance().clone(),
            intent_digest: admission.intent().intent_digest(),
            admission_digest: admission.digest(),
            admitted_at: admission.admitted_at(),
            terminal,
            status: lifecycle.status(),
            outcome_digest,
            usage: usage.clone(),
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), ChildRunBudgetError> {
        if !self.status.is_terminal() {
            return Err(ChildRunBudgetError::NotTerminal);
        }
        if self.child.tenant_id() != self.terminal.tenant_id()
            || self.child.run_id() != self.terminal.run_id()
            || self.terminal.recorded_at() < self.admitted_at
            || self.terminal.sequence().get() <= 1
        {
            return Err(ChildRunBudgetError::TerminalMismatch);
        }
        if self.usage.unpriced_cost_events() != ExecutionCount::ZERO {
            return Err(ChildRunBudgetError::UnpricedSettlement);
        }
        Ok(())
    }

    /// Returns the independently bound terminal journal observation.
    #[must_use]
    pub const fn terminal(&self) -> &JournalHead {
        &self.terminal
    }

    /// Returns complete child subtree usage, before removing child-local peaks.
    #[must_use]
    pub const fn usage(&self) -> &BudgetUsage {
        &self.usage
    }

    /// Returns the checksum of the complete terminal lifecycle snapshot.
    #[must_use]
    pub const fn outcome_digest(&self) -> Digest {
        self.outcome_digest
    }

    /// Returns the exact child admission commit fingerprint.
    #[must_use]
    pub const fn admission_digest(&self) -> Digest {
        self.admission_digest
    }
}

impl<'de> Deserialize<'de> for ChildRunBudgetSettlement {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            child: AgentResultProvenance,
            intent_digest: Digest,
            admission_digest: Digest,
            admitted_at: Timestamp,
            terminal: JournalHead,
            status: RunStatus,
            outcome_digest: Digest,
            usage: BudgetUsage,
        }
        let wire = Wire::deserialize(deserializer)?;
        let value = Self {
            child: wire.child,
            intent_digest: wire.intent_digest,
            admission_digest: wire.admission_digest,
            admitted_at: wire.admitted_at,
            terminal: wire.terminal,
            status: wire.status,
            outcome_digest: wire.outcome_digest,
            usage: wire.usage,
        };
        value.validate().map_err(de::Error::custom)?;
        Ok(value)
    }
}

/// A lifetime child slot record. Settled entries are retained for retry identity.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunBudgetEntry {
    key: ChildRunKey,
    spawn_digest: Digest,
    child: AgentResultProvenance,
    intent_digest: Digest,
    reservation: CumulativeBudgetReservation,
    settlement: Option<ChildRunBudgetSettlement>,
}

impl ChildRunBudgetEntry {
    fn validate(&self) -> Result<(), ChildRunBudgetError> {
        if self.key.tenant_id() != self.child.tenant_id()
            || self.key.parent_run_id() == self.child.run_id()
        {
            return Err(ChildRunBudgetError::ParentMismatch);
        }
        if let Some(settlement) = &self.settlement {
            settlement.validate()?;
            if settlement.child != self.child || settlement.intent_digest != self.intent_digest {
                return Err(ChildRunBudgetError::TerminalMismatch);
            }
        }
        Ok(())
    }
    /// Returns logical ownership, independent of physical attempts and fences.
    #[must_use]
    pub const fn key(&self) -> &ChildRunKey {
        &self.key
    }

    /// Returns the first accepted child identities, including on an exact retry.
    #[must_use]
    pub const fn child(&self) -> &AgentResultProvenance {
        &self.child
    }

    /// Returns the complete immutable spawn fingerprint used for retry comparison.
    #[must_use]
    pub const fn spawn_digest(&self) -> Digest {
        self.spawn_digest
    }

    /// Returns the first-selected child admission-intent fingerprint.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }

    /// Returns the original finite reservation, retained after settlement.
    #[must_use]
    pub const fn reservation(&self) -> &CumulativeBudgetReservation {
        &self.reservation
    }

    /// Returns terminal accounting only after one exact settlement.
    #[must_use]
    pub const fn settlement(&self) -> Option<&ChildRunBudgetSettlement> {
        self.settlement.as_ref()
    }
}

impl<'de> Deserialize<'de> for ChildRunBudgetEntry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            key: ChildRunKey,
            spawn_digest: Digest,
            child: AgentResultProvenance,
            intent_digest: Digest,
            reservation: CumulativeBudgetReservation,
            settlement: Option<ChildRunBudgetSettlement>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let value = Self {
            key: wire.key,
            spawn_digest: wire.spawn_digest,
            child: wire.child,
            intent_digest: wire.intent_digest,
            reservation: wire.reservation,
            settlement: wire.settlement,
        };
        value.validate().map_err(de::Error::custom)?;
        Ok(value)
    }
}

/// Bounded, integrity-bound accounting state for one parent admission.
///
/// Separates parent direct observations, settled immediate-child subtree charges,
/// and outstanding ceilings. Mutations return a new snapshot; failures preserve
/// the old value. Retained keys make spawn/settlement retries idempotent even
/// after closure or deadline expiry. No child record can be removed or refunded.
///
/// This is a pure contract, NOT a durable account, lock, authority grant, child
/// ownership record or scheduler. A storage adapter must verify all evidence and
/// persist the old-to-new digest transition atomically with parent direct work,
/// child ownership/admission/settlement and lifecycle guards. In particular,
/// storing two independently computed snapshots without compare-and-swap loses
/// reservations. Existing root admission is not an implementation of this API.
///
/// ```
/// use stateknot_core::{AgentAdmission, BudgetUsage, ChildRunAdmissionIntent,
///     ChildRunBudgetAccount, ChildRunBudgetError, CompiledGraph, JournalHead, Timestamp};
///
/// fn prepare_account_transition(
///     parent: &AgentAdmission,
///     graph: &CompiledGraph,
///     direct_head: JournalHead,
///     direct_usage: BudgetUsage,
///     child: &ChildRunAdmissionIntent,
///     now: Timestamp,
/// ) -> Result<ChildRunBudgetAccount, ChildRunBudgetError> {
///     let account = ChildRunBudgetAccount::new(parent, graph, direct_head, direct_usage)?;
///     account.reserve(child, graph, now)
/// }
/// ```
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunBudgetAccount {
    #[schemars(range(min = 1, max = 1))]
    version: u8,
    parent: AgentResultProvenance,
    parent_admission_digest: Digest,
    parent_admitted_at: Timestamp,
    graph: GraphReference,
    budget: ResolvedBudget,
    #[schemars(range(min = 1, max = 256))]
    maximum_children: u16,
    direct_head: JournalHead,
    direct_usage: BudgetUsage,
    #[schemars(length(max = 256))]
    children: Vec<ChildRunBudgetEntry>,
    digest: Digest,
}

impl ChildRunBudgetAccount {
    /// Hard lifetime bound; settled entries still count toward the bound.
    pub const MAX_CHILDREN: usize = 256;
    /// Hard serialized snapshot size. Adapters must also cap input before decode.
    pub const MAX_SNAPSHOT_BYTES: usize = 8 * 1024 * 1024;

    /// Starts from an authoritative parent direct-usage observation, not zero
    /// by assumption. Parent graph declarations and immutable admission must agree.
    pub fn new(
        parent: &AgentAdmission,
        graph: &CompiledGraph,
        direct_head: JournalHead,
        direct_usage: BudgetUsage,
    ) -> Result<Self, ChildRunBudgetError> {
        let policy = graph
            .child_runs()
            .ok_or(ChildRunBudgetError::ParentMismatch)?;
        if direct_head.recorded_at() < parent.admitted_at() {
            return Err(ChildRunBudgetError::DirectObservationMismatch);
        }
        let value = Self {
            version: 1,
            parent: parent.intent().provenance().clone(),
            parent_admission_digest: parent.digest(),
            parent_admitted_at: parent.admitted_at(),
            graph: graph.reference(),
            budget: parent.intent().budget().clone(),
            maximum_children: policy.limits().maximum_children_per_run(),
            direct_head,
            direct_usage,
            children: Vec::new(),
            digest: Digest::sha256([]),
        }
        .seal()?;
        value.validate_for(parent, graph)?;
        Ok(value)
    }

    /// Rebinds restored accounting to authoritative immutable admission/graph pins.
    /// Does not establish that direct/child observations are current or authentic.
    pub fn validate_for(
        &self,
        parent: &AgentAdmission,
        graph: &CompiledGraph,
    ) -> Result<(), ChildRunBudgetError> {
        if self.parent != *parent.intent().provenance()
            || self.parent_admission_digest != parent.digest()
            || self.parent_admitted_at != parent.admitted_at()
            || self.graph != *parent.intent().graph()
            || self.graph != graph.reference()
            || self.budget != *parent.intent().budget()
            || graph
                .child_runs()
                .map(|policy| policy.limits().maximum_children_per_run())
                != Some(self.maximum_children)
            || self.direct_head.recorded_at() < parent.admitted_at()
        {
            return Err(ChildRunBudgetError::ParentMismatch);
        }
        Ok(())
    }

    /// Adds one finite reservation, or returns the unchanged original on a
    /// matching ownership-key/spawn-digest retry. Replacement candidate Run IDs
    /// do not replace the first accepted identities. Rechecks the frozen node/slot
    /// declaration. Readiness, fences, authority, and live topology are additional
    /// adapter responsibilities.
    pub fn reserve(
        &self,
        intent: &ChildRunAdmissionIntent,
        parent_graph: &CompiledGraph,
        observed_at: Timestamp,
    ) -> Result<Self, ChildRunBudgetError> {
        if intent.parent_admission_digest() != self.parent_admission_digest
            || intent.key().tenant_id() != self.parent.tenant_id()
            || intent.key().parent_run_id() != self.parent.run_id()
            || intent.key().parent().base_checkpoint().graph() != &self.graph
            || parent_graph.reference() != self.graph
        {
            return Err(ChildRunBudgetError::ParentMismatch);
        }
        intent.validate_declaration(parent_graph)?;
        intent.child().budget().validate_narrowing(&self.budget)?;
        if let Some(existing) = self.entry(intent.key()) {
            return if existing.spawn_digest == intent.spawn_digest() {
                Ok(self.clone())
            } else {
                Err(ChildRunBudgetError::SpawnConflict)
            };
        }
        if self.children.len() >= usize::from(self.maximum_children) {
            return Err(ChildRunBudgetError::TooManyChildren);
        }
        let mut next = self.clone();
        next.children.push(ChildRunBudgetEntry {
            key: intent.key().clone(),
            spawn_digest: intent.spawn_digest(),
            child: intent.child().provenance().clone(),
            intent_digest: intent.child().intent_digest(),
            reservation: CumulativeBudgetReservation::from_budget(intent.child().budget())?,
            settlement: None,
        });
        next.remaining(observed_at)?;
        next.seal()
    }

    /// Replaces an outstanding reservation with exact terminal usage once.
    /// A known overrun is retained, never clipped or dropped; `remaining` then
    /// blocks further admission. Unknown pricing keeps the old reservation.
    /// This does not release child ownership or satisfy a lifecycle join.
    pub fn settle(
        &self,
        key: &ChildRunKey,
        settlement: ChildRunBudgetSettlement,
    ) -> Result<Self, ChildRunBudgetError> {
        settlement.validate()?;
        let index = self
            .children
            .iter()
            .position(|entry| &entry.key == key)
            .ok_or(ChildRunBudgetError::ChildNotFound)?;
        let entry = &self.children[index];
        if entry.child != settlement.child || entry.intent_digest != settlement.intent_digest {
            return Err(ChildRunBudgetError::TerminalMismatch);
        }
        if settlement.admitted_at < self.parent_admitted_at {
            return Err(ChildRunBudgetError::TerminalMismatch);
        }
        if let Some(existing) = &entry.settlement {
            return if existing == &settlement {
                Ok(self.clone())
            } else {
                Err(ChildRunBudgetError::SettlementConflict)
            };
        }
        let mut next = self.clone();
        next.children[index].settlement = Some(settlement);
        next.seal()
    }

    /// Records an absolute, non-regressing DIRECT usage observation at a later
    /// journal head, not a delta and not a total that already contains children.
    /// Exact replay is a no-op; changed evidence at the same head is a conflict.
    /// Actual over-budget/unpriced usage remains visible and blocks admission.
    pub fn observe_direct(
        &self,
        head: JournalHead,
        usage: BudgetUsage,
    ) -> Result<Self, ChildRunBudgetError> {
        if head == self.direct_head && usage == self.direct_usage {
            return Ok(self.clone());
        }
        if head.tenant_id() != self.parent.tenant_id()
            || head.run_id() != self.parent.run_id()
            || head.sequence() <= self.direct_head.sequence()
            || head.recorded_at() < self.direct_head.recorded_at()
        {
            return Err(ChildRunBudgetError::DirectObservationMismatch);
        }
        usage.validate_monotonic_after(&self.direct_usage)?;
        let mut next = self.clone();
        next.direct_head = head;
        next.direct_usage = usage;
        next.seal()
    }

    /// Returns additive settled usage of immediate child subtrees, exactly once.
    /// Child-local topology peaks do not leak into the parent's peak observations.
    pub fn delegated_usage(&self) -> Result<BudgetUsage, ChildRunBudgetError> {
        let mut total = BudgetUsage::zero();
        for entry in &self.children {
            if let Some(settlement) = &entry.settlement {
                total = total.checked_accumulate(&settlement.usage.cumulative_only())?;
            }
        }
        Ok(total)
    }

    /// Returns direct plus settled delegated usage, excluding all reservations.
    pub fn accounted_usage(&self) -> Result<BudgetUsage, ChildRunBudgetError> {
        Ok(self
            .direct_usage
            .checked_accumulate(&self.delegated_usage()?)?)
    }

    /// Checks actual accounted usage plus all outstanding ceilings at a fresh
    /// supplied clock. Capacity is a projection, never evidence of expenditure.
    pub fn remaining(
        &self,
        observed_at: Timestamp,
    ) -> Result<BudgetRemaining, ChildRunBudgetError> {
        if observed_at < self.direct_head.recorded_at()
            || self
                .children
                .iter()
                .filter_map(|entry| entry.settlement.as_ref())
                .any(|settlement| observed_at < settlement.terminal.recorded_at())
        {
            return Err(ChildRunBudgetError::ClockBeforeEvidence);
        }
        let outstanding: Vec<_> = self
            .children
            .iter()
            .filter(|entry| entry.settlement.is_none())
            .map(|entry| entry.reservation.clone())
            .collect();
        Ok(CumulativeBudgetReservation::check_capacity(
            &self.budget,
            &self.accounted_usage()?,
            &outstanding,
            observed_at,
        )?)
    }

    /// Returns the original retained record for an exact logical key.
    #[must_use]
    pub fn entry(&self, key: &ChildRunKey) -> Option<&ChildRunBudgetEntry> {
        self.children.iter().find(|entry| &entry.key == key)
    }

    /// Returns canonically ordered lifetime records, including settled children.
    #[must_use]
    pub fn children(&self) -> &[ChildRunBudgetEntry] {
        &self.children
    }

    /// Returns the separately retained direct-usage observation.
    #[must_use]
    pub const fn direct_usage(&self) -> &BudgetUsage {
        &self.direct_usage
    }

    /// Returns the exact journal head of the separately retained direct observation.
    #[must_use]
    pub const fn direct_head(&self) -> &JournalHead {
        &self.direct_head
    }

    /// Returns the fingerprint an adapter must use for atomic snapshot replacement.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Encodes closed, bounded canonical state. Checksums are not authentication.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ChildRunBudgetError> {
        let bytes = crate::agent_admission::canonical_bytes(self)
            .map_err(|_| ChildRunBudgetError::Encoding)?;
        if bytes.len() > Self::MAX_SNAPSHOT_BYTES {
            return Err(ChildRunBudgetError::Encoding);
        }
        Ok(bytes)
    }

    fn seal(mut self) -> Result<Self, ChildRunBudgetError> {
        if self.version != 1 {
            return Err(ChildRunBudgetError::UnsupportedVersion);
        }
        if self.maximum_children == 0
            || usize::from(self.maximum_children) > Self::MAX_CHILDREN
            || self.children.len() > usize::from(self.maximum_children)
        {
            return Err(ChildRunBudgetError::TooManyChildren);
        }
        if self.direct_head.tenant_id() != self.parent.tenant_id()
            || self.direct_head.run_id() != self.parent.run_id()
            || self.direct_head.recorded_at() < self.parent_admitted_at
        {
            return Err(ChildRunBudgetError::DirectObservationMismatch);
        }
        self.children.sort_by_key(|entry| entry.key.digest());
        let mut keys = BTreeSet::new();
        let mut runs = BTreeSet::new();
        for entry in &self.children {
            entry.validate()?;
            if entry.key.tenant_id() != self.parent.tenant_id()
                || entry.key.parent_run_id() != self.parent.run_id()
                || entry.key.parent().base_checkpoint().graph() != &self.graph
                || entry.child.tenant_id() != self.parent.tenant_id()
                || entry.child.run_id() == self.parent.run_id()
            {
                return Err(ChildRunBudgetError::ParentMismatch);
            }
            if !keys.insert(entry.key.digest()) || !runs.insert(entry.child.run_id()) {
                return Err(ChildRunBudgetError::DuplicateChild);
            }
            if let Some(settlement) = &entry.settlement {
                if settlement.admitted_at < self.parent_admitted_at {
                    return Err(ChildRunBudgetError::TerminalMismatch);
                }
            }
        }
        self.accounted_usage()?;
        // Fixed zero in the preimage avoids serializing a second shadow wire.
        self.digest = Digest::sha256([]);
        let mut preimage = DOMAIN.to_vec();
        preimage.extend(self.canonical_bytes()?);
        self.digest = Digest::sha256(preimage);
        Ok(self)
    }
}

impl<'de> Deserialize<'de> for ChildRunBudgetAccount {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            version: u8,
            parent: AgentResultProvenance,
            parent_admission_digest: Digest,
            parent_admitted_at: Timestamp,
            graph: GraphReference,
            budget: ResolvedBudget,
            maximum_children: u16,
            direct_head: JournalHead,
            direct_usage: BudgetUsage,
            #[serde(deserialize_with = "bounded_children")]
            children: Vec<ChildRunBudgetEntry>,
            digest: Digest,
        }
        let wire = Wire::deserialize(deserializer)?;
        let expected = wire.digest;
        let value = Self {
            version: wire.version,
            parent: wire.parent,
            parent_admission_digest: wire.parent_admission_digest,
            parent_admitted_at: wire.parent_admitted_at,
            graph: wire.graph,
            budget: wire.budget,
            maximum_children: wire.maximum_children,
            direct_head: wire.direct_head,
            direct_usage: wire.direct_usage,
            children: wire.children,
            digest: expected,
        }
        .seal()
        .map_err(de::Error::custom)?;
        if value.digest != expected {
            return Err(de::Error::custom(ChildRunBudgetError::DigestMismatch));
        }
        Ok(value)
    }
}

fn bounded_children<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<ChildRunBudgetEntry>, D::Error> {
    struct Visitor;
    impl<'de> de::Visitor<'de> for Visitor {
        type Value = Vec<ChildRunBudgetEntry>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("at most 256 lifetime child budget entries")
        }
        fn visit_seq<A: de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let maximum = ChildRunBudgetAccount::MAX_CHILDREN;
            if sequence.size_hint().is_some_and(|size| size > maximum) {
                return Err(de::Error::custom(ChildRunBudgetError::TooManyChildren));
            }
            let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(maximum));
            while let Some(value) = sequence.next_element()? {
                if values.len() == maximum {
                    return Err(de::Error::custom(ChildRunBudgetError::TooManyChildren));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Visitor)
}

/// Closed, public-safe accounting refusal without child inputs or outputs.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ChildRunBudgetError {
    /// Admission, tenant, run or graph does not own this account or child.
    #[error("child budget parent mismatch")]
    ParentMismatch,
    /// Unsupported wire version cannot silently acquire current semantics.
    #[error("unsupported child budget account version")]
    UnsupportedVersion,
    /// Lifetime capacity includes settled entries and cannot be recycled.
    #[error("child budget lifetime bound exceeded")]
    TooManyChildren,
    /// Logical key or selected child identity was repeated.
    #[error("duplicate child budget identity")]
    DuplicateChild,
    /// Same logical key was retried with changed immutable spawn intent.
    #[error("child budget spawn conflict")]
    SpawnConflict,
    /// Terminal evidence was changed after one accepted settlement.
    #[error("child budget settlement conflict")]
    SettlementConflict,
    /// Settlement references no retained reservation.
    #[error("child budget reservation not found")]
    ChildNotFound,
    /// Lifecycle is not terminal and cannot release its reservation.
    #[error("child budget requires terminal evidence")]
    NotTerminal,
    /// Terminal evidence and immutable child/admission/journal identities differ.
    #[error("child budget terminal evidence mismatch")]
    TerminalMismatch,
    /// Unknown cost must retain its outstanding reservation.
    #[error("child budget cannot settle unpriced usage")]
    UnpricedSettlement,
    /// Direct usage was substituted, stale, or bound to another journal.
    #[error("child budget direct observation mismatch")]
    DirectObservationMismatch,
    /// Capacity was evaluated with a clock older than retained accounting evidence.
    #[error("child budget clock predates accounting evidence")]
    ClockBeforeEvidence,
    /// The bounded canonical account could not be encoded.
    #[error("child budget account encoding failed")]
    Encoding,
    /// Restored state differs from its integrity fingerprint.
    #[error("child budget account digest mismatch")]
    DigestMismatch,
    /// Scalar/currency arithmetic overflow or monotonicity violation.
    #[error(transparent)]
    Usage(#[from] BudgetUsageError),
    /// Reservation projection exceeds finite or observable capacity.
    #[error(transparent)]
    Reservation(#[from] CumulativeBudgetReservationError),
    /// Declared parent/child closure validation failed.
    #[error(transparent)]
    Declaration(#[from] ChildRunPolicyError),
    /// A child ceiling or deadline widened the immutable parent budget.
    #[error(transparent)]
    Narrowing(#[from] BudgetNarrowingError),
}
