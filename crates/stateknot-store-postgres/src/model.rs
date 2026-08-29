// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use stateknot_core::{
    Checkpoint, CheckpointHead, CheckpointId, Digest, FencingEpoch, JournalEvent, JournalHead,
    ModelInvocation, NodeAttempt, PendingNodeResult, PendingNodeResultHead, RunLease, RunLifecycle,
    RunRevision, RunTransition, Superstep, ToolInvocation,
};

use crate::StoreError;

/// Result of idempotent run admission.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum AdmissionOutcome {
    /// A new pending run row committed.
    Committed(RunLifecycle),
    /// The same provenance was already admitted; the current snapshot is returned.
    Idempotent(RunLifecycle),
}

impl AdmissionOutcome {
    /// Returns the validated lifecycle snapshot.
    #[must_use]
    pub const fn lifecycle(&self) -> &RunLifecycle {
        match self {
            Self::Committed(lifecycle) | Self::Idempotent(lifecycle) => lifecycle,
        }
    }
}

/// Result of an idempotent journal append.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum AppendOutcome {
    /// A new event and projection committed atomically.
    Committed(JournalEvent),
    /// The identical event intent was already committed.
    Idempotent(JournalEvent),
}

/// Result of atomically committing a journal event and initial graph
/// checkpoint.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum CheckpointCommitOutcome {
    /// A new event, checkpoint, run head, and optional lifecycle projection committed.
    Committed {
        /// Newly committed journal event.
        event: JournalEvent,
        /// Newly committed immutable checkpoint.
        checkpoint: Checkpoint,
    },
    /// The exact event, projection, and checkpoint intent had already committed.
    Idempotent {
        /// Previously committed journal event.
        event: JournalEvent,
        /// Previously committed immutable checkpoint.
        checkpoint: Checkpoint,
    },
}

impl CheckpointCommitOutcome {
    /// Returns the validated anchoring journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the validated immutable checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        match self {
            Self::Committed { checkpoint, .. } | Self::Idempotent { checkpoint, .. } => checkpoint,
        }
    }
}

/// Result of atomically consuming one complete result barrier and committing
/// its anchoring event and successor checkpoint.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum BarrierCommitOutcome {
    /// The event, successor checkpoint, and every result-consumption record
    /// committed in one transaction.
    Committed {
        /// Newly committed anchoring journal event.
        event: JournalEvent,
        /// Newly committed immutable successor checkpoint.
        checkpoint: Checkpoint,
    },
    /// The exact barrier transaction had already committed.
    Idempotent {
        /// Previously committed anchoring journal event.
        event: JournalEvent,
        /// Previously committed immutable successor checkpoint.
        checkpoint: Checkpoint,
    },
}

impl BarrierCommitOutcome {
    /// Returns the validated anchoring journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the validated immutable successor checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        match self {
            Self::Committed { checkpoint, .. } | Self::Idempotent { checkpoint, .. } => checkpoint,
        }
    }
}

/// Result of atomically committing a journal event and tool invocation revision.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ToolInvocationCommitOutcome {
    /// A new event and immutable invocation revision committed atomically.
    Committed {
        /// Newly committed journal event.
        event: JournalEvent,
        /// Newly committed invocation revision.
        invocation: ToolInvocation,
    },
    /// The exact event and invocation mutation had already committed.
    Idempotent {
        /// Previously committed journal event.
        event: JournalEvent,
        /// Previously committed invocation revision.
        invocation: ToolInvocation,
    },
}

impl ToolInvocationCommitOutcome {
    /// Returns the validated anchoring journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the validated immutable invocation revision.
    #[must_use]
    pub const fn invocation(&self) -> &ToolInvocation {
        match self {
            Self::Committed { invocation, .. } | Self::Idempotent { invocation, .. } => invocation,
        }
    }
}

/// Result of atomically committing a journal event and model invocation revision.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ModelInvocationCommitOutcome {
    /// A new event and immutable invocation revision committed atomically.
    Committed {
        /// Newly committed journal event.
        event: JournalEvent,
        /// Newly committed invocation revision.
        invocation: ModelInvocation,
    },
    /// The exact event and invocation mutation had already committed.
    Idempotent {
        /// Previously committed journal event.
        event: JournalEvent,
        /// Previously committed invocation revision.
        invocation: ModelInvocation,
    },
}

impl ModelInvocationCommitOutcome {
    /// Returns the validated anchoring journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the validated immutable invocation revision.
    #[must_use]
    pub const fn invocation(&self) -> &ModelInvocation {
        match self {
            Self::Committed { invocation, .. } | Self::Idempotent { invocation, .. } => invocation,
        }
    }
}

/// Legacy outcome shape for the pre-v6 direct pending-result API.
///
/// New writes return [`NodeAttemptCommitOutcome`] because every result must be
/// owned by a durable physical node attempt. This type remains public so the
/// fail-closed compatibility method can preserve its original signature.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum PendingNodeResultCommitOutcome {
    /// A new event and immutable pending result committed atomically.
    Committed {
        /// Newly committed journal event.
        event: JournalEvent,
        /// Newly committed pending node result.
        result: PendingNodeResult,
    },
    /// The same semantic node result had already committed.
    Idempotent {
        /// Original journal event that anchored the stored winner.
        event: JournalEvent,
        /// Previously committed pending node result.
        result: PendingNodeResult,
    },
}

impl PendingNodeResultCommitOutcome {
    /// Returns the validated anchoring journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the validated immutable pending result.
    #[must_use]
    pub const fn result(&self) -> &PendingNodeResult {
        match self {
            Self::Committed { result, .. } | Self::Idempotent { result, .. } => result,
        }
    }
}

/// Result of atomically committing a node-attempt start or completion event.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum NodeAttemptCommitOutcome {
    /// A new event and immutable node-attempt record committed atomically.
    Committed {
        /// Newly committed anchoring journal event.
        event: JournalEvent,
        /// Fully restored attempt after this mutation.
        attempt: NodeAttempt,
    },
    /// The exact start or completion had already committed.
    Idempotent {
        /// Original journal event that anchored the stored mutation.
        event: JournalEvent,
        /// Fully restored durable attempt.
        attempt: NodeAttempt,
    },
}

impl NodeAttemptCommitOutcome {
    /// Returns the validated anchoring journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the fully restored physical attempt.
    #[must_use]
    pub const fn attempt(&self) -> &NodeAttempt {
        match self {
            Self::Committed { attempt, .. } | Self::Idempotent { attempt, .. } => attempt,
        }
    }
}

/// Hard-bounded number of physical node attempts in one history page.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeAttemptHistoryPageSize(u8);

impl NodeAttemptHistoryPageSize {
    /// Largest page accepted by the provider.
    ///
    /// A start and completion can occupy about 17 MiB together, so two
    /// records cap provider-owned decoded page memory near 34 MiB before
    /// driver overhead.
    pub const MAX: u8 = 2;

    /// Constructs a positive bounded history page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidNodeAttemptPageSize`] for zero or values
    /// above two.
    pub const fn new(value: u8) -> Result<Self, StoreError> {
        if value == 0 || value > Self::MAX {
            return Err(StoreError::InvalidNodeAttemptPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// One bounded, fully verified ascending physical-attempt history page.
#[derive(Clone, Debug)]
pub struct NodeAttemptHistoryPage {
    pub(crate) records: Vec<NodeAttempt>,
    pub(crate) has_more: bool,
}

impl NodeAttemptHistoryPage {
    /// Returns immutable physical attempts in ascending start order.
    #[must_use]
    pub fn records(&self) -> &[NodeAttempt] {
        &self.records
    }

    /// Returns whether a later physical attempt remains in the snapshot.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the full last attempt required as the exact next-page cursor.
    #[must_use]
    pub fn next_cursor(&self) -> Option<NodeAttempt> {
        self.records.last().cloned()
    }
}

/// Hard-bounded number of fully decoded pending results in one recovery page.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PendingNodeResultPageSize(u8);

impl PendingNodeResultPageSize {
    /// Largest page accepted by the provider.
    ///
    /// One canonical pending result may occupy 16 MiB and can reference large
    /// model records that are verified in bounded sub-batches. Two retained
    /// results cap provider-owned decoded page memory near 32 MiB before driver
    /// and invocation-verification overhead. Look-ahead rows remain compact.
    pub const MAX: u8 = 2;

    /// Constructs a positive bounded pending-result page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidPendingNodeResultPageSize`] for zero or
    /// values above two.
    pub const fn new(value: u8) -> Result<Self, StoreError> {
        if value == 0 || value > Self::MAX {
            return Err(StoreError::InvalidPendingNodeResultPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// Exact stable-snapshot continuation for unconsumed pending-result scanning.
///
/// The cursor binds the immutable base checkpoint, the run journal head seen
/// by the first page, and the last fully verified result. A later result commit
/// changes the run journal head, making continuation fail explicitly instead
/// of silently skipping a newly inserted lower sort key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingNodeResultPageCursor {
    pub(crate) base_checkpoint: CheckpointHead,
    pub(crate) snapshot_journal_head: JournalHead,
    pub(crate) after: PendingNodeResultHead,
}

impl PendingNodeResultPageCursor {
    /// Returns the exact checkpoint whose unconsumed set is being scanned.
    #[must_use]
    pub const fn base_checkpoint(&self) -> &CheckpointHead {
        &self.base_checkpoint
    }

    /// Returns the run head that must remain unchanged across page calls.
    #[must_use]
    pub const fn snapshot_journal_head(&self) -> &JournalHead {
        &self.snapshot_journal_head
    }

    /// Returns the last fully verified result from the preceding page.
    #[must_use]
    pub const fn after(&self) -> &PendingNodeResultHead {
        &self.after
    }
}

/// One bounded, fully verified page of unconsumed pending node results.
#[derive(Clone, Debug)]
pub struct PendingNodeResultPage {
    pub(crate) base_checkpoint: CheckpointHead,
    pub(crate) snapshot_journal_head: JournalHead,
    pub(crate) records: Vec<PendingNodeResult>,
    pub(crate) has_more: bool,
}

impl PendingNodeResultPage {
    /// Returns the exact base checkpoint observed by this page.
    #[must_use]
    pub const fn base_checkpoint(&self) -> &CheckpointHead {
        &self.base_checkpoint
    }

    /// Returns the stable run journal head shared by every page continuation.
    #[must_use]
    pub const fn snapshot_journal_head(&self) -> &JournalHead {
        &self.snapshot_journal_head
    }

    /// Returns immutable pending results in canonical activation order.
    #[must_use]
    pub fn records(&self) -> &[PendingNodeResult] {
        &self.records
    }

    /// Returns whether a later result remains in the observed snapshot.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the exact continuation required for the next page.
    #[must_use]
    pub fn next_cursor(&self) -> Option<PendingNodeResultPageCursor> {
        self.records
            .last()
            .map(|result| PendingNodeResultPageCursor {
                base_checkpoint: self.base_checkpoint.clone(),
                snapshot_journal_head: self.snapshot_journal_head.clone(),
                after: result.head(),
            })
    }
}

/// Hard-bounded number of model invocation revisions in one history page.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ModelInvocationHistoryPageSize(u8);

impl ModelInvocationHistoryPageSize {
    /// Largest page accepted by the provider.
    ///
    /// A compact revision and its separately loaded intent may each occupy up
    /// to 128 MiB, so the provider admits only one revision per decoded page.
    pub const MAX: u8 = 1;

    /// Constructs a positive bounded history page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidModelInvocationPageSize`] unless `value` is one.
    pub const fn new(value: u8) -> Result<Self, StoreError> {
        if value != Self::MAX {
            return Err(StoreError::InvalidModelInvocationPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// One bounded, fully verified ascending model-invocation history page.
#[derive(Clone, Debug)]
pub struct ModelInvocationHistoryPage {
    pub(crate) records: Vec<ModelInvocation>,
    pub(crate) has_more: bool,
}

impl ModelInvocationHistoryPage {
    /// Returns immutable revisions in ascending order.
    #[must_use]
    pub fn records(&self) -> &[ModelInvocation] {
        &self.records
    }

    /// Returns whether a later revision remains in the observed snapshot.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the full last record required as the exact next-page cursor.
    #[must_use]
    pub fn next_cursor(&self) -> Option<ModelInvocation> {
        self.records.last().cloned()
    }
}

/// Hard-bounded number of invocation revisions returned in one history page.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ToolInvocationHistoryPageSize(u8);

impl ToolInvocationHistoryPageSize {
    /// Largest page accepted by the provider.
    ///
    /// One canonical record may occupy up to 16 MiB, so two records cap the
    /// provider-owned decoded page near 32 MiB before driver overhead.
    pub const MAX: u8 = 2;

    /// Constructs a positive bounded history page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidToolInvocationPageSize`] for zero or values
    /// above two.
    pub const fn new(value: u8) -> Result<Self, StoreError> {
        if value == 0 || value > Self::MAX {
            return Err(StoreError::InvalidToolInvocationPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// One bounded, fully verified ascending invocation-history page.
#[derive(Clone, Debug)]
pub struct ToolInvocationHistoryPage {
    pub(crate) records: Vec<ToolInvocation>,
    pub(crate) has_more: bool,
}

impl ToolInvocationHistoryPage {
    /// Returns immutable revisions in ascending order.
    #[must_use]
    pub fn records(&self) -> &[ToolInvocation] {
        &self.records
    }

    /// Returns whether a later revision remains in the observed snapshot.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the full last record required as the exact next-page cursor.
    #[must_use]
    pub fn next_cursor(&self) -> Option<ToolInvocation> {
        self.records.last().cloned()
    }
}

impl AppendOutcome {
    /// Returns the validated committed event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed(event) | Self::Idempotent(event) => event,
        }
    }
}

/// Lifecycle projection to commit atomically with one journal append.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum RunProjection {
    /// Preserve the current lifecycle bytes and revision.
    Unchanged,
    /// Apply one pure lifecycle transition after matching the exact revision.
    Transition {
        /// Revision from which the pure lifecycle transition was derived.
        expected_revision: RunRevision,
        /// Transition to apply to the locked durable lifecycle.
        transition: RunTransition,
    },
}

/// Compact current-checkpoint pointer projected into the locked run row.
///
/// The full checkpoint must still be loaded and validated before recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointPointer {
    pub(crate) checkpoint_id: CheckpointId,
    pub(crate) superstep: Superstep,
    pub(crate) digest: Digest,
}

impl CheckpointPointer {
    /// Returns the immutable checkpoint identity.
    #[must_use]
    pub const fn checkpoint_id(&self) -> CheckpointId {
        self.checkpoint_id
    }

    /// Returns the current committed barrier position.
    #[must_use]
    pub const fn superstep(&self) -> Superstep {
        self.superstep
    }

    /// Returns the complete checkpoint checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

/// Hard-bounded number of checkpoints returned in one lineage page.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CheckpointLineagePageSize(u8);

impl CheckpointLineagePageSize {
    /// Largest page accepted by the provider.
    ///
    /// One checkpoint envelope may occupy roughly 2.5 MiB, so this deliberately
    /// remains much smaller than [`JournalPageSize::MAX`]. The provider may
    /// decode one additional parent as a fail-closed lineage look-ahead.
    pub const MAX: u8 = 8;

    /// Constructs a positive bounded checkpoint-lineage page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidCheckpointPageSize`] for zero or values
    /// above eight.
    pub const fn new(value: u8) -> Result<Self, StoreError> {
        if value == 0 || value > Self::MAX {
            return Err(StoreError::InvalidCheckpointPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// One bounded, fully verified reverse checkpoint-lineage page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointLineagePage {
    pub(crate) checkpoints: Vec<Checkpoint>,
    pub(crate) next_cursor: Option<CheckpointHead>,
}

impl CheckpointLineagePage {
    /// Returns checkpoints in newest-to-oldest lineage order.
    #[must_use]
    pub fn checkpoints(&self) -> &[Checkpoint] {
        &self.checkpoints
    }

    /// Returns whether older ancestors remain before the superstep-zero root.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.next_cursor.is_some()
    }

    /// Returns the exact parent head at which the next reverse page must start.
    #[must_use]
    pub fn next_cursor(&self) -> Option<CheckpointHead> {
        self.next_cursor.clone()
    }
}

impl RunProjection {
    /// Constructs a no-op lifecycle projection.
    #[must_use]
    pub const fn unchanged() -> Self {
        Self::Unchanged
    }

    /// Constructs an exact-revision pure lifecycle transition.
    #[must_use]
    pub const fn transition(expected_revision: RunRevision, transition: RunTransition) -> Self {
        Self::Transition {
            expected_revision,
            transition,
        }
    }
}

/// Validated durable snapshot of one tenant-scoped run row.
#[derive(Clone, Debug)]
pub struct StoredRun {
    pub(crate) lifecycle: RunLifecycle,
    pub(crate) journal_head: Option<JournalHead>,
    pub(crate) lease: Option<RunLease>,
    pub(crate) last_fencing_epoch: Option<FencingEpoch>,
    pub(crate) checkpoint: Option<CheckpointPointer>,
    pub(crate) quarantined: bool,
}

impl StoredRun {
    /// Returns current validated business lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> &RunLifecycle {
        &self.lifecycle
    }

    /// Returns the current exact journal head, if non-empty.
    #[must_use]
    pub const fn journal_head(&self) -> Option<&JournalHead> {
        self.journal_head.as_ref()
    }

    /// Returns the active lease, including an expired lease not yet reclaimed.
    #[must_use]
    pub const fn lease(&self) -> Option<&RunLease> {
        self.lease.as_ref()
    }

    /// Returns the last issued fencing epoch; an unleased new run has none.
    #[must_use]
    pub const fn last_fencing_epoch(&self) -> Option<FencingEpoch> {
        self.last_fencing_epoch
    }

    /// Returns the compact current-checkpoint pointer, if the graph has reached
    /// its first durable barrier.
    #[must_use]
    pub const fn checkpoint(&self) -> Option<&CheckpointPointer> {
        self.checkpoint.as_ref()
    }

    /// Returns whether integrity or operator policy quarantined the run.
    #[must_use]
    pub const fn is_quarantined(&self) -> bool {
        self.quarantined
    }
}

/// Result of a lease renewal with stable desired expiry.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LeaseRenewalOutcome {
    /// Renewal extended the durable expiry.
    Renewed(RunLease),
    /// The same desired expiry had already committed.
    Idempotent(RunLease),
}

/// Result of claiming or explicitly superseding execution ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LeaseClaimOutcome {
    /// A new attempt and successor fencing epoch committed.
    Claimed(RunLease),
    /// The same stable attempt already owns the current unexpired lease.
    Idempotent(RunLease),
}

impl LeaseClaimOutcome {
    /// Returns the current validated lease.
    #[must_use]
    pub const fn lease(&self) -> &RunLease {
        match self {
            Self::Claimed(lease) | Self::Idempotent(lease) => lease,
        }
    }
}

impl LeaseRenewalOutcome {
    /// Returns the current validated lease.
    #[must_use]
    pub const fn lease(&self) -> &RunLease {
        match self {
            Self::Renewed(lease) | Self::Idempotent(lease) => lease,
        }
    }
}

/// Result of an exact-fence lease release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LeaseReleaseOutcome {
    /// The active lease was cleared.
    Released,
    /// The exact epoch was already released and no successor was issued.
    Idempotent,
}

/// Hard-bounded number of events returned by one journal read.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JournalPageSize(u16);

impl JournalPageSize {
    /// Largest page accepted by the provider.
    pub const MAX: u16 = 1_000;

    /// Constructs a positive bounded page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidPageSize`] for zero or values above 1,000.
    pub const fn new(value: u16) -> Result<Self, StoreError> {
        if value == 0 || value > Self::MAX {
            return Err(StoreError::InvalidPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// One bounded, validated page from a run journal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalPage {
    pub(crate) events: Vec<JournalEvent>,
    pub(crate) has_more: bool,
}

impl JournalPage {
    /// Returns events in ascending contiguous sequence order.
    #[must_use]
    pub fn events(&self) -> &[JournalEvent] {
        &self.events
    }

    /// Returns whether another page exists after the final returned sequence.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the complete final event head to use as the next exact cursor.
    #[must_use]
    pub fn next_cursor(&self) -> Option<JournalHead> {
        self.events.last().map(JournalEvent::head)
    }
}
