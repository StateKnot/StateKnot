// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use stateknot_core::{
    Checkpoint, CheckpointId, Digest, FencingEpoch, JournalEvent, JournalHead, RunLease,
    RunLifecycle, RunRevision, RunTransition, Superstep,
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

/// Result of atomically committing a journal event and graph checkpoint.
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
