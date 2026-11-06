// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Exact-journal trusted accounting for framework graph actions.

use stateknot_core::{
    AgentResultProvenance, BudgetUsage, CheckpointHead, GraphFrameCheckpointHead, JournalHead,
    RunFence,
};

/// Payload-free observation for complete cumulative Run-wide DIRECT accounting.
///
/// The provider must recover all already-durable work at this exact journal,
/// including failed attempts and framework charges, retaining unknown cost
/// events and excluding child-Run subtree usage. A usage floor is insufficient.
/// This context and the returned evidence grant no mutation or launch authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphDirectUsageEvidenceContext {
    provenance: AgentResultProvenance,
    root: CheckpointHead,
    leaf: Option<GraphFrameCheckpointHead>,
    journal_head: JournalHead,
    fence: RunFence,
}

impl GraphDirectUsageEvidenceContext {
    pub(crate) const fn new(
        provenance: AgentResultProvenance,
        root: CheckpointHead,
        leaf: Option<GraphFrameCheckpointHead>,
        journal_head: JournalHead,
        fence: RunFence,
    ) -> Self {
        Self {
            provenance,
            root,
            leaf,
            journal_head,
            fence,
        }
    }

    /// Returns the original admitted tenant, Run, and invocation provenance.
    #[must_use]
    pub const fn provenance(&self) -> &AgentResultProvenance {
        &self.provenance
    }

    /// Returns the exact Root checkpoint, including during scoped execution.
    #[must_use]
    pub const fn root_checkpoint(&self) -> &CheckpointHead {
        &self.root
    }

    /// Returns the exact actual leaf, or None at authenticated Root scope.
    #[must_use]
    pub const fn leaf(&self) -> Option<&GraphFrameCheckpointHead> {
        self.leaf.as_ref()
    }

    /// Returns the precise immutable journal boundary of this observation.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the claimed original worker identity, epoch, and attempt.
    #[must_use]
    pub const fn fence(&self) -> &RunFence {
        &self.fence
    }
}

/// Trusted complete DIRECT usage for one exact framework-action observation.
///
/// Providers attest completeness, not merely a monotonic lower bound. The Driver
/// verifies the retained context and Store repeats actual shared-budget, scope,
/// journal and live-fence checks. Constructing evidence authorizes no work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphDirectUsageEvidence {
    context: GraphDirectUsageEvidenceContext,
    usage: BudgetUsage,
}

impl GraphDirectUsageEvidence {
    /// Binds the provider's complete already-durable usage to its exact context.
    #[must_use]
    pub const fn new(context: GraphDirectUsageEvidenceContext, usage: BudgetUsage) -> Self {
        Self { context, usage }
    }

    /// Returns the context retained by the trusted provider.
    #[must_use]
    pub const fn context(&self) -> &GraphDirectUsageEvidenceContext {
        &self.context
    }

    /// Returns complete Run-wide DIRECT usage, excluding child subtrees.
    #[must_use]
    pub const fn usage(&self) -> &BudgetUsage {
        &self.usage
    }

    pub(crate) fn into_usage(self) -> BudgetUsage {
        self.usage
    }
}
