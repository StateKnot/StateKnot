// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::{
    AgentRunSnapshot, AgentServiceCaller, AgentServiceError, AgentServiceRunOperation,
    AgentServiceRunTarget, AgentServiceV1,
};
use stateknot_core::{JournalHead, RunId};
use stateknot_store_postgres::JournalPageSize;

/// Public metadata for at most one durable activity notification and a current
/// snapshot. These are separate observations, NOT a historical state projection.
/// Journal payloads, custom kinds and worker details never cross this boundary.
#[derive(Clone, Debug)]
pub struct AgentRunActivityPage {
    snapshot: AgentRunSnapshot,
    head: Option<JournalHead>,
    has_more: bool,
}

impl AgentRunActivityPage {
    /// Returns the verified snapshot observed before the journal page read.
    #[must_use]
    pub const fn snapshot(&self) -> &AgentRunSnapshot {
        &self.snapshot
    }

    /// Exact committed metadata; the checksum is integrity evidence, not authority.
    #[must_use]
    pub const fn head(&self) -> Option<&JournalHead> {
        self.head.as_ref()
    }

    /// Whether another event existed in the journal read's database snapshot.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }
}

impl AgentServiceV1 {
    /// Authorizes before verifying an Agent and reading one exact journal suffix.
    ///
    /// A one-event page bounds private payload materialization in the store. No
    /// cursor starts at sequence one. The store checks a supplied head against
    /// the retained event; it never treats caller metadata as trusted authority.
    /// Snapshots may coalesce transitions and must not be interpreted at the head.
    ///
    /// # Errors
    ///
    /// Denial precedes existence/cursor validation. Missing history, changed scope
    /// or any mismatch fails closed rather than silently restarting replay.
    pub async fn load_activity(
        &self,
        caller: AgentServiceCaller,
        run_id: RunId,
        after: Option<&JournalHead>,
    ) -> Result<AgentRunActivityPage, AgentServiceError> {
        self.authorize_run(
            &caller,
            AgentServiceRunTarget::Run(run_id),
            AgentServiceRunOperation::Read,
        )
        .await?;
        let snapshot = self.runs.load(caller.tenant_id(), run_id).await?;
        let page = self
            .store
            .load_journal_page(
                caller.tenant_id(),
                run_id,
                after,
                JournalPageSize::new(1).expect("static bounded journal page"),
            )
            .await?;
        Ok(AgentRunActivityPage {
            snapshot,
            head: page.next_cursor(),
            has_more: page.has_more(),
        })
    }
}
