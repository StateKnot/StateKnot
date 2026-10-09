// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Prefix proofs live only inside their owning database transaction.
#[allow(clippy::wildcard_imports)]
use super::*;

#[derive(Clone)]
pub(super) struct Prefix {
    pub(super) entry_digest: Digest,
    pub(super) checkpoint_id: CheckpointId,
    pub(super) superstep: u64,
    pub(super) checkpoint_digest: Digest,
    pub(super) frame_checkpoint_digest: Digest,
    pub(super) usage: BudgetUsage,
}

// No process-global cache or full state/result buffers. SQL scope uniqueness
// and immutable facts make a previously verified prefix reusable in this
// repeatable-read snapshot or under the owning Run mutation lock. Owning the
// transaction borrow prevents a proof from crossing a transaction boundary.
pub(super) struct Replay<'borrow, 'connection> {
    pub(super) tx: &'borrow mut Transaction<'connection, Postgres>,
    prefixes: BTreeMap<(TenantId, RunId, GraphNamespace), Prefix>,
    returns: BTreeMap<(TenantId, RunId, u64), (Digest, BudgetUsage)>,
    visiting_returns: std::collections::BTreeSet<(TenantId, RunId, u64)>,
}
impl<'borrow, 'connection> Replay<'borrow, 'connection> {
    pub(super) fn new(tx: &'borrow mut Transaction<'connection, Postgres>) -> Self {
        Self {
            tx,
            prefixes: BTreeMap::new(),
            returns: BTreeMap::new(),
            visiting_returns: std::collections::BTreeSet::new(),
        }
    }
    fn key(entry: &StoredGraphFrameEntry) -> (TenantId, RunId, GraphNamespace) {
        let frame = entry.entry.checkpoint();
        (
            frame.checkpoint().tenant_id().clone(),
            frame.checkpoint().run_id(),
            frame.frame().namespace().clone(),
        )
    }
    pub(super) fn prefix(
        &self,
        entry: &StoredGraphFrameEntry,
    ) -> Result<Option<Prefix>, StoreError> {
        let prefix = self.prefixes.get(&Self::key(entry));
        if prefix.is_some_and(|p| p.entry_digest != entry.digest) {
            return Err(StoreError::corrupt("frame replay entry substitution"));
        }
        Ok(prefix.cloned())
    }
    pub(super) fn remember(
        &mut self,
        entry: &StoredGraphFrameEntry,
        checkpoint: &GraphFrameCheckpoint,
        usage: &BudgetUsage,
    ) -> Result<(), StoreError> {
        let key = Self::key(entry);
        if !self.prefixes.contains_key(&key) && self.prefixes.len() == 4096 {
            return Err(StoreError::GraphReplayResourceLimit);
        }
        let position = checkpoint.checkpoint().superstep().get();
        if self
            .prefixes
            .get(&key)
            .is_some_and(|p| p.superstep > position)
        {
            return Ok(());
        }
        if checkpoint.frame() != entry.entry.checkpoint().frame() {
            return Err(StoreError::corrupt("frame replay scope substitution"));
        }
        self.prefixes.insert(
            key,
            Prefix {
                entry_digest: entry.digest,
                checkpoint_id: checkpoint.checkpoint().checkpoint_id(),
                superstep: position,
                checkpoint_digest: checkpoint.checkpoint().digest(),
                frame_checkpoint_digest: checkpoint.digest(),
                usage: usage.clone(),
            },
        );
        Ok(())
    }
    pub(super) fn return_usage(
        &self,
        tenant: &TenantId,
        run: RunId,
        sequence: u64,
        digest: Digest,
    ) -> Result<Option<BudgetUsage>, StoreError> {
        let value = self.returns.get(&(tenant.clone(), run, sequence));
        if value.is_some_and(|(proof, _)| *proof != digest) {
            return Err(StoreError::corrupt("frame replay return substitution"));
        }
        Ok(value.map(|(_, usage)| usage.clone()))
    }
    pub(super) fn latest_return_before(&self, tenant: &TenantId, run: RunId, through: u64) -> u64 {
        self.returns
            .range((tenant.clone(), run, 0)..=(tenant.clone(), run, through))
            .next_back()
            .map_or(0, |(key, _)| key.2)
    }
    pub(super) fn remember_return(
        &mut self,
        tenant: &TenantId,
        run: RunId,
        sequence: u64,
        digest: Digest,
        usage: &BudgetUsage,
    ) -> Result<(), StoreError> {
        let key = (tenant.clone(), run, sequence);
        if !self.returns.contains_key(&key) && self.returns.len() == 4096 {
            return Err(StoreError::GraphReplayResourceLimit);
        }
        if self
            .returns
            .get(&key)
            .is_some_and(|(proof, counters)| *proof != digest || counters != usage)
        {
            return Err(StoreError::corrupt(
                "frame replay return proof substitution",
            ));
        }
        self.returns.insert(key, (digest, usage.clone()));
        Ok(())
    }
    pub(super) fn enter_return(
        &mut self,
        tenant: &TenantId,
        run: RunId,
        sequence: u64,
    ) -> Result<(), StoreError> {
        if self.visiting_returns.len() == 8
            || !self
                .visiting_returns
                .insert((tenant.clone(), run, sequence))
        {
            return Err(StoreError::corrupt("cyclic frame return proof"));
        }
        Ok(())
    }
    pub(super) fn leave_return(&mut self, tenant: &TenantId, run: RunId, sequence: u64) {
        self.visiting_returns
            .remove(&(tenant.clone(), run, sequence));
    }
}
