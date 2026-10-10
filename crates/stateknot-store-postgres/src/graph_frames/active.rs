// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! One bounded, authenticated active stack snapshot for framework recovery.
#[allow(clippy::wildcard_imports)]
use super::*;

/// Compact binding for one open frame, authenticated with the entire stack.
///
/// This is a snapshot observation, not a portable proof or mutation authority.
#[derive(Clone, Debug)]
pub struct StoredOpenGraphFrame {
    checkpoint: GraphFrameCheckpointHead,
    entry_digest: Digest,
    caller: NodeAttemptStartHead,
    caller_binding_digest: Digest,
}
impl StoredOpenGraphFrame {
    /// Exact current checkpoint and logical frame identity.
    #[must_use]
    pub const fn checkpoint(&self) -> &GraphFrameCheckpointHead {
        &self.checkpoint
    }
    /// Compound immutable entry owning this logical scope.
    #[must_use]
    pub const fn entry_digest(&self) -> Digest {
        self.entry_digest
    }
    /// Latest physical caller, preserving its actual original fence.
    #[must_use]
    pub const fn caller(&self) -> &NodeAttemptStartHead {
        &self.caller
    }
    /// Complete entry or takeover binding of that physical caller.
    #[must_use]
    pub const fn caller_binding_digest(&self) -> Digest {
        self.caller_binding_digest
    }
}

/// Complete current leaf and physical caller, authenticated in one snapshot.
///
/// This read grants no execution authority. Dispatch must claim/revalidate the
/// exact Run fence and use the dedicated durable scoped mutation APIs.
#[derive(Clone, Debug)]
pub struct StoredActiveGraphFrame {
    run: StoredRun,
    entry: StoredGraphFrameEntry,
    checkpoint: GraphFrameCheckpoint,
    caller: NodeAttemptStart,
    caller_binding_digest: Digest,
    minimum_direct_usage: BudgetUsage,
    open_frames: Vec<StoredOpenGraphFrame>,
}
impl StoredActiveGraphFrame {
    /// Run projection and journal observation from the same database snapshot.
    #[must_use]
    pub const fn run(&self) -> &StoredRun {
        &self.run
    }
    /// Immutable logical entry; takeover never creates a replacement child.
    #[must_use]
    pub const fn entry(&self) -> &StoredGraphFrameEntry {
        &self.entry
    }
    /// Whole current leaf checkpoint with authenticated forward lineage.
    #[must_use]
    pub const fn checkpoint(&self) -> &GraphFrameCheckpoint {
        &self.checkpoint
    }
    /// Latest verified physical caller, which may belong to an older fence.
    #[must_use]
    pub const fn caller(&self) -> &NodeAttemptStart {
        &self.caller
    }
    /// Compound entry or takeover proof owning the current caller.
    #[must_use]
    pub const fn caller_binding_digest(&self) -> Digest {
        self.caller_binding_digest
    }
    /// Compact current bindings for every open frame, ordered root-to-leaf.
    ///
    /// At most seven entries. Every parent is suspended at the exact next
    /// caller's base; only the final entry is the active leaf.
    #[must_use]
    pub fn open_frames(&self) -> &[StoredOpenGraphFrame] {
        &self.open_frames
    }
    /// Minimum authenticated DIRECT usage from structural committed facts.
    ///
    /// This is a floor, not complete provider/node accounting. Callers must add
    /// all actual work before mutations that require complete direct usage.
    #[must_use]
    pub const fn minimum_direct_usage(&self) -> &BudgetUsage {
        &self.minimum_direct_usage
    }
}

impl PostgresStore {
    /// Restores the active leaf, suspended ancestors and latest physical caller
    /// in one repeatable-read snapshot, independently of current lease ownership.
    ///
    /// `None` means the verified stack is at Root. Missing stack rows cannot
    /// hide retained frame entries. At most seven open scopes and 4,096 lifetime
    /// entries are accepted; full state buffers are released at each ancestor.
    /// Returned history, current head bindings, original admission/graph pins,
    /// shared usage floors and whole wait terminal records are authenticated.
    ///
    /// # Errors
    /// Returns missing Run, durable corruption, replay bound or database errors.
    pub fn load_active_graph_frame<'a>(
        &'a self,
        tenant: &'a TenantId,
        run_id: RunId,
    ) -> stateknot_core::BoxFuture<'a, Result<Option<StoredActiveGraphFrame>, StoreError>> {
        Box::pin(self.load_active_graph_frame_inner(tenant, run_id))
    }

    // Erase the recovery future at the public boundary so caller state machines
    // do not embed every full replay branch on the normal thread stack.
    #[allow(clippy::too_many_lines)]
    async fn load_active_graph_frame_inner(
        &self,
        tenant: &TenantId,
        run_id: RunId,
    ) -> Result<Option<StoredActiveGraphFrame>, StoreError> {
        let mut tx = self
            .begin_repeatable_read("active frame recovery snapshot")
            .await?;
        let stored = query_as::<_, RunRow>(SELECT_RUN)
            .bind(tenant.as_str())
            .bind(*run_id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StoreError::database("active frame Run snapshot", e))?
            .ok_or(StoreError::RunNotFound)?;
        // Even Root/legacy reads must authenticate the existing Run projection.
        let stored = decode_run(stored)?;
        let active = Box::pin(verified_snapshot(&mut tx, tenant, run_id, stored)).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::database("active frame recovery commit", e))?;
        Ok(active)
    }
}

// The transaction borrow owns every proof. Mutations call this under the Run
// row lock, so they never stitch an external read into their authorization.
#[allow(clippy::too_many_lines)]
pub(super) async fn verified_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run_id: RunId,
    stored: StoredRun,
) -> Result<Option<StoredActiveGraphFrame>, StoreError> {
    let stack = query_as::<_, StackRow>("SELECT admission_digest,lifetime_starts,active_namespace,active_frame_identity_digest FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2")
            .bind(tenant.as_str()).bind(*run_id.as_uuid()).fetch_optional(&mut **tx).await
            .map_err(|e| StoreError::database("active frame stack snapshot", e))?;
    let (total, maximum): (i64, Option<i32>) = query_as("SELECT count(*),max(ordinal) FROM stateknot.graph_frame_entries WHERE tenant_id=$1 AND run_id=$2")
            .bind(tenant.as_str()).bind(*run_id.as_uuid()).fetch_one(&mut **tx).await
            .map_err(|e| StoreError::database("active frame lifetime inventory", e))?;
    let Some(stack) = stack else {
        if total != 0 {
            return Err(StoreError::corrupt("active frame stack missing"));
        }
        return Ok(None);
    };
    if !(1..=4096).contains(&stack.lifetime_starts)
        || total != i64::from(stack.lifetime_starts)
        || maximum != Some(stack.lifetime_starts)
    {
        return Err(StoreError::corrupt("active frame lifetime inventory"));
    }
    let namespace = GraphNamespace::new(stack.active_namespace.clone())
        .map_err(|_| StoreError::corrupt("active frame namespace"))?;
    if namespace.is_root() != stack.active_frame_identity_digest.is_none() {
        return Err(StoreError::corrupt("active frame identity shape"));
    }
    if !namespace.is_root()
        && !matches!(
            stored.lifecycle().status(),
            RunStatus::Active | RunStatus::Waiting | RunStatus::CancellationRequested
        )
    {
        return Err(StoreError::corrupt("active frame terminal Run"));
    }
    if let Some(closed) = closures::row(tx, tenant, run_id).await? {
        Box::pin(closures::verified_record(tx, &stored, closed)).await?;
        if !namespace.is_root() {
            return Err(StoreError::corrupt("closed Run retained active frame"));
        }
        return Ok(None);
    }
    let mut expected = Vec::with_capacity(7);
    if !namespace.is_root() {
        let mut prefix = String::new();
        for segment in namespace.as_str().split('/') {
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(segment);
            expected.push(prefix.clone());
        }
    }
    // Limit before materialization, including the overflow sentinel.
    let open: Vec<String> = query_scalar("SELECT e.graph_namespace FROM stateknot.graph_frame_entries e WHERE e.tenant_id=$1 AND e.run_id=$2 AND NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_returns x WHERE x.tenant_id=e.tenant_id AND x.run_id=e.run_id AND x.graph_namespace=e.graph_namespace) ORDER BY length(e.graph_namespace),e.graph_namespace LIMIT 8")
            .bind(tenant.as_str()).bind(*run_id.as_uuid()).fetch_all(&mut **tx).await
            .map_err(|e| StoreError::database("active frame open inventory", e))?;
    if open != expected {
        return Err(StoreError::corrupt("active frame open ancestry"));
    }
    let leaf = Box::pin(verify_chain(
        tx,
        tenant,
        run_id,
        stored,
        expected,
        true,
        decode_digest(&stack.admission_digest, "active frame admission")?,
    ))
    .await?;
    if leaf
        .as_ref()
        .map(|leaf| leaf.checkpoint.frame().digest().as_bytes().to_vec())
        != stack.active_frame_identity_digest
    {
        return Err(StoreError::corrupt("active frame leaf identity"));
    }
    Ok(leaf)
}

// Historical close reads use the same full entry/checkpoint/caller chain. Their
// exact completion facts are authenticated by the owning closure verifier.
#[allow(clippy::too_many_lines)]
pub(super) async fn verify_chain(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run_id: RunId,
    stored: StoredRun,
    expected: Vec<String>,
    require_open: bool,
    admission_digest: Digest,
) -> Result<Option<StoredActiveGraphFrame>, StoreError> {
    let admission = barriers::admission_snapshot(tx, tenant, run_id).await?;
    if admission_digest != admission.admission().digest()
        || serde_json_canonicalizer::to_vec(stored.lifecycle())
            .map_err(|_| StoreError::corrupt("active frame lifecycle"))?
            != serde_json_canonicalizer::to_vec(admission.run().lifecycle())
                .map_err(|_| StoreError::corrupt("active frame lifecycle"))?
        || stored.journal_head() != admission.run().journal_head()
    {
        return Err(StoreError::corrupt("active frame admission projection"));
    }
    let graphs = closure(tx, tenant, admission.admission().intent().graph()).await?;
    let root = load_locked_current_checkpoint(tx, &stored, tenant, run_id)
        .await?
        .ok_or_else(|| StoreError::corrupt("active frame Root checkpoint"))?;
    let mut parent = root.head();
    drop(root);
    let mut replay = Replay::new(tx);
    // Prime complete returned history before walking suspended ancestors.
    // Otherwise a current-parent read nests the entire return proof inside
    // checkpoint recovery. Only compact proofs survive in this transaction.
    let through = stored
        .journal_head()
        .ok_or_else(|| StoreError::corrupt("active frame journal head"))?
        .sequence()
        .get();
    Box::pin(returns::usage_floor_before(
        &mut replay,
        &admission,
        &graphs,
        through,
        &admission_usage_floor(&admission)?,
    ))
    .await?;
    let mut leaf = None;
    let mut open_frames = Vec::with_capacity(expected.len());
    for path in expected {
        let path = GraphNamespace::new(path)
            .map_err(|_| StoreError::corrupt("active frame ancestor namespace"))?;
        let entry = Box::pin(verified_entry_replay(
            &mut replay,
            tenant,
            run_id,
            &path,
            &admission,
            &graphs,
        ))
        .await?;
        if entry.entry.checkpoint().frame().origin().base_checkpoint() != &parent {
            return Err(StoreError::corrupt("active frame suspended parent head"));
        }
        let (checkpoint, floor) = Box::pin(barriers::current_checkpoint_replay(
            &mut replay,
            &entry,
            &admission,
            &graphs,
        ))
        .await?;
        let history = Box::pin(caller_bindings::verified_history_replay(
            &mut replay,
            &entry,
            &admission,
            &graphs,
            None,
        ))
        .await?;
        floor
            .validate_monotonic_after(&history.floor)
            .map_err(|_| StoreError::corrupt("active frame caller usage floor"))?;
        let completed: bool = query_scalar("SELECT EXISTS(SELECT 1 FROM stateknot.node_attempt_completions WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3)")
                .bind(tenant.as_str()).bind(*run_id.as_uuid()).bind(*history.current.attempt_id().as_uuid())
                .fetch_one(&mut **replay.tx).await.map_err(|e| StoreError::database("active frame caller completion", e))?;
        if require_open && completed {
            return Err(StoreError::corrupt("active frame caller already complete"));
        }
        parent = checkpoint.checkpoint().head();
        open_frames.push(StoredOpenGraphFrame {
            checkpoint: checkpoint.head(),
            entry_digest: entry.digest(),
            caller: history.current.head(),
            caller_binding_digest: history.digest,
        });
        leaf = Some(StoredActiveGraphFrame {
            run: stored.clone(),
            entry,
            checkpoint,
            caller: history.current,
            caller_binding_digest: history.digest,
            minimum_direct_usage: floor,
            open_frames: Vec::new(),
        });
    }
    if let Some(leaf) = &mut leaf {
        leaf.open_frames = open_frames;
    }
    Ok(leaf)
}
