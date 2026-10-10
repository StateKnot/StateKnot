// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Whole-history application replay; transaction ownership never crosses callbacks.
#[allow(clippy::wildcard_imports)]
use super::*;
use stateknot_core::{BoxFuture, GraphBarrierDisposition, GraphFrameBarrierPlan};

const MAX_FRAMES: usize = 4096;
const MAX_MANIFEST_BYTES: usize = 64 * 1024 * 1024;
const TRANSCRIPT_DOMAIN: &[u8] = b"stateknot-scoped-application-replay-v1\0";

/// Evidence for one exact claimed observation after whole-history replay.
///
/// This ephemeral report grants no dispatch or portable execution authority.
/// Root histories without a complete disposition proof return an operational
/// error instead of this report. RFC-0022 and the nested Driver remain experimental.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphRecoveryReplayReport {
    journal_head: JournalHead,
    leaf: Option<GraphFrameCheckpointHead>,
    root_replay: GraphReplayReport,
    frame_checkpoints_validated: u64,
    barriers_replayed: u64,
    results_replayed: u64,
    frames_validated: u64,
    returns_validated: u64,
    waits_validated: u64,
    maximum_barrier_result_bytes: usize,
    manifest_bytes: usize,
    structural_edges_verified: u64,
}

impl GraphRecoveryReplayReport {
    /// Returns the original exact journal observation revalidated after callbacks.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }
    /// Returns the exact active leaf; `None` means authenticated Root scope.
    #[must_use]
    pub const fn leaf(&self) -> Option<&GraphFrameCheckpointHead> {
        self.leaf.as_ref()
    }
    /// Returns all Root checkpoints, including the initial snapshot.
    #[must_use]
    pub const fn root_checkpoints_validated(&self) -> u64 {
        self.root_replay.checkpoints_validated()
    }
    /// Returns the Root counters measured by this same whole-history replay.
    ///
    /// This copy supports existing Root report consumers without repeating
    /// application callbacks or including any scoped frame counters.
    #[must_use]
    pub const fn root_report(&self) -> GraphReplayReport {
        self.root_replay
    }
    /// Returns all lifetime frame checkpoints, including every initial snapshot.
    #[must_use]
    pub const fn frame_checkpoints_validated(&self) -> u64 {
        self.frame_checkpoints_validated
    }
    /// Returns every transition independently planned with its committed ID.
    #[must_use]
    pub const fn barriers_replayed(&self) -> u64 {
        self.barriers_replayed
    }
    /// Returns every exact result consumed by those application transitions.
    #[must_use]
    pub const fn results_replayed(&self) -> u64 {
        self.results_replayed
    }
    /// Returns all lifetime frame entries, including returned and suspended frames.
    #[must_use]
    pub const fn frames_validated(&self) -> u64 {
        self.frames_validated
    }
    /// Returns complete settlements independently compared with actual terminal plans.
    #[must_use]
    pub const fn returns_validated(&self) -> u64 {
        self.returns_validated
    }
    /// Returns scoped wait barriers with complete registration/terminal proofs.
    #[must_use]
    pub const fn waits_validated(&self) -> u64 {
        self.waits_validated
    }
    /// Returns the largest compact result set retained for one barrier.
    #[must_use]
    pub const fn maximum_barrier_result_bytes(&self) -> usize {
        self.maximum_barrier_result_bytes
    }
    /// Returns measured canonical bytes for the compact lifetime inventory.
    /// Graph definitions, state readers and one transient result have separate bounds.
    #[must_use]
    pub const fn manifest_bytes(&self) -> usize {
        self.manifest_bytes
    }
    /// Returns actual full structural edge visits plus direct materializations.
    /// Capture/application/final passes retain no per-edge cache across callbacks.
    #[must_use]
    pub const fn structural_edges_verified(&self) -> u64 {
        self.structural_edges_verified
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Frame {
    namespace: GraphNamespace,
    ordinal: u16,
    initial: GraphFrameCheckpointHead,
    head: GraphFrameCheckpointHead,
    entry_digest: Digest,
    returned: Option<(u64, Digest, PendingNodeResultHead)>,
}

struct Manifest {
    root_initial: CheckpointHead,
    root_head: CheckpointHead,
    frames: Vec<Frame>,
    returns: BTreeMap<u64, (Digest, PendingNodeResultHead)>,
    graphs: BTreeMap<Digest, CompiledGraph>,
    admission: Option<StoredAgentAdmission>,
    bytes: usize,
    structural_edges: u64,
}

/// Constant-memory, ordered, domain-separated, length-prefixed digest chain.
/// Only canonical heads and component digests enter it, never application values.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Transcript {
    digest: Digest,
    checkpoints: u64,
    barriers: u64,
    results: u64,
    returns: u64,
    waits: u64,
}
impl Transcript {
    fn new() -> Self {
        Self {
            digest: Digest::sha256(TRANSCRIPT_DOMAIN),
            checkpoints: 0,
            barriers: 0,
            results: 0,
            returns: 0,
            waits: 0,
        }
    }
    fn bind(&mut self, tag: &'static [u8], value: &impl Serialize) -> Result<(), StoreError> {
        let bytes = serde_json_canonicalizer::to_vec(value)
            .map_err(|_| StoreError::corrupt("application replay transcript encoding"))?;
        let mut preimage = Vec::with_capacity(32 + 16 + tag.len() + bytes.len());
        preimage.extend_from_slice(self.digest.as_bytes());
        preimage.extend_from_slice(
            &u64::try_from(tag.len())
                .map_err(|_| StoreError::GraphReplayResourceLimit)?
                .to_be_bytes(),
        );
        preimage.extend_from_slice(tag);
        preimage.extend_from_slice(
            &u64::try_from(bytes.len())
                .map_err(|_| StoreError::GraphReplayResourceLimit)?
                .to_be_bytes(),
        );
        preimage.extend_from_slice(&bytes);
        self.digest = Digest::sha256(preimage);
        Ok(())
    }
}
fn count(counter: &mut u64, delta: u64) -> Result<(), StoreError> {
    *counter = counter
        .checked_add(delta)
        .ok_or(StoreError::GraphReplayResourceLimit)?;
    Ok(())
}

impl ClaimedRunRecovery<'_> {
    /// Replays Root and every lifetime frame against exact executable dependencies.
    ///
    /// Capture and final structural passes use transaction-owned prefix proofs.
    /// Application materialization authenticates one direct edge per transaction;
    /// schemas, resolver and reducers run after that transaction closes. Complete
    /// successor, scoped disposition and return intents are compared, followed
    /// by the original journal/Root/leaf/fence/database-clock recheck. No event,
    /// result, checkpoint or application node is created by this read.
    ///
    /// Legacy Root Continue edges authenticate the exact no-op lifecycle/barrier
    /// projection. Legacy Root Wait/Terminal lack an independent disposition
    /// bundle and fail closed with `GraphReplayHistoricalDispositionUnavailable`.
    /// A larger local result limit never supplies a missing durable proof.
    ///
    /// # Errors
    /// Returns actual authority/staleness/database errors; local limits and
    /// unavailable dependencies do not quarantine. Durable contradictions are
    /// quarantined only with this session's original exact context and fence.
    pub fn validate_graph_replay<'a, 'dependencies: 'a, V, F>(
        &'a self,
        expected_leaf: Option<&GraphFrameCheckpointHead>,
        schemas: &'a V,
        resolve_reducer: F,
        limits: GraphReplayLimits,
    ) -> BoxFuture<'a, Result<GraphRecoveryReplayReport, StoreError>>
    where
        V: GraphSchemaValidator + ?Sized,
        F: Fn(&GraphReference) -> Option<&'dependencies (dyn GraphReducer + 'static)>
            + Send
            + Sync
            + 'a,
    {
        let expected_leaf = expected_leaf.cloned();
        Box::pin(async move {
            if let Some(head) = expected_leaf.as_ref() {
                self.validate_checkpoint_scope(head.checkpoint())?;
            }
            Box::pin(self.read(Box::pin(self.validate_graph_replay_inner(
                expected_leaf.as_ref(),
                schemas,
                &resolve_reducer,
                limits,
            ))))
            .await
        })
    }

    // Keep the capture/application/final ownership sequence explicit in one body.
    #[allow(clippy::too_many_lines)]
    async fn validate_graph_replay_inner<'dependencies, V, F>(
        &self,
        expected: Option<&GraphFrameCheckpointHead>,
        schemas: &V,
        resolve: &F,
        limits: GraphReplayLimits,
    ) -> Result<GraphRecoveryReplayReport, StoreError>
    where
        V: GraphSchemaValidator + ?Sized,
        F: Fn(&GraphReference) -> Option<&'dependencies (dyn GraphReducer + 'static)> + Send + Sync,
    {
        let mut tx = self
            .store
            .begin_repeatable_read("application replay capture")
            .await?;
        let manifest = Box::pin(capture(self, &mut tx, expected, limits)).await?;
        let captured = Box::pin(capture_transcript(&mut tx, &manifest, limits)).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::database("application replay capture commit", e))?;

        let mut observed = Transcript::new();
        let mut completed_returns = BTreeMap::new();
        let mut maximum_bytes = 0;
        let mut frame_checkpoints = 0;
        for frame in &manifest.frames {
            let mut tx = self
                .store
                .begin_repeatable_read("application replay entry")
                .await?;
            Box::pin(recheck(self, &mut tx, expected)).await?;
            Box::pin(verify_frozen_head(&mut tx, frame)).await?;
            let (entry, parent) = Box::pin(direct_entry(&mut tx, frame, &manifest.graphs)).await?;
            observed.bind(
                b"entry",
                &(
                    entry.digest(),
                    entry.entry().checkpoint().head(),
                    parent.checkpoint().head(),
                ),
            )?;
            let mut base = entry.entry().checkpoint().clone();
            tx.commit()
                .await
                .map_err(|e| StoreError::database("application replay entry commit", e))?;
            let graph = exact_graph(&manifest.graphs, base.checkpoint().graph())?;
            let reducer = exact_reducer(resolve, graph)?;
            validate_graph_replay_checkpoint_state(graph, base.checkpoint(), schemas)?;
            validate_value(
                schemas,
                graph.input_schema(),
                base.checkpoint().state().data(),
            )?;
            count(&mut observed.checkpoints, 1)?;
            count(&mut frame_checkpoints, 1)?;
            let mut terminal: Option<(GraphFrameCheckpoint, GraphFrameBarrierPlan)> = None;
            while base.head() != frame.head {
                let mut tx = self
                    .store
                    .begin_repeatable_read("application replay scoped edge")
                    .await?;
                Box::pin(recheck(self, &mut tx, expected)).await?;
                Box::pin(verify_frozen_head(&mut tx, frame)).await?;
                let barrier = Box::pin(barriers::application_edge(
                    &mut tx,
                    &entry,
                    &base,
                    self.initial_run.lifecycle().revision(),
                ))
                .await?;
                let (results, bytes) = Box::pin(direct_results(
                    &mut tx,
                    base.checkpoint(),
                    Some(&base),
                    barrier.checkpoint().checkpoint(),
                    barrier.barrier().result_heads(),
                    &completed_returns,
                    limits,
                ))
                .await?;
                bind_edge(
                    &mut observed,
                    base.checkpoint(),
                    Some(&barrier),
                    barrier.checkpoint().checkpoint(),
                    &results,
                )?;
                Box::pin(bind_waits(&mut tx, &barrier, &mut observed)).await?;
                tx.commit().await.map_err(|e| {
                    StoreError::database("application replay scoped edge commit", e)
                })?;
                maximum_bytes = maximum_bytes.max(bytes);
                let plan = catch_unwind(AssertUnwindSafe(|| {
                    graph.plan_frame_barrier(
                        &base,
                        &results,
                        barrier.checkpoint().checkpoint().checkpoint_id(),
                        schemas,
                        reducer,
                    )
                }))
                .map_err(|_| StoreError::GraphReplayDependencyUnavailable)?
                .map_err(|e| map_graph_replay_plan_error(&e))?;
                if plan.barrier() != barrier.barrier()
                    || plan.disposition() != &barrier.disposition()
                    || !barrier
                        .checkpoint()
                        .checkpoint()
                        .matches_write(plan.barrier().successor())
                {
                    return Err(StoreError::corrupt(
                        "application replay scoped successor/disposition",
                    ));
                }
                validate_graph_replay_checkpoint_state(
                    graph,
                    barrier.checkpoint().checkpoint(),
                    schemas,
                )?;
                count(&mut frame_checkpoints, 1)?;
                if plan.disposition().terminal_output().is_some() {
                    terminal = Some((base.clone(), plan));
                }
                base = barrier.checkpoint().clone();
                if base.checkpoint().superstep() > frame.head.checkpoint().superstep() {
                    return Err(StoreError::corrupt("application replay scoped coverage"));
                }
            }
            let mut tx = self
                .store
                .begin_repeatable_read("application replay return")
                .await?;
            Box::pin(recheck(self, &mut tx, expected)).await?;
            let returned = Box::pin(returns::application_return(
                &mut tx,
                self.fence.tenant_id(),
                self.fence.run_id(),
                &frame.namespace,
            ))
            .await?;
            bind_return(&mut observed, frame, returned.as_ref())?;
            tx.commit()
                .await
                .map_err(|e| StoreError::database("application replay return commit", e))?;
            if let Some(returned) = returned {
                let (terminal_base, terminal_plan) = terminal.as_ref().ok_or_else(|| {
                    StoreError::corrupt("application replay returned terminal missing")
                })?;
                if returned.terminal_checkpoint() != &base.head() {
                    return Err(StoreError::corrupt("application replay returned head"));
                }
                let parent_graph = exact_graph(&manifest.graphs, parent.checkpoint().graph())?;
                let call = parent_graph
                    .frame_calls()
                    .and_then(|calls| calls.call(base.frame().origin().node_id()))
                    .ok_or_else(|| StoreError::corrupt("application replay return declaration"))?;
                let intent = match &parent {
                    Parent::Root(cp) => call.prepare_root_return(
                        parent_graph,
                        cp,
                        graph,
                        terminal_base,
                        terminal_plan,
                    ),
                    Parent::Frame(cp) => call.prepare_frame_return(
                        parent_graph,
                        cp,
                        graph,
                        terminal_base,
                        terminal_plan,
                    ),
                }
                .map_err(|_| StoreError::corrupt("application replay return intent"))?;
                if returned.result().intent() != &intent {
                    return Err(StoreError::corrupt(
                        "application replay return intent mismatch",
                    ));
                }
                completed_returns.insert(
                    returned.event().sequence().get(),
                    (returned.digest(), returned.result().head()),
                );
            }
        }
        let root_replay = Box::pin(self.replay_root(
            &manifest,
            expected,
            schemas,
            resolve,
            limits,
            &completed_returns,
            &mut observed,
            &mut maximum_bytes,
        ))
        .await?;
        if observed != captured || completed_returns != manifest.returns {
            return Err(StoreError::corrupt(
                "application replay transcript coverage",
            ));
        }
        let mut tx = self
            .store
            .begin_repeatable_read("application replay final verification")
            .await?;
        let after = Box::pin(capture(self, &mut tx, expected, limits)).await?;
        if after.root_initial != manifest.root_initial
            || after.root_head != manifest.root_head
            || after.frames != manifest.frames
            || after.returns != manifest.returns
            || after.bytes != manifest.bytes
        {
            return Err(StoreError::corrupt(
                "application replay immutable inventory changed",
            ));
        }
        let final_transcript = Box::pin(capture_transcript(&mut tx, &after, limits)).await?;
        if final_transcript != captured {
            return Err(StoreError::corrupt("application replay final transcript"));
        }
        Box::pin(recheck(self, &mut tx, expected)).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::database("application replay final verification commit", e))?;
        Ok(GraphRecoveryReplayReport {
            journal_head: self
                .context
                .expectation()
                .head()
                .cloned()
                .ok_or(StoreError::StaleClaimedRunRecoveryObservation)?,
            leaf: expected.cloned(),
            root_replay,
            frame_checkpoints_validated: frame_checkpoints,
            barriers_replayed: observed.barriers,
            results_replayed: observed.results,
            frames_validated: u64::try_from(manifest.frames.len())
                .map_err(|_| StoreError::GraphReplayResourceLimit)?,
            returns_validated: observed.returns,
            waits_validated: observed.waits,
            maximum_barrier_result_bytes: maximum_bytes,
            manifest_bytes: manifest.bytes,
            structural_edges_verified: manifest
                .structural_edges
                .checked_add(after.structural_edges)
                .and_then(|edges| {
                    observed
                        .barriers
                        .checked_mul(3)
                        .and_then(|direct| edges.checked_add(direct))
                })
                .ok_or(StoreError::GraphReplayResourceLimit)?,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn replay_root<'dependencies, V, F>(
        &self,
        manifest: &Manifest,
        expected: Option<&GraphFrameCheckpointHead>,
        schemas: &V,
        resolve: &F,
        limits: GraphReplayLimits,
        returns: &BTreeMap<u64, (Digest, PendingNodeResultHead)>,
        transcript: &mut Transcript,
        maximum_bytes: &mut usize,
    ) -> Result<GraphReplayReport, StoreError>
    where
        V: GraphSchemaValidator + ?Sized,
        F: Fn(&GraphReference) -> Option<&'dependencies (dyn GraphReducer + 'static)> + Send + Sync,
    {
        let mut tx = self
            .store
            .begin_repeatable_read("application replay Root initial")
            .await?;
        Box::pin(recheck(self, &mut tx, expected)).await?;
        let mut base = Box::pin(root_checkpoint(&mut tx, &manifest.root_initial)).await?;
        transcript.bind(b"root-initial", &base.head())?;
        tx.commit()
            .await
            .map_err(|e| StoreError::database("application replay Root initial commit", e))?;
        let graph = exact_graph(&manifest.graphs, base.graph())?;
        let reducer = exact_reducer(resolve, graph)?;
        validate_graph_replay_checkpoint_state(graph, &base, schemas)?;
        count(&mut transcript.checkpoints, 1)?;
        let mut report = GraphReplayReport {
            checkpoints_validated: 1,
            barriers_replayed: 0,
            results_replayed: 0,
            maximum_barrier_result_bytes: 0,
        };
        while base.head() != manifest.root_head {
            let mut tx = self
                .store
                .begin_repeatable_read("application replay Root edge")
                .await?;
            Box::pin(recheck(self, &mut tx, expected)).await?;
            let edge = Box::pin(root_edge(&mut tx, &base, returns, limits)).await?;
            bind_edge(transcript, &base, None, &edge.successor, &edge.results)?;
            transcript.bind(b"root-projection", &edge.projection)?;
            tx.commit()
                .await
                .map_err(|e| StoreError::database("application replay Root edge commit", e))?;
            *maximum_bytes = (*maximum_bytes).max(edge.bytes);
            // Authenticate the legacy compatibility boundary before comparing
            // an application successor. The generic control API could legally
            // commit another projection and did not persist its full plan.
            if edge.projection
                != Some(barrier_projection_digest(
                    &RunProjection::unchanged(),
                    edge.barrier.intent_digest(),
                )?)
                || edge.results.iter().any(|result| {
                    matches!(
                        result.intent().control().kind(),
                        NodeControlKind::Wait | NodeControlKind::Terminal
                    )
                })
            {
                return Err(StoreError::GraphReplayHistoricalDispositionUnavailable);
            }
            let plan = catch_unwind(AssertUnwindSafe(|| {
                graph.plan_barrier(
                    &base,
                    &edge.results,
                    edge.successor.checkpoint_id(),
                    schemas,
                    reducer,
                )
            }))
            .map_err(|_| StoreError::GraphReplayDependencyUnavailable)?
            .map_err(|e| map_graph_replay_plan_error(&e))?;
            if plan.disposition() != &GraphBarrierDisposition::Continue {
                return Err(StoreError::GraphReplayHistoricalDispositionUnavailable);
            }
            if plan.barrier() != &edge.barrier
                || !edge.successor.matches_write(plan.barrier().successor())
            {
                return Err(StoreError::corrupt("application replay Root successor"));
            }
            validate_graph_replay_checkpoint_state(graph, &edge.successor, schemas)?;
            count(&mut report.checkpoints_validated, 1)?;
            count(&mut report.barriers_replayed, 1)?;
            count(
                &mut report.results_replayed,
                u64::try_from(edge.results.len())
                    .map_err(|_| StoreError::GraphReplayResourceLimit)?,
            )?;
            report.maximum_barrier_result_bytes =
                report.maximum_barrier_result_bytes.max(edge.bytes);
            base = edge.successor;
            if base.superstep() > manifest.root_head.superstep() {
                return Err(StoreError::corrupt("application replay Root coverage"));
            }
        }
        Ok(report)
    }
}

fn exact_graph<'a>(
    graphs: &'a BTreeMap<Digest, CompiledGraph>,
    reference: &GraphReference,
) -> Result<&'a CompiledGraph, StoreError> {
    let graph = graphs
        .get(&reference.definition_digest())
        .filter(|graph| graph.reference() == *reference)
        .ok_or_else(|| StoreError::corrupt("application replay exact graph pin"))?;
    Ok(graph)
}
fn exact_reducer<'a, F>(
    resolve: &F,
    graph: &CompiledGraph,
) -> Result<&'a (dyn GraphReducer + 'static), StoreError>
where
    F: Fn(&GraphReference) -> Option<&'a (dyn GraphReducer + 'static)>,
{
    match catch_unwind(AssertUnwindSafe(|| {
        resolve(&graph.reference()).filter(|reducer| reducer.reference() == graph.reducer())
    })) {
        Ok(Some(reducer)) => Ok(reducer),
        _ => Err(StoreError::GraphReplayDependencyUnavailable),
    }
}
fn validate_value<V: GraphSchemaValidator + ?Sized>(
    schemas: &V,
    schema: &stateknot_core::SchemaReference,
    value: &BoundedJson,
) -> Result<(), StoreError> {
    match catch_unwind(AssertUnwindSafe(|| schemas.validate(schema, value))) {
        Err(_) | Ok(Err(GraphSchemaValidationError::Unavailable)) => {
            Err(StoreError::GraphReplayDependencyUnavailable)
        }
        Ok(Err(_)) => Err(StoreError::corrupt("application replay entry schema")),
        Ok(Ok(())) => Ok(()),
    }
}

async fn recheck(
    session: &ClaimedRunRecovery<'_>,
    tx: &mut Transaction<'_, Postgres>,
    expected: Option<&GraphFrameCheckpointHead>,
) -> Result<(), StoreError> {
    let stored = decode_run(
        query_as::<_, RunRow>(SELECT_RUN)
            .bind(session.fence.tenant_id().as_str())
            .bind(*session.fence.run_id().as_uuid())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| StoreError::database("application replay claimed observation", e))?
            .ok_or(StoreError::RunNotFound)?,
    )?;
    validate_runnable(&stored)?;
    let now = database_now(tx, "application replay authority clock").await?;
    authorize_worker(&stored, &session.fence, now)?;
    if stored.journal_head() != session.context.expectation().head() {
        return Err(StoreError::StaleClaimedRunRecoveryObservation);
    }
    // A matching journal cannot substitute for the original lifecycle and
    // scheduler/wait projection. Ordinary renewal may change lease times while
    // retaining the same authorized fence; those times are checked above.
    let original = &session.initial_run;
    if encode_lifecycle(stored.lifecycle())? != encode_lifecycle(original.lifecycle())?
        || stored.scheduler_ready_at() != original.scheduler_ready_at()
        || stored.scheduler_not_before() != original.scheduler_not_before()
        || stored.wait_set_digest() != original.wait_set_digest()
        || stored.unresolved_wait_count() != original.unresolved_wait_count()
        || stored.next_timer_due_at() != original.next_timer_due_at()
        || stored.next_interrupt_expiry_at() != original.next_interrupt_expiry_at()
    {
        return Err(StoreError::StaleClaimedRunRecoveryObservation);
    }
    if stored.checkpoint() != session.initial_run.checkpoint() {
        return Err(StoreError::StaleCheckpointHead);
    }
    let stack = query_as::<_, (String, Option<Vec<u8>>)>("SELECT active_namespace,active_frame_identity_digest FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2").bind(session.fence.tenant_id().as_str()).bind(*session.fence.run_id().as_uuid()).fetch_optional(&mut **tx).await.map_err(|e| StoreError::database("application replay active observation", e))?;
    match (expected, stack) {
        (None, None) => Ok(()),
        (None, Some((namespace, None))) if namespace.is_empty() => Ok(()),
        (Some(head), Some((namespace, Some(identity))))
            if namespace == head.frame().namespace().as_str()
                && identity.as_slice() == head.frame().digest().as_bytes() =>
        {
            let cp = decode_frame_checkpoint(
                checkpoint_row(
                    tx,
                    session.fence.tenant_id(),
                    session.fence.run_id(),
                    head.frame().namespace(),
                    head.checkpoint().checkpoint_id(),
                )
                .await?,
            )?;
            if cp.head() != *head {
                return Err(StoreError::StaleCheckpointHead);
            }
            let actual = query_as::<_, (Uuid, i64, Vec<u8>, Vec<u8>)>("SELECT checkpoint_id,superstep,checkpoint_digest,frame_checkpoint_digest FROM stateknot.graph_frame_heads WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3").bind(session.fence.tenant_id().as_str()).bind(*session.fence.run_id().as_uuid()).bind(namespace).fetch_optional(&mut **tx).await.map_err(|e| StoreError::database("application replay active head", e))?.ok_or_else(|| StoreError::corrupt("application replay active head missing"))?;
            if actual.0 != *head.checkpoint().checkpoint_id().as_uuid()
                || nonnegative_superstep(actual.1)? != head.checkpoint().superstep()
                || decode_digest(&actual.2, "application replay head checkpoint")?
                    != head.checkpoint().digest()
                || decode_digest(&actual.3, "application replay head frame")? != head.digest()
            {
                return Err(StoreError::StaleCheckpointHead);
            }
            Ok(())
        }
        _ => Err(StoreError::StaleCheckpointHead),
    }
}

async fn verify_frozen_head(
    tx: &mut Transaction<'_, Postgres>,
    frame: &Frame,
) -> Result<(), StoreError> {
    let head = &frame.head;
    let row = query_as::<_, (Uuid, i64, Vec<u8>, Vec<u8>)>("SELECT checkpoint_id,superstep,checkpoint_digest,frame_checkpoint_digest FROM stateknot.graph_frame_heads WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3").bind(head.checkpoint().tenant_id().as_str()).bind(*head.checkpoint().run_id().as_uuid()).bind(frame.namespace.as_str()).fetch_optional(&mut **tx).await.map_err(|e| StoreError::database("application replay frozen frame head", e))?.ok_or_else(|| StoreError::corrupt("application replay frozen frame head missing"))?;
    if row.0 != *head.checkpoint().checkpoint_id().as_uuid()
        || nonnegative_superstep(row.1)? != head.checkpoint().superstep()
        || decode_digest(&row.2, "application replay frozen checkpoint")?
            != head.checkpoint().digest()
        || decode_digest(&row.3, "application replay frozen frame")? != head.digest()
    {
        return Err(StoreError::corrupt("application replay frozen frame head"));
    }
    Ok(())
}

async fn root_checkpoint(
    tx: &mut Transaction<'_, Postgres>,
    head: &CheckpointHead,
) -> Result<Checkpoint, StoreError> {
    let cp = decode_checkpoint(
        checkpoint_row(
            tx,
            head.tenant_id(),
            head.run_id(),
            &GraphNamespace::root(),
            head.checkpoint_id(),
        )
        .await?,
    )?;
    if cp.head() != *head {
        return Err(StoreError::corrupt("application replay Root head"));
    }
    verify_checkpoint_anchor(tx, &cp).await?;
    Ok(cp)
}

#[allow(clippy::too_many_lines)]
async fn capture(
    session: &ClaimedRunRecovery<'_>,
    tx: &mut Transaction<'_, Postgres>,
    expected: Option<&GraphFrameCheckpointHead>,
    limits: GraphReplayLimits,
) -> Result<Manifest, StoreError> {
    Box::pin(recheck(session, tx, expected)).await?;
    let tenant = session.fence.tenant_id();
    let run = session.fence.run_id();
    // The look-ahead contains only small identity columns. Decode no full row 4097.
    let inventory = query_as::<_, (String, i32)>("SELECT graph_namespace,ordinal FROM stateknot.graph_frame_entries WHERE tenant_id=$1 AND run_id=$2 ORDER BY ordinal LIMIT 4097").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_all(&mut **tx).await.map_err(|e| StoreError::database("application replay lifetime inventory", e))?;
    if inventory.len() > MAX_FRAMES {
        return Err(StoreError::GraphReplayResourceLimit);
    }
    for (index, (_, ordinal)) in inventory.iter().enumerate() {
        if usize::try_from(*ordinal).ok() != Some(index + 1) {
            return Err(StoreError::corrupt("application replay lifetime ordinal"));
        }
    }
    let run_row = query_as::<_, RunRow>(SELECT_RUN)
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| StoreError::database("application replay Run capture", e))?;
    let stored = decode_run(run_row)?;
    let root = load_locked_current_checkpoint(tx, &stored, tenant, run)
        .await?
        .ok_or(StoreError::ReadyNodeRecoveryCheckpointMissing)?;
    let root_head = root.head();
    let root_graph = graph(tx, tenant, root.graph()).await?;
    let graphs = closure(tx, tenant, root.graph()).await?;
    let admission = match load_agent_admission_row(tx, tenant, run).await? {
        Some(row) => Some(verify_stored_agent_admission(tx, stored.clone(), row).await?),
        None if inventory.is_empty() => None,
        None => {
            return Err(StoreError::corrupt(
                "application replay frame admission missing",
            ));
        }
    };
    let stack = query_as::<_, (Vec<u8>, i32, String, Option<Vec<u8>>)>("SELECT admission_digest,lifetime_starts,active_namespace,active_frame_identity_digest FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_optional(&mut **tx).await.map_err(|e| StoreError::database("application replay complete stack inventory", e))?;
    if let Some((digest, lifetime, _, _)) = &stack {
        if admission
            .as_ref()
            .map(|admission| admission.admission().digest())
            != Some(decode_digest(digest, "application replay stack admission")?)
            || usize::try_from(*lifetime).ok() != Some(inventory.len())
        {
            return Err(StoreError::corrupt(
                "application replay lifetime stack projection",
            ));
        }
    } else if !inventory.is_empty() {
        return Err(StoreError::corrupt(
            "application replay missing lifetime stack",
        ));
    }
    let open = query_scalar::<_, String>("SELECT e.graph_namespace FROM stateknot.graph_frame_entries e WHERE e.tenant_id=$1 AND e.run_id=$2 AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_returns r WHERE r.tenant_id=e.tenant_id AND r.run_id=e.run_id AND r.graph_namespace=e.graph_namespace) ORDER BY e.graph_namespace LIMIT 8").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_all(&mut **tx).await.map_err(|e| StoreError::database("application replay compact open inventory", e))?;
    let expected_open: Vec<String> = expected.map_or_else(Vec::new, |head| {
        let mut path = String::new();
        head.frame()
            .namespace()
            .as_str()
            .split('/')
            .map(|part| {
                if !path.is_empty() {
                    path.push('/');
                }
                path.push_str(part);
                path.clone()
            })
            .collect()
    });
    if open.len() > 7 || open != expected_open {
        return Err(StoreError::corrupt("application replay hidden open frame"));
    }
    let initial_id = query_scalar::<_, Uuid>("SELECT checkpoint_id FROM stateknot.run_checkpoints WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace='' AND superstep=0").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_optional(&mut **tx).await.map_err(|e| StoreError::database("application replay Root initial", e))?.ok_or_else(|| StoreError::corrupt("application replay Root initial missing"))?;
    let initial = decode_checkpoint(
        checkpoint_row(
            tx,
            tenant,
            run,
            &GraphNamespace::root(),
            CheckpointId::from_uuid(initial_id)
                .map_err(|_| StoreError::corrupt("application replay Root initial ID"))?,
        )
        .await?,
    )?;
    if initial.parent().is_some()
        || initial.superstep() != Superstep::INITIAL
        || initial.graph() != root.graph()
        || initial.ready_nodes() != root_graph.entry_nodes()
    {
        return Err(StoreError::corrupt("application replay Root initial shape"));
    }
    verify_checkpoint_anchor(tx, &initial).await?;
    if root.superstep().get() > root_graph.limits().maximum_supersteps().get() {
        return Err(StoreError::corrupt("application replay Root step bound"));
    }
    let root_count = query_scalar::<_, i64>("SELECT count(*) FROM stateknot.run_checkpoints WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=''").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(&mut **tx).await.map_err(|e| StoreError::database("application replay Root inventory count", e))?;
    if u64::try_from(root_count).ok() != root.superstep().get().checked_add(1) {
        return Err(StoreError::corrupt(
            "application replay hidden Root checkpoint",
        ));
    }
    let mut frames = Vec::with_capacity(inventory.len());
    let mut returned = BTreeMap::new();
    let mut bytes = 0_usize;
    let mut structural_edges = 0;
    let mut steps = root.superstep().get();
    if let Some(admission) = &admission {
        steps = steps
            .checked_add(
                u64::try_from(inventory.len()).map_err(|_| StoreError::GraphReplayResourceLimit)?,
            )
            .ok_or(StoreError::GraphReplayResourceLimit)?;
        if steps > admission.admission().intent().budget().graph_steps().get() {
            return Err(StoreError::corrupt(
                "application replay admitted graph step bound",
            ));
        }
        let mut replay = Replay::with_limits(tx, limits);
        Box::pin(returns::usage_floor_before(
            &mut replay,
            admission,
            &graphs,
            stored
                .journal_head()
                .ok_or(StoreError::StaleJournalHead)?
                .sequence()
                .get(),
            &admission_usage_floor(admission)?,
        ))
        .await?;
        for (path, ordinal) in inventory {
            let namespace = GraphNamespace::new(path)
                .map_err(|_| StoreError::corrupt("application replay namespace"))?;
            let entry = Box::pin(verified_entry_replay(
                &mut replay,
                tenant,
                run,
                &namespace,
                admission,
                &graphs,
            ))
            .await?;
            let (head, _) = Box::pin(barriers::current_checkpoint_replay(
                &mut replay,
                &entry,
                admission,
                &graphs,
            ))
            .await?;
            let history = Box::pin(caller_bindings::verified_history_replay(
                &mut replay,
                &entry,
                admission,
                &graphs,
                None,
            ))
            .await?;
            if open.contains(&namespace.as_str().to_owned()) {
                let completed = query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM stateknot.node_attempt_completions WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3)").bind(tenant.as_str()).bind(*run.as_uuid()).bind(*history.current.attempt_id().as_uuid()).fetch_one(&mut **replay.tx).await.map_err(|e| StoreError::database("application replay open caller completion", e))?;
                if completed {
                    return Err(StoreError::corrupt(
                        "application replay completed open caller",
                    ));
                }
            }
            steps = steps
                .checked_add(head.checkpoint().superstep().get())
                .ok_or(StoreError::GraphReplayResourceLimit)?;
            if steps > admission.admission().intent().budget().graph_steps().get() {
                return Err(StoreError::corrupt(
                    "application replay admitted graph step bound",
                ));
            }
            let row_count = query_as::<_, (i64, i64)>("SELECT (SELECT count(*) FROM stateknot.run_checkpoints WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3),(SELECT count(*) FROM stateknot.graph_frame_barriers WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3)").bind(tenant.as_str()).bind(*run.as_uuid()).bind(namespace.as_str()).fetch_one(&mut **replay.tx).await.map_err(|e| StoreError::database("application replay scoped inventory count", e))?;
            if u64::try_from(row_count.0).ok() != head.checkpoint().superstep().get().checked_add(1)
                || u64::try_from(row_count.1).ok() != Some(head.checkpoint().superstep().get())
            {
                return Err(StoreError::corrupt(
                    "application replay hidden scoped history",
                ));
            }
            let settlement = Box::pin(returns::application_return(
                replay.tx, tenant, run, &namespace,
            ))
            .await?;
            let settlement = settlement.map(|record| {
                (
                    record.event().sequence().get(),
                    record.digest(),
                    record.result().head(),
                )
            });
            if let Some((sequence, digest, result)) = &settlement {
                if returned
                    .insert(*sequence, (*digest, result.clone()))
                    .is_some()
                {
                    return Err(StoreError::corrupt(
                        "application replay duplicate settlement",
                    ));
                }
                steps = steps
                    .checked_add(1)
                    .ok_or(StoreError::GraphReplayResourceLimit)?;
                if steps > admission.admission().intent().budget().graph_steps().get() {
                    return Err(StoreError::corrupt(
                        "application replay admitted settlement step bound",
                    ));
                }
            }
            let frame = Frame {
                namespace,
                ordinal: u16::try_from(ordinal)
                    .map_err(|_| StoreError::corrupt("application replay ordinal"))?,
                initial: entry.entry().checkpoint().head(),
                head: head.head(),
                entry_digest: entry.digest(),
                returned: settlement,
            };
            let mut counter = CompactByteCounter::default();
            serde_json::to_writer(
                &mut counter,
                &(
                    &frame.namespace,
                    frame.ordinal,
                    &frame.initial,
                    &frame.head,
                    frame.entry_digest,
                    &frame.returned,
                ),
            )
            .map_err(|_| StoreError::GraphReplayResourceLimit)?;
            bytes = bytes
                .checked_add(counter.bytes)
                .ok_or(StoreError::GraphReplayResourceLimit)?;
            if bytes > MAX_MANIFEST_BYTES {
                return Err(StoreError::GraphReplayResourceLimit);
            }
            frames.push(frame);
        }
        structural_edges = replay.edge_visits();
    }
    let mut parent = root.head();
    for path in &open {
        let frame = frames
            .iter()
            .find(|frame| frame.namespace.as_str() == path)
            .ok_or_else(|| StoreError::corrupt("application replay open frame inventory"))?;
        if frame.initial.frame().origin().base_checkpoint() != &parent {
            return Err(StoreError::corrupt(
                "application replay suspended parent head",
            ));
        }
        parent = frame.head.checkpoint().clone();
    }
    if expected.is_some_and(|head| head.checkpoint() != &parent) {
        return Err(StoreError::StaleCheckpointHead);
    }
    // Postorder releases child payloads before parents. Sibling lifetime entries
    // remain independent, and a stable ordinal breaks ties without payloads.
    frames.sort_by(|left, right| {
        right
            .namespace
            .as_str()
            .split('/')
            .count()
            .cmp(&left.namespace.as_str().split('/').count())
            .then_with(|| left.ordinal.cmp(&right.ordinal))
    });
    let return_count = query_scalar::<_, i64>(
        "SELECT count(*) FROM stateknot.graph_frame_returns WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(tenant.as_str())
    .bind(*run.as_uuid())
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| StoreError::database("application replay return inventory count", e))?;
    if usize::try_from(return_count).ok() != Some(returned.len()) {
        return Err(StoreError::corrupt("application replay omitted return"));
    }
    let extra = query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM stateknot.run_checkpoints cp WHERE cp.tenant_id=$1 AND cp.run_id=$2 AND cp.graph_namespace<>'' AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=cp.tenant_id AND e.run_id=cp.run_id AND e.graph_namespace=cp.graph_namespace))").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(&mut **tx).await.map_err(|e| StoreError::database("application replay orphan checkpoint proof", e))?;
    if extra {
        return Err(StoreError::corrupt(
            "application replay orphan scoped checkpoint",
        ));
    }
    let mut counter = CompactByteCounter::default();
    serde_json::to_writer(&mut counter, &(&returned, initial.head(), &root_head))
        .map_err(|_| StoreError::GraphReplayResourceLimit)?;
    bytes = bytes
        .checked_add(counter.bytes)
        .ok_or(StoreError::GraphReplayResourceLimit)?;
    if bytes > MAX_MANIFEST_BYTES {
        return Err(StoreError::GraphReplayResourceLimit);
    }
    Ok(Manifest {
        root_initial: initial.head(),
        root_head,
        frames,
        returns: returned,
        graphs,
        admission,
        bytes,
        structural_edges,
    })
}

#[allow(clippy::too_many_lines)]
async fn direct_entry(
    tx: &mut Transaction<'_, Postgres>,
    frame: &Frame,
    graphs: &BTreeMap<Digest, CompiledGraph>,
) -> Result<(StoredGraphFrameEntry, Parent), StoreError> {
    let tenant = frame.head.checkpoint().tenant_id();
    let run = frame.head.checkpoint().run_id();
    let row = entry_row(tx, tenant, run, &frame.namespace)
        .await?
        .ok_or_else(|| StoreError::corrupt("application replay entry missing"))?;
    let wire = decode_entry_wire(&row)?;
    if wire.initial_checkpoint != frame.initial || wire.compound_digest != frame.entry_digest {
        return Err(StoreError::corrupt("application replay captured entry"));
    }
    let origin = wire.initial_checkpoint.frame().origin();
    let parent_row = checkpoint_row(
        tx,
        tenant,
        run,
        origin.graph_namespace(),
        origin.base_checkpoint().checkpoint_id(),
    )
    .await?;
    let parent = if origin.graph_namespace().is_root() {
        let cp = decode_checkpoint(parent_row)?;
        verify_checkpoint_anchor(tx, &cp).await?;
        Parent::Root(Box::new(cp))
    } else {
        Parent::Frame(Box::new(decode_frame_checkpoint(parent_row)?))
    };
    if parent.checkpoint().head() != *origin.base_checkpoint() {
        return Err(StoreError::corrupt(
            "application replay entry parent snapshot",
        ));
    }
    let (event, projection) =
        child_runs::anchored_event(tx, tenant, run, row.journal_sequence).await?;
    let (before, _) = child_runs::anchored_event(
        tx,
        tenant,
        run,
        row.journal_sequence
            .checked_sub(1)
            .ok_or_else(|| StoreError::corrupt("application replay entry predecessor"))?,
    )
    .await?;
    let start = load_node_attempt_record(
        tx,
        tenant,
        &run,
        AttemptId::from_uuid(row.caller_attempt_id)
            .map_err(|_| StoreError::corrupt("application replay entry caller ID"))?,
    )
    .await?
    .ok_or_else(|| StoreError::corrupt("application replay entry caller"))?;
    returns::verify_completion_anchor(tx, &start).await?;
    let cp = decode_frame_checkpoint(
        checkpoint_row(
            tx,
            tenant,
            run,
            &frame.namespace,
            wire.initial_checkpoint.checkpoint().checkpoint_id(),
        )
        .await?,
    )?;
    let plan = parent.prepare(
        exact_graph(graphs, parent.checkpoint().graph())?,
        exact_graph(graphs, cp.checkpoint().graph())?,
        cp.frame(),
        cp.checkpoint().checkpoint_id(),
        start.start().attempt_id(),
        start.start().fence().clone(),
    )?;
    let entry = plan
        .verify_committed(
            &before.head(),
            &event,
            start.start(),
            &cp,
            wire.core_record_digest,
        )
        .map_err(|_| StoreError::corrupt("application replay entry whole Core proof"))?;
    if event.event_id().as_uuid() != &row.journal_event_id
        || event.recorded_at() != from_database_time(row.journal_recorded_at)?
        || event.digest() != decode_digest(&row.journal_digest, "application replay entry journal")?
        || projection != Some(wire.compound_digest)
        || wire.core_intent_digest != plan.intent_digest()
        || wire.scope_intent_digest != scope_intent(&plan, &wire.scope, &wire.budget)?
        || wire.compound_digest != compound_digest(wire.scope_intent_digest, entry.digest())?
        || wire.budget.observed_head != before.head()
    {
        return Err(StoreError::corrupt("application replay entry compound"));
    }
    Ok((
        StoredGraphFrameEntry {
            event,
            entry,
            scope: wire.scope,
            budget: wire.budget,
            digest: wire.compound_digest,
        },
        parent,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn direct_results(
    tx: &mut Transaction<'_, Postgres>,
    base: &Checkpoint,
    frame: Option<&GraphFrameCheckpoint>,
    successor: &Checkpoint,
    heads: &[PendingNodeResultHead],
    returned: &BTreeMap<u64, (Digest, PendingNodeResultHead)>,
    limits: GraphReplayLimits,
) -> Result<(Vec<PendingNodeResult>, usize), StoreError> {
    let durable = load_locked_barrier_result_heads(tx, &base.head()).await?;
    validate_complete_barrier_result_heads(&durable, heads)
        .map_err(map_graph_replay_consumption_error)?;
    if heads.len() > ReadyNodes::MAX_LEN {
        return Err(StoreError::GraphReplayResourceLimit);
    }
    let mut results = Vec::with_capacity(heads.len());
    let mut bytes = 0_usize;
    for head in heads {
        let canonical = match frame {
            Some(frame) => frame
                .activation(head.activation().node_id().clone())
                .map_err(|_| StoreError::corrupt("application replay result activation"))?,
            None => NodeActivation::for_ready_root(base, head.activation().node_id().clone())
                .map_err(|_| StoreError::corrupt("application replay result activation"))?,
        };
        if &canonical != head.activation() {
            return Err(StoreError::corrupt("application replay result scope"));
        }
        let row = load_pending_node_result_row(tx, &canonical)
            .await?
            .ok_or_else(|| StoreError::corrupt("application replay result missing"))?;
        let result = decode_pending_node_result(&row)?;
        if result.head() != *head
            || result.journal_head().sequence() >= successor.journal_head().sequence()
        {
            return Err(StoreError::corrupt("application replay result head/order"));
        }
        let (event, _) = child_runs::anchored_event(
            tx,
            canonical.tenant_id(),
            canonical.run_id(),
            i64::try_from(result.journal_head().sequence().get())
                .map_err(|_| StoreError::GraphReplayResourceLimit)?,
        )
        .await?;
        if event.payload().kind().as_str() == "graph-frame-returned" {
            let expected = returned.get(&event.sequence().get()).ok_or_else(|| {
                StoreError::corrupt("application replay returned child not validated")
            })?;
            let namespace = query_scalar::<_, String>("SELECT graph_namespace FROM stateknot.graph_frame_returns WHERE tenant_id=$1 AND run_id=$2 AND journal_sequence=$3").bind(canonical.tenant_id().as_str()).bind(*canonical.run_id().as_uuid()).bind(i64::try_from(event.sequence().get()).map_err(|_| StoreError::GraphReplayResourceLimit)?).fetch_optional(&mut **tx).await.map_err(|e| StoreError::database("application replay returned result witness", e))?.ok_or_else(|| StoreError::corrupt("application replay returned result owner"))?;
            let namespace = GraphNamespace::new(namespace)
                .map_err(|_| StoreError::corrupt("application replay returned namespace"))?;
            let record = Box::pin(returns::application_return(
                tx,
                canonical.tenant_id(),
                canonical.run_id(),
                &namespace,
            ))
            .await?
            .ok_or_else(|| StoreError::corrupt("application replay returned result witness"))?;
            if record.digest() != expected.0
                || record.result().head() != expected.1
                || record.result() != &result
            {
                return Err(StoreError::corrupt(
                    "application replay returned result substitution",
                ));
            }
        } else {
            Box::pin(verify_ordinary_pending_node_result_components(tx, &result)).await?;
        }
        let mut counter = CompactByteCounter::default();
        serde_json::to_writer(&mut counter, &result)
            .map_err(|_| StoreError::corrupt("application replay result encoding"))?;
        bytes = bytes
            .checked_add(counter.bytes)
            .ok_or(StoreError::GraphReplayResourceLimit)?;
        if bytes > limits.maximum_barrier_result_bytes() {
            return Err(StoreError::GraphReplayResourceLimit);
        }
        results.push(result);
    }
    verify_barrier_consumption_parts(tx, &base.head(), heads, successor)
        .await
        .map_err(map_graph_replay_consumption_error)?;
    Ok((results, bytes))
}

struct RootEdge {
    barrier: CheckpointBarrier,
    successor: Checkpoint,
    results: Vec<PendingNodeResult>,
    bytes: usize,
    projection: Option<Digest>,
}
async fn root_edge(
    tx: &mut Transaction<'_, Postgres>,
    base: &Checkpoint,
    returned: &BTreeMap<u64, (Digest, PendingNodeResultHead)>,
    limits: GraphReplayLimits,
) -> Result<RootEdge, StoreError> {
    let next = query_scalar::<_, Uuid>("SELECT checkpoint_id FROM stateknot.run_checkpoints WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace='' AND parent_checkpoint_id=$3").bind(base.tenant_id().as_str()).bind(*base.run_id().as_uuid()).bind(*base.checkpoint_id().as_uuid()).fetch_optional(&mut **tx).await.map_err(|e| StoreError::database("application replay Root successor", e))?.ok_or_else(|| StoreError::corrupt("application replay Root edge missing"))?;
    let successor = decode_checkpoint(
        checkpoint_row(
            tx,
            base.tenant_id(),
            base.run_id(),
            &GraphNamespace::root(),
            CheckpointId::from_uuid(next)
                .map_err(|_| StoreError::corrupt("application replay Root successor ID"))?,
        )
        .await?,
    )?;
    if successor.parent() != Some(&base.head()) {
        return Err(StoreError::corrupt("application replay Root lineage"));
    }
    verify_checkpoint_anchor(tx, &successor).await?;
    let (_, projection) = child_runs::anchored_event(
        tx,
        base.tenant_id(),
        base.run_id(),
        i64::try_from(successor.journal_head().sequence().get())
            .map_err(|_| StoreError::GraphReplayResourceLimit)?,
    )
    .await?;
    let heads = load_locked_barrier_result_heads(tx, &base.head()).await?;
    let (results, bytes) = Box::pin(direct_results(
        tx, base, None, &successor, &heads, returned, limits,
    ))
    .await?;
    let write = CheckpointWrite::successor(
        successor.checkpoint_id(),
        base,
        successor.state().clone(),
        successor.ready_nodes().clone(),
    )
    .map_err(|_| StoreError::corrupt("application replay Root committed write"))?;
    let barrier = CheckpointBarrier::new(base, write, heads.iter().cloned())
        .map_err(|_| StoreError::corrupt("application replay Root committed barrier"))?;
    Ok(RootEdge {
        barrier,
        successor,
        results,
        bytes,
        projection,
    })
}

fn bind_edge(
    transcript: &mut Transcript,
    base: &Checkpoint,
    frame: Option<&StoredGraphFrameBarrier>,
    successor: &Checkpoint,
    results: &[PendingNodeResult],
) -> Result<(), StoreError> {
    transcript.bind(
        b"edge",
        &(
            base.head(),
            successor.head(),
            frame.map(StoredGraphFrameBarrier::digest),
            frame.and_then(StoredGraphFrameBarrier::wait_revision),
        ),
    )?;
    for result in results {
        transcript.bind(b"result", &result.head())?;
        count(&mut transcript.results, 1)?;
    }
    count(&mut transcript.barriers, 1)?;
    count(&mut transcript.checkpoints, 1)
}
async fn bind_waits(
    tx: &mut Transaction<'_, Postgres>,
    barrier: &StoredGraphFrameBarrier,
    transcript: &mut Transcript,
) -> Result<(), StoreError> {
    if barrier.disposition().waits().is_none() {
        return Ok(());
    }
    // Hash complete status/terminal projections server-side, retaining compact
    // digests only. Same server/canonical encoding is used in all three passes.
    let digests = query_as::<_, (Uuid, Vec<u8>)>("SELECT wait_id,sha256(convert_to(row_to_json(w)::text,'UTF8')) FROM stateknot.run_wait_registrations w WHERE tenant_id=$1 AND run_id=$2 AND registration_sequence=$3 ORDER BY wait_id").bind(barrier.event().tenant_id().as_str()).bind(*barrier.event().run_id().as_uuid()).bind(i64::try_from(barrier.event().sequence().get()).map_err(|_| StoreError::GraphReplayResourceLimit)?).fetch_all(&mut **tx).await.map_err(|e| StoreError::database("application replay wait transcript", e))?;
    for (id, digest) in digests {
        transcript.bind(
            b"wait-terminal",
            &(
                id.to_string(),
                decode_digest(&digest, "application replay wait transcript digest")?,
            ),
        )?;
    }
    count(&mut transcript.waits, 1)
}
fn bind_return(
    transcript: &mut Transcript,
    frame: &Frame,
    record: Option<&StoredGraphFrameReturn>,
) -> Result<(), StoreError> {
    match (&frame.returned, record) {
        (None, None) => Ok(()),
        (Some((sequence, digest, result)), Some(record))
            if *sequence == record.event().sequence().get()
                && *digest == record.digest()
                && *result == record.result().head() =>
        {
            transcript.bind(
                b"return",
                &(
                    record.digest(),
                    record.terminal_checkpoint(),
                    record.result().head(),
                    record.completion().digest(),
                ),
            )?;
            count(&mut transcript.returns, 1)
        }
        _ => Err(StoreError::corrupt(
            "application replay settlement inventory",
        )),
    }
}

async fn capture_transcript(
    tx: &mut Transaction<'_, Postgres>,
    manifest: &Manifest,
    limits: GraphReplayLimits,
) -> Result<Transcript, StoreError> {
    let mut transcript = Transcript::new();
    for frame in &manifest.frames {
        let (entry, parent) = Box::pin(direct_entry(tx, frame, &manifest.graphs)).await?;
        transcript.bind(
            b"entry",
            &(
                entry.digest(),
                entry.entry().checkpoint().head(),
                parent.checkpoint().head(),
            ),
        )?;
        count(&mut transcript.checkpoints, 1)?;
        let mut base = entry.entry().checkpoint().clone();
        while base.head() != frame.head {
            let revision = manifest
                .admission
                .as_ref()
                .ok_or_else(|| StoreError::corrupt("application replay scoped admission"))?
                .run()
                .lifecycle()
                .revision();
            let barrier = Box::pin(barriers::application_edge(tx, &entry, &base, revision)).await?;
            let (results, _) = Box::pin(direct_results(
                tx,
                base.checkpoint(),
                Some(&base),
                barrier.checkpoint().checkpoint(),
                barrier.barrier().result_heads(),
                &manifest.returns,
                limits,
            ))
            .await?;
            bind_edge(
                &mut transcript,
                base.checkpoint(),
                Some(&barrier),
                barrier.checkpoint().checkpoint(),
                &results,
            )?;
            Box::pin(bind_waits(tx, &barrier, &mut transcript)).await?;
            base = barrier.checkpoint().clone();
            if base.checkpoint().superstep() > frame.head.checkpoint().superstep() {
                return Err(StoreError::corrupt(
                    "application replay scoped transcript coverage",
                ));
            }
        }
        let returned = Box::pin(returns::application_return(
            tx,
            frame.head.checkpoint().tenant_id(),
            frame.head.checkpoint().run_id(),
            &frame.namespace,
        ))
        .await?;
        bind_return(&mut transcript, frame, returned.as_ref())?;
    }
    let mut base = Box::pin(root_checkpoint(tx, &manifest.root_initial)).await?;
    transcript.bind(b"root-initial", &base.head())?;
    count(&mut transcript.checkpoints, 1)?;
    while base.head() != manifest.root_head {
        let edge = Box::pin(root_edge(tx, &base, &manifest.returns, limits)).await?;
        bind_edge(&mut transcript, &base, None, &edge.successor, &edge.results)?;
        transcript.bind(b"root-projection", &edge.projection)?;
        base = edge.successor;
        if base.superstep() > manifest.root_head.superstep() {
            return Err(StoreError::corrupt(
                "application replay Root transcript coverage",
            ));
        }
    }
    Ok(transcript)
}
