// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Fixture-specific accounting: recover the one mock model node's actual durable
// completion. Never invent a model turn to satisfy AgentResult validation.
pub(super) struct Evidence {
    store: PostgresStore,
    pub mode: AtomicUsize,
    pub entered: AtomicUsize,
}
impl GraphLifecycleEvidenceProvider for Evidence {
    fn terminal_evidence(
        &self,
        context: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async move {
            self.entered.fetch_add(1, Ordering::SeqCst);
            match self.mode.load(Ordering::SeqCst) {
                1 => std::future::pending::<()>().await,
                2 => panic!("fixture lifecycle evidence panic"),
                3 => return Err(GraphLifecycleEvidenceError::TemporarilyUnavailable),
                _ => {}
            }
            let stored = self
                .store
                .load_agent_admission(
                    context.provenance().tenant_id(),
                    context.provenance().run_id(),
                )
                .await
                .unwrap();
            let intent = stored.admission().intent();
            assert_eq!(&stored.checkpoint().head(), context.checkpoint());
            let activation =
                NodeActivation::for_ready_root(stored.checkpoint(), NodeId::new("finish").unwrap())
                    .unwrap();
            let history = self
                .store
                .load_node_attempt_history_page(
                    &activation,
                    None,
                    NodeAttemptHistoryPageSize::new(2).unwrap(),
                )
                .await
                .unwrap();
            assert!(!history.has_more());
            assert_eq!(history.records().len(), 1);
            let completion = history.records()[0].completion().unwrap();
            assert_eq!(completion.status(), NodeAttemptStatus::Succeeded);
            Ok(GraphTerminalEvidence::new(
                intent.descriptor().clone(),
                intent.request().clone(),
                intent.budget().clone(),
                AgentArtifacts::empty(),
                completion.usage().clone(),
            ))
        })
    }
    fn failure_evidence(
        &self,
        _: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async { Err(GraphLifecycleEvidenceError::Unavailable) })
    }
    fn cancellation_evidence(
        &self,
        _: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async { Ok(GraphCancellationEvidence::new(BudgetUsage::zero())) })
    }
}
pub(super) fn evidence(f: &Fixture) -> Arc<Evidence> {
    Arc::new(Evidence {
        store: f.store.clone(),
        mode: AtomicUsize::new(0),
        entered: AtomicUsize::new(0),
    })
}
