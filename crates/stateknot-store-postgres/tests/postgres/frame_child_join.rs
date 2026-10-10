// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Publication, actual higher-fence dispatch, consumption and retained ownership.
use super::*;
use stateknot_core::{ChildRunJoinRequest, ExecutionCount};
use stateknot_store_postgres::{ChildJoinCommitOutcome, GraphFrameReturnCommitOutcome};

async fn control_append(store: &PostgresStore, value: &Fixture, kind: &str) -> JournalAppend {
    let run = store
        .load_run(value.fence.tenant_id(), value.fence.run_id())
        .await
        .unwrap();
    JournalAppend::new(
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(
            value.fence.tenant_id().clone(),
            value.fence.run_id(),
            EventId::generate(),
            payload(kind),
        )
        .unwrap(),
    )
    .unwrap()
}

async fn settle(store: &PostgresStore, value: &Fixture, child: &StoredAgentAdmission) {
    let provenance = child.admission().intent().provenance();
    let run = store
        .load_run(provenance.tenant_id(), provenance.run_id())
        .await
        .unwrap();
    let event = EventId::generate();
    let at = store.observe_database_clock().await.unwrap();
    // The admitted child has performed no node/provider I/O. Its actual initial
    // event, checkpoint and request input are the complete prior observation.
    let usage = initial_usage(child)
        .checked_accumulate(
            &BudgetUsage::builder()
                .input_bytes(ByteCount::new(
                    serde_json_canonicalizer::to_vec(child.admission().intent().request().input())
                        .unwrap()
                        .len() as u64,
                ))
                .build()
                .unwrap(),
        )
        .unwrap();
    let failure =
        RunFailure::new(terminal_run_failure(event, at).failure().clone(), at, usage).unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(
            provenance.tenant_id().clone(),
            provenance.run_id(),
            event,
            payload("test-failed"),
        )
        .unwrap(),
    )
    .unwrap();
    store
        .append_control_plane(
            append,
            RunProjection::transition(run.lifecycle().revision(), RunTransition::Fail { failure }),
        )
        .await
        .unwrap();
    let append = control_append(store, value, "child-run-settled").await;
    Box::pin(store.settle_child_run(value.intent.key(), append.clone()))
        .await
        .unwrap();
    assert!(matches!(
        Box::pin(store.settle_child_run(value.intent.key(), append))
            .await
            .unwrap(),
        stateknot_store_postgres::ChildRunSettlementOutcome::Idempotent { .. }
    ));
}

async fn resume(
    store: &PostgresStore,
    value: &Fixture,
    fence: &RunFence,
) -> stateknot_core::NodeAttempt {
    let activation = value.intent.key().parent();
    let history = store
        .load_node_attempt_history_page(
            activation,
            None,
            stateknot_store_postgres::NodeAttemptHistoryPageSize::new(2).unwrap(),
        )
        .await
        .unwrap();
    assert!(!history.has_more());
    assert_eq!(history.records().len(), 1);
    assert_eq!(history.records()[0].start().head(), value.node);
    let mut planner =
        ReadyNodeRecoveryPlanner::for_frame(value.frame.clone(), fence.clone()).unwrap();
    planner.observe_attempt(&history.records()[0]).unwrap();
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let dispatch = planner
        .finish(
            run.journal_head().unwrap().clone(),
            store.observe_database_clock().await.unwrap(),
        )
        .unwrap();
    assert_eq!(
        dispatch.nodes()[0].dispatch_reason(),
        Some(NodeDispatchReason::SupersededAttempt)
    );
    let started = Box::pin(store.start_recovered_graph_frame_node_attempt(
        worker_append(
            fence.tenant_id().clone(),
            fence.run_id(),
            EventId::generate(),
            JournalExpectation::exact(run.journal_head().unwrap().clone()),
            fence.clone(),
            42_003,
        ),
        &dispatch,
        activation.node_id(),
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        started,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    started.attempt().clone()
}

// This fixture has one unchanged frame checkpoint, no provider/tool work and
// no other Run work. Count every actual parent journal event; child Run bytes
// and input are delegated charges, never added to DIRECT a second time.
async fn direct(
    store: &PostgresStore,
    value: &Fixture,
    starts: u64,
    output: &BudgetUsage,
) -> BudgetUsage {
    let journal = store
        .load_journal_page(
            value.fence.tenant_id(),
            value.fence.run_id(),
            None,
            stateknot_store_postgres::JournalPageSize::new(32).unwrap(),
        )
        .await
        .unwrap();
    assert!(!journal.has_more());
    let bytes: usize = journal
        .events()
        .iter()
        .map(|event| serde_json_canonicalizer::to_vec(event).unwrap().len())
        .sum();
    BudgetUsage::builder()
        .graph_depth(ExecutionCount::new(2))
        .graph_steps(ExecutionCount::new(starts))
        // Both activations first started under the original fence. Each actual
        // successor physical start is one retry, including caller rebinding.
        .retries(ExecutionCount::new(starts - 2))
        .concurrent_branches(ExecutionCount::new(1))
        .fan_out(ExecutionCount::new(1))
        .event_bytes(ByteCount::new(bytes as u64))
        .checkpoint_bytes(value.direct.checkpoint_bytes())
        .input_bytes(value.direct.input_bytes())
        .build()
        .unwrap()
        .checked_accumulate(output)
        .unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_join_consumes_original_publication_under_a_higher_fence() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = Box::pin(started(&store, "scoped-child-join-consume")).await;
    let spawned = Box::pin(spawn(&store, &value)).await.unwrap();
    let request = ChildRunJoinRequest::new([value.intent.key().clone()]).unwrap();
    Box::pin(store.register_child_join(
        request.clone(),
        &value.node,
        join_append(&value, &spawned.record().spawn().head()),
    ))
    .await
    .unwrap();
    let append = control_append(&store, &value, "child-join-published").await;
    assert!(matches!(
        Box::pin(store.publish_child_join(&request, append)).await,
        Err(StoreError::ChildRunSettlementUnavailable)
    ));
    Box::pin(settle(&store, &value, spawned.record().child())).await;
    let append = control_append(&store, &value, "child-join-published").await;
    let published = Box::pin(store.publish_child_join(&request, append.clone()))
        .await
        .unwrap();
    let head = published.record().head().unwrap().clone();
    assert!(matches!(
        Box::pin(store.publish_child_join(&request, append))
            .await
            .unwrap(),
        ChildJoinCommitOutcome::Idempotent(_)
    ));
    let fence = store
        .claim_lease(
            value.fence.tenant_id(),
            value.fence.run_id(),
            AttemptId::generate(),
        )
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    assert!(fence.epoch() > value.fence.epoch());
    let node = Box::pin(resume(&store, &value, &fence)).await;
    let without_join = PendingNodeResultIntent::new(
        node.start().activation().clone(),
        NodeStateChange::Unchanged,
        super::super::frame_barriers::terminal(&value.graph),
        NodeInvocationBindings::empty(),
    )
    .unwrap();
    let usage = BudgetUsage::builder()
        .output_bytes(ByteCount::new(
            serde_json_canonicalizer::to_vec(&json!({"count":7,"source":"terminal"}))
                .unwrap()
                .len() as u64,
        ))
        .build()
        .unwrap();
    let append = worker_append(
        fence.tenant_id().clone(),
        fence.run_id(),
        EventId::generate(),
        JournalExpectation::exact(node.start().journal_head().clone()),
        fence.clone(),
        42_004,
    );
    assert!(matches!(
        Box::pin(store.succeed_node_attempt(
            append.clone(),
            &node.start().head(),
            without_join.clone(),
            usage.clone()
        ))
        .await,
        Err(StoreError::ChildJoinRejected)
    ));
    let intent = without_join.with_child_join(head.clone()).unwrap();
    let completed = Box::pin(store.succeed_node_attempt(
        append.clone(),
        &node.start().head(),
        intent.clone(),
        usage.clone(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        completed,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        Box::pin(store.succeed_node_attempt(append, &node.start().head(), intent, usage.clone()))
            .await
            .unwrap(),
        NodeAttemptCommitOutcome::Idempotent { .. }
    ));
    let joined = Box::pin(store.load_child_join(value.intent.key().parent()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(joined.head(), Some(&head));
    assert_eq!(joined.consumed(), Some(&completed.event().head()));
    let account = store
        .load_child_budget_account(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 1);
    assert!(account.children()[0].settlement().is_some());
    let result = store
        .load_pending_node_result(value.intent.key().parent())
        .await
        .unwrap();
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(
        store
            .load_current_checkpoint(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .as_ref(),
        Some(value.admission.checkpoint())
    );
    let observation = Box::pin(direct(&store, &value, 3, &usage)).await;
    let rebound = Box::pin(store.rebind_graph_frame_caller(
        value.frame.frame(),
        AttemptId::generate(),
        EventId::generate(),
        fence.clone(),
        run.journal_head().unwrap().clone(),
        observation,
    ))
    .await
    .unwrap();
    assert!(matches!(
        rebound,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    let plan = super::super::frame_barriers::plan(&value.graph, &value.frame, &result);
    let observation = Box::pin(direct(&store, &value, 4, &usage)).await;
    let barrier = Box::pin(super::super::frame_barriers::commit(
        &store,
        plan.clone(),
        &fence,
        observation,
        &value.graph,
    ))
    .await;
    let returned = Box::pin(store.return_graph_frame(
        plan,
        EventId::generate(),
        fence.clone(),
        barrier.event().head(),
        barrier.direct_usage_after().unwrap(),
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: value.graph.reducer().clone(),
        },
    ))
    .await
    .unwrap();
    assert!(matches!(
        returned,
        GraphFrameReturnCommitOutcome::Committed(_)
    ));
    assert!(
        store
            .load_active_graph_frame(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .is_none()
    );
    // Historical ownership must survive the frame's Return and checkpoint move.
    assert_eq!(
        store
            .load_child_run(value.intent.key())
            .await
            .unwrap()
            .spawn(),
        spawned.record().spawn()
    );
    assert_eq!(
        Box::pin(store.load_child_join(value.intent.key().parent()))
            .await
            .unwrap()
            .unwrap()
            .head(),
        Some(&head)
    );
    store.close().await;
}
