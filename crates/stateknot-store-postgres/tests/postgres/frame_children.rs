// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Isolated children keep their real frame owner and the original Run account.
use super::frame_transactions::{admit, caller_graph, graphs, initial_usage};
use super::*;
use stateknot_core::{
    ChildRunAdmissionIntent, ChildRunDeclaration, ChildRunKey, ChildRunSlot,
    ChildRunTopologyLimits, GraphChildRunPolicy, GraphFrameEntryPlan, NodeAttemptStartHead,
    RunFence,
};
use stateknot_store_postgres::{ChildRunCommitOutcome, GraphFrameEntryCommitOutcome};

struct Fixture {
    admission: StoredAgentAdmission,
    intent: ChildRunAdmissionIntent,
    node: NodeAttemptStartHead,
    fence: RunFence,
    head: JournalHead,
    direct: BudgetUsage,
}

fn child_descriptor(graph: &CompiledGraph) -> AgentDescriptor {
    let template = agent_fixture_definition();
    AgentDescriptor::new(
        template.metadata().clone(),
        graph.input_schema().clone(),
        graph.output_schema().clone(),
        template.model().clone(),
        template.instructions().clone(),
        template.tools().clone(),
        template.execution().clone(),
        template.budget_limits().clone(),
    )
    .unwrap()
}

fn declared(graph: CompiledGraph, child: &CompiledGraph, agent: &AgentDescriptor) -> CompiledGraph {
    graph
        .with_child_runs(
            GraphChildRunPolicy::new(
                ChildRunTopologyLimits::new(2, 8, 4).unwrap(),
                [ChildRunDeclaration::new(
                    NodeId::new("finish").unwrap(),
                    ChildRunSlot::new("analysis").unwrap(),
                    agent,
                    child,
                )
                .unwrap()],
            )
            .unwrap(),
        )
        .unwrap()
}

#[allow(clippy::too_many_lines)]
async fn started(store: &PostgresStore, name: &str) -> Fixture {
    let tenant = tenant(name);
    let run = RunId::generate();
    let (_, template, _) = graphs();
    let isolated = CompiledGraph::compile(
        CapabilityIdentity::new(
            template.identity().owner().clone(),
            CapabilityReference::new("isolated-child".parse().unwrap(), Version::new(1, 0, 0)),
        ),
        template.input_schema().clone(),
        template.state_schema().clone(),
        template.update_schema().clone(),
        template.output_schema().clone(),
        template.reducer().clone(),
        template.entry_nodes().clone(),
        template.nodes().iter().cloned(),
        template.limits(),
    )
    .unwrap();
    let agent = child_descriptor(&isolated);
    let leaf = declared(template, &isolated, &agent);
    let root = declared(
        caller_graph("scoped-child-root", &leaf, 8),
        &isolated,
        &agent,
    );
    store
        .register_graph_definition(tenant.clone(), isolated.clone())
        .await
        .unwrap();
    let admission = Box::pin(admit(store, &tenant, run, &root, &leaf)).await;
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let call = &root.frame_calls().unwrap().calls()[0];
    let plan = GraphFrameEntryPlan::for_root(
        call,
        &root,
        admission.checkpoint(),
        &leaf,
        CheckpointId::generate(),
        AttemptId::generate(),
        fence.clone(),
    )
    .unwrap();
    let GraphFrameEntryCommitOutcome::Committed(entry) = Box::pin(store.enter_graph_frame(
        plan,
        EventId::generate(),
        admission.event().head(),
        initial_usage(&admission),
        &AcceptGraphSchemas,
    ))
    .await
    .unwrap() else {
        panic!("fresh frame entry");
    };
    let planner =
        ReadyNodeRecoveryPlanner::for_frame(entry.entry().checkpoint().clone(), fence.clone())
            .unwrap();
    let dispatch = planner
        .finish(entry.event().head(), entry.event().recorded_at())
        .unwrap();
    let start = Box::pin(store.start_recovered_graph_frame_node_attempt(
        worker_append(
            tenant.clone(),
            run,
            EventId::generate(),
            JournalExpectation::exact(entry.event().head()),
            fence.clone(),
            42_000,
        ),
        &dispatch,
        &NodeId::new("finish").unwrap(),
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    let node = start.attempt().start().head();
    let head = start.attempt().start().journal_head().clone();
    let parent = admission.admission().intent();
    let request = AgentRequest::new(
        isolated.input_schema().clone(),
        BoundedJson::try_from(json!({"count":1})).unwrap(),
        parent
            .request()
            .budget_limits()
            .clone()
            .with_event_bytes(ByteCount::new(1_048_576))
            .with_checkpoint_bytes(ByteCount::new(1_048_576))
            .with_graph_steps(stateknot_core::ExecutionCount::new(8))
            .with_input_bytes(ByteCount::new(512))
            .with_output_bytes(ByteCount::new(512)),
    );
    let child = AgentAdmissionIntent::new(
        AgentResultProvenance::for_agent(
            tenant.clone(),
            RunId::generate(),
            ThreadId::generate(),
            InvocationId::generate(),
            &agent,
        ),
        agent,
        request,
        parent.budget_layers().iter().cloned(),
        isolated.reference(),
        parent.authority().clone(),
    )
    .unwrap();
    let intent = ChildRunAdmissionIntent::for_frame(
        admission.admission(),
        entry.entry().checkpoint(),
        ChildRunKey::new(
            node.activation().clone(),
            ChildRunSlot::new("analysis").unwrap(),
        )
        .unwrap(),
        child,
        isolated.clone(),
        CheckpointState::new(
            isolated.state_schema().clone(),
            BoundedJson::try_from(json!({"count":1})).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    // This zero-I/O fixture has exactly three real events and two checkpoints.
    // Build the complete observation from those facts, not the structural floor.
    let events = store
        .load_journal_page(
            &tenant,
            run,
            None,
            stateknot_store_postgres::JournalPageSize::new(8).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(events.events().len(), 3);
    assert_eq!(events.events().last().unwrap().head(), head);
    let event_bytes: usize = events
        .events()
        .iter()
        .map(|event| serde_json_canonicalizer::to_vec(event).unwrap().len())
        .sum();
    let checkpoint_bytes = serde_json_canonicalizer::to_vec(admission.checkpoint())
        .unwrap()
        .len()
        + serde_json_canonicalizer::to_vec(entry.entry().checkpoint().checkpoint())
            .unwrap()
            .len()
        + serde_json_canonicalizer::to_vec(&entry.entry().checkpoint().head())
            .unwrap()
            .len();
    let direct = BudgetUsage::builder()
        .graph_depth(stateknot_core::ExecutionCount::new(2))
        .graph_steps(stateknot_core::ExecutionCount::new(2))
        .concurrent_branches(stateknot_core::ExecutionCount::new(1))
        .fan_out(stateknot_core::ExecutionCount::new(1))
        .event_bytes(ByteCount::new(event_bytes as u64))
        .checkpoint_bytes(ByteCount::new(checkpoint_bytes as u64))
        .input_bytes(ByteCount::new(
            serde_json_canonicalizer::to_vec(parent.request().input())
                .unwrap()
                .len() as u64,
        ))
        .build()
        .unwrap();
    Fixture {
        admission,
        intent,
        node,
        fence,
        head,
        direct,
    }
}

fn payload(kind: &str) -> JournalPayload {
    JournalPayload::new(
        checkpoint_schema("scoped-child-event"),
        kind.parse().unwrap(),
        BoundedJson::try_from(json!({"test":true})).unwrap(),
    )
    .unwrap()
}

async fn spawn(
    store: &PostgresStore,
    value: &Fixture,
) -> Result<ChildRunCommitOutcome, StoreError> {
    let key = value.intent.key();
    let child = value.intent.child().provenance();
    let parent_append = JournalAppend::new(
        JournalExpectation::exact(value.head.clone()),
        JournalEventIntent::worker(
            key.tenant_id().clone(),
            key.parent_run_id(),
            EventId::generate(),
            value.fence.clone(),
            payload("child-run-admitted"),
        )
        .unwrap(),
    )
    .unwrap();
    let child_append = JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(
            child.tenant_id().clone(),
            child.run_id(),
            EventId::generate(),
            payload(AgentAdmission::JOURNAL_EVENT_KIND),
        )
        .unwrap(),
    )
    .unwrap();
    let checkpoint = CheckpointWrite::initial(
        child.tenant_id().clone(),
        child.run_id(),
        CheckpointId::generate(),
        value.intent.child().graph().clone(),
        value.intent.initial_state().clone(),
        value.intent.child_graph().entry_nodes().clone(),
    )
    .unwrap();
    Box::pin(store.admit_child_run(
        value.intent.clone(),
        &value.node,
        parent_append,
        child_append,
        checkpoint,
        value.direct.clone(),
        &AcceptGraphSchemas,
    ))
    .await
}

#[tokio::test]
async fn scoped_child_admission_keeps_root_account_and_original_frame_owner() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = Box::pin(started(&store, "scoped-child-admission")).await;
    let committed = Box::pin(spawn(&store, &value)).await.unwrap();
    assert!(matches!(committed, ChildRunCommitOutcome::Committed(_)));
    let record = committed.record();
    assert_eq!(record.intent(), &value.intent);
    assert_eq!(record.ancestors(), &[value.intent.key().parent_run_id()]);
    let restored = store.load_child_run(value.intent.key()).await.unwrap();
    assert_eq!(restored.spawn(), record.spawn());
    let account = store
        .load_child_budget_account(
            value.intent.key().tenant_id(),
            value.intent.key().parent_run_id(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 1);
    assert_eq!(account.children()[0].key(), value.intent.key());
    let root = store
        .load_current_checkpoint(
            value.intent.key().tenant_id(),
            value.intent.key().parent_run_id(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(root, *value.admission.checkpoint());
    store.close().await;
}
