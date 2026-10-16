// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Recovery of genuine absolute Wait results after their original due instant.

#[path = "elapsed_graph_wait_races.rs"]
mod races;

use super::*;
use std::sync::OnceLock;

struct AbsoluteWaitExecutor {
    store: PostgresStore,
    graph: GraphReference,
    node_id: NodeId,
    timer_id: TimerId,
    offset_micros: i64,
    calls: Arc<AtomicUsize>,
    original: Arc<OnceLock<(Timestamp, Timestamp, NodeActivation)>>,
}

impl GraphNodeExecutor for AbsoluteWaitExecutor {
    fn graph(&self) -> &GraphReference {
        &self.graph
    }

    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        Box::pin(async move {
            assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
            let observed = self
                .store
                .observe_live_lease(context.attempt().fence())
                .await
                .unwrap()
                .observed_at();
            let due =
                Timestamp::from_unix_micros(observed.unix_micros() + self.offset_micros).unwrap();
            self.original
                .set((observed, due, context.attempt().activation().clone()))
                .unwrap();
            Ok(GraphNodeExecution::new(
                NodeStateChange::Unchanged,
                NodeControl::Wait {
                    waits: NodeWaits::try_new([NodeWait::timer(
                        self.timer_id,
                        RunTimerKind::Sleep,
                        due,
                    )])
                    .unwrap(),
                },
                NodeInvocationBindings::empty(),
                // This application node performs no external I/O. The complete
                // DIRECT adapter independently authenticates nonzero storage
                // and graph charges for every actual durable event.
                BudgetUsage::zero(),
            ))
        })
    }
}

fn absolute_wait_fixture(
    store: &PostgresStore,
    scoped: bool,
    timer_id: TimerId,
    original: &Arc<OnceLock<(Timestamp, Timestamp, NodeActivation)>>,
) -> NestedFixture {
    absolute_wait_fixture_with_offset(store, scoped, timer_id, original, 500_000)
}

fn absolute_wait_fixture_with_offset(
    store: &PostgresStore,
    scoped: bool,
    timer_id: TimerId,
    original: &Arc<OnceLock<(Timestamp, Timestamp, NodeActivation)>>,
    offset_micros: i64,
) -> NestedFixture {
    let ordinary = driver_fixture();
    let mut fixture = if scoped {
        let waits = NodeWaits::try_new([NodeWait::timer(
            timer_id,
            RunTimerKind::Sleep,
            Timestamp::MAX,
        )])
        .unwrap();
        nested_fixture_with_behavior(1, &TestNodeBehavior::Wait(waits))
    } else {
        let graph = CompiledGraph::compile(
            capability("absolute-root-wait"),
            ordinary.graph.input_schema().clone(),
            ordinary.graph.state_schema().clone(),
            ordinary.graph.update_schema().clone(),
            ordinary.graph.output_schema().clone(),
            ordinary.graph.reducer().clone(),
            ordinary.graph.entry_nodes().clone(),
            [
                GraphNode::new(
                    NodeId::new("Step_A").unwrap(),
                    None,
                    GraphRoutes::empty(),
                    Some(ReadyNodes::try_new([NodeId::new("Step.B").unwrap()]).unwrap()),
                    false,
                )
                .unwrap(),
                GraphNode::new(
                    NodeId::new("Step.B").unwrap(),
                    None,
                    GraphRoutes::empty(),
                    None,
                    true,
                )
                .unwrap(),
            ],
            ordinary.graph.limits(),
        )
        .unwrap();
        NestedFixture {
            driver: DriverFixture {
                graph: graph.clone(),
                registry: ordinary.registry,
                first_calls: Arc::clone(&ordinary.first_calls),
                second_calls: Arc::clone(&ordinary.second_calls),
            },
            leaf: graph.clone(),
            leaf_calls: ordinary.first_calls,
            root_calls: Arc::new(AtomicUsize::new(0)),
            intermediate_calls: Vec::new(),
            graphs: vec![graph],
        }
    };
    let mut builder =
        ExecutableGraphRegistryBuilder::new(fixture.driver.registry.schemas().clone());
    builder
        .register_reducer(Arc::new(TestReducer {
            reference: fixture.leaf.reducer().clone(),
        }))
        .unwrap();
    for graph in &fixture.graphs {
        builder.register_graph(graph.clone()).unwrap();
        for node in graph.nodes() {
            if graph.reference() == fixture.leaf.reference() && node.node_id().as_str() == "Step_A"
            {
                builder
                    .register_node(Arc::new(AbsoluteWaitExecutor {
                        store: store.clone(),
                        graph: graph.reference(),
                        node_id: node.node_id().clone(),
                        timer_id,
                        offset_micros,
                        calls: Arc::clone(&fixture.leaf_calls),
                        original: Arc::clone(original),
                    }))
                    .unwrap();
            } else if node.allows_terminal() {
                builder
                    .register_node(Arc::new(TestNodeExecutor {
                        graph: graph.reference(),
                        node_id: node.node_id().clone(),
                        behavior: TestNodeBehavior::Terminal(graph.output_schema().clone()),
                        delay: Duration::ZERO,
                        calls: Arc::clone(if graph.reference() == fixture.leaf.reference() {
                            &fixture.driver.second_calls
                        } else {
                            &fixture.root_calls
                        }),
                    }))
                    .unwrap();
            }
        }
    }
    fixture.driver.registry = builder.build().unwrap();
    fixture
}

#[tokio::test]
async fn root_absolute_wait_survives_real_delay_and_higher_fence_without_reexecution() {
    Box::pin(recover_absolute_wait(false)).await;
}

#[tokio::test]
async fn scoped_absolute_wait_survives_real_delay_and_higher_fence_without_reexecution() {
    Box::pin(recover_absolute_wait(true)).await;
}

#[allow(clippy::too_many_lines)]
async fn recover_absolute_wait(scoped: bool) {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let original = Arc::new(OnceLock::new());
    let timer_id = TimerId::generate();
    let fixture = absolute_wait_fixture(&store, scoped, timer_id, &original);
    let tenant_id = tenant(if scoped {
        "elapsed-scoped"
    } else {
        "elapsed-root"
    });
    let ids = AgentRunIds::generate();
    admit_nested_fixture(&store, &fixture, &tenant_id, ids).await;
    let admitted = store
        .load_agent_admission(&tenant_id, ids.run_id())
        .await
        .unwrap();
    let defaults = DurableGraphDriverOptions::default();
    let options = DurableGraphDriverOptions::new(
        defaults.replay_limits(),
        if scoped { 3 } else { 2 },
        defaults.lease_renewal_interval(),
        defaults.node_execution_timeout(),
        defaults.maximum_mutation_attempts(),
        defaults.mutation_retry_initial_delay(),
    )
    .unwrap();
    let evidence = Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    });
    let first_driver =
        DurableGraphDriver::new(store.clone(), fixture.driver.registry.clone(), options)
            .unwrap()
            .with_direct_usage_evidence(evidence.clone());
    let first = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let first_outcome = first_driver
        .drive(first.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        first_outcome.outcome(),
        GraphDriveOutcome::Yielded {
            release: LeaseReleaseOutcome::Released
        }
    ));
    assert_eq!(first_outcome.report().node_attempts_started(), 1);
    assert_eq!(first_outcome.report().node_attempts_completed(), 1);
    assert_eq!(
        first_outcome.report().durable_events(),
        if scoped { 3 } else { 2 }
    );
    let (created, due, activation) = original.get().unwrap();
    assert_eq!(due.unix_micros() - created.unix_micros(), 500_000);
    let pending = store.load_pending_node_result(activation).await.unwrap();
    assert_eq!(pending.fence(), first.fence());
    assert_eq!(
        pending.intent().control(),
        &NodeControl::Wait {
            waits: NodeWaits::try_new([NodeWait::timer(timer_id, RunTimerKind::Sleep, *due)])
                .unwrap()
        }
    );
    let pending_bytes = serde_json_canonicalizer::to_vec(&pending).unwrap();
    let before = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(before.lifecycle().status(), RunStatus::Active);
    assert!(before.lease().is_none());
    assert_eq!(before.checkpoint(), admitted.run().checkpoint());
    let lifecycle_bytes = serde_json_canonicalizer::to_vec(before.lifecycle()).unwrap();
    let active_before = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap();
    assert_eq!(active_before.is_some(), scoped);
    let direct = evidence
        .usage_at(&tenant_id, ids.run_id(), before.journal_head().unwrap())
        .await
        .unwrap();
    assert_ne!(direct, BudgetUsage::zero());
    let delay_start = Instant::now();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(delay_start.elapsed() >= Duration::from_millis(600));
    let second = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert!(second.fence().epoch() > first.fence().epoch());
    let clock = store
        .observe_live_lease(second.fence())
        .await
        .unwrap()
        .observed_at();
    assert!(clock > *due);
    let continuation_options = if scoped {
        DurableGraphDriverOptions::new(
            defaults.replay_limits(),
            2,
            defaults.lease_renewal_interval(),
            defaults.node_execution_timeout(),
            defaults.maximum_mutation_attempts(),
            defaults.mutation_retry_initial_delay(),
        )
        .unwrap()
    } else {
        defaults
    };
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry.clone(),
        continuation_options,
    )
    .unwrap()
    .with_direct_usage_evidence(evidence.clone());
    let resumed = driver
        .drive(second.fence().clone(), CancellationSignal::never())
        .await;
    if scoped {
        let result = resumed.unwrap();
        assert!(matches!(
            result.outcome(),
            GraphDriveOutcome::Yielded {
                release: LeaseReleaseOutcome::Released
            }
        ));
        // One genuine caller rebind and one Ready barrier exhaust this original
        // two-event quantum; no successor Start receives a new quantum here.
        assert_eq!(result.report().durable_events(), 2);
        assert_eq!(result.report().frame_barriers_committed(), 1);
        assert_eq!(result.report().node_attempts_started(), 0);
        assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
        let original_scope = active_before.as_ref().unwrap().checkpoint();
        let record = store
            .load_graph_frame_barrier(
                &tenant_id,
                ids.run_id(),
                original_scope.frame().namespace(),
                original_scope.checkpoint().checkpoint_id(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record.wait_effect(),
            Some(stateknot_store_postgres::GraphWaitEffect::Ready)
        );
        assert_eq!(record.wait_revision(), Some(before.lifecycle().revision()));
        assert_eq!(
            record.disposition(),
            GraphBarrierDisposition::Wait {
                waits: NodeWaits::try_new([NodeWait::timer(timer_id, RunTimerKind::Sleep, *due)])
                    .unwrap(),
            }
        );
        assert_eq!(record.waits().unwrap(), Some(Vec::new()));
        assert_eq!(record.event().source().worker_fence(), Some(second.fence()));
        assert!(record.event().recorded_at() >= *due);
        assert_eq!(
            record.event().payload().schema().version(),
            Version::new(3, 0, 0)
        );
        assert!(matches!(
            store
                .load_durable_timer(&tenant_id, ids.run_id(), timer_id)
                .await,
            Err(StoreError::WaitRegistrationNotFound)
        ));
        let after = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
        assert_eq!(
            serde_json_canonicalizer::to_vec(after.lifecycle()).unwrap(),
            lifecycle_bytes
        );
        assert_eq!(after.checkpoint(), before.checkpoint());
        assert!(after.lease().is_none());
        assert!(!after.is_quarantined());
        let active = store
            .load_active_graph_frame(&tenant_id, ids.run_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.checkpoint().head(), record.checkpoint().head());
        assert_eq!(active.checkpoint().frame(), original_scope.frame());
        assert_eq!(
            serde_json_canonicalizer::to_vec(
                &store.load_pending_node_result(activation).await.unwrap()
            )
            .unwrap(),
            pending_bytes
        );
        eprintln!(
            "SCOPED_ELAPSED_READY_RECEIPT: {}",
            json!({
                "tenant": tenant_id,"run":ids.run_id(),"timer":timer_id,
                "original_due":due,"resume_clock":clock,"whole_journal":record.event().head(),
                "whole_digest":record.digest(),"pending":pending.head(),"original_scope":original_scope.head(),
                "successor":record.checkpoint().head(),"original_lifecycle":before.lifecycle(),
                "committed_lifecycle":after.lifecycle(),"direct":direct,"original_fence":first.fence(),
                "higher_fence":second.fence(),"quantum":2,"actual_events":2,"no_successor_start":true,
            })
        );
        // A genuinely fresh scheduler claim performs whole application replay
        // before the next Start. It does not rerun the successful original node.
        let third = store
            .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .clone();
        assert!(third.fence().epoch() > second.fence().epoch());
        let resumed_driver =
            DurableGraphDriver::new(store.clone(), fixture.driver.registry.clone(), defaults)
                .unwrap()
                .with_direct_usage_evidence(evidence);
        let final_result = resumed_driver
            .drive(third.fence().clone(), CancellationSignal::never())
            .await
            .unwrap();
        let GraphDriveOutcome::LifecycleBarrierReady(handoff) = final_result.outcome() else {
            panic!(
                "fresh scheduler must reach the real Root terminal plan: {:?}",
                final_result.outcome()
            );
        };
        assert!(matches!(
            handoff.plan().disposition(),
            GraphBarrierDisposition::Terminal { .. }
        ));
        assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 1);
        let reducer = TestReducer {
            reference: fixture.leaf.reducer().clone(),
        };
        let plan = fixture
            .leaf
            .plan_frame_barrier(
                original_scope,
                &[pending.clone()],
                record.checkpoint().checkpoint().checkpoint_id(),
                fixture.driver.registry.schemas(),
                &reducer,
            )
            .unwrap();
        let callbacks = ForbiddenElapsedWaitCallbacks {
            reference: fixture.leaf.reducer().clone(),
        };
        let acknowledged = store
            .commit_graph_frame_wait(
                plan,
                EventId::generate(),
                second.fence().clone(),
                before.journal_head().unwrap().clone(),
                direct.clone(),
                before.lifecycle().revision(),
                &callbacks,
                &callbacks,
            )
            .await
            .unwrap();
        let stateknot_store_postgres::GraphFrameBarrierCommitOutcome::Idempotent(ack) =
            acknowledged
        else {
            panic!("lost ACK must authenticate the original whole Ready transaction");
        };
        assert_eq!(ack.digest(), record.digest());
        assert_eq!(
            ack.wait_effect(),
            Some(stateknot_store_postgres::GraphWaitEffect::Ready)
        );
        assert_eq!(ack.checkpoint().head(), record.checkpoint().head());
        let current = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
        assert_eq!(current.lease().unwrap().fence(), third.fence());
        assert!(!current.is_quarantined());
        assert!(matches!(
            store.load_run_quarantine(&tenant_id, ids.run_id()).await,
            Err(StoreError::RunQuarantineNotFound)
        ));
        assert_eq!(
            serde_json_canonicalizer::to_vec(
                &store.load_pending_node_result(activation).await.unwrap()
            )
            .unwrap(),
            pending_bytes
        );
        store.release_lease(third.fence()).await.unwrap();
        store.close().await;
        return;
    }
    let resumed = resumed.unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = resumed.outcome() else {
        panic!(
            "Root original Wait must reach its exact lifecycle handoff: {:?}",
            resumed.outcome()
        );
    };
    assert_eq!(
        handoff.plan().disposition(),
        &GraphBarrierDisposition::Wait {
            waits: NodeWaits::try_new([NodeWait::timer(timer_id, RunTimerKind::Sleep, *due)])
                .unwrap(),
        }
    );
    let original_handoff = handoff.as_ref().clone();
    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.driver.registry.clone(),
        evidence.clone(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let committed = lifecycle
        .commit_barrier(original_handoff.clone())
        .await
        .unwrap();
    let GraphBarrierLifecycleOutcome::ReadyReleased {
        receipt,
        release: Some(LeaseReleaseOutcome::Released),
    } = committed
    else {
        panic!("Root elapsed Wait must return its actual Ready release: {committed:?}");
    };
    let stateknot_store_postgres::RootGraphBarrierCommitOutcome::Committed(record) =
        receipt.as_ref()
    else {
        panic!("first complete Root elapsed transaction must commit");
    };
    assert_eq!(
        record.wait_effect(),
        Some(stateknot_store_postgres::GraphWaitEffect::Ready)
    );
    assert!(record.waits().is_empty());
    assert_eq!(record.event().source().worker_fence(), Some(second.fence()));
    assert!(record.event().recorded_at() >= *due);
    assert_eq!(
        record.event().payload().schema().version(),
        Version::new(3, 0, 0)
    );
    assert_eq!(record.disposition(), handoff.plan().disposition());
    let after = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(
        serde_json_canonicalizer::to_vec(after.lifecycle()).unwrap(),
        lifecycle_bytes
    );
    assert_eq!(
        after.checkpoint().unwrap().checkpoint_id(),
        record.checkpoint().checkpoint_id()
    );
    assert!(after.lease().is_none());
    assert!(!after.is_quarantined());
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        store
            .load_durable_timer(&tenant_id, ids.run_id(), timer_id)
            .await,
        Err(StoreError::WaitRegistrationNotFound)
    ));
    assert_eq!(
        serde_json_canonicalizer::to_vec(
            &store.load_pending_node_result(activation).await.unwrap()
        )
        .unwrap(),
        pending_bytes
    );
    // A fresh scheduler claim, rather than the Ready receipt, grants the next
    // business quantum. It must not repeat the immutable successful Wait node.
    let third = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let final_result = driver
        .drive(third.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(
        matches!(final_result.outcome(), GraphDriveOutcome::LifecycleBarrierReady(handoff)
        if matches!(handoff.plan().disposition(), GraphBarrierDisposition::Terminal { .. }))
    );
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 1);
    let before_ack = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    let acknowledged = lifecycle.commit_barrier(original_handoff).await.unwrap();
    let GraphBarrierLifecycleOutcome::ReadyReleased {
        receipt: ack,
        release: None,
    } = acknowledged
    else {
        panic!(
            "late Root ACK must retain Ready without releasing a higher fence: {acknowledged:?}"
        );
    };
    let stateknot_store_postgres::RootGraphBarrierCommitOutcome::Idempotent(ack) = ack.as_ref()
    else {
        panic!("late Root ACK must return the original full transaction");
    };
    assert_eq!(ack.digest(), record.digest());
    assert_eq!(ack.checkpoint(), record.checkpoint());
    assert_eq!(
        ack.wait_effect(),
        Some(stateknot_store_postgres::GraphWaitEffect::Ready)
    );
    let after_ack = store.load_run(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(after_ack.lease(), before_ack.lease());
    assert_eq!(after_ack.journal_head(), before_ack.journal_head());
    assert_eq!(after_ack.lease().unwrap().fence(), third.fence());
    assert!(!after_ack.is_quarantined());
    store.release_lease(third.fence()).await.unwrap();
    store.close().await;
}

struct ForbiddenElapsedWaitCallbacks {
    reference: GraphReducerReference,
}
impl stateknot_core::GraphSchemaValidator for ForbiddenElapsedWaitCallbacks {
    fn validate(
        &self,
        _: &SchemaReference,
        _: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError> {
        panic!("full Ready ACK must precede schema callbacks");
    }
}
impl GraphReducer for ForbiddenElapsedWaitCallbacks {
    fn reference(&self) -> &GraphReducerReference {
        &self.reference
    }
    fn reduce(
        &self,
        _: &BoundedJson,
        _: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        panic!("full Ready ACK must precede reducer callbacks");
    }
}
