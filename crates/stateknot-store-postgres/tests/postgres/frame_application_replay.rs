// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Whole Root/lifetime-frame application replay through the claimed-session seam.
use super::frame_barriers::{commit, enter, plan, succeed};
use super::*;
use stateknot_core::{GraphFrameCheckpoint, NodeWait, NodeWaits, PendingNodeResult, RunFence};
use stateknot_store_postgres::GraphReplayLimits;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_linear_history_replays_eight_sixteen_and_thirty_two_edges_once() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    for edges in [8_u64, 16, 32] {
        let (_, template, _) = super::frame_transactions::graphs();
        let ready = |index: u64| {
            ReadyNodes::try_new([NodeId::new(format!("linear-{index:02}")).unwrap()]).unwrap()
        };
        let nodes = (0..=edges).map(|index| {
            GraphNode::new(
                NodeId::new(format!("linear-{index:02}")).unwrap(),
                (index < edges).then(|| ready(index + 1)),
                stateknot_core::GraphRoutes::empty(),
                None,
                index == edges,
            )
            .unwrap()
        });
        let child = CompiledGraph::compile(
            CapabilityIdentity::new(
                template.identity().owner().clone(),
                CapabilityReference::new(
                    format!("replay-linear-{edges}").parse().unwrap(),
                    Version::new(1, 0, 0),
                ),
            ),
            template.input_schema().clone(),
            template.state_schema().clone(),
            template.update_schema().clone(),
            template.output_schema().clone(),
            template.reducer().clone(),
            ready(0),
            nodes,
            GraphExecutionLimits::new(Superstep::new(edges + 1).unwrap(), 1).unwrap(),
        )
        .unwrap();
        let (_, entry, child, fence) = Box::pin(super::frame_barriers::enter_graph(
            &store,
            &format!("application-replay-linear-{edges}"),
            child,
            &[],
        ))
        .await;
        let parent = super::frame_transactions::caller_graph("barrier-parent", &child, 4096);
        let mut base = entry.entry().checkpoint().clone();
        let mut usage = entry.direct_usage_after().unwrap();
        for _ in 0..edges {
            let live = store.observe_live_lease(&fence).await.unwrap();
            store
                .renew_lease(
                    &fence,
                    Timestamp::from_unix_micros(live.observed_at().unix_micros() + 30_000_000)
                        .unwrap(),
                )
                .await
                .unwrap();
            let result =
                Box::pin(succeed_update(&store, &base, &fence, NodeControl::Continue)).await;
            let advanced = Box::pin(commit(
                &store,
                plan(&child, &base, &result),
                &fence,
                usage,
                &child,
            ))
            .await;
            usage = advanced.direct_usage_after().unwrap();
            base = advanced.checkpoint().clone();
        }
        let live = store.observe_live_lease(&fence).await.unwrap();
        store
            .renew_lease(
                &fence,
                Timestamp::from_unix_micros(live.observed_at().unix_micros() + 30_000_000).unwrap(),
            )
            .await
            .unwrap();
        let reducers = [
            ObservedReducer::new(&child, 0),
            ObservedReducer::new(&parent, 0),
        ];
        let report = recovery(&store, &fence)
            .await
            .validate_graph_replay(
                Some(&base.head()),
                &AcceptGraphSchemas,
                |reference| {
                    if *reference == child.reference() {
                        Some(&reducers[0] as &dyn GraphReducer)
                    } else if *reference == parent.reference() {
                        Some(&reducers[1] as &dyn GraphReducer)
                    } else {
                        None
                    }
                },
                GraphReplayLimits::default(),
            )
            .await
            .unwrap();
        assert_eq!(report.frames_validated(), 1);
        assert_eq!(report.frame_checkpoints_validated(), edges + 1);
        assert_eq!(report.barriers_replayed(), edges);
        assert_eq!(report.results_replayed(), edges);
        assert_eq!(report.returns_validated(), 0);
        assert_eq!(report.structural_edges_verified(), 5 * edges);
        assert_eq!(
            reducers[0].calls.load(Ordering::SeqCst),
            usize::try_from(edges).unwrap()
        );
        assert_eq!(reducers[1].calls.load(Ordering::SeqCst), 0);
    }
    store.close().await;
}

struct ObservedReducer {
    reference: GraphReducerReference,
    calls: AtomicUsize,
    mode: u8,
}

#[tokio::test]
async fn maximum_legal_individual_json_replays_with_a_sufficient_local_result_limit() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, child, fence) =
        Box::pin(enter(&store, "application-replay-maximum-json", true)).await;
    let limits = stateknot_core::JsonLimits::MAXIMUM;
    let value = json!({"one":"x".repeat(limits.max_string_bytes()),"two":"y".repeat(limits.max_bytes()-limits.max_string_bytes()-19)});
    let value = BoundedJson::try_from_value_with_limits(value, limits).unwrap();
    assert_eq!(
        serde_json_canonicalizer::to_vec(&value).unwrap().len(),
        limits.max_bytes()
    );
    let result = Box::pin(succeed_value(
        &store,
        entry.entry().checkpoint(),
        &fence,
        NodeControl::Continue,
        value,
    ))
    .await;
    let barrier = Box::pin(commit(
        &store,
        plan(&child, entry.entry().checkpoint(), &result),
        &fence,
        entry.direct_usage_after().unwrap(),
        &child,
    ))
    .await;
    let parent = super::frame_transactions::caller_graph("barrier-parent", &child, 4096);
    let reducers = [
        ObservedReducer::new(&child, 0),
        ObservedReducer::new(&parent, 0),
    ];
    let resolve = |reference: &GraphReference| {
        if *reference == child.reference() {
            Some(&reducers[0] as &dyn GraphReducer)
        } else if *reference == parent.reference() {
            Some(&reducers[1] as &dyn GraphReducer)
        } else {
            None
        }
    };
    let session = recovery(&store, &fence).await;
    assert!(matches!(
        session
            .validate_graph_replay(
                Some(&barrier.checkpoint().head()),
                &AcceptGraphSchemas,
                resolve,
                GraphReplayLimits::new(1024 * 1024).unwrap()
            )
            .await,
        Err(StoreError::GraphReplayResourceLimit)
    ));
    assert_eq!(reducers[0].calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        store
            .load_run_quarantine(fence.tenant_id(), fence.run_id())
            .await,
        Err(StoreError::RunQuarantineNotFound)
    ));
    let report = session
        .validate_graph_replay(
            Some(&barrier.checkpoint().head()),
            &AcceptGraphSchemas,
            resolve,
            GraphReplayLimits::new(4 * 1024 * 1024).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reducers[0].calls.load(Ordering::SeqCst), 1);
    assert!(report.maximum_barrier_result_bytes() > limits.max_bytes());
    store.close().await;
}

struct CallbackSchemas<F>(F);
impl<F> GraphSchemaValidator for CallbackSchemas<F>
where
    F: Fn(&SchemaReference) -> Result<(), GraphSchemaValidationError> + Send + Sync,
{
    fn validate(
        &self,
        schema: &SchemaReference,
        _: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError> {
        (self.0)(schema)
    }
}

fn callback_block_on<F: std::future::Future + Send>(future: F) -> F::Output
where
    F::Output: Send,
{
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(future)
            })
            .join()
            .unwrap()
    })
}

#[tokio::test]
async fn fresh_child_input_schema_availability_and_rejection_are_distinct() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, child, fence) =
        Box::pin(enter(&store, "application-replay-entry-schema", false)).await;
    let parent = super::frame_transactions::caller_graph("barrier-parent", &child, 4096);
    let reducers = [
        ObservedReducer::new(&child, 0),
        ObservedReducer::new(&parent, 0),
    ];
    let resolve = |reference: &GraphReference| {
        if *reference == child.reference() {
            Some(&reducers[0] as &dyn GraphReducer)
        } else if *reference == parent.reference() {
            Some(&reducers[1] as &dyn GraphReducer)
        } else {
            None
        }
    };
    let session = recovery(&store, &fence).await;
    for mode in [0, 1] {
        let calls = AtomicUsize::new(0);
        let schemas = CallbackSchemas(|schema: &SchemaReference| {
            if schema != child.input_schema() {
                return Ok(());
            }
            calls.fetch_add(1, Ordering::SeqCst);
            assert_ne!(mode, 1, "private schema panic");
            Err(GraphSchemaValidationError::Unavailable)
        });
        assert!(matches!(
            session
                .validate_graph_replay(
                    Some(&entry.entry().checkpoint().head()),
                    &schemas,
                    resolve,
                    GraphReplayLimits::default()
                )
                .await,
            Err(StoreError::GraphReplayDependencyUnavailable)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            store
                .load_run_quarantine(fence.tenant_id(), fence.run_id())
                .await,
            Err(StoreError::RunQuarantineNotFound)
        ));
    }
    let schemas = CallbackSchemas(|schema: &SchemaReference| {
        if schema == child.input_schema() {
            Err(GraphSchemaValidationError::Rejected)
        } else {
            Ok(())
        }
    });
    assert!(matches!(
        session
            .validate_graph_replay(
                Some(&entry.entry().checkpoint().head()),
                &schemas,
                resolve,
                GraphReplayLimits::default()
            )
            .await,
        Err(StoreError::RunQuarantined)
    ));
    store.close().await;
}

#[tokio::test]
async fn crossed_leaf_wrong_reducer_and_resolver_panic_never_quarantine() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, child, fence) =
        Box::pin(enter(&store, "application-replay-exact-dependency", false)).await;
    let (_, crossed, _, _) =
        Box::pin(enter(&store, "application-replay-crossed-leaf", false)).await;
    let mut wrong = ObservedReducer::new(&child, 0);
    wrong.reference = GraphReducerReference::new(
        child.reducer().identity().clone(),
        Digest::sha256(b"unregistered reducer revision"),
    );
    let session = recovery(&store, &fence).await;
    assert!(matches!(
        session
            .validate_graph_replay(
                Some(&crossed.entry().checkpoint().head()),
                &AcceptGraphSchemas,
                |_| None,
                GraphReplayLimits::default()
            )
            .await,
        Err(StoreError::InvalidClaimedRunRecoveryContext)
    ));
    assert!(matches!(
        session
            .validate_graph_replay(
                Some(&entry.entry().checkpoint().head()),
                &AcceptGraphSchemas,
                |_| Some(&wrong as &dyn GraphReducer),
                GraphReplayLimits::default()
            )
            .await,
        Err(StoreError::GraphReplayDependencyUnavailable)
    ));
    assert!(matches!(
        session
            .validate_graph_replay(
                Some(&entry.entry().checkpoint().head()),
                &AcceptGraphSchemas,
                |_| panic!("private resolver panic"),
                GraphReplayLimits::default()
            )
            .await,
        Err(StoreError::GraphReplayDependencyUnavailable)
    ));
    assert!(matches!(
        store
            .load_run_quarantine(fence.tenant_id(), fence.run_id())
            .await,
        Err(StoreError::RunQuarantineNotFound)
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callbacks_can_reuse_a_one_connection_store_and_obtain_the_run_mutation_lock() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) =
        test_store_with_options(test_options(Duration::from_secs(30)).with_pool_size(1, 1)).await
    else {
        return;
    };
    let (_, child, fence, barrier) =
        Box::pin(updated(&store, "application-replay-callback-transaction")).await;
    let parent = super::frame_transactions::caller_graph("barrier-parent", &child, 4096);
    let reducers = [
        ObservedReducer::new(&child, 0),
        ObservedReducer::new(&parent, 0),
    ];
    let calls = AtomicUsize::new(0);
    let schemas = CallbackSchemas(|_: &SchemaReference| {
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            callback_block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(2),
                    store.load_run(fence.tenant_id(), fence.run_id()),
                )
                .await
                .unwrap()
                .unwrap();
                let db = super::frame_barriers::pool().await;
                let mut tx = db.begin().await.unwrap();
                query("SET LOCAL statement_timeout='500ms'")
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                query(
                    "SELECT run_id FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2 FOR UPDATE",
                )
                .bind(fence.tenant_id().as_str())
                .bind(*fence.run_id().as_uuid())
                .fetch_one(&mut *tx)
                .await
                .unwrap();
                tx.rollback().await.unwrap();
                db.close().await;
            });
        }
        Ok(())
    });
    let session = recovery(&store, &fence).await;
    let report = session
        .validate_graph_replay(
            Some(&barrier.checkpoint().head()),
            &schemas,
            |reference| {
                if *reference == child.reference() {
                    Some(&reducers[0] as &dyn GraphReducer)
                } else if *reference == parent.reference() {
                    Some(&reducers[1] as &dyn GraphReducer)
                } else {
                    None
                }
            },
            GraphReplayLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(report.barriers_replayed(), 1);
    assert!(calls.load(Ordering::SeqCst) > 1);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_journal_progress_successor_fence_and_expiry_reject_the_original_handoff() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    for mode in [0, 1, 2] {
        let (_, entry, child, fence) = Box::pin(enter(
            &store,
            &format!("application-replay-callback-authority-{mode}"),
            false,
        ))
        .await;
        let parent = super::frame_transactions::caller_graph("barrier-parent", &child, 4096);
        let reducers = [
            ObservedReducer::new(&child, 0),
            ObservedReducer::new(&parent, 0),
        ];
        let done = AtomicUsize::new(0);
        let session = recovery(&store, &fence).await;
        let schemas = CallbackSchemas(|_: &SchemaReference| {
            if done.fetch_add(1, Ordering::SeqCst) == 0 {
                callback_block_on(async {
                    match mode {
                        0 => {
                            let run = store
                                .load_run(fence.tenant_id(), fence.run_id())
                                .await
                                .unwrap();
                            store
                                .append_worker(
                                    worker_append(
                                        fence.tenant_id().clone(),
                                        fence.run_id(),
                                        EventId::generate(),
                                        JournalExpectation::exact(
                                            run.journal_head().unwrap().clone(),
                                        ),
                                        fence.clone(),
                                        28_010,
                                    ),
                                    RunProjection::unchanged(),
                                )
                                .await
                                .unwrap();
                        }
                        1 => {
                            store
                                .supersede_lease(
                                    fence.tenant_id(),
                                    fence.run_id(),
                                    AttemptId::generate(),
                                )
                                .await
                                .unwrap();
                        }
                        _ => {
                            let db = super::frame_barriers::pool().await;
                            query("UPDATE stateknot.runs SET lease_acquired_at=clock_timestamp()-interval '2 seconds',lease_renewed_at=clock_timestamp()-interval '1 second',lease_expires_at=clock_timestamp()-interval '1 microsecond' WHERE tenant_id=$1 AND run_id=$2").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).execute(&db).await.unwrap();
                            db.close().await;
                        }
                    }
                });
            }
            if mode == 0 {
                Ok(())
            } else {
                Err(GraphSchemaValidationError::Rejected)
            }
        });
        let result = session
            .validate_graph_replay(
                Some(&entry.entry().checkpoint().head()),
                &schemas,
                |reference| {
                    if *reference == child.reference() {
                        Some(&reducers[0] as &dyn GraphReducer)
                    } else if *reference == parent.reference() {
                        Some(&reducers[1] as &dyn GraphReducer)
                    } else {
                        None
                    }
                },
                GraphReplayLimits::default(),
            )
            .await;
        match mode {
            0 => assert!(matches!(
                result,
                Err(StoreError::StaleClaimedRunRecoveryObservation)
            )),
            1 => assert!(matches!(result, Err(StoreError::StaleFence))),
            _ => assert!(matches!(result, Err(StoreError::LeaseExpired))),
        }
        assert!(matches!(
            store
                .load_run_quarantine(fence.tenant_id(), fence.run_id())
                .await,
            Err(StoreError::RunQuarantineNotFound)
        ));
    }
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn administrative_component_change_after_capture_is_not_hidden_by_the_same_journal() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, child, fence, barrier) =
        Box::pin(updated(&store, "application-replay-component-change")).await;
    let parent = super::frame_transactions::caller_graph("barrier-parent", &child, 4096);
    let reducers = [
        ObservedReducer::new(&child, 0),
        ObservedReducer::new(&parent, 0),
    ];
    let done = AtomicUsize::new(0);
    let schemas = CallbackSchemas(|_: &SchemaReference| {
        if done.fetch_add(1, Ordering::SeqCst) == 0 {
            callback_block_on(async {
                let db = super::frame_barriers::pool().await;
                let mut tx = db.begin().await.unwrap();
                query("ALTER TABLE stateknot.pending_node_results DISABLE TRIGGER USER")
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                query("UPDATE stateknot.pending_node_results SET control_kind='terminal' WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace<>''").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).execute(&mut *tx).await.unwrap();
                query("ALTER TABLE stateknot.pending_node_results ENABLE TRIGGER USER")
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
                db.close().await;
            });
        }
        Ok(())
    });
    let session = recovery(&store, &fence).await;
    assert!(matches!(
        session
            .validate_graph_replay(
                Some(&barrier.checkpoint().head()),
                &schemas,
                |reference| if *reference == child.reference() {
                    Some(&reducers[0] as &dyn GraphReducer)
                } else if *reference == parent.reference() {
                    Some(&reducers[1] as &dyn GraphReducer)
                } else {
                    None
                },
                GraphReplayLimits::default()
            )
            .await,
        Err(StoreError::RunQuarantined)
    ));
    let db = super::frame_barriers::pool().await;
    let enabled: bool = query_scalar("SELECT bool_and(tgenabled='O') FROM pg_trigger WHERE tgrelid='stateknot.pending_node_results'::regclass AND NOT tgisinternal").fetch_one(&db).await.unwrap();
    assert!(enabled);
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn root_only_eight_sixteen_and_thirty_two_edges_have_linear_actual_visits() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    for edges in [8, 16, 32] {
        let tenant = tenant(&format!("application-replay-root-linear-{edges}"));
        let run = RunId::generate();
        let initial = Box::pin(start_run_with_checkpoint(&store, &tenant, run, 28_020)).await;
        store
            .register_graph_definition(tenant.clone(), checkpoint_compiled_graph())
            .await
            .unwrap();
        let fence = store
            .claim_lease(&tenant, run, AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .fence()
            .clone();
        let mut base = initial.checkpoint().clone();
        for edge in 0..edges {
            let (heads, journal) = Box::pin(commit_ready_results(
                &store,
                &base,
                &fence,
                28_030 + edge * 3,
            ))
            .await;
            let successor = CheckpointWrite::successor(
                CheckpointId::generate(),
                &base,
                base.state().clone(),
                ready_node(edge + 2),
            )
            .unwrap();
            let barrier = CheckpointBarrier::new(&base, successor, heads).unwrap();
            base = store
                .append_worker_barrier(
                    worker_append(
                        tenant.clone(),
                        run,
                        EventId::generate(),
                        JournalExpectation::exact(journal),
                        fence.clone(),
                        28_031 + edge * 3,
                    ),
                    RunProjection::unchanged(),
                    barrier,
                )
                .await
                .unwrap()
                .checkpoint()
                .clone();
        }
        let session = recovery(&store, &fence).await;
        let reducer = IntegrationGraphReducer::new();
        let report = session
            .validate_graph_replay(
                None,
                &AcceptGraphSchemas,
                |_| Some(&reducer as &dyn GraphReducer),
                GraphReplayLimits::default(),
            )
            .await
            .unwrap();
        assert_eq!(report.root_checkpoints_validated(), edges + 1);
        let root = report.root_report();
        assert_eq!(root.checkpoints_validated(), edges + 1);
        assert_eq!(root.barriers_replayed(), edges);
        assert_eq!(root.results_replayed(), edges);
        assert!(root.maximum_barrier_result_bytes() > 0);
        assert_eq!(report.barriers_replayed(), edges);
        assert_eq!(report.structural_edges_verified(), edges * 3);
        assert_eq!(report.frames_validated(), 0);
    }
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn legacy_root_dispositions_without_a_complete_bundle_are_operational() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    for mode in [0, 1, 2] {
        let tenant = tenant(&format!("application-replay-legacy-root-{mode}"));
        let run = RunId::generate();
        let graph = if mode <= 1 {
            let mut nodes = vec![
                GraphNode::new(
                    NodeId::new("node-0001").unwrap(),
                    None,
                    GraphRoutes::empty(),
                    (mode == 0).then(|| ready_node(2)),
                    mode == 1,
                )
                .unwrap(),
            ];
            if mode == 0 {
                nodes.push(
                    GraphNode::new(
                        NodeId::new("node-0002").unwrap(),
                        None,
                        GraphRoutes::empty(),
                        None,
                        true,
                    )
                    .unwrap(),
                );
            }
            CompiledGraph::compile(
                checkpoint_capability("replay-root-disposition"),
                checkpoint_schema("integration-input"),
                checkpoint_state_schema(),
                checkpoint_schema("integration-update"),
                checkpoint_schema("integration-output"),
                checkpoint_compiled_graph().reducer().clone(),
                ready_node(1),
                nodes,
                GraphExecutionLimits::new(Superstep::new(2).unwrap(), 1).unwrap(),
            )
            .unwrap()
        } else {
            checkpoint_compiled_graph()
        };
        let admission = Box::pin(super::frame_transactions::admit(
            &store, &tenant, run, &graph, &graph,
        ))
        .await;
        let fence = store
            .claim_lease(&tenant, run, AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .fence()
            .clone();
        let activation = NodeActivation::for_ready_root(
            admission.checkpoint(),
            NodeId::new("node-0001").unwrap(),
        )
        .unwrap();
        let control = if mode == 1 {
            super::frame_barriers::terminal(&graph)
        } else if mode == 0 {
            NodeControl::Wait {
                waits: NodeWaits::try_new([NodeWait::timer(
                    TimerId::generate(),
                    RunTimerKind::Sleep,
                    Timestamp::from_unix_micros(
                        admission.event().recorded_at().unix_micros() + 1_000_000,
                    )
                    .unwrap(),
                )])
                .unwrap(),
            }
        } else {
            NodeControl::Continue
        };
        let result = store
            .commit_test_pending_node_result(
                worker_append(
                    tenant.clone(),
                    run,
                    EventId::generate(),
                    JournalExpectation::exact(admission.event().head()),
                    fence.clone(),
                    28_050,
                ),
                PendingNodeResultIntent::new(
                    activation,
                    NodeStateChange::Unchanged,
                    control,
                    NodeInvocationBindings::empty(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let reducer = IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        };
        let plan = graph
            .plan_barrier(
                admission.checkpoint(),
                std::slice::from_ref(result.result()),
                CheckpointId::generate(),
                &AcceptGraphSchemas,
                &reducer,
            )
            .unwrap();
        if mode == 2 {
            let current = store.load_run(&tenant, run).await.unwrap();
            // The legacy generic API accepts a control transition with its own
            // valid state projection. It did not persist a GraphBarrierPlan;
            // a missing disposition cannot certify that it used the reducer.
            let generic_successor = CheckpointWrite::successor(
                CheckpointId::generate(),
                admission.checkpoint(),
                CheckpointState::new(
                    graph.state_schema().clone(),
                    BoundedJson::try_from(json!({"count":42,"source":"legacy-control"})).unwrap(),
                )
                .unwrap(),
                plan.barrier().successor().ready_nodes().clone(),
            )
            .unwrap();
            let generic_barrier = CheckpointBarrier::new(
                admission.checkpoint(),
                generic_successor,
                plan.barrier().result_heads().iter().cloned(),
            )
            .unwrap();
            store
                .append_control_plane_barrier(
                    control_append(
                        tenant.clone(),
                        run,
                        EventId::generate(),
                        JournalExpectation::exact(result.event().head()),
                        28_051,
                    ),
                    RunProjection::transition(
                        current.lifecycle().revision(),
                        RunTransition::RequestCancellation {
                            request: cancellation_request(result.event().recorded_at()),
                        },
                    ),
                    generic_barrier,
                )
                .await
                .unwrap();
        } else {
            store
                .append_worker_barrier(
                    worker_append(
                        tenant.clone(),
                        run,
                        EventId::generate(),
                        JournalExpectation::exact(result.event().head()),
                        fence.clone(),
                        28_051,
                    ),
                    RunProjection::unchanged(),
                    plan.barrier().clone(),
                )
                .await
                .unwrap();
        }
        let session = recovery(&store, &fence).await;
        assert!(matches!(
            session
                .validate_graph_replay(
                    None,
                    &AcceptGraphSchemas,
                    |_| Some(&reducer as &dyn GraphReducer),
                    GraphReplayLimits::default()
                )
                .await,
            Err(StoreError::GraphReplayHistoricalDispositionUnavailable)
        ));
        assert!(matches!(
            store.load_run_quarantine(&tenant, run).await,
            Err(StoreError::RunQuarantineNotFound)
        ));
    }
    store.close().await;
}
impl ObservedReducer {
    fn new(graph: &CompiledGraph, mode: u8) -> Self {
        Self {
            reference: graph.reducer().clone(),
            calls: AtomicUsize::new(0),
            mode,
        }
    }
}
impl GraphReducer for ObservedReducer {
    fn reference(&self) -> &GraphReducerReference {
        &self.reference
    }
    fn reduce(
        &self,
        state: &BoundedJson,
        updates: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        assert!(!updates.is_empty());
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            1 => Ok(BoundedJson::try_from(json!({"count":8,"source":"changed-reducer"})).unwrap()),
            2 => Err(GraphReducerError::Unavailable),
            3 => Err(GraphReducerError::ResourceLimit),
            4 => panic!("private reducer panic"),
            _ => Ok(state.clone()),
        }
    }
}

async fn succeed_update(
    store: &PostgresStore,
    base: &GraphFrameCheckpoint,
    fence: &RunFence,
    control: NodeControl,
) -> PendingNodeResult {
    Box::pin(succeed_value(
        store,
        base,
        fence,
        control,
        BoundedJson::try_from(json!({"count":2,"source":"actual-update"})).unwrap(),
    ))
    .await
}

async fn succeed_value(
    store: &PostgresStore,
    base: &GraphFrameCheckpoint,
    fence: &RunFence,
    control: NodeControl,
    value: BoundedJson,
) -> PendingNodeResult {
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let observed = run.journal_head().unwrap();
    let ready = ReadyNodeRecoveryPlanner::for_frame(base.clone(), fence.clone())
        .unwrap()
        .finish(observed.clone(), observed.recorded_at())
        .unwrap();
    let start = Box::pin(store.start_recovered_graph_frame_node_attempt(
        worker_append(
            fence.tenant_id().clone(),
            fence.run_id(),
            EventId::generate(),
            JournalExpectation::exact(observed.clone()),
            fence.clone(),
            28_000,
        ),
        &ready,
        base.checkpoint().ready_nodes().iter().next().unwrap(),
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    let graph = store
        .load_graph_definition(fence.tenant_id(), base.checkpoint().graph())
        .await
        .unwrap();
    let intent = PendingNodeResultIntent::new(
        start.attempt().start().activation().clone(),
        NodeStateChange::Update {
            update: NodeStateUpdate::new(graph.graph().update_schema().clone(), value).unwrap(),
        },
        control,
        NodeInvocationBindings::empty(),
    )
    .unwrap();
    Box::pin(store.succeed_node_attempt(
        worker_append(
            fence.tenant_id().clone(),
            fence.run_id(),
            EventId::generate(),
            JournalExpectation::exact(start.event().head()),
            fence.clone(),
            28_001,
        ),
        &start.attempt().start().head(),
        intent.clone(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    Box::pin(store.load_pending_node_result(intent.activation()))
        .await
        .unwrap()
}

async fn updated(
    store: &PostgresStore,
    name: &str,
) -> (
    stateknot_store_postgres::StoredGraphFrameEntry,
    CompiledGraph,
    RunFence,
    stateknot_store_postgres::StoredGraphFrameBarrier,
) {
    let (_, entry, child, fence) = Box::pin(enter(store, name, true)).await;
    let result = Box::pin(succeed_update(
        store,
        entry.entry().checkpoint(),
        &fence,
        NodeControl::Continue,
    ))
    .await;
    let barrier = Box::pin(commit(
        store,
        plan(&child, entry.entry().checkpoint(), &result),
        &fence,
        entry.direct_usage_after().unwrap(),
        &child,
    ))
    .await;
    (entry, child, fence, barrier)
}

async fn recovery<'a>(
    store: &'a PostgresStore,
    fence: &stateknot_core::RunFence,
) -> stateknot_store_postgres::ClaimedRunRecovery<'a> {
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let context = CorruptionQuarantineContext::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        QuarantineId::generate(),
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        Digest::sha256(b"whole application replay evidence"),
    )
    .unwrap();
    store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await
        .unwrap()
}

#[tokio::test]
async fn actual_noninitial_leaf_replay_covers_root_and_child_without_dispatch() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, child, fence) = Box::pin(enter(&store, "application-replay-leaf", true)).await;
    let result = Box::pin(succeed(
        &store,
        entry.entry().checkpoint(),
        &fence,
        NodeControl::Continue,
    ))
    .await;
    let barrier = Box::pin(commit(
        &store,
        plan(&child, entry.entry().checkpoint(), &result),
        &fence,
        entry.direct_usage_after().unwrap(),
        &child,
    ))
    .await;
    let parent = super::frame_transactions::caller_graph("barrier-parent", &child, 4096);
    let reducers = [
        IntegrationGraphReducer {
            reference: child.reducer().clone(),
        },
        IntegrationGraphReducer {
            reference: parent.reducer().clone(),
        },
    ];
    let session = recovery(&store, &fence).await;
    let report = session
        .validate_graph_replay(
            Some(&barrier.checkpoint().head()),
            &AcceptGraphSchemas,
            |reference| {
                if *reference == child.reference() {
                    Some(&reducers[0] as &dyn GraphReducer)
                } else if *reference == parent.reference() {
                    Some(&reducers[1] as &dyn GraphReducer)
                } else {
                    None
                }
            },
            GraphReplayLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(report.root_checkpoints_validated(), 1);
    let root = report.root_report();
    assert_eq!(root.checkpoints_validated(), 1);
    assert_eq!(root.barriers_replayed(), 0);
    assert_eq!(root.results_replayed(), 0);
    assert_eq!(root.maximum_barrier_result_bytes(), 0);
    assert_eq!(report.frame_checkpoints_validated(), 2);
    assert_eq!(report.barriers_replayed(), 1);
    assert_eq!(report.results_replayed(), 1);
    assert_eq!(report.frames_validated(), 1);
    assert_eq!(report.returns_validated(), 0);
    assert_eq!(report.leaf(), Some(&barrier.checkpoint().head()));
    assert_eq!(report.journal_head(), &barrier.event().head());
    store.close().await;
}

#[tokio::test]
async fn missing_exact_reducer_is_operational_and_fresh_entry_schema_is_replayed() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, _, fence) =
        Box::pin(enter(&store, "application-replay-dependency", false)).await;
    let session = recovery(&store, &fence).await;
    assert!(matches!(
        session
            .validate_graph_replay(
                Some(&entry.entry().checkpoint().head()),
                &AcceptGraphSchemas,
                |_| None,
                GraphReplayLimits::default()
            )
            .await,
        Err(StoreError::GraphReplayDependencyUnavailable)
    ));
    assert!(matches!(
        store
            .load_run_quarantine(fence.tenant_id(), fence.run_id())
            .await,
        Err(StoreError::RunQuarantineNotFound)
    ));
    store.close().await;
}

#[tokio::test]
async fn actual_update_calls_the_pinned_reducer_once_and_limits_are_operational() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, child, fence, barrier) = Box::pin(updated(&store, "application-replay-update")).await;
    let parent = super::frame_transactions::caller_graph("barrier-parent", &child, 4096);
    let reducer = ObservedReducer::new(&child, 0);
    let parent_reducer = ObservedReducer::new(&parent, 0);
    let session = recovery(&store, &fence).await;
    let resolve = |reference: &GraphReference| {
        if *reference == child.reference() {
            Some(&reducer as &dyn GraphReducer)
        } else if *reference == parent.reference() {
            Some(&parent_reducer as &dyn GraphReducer)
        } else {
            None
        }
    };
    assert!(matches!(
        session
            .validate_graph_replay(
                Some(&barrier.checkpoint().head()),
                &AcceptGraphSchemas,
                resolve,
                GraphReplayLimits::new(1).unwrap()
            )
            .await,
        Err(StoreError::GraphReplayResourceLimit)
    ));
    assert_eq!(reducer.calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        store
            .load_run_quarantine(fence.tenant_id(), fence.run_id())
            .await,
        Err(StoreError::RunQuarantineNotFound)
    ));
    let report = session
        .validate_graph_replay(
            Some(&barrier.checkpoint().head()),
            &AcceptGraphSchemas,
            resolve,
            GraphReplayLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(reducer.calls.load(Ordering::SeqCst), 1);
    assert_eq!(parent_reducer.calls.load(Ordering::SeqCst), 0);
    assert_eq!(report.structural_edges_verified(), 5);
    assert!(report.maximum_barrier_result_bytes() > 1);
    assert!(report.manifest_bytes() > 0);
    store.close().await;
}

#[tokio::test]
async fn reducer_availability_resource_limit_and_panic_never_quarantine_durable_data() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, child, fence, barrier) =
        Box::pin(updated(&store, "application-replay-operational")).await;
    let parent = super::frame_transactions::caller_graph("barrier-parent", &child, 4096);
    let parent_reducer = ObservedReducer::new(&parent, 0);
    let session = recovery(&store, &fence).await;
    let journal = session.initial_run().journal_head().unwrap().clone();
    for mode in [2, 3, 4] {
        let reducer = ObservedReducer::new(&child, mode);
        assert!(matches!(
            session
                .validate_graph_replay(
                    Some(&barrier.checkpoint().head()),
                    &AcceptGraphSchemas,
                    |reference| if *reference == child.reference() {
                        Some(&reducer as &dyn GraphReducer)
                    } else if *reference == parent.reference() {
                        Some(&parent_reducer as &dyn GraphReducer)
                    } else {
                        None
                    },
                    GraphReplayLimits::default()
                )
                .await,
            Err(StoreError::GraphReplayDependencyUnavailable)
        ));
        assert_eq!(reducer.calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            store
                .load_run_quarantine(fence.tenant_id(), fence.run_id())
                .await,
            Err(StoreError::RunQuarantineNotFound)
        ));
        assert_eq!(
            store
                .load_run(fence.tenant_id(), fence.run_id())
                .await
                .unwrap()
                .journal_head(),
            Some(&journal)
        );
    }
    store.close().await;
}

#[tokio::test]
async fn deployed_reducer_successor_mismatch_is_fenced_corruption() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, child, fence, barrier) =
        Box::pin(updated(&store, "application-replay-state-mismatch")).await;
    let parent = super::frame_transactions::caller_graph("barrier-parent", &child, 4096);
    let reducer = ObservedReducer::new(&child, 1);
    let parent_reducer = ObservedReducer::new(&parent, 0);
    let session = recovery(&store, &fence).await;
    assert!(matches!(
        session
            .validate_graph_replay(
                Some(&barrier.checkpoint().head()),
                &AcceptGraphSchemas,
                |reference| if *reference == child.reference() {
                    Some(&reducer as &dyn GraphReducer)
                } else if *reference == parent.reference() {
                    Some(&parent_reducer as &dyn GraphReducer)
                } else {
                    None
                },
                GraphReplayLimits::default()
            )
            .await,
        Err(StoreError::RunQuarantined)
    ));
    assert_eq!(reducer.calls.load(Ordering::SeqCst), 1);
    assert!(
        store
            .load_run_quarantine(fence.tenant_id(), fence.run_id())
            .await
            .is_ok()
    );
    store.close().await;
}

#[tokio::test]
async fn returned_child_and_root_consumption_are_replayed_after_the_final_pop() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, _, child, fence, terminal_plan, barrier) = Box::pin(
        super::frame_returns::finished(&store, "application-replay-returned-root"),
    )
    .await;
    let stateknot_store_postgres::GraphFrameReturnCommitOutcome::Committed(returned) =
        Box::pin(store.return_graph_frame(
            terminal_plan,
            EventId::generate(),
            fence.clone(),
            barrier.event().head(),
            barrier.direct_usage_after().unwrap(),
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: child.reducer().clone(),
            },
        ))
        .await
        .unwrap()
    else {
        panic!("new return")
    };
    let parent = super::frame_transactions::caller_graph("barrier-parent", &child, 4096);
    let root_plan = parent
        .plan_barrier(
            admission.checkpoint(),
            std::slice::from_ref(returned.result()),
            CheckpointId::generate(),
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: parent.reducer().clone(),
            },
        )
        .unwrap();
    let committed = store
        .append_worker_barrier(
            worker_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                EventId::generate(),
                JournalExpectation::exact(returned.event().head()),
                fence.clone(),
                28_020,
            ),
            RunProjection::unchanged(),
            root_plan.barrier().clone(),
        )
        .await
        .unwrap();
    let reducer = ObservedReducer::new(&child, 0);
    let parent_reducer = ObservedReducer::new(&parent, 0);
    let session = recovery(&store, &fence).await;
    let report = session
        .validate_graph_replay(
            None,
            &AcceptGraphSchemas,
            |reference| {
                if *reference == child.reference() {
                    Some(&reducer as &dyn GraphReducer)
                } else if *reference == parent.reference() {
                    Some(&parent_reducer as &dyn GraphReducer)
                } else {
                    None
                }
            },
            GraphReplayLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(report.leaf(), None);
    assert_eq!(report.journal_head(), &committed.event().head());
    assert_eq!(report.root_checkpoints_validated(), 2);
    let root = report.root_report();
    assert_eq!(root.checkpoints_validated(), 2);
    assert_eq!(root.barriers_replayed(), 1);
    assert_eq!(root.results_replayed(), 1);
    assert_eq!(
        root.maximum_barrier_result_bytes(),
        serde_json::to_vec(returned.result()).unwrap().len(),
    );
    assert_eq!(report.frame_checkpoints_validated(), 2);
    assert_eq!(report.barriers_replayed(), 2);
    assert_eq!(report.returns_validated(), 1);
    assert_eq!(parent_reducer.calls.load(Ordering::SeqCst), 1);
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn seven_level_open_and_returned_cascade_replays_all_lifetime_scopes_on_default_stack() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, entries, fence, mut usage) = Box::pin(super::frame_closures::seven(
        &store,
        "application-replay-seven",
    ))
    .await;
    let mut definitions = Vec::new();
    definitions.push(
        store
            .load_graph_definition(fence.tenant_id(), admission.checkpoint().graph())
            .await
            .unwrap()
            .graph()
            .clone(),
    );
    for entry in &entries {
        definitions.push(
            store
                .load_graph_definition(
                    fence.tenant_id(),
                    entry.entry().checkpoint().checkpoint().graph(),
                )
                .await
                .unwrap()
                .graph()
                .clone(),
        );
    }
    let reducers: Vec<ObservedReducer> = definitions
        .iter()
        .map(|graph| ObservedReducer::new(graph, 0))
        .collect();
    let resolve = |reference: &GraphReference| {
        definitions
            .iter()
            .position(|graph| graph.reference() == *reference)
            .map(|index| &reducers[index] as &dyn GraphReducer)
    };
    let session = recovery(&store, &fence).await;
    let report = session
        .validate_graph_replay(
            Some(&entries.last().unwrap().entry().checkpoint().head()),
            &AcceptGraphSchemas,
            resolve,
            GraphReplayLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(report.frames_validated(), 7);
    assert_eq!(report.frame_checkpoints_validated(), 7);
    assert_eq!(report.barriers_replayed(), 0);
    drop(session);
    let mut previous = None;
    for level in (0..7).rev() {
        let live = store.observe_live_lease(&fence).await.unwrap();
        let expiry =
            Timestamp::from_unix_micros(live.observed_at().unix_micros() + 30_000_000).unwrap();
        store.renew_lease(&fence, expiry).await.unwrap();
        let returned = Box::pin(super::frame_returns::finish_and_return(
            &store,
            &entries[level],
            &definitions[level + 1],
            &fence,
            usage,
            previous.as_ref(),
        ))
        .await;
        usage = returned.direct_usage_after().unwrap();
        previous = Some(returned);
    }
    let session = recovery(&store, &fence).await;
    let report = session
        .validate_graph_replay(
            None,
            &AcceptGraphSchemas,
            resolve,
            GraphReplayLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(report.frames_validated(), 7);
    assert_eq!(report.returns_validated(), 7);
    assert_eq!(report.frame_checkpoints_validated(), 20);
    assert_eq!(report.barriers_replayed(), 13);
    // Capture and final proofs visit 13 edges plus one terminal edge per return;
    // the three direct transcript/application passes visit only the 13 edges.
    assert_eq!(report.structural_edges_verified(), 79);
    assert_eq!(
        reducers
            .iter()
            .map(|reducer| reducer.calls.load(Ordering::SeqCst))
            .sum::<usize>(),
        6
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_interrupt_and_timer_resume_replay_original_wait_without_an_extra_transition() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = super::frame_barriers::pool().await;
    let now: i64 = query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint")
        .fetch_one(&db)
        .await
        .unwrap();
    let due = Timestamp::from_unix_micros(now + 500_000).unwrap();
    let interrupt_id = InterruptId::generate();
    let timer_id = TimerId::generate();
    let waits = NodeWaits::try_new([
        super::frame_waits::interrupt(interrupt_id),
        NodeWait::timer(timer_id, RunTimerKind::Sleep, due),
    ])
    .unwrap();
    let (_, entry, child, old, plan, observed, revision) = Box::pin(super::frame_waits::prepared(
        &store,
        "application-replay-wait",
        waits,
    ))
    .await;
    let saved = Box::pin(super::frame_waits::suspend(
        &store,
        plan,
        &old,
        observed,
        entry.direct_usage_after().unwrap(),
        revision,
        &child,
    ))
    .await;
    assert!(
        store
            .claim_lease(old.tenant_id(), old.run_id(), AttemptId::generate())
            .await
            .is_err()
    );
    let waiting = store.load_run(old.tenant_id(), old.run_id()).await.unwrap();
    let request = store
        .load_interrupt_request(old.tenant_id(), old.run_id(), interrupt_id)
        .await
        .unwrap();
    let event_id = EventId::generate();
    let intent = InterruptResolutionIntent::new(
        &request,
        event_id,
        payload(28_031),
        InterruptResolver::new(
            super::frame_waits::principal(),
            super::frame_waits::scopes(),
        ),
    )
    .unwrap();
    let resolved = store
        .resolve_interrupt(
            control_append(
                old.tenant_id().clone(),
                old.run_id(),
                event_id,
                JournalExpectation::exact(saved.event().head()),
                28_032,
            ),
            waiting.lifecycle().revision(),
            intent,
        )
        .await
        .unwrap();
    let remaining = store.load_run(old.tenant_id(), old.run_id()).await.unwrap();
    assert_eq!(remaining.unresolved_wait_count(), 1);
    let timer = store
        .load_durable_timer(old.tenant_id(), old.run_id(), timer_id)
        .await
        .unwrap();
    let now: i64 = query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint")
        .fetch_one(&db)
        .await
        .unwrap();
    if now < due.unix_micros() {
        tokio::time::sleep(Duration::from_micros(
            u64::try_from(due.unix_micros() - now + 1000).unwrap(),
        ))
        .await;
    }
    let event_id = EventId::generate();
    store
        .fire_timer(
            control_append(
                old.tenant_id().clone(),
                old.run_id(),
                event_id,
                JournalExpectation::exact(resolved.event().head()),
                28_033,
            ),
            remaining.lifecycle().revision(),
            TimerFiringIntent::new(&timer, event_id).unwrap(),
        )
        .await
        .unwrap();
    let newer = store
        .claim_lease(old.tenant_id(), old.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let parent = super::frame_transactions::caller_graph("barrier-parent", &child, 4096);
    let reducers = [
        ObservedReducer::new(&child, 0),
        ObservedReducer::new(&parent, 0),
    ];
    let session = recovery(&store, &newer).await;
    let report = session
        .validate_graph_replay(
            Some(&saved.checkpoint().head()),
            &AcceptGraphSchemas,
            |reference| {
                if *reference == child.reference() {
                    Some(&reducers[0] as &dyn GraphReducer)
                } else if *reference == parent.reference() {
                    Some(&reducers[1] as &dyn GraphReducer)
                } else {
                    None
                }
            },
            GraphReplayLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(report.waits_validated(), 1);
    assert_eq!(report.barriers_replayed(), 1);
    assert_eq!(report.frame_checkpoints_validated(), 2);
    assert_eq!(report.leaf(), Some(&saved.checkpoint().head()));
    assert_eq!(saved.checkpoint().checkpoint().superstep().get(), 1);

    // Resolved waits authenticate the saved step once; a later actual terminal
    // transition must replay both edges under the successor fence.
    drop(session);
    let result = Box::pin(succeed_update(
        &store,
        saved.checkpoint(),
        &newer,
        super::frame_barriers::terminal(&child),
    ))
    .await;
    let continued = Box::pin(commit(
        &store,
        super::frame_barriers::plan(&child, saved.checkpoint(), &result),
        &newer,
        saved.direct_usage_after().unwrap(),
        &child,
    ))
    .await;
    let report = recovery(&store, &newer)
        .await
        .validate_graph_replay(
            Some(&continued.checkpoint().head()),
            &AcceptGraphSchemas,
            |reference| {
                if *reference == child.reference() {
                    Some(&reducers[0] as &dyn GraphReducer)
                } else if *reference == parent.reference() {
                    Some(&reducers[1] as &dyn GraphReducer)
                } else {
                    None
                }
            },
            GraphReplayLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(report.waits_validated(), 1);
    assert_eq!(report.barriers_replayed(), 2);
    assert_eq!(report.frame_checkpoints_validated(), 3);
    assert_eq!(report.leaf(), Some(&continued.checkpoint().head()));
    assert_eq!(reducers[0].calls.load(Ordering::SeqCst), 1);
    db.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn callback_projection_changes_reject_the_original_observation_but_lease_renewal_is_allowed()
{
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    for mode in 0..3 {
        let (_, entry, child, fence) = Box::pin(enter(
            &store,
            &format!("application-replay-original-projection-{mode}"),
            false,
        ))
        .await;
        let parent = super::frame_transactions::caller_graph("barrier-parent", &child, 4096);
        let reducers = [
            ObservedReducer::new(&child, 0),
            ObservedReducer::new(&parent, 0),
        ];
        let session = recovery(&store, &fence).await;
        let original = session.initial_run().clone();
        let done = AtomicUsize::new(0);
        let schemas = CallbackSchemas(|_: &SchemaReference| {
            if done.fetch_add(1, Ordering::SeqCst) == 0 {
                callback_block_on(async {
                    if mode == 2 {
                        replay_renew(&store, &fence).await;
                    } else {
                        let db = super::frame_barriers::pool().await;
                        let mut tx = db.begin().await.unwrap();
                        query("ALTER TABLE stateknot.runs DISABLE TRIGGER USER")
                            .execute(&mut *tx)
                            .await
                            .unwrap();
                        if mode == 0 {
                            // Write an independently valid lifecycle and matching SQL
                            // projection without moving the journal observation.
                            let revision = original
                                .lifecycle()
                                .revision()
                                .get()
                                .checked_add(1)
                                .unwrap()
                                .to_string();
                            let mut value = serde_json::to_value(original.lifecycle()).unwrap();
                            value["revision"] = json!(revision);
                            let lifecycle: stateknot_core::RunLifecycle =
                                serde_json::from_value(value).unwrap();
                            let bytes = serde_json_canonicalizer::to_vec(&lifecycle).unwrap();
                            let changed = query("UPDATE stateknot.runs SET lifecycle_bytes=$3,lifecycle_revision=$4::text::numeric WHERE tenant_id=$1 AND run_id=$2")
                                .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid())
                                .bind(bytes).bind(revision).execute(&mut *tx).await.unwrap();
                            assert_eq!(changed.rows_affected(), 1);
                        } else {
                            let changed = query("UPDATE stateknot.runs SET scheduler_ready_at=scheduler_ready_at+interval '1 microsecond' WHERE tenant_id=$1 AND run_id=$2")
                                .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).execute(&mut *tx).await.unwrap();
                            assert_eq!(changed.rows_affected(), 1);
                        }
                        query("ALTER TABLE stateknot.runs ENABLE TRIGGER USER")
                            .execute(&mut *tx)
                            .await
                            .unwrap();
                        tx.commit().await.unwrap();
                        db.close().await;
                    }
                });
            }
            Ok(())
        });
        let result = session
            .validate_graph_replay(
                Some(&entry.entry().checkpoint().head()),
                &schemas,
                |reference| {
                    if *reference == child.reference() {
                        Some(&reducers[0] as &dyn GraphReducer)
                    } else if *reference == parent.reference() {
                        Some(&reducers[1] as &dyn GraphReducer)
                    } else {
                        None
                    }
                },
                GraphReplayLimits::default(),
            )
            .await;
        if mode == 2 {
            assert!(result.is_ok(), "{result:?}");
        } else {
            assert!(
                matches!(result, Err(StoreError::StaleClaimedRunRecoveryObservation)),
                "{result:?}"
            );
        }
        let current = store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap();
        assert_eq!(current.journal_head(), original.journal_head());
        assert!(matches!(
            store
                .load_run_quarantine(fence.tenant_id(), fence.run_id())
                .await,
            Err(StoreError::RunQuarantineNotFound)
        ));
    }
    store.close().await;
}

struct AccumulatingReplayReducer {
    reference: GraphReducerReference,
    calls: AtomicUsize,
    wrong: bool,
}
impl AccumulatingReplayReducer {
    fn new(graph: &CompiledGraph, wrong: bool) -> Self {
        Self {
            reference: graph.reducer().clone(),
            calls: AtomicUsize::new(0),
            wrong,
        }
    }
}
impl GraphReducer for AccumulatingReplayReducer {
    fn reference(&self) -> &GraphReducerReference {
        &self.reference
    }
    fn reduce(
        &self,
        state: &BoundedJson,
        updates: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let sum = updates
            .iter()
            .map(|update| update.update().data().as_value()["count"].as_u64().unwrap())
            .sum::<u64>();
        let value = state.as_value()["count"].as_u64().unwrap() + sum + u64::from(self.wrong);
        Ok(BoundedJson::try_from(json!({"count":value,"source":"accumulating"})).unwrap())
    }
}

fn replay_graph_identity(template: &CompiledGraph, name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        template.identity().owner().clone(),
        CapabilityReference::new(name.parse().unwrap(), Version::new(1, 0, 0)),
    )
}
fn replay_ready(name: &str) -> ReadyNodes {
    ReadyNodes::try_new([NodeId::new(name).unwrap()]).unwrap()
}
fn warm_replay_caller(name: &str, target: &CompiledGraph) -> CompiledGraph {
    use stateknot_core::{GraphFrameCall, GraphFrameCallPolicy, GraphRoute, GraphRoutes, RouteId};
    let (_, template, _) = super::frame_transactions::graphs();
    let call = GraphFrameCall::new(
        NodeId::new("call").unwrap(),
        NodeId::new("slot.a").unwrap(),
        target,
        RouteId::new("return").unwrap(),
    )
    .unwrap();
    CompiledGraph::compile(
        replay_graph_identity(&template, name),
        template.input_schema().clone(),
        template.state_schema().clone(),
        template.update_schema().clone(),
        template.output_schema().clone(),
        template.reducer().clone(),
        replay_ready("first"),
        [
            GraphNode::new(
                NodeId::new("first").unwrap(),
                Some(replay_ready("call")),
                GraphRoutes::empty(),
                None,
                false,
            )
            .unwrap(),
            GraphNode::new(
                NodeId::new("call").unwrap(),
                None,
                GraphRoutes::try_new([GraphRoute::new(
                    RouteId::new("return").unwrap(),
                    replay_ready("finish"),
                )
                .unwrap()])
                .unwrap(),
                None,
                false,
            )
            .unwrap(),
            GraphNode::new(
                NodeId::new("finish").unwrap(),
                None,
                GraphRoutes::empty(),
                None,
                true,
            )
            .unwrap(),
        ],
        template.limits(),
    )
    .unwrap()
    .with_frame_calls(GraphFrameCallPolicy::new(7, 4096, [call]).unwrap())
    .unwrap()
}
fn siblings_replay_root(terminal: &CompiledGraph, chain: &CompiledGraph) -> CompiledGraph {
    use stateknot_core::{GraphFrameCall, GraphFrameCallPolicy, GraphRoute, GraphRoutes, RouteId};
    let (_, template, _) = super::frame_transactions::graphs();
    let mut nodes = Vec::new();
    let mut calls = Vec::new();
    for index in 0..3 {
        let node = NodeId::new(format!("call-{index}")).unwrap();
        let route = RouteId::new(format!("return-{index}")).unwrap();
        let target = if index < 2 { terminal } else { chain };
        calls.push(
            GraphFrameCall::new(
                node.clone(),
                NodeId::new(format!("slot.{index}")).unwrap(),
                target,
                route.clone(),
            )
            .unwrap(),
        );
        let successor = if index < 2 {
            format!("call-{}", index + 1)
        } else {
            "finish".to_owned()
        };
        nodes.push(
            GraphNode::new(
                node,
                None,
                GraphRoutes::try_new([GraphRoute::new(route, replay_ready(&successor)).unwrap()])
                    .unwrap(),
                None,
                false,
            )
            .unwrap(),
        );
    }
    nodes.push(
        GraphNode::new(
            NodeId::new("finish").unwrap(),
            None,
            GraphRoutes::empty(),
            None,
            true,
        )
        .unwrap(),
    );
    CompiledGraph::compile(
        replay_graph_identity(&template, "replay-siblings-root"),
        template.input_schema().clone(),
        template.state_schema().clone(),
        template.update_schema().clone(),
        template.output_schema().clone(),
        template.reducer().clone(),
        replay_ready("call-0"),
        nodes,
        template.limits(),
    )
    .unwrap()
    .with_frame_calls(GraphFrameCallPolicy::new(7, 4096, calls).unwrap())
    .unwrap()
}
async fn replay_renew(store: &PostgresStore, fence: &RunFence) {
    let live = store.observe_live_lease(fence).await.unwrap();
    store
        .renew_lease(
            fence,
            Timestamp::from_unix_micros(live.observed_at().unix_micros() + 30_000_000).unwrap(),
        )
        .await
        .unwrap();
}
async fn accumulating_replay_barrier(
    store: &PostgresStore,
    graph: &CompiledGraph,
    base: &GraphFrameCheckpoint,
    result: &PendingNodeResult,
    fence: &RunFence,
    usage: BudgetUsage,
) -> stateknot_store_postgres::StoredGraphFrameBarrier {
    let reducer = AccumulatingReplayReducer::new(graph, false);
    let plan = graph
        .plan_frame_barrier(
            base,
            std::slice::from_ref(result),
            CheckpointId::generate(),
            &AcceptGraphSchemas,
            &reducer,
        )
        .unwrap();
    let observed = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    let stateknot_store_postgres::GraphFrameBarrierCommitOutcome::Committed(record) =
        Box::pin(store.commit_graph_frame_barrier(
            plan,
            EventId::generate(),
            fence.clone(),
            observed,
            usage,
            &AcceptGraphSchemas,
            &reducer,
        ))
        .await
        .unwrap()
    else {
        panic!("fresh scoped barrier");
    };
    record
}

// This fixture commits two returned siblings and their actual Root reductions,
// then leaves seven open scopes whose six suspended parents already advanced.
#[allow(clippy::too_many_lines)]
async fn siblings_before_warm_seven(
    store: &PostgresStore,
    name: &str,
) -> (Vec<CompiledGraph>, RunFence, GraphFrameCheckpoint) {
    use stateknot_core::{ExecutionCount, GraphFrameEntryPlan};
    let (_, terminal, _) = super::frame_transactions::graphs();
    let mut chain = vec![terminal.clone()];
    for level in (0..6).rev() {
        chain.push(warm_replay_caller(
            &format!("warm-replay-level-{level}"),
            chain.last().unwrap(),
        ));
    }
    chain.reverse();
    let root = siblings_replay_root(&terminal, &chain[0]);
    let tenant = tenant(name);
    let run = RunId::generate();
    for graph in &chain {
        store
            .register_graph_definition(tenant.clone(), graph.clone())
            .await
            .unwrap();
    }
    let admission = Box::pin(super::frame_transactions::admit(
        store, &tenant, run, &root, &terminal,
    ))
    .await;
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let mut root_base = admission.checkpoint().clone();
    let mut usage = super::frame_transactions::initial_usage(&admission);
    for index in 0..2 {
        replay_renew(store, &fence).await;
        let call = root
            .frame_calls()
            .unwrap()
            .call(&NodeId::new(format!("call-{index}")).unwrap())
            .unwrap();
        let plan = GraphFrameEntryPlan::for_root(
            call,
            &root,
            &root_base,
            &terminal,
            CheckpointId::generate(),
            AttemptId::generate(),
            fence.clone(),
        )
        .unwrap();
        let observed = store
            .load_run(&tenant, run)
            .await
            .unwrap()
            .journal_head()
            .unwrap()
            .clone();
        let stateknot_store_postgres::GraphFrameEntryCommitOutcome::Committed(entry) =
            Box::pin(store.enter_graph_frame(
                plan,
                EventId::generate(),
                observed,
                usage,
                &AcceptGraphSchemas,
            ))
            .await
            .unwrap()
        else {
            panic!("fresh sibling");
        };
        let result = Box::pin(succeed_update(
            store,
            entry.entry().checkpoint(),
            &fence,
            super::frame_barriers::terminal(&terminal),
        ))
        .await;
        let terminal_barrier = Box::pin(accumulating_replay_barrier(
            store,
            &terminal,
            entry.entry().checkpoint(),
            &result,
            &fence,
            entry.direct_usage_after().unwrap(),
        ))
        .await;
        // Return must reuse the already committed checkpoint ID.
        let terminal_plan = terminal
            .plan_frame_barrier(
                entry.entry().checkpoint(),
                std::slice::from_ref(&result),
                terminal_barrier.checkpoint().checkpoint().checkpoint_id(),
                &AcceptGraphSchemas,
                &AccumulatingReplayReducer::new(&terminal, false),
            )
            .unwrap();
        let stateknot_store_postgres::GraphFrameReturnCommitOutcome::Committed(returned) =
            Box::pin(store.return_graph_frame(
                terminal_plan,
                EventId::generate(),
                fence.clone(),
                terminal_barrier.event().head(),
                terminal_barrier.direct_usage_after().unwrap(),
                &AcceptGraphSchemas,
                &AccumulatingReplayReducer::new(&terminal, false),
            ))
            .await
            .unwrap()
        else {
            panic!("fresh sibling return");
        };
        let root_plan = root
            .plan_barrier(
                &root_base,
                std::slice::from_ref(returned.result()),
                CheckpointId::generate(),
                &AcceptGraphSchemas,
                &AccumulatingReplayReducer::new(&root, false),
            )
            .unwrap();
        let advanced = store
            .append_worker_barrier(
                worker_append(
                    tenant.clone(),
                    run,
                    EventId::generate(),
                    JournalExpectation::exact(returned.event().head()),
                    fence.clone(),
                    28_080 + index,
                ),
                RunProjection::unchanged(),
                root_plan.barrier().clone(),
            )
            .await
            .unwrap();
        usage = returned
            .direct_usage_after()
            .unwrap()
            .checked_accumulate(
                &BudgetUsage::builder()
                    .graph_steps(ExecutionCount::new(1))
                    .event_bytes(ByteCount::new(
                        u64::try_from(
                            serde_json_canonicalizer::to_vec(advanced.event())
                                .unwrap()
                                .len(),
                        )
                        .unwrap(),
                    ))
                    .checkpoint_bytes(ByteCount::new(
                        u64::try_from(
                            serde_json_canonicalizer::to_vec(advanced.checkpoint())
                                .unwrap()
                                .len(),
                        )
                        .unwrap(),
                    ))
                    .build()
                    .unwrap(),
            )
            .unwrap();
        root_base = advanced.checkpoint().clone();
    }
    assert_eq!(root_base.superstep().get(), 2);
    assert_eq!(root_base.state().data().as_value()["count"], 21);
    let mut previous: Option<GraphFrameCheckpoint> = None;
    for (level, target) in chain.iter().enumerate() {
        replay_renew(store, &fence).await;
        let (parent, call) = if level == 0 {
            (
                &root,
                root.frame_calls()
                    .unwrap()
                    .call(&NodeId::new("call-2").unwrap())
                    .unwrap(),
            )
        } else {
            (
                &chain[level - 1],
                chain[level - 1]
                    .frame_calls()
                    .unwrap()
                    .call(&NodeId::new("call").unwrap())
                    .unwrap(),
            )
        };
        let entry_plan = if let Some(base) = &previous {
            GraphFrameEntryPlan::for_frame(
                call,
                parent,
                base,
                target,
                CheckpointId::generate(),
                AttemptId::generate(),
                fence.clone(),
            )
        } else {
            GraphFrameEntryPlan::for_root(
                call,
                parent,
                &root_base,
                target,
                CheckpointId::generate(),
                AttemptId::generate(),
                fence.clone(),
            )
        }
        .unwrap();
        let observed = store
            .load_run(&tenant, run)
            .await
            .unwrap()
            .journal_head()
            .unwrap()
            .clone();
        let stateknot_store_postgres::GraphFrameEntryCommitOutcome::Committed(entry) =
            Box::pin(store.enter_graph_frame(
                entry_plan,
                EventId::generate(),
                observed,
                usage,
                &AcceptGraphSchemas,
            ))
            .await
            .unwrap()
        else {
            panic!("fresh warm child");
        };
        usage = entry.direct_usage_after().unwrap();
        let mut base = entry.entry().checkpoint().clone();
        if level < 6 {
            let result =
                Box::pin(succeed_update(store, &base, &fence, NodeControl::Continue)).await;
            let advanced = Box::pin(accumulating_replay_barrier(
                store, target, &base, &result, &fence, usage,
            ))
            .await;
            usage = advanced.direct_usage_after().unwrap();
            base = advanced.checkpoint().clone();
            assert_eq!(base.checkpoint().superstep().get(), 1);
            assert_eq!(
                base.checkpoint().state().data().as_value()["count"],
                23 + u64::try_from(level).unwrap() * 2
            );
        }
        previous = Some(base);
    }
    replay_renew(store, &fence).await;
    let mut definitions = vec![root];
    definitions.extend(chain);
    (definitions, fence, previous.unwrap())
}

#[tokio::test]
async fn sequential_returned_siblings_and_seven_noninitial_suspended_parents_are_all_replayed() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    for mode in 0..3 {
        let (definitions, fence, leaf) = Box::pin(siblings_before_warm_seven(
            &store,
            &format!("application-replay-siblings-warm-{mode}"),
        ))
        .await;
        let journal_before = store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .journal_head()
            .unwrap()
            .clone();
        let reducers: Vec<_> = definitions
            .iter()
            .enumerate()
            .map(|(index, graph)| {
                AccumulatingReplayReducer::new(
                    graph,
                    (mode == 1 && index == 7) || (mode == 2 && index == 0),
                )
            })
            .collect();
        let result = recovery(&store, &fence)
            .await
            .validate_graph_replay(
                Some(&leaf.head()),
                &AcceptGraphSchemas,
                |reference| {
                    definitions
                        .iter()
                        .position(|graph| graph.reference() == *reference)
                        .map(|index| &reducers[index] as &dyn GraphReducer)
                },
                GraphReplayLimits::default(),
            )
            .await;
        if mode == 0 {
            let report = result.unwrap();
            assert_eq!(report.journal_head(), &journal_before);
            assert_eq!(
                store
                    .load_run(fence.tenant_id(), fence.run_id())
                    .await
                    .unwrap()
                    .journal_head(),
                Some(&journal_before)
            );
            assert_eq!(report.root_checkpoints_validated(), 3);
            assert_eq!(report.frames_validated(), 9);
            assert_eq!(report.returns_validated(), 2);
            assert_eq!(report.frame_checkpoints_validated(), 17);
            assert_eq!(report.barriers_replayed(), 10);
            assert_eq!(report.results_replayed(), 10);
            assert_eq!(report.structural_edges_verified(), 50);
            assert_eq!(
                reducers
                    .iter()
                    .map(|reducer| reducer.calls.load(Ordering::SeqCst))
                    .sum::<usize>(),
                10
            );
        } else {
            assert!(matches!(result, Err(StoreError::RunQuarantined)));
            assert!(
                store
                    .load_run_quarantine(fence.tenant_id(), fence.run_id())
                    .await
                    .is_ok()
            );
        }
    }
    store.close().await;
}
