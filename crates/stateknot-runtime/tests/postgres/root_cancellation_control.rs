// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Root cancellation keeps its original accounting and obeys host control.

use super::*;
use sqlx_core::connection::ConnectOptions;
use sqlx_postgres::PgConnectOptions;
use stateknot_core::{CancellationObserver, RunLease};
use stateknot_runtime::GraphLifecycleError;
use stateknot_store_postgres::{AppendOutcome, StoredRun};

#[derive(Clone, Default)]
struct HostShutdown {
    requested: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

impl HostShutdown {
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

impl CancellationObserver for HostShutdown {
    fn is_cancelled(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            loop {
                let notified = self.notify.notified();
                tokio::pin!(notified);
                let _ = notified.as_mut().enable();
                if self.is_cancelled() {
                    return;
                }
                notified.await;
            }
        })
    }
}

// A finite read-only delay around the actual admitted provider-native ledger.
// No usage, lease, cancellation receipt or terminal result is supplied by this adapter.
struct ControlledReader {
    ledger: ProviderNativeAgentLifecycleEvidence,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    observations: AtomicUsize,
    panic_first: bool,
}

impl ControlledReader {
    fn new(fixture: &ProviderNativeFixture, store: &PostgresStore, panic_first: bool) -> Self {
        Self {
            ledger: ProviderNativeAgentLifecycleEvidence::new(
                fixture.definition.clone(),
                store.clone(),
            ),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            observations: AtomicUsize::new(0),
            panic_first,
        }
    }

    async fn wait_entered(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.entered.notified())
            .await
            .expect("Root cancellation must enter the real evidence boundary");
    }
}

impl GraphLifecycleEvidenceProvider for ControlledReader {
    fn terminal_evidence(
        &self,
        context: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>> {
        self.ledger.terminal_evidence(context)
    }

    fn failure_evidence(
        &self,
        context: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        self.ledger.failure_evidence(context)
    }

    fn cancellation_evidence(
        &self,
        context: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async move {
            assert!(context.leaf().is_none(), "exercise the ordinary Root path");
            if self.observations.fetch_add(1, Ordering::SeqCst) == 0 {
                self.entered.notify_one();
                assert!(!self.panic_first, "test-only Root evidence reader panic");
                // This reader always finishes, even if the caller never releases it.
                let _ =
                    tokio::time::timeout(Duration::from_secs(10), self.release.notified()).await;
            }
            self.ledger.cancellation_evidence(context).await
        })
    }
}

struct CancellationFixture {
    native: ProviderNativeFixture,
    tenant: TenantId,
    run: RunId,
    lease: RunLease,
    original: StoredRun,
}

async fn cancellation_fixture(store: &PostgresStore, name: &str) -> CancellationFixture {
    let native = provider_native_fixture(store.clone());
    let tenant = tenant(name);
    let ids = AgentRunIds::generate();
    let run = ids.run_id();
    store
        .register_graph_definition(tenant.clone(), native.definition.graph().clone())
        .await
        .unwrap();
    let admitted = DurableAgentAdmission::new(store.clone(), native.registry.clone())
        .unwrap()
        .admit(provider_native_admission_request(
            &native,
            tenant.clone(),
            ids,
        ))
        .await
        .unwrap();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Cancelled,
        FailureCode::new("runtime.root.cancellation.control").unwrap(),
        FailureOrigin::new("stateknot.runtime.integration").unwrap(),
        FailureMessage::new("Root cancellation retains its original accounting.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap();
    let request =
        RunCancellationRequest::new(failure, store.observe_database_clock().await.unwrap())
            .unwrap();
    store
        .append_control_plane(
            JournalAppend::new(
                JournalExpectation::exact(admitted.stored().event().head()),
                JournalEventIntent::control_plane(
                    tenant.clone(),
                    run,
                    EventId::generate(),
                    test_payload(),
                )
                .unwrap(),
            )
            .unwrap(),
            RunProjection::transition(
                admitted.stored().run().lifecycle().revision(),
                RunTransition::RequestCancellation { request },
            ),
        )
        .await
        .unwrap();
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let original = store.load_run(&tenant, run).await.unwrap();
    CancellationFixture {
        native,
        tenant,
        run,
        lease,
        original,
    }
}

fn cancellation_loop(
    store: &PostgresStore,
    fixture: &CancellationFixture,
    reader: Arc<ControlledReader>,
    timeout: Duration,
) -> DurableAgentLoop {
    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        1024,
        Duration::from_millis(100),
        timeout,
        3,
        Duration::from_millis(25),
    )
    .unwrap();
    DurableAgentLoop::new(
        store.clone(),
        fixture.native.registry.clone(),
        reader,
        options,
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap()
}

async fn joined<T>(task: &mut tokio::task::JoinHandle<T>, bound: Duration) -> T {
    if let Ok(result) = tokio::time::timeout(bound, &mut *task).await {
        result.expect("the cancellation boundary must contain provider panic")
    } else {
        task.abort();
        let _ = task.await;
        panic!("Root cancellation did not finish within its host control bound");
    }
}

async fn assert_pending(store: &PostgresStore, fixture: &CancellationFixture) {
    let current = store.load_run(&fixture.tenant, fixture.run).await.unwrap();
    assert_eq!(
        serde_json::to_value(current.lifecycle()).unwrap(),
        serde_json::to_value(fixture.original.lifecycle()).unwrap()
    );
    assert_eq!(current.journal_head(), fixture.original.journal_head());
    assert_eq!(current.checkpoint(), fixture.original.checkpoint());
    assert!(current.lifecycle().terminal_usage().is_none());
    assert!(!current.is_quarantined());
    assert_eq!(fixture.native.model_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.native.tool_calls.load(Ordering::SeqCst), 0);
}

async fn wait_past_expiry(store: &PostgresStore, expires_at: Timestamp) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while store.observe_database_clock().await.unwrap() <= expires_at {
        assert!(
            Instant::now() < deadline,
            "the actual database clock must advance"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn root_cancellation_shutdown_releases_pending_usage_and_can_retry() {
    Box::pin(assert_root_control(true)).await;
}

#[tokio::test]
async fn root_cancellation_original_timeout_releases_pending_usage_and_can_retry() {
    Box::pin(assert_root_control(false)).await;
}

async fn assert_root_control(shutdown_requested: bool) {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = cancellation_fixture(&store, "root-cancellation-control").await;
    let reader = Arc::new(ControlledReader::new(&fixture.native, &store, false));
    let timeout = if shutdown_requested {
        Duration::from_secs(20)
    } else {
        Duration::from_millis(250)
    };
    let agent_loop = cancellation_loop(&store, &fixture, reader.clone(), timeout);
    let shutdown = HostShutdown::default();
    let signal = CancellationSignal::new(shutdown.clone());
    let running = agent_loop.clone();
    let fence = fixture.lease.fence().clone();
    let mut task = tokio::spawn(async move { running.run(fence, signal).await });
    reader.wait_entered().await;
    if shutdown_requested {
        shutdown.request();
    }
    let result = joined(&mut task, Duration::from_secs(3)).await;
    if shutdown_requested {
        assert!(matches!(
            result.unwrap().outcome(),
            AgentLoopOutcome::RecoveryReleased(_)
        ));
    } else {
        assert!(matches!(
            result,
            Err(AgentLoopError::Lifecycle { source })
                if matches!(*source, GraphLifecycleError::Evidence(
                    GraphLifecycleEvidenceError::TemporarilyUnavailable
                ))
        ));
    }
    assert_pending(&store, &fixture).await;
    assert!(
        store
            .load_run(&fixture.tenant, fixture.run)
            .await
            .unwrap()
            .lease()
            .is_none()
    );
    let replacement = store
        .claim_lease(&fixture.tenant, fixture.run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert!(replacement.fence().epoch() > fixture.lease.fence().epoch());
    let retried = agent_loop
        .run(replacement.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        retried.outcome(),
        AgentLoopOutcome::CancellationConfirmed(AppendOutcome::Committed(_))
    ));
    let terminal = store.load_run(&fixture.tenant, fixture.run).await.unwrap();
    assert_eq!(terminal.lifecycle().status(), RunStatus::Cancelled);
    assert_eq!(
        terminal.lifecycle().terminal_usage(),
        Some(&BudgetUsage::zero())
    );
    assert_eq!(
        serde_json::to_value(terminal.lifecycle().cancellation_request()).unwrap(),
        serde_json::to_value(fixture.original.lifecycle().cancellation_request()).unwrap()
    );
    assert!(terminal.lease().is_none());
    store.close().await;
}

#[tokio::test]
async fn root_cancellation_slow_complete_reader_keeps_its_original_fence() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let fixture = cancellation_fixture(&store, "root-cancellation-slow-reader").await;
    let reader = Arc::new(ControlledReader::new(&fixture.native, &store, false));
    let agent_loop = cancellation_loop(&store, &fixture, reader.clone(), Duration::from_secs(20));
    let fence = fixture.lease.fence().clone();
    let mut task =
        tokio::spawn(async move { agent_loop.run(fence, CancellationSignal::never()).await });
    reader.wait_entered().await;
    // Driver recovery has drained its own guardian before this boundary.
    let entered = store.load_run(&fixture.tenant, fixture.run).await.unwrap();
    let original_lease = entered.lease().unwrap().clone();
    wait_past_expiry(&store, original_lease.expires_at()).await;
    let during = store.load_run(&fixture.tenant, fixture.run).await.unwrap();
    reader.release.notify_one();
    let result = joined(&mut task, Duration::from_secs(10)).await;
    assert_eq!(during.lease().unwrap().fence(), fixture.lease.fence());
    assert!(during.lease().unwrap().expires_at() > original_lease.expires_at());
    assert!(matches!(
        result.unwrap().outcome(),
        AgentLoopOutcome::CancellationConfirmed(AppendOutcome::Committed(_))
    ));
    let terminal = store.load_run(&fixture.tenant, fixture.run).await.unwrap();
    assert_eq!(terminal.lifecycle().status(), RunStatus::Cancelled);
    assert_eq!(
        terminal.lifecycle().terminal_usage(),
        Some(&BudgetUsage::zero())
    );
    assert!(terminal.lease().is_none());
    assert_eq!(fixture.native.model_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.native.tool_calls.load(Ordering::SeqCst), 0);
    store.close().await;
}

#[tokio::test]
async fn root_cancellation_reader_panic_is_closed_and_releases_ownership() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = cancellation_fixture(&store, "root-cancellation-reader-panic").await;
    let reader = Arc::new(ControlledReader::new(&fixture.native, &store, true));
    let agent_loop = cancellation_loop(&store, &fixture, reader, Duration::from_secs(20));
    let fence = fixture.lease.fence().clone();
    let mut task =
        tokio::spawn(async move { agent_loop.run(fence, CancellationSignal::never()).await });
    let result = joined(&mut task, Duration::from_secs(10)).await;
    assert!(matches!(
        result,
        Err(AgentLoopError::Lifecycle { source })
            if matches!(*source, GraphLifecycleError::Evidence(
                GraphLifecycleEvidenceError::Unavailable
            ))
    ));
    assert_pending(&store, &fixture).await;
    assert!(
        store
            .load_run(&fixture.tenant, fixture.run)
            .await
            .unwrap()
            .lease()
            .is_none()
    );
    store.close().await;
}

#[tokio::test]
async fn root_cancellation_lost_claim_cannot_confirm_or_release_replacement() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = cancellation_fixture(&store, "root-cancellation-lost-claim").await;
    let reader = Arc::new(ControlledReader::new(&fixture.native, &store, false));
    let agent_loop = cancellation_loop(&store, &fixture, reader.clone(), Duration::from_secs(20));
    let fence = fixture.lease.fence().clone();
    let mut task =
        tokio::spawn(async move { agent_loop.run(fence, CancellationSignal::never()).await });
    reader.wait_entered().await;
    store.release_lease(fixture.lease.fence()).await.unwrap();
    let replacement = store
        .claim_lease(&fixture.tenant, fixture.run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    reader.release.notify_one();
    assert!(joined(&mut task, Duration::from_secs(10)).await.is_err());
    assert_pending(&store, &fixture).await;
    let current = store.load_run(&fixture.tenant, fixture.run).await.unwrap();
    assert_eq!(current.lease().unwrap().fence(), replacement.fence());
    store.release_lease(replacement.fence()).await.unwrap();
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn root_cancellation_original_commit_ack_survives_expiry_and_replays_without_evidence() {
    use crate::commit_proxy::{CommitProxy, Cut, TransactionTarget};

    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let connection: PgConnectOptions = std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
    let mut proxy = CommitProxy::start(
        &connection,
        Cut::CommitResponse,
        TransactionTarget::GraphFrameCancellationConfirmation,
    )
    .await;
    let proxied = PostgresStore::connect(
        connection
            .host("127.0.0.1")
            .port(proxy.port())
            .to_url_lossy()
            .as_str(),
        store.options().clone().with_pool_size(1, 1),
    )
    .await
    .unwrap();
    // Finish schema/proxy setup before acquiring this deliberately short lease.
    let fixture = cancellation_fixture(&store, "root-cancellation-original-ack").await;
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.native.registry.clone(),
        DurableGraphDriverOptions::new(
            GraphReplayLimits::default(),
            1024,
            Duration::from_millis(100),
            Duration::from_secs(15 * 60),
            3,
            Duration::from_millis(25),
        )
        .unwrap(),
    )
    .unwrap();
    let recovered = driver
        .drive(fixture.lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::CancellationRequested(handoff) = recovered.into_parts().0 else {
        panic!("the real Driver must return a cancellation handoff");
    };
    assert!(handoff.leaf().is_none());
    let reader = Arc::new(ControlledReader::new(&fixture.native, &store, false));
    reader.release.notify_one();
    let lifecycle = DurableGraphLifecycle::new(
        proxied.clone(),
        fixture.native.registry.clone(),
        reader.clone(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let confirming = lifecycle.clone();
    let original = handoff.as_ref().clone();
    let mut task = tokio::spawn(async move { confirming.confirm_cancellation(original).await });
    tokio::select! {
        () = proxy.wait_cut(Cut::CommitResponse) => {},
        result = &mut task => panic!("confirmation ended before its original commit cut: {result:?}"),
    }
    let committed = store.load_run(&fixture.tenant, fixture.run).await.unwrap();
    // The final live DB clock follows the drained guardian. Every renewal
    // target is bounded by that clock plus the unchanged Store lease duration.
    let lease_micros = i64::try_from(store.options().lease_duration().as_micros()).unwrap();
    let terminal_bound = Timestamp::from_unix_micros(
        committed
            .lifecycle()
            .changed_at()
            .unix_micros()
            .checked_add(lease_micros)
            .unwrap(),
    )
    .unwrap();
    let expiry_bound = if terminal_bound > fixture.lease.expires_at() {
        terminal_bound
    } else {
        fixture.lease.expires_at()
    };
    wait_past_expiry(&store, expiry_bound).await;
    let retained_ack = !task.is_finished();
    let held_messages = proxy.messages_while_ack_held();
    proxy.resume_ack();
    let result = joined(&mut task, Duration::from_secs(10)).await.unwrap();
    assert!(
        retained_ack,
        "the complete original commit ACK must be retained"
    );
    assert_eq!(held_messages, 0);
    assert!(matches!(
        result,
        GraphBarrierLifecycleOutcome::Cancelled(AppendOutcome::Committed(_))
    ));
    let replayed = lifecycle.confirm_cancellation(*handoff).await.unwrap();
    assert!(matches!(
        replayed,
        GraphBarrierLifecycleOutcome::Cancelled(AppendOutcome::Idempotent(_))
    ));
    let current = store.load_run(&fixture.tenant, fixture.run).await.unwrap();
    assert_eq!(current.journal_head(), committed.journal_head());
    assert_eq!(
        serde_json::to_value(current.lifecycle()).unwrap(),
        serde_json::to_value(committed.lifecycle()).unwrap()
    );
    assert_eq!(current.lifecycle().status(), RunStatus::Cancelled);
    assert_eq!(
        current.lifecycle().terminal_usage(),
        Some(&BudgetUsage::zero())
    );
    assert!(current.lease().is_none());
    assert_eq!(reader.observations.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.native.model_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.native.tool_calls.load(Ordering::SeqCst), 0);
    proxied.close().await;
    proxy.stop().await;
    store.close().await;
}

// This adapter observes read-only future retirement, not database connection release.
struct EvidenceReadDrop(HostShutdown);

impl Drop for EvidenceReadDrop {
    fn drop(&mut self) {
        self.0.request();
    }
}

struct DropObservedReader {
    inner: ControlledReader,
    started: Arc<AtomicUsize>,
    dropped: HostShutdown,
}

impl GraphLifecycleEvidenceProvider for DropObservedReader {
    fn terminal_evidence(
        &self,
        context: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>> {
        self.inner.terminal_evidence(context)
    }

    fn failure_evidence(
        &self,
        context: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        self.inner.failure_evidence(context)
    }

    fn cancellation_evidence(
        &self,
        context: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async move {
            let _read_scope = EvidenceReadDrop(self.dropped.clone());
            self.started.fetch_add(1, Ordering::SeqCst);
            self.inner.cancellation_evidence(context).await
        })
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn root_cancellation_shutdown_drops_reader_before_original_renewal_ack() {
    use crate::commit_proxy::{CommitProxy, Cut, TransactionTarget};

    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let connection: PgConnectOptions = std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
    let started = Arc::new(AtomicUsize::new(0));
    // This gate selects the first real renewal after the reader was polled;
    // it does not claim a model/tool launch or fabricate a durable call count.
    let mut proxy = CommitProxy::start_after_node_launch(
        &connection,
        Cut::CommitResponse,
        TransactionTarget::LeaseRenewal,
        started.clone(),
    )
    .await;
    let proxied = PostgresStore::connect(
        connection
            .host("127.0.0.1")
            .port(proxy.port())
            .statement_cache_capacity(0)
            .to_url_lossy()
            .as_str(),
        store.options().clone().with_pool_size(1, 1),
    )
    .await
    .unwrap();
    // Complete proxy/schema setup before acquiring the unchanged short lease.
    let fixture = Box::pin(cancellation_fixture(
        &store,
        "root-cancellation-reader-drop",
    ))
    .await;
    let dropped = HostShutdown::default();
    let reader = Arc::new(DropObservedReader {
        inner: ControlledReader::new(&fixture.native, &proxied, false),
        started,
        dropped: dropped.clone(),
    });
    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        1024,
        Duration::from_millis(100),
        Duration::from_secs(20),
        3,
        Duration::from_millis(25),
    )
    .unwrap();
    let agent_loop = DurableAgentLoop::new(
        proxied.clone(),
        fixture.native.registry.clone(),
        reader.clone(),
        options,
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let shutdown = HostShutdown::default();
    let signal = CancellationSignal::new(shutdown.clone());
    let fence = fixture.lease.fence().clone();
    let mut task = tokio::spawn(async move { agent_loop.run(fence, signal).await });
    reader.inner.wait_entered().await;
    tokio::select! {
        () = proxy.wait_cut(Cut::CommitResponse) => {},
        result = &mut task => panic!("Root ended before its original renewal ACK cut: {result:?}"),
    }
    // Independent Store observation proves the original renewal really committed.
    let renewed = store.load_run(&fixture.tenant, fixture.run).await.unwrap();
    let renewed_same_fence = renewed.lease().unwrap().fence() == fixture.lease.fence();
    shutdown.request();
    let dropped_before_ack = tokio::time::timeout(Duration::from_secs(3), dropped.cancelled())
        .await
        .is_ok();
    let retained_ack = !task.is_finished();
    let held_messages = proxy.messages_while_ack_held();
    // Resume and retire the original ACK/task before asserting the expected RED.
    proxy.resume_ack();
    let result = joined(&mut task, Duration::from_secs(10)).await;
    assert_pending(&store, &fixture).await;
    let released = store.load_run(&fixture.tenant, fixture.run).await.unwrap();
    let original_released = released.lease().is_none();
    let replacement = store
        .claim_lease(&fixture.tenant, fixture.run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let replacement_is_new = replacement.fence().epoch() > fixture.lease.fence().epoch();
    let current = store.load_run(&fixture.tenant, fixture.run).await.unwrap();
    let replacement_retained = current.lease().unwrap().fence() == replacement.fence();
    store.release_lease(replacement.fence()).await.unwrap();
    proxied.close().await;
    proxy.stop().await;
    store.close().await;

    assert!(
        dropped_before_ack,
        "Root must drop its read-only evidence future before draining the original renewal ACK"
    );
    assert!(
        retained_ack,
        "shutdown must retain the original renewal ACK"
    );
    assert_eq!(held_messages, 0);
    assert!(renewed_same_fence);
    assert!(original_released);
    assert!(replacement_is_new && replacement_retained);
    assert!(matches!(
        result.unwrap().outcome(),
        AgentLoopOutcome::RecoveryReleased(_)
    ));
}
