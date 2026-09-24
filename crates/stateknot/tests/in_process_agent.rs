// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real `PostgreSQL` qualification of the typed HTTP-free execution facade.
#![allow(clippy::wildcard_imports, clippy::too_many_lines)]

use serde_json::{Value, json};
use stateknot::runtime::agent_policy::{
    AgentResourcePolicy, PolicyArtifact, RunAccessTarget, RunPermission, RunRule,
};
use stateknot::{
    agent_http::*,
    agent_maintenance::{
        AgentMaintenanceMutationOptions, AgentMaintenanceOptions, AgentMaintenanceReadiness,
        AgentMaintenanceReadinessError,
    },
    agent_worker::*,
    core::*,
    in_process_agent::*,
    postgres::*,
    runtime::*,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Notify, time::timeout};

#[path = "agent_http/fixture.rs"]
#[allow(dead_code)]
mod fixture;
use fixture::{Fixture, ValuePayload};

#[path = "agent_http/execution_evidence.rs"]
mod execution_evidence;
use execution_evidence::evidence;

#[derive(Default)]
struct Host;

impl AgentWorkerReadiness for Host {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentWorkerReadinessError>> {
        Box::pin(async { Ok(()) })
    }
}

impl AgentMaintenanceReadiness for Host {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentMaintenanceReadinessError>> {
        Box::pin(async { Ok(()) })
    }
}

fn runtime_options() -> InProcessAgentRuntimeOptions {
    InProcessAgentRuntimeOptions {
        worker: AgentWorkerOptions::default()
            .with_execution_limits(2, Duration::from_secs(20), Duration::from_secs(2))
            .unwrap()
            .with_pacing(
                Duration::from_millis(10),
                Duration::from_millis(20),
                Duration::from_millis(100),
            )
            .unwrap(),
        maintenance: AgentMaintenanceOptions::default()
            .with_deadlines(Duration::from_secs(20), Duration::from_secs(2))
            .unwrap()
            .with_pacing(Duration::from_millis(10), Duration::from_millis(20))
            .unwrap(),
    }
}

fn request(key: AgentSubmissionKey, value: i64) -> InProcessAgentRequest<ValuePayload> {
    InProcessAgentRequest::new(key, ValuePayload { value }, BudgetLimits::empty())
}

#[tokio::test]
async fn postgres_typed_run_accepts_exact_submission_key_read_without_tenant_wide_access() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let key = AgentSubmissionKey::new(format!("typed-key-only-{}", EventId::generate())).unwrap();
    let mut document = f.resource_document.clone();
    document.runs.push(RunRule {
        tenant: f.caller.tenant_id().clone(),
        principal: f.caller.principal().clone(),
        operation: RunPermission::Read,
        target: RunAccessTarget::Submission(key.digest_for(f.caller.tenant_id())),
    });
    let policy = Arc::new(
        AgentResourcePolicy::new(
            PolicyArtifact::new(document).unwrap(),
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let service = AgentServiceV1::new(
        f.store.clone(),
        f.executable.clone(),
        f.deployments.clone(),
        policy.clone(),
    )
    .unwrap();
    let binding = InProcessAgentBinding::tenant(
        f.store.clone(),
        f.executable.clone(),
        f.deployments.clone(),
        policy,
        evidence(&f),
        f.caller.tenant_id().clone(),
        AgentWorkerExecutionOptions::default(),
        AgentMaintenanceMutationOptions::default(),
    )
    .unwrap();
    let dependencies = InProcessAgentDependencies {
        worker: Arc::new(Host),
        maintenance: Arc::new(Host),
    };
    let mut runtime = InProcessAgentRuntime::start(binding, dependencies, runtime_options())
        .await
        .unwrap();
    let agent = runtime.agent(f.typed.clone(), f.caller.clone()).unwrap();
    let first = agent.run(request(key.clone(), 7)).await.unwrap();
    let InProcessAgentRun::Succeeded { snapshot, .. } = first else {
        panic!("exact-key authorized run must succeed");
    };
    let run_id = snapshot.provenance().run_id();
    assert!(matches!(
        service.load(f.caller.clone(), run_id).await,
        Err(AgentServiceError::Authorization(
            AgentServiceAuthorizationError::Denied
        ))
    ));
    assert_eq!(
        service
            .load_by_key(f.caller.clone(), &key)
            .await
            .unwrap()
            .provenance()
            .run_id(),
        run_id,
    );
    let other_key =
        AgentSubmissionKey::new(format!("typed-ungranted-{}", EventId::generate())).unwrap();
    assert!(matches!(
        service.load_by_key(f.caller.clone(), &other_key).await,
        Err(AgentServiceError::Authorization(
            AgentServiceAuthorizationError::Denied
        ))
    ));
    let repeated = agent.run(request(key, 7)).await.unwrap();
    assert!(matches!(
        repeated,
        InProcessAgentRun::Succeeded { snapshot, .. }
            if snapshot.provenance().run_id() == run_id
    ));
    drop(agent);
    let report = runtime.shutdown().await.unwrap();
    assert!(report.failure.is_none());
    assert!(report.worker.unwrap().is_ok());
    assert!(report.maintenance.unwrap().is_ok());
}

#[tokio::test]
async fn postgres_typed_run_is_idempotent_and_resumes_after_wait_timeout() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let binding = InProcessAgentBinding::tenant(
        f.store.clone(),
        f.executable.clone(),
        f.deployments.clone(),
        f.policy.clone(),
        evidence(&f),
        f.caller.tenant_id().clone(),
        AgentWorkerExecutionOptions::default(),
        AgentMaintenanceMutationOptions::default(),
    )
    .unwrap();
    let dependencies = InProcessAgentDependencies {
        worker: Arc::new(Host),
        maintenance: Arc::new(Host),
    };
    let mut runtime = InProcessAgentRuntime::start(binding, dependencies, runtime_options())
        .await
        .unwrap();
    assert_eq!(
        runtime.health().status(),
        InProcessAgentRuntimeStatus::Ready
    );

    let key = AgentSubmissionKey::new(format!("typed-{}", EventId::generate())).unwrap();
    let agent = runtime
        .agent(f.typed.clone(), f.caller.clone())
        .unwrap()
        .with_options(
            InProcessAgentRunOptions::new(Duration::from_millis(10), Duration::from_secs(5))
                .unwrap(),
        );
    let first = agent.run(request(key.clone(), 1)).await.unwrap();
    match first {
        InProcessAgentRun::Succeeded { output, snapshot } => {
            assert_eq!(output, ValuePayload { value: 1 });
            assert_eq!(snapshot.status(), RunStatus::Succeeded);
        }
        other => panic!("expected successful typed run, got {other:?}"),
    }
    let calls = f.node_calls.load(Ordering::SeqCst);
    assert_eq!(calls, 1);
    assert!(matches!(
        agent.run(request(key, 1)).await.unwrap(),
        InProcessAgentRun::Succeeded { .. }
    ));
    assert_eq!(f.node_calls.load(Ordering::SeqCst), calls);

    f.node_block.store(true, Ordering::SeqCst);
    let recovery_key =
        AgentSubmissionKey::new(format!("typed-recovery-{}", EventId::generate())).unwrap();
    let short = runtime
        .agent(f.typed.clone(), f.caller.clone())
        .unwrap()
        .with_options(
            InProcessAgentRunOptions::new(Duration::from_millis(10), Duration::from_millis(50))
                .unwrap(),
        );
    let pending = short.run(request(recovery_key.clone(), 1)).await.unwrap();
    assert!(matches!(
        pending,
        InProcessAgentRun::Pending {
            reason: InProcessAgentPendingReason::WaitTimeout,
            ..
        }
    ));
    timeout(Duration::from_secs(5), async {
        while f.node_calls.load(Ordering::SeqCst) == calls {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the preserved run must remain dispatchable after caller timeout");
    assert_eq!(f.node_calls.load(Ordering::SeqCst), calls + 1);
    f.node_block.store(false, Ordering::SeqCst);
    f.node_release.notify_waiters();
    let resumed = timeout(Duration::from_secs(10), agent.run(request(recovery_key, 1)))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(resumed, InProcessAgentRun::Succeeded { .. }));
    assert_eq!(f.node_calls.load(Ordering::SeqCst), calls + 1);

    runtime.begin_shutdown();
    assert_eq!(
        runtime.health().status(),
        InProcessAgentRuntimeStatus::Draining
    );
    let rejected = agent
        .run(request(
            AgentSubmissionKey::new(format!("after-drain-{}", EventId::generate())).unwrap(),
            1,
        ))
        .await;
    assert!(matches!(
        rejected,
        Err(InProcessAgentRunError::RuntimeUnavailable)
    ));
    drop(short);
    drop(agent);
    let report = runtime.wait().await.unwrap();
    assert_eq!(report.failure, None);
    assert_eq!(report.worker.unwrap().unwrap().failure, None);
    assert_eq!(report.maintenance.unwrap().unwrap().failure, None);
    assert_eq!(
        runtime.health().status(),
        InProcessAgentRuntimeStatus::Stopped
    );
    drop(runtime);
    f.store.close().await;
    println!(
        "\nSTATEKNOT_IN_PROCESS_AGENT_EVIDENCE={{\"typed_terminal\":true,\"same_key_idempotent\":true,\"timeout_preserves_run\":true,\"same_key_resume\":true,\"drain_closes_submission\":true,\"roles_joined\":true}}"
    );
}
