// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use stateknot::{
    agent_host::{AgentHost, AgentHostStatus},
    postgres::JournalPageSize,
};
use stateknot_testkit::{
    FaultCaseId, FaultPlan, IntegrityEnvelope, LatencySignal, MachineEnvironment, OperationOutcome,
    PostgresEnvironment, QualificationEnvironment, QualificationProfile, QualificationRun,
    QualificationScenario, QualificationWindow, SourceIdentity, StandbyMode, Topology,
};
use std::time::Instant;

struct Events {
    response: reqwest::Response,
    buffer: String,
}

impl Events {
    async fn open(host: &AgentHost, run: RunId, last_event_id: Option<&str>) -> Self {
        let mut request = host::client()
            .get(format!(
                "http://{}/v1/agent-runs/{run}/events",
                host.local_addr()
            ))
            .bearer_auth(fixture::TOKEN)
            .header(reqwest::header::ACCEPT, "text/event-stream");
        if let Some(last_event_id) = last_event_id {
            request = request.header("last-event-id", last_event_id);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), 200);
        Self {
            response,
            buffer: String::new(),
        }
    }

    async fn next(&mut self, kind: &str) -> (Option<String>, Value) {
        timeout(Duration::from_secs(5), async {
            loop {
                if let Some(frame) = take_frame(&mut self.buffer, kind) {
                    let id = frame
                        .lines()
                        .find_map(|line| line.strip_prefix("id: "))
                        .map(str::to_owned);
                    let data = frame
                        .lines()
                        .find_map(|line| line.strip_prefix("data: "))
                        .unwrap();
                    return (id, serde_json::from_str(data).unwrap());
                }
                let chunk =
                    self.response.chunk().await.unwrap().unwrap_or_else(|| {
                        panic!("qualification SSE ended while waiting for {kind}")
                    });
                self.buffer.push_str(std::str::from_utf8(&chunk).unwrap());
                assert!(self.buffer.len() <= 8_388_608);
            }
        })
        .await
        .unwrap()
    }
}

fn take_frame(buffer: &mut String, kind: &str) -> Option<String> {
    let prefix = format!("event: {kind}\n");
    let mut offset = 0;
    for frame in buffer.split_inclusive("\n\n") {
        if !frame.ends_with("\n\n") {
            break;
        }
        let end = offset + frame.len();
        if frame.starts_with(&prefix) {
            return Some(buffer.drain(offset..end).collect());
        }
        offset = end;
    }
    None
}

fn env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("mandatory qualification metadata missing: {name}"))
}

async fn environment(f: &Fixture) -> QualificationEnvironment {
    let started = Instant::now();
    f.store.observe_database_clock().await.unwrap();
    let database_rtt = micros(started.elapsed());
    let cpus = env("STATEKNOT_QUALIFICATION_LOGICAL_CPUS")
        .parse::<u16>()
        .unwrap();
    let memory = env("STATEKNOT_QUALIFICATION_MEMORY_BYTES")
        .parse::<u64>()
        .unwrap();
    let cpu_model = env("STATEKNOT_QUALIFICATION_CPU_MODEL");
    let kernel = env("STATEKNOT_QUALIFICATION_KERNEL");
    let source = SourceIdentity::new(
        env("STATEKNOT_QUALIFICATION_SOURCE_COMMIT"),
        env("STATEKNOT_QUALIFICATION_SOURCE_TREE"),
        env("STATEKNOT_QUALIFICATION_CARGO_LOCK_SHA256"),
        env("STATEKNOT_QUALIFICATION_DATASET_SHA256"),
        env("STATEKNOT_QUALIFICATION_CONFIGURATION_SHA256"),
    )
    .unwrap();
    QualificationEnvironment::new(
        source,
        MachineEnvironment::new(
            cpus,
            memory,
            &cpu_model,
            "github-actions-ephemeral",
            &kernel,
            "github-actions-host",
        )
        .unwrap(),
        MachineEnvironment::new(
            cpus,
            memory,
            cpu_model,
            "github-actions-ephemeral",
            kernel,
            "github-actions-service-container",
        )
        .unwrap(),
        PostgresEnvironment::new(
            env("STATEKNOT_QUALIFICATION_POSTGRES_MAJOR")
                .parse()
                .unwrap(),
            true,
            StandbyMode::None,
        )
        .unwrap(),
        Topology::new(1, 1, 1, 1).unwrap(),
        database_rtt,
    )
    .unwrap()
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros())
        .unwrap()
        .clamp(1, 3_600_000_000)
}

async fn qualification_until(label: &str, mut predicate: impl FnMut() -> bool) {
    timeout(Duration::from_secs(30), async {
        while !predicate() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("qualification timed out waiting for {label}"));
}

async fn runnable_claim_latency(f: &Fixture, run: RunId) -> Duration {
    let page = f
        .store
        .load_journal_page(
            f.caller.tenant_id(),
            run,
            None,
            JournalPageSize::new(64).unwrap(),
        )
        .await
        .unwrap();
    assert!(!page.has_more());
    let admitted = page
        .events()
        .iter()
        .find(|event| event.payload().kind().as_str() == "agent-admitted")
        .unwrap()
        .recorded_at()
        .unix_micros();
    let claimed = page
        .events()
        .iter()
        .find(|event| event.payload().kind().as_str() == "graph-node-attempt-started")
        .unwrap()
        .recorded_at()
        .unix_micros();
    Duration::from_micros(
        u64::try_from(claimed.checked_sub(admitted).unwrap())
            .unwrap()
            .max(1),
    )
}

fn encoded_head(head: &JournalHead) -> String {
    format!(
        "sk1.{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(head).unwrap())
    )
}

async fn submit_and_complete(
    host: &AgentHost,
    f: &Fixture,
    recorder: &stateknot_testkit::QualificationRecorder,
) -> RunId {
    let operation = recorder.begin_operation().unwrap();
    let started = Instant::now();
    let response = host::submit(host, f).await;
    recorder
        .record_latency(LatencySignal::AdmissionCommit, started.elapsed(), None)
        .unwrap();
    assert_eq!(response.status(), 201);
    let run = response
        .json::<AgentHttpRunResponse>()
        .await
        .unwrap()
        .snapshot
        .provenance()
        .run_id();
    succeeded(f, run).await;
    recorder
        .record_latency(
            LatencySignal::RunnableClaim,
            runnable_claim_latency(f, run).await,
            None,
        )
        .unwrap();
    operation.finish(OperationOutcome::Completed).unwrap();
    run
}

#[tokio::test]
async fn postgres_reduced_host_qualification_emits_verified_non_release_evidence() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let plan = FaultPlan::ci_default();
    let run = QualificationRun::start(
        QualificationProfile::CiReduced,
        QualificationScenario::AgentHostControlPlaneV1,
        QualificationWindow::new(
            Duration::from_millis(1),
            Duration::from_millis(1),
            Duration::from_millis(1),
        )
        .unwrap(),
        environment(&f).await,
        &plan,
    )
    .unwrap();
    let checks_a = host::checks();
    let (mut host_a, _) = host::launch(&f, &checks_a, evidence(&f), host::limits()).await;
    host::ready(&host_a).await;
    let (mut operations, _) =
        operations::start(&host_a, operations::auth(&f), |options| options).await;
    sleep(Duration::from_millis(2)).await;
    run.begin_measurement().unwrap();
    let recorder = run.recorder();

    let first = submit_and_complete(&host_a, &f, &recorder).await;
    let mut replay = Events::open(&host_a, first, None).await;
    replay.next("snapshot").await;
    let (cursor, _) = replay.next("activity").await;
    let reconnect_started = Instant::now();
    let mut reconnected = Events::open(&host_a, first, cursor.as_deref()).await;
    reconnected.next("snapshot").await;
    reconnected.next("activity").await;
    recorder
        .record_latency(
            LatencySignal::SseReconnect,
            reconnect_started.elapsed(),
            None,
        )
        .unwrap();
    drop(replay);
    drop(reconnected);

    f.node_block.store(true, Ordering::SeqCst);
    let blocked = host::submit(&host_a, &f).await;
    assert_eq!(blocked.status(), 201);
    let blocked = blocked
        .json::<AgentHttpRunResponse>()
        .await
        .unwrap()
        .snapshot
        .provenance()
        .run_id();
    qualification_until("the live-SSE node to enter execution", || {
        host_a.health().worker().unwrap().active_nodes() > 0
    })
    .await;
    let page = f
        .store
        .load_journal_page(
            f.caller.tenant_id(),
            blocked,
            None,
            JournalPageSize::new(64).unwrap(),
        )
        .await
        .unwrap();
    let head = page.next_cursor().unwrap();
    let mut live = Events::open(&host_a, blocked, Some(&encoded_head(&head))).await;
    live.next("snapshot").await;
    let delivery_started = Instant::now();
    f.service
        .request_cancellation(f.caller.clone(), blocked, AgentCancellationIds::generate())
        .await
        .unwrap();
    live.next("activity").await;
    recorder
        .record_latency(LatencySignal::SseDelivery, delivery_started.elapsed(), None)
        .unwrap();
    drop(live);
    f.node_block.store(false, Ordering::SeqCst);
    f.node_release.notify_one();
    timeout(Duration::from_secs(12), async {
        loop {
            let snapshot = f.service.load(f.caller.clone(), blocked).await.unwrap();
            if snapshot.status() == RunStatus::Cancelled {
                break;
            }
            assert!(matches!(
                snapshot.status(),
                RunStatus::Active | RunStatus::CancellationRequested
            ));
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    recorder
        .fault_injected(FaultCaseId::HostDependencyUnavailable)
        .unwrap();
    checks_a[2].mode.store(1, Ordering::SeqCst);
    qualification_until("the host dependency gate to fail closed", || {
        host_a.health().status() == AgentHostStatus::Unavailable
    })
    .await;
    assert_eq!(host::submit(&host_a, &f).await.status(), 503);
    recorder
        .fault_invariant_checked(FaultCaseId::HostDependencyUnavailable)
        .unwrap();
    assert_eq!(
        operations::get(&operations, "/v1/host/ready", "ops-fixture")
            .await
            .status(),
        503
    );
    checks_a[2].mode.store(0, Ordering::SeqCst);
    host::ready(&host_a).await;
    recorder
        .fault_recovered(FaultCaseId::HostDependencyUnavailable)
        .unwrap();
    assert_eq!(
        operations::get(&operations, "/v1/host/ready", "ops-fixture")
            .await
            .status(),
        200
    );
    recorder
        .fault_invariant_checked(FaultCaseId::HostDependencyUnavailable)
        .unwrap();

    recorder
        .fault_injected(FaultCaseId::HostRollingReplacement)
        .unwrap();
    let checks_b = host::checks();
    let (mut host_b, _) = host::launch(&f, &checks_b, evidence(&f), host::limits()).await;
    host::ready(&host_b).await;
    recorder
        .fault_invariant_checked(FaultCaseId::HostRollingReplacement)
        .unwrap();
    host_a.shutdown().await.unwrap();
    host::joined(&host_a, &f).await;
    recorder
        .fault_invariant_checked(FaultCaseId::HostRollingReplacement)
        .unwrap();
    let stopped = operations::get(&operations, "/v1/host/status", "ops-fixture").await;
    assert_eq!(stopped.status(), 200);
    let second = submit_and_complete(&host_b, &f, &recorder).await;
    assert_ne!(first, second);
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 3);
    recorder
        .fault_invariant_checked(FaultCaseId::HostRollingReplacement)
        .unwrap();
    recorder
        .fault_recovered(FaultCaseId::HostRollingReplacement)
        .unwrap();

    host_b.shutdown().await.unwrap();
    host::joined(&host_b, &f).await;
    operations.shutdown().await.unwrap();
    sleep(Duration::from_millis(2)).await;
    run.begin_drain().unwrap();
    sleep(Duration::from_millis(2)).await;
    let report = run.finish().unwrap();
    assert!(report.evidence_valid());
    assert!(!report.release_qualified());
    let envelope = report.to_integrity_envelope().unwrap();
    let bytes = envelope.canonical_bytes().unwrap();
    assert_eq!(IntegrityEnvelope::from_json(&bytes).unwrap(), envelope);
    let text = String::from_utf8(bytes).unwrap();
    for secret in [
        f.caller.tenant_id().as_str(),
        fixture::TOKEN,
        &first.to_string(),
        &second.to_string(),
    ] {
        assert!(!text.contains(secret));
    }
    f.store.close().await;
    println!("\nSTATEKNOT_HOST_QUALIFICATION_REPORT={text}");
}
