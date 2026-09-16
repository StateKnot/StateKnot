// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot::runtime::agent_policy::*;

pub(super) fn operator_document(f: &Fixture) -> PolicyDocument {
    let mut doc = f.resource_document.clone();
    for operation in [RunPermission::Read, RunPermission::Cancel] {
        doc.runs.push(RunRule {
            tenant: f.caller.tenant_id().clone(),
            principal: f.caller.principal().clone(),
            operation,
            target: RunAccessTarget::TenantRuns,
        });
    }
    doc
}
pub(super) fn install(f: &Fixture, policy: Arc<AgentResourcePolicy>) -> AgentServiceV1 {
    AgentServiceV1::new(
        f.store.clone(),
        f.executable.clone(),
        f.deployments.clone(),
        policy,
    )
    .unwrap()
}
fn artifact(doc: &PolicyDocument) -> PolicyArtifact {
    PolicyArtifact::new(doc.clone()).unwrap()
}

#[tokio::test]
async fn exact_resource_policy_http_evidence_recovery_revocation_and_expiry() {
    let Some(mut f) = Fixture::new().await else {
        return;
    };
    let mut doc = f.resource_document.clone();
    let submission = f.submission();
    let key_digest = submission.submission_key.digest_for(f.caller.tenant_id());
    // Grant key lookup explicitly before submission; no implicit run ownership.
    doc.runs.push(RunRule {
        tenant: f.caller.tenant_id().clone(),
        principal: f.caller.principal().clone(),
        operation: RunPermission::Read,
        target: RunAccessTarget::Submission(key_digest),
    });
    let retained = artifact(&doc);
    let retained =
        PolicyArtifact::from_json(retained.canonical_bytes(), retained.digest()).unwrap();
    let policy = Arc::new(AgentResourcePolicy::new(retained, Duration::from_secs(60)).unwrap());
    f.service = install(&f, policy.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let http = AgentHttpService::new(
        f.service.clone(),
        f.auth.clone(),
        AgentHttpOptions::loopback(address.port())
            .unwrap()
            .with_sse(
                AgentHttpSseOptions::new(
                    2,
                    Duration::from_secs(30),
                    Duration::from_millis(50),
                    Duration::from_secs(1),
                )
                .unwrap(),
            ),
    );
    let mut owned = AgentHttpServer::start(
        listener,
        http,
        policy.clone(),
        AgentHttpServerOptions::default(),
    )
    .await
    .unwrap();
    let health = owned.health();
    let url = format!("http://{address}/v1/agent-runs");
    let client = client();

    // Commit while withholding the first HTTP response, then recover on another listener.
    let gate = Arc::new(LossGate::default());
    let lossy = server(&f, None, Duration::from_secs(10), Some(gate.clone())).await;
    let request = post(&lossy, "/v1/agent-runs", &submission);
    let pending = tokio::spawn(async move { request.send().await });
    tokio::time::timeout(Duration::from_secs(10), gate.committed.notified())
        .await
        .unwrap();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    gate.release.notify_waiters();
    let loaded = snapshot(
        client
            .post(format!("{url}/lookup"))
            .bearer_auth(TOKEN)
            .json(&AgentHttpLookup {
                submission_key: submission.submission_key.clone(),
            })
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let run = loaded.provenance().run_id();
    let run_url = format!("{url}/{run}");
    assert_eq!(
        client
            .get(&run_url)
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .get(format!("{url}/{}", RunId::generate()))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    // Authorize this run only. Adding unrelated run rules must not alter submission evidence.
    for operation in [RunPermission::Read, RunPermission::Cancel] {
        doc.runs.push(RunRule {
            tenant: f.caller.tenant_id().clone(),
            principal: f.caller.principal().clone(),
            operation,
            target: RunAccessTarget::Run(run),
        });
    }
    policy
        .replace(1, artifact(&doc), Duration::from_secs(60))
        .unwrap();
    let recovered = snapshot(
        client
            .post(&url)
            .bearer_auth(TOKEN)
            .json(&submission)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(recovered.provenance().run_id(), run);
    assert_eq!(
        client
            .get(&run_url)
            .bearer_auth("other-tenant-fixture")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let stored = f
        .store
        .load_agent_admission(f.caller.tenant_id(), run)
        .await
        .unwrap();
    let intent = stored.admission().intent();
    assert_eq!(intent.authority().policy(), &doc.policy);
    assert_eq!(
        intent.authority().evidence().schema(),
        &agent_policy_evidence_schema().unwrap().0
    );
    assert_eq!(
        intent.budget_layers()[0].decision_digest(),
        intent.authority().evidence().digest()
    );
    assert_eq!(
        intent.budget_layers()[0].limits(),
        &doc.submissions[0].budget_limits
    );

    // Open live SSE then revoke only its resource grant, with identity still valid.
    let mut stream = client
        .get(format!("{run_url}/events"))
        .bearer_auth(TOKEN)
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(stream.status(), 200);
    assert!(stream.chunk().await.unwrap().is_some());
    let read_doc = doc.clone();
    doc.runs
        .retain(|rule| rule.operation != RunPermission::Read);
    policy
        .replace(2, artifact(&doc), Duration::from_secs(60))
        .unwrap();
    assert_eq!(
        client
            .get(&run_url)
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut bytes = 0;
        while let Some(chunk) = stream.chunk().await.unwrap() {
            bytes += chunk.len();
            assert!(bytes < 2_097_152);
        }
        while health.active_streams() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    policy
        .replace(3, artifact(&read_doc), Duration::from_secs(60))
        .unwrap();

    let ids = AgentCancellationIds::generate();
    let cancelling = snapshot(
        client
            .post(format!("{run_url}/cancellation"))
            .bearer_auth(TOKEN)
            .json(&ids)
            .send()
            .await
            .unwrap(),
        202,
    )
    .await;
    assert_eq!(cancelling.status(), RunStatus::CancellationRequested);
    snapshot(
        client
            .post(format!("{run_url}/cancellation"))
            .bearer_auth(TOKEN)
            .json(&ids)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    // Changing the selected admission rule cannot silently reuse obsolete authority.
    let mut changed = read_doc.clone();
    changed.submissions[0].budget_limits = changed.submissions[0]
        .budget_limits
        .clone()
        .with_tool_calls(ExecutionCount::new(0));
    policy
        .replace(4, artifact(&changed), Duration::from_secs(60))
        .unwrap();
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(TOKEN)
            .json(&submission)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    snapshot(
        client
            .post(format!("{url}/lookup"))
            .bearer_auth(TOKEN)
            .json(&AgentHttpLookup {
                submission_key: submission.submission_key,
            })
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;

    policy
        .replace(5, artifact(&read_doc), Duration::from_millis(10))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while policy.check_readiness().is_ok() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(policy.check().await.is_err());
    assert_eq!(
        client
            .get(&run_url)
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
        503
    );
    policy
        .replace(6, artifact(&read_doc), Duration::from_secs(60))
        .unwrap();
    assert!(policy.check().await.is_ok());
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 0);
    owned.shutdown().await.unwrap();
    assert_eq!(health.active_streams(), 0);
    assert_eq!(health.active_connections(), 0);
    println!(
        "\nSTATEKNOT_RESOURCE_POLICY_EVIDENCE=exact_targets;cross_tenant_denied;lost_submit_recovered;unrelated_refresh_stable;durable_evidence;cancel_idempotent;revoked_sse_closed;expired_unavailable;no_inline_dispatch"
    );
}
