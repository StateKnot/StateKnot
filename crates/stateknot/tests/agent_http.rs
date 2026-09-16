// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real HTTP plus mandatory `PostgreSQL` qualification; credentials are test-only.
#![allow(
    clippy::wildcard_imports,
    clippy::too_many_lines,
    clippy::similar_names
)]

use axum::{
    extract::{Request, State},
    middleware::{self, Next},
    response::Response,
};
use serde_json::{Value, json};
use stateknot::{agent_http::*, core::*, postgres::*, runtime::*};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};

#[path = "agent_http/fixture.rs"]
mod fixture;
use fixture::*;
#[path = "agent_http/identity.rs"]
mod identity;
#[path = "agent_http/server.rs"]
mod owned_server;
#[path = "agent_http/sse.rs"]
mod sse;

#[derive(Default)]
struct LossGate {
    committed: Notify,
    release: Notify,
}
async fn lose_response(
    State(gate): State<Arc<LossGate>>,
    request: Request,
    next: Next,
) -> Response {
    let response = next.run(request).await;
    // Register the release waiter BEFORE reporting commit, so no wake is lost.
    let release = gate.release.notified();
    tokio::pin!(release);
    release.as_mut().enable();
    gate.committed.notify_one();
    release.await;
    response
}

struct Server {
    url: String,
    http: AgentHttpService,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.http.shutdown();
        self.task.abort();
    }
}
async fn server(
    f: &Fixture,
    limits: Option<(usize, usize, usize)>,
    deadline: Duration,
    gate: Option<Arc<LossGate>>,
) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut options = AgentHttpOptions::loopback(address.port())
        .unwrap()
        .with_deadline(deadline)
        .unwrap();
    if let Some((request, response, concurrency)) = limits {
        options = options.with_limits(request, response, concurrency).unwrap();
    }
    let http = AgentHttpService::new(f.service.clone(), f.auth.clone(), options);
    let mut router = http.router();
    if let Some(gate) = gate {
        router = router.layer(middleware::from_fn_with_state(gate, lose_response));
    }
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Server {
        url: format!("http://{address}"),
        http,
        task,
    }
}
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .no_proxy()
        .build()
        .unwrap()
}
fn post(server: &Server, path: &str, body: &impl serde::Serialize) -> reqwest::RequestBuilder {
    client()
        .post(format!("{}{path}", server.url))
        .bearer_auth(TOKEN)
        .json(body)
}
fn replace_header(
    request: reqwest::RequestBuilder,
    name: &str,
    value: &str,
) -> reqwest::RequestBuilder {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
        value.parse().unwrap(),
    );
    request.headers(headers)
}
async fn snapshot(response: reqwest::Response, status: u16) -> AgentRunSnapshot {
    assert_eq!(response.status().as_u16(), status);
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    assert_eq!(response.headers()["stateknot-api-version"], "1");
    let request_id = response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let bytes = response.bytes().await.unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("never-log"));
    let envelope: AgentHttpRunResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(request_id, envelope.request_id.to_string());
    envelope.snapshot
}
async fn error(response: reqwest::Response, status: u16, code: &str) {
    assert_eq!(response.status().as_u16(), status);
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    assert!(
        !response
            .headers()
            .contains_key("access-control-allow-origin")
    );
    let value: Value = response.json().await.unwrap();
    assert_eq!(value["error"]["code"], format!("agent_http.{code}"));
    assert_eq!(value["error"].as_object().unwrap().len(), 2);
}

async fn chunked_over_limit(server: &Server) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let address = server.url.strip_prefix("http://").unwrap();
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let request = format!(
        "POST /v1/agent-runs HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\nc0\r\n{}\r\n0\r\n\r\n",
        "a".repeat(192)
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        socket.read_to_string(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(response.starts_with("HTTP/1.1 413"), "{response}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_http_commit_loss_recovery_concurrency_and_cancellation() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let server = server(&f, None, Duration::from_secs(10), None).await;
    let submission = f.submission();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let request = post(&server, "/v1/agent-runs", &submission);
        tasks.spawn(async move {
            let response = request.send().await.unwrap();
            let status = response.status().as_u16();
            assert!(status == 200 || status == 201, "status={status}");
            (status, snapshot(response, status).await)
        });
    }
    let mut created = 0;
    let mut runs = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let (status, run) = result.unwrap();
        created += usize::from(status == 201);
        runs.push(run.provenance().run_id());
    }
    assert_eq!(created, 1);
    assert!(runs.iter().all(|run| *run == runs[0]));
    let run = runs[0];
    let loaded = snapshot(
        post(
            &server,
            "/v1/agent-runs/lookup",
            &AgentHttpLookup {
                submission_key: submission.submission_key.clone(),
            },
        )
        .send()
        .await
        .unwrap(),
        200,
    )
    .await;
    assert_eq!(loaded.provenance().run_id(), run);
    let path = format!("/v1/agent-runs/{run}");
    let read = client()
        .get(format!("{}{path}", server.url))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(snapshot(read, 200).await.provenance().run_id(), run);
    let mut conflict = submission.clone();
    conflict.request = AgentRequest::new(
        f.schema.clone(),
        bounded(json!({"value":2})),
        BudgetLimits::empty(),
    );
    error(
        post(&server, "/v1/agent-runs", &conflict)
            .send()
            .await
            .unwrap(),
        409,
        "conflict",
    )
    .await;

    // Observe a committed mutation while deliberately withholding its HTTP response.
    // Abort the client before recovery through a fresh independent listener.
    let gate = Arc::new(LossGate::default());
    let lossy = self::server(&f, None, Duration::from_secs(10), Some(gate.clone())).await;
    let lost_submission = f.submission();
    let request = post(&lossy, "/v1/agent-runs", &lost_submission);
    let pending = tokio::spawn(async move { request.send().await });
    tokio::time::timeout(Duration::from_secs(10), gate.committed.notified())
        .await
        .unwrap();
    let committed = f
        .service
        .load_by_key(f.caller.clone(), &lost_submission.submission_key)
        .await
        .unwrap();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    gate.release.notify_waiters();
    let recovered = snapshot(
        post(&server, "/v1/agent-runs", &lost_submission)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(
        recovered.provenance().run_id(),
        committed.provenance().run_id()
    );

    let ids = AgentCancellationIds::generate();
    let cancel_path = format!("{path}/cancellation");
    let request = post(&lossy, &cancel_path, &ids);
    let pending = tokio::spawn(async move { request.send().await });
    tokio::time::timeout(Duration::from_secs(10), gate.committed.notified())
        .await
        .unwrap();
    let stored = f
        .store
        .load_agent_admission(f.caller.tenant_id(), run)
        .await
        .unwrap();
    let cancellation = stored.run().lifecycle().cancellation_request().unwrap();
    assert_eq!(cancellation.failure().id(), ids.failure_id());
    assert_eq!(
        cancellation.failure().caused_by_event_id(),
        Some(ids.event_id())
    );
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    gate.release.notify_waiters();
    let cancelled = snapshot(post(&server, &cancel_path, &ids).send().await.unwrap(), 200).await;
    assert_eq!(cancelled.status(), RunStatus::CancellationRequested);
    assert!(
        cancelled.outcome().is_none(),
        "request is not terminal cancellation"
    );
    error(
        post(&server, &cancel_path, &AgentCancellationIds::generate())
            .send()
            .await
            .unwrap(),
        409,
        "conflict",
    )
    .await;
    let raced = snapshot(
        post(&server, "/v1/agent-runs", &f.submission())
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let race_path = format!(
        "/v1/agent-runs/{}/cancellation",
        raced.provenance().run_id()
    );
    let mut cancellations = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let ids = AgentCancellationIds::generate();
        let request = post(&server, &race_path, &ids);
        cancellations.spawn(async move { (ids, request.send().await.unwrap()) });
    }
    let mut winners = Vec::new();
    while let Some(result) = cancellations.join_next().await {
        let (ids, response) = result.unwrap();
        if response.status().as_u16() == 202 {
            winners.push(ids);
            snapshot(response, 202).await;
        } else {
            error(response, 409, "conflict").await;
        }
    }
    assert_eq!(winners.len(), 1);
    snapshot(
        post(&server, &race_path, &winners[0]).send().await.unwrap(),
        200,
    )
    .await;
    assert_eq!(
        f.node_calls.load(Ordering::SeqCst),
        0,
        "HTTP never dispatches graph/provider work"
    );
    f.store.close().await;
    println!(
        "\nSTATEKNOT_AGENT_HTTP_EVIDENCE=24_concurrent_submissions_one_run;24_cancellation_races_one_commit;lost_submit_recovered;lost_cancel_recovered;no_inline_dispatch"
    );
}

#[tokio::test]
async fn postgres_http_adversarial_envelopes_authorization_and_recovery_limits() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let server = server(&f, None, Duration::from_secs(5), None).await;
    let submission = f.submission();
    let root = "/v1/agent-runs";
    // Unauthenticated requests cannot observe routing, parsing, or durable existence.
    error(
        client()
            .post(format!("{}/not-a-route", server.url))
            .body("{never-log")
            .send()
            .await
            .unwrap(),
        401,
        "unauthenticated",
    )
    .await;
    assert_eq!(f.policy_calls.load(Ordering::SeqCst), 0);
    error(
        replace_header(
            post(&server, root, &submission),
            "authorization",
            "Bearer read-only-fixture",
        )
        .send()
        .await
        .unwrap(),
        403,
        "denied",
    )
    .await;
    assert_eq!(f.policy_calls.load(Ordering::SeqCst), 0);
    for (name, value, status, code) in [
        ("host", "attacker.invalid", 403, "denied"),
        ("origin", "https://attacker.invalid", 403, "denied"),
        ("content-type", "text/plain", 415, "unsupported_media_type"),
        ("content-encoding", "gzip", 415, "unsupported_media_type"),
        ("accept", "text/event-stream", 406, "not_acceptable"),
    ] {
        error(
            replace_header(post(&server, root, &submission), name, value)
                .send()
                .await
                .unwrap(),
            status,
            code,
        )
        .await;
    }
    for body in [
        "{\"submission_key\":\"a\",\"submission_key\":\"b\"}".to_owned(),
        serde_json::to_string(&submission).unwrap().replacen(
            "\"value\":1",
            "\"value\":1,\"value\":2",
            1,
        ),
        {
            let mut v = serde_json::to_value(&submission).unwrap();
            v["tenant_id"] = json!("victim");
            v.to_string()
        },
        "{".to_owned(),
    ] {
        error(
            client()
                .post(format!("{}{root}", server.url))
                .bearer_auth(TOKEN)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .unwrap(),
            400,
            "invalid_request",
        )
        .await;
    }
    error(
        post(&server, "/v1/agent-runs?key=never-log", &submission)
            .send()
            .await
            .unwrap(),
        400,
        "invalid_request",
    )
    .await;
    let response = client()
        .delete(format!("{}{root}", server.url))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["allow"], "POST");
    error(response, 405, "method_not_allowed").await;
    let mut bad_input = submission.clone();
    bad_input.request = AgentRequest::new(
        f.schema.clone(),
        bounded(json!({"value":"wrong"})),
        BudgetLimits::empty(),
    );
    error(
        post(&server, root, &bad_input).send().await.unwrap(),
        400,
        "invalid_request",
    )
    .await;
    let accepted = snapshot(post(&server, root, &submission).send().await.unwrap(), 201).await;
    let path = format!("/v1/agent-runs/{}", accepted.provenance().run_id());
    error(
        client()
            .get(format!("{}{path}", server.url))
            .bearer_auth("other-tenant-fixture")
            .send()
            .await
            .unwrap(),
        403,
        "denied",
    )
    .await;
    error(
        client()
            .get(format!("{}{path}", server.url))
            .bearer_auth(TOKEN)
            .body("{}")
            .send()
            .await
            .unwrap(),
        400,
        "invalid_request",
    )
    .await;
    let ids = AgentCancellationIds::generate();
    let read_only = client()
        .get(format!("{}{path}", server.url))
        .bearer_auth("read-only-fixture")
        .send()
        .await
        .unwrap();
    snapshot(read_only, 200).await;
    error(
        replace_header(
            post(&server, &format!("{path}/cancellation"), &ids),
            "authorization",
            "Bearer read-only-fixture",
        )
        .send()
        .await
        .unwrap(),
        403,
        "denied",
    )
    .await;
    let cancelled = snapshot(
        post(&server, &format!("{path}/cancellation"), &ids)
            .send()
            .await
            .unwrap(),
        202,
    )
    .await;
    assert_eq!(cancelled.status(), RunStatus::CancellationRequested);

    let tiny = self::server(&f, Some((128, 1024, 1)), Duration::from_secs(5), None).await;
    chunked_over_limit(&tiny).await;
    error(
        post(&tiny, root, &submission).send().await.unwrap(),
        413,
        "request_too_large",
    )
    .await;
    let response_limited =
        self::server(&f, Some((262_144, 1024, 1)), Duration::from_secs(5), None).await;
    let large_response = f.submission();
    error(
        post(&response_limited, root, &large_response)
            .send()
            .await
            .unwrap(),
        500,
        "response_too_large",
    )
    .await;
    snapshot(
        post(&server, root, &large_response).send().await.unwrap(),
        200,
    )
    .await;
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 0);

    // Resource authorization remains ahead of storage even when DB is unavailable.
    f.denied.store(true, Ordering::SeqCst);
    f.store.close().await;
    error(
        client()
            .get(format!("{}{path}", server.url))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap(),
        403,
        "denied",
    )
    .await;
    error(
        post(&server, root, &f.submission()).send().await.unwrap(),
        403,
        "denied",
    )
    .await;
    f.denied.store(false, Ordering::SeqCst);
    error(
        client()
            .get(format!("{}{path}", server.url))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap(),
        503,
        "unavailable",
    )
    .await;
}

#[tokio::test]
async fn postgres_http_slow_body_auth_deadline_overload_and_shutdown() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let Some(f) = Fixture::new().await else {
        return;
    };
    let server = server(&f, Some((1024, 4096, 1)), Duration::from_secs(1), None).await;
    let address = server.url.strip_prefix("http://").unwrap();
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    socket.write_all(format!("POST /v1/agent-runs HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: 10\r\nConnection: close\r\n\r\n{{").as_bytes()).await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(3), socket.read_to_string(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(response.starts_with("HTTP/1.1 503"), "{response}");
    f.auth.block.store(true, Ordering::SeqCst);
    let request = post(&server, "/v1/agent-runs", &f.submission());
    let pending = tokio::spawn(async move { request.send().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(3), f.auth.entered.notified())
        .await
        .unwrap();
    error(
        post(&server, "/v1/agent-runs", &f.submission())
            .send()
            .await
            .unwrap(),
        429,
        "overloaded",
    )
    .await;
    error(pending.await.unwrap(), 503, "unavailable").await;
    let request = post(&server, "/v1/agent-runs", &f.submission());
    let pending = tokio::spawn(async move { request.send().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(3), f.auth.entered.notified())
        .await
        .unwrap();
    server.http.shutdown();
    error(pending.await.unwrap(), 503, "unavailable").await;
    f.auth.block.store(false, Ordering::SeqCst);
    error(
        post(&server, "/v1/agent-runs", &f.submission())
            .send()
            .await
            .unwrap(),
        503,
        "unavailable",
    )
    .await;
    assert_eq!(f.policy_calls.load(Ordering::SeqCst), 0);
    f.store.close().await;
}
