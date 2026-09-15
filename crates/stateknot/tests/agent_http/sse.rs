// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use tower_service::Service as _;

fn options() -> AgentHttpSseOptions {
    AgentHttpSseOptions::new(
        1,
        Duration::from_secs(5),
        Duration::from_millis(50),
        Duration::from_millis(100),
    )
    .unwrap()
}
async fn streaming_server(f: &Fixture) -> Server {
    streaming_server_with(f, options()).await
}
async fn streaming_server_with(f: &Fixture, streams: AgentHttpSseOptions) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let options = AgentHttpOptions::loopback(address.port())
        .unwrap()
        .with_limits(262_144, 2_097_152, 1)
        .unwrap()
        .with_deadline(Duration::from_secs(1))
        .unwrap()
        .with_sse(streams);
    let http = AgentHttpService::new(f.service.clone(), f.auth.clone(), options);
    let router = http.router();
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Server {
        url: format!("http://{address}"),
        http,
        task,
    }
}
fn get(server: &Server, run: RunId) -> reqwest::RequestBuilder {
    client()
        .get(format!("{}/v1/agent-runs/{run}/events", server.url))
        .bearer_auth(TOKEN)
        .header("accept", "text/event-stream")
}
struct Events {
    response: reqwest::Response,
    buffer: String,
}
impl Events {
    async fn open(request: reqwest::RequestBuilder) -> Self {
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        assert_eq!(
            response.headers()["cache-control"],
            "private, no-store, no-transform"
        );
        assert_eq!(response.headers()["x-accel-buffering"], "no");
        Self {
            response,
            buffer: String::new(),
        }
    }
    async fn next(&mut self, kind: &str) -> (Option<String>, Value) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(end) = self.buffer.find("\n\n") {
                    let frame = self.buffer.drain(..end + 2).collect::<String>();
                    assert!(!frame.contains("never-log"));
                    if !frame.starts_with(&format!("event: {kind}\n")) {
                        continue;
                    }
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
                let chunk = self
                    .response
                    .chunk()
                    .await
                    .unwrap()
                    .expect("stream ended before expected event");
                self.buffer.push_str(std::str::from_utf8(&chunk).unwrap());
                assert!(self.buffer.len() <= 8_388_608);
            }
        })
        .await
        .unwrap()
    }
}
async fn admit(f: &Fixture) -> RunId {
    let input = f.submission();
    f.service
        .submit(
            f.caller.clone(),
            &input.submission_key,
            &input.agent,
            input.request,
        )
        .await
        .unwrap()
        .snapshot()
        .provenance()
        .run_id()
}
fn encoded(head: &JournalHead) -> String {
    format!(
        "sk1.{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(head).unwrap())
    )
}

#[tokio::test]
async fn postgres_sse_replays_across_process_restart_and_live_updates() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let run = admit(&f).await;
    let s = streaming_server(&f).await;
    let mut events = Events::open(get(&s, run)).await;
    let (id, view) = events.next("snapshot").await;
    assert!(id.is_none());
    assert_eq!(view["snapshot"]["status"], "active");
    let (first, activity) = events.next("activity").await;
    assert_eq!(activity.as_object().unwrap().len(), 2);
    assert_eq!(activity["sequence"], "1");
    let first = first.unwrap();
    // JSON capacity remains available while the separate SSE capacity is held.
    error(get(&s, run).send().await.unwrap(), 429, "overloaded").await;
    let cancellation = AgentCancellationIds::generate();
    snapshot(
        post(
            &s,
            &format!("/v1/agent-runs/{run}/cancellation"),
            &cancellation,
        )
        .send()
        .await
        .unwrap(),
        202,
    )
    .await;
    let (_, changed) = events.next("snapshot").await;
    assert_eq!(changed["snapshot"]["status"], "cancellation_requested");
    let (second, activity) = events.next("activity").await;
    assert_eq!(activity["sequence"], "2");
    let second = second.unwrap();
    drop(events);
    drop(s);
    // A new OS process reconstructs the pool, registries, policy and ingress.
    let output = tokio::time::timeout(
        Duration::from_secs(45),
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "sse::postgres_sse_restart_child",
                "--nocapture",
            ])
            .env("STATEKNOT_SSE_CHILD_TENANT", f.caller.tenant_id().as_str())
            .env("STATEKNOT_SSE_CHILD_RUN", run.to_string())
            .env("STATEKNOT_SSE_CHILD_CURSOR", &first)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains(&format!("STATEKNOT_SSE_CHILD_CURSOR={second}"))
    );
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 0);
    println!(
        "\nSTATEKNOT_AGENT_SSE_EVIDENCE=exact_process_restart_suffix;live_cancellation;separate_capacity;no_inline_dispatch"
    );
}

#[tokio::test]
#[ignore = "subprocess-only SSE restart fixture"]
async fn postgres_sse_restart_child() {
    let f = Fixture::for_tenant(
        std::env::var("STATEKNOT_SSE_CHILD_TENANT")
            .unwrap()
            .parse()
            .unwrap(),
    )
    .await
    .unwrap();
    let run = std::env::var("STATEKNOT_SSE_CHILD_RUN")
        .unwrap()
        .parse()
        .unwrap();
    let s = streaming_server(&f).await;
    let mut events = Events::open(get(&s, run).header(
        "last-event-id",
        std::env::var("STATEKNOT_SSE_CHILD_CURSOR").unwrap(),
    ))
    .await;
    assert!(events.next("snapshot").await.0.is_none());
    let (id, activity) = events.next("activity").await;
    assert_eq!(activity["sequence"], "2");
    println!("\nSTATEKNOT_SSE_CHILD_CURSOR={}", id.unwrap());
}

#[tokio::test]
async fn postgres_sse_rejects_hostile_cursors_and_rechecks_authority() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let run = admit(&f).await;
    let s = streaming_server(&f).await;
    let head = f
        .store
        .load_journal_page(
            f.caller.tenant_id(),
            run,
            None,
            JournalPageSize::new(1).unwrap(),
        )
        .await
        .unwrap()
        .next_cursor()
        .unwrap();
    for invalid in ["bad".to_owned(), "x".repeat(1025)] {
        error(
            get(&s, run)
                .header("last-event-id", invalid)
                .send()
                .await
                .unwrap(),
            400,
            "invalid_request",
        )
        .await;
    }
    error(
        get(&s, run)
            .header("last-event-id", "bad")
            .header("last-event-id", "bad")
            .send()
            .await
            .unwrap(),
        400,
        "invalid_request",
    )
    .await;
    error(
        replace_header(get(&s, run), "accept", "application/json")
            .send()
            .await
            .unwrap(),
        406,
        "not_acceptable",
    )
    .await;
    error(
        get(&s, run).body("x").send().await.unwrap(),
        400,
        "invalid_request",
    )
    .await;
    for altered in [
        JournalHead::new(
            head.tenant_id().clone(),
            RunId::generate(),
            head.sequence(),
            head.event_id(),
            head.recorded_at(),
            head.digest(),
        ),
        JournalHead::new(
            head.tenant_id().clone(),
            run,
            JournalSequence::new(2).unwrap(),
            head.event_id(),
            head.recorded_at(),
            head.digest(),
        ),
        JournalHead::new(
            head.tenant_id().clone(),
            run,
            head.sequence(),
            head.event_id(),
            head.recorded_at(),
            Digest::sha256(b"tampered"),
        ),
        JournalHead::new(
            TenantId::new("other").unwrap(),
            run,
            head.sequence(),
            head.event_id(),
            head.recorded_at(),
            head.digest(),
        ),
    ] {
        error(
            get(&s, run)
                .header("last-event-id", encoded(&altered))
                .send()
                .await
                .unwrap(),
            409,
            "invalid_cursor",
        )
        .await;
    }
    error(
        replace_header(
            get(&s, RunId::generate()),
            "authorization",
            "Bearer other-tenant-fixture",
        )
        .send()
        .await
        .unwrap(),
        403,
        "denied",
    )
    .await;
    let mut events = Events::open(get(&s, run).header("last-event-id", encoded(&head))).await;
    events.next("snapshot").await;
    f.auth.revoked.store(true, Ordering::SeqCst);
    let (id, failure) = events.next("error").await;
    assert!(id.is_none());
    assert_eq!(failure["error"]["code"], "agent_http.unauthenticated");
    drop(events);
    f.auth.revoked.store(false, Ordering::SeqCst);
    let mut events = Events::open(get(&s, run)).await;
    events.next("snapshot").await;
    f.denied.store(true, Ordering::SeqCst);
    assert_eq!(
        events.next("error").await.1["error"]["code"],
        "agent_http.denied"
    );
    drop(events);
    f.store.close().await;
    error(
        get(&s, run)
            .header("last-event-id", encoded(&head))
            .send()
            .await
            .unwrap(),
        403,
        "denied",
    )
    .await;
    f.denied.store(false, Ordering::SeqCst);
    error(get(&s, run).send().await.unwrap(), 503, "unavailable").await;
}

#[tokio::test]
async fn postgres_sse_unpolled_body_releases_permit_and_shutdown_is_bounded() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let run = admit(&f).await;
    let request = || {
        Request::builder()
            .uri(format!("/v1/agent-runs/{run}/events"))
            .header("host", "localhost:1234")
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("accept", "text/event-stream")
            .body(axum::body::Body::empty())
            .unwrap()
    };
    let disabled = AgentHttpService::new(
        f.service.clone(),
        f.auth.clone(),
        AgentHttpOptions::loopback(1234).unwrap(),
    );
    assert_eq!(
        disabled.router().call(request()).await.unwrap().status(),
        404
    );
    let tiny = AgentHttpService::new(
        f.service.clone(),
        f.auth.clone(),
        AgentHttpOptions::loopback(1234)
            .unwrap()
            .with_limits(262_144, 1024, 1)
            .unwrap()
            .with_sse(options()),
    );
    for _ in 0..2 {
        let rejected = tiny.router().call(request()).await.unwrap();
        assert_eq!(rejected.status(), 500); // Never commit SSE 200 with an oversized first batch.
        let value: Value = serde_json::from_slice(
            &axum::body::to_bytes(rejected.into_body(), 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(value["error"]["code"], "agent_http.response_too_large");
    }
    let config = AgentHttpOptions::loopback(1234)
        .unwrap()
        .with_deadline(Duration::from_secs(1))
        .unwrap()
        .with_sse(options());
    let http = AgentHttpService::new(f.service.clone(), f.auth.clone(), config);
    let mut router = http.router();
    let unpolled = router.call(request()).await.unwrap();
    assert_eq!(unpolled.status(), 200);
    assert_eq!(router.call(request()).await.unwrap().status(), 429);
    // Keep the body alive and NEVER poll it: the queue itself must time out.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let next = router.call(request()).await.unwrap();
    assert_eq!(next.status(), 200);
    drop(next);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let active = router.call(request()).await.unwrap();
    assert_eq!(active.status(), 200);
    http.shutdown();
    let bytes = tokio::time::timeout(
        Duration::from_secs(1),
        axum::body::to_bytes(active.into_body(), 4_194_304),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!bytes.is_empty());
    assert_eq!(router.call(request()).await.unwrap().status(), 503);
    drop(unpolled);
}

#[tokio::test]
async fn postgres_sse_observes_quarantine_without_revision_change_and_expires() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let run = admit(&f).await;
    let s = streaming_server_with(
        &f,
        AgentHttpSseOptions::new(
            1,
            Duration::from_secs(1),
            Duration::from_millis(50),
            Duration::from_millis(100),
        )
        .unwrap(),
    )
    .await;
    let mut events = Events::open(get(&s, run)).await;
    let (_, first) = events.next("snapshot").await;
    let (id, _) = events.next("activity").await;
    let head: JournalHead = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(id.unwrap().strip_prefix("sk1.").unwrap())
            .unwrap(),
    )
    .unwrap();
    f.store
        .quarantine_run(
            RunQuarantineRequest::new(
                f.caller.tenant_id().clone(),
                run,
                QuarantineId::generate(),
                JournalExpectation::exact(head),
                RunQuarantineCause::ProjectionMismatch,
                RunQuarantineComponent::new("fixture.quarantine").unwrap(),
                Digest::sha256(b"never-log-private-evidence"),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let (id, changed) = events.next("snapshot").await;
    assert!(id.is_none());
    assert_eq!(
        changed["snapshot"]["revision"],
        first["snapshot"]["revision"]
    );
    assert_eq!(changed["snapshot"]["quarantined"], true);
    // A full queue may suppress the best-effort error; bounded EOF is mandatory.
    tokio::time::timeout(Duration::from_secs(3), events.response.bytes())
        .await
        .unwrap()
        .unwrap();
    let mut fresh = Events::open(get(&s, run)).await;
    fresh.next("snapshot").await;
    f.auth.block.store(true, Ordering::SeqCst);
    // A blocked verifier cannot extend the connection's absolute lifetime.
    tokio::time::timeout(Duration::from_secs(3), f.auth.entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), fresh.response.bytes())
        .await
        .unwrap()
        .unwrap();
}
