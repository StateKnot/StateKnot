// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot::agent_host::{AgentHost, AgentHostStatus, operations::*};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

struct Auth {
    caller: AgentServiceCaller,
    mode: AtomicUsize,
    active: Arc<AtomicUsize>,
    maximum: AtomicUsize,
    release: Notify,
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl AgentHttpAuthenticator for Auth {
    fn authenticate(
        &self,
        token: AgentHttpCredential,
    ) -> BoxFuture<'_, Result<AgentHttpPrincipal, AgentHttpAuthenticationError>> {
        assert_ne!(
            self.mode.load(Ordering::SeqCst),
            4,
            "fixture synchronous auth panic"
        );
        Box::pin(async move {
            let n = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            let _guard = Active(self.active.clone());
            self.maximum.fetch_max(n, Ordering::SeqCst);
            match self.mode.load(Ordering::SeqCst) {
                1 => self.release.notified().await,
                2 => return Err(AgentHttpAuthenticationError::Unavailable),
                3 => panic!("fixture future auth panic"),
                _ => (),
            }
            let (caller, operations) = match token.expose_secret() {
                "ops-fixture" => (self.caller.clone(), vec![AgentHttpOperation::InspectHost]),
                "business-fixture" => (self.caller.clone(), vec![AgentHttpOperation::Read]),
                "cross-tenant-fixture" => (
                    AgentServiceCaller::new(
                        "other-tenant".parse().unwrap(),
                        self.caller.principal().clone(),
                    ),
                    vec![AgentHttpOperation::InspectHost],
                ),
                _ => return Err(AgentHttpAuthenticationError::Unauthenticated),
            };
            Ok(AgentHttpPrincipal::new(caller, operations))
        })
    }
}
fn auth(f: &Fixture) -> Arc<Auth> {
    Arc::new(Auth {
        caller: f.caller.clone(),
        mode: AtomicUsize::new(0),
        active: Arc::new(AtomicUsize::new(0)),
        maximum: AtomicUsize::new(0),
        release: Notify::new(),
    })
}
async fn start(
    host: &AgentHost,
    auth: Arc<Auth>,
    limits: impl FnOnce(AgentHostOperationsOptions) -> AgentHostOperationsOptions,
) -> (AgentHostOperations, Arc<AgentHostOperationsPolicy>) {
    let policy = Arc::new(
        AgentHostOperationsPolicy::new(
            host.health(),
            vec![auth.caller.clone()],
            Duration::from_secs(300),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let options = limits(
        AgentHostOperationsOptions::new([listener.local_addr().unwrap().to_string()]).unwrap(),
    );
    (
        AgentHostOperations::start(listener, policy.clone(), auth, options).unwrap(),
        policy,
    )
}
fn url(ops: &AgentHostOperations, path: &str) -> String {
    format!("http://{}{path}", ops.local_addr())
}
async fn get(ops: &AgentHostOperations, path: &str, token: &str) -> reqwest::Response {
    host::client()
        .get(url(ops, path))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
}
async fn body(response: reqwest::Response, expected: u16) -> Value {
    assert_eq!(response.status(), expected);
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    assert_eq!(response.headers()["stateknot-api-version"], "1");
    let id = response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let bytes = response.bytes().await.unwrap();
    assert!(bytes.len() < 16 * 1024);
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["request_id"], id);
    result
}

#[tokio::test]
async fn postgres_operations_authorization_privacy_and_revocation() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (mut host, _) = host::launch(&f, &host::checks(), evidence(&f), host::limits()).await;
    host::ready(&host).await;
    let verifier = auth(&f);
    let (mut ops, policy) = start(&host, verifier.clone(), |o| o).await;
    for (token, expected) in [
        ("invalid", 401),
        ("business-fixture", 403),
        ("cross-tenant-fixture", 403),
    ] {
        let result = get(&ops, "/v1/host/status", token).await;
        assert_eq!(result.status(), expected);
        assert!(result.json::<Value>().await.unwrap().get("host").is_none());
    }
    let run = admit(&f).await;
    succeeded(&f, run).await;
    until(|| host.health().worker().unwrap().report().executed_quanta > 0).await;
    let value = body(get(&ops, "/v1/host/status", "ops-fixture").await, 200).await;
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["host"]["status"], "ready");
    assert!(
        value["host"]["worker"]["executed_quanta"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 0
    );
    assert_eq!(
        value["host"]["maintenance"]["jobs"]
            .as_object()
            .unwrap()
            .len(),
        4
    );
    for secret in [
        f.caller.tenant_id().as_str(),
        "ops-fixture",
        &run.to_string(),
        "fixture-subject",
    ] {
        assert!(!value.to_string().contains(secret));
    }
    policy.replace(1, vec![], Duration::from_secs(300)).unwrap();
    assert_eq!(
        get(&ops, "/v1/host/status", "ops-fixture").await.status(),
        403
    );
    policy
        .replace(2, vec![f.caller.clone()], Duration::from_millis(1))
        .unwrap();
    sleep(Duration::from_millis(5)).await;
    assert_eq!(
        get(&ops, "/v1/host/status", "ops-fixture").await.status(),
        503
    );
    policy
        .replace(3, vec![f.caller.clone()], Duration::from_secs(300))
        .unwrap();
    assert_eq!(
        get(&ops, "/v1/host/status", "ops-fixture").await.status(),
        200
    );
    // Revocation while verification is in flight cannot use an old ACL snapshot.
    verifier.mode.store(1, Ordering::SeqCst);
    let request = host::client()
        .get(url(&ops, "/v1/host/status"))
        .bearer_auth("ops-fixture")
        .send();
    let task = tokio::spawn(request);
    until(|| verifier.active.load(Ordering::SeqCst) == 1).await;
    policy.replace(4, vec![], Duration::from_secs(300)).unwrap();
    verifier.release.notify_one();
    assert_eq!(task.await.unwrap().unwrap().status(), 403);
    assert_eq!(ops.shutdown().await.unwrap().forced_connections, 0);
    assert_eq!(ops.active_connections(), 0);
    assert_eq!(host.health().status(), AgentHostStatus::Ready);
    succeeded(&f, admit(&f).await).await;
    host.shutdown().await.unwrap();
    host::joined(&host, &f).await;
    f.store.close().await;
    println!(
        "\nSTATEKNOT_OPERATIONS_AUTHORIZATION_EVIDENCE={{\"separate_permission\":true,\"exact_tenant\":true,\"redacted_counters\":true,\"expiry_recovery\":true,\"in_flight_revocation\":true,\"independent_shutdown\":true}}"
    );
}

#[tokio::test]
async fn postgres_operations_observes_starting_unavailable_draining_stopped() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let checks = host::checks();
    checks[0].mode.store(2, Ordering::SeqCst);
    let (mut host, _) = host::launch(&f, &checks, evidence(&f), host::limits()).await;
    until(|| checks[0].calls.load(Ordering::SeqCst) > 0).await;
    let (mut ops, _) = start(&host, auth(&f), |o| o).await;
    let value = body(get(&ops, "/v1/host/ready", "ops-fixture").await, 503).await;
    assert_eq!(value["host"]["status"], "starting");
    assert_eq!(
        get(&ops, "/v1/host/live", "ops-fixture").await.status(),
        200
    );
    host.shutdown().await.unwrap();
    let calls: Vec<_> = checks
        .iter()
        .map(|h| h.calls.load(Ordering::SeqCst))
        .collect();
    assert_eq!(
        body(get(&ops, "/v1/host/status", "ops-fixture").await, 200).await["host"]["status"],
        "stopped"
    );
    assert_eq!(
        get(&ops, "/v1/host/live", "ops-fixture").await.status(),
        503
    );
    assert_eq!(
        get(&ops, "/v1/host/ready", "ops-fixture").await.status(),
        503
    );
    assert_eq!(
        calls,
        checks
            .iter()
            .map(|h| h.calls.load(Ordering::SeqCst))
            .collect::<Vec<_>>(),
        "reads cannot invoke host probes"
    );
    ops.shutdown().await.unwrap();
    let checks = host::checks();
    let (mut host, _) = host::launch(&f, &checks, evidence(&f), host::limits()).await;
    host::ready(&host).await;
    let (mut ops, _) = start(&host, auth(&f), |o| o).await;
    checks[2].mode.store(1, Ordering::SeqCst);
    until(|| host.health().status() == AgentHostStatus::Unavailable).await;
    assert_eq!(host::submit(&host, &f).await.status(), 503);
    assert_eq!(
        body(get(&ops, "/v1/host/ready", "ops-fixture").await, 503).await["host"]["status"],
        "unavailable"
    );
    assert_eq!(
        get(&ops, "/v1/host/live", "ops-fixture").await.status(),
        200
    );
    checks[2].mode.store(0, Ordering::SeqCst);
    host::ready(&host).await;
    host.begin_shutdown();
    // begin_shutdown synchronously closes host ingress; eventual Stopped is also
    // valid by the time a separate network request arrives.
    let state = body(get(&ops, "/v1/host/ready", "ops-fixture").await, 503).await;
    assert!(matches!(
        state["host"]["status"].as_str(),
        Some("draining" | "stopped")
    ));
    host.wait().await.unwrap();
    host::joined(&host, &f).await;
    let stopped = body(get(&ops, "/v1/host/status", "ops-fixture").await, 200).await;
    assert_eq!(stopped["host"]["worker"]["active_ticks"], 0);
    assert_eq!(stopped["host"]["http"]["active_connections"], 0);
    ops.shutdown().await.unwrap();
    f.store.close().await;
    println!(
        "\nSTATEKNOT_OPERATIONS_LIFECYCLE_EVIDENCE={{\"starting\":true,\"business_outage\":true,\"stopped_inspection\":true,\"no_read_probes\":true,\"zero_activity\":true}}"
    );
}

#[tokio::test]
async fn postgres_operations_hostile_inputs_and_loopback_only() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (mut host, _) = host::launch(&f, &host::checks(), evidence(&f), host::limits()).await;
    host::ready(&host).await;
    let verifier = auth(&f);
    let (mut ops, policy) = start(&host, verifier.clone(), |o| o).await;
    assert!(matches!(
        AgentHostOperations::start(
            TcpListener::bind("0.0.0.0:0").await.unwrap(),
            policy,
            verifier,
            AgentHostOperationsOptions::new(["ops.example.test".into()]).unwrap()
        ),
        Err(AgentHostOperationsError::InvalidListener)
    ));
    for (path, expected) in [
        ("/unknown", 404),
        ("/v1/host/%73tatus", 400),
        ("/v1/host/status?tenant=anything", 400),
        ("/v1/host/status/", 404),
    ] {
        assert_eq!(get(&ops, path, "ops-fixture").await.status(), expected);
        assert_eq!(get(&ops, path, "invalid").await.status(), 401);
    }
    for method in [
        reqwest::Method::POST,
        reqwest::Method::HEAD,
        reqwest::Method::DELETE,
        reqwest::Method::OPTIONS,
    ] {
        let result = host::client()
            .request(method, url(&ops, "/v1/host/status"))
            .bearer_auth("ops-fixture")
            .send()
            .await
            .unwrap();
        assert_eq!(result.status(), 405);
        assert_eq!(result.headers()["allow"], "GET");
    }
    for (key, value, expected) in [
        ("origin", "https://ops.example.test", 403),
        ("host", "evil.example.test", 403),
        ("accept", "text/event-stream", 406),
        ("content-encoding", "gzip", 415),
    ] {
        assert_eq!(
            host::client()
                .get(url(&ops, "/v1/host/status"))
                .bearer_auth("ops-fixture")
                .header(key, value)
                .send()
                .await
                .unwrap()
                .status(),
            expected
        );
    }
    assert_eq!(
        host::client()
            .get(url(&ops, "/v1/host/status"))
            .bearer_auth("ops-fixture")
            .body("x")
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        host::client()
            .get(url(&ops, "/v1/host/status"))
            .header("cookie", "Authorization=Bearer ops-fixture")
            .header("x-forwarded-user", "operator")
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    for extra in [
        "Authorization: Bearer ops-fixture\r\nAuthorization: Bearer ops-fixture\r\n",
        "Authorization: Bearer ops-fixture\r\nAccept: application/json\r\nAccept: */*\r\n",
        "Authorization: Bearer ops-fixture\r\nTransfer-Encoding: chunked\r\n",
    ] {
        let mut socket = TcpStream::connect(ops.local_addr()).await.unwrap();
        socket.write_all(format!("GET /v1/host/status HTTP/1.1\r\nHost: {}\r\n{extra}Connection: close\r\n\r\n0\r\n\r\n",ops.local_addr()).as_bytes()).await.unwrap();
        let mut bytes = Vec::new();
        timeout(Duration::from_secs(3), socket.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).starts_with("HTTP/1.1 400"));
    }
    ops.shutdown().await.unwrap();
    host.shutdown().await.unwrap();
    f.store.close().await;
    println!(
        "\nSTATEKNOT_OPERATIONS_INPUT_EVIDENCE={{\"loopback_only\":true,\"authentication_first\":true,\"exact_host\":true,\"no_origin\":true,\"no_body\":true,\"duplicate_headers\":true,\"read_only\":true}}"
    );
}

#[tokio::test]
async fn postgres_operations_auth_deadline_concurrency_panics_and_join() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (mut host, _) = host::launch(&f, &host::checks(), evidence(&f), host::limits()).await;
    host::ready(&host).await;
    let verifier = auth(&f);
    let (mut ops, _) = start(&host, verifier.clone(), |o| {
        o.with_request_limits(2, Duration::from_millis(300))
            .unwrap()
            .with_transport_limits(
                32,
                Duration::from_secs(1),
                Duration::from_secs(60),
                Duration::from_millis(30),
            )
            .unwrap()
    })
    .await;
    verifier.mode.store(1, Ordering::SeqCst);
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..2 {
        tasks.spawn(
            host::client()
                .get(url(&ops, "/v1/host/status"))
                .bearer_auth("ops-fixture")
                .send(),
        );
    }
    until(|| verifier.active.load(Ordering::SeqCst) == 2).await;
    assert_eq!(
        get(&ops, "/v1/host/status", "ops-fixture").await.status(),
        429
    );
    while let Some(result) = tasks.join_next().await {
        assert_eq!(result.unwrap().unwrap().status(), 503);
    }
    assert_eq!(verifier.maximum.load(Ordering::SeqCst), 2);
    assert_eq!(verifier.active.load(Ordering::SeqCst), 0);
    for mode in [2, 3, 4] {
        verifier.mode.store(mode, Ordering::SeqCst);
        let result = get(&ops, "/v1/host/status", "ops-fixture").await;
        assert_eq!(result.status(), 503);
        assert!(!result.text().await.unwrap().contains("fixture"));
    }
    verifier.mode.store(0, Ordering::SeqCst);
    assert_eq!(
        get(&ops, "/v1/host/status", "ops-fixture").await.status(),
        200
    );
    assert!(
        timeout(Duration::from_millis(10), ops.wait())
            .await
            .is_err()
    );
    verifier.mode.store(1, Ordering::SeqCst);
    let request = tokio::spawn(
        host::client()
            .get(url(&ops, "/v1/host/status"))
            .bearer_auth("ops-fixture")
            .send(),
    );
    until(|| verifier.active.load(Ordering::SeqCst) == 1).await;
    assert!(
        timeout(Duration::from_millis(5), ops.shutdown())
            .await
            .is_err()
    );
    let report = ops.wait().await.unwrap();
    assert_eq!(report.forced_connections, 1);
    assert_eq!(ops.active_connections(), 0);
    assert_eq!(verifier.active.load(Ordering::SeqCst), 0);
    assert!(request.await.unwrap().is_err());
    assert_eq!(ops.wait().await, Err(AgentHostOperationsError::Stopped));
    host.shutdown().await.unwrap();
    f.store.close().await;
    println!(
        "\nSTATEKNOT_OPERATIONS_BOUNDS_EVIDENCE={{\"request_ceiling\":2,\"deadline\":true,\"sync_async_panics\":true,\"cancelled_wait_retains_owner\":true,\"forced_join\":true,\"zero_auth_activity\":true}}"
    );
}

#[tokio::test]
async fn postgres_operations_transport_limits_and_drop_cleanup() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (mut host, _) = host::launch(&f, &host::checks(), evidence(&f), host::limits()).await;
    host::ready(&host).await;
    let verifier = auth(&f);
    let (mut ops, _) = start(&host, verifier.clone(), |o| {
        o.with_transport_limits(
            1,
            Duration::from_millis(150),
            Duration::from_millis(500),
            Duration::from_millis(30),
        )
        .unwrap()
    })
    .await;
    let mut first = TcpStream::connect(ops.local_addr()).await.unwrap();
    first
        .write_all(b"GET /v1/host/status HTTP/1.1\r\n")
        .await
        .unwrap();
    until(|| ops.active_connections() == 1).await;
    let mut second = TcpStream::connect(ops.local_addr()).await.unwrap();
    let mut bytes = Vec::new();
    assert_eq!(
        timeout(Duration::from_secs(2), second.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    bytes.clear();
    timeout(Duration::from_secs(2), first.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    until(|| ops.active_connections() == 0).await;
    verifier.mode.store(1, Ordering::SeqCst);
    let request = tokio::spawn(
        host::client()
            .get(url(&ops, "/v1/host/status"))
            .bearer_auth("ops-fixture")
            .send(),
    );
    until(|| verifier.active.load(Ordering::SeqCst) == 1).await;
    assert!(
        request.await.unwrap().is_err(),
        "absolute lifetime bounds active authentication"
    );
    until(|| verifier.active.load(Ordering::SeqCst) == 0).await;
    let report = ops.shutdown().await.unwrap();
    assert!(report.rejected_connections >= 1);
    assert!(report.connection_failures >= 2);
    let (ops, _) = start(&host, verifier.clone(), |o| o).await;
    let address = ops.local_addr();
    let request = tokio::spawn(
        host::client()
            .get(url(&ops, "/v1/host/status"))
            .bearer_auth("ops-fixture")
            .send(),
    );
    until(|| verifier.active.load(Ordering::SeqCst) == 1).await;
    drop(ops);
    assert!(request.await.unwrap().is_err());
    until(|| verifier.active.load(Ordering::SeqCst) == 0).await;
    assert!(TcpStream::connect(address).await.is_err());
    // Current-thread runtime: no yield between start and Drop exercises an
    // unpolled coordinator, not merely an already-running shutdown.
    let (ops, _) = start(&host, verifier, |o| o).await;
    let address = ops.local_addr();
    drop(ops);
    tokio::task::yield_now().await;
    assert!(TcpStream::connect(address).await.is_err());
    assert_eq!(host.health().status(), AgentHostStatus::Ready);
    host.shutdown().await.unwrap();
    f.store.close().await;
    println!(
        "\nSTATEKNOT_OPERATIONS_TRANSPORT_EVIDENCE={{\"connection_ceiling\":true,\"header_deadline\":true,\"absolute_lifetime\":true,\"running_drop\":true,\"unpolled_drop\":true}}"
    );
}
