// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    sync::Semaphore,
    time::{Instant, timeout},
};

struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
struct Host {
    available: AtomicBool,
    block: AtomicBool,
    active: AtomicUsize,
    maximum: AtomicUsize,
    calls: AtomicUsize,
}
impl Host {
    fn ready() -> Arc<Self> {
        Arc::new(Self {
            available: AtomicBool::new(true),
            block: AtomicBool::new(false),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
        })
    }
}
impl AgentHttpReadiness for Host {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentHttpReadinessError>> {
        Box::pin(async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            let _active = Active(&self.active);
            self.maximum.fetch_max(active, Ordering::SeqCst);
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.block.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if self.available.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(AgentHttpReadinessError)
            }
        })
    }
}

fn options() -> AgentHttpServerOptions {
    AgentHttpServerOptions::default()
        .with_transport_limits(
            8,
            Duration::from_secs(1),
            Duration::from_secs(10),
            Duration::from_millis(150),
        )
        .unwrap()
        .with_readiness_limits(
            Duration::from_millis(50),
            Duration::from_secs(3),
            Duration::from_secs(10),
        )
        .unwrap()
}

async fn configured(
    f: &Fixture,
    auth: Arc<dyn AgentHttpAuthenticator>,
) -> (TcpListener, AgentHttpService) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = AgentHttpService::new(
        f.service.clone(),
        auth,
        AgentHttpOptions::loopback(listener.local_addr().unwrap().port())
            .unwrap()
            .with_sse(AgentHttpSseOptions::default()),
    );
    (listener, http)
}

async fn start(f: &Fixture, host: Arc<Host>, options: AgentHttpServerOptions) -> AgentHttpServer {
    let (listener, http) = configured(f, f.auth.clone()).await;
    AgentHttpServer::start(listener, http, host, options)
        .await
        .unwrap()
}

async fn until(mut condition: impl FnMut() -> bool) {
    timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

async fn closed(socket: &mut TcpStream) {
    let mut bytes = Vec::new();
    // EOF or reset both prove closure; a timeout never does.
    let _ = timeout(Duration::from_secs(3), socket.read_to_end(&mut bytes))
        .await
        .unwrap();
    assert!(bytes.len() < 4096);
}

#[tokio::test]
async fn postgres_owned_startup_dependencies_and_exclusive_ownership() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.service.check_readiness().await.unwrap();
    let empty = AgentServiceV1::new(
        f.store.clone(),
        f.executable.clone(),
        AgentServiceRegistryBuilder::new().build(),
        f.policy.clone(),
    )
    .unwrap();
    assert_eq!(
        empty.check_readiness().await,
        Err(AgentServiceReadinessError::DeploymentUnavailable)
    );
    let other = Fixture::for_graph(
        TenantId::new(format!("other-{}", RunId::generate())).unwrap(),
        "different-graph",
    )
    .await
    .unwrap();
    let missing = AgentServiceV1::new(
        f.store.clone(),
        other.executable,
        f.deployments.clone(),
        f.policy.clone(),
    )
    .unwrap();
    assert_eq!(
        missing.check_readiness().await,
        Err(AgentServiceReadinessError::DeploymentUnavailable)
    );
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.policy_calls.load(Ordering::SeqCst), 0);

    let (listener, http) = configured(&f, f.auth.clone()).await;
    let address = listener.local_addr().unwrap();
    let host = Host::ready();
    host.available.store(false, Ordering::SeqCst);
    assert!(matches!(
        AgentHttpServer::start(listener, http.clone(), host, options()).await,
        Err(AgentHttpServerError::Unavailable)
    ));
    assert!(TcpStream::connect(address).await.is_err());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    assert!(matches!(
        AgentHttpServer::start(listener, http, Host::ready(), options()).await,
        Err(AgentHttpServerError::AlreadyClaimed)
    ));

    let (listener, http) = configured(&f, f.auth.clone()).await;
    drop(listener);
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    assert!(matches!(
        AgentHttpServer::start(listener, http, Host::ready(), options()).await,
        Err(AgentHttpServerError::InvalidListener)
    ));

    let (listener, http) = configured(&f, f.auth.clone()).await;
    let mut server = AgentHttpServer::start(listener, http.clone(), Host::ready(), options())
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    assert!(matches!(
        AgentHttpServer::start(listener, http, Host::ready(), options()).await,
        Err(AgentHttpServerError::AlreadyClaimed)
    ));
    assert!(server.health().is_ready());
    server.shutdown().await.unwrap();
    assert_eq!(server.wait().await, Err(AgentHttpServerError::Stopped));
    assert!(!server.health().is_live());

    f.store.close().await;
    assert_eq!(
        f.service.check_readiness().await,
        Err(AgentServiceReadinessError::StorageUnavailable)
    );
    let (listener, http) = configured(&f, f.auth.clone()).await;
    assert!(matches!(
        AgentHttpServer::start(listener, http, Host::ready(), options()).await,
        Err(AgentHttpServerError::Unavailable)
    ));
}

#[tokio::test]
async fn postgres_owned_readiness_loss_recovery_timeout_and_no_inline_work() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let host = Host::ready();
    let mut server = start(&f, host.clone(), options()).await;
    let health = server.health();
    let url = format!("http://{}/v1/agent-runs", server.local_addr());
    let request = f.submission();
    host.available.store(false, Ordering::SeqCst);
    until(|| !health.is_ready()).await;
    error(
        client()
            .post(&url)
            .bearer_auth(TOKEN)
            .json(&request)
            .send()
            .await
            .unwrap(),
        503,
        "unavailable",
    )
    .await;
    assert_eq!(f.policy_calls.load(Ordering::SeqCst), 0);
    host.available.store(true, Ordering::SeqCst);
    until(|| health.is_ready()).await;
    let admitted = snapshot(
        client()
            .post(&url)
            .bearer_auth(TOKEN)
            .json(&request)
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 0);
    host.block.store(true, Ordering::SeqCst);
    until(|| host.active.load(Ordering::SeqCst) == 1).await;
    until(|| !health.is_ready()).await;
    assert_eq!(host.maximum.load(Ordering::SeqCst), 1);
    let count = host.calls.load(Ordering::SeqCst);
    for _ in 0..100 {
        assert!(health.is_live());
        assert!(!health.is_ready());
    }
    assert_eq!(host.calls.load(Ordering::SeqCst), count);
    host.block.store(false, Ordering::SeqCst);
    until(|| health.is_ready()).await;
    let recovered = snapshot(
        client()
            .post(&url)
            .bearer_auth(TOKEN)
            .json(&request)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(
        admitted.provenance().run_id(),
        recovered.provenance().run_id()
    );
    f.store.close().await;
    until(|| !health.is_ready()).await;
    error(
        client()
            .post(&url)
            .bearer_auth(TOKEN)
            .json(&request)
            .send()
            .await
            .unwrap(),
        503,
        "unavailable",
    )
    .await;
    let report = server.shutdown().await.unwrap();
    assert_eq!(report.forced_connections, 0);
    assert_eq!(health.active_connections(), 0);
    assert_eq!(host.active.load(Ordering::SeqCst), 0);
    println!(
        "\nSTATEKNOT_AGENT_SERVER_EVIDENCE=real-store-readiness;host-recovery;single-flight;fail-closed-admission;idempotent-recovery;no-inline-executor"
    );
}

struct GateAuth {
    inner: Arc<Auth>,
    entered: Notify,
    release: Semaphore,
}
impl AgentHttpAuthenticator for GateAuth {
    fn authenticate(
        &self,
        credential: AgentHttpCredential,
    ) -> BoxFuture<'_, Result<AgentHttpPrincipal, AgentHttpAuthenticationError>> {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
            self.inner.authenticate(credential).await
        })
    }
}

#[tokio::test]
async fn postgres_owned_graceful_commit_and_cancellation_safe_join() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let auth = Arc::new(GateAuth {
        inner: f.auth.clone(),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (listener, http) = configured(&f, auth.clone()).await;
    let settings = options()
        .with_transport_limits(
            8,
            Duration::from_secs(1),
            Duration::from_secs(10),
            Duration::from_secs(3),
        )
        .unwrap();
    let mut server = AgentHttpServer::start(listener, http, Host::ready(), settings)
        .await
        .unwrap();
    let address = server.local_addr();
    let input = f.submission();
    let request = client()
        .post(format!("http://{address}/v1/agent-runs"))
        .bearer_auth(TOKEN)
        .json(&input);
    let response = tokio::spawn(async { request.send().await.unwrap() });
    timeout(Duration::from_secs(5), auth.entered.notified())
        .await
        .unwrap();
    server.begin_shutdown();
    assert_eq!(server.health().status(), AgentHttpServerStatus::Draining);
    assert!(
        timeout(Duration::from_millis(25), server.shutdown())
            .await
            .is_err()
    );
    assert!(TcpStream::connect(address).await.is_err());
    auth.release.add_permits(1);
    let admitted = snapshot(response.await.unwrap(), 201).await;
    let report = server.shutdown().await.unwrap();
    assert_eq!(report.forced_connections, 0);
    assert_eq!(report.connection_failures, 0);
    assert_eq!(server.health().active_connections(), 0);
    assert_eq!(
        f.service
            .load_by_key(f.caller.clone(), &input.submission_key)
            .await
            .unwrap()
            .provenance()
            .run_id(),
        admitted.provenance().run_id()
    );
    // The server owns no pool close and starts no executor.
    f.store.health_check().await.unwrap();
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn postgres_owned_forced_body_sse_and_handle_drop_close_sockets() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let mut server = start(&f, Host::ready(), options()).await;
    let address = server.local_addr();
    let input = f.submission();
    let admitted = snapshot(
        client()
            .post(format!("http://{address}/v1/agent-runs"))
            .bearer_auth(TOKEN)
            .json(&input)
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let mut stream = client()
        .get(format!(
            "http://{address}/v1/agent-runs/{}/events",
            admitted.provenance().run_id()
        ))
        .bearer_auth(TOKEN)
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(stream.status(), 200);
    stream.chunk().await.unwrap().unwrap();
    let mut body = TcpStream::connect(address).await.unwrap();
    body.write_all(format!("POST /v1/agent-runs HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: 500\r\n\r\n{{").as_bytes()).await.unwrap();
    until(|| server.health().active_connections() >= 2).await;
    // Allow the complete headers to enter the handler before drain.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let report = timeout(Duration::from_secs(3), server.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(report.forced_connections >= 1);
    assert_eq!(server.health().active_connections(), 0);
    assert_eq!(server.health().active_streams(), 0);
    closed(&mut body).await;
    timeout(Duration::from_secs(3), async {
        while let Ok(Some(_)) = stream.chunk().await {}
    })
    .await
    .unwrap();
    assert!(TcpStream::connect(address).await.is_err());
    f.store.health_check().await.unwrap();

    let server = start(&f, Host::ready(), options()).await;
    let health = server.health();
    let address = server.local_addr();
    let mut idle = TcpStream::connect(address).await.unwrap();
    until(|| health.active_connections() == 1).await;
    drop(server);
    closed(&mut idle).await;
    until(|| health.active_connections() == 0).await;
    assert_eq!(health.status(), AgentHttpServerStatus::Stopped);
    assert!(TcpStream::connect(address).await.is_err());
}

#[tokio::test]
async fn postgres_owned_transport_capacity_headers_and_absolute_lifetime() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let settings = options()
        .with_transport_limits(
            1,
            Duration::from_millis(150),
            Duration::from_secs(2),
            Duration::from_millis(150),
        )
        .unwrap();
    let mut server = start(&f, Host::ready(), settings).await;
    let address = server.local_addr();
    let mut idle = TcpStream::connect(address).await.unwrap();
    until(|| server.health().active_connections() == 1).await;
    let mut excess = TcpStream::connect(address).await.unwrap();
    closed(&mut excess).await;
    closed(&mut idle).await;
    until(|| server.health().active_connections() == 0).await;
    let mut headers = TcpStream::connect(address).await.unwrap();
    headers
        .write_all(
            format!(
                "GET /v1/agent-runs HTTP/1.1\r\nHost: {address}\r\n{}\r\n",
                "X-Test: value\r\n".repeat(70)
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    timeout(
        Duration::from_secs(3),
        headers.read_to_string(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(response.starts_with("HTTP/1.1 431"));
    until(|| server.health().active_connections() == 0).await;
    let mut oversized = TcpStream::connect(address).await.unwrap();
    let _ = oversized
        .write_all(
            format!(
                "GET / HTTP/1.1\r\nHost: {address}\r\nX-Large: {}\r\n\r\n",
                "a".repeat(40_000)
            )
            .as_bytes(),
        )
        .await;
    closed(&mut oversized).await;
    let report = server.shutdown().await.unwrap();
    assert!(report.rejected_connections >= 1);
    assert!(report.connection_failures >= 1);

    let settings = options()
        .with_transport_limits(
            8,
            Duration::from_millis(200),
            Duration::from_millis(350),
            Duration::from_millis(150),
        )
        .unwrap();
    let mut server = start(&f, Host::ready(), settings).await;
    let address = server.local_addr();
    let input = f.submission();
    let run = f
        .service
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
        .run_id();
    let began = Instant::now();
    let mut stream = client()
        .get(format!("http://{address}/v1/agent-runs/{run}/events"))
        .bearer_auth(TOKEN)
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(stream.status(), 200);
    timeout(Duration::from_secs(3), async {
        while let Ok(Some(_)) = stream.chunk().await {}
    })
    .await
    .unwrap();
    assert!(began.elapsed() < Duration::from_secs(3));
    let report = server.shutdown().await.unwrap();
    assert_eq!(report.forced_connections, 0);
    assert!(report.connection_failures >= 1);
}

#[tokio::test]
async fn postgres_owned_startup_timeout_and_cancel_drop_claimed_service() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let host = Host::ready();
    host.block.store(true, Ordering::SeqCst);
    let (listener, http) = configured(&f, f.auth.clone()).await;
    let settings = options()
        .with_readiness_limits(
            Duration::from_millis(50),
            Duration::from_millis(150),
            Duration::from_secs(1),
        )
        .unwrap();
    assert!(matches!(
        AgentHttpServer::start(listener, http, host.clone(), settings).await,
        Err(AgentHttpServerError::Unavailable)
    ));
    assert_eq!(host.active.load(Ordering::SeqCst), 0);
    let (listener, http) = configured(&f, f.auth.clone()).await;
    let address = listener.local_addr().unwrap();
    assert!(
        timeout(
            Duration::from_millis(100),
            AgentHttpServer::start(listener, http.clone(), host.clone(), options())
        )
        .await
        .is_err()
    );
    assert!(TcpStream::connect(address).await.is_err());
    assert_eq!(host.active.load(Ordering::SeqCst), 0);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    assert!(matches!(
        AgentHttpServer::start(listener, http, Host::ready(), options()).await,
        Err(AgentHttpServerError::AlreadyClaimed)
    ));
}

#[test]
fn owned_server_options_reject_unbounded_values() {
    let settings = AgentHttpServerOptions::default();
    assert!(
        settings
            .clone()
            .with_transport_limits(
                0,
                Duration::from_secs(1),
                Duration::from_secs(10),
                Duration::from_secs(1)
            )
            .is_err()
    );
    assert!(
        settings
            .clone()
            .with_transport_limits(
                4097,
                Duration::from_secs(1),
                Duration::from_secs(10),
                Duration::from_secs(1)
            )
            .is_err()
    );
    assert!(
        settings
            .clone()
            .with_readiness_limits(
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1)
            )
            .is_err()
    );
    assert!(
        settings
            .with_readiness_limits(
                Duration::ZERO,
                Duration::from_secs(1),
                Duration::from_secs(2)
            )
            .is_err()
    );
}
