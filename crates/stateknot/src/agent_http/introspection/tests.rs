// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::super::AgentHttpOperation;
use super::*;
use axum::{Router, body::Body, extract::Request, response::Response};
use serde_json::json;
use stateknot_core::PrincipalIdentity;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::{net::TcpListener, task::JoinHandle};

fn options() -> IntrospectionOptions {
    IntrospectionOptions::new(
        "https://issuer.example.test/introspect",
        "https://issuer.example.test".parse().unwrap(),
        "agents".into(),
        "client: id".into(),
        ["submit".into(), "read".into(), "cancel".into()],
    )
    .unwrap()
}
fn identity() -> PrincipalIdentity {
    PrincipalIdentity::new(options().issuer, "subject".parse().unwrap())
}
fn binding() -> TenantBinding {
    TenantBinding::new(
        "tenant-one".parse().unwrap(),
        identity(),
        &[AgentHttpOperation::Read],
    )
}
fn policy() -> Arc<TenantPolicy> {
    Arc::new(TenantPolicy::new(vec![binding()], Duration::from_secs(30)).unwrap())
}
fn active() -> Value {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    json!({"active":true,"iss":"https://issuer.example.test","sub":"subject","aud":["agents","other"],"iat":now,"exp":now+300,"token_type":"Bearer","scope":"read submit"})
}
fn secret() -> ClientSecret {
    ClientSecret::new("secret:/ +é".into()).unwrap()
}
fn token() -> AgentHttpCredential {
    AgentHttpCredential::new("opaque-token").unwrap()
}

#[test]
fn configuration_and_secrets_fail_closed() {
    for endpoint in [
        "http://localhost/x",
        "https://id:secret@issuer.example.test/x",
        "https://issuer.example.test/x?q=1",
        "https://issuer.example.test/#x",
    ] {
        assert!(
            IntrospectionOptions::new(
                endpoint,
                options().issuer,
                "agents".into(),
                "client".into(),
                ["s".into(), "r".into(), "c".into()]
            )
            .is_err()
        );
    }
    for value in [String::new(), "s".repeat(4097), "secret\n".into()] {
        assert!(ClientSecret::new(value).is_err());
    }
    assert_eq!(format!("{:?}", secret()), "ClientSecret([REDACTED])");
    assert_eq!(form_encode("client: id").as_str(), "client%3A+id");
    assert_eq!(
        form_encode("secret:/ +é").as_str(),
        "secret%3A%2F+%2B%C3%A9"
    );
    assert!(
        options()
            .with_limits(Duration::ZERO, 1, Duration::from_secs(1))
            .is_err()
    );
    assert!(
        options()
            .with_limits(Duration::from_secs(1), 257, Duration::from_secs(1))
            .is_err()
    );
    assert!(
        options()
            .with_limits(Duration::from_secs(1), 1, Duration::from_millis(1500))
            .is_err()
    );
    assert!(
        options()
            .with_root_certificate(b"not a certificate")
            .is_err()
    );
}

#[test]
fn claims_are_exact_bounded_and_never_trust_tenant() {
    let mut good = active();
    good["tenant"] = json!("attacker-tenant");
    let (principal, scopes) = claims::verify(&good, &options()).unwrap();
    let mapped = policy()
        .resolve(&principal, &scopes, &options().required_scopes)
        .unwrap();
    assert_eq!(mapped.caller().tenant_id().as_str(), "tenant-one");
    assert!(mapped.allows(AgentHttpOperation::Read));
    assert!(!mapped.allows(AgentHttpOperation::Submit));
    assert!(!mapped.allows(AgentHttpOperation::Cancel));
    for (field, value) in [
        ("active", json!(false)),
        ("iss", json!("https://issuer.example.test/")),
        ("aud", json!("other")),
        ("aud", json!(["agents", 1])),
        ("aud", json!([])),
        ("exp", json!(0)),
        ("exp", json!("9999999999")),
        ("exp", json!(1.5)),
        ("iat", json!(u64::MAX)),
        ("nbf", json!(u64::MAX)),
        ("nbf", json!(-1)),
        ("sub", json!("")),
        ("token_type", json!("Refresh")),
        ("cnf", json!(null)),
        ("scope", json!("read  submit")),
        ("scope", json!("read read")),
        ("scope", json!("read\tsubmit")),
        ("scope", json!("x".repeat(129))),
    ] {
        let mut wrong = active();
        wrong[field] = value;
        assert!(claims::verify(&wrong, &options()).is_err(), "field {field}");
    }
    for field in ["iss", "aud", "sub", "exp", "iat", "token_type"] {
        let mut wrong = active();
        wrong.as_object_mut().unwrap().remove(field);
        assert!(
            claims::verify(&wrong, &options()).is_err(),
            "missing {field}"
        );
    }
    assert!(
        claims::verify(
            &active(),
            &options()
                .with_limits(Duration::from_secs(1), 1, Duration::from_secs(60))
                .unwrap()
        )
        .is_err()
    );
}

#[tokio::test]
async fn policy_replacement_is_atomic_default_deny_and_expires() {
    let policy = policy();
    let scopes = vec!["read".into()];
    assert!(TenantPolicy::new(vec![binding(), binding()], Duration::from_secs(1)).is_err());
    assert!(TenantPolicy::new(vec![binding(); 1025], Duration::from_secs(1)).is_err());
    assert!(TenantPolicy::new(vec![], Duration::ZERO).is_err());
    assert!(
        policy
            .replace(1, vec![binding(), binding()], Duration::from_secs(1))
            .is_err()
    );
    assert!(
        policy
            .resolve(&identity(), &scopes, &options().required_scopes)
            .is_ok()
    );
    assert!(policy.replace(0, vec![], Duration::from_secs(1)).is_err());
    assert_eq!(
        policy.replace(1, vec![], Duration::from_secs(1)).unwrap(),
        2
    );
    assert_eq!(
        policy
            .resolve(&identity(), &scopes, &options().required_scopes)
            .unwrap_err(),
        AgentHttpAuthenticationError::Unauthenticated
    );
    assert_eq!(
        policy
            .replace(2, vec![binding()], Duration::from_millis(1))
            .unwrap(),
        3
    );
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        policy.check().unwrap_err(),
        AgentHttpAuthenticationError::Unavailable
    );
    assert!(
        policy
            .resolve(&identity(), &scopes, &options().required_scopes)
            .is_err()
    );
    assert_eq!(
        policy
            .replace(3, vec![binding()], Duration::from_secs(1))
            .unwrap(),
        4
    );
    assert!(policy.check().is_ok());
}

struct Server(JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// HTTP is permitted only in this private test fixture, never in the public API.
async fn mock<F, Fut>(handler: F, timeout: Duration) -> (AgentHttpIntrospection, Server)
where
    F: Fn(Request) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Response> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/introspect", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, Router::new().fallback(handler))
            .await
            .unwrap();
    });
    let mut config = options()
        .with_limits(timeout, 1, Duration::from_secs(3600))
        .unwrap();
    config.endpoint = endpoint.parse().unwrap();
    let client = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()
        .unwrap();
    (
        AgentHttpIntrospection {
            client,
            options: config,
            secret: RwLock::new(secret()),
            policy: policy(),
            permits: Semaphore::new(1),
        },
        Server(task),
    )
}
fn response(status: u16, body: String) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

#[tokio::test]
async fn wire_credentials_rotation_and_no_token_cache() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let (auth, _server) = mock(
        move |request| {
            let tx = tx.clone();
            async move {
                let header = request.headers()["authorization"]
                    .to_str()
                    .unwrap()
                    .strip_prefix("Basic ")
                    .unwrap()
                    .to_owned();
                let body = axum::body::to_bytes(request.into_body(), 32768)
                    .await
                    .unwrap();
                tx.send((
                    String::from_utf8(STANDARD.decode(header).unwrap()).unwrap(),
                    String::from_utf8(body.to_vec()).unwrap(),
                ))
                .await
                .unwrap();
                response(200, active().to_string())
            }
        },
        Duration::from_secs(1),
    )
    .await;
    assert!(auth.authenticate(token()).await.is_ok());
    let (header, body) = rx.recv().await.unwrap();
    assert_eq!(header, "client%3A+id:secret%3A%2F+%2B%C3%A9");
    assert_eq!(body, "token=opaque-token&token_type_hint=access_token");
    auth.replace_client_secret(ClientSecret::new("rotated".into()).unwrap())
        .unwrap();
    assert!(auth.authenticate(token()).await.is_ok());
    assert_eq!(rx.recv().await.unwrap().0, "client%3A+id:rotated");
    auth.policy
        .replace(1, vec![], Duration::from_secs(1))
        .unwrap();
    assert_eq!(
        auth.authenticate(token()).await.unwrap_err(),
        AgentHttpAuthenticationError::Unauthenticated
    );
}

#[tokio::test]
async fn protocol_faults_are_unavailable_not_fallback_identity() {
    for (status, body) in [
        (401, "{\"error\":\"secret provider text\"}".into()),
        (500, "{}".into()),
        (302, active().to_string()),
        (200, "{\"active\":true,\"active\":false}".into()),
        (200, "{\"active\":\"true\"}".into()),
        (200, "[]".into()),
        (
            200,
            format!(
                "{{\"active\":false,\"x\":\"{}\"}}",
                "x".repeat(MAX_RESPONSE)
            ),
        ),
        (
            200,
            format!(
                "{{\"active\":false,\"x\":{}0{}}}",
                "[".repeat(9),
                "]".repeat(9)
            ),
        ),
    ] {
        let (auth, _server) = mock(
            move |_| {
                let body = body.clone();
                async move { response(status, body) }
            },
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(
            auth.authenticate(token()).await.unwrap_err(),
            AgentHttpAuthenticationError::Unavailable
        );
    }
    let (auth, _server) = mock(
        |_| async { response(200, "{\"active\":false}".into()) },
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        auth.authenticate(token()).await.unwrap_err(),
        AgentHttpAuthenticationError::Unauthenticated
    );
    assert!(auth.check().await.is_ok());
}

#[tokio::test]
async fn timeout_and_cancellation_release_bounded_capacity() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let notified = entered.clone();
    let (auth, _server) = mock(
        move |_| {
            let entered = entered.clone();
            async move {
                entered.notify_one();
                std::future::pending().await
            }
        },
        Duration::from_millis(100),
    )
    .await;
    let auth = Arc::new(auth);
    let running = auth.clone();
    let task = tokio::spawn(async move { running.authenticate(token()).await });
    notified.notified().await;
    assert_eq!(
        auth.authenticate(token()).await.unwrap_err(),
        AgentHttpAuthenticationError::Unavailable
    );
    task.abort();
    let _ = task.await;
    assert_eq!(auth.permits.available_permits(), 1);
    assert_eq!(
        auth.authenticate(token()).await.unwrap_err(),
        AgentHttpAuthenticationError::Unavailable
    );
    assert_eq!(auth.permits.available_permits(), 1);
}

#[tokio::test]
async fn content_headers_and_redirects_never_bypass_verification() {
    for (name, value) in [
        ("content-type", "text/plain"),
        ("content-type", "application/json; charset=latin1"),
        ("content-encoding", "gzip"),
    ] {
        let (auth, _server) = mock(
            move |_| async move {
                let mut response = response(200, active().to_string());
                response.headers_mut().insert(
                    header::HeaderName::from_static(name),
                    header::HeaderValue::from_static(value),
                );
                response
            },
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(
            auth.authenticate(token()).await.unwrap_err(),
            AgentHttpAuthenticationError::Unavailable
        );
    }
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = calls.clone();
    let (auth, _server) = mock(
        move |_| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Response::builder()
                    .status(307)
                    .header("location", "/redirected")
                    .body(Body::empty())
                    .unwrap()
            }
        },
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        auth.authenticate(token()).await.unwrap_err(),
        AgentHttpAuthenticationError::Unavailable
    );
    assert_eq!(observed.load(std::sync::atomic::Ordering::SeqCst), 1);
    let (auth, _server) = mock(
        |_| async { response(200, active().to_string()) },
        Duration::from_secs(1),
    )
    .await;
    assert!(
        auth.check().await.is_err(),
        "active canary is not readiness"
    );
}
