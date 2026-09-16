// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot::agent_http::introspection::{
    AgentHttpIntrospection, ClientSecret, IntrospectionOptions, TenantBinding, TenantPolicy,
};

const SUBJECT: &str = "4cd727b3-c1b7-4ae5-9731-b42d3fd566ca";
struct Dependencies {
    identity: Arc<AgentHttpIntrospection>,
    resources: Arc<agent_policy::AgentResourcePolicy>,
}
impl AgentHttpReadiness for Dependencies {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentHttpReadinessError>> {
        Box::pin(async move {
            self.resources.check().await?;
            self.identity.check().await?;
            self.resources.check().await
        })
    }
}
fn secret(value: &str) -> ClientSecret {
    ClientSecret::new(value.to_owned()).unwrap()
}
fn credential(value: &str) -> AgentHttpCredential {
    AgentHttpCredential::new(value).unwrap()
}
fn config(issuer: &str) -> IntrospectionOptions {
    IntrospectionOptions::new(
        &format!("{issuer}/protocol/openid-connect/token/introspect"),
        issuer.parse().unwrap(),
        "stateknot-http".into(),
        "stateknot-http".into(),
        [
            "stateknot:submit".into(),
            "stateknot:read".into(),
            "stateknot:cancel".into(),
        ],
    )
    .unwrap()
}
async fn access_token(client: &reqwest::Client, issuer: &str, id: &str, secret: &str) -> String {
    let response = client
        .post(format!("{issuer}/protocol/openid-connect/token"))
        .basic_auth(id, Some(secret))
        .form(&[("grant_type", "client_credentials")])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "fixture token endpoint failed");
    response.json::<Value>().await.unwrap()["access_token"]
        .as_str()
        .expect("access token missing")
        .to_owned()
}
fn binding(tenant: TenantId, issuer: &str, operations: &[AgentHttpOperation]) -> TenantBinding {
    TenantBinding::new(
        tenant,
        PrincipalIdentity::new(issuer.parse().unwrap(), SUBJECT.parse().unwrap()),
        operations,
    )
}
async fn status_until(health: &AgentHttpServerHealth, ready: bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while health.is_ready() != ready {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn keycloak_tls_authentication_rotation_revocation_and_owned_ingress() {
    let issuer = match std::env::var("STATEKNOT_TEST_IDENTITY_ISSUER") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent)
            if std::env::var_os("STATEKNOT_REQUIRE_IDENTITY_TESTS").is_none() =>
        {
            return;
        }
        _ => panic!("mandatory Keycloak issuer missing or invalid"),
    };
    let ca = std::fs::read(std::env::var("STATEKNOT_TEST_IDENTITY_CA").expect("mandatory CA path"))
        .unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .add_root_certificate(reqwest::Certificate::from_pem(&ca).unwrap())
        .build()
        .unwrap();
    let token = access_token(&client, &issuer, "agent-caller", "fixture-caller-secret").await;
    let tenant = TenantId::new(format!("identity-{}", RunId::generate())).unwrap();
    let principal = PrincipalIdentity::new(issuer.parse().unwrap(), SUBJECT.parse().unwrap());
    let all = [
        AgentHttpOperation::Submit,
        AgentHttpOperation::Read,
        AgentHttpOperation::Cancel,
    ];
    let policy = Arc::new(
        TenantPolicy::new(
            vec![binding(tenant.clone(), &issuer, &all)],
            Duration::from_secs(300),
        )
        .unwrap(),
    );
    let auth = Arc::new(
        AgentHttpIntrospection::new(
            config(&issuer).with_root_certificate(&ca).unwrap(),
            secret("fixture-introspection-secret"),
            policy.clone(),
        )
        .unwrap(),
    );
    assert!(auth.check().await.is_ok(), "real TLS/client-auth readiness");
    let verified = auth.authenticate(credential(&token)).await.unwrap();
    assert_eq!(verified.caller().principal(), &principal);
    assert_eq!(verified.caller().tenant_id(), &tenant);
    assert!(all.into_iter().all(|op| verified.allows(op)));
    let no_trust = AgentHttpIntrospection::new(
        config(&issuer),
        secret("fixture-introspection-secret"),
        policy.clone(),
    )
    .unwrap();
    assert_eq!(
        no_trust.authenticate(credential(&token)).await.unwrap_err(),
        AgentHttpAuthenticationError::Unavailable
    );

    let mut f = Fixture::for_identity(tenant.clone(), "identity-graph", principal)
        .await
        .expect("real PostgreSQL required");
    let resource_doc = super::resource_policy::operator_document(&f);
    let resources = Arc::new(
        agent_policy::AgentResourcePolicy::new(
            agent_policy::PolicyArtifact::new(resource_doc.clone()).unwrap(),
            Duration::from_secs(300),
        )
        .unwrap(),
    );
    f.service = super::resource_policy::install(&f, resources.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let http = AgentHttpService::new(
        f.service.clone(),
        auth.clone(),
        AgentHttpOptions::loopback(address.port())
            .unwrap()
            .with_sse(AgentHttpSseOptions::default()),
    );
    let server_options = AgentHttpServerOptions::default()
        .with_readiness_limits(
            Duration::from_millis(100),
            Duration::from_secs(4),
            Duration::from_secs(10),
        )
        .unwrap();
    let dependencies = Arc::new(Dependencies {
        identity: auth.clone(),
        resources: resources.clone(),
    });
    let mut server = AgentHttpServer::start(listener, http, dependencies, server_options)
        .await
        .unwrap();
    let health = server.health();
    let url = format!("http://{address}/v1/agent-runs");
    let submission = f.submission();
    let admitted = snapshot(
        client
            .post(&url)
            .bearer_auth(&token)
            .json(&submission)
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let run_url = format!("{url}/{}", admitted.provenance().run_id());
    assert_eq!(
        f.node_calls.load(Ordering::SeqCst),
        0,
        "ingress does not execute work"
    );
    assert_eq!(
        client
            .get(&run_url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    // Resource policy remains mandatory even after successful authentication.
    let mut denied = resource_doc.clone();
    denied.runs.clear();
    resources
        .replace(
            1,
            agent_policy::PolicyArtifact::new(denied).unwrap(),
            Duration::from_secs(300),
        )
        .unwrap();
    assert_eq!(
        client
            .get(&run_url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    resources
        .replace(
            2,
            agent_policy::PolicyArtifact::new(resource_doc).unwrap(),
            Duration::from_secs(300),
        )
        .unwrap();
    policy
        .replace(
            1,
            vec![binding("another-tenant".parse().unwrap(), &issuer, &all)],
            Duration::from_secs(300),
        )
        .unwrap();
    assert_eq!(
        client
            .get(&run_url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    policy
        .replace(
            2,
            vec![binding(
                tenant.clone(),
                &issuer,
                &[AgentHttpOperation::Read],
            )],
            Duration::from_secs(300),
        )
        .unwrap();
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(&token)
            .json(&submission)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .get(&run_url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    policy
        .replace(
            3,
            vec![binding(tenant.clone(), &issuer, &all)],
            Duration::from_secs(300),
        )
        .unwrap();

    let admin = access_token(&client, &issuer, "fixture-admin", "fixture-admin-secret").await;
    let admin_url = format!(
        "{}/admin/realms/stateknot-qualification",
        issuer.split("/realms/").next().unwrap()
    );
    let rotate = client
        .post(format!(
            "{admin_url}/clients/stateknot-introspection-client/client-secret"
        ))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(rotate.status(), 200, "real secret rotation");
    let rotated = rotate.json::<Value>().await.unwrap()["value"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        auth.authenticate(credential(&token)).await.unwrap_err(),
        AgentHttpAuthenticationError::Unavailable
    );
    status_until(&health, false).await;
    assert_eq!(
        client
            .get(&run_url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        503
    );
    auth.replace_client_secret(secret(&rotated)).unwrap();
    assert!(auth.authenticate(credential(&token)).await.is_ok());
    status_until(&health, true).await;
    assert_eq!(
        client
            .get(&run_url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    // A live SSE subscription uses the same online revalidation, not a cache.
    let mut events = client
        .get(format!("{run_url}/events"))
        .bearer_auth(&token)
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(events.status(), 200);
    assert!(events.chunk().await.unwrap().is_some());
    let disable = client
        .put(format!("{admin_url}/users/{SUBJECT}"))
        .bearer_auth(&admin)
        .json(&json!({"enabled":false}))
        .send()
        .await
        .unwrap();
    assert_eq!(disable.status(), 204, "real subject revocation");
    assert_eq!(
        auth.authenticate(credential(&token)).await.unwrap_err(),
        AgentHttpAuthenticationError::Unauthenticated
    );
    assert_eq!(
        client
            .get(&run_url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while events.chunk().await.unwrap().is_some() {}
    })
    .await
    .unwrap();

    policy
        .replace(
            4,
            vec![binding(tenant, &issuer, &all)],
            Duration::from_millis(1),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(auth.check().await.is_err());
    status_until(&health, false).await;
    let report = server.shutdown().await.unwrap();
    assert!(!report.listener_failed);
    assert_eq!(health.active_connections(), 0);
    assert_eq!(health.active_streams(), 0);
    println!(
        "\nSTATEKNOT_IDENTITY_EVIDENCE={{\"provider\":\"keycloak-26.7.3\",\"verified_tls\":true,\"rotation\":true,\"revocation\":true,\"cross_tenant_denied\":true,\"resource_policy_required\":true,\"sse_closed\":true,\"readiness_recovered\":true,\"policy_expiry\":true,\"drained\":true}}"
    );
}
