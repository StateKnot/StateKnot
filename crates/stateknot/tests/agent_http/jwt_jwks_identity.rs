// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use aws_lc_rs::{
    rand::SystemRandom,
    rsa::KeySize,
    signature::{KeyPair, RSA_PKCS1_SHA256, RsaKeyPair, RsaPublicKeyComponents},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use stateknot::agent_http::{
    introspection::{TenantBinding, TenantPolicy},
    jwt_jwks::{AgentHttpJwtJwks, JwtJwksOptions},
};
use std::time::{SystemTime, UNIX_EPOCH};

const ISSUER: &str = "https://jwt-issuer.example.test";

struct Signer {
    key: RsaKeyPair,
    jwk: Value,
    kid: String,
}
impl Signer {
    fn new(seed: u64) -> Self {
        // Test-only private key; never persisted or deployed.
        let key = RsaKeyPair::generate(KeySize::Rsa2048).unwrap();
        let public = RsaPublicKeyComponents::<Vec<u8>>::from(key.public_key());
        let kid = format!("jwt-fixture-{seed}");
        Self {
            key,
            jwk: json!({"kty":"RSA","alg":"RS256","use":"sig","kid":kid,
                "n":URL_SAFE_NO_PAD.encode(public.n),
                "e":URL_SAFE_NO_PAD.encode(public.e)}),
            kid,
        }
    }
    fn token(&self, scopes: &str) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let header = json!({"alg":"RS256", "typ":"at+jwt", "kid":self.kid});
        let claims = json!({"iss":ISSUER,"aud":"stateknot-http","sub":"jwt-subject",
            "iat":now,"exp":now+300,"jti":"test-access-token","client_id":"test-client",
            "scope":scopes,"tenant":"attacker-tenant"});
        let input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap()),
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
        );
        let mut signature = vec![0; self.key.public_modulus_len()];
        self.key
            .sign(
                &RSA_PKCS1_SHA256,
                &SystemRandom::new(),
                input.as_bytes(),
                &mut signature,
            )
            .unwrap();
        format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature))
    }

    fn jwks(&self) -> Vec<u8> {
        serde_json::to_vec(&json!({"keys":[self.jwk]})).unwrap()
    }
}

struct Dependencies {
    identity: Arc<AgentHttpJwtJwks>,
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
fn binding(tenant: TenantId) -> TenantBinding {
    TenantBinding::new(
        tenant,
        PrincipalIdentity::new(ISSUER.parse().unwrap(), "jwt-subject".parse().unwrap()),
        &[
            AgentHttpOperation::Submit,
            AgentHttpOperation::Read,
            AgentHttpOperation::Cancel,
        ],
    )
}
fn options() -> JwtJwksOptions {
    JwtJwksOptions::new(
        ISSUER.parse().unwrap(),
        "stateknot-http".into(),
        ["submit".into(), "read".into(), "cancel".into()],
    )
    .unwrap()
}
async fn start(
    f: &Fixture,
    identity: Arc<AgentHttpJwtJwks>,
    resources: Arc<agent_policy::AgentResourcePolicy>,
) -> (AgentHttpServer, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let http = AgentHttpService::new(
        f.service.clone(),
        identity.clone(),
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
    let server = AgentHttpServer::start(
        listener,
        http,
        Arc::new(Dependencies {
            identity,
            resources,
        }),
        AgentHttpServerOptions::default()
            .with_readiness_limits(
                Duration::from_millis(50),
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    (server, format!("http://{address}/v1/agent-runs"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_jwt_rotation_authorization_sse_expiry_and_restart() {
    let tenant = TenantId::new(format!("jwt-{}", RunId::generate())).unwrap();
    let principal = PrincipalIdentity::new(ISSUER.parse().unwrap(), "jwt-subject".parse().unwrap());
    let Some(mut f) = Fixture::for_identity(tenant.clone(), "jwt-graph", principal).await else {
        return;
    };
    let old = Signer::new(10);
    let new = Signer::new(11);
    let policy = Arc::new(
        TenantPolicy::new(vec![binding(tenant.clone())], Duration::from_secs(300)).unwrap(),
    );
    let auth = Arc::new(
        AgentHttpJwtJwks::new(
            options(),
            &old.jwks(),
            Duration::from_secs(300),
            policy.clone(),
        )
        .unwrap(),
    );
    let resource_doc = super::resource_policy::operator_document(&f);
    let resources = Arc::new(
        agent_policy::AgentResourcePolicy::new(
            agent_policy::PolicyArtifact::new(resource_doc.clone()).unwrap(),
            Duration::from_secs(300),
        )
        .unwrap(),
    );
    f.service = super::resource_policy::install(&f, resources.clone());
    let (mut server, url) = start(&f, auth.clone(), resources.clone()).await;
    let token = old.token("submit read cancel");
    let submission = f.submission();
    let admitted = snapshot(
        client()
            .post(&url)
            .bearer_auth(&token)
            .json(&submission)
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let run = admitted.provenance().run_id();
    let run_url = format!("{url}/{run}");
    assert_eq!(admitted.provenance().tenant_id(), &tenant);
    assert_eq!(
        client()
            .get(&run_url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    // Independently verified identity still cannot bypass resource permission.
    let mut denied = resource_doc.clone();
    denied.runs.clear();
    resources
        .replace(
            1,
            agent_policy::PolicyArtifact::new(denied).unwrap(),
            Duration::from_secs(300),
        )
        .unwrap();
    error(
        client()
            .get(&run_url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        403,
        "denied",
    )
    .await;
    resources
        .replace(
            2,
            agent_policy::PolicyArtifact::new(resource_doc.clone()).unwrap(),
            Duration::from_secs(300),
        )
        .unwrap();
    policy
        .replace(
            1,
            vec![binding("another-jwt-tenant".parse().unwrap())],
            Duration::from_secs(300),
        )
        .unwrap();
    error(
        client()
            .get(&run_url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        403,
        "denied",
    )
    .await;
    policy
        .replace(2, vec![binding(tenant.clone())], Duration::from_secs(300))
        .unwrap();
    error(
        client()
            .post(&url)
            .bearer_auth(old.token("read"))
            .json(&submission)
            .send()
            .await
            .unwrap(),
        403,
        "denied",
    )
    .await;
    error(
        client()
            .post(&url)
            .bearer_auth("invalid.jwt.token")
            .body("malformed-json")
            .send()
            .await
            .unwrap(),
        401,
        "unauthenticated",
    )
    .await;

    let mut events = client()
        .get(format!("{run_url}/events"))
        .bearer_auth(&token)
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(events.status(), 200);
    assert!(events.chunk().await.unwrap().is_some());
    assert!(
        auth.replace_jwks(1, b"bad-jwks", Duration::from_secs(300))
            .is_err()
    );
    assert_eq!(auth.generation().unwrap(), 1);
    auth.replace_jwks(1, &new.jwks(), Duration::from_secs(300))
        .unwrap();
    error(
        client()
            .get(&run_url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        401,
        "unauthenticated",
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while events.chunk().await.unwrap().is_some() {}
    })
    .await
    .unwrap();
    let new_token = new.token("submit read cancel");
    assert_eq!(
        client()
            .get(&run_url)
            .bearer_auth(&new_token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    auth.replace_jwks(2, &new.jwks(), Duration::from_millis(100))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(auth.check().await.is_err());
    error(
        client()
            .get(&run_url)
            .bearer_auth(&new_token)
            .send()
            .await
            .unwrap(),
        503,
        "unavailable",
    )
    .await;
    auth.replace_jwks(3, &new.jwks(), Duration::from_secs(300))
        .unwrap();
    auth.check().await.unwrap();
    let health = server.health();
    let report = server.shutdown().await.unwrap();
    assert!(!report.listener_failed);
    assert_eq!(health.active_streams(), 0);
    assert_eq!(health.active_connections(), 0);

    // A newly constructed verifier and owned listener recover the exact durable
    // admission using the new key. No old process-local key generation is reused.
    let policy =
        Arc::new(TenantPolicy::new(vec![binding(tenant)], Duration::from_secs(300)).unwrap());
    let restarted = Arc::new(
        AgentHttpJwtJwks::new(options(), &new.jwks(), Duration::from_secs(300), policy).unwrap(),
    );
    let (mut server, url) = start(&f, restarted, resources).await;
    let recovered = snapshot(
        client()
            .post(&url)
            .bearer_auth(&new_token)
            .json(&submission)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(recovered.provenance().run_id(), run);
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 0);
    assert!(!server.shutdown().await.unwrap().listener_failed);
    println!(
        "\nSTATEKNOT_JWT_JWKS_EVIDENCE={{\"profile\":\"rfc9068-rs256-local-jwks\",\"real_signature\":true,\"key_rotation\":true,\"cross_tenant_denied\":true,\"resource_policy_required\":true,\"sse_closed\":true,\"key_expiry_recovered\":true,\"fresh_verifier_replay\":true,\"drained\":true}}"
    );
}

#[tokio::test]
async fn keycloak_tls_rfc9068_jwks_and_durable_admission() {
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
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .add_root_certificate(reqwest::Certificate::from_pem(&ca).unwrap())
        .build()
        .unwrap();
    let response = client
        .post(format!("{issuer}/protocol/openid-connect/token"))
        .basic_auth("jwt-agent-caller", Some("fixture-jwt-caller-secret"))
        .form(&[("grant_type", "client_credentials")])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "fixture token endpoint failed");
    let response = response.json::<Value>().await.unwrap();
    let token = response["access_token"]
        .as_str()
        .expect("access token missing");
    // This fixed HTTPS endpoint is trusted fixture provisioning, not token discovery.
    let response = client
        .get(format!("{issuer}/protocol/openid-connect/certs"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut jwks = response.json::<Value>().await.unwrap();
    jwks["keys"]
        .as_array_mut()
        .unwrap()
        .retain(|key| key["alg"] == "RS256" && key["use"] == "sig");
    let jwks = serde_json::to_vec(&jwks).unwrap();
    let tenant = TenantId::new(format!("keycloak-jwt-{}", RunId::generate())).unwrap();
    let principal = PrincipalIdentity::new(
        issuer.parse().unwrap(),
        "5897f148-cdb3-4b7f-b17e-cc1ace766a71".parse().unwrap(),
    );
    let operations = [
        AgentHttpOperation::Submit,
        AgentHttpOperation::Read,
        AgentHttpOperation::Cancel,
    ];
    let policy = Arc::new(
        TenantPolicy::new(
            vec![TenantBinding::new(
                tenant.clone(),
                principal.clone(),
                &operations,
            )],
            Duration::from_secs(300),
        )
        .unwrap(),
    );
    let options = JwtJwksOptions::new(
        issuer.parse().unwrap(),
        "stateknot-http".into(),
        [
            "stateknot:submit".into(),
            "stateknot:read".into(),
            "stateknot:cancel".into(),
        ],
    )
    .unwrap();
    let identity =
        Arc::new(AgentHttpJwtJwks::new(options, &jwks, Duration::from_secs(300), policy).unwrap());
    let verified = identity
        .authenticate(AgentHttpCredential::new(token).unwrap())
        .await
        .unwrap();
    assert_eq!(verified.caller().principal(), &principal);
    assert_eq!(verified.caller().tenant_id(), &tenant);
    assert!(operations.into_iter().all(|op| verified.allows(op)));
    let mut f = Fixture::for_identity(tenant, "keycloak-jwt-graph", principal)
        .await
        .expect("real PostgreSQL required");
    let resources = Arc::new(
        agent_policy::AgentResourcePolicy::new(
            agent_policy::PolicyArtifact::new(super::resource_policy::operator_document(&f))
                .unwrap(),
            Duration::from_secs(300),
        )
        .unwrap(),
    );
    f.service = super::resource_policy::install(&f, resources.clone());
    let (mut server, url) = start(&f, identity.clone(), resources).await;
    let submission = f.submission();
    let admitted = snapshot(
        client
            .post(&url)
            .bearer_auth(token)
            .json(&submission)
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let recovered = snapshot(
        client
            .post(&url)
            .bearer_auth(token)
            .json(&submission)
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
    identity
        .replace_jwks(1, br#"{"keys":[]}"#, Duration::from_secs(300))
        .unwrap();
    assert!(identity.check().await.is_err());
    assert_eq!(
        identity
            .authenticate(AgentHttpCredential::new(token).unwrap())
            .await
            .unwrap_err(),
        AgentHttpAuthenticationError::Unauthenticated
    );
    assert!(!server.shutdown().await.unwrap().listener_failed);
    println!(
        "\nSTATEKNOT_JWT_IDENTITY_EVIDENCE={{\"provider\":\"keycloak-26.7.3\",\"verified_tls\":true,\"rfc9068\":true,\"algorithm\":\"RS256\",\"operator_provisioned_jwks\":true,\"trusted_tenant\":true,\"exact_key_replay\":true,\"empty_key_revocation\":true,\"drained\":true}}"
    );
}
