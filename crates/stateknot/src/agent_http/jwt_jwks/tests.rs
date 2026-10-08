// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::agent_http::{AgentHttpOperation, introspection::TenantBinding};
use aws_lc_rs::{
    rand::SystemRandom,
    rsa::KeySize,
    signature::{KeyPair, RSA_PKCS1_SHA256, RsaKeyPair, RsaPublicKeyComponents},
};
use serde_json::json;
use std::sync::OnceLock;

struct TestKey {
    jwk: Value,
    signing: RsaKeyPair,
    kid: String,
}

fn keys() -> &'static [TestKey; 2] {
    static KEYS: OnceLock<[TestKey; 2]> = OnceLock::new();
    KEYS.get_or_init(|| {
        [1, 2].map(|seed| {
            // Test-only private keys are generated in memory and never persisted.
            let key = RsaKeyPair::generate(KeySize::Rsa2048).unwrap();
            let public = RsaPublicKeyComponents::<Vec<u8>>::from(key.public_key());
            let kid = format!("test-key-{seed}");
            TestKey {
                jwk: json!({"kty":"RSA","use":"sig","alg":"RS256","kid":kid,
                    "n":URL_SAFE_NO_PAD.encode(public.n),
                    "e":URL_SAFE_NO_PAD.encode(public.e)}),
                signing: key,
                kid,
            }
        })
    })
}

fn options() -> JwtJwksOptions {
    JwtJwksOptions::new(
        "https://issuer.example.test".parse().unwrap(),
        "agents".into(),
        ["submit".into(), "read".into(), "cancel".into()],
    )
    .unwrap()
}

fn policy() -> Arc<TenantPolicy> {
    Arc::new(
        TenantPolicy::new(
            vec![TenantBinding::new(
                "tenant-one".parse().unwrap(),
                PrincipalIdentity::new(options().issuer, "subject".parse().unwrap()),
                &[AgentHttpOperation::Read, AgentHttpOperation::InspectHost],
            )],
            Duration::from_secs(300),
        )
        .unwrap(),
    )
}

fn jwks(indexes: &[usize]) -> Vec<u8> {
    serde_json::to_vec(
        &json!({"keys": indexes.iter().map(|index| &keys()[*index].jwk).collect::<Vec<_>>()}),
    )
    .unwrap()
}

fn verifier() -> AgentHttpJwtJwks {
    AgentHttpJwtJwks::new(options(), &jwks(&[0]), Duration::from_secs(300), policy()).unwrap()
}

fn claims() -> Value {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    json!({"iss":"https://issuer.example.test","sub":"subject","aud":["agents","other"],
        "iat":now,"exp":now+300,"jti":"test-token","client_id":"test-client", "scope":"read submit inspect",
        "tenant":"attacker-tenant"})
}

fn sign_with(index: usize, claims: &Value, header: &Value) -> String {
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(header).unwrap()),
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap())
    );
    let key = &keys()[index].signing;
    let mut signature = vec![0; key.public_modulus_len()];
    key.sign(
        &RSA_PKCS1_SHA256,
        &SystemRandom::new(),
        input.as_bytes(),
        &mut signature,
    )
    .unwrap();
    format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature))
}

fn header(index: usize) -> Value {
    json!({"alg":"RS256", "typ":"at+jwt", "kid":keys()[index].kid})
}

fn token(index: usize) -> AgentHttpCredential {
    AgentHttpCredential::new(sign_with(index, &claims(), &header(index))).unwrap()
}

#[test]
fn configuration_rejects_unsafe_limits_and_scope_aliases() {
    for scope in ["", "read", "submit", "cancel", "bad scope", "bad\n"] {
        assert!(options().with_host_inspection_scope(scope.into()).is_err());
    }
    for (lifetime, deadline, capacity) in [
        (Duration::ZERO, Duration::from_secs(1), 1),
        (Duration::from_secs(3601), Duration::from_secs(1), 1),
        (Duration::from_millis(1500), Duration::from_secs(1), 1),
        (Duration::from_secs(1), Duration::ZERO, 1),
        (Duration::from_secs(1), Duration::from_secs(11), 1),
        (Duration::from_secs(1), Duration::from_secs(1), 0),
        (Duration::from_secs(1), Duration::from_secs(1), 65),
    ] {
        assert!(options().with_limits(lifetime, deadline, capacity).is_err());
    }
    for audience in ["", "bad\n", &"a".repeat(513)] {
        assert!(
            JwtJwksOptions::new(options().issuer, audience.into(), options().required_scopes)
                .is_err()
        );
    }
    for lease in [Duration::ZERO, Duration::from_secs(3601)] {
        assert!(AgentHttpJwtJwks::new(options(), &jwks(&[0]), lease, policy()).is_err());
    }
    assert!(
        AgentHttpJwtJwks::new(options(), &jwks(&[]), Duration::from_secs(1), policy()).is_err()
    );
}

#[tokio::test]
async fn signed_access_token_maps_only_trusted_tenant_and_intersected_permissions() {
    let verifier = verifier();
    verifier.check().await.unwrap();
    let principal = verifier.authenticate(token(0)).await.unwrap();
    assert_eq!(principal.caller().tenant_id().as_str(), "tenant-one");
    assert!(principal.allows(AgentHttpOperation::Read));
    assert!(!principal.allows(AgentHttpOperation::Submit));
    assert!(!principal.allows(AgentHttpOperation::Cancel));
    assert!(!principal.allows(AgentHttpOperation::InspectHost));
    let enabled = AgentHttpJwtJwks::new(
        options()
            .with_host_inspection_scope("inspect".into())
            .unwrap(),
        &jwks(&[0]),
        Duration::from_secs(300),
        policy(),
    )
    .unwrap();
    assert!(
        enabled
            .authenticate(token(0))
            .await
            .unwrap()
            .allows(AgentHttpOperation::InspectHost)
    );
    let mut unknown = claims();
    unknown["sub"] = json!("unbound");
    assert_eq!(
        verifier
            .authenticate(AgentHttpCredential::new(sign_with(0, &unknown, &header(0))).unwrap())
            .await
            .unwrap_err(),
        AgentHttpAuthenticationError::Unauthenticated,
    );
    let generation = verifier.policy.generation().unwrap();
    verifier
        .policy
        .replace(generation, vec![], Duration::from_secs(300))
        .unwrap();
    assert_eq!(
        verifier.authenticate(token(0)).await.unwrap_err(),
        AgentHttpAuthenticationError::Unauthenticated
    );
}

#[test]
fn valid_signatures_still_require_exact_access_token_claims() {
    let key = parse_jwks(&jwks(&[0]))
        .unwrap()
        .remove(&keys()[0].kid)
        .unwrap();
    let good = claims();
    let now = good["iat"].as_u64().unwrap();
    for (name, value) in [
        ("iss", json!("https://issuer.example.test/")),
        ("aud", json!("other")),
        ("aud", json!(["agents", 1])),
        ("aud", json!([])),
        ("sub", json!("")),
        ("exp", json!(now)),
        ("exp", json!(now + 3601)),
        ("exp", json!("9999999999")),
        ("exp", json!(9999999999.5)),
        ("iat", json!(now + 10)),
        ("iat", json!(now + 300)),
        ("iat", json!(-1)),
        ("nbf", json!(now + 10)),
        ("nbf", json!(-1)),
        ("nbf", json!(null)),
        ("cnf", json!(null)),
        ("client_id", json!("")),
        ("jti", json!("")),
        ("scope", json!("read read")),
        ("scope", json!("read  submit")),
        ("scope", json!("read\tsubmit")),
        ("scope", json!("x".repeat(129))),
    ] {
        let mut wrong = good.clone();
        wrong[name] = value;
        assert!(
            verify_token(&sign_with(0, &wrong, &header(0)), &key, &options()).is_err(),
            "claim {name}"
        );
    }
    for missing in ["iss", "sub", "aud", "exp", "iat", "client_id", "jti"] {
        let mut wrong = good.clone();
        wrong.as_object_mut().unwrap().remove(missing);
        assert!(
            verify_token(&sign_with(0, &wrong, &header(0)), &key, &options()).is_err(),
            "missing {missing}"
        );
    }
    let mut no_scope = good;
    no_scope.as_object_mut().unwrap().remove("scope");
    assert!(
        verify_token(&sign_with(0, &no_scope, &header(0)), &key, &options())
            .unwrap()
            .scopes
            .is_empty()
    );
}

#[tokio::test]
async fn rejects_signature_tampering_wrong_key_and_id_token_substitution() {
    let verifier = verifier();
    for typ in ["JWT", "id+jwt", "token-introspection+jwt", ""] {
        let mut wrong = header(0);
        wrong["typ"] = json!(typ);
        assert_eq!(
            verifier
                .authenticate(AgentHttpCredential::new(sign_with(0, &claims(), &wrong)).unwrap())
                .await
                .unwrap_err(),
            AgentHttpAuthenticationError::Unauthenticated
        );
    }
    let mut wrong = header(1);
    wrong["kid"] = json!(keys()[0].kid);
    assert_eq!(
        verifier
            .authenticate(AgentHttpCredential::new(sign_with(1, &claims(), &wrong)).unwrap())
            .await
            .unwrap_err(),
        AgentHttpAuthenticationError::Unauthenticated
    );
    let signed = sign_with(0, &claims(), &header(0));
    let (head, payload, signature) = token_parts(&signed).unwrap();
    let signature = format!(
        "{}{}",
        if signature.starts_with('A') { 'B' } else { 'A' },
        &signature[1..]
    );
    assert_eq!(
        verifier
            .authenticate(
                AgentHttpCredential::new(format!("{head}.{payload}.{signature}")).unwrap()
            )
            .await
            .unwrap_err(),
        AgentHttpAuthenticationError::Unauthenticated
    );
    // An HS256 header is refused before cryptography, even with a real RSA signature.
    let mut hmac = header(0);
    hmac["alg"] = json!("HS256");
    let confused = sign_with(0, &claims(), &hmac);
    assert_eq!(
        verifier
            .authenticate(AgentHttpCredential::new(confused).unwrap())
            .await
            .unwrap_err(),
        AgentHttpAuthenticationError::Unauthenticated
    );
}

#[test]
fn header_and_claim_parsing_rejects_duplicates_and_untrusted_key_headers() {
    let signed = sign_with(0, &claims(), &header(0));
    let (_, payload, signature) = token_parts(&signed).unwrap();
    for header in [
        r#"{"alg":"RS256","alg":"RS256","typ":"at+jwt","kid":"test-key-1"}"#,
        r#"{"alg":"none","typ":"at+jwt","kid":"test-key-1"}"#,
        r#"{"alg":"RS256","typ":"at+jwt","kid":"test-key-1","jku":"https://attacker.test"}"#,
        r#"{"alg":"RS256","typ":"at+jwt","kid":"test-key-1","jwk":{}}"#,
        r#"{"alg":"RS256","typ":"at+jwt","kid":"test-key-1","crit":["b64"],"b64":false}"#,
    ] {
        assert!(
            parse_header(&format!(
                "{}.{}.{}",
                URL_SAFE_NO_PAD.encode(header),
                payload,
                signature
            ))
            .is_err()
        );
    }
    let key = parse_jwks(&jwks(&[0]))
        .unwrap()
        .remove(&keys()[0].kid)
        .unwrap();
    let bad_claims = URL_SAFE_NO_PAD.encode(r#"{"iss":"a","iss":"b"}"#);
    let (head, _, signature) = token_parts(&signed).unwrap();
    assert!(
        verify_token(
            &format!("{head}.{bad_claims}.{signature}"),
            &key,
            &options()
        )
        .is_err()
    );
    assert!(parse_header(&format!("{signed}.extra")).is_err());
    assert!(parse_header(&format!("{signed}=")).is_err());
}

#[test]
fn keysets_reject_private_weak_ambiguous_or_excessive_keys() {
    for (name, value) in [
        ("alg", json!("HS256")),
        ("kty", json!("EC")),
        ("use", json!("enc")),
        ("kid", json!("")),
        ("kid", json!("x".repeat(129))),
        ("key_ops", json!(["sign", "verify"])),
        ("key_ops", json!(["verify", "verify"])),
        ("e", json!("Aw")),
        ("n", json!(URL_SAFE_NO_PAD.encode([0xff; 128]))),
        ("n", json!(URL_SAFE_NO_PAD.encode([0xff; 513]))),
        ("n", json!(URL_SAFE_NO_PAD.encode([0; 256]))),
        ("d", json!(null)),
        ("p", json!("private")),
        ("oth", json!([])),
    ] {
        let mut wrong = keys()[0].jwk.clone();
        wrong[name] = value;
        assert!(
            parse_jwks(&serde_json::to_vec(&json!({"keys":[wrong]})).unwrap()).is_err(),
            "key field {name}"
        );
    }
    assert!(parse_jwks(&jwks(&[0, 0])).is_err());
    assert!(
        parse_jwks(&serde_json::to_vec(&json!({"keys":vec![keys()[0].jwk.clone();17]})).unwrap())
            .is_err()
    );
    assert!(parse_jwks(br#"{"keys":[],"keys":[]}"#).is_err());
    assert!(parse_jwks(&vec![b' '; 16 * 1024 + 1]).is_err());
    assert!(parse_jwks(br#"{"keys":"wrong"}"#).is_err());
    assert!(parse_jwks(br#"{"keys":[],"unknown":true}"#).is_err());
}

#[tokio::test]
async fn key_rotation_is_atomic_and_removed_keys_cannot_authenticate() {
    let verifier = verifier();
    let old_generation = verifier.generation().unwrap();
    assert!(
        verifier
            .replace_jwks(old_generation, b"invalid", Duration::from_secs(300))
            .is_err()
    );
    verifier.authenticate(token(0)).await.unwrap();
    assert_eq!(
        verifier
            .replace_jwks(old_generation, &jwks(&[0, 1]), Duration::from_secs(300))
            .unwrap(),
        2
    );
    verifier.authenticate(token(0)).await.unwrap();
    verifier.authenticate(token(1)).await.unwrap();
    assert!(
        verifier
            .replace_jwks(old_generation, &jwks(&[0]), Duration::from_secs(300))
            .is_err()
    );
    assert_eq!(
        verifier.check_generation(old_generation).unwrap_err(),
        AgentHttpAuthenticationError::Unavailable
    );
    verifier
        .replace_jwks(2, &jwks(&[1]), Duration::from_secs(300))
        .unwrap();
    assert_eq!(
        verifier.authenticate(token(0)).await.unwrap_err(),
        AgentHttpAuthenticationError::Unauthenticated
    );
    verifier.authenticate(token(1)).await.unwrap();
    verifier
        .replace_jwks(3, &jwks(&[]), Duration::from_secs(300))
        .unwrap();
    assert!(verifier.check().await.is_err());
    assert_eq!(
        verifier.authenticate(token(1)).await.unwrap_err(),
        AgentHttpAuthenticationError::Unauthenticated
    );
}

#[tokio::test]
async fn expired_keys_and_capacity_fail_closed_and_recover() {
    let verifier = AgentHttpJwtJwks::new(
        options()
            .with_limits(Duration::from_secs(900), Duration::from_secs(3), 1)
            .unwrap(),
        &jwks(&[0]),
        Duration::from_secs(300),
        policy(),
    )
    .unwrap();
    let permit = verifier.permits.clone().try_acquire_owned().unwrap();
    assert_eq!(
        verifier.authenticate(token(0)).await.unwrap_err(),
        AgentHttpAuthenticationError::Unavailable
    );
    drop(permit);
    verifier.authenticate(token(0)).await.unwrap();
    verifier.keys.write().unwrap().expires = Instant::now();
    assert!(verifier.check().await.is_err());
    assert_eq!(
        verifier.authenticate(token(0)).await.unwrap_err(),
        AgentHttpAuthenticationError::Unavailable
    );
    verifier
        .replace_jwks(1, &jwks(&[0]), Duration::from_secs(300))
        .unwrap();
    verifier.check().await.unwrap();
    verifier.authenticate(token(0)).await.unwrap();
}

#[test]
fn concurrent_key_replacement_has_one_cas_winner() {
    let verifier = Arc::new(verifier());
    let jwks = jwks(&[1]);
    let gate = Arc::new(std::sync::Barrier::new(16));
    let outcomes = std::thread::scope(|scope| {
        let jobs: Vec<_> = (0..16)
            .map(|_| {
                let verifier = &verifier;
                let jwks = &jwks;
                let gate = &gate;
                scope.spawn(move || {
                    gate.wait();
                    verifier.replace_jwks(1, jwks, Duration::from_secs(300))
                })
            })
            .collect();
        jobs.into_iter()
            .map(|job| job.join().unwrap().is_ok())
            .collect::<Vec<_>>()
    });
    assert_eq!(outcomes.into_iter().filter(|won| *won).count(), 1);
    assert_eq!(verifier.generation().unwrap(), 2);
}
