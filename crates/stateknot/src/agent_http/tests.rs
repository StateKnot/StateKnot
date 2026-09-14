// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::time::Duration;

#[test]
fn credential_is_bounded_redacted_and_requires_nonpadding_content() {
    for invalid in ["", "=", "==", "a=b", "hello world", "bearer\nsecret", "中"] {
        assert!(AgentHttpCredential::new(invalid).is_err());
    }
    assert!(AgentHttpCredential::new("a".repeat(8193)).is_err());
    for valid in ["a", "eyJ0.e30.sig", "a/+~_-.=="] {
        let credential = AgentHttpCredential::new(valid).unwrap();
        assert_eq!(credential.expose_secret(), valid);
        assert_eq!(format!("{credential:?}"), "AgentHttpCredential([REDACTED])");
    }
}

#[test]
fn finite_configuration_rejects_wildcards_duplicates_and_unsafe_origins() {
    assert!(AgentHttpOptions::new([]).is_err());
    for invalid in [
        "*",
        "https://api.example",
        "user@api.example",
        " api.example",
        "api.example:bad",
        "api.example:65536",
        "api.example:0",
    ] {
        assert!(AgentHttpOptions::new([invalid.to_owned()]).is_err());
    }
    assert!(AgentHttpOptions::new(["API.example".into(), "api.example".into()]).is_err());
    assert!(AgentHttpOptions::loopback(0).is_err());
    let options = AgentHttpOptions::new(["api.example".into()]).unwrap();
    for origin in [
        "null",
        "*",
        "http://app.example",
        "https://app.example/",
        "https://user@app.example",
        "https://app.example?q=x",
    ] {
        assert!(
            options
                .clone()
                .with_allowed_origins([origin.into()])
                .is_err()
        );
    }
    assert!(
        options
            .clone()
            .with_allowed_origins(["https://app.example".into()])
            .is_ok()
    );
    for deadline in [
        Duration::ZERO,
        Duration::from_millis(9),
        Duration::from_secs(61),
    ] {
        assert!(options.clone().with_deadline(deadline).is_err());
    }
    for (request, response, concurrency) in [
        (0, 1024, 1),
        (1, 1023, 1),
        (1, 1024, 0),
        (2_097_153, 1024, 1),
        (1, 8_388_609, 1),
        (1, 1024, 1025),
    ] {
        assert!(
            options
                .clone()
                .with_limits(request, response, concurrency)
                .is_err()
        );
    }
}

#[test]
fn authority_and_origin_cannot_be_forged_or_duplicated() {
    let options = AgentHttpOptions::new(["api.example".into()])
        .unwrap()
        .with_allowed_origins(["https://app.example".into()])
        .unwrap();
    let uri = "/v1/agent-runs".parse().unwrap();
    let mut headers = HeaderMap::new();
    assert_eq!(
        validate_origin_host(&headers, &uri, &options),
        Err(HttpError::Invalid)
    );
    headers.insert("host", "API.example".parse().unwrap());
    assert!(validate_origin_host(&headers, &uri, &options).is_ok());
    headers.insert("origin", "https://app.example".parse().unwrap());
    assert!(validate_origin_host(&headers, &uri, &options).is_ok());
    assert_eq!(
        validate_origin_host(
            &headers,
            &"http://elsewhere/v1/agent-runs".parse().unwrap(),
            &options
        ),
        Err(HttpError::Invalid)
    );
    headers.append("origin", "https://app.example".parse().unwrap());
    assert_eq!(
        validate_origin_host(&headers, &uri, &options),
        Err(HttpError::Invalid)
    );
    headers.append("authorization", "Bearer one".parse().unwrap());
    headers.append("authorization", "Bearer two".parse().unwrap());
    assert_eq!(single(&headers, "authorization"), Err(HttpError::Invalid));
}

#[test]
fn json_rejects_duplicate_unknown_deep_and_trailing_input() {
    for input in [
        r#"{"submission_key":"a","submission_key":"b"}"#,
        r#"{"submission_key":"valid-key","tenant_id":"victim"}"#,
        r#"{"submission_key":"valid-key"}{}"#,
    ] {
        assert!(decode::<AgentHttpLookup>(input.as_bytes(), 262_144).is_err());
    }
    let deep = format!("{}0{}", "[".repeat(34), "]".repeat(34));
    assert!(decode::<serde_json::Value>(deep.as_bytes(), 262_144).is_err());
    assert!(decode::<serde_json::Value>(br#"{"input":{"x":1,"x":2}}"#, 262_144).is_err());
    assert!(
        decode::<AgentHttpLookup>(br#"{"submission_key":"valid-request-key-0001"}"#, 262_144)
            .is_ok()
    );
    assert!(encode(&json!({"secret":"never-log".repeat(200)}), 1024).is_err());
    assert!(encode(&json!({"ok":true}), 1024).is_ok());
}

#[test]
fn exact_versioned_routes_and_closed_wire_schemas() {
    for path in [
        "/v1/agent-runs?key=secret",
        "/v1/agent-runs/%31",
        "/v1/agent-runs/not-a-uuid",
        "/v1/agent-runs/lookup/",
    ] {
        assert!(route(&Method::POST, &path.parse().unwrap()).is_err());
    }
    assert!(matches!(
        route(&Method::POST, &"/v1/agent-runs".parse().unwrap()),
        Ok(Route::Submit)
    ));
    assert!(matches!(
        route(&Method::POST, &"/v1/agent-runs/lookup".parse().unwrap()),
        Ok(Route::Lookup)
    ));
    let run = RunId::generate();
    assert!(
        matches!(route(&Method::GET,&format!("/v1/agent-runs/{run}").parse().unwrap()), Ok(Route::Read(id)) if id==run)
    );
    for schema in [
        schemars::schema_for!(AgentHttpSubmission),
        schemars::schema_for!(AgentHttpLookup),
        schemars::schema_for!(AgentCancellationIds),
        schemars::schema_for!(AgentHttpRunResponse),
    ] {
        assert_eq!(
            serde_json::to_value(schema).unwrap()["additionalProperties"],
            false
        );
    }
}

#[test]
fn error_mapping_separates_client_conflicts_from_host_integrity_errors() {
    assert_eq!(
        store_error(StoreError::AgentAdmissionRejected),
        HttpError::Conflict
    );
    assert_eq!(
        store_error(StoreError::RunFailureClosing),
        HttpError::Conflict
    );
    assert_eq!(
        store_error(StoreError::GraphDefinitionNotFound),
        HttpError::Internal
    );
    assert_eq!(
        store_error(StoreError::AgentAdmissionStateRejected),
        HttpError::Internal
    );
    assert_eq!(
        service_error(AgentServiceError::Authorization(
            AgentServiceAuthorizationError::InvalidEvidence
        )),
        HttpError::Internal
    );
    assert_eq!(
        service_error(AgentServiceError::AdmissionRequest(
            DurableAgentAdmissionRequestError::InitialStateSchemaMismatch
        )),
        HttpError::Internal
    );
}
