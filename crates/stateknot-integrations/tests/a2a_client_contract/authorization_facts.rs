// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::StreamExt;
use serde_json::{Value, json};
use stateknot_core::{
    BoundedJson, BoxFuture, DurationMillis, ErasedTool, FailureCategory, ToolDescriptor,
    ToolExternalEffect, ToolInput, ToolReconciliationObservation,
};
use stateknot_integrations::{
    A2aBearerTokenProvider, A2aBinding, A2aClient, A2aClientAuthorizationError,
    A2aClientAuthorizationRequest, A2aClientError, A2aClientOperation, A2aClientOptions,
    A2aClientSecurity, A2aMessage, A2aMessageRole, A2aPart, A2aRemoteAgent, A2aRemoteAgentDelivery,
    A2aRemoteAgentRecovery, A2aSendMessageRequest, A2aSendMessageResponse, A2aStreamEvent, ApiKey,
    ProviderHttpOptions,
};
use tokio::sync::Notify;

use super::{
    ATTEMPT_ID, DirectCapturingTokenProvider, INVOCATION_ID, OperationBehavior, ROUTING_TENANT,
    RUN_ID, TOKEN, TestA2aServer, context, discover, find_bytes, reconciliation_context,
    reconciliation_descriptor, request_body, request_method, request_target, schemas,
};

const ALLOWED_PAYLOAD: &str = "allowed-public-message";
const BLOCKED_PAYLOAD: &str = "blocked-private-value";

#[derive(Default)]
pub(super) struct RequestPolicy {
    observations: Mutex<Vec<A2aClientAuthorizationRequest>>,
    revoked: AtomicBool,
    allowed_url: Option<String>,
}

impl RequestPolicy {
    fn observations(&self) -> Vec<A2aClientAuthorizationRequest> {
        self.observations.lock().unwrap().clone()
    }

    pub(super) fn assert_wire_requests(&self, calls: &[Vec<u8>]) {
        let observations = self.observations();
        assert_eq!(observations.len(), calls.len());
        for (observation, wire) in observations.iter().zip(calls) {
            assert_wire_snapshot(observation, wire);
        }
    }
}

impl A2aBearerTokenProvider for RequestPolicy {
    fn resolve(
        &self,
        request: &A2aClientAuthorizationRequest,
    ) -> BoxFuture<'_, Result<ApiKey, A2aClientAuthorizationError>> {
        assert_redacted(request);
        let denied = self.revoked.load(Ordering::SeqCst)
            || self
                .allowed_url
                .as_deref()
                .is_some_and(|allowed| request.request_url() != allowed)
            || request
                .request_body()
                .is_some_and(|body| find_bytes(body, BLOCKED_PAYLOAD.as_bytes()).is_some());
        self.observations.lock().unwrap().push(request.clone());
        Box::pin(async move {
            if denied {
                Err(A2aClientAuthorizationError::PermissionDenied)
            } else {
                Ok(ApiKey::new(TOKEN).unwrap())
            }
        })
    }
}

fn assert_redacted(request: &A2aClientAuthorizationRequest) {
    let debug = format!("{request:?}");
    assert!(!debug.contains(request.request_url()));
    for secret in [
        TOKEN,
        ALLOWED_PAYLOAD,
        BLOCKED_PAYLOAD,
        "exercise every operation",
        "hooks.example.test",
        "question",
    ] {
        assert!(
            !debug.contains(secret),
            "authorization Debug exposed {secret}"
        );
    }
    if let Some(body) = request.request_body() {
        assert!(find_bytes(body, TOKEN.as_bytes()).is_none());
    }
}

fn wire_header<'a>(request: &'a [u8], name: &str) -> Option<&'a str> {
    let end = find_bytes(request, b"\r\n\r\n").unwrap();
    std::str::from_utf8(&request[..end])
        .unwrap()
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim())
        })
}

fn assert_wire_snapshot(observation: &A2aClientAuthorizationRequest, wire: &[u8]) {
    assert_eq!(observation.request_method(), request_method(wire));
    let actual_url = format!(
        "http://{}{}",
        wire_header(wire, "host").unwrap(),
        request_target(wire)
    );
    assert_eq!(observation.request_url(), actual_url);
    let body = request_body(wire);
    if body.is_empty() {
        assert!(observation.request_body().is_none());
    } else {
        assert_eq!(observation.request_body(), Some(body));
    }
    assert_eq!(
        wire_header(wire, "authorization").unwrap(),
        format!("Bearer {TOKEN}")
    );
    assert!(find_bytes(body, TOKEN.as_bytes()).is_none());
    assert_redacted(observation);
}

fn message(payload: &str) -> A2aSendMessageRequest {
    A2aSendMessageRequest::new(
        A2aMessage::new(
            "policy-request",
            A2aMessageRole::User,
            vec![A2aPart::data(json!({"question": payload})).unwrap()],
        )
        .unwrap(),
    )
}

async fn send(client: &A2aClient, streaming: bool, payload: &str) -> Result<(), A2aClientError> {
    if streaming {
        let mut events = client.send_streaming_message(message(payload)).await?;
        assert!(matches!(
            events.next().await.unwrap()?,
            A2aStreamEvent::Message(_)
        ));
        assert!(events.next().await.is_none());
    } else {
        assert!(matches!(
            client.send_message(message(payload)).await?,
            A2aSendMessageResponse::Message(_)
        ));
    }
    Ok(())
}

async fn assert_no_send(server: &mut TestA2aServer) {
    assert!(
        tokio::time::timeout(Duration::from_millis(150), server.requests.recv())
            .await
            .is_err(),
        "an unauthorized business request reached the real server"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payload_policy_allows_public_data_and_refuses_private_data_before_send() {
    for binding in [A2aBinding::HttpJson, A2aBinding::JsonRpc] {
        for streaming in [false, true] {
            let mut server = TestA2aServer::start(
                binding,
                Some(ROUTING_TENANT),
                true,
                true,
                OperationBehavior::OperationMatrix,
            )
            .await;
            let policy = Arc::new(RequestPolicy::default());
            let client = discover(
                &server,
                A2aClientSecurity::bearer("oidc", policy.clone()).unwrap(),
                A2aClientOptions::default(),
            )
            .await;
            let _discovery = server.next_request().await;
            send(&client, streaming, ALLOWED_PAYLOAD).await.unwrap();
            let allowed = server.next_request().await;
            assert!(find_bytes(request_body(&allowed), ALLOWED_PAYLOAD.as_bytes()).is_some());
            policy.assert_wire_requests(&[allowed]);

            let error = send(&client, streaming, BLOCKED_PAYLOAD).await.unwrap_err();
            assert!(!error.was_dispatched());
            assert_eq!(
                error.authorization_error(),
                Some(A2aClientAuthorizationError::PermissionDenied)
            );
            let observations = policy.observations();
            assert_eq!(observations.len(), 2);
            assert!(
                find_bytes(
                    observations[1].request_body().unwrap(),
                    BLOCKED_PAYLOAD.as_bytes()
                )
                .is_some()
            );
            assert_no_send(&mut server).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_provider_refuses_a_destination_outside_its_preconfigured_url() {
    for binding in [A2aBinding::HttpJson, A2aBinding::JsonRpc] {
        let mut allowed_server =
            TestA2aServer::start(binding, None, true, false, OperationBehavior::Success).await;
        let mut other_server =
            TestA2aServer::start(binding, None, true, false, OperationBehavior::Success).await;
        // Both URLs come from fixture construction, before any policy callback.
        let send_url = |server: &TestA2aServer| match binding {
            A2aBinding::HttpJson => format!("{}/message:send", server.configured_interface_url),
            A2aBinding::JsonRpc => server.configured_interface_url.clone(),
            _ => unreachable!("the fixture covers the two supported bindings"),
        };
        let allowed_url = send_url(&allowed_server);
        let other_url = send_url(&other_server);
        assert_ne!(allowed_url, other_url);
        let policy = Arc::new(RequestPolicy {
            allowed_url: Some(allowed_url.clone()),
            ..RequestPolicy::default()
        });
        let allowed_client = discover(
            &allowed_server,
            A2aClientSecurity::bearer("oidc", policy.clone()).unwrap(),
            A2aClientOptions::default(),
        )
        .await;
        let other_client = discover(
            &other_server,
            A2aClientSecurity::bearer("oidc", policy.clone()).unwrap(),
            A2aClientOptions::default(),
        )
        .await;
        let _allowed_discovery = allowed_server.next_request().await;
        let _other_discovery = other_server.next_request().await;
        send(&allowed_client, false, ALLOWED_PAYLOAD).await.unwrap();
        let allowed_wire = allowed_server.next_request().await;
        policy.assert_wire_requests(&[allowed_wire]);

        let error = send(&other_client, false, ALLOWED_PAYLOAD)
            .await
            .unwrap_err();
        assert!(!error.was_dispatched());
        assert_eq!(
            error.authorization_error(),
            Some(A2aClientAuthorizationError::PermissionDenied)
        );
        let observations = policy.observations();
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0].request_url(), allowed_url);
        assert_eq!(observations[1].request_url(), other_url);
        assert_eq!(
            observations[0].request_body(),
            observations[1].request_body()
        );
        assert_no_send(&mut other_server).await;
    }
}

#[derive(Default)]
struct PendingPolicy {
    polled: Notify,
    dropped: AtomicBool,
    observations: Mutex<Vec<A2aClientAuthorizationRequest>>,
}

struct RecordDrop<'a>(&'a AtomicBool);

impl Drop for RecordDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl A2aBearerTokenProvider for PendingPolicy {
    fn resolve(
        &self,
        request: &A2aClientAuthorizationRequest,
    ) -> BoxFuture<'_, Result<ApiKey, A2aClientAuthorizationError>> {
        assert_redacted(request);
        self.observations.lock().unwrap().push(request.clone());
        Box::pin(async move {
            let _drop = RecordDrop(&self.dropped);
            self.polled.notify_one();
            std::future::pending().await
        })
    }
}

async fn pending_authorization_is_not_sent(deadline: bool) {
    for binding in [A2aBinding::HttpJson, A2aBinding::JsonRpc] {
        for streaming in [false, true] {
            let mut server = TestA2aServer::start(
                binding,
                None,
                true,
                true,
                OperationBehavior::OperationMatrix,
            )
            .await;
            let policy = Arc::new(PendingPolicy::default());
            let options = if deadline {
                A2aClientOptions::new(
                    ProviderHttpOptions::default(),
                    Duration::from_secs(15),
                    Duration::from_millis(250),
                    Duration::from_secs(60),
                    4096,
                )
                .unwrap()
            } else {
                A2aClientOptions::default()
            };
            let client = discover(
                &server,
                A2aClientSecurity::bearer("oidc", policy.clone()).unwrap(),
                options,
            )
            .await;
            let _discovery = server.next_request().await;
            let call = tokio::spawn(async move { send(&client, streaming, ALLOWED_PAYLOAD).await });
            tokio::time::timeout(Duration::from_secs(2), policy.polled.notified())
                .await
                .expect("the authorization future must actually be polled");
            if deadline {
                let error = tokio::time::timeout(Duration::from_secs(2), call)
                    .await
                    .expect("the configured authorization deadline must finish the call")
                    .unwrap()
                    .unwrap_err();
                assert!(!error.was_dispatched());
                assert_eq!(
                    error.authorization_error(),
                    Some(A2aClientAuthorizationError::Unavailable)
                );
            } else {
                call.abort();
                assert!(call.await.unwrap_err().is_cancelled());
            }
            assert!(policy.dropped.load(Ordering::SeqCst));
            {
                let observations = policy.observations.lock().unwrap();
                assert_eq!(observations.len(), 1);
                assert!(observations[0].request_body().is_some());
            }
            assert_no_send(&mut server).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authorization_deadline_drops_the_polled_callback_without_sending() {
    pending_authorization_is_not_sent(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_a_call_drops_the_polled_callback_without_sending() {
    pending_authorization_is_not_sent(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anonymous_and_metadata_only_providers_keep_their_existing_wire_contract() {
    for binding in [A2aBinding::HttpJson, A2aBinding::JsonRpc] {
        for secured in [false, true] {
            let mut server =
                TestA2aServer::start(binding, None, secured, false, OperationBehavior::Success)
                    .await;
            let legacy = Arc::new(DirectCapturingTokenProvider::default());
            let security = if secured {
                A2aClientSecurity::bearer("oidc", legacy.clone()).unwrap()
            } else {
                A2aClientSecurity::Anonymous
            };
            let client = discover(&server, security, A2aClientOptions::default()).await;
            send(&client, false, ALLOWED_PAYLOAD).await.unwrap();
            let _discovery = server.next_request().await;
            let wire = server.next_request().await;
            let observations = legacy.observations.lock().unwrap();
            if secured {
                assert_eq!(
                    observations.as_slice(),
                    &[(A2aClientOperation::SendMessage, None)]
                );
                assert_eq!(
                    wire_header(&wire, "authorization").unwrap(),
                    format!("Bearer {TOKEN}")
                );
            } else {
                assert!(observations.is_empty());
                assert!(wire_header(&wire, "authorization").is_none());
            }
            assert!(find_bytes(request_body(&wire), TOKEN.as_bytes()).is_none());
        }
    }
}

struct ReplayFixture {
    server: TestA2aServer,
    policy: Arc<RequestPolicy>,
    adapter: A2aRemoteAgent,
    descriptor: ToolDescriptor,
    input: ToolInput,
}

async fn replay_fixture() -> ReplayFixture {
    // This existing lost-ACK server implements HTTP+JSON, not JSON-RPC replay.
    let server = TestA2aServer::start(
        A2aBinding::HttpJson,
        Some(ROUTING_TENANT),
        true,
        false,
        OperationBehavior::DeduplicatedReplayRecovery,
    )
    .await;
    let policy = Arc::new(RequestPolicy::default());
    let client = discover(
        &server,
        A2aClientSecurity::bearer("oidc", policy.clone()).unwrap(),
        A2aClientOptions::default(),
    )
    .await;
    let (registry, input_schema, output_schema) = schemas();
    let mut value = serde_json::to_value(reconciliation_descriptor(
        &input_schema,
        &output_schema,
        A2aRemoteAgentDelivery::MessageIdDeduplicated,
    ))
    .unwrap();
    value["resources"]["credentials"] = Value::Bool(true);
    let descriptor = serde_json::from_value::<ToolDescriptor>(value).unwrap();
    let recovery = A2aRemoteAgentRecovery::operator_attested_message_id_replay(
        DurationMillis::new(250).unwrap(),
    )
    .unwrap();
    let adapter = A2aRemoteAgent::bind_with_recovery(
        descriptor.clone(),
        client,
        "answer",
        A2aRemoteAgentDelivery::MessageIdDeduplicated,
        recovery,
        registry,
    )
    .unwrap();
    let input = ToolInput::new(
        input_schema,
        BoundedJson::try_from_value(json!({"question": ALLOWED_PAYLOAD})).unwrap(),
    )
    .unwrap();
    ReplayFixture {
        server,
        policy,
        adapter,
        descriptor,
        input,
    }
}

fn assert_original_replay_identity(observations: &[A2aClientAuthorizationRequest]) {
    assert_eq!(observations.len(), 2);
    let original = &observations[0];
    let replay = &observations[1];
    assert_eq!(original.operation(), A2aClientOperation::SendMessage);
    assert_eq!(replay.operation(), original.operation());
    assert_eq!(replay.request_method(), original.request_method());
    assert_eq!(replay.request_url(), original.request_url());
    assert_eq!(replay.request_body(), original.request_body());
    assert_eq!(replay.card_digest(), original.card_digest());
    assert_eq!(replay.binding(), original.binding());
    assert_eq!(original.remote_tenant(), Some(ROUTING_TENANT));
    assert_eq!(replay.remote_tenant(), original.remote_tenant());
    let original = original.attempt().unwrap();
    let replay = replay.attempt().unwrap();
    assert_eq!(original.tenant_id().as_str(), "tenant-a2a-contract");
    assert_eq!(original.run_id().to_string(), RUN_ID);
    assert_eq!(original.invocation_id().to_string(), INVOCATION_ID);
    assert_eq!(original.attempt_id().to_string(), ATTEMPT_ID);
    assert_eq!(replay.tenant_id(), original.tenant_id());
    assert_eq!(replay.run_id(), original.run_id());
    assert_eq!(replay.invocation_id(), original.invocation_id());
    assert_eq!(replay.attempt_id(), original.attempt_id());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attested_http_replay_authorizes_the_same_actual_request_and_original_attempt() {
    let mut fixture = replay_fixture().await;
    let error = fixture
        .adapter
        .call(context(&fixture.descriptor), fixture.input.clone())
        .await
        .unwrap_err();
    assert_eq!(error.external_effect(), ToolExternalEffect::Unknown);
    let result = fixture
        .adapter
        .reconcile(reconciliation_context(&fixture.descriptor), fixture.input)
        .await
        .unwrap();
    assert!(matches!(result, ToolReconciliationObservation::Result(_)));
    let _discovery = fixture.server.next_request().await;
    let original = fixture.server.next_request().await;
    let replay = fixture.server.next_request().await;
    fixture.policy.assert_wire_requests(&[original, replay]);
    assert_original_replay_identity(&fixture.policy.observations());
    assert_no_send(&mut fixture.server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn current_policy_revocation_prevents_an_attested_http_replay() {
    let mut fixture = replay_fixture().await;
    let error = fixture
        .adapter
        .call(context(&fixture.descriptor), fixture.input.clone())
        .await
        .unwrap_err();
    assert_eq!(error.external_effect(), ToolExternalEffect::Unknown);
    let _discovery = fixture.server.next_request().await;
    let original = fixture.server.next_request().await;
    fixture.policy.assert_wire_requests(&[original]);

    fixture.policy.revoked.store(true, Ordering::SeqCst);
    let error = fixture
        .adapter
        .reconcile(reconciliation_context(&fixture.descriptor), fixture.input)
        .await
        .unwrap_err();
    assert_eq!(
        error.failure().category(),
        FailureCategory::PermissionDenied
    );
    assert_eq!(
        error.failure().code().as_str(),
        "authorization.permission_denied"
    );
    assert_original_replay_identity(&fixture.policy.observations());
    assert_no_send(&mut fixture.server).await;
}
