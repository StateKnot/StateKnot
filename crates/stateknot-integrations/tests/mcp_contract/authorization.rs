// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use stateknot_core::{BoxFuture, EventId, ToolErrorPhase};
use stateknot_integrations::{
    McpAuthorization, McpAuthorizationError, McpToolApproval, McpToolAuthorizationRequest,
    McpToolAuthorizer, mcp_tool_descriptor_digest,
};
use tokio::sync::Notify;

use super::*;

struct Policy {
    descriptor: ToolDescriptor,
    digest: Digest,
    mode: AtomicUsize,
    calls: AtomicUsize,
    entered: Notify,
    release: Notify,
}

impl McpToolAuthorizer for Policy {
    fn resolve_startup(
        &self,
        descriptor: &ToolDescriptor,
    ) -> BoxFuture<'_, Result<McpAuthorization, McpAuthorizationError>> {
        assert_eq!(descriptor, &self.descriptor);
        Box::pin(async {
            Ok(McpAuthorization::Bearer(
                ApiKey::new("discovery-only").unwrap(),
            ))
        })
    }

    fn authorize_call<'a>(
        &'a self,
        request: McpToolAuthorizationRequest<'a>,
    ) -> BoxFuture<'a, Result<McpAuthorization, McpAuthorizationError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.descriptor(), &self.descriptor);
            assert_eq!(request.tool_digest(), self.digest);
            assert_eq!(request.remote_name(), "echo");
            assert!(!request.endpoint().is_https());
            assert!(request.context().origin_event_id().is_some());
            assert!(!format!("{request:?}").contains("approved-private-input"));
            if request.context().tenant_id().as_str() != "tenant-mcp-contract"
                || request.input().value().as_value()["question"] != "approved-private-input"
            {
                return Err(McpAuthorizationError::PermissionDenied);
            }
            let mode = self.mode.load(Ordering::SeqCst);
            self.entered.notify_one();
            if mode == 2 {
                self.release.notified().await;
            }
            match mode {
                1 => Err(McpAuthorizationError::PermissionDenied),
                3 => Err(McpAuthorizationError::Unavailable),
                _ => Ok(McpAuthorization::Bearer(ApiKey::new(SECRET).unwrap())),
            }
        })
    }
}

fn input(reference: &SchemaReference, value: Value) -> ToolInput {
    ToolInput::new(
        reference.clone(),
        BoundedJson::try_from_value(value).unwrap(),
    )
    .unwrap()
}

async fn fixture() -> (McpRemoteTool, Arc<Policy>, TestMcpServer) {
    fixture_with(CallBehavior::StructuredSuccess).await
}

async fn fixture_with(behavior: CallBehavior) -> (McpRemoteTool, Arc<Policy>, TestMcpServer) {
    let (registry, i, input, o, output) = schemas();
    let raw = remote_tool(&input, &output);
    let policy = Arc::new(Policy {
        descriptor: write_descriptor(&i, &o),
        digest: mcp_tool_descriptor_digest(&raw).unwrap(),
        mode: AtomicUsize::new(0),
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let server = TestMcpServer::start_tool(raw, behavior, 8).await;
    let adapter = McpRemoteTool::connect_authorized(
        policy.descriptor.clone(),
        "echo",
        server.endpoint.clone(),
        McpServerIdentity::new("stateknot-test-mcp", "1.0.0").unwrap(),
        registry,
        McpToolApproval::new(policy.digest, policy.clone()),
        McpHttpOptions::default(),
    )
    .await
    .unwrap();
    (adapter, policy, server)
}

fn durable_context(policy: &Policy) -> ToolContext {
    context(&policy.descriptor).with_durable_origin_event(EventId::generate())
}

async fn assert_only_discovery(server: &mut TestMcpServer) {
    for method in ["server/discover", "tools/list"] {
        let raw = tokio::time::timeout(Duration::from_secs(2), server.requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(request_body(&raw)).unwrap()["method"],
            method
        );
        assert!(String::from_utf8_lossy(&raw).contains("Bearer discovery-only"));
    }
    assert!(server.requests.try_recv().is_err());
}

#[tokio::test]
async fn approved_calls_validate_origin_input_and_policy_before_dispatch() {
    let (adapter, policy, mut server) = fixture().await;
    let good = input(
        policy.descriptor.input_schema(),
        json!({"question":"approved-private-input"}),
    );
    let error = adapter
        .call(context(&policy.descriptor), good.clone())
        .await
        .unwrap_err();
    assert_eq!(
        error.failure().category(),
        FailureCategory::PermissionDenied
    );
    assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
    let invalid = input(policy.descriptor.input_schema(), json!({"question":17}));
    assert_eq!(
        adapter
            .call(durable_context(&policy), invalid)
            .await
            .unwrap_err()
            .failure()
            .category(),
        FailureCategory::InvalidInput
    );
    assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
    let denied = input(
        policy.descriptor.input_schema(),
        json!({"question":"another-resource"}),
    );
    let error = adapter
        .call(durable_context(&policy), denied)
        .await
        .unwrap_err();
    assert_eq!(
        error.failure().category(),
        FailureCategory::PermissionDenied
    );
    assert_eq!(error.external_effect(), ToolExternalEffect::NotStarted);
    assert_eq!(error.phase(), ToolErrorPhase::Preparation);
    assert_eq!(error.failure().retry_advice(), RetryAdvice::Never);
    policy.mode.store(3, Ordering::SeqCst);
    let error = adapter
        .call(durable_context(&policy), good.clone())
        .await
        .unwrap_err();
    assert_eq!(
        error.failure().category(),
        FailureCategory::DependencyUnavailable
    );
    assert_eq!(error.external_effect(), ToolExternalEffect::NotStarted);
    assert_only_discovery(&mut server).await;
    policy.mode.store(0, Ordering::SeqCst);
    adapter.call(durable_context(&policy), good).await.unwrap();
    let raw = server.requests.recv().await.unwrap();
    assert!(String::from_utf8_lossy(&raw).contains(&format!("Bearer {SECRET}")));
    let call: Value = serde_json::from_slice(request_body(&raw)).unwrap();
    assert_eq!(
        call["params"],
        json!({"name":"echo", "arguments":{"question":"approved-private-input"}, "_meta": {
            "io.modelcontextprotocol/clientCapabilities": {},
            "io.modelcontextprotocol/clientInfo": {"name":"stateknot", "version":env!("CARGO_PKG_VERSION")},
            "io.modelcontextprotocol/protocolVersion":"2026-07-28", "progressToken":1
        }})
    );
    assert!(!String::from_utf8_lossy(request_body(&raw)).contains(ATTEMPT_ID));
    assert!(!String::from_utf8_lossy(request_body(&raw)).contains(RUN_ID));
    assert!(!format!("{adapter:?}").contains(SECRET));
    assert_eq!(policy.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn queued_call_observes_revocation_and_cannot_reuse_previous_credential() {
    let (adapter, policy, mut server) = fixture().await;
    let adapter = Arc::new(adapter);
    policy.mode.store(2, Ordering::SeqCst);
    let first = {
        let adapter = adapter.clone();
        let policy = policy.clone();
        tokio::spawn(async move {
            adapter
                .call(
                    durable_context(&policy),
                    input(
                        policy.descriptor.input_schema(),
                        json!({"question":"approved-private-input"}),
                    ),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), policy.entered.notified())
        .await
        .unwrap();
    let second = {
        let adapter = adapter.clone();
        let policy = policy.clone();
        tokio::spawn(async move {
            adapter
                .call(
                    durable_context(&policy),
                    input(
                        policy.descriptor.input_schema(),
                        json!({"question":"approved-private-input"}),
                    ),
                )
                .await
        })
    };
    policy.mode.store(1, Ordering::SeqCst);
    policy.release.notify_one();
    first.await.unwrap().unwrap();
    let error = second.await.unwrap().unwrap_err();
    assert_eq!(
        error.failure().category(),
        FailureCategory::PermissionDenied
    );
    assert_eq!(error.external_effect(), ToolExternalEffect::NotStarted);
    for _ in 0..3 {
        server.requests.recv().await.unwrap();
    }
    assert!(server.requests.try_recv().is_err());
    assert_eq!(policy.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn approved_pin_covers_unknown_extensions_and_refuses_unsafe_execution() {
    for case in 0..4 {
        let (registry, i, input, o, output) = schemas();
        let mut raw = remote_tool(&input, &output);
        let original = mcp_tool_descriptor_digest(&raw).unwrap();
        match case {
            0 => raw["description"] = json!("Changed deployment"),
            1 => raw["x-private-extension"] = json!({"revision":2}),
            2 => raw["inputSchema"]["properties"]["question"]["x-mcp-header"] = json!("X-Target"),
            _ => raw["execution"] = json!({"taskSupport":"optional"}),
        }
        let digest = if case < 2 {
            original
        } else {
            mcp_tool_descriptor_digest(&raw).unwrap()
        };
        let policy = Arc::new(Policy {
            descriptor: write_descriptor(&i, &o),
            digest,
            mode: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Notify::new(),
        });
        let server = TestMcpServer::start_tool(raw, CallBehavior::StructuredSuccess, 2).await;
        let error = McpRemoteTool::connect_authorized(
            policy.descriptor.clone(),
            "echo",
            server.endpoint.clone(),
            McpServerIdentity::new("stateknot-test-mcp", "1.0.0").unwrap(),
            registry,
            McpToolApproval::new(digest, policy.clone()),
            McpHttpOptions::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(error, McpRemoteToolBuildError::ToolCatalogProtocol);
        assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn descriptor_digest_is_bounded_and_includes_all_fields() {
    assert!(mcp_tool_descriptor_digest(&json!(null)).is_err());
    assert!(mcp_tool_descriptor_digest(&json!({"x":"x".repeat(300_000)})).is_err());
    assert_eq!(
        mcp_tool_descriptor_digest(&json!({"b":2,"a":1})).unwrap(),
        mcp_tool_descriptor_digest(&json!({"a":1,"b":2})).unwrap()
    );
    assert_ne!(
        mcp_tool_descriptor_digest(&json!({"a":1})).unwrap(),
        mcp_tool_descriptor_digest(&json!({"a":1,"x-private":null})).unwrap()
    );
}

#[tokio::test]
async fn cross_tenant_and_policy_timeout_fail_before_dispatch() {
    let (adapter, policy, mut server) = fixture().await;
    let good = input(
        policy.descriptor.input_schema(),
        json!({"question":"approved-private-input"}),
    );
    let crossed = scoped_context(
        &policy.descriptor,
        "other-tenant",
        30_000,
        CancellationSignal::never(),
    )
    .with_durable_origin_event(EventId::generate());
    let error = adapter.call(crossed, good.clone()).await.unwrap_err();
    assert_eq!(
        error.failure().category(),
        FailureCategory::PermissionDenied
    );
    assert_eq!(error.external_effect(), ToolExternalEffect::NotStarted);
    policy.mode.store(2, Ordering::SeqCst);
    let deadline = scoped_context(
        &policy.descriptor,
        "tenant-mcp-contract",
        30,
        CancellationSignal::never(),
    )
    .with_durable_origin_event(EventId::generate());
    let error = adapter.call(deadline, good.clone()).await.unwrap_err();
    assert_eq!(error.external_effect(), ToolExternalEffect::NotStarted);
    assert_only_discovery(&mut server).await;
    // A policy-only timeout has not queued transport work: the binding is safe to reuse.
    policy.mode.store(0, Ordering::SeqCst);
    adapter.call(durable_context(&policy), good).await.unwrap();
    assert_eq!(policy.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn dropped_dispatched_future_retires_binding_before_next_credential() {
    let (adapter, policy, mut server) = fixture_with(CallBehavior::HoldWithoutResponse).await;
    let adapter = Arc::new(adapter);
    assert_only_discovery(&mut server).await;
    let running = {
        let adapter = adapter.clone();
        let policy = policy.clone();
        tokio::spawn(async move {
            adapter
                .call(
                    durable_context(&policy),
                    input(
                        policy.descriptor.input_schema(),
                        json!({"question":"approved-private-input"}),
                    ),
                )
                .await
        })
    };
    let raw = tokio::time::timeout(Duration::from_secs(2), server.requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&raw).contains(&format!("Bearer {SECRET}")));
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    let error = adapter
        .call(
            durable_context(&policy),
            input(
                policy.descriptor.input_schema(),
                json!({"question":"approved-private-input"}),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.failure().code().as_str(),
        "authorization.binding_retired"
    );
    assert_eq!(error.external_effect(), ToolExternalEffect::NotStarted);
    assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
    assert!(server.requests.try_recv().is_err());
}

#[tokio::test]
async fn dispatched_timeout_is_unknown_and_retires_binding() {
    let (adapter, policy, mut server) = fixture_with(CallBehavior::HoldWithoutResponse).await;
    assert_only_discovery(&mut server).await;
    let ctx = scoped_context(
        &policy.descriptor,
        "tenant-mcp-contract",
        100,
        CancellationSignal::never(),
    )
    .with_durable_origin_event(EventId::generate());
    let error = adapter
        .call(
            ctx,
            input(
                policy.descriptor.input_schema(),
                json!({"question":"approved-private-input"}),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.external_effect(), ToolExternalEffect::Unknown);
    assert_eq!(error.failure().retry_advice(), RetryAdvice::ReconcileFirst);
    let error = adapter
        .call(
            durable_context(&policy),
            input(
                policy.descriptor.input_schema(),
                json!({"question":"approved-private-input"}),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.failure().code().as_str(),
        "authorization.binding_retired"
    );
    assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
}
