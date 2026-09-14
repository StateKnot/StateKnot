// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

const ERROR_TOOL: &str = "stateknot_reconcile_tool_error_v1";

fn request(
    original: &McpReconciliationRequest,
    effect: McpKnownToolEffect,
) -> McpErrorReconciliationRequest {
    McpErrorReconciliationRequest {
        event_id: original.event_id,
        run_id: original.run_id,
        invocation_id: original.invocation_id,
        attempt_id: original.attempt_id,
        expected_revision: original.expected_revision,
        expected_digest: original.expected_digest,
        failure_id: FailureId::generate(),
        failure_category: FailureCategory::Conflict,
        failure_code: FailureCode::new("fixture.authoritative_failure").unwrap(),
        failure_message: FailureMessage::new("The provider confirmed this operation failed.")
            .unwrap(),
        external_effect: effect,
    }
}

fn wire_request() -> McpErrorReconciliationRequest {
    request(
        &McpReconciliationRequest {
            event_id: EventId::generate(),
            run_id: RunId::generate(),
            invocation_id: InvocationId::generate(),
            attempt_id: AttemptId::generate(),
            expected_revision: ToolInvocationRevision::new(2).unwrap(),
            expected_digest: Digest::sha256(b"unknown"),
            output: BoundedJson::try_from_value(json!({})).unwrap(),
        },
        McpKnownToolEffect::NotApplied,
    )
}

async fn submit(url: &str, token: &str, arguments: Value, lose: bool) -> Value {
    call_tool(url, token, ERROR_TOOL, arguments, lose).await
}

#[test]
fn known_error_wire_rejects_foreign_authority_uncertainty_and_private_debug() {
    let mut request = wire_request();
    request.failure_message = FailureMessage::new("fixture-message-not-for-debug").unwrap();
    assert!(!format!("{request:?}").contains("fixture-message-not-for-debug"));
    assert!(!format!("{request:?}").contains("authoritative_failure"));
    let value = json!(request);
    assert_eq!(
        json!(serde_json::from_value::<McpErrorReconciliationRequest>(value.clone()).unwrap()),
        value
    );
    for field in [
        "tenant_id",
        "principal",
        "fence",
        "output",
        "artifacts",
        "retry_advice",
        "phase",
        "origin",
        "provenance",
        "details",
        "usage",
        "recovery_handle",
    ] {
        let mut bad = value.clone();
        bad[field] = json!(null);
        assert!(
            serde_json::from_value::<McpErrorReconciliationRequest>(bad).is_err(),
            "{field}"
        );
    }
    for effect in ["unknown", "not_started", "not_applicable", "partial"] {
        let mut bad = value.clone();
        bad["external_effect"] = json!(effect);
        assert!(
            serde_json::from_value::<McpErrorReconciliationRequest>(bad).is_err(),
            "{effect}"
        );
    }
    for (field, invalid) in [
        ("failure_message", json!("密".repeat(400))),
        ("failure_message", json!("private\ntrace")),
        ("failure_code", json!("Not_A_Code")),
        ("failure_id", json!("not-a-uuid")),
        ("expected_revision", json!("9223372036854775808")),
        ("expected_revision", json!("02")),
        ("expected_revision", json!(2)),
        ("expected_digest", json!("sha256:bad")),
    ] {
        let mut bad = value.clone();
        bad[field] = invalid;
        assert!(
            serde_json::from_value::<McpErrorReconciliationRequest>(bad).is_err(),
            "{field}"
        );
    }
}

#[test]
fn known_error_definition_is_separate_closed_and_cannot_grant_retry() {
    let definition = McpToolErrorReconciler::definition().unwrap();
    assert_eq!(definition.name(), ERROR_TOOL);
    assert_eq!(
        definition.required_scopes().collect::<Vec<_>>(),
        ["stateknot:reconcile-error"]
    );
    assert_eq!(definition.input_schema()["additionalProperties"], false);
    assert_eq!(
        definition.input_schema()["required"]
            .as_array()
            .unwrap()
            .len(),
        11
    );
    assert_eq!(
        definition.input_schema()["properties"]["external_effect"]["enum"],
        json!(["not_applied", "applied"])
    );
    assert!(
        !definition.input_schema()["properties"]["failure_category"]["enum"]
            .as_array()
            .unwrap()
            .contains(&json!("ambiguous_external_outcome"))
    );
    assert!(
        definition.input_schema()["properties"]
            .get("retry_advice")
            .is_none()
    );
    assert_eq!(
        definition.output_schema(),
        McpToolReconciler::definition().unwrap().output_schema()
    );
    assert_ne!(
        reference(&McpToolErrorReconciler::audit_schema()),
        reference(&McpToolReconciler::audit_schema())
    );
}

#[test]
fn successful_v1_schemas_remain_byte_compatible() {
    let definition = McpToolReconciler::definition().unwrap();
    assert_eq!(definition.name(), "stateknot_reconcile_tool_result_v1");
    assert_eq!(
        definition.required_scopes().collect::<Vec<_>>(),
        ["stateknot:reconcile-result"]
    );
    // RFC 8785 digests frozen before adding the independent error profile.
    for (schema, expected) in [
        (
            McpToolReconciler::audit_schema(),
            "sha256:98f381e0964f983b2bd8ee90595f7816ab6f01f45ba4f78c5e8bf8fb686b5211",
        ),
        (
            definition.input_schema().clone(),
            "sha256:27eff7e2e98312c499c6c79e5297d793444294c37ebbbce5bea57fbee8f3d2ae",
        ),
        (
            definition.output_schema().unwrap().clone(),
            "sha256:a3f7c73c76a4a8140d391e5ffa1154256168d78e4562b692555522466ba5a0f2",
        ),
    ] {
        assert_eq!(
            Digest::sha256(serde_json_canonicalizer::to_vec(&schema).unwrap()).to_string(),
            expected
        );
    }
}

#[tokio::test]
async fn error_startup_rejects_missing_and_drifted_audit_schemas() {
    let Some(store) = store().await else { return };
    let policy = Arc::new(Policy {
        tenant: TenantId::new("error-schema-startup").unwrap(),
        mode: AtomicUsize::new(0),
        calls: AtomicUsize::new(0),
    });
    for drifted in [false, true] {
        let mut builder = JsonSchemaRegistryBuilder::with_default_limits();
        let result = McpToolReconciler::audit_schema();
        builder.register(reference(&result), result).unwrap();
        if drifted {
            let mut error = McpToolErrorReconciler::audit_schema();
            error["description"] = json!("unapproved schema drift");
            builder.register(reference(&error), error).unwrap();
        }
        let registry = builder.build().unwrap();
        assert!(McpToolReconciler::new(store.clone(), registry.clone(), policy.clone()).is_ok());
        assert!(matches!(
            McpToolErrorReconciler::new(store.clone(), registry, policy.clone()),
            Err(McpReconciliationError::Invalid)
        ));
    }
    let (registry, _, _) = schemas();
    assert!(McpToolErrorReconciler::new(store.clone(), registry, policy).is_ok());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn known_errors_require_distinct_authority_and_recover_exact_lost_receipts() {
    let Some(store) = store().await else { return };
    let (schemas, input, output) = schemas();
    let tenant = TenantId::new(format!("error-reconciliation-{}", RunId::generate())).unwrap();
    let policy = Arc::new(Policy {
        tenant: tenant.clone(),
        mode: AtomicUsize::new(0),
        calls: AtomicUsize::new(0),
    });
    let server = Server::start(store.clone(), schemas.clone(), policy.clone()).await;
    for effect in [McpKnownToolEffect::NotApplied, McpKnownToolEffect::Applied] {
        let (original, old_fence) =
            Box::pin(unknown(&store, tenant.clone(), &input, &output)).await;
        let request = request(&original, effect);
        let args = json!(request);
        let before = store
            .load_run(&tenant, request.run_id)
            .await
            .unwrap()
            .journal_head()
            .cloned()
            .unwrap();
        let calls_before = policy.calls.load(Ordering::SeqCst);
        for token in ["ordinary-worker", "ops-a"] {
            assert!(
                submit(&server.url, token, args.clone(), false)
                    .await
                    .get("error")
                    .is_some()
            );
        }
        assert!(
            call(&server.url, "error-ops-a", json!(original), false)
                .await
                .get("error")
                .is_some()
        );
        assert_eq!(policy.calls.load(Ordering::SeqCst), calls_before);

        for (field, value) in [
            ("retry_advice", json!({"kind":"safe_after","delay_ms":"1"})),
            ("external_effect", json!("unknown")),
            ("failure_category", json!("ambiguous_external_outcome")),
            ("origin", json!("provider.claim")),
            ("usage", json!({"tokens":"0"})),
        ] {
            let mut bad = args.clone();
            bad[field] = value;
            assert!(
                submit(&server.url, "error-ops-a", bad, false)
                    .await
                    .get("error")
                    .is_some(),
                "{field}"
            );
        }
        policy.mode.store(1, Ordering::SeqCst);
        error(
            &submit(&server.url, "error-ops-a", args.clone(), false).await,
            "reconciliation.denied",
        );
        let mut missing = args.clone();
        missing["run_id"] = json!(RunId::generate());
        error(
            &submit(&server.url, "error-ops-a", missing, false).await,
            "reconciliation.denied",
        );
        policy.mode.store(0, Ordering::SeqCst);
        let mut rejected = args.clone();
        rejected["failure_code"] = json!("fixture.rejected_evidence");
        error(
            &submit(&server.url, "error-ops-a", rejected, false).await,
            "reconciliation.denied",
        );
        error(
            &submit(&server.url, "error-other-tenant", args.clone(), false).await,
            "reconciliation.conflict",
        );
        error(
            &submit(&server.url, "error-ops-a", args.clone(), false).await,
            "reconciliation.busy",
        );
        assert_eq!(
            store
                .load_run(&tenant, request.run_id)
                .await
                .unwrap()
                .journal_head(),
            Some(&before)
        );
        assert_eq!(
            store
                .load_tool_invocation(&tenant, request.run_id, request.invocation_id)
                .await
                .unwrap()
                .status(),
            ToolInvocationStatus::Unknown
        );
        store.release_lease(&old_fence).await.unwrap();

        let pending = {
            let url = server.url.clone();
            let args = args.clone();
            tokio::spawn(async move { submit(&url, "error-ops-a", args, true).await })
        };
        tokio::time::timeout(Duration::from_secs(5), server.loss.committed.notified())
            .await
            .unwrap();
        let committed = store
            .load_tool_invocation(&tenant, request.run_id, request.invocation_id)
            .await
            .unwrap();
        assert_eq!(committed.status(), ToolInvocationStatus::Failed);
        assert_eq!(
            committed.revision(),
            request.expected_revision.checked_next().unwrap()
        );
        let ToolInvocationState::Failed { error: known } = committed.state() else {
            panic!("expected known failure")
        };
        assert_eq!(
            known.external_effect(),
            match effect {
                McpKnownToolEffect::NotApplied => ToolExternalEffect::NotApplied,
                McpKnownToolEffect::Applied => ToolExternalEffect::Applied,
            }
        );
        assert_eq!(known.failure().id(), request.failure_id);
        assert_eq!(known.failure().category(), request.failure_category);
        assert_eq!(known.failure().code(), &request.failure_code);
        assert_eq!(known.failure().message(), &request.failure_message);
        assert_eq!(known.failure().origin().as_str(), "mcp.reconciliation");
        assert_eq!(known.failure().retry_advice(), RetryAdvice::Never);
        assert_eq!(known.phase(), ToolErrorPhase::Execution);
        assert_eq!(known.provenance().attempt_id(), request.attempt_id);
        assert_eq!(known.provenance().invocation_id(), request.invocation_id);
        assert!(known.failure().details().is_none());
        assert!(known.recovery_handle().is_none());
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        server.loss.release();
        let recovered = submit(&server.url, "error-ops-a", args.clone(), false).await;
        assert_eq!(recovered["result"]["content"], json!([]));
        let receipt = recovered["result"]["structuredContent"].clone();
        assert_eq!(
            receipt,
            json!({"event_id":request.event_id,"revision":committed.revision(),"invocation_digest":committed.digest()})
        );
        let (audit, _) = store
            .load_tool_invocation_revision(
                &tenant,
                request.run_id,
                request.invocation_id,
                committed.revision(),
            )
            .await
            .unwrap();
        assert_eq!(audit.payload().kind().as_str(), "mcp-tool-error-reconciled");
        assert_eq!(
            audit.payload().schema(),
            &reference(&McpToolErrorReconciler::audit_schema())
        );
        assert_eq!(
            audit.payload().data().as_value()["policy_digest"],
            json!(Digest::sha256(b"retained policy"))
        );
        assert!(
            !audit
                .payload()
                .data()
                .as_value()
                .to_string()
                .contains("authoritative_failure")
        );
        assert!(!receipt.to_string().contains("confirmed"));

        let mut duplicates = Vec::new();
        for _ in 0..24 {
            let url = server.url.clone();
            let args = args.clone();
            duplicates.push(tokio::spawn(async move {
                submit(&url, "error-ops-a", args, false).await
            }));
        }
        for duplicate in duplicates {
            assert_eq!(
                duplicate.await.unwrap()["result"]["structuredContent"],
                receipt
            );
        }
        for (field, value) in [
            ("event_id", json!(EventId::generate())),
            ("failure_id", json!(FailureId::generate())),
            ("failure_message", json!("Another valid public failure.")),
            ("failure_code", json!("fixture.different")),
            ("failure_category", json!("internal")),
            ("expected_revision", json!(committed.revision())),
            ("attempt_id", json!(AttemptId::generate())),
            ("expected_digest", json!(Digest::sha256(b"other"))),
            (
                "external_effect",
                json!(match effect {
                    McpKnownToolEffect::NotApplied => "applied",
                    McpKnownToolEffect::Applied => "not_applied",
                }),
            ),
        ] {
            let mut conflict = args.clone();
            conflict[field] = value;
            error(
                &submit(&server.url, "error-ops-a", conflict, false).await,
                "reconciliation.conflict",
            );
        }
        error(
            &submit(&server.url, "error-ops-b", args.clone(), false).await,
            "reconciliation.conflict",
        );
        error(
            &call(&server.url, "ops-a", json!(original), false).await,
            "reconciliation.conflict",
        );
        assert_eq!(
            store
                .load_run(&tenant, request.run_id)
                .await
                .unwrap()
                .journal_head(),
            Some(committed.journal_head())
        );

        let replacement = Server::start(store.clone(), schemas.clone(), policy.clone()).await;
        let later = store
            .claim_lease(&tenant, request.run_id, AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .clone();
        assert_eq!(
            submit(&replacement.url, "error-ops-a", args.clone(), false).await["result"]["structuredContent"],
            receipt
        );
        // A recovered receipt must not release or mutate the later Worker's lease.
        let noise = store
            .append_worker(
                append(later.fence(), committed.journal_head().clone(), &input),
                RunProjection::Unchanged,
            )
            .await
            .unwrap();
        assert_eq!(
            submit(&replacement.url, "error-ops-a", args.clone(), false).await["result"]["structuredContent"],
            receipt
        );
        assert!(matches!(
            store
                .append_worker(
                    append(&old_fence, noise.event().head(), &input),
                    RunProjection::Unchanged
                )
                .await,
            Err(StoreError::StaleFence)
        ));
        store.release_lease(later.fence()).await.unwrap();
        policy.mode.store(1, Ordering::SeqCst);
        error(
            &submit(&replacement.url, "error-ops-a", args, false).await,
            "reconciliation.denied",
        );
        policy.mode.store(0, Ordering::SeqCst);
    }
    store.close().await;
    policy.mode.store(1, Ordering::SeqCst);
    error(
        &submit(&server.url, "error-ops-a", json!(wire_request()), false).await,
        "reconciliation.denied",
    );
    println!(
        "\nSTATEKNOT_MCP_ERROR_RECONCILIATION_EVIDENCE={{\"profile\":\"known-error-reconciliation-v1\",\"effects\":2,\"authorization_before_lookup\":true,\"distinct_scopes\":true,\"atomic_audit\":true,\"lost_http_receipt\":true,\"duplicate_receipts_per_effect\":24,\"retry_never\":true,\"fresh_service_recovery\":true,\"stale_fence_rejected\":true,\"invariants\":\"passed\"}}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_success_and_failure_submissions_have_one_authoritative_winner() {
    let Some(store) = store().await else { return };
    let (schemas, input, output) = schemas();
    let tenant = TenantId::new(format!("reconciliation-race-{}", RunId::generate())).unwrap();
    let policy = Arc::new(Policy {
        tenant: tenant.clone(),
        mode: AtomicUsize::new(0),
        calls: AtomicUsize::new(0),
    });
    let server = Server::start(store.clone(), schemas, policy).await;
    for mixed in [false, true] {
        let (success, fence) = Box::pin(unknown(&store, tenant.clone(), &input, &output)).await;
        let failure = request(&success, McpKnownToolEffect::Applied);
        store.release_lease(&fence).await.unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(24));
        let mut races = Vec::new();
        for index in 0..24 {
            let url = server.url.clone();
            let barrier = barrier.clone();
            let use_success = mixed && index % 2 == 0;
            let args = if use_success {
                json!(success)
            } else {
                json!(failure)
            };
            races.push(tokio::spawn(async move {
                barrier.wait().await;
                let response = if use_success {
                    call(&url, "ops-a", args, false).await
                } else {
                    submit(&url, "error-ops-a", args, false).await
                };
                (use_success, response)
            }));
        }
        let mut accepted = Vec::new();
        for race in races {
            let (success, response) = race.await.unwrap();
            if response["result"]["isError"] == true {
                assert!(
                    ["reconciliation.busy", "reconciliation.conflict"]
                        .contains(&response["result"]["content"][0]["text"].as_str().unwrap()),
                    "{response}"
                );
            } else {
                accepted.push((success, response["result"]["structuredContent"].clone()));
            }
        }
        assert!(!accepted.is_empty());
        let current = store
            .load_tool_invocation(&tenant, success.run_id, success.invocation_id)
            .await
            .unwrap();
        assert_eq!(
            current.revision(),
            success.expected_revision.checked_next().unwrap()
        );
        assert_eq!(
            store
                .load_run(&tenant, success.run_id)
                .await
                .unwrap()
                .journal_head(),
            Some(current.journal_head())
        );
        let winning_success = current.status() == ToolInvocationStatus::Committed;
        assert!(winning_success || current.status() == ToolInvocationStatus::Failed);
        if !mixed {
            assert!(!winning_success);
        }
        let winning_receipt = json!({"event_id":success.event_id,"invocation_digest":current.digest(),"revision":current.revision()});
        for (success, receipt) in accepted {
            assert_eq!(success, winning_success);
            assert_eq!(receipt, winning_receipt);
        }
        if winning_success {
            error(
                &submit(&server.url, "error-ops-a", json!(failure), false).await,
                "reconciliation.conflict",
            );
            assert_eq!(
                call(&server.url, "ops-a", json!(success), false).await["result"]["structuredContent"],
                winning_receipt
            );
        } else {
            error(
                &call(&server.url, "ops-a", json!(success), false).await,
                "reconciliation.conflict",
            );
            assert_eq!(
                submit(&server.url, "error-ops-a", json!(failure), false).await["result"]["structuredContent"],
                winning_receipt
            );
        }
    }
    // Exercise the success-first direction deterministically, regardless of race scheduling.
    let (success, fence) = Box::pin(unknown(&store, tenant.clone(), &input, &output)).await;
    let failure = request(&success, McpKnownToolEffect::NotApplied);
    store.release_lease(&fence).await.unwrap();
    let accepted = call(&server.url, "ops-a", json!(success), false).await;
    assert!(accepted["result"]["structuredContent"].is_object());
    error(
        &submit(&server.url, "error-ops-a", json!(failure), false).await,
        "reconciliation.conflict",
    );
    let current = store
        .load_tool_invocation(&tenant, success.run_id, success.invocation_id)
        .await
        .unwrap();
    assert_eq!(current.status(), ToolInvocationStatus::Committed);
    assert_eq!(
        current.revision(),
        success.expected_revision.checked_next().unwrap()
    );
    assert_eq!(
        call(&server.url, "ops-a", json!(success), false).await["result"]["structuredContent"],
        accepted["result"]["structuredContent"]
    );
    store.close().await;
    println!(
        "\nSTATEKNOT_MCP_ERROR_RECONCILIATION_RACE_EVIDENCE={{\"error_first_submissions\":24,\"mixed_success_failure_submissions\":24,\"same_event_identity\":true,\"single_revision\":true,\"success_first_conflict\":true,\"invariants\":\"passed\"}}"
    );
}
