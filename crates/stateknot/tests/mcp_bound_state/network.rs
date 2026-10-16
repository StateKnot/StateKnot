// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn three_consumers_round_trip_over_tcp_and_refuse_rebound_state_before_business() {
    for consumer in Mount::CONSUMERS {
        for change in ["tenant", "effective", "generation", "aba", "scope"] {
            let host = Host::new();
            let original = host.current();
            let server = Server::start(host.clone(), Mount::Composite, true, false, false).await;
            let (method, params) = consumer.operation();
            let token = state(&server.invoke(method, params.clone()).await);
            assert_ne!(token, INNER);
            complete(
                &server
                    .invoke(method, continued(params.clone(), &token))
                    .await,
            );
            assert_eq!(
                host.0.inner_states.lock().unwrap().as_slice(),
                &[None, Some(INNER.to_owned())]
            );
            assert_eq!(
                host.0.wire_states.lock().unwrap().last(),
                Some(&Some(token.clone()))
            );
            let operation = params.get("name").or_else(|| params.get("uri")).unwrap();
            assert_eq!(
                host.0.business_facts.lock().unwrap().last(),
                Some(
                    &json!({"operation":operation,"arguments":params.get("arguments").unwrap_or(&Value::Null),"responses":{"approval":{"roots":[]}}})
                )
            );
            let mut rebound = original.clone();
            rebound.generation = 2;
            match change {
                "tenant" => rebound.tenant = TenantId::new("bound-network-b").unwrap(),
                "effective" => rebound.effective = identity("different-local-operator"),
                "generation" => {}
                "scope" => {
                    rebound = original.clone();
                    rebound.scopes = vec!["bound:use".to_owned()];
                }
                "aba" => {
                    let mut intermediate = rebound.clone();
                    intermediate.tenant = TenantId::new("bound-network-b").unwrap();
                    host.set(intermediate);
                    rebound = original.clone();
                    rebound.generation = 3;
                }
                _ => unreachable!(),
            }
            // External issuer/subject remain identical; only the separate scope case narrows scopes.
            assert_eq!(
                original
                    .principal()
                    .host_binding()
                    .unwrap()
                    .authenticated_identity(),
                rebound
                    .principal()
                    .host_binding()
                    .unwrap()
                    .authenticated_identity()
            );
            if change == "scope" {
                assert_eq!(
                    original.principal().host_binding(),
                    rebound.principal().host_binding()
                );
                assert_ne!(original.scopes, rebound.scopes);
            } else {
                assert_eq!(original.scopes, rebound.scopes);
            }
            host.set(rebound);
            let before = host.count();
            invalid(
                &server
                    .invoke(method, continued(params.clone(), &token))
                    .await,
            );
            assert_eq!(
                host.count(),
                before,
                "{consumer:?}/{change} dispatched old state"
            );
            // The new mapping is usable; rejection is not a blanket denial of its operation.
            let fresh = state(&server.invoke(method, params.clone()).await);
            complete(&server.invoke(method, continued(params, &fresh)).await);
            assert_eq!(host.0.completions.load(Ordering::SeqCst), 2);
        }
    }
}

#[tokio::test]
async fn direct_shared_and_composite_order_keep_actual_three_consumer_guards() {
    for (shared, early) in [(false, false), (true, true)] {
        for consumer in Mount::CONSUMERS {
            let host = Host::new();
            let server = Server::start(host.clone(), consumer, true, shared, early).await;
            let (method, params) = consumer.operation();
            let token = state(&server.invoke(method, params.clone()).await);
            complete(
                &server
                    .invoke(method, continued(params.clone(), &token))
                    .await,
            );
            let mut changed = continued(params.clone(), &token);
            match consumer {
                Mount::Resource => changed["uri"] = json!("test://bound/b"),
                _ => changed["arguments"]["value"] = json!("changed"),
            }
            let before = host.count();
            invalid(&server.invoke(method, changed).await);
            assert_eq!(host.count(), before);
            if !matches!(consumer, Mount::Resource) {
                let mut changed = continued(params, &token);
                changed["name"] = json!("bound_b");
                invalid(&server.invoke(method, changed).await);
                assert_eq!(host.count(), before);
            }
        }
        let host = Host::new();
        let server = Server::start(host.clone(), Mount::Composite, true, shared, early).await;
        for consumer in Mount::CONSUMERS {
            let (method, params) = consumer.operation();
            let token = state(&server.invoke(method, params.clone()).await);
            complete(&server.invoke(method, continued(params, &token)).await);
        }
        complete(&server.invoke("completion/complete", json!({"ref":{"type":"ref/prompt","name":"bound_a"},"argument":{"name":"value","value":"a"}})).await);
        assert_eq!(host.0.completions.load(Ordering::SeqCst), 3);
    }
}

#[tokio::test]
async fn decoded_presence_and_strict_envelope_reach_each_real_consumer_guard() {
    for consumer in Mount::CONSUMERS {
        let host = Host::new();
        let server = Server::start(host.clone(), consumer, true, false, false).await;
        let (method, params) = consumer.operation();
        let token = state(&server.invoke(method, params.clone()).await);
        let ad = host.0.bindings.lock().unwrap().last().unwrap().clone();
        let before = host.count();
        for responses in [json!({}), json!({"approval":{"roots":[]}})] {
            let mut no_state = params.clone();
            no_state["inputResponses"] = responses;
            invalid(&server.invoke(method, no_state).await);
            assert_eq!(host.count(), before);
        }
        for envelope in [
            json!({"version":1}),
            json!({"version":1,"applicationState":INNER,"unknown":true}),
            json!({"version":2,"applicationState":INNER}),
            json!({"version":1.0,"applicationState":INNER}),
            json!({"version":1,"applicationState":false}),
            json!({"version":1,"applicationState":""}),
            json!([1, INNER]),
        ] {
            // Use the public codec and actual handler-observed AD, not a test reimplementation of hashing.
            let malformed = codec()
                .seal_json(&envelope, &ad, Duration::from_secs(30))
                .unwrap();
            invalid(
                &server
                    .invoke(method, continued(params.clone(), &malformed))
                    .await,
            );
            assert_eq!(host.count(), before);
        }
        let wrong_key = McpServerRequestStateCodec::new([72; 32])
            .unwrap()
            .seal_json(
                &json!({"version":1,"applicationState":INNER}),
                &ad,
                Duration::from_secs(30),
            )
            .unwrap();
        for bad in [format!("x{token}"), INNER.to_owned(), wrong_key] {
            invalid(&server.invoke(method, continued(params.clone(), &bad)).await);
            assert_eq!(host.count(), before);
        }
        let mut bad_without_responses = params.clone();
        bad_without_responses["requestState"] = json!(INNER);
        invalid(&server.invoke(method, bad_without_responses).await);
        assert_eq!(host.count(), before);
        // A valid state with no responses is still checked and unwrapped, then may re-challenge.
        let mut only_state = params.clone();
        only_state["requestState"] = json!(token);
        state(&server.invoke(method, only_state).await);
        assert_eq!(
            host.0.inner_states.lock().unwrap().last(),
            Some(&Some(INNER.to_owned()))
        );
        // Wire null preserves the SDK None contract, and is an initial call, not a continuation redemption.
        let mut initial = params;
        initial["requestState"] = Value::Null;
        initial["inputResponses"] = Value::Null;
        state(&server.invoke(method, initial).await);
        assert_eq!(host.0.inner_states.lock().unwrap().last(), Some(&None));
    }
}

#[tokio::test]
async fn null_inner_state_is_sealed_and_round_trips_in_all_three_consumers() {
    for consumer in Mount::CONSUMERS {
        let mut host = Host::new();
        Arc::get_mut(&mut host.0).unwrap().inner = None;
        let server = Server::start(host.clone(), consumer, true, false, false).await;
        let (method, params) = consumer.operation();
        let token = state(&server.invoke(method, params.clone()).await);
        assert!(!token.is_empty());
        complete(&server.invoke(method, continued(params, &token)).await);
        assert_eq!(
            host.0.inner_states.lock().unwrap().as_slice(),
            &[None, None]
        );
        assert_eq!(host.0.completions.load(Ordering::SeqCst), 1);
    }
}

async fn all_page_items(server: &Server, method: &str) -> Vec<Value> {
    let field = match method {
        "tools/list" => "tools",
        "prompts/list" => "prompts",
        "resources/templates/list" => "resourceTemplates",
        "skills/list" => "skills",
        _ => "resources",
    };
    let mut items = Vec::new();
    let mut cursor = None;
    for _ in 0..8 {
        let params = cursor
            .as_ref()
            .map_or_else(|| json!({}), |cursor| json!({"cursor":cursor}));
        let response = server.invoke(method, params).await;
        complete(&response);
        items.extend(
            response["result"][field]
                .as_array()
                .unwrap()
                .iter()
                .cloned(),
        );
        cursor = response
            .pointer("/result/nextCursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if cursor.is_none() {
            return items;
        }
    }
    panic!("fixture catalog did not terminate");
}

#[tokio::test]
async fn private_pages_share_binding_scope_and_refuse_remaps_scope_changes_and_legacy_cursors() {
    let mut common_fingerprint = None;
    for (mount, method) in [
        (Mount::Tool, "tools/list"),
        (Mount::Resource, "resources/list"),
        (Mount::Resource, "resources/templates/list"),
        (Mount::Prompt, "prompts/list"),
        (Mount::Skill, "skills/list"),
        (Mount::Skill, "resources/list"),
    ] {
        let host = Host::new();
        let server = Server::start(host.clone(), mount, true, true, true).await;
        let first = server.invoke(method, json!({})).await;
        assert_eq!(
            first.pointer("/result/cacheScope"),
            Some(&json!("private")),
            "{first}"
        );
        let cursor = first
            .pointer("/result/nextCursor")
            .and_then(Value::as_str)
            .unwrap();
        assert!(cursor.starts_with("v2."), "{cursor}");
        let fingerprint = cursor.split('.').rev().nth(1).unwrap().to_owned();
        if let Some(expected) = &common_fingerprint {
            assert_eq!(&fingerprint, expected);
        } else {
            common_fingerprint = Some(fingerprint);
        }
        complete(&server.invoke(method, json!({"cursor":cursor})).await);
        let mut reordered = host.current();
        reordered.scopes.reverse();
        host.set(reordered);
        complete(&server.invoke(method, json!({"cursor":cursor})).await);
        assert!(
            server
                .invoke(method, json!({"cursor":cursor.replacen("v2.","v1.",1)}))
                .await
                .get("error")
                .is_some()
        );
        for change in ["tenant", "effective", "generation", "scope", "aba"] {
            // Mint under the current mapping each time; generations never roll back or get reused.
            let page = server.invoke(method, json!({})).await;
            let old = page
                .pointer("/result/nextCursor")
                .and_then(Value::as_str)
                .unwrap();
            let original = host.current();
            let mut rebound = original.clone();
            match change {
                "tenant" => {
                    rebound.tenant = TenantId::new("bound-page-b").unwrap();
                    rebound.generation += 1;
                }
                "effective" => {
                    rebound.effective = identity("page-local-b");
                    rebound.generation += 1;
                }
                "generation" => rebound.generation += 1,
                "scope" => rebound.scopes = vec!["bound:use".to_owned()],
                "aba" => {
                    let mut away = original.clone();
                    away.tenant = TenantId::new("bound-page-away").unwrap();
                    away.generation += 1;
                    host.set(away);
                    rebound.generation += 2;
                }
                _ => unreachable!(),
            }
            host.set(rebound);
            assert!(
                server
                    .invoke(method, json!({"cursor":old}))
                    .await
                    .get("error")
                    .is_some(),
                "{mount:?}/{method}/{change}"
            );
            let items = all_page_items(&server, method).await;
            if change == "scope" {
                assert_eq!(items.len(), 2, "{method}: {items:?}");
                assert!(
                    items
                        .iter()
                        .all(|value| !value.to_string().contains("scoped")),
                    "{method}: {items:?}"
                );
                let mut restored = host.current();
                restored.scopes = SCOPES.into_iter().map(str::to_owned).collect();
                host.set(restored);
            } else {
                assert_eq!(items.len(), 3, "{method}: {items:?}");
            }
        }
        assert_eq!(
            host.count(),
            0,
            "catalog must not invoke business callbacks"
        );
    }
}

#[tokio::test]
async fn unbound_principal_is_rejected_at_metadata_empty_templates_and_composite_skill_internal_routes()
 {
    for mount in [
        Mount::Composite,
        Mount::Tool,
        Mount::Resource,
        Mount::Prompt,
        Mount::Skill,
    ] {
        let host = Host::new();
        host.0.legacy_principal.store(true, Ordering::SeqCst);
        let server = Server::start(host.clone(), mount, true, false, true).await;
        let mut cases = vec![("server/discover", json!({}))];
        match mount {
            Mount::Composite => cases.extend([
                ("skills/list", json!({})),
                ("resources/read", json!({"uri":"skill://bound-a/SKILL.md"})),
                ("completion/complete", json!({"ref":{"type":"ref/prompt","name":"bound_a"},"argument":{"name":"value","value":"a"}})),
            ]),
            Mount::Tool => cases.extend([
                ("tools/list",json!({})),("prompts/list",json!({})),
                ("completion/complete",json!({"ref":{"type":"ref/prompt","name":"bound_a"},"argument":{"name":"value","value":"a"}})),
            ]),
            Mount::Resource => cases.push(("resources/templates/list",json!({}))),
            Mount::Prompt => cases.push(("prompts/list",json!({}))),
            Mount::Skill => cases.extend([("resources/templates/list",json!({})),("skills/list",json!({})),("resources/read",json!({"uri":"skill://bound-a/SKILL.md"}))]),
        }
        for (method, params) in cases {
            let response = server.invoke(method, params).await;
            assert_eq!(
                response.pointer("/error/message"),
                Some(&json!("MCP host binding is unavailable")),
                "{mount:?}/{method}: {response}"
            );
        }
        assert_eq!(host.count(), 0);
    }
    let host = Host::new();
    let server = Server::start(host, Mount::Skill, true, false, false).await;
    complete(&server.invoke("resources/templates/list", json!({})).await);
    assert!(
        server
            .invoke(
                "resources/read",
                json!({"uri":"skill://bound-a/SKILL.md","inputResponses":{}})
            )
            .await
            .get("error")
            .is_some()
    );
}

#[tokio::test]
async fn no_profile_preserves_legacy_plain_inner_state_for_legacy_and_host_bound_principals() {
    for consumer in Mount::CONSUMERS {
        for legacy in [true, false] {
            let host = Host::new();
            host.0.legacy_principal.store(legacy, Ordering::SeqCst);
            let server = Server::start(host.clone(), consumer, false, true, false).await;
            let (method, params) = consumer.operation();
            let token = state(&server.invoke(method, params.clone()).await);
            assert_eq!(token, INNER);
            complete(&server.invoke(method, continued(params, &token)).await);
            assert_eq!(host.0.completions.load(Ordering::SeqCst), 1);
        }
    }
}

#[test]
fn public_catalog_and_reconfiguration_refusals_cover_composite_and_standalone() {
    let host = Host::new();
    assert!(matches!(
        McpServerApplicationBuilder::new(options(McpServerCacheScope::Public))
            .with_bound_state(profile()),
        Err(McpServerApplicationBuildError::PublicCacheWithBoundState)
    ));
    assert!(matches!(
        McpServerApplicationBuilder::new(options(McpServerCacheScope::Private))
            .with_bound_state(profile())
            .unwrap()
            .with_bound_state(profile()),
        Err(McpServerApplicationBuildError::DuplicateBoundState)
    ));
    let tool = McpServerToolService::with_shared_authorization(
        tools(&host, false),
        options(McpServerCacheScope::Public),
        Arc::new(host.clone()),
    )
    .unwrap();
    assert!(matches!(
        tool.with_bound_state(profile()),
        Err(McpServerToolServiceBuildError::PublicCacheWithBoundState)
    ));
    let resource = McpServerResourceService::with_shared(
        resources(false),
        options(McpServerCacheScope::Public),
        Arc::new(host.clone()),
        Arc::new(host.clone()),
    )
    .unwrap();
    assert!(matches!(
        resource.with_bound_state(profile()),
        Err(McpServerResourceServiceBuildError::PublicCacheWithBoundState)
    ));
    let prompt = McpServerPromptService::with_shared(
        prompts(false),
        options(McpServerCacheScope::Public),
        Arc::new(host.clone()),
        Arc::new(host.clone()),
    )
    .unwrap();
    assert!(matches!(
        prompt.with_bound_state(profile()),
        Err(McpServerPromptServiceBuildError::PublicCacheWithBoundState)
    ));
    let skill = McpServerSkillService::new(
        skills(false),
        options(McpServerCacheScope::Public),
        AllowMcpServerSkillAuthorization,
    )
    .unwrap();
    assert!(matches!(
        skill.with_bound_state(profile()),
        Err(McpServerSkillServiceBuildError::PublicCacheWithBoundState)
    ));
    let tool = McpServerToolService::new(
        tools(&host, false),
        options(McpServerCacheScope::Private),
        host.clone(),
    )
    .unwrap()
    .with_bound_state(profile())
    .unwrap();
    assert!(matches!(
        tool.with_bound_state(profile()),
        Err(McpServerToolServiceBuildError::BoundStateAlreadyConfigured)
    ));
    let resource = McpServerResourceService::new(
        resources(false),
        options(McpServerCacheScope::Private),
        host.clone(),
        host.clone(),
    )
    .unwrap()
    .with_bound_state(profile())
    .unwrap();
    assert!(matches!(
        resource.with_bound_state(profile()),
        Err(McpServerResourceServiceBuildError::BoundStateAlreadyConfigured)
    ));
    let prompt = McpServerPromptService::new(
        prompts(false),
        options(McpServerCacheScope::Private),
        host.clone(),
        host.clone(),
    )
    .unwrap()
    .with_bound_state(profile())
    .unwrap();
    assert!(matches!(
        prompt.with_bound_state(profile()),
        Err(McpServerPromptServiceBuildError::BoundStateAlreadyConfigured)
    ));
    let skill =
        McpServerSkillService::new(skills(false), options(McpServerCacheScope::Private), host)
            .unwrap()
            .with_bound_state(profile())
            .unwrap();
    assert!(matches!(
        skill.with_bound_state(profile()),
        Err(McpServerSkillServiceBuildError::BoundStateAlreadyConfigured)
    ));
}

#[tokio::test]
async fn private_catalog_does_not_rewrite_explicit_public_resource_contents() {
    let host = Host::new();
    let server = Server::start(host, Mount::Resource, true, false, false).await;
    let list = server.invoke("resources/list", json!({})).await;
    assert_eq!(list.pointer("/result/cacheScope"), Some(&json!("private")));
    let (method, params) = Mount::Resource.operation();
    let token = state(&server.invoke(method, params.clone()).await);
    let result = server.invoke(method, continued(params, &token)).await;
    complete(&result);
    assert_eq!(result.pointer("/result/cacheScope"), Some(&json!("public")));
    assert_eq!(
        result.pointer("/result/contents/0/text"),
        Some(&json!("committed"))
    );
}

#[tokio::test]
async fn queued_decoded_policy_and_admission_recheck_current_generation() {
    for consumer in Mount::CONSUMERS {
        let host = Host::new();
        let server = Arc::new(Server::start(host.clone(), consumer, true, false, false).await);
        let (method, params) = consumer.operation();
        let token = state(&server.invoke(method, params.clone()).await);
        host.0.policy_gate.arm();
        let pending_server = server.clone();
        // A syntactically bounded but unauthentic wire token proves decoded policy
        // runs before opening state; otherwise the policy latch is never reached.
        let pending_wire = format!("x{token}");
        let continued = continued(params, &pending_wire);
        let pending = tokio::spawn(async move { pending_server.invoke(method, continued).await });
        tokio::time::timeout(
            Duration::from_secs(5),
            host.0.policy_gate.entered.notified(),
        )
        .await
        .unwrap();
        let before = host.count();
        let mut next = host.current();
        next.generation += 1;
        host.set(next);
        host.0.policy_gate.release.notify_waiters();
        let refused = pending.await.unwrap();
        let denial = match consumer {
            Mount::Tool => "Unknown or unavailable tool",
            Mount::Resource => "Resource not found or unavailable",
            Mount::Prompt => "Unknown or unavailable prompt",
            _ => unreachable!(),
        };
        assert_eq!(
            refused.pointer("/error/message"),
            Some(&json!(denial)),
            "{refused}"
        );
        assert_eq!(host.count(), before);
        assert_eq!(
            host.0.wire_states.lock().unwrap().last(),
            Some(&Some(pending_wire))
        );
    }
    // Catalogs have no decoded policy. The host admission performs its current check after queue release.
    let host = Host::new();
    let server = Arc::new(Server::start(host.clone(), Mount::Tool, true, false, false).await);
    host.0.admission_gate.arm();
    let pending_server = server.clone();
    let pending = tokio::spawn(async move { pending_server.invoke("tools/list", json!({})).await });
    tokio::time::timeout(
        Duration::from_secs(5),
        host.0.admission_gate.entered.notified(),
    )
    .await
    .unwrap();
    let mut next = host.current();
    next.generation += 1;
    host.set(next);
    host.0.admission_gate.release.notify_waiters();
    let refused = pending.await.unwrap();
    assert_eq!(refused.pointer("/error/http_status"), Some(&json!(403)));
    assert_eq!(host.count(), 0);
    // A current request on the same live server succeeds after the queued stale request was refused.
    complete(&server.invoke("tools/list", json!({})).await);
}

#[tokio::test]
async fn actual_expired_state_refuses_before_business_and_outgoing_aggregate_limit_is_after_business()
 {
    let host = Host::new();
    let server = Server::start(host.clone(), Mount::Tool, true, false, false).await;
    let (method, params) = Mount::Tool.operation();
    state(&server.invoke(method, params.clone()).await);
    let ad = host.0.bindings.lock().unwrap().last().unwrap().clone();
    let expiring = codec()
        .seal_json(
            &json!({"version":1,"applicationState":INNER}),
            &ad,
            Duration::from_secs(1),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let before = host.count();
    invalid(&server.invoke(method, continued(params, &expiring)).await);
    assert_eq!(host.count(), before);
    for consumer in Mount::CONSUMERS {
        let mut host = Host::new();
        Arc::get_mut(&mut host.0).unwrap().inner = Some("x".repeat(64 * 1024));
        let server = Server::start(host.clone(), consumer, true, false, false).await;
        let (method, params) = consumer.operation();
        let response = server.invoke(method, params).await;
        assert_eq!(
            response.pointer("/error/code"),
            Some(&json!(-32603)),
            "{response}"
        );
        assert_eq!(
            host.count(),
            1,
            "outgoing seal failed after application callback"
        );
        assert_eq!(host.0.completions.load(Ordering::SeqCst), 0);
        assert!(
            !response.to_string().contains(&"x".repeat(64)),
            "output error reflected state"
        );
    }
}

#[tokio::test]
async fn configured_direct_default_metadata_and_no_profile_cursor_versions_keep_their_scope() {
    let host = Host::new();
    let server = Server::start(host, Mount::Tool, true, false, false).await;
    let prompts = server.invoke("prompts/list", json!({})).await;
    complete(&prompts);
    assert_eq!(prompts.pointer("/result/prompts"), Some(&json!([])));
    complete(&server.invoke("completion/complete",json!({"ref":{"type":"ref/prompt","name":"bound_a"},"argument":{"name":"value","value":"a"}})).await);
    for (mount, method) in [
        (Mount::Tool, "tools/list"),
        (Mount::Resource, "resources/list"),
        (Mount::Resource, "resources/templates/list"),
        (Mount::Prompt, "prompts/list"),
        (Mount::Skill, "skills/list"),
        (Mount::Skill, "resources/list"),
    ] {
        let host = Host::new();
        let server = Server::start(host, mount, false, false, false).await;
        let first = server.invoke(method, json!({})).await;
        let cursor = first
            .pointer("/result/nextCursor")
            .and_then(Value::as_str)
            .unwrap();
        assert!(cursor.starts_with("v1."), "unconfigured {method}: {cursor}");
        complete(&server.invoke(method, json!({"cursor":cursor})).await);
    }
}

#[tokio::test]
async fn configuring_a_clone_retains_the_old_legacy_handle_and_public_legacy_cursor_contract() {
    let host = Host::new();
    let legacy = McpServerToolService::new(
        tools(&host, false),
        options(McpServerCacheScope::Private),
        host.clone(),
    )
    .unwrap();
    let strict = legacy.clone().with_bound_state(profile()).unwrap();
    let old = Server::tool_handle(host.clone(), legacy).await;
    let new = Server::tool_handle(host.clone(), strict.clone()).await;
    let (method, params) = Mount::Tool.operation();
    let old_state = state(&old.invoke(method, params.clone()).await);
    assert_eq!(old_state, INNER);
    let new_state = state(&new.invoke(method, params.clone()).await);
    assert_ne!(new_state, INNER);
    complete(
        &old.invoke(method, continued(params.clone(), &old_state))
            .await,
    );
    complete(
        &new.invoke(method, continued(params.clone(), &new_state))
            .await,
    );
    let mut rebound = host.current();
    rebound.generation += 1;
    host.set(rebound);
    let before = host.count();
    invalid(
        &new.invoke(method, continued(params.clone(), &new_state))
            .await,
    );
    assert_eq!(host.count(), before);
    complete(&old.invoke(method, continued(params, &old_state)).await);
    let public = McpServerToolService::new(
        tools(&host, false),
        options(McpServerCacheScope::Public),
        host.clone(),
    )
    .unwrap();
    let server = Server::tool_handle(host.clone(), public).await;
    let first = server.invoke("tools/list", json!({})).await;
    assert_eq!(first.pointer("/result/cacheScope"), Some(&json!("public")));
    let cursor = first
        .pointer("/result/nextCursor")
        .and_then(Value::as_str)
        .unwrap();
    let mut rebound = host.current();
    rebound.tenant = TenantId::new("public-legacy-other-tenant").unwrap();
    rebound.generation += 1;
    host.set(rebound);
    complete(&server.invoke("tools/list", json!({"cursor":cursor})).await);
}

// Preserve the supplied argument bytes on the actual TCP request. Passing a Value
// to Server::invoke would let the client's map serialization reorder keys first.
async fn invoke_argument_bytes(
    url: &str,
    method: &str,
    arguments: &str,
    token: Option<&str>,
) -> Value {
    assert!(
        serde_json::from_str::<Value>(arguments)
            .unwrap()
            .is_object()
    );
    let metadata = json!({
        "io.modelcontextprotocol/protocolVersion":"2026-07-28",
        "io.modelcontextprotocol/clientInfo":{"name":"bound-state-test","version":"1"},
        "io.modelcontextprotocol/clientCapabilities":{"roots":{"listChanged":false},"extensions":{(MCP_SKILLS_EXTENSION_ID):{}}}
    });
    let mut body = String::from("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":");
    body.push_str(&serde_json::to_string(method).unwrap());
    body.push_str(",\"params\":{\"name\":\"bound_a\",\"arguments\":");
    body.push_str(arguments);
    body.push_str(",\"_meta\":");
    body.push_str(&metadata.to_string());
    if let Some(token) = token {
        body.push_str(",\"requestState\":");
        body.push_str(&serde_json::to_string(token).unwrap());
        body.push_str(",\"inputResponses\":{\"approval\":{\"roots\":[]}}");
    }
    body.push_str("}}");
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
        .post(url)
        .bearer_auth("host-token")
        .header("MCP-Protocol-Version", "2026-07-28")
        .header("MCP-Method", method)
        .header("MCP-Name", "bound_a")
        .header("Accept", "application/json, text/event-stream")
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    let mut decoded = serde_json::from_str::<Value>(&body).unwrap();
    assert_eq!(decoded.get("jsonrpc"), Some(&json!("2.0")), "{body}");
    decoded["_fixture_http_status"] = json!(status.as_u16());
    decoded
}

fn assert_decoded_argument_encoding(host: &Host, arguments: &str) {
    let expected = serde_json::from_str::<Value>(arguments).unwrap();
    let facts = host.0.business_facts.lock().unwrap();
    let fact = facts.last().unwrap();
    assert_eq!(fact.get("operation"), Some(&json!("bound_a")));
    // These are the real handler's decoded arguments, not the request-side
    // literal. Byte comparison retains integer/float and signed-zero differences
    // that Value equality alone would not distinguish.
    assert_eq!(
        serde_json::to_vec(&fact["arguments"]).unwrap(),
        serde_json::to_vec(&expected).unwrap(),
        "actual decoded business arguments differ from {arguments}"
    );
}

fn argument_business_snapshot(host: &Host) -> (usize, Vec<u8>, usize) {
    (
        host.count(),
        serde_json::to_vec(&*host.0.business_facts.lock().unwrap()).unwrap(),
        host.0.completions.load(Ordering::SeqCst),
    )
}

async fn argument_round_trip(url: &str, host: &Host, method: &str, arguments: &str) -> String {
    let before = host.count();
    let challenge = invoke_argument_bytes(url, method, arguments, None).await;
    assert_eq!(challenge.get("_fixture_http_status"), Some(&json!(200)));
    let token = state(&challenge);
    assert_eq!(host.count(), before + 1);
    assert_decoded_argument_encoding(host, arguments);
    let response = invoke_argument_bytes(url, method, arguments, Some(&token)).await;
    assert_eq!(response.get("_fixture_http_status"), Some(&json!(200)));
    complete(&response);
    assert_eq!(host.count(), before + 2);
    assert_decoded_argument_encoding(host, arguments);
    token
}

#[tokio::test]
async fn lossless_numeric_argument_binding_is_enforced_over_tcp() {
    let pairs = [
        (
            "adjacent integers beyond exact f64 precision",
            r#"{"value":9007199254740992}"#,
            r#"{"value":9007199254740993}"#,
        ),
        (
            "adjacent negative integers beyond exact f64 precision",
            r#"{"value":-9007199254740992}"#,
            r#"{"value":-9007199254740993}"#,
        ),
        (
            "adjacent u64 maximum values",
            r#"{"value":18446744073709551614}"#,
            r#"{"value":18446744073709551615}"#,
        ),
        (
            "adjacent i64 minimum values",
            r#"{"value":-9223372036854775808}"#,
            r#"{"value":-9223372036854775807}"#,
        ),
        (
            "integer versus floating-point representation",
            r#"{"value":1}"#,
            r#"{"value":1.0}"#,
        ),
        (
            "positive versus negative floating-point zero",
            r#"{"value":0.0}"#,
            r#"{"value":-0.0}"#,
        ),
        (
            "wide integer inside a nested object",
            r#"{"value":{"outer":{"number":9007199254740992,"maximum":18446744073709551615}}}"#,
            r#"{"value":{"outer":{"number":9007199254740993,"maximum":18446744073709551615}}}"#,
        ),
        (
            "wide integer inside nested arrays and objects",
            r#"{"value":[-9223372036854775808,{"numbers":[9007199254740993,18446744073709551614]}]}"#,
            r#"{"value":[-9223372036854775808,{"numbers":[9007199254740993,18446744073709551615]}]}"#,
        ),
    ];
    for (label, original, changed) in pairs {
        let original_value = serde_json::from_str::<Value>(original).unwrap();
        let changed_value = serde_json::from_str::<Value>(changed).unwrap();
        assert_ne!(
            serde_json::to_vec(&original_value).unwrap(),
            serde_json::to_vec(&changed_value).unwrap(),
            "{label} must have distinct decoded encodings"
        );
        let host = Host::new();
        let server = Server::start(host.clone(), Mount::Tool, true, false, false).await;
        let token = argument_round_trip(&server.url, &host, "tools/call", original).await;
        let before = argument_business_snapshot(&host);
        invalid(&invoke_argument_bytes(&server.url, "tools/call", changed, Some(&token)).await);
        assert_eq!(argument_business_snapshot(&host), before, "{label}");
        // The changed number is itself schema-valid and reaches business intact
        // under a fresh challenge, so refusal is not blanket numeric rejection.
        argument_round_trip(&server.url, &host, "tools/call", changed).await;
    }

    let host = Host::new();
    let server = Server::start(host.clone(), Mount::Tool, true, false, false).await;
    let original = r#"{"value":{"z":18446744073709551615,"a":[9007199254740993,{"y":-9223372036854775808,"b":1.0}]},"label":"ordered"}"#;
    let reordered = r#"{"label":"ordered","value":{"a":[9007199254740993,{"b":1.0,"y":-9223372036854775808}],"z":18446744073709551615}}"#;
    assert_ne!(original.as_bytes(), reordered.as_bytes());
    assert_eq!(
        serde_json::from_str::<Value>(original).unwrap(),
        serde_json::from_str::<Value>(reordered).unwrap()
    );
    let token = argument_round_trip(&server.url, &host, "tools/call", original).await;
    let before = host.count();
    let response = invoke_argument_bytes(&server.url, "tools/call", reordered, Some(&token)).await;
    assert_eq!(response.get("_fixture_http_status"), Some(&json!(200)));
    complete(&response);
    assert_eq!(host.count(), before + 1);
    assert_decoded_argument_encoding(&host, reordered);

    let host = Host::new();
    let server = Server::start(host.clone(), Mount::Prompt, true, false, false).await;
    let original = r#"{"value":"9007199254740993"}"#;
    let token = argument_round_trip(&server.url, &host, "prompts/get", original).await;
    // The existing Prompt catalog admits only a String value, so numeric
    // arguments are a schema-boundary control, not a reachable numeric AD case.
    for numeric in [r#"{"value":9007199254740993}"#, r#"{"value":1.0}"#] {
        let before = argument_business_snapshot(&host);
        let response =
            invoke_argument_bytes(&server.url, "prompts/get", numeric, Some(&token)).await;
        assert_eq!(response.get("_fixture_http_status"), Some(&json!(400)));
        assert_eq!(response.pointer("/error/code"), Some(&json!(-32602)));
        assert_eq!(
            response.pointer("/error/message"),
            Some(&json!("Prompt arguments are invalid"))
        );
        assert!(response.pointer("/error/data").is_none());
        assert_eq!(argument_business_snapshot(&host), before);
    }
}
