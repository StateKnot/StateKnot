// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::wildcard_imports)]
use super::*;
use crate::{AgentServiceCaller, JsonSchemaRegistryBuilder};
use serde_json::json;
use stateknot_core::*;

fn identity(name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        PrincipalIdentity::new(
            "https://policy.example.test".parse().unwrap(),
            "operator".parse().unwrap(),
        ),
        CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
    )
}
fn document() -> PolicyDocument {
    let principal = identity("policy").owner().clone();
    PolicyDocument {
        format_version: 1,
        policy: identity("policy"),
        valid_until: Timestamp::from_unix_micros(now_micros().unwrap() + 600_000_000).unwrap(),
        submissions: vec![SubmissionRule {
            tenant: TenantId::new("tenant-a").unwrap(),
            principal: principal.clone(),
            agent: identity("agent"),
            input_schema: SchemaReference::new(
                "https://policy.example.test/input".parse().unwrap(),
                Version::new(1, 0, 0),
                Digest::sha256(b"input"),
            ),
            granted_scopes: ScopeSet::empty(),
            budget_limits: BudgetLimits::empty().with_tool_calls(ExecutionCount::new(2)),
        }],
        runs: vec![RunRule {
            tenant: TenantId::new("tenant-a").unwrap(),
            principal,
            operation: RunPermission::Read,
            target: RunAccessTarget::Run(RunId::generate()),
        }],
    }
}
fn context(doc: &PolicyDocument) -> AgentServiceSubmissionAuthorization {
    let rule = &doc.submissions[0];
    AgentServiceSubmissionAuthorization {
        caller: AgentServiceCaller::new(rule.tenant.clone(), rule.principal.clone()),
        agent: rule.agent.clone(),
        request: AgentRequest::new(
            rule.input_schema.clone(),
            BoundedJson::try_from_value(json!({"private":"value"})).unwrap(),
            BudgetLimits::empty(),
        ),
    }
}
fn run_context(doc: &PolicyDocument) -> AgentServiceRunAuthorization {
    let rule = &doc.runs[0];
    let RunAccessTarget::Run(run) = rule.target else {
        panic!("fixture")
    };
    AgentServiceRunAuthorization {
        caller: AgentServiceCaller::new(rule.tenant.clone(), rule.principal.clone()),
        target: AgentServiceRunTarget::Run(run),
        operation: AgentServiceRunOperation::Read,
    }
}
fn policy(doc: PolicyDocument) -> AgentResourcePolicy {
    AgentResourcePolicy::new(PolicyArtifact::new(doc).unwrap(), Duration::from_secs(300)).unwrap()
}

#[test]
fn exact_submission_dimensions_deny_without_grants() {
    let doc = document();
    let valid = context(&doc);
    let policy = policy(doc);
    assert!(policy.submission(&valid).is_ok());
    let mut wrong = valid.clone();
    wrong.agent = identity("other-agent");
    assert_eq!(
        policy.submission(&wrong).unwrap_err(),
        AgentServiceAuthorizationError::Denied
    );
    let mut wrong = valid.clone();
    wrong.caller = AgentServiceCaller::new(
        TenantId::new("tenant-b").unwrap(),
        valid.caller.principal().clone(),
    );
    assert_eq!(
        policy.submission(&wrong).unwrap_err(),
        AgentServiceAuthorizationError::Denied
    );
    for principal in [
        PrincipalIdentity::new(
            "https://other.example.test".parse().unwrap(),
            "operator".parse().unwrap(),
        ),
        PrincipalIdentity::new(
            "https://policy.example.test".parse().unwrap(),
            "other".parse().unwrap(),
        ),
    ] {
        wrong.caller = AgentServiceCaller::new(valid.caller.tenant_id().clone(), principal);
        assert_eq!(
            policy.submission(&wrong).unwrap_err(),
            AgentServiceAuthorizationError::Denied
        );
    }
    let mut wrong = valid.clone();
    wrong.request = AgentRequest::new(
        SchemaReference::new(
            valid.request.input_schema().id().clone(),
            Version::new(1, 0, 0),
            Digest::sha256(b"substitution"),
        ),
        valid.request.input().clone(),
        BudgetLimits::empty(),
    );
    assert_eq!(
        policy.submission(&wrong).unwrap_err(),
        AgentServiceAuthorizationError::Denied
    );
    let mut empty = document();
    empty.submissions.clear();
    empty.runs.clear();
    let empty = self::policy(empty);
    assert!(empty.check_readiness().is_ok());
    assert_eq!(
        empty.submission(&valid).unwrap_err(),
        AgentServiceAuthorizationError::Denied
    );
}

#[test]
fn exact_run_operation_target_and_tenant_are_independent() {
    let doc = document();
    let valid = run_context(&doc);
    let policy = policy(doc.clone());
    assert!(policy.run(&valid).is_ok());
    let mut wrong = valid.clone();
    wrong.operation = AgentServiceRunOperation::Cancel;
    assert_eq!(
        policy.run(&wrong).unwrap_err(),
        AgentServiceAuthorizationError::Denied
    );
    wrong = valid.clone();
    wrong.target = AgentServiceRunTarget::Run(RunId::generate());
    assert_eq!(
        policy.run(&wrong).unwrap_err(),
        AgentServiceAuthorizationError::Denied
    );
    wrong.target = AgentServiceRunTarget::Submission(Digest::sha256(b"key"));
    assert_eq!(
        policy.run(&wrong).unwrap_err(),
        AgentServiceAuthorizationError::Denied
    );
    let mut operator = doc;
    operator.runs[0].target = RunAccessTarget::TenantRuns;
    let operator = self::policy(operator);
    assert!(operator.run(&wrong).is_ok());
    let first = operator.run(&valid).unwrap();
    let second = operator.run(&wrong).unwrap();
    assert_eq!(first.policy_digest(), second.policy_digest());
    assert_ne!(first.decision_digest(), second.decision_digest());
    wrong.caller = AgentServiceCaller::new(
        TenantId::new("tenant-b").unwrap(),
        valid.caller.principal().clone(),
    );
    assert_eq!(
        operator.run(&wrong).unwrap_err(),
        AgentServiceAuthorizationError::Denied
    );
}

#[test]
fn artifact_integrity_bounds_unknown_fields_and_duplicate_keys() {
    let artifact = PolicyArtifact::new(document()).unwrap();
    assert!(PolicyArtifact::from_json(artifact.canonical_bytes(), artifact.digest()).is_ok());
    assert!(
        PolicyArtifact::from_json(artifact.canonical_bytes(), Digest::sha256(b"wrong")).is_err()
    );
    let mut value: serde_json::Value = serde_json::from_slice(artifact.canonical_bytes()).unwrap();
    value["unexpected"] = json!(true);
    assert!(
        PolicyArtifact::from_json(&serde_json::to_vec(&value).unwrap(), artifact.digest()).is_err()
    );
    assert!(
        PolicyArtifact::from_json(
            b"{\"format_version\":1,\"format_version\":1}",
            artifact.digest()
        )
        .is_err()
    );
    assert!(PolicyArtifact::from_json(&vec![b' '; 262_145], artifact.digest()).is_err());
    let mut doc = document();
    doc.format_version = 2;
    assert!(PolicyArtifact::new(doc).is_err());
    let mut doc = document();
    doc.submissions[0].budget_limits = BudgetLimits::empty();
    assert!(PolicyArtifact::new(doc).is_err());
    let mut doc = document();
    doc.runs = vec![doc.runs[0].clone(); 1025];
    assert!(PolicyArtifact::new(doc).is_err());
}

#[test]
fn ambiguous_rules_and_unusable_cancel_by_key_fail_closed() {
    let mut doc = document();
    doc.submissions.push(doc.submissions[0].clone());
    assert!(PolicyArtifact::new(doc).is_err());
    let mut doc = document();
    doc.runs.push(doc.runs[0].clone());
    assert!(PolicyArtifact::new(doc).is_err());
    let mut doc = document();
    let mut broad = doc.runs[0].clone();
    broad.target = RunAccessTarget::TenantRuns;
    doc.runs.push(broad);
    assert!(PolicyArtifact::new(doc.clone()).is_err());
    doc.runs.reverse();
    assert!(PolicyArtifact::new(doc).is_err());
    let mut doc = document();
    doc.runs[0].target = RunAccessTarget::Submission(Digest::sha256(b"key"));
    assert!(PolicyArtifact::new(doc.clone()).is_ok());
    doc.runs[0].operation = RunPermission::Cancel;
    assert!(PolicyArtifact::new(doc).is_err());
}

#[test]
fn stable_selected_rule_evidence_survives_unrelated_refresh() {
    let doc = document();
    let context = context(&doc);
    let policy = policy(doc.clone());
    let before = policy.submission(&context).unwrap();
    let mut changed = doc.clone();
    changed.runs.clear();
    changed.valid_until =
        Timestamp::from_unix_micros(doc.valid_until.unix_micros() + 1_000_000).unwrap();
    let original = PolicyArtifact::new(doc).unwrap();
    let changed = PolicyArtifact::new(changed).unwrap();
    assert_ne!(original.digest(), changed.digest());
    policy.replace(1, changed, Duration::from_secs(60)).unwrap();
    let after = policy.submission(&context).unwrap();
    assert_eq!(before.authority(), after.authority());
    assert_eq!(before.budget_layers(), after.budget_layers());
    assert_eq!(
        before.budget_layers()[0].decision_digest(),
        before.authority().evidence().digest()
    );
    assert_eq!(
        before.budget_layers()[0].limits().tool_calls(),
        Some(ExecutionCount::new(2))
    );
    let mut changed_request = context.clone();
    changed_request.request = AgentRequest::new(
        context.request.input_schema().clone(),
        context.request.input().clone(),
        BudgetLimits::empty().with_tool_calls(ExecutionCount::new(1)),
    );
    assert_ne!(
        before.authority().evidence().digest(),
        policy
            .submission(&changed_request)
            .unwrap()
            .authority()
            .evidence()
            .digest()
    );
}

#[test]
fn selected_rule_scopes_budgets_and_policy_identity_change_evidence() {
    let doc = document();
    let context = context(&doc);
    let old = policy(doc.clone()).submission(&context).unwrap();
    for dimension in 0..3 {
        let mut changed = doc.clone();
        match dimension {
            0 => {
                changed.submissions[0].budget_limits =
                    BudgetLimits::empty().with_tool_calls(ExecutionCount::new(1));
            }
            1 => {
                changed.submissions[0].granted_scopes =
                    ScopeSet::try_new(["tool:read".parse().unwrap()]).unwrap();
            }
            _ => changed.policy = identity("other-policy"),
        }
        let new = policy(changed).submission(&context).unwrap();
        assert_ne!(
            old.authority().policy_digest(),
            new.authority().policy_digest()
        );
        assert_ne!(
            old.authority().evidence().digest(),
            new.authority().evidence().digest()
        );
    }
}

#[test]
fn evidence_schema_is_closed_and_cannot_contain_request_data() {
    let doc = document();
    let context = context(&doc);
    let grant = policy(doc).submission(&context).unwrap();
    let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
    let schema = register_agent_policy_evidence_schema(&mut schemas).unwrap();
    let registry = schemas.build().unwrap();
    registry
        .validate_bounded(&schema, grant.authority().evidence().data())
        .unwrap();
    let bytes = serde_json::to_vec(grant.authority().evidence()).unwrap();
    assert!(!String::from_utf8(bytes).unwrap().contains("private"));
    let mut leaked = serde_json::to_value(grant.authority().evidence().data()).unwrap();
    leaked["input"] = json!("secret");
    assert!(
        registry
            .validate_bounded(&schema, &BoundedJson::try_from_value(leaked).unwrap())
            .is_err()
    );
    assert_eq!(
        PolicyError.to_string(),
        "Agent resource policy is invalid or unavailable"
    );
}

#[test]
fn expiration_and_failed_refresh_never_restore_authority() {
    let doc = document();
    let context = context(&doc);
    let live = policy(doc.clone());
    for lease in [
        Duration::ZERO,
        AgentResourcePolicy::MAX_LEASE + Duration::from_nanos(1),
    ] {
        assert!(
            AgentResourcePolicy::new(PolicyArtifact::new(doc.clone()).unwrap(), lease).is_err()
        );
        assert!(
            live.replace(1, PolicyArtifact::new(doc.clone()).unwrap(), lease)
                .is_err()
        );
    }
    assert!(
        live.replace(
            0,
            PolicyArtifact::new(doc.clone()).unwrap(),
            Duration::from_secs(1)
        )
        .is_err()
    );
    assert!(
        live.replace(
            u64::MAX,
            PolicyArtifact::new(doc.clone()).unwrap(),
            Duration::from_secs(1)
        )
        .is_err()
    );
    assert_eq!(live.generation().unwrap(), 1);
    assert!(live.submission(&context).is_ok());
    live.0.write().unwrap().expires = Instant::now();
    assert_eq!(live.check_readiness(), Err(PolicyError));
    assert_eq!(
        live.submission(&context).unwrap_err(),
        AgentServiceAuthorizationError::Unavailable
    );
    let mut expired = doc.clone();
    expired.valid_until = Timestamp::from_unix_micros(now_micros().unwrap() - 1).unwrap();
    assert!(
        AgentResourcePolicy::new(
            PolicyArtifact::new(expired.clone()).unwrap(),
            Duration::from_secs(1)
        )
        .is_err()
    );
    assert!(
        live.replace(
            1,
            PolicyArtifact::new(expired).unwrap(),
            Duration::from_secs(1)
        )
        .is_err()
    );
    live.replace(
        1,
        PolicyArtifact::new(doc).unwrap(),
        Duration::from_secs(10),
    )
    .unwrap();
    assert!(live.check_readiness().is_ok());
}

#[test]
fn concurrent_refresh_has_exactly_one_winner_and_poison_fails_closed() {
    let doc = document();
    let policy = Arc::new(policy(doc.clone()));
    let threads: Vec<_> = (0..16)
        .map(|_| {
            let policy = policy.clone();
            let artifact = PolicyArtifact::new(doc.clone()).unwrap();
            std::thread::spawn(move || policy.replace(1, artifact, Duration::from_secs(30)).is_ok())
        })
        .collect();
    let winners = threads
        .into_iter()
        .map(|t| usize::from(t.join().unwrap()))
        .sum::<usize>();
    assert_eq!(winners, 1);
    assert_eq!(policy.generation().unwrap(), 2);
    let poisoned = policy.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poisoned.0.write().unwrap();
            panic!("intentional policy lock poison");
        })
        .join()
        .is_err()
    );
    assert_eq!(policy.check_readiness(), Err(PolicyError));
    assert_eq!(
        policy.submission(&context(&doc)).unwrap_err(),
        AgentServiceAuthorizationError::Unavailable
    );
}

#[test]
fn absolute_expiry_caps_lease_and_all_request_content_is_bound() {
    let mut doc = document();
    doc.valid_until = Timestamp::from_unix_micros(now_micros().unwrap() + 5_000_000).unwrap();
    let context = context(&doc);
    let policy = policy(doc.clone());
    assert!(policy.0.read().unwrap().expires <= Instant::now() + Duration::from_secs(5));
    let original = policy.submission(&context).unwrap();
    let mut changed = context.clone();
    changed.request = AgentRequest::new(
        context.request.input_schema().clone(),
        BoundedJson::try_from_value(json!({"private":"changed"})).unwrap(),
        BudgetLimits::empty(),
    );
    assert_ne!(
        original.authority().evidence().digest(),
        policy
            .submission(&changed)
            .unwrap()
            .authority()
            .evidence()
            .digest()
    );
    // Simulate a wall-clock deadline passing while the monotonic lease is fresh.
    doc.valid_until = Timestamp::from_unix_micros(now_micros().unwrap() - 1).unwrap();
    policy.0.write().unwrap().artifact = Arc::new(PolicyArtifact::new(doc).unwrap());
    assert_eq!(policy.check_readiness(), Err(PolicyError));
    assert_eq!(
        policy.submission(&context).unwrap_err(),
        AgentServiceAuthorizationError::Unavailable
    );
}
