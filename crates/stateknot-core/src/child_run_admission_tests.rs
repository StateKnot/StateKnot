// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    AgentAdmissionAuthority, AgentAdmissionBudgetLayer, AgentDescriptor, AgentRequest,
    AgentResultProvenance, BoundedJson, BudgetLimits, ChildRunSlot, GraphExecutionLimits,
    GraphNamespace, GraphNode, GraphReducerReference, GraphRoutes, JournalEventKind,
    JournalPayload, NodeId, ReadyNodes, SchemaReference, ScopeSet, Superstep,
};
use serde_json::{Value, from_value, json, to_value};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Fixture {
    parent: AgentAdmission,
    checkpoint: Checkpoint,
    child: AgentAdmissionIntent,
    graph: CompiledGraph,
    state: CheckpointState,
    key: ChildRunKey,
}

fn now() -> Timestamp {
    "2030-01-01T00:00:02.000000Z".parse().unwrap()
}

#[allow(clippy::too_many_lines)]
fn fixture() -> Fixture {
    let agent: Value =
        serde_json::from_str(include_str!("../tests/fixtures/core-agent-v1.json")).unwrap();
    let runtime: Value =
        serde_json::from_str(include_str!("../tests/fixtures/core-agent-runtime-v1.json")).unwrap();
    let checkpoints: Value =
        serde_json::from_str(include_str!("../tests/fixtures/core-checkpoint-v1.json")).unwrap();
    let journal: Value =
        serde_json::from_str(include_str!("../tests/fixtures/core-journal-v1.json")).unwrap();
    let checkpoint: Checkpoint = from_value(checkpoints["checkpoints"][0].clone()).unwrap();
    let descriptor: AgentDescriptor = from_value(agent["descriptors"]["valid"][0].clone()).unwrap();
    let provenance: AgentResultProvenance =
        from_value(runtime["result_provenances"]["valid"][0].clone()).unwrap();
    let mut request_wire = runtime["requests"]["valid"][0].clone();
    request_wire["budget_limits"]["deadline"] = json!("2031-01-01T00:00:00.000000Z");
    let request: AgentRequest = from_value(request_wire).unwrap();
    let mut budget_wire = runtime["base_budget_layers"][0].clone();
    budget_wire["deadline"] = json!("2031-01-01T00:00:00.000000Z");
    let limits: BudgetLimits = from_value(budget_wire).unwrap();
    let policy = checkpoint.graph().identity().clone();
    let evidence = JournalPayload::new(
        from_value(journal["schema_reference"].clone()).unwrap(),
        JournalEventKind::new(AgentAdmissionAuthority::EVIDENCE_KIND).unwrap(),
        BoundedJson::try_from(json!({"decision":"allow", "private":"do-not-log-policy"})).unwrap(),
    )
    .unwrap();
    let authority = AgentAdmissionAuthority::new(
        policy.owner().clone(),
        ScopeSet::empty(),
        policy.clone(),
        Digest::sha256("policy"),
        evidence,
    )
    .unwrap();
    let layer =
        AgentAdmissionBudgetLayer::new(policy.clone(), authority.evidence().digest(), limits)
            .unwrap();
    let parent_intent = AgentAdmissionIntent::new(
        provenance.clone(),
        descriptor.clone(),
        request.clone(),
        [layer.clone()],
        checkpoint.graph().clone(),
        authority.clone(),
    )
    .unwrap();
    let parent = AgentAdmission::commit(
        parent_intent,
        "2030-01-01T00:00:00.000000Z".parse().unwrap(),
    )
    .unwrap();

    let mut identity_wire = to_value(&policy).unwrap();
    identity_wire["capability"]["name"] = json!("child.graph");
    let graph = CompiledGraph::compile(
        from_value(identity_wire).unwrap(),
        descriptor.input_schema().clone(),
        checkpoint.graph().state_schema().clone(),
        checkpoint.graph().state_schema().clone(),
        descriptor.output_schema().clone(),
        GraphReducerReference::new(policy, Digest::sha256("child-reducer")),
        ReadyNodes::try_new([NodeId::new("work").unwrap()]).unwrap(),
        [GraphNode::new(
            NodeId::new("work").unwrap(),
            None,
            GraphRoutes::empty(),
            None,
            true,
        )
        .unwrap()],
        GraphExecutionLimits::new(Superstep::new(10).unwrap(), 1).unwrap(),
    )
    .unwrap();
    let mut ids = to_value(provenance).unwrap();
    ids["run_id"] = json!("01912345-6789-7abc-8def-0123456789f1");
    ids["thread_id"] = json!("01912345-6789-7abc-8def-0123456789f2");
    ids["invocation_id"] = json!("01912345-6789-7abc-8def-0123456789f3");
    let child = AgentAdmissionIntent::new(
        from_value(ids).unwrap(),
        descriptor,
        request,
        [layer],
        graph.reference(),
        authority,
    )
    .unwrap();
    let state = CheckpointState::new(
        graph.state_schema().clone(),
        BoundedJson::try_from(json!({"private":"do-not-log-state"})).unwrap(),
    )
    .unwrap();
    let key = ChildRunKey::new(
        NodeActivation::for_ready_root(&checkpoint, NodeId::new("authorize").unwrap()).unwrap(),
        ChildRunSlot::new("analysis").unwrap(),
    )
    .unwrap();
    Fixture {
        parent,
        checkpoint,
        child,
        graph,
        state,
        key,
    }
}

impl Fixture {
    fn intent(&self) -> Result<ChildRunAdmissionIntent, ChildRunAdmissionIntentError> {
        ChildRunAdmissionIntent::new(
            &self.parent,
            self.key.clone(),
            self.child.clone(),
            self.graph.clone(),
            self.state.clone(),
        )
    }
    fn replace_child(
        &mut self,
        provenance: AgentResultProvenance,
        request: AgentRequest,
        layers: Vec<AgentAdmissionBudgetLayer>,
        authority: AgentAdmissionAuthority,
    ) {
        self.child = AgentAdmissionIntent::new(
            provenance,
            self.child.descriptor().clone(),
            request,
            layers,
            self.graph.reference(),
            authority,
        )
        .unwrap();
    }
}

struct Schemas {
    count: AtomicUsize,
    reject: Option<usize>,
}
impl Schemas {
    fn allow() -> Self {
        Self {
            count: AtomicUsize::new(0),
            reject: None,
        }
    }
}
impl GraphSchemaValidator for Schemas {
    fn validate(
        &self,
        _: &SchemaReference,
        _: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError> {
        if Some(self.count.fetch_add(1, Ordering::SeqCst)) == self.reject {
            Err(GraphSchemaValidationError::Unavailable)
        } else {
            Ok(())
        }
    }
}

#[test]
fn child_preparation_roundtrip_and_fresh_validation() {
    let fixture = fixture();
    let intent = fixture.intent().unwrap();
    let restored: ChildRunAdmissionIntent =
        serde_json::from_slice(&intent.canonical_bytes().unwrap()).unwrap();
    assert_eq!(restored, intent);
    let schemas = Schemas::allow();
    restored
        .validate_for(&fixture.parent, &fixture.checkpoint, &schemas, now())
        .unwrap();
    assert_eq!(schemas.count.load(Ordering::SeqCst), 3);
    assert_eq!(intent.key(), &fixture.key);
    assert_eq!(intent.parent_admission_digest(), fixture.parent.digest());
    assert_eq!(intent.child(), &fixture.child);
    assert_eq!(intent.child_graph(), &fixture.graph);
    assert_eq!(intent.initial_state(), &fixture.state);
    assert_eq!(
        intent.spawn_digest().to_string(),
        "sha256:64a2d833b22e3f017d502b4a1c30281ef44680c3c0a710399b1eec02c5d595b8"
    );
}

#[test]
fn generated_candidate_ids_do_not_change_spawn_identity() {
    let mut fixture = fixture();
    let first = fixture.intent().unwrap();
    for field in ["run_id", "thread_id", "invocation_id"] {
        let mut ids = to_value(fixture.child.provenance()).unwrap();
        ids[field] = json!("01912345-6789-7abc-8def-0123456789ff");
        fixture.replace_child(
            from_value(ids).unwrap(),
            fixture.child.request().clone(),
            fixture.child.budget_layers().to_vec(),
            fixture.child.authority().clone(),
        );
        let next = fixture.intent().unwrap();
        assert_ne!(first.child().intent_digest(), next.child().intent_digest());
        assert_ne!(
            first.canonical_bytes().unwrap(),
            next.canonical_bytes().unwrap()
        );
        assert_eq!(first.spawn_digest(), next.spawn_digest());
    }
}

#[test]
fn semantic_request_state_policy_and_slot_changes_conflict() {
    let baseline = fixture().intent().unwrap().spawn_digest();
    for change in ["input", "budget", "policy", "state", "slot", "graph"] {
        let mut fixture = fixture();
        let mut request = fixture.child.request().clone();
        let mut layers = fixture.child.budget_layers().to_vec();
        let mut authority = fixture.child.authority().clone();
        match change {
            "input" => {
                request = AgentRequest::new(
                    request.input_schema().clone(),
                    BoundedJson::try_from(json!({"question":"another"})).unwrap(),
                    request.budget_limits().clone(),
                );
            }
            "budget" => {
                let layer = &layers[0];
                layers = vec![
                    AgentAdmissionBudgetLayer::new(
                        layer.source().clone(),
                        layer.decision_digest(),
                        layer
                            .limits()
                            .clone()
                            .with_graph_steps(crate::ExecutionCount::new(500)),
                    )
                    .unwrap(),
                ];
            }
            "policy" => {
                authority = AgentAdmissionAuthority::new(
                    authority.principal().clone(),
                    authority.granted_scopes().clone(),
                    authority.policy().clone(),
                    Digest::sha256("policy-v2"),
                    authority.evidence().clone(),
                )
                .unwrap();
            }
            "state" => {
                fixture.state = CheckpointState::new(
                    fixture.state.schema().clone(),
                    BoundedJson::try_from(json!({"private":"changed"})).unwrap(),
                )
                .unwrap();
            }
            "slot" => {
                fixture.key = ChildRunKey::new(
                    fixture.key.parent().clone(),
                    ChildRunSlot::new("other-slot").unwrap(),
                )
                .unwrap();
            }
            "graph" => {
                fixture.graph = CompiledGraph::compile(
                    fixture.graph.identity().clone(),
                    fixture.graph.input_schema().clone(),
                    fixture.graph.state_schema().clone(),
                    fixture.graph.update_schema().clone(),
                    fixture.graph.output_schema().clone(),
                    fixture.graph.reducer().clone(),
                    fixture.graph.entry_nodes().clone(),
                    fixture.graph.nodes().to_vec(),
                    GraphExecutionLimits::new(Superstep::new(11).unwrap(), 1).unwrap(),
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        fixture.replace_child(
            fixture.child.provenance().clone(),
            request,
            layers,
            authority,
        );
        assert_ne!(
            baseline,
            fixture.intent().unwrap().spawn_digest(),
            "{change}"
        );
    }
}

#[test]
fn cross_tenant_parent_id_reuse_and_graph_substitution_fail_closed() {
    for field in ["tenant_id", "run_id"] {
        let mut fixture = fixture();
        let mut ids = to_value(fixture.child.provenance()).unwrap();
        ids[field] = if field == "tenant_id" {
            json!("another-tenant")
        } else {
            json!(fixture.key.parent_run_id())
        };
        fixture.replace_child(
            from_value(ids).unwrap(),
            fixture.child.request().clone(),
            fixture.child.budget_layers().to_vec(),
            fixture.child.authority().clone(),
        );
        assert!(matches!(
            fixture.intent(),
            Err(ChildRunAdmissionIntentError::ChildScopeMismatch)
        ));
    }
    let mut fixture = fixture();
    fixture.child = AgentAdmissionIntent::new(
        fixture.child.provenance().clone(),
        fixture.child.descriptor().clone(),
        fixture.child.request().clone(),
        fixture.child.budget_layers().to_vec(),
        fixture.parent.intent().graph().clone(),
        fixture.child.authority().clone(),
    )
    .unwrap();
    assert!(matches!(
        fixture.intent(),
        Err(ChildRunAdmissionIntentError::ChildGraphMismatch)
    ));
}

#[test]
fn scope_principal_and_budget_widening_are_rejected() {
    for change in ["scopes", "principal", "budget"] {
        let mut fixture = fixture();
        let mut authority = fixture.child.authority().clone();
        let mut layers = fixture.child.budget_layers().to_vec();
        if change == "budget" {
            let layer = &layers[0];
            layers = vec![
                AgentAdmissionBudgetLayer::new(
                    layer.source().clone(),
                    layer.decision_digest(),
                    layer
                        .limits()
                        .clone()
                        .with_graph_steps(crate::ExecutionCount::new(1001)),
                )
                .unwrap(),
            ];
        } else {
            let mut wire = to_value(&authority).unwrap();
            if change == "scopes" {
                wire["granted_scopes"] = json!(["admin"]);
            } else {
                wire["principal"]["subject"] = json!("other-user");
            }
            authority = from_value(wire).unwrap();
        }
        fixture.replace_child(
            fixture.child.provenance().clone(),
            fixture.child.request().clone(),
            layers,
            authority,
        );
        let error = fixture.intent().unwrap_err();
        assert!(matches!(
            (change, error),
            ("scopes", ChildRunAdmissionIntentError::ScopeWidening)
                | ("principal", ChildRunAdmissionIntentError::PrincipalMismatch)
                | ("budget", ChildRunAdmissionIntentError::Budget(_))
        ));
    }
}

#[test]
fn fresh_admission_rechecks_ready_activation_deadline_and_clock() {
    let mut fixture = fixture();
    let good = fixture.intent().unwrap();
    assert!(matches!(
        good.validate_for(
            &fixture.parent,
            &fixture.checkpoint,
            &Schemas::allow(),
            fixture.child.budget().deadline()
        ),
        Err(ChildRunAdmissionIntentError::DeadlineExpired)
    ));
    assert!(matches!(
        good.validate_for(
            &fixture.parent,
            &fixture.checkpoint,
            &Schemas::allow(),
            fixture.parent.admitted_at()
        ),
        Err(ChildRunAdmissionIntentError::ClockBeforeCheckpoint)
    ));
    for node in ["not-ready", "authorize"] {
        fixture.key = ChildRunKey::new(
            NodeActivation::new(
                fixture.checkpoint.head(),
                GraphNamespace::root(),
                NodeId::new(node).unwrap(),
                Digest::sha256("forged-input"),
            ),
            ChildRunSlot::new("analysis").unwrap(),
        )
        .unwrap();
        let intent = fixture.intent().unwrap();
        let result = intent.validate_for(
            &fixture.parent,
            &fixture.checkpoint,
            &Schemas::allow(),
            now(),
        );
        assert!(matches!(
            (node, result),
            (
                "not-ready",
                Err(ChildRunAdmissionIntentError::ParentNotReady)
            ) | (
                "authorize",
                Err(ChildRunAdmissionIntentError::ParentActivationMismatch)
            )
        ));
    }
}

#[test]
fn restored_intent_requires_exact_trusted_parent_and_checkpoint() {
    let fixture = fixture();
    let intent = fixture.intent().unwrap();
    let unrelated = AgentAdmission::commit(
        fixture.parent.intent().clone(),
        "2029-12-31T23:59:59.000000Z".parse().unwrap(),
    )
    .unwrap();
    assert!(matches!(
        intent.validate_for(&unrelated, &fixture.checkpoint, &Schemas::allow(), now()),
        Err(ChildRunAdmissionIntentError::ParentMismatch)
    ));
    let checkpoints: Value =
        serde_json::from_str(include_str!("../tests/fixtures/core-checkpoint-v1.json")).unwrap();
    let later: Checkpoint = from_value(checkpoints["checkpoints"][1].clone()).unwrap();
    assert!(matches!(
        intent.validate_for(&fixture.parent, &later, &Schemas::allow(), now()),
        Err(ChildRunAdmissionIntentError::ParentCheckpointMismatch)
    ));
    // A structurally self-consistent envelope cannot authenticate a parent.
    let forged = ChildRunAdmissionIntent::build(
        intent.key.clone(),
        Digest::sha256("untrusted-parent"),
        intent.child.clone(),
        intent.child_graph.clone(),
        intent.initial_state.clone(),
    )
    .unwrap();
    let restored: ChildRunAdmissionIntent = from_value(to_value(forged).unwrap()).unwrap();
    assert!(matches!(
        restored.validate_for(
            &fixture.parent,
            &fixture.checkpoint,
            &Schemas::allow(),
            now()
        ),
        Err(ChildRunAdmissionIntentError::ParentMismatch)
    ));
}

#[test]
fn every_external_schema_is_required_and_errors_redact_payloads() {
    let fixture = fixture();
    let intent = fixture.intent().unwrap();
    for index in 0..3 {
        let schemas = Schemas {
            count: AtomicUsize::new(0),
            reject: Some(index),
        };
        assert!(matches!(
            intent.validate_for(&fixture.parent, &fixture.checkpoint, &schemas, now()),
            Err(ChildRunAdmissionIntentError::Schema(
                GraphSchemaValidationError::Unavailable
            ))
        ));
    }
    let debug = format!("{intent:?}");
    for secret in [
        "do-not-log-policy",
        "do-not-log-state",
        "INC-42",
        "Summarize",
    ] {
        assert!(!debug.contains(secret));
    }
}

#[test]
fn strict_wire_rejects_unknown_fields_and_integrity_drift() {
    let value = to_value(fixture().intent().unwrap()).unwrap();
    for pointer in [
        "/spawn_digest",
        "/parent_admission_digest",
        "/key/digest",
        "/child/intent_digest",
        "/child_graph/definition_digest",
        "/initial_state/digest",
    ] {
        let mut changed = value.clone();
        *changed.pointer_mut(pointer).unwrap() = json!(Digest::sha256("tampered"));
        assert!(
            from_value::<ChildRunAdmissionIntent>(changed).is_err(),
            "{pointer}"
        );
    }
    let mut changed = value.clone();
    changed["authorized"] = json!(true);
    assert!(from_value::<ChildRunAdmissionIntent>(changed).is_err());
    let mut changed = value;
    changed.as_object_mut().unwrap().remove("spawn_digest");
    assert!(from_value::<ChildRunAdmissionIntent>(changed).is_err());
    assert_eq!(
        to_value(schemars::schema_for!(ChildRunAdmissionIntent)).unwrap()["additionalProperties"],
        false
    );
}

#[test]
fn namespaced_parent_and_non_interoperable_state_are_rejected() {
    let mut fixture = fixture();
    fixture.key = ChildRunKey::new(
        NodeActivation::new(
            fixture.checkpoint.head(),
            GraphNamespace::new("nested").unwrap(),
            NodeId::new("authorize").unwrap(),
            Digest::sha256("input"),
        ),
        ChildRunSlot::new("analysis").unwrap(),
    )
    .unwrap();
    assert!(matches!(
        fixture.intent(),
        Err(ChildRunAdmissionIntentError::UnsupportedParentNamespace)
    ));
    // CheckpointState itself rejects non-I-JSON state before it reaches spawn hashing.
    assert!(
        CheckpointState::new(
            fixture.state.schema().clone(),
            BoundedJson::try_from(json!({"unsafe":9_007_199_254_740_993_u64})).unwrap()
        )
        .is_err()
    );
}

fn policy_fixture(declared: bool) -> (Fixture, CompiledGraph) {
    use crate::{ChildRunDeclaration, ChildRunTopologyLimits, GraphChildRunPolicy};
    let mut value = fixture();
    let node = value.key.parent().node_id().clone();
    let mut graph = CompiledGraph::compile(
        value.checkpoint.graph().identity().clone(),
        value.graph.input_schema().clone(),
        value.graph.state_schema().clone(),
        value.graph.update_schema().clone(),
        value.graph.output_schema().clone(),
        value.graph.reducer().clone(),
        ReadyNodes::try_new([node.clone()]).unwrap(),
        [GraphNode::new(node.clone(), None, GraphRoutes::empty(), None, true).unwrap()],
        value.graph.limits(),
    )
    .unwrap();
    if declared {
        graph = graph
            .with_child_runs(
                GraphChildRunPolicy::new(
                    ChildRunTopologyLimits::new(1, 8, 4).unwrap(),
                    [ChildRunDeclaration::new(
                        node.clone(),
                        value.key.slot().clone(),
                        value.child.descriptor(),
                        &value.graph,
                    )
                    .unwrap()],
                )
                .unwrap(),
            )
            .unwrap();
    }
    let old = value.parent.intent();
    value.parent = AgentAdmission::commit(
        AgentAdmissionIntent::new(
            old.provenance().clone(),
            old.descriptor().clone(),
            old.request().clone(),
            old.budget_layers().to_vec(),
            graph.reference(),
            old.authority().clone(),
        )
        .unwrap(),
        value.parent.admitted_at(),
    )
    .unwrap();
    value.checkpoint = Checkpoint::commit(
        crate::CheckpointWrite::initial(
            value.checkpoint.tenant_id().clone(),
            value.checkpoint.run_id(),
            value.checkpoint.checkpoint_id(),
            graph.reference(),
            value.checkpoint.state().clone(),
            graph.entry_nodes().clone(),
        )
        .unwrap(),
        value.checkpoint.journal_head().clone(),
    )
    .unwrap();
    value.key = ChildRunKey::new(
        NodeActivation::for_ready_root(&value.checkpoint, node).unwrap(),
        value.key.slot().clone(),
    )
    .unwrap();
    (value, graph)
}

#[test]
fn declaration_validation_denies_legacy_and_undeclared_slots() {
    use crate::ChildRunPolicyError;
    let (value, graph) = policy_fixture(false);
    assert_eq!(
        value.intent().unwrap().validate_declaration(&graph),
        Err(ChildRunPolicyError::DelegationNotDeclared)
    );
    let (mut value, graph) = policy_fixture(true);
    let intent = value.intent().unwrap();
    intent.validate_declaration(&graph).unwrap();
    intent
        .validate_for(&value.parent, &value.checkpoint, &Schemas::allow(), now())
        .unwrap();
    value.key = ChildRunKey::new(
        value.key.parent().clone(),
        ChildRunSlot::new("other").unwrap(),
    )
    .unwrap();
    assert_eq!(
        value.intent().unwrap().validate_declaration(&graph),
        Err(ChildRunPolicyError::DelegationNotDeclared)
    );
    assert_eq!(
        intent.validate_declaration(&value.graph),
        Err(ChildRunPolicyError::ParentGraphMismatch)
    );
}

#[test]
fn declaration_revalidation_rejects_same_identity_agent_replacement() {
    let (mut value, graph) = policy_fixture(true);
    let original = value.intent().unwrap();
    let restored: ChildRunAdmissionIntent =
        serde_json::from_slice(&original.canonical_bytes().unwrap()).unwrap();
    restored.validate_declaration(&graph).unwrap();
    let mut wire = to_value(value.child.descriptor()).unwrap();
    wire["budget_limits"]["model_turns"] = json!("1");
    let changed: AgentDescriptor = from_value(wire).unwrap();
    assert_eq!(
        changed.metadata().identity(),
        value.child.descriptor().metadata().identity()
    );
    value.child = AgentAdmissionIntent::new(
        value.child.provenance().clone(),
        changed,
        value.child.request().clone(),
        value.child.budget_layers().to_vec(),
        value.child.graph().clone(),
        value.child.authority().clone(),
    )
    .unwrap();
    let replaced = value.intent().unwrap();
    assert_ne!(original.spawn_digest(), replaced.spawn_digest());
    assert_eq!(
        replaced.validate_declaration(&graph),
        Err(crate::ChildRunPolicyError::TargetAgentMismatch)
    );
}
