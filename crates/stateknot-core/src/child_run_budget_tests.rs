// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    BudgetUsage, ChildRunBudgetAccount, ChildRunBudgetError, ChildRunBudgetSettlement,
    CumulativeBudgetReservation, EventId, ExecutionCount, Failure, JournalHead, JournalSequence,
    KnownCosts, Money, RunFailure, RunLifecycle, RunTransition, TokenCount,
};
use proptest::prelude::*;

fn account(value: &Fixture, graph: &CompiledGraph) -> ChildRunBudgetAccount {
    ChildRunBudgetAccount::new(
        &value.parent,
        graph,
        value.checkpoint.journal_head().clone(),
        BudgetUsage::zero(),
    )
    .unwrap()
}

fn with_fraction(value: &mut Fixture, denominator: u64) {
    let mut limits = to_value(value.child.budget()).unwrap();
    for (field, amount) in limits.as_object_mut().unwrap() {
        if matches!(
            field.as_str(),
            "deadline" | "graph_depth" | "concurrent_branches" | "fan_out"
        ) {
            continue;
        }
        if field == "costs" {
            for cost in amount.as_array_mut().unwrap() {
                let units: u64 = cost["micro_units"].as_str().unwrap().parse().unwrap();
                cost["micro_units"] = json!((units / denominator).to_string());
            }
        } else {
            let units: u64 = amount.as_str().unwrap().parse().unwrap();
            *amount = json!((units / denominator).to_string());
        }
    }
    let request = AgentRequest::new(
        value.child.request().input_schema().clone(),
        value.child.request().input().clone(),
        from_value(limits).unwrap(),
    );
    value.replace_child(
        value.child.provenance().clone(),
        request,
        value.child.budget_layers().to_vec(),
        value.child.authority().clone(),
    );
}

fn next_candidate(value: &mut Fixture, ordinal: u64) {
    let mut provenance = to_value(value.child.provenance()).unwrap();
    provenance["run_id"] = json!(format!("01912345-6789-7abc-8def-{ordinal:012}"));
    value.replace_child(
        from_value(provenance).unwrap(),
        value.child.request().clone(),
        value.child.budget_layers().to_vec(),
        value.child.authority().clone(),
    );
    // Distinct lifetime activation identities; committed readiness remains a store check.
    value.key = ChildRunKey::new(
        NodeActivation::new(
            value.checkpoint.head(),
            GraphNamespace::root(),
            value.key.parent().node_id().clone(),
            Digest::sha256(ordinal.to_be_bytes()),
        ),
        value.key.slot().clone(),
    )
    .unwrap();
}

fn terminal_head(admission: &AgentAdmission, at: Timestamp) -> JournalHead {
    JournalHead::new(
        admission.intent().provenance().tenant_id().clone(),
        admission.intent().provenance().run_id(),
        JournalSequence::new(3).unwrap(),
        "01912345-6789-7abc-8def-0123456789a1".parse().unwrap(),
        at,
        Digest::sha256("terminal"),
    )
}

fn failed_lifecycle(admission: &AgentAdmission, usage: BudgetUsage) -> RunLifecycle {
    let fixture: Value =
        serde_json::from_str(include_str!("../tests/fixtures/core-failure-v1.json")).unwrap();
    let failure: Failure = from_value(fixture["failures"]["valid"][0].clone()).unwrap();
    RunLifecycle::admitted(
        admission.intent().provenance().clone(),
        admission.admitted_at(),
    )
    .apply(RunTransition::Start {
        started_at: admission.admitted_at(),
    })
    .unwrap()
    .apply(RunTransition::Fail {
        failure: RunFailure::new(failure, now(), usage).unwrap(),
    })
    .unwrap()
}

fn settlement(value: &Fixture, usage: BudgetUsage) -> ChildRunBudgetSettlement {
    let admission =
        AgentAdmission::commit(value.child.clone(), value.parent.admitted_at()).unwrap();
    let lifecycle = failed_lifecycle(&admission, usage);
    ChildRunBudgetSettlement::new(&admission, &lifecycle, terminal_head(&admission, now())).unwrap()
}

fn later_head(account: &ChildRunBudgetAccount) -> JournalHead {
    let wire = to_value(account).unwrap();
    let old: JournalHead = from_value(wire["direct_head"].clone()).unwrap();
    JournalHead::new(
        old.tenant_id().clone(),
        old.run_id(),
        JournalSequence::new(old.sequence().get() + 1).unwrap(),
        EventId::generate(),
        now(),
        Digest::sha256("direct"),
    )
}

#[test]
fn reserve_settle_retry_and_roundtrip_preserve_exact_accounting() {
    let (mut value, graph) = policy_fixture(true);
    with_fraction(&mut value, 2);
    let original = account(&value, &graph);
    let intent = value.intent().unwrap();
    let reserved = original.reserve(&intent, &graph, now()).unwrap();
    let usage = BudgetUsage::builder()
        .input_tokens(TokenCount::new(7))
        .graph_depth(ExecutionCount::new(8))
        .build()
        .unwrap();
    let evidence = settlement(&value, usage.clone());
    let settled = reserved.settle(&value.key, evidence.clone()).unwrap();
    assert_eq!(settled.accounted_usage().unwrap(), usage.cumulative_only());
    assert_eq!(settled.direct_usage(), &BudgetUsage::zero());
    assert_eq!(settled.children().len(), 1);
    assert_eq!(settled.settle(&value.key, evidence).unwrap(), settled);
    assert_eq!(
        settled
            .reserve(
                &intent,
                &graph,
                "2040-01-01T00:00:00.000000Z".parse().unwrap()
            )
            .unwrap(),
        settled
    );
    assert_eq!(reserved.accounted_usage().unwrap(), BudgetUsage::zero());
    assert!(
        settled.remaining(now()).unwrap().input_tokens()
            > reserved.remaining(now()).unwrap().input_tokens()
    );
    for snapshot in [original, reserved, settled] {
        let bytes = snapshot.canonical_bytes().unwrap();
        let restored: ChildRunBudgetAccount = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(restored, snapshot);
        restored.validate_for(&value.parent, &graph).unwrap();
    }
}

#[test]
fn candidate_ids_do_not_replace_first_reservation_and_changed_intents_conflict() {
    let (mut value, graph) = policy_fixture(true);
    with_fraction(&mut value, 2);
    let reserved = account(&value, &graph)
        .reserve(&value.intent().unwrap(), &graph, now())
        .unwrap();
    let first = value.child.provenance().clone();
    let key = value.key.clone();
    next_candidate(&mut value, 12);
    value.key = key;
    assert_ne!(value.child.provenance(), &first);
    let retry = reserved
        .reserve(&value.intent().unwrap(), &graph, now())
        .unwrap();
    assert_eq!(retry, reserved);
    assert_eq!(retry.entry(&value.key).unwrap().child(), &first);
    with_fraction(&mut value, 2);
    assert_eq!(
        reserved.reserve(&value.intent().unwrap(), &graph, now()),
        Err(ChildRunBudgetError::SpawnConflict)
    );
}

#[test]
fn sibling_reservations_and_direct_usage_share_the_same_capacity() {
    let (mut value, graph) = policy_fixture(true);
    with_fraction(&mut value, 2);
    let first = account(&value, &graph)
        .reserve(&value.intent().unwrap(), &graph, now())
        .unwrap();
    next_candidate(&mut value, 13);
    let second = first
        .reserve(&value.intent().unwrap(), &graph, now())
        .unwrap();
    next_candidate(&mut value, 14);
    assert!(
        second
            .reserve(&value.intent().unwrap(), &graph, now())
            .is_err()
    );
    let direct = first
        .observe_direct(
            later_head(&first),
            CumulativeBudgetReservation::from_budget(value.parent.intent().budget())
                .unwrap()
                .amount()
                .clone(),
        )
        .unwrap();
    assert!(direct.remaining(now()).is_err());
    assert!(
        direct
            .reserve(&value.intent().unwrap(), &graph, now())
            .is_err()
    );
    assert_eq!(first.children().len(), 1);
}

#[test]
fn settlement_preserves_known_overruns_and_rejects_unknown_cost() {
    let (mut value, graph) = policy_fixture(true);
    with_fraction(&mut value, 2);
    let reserved = account(&value, &graph)
        .reserve(&value.intent().unwrap(), &graph, now())
        .unwrap();
    let overrun = BudgetUsage::builder()
        .input_tokens(
            value
                .parent
                .intent()
                .budget()
                .input_tokens()
                .checked_add(TokenCount::new(1))
                .unwrap(),
        )
        .build()
        .unwrap();
    let settled = reserved
        .settle(&value.key, settlement(&value, overrun.clone()))
        .unwrap();
    assert_eq!(settled.delegated_usage().unwrap(), overrun);
    assert!(settled.remaining(now()).is_err());
    let admission =
        AgentAdmission::commit(value.child.clone(), value.parent.admitted_at()).unwrap();
    let unknown = BudgetUsage::builder()
        .unpriced_cost_events(ExecutionCount::new(1))
        .build()
        .unwrap();
    let lifecycle = failed_lifecycle(&admission, unknown);
    assert_eq!(
        ChildRunBudgetSettlement::new(&admission, &lifecycle, terminal_head(&admission, now())),
        Err(ChildRunBudgetError::UnpricedSettlement)
    );
    assert!(reserved.entry(&value.key).unwrap().settlement().is_none());
}

#[test]
fn settlement_requires_matching_child_and_immutable_terminal_evidence() {
    let (mut value, graph) = policy_fixture(true);
    with_fraction(&mut value, 2);
    let key = value.key.clone();
    let reserved = account(&value, &graph)
        .reserve(&value.intent().unwrap(), &graph, now())
        .unwrap();
    let evidence = settlement(&value, BudgetUsage::zero());
    let settled = reserved.settle(&key, evidence.clone()).unwrap();
    let changed = settlement(
        &value,
        BudgetUsage::builder()
            .input_tokens(TokenCount::new(1))
            .build()
            .unwrap(),
    );
    assert_eq!(
        settled.settle(&key, changed),
        Err(ChildRunBudgetError::SettlementConflict)
    );
    next_candidate(&mut value, 18);
    assert_eq!(
        reserved.settle(&key, settlement(&value, BudgetUsage::zero())),
        Err(ChildRunBudgetError::TerminalMismatch)
    );
    assert_eq!(
        reserved.settle(&value.key, evidence),
        Err(ChildRunBudgetError::ChildNotFound)
    );
}

#[test]
fn direct_observations_are_absolute_head_bound_and_monotonic() {
    let (value, graph) = policy_fixture(true);
    let initial = account(&value, &graph);
    let usage = BudgetUsage::builder()
        .input_tokens(TokenCount::new(10))
        .build()
        .unwrap();
    let head = later_head(&initial);
    let observed = initial.observe_direct(head.clone(), usage.clone()).unwrap();
    assert_eq!(
        observed.observe_direct(head.clone(), usage).unwrap(),
        observed
    );
    assert_eq!(
        observed.observe_direct(head, BudgetUsage::zero()),
        Err(ChildRunBudgetError::DirectObservationMismatch)
    );
    assert!(matches!(
        observed.observe_direct(later_head(&observed), BudgetUsage::zero()),
        Err(ChildRunBudgetError::Usage(_))
    ));
    assert_eq!(
        observed.accounted_usage().unwrap().input_tokens(),
        TokenCount::new(10)
    );
    assert_eq!(initial.accounted_usage().unwrap(), BudgetUsage::zero());
}

#[test]
fn all_monotonic_dimensions_and_currencies_refuse_erasure() {
    let (value, _) = policy_fixture(true);
    let mut wire = to_value(
        CumulativeBudgetReservation::from_budget(value.parent.intent().budget())
            .unwrap()
            .amount(),
    )
    .unwrap();
    for (_, amount) in wire.as_object_mut().unwrap() {
        if amount.is_string() {
            *amount = json!("1");
        }
    }
    let previous: BudgetUsage = from_value(wire.clone()).unwrap();
    previous.validate_monotonic_after(&previous).unwrap();
    for field in wire.as_object().unwrap().keys() {
        let mut regressed = wire.clone();
        regressed[field] = if field == "known_costs" {
            json!([])
        } else {
            json!("0")
        };
        // Inclusive/subset violations are already refused during decoding.
        if let Ok(regressed) = from_value::<BudgetUsage>(regressed) {
            assert!(
                regressed.validate_monotonic_after(&previous).is_err(),
                "{field}"
            );
        }
    }
    let currency = "USD".parse().unwrap();
    let before = BudgetUsage::builder()
        .known_costs(KnownCosts::try_new([Money::new(currency, 5)]).unwrap())
        .build()
        .unwrap();
    let after = BudgetUsage::builder()
        .known_costs(KnownCosts::try_new([Money::new(currency, 4)]).unwrap())
        .build()
        .unwrap();
    assert!(after.validate_monotonic_after(&before).is_err());
}

#[test]
fn unsupported_versions_tampering_duplicates_and_schema_bounds_fail_closed() {
    let (mut value, graph) = policy_fixture(true);
    with_fraction(&mut value, 2);
    let reserved = account(&value, &graph)
        .reserve(&value.intent().unwrap(), &graph, now())
        .unwrap();
    let wire = to_value(&reserved).unwrap();
    for (field, changed) in [
        ("version", json!(2)),
        ("maximum_children", json!(0)),
        ("digest", json!(Digest::sha256("tampered"))),
        ("unknown", json!(true)),
    ] {
        let mut altered = wire.clone();
        altered[field] = changed;
        assert!(
            from_value::<ChildRunBudgetAccount>(altered).is_err(),
            "{field}"
        );
    }
    let mut duplicated = wire.clone();
    duplicated["children"] = json!([wire["children"][0].clone(), wire["children"][0].clone()]);
    assert!(from_value::<ChildRunBudgetAccount>(duplicated).is_err());
    let mut too_many = wire;
    too_many["children"] = json!(vec![
        too_many["children"][0].clone();
        ChildRunBudgetAccount::MAX_CHILDREN + 1
    ]);
    assert!(from_value::<ChildRunBudgetAccount>(too_many).is_err());
    let schema = to_value(schemars::schema_for!(ChildRunBudgetAccount)).unwrap();
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(schema["properties"]["children"]["maxItems"], 256);
    assert_eq!(schema["properties"]["version"]["maximum"], 1);
}

#[test]
fn standalone_settlement_wire_refuses_nonterminal_unpriced_and_cross_run_evidence() {
    let (value, _) = policy_fixture(true);
    let evidence = settlement(&value, BudgetUsage::zero());
    let wire = to_value(&evidence).unwrap();
    for status in ["active", "pending", "waiting", "cancellation_requested"] {
        let mut altered = wire.clone();
        altered["status"] = json!(status);
        assert!(from_value::<ChildRunBudgetSettlement>(altered).is_err());
    }
    let mut altered = wire.clone();
    altered["usage"]["unpriced_cost_events"] = json!("1");
    assert!(from_value::<ChildRunBudgetSettlement>(altered).is_err());
    let mut altered = wire;
    altered["terminal"]["run_id"] = json!(value.parent.intent().provenance().run_id());
    assert!(from_value::<ChildRunBudgetSettlement>(altered).is_err());
}

#[test]
fn expiry_and_stale_clock_prevent_new_admission_but_not_exact_replay() {
    let (mut value, graph) = policy_fixture(true);
    with_fraction(&mut value, 2);
    let initial = account(&value, &graph);
    let intent = value.intent().unwrap();
    let reserved = initial.reserve(&intent, &graph, now()).unwrap();
    let expired = value.parent.intent().budget().deadline();
    assert!(initial.reserve(&intent, &graph, expired).is_err());
    assert_eq!(
        reserved.reserve(&intent, &graph, expired).unwrap(),
        reserved
    );
    let settled = reserved
        .settle(&value.key, settlement(&value, BudgetUsage::zero()))
        .unwrap();
    assert_eq!(
        settled.remaining(value.parent.admitted_at()),
        Err(ChildRunBudgetError::ClockBeforeEvidence)
    );
}

#[test]
fn lifetime_bound_does_not_recycle_settled_slots() {
    let (mut value, graph) = policy_fixture(true);
    with_fraction(&mut value, 2);
    let mut current = account(&value, &graph);
    for ordinal in 40..48 {
        next_candidate(&mut value, ordinal);
        current = current
            .reserve(&value.intent().unwrap(), &graph, now())
            .unwrap();
        current = current
            .settle(&value.key, settlement(&value, BudgetUsage::zero()))
            .unwrap();
    }
    assert_eq!(current.children().len(), 8);
    next_candidate(&mut value, 48);
    assert_eq!(
        current.reserve(&value.intent().unwrap(), &graph, now()),
        Err(ChildRunBudgetError::TooManyChildren)
    );
}

#[test]
fn parent_scope_declarations_and_child_identity_are_not_interchangeable() {
    let (mut value, graph) = policy_fixture(true);
    with_fraction(&mut value, 4);
    let initial = account(&value, &graph);
    let first = initial
        .reserve(&value.intent().unwrap(), &graph, now())
        .unwrap();
    let original_child = value.child.clone();
    next_candidate(&mut value, 72);
    value.child = original_child;
    assert_eq!(
        first.reserve(&value.intent().unwrap(), &graph, now()),
        Err(ChildRunBudgetError::DuplicateChild)
    );
    value.key = ChildRunKey::new(
        value.key.parent().clone(),
        ChildRunSlot::new("undeclared").unwrap(),
    )
    .unwrap();
    assert!(matches!(
        initial.reserve(&value.intent().unwrap(), &graph, now()),
        Err(ChildRunBudgetError::Declaration(_))
    ));
    let (other, legacy_graph) = policy_fixture(false);
    assert!(initial.validate_for(&other.parent, &legacy_graph).is_err());
    assert!(
        initial
            .reserve(&other.intent().unwrap(), &legacy_graph, now())
            .is_err()
    );
    assert!(
        ChildRunBudgetAccount::new(
            &other.parent,
            &legacy_graph,
            other.checkpoint.journal_head().clone(),
            BudgetUsage::zero()
        )
        .is_err()
    );
}

#[test]
fn child_settlement_order_is_canonical_and_subtree_usage_is_added_only_once() {
    let (mut value, graph) = policy_fixture(true);
    with_fraction(&mut value, 4);
    let first_intent = value.intent().unwrap();
    let first_key = value.key.clone();
    let first_evidence = settlement(
        &value,
        BudgetUsage::builder()
            .input_tokens(TokenCount::new(20))
            .fan_out(ExecutionCount::new(9))
            .build()
            .unwrap(),
    );
    next_candidate(&mut value, 82);
    let second_intent = value.intent().unwrap();
    let second_evidence = settlement(
        &value,
        BudgetUsage::builder()
            .input_tokens(TokenCount::new(30))
            .concurrent_branches(ExecutionCount::new(4))
            .build()
            .unwrap(),
    );
    let initial = account(&value, &graph);
    let forward = initial
        .reserve(&first_intent, &graph, now())
        .unwrap()
        .reserve(&second_intent, &graph, now())
        .unwrap();
    let reverse = initial
        .reserve(&second_intent, &graph, now())
        .unwrap()
        .reserve(&first_intent, &graph, now())
        .unwrap();
    assert_eq!(forward, reverse);
    let forward = forward
        .settle(&first_key, first_evidence.clone())
        .unwrap()
        .settle(&value.key, second_evidence.clone())
        .unwrap();
    let reverse = reverse
        .settle(&value.key, second_evidence)
        .unwrap()
        .settle(&first_key, first_evidence)
        .unwrap();
    assert_eq!(forward, reverse);
    let usage = forward.accounted_usage().unwrap();
    assert_eq!(usage.input_tokens(), TokenCount::new(50));
    assert_eq!(usage.fan_out(), ExecutionCount::ZERO);
    assert_eq!(usage.concurrent_branches(), ExecutionCount::ZERO);
}

#[test]
fn cumulative_projection_preserves_every_additive_field_and_unknown_price() {
    let (value, _) = policy_fixture(true);
    let mut wire = to_value(
        CumulativeBudgetReservation::from_budget(value.parent.intent().budget())
            .unwrap()
            .amount(),
    )
    .unwrap();
    for field in [
        "graph_depth",
        "concurrent_branches",
        "fan_out",
        "unpriced_cost_events",
    ] {
        wire[field] = json!("7");
    }
    let original: BudgetUsage = from_value(wire.clone()).unwrap();
    let projected = to_value(original.cumulative_only()).unwrap();
    for (field, amount) in wire.as_object().unwrap() {
        if matches!(
            field.as_str(),
            "graph_depth" | "concurrent_branches" | "fan_out"
        ) {
            assert_eq!(projected[field], "0");
        } else {
            assert_eq!(&projected[field], amount, "{field}");
        }
    }
}

#[test]
fn frozen_account_and_terminal_fingerprints() {
    let (mut value, graph) = policy_fixture(true);
    with_fraction(&mut value, 2);
    let initial = account(&value, &graph);
    let evidence = settlement(
        &value,
        BudgetUsage::builder()
            .input_tokens(TokenCount::new(7))
            .build()
            .unwrap(),
    );
    let settled = initial
        .reserve(&value.intent().unwrap(), &graph, now())
        .unwrap()
        .settle(&value.key, evidence.clone())
        .unwrap();
    assert_eq!(
        initial.digest().to_string(),
        "sha256:2fc105ff022b1dd295f7f7972c84d23cfb6494c7b1ada430bdcffc430782c01f"
    );
    assert_eq!(
        evidence.outcome_digest().to_string(),
        "sha256:1f39e28e8e92cafc14e295ff5cb27d9c94a0b510653992f1744f8b94a48233b1"
    );
    assert_eq!(
        settled.digest().to_string(),
        "sha256:83dc04d72040a354b4206a0f9594f46fb9a616ac9c55b150a09c9a2c43e8aaf1"
    );
}

#[test]
fn success_and_cancelled_children_keep_actual_usage_and_no_private_output() {
    let (mut value, graph) = policy_fixture(true);
    with_fraction(&mut value, 2);
    let reserved = account(&value, &graph)
        .reserve(&value.intent().unwrap(), &graph, now())
        .unwrap();
    let admission =
        AgentAdmission::commit(value.child.clone(), value.parent.admitted_at()).unwrap();
    let usage = BudgetUsage::builder()
        .model_attempts(ExecutionCount::new(1))
        .model_turns(ExecutionCount::new(1))
        .output_bytes(crate::ByteCount::new(100))
        .build()
        .unwrap();
    let active = RunLifecycle::admitted(
        admission.intent().provenance().clone(),
        admission.admitted_at(),
    )
    .apply(RunTransition::Start {
        started_at: admission.admitted_at(),
    })
    .unwrap();
    let result = crate::AgentResult::new(
        admission.intent().provenance().clone(),
        now(),
        admission.intent().descriptor().output_schema().clone(),
        BoundedJson::try_from(json!({"private":"never-in-budget-snapshot"})).unwrap(),
        crate::AgentArtifacts::empty(),
        usage.clone(),
    )
    .unwrap();
    let succeeded = active
        .clone()
        .apply(RunTransition::Succeed { result })
        .unwrap();
    let fixture: Value =
        serde_json::from_str(include_str!("../tests/fixtures/core-failure-v1.json")).unwrap();
    let mut failure = fixture["failures"]["valid"][0].clone();
    failure["category"] = json!("cancelled");
    let request = crate::RunCancellationRequest::new(from_value(failure).unwrap(), now()).unwrap();
    let cancelled = active
        .apply(RunTransition::RequestCancellation { request })
        .unwrap()
        .apply(RunTransition::ConfirmCancellation {
            completed_at: now(),
            usage: usage.clone(),
        })
        .unwrap();
    for lifecycle in [succeeded, cancelled] {
        let evidence =
            ChildRunBudgetSettlement::new(&admission, &lifecycle, terminal_head(&admission, now()))
                .unwrap();
        let settled = reserved.settle(&value.key, evidence).unwrap();
        assert_eq!(settled.accounted_usage().unwrap(), usage);
        assert!(
            !String::from_utf8(settled.canonical_bytes().unwrap())
                .unwrap()
                .contains("never-in-budget-snapshot")
        );
    }
}

#[test]
fn arithmetic_overflow_does_not_destroy_the_previous_reservation() {
    let (mut value, graph) = policy_fixture(true);
    with_fraction(&mut value, 2);
    let reserved = account(&value, &graph)
        .reserve(&value.intent().unwrap(), &graph, now())
        .unwrap();
    let observed = reserved
        .observe_direct(
            later_head(&reserved),
            BudgetUsage::builder()
                .input_tokens(TokenCount::new(1))
                .build()
                .unwrap(),
        )
        .unwrap();
    let huge = settlement(
        &value,
        BudgetUsage::builder()
            .input_tokens(TokenCount::MAX)
            .build()
            .unwrap(),
    );
    assert!(matches!(
        observed.settle(&value.key, huge),
        Err(ChildRunBudgetError::Usage(_))
    ));
    assert!(observed.entry(&value.key).unwrap().settlement().is_none());
    assert_eq!(
        observed.accounted_usage().unwrap().input_tokens(),
        TokenCount::new(1)
    );
}

#[test]
fn terminal_admission_and_direct_evidence_must_obey_parent_time_and_scope() {
    let (mut value, graph) = policy_fixture(true);
    with_fraction(&mut value, 2);
    let reserved = account(&value, &graph)
        .reserve(&value.intent().unwrap(), &graph, now())
        .unwrap();
    let early = AgentAdmission::commit(
        value.child.clone(),
        "2029-12-31T23:59:59.000000Z".parse().unwrap(),
    )
    .unwrap();
    let evidence = ChildRunBudgetSettlement::new(
        &early,
        &failed_lifecycle(&early, BudgetUsage::zero()),
        terminal_head(&early, now()),
    )
    .unwrap();
    assert_eq!(
        reserved.settle(&value.key, evidence),
        Err(ChildRunBudgetError::TerminalMismatch)
    );
    let head = later_head(&reserved);
    let foreign = JournalHead::new(
        "another-tenant".parse().unwrap(),
        head.run_id(),
        head.sequence(),
        head.event_id(),
        head.recorded_at(),
        head.digest(),
    );
    assert_eq!(
        reserved.observe_direct(foreign, BudgetUsage::zero()),
        Err(ChildRunBudgetError::DirectObservationMismatch)
    );
    let pending =
        RunLifecycle::admitted(value.child.provenance().clone(), value.parent.admitted_at());
    let admission =
        AgentAdmission::commit(value.child.clone(), value.parent.admitted_at()).unwrap();
    assert_eq!(
        ChildRunBudgetSettlement::new(
            &admission,
            &pending,
            terminal_head(&admission, value.parent.admitted_at())
        ),
        Err(ChildRunBudgetError::NotTerminal)
    );
}

proptest! {
    #[test]
    fn repeated_settlement_never_double_charges(tokens in 0_u64..500, replays in 0_usize..12) {
        let (mut value, graph) = policy_fixture(true);
        with_fraction(&mut value, 2);
        let reserved = account(&value, &graph).reserve(&value.intent().unwrap(), &graph, now()).unwrap();
        let evidence = settlement(&value, BudgetUsage::builder().input_tokens(TokenCount::new(tokens)).build().unwrap());
        let mut settled = reserved.settle(&value.key, evidence.clone()).unwrap();
        let original_digest = settled.digest();
        for _ in 0..replays { settled = settled.settle(&value.key, evidence.clone()).unwrap(); }
        prop_assert_eq!(settled.digest(), original_digest);
        prop_assert_eq!(settled.accounted_usage().unwrap().input_tokens(), TokenCount::new(tokens));
    }
}
