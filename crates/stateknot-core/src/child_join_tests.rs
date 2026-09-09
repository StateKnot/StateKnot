// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    ChildRunJoinBinding, ChildRunJoinError, ChildRunJoinRequest, NodeControl,
    NodeInvocationBindings, NodeStateChange, PendingNodeResultIntent,
};

fn key(value: &Fixture, slot: &str) -> ChildRunKey {
    ChildRunKey::new(value.key.parent().clone(), ChildRunSlot::new(slot).unwrap()).unwrap()
}
fn publication(value: &Fixture, sequence: u64) -> JournalHead {
    JournalHead::new(
        value.key.tenant_id().clone(),
        value.key.parent_run_id(),
        JournalSequence::new(sequence).unwrap(),
        EventId::generate(),
        now(),
        Digest::sha256("publication"),
    )
}

#[test]
fn join_request_canonical_order_bounds_and_tamper_checks() {
    let (value, _) = policy_fixture(true);
    let request = ChildRunJoinRequest::new([key(&value, "z"), key(&value, "A")]).unwrap();
    assert_eq!(request.keys()[0].slot().as_str(), "A");
    assert_eq!(
        request,
        ChildRunJoinRequest::new([key(&value, "A"), key(&value, "z")]).unwrap()
    );
    assert_eq!(
        from_value::<ChildRunJoinRequest>(to_value(&request).unwrap()).unwrap(),
        request
    );
    for field in ["digest", "activation_digest"] {
        let mut wire = to_value(&request).unwrap();
        wire[field] = to_value(Digest::sha256("tampered")).unwrap();
        assert!(from_value::<ChildRunJoinRequest>(wire).is_err());
    }
    let mut wire = to_value(&request).unwrap();
    wire["keys"].as_array_mut().unwrap().reverse();
    assert!(from_value::<ChildRunJoinRequest>(wire).is_err());
    let mut wire = to_value(&request).unwrap();
    wire["version"] = json!(2);
    assert!(from_value::<ChildRunJoinRequest>(wire).is_err());
    assert_eq!(ChildRunJoinRequest::new([]), Err(ChildRunJoinError::Bounds));
    assert_eq!(
        ChildRunJoinRequest::new([value.key.clone(), value.key.clone()]),
        Err(ChildRunJoinError::Duplicate)
    );
    let keys = (0..65)
        .map(|n| key(&value, &format!("s{n:02}")))
        .collect::<Vec<_>>();
    assert!(ChildRunJoinRequest::new(keys[..64].iter().cloned()).is_ok());
    assert_eq!(
        ChildRunJoinRequest::new(keys.clone()),
        Err(ChildRunJoinError::Bounds)
    );
    let mut wire = to_value(&request).unwrap();
    wire["keys"] = to_value(keys).unwrap();
    assert!(from_value::<ChildRunJoinRequest>(wire).is_err());
    let crossed = ChildRunKey::new(
        NodeActivation::new(
            value.checkpoint.head(),
            GraphNamespace::root(),
            value.key.parent().node_id().clone(),
            Digest::sha256("crossed"),
        ),
        value.key.slot().clone(),
    )
    .unwrap();
    assert_eq!(
        ChildRunJoinRequest::new([value.key.clone(), crossed]),
        Err(ChildRunJoinError::Scope)
    );
}

#[test]
fn join_terminal_order_digest_and_pending_result_binding_are_explicit() {
    let (value, _) = policy_fixture(true);
    let request = ChildRunJoinRequest::new([value.key.clone()]).unwrap();
    let terminal = settlement(&value, BudgetUsage::zero());
    let binding = ChildRunJoinBinding::new(request.clone(), [terminal.clone()]).unwrap();
    assert_eq!(
        from_value::<ChildRunJoinBinding>(to_value(&binding).unwrap()).unwrap(),
        binding
    );
    assert!(ChildRunJoinBinding::new(request.clone(), []).is_err());
    let two = ChildRunJoinRequest::new([key(&value, "a"), key(&value, "b")]).unwrap();
    assert_eq!(
        ChildRunJoinBinding::new(two, [terminal.clone(), terminal]),
        Err(ChildRunJoinError::Duplicate)
    );
    let mut wire = to_value(&binding).unwrap();
    wire["digest"] = to_value(Digest::sha256("changed")).unwrap();
    assert!(from_value::<ChildRunJoinBinding>(wire).is_err());
    let head = binding.head(publication(&value, 10)).unwrap();
    let plain = PendingNodeResultIntent::new(
        value.key.parent().clone(),
        NodeStateChange::Unchanged,
        NodeControl::Continue,
        NodeInvocationBindings::empty(),
    )
    .unwrap();
    let intent = plain.clone().with_child_join(head).unwrap();
    assert_ne!(plain.intent_digest(), intent.intent_digest());
    assert!(to_value(&plain).unwrap().get("child_join").is_none());
    assert_eq!(
        from_value::<PendingNodeResultIntent>(to_value(&intent).unwrap()).unwrap(),
        intent
    );
    let mut wire = to_value(&intent).unwrap();
    wire.as_object_mut().unwrap().remove("child_join");
    assert!(from_value::<PendingNodeResultIntent>(wire).is_err());
    let mut wire = to_value(&intent).unwrap();
    wire["child_join"]["binding_digest"] = to_value(Digest::sha256("changed")).unwrap();
    assert!(from_value::<PendingNodeResultIntent>(wire).is_err());
    let mut other = to_value(&plain).unwrap();
    other["activation"]["input_digest"] = to_value(Digest::sha256("other")).unwrap();
    let other: NodeActivation = from_value(other["activation"].clone()).unwrap();
    let crossed = PendingNodeResultIntent::new(
        other,
        NodeStateChange::Unchanged,
        NodeControl::Continue,
        NodeInvocationBindings::empty(),
    )
    .unwrap();
    assert!(
        crossed
            .with_child_join(intent.child_join().unwrap().clone())
            .is_err()
    );
}

#[test]
fn join_schema_exposes_wire_bounds_and_version() {
    let schema = to_value(schemars::schema_for!(ChildRunJoinRequest)).unwrap();
    assert_eq!(schema["properties"]["version"]["const"], json!(1));
    assert_eq!(schema["properties"]["keys"]["maxItems"], json!(64));
    assert_eq!(schema["properties"]["keys"]["minItems"], json!(1));
    let schema = to_value(schemars::schema_for!(ChildRunJoinBinding)).unwrap();
    assert_eq!(schema["properties"]["terminals"]["maxItems"], json!(64));
}

#[test]
fn join_publication_and_parent_result_obey_parent_journal_order() {
    let (value, _) = policy_fixture(true);
    let binding = ChildRunJoinBinding::new(
        ChildRunJoinRequest::new([value.key.clone()]).unwrap(),
        [settlement(&value, BudgetUsage::zero())],
    )
    .unwrap();
    let base = value.key.parent().base_checkpoint().journal_head();
    assert!(binding.head(base.clone()).is_err());
    let early = JournalHead::new(
        value.key.tenant_id().clone(),
        value.key.parent_run_id(),
        JournalSequence::new(10).unwrap(),
        EventId::generate(),
        "2030-01-01T00:00:01.000000Z".parse().unwrap(),
        Digest::sha256("early"),
    );
    assert_eq!(binding.head(early), Err(ChildRunJoinError::Clock));
    let published = publication(&value, 10);
    let head = binding.head(published.clone()).unwrap();
    let intent = PendingNodeResultIntent::new(
        value.key.parent().clone(),
        NodeStateChange::Unchanged,
        NodeControl::Continue,
        NodeInvocationBindings::empty(),
    )
    .unwrap()
    .with_child_join(head)
    .unwrap();
    let fence = crate::RunFence::new(
        value.key.tenant_id().clone(),
        value.key.parent_run_id(),
        crate::AttemptId::generate(),
        crate::FencingEpoch::new(1).unwrap(),
    );
    assert!(crate::PendingNodeResult::commit(intent.clone(), fence.clone(), published).is_err());
    let result = crate::PendingNodeResult::commit(intent, fence, publication(&value, 11)).unwrap();
    assert_eq!(
        from_value::<crate::PendingNodeResult>(to_value(&result).unwrap()).unwrap(),
        result
    );
    // Child-local sequences are deliberately not compared with the parent's.
    let mut terminal = to_value(binding.terminals()[0].clone()).unwrap();
    terminal["terminal"]["sequence"] = json!("500");
    let terminal: ChildRunBudgetSettlement = from_value(terminal).unwrap();
    let independent = ChildRunJoinBinding::new(binding.request().clone(), [terminal]).unwrap();
    assert!(independent.head(publication(&value, 10)).is_ok());
}
