// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Compound data integrity only: no store admission, lease or leaf authority.
use serde_json::{Value, json};
use stateknot_core::*;

#[path = "support/frame_calls.rs"]
mod data;

fn uuid(ordinal: u64) -> String {
    format!("01912345-6789-7abc-8def-{ordinal:012x}")
}
fn at(ordinal: i64) -> Timestamp {
    Timestamp::from_unix_micros(ordinal).unwrap()
}
fn head(ordinal: u64) -> JournalHead {
    JournalHead::new(
        "frame-entry-tenant".parse().unwrap(),
        uuid(100).parse().unwrap(),
        JournalSequence::new(ordinal).unwrap(),
        uuid(ordinal).parse().unwrap(),
        at(i64::try_from(ordinal).unwrap()),
        Digest::sha256(ordinal.to_be_bytes()),
    )
}
fn fence(epoch: u64) -> RunFence {
    RunFence::new(
        head(1).tenant_id().clone(),
        head(1).run_id(),
        uuid(200 + epoch).parse().unwrap(),
        FencingEpoch::new(epoch).unwrap(),
    )
}
fn base(graph: &CompiledGraph) -> Checkpoint {
    let write = CheckpointWrite::initial(
        head(1).tenant_id().clone(),
        head(1).run_id(),
        uuid(1).parse().unwrap(),
        graph.reference(),
        CheckpointState::new(
            data::schema(),
            BoundedJson::try_from(json!({"value":"entry-secret-state"})).unwrap(),
        )
        .unwrap(),
        graph.entry_nodes().clone(),
    )
    .unwrap();
    Checkpoint::commit(write, head(1)).unwrap()
}
fn graphs() -> (CompiledGraph, Checkpoint, GraphFrameCall, CompiledGraph) {
    let graph = data::parent(1).with_frame_calls(data::policy()).unwrap();
    let checkpoint = base(&graph);
    let call = graph.frame_calls().unwrap().calls()[0].clone();
    (graph, checkpoint, call, data::child())
}
fn plan() -> GraphFrameEntryPlan {
    let (graph, checkpoint, call, target) = graphs();
    GraphFrameEntryPlan::for_root(
        &call,
        &graph,
        &checkpoint,
        &target,
        uuid(2).parse().unwrap(),
        uuid(300).parse().unwrap(),
        fence(1),
    )
    .unwrap()
}
fn event(plan: &GraphFrameEntryPlan, ordinal: u64) -> JournalEvent {
    JournalEvent::commit(
        plan.append(uuid(400 + ordinal).parse().unwrap(), head(1))
            .unwrap(),
        at(10),
    )
    .unwrap()
}
fn domain(domain: &[u8], value: &Value) -> Digest {
    let mut bytes = domain.to_vec();
    bytes.extend(serde_json_canonicalizer::to_vec(value).unwrap());
    Digest::sha256(bytes)
}
fn record_digest(
    plan: &GraphFrameEntryPlan,
    event: &JournalEvent,
    start: &NodeAttemptStart,
    checkpoint: &GraphFrameCheckpoint,
) -> Digest {
    domain(
        b"stateknot-graph-frame-entry-record-v1\0",
        &json!({
            "intent_digest":plan.intent_digest(), "event":event.head(),
            "caller_start_digest":start.digest(), "checkpoint":checkpoint.head(),
        }),
    )
}

#[test]
fn exact_entry_uses_one_event_and_matches_independent_compound_preimages() {
    let plan = plan();
    let payload = plan.payload().unwrap();
    let mut intent = payload.data().as_value().clone();
    intent.as_object_mut().unwrap().remove("intent_digest");
    assert_eq!(
        plan.intent_digest(),
        domain(b"stateknot-graph-frame-entry-intent-v1\0", &intent)
    );
    assert_eq!(payload.kind().as_str(), "graph-frame-entered");
    assert!(
        !serde_json::to_string(&payload)
            .unwrap()
            .contains("entry-secret-state")
    );
    assert!(!format!("{plan:?}").contains("entry-secret-state"));
    let event = event(&plan, 1);
    let record = plan.materialize(&head(1), &event).unwrap();
    assert_eq!(record.intent_digest(), plan.intent_digest());
    assert_eq!(record.start().journal_head(), &event.head());
    assert_eq!(
        record.checkpoint().checkpoint().journal_head(),
        &event.head()
    );
    assert_eq!(record.start().activation(), plan.frame().origin());
    assert_eq!(record.start().fence(), plan.fence());
    assert_eq!(record.start().attempt_id(), plan.attempt_id());
    assert!(
        record
            .checkpoint()
            .checkpoint()
            .matches_write(plan.checkpoint())
    );
    assert_eq!(
        record.digest(),
        record_digest(&plan, &event, record.start(), record.checkpoint())
    );
    assert_ne!(record.digest(), record.start().digest());
    assert_ne!(record.digest(), record.checkpoint().checkpoint().digest());
    assert_eq!(
        plan.verify_committed(
            &head(1),
            &event,
            record.start(),
            record.checkpoint(),
            record.digest()
        )
        .unwrap(),
        record
    );
}

#[test]
fn retries_preserve_the_logical_frame_but_bind_every_physical_entry_fact() {
    let (graph, checkpoint, call, target) = graphs();
    let make = |checkpoint_id, attempt_id, fence| {
        GraphFrameEntryPlan::for_root(
            &call,
            &graph,
            &checkpoint,
            &target,
            checkpoint_id,
            attempt_id,
            fence,
        )
        .unwrap()
    };
    let original = plan();
    assert_eq!(
        original,
        make(
            original.checkpoint().checkpoint_id(),
            original.attempt_id(),
            original.fence().clone()
        )
    );
    for changed in [
        make(
            uuid(3).parse().unwrap(),
            original.attempt_id(),
            original.fence().clone(),
        ),
        make(
            original.checkpoint().checkpoint_id(),
            uuid(301).parse().unwrap(),
            original.fence().clone(),
        ),
        make(
            original.checkpoint().checkpoint_id(),
            original.attempt_id(),
            fence(2),
        ),
    ] {
        assert_eq!(original.frame(), changed.frame());
        assert_ne!(original.intent_digest(), changed.intent_digest());
    }
    let first = original
        .materialize(&head(1), &event(&original, 1))
        .unwrap();
    let second = original
        .materialize(&head(1), &event(&original, 2))
        .unwrap();
    assert_eq!(first.intent_digest(), second.intent_digest());
    assert_ne!(first.digest(), second.digest());
    assert_ne!(first.start().digest(), second.start().digest());
    assert_ne!(first.checkpoint().digest(), second.checkpoint().digest());
}

#[test]
fn observed_journal_may_advance_but_never_cross_or_replace_the_caller_base() {
    let plan = plan();
    for observed in [
        JournalHead::new(
            head(1).tenant_id().clone(),
            head(1).run_id(),
            JournalSequence::FIRST,
            head(1).event_id(),
            head(1).recorded_at(),
            Digest::sha256(b"changed base"),
        ),
        JournalHead::new(
            "other-tenant".parse().unwrap(),
            head(1).run_id(),
            JournalSequence::FIRST,
            head(1).event_id(),
            head(1).recorded_at(),
            head(1).digest(),
        ),
        JournalHead::new(
            head(1).tenant_id().clone(),
            uuid(101).parse().unwrap(),
            JournalSequence::FIRST,
            head(1).event_id(),
            head(1).recorded_at(),
            head(1).digest(),
        ),
        JournalHead::new(
            head(1).tenant_id().clone(),
            head(1).run_id(),
            JournalSequence::new(2).unwrap(),
            uuid(2).parse().unwrap(),
            at(0),
            Digest::sha256(b"old time"),
        ),
    ] {
        assert_eq!(
            plan.append(uuid(400).parse().unwrap(), observed),
            Err(GraphFrameEntryError::InvalidObservation)
        );
    }
    let observed = head(4);
    let event = JournalEvent::commit(
        plan.append(uuid(400).parse().unwrap(), observed.clone())
            .unwrap(),
        at(10),
    )
    .unwrap();
    let record = plan.materialize(&observed, &event).unwrap();
    assert_eq!(event.sequence(), JournalSequence::new(5).unwrap());
    assert_eq!(record.checkpoint().frame(), plan.frame());
}

#[test]
fn exact_kind_schema_payload_and_worker_source_are_all_required() {
    let plan = plan();
    let payload = plan.payload().unwrap();
    let changed_schema = SchemaReference::new(
        payload.schema().id().clone(),
        payload.schema().version(),
        Digest::sha256(b"other schema"),
    );
    let mut changed_data = payload.data().as_value().clone();
    changed_data["intent_digest"] = json!(Digest::sha256(b"other intent"));
    let mut unknown = payload.data().as_value().clone();
    unknown["extra"] = json!(false);
    let mut positional = payload
        .data()
        .as_value()
        .as_object()
        .unwrap()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    positional.reverse();
    let payloads = [
        JournalPayload::new(
            changed_schema,
            payload.kind().clone(),
            payload.data().clone(),
        )
        .unwrap(),
        JournalPayload::new(
            payload.schema().clone(),
            "node-started".parse().unwrap(),
            payload.data().clone(),
        )
        .unwrap(),
        JournalPayload::new(
            payload.schema().clone(),
            payload.kind().clone(),
            BoundedJson::try_from(changed_data).unwrap(),
        )
        .unwrap(),
        JournalPayload::new(
            payload.schema().clone(),
            payload.kind().clone(),
            BoundedJson::try_from(unknown).unwrap(),
        )
        .unwrap(),
        JournalPayload::new(
            payload.schema().clone(),
            payload.kind().clone(),
            BoundedJson::try_from(json!(positional)).unwrap(),
        )
        .unwrap(),
    ];
    for payload in payloads {
        let intent = JournalEventIntent::worker(
            head(1).tenant_id().clone(),
            head(1).run_id(),
            uuid(400).parse().unwrap(),
            fence(1),
            payload,
        )
        .unwrap();
        let event = JournalEvent::commit(
            JournalAppend::new(JournalExpectation::exact(head(1)), intent).unwrap(),
            at(10),
        )
        .unwrap();
        assert_eq!(
            plan.materialize(&head(1), &event),
            Err(GraphFrameEntryError::EventMismatch)
        );
    }
    for intent in [
        JournalEventIntent::control_plane(
            head(1).tenant_id().clone(),
            head(1).run_id(),
            uuid(400).parse().unwrap(),
            payload.clone(),
        )
        .unwrap(),
        JournalEventIntent::worker(
            head(1).tenant_id().clone(),
            head(1).run_id(),
            uuid(400).parse().unwrap(),
            fence(2),
            payload,
        )
        .unwrap(),
    ] {
        let event = JournalEvent::commit(
            JournalAppend::new(JournalExpectation::exact(head(1)), intent).unwrap(),
            at(10),
        )
        .unwrap();
        assert_eq!(
            plan.materialize(&head(1), &event),
            Err(GraphFrameEntryError::EventMismatch)
        );
    }
}

#[test]
fn a_valid_event_with_the_same_intent_cannot_replace_its_observed_predecessor() {
    let plan = plan();
    let good = plan.append(uuid(400).parse().unwrap(), head(1)).unwrap();
    let wrong = JournalHead::new(
        head(1).tenant_id().clone(),
        head(1).run_id(),
        JournalSequence::FIRST,
        head(1).event_id(),
        head(1).recorded_at(),
        Digest::sha256(b"replaced predecessor"),
    );
    for previous in [wrong, head(3)] {
        let append =
            JournalAppend::new(JournalExpectation::exact(previous), good.intent().clone()).unwrap();
        let event = JournalEvent::commit(append, at(10)).unwrap();
        assert!(event.matches_intent(good.intent()));
        assert_eq!(
            plan.materialize(&head(1), &event),
            Err(GraphFrameEntryError::EventMismatch)
        );
    }
}

#[test]
fn whole_compound_verification_rejects_independently_valid_changed_components() {
    let plan = plan();
    let event = event(&plan, 1);
    let record = plan.materialize(&head(1), &event).unwrap();
    let wrong_start = NodeAttemptStart::new(
        plan.frame().origin().clone(),
        uuid(301).parse().unwrap(),
        plan.fence().clone(),
        event.head(),
    )
    .unwrap();
    let wrong_write = CheckpointWrite::initial(
        plan.checkpoint().tenant_id().clone(),
        plan.checkpoint().run_id(),
        plan.checkpoint().checkpoint_id(),
        plan.checkpoint().graph().clone(),
        CheckpointState::new(
            data::schema(),
            BoundedJson::try_from(json!({"value":"substituted state"})).unwrap(),
        )
        .unwrap(),
        plan.checkpoint().ready_nodes().clone(),
    )
    .unwrap();
    let wrong_checkpoint = GraphFrameCheckpoint::new(
        plan.frame().clone(),
        Checkpoint::commit(wrong_write, event.head()).unwrap(),
    )
    .unwrap();
    for (start, checkpoint, projection) in [
        (
            &wrong_start,
            record.checkpoint(),
            record_digest(&plan, &event, &wrong_start, record.checkpoint()),
        ),
        (
            record.start(),
            &wrong_checkpoint,
            record_digest(&plan, &event, record.start(), &wrong_checkpoint),
        ),
        (record.start(), record.checkpoint(), record.start().digest()),
        (
            record.start(),
            record.checkpoint(),
            record.checkpoint().digest(),
        ),
        (
            record.start(),
            record.checkpoint(),
            Digest::sha256(b"arbitrary projection"),
        ),
    ] {
        assert_eq!(
            plan.verify_committed(&head(1), &event, start, checkpoint, projection),
            Err(GraphFrameEntryError::ComponentMismatch)
        );
    }
}

#[test]
fn scoped_entry_retains_the_real_outer_origin_and_unchanged_parent_bytes() {
    let middle = data::parent(1).with_frame_calls(data::policy()).unwrap();
    let outer_call = GraphFrameCall::new(
        "call".parse().unwrap(),
        "outer".parse().unwrap(),
        &middle,
        "return".parse().unwrap(),
    )
    .unwrap();
    let outer = data::parent_named("outer", 1)
        .with_frame_calls(GraphFrameCallPolicy::new(2, 16, [outer_call.clone()]).unwrap())
        .unwrap();
    let root = base(&outer);
    let outer_plan = GraphFrameEntryPlan::for_root(
        &outer_call,
        &outer,
        &root,
        &middle,
        uuid(2).parse().unwrap(),
        uuid(300).parse().unwrap(),
        fence(1),
    )
    .unwrap();
    let outer_event = event(&outer_plan, 1);
    let outer_record = outer_plan.materialize(&head(1), &outer_event).unwrap();
    let parent = outer_record.checkpoint();
    let bytes = serde_json::to_vec(parent).unwrap();
    let inner_plan = GraphFrameEntryPlan::for_frame(
        &middle.frame_calls().unwrap().calls()[0],
        &middle,
        parent,
        &data::child(),
        uuid(3).parse().unwrap(),
        uuid(301).parse().unwrap(),
        fence(1),
    )
    .unwrap();
    let inner_event = JournalEvent::commit(
        inner_plan
            .append(uuid(402).parse().unwrap(), outer_event.head())
            .unwrap(),
        at(11),
    )
    .unwrap();
    let inner = inner_plan
        .materialize(&outer_event.head(), &inner_event)
        .unwrap();
    assert_eq!(
        inner.start().activation(),
        &parent.activation("call".parse().unwrap()).unwrap()
    );
    assert_eq!(
        inner
            .checkpoint()
            .frame()
            .namespace()
            .as_str()
            .split('/')
            .count(),
        2
    );
    assert_eq!(
        inner.checkpoint().checkpoint().state(),
        parent.checkpoint().state()
    );
    assert_eq!(serde_json::to_vec(parent).unwrap(), bytes);
}

#[test]
fn crossed_fence_and_worker_attempt_reuse_fail_before_any_append() {
    let (graph, checkpoint, call, target) = graphs();
    let make = |attempt_id, fence| {
        GraphFrameEntryPlan::for_root(
            &call,
            &graph,
            &checkpoint,
            &target,
            uuid(2).parse().unwrap(),
            attempt_id,
            fence,
        )
    };
    assert_eq!(
        make(fence(1).attempt_id(), fence(1)),
        Err(GraphFrameEntryError::InvalidFence)
    );
    for crossed in [
        RunFence::new(
            "other".parse().unwrap(),
            head(1).run_id(),
            fence(1).attempt_id(),
            fence(1).epoch(),
        ),
        RunFence::new(
            head(1).tenant_id().clone(),
            uuid(101).parse().unwrap(),
            fence(1).attempt_id(),
            fence(1).epoch(),
        ),
    ] {
        assert_eq!(
            make(uuid(300).parse().unwrap(), crossed),
            Err(GraphFrameEntryError::InvalidFence)
        );
    }
    assert_eq!(
        GraphFrameEntryPlan::for_root(
            &call,
            &graph,
            &checkpoint,
            &data::parent(1),
            uuid(2).parse().unwrap(),
            uuid(300).parse().unwrap(),
            fence(1)
        ),
        Err(GraphFrameEntryError::InvalidCall)
    );
}

#[test]
fn event_schema_is_closed_versioned_and_digest_bound() {
    let (reference, document) = GraphFrameEntryPlan::event_schema().unwrap();
    assert_eq!(
        reference.id().as_str(),
        "https://stknot.com/schemas/core/graph-frame-entry-event/1.0.0"
    );
    assert_eq!(reference.version(), Version::new(1, 0, 0));
    assert_eq!(
        reference.digest(),
        "sha256:a2f2d5679fdf7c4dc9a7fa906121e41b9571177aee0d9e28cb29f1d921fa4ea3"
            .parse()
            .unwrap()
    );
    assert_eq!(document["$id"], json!(reference.id().as_str()));
    assert_eq!(
        reference.digest(),
        Digest::sha256(serde_json_canonicalizer::to_vec(&document).unwrap())
    );
    assert_eq!(document["type"], json!("object"));
    assert_eq!(document["additionalProperties"], json!(false));
    assert_eq!(document["properties"]["version"]["minimum"], json!(1));
    assert_eq!(document["properties"]["version"]["maximum"], json!(1));
    assert_eq!(document["required"].as_array().unwrap().len(), 7);
    assert_eq!(plan().payload().unwrap().schema(), &reference);
}
