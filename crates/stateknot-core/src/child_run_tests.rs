// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    Checkpoint, EventId, FencingEpoch, GraphNamespace, JournalHead, JournalSequence,
    NodeAttemptStart, RunFence, Timestamp,
};
use serde_json::{Value, from_value, json, to_value};

fn activation() -> NodeActivation {
    let fixture: Value =
        serde_json::from_str(include_str!("../tests/fixtures/core-checkpoint-v1.json")).unwrap();
    let checkpoint: Checkpoint = from_value(fixture["checkpoints"][0].clone()).unwrap();
    NodeActivation::new(
        checkpoint.head(),
        GraphNamespace::root(),
        NodeId::new("authorize").unwrap(),
        Digest::sha256("authorize-input"),
    )
}

fn key() -> ChildRunKey {
    ChildRunKey::new(activation(), ChildRunSlot::new("risk-check").unwrap()).unwrap()
}

#[test]
fn ownership_key_roundtrip_and_golden_digest() {
    let key = key();
    assert_eq!(key.parent(), &activation());
    assert_eq!(key.tenant_id(), activation().tenant_id());
    assert_eq!(key.parent_run_id(), activation().run_id());
    assert_eq!(key.slot().as_str(), "risk-check");
    assert_eq!(
        from_value::<ChildRunKey>(to_value(&key).unwrap()).unwrap(),
        key
    );
    // Independently reproduced from the fixture with sorted-key canonical JSON
    // and SHA-256; changes require an explicit ownership-key wire review.
    assert_eq!(
        key.digest().to_string(),
        "sha256:21da0ea8e1d1e20c7745a8a4f1794bffc6f42f7d62c24145db141c2dfb855a48"
    );
}

#[test]
fn slot_uses_strict_bounded_node_grammar_on_all_paths() {
    for valid in ["a", "Risk.check_1-v2", &"x".repeat(ChildRunSlot::MAX_LEN)] {
        let slot = ChildRunSlot::new(valid).unwrap();
        assert_eq!(from_value::<ChildRunSlot>(json!(valid)).unwrap(), slot);
    }
    for invalid in [
        "",
        ".",
        "..",
        "../child",
        "a/b",
        " a",
        "a\n",
        "子",
        "_a",
        &"x".repeat(129),
    ] {
        assert!(ChildRunSlot::new(invalid).is_err(), "{invalid:?}");
        assert!(from_value::<ChildRunSlot>(json!(invalid)).is_err());
    }
    for invalid in [Value::Null, json!(42), json!({"slot":"child"})] {
        assert!(from_value::<ChildRunSlot>(invalid).is_err());
    }
    assert_ne!(
        ChildRunSlot::new("Risk").unwrap(),
        ChildRunSlot::new("risk").unwrap()
    );
}

#[test]
fn ownership_rejects_field_substitution_and_unknown_fields() {
    let wire = to_value(key()).unwrap();
    for (pointer, value) in [
        ("/slot", json!("other-slot")),
        ("/parent/node_id", json!("reserve-stock")),
        ("/parent/graph_namespace", json!("child")),
        ("/parent/input_digest", json!(Digest::sha256("other-input"))),
        (
            "/parent/base_checkpoint/digest",
            json!(Digest::sha256("other-checkpoint")),
        ),
        ("/digest", json!(Digest::sha256("forged-key"))),
    ] {
        let mut changed = wire.clone();
        *changed.pointer_mut(pointer).unwrap() = value;
        assert!(from_value::<ChildRunKey>(changed).is_err(), "{pointer}");
    }
    let mut changed = wire.clone();
    changed["attempt_id"] = json!("01912345-6789-7abc-8def-0123456789a1");
    assert!(from_value::<ChildRunKey>(changed).is_err());
    let mut changed = wire;
    changed["parent"]["fence"] = json!(1);
    assert!(from_value::<ChildRunKey>(changed).is_err());
}

#[test]
fn every_logical_identity_boundary_changes_the_key() {
    let base = key();
    let parent_wire = to_value(activation()).unwrap();
    for (pointer, value) in [
        ("/node_id", json!("reserve-stock")),
        ("/graph_namespace", json!("scope")),
        ("/input_digest", json!(Digest::sha256("new-input"))),
        (
            "/base_checkpoint/digest",
            json!(Digest::sha256("new-checkpoint")),
        ),
        (
            "/base_checkpoint/graph/definition_digest",
            json!(Digest::sha256("new-graph")),
        ),
    ] {
        let mut changed = parent_wire.clone();
        *changed.pointer_mut(pointer).unwrap() = value;
        let parent = from_value(changed).unwrap();
        let other = ChildRunKey::new(parent, base.slot().clone()).unwrap();
        assert_ne!(other.digest(), base.digest(), "{pointer}");
    }
    for field in ["tenant_id", "run_id"] {
        let mut changed = parent_wire.clone();
        let replacement = if field == "tenant_id" {
            json!("another-tenant")
        } else {
            json!("01912345-6789-7abc-8def-0123456789af")
        };
        changed["base_checkpoint"][field] = replacement.clone();
        changed["base_checkpoint"]["journal_head"][field] = replacement;
        let other = ChildRunKey::new(from_value(changed).unwrap(), base.slot().clone()).unwrap();
        assert_ne!(other.digest(), base.digest());
    }
    assert_ne!(
        ChildRunKey::new(activation(), ChildRunSlot::new("other").unwrap())
            .unwrap()
            .digest(),
        base.digest()
    );
}

#[test]
fn physical_retry_and_fence_takeover_preserve_logical_ownership() {
    let parent = activation();
    let mut starts = Vec::new();
    for suffix in [1_u8, 2] {
        let node_attempt = format!("01912345-6789-7abc-8def-0123456789a{suffix}")
            .parse()
            .unwrap();
        let worker_attempt = format!("01912345-6789-7abc-8def-0123456789b{suffix}")
            .parse()
            .unwrap();
        let fence = RunFence::new(
            parent.tenant_id().clone(),
            parent.run_id(),
            worker_attempt,
            FencingEpoch::new(u64::from(suffix)).unwrap(),
        );
        let journal = JournalHead::new(
            parent.tenant_id().clone(),
            parent.run_id(),
            JournalSequence::new(u64::from(suffix) + 1).unwrap(),
            format!("01912345-6789-7abc-8def-0123456789e{suffix}")
                .parse::<EventId>()
                .unwrap(),
            Timestamp::from_unix_micros(
                parent
                    .base_checkpoint()
                    .journal_head()
                    .recorded_at()
                    .unix_micros()
                    + i64::from(suffix) * 1_000_000,
            )
            .unwrap(),
            Digest::sha256([suffix]),
        );
        starts.push(NodeAttemptStart::new(parent.clone(), node_attempt, fence, journal).unwrap());
    }
    assert_ne!(starts[0].digest(), starts[1].digest());
    assert_eq!(starts[0].activation_digest(), starts[1].activation_digest());
    assert_eq!(
        ChildRunKey::new(starts[0].activation().clone(), key().slot().clone()).unwrap(),
        ChildRunKey::new(starts[1].activation().clone(), key().slot().clone()).unwrap()
    );
}

#[test]
fn child_wire_schema_is_closed_and_slot_is_bounded() {
    let schema = to_value(schemars::schema_for!(ChildRunKey)).unwrap();
    assert_eq!(schema["additionalProperties"], false);
    let schema = to_value(schemars::schema_for!(ChildRunSlot)).unwrap();
    assert_eq!(schema["maxLength"], ChildRunSlot::MAX_LEN);
    assert_eq!(schema["type"], "string");
}
