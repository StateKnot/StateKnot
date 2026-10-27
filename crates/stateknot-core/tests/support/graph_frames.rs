// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Deterministic current-source data; schema digests are synthetic declarations.
use serde_json::Value;
use stateknot_core::*;

pub(crate) fn graph(name: &str) -> GraphReference {
    GraphReference::new(
        CapabilityIdentity::new(
            PrincipalIdentity::new(
                "https://issuer.example.com/frames".parse().unwrap(),
                "frame-contract".parse().unwrap(),
            ),
            CapabilityReference::new(name.parse().unwrap(), Version::new(1, 0, 0)),
        ),
        Digest::sha256(name),
        SchemaReference::new(
            "https://schemas.example.com/frame-state/1.0.0"
                .parse()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(b"synthetic frame state schema"),
        ),
    )
}

pub(crate) fn journal(ordinal: u64, tenant: &str) -> JournalHead {
    JournalHead::new(
        tenant.parse().unwrap(),
        "01912345-6789-7abc-8def-0123456789a1".parse().unwrap(),
        JournalSequence::new(ordinal).unwrap(),
        format!("01912345-6789-7abc-8def-{ordinal:012x}")
            .parse()
            .unwrap(),
        Timestamp::from_unix_micros(i64::try_from(ordinal).unwrap() * 1_000_000).unwrap(),
        Digest::sha256(ordinal.to_be_bytes()),
    )
}

pub(crate) fn checkpoint(
    graph: GraphReference,
    ordinal: u64,
    tenant: &str,
    value: Value,
) -> Checkpoint {
    let head = journal(ordinal, tenant);
    let state = CheckpointState::new(
        graph.state_schema().clone(),
        BoundedJson::try_from_value(value).unwrap(),
    )
    .unwrap();
    let write = CheckpointWrite::initial(
        head.tenant_id().clone(),
        head.run_id(),
        format!("01912345-6789-7abc-8def-{ordinal:012x}")
            .parse()
            .unwrap(),
        graph,
        state,
        ReadyNodes::try_new([NodeId::new("call").unwrap()]).unwrap(),
    )
    .unwrap();
    Checkpoint::commit(write, head).unwrap()
}

pub(crate) fn frame(parent: &Checkpoint, slot: &str) -> GraphFrameIdentity {
    GraphFrameIdentity::new(
        NodeActivation::for_ready_root(parent, NodeId::new("call").unwrap()).unwrap(),
        NodeId::new(slot).unwrap(),
        graph("child"),
    )
    .unwrap()
}

pub(crate) fn successor(parent: &Checkpoint, ordinal: u64, value: Value) -> Checkpoint {
    let write = CheckpointWrite::successor(
        format!("01912345-6789-7abc-8def-{ordinal:012x}")
            .parse()
            .unwrap(),
        parent,
        CheckpointState::new(
            parent.graph().state_schema().clone(),
            BoundedJson::try_from_value(value).unwrap(),
        )
        .unwrap(),
        ReadyNodes::try_new([NodeId::new("call").unwrap()]).unwrap(),
    )
    .unwrap();
    Checkpoint::commit(write, journal(ordinal, parent.tenant_id().as_str())).unwrap()
}
