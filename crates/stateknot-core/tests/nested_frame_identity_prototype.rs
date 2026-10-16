// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Unpublished constructor prototype; no runtime or persistence support claim.
use serde::Serialize;
use serde_json::json;
use stateknot_core::{
    BoundedJson, CanonicalJson, CapabilityIdentity, CapabilityReference, Checkpoint, CheckpointId,
    CheckpointState, CheckpointWrite, Digest, GraphNamespace, GraphReference, JournalHead,
    JournalSequence, NodeActivation, NodeId, PrincipalIdentity, ReadyNodes, SchemaReference,
    Timestamp, Version,
};
use std::fmt::Write;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct FrameIdentity {
    origin: NodeActivation,
    slot: NodeId,
    target: GraphReference,
    namespace: GraphNamespace,
    digest: Digest,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScopeError {
    Depth,
    Namespace,
    CheckpointScope,
    CheckpointGraph,
    Owner,
}
fn hash(domain: &[u8], data: impl Serialize) -> Digest {
    let value = BoundedJson::try_from_value(serde_json::to_value(data).unwrap()).unwrap();
    let canonical = CanonicalJson::new(&value).unwrap();
    let mut bytes = domain.to_vec();
    bytes.extend_from_slice(canonical.as_bytes());
    Digest::sha256(bytes)
}
impl FrameIdentity {
    fn new(
        origin: NodeActivation,
        slot: NodeId,
        target: GraphReference,
    ) -> Result<Self, ScopeError> {
        if origin.base_checkpoint().graph().identity().owner() != target.identity().owner() {
            return Err(ScopeError::Owner);
        }
        let parent = origin.graph_namespace().as_str();
        let depth = if parent.is_empty() {
            0
        } else {
            parent.split('/').count()
        };
        if depth >= 7 {
            return Err(ScopeError::Depth);
        }
        let segment = hash(
            b"stateknot-graph-frame-namespace-v1\0",
            json!({"origin":origin,"slot":slot}),
        );
        let mut encoded = String::with_capacity(Digest::SHA256_LEN * 2);
        for byte in segment.as_bytes() {
            write!(encoded, "{byte:02x}").unwrap();
        }
        let segment = encoded;
        let namespace = GraphNamespace::new(if parent.is_empty() {
            segment
        } else {
            format!("{parent}/{segment}")
        })
        .map_err(|_| ScopeError::Namespace)?;
        let digest = hash(
            b"stateknot-graph-frame-identity-v1\0",
            json!({"origin":origin,"slot":slot,"target":target,"namespace":namespace}),
        );
        Ok(Self {
            origin,
            slot,
            target,
            namespace,
            digest,
        })
    }
    fn checkpoint(&self, checkpoint: &Checkpoint) -> Result<Digest, ScopeError> {
        if self.origin.tenant_id() != checkpoint.tenant_id()
            || self.origin.run_id() != checkpoint.run_id()
        {
            return Err(ScopeError::CheckpointScope);
        }
        if self.target != *checkpoint.graph() {
            return Err(ScopeError::CheckpointGraph);
        }
        Ok(hash(
            b"stateknot-graph-frame-checkpoint-v1\0",
            json!({"frame_identity":self.digest,"checkpoint_head":checkpoint.head()}),
        ))
    }
}
fn graph(name: &str) -> GraphReference {
    GraphReference::new(
        CapabilityIdentity::new(
            PrincipalIdentity::new(
                "https://issuer.example.com/prototype".parse().unwrap(),
                "nested-prototype".parse().unwrap(),
            ),
            CapabilityReference::new(name.parse().unwrap(), Version::new(1, 0, 0)),
        ),
        Digest::sha256(name),
        SchemaReference::new(
            "https://schemas.example.com/prototype-state/1.0.0"
                .parse()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(b"synthetic schema declaration"),
        ),
    )
}
fn checkpoint(graph: GraphReference, ordinal: u64, tenant: &str) -> Checkpoint {
    checkpoint_in_run(
        graph,
        ordinal,
        tenant,
        "01912345-6789-7abc-8def-0123456789a1",
    )
}
fn checkpoint_in_run(graph: GraphReference, ordinal: u64, tenant: &str, run: &str) -> Checkpoint {
    let id: CheckpointId = format!("01912345-6789-7abc-8def-{ordinal:012x}")
        .parse()
        .unwrap();
    let state = CheckpointState::new(
        graph.state_schema().clone(),
        BoundedJson::try_from_value(json!({"input":true})).unwrap(),
    )
    .unwrap();
    let head = JournalHead::new(
        tenant.parse().unwrap(),
        run.parse().unwrap(),
        JournalSequence::new(ordinal).unwrap(),
        format!("01912345-6789-7abc-8def-{ordinal:012x}")
            .parse()
            .unwrap(),
        Timestamp::from_unix_micros(i64::try_from(ordinal).unwrap() * 1_000_000).unwrap(),
        Digest::sha256(ordinal.to_be_bytes()),
    );
    let write = CheckpointWrite::initial(
        head.tenant_id().clone(),
        head.run_id(),
        id,
        graph,
        state,
        ReadyNodes::try_new([NodeId::new("call").unwrap()]).unwrap(),
    )
    .unwrap();
    Checkpoint::commit(write, head).unwrap()
}
#[test]
fn repeated_logical_call_is_stable_and_other_slot_or_parent_is_distinct() {
    let cp = checkpoint(graph("root"), 1, "tenant-prototype");
    let origin = NodeActivation::for_ready_root(&cp, NodeId::new("call").unwrap()).unwrap();
    let first = FrameIdentity::new(
        origin.clone(),
        NodeId::new("slot.a").unwrap(),
        graph("child"),
    )
    .unwrap();
    let repeated = FrameIdentity::new(
        origin.clone(),
        NodeId::new("slot.a").unwrap(),
        graph("child"),
    )
    .unwrap();
    assert_eq!(first, repeated);
    let other = FrameIdentity::new(origin, NodeId::new("slot.b").unwrap(), graph("child")).unwrap();
    assert_ne!(first.namespace, other.namespace);
    let next = checkpoint(graph("root"), 2, "tenant-prototype");
    let next = NodeActivation::for_ready_root(&next, NodeId::new("call").unwrap()).unwrap();
    assert_ne!(
        first.namespace,
        FrameIdentity::new(next, NodeId::new("slot.a").unwrap(), graph("child"))
            .unwrap()
            .namespace
    );
}
#[test]
fn full_digest_segments_fit_seven_levels_and_reject_an_eighth() {
    let cp = checkpoint(graph("root"), 1, "tenant-prototype");
    let mut origin = NodeActivation::for_ready_root(&cp, NodeId::new("call").unwrap()).unwrap();
    for depth in 1..=7 {
        let frame =
            FrameIdentity::new(origin.clone(), NodeId::new("slot").unwrap(), graph("child"))
                .unwrap();
        assert_eq!(frame.namespace.as_str().len(), depth * 64 + depth - 1);
        assert!(frame.namespace.as_str().len() <= GraphNamespace::MAX_LEN);
        origin = NodeActivation::new(
            cp.head(),
            frame.namespace,
            NodeId::new("call").unwrap(),
            Digest::sha256(u64::try_from(depth).unwrap().to_be_bytes()),
        );
    }
    assert_eq!(
        FrameIdentity::new(origin, NodeId::new("slot").unwrap(), graph("child")),
        Err(ScopeError::Depth)
    );
}
#[test]
fn checkpoint_binding_rejects_crossed_tenant_and_graph_and_changes_with_head() {
    let parent = checkpoint(graph("root"), 1, "tenant-prototype");
    let origin = NodeActivation::for_ready_root(&parent, NodeId::new("call").unwrap()).unwrap();
    let frame = FrameIdentity::new(origin, NodeId::new("slot").unwrap(), graph("child")).unwrap();
    let first = frame
        .checkpoint(&checkpoint(graph("child"), 2, "tenant-prototype"))
        .unwrap();
    let other = frame
        .checkpoint(&checkpoint(graph("child"), 3, "tenant-prototype"))
        .unwrap();
    assert_ne!(first, other);
    assert_eq!(
        frame.checkpoint(&checkpoint(graph("child"), 2, "other-tenant")),
        Err(ScopeError::CheckpointScope)
    );
    assert_eq!(
        frame.checkpoint(&checkpoint(graph("other"), 2, "tenant-prototype")),
        Err(ScopeError::CheckpointGraph)
    );
}

#[test]
fn checkpoint_binding_rejects_another_run_and_owner_is_never_implicitly_widened() {
    let parent = checkpoint(graph("root"), 1, "tenant-prototype");
    let origin = NodeActivation::for_ready_root(&parent, NodeId::new("call").unwrap()).unwrap();
    let frame =
        FrameIdentity::new(origin.clone(), NodeId::new("slot").unwrap(), graph("child")).unwrap();
    let other_run = checkpoint_in_run(
        graph("child"),
        2,
        "tenant-prototype",
        "01912345-6789-7abc-8def-0123456789a2",
    );
    assert_eq!(
        frame.checkpoint(&other_run),
        Err(ScopeError::CheckpointScope)
    );
    let other_owner = GraphReference::new(
        CapabilityIdentity::new(
            PrincipalIdentity::new(
                "https://issuer.example.com/prototype".parse().unwrap(),
                "other-owner".parse().unwrap(),
            ),
            CapabilityReference::new("child".parse().unwrap(), Version::new(1, 0, 0)),
        ),
        Digest::sha256(b"other graph"),
        graph("child").state_schema().clone(),
    );
    assert_eq!(
        FrameIdentity::new(origin, NodeId::new("slot").unwrap(), other_owner),
        Err(ScopeError::Owner)
    );
}
#[test]
fn same_logical_frame_key_with_changed_graph_pin_has_a_conflicting_identity() {
    let parent = checkpoint(graph("root"), 1, "tenant-prototype");
    let origin = NodeActivation::for_ready_root(&parent, NodeId::new("call").unwrap()).unwrap();
    let first =
        FrameIdentity::new(origin.clone(), NodeId::new("slot").unwrap(), graph("child")).unwrap();
    let altered = GraphReference::new(
        graph("child").identity().clone(),
        Digest::sha256(b"replaced definition"),
        graph("child").state_schema().clone(),
    );
    let changed = FrameIdentity::new(origin, NodeId::new("slot").unwrap(), altered).unwrap();
    assert_eq!(first.namespace, changed.namespace);
    assert_ne!(first.digest, changed.digest);
}
