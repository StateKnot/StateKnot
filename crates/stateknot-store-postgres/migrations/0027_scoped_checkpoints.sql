-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Scope integrity for RFC-0022. Existing canonical checkpoint bytes remain
-- unchanged; every existing row backfills root scope. These keys do not grant
-- frame admission, dispatch, return or lifecycle authority.
LOCK TABLE stateknot.run_checkpoints, stateknot.runs IN SHARE ROW EXCLUSIVE MODE;
ALTER TABLE stateknot.run_checkpoints
 ADD COLUMN graph_namespace text NOT NULL DEFAULT '',
 ADD COLUMN frame_identity_digest bytea,
 ADD COLUMN frame_checkpoint_digest bytea,
 ADD COLUMN frame_checkpoint_head_bytes bytea,
 ADD COLUMN frame_checkpoint_head_checksum bytea GENERATED ALWAYS AS (sha256(frame_checkpoint_head_bytes)) STORED,
 ADD CONSTRAINT run_checkpoints_frame_shape CHECK (
  (graph_namespace = '' AND frame_identity_digest IS NULL AND frame_checkpoint_digest IS NULL AND frame_checkpoint_head_bytes IS NULL)
  OR (graph_namespace ~ '^[0-9a-f]{64}(/[0-9a-f]{64}){0,6}$'
   AND octet_length(graph_namespace) <= 454
   AND frame_identity_digest IS NOT NULL AND octet_length(frame_identity_digest) = 32
   AND frame_checkpoint_digest IS NOT NULL AND octet_length(frame_checkpoint_digest) = 32
   AND frame_checkpoint_head_bytes IS NOT NULL AND octet_length(frame_checkpoint_head_bytes) BETWEEN 1 AND 65536)
 ),
 ADD CONSTRAINT run_checkpoints_scoped_position_unique UNIQUE (tenant_id, run_id, graph_namespace, superstep),
 ADD CONSTRAINT run_checkpoints_scoped_id_unique UNIQUE (tenant_id, run_id, graph_namespace, checkpoint_id),
 ADD CONSTRAINT run_checkpoints_scoped_identity_unique UNIQUE (tenant_id, run_id, graph_namespace, checkpoint_id, superstep, checkpoint_digest),
 ADD CONSTRAINT run_checkpoints_scoped_anchor_unique UNIQUE (tenant_id, run_id, graph_namespace, checkpoint_id, superstep, checkpoint_digest, journal_sequence, journal_event_id, journal_recorded_at, journal_digest),
 ADD CONSTRAINT run_checkpoints_scoped_parent_fk FOREIGN KEY (tenant_id, run_id, graph_namespace, parent_checkpoint_id, parent_superstep, parent_digest)
  REFERENCES stateknot.run_checkpoints (tenant_id, run_id, graph_namespace, checkpoint_id, superstep, checkpoint_digest) ON DELETE RESTRICT,
 ADD CONSTRAINT run_checkpoints_frame_parent_identity_unique UNIQUE (tenant_id, run_id, graph_namespace, checkpoint_id, superstep, checkpoint_digest, frame_identity_digest),
 ADD CONSTRAINT run_checkpoints_frame_parent_identity_fk FOREIGN KEY (tenant_id, run_id, graph_namespace, parent_checkpoint_id, parent_superstep, parent_digest, frame_identity_digest)
  REFERENCES stateknot.run_checkpoints (tenant_id, run_id, graph_namespace, checkpoint_id, superstep, checkpoint_digest, frame_identity_digest) ON DELETE RESTRICT;
ALTER TABLE stateknot.run_checkpoints DROP CONSTRAINT run_checkpoints_superstep_unique;

-- A generated constant cannot be widened by a writer supplying another scope.
ALTER TABLE stateknot.runs
 ADD COLUMN checkpoint_graph_namespace text GENERATED ALWAYS AS (''::text) STORED,
 ADD CONSTRAINT runs_root_checkpoint_fk FOREIGN KEY (tenant_id, run_id, checkpoint_graph_namespace, checkpoint_id, checkpoint_superstep, checkpoint_digest)
  REFERENCES stateknot.run_checkpoints (tenant_id, run_id, graph_namespace, checkpoint_id, superstep, checkpoint_digest) ON DELETE RESTRICT;
ALTER TABLE stateknot.agent_admissions
 ADD COLUMN checkpoint_graph_namespace text GENERATED ALWAYS AS (''::text) STORED,
 ADD CONSTRAINT agent_admissions_root_checkpoint_fk FOREIGN KEY (tenant_id, run_id, checkpoint_graph_namespace, checkpoint_id, checkpoint_superstep, checkpoint_digest)
  REFERENCES stateknot.run_checkpoints (tenant_id, run_id, graph_namespace, checkpoint_id, superstep, checkpoint_digest) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE stateknot.tool_invocations ADD CONSTRAINT tool_invocations_scoped_checkpoint_fk
 FOREIGN KEY (tenant_id, run_id, graph_namespace, base_checkpoint_id, base_superstep, base_checkpoint_digest)
 REFERENCES stateknot.run_checkpoints (tenant_id, run_id, graph_namespace, checkpoint_id, superstep, checkpoint_digest) ON DELETE RESTRICT;
ALTER TABLE stateknot.model_invocations ADD CONSTRAINT model_invocations_scoped_checkpoint_fk
 FOREIGN KEY (tenant_id, run_id, graph_namespace, base_checkpoint_id, base_superstep, base_checkpoint_digest)
 REFERENCES stateknot.run_checkpoints (tenant_id, run_id, graph_namespace, checkpoint_id, superstep, checkpoint_digest) ON DELETE RESTRICT;
ALTER TABLE stateknot.pending_node_results ADD CONSTRAINT pending_node_results_scoped_checkpoint_fk
 FOREIGN KEY (tenant_id, run_id, graph_namespace, base_checkpoint_id, base_superstep, base_checkpoint_digest, base_journal_sequence, base_journal_event_id, base_journal_recorded_at, base_journal_digest)
 REFERENCES stateknot.run_checkpoints (tenant_id, run_id, graph_namespace, checkpoint_id, superstep, checkpoint_digest, journal_sequence, journal_event_id, journal_recorded_at, journal_digest) ON DELETE RESTRICT;
ALTER TABLE stateknot.node_attempts ADD CONSTRAINT node_attempts_scoped_checkpoint_fk
 FOREIGN KEY (tenant_id, run_id, graph_namespace, base_checkpoint_id, base_superstep, base_checkpoint_digest, base_journal_sequence, base_journal_event_id, base_journal_recorded_at, base_journal_digest)
 REFERENCES stateknot.run_checkpoints (tenant_id, run_id, graph_namespace, checkpoint_id, superstep, checkpoint_digest, journal_sequence, journal_event_id, journal_recorded_at, journal_digest) ON DELETE RESTRICT;
ALTER TABLE stateknot.pending_node_result_consumptions ADD CONSTRAINT pending_node_result_consumptions_scoped_successor_fk
 FOREIGN KEY (tenant_id, run_id, graph_namespace, successor_checkpoint_id, successor_superstep, successor_checkpoint_digest, successor_journal_sequence, successor_journal_event_id, successor_journal_recorded_at, successor_journal_digest)
 REFERENCES stateknot.run_checkpoints (tenant_id, run_id, graph_namespace, checkpoint_id, superstep, checkpoint_digest, journal_sequence, journal_event_id, journal_recorded_at, journal_digest) ON DELETE RESTRICT;

-- The attempt is authoritative scope evidence for existing child ownership;
-- no ownership/Join decision is fabricated by this backfill.
ALTER TABLE stateknot.child_run_ownership ADD COLUMN parent_graph_namespace text NOT NULL DEFAULT '';
UPDATE stateknot.child_run_ownership AS owned SET parent_graph_namespace = attempt.graph_namespace
 FROM stateknot.node_attempts AS attempt
 WHERE attempt.tenant_id = owned.tenant_id AND attempt.run_id = owned.parent_run_id AND attempt.attempt_id = owned.parent_node_attempt_id;
ALTER TABLE stateknot.child_run_ownership ADD CONSTRAINT child_run_ownership_scoped_checkpoint_fk
 FOREIGN KEY (tenant_id, parent_run_id, parent_graph_namespace, parent_checkpoint_id, parent_checkpoint_superstep, parent_checkpoint_digest)
 REFERENCES stateknot.run_checkpoints (tenant_id, run_id, graph_namespace, checkpoint_id, superstep, checkpoint_digest) ON DELETE RESTRICT;
ALTER TABLE stateknot.child_run_joins ADD CONSTRAINT child_run_joins_scoped_checkpoint_fk
 FOREIGN KEY (tenant_id, parent_run_id, graph_namespace, base_checkpoint_id)
 REFERENCES stateknot.run_checkpoints (tenant_id, run_id, graph_namespace, checkpoint_id) ON DELETE RESTRICT;

ALTER TABLE stateknot.node_attempts
 ADD CONSTRAINT node_attempts_scoped_origin_unique UNIQUE (tenant_id, run_id, attempt_id, base_checkpoint_id, graph_namespace),
 ADD CONSTRAINT node_attempts_scoped_node_origin_unique UNIQUE (tenant_id, run_id, attempt_id, base_checkpoint_id, graph_namespace, node_id);
ALTER TABLE stateknot.child_run_ownership ADD CONSTRAINT child_run_ownership_scoped_attempt_fk
 FOREIGN KEY (tenant_id, parent_run_id, parent_node_attempt_id, parent_checkpoint_id, parent_graph_namespace)
 REFERENCES stateknot.node_attempts (tenant_id, run_id, attempt_id, base_checkpoint_id, graph_namespace) ON DELETE RESTRICT;
ALTER TABLE stateknot.child_run_joins ADD CONSTRAINT child_run_joins_scoped_attempt_fk
 FOREIGN KEY (tenant_id, parent_run_id, node_attempt_id, base_checkpoint_id, graph_namespace, node_id)
 REFERENCES stateknot.node_attempts (tenant_id, run_id, attempt_id, base_checkpoint_id, graph_namespace, node_id) ON DELETE RESTRICT;

-- Existing unscoped FKs remain in force. Root byte/identity checks, UUIDs,
-- journal anchors, predecessor steps and consumption identities stay closed.
