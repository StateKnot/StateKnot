-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- A deterministic tool failure is a terminal result that an Agent node may
-- consume into its next model transcript. Preserve the exact terminal status
-- in the binding instead of projecting every tool revision as "committed".
-- Existing rows retain their generated "committed" value when the expression
-- is dropped. Model bindings remain committed-only, but use the same explicit
-- insert shape as tool bindings.

ALTER TABLE stateknot.pending_node_result_tool_bindings
    DROP CONSTRAINT pending_node_result_tool_bindings_revision_fk;

ALTER TABLE stateknot.pending_node_result_model_bindings
    DROP CONSTRAINT pending_node_result_model_bindings_revision_fk;

ALTER TABLE stateknot.pending_node_result_tool_bindings
    ALTER COLUMN invocation_status DROP EXPRESSION;

ALTER TABLE stateknot.pending_node_result_model_bindings
    ALTER COLUMN invocation_status DROP EXPRESSION;

ALTER TABLE stateknot.pending_node_result_tool_bindings
    ADD CONSTRAINT pending_node_result_tool_bindings_status_valid CHECK (
        invocation_status IN ('committed', 'failed')
    ),
    ADD CONSTRAINT pending_node_result_tool_bindings_revision_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            invocation_id,
            invocation_revision,
            invocation_record_digest,
            invocation_status,
            invocation_journal_sequence,
            invocation_journal_recorded_at,
            invocation_journal_digest
        )
        REFERENCES stateknot.tool_invocation_revisions (
            tenant_id,
            run_id,
            invocation_id,
            revision,
            record_digest,
            status,
            journal_sequence,
            journal_recorded_at,
            journal_digest
        )
        ON DELETE RESTRICT;

ALTER TABLE stateknot.pending_node_result_model_bindings
    ADD CONSTRAINT pending_node_result_model_bindings_status_valid CHECK (
        invocation_status = 'committed'
    ),
    ADD CONSTRAINT pending_node_result_model_bindings_revision_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            invocation_id,
            invocation_revision,
            invocation_record_digest,
            invocation_status,
            invocation_journal_sequence,
            invocation_journal_recorded_at,
            invocation_journal_digest
        )
        REFERENCES stateknot.model_invocation_revisions (
            tenant_id,
            run_id,
            invocation_id,
            revision,
            record_digest,
            status,
            journal_sequence,
            journal_recorded_at,
            journal_digest
        )
        ON DELETE RESTRICT;
