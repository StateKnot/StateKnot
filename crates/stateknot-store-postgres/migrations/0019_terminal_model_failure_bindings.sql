-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- A response-phase model failure with exact usage can be deterministic node
-- input (for example, a provider-native structured-output repair). Preserve
-- that exact failed revision in the pending result barrier just as migration
-- 0017 already does for deterministic tool failures. Prepared and executing
-- revisions remain ineligible.

ALTER TABLE stateknot.pending_node_result_model_bindings
    DROP CONSTRAINT pending_node_result_model_bindings_status_valid;

ALTER TABLE stateknot.pending_node_result_model_bindings
    ADD CONSTRAINT pending_node_result_model_bindings_status_valid CHECK (
        invocation_status IN ('committed', 'failed')
    );
