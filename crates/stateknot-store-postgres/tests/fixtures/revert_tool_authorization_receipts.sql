-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE stateknot.tool_authorization_receipts;
DROP FUNCTION stateknot.reject_tool_authorization_receipt_mutation();
ALTER TABLE stateknot.tool_invocation_revisions
    DROP CONSTRAINT tool_invocation_revisions_attempt_event_unique;
ALTER TABLE stateknot.runs
    DROP CONSTRAINT runs_exact_thread_unique;
DELETE FROM _sqlx_migrations WHERE version=25;
