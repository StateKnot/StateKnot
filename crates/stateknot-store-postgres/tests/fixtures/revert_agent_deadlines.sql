-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0
-- TEST-ONLY: remove v23 in an isolated database before exercising older upgrades.
DROP TRIGGER agent_admissions_deadline_capture ON stateknot.agent_admissions;
DROP FUNCTION stateknot.capture_agent_deadline();
DROP TRIGGER runs_agent_deadline_guard ON stateknot.runs;
DROP FUNCTION stateknot.guard_agent_deadline();
DROP INDEX stateknot.runs_due_agent_deadlines;
ALTER TABLE stateknot.runs DROP COLUMN agent_deadline_at;
DELETE FROM _sqlx_migrations WHERE version=23;
