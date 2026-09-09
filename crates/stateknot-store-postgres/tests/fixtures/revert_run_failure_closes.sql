-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0
-- Test-only reconstruction of the published v23 schema in an isolated database.
DROP TRIGGER runs_failure_close_guard ON stateknot.runs;
DROP TRIGGER runs_failure_close_complete ON stateknot.runs;
DROP TRIGGER child_ownership_failure_close_guard ON stateknot.child_run_ownership;
DROP TABLE stateknot.run_failure_closes;
DROP FUNCTION stateknot.guard_run_failure_close();
DROP FUNCTION stateknot.capture_failure_close_children();
DROP FUNCTION stateknot.guard_failure_close_spawn();
DROP FUNCTION stateknot.guard_failure_close_evidence();
DROP FUNCTION stateknot.complete_run_failure_close();
DELETE FROM _sqlx_migrations WHERE version=24;
