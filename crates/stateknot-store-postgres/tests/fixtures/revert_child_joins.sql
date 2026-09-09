-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0
-- TEST-ONLY: isolated populated upgrade fixtures with no Join registrations.
DROP TRIGGER runs_child_join_guard ON stateknot.runs;
DROP TRIGGER child_join_spawn_guard ON stateknot.child_run_ownership;
DROP TRIGGER pending_results_child_join_consume ON stateknot.pending_node_results;
DROP TABLE stateknot.child_run_join_consumptions;
DROP TABLE stateknot.child_run_join_bindings;
DROP TABLE stateknot.child_run_joins;
DROP FUNCTION stateknot.guard_child_join_run();
DROP FUNCTION stateknot.guard_child_join_spawn();
DROP FUNCTION stateknot.consume_child_join_result();
DROP FUNCTION stateknot.guard_child_join_evidence();
DELETE FROM _sqlx_migrations WHERE version=22;
