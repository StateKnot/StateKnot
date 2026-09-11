-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0
-- Schema-24 trusted-server ACL profile. Apply and audit share one allowlist.
-- Execute in one transaction with search_path=pg_catalog (see .psql wrapper).
DO $profile$
DECLARE
    runtime_name text := current_setting('stateknot.profile_runtime');
    retention_name text := current_setting('stateknot.profile_retention');
    apply_changes boolean := current_setting('stateknot.profile_apply')::boolean;
    owner_id oid := (SELECT oid FROM pg_roles WHERE rolname=current_user);
    runtime_id oid;
    retention_id oid;
    tables text[] := ARRAY[
        'agent_admissions', 'agent_submission_keys', 'artifact_parents', 'artifacts',
        'child_run_budget_accounts', 'child_run_cancellation_receipts', 'child_run_cancellations',
        'child_run_join_bindings', 'child_run_join_consumptions', 'child_run_joins',
        'child_run_ownership', 'child_run_settlements', 'child_run_terminals',
        'graph_definitions', 'interrupt_resolutions', 'model_invocation_revisions',
        'model_invocations', 'node_attempt_completions', 'node_attempts',
        'outbox_attempt_completions', 'outbox_attempts', 'outbox_deliveries',
        'outbox_destinations', 'pending_node_result_consumptions',
        'pending_node_result_model_bindings', 'pending_node_result_tool_bindings',
        'pending_node_results', 'run_attempt_claims', 'run_checkpoints', 'run_events',
        'run_failure_closes', 'run_quarantines', 'run_wait_registrations', 'runs',
        'scheduler_fairness_reservations', 'scheduler_fairness_shards', 'timer_firings',
        'tool_invocation_revisions', 'tool_invocations', 'wait_abandonments'
    ];
    updates jsonb := '{
      "runs": ["lifecycle_bytes","lifecycle_revision","lifecycle_status","changed_at",
        "journal_sequence","journal_event_id","journal_recorded_at","journal_digest",
        "fencing_epoch","lease_attempt_id","lease_acquired_at","lease_renewed_at",
        "lease_expires_at","quarantined_at","quarantine_reason","updated_at",
        "checkpoint_id","checkpoint_digest","checkpoint_superstep",
        "scheduler_ready_at","scheduler_not_before","wait_set_digest","unresolved_wait_count",
        "next_timer_due_at","next_interrupt_expiry_at","child_runtime_version","agent_deadline_at"],
      "tool_invocations": ["current_revision","current_status","current_attempt_id","current_record_digest","updated_at"],
      "model_invocations": ["current_revision","current_status","current_attempt_id","current_record_digest","updated_at"],
      "outbox_deliveries": ["status","attempt_count","current_attempt_id","current_epoch",
        "current_attempt_started_at","current_attempt_expires_at","next_attempt_at",
        "last_completion_digest","terminal_at","updated_at"],
      "run_wait_registrations": ["status","terminal_sequence","terminal_event_id",
        "terminal_recorded_at","terminal_event_digest","resolution_digest","firing_digest",
        "abandonment_digest","updated_at"],
      "scheduler_fairness_shards": ["next_slot","next_sequence","updated_at"],
      "child_run_budget_accounts": ["account_digest","account_bytes","journal_sequence",
        "journal_event_id","journal_recorded_at","journal_digest"],
      "child_run_ownership": ["settled","terminal_pending_at"],
      "child_run_cancellations": ["delivered_at"],
      "child_run_joins": ["ready_at","consumed_at"],
      "run_failure_closes": ["completed_at"]
    }';
    principal record;
    relation record;
    col record;
    routine record;
    table_name text;
    column_name text;
    privilege text;
    allowed boolean;
    is_runtime boolean;
    column_list text;
    actual_tables text[];
BEGIN
    IF runtime_name = retention_name OR runtime_name = current_user OR retention_name = current_user
       OR runtime_name = 'public' OR retention_name = 'public' THEN
        RAISE EXCEPTION 'profile requires three distinct dedicated roles';
    END IF;
    IF (SELECT count(*) FROM pg_roles WHERE rolname IN (runtime_name,retention_name)) <> 2 THEN
        RAISE EXCEPTION 'profile principals must already exist';
    END IF;
    SELECT oid INTO runtime_id FROM pg_roles WHERE rolname=runtime_name;
    SELECT oid INTO retention_id FROM pg_roles WHERE rolname=retention_name;
    IF EXISTS (SELECT FROM pg_roles WHERE oid=owner_id AND (rolsuper OR rolcreaterole OR rolreplication OR rolbypassrls))
       OR (SELECT datdba FROM pg_database WHERE datname=current_database()) <> owner_id
       OR (SELECT nspowner FROM pg_namespace WHERE nspname='stateknot') IS DISTINCT FROM owner_id
       OR (SELECT relowner FROM pg_class WHERE oid='public._sqlx_migrations'::regclass) <> owner_id THEN
        RAISE EXCEPTION 'run as the non-superuser dedicated database and migration owner';
    END IF;
    FOR principal IN SELECT * FROM pg_roles WHERE rolname IN (runtime_name,retention_name) LOOP
        IF principal.rolsuper OR principal.rolcreatedb OR principal.rolcreaterole
           OR principal.rolreplication OR principal.rolbypassrls OR NOT principal.rolcanlogin
           OR EXISTS (SELECT FROM pg_auth_members WHERE member=principal.oid)
           OR EXISTS (SELECT FROM pg_auth_members WHERE roleid=principal.oid)
           OR EXISTS (SELECT FROM pg_database WHERE datdba=principal.oid) THEN
            RAISE EXCEPTION 'profile principals must be unprivileged standalone login roles';
        END IF;
    END LOOP;
    IF (SELECT array_agg(version ORDER BY version) FROM public._sqlx_migrations WHERE success)
       IS DISTINCT FROM ARRAY(SELECT generate_series(1,24)::bigint)
       OR (SELECT count(*) FROM public._sqlx_migrations) <> 24 THEN
        RAISE EXCEPTION 'profile requires exact schema version 24';
    END IF;
    SELECT array_agg(relname::text ORDER BY relname COLLATE "C") INTO actual_tables
      FROM pg_class WHERE relnamespace='stateknot'::regnamespace AND relkind IN ('r','p','v','m','f','S');
    IF actual_tables IS DISTINCT FROM tables THEN
        RAISE EXCEPTION 'profile table inventory does not match schema 24';
    END IF;
    IF EXISTS (SELECT FROM pg_class WHERE relnamespace='stateknot'::regnamespace AND relowner<>owner_id)
       OR EXISTS (SELECT FROM pg_proc WHERE pronamespace='stateknot'::regnamespace AND (proowner<>owner_id OR prosecdef)) THEN
        RAISE EXCEPTION 'profile requires owner-controlled objects and invoker functions';
    END IF;
    -- Reject misspelled or removed update columns rather than silently weakening
    -- the audit. New columns receive no UPDATE permission automatically.
    FOR table_name, column_name IN SELECT key, jsonb_array_elements_text(value) FROM jsonb_each(updates) LOOP
        IF NOT EXISTS (SELECT FROM pg_attribute WHERE attrelid=format('stateknot.%I',table_name)::regclass
                       AND attname=column_name AND attnum>0 AND NOT attisdropped AND attgenerated='') THEN
            RAISE EXCEPTION 'unknown mutable column in role profile: %.%', table_name,column_name;
        END IF;
    END LOOP;
    IF apply_changes THEN
        EXECUTE format('REVOKE CREATE,TEMPORARY ON DATABASE %I FROM PUBLIC',current_database());
        EXECUTE format('REVOKE ALL ON DATABASE %I FROM %I,%I',current_database(),runtime_name,retention_name);
        EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I,%I',current_database(),runtime_name,retention_name);
        EXECUTE format('REVOKE ALL ON SCHEMA public,stateknot FROM PUBLIC,%I,%I',runtime_name,retention_name);
        EXECUTE format('GRANT USAGE ON SCHEMA public,stateknot TO %I,%I',runtime_name,retention_name);
        EXECUTE format('REVOKE ALL ON ALL TABLES IN SCHEMA stateknot FROM PUBLIC,%I,%I',runtime_name,retention_name);
        EXECUTE format('REVOKE ALL ON TABLE public._sqlx_migrations FROM PUBLIC,%I,%I',runtime_name,retention_name);
        FOR relation IN SELECT oid FROM pg_class WHERE (relnamespace='stateknot'::regnamespace AND relkind='r')
                        OR oid='public._sqlx_migrations'::regclass LOOP
            SELECT string_agg(quote_ident(attname),',') INTO column_list FROM pg_attribute
             WHERE attrelid=relation.oid AND attnum>0 AND NOT attisdropped;
            EXECUTE format('REVOKE ALL (%s) ON TABLE %s FROM PUBLIC,%I,%I',column_list,relation.oid::regclass,runtime_name,retention_name);
        END LOOP;
        EXECUTE format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA stateknot FROM PUBLIC,%I,%I',runtime_name,retention_name);
        EXECUTE format('GRANT EXECUTE ON FUNCTION stateknot.is_uuid_v7(uuid) TO %I',runtime_name);
        EXECUTE format('GRANT SELECT ON public._sqlx_migrations TO %I,%I',runtime_name,retention_name);
        FOREACH table_name IN ARRAY tables LOOP
            EXECUTE format('GRANT SELECT,INSERT ON stateknot.%I TO %I',table_name,runtime_name);
        END LOOP;
        FOR table_name, column_name IN SELECT key,jsonb_array_elements_text(value) FROM jsonb_each(updates) LOOP
            EXECUTE format('GRANT UPDATE (%I) ON stateknot.%I TO %I',column_name,table_name,runtime_name);
        END LOOP;
        -- PostgreSQL requires UPDATE on at least one column for FOR UPDATE SKIP
        -- LOCKED. This trusted destructive-maintenance principal is restricted
        -- to the disposable reservation ledger, never run/invocation evidence.
        EXECUTE format('GRANT SELECT,DELETE,UPDATE (reservation_id) ON stateknot.scheduler_fairness_reservations TO %I',retention_name);
        -- No automatic grants for future migrations. New objects require a
        -- reviewed profile update and reapplication before runtime rollout.
        EXECUTE format('ALTER DEFAULT PRIVILEGES REVOKE ALL ON TABLES FROM PUBLIC,%I,%I',runtime_name,retention_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA stateknot REVOKE ALL ON TABLES FROM PUBLIC,%I,%I',runtime_name,retention_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES REVOKE ALL ON FUNCTIONS FROM PUBLIC,%I,%I',runtime_name,retention_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA stateknot REVOKE ALL ON FUNCTIONS FROM PUBLIC,%I,%I',runtime_name,retention_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES REVOKE ALL ON SEQUENCES FROM PUBLIC,%I,%I',runtime_name,retention_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA stateknot REVOKE ALL ON SEQUENCES FROM PUBLIC,%I,%I',runtime_name,retention_name);
    END IF;
    -- Global defaults can be implicit (functions normally grant PUBLIC EXECUTE)
    -- or explicit; per-schema defaults add privileges rather than subtracting.
    FOREACH privilege IN ARRAY ARRAY['r','f','S'] LOOP
        IF EXISTS (
            SELECT FROM aclexplode(coalesce(
                (SELECT defaclacl FROM pg_default_acl WHERE defaclrole=owner_id
                 AND defaclnamespace=0 AND defaclobjtype=privilege::"char"),
                acldefault(privilege::"char",owner_id))) acl
            WHERE grantee=0 OR grantee IN (runtime_id,retention_id)
        ) OR EXISTS (
            SELECT FROM pg_default_acl d CROSS JOIN LATERAL aclexplode(d.defaclacl) acl
             WHERE defaclrole=owner_id AND defaclobjtype=privilege::"char"
               AND defaclnamespace='stateknot'::regnamespace
               AND (grantee=0 OR grantee IN (runtime_id,retention_id))
        ) THEN
            RAISE EXCEPTION 'unsafe default privileges for future profile objects';
        END IF;
    END LOOP;
    -- Effective privileges, not only direct ACL entries: PUBLIC grants and
    -- column grants are additive and must not bypass the allowlist.
    FOR principal IN SELECT oid,rolname FROM pg_roles WHERE rolname IN (runtime_name,retention_name) LOOP
        is_runtime := principal.rolname=runtime_name;
        IF NOT has_database_privilege(principal.oid,current_database(),'CONNECT')
           OR has_database_privilege(principal.oid,current_database(),'CREATE,TEMPORARY')
           OR has_database_privilege(principal.oid,current_database(),'CONNECT WITH GRANT OPTION')
           OR has_parameter_privilege(principal.oid,'session_replication_role','SET,ALTER SYSTEM')
           OR EXISTS (SELECT FROM pg_namespace WHERE nspname !~ '^pg_' AND has_schema_privilege(principal.oid,oid,'CREATE'))
           OR NOT has_schema_privilege(principal.oid,'stateknot','USAGE')
           OR NOT has_schema_privilege(principal.oid,'public','USAGE') THEN
            RAISE EXCEPTION 'unsafe database/schema privileges for profile principal';
        END IF;
        IF has_schema_privilege(principal.oid,'public','USAGE WITH GRANT OPTION')
           OR has_schema_privilege(principal.oid,'stateknot','USAGE WITH GRANT OPTION') THEN
            RAISE EXCEPTION 'unexpected schema grant option';
        END IF;
        FOR relation IN SELECT oid,relname FROM pg_class WHERE (relnamespace='stateknot'::regnamespace AND relkind='r')
                        OR oid='public._sqlx_migrations'::regclass LOOP
            FOREACH privilege IN ARRAY ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER'] LOOP
                allowed := CASE
                    WHEN relation.relname='_sqlx_migrations' THEN privilege='SELECT'
                    WHEN is_runtime THEN privilege IN ('SELECT','INSERT')
                    ELSE relation.relname='scheduler_fairness_reservations' AND privilege IN ('SELECT','DELETE') END;
                IF has_table_privilege(principal.oid,relation.oid,privilege) <> allowed
                   OR has_table_privilege(principal.oid,relation.oid,privilege||' WITH GRANT OPTION') THEN
                    RAISE EXCEPTION 'table privilege mismatch: %.%',relation.relname,privilege;
                END IF;
            END LOOP;
            IF current_setting('server_version_num')::int >= 170000
               AND has_table_privilege(principal.oid,relation.oid,'MAINTAIN') THEN
                RAISE EXCEPTION 'unexpected table maintenance privilege';
            END IF;
            FOR col IN SELECT attnum,attname FROM pg_attribute WHERE attrelid=relation.oid AND attnum>0 AND NOT attisdropped LOOP
                FOREACH privilege IN ARRAY ARRAY['SELECT','INSERT','UPDATE','REFERENCES'] LOOP
                    allowed := CASE
                        WHEN relation.relname='_sqlx_migrations' THEN privilege='SELECT'
                        WHEN is_runtime THEN privilege IN ('SELECT','INSERT') OR
                            (privilege='UPDATE' AND coalesce(updates->relation.relname ? col.attname,false))
                        ELSE relation.relname='scheduler_fairness_reservations' AND
                            (privilege='SELECT' OR (privilege='UPDATE' AND col.attname='reservation_id')) END;
                    IF has_column_privilege(principal.oid,relation.oid,col.attnum,privilege) <> allowed
                       OR has_column_privilege(principal.oid,relation.oid,col.attnum,privilege||' WITH GRANT OPTION') THEN
                        RAISE EXCEPTION 'column privilege mismatch: %.%.%',relation.relname,col.attname,privilege;
                    END IF;
                END LOOP;
            END LOOP;
        END LOOP;
        FOR routine IN SELECT oid FROM pg_proc WHERE pronamespace='stateknot'::regnamespace LOOP
            allowed := is_runtime AND routine.oid='stateknot.is_uuid_v7(uuid)'::regprocedure;
            IF has_function_privilege(principal.oid,routine.oid,'EXECUTE') <> allowed
               OR has_function_privilege(principal.oid,routine.oid,'EXECUTE WITH GRANT OPTION') THEN
                RAISE EXCEPTION 'function privilege mismatch';
            END IF;
        END LOOP;
    END LOOP;
END
$profile$;
