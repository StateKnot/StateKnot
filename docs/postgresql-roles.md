<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Trusted PostgreSQL server roles

[简体中文](postgresql-roles.zh-CN.md)

`trusted-server-roles-v1` is an executable least-privilege deployment profile for
the **trusted server-side** PostgreSQL 16/17 provider at schema 24. It separates
migration, application runtime and fairness-reservation retention credentials.
It is not an untrusted-worker or tenant SQL security boundary: runtime still
has cross-tenant reads and control-plane projection writes. Do not distribute
any of these credentials to users, Tools, plugins or remote workers. RFC-0003's
worker-only procedure/service boundary and RFC-0004's full child qualification
remain release gates.

## Privilege contract

| Principal | Allowed | Not allowed |
|---|---|---|
| Migration owner | Own the dedicated database, schema, tables and invoker functions; explicit migrations and transactional profile apply/audit | Superuser, role administration, replication or RLS bypass in this profile; credentials in the runtime process |
| Runtime | CONNECT; schema USAGE; read migration metadata; SELECT/INSERT on the exact 40 framework tables; UPDATE only enumerated mutable projection columns; execute the UUID check function | DDL, temporary objects, DELETE/TRUNCATE, updating immutable evidence/identity columns, migration metadata writes, grant options, owner membership, disabling triggers |
| Retention | Read migration metadata; SELECT/DELETE on fairness reservations; UPDATE on that table's `reservation_id` only, required by PostgreSQL's row-lock permission check | Runtime writes, journal/checkpoint reads or mutation, shard-policy/cursor mutation, DDL or migration |

The retention identity is a **trusted destructive-maintenance account**.
PostgreSQL requires UPDATE privilege on at least one column for `FOR UPDATE
SKIP LOCKED`; that column grant can also be used for an actual ID update. This
principal can issue raw reservation deletes/ID updates, not just age-bounded
deletes. Only the existing `prune_scheduler_fairness_reservations` API enforces
the database-clock cutoff, one-hour minimum and bounded batch. Never expose its
credentials or raw SQL to tenants, and do not advertise SQL-enforced retention
windows. Other immutable ledgers receive no such UPDATE/DELETE grant.

Two redundant immutable-record locks were removed from node-attempt reads:
their callers already serialize starts, completions and Join registration with
the Run row lock. Submission mappings likewise use their existing transaction
advisory key lock, followed by the Run lock; no mapping UPDATE privilege is
needed. Fencing, journal compare-and-swap, unique constraints and lock order are
unchanged. The profile tests actual 24-way completion and submission races.

## Provision and apply

Use a **dedicated database and migration owner**, not a shared application's
database. The profile revokes PUBLIC CREATE/TEMPORARY on that database and
PUBLIC access to `public`/`stateknot`, their framework tables and functions. It
also changes this owner's default privileges in the current database. Other
applications must not depend on those grants. It does not change `pg_hba.conf`,
network rules, role passwords, global role attributes or object ownership.

Have your database administrator provision three distinct standalone LOGIN
roles, with NOSUPERUSER, NOCREATEDB, NOCREATEROLE, NOREPLICATION and NOBYPASSRLS.
The runtime/retention roles must have **no role memberships in either direction**,
including SET-only or administrative membership; NOINHERIT alone is not enough.
Role-group deployments do not qualify. Assign the database to the migration
role, create all framework objects through that same role, and provision
authentication through your secret manager or interactive `psql \password`.
Do not put passwords in versioned SQL, command-line URLs or logs. Managed
database products with mandatory role memberships must supply a separately
reviewed profile; this one rejects them rather than guessing their trust model.

1. Run the pinned binary's `PostgresStore::migrate_database` with the migration
   credential. It performs checksum-pinned migration and closes its DDL pool.
2. Configure libpq service `stateknot_migration` with the correct database,
   migration user, CA and `sslmode=verify-full`; use a restricted passfile or
   secret-managed authentication. Review the [SQL allowlist](../crates/stateknot-store-postgres/ops/trusted-role-profile.sql).
3. From the repository root, apply the reviewed profile:

```sh
PGSERVICE=stateknot_migration psql -X --no-password \
  --set=runtime_role=sk_runtime --set=retention_role=sk_retention --set=apply=true \
  --file=crates/stateknot-store-postgres/ops/trusted-role-profile.psql
```

Replace the role names with your provisioned identities. The wrapper quotes
them as SQL literals; dynamic identifiers in the profile use `%I`. Run it as
the **non-superuser migration owner**, not as a bootstrap administrator. Wrong
ownership, missing roles, unexpected tables/schema versions, elevated role
attributes, memberships and grant drift fail closed. An existing database
owned by another identity requires a separately reviewed ownership transition;
the script does not perform REASSIGN OWNED or broad cluster repair.

Apply explicitly revokes stale table **and column** grants, restores the exact
allowlist, then checks effective privileges before COMMIT. A post-grant failure
rolls everything back. No SECURITY DEFINER procedure or runtime role escalation
is introduced. The transaction has a 5-second lock and 30-second statement
timeout: investigate contention and retry the whole deployment step if it fails.

4. Audit without changing ACLs using the same command with `--set=apply=false`.
   This checks effective table/column/function/schema/database privileges,
   grant options, dangerous replication settings, memberships and both global
   and schema-local defaults. Treat nonzero exit as a rollout blocker.
5. Connect the application via `PostgresStore::connect` using only the runtime
   credential and default VerifyFull transport. Run the retention job in a
   separate process/pool with only its credential. `connect` verifies the exact
   schema, **not this ACL profile**: invoke the deployment audit before rollout
   and after every administrative privilege change.

## Upgrades, rotation and incident handling

- New tables/columns/functions receive no automatic runtime privilege. New
  migrations require a reviewed version-specific allowlist, migration, apply,
  audit and functional smoke tests before the new application starts. Schema
  24 is checked here; migration checksums are checked by the pinned provider.
- Reapplication is idempotent and is tested against populated durable histories.
  A read-only audit detects drift without silently fixing it; apply is a
  deliberate administrative change. Unrelated schema privilege drift is rejected
  and rolled back, not broadly revoked.
- Rotate passwords/certificates for each identity independently through the
  secret manager and drain/recreate its pools. For blue/green identities, review
  a dedicated grant/retirement change; the script does not discover or revoke
  forgotten historical identities automatically.
- A database owner/admin can bypass or change ACLs and data. ACL checks are not
  tamper-proof protection against a compromised DBA. Audit privileged sessions,
  restrict network access, encrypt backups, test restore/failover, and retain
  source-bound evidence. Do not repair drift by granting ALL to the runtime.

## Executable evidence and limits

The mandatory PostgreSQL 16/17 CI creates a unique disposable database and three
real LOGIN principals, migrates through the non-superuser owner and connects
runtime/retention with their own credentials (current_user = session_user).
It checks exact effective ACLs across all 40 tables and columns, SQLSTATE 42501
for forbidden operations, PUBLIC/column/grant-option/default drift, SET-only
membership rejection, atomic apply rollback and denied access to future objects.
It runs original-failure/child cancellation/settlement accounting, Join
publication/consumption/noninitial checkpoint recovery, Agent Service submission,
provider-native model/Tool ledger recovery and isolated bounded retention.
Provider responses are deterministic test fixtures, not live-provider attestations.

With an isolated loopback administrator URL in `STATEKNOT_TEST_DATABASE_URL`:

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::failure_close::role_profile::trusted_sql_role_profile_enforces_privileges_and_runs_durable_work \
  --nocapture --test-threads=1
```

The test refuses remote targets, does not change the supplied database's schema
or ACLs, and removes only its generated database/roles on success. On failure,
discard the isolated test container; never run it against a production cluster.
CI retains `trusted-role-profile-postgres-<version>-<run>` for 30 days with one
machine-readable passing record, successful test exit, exact checkout/tree,
Cargo.lock checksum and pinned image/toolchain/environment metadata. Missing
filters/evidence cannot qualify. Existing COMMIT/SIGKILL tests still run separately;
this is not the entire failure matrix under these roles, an RLS boundary, a
worker-only SQL capability, a failover/restore result or a capacity/SLO claim.

PostgreSQL references: [GRANT and additive column privileges](https://www.postgresql.org/docs/17/sql-grant.html),
[default privileges](https://www.postgresql.org/docs/17/sql-alterdefaultprivileges.html),
[effective privilege inspection](https://www.postgresql.org/docs/17/functions-info.html).
