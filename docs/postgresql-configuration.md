<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# PostgreSQL startup configuration

`PostgresStoreConfig` is the single validated startup boundary for environment,
builder, and explicit local-development setup. It does not change the durable
schema or weaken the existing PostgreSQL 16/17 qualification contract.

## Environment entry point

```rust,ignore
use stateknot_store_postgres::{PostgresStore, PostgresStoreConfig};

let config = PostgresStoreConfig::from_env()?;
let store = PostgresStore::connect_config(config).await?;
store.health_check().await?;
```

The environment contract is closed:

| Variable | Production meaning |
| --- | --- |
| `DATABASE_URL` | Required runtime-role connection URL. |
| `STATEKNOT_MIGRATION_DATABASE_URL` | Dedicated migration-role URL; required when production auto-migration is enabled. |
| `STATEKNOT_AUTO_MIGRATE` | Optional exact `true`/`false`; defaults to `false` in production. |
| `STATEKNOT_DEV_MODE` | Optional exact `true`/`false`; defaults to `false`. |

Non-Unicode values, unknown boolean spellings, empty URLs, missing production
migration credentials, and an exactly shared production runtime/migration URL
fail before network I/O. Error and `Debug` output never contains either URL.

Production uses verified TLS, a bounded 1–16 connection pool, and no implicit
migration by default. If startup migration is intentionally enabled, StateKnot
opens and closes a dedicated migration pool before connecting the runtime pool:

```text
DATABASE_URL=postgres://stateknot_runtime:...@db.example/stateknot
STATEKNOT_MIGRATION_DATABASE_URL=postgres://stateknot_migrator:...@db.example/stateknot
STATEKNOT_AUTO_MIGRATE=true
STATEKNOT_DEV_MODE=false
```

The two URLs must name separately privileged roles. The string inequality check
is only an early guard; deployment must still apply and audit the
[trusted-server role profile](postgresql-roles.md).

## Production builder

```rust,ignore
use stateknot_store_postgres::{
    PostgresStore, PostgresStoreConfig, PostgresStoreOptions,
};

let config = PostgresStoreConfig::builder(runtime_url)
    .with_options(PostgresStoreOptions::default().with_pool_size(2, 24))
    .with_migration_database_url(migration_url)
    .with_auto_migrate(true)
    .build()?;
let store = PostgresStore::connect_config(config).await?;
```

`PostgresStore::connect(runtime_url, options)` and
`PostgresStore::migrate_database(migration_url, options)` remain the preferred
fully explicit deployment sequence. The configuration object exists to remove
application boilerplate, not to merge privilege boundaries.

## Explicit local development

For a local PostgreSQL instance on a trusted machine:

```text
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stateknot
STATEKNOT_DEV_MODE=true
```

Development mode enables embedded migrations, reuses that one URL, disables
TLS, and limits the pool to four connections. It is deliberately unsafe for an
untrusted network and is never selected by omission.

The equivalent programmatic helper is:

```rust,ignore
let store = PostgresStore::connect_development(
    "postgres://postgres:postgres@127.0.0.1:5432/stateknot",
)
.await?;
```

Set `STATEKNOT_AUTO_MIGRATE=false` to retain development transport/pool defaults
while requiring an externally migrated schema. Startup still verifies every
embedded migration version/checksum and every required schema object.

## CI and secret handling

- inject URLs through the CI secret store; never commit `.env` files;
- use a disposable database per job and `STATEKNOT_DEV_MODE=true` only on the
  isolated job network;
- do not log the configuration with a generic serializer; StateKnot provides a
  redacted `Debug` implementation and no public URL accessor;
- close `PostgresStore` during joined shutdown so the pool cannot outlive the
  process role that owns it.

