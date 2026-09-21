<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# PostgreSQL 启动配置

`PostgresStoreConfig` 是环境变量、Builder 与显式本地开发配置共用的唯一校验边界；
它不会改变持久化 Schema，也不会削弱现有 PostgreSQL 16/17 资格验证契约。

## 环境变量入口

```rust,ignore
use stateknot_store_postgres::{PostgresStore, PostgresStoreConfig};

let config = PostgresStoreConfig::from_env()?;
let store = PostgresStore::connect_config(config).await?;
store.health_check().await?;
```

环境变量契约是封闭的：

| 变量 | 生产语义 |
| --- | --- |
| `DATABASE_URL` | 必填的 Runtime Role 连接 URL。 |
| `STATEKNOT_MIGRATION_DATABASE_URL` | 独立 Migration Role URL；生产环境启用自动迁移时必填。 |
| `STATEKNOT_AUTO_MIGRATE` | 可选且只能是 `true`/`false`；生产默认 `false`。 |
| `STATEKNOT_DEV_MODE` | 可选且只能是 `true`/`false`；默认 `false`。 |

非 Unicode 值、未知布尔值、空 URL、缺少生产迁移凭据，或生产 Runtime/Migration
使用完全相同的 URL，都会在网络 I/O 前 Fail Closed。错误和 `Debug` 输出都不会包含 URL。

生产默认验证 TLS，使用有界的 1–16 连接池，并禁止隐式迁移。如果明确选择启动时迁移，
StateKnot 会先打开并关闭独立迁移连接池，再连接运行时连接池：

```text
DATABASE_URL=postgres://stateknot_runtime:...@db.example/stateknot
STATEKNOT_MIGRATION_DATABASE_URL=postgres://stateknot_migrator:...@db.example/stateknot
STATEKNOT_AUTO_MIGRATE=true
STATEKNOT_DEV_MODE=false
```

两个 URL 必须对应不同权限的 Role。字符串不相等只是一道前置保护；部署时仍必须应用并审计
[可信 Server Role Profile](postgresql-roles.zh-CN.md)。

## 生产 Builder

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

`PostgresStore::connect(runtime_url, options)` 与
`PostgresStore::migrate_database(migration_url, options)` 仍是最明确的部署方式。
配置对象用于减少应用样板代码，不会合并权限边界。

## 显式本地开发模式

可信本机上的 PostgreSQL 可以使用：

```text
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stateknot
STATEKNOT_DEV_MODE=true
```

Development Profile 会启用内嵌迁移、复用该 URL、关闭 TLS，并把连接池限制为最多四个连接。
它不适用于不可信网络，也绝不会因缺少配置而被自动选择。

等价的程序化入口是：

```rust,ignore
let store = PostgresStore::connect_development(
    "postgres://postgres:postgres@127.0.0.1:5432/stateknot",
)
.await?;
```

设置 `STATEKNOT_AUTO_MIGRATE=false` 可以保留开发环境的传输/连接池默认值，
同时要求外部先完成迁移。启动仍会校验所有内嵌 Migration 版本、Checksum 与必需 Schema 对象。

## CI 与 Secret 处理

- 通过 CI Secret Store 注入 URL，禁止提交 `.env` 文件；
- 每个 Job 使用独立数据库，仅在隔离 Job 网络上设置 `STATEKNOT_DEV_MODE=true`；
- 不要通过通用序列化器输出配置；StateKnot 只提供脱敏 `Debug`，且没有 URL Getter；
- Joined Shutdown 时关闭 `PostgresStore`，避免连接池超过所属进程角色的生命周期。

