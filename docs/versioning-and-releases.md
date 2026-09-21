<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Versioning and release policy

Status: normative for published StateKnot crates. The first public line is
`0.1.0-alpha.1`; it is a consumable preview, not a production-readiness claim.

[Simplified Chinese](versioning-and-releases.zh-CN.md)

## Published package set

StateKnot versions the product crates together and publishes them in dependency
order:

| Order | Crate | Boundary |
| --- | --- | --- |
| 1 | `stateknot-core` | Protocol-neutral public types and contracts |
| 2 | `stateknot-store-postgres` | PostgreSQL durability provider |
| 3 | `stateknot-runtime` | Registries, durable execution, scheduling, and service APIs |
| 4 | `stateknot-integrations` | Model, MCP, and A2A adapters |
| 5 | `stateknot-artifact-store` | Integrity-checked S3-compatible artifact persistence |
| 6 | `stateknot-testkit` | Runtime-neutral host qualification evidence and objective evaluation |
| 7 | `stateknot` | Controlled facade, HTTP host, Worker, maintenance, and in-process Agent API |

The Testkit is public so the Facade's qualification tests and external hosts
resolve the same exact release set; its reduced evidence never constitutes a
production SLO. Every package embeds `LICENSE`, `NOTICE`, and the English
`README.md`; docs.rs builds the same source with warnings denied.

Use exact prerelease requirements so dependency resolution cannot cross an
unreviewed preview:

```toml
[dependencies]
stateknot = "=0.1.0-alpha.1"
```

All StateKnot product crates in one application must use the same exact
version. Mixing product versions is unsupported because durable schemas,
registry bindings, and recovery semantics are qualified as one release set.

## Compatibility contract

StateKnot follows Cargo's interpretation of Semantic Versioning:

- prerelease identifiers such as `alpha.1` may contain breaking API changes;
  consumers must opt into and pin each preview explicitly;
- after a non-prerelease `0.y.z` is published, incompatible Rust API changes
  increment `y`; compatible additions and fixes increment `z`;
- after `1.0.0`, incompatible public API changes increment the major version;
- an MSRV increase is incompatible for supported consumers and requires at
  least the same bump as an incompatible Rust API change;
- a public item is covered unless its documentation explicitly marks it
  experimental or implementation-private. Deprecation precedes removal in a
  later incompatible release whenever safety or correctness does not require
  immediate removal.

The current MSRV is Rust `1.88.0`. Edition 2024 and first-party dependency
requirements are real constraints; publication does not imply Rust 1.83
compatibility.

Semantic versioning of Rust crates does not rewrite durable data or protocol
identity. Versioned JSON Schemas, Graph/Agent capability identities, provider
profiles, MCP/A2A profiles, and PostgreSQL migrations retain their own explicit
versions and fail closed on incompatible drift.

## Durable-data and upgrade policy

- Database migrations are forward-only, ordered, transactional where
  PostgreSQL permits, and never fabricate missing execution evidence.
- Downgrade is not supported. Restore a pre-upgrade backup into a separately
  qualified deployment if rollback is required.
- A release note must identify every new migration, durable-schema change,
  operator action, and compatibility boundary.
- The initial alpha line guarantees only upgrades explicitly covered by the
  PostgreSQL 16/17 matrix. A later support window is not inferred from a
  successful compile.

## Release gates

A version is publishable only from an immutable `v<version>` tag reachable from
`main`, with a matching lockstep workspace version and a curated changelog.
The release commit must pass:

1. formatting, Clippy with warnings denied, all workspace tests, Rustdoc, and
   dependency policy;
2. Linux, macOS, and Windows builds plus PostgreSQL 16/17 integration evidence;
3. frozen MCP and A2A conformance gates and the complete website suite;
4. `scripts/verify-release.sh`, including every Cargo file list and source-size
   ceiling, a workspace/Rustdoc rebuild, and normalized dependency-free
   bootstrap archive rebuilds; downstream normalized archives are built and verified in
   dependency order by the publisher as their same-version dependencies appear;
5. protected-environment approval before crates.io publication.

Future releases use crates.io Trusted Publishing: the protected GitHub Actions
workflow exchanges OIDC for a short-lived token, publishes in dependency order,
then downloads and SHA-256 compares every published archive. A rerun accepts an
already-published package only when its bytes exactly match the tagged build.
The first release of each new crate is the unavoidable bootstrap exception: it
uses a short-lived manually created token, which is revoked immediately after
Trusted Publisher records are installed.

Operational steps and partial-release recovery are in
[`RELEASING.md`](../RELEASING.md). Security fixes follow
[`SECURITY.md`](../SECURITY.md); a security correction may shorten the normal
deprecation window, but it still receives an explicit changelog entry.
