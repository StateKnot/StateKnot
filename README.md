<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot

**English** | [简体中文](README_zh.md)

[![CI](https://github.com/StateKnot/StateKnot/actions/workflows/ci.yml/badge.svg)](https://github.com/StateKnot/StateKnot/actions/workflows/ci.yml)
[![Supply chain](https://github.com/StateKnot/StateKnot/actions/workflows/supply-chain.yml/badge.svg)](https://github.com/StateKnot/StateKnot/actions/workflows/supply-chain.yml)
[![crates.io](https://img.shields.io/crates/v/stateknot.svg)](https://crates.io/crates/stateknot)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Durable agent orchestration for Rust.**

StateKnot combines typed Agent and Graph contracts, PostgreSQL-backed recovery,
and MCP/A2A adapters. It is a Rust-native runtime, not a line-by-line port of a
Python framework. Read the [documentation](https://stknot.com/docs/) or the
[Chinese documentation](https://stknot.com/zh/docs/).

> [!IMPORTANT]
> StateKnot is **pre-alpha**. The public `0.1.0-alpha.1` crates are for
> evaluation, not a stable API or production support claim. Pin the exact
> prerelease and do not use it in production yet.

## Install and explore

StateKnot requires Rust 1.88 and Edition 2024. Pin every StateKnot crate to the
same exact prerelease:

```toml
[dependencies]
stateknot = "=0.1.0-alpha.1"
```

From a checkout of this repository, a local, credential-free example validates
the typed Agent contract:

```sh
cargo run -p stateknot-core --example first_agent --locked
```

This example does **not** execute a durable run or call a provider. For actual
HTTP-free execution, follow the [in-process Agent guide](docs/in-process-agent.md);
it requires a qualified PostgreSQL deployment, explicit authorization, Worker
and maintenance roles. For network hosting, see [Agent Host](docs/agent-host.md)
and [authenticated HTTP ingress](docs/agent-http.md). No sample silently
installs an allow-all policy or an in-memory durability substitute.

## What exists, and what remains

The current preview includes validated core contracts, a durable Graph/Agent
runtime, PostgreSQL journaling and fenced recovery, OpenAI Responses and
Anthropic Messages adapters, bounded MCP Client/Server and static MCP Skills
profiles, and A2A 1.0 Client/Server profiles. The exact implemented slices and
their limits are listed on the [status page](https://stknot.com/docs/status/).

**Production completion is still open.** General retention and garbage
collection, full identity and policy qualification, durable A2A task/push
hosting, observability, OCI role delivery, multi-role failover, reference load,
24-hour soak, upgrade coverage and release provenance are separate gates. A
passing unit test or protocol conformance suite does not close them. The
[production completion order](docs/roadmap.md#production-completion-order),
[v1 scope](docs/v1-scope.md), and [three qualification scenarios](docs/scenarios/README.md)
are the controlling sources.

## Documentation and development

- [Documentation index](docs/README.md) and [public website](https://stknot.com/docs/)
- [Versioning and release policy](docs/versioning-and-releases.md)
- [Graph and Agent examples](docs/core-contract-examples.md)
- [MCP conformance](docs/mcp-conformance.md) and [A2A conformance](docs/a2a-conformance.md)
- [Contributing](CONTRIBUTING.md), [governance](GOVERNANCE.md), and
  [security reporting](SECURITY.md)

The repository pins Rust 1.88.0. The main local checks are:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
```

Database, protocol and release qualification require their documented external
services and runners; skipped integration tests are not passes. The website has
its own [verification guide](website/README.md).

## License

StateKnot is released under the [Apache License 2.0](LICENSE). See
[NOTICE](NOTICE) for attribution.
