<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot

[![CI](https://github.com/StateKnot/StateKnot/actions/workflows/ci.yml/badge.svg)](https://github.com/StateKnot/StateKnot/actions/workflows/ci.yml)
[![Supply chain](https://github.com/StateKnot/StateKnot/actions/workflows/supply-chain.yml/badge.svg)](https://github.com/StateKnot/StateKnot/actions/workflows/supply-chain.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Durable agent orchestration for Rust.**

StateKnot is an open-source Rust framework under development for building typed,
durable, observable, and protocol-native agent systems. It is designed as a
Rust-native runtime rather than a line-by-line port of a Python agent framework.

> [!IMPORTANT]
> StateKnot is currently **pre-alpha**. The repository contains the reviewed
> architecture baseline and project infrastructure, but no production release
> or stable public API yet. Do not use it in production at this stage.

## Direction

StateKnot is being designed around five commitments:

- **Typed by default:** explicit state, tool schemas, structured output, and
  capability negotiation instead of untyped maps flowing through the runtime.
- **Durable execution:** journaled events, checkpoints, pause and resume,
  lease/fencing, transactional outbox, and realistic external-side-effect
  guarantees.
- **Graph and agent ergonomics:** a direct agent loop for common cases plus
  deterministic typed graphs for branching, parallelism, joins, loops, and
  human approval.
- **Protocol-native interoperability:** first-class MCP and A2A adapters without
  leaking their wire types into the stable core domain model.
- **Production governance:** tenant isolation, policy enforcement, budgets,
  auditability, OpenTelemetry, evaluation, failure injection, and compatibility
  testing are design requirements rather than optional add-ons.

The v1 scope baseline targets PostgreSQL-backed execution, OpenAI-compatible and
Anthropic model adapters, MCP client/server support, and A2A REST/JSON-RPC
client/server support. Its supported surface and explicit exclusions are
recorded in the [v1 scope baseline](docs/v1-scope.md).

## Current milestone

The project is in the **architecture-contract and vertical-validation phase**.
The unpublished core crate validates model, tool, agent admission/result,
durable run-lifecycle, canonical journal-envelope, and lease/fencing contracts.
The first PostgreSQL 16/17 durability slice now implements run admission,
canonical journal append/read, locked lifecycle transitions, schema verification,
and database-enforced lease fencing. Checkpoints, node/tool ledgers, outbox,
recovery scheduling, quarantine workflows, and a runnable agent loop have not
shipped yet.

The current milestone is to:

1. validate the three frozen production scenarios and their load/failure models;
2. accept the core domain, graph, durability, and protocol/security RFCs;
3. prove one end-to-end durable execution path with crash recovery;
4. publish compatibility and performance evidence before claiming support.

See the [qualification scenarios](docs/scenarios/README.md), the
[roadmap](docs/roadmap.md), the full
[research and implementation plan](docs/research-and-implementation-plan.md),
the [PostgreSQL provider operations guide](docs/postgresql-provider.md), and the
[completeness audit](docs/plan-completeness-audit.md).

## Repository layout

```text
crates/stateknot/        Unpublished facade crate used to validate the workspace
crates/stateknot-core/   Validated domain, run, journal, and ownership contracts
crates/stateknot-store-postgres/  PostgreSQL run/journal/lease durability slice
docs/                    Architecture contracts, qualification scenarios, and roadmap
.github/                 Contribution templates and automated quality gates
```

Additional crates will be created only after their dependency or semantic
boundaries are proven. Empty provider, protocol, and runtime crates are
deliberately avoided.

## Development

The repository pins Rust 1.85.0, the initial minimum supported Rust version.
With `rustup` installed, the toolchain is selected automatically.

```console
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
```

Before proposing an implementation, read [CONTRIBUTING.md](CONTRIBUTING.md).
Changes to public APIs, durable semantics, protocols, persistence, or security
boundaries require an RFC.

## Community and security

- Use GitHub issues for reproducible bugs and scoped feature proposals.
- Follow the [Code of Conduct](CODE_OF_CONDUCT.md).
- Report vulnerabilities through the private process in [SECURITY.md](SECURITY.md).
- Project decision-making is documented in [GOVERNANCE.md](GOVERNANCE.md).

## License

StateKnot is licensed under the [Apache License 2.0](LICENSE). Contributions are
accepted under the same license and require a Developer Certificate of Origin
sign-off. The license does not grant rights to project names or logos.
