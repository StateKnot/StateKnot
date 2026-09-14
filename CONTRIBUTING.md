<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Contributing to StateKnot

StateKnot welcomes focused issues, design feedback, documentation improvements,
tests, and code contributions. The project is pre-alpha, so early review of
semantics is more valuable than large speculative implementations.

## Before opening a pull request

1. Search existing issues and RFCs.
2. Open an issue before substantial work so scope and ownership are clear.
3. Use the RFC process for public APIs, execution semantics, persistence,
   protocols, security boundaries, compatibility promises, or new crates.
4. Keep a pull request small enough to review and revert independently.

Security vulnerabilities must not be reported in a public issue. Follow
[SECURITY.md](SECURITY.md) instead.

## Local quality gates

Install Rust through `rustup`; the repository selects its pinned toolchain.
Before submitting a change, run:

```console
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

Changes that affect behavior require tests. User-visible changes require
documentation and an entry under `Unreleased` in `CHANGELOG.md`.

## Chinese documentation terminology

Translate technical meaning, not individual English adjectives. Across the
website and repository guides, use `持久执行` for **Durable Execution**, `可恢复`
when describing an Agent or Graph's recovery capability, `持久化` for saving
state, logs, credentials or other records, and `持久性` for the storage guarantee.
Do not use `耐久` as a translation of software durability. Preserve Rust API
identifiers, code examples and published URLs so readers can find the source.

The [Chinese execution concept guide](https://stknot.com/zh/docs/concepts/durability/)
explains these distinctions. Persisting data alone does not guarantee correct
execution recovery or exactly-once external effects. Wording changes must not
broaden implementation status, recovery guarantees or production readiness.

## Commit sign-off

StateKnot uses the [Developer Certificate of Origin 1.1](https://developercertificate.org/).
Sign every commit with:

```console
git commit -s
```

The sign-off certifies that you have the right to submit the contribution under
the project's Apache-2.0 license. It is not a copyright assignment.

## RFC process

Copy the RFC template from `docs/rfcs/0000-template.md`, choose the next
available number, and open it as a pull request in `Draft` state. An RFC must
state observable guarantees, failure behavior, security impact, migration and
compatibility consequences, alternatives, and executable acceptance tests.

Implementation may begin experimentally while an RFC is under review, but no
unstable experiment may be presented as a supported public contract. See
`docs/rfcs/README.md` for lifecycle details.

## Review expectations

Maintainers evaluate correctness, scope, operational impact, security,
compatibility, and long-term maintenance cost. Passing CI is necessary but does
not by itself imply acceptance. Contributors should expect requests to reduce
scope when a smaller solution meets the same verified use case.

By contributing, you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
