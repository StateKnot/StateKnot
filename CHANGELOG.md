<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Changelog

All notable changes to StateKnot will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and released versions will follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Initial Rust workspace and repository governance.
- Architecture research, implementation plan, completeness audit, and roadmap.
- Frozen v1 scope and production qualification scenarios with measurable load,
  failure, recovery, security, and interoperability gates.
- Initial RFC draft for the core domain, typed capability, identity, budget,
  error, and canonical serialization contracts.
- Initial `stateknot-core` implementation with validated tenant identifiers and
  canonical, strongly typed UUIDv7 identifiers.
- Canonical three-component contract versions and SHA-256 integrity digests,
  including strict Serde/JSON Schema validation and external compatibility
  fixtures.
- Canonical UTC microsecond timestamps and checked, non-negative millisecond
  durations with strict precision-preserving standard-library conversions and
  exact decimal-string JSON encoding.
- Checked token/byte counters and exact micro-unit money with strict currency,
  overflow, cross-currency, Serde, schema, property, and fixture validation.
- Normalized, offline-only HTTPS schema identifiers and immutable
  ID/version/digest schema references.
- MCP-compatible capability names plus bounded OAuth-compatible scopes and
  deterministic, duplicate-rejecting scope sets with narrowing property tests.
- Exact OIDC/OAuth issuer and redacted subject identifiers composed into a
  strict principal identity key, without unsafe URI normalization.
- Streaming bounded JSON materialization with immutable hard ceilings,
  decoded duplicate-key rejection, exact compact-size accounting, redacted
  diagnostics, property tests, and cross-version fixtures.
- Bounded text and structured content with stable RFC 5646 language tags,
  opaque security labels, explicit source/trust/redaction metadata, redacted
  diagnostics, closed schemas, exhaustive Unicode checks, and versioned wire
  fixtures.
- Tenant-scoped immutable artifact references with canonical RFC media types,
  bounded path-safe presentation metadata, integrity and schema bindings,
  explicit retention/provenance/lineage, closed content-part envelopes,
  redacted diagnostics, and versioned wire fixtures.
- Integrity-bound application instructions separated from provenance-bound
  user/assistant/tool messages, with strict producer/source-role matrices,
  aggregate inline payload limits, redacted diagnostics, closed schemas, and
  versioned wire fixtures.
- CI, dependency policy, issue forms, and security reporting guidance.
