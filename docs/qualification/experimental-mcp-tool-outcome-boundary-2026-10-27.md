<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Shared MCP Tool outcome decoding

Updated: 2026-10-27. This is a private, behavior-preserving preparation for RFC-0024. The owned stdio session and process-tree lifecycle remain unimplemented; the RFC and Issue 140 remain open.

The existing private protocol module now decodes complete and input-required Tool outcomes without an HTTP client. The real HTTP call path uses it and binds the existing client, Tool and original arguments to the continuation in the HTTP Adapter. Public types, resume signatures, authorization, header promotion, retries and durable effect classification retain their existing contracts. Tool errors remain complete Tool results, distinct from JSON-RPC and transport errors. Opaque request state and exact response keys are preserved; the decoder performs no requested host action or automatic resume.

The tested code candidate contains 987 source files (`43e3adf7fb881ffc9eb103c653c59f98b354d43fdfd541c741d81f8bdc4dbed1`) and 564 compiled inputs (`ef0b090ed6b7e352127f8e8d8324363e9cf18d98183cab243bbfce2565b872ec`). This delivery adds documentation without changing those compiled inputs. Local evidence includes:

- Original HTTP behavior characterization, four pure decoder tests, five actual HTTP contract tests and the existing ten protocol contract tests. JSON and fragmented SSE preserve results, errors and continuation behavior; invalid keys cause no additional request.
- Three functional mutation controls fail their intended runtime assertions when opaque state, isError or response-key validation is deliberately broken. They are not claimed as a regression in the original implementation.
- All six final Rust gates pass after the mutation runs have ended and the task Integration package cache has been cleaned. An earlier Clippy failure on a decomposed Unicode test literal is retained; spelling its combining character as a Rust escape preserves runtime bytes `65cc81` without normalization or a lint exception.
- Mandatory PostgreSQL 16 and 17 runs each pass all six durable MCP/A2A tests in a fresh template0 UTF8 database and leave six actual Run rows. The earlier no-database harness result does not establish this database coverage.
- The unchanged official runner `0.2.0-alpha.11` and frozen `2026-07-28` verifiers pass all required Client 32 and Server 37 scenarios. Required check failures are zero. The seven unscored Client scenarios retain 17 failures; ten reported task extensions retain 30 failures. These are not newly supported features.

A retained first protocol run failed its required MRTR scenario. An executable saved after that failure reproduces four failed checks; a clean rebuild passes the same five checks without changing source. The subsequent complete frozen Client/Server gates pass. Both binaries, raw reports and the A/B control are retained in the local delivery evidence. The precise low-level Cargo cache mechanism was not proven. Future mutable negative-control builds must use their own empty target directory, separate from the positive validation cache.

Independent code, style and data reviews bind the candidate and actual receipts. This record does not transfer the prior revision's CI result to this new code. The exact published delivery still requires its own complete CI. Native containment, resource ceilings, public stdio configuration, platform crash evidence and an independently pinned consumer remain RFC-0024 acceptance requirements. No stdio, release or R1 completion is claimed.

[简体中文](experimental-mcp-tool-outcome-boundary-2026-10-27.zh-CN.md)
