<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Native Runtime CI execution

Updated: 2026-10-27. This change concerns CI execution and does not change the public Rust API, database schema or product support level.

The PostgreSQL 16 and 17 jobs retain the same cold, same-run producer and its two Store/Runtime binaries. Store runs its complete 276-test harness. Runtime's complete 229-name inventory is sorted and assigned alternately to two independent databases and processes. Each process receives its exact full names and uses one test thread. The two result sets must be disjoint, cover the complete producer inventory exactly once, and match their expected passed and filtered counts. Six raw output streams are retained; no synthetic full Runtime stream is generated.

The execution schema is `stateknot-native-runtime-shards-execution-v1`, under the current `stateknot-postgres-native-shared-build-ci-contract-v2` contract. Three original leader identities own their helpers and binaries. Validation binds actual process identity, database routing, source, binary hashes, profile, ABI, complete raw streams and retirement. Legacy single-stream commands remain strict and cannot validate the new layout. The producer still exports two binaries; job, checkout and artifact identities remain unchanged.

The 30-second lease, 90-second readiness, 1,800-second pair limit and 30-minute Native job limit remain unchanged. This addresses an observed scheduling limit: in [run 39548415549](https://github.com/StateKnot/StateKnot/actions/runs/39548415549), all Store and Runtime harnesses passed on both database versions, but PostgreSQL 16's Runtime took 1,771.99 seconds and its job was cancelled at the total job limit. The PostgreSQL 17 Runtime took 1,696.22 seconds. The cancelled job remains unsuccessful.

Local validation passed 60 new codec/control tests, the 12 original artifact controls, Python 3.9 syntax checks and both shell syntax checks. These are local control results. Full Linux execution of the new three-process layout and the complete CI cohort must still pass on the published candidate before qualification. R1 and release qualification remain open.

The same delivery refreshes only vulnerable npm transitive lock entries. Website and frozen MCP runner audits return zero vulnerabilities, and their existing browser and frozen protocol gates pass locally. The MCP runner and protocol requirement versions remain unchanged; unscored protocol features retain their original results.

[简体中文](experimental-native-runtime-shards-2026-10-27.zh-CN.md)
