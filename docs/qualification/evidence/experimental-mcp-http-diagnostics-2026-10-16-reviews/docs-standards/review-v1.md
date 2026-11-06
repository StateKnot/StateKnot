# HTTP MCP Debug adoption docs — Standards, v1 input

Reviewed immutable `mcp-http-debug-adoption-docs-v1` (manifest `cb005ee6…`, baseline commit `8016b74e…`); this reviewer did not author the three documents. One wording finding remains open in this input:

- **P2, `docs/mcp-client.md:237`**: “Earlier bound-state PostgreSQL15-case results” can identify PostgreSQL major 15 rather than the actual PostgreSQL 16/17 suites with 15 tests per major. State the versions and test count separately; the paired Chinese “双PG15项” can be clarified in the same small revision. This concerns accurate technical identity and qualification scope under `CONTRIBUTING.md`; it is not a production-code defect.

No other documented-standard or actionable smell finding was identified. All three baseline copies equal their Git8016 objects. Changes only insert text; original guide sections/code fences and the existing MCP-bound-state Unreleased paragraph remain byte-identical. Both languages specify exactly four Debug types/allowed fields and preserve the bare InputRequired exclusions, explicit untrusted getters, existing Clone/PartialEq, MRTR/wire/effect/resource behavior and HTTP `Applied + Never` boundary.

Independent raw-summary recount: workspace 99 suites / 2099 passed / 0 failed / 6 ignored / 0 filtered; doc tests 10 suites / 48 passed / 0 failed. The new TCP family has an actual `ok` line. The original first-branch canary red and borrowed-ID Clippy repair are described without claiming all eight old branches ran red.

The 17 local links include five references to three explicitly reserved qualification reports. Existing targets are present; the three new reports must accompany publication. This review does not claim they exist yet, transfer prior PostgreSQL results, or grant Native/full-CI/stdio/RFC/R1 qualification. It runs no Cargo, network, database, Docker, cache operation or saved runner. A separate immutable docs revision can close the wording finding and must be reviewed for any new Native claims.
