# Experimental MCP HTTP response diagnostics — 2026-10-16

This record covers four response `Debug` implementations on frozen source
`31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca`
(928 files), with the supplied 536 compilation inputs
`1ec97f720269934853dcb3fd66241b38a5b381fda48f4164c11f3dcc091f945c`.
Six local gates pass. The new single real-TCP test family completes all eight
JSON/SSE branches. Separately, two complete targeted Native suites pass on each
of PostgreSQL 16.15 and 17.11. This is a bounded experimental record; R1 remains
open, RFC24/RFC25 remain drafts and no Supported or full CI status is granted.

The [machine record](experimental-mcp-http-diagnostics-2026-10-16.json) contains
full hashes, exact gate commands, binary profiles, all Native marker objects and
explicit scope. [中文](experimental-mcp-http-diagnostics-2026-10-16.zh-CN.md).

## Response contract

Both ordinary and pretty formatting expose only a fixed type name, a
non-exhaustive marker and these fields:

| Type | Retained fields |
| --- | --- |
| `McpNotification` | `has_params` |
| `McpCompleteToolResult` | `content_count`, `has_structured_content`, `is_error` |
| `McpRemoteError` | original i32 `code`, `has_data` |
| `McpToolCallResponse` | fixed `outcome` (`complete`/`input_required`), `notification_count` |

The outer response does not recursively format its result, continuation or
notifications. Remote method/message, params/content, structured/raw values,
data, state and Tool metadata do not appear in these four summaries. This also
makes `StatelessMcpClientError::Remote` and `McpToolCall::Complete` safe through
their nested response values. Bare `McpInputRequired` and bare
`McpToolCall::InputRequired` retain their existing Tool-metadata Debug behavior;
there is no claim that every MCP value has payload-free Debug.

Explicit raw getters still return untrusted remote values. Existing Clone,
PartialEq, getters, explicit resume, wire shape, admission/resource limits and
effect/retry behavior are unchanged. HTTP malformed-complete writes still map
to `Applied + Never`; the proposed stdio `Unknown` mapping is not adopted.
No new API, dependency, verifier or credential authentication is added.

## Actual component execution

| Gate | Actual result |
| --- | --- |
| Rust1.88 format check | exit0 |
| Strict all-target/all-feature Clippy | exit0 |
| Workspace all-target/all-feature tests | 99 suites; 2,099 passed, 0 failed, 6 original ignored, 0 filtered |
| Workspace documentation tests | 10 suites; 48 passed, 0 failed/ignored/filtered |
| Strict Rustdoc | exit0 |
| Fixed nightly-2026-10-07 check | exit0 |

The original opt0 profile, default stack, debug assertions and overflow checks
are retained; source bytes match before/after every gate. Conditional database
workspace entries do not supply Native evidence. The original six-gate receipts
record PID/start, command/profile, log hash and source checks; they have no
contemporaneous runner hash or process-environment allowlist. Retaining the full
runner sources now does not retroactively attest those process environments.
Native receipts separately include the actual runner hash and environment
allowlist; these two evidence strengths are kept distinct.

The added family uses real HTTP JSON and fragmented SSE responses for Complete
with both `isError` values, InputRequired with notifications and an explicit
resume, and Remote errors. Both formatting styles exclude nested canaries,
including continuation Tool names. Positive controls compare parsed raw Values
and their serialization bytes, existing Clone/PartialEq, params/structured/data
presence, exact resume state/arguments/response keys, fresh IDs and call counts.
The outer response never leaks its InputRequired Tool metadata.

The real original-Debug canary run fails in its first JSON Complete branch:
exit101, 0 passed/1 failed/2 filtered; raw SHA `fde49495…`. The first fixed
candidate's Clippy run also exits101 (`Value` passed to the test fixture); raw
SHA `96e4fed5…`. Both failures and full runners remain in `history/`. Final v3
only borrows the fixture's JSON ID: no new lint allowance or production change.
These are distinct execution facts, not overwritten green records.

## Separate targeted Native execution

| PostgreSQL | Complete `mcp_durable` suite | Complete `mcp_reconciliation` suite | Complete JSON markers |
| --- | --- | --- | --- |
| 16.15 | 6 passed/0 failed/0 ignored/0 filtered | 10/0/0/0 | 7 exact objects |
| 17.11 | 6 passed/0 failed/0 ignored/0 filtered | 10/0/0/0 | 7 exact objects |

`mcp_durable` includes A2A and MCP regressions. Reconciliation includes definition
and wire cases as well as database cases: 32 harness executions across two
majors are not 32 database-only cases. Each suite is freshly built on this exact
source with per-package default features (`[]` in Cargo's artifact), opt0/debug0,
assertions/overflow true, and then run serially with the default stack. Full
suite lists and actual Cargo artifact records are retained. Binary hashes:
`2717d6db539ad1604ba541d7f2f2d397c758a0f99deaf148cc7fd2627c9048ff`
and `bc4f33032c7b0b207e1e5eac62e741e9445e52970981fb8cb818ca5a56f1c3c3`.

Each major's raw markers contain 2 MCP authorization, 2 A2A authorization and
3 reconciliation objects; the verifier checks complete objects, not just a
prefix/count. The legacy `schema:24` marker is a profile constant, not current
database schema36. Exact source checks surround all four Native runs. The new
`stateknot_r1_mcp_debug_20261016v3` database was retained on each major. The
complete actual runner is SHA `234a01c8…`; Native summary is `d0927746…`.
Earlier bound-state PostgreSQL 16/17 full 15-test serial-suite results belong to
their own source; they are not transferred or described as a current rerun.
This does not qualify full Store/Runtime Native or official conformance.

## Integrity and limits

The [evidence manifest](evidence/experimental-mcp-http-diagnostics-2026-10-16/manifest.json)
and [seal](evidence/experimental-mcp-http-diagnostics-2026-10-16/seal.json) bind all
raw logs, complete runners, source references/payloads and offline verifiers.
Only two new compressed source payloads are stored over 18 existing source-only
references; no prior full archive, old 449-file package, binary or cache is copied.
The source reconstruction also checks the original 357-line test prefix and
unchanged error/network/parser/unit suffix. Normalization is an additional scope
check; the real TCP execution supplies behavior evidence.

```console
python3 docs/qualification/evidence/experimental-mcp-http-diagnostics-2026-10-16/verify.py --repository-root .
# After exact source adoption, also check the supplied 536 workspace inputs:
python3 docs/qualification/evidence/experimental-mcp-http-diagnostics-2026-10-16/verify.py --repository-root . --workspace
```

The frozen 928-file source is not claimed to equal current main, and the input
map is not a compiler read closure. The verifier reads hashes/logs and does not
execute Cargo, network, Docker or databases. At Native completion `df` recorded
315,708,296 KiB available; eight task caches totalled 10,554,584 logical KiB.
The proof-base cache had valid TAG/untracked status, empty lsof output and free
Cargo locks at that observation. Logical sizes are not APFS reclaimable space;
no freed-space claim is made. Credentials and executable bytes are excluded.

The adoption candidate is docs-v3: its three document bytes equal v2; only a
receipt scope key was clarified. v1/v2 remain immutable. That metadata correction
is not a product regression. The original bound-state Added section, opt1
tracking and existing guide examples remain intact. Independent review of this
authored source/docs/evidence is a separate step.

Base commit `8016b74…` CI exposed a Windows checkout failure (`Filename too long`,
exit128 before Rust) in the parent's separate CI work. This record asserts no
final overall CI result or CI qualification for the changed Debug tree. Full CI,
fresh official MCP conformance, deployment/capacity, stdio ownership, durable
A2A backend, real credential authentication/long-stream revocation and full R1
acceptance remain separate. No older execution is transferred to this tree.
