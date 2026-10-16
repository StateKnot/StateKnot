<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Experimental A2A request facts: exact execution record

2026-10-15. The fixed request-facts candidate passed six strict static gates,
26 real loopback network tests, and the complete mandatory six-test durable
suite on each PostgreSQL major. This is bounded execution evidence for the
outbound client seam; complete production policy, protocol-service and CI
qualification remain open. [中文](experimental-a2a-request-facts-2026-10-15.zh-CN.md).

The [machine profile](experimental-a2a-request-facts-2026-10-15.json),
[seal](evidence/experimental-a2a-request-facts-2026-10-15/seal.json) and
[offline verifier](evidence/experimental-a2a-request-facts-2026-10-15/verify.py)
bind the source, actual terminal logs, test inventory and claim limits.

## Source, review and adoption

The sole full source archive contains923 files, tree
`250066800739a33390a316c7fbb40181335de67394cd9de6bb67ccb425226433`.
The one production file `a2a_client.rs` is SHA256
`46022f375430c2387bb73e7914229a55de62144067417053327e0c06e3397216`.
Its independent fixed-file review reports Standards0/Spec0; it did not review
or execute the evolving tests. The separate final five-document review reports
Standards0/Spec0 and preserves the earlier P2 wording correction.

The [exact four-file delta](evidence/experimental-a2a-request-facts-2026-10-15/source/delta-from922.patch.gz)
changes that production file, two existing integration test modules and one new
network-test module relative to the schema36/922 source adopted in `0e8b4b5`.
Core/Store/Runtime, Cargo configuration/lock, migrations1–36 and their SQL are
unchanged. No schema37, B, new owner or PITR-cleanup helper is included. Actual
adoption applied four code and five reviewed documentation files. **531**
conservative crate/config/fixture inputs match the candidate; this is not
whole-main-Git-tree equality. The two fresh test binaries and four workspace
libraries also retain compiler-emitted dep-info covering215 workspace source
entries. That narrower observed set is not a full compiler/filesystem/registry
read closure. Binary bytes and build caches are excluded from this bundle.

## Exact request facts and policy boundary

A private immutable snapshot holds the prepared method, normalized complete
URL/query and already-bounded serialized body. Authorization sees a clone;
the HTTP builder consumes the same snapshot without another serializer or
application rewrite. REST reads may have no body and release resource/filter
facts in URL/query. JSON-RPC reads use POST envelopes, so method alone cannot
decide whether an operation writes. These accessors supply trusted host policy
with actual request data; they do not implement policy or consent.

Anonymous calls no token provider. Existing metadata-only/static providers may
ignore the new getters and retain their prior behavior; they do not acquire
argument-aware authorization. Debug redacts the added URL/body but preserves
previous tenant/task/scope/attempt identity fields. Clone shares the immutable
body and retains a URL; a trusted provider can retain them beyond the call.
The existing body-length bound is not aggregate retention, peak allocation or
zeroization protection. Facts do not attest every header, DNS, TLS peer or proxy.

Refusal occurs before business send. Duplicate recovery of the **same already
committed durable Failed/Unknown attempt receipt** performs neither another send
nor another policy evaluation. A fresh probe or operator-attested MessageIdReplay
is a new recovery operation: it must reapply current policy and may send under
its original contract. The HTTP replay tests demonstrate both a second authorized
send with original business identity and revocation blocking that second send.
They do not qualify JSON-RPC replay or authorize a general retry policy.

## Observed execution

| Gate | Actual result |
| --- | --- |
| fmt / strict workspace all-targets all-features Clippy | exit0 each |
| Workspace all targets/features, locked | 98 suites;2073 passed;0 failed;6 original ignored |
| Independent doctests | 45 passed;0 failed |
| Strict Rustdoc / pinned nightly2026-10-07 check | exit0 each |
| Fresh saved network binary | 26 passed;0 failed/ignored/filtered |
| Mandatory durable binary, PostgreSQL16.15 | 6 passed;0 failed/ignored/filtered |
| Mandatory durable binary, PostgreSQL17.11 | 6 passed;0 failed/ignored/filtered |

Static gates cleared DB environment; their conditional entries are not Native
database evidence. Fresh Cargo executable artifacts have `fresh:false`, debug
assertions/overflow checks retained and development/test symbols0. The original
strict flags, default stack and fixture resources were not increased. Actual
saved binary identities and complete `--list` logs are retained. Static26 and
separately executed26 are executions of the same inventory, not52 distinct cases.

The network26 retain19 earlier tests and add7. Two existing matrix tests exercise
11 operations on each binding and compare received method, URL/query and raw
body with callback facts, including streaming send and subscribe. They are two
test functions containing22 operation calls. The new tests cover payload
allow/deny in REST/RPC unary/SSE, one provider with two independently preconfigured
destination URLs in both unary bindings, an actually polled callback dropped on
deadline/abort, Anonymous/legacy unary behavior, and **HTTP-only** attested
replay/current revocation. [Coverage](evidence/experimental-a2a-request-facts-2026-10-15/coverage.json)
lists the exact combinations; no Cartesian-product coverage is inferred.

The durable suite contains four existing and two new tests: three MCP and three
A2A cases. A2A durable request-facts coverage is **HTTP+JSON only**. Each major's
actual authorization callback loads the durable Start and verifies complete
input/attempt/fence before examining the outgoing payload. Denial has0 business
sends/1 policy call; lost response has1 send/1 policy call and remains Unknown;
duplicate receipt recovery preserves complete ledger and performs neither again.
Both original MCP and new A2A denial/lost-response markers are checked separately.
Database identities show distinct task databases on the original16.15/17.11
servers. These twelve libtest executions are not twelve new independent cases.

## Preserved failures and source provenance

The `755ffa…` snapshot was source-only and lacked an explicit `DurationMillis`
import; no compile or runtime red is invented for it. The import-only correction
produced `59fd…`. Its first strict workspace run failed on sandbox loopback bind;
the authorized loopback rerun then exposed a real fixture routing failure:
25 passed/1 failed because the fixture omitted the exact remote-tenant REST send
path. Final `250066…` adds only the two exact tenant send/stream fixture routes.
Production46022 and original resource/validation behavior remain unchanged.
Original logs, exit codes, manifests and both tiny patches are preserved; the
verifier reconstructs both earlier923-file snapshots from the sole final archive.

The compiler-dep-info collector's earlier assertion about expected Cargo target
names is also retained as a tooling failure, not a compilation/test failure.
The offline evidence collector's prefix/index/path-normalization assertions are
also preserved separately and did not execute product code. All final execution
stages were waited to terminal0; final source hashes match,
no matching build/test/HTTP fixture process remains, and inactive task caches
are retained with lsof/lock and actual df evidence. No DB was dropped and this
collector performed no Docker/DB operation or cleanup.

## Historical and remaining qualification limits

The [schema36 record](experimental-original-graph-waits-2026-10-15.md) retains
its own922 source, 530-input adoption commit `0e8b4b5` and916 Native executions.
That old offline evidence stays valid. Its whole-workspace530 check is historical
and correctly rejects this additional module; **none of its full916 executions
is transferred to the new531-input binary**. Its PG16 PITR WAL cleanup exception
also remains open. This bundle references the original parent source/seal and
stores the exact four-file delta, not another copy of its750 evidence files.

The five adopted guides/RFC documents and two latest tracking-file snapshots
have exact hashes. Their wording separates the delivered facts interface from
host data-release/current-consent policy, MCP principal rebind rejection,
verifier/delegation profiles and a persistent A2A service backend still open.
This execution did not run the independent server TCK, CLI/release profiles,
multiple independent official peers, full Store/Runtime Native on531, or full CI.
It confers no latency/capacity, security certificate, owner delivery, Supported
status or complete production qualification. RFC-0025 and R1 remain open and
the PR remains Draft.

Run the default verifier offline first. After adoption, `--workspace` additionally
checks actual531 input paths/bytes and the five reviewed documents:

```console
python3 docs/qualification/evidence/experimental-a2a-request-facts-2026-10-15/verify.py --require-execution-record --workspace
```

Original patches and text evidence with original whitespace use deterministic gzip; raw hashes/bytes remain exact. No source or whitespace policy changes.
