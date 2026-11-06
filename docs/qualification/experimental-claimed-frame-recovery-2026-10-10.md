<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental claimed frame recovery qualification

This report qualifies the bounded experimental Core/PostgreSQL increment at
`8285f25130eeba18b7f6f2059089f851a64e1fac`, tree
`9428ca4c0c7e48b58c85beeeaa120323739f9d42`. It does not qualify actual nested
Driver execution or complete whole application replay. RFC-0022 remains Draft;
R1/G3/R6/R7 remain open. The [machine report](experimental-claimed-frame-recovery-2026-10-10.json)
retains exact-source identities, actual results, limits, log hashes and failures.

Claimed scoped planning proves the original claimed context, whole active frame
snapshot, exact leaf, bounded pending-result pages and complete physical attempt
history. It repeats the journal, Root/leaf and database-clock fence checks before
returning a plan. This observation grants no dispatch authority. Exact-checkpoint
result namespace contradictions are checked before durable cursor reloading;
claimed corruption quarantine remains bound to the original live owner.

Private return intent decoding uses heap allocation. Canonical bytes, digest
versions, public return records, schema and dependency locks are unchanged.

## Exact-source passes

- Strict full workspace/all-targets/all-features/locked Clippy and Rustdoc pass.
  The full workspace has 98 suites, 1,909 passing cases and three explicit ignores;
  the separate mandatory native database gates below actually execute PostgreSQL.
- PostgreSQL 16.15 and 17.11 full Store each pass 10 unit, one artifact and 182 native
  tests; native durations are 320.94 and 355.44 seconds. Full Runtime each passes
  140 tests, including actual independent LOGIN Schema 33 trusted-role profiles.
- The complete Linux seven-level return regression passes in 74.87 seconds under
  pinned Rust 1.88, original 2 CPU / 2 GiB limits and default thread stack. All 847
  source files, file modes and lock files match the frozen source. No stack,
  lease, deadline or assertion limits were relaxed.
- Local and immutable CI ASan each replay 2,297 identical seeds and execute three
  targets at 10,000 actual mutations within the fixed caps. Local peak RSS is
  512 / 1,019 / 448 MiB; CI peak RSS is 428 / 1,039 / 299 MiB. Full source snapshot,
  manifests, tools, locks, actual stats and log hashes are independently checked.
- All 13 jobs in [CI 38055431022](https://github.com/StateKnot/StateKnot/actions/runs/38055431022)
  and [Supply chain 38056473049](https://github.com/StateKnot/StateKnot/actions/runs/38056473049)
  pass. CI merge `721b38b9eff13495f5b24283d26af2c342f6f9d9` has the exact frozen
  tree and parents `aa11b4f` / `8285f25`; ASan artifact identity is `11671561451`.
- Exact-source public readers verify seven real prechange returns at depths 1–7
  in read-only PostgreSQL 16/17 logical restores. Canonical bytes/digests, complete
  ancestor/caller proofs, 16 closures / 40 callers / four provider bindings,
  all 52 table counts and row hashes, exact catalog and 33 migration records are
  preserved. This is current-reader logical backup compatibility. It does not
  qualify ACL restore, published historical N-1/N-2 executables or immutable
  historical mutation retries under this new reader.

## Retained observations and open work

Runtime 16's first complete run had 139 passes and one new connection `Protocol`
unknown message type `\0` failure before that test's business execution. TLS was
explicitly Disabled; its raw wire cause remains unproven. The failed entry passes
independently, followed by all 140 tests passing in 359.28 seconds under unchanged
source, configuration and limits. The original test/service logs remain separate.
The earlier parent f05 SSLRequest failure is a different retained observation.

An initial Linux post-run guard failed because Docker changed mount enumeration
order. The original failed guard is retained. A complete field-preserving
permutation proof accounts for both full configuration hashes; no field or mount
was ignored, no test was rerun and the corrected report identifies the proof.

Pinned nightly ASan passes but warns that Runtime's deep generic Future `Send`
proof exceeds a compiler recursion limit. A private heap-erased read seam is
being developed separately; no recursion-limit override was used. This result
does not transfer qualification to that future increment.

Scoped retry wakeup, complete multi-graph application replay, a durable Root
Wait/Terminal disposition bundle, actual nested Driver and scoped child Join
remain required work. Root legacy histories lacking that complete disposition
must fail operationally without a whole-replay report or incorrect quarantine.
Full production process/commit faults, historical executable compatibility,
capacity, independent security review and release gates remain open. Parent f05
qualification remains independently recorded in the [whole-frame report](experimental-whole-frames-2026-10-10.md).
