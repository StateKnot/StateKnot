<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Scoped attempt base proof evidence

The qualification report and compact receipt are two directories above this
directory. The final source manifest binds the 899 tested input files; the
receipt identifies the three adopted Rust files and the independently rebuilt
Store binary. Reports were added after the frozen source was tested.

Run `python3 docs/qualification/evidence/scoped-attempt-base-proof-2026-10-15/verify.py`
from the repository root to check the evidence hashes, complete Start helper
body equivalence, 139 unchanged SQL literals, five unchanged complete verifier
functions, and the original test summaries. Add `--workspace` to check the 515
crate/build-configuration inputs against the tested manifest, the Runtime
coverage inventory and CI PITR's exact adoption tree/lock. This validates
retained evidence; it does not run Cargo, PostgreSQL or a production workload.

`workspace-clippy-before.log` preserves the original candidate's two E0275
errors. `gates/` contains the final six logs, exact receipts and original runner
text. `build/` records the explicit package clean and actual fresh=false Cargo
artifact. Each `native-pg*/` directory preserves six exact test logs, receipts
and the actual server/container metadata. `independent-review.json` applies
only to the complete private Future-erasure patch; source checks do not replace
Native verification or review of the complete feature.

`start-helper-before.txt`, `start-helper-after.txt`, the compressed original A
Store source and layout baseline make the complete body and surrounding-source
comparison reproducible. These are frozen source data, not build inputs.
The original unified patch is preserved as `erase-only.patch.gz`. Logs with
native EOF blank lines also use gzip; no raw whitespace was normalized. Gzip
headers use a zero timestamp. The verifier decompresses these files before
checking the original receipt hashes, patch hash and test summaries, while the
evidence manifest also binds the compressed bytes.

`diagnostic/` contains the older instrumented binary receipts, native counter
logs, complete PostgreSQL session logs and source manifests. Every compressed
file uses gzip; its exact uncompressed SHA256 is also in the evidence manifest.
The five trial counts of 93 and 53 SQL statements apply only to those diagnostic
binaries. They do not qualify the final binary's latency or deep recovery.

`ci-1fcc/` preserves the older immutable workflow observation, raw annotations
and logs. Both PostgreSQL jobs exceeded the original 30-minute job limit after
Store tests passed 251/251; Runtime and later gates did not finish. Eleven other
jobs succeeded on that older commit. These observations do not qualify the new
candidate. Its incomplete Native/CI result remains separate from the later
local suite and resource evidence below.

`full-native/` adds all eight terminal local suite logs: Store 252, Runtime 185,
provider integrations 2 and Agent example 1 per PostgreSQL version, zero failed
and ignored. It retains the three additional build records, exact test lists,
environment/runner evidence and a post-test check of all 899 frozen inputs.
The original higher-fence, actual seven-level, serial and shared-quantum Runtime
cases pass on both versions with unchanged default resources. The older layout
candidate's failures remain distinct. `coverage.json` records the 185 entries
as 164 ordinary database/parent scenarios, 15 conditional auxiliary workers,
two independently gated resource branches and four harness/protocol self-tests.
Zero ignored is not evidence that a conditional branch ran. `runtime-markers.json`
retains the actual local process/COMMIT-loss, role and maintenance payloads;
ordinary full Runtime logs have no backup/restore or PITR marker.

`resources/` separately retains forced local logical backup/restore (one exact
pass/marker per version) and Linux CI named PITR (one exact pass/marker per
version). Raw logs and environment captures use deterministic gzip with their
original hashes unchanged. PITR checked out a synthetic merge commit; the
complete actual tree equals adoption commit 83559f7's tree and its Cargo.lock
matches. Both the initial commit-ID-only rejection and final complete-tree
verification are retained. The CI snapshot binds only these successful PITR
jobs; it does not claim complete CI success. Local four-suite Native and these
resource gates do not qualify Linux deep Driver recovery, all other dedicated
CI steps, the final combined source, capacity, R1 or release.

`ci-83559/` retains the adoption head's final run/jobs snapshots and both
ordinary PostgreSQL job objects, all annotations and lossless complete job
logs. Eleven jobs succeeded and two were cancelled by the original 30-minute
job limit. Both Store summaries are 252/0/0; both Runtime harnesses started 185
tests but have no terminal summary or explicit failed-test line. Provider,
example and subsequent dedicated qualification steps were skipped, with the
original missing-artifact annotations retained. The verifier checks these
observations without interpreting incomplete Runtime output as pass/fail or
the harness durations as throughput. This does not replace the successful
local four-suite qualification or turn the whole CI run green.
