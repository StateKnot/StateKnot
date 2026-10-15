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
crate/build-configuration inputs against the tested manifest. This validates
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
candidate. Full Native, higher-fence/seven-level, Linux, final composition and
production qualification remain open.
