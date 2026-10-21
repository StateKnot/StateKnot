<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Composed request facts and PITR cleanup: original-profile local gates

2026-10-15. The actual main working tree containing the adopted A2A request-facts
change and PITR test-only cleanup confirmation passed all six original **opt0**
local gates. Workspace:98 suites,2073 passed,0 failed,6 original ignored;
independent doctests:45 passed,0 failed/ignored. Formatting, strict all-target/
all-feature Clippy, strict Rustdoc and pinned nightly2026-10-07 also exited0.
The65.43-second gate duration is not a performance result. [中文](experimental-composed-request-facts-pitr-2026-10-15.zh-CN.md).

Actual HEAD during execution was `04608c6b20a7c18d6b042eeccc83dd38d096b1d1` with
the PITR helper and its127 publication paths applied in the working tree.
**531 compiled-input paths/bytes/hashes and all2182 then-nonignored files were
identical before and after the run.** New reports/tracking text published later
are not claimed to belong to that tested2182-file snapshot. Default stack,
original resource/lease budgets, debug assertions and overflow checks remain;
incremental/debug-symbol cache controls are recorded separately from opt level.
DB test environment was removed. Conditional database entries do not constitute
Native database execution.

The [machine record](experimental-composed-request-facts-pitr-2026-10-15.json)
and [evidence seal](evidence/experimental-composed-request-facts-pitr-2026-10-15/seal.json)
bind every receipt/raw log, command/environment, git context and complete before/
after inventories. The first runner stopped at an ordering assertion before any
product command; the531 path/bytes/SHA maps were already equal. Its original
runner/error are preserved. The correction compares maps; it changes no product
source, resources or validation assertion.

The [A2A record](experimental-a2a-request-facts-2026-10-15.md) and
[PITR record](experimental-pitr-cleanup-confirmation-2026-10-15.md) keep their
original source/binary/execution scopes and seals. In particular, the PITR
report's initially pending composition statement is historical; this later
record supplies the actual local-gate result. The old PG16 WAL remains, and
all-failure-path cleanup is still open. Neither old916 Native nor the targeted
PITR/A2A executions are transferred into a new full Native qualification.

The default verifier is fully offline. With `--workspace`, it also reconstructs
only the531 compiled inputs from the already-published A2A923 archive plus the
exact one-file PITR patch and compares actual main paths/bytes. This small bundle
adds no complete source archive, executable binary or build cache. It does not
run full Runtime189/four-suite Native, full CI, independent TCK/CLI/release
profiles, or grant capacity, latency, security or complete production/R1
qualification. RFC-0022/RFC-0025 remain Draft.

```console
python3 docs/qualification/evidence/experimental-composed-request-facts-pitr-2026-10-15/verify.py --workspace
```

Adoption commit `ae19684cc029b4309a3fdfb21f5150ebedb36451` retains the same531 compiled input bytes; the original execution git-context remains04608 with its then-pending delta.
