<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental published scoped ChildJoin resume qualification

Evidence checked 2026-10-19. [中文](experimental-scoped-child-join-resume-2026-10-19.zh-CN.md) · [Machine evidence index](experimental-scoped-child-join-resume-2026-10-19.json).

Exact head [`343b244d59a9f13987139fe98f802491a474640b`](https://github.com/StateKnot/StateKnot/commit/343b244d59a9f13987139fe98f802491a474640b) passed complete [CI run 39228803615](https://github.com/StateKnot/StateKnot/actions/runs/39228803615): all 21 jobs and all 120 required PostgreSQL step observations succeeded. Strict offline analysis returned actual 0, both full-CI flags true, no errors and all 14 components verified. The actual checkout is `4a05c35a9ef4f17b4176caf6b3afd85d94c39b7c`, with ordered parents main `aa11b4f44a948aaf2e2baba4c30a297dc828ce6d` and that head; both trees are `bdbddf21d9bd0a98b0fc25171e0c5a2df7ed15a6`.

The bounded G1 contract resumes an already published, unconsumed scoped Join after full owning preflight. Real parent callback completion atomically consumes the original publication, followed by scoped barriers, frame Return and Root continuation. The original F1 remains Executing; F2 succeeds once. Root terminal handoff still leaves the Agent Active. Fresh scoped suspension and partial-spawn recovery are separate work.

This positive case reconnects and rebuilds within one OS process. Its child has no application/provider I/O and uses a retained trusted failure terminal before settlement/publication; it is not fresh child application execution. The three negative cases retain fresh-target refusal, pre-drive child-quarantine refusal and unsupported next-target refusal before caller rebind. Preflight does not promise revocation of concurrent in-flight I/O after its gate; final consumption retains atomic fresh guards.

Local final gates all returned 0: workspace 2144 passed/6 original ignored and 48 doctests. Four G1 selectors and six existing regressions ran on both PostgreSQL majors (20 actual targeted successes). All 553 compilation inputs match the measured Native source; the final 949-file snapshot adds only six noncompiled overlays. CI runs complete Store 267 and Runtime 210 suites on both majors. Bootstrap 17 GETs and terminal 99 GETs retain 54 ZIP digests and 1544 indexed files. All 131 branch commits carry exact DCO.

Old C9dd run 39221903796 remains failed (16/21 successful jobs), and its original 37-path local 2140/Runtime 206 record and portable archive remain unchanged. New G2 development is outside this measured head. This later documentation candidate has no adopted documentation commit and does not relabel the runtime CI head. RFC-0022 and PR 161 remain Draft; Supported status and R1/R6/R7 are not closed. The concise index locates retained local evidence; it is not a replacement portable archive or a claim that future documentation source has passed CI.
