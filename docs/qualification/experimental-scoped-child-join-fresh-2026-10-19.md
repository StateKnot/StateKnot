<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental fresh scoped ChildJoin adoption and pending qualification

Checked 2026-10-19. [中文](experimental-scoped-child-join-fresh-2026-10-19.zh-CN.md) · [Machine evidence index](experimental-scoped-child-join-fresh-2026-10-19.json). G2 here names a development slice, not the gap-ledger schema item.

This is a documentation candidate. The frozen runtime candidate has 959 selected source files (`5f5c726a420de718fbacc329c21753432df31e3907dd40dfb1a086ace40edd68`) and 560 compilation inputs (`c0a88f666c4704a7636901dbf65a8e72cf8de9e0adfa894b4740cac2b0fbd2e1`). CI pins are Store272/Runtime216. Store build and complete --list on this exact source actually returned0 with inventory272 and binary7d48b69c924d428830f48e4d0a29d16048240626f430657183630e30e619acf4; this is not a complete Store-suite pass. The final Runtime216 evidence binding remains pending. Final gates, complete Native suites, publication head/tree, actual checkout S and a new complete CI run still need exact evidence. This later documentation overlay is noncompiled and changes the final selected-source snapshot.

## Adoption contract

Fresh entry requires Exclusive executors with typed ChildJoin support for every declared child owner in the target and reachable nested frames. Obtain the complete actual frame with public load_active_graph_frame, validate checkpoint/activation/namespace/fence and use ChildRunAdmissionIntent::for_frame with the existing Store admission. Partial GraphNodeContext fields do not replace full ownership; no new getter or coordinator is needed.

Keep logical activation and slot stable. The retry digest binds parent admission, tenant/key, complete descriptor, request, budget layers, resolved budget, graph, authority, initial state and initial ready nodes. Candidate child Run/thread/invocation IDs may change; an idempotent result recovers the original child, events/checkpoint, ownership and reservation. A digest is not authorization.

Provider-native initial_state() generates message/model-plan identities. After process loss or an unknown admission COMMIT, load the original key through the complete owning reader and reuse its exact retained initial state while still checking the stable application request/authority/budget template. Only precise ChildRunNotFound permits first creation. Corruption, quarantine or database errors never mean absent. An uncommitted candidate selected no durable state.

An unfinished activation with no Join may enter or re-enter its callback under a higher fence after partial admission. A registered unpublished Join waits for actual terminals, settlement and complete publication; normal claims cannot redispatch it. A callback receiving context.child_join() reads the original publication and returns Completed instead of spawning or rejoining. Existing atomic result/publication consumption, scoped barriers, Return and Root continuation remain authoritative. Arbitrary external effects still need durable invocation and reconciliation.

## Capacity and terminal accounting

Reserve parent headroom for real DIRECT work, physical caller takeover, completion and recovery. Complete capacity includes spent DIRECT, settled actual child usage and every outstanding reservation ceiling at the transaction clock. Pending children permit only the checked structural caller takeover, not ordinary barriers, checkpoint advancement, Return or provider dispatch.

Reservation ceilings are policy, never delegated spend. Do not persist them as usage, pre-add child usage, increase the parent budget to hide a failure, or remove settlement guards. The old admission fixture inherited all 25 parent retries for its child; after one real F2 retry, 25+1>25 correctly failed capacity. V5 narrows only that fixture child ceiling to 4 and retains a typed Core rejection for the old case. V5 has not run at this evidence cutoff; 4 is not a general production recommendation. Plan headroom in every dimension.

A Root terminal handoff is Active until the public lifecycle coordinator actually commits Succeeded. A zero-I/O parent does not fabricate model turns: its DIRECT ledger accounts for real journal/framework work and terminal output bytes; an actual Native child supplies two Model and one Tool calls. The coordinator adds the settled child cumulative subtree once. AgentResult must equal DIRECT plus that subtree while preserving original reservation, settlement, child result and consumption.

The separately measured parent-terminal v1 succeeded on both PostgreSQL majors: real parent Succeeded, complete result/account, two original Idempotent ACKs with unavailable evidence, active-F2 old-F1 StaleFence, terminal F1/F2 and fresh claim RunNotRunnable, unchanged whole-tenant facts and empty repeated maintenance. This case reconnects and rebuilds; it does not inject OS death or transport ACK loss and does not qualify later styles or the final combination.

## Evidence cutoff and remaining work

The retained intermediate index binds 16 actual receipts to two distinct source/compiled pairs: eight Store-capacity successes, two partial-spawn successes, two consumption successes and two parent-Succeeded successes; the two old admission/registration failures remain exit101. The first three groups use source b1cd1f58…/compiled34e0fd4c…; parent-terminal uses 76e7143d…/4136bc98…. Partial-spawn includes real kill/reap and natural lease expiry. These 14 successes do not constitute one final-source pass.

The final candidate includes admission v5, the deterministic caller/child-terminal overlap case, parent style v3 and CI272/216. On this same959/560 source the new race now passed once on each PostgreSQL major (1 passed/271 filtered; process1.335/4.217seconds). V5 has no actual result bound by this document yet. Strict Clippy on the same source returned101 for two new-test style diagnostics (similar_names and redundant closure); that failure remains. The later style overlay needs its own qualification. The machine index leaves final gates, complete inventories/suites, all directed cases, publication and CI explicitly pending. Missing evidence cannot be replaced with old G1 or another candidate. Later actual results require a new evidence version; failures and this cutoff remain unchanged.

The immutable [G1 record](experimental-scoped-child-join-resume-2026-10-19.md) remains specific to head343 and run39228803615: published-Join recovery, a zero-I/O child failure prerequisite, same-process reconnect and Active Root handoff. That CI contains no G2 code. Pre-drive child quarantine refusal and final atomic fresh guards remain; preflight does not revoke concurrent I/O after its gate.

General scoped cancellation, the separate cancellation-F2 matrix, stdio ownership/real platform consumers, historical windows, R6 capacity/deployment and R7 release remain open. This document changes neither Draft PR161/RFC0022, Supported status nor R1/R6/R7 closure. The concise local evidence index is not a portable archive.


## 2026-10-20: final-v1 observations and unqualified repair diagnosis

This append-only section binds the earlier frozen `scoped-child-join-g2-final-source-v1` (962 source files, 560 compiled inputs), not the future composition containing this section or a repair. The earlier 959/560 cutoff, failures and pending statements retain their historical scope. On that 962/560 source all six gates-v2 actually exited 0: workspace 2155 passed with 6 existing ignored, and 48 independent doctests passed.

Complete Native-v2 ran on a macOS harness with retained PostgreSQL containers; it gives no Linux-host qualification. Store PG16/17 each passed 272/0 failed/0 ignored/0 filtered. Runtime PG17 passed 216/0/0/0. Runtime PG16 exited 101 with 215 passed/1 failed/0 ignored/0 filtered: partial-spawn hit the original 90-second cold-process readiness wait. All four wall/monotonic differences were below 0.054 seconds in absolute value; Root recorded an awake round without the earlier sleep/wake interference. The failure therefore remains under stable clocks; the complete pair did not pass. The earlier sleep-affected Native-v1 failures are retained too.

The same saved Runtime binary `955d35d2a32f175f337925a82ca33296eb575c36810b67cbb75cc6f09c675beb` subsequently passed the PG16 exact diagnostic in 47.081 seconds, 1 passed/215 filtered, exit 0. That single-case rerun does not replace the failed complete suite or grant full-suite, CI, performance or capacity qualification. The failed database's read-only journal reaches sequence 22, Root-node `graph-node-attempt-succeeded`, with guardian renewal observed. This does not distinguish the end of the first `cold_driver.drive` from the later repeated drive at frozen-source line 1316 after the first returned. The repeated drive can start its guardian without appending journal events. Neither first-drive localization nor a unique DirectEvidence root cause is established.

Confirmed narrow overhead is repeated complete reader calls for the same child keys, physical AttemptIds and activation Join within one `usage_at`. The proposed test-only repair loads each complete record once within that call, while rescanning every original key for every spawn/settlement event and retaining unique matching, settlement, tenant/run/digest and all original predicates against the same authenticated journal page. Every new call reloads parent admission/account/journal/head and its required complete records. It adds no cross-call cache, changes no Store/Driver live/quarantine/write guard, increases neither the 90-second readiness limit nor any business budget or fence, and has no post-repair timing result yet. Reducing confirmed duplicate work does not prove the unique timeout cause.

Root must independently compute the later source/binary identity; this document embeds no hash of itself or the new global source. The repair composition, new six gates, complete Native on both majors, publication and fresh CI remain pending. G1's exact head/run qualification does not transfer. The old Clippy 101, admission-budget failures, intermediate successes and all earlier limits remain; RFC Draft, R1/R6/R7 and all remaining exit criteria stay open.
