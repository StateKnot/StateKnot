<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0024: Owned, bounded MCP stdio sessions

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-10-15
- Tracking issue: https://github.com/StateKnot/StateKnot/issues/140
- Supersedes: None
- Superseded by: None
- Review baseline: c7d8768356b30207cd637d81e406056b745d35ac
- Note: implementation unavailable; public names remain provisional while Draft
- [中文对照](0024-owned-bounded-mcp-stdio.zh-CN.md)

## Summary

Add an owned MCP stdio session that starts an explicitly configured executable,
admits its complete bounded Tool catalog, routes concurrent requests, and owns
shutdown of the entire contained process tree. A cloneable client shares admission state controlled by the
session; it has no independent OS-owner capability. Dropping a request never
transfers process ownership to that caller. The modern protocol profile is MCP 2026-07-28. HTTP MCP retains
its existing contract.

This is a proposed contract, not implemented or Supported stdio functionality.
Acceptance requires resolving the platform owner and dependency questions
below. Parser tests, an SDK child transport, and termination of the initial
child alone cannot satisfy this RFC.

## Motivation

Issue 140 requests local Tool discovery and calls from JiaClaw. R1 requires
literal arguments, explicit environment, bounded protocol resources, complete
tree cleanup, and a separately pinned external consumer. The existing general
MCP client supports HTTP and interactive results; the durable MCP Adapter also
has stricter authorization and uncertain-effect semantics. Both must remain
usable without a caller assembling a second protocol implementation.

The [process-ownership research](../research/mcp-stdio-process-ownership-2026-10-12.md)
records the exact SDK and platform limitations. In particular, process groups
do not provide permanent membership for arbitrary descendants. A process
owner therefore needs an independently verified containment mechanism.

## Goals and non-goals

- One owned session Module hides discovery, catalog admission, framing,
  correlation, bounded queues, stderr draining and process cleanup.
- A shared protocol Module reuses the current discovery, metadata, descriptor,
  result, notification and interactive-result semantics. HTTP authorization
  headers and HTTP retry rules remain specific to that Adapter.
- The durable Tool Adapter preserves a committed durable attempt start before dispatch,
  per-attempt authorization, and Unknown + ReconcileFirst for uncertain writes.
- A platform is supported only after native process-tree and host-crash tests.
  Unsupported containment is rejected before executing the configured server.

Legacy negotiation, automatic installation, automatic restart/replay,
cross-tenant process sharing, and a general OS sandbox are outside this profile.
RFC-0005's compute-only restriction is unchanged.

## User-facing design

The proposed Interface has an owning `McpStdioSession` and a cloneable
`McpStdioClient`. Construction validates configuration without spawning.
`start(&mut self)` owns discovery and admission. After its first poll begins
startup, dropping the unfinished Future synchronously closes admission and signals the
independent owner to stop; it does not merely leave cleanup as the caller's
next optional action. The session retains the owner and can await cleanup.
Dropping an unpolled start Future launches nothing and leaves Configured. A client is available only after
complete admission. No partial catalog is published.

Configuration contains an absolute executable, literal OS arguments, explicit
working directory and environment, a bounded policy, a local schema registry,
approved descriptor pins, expected server implementation identity, one trusted
local security scope, and an operator-configured containment profile.
There is no shell string, PATH lookup, inherited environment filter, implicit
installer, or fallback to a different executable/profile.

The caller can inspect sanitized session status, obtain its admitted catalog,
call an admitted Tool, and request explicit shutdown. `begin_shutdown()` closes admission synchronously; `shutdown(&mut self)`
requests cleanup and `wait(&mut self)` observes it without initiating a new
stop. Dropping either wait retains ownership. Shutdown succeeds only with a
confirmed tree-empty/reaped report; pending or failed cleanup is an error,
with the original reason and recovery status available on the retained owner. It does not replace the original call error. Pending cleanup retains
an owner and a recoverable containment identity; it never reports success or
permits another generation to reuse that identity.

Reuse `McpTool`, `McpToolCallResponse`, `McpCompleteToolResult`,
`McpNotification` and `McpInputRequired`. Preserve the existing public resume
signature using a private HTTP/stdio continuation enum. The existing
non-exhaustive client error gains sanitized stdio classification; its public
name need not be renamed. Configuration/startup/cleanup errors distinguish
invalid configuration, unsupported containment, admission refusal, protocol
failure, exhausted limits, timeout, and owner failure. A call error includes
`NotSent` or `MayHaveBeenSent`; neither an OS error nor server text is included
in Display or Debug of that error. Every stdio call failure, including local
validation or MRTR-key refusal, must retain its own send phase. Shared protocol
parsing errors are wrapped by the stdio Adapter rather than escaping through
old HTTP-specific variants without a phase.

The current McpRemoteError, McpCompleteToolResult and McpNotification derive
Debug containing raw remote text/data; McpToolCallResponse exposes those Debug
implementations transitively. Reusing these types does not already provide
redaction. The proposed Implementation must replace those Debug derives with
bounded payload-free formatting and test HTTP and stdio formatting. Explicit
raw accessors still return untrusted data. This is a deliberate diagnostic
behavior change; HTTP wire/auth/replay behavior is unchanged.

Public names, exact configuration/error/report types and a realistic example
must be compiled in an external consumer before acceptance. The prose above does not claim that these Rust types already exist.

## Detailed semantics

### Ownership and state

One generation follows `Configured -> Starting -> Ready -> Stopping -> Closed`.
Startup/admission failure goes to Stopping. A terminal reason is retained
separately from cleanup status. There is no transition from Stopping to Ready
and no implicit restart. A new session requires fresh configuration/admission.

The owner exists before untrusted execution can begin. It owns the containment
identity, initial child, stdin/stdout/stderr handles, dispatcher and pending
requests. The start stop-guard is disarmed only when complete admission and
Ready publication succeed; ordinary destruction of an already completed start
Future is not cancellation. Client clones do not own independent pipes or keep a retired
generation usable. Explicit shutdown, final session drop, runtime teardown,
host crash, or loss of the owner control connection all have a defined cleanup
path; a Tokio task alone cannot be that crash-surviving owner. A stop guard is
installed before the first possible target execution and retained across every
await. Normal work cannot fill or block the reserved stop path. OS cleanup
fallback must work when no Tokio runtime is running; Drop must not depend on
spawning a new async task. Clients and returned MRTR continuations must not
retain the last Job/containment-control handle after Session drop.

Owner control loss makes observed readiness false and closes new admission.
A call already crossing dispatch is still classified conservatively; this is
not an atomic transaction between owner liveness and an external write. The
platform profile must define behavior when the supervisor itself dies or is
killed, not only when its client goes away.

Any call timeout, cancellation, Future drop after admission to the dispatcher,
partial write, or fatal reader/writer/output violation retires the generation.
Other pending calls receive the same terminal reason but retain their own
NotSent or MayHaveBeenSent phase. A queued unsent call is not converted into an
ambiguous write merely because another request caused generation retirement.
A request cancelled before the Module accepts it has not been dispatched. A bounded best-effort
protocol cancellation notification may precede shutdown; it is not evidence
that an external effect stopped or rolled back.

### Protocol and correlation

The client first sends `server/discover`, carries the existing three inline
metadata values, and requires intersection with MCP 2026-07-28. A silent or
legacy-only server fails within the startup deadline. Unknown errors never
trigger a legacy fallback or implicit replay. Discovery must advertise Tools
and the configured server name/version assertion before catalog admission.
Self-reported serverInfo is not executable or cryptographic attestation.
This version profile has no server-request support.

Each generation has one reader and a serialized writer. Allocate and register
a unique pending ID before any request byte can be sent. IDs do not wrap or
repeat in a generation; numeric and string IDs are distinct. Tool binding IDs
come from one checked process-wide allocator shared with HTTP and all stdio
generations; a foreign or retired Tool can never match another Adapter merely
because two local counters started at 1. Unknown,
duplicate, id-less or stale responses fail the generation. Notifications use
their actual progress token; unassociated logs do not acquire the identity of
the current call. Allocate active progress tokens using the pinned SDK's real
request _meta shape. Only notifications provably correlated to that request
appear in its McpToolCallResponse; unassociated logs/unknown notifications go
to a separate bounded sink/counter, never the current request. Unknown tokens
follow an explicit bounded discard/refusal policy, not a growing lookup table.
Subscriptions remain disabled, and the profile must not advertise them.

The LF scanner bounds bytes while reading fixed chunks. It accepts one terminal
CR before LF, rejects malformed UTF-8, batch values, duplicate JSON keys, bare
CR, stdout log lines, empty frames and EOF with an incomplete frame. Parsing
uses Core's bounded JSON dimensions in addition to the byte ceiling. A record
ceiling excludes LF but includes an optional terminal CR; every byte read,
including delimiters, counts toward generation output. Buffer headroom for a
fixed read chunk and delimiters is separately bounded. No
`read_until`-then-check allocation is permitted.

Startup traverses the entire bounded catalog, counting rejected entries and
all pages. Cursor cycles, ambiguity, an absent approved Tool, pin drift, or a
local schema mismatch fail admission atomically. Inputs are locally validated
before dispatch; structured successful outputs are locally validated afterward.
The server cannot cause network schema resolution or unbounded compilation.
An admission entry pins a remote name, the complete reviewed descriptor digest
and local input schema, plus an explicit optional local output schema policy.
Advertised output-schema presence and canonical bytes must match that policy;
a structured successful output required by the policy cannot be omitted. A
general text-only Tool can be admitted under an explicit no-output-schema
policy; the durable Adapter still requires its existing structured output
contract. Any duplicate approved name is fatal, even if one duplicate would
otherwise be rejected. The strict initial profile rejects x-mcp-header and
non-forbidden Task execution on approved Tools; stdio has no header promotion
and never rewrites arguments into HTTP headers.

Interactive continuation retains the original generation, opaque state,
arguments and exact requested keys. Resume consumes that continuation and is
an explicit new round; it never migrates to a replacement process. The durable
write profile rejects interactive/incomplete outcomes just as its HTTP Adapter
does, retaining uncertain-effect accounting after possible dispatch. MRTR
state/input requests/original arguments held in returned continuations remain
charged to the generation's bounded continuation count and byte/node budgets
until consumed or dropped. This retention is distinct from an in-flight round.
A retired continuation cannot restart a process or obtain new credentials.
InputRequired is an untrusted request for host action, not permission to read
roots, run sampling or display/answer elicitation automatically. The host
explicitly authorizes responses under the same frozen security scope. Resume
rechecks generation/admission and bounded input-response serialization, retains
the exact original arguments/state and allocates a fresh correlation ID.

### Deadlines and limits

All limits are finite, nonzero, checked at construction, and have hard ceilings.
Startup includes preflight, containment, spawn, discovery and full admission.
One absolute call deadline includes permits, queue residence, write/flush and
response, local validation and per-attempt policy; cleanup uses a separate
absolute deadline shared by every stop step. Synchronous host callbacks must be
bounded and nonblocking, and deadlines are rechecked after them. A Rust Future
timeout cannot preempt an uninterruptible OS operation; missing cleanup proof
returns Pending with an independent owner, never a fictional clean result.
Sequential steps cannot each restart the entire timeout.

Proposed initial defaults are startup 10 seconds, call 30 seconds and cleanup
5 seconds. Proposed byte/count defaults are frame 256 KiB (hard Core 2 MiB),
16 active-plus-queued calls (hard 1024), 8 MiB aggregate retained request bytes
(hard 64 MiB), 16 catalog pages (hard 64), 1024 advertised Tools (hard 4096),
8 MiB retained catalog (hard 32 MiB), 1024 notifications per call (hard 4096),
and 64 KiB retained stderr (hard 256 KiB). These are design inputs, not capacity
results; the accepted contract must fix and validate every remaining hard
ceiling, generation lifetime/request/output totals and stderr rate policy.

The accepted limits must distinguish actual raw-buffer capacity, retained
compact JSON bytes, retained JSON node counts and bounded object counts.
Compact/wire bytes are not exact allocator/RSS bytes. Reserve before allocation
or copying; move a reservation with owned storage, and share it only for one
immutable allocation. Cloning a materialized JSON value requires a new charge.
Reservations survive result handoff queues and MRTR continuations, and are
released exactly once when the last charged storage is dropped. Oversize
outbound serialization uses a bounded sink, not an arbitrarily large Vec that
is checked afterward. Calls waiting for a permit must not become an unbounded
queue of captured argument clones.

The Module budget covers queued objects, active state, writer buffers,
undelivered replies and its own retained continuation state, including rejected
catalog entries. Ordinary completed values already handed to trusted callers,
caller-created clones, and inputs in caller-owned/unpolled request Futures are
outside this per-session storage guarantee;
this limitation must be stated, with host session and OS memory quotas supplied
by the deployment. The Interface cannot bound arbitrary caller allocations. Unknown messages
and whitespace consume output budgets. Stderr drains from spawn independently
of user consumption; retention exhaustion never blocks the child. Crossing a
total-output/rate limit retires the generation. A slow notification consumer
cannot backpressure the reader or create an unbounded queue.

### Dispatch and shutdown

Immediately before the first possible byte write, transition the call to
MayHaveBeenSent. Partial writes poison the shared generation; another frame
cannot be appended after an incomplete request. No automatic retry is allowed.
A complete remote JSON-RPC error, a Tool `isError` result and a transport
failure remain distinct outcomes. Completion versus retirement is linearized
by the dispatcher; it must not overwrite a delivered validated outcome or
pretend that an undelivered outcome was committed to the durable ledger. None alone proves that a durable write had
no external effect.

For stdio durable writes, any post-send loss, isError, incomplete response or
invalid/unverifiable result is Unknown + AmbiguousExternalOutcome +
ReconcileFirst; only a locally validated successful result follows normal
ToolResult completion. This is explicitly stricter than the current HTTP
McpRemoteTool::invalid_result mapping, which uses Applied + Never for write
risks after a malformed complete result. That HTTP behavior is not changed by
this RFC and must not be accidentally imported into stdio as a shared helper.
The existing HTTP incomplete/isError/transport paths already use Unknown.

Shutdown closes admission, attempts bounded cancellation, closes stdin, allows
a bounded grace period, force-terminates containment, verifies tree-empty and
reaping, then joins owned I/O/dispatcher work. Its report preserves both the
original failure and actual cleanup result. Drop can request cleanup but
cannot synchronously claim verified completion.

## Persistence and migration

Session configuration, clients, owners, grants, credentials and pending
continuations are transient and implement neither Serialize/Deserialize nor
JsonSchema. No database migration, Core checkpoint encoding, Tool ledger wire
change or A2A field is introduced. Existing durable tool origin events remain
the authority for dispatch.

A crash-surviving Linux owner needs a versioned, restricted recovery record
for boot/generation and the real containment identity. PID alone is never
recovery authority. Its atomic-write/permission/validation/retention contract
must be fixed alongside the selected owner Implementation. Recovery cleans up
old generations; it does not re-execute their uncertain Tool calls. The selected
owner must persist recoverable ownership before target execution, with atomic
write/fsync and directory/record permission rules, and remove a record only
after verified cleanup. Crash cuts before/after record creation, spawn, kill,
proof and record removal must not lose ownership or authorize cleanup of an
unrelated reused path/PID. A record by itself does not kill a live tree.

## Security and privacy

Clear the child environment and set only explicit bounded key/value entries.
Validate OS encoding lengths, NUL, invalid keys and platform-equivalent duplicate
keys. Redact executable arguments, environment values, working-directory data,
stderr and remote error text by default. Only explicitly permitted standard
pipes and owner handles may be inherited; owner-control capabilities must not
be inherited by the server.

On Windows, literal argument support is limited to verified native executables
using the declared CRT/Rust argument convention; `.bat`/`.cmd` and automatic
`cmd.exe` invocation are rejected. Unix passes OS arguments directly to exec.
An explicitly configured interpreter remains an operator-reviewed executable.

Descriptor pins authenticate the admitted Tool description, not executable
bytes or external effects. The containment deployment separately fixes OS
identity, file/network access and CPU/memory/PID quotas. A server may not gain
permission to migrate out of its containment or interfere with its owner.

The durable Adapter uses an explicit local-process authorizer with immutable
Tool/context/input facts and frozen process identity. It defaults to refusal.
It does not change the HTTP authorizer's endpoint contract or pretend that
process-lifetime environment credentials are per-attempt Bearer credentials.
The trusted host must bind the process scope before startup and resolve the
Run's principal/consent from its own records; ToolContext alone is not proof
of an external principal. One process is never shared across different
tenants/security principals.

Environment credentials are released when the process starts, before Tool
admission and before later per-attempt checks. Host refusal prevents a tools/call
send; it cannot revoke credentials already copied by the process or prove that
the executable made no background side effects. This is an explicit authority
and data-release decision for the reviewed executable. There is no credential
refresh/rebinding of a live generation. The host must call begin_shutdown on
trusted revocation/expiry; the Module does not discover credential expiry from
an environment value. This closes admission and retires the generation;
reconstruction authorizes new work only, never an
Unknown write retry. No all-buffer zeroization or secret-free core dump is
claimed merely because Debug is sanitized.
Possible writes without authenticated complete results remain Unknown with
AmbiguousExternalOutcome and ReconcileFirst. Killing a process does not change
that durable fact.

### Platform ownership profiles

Linux requires an operator-controlled cgroup-v2 owner/supervisor that places
the process in its domain before target execution, prevents migration, applies
tree-wide kill, observes populated=0 and reaps owned children. Host control
loss and supervisor failure/restart need a verified enclosing lifecycle and
recovery path established before target execution. The kill authority must be
outside the cgroup it kills. The target must lack migration, ptrace/signal,
control-IPC and namespace/privilege paths that can defeat the chosen owner.
A separately managed supervisor or kernel-backed enclosing lifetime must
handle the inner supervisor's death. Stale record scanning at a later startup
alone cannot promise prompt crash cleanup. A stand-alone helper that itself can disappear leaving children
does not satisfy this requirement. The final dependency/deployment decision
must prove these points, including concurrent fork and early direct-child exit.

Windows requires a non-inheritable Job Object with kill-on-last-handle-close,
no breakaway, suspended creation followed by successful assignment before
resume, a handle whitelist, Job termination and actual ActiveProcesses==0 plus
initial-child wait. Failed assignment never resumes the target. Native owner
handle lifetime and crash cleanup require independent tests. This is not a
complete arbitrary-executable containment recipe by itself. The selected token,
DACL/handle and enclosing-job policy must deny access to outside-process
PROCESS_CREATE_PROCESS/PROCESS_DUP_HANDLE or equivalent interference paths,
including injection, DACL rewriting and privilege escalation. An ordinary
same-user unrestricted token cannot simply be presumed to satisfy that policy.
Microsoft documents that PROC_THREAD_ATTRIBUTE_PARENT_PROCESS can inherit the
selected parent's Job; no-breakaway alone does not establish the necessary
alternate-parent policy. No usable owner Job handle may be inherited,
duplicated into, or kept open by the target. WMI or another external launch
broker is not contained automatically: either the deployment prevents that
authority or its work is explicitly outside this process-tree contract. The
accepted Windows profile must state that scope without calling it arbitrary
host-wide isolation. Nested-job compatibility, assignment failure, owner kill,
last-handle close, alternate-parent and handle-duplication fixtures are required.

macOS native arbitrary-executable containment is currently unsupported by the
reviewed mechanisms. Native-profile selection fails before any target/helper
spawn or release of environment credentials; no probe runs the configured
executable first. A sentinel test must prove this refusal. A future VM/container
profile must explicitly declare its different executable/filesystem/credential
domain and obtain separate native evidence; it is never an automatic fallback.

Existing crates retain `forbid(unsafe_code)`. A low-level safe owner dependency
or companion requires explicit review. Any proposed local unsafe policy
exception must be separately justified and approved through the RFC process;
it is not authorized by silently omitting workspace lints. Merely enabling an
SDK child-process feature does not resolve these ownership requirements.

## Observability and operations

Expose bounded local generation/phase/failure-kind counters, queue/output
budget exhaustion, startup/admission refusal, and cleanup pending/failure.
Do not label metrics with Tool arguments, raw remote text, paths or credentials.
Readiness means complete catalog admission and a live owner, not an open pipe.
Pending cleanup is actionable and prevents generation reuse; operators can
inspect its sanitized identity and invoke the authenticated owner recovery
path. No RPO, RTO, throughput or SLO claim precedes final native measurements.

## Compatibility

Retain Rust 1.88 and the current HTTP Interface, wire behavior and MRTR resume
signature, except for the explicitly safer shared Debug formatting described
above. No rmcp upgrade or child-process feature is implicit. Current rmcp
3.5.0 and Tokio 1.53.2 are the inspected baseline. Target-specific explicit
dependency edges, feature flags and the owner deployment are reviewed before
acceptance; a transitive low-level crate is not an existing direct Interface.

## Alternatives considered

Wrapping the existing SDK child transport is smaller, but does not establish
the bounded parser, total budgets and complete tree/crash oracle required here.
PGID termination is insufficient for descendants that change session/group.
Recursive PID scanning has a fork/reparenting race. A parser-only initial
client would move lifecycle correctness to every consumer. An arbitrary
transport trait would add an unsupported hypothetical Adapter surface; the
real shared Seam is protocol semantics between HTTP and stdio.

## Validation and rollout

1. Review the full Interface, platform/dependency choice and fixed resource
   policy before accepting the RFC. Internal unchanged HTTP extraction may
   precede acceptance but grants no stdio qualification.
2. Test discovery/version/metadata, complete catalog limits and last-page drift,
   schema/pin refusal, reverse-order concurrent replies, exact ID types,
   interleaved progress/logs, HTTP/stdio cross-binding refusal, held MRTR leases,
   caller-retained result scope, MRTR generation binding, malformed/oversized/deep
   JSON, no-LF and partial EOF, cursor cycles, unknown/duplicate replies and all
   aggregate budgets through the public session/client Interface.
3. Exercise permit/write/flush/response deadlines, Future drop, partial writes,
   blocked writer, stderr saturation and every startup/stop failure. Verify
   MayHaveBeenSent and durable Unknown without automatic retry.
4. On each supported platform, independently observe tree-empty/reaped after
   success, failure, timeout, cancellation, final owner drop, runtime teardown,
   host kill, inner supervisor kill, outer owner loss and owner restart. Fixtures
   must verify every surviving OS member/owner handle and crash cut, including
   direct-child early exit,
   held pipes, ignored EOF/TERM, session changes and bounded fork contention.
   Escape fixtures run only inside already established real containment.
5. Run existing HTTP authorization, MRTR, descriptor pin, durable Tool, protocol
   conformance and full workspace/release gates. Unsupported targets prove
   rejection before executing a sentinel server.
6. An independent external consumer pins the exact crate/revision and exercises
   real discovery/call, `isError` versus transport error, admission refusal,
   cancellation and verified cleanup. Preserve binary/source/environment,
   actual markers and default resources in bilingual qualification material.

Issue 140 and R1 close only after the complete Implementation and all applicable
evidence pass. A Draft RFC or a positive harness summary with conditional
resource branches does not close either.
Completing stdio alone cannot close R1's other semantics, identity, compatibility
and qualification requirements. The supported-platform matrix and external
consumer must match the actual agreed deployment; macOS rejection is an honest
capability limit, not evidence that a macOS consumer's native use case works.

## Unresolved questions

- Select and audit the precise Linux crash-surviving owner deployment and
  Windows safe Job owner dependency; demonstrate their complete invariants.
- Fix remaining hard time/rate/generation ceilings and the aggregate accounting
  representation, then validate the proposed defaults without enlarging limits
  to pass tests.
- Freeze the compilable public configuration, error and cleanup report types,
  external consumer revision, and actual initial supported-platform matrix.
- Fix the MRTR/returned-value accounting representation, call-error phase
  wrapping, shared Debug change and explicit stdio-versus-HTTP invalid-result
  mapping in the final compiled Interface.
- Prove the Windows security/handle/alternate-parent profile and Linux enclosing
  owner lifecycle; neither is established by the dependencies inspected so far.

These affect the public contract, so this RFC remains Draft until resolved.
The next validation exercises owner capabilities, escape attempts and crash
cleanup on one named deployment before Tool publication. Select an exact
dependency/companion revision, inspect its sources, then test the complete
oracle under the fixed limits. Unsupported capability
must fail before executing a sentinel target. A private unchanged-HTTP protocol
extraction can proceed independently, but is not stdio acceptance.

## Review sources and status

This reviewed draft changes no source, dependencies, public Rust types or unsafe
policy and has not been compiled or run. Source observations use the current
c7d8768 tree; rmcp 3.5.0 and Tokio 1.53.2 remain the fixed inspected versions.
The linked research records the fixed SDK source observations. This RFC's
reviewed requirements remain proposals until the Implementation and platform
evidence are complete.

- [Current HTTP types](../../crates/stateknot-integrations/src/mcp_client.rs)
- [Durable HTTP mapping](../../crates/stateknot-integrations/src/mcp.rs)
- [Authorization Interface](../../crates/stateknot-integrations/src/mcp_authorization.rs)
- [Core bounded JSON](../../crates/stateknot-core/src/json.rs)
- [RFC process](README.md)
- [Fixed Windows parent-attribute documentation](https://github.com/MicrosoftDocs/sdk-api/blob/d43984d7c322f7d7d35704d6d1ad36dc3539703b/sdk-api-src/content/processthreadsapi/nf-processthreadsapi-updateprocthreadattribute.md)
- [Fixed Windows process-rights documentation](https://github.com/MicrosoftDocs/win32/blob/3aee48ac3f48e187d17bdfd56951741737331749/desktop-src/ProcThread/process-security-and-access-rights.md)
