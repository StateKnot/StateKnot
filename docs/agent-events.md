<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Resumable Agent activity events

[中文](agent-events.zh-CN.md) · [HTTP setup](agent-http.md) ·
[RFC-0009](rfcs/0009-agent-sse-replay.md)

This opt-in pre-alpha profile provides replayable activity notifications, not
token deltas or historical state reconstruction. It never executes Agent work.
Only the documentation is deployed on stknot.com, not a public Agent API.

## Enable on an authenticated host

```rust
use stateknot::agent_http::{AgentHttpOptions, AgentHttpSseOptions};

let options = AgentHttpOptions::new(["agents.example.com".to_owned()])?
    .with_sse(AgentHttpSseOptions::default());
// Pass options to AgentHttpService::new with your real verifier and service policy.
```

SSE is disabled unless `with_sse` is used. Keep the JSON API's mandatory verifier,
tenant mapping, resource authorizer, exact Host/Origin policy and TLS boundary.
The endpoint requires the transport `Read` grant AND exact Run read permission.
No cookies, credentials in URLs, query parameters or CORS are introduced.

```http
GET /v1/agent-runs/{run_id}/events HTTP/1.1
Host: agents.example.com
Authorization: Bearer <verified-credential>
Accept: text/event-stream
Last-Event-ID: <last-completely-processed-activity-id>
```

Omit Last-Event-ID on the first connection. It must be a single, nonempty header
when present. JSON endpoints reject this header. Accept must be exactly
`text/event-stream`; the request body must be empty. Use a streaming HTTP client
that supports Authorization. Native browser EventSource does not expose a custom
Authorization header option; do not weaken authentication to accommodate it.

## Two distinct observations

The server sends UTF-8 SSE frames separated by a blank line. An illustrative
activity frame is:

```text
event: activity
id: <opaque-sk1-cursor>
data: {"sequence":"2","recorded_at":"2026-09-15T00:00:00.000000Z"}

```

`AgentHttpActivity` has only a decimal-string journal sequence and database
recording time. Every retained journal entry produces one activity notification.
It does not expose custom event kinds, private inputs, transcript, worker fences,
schemas or provider data. The sequence is not a lifecycle revision. The cursor
encodes public head metadata (including tenant/Run/event/time/checksum), not an
encrypted secret or a bearer credential; treat its encoding as opaque.

`snapshot` frames contain `AgentHttpRunResponse { request_id, snapshot }`. They
have no `id` field and never advance replay progress. A snapshot is sent on every
new connection and whenever its complete serialized content changes. This includes
quarantine-only changes without a new lifecycle revision. Snapshot and journal
reads are separate: the snapshot is observed first and may precede an activity
committed concurrently. There is no claim that a snapshot describes state *at*
an activity ID. Intermediate snapshots can coalesce; activity entries do not.

When unchanged, `: keep-alive` comments keep the connection active. Error frames
also have no id. An SSE parser may inherit its last event ID on subsequent events;
persist progress only while handling an `activity`, not a snapshot/error/comment.

## Reconnect correctly

1. Parse a complete frame. Discard an incomplete frame at disconnect.
2. Apply the activity idempotently, then durably save its entire ID. Deduplicate
   by exact ID or tenant/Run/sequence. Save application progress and cursor together
   if processing has side effects; the transport does not provide exactly-once.
3. Reconnect with current credentials and the last saved Last-Event-ID. The service
   replays strictly after that exact event, even in a fresh OS process/replica.
4. Use bounded exponential backoff with jitter on EOF/429/503. A best-effort
   `error` event can explain termination, but is never required for recovery.
5. For 401 refresh/reverify credentials; for 403 stop and resolve permission. For
   `409 agent_http.invalid_cursor`, reconcile history explicitly: do not silently
   erase the cursor and replay as if nothing happened. Malformed/noncanonical
   encodings are 400; missing authorized Agent is 404; integrity failures are 500.

The store checks every cursor field against its retained event, then verifies a
contiguous hash-chain suffix. Altered, cross-tenant, cross-Run, future and
unavailable historical cursors fail closed. Unkeyed checksums detect corruption,
not a privileged database operator rewriting an entire history. Retain journals
for the required reconnect window; restoring an older backup may invalidate a
previously acknowledged cursor. RPO/RTO remain database/host responsibilities.

Do not interpret EOF as successful completion. Read the snapshot's verified
terminal outcome. `CancellationRequested` is still nonterminal. Even a terminal
snapshot does not close the stream immediately: it can still observe quarantine
until the configured finite lifetime ends. Stop explicitly when the application
no longer needs observations.

## Resource and revocation bounds

Defaults: 16 active producers, 60-second connection lifetime, 1-second idle poll
and heartbeat interval, 5-second queue reservation timeout. `AgentHttpSseOptions::new`
accepts 1–128 producers, 1–600 seconds lifetime, 50 ms–15 seconds polling/queue wait
(neither may exceed lifetime). Hard per-stream output is 64 MiB. Each batch is
bounded by the existing `max_response_bytes`, including SSE framing; a snapshot
plus one activity may need more space than the JSON snapshot alone.

The producer reserves its single-slot queue before reauthentication and database
work. One batch has at most one snapshot and one activity; each database page
materializes at most two private events for suffix lookahead and returns one
public head. Replay reads immediately while another event is available, otherwise
polling waits. This prioritizes bounded memory and correctness, not an unmeasured
high-throughput claim. Benchmark pool/verifier capacity with your actual journals.

Credential and exact resource policy are rechecked for every iteration, including
heartbeats; tenant/principal remapping fails closed. The JSON deadline bounds each
iteration and a separate absolute lifetime bounds the whole producer. Async
timeouts cannot preempt blocking verifier/policy code. Revocation is not
instantaneous: the current authorized batch and bytes already handed to the HTTP
stack cannot be recalled. Bound verification and policy latency at their sources.

SSE has a separate nonqueued permit pool; an open stream does not hold the JSON
request permit. Stream setup still shares short-request admission capacity.
Dropping the response aborts its producer. A full, unpolled queue times out without
requiring the body to be polled. Shutdown cancels pending work. A terminal error
is attempted without waiting for queue capacity; EOF is the fallback.

## Production host checklist

- Configure TLS, real credential/policy verification, tenant and replica quotas,
  DB pool capacity and finite socket/header/write/connection limits. Completed
  producers do not own host socket buffers; these limits remain necessary.
- Disable proxy buffering, caching, transformation and compression on this route.
  Responses set `private, no-store, no-transform` and `X-Accel-Buffering: no`, but
  the host must verify that its actual proxy honors the intended behavior.
- Set proxy idle timeout above polling plus bounded verifier/database latency;
  exercise slow readers through the real proxy, not only loopback tests.
- Monitor active connections, replay lag, error classes, pool pressure and drain.
  Suppress credentials, private snapshots and raw paths in logs; avoid Run labels.
- On deployment, stop accepting, call `shutdown()`, bound host drain, replace with
  compatible registries, and reconnect with retained cursors. Retain a rollback
  binary and qualified database backups. No migration is introduced here.

## Qualification

`cargo test -p stateknot --test agent_http --locked -- --nocapture --test-threads=1`
uses `STATEKNOT_TEST_DATABASE_URL`; set `STATEKNOT_REQUIRE_POSTGRES_TESTS=1` to make
missing configuration fail instead of skip. CI runs PostgreSQL 16/17 and retains
separate HTTP/SSE evidence markers with source/tree provenance. Tests cover a new
OS process recovering the exact suffix, live cancellation, hostile cursors,
revocation, quarantine-only observations, producer lifetime, unpolled consumers,
shutdown and no inline dispatch. Full host-role/release qualification remains open.
