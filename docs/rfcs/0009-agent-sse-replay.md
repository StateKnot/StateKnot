<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0009: Resumable Agent activity SSE

- Status: Draft (bounded implementation profile, not stable release acceptance)
- Authors: StateKnot contributors
- Created: 2026-09-15
- Tracking issue: https://github.com/StateKnot/StateKnot/issues/50
- Supersedes: None; extends RFC-0008
- Superseded by: None

## Summary and motivation

Expose authenticated `GET /v1/agent-runs/{run_id}/events` with durable activity
notifications and current verified snapshots. Reuse PostgreSQL journal paging and
exact-head validation instead of introducing a process-local replay cache.

## Goals and non-goals

Recover notification progress after disconnect and service replacement. Preserve
authorization-first reads, bounded resources and public-safe output. This is not
token streaming, raw journal export, historical snapshot reconstruction, A2A SSE,
or an exactly-once delivery guarantee. No Agent work runs inside ingress.

## User-facing design

Enable streams explicitly through validated `AgentHttpSseOptions`. Send a bearer
credential, `Accept: text/event-stream`, and optionally one `Last-Event-ID` header.
No query parameters, cookies or URL credentials. Native EventSource cannot supply
this bearer header: use a streaming HTTP client, not an anonymous workaround.

Each `activity` event has an `id` and JSON containing only sequence and database
recording time. Every journal entry produces one notification without exposing
event kinds, payloads, worker identity, schemas or provider text. A `snapshot`
event carries the existing verified response DTO, with no `id` field. Heartbeats
are SSE comments. Only activity IDs advance client replay progress. Complete
application handling before saving a cursor; discard incomplete frames.

## Detailed semantics

The versioned cursor is bounded canonical base64url encoding of an exact journal
head. It is metadata, not a credential or cryptographic authorization token.
PostgreSQL verifies all cursor fields against the retained row after authorization,
then verifies each contiguous suffix and the final durable head. Missing history,
crossed scope, altered or future cursors fail closed; never reset silently.

No cursor starts at sequence one. A supplied cursor resumes strictly after it.
Snapshots and journal pages are separate reads: snapshots are current observations,
not state at an activity sequence, and polling may coalesce intermediate snapshots.
Quarantine changes can occur without a lifecycle revision change, so compare the
complete snapshot, not revision alone. The stream remains open at terminal state
until its finite lifetime expires; EOF never proves successful completion.

Reauthenticate the retained redacted credential and require the same tenant and
principal during each bounded iteration. Resource policy is checked before every
page and snapshot lookup. Already authorized bytes handed to the HTTP stack cannot
be recalled after revocation. Policy changes stop subsequent authorized batches.

## Persistence and migration

No new table or migration; existing durable admission and journal remain the
source of truth. Retain the journal for the required reconnect window. The cursor
contains a version so incompatible encodings can be rejected. Binary rollback
disables this optional endpoint without modifying Runs. Restoration that loses an
acknowledged cursor requires explicit client reconciliation, not an automatic reset.

## Security and privacy

Existing host/origin/body/media/auth validation applies. SSE is opt-in with a
separate nonqueued stream semaphore, finite lifetime, bounded frames, one queued
frame and a bounded producer send wait. Drop/shutdown stop producer work. JSON
request capacity is not held for a stream's lifetime. Reconnect admission still
uses the shared short-request quota. Host TLS, socket write/header deadlines,
replica/tenant quotas and suppression of sensitive access logs remain mandatory.

## Observability and operations

Reuse correlation headers and fixed error codes before response headers. After
200, a best-effort `error` frame has no id; abrupt EOF is always possible. Reconnect
with jitter/backoff and the last processed activity id. Disable proxy buffering
and compression for this route, configure idle timeouts above polling/heartbeat
intervals, and bound total connection lifetime at the host as well. Track active
streams, replay lag and admission errors without Run/credential metric labels.

## Compatibility and alternatives

Reuse workspace dependencies, MSRV and public snapshot types. The rollout also
updates locked Rustls to 0.23.45 for RUSTSEC-2026-0285; no advisory is ignored. The API stays
pre-alpha. Polling only the latest revision loses intermediate notifications and
quarantine-only changes. Returning raw journal records leaks private data. A new
outbox duplicates an existing verified append-only notification source. Full
historical semantic state projection is intentionally a separate capability.

## Validation and rollout

Require real TCP plus PostgreSQL 16/17 tests for prefix replay, exact reconnect
after replacement, live cancellation, hostile cursors, cross-tenant denial,
revocation, limits, shutdown and no inline dispatch. Unit tests cover canonical
cursor parsing, frame limits, slow/unpolled consumers and options. Retain CI
evidence with source/tree provenance. Run existing workspace, protocol, platform
and bilingual website gates. Deploy only documentation, never fixture credentials.

## Unresolved questions

No claim of complete historical state replay or stable release support is made.
Release gates and real deployment qualification remain required separately.

## Reference

[WHATWG Server-sent events](https://html.spec.whatwg.org/multipage/server-sent-events.html)
defines UTF-8 framing, comments and Last-Event-ID behavior; this RFC defines the
application's authenticated replay profile.
