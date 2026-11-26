<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0026 — A2A timestamp migration for the next preview

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-11-26
- Related: [dependency policy failure](https://github.com/StateKnot/StateKnot/actions/runs/45785800604), [patched dependency baseline](https://github.com/StateKnot/StateKnot/pull/187)

## Motivation

The published `stateknot-integrations = "=0.1.0-alpha.1"` exposes
`chrono::DateTime<chrono::Utc>` in four A2A methods. RustSec lists chrono as
unmaintained and provides no patched version. A lockfile update alone cannot
migrate an application compiled against those signatures.
[RustSec advisory](https://rustsec.org/advisories/RUSTSEC-2026-0543.html)

## Public API and migration

Introduce `stateknot_integrations::A2aTimestamp` as a copyable, ordered UTC
instant. An application can parse a timestamp received from a peer, pass it to
either API, compare returned values, and serialize the result in UTC.

| Method | Published alpha.1 timestamp | Proposed timestamp |
| --- | --- | --- |
| `A2aTaskStatus::new(state, message, timestamp)` | `Option<DateTime<Utc>>` | `Option<A2aTimestamp>` |
| `A2aTaskStatus::timestamp()` | `Option<DateTime<Utc>>` | `Option<A2aTimestamp>` |
| `A2aListTasksRequest::with_status_timestamp_after(value)` | `DateTime<Utc>` | `A2aTimestamp` |
| `A2aListTasksRequest::status_timestamp_after()` | `Option<DateTime<Utc>>` | `Option<A2aTimestamp>` |

```rust
use stateknot_integrations::{A2aListTasksRequest, A2aTaskState, A2aTaskStatus, A2aTimestamp};

fn example() -> Result<(), Box<dyn std::error::Error>> {
    let at: A2aTimestamp = "2026-10-08T10:00:00.123456789+08:00".parse()?;
    let status = A2aTaskStatus::new(A2aTaskState::Working, None, Some(at))?;
    let filter = A2aListTasksRequest::new().with_status_timestamp_after(at);
    assert_eq!(status.timestamp(), filter.status_timestamp_after());
    assert_eq!(at.to_string(), "2026-10-08T02:00:00.123456789Z");
    Ok(())
}
```

The already published alpha keeps its exact version. Applications opt into
this source change by rebuilding against the next lockstep preview. Callers
that use `None` or propagate an inferred value between the two APIs usually
need only to rebuild. Callers with explicit field types or chrono-specific
methods must update their boundary. See the migration notes in the
[English](../a2a-client.md#timestamp-migration) and
[Chinese](../a2a-client.zh-CN.md#timestamp-migration) client guides.

## Ordering and wire representation

Task status and the client's validation of `statusTimestampAfter` use the
same UTC ordering. The client accepts a returned task at the lower bound.
Offsets identifying the same instant compare equal. Legacy leap
seconds retain their position between the preceding regular second and the
following minute. Preserve nanoseconds and the previously emitted fractional
widths of zero, three, six or nine digits. The HTTP+JSON query and JSON-RPC
parameter encode the same instant. A missing optional timestamp remains absent.
The existing A2A binding and protocol version apply to upgraded peers.

`from_unix_parts(seconds: i64, nanoseconds: u32) -> Option<Self>` provides
an allocation-free bridge from chrono's `timestamp()` and
`timestamp_subsec_nanos()`. The matching readers are `unix_seconds()` and
`subsec_nanos()`. For a leap second the seconds field identifies the last
ordinary second of a minute and the nanoseconds field is in
`1_000_000_000..2_000_000_000`. Invalid parts return `None`.

The old Rust type could also hold a leap marker on an ordinary second through
component editing or a second-resolution fixed offset. Chrono's own
`from_timestamp` rejects those parts. Its A2A serialization normalizes them
on readback. An application that needs this legacy wire value can serialize
the old value and deserialize the resulting JSON string into `A2aTimestamp`.
The original in-memory identity cannot be recovered from that message.

## Persistence and operations

Core continues to represent UTC microseconds in years 0000 through 9999.
The Core reader requires exactly six fractional digits and a trailing `Z`.
It rejects leap seconds. Core schema fingerprints, canonical bytes, digests
and the database column type `timestamptz` are compatibility fixtures.
Replace the internal calendar library and the SQLx codec together. The new
codec must read previously stored values and apply Core range checks on
ingress, including to a database row from outside the supported year range.
The migration adds no SQL and needs no data rewrite.

Before deployment, rebuild the whole application and verify its task filters
against known timestamps. Keep the pre-upgrade backup required by the release
policy. Operational rollback follows the separately qualified restore
procedure for the release being deployed. [Versioning and release policy](../versioning-and-releases.md)

## Dependency policy and alternatives

Use `time` for Core, SQLx and UTC formatting in Integrations. The A2A wire
SDK still parses timestamps using chrono. The same parser reads an
`A2aTimestamp` from text so both entry points accept the published wire
grammar. The application-facing value holds seconds and nanoseconds. Other
upstream users of chrono include `object_store` and `oauth2`. Operators can
inspect the full resolved graph with `cargo tree --locked -i chrono`.
The dependency policy checks direct workspace use of unmaintained crates.
Keep the existing policy and advisory exceptions. The Rust minimum remains 1.88.

Publishing a different general-purpose timestamp type directly would commit
the Rust API to its calendar range and leap-second behavior. Reusing Core's
microsecond timestamp would lose A2A nanosecond values and change task order.

## Validation and rollout

1. Compile a consumer against the new signatures and test the old wire samples
   through status construction, reads and list filters on both bindings.
   Verify offset normalization, fractional widths, leap seconds, date range,
   invalid input and order around second boundaries.
2. Run Core's existing timestamp, canonical wire and schema tests on Rust 1.88.
   Compare writes and reads of historical values through PostgreSQL 16 and 17,
   including a pre-epoch microsecond, supported endpoints and an out-of-range row.
3. Run the locked workspace and fuzz dependency policies, conformance and
   release-package checks on the integrated commit. Submit the compatibility
   decision for independent review before marking it accepted or publishing.
