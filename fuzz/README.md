<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Bounded Core fuzz qualification

This private workspace runs three actual production boundaries:

| Target | Input and oracle |
| --- | --- |
| `bounded_json` | Raw bytes through strict parsing with narrow/default/hard JSON limits; exact compact statistics, bounded round trips, RFC 8785 bytes and digest stability. Duplicate names and malformed Unicode remain raw bytes. |
| `core_readers` | `RustTypeName\nraw JSON`, then the bounded JSON gate and one of all 313 reviewed readers; accepted input must match its offline deserialization schema, producer output its serialization schema, and ordinary/canonical round trips remain stable. Input validation runs after the actual reader, so alternate accepted shapes cannot be hidden. Names preserve reproducers when the type list grows. |
| `schema_registry` | Bounded `{"schema": ..., "instance": ..., "bad_pin": false}`; the real immutable runtime registry verifies dialect/URI/pins, duplicate identity, registration atomicity, 32 KiB single/48 KiB aggregate schema limits, offline compilation and instance validation. Missing dialect/URI receive the fixed fixture identity; supplied values are never overwritten. |

The reader list is shared with the closed Core inventory, and fixed positive
seeds are derived from its exact fixture pointers. All 298 alternatives of the
71 serialized public enums are also replayed from the separately sealed variant
fixture; a regression checks every alternative against both real schema oracles.
Variant seed identities include the case digest, so adding a case does not
renumber existing alternatives. The permanent `seeds/`
directory retains malformed UTF-8, surrogate escapes, duplicate names, unknown
authority fields, incompatible shapes and the optional producer regressions.
The timestamp regression retains a malformed nested Run transition and checks
rejection by the real typed reader; the Core suite independently covers every
ASCII non-digit position and fixed-length Unicode replacement.
Three object-shape regressions retain a positional SchemaReference and nested
ToolInput/child-admission schema references. The actual reader rejects them;
accepted values must satisfy the input-schema oracle after deserialization.
The object contract and binary-format compatibility limit are specified in
[RFC-0021](../docs/rfcs/0021-core-object-readers.md).
Deterministic generators add deep structures, oversized strings/keys/container
counts and schema byte ceilings. Each run's `seeds.json` records source, bytes and
SHA-256 for every generated seed. No historical version is invented.

On Linux or macOS, from the repository root:

```console
rustup toolchain install 1.98.1 --profile minimal
rustup toolchain install nightly-2026-10-07 --profile minimal
cargo +1.98.1 install cargo-fuzz --version 0.13.2 --locked
cargo +nightly-2026-10-07 fetch --locked
cargo +nightly-2026-10-07 fetch --manifest-path fuzz/Cargo.toml --locked
python3 fuzz/qualify.py
```

The fixed runner first compares all resolved dependency versions with the
product lock, runs seed regressions, and builds all three ASan targets without
`cfg(fuzzing)`. It replays every seed explicitly before coverage-guided mutation.
Each mutation phase is bounded by 10,000 executions and 60 seconds, each input
by 10 seconds and 128 KiB, and each process by a 2,048 MiB libFuzzer RSS ceiling.
The harness separately checks input length; JSON/schema dimensions retain their
own stricter limits. Compilation uses two jobs and finite deadlines. The runner
owns process groups and terminates descendants on timeout or interruption.
The ceilings are failure gates, not a claim that every accepted schema can be
validated within a production request deadline.

Each run starts with a fresh coverage directory under `results/qualification-*`.
The latest report is also saved as `results/qualification.json`; source files
and both lockfiles must remain byte-identical throughout a run. The only added registry packages
are fixed `libfuzzer-sys` 0.4.13 and `arbitrary` 1.5.0. Neither joins the product
workspace or its MSRV/runtime dependency graph. The ordinary `deny.toml` policy
also checks this graph; `deny.exceptions.toml` adds NCSA permission only for the
exact libFuzzer package. Preserve its bundled NCSA/MIT/Apache attribution when
redistributing fuzz executables; no advisory is ignored.

Qualification records source commit and dirty state, compiler/engine, lockfile
hashes, flags, seed replay, retained corpus size and log/binary hashes. A dirty
local run is development evidence. The immutable CI run is the merge gate;
CI retains reports, discovered coverage inputs and failures for 30 days.
Each run retains a bounded number/size of new coverage files; previous local
evidence and crash files should be archived under the operator's storage policy.

To reproduce a saved failure without mutation:

```console
cargo +nightly-2026-10-07 fuzz run core_readers fuzz/artifacts/core_readers/crash-<digest> --no-cfg-fuzzing -- -runs=1 -timeout=10 -rss_limit_mb=2048
```

Retain the exact source/lock/compiler and original bytes. Minimize a confirmed
failure, put the reviewed synthetic case in the matching `seeds/` directory,
add a direct invariant regression and fix the actual production boundary.
Do not remove an oracle or bless a new schema/wire pin to silence a failure.
Fuzz inputs and crash artifacts must remain synthetic; never add credentials,
provider payloads or customer data.

The source-only optional output correction is specified in
[RFC-0020](../docs/rfcs/0020-core-optional-output-schemas.md). All 308 input pins
remain fixed, and a separate inventory fixes the 308 serialization pins.
`schema_pins` prints candidate output pins for explicit review;
`schema_pins --deserialize` prints candidate input/default pins. Neither command
rewrites fixtures. Actual parent-source output documents are retained only as
regression baselines; they are not N-1/N-2 upgrade qualification.

The RFC-0022 draft adds six strictly read frame/call data types to the reader
set. Current inventories pin 314 input/default and 314 producer schemas.
`CompiledGraph` and `ChildRunAdmissionIntent` explicitly change both profiles
for the new optional call metadata.
These experimental data records do not enable nested execution. Final ASan
qualification of the ongoing draft implementation remains required.

[Draft RFC-0023](../docs/rfcs/0023-media-type-input-schema.md) retains the CI
mixed-case media type reproducer. The existing reader normalizes such names,
so its input/default pattern now admits ASCII upper/lowercase while its output
pattern remains lowercase. The affected nested input pins change explicitly;
all output pins, canonical wires and unrelated input pins stay exact. Tests
keep both real schema oracles and verify the reproducer, mixed names, suffixes,
parameters and name-length boundaries. Final-source qualification is required.

This finite qualification establishes the reproducible C4 entry points and
retained corpus. It does not establish exhaustive branch/variant coverage,
historical migration, production capacity/SLOs, independent security review,
overall RFC-0001 acceptance or a new release. Product support remains limited
to its accepted and qualified contracts.
