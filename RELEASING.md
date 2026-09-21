<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot release operations

This runbook is for maintainers. The public compatibility contract is
[`docs/versioning-and-releases.md`](docs/versioning-and-releases.md).

## Release set and order

All product crates use one exact workspace version. Publish only in this order:

1. `stateknot-core`
2. `stateknot-store-postgres`
3. `stateknot-runtime`
4. `stateknot-integrations`
5. `stateknot-artifact-store`
6. `stateknot-testkit`
7. `stateknot`

Never use `--no-verify` with `cargo publish`.
Before the same-version dependency set exists on crates.io, Cargo cannot build
all downstream normalized archives. The preflight therefore validates every
Cargo file list and source-size ceiling, builds the complete workspace and
Rustdoc, then packages and rebuilds the dependency-free Core and Testkit
bootstrap archives.
The publisher packages and verifies each downstream crate immediately after its
same-version dependencies become downloadable.

## Prepare and qualify

1. Choose a SemVer version and update the workspace version, every exact
   internal dependency, the CI package gate, both versioning guides, and the
   changelog.
2. Confirm every package name and version is absent from crates.io unless this
   is recovery of the same tagged release.
3. Run the complete repository CI and PostgreSQL 16/17 matrix.
4. From a clean checkout, run:

   ```bash
   ./scripts/verify-release.sh 0.1.0-alpha.1
   ```

5. Merge the release PR and create an annotated `v<version>` tag on that exact
   `main` commit. Do not move or reuse a release tag. For the first release of
   new crate names, do not publish the GitHub Release until bootstrap and
   Trusted Publisher configuration are complete.

## First-release bootstrap

crates.io cannot configure Trusted Publishing before a crate's first version
exists. Create one short-expiry crates.io token immediately before bootstrap;
do not store it in GitHub, a shell history, the repository, or a command line.
Expose it only through Cargo's credential provider/environment for the release
process, then run from the clean tagged commit:

```bash
git fetch origin main --tags
read -rsp "crates.io bootstrap token: " CARGO_REGISTRY_TOKEN
export CARGO_REGISTRY_TOKEN
./scripts/publish-release.sh 0.1.0-alpha.1
unset CARGO_REGISTRY_TOKEN
```

The script is resumable. For an already-published package it downloads the
registry archive and proceeds only if its SHA-256 digest exactly matches the
local tagged archive. It never overwrites a version.

Immediately after all seven packages exist:

1. add a Trusted Publisher to each crate with GitHub owner `StateKnot`,
   repository `StateKnot`, workflow `release.yml`, and environment `crates-io`;
2. require maintainer approval on the GitHub `crates-io` environment;
3. disable other publish methods for each crate after an OIDC release has been
   proven;
4. revoke the bootstrap token and remove any local credential copy.

Then publish the GitHub Release from the existing immutable tag. Its first OIDC
run is intentionally idempotent: it rebuilds each package in dependency order
and accepts every bootstrap upload only after an exact archive digest match.

## Normal OIDC release

Publishing a GitHub Release triggers `.github/workflows/release.yml`. The
workflow verifies the archives without credentials, waits at the protected
`crates-io` environment, exchanges GitHub OIDC for a short-lived token using the
SHA-pinned official action, publishes in dependency order, and verifies the
downloaded bytes. Do not add a long-lived `CARGO_REGISTRY_TOKEN` repository or
environment secret.

## Post-release verification

Before closing the release issue:

- all seven exact versions are downloadable from crates.io;
- the downloaded `.crate` archives match the tagged local archives;
- docs.rs reports successful builds for all seven packages;
- a new empty consumer can resolve the exact `stateknot` version on Rust 1.88;
- the GitHub Release and immutable tag point at the published commit;
- the website and both versioning guides show the released version.

If publication stops midway, do not bump or recreate the tag. Correct the
external outage or authorization failure and rerun the same release. If any
existing archive differs, stop: that is an integrity incident, not a retryable
partial release.
