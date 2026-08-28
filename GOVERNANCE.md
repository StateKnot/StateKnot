<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot governance

StateKnot is currently a maintainer-led open-source project. Governance is kept
small while the project establishes real users and contributors, and will be
expanded through public changes to this document rather than through informal
private rules.

## Roles

- **Contributors** submit issues, design feedback, documentation, tests, or code.
- **Committers** are trusted contributors who may triage issues and merge scoped
  changes within areas where they have demonstrated sustained ownership.
- **Maintainers** set release and compatibility policy, approve RFCs, handle
  security and conduct reports, and manage repository access.

The initial maintainer is [@jiawenyao401](https://github.com/jiawenyao401).
Maintainer and committer additions or removals are recorded through public pull
requests to this file, except when private safety concerns require limited
disclosure.

## Decisions

Routine changes are decided in pull-request review. Public APIs, runtime and
persistence semantics, protocol mappings, security boundaries, compatibility
promises, and new workspace crates require an RFC.

Maintainers seek consensus and document material objections. If consensus is
not possible, a maintainer records the decision, alternatives, and rationale in
the RFC. A maintainer with a direct conflict of interest must disclose it and
recuse themselves where practical.

## Releases

No production release will be made until its documented release gates pass.
Every release requires green mandatory CI, an updated changelog, reviewed
dependency and license reports, reproducible source contents, and an annotated
signed tag. Compatibility promises apply only when explicitly documented for a
released version.

## Changes to governance

Governance changes use the normal pull-request process and require maintainer
approval. When the project has at least three active maintainers from more than
one organization, this document should be revised to define quorum, voting, and
succession rules.
