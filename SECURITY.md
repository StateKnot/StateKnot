<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Security policy

## Supported versions

StateKnot is pre-alpha and has no supported production release yet. The `main`
branch receives security fixes, but it must not be treated as a production
support commitment.

| Version | Supported |
|---|---|
| `main` / `0.0.0` | Best effort during pre-alpha |
| Released versions | None yet |

A supported-version and backport policy will be published before the first
release intended for production evaluation.

## Reporting a vulnerability

Use GitHub's
[private vulnerability reporting](https://github.com/StateKnot/StateKnot/security/advisories/new).
Do not disclose a suspected vulnerability in a public issue, discussion, or
pull request.

Include, when possible:

- the affected commit, component, configuration, and deployment assumptions;
- impact and a realistic attack scenario;
- reproduction steps or a minimal proof of concept;
- whether secrets or personal data may have been exposed; and
- any proposed mitigation or disclosure deadline.

The project aims to acknowledge a complete report within three business days
and provide an initial triage within seven business days. Fix and disclosure
timelines depend on severity, affected releases, and coordination needs. These
targets are goals until a staffed security response team is established.

## Scope

Reports may cover StateKnot-owned source code, official release artifacts,
documented deployment defaults, and official CI/release workflows. Vulnerabilities
in third-party services or dependencies should also be reported upstream; tell
us privately when StateKnot users are exposed.

There is currently no paid bug bounty. Good-faith research that avoids privacy
violations, data destruction, service disruption, and public disclosure before
coordination is complete is welcome.
