<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot website

The public website is a bilingual Astro static build. English is served from
`/` and `/docs/`; Simplified Chinese is served from `/zh/` and `/zh/docs/`.
Every published documentation route has an equivalent route in both languages,
with an explicit language switch instead of an automatic locale redirect. The
site deliberately describes the repository as pre-alpha and separates
implemented slices from in-progress and planned work.

The immutable Graph Driver audit schema is published at
`/schemas/runtime/graph-driver-event/1.0.0`. Its public copy must remain
byte-for-byte identical to the schema embedded in `stateknot-runtime`; Caddy
serves versioned schema paths as `application/schema+json` with immutable cache
headers.

## Local development

Use the Node version in `.nvmrc` and install from the committed lockfile:

```console
npm ci
npm run dev
```

Run the complete local gate before a change is merged:

```console
npx playwright install chromium
npm run verify
```

Canonical, Open Graph, and language-alternate URLs default to
`https://stknot.com`. `SITE_URL` may override that origin for a preview or an
alternate deployment.

The browser suite checks English/Chinese route parity, canonical and `hreflang`
metadata, internal-link resolution, localized command search and copy feedback,
WCAG automated checks, contrast, responsive behavior at 320/375/414/768 pixels,
and both language-specific error templates.

## Deployment

The production origin is `https://stknot.com`; `www.stknot.com` and direct HTTP
requests permanently redirect to it. Both DNS names must resolve to the server,
and inbound TCP ports 80 and 443 must be allowed, before the one-time TLS
bootstrap:

```console
STATEKNOT_SSH_IDENTITY=/path/to/stateknot_deploy \
  ./scripts/bootstrap-tls.sh ubuntu@49.232.33.76
```

The bootstrap installs the Ubuntu security-maintained Caddy package, validates
the committed Caddyfile, and switches from the bootstrap Nginx service with a
failure rollback. Certificate issuance is restricted to ACME TLS-ALPN-01 because
the server's mainland HTTP path may be intercepted before ICP filing. Caddy
stores certificates under its protected application data directory and renews
them automatically without stopping the web service. The script is idempotent
and verifies trusted certificates for both hostnames before committing the
service migration.

The deployment layout uses immutable releases and an atomic `current` symlink:

```text
/var/www/stateknot/
├── current -> releases/<git-sha>
└── releases/
    └── <git-sha>/
```

After `npm run verify`, deploy from this directory:

```console
SITE_URL=https://stknot.com npm run build
STATEKNOT_SSH_IDENTITY=/path/to/stateknot_deploy \
  ./scripts/deploy.sh ubuntu@49.232.33.76
```

The script refuses to overwrite an existing release, validates Caddy before the
symlink switch, reloads Caddy only after a valid configuration, rolls back local
activation failures, and checks a release-backed health endpoint locally and
externally over TLS. It requires passwordless `sudo` on the target host. Caddy's
secure defaults enforce TLS 1.2/1.3; the site adds one year of HSTS without
preload and the same restrictive application security headers used locally.
