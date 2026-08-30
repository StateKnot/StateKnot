<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot website

The public website is an Astro static build. It deliberately describes the
repository as pre-alpha and separates implemented slices from in-progress and
planned work.

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

`SITE_URL` is optional during local development. Set it to the final public
origin during a production build so Astro emits canonical URLs.

## Deployment

The production origin is `https://stknot.com`; `www.stknot.com` and direct HTTP
requests permanently redirect to it. Both DNS names must resolve to the server
before the one-time TLS bootstrap:

```console
STATEKNOT_SSH_IDENTITY=/path/to/stateknot_deploy \
  ./scripts/bootstrap-tls.sh ubuntu@49.232.33.76
```

The bootstrap uses the ACME webroot challenge, installs the certificate renewal
timer and a deploy hook that validates and reloads Nginx after renewal. It is
safe to rerun because Certbot keeps a certificate that is not due for renewal.

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

The script refuses to overwrite an existing release, validates Nginx before the
symlink switch, reloads Nginx only after a valid configuration, rolls back local
activation failures, and checks a release-backed health endpoint locally and
externally over TLS. It requires passwordless `sudo` on the target host. The
HTTPS virtual host enables TLS 1.2/1.3, one year of HSTS without preload, and
the same restrictive application security headers used by the site.
