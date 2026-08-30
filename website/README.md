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

The deployment layout uses immutable releases and an atomic `current` symlink:

```text
/var/www/stateknot/
├── current -> releases/<git-sha>
└── releases/
    └── <git-sha>/
```

After `npm run verify`, deploy from this directory:

```console
SITE_URL=http://49.232.33.76 npm run build
STATEKNOT_SSH_IDENTITY="$HOME/.ssh/stateknot_deploy" \
  ./scripts/deploy.sh ubuntu@49.232.33.76
```

The script refuses to overwrite an existing release, validates Nginx before the
symlink switch, reloads Nginx only after a valid configuration, rolls back local
activation failures, and checks a release-backed health endpoint locally and
externally. It requires passwordless `sudo` on the target host. HTTPS should be
enabled after the final domain is pointed at the server; do not enable HSTS
before that cutover is verified.
