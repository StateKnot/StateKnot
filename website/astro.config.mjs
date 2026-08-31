// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from "astro/config";

export default defineConfig({
  site: process.env.SITE_URL ?? "https://stknot.com",
  output: "static",
  compressHTML: true,
  build: {
    format: "directory",
    inlineStylesheets: "never",
  },
  vite: {
    build: {
      assetsInlineLimit: 0,
    },
  },
  server: {
    host: "127.0.0.1",
    port: 4321,
  },
});
