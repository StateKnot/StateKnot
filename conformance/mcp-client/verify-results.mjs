// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";

const required = new Map([
  ["tools_call", { success: 2, skipped: 0, info: 0 }],
  ["request-metadata", { success: 5, skipped: 3, info: 0 }],
  ["http-standard-headers", { success: 3, skipped: 8, info: 0 }],
  ["http-custom-headers", { success: 18, skipped: 0, info: 0 }],
  ["http-invalid-tool-headers", { success: 11, skipped: 0, info: 0 }],
  ["json-schema-ref-no-deref", { success: 1, skipped: 0, info: 0 }],
  ["sep-2322-client-request-state", { success: 5, skipped: 0, info: 0 }],
  ["auth/metadata-default", { success: 13, skipped: 0, info: 18 }],
  ["auth/metadata-var1", { success: 14, skipped: 0, info: 24 }],
  ["auth/metadata-var2", { success: 14, skipped: 0, info: 26 }],
  ["auth/metadata-var3", { success: 13, skipped: 0, info: 22 }],
  ["auth/basic-cimd", { success: 12, skipped: 0, info: 16 }],
  ["auth/scope-from-www-authenticate", { success: 14, skipped: 0, info: 18 }],
  ["auth/scope-from-scopes-supported", { success: 14, skipped: 0, info: 18 }],
  ["auth/scope-omitted-when-undefined", { success: 14, skipped: 0, info: 18 }],
  ["auth/scope-step-up", { success: 25, skipped: 0, info: 28 }],
  ["auth/scope-retry-limit", { success: 11, skipped: 0, info: 15 }],
  ["auth/token-endpoint-auth-basic", { success: 18, skipped: 0, info: 18 }],
  ["auth/token-endpoint-auth-post", { success: 18, skipped: 0, info: 18 }],
  ["auth/token-endpoint-auth-none", { success: 18, skipped: 0, info: 18 }],
  ["auth/pre-registration", { success: 12, skipped: 0, info: 16 }],
  ["auth/resource-mismatch", { success: 2, skipped: 0, info: 4 }],
  ["auth/offline-access-scope", { success: 12, skipped: 0, info: 17 }],
  ["auth/offline-access-not-supported", { success: 14, skipped: 0, info: 18 }],
  [
    "auth/authorization-server-migration",
    { success: 27, skipped: 0, info: 30 },
  ],
  ["auth/iss-supported", { success: 14, skipped: 0, info: 18 }],
  ["auth/iss-not-advertised", { success: 14, skipped: 0, info: 18 }],
  ["auth/iss-supported-missing", { success: 8, skipped: 0, info: 10 }],
  ["auth/iss-wrong-issuer", { success: 8, skipped: 0, info: 10 }],
  ["auth/iss-unexpected", { success: 8, skipped: 0, info: 10 }],
  ["auth/iss-normalized", { success: 8, skipped: 0, info: 10 }],
  ["auth/metadata-issuer-mismatch", { success: 3, skipped: 0, info: 6 }],
]);

const notScored = new Set([
  "auth/client-credentials-jwt",
  "auth/client-credentials-basic",
  "auth/enterprise-managed-authorization",
  "auth/dpop",
  "auth/dpop-nonce",
  "auth/wif-jwt-bearer",
  "json-schema-2020-12-preservation",
]);

const root = process.argv[2];
if (!root) {
  throw new Error("usage: node verify-results.mjs <run-directory>");
}

async function findChecks(directory) {
  const output = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const candidate = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      output.push(...(await findChecks(candidate)));
    } else if (entry.isFile() && entry.name === "checks.json") {
      output.push(candidate);
    }
  }
  return output;
}

const observed = new Map();
for (const file of await findChecks(root)) {
  const relativeParent = path.dirname(path.relative(root, file));
  const scenario = relativeParent.replace(
    /-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z$/u,
    "",
  );
  if (!required.has(scenario) && !notScored.has(scenario)) {
    throw new Error(`unexpected scenario evidence: ${scenario}`);
  }
  if (observed.has(scenario)) {
    throw new Error(`duplicate scenario evidence: ${scenario}`);
  }
  const checks = JSON.parse(await readFile(file, "utf8"));
  if (!Array.isArray(checks)) {
    throw new Error(`checks are not an array: ${scenario}`);
  }
  const counts = {
    success: 0,
    skipped: 0,
    info: 0,
    failure: 0,
    warning: 0,
    other: 0,
  };
  for (const check of checks) {
    if (check.status === "SUCCESS") counts.success += 1;
    else if (check.status === "SKIPPED") counts.skipped += 1;
    else if (check.status === "INFO") counts.info += 1;
    else if (check.status === "FAILURE") counts.failure += 1;
    else if (check.status === "WARNING") counts.warning += 1;
    else counts.other += 1;
  }
  observed.set(scenario, counts);
}

const summary = {
  protocolVersion: "2026-07-28",
  required: { scenarios: {}, totals: {} },
  notScored: { scenarios: {}, totals: {} },
};
let totalSuccess = 0;
let totalSkipped = 0;
let totalInfo = 0;
for (const [scenario, expectedCounts] of required) {
  const counts = observed.get(scenario);
  if (!counts) throw new Error(`missing scenario evidence: ${scenario}`);
  if (
    counts.success !== expectedCounts.success ||
    counts.skipped !== expectedCounts.skipped ||
    counts.info !== expectedCounts.info ||
    counts.failure !== 0 ||
    counts.warning !== 0 ||
    counts.other !== 0
  ) {
    throw new Error(
      `${scenario} result drift: expected ${JSON.stringify(expectedCounts)}, observed ${JSON.stringify(counts)}`,
    );
  }
  summary.required.scenarios[scenario] = counts;
  totalSuccess += counts.success;
  totalSkipped += counts.skipped;
  totalInfo += counts.info;
}
summary.required.totals = {
  scenarios: required.size,
  success: totalSuccess,
  skipped: totalSkipped,
  info: totalInfo,
  failure: 0,
  warning: 0,
  other: 0,
};

let notScoredSuccess = 0;
let notScoredSkipped = 0;
let notScoredInfo = 0;
let notScoredFailure = 0;
for (const scenario of notScored) {
  const counts = observed.get(scenario);
  if (!counts)
    throw new Error(`missing not-scored scenario evidence: ${scenario}`);
  if (counts.warning !== 0 || counts.other !== 0) {
    throw new Error(
      `${scenario} emitted an unsupported status: ${JSON.stringify(counts)}`,
    );
  }
  summary.notScored.scenarios[scenario] = counts;
  notScoredSuccess += counts.success;
  notScoredSkipped += counts.skipped;
  notScoredInfo += counts.info;
  notScoredFailure += counts.failure;
}
summary.notScored.totals = {
  scenarios: notScored.size,
  success: notScoredSuccess,
  skipped: notScoredSkipped,
  info: notScoredInfo,
  failure: notScoredFailure,
  warning: 0,
  other: 0,
};
console.log(JSON.stringify(summary, null, 2));
