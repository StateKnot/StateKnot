// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";

const expected = new Map([
  ["tools_call", { success: 2, skipped: 0 }],
  ["request-metadata", { success: 5, skipped: 3 }],
  ["http-standard-headers", { success: 3, skipped: 8 }],
  ["http-custom-headers", { success: 18, skipped: 0 }],
  ["http-invalid-tool-headers", { success: 11, skipped: 0 }],
  ["json-schema-ref-no-deref", { success: 1, skipped: 0 }],
  ["sep-2322-client-request-state", { success: 5, skipped: 0 }],
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
  const scenario = path.relative(root, file).split(path.sep)[0];
  if (!expected.has(scenario)) {
    throw new Error(`unexpected scenario evidence: ${scenario}`);
  }
  if (observed.has(scenario)) {
    throw new Error(`duplicate scenario evidence: ${scenario}`);
  }
  const checks = JSON.parse(await readFile(file, "utf8"));
  if (!Array.isArray(checks)) {
    throw new Error(`checks are not an array: ${scenario}`);
  }
  const counts = { success: 0, skipped: 0, failure: 0, other: 0 };
  for (const check of checks) {
    if (check.status === "SUCCESS") counts.success += 1;
    else if (check.status === "SKIPPED") counts.skipped += 1;
    else if (check.status === "FAILURE") counts.failure += 1;
    else counts.other += 1;
  }
  observed.set(scenario, counts);
}

const summary = { protocolVersion: "2026-07-28", scenarios: {}, totals: {} };
let totalSuccess = 0;
let totalSkipped = 0;
for (const [scenario, expectedCounts] of expected) {
  const counts = observed.get(scenario);
  if (!counts) throw new Error(`missing scenario evidence: ${scenario}`);
  if (
    counts.success !== expectedCounts.success ||
    counts.skipped !== expectedCounts.skipped ||
    counts.failure !== 0 ||
    counts.other !== 0
  ) {
    throw new Error(
      `${scenario} result drift: expected ${JSON.stringify(expectedCounts)}, observed ${JSON.stringify(counts)}`,
    );
  }
  summary.scenarios[scenario] = counts;
  totalSuccess += counts.success;
  totalSkipped += counts.skipped;
}
summary.totals = {
  scenarios: expected.size,
  success: totalSuccess,
  skipped: totalSkipped,
  failure: 0,
  other: 0,
};
console.log(JSON.stringify(summary, null, 2));
