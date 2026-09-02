// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";

const required = new Map([
  ["server-stateless", { success: 25, skipped: 5, info: 0 }],
  ["completion-complete", { success: 2, skipped: 0, info: 0 }],
  ["tools-list", { success: 3, skipped: 0, info: 0 }],
  ["tools-call-simple-text", { success: 2, skipped: 0, info: 0 }],
  ["tools-call-image", { success: 2, skipped: 0, info: 0 }],
  ["tools-call-audio", { success: 2, skipped: 0, info: 0 }],
  ["tools-call-embedded-resource", { success: 2, skipped: 0, info: 0 }],
  ["tools-call-mixed-content", { success: 2, skipped: 0, info: 0 }],
  ["tools-call-error", { success: 2, skipped: 0, info: 0 }],
  ["tools-call-with-progress", { success: 2, skipped: 0, info: 0 }],
  ["server-sse-multiple-streams", { success: 1, skipped: 0, info: 1 }],
  ["resources-list", { success: 2, skipped: 0, info: 0 }],
  ["resources-read-text", { success: 2, skipped: 0, info: 0 }],
  ["resources-read-binary", { success: 2, skipped: 0, info: 0 }],
  ["resources-templates-read", { success: 2, skipped: 0, info: 0 }],
  ["sep-2164-resource-not-found", { success: 4, skipped: 0, info: 0 }],
  ["prompts-list", { success: 2, skipped: 0, info: 0 }],
  ["prompts-get-simple", { success: 2, skipped: 0, info: 0 }],
  ["prompts-get-with-args", { success: 2, skipped: 0, info: 0 }],
  ["prompts-get-embedded-resource", { success: 2, skipped: 0, info: 0 }],
  ["prompts-get-with-image", { success: 2, skipped: 0, info: 0 }],
  ["dns-rebinding-protection", { success: 2, skipped: 0, info: 0 }],
  ["caching", { success: 8, skipped: 0, info: 0 }],
  ["input-required-result-basic-elicitation", { success: 3, skipped: 0, info: 0 }],
  ["input-required-result-basic-sampling", { success: 3, skipped: 0, info: 0 }],
  ["input-required-result-basic-list-roots", { success: 3, skipped: 0, info: 0 }],
  ["input-required-result-request-state", { success: 3, skipped: 0, info: 0 }],
  ["input-required-result-multiple-input-requests", { success: 3, skipped: 0, info: 0 }],
  ["input-required-result-multi-round", { success: 4, skipped: 0, info: 0 }],
  ["input-required-result-missing-input-response", { success: 2, skipped: 0, info: 0 }],
  ["input-required-result-non-tool-request", { success: 3, skipped: 0, info: 0 }],
  ["input-required-result-result-type", { success: 2, skipped: 0, info: 0 }],
  ["input-required-result-unsupported-methods", { success: 2, skipped: 0, info: 0 }],
  ["input-required-result-tampered-state", { success: 2, skipped: 0, info: 0 }],
  ["input-required-result-capability-check", { success: 2, skipped: 0, info: 0 }],
  ["input-required-result-ignore-extra-params", { success: 2, skipped: 0, info: 0 }],
  ["input-required-result-validate-input", { success: 3, skipped: 0, info: 0 }],
]);

// These scenarios are explicitly not scored by the frozen official revision.
// StateKnot nevertheless treats the three completed pending checks as separate
// regression gates without adding them to the conformance claim.
const extraGates = new Map([
  ["json-schema-2020-12", { success: 8, skipped: 0, info: 0 }],
  ["http-header-validation", { success: 14, skipped: 0, info: 0 }],
  ["http-custom-header-server-validation", { success: 10, skipped: 0, info: 0 }],
]);

const reportedExtensions = new Set([
  "tasks-lifecycle",
  "tasks-capability-negotiation",
  "tasks-wire-fields",
  "tasks-request-state-removal",
  "tasks-mrtr-input",
  "tasks-request-headers",
  "tasks-dispatch-and-envelope",
  "tasks-status-notifications",
  "tasks-required-task-error",
  "tasks-mrtr-composition",
]);

const root = process.argv[2];
if (!root) throw new Error("usage: node verify-results.mjs <run-directory>");

async function findChecks(directory) {
  const output = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const candidate = path.join(directory, entry.name);
    if (entry.isDirectory()) output.push(...(await findChecks(candidate)));
    else if (entry.isFile() && entry.name === "checks.json") output.push(candidate);
  }
  return output;
}

function scenarioName(rootDirectory, file) {
  const relativeParent = path.dirname(path.relative(rootDirectory, file));
  return relativeParent
    .replace(/^server-/u, "")
    .replace(/-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z$/u, "");
}

function countChecks(checks) {
  const counts = { success: 0, skipped: 0, info: 0, failure: 0, warning: 0, other: 0 };
  for (const check of checks) {
    if (check.status === "SUCCESS") counts.success += 1;
    else if (check.status === "SKIPPED") counts.skipped += 1;
    else if (check.status === "INFO") counts.info += 1;
    else if (check.status === "FAILURE") counts.failure += 1;
    else if (check.status === "WARNING") counts.warning += 1;
    else counts.other += 1;
  }
  return counts;
}

const observed = new Map();
for (const file of await findChecks(root)) {
  const scenario = scenarioName(root, file);
  if (!required.has(scenario) && !extraGates.has(scenario) && !reportedExtensions.has(scenario)) {
    throw new Error(`unexpected scenario evidence: ${scenario}`);
  }
  if (observed.has(scenario)) throw new Error(`duplicate scenario evidence: ${scenario}`);
  const checks = JSON.parse(await readFile(file, "utf8"));
  if (!Array.isArray(checks)) throw new Error(`checks are not an array: ${scenario}`);
  observed.set(scenario, countChecks(checks));
}

function assertExact(scenario, expected, counts) {
  if (
    counts.success !== expected.success ||
    counts.skipped !== expected.skipped ||
    counts.info !== expected.info ||
    counts.failure !== 0 ||
    counts.warning !== 0 ||
    counts.other !== 0
  ) {
    throw new Error(
      `${scenario} result drift: expected ${JSON.stringify(expected)}, observed ${JSON.stringify(counts)}`,
    );
  }
}

function blankTotals() {
  return { scenarios: 0, success: 0, skipped: 0, info: 0, failure: 0, warning: 0, other: 0 };
}

function addTotals(totals, counts) {
  totals.scenarios += 1;
  for (const status of ["success", "skipped", "info", "failure", "warning", "other"])
    totals[status] += counts[status];
}

const summary = {
  protocolVersion: "2026-07-28",
  required: { scenarios: {}, totals: blankTotals() },
  extraNotScoredGates: { scenarios: {}, totals: blankTotals() },
  reportedTaskExtension: { scenarios: {}, totals: blankTotals() },
};

for (const [scenario, expected] of required) {
  const counts = observed.get(scenario);
  if (!counts) throw new Error(`missing required scenario evidence: ${scenario}`);
  assertExact(scenario, expected, counts);
  summary.required.scenarios[scenario] = counts;
  addTotals(summary.required.totals, counts);
}

for (const [scenario, expected] of extraGates) {
  const counts = observed.get(scenario);
  if (!counts) throw new Error(`missing extra gate evidence: ${scenario}`);
  assertExact(scenario, expected, counts);
  summary.extraNotScoredGates.scenarios[scenario] = counts;
  addTotals(summary.extraNotScoredGates.totals, counts);
}

for (const scenario of reportedExtensions) {
  const counts = observed.get(scenario);
  if (!counts) throw new Error(`missing reported extension evidence: ${scenario}`);
  if (counts.warning !== 0 || counts.other !== 0)
    throw new Error(`${scenario} emitted unsupported status: ${JSON.stringify(counts)}`);
  summary.reportedTaskExtension.scenarios[scenario] = counts;
  addTotals(summary.reportedTaskExtension.totals, counts);
}

console.log(JSON.stringify(summary, null, 2));
