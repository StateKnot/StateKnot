#!/usr/bin/env python3
"""Pure offline future trace classifier. No build/container/network operations."""
import argparse
from collections import Counter
from datetime import datetime
from full_acceptance import validate_trace
import hashlib
import json
from pathlib import Path
import re
import sys
from mcp_text_marker import (
    observe as observe_mcp_text,
    strict_step_lines,
    validate as validate_mcp_text,
    validate_step as validate_mcp_step,
)


def duration(value):
    return sum(float(n) * {"h": 3600, "m": 60, "s": 1}[unit]
               for n, unit in re.findall(r"([0-9.]+)([hms])", value))



def effective_codegen_options(argv):
    """Honor rustc argv order across both -C value and -Cvalue spellings."""
    effective = {}
    index = 0
    while index < len(argv):
        item = argv[index]
        if item == "-C":
            index += 1
            if index == len(argv):
                raise ValueError("truncated actual -C option")
            option = argv[index]
        elif item.startswith("-C") and len(item) > 2:
            option = item[2:]
        else:
            index += 1
            continue
        key, separator, value = option.partition("=")
        effective[key] = value if separator else None
        index += 1
    return effective


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--log", type=Path, required=True)
    p.add_argument("--job-json", type=Path, required=True)
    p.add_argument("--run-json", type=Path, required=True)
    p.add_argument("--all-jobs-json", type=Path, required=True)
    p.add_argument("--measurement-anchor", type=Path, required=True)
    p.add_argument("--measurement-anchor-sha256", required=True, help="Owner recorded hash from the external pre-dispatch receipt; not read from the measured checkout")
    p.add_argument("--assets", type=Path, required=True)
    p.add_argument("--evidence", type=Path, required=True)
    p.add_argument("--require-qualified", action="store_true")
    args = p.parse_args()
    load = lambda f: json.loads(f.read_text())
    contract = load(args.assets / "frozen-contract.json")
    expected = load(args.assets / "measured-run-bodies.json")
    workflow = load(args.assets / "workflow-contract.json")
    job = load(args.job_json)
    run = load(args.run_json)
    all_jobs = load(args.all_jobs_json)["jobs"]
    anchor = load(args.measurement_anchor)
    marker_definitions = load(args.assets / "marker-contracts.json")["markers"]
    active = None
    blocks = {}
    errors = []
    if not re.fullmatch("[0-9a-f]{64}", args.measurement_anchor_sha256) or hashlib.sha256(args.measurement_anchor.read_bytes()).hexdigest() != args.measurement_anchor_sha256:
        errors.append("external pre-dispatch anchor hash differs from owner supplied receipt")
    for number, raw in enumerate(args.log.read_text().splitlines(), 1):
        line = re.sub(r"^\d{4}-\d\d-\d\dT[^ ]+Z ", "", raw)
        if line.startswith("STATEKNOT_COLD_STEP_BEGIN="):
            begin = json.loads(line.split("=", 1)[1])
            active = begin["step"]
            if active in blocks:
                errors.append("duplicate/replayed step: " + active)
            blocks[active] = {"line": number, "build_wall_seconds": [], "finished": [],
                              "harness_elapsed_seconds": [], "summaries": [], "markers": Counter(), "terminal": None}
        elif line.startswith("STATEKNOT_COLD_STEP_END="):
            end = json.loads(line.split("=", 1)[1])
            if active != end["step"] or active not in blocks:
                errors.append("unmatched terminal step")
            else:
                blocks[active]["terminal"] = end
                record = args.evidence / (active + ".json")
                if not record.exists() or load(record) != end:
                    errors.append("raw/receipt terminal mismatch: " + active)
            active = None
        elif active:
            block = blocks[active]
            m = re.search(r"Finished `([^`]+)` profile \[([^]]+)\] target\(s\) in (.+)$", line)
            if m:
                block["finished"].append({"profile": m[1], "label": m[2], "reported": m[3], "line": number})
                block["build_wall_seconds"].append(duration(m[3]))
            m = re.search(r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in ([0-9.]+)s", line)
            if m:
                block["summaries"].append(dict(zip(("passed", "failed", "ignored", "measured", "filtered"), map(int, m.groups()[1:6]))))
                block["harness_elapsed_seconds"].append(float(m[7]))
                if m[1] != "ok":
                    errors.append("explicit failed libtest summary: " + active)
            marker = re.match(r"^(STATEKNOT_[A-Z0-9_]+(?:EVIDENCE|REPORT))=(\{.*\})$", line)
            if marker:
                try:
                    json.loads(marker[2])
                except json.JSONDecodeError:
                    errors.append("incomplete marker JSON: " + active)
                else:
                    block["markers"][marker[1]] += 1
    if "new-mcp" in blocks:
        for line in strict_step_lines(args.log.read_bytes(), "new-mcp"):
            observe_mcp_text(blocks["new-mcp"], line)
    if contract.get("frozen") is not True:
        errors.append("contract pending freeze")
    expected_labels = {b["label"] for b in expected}
    if set(blocks) != expected_labels:
        errors.append("missing or unexpected measured run body")
    for label in expected_labels:
        block = blocks.get(label)
        if not block:
            continue
        terminal = block["terminal"]
        if not terminal or not terminal["complete"] or terminal["exit_code"] != 0:
            errors.append("incomplete/nonzero run: " + label)
        acceptance = (contract.get("acceptance_by_run_label") or {}).get(label)
        if label == "new-mcp":
            errors.extend(validate_mcp_text(block, acceptance or {}, contract.get("mcp_expected_tests")))
        if acceptance is None or block["summaries"] != acceptance["summaries"]:
            errors.append("libtest inventory mismatch: " + label)
        if acceptance and any(block["markers"][name] != count for name, count in acceptance["markers"].items()
                              if marker_definitions[name]["kind"] != "exact_text"):
            errors.append("mandatory unique marker mismatch: " + label)
    mcp_steps = [s for s in workflow["jobs"]["postgres"]["steps"]
                 if s.get("run") and s["run"] == contract.get("mcp_run")]
    if len(mcp_steps) != 1:
        errors.append("MCP run body missing or ambiguous in frozen workflow")
    else:
        errors.extend(validate_mcp_step(mcp_steps[0]))
    for name in [s["name"] for s in workflow["jobs"]["postgres"]["steps"]]:
        actual = [s for s in job["steps"] if s["name"] == name]
        if len(actual) != 1 or actual[0]["conclusion"] != "success":
            errors.append("action/run not successful: " + name)
    if job["status"] != "completed" or job["conclusion"] != "success":
        errors.append("GitHub job not completed/success; API step wall is not CPU")
    if workflow["jobs"]["postgres"]["timeout-minutes"] != 30:
        errors.append("job budget changed")
    before = load(args.evidence / "preflight.json")
    after = load(args.evidence / "audit.json")
    for key, name in (("wrapper_sha256", "rustc_wrapper.py"), ("wrapper_entry_sha256", "rustc_wrapper.sh"),
                      ("locale_guard_sha256", "locale_guard.sh"), ("runtime_controls_sha256", "runtime_controls.py"),
                      ("probe_sha256", "cold_probe.py"),
                      ("contract_sha256", "frozen-contract.json"), ("run_bodies_sha256", "measured-run-bodies.json")):
        if before[key] != hashlib.sha256((args.assets / name).read_bytes()).hexdigest():
            errors.append("executed instrumentation/contract mismatch: " + name)
    locale_artifacts = args.evidence / "locale-controls"
    controls_raw = (locale_artifacts / "runtime-controls.json").read_bytes()
    controls = json.loads(controls_raw)
    observed = before.get("runtime_controls", {})
    modules = {name: hashlib.sha256((args.assets / name).read_bytes()).hexdigest() for name in
               ("locale_guard.sh", "runtime_controls.py", "rustc_wrapper.sh", "rustc_wrapper.py", "cold_probe.py")}
    required_controls = {"environment_preserved", "literal_argv_preserved", "cwd_preserved",
                         "jobserver_fds_and_token_preserved", "exit37_preserved", "C_locale_rejected",
                         "invalid_locale_rejected", "entry_locale_drift_rejected"}
    variables = controls.get("baseline", {}).get("variables", {})
    fields = []
    for key in ("LANG", "LC_ALL", "LC_CTYPE"):
        value = variables.get(key, {})
        if type(value.get("present")) is not bool or not isinstance(value.get("value"), str) or (value.get("present") is False and value.get("value")):
            errors.append("original locale allowlist structure differs")
            continue
        fields.extend([key.encode(), b"1" if value["present"] else b"0", value["value"].encode("utf-8", "surrogateescape")])
    fields.append(b"")
    baseline = (locale_artifacts / "baseline.nul").read_bytes()
    if (set(variables) != {"LANG", "LC_ALL", "LC_CTYPE"} or b"\0".join(fields) != baseline
            or controls.get("baseline", {}).get("snapshot_sha256") != hashlib.sha256(baseline).hexdigest()
            or controls.get("baseline", {}).get("charmap") != "UTF-8"
            or (locale_artifacts / "charmap.txt").read_bytes() != b"UTF-8\n"):
        errors.append("actual original Bash locale proof differs")
    if (controls.get("schema") != "stateknot.cold-locale-runtime-controls.v1" or controls.get("state") != "passed"
            or controls.get("runtime_controls_executed") is not True
            or controls.get("platform") != {"system": "Linux", "machine": "x86_64"}
            or controls.get("modules") != modules or set(controls.get("controls", {})) != required_controls
            or any(v is not True for v in controls.get("controls", {}).values())
            or controls.get("cargo_or_real_rustc_executed") is not False
            or observed != {"sha256": hashlib.sha256(controls_raw).hexdigest(), "state": "passed",
                 "runtime_controls_executed": True, "baseline_snapshot_sha256": hashlib.sha256(baseline).hexdigest(),
                 "locale_allowlist": variables, "controls": controls.get("controls"), "modules": modules}
            or after.get("runtime_controls_after") != observed):
        errors.append("mandatory actual Linux controls / executed module binding incomplete")
    control_directory = locale_artifacts / Path(controls.get("control_directory", "")).name
    environment = load(control_directory / "environment.json")
    forwarding = load(control_directory / "forwarding.json")
    if environment != {"before": variables, "after": variables}:
        errors.append("actual before/after original environment allowlist control differs")
    if (forwarding.get("argv") != ["space value", "--cfg", 'feature="control"', "--literal=$()`", "中文"]
            or forwarding.get("cwd") != controls.get("control_directory")
            or forwarding.get("token") != "J" or forwarding.get("fd_a") != forwarding.get("fd_b")
            or not re.fullmatch(r"--jobserver-auth=[0-9]+,[0-9]+ -j", forwarding.get("jobserver", ""))):
        errors.append("actual literal argv/cwd/inherited FD/token control artifact differs")
    if not before["target_was_new_and_empty"] or before["artifact_cache_restore"] or before["incremental"]:
        errors.append("target not artifact-cold")
    if before["source"] != after["source_after"] or not after["all_run_bodies_terminal_zero"]:
        errors.append("source or complete execution audit mismatch")
    if contract.get("full_cold_contract_frozen") is not True or not contract.get("source_commit") or not contract.get("source_tree"):
        errors.append("full source/acceptance/owner freeze pending")
    if run.get("event") != "workflow_dispatch" or run.get("path") != contract["measurement_workflow_path"]:
        errors.append("actual run event/workflow path differs")
    if run.get("id") != job.get("run_id") or run.get("head_sha") != job.get("head_sha") or run.get("run_attempt") != job.get("run_attempt"):
        errors.append("actual run/job API identity differs")
    if run.get("status") != "completed" or run.get("conclusion") != "success":
        errors.append("actual whole workflow run not completed/success")
    if hashlib.sha256((args.assets / "workflow-contract.json").read_bytes()).hexdigest() != contract["workflow_object_sha256"]:
        errors.append("offline full-workflow object differs from frozen workflow-object hash")
    if before["source"].get("workflow_path") != contract["measurement_workflow_path"] or before["source"].get("workflow_sha256") != contract["measurement_workflow_sha256"]:
        errors.append("preflight actual checkout measurement workflow differs")
    if anchor.get("external_to_measurement_Git_tree") is not True or anchor.get("created_before_dispatch") is not True:
        errors.append("independent pre-dispatch measurement anchor missing")
    if anchor.get("measurement_commit") != before["source"].get("commit") or anchor.get("measurement_tree") != before["source"].get("tree"):
        errors.append("self-recorded checkout does not match external frozen head/tree")
    if anchor.get("workflow_id") != run.get("workflow_id") or anchor.get("measurement_ref") != run.get("head_branch"):
        errors.append("external workflow-id/ref differs from actual run API")
    if anchor.get("workflow_path") != contract["measurement_workflow_path"] or anchor.get("workflow_sha256") != contract["measurement_workflow_sha256"]:
        errors.append("external workflow path/hash differs")
    if anchor.get("source_origin_commit") != contract["source_commit"] or anchor.get("source_compiled_canonical_sha256") != contract["compiled_canonical_sha256"]:
        errors.append("source origin/external compiled input binding differs")
    if anchor.get("contract_sha256") != hashlib.sha256((args.assets / "frozen-contract.json").read_bytes()).hexdigest():
        errors.append("external frozen contract differs")
    expected_job_names = contract["all_original_expanded_job_names"]
    if sorted(j.get("name") for j in all_jobs) != sorted(expected_job_names) or len(all_jobs) != 13:
        errors.append("all original13 expanded job instances not retained")
    if any(j.get("status") != "completed" or j.get("conclusion") != "success" or j.get("head_sha") != anchor.get("measurement_commit") or j.get("run_id") != run.get("id") for j in all_jobs):
        errors.append("one of all13 original experimental checks not complete/success on anchored head")
    if len([j for j in all_jobs if j.get("id") == job.get("id")]) != 1:
        errors.append("selected PG job absent/ambiguous in complete run jobs API")
    if job.get("name") != "PostgreSQL (" + before["actual_postgres"]["version"].split(".")[0] + ")":
        errors.append("selected API job major differs from actual PG service evidence")
    if any(j.get("run_attempt") != run.get("run_attempt") for j in all_jobs):
        errors.append("mixed workflow attempts in jobs API")
    try:
        anchor_created = datetime.fromisoformat(anchor["created_at"].replace("Z", "+00:00"))
        run_created = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
        if anchor_created.tzinfo is None or run_created.tzinfo is None or anchor_created.replace(microsecond=0) > run_created:
            errors.append("external freeze did not precede actual run creation")
    except (KeyError, ValueError, TypeError):
        errors.append("missing external anchor/run creation timestamps")
    if before["source"].get("commit") != job.get("head_sha"):
        errors.append("actual measured checkout/API head mismatch")
    if before["source"].get("compiled_input_count") != 536 or before["source"].get("manifest_sha256") != contract["compiled_input_manifest_sha256"]:
        errors.append("complete536 input identity differs")
    try:
        started = datetime.fromisoformat(job["started_at"].replace("Z", "+00:00"))
        completed = datetime.fromisoformat(job["completed_at"].replace("Z", "+00:00"))
        job_wall_seconds = (completed - started).total_seconds()
        if job_wall_seconds < 0:
            errors.append("invalid actual job timestamp order")
    except (KeyError, ValueError, TypeError):
        job_wall_seconds = None
        errors.append("missing actual complete job timestamps")
    if b"The job has exceeded the maximum execution time" in args.log.read_bytes():
        errors.append("actual GitHub job timeout annotation in raw log")
    if before["opt_level"] != workflow["jobs"]["postgres"]["env"]["CARGO_PROFILE_TEST_OPT_LEVEL"] and workflow["jobs"]["postgres"]["env"]["CARGO_PROFILE_TEST_OPT_LEVEL"] != "${{ inputs.test_opt_level }}":
        errors.append("profile/workflow setting mismatch")
    expected_env = {**{k:v for k,v in workflow["jobs"]["postgres"]["env"].items() if k.startswith("CARGO_")}, "CARGO_PROFILE_TEST_OPT_LEVEL":before["opt_level"]}
    if before.get("profile_environment") != expected_env:
        errors.append("actual six profile environment differs")
    if before["opt_level"] not in {"1", "2"} or before["opt_level"] != contract["expected_profile_opt"]:
        errors.append("unsupported profile experiment")
    argv_files = list((args.evidence / "rustc-argv").glob("*.json"))
    test_units = [load(p) for p in argv_files if "--test" in load(p).get("argv", [])]
    if not test_units:
        errors.append("no actual rustc test-unit argv evidence")
    for unit in test_units:
        argv = unit["argv"]
        effective = effective_codegen_options(argv)
        if effective.get("opt-level") != before["opt_level"] or effective.get("debug-assertions") != "on" or effective.get("overflow-checks") != "on" or effective.get("debuginfo", "0") != "0" or "incremental" in effective:
            errors.append("actual final test-unit profile not frozen opt/assert/overflow")
            break
    errors.extend(validate_trace(args.log.read_bytes(), contract, marker_definitions,
                                 int(before["actual_postgres"]["version"].split(".")[0]), before["source"]))
    report = {"qualified": not errors, "errors": errors, "profile": before["opt_level"],
              "scope": "this major's complete artifact-cold PostgreSQL job only; not whole CI/CPU/capacity/R1",
              "download_cache_may_be_warm": before["download_cache_may_be_warm"],
              "blocks": blocks, "cpu_measured": False,
              "build_plus_harness_is_not_whole_job_wall": True,
              "measurement_and_actions_overhead_in_original_30m": True,
              "actual_job_API_wall_seconds": job_wall_seconds,
              "external_measurement_anchor_sha256": hashlib.sha256(args.measurement_anchor.read_bytes()).hexdigest(),
              "experimental_all13_job_success_required": True,
              "ordinary_PR_opt1_CI_qualified": False,
              "actual_Linux_runtime_controls_required": True,
              "actual_controls_report_sha256": hashlib.sha256(controls_raw).hexdigest(),
              "environment_claim_is_three_locale_variables_not_all_process_environment": True,
              "API_wall_includes_setup_postcleanup_not_direct_product_budget": True,
              "all_current55_original_steps_and32_runs_retained": True,
              "source_owner_freeze_required": True}
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if args.require_qualified and errors else 0


if __name__ == "__main__":
    sys.exit(main())
