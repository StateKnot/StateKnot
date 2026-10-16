"""Strict fixed CI15 text evidence, separate from all existing JSON evidence."""
import json
import re

PREFIX = "STATEKNOT_MCP_BOUND_STATE_POSTGRES:"
EXACT = PREFIX + " consumers=3 positive_commits=6 refused_rebinds=12 tenant_ledgers=2 complete_journals=true"
SKIPPED = "STATEKNOT_MCP_BOUND_STATE_POSTGRES_SKIPPED:"
TESTS = 15
TEXT_CONTRACT = {PREFIX: {"exact": EXACT, "count": 1}}


def strict_step_lines(raw, label):
    """Read only LF-delimited raw step lines; never repair CR/Unicode separators."""
    active = False
    observed = []
    for raw_line in raw.decode().split("\n"):
        line = re.sub(r"^\d{4}-\d\d-\d\dT[^ ]+Z ", "", raw_line)
        if line.startswith("STATEKNOT_COLD_STEP_BEGIN="):
            active = json.loads(line.split("=", 1)[1])["step"] == label
        elif line.startswith("STATEKNOT_COLD_STEP_END="):
            active = False
        elif active:
            observed.append(line)
    return observed


def observe(block, line):
    # No strip(): indentation, suffixes, field order and trailing spaces are significant.
    if line.startswith(PREFIX):
        block.setdefault("mcp_text_prefix_lines", []).append(line)
    if SKIPPED in line:
        block.setdefault("mcp_skipped_lines", []).append(line)
    if line.startswith("running "):
        block.setdefault("mcp_running_lines", []).append(line)
    if line.startswith("test result:"):
        block.setdefault("mcp_summary_lines", []).append(line)


def validate(block, acceptance, expected_tests):
    errors = []
    if type(expected_tests) is not int or expected_tests != TESTS:
        errors.append("MCP component is fixed to the reviewed 15-test CI inventory")
    text_contract = acceptance.get("text_markers")
    if text_contract != TEXT_CONTRACT or type((text_contract or {}).get(PREFIX, {}).get("count")) is not int:
        errors.append("MCP strict text-marker contract missing or changed")
    lines = block.get("mcp_text_prefix_lines", [])
    if len(lines) != 1 or lines.count(EXACT) != 1:
        errors.append("MCP requires one prefix line and one exact full invariant line")
    if block.get("mcp_skipped_lines"):
        errors.append("MCP skipped marker appears")
    if block.get("mcp_running_lines", []) != ["running 15 tests"]:
        errors.append("MCP requires one exact running15 header")
    summaries = block.get("mcp_summary_lines", [])
    if len(summaries) != 1 or not re.fullmatch(
        r"test result: ok[.] 15 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in [0-9.]+s",
        summaries[0],
    ):
        errors.append("MCP requires one complete15/0/0/0/0 summary")
    return errors


def validate_step(step):
    env = step.get("env") or {}
    errors = []
    if env.get("STATEKNOT_REQUIRE_POSTGRES_TESTS") != "1":
        errors.append("MCP mandatory PostgreSQL switch missing or disabled")
    url = env.get("STATEKNOT_TEST_DATABASE_URL")
    if not isinstance(url, str) or not url.strip():
        errors.append("MCP PostgreSQL URL missing or empty")
    return errors
