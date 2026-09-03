#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0

"""Reject incomplete or drifting A2A 1.0 conformance evidence."""

from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET

from pathlib import Path


TCK_COMMIT = "263b9cfaf16a554bdfb166a7ba5b67716e946349"
TCK_ARCHIVE_SHA256 = "694c798e93fff30f650d44bdb3db0e1768b865a4f3ddbed64ec158db209bf5db"
EXPECTED = {"tests": 265, "failures": 0, "errors": 0, "skipped": 88}
REQUIRED_PASSES = {
    (
        "tests.compatibility.agent_card.test_agent_card_caching.TestAgentCardETag",
        "test_etag_present",
    ),
    (
        "tests.compatibility.core_operations.test_data_model.TestIgnoreUnrecognizedFields",
        "test_extra_fields_ignored_jsonrpc",
    ),
    (
        "tests.compatibility.core_operations.test_data_model.TestIgnoreUnrecognizedFields",
        "test_extra_fields_ignored_rest",
    ),
    (
        "tests.compatibility.core_operations.test_multi_stream.TestMultiStreamOrdering",
        "test_events_broadcast_to_all_streams[jsonrpc]",
    ),
    (
        "tests.compatibility.core_operations.test_multi_stream.TestMultiStreamOrdering",
        "test_events_broadcast_to_all_streams[http_json]",
    ),
    (
        "tests.compatibility.core_operations.test_push_notifications.TestPushNotificationDelivery",
        "test_delivery_includes_auth[jsonrpc]",
    ),
    (
        "tests.compatibility.core_operations.test_push_notifications.TestPushNotificationDelivery",
        "test_delivery_includes_auth[http_json]",
    ),
    (
        "tests.compatibility.core_operations.test_stream_ordering.TestStreamEventOrdering",
        "test_streaming_event_ordering[jsonrpc]",
    ),
    (
        "tests.compatibility.core_operations.test_stream_ordering.TestStreamEventOrdering",
        "test_streaming_event_ordering[http_json]",
    ),
    (
        "tests.compatibility.core_operations.test_error_handling.TestExtendedCardRequiresAuth",
        "test_extended_card_requires_auth_jsonrpc",
    ),
    (
        "tests.compatibility.core_operations.test_error_handling.TestExtendedCardRequiresAuth",
        "test_extended_card_requires_auth_http_json",
    ),
    (
        "tests.compatibility.http_json.test_http_status.TestHttpJsonStatusCodes",
        "test_content_type_not_supported_returns_415",
    ),
    (
        "tests.compatibility.jsonrpc.test_sse_streaming.TestSseStreamingFormat",
        "test_streaming_events_have_jsonrpc_envelope",
    ),
}


def fail(message: str) -> None:
    raise SystemExit(f"A2A conformance verification failed: {message}")


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: verify-results.py <junit-xml> <summary-json>")

    report_path = Path(sys.argv[1])
    summary_path = Path(sys.argv[2])
    if not report_path.is_file():
        fail(f"missing JUnit report: {report_path}")

    try:
        root = ET.parse(report_path).getroot()
    except (ET.ParseError, OSError) as error:
        fail(f"cannot parse JUnit report: {error}")

    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    if not suites:
        fail("JUnit report contains no test suites")

    observed = {
        field: sum(int(suite.attrib.get(field, "0")) for suite in suites)
        for field in EXPECTED
    }
    if observed != EXPECTED:
        fail(f"result drift: expected {EXPECTED}, observed {observed}")

    cases: dict[tuple[str, str], ET.Element] = {}
    for suite in suites:
        for case in suite.findall("testcase"):
            identity = (case.attrib.get("classname", ""), case.attrib.get("name", ""))
            if identity in cases:
                fail(f"duplicate test identity: {identity[0]}::{identity[1]}")
            cases[identity] = case
            if case.find("failure") is not None or case.find("error") is not None:
                fail(f"failed test present: {identity[0]}::{identity[1]}")
            skipped = case.find("skipped")
            if skipped is not None and "xfail" in (
                skipped.attrib.get("type", "") + skipped.attrib.get("message", "")
            ).lower():
                fail(f"unexpected xfail present: {identity[0]}::{identity[1]}")

    missing = sorted(REQUIRED_PASSES - cases.keys())
    if missing:
        fail("required tests missing: " + ", ".join(f"{c}::{n}" for c, n in missing))
    skipped_required = sorted(
        identity for identity in REQUIRED_PASSES if cases[identity].find("skipped") is not None
    )
    if skipped_required:
        fail(
            "required tests skipped: "
            + ", ".join(f"{c}::{n}" for c, n in skipped_required)
        )

    summary = {
        "profile": "A2A 1.0 HTTP+JSON and JSON-RPC server",
        "tck": {
            "repository": "https://github.com/a2aproject/a2a-tck",
            "commit": TCK_COMMIT,
            "archiveSha256": TCK_ARCHIVE_SHA256,
            "compatibilityPatch": "tck-compat.patch",
        },
        "result": {**observed, "passed": observed["tests"] - observed["skipped"]},
        "requiredPasses": [f"{c}::{n}" for c, n in sorted(REQUIRED_PASSES)],
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
