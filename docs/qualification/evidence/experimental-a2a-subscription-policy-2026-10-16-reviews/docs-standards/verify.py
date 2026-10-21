#!/usr/bin/env python3
"""Replay only this five-document Standards review's byte/count checks."""
from pathlib import Path
import hashlib
import json
import re

HERE = Path(__file__).resolve().parent
BASE = HERE.parents[1]
sha = lambda data: hashlib.sha256(data).hexdigest()


def main():
    bindings = json.loads((HERE / "input-bindings.json").read_bytes())
    source = BASE / "a2a-subscription-adoption-docs-v1"
    assert sha((source / "candidate-manifest.json").read_bytes()) == bindings["candidate_manifest_sha256"]
    assert sha((source / "freeze.json").read_bytes()) == bindings["candidate_freeze_sha256"]
    for row in bindings["files"]:
        data = (source / "candidate" / row["path"]).read_bytes()
        assert (len(data), sha(data)) == (row["candidate_bytes"], row["candidate_sha256"])
        assert sha((source / "baseline" / row["path"]).read_bytes()) == row["actual_Git4e_baseline_sha256"]
    for link in bindings["new_link_occurrences"]:
        assert Path(link["observed_at"]).is_file()
    gate = BASE / "a2a-subscription-qualification-v5-network-enabled-retry-v1"
    assert sha((gate / "six-gates-summary.json").read_bytes()) == bindings["actual_six_summary_sha256"]
    for label, expected in bindings["raw_counts"].items():
        values = [tuple(map(int, item)) for item in re.findall(rb"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in [0-9.]+s$", (gate / (label + ".log")).read_bytes(), re.M)]
        assert [len(values)] + [sum(value[index] for value in values) for index in range(5)] == expected
    manifest = json.loads((HERE / "manifest.json").read_bytes())
    for row in manifest["artifacts"]:
        data = (HERE / row["path"]).read_bytes()
        assert (len(data), sha(data)) == (row["bytes"], row["sha256"])
    print("PASS: fixed5docs Standards0; exact baseline/examples, links and actual local counts; no source/publication independent-review claim")


if __name__ == "__main__":
    main()
