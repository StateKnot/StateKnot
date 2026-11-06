#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Verify retained review bytes without executing any review or product code."""

import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath


def main():
    root = Path(__file__).resolve().parent
    record = json.loads((root / "delivery.json").read_bytes())
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == record["verifier_sha256"]
    logical = {}
    stored = set()
    for row in record["files"]:
        original = PurePosixPath(row["original_path"])
        path = PurePosixPath(row["stored_path"])
        assert not original.is_absolute() and ".." not in original.parts
        assert not path.is_absolute() and ".." not in path.parts
        file = root / path
        assert not file.is_symlink()
        raw = file.read_bytes()
        assert len(raw) == row["stored_bytes"]
        assert hashlib.sha256(raw).hexdigest() == row["stored_sha256"]
        assert row["encoding"] in {"identity", "gzip"}
        data = gzip.decompress(raw) if row["encoding"] == "gzip" else raw
        assert len(data) == row["original_bytes"]
        assert hashlib.sha256(data).hexdigest() == row["original_sha256"]
        assert str(original) not in logical and str(path) not in stored
        logical[str(original)] = data
        stored.add(str(path))
    actual = {
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file() and path not in {root / "delivery.json", root / "verify.py"}
    }
    assert actual == stored
    for group in record["review_groups"]:
        manifest = json.loads(logical[group + "/manifest.json"])
        expected = set()
        for row in manifest["files"]:
            path = group + "/" + row["path"]
            data = logical[path]
            assert len(data) == row["bytes"]
            assert hashlib.sha256(data).hexdigest() == row["sha256"]
            expected.add(path)
        assert expected | {group + "/manifest.json"} == {
            path for path in logical if path.startswith(group + "/")
        }
    print(json.dumps({
        "state": "original review manifests and all retained bytes verified",
        "review_groups": len(record["review_groups"]),
        "original_files": len(logical),
        "losslessly_compressed_patches": sum(row["encoding"] == "gzip" for row in record["files"]),
        "product_or_review_code_executed": False,
        "new_independent_review_or_product_qualification": False,
    }))


if __name__ == "__main__":
    main()
