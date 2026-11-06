#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Hash/decompress original review bytes; never execute retained review code."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath

HERE = Path(__file__).resolve().parent
sha = lambda raw: hashlib.sha256(raw).hexdigest()


def safe(value):
    path = PurePosixPath(value)
    assert not path.is_absolute() and ".." not in path.parts and value
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-candidate-root", type=Path,
                        help="root containing the unchanged original178 published paths; defaults to repository")
    args = parser.parse_args()
    base_root = args.base_candidate_root.resolve() if args.base_candidate_root else HERE.parents[3]
    delivery = json.loads((HERE / "delivery.json").read_bytes())
    assert sha(Path(__file__).read_bytes()) == delivery["verifier_sha256"]
    assert sha((HERE / "README.md").read_bytes()) == delivery["README_sha256"]
    assert delivery["source930_tree"] == "0e91c287e33ccf7f33c0f11e3ed0c204c2826f3373c4a6f3e701a56cd66c7bad"
    assert delivery["compiled538_sha256"] == "43e9cb48b9fe45bb8631b99fe9548db84f8e652dba2d147c8198bd04dff3a6f4"
    logical, stored = {}, set()
    for row in delivery["files"]:
        original = str(safe(row["original_path"]))
        name = str(safe(row["stored_path"]))
        path = HERE / name
        assert not path.is_symlink()
        raw = path.read_bytes()
        assert (len(raw), sha(raw)) == (row["stored_bytes"], row["stored_sha256"])
        if row["encoding"] == "gzip":
            assert raw[4:8] == b"\0" * 4 and not raw[3] & 8
            value = gzip.decompress(raw)
        else:
            assert row["encoding"] == "identity"
            value = raw
        assert (len(value), sha(value)) == (row["original_bytes"], row["original_sha256"])
        if row.get("already_gzip_original"):
            assert value[4:8] == b"\0" * 4 and not value[3] & 8
            inflated = gzip.decompress(value)
            assert (len(inflated), sha(inflated)) == (row["uncompressed_bytes"], row["uncompressed_sha256"])
        assert original not in logical and name not in stored
        logical[original] = value
        stored.add(name)
    review_names = {name for name in logical if not name.startswith("adoption/")}
    assert len(review_names) == delivery["original_new_group_file_count"] == 48
    assert {name for name in logical if name.startswith("adoption/")} == set(delivery["non_review_adoption_records"])
    assert len(logical) == delivery["retained_original_file_count"] == 50
    for group in delivery["review_groups"]:
        prefix = group["name"] + "/"
        manifest_path = prefix + group["original_manifest"]
        manifest_raw = logical[manifest_path]
        assert sha(manifest_raw) == group["manifest_sha256"]
        assert sha(logical[prefix + "receipt.json"]) == group["original_receipt_sha256"]
        manifest = json.loads(manifest_raw)
        rows = manifest[group["manifest_entries_field"]]
        assert len(rows) == group["manifest_covered_files"]
        covered = set()
        for row in rows:
            name = prefix + str(safe(row["path"]))
            assert (len(logical[name]), sha(logical[name])) == (row["bytes"], row["sha256"])
            covered.add(name)
        actual = {name for name in logical if name.startswith(prefix)}
        assert covered | {manifest_path, prefix + "receipt.json"} == actual
        assert len(actual) == group["original_file_count"]
    metadata = {"delivery.json", "verify.py", "README.md", "base-publication-manifest.json"}
    actual = {str(p.relative_to(HERE)) for p in HERE.rglob("*") if p.is_file()}
    assert actual == stored | metadata
    base = delivery["base_publication"]
    raw_manifest = (HERE / base["candidate_manifest_file"]).read_bytes()
    assert sha(raw_manifest) == base["candidate_manifest_sha256"] == "739688ef10fe7f01579d1cb4ba0174ea3e257a0fc6782f82dc5a64f3cb90939c"
    manifest = json.loads(raw_manifest)
    assert manifest["count"] == len(manifest["files"]) == base["candidate_files"] == 178
    for row in manifest["files"]:
        path = base_root / safe(row["path"])
        assert not path.is_symlink()
        value = path.read_bytes()
        assert (len(value), sha(value)) == (row["bytes"], row["sha256"]), row["path"]
    evidence = base_root / base["evidence_path"]
    assert sha((evidence / "manifest.json").read_bytes()) == base["evidence_manifest_sha256"] == "af5c515f06572f89f930b03d00d88abc52c927d963f382bfb8a4cc0a6c7e7e17"
    assert sha((evidence / "seal.json").read_bytes()) == base["evidence_seal_sha256"]
    # Existing source Standards groups are read in place, never recopied here.
    index = {row["path"]: row for row in json.loads((evidence / "artifact-index.json").read_bytes())["artifacts"]}
    for group in delivery["existing_base_groups"]:
        def original(relative):
            name = group["base_path"] + "/" + relative
            name = name if name in index else name + ".gz"
            row = index[name]
            value = (evidence / name).read_bytes()
            assert (len(value), sha(value)) == (row["stored_bytes"], row["stored_sha256"])
            # An original .gz is itself a manifest-bound file; preserve its container.
            return value if relative.endswith(".gz") or row["storage_encoding"] == "identity" else gzip.decompress(value)
        raw_manifest = original(group["manifest"])
        assert sha(raw_manifest) == group["manifest_sha256"]
        assert sha(original("receipt.json")) == group["receipt_sha256"]
        definition = json.loads(raw_manifest)
        rows = definition.get("files", definition.get("artifacts"))
        for row in rows:
            value = original(row["path"])
            assert (len(value), sha(value)) == (row["bytes"], row["sha256"])
    print(json.dumps({"state": "all original review bytes and manifest entries verified",
                      "new_groups": 5, "new_original_review_files": 48, "base_groups_reused": 2,
                      "non_review_adoption_records": 2,
                      "base_candidate_paths": 178, "new_delivery_files": len(actual),
                      "review_or_product_code_executed": False,
                      "new_independent_review_or_product_qualification": False}, sort_keys=True))


if __name__ == "__main__":
    main()
