# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Exact source-only reader functions extracted from the published verifier."""
import gzip,hashlib,io,json,re,tarfile
from pathlib import Path
BASE="31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca"
sha=lambda data:hashlib.sha256(data).hexdigest()
canon=lambda value:json.dumps(value,sort_keys=True,separators=(",",":")).encode()

def safe_path(value):
    path = Path(value)
    assert value and not path.is_absolute() and ".." not in path.parts, value
    return path


def apply_patch(files, raw, reverse=False):
    """Exact byte/context application of the archived fixture unified diffs."""
    lines = raw.splitlines(keepends=True)
    result = dict(files)
    index = 0
    while index < len(lines):
        assert lines[index].startswith(b"--- a/"), lines[index]
        name = lines[index][6:].rstrip(b"\n").decode()
        safe_path(name)
        index += 1
        assert lines[index] == ("+++ b/" + name + "\n").encode()
        index += 1
        source = files.get(name, b"").splitlines(keepends=True)
        output, cursor = [], 0
        while index < len(lines) and lines[index].startswith(b"@@ "):
            match = re.fullmatch(rb"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@[^\n]*\n", lines[index])
            assert match, lines[index]
            a, ac, b, bc = match.groups()
            start, old_count, new_count = (int(b), int(bc or b"1"), int(ac or b"1")) if reverse else (int(a), int(ac or b"1"), int(bc or b"1"))
            start = max(start - 1, 0)
            assert start >= cursor
            output.extend(source[cursor:start])
            cursor = start
            index += 1
            old_seen = new_seen = 0
            while old_seen < old_count or new_seen < new_count:
                line = lines[index]
                index += 1
                kind, content = line[:1], line[1:]
                if reverse:
                    kind = {b"+": b"-", b"-": b"+", b" ": b" "}[kind]
                if kind in (b" ", b"-"):
                    assert source[cursor] == content, (name, cursor, line)
                    cursor += 1
                    old_seen += 1
                if kind in (b" ", b"+"):
                    output.append(content)
                    new_seen += 1
                assert kind in (b" ", b"-", b"+")
            assert (old_seen, new_seen) == (old_count, new_count)
        output.extend(source[cursor:])
        result[name] = b"".join(output)
    return result


def check_rows(files, manifest):
    rows = manifest["files"]
    assert len(files) == manifest["count"] == len(rows)
    assert len({row["path"] for row in rows}) == len(rows)
    actual = [{"path": row["path"], "bytes": len(files[row["path"]]), "sha256": sha(files[row["path"]])} for row in rows]
    assert actual == rows
    assert sha(canon(rows)) == manifest["source_tree_sha256"]


def reconstruct_base(repo):
    """Source-only A2A-facts -> PITR fixture -> lossless MCP -> Debug chain."""
    old = repo / "docs/qualification/evidence/experimental-mcp-lossless-bound-state-2026-10-16"
    refs = json.loads((old / "source/base-references.json").read_bytes())["files"]
    with tarfile.open(fileobj=io.BytesIO((repo / refs[0]["path"]).read_bytes()), mode="r:gz") as archive:
        members = archive.getmembers()
        assert len(members) == len({member.name for member in members}) == 923
        assert all(member.isfile() and member.name.startswith("source/") for member in members)
        for member in members:
            safe_path(member.name)
        files = {member.name[7:]: archive.extractfile(member).read() for member in members}
    files["crates/stateknot-runtime/tests/postgres/child_join_pitr.rs"] = gzip.decompress((repo / refs[2]["path"]).read_bytes())
    for change in json.loads((old / "source/final-reconstruction.json").read_bytes())["changes"]:
        assert (sha(files[change["path"]]) if change["path"] in files else None) == change["before_sha256"]
        data = gzip.decompress((old / change["payload"]).read_bytes())
        assert (len(data), sha(data)) == (change["bytes"], change["after_sha256"])
        files[change["path"]] = data
    manifest = json.loads(gzip.decompress((old / "source/final-source-manifest.json.gz").read_bytes()))
    check_rows(files, manifest)
    assert manifest["source_tree_sha256"] == "69af3a0f28d333f4e67d2b2cfcc2afc071938a6ce398527cc098d31ee8db60d8"
    debug = repo / "docs/qualification/evidence/experimental-mcp-http-diagnostics-2026-10-16"
    for change in json.loads((debug / "source/reconstruction.json").read_bytes())["changes"]:
        assert sha(files[change["path"]]) == change["before_sha256"]
        data = gzip.decompress((debug / change["payload"]).read_bytes())
        assert (len(data), sha(data)) == (change["bytes"], change["after_sha256"])
        files[change["path"]] = data
    manifest = json.loads(gzip.decompress((debug / "source/final-source-manifest.json.gz").read_bytes()))
    check_rows(files, manifest)
    assert manifest["source_tree_sha256"] == BASE and len(files) == 928
    return files
