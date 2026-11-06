#!/usr/bin/env python3
"""Verify every original evidence byte; optionally extract only after verification."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import tarfile


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extract", type=Path, help="new directory; never overwrite an existing path")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    index = json.loads((root / "artifact-index.json").read_text(encoding="utf-8"))
    archive_name = index["archive"]["path"]
    if archive_name != "review-originals.tar.gz":
        raise ValueError("unexpected archive path")
    archive = root / archive_name
    if archive.stat().st_size != index["archive"]["bytes"] or sha256_file(archive) != index["archive"]["sha256"]:
        raise ValueError("archive digest/size mismatch")
    expected = {}
    for row in index["files"]:
        path = PurePosixPath(row["path"])
        if path.is_absolute() or ".." in path.parts or str(path) != row["path"] or not path.parts or "\\" in row["path"] or ":" in path.parts[0] or row["path"] in expected:
            raise ValueError("unsafe or duplicate index path")
        expected[row["path"]] = row
    if len(expected) != index["count"] or sum(row["bytes"] for row in expected.values()) != index["uncompressed_file_bytes"]:
        raise ValueError("index totals mismatch")
    seen = set()
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar:
            if not member.isfile() or member.name not in expected or member.name in seen or member.pax_headers:
                raise ValueError("unexpected, duplicate, or non-regular archive member: " + member.name)
            row = expected[member.name]
            if member.size != row["bytes"] or member.mode != 0o644 or member.uid or member.gid or member.mtime or member.uname or member.gname:
                raise ValueError("archive member metadata mismatch: " + member.name)
            stream = tar.extractfile(member)
            if stream is None:
                raise ValueError("unreadable archive member")
            digest = hashlib.sha256()
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
            if digest.hexdigest() != row["sha256"]:
                raise ValueError("member digest mismatch: " + member.name)
            seen.add(member.name)
    if seen != set(expected):
        raise ValueError("missing archive members")
    if args.extract is not None:
        destination = args.extract.absolute()
        destination.mkdir(parents=True, exist_ok=False)
        with tarfile.open(archive, "r:gz") as tar:
            for member in tar:
                target = destination.joinpath(*PurePosixPath(member.name).parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(member) as source, target.open("xb") as output:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        output.write(chunk)
    print(json.dumps({"verified": True, "files": len(seen), "archive_sha256": index["archive"]["sha256"], "execution_qualification_awarded": False}))


if __name__ == "__main__":
    main()
