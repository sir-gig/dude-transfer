"""Fail a tag build when the Git tag and package version disagree."""

from __future__ import annotations

import re
import sys
from pathlib import Path


def project_version(pyproject: str | Path = "pyproject.toml") -> str:
    contents = Path(pyproject).read_text(encoding="utf-8")
    project = contents.split("[project]", 1)[1].split("\n[", 1)[0]
    match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']\s*$', project, re.MULTILINE)
    if not match:
        raise ValueError("pyproject.toml has no [project] version")
    return match.group(1)


def check_tag(tag: str, version: str) -> None:
    expected = f"v{version}"
    if tag != expected:
        raise ValueError(f"release tag {tag!r} does not match package version {expected!r}")


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 1:
        raise SystemExit("usage: check_release_version.py v<version>")
    check_tag(arguments[0], project_version())
    print(f"release version verified: {arguments[0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
