import re
from pathlib import Path

WORKFLOWS = Path(".github/workflows")


def test_third_party_actions_are_pinned_to_full_commits():
    references = []
    for path in WORKFLOWS.glob("*.yml"):
        references.extend(re.findall(r"\buses:\s+[^\s@]+@([^\s#]+)", path.read_text()))
    assert references
    assert all(re.fullmatch(r"[0-9a-f]{40}", reference) for reference in references)


def test_release_generates_provenance_before_publishing():
    workflow = (WORKFLOWS / "release.yml").read_text(encoding="utf-8")
    attest = workflow.index("actions/attest-build-provenance@")
    publish = workflow.index("gh release create")
    assert attest < publish
    assert "attestations: write" in workflow
    assert "id-token: write" in workflow
