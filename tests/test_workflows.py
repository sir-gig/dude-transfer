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


def test_ci_does_not_duplicate_pull_request_runs():
    workflow = (WORKFLOWS / "ci.yml").read_text(encoding="utf-8")
    assert "push:\n    branches: [main]" in workflow
    assert "pull_request:" in workflow


def test_live_s3_workflow_is_manual_oidc_only_and_scope_guarded():
    workflow = (WORKFLOWS / "live-s3.yml").read_text(encoding="utf-8")
    assert "workflow_dispatch:" in workflow
    assert "pull_request:" not in workflow
    assert "id-token: write" in workflow
    assert "environment: live-s3-integration" in workflow
    assert "confirm_synthetic:" in workflow
    assert "allowed-account-ids: ${{ inputs.expected_bucket_owner }}" in workflow
    assert "inline-session-policy: ${{ steps.scope.outputs.policy }}" in workflow
    assert "aws-access-key-id" not in workflow.lower()
    assert "aws-secret-access-key" not in workflow.lower()
    assert "secrets." not in workflow
