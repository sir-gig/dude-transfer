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
    pypi = workflow.index("pypa/gh-action-pypi-publish@")
    publish = workflow.index("gh release create")
    assert attest < pypi < publish
    assert "attestations: write" in workflow
    assert "id-token: write" in workflow


def test_release_build_cannot_request_an_oidc_token():
    workflow = (WORKFLOWS / "release.yml").read_text(encoding="utf-8")
    build = workflow.split("  build-release:", 1)[1].split("  attest-release:", 1)[0]
    assert "permissions: {}" in workflow
    assert "id-token:" not in build
    assert "python -m build" in build
    assert "git merge-base --is-ancestor" in build
    assert "actions/upload-artifact@" in build


def test_release_uses_trusted_publishing_without_static_pypi_secrets():
    workflow = (WORKFLOWS / "release.yml").read_text(encoding="utf-8")
    pypi = workflow.split("  publish-pypi:", 1)[1].split("  publish-github:", 1)[0]
    github = workflow.split("  publish-github:", 1)[1]
    assert "name: pypi" in pypi
    assert "id-token: write" in pypi
    assert "pypa/gh-action-pypi-publish@" in pypi
    assert "packages-dir: dist/" in pypi
    assert "password:" not in pypi
    assert "actions/checkout@" not in pypi
    assert "run:" not in pypi
    assert "secrets." not in workflow
    assert workflow.count("name: release-distributions") == 4
    assert "needs: [build-release, attest-release, publish-pypi]" in github


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
