from __future__ import annotations

import json
import subprocess
import sys

import pytest

from scripts.live_integration_policy import build_policy


def valid_arguments(**overrides):
    arguments = {
        "bucket": "dude-disposable-123",
        "prefix": "dude-integration/contracts",
        "region": "us-east-2",
        "expected_account": "123456789012",
        "role_arn": "arn:aws:iam::123456789012:role/github/dude-live",
        "confirm_synthetic": "true",
    }
    arguments.update(overrides)
    return arguments


def test_policy_is_restricted_to_exact_bucket_prefix():
    policy = build_policy(**valid_arguments())
    listing, objects = policy["Statement"]

    assert listing["Resource"] == "arn:aws:s3:::dude-disposable-123"
    assert listing["Condition"]["StringLike"]["s3:prefix"] == [
        "dude-integration/contracts",
        "dude-integration/contracts/*",
    ]
    assert objects["Resource"] == (
        "arn:aws:s3:::dude-disposable-123/dude-integration/contracts/*"
    )
    assert set(objects["Action"]) == {
        "s3:AbortMultipartUpload",
        "s3:DeleteObject",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListMultipartUploadParts",
        "s3:PutObject",
    }
    assert "s3:DeleteBucket" not in objects["Action"]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"confirm_synthetic": "false"}, "must be true"),
        ({"bucket": "192.168.1.1"}, "must not be formatted as an IP"),
        ({"bucket": "Bad_Bucket"}, "DNS-compatible"),
        ({"prefix": "dude/../production"}, "dot-dot"),
        ({"prefix": "/dude-integration"}, "must be dude-integration"),
        ({"prefix": "production/diagnostics"}, "must be dude-integration"),
        ({"region": "$(unsafe)"}, "region identifier"),
        ({"expected_account": "123"}, "twelve-digit"),
        ({"role_arn": "arn:aws:iam::123456789012:user/not-a-role"}, "IAM role ARN"),
        (
            {"role_arn": "arn:aws:iam::210987654321:role/dude-live"},
            "same account",
        ),
    ],
)
def test_invalid_or_ambiguous_scope_is_rejected(overrides, message):
    with pytest.raises(ValueError, match=message):
        build_policy(**valid_arguments(**overrides))


def test_cli_writes_single_line_github_output(tmp_path):
    output = tmp_path / "github-output.txt"
    command = [
        sys.executable,
        "scripts/live_integration_policy.py",
        "--bucket",
        "dude-disposable-123",
        "--prefix",
        "dude-integration",
        "--region",
        "us-east-1",
        "--expected-account",
        "123456789012",
        "--role-arn",
        "arn:aws:iam::123456789012:role/dude-live",
        "--confirm-synthetic",
        "true",
        "--github-output",
        str(output),
    ]
    completed = subprocess.run(command, check=True, capture_output=True, text=True)

    assert completed.stdout == ""
    key, encoded = output.read_text(encoding="utf-8").rstrip("\n").split("=", 1)
    assert key == "policy"
    assert json.loads(encoded)["Version"] == "2012-10-17"
