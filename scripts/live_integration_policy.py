"""Validate live-test scope and generate a least-privilege AWS session policy."""

from __future__ import annotations

import argparse
import ipaddress
import json
import re
from pathlib import Path
from typing import Any

_BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_PREFIX = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$")
_ACCOUNT = re.compile(r"^[0-9]{12}$")
_REGION = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)+-[0-9]+$")
_ROLE = re.compile(
    r"^arn:aws:iam::([0-9]{12}):role/([A-Za-z0-9+=,.@_/-]{1,512})$"
)


def _validate_bucket(bucket: str) -> None:
    if not _BUCKET.fullmatch(bucket) or ".." in bucket or ".-" in bucket or "-." in bucket:
        raise ValueError("bucket must be a valid DNS-compatible S3 bucket name")
    try:
        ipaddress.ip_address(bucket)
    except ValueError:
        return
    raise ValueError("bucket must not be formatted as an IP address")


def _validate_prefix(prefix: str) -> None:
    if (
        not _PREFIX.fullmatch(prefix)
        or prefix.endswith("/")
        or "//" in prefix
        or any(segment in {".", ".."} for segment in prefix.split("/"))
        or (prefix != "dude-integration" and not prefix.startswith("dude-integration/"))
    ):
        raise ValueError(
            "prefix must be dude-integration or its relative 1-128 character child "
            "without empty, dot, or dot-dot segments"
        )


def build_policy(
    *,
    bucket: str,
    prefix: str,
    region: str,
    expected_account: str,
    role_arn: str,
    confirm_synthetic: str,
) -> dict[str, Any]:
    if confirm_synthetic.lower() != "true":
        raise ValueError("confirm_synthetic must be true before live AWS writes are allowed")
    _validate_bucket(bucket)
    _validate_prefix(prefix)
    if not _ACCOUNT.fullmatch(expected_account):
        raise ValueError("expected_account must be a twelve-digit AWS account ID")
    if not _REGION.fullmatch(region):
        raise ValueError("region must be an AWS region identifier")
    role = _ROLE.fullmatch(role_arn)
    if not role:
        raise ValueError("DUDE_AWS_ROLE_ARN must be a valid IAM role ARN")
    role_account, _ = role.groups()
    if role_account != expected_account:
        raise ValueError("integration role and expected bucket owner must use the same account")

    bucket_arn = f"arn:aws:s3:::{bucket}"
    object_arn = f"{bucket_arn}/{prefix}/*"
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ListOwnedSyntheticPrefix",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": bucket_arn,
                "Condition": {
                    "StringLike": {
                        "s3:prefix": [prefix, f"{prefix}/*"],
                    }
                },
            },
            {
                "Sid": "OperateOnlyOnOwnedSyntheticObjects",
                "Effect": "Allow",
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:DeleteObject",
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:ListMultipartUploadParts",
                    "s3:PutObject",
                ],
                "Resource": object_arn,
            },
        ],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--expected-account", required=True)
    parser.add_argument("--role-arn", required=True)
    parser.add_argument("--confirm-synthetic", required=True)
    parser.add_argument("--github-output")
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    try:
        policy = build_policy(
            bucket=arguments.bucket,
            prefix=arguments.prefix,
            region=arguments.region,
            expected_account=arguments.expected_account,
            role_arn=arguments.role_arn,
            confirm_synthetic=arguments.confirm_synthetic,
        )
    except ValueError as error:
        raise SystemExit(f"refusing live integration run: {error}") from error
    encoded = json.dumps(policy, separators=(",", ":"), sort_keys=True)
    if arguments.github_output:
        output_path = Path(arguments.github_output)
        with output_path.open("a", encoding="utf-8", newline="\n") as output:
            output.write(f"policy={encoded}\n")
    else:
        print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
