"""Opt-in live S3 contract test using only a random, self-cleaned prefix."""

from __future__ import annotations

import os
import uuid

import pytest

from dude_transfer import ComposeConfig, compose_s3_objects
from dude_transfer.utils import MIB

boto3 = pytest.importorskip("boto3")

BUCKET = os.environ.get("DUDE_INTEGRATION_BUCKET")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not BUCKET, reason="set DUDE_INTEGRATION_BUCKET to run live S3 tests"),
]


def test_live_server_side_composition_matches_ordered_bytes(tmp_path):
    assert BUCKET is not None
    client = boto3.client("s3")
    base = os.environ.get("DUDE_INTEGRATION_PREFIX", "dude-integration").strip("/")
    run_prefix = f"{base}/run-{uuid.uuid4().hex}"
    first_key = f"{run_prefix}/first.bin"
    last_key = f"{run_prefix}/last.bin"
    destination_key = f"{run_prefix}/result.bin"
    expected_owner = os.environ.get("DUDE_EXPECTED_BUCKET_OWNER") or None
    kms_key = os.environ.get("DUDE_INTEGRATION_KMS_KEY") or None
    owner_args = {"ExpectedBucketOwner": expected_owner} if expected_owner else {}
    create_args = dict(owner_args)
    if kms_key:
        create_args.update(ServerSideEncryption="aws:kms", SSEKMSKeyId=kms_key)

    first = b"a" * (6 * MIB)
    last = b"b" * MIB
    try:
        client.put_object(Bucket=BUCKET, Key=first_key, Body=first, **create_args)
        client.put_object(Bucket=BUCKET, Key=last_key, Body=last, **create_args)
        result = compose_s3_objects(
            [f"s3://{BUCKET}/{first_key}", f"s3://{BUCKET}/{last_key}"],
            f"s3://{BUCKET}/{destination_key}",
            client=client,
            config=ComposeConfig(
                checkpoint_path=tmp_path / "compose.json",
                verify_after_compose=True,
                expected_source_bucket_owner=expected_owner,
                if_none_match=True,
            ),
            create_args=create_args,
        )
        payload = client.get_object(
            Bucket=BUCKET,
            Key=destination_key,
            **owner_args,
        )["Body"].read()
        assert payload == first + last
        assert result.metadata["payload_bytes_downloaded_by_client"] == 0
    finally:
        client.delete_objects(
            Bucket=BUCKET,
            Delete={
                "Objects": [
                    {"Key": first_key},
                    {"Key": last_key},
                    {"Key": destination_key},
                ],
                "Quiet": True,
            },
            **owner_args,
        )
