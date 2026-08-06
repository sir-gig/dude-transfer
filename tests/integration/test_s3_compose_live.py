"""Opt-in live S3 contracts using only random, self-cleaned prefixes."""

from __future__ import annotations

import os
import uuid

import pytest
from botocore.exceptions import ClientError

from dude_transfer import ComposeConfig, MultipartConfig, compose_s3_objects, send_file
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


def test_live_multipart_upload_is_exact_and_create_only_is_atomic(tmp_path):
    assert BUCKET is not None
    client = boto3.client("s3")
    base = os.environ.get("DUDE_INTEGRATION_PREFIX", "dude-integration").strip("/")
    run_prefix = f"{base}/run-{uuid.uuid4().hex}"
    destination_key = f"{run_prefix}/multipart.bin"
    destination = f"s3://{BUCKET}/{destination_key}"
    expected_owner = os.environ.get("DUDE_EXPECTED_BUCKET_OWNER") or None
    kms_key = os.environ.get("DUDE_INTEGRATION_KMS_KEY") or None
    owner_args = {"ExpectedBucketOwner": expected_owner} if expected_owner else {}
    create_args = dict(owner_args)
    if kms_key:
        create_args.update(ServerSideEncryption="aws:kms", SSEKMSKeyId=kms_key)

    payload = b"a" * (5 * MIB) + b"b" * (5 * MIB) + b"c" * MIB
    source_path = tmp_path / "multipart-source.bin"
    source_path.write_bytes(payload)
    checkpoint = tmp_path / "multipart.json"
    collision_checkpoint = tmp_path / "collision.json"

    def config(path):
        return MultipartConfig(
            multipart_threshold=5 * MIB,
            part_size=5 * MIB,
            max_concurrency=2,
            initial_concurrency=2,
            adaptive=False,
            max_in_flight_bytes=10 * MIB,
            checkpoint_path=path,
            verify_after_upload=True,
            if_none_match=True,
        )

    try:
        result = send_file(
            source_path,
            destination,
            client=client,
            config=config(checkpoint),
            create_args=create_args,
        )
        body = client.get_object(
            Bucket=BUCKET,
            Key=destination_key,
            **owner_args,
        )["Body"].read()

        assert body == payload
        assert result.parts == 3
        assert result.metadata["verified"] is True
        assert result.metadata["destination_condition"] == {"IfNoneMatch": "*"}
        assert not checkpoint.exists()

        with pytest.raises(ClientError) as collision:
            send_file(
                source_path,
                destination,
                client=client,
                config=config(collision_checkpoint),
                create_args=create_args,
            )
        assert collision.value.response["Error"]["Code"] in {
            "ConditionalRequestConflict",
            "PreconditionFailed",
        }
        assert collision.value.response["ResponseMetadata"]["HTTPStatusCode"] in {409, 412}
        assert not collision_checkpoint.exists()
        unchanged = client.get_object(
            Bucket=BUCKET,
            Key=destination_key,
            **owner_args,
        )["Body"].read()
        assert unchanged == payload
    finally:
        client.delete_object(
            Bucket=BUCKET,
            Key=destination_key,
            **owner_args,
        )
