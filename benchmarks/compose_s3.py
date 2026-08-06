"""Benchmark S3-side composition against a driver round trip.

The script never deletes source or destination objects. Use synthetic data and
an authorized benchmark prefix, then remove outputs under your storage policy.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import platform
import tempfile
import time
import uuid
from pathlib import Path

import boto3

from dude_transfer import ComposeConfig, MultipartConfig, compose_s3_objects, send_file
from dude_transfer.s3 import parse_s3_uri
from dude_transfer.utils import parse_size


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("destination_prefix", help="unique authorized s3:// benchmark prefix")
    parser.add_argument("sources", nargs="+", help="ordered, byte-concatenable S3 objects")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--copy-part-size", default="512MiB")
    parser.add_argument("--upload-part-size", default="64MiB")
    parser.add_argument("--max-local-size", default="20GiB")
    parser.add_argument("--expected-bucket-owner")
    return parser.parse_args()


def _destination(prefix: str, run_id: str, repetition: int, engine: str) -> str:
    bucket, key = parse_s3_uri(prefix)
    return f"s3://{bucket}/{key.rstrip('/')}/run={run_id}/rep={repetition}/{engine}.bin"


def _server_side(args, client, destination):
    started = time.monotonic()
    result = compose_s3_objects(
        args.sources,
        destination,
        client=client,
        config=ComposeConfig(
            copy_part_size=args.copy_part_size,
            max_concurrency=args.concurrency,
            verify_after_compose=True,
            expected_source_bucket_owner=args.expected_bucket_owner,
        ),
        create_args={
            "ExpectedBucketOwner": args.expected_bucket_owner
        } if args.expected_bucket_owner else None,
    )
    return {
        "engine": "dude-server-side-compose",
        "destination": destination,
        "elapsed_seconds": time.monotonic() - started,
        "bytes": result.bytes_transferred,
        "client_payload_bytes_downloaded": 0,
        "parts": result.parts,
        "result": result.as_dict(),
    }


def _driver_round_trip(args, client, destination, total_size):
    if total_size > parse_size(args.max_local_size):
        raise ValueError(
            f"baseline needs {total_size} local bytes; increase --max-local-size explicitly"
        )
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="dude-compose-benchmark-") as directory:
        local = Path(directory) / "concatenated.bin"
        with local.open("wb") as stream:
            for uri in args.sources:
                bucket, key = parse_s3_uri(uri)
                download_args = {}
                if args.expected_bucket_owner:
                    download_args["ExpectedBucketOwner"] = args.expected_bucket_owner
                client.download_fileobj(bucket, key, stream, ExtraArgs=download_args or None)
        result = send_file(
            local,
            destination,
            client=client,
            config=MultipartConfig(
                part_size=args.upload_part_size,
                max_concurrency=args.concurrency,
                verify_after_upload=True,
            ),
            create_args={
                "ExpectedBucketOwner": args.expected_bucket_owner
            } if args.expected_bucket_owner else None,
        )
    return {
        "engine": "driver-download-then-upload",
        "destination": destination,
        "elapsed_seconds": time.monotonic() - started,
        "bytes": result.bytes_transferred,
        "client_payload_bytes_downloaded": total_size,
        "parts": result.parts,
        "result": result.as_dict(),
    }


def main() -> None:
    args = _arguments()
    if args.repetitions < 1:
        raise ValueError("--repetitions must be positive")
    client = boto3.client("s3")
    source_sizes = []
    for uri in args.sources:
        bucket, key = parse_s3_uri(uri)
        request = {"Bucket": bucket, "Key": key}
        if args.expected_bucket_owner:
            request["ExpectedBucketOwner"] = args.expected_bucket_owner
        source_sizes.append(int(client.head_object(**request)["ContentLength"]))
    total_size = sum(source_sizes)
    run_id = uuid.uuid4().hex
    results = []
    for repetition in range(1, args.repetitions + 1):
        server_destination = _destination(
            args.destination_prefix, run_id, repetition, "server-side-compose"
        )
        baseline_destination = _destination(
            args.destination_prefix, run_id, repetition, "driver-round-trip"
        )
        server_operation = (args, client, server_destination)
        baseline_operation = (args, client, baseline_destination, total_size)
        if repetition % 2:
            results.append(_server_side(*server_operation))
            results.append(_driver_round_trip(*baseline_operation))
        else:
            results.append(_driver_round_trip(*baseline_operation))
            results.append(_server_side(*server_operation))

    print(
        json.dumps(
            {
                "run_id": run_id,
                "python": platform.python_version(),
                "platform": platform.platform(),
                "boto3": importlib.metadata.version("boto3"),
                "dude_transfer": importlib.metadata.version("dude-transfer"),
                "source_count": len(args.sources),
                "source_sizes": source_sizes,
                "total_size": total_size,
                "results": results,
                "notes": [
                    "Execution order alternates between repetitions.",
                    "Outputs are retained; the script never deletes S3 objects.",
                    "Report cluster, runtime, regions, endpoint, encryption, and raw JSON.",
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
