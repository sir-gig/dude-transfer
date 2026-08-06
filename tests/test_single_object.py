from __future__ import annotations

import pytest
from test_compose import FakeComposeS3, source
from test_dataframe import FakeDataFrame, FakeWriter

from dude_transfer import ConfigurationError, send_dataframe_as_single_object
from dude_transfer.compose import ComposeConfig
from dude_transfer.s3 import parse_s3_uri
from dude_transfer.single_object import _cleanup_stage, _list_stage
from dude_transfer.utils import MIB


class MissingObjectError(Exception):
    response = {
        "Error": {"Code": "NoSuchKey"},
        "ResponseMetadata": {"HTTPStatusCode": 404},
    }


class StageWriter(FakeWriter):
    def __init__(self, client, outputs):
        super().__init__()
        self.client = client
        self.outputs = outputs

    def save(self, destination):
        super().save(destination)
        bucket, prefix = parse_s3_uri(destination)
        for name, size in self.outputs:
            key = f"{prefix}{name}"
            self.client.stage_objects[(bucket, key)] = size
            self.client.sources[(bucket, key)] = source(size, f'"{name}"')


class RetryStageWriter(FakeWriter):
    def __init__(self, client, outputs_by_attempt):
        super().__init__()
        self.client = client
        self.outputs_by_attempt = outputs_by_attempt
        self.save_count = 0

    def save(self, destination):
        FakeWriter.save(self, destination)
        bucket, prefix = parse_s3_uri(destination)
        outputs = self.outputs_by_attempt[self.save_count]
        self.save_count += 1
        for name, size in outputs:
            key = f"{prefix}{name}"
            self.client.stage_objects[(bucket, key)] = size
            self.client.sources[(bucket, key)] = source(size, f'"{name}-{self.save_count}"')


class SingleObjectS3(FakeComposeS3):
    def __init__(self):
        super().__init__({})
        self.stage_objects = {}
        self.list_requests = []
        self.delete_requests = []
        self.put_args = None

    def head_object(self, **kwargs):
        self.head_requests.append(kwargs)
        identity = (kwargs["Bucket"], kwargs["Key"])
        if self.destination and identity == self.destination["identity"]:
            return {
                "ContentLength": self.destination["size"],
                "Metadata": self.destination["metadata"],
            }
        if identity in self.sources:
            item = self.sources[identity]
            return {"ContentLength": item["size"], "ETag": item["etag"]}
        raise MissingObjectError(identity)

    def list_objects_v2(self, **kwargs):
        self.list_requests.append(kwargs)
        contents = [
            {"Key": key, "Size": size}
            for (bucket, key), size in self.stage_objects.items()
            if bucket == kwargs["Bucket"] and key.startswith(kwargs["Prefix"])
        ]
        return {"Contents": contents, "IsTruncated": False}

    def delete_objects(self, **kwargs):
        self.delete_requests.append(kwargs)
        for item in kwargs["Delete"]["Objects"]:
            identity = (kwargs["Bucket"], item["Key"])
            self.stage_objects.pop(identity, None)
            self.sources.pop(identity, None)
        return {}

    def put_object(self, **kwargs):
        self.put_args = kwargs
        self.destination = {
            "identity": (kwargs["Bucket"], kwargs["Key"]),
            "size": len(kwargs["Body"]),
            "metadata": kwargs["Metadata"],
        }
        return {"ETag": '"empty"'}


class CleanupDeniedError(Exception):
    response = {
        "Error": {"Code": "AccessDenied"},
        "ResponseMetadata": {"HTTPStatusCode": 403},
    }


class CleanupDeniedS3(SingleObjectS3):
    def delete_objects(self, **kwargs):
        self.delete_requests.append(kwargs)
        raise CleanupDeniedError("cleanup denied")


class PaginatedStageS3(SingleObjectS3):
    def list_objects_v2(self, **kwargs):
        self.list_requests.append(kwargs)
        if "ContinuationToken" not in kwargs:
            return {
                "Contents": [
                    {"Key": f'{kwargs["Prefix"]}part-00010-run-c000.json', "Size": 7 * MIB}
                ],
                "IsTruncated": True,
                "NextContinuationToken": "next-page",
            }
        assert kwargs["ContinuationToken"] == "next-page"
        return {
            "Contents": [
                {"Key": f'{kwargs["Prefix"]}part-00002-run-c000.json', "Size": 6 * MIB},
                {"Key": f'{kwargs["Prefix"]}_SUCCESS', "Size": 0},
            ],
            "IsTruncated": False,
        }


def dataframe_with_outputs(client, outputs):
    df = FakeDataFrame(size=20 * MIB, partitions=len(outputs))
    df.write = StageWriter(client, outputs)
    return df


def dataframe_with_output_attempts(client, outputs_by_attempt):
    df = FakeDataFrame(size=20 * MIB, partitions=len(outputs_by_attempt[0]))
    df.write = RetryStageWriter(client, outputs_by_attempt)
    return df


def test_dataframe_jsonl_stages_in_parallel_and_composes_in_numeric_order(tmp_path):
    client = SingleObjectS3()
    df = dataframe_with_outputs(
        client,
        [
            ("part-00010-run-c000.json", 7 * MIB),
            ("_SUCCESS", 0),
            ("part-00002-run-c000.json", 6 * MIB),
            ("part-00011-run-c000.json", MIB),
        ],
    )
    events = []
    result = send_dataframe_as_single_object(
        df,
        "s3://destination/final/run.jsonl",
        client=client,
        compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
        create_args={"ExpectedBucketOwner": "123456789012"},
        events=events.append,
    )

    assert result.kind == "dataframe-single-object"
    assert result.bytes_transferred == 14 * MIB
    assert result.metadata["payload_bytes_downloaded_by_client"] == 0
    assert result.metadata["global_row_order_guaranteed"] is False
    assert client.completed["IfNoneMatch"] == "*"
    assert [
        key.rsplit("/", 1)[-1]
        for key in [
            client.uploaded[1]["source_key"],
            client.uploaded[2]["source_key"],
            client.uploaded[3]["source_key"],
        ]
    ] == [
        "part-00002-run-c000.json",
        "part-00010-run-c000.json",
        "part-00011-run-c000.json",
    ]
    assert result.metadata["staging_cleanup"]["completed"] is True
    assert result.metadata["staging_cleanup"]["deleted_objects"] == 4
    assert not client.stage_objects
    assert client.list_requests[0]["ExpectedBucketOwner"] == "123456789012"
    assert all(
        upload["request"]["ExpectedSourceBucketOwner"] == "123456789012"
        for upload in client.uploaded.values()
    )
    assert ("option", "lineSep", "\n") in df.write.calls
    assert ("option", "encoding", "UTF-8") in df.write.calls
    assert events[0].phase == "dataframe_staging_started"
    assert events[0].detail == result.metadata["staging_prefix"]
    assert events[-1].phase == "single_object_completed"


def test_failed_compose_preserves_owned_staging_objects(tmp_path):
    client = SingleObjectS3()
    df = dataframe_with_outputs(
        client,
        [
            ("part-00000-run-c000.json", MIB),
            ("part-00001-run-c000.json", 6 * MIB),
        ],
    )
    with pytest.raises(ConfigurationError, match="undersized nonfinal"):
        send_dataframe_as_single_object(
            df,
            "s3://destination/final/run.jsonl",
            client=client,
            max_skew_retries=0,
            compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
        )
    assert len(client.stage_objects) == 2
    assert client.delete_requests == []
    assert client.destination is None


def test_skewed_stage_is_rewritten_with_measured_parallelism(tmp_path):
    client = SingleObjectS3()
    df = dataframe_with_output_attempts(
        client,
        [
            [
                ("part-00000-run-c000.json", MIB),
                ("part-00001-run-c000.json", 19 * MIB),
            ],
            [
                ("part-00000-run-c000.json", 10 * MIB),
                ("part-00001-run-c000.json", 10 * MIB),
            ],
        ],
    )
    events = []

    result = send_dataframe_as_single_object(
        df,
        "s3://destination/final/repaired.jsonl",
        target_file_size="8MiB",
        client=client,
        compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
        events=events.append,
    )

    assert df.repartitioned == 2
    assert result.bytes_transferred == 20 * MIB
    assert result.metadata["skew_repair"] == {
        "enabled": True,
        "max_retries": 2,
        "rewrites": 1,
        "repaired": True,
        "final_requested_partitions": 2,
    }
    assert len(result.metadata["staging_attempts"]) == 2
    assert result.metadata["staging_attempts"][0]["valid_for_compose"] is False
    assert result.metadata["staging_attempts"][1]["valid_for_compose"] is True
    assert result.metadata["staging_cleanup"]["completed"] is True
    assert result.metadata["staging_cleanup"]["deleted_objects"] == 4
    assert len(result.metadata["staging_cleanup"]["attempts"]) == 2
    assert not client.stage_objects
    assert "dataframe_staging_skew_detected" in [event.phase for event in events]
    assert "dataframe_staging_repartitioned" in [event.phase for event in events]


def test_exhausted_skew_repair_cleans_prior_attempt_and_retains_last(tmp_path):
    client = SingleObjectS3()
    skewed = [
        ("part-00000-run-c000.json", MIB),
        ("part-00001-run-c000.json", 19 * MIB),
    ]
    df = dataframe_with_output_attempts(client, [skewed, skewed])

    with pytest.raises(ConfigurationError, match="stopped after 1 rewrite"):
        send_dataframe_as_single_object(
            df,
            "s3://destination/final/exhausted.jsonl",
            target_file_size="8MiB",
            max_skew_retries=1,
            client=client,
            compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
        )

    assert len(client.delete_requests) == 1
    assert len(client.stage_objects) == 2
    assert all("attempt-01/" in key for _, key in client.stage_objects)
    assert client.destination is None


def test_small_export_can_use_one_partition_repair(tmp_path):
    client = SingleObjectS3()
    df = dataframe_with_output_attempts(
        client,
        [
            [
                ("part-00000-run-c000.json", MIB),
                ("part-00001-run-c000.json", 6 * MIB),
            ],
            [("part-00000-run-c000.json", 7 * MIB)],
        ],
    )

    result = send_dataframe_as_single_object(
        df,
        "s3://destination/final/small.jsonl",
        target_file_size="8MiB",
        client=client,
        compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
    )

    assert result.metadata["skew_repair"]["final_requested_partitions"] == 1
    assert result.metadata["skew_repair"]["rewrites"] == 1
    assert result.bytes_transferred == 7 * MIB


def test_skew_repair_cleanup_failures_are_reported_after_commit(tmp_path):
    client = CleanupDeniedS3()
    df = dataframe_with_output_attempts(
        client,
        [
            [
                ("part-00000-run-c000.json", MIB),
                ("part-00001-run-c000.json", 19 * MIB),
            ],
            [
                ("part-00000-run-c000.json", 10 * MIB),
                ("part-00001-run-c000.json", 10 * MIB),
            ],
        ],
    )

    result = send_dataframe_as_single_object(
        df,
        "s3://destination/final/cleanup-report.jsonl",
        target_file_size="8MiB",
        client=client,
        compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
    )

    cleanup = result.metadata["staging_cleanup"]
    assert cleanup["completed"] is False
    assert cleanup["deleted_objects"] == 0
    assert len(cleanup["attempts"]) == 2
    assert len(cleanup["errors"]) == 2
    assert client.destination["size"] == 20 * MIB
    assert len(client.stage_objects) == 4


def test_skew_repair_retains_every_attempt_when_cleanup_is_disabled(tmp_path):
    client = SingleObjectS3()
    df = dataframe_with_output_attempts(
        client,
        [
            [
                ("part-00000-run-c000.json", MIB),
                ("part-00001-run-c000.json", 19 * MIB),
            ],
            [
                ("part-00000-run-c000.json", 10 * MIB),
                ("part-00001-run-c000.json", 10 * MIB),
            ],
        ],
    )

    result = send_dataframe_as_single_object(
        df,
        "s3://destination/final/retained.jsonl",
        target_file_size="8MiB",
        cleanup_staging=False,
        client=client,
        compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
    )

    assert result.metadata["staging_cleanup"] == {
        "requested": False,
        "completed": False,
        "deleted_objects": 0,
        "attempts": [],
    }
    assert len(client.stage_objects) == 4
    assert client.delete_requests == []


def test_cleanup_can_be_disabled(tmp_path):
    client = SingleObjectS3()
    df = dataframe_with_outputs(client, [("part-00000-run-c000.json", 6 * MIB)])
    result = send_dataframe_as_single_object(
        df,
        "s3://destination/final/run.jsonl",
        client=client,
        cleanup_staging=False,
        compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
    )
    assert result.metadata["staging_cleanup"]["requested"] is False
    assert len(client.stage_objects) == 1


def test_cleanup_failure_is_reported_without_hiding_committed_destination(tmp_path):
    client = CleanupDeniedS3()
    df = dataframe_with_outputs(client, [("part-00000-run-c000.json", 6 * MIB)])
    result = send_dataframe_as_single_object(
        df,
        "s3://destination/final/run.jsonl",
        client=client,
        compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
    )
    cleanup = result.metadata["staging_cleanup"]
    assert cleanup["requested"] is True
    assert cleanup["completed"] is False
    assert "CleanupDeniedError" in cleanup["error"]
    assert client.destination["size"] == 6 * MIB
    assert len(client.stage_objects) == 1


def test_empty_dataframe_creates_verified_empty_object_and_cleans_stage(tmp_path):
    client = SingleObjectS3()
    df = dataframe_with_outputs(
        client,
        [("part-00000-run-c000.json", 0), ("_SUCCESS", 0)],
    )
    result = send_dataframe_as_single_object(
        df,
        "s3://destination/final/empty.jsonl",
        client=client,
        compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
    )
    assert result.bytes_transferred == 0
    assert client.put_args["Body"] == b""
    assert client.put_args["IfNoneMatch"] == "*"
    assert result.metadata["composition"] is None
    assert result.metadata["staging_cleanup"]["deleted_objects"] == 2
    assert not client.stage_objects


def test_existing_destination_fails_before_spark_write(tmp_path):
    client = SingleObjectS3()
    client.destination = {
        "identity": ("destination", "final/run.jsonl"),
        "size": 1,
        "metadata": {},
    }
    df = dataframe_with_outputs(client, [("part-00000-run-c000.json", 6 * MIB)])
    with pytest.raises(FileExistsError):
        send_dataframe_as_single_object(
            df,
            "s3://destination/final/run.jsonl",
            client=client,
            compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
        )
    assert not any(call[0] == "save" for call in df.write.calls)


def test_overwrite_mode_does_not_add_create_only_condition(tmp_path):
    client = SingleObjectS3()
    df = dataframe_with_outputs(client, [("part-00000-run-c000.json", 6 * MIB)])
    send_dataframe_as_single_object(
        df,
        "s3://destination/final/run.jsonl",
        mode="overwrite",
        client=client,
        compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
    )
    assert "IfNoneMatch" not in client.completed


def test_stage_discovery_paginates_then_sorts_numeric_partition_ids():
    client = PaginatedStageS3()
    sources, keys, size = _list_stage(
        client,
        "s3://stage/run/",
        {},
        1,
    )
    assert [uri.rsplit("/", 1)[-1] for uri in sources] == [
        "part-00002-run-c000.json",
        "part-00010-run-c000.json",
    ]
    assert len(keys) == 3
    assert size == 13 * MIB
    assert len(client.list_requests) == 2


def test_staging_cleanup_batches_at_s3_delete_limit():
    client = SingleObjectS3()
    prefix = "run/owned/"
    keys = [f"{prefix}{index:04d}" for index in range(1001)]
    for key in keys:
        client.stage_objects[("stage", key)] = 1
    result = _cleanup_stage(client, "s3://stage/run/owned/", keys, {}, 1)
    assert result["completed"] is True
    assert result["deleted_objects"] == 1001
    assert [len(request["Delete"]["Objects"]) for request in client.delete_requests] == [
        1000,
        1,
    ]


def test_staging_cleanup_refuses_keys_outside_owned_run():
    client = SingleObjectS3()
    with pytest.raises(ConfigurationError, match="outside Dude's staging run"):
        _cleanup_stage(
            client,
            "s3://stage/run/owned/",
            ["run/another-run/payload.json"],
            {},
            1,
        )
    assert client.delete_requests == []


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"file_format": "parquet"}, "only json or text"),
        ({"options": {"compression": "gzip"}}, "must be uncompressed"),
        ({"options": {"lineSep": "|"}}, "newline separator"),
        ({"options": {"maxRecordsPerFile": 100}}, "cannot use maxRecordsPerFile"),
        ({"target_file_size": "1MiB"}, "at least 5 MiB"),
        ({"max_skew_retries": -1}, "nonnegative integer"),
        ({"max_skew_retries": True}, "nonnegative integer"),
    ],
)
def test_unsafe_single_object_formats_and_options_fail_before_write(
    tmp_path, kwargs, message
):
    client = SingleObjectS3()
    df = dataframe_with_outputs(client, [("part-00000-run-c000.json", 6 * MIB)])
    with pytest.raises(ConfigurationError, match=message):
        send_dataframe_as_single_object(
            df,
            "s3://destination/final/run.jsonl",
            client=client,
            compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
            **kwargs,
        )
    assert not any(call[0] == "save" for call in df.write.calls)
