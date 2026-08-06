from __future__ import annotations

import json
from urllib.parse import parse_qs, unquote, urlsplit

import pytest

from dude_transfer import CheckpointError, ConfigurationError, UploadVerificationError
from dude_transfer.compose import ComposeConfig, compose_s3_objects, plan_compose
from dude_transfer.s3 import S3_MAX_PART_SIZE, S3_MAX_PARTS
from dude_transfer.utils import GIB, MIB


class FakeComposeS3:
    def __init__(self, sources):
        self.sources = sources
        self.head_requests = []
        self.uploaded = {}
        self.create_args = None
        self.completed = None
        self.aborted = None
        self.destination = None

    def head_object(self, **kwargs):
        self.head_requests.append(kwargs)
        identity = (kwargs["Bucket"], kwargs["Key"])
        if self.destination and identity == self.destination["identity"]:
            return {
                "ContentLength": self.destination["size"],
                "Metadata": self.destination["metadata"],
            }
        source = self.sources[identity]
        response = {"ContentLength": source["size"], "ETag": source["etag"]}
        if source.get("version_id"):
            response["VersionId"] = source["version_id"]
        return response

    def create_multipart_upload(self, **kwargs):
        self.create_args = kwargs
        return {"UploadId": "compose-1"}

    def list_parts(self, **kwargs):
        parts = []
        for number, upload in sorted(self.uploaded.items()):
            copied = upload["response"]["CopyPartResult"]
            parts.append(
                {
                    "PartNumber": number,
                    "ETag": copied["ETag"],
                    "Size": upload["size"],
                    "ChecksumCRC32": copied["ChecksumCRC32"],
                }
            )
        return {"Parts": parts, "IsTruncated": False}

    def upload_part_copy(self, **kwargs):
        parsed = urlsplit(kwargs["CopySource"])
        bucket, encoded_key = parsed.path.split("/", 1)
        key = unquote(encoded_key)
        source_state = self.sources[(bucket, key)]
        assert kwargs["CopySourceIfMatch"] == source_state["etag"]
        version_id = parse_qs(parsed.query).get("versionId", [None])[0]
        if version_id:
            assert version_id == source_state["version_id"]
        if "CopySourceRange" in kwargs:
            first, last = kwargs["CopySourceRange"].removeprefix("bytes=").split("-")
            size = int(last) - int(first) + 1
        else:
            size = source_state["size"]
        number = kwargs["PartNumber"]
        response = {
            "CopyPartResult": {
                "ETag": f'"copy-{number}"',
                "ChecksumCRC32": f"checksum-{number}",
            }
        }
        self.uploaded[number] = {
            "request": kwargs,
            "response": response,
            "size": size,
            "source_key": key,
        }
        return response

    def complete_multipart_upload(self, **kwargs):
        self.completed = kwargs
        self.destination = {
            "identity": (kwargs["Bucket"], kwargs["Key"]),
            "size": sum(item["size"] for item in self.uploaded.values()),
            "metadata": self.create_args["Metadata"],
        }
        return {"ETag": '"composed"'}

    def abort_multipart_upload(self, **kwargs):
        self.aborted = kwargs


class FailingOnceComposeS3(FakeComposeS3):
    def __init__(self, sources):
        super().__init__(sources)
        self.should_fail = True

    def upload_part_copy(self, **kwargs):
        if kwargs["PartNumber"] == 2 and self.should_fail:
            self.should_fail = False
            raise RuntimeError("temporary copy outage")
        return super().upload_part_copy(**kwargs)


class CompletionLostComposeS3(FakeComposeS3):
    def complete_multipart_upload(self, **kwargs):
        super().complete_multipart_upload(**kwargs)
        raise RuntimeError("completion response lost")


class MissingChecksumComposeS3(FakeComposeS3):
    def upload_part_copy(self, **kwargs):
        response = super().upload_part_copy(**kwargs)
        response["CopyPartResult"].pop("ChecksumCRC32")
        return response


class PreconditionFailedError(Exception):
    response = {
        "Error": {"Code": "PreconditionFailed"},
        "ResponseMetadata": {"HTTPStatusCode": 412},
    }


class ChangedSourceComposeS3(FakeComposeS3):
    def __init__(self, sources):
        super().__init__(sources)
        self.copy_attempts = 0

    def upload_part_copy(self, **kwargs):
        self.copy_attempts += 1
        raise PreconditionFailedError("source changed")


def source(size, etag, version_id=None):
    return {"size": size, "etag": etag, "version_id": version_id}


def test_plan_preserves_source_order_and_balances_large_ranges():
    client = FakeComposeS3(
        {
            ("source", "first"): source(6 * GIB, '"first"', "v1"),
            ("source", "last"): source(1 * MIB, '"last"'),
        }
    )
    plan = plan_compose(
        ["s3://source/first", "s3://source/last"],
        "s3://destination/result",
        client=client,
        config=ComposeConfig(copy_part_size=5 * GIB),
    )
    assert [item.uri for item in plan.sources] == [
        "s3://source/first",
        "s3://source/last",
    ]
    assert plan.sources[0].part_count == 2
    assert plan.sources[1].part_count == 1
    assert plan.part_count == 3
    assert plan.size_bytes == 6 * GIB + MIB


def test_small_nonfinal_source_is_rejected():
    client = FakeComposeS3(
        {
            ("source", "tiny"): source(MIB, '"tiny"'),
            ("source", "large"): source(6 * MIB, '"large"'),
        }
    )
    with pytest.raises(ConfigurationError, match="except the final"):
        plan_compose(
            ["s3://source/tiny", "s3://source/large"],
            "s3://destination/result",
            client=client,
        )


def test_empty_manifest_and_zero_length_source_are_rejected():
    with pytest.raises(ConfigurationError, match="at least one"):
        plan_compose([], "s3://destination/result", client=FakeComposeS3({}))

    client = FakeComposeS3({("source", "empty"): source(0, '"empty"')})
    with pytest.raises(ConfigurationError, match="zero-length"):
        plan_compose(
            ["s3://source/empty"],
            "s3://destination/result",
            client=client,
        )


def test_plan_rejects_more_than_s3_maximum_copy_parts():
    impossible_size = S3_MAX_PART_SIZE * S3_MAX_PARTS + 1
    client = FakeComposeS3(
        {("source", "too-large"): source(impossible_size, '"too-large"')}
    )
    with pytest.raises(ConfigurationError, match="at most 10000"):
        plan_compose(
            ["s3://source/too-large"],
            "s3://destination/result",
            client=client,
            config=ComposeConfig(copy_part_size=S3_MAX_PART_SIZE),
        )


def test_fixed_concurrency_starts_at_the_configured_ceiling():
    client = FakeComposeS3(
        {
            ("source", "a"): source(6 * MIB, '"a"'),
            ("source", "b"): source(6 * MIB, '"b"'),
            ("source", "c"): source(6 * MIB, '"c"'),
        }
    )
    plan = plan_compose(
        ["s3://source/a", "s3://source/b", "s3://source/c"],
        "s3://destination/result",
        client=client,
        config=ComposeConfig(
            adaptive=False,
            initial_concurrency=1,
            max_concurrency=3,
        ),
    )
    assert plan.adaptive is False
    assert plan.initial_concurrency == 3
    assert plan.concurrency == 3


def test_compose_uses_server_side_copy_in_order_and_preserves_sources(tmp_path):
    sources = {
        ("source", "a"): source(6 * MIB, '"a"', "v1"),
        ("source", "b"): source(7 * MIB, '"b"'),
        ("source", "c"): source(MIB, '"c"'),
    }
    client = FakeComposeS3(sources)
    events = []
    result = compose_s3_objects(
        ["s3://source/a", "s3://source/b", "s3://source/c"],
        "s3://destination/result",
        client=client,
        config=ComposeConfig(
            checkpoint_path=tmp_path / "compose.json",
            verify_after_compose=True,
            expected_source_bucket_owner="123456789012",
        ),
        create_args={"ExpectedBucketOwner": "123456789012"},
        events=events.append,
    )
    assert result.kind == "compose"
    assert result.metadata["payload_bytes_downloaded_by_client"] == 0
    assert result.metadata["sources_preserved"] is True
    assert result.metadata["verified"] is True
    ordered = client.completed["MultipartUpload"]["Parts"]
    assert [part["PartNumber"] for part in ordered] == [1, 2, 3]
    assert client.uploaded[1]["source_key"] == "a"
    assert client.uploaded[2]["source_key"] == "b"
    assert client.uploaded[3]["source_key"] == "c"
    assert all(
        item["request"]["ExpectedSourceBucketOwner"] == "123456789012"
        for item in client.uploaded.values()
    )
    assert events[0].phase == "planned"
    assert events[-1].phase == "completed"


def test_compose_resumes_completed_server_side_parts(tmp_path):
    sources = {
        ("source", "a"): source(6 * MIB, '"a"'),
        ("source", "b"): source(6 * MIB, '"b"'),
        ("source", "c"): source(6 * MIB, '"c"'),
    }
    client = FailingOnceComposeS3(sources)
    config = ComposeConfig(
        initial_concurrency=2,
        max_concurrency=3,
        max_attempts=1,
        checkpoint_path=tmp_path / "compose.json",
    )
    uris = ["s3://source/a", "s3://source/b", "s3://source/c"]
    with pytest.raises(RuntimeError, match="temporary copy outage"):
        compose_s3_objects(uris, "s3://destination/result", client=client, config=config)

    result = compose_s3_objects(
        uris,
        "s3://destination/result",
        client=client,
        config=config,
    )
    assert result.resumed is True
    assert 1 <= result.metadata["reused_parts"] <= 2
    assert [part["PartNumber"] for part in client.completed["MultipartUpload"]["Parts"]] == [
        1,
        2,
        3,
    ]


def test_compose_recovers_lost_completion_response(tmp_path):
    sources = {("source", "a"): source(6 * MIB, '"a"')}
    client = CompletionLostComposeS3(sources)
    result = compose_s3_objects(
        ["s3://source/a"],
        "s3://destination/result",
        client=client,
        config=ComposeConfig(
            max_attempts=1,
            checkpoint_path=tmp_path / "compose.json",
            verify_after_compose=True,
        ),
    )
    assert result.metadata["completion_recovered"] is True


def test_missing_copy_checksum_aborts_nonresumable_upload(tmp_path):
    sources = {("source", "a"): source(6 * MIB, '"a"')}
    client = MissingChecksumComposeS3(sources)
    checkpoint = tmp_path / "compose.json"
    with pytest.raises(UploadVerificationError, match="no CRC32 checksum"):
        compose_s3_objects(
            ["s3://source/a"],
            "s3://destination/result",
            client=client,
            config=ComposeConfig(
                resume=False,
                checkpoint_path=checkpoint,
            ),
        )
    assert client.aborted["UploadId"] == "compose-1"
    assert not checkpoint.exists()


def test_changed_source_precondition_fails_without_retry_and_preserves_resume(tmp_path):
    sources = {("source", "a"): source(6 * MIB, '"a"')}
    client = ChangedSourceComposeS3(sources)
    checkpoint = tmp_path / "compose.json"
    with pytest.raises(PreconditionFailedError):
        compose_s3_objects(
            ["s3://source/a"],
            "s3://destination/result",
            client=client,
            config=ComposeConfig(
                max_attempts=4,
                checkpoint_path=checkpoint,
            ),
        )
    assert client.copy_attempts == 1
    assert client.aborted is None
    assert checkpoint.exists()


def test_s3_uri_query_is_rejected():
    client = FakeComposeS3({("source", "a"): source(6 * MIB, '"a"')})
    with pytest.raises(ConfigurationError, match="queries"):
        plan_compose(
            ["s3://source/a?versionId=not-supported-here"],
            "s3://destination/result",
            client=client,
        )


def test_copy_source_header_encodes_key_and_version(tmp_path):
    key = "folder/a b+%.bin"
    client = FakeComposeS3(
        {("source", key): source(6 * MIB, '"etag"', "version 1/+")}
    )
    compose_s3_objects(
        ["s3://source/folder/a%20b%2B%25.bin"],
        "s3://destination/result",
        client=client,
        config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
    )
    assert client.uploaded[1]["request"]["CopySource"] == (
        "source/folder/a%20b%2B%25.bin?versionId=version%201%2F%2B"
    )


def test_corrupt_matching_checkpoint_fails_closed(tmp_path):
    sources = {
        ("source", "a"): source(6 * MIB, '"a"'),
        ("source", "b"): source(6 * MIB, '"b"'),
    }
    client = FailingOnceComposeS3(sources)
    checkpoint = tmp_path / "compose.json"
    config = ComposeConfig(max_attempts=1, checkpoint_path=checkpoint)
    uri = ["s3://source/a", "s3://source/b"]
    with pytest.raises(RuntimeError):
        compose_s3_objects(uri, "s3://destination/result", client=client, config=config)

    state = json.loads(checkpoint.read_text(encoding="utf-8"))
    state.pop("transfer_id")
    checkpoint.write_text(json.dumps(state), encoding="utf-8")
    with pytest.raises(CheckpointError, match="missing its upload identity"):
        compose_s3_objects(uri, "s3://destination/result", client=client, config=config)
