import pytest
from test_compose import FakeComposeS3, source
from test_s3 import FakeS3, make_sparse_file
from test_single_object import SingleObjectS3, dataframe_with_outputs

from dude_transfer import send_dataframe_as_single_object
from dude_transfer.compose import ComposeConfig, compose_s3_objects
from dude_transfer.s3 import MultipartConfig, send_file
from dude_transfer.utils import MIB

botocore_validate = pytest.importorskip("botocore.validate")
botocore_session = pytest.importorskip("botocore.session")


def _validate(operation, parameters):
    model = botocore_session.get_session().get_service_model("s3")
    shape = model.operation_model(operation).input_shape
    botocore_validate.validate_parameters(parameters, shape)


def test_generated_multipart_requests_match_botocore_models(tmp_path):
    source = make_sparse_file(tmp_path / "data.bin", 6 * 1024**2)
    client = FakeS3()
    send_file(
        source,
        "s3://bucket/key",
        client=client,
        config=MultipartConfig(
            multipart_threshold=1,
            part_size="5MiB",
            if_none_match=True,
        ),
        create_args={
            "ExpectedBucketOwner": "123456789012",
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": "alias/test",
        },
    )

    _validate("CreateMultipartUpload", client.create_args)
    for parameters in client.uploaded.values():
        _validate("UploadPart", parameters)
    completion = dict(client.completed)
    _validate("CompleteMultipartUpload", completion)


def test_generated_multipart_copy_requests_match_botocore_models(tmp_path):
    client = FakeComposeS3(
        {
            ("source", "a"): source(6 * MIB, '"a"', "version-a"),
            ("source", "b"): source(MIB, '"b"'),
        }
    )
    compose_s3_objects(
        ["s3://source/a", "s3://source/b"],
        "s3://destination/result",
        client=client,
        config=ComposeConfig(
            checkpoint_path=tmp_path / "compose.json",
            expected_source_bucket_owner="123456789012",
            request_payer="requester",
        ),
        create_args={
            "ExpectedBucketOwner": "123456789012",
            "RequestPayer": "requester",
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": "alias/test",
        },
    )

    _validate("CreateMultipartUpload", client.create_args)
    for request in client.head_requests:
        _validate("HeadObject", request)
    for upload in client.uploaded.values():
        _validate("UploadPartCopy", upload["request"])
    _validate("CompleteMultipartUpload", dict(client.completed))


def test_single_object_stage_discovery_and_cleanup_requests_match_models(tmp_path):
    client = SingleObjectS3()
    df = dataframe_with_outputs(
        client,
        [("part-00000-run-c000.json", 6 * MIB), ("_SUCCESS", 0)],
    )
    send_dataframe_as_single_object(
        df,
        "s3://destination/final/run.jsonl",
        client=client,
        compose_config=ComposeConfig(
            checkpoint_path=tmp_path / "compose.json",
            request_payer="requester",
        ),
        create_args={
            "ExpectedBucketOwner": "123456789012",
            "RequestPayer": "requester",
        },
    )
    for request in client.list_requests:
        _validate("ListObjectsV2", request)
    for request in client.delete_requests:
        _validate("DeleteObjects", request)


def test_empty_single_object_put_request_matches_botocore_model(tmp_path):
    client = SingleObjectS3()
    df = dataframe_with_outputs(
        client,
        [("part-00000-run-c000.json", 0), ("_SUCCESS", 0)],
    )
    send_dataframe_as_single_object(
        df,
        "s3://destination/final/empty.jsonl",
        client=client,
        compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
    )
    _validate("PutObject", client.put_args)
