from test_single_object import SingleObjectS3, dataframe_with_outputs

from dude_transfer import send
from dude_transfer.compose import ComposeConfig
from dude_transfer.utils import MIB


def test_automatic_api_dispatches_dataframe_single_object(tmp_path):
    client = SingleObjectS3()
    df = dataframe_with_outputs(client, [("part-00000-run-c000.json", 6 * MIB)])
    result = send(
        df,
        "s3://destination/final/run.jsonl",
        single_object=True,
        client=client,
        compose_config=ComposeConfig(checkpoint_path=tmp_path / "compose.json"),
    )
    assert result.kind == "dataframe-single-object"
