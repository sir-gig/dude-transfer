import pytest

from dude_transfer.utils import parse_size


@pytest.mark.parametrize(
    ("value", "expected"),
    [("64MiB", 64 * 1024**2), ("1.5 GB", 1_500_000_000), (1024, 1024)],
)
def test_parse_size(value, expected):
    assert parse_size(value) == expected


def test_parse_size_rejects_invalid_values():
    with pytest.raises(ValueError):
        parse_size("soon")
