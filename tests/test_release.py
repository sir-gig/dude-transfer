import pytest

from scripts.check_release_version import check_tag, project_version


def test_project_version_matches_current_release_line():
    assert project_version() == "0.1.0"


def test_release_tag_must_match_package_version():
    check_tag("v0.1.0", "0.1.0")
    with pytest.raises(ValueError, match="does not match"):
        check_tag("v0.2.0", "0.1.0")
