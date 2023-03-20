"""Test indexer filename parsing."""

import pathlib

import pytest

from indexer.index import ACCEPTED_ROOTS, path_in_denylist, validate_path
from indexer.utils.file_utils import sorted_unique_filepaths


def test_accepted_roots() -> None:
    """Test contents of ACCEPTED_ROOTS."""
    assert "/data" in ACCEPTED_ROOTS


def test_check_path() -> None:
    """Test filepath white-listing."""
    validate_path("/data/foo")
    validate_path("/data/foo/bar")
    validate_path("/data/")
    validate_path("/data")

    with pytest.raises(Exception):
        validate_path("foo")
    with pytest.raises(Exception):
        validate_path("/data2")
    with pytest.raises(Exception):
        validate_path("~/data")
    with pytest.raises(Exception):
        validate_path("data/")


def test_denylist() -> None:
    """Test filepath deny-listing."""
    denylist = ["/foo/bar", "/foo/baz"]

    assert path_in_denylist("/foo/bar", denylist)
    assert path_in_denylist("/foo/baz", denylist)
    assert path_in_denylist("/foo/baz/foobar", denylist)

    assert not path_in_denylist("/foo/baz2", denylist)
    assert not path_in_denylist("/foo/baz2/foobar", denylist)
    assert not path_in_denylist("/foo", denylist)


def test_sorted_unique_filepaths() -> None:
    """Test sorting, removing duplicates, and detecting illegal characters."""
    filepaths = ["foo/bar/baz.txt", "foo/bar/baz.txt", "baz/FOO.txt"]

    this_dir = pathlib.Path(__file__).parent.absolute()
    result = sorted_unique_filepaths(
        file_of_filepaths=f"{this_dir}/illegal_filepaths", list_of_filepaths=filepaths
    )
    expected = ["/bar/baz.py", "baz/FOO.txt", "foo/bar/baz.txt"]
    assert result == expected
