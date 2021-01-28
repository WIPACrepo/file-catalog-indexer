"""Test pipeline starting at MetadataManager.

1. manager = MetadataManager(...)
2. metadata_file = manager.new_file(filepath)
3. metadata = metadata_file.generate()
"""
import sys

# local imports
import data

sys.path.append(".")
from indexer import metadata_manager  # isort:skip # noqa # pylint: disable=C0413


def test_1() -> None:
    """Test all example passing cases."""
    for fpath, metadata in data.EXAMPLES.items():
        manager = metadata_manager.MetadataManager("WIPAC")
        metadata_file = manager.new_file(fpath)
        assert metadata == metadata_file.generate()
