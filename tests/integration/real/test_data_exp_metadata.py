"""Test pipeline starting at MetadataManager.

1. manager = MetadataManager(...)
2. metadata_file = manager.new_file(filepath)
3. metadata = metadata_file.generate()
"""
import sys
from unittest.mock import Mock, patch

# local imports
import data

sys.path.append(".")
from indexer import metadata_manager  # isort:skip # noqa # pylint: disable=C0413


@patch("indexer.metadata_manager.MetadataManager._is_data_sim_filepath")
@patch("indexer.metadata_manager.MetadataManager._is_data_exp_filepath")
def test_1(_is_data_exp_filepath: Mock, _is_data_sim_filepath: Mock) -> None:
    """Test all example passing cases."""
    for fpath, metadata in data.EXAMPLES.items():
        # mock MetadataManager.new_file's initial factory logic
        _is_data_sim_filepath.return_value = False
        _is_data_exp_filepath.return_value = True

        # run
        manager = metadata_manager.MetadataManager("WIPAC")
        metadata_file = manager.new_file(fpath)

        # assert
        assert metadata == metadata_file.generate()
