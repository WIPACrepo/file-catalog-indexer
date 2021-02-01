"""Test pipeline starting at MetadataManager.

1. manager = MetadataManager(...)
2. metadata_file = manager.new_file(filepath)
3. metadata = metadata_file.generate()
"""

import os
import sys
from unittest.mock import Mock, patch

# local imports
import data

sys.path.append(".")
from indexer import metadata_manager  # isort:skip # noqa # pylint: disable=C0413


SKIP_FIELDS = ["_links", "meta_modify_date", "uuid"]


@patch("indexer.metadata_manager.MetadataManager._is_data_sim_filepath")
@patch("indexer.metadata_manager.MetadataManager._is_data_exp_filepath")
@patch("indexer.metadata.i3.I3FileMetadata._get_events_data")
def test_1(
    _is_data_exp_filepath: Mock, _is_data_sim_filepath: Mock, _get_events_data: Mock
) -> None:
    """Test all example passing cases."""
    for fpath, metadata in data.EXAMPLES.items():
        print(fpath)

        # prep
        fullpath = os.path.join(os.path.dirname(os.path.realpath(__file__)), fpath)
        metadata["logical_name"] = fullpath
        metadata["locations"] = [
            {"site": metadata["locations"][0]["site"], "path": fullpath}
        ]

        # mock MetadataManager.new_file's initial factory logic
        _is_data_sim_filepath.return_value = True
        _is_data_exp_filepath.return_value = False
        # mock I3Reader-dependent method
        dummy_event_data = {
            "first_event": metadata["run"]["first_event"],
            "last_event": metadata["run"]["last_event"],
            "event_count": metadata["run"]["event_count"],
            "status": metadata["content_status"],
        }
        _get_events_data.return_value = dummy_event_data

        # run
        manager = metadata_manager.MetadataManager(  # TODO think about mocking the rest/db
            "WIPAC",
            iceprodv2_rc_token=open("fc.token").read().strip(),
            iceprodv1_db_pass=open("ipdb.pass").read().strip(),
        )
        metadata_file = manager.new_file(fullpath)
        generated_metadata = metadata_file.generate()

        # assert
        for field in metadata:
            if field in SKIP_FIELDS:
                continue
            print(field)
            assert metadata[field] == generated_metadata[field]  # type: ignore[misc]
