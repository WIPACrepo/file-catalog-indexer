"""Test pipeline starting at MetadataManager.

1. manager = MetadataManager(...)
2. metadata_file = manager.new_file(filepath)
3. metadata = metadata_file.generate()
"""

import sys
from datetime import date
from os import path
from unittest.mock import Mock, patch, PropertyMock

# local imports
import data

sys.path.append(".")
from indexer import metadata_manager  # isort:skip # noqa # pylint: disable=C0413


SKIP_FIELDS = ["_links", "meta_modify_date", "uuid"]


@patch(
    "indexer.metadata.simulation.iceprod_tools._IceProdV2Querier.filepath",
    new_callable=PropertyMock,
)
@patch("indexer.metadata.i3.I3FileMetadata._get_events_data")
@patch("indexer.metadata_manager.MetadataManager._is_data_sim_filepath")
@patch("indexer.metadata_manager.MetadataManager._is_data_exp_filepath")
def test_1(
    _is_data_exp_filepath: Mock,
    _is_data_sim_filepath: Mock,
    _get_events_data: Mock,
    _iceprodv2querier_filepath: PropertyMock,
) -> None:
    """Test all example passing cases."""
    for fpath, metadata in data.EXAMPLES.items():
        print(fpath)

        # prep
        fullpath = path.join(path.dirname(path.realpath(__file__)), fpath)
        orignal_path = metadata["logical_name"]
        metadata.update(
            {
                "logical_name": fullpath,
                "locations": [
                    {"site": metadata["locations"][0]["site"], "path": fullpath}
                ],
                "create_date": date.fromtimestamp(path.getctime(fullpath)).isoformat(),
            }
        )

        # mock MetadataManager.new_file's initial factory logic
        _is_data_sim_filepath.return_value = True
        _is_data_exp_filepath.return_value = False
        # mock I3Reader-dependent method
        dummy_event_data = {"status": metadata["content_status"]}
        _get_events_data.return_value = dummy_event_data
        # mock iceprod_tool's filepath so output-file matching can work
        _iceprodv2querier_filepath.return_value = orignal_path

        # run
        manager = metadata_manager.MetadataManager(  # TODO mock the rest/db
            "WIPAC",
            iceprodv2_rc_token=open("ip2.token").read().strip(),
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
