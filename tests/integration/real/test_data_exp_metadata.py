"""Test pipeline starting at MetadataManager.

1. manager = MetadataManager(...)
2. metadata_file = manager.new_file(filepath)
3. metadata = metadata_file.generate()
"""

from argparse import Namespace
import os
from datetime import datetime as dt
from unittest.mock import Mock, patch

from indexer import metadata_manager

import tests.integration.real.exp_data as data

SKIP_FIELDS = ["_links", "meta_modify_date", "uuid"]


@patch("indexer.metadata.real.l2.L2FileMetadata._i3time_to_datetime")
@patch("indexer.metadata.i3.I3FileMetadata._get_events_data")
@patch("indexer.metadata_manager.MetadataManager._is_data_sim_filepath")
@patch("indexer.metadata_manager.MetadataManager._is_data_exp_filepath")
def test_1(
    _is_data_exp_filepath: Mock,
    _is_data_sim_filepath: Mock,
    _get_events_data: Mock,
    _i3time_to_datetime: Mock,
) -> None:
    """Test all example passing cases."""
    for fpath, metadata in data.EXAMPLES.items():
        print(fpath)

        # prep
        fullpath = os.path.join(os.path.dirname(os.path.realpath(__file__)), fpath)
        metadata.update(
            {
                "logical_name": fullpath,
                "locations": [
                    {"site": metadata["locations"][0]["site"], "path": fullpath}
                ],
            }
        )

        # mock MetadataManager.new_file's initial factory logic
        _is_data_sim_filepath.return_value = False
        _is_data_exp_filepath.return_value = True
        # mock I3Reader-dependent method
        dummy_event_data = {
            "first_event": metadata["run"]["first_event"],
            "last_event": metadata["run"]["last_event"],
            "event_count": metadata["run"]["event_count"],
            "status": metadata["content_status"],
        }
        _get_events_data.return_value = dummy_event_data

        # extra L2 stuff
        if metadata["processing_level"] == "L2":
            gaps_dict = metadata["offline_processing_metadata"]["gaps"][0]
            _i3time_to_datetime.side_effect = [
                dt.fromisoformat(gaps_dict["start_date"]),
                dt.fromisoformat(gaps_dict["stop_date"]),
            ]
            metadata["offline_processing_metadata"]["L2_gcd_file"] = os.path.join(
                os.path.dirname(fullpath),
                metadata["offline_processing_metadata"]["L2_gcd_file"].split("/")[-1],
            )

        # run
        args = Namespace()
        args.basic_only = False
        args.iceprod_rest_url = ""
        args.iceprodv1_db_pass = "hunter2"
        args.oauth_client_id = "file-catalog-indexer"
        args.oauth_client_secret = "hunter2"
        args.oauth_url = ""
        args.rest_timeout = 60
        args.rest_retries = 10
        args.site = "WIPAC"
        manager = metadata_manager.MetadataManager(args)
        metadata_file = manager.new_file(fullpath)
        generated_metadata = metadata_file.generate()

        # assert
        for field in metadata:
            if field in SKIP_FIELDS:
                continue
            print(field)
            assert metadata[field] == generated_metadata[field]  # type: ignore[literal-required]
