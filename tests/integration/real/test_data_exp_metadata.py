"""Test pipeline starting at MetadataManager.

1. manager = MetadataManager(...)
2. metadata_file = manager.new_file(filepath)
3. metadata = metadata_file.generate()
"""

from datetime import datetime as dt
import os
from unittest.mock import Mock, patch

from indexer import defaults
from indexer.config import IndexerConfiguration, OAuthConfiguration, RestConfiguration
from indexer.metadata_manager import MetadataManager

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
        index_config: IndexerConfiguration = {
            "basic_only": False,
            "denylist": defaults.DENYLIST,
            "denylist_file": defaults.DENYLIST_FILE,
            "dryrun": False,
            "iceprodv1_db_pass": "hunter2",
            "n_processes": defaults.N_PROCESSES,
            "non_recursive": False,
            "patch": False,
            "paths": defaults.PATHS,
            "paths_file": defaults.PATHS_FILE,
            "site": "WIPAC",
        }
        oauth_config: OAuthConfiguration = {
            "oauth_client_id": "file-catalog-indexer",
            "oauth_client_secret": "hunter2",
            "oauth_url": "",
        }
        rest_config: RestConfiguration = {
            "file_catalog_rest_url": "",
            "iceprod_rest_url": "",
            "rest_timeout": 60,
            "rest_retries": 10,
        }
        manager = MetadataManager(index_config, oauth_config, rest_config)
        metadata_file = manager.new_file(fullpath)
        generated_metadata = metadata_file.generate()

        # assert
        for field in metadata:
            if field in SKIP_FIELDS:
                continue
            print(field)
            assert metadata[field] == generated_metadata[field]  # type: ignore[literal-required]
