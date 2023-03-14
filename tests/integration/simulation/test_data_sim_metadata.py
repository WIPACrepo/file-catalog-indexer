"""Test pipeline starting at MetadataManager.

1. manager = MetadataManager(...)
2. metadata_file = manager.new_file(filepath)
3. metadata = metadata_file.generate()
"""

import json
from datetime import date
from os import listdir, path
from unittest.mock import ANY, AsyncMock, Mock, PropertyMock, patch

from indexer import metadata_manager

import tests.integration.simulation.sim_data as data

SKIP_FIELDS = ["_links", "meta_modify_date", "uuid"]


@patch("rest_tools.client.RestClient.request_seq")
@patch("pymysql.connect")
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
    pymysql_connect: Mock,
    rest_client_request_seq: AsyncMock,
) -> None:
    """Test all example passing cases."""
    for fpath, metadata in data.EXAMPLES.items():
        print(fpath)

        # prep
        fullpath = path.join(path.dirname(path.realpath(__file__)), fpath)
        print(fullpath)
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
        # mock SQL queries & REST requests
        dir_ = path.dirname(fullpath)
        if any(f.startswith("ip1-") for f in listdir(dir_)):
            with open(path.join(dir_, "ip1-dataset-steering-params.json")) as f:
                sps = json.load(f)
            pymysql_connect.return_value.cursor.return_value.fetchall.return_value = sps
        elif any(f.startswith("ip2-") for f in listdir(dir_)):
            with open(path.join(dir_, "ip2-datasets.json")) as f:
                datasets = json.load(f)
            with open(path.join(dir_, "ip2-job-config.json")) as f:
                job_config = json.load(f)
            with open(path.join(dir_, "ip2-dataset-tasks.json")) as f:
                tasks = json.load(f)
            rest_client_request_seq.side_effect = [datasets, job_config, tasks]
        else:
            raise Exception("Missing testing data")

        # run
        manager = metadata_manager.MetadataManager(
            "WIPAC",
            iceprodv2_rc_token=ANY,
            iceprodv1_db_pass=ANY,
        )
        metadata_file = manager.new_file(fullpath)
        generated_metadata = metadata_file.generate()

        # assert
        for field in metadata:
            if field in SKIP_FIELDS:
                continue
            print(field)
            assert metadata[field] == generated_metadata[field]  # type: ignore[literal-required]
