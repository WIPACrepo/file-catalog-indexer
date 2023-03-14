"""Test parsing for /data/sim Iceprod steering-parameter dicts."""

# pylint: disable=W0621


import logging
from typing import Any, Dict
from unittest.mock import ANY

from _pytest.logging import LogCaptureFixture
from indexer.metadata.simulation import data_sim

import tests.unit.simulation.steering_param_data as data


def test_good() -> None:
    """Test get_simulation_metadata()."""
    for example in data.EXMAPLES:
        print(example)
        meta = data_sim.DataSimI3FileMetadata.get_simulation_metadata(
            example["steering_parameters"], ANY
        )
        assert meta == example["simulation_metadata"]


def test_bad(caplog: LogCaptureFixture) -> None:
    """Failure-test get_simulation_metadata()."""
    examples: Dict[str, Dict[str, Any]] = {
        "oversize": {
            "value": "seventy-seven",
            "type": int,
            "metadata_key": "DOMoversize",
        },
        "OVERSIZE": {
            "value": ["seventy-seven", "list"],
            "type": int,
            "metadata_key": "DOMoversize",
        },
    }

    for i, (key, dict_) in enumerate(examples.items()):
        caplog.set_level(logging.DEBUG)
        sim_meta = data_sim.DataSimI3FileMetadata.get_simulation_metadata(
            {key: dict_["value"]}, i
        )
        log_msg = f"Wrong data type stored for \"simulation\" key, ({dict_['metadata_key']}:{dict_['value']}) should be {dict_['type']} (dataset:{i})"
        # failed type-cast should be logged
        assert log_msg in caplog.text
        # but the value should be kept anyways
        assert sim_meta == {dict_["metadata_key"]: dict_["value"]}
