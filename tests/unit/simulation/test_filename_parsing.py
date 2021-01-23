"""Test filename parsing for /data/sim files."""

# pylint: disable=W0621

import re
import sys
from typing import Dict, List, Optional, Pattern, TypedDict

import pytest

sys.path.append(".")
from indexer.metadata.simulation import (  # isort:skip # noqa # pylint: disable=C0413
    data_sim,
    filename_patterns,
)
from indexer.utils import utils  # isort:skip # noqa # pylint: disable=C0413


@pytest.fixture  # type: ignore
def sim_regexes() -> List[Pattern[str]]:
    """List of compiled regex patterns."""
    return [re.compile(r) for r in filename_patterns.regex_patterns]


def test_good(sim_regexes: List[Pattern[str]]) -> None:  # pylint: disable=C0103
    """Test sim filename parsing."""

    class _FilenameValues(TypedDict):
        proc_level: Optional[utils.ProcessingLevel]
        dataset: Optional[int]
        job: Optional[int]

    filenames_and_values: Dict[str, _FilenameValues] = {
        "TODO": {"proc_level": 0, "dataset": 0, "job": 0},  # TODO
    }

    # is_valid_filename()
    for fname in filenames_and_values.keys():
        print(fname)
        assert data_sim.DataSimI3FileMetadata.is_valid_filename(fname)

    # figure_processing_level()
    for fname, values in filenames_and_values.items():
        print(fname)
        proc_level = data_sim.DataSimI3FileMetadata.figure_processing_level(
            utils.FileInfo(fname)
        )
        assert proc_level == values["proc_level"]

    # parse_iceprod_dataset_job_ids()
    for fname, values in filenames_and_values.items():
        print(fname)
        dataset, job = data_sim.DataSimI3FileMetadata.parse_iceprod_dataset_job_ids(
            sim_regexes, utils.FileInfo(fname)
        )
        assert dataset == values["dataset"]
        assert job == values["job"]


def test_bad(sim_regexes: List[Pattern[str]]) -> None:  # pylint: disable=C0103
    """Test bad sim filename parsing."""
    filenames = ["TODO"]  # TODO

    for fname in filenames:
        print(fname)
        with pytest.raises(ValueError) as e:
            data_sim.DataSimI3FileMetadata.parse_iceprod_dataset_job_ids(
                sim_regexes, utils.FileInfo(fname)
            )
        assert "Filename does not match any pattern, " in str(e.value)
