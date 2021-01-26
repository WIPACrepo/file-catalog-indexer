"""Test filename parsing for /data/sim files."""

# pylint: disable=W0621

import re
import sys
from typing import List, Pattern

import pytest

# local imports
import data

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
    # is_valid_filename()
    for fpath, values in data.EXAMPLES.items():
        print(fpath)
        assert data_sim.DataSimI3FileMetadata.is_valid_filename(values["fileinfo"].name)

    # figure_processing_level()
    for fpath, values in data.EXAMPLES.items():
        print(fpath)
        proc_level = data_sim.DataSimI3FileMetadata.figure_processing_level(
            values["fileinfo"]
        )
        assert proc_level == values["proc_level"]

    # parse_iceprod_dataset_job_ids()
    for fpath, values in data.EXAMPLES.items():
        print(fpath)
        dataset, job = data_sim.DataSimI3FileMetadata.parse_iceprod_dataset_job_ids(
            sim_regexes, values["fileinfo"]
        )
        assert dataset == values["dataset"]
        assert job == values["job"]


def test_invalid() -> None:  # pylint: disable=C0103
    """Test invalid sim filenames."""
    # Ex: /data/sim/IceCube/2012/generated/CORSIKA-in-ice/12359/IC86_2015/basic_filters/Run126291/Level2_IC86.2015_data_Run00126291_Subrun00000000.i3.bz2
    # Ex: /data/sim/IceCube/2013/generated/CORSIKA-in-ice/photo-electrons/briedel/muongun/mcpes/gamma_2_all/IC86_Merged_Muons_Emin_0.500000_TeV_Emax_10.000000_PeV_Gamma_2.000000_RunNumber_3881_Seed_107942_L1_L2_IC2011.i3.bz2"
    filenames = [
        "Level2_IC86.2013_data_Run555_Subrun666.i3",
        "IC86_Merged_Muons_Emin_0.500000_TeV_Emax_10.000000_PeV_Gamma_2.000000_RunNumber_3881_Seed_107942_L1_L2_IC2011.i3.bz2",
    ]  # TODO

    for fname in filenames:
        print(fname)
        assert not data_sim.DataSimI3FileMetadata.is_valid_filename(fname)


def test_bad(sim_regexes: List[Pattern[str]]) -> None:  # pylint: disable=C0103
    """Test bad sim filename parsing."""
    filenames = [""]  # TODO

    for fname in filenames:
        print(fname)
        with pytest.raises(ValueError) as e:
            data_sim.DataSimI3FileMetadata.parse_iceprod_dataset_job_ids(
                sim_regexes, utils.FileInfo(fname)
            )
        assert "Filename does not match any pattern, " in str(e.value)
