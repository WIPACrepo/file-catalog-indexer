"""Test filename parsing for /data/sim files."""

# pylint: disable=W0621

import re
from typing import List, Pattern

import pytest
from indexer.metadata.simulation import data_sim, filename_patterns
from indexer.utils import utils

import tests.unit.simulation.filepath_data as data


@pytest.fixture
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
        "this.is.a.file",
        "not.really.i3.log",
        "",
    ]

    for fname in filenames:
        print(fname)
        assert not data_sim.DataSimI3FileMetadata.is_valid_filename(fname)


def test_bad(sim_regexes: List[Pattern[str]]) -> None:  # pylint: disable=C0103
    """Test bad sim filename parsing."""
    filepaths = [
        # Illegal variations on "/test/Level2_IC86.2011_corsika.010285.000000.i3.bz2"...
        "/test/Level2_IC86.2011_shmorsika.010285.000000.i3.bz2",
        "/test/Level9_IC86.2011_corsika.010285.000000.i3.bz2",
        "/test/Level1_IC86.2011_corsika.010285.000000.i3.bz2"
        "/test/Level3_IC86.2011_corsika.010285.000000.i3.bz2"
        "/test/Level0_IC86.2011_corsika.010285.000000.i3.bz2",
        "/test/Level2_IC86.20110_corsika.010285.000000.i3.bz2",
        "/test/Level2_IC86.2011_corsika.010285.000000.002.002.i3.bz2",
        "/test/Level2_IC86.2011_corsika.010285.000000.i4.bz2",
        "/test/Level2_IC86.2011_corsika.010285.000000",
        # misc...
        "",
        "/",
        "green.eggs.ham",
    ]  # NOTE: This could be more extensive. Essentially, filename patterns are very strict

    for fpath in filepaths:
        print(fpath)
        with pytest.raises(ValueError) as e:
            data_sim.DataSimI3FileMetadata.parse_iceprod_dataset_job_ids(
                sim_regexes, utils.FileInfo(fpath)
            )
        assert "Filename does not match any pattern, " in str(e.value)
