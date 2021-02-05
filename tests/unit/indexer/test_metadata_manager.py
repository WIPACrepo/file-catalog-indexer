"""Selectively unit test metadata_mangager.py."""

# pylint: disable=W0212

import sys

sys.path.append(".")
from api import metadata_manager  # isort:skip # noqa # pylint: disable=C0413


def test_new_file_logic() -> None:
    """Test MetadataManager.new_file()'s factory logic."""
    # good - /data/sim/*
    assert metadata_manager.MetadataManager._is_data_sim_filepath("/data/sim/foo")
    assert not metadata_manager.MetadataManager._is_data_exp_filepath("/data/sim/foo")

    # good - /data/exp/*
    assert not metadata_manager.MetadataManager._is_data_sim_filepath("/data/exp/foo")
    assert metadata_manager.MetadataManager._is_data_exp_filepath("/data/exp/foo")

    # bad - */data/sim/*
    assert not metadata_manager.MetadataManager._is_data_sim_filepath("~/data/sim/bar")
    assert not metadata_manager.MetadataManager._is_data_exp_filepath("~/data/sim/bar")

    # bad - */data/sim/*
    assert not metadata_manager.MetadataManager._is_data_sim_filepath("../exp/sim/bar")
    assert not metadata_manager.MetadataManager._is_data_exp_filepath("../exp/sim/bar")

    # bad - /mnt/*
    assert not metadata_manager.MetadataManager._is_data_sim_filepath("/mnt/lfs6/sim/b")
    assert not metadata_manager.MetadataManager._is_data_exp_filepath("/mnt/lfs6/sim/b")
    assert not metadata_manager.MetadataManager._is_data_sim_filepath("/mnt/lfs6/exp/b")
    assert not metadata_manager.MetadataManager._is_data_exp_filepath("/mnt/lfs6/exp/b")

    # bad - misc
    assert not metadata_manager.MetadataManager._is_data_sim_filepath("//data/sim/foo")
    assert not metadata_manager.MetadataManager._is_data_exp_filepath("//data/sim/foo")
    assert not metadata_manager.MetadataManager._is_data_sim_filepath("//data/exp/foo")
    assert not metadata_manager.MetadataManager._is_data_exp_filepath("//data/exp/foo")
    #
    assert not metadata_manager.MetadataManager._is_data_sim_filepath("/data/simu/a")
    assert not metadata_manager.MetadataManager._is_data_exp_filepath("/data/simu/a")
    assert not metadata_manager.MetadataManager._is_data_sim_filepath("/data/expo/b")
    assert not metadata_manager.MetadataManager._is_data_exp_filepath("/data/expo/b")
