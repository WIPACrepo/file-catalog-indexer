"""Test helper functions for iceprod_tools.py."""

# pylint: disable=W0212

from typing import Dict, List, Union
from unittest.mock import ANY

import pytest
from indexer.metadata.simulation import iceprod_tools


def test_get_iceprod_querier_good() -> None:  # pylint: disable=C0103
    """Test _get_iceprod_querier()."""
    assert iceprod_tools._ICEPROD_V2_DATASET_RANGE == range(20000, 30000)
    assert iceprod_tools._ICEPROD_V1_DATASET_RANGE == range(0, 20000)

    goods: Dict[int, type] = {
        0: iceprod_tools._IceProdV1Querier,
        1: iceprod_tools._IceProdV1Querier,
        2020: iceprod_tools._IceProdV1Querier,
        19999: iceprod_tools._IceProdV1Querier,
        20000: iceprod_tools._IceProdV2Querier,
        29999: iceprod_tools._IceProdV2Querier,
    }

    for dataset_num, querier_type in goods.items():
        ret = iceprod_tools._get_iceprod_querier(dataset_num, ANY, ANY)
        assert isinstance(ret, querier_type)


def test_get_iceprod_querier_errors() -> None:  # pylint: disable=C0103
    """Test _get_iceprod_querier() error-cases."""
    errors: List[Union[float, int]] = [-1, 30000, 25000.5]  # out of range & floats

    for dataset_num in errors:
        print(dataset_num)
        with pytest.raises(iceprod_tools.DatasetNotFound):
            iceprod_tools._get_iceprod_querier(dataset_num, ANY, ANY)  # type: ignore[arg-type]


def test_parse_dataset_num() -> None:  # pylint: disable=C0103
    """Test _parse_dataset_num()."""
    goods: Dict[str, int] = {
        "/foo/bar/15000/baz.tar": 15000,
        "/ice/cube/0/abc.txt": 0,
        "oh/123/canada/345/eh.i3": 345,  # search right-to-left
        "new/york/4321/big/25000/apple.baz": 25000,
        "boston/mass/25000/bean/4321/town.baz": 25000,  # favor IP2 range (vs IP1)
        "grand/123/canyon/-1/natl/50000/park.txt": 123,
        "789/abc123/easy.txt": 789,  # ignore alphanumerics
        "this/is/a/dir/55/": 55,  # passing a dirpath (ends in "/")
        "icprod1/2005/looks/like/a/year.txt": 2005,  # possible false-positive match
    }

    for fpath, dataset_num in goods.items():
        assert dataset_num == iceprod_tools._parse_dataset_num_from_dirpath(fpath)


def test_parse_dataset_num_errors() -> None:  # pylint: disable=C0103
    """Test _parse_dataset_num() error-cases."""
    errors: List[str] = [
        "green/eggs/and/ham.txt",  # no numbers
        "/ice/cube/0.txt",  # no numbers in dirpath
        "grand/canyon/-1/natl/50000/park.txt",  # no numbers in range
        "/abc123/easy.txt",  # alpha-numeric dir
        "",
        "/",
        "5",
        "/6",
    ]

    for fpath in errors:
        with pytest.raises(iceprod_tools.DatasetNotFound):
            iceprod_tools._parse_dataset_num_from_dirpath(fpath)
