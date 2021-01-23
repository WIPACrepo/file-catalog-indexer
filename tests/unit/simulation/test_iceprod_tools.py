"""Test helper functions for iceprod_tools.py."""

# pylint: disable=W0212

import sys
from typing import Dict, List
from unittest.mock import ANY

import pytest

sys.path.append(".")
from indexer.metadata.simulation import (  # isort:skip # noqa # pylint: disable=C0413
    iceprod_tools,
)


def test_get_iceprod_querier_good() -> None:  # pylint: disable=C0103
    """Test _get_iceprod_querier()."""
    goods: Dict[int, type] = {0: type(iceprod_tools._IceProdV1Querier)}

    for dataset_num, querier_type in goods.items():
        ret = iceprod_tools._get_iceprod_querier(dataset_num, ANY, ANY)  # TODO
        assert isinstance(ret, querier_type)


def test_get_iceprod_querier_errors() -> None:  # pylint: disable=C0103
    """Test _get_iceprod_querier() error-cases."""
    errors: List[int] = [0]

    for dataset_num in errors:
        with pytest.raises(iceprod_tools.DatasetNotFound):
            iceprod_tools._get_iceprod_querier(dataset_num, ANY, ANY)  # TODO


def test_parse_dataset_num() -> None:  # pylint: disable=C0103
    """Test _parse_dataset_num()."""
    goods: Dict[str, int] = {"TODO": 0}  # TODO

    for fpath, dataset_num in goods.items():
        assert dataset_num == iceprod_tools._parse_dataset_num(fpath)  # TODO


def test_parse_dataset_num_errors() -> None:  # pylint: disable=C0103
    """Test _parse_dataset_num() error-cases."""
    errors: List[str] = ["TODO"]  # TODO

    for fpath in errors:
        with pytest.raises(iceprod_tools.DatasetNotFound):
            iceprod_tools._parse_dataset_num(fpath)  # TODO
