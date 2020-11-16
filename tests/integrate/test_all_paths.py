"""Integration test resources/all_paths.py."""


import os
import shutil
import sys
import time
from shutil import copyfile
from typing import Tuple

import bitmath  # type: ignore[import]

sys.path.append("./resources/all_paths")
import all_paths  # type: ignore[import]  # isort:skip  # noqa # pylint: disable=E0401,C0413,C0411
import common_args  # isort:skip  # noqa # pylint: disable=E0401,C0413,C0411


def _write_file(path: str, size: str) -> None:
    """Write sparse file."""
    with open(path, "wb") as f:
        bytes_ = int(bitmath.parse_string_unsafe(size).to_Byte())
        f.seek(bytes_)
        f.write(str.encode("0"))


def _write_n_files(root: str, num: int) -> None:
    """Write n sparse files, each in increasing increments of 100KB."""
    os.makedirs(root, exist_ok=True)

    for i in range(1, num):
        size = f"{i*100}KiB"
        _write_file(f"{root}/{size}", size)


def _setup() -> Tuple[str, str]:
    """Create a bunch of files and directories."""
    now = int(time.time())

    # write files
    _write_n_files(f"./test-traverse-{now}/alpha", 15)
    _write_n_files(f"./test-traverse-{now}/beta", 10)
    _write_n_files(f"./test-traverse-{now}/beta/one", 1)
    _write_n_files(f"./test-traverse-{now}/beta/two", 3)
    _write_n_files(f"./test-traverse-{now}/gamma/one", 20)

    # make dirs
    root = common_args.get_full_path(f"./test-traverse-{now}")
    stage = f"{root}-stage"
    os.makedirs(stage)
    stage = common_args.get_full_path(stage)

    return stage, root


def _cleanup(stage: str, root: str) -> None:
    """Delete directories and files."""
    shutil.rmtree(stage)
    shutil.rmtree(root)


def test_directly() -> None:
    """Test via invoking the function."""
    # setup
    stage, root = _setup()

    # execute
    all_paths.write_all_filepaths_to_files(
        stage, root, 1, "", int(bitmath.parse_string("1MiB").to_Byte()), [], None,
    )

    # test

    # cleanup
    _cleanup(stage, root)


# def test_shell() -> None:
#     """Test via the shell."""
#     root = _setup_traverse_files()
