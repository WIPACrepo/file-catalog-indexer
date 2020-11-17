"""Integration test resources/all_paths.py."""


import logging
import os
import re
import shutil
import stat
import sys
import time
from typing import List, Tuple

import bitmath  # type: ignore[import]

sys.path.append("./resources/all_paths")
import all_paths  # type: ignore[import]  # isort:skip  # noqa # pylint: disable=E0401,C0413,C0411
import common_args  # isort:skip  # noqa # pylint: disable=E0401,C0413,C0411


logging.getLogger().setLevel("DEBUG")


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


def _test_out_files(stage: str, chunk_size: int) -> None:
    """Test outputted files."""
    # look at dir-structure
    assert len(os.listdir(stage)) == 2
    stage_dirs = [
        d for d in os.scandir(stage) if stat.S_ISDIR(os.lstat(d.path).st_mode)
    ]
    assert len(stage_dirs) == 1
    stage_files = [
        d for d in os.scandir(stage) if stat.S_ISREG(os.lstat(d.path).st_mode)
    ]
    assert len(stage_files) == 1
    assert set(os.listdir(stage_dirs[0])) == set(["argv.txt", "paths.log", "paths"])

    # look at chunks
    all_lines: List[str] = []
    paths_dir = os.path.join(stage_dirs[0].path, "paths")
    assert os.listdir(paths_dir)
    # no chunking
    if chunk_size == 0:
        assert os.listdir(paths_dir) == ["chunk-0"]
        logging.info("chunk-0")
        with open(list(os.scandir(paths_dir))[0], "r") as f:
            all_lines = [ln.strip() for ln in f]
    # yes chunking
    else:

        def _is_last_chunk(_chunk_name: str) -> bool:
            return _chunk_name == sorted(os.listdir(paths_dir))[-1]

        nums: List[int] = []
        for chunk in os.scandir(paths_dir):
            # assert chunk name # pylint: disable=C0325
            assert (match := re.match(r"chunk-(?P<num>\d+)", chunk.name))
            assert int(match.groupdict()["num"]) not in nums
            nums.append(int(match.groupdict()["num"]))
            # assert about chunk's aggregate size
            with open(chunk.path, "r") as f:
                lines = [ln.strip() for ln in f]
                all_lines.extend(lines)
                # log
                logging.info(f"{chunk.path=}")
                logging.debug({ln: os.stat(ln).st_size for ln in lines})
                if not _is_last_chunk(chunk.name):
                    # check that the chunk's aggregate size is not less than `chunk_size`
                    assert sum(int(os.stat(ln).st_size) for ln in lines) >= chunk_size
                    # check that the last chunk was what pushed it past the limit
                    assert (
                        sum(int(os.stat(ln).st_size) for ln in lines[:-1]) < chunk_size
                    )
                else:
                    assert sum(int(os.stat(ln).st_size) for ln in lines)
        # assert all the chunks are there
        for num, i in zip(sorted(nums), range(1, len(nums) + 1)):
            assert i == num

    # assert the archive file and the chunk(s) have the same content
    with open(stage_files[0], "r") as f:
        assert set(all_lines) == set(ln.strip() for ln in f)


def test_chunk_sizes() -> None:
    """Test various chunk sizes."""
    for kibs in [0] + [10 ** i for i in range(0, 8)]:
        chunk_size = int(bitmath.parse_string(f"{kibs}KiB").to_Byte())
        logging.warning(
            f"chunk_size => {bitmath.best_prefix(chunk_size).format('{value:.2f} {unit}')} ({chunk_size} bytes)"
        )

        # setup
        stage, root = _setup()

        # execute
        all_paths.write_all_filepaths_to_files(stage, root, 1, "", chunk_size, [], None)

        # test
        _test_out_files(stage, chunk_size)

        # cleanup
        _cleanup(stage, root)

        print("~ " * 60)


# TODO
# def test_shell() -> None:
#     """Test via the shell."""
#     root = _setup_traverse_files()

# TODO --traverse-file

# TODO --exclude

# TODO --previous-traverse
