"""Integration test resources/all_paths.py."""


import filecmp
import glob
import logging
import os
import re
import shutil
import stat
import subprocess
import sys
import time
from typing import Any, Final, List, Tuple

import bitmath  # type: ignore[import]
import pytest

sys.path.append("./resources/all_paths")
import all_paths  # type: ignore[import]  # isort:skip  # noqa # pylint: disable=E0401,C0413,C0411
import common_args  # isort:skip  # noqa # pylint: disable=E0401,C0413,C0411


# logging.getLogger().setLevel("DEBUG")


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


def _setup_testfiles(suffix: str) -> Tuple[str, str]:
    """Create a bunch of files and directories.

    Return traverse root and staging directory.
    """
    # write files
    _write_n_files(f"./test-traverse-{suffix}/alpha", 15)
    _write_n_files(f"./test-traverse-{suffix}/beta", 10)
    _write_n_files(f"./test-traverse-{suffix}/beta/one", 1)
    _write_n_files(f"./test-traverse-{suffix}/beta/two", 3)
    _write_n_files(f"./test-traverse-{suffix}/gamma/one", 20)

    # make dirs
    root = common_args.get_full_path(f"./test-traverse-{suffix}")
    stage = f"{root}-stage"
    os.makedirs(stage)
    stage = common_args.get_full_path(stage)

    return stage, root


def _remove_all(*args: Any) -> None:
    """Delete directories (recursively) and/or files."""
    for f in args:
        try:
            shutil.rmtree(f)
        except NotADirectoryError:
            os.remove(f)


def _get_archive_file(stage: str) -> os.DirEntry:  # type: ignore[type-arg]
    return [d for d in os.scandir(stage) if stat.S_ISREG(os.lstat(d.path).st_mode)][0]


def _assert_out_files(stage: str, no_paths_log: bool = False) -> None:
    """Test outputted files and directories."""
    # 2 entries in staging directory
    assert len(os.listdir(stage)) == 2

    # 1 dir in staging directory
    stage_dirs = [
        d for d in os.scandir(stage) if stat.S_ISDIR(os.lstat(d.path).st_mode)
    ]
    assert len(stage_dirs) == 1

    # 1 file in staging directory
    stage_files = [
        d for d in os.scandir(stage) if stat.S_ISREG(os.lstat(d.path).st_mode)
    ]
    assert len(stage_files) == 1

    # the files in traverse staging directory...
    if no_paths_log:
        assert set(os.listdir(stage_dirs[0])) == set(["argv.txt", "paths"])
    else:
        assert set(os.listdir(stage_dirs[0])) == set(["argv.txt", "paths.log", "paths"])


def _get_traverse_staging_dir(stage: str) -> os.DirEntry:  # type: ignore[type-arg]
    return [d for d in os.scandir(stage) if stat.S_ISDIR(os.lstat(d.path).st_mode)][0]


def _get_paths_dir(stage: str) -> str:
    return os.path.join(_get_traverse_staging_dir(stage).path, "paths")


def _get_chunk_0(stage: str) -> os.DirEntry:  # type: ignore[type-arg]
    return list(os.scandir(_get_paths_dir(stage)))[0]


def _assert_out_chunks(stage: str, chunk_size: int) -> None:
    """Test outputted chunk-files."""
    all_lines: List[str] = []
    paths_dir = _get_paths_dir(stage)
    assert os.listdir(paths_dir)

    # no chunking
    if chunk_size == 0:
        assert os.listdir(paths_dir) == ["chunk-0"]
        logging.info("chunk-0")
        with open(_get_chunk_0(stage), "r") as f:
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
    with open(_get_archive_file(stage), "r") as f:
        assert set(all_lines) == set(ln.strip() for ln in f)


def test_chunk_sizes() -> None:
    """Test various chunk sizes."""

    def _shell() -> None:
        subprocess.check_call(
            f"python ./resources/all_paths/all_paths.py {root}"
            f" --staging-dir {stage}"
            f" --workers 1"
            f" --chunk-size {chunk_size}".split(),
            cwd=".",
        )

    def _direct() -> None:
        all_paths.write_all_filepaths_to_files(stage, root, 1, "", chunk_size, [], None)

    for func in [_direct, _shell]:
        logging.warning(f"Using invocation function: {func}")
        now = str(time.time())

        for kibs in [0] + [10 ** i for i in range(0, 8)]:
            print("~ " * 60)
            chunk_size = int(bitmath.parse_string(f"{kibs}KiB").to_Byte())
            logging.warning(
                f"chunk_size => {bitmath.best_prefix(chunk_size).format('{value:.2f} {unit}')} ({chunk_size} bytes)"
            )
            stage, root = _setup_testfiles(f"{now}-{kibs}KiB")
            func()
            _assert_out_files(stage)
            _assert_out_chunks(stage, chunk_size)
            _remove_all(stage, root)


def test_w_traverse_file() -> None:  # pylint: disable=R0915
    """Test using --traverse-file."""
    chunk_size: Final[int] = int(bitmath.parse_string("500MiB").to_Byte())

    def _shell(traverse_file: str, w_chunks: bool = False) -> None:
        if w_chunks:
            subprocess.check_call(
                f"python ./resources/all_paths/all_paths.py {root}"
                f" --staging-dir {stage}"
                f" --traverse-file {traverse_file}"
                f" --chunk-size {chunk_size}"
                f" --workers 1".split(),
                cwd=".",
            )
        else:
            subprocess.check_call(
                f"python ./resources/all_paths/all_paths.py {root}"
                f" --staging-dir {stage}"
                f" --traverse-file {traverse_file}"
                f" --workers 1".split(),
                cwd=".",
            )

    def _direct(traverse_file: str, w_chunks: bool = False) -> None:
        if w_chunks:
            all_paths.write_all_filepaths_to_files(
                stage, root, 1, "", chunk_size, [], traverse_file
            )
        else:
            all_paths.write_all_filepaths_to_files(
                stage, root, 1, "", 0, [], traverse_file
            )

    # TEST
    for func in [_direct, _shell]:
        logging.warning(f"Using invocation function: {func}")

        #
        # test good traverse file w/o chunking
        print("~ " * 60)
        logging.warning("traverse_file => good.txt (no chunks)")
        stage, root = _setup_testfiles("good-traverse-file")
        with open("good.txt", "w") as f:
            f.writelines(ln + "\n" for ln in glob.glob(f"{root}/**", recursive=True))
        func("good.txt")
        _assert_out_files(stage, no_paths_log=True)  # no 'paths.log'
        assert filecmp.cmp(_get_archive_file(stage), "good.txt")
        assert filecmp.cmp(_get_chunk_0(stage), "good.txt")
        _remove_all(stage, root, "good.txt")

        #
        # test good traverse file w/ chunking
        print("~ " * 60)
        logging.warning("traverse_file => good.txt (w/ chunks)")
        stage, root = _setup_testfiles("good-traverse-file")
        with open("good.txt", "w") as f:
            f.writelines(ln + "\n" for ln in glob.glob(f"{root}/**", recursive=True))
        func("good.txt", w_chunks=True)
        _assert_out_files(stage, no_paths_log=True)  # no 'paths.log'
        assert filecmp.cmp(_get_archive_file(stage), "good.txt")
        _assert_out_chunks(stage, chunk_size)
        _remove_all(stage, root, "good.txt")

        #
        # test empty traverse file w/o chunking
        # -- there will be a paths/chunk-0 file, but it's empty
        print("~ " * 60)
        logging.warning("traverse_file => empty.txt (no chunks)")
        with open("empty.txt", "w") as f:
            pass
        stage, root = _setup_testfiles("empty-traverse-file")
        func("empty.txt")
        _assert_out_files(stage, no_paths_log=True)  # no 'paths.log'
        assert int(os.lstat("empty.txt").st_size) == 0
        assert len(os.listdir(_get_paths_dir(stage))) == 1  # 'chunk-0' in paths/
        assert filecmp.cmp(_get_archive_file(stage), "empty.txt")
        assert filecmp.cmp(_get_chunk_0(stage), "empty.txt")
        _remove_all(stage, root, "empty.txt")

        #
        # test empty traverse file w/ chunking
        # -- there will be a paths/ directory, but it's empty
        print("~ " * 60)
        logging.warning("traverse_file => empty.txt (w/ chunks)")
        with open("empty.txt", "w") as f:
            pass
        stage, root = _setup_testfiles("empty-traverse-file")
        func("empty.txt", w_chunks=True)
        _assert_out_files(stage, no_paths_log=True)  # no 'paths.log'
        assert int(os.lstat("empty.txt").st_size) == 0
        assert not os.listdir(_get_paths_dir(stage))  # empty paths/
        assert filecmp.cmp(_get_archive_file(stage), "empty.txt")
        _remove_all(stage, root, "empty.txt")

        #
        # test traverse file w/ bad lines (filepaths) w/o chunking
        # -- there will be a paths/chunk-0 file, but it's empty
        print("~ " * 60)
        logging.warning("traverse_file => bad-filepaths.txt")
        with open("bad-filepaths.txt", "w") as f:
            f.write("foo\nbar\nbaz")
        stage, root = _setup_testfiles("bad-filepaths-traverse-file")
        func("bad-filepaths.txt")
        _assert_out_files(stage, no_paths_log=True)  # no 'paths.log'
        assert filecmp.cmp(_get_archive_file(stage), "bad-filepaths.txt")
        assert len(os.listdir(_get_paths_dir(stage))) == 1  # 'chunk-0' in paths/
        assert filecmp.cmp(_get_chunk_0(stage), "bad-filepaths.txt")
        _remove_all(stage, root, "bad-filepaths.txt")

        #
        # test traverse file w/ bad lines (filepaths) w/ chunking
        # -- there will be a paths/ directory, but it's empty
        # -- no archive file, but there is argv.txt
        print("~ " * 60)
        logging.warning("traverse_file => bad-filepaths.txt (w/ chunks)")
        with open("bad-filepaths.txt", "w") as f:
            f.write("foo\nbar\nbaz")
        stage, root = _setup_testfiles("bad-filepaths-traverse-file")
        with pytest.raises((FileNotFoundError, subprocess.CalledProcessError)):
            func("bad-filepaths.txt", w_chunks=True)
        with pytest.raises(AssertionError):
            _assert_out_files(stage, no_paths_log=True)  # no 'paths.log'
        assert os.path.exists(_get_paths_dir(stage))
        with pytest.raises(IndexError):
            _get_archive_file(stage)
        assert not os.listdir(_get_paths_dir(stage))
        assert "argv.txt" in os.listdir(_get_traverse_staging_dir(stage))
        _remove_all(stage, root, "bad-filepaths.txt")


# TODO --exclude

# TODO --previous-traverse
