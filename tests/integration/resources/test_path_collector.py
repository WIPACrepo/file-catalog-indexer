"""Integration test resources/path_collector.py."""


import filecmp
import glob
import logging
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import time
from typing import Any, Final, List, Tuple

import bitmath  # type: ignore[import]
import coloredlogs  # type: ignore[import]
import pytest

from resources.path_collector import common_args, path_collector  # isort:skip  # noqa # pylint: disable=E0401,C0413,C0411
# import resources.path_collector.common_args as common_args
# import resources.path_collector.path_collector as path_collector

# logging.getLogger().setLevel("DEBUG")
coloredlogs.install(level="DEBUG")


def _make_traverse_staging_dir(stage: str, traverse_root: str) -> None:
    # pylint: disable=W0212
    suffix = path_collector._suffix(traverse_root)
    _dir = path_collector._get_traverse_staging_dir(stage, suffix)
    logging.info(f"mkdir {_dir}")
    os.mkdir(_dir)


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


def _setup_testfiles(tmp_root: Path, suffix: str) -> Tuple[str, str]:
    """Create a bunch of files and directories.

    Return traverse root and staging directory.
    """
    # write files
    _write_n_files(f"{tmp_root}/test-traverse-{suffix}/alpha", 15)
    _write_n_files(f"{tmp_root}/test-traverse-{suffix}/beta", 10)
    _write_n_files(f"{tmp_root}/test-traverse-{suffix}/beta/one", 1)
    _write_n_files(f"{tmp_root}/test-traverse-{suffix}/beta/two", 3)
    _write_n_files(f"{tmp_root}/test-traverse-{suffix}/gamma/one", 20)

    # make dirs
    root = common_args.get_full_path(f"{tmp_root}/test-traverse-{suffix}")
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


def _get_archive_file(stage: str) -> os.DirEntry:
    return [d for d in os.scandir(stage) if stat.S_ISREG(os.lstat(d.path).st_mode)][0]


def _assert_out_files(
    stage: str,
    ran_via_shell: bool,
    no_traverser_log: bool = False,
) -> None:
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
    expected_files = [
        "argv.txt",
        "traverser.log",
        "traverse-chunks",
        "path_collector.log",
    ]
    if no_traverser_log:
        expected_files.remove("traverser.log")
    if not ran_via_shell:
        expected_files.remove("path_collector.log")
    assert set(os.listdir(stage_dirs[0])) == set(expected_files)


def _get_traverse_staging_dir(stage: str) -> os.DirEntry:
    return [d for d in os.scandir(stage) if stat.S_ISDIR(os.lstat(d.path).st_mode)][0]


def _get_traverser_log(stage: str) -> str:
    return os.path.join(_get_traverse_staging_dir(stage).path, "traverser.log")


def _get_chunks_dir(stage: str) -> str:
    return os.path.join(_get_traverse_staging_dir(stage).path, "traverse-chunks")


def _get_chunk_0(stage: str) -> os.DirEntry:
    return list(os.scandir(_get_chunks_dir(stage)))[0]


def _assert_out_chunks(stage: str, chunk_size: int) -> None:
    """Test outputted chunk-files."""
    all_lines: List[str] = []
    paths_dir = _get_chunks_dir(stage)
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


def test_chunk_size(tmp_path: Path) -> None:
    """Test using --chunk-size."""

    def _shell() -> None:
        subprocess.check_call(
            f"python3 -m resources.path_collector.path_collector {root}"
            f" --staging-dir {stage}"
            f" --workers 1"
            f" --chunk-size {chunk_size}".split(),
            cwd=".",
        )

    def _direct() -> None:
        _make_traverse_staging_dir(stage, root)
        path_collector.write_all_filepaths_to_files(
            stage, root, 1, "", chunk_size, [], None
        )

    for func in [_direct, _shell]:
        logging.warning(f"Using invocation function: {func}")
        now = str(time.time())

        for kibs in [0] + [10**i for i in range(0, 8)]:
            print("~ " * 60)
            chunk_size = int(bitmath.parse_string(f"{kibs}KiB").to_Byte())
            logging.warning(
                f"chunk_size => {bitmath.best_prefix(chunk_size).format('{value:.2f} {unit}')} ({chunk_size} bytes)"
            )
            stage, root = _setup_testfiles(tmp_path, f"{now}-{kibs}KiB")
            func()
            _assert_out_files(stage, func == _shell)
            _assert_out_chunks(stage, chunk_size)
            _remove_all(stage, root)


def test_w_fast_forward(tmp_path: Path) -> None:  # pylint: disable=R0915
    """Test using --fast-forward."""
    chunk_size: Final[int] = int(bitmath.parse_string("500MiB").to_Byte())

    def _shell(ff_traverse_file: str, w_chunks: bool = False) -> None:
        _make_traverse_staging_dir(stage, root)
        shutil.copy(ff_traverse_file, _get_traverse_staging_dir(stage))
        if w_chunks:
            subprocess.check_call(
                f"python3 -m resources.path_collector.path_collector {root}"
                f" --staging-dir {stage}"
                f" --fast-forward"
                f" --chunk-size {chunk_size}"
                f" --workers 1".split(),
                cwd=".",
            )
        else:
            subprocess.check_call(
                f"python3 -m resources.path_collector.path_collector {root}"
                f" --staging-dir {stage}"
                f" --fast-forward"
                f" --chunk-size 0"
                f" --workers 1".split(),
                cwd=".",
            )
        # assert that fast-forwarding happened by checking the log file
        found = False
        with open(
            os.path.join(_get_traverse_staging_dir(stage), "path_collector.log")
        ) as f:
            for line in f.readlines():
                if "Fast-forwarding past traversing..." in line:
                    found = True
        assert found

    def _direct(ff_traverse_file: str, w_chunks: bool = False) -> None:
        _make_traverse_staging_dir(stage, root)
        if w_chunks:
            path_collector.write_all_filepaths_to_files(
                stage, root, 1, "", chunk_size, [], ff_traverse_file
            )
        else:
            path_collector.write_all_filepaths_to_files(
                stage, root, 1, "", 0, [], ff_traverse_file
            )

    # TEST
    for func in [_direct, _shell]:
        logging.warning(f"Using invocation function: {func}")

        #
        # test good traverse file w/o chunking
        print("~ " * 60)
        logging.warning("ff_traverse_file => good (no chunks)")
        stage, root = _setup_testfiles(tmp_path, "good-traverse-file")
        with open("traverse.raw", "w") as f:
            f.writelines(
                sorted(ln + "\n" for ln in glob.glob(f"{root}/**", recursive=True))
            )
        func("traverse.raw")
        _assert_out_files(stage, func == _shell, no_traverser_log=True)
        assert filecmp.cmp(_get_archive_file(stage), "traverse.raw")
        assert filecmp.cmp(_get_chunk_0(stage), "traverse.raw")
        _remove_all(stage, root, "traverse.raw")

        #
        # test good traverse file w/ chunking
        print("~ " * 60)
        logging.warning("ff_traverse_file => good (w/ chunks)")
        stage, root = _setup_testfiles(tmp_path, "good-traverse-file")
        with open("traverse.raw", "w") as f:
            f.writelines(
                sorted(ln + "\n" for ln in glob.glob(f"{root}/**", recursive=True))
            )
        func("traverse.raw", w_chunks=True)
        _assert_out_files(stage, func == _shell, no_traverser_log=True)
        assert filecmp.cmp(_get_archive_file(stage), "traverse.raw")
        _assert_out_chunks(stage, chunk_size)
        _remove_all(stage, root, "traverse.raw")

        #
        # test empty traverse file w/o chunking
        # -- there will be a traverse-chunks/chunk-0 file, but it's empty
        print("~ " * 60)
        logging.warning("ff_traverse_file => empty (no chunks)")
        with open("traverse.raw", "w") as f:
            pass
        stage, root = _setup_testfiles(tmp_path, "empty-traverse-file")
        func("traverse.raw")
        _assert_out_files(stage, func == _shell, no_traverser_log=True)
        assert int(os.lstat("traverse.raw").st_size) == 0
        assert (
            len(os.listdir(_get_chunks_dir(stage))) == 1
        )  # 'chunk-0' in traverse-chunks/
        assert filecmp.cmp(_get_archive_file(stage), "traverse.raw")
        assert filecmp.cmp(_get_chunk_0(stage), "traverse.raw")
        _remove_all(stage, root, "traverse.raw")

        #
        # test empty traverse file w/ chunking
        # -- there will be a traverse-chunks/ directory, but it's empty
        print("~ " * 60)
        logging.warning("ff_traverse_file => empty (w/ chunks)")
        with open("traverse.raw", "w") as f:
            pass
        stage, root = _setup_testfiles(tmp_path, "empty-traverse-file")
        func("traverse.raw", w_chunks=True)
        _assert_out_files(stage, func == _shell, no_traverser_log=True)
        assert int(os.lstat("traverse.raw").st_size) == 0
        assert not os.listdir(_get_chunks_dir(stage))  # empty traverse-chunks/
        assert filecmp.cmp(_get_archive_file(stage), "traverse.raw")
        _remove_all(stage, root, "traverse.raw")

        #
        # test traverse file w/ bad lines (filepaths) w/o chunking
        # -- there will be a traverse-chunks/chunk-0 file that contains all the lines
        print("~ " * 60)
        logging.warning("ff_traverse_file => bad-filepaths")
        with open("traverse.raw", "w") as f:
            f.write("a-foo\nb-bar\nc-baz\n")
        stage, root = _setup_testfiles(tmp_path, "bad-filepaths-traverse-file")
        func("traverse.raw")
        _assert_out_files(stage, func == _shell, no_traverser_log=True)
        assert filecmp.cmp(_get_archive_file(stage), "traverse.raw")
        # 'chunk-0' in traverse-chunks/
        assert len(os.listdir(_get_chunks_dir(stage))) == 1
        assert filecmp.cmp(_get_chunk_0(stage), "traverse.raw")
        _remove_all(stage, root, "traverse.raw")

        #
        # test traverse file w/ bad lines (filepaths) w/ chunking
        # -- there will be a traverse-chunks/ directory, but it's empty
        # -- there is an archive file and argv.txt
        print("~ " * 60)
        logging.warning("ff_traverse_file => bad-filepaths (w/ chunks)")
        with open("traverse.raw", "w") as f:
            f.write("a-foo\nb-bar\nc-baz\n")
        stage, root = _setup_testfiles(tmp_path, "bad-filepaths-traverse-file")
        func("traverse.raw", w_chunks=True)
        _assert_out_files(stage, func == _shell, no_traverser_log=True)
        assert os.path.exists(_get_chunks_dir(stage))
        _get_archive_file(stage)  # no raised exception AKA file exists
        assert not os.listdir(_get_chunks_dir(stage))  # empty dir
        assert "argv.txt" in os.listdir(_get_traverse_staging_dir(stage))
        _remove_all(stage, root, "traverse.raw")


def test_exclude(tmp_path: Path) -> None:
    """Test using --exclude."""
    chunk_size: Final[int] = int(bitmath.parse_string("500MiB").to_Byte())

    def _shell(excludes: List[str]) -> None:
        subprocess.check_call(
            f"python3 -m resources.path_collector.path_collector {root}"
            f" --staging-dir {stage}"
            f" --workers 1"
            f" --exclude {' '.join(os.path.abspath(e) for e in excludes)}"
            f" --chunk-size {chunk_size}".split(),
            cwd=".",
        )

    def _direct(excludes: List[str]) -> None:
        _make_traverse_staging_dir(stage, root)
        path_collector.write_all_filepaths_to_files(
            stage, root, 1, "", chunk_size, excludes, None
        )

    for func in [_direct, _shell]:
        logging.warning(f"Using invocation function: {func}")

        #
        # good excludes
        print("~ " * 60)
        logging.warning("exclude => good")
        stage, root = _setup_testfiles(tmp_path, "good-excludes")
        logging.error(f"{stage=} {root=}")
        func(
            [
                f"{root}/gamma",
                f"{root}/beta/two",
            ]
        )
        _assert_out_files(stage, func == _shell)
        _assert_out_chunks(stage, chunk_size)
        with open(_get_archive_file(stage)) as f:
            lines = f.read()
            assert lines
        for e in ["gamma", "beta/two"]:
            assert e not in lines
        _remove_all(stage, root)

        #
        # real but needless excludes
        print("~ " * 60)
        logging.warning("exclude => real but needless")
        stage, root = _setup_testfiles(tmp_path, "real-but-needless-excludes")
        func(["./.gitignore", "./README.md"])
        _assert_out_files(stage, func == _shell)
        _assert_out_chunks(stage, chunk_size)
        with open(_get_archive_file(stage)) as f:
            lines = f.read()
            assert lines
        for e in [".gitignore", "README.md"]:
            assert e not in lines
        _remove_all(stage, root)

        #
        # bad excludes
        print("~ " * 60)
        logging.warning("exclude => bad")
        stage, root = _setup_testfiles(tmp_path, "bad-excludes")
        with pytest.raises(subprocess.CalledProcessError):  # raised by traverser.py
            func(["./foo", "./bar"])
        if func == _direct:
            with open(_get_traverser_log(stage)) as f:
                assert "FileNotFoundError" in f.read()
        # when using _shell, it's a nested CalledProcessError, so no files are written
        _remove_all(stage, root)


def test_previous_traverse(tmp_path: Path) -> None:
    """Test using --previous-traverse."""

    def _shell(prev_traverse: str) -> None:
        subprocess.check_call(
            f"python3 -m resources.path_collector.path_collector {root}"
            f" --staging-dir {stage}"
            f" --previous-traverse {prev_traverse}"
            f" --chunk-size 0"
            f" --workers 1".split(),
            cwd=".",
        )

    def _direct(prev_traverse: str) -> None:
        _make_traverse_staging_dir(stage, root)
        path_collector.write_all_filepaths_to_files(
            stage, root, 1, prev_traverse, 0, [], None
        )

    for func in [_direct, _shell]:
        logging.warning(f"Using invocation function: {func}")

        # good previous-traverse
        print("~ " * 60)
        logging.warning("previous-traverse => good")
        with open("./archive-prev.txt", "w") as f:
            stage, root = _setup_testfiles(tmp_path, "previous")
            _make_traverse_staging_dir(stage, root)
            path_collector.write_all_filepaths_to_files(stage, root, 1, "", 0, [], None)
            with open(_get_archive_file(stage), "r") as a_f:
                f.writelines(a_f.readlines()[5:15])
        _remove_all(stage, root)
        stage, root = _setup_testfiles(tmp_path, "prev-traverse-good")
        func("./archive-prev.txt")
        _assert_out_files(stage, func == _shell)
        _assert_out_chunks(stage, 0)
        with open(_get_archive_file(stage), "r") as a_f:
            with open("./archive-prev.txt", "r") as p_f:
                assert all(ln not in a_f.readlines() for ln in p_f.readlines())
        _remove_all(stage, root, "./archive-prev.txt")

        # bad lines previous-traverse
        # -- just as okay as good lines
        print("~ " * 60)
        logging.warning("previous-traverse => bad lines")
        with open("./archive-prev.txt", "w") as f:
            stage, root = _setup_testfiles(tmp_path, "previous")
            _make_traverse_staging_dir(stage, root)
            path_collector.write_all_filepaths_to_files(stage, root, 1, "", 0, [], None)
            with open(_get_archive_file(stage), "r") as a_f:
                f.writelines(["!FOOBARBAZ!"] + a_f.readlines()[5:15])
        _remove_all(stage, root)
        stage, root = _setup_testfiles(tmp_path, "prev-traverse-bad-lines")
        func("./archive-prev.txt")
        _assert_out_files(stage, func == _shell)
        _assert_out_chunks(stage, 0)
        with open(_get_archive_file(stage), "r") as a_f:
            with open("./archive-prev.txt", "r") as p_f:
                assert all(ln not in a_f.readlines() for ln in p_f.readlines())
        _remove_all(stage, root, "./archive-prev.txt")


def test_fast_forward_logic(tmp_path: Path) -> None:
    """Test using --fast-forward."""
    #
    # test good use of fast_forward w/o an existing traverse_staging_dir
    stage, root = _setup_testfiles(tmp_path, "w-fast_forward-no-traverse-staging-dir")
    subprocess.check_call(
        f"python3 -m resources.path_collector.path_collector {root}"
        f" --staging-dir {stage}"
        f" --workers 1"
        f" --fast-forward".split(),
        cwd=".",
    )
    _remove_all(stage, root)

    #
    # test good use of fast_forward w/ an existing traverse_staging_dir
    stage, root = _setup_testfiles(tmp_path, "w-fast_forward-w-traverse-staging-dir")
    _make_traverse_staging_dir(stage, root)
    subprocess.check_call(
        f"python3 -m resources.path_collector.path_collector {root}"
        f" --staging-dir {stage}"
        f" --workers 1"
        f" --fast-forward".split(),
        cwd=".",
    )
    _remove_all(stage, root)

    #
    # test w/o --fast-forward & w/o an existing traverse_staging_dir
    stage, root = _setup_testfiles(tmp_path, "no-fast_forward-no-traverse-staging-dir")
    subprocess.check_call(
        f"python3 -m resources.path_collector.path_collector {root}"
        f" --staging-dir {stage}"
        f" --workers 1".split(),
        cwd=".",
    )
    _remove_all(stage, root)

    #
    # test w/o --fast-forward, but w/ an existing traverse_staging_dir
    stage, root = _setup_testfiles(tmp_path, "no-fast_forward-w-traverse-staging-dir")
    _make_traverse_staging_dir(stage, root)
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_call(
            f"python3 -m resources.path_collector.path_collector {root}"
            f" --staging-dir {stage}"
            f" --workers 1".split(),
            cwd=".",
        )
    _remove_all(stage, root)
