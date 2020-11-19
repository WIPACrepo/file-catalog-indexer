"""Traverse given directory for all filepaths, and split list into chunks.

These chunks are outputted files, which are used as input in
indexer_make_dag.py jobs.
"""

import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime as dt
from typing import List, Optional, Union

import bitmath  # type: ignore[import]
import coloredlogs  # type: ignore[import]

sys.path.append(".")
from common_args import (  # isort:skip  # noqa # pylint: disable=E0401,C0413,C0411
    get_parser_w_common_args,
    get_full_path,
)


def check_call_and_log(
    cmd: Union[List[str], str], cwd: str = ".", shell: bool = False
) -> None:
    """Wrap subprocess.check_call and print command."""
    if shell and isinstance(cmd, list):
        raise Exception("Do not set shell=True and pass a list--pass a string.")
    logging.info(f"Execute: {cmd} @ {cwd}")
    subprocess.check_call(cmd, cwd=cwd, shell=shell)


def _full_traverse(
    traverse_staging_dir: str,
    traverse_root: str,
    excluded_paths: List[str],
    workers: int,
) -> str:
    """Get all filepaths in traverse_root and sort the list."""
    traverse_out = os.path.join(traverse_staging_dir, "traverser.out")
    traverse_sorted = os.path.join(traverse_staging_dir, "traverse.sorted")
    traverse_log = os.path.join(traverse_staging_dir, "traverser.log")

    # traverse
    exculdes_args = "--exclude " + " ".join(excluded_paths) if excluded_paths else ""
    check_call_and_log(
        f"python traverser.py {traverse_root} "
        f"--workers {workers}"
        f" {exculdes_args} > {traverse_out} 2> {traverse_log}",
        shell=True,
    )

    # remove blanks
    check_call_and_log(f"""sed -i '/^[[:space:]]*$/d' {traverse_out}""", shell=True)

    # sort -- this'll ensure chunks/jobs have filepaths from the same "region"
    check_call_and_log(
        f"sort -T {traverse_staging_dir} {traverse_out} > {traverse_sorted}", shell=True
    )

    # cleanup
    check_call_and_log(f"rm {traverse_out}".split())

    return traverse_sorted


def _remove_already_collected_files(prev_traverse: str, traverse_file: str) -> None:
    """Get lines(filepaths) unique to this traverse versus the previous."""
    if prev_traverse:
        check_call_and_log(
            f"comm -1 -3 {prev_traverse} {traverse_file} > {traverse_file}.unique",
            shell=True,
        )
        check_call_and_log(f"mv {traverse_file}.unique {traverse_file}".split())


def _chunk(traverse_staging_dir: str, chunk_size: int, traverse_file: str) -> None:
    """Chunk the traverse file up by approx equal aggregate file size.

    Assumes: `chunk_size` >> any one file's size

    Chunks are guaranteed to be equal to or barely greater than
    `chunk_size`. If `chunk_size` is too small (< `MINIMUM_CHUNK_SIZE`),
    only one chunk is made ("chunk-0"), a copy of `traverse_file`.

    Example:
    `traverse_staging_dir/chunks/chunk-1645`
    """
    chunks_dir = os.path.join(traverse_staging_dir, "traverse-chunks/")

    check_call_and_log(f"mkdir {chunks_dir}".split())

    if chunk_size == 0:
        logging.warning("Chunking bypassed, --chunk-size is zero")
        check_call_and_log(
            f"cp {traverse_file} {os.path.join(chunks_dir, 'chunk-0')}".split()
        )
        return

    def _chunk_it(i: int, chunk_lines: List[str]) -> str:
        filename = f"chunk-{i}"
        with open(os.path.join(chunks_dir, filename), "w") as chunk_f:
            chunk_f.writelines(chunk_lines)
        return filename

    _id = 0
    queue_f_size, queue = 0, []
    total_f_size = 0
    with open(traverse_file, "r") as traverse_f:
        for path in traverse_f:
            queue.append(path)
            f_size = int(os.stat(path.strip()).st_size)
            queue_f_size += f_size
            total_f_size += f_size
            # time to chunk?
            if queue_f_size >= chunk_size:
                _id += 1
                _chunk_it(_id, queue)
                queue_f_size, queue = 0, []  # reset
    # chunk whatever is left
    if queue:
        _id += 1
        _chunk_it(_id, queue)

    logging.info(
        f"Chunked traverse into {_id} chunk-files"
        f" ~{bitmath.best_prefix(chunk_size).format('{value:.2f} {unit}')}"
        f" ({chunk_size} bytes) each @ {chunks_dir}."
        f" Total ~{bitmath.best_prefix(total_f_size).format('{value:.2f} {unit}')}."
    )


def _archive(
    staging_dir: str, suffix: str, traverse_file: str, dont_replace: bool = False
) -> None:
    """Copy/Archive traverse into a file.

    Example:
    /data/user/eevans/data-exp-2020-03-10T15:11:42
    """
    time = dt.now().isoformat(timespec="seconds")
    file_archive = os.path.join(staging_dir, f"{suffix}-{time}")
    if dont_replace:
        check_call_and_log(f"cp {traverse_file} {file_archive}".split())
    else:
        check_call_and_log(f"mv {traverse_file} {file_archive}".split())
    logging.info(f"Archive File: at {file_archive}")


def _suffix(
    traverse_root: str, has_excluded_paths: bool, has_traverse_file_arg: bool
) -> str:
    suffix = traverse_root.strip("/").replace("/", "-")  # Ex: 'data-exp'
    if has_excluded_paths and not has_traverse_file_arg:
        suffix += "-W-EXCLS"
    return suffix


def _get_traverse_staging_dir(staging_dir: str, suffix: str) -> str:
    return os.path.join(staging_dir, f"pre-index-{suffix}/")


def write_all_filepaths_to_files(  # pylint: disable=R0913
    staging_dir: str,
    traverse_root: str,
    workers: int,
    prev_traverse: str,
    chunk_size: int,
    excluded_paths: List[str],
    traverse_file_arg: Optional[str],
) -> None:
    """Write all filepaths (rooted from `traverse_root`) to multiple files."""
    suffix = _suffix(traverse_root, bool(excluded_paths), bool(traverse_file_arg))
    traverse_staging_dir = _get_traverse_staging_dir(staging_dir, suffix)

    # traverse_staging_dir must already exist
    if not os.path.exists(traverse_staging_dir):
        raise FileNotFoundError(traverse_staging_dir)

    # output argv to a file
    with open(os.path.join(traverse_staging_dir, "argv.txt"), "w") as f:
        f.write(" ".join(sys.argv) + "\n")

    # get traverse file
    if traverse_file_arg:
        logging.info(f"Using --traverse-file {traverse_file_arg}.")
        traverse_file = traverse_file_arg
    else:
        logging.info(f"Traversing {traverse_root}...")
        traverse_file = _full_traverse(
            traverse_staging_dir, traverse_root, excluded_paths, workers
        )

    _remove_already_collected_files(prev_traverse, traverse_file)
    _chunk(traverse_staging_dir, chunk_size, traverse_file)
    _archive(staging_dir, suffix, traverse_file, dont_replace=bool(traverse_file_arg))


def _get_path_collector_log(traverse_staging_dir: str) -> str:
    return os.path.join(traverse_staging_dir, "path_collector.log")


def main() -> None:
    """Get all filepaths rooted at directory and split-up/write to files."""
    parser = get_parser_w_common_args(
        "Run this script via path_collector_make_condor.py."
    )
    parser.add_argument(
        "--staging-dir",
        dest="staging_dir",
        type=get_full_path,
        required=True,
        help="the base directory to store files for jobs, eg: /data/user/eevans/",
    )
    parser.add_argument(
        "--workers",
        type=int,
        help="max number of workers. **Ignored if also using --traverse-file**",
        required=True,
    )
    parser.add_argument(
        "--force",
        "-f",
        default=False,
        action="store_true",
        help="write over any pre-exiting *STAGING* files -- useful for condor restarts.",
    )
    args = parser.parse_args()
    # print args
    for arg, val in vars(args).items():
        print(f"{arg}: {val}")

    #
    # figure traverse_staging_dir
    suffix = _suffix(args.traverse_root, bool(args.exclude), bool(args.traverse_file))
    traverse_staging_dir = _get_traverse_staging_dir(args.staging_dir, suffix)
    if args.force:
        shutil.move(_get_path_collector_log(traverse_staging_dir), "collector.log.temp")
        shutil.rmtree(traverse_staging_dir)
        os.mkdir(traverse_staging_dir)
        shutil.move("collector.log.temp", _get_path_collector_log(traverse_staging_dir))
    elif os.path.exists(traverse_staging_dir):
        raise FileExistsError(traverse_staging_dir)
    else:
        os.mkdir(traverse_staging_dir)

    #
    # setup logging
    coloredlogs.install(level="DEBUG")
    # also log to a file -- use the formatter (and level) from coloredlogs
    fhandler = logging.FileHandler(_get_path_collector_log(traverse_staging_dir))
    fhandler.setFormatter(logging.getLogger().handlers[0].formatter)  # type: ignore[arg-type]
    logging.getLogger().addHandler(fhandler)
    # log args
    for arg, val in vars(args).items():
        logging.warning(f"{arg}: {val}")

    #
    # traverse and chunk!
    write_all_filepaths_to_files(
        args.staging_dir,
        args.traverse_root,
        args.workers,
        args.previous_traverse,
        args.chunk_size,
        args.exclude,
        args.traverse_file,
    )


if __name__ == "__main__":
    main()
