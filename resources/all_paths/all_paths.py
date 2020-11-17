"""Traverse given directory for all filepaths, and split list into chunks.

These chunks are outputted files, which are used as input in
indexer_make_dag.py jobs.
"""

import os
import subprocess
import sys
from datetime import datetime as dt
from typing import List, Optional, Union

import bitmath  # type: ignore[import]

sys.path.append(".")
from common_args import (  # isort:skip  # noqa # pylint: disable=E0401,C0413,C0411
    get_parser_w_common_args,
    get_full_path,
)


def check_call_print(
    cmd: Union[List[str], str], cwd: str = ".", shell: bool = False
) -> None:
    """Wrap subprocess.check_call and print command."""
    if shell and isinstance(cmd, list):
        raise Exception("Do not set shell=True and pass a list--pass a string.")
    print(f"Execute: {cmd} @ {cwd}")
    subprocess.check_call(cmd, cwd=cwd, shell=shell)


def _full_traverse(
    traverse_staging_dir: str,
    traverse_root: str,
    excluded_paths: List[str],
    workers: int,
) -> str:
    """Get all filepaths in traverse_root and sort the list."""
    file_orig = os.path.join(traverse_staging_dir, "paths.orig")
    traverse_file = os.path.join(traverse_staging_dir, "paths.sort")
    file_log = os.path.join(traverse_staging_dir, "paths.log")

    exculdes_args = ""
    if excluded_paths:
        exculdes_args = "--exclude " + " ".join(excluded_paths)
    check_call_print(
        f"python traverser.py {traverse_root} --workers {workers} {exculdes_args} > {file_orig} 2> {file_log}",
        shell=True,
    )
    check_call_print(
        f"""sed -i '/^[[:space:]]*$/d' {file_orig}""", shell=True
    )  # remove blanks
    check_call_print(
        f"sort -T {traverse_staging_dir} {file_orig} > {traverse_file}", shell=True
    )
    check_call_print(f"rm {file_orig}".split())  # Cleanup

    return traverse_file


def _remove_already_collected_files(prev_traverse: str, traverse_file: str) -> None:
    """Get lines(filepaths) unique to this traverse versus the previous."""
    if prev_traverse:
        check_call_print(
            f"comm -1 -3 {prev_traverse} {traverse_file} > {traverse_file}.unique",
            shell=True,
        )
        check_call_print(f"mv {traverse_file}.unique {traverse_file}".split())


def _chunk(traverse_staging_dir: str, chunk_size: int, traverse_file: str) -> None:
    """Chunk the traverse file up by approx equal aggregate file size.

    Assumes: `chunk_size` >> any one file's size

    Chunks are guaranteed to be equal to or barely greater than
    `chunk_size`. If `chunk_size` is too small (< `MINIMUM_CHUNK_SIZE`),
    only one chunk is made ("chunk-0"), a copy of `traverse_file`.

    Example:
    `traverse_staging_dir/paths/chunk-1645`
    """
    dir_ = os.path.join(traverse_staging_dir, "paths/")

    check_call_print(f"mkdir {dir_}".split())

    if chunk_size == 0:
        print("Chunking bypassed, --chunk-size is zero")
        check_call_print(f"cp {traverse_file} {os.path.join(dir_, 'chunk-0')}".split())
        return

    def _chunk_it(i: int, chunk_lines: List[str]) -> str:
        filename = f"chunk-{i}"
        with open(os.path.join(dir_, filename), "w") as chunk_f:
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

    print(
        f"Chunked traverse into {_id} chunk-files"
        f" ~{bitmath.best_prefix(chunk_size).format('{value:.2f} {unit}')}"
        f" ({chunk_size} bytes) each @ {dir_}."
        f" Total ~{bitmath.best_prefix(total_f_size).format('{value:.2f} {unit}')}."
    )


def _archive(staging_dir: str, name: str, traverse_file: str) -> None:
    """Copy/Archive traverse into a file.

    Example:
    /data/user/eevans/data-exp-2020-03-10T15:11:42
    """
    time = dt.now().isoformat(timespec="seconds")
    file_archive = os.path.join(staging_dir, f"{name}-{time}")
    check_call_print(f"mv {traverse_file} {file_archive}".split())
    print(f"Archive File: at {file_archive}")


def write_all_filepaths_to_files(  # pylint: disable=R0913
    staging_dir: str,
    traverse_root: str,
    workers: int,
    prev_traverse: str,
    chunk_size: int,
    excluded_paths: List[str],
    traverse_file: Optional[str],
) -> None:
    """Write all filepaths (rooted from `traverse_root`) to multiple files."""
    name = traverse_root.strip("/").replace("/", "-")  # Ex: 'data-exp'
    if excluded_paths:
        name += "-W-EXCLS"

    traverse_staging_dir = os.path.join(staging_dir, f"indexer-{name}/")

    if not os.path.exists(traverse_staging_dir):
        check_call_print(f"mkdir {traverse_staging_dir}".split())

        # output argv to a file
        with open(os.path.join(traverse_staging_dir, "argv.txt"), "w") as f:
            f.write(" ".join(sys.argv) + "\n")

        if not traverse_file:
            traverse_file = _full_traverse(
                traverse_staging_dir, traverse_root, excluded_paths, workers
            )
        _remove_already_collected_files(prev_traverse, traverse_file)
        _chunk(traverse_staging_dir, chunk_size, traverse_file)
        _archive(staging_dir, name, traverse_file)

    else:
        print(
            f"Writing Bypassed: {traverse_staging_dir} already exists. Use preexisting files."
        )


def main() -> None:
    """Get all filepaths rooted at directory and split-up/write to files."""
    parser = get_parser_w_common_args("Run this script via all_paths_make_condor.py.")
    parser.add_argument(
        "--staging-dir",
        dest="staging_dir",
        type=get_full_path,
        required=True,
        help="the base directory to store files for jobs, eg: /data/user/eevans/",
    )
    parser.add_argument(
        "--workers", type=int, help="max number of workers", required=True
    )
    args = parser.parse_args()

    for arg, val in vars(args).items():
        print(f"{arg}: {val}")

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
