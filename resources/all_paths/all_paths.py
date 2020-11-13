"""Traverse given directory for all filepaths, and split list into chunks.

These chunks are outputted files, which are used as input in
indexer_make_dag.py jobs.
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime as dt
from typing import List, Union


def _full_path(path: str) -> str:
    if not path:
        return path

    full_path = os.path.abspath(path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(full_path)

    return full_path


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


def _split(traverse_staging_dir: str, paths_per_file: int, traverse_file: str) -> None:
    """Split the file into n files."""
    dir_split = os.path.join(traverse_staging_dir, "paths/")

    check_call_print(f"mkdir {dir_split}".split())
    # TODO - split by quota
    check_call_print(
        f"split -l{paths_per_file} {traverse_file} paths_file_".split(), cwd=dir_split
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
    paths_per_file: int,
    excluded_paths: List[str],
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
            f.write(" ".join(sys.argv))

        traverse_file = _full_traverse(
            traverse_staging_dir, traverse_root, excluded_paths, workers
        )
        _remove_already_collected_files(prev_traverse, traverse_file)
        _split(traverse_staging_dir, paths_per_file, traverse_file)
        _archive(staging_dir, name, traverse_file)

    else:
        print(
            f"Writing Bypassed: {traverse_staging_dir} already exists. Use preexisting files."
        )


def main() -> None:
    """Get all filepaths rooted at directory and split-up/write to files."""
    parser = argparse.ArgumentParser(
        description="Run this script via all_paths_make_condor.py."
    )
    parser.add_argument(
        "traverse_root", help="root directory to traverse for files.", type=_full_path,
    )
    parser.add_argument(
        "--staging-dir",
        dest="staging_dir",
        type=_full_path,
        required=True,
        help="the base directory to store files for jobs, eg: /data/user/eevans/",
    )
    parser.add_argument(
        "--previous-traverse",
        dest="prev_traverse",
        type=_full_path,
        help="prior file with filepaths, eg: /data/user/eevans/data-exp-2020-03-10T15:11:42."
        " These files will be skipped.",
    )
    parser.add_argument(
        "--exclude",
        "-e",
        nargs="*",
        default=[],
        type=_full_path,
        help='directories/paths to exclude from the traverse -- keep it short, this is "all paths" after all.',
    )
    parser.add_argument(
        "--workers", type=int, help="max number of workers", required=True
    )
    parser.add_argument(
        "--paths-per-file",
        dest="paths_per_file",
        type=int,
        default=1000,
        help="number of paths per file/job",
    )
    args = parser.parse_args()

    for arg, val in vars(args).items():
        print(f"{arg}: {val}")

    write_all_filepaths_to_files(
        args.staging_dir,
        args.traverse_root,
        args.workers,
        args.prev_traverse,
        args.paths_per_file,
        args.exclude,
    )


if __name__ == "__main__":
    main()
