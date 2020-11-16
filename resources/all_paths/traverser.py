"""Traverse directory paths, and print all filepaths."""

import logging
import os
import stat
import sys
from concurrent.futures import Future, ProcessPoolExecutor
from time import sleep
from typing import List, Tuple

sys.path.append(".")
from common_args import (  # isort:skip  # noqa # pylint: disable=E0401,C0413,C0411
    get_parser_w_common_args,
)


def is_excluded_path(path: str, excluded_paths: List[str]) -> bool:
    """Return `True` if `path` should be excluded.

    Either:
    - `path` is in `excluded_paths`, or
    - `path` has a parent path in `excluded_paths`.
    """
    for excl in excluded_paths:
        if (path == excl) or (os.path.commonpath([path, excl]) == excl):
            logging.debug(
                f"Skipping {path}, file and/or directory path is in `--exclude` ({excl})."
            )
            return True
    return False


def traverse(path: str, excluded_paths: List[str]) -> Tuple[List[str], int]:
    """Print out file paths and return sub-directories."""
    try:
        scan = os.scandir(path)
    except (PermissionError, FileNotFoundError):
        scan = []  # type: ignore[assignment]
    dirs = []

    all_file_count = 0
    for dir_entry in scan:
        try:
            mode = os.lstat(dir_entry.path).st_mode
            if (
                stat.S_ISLNK(mode)
                or stat.S_ISSOCK(mode)
                or stat.S_ISFIFO(mode)
                or stat.S_ISBLK(mode)
                or stat.S_ISCHR(mode)
            ):
                logging.info(f"Non-processable file: {dir_entry.path}")
                continue
        except PermissionError:
            logging.info(f"Permission denied: {dir_entry.path}")
            continue

        if is_excluded_path(path, excluded_paths):
            continue

        # append if it's a directory
        if dir_entry.is_dir():
            dirs.append(dir_entry.path)
        # print if it's a good file
        elif dir_entry.is_file():
            all_file_count = all_file_count + 1
            if not dir_entry.path.strip():
                logging.info(f"Blank file name in: {os.path.dirname(dir_entry.path)}")
            else:
                try:
                    print(dir_entry.path)
                except UnicodeEncodeError:
                    logging.info(
                        f"Invalid file name in: {os.path.dirname(dir_entry.path)}"
                    )

    return dirs, all_file_count


def main() -> None:
    """Recursively scan directory paths and print all file paths."""
    parser = get_parser_w_common_args(
        "Traverse directories under PATH(s) and print each filepath.",
        only=["--exclude"],
    )
    parser.add_argument(
        "--workers", type=int, help="max number of workers", required=True
    )
    args = parser.parse_args()

    dirs = args.paths
    futures: List[Future] = []  # type: ignore[type-arg]
    all_file_count = 0
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        while futures or dirs:
            # submit process job
            futures.extend(pool.submit(traverse, d, args.exclude) for d in dirs)
            # wait
            while not futures[0].done():
                sleep(0.1)
            # cleanup and prep for next job
            future = futures.pop(0)
            dirs, result_all_file_count = future.result()
            all_file_count = all_file_count + result_all_file_count

    logging.info(f"File Count: {all_file_count}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
