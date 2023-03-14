"""Traverse directory paths, and print all filepaths."""

import logging
import os
import stat
from concurrent.futures import Future, ProcessPoolExecutor
from time import sleep
from typing import List, Tuple

import coloredlogs  # type: ignore[import]

from resources.path_collector.common_args import (  # isort:skip  # noqa # pylint: disable=E0401,C0413,C0411
    get_parser_w_common_args,
    get_full_path,
)


def is_excluded_path(path: str, excluded_paths: List[str]) -> bool:
    """Return `True` if `path` should be excluded.

    Either:
    - `path` is in `excluded_paths`, or
    - `path` has a parent path in `excluded_paths`.
    """
    for excl in excluded_paths:
        if (path == excl) or (os.path.commonpath([path, excl]) == excl):
            logging.info(
                f"Skipping {path}, file and/or directory path is in `--exclude` ({excl})."
            )
            return True
    return False


def scan_directory(path: str, excluded_paths: List[str]) -> Tuple[List[str], List[str]]:
    """Return sub-directories' paths and regular-file's paths.

    Ignore all other file types.
    """
    logging.debug(f"Scanning directory: {path}...")

    try:
        scan = os.scandir(path)
    except (PermissionError, FileNotFoundError):
        scan = []  # type: ignore[assignment]

    subdirs = []
    filepaths = []
    for dir_entry in scan:
        try:
            mode = os.lstat(dir_entry.path).st_mode
            if (
                stat.S_ISLNK(mode)
                or stat.S_ISSOCK(mode)  # noqa: W503
                or stat.S_ISFIFO(mode)  # noqa: W503
                or stat.S_ISBLK(mode)  # noqa: W503
                or stat.S_ISCHR(mode)  # noqa: W503
            ):
                logging.info(f"Non-processable file: {dir_entry.path}")
                continue
        except PermissionError:
            logging.info(f"Permission denied: {dir_entry.path}")
            continue

        if is_excluded_path(dir_entry.path, excluded_paths):
            continue

        # append if it's a directory
        if dir_entry.is_dir():
            subdirs.append(dir_entry.path)
        # print if it's a good file
        elif dir_entry.is_file():
            if not dir_entry.path.strip():
                logging.info(f"Blank file name in: {os.path.dirname(dir_entry.path)}")
            else:
                filepaths.append(dir_entry.path)

    logging.debug(f"Scan finished, directory: {path}")
    return subdirs, filepaths


def main() -> None:
    """Recursively scan directory paths and print all file paths."""
    parser = get_parser_w_common_args(
        "Traverse directories under PATH(s) and print each filepath.",
        only=["--exclude"],
    )
    parser.add_argument(
        "paths", metavar="PATH", nargs="+", type=get_full_path, help="path(s) to scan."
    )
    parser.add_argument(
        "--workers", type=int, help="max number of workers", required=True
    )
    args = parser.parse_args()

    dirs = args.paths
    futures: List[Future[Tuple[List[str], List[str]]]] = []  # pylint: disable=E1136
    all_file_count = 0
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        while futures or dirs:
            # submit directory-paths for scanning
            for dir_path in dirs:
                logging.debug(f"Submitting directory: {dir_path}...")
                futures.append(pool.submit(scan_directory, dir_path, args.exclude))
            # get next finished future
            while True:
                try:
                    fin_future = next(f for f in futures if f.done())
                    futures.remove(fin_future)
                    break
                except StopIteration:  # there were no finished futures
                    sleep(0.1)
            # grab subdirectories for traversing and print filepaths
            dirs, filepaths = fin_future.result()
            result_file_count = 0
            for fpath in filepaths:
                try:
                    print(fpath)
                    result_file_count += 1
                except UnicodeEncodeError:
                    logging.info(f"Invalid file name in: {os.path.dirname(fpath)}")
            all_file_count += result_file_count

    logging.info(f"File Count: {all_file_count}")


if __name__ == "__main__":
    coloredlogs.install(level="DEBUG")
    # logging.basicConfig(level=logging.DEBUG)
    main()
