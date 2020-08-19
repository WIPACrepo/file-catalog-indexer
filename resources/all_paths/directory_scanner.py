"""Recursively scan directory paths and print all file paths."""

import argparse
import logging
import os
import stat
from concurrent.futures import Future, ProcessPoolExecutor
from time import sleep
from typing import List, Tuple


def _full_path(path: str) -> str:
    if not path:
        return path

    full_path = os.path.abspath(path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(full_path)

    return full_path


def exclude_path(path: str, exclude: List[str]) -> bool:
    """Return `True` if `path` should be excluded.

    Either:
    - `path` is in `exclude`, or
    - `path` has a parent path in `exclude`.
    """
    for bad_path in exclude:
        if (path == bad_path) or (os.path.commonpath([path, bad_path]) == bad_path):
            logging.debug(
                f"Skipping {path}, file and/or directory path is in `exclude` ({bad_path})."
            )
            return True
    return False


def process_dir(path: str, exclude: List[str]) -> Tuple[List[str], int]:
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

        # check `exclude` paths
        if exclude_path(path, exclude):
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
    parser = argparse.ArgumentParser(
        description="Find directories under PATH(s)",
        epilog="Notes: (1) symbolic links are never followed.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "paths", metavar="PATH", nargs="+", type=_full_path, help="path(s) to scan."
    )
    parser.add_argument(
        "--exclude",
        "-e",
        nargs="*",
        default=[],
        type=_full_path,
        help="directories/paths to exclude from the traverse.",
    )
    parser.add_argument(
        "--workers", type=int, help="max number of workers", required=True
    )
    args = parser.parse_args()

    dirs = args.paths
    futures = []  # type: List[Future]  # type: ignore[type-arg]
    all_file_count = 0
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        while futures or dirs:
            # submit process job
            futures.extend(pool.submit(process_dir, d, args.exclude) for d in dirs)
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
