"""Data-indexing script for File Catalog."""

import argparse
import asyncio
import logging
import math
import os
import re
import stat
import string
from concurrent.futures import Future, ProcessPoolExecutor
from time import sleep
from typing import List, Optional, TypedDict

import requests

# local imports
from rest_tools.client import RestClient  # type: ignore[import]

from .metadata_manager import MetadataManager
from .utils import types

# Types --------------------------------------------------------------------------------


class RestClientArgs(TypedDict):
    """TypedDict for RestClient parameters."""

    url: str
    token: str
    timeout: int
    retries: int


class IndexerFlags(TypedDict):
    """TypedDict for Indexer bool parameters."""

    basic_only: bool
    no_patch: bool


# Constants ----------------------------------------------------------------------------


ACCEPTED_ROOTS = ["/data"]  # don't include trailing slash


# Utilities ----------------------------------------------------------------------------


def is_processable_path(path: str) -> bool:
    """Return `True` if `path` is processable.

    AKA, not a symlink, a socket, a FIFO, a device, nor char device.
    """
    mode = os.lstat(path).st_mode
    return not (
        stat.S_ISLNK(mode)
        or stat.S_ISSOCK(mode)
        or stat.S_ISFIFO(mode)
        or stat.S_ISBLK(mode)
        or stat.S_ISCHR(mode)
    )


# Functions ----------------------------------------------------------------------------


def sorted_unique_filepaths(
    file_of_filepaths: Optional[str] = None,
    list_of_filepaths: Optional[List[str]] = None,
) -> List[str]:
    """Return an aggregated, sorted, and set-unique list of filepaths.

    Read in lines from the `file_of_filepaths` file, and/or aggregate with those
    in `list_of_filepaths` list. Do not check if filepaths exist.

    Keyword Arguments:
        file_of_filepaths {Optional[str]} -- a file with a filepath on each line (default: {None})
        list_of_filepaths {Optional[List[str]]} -- a list of filepaths (default: {None})

    Returns:
        List[str] -- all unique filepaths
    """

    def convert_to_good_string(b_string: bytes) -> Optional[str]:
        # strip trailing new-line char
        if b_string[-1] == ord("\n"):
            b_string = b_string[:-1]
        # ASCII parse
        for b_char in b_string:
            if not (ord(" ") <= b_char <= ord("~")):  # pylint: disable=C0325
                logging.info(
                    f"Invalid filename, {b_string!r}, has special character(s)."
                )
                return None
        # Decode UTF-8
        try:
            path = b_string.decode("utf-8", "strict").rstrip()
        except UnicodeDecodeError as e:
            logging.info(f"Invalid filename, {b_string!r}, {e.__class__.__name__}.")
            return None
        # Non-printable chars
        if not set(path).issubset(string.printable):
            logging.info(f"Invalid filename, {path}, has non-printable character(s).")
            return None
        # all good
        return path

    filepaths = []
    if list_of_filepaths:
        filepaths.extend(list_of_filepaths)
    if file_of_filepaths:
        with open(file_of_filepaths, "rb") as bin_file:
            for bin_line in bin_file:
                path = convert_to_good_string(bin_line)
                if path:
                    filepaths.append(path)

    filepaths = [f for f in sorted(set(filepaths)) if f]
    return filepaths


async def request_post_patch(
    fc_rc: RestClient, metadata: types.Metadata, dont_patch: bool = False
) -> RestClient:
    """POST metadata, and PATCH if file is already in the file catalog."""
    try:
        _ = await fc_rc.request("POST", "/api/files", metadata)
        logging.debug("POSTed.")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            if dont_patch:
                logging.debug("File already exists, not replacing.")
            else:
                patch_path = e.response.json()["file"]  # /api/files/{uuid}
                _ = await fc_rc.request("PATCH", patch_path, metadata)
                logging.debug("PATCHed.")
        else:
            raise
    return fc_rc


async def process_file(
    filepath: str, manager: MetadataManager, fc_rc: RestClient, no_patch: bool
) -> None:
    """Gather and POST metadata for a file."""
    try:
        metadata_file = manager.new_file(filepath)
        metadata = metadata_file.generate()
    # OSError is thrown for special files like sockets
    except (OSError, PermissionError, FileNotFoundError) as e:
        logging.exception(f"{filepath} not gathered, {e.__class__.__name__}.")
        return
    except:  # noqa: E722
        logging.exception(f"Unexpected exception raised for {filepath}.")
        raise

    logging.debug(f"{filepath} gathered.")
    logging.debug(metadata)
    await request_post_patch(fc_rc, metadata, no_patch)


def fix_known_filepath_issues(filepath: str) -> Optional[List[str]]:
    """Deal with known weird quirks in filenames."""
    # split filenames that were concatenated at some point in preprocessing
    match = re.match(r"(?P<first>/data/exp/.*)(?P<second>/data/exp/.*)", filepath)
    if match:
        files = list(match.groupdict().values())
        files = sorted_unique_filepaths(list_of_filepaths=files)
        return files
    return None


async def process_paths(
    paths: List[str], manager: MetadataManager, fc_rc: RestClient, no_patch: bool
) -> List[str]:
    """POST metadata of files given by paths, and return any directories."""
    sub_files = []  # type: List[str]

    for p in paths:
        try:
            if is_processable_path(p):
                if os.path.isfile(p):
                    await process_file(p, manager, fc_rc, no_patch)
                elif os.path.isdir(p):
                    logging.debug(f"Directory found, {p}. Queuing its contents...")
                    sub_files.extend(
                        dir_entry.path
                        for dir_entry in os.scandir(p)
                        if not dir_entry.is_symlink()
                    )  # don't add symbolic links
            else:
                logging.info(f"Skipping {p}, not a directory nor file.")

        except (PermissionError, FileNotFoundError) as e:
            logging.info(f"Skipping {p}, {e.__class__.__name__}.")

        except NotADirectoryError as e:
            fixed_filepaths = fix_known_filepath_issues(p)
            if fixed_filepaths:
                paths.extend(fixed_filepaths)
                logging.info(
                    f"Fixed known issue with filepath, {p} -> {fixed_filepaths}."
                )
            else:
                logging.info(f"Skipping {p}, {e.__class__.__name__}.")

    return sub_files


def path_in_blacklist(path: str, blacklist: List[str]) -> bool:
    """Return `True` if `path` is blacklisted.

    Either:
    - `path` is in `blacklist`, or
    - `path` has a parent path in `blacklist`.
    """
    for bad_path in blacklist:
        if (path == bad_path) or (os.path.commonpath([path, bad_path]) == bad_path):
            logging.debug(
                f"Skipping {path}, file and/or directory path is blacklisted ({bad_path})."
            )
            return True
    return False


def process_work(
    paths: List[str],
    blacklist: List[str],
    rest_client_args: RestClientArgs,
    site: str,
    indexer_flags: IndexerFlags,
) -> List[str]:
    """Wrap async function, `process_paths`.

    Return files nested under any directories.
    """
    if not isinstance(paths, list):
        raise TypeError(f"`paths` object is not list {paths}")
    if not paths:
        return []

    # Check blacklist
    paths = [p for p in paths if not path_in_blacklist(p, blacklist)]

    # Process Paths
    fc_rc = RestClient(
        rest_client_args["url"],
        token=rest_client_args["token"],
        timeout=rest_client_args["timeout"],
        retries=rest_client_args["retries"],
    )
    manager = MetadataManager(site, indexer_flags["basic_only"])
    sub_files = asyncio.get_event_loop().run_until_complete(
        process_paths(paths, manager, fc_rc, indexer_flags["no_patch"])
    )

    fc_rc.close()
    return sub_files


def check_path(path: str) -> None:
    """Check if `path` is rooted at a white-listed root path."""
    for root in ACCEPTED_ROOTS:
        if root == os.path.commonpath([path, root]):
            return
    message = f"{path} is not rooted at: {', '.join(ACCEPTED_ROOTS)}"
    logging.critical(message)
    raise Exception(f"Invalid path ({message}).")


def gather_file_info(  # pylint: disable=R0913
    starting_paths: List[str],
    blacklist: List[str],
    rest_client_args: RestClientArgs,
    site: str,
    indexer_flags: IndexerFlags,
    processes: int,
) -> None:
    """Gather and post metadata from files rooted at `starting_paths`.

    Do this multi-processed.
    """
    # Get full paths
    starting_paths = [os.path.abspath(p) for p in starting_paths]
    for p in starting_paths:
        check_path(p)

    # Traverse paths and process files
    futures: List[Future] = []  # type: ignore[type-arg]
    with ProcessPoolExecutor() as pool:
        queue = starting_paths
        split = math.ceil(len(queue) / processes)
        while futures or queue:
            logging.debug(f"Queue: {len(queue)}.")
            # Divvy up queue among available worker(s). Each worker gets 1/nth of the queue.
            if queue:
                queue = sorted_unique_filepaths(list_of_filepaths=queue)
                while processes != len(futures):
                    paths, queue = queue[:split], queue[split:]
                    logging.debug(
                        f"Worker Assigned: {len(futures)+1}/{processes} ({len(paths)} paths)."
                    )
                    futures.append(
                        pool.submit(
                            process_work,
                            paths,
                            blacklist,
                            rest_client_args,
                            site,
                            indexer_flags,
                        )
                    )
            logging.debug(f"Workers: {len(futures)} {futures}.")
            # Extend the queue
            # concurrent.futures.wait(FIRST_COMPLETED) is slower
            while not futures[0].done():
                sleep(0.1)
            future = futures.pop(0)
            result = future.result()
            if result:
                queue.extend(result)
                split = math.ceil(len(queue) / processes)
            logging.debug(f"Worker finished: {future} (enqueued {len(result)}).")


def main() -> None:
    """Traverse paths, recursively, and index."""
    parser = argparse.ArgumentParser(
        description="Find files under PATH(s), compute their metadata and "
        "upload it to File Catalog.",
        epilog="Notes: (1) symbolic links are never followed.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "paths", metavar="PATHS", nargs="*", help="path(s) to scan for files."
    )
    parser.add_argument(
        "--paths-file",
        dest="paths_file",
        default=None,
        help="file containing path(s) to scan for files. (use this option for a large number of paths)",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="number of processes for multi-processing",
    )
    parser.add_argument(
        "-u",
        "--url",
        default="https://file-catalog.icecube.wisc.edu/",  # 'http://localhost:8888'
        help="File Catalog URL",
    )
    parser.add_argument(
        "-s", "--site", required=True, help='site value of the "locations" object'
    )
    parser.add_argument("-t", "--token", required=True, help="LDAP token")
    parser.add_argument(
        "--timeout", type=int, default=15, help="REST client timeout duration"
    )
    parser.add_argument(
        "--retries", type=int, default=3, help="REST client number of retries"
    )
    parser.add_argument(
        "--basic-only",
        dest="basic_only",
        default=False,
        action="store_true",
        help="only collect basic metadata",
    )
    parser.add_argument(
        "--no-patch",
        dest="no_patch",
        default=False,
        action="store_true",
        help="do not PATCH if the file already exists in the file catalog",
    )
    parser.add_argument(
        "--blacklist-file",
        dest="blacklist_file",
        help="blacklist file containing all paths to skip",
    )
    parser.add_argument("-l", "--log", default="DEBUG", help="the output logging level")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log.upper()))
    for arg, val in vars(args).items():
        logging.info(f"{arg}: {val}")

    logging.info(
        f"Collecting metadata from {args.paths} and those in file (at {args.paths_file})..."
    )

    # Aggregate and sort all paths
    paths = sorted_unique_filepaths(
        file_of_filepaths=args.paths_file, list_of_filepaths=args.paths
    )

    # Read blacklisted paths
    blacklist = sorted_unique_filepaths(file_of_filepaths=args.blacklist_file)

    # Grab and pack args
    rest_client_args = {
        "url": args.url,
        "token": args.token,
        "timeout": args.timeout,
        "retries": args.retries,
    }  # type: RestClientArgs
    indexer_flags = {
        "basic_only": args.basic_only,
        "no_patch": args.no_patch,
    }  # type: IndexerFlags

    # Go!
    gather_file_info(
        paths, blacklist, rest_client_args, args.site, indexer_flags, args.processes
    )


if __name__ == "__main__":
    main()
