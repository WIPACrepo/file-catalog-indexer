"""Data-indexing script for File Catalog."""

import argparse
import asyncio
import logging
import math
import os
import stat
import string
from concurrent.futures import Future, ProcessPoolExecutor
from time import sleep
from typing import List, Optional

import coloredlogs  # type: ignore[import]
import requests
from file_catalog.schema import types
from rest_tools.client import RestClient  # type: ignore[import]

# local imports
from indexer_api.metadata_manager import MetadataManager

try:
    from typing import Final, TypedDict
except ImportError:
    from typing_extensions import Final, TypedDict  # type: ignore[misc]


_DEFAULT_TIMEOUT: Final[int] = 30  # seconds
_AGGREGATE_LATENCY_MINUTES: Final[int] = 30  # minutes
_DEFAULT_RETRIES: Final[int] = int((60 / _DEFAULT_TIMEOUT) * _AGGREGATE_LATENCY_MINUTES)


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
    patch: bool
    iceprodv2_rc_token: str
    iceprodv1_db_pass: str
    dryrun: bool


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


def sorted_unique_filepaths(
    file_of_filepaths: Optional[str] = None,
    list_of_filepaths: Optional[List[str]] = None,
    abspaths: bool = False,
) -> List[str]:
    """Return an aggregated, sorted, and set-unique list of filepaths.

    Read in lines from the `file_of_filepaths` file, and/or aggregate with those
    in `list_of_filepaths` list. Do not check if filepaths exist.

    Keyword Arguments:
        file_of_filepaths -- a file with a filepath on each line
        list_of_filepaths -- a list of filepaths
        abspaths -- call `os.path.abspath()` on each filepath

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

    if abspaths:
        filepaths = [os.path.abspath(p) for p in filepaths]
    filepaths = [f for f in sorted(set(filepaths)) if f]
    return filepaths


# Indexing Functions -------------------------------------------------------------------


async def post_metadata(
    fc_rc: RestClient,
    metadata: types.Metadata,
    patch: bool = False,
    dryrun: bool = False,
) -> RestClient:
    """POST metadata, and PATCH if file is already in the file catalog."""
    if dryrun:
        logging.warning(f"Dry-Run Enabled: Not POSTing to File Catalog! {metadata}")
        sleep(0.1)
        return fc_rc

    try:
        _ = await fc_rc.request("POST", "/api/files", metadata)
        logging.debug("POSTed.")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            if patch:
                patch_path = e.response.json()["file"]  # /api/files/{uuid}
                _ = await fc_rc.request("PATCH", patch_path, metadata)
                logging.debug("PATCHed.")
            else:
                logging.debug("File already exists, not patching entry.")
        else:
            raise
    return fc_rc


async def file_exists_in_fc(fc_rc: RestClient, filepath: str) -> bool:
    """Return whether the filepath is currently in the File Catalog."""
    ret = await fc_rc.request("GET", "/api/files", {"path": filepath})
    return bool(ret["files"])


async def index_file(
    filepath: str,
    manager: MetadataManager,
    fc_rc: RestClient,
    patch: bool,
    dryrun: bool,
) -> None:
    """Gather and POST metadata for a file."""
    if not patch and await file_exists_in_fc(fc_rc, filepath):
        logging.info(
            f"File already exists in the File Catalog (use --patch to overwrite); "
            f"skipping ({filepath})"
        )
        return

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
    await post_metadata(fc_rc, metadata, patch, dryrun)


async def index_paths(
    paths: List[str],
    manager: MetadataManager,
    fc_rc: RestClient,
    patch: bool,
    dryrun: bool,
) -> List[str]:
    """POST metadata of files given by paths, and return all child paths."""
    child_paths: List[str] = []

    for p in paths:  # pylint: disable=C0103
        try:
            if is_processable_path(p):
                if os.path.isfile(p):
                    await index_file(p, manager, fc_rc, patch, dryrun)
                elif os.path.isdir(p):
                    logging.debug(f"Directory found, {p}. Queuing its contents...")
                    child_paths.extend(
                        dir_entry.path
                        for dir_entry in os.scandir(p)
                        if not dir_entry.is_symlink()
                    )  # don't add symbolic links
            else:
                logging.info(f"Skipping {p}, not a directory nor file.")

        except (PermissionError, FileNotFoundError, NotADirectoryError) as e:
            logging.info(f"Skipping {p}, {e.__class__.__name__}.")

    return child_paths


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


def index(
    paths: List[str],
    blacklist: List[str],
    rest_client_args: RestClientArgs,
    site: str,
    indexer_flags: IndexerFlags,
) -> List[str]:
    """Index paths, excluding any matching the blacklist.

    Return all child paths nested under any directories.
    """
    if not isinstance(paths, list):
        raise TypeError(f"`paths` object is not list {paths}")
    if not paths:
        return []

    # Filter
    paths = sorted_unique_filepaths(list_of_filepaths=paths)
    paths = [p for p in paths if not path_in_blacklist(p, blacklist)]

    # Prep
    fc_rc = RestClient(
        rest_client_args["url"],
        token=rest_client_args["token"],
        timeout=rest_client_args["timeout"],
        retries=rest_client_args["retries"],
    )
    manager = MetadataManager(
        site,
        basic_only=indexer_flags["basic_only"],
        iceprodv2_rc_token=indexer_flags["iceprodv2_rc_token"],
        iceprodv1_db_pass=indexer_flags["iceprodv1_db_pass"],
    )

    # Index
    child_paths = asyncio.get_event_loop().run_until_complete(
        index_paths(
            paths, manager, fc_rc, indexer_flags["patch"], indexer_flags["dryrun"]
        )
    )

    fc_rc.close()
    return child_paths


# Recursively-Indexing Functions -------------------------------------------------------


def recursively_index_multiprocessed(  # pylint: disable=R0913
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
                            index,
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


def recursively_index(  # pylint: disable=R0913
    starting_paths: List[str],
    blacklist: List[str],
    rest_client_args: RestClientArgs,
    site: str,
    indexer_flags: IndexerFlags,
    processes: int,
) -> None:
    """Gather and post metadata from files rooted at `starting_paths`."""
    if processes > 1:
        recursively_index_multiprocessed(
            starting_paths, blacklist, rest_client_args, site, indexer_flags, processes
        )
    else:
        queue = starting_paths
        i = 0
        while queue:
            logging.debug(f"Queue Iteration #{i}")
            queue = index(queue, blacklist, rest_client_args, site, indexer_flags)
            i += 1


# Main ---------------------------------------------------------------------------------


def validate_path(path: str) -> None:
    """Check if `path` is rooted at a white-listed root path."""
    for root in ACCEPTED_ROOTS:
        if root == os.path.commonpath([path, root]):
            return
    message = f"{path} is not rooted at: {', '.join(ACCEPTED_ROOTS)}"
    logging.critical(message)
    raise Exception(f"Invalid path ({message}).")


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
        default=None,
        help="file containing path(s) to scan for files. "
        "(use this option for a large number of paths)",
    )
    parser.add_argument(
        "-n",
        "--non-recursive",
        default=False,
        action="store_true",
        help="do not recursively index / do not descend into subdirectories",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="number of processes for multi-processing "
        "(ignored if using --non-recursive)",
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
    parser.add_argument(
        "-t", "--token", required=True, help="REST token for File Catalog"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=_DEFAULT_RETRIES,
        help="timeout duration (seconds) for File Catalog REST requests",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=_DEFAULT_TIMEOUT,
        help="number of retries for File Catalog REST requests",
    )
    parser.add_argument(
        "--basic-only",
        default=False,
        action="store_true",
        help="only collect basic metadata",
    )
    parser.add_argument(
        "--patch",
        default=False,
        action="store_true",
        help="replace/overwrite any existing File-Catalog entries (aka patch)",
    )
    parser.add_argument(
        "--blacklist",
        metavar="BLACKPATH",
        nargs="+",
        default=None,
        help="list of blacklisted filepaths; Ex: /foo/bar/ will skip /foo/bar/*",
    )
    parser.add_argument(
        "--blacklist-file",
        help="a file containing blacklisted filepaths on each line "
        "(this is a useful alternative to `--blacklist` when there's many blacklisted paths); "
        "Ex: /foo/bar/ will skip /foo/bar/*",
    )
    parser.add_argument("-l", "--log", default="INFO", help="the output logging level")
    parser.add_argument("--iceprodv2-rc-token", default="", help="IceProd2 REST token")
    parser.add_argument("--iceprodv1-db-pass", default="", help="IceProd1 SQL password")
    parser.add_argument(
        "--dryrun",
        default=False,
        action="store_true",
        help="do everything except POSTing/PATCHing to the File Catalog",
    )

    args = parser.parse_args()
    coloredlogs.install(level=args.log.upper())
    for arg, val in vars(args).items():
        logging.warning(f"{arg}: {val}")

    logging.info(
        f"Collecting metadata from {args.paths} and those in file (at {args.paths_file})..."
    )

    # Aggregate, sort, and validate filepaths
    paths = sorted_unique_filepaths(
        file_of_filepaths=args.paths_file, list_of_filepaths=args.paths, abspaths=True
    )
    for p in paths:  # pylint: disable=C0103
        validate_path(p)

    # Aggregate & sort blacklisted paths
    blacklist = sorted_unique_filepaths(
        file_of_filepaths=args.blacklist_file,
        list_of_filepaths=args.blacklist,
        abspaths=True,
    )

    # Grab and pack args
    rest_client_args: RestClientArgs = {
        "url": args.url,
        "token": args.token,
        "timeout": args.timeout,
        "retries": args.retries,
    }
    indexer_flags: IndexerFlags = {
        "basic_only": args.basic_only,
        "patch": args.patch,
        "iceprodv2_rc_token": args.iceprodv2_rc_token,
        "iceprodv1_db_pass": args.iceprodv1_db_pass,
        "dryrun": args.dryrun,
    }

    # Go!
    if args.non_recursive:
        index(paths, blacklist, rest_client_args, args.site, indexer_flags)
    else:
        recursively_index(
            paths, blacklist, rest_client_args, args.site, indexer_flags, args.processes
        )


if __name__ == "__main__":
    main()
