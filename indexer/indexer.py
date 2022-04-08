"""Data-indexing script for File Catalog."""


import asyncio
import json
import logging
import math
import os
from concurrent.futures import Future, ProcessPoolExecutor
from time import sleep
from typing import List, Optional

import file_utils
import requests
from file_catalog.schema import types
from indexer_api.metadata_manager import MetadataManager
from rest_tools.client import RestClient  # type: ignore[import]

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

from . import defaults

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
                logging.debug("File (file-version) already exists, not patching entry.")
        else:
            raise
    return fc_rc


async def file_exists_in_fc(fc_rc: RestClient, filepath: str) -> bool:
    """Return whether the filepath is currently in the File Catalog."""
    ret = await fc_rc.request(
        "GET",
        "/api/files",
        {
            "logical_name": filepath,  # filepath may exist as multiple logical_names
            "query": json.dumps({"locations.path": filepath}),
        },
    )
    # NOTE - if there is no response, it's still possible this file-version exists in FC
    # See: https://github.com/WIPACrepo/file-catalog-indexer/tree/master#re-indexing-files-is-tricky-two-scenarios
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
            if file_utils.is_processable_path(p):
                if os.path.isfile(p):
                    await index_file(p, manager, fc_rc, patch, dryrun)
                elif os.path.isdir(p):
                    logging.debug(f"Directory found, {p}. Queuing its contents...")
                    child_paths.extend(file_utils.get_subpaths(p))
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
        if bad_path == file_utils.commonpath([path, bad_path]):
            logging.debug(
                f"Skipping {path}, file and/or directory path is blacklisted ({bad_path})."
            )
            return True
    return False


# Indexing-Wrapper Functions --------------------------------------------------


def _index(
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
    paths = file_utils.sorted_unique_filepaths(list_of_filepaths=paths)
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


def _recursively_index_multiprocessed(  # pylint: disable=R0913
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
                queue = file_utils.sorted_unique_filepaths(list_of_filepaths=queue)
                while processes != len(futures):
                    paths, queue = queue[:split], queue[split:]
                    logging.debug(
                        f"Worker Assigned: {len(futures)+1}/{processes} ({len(paths)} paths)."
                    )
                    futures.append(
                        pool.submit(
                            _index,
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


def _recursively_index(  # pylint: disable=R0913
    starting_paths: List[str],
    blacklist: List[str],
    rest_client_args: RestClientArgs,
    site: str,
    indexer_flags: IndexerFlags,
    processes: int,
) -> None:
    """Gather and post metadata from files rooted at `starting_paths`."""
    if processes > 1:
        _recursively_index_multiprocessed(
            starting_paths, blacklist, rest_client_args, site, indexer_flags, processes
        )
    else:
        queue = starting_paths
        i = 0
        while queue:
            logging.debug(f"Queue Iteration #{i}")
            queue = _index(queue, blacklist, rest_client_args, site, indexer_flags)
            i += 1


# Main ---------------------------------------------------------------------------------


def validate_path(path: str) -> None:
    """Check if `path` is rooted at a white-listed root path."""
    for root in ACCEPTED_ROOTS:
        if root == file_utils.commonpath([path, root]):
            return
    message = f"{path} is not rooted at: {', '.join(ACCEPTED_ROOTS)}"
    logging.critical(message)
    raise Exception(f"Invalid path ({message}).")


def index(
    token: str,
    site: str,
    paths: Optional[List[str]] = defaults.PATHS,
    paths_file: str = defaults.PATHS_FILE,
    blacklist: Optional[List[str]] = defaults.BLACKLIST,
    blacklist_file: str = defaults.BLACKLIST_FILE,
    url: str = defaults.URL,
    timeout: int = defaults.TIMEOUT,
    retries: int = defaults.RETRIES,
    basic_only: bool = defaults.BASIC_ONLY,
    patch: bool = defaults.PATCH,
    iceprodv2_rc_token: str = defaults.ICEPRODV2_RC_TOKEN,
    iceprodv1_db_pass: str = defaults.ICEPRODV1_DB_PASS,
    dryrun: bool = defaults.DRYRUN,
    non_recursive: bool = defaults.NON_RECURSIVE,
    processes: int = defaults.PROCESSES,
) -> None:
    """Traverse paths, recursively, and index."""

    logging.info(
        f"Collecting metadata from {paths} and those in file (at {paths_file})..."
    )

    # Aggregate, sort, and validate filepaths
    paths = file_utils.sorted_unique_filepaths(
        file_of_filepaths=paths_file, list_of_filepaths=paths, abspaths=True
    )
    for p in paths:  # pylint: disable=C0103
        validate_path(p)

    # Aggregate & sort blacklisted paths
    blacklist = file_utils.sorted_unique_filepaths(
        file_of_filepaths=blacklist_file,
        list_of_filepaths=blacklist,
        abspaths=True,
    )

    # Grab and pack args
    rest_client_args: RestClientArgs = {
        "url": url,
        "token": token,
        "timeout": timeout,
        "retries": retries,
    }
    indexer_flags: IndexerFlags = {
        "basic_only": basic_only,
        "patch": patch,
        "iceprodv2_rc_token": iceprodv2_rc_token,
        "iceprodv1_db_pass": iceprodv1_db_pass,
        "dryrun": dryrun,
    }

    # Go!
    if non_recursive:
        _index(paths, blacklist, rest_client_args, site, indexer_flags)
    else:
        _recursively_index(
            paths, blacklist, rest_client_args, site, indexer_flags, processes
        )
