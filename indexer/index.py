#!/usr/bin/env python3
# index.py
"""Data-indexing script for File Catalog."""

import argparse
import asyncio
from concurrent.futures import Future, ProcessPoolExecutor
from functools import partial
import json
import logging
import math
import os
from time import sleep
from typing import Any, Callable, cast, Dict, List, TypedDict

import requests
from file_catalog.schema import types
from rest_tools.client import RestClient
from wipac_dev_tools import logging_tools

from indexer import defaults
from indexer.client_auth import (
    add_auth_to_argparse,
    create_file_catalog_rest_client,
    create_oauth_config,
    create_rest_config,
)
from indexer.config import IndexerConfiguration, OAuthConfiguration, RestConfiguration
from indexer.metadata_manager import MetadataManager
from indexer.utils import file_utils

# Types --------------------------------------------------------------------------------


class IndexerFlags(TypedDict):
    # only post basic metadata
    basic_only: bool
    # do everything except POSTing/PATCHing to the File Catalog
    dryrun: bool
    # replace/overwrite any existing File-Catalog entries (aka PATCH)
    patch: bool


# Constants ----------------------------------------------------------------------------


ACCEPTED_ROOTS = ["/data"]  # don't include trailing slash


# Indexing Functions -------------------------------------------------------------------


async def _post_metadata(
    fc_rc: RestClient,
    metadata: types.Metadata,
    patch: bool,
    dryrun: bool,
) -> None:
    """POST metadata, and PATCH if file is already in the file catalog."""
    if dryrun:
        logging.warning(f"Dry-Run Enabled: Not POSTing to File Catalog! {metadata}")
        sleep(0.1)

    try:
        await fc_rc.request("POST", "/api/files", cast(Dict[str, Any], metadata))
        logging.debug("POSTed.")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            if patch:
                patch_path = e.response.json()["file"]  # /api/files/{uuid}
                await fc_rc.request("PATCH", patch_path, cast(Dict[str, Any], metadata))
                logging.debug("PATCHed.")
            else:
                logging.debug("File (file-version) already exists, not patching entry.")
        else:
            raise


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
    await _post_metadata(fc_rc, metadata, patch, dryrun)


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


def path_in_denylist(path: str, denylist: List[str]) -> bool:
    """Return `True` if `path` is denylisted.

    Either:
    - `path` is in `denylist`, or
    - `path` has a parent path in `denylist`.
    """
    for bad_path in denylist:
        if bad_path == file_utils.commonpath([path, bad_path]):
            logging.debug(
                f"Skipping {path}, file and/or directory path is denylisted ({bad_path})."
            )
            return True
    return False


# Indexing-Wrapper Functions --------------------------------------------------


def _index(
    paths: List[str],
    denylist: List[str],
    fc_rc_creator: Callable[..., RestClient],
    manager_creator: Callable[..., MetadataManager],
    indexer_flags: IndexerFlags,
) -> List[str]:
    """Index paths, excluding any matching the denylist.

    Return all child paths nested under any directories.
    """
    if not isinstance(paths, list):
        raise TypeError(f"`paths` object is not list {paths}")
    if not paths:
        return []

    # Make FC Rest Client and IceProd Manager
    fc_rc = fc_rc_creator()
    manager = manager_creator()

    # Filter
    paths = file_utils.sorted_unique_filepaths(list_of_filepaths=paths)
    paths = [p for p in paths if not path_in_denylist(p, denylist)]

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
    denylist: List[str],
    fc_rc_creator: Callable[..., RestClient],
    manager_creator: Callable[..., MetadataManager],
    indexer_flags: IndexerFlags,
    n_processes: int,
) -> None:
    """Gather and post metadata from files rooted at `starting_paths`.

    Do this multi-processed.
    """
    # Traverse paths and process files
    futures: List[Future[List[str]]] = []
    with ProcessPoolExecutor() as pool:
        queue = starting_paths
        split = math.ceil(len(queue) / n_processes)
        while futures or queue:
            logging.debug(f"Queue: {len(queue)}.")
            # Divvy up queue among available worker(s). Each worker gets 1/nth of the queue.
            if queue:
                queue = file_utils.sorted_unique_filepaths(list_of_filepaths=queue)
                while n_processes != len(futures):
                    paths, queue = queue[:split], queue[split:]
                    logging.debug(
                        f"Worker Assigned: {len(futures)+1}/{n_processes} ({len(paths)} paths)."
                    )
                    futures.append(
                        pool.submit(
                            _index,
                            paths,
                            denylist,
                            fc_rc_creator,
                            manager_creator,
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
                split = math.ceil(len(queue) / n_processes)
            logging.debug(f"Worker finished: {future} (enqueued {len(result)}).")


def _recursively_index(  # pylint: disable=R0913
    starting_paths: List[str],
    denylist: List[str],
    fc_rc_creator: Callable[..., RestClient],
    manager_creator: Callable[..., MetadataManager],
    indexer_flags: IndexerFlags,
    n_processes: int,
) -> None:
    """Gather and post metadata from files rooted at `starting_paths`."""
    if n_processes > 1:
        _recursively_index_multiprocessed(
            starting_paths,
            denylist,
            fc_rc_creator,
            manager_creator,
            indexer_flags,
            n_processes,
        )
    else:
        queue = starting_paths
        i = 0
        while queue:
            logging.debug(f"Queue Iteration #{i}")
            queue = _index(queue, denylist, fc_rc_creator, manager_creator, indexer_flags)
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


def index(index_config: IndexerConfiguration,
          oauth_config: OAuthConfiguration,
          rest_config: RestConfiguration) -> None:
    """Traverse paths and index."""
    basic_only = index_config["basic_only"]
    denylist = index_config["denylist"]
    denylist_file = index_config["denylist_file"]
    dryrun = index_config["dryrun"]
    n_processes = index_config["n_processes"]
    non_recursive = index_config["non_recursive"]
    patch = index_config["patch"]
    paths = index_config["paths"]
    paths_file = index_config["paths_file"]

    logging.info(
        f"Collecting metadata from {paths} and those in file (at {paths_file})..."
    )

    # Aggregate, sort, and validate filepaths
    paths = file_utils.sorted_unique_filepaths(
        file_of_filepaths=paths_file, list_of_filepaths=paths, abspaths=True
    )
    for p in paths:  # pylint: disable=C0103
        validate_path(p)

    # Aggregate & sort denylisted paths
    denylist = file_utils.sorted_unique_filepaths(
        file_of_filepaths=denylist_file,
        list_of_filepaths=denylist,
        abspaths=True,
    )

    # Grab and pack args
    indexer_flags: IndexerFlags = {
        "basic_only": basic_only,
        "dryrun": dryrun,
        "patch": patch,
    }

    # get a rest client to talk to the file catalog
    fc_rc_creator = partial(create_file_catalog_rest_client, oauth_config, rest_config)

    # create an IceProd metadata manager
    manager_creator = partial(MetadataManager, index_config, oauth_config, rest_config)

    # Go!
    if non_recursive:
        _index(paths, denylist, fc_rc_creator, manager_creator, indexer_flags)
    else:
        _recursively_index(
            paths, denylist, fc_rc_creator, manager_creator, indexer_flags, n_processes
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find files under PATH(s), compute their metadata and "
        "upload it to File Catalog.",
        epilog="Notes: (1) symbolic links are never followed.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "paths",
        default=defaults.PATHS,
        metavar="PATHS",
        nargs="*",
        help="path(s) to scan for files.",
    )
    parser.add_argument(
        "--basic-only",
        action="store_true",
        default=False,
        help="only collect basic metadata",
    )
    parser.add_argument(
        "--denylist",
        metavar="DENYPATH",
        nargs="+",
        default=defaults.DENYLIST,
        help="list of denylisted filepaths; Ex: /foo/bar/ will skip /foo/bar/*",
    )
    parser.add_argument(
        "--denylist-file",
        default=defaults.DENYLIST_FILE,
        help="a file containing denylisted filepaths on each line "
        "(this is a useful alternative to `--denylist` when there's many denylisted paths); "
        "Ex: /foo/bar/ will skip /foo/bar/*",
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        default=False,
        help="do everything except POSTing/PATCHing to the File Catalog",
    )
    parser.add_argument(
        "--iceprodv1-db-pass",
        default=defaults.ICEPRODV1_DB_PASS,
        help="IceProd1 SQL password",
    )
    parser.add_argument(
        "-l",
        "--log",
        default=defaults.LOG_LEVEL,
        help="the output logging level",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=defaults.N_PROCESSES,
        help="number of processes for multi-processing "
        "(ignored if using --non-recursive)",
    )
    parser.add_argument(
        "-n",
        "--non-recursive",
        action="store_true",
        default=False,
        help="do not recursively index / do not descend into subdirectories",
    )
    parser.add_argument(
        "--patch",
        action="store_true",
        default=False,
        help="replace/overwrite any existing File-Catalog entries (aka patch)",
    )
    parser.add_argument(
        "-f",
        "--paths-file",
        default=defaults.PATHS_FILE,
        help="new-line-delimited text file containing path(s) to scan for files. "
        "(use this option for a large number of paths)",
    )
    parser.add_argument(
        "-s",
        "--site",
        required=True,
        help='site value of the "locations" object',
    )
    add_auth_to_argparse(parser)
    args = parser.parse_args()

    # do some logging
    logging_tools.set_level(args.log, use_coloredlogs=True)
    logging_tools.log_argparse_args(args)

    index_config: IndexerConfiguration = {
        "basic_only": args.basic_only,
        "denylist": args.denylist,
        "denylist_file": args.denylist_file,
        "dryrun": args.dryrun,
        "iceprodv1_db_pass": args.iceprodv1_db_pass,
        "n_processes": args.processes,
        "non_recursive": args.non_recursive,
        "patch": args.patch,
        "paths": args.paths,
        "paths_file": args.paths_file,
        "site": args.site,
    }
    oauth_config = create_oauth_config(args)
    rest_config = create_rest_config(args)

    index(index_config, oauth_config, rest_config)
